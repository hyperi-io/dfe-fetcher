<!-- Project:   dfe-fetcher                                                              -->
<!-- File:      docs/extractors.md                                                       -->
<!-- Purpose:   Container and Vector extractors: lifecycle, communication and guarantees -->
<!-- Language:  Markdown                                                                 -->
<!--                                                                                     -->
<!-- License:   BUSL-1.1                                                                 -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED                                              -->

# Extractors

Beside the source framework the fetcher runs external tools: a container, read through its stdout JSON lines or HTTP posts, and Vector, received over its native gRPC protocol. Neither runs a tick or keeps a checkpoint: an extractor's records go through the same enrichment and emit path as a source's ([delivery.md](delivery.md)), and its guarantee is set out below.

An external tool's records reach the fetcher three ways, and all three meet at the extractor sink before the emitter.

```mermaid
---
config:
  theme: base
  look: neo
  fontFamily: Inter, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif
  flowchart:
    curve: linear
    nodeSpacing: 40
    rankSpacing: 64
    padding: 18
    wrappingWidth: 280
    htmlLabels: true
  elk:
    preset: modelOrder
  themeVariables:
    fontSize: 15px
    lineColor: "#64748B"
    primaryTextColor: "#0F172A"
    clusterBorder: "#0072B2"
    edgeLabelBackground: "#FFFFFF"
  themeCSS: >-
    p{margin:0}
    .node[data-look] rect,.node[data-look] path,.node[data-look] polygon,.node[data-look] .outer-path{filter:drop-shadow(0 1px 2px rgba(15,23,42,.10)) drop-shadow(0 6px 14px rgba(15,23,42,.10))}
    .node rect{rx:10px;ry:10px}
    .cluster rect{rx:16px;ry:16px}
    .node .label div{min-width:150px}
    .node.decision .label div{min-width:0}
    .cluster[data-look] rect{filter:none}
    .nodeLabel{line-height:1.35}
    .nodeLabel b{font-weight:600;font-size:15px;letter-spacing:-0.01em}
    .nodeLabel span{font-size:12.5px;font-weight:400;color:#334155}
    .cluster-label span.nodeLabel{display:inline-block;background:#FFFFFF;color:#0B2A3F;border:1.5px solid #0072B2;border-radius:999px;padding:2px 12px;font-size:11.5px;font-weight:700;letter-spacing:.09em;text-transform:uppercase}
    .flowchart-link{stroke-width:1.75px}
    .labelBkg{background:transparent!important}
    span.edgeLabel{display:inline-block;background:#FFFFFF!important;color:#1E293B;border:1px solid #94A3B8;border-radius:999px;padding:2px 10px;font-size:12px;font-weight:500}
    span.edgeLabel p{background:transparent!important}
    .label-icon{width:20px;height:20px;vertical-align:-4px;margin-right:6px}
    .ingest .label-icon{color:#0E8A7D}
    .xform .label-icon{color:#7B4FB0}
    .load .label-icon{color:#0072B2}
    .bus .label-icon{color:#A8517F}
    .store .label-icon{color:#00785A}
    .ui .label-icon{color:#D9480F}
    .ext .label-icon{color:#64748B}
---
flowchart LR
    accTitle: L4 Detail -- Extractors
    accDescr: External tools reach the extractors three ways. The container extractor reads a container's stdout line by line, the ingest server takes HTTP posts and the Vector receiver takes Vector's gRPC protocol. All three hand JSON records to the extractor sink, which enriches and batches them for the emitter.

    tools["<b>External tools</b>"]

    subgraph extractors["Extractors"]
        container[["<b>Container</b><br/><span>stdout reader</span>"]]
        ingest["<b>Ingest server</b><br/><span>HTTP 8080</span>"]
        vector["<b>Vector receiver</b><br/><span>gRPC 6000</span>"]
        sink["<b>Extractor sink</b>"]
    end

    emitter["<b>Emitter</b>"]

    tools -->|records| container
    tools -->|records| ingest
    tools -->|records| vector
    container --> sink
    ingest --> sink
    vector --> sink
    sink -->|records| emitter
  classDef ext fill:#F4F6F8,stroke:#94A3B8,stroke-width:1.5px,stroke-dasharray:5 4,color:#0F172A
  classDef ingest fill:#FFFFFF,stroke:#0E8A7D,stroke-width:3px,color:#0F172A
  classDef xform fill:#FFFFFF,stroke:#7B4FB0,stroke-width:3px,color:#0F172A
  classDef load fill:#FFFFFF,stroke:#0072B2,stroke-width:3px,color:#0F172A
  classDef bus fill:#FFFFFF,stroke:#A8517F,stroke-width:3px,color:#0F172A
  classDef store fill:#FFFFFF,stroke:#00785A,stroke-width:3px,color:#0F172A
  classDef ui fill:#FFFFFF,stroke:#D9480F,stroke-width:3px,color:#0F172A
  classDef decision fill:#FFFFFF,stroke:#9A6700,stroke-width:3px,color:#0F172A
  classDef error fill:#FFFFFF,stroke:#C2410C,stroke-width:3px,color:#0F172A
  classDef control fill:#FFFFFF,stroke:#334155,stroke-width:3px,color:#0F172A
  classDef part fill:#FFFFFF,stroke:#94A3B8,stroke-width:1.5px,color:#0F172A
  class tools ext
  class container,ingest,vector,sink,emitter part
  style extractors fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
```

*Figure 1. L4 Detail -- Extractors. The dashed box is external.* Up: [L3 Components -- dfe-fetcher](../README.md#architecture). Down: [L4 Detail -- Container extractor lifecycle](#container-extractor-lifecycle).

A container extractor runs its tool in Docker or podman and reads JSON lines from its stdout. A container can instead post JSON to the ingest server, and Vector, in a container or a sidecar, pushes over Vector's gRPC protocol. The ingest server and the Vector receiver are off until a deployment enables them.

## Container Extractor Lifecycle

A scheduled container waits between runs, a continuous one restarts after a backoff, and either stops at shutdown or, if continuous, at its restart limit.

```mermaid
---
config:
  theme: base
  look: neo
  fontFamily: Inter, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif
  themeVariables:
    fontSize: 15px
    lineColor: "#64748B"
    primaryTextColor: "#0F172A"
    clusterBorder: "#0072B2"
    edgeLabelBackground: "#FFFFFF"
    primaryColor: "#FFFFFF"
    primaryBorderColor: "#94A3B8"
    tertiaryColor: "#F4F6F8"
---
stateDiagram-v2
    direction LR
    accTitle: L4 Detail -- Container extractor lifecycle
    accDescr: A scheduled container waits an interval, runs, and waits again. A continuous container runs at once and, when it exits, waits out a backoff and runs again, It stops for good when it exits more than max_restart_attempts times with no stable run between. Shutdown stops it from any state.
    [*] --> Running: continuous
    [*] --> Waiting: scheduled
    Waiting --> Running: tick
    Running --> Waiting: done
    Running --> Backoff: exited
    Backoff --> Running: retry
    Running --> Stopped: limit
    Running --> Stopped: shutdown
    Waiting --> Stopped: shutdown
    Backoff --> Stopped: shutdown
```

*Figure 2. L4 Detail -- Container extractor lifecycle.* Up: [L4 Detail -- Extractors](extractors.md).

A continuous extractor that exits is restarted after a backoff that doubles up
to 64 s, or up to `max_restart_backoff_secs` when that is lower, so a setting
above 64 changes nothing. The backoff resets once a run has stayed
up for `stable_after_secs`. A container that exits more than
`max_restart_attempts` times with no stable run between stops for good (0, the
default, restarts it for ever). No health check runs during a container's run:
an extractor is judged by its container's exit.

## Container Communication

The HTTP mode listener needs `ingest.auth_token`: every `/ingest` call has `Authorization: Bearer <token>`. With no token the listener will not start unless `ingest.allow_unauthenticated: true` is set. The `/livez` and `/readyz` probes are exempt.

The stdout reader and the ingest server hand each line or body to the extractor sink as it came, and the Vector receiver converts a push's events to JSON first. The sink enriches them as [delivery.md](delivery.md) describes.

What each mode guarantees:

- Vector: the receiver is built armed, so with `extractors.vector.acknowledgements.enabled` (the default) a push is not answered until its events are emitted: `OK` once the outputs took them or the DLQ confirmed them, `UNAVAILABLE` otherwise, which Vector's sink retries. A push still unanswered near its hold budget (25 s, less when Vector sets a deadline) is answered `UNAVAILABLE` as well, so a slow output can duplicate but not lose. Disabled, a push is answered once queued and a crash loses it. When the fetcher's memory-pressure latch holds, a push is refused `UNAVAILABLE` before any work.
- HTTP: with `ingest.acknowledgements.enabled` (the default), `/ingest` answers `200` once the outputs took the record, not before, or the DLQ confirmed it, and `503` otherwise, with `Retry-After` when the outputs pushed back. Disabled, a ready pipeline answers `200` at once and delivers the record after, so a failed delivery or a crash loses it. Such a loss counts in `dfe_fetcher_extractor_records_failed_total{extractor="ingest",outcome="dropped"}`.
- stdout: a pipe cannot be read twice, so a line the outputs cannot take yet is held and retried, pausing from 1 s up to 30 s, and the container's stdout is not read meanwhile, so the container blocks on its writes until the outputs recover. A line is dropped when a failure no retry can clear (no output configured), or a line still held at shutdown after one more attempt, is dropped, counted in `dfe_fetcher_extractor_records_failed_total{extractor="container",outcome="dropped"}` and never in the received count.

## Shutdown and the Vector listeners

At shutdown the extractors deliver what they hold before the outputs close, for up to 20 s: the Vector receiver rejects further pushes and delivers what it has queued, and a container extractor stops its container and reads its stdout to the end. A scheduled container's `timeout_secs` bounds the whole run, stdout included: at the timeout the container is killed and the run fails. A line held for an output outage is not interrupted by the timeout. The kill follows once the line is delivered.

A Vector listener that cannot bind at start-up fails the process: it drains, stops its containers and exits non-zero, so the pod restarts rather than stay Ready with a dead intake.

A Vector push names no instance, so the listener it arrives on decides its topic. The shared `extractors.vector.grpc_bind_address`, the one port the chart publishes, has at most one instance and lands on that instance's `topic`, or on `vector` when none shares it. Every other instance names a separate `grpc_bind_address` and lands on that `topic`. Two instances sharing the listener are refused at load.

## Profile, container or Vector

A REST or JSON API becomes a profile, a database or files a db or file block, and anything else a Vector or container extractor.

```mermaid
---
config:
  theme: base
  look: neo
  fontFamily: Inter, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif
  flowchart:
    curve: linear
    nodeSpacing: 40
    rankSpacing: 64
    padding: 18
    wrappingWidth: 280
    htmlLabels: true
  elk:
    preset: modelOrder
  themeVariables:
    fontSize: 15px
    lineColor: "#64748B"
    primaryTextColor: "#0F172A"
    clusterBorder: "#0072B2"
    edgeLabelBackground: "#FFFFFF"
  themeCSS: >-
    p{margin:0}
    .node[data-look] rect,.node[data-look] path,.node[data-look] polygon,.node[data-look] .outer-path{filter:drop-shadow(0 1px 2px rgba(15,23,42,.10)) drop-shadow(0 6px 14px rgba(15,23,42,.10))}
    .node rect{rx:10px;ry:10px}
    .cluster rect{rx:16px;ry:16px}
    .node .label div{min-width:150px}
    .node.decision .label div{min-width:0}
    .cluster[data-look] rect{filter:none}
    .nodeLabel{line-height:1.35}
    .nodeLabel b{font-weight:600;font-size:15px;letter-spacing:-0.01em}
    .nodeLabel span{font-size:12.5px;font-weight:400;color:#334155}
    .cluster-label span.nodeLabel{display:inline-block;background:#FFFFFF;color:#0B2A3F;border:1.5px solid #0072B2;border-radius:999px;padding:2px 12px;font-size:11.5px;font-weight:700;letter-spacing:.09em;text-transform:uppercase}
    .flowchart-link{stroke-width:1.75px}
    .labelBkg{background:transparent!important}
    span.edgeLabel{display:inline-block;background:#FFFFFF!important;color:#1E293B;border:1px solid #94A3B8;border-radius:999px;padding:2px 10px;font-size:12px;font-weight:500}
    span.edgeLabel p{background:transparent!important}
    .label-icon{width:20px;height:20px;vertical-align:-4px;margin-right:6px}
    .ingest .label-icon{color:#0E8A7D}
    .xform .label-icon{color:#7B4FB0}
    .load .label-icon{color:#0072B2}
    .bus .label-icon{color:#A8517F}
    .store .label-icon{color:#00785A}
    .ui .label-icon{color:#D9480F}
    .ext .label-icon{color:#64748B}
---
flowchart LR
    accTitle: L4 Detail -- Choosing a mode for a source
    accDescr: A REST or JSON API becomes a profile. A database or a set of files becomes a sources.db or sources.file block. Anything else runs on Vector when Vector has a source for it, and in a container otherwise.

    api{"<b>REST or<br/>JSON API?</b>"}
    profile(["<b>Profile</b>"])
    store{"<b>Database<br/>or files?</b>"}
    dbfile(["<b>sources.db or<br/>sources.file</b>"])
    hasvector{"<b>Vector has<br/>a source?</b>"}
    vector(["<b>Vector</b>"])
    container(["<b>Container</b>"])

    api -->|yes| profile
    api -->|no| store
    store -->|yes| dbfile
    store -->|no| hasvector
    hasvector -->|yes| vector
    hasvector -->|no| container
  classDef ext fill:#F4F6F8,stroke:#94A3B8,stroke-width:1.5px,stroke-dasharray:5 4,color:#0F172A
  classDef ingest fill:#FFFFFF,stroke:#0E8A7D,stroke-width:3px,color:#0F172A
  classDef xform fill:#FFFFFF,stroke:#7B4FB0,stroke-width:3px,color:#0F172A
  classDef load fill:#FFFFFF,stroke:#0072B2,stroke-width:3px,color:#0F172A
  classDef bus fill:#FFFFFF,stroke:#A8517F,stroke-width:3px,color:#0F172A
  classDef store fill:#FFFFFF,stroke:#00785A,stroke-width:3px,color:#0F172A
  classDef ui fill:#FFFFFF,stroke:#D9480F,stroke-width:3px,color:#0F172A
  classDef decision fill:#FFFFFF,stroke:#9A6700,stroke-width:3px,color:#0F172A
  classDef error fill:#FFFFFF,stroke:#C2410C,stroke-width:3px,color:#0F172A
  classDef control fill:#FFFFFF,stroke:#334155,stroke-width:3px,color:#0F172A
  classDef part fill:#FFFFFF,stroke:#94A3B8,stroke-width:1.5px,color:#0F172A
  class api,store,hasvector decision
  class profile,dbfile,vector,container part
```

*Figure 3. L4 Detail -- Choosing a mode for a source.* Up: [L3 Components -- dfe-fetcher](../README.md#architecture).

A profile that needs a signer, a lister, a row builder or a paging quirk adds a hook on the matching axis in `crates/rest` and stays a profile.

| Criterion | REST profile | Container | Vector |
|-----------|--------------|-----------|--------|
| A plain REST or JSON API | Yes | - | - |
| A mature tool in another language does the job | - | Yes | - |
| Vector has a native source for it | - | - | Yes |
| Needs process isolation from the fetcher | - | Yes | - |
| Third-party maintained | - | Yes | Yes |
| Wanted: checkpoint after ack, the snapshot envelope, the memory brake | Yes | - | - |

Sources the fetcher pulls, and the mode each takes:

| Source | Mode | Reason |
|--------|------|--------|
| AWS CloudTrail | Profile | JSON API signed with the `sigv4` auth mode |
| Azure Activity Log | Profile | REST API paged by `nextLink` |
| M365 Audit Log | Profile | A prelude, a paged content list and a manifest per blob |
| runZero inventory | Profile | NDJSON exports as snapshot dumps |
| An inventory database | `sources.db` | A table dumped whole or tailed by key |
| Prometheus exporters | Container | A mature exporter run as a sidecar |
| Syslog collection | Vector | Vector has a native syslog source |

For log sources that established tools handle well, use container extractors
with a collection agent that can print JSON lines or POST JSON, such as Vector
or Fluent Bit. Filebeat has no HTTP output ([its outputs](https://www.elastic.co/docs/reference/beats/filebeat/configuring-output) are Elastic Cloud, Elasticsearch, Logstash, Kafka, Redis, file, console and discard), so ship Beats data to dfe-receiver's Lumberjack listener instead.

| Source Type | Sidecar Tool | Communication |
|-------------|-------------|---------------|
| Syslog | Vector or Fluent Bit | HTTP POST to `/ingest/syslog` |
| Windows Event Logs | A forwarder that POSTs JSON (winlogbeat is a separate Beat with the same output limits) | HTTP POST to `/ingest/windows_events` |
| Custom app logs | A shipper that prints JSON lines | stdout JSON lines |
| Network captures | Zeek / Suricata | stdout JSON lines |
| Cloud provider logs | Cloud-specific CLI tools | stdout JSON lines |

Each sidecar runs as a container extractor with `mode: continuous` and
communicates via stdout (JSON lines) or HTTP POST to the ingest endpoint.
The fetcher manages the container lifecycle including restart-on-crash with
exponential backoff. A log file the fetcher itself can reach is a
`sources.file` tail unit instead.
