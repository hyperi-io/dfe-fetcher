<!-- Project:   dfe-fetcher                                                              -->
<!-- File:      docs/extractors.md                                                       -->
<!-- Purpose:   Container and Vector extractors: lifecycle, communication and guarantees -->
<!-- Language:  Markdown                                                                 -->
<!--                                                                                     -->
<!-- License:   BUSL-1.1                                                                 -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED                                              -->

# Extractors

The extractors are the part of [dfe-fetcher](architecture.md) that runs external tools and takes their records, with no tick and no checkpoint behind them.

| | |
|---|---|
| Repo | [hyperi-io/dfe-fetcher](https://github.com/hyperi-io/dfe-fetcher), `crates/fetcher/src/extractor/` and `crates/fetcher/src/ingest/` |
| Image and chart | `ghcr.io/hyperi-io/dfe-fetcher`, and the thin chart each release assembles from `crates/fetcher/src/deployment.rs` |
| Config reference | `extractors` and `ingest` in [config.example.yaml](../config.example.yaml). Every change needs a restart |
| Endpoints | Ingest HTTP on 8080 (`POST /ingest/{source}`, `POST /ingest/{source}/{topic}`), Vector gRPC on 6000. Both ship off |
| Metrics | `dfe_fetcher_extractor_*` and `dfe_fetcher_ingest_*` on `:9090/metrics` |

A container can reach the fetcher two ways: printing JSON lines to stdout, or posting JSON to the ingest server. Vector runs in a container or as a sidecar and pushes over its native gRPC protocol.

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
flowchart TB
    accTitle: L4 Detail -- Extractors
    accDescr: External tools reach the extractors three ways. The container extractor reads a container's stdout line by line, the ingest server takes HTTP posts and the Vector receiver takes Vector's gRPC protocol. All three hand JSON records to the extractor sink, which stamps and batches them for the emitter.

    tools("<b>External tools</b><br/><span>a container, Vector, a shipper</span>")

    subgraph EX["Extractors"]
        direction LR
        container("<b>Container</b><br/><span>stdout reader</span>")
        ingest("<b>Ingest server</b><br/><span>HTTP 8080</span>")
        vector("<b>Vector receiver</b><br/><span>gRPC 6000</span>")
        sink("<b>Extractor sink</b><br/><span>stamp, batch</span>")
        container --> sink
        ingest --> sink
        vector --> sink
    end

    emitter("<b>Emitter</b><br/><span>send, retry, dead-letter</span>")

    tools -->|records| EX
    EX -->|records| emitter
  class tools ext
  class container,ingest,vector,sink,emitter part
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
  style EX fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
```

*Figure 1. L4 Detail -- Extractors. The dashed box is outside DFE.* Up: [L3 Components -- dfe-fetcher](architecture.md). Down: [L4 Detail -- Container extractor lifecycle](#how-it-works).

## How it works

A container extractor runs its tool in Docker or podman. A scheduled one runs every `interval_secs` and waits between runs. A continuous one starts at once and is restarted after it exits.

```mermaid
---
config:
  theme: base
  fontFamily: Inter, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif
  themeVariables:
    fontSize: 15px
    lineColor: "#64748B"
    primaryTextColor: "#0F172A"
    actorBkg: "#FFFFFF"
    actorBorder: "#94A3B8"
    actorTextColor: "#0F172A"
    actorLineColor: "#64748B"
    signalColor: "#64748B"
    signalTextColor: "#0F172A"
  themeCSS: >-
    .messageText{paint-order:stroke;stroke:#FFFFFF;stroke-width:3px;stroke-linejoin:round}
---
stateDiagram-v2
    direction LR
    accTitle: L4 Detail -- Container extractor lifecycle
    accDescr: A scheduled container waits an interval, runs, and waits again. A continuous container runs at once and, when it exits, waits out a backoff and runs again. It stops for good when it exits more than max_restart_attempts times with no stable run between. Shutdown stops it from any state.
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

The backoff doubles up to 64 s, or up to `max_restart_backoff_secs` when that is lower. It resets once a run stays up for `stable_after_secs`. A container that exits more than `max_restart_attempts` times with no stable run between stops for good (0, the default, restarts it forever). No health check runs during a run: an extractor is judged by its container's exit.

The stdout reader and the ingest server hand each line or body to the sink as it came. The Vector receiver converts a push's events to JSON first. The sink stamps them as the [Emitter](delivery.md) page sets out.

## Interfaces

The ingest server needs `ingest.auth_token`, and every `/ingest` call carries `Authorization: Bearer <token>`. With no token it won't start unless `ingest.allow_unauthenticated: true` is set. `/livez` and `/readyz` are exempt. `/ingest/{source}/{topic}` takes its topic from the URL, so an open listener lets anything that reaches the port write to any topic.

The Vector receiver speaks cleartext gRPC, so a proxy in front of it must speak h2c. A Vector push names no instance, so the listener it arrives on decides its topic. The shared `extractors.vector.grpc_bind_address`, the one port the chart publishes, takes at most one instance and lands on its `topic`, or on `vector` when none shares it. Every other instance names a separate `grpc_bind_address`. Two instances sharing a listener are refused at load.

## Scaling and failure

What each mode guarantees its sender:

- Vector: With `extractors.vector.acknowledgements.enabled` (the default), a push is answered `OK` once the outputs took its events or the DLQ confirmed them, and `UNAVAILABLE` otherwise. Vector's sink retries. A push still unanswered near its 25 s hold budget is answered `UNAVAILABLE` too, so a slow output can duplicate but not lose. Disabled, a push is answered once queued and a crash loses it. Under memory pressure a push is refused `UNAVAILABLE` before any work.
- HTTP: With `ingest.acknowledgements.enabled` (the default), `/ingest` answers `200` once the outputs took the record or the DLQ confirmed it, and `503` otherwise, with `Retry-After` on backpressure. Disabled, it answers `200` at once and holds the record until the outputs take it. At most `accumulate.in_flight` records wait that way, and past that `/ingest` answers `503` with `Retry-After`. A crash, or a shutdown while the outputs still refuse it, loses the record.
- stdout: A pipe can't be read twice, so a line the outputs can't take is held and retried from 1 s up to 30 s. The container blocks on its writes meanwhile. A line with no output to go to, or still held at shutdown, is dropped and counted.

At shutdown the extractors deliver what they hold for up to 20 s before the outputs close. A scheduled container's `timeout_secs` bounds the whole run, stdout included. A Vector listener that can't bind at start-up fails the process, so the pod restarts rather than stay Ready with a dead intake.

## Profile, container or Vector

A REST or JSON API becomes a profile, a database or files a `sources.db` or `sources.file` block, and anything else an extractor.

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
flowchart TB
    accTitle: L4 Detail -- Choosing a mode for a source
    accDescr: A REST or JSON API becomes a profile. A database or a set of files becomes a sources.db or sources.file block. Anything else runs on Vector when Vector has a source for it, and in a container otherwise.

    subgraph Q1["API"]
        direction LR
        api{"<b>REST or<br/>JSON API?</b>"}
        profile(["<b>Profile</b>"])
        api -->|yes| profile
    end

    subgraph Q2["Store"]
        direction LR
        store{"<b>Database<br/>or files?</b>"}
        dbfile(["<b>sources.db or<br/>sources.file</b>"])
        store -->|yes| dbfile
    end

    subgraph Q3["Anything else"]
        direction LR
        hasvector{"<b>Vector has<br/>a source?</b>"}
        vector(["<b>Vector</b>"])
        container(["<b>Container</b>"])
        hasvector -->|yes| vector
        hasvector -->|no| container
    end

    Q1 -->|no| Q2
    Q2 -->|no| Q3

  class api,store,hasvector decision
  class profile,dbfile,vector,container part
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
  style Q1 fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style Q2 fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style Q3 fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
```

*Figure 3. L4 Detail -- Choosing a mode for a source.* Up: [L4 Detail -- Extractors](extractors.md).

A profile that needs a signer, a lister, a row builder or a paging quirk adds a hook on the matching axis in `crates/rest` and stays a profile.

| Criterion | REST profile | Container | Vector |
|-----------|--------------|-----------|--------|
| A plain REST or JSON API | Yes | - | - |
| A mature tool in another language does the job | - | Yes | - |
| Vector has a native source for it | - | - | Yes |
| Needs process isolation from the fetcher | - | Yes | - |
| Third-party maintained | - | Yes | Yes |
| Wanted: checkpoint after ack, the snapshot envelope, the memory brake | Yes | - | - |

| Source | Mode | Reason |
|--------|------|--------|
| AWS CloudTrail | Profile | JSON API signed with the `sigv4` auth mode |
| Azure Activity Log | Profile | REST API paged by `nextLink` |
| M365 Audit Log | Profile | A prelude, a paged content list and a manifest per blob |
| runZero inventory | Profile | NDJSON exports as snapshot dumps |
| SAP S/4HANA entity set | Profile | OData V2 pages followed by `d.__next` |
| An inventory database | `sources.db` | A table dumped whole or tailed by key |
| Prometheus exporters | Container | A mature exporter run as a sidecar |
| Syslog collection | Vector | Vector has a native syslog source |
| Network captures | Container | Zeek or Suricata printing JSON lines |

A log file the fetcher can read is a `sources.file` tail unit, not a container. Filebeat has no HTTP output ([its outputs](https://www.elastic.co/docs/reference/beats/filebeat/configuring-output)), so Beats data goes to dfe-receiver's Lumberjack listener instead.

## Key metrics

| Metric | Healthy when |
|---|---|
| `dfe_fetcher_active_extractors` | Matches the extractors configured |
| `dfe_fetcher_extractor_runs_error_total` | Flat. A rise is a container exiting non-zero |
| `dfe_fetcher_extractor_restart_exhausted_total` | Zero. Any count is a container stopped for good |
| `dfe_fetcher_extractor_records_failed_total{extractor,outcome}` | No `dropped` outcomes |
| `dfe_fetcher_ingest_requests_total` | `503`s stay rare |

## Related

- [Emitter](delivery.md), where the sink's batches go
- [Source framework](internals.md), the scheduled path for APIs, databases and files
- [config.example.yaml](../config.example.yaml), every extractor and ingest key
