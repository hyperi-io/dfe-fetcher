<!-- Project:   dfe-fetcher                                        -->
<!-- File:      docs/reference/configuration.md                    -->
<!-- Purpose:   How the configuration loads, and what reloads      -->
<!-- Language:  Markdown                                           -->
<!--                                                               -->
<!-- License:   BUSL-1.1                                           -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED                        -->

# Configuration cascade

Four layers merge, the first winning, and a reload runs the merge again.

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
    nodeBorder: "#334155"
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
    .icon-shape path[stroke-width="0"][fill="none"]{fill:#FFFFFF;stroke:#FFFFFF;stroke-width:12px;stroke-linejoin:round}
    .icon-shape p{background:transparent!important;color:#0F172A}
    .ingest .label-icon{color:#0E8A7D}
    .xform .label-icon{color:#7B4FB0}
    .load .label-icon{color:#0072B2}
    .bus .label-icon{color:#A8517F}
    .store .label-icon{color:#00785A}
    .ui .label-icon{color:#D9480F}
    .ext .label-icon{color:#64748B}
---
flowchart TB
    accTitle: L4 Detail -- Configuration cascade
    accDescr: CLI flags, DFE_FETCHER environment variables, the config file and defaults merge, the first layer winning. The merged config is validated and becomes the running config. A reload goes back through the layers through the same merge.

    subgraph layers["Layers, first wins"]
        direction LR
        cli("<b>CLI flags</b>")
        env("<b>Env vars</b><br/><span>DFE_FETCHER_*</span>")
        file("<b>Config file</b><br/><span>--config</span>")
        defaults("<b>Defaults</b>")
        cli ~~~ env ~~~ file ~~~ defaults
    end

    subgraph load["Load"]
        direction LR
        merge("<b>Merge</b>")
        validate("<b>Validate</b>")
        running("<b>Running config</b>")
        merge --> validate --> running
        running -.->|reload| merge
    end

    layers --> load
  class cli,env,file,defaults,merge,validate,running part
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
  style layers fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style load fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
```

*Figure 1. L4 Detail -- Configuration cascade. The reload arrow fires on SIGHUP, a file change or every `config_reload_secs`.* Up: [L3 Components -- dfe-fetcher](../architecture.md).

The file is the one `--config` names, and there is no `DFE_FETCHER_CONFIG` variable. Without `--config`, scalo's cascade stands in for the file: `.env` from the working directory, then `settings.<env>.yaml`, `settings.yaml` and `defaults.yaml` from the working directory, `./config/`, `/config/` and the user's config directory. A nested name reaches its key with either separator after the prefix (`DFE_FETCHER_SOURCES__AWS__REGION` and `DFE_FETCHER__SOURCES__AWS__REGION`), and an empty value counts as unset. The flat names `apply_flat_env` reads (`DFE_FETCHER_KAFKA_BROKERS` and the like) are applied last. The runtime's sections (`logger`, `metrics`, `version_check`) come from the `--config` file, or from scalo's cascade without one.

Validation binds every profile, database and file instance, so a bad field fails the load with its path rather than the first tick. Outside the profile grammar a key the config does not know is skipped and logged once the logger is up. A key inside a section logs at WARN, because a misspelt key does nothing. A top-level key logs at INFO, because it may be a section the scalo runtime reads.

## What reloads

The file is polled for a change at least every 5 s. A reload that does not validate is refused and the running config kept.

Each source reads its filter, the routes, the topic suffix, the org, `unwrap_nested_json`, its batch bounds, the oversize policy and the memory brake once per tick. The scheduler re-reads the interval, the jitter and the `cursor` window settings. A reload that adds, removes or changes a source starts, stops or rebuilds that source as a restart would, its `env:` and `vault:` specs resolved first.

The outputs, the DLQ, the memory guard, the cursor store, `scheduler.max_concurrent_fetches`, the instance id, the ingest listener and the container and Vector extractors are built once, so a change to them needs a restart. On Kubernetes the chart rolls the pod for any config edit outside the deployment contract's `hot_paths` (`crates/fetcher/src/deployment.rs`), so there most edits restart the pod whatever the process could have reloaded.
