<!-- Project:   dfe-fetcher                           -->
<!-- File:      docs/cloud-setup/README.md             -->
<!-- Purpose:   Index of provider setup guides and source-maturity reference -->
<!-- Language:  Markdown                               -->
<!--                                                   -->
<!-- License:   BUSL-1.1                               -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED            -->

# Cloud and SaaS Setup Guides

Each guide here covers what a provider administrator configures so dfe-fetcher
can pull data from that provider: the access it needs, how to grant it
least-privilege, where to put credentials, and how to verify the connection.
Every source reads only -- the fetcher never writes, modifies, or deletes
anything on the provider side.

For the fetcher's own configuration (scheduler, output, credential resolution,
the `sources.db` and `sources.file` blocks), see
[config.example.yaml](../../config.example.yaml); the architecture and the
profile grammar are in [../DESIGN.md](../DESIGN.md).

## Credentials in Kubernetes

The released chart mounts one Secret, Kafka's: its `username`, `password` and `sasl.mechanism` keys become `DFE_FETCHER_KAFKA_SASL_USER`, `DFE_FETCHER_KAFKA_SASL_PASSWORD` and `DFE_FETCHER_KAFKA_SASL_MECHANISM`. Name an existing Secret with the chart value `secrets.kafka.existingSecret`.

A provider's credentials are yours to supply, through the chart's `extraEnv` (a map of variable name to value or `valueFrom`) or `extraEnvFrom` (a list of `envFrom` sources) values. Two shapes work, and each guide's Environment Variables section names the fields:

- set the field itself, `DFE_FETCHER_SOURCES__<BLOCK>__<FIELD>`, from a Secret key, e.g. `DFE_FETCHER_SOURCES__AWS__SECRET_ACCESS_KEY`
- write the field as an `env:NAME` spec in the config, and load a Secret whose keys are those names with `extraEnvFrom`

```yaml
extraEnv:
  DFE_FETCHER_SOURCES__AZURE__CLIENT_SECRET:
    valueFrom:
      secretKeyRef:
        name: fetcher-azure
        key: client-secret
extraEnvFrom:
  - secretRef:
      name: fetcher-credentials
```

An empty value counts as unset, so a Secret key left blank leaves the field unset rather than signing requests with an empty key.

## Providers

| Provider | Data it pulls | Guide |
|----------|---------------|-------|
| AWS | Security and operational logs | [aws.md](aws.md) |
| Azure | Platform and security logs | [azure.md](azure.md) |
| Microsoft 365 | Audit and security logs | [m365.md](m365.md) |
| Google Cloud | Audit and security logs | [gcp.md](gcp.md) |
| Google Cloud Pub/Sub | Subscription streaming | [gcp_pubsub.md](gcp_pubsub.md) |
| Google Workspace | Admin and audit logs | [google_workspace.md](google_workspace.md) |
| Okta | System log | [okta.md](okta.md) |
| Cisco Duo | Admin and authentication logs | [duo.md](duo.md) |
| CrowdStrike | Falcon detections and events | [crowdstrike.md](crowdstrike.md) |
| Cloudflare | Audit logs | [cloudflare.md](cloudflare.md) |
| Datadog | Audit trail and security signals | [datadog.md](datadog.md) |
| Bitwarden | Event logs | [bitwarden.md](bitwarden.md) |
| 1Password | Audit and sign-in events | [onepassword.md](onepassword.md) |
| Slack | Audit logs | [slack.md](slack.md) |
| GitHub | Audit log | [github.md](github.md) |
| Salesforce | Event monitoring logs | [salesforce.md](salesforce.md) |
| runZero | Asset inventory snapshots | [runzero.md](runzero.md) |
| Object store | Buckets (S3 and compatible) | [object_store.md](object_store.md) |
| Package registries | PyPI, crates.io, Go modules | [supply-chain.md](supply-chain.md) |

## Source Maturity

A source's release maturity is the `maturity` field of its shipped profile
under `crates/fetcher/profiles/`; the capability catalog
(`docs/capability-catalog.yaml`) repeats it and a test keeps the two equal.
The fetcher logs a warning at startup for any enabled source that is not yet
stable.

| Stage | Meaning |
|-------|---------|
| Alpha | Code-complete but not production-validated. Behaviour and config may change. The default for a new source. |
| Beta | Validated against a live service; hardening in progress. |
| Stable | Production-ready. |

Promote a source in its profile rather than recording its stage in these
guides.
