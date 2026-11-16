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
Every source is read-side: M365 starts its activity-feed subscription and
Pub/Sub acknowledges what it delivered. Nothing else changes provider state.

For the fetcher's configuration (scheduler, output, credential resolution,
the `sources.db` and `sources.file` blocks), see
[config.example.yaml](../../config.example.yaml). The architecture is in
[../internals.md](../internals.md) and the profile grammar in
[../reference/profile-grammar.md](../reference/profile-grammar.md).

| Provider | Data it pulls | Guide |
|----------|---------------|-------|
| AWS | Security and operational logs | [aws.md](aws.md) |
| Azure | Platform and security logs | [azure.md](azure.md) |
| Microsoft 365 | Audit and security logs | [m365.md](m365.md) |
| Google Cloud | Audit and security logs | [gcp.md](gcp.md) |
| Google Cloud Pub/Sub | Pull subscriptions | [gcp-pubsub.md](gcp-pubsub.md) |
| Google Workspace | Reports API activity per application | [google-workspace.md](google-workspace.md) |
| Okta | System log | [okta.md](okta.md) |
| Cisco Duo | Authentication logs | [duo.md](duo.md) |
| CrowdStrike | Falcon alerts | [crowdstrike.md](crowdstrike.md) |
| Cloudflare | Audit logs | [cloudflare.md](cloudflare.md) |
| Datadog | Audit trail and security signals | [datadog.md](datadog.md) |
| Bitwarden | Event logs | [bitwarden.md](bitwarden.md) |
| 1Password | Audit and sign-in events | [onepassword.md](onepassword.md) |
| Slack | Audit logs | [slack.md](slack.md) |
| GitHub | Audit log | [github.md](github.md) |
| Salesforce | Setup audit trail, login history, event log files | [salesforce.md](salesforce.md) |
| runZero | Asset inventory snapshots | [runzero.md](runzero.md) |
| Object store | Buckets (S3 and compatible) | [object-store.md](object-store.md) |
| Package registries | PyPI, crates.io, Go modules | [supply-chain.md](supply-chain.md) |

A source's maturity is the `maturity` field of its profile under `crates/fetcher/profiles/`: `alpha`, `beta` or `stable`. The fetcher logs a warning at startup for an enabled source below stable. The capability catalog (`docs/capability-catalog.yaml`) repeats the field, and `catalog_maturity_matches_source_maturity` keeps the two equal.

Each guide's health check is the profile's probe, or its credential exchange where the profile has none. It runs from that source's live test where it has one, `cargo nextest run -p dfe-fetcher --test e2e --run-ignored only <source>_health_check`, never from a deployed fetcher: its `/readyz` answers from startup and memory pressure alone. In a deployment the first tick is the check: it runs one fetch interval after start and logs `unit tick complete` for each unit, or `unit tick failed` with the provider's answer.

## Credentials in Kubernetes

The released chart mounts one Secret, Kafka's: its `username`, `password` and `sasl.mechanism` keys become `DFE_FETCHER_KAFKA_SASL_USER`, `DFE_FETCHER_KAFKA_SASL_PASSWORD` and `DFE_FETCHER_KAFKA_SASL_MECHANISM`. Name an existing Secret with the chart value `secrets.kafka.existingSecret`.

A provider's credentials are yours to supply, through the chart's `extraEnv` (a map of variable name to value or `valueFrom`) or `extraEnvFrom` (a list of `envFrom` sources) values. Two ways work, and each guide's Environment Variables section names the fields:

- set the field itself, `DFE_FETCHER_SOURCES__<BLOCK>__<FIELD>`, from a Secret key, e.g. `DFE_FETCHER_SOURCES__AWS__SECRET_ACCESS_KEY`
- write the field as an `env:NAME` spec in the config, and load a Secret keyed by those names with `extraEnvFrom`

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
