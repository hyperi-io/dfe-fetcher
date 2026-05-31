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
All native sources read only -- the fetcher never writes, modifies, or deletes
anything on the provider side.

For the fetcher's own configuration (scheduler, output, credential resolution),
see [config.example.yaml](../../config.example.yaml) and
[../DESIGN.md](../DESIGN.md).

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
| Bitwarden | Event logs | [bitwarden.md](bitwarden.md) |
| 1Password | Audit and sign-in events | [onepassword.md](onepassword.md) |
| Slack | Audit logs | [slack.md](slack.md) |
| GitHub | Audit log | [github.md](github.md) |
| Salesforce | Event monitoring logs | [salesforce.md](salesforce.md) |
| Object store | Buckets (S3 and compatible) | [object_store.md](object_store.md) |
| Package registries | PyPI, crates.io, Go modules | [supply-chain.md](supply-chain.md) |

## Source Maturity

A source declares its own release maturity in code; this is the single source of
truth. The fetcher logs a warning at startup for any enabled source that is not
yet stable.

| Stage | Meaning |
|-------|---------|
| Alpha | Code-complete but not production-validated. Behaviour and config may change. The default for a new source. |
| Beta | Validated against a live service; hardening in progress. |
| Stable | Production-ready. |

The core providers -- AWS, Azure, Microsoft 365, and Google Cloud -- are stable.
Every other source is alpha until explicitly promoted. The authoritative value
is the `maturity()` method on each source (`src/source/mod.rs`); promote a source
there rather than recording its stage in these guides.
