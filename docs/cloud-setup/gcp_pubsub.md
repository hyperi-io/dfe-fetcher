<!-- Project:   dfe-fetcher                         -->
<!-- File:      docs/cloud-setup/gcp_pubsub.md        -->
<!-- Purpose:   GCP Pub/Sub pull cloud admin setup guide -->
<!-- Language:  Markdown                              -->
<!--                                                  -->
<!-- License:   FSL-1.1-ALv2                          -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED           -->

# GCP Pub/Sub Setup for dfe-fetcher

What a GCP administrator needs to configure so dfe-fetcher can pull audit and
log entries delivered through a Cloud Logging sink into a Pub/Sub topic.

> Status: alpha - code-complete, not production-validated; additionally
> pending a Log Sink + Pub/Sub topic + subscription before it can be
> exercised against a live tenant.

## Overview

dfe-fetcher consumes Pub/Sub via **REST synchronous pull**. For each configured
subscription it calls
`POST /v1/projects/<project>/subscriptions/<sub>:pull`, then acknowledges the
batch with
`POST /v1/projects/<project>/subscriptions/<sub>:acknowledge`. Each message's
base64 `data` field is decoded; if it parses as JSON (typical for Cloud Logging
sink payloads, where `data` is a base64-encoded `LogEntry`) the value is emitted
directly, otherwise the raw string is wrapped as `{"data": "..."}`. The Pub/Sub
envelope (subscription, message ID, publish time, attributes, ordering key) is
attached under `_dfe_fetcher_pubsub` on every record. Each subscription produces
records tagged `gcp_pubsub.<subscription-id>`.

Authentication is a GCP **service account** (JSON key, or vault-resolved key),
signing an RS256 JWT exchanged for an access token with the read-side scope:

- `https://www.googleapis.com/auth/pubsub`

The tenant side owns the delivery pipeline: a Cloud Logging **log sink** routes
matching entries to a Pub/Sub **topic**, a **pull subscription** sits on that
topic, and the fetcher's service account holds `roles/pubsub.subscriber` on that
subscription. dfe-fetcher only pulls and acknowledges; it never publishes or
administers Pub/Sub.

## Prerequisites

- A GCP project to own the topic, subscription, and log sink. A separate project
  can own the fetcher service account if you prefer.
- `gcloud` CLI authenticated with at least `roles/pubsub.editor` and
  `roles/logging.configWriter` (or Owner) on the destination project.
- Owner (or equivalent) on the destination project to bind the sink writer
  identity to the topic.

## Required Permissions

| Service | Object/Endpoint/Scope | IAM role or grant | Notes |
|---------|-----------------------|-------------------|-------|
| Cloud Logging sink | `pubsub.googleapis.com/projects/<p>/topics/<t>` | `roles/logging.configWriter` (operator, to create the sink) | gcloud does NOT auto-grant the publish role |
| Sink writer identity | the sink's `writerIdentity` SA | `roles/pubsub.publisher` on the topic | Without this, no messages are routed |
| Fetcher service account | `.../subscriptions/<sub>:pull` and `:acknowledge`; scope `https://www.googleapis.com/auth/pubsub` | `roles/pubsub.subscriber` on the subscription | Subscription-scoped (least privilege) |

The fetcher's access is **read-side only** - pull and acknowledge. It cannot
publish or change subscription configuration.

## Source-Side Setup

### 1. Create the Pub/Sub topic

```bash
gcloud pubsub topics create dfe-audit-logs \
  --project=your-project-id
```

### 2. Create the Cloud Logging sink to the topic

The Pub/Sub destination path format is
`pubsub.googleapis.com/projects/<project>/topics/<topic>`. Use `--log-filter` to
scope what is routed (Logging query language). For Cloud Audit Logs:

```bash
gcloud logging sinks create dfe-audit-sink \
  pubsub.googleapis.com/projects/your-project-id/topics/dfe-audit-logs \
  --log-filter='logName:"cloudaudit.googleapis.com"' \
  --project=your-project-id
```

An omitted `--log-filter` routes ALL log entries - usually far more than wanted.
Other useful filters: `severity>=WARNING`,
`logName:"cloudaudit.googleapis.com" OR logName:"iam.googleapis.com"`.

### 3. Grant the sink writer identity publish rights on the topic

Creating a sink via gcloud does NOT authorise the destination - you must do it
manually. Read the sink's writer identity, then bind `roles/pubsub.publisher` on
the topic:

```bash
WRITER=$(gcloud logging sinks describe dfe-audit-sink \
  --project=your-project-id \
  --format='value(writerIdentity)')

gcloud pubsub topics add-iam-policy-binding dfe-audit-logs \
  --member="$WRITER" \
  --role="roles/pubsub.publisher" \
  --project=your-project-id
```

The writer identity looks like
`serviceAccount:service-<num>@gcp-sa-logging.iam.gserviceaccount.com`. Routing
of newly ingested entries begins as soon as this binding exists.

### 4. Create a pull subscription on the topic

```bash
gcloud pubsub subscriptions create dfe-audit-fetcher \
  --topic=dfe-audit-logs \
  --ack-deadline=60 \
  --message-retention-duration=7d \
  --project=your-project-id
```

Subscriptions are pull by default (no push config set), which is what
dfe-fetcher expects.

### 5. Create the fetcher service account and key

```bash
gcloud iam service-accounts create dfe-fetcher-pubsub \
  --display-name="dfe-fetcher Pub/Sub puller" \
  --project=your-project-id

gcloud iam service-accounts keys create pubsub-sa.json \
  --iam-account=dfe-fetcher-pubsub@your-project-id.iam.gserviceaccount.com
```

Store `pubsub-sa.json` securely. For GKE, prefer Workload Identity over a static
key.

### 6. Grant the fetcher SA subscriber rights on the subscription

Subscription-scoped grant (least privilege - the SA can read only this one
subscription):

```bash
gcloud pubsub subscriptions add-iam-policy-binding dfe-audit-fetcher \
  --member="serviceAccount:dfe-fetcher-pubsub@your-project-id.iam.gserviceaccount.com" \
  --role="roles/pubsub.subscriber" \
  --project=your-project-id
```

## dfe-fetcher Configuration

Each entry under `subscriptions` is one `project_id` + `subscription_id` pair.
`max_messages` defaults to 1000 (the REST cap); `return_immediately` defaults to
`true` (single-shot per tick, fitting the polling model). The `subscription_id`
is the short name, not the fully-qualified path.

### Config File

```yaml
sources:
  gcp_pubsub:
    enabled: true
    service_account_key: "/etc/gcp/pubsub-sa.json"
    # api_url_override: "https://pubsub.googleapis.com"           # default
    # token_url_override: "https://oauth2.googleapis.com/token"   # default
    subscriptions:
      - project_id: "your-project-id"
        subscription_id: "dfe-audit-fetcher"
        max_messages: 1000          # default; REST cap is 1000
        return_immediately: true    # default; single-shot per tick
    topic: "gcp_pubsub"
    # filter: '_dfe_fetcher_pubsub.subscription.contains("audit")'   # hot-reloaded
```

### Environment Variables

```bash
DFE_FETCHER_SOURCES__GCP_PUBSUB__ENABLED="true"
DFE_FETCHER_SOURCES__GCP_PUBSUB__SERVICE_ACCOUNT_KEY="/etc/gcp/pubsub-sa.json"
```

The `subscriptions` list is a structured array - set it in the config file (or a
vault-backed config), not via individual environment variables.

### Secrets Manager

Keep the SA key out of the config file with a vault spec:

```yaml
sources:
  gcp_pubsub:
    enabled: true
    credential_secret: "vault:secret/gcp-pubsub:sa_key"
    subscriptions:
      - project_id: "your-project-id"
        subscription_id: "dfe-audit-fetcher"
    topic: "gcp_pubsub"
```

`credential_secret` resolves to the full service account JSON key as a string.
Provide exactly one of `credential_secret` or `service_account_key`.

## Verification

dfe-fetcher's `health_check` for this source signs a JWT and performs the
Pub/Sub-scoped token exchange; it returns `true` when the SA key is valid. The
env-gated e2e tests in
[`tests/e2e/smoke_remote.rs`](../../tests/e2e/smoke_remote.rs) exercise the live
pull path. They are `#[ignore]`'d until the tenant side is provisioned.

```bash
export GCP_PUBSUB_SA_KEY="/etc/gcp/pubsub-sa.json"
# or GCP_PUBSUB_CREDENTIAL_SECRET="vault:secret/gcp-pubsub:sa_key"

# Token exchange only:
cargo test --test smoke_remote gcp_pubsub_health_check -- --ignored --nocapture

# Real pull from a subscription (empty subscription is a valid pass):
export GCP_PUBSUB_PROJECT_ID="your-project-id"
export GCP_PUBSUB_SUBSCRIPTION_ID="dfe-audit-fetcher"
cargo test --test smoke_remote gcp_pubsub_pull_subscription -- --ignored --nocapture
```

Common failures:

- `403` on `:pull` - the fetcher SA lacks `roles/pubsub.subscriber` on the
  subscription, or the scope/key is wrong.
- `404` on `:pull` - wrong `project_id`/`subscription_id`, or the subscription
  was never created.
- Pull succeeds but returns nothing - the sink writer identity was not granted
  `roles/pubsub.publisher` on the topic (step 3), the `--log-filter` matches no
  entries, or the window is genuinely quiet. Verify routing with the Cloud
  console's log-sink view.
- `invalid SA key / missing client_email` - the key JSON is malformed or the
  wrong file.

## Cost

dfe-fetcher only reads (pull + acknowledge). Pub/Sub and log routing may
generate usage charges depending on message throughput and retention, so
there could be a cost at higher volumes; keep the sink `--log-filter` tight
to limit what is routed. Confirm any cost implications against your own GCP
agreement.

## References

- Route logs to supported destinations: https://docs.cloud.google.com/logging/docs/export/configure_export_v2
- View logs routed to Pub/Sub: https://docs.cloud.google.com/logging/docs/export/pubsub
- gcloud logging sinks create / CLI reference: https://cloud.google.com/logging/docs/reference/tools/gcloud-logging
- Create pull subscriptions: https://docs.cloud.google.com/pubsub/docs/create-subscription
- gcloud pubsub subscriptions create: https://cloud.google.com/sdk/gcloud/reference/pubsub/subscriptions/create
- Pub/Sub access control (roles): https://docs.cloud.google.com/pubsub/docs/access-control
