// Project:   dfe-fetcher
// File:      src/source/gcp_pubsub/mod.rs
// Purpose:   GCP Pub/Sub pull source (REST synchronous pull)
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! GCP Pub/Sub pull source (REST synchronous pull).
//!
//! **SPECULATIVE - pending hyperi-infra issue (TBD).** This module is
//! fully written against the documented Pub/Sub REST API but cannot be
//! exercised against the live HyperI tenant until:
//!
//! 1. A Cloud Logging Log Sink is provisioned to fan log entries into a
//!    Pub/Sub topic.
//! 2. One or more Pub/Sub subscriptions are created on that topic.
//! 3. The fetcher's GCP service account is granted
//!    `roles/pubsub.subscriber` on each subscription.
//!
//! Until then the e2e tests in this area are `#[ignore]`'d with explicit
//! notes pointing at the pending infra issue.
//!
//! Transport: REST synchronous pull via
//! `POST /v1/projects/<project>/subscriptions/<sub>:pull`, then ack via
//! `POST /v1/projects/<project>/subscriptions/<sub>:acknowledge`. The
//! fetcher's volumes don't justify the gRPC StreamingPull variant -
//! it would require pulling in tonic + protobuf for marginal gain. If
//! a tenant ever sustains volumes that REST pull cannot keep up with,
//! revisit with a v2 source.
//!
//! Message handling: each Pub/Sub message's `data` field (base64) is
//! decoded; if the bytes parse as JSON the decoded value is emitted,
//! otherwise the raw UTF-8 string is wrapped in `{"data": "..."}`.
//! Pub/Sub attributes and the message ID are preserved under
//! `_dfe_fetcher_pubsub` on every record so consumers can correlate
//! back to the source message.

use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use tracing::{debug, info, warn};

use crate::config::{GcpPubsubSourceConfig, GcpPubsubSubscription};
use crate::credential;
use crate::error::{Error, Result};
use crate::source::{FetchResult, FetchWindow, Source};

const REQUEST_TIMEOUT: Duration = Duration::from_mins(1);

/// Pub/Sub subscriber scope. Allows `:pull` and `:acknowledge`; does not
/// allow publish or admin operations.
const PUBSUB_SCOPE: &str = "https://www.googleapis.com/auth/pubsub";

/// Default Pub/Sub API base.
const API_BASE_DEFAULT: &str = "https://pubsub.googleapis.com";

/// Default OAuth2 token endpoint.
const TOKEN_URL_DEFAULT: &str = "https://oauth2.googleapis.com/token";

/// Maximum ack IDs to send in a single `:acknowledge` request. The REST
/// API caps at 2048; we use 500 to keep request size small.
const ACK_BATCH_SIZE: usize = 500;

/// GCP Pub/Sub pull source.
pub struct GcpPubsubSource {
    config: GcpPubsubSourceConfig,
    client: reqwest::Client,
}

impl GcpPubsubSource {
    pub fn new(config: GcpPubsubSourceConfig) -> Self {
        let client = credential::http_client_with_timeout(REQUEST_TIMEOUT)
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { config, client }
    }

    fn api_base(&self) -> &str {
        self.config
            .api_url_override
            .as_deref()
            .unwrap_or(API_BASE_DEFAULT)
    }

    /// Exchange the SA key for a Pub/Sub-scoped access token. Mirrors the
    /// GCP source's plain-SA flow but uses the pubsub scope instead of
    /// `cloud-platform`.
    async fn get_access_token(&self) -> Result<String> {
        let key_json = if let Some(spec) = self.config.credential_secret.as_deref() {
            credential::resolve(spec).await?
        } else if let Some(path) = self.config.service_account_key.as_deref() {
            let resolved = credential::resolve(path).await?;
            std::fs::read_to_string(&resolved).map_err(|e| {
                Error::Credential(format!("failed to read Pub/Sub SA key file: {e}"))
            })?
        } else {
            return Err(Error::Credential(
                "gcp_pubsub requires either credential_secret or service_account_key".into(),
            ));
        };

        let key: serde_json::Value = serde_json::from_str(&key_json)
            .map_err(|e| Error::Credential(format!("invalid Pub/Sub SA key JSON: {e}")))?;

        let client_email = key["client_email"]
            .as_str()
            .ok_or_else(|| Error::Credential("missing client_email in Pub/Sub SA key".into()))?;
        let private_key = key["private_key"]
            .as_str()
            .ok_or_else(|| Error::Credential("missing private_key in Pub/Sub SA key".into()))?;
        let token_uri = self
            .config
            .token_url_override
            .as_deref()
            .or_else(|| key["token_uri"].as_str())
            .unwrap_or(TOKEN_URL_DEFAULT);

        let now = chrono::Utc::now().timestamp();
        let claims = serde_json::json!({
            "iss": client_email,
            "scope": PUBSUB_SCOPE,
            "aud": token_uri,
            "iat": now,
            "exp": now + 3600,
        });

        let encoding_key = jsonwebtoken::EncodingKey::from_rsa_pem(private_key.as_bytes())
            .map_err(|e| Error::Credential(format!("invalid Pub/Sub SA private key: {e}")))?;
        let header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
        let jwt = jsonwebtoken::encode(&header, &claims, &encoding_key)
            .map_err(|e| Error::Credential(format!("Pub/Sub JWT signing failed: {e}")))?;

        let resp = self
            .client
            .post(token_uri)
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("assertion", &jwt),
            ])
            .send()
            .await
            .map_err(|e| Error::Credential(format!("Pub/Sub token exchange failed: {e}")))?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Credential(format!(
                "Pub/Sub token exchange error: {body}"
            )));
        }

        let body: serde_json::Value = resp.json().await.map_err(|e| {
            Error::Credential(format!("failed to parse Pub/Sub token response: {e}"))
        })?;

        body["access_token"]
            .as_str()
            .map(String::from)
            .ok_or_else(|| Error::Credential("missing access_token in Pub/Sub response".into()))
    }

    /// Pull a batch of messages from one subscription, ack them, and
    /// return the decoded records. Returns `None` when the subscription
    /// was empty.
    async fn pull_subscription(
        &self,
        token: &str,
        sub: &GcpPubsubSubscription,
    ) -> Result<Option<FetchResult>> {
        let project = sub.project_id.as_str();
        let sub_id = sub.subscription_id.as_str();
        let pull_url = format!(
            "{}/v1/projects/{}/subscriptions/{}:pull",
            self.api_base(),
            project,
            sub_id,
        );

        let pull_body = serde_json::json!({
            "maxMessages": sub.max_messages,
            "returnImmediately": sub.return_immediately,
        });

        let resp = self
            .client
            .post(&pull_url)
            .bearer_auth(token)
            .json(&pull_body)
            .send()
            .await
            .map_err(|e| {
                Error::Source(format!(
                    "Pub/Sub pull request for {project}/{sub_id} failed: {e}"
                ))
            })?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Source(format!(
                "Pub/Sub pull returned {status} for {project}/{sub_id}: {body}"
            )));
        }

        let body: serde_json::Value = resp.json().await.map_err(|e| {
            Error::Source(format!(
                "Pub/Sub pull parse failed for {project}/{sub_id}: {e}"
            ))
        })?;

        let received = match body["receivedMessages"].as_array() {
            Some(arr) if !arr.is_empty() => arr.clone(),
            _ => return Ok(None),
        };

        let mut records: Vec<Bytes> = Vec::with_capacity(received.len());
        let mut ack_ids: Vec<String> = Vec::with_capacity(received.len());

        for rm in &received {
            if let Some(ack_id) = rm["ackId"].as_str() {
                ack_ids.push(ack_id.to_string());
            }

            let message = &rm["message"];
            let data_b64 = message["data"].as_str().unwrap_or("");
            let decoded = BASE64.decode(data_b64).unwrap_or_default();

            // Try JSON first - typical for Log Sink payloads. Fall back to
            // a string wrapper so non-JSON producers still flow through.
            let mut payload: serde_json::Value = match serde_json::from_slice(&decoded) {
                Ok(v) => v,
                Err(_) => {
                    let s = String::from_utf8_lossy(&decoded).into_owned();
                    serde_json::json!({ "data": s })
                }
            };

            // Attach Pub/Sub envelope so consumers can correlate back.
            let envelope = serde_json::json!({
                "subscription": format!("projects/{project}/subscriptions/{sub_id}"),
                "message_id":   message["messageId"].clone(),
                "publish_time": message["publishTime"].clone(),
                "attributes":   message["attributes"].clone(),
                "ordering_key": message["orderingKey"].clone(),
            });

            if let Some(obj) = payload.as_object_mut() {
                obj.insert("_dfe_fetcher_pubsub".to_string(), envelope);
            } else {
                payload = serde_json::json!({
                    "payload": payload,
                    "_dfe_fetcher_pubsub": envelope,
                });
            }

            let buf = serde_json::to_vec(&payload)
                .map_err(|e| Error::Source(format!("Pub/Sub record serialise failed: {e}")))?;
            records.push(Bytes::from(buf));
        }

        // Ack drained messages. Failure to ack is not fatal - the
        // message will redeliver after its ack deadline, downstream
        // dedupe by message_id handles the duplicate. Log and continue.
        for batch in ack_ids.chunks(ACK_BATCH_SIZE) {
            if let Err(e) = self.acknowledge(token, project, sub_id, batch).await {
                warn!(
                    error = %e,
                    project,
                    subscription = sub_id,
                    batch_size = batch.len(),
                    "Pub/Sub ack failed, will redeliver"
                );
            }
        }

        debug!(
            project,
            subscription = sub_id,
            records = records.len(),
            "Pub/Sub batch pulled"
        );

        Ok(Some(FetchResult {
            records,
            source: format!("gcp_pubsub.{sub_id}"),
            topic: self.config.topic.clone(),
        }))
    }

    async fn acknowledge(
        &self,
        token: &str,
        project: &str,
        sub_id: &str,
        ack_ids: &[String],
    ) -> Result<()> {
        let url = format!(
            "{}/v1/projects/{}/subscriptions/{}:acknowledge",
            self.api_base(),
            project,
            sub_id,
        );
        let body = serde_json::json!({ "ackIds": ack_ids });
        let resp = self
            .client
            .post(&url)
            .bearer_auth(token)
            .json(&body)
            .send()
            .await
            .map_err(|e| Error::Source(format!("Pub/Sub ack request failed: {e}")))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Source(format!(
                "Pub/Sub ack returned {status}: {body}"
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl Source for GcpPubsubSource {
    fn name(&self) -> &'static str {
        "gcp_pubsub"
    }

    fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    async fn fetch(&self, _window: Option<&FetchWindow>) -> Result<Vec<FetchResult>> {
        if !self.config.enabled || self.config.subscriptions.is_empty() {
            return Ok(vec![]);
        }

        info!(
            subscriptions = self.config.subscriptions.len(),
            "Pulling GCP Pub/Sub subscriptions"
        );

        let token = self.get_access_token().await?;

        let mut results = Vec::new();
        for sub in &self.config.subscriptions {
            match self.pull_subscription(&token, sub).await {
                Ok(Some(r)) => results.push(r),
                Ok(None) => {}
                Err(e) => warn!(
                    error = %e,
                    project = sub.project_id,
                    subscription = sub.subscription_id,
                    "Pub/Sub subscription pull failed, continuing"
                ),
            }
        }
        Ok(results)
    }

    async fn health_check(&self) -> Result<bool> {
        if !self.config.enabled {
            return Ok(false);
        }
        match self.get_access_token().await {
            Ok(_) => Ok(true),
            Err(e) => {
                warn!(error = %e, "Pub/Sub health check failed");
                Ok(false)
            }
        }
    }

    fn cursor_prefix(&self) -> String {
        "gcp_pubsub".to_string()
    }

    fn service_names(&self) -> Vec<&str> {
        self.config
            .subscriptions
            .iter()
            .map(|s| s.subscription_id.as_str())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_base_default_when_no_override() {
        let src = GcpPubsubSource::new(GcpPubsubSourceConfig::default());
        assert_eq!(src.api_base(), API_BASE_DEFAULT);
    }

    #[test]
    fn api_base_uses_override_when_present() {
        let cfg = GcpPubsubSourceConfig {
            api_url_override: Some("https://pubsub.googleapis.test".into()),
            ..GcpPubsubSourceConfig::default()
        };
        let src = GcpPubsubSource::new(cfg);
        assert_eq!(src.api_base(), "https://pubsub.googleapis.test");
    }

    #[test]
    fn subscription_defaults_are_sane() {
        // Direct struct construction would skip serde defaults, so go via
        // serde to exercise the default fns.
        let s: GcpPubsubSubscription =
            serde_json::from_str(r#"{"project_id":"p","subscription_id":"s"}"#)
                .expect("subscription parse");
        assert_eq!(s.max_messages, 1000);
        assert!(s.return_immediately);
    }

    #[tokio::test]
    async fn fetch_returns_empty_when_disabled() {
        let src = GcpPubsubSource::new(GcpPubsubSourceConfig::default());
        let r = src.fetch(None).await.expect("fetch should not error");
        assert!(r.is_empty());
    }
}
