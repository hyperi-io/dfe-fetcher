// Project:   dfe-fetcher
// File:      crates/rest/src/request.rs
// Purpose:   The one place a request is built, signed, sent, retried and measured
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The request executor.
//!
//! Every provider call in the REST shapes goes through [`RequestExecutor::send`]:
//! build the request (a `reqwest::Request` is not reusable across attempts, so
//! the caller supplies a builder closure), apply the [`AuthMode`], send, and
//! retry the statuses the profile's [`RetrySpec`] names with exponential
//! backoff and jitter, honouring `Retry-After` up to the backoff ceiling.
//! 401 and 403 are never retried: a refused call ticks the provider's own
//! throttle and a second refusal only lengthens the penalty. The metrics label
//! comes from the TYPED status; nothing here matches on message text.
//!
//! scalo's `HttpClient::execute` has the same retry loop but only a synchronous
//! customise hook, and signing is async, so the loop lives here as well.

use std::time::{Duration, Instant};

use backon::BackoffBuilder;
use bytes::Bytes;

use dfe_fetcher_core::error::{Error, Result};
use dfe_fetcher_core::metric_names;

use crate::auth::AuthMode;
use crate::profile::RetrySpec;
use crate::profile::template::TemplateCtx;

/// Bytes of a non-2xx body read for the error text.
const ERROR_BODY_BYTES: usize = 4096;

/// The HTTP client every REST shape sends through, named here so the app
/// never spells the HTTP crate.
pub type HttpClient = reqwest::Client;

/// How long a connection may take to open.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// How long one body read may wait for the next bytes before the request
/// fails; the clock resets on every chunk, so a body that keeps arriving
/// has no total bound. The driver's memory-gate hold
/// (`self_regulation.max_hold_secs`) plus one flush must stay under it: the
/// clock is armed when a read finds nothing ready and is not read again
/// until the driver polls the body next.
pub const READ_TIMEOUT: Duration = Duration::from_secs(60);

/// Build the shared client every REST shape sends through: a connect
/// timeout, an idle read timeout instead of a total one (a store dumped as
/// one streamed body takes as long as it takes, gated by the driver), the
/// fetcher's `User-Agent` (the workspace version is the app's), TLS via
/// rustls, gzip and brotli decoded. A unit whose decoder reads the page
/// whole may put a total bound on its own requests with `timeout_secs`.
///
/// # Errors
///
/// Returns [`Error::Source`] when the TLS backend cannot be initialised.
pub fn http_client() -> Result<HttpClient> {
    http_client_with(CONNECT_TIMEOUT, READ_TIMEOUT)
}

/// [`http_client`] with its two timeouts chosen by the caller.
///
/// Redirects are followed within the request's own origin only (scheme,
/// host and port unchanged): the profile's `api_key.header` may name any
/// header, so a cross-host or HTTPS-to-HTTP hop would carry the credential
/// to a host the profile never named; such a 3xx surfaces as the API error
/// it is and a profile that expects one can `ignore_status` it. The pagers
/// follow next-page URLs themselves.
///
/// # Errors
///
/// Returns [`Error::Source`] when the TLS backend cannot be initialised.
pub fn http_client_with(connect: Duration, read: Duration) -> Result<HttpClient> {
    reqwest::Client::builder()
        .user_agent(concat!("dfe-fetcher/", env!("CARGO_PKG_VERSION")))
        .connect_timeout(connect)
        .read_timeout(read)
        .redirect(reqwest::redirect::Policy::custom(same_origin_only))
        .pool_max_idle_per_host(10)
        .build()
        .map_err(|e| Error::Source(format!("failed to build HTTP client: {e}")))
}

/// Follow a redirect only when it stays on the origin the request went to.
fn same_origin_only(attempt: reqwest::redirect::Attempt) -> reqwest::redirect::Action {
    let target = attempt.url();
    let same = attempt.previous().first().is_some_and(|origin| {
        origin.scheme() == target.scheme()
            && origin.host() == target.host()
            && origin.port_or_known_default() == target.port_or_known_default()
    });
    if same && attempt.previous().len() <= 10 {
        attempt.follow()
    } else {
        attempt.stop()
    }
}

/// Builds, signs, sends and retries one instance's requests.
#[derive(Debug, Clone)]
pub struct RequestExecutor {
    client: reqwest::Client,
    retry: RetrySpec,
    error_at: Option<String>,
    quota: Vec<(String, String)>,
}

impl RequestExecutor {
    /// An executor over `client` with the profile's retry policy, error
    /// pointer and quota headers.
    #[must_use]
    pub fn new(
        client: reqwest::Client,
        retry: RetrySpec,
        error_at: Option<String>,
        quota: Vec<(String, String)>,
    ) -> Self {
        Self {
            client,
            retry,
            error_at,
            quota,
        }
    }

    /// The HTTP client, for the token exchange.
    #[must_use]
    pub fn client(&self) -> &reqwest::Client {
        &self.client
    }

    /// Send a request, retrying per the policy. `make` builds a fresh request
    /// per attempt; `idempotent` says whether a non-2xx may be retried at
    /// all; a status in `ignore` is the caller's expected answer, handed back
    /// as `None` without a retry and without counting as an API error.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Api`] for the final non-2xx status (never retried, or
    /// retries exhausted), [`Error::Source`] when the provider is unreachable,
    /// and the credential error when the auth mode cannot be applied.
    pub async fn send(
        &self,
        source: &str,
        auth: &AuthMode,
        auth_ctx: &TemplateCtx,
        idempotent: bool,
        ignore: &[u16],
        make: impl Fn() -> Result<reqwest::Request>,
    ) -> Result<Option<reqwest::Response>> {
        let may_retry = idempotent || self.retry.retry_non_idempotent;
        let mut backoff = backon::ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(self.retry.min_backoff_ms))
            .with_max_delay(Duration::from_millis(self.retry.max_backoff_ms))
            .with_max_times(self.retry.max_retries as usize)
            .with_jitter()
            .build();
        loop {
            let mut request = make()?;
            auth.authorize(&mut request, auth_ctx).await?;
            let started = Instant::now();
            let outcome = self.client.execute(request).await;
            metrics::histogram!(metric_names::API_DURATION_SECONDS, "source" => source.to_owned())
                .record(started.elapsed().as_secs_f64());
            let (error, retry_after) = match outcome {
                Ok(response) if response.status().is_success() => {
                    metrics::counter!(metric_names::PAGES_FETCHED_TOTAL, "source" => source.to_owned())
                        .increment(1);
                    self.record_quota(source, &response);
                    return Ok(Some(response));
                }
                Ok(response) if ignore.contains(&response.status().as_u16()) => {
                    tracing::debug!(
                        source,
                        status = response.status().as_u16(),
                        "ignored status"
                    );
                    return Ok(None);
                }
                Ok(response) => {
                    let status = response.status().as_u16();
                    let retry_after = self.retry_after(&response);
                    let text = self.error_text(response).await;
                    let error = Error::Api { status, text };
                    if !(may_retry && self.retry.retries(status)) {
                        return Err(self.fail(source, error));
                    }
                    (error, retry_after)
                }
                Err(e) => {
                    // reqwest's Display appends the request URL, which carries
                    // the credential under `auth.api_key.query`.
                    let error = Error::Source(format!("request failed: {}", e.without_url()));
                    if !may_retry {
                        return Err(self.fail(source, error));
                    }
                    (error, None)
                }
            };
            let Some(delay) = backoff.next() else {
                return Err(self.fail(source, error));
            };
            let delay = retry_after.map_or(delay, |ra| {
                ra.min(Duration::from_millis(self.retry.max_backoff_ms))
            });
            tracing::debug!(source, error = %error, delay_ms = delay.as_millis(), "retrying request");
            tokio::time::sleep(delay).await;
        }
    }

    /// Count the final error under its typed code and hand it back.
    fn fail(&self, source: &str, error: Error) -> Error {
        metrics::counter!(
            metric_names::API_ERRORS_TOTAL,
            "source" => source.to_owned(),
            "code" => error.api_error_code()
        )
        .increment(1);
        error
    }

    fn retry_after(&self, response: &reqwest::Response) -> Option<Duration> {
        if !self.retry.retry_after_header {
            return None;
        }
        response
            .headers()
            .get(reqwest::header::RETRY_AFTER)?
            .to_str()
            .ok()?
            .trim()
            .parse::<u64>()
            .ok()
            .map(Duration::from_secs)
    }

    /// The provider's error text: the value at `error.at` when the body is JSON
    /// and carries it, else the body's first bytes.
    async fn error_text(&self, response: reqwest::Response) -> String {
        let body: Bytes = response.bytes().await.unwrap_or_default();
        let body = &body[..body.len().min(ERROR_BODY_BYTES)];
        if let Some(pointer) = &self.error_at
            && let Ok(value) = serde_json::from_slice::<serde_json::Value>(body)
            && let Some(text) = value.pointer(pointer)
        {
            return match text {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
        }
        String::from_utf8_lossy(body).into_owned()
    }

    fn record_quota(&self, source: &str, response: &reqwest::Response) {
        for (name, header) in &self.quota {
            if let Some(value) = response
                .headers()
                .get(header.as_str())
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.trim().parse::<f64>().ok())
            {
                metrics::gauge!(
                    format!("{}{name}", metric_names::API_QUOTA_PREFIX),
                    "source" => source.to_owned()
                )
                .set(value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_shared_client_builds_on_this_platform() {
        assert!(http_client().is_ok());
    }
}
