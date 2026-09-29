// Project:   dfe-fetcher
// File:      crates/rest/src/origin.rs
// Purpose:   The hosts one unit's requests may be sent to
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The origins a unit may address.
//!
//! A rendered path that is a whole URL replaces the base URL, so a value the
//! provider supplied -- a manifest item's field, a pager's next URL, a key read
//! out of a response -- would otherwise choose the host the instance's
//! credential is sent to, over cleartext if it says `http://`.
//!
//! An [`OriginSet`] is what that value is held to. It is built per unit when the
//! profile binds, from configuration only: the unit's own `base_url`, the
//! instance's, and each `allow_hosts` entry the profile or the endpoint
//! declares. What the credential mode exposes as `auth.*` is added per tick
//! instead of at bind, because a token exchange may answer the host its own
//! token is for (Salesforce's `instance_url`).
//!
//! [`OriginSet::check`] runs in front of the signer, so the credential is never
//! applied to, nor minted for, a host the source may not address.

use dfe_fetcher_core::error::{Error, Result};
use serde_json::Value;

/// Scheme, host and port of a URL, with the port spelled out so an origin
/// written with its default port compares equal to the same one without it.
///
/// This is the comparison the redirect policy makes in
/// [`crate::request::http_client_with`], for the same reason.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Origin {
    scheme: String,
    host: String,
    port: u16,
}

impl Origin {
    /// The origin of `url`; `None` when it names no host, or when its scheme
    /// has no known default port and the URL spells none.
    fn of(url: &reqwest::Url) -> Option<Self> {
        Some(Self {
            scheme: url.scheme().to_owned(),
            host: url.host_str()?.to_ascii_lowercase(),
            port: url.port_or_known_default()?,
        })
    }

    /// The origin of a rendered template; `None` when it is not an absolute URL.
    fn parse(text: &str) -> Option<Self> {
        Self::of(&reqwest::Url::parse(text.trim()).ok()?)
    }
}

impl std::fmt::Display for Origin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}://{}:{}", self.scheme, self.host, self.port)
    }
}

/// The origins one unit's requests may go to.
#[derive(Debug, Clone, Default)]
pub struct OriginSet {
    permitted: Vec<Origin>,
}

impl OriginSet {
    /// An empty set, which permits nothing.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Permit the origin of `url`, answering false when it is not an absolute
    /// URL naming a host.
    pub fn permit(&mut self, url: &str) -> bool {
        let Some(origin) = Origin::parse(url) else {
            return false;
        };
        if !self.permitted.contains(&origin) {
            self.permitted.push(origin);
        }
        true
    }

    /// Whether `url` names a permitted origin; `exposed` is what the credential
    /// mode put in `auth.*` for this tick.
    fn permits(&self, url: &reqwest::Url, exposed: Option<&Value>) -> bool {
        let Some(origin) = Origin::of(url) else {
            return false;
        };
        self.permitted.contains(&origin) || exposed_origins(exposed).any(|named| named == origin)
    }

    /// Refuse a request whose host this unit may not address.
    ///
    /// # Errors
    ///
    /// Returns [`Error::OriginRefused`], counted as `origin_refused`: the text
    /// names the ORIGIN and never the URL, whose query carries the credential
    /// under `auth.api_key.query`.
    pub fn check(&self, url: &reqwest::Url, exposed: Option<&Value>) -> Result<()> {
        if self.permits(url, exposed) {
            return Ok(());
        }
        Err(Error::OriginRefused(format!(
            "this source's credential is not sent to `{}`: a rendered URL may name only the \
             host of the unit's `base_url`, of the instance's, of a credential field the profile \
             exposes, or of an `allow_hosts` entry",
            refused_name(url)
        )))
    }
}

/// How a refused URL is named: its origin, or its scheme and host when no port
/// is known for that scheme.
fn refused_name(url: &reqwest::Url) -> String {
    Origin::of(url).map_or_else(
        || format!("{}://{}", url.scheme(), url.host_str().unwrap_or_default()),
        |origin| origin.to_string(),
    )
}

/// The origins of the `auth.*` values that are absolute URLs.
fn exposed_origins(exposed: Option<&Value>) -> impl Iterator<Item = Origin> + '_ {
    exposed
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(serde_json::Map::values)
        .filter_map(|value| Origin::parse(value.as_str()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(text: &str) -> reqwest::Url {
        reqwest::Url::parse(text).expect("a URL")
    }

    fn set(permitted: &[&str]) -> OriginSet {
        let mut origins = OriginSet::new();
        for entry in permitted {
            assert!(origins.permit(entry), "`{entry}` is an origin");
        }
        origins
    }

    /// The comparison is exact on all three parts of an origin, with the
    /// default port of a scheme standing in for an unwritten one.
    #[test]
    fn an_origin_matches_on_scheme_host_and_port_and_nothing_else() {
        let permitted = set(&["https://example.com", "http://127.0.0.1:8080"]);
        assert!(permitted.permits(&url("https://example.com/v1/events?a=b"), None));
        assert!(
            permitted.permits(&url("https://example.com:443/v1"), None),
            "the default port of the scheme is the same origin"
        );
        assert!(
            permitted.permits(&url("https://EXAMPLE.COM/v1"), None),
            "a host is matched case-insensitively"
        );
        assert!(
            !permitted.permits(&url("https://example.com.evil.net/v1"), None),
            "a suffix is a different host, which is why there are no wildcards"
        );
        assert!(
            !permitted.permits(&url("https://sub.example.com/v1"), None),
            "so is a subdomain"
        );
        assert!(
            !permitted.permits(&url("http://example.com/v1"), None),
            "http is not https: a cleartext hop hands the credential to the path"
        );
        assert!(
            !permitted.permits(&url("https://example.com:8443/v1"), None),
            "another port is another origin"
        );
        assert!(
            !permitted.permits(&url("http://127.0.0.1:9090/x"), None),
            "and so is another port on loopback"
        );
    }

    /// `localhost` and `127.0.0.1` are one socket and two hosts, which is what
    /// the fixture tests refuse across.
    #[test]
    fn a_name_and_the_address_it_resolves_to_are_different_origins() {
        let permitted = set(&["http://127.0.0.1:34567"]);
        assert!(permitted.permits(&url("http://127.0.0.1:34567/x"), None));
        assert!(!permitted.permits(&url("http://localhost:34567/x"), None));
    }

    /// An IPv6 host is compared as the URL writes it, brackets and all.
    #[test]
    fn a_bracketed_ipv6_host_matches_itself_and_not_another_address() {
        let permitted = set(&["https://[2001:db8::1]"]);
        assert!(permitted.permits(&url("https://[2001:db8::1]/v1"), None));
        assert!(
            permitted.permits(&url("https://[2001:DB8::1]:443/v1"), None),
            "the same address written in upper case, on the default port"
        );
        assert!(!permitted.permits(&url("https://[2001:db8::2]/v1"), None));
        assert!(!permitted.permits(&url("https://[::1]/v1"), None));
    }

    /// A URL the credential mode exposed is permitted for that tick alone, and
    /// only the ones that are URLs are read.
    #[test]
    fn an_exposed_field_that_is_a_url_permits_its_origin() {
        let permitted = set(&["https://login.example.com"]);
        let exposed = serde_json::json!({
            "instance_url": "https://acme.my.example.com",
            "token_type": "Bearer",
            "expires_in": 3600
        });
        let target = url("https://acme.my.example.com/services/data/query");
        assert!(!permitted.permits(&target, None));
        assert!(permitted.permits(&target, Some(&exposed)));
        assert!(
            !permitted.permits(&url("https://other.example.com/x"), Some(&exposed)),
            "a host no field named stays refused"
        );
    }

    /// An empty set permits nothing, and a value that is not an absolute URL
    /// widens nothing.
    #[test]
    fn nothing_is_permitted_by_default_and_a_bare_host_permits_nothing() {
        let mut origins = OriginSet::new();
        assert!(!origins.permits(&url("https://manage.office.com/x"), None));
        assert!(!origins.permit("manage.office.com"), "no scheme, no origin");
        assert!(!origins.permit(""), "and an empty entry is not one either");
        assert!(
            !origins.permits(&url("https://manage.office.com/x"), None),
            "neither entry widened anything"
        );
    }

    /// The refusal names the origin and carries no path or query, because a
    /// query may hold the credential.
    #[test]
    fn the_refusal_names_the_origin_and_not_the_url() {
        let err = set(&["https://example.com"])
            .check(&url("http://evil.net:8080/blob/1?api_key=sekrit"), None)
            .expect_err("another host");
        let text = err.to_string();
        assert!(matches!(err, Error::OriginRefused(_)), "{err:?}");
        assert_eq!(err.api_error_code(), "origin_refused");
        assert!(text.contains("http://evil.net:8080"), "{text}");
        assert!(!text.contains("/blob/1"), "{text}");
        assert!(!text.contains("sekrit"), "{text}");
    }
}
