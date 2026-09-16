// Project:   dfe-fetcher
// File:      crates/core/src/secret.rs
// Purpose:   A credential spec resolved once, on first use, through the resolver an I/O crate supplies
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The once-resolved secret.
//!
//! A credential spec (`vault:<mount>/data/<path>:<key>` and its `bao:` and
//! `openbao:` spellings, `env:VAR`, `file:<path>`, or a literal) is resolved
//! the first time it is read and held for the life of its owner, so a vault
//! round trip happens once per process, not once per tick. The cell is here;
//! the resolver is the I/O crate's, named by the type parameter, so core never
//! reaches a vault or the environment itself.

use std::fmt;
use std::marker::PhantomData;

use scalo::SensitiveString;
use tokio::sync::OnceCell;

use crate::error::Result;

/// Turns a credential spec into its plaintext.
pub trait ResolveSecret {
    /// Resolve `spec`; a spec that does not resolve is a credential error.
    fn resolve(spec: &str) -> impl Future<Output = Result<SensitiveString>> + Send;
}

/// Prefixes the resolver strips and resolves; a spec starting with anything
/// else is a literal credential. `bao:` and `openbao:` are the OpenBao
/// tooling's spellings of the same KV lookup as `vault:`.
const RESOLVED_PREFIXES: [&str; 5] = ["vault:", "bao:", "openbao:", "env:", "file:"];

/// Prefixes the resolver's vocabulary has but this build does not carry the
/// provider for, so one would reach its consumer as its own literal text.
/// `aws:` is AWS Secrets Manager, behind scalo's `secrets-aws` feature, which
/// pulls the AWS SDK in; the fetcher signs AWS requests with reqsign instead
/// and takes its keys as `sigv4` identity fields.
const UNRESOLVED_PREFIXES: [&str; 1] = ["aws:"];

/// The credential-spec prefix `spec` carries, and whether it is written
/// exactly as the resolver matches it.
///
/// The resolver compares the prefix exactly, so `Vault:` and a leading space
/// are near misses that resolve to nothing; they are recognised here so
/// [`spec_issue`] can refuse them rather than let them reach a driver or a
/// provider as literal text.
fn spec_prefix(spec: &str) -> Option<(&'static str, bool)> {
    let candidate = spec.trim_start().to_ascii_lowercase();
    RESOLVED_PREFIXES
        .iter()
        .chain(UNRESOLVED_PREFIXES.iter())
        .find(|prefix| candidate.starts_with(**prefix))
        .map(|prefix| (*prefix, spec.starts_with(prefix)))
}

/// Whether `spec` references a credential rather than carrying one, so its
/// text is a path or a variable name and never the value itself.
#[must_use]
pub fn is_reference(spec: &str) -> bool {
    spec_prefix(spec).is_some()
}

/// What is wrong with `spec` as a credential reference, or `None` when the
/// resolver can be left to it.
///
/// A spec the resolver cannot read otherwise reaches its consumer as literal
/// text and fails as a bad DSN or a rejected credential rather than as a
/// configuration error. The caller prefixes its own field path; every message
/// names the prefix alone, because the rest of a spec is a path.
#[must_use]
pub fn spec_issue(spec: &str) -> Option<String> {
    let resolvable = || {
        RESOLVED_PREFIXES
            .iter()
            .map(|p| format!("`{p}`"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    match spec_prefix(spec) {
        Some((prefix, _)) if UNRESOLVED_PREFIXES.contains(&prefix) => Some(format!(
            "`{prefix}` is not a credential spec the resolver handles without the `secrets-aws` \
             feature; use {}",
            resolvable()
        )),
        Some((prefix, false)) => Some(format!(
            "a `{prefix}` spec is matched exactly; write it in lower case with no leading space"
        )),
        Some((prefix, true)) if VAULT_PREFIXES.contains(&prefix) && !names_a_key(spec, prefix) => {
            Some(format!(
                "a vault spec is `{prefix}<path>:<key>` and this one names no key"
            ))
        }
        _ => None,
    }
}

/// The spellings of the KV lookup, all of which split a `:key` off the path.
const VAULT_PREFIXES: [&str; 3] = ["vault:", "bao:", "openbao:"];

/// Whether a KV spec carries the `:key` the resolver splits on.
fn names_a_key(spec: &str, prefix: &str) -> bool {
    spec.trim_start()
        .strip_prefix(prefix)
        .is_some_and(|rest| rest.contains(':'))
}

/// A credential spec resolved on first use through `R`.
pub struct Secret<R> {
    spec: SensitiveString,
    resolved: OnceCell<SensitiveString>,
    resolver: PhantomData<fn() -> R>,
}

impl<R> fmt::Debug for Secret<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Secret")
            .field("resolved", &self.resolved.initialized())
            .finish_non_exhaustive()
    }
}

impl<R: ResolveSecret> Secret<R> {
    /// Wrap a spec.
    #[must_use]
    pub fn new(spec: SensitiveString) -> Self {
        Self {
            spec,
            resolved: OnceCell::new(),
            resolver: PhantomData,
        }
    }

    /// The plaintext value, resolving the spec the first time.
    ///
    /// # Errors
    ///
    /// Returns the resolver's error when the spec does not resolve.
    pub async fn value(&self) -> Result<&str> {
        let resolved = self
            .resolved
            .get_or_try_init(|| R::resolve(self.spec.expose()))
            .await?;
        Ok(resolved.expose())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::error::Error;

    static CALLS: AtomicUsize = AtomicUsize::new(0);

    /// Upper-cases a spec and refuses one starting with `missing:`.
    struct Upper;

    impl ResolveSecret for Upper {
        fn resolve(spec: &str) -> impl Future<Output = Result<SensitiveString>> + Send {
            CALLS.fetch_add(1, Ordering::SeqCst);
            std::future::ready(match spec.strip_prefix("missing:") {
                Some(name) => Err(Error::Credential(format!("`{name}` is not set"))),
                None => Ok(SensitiveString::from(spec.to_ascii_uppercase())),
            })
        }
    }

    #[tokio::test]
    async fn a_spec_resolves_once_and_the_plaintext_never_reaches_debug() {
        let secret: Secret<Upper> =
            Secret::new(SensitiveString::from("Driver=x;Server=y".to_owned()));
        let before = CALLS.load(Ordering::SeqCst);
        assert_eq!(secret.value().await.unwrap(), "DRIVER=X;SERVER=Y");
        assert_eq!(secret.value().await.unwrap(), "DRIVER=X;SERVER=Y");
        assert_eq!(
            CALLS.load(Ordering::SeqCst) - before,
            1,
            "the resolver runs once per secret"
        );
        let debug = format!("{secret:?}");
        assert!(
            !debug.contains("Server=y") && !debug.contains("SERVER=Y"),
            "neither the spec nor the value is printed: {debug}"
        );
    }

    /// Fixtures are written out rather than built from the prefix lists, so
    /// emptying either list fails this test instead of passing vacuously.
    #[test]
    fn a_spec_the_resolver_cannot_read_is_named_and_a_usable_one_is_left_alone() {
        for spec in [
            "vault:kv/data/team/db:uri",
            "bao:kv/data/team/db:uri",
            "openbao:kv/data/team/db:uri",
            "env:INVENTORY_DSN",
            "file:/run/secrets/dsn",
            "Driver=PostgreSQL Unicode;Server=db",
            "mongodb://user:pw@host:27017/?authSource=admin",
            "http://user:pw@host:8123/db",
        ] {
            assert!(spec_issue(spec).is_none(), "{spec}");
        }

        // The one prefix the resolver knows and this build has no provider for.
        for spec in ["aws:prod/team/db", "aws:prod/team/db:uri"] {
            let issue = spec_issue(spec).unwrap_or_else(|| panic!("{spec} is refused"));
            assert!(issue.contains("is not a credential spec"), "{issue}");
            assert!(
                issue.contains("`secrets-aws`"),
                "the refusal names the feature that would serve it: {issue}"
            );
            assert!(
                issue.contains("`vault:`") && issue.contains("`file:`"),
                "the refusal names what does work: {issue}"
            );
            assert!(
                !issue.contains("prod") && !issue.contains("team"),
                "the refusal names the prefix alone, never the path: {issue}"
            );
        }

        // The resolver matches exactly, so a near miss is refused rather than
        // silently treated as a literal.
        for spec in ["Vault:kv/data/x:k", " env:VAR", "ENV:VAR"] {
            let issue = spec_issue(spec).unwrap_or_else(|| panic!("{spec} is refused"));
            assert!(issue.contains("matched exactly"), "{spec}: {issue}");
        }

        // A KV spec the resolver cannot split names no key, whichever of the
        // three spellings it is written in.
        for spec in [
            "vault:kv/data/team/db",
            "bao:kv/data/team/db",
            "openbao:kv/data/team/db",
        ] {
            let no_key = spec_issue(spec).unwrap_or_else(|| panic!("{spec} is refused"));
            assert!(no_key.contains("names no key"), "{spec}: {no_key}");
        }
    }

    #[test]
    fn only_a_prefixed_spec_is_a_reference() {
        assert!(is_reference("vault:kv/data/x:k"));
        assert!(is_reference("bao:kv/data/x:k"));
        assert!(is_reference("openbao:kv/data/x:k"));
        assert!(is_reference("env:VAR"));
        assert!(is_reference("file:/run/secrets/token"));
        // A prefix with no provider in this build is a reference too: the text
        // is a path, so it must not be inspected as a literal credential.
        assert!(is_reference("aws:prod/team/db"));
        assert!(!is_reference("Driver=PostgreSQL Unicode;Server=db"));
        assert!(
            !is_reference("mongodb://user:pw@host/"),
            "a DSN carrying colons is not a spec"
        );
    }

    #[tokio::test]
    async fn a_spec_that_does_not_resolve_is_the_resolvers_error_every_time() {
        let secret: Secret<Upper> = Secret::new(SensitiveString::from("missing:VAR".to_owned()));
        let err = secret.value().await.unwrap_err();
        assert!(matches!(err, Error::Credential(_)), "{err:?}");
        assert!(err.to_string().contains("`VAR` is not set"), "{err}");
        assert!(
            secret.value().await.is_err(),
            "a failed resolution is not cached as a value"
        );
    }
}
