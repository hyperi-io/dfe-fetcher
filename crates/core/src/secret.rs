// Project:   dfe-fetcher
// File:      crates/core/src/secret.rs
// Purpose:   A credential spec resolved once, on first use, through the resolver an I/O crate supplies
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The once-resolved secret.
//!
//! A credential spec (`vault:<mount>/data/<path>:<key>`, `env:VAR`, or a
//! literal) is resolved the first time it is read and held for the life of
//! its owner, so a vault round trip happens once per process, not once per
//! tick. The cell is here; the resolver is the I/O crate's, named by the type
//! parameter, so core never reaches a vault or the environment itself.

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
