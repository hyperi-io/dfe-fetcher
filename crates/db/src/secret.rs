// Project:   dfe-fetcher
// File:      crates/db/src/secret.rs
// Purpose:   The connection string as a once-resolved secret
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The connection string as a secret.
//!
//! The spec (`vault:<mount>/data/<path>:<key>`, `env:VAR`, or a literal) is
//! resolved through scalo's secret resolver the first time a store connects
//! and cached for the life of the shape by the core's once-resolved cell.

use scalo::SensitiveString;

use dfe_fetcher_core::error::{Error, Result};
use dfe_fetcher_core::secret::ResolveSecret;

/// scalo's secret resolver as the core cell's resolver.
#[derive(Debug)]
pub struct ScaloSecrets;

impl ResolveSecret for ScaloSecrets {
    async fn resolve(spec: &str) -> Result<SensitiveString> {
        scalo::secrets::resolve(spec)
            .await
            .map(SensitiveString::from)
            .map_err(|e| Error::Credential(e.to_string()))
    }
}

/// A credential spec resolved on first use.
pub type Secret = dfe_fetcher_core::secret::Secret<ScaloSecrets>;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_literal_resolves_to_itself_and_an_unset_env_reference_is_a_credential_error() {
        let secret = Secret::new(SensitiveString::from("Driver=x;Server=y".to_owned()));
        assert_eq!(secret.value().await.unwrap(), "Driver=x;Server=y");
        let unset = Secret::new(SensitiveString::from(
            "env:DFE_FETCHER_DB_TEST_UNSET_VARIABLE".to_owned(),
        ));
        assert!(matches!(
            unset.value().await.unwrap_err(),
            Error::Credential(_)
        ));
    }
}
