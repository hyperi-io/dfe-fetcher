// Project:   dfe-fetcher
// File:      crates/fetcher/src/credential.rs
// Purpose:   Credential re-exports
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Credential resolution.
//!
//! `resolve` / `resolve_optional` / `CredentialError` are re-exported from
//! [`scalo::secrets`] for the config cascade's `env:` / `vault:` specs; the
//! HTTP client and token minting live in the REST crate.

pub use scalo::secrets::{CredentialError, resolve, resolve_optional};
