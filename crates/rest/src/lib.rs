// Project:   dfe-fetcher
// File:      crates/rest/src/lib.rs
// Purpose:   REST shapes of the source framework
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! REST shapes for the dfe-fetcher source framework.
//!
//! A REST source is a declarative [`profile::RestProfile`] (the shape of an
//! API) bound to a [`profile::RestInstance`] (one deployment's identity) into
//! a [`shape::RestShape`], which implements `RowSource` for the driver. The
//! four axes are runtime-selected sum types: [`auth::AuthMode`],
//! [`page::Pager`], [`decode::Decoder`], and the checkpoint kind carried by
//! core, with [`hooks::RowBuilder`] as the last escape for a row shape no
//! decoder expresses. Every request goes through
//! [`request::RequestExecutor`], the one place a request is built, signed,
//! sent, retried and measured.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::missing_errors_doc)]
#![warn(rustdoc::broken_intra_doc_links)]
#![cfg_attr(test, allow(clippy::unwrap_used))]
#![cfg_attr(test, allow(clippy::expect_used))]

pub mod auth;
pub mod decode;
pub mod hooks;
pub mod page;
pub mod profile;
pub mod request;
pub mod shape;

pub use profile::{RestInstance, RestProfile};
pub use request::{HttpClient, http_client};
pub use shape::RestShape;
