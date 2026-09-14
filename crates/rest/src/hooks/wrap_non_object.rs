// Project:   dfe-fetcher
// File:      crates/rest/src/hooks/wrap_non_object.rs
// Purpose:   The row builder that makes every framed row a JSON object
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A row that is not a JSON object is wrapped as one so the fields added
//! to it (an object envelope) have somewhere to go and a consumer sees the
//! bad data rather than losing it: a JSON scalar or array lands under
//! `payload`, text that is not JSON under `_dfe_fetcher_raw_line` with the
//! parse error beside it. An object passes through untouched.

use bytes::Bytes;
use serde_json::Value;

use dfe_fetcher_core::error::{Error, Result};

/// The row as an object.
pub(super) fn expand(row: &[u8]) -> Result<Vec<Bytes>> {
    let wrapped = match serde_json::from_slice::<Value>(row) {
        Ok(Value::Object(_)) => return Ok(vec![Bytes::copy_from_slice(row)]),
        Ok(other) => serde_json::json!({ "payload": other }),
        Err(e) => serde_json::json!({
            "_dfe_fetcher_raw_line": String::from_utf8_lossy(row),
            "_dfe_fetcher_parse_error": e.to_string(),
        }),
    };
    serde_json::to_vec(&wrapped)
        .map(|bytes| vec![Bytes::from(bytes)])
        .map_err(|e| Error::Decode(format!("wrap_non_object: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn an_object_passes_and_anything_else_is_wrapped() {
        let same = expand(br#"{"a": 1}"#).unwrap();
        assert_eq!(&same[0][..], br#"{"a": 1}"#, "the bytes as they came");
        let scalar: Value = serde_json::from_slice(&expand(b"[1, 2]").unwrap()[0]).unwrap();
        assert_eq!(scalar, json!({"payload": [1, 2]}));
        let text: Value = serde_json::from_slice(&expand(b"NOT JSON").unwrap()[0]).unwrap();
        assert_eq!(text["_dfe_fetcher_raw_line"], "NOT JSON");
        assert!(text["_dfe_fetcher_parse_error"].is_string());
    }
}
