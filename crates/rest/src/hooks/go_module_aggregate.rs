// Project:   dfe-fetcher
// File:      crates/rest/src/hooks/go_module_aggregate.rs
// Purpose:   The Go module fold: N version `.info` documents into one row per module
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! One module's `.info` documents, in the order the proxy listed their
//! versions, fold into the one row the Go module source emits per module:
//! `versions` (the list, in order) and `version_info` (each document keyed
//! by its `Version`). Downstream tooling diffs the rows between ticks to
//! spot a publication or a commit-hash drift, so the fold keeps every
//! document whole.

use bytes::Bytes;
use serde_json::Value;

use dfe_fetcher_core::error::{Error, Result};

use crate::profile::template::TemplateCtx;

/// The module row, or `None` when the key yielded no document.
pub(super) fn fold(rows: &[Bytes], _ctx: &TemplateCtx) -> Result<Option<Bytes>> {
    if rows.is_empty() {
        return Ok(None);
    }
    let mut versions = Vec::with_capacity(rows.len());
    let mut infos = serde_json::Map::with_capacity(rows.len());
    for row in rows {
        let info: Value = serde_json::from_slice(row).map_err(|e| {
            Error::Decode(format!(
                "go_module_aggregate: a version info is not JSON: {e}"
            ))
        })?;
        let version = info
            .get("Version")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                Error::Decode("go_module_aggregate: a version info carries no `Version`".into())
            })?
            .to_owned();
        versions.push(Value::String(version.clone()));
        infos.insert(version, info);
    }
    let row = serde_json::json!({ "versions": versions, "version_info": infos });
    serde_json::to_vec(&row)
        .map(|bytes| Some(Bytes::from(bytes)))
        .map_err(|e| Error::Decode(format!("go_module_aggregate: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_documents_fold_into_the_version_list_and_the_info_map_in_list_order() {
        let docs = [
            json!({"Version": "v0.3.0", "Time": "2026-01-01T00:00:00Z"}),
            json!({"Version": "v0.4.0", "Time": "2026-02-01T00:00:00Z", "Origin": {"VCS": "git"}}),
        ];
        let rows: Vec<Bytes> = docs
            .iter()
            .map(|d| Bytes::from(serde_json::to_vec(d).unwrap()))
            .collect();
        let folded = fold(&rows, &TemplateCtx::new()).unwrap().expect("a row");
        let value: Value = serde_json::from_slice(&folded).unwrap();
        assert_eq!(value["versions"], json!(["v0.3.0", "v0.4.0"]));
        assert_eq!(value["version_info"]["v0.4.0"], docs[1]);
        assert_eq!(value["version_info"]["v0.3.0"], docs[0]);
        assert!(
            fold(&[], &TemplateCtx::new()).unwrap().is_none(),
            "no documents, no row"
        );
        assert!(fold(&[Bytes::from_static(b"{}")], &TemplateCtx::new()).is_err());
        assert!(fold(&[Bytes::from_static(b"nope")], &TemplateCtx::new()).is_err());
    }
}
