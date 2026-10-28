// Project:   dfe-fetcher
// File:      crates/rest/src/hooks/event_time.rs
// Purpose:   The event-time row builder: a vendor timestamp field copied to the header's `timestamp`
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The common header reads an event's time from `timestamp` or `@timestamp`,
//! and Microsoft Graph names it `activityDateTime` or `createdDateTime`. A
//! record with neither lands with its load time as its event time.
//!
//! The row is copied to `timestamp` from the field the unit's `time_field`
//! var names, and keeps every other key, so schemas that read the flat record
//! still find their fields. The value is carried as the provider wrote it.
//! A row that is not a JSON object, has no usable time field or already has
//! a `timestamp` lands unchanged and byte for byte: a failing row aborts the
//! unit before its checkpoint moves, and a missing time is not worth that.

use bytes::Bytes;
use serde_json::Value;

use dfe_fetcher_core::error::{Error, Result};

use crate::profile::template::TemplateCtx;

/// The header key the event time is stamped under.
const HEADER_TIMESTAMP: &str = "timestamp";

/// The row, with the unit's time field also under `timestamp`.
pub(super) fn expand(row: &[u8], ctx: &TemplateCtx) -> Result<Vec<Bytes>> {
    let Some(field) = ctx
        .get("vars")
        .and_then(|vars| vars.get("time_field"))
        .and_then(Value::as_str)
    else {
        return Ok(vec![Bytes::copy_from_slice(row)]);
    };
    if row.trim_ascii_start().first() != Some(&b'{') {
        return Ok(vec![Bytes::copy_from_slice(row)]);
    }
    let Ok(Value::Object(mut record)) = serde_json::from_slice::<Value>(row) else {
        return Ok(vec![Bytes::copy_from_slice(row)]);
    };
    let time = match record.get(field) {
        Some(Value::String(time)) if !time.is_empty() => time.clone(),
        _ => return Ok(vec![Bytes::copy_from_slice(row)]),
    };
    if record.contains_key(HEADER_TIMESTAMP) {
        return Ok(vec![Bytes::copy_from_slice(row)]);
    }
    record.insert(HEADER_TIMESTAMP.to_owned(), Value::String(time));
    serde_json::to_vec(&record)
        .map(|stamped| vec![Bytes::from(stamped)])
        .map_err(|e| Error::Decode(format!("event_time: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ctx(time_field: Option<&str>) -> TemplateCtx {
        let mut ctx = TemplateCtx::new();
        let vars = time_field.map_or_else(|| json!({}), |field| json!({ "time_field": field }));
        ctx.set("vars", vars);
        ctx
    }

    fn stamped(row: &Value, time_field: Option<&str>) -> Value {
        let out = expand(row.to_string().as_bytes(), &ctx(time_field)).expect("expands");
        assert_eq!(out.len(), 1);
        serde_json::from_slice(&out[0]).expect("a JSON row")
    }

    /// A Graph `directoryAudit`, as the Graph reference shows it.
    fn directory_audit() -> Value {
        json!({
            "id": "Directory_da159bfb-54fa-4092-8a38-6e1fa7870e30_ABCDE_123456",
            "category": "GroupManagement",
            "result": "success",
            "activityDisplayName": "Add member to group",
            "activityDateTime": "2018-01-09T21:20:02.7215374Z",
            "initiatedBy": { "user": { "userPrincipalName": "bob@wingtiptoysonline.com" }, "app": null }
        })
    }

    #[test]
    fn the_time_field_is_copied_and_the_flat_record_kept() {
        let record = directory_audit();
        let out = stamped(&record, Some("activityDateTime"));
        assert_eq!(out["timestamp"], "2018-01-09T21:20:02.7215374Z");
        for (key, value) in record.as_object().expect("an object") {
            assert_eq!(&out[key], value, "{key} must survive the stamp");
        }
    }

    #[test]
    fn a_row_without_a_usable_time_lands_as_it_came() {
        let cases = [
            ("no time_field var", directory_audit(), None),
            ("field absent", json!({"id": "x"}), Some("activityDateTime")),
            (
                "field null",
                json!({"activityDateTime": null}),
                Some("activityDateTime"),
            ),
            (
                "field empty",
                json!({"activityDateTime": ""}),
                Some("activityDateTime"),
            ),
            (
                "field a number",
                json!({"activityDateTime": 1_514_764_800}),
                Some("activityDateTime"),
            ),
            (
                "timestamp already set",
                json!({"activityDateTime": "2018-01-09T21:20:02Z", "timestamp": "kept"}),
                Some("activityDateTime"),
            ),
        ];
        for (name, record, time_field) in cases {
            let text = record.to_string();
            let out = expand(text.as_bytes(), &ctx(time_field)).expect("expands");
            assert_eq!(out, vec![Bytes::from(text)], "{name}");
        }
    }

    #[test]
    fn a_row_that_is_not_an_object_lands_as_it_came() {
        let cases: [(&str, &[u8]); 4] = [
            ("array", b"[1, 2]"),
            ("scalar", b"\"text\""),
            ("not json", b"{not json"),
            ("empty", b""),
        ];
        for (name, row) in cases {
            let out = expand(row, &ctx(Some("activityDateTime"))).expect("expands");
            assert_eq!(out, vec![Bytes::copy_from_slice(row)], "{name}");
        }
    }
}
