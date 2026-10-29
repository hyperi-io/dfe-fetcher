// Project:   dfe-fetcher
// File:      crates/rest/src/hooks/cloudwatch_logs.rs
// Purpose:   The CloudWatch Logs row builder: the log group and region of the call stamped on each event
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A `FilterLogEvents` event carries `eventId`, `logStreamName`, `timestamp`,
//! `ingestionTime` and `message`. The log group and the region belong to the
//! call, so a row read without them cannot say which group it came from, and
//! a deployment polling several groups from one source cannot tell them apart.
//!
//! The row gets `logGroupName` from the unit's `log_group_name` var and
//! `awsRegion` from its `region` var, the keys the meta schema and CloudTrail
//! already use. A key the event already carries is left alone, and a row that
//! is not a JSON object lands unchanged.

use bytes::Bytes;
use serde_json::Value;

use dfe_fetcher_core::error::{Error, Result};

use crate::profile::template::TemplateCtx;

/// (row key, unit var) pairs stamped onto each event.
const STAMPS: [(&str, &str); 2] = [("logGroupName", "log_group_name"), ("awsRegion", "region")];

/// The event with the call's log group and region on it.
pub(super) fn expand(row: &[u8], ctx: &TemplateCtx) -> Result<Vec<Bytes>> {
    let unchanged = || Ok(vec![Bytes::copy_from_slice(row)]);
    if row.trim_ascii_start().first() != Some(&b'{') {
        return unchanged();
    }
    let Ok(Value::Object(mut event)) = serde_json::from_slice::<Value>(row) else {
        return unchanged();
    };
    let mut stamped = false;
    for (key, var) in STAMPS {
        let value = ctx
            .get("vars")
            .and_then(|vars| vars.get(var))
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty());
        if let Some(value) = value
            && !event.contains_key(key)
        {
            event.insert(key.to_owned(), Value::String(value.to_owned()));
            stamped = true;
        }
    }
    if !stamped {
        return unchanged();
    }
    serde_json::to_vec(&event)
        .map(|bytes| vec![Bytes::from(bytes)])
        .map_err(|e| Error::Decode(format!("cloudwatch_logs: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ctx(vars: Value) -> TemplateCtx {
        let mut ctx = TemplateCtx::new();
        ctx.set("vars", vars);
        ctx
    }

    fn event() -> Value {
        json!({
            "eventId": "ev-1",
            "logStreamName": "stream-1",
            "timestamp": 1_700_000_000_000_u64,
            "ingestionTime": 1_700_000_000_500_u64,
            "message": "INFO hello"
        })
    }

    fn expanded(row: &str, vars: Value) -> Vec<u8> {
        let out = expand(row.as_bytes(), &ctx(vars)).expect("expands");
        assert_eq!(out.len(), 1);
        out[0].to_vec()
    }

    #[test]
    fn the_log_group_and_region_are_stamped_and_the_event_kept() {
        let vars = json!({"log_group_name": "/aws/lambda/test", "region": "ap-southeast-2"});
        let out: Value =
            serde_json::from_slice(&expanded(&event().to_string(), vars)).expect("a JSON row");
        assert_eq!(out["logGroupName"], "/aws/lambda/test");
        assert_eq!(out["awsRegion"], "ap-southeast-2");
        for (key, value) in event().as_object().expect("an object") {
            assert_eq!(&out[key], value, "{key} must survive the stamp");
        }
    }

    #[test]
    fn a_row_with_nothing_to_stamp_lands_byte_for_byte() {
        let carried = json!({"eventId": "x", "logGroupName": "theirs", "awsRegion": "theirs"});
        let cases = [
            ("not an object", "[1, 2]".to_owned(), json!({"region": "r"})),
            ("not json", "{not json".to_owned(), json!({"region": "r"})),
            ("no vars", event().to_string(), json!({})),
            ("empty vars", event().to_string(), json!({"region": ""})),
            (
                "keys already on the event",
                carried.to_string(),
                json!({"log_group_name": "mine", "region": "mine"}),
            ),
        ];
        for (name, row, vars) in cases {
            assert_eq!(expanded(&row, vars), row.as_bytes(), "{name}");
        }
    }

    #[test]
    fn only_the_missing_key_is_stamped() {
        let row = json!({"eventId": "x", "awsRegion": "theirs"}).to_string();
        let vars = json!({"log_group_name": "g", "region": "mine"});
        let out: Value = serde_json::from_slice(&expanded(&row, vars)).expect("a JSON row");
        assert_eq!(out["logGroupName"], "g");
        assert_eq!(out["awsRegion"], "theirs");
    }
}
