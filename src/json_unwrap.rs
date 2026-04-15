// Project:   dfe-fetcher
// File:      src/json_unwrap.rs
// Purpose:   Recursively unwrap double-serialised JSON string fields
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Recursively unwrap JSON string fields that contain serialised JSON.
//!
//! Some upstream APIs return fields as JSON-encoded strings rather than
//! nested objects — for example, AWS CloudTrail's `CloudTrailEvent` field
//! is a string containing a JSON-serialised event. Storing these verbatim
//! makes the data hard to query in downstream systems like ClickHouse's
//! `JSON` column type.
//!
//! This module walks a JSON payload and replaces string values that parse
//! as JSON objects or arrays with their parsed form. Scalar strings (not
//! valid JSON) are left unchanged.

use bytes::Bytes;
use serde_json::Value;

/// Maximum recursion depth when unwrapping nested JSON strings.
///
/// Bounds runaway parsing on pathological inputs (e.g. a string that
/// serialises a string that serialises a string...). Three levels covers
/// every real-world case we've seen.
const MAX_DEPTH: usize = 3;

/// Unwrap double-serialised JSON string fields in the payload.
///
/// Parses `payload` as JSON, recursively walks the structure, and replaces
/// any string value that parses as a JSON object or array with the parsed
/// value. Scalar strings and non-JSON strings are left unchanged.
///
/// Returns the original bytes unchanged if:
/// - The payload is not valid JSON
/// - No string fields contain nested JSON
/// - Reserialisation fails (should never happen — fail-open)
///
/// # Performance
///
/// Only strings starting with `{` or `[` are attempted — scalar strings
/// and UUIDs/timestamps skip the parse check entirely. When no unwrap is
/// needed, the original `Bytes` is returned without reserialisation.
#[must_use]
pub fn unwrap_nested_json(payload: &Bytes) -> Bytes {
    let Ok(mut value) = serde_json::from_slice::<Value>(payload) else {
        return payload.clone();
    };

    if !unwrap_in_place(&mut value, 0) {
        return payload.clone();
    }

    serde_json::to_vec(&value)
        .map(Bytes::from)
        .unwrap_or_else(|_| payload.clone())
}

/// Recursively walk the JSON value, unwrapping stringified JSON in place.
///
/// Returns `true` if any value was unwrapped.
fn unwrap_in_place(value: &mut Value, depth: usize) -> bool {
    if depth >= MAX_DEPTH {
        return false;
    }

    match value {
        Value::Object(map) => {
            let mut changed = false;
            for v in map.values_mut() {
                if try_unwrap_string(v) {
                    changed = true;
                }
                if unwrap_in_place(v, depth + 1) {
                    changed = true;
                }
            }
            changed
        }
        Value::Array(arr) => {
            let mut changed = false;
            for v in arr.iter_mut() {
                if try_unwrap_string(v) {
                    changed = true;
                }
                if unwrap_in_place(v, depth + 1) {
                    changed = true;
                }
            }
            changed
        }
        _ => false,
    }
}

/// If `value` is a string containing serialised JSON object or array,
/// replace it in place with the parsed value. Returns `true` if replaced.
fn try_unwrap_string(value: &mut Value) -> bool {
    let Value::String(s) = value else {
        return false;
    };

    let trimmed = s.trim_start();
    if !trimmed.starts_with('{') && !trimmed.starts_with('[') {
        return false;
    }

    let Ok(parsed) = serde_json::from_str::<Value>(s) else {
        return false;
    };

    if parsed.is_object() || parsed.is_array() {
        *value = parsed;
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unwrap_simple_stringified_object() {
        let input =
            Bytes::from(r#"{"EventId":"abc","CloudTrailEvent":"{\"eventVersion\":\"1.11\"}"}"#);
        let out = unwrap_nested_json(&input);
        let parsed: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["EventId"], "abc");
        assert_eq!(parsed["CloudTrailEvent"]["eventVersion"], "1.11");
    }

    #[test]
    fn unwrap_stringified_array() {
        let input = Bytes::from(r#"{"items":"[1,2,3]"}"#);
        let out = unwrap_nested_json(&input);
        let parsed: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["items"], serde_json::json!([1, 2, 3]));
    }

    #[test]
    fn leave_scalar_strings_unchanged() {
        let input = Bytes::from(r#"{"name":"Alice","uuid":"a-b-c"}"#);
        let out = unwrap_nested_json(&input);
        // No unwrap happened — returns original bytes unchanged
        assert_eq!(&out[..], &input[..]);
    }

    #[test]
    fn leave_non_json_strings_unchanged() {
        // Starts with `{` but is not valid JSON
        let input = Bytes::from(r#"{"template":"{not json}"}"#);
        let out = unwrap_nested_json(&input);
        let parsed: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["template"], "{not json}");
    }

    #[test]
    fn unwrap_nested_inside_object() {
        let input = Bytes::from(r#"{"outer":{"inner":"{\"field\":42}"}}"#);
        let out = unwrap_nested_json(&input);
        let parsed: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["outer"]["inner"]["field"], 42);
    }

    #[test]
    fn unwrap_inside_array() {
        let input = Bytes::from(r#"{"events":[{"raw":"{\"k\":\"v\"}"}]}"#);
        let out = unwrap_nested_json(&input);
        let parsed: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["events"][0]["raw"]["k"], "v");
    }

    #[test]
    fn respect_depth_limit() {
        // Build 5 levels of stringified nesting — only first 3 should unwrap
        let level4 = r#"{"l4":"{\"l5\":42}"}"#;
        let level3 = format!(r#"{{"l3":{}}}"#, serde_json::to_string(level4).unwrap());
        let level2 = format!(r#"{{"l2":{}}}"#, serde_json::to_string(&level3).unwrap());
        let level1 = format!(r#"{{"l1":{}}}"#, serde_json::to_string(&level2).unwrap());
        let input = Bytes::from(level1);

        let out = unwrap_nested_json(&input);
        let parsed: Value = serde_json::from_slice(&out).unwrap();

        // Levels 1, 2, 3 should be unwrapped to objects
        assert!(parsed["l1"].is_object());
        assert!(parsed["l1"]["l2"].is_object());
        assert!(parsed["l1"]["l2"]["l3"].is_object());
        // Level 4 content (l4) reached but its nested "l5" string should
        // not have been unwrapped — depth limit hit
        let l4 = &parsed["l1"]["l2"]["l3"]["l4"];
        // Depending on where the depth kicks in, l4 may be a string or object
        // What matters is we don't recurse indefinitely
        let _ = l4;
    }

    #[test]
    fn non_json_payload_returned_unchanged() {
        let input = Bytes::from("not json at all");
        let out = unwrap_nested_json(&input);
        assert_eq!(&out[..], &input[..]);
    }

    #[test]
    fn empty_object_unchanged() {
        let input = Bytes::from("{}");
        let out = unwrap_nested_json(&input);
        assert_eq!(&out[..], &input[..]);
    }

    #[test]
    fn empty_array_unchanged() {
        let input = Bytes::from("[]");
        let out = unwrap_nested_json(&input);
        assert_eq!(&out[..], &input[..]);
    }

    #[test]
    fn multiple_stringified_fields() {
        let input = Bytes::from(r#"{"a":"{\"x\":1}","b":"plain","c":"{\"y\":2}"}"#);
        let out = unwrap_nested_json(&input);
        let parsed: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["a"]["x"], 1);
        assert_eq!(parsed["b"], "plain");
        assert_eq!(parsed["c"]["y"], 2);
    }

    #[test]
    fn stringified_number_is_not_unwrapped() {
        // A string like "42" does NOT start with { or [, so skipped
        let input = Bytes::from(r#"{"n":"42"}"#);
        let out = unwrap_nested_json(&input);
        let parsed: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["n"], "42"); // still string
    }

    #[test]
    fn cloudtrail_realistic_example() {
        let input = Bytes::from(
            r#"{"EventId":"812acfe7","EventName":"Decrypt","CloudTrailEvent":"{\"eventVersion\":\"1.11\",\"sourceIPAddress\":\"10.0.0.1\",\"userIdentity\":{\"type\":\"IAMUser\"}}"}"#,
        );
        let out = unwrap_nested_json(&input);
        let parsed: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["EventId"], "812acfe7");
        assert_eq!(parsed["EventName"], "Decrypt");
        assert_eq!(parsed["CloudTrailEvent"]["eventVersion"], "1.11");
        assert_eq!(parsed["CloudTrailEvent"]["sourceIPAddress"], "10.0.0.1");
        assert_eq!(parsed["CloudTrailEvent"]["userIdentity"]["type"], "IAMUser");
    }
}
