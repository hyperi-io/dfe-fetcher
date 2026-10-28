// Project:   dfe-fetcher
// File:      crates/rest/src/hooks/cloudtrail_event.rs
// Purpose:   The CloudTrail row builder: a LookupEvents record into the event it carries
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A CloudTrail `LookupEvents` record wraps the event it describes as a JSON
//! string under `CloudTrailEvent`, beside a handful of lookup fields
//! (`EventId`, `EventName`, `EventTime`, `Username`, `Resources`, ...). The
//! row is the event itself, the shape CloudTrail delivers to S3 and the one
//! an Elastic `aws.cloudtrail` pipeline reads (`eventName`, `eventTime`,
//! `userIdentity` at the top). The string's bytes are emitted as they came,
//! so key order and number formatting survive.
//!
//! A record whose `CloudTrailEvent` is missing, is not a string holding a
//! JSON object, or does not parse is kept rather than lost, and rather than
//! failing the page (a failing row aborts the unit before its checkpoint
//! moves, so one bad record would stall the window for good): the lookup
//! record lands unchanged with the reason under `_dfe_fetcher_parse_error`.

use bytes::Bytes;
use serde::Deserialize;
use serde_json::Value;

use dfe_fetcher_core::error::{Error, Result};

/// The one field of a `LookupEvents` record the builder reads.
#[derive(Deserialize)]
struct Lookup {
    #[serde(rename = "CloudTrailEvent")]
    event: Option<Value>,
}

/// The event one lookup record carries.
pub(super) fn expand(row: &[u8]) -> Result<Vec<Bytes>> {
    // serde reads a struct from a JSON array too, so the object check is ours.
    if row.trim_ascii_start().first() != Some(&b'{') {
        return Err(Error::Decode(
            "cloudtrail_event: the row is not a JSON object".into(),
        ));
    }
    let lookup: Lookup = serde_json::from_slice(row).map_err(|e| {
        Error::Decode(format!(
            "cloudtrail_event: the row is not a JSON object: {e}"
        ))
    })?;
    match event_of(lookup.event) {
        Ok(event) => Ok(vec![event]),
        Err(reason) => keep(row, &reason).map(|kept| vec![kept]),
    }
}

/// The event's bytes, or why the record does not carry one.
fn event_of(field: Option<Value>) -> std::result::Result<Bytes, String> {
    match field {
        Some(Value::String(text)) => match serde_json::from_str::<Value>(&text) {
            Ok(Value::Object(_)) => Ok(Bytes::from(text)),
            Ok(_) => Err("`CloudTrailEvent` is not a JSON object".to_owned()),
            Err(e) => Err(format!("`CloudTrailEvent` is not valid JSON: {e}")),
        },
        Some(Value::Object(event)) => serde_json::to_vec(&event)
            .map(Bytes::from)
            .map_err(|e| format!("`CloudTrailEvent` cannot be re-encoded: {e}")),
        Some(_) => Err("`CloudTrailEvent` is neither a string nor an object".to_owned()),
        None => Err("`CloudTrailEvent` is missing".to_owned()),
    }
}

/// The lookup record as it came, with the reason beside it.
fn keep(row: &[u8], reason: &str) -> Result<Bytes> {
    let mut record: Value =
        serde_json::from_slice(row).map_err(|e| Error::Decode(format!("cloudtrail_event: {e}")))?;
    if let Some(object) = record.as_object_mut() {
        object.insert(
            "_dfe_fetcher_parse_error".into(),
            Value::String(format!("cloudtrail_event: {reason}")),
        );
    }
    serde_json::to_vec(&record)
        .map(Bytes::from)
        .map_err(|e| Error::Decode(format!("cloudtrail_event: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A LookupEvents record shaped like the API's, with documentation
    /// addresses and a made-up account in place of any real one.
    fn lookup_record(event: &Value) -> Value {
        json!({
            "AccessKeyId": "AKIAIOSFODNN7EXAMPLE",
            "CloudTrailEvent": event.to_string(),
            "EventId": "11111111-2222-3333-4444-555555555555",
            "EventName": "ConsoleLogin",
            "EventSource": "signin.amazonaws.com",
            "EventTime": 1_771_459_200.0,
            "ReadOnly": "false",
            "Resources": [],
            "Username": "alice"
        })
    }

    fn event() -> Value {
        json!({
            "eventVersion": "1.08",
            "eventTime": "2026-02-19T00:00:00Z",
            "eventSource": "signin.amazonaws.com",
            "eventName": "ConsoleLogin",
            "awsRegion": "ap-southeast-2",
            "sourceIPAddress": "203.0.113.7",
            "userIdentity": {
                "type": "IAMUser",
                "arn": "arn:aws:iam::123456789012:user/alice",
                "accountId": "123456789012"
            },
            "eventID": "11111111-2222-3333-4444-555555555555",
            "recipientAccountId": "123456789012"
        })
    }

    fn run(row: &Value) -> Value {
        let rows = expand(&serde_json::to_vec(row).unwrap()).unwrap();
        assert_eq!(rows.len(), 1, "one lookup record is one row");
        serde_json::from_slice(&rows[0]).unwrap()
    }

    #[test]
    fn the_event_is_the_row() {
        let landed = run(&lookup_record(&event()));
        assert_eq!(landed, event(), "the bare event, none of the lookup fields");
    }

    #[test]
    fn the_events_bytes_are_not_re_encoded() {
        let text = r#"{"eventName":"ConsoleLogin",  "eventTime":"2026-02-19T00:00:00Z","n":1.0}"#;
        let row = json!({"EventId": "e", "CloudTrailEvent": text});
        let rows = expand(&serde_json::to_vec(&row).unwrap()).unwrap();
        assert_eq!(&rows[0][..], text.as_bytes());
    }

    #[test]
    fn a_record_without_a_usable_event_is_kept_with_the_reason() {
        let cases: [(&str, Value, &str); 6] = [
            (
                "not JSON",
                json!({"EventId": "e", "CloudTrailEvent": "{\"eventName\": "}),
                "is not valid JSON",
            ),
            (
                "JSON but not an object",
                json!({"EventId": "e", "CloudTrailEvent": "[1, 2]"}),
                "is not a JSON object",
            ),
            (
                "plain text",
                json!({"EventId": "e", "CloudTrailEvent": "denied"}),
                "is not valid JSON",
            ),
            (
                "a number",
                json!({"EventId": "e", "CloudTrailEvent": 7}),
                "neither a string nor an object",
            ),
            (
                "null",
                json!({"EventId": "e", "CloudTrailEvent": null}),
                "is missing",
            ),
            ("absent", json!({"EventId": "e"}), "is missing"),
        ];
        for (name, row, reason) in cases {
            let landed = run(&row);
            assert_eq!(landed["EventId"], "e", "{name}: the lookup record survives");
            let error = landed["_dfe_fetcher_parse_error"].as_str().unwrap();
            assert!(error.contains(reason), "{name}: {error}");
        }
    }

    #[test]
    fn an_already_parsed_event_passes_as_the_event() {
        let row = json!({"EventId": "e", "CloudTrailEvent": event()});
        assert_eq!(run(&row), event());
    }

    #[test]
    fn a_row_that_is_not_an_object_fails_the_page() {
        for bad in [&b"[1]"[..], b"\"text\"", b"not json"] {
            assert!(expand(bad).is_err(), "{}", String::from_utf8_lossy(bad));
        }
    }
}
