// Project:   dfe-fetcher
// File:      crates/rest/src/hooks/pubsub_message.rs
// Purpose:   The Pub/Sub row builder: a received message into the record it carries
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A `receivedMessages[]` entry of a Pub/Sub pull becomes the record its
//! base64 `message.data` holds: JSON as itself, anything else as
//! `{"data": <text>}`, a non-object as `{"payload": <value>}`. The message
//! envelope rides under `_dfe_fetcher_pubsub` (the subscription the unit's
//! vars name, the message id, publish time, attributes and ordering key) so
//! a consumer can correlate back to the source message.

use base64::Engine as _;
use bytes::Bytes;
use serde_json::Value;

use dfe_fetcher_core::error::{Error, Result};

use crate::profile::template::TemplateCtx;

/// The record one received message carries.
pub(super) fn expand(row: &[u8], ctx: &TemplateCtx) -> Result<Vec<Bytes>> {
    let received: Value = serde_json::from_slice(row)
        .map_err(|e| Error::Decode(format!("pubsub_message: the row is not JSON: {e}")))?;
    let message = received.get("message").cloned().unwrap_or(Value::Null);
    let data = message
        .get("data")
        .and_then(Value::as_str)
        .map(|b64| base64::engine::general_purpose::STANDARD.decode(b64))
        .transpose()
        .map_err(|e| Error::Decode(format!("pubsub_message: `message.data` is not base64: {e}")))?
        .unwrap_or_default();
    let mut payload: Value = serde_json::from_slice(&data)
        .unwrap_or_else(|_| serde_json::json!({ "data": String::from_utf8_lossy(&data) }));
    let var = |name: &str| {
        ctx.get("vars")
            .and_then(|v| v.get(name))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    let field = |name: &str| message.get(name).cloned().unwrap_or(Value::Null);
    let envelope = serde_json::json!({
        "subscription": format!("projects/{}/subscriptions/{}", var("project_id"), var("subscription_id")),
        "message_id": field("messageId"),
        "publish_time": field("publishTime"),
        "attributes": field("attributes"),
        "ordering_key": field("orderingKey"),
    });
    match payload.as_object_mut() {
        Some(object) => {
            object.insert("_dfe_fetcher_pubsub".into(), envelope);
        }
        None => {
            payload = serde_json::json!({ "payload": payload, "_dfe_fetcher_pubsub": envelope });
        }
    }
    serde_json::to_vec(&payload)
        .map(|bytes| vec![Bytes::from(bytes)])
        .map_err(|e| Error::Decode(format!("pubsub_message: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ctx() -> TemplateCtx {
        let mut ctx = TemplateCtx::new();
        ctx.set(
            "vars",
            json!({"project_id": "proj", "subscription_id": "audit-sub"}),
        );
        ctx
    }

    fn received(data: &[u8]) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "ackId": "ack-1",
            "message": {
                "data": base64::engine::general_purpose::STANDARD.encode(data),
                "messageId": "m-1",
                "publishTime": "2026-05-21T10:00:00.000Z",
                "attributes": {"logging.googleapis.com/timestamp": "t"},
                "orderingKey": ""
            }
        }))
        .unwrap()
    }

    #[test]
    fn json_data_lands_as_itself_with_the_envelope() {
        let rows = expand(&received(br#"{"severity":"ERROR"}"#), &ctx()).unwrap();
        let row: Value = serde_json::from_slice(&rows[0]).unwrap();
        assert_eq!(row["severity"], "ERROR");
        let envelope = &row["_dfe_fetcher_pubsub"];
        assert_eq!(
            envelope["subscription"],
            "projects/proj/subscriptions/audit-sub"
        );
        assert_eq!(envelope["message_id"], "m-1");
        assert_eq!(envelope["publish_time"], "2026-05-21T10:00:00.000Z");
        assert_eq!(
            envelope["attributes"]["logging.googleapis.com/timestamp"],
            "t"
        );
        assert_eq!(envelope["ordering_key"], "");
    }

    #[test]
    fn text_data_is_wrapped_and_a_non_object_goes_under_payload() {
        let rows = expand(&received(b"plain text"), &ctx()).unwrap();
        let row: Value = serde_json::from_slice(&rows[0]).unwrap();
        assert_eq!(row["data"], "plain text");
        assert!(row["_dfe_fetcher_pubsub"].is_object());
        let rows = expand(&received(b"[1, 2]"), &ctx()).unwrap();
        let row: Value = serde_json::from_slice(&rows[0]).unwrap();
        assert_eq!(row["payload"], json!([1, 2]));
        let missing = serde_json::to_vec(&json!({"ackId": "a", "message": {}})).unwrap();
        let rows = expand(&missing, &ctx()).unwrap();
        let row: Value = serde_json::from_slice(&rows[0]).unwrap();
        assert_eq!(row["data"], "", "no data decodes to an empty string");
        assert!(row["_dfe_fetcher_pubsub"]["message_id"].is_null());
        assert!(expand(b"not json", &ctx()).is_err());
    }
}
