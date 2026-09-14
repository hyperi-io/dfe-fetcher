// Project:   dfe-fetcher
// File:      crates/rest/src/hooks/columnar_table.rs
// Purpose:   The columnar-table row builder: `{columns, rows}` into one object per row
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A columnar table (`{"columns": [{"name": ...}, ...], "rows": [[...], ...]}`,
//! the Log Analytics query result) becomes one JSON object per row, each
//! value keyed by its column's name. A row shorter than the column list keeps
//! the columns it has; a column without a name is skipped with its values.

use bytes::Bytes;
use serde::Deserialize;
use serde_json::Value;

use dfe_fetcher_core::error::{Error, Result};

#[derive(Deserialize)]
struct Column {
    #[serde(default)]
    name: Option<String>,
}

#[derive(Deserialize)]
struct Table {
    #[serde(default)]
    columns: Vec<Column>,
    #[serde(default)]
    rows: Vec<Vec<Value>>,
}

/// The objects a table holds, in row order.
pub(super) fn expand(table: &[u8]) -> Result<Vec<Bytes>> {
    let table: Table = serde_json::from_slice(table)
        .map_err(|e| Error::Decode(format!("columnar_table: the row is not a table: {e}")))?;
    table
        .rows
        .into_iter()
        .map(|row| {
            let object: serde_json::Map<String, Value> = table
                .columns
                .iter()
                .zip(row)
                .filter_map(|(column, value)| column.name.clone().map(|name| (name, value)))
                .collect();
            serde_json::to_vec(&Value::Object(object))
                .map(Bytes::from)
                .map_err(|e| Error::Decode(format!("columnar_table: {e}")))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn each_row_becomes_an_object_keyed_by_column_name() {
        let table = json!({
            "name": "PrimaryResult",
            "columns": [{"name": "TimeGenerated", "type": "datetime"}, {"name": "Computer", "type": "string"}, {"type": "int"}],
            "rows": [["2026-01-01T00:00:00Z", "web-1", 7], ["2026-01-01T00:01:00Z", null]]
        });
        let rows = expand(&serde_json::to_vec(&table).unwrap()).unwrap();
        let rows: Vec<Value> = rows
            .iter()
            .map(|r| serde_json::from_slice(r).unwrap())
            .collect();
        assert_eq!(
            rows,
            [
                json!({"TimeGenerated": "2026-01-01T00:00:00Z", "Computer": "web-1"}),
                json!({"TimeGenerated": "2026-01-01T00:01:00Z", "Computer": null}),
            ],
            "a nameless column is skipped and a short row keeps what it has"
        );
        assert!(
            expand(br#"{"columns": [], "rows": []}"#)
                .unwrap()
                .is_empty()
        );
        assert_eq!(expand(br#"{"rows": [[1]]}"#).unwrap()[0].as_ref(), b"{}");
        assert!(expand(b"[1, 2]").is_err(), "not a table");
        assert!(expand(br#"{"rows": [1]}"#).is_err(), "a row is a list");
    }
}
