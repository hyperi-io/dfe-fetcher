// Project:   dfe-fetcher
// File:      crates/db/src/lines.rs
// Purpose:   NDJSON blocks from an engine turned into rows carrying their keyset mark
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Blocks into rows.
//!
//! Every engine hands the async side NDJSON in blocks: one `arrow-json`
//! buffer per fetched batch for ODBC, one HTTP body chunk for ClickHouse. The
//! framework's line framer ([`dfe_fetcher_core::frame`]) turns the block
//! stream into rows, holding each [`LeasedBlock`] -- and its lease on the
//! memory guard -- until its last row is out; what is the engine's own here
//! is the keyset mark a tail row carries.

use std::sync::Arc;

use futures::StreamExt;
use futures::stream::BoxStream;
use smallvec::SmallVec;

use dfe_fetcher_core::error::{Error, Result};
use dfe_fetcher_core::frame::framed;
pub use dfe_fetcher_core::frame::{LeasedBlock, LineFramer};
use dfe_fetcher_core::{Mark, Row, RowStream};

/// The keyset mark of a row: its `keys` columns, in order.
///
/// # Errors
///
/// Returns [`Error::Decode`] when the row is not a JSON object or lacks a key
/// column; a tail must never checkpoint a value it did not read.
pub fn keyset_mark(row: &[u8], keys: &[String]) -> Result<Mark> {
    let value: serde_json::Value =
        serde_json::from_slice(row).map_err(|e| Error::Decode(format!("tail row: {e}")))?;
    let object = value
        .as_object()
        .ok_or_else(|| Error::Decode("tail row is not a JSON object".into()))?;
    let mut tuple = SmallVec::new();
    for key in keys {
        let v = object.get(key).ok_or_else(|| {
            Error::Decode(format!(
                "tail row has no `{key}` column; the query must select every key column"
            ))
        })?;
        tuple.push(v.clone());
    }
    Ok(Mark::Keyset(tuple))
}

/// Rows from a block stream; each row carries a keyset mark when `keys` is set.
#[must_use]
pub fn rows_of_blocks(
    blocks: BoxStream<'_, Result<LeasedBlock>>,
    keys: Option<Arc<[String]>>,
) -> RowStream<'_> {
    framed(blocks, LineFramer::new(false))
        .map(move |line| {
            let line = line?;
            let mark = match &keys {
                Some(keys) => Some(keyset_mark(&line, keys)?),
                None => None,
            };
            Ok(Row {
                payload: line,
                mark,
            })
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use dfe_fetcher_core::batch::{Lease, NoLease};

    #[tokio::test]
    async fn tail_rows_carry_their_key_tuple_and_a_missing_key_is_an_error() {
        let l: Arc<dyn Lease> = Arc::new(NoLease);
        let blocks = futures::stream::iter(vec![
            Ok(LeasedBlock::new(
                Bytes::from_static(b"{\"ts\":\"2026-01-01T00:00:00Z\",\"id\":7,\"x\":1}\n"),
                l.clone(),
            )),
            Ok(LeasedBlock::new(
                Bytes::from_static(b"{\"id\":8}\n"),
                l.clone(),
            )),
        ])
        .boxed();
        let keys: Arc<[String]> = Arc::from(vec!["ts".to_owned(), "id".to_owned()]);
        let mut rows = rows_of_blocks(blocks, Some(keys));
        let first = rows.next().await.unwrap().unwrap();
        assert_eq!(
            first.mark,
            Some(Mark::Keyset(smallvec::smallvec![
                serde_json::json!("2026-01-01T00:00:00Z"),
                serde_json::json!(7)
            ]))
        );
        let err = rows.next().await.unwrap().unwrap_err();
        assert!(err.to_string().contains("no `ts` column"), "{err}");
    }

    #[tokio::test]
    async fn dump_rows_carry_no_mark_and_a_chunk_boundary_inside_a_row_does_not_split_it() {
        let l: Arc<dyn Lease> = Arc::new(NoLease);
        let blocks = futures::stream::iter(vec![
            Ok(LeasedBlock::new(
                Bytes::from_static(b"{\"id\":1}\n{\"id\""),
                l.clone(),
            )),
            Ok(LeasedBlock::new(Bytes::from_static(b":2}"), l.clone())),
        ])
        .boxed();
        let rows: Vec<Row> = rows_of_blocks(blocks, None)
            .map(|r| r.unwrap())
            .collect()
            .await;
        assert_eq!(rows.len(), 2);
        assert_eq!(&rows[1].payload[..], b"{\"id\":2}");
        assert!(rows.iter().all(|r| r.mark.is_none()));
    }
}
