// Project:   dfe-fetcher
// File:      crates/db/src/odbc.rs
// Purpose:   The ODBC engine: a block cursor on the blocking pool, rows typed by arrow-odbc, JSON by arrow-json
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The ODBC engine.
//!
//! One [`OdbcStore`] per configured store. A dump or tail opens a connection
//! on the blocking pool, runs the query, binds a block cursor bounded by the
//! instance's [`BatchSpec`], and for every fetched block lets `arrow-odbc`
//! type the columns and `arrow-json` write them as NDJSON. Each block crosses
//! to the async side as a [`LeasedBlock`] through the bounded [`pump`], so an
//! unpolled driver parks the cursor and the engine stops fetching.
//!
//! # Type mapping
//!
//! The columns are typed by `arrow-odbc` from the driver's metadata and
//! written by `arrow-json`; nothing here maps a SQL type by hand. As landed:
//!
//! | SQL (ODBC) type | JSON |
//! |---|---|
//! | `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT` | number |
//! | `NUMERIC(p, s)`, `DECIMAL(p, s)`, `p <= 38` | number with `s` fraction digits |
//! | `REAL`, `FLOAT`, `DOUBLE` | number |
//! | `BIT` (`boolean` on PostgreSQL with `BoolsAsChar=0`) | `true` / `false` |
//! | `DATE` | `"YYYY-MM-DD"` |
//! | `TIME` | `"HH:MM:SS[.fff]"` |
//! | `TIMESTAMP` (with or without zone) | `"YYYY-MM-DDTHH:MM:SS[.ffffff]Z"` |
//! | `BINARY(n)`, `VARBINARY`, `LONGVARBINARY` (`bytea`, `BLOB`) | base64 string |
//! | `CHAR`, `VARCHAR`, `TEXT`, and every other type (UUID, JSON, arrays, INET, ...) | string, as the driver renders it (psqlodbc upper-cases a `uuid`) |
//! | `NULL` | `null`, the key always present |
//!
//! Timestamps: the session zone is pinned to UTC ([`Dialect::session_prelude`])
//! so a zoned column converts exactly; ODBC reports a zone-less column the
//! same way, so it is emitted as UTC by convention.
//!
//! # Drivers
//!
//! The driver manager is unixODBC (LGPL-2.1, linked dynamically) and each
//! engine's driver is loaded by name from `odbcinst.ini` at connect time:
//! psqlodbc (LGPL-2.1) and MariaDB Connector/ODBC (LGPL-2.1) ship in the
//! image, the proprietary drivers (msodbcsql, Oracle Instant Client,
//! Snowflake, Simba) are operator-supplied under their own licences. The
//! per-engine matrix is documented with the deployment.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::{Array, AsArray, FixedSizeBinaryArray, GenericBinaryArray, OffsetSizeTrait};
use arrow::datatypes::{DataType, FieldRef};
use arrow::error::ArrowError;
use arrow::json::writer::{LineDelimited, WriterBuilder};
use arrow::json::{Encoder, EncoderFactory, EncoderOptions, writer::NullableEncoder};
use arrow::record_batch::RecordBatch;
use arrow_odbc::OdbcReaderBuilder;
use base64::Engine as _;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryStreamExt};
use odbc_api::parameter::InputParameter;
use odbc_api::{ConnectionOptions, Environment, IntoParameter};
use serde_json::Value;
use tracing::{debug, warn};

use dfe_fetcher_core::RowStream;
use dfe_fetcher_core::batch::Lease;
use dfe_fetcher_core::error::{Error, Result};

use crate::config::BatchSpec;
use crate::keyset;
use crate::lines::{LeasedBlock, rows_of_blocks};
use crate::pump::{classify_sqlstate, pump};
use crate::secret::Secret;
use crate::store::{Dialect, Store};

/// RFC 3339 with the fraction the value carries and an explicit UTC marker.
const TIMESTAMP_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.fZ";

/// Blocks the async side may hold before the cursor parks.
const PUMP_CAPACITY: usize = 2;

/// Base64 for binary columns, where `arrow-json` would write hex.
#[derive(Debug)]
struct Base64Binary;

struct Base64Encoder<A>(A);

impl<O: OffsetSizeTrait> Encoder for Base64Encoder<&GenericBinaryArray<O>> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        write_base64(self.0.value(idx), out);
    }
}

impl Encoder for Base64Encoder<&FixedSizeBinaryArray> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        write_base64(self.0.value(idx), out);
    }
}

/// One quoted base64 value, one allocation per value.
fn write_base64(bytes: &[u8], out: &mut Vec<u8>) {
    out.push(b'"');
    let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
    out.extend_from_slice(encoded.as_bytes());
    out.push(b'"');
}

impl EncoderFactory for Base64Binary {
    fn make_default_encoder<'a>(
        &self,
        _field: &'a FieldRef,
        array: &'a dyn Array,
        _options: &'a EncoderOptions,
    ) -> std::result::Result<Option<NullableEncoder<'a>>, ArrowError> {
        let nulls = array.nulls().cloned();
        let encoder: Box<dyn Encoder + 'a> = match array.data_type() {
            DataType::Binary => Box::new(Base64Encoder(array.as_binary::<i32>())),
            DataType::LargeBinary => Box::new(Base64Encoder(array.as_binary::<i64>())),
            DataType::FixedSizeBinary(_) => Box::new(Base64Encoder(array.as_fixed_size_binary())),
            _ => return Ok(None),
        };
        Ok(Some(NullableEncoder::new(encoder, nulls)))
    }
}

/// One record batch as NDJSON: every column present (`null` where NULL),
/// timestamps RFC 3339 UTC, binary base64.
///
/// # Errors
///
/// Returns [`Error::Decode`] when a column type has no JSON encoding.
pub fn ndjson_of(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(batch.get_array_memory_size());
    let mut writer = WriterBuilder::new()
        .with_explicit_nulls(true)
        .with_timestamp_format(TIMESTAMP_FORMAT.to_owned())
        .with_encoder_factory(Arc::new(Base64Binary))
        .build::<_, LineDelimited>(&mut out);
    writer
        .write(batch)
        .and_then(|()| writer.finish())
        .map_err(|e| Error::Decode(format!("record batch to JSON: {e}")))?;
    Ok(out)
}

/// A JSON key value as an ODBC parameter for the keyset predicate.
///
/// # Errors
///
/// Returns [`Error::Cursor`] for a value that cannot order rows: `null`, an
/// object, or an array.
pub fn parameter_of(value: &Value) -> Result<Box<dyn InputParameter>> {
    Ok(match value {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Box::new(i.into_parameter())
            } else if n.is_u64() {
                // ODBC has no unsigned BIGINT; the engine casts the text.
                Box::new(n.to_string().into_parameter())
            } else if let Some(f) = n.as_f64() {
                Box::new(f.into_parameter())
            } else {
                return Err(Error::Cursor(format!("keyset value {n} is not a number")));
            }
        }
        Value::String(s) => Box::new(s.clone().into_parameter()),
        Value::Bool(b) => Box::new(odbc_api::Bit::from_bool(*b).into_parameter()),
        Value::Null => {
            return Err(Error::Cursor(
                "keyset value is null; a tail key column must be NOT NULL".into(),
            ));
        }
        Value::Array(_) | Value::Object(_) => {
            return Err(Error::Cursor(
                "keyset value is a JSON container, not a scalar".into(),
            ));
        }
    })
}

/// The framework error for an ODBC failure: SQLSTATE classes decide.
fn odbc_error(e: odbc_api::Error) -> Error {
    match e {
        odbc_api::Error::Diagnostics { record, .. } => {
            classify_sqlstate(record.state.as_str(), &record.to_string())
        }
        odbc_api::Error::TooLargeValueForBuffer {
            indicator,
            buffer_index,
        } => Error::Decode(format!(
            "a value in column {buffer_index} does not fit the transit buffer (indicator \
             {indicator:?}); raise batch.max_text_bytes or batch.max_binary_bytes"
        )),
        other => Error::Source(other.to_string()),
    }
}

fn arrow_odbc_error(e: &arrow_odbc::Error) -> Error {
    Error::Decode(format!("odbc to arrow: {e}"))
}

fn arrow_error(e: ArrowError) -> Error {
    match e {
        ArrowError::ExternalError(inner) => match inner.downcast::<odbc_api::Error>() {
            Ok(odbc) => odbc_error(*odbc),
            Err(other) => Error::Decode(format!("fetch: {other}")),
        },
        other => Error::Decode(format!("fetch: {other}")),
    }
}

/// The process-wide ODBC environment.
///
/// # Errors
///
/// Returns [`Error::Source`] when the driver manager cannot be initialised.
pub fn environment() -> Result<&'static Environment> {
    odbc_api::environment().map_err(odbc_error)
}

/// The driver names the driver manager knows, from `odbcinst.ini`.
///
/// # Errors
///
/// Returns [`Error::Source`] when the driver manager cannot be asked.
pub fn installed_drivers() -> Result<Vec<String>> {
    Ok(environment()?
        .drivers()
        .map_err(odbc_error)?
        .into_iter()
        .map(|d| d.description)
        .collect())
}

/// Run `statements` in order on a fresh connection, on the blocking pool,
/// discarding any result sets: the dialect's session prelude first, then
/// the caller's DDL or DML.
///
/// # Errors
///
/// Returns the first statement's error, SQLSTATE-classified.
pub async fn run_statements(
    connection_string: &str,
    dialect: Dialect,
    statements: &[&str],
) -> Result<()> {
    let connection_string = connection_string.to_owned();
    let statements: Vec<String> = statements.iter().map(|s| (*s).to_owned()).collect();
    tokio::task::spawn_blocking(move || {
        let conn = environment()?
            .connect_with_connection_string(&connection_string, ConnectionOptions::default())
            .map_err(odbc_error)?;
        for statement in dialect
            .session_prelude()
            .iter()
            .copied()
            .chain(statements.iter().map(String::as_str))
        {
            conn.execute(statement, (), None).map_err(odbc_error)?;
        }
        Ok(())
    })
    .await
    .map_err(|e| Error::Source(format!("statement task: {e}")))?
}

/// What one cursor run needs, owned so it can move to the blocking pool.
struct Job {
    connection_string: String,
    dialect: Dialect,
    sql: String,
    params: Vec<Value>,
    batch: BatchSpec,
    lease: Arc<dyn Lease>,
    unit: Arc<str>,
    /// Shared with the store: the streaming-knob warning is a fact about the
    /// connection string, which does not change, so it is logged once.
    warned_streaming: Arc<AtomicBool>,
}

impl Job {
    /// Connect, run the query, and hand every block to `emit` until the
    /// cursor is exhausted or `emit` says the consumer is gone.
    fn run(self, emit: &mut dyn FnMut(LeasedBlock) -> bool) -> Result<()> {
        let env = environment()?;
        // Once per store: a referenced connection string cannot be inspected
        // at load, and the answer does not change for the store's life.
        if let Some(knob) = self.dialect.streaming_knob()
            && !self
                .connection_string
                .to_ascii_lowercase()
                .contains(&knob.to_ascii_lowercase())
            && !self.warned_streaming.swap(true, Ordering::Relaxed)
        {
            warn!(
                unit = %self.unit,
                dialect = self.dialect.as_str(),
                "connection string lacks `{knob}`; the driver will buffer the whole result set client-side"
            );
        }
        let conn = env
            .connect_with_connection_string(&self.connection_string, ConnectionOptions::default())
            .map_err(odbc_error)?;
        for statement in self.dialect.session_prelude() {
            conn.execute(statement, (), None).map_err(odbc_error)?;
        }
        let dbms = conn.database_management_system_name().ok();
        let params: Vec<Box<dyn InputParameter>> = self
            .params
            .iter()
            .map(parameter_of)
            .collect::<Result<_>>()?;
        let Some(cursor) = conn
            .execute(&self.sql, &params[..], None)
            .map_err(odbc_error)?
        else {
            debug!(unit = %self.unit, "query produced no result set");
            return Ok(());
        };
        let mut builder = OdbcReaderBuilder::new();
        builder
            .with_max_num_rows_per_batch(self.batch.max_rows)
            .with_max_bytes_per_batch(self.batch.max_bytes)
            .with_max_text_size(self.batch.max_text_bytes)
            .with_max_binary_size(self.batch.max_binary_bytes)
            .with_fallibale_allocations(true);
        if let Some(name) = dbms {
            builder.with_dbms_name(name);
        }
        let reader = builder.build(cursor).map_err(|e| arrow_odbc_error(&e))?;
        for batch in reader {
            let batch = batch.map_err(arrow_error)?;
            let block = ndjson_of(&batch)?;
            if !emit(LeasedBlock::new(
                Bytes::from(block),
                Arc::clone(&self.lease),
            )) {
                debug!(unit = %self.unit, "consumer gone; cursor released");
                break;
            }
        }
        Ok(())
    }
}

/// One store on an ODBC connection.
pub struct OdbcStore {
    connection_string: Arc<Secret>,
    dialect: Dialect,
    query: String,
    /// The key columns as configured, written into the SQL.
    keys: Vec<String>,
    /// The same keys as the row JSON names them, read for the mark.
    columns: Arc<[String]>,
    batch: BatchSpec,
    lease: Arc<dyn Lease>,
    unit: Arc<str>,
    /// Set once the streaming-knob warning has been logged for this store.
    warned_streaming: Arc<AtomicBool>,
}

impl OdbcStore {
    /// A store over `query` on the connection `connection_string` resolves to.
    ///
    /// `keys` is empty for a dump; a tail's rows carry their `keys` values
    /// as the keyset mark.
    #[must_use]
    pub fn new(
        unit: &str,
        connection_string: Arc<Secret>,
        dialect: Dialect,
        query: String,
        keys: Vec<String>,
        batch: BatchSpec,
        lease: Arc<dyn Lease>,
    ) -> Self {
        let columns: Vec<String> = keys.iter().map(|k| keyset::column(dialect, k)).collect();
        Self {
            connection_string,
            dialect,
            query,
            keys,
            columns: Arc::from(columns),
            batch,
            lease,
            unit: Arc::from(unit),
            warned_streaming: Arc::new(AtomicBool::new(false)),
        }
    }

    /// The tail statement for this store.
    #[must_use]
    pub fn tail_sql(&self, has_after: bool, limit: u32) -> String {
        let keys: Vec<&str> = self.keys.iter().map(String::as_str).collect();
        keyset::tail_sql(
            self.dialect,
            &self.query,
            &keys,
            has_after,
            limit,
            &keyset::question_mark,
        )
    }

    /// Resolve the secret, then stream the blocks of one cursor run.
    fn blocks(
        &self,
        sql: String,
        params: Vec<Value>,
    ) -> futures::stream::BoxStream<'_, Result<LeasedBlock>> {
        let start = async move {
            let connection_string = self.connection_string.value().await?.to_owned();
            let job = Job {
                connection_string,
                dialect: self.dialect,
                sql,
                params,
                batch: self.batch,
                lease: Arc::clone(&self.lease),
                unit: Arc::clone(&self.unit),
                warned_streaming: Arc::clone(&self.warned_streaming),
            };
            Ok::<_, Error>(pump(PUMP_CAPACITY, move |emit| job.run(emit)))
        };
        futures::stream::once(start).try_flatten().boxed()
    }
}

impl Store for OdbcStore {
    fn dump(&self) -> RowStream<'_> {
        rows_of_blocks(self.blocks(self.query.clone(), Vec::new()), None)
    }

    fn tail(&self, after: Option<Vec<Value>>, limit: u32) -> RowStream<'_> {
        if self.keys.is_empty() {
            return futures::stream::once(async {
                Err(Error::Config("tail on a store with no key columns".into()))
            })
            .boxed();
        }
        let sql = self.tail_sql(after.is_some(), limit);
        let params = match after {
            Some(values) if values.len() != self.keys.len() => {
                let problem = format!(
                    "checkpoint has {} values for {} key columns; the key list changed under a \
                     live checkpoint",
                    values.len(),
                    self.keys.len()
                );
                return futures::stream::once(async move { Err(Error::Cursor(problem)) }).boxed();
            }
            Some(values) => keyset::bind_order(self.dialect, self.keys.len())
                .into_iter()
                .map(|i| values[i].clone())
                .collect(),
            None => Vec::new(),
        };
        rows_of_blocks(self.blocks(sql, params), Some(Arc::clone(&self.columns)))
    }

    fn probe(&self) -> BoxFuture<'_, Result<()>> {
        async move {
            let connection_string = self.connection_string.value().await?.to_owned();
            let dialect = self.dialect;
            tokio::task::spawn_blocking(move || {
                let conn = environment()?
                    .connect_with_connection_string(
                        &connection_string,
                        ConnectionOptions::default(),
                    )
                    .map_err(odbc_error)?;
                for statement in dialect.session_prelude() {
                    conn.execute(statement, (), None).map_err(odbc_error)?;
                }
                conn.database_management_system_name()
                    .map_err(odbc_error)
                    .map(|_| ())
            })
            .await
            .map_err(|e| Error::Source(format!("probe task: {e}")))?
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
        Float64Array, Int32Array, Int64Array, StringArray, Time32SecondArray,
        TimestampMicrosecondArray, TimestampSecondArray,
    };
    use arrow::datatypes::{Field, Schema, TimeUnit};
    use dfe_fetcher_core::batch::NoLease;
    use scalo::SensitiveString;

    fn batch() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("i32", DataType::Int32, true),
            Field::new("i64", DataType::Int64, false),
            Field::new("dec", DataType::Decimal128(10, 2), true),
            Field::new("f64", DataType::Float64, true),
            Field::new("flag", DataType::Boolean, true),
            Field::new("day", DataType::Date32, true),
            Field::new(
                "at_us",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("at_s", DataType::Timestamp(TimeUnit::Second, None), true),
            Field::new("clock", DataType::Time32(TimeUnit::Second), true),
            Field::new("text", DataType::Utf8, true),
            Field::new("blob", DataType::Binary, true),
            Field::new("fixed", DataType::FixedSizeBinary(2), true),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![Some(-7), None])),
                Arc::new(Int64Array::from(vec![9_007_199_254_740_993_i64, 0])),
                Arc::new(
                    Decimal128Array::from(vec![Some(123_456_i128), None])
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ),
                Arc::new(Float64Array::from(vec![Some(1.5), None])),
                Arc::new(BooleanArray::from(vec![Some(true), Some(false)])),
                Arc::new(Date32Array::from(vec![Some(20_454), None])),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(1_767_225_600_123_456_i64),
                    None,
                ])),
                Arc::new(TimestampSecondArray::from(vec![
                    Some(1_767_225_600_i64),
                    None,
                ])),
                Arc::new(Time32SecondArray::from(vec![Some(3_661), None])),
                Arc::new(StringArray::from(vec![Some("h\u{e9}llo \"q\""), None])),
                Arc::new(BinaryArray::from(vec![
                    Some(&[0xde_u8, 0xad, 0xbe, 0xef][..]),
                    None,
                ])),
                Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                        vec![Some([0x00_u8, 0xff]), None].into_iter(),
                        2,
                    )
                    .unwrap(),
                ),
            ],
        )
        .unwrap()
    }

    #[test]
    fn every_arrow_type_the_reader_produces_lands_as_the_documented_json() {
        let out = ndjson_of(&batch()).unwrap();
        let text = String::from_utf8(out).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        let first: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["i32"], -7);
        assert_eq!(
            first["i64"], 9_007_199_254_740_993_i64,
            "an i64 stays exact"
        );
        assert_eq!(first["dec"], 1234.56);
        assert_eq!(first["f64"], 1.5);
        assert_eq!(first["flag"], true);
        assert_eq!(first["day"], "2026-01-01");
        assert_eq!(first["at_us"], "2026-01-01T00:00:00.123456Z");
        assert_eq!(first["at_s"], "2026-01-01T00:00:00Z");
        assert_eq!(first["clock"], "01:01:01");
        assert_eq!(first["text"], "h\u{e9}llo \"q\"");
        assert_eq!(first["blob"], "3q2+7w==");
        assert_eq!(first["fixed"], "AP8=");

        let second: Value = serde_json::from_str(lines[1]).unwrap();
        let object = second.as_object().unwrap();
        for key in [
            "i32", "dec", "f64", "day", "at_us", "at_s", "clock", "text", "blob", "fixed",
        ] {
            assert!(object.contains_key(key), "`{key}` is present when NULL");
            assert!(object[key].is_null(), "`{key}` is null");
        }
        assert_eq!(second["i64"], 0);
        assert_eq!(second["flag"], false);
    }

    #[test]
    fn keyset_values_bind_as_parameters_and_nulls_are_refused() {
        assert!(parameter_of(&serde_json::json!(42)).is_ok());
        assert!(parameter_of(&serde_json::json!(18_446_744_073_709_551_615_u64)).is_ok());
        assert!(parameter_of(&serde_json::json!(1.5)).is_ok());
        assert!(parameter_of(&serde_json::json!("2026-01-01T00:00:00Z")).is_ok());
        assert!(parameter_of(&serde_json::json!(true)).is_ok());
        assert!(matches!(parameter_of(&Value::Null), Err(Error::Cursor(_))));
        assert!(matches!(
            parameter_of(&serde_json::json!({"a": 1})),
            Err(Error::Cursor(_))
        ));
    }

    #[test]
    fn the_tail_statement_wraps_the_query_and_binds_in_dialect_order() {
        let store = OdbcStore::new(
            "events",
            Arc::new(Secret::new(SensitiveString::from("x".to_owned()))),
            Dialect::Postgres,
            "SELECT ts, id, body FROM events".into(),
            vec!["ts".into(), "id".into()],
            BatchSpec::default(),
            Arc::new(NoLease),
        );
        assert_eq!(
            store.tail_sql(true, 500),
            "SELECT * FROM (SELECT ts, id, body FROM events) AS dfe_tail WHERE (ts, id) > (?, ?) \
             ORDER BY ts, id LIMIT 500"
        );
        assert_eq!(
            store.tail_sql(false, 500),
            "SELECT * FROM (SELECT ts, id, body FROM events) AS dfe_tail ORDER BY ts, id LIMIT 500"
        );
    }

    #[test]
    fn a_delimited_key_is_written_as_given_and_its_mark_read_under_the_bare_column() {
        let store = OdbcStore::new(
            "events",
            Arc::new(Secret::new(SensitiveString::from("x".to_owned()))),
            Dialect::Mssql,
            "SELECT * FROM events".into(),
            vec!["[Order Date]".into(), "id".into()],
            BatchSpec::default(),
            Arc::new(NoLease),
        );
        assert_eq!(
            store.tail_sql(true, 5),
            "SELECT * FROM (SELECT * FROM events) AS dfe_tail WHERE [Order Date] > ? OR ([Order Date] = ? AND id > ?) \
             ORDER BY [Order Date], id OFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY"
        );
        assert_eq!(&*store.columns, ["Order Date".to_owned(), "id".to_owned()]);
    }

    #[test]
    fn sqlstate_and_truncation_errors_map_to_the_framework_vocabulary() {
        let truncated = odbc_error(odbc_api::Error::TooLargeValueForBuffer {
            indicator: Some(70_000),
            buffer_index: 3,
        });
        assert!(matches!(truncated, Error::Decode(_)));
        assert!(truncated.to_string().contains("max_text_bytes"));
        let other = odbc_error(odbc_api::Error::FailedAllocatingEnvironment);
        assert!(matches!(other, Error::Source(_)));
    }
}
