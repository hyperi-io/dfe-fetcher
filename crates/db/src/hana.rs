// Project:   dfe-fetcher
// File:      crates/db/src/hana.rs
// Purpose:   The SAP HANA engine: rows streamed over the pure-Rust wire protocol, keyset tail with typed parameters
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The SAP HANA engine.
//!
//! One [`HanaStore`] per configured store over `hdbconnect_async`, a pure
//! Rust client of HANA's own wire protocol: no driver manager, no SAP client
//! package, TLS through rustls. A connection is opened for each dump and each
//! tail page and lives as long as its row stream, so a connection broken
//! between ticks costs nothing the next tick does not reopen.
//!
//! The connection string is `hdbsql://user:password@host:port` for a plain
//! connection, or `hdbsqls://...` with one TLS option for an encrypted one:
//! `use_mozillas_root_certificates` (HANA Cloud's certificate chains to a public
//! root), `tls_certificate_dir=<dir>` or `tls_certificate_env=<variable>` for a
//! private authority. `db=<name>` selects the tenant database of a multi-database
//! system.
//!
//! # Streaming
//!
//! The driver fetches `batch.max_rows` rows a round trip and this engine reads
//! them one row at a time, closing a block at `batch.max_rows` rows or
//! `batch.max_bytes` bytes, so a stalled consumer leaves the rest of the
//! result set on the server. A LOB or a string or binary value over the
//! `batch` caps fails the tick, as it does on the other engines. The driver holds
//! the `batch.max_rows` rows of a round trip before this engine reads the first,
//! outside the memory lease. The first round trip fetches one row. Each later one is
//! sized from the width of the recent rows to about `batch.max_bytes`, at most doubling.
//!
//! # Type mapping
//!
//! Integers and booleans land as JSON numbers and booleans. A `DECIMAL` lands as
//! a number when 15 digits or fewer hold it exactly and as a string otherwise.
//! `TIMESTAMP`, `SECONDDATE`, `DATE` and `TIME` land as ISO text with no zone,
//! as stored (`2026-01-01T00:00:00.1234567`): HANA keeps no zone on them.
//! Binary and geometry values land as base64.
//!
//! # Keyset tail
//!
//! The operator's query becomes a derived table and the keyset predicate is
//! written as the expanded disjunction (`a > ? OR (a = ? AND b > ?)`), because
//! HANA's ordered comparison of row values is not documented. Values are bound,
//! never written into the SQL. The key columns' types are read once, from the
//! query's own result metadata, because a timestamp checkpoint is text in the
//! cursor store and has to be cast back (`CAST(? AS TIMESTAMP)`) to compare
//! as one. A key column must be `NOT NULL`, and one the result reports as nullable
//! is refused at the first tail: a null never compares greater than anything, so
//! a row with one would be skipped.

use std::sync::{Arc, Once};

use base64::Engine as _;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt, TryStreamExt};
use hdbconnect_async::{
    ConnectParams, ConnectParamsBuilder, Connection, ConnectionConfiguration, FieldMetadata,
    HdbError, HdbValue, ResultSet, TypeId,
};
use serde_json::{Map, Value};
use tokio::sync::OnceCell;

use dfe_fetcher_core::RowStream;
use dfe_fetcher_core::batch::Lease;
use dfe_fetcher_core::error::{Error, Result};

use crate::config::BatchSpec;
use crate::keyset;
use crate::lines::{LeasedBlock, rows_of_blocks};
use crate::secret::Secret;
use crate::store::{Dialect, Store};

/// The driver crate, re-exported so a caller can seed or inspect a system
/// with the client the engine itself speaks through.
pub use hdbconnect_async;

/// HANA's authentication failure, `authentication failed`.
const AUTHENTICATION_FAILED: i32 = 10;

/// The framework error for a driver failure: the class decides whether the
/// operator fixes a credential, a setting or the network.
fn hana_error(e: &HdbError) -> Error {
    let mut text = e.to_string();
    let mut cause = std::error::Error::source(e);
    while let Some(inner) = cause {
        text.push_str(": ");
        text.push_str(&inner.to_string());
        cause = std::error::Error::source(inner);
    }
    match e {
        HdbError::Authentication { .. } => Error::Credential(format!("hana: {text}")),
        HdbError::DbError { source } if source.code() == AUTHENTICATION_FAILED => {
            Error::Credential(format!("hana ({}): {}", source.code(), source.text()))
        }
        HdbError::DbError { source } => {
            Error::Source(format!("hana ({}): {}", source.code(), source.text()))
        }
        HdbError::ConnParams { .. } => Error::Config(format!("hana connection_string: {text}")),
        HdbError::TlsInit { .. }
        | HdbError::TlsProtocol { .. }
        | HdbError::TlsInvalidDnsName { .. } => Error::Source(format!("hana tls: {text}")),
        _ => Error::Source(format!("hana: {text}")),
    }
}

/// The connection parameters a URL describes, with the user name and password
/// percent-decoded.
///
/// The driver hands the user and password of a URL to the server exactly as the
/// URL spells them, so a password written `p%40ss` would be sent with its `%40`.
/// A URL has to percent-encode `@`, `:`, `/`, `?`, `#` and `%` in a password, so
/// the decoding is done here and the rest of the URL is left to the driver.
///
/// # Errors
///
/// Returns [`Error::Config`] for a URL the driver rejects, or whose user or
/// password is not valid UTF-8 once decoded.
pub fn connect_params(url: &str) -> Result<ConnectParams> {
    let mut parsed =
        url::Url::parse(url).map_err(|e| Error::Config(format!("hana connection_string: {e}")))?;
    let decode = |what: &str, text: &str| -> Result<String> {
        percent_encoding::percent_decode_str(text)
            .decode_utf8()
            .map(std::borrow::Cow::into_owned)
            .map_err(|_| Error::Config(format!("hana connection_string: the {what} is not UTF-8")))
    };
    let user = decode("user name", parsed.username())?;
    let password = decode("password", parsed.password().unwrap_or_default())?;
    // The credentials come out of the URL the driver parses and go in by hand.
    let _ = parsed.set_username("");
    let _ = parsed.set_password(None);
    let invalid = |e: HdbError| Error::Config(format!("hana connection_string: {e}"));
    let mut builder = ConnectParamsBuilder::from(parsed.as_str()).map_err(invalid)?;
    builder.dbuser(user).password(password);
    builder.build().map_err(invalid)
}

/// Install rustls's process-wide crypto provider when none is set, once: the
/// driver builds its client config from the default, and a build carrying more
/// than one provider has none to pick.
fn ensure_crypto_provider() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        if rustls::crypto::CryptoProvider::get_default().is_none() {
            // Another thread may have installed one first, which is as good.
            let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        }
    });
}

/// How a key column is compared: the placeholder text and the bound value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyKind {
    /// `TIMESTAMP` and `SECONDDATE`, bound as `YYYY-MM-DD HH:MM:SS[.fffffff]` text.
    Timestamp,
    /// `DATE`.
    Date,
    /// `TIME`.
    Time,
    /// An integer type, bound as a number.
    Integer,
    /// Anything else the server converts from the bound text or number.
    Other,
}

impl KeyKind {
    fn of(type_id: TypeId) -> Self {
        match type_id {
            TypeId::LONGDATE | TypeId::SECONDDATE => KeyKind::Timestamp,
            TypeId::DAYDATE => KeyKind::Date,
            TypeId::SECONDTIME => KeyKind::Time,
            TypeId::TINYINT | TypeId::SMALLINT | TypeId::INT | TypeId::BIGINT => KeyKind::Integer,
            _ => KeyKind::Other,
        }
    }

    /// The SQL for one bound value of this kind.
    fn placeholder(self) -> &'static str {
        match self {
            KeyKind::Timestamp => "CAST(? AS TIMESTAMP)",
            KeyKind::Date => "CAST(? AS DATE)",
            KeyKind::Time => "CAST(? AS TIME)",
            KeyKind::Integer | KeyKind::Other => "?",
        }
    }

    /// The text a checkpointed key is bound as. The prepared statement's own
    /// parameter descriptor parses it into the exact driver type the server
    /// expects, which the driver insists on: an `INT` parameter takes no `BIGINT`.
    fn text(self, value: &Value) -> Result<String> {
        match value {
            Value::Null => Err(Error::Cursor(
                "keyset value is null. A tail key column must be NOT NULL".into(),
            )),
            Value::Array(_) | Value::Object(_) => Err(Error::Cursor(
                "keyset value is a JSON container, not a scalar".into(),
            )),
            Value::Bool(b) => Ok(b.to_string()),
            Value::Number(n) => Ok(n.to_string()),
            // The row JSON writes a timestamp with a `T`, and the cast reads a space.
            Value::String(s) if self == KeyKind::Timestamp => Ok(s.replacen('T', " ", 1)),
            Value::String(s) => Ok(s.clone()),
        }
    }
}

/// A number from a `DECIMAL`'s text when 15 significant digits hold it exactly,
/// else the text.
fn decimal_json(text: &str) -> Value {
    let digits = text.chars().filter(char::is_ascii_digit).count();
    if let Ok(i) = text.parse::<i64>() {
        return Value::from(i);
    }
    if digits <= 15
        && let Ok(f) = text.parse::<f64>()
        && let Some(n) = serde_json::Number::from_f64(f)
    {
        return Value::Number(n);
    }
    Value::String(text.to_owned())
}

/// A float as JSON. JSON has no `NaN` or infinity, so those land as text.
fn float_json(f: f64) -> Value {
    serde_json::Number::from_f64(f).map_or_else(|| Value::String(f.to_string()), Value::Number)
}

/// A string value, held to `batch.max_text_bytes`.
fn text_json(s: String, batch: &BatchSpec) -> Result<Value> {
    if s.len() > batch.max_text_bytes {
        return Err(Error::Source(format!(
            "hana: a text value of {} bytes is over batch.max_text_bytes ({}); raise \
             batch.max_text_bytes",
            s.len(),
            batch.max_text_bytes
        )));
    }
    Ok(Value::String(s))
}

/// A binary value as base64, held to `batch.max_binary_bytes`.
fn binary_json(bytes: &[u8], batch: &BatchSpec) -> Result<Value> {
    if bytes.len() > batch.max_binary_bytes {
        return Err(Error::Source(format!(
            "hana: a binary value of {} bytes is over batch.max_binary_bytes ({}); raise \
             batch.max_binary_bytes",
            bytes.len(),
            batch.max_binary_bytes
        )));
    }
    Ok(Value::String(
        base64::engine::general_purpose::STANDARD.encode(bytes),
    ))
}

/// The error for a LOB over its cap, read before any of it is fetched.
fn lob_too_big(kind: &str, size: u64, cap: usize, field: &str) -> Error {
    Error::Source(format!(
        "hana: a {kind} value of {size} bytes is over batch.{field} ({cap}); raise batch.{field}"
    ))
}

/// One value as JSON, reading a LOB in full when it fits the batch caps.
fn value_json<'a>(value: HdbValue<'static>, batch: &'a BatchSpec) -> BoxFuture<'a, Result<Value>> {
    async move {
        Ok(match value {
            HdbValue::NULL => Value::Null,
            HdbValue::TINYINT(v) => Value::from(v),
            HdbValue::SMALLINT(v) => Value::from(v),
            HdbValue::INT(v) => Value::from(v),
            HdbValue::BIGINT(v) => Value::from(v),
            HdbValue::DECIMAL(d) => decimal_json(&d.to_string()),
            // The shortest decimal text of the f32, not its widened f64 digits.
            HdbValue::REAL(v) => v.to_string().parse::<f64>().map_or(Value::Null, float_json),
            HdbValue::DOUBLE(v) => float_json(v),
            HdbValue::BOOLEAN(v) => Value::Bool(v),
            HdbValue::STRING(s) => text_json(s, batch)?,
            HdbValue::STR(s) => text_json(s.to_owned(), batch)?,
            HdbValue::DBSTRING(bytes) => {
                text_json(String::from_utf8_lossy(&bytes).into_owned(), batch)?
            }
            HdbValue::BINARY(bytes) | HdbValue::GEOMETRY(bytes) | HdbValue::POINT(bytes) => {
                binary_json(&bytes, batch)?
            }
            HdbValue::ASYNC_CLOB(lob) => {
                let size = lob.total_byte_length();
                if size > batch.max_text_bytes as u64 {
                    return Err(lob_too_big(
                        "CLOB",
                        size,
                        batch.max_text_bytes,
                        "max_text_bytes",
                    ));
                }
                Value::String(lob.into_string().await.map_err(|e| hana_error(&e))?)
            }
            HdbValue::ASYNC_NCLOB(lob) => {
                let size = lob.total_byte_length();
                if size > batch.max_text_bytes as u64 {
                    return Err(lob_too_big(
                        "NCLOB",
                        size,
                        batch.max_text_bytes,
                        "max_text_bytes",
                    ));
                }
                Value::String(lob.into_string().await.map_err(|e| hana_error(&e))?)
            }
            HdbValue::ASYNC_BLOB(lob) => {
                let size = lob.total_byte_length();
                if size > batch.max_binary_bytes as u64 {
                    return Err(lob_too_big(
                        "BLOB",
                        size,
                        batch.max_binary_bytes,
                        "max_binary_bytes",
                    ));
                }
                let bytes = lob.into_bytes().await.map_err(|e| hana_error(&e))?;
                Value::String(base64::engine::general_purpose::STANDARD.encode(bytes))
            }
            HdbValue::LONGDATE(v) => Value::String(v.to_string()),
            HdbValue::SECONDDATE(v) => Value::String(v.to_string()),
            HdbValue::DAYDATE(v) => Value::String(v.to_string()),
            HdbValue::SECONDTIME(v) => Value::String(v.to_string()),
            HdbValue::ARRAY(items) => {
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    out.push(value_json(item, batch).await?);
                }
                Value::Array(out)
            }
            HdbValue::ASYNC_LOBSTREAM(_) => {
                return Err(Error::Decode(
                    "hana: a value of a type the engine cannot read".into(),
                ));
            }
        })
    }
    .boxed()
}

/// Rows the first round trip of a read fetches, before any row's width is known.
const FIRST_FETCH_ROWS: u32 = 1;

/// The rows the next round trip fetches so its rows hold about `max_bytes` of
/// JSON, given `rows` recent rows that came to `bytes`. Never fewer than one row,
/// never more than `max_rows`, and never more than twice the round trip before,
/// so a run of narrow rows followed by wide ones is caught one doubling late.
fn next_fetch_rows(
    max_rows: usize,
    max_bytes: usize,
    current: u32,
    rows: usize,
    bytes: usize,
) -> u32 {
    let width = bytes.checked_div(rows).unwrap_or(0).max(1);
    let fit = (max_bytes / width).clamp(1, max_rows.max(1));
    let growth = usize::try_from(current)
        .unwrap_or(usize::MAX)
        .saturating_mul(2);
    u32::try_from(fit.min(growth.max(1))).unwrap_or(u32::MAX)
}

/// The SQL of one read and the text its placeholders are bound with, in order.
struct Statement {
    sql: String,
    params: Vec<String>,
}

/// One store on a HANA system.
pub struct HanaStore {
    url: Arc<Secret>,
    query: String,
    /// The key columns as configured, written into the SQL.
    keys: Vec<String>,
    /// The same keys as the row JSON names them, read for the mark.
    columns: Arc<[String]>,
    /// The key columns' kinds, read from the query's metadata on first use.
    key_kinds: OnceCell<Vec<KeyKind>>,
    batch: BatchSpec,
    lease: Arc<dyn Lease>,
    unit: Arc<str>,
}

impl HanaStore {
    /// A store over `query` on the system `url` resolves to. `keys` is empty
    /// for a dump.
    #[must_use]
    pub fn new(
        unit: &str,
        url: Arc<Secret>,
        query: String,
        keys: Vec<String>,
        batch: BatchSpec,
        lease: Arc<dyn Lease>,
    ) -> Self {
        let columns: Vec<String> = keys
            .iter()
            .map(|k| keyset::column(Dialect::Hana, k))
            .collect();
        Self {
            url,
            query,
            keys,
            columns: Arc::from(columns),
            key_kinds: OnceCell::new(),
            batch,
            lease,
            unit: Arc::from(unit),
        }
    }

    /// A fresh authenticated connection as the engine opens one, fetching
    /// `batch.max_rows` rows a round trip. Public so a test can seed and
    /// inspect a system through the same client, TLS and credential.
    ///
    /// # Errors
    ///
    /// Returns the credential, configuration or network error of the attempt.
    pub async fn connect(&self) -> Result<Connection> {
        let url = self.url.value().await?;
        if url.starts_with("hdbsqls://") {
            ensure_crypto_provider();
        }
        // The driver fetches by rows. The first round trip is one row and `blocks` resizes
        // each later one from the width of the recent rows, so a round trip holds about
        // `batch.max_bytes` of rows rather than `batch.max_rows` of the widest.
        let first = FIRST_FETCH_ROWS.min(u32::try_from(self.batch.max_rows).unwrap_or(u32::MAX));
        let configuration = ConnectionConfiguration::default().with_fetch_size(first.max(1));
        Connection::with_configuration(connect_params(url)?, &configuration)
            .await
            .map_err(|e| hana_error(&e))
    }

    /// The key columns' kinds as the query's own result metadata reports them,
    /// read once.
    async fn key_kinds(&self) -> Result<&[KeyKind]> {
        self.key_kinds
            .get_or_try_init(|| async {
                let connection = self.connect().await?;
                let probe = format!("SELECT * FROM ({}) AS dfe_tail WHERE 1 = 0", self.query);
                let result = connection.query(probe).await.map_err(|e| hana_error(&e))?;
                let metadata = result.metadata();
                self.columns
                    .iter()
                    .map(|column| {
                        let field = metadata
                            .iter()
                            .find(|field| field.displayname() == column)
                            .ok_or_else(|| {
                                Error::Config(format!(
                                    "key column `{column}` is not in the query's result; it has {:?}",
                                    metadata
                                        .iter()
                                        .map(FieldMetadata::displayname)
                                        .collect::<Vec<_>>()
                                ))
                            })?;
                        if field.is_nullable() {
                            return Err(Error::Config(format!(
                                "key column `{column}` is nullable in the query's result. A NULL key \
                                 never compares greater than a checkpoint, so its rows would be \
                                 skipped. Write `COALESCE({column}, <default>) AS {column}`, or key \
                                 on a NOT NULL column"
                            )));
                        }
                        Ok(KeyKind::of(field.type_id()))
                    })
                    .collect()
            })
            .await
            .map(Vec::as_slice)
    }

    /// The tail statement past `after`, its typed placeholders and the values
    /// that fill them in order.
    fn tail_statement(
        &self,
        kinds: &[KeyKind],
        after: Option<&[Value]>,
        limit: u32,
    ) -> Result<Statement> {
        let keys: Vec<&str> = self.keys.iter().map(String::as_str).collect();
        let placeholder = |i: usize| kinds[i].placeholder().to_owned();
        let sql = keyset::tail_sql(
            Dialect::Hana,
            &self.query,
            &keys,
            after.is_some(),
            limit,
            &placeholder,
        );
        let params = match after {
            None => Vec::new(),
            Some(values) => keyset::bind_order(Dialect::Hana, keys.len())
                .into_iter()
                .map(|i| kinds[i].text(&values[i]))
                .collect::<Result<Vec<_>>>()?,
        };
        Ok(Statement { sql, params })
    }

    /// Stream `statement` as blocks of NDJSON.
    fn blocks(&self, statement: Statement) -> BoxStream<'_, Result<LeasedBlock>> {
        /// Where a read is up to.
        struct Read {
            /// Held so the session outlives its result set, and resized as rows arrive.
            connection: Connection,
            rows: ResultSet,
            names: Vec<String>,
            /// The rows a round trip is fetching now.
            fetch: u32,
            /// Rows and JSON bytes read since the fetch size was last reviewed.
            recent_rows: usize,
            recent_bytes: usize,
            done: bool,
        }

        let start = async move {
            let connection = self.connect().await?;
            let rows = if statement.params.is_empty() {
                connection
                    .query(&statement.sql)
                    .await
                    .map_err(|e| hana_error(&e))?
            } else {
                let mut prepared = connection
                    .prepare(&statement.sql)
                    .await
                    .map_err(|e| hana_error(&e))?;
                let values = prepared
                    .parameter_descriptors()
                    .iter_in()
                    .zip(&statement.params)
                    .map(|(descriptor, text)| descriptor.parse_value(text))
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|e| {
                        Error::Cursor(format!("checkpoint does not fit its key column: {e}"))
                    })?;
                prepared
                    .execute_row(values)
                    .await
                    .map_err(|e| hana_error(&e))?
                    .into_result_set()
                    .map_err(|e| hana_error(&e))?
            };
            let names = rows
                .metadata()
                .iter()
                .map(|field| field.displayname().to_owned())
                .collect();
            tracing::debug!(unit = %self.unit, sql = %statement.sql, "hana statement");
            let fetch = connection.fetch_size().await;
            let read = Read {
                connection,
                rows,
                names,
                fetch,
                recent_rows: 0,
                recent_bytes: 0,
                done: false,
            };
            Ok::<_, Error>(
                futures::stream::unfold(read, move |mut read| async move {
                    if read.done {
                        return None;
                    }
                    let mut block: Vec<u8> = Vec::new();
                    let mut count = 0usize;
                    while count < self.batch.max_rows && block.len() < self.batch.max_bytes {
                        match read.rows.next_row().await {
                            Ok(Some(mut row)) => {
                                let before = block.len();
                                let mut object = Map::with_capacity(read.names.len());
                                for name in &read.names {
                                    let value = row.next_value().unwrap_or(HdbValue::NULL);
                                    match value_json(value, &self.batch).await {
                                        Ok(json) => {
                                            object.insert(name.clone(), json);
                                        }
                                        Err(e) => {
                                            read.done = true;
                                            return Some((Err(e), read));
                                        }
                                    }
                                }
                                if let Err(e) =
                                    serde_json::to_writer(&mut block, &Value::Object(object))
                                {
                                    read.done = true;
                                    let error = Error::Decode(format!("row to JSON: {e}"));
                                    return Some((Err(error), read));
                                }
                                block.push(b'\n');
                                count += 1;
                                read.recent_rows += 1;
                                read.recent_bytes += block.len() - before;
                                // Reviewed every `fetch` rows across blocks, from those rows alone.
                                if u32::try_from(read.recent_rows).is_ok_and(|n| n >= read.fetch) {
                                    let next = next_fetch_rows(
                                        self.batch.max_rows,
                                        self.batch.max_bytes,
                                        read.fetch,
                                        read.recent_rows,
                                        read.recent_bytes,
                                    );
                                    read.recent_rows = 0;
                                    read.recent_bytes = 0;
                                    if next != read.fetch {
                                        read.connection.set_fetch_size(next).await;
                                        read.fetch = next;
                                    }
                                }
                            }
                            Ok(None) => {
                                read.done = true;
                                break;
                            }
                            Err(e) => {
                                read.done = true;
                                return Some((Err(hana_error(&e)), read));
                            }
                        }
                    }
                    if block.is_empty() {
                        return None;
                    }
                    let leased = LeasedBlock::new(Bytes::from(block), Arc::clone(&self.lease));
                    Some((Ok(leased), read))
                })
                .boxed(),
            )
        };
        futures::stream::once(start).try_flatten().boxed()
    }
}

impl Store for HanaStore {
    fn dump(&self) -> RowStream<'_> {
        rows_of_blocks(
            self.blocks(Statement {
                sql: self.query.clone(),
                params: Vec::new(),
            }),
            None,
        )
    }

    fn tail(&self, after: Option<Vec<Value>>, limit: u32) -> RowStream<'_> {
        if self.keys.is_empty() {
            return futures::stream::once(async {
                Err(Error::Config("tail on a store with no key columns".into()))
            })
            .boxed();
        }
        if let Some(values) = &after
            && values.len() != self.keys.len()
        {
            let problem = format!(
                "checkpoint has {} values for {} key columns; the key list changed under a live \
                 checkpoint",
                values.len(),
                self.keys.len()
            );
            return futures::stream::once(async move { Err(Error::Cursor(problem)) }).boxed();
        }
        let start = async move {
            let kinds = self.key_kinds().await?;
            let statement = self.tail_statement(kinds, after.as_deref(), limit)?;
            Ok::<_, Error>(self.blocks(statement))
        };
        let blocks = futures::stream::once(start).try_flatten().boxed();
        rows_of_blocks(blocks, Some(Arc::clone(&self.columns)))
    }

    fn probe(&self) -> BoxFuture<'_, Result<()>> {
        async move {
            let connection = self.connect().await?;
            connection
                .query("SELECT 1 FROM DUMMY")
                .await
                .map_err(|e| hana_error(&e))?
                .into_single_value()
                .await
                .map_err(|e| hana_error(&e))?;
            Ok(())
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dfe_fetcher_core::batch::NoLease;
    use scalo::SensitiveString;
    use serde_json::json;

    fn store(keys: &[&str]) -> HanaStore {
        HanaStore::new(
            "audit",
            Arc::new(Secret::new(SensitiveString::from(
                "hdbsql://u:p@h:1".to_owned(),
            ))),
            "SELECT TIMESTAMP, CONNECTION_ID, USER_NAME FROM SYS.AUDIT_LOG".into(),
            keys.iter().map(|k| (*k).to_owned()).collect(),
            BatchSpec::default(),
            Arc::new(NoLease),
        )
    }

    #[test]
    fn a_percent_encoded_user_and_password_reach_the_driver_decoded() {
        let params = connect_params(
            "hdbsqls://DB%40ADMIN:p%40ss%3Aw%23rd%25@h.example:443?use_mozillas_root_certificates&db=HXE",
        )
        .unwrap();
        assert_eq!(params.dbuser(), "DB@ADMIN");
        assert_eq!(params.password().unsecure(), "p@ss:w#rd%");
        assert_eq!(params.host(), "h.example");
        assert_eq!(params.addr(), "h.example:443");
        assert!(params.is_tls());
        assert_eq!(params.dbname(), Some("HXE"));
        let plain = connect_params("hdbsql://SYSTEM:Secret1@h:39017").unwrap();
        assert!(!plain.is_tls());
        assert_eq!(plain.password().unsecure(), "Secret1");
    }

    #[test]
    fn a_url_the_driver_cannot_use_is_a_config_error_that_leaks_no_password() {
        for bad in [
            "jdbc:sap://h:1",
            "hdbsqls://u:hunter2@h:443",
            "hdbsql://u:hunter2@h",
        ] {
            let err = connect_params(bad).unwrap_err();
            assert!(matches!(err, Error::Config(_)), "{bad}: {err:?}");
            assert!(!err.to_string().contains("hunter2"), "{err}");
        }
    }

    #[test]
    fn a_round_trip_is_sized_to_the_byte_budget_and_clamped_to_the_row_cap() {
        // 100 rows of 1 KiB against a 64 KiB budget: 64 rows, within double the 40 before.
        assert_eq!(next_fetch_rows(5000, 65_536, 40, 100, 102_400), 64);
        // Narrow rows are held to max_rows.
        assert_eq!(next_fetch_rows(500, 4_194_304, 400, 100, 5_000), 500);
        // A row wider than the whole budget still fetches one.
        assert_eq!(next_fetch_rows(5000, 1_000, 16, 3, 30_000), 1);
        // No rows read yet reads as a 1 byte row, never a divide by zero.
        assert_eq!(next_fetch_rows(40, 1_000_000, 16, 0, 0), 32);
    }

    #[test]
    fn a_fetch_at_most_doubles_so_a_narrow_run_followed_by_wide_rows_is_caught_early() {
        // 16 narrow rows would fit 5000, but the next round trip is 32.
        let narrow = next_fetch_rows(5000, 4_194_304, 16, 16, 1_600);
        assert_eq!(narrow, 32);
        // A first round trip of one row reaches the cap in 13 doublings.
        let mut fetch = FIRST_FETCH_ROWS;
        let mut trips = 0;
        while fetch < 5000 {
            fetch = next_fetch_rows(5000, 4_194_304, fetch, 1, 100);
            trips += 1;
        }
        assert_eq!((fetch, trips), (5000, 13));
        // And it shrinks at once when the rows turn wide.
        assert_eq!(
            next_fetch_rows(5000, 4_194_304, 2048, 2048, 2048 * 8_000),
            524
        );
    }

    #[test]
    fn plain_key_names_fold_to_upper_case_as_the_columns_come_back() {
        let s = store(&["timestamp", "\"MixedCase\""]);
        assert_eq!(&*s.columns, ["TIMESTAMP", "MixedCase"]);
    }

    #[test]
    fn the_first_tail_page_orders_by_the_keys_and_binds_nothing() {
        let s = store(&["TIMESTAMP", "CONNECTION_ID"]);
        let kinds = [KeyKind::Timestamp, KeyKind::Integer];
        let st = s.tail_statement(&kinds, None, 100).unwrap();
        assert_eq!(
            st.sql,
            "SELECT * FROM (SELECT TIMESTAMP, CONNECTION_ID, USER_NAME FROM SYS.AUDIT_LOG) AS \
             dfe_tail ORDER BY TIMESTAMP, CONNECTION_ID LIMIT 100"
        );
        assert_eq!(st.params, [] as [String; 0]);
    }

    #[test]
    fn a_resumed_tail_page_expands_the_comparison_and_casts_a_timestamp_each_time_it_is_bound() {
        let s = store(&["TIMESTAMP", "CONNECTION_ID"]);
        let kinds = [KeyKind::Timestamp, KeyKind::Integer];
        let after = [json!("2026-11-20T01:02:03.1234567"), json!(300_123)];
        let st = s.tail_statement(&kinds, Some(&after), 50).unwrap();
        assert_eq!(
            st.sql,
            "SELECT * FROM (SELECT TIMESTAMP, CONNECTION_ID, USER_NAME FROM SYS.AUDIT_LOG) AS \
             dfe_tail WHERE TIMESTAMP > CAST(? AS TIMESTAMP) OR (TIMESTAMP = CAST(? AS TIMESTAMP) \
             AND CONNECTION_ID > ?) ORDER BY TIMESTAMP, CONNECTION_ID LIMIT 50"
        );
        assert_eq!(
            st.params.len(),
            3,
            "the leading key is bound once per disjunct"
        );
        assert_eq!(
            st.params,
            [
                "2026-11-20 01:02:03.1234567",
                "2026-11-20 01:02:03.1234567",
                "300123"
            ]
        );
    }

    #[test]
    fn a_null_or_container_checkpoint_value_is_a_cursor_error() {
        let s = store(&["ID"]);
        for bad in [json!(null), json!([1]), json!({"a": 1})] {
            let err = s
                .tail_statement(&[KeyKind::Integer], Some(&[bad]), 10)
                .err()
                .expect("refused");
            assert!(matches!(err, Error::Cursor(_)), "{err:?}");
        }
    }

    #[test]
    fn decimals_are_numbers_only_when_they_are_exact() {
        assert_eq!(decimal_json("42"), json!(42));
        assert_eq!(decimal_json("-3.25"), json!(-3.25));
        assert_eq!(
            decimal_json("12345678901234567890.123456789"),
            json!("12345678901234567890.123456789")
        );
    }

    #[test]
    fn a_value_over_a_batch_cap_fails_naming_the_cap() {
        let batch = BatchSpec {
            max_text_bytes: 4,
            max_binary_bytes: 2,
            ..BatchSpec::default()
        };
        let err = text_json("too long".into(), &batch)
            .unwrap_err()
            .to_string();
        assert!(err.contains("batch.max_text_bytes"), "{err}");
        let err = binary_json(&[1, 2, 3], &batch).unwrap_err().to_string();
        assert!(err.contains("batch.max_binary_bytes"), "{err}");
        assert_eq!(binary_json(&[1, 2], &batch).unwrap(), json!("AQI="));
    }

    #[tokio::test]
    async fn scalars_map_to_their_json_forms() {
        let batch = BatchSpec::default();
        let cases: Vec<(HdbValue<'static>, Value)> = vec![
            (HdbValue::NULL, Value::Null),
            (HdbValue::BOOLEAN(true), json!(true)),
            (HdbValue::INT(-7), json!(-7)),
            (
                HdbValue::BIGINT(9_007_199_254_740_993),
                json!(9_007_199_254_740_993_i64),
            ),
            (HdbValue::REAL(0.1), json!(0.1)),
            (HdbValue::STRING("x".into()), json!("x")),
            (HdbValue::BINARY(vec![1, 2]), json!("AQI=")),
        ];
        for (value, expected) in cases {
            assert_eq!(value_json(value, &batch).await.unwrap(), expected);
        }
    }
}
