// Project:   dfe-fetcher
// File:      crates/db/src/clickhouse.rs
// Purpose:   The ClickHouse engine: server-side JSON streamed over HTTP with bound parameters
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The ClickHouse engine.
//!
//! One [`ChStore`] per configured store over the official HTTP client. A dump
//! asks the server for `FORMAT JSONEachRow`, so ClickHouse types every column
//! itself, and reads the body chunk by chunk: an unpolled body stalls the TCP
//! window, which is the backpressure. Parameters travel as `param_<name>`
//! query arguments and are bound server-side, never interpolated. The
//! `clickhouse-dfe` extensions add the ping the probe uses and the typed
//! server exception the error mapping reads.
//!
//! # Type mapping
//!
//! ClickHouse's own JSON output, with two settings pinned:
//! `output_format_json_quote_64bit_integers=0` so `Int64`/`UInt64` land as
//! numbers, and `date_time_output_format=iso` so `DateTime`/`DateTime64`
//! land as RFC 3339 UTC (`2026-01-01T00:00:00Z`). `Nullable` columns land as
//! `null`. `String` is text as stored (ClickHouse has no binary type).
//!
//! # Keyset tail
//!
//! ClickHouse placeholders are typed: `{name:Type}`, filled server-side from
//! `param_<name>` query arguments. The key columns' types are not in the
//! config; the store reads them from the server once, with
//! `DESCRIBE (query)`, the first time it tails, and keeps them for its life.
//! Read rather than declared because the operator already wrote the query
//! and the key names, a second copy of the schema in the config goes stale
//! silently, and the spelling `DESCRIBE` returns is exactly what the
//! placeholder grammar accepts. `Nullable(..)` and `LowCardinality(..)` are
//! unwrapped: a key column cannot be null and a parameter has no cardinality.
//! A `DateTime` placeholder is declared without the column's zone and its
//! value bound without the `Z` the row JSON carries: the parameter parser
//! takes the ISO form but no zone suffix, and with `session_timezone` pinned
//! to UTC a zone-less parse is the same instant the row reported, whatever
//! zone the column renders in.

use std::sync::Arc;

use clickhouse::Client;
use clickhouse::query::BytesCursor;
use clickhouse_dfe::{ClientExt, ServerException};
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryStreamExt};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::OnceCell;

use dfe_fetcher_core::RowStream;
use dfe_fetcher_core::batch::Lease;
use dfe_fetcher_core::error::{Error, Result};

use crate::keyset;
use crate::lines::{LeasedBlock, rows_of_blocks};
use crate::secret::Secret;
use crate::store::{Dialect, Store};

/// The framework error for a ClickHouse failure: the server's own code
/// decides the class.
fn ch_error(e: clickhouse::error::Error) -> Error {
    if let Some(exc) = ServerException::parse(&e) {
        let name = exc.name.as_deref().unwrap_or("");
        return match exc.code {
            // UNKNOWN_USER, WRONG_PASSWORD, ACCESS_DENIED, AUTHENTICATION_FAILED.
            192 | 193 | 497 | 516 => {
                Error::Credential(format!("clickhouse ({} {name}): {}", exc.code, exc.message))
            }
            _ => Error::Source(format!("clickhouse ({} {name}): {}", exc.code, exc.message)),
        };
    }
    match e {
        clickhouse::error::Error::TimedOut => Error::Source("clickhouse: timeout".into()),
        clickhouse::error::Error::Network(inner) => {
            Error::Source(format!("clickhouse network: {inner}"))
        }
        other => Error::Source(format!("clickhouse: {other}")),
    }
}

/// A client from a URL of the form `http[s]://[user[:password]@]host[:port][/database]`.
///
/// # Errors
///
/// Returns [`Error::Config`] when the URL does not parse or is not HTTP.
pub fn client_from_url(url: &str) -> Result<Client> {
    let parsed = url::Url::parse(url)
        .map_err(|e| Error::Config(format!("clickhouse connection_string: {e}")))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(Error::Config(format!(
            "clickhouse connection_string: scheme `{}` is not http or https",
            parsed.scheme()
        )));
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| Error::Config("clickhouse connection_string: no host".into()))?;
    let mut base = format!("{}://{host}", parsed.scheme());
    if let Some(port) = parsed.port() {
        base.push(':');
        base.push_str(&port.to_string());
    }
    let mut client = Client::default()
        .with_url(base)
        .with_setting("output_format_json_quote_64bit_integers", "0")
        .with_setting("date_time_output_format", "iso")
        .with_setting("session_timezone", "UTC");
    if !parsed.username().is_empty() {
        client = client.with_user(parsed.username());
    }
    if let Some(password) = parsed.password() {
        client = client.with_password(password);
    }
    let database = parsed.path().trim_matches('/');
    if !database.is_empty() {
        client = client.with_database(database);
    }
    Ok(client)
}

/// The body chunks of one query as leased blocks.
struct Chunks {
    cursor: BytesCursor,
    lease: Arc<dyn Lease>,
    done: bool,
}

impl Chunks {
    async fn next(&mut self) -> Option<Result<LeasedBlock>> {
        if self.done {
            return None;
        }
        match self.cursor.next().await {
            Ok(Some(chunk)) => Some(Ok(LeasedBlock::new(chunk, Arc::clone(&self.lease)))),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(e) => {
                self.done = true;
                Some(Err(ch_error(e)))
            }
        }
    }
}

/// One row of `DESCRIBE`: a column and its declared type.
#[derive(Debug, Deserialize)]
struct Described {
    name: String,
    #[serde(rename = "type")]
    ty: String,
}

/// The type a placeholder is declared with: the column's, without the
/// `Nullable` and `LowCardinality` wrappers a parameter cannot carry and
/// without a `DateTime` zone, so the value parses in the session's UTC.
fn placeholder_type(declared: &str) -> String {
    let mut ty = declared.trim();
    loop {
        let inner = ty
            .strip_prefix("Nullable(")
            .or_else(|| ty.strip_prefix("LowCardinality("))
            .and_then(|rest| rest.strip_suffix(')'));
        match inner {
            Some(inner) => ty = inner.trim(),
            None => break,
        }
    }
    if let Some(args) = ty
        .strip_prefix("DateTime64(")
        .and_then(|r| r.strip_suffix(')'))
    {
        let precision = args.split(',').next().unwrap_or("").trim();
        return format!("DateTime64({precision})");
    }
    if ty.starts_with("DateTime(") {
        return "DateTime".to_owned();
    }
    ty.to_owned()
}

/// Whether a placeholder type takes a timestamp, whose bound value drops
/// the `Z` the row JSON carries.
fn is_datetime(placeholder: &str) -> bool {
    placeholder.starts_with("DateTime")
}

/// The value a placeholder is bound with.
fn parameter_value(placeholder: &str, value: Value) -> Result<Value> {
    match value {
        Value::Null => Err(Error::Cursor(
            "keyset value is null; a tail key column must be NOT NULL".into(),
        )),
        Value::Array(_) | Value::Object(_) => Err(Error::Cursor(
            "keyset value is a JSON container, not a scalar".into(),
        )),
        Value::String(s) if is_datetime(placeholder) => Ok(Value::String(
            s.strip_suffix('Z').map_or_else(|| s.clone(), str::to_owned),
        )),
        scalar => Ok(scalar),
    }
}

/// One store on a ClickHouse HTTP endpoint.
pub struct ChStore {
    url: Arc<Secret>,
    client: OnceCell<Client>,
    query: String,
    /// The key columns as configured, written into the SQL.
    keys: Vec<String>,
    /// The same keys as the row JSON names them, read for the mark.
    columns: Arc<[String]>,
    /// The key columns' placeholder types, read from the server on first use.
    key_types: OnceCell<Vec<String>>,
    lease: Arc<dyn Lease>,
    unit: Arc<str>,
}

impl ChStore {
    /// A store over `query` on the endpoint `url` resolves to; `keys` is
    /// empty for a dump.
    #[must_use]
    pub fn new(
        unit: &str,
        url: Arc<Secret>,
        query: String,
        keys: Vec<String>,
        lease: Arc<dyn Lease>,
    ) -> Self {
        let columns: Vec<String> = keys
            .iter()
            .map(|k| keyset::column(Dialect::Clickhouse, k))
            .collect();
        Self {
            url,
            client: OnceCell::new(),
            query,
            keys,
            columns: Arc::from(columns),
            key_types: OnceCell::new(),
            lease,
            unit: Arc::from(unit),
        }
    }

    async fn client(&self) -> Result<&Client> {
        self.client
            .get_or_try_init(|| async { client_from_url(self.url.value().await?) })
            .await
    }

    /// The key columns' types as `DESCRIBE (query)` reports them, read once.
    async fn key_types(&self) -> Result<&[String]> {
        self.key_types
            .get_or_try_init(|| async {
                let client = self.client().await?;
                let body = client
                    .query_raw(&format!("DESCRIBE ({})", self.query))
                    .fetch_bytes("JSONEachRow")
                    .map_err(ch_error)?
                    .collect()
                    .await
                    .map_err(ch_error)?;
                let described: Vec<Described> = body
                    .split(|b| *b == b'\n')
                    .filter(|line| !line.is_empty())
                    .map(|line| {
                        serde_json::from_slice(line)
                            .map_err(|e| Error::Decode(format!("DESCRIBE row: {e}")))
                    })
                    .collect::<Result<_>>()?;
                self.columns
                    .iter()
                    .map(|column| {
                        described
                            .iter()
                            .find(|d| d.name == *column)
                            .map(|d| placeholder_type(&d.ty))
                            .ok_or_else(|| {
                                Error::Config(format!(
                                    "key column `{column}` is not in the query's result; DESCRIBE names {:?}",
                                    described.iter().map(|d| d.name.as_str()).collect::<Vec<_>>()
                                ))
                            })
                    })
                    .collect()
            })
            .await
            .map(Vec::as_slice)
    }

    /// The tail statement with typed placeholders `{k<i>:Type}` per key.
    fn tail_sql(&self, key_types: &[String], has_after: bool, limit: u32) -> String {
        let keys: Vec<&str> = self.keys.iter().map(String::as_str).collect();
        let placeholder = |i: usize| format!("{{k{i}:{}}}", key_types[i]);
        keyset::tail_sql(
            Dialect::Clickhouse,
            &self.query,
            &keys,
            has_after,
            limit,
            &placeholder,
        )
    }

    /// Stream the JSONEachRow body of `sql`, chunk by chunk, with `params`
    /// bound server-side as `k0`, `k1`, ... in their placeholder form.
    fn blocks(
        &self,
        sql: String,
        params: Vec<Value>,
    ) -> futures::stream::BoxStream<'_, Result<LeasedBlock>> {
        let start = async move {
            let client = self.client().await?;
            let mut query = client.query_raw(&sql);
            for (i, value) in params.into_iter().enumerate() {
                query = query.param(&format!("k{i}"), value);
            }
            let cursor = query.fetch_bytes("JSONEachRow").map_err(ch_error)?;
            let chunks = Chunks {
                cursor,
                lease: Arc::clone(&self.lease),
                done: false,
            };
            Ok::<_, Error>(
                futures::stream::unfold(chunks, |mut c| async move {
                    c.next().await.map(|item| (item, c))
                })
                .boxed(),
            )
        };
        futures::stream::once(start).try_flatten().boxed()
    }
}

impl Store for ChStore {
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
            let key_types = self.key_types().await?;
            let sql = self.tail_sql(key_types, after.is_some(), limit);
            let params = after
                .unwrap_or_default()
                .into_iter()
                .zip(key_types)
                .map(|(value, placeholder)| parameter_value(placeholder, value))
                .collect::<Result<Vec<Value>>>()?;
            tracing::debug!(unit = %self.unit, sql, "clickhouse tail statement");
            Ok::<_, Error>(self.blocks(sql, params))
        };
        let blocks = futures::stream::once(start).try_flatten().boxed();
        rows_of_blocks(blocks, Some(Arc::clone(&self.columns)))
    }

    fn probe(&self) -> BoxFuture<'_, Result<()>> {
        async move { self.client().await?.ping().await.map_err(ch_error) }.boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_url_carries_identity_database_and_the_json_settings() {
        let client = client_from_url("https://reader:s3cret@ch.example:8443/audit").unwrap();
        assert_eq!(client.database(), Some("audit"));
        assert_eq!(
            client.get_setting("output_format_json_quote_64bit_integers"),
            Some("0")
        );
        assert_eq!(client.get_setting("date_time_output_format"), Some("iso"));
        assert_eq!(
            client.get_setting("session_timezone"),
            Some("UTC"),
            "a zone-less DateTime parameter parses as the UTC instant the row reported"
        );
        let bare = client_from_url("http://localhost:8123").unwrap();
        assert_eq!(bare.database(), None);
    }

    #[test]
    fn the_tail_statement_declares_each_key_placeholder_with_the_described_type() {
        use dfe_fetcher_core::batch::NoLease;
        use scalo::SensitiveString;
        let store = ChStore::new(
            "audit",
            Arc::new(Secret::new(SensitiveString::from("http://x".to_owned()))),
            "SELECT ts, id, body FROM audit".into(),
            vec!["ts".into(), "id".into()],
            Arc::new(NoLease),
        );
        let types = vec!["DateTime64(3)".to_owned(), "UInt64".to_owned()];
        assert_eq!(
            store.tail_sql(&types, true, 10),
            "SELECT * FROM (SELECT ts, id, body FROM audit) AS dfe_tail WHERE (ts, id) > \
             ({k0:DateTime64(3)}, {k1:UInt64}) ORDER BY ts, id LIMIT 10"
        );
        assert_eq!(
            store.tail_sql(&types, false, 10),
            "SELECT * FROM (SELECT ts, id, body FROM audit) AS dfe_tail ORDER BY ts, id LIMIT 10"
        );
    }

    #[test]
    fn placeholder_types_drop_the_wrappers_and_zones_a_parameter_cannot_carry() {
        assert_eq!(placeholder_type("UInt64"), "UInt64");
        assert_eq!(placeholder_type("Nullable(String)"), "String");
        assert_eq!(placeholder_type("LowCardinality(String)"), "String");
        assert_eq!(
            placeholder_type("LowCardinality(Nullable(String))"),
            "String"
        );
        assert_eq!(
            placeholder_type("DateTime64(3, 'Asia/Tokyo')"),
            "DateTime64(3)"
        );
        assert_eq!(placeholder_type("DateTime64(6)"), "DateTime64(6)");
        assert_eq!(placeholder_type("DateTime('UTC')"), "DateTime");
        assert_eq!(placeholder_type("DateTime"), "DateTime");
        assert_eq!(placeholder_type("Nullable(DateTime64(3))"), "DateTime64(3)");
        assert_eq!(placeholder_type("Date"), "Date");
    }

    #[test]
    fn a_timestamp_value_binds_without_its_zone_suffix_and_containers_are_refused() {
        assert_eq!(
            parameter_value(
                "DateTime64(3)",
                serde_json::json!("2026-03-01T00:00:00.000Z")
            )
            .unwrap(),
            serde_json::json!("2026-03-01T00:00:00.000")
        );
        assert_eq!(
            parameter_value("DateTime", serde_json::json!("2026-03-01T00:00:00Z")).unwrap(),
            serde_json::json!("2026-03-01T00:00:00")
        );
        assert_eq!(
            parameter_value("String", serde_json::json!("2026-03-01T00:00:00Z")).unwrap(),
            serde_json::json!("2026-03-01T00:00:00Z"),
            "a string key keeps its bytes"
        );
        assert_eq!(
            parameter_value("UInt64", serde_json::json!(7)).unwrap(),
            serde_json::json!(7)
        );
        assert!(matches!(
            parameter_value("UInt64", Value::Null),
            Err(Error::Cursor(_))
        ));
        assert!(matches!(
            parameter_value("String", serde_json::json!({"a": 1})),
            Err(Error::Cursor(_))
        ));
    }

    #[test]
    fn a_non_http_url_is_a_config_error() {
        assert!(matches!(
            client_from_url("tcp://localhost:9000"),
            Err(Error::Config(_))
        ));
        assert!(matches!(
            client_from_url("not a url"),
            Err(Error::Config(_))
        ));
    }

    #[test]
    fn server_exception_codes_map_to_credential_or_source() {
        let denied = clickhouse::error::Error::BadResponse(
            "Code: 516. DB::Exception: default: Authentication failed. (AUTHENTICATION_FAILED) (version 26.3.1)".into(),
        );
        assert!(matches!(ch_error(denied), Error::Credential(_)));
        let missing = clickhouse::error::Error::BadResponse(
            "Code: 60. DB::Exception: Unknown table expression identifier 'nope'. (UNKNOWN_TABLE) (version 26.3.1)".into(),
        );
        let mapped = ch_error(missing);
        assert!(matches!(mapped, Error::Source(_)));
        assert!(mapped.to_string().contains("60 UNKNOWN_TABLE"));
        assert_eq!(
            ch_error(clickhouse::error::Error::TimedOut).api_error_code(),
            "timeout"
        );
    }
}
