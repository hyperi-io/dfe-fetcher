// Project:   dfe-fetcher
// File:      crates/db/src/store.rs
// Purpose:   The contract every database engine implements: dump and keyset tail as row streams
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The store contract.

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use dfe_fetcher_core::RowStream;
use dfe_fetcher_core::error::Result;

/// SQL dialect, which decides the keyset predicate form, the streaming knob
/// the connection string must carry, and the session statements run after
/// connecting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Dialect {
    /// PostgreSQL and wire-compatible engines (Redshift); psqlodbc needs
    /// `UseDeclareFetch=1` to stream instead of buffering the result set.
    Postgres,
    /// MySQL and MariaDB; MariaDB Connector/ODBC needs `NO_CACHE=1` (and
    /// `FORWARDONLY=1`) to stream instead of buffering the result set.
    Mysql,
    /// Microsoft SQL Server; no row-value comparison, streams by default.
    Mssql,
    /// Oracle; no row-value comparison, streams by default.
    Oracle,
    /// ClickHouse over ODBC or the native client.
    Clickhouse,
    /// SQLite.
    Sqlite,
    /// Snowflake.
    Snowflake,
    /// BigQuery.
    Bigquery,
    /// Databricks.
    Databricks,
}

impl Dialect {
    /// Whether the engine accepts `(a, b) > (?, ?)`; the others take the
    /// expanded `a > ? OR (a = ? AND b > ?)` form.
    #[must_use]
    pub const fn supports_row_values(self) -> bool {
        !matches!(self, Dialect::Mssql | Dialect::Oracle)
    }

    /// The config spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Dialect::Postgres => "postgres",
            Dialect::Mysql => "mysql",
            Dialect::Mssql => "mssql",
            Dialect::Oracle => "oracle",
            Dialect::Clickhouse => "clickhouse",
            Dialect::Sqlite => "sqlite",
            Dialect::Snowflake => "snowflake",
            Dialect::Bigquery => "bigquery",
            Dialect::Databricks => "databricks",
        }
    }

    /// The connection-string knob without which the ODBC driver caches the
    /// whole result set client-side, defeating the block cursor; `None` for a
    /// driver that streams by default.
    #[must_use]
    pub const fn streaming_knob(self) -> Option<&'static str> {
        match self {
            Dialect::Postgres => Some("UseDeclareFetch=1"),
            Dialect::Mysql => Some("NO_CACHE=1"),
            _ => None,
        }
    }

    /// Statements run on a fresh connection before the query, so zoned
    /// timestamps come back in UTC and the JSON can say so.
    #[must_use]
    pub const fn session_prelude(self) -> &'static [&'static str] {
        match self {
            Dialect::Postgres => &["SET TIME ZONE 'UTC'"],
            Dialect::Mysql => &["SET time_zone = '+00:00'"],
            _ => &[],
        }
    }
}

/// A store an engine can dump or tail.
///
/// Rows are NDJSON objects (one per database row, columns as keys) so the
/// engine feeds the same framer every REST unit uses. Both streams are pulled:
/// an engine fetches its next block only when the driver polls, holds at most
/// the pump's bounded blocks in memory, and reports the store's real error
/// through [`dfe_fetcher_core::Error`] with the SQLSTATE class mapped to the
/// `api_errors_total{code}` vocabulary.
///
/// # Errors
///
/// `dump` and `tail` yield `Err` for a connection, query or conversion failure;
/// the driver aborts the tick without a checkpoint. `probe` returns `Err` when
/// the connection cannot be opened.
pub trait Store: Send + Sync {
    /// The whole store, in the engine's own order.
    fn dump(&self) -> RowStream<'_>;

    /// Rows whose ordering key sorts after `after` (the last committed tuple),
    /// in key order, at most `limit` per call; each row carries
    /// `Mark::Keyset` with its own tuple.
    ///
    /// `after` is owned: the shape re-issues this once per page within a tick,
    /// each page starting past the last row of the one before.
    fn tail(&self, after: Option<Vec<Value>>, limit: u32) -> RowStream<'_>;

    /// Open a connection and run a trivial statement.
    fn probe(&self) -> BoxFuture<'_, Result<()>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_mssql_and_oracle_lack_row_value_comparison() {
        assert!(Dialect::Postgres.supports_row_values());
        assert!(Dialect::Mysql.supports_row_values());
        assert!(Dialect::Clickhouse.supports_row_values());
        assert!(!Dialect::Mssql.supports_row_values());
        assert!(!Dialect::Oracle.supports_row_values());
    }

    #[test]
    fn dialect_names_are_snake_case_on_the_wire() {
        let d: Dialect = serde_json::from_str("\"mssql\"").unwrap();
        assert_eq!(d, Dialect::Mssql);
        assert!(serde_json::from_str::<Dialect>("\"MSSQL\"").is_err());
        assert_eq!(
            serde_json::to_string(&Dialect::Bigquery).unwrap(),
            "\"bigquery\""
        );
        assert_eq!(Dialect::Bigquery.as_str(), "bigquery");
    }

    #[test]
    fn only_the_caching_drivers_have_a_streaming_knob_and_a_utc_prelude() {
        assert_eq!(
            Dialect::Postgres.streaming_knob(),
            Some("UseDeclareFetch=1")
        );
        assert_eq!(Dialect::Mysql.streaming_knob(), Some("NO_CACHE=1"));
        assert_eq!(Dialect::Mssql.streaming_knob(), None);
        assert_eq!(Dialect::Clickhouse.streaming_knob(), None);
        assert_eq!(Dialect::Postgres.session_prelude(), ["SET TIME ZONE 'UTC'"]);
        assert_eq!(
            Dialect::Mysql.session_prelude(),
            ["SET time_zone = '+00:00'"]
        );
        assert!(Dialect::Oracle.session_prelude().is_empty());
    }
}
