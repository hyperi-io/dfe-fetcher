// Project:   dfe-fetcher
// File:      crates/db/src/config.rs
// Purpose:   The `sources.db.<id>` grammar: engine, connection, batch bounds, stores
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The database instance grammar.
//!
//! One [`DbInstance`] per `sources.db.<id>` entry: the engine and its
//! connection string (a credential spec, resolved on first use), the
//! per-fetch batch bounds, and the stores -- each a query the engine dumps
//! whole or tails by keyset, or for MongoDB a collection dumped whole or
//! tailed by change stream. Everything that can be checked without a
//! connection is checked by [`DbInstance::validate`], with the field path, so
//! a bad instance fails at load rather than at the first tick.

use std::collections::BTreeSet;

use scalo::SensitiveString;
use serde::{Deserialize, Serialize};

use dfe_fetcher_core::UnitShape;
use dfe_fetcher_core::batch::AccumulateConfig;
use dfe_fetcher_core::secret::{is_reference, spec_issue};

use crate::store::Dialect;

/// Which client library an instance speaks through.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum Engine {
    /// unixODBC plus the engine's own ODBC driver; every SQL dialect.
    #[default]
    Odbc,
    /// The ClickHouse HTTP interface, server-side JSON.
    Clickhouse,
    /// The official MongoDB driver: a collection dumped by `find`, tailed by
    /// change stream or by `_id`.
    Mongodb,
}

impl Engine {
    /// The config spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Engine::Odbc => "odbc",
            Engine::Clickhouse => "clickhouse",
            Engine::Mongodb => "mongodb",
        }
    }

    /// The app feature that compiles this engine in.
    #[must_use]
    pub const fn feature(self) -> &'static str {
        match self {
            Engine::Odbc => "db-odbc",
            Engine::Clickhouse => "db-clickhouse",
            Engine::Mongodb => "db-mongodb",
        }
    }

    /// Whether this binary was built with the engine.
    #[must_use]
    pub const fn is_built(self) -> bool {
        match self {
            Engine::Odbc => cfg!(feature = "odbc"),
            Engine::Clickhouse => cfg!(feature = "clickhouse"),
            Engine::Mongodb => cfg!(feature = "mongodb"),
        }
    }

    /// Whether the engine speaks SQL: a store is a query with a dialect.
    #[must_use]
    pub const fn is_sql(self) -> bool {
        !matches!(self, Engine::Mongodb)
    }
}

/// How a MongoDB tail follows its collection.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum TailMode {
    /// A change stream: every insert, update, replace and delete as an event,
    /// resumed from the last committed resume token; needs a replica set.
    #[default]
    ChangeStream,
    /// A keyset over `_id`: documents past the last committed `_id` in `_id`
    /// order, for a standalone server with no oplog.
    Keyset,
}

impl TailMode {
    /// The config spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            TailMode::ChangeStream => "change_stream",
            TailMode::Keyset => "keyset",
        }
    }
}

/// Whether a store is dumped whole or tailed by keyset.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum StoreShape {
    /// The whole result set every tick, wrapped in the snapshot envelope.
    #[default]
    Dump,
    /// Rows past the last committed key tuple, in key order.
    Tail,
}

impl StoreShape {
    /// The driver-facing unit shape: a tail is an incremental unit with a
    /// keyset checkpoint.
    #[must_use]
    pub const fn unit_shape(self) -> UnitShape {
        match self {
            StoreShape::Dump => UnitShape::Dump,
            StoreShape::Tail => UnitShape::Incremental,
        }
    }
}

/// Per-fetch bounds on the block cursor.
///
/// A block is what one round trip to the engine fetches; the pump holds at
/// most two blocks plus the one being read, so `3 x max_bytes` is the
/// store's memory ceiling before the batcher's own bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct BatchSpec {
    /// Rows per block.
    pub max_rows: usize,
    /// Transit-buffer bytes per block; whichever of the two bounds is smaller wins.
    pub max_bytes: usize,
    /// Largest text value the transit buffer accepts; a longer value fails the
    /// tick rather than being truncated.
    pub max_text_bytes: usize,
    /// Largest binary value the transit buffer accepts, likewise.
    pub max_binary_bytes: usize,
}

impl Default for BatchSpec {
    fn default() -> Self {
        Self {
            max_rows: 5000,
            max_bytes: 4 * 1024 * 1024,
            max_text_bytes: 64 * 1024,
            max_binary_bytes: 1024 * 1024,
        }
    }
}

/// One store: a query and how it is fetched, or for MongoDB a collection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct StoreSpec {
    /// Unit name: the second half of `_source_fetcher` and of a dump's `store`.
    pub unit: String,
    /// Dump or tail.
    pub shape: StoreShape,
    /// SQL engines: the SELECT, without an ORDER BY or LIMIT for a tail; the
    /// framework appends the keyset predicate, the ordering and the limit.
    pub query: String,
    /// SQL tail only: the ordering key columns, in precedence order, each as
    /// it appears in the query's result (delimit a case-sensitive name in
    /// the dialect's own form).
    pub key: Vec<String>,
    /// Tail only: rows per page.
    pub limit: u32,
    /// Tail only: pages of `limit` rows read per tick before the rest of the
    /// backlog waits for the next one. A tick ends early on the first short
    /// page, so this bounds only a store that is behind.
    pub max_pages_per_tick: u32,
    /// JSON pointer to the row's identity, for the oversize stub and logs.
    pub row_key: Option<String>,
    /// MongoDB only; refused for a SQL engine.
    pub mongodb: Option<MongoStoreSpec>,
}

impl Default for StoreSpec {
    fn default() -> Self {
        Self {
            unit: String::new(),
            shape: StoreShape::Dump,
            query: String::new(),
            key: Vec::new(),
            limit: 5000,
            max_pages_per_tick: 10,
            row_key: None,
            mongodb: None,
        }
    }
}

/// What the MongoDB engine needs of a store, which is a collection rather than
/// a query.
///
/// It sits in its own block so the keys every engine shares stay one
/// vocabulary: `filter` here is a query document sent to the server, distinct
/// from the instance-level `filter`, which is a CEL expression over each row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MongoStoreSpec {
    /// The database holding the collection.
    pub database: String,
    /// The collection.
    pub collection: String,
    /// A query document over what the store yields -- the documents of a dump
    /// or keyset tail, the change events of a change stream.
    pub filter: Option<serde_json::Map<String, serde_json::Value>>,
    /// Tail only: change stream (the default) or keyset over `_id`.
    pub tail: Option<TailMode>,
}

impl MongoStoreSpec {
    /// The tail mode in force: the configured one, else the change stream,
    /// because it is the lossless one and a keyset over `_id` only sees
    /// inserts.
    #[must_use]
    pub fn tail_mode(&self) -> TailMode {
        self.tail.unwrap_or_default()
    }
}

/// One `sources.db.<id>` instance.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct DbInstance {
    /// Whether the instance runs.
    pub enabled: bool,
    /// The client library.
    pub engine: Engine,
    /// The SQL dialect; required for `odbc`, implied for `clickhouse`.
    pub dialect: Option<Dialect>,
    /// The connection string or URL as a credential spec (`vault:...`, also
    /// spelled `bao:` or `openbao:`, `env:VAR`, `file:<path>`, or a literal),
    /// resolved on first use and never logged.
    pub connection_string: SensitiveString,
    /// Fetch interval; the scheduler default when unset.
    pub interval_secs: Option<u64>,
    /// Topic base; dump units land on `<topic>-<unit>`, tails on `<topic>`.
    pub topic: String,
    /// CEL keep-filter over each row, hot-reloaded.
    pub filter: Option<String>,
    /// Block cursor bounds.
    pub batch: BatchSpec,
    /// The stores, in tick order.
    pub stores: Vec<StoreSpec>,
    /// Batch bounds for this instance; the deployment's when unset.
    pub accumulate: Option<AccumulateConfig>,
}

impl Default for DbInstance {
    fn default() -> Self {
        Self {
            enabled: true,
            engine: Engine::Odbc,
            dialect: None,
            connection_string: SensitiveString::default(),
            interval_secs: None,
            topic: String::new(),
            filter: None,
            batch: BatchSpec::default(),
            stores: Vec::new(),
            accumulate: None,
        }
    }
}

/// The value of one option in a MongoDB URI's query string: the name is
/// matched without regard to case and the value percent-decoded, both as the
/// driver does before it reads them.
fn uri_option(uri: &str, name: &str) -> Option<String> {
    let (_, query) = uri.split_once('?')?;
    query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| {
            percent_encoding::percent_decode_str(v)
                .decode_utf8_lossy()
                .into_owned()
        })
}

/// A MongoDB auth mechanism this binary cannot perform, named with the
/// reason, or `None` when the driver can be left to it.
///
/// `MONGODB-AWS` and `GSSAPI` sit behind driver build features this crate
/// does not enable, and the driver refuses `MONGODB-CR` outright as
/// deprecated. `MONGODB-OIDC` needs a token callback, which the driver
/// supplies only for the `azure`, `gcp` and `k8s` environments; any other
/// form expects the application to hand one over, and this one does not.
/// Left to the driver these fail at connect naming its feature rather than
/// the binary, so a literal string is checked at load and a resolved
/// reference at connect. The mechanism is compared without regard to case
/// so a mis-cased spelling is refused here rather than at connect; the
/// property key and value are the driver's to match exactly.
#[must_use]
pub fn mongodb_auth_issue(uri: &str) -> Option<String> {
    let mechanism = uri_option(uri, "authMechanism")?.to_ascii_uppercase();
    match mechanism.as_str() {
        "MONGODB-AWS" | "GSSAPI" => Some(format!("`{mechanism}` is not built into this binary")),
        "MONGODB-CR" => Some("`MONGODB-CR` is refused by the driver as deprecated".to_owned()),
        "MONGODB-OIDC" => {
            let environment = uri_option(uri, "authMechanismProperties").and_then(|props| {
                props
                    .split(',')
                    .filter_map(|p| p.split_once(':'))
                    .find(|(k, _)| *k == "ENVIRONMENT")
                    .map(|(_, v)| v.to_owned())
            });
            match environment.as_deref() {
                Some("azure" | "gcp" | "k8s") => None,
                _ => Some(
                    "`MONGODB-OIDC` needs a token callback this binary does not provide; set `authMechanismProperties=ENVIRONMENT:` to `azure`, `gcp` or `k8s` so the driver mints from that environment"
                        .to_owned(),
                ),
            }
        }
        _ => None,
    }
}

impl DbInstance {
    /// The dialect in force: the configured one, ClickHouse for that engine,
    /// none for MongoDB.
    #[must_use]
    pub fn dialect(&self) -> Option<Dialect> {
        match (self.engine, self.dialect) {
            (Engine::Clickhouse, None) => Some(Dialect::Clickhouse),
            (Engine::Mongodb, _) => None,
            (_, d) => d,
        }
    }

    /// Whether the connection string is a literal rather than a reference, so
    /// it can be inspected at load.
    ///
    /// Anything carrying a spec prefix is a reference and is not inspected; an
    /// unusable one is refused separately by [`DbInstance::validate`], so it
    /// never also draws a complaint about a missing streaming knob.
    fn literal_connection_string(&self) -> Option<&str> {
        let spec = self.connection_string.expose();
        (!is_reference(spec)).then_some(spec)
    }

    /// Every problem with this instance, each as `field: problem`.
    #[must_use]
    pub fn validate(&self) -> Vec<String> {
        let mut issues = Vec::new();
        if self.topic.trim().is_empty() {
            issues.push("topic: is required".to_owned());
        }
        if self.connection_string.expose().trim().is_empty() {
            issues.push("connection_string: is required".to_owned());
        }
        // A spec the resolver cannot read otherwise reaches the driver as
        // literal text and fails about the DSN, not the credential.
        if let Some(issue) = spec_issue(self.connection_string.expose()) {
            issues.push(format!("connection_string: {issue}"));
        }
        if !self.engine.is_built() {
            issues.push(format!(
                "engine: `{}` is not built into this binary; build with `--features {}`",
                self.engine.as_str(),
                self.engine.feature()
            ));
        }
        match (self.engine, self.dialect) {
            (Engine::Odbc, None) => {
                issues.push("dialect: is required for the odbc engine".to_owned());
            }
            (Engine::Clickhouse, Some(d)) if d != Dialect::Clickhouse => issues.push(format!(
                "dialect: the clickhouse engine is always `clickhouse`, not `{}`",
                d.as_str()
            )),
            (Engine::Mongodb, Some(d)) => issues.push(format!(
                "dialect: the mongodb engine speaks no SQL dialect, drop `{}`",
                d.as_str()
            )),
            _ => {}
        }
        if self.engine == Engine::Mongodb
            && let Some(literal) = self.literal_connection_string()
            && let Some(issue) = mongodb_auth_issue(literal)
        {
            issues.push(format!("connection_string: {issue}"));
        }
        if let (Some(dialect), Some(literal)) = (self.dialect(), self.literal_connection_string())
            && let Some(knob) = dialect.streaming_knob()
            && !literal
                .to_ascii_lowercase()
                .contains(&knob.to_ascii_lowercase())
        {
            issues.push(format!(
                "connection_string: the {} driver buffers the whole result set unless it carries `{knob}`",
                dialect.as_str()
            ));
        }
        for (name, value) in [
            ("max_rows", self.batch.max_rows),
            ("max_bytes", self.batch.max_bytes),
            ("max_text_bytes", self.batch.max_text_bytes),
            ("max_binary_bytes", self.batch.max_binary_bytes),
        ] {
            if value == 0 {
                issues.push(format!("batch.{name}: must be at least 1"));
            }
        }
        if self.stores.is_empty() {
            issues.push("stores: at least one store is required".to_owned());
        }
        let mut seen = BTreeSet::new();
        for (i, store) in self.stores.iter().enumerate() {
            let at = |f: &str| format!("stores[{i}].{f}");
            if store.unit.trim().is_empty() {
                issues.push(format!("{}: is required", at("unit")));
            } else if !seen.insert(store.unit.as_str()) {
                issues.push(format!(
                    "{}: `{}` is declared twice",
                    at("unit"),
                    store.unit
                ));
            } else if store.unit.contains('.') {
                issues.push(format!(
                    "{}: `{}` cannot contain `.`, which separates the connection from the unit",
                    at("unit"),
                    store.unit
                ));
            }
            if self.engine.is_sql() {
                issues.extend(
                    sql_store_issues(store)
                        .into_iter()
                        .map(|(f, p)| format!("{}: {p}", at(f))),
                );
            } else {
                issues.extend(
                    mongo_store_issues(store)
                        .into_iter()
                        .map(|(f, p)| format!("{}: {p}", at(f))),
                );
            }
            if store.shape == StoreShape::Tail && store.limit == 0 {
                issues.push(format!("{}: must be at least 1", at("limit")));
            }
            if store.shape == StoreShape::Tail && store.max_pages_per_tick == 0 {
                issues.push(format!("{}: must be at least 1", at("max_pages_per_tick")));
            }
            if let Some(pointer) = &store.row_key
                && !pointer.starts_with('/')
            {
                issues.push(format!(
                    "{}: `{pointer}` is not a JSON pointer (must start with `/`)",
                    at("row_key")
                ));
            }
        }
        if let Some(acc) = &self.accumulate
            && let Err(e) = acc.validate()
        {
            issues.push(e.to_string());
        }
        issues
    }
}

/// What a store on a SQL engine gets wrong, as `(field, problem)`.
fn sql_store_issues(store: &StoreSpec) -> Vec<(&'static str, String)> {
    let mut issues = Vec::new();
    if store.query.trim().is_empty() {
        issues.push(("query", "is required".to_owned()));
    }
    match store.shape {
        StoreShape::Dump if !store.key.is_empty() => issues.push((
            "key",
            "only a tail has a key; a dump reads the whole store".to_owned(),
        )),
        StoreShape::Tail if store.key.is_empty() => issues.push((
            "key",
            "a tail needs at least one ordering key column".to_owned(),
        )),
        _ => {}
    }
    if store.mongodb.is_some() {
        issues.push(("mongodb", "only the mongodb engine reads it".to_owned()));
    }
    issues
}

/// What a store on the MongoDB engine gets wrong, as `(field, problem)`.
fn mongo_store_issues(store: &StoreSpec) -> Vec<(&'static str, String)> {
    let mut issues = Vec::new();
    if !store.query.trim().is_empty() {
        issues.push((
            "query",
            "the mongodb engine reads a collection, not SQL; name it under `mongodb`".to_owned(),
        ));
    }
    if !store.key.is_empty() {
        issues.push((
            "key",
            "the mongodb engine tails by change stream or by `_id`; the key is not configurable"
                .to_owned(),
        ));
    }
    let Some(mongo) = &store.mongodb else {
        issues.push(("mongodb", "is required for the mongodb engine".to_owned()));
        return issues;
    };
    for (field, value) in [
        ("mongodb.database", mongo.database.as_str()),
        ("mongodb.collection", mongo.collection.as_str()),
    ] {
        if value.trim().is_empty() {
            issues.push((field, "must not be empty".to_owned()));
        }
    }
    if store.shape == StoreShape::Dump && mongo.tail.is_some() {
        issues.push(("mongodb.tail", "only a tail has a tail mode".to_owned()));
    }
    issues
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instance(yaml: &str) -> DbInstance {
        serde_yaml_ng::from_str(yaml).expect("instance parses")
    }

    const GOOD: &str = r#"
engine: odbc
dialect: postgres
connection_string: "vault:kv/data/dfe/inventory:odbc_dsn"
topic: inventory
stores:
  - { unit: hosts, shape: dump, query: "SELECT * FROM hosts", row_key: "/id" }
  - { unit: events, shape: tail, query: "SELECT * FROM events", key: [ts, id], limit: 100 }
"#;

    /// The issues minus the engine gate, so a test reads the same on a build
    /// without the engine. Only the `engine:` line is set aside: a mechanism
    /// refusal on `connection_string:` uses the same words and must stay.
    fn without_build_gate(issues: Vec<String>) -> Vec<String> {
        issues
            .into_iter()
            .filter(|i| !(i.starts_with("engine:") && i.contains("not built into this binary")))
            .collect()
    }

    #[test]
    fn a_complete_instance_has_no_issues_beyond_the_build_gate() {
        let inst = instance(GOOD);
        assert!(without_build_gate(inst.validate()).is_empty());
        assert_eq!(inst.stores[0].shape.unit_shape(), UnitShape::Dump);
        assert_eq!(inst.stores[1].shape.unit_shape(), UnitShape::Incremental);
        assert_eq!(inst.stores[1].limit, 100);
        assert_eq!(inst.stores[0].limit, 5000, "the default limit");
        assert_eq!(inst.stores[1].max_pages_per_tick, 10);
    }

    #[test]
    fn the_build_gate_names_the_feature() {
        let inst = instance(GOOD);
        let issues = inst.validate();
        if Engine::Odbc.is_built() {
            assert!(issues.iter().all(|i| !i.contains("not built")));
        } else {
            assert!(
                issues
                    .iter()
                    .any(|i| i.contains("`odbc` is not built") && i.contains("db-odbc")),
                "{issues:?}"
            );
        }
    }

    #[test]
    fn missing_topic_connection_and_dialect_are_named() {
        let inst = instance("engine: odbc\nstores: [{ unit: a, query: 'SELECT 1' }]\n");
        let issues = without_build_gate(inst.validate());
        assert!(
            issues.iter().any(|i| i == "topic: is required"),
            "{issues:?}"
        );
        assert!(
            issues.iter().any(|i| i == "connection_string: is required"),
            "{issues:?}"
        );
        assert!(
            issues
                .iter()
                .any(|i| i == "dialect: is required for the odbc engine"),
            "{issues:?}"
        );
    }

    #[test]
    fn a_literal_postgres_connection_string_without_the_streaming_knob_is_refused() {
        let inst = instance(
            "engine: odbc\ndialect: postgres\nconnection_string: \"Driver=PostgreSQL Unicode;Server=db\"\ntopic: t\nstores: [{ unit: a, query: 'SELECT 1' }]\n",
        );
        let issues = without_build_gate(inst.validate());
        assert!(
            issues
                .iter()
                .any(|i| i.starts_with("connection_string:") && i.contains("UseDeclareFetch=1")),
            "{issues:?}"
        );
        let ok = instance(
            "engine: odbc\ndialect: postgres\nconnection_string: \"Driver=PostgreSQL Unicode;Server=db;UseDeclareFetch=1\"\ntopic: t\nstores: [{ unit: a, query: 'SELECT 1' }]\n",
        );
        assert!(without_build_gate(ok.validate()).is_empty());
    }

    #[test]
    fn a_referenced_connection_string_is_not_inspected_at_load() {
        let inst = instance(
            "engine: odbc\ndialect: mysql\nconnection_string: \"env:INVENTORY_DSN\"\ntopic: t\nstores: [{ unit: a, query: 'SELECT 1' }]\n",
        );
        assert!(without_build_gate(inst.validate()).is_empty());
    }

    /// Both halves of the prefix contract, so the accepted list and the
    /// resolver cannot drift apart again: a prefix the resolver handles is
    /// accepted and left uninspected, and one it does not is refused at load
    /// naming the prefix. An unresolvable prefix otherwise reaches the driver
    /// as literal text and fails talking about the DSN, not the credential.
    /// One instance of `engine: odbc` carrying `spec` as its connection string.
    fn with_spec(spec: &str) -> Vec<String> {
        without_build_gate(
            instance(&format!(
                "engine: odbc\ndialect: mysql\nconnection_string: \"{spec}\"\ntopic: t\nstores: [{{ unit: a, query: 'SELECT 1' }}]\n"
            ))
            .validate(),
        )
    }

    /// A connection_string issue, or nothing.
    fn spec_issues(spec: &str) -> Vec<String> {
        with_spec(spec)
            .into_iter()
            .filter(|i| i.starts_with("connection_string:"))
            .collect()
    }

    #[test]
    fn a_spec_the_resolver_cannot_read_is_refused_at_load() {
        // Well formed, so accepted and left uninspected; a KV spec carries
        // its `:key`.
        for spec in [
            "vault:kv/data/team/db:dsn",
            "bao:kv/data/team/db:dsn",
            "openbao:kv/data/team/db:dsn",
            "env:INVENTORY_DSN",
            "file:/run/secrets/dsn",
        ] {
            assert!(
                with_spec(spec).is_empty(),
                "`{spec}` is a spec the resolver reads, so nothing is wrong with it"
            );
        }
        // Not derived from the consts under test, so emptying one fails here.
        for (spec, secret) in [
            ("aws:prod/team/db", "team/db"),
            ("aws:prod/team/db:dsn", "team/db"),
        ] {
            let issues = spec_issues(spec);
            assert!(!issues.is_empty(), "`{spec}` must be refused");
            assert!(
                !issues.iter().any(|i| i.contains(secret)),
                "the refusal names the prefix only, never the rest of the spec: {issues:?}"
            );
            assert!(
                !with_spec(spec)
                    .iter()
                    .any(|i| i.contains("UseDeclareFetch")),
                "a refused spec must not also be inspected for a streaming knob"
            );
        }
        // The resolver matches a prefix exactly, so a near miss resolves to
        // nothing and must not be mistaken for a reference.
        for spec in ["Vault:kv/data/team/db:dsn", " vault:kv/data/team/db:dsn"] {
            assert!(
                !spec_issues(spec).is_empty(),
                "`{spec}` is not written as the resolver matches it, so it must be refused"
            );
        }
        // Split into path and key only at resolve time, so a missing key is
        // otherwise a per-tick failure rather than a load error.
        for spec in [
            "vault:kv/data/team/db",
            "bao:kv/data/team/db",
            "openbao:kv/data/team/db",
        ] {
            assert!(
                !spec_issues(spec).is_empty(),
                "`{spec}` names no key, so it must be refused at load"
            );
        }
    }

    #[test]
    fn store_problems_carry_their_index_and_field() {
        let inst = instance(
            r#"
engine: odbc
dialect: mysql
connection_string: "env:DSN"
topic: t
stores:
  - { unit: a, query: "SELECT 1", key: [id] }
  - { unit: a, shape: tail, query: "SELECT 1" }
  - { unit: "x.y", query: "" , row_key: "id" }
  - { unit: z, shape: tail, query: "SELECT 1", key: [id], limit: 0 }
"#,
        );
        let issues = without_build_gate(inst.validate());
        for expected in [
            "stores[0].key: only a tail has a key",
            "stores[1].unit: `a` is declared twice",
            "stores[1].key: a tail needs at least one ordering key column",
            "stores[2].unit: `x.y` cannot contain `.`",
            "stores[2].query: is required",
            "stores[2].row_key: `id` is not a JSON pointer",
            "stores[3].limit: must be at least 1",
        ] {
            assert!(
                issues.iter().any(|i| i.starts_with(expected)),
                "missing `{expected}` in {issues:?}"
            );
        }
    }

    #[test]
    fn clickhouse_implies_its_dialect_and_accepts_a_tail() {
        let inst = instance(
            "engine: clickhouse\nconnection_string: \"env:CH\"\ntopic: t\nstores: [{ unit: audit, shape: tail, query: 'SELECT 1', key: [ts] }]\n",
        );
        assert_eq!(inst.dialect(), Some(Dialect::Clickhouse));
        assert!(
            without_build_gate(inst.validate()).is_empty(),
            "{:?}",
            inst.validate()
        );
        let wrong = instance(
            "engine: clickhouse\ndialect: postgres\nconnection_string: \"env:CH\"\ntopic: t\nstores: [{ unit: a, query: 'SELECT 1' }]\n",
        );
        assert!(
            without_build_gate(wrong.validate())
                .iter()
                .any(|i| i.starts_with("dialect: the clickhouse engine is always `clickhouse`"))
        );
    }

    #[test]
    fn zero_batch_bounds_and_an_empty_store_list_are_refused() {
        let inst = instance(
            "engine: odbc\ndialect: mssql\nconnection_string: \"env:DSN\"\ntopic: t\nbatch: { max_rows: 0, max_bytes: 0 }\n",
        );
        let issues = without_build_gate(inst.validate());
        assert!(
            issues
                .iter()
                .any(|i| i == "batch.max_rows: must be at least 1")
        );
        assert!(
            issues
                .iter()
                .any(|i| i == "batch.max_bytes: must be at least 1")
        );
        assert!(
            issues
                .iter()
                .any(|i| i == "stores: at least one store is required")
        );
    }

    #[test]
    fn the_connection_string_is_redacted_on_serialise() {
        let inst = instance(GOOD);
        let out = serde_json::to_string(&inst).unwrap();
        assert!(!out.contains("odbc_dsn"), "{out}");
    }

    #[test]
    fn unknown_fields_fail_the_load() {
        assert!(serde_yaml_ng::from_str::<DbInstance>("engine: odbc\ndsn: x\n").is_err());
    }

    const MONGO: &str = r#"
engine: mongodb
connection_string: "vault:kv/data/dfe/mongo:uri"
topic: inventory
stores:
  - { unit: assets, shape: dump, row_key: "/_id/$oid", mongodb: { database: inventory, collection: assets, filter: { alive: true } } }
  - { unit: changes, shape: tail, limit: 100, mongodb: { database: inventory, collection: assets } }
  - { unit: rows, shape: tail, mongodb: { database: inventory, collection: assets, tail: keyset } }
"#;

    #[test]
    fn a_mongodb_instance_names_collections_and_defaults_its_tail_to_the_change_stream() {
        let inst = instance(MONGO);
        assert_eq!(inst.engine, Engine::Mongodb);
        assert_eq!(inst.dialect(), None);
        assert!(
            without_build_gate(inst.validate()).is_empty(),
            "{:?}",
            inst.validate()
        );
        let mongo = |i: usize| inst.stores[i].mongodb.as_ref().expect("mongodb block");
        assert_eq!(mongo(0).database, "inventory");
        assert_eq!(mongo(0).collection, "assets");
        assert_eq!(mongo(1).tail_mode(), TailMode::ChangeStream);
        assert_eq!(mongo(2).tail_mode(), TailMode::Keyset);
        assert_eq!(
            mongo(0).filter.as_ref().unwrap()["alive"],
            serde_json::json!(true)
        );
        assert_eq!(Engine::Mongodb.feature(), "db-mongodb");
        assert_eq!(Engine::Mongodb.as_str(), "mongodb");
        assert!(!Engine::Mongodb.is_sql());
        assert_eq!(TailMode::ChangeStream.as_str(), "change_stream");
        assert_eq!(
            serde_json::from_str::<TailMode>("\"keyset\"").unwrap(),
            TailMode::Keyset
        );
    }

    #[test]
    fn the_mongodb_build_gate_names_its_feature() {
        let issues = instance(MONGO).validate();
        if Engine::Mongodb.is_built() {
            assert!(
                issues.iter().all(|i| !i.contains("not built")),
                "{issues:?}"
            );
        } else {
            assert!(
                issues
                    .iter()
                    .any(|i| i.contains("`mongodb` is not built") && i.contains("db-mongodb")),
                "{issues:?}"
            );
        }
    }

    /// One mongodb instance carrying `uri` as a literal connection string;
    /// only the `connection_string` issues are returned, which leaves the
    /// engine gate out so the test reads the same on a build without it.
    fn mongo_uri_issues(uri: &str) -> Vec<String> {
        let inst = instance(&format!(
            "engine: mongodb\nconnection_string: \"{uri}\"\ntopic: t\nstores: [{{ unit: a, mongodb: {{ database: d, collection: c }} }}]\n"
        ));
        inst.validate()
            .into_iter()
            .filter(|i| i.starts_with("connection_string:"))
            .collect()
    }

    #[test]
    fn a_mongodb_mechanism_this_binary_cannot_perform_is_refused_at_load() {
        // SCRAM, X.509 and the three OIDC environments the driver mints from
        // are the driver's to validate; nothing is said here. The driver
        // percent-decodes each value before reading it, so an encoded
        // property list is the same list; the `+srv` and `/db?` forms put
        // the query in the same place.
        for uri in [
            "mongodb://u:p@h/?authSource=admin&authMechanism=SCRAM-SHA-256",
            "mongodb://h/?authMechanism=MONGODB-X509&tls=true",
            "mongodb://u:p@h/",
            "mongodb://h/?authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:k8s",
            "mongodb://h/?authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:azure,TOKEN_RESOURCE:x",
            "mongodb://h/?authMechanism=MONGODB-OIDC&authMechanismProperties=TOKEN_RESOURCE:x,ENVIRONMENT:gcp",
            "mongodb://h/?authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT%3Ak8s",
            "mongodb://h/?authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT%3Aazure%2CTOKEN_RESOURCE%3Aapi%3A%2F%2Fx",
            "mongodb+srv://h/?authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:k8s",
            "mongodb://h/admin?authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:k8s",
        ] {
            assert!(mongo_uri_issues(uri).is_empty(), "{uri}");
        }

        // Mechanisms behind driver features this crate leaves off, the one
        // the driver refuses as deprecated, and the callback-only OIDC form
        // are refused naming the reason, an encoded spelling included.
        for (uri, names) in [
            (
                "mongodb://h/?authMechanism=MONGODB-AWS",
                "`MONGODB-AWS` is not built",
            ),
            (
                "mongodb://h/?authMechanism=MONGODB%2DAWS",
                "`MONGODB-AWS` is not built",
            ),
            ("mongodb://h/?authMechanism=GSSAPI", "`GSSAPI` is not built"),
            ("mongodb://h/?authMechanism=MONGODB-CR", "deprecated"),
            (
                "mongodb://h/?authMechanism=MONGODB-OIDC",
                "`azure`, `gcp` or `k8s`",
            ),
            (
                "mongodb://h/?authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:test",
                "`azure`, `gcp` or `k8s`",
            ),
        ] {
            let issues = mongo_uri_issues(uri);
            assert_eq!(issues.len(), 1, "{uri}: {issues:?}");
            assert!(issues[0].contains(names), "{uri}: {issues:?}");
        }

        // The option name and the mechanism are matched without regard to
        // case, as the driver matches the name, so neither is a way past.
        let lower = "mongodb://h/?authmechanism=mongodb-aws";
        assert_eq!(mongo_uri_issues(lower).len(), 1, "{lower}");

        // The property key is matched exactly, as the driver matches it, so
        // a mis-cased key is no environment at all and the string is the
        // callback-only form the driver would refuse too.
        let cased =
            "mongodb://h/?authMechanism=MONGODB-OIDC&authMechanismProperties=environment:k8s";
        assert_eq!(mongo_uri_issues(cased).len(), 1, "{cased}");

        // A referenced spec cannot be read at load; the same check runs at
        // connect, where the resolved text is first seen.
        let referenced = instance(
            "engine: mongodb\nconnection_string: \"env:MONGO_URI\"\ntopic: t\nstores: [{ unit: a, mongodb: { database: d, collection: c } }]\n",
        );
        assert!(
            without_build_gate(referenced.validate()).is_empty(),
            "{:?}",
            referenced.validate()
        );
        assert!(mongodb_auth_issue("mongodb://h/?authMechanism=GSSAPI").is_some());
    }

    #[test]
    fn a_mongodb_store_refuses_sql_keys_and_needs_its_block() {
        let inst = instance(
            r#"
engine: mongodb
dialect: postgres
connection_string: "env:MONGO"
topic: t
stores:
  - { unit: a, shape: tail, query: "SELECT 1", key: [id], mongodb: { database: d, collection: c } }
  - { unit: b, shape: dump }
  - { unit: c, shape: dump, mongodb: { database: "", collection: c } }
  - { unit: d, shape: dump, mongodb: { database: d, collection: c, tail: keyset } }
"#,
        );
        let issues = without_build_gate(inst.validate());
        for expected in [
            "dialect: the mongodb engine speaks no SQL dialect, drop `postgres`",
            "stores[0].query: the mongodb engine reads a collection, not SQL",
            "stores[0].key: the mongodb engine tails by change stream or by `_id`",
            "stores[1].mongodb: is required for the mongodb engine",
            "stores[2].mongodb.database: must not be empty",
            "stores[3].mongodb.tail: only a tail has a tail mode",
        ] {
            assert!(
                issues.iter().any(|i| i.starts_with(expected)),
                "missing `{expected}` in {issues:?}"
            );
        }
    }

    /// A block missing one of its required names fails to PARSE rather than
    /// reaching validation, because serde enforces presence.
    #[test]
    fn a_mongodb_block_without_its_names_does_not_parse() {
        assert!(
            serde_yaml_ng::from_str::<DbInstance>(
                "engine: mongodb\nstores: [{ unit: a, mongodb: { collection: c } }]\n"
            )
            .is_err(),
            "database is required inside the mongodb block"
        );
    }

    /// The old flat spelling is refused outright, so a config written for the
    /// previous grammar fails at load rather than silently fetching nothing.
    #[test]
    fn the_flat_mongodb_keys_are_no_longer_accepted() {
        assert!(
            serde_yaml_ng::from_str::<DbInstance>(
                "engine: mongodb\nstores: [{ unit: a, database: d, collection: c }]\n"
            )
            .is_err(),
            "database at store level is not part of the grammar"
        );
    }

    #[test]
    fn a_sql_store_refuses_the_mongodb_keys() {
        let inst = instance(
            "engine: odbc\ndialect: postgres\nconnection_string: \"env:DSN\"\ntopic: t\nstores: [{ unit: a, query: 'SELECT 1', mongodb: { database: d, collection: c } }]\n",
        );
        let issues = without_build_gate(inst.validate());
        assert!(
            issues.contains(&"stores[0].mongodb: only the mongodb engine reads it".to_owned()),
            "{issues:?}"
        );
    }

    #[test]
    fn a_mongodb_filter_must_be_a_document() {
        assert!(
            serde_yaml_ng::from_str::<DbInstance>(
                "engine: mongodb\nstores: [{ unit: a, mongodb: { database: d, collection: c, filter: [1] } }]\n"
            )
            .is_err(),
            "an array is not a query document"
        );
    }

    /// One grammar for every engine. The tests above pin the REFUSALS -- a key
    /// one engine cannot read is named rather than ignored -- and this pins the
    /// other half: every engine accepts the whole common surface, so no shared
    /// key can quietly become engine-specific. Only the store's identity
    /// differs, because a SQL engine names a query and mongodb names a
    /// collection.
    #[test]
    fn every_engine_accepts_the_common_keys() {
        for (engine, extra, identity) in [
            (
                "odbc",
                "dialect: postgres\n",
                r#"query: "SELECT 1", key: [id]"#,
            ),
            ("clickhouse", "", r#"query: "SELECT 1", key: [id]"#),
            ("mongodb", "", "mongodb: { database: d, collection: c }"),
        ] {
            let yaml = format!(
                r#"
enabled: true
engine: {engine}
{extra}connection_string: "env:DSN"
interval_secs: 900
topic: inventory
filter: 'kept == true'
batch: {{ max_rows: 100, max_bytes: 1048576, max_text_bytes: 4096, max_binary_bytes: 8192 }}
stores:
  - {{ unit: rows, shape: tail, limit: 50, max_pages_per_tick: 3, row_key: "/id", {identity} }}
"#
            );
            let inst = instance(&yaml);
            assert!(
                without_build_gate(inst.validate()).is_empty(),
                "`{engine}` refused the common surface: {:?}",
                inst.validate()
            );
            assert!(inst.enabled);
            assert_eq!(inst.interval_secs, Some(900));
            assert_eq!(inst.topic, "inventory");
            assert_eq!(inst.filter.as_deref(), Some("kept == true"));
            assert_eq!(inst.batch.max_rows, 100);
            assert_eq!(inst.batch.max_bytes, 1_048_576);
            assert_eq!(inst.batch.max_text_bytes, 4096);
            assert_eq!(inst.batch.max_binary_bytes, 8192);
            assert_eq!(inst.stores[0].shape, StoreShape::Tail);
            assert_eq!(inst.stores[0].limit, 50);
            assert_eq!(inst.stores[0].max_pages_per_tick, 3);
            assert_eq!(inst.stores[0].row_key.as_deref(), Some("/id"));
        }
    }
}
