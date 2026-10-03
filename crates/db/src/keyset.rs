// Project:   dfe-fetcher
// File:      crates/db/src/keyset.rs
// Purpose:   The per-dialect keyset predicate a tail appends to its query
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Keyset predicates.
//!
//! A tail resumes from the last committed key tuple with `WHERE (a, b) > (?, ?)
//! ORDER BY a, b LIMIT n`. The tuple comparison is invalid on MSSQL and Oracle,
//! which take the expanded `a > ? OR (a = ? AND b > ?)` form. Values are always
//! bound as parameters, never written into the SQL text: ODBC binds a `?` per
//! value, ClickHouse a typed `{name:Type}` placeholder the server fills.
//!
//! Dialect facts encoded here, each verified against the vendor's own
//! reference: MSSQL caps rows with `OFFSET ... ROWS FETCH NEXT ... ROWS ONLY`,
//! which is a sub-clause of `ORDER BY` and so needs one, and `TOP` when there
//! is no ordering; Oracle 12c and later cap with `FETCH FIRST n ROWS ONLY` and
//! refuse `AS` before a table alias; MSSQL delimits identifiers with `[...]`,
//! Oracle, PostgreSQL, Snowflake and SQLite with `"..."`, MySQL, ClickHouse,
//! BigQuery and Databricks with backticks.

use crate::store::Dialect;

/// How a dialect delimits an identifier that is not a plain name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteStyle {
    /// `"name"`, a `"` inside doubled.
    DoubleQuote,
    /// `` `name` ``, a backtick inside doubled.
    Backtick,
    /// `[name]`, a `]` inside doubled.
    Bracket,
}

impl QuoteStyle {
    const fn open(self) -> char {
        match self {
            QuoteStyle::DoubleQuote => '"',
            QuoteStyle::Backtick => '`',
            QuoteStyle::Bracket => '[',
        }
    }

    const fn close(self) -> char {
        match self {
            QuoteStyle::DoubleQuote => '"',
            QuoteStyle::Backtick => '`',
            QuoteStyle::Bracket => ']',
        }
    }
}

impl Dialect {
    /// The delimiter form this dialect accepts for an identifier.
    #[must_use]
    pub const fn quote_style(self) -> QuoteStyle {
        match self {
            Dialect::Mssql => QuoteStyle::Bracket,
            Dialect::Mysql | Dialect::Clickhouse | Dialect::Bigquery | Dialect::Databricks => {
                QuoteStyle::Backtick
            }
            Dialect::Postgres | Dialect::Oracle | Dialect::Sqlite | Dialect::Snowflake => {
                QuoteStyle::DoubleQuote
            }
        }
    }

    /// Whether the dialect writes `AS` before a derived-table alias; Oracle
    /// answers `ORA-00933` to it.
    #[must_use]
    pub const fn aliases_with_as(self) -> bool {
        !matches!(self, Dialect::Oracle)
    }

    /// How the engine folds the case of a plain (undelimited) identifier,
    /// which is the name the column then has in the result: PostgreSQL to
    /// lower case, Oracle and Snowflake to upper, the rest keep it.
    #[must_use]
    pub const fn case_fold(self) -> CaseFold {
        match self {
            Dialect::Postgres => CaseFold::Lower,
            Dialect::Oracle | Dialect::Snowflake => CaseFold::Upper,
            Dialect::Mysql
            | Dialect::Mssql
            | Dialect::Clickhouse
            | Dialect::Sqlite
            | Dialect::Bigquery
            | Dialect::Databricks => CaseFold::Keep,
        }
    }
}

/// What an engine does to the case of a plain identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaseFold {
    /// Stored and resolved in lower case.
    Lower,
    /// Stored and resolved in upper case.
    Upper,
    /// Kept as written.
    Keep,
}

/// Whether `name` is a plain identifier the engine reads as is: a letter or
/// underscore, then letters, digits and underscores.
fn is_plain(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Whether `name` already carries the dialect's own delimiters.
fn is_delimited(style: QuoteStyle, name: &str) -> bool {
    name.len() >= 2 && name.starts_with(style.open()) && name.ends_with(style.close())
}

/// The key as it goes into the SQL: a plain name as given, so the engine
/// folds its case by its own rule; a name already delimited in the dialect's
/// form as given; anything else delimited by us.
#[must_use]
pub fn quote(dialect: Dialect, name: &str) -> String {
    let style = dialect.quote_style();
    if is_plain(name) || is_delimited(style, name) {
        return name.to_owned();
    }
    let close = style.close();
    let mut out = String::with_capacity(name.len() + 2);
    out.push(style.open());
    for c in name.chars() {
        out.push(c);
        if c == close {
            out.push(close);
        }
    }
    out.push(close);
    out
}

/// The column name the key's value lands under in the row JSON: the bare
/// name inside a delimited key, a plain key folded as the engine folds it,
/// anything else as given.
#[must_use]
pub fn column(dialect: Dialect, name: &str) -> String {
    let style = dialect.quote_style();
    if is_delimited(style, name) {
        let inner = &name[1..name.len() - 1];
        let doubled = format!("{0}{0}", style.close());
        return inner.replace(&doubled, &style.close().to_string());
    }
    if is_plain(name) {
        return match dialect.case_fold() {
            CaseFold::Lower => name.to_ascii_lowercase(),
            CaseFold::Upper => name.to_ascii_uppercase(),
            CaseFold::Keep => name.to_owned(),
        };
    }
    name.to_owned()
}

/// The predicate and ordering for a tail over `keys`; `placeholder(i)` is the
/// parameter text for key `i`, `?` for ODBC or a typed name for ClickHouse.
///
/// Returns `None` for an empty key list, which is not a keyset.
#[must_use]
pub fn predicate(
    dialect: Dialect,
    keys: &[&str],
    placeholder: &dyn Fn(usize) -> String,
) -> Option<String> {
    if keys.is_empty() {
        return None;
    }
    let quoted: Vec<String> = keys.iter().map(|k| quote(dialect, k)).collect();
    let order = quoted.join(", ");
    let predicate = if dialect.supports_row_values() {
        let marks: Vec<String> = (0..keys.len()).map(placeholder).collect();
        format!("({}) > ({})", quoted.join(", "), marks.join(", "))
    } else {
        // For keys a, b, c: a > ? OR (a = ? AND b > ?) OR (a = ? AND b = ? AND c > ?)
        let mut terms = Vec::with_capacity(keys.len());
        for (i, key) in quoted.iter().enumerate() {
            let mut conj: Vec<String> = quoted[..i]
                .iter()
                .enumerate()
                .map(|(j, k)| format!("{k} = {}", placeholder(j)))
                .collect();
            conj.push(format!("{key} > {}", placeholder(i)));
            terms.push(if conj.len() == 1 {
                conj.remove(0)
            } else {
                format!("({})", conj.join(" AND "))
            });
        }
        terms.join(" OR ")
    };
    Some(format!("{predicate} ORDER BY {order}"))
}

/// The row cap in the dialect's own spelling after an `ORDER BY`; MSSQL and
/// Oracle have no `LIMIT`.
#[must_use]
pub fn limit_clause(dialect: Dialect, limit: u32) -> String {
    match dialect {
        Dialect::Mssql => format!("OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY"),
        Dialect::Oracle => format!("FETCH FIRST {limit} ROWS ONLY"),
        _ => format!("LIMIT {limit}"),
    }
}

/// The derived table the operator's query becomes, aliased in the dialect's
/// form.
fn derived(dialect: Dialect, query: &str) -> String {
    if dialect.aliases_with_as() {
        format!("({query}) AS dfe_tail")
    } else {
        format!("({query}) dfe_tail")
    }
}

/// The whole tail statement: the operator's query as a derived table, the
/// keyset predicate when a checkpoint exists, the ordering, and the cap.
///
/// The derived table keeps the operator's query opaque -- it may carry its
/// own WHERE, joins or aliases -- and the keys must be column names of its
/// result. Returns the bare capped select for an empty key list, which on
/// MSSQL is `TOP` because `OFFSET ... FETCH` needs an `ORDER BY`.
#[must_use]
pub fn tail_sql(
    dialect: Dialect,
    query: &str,
    keys: &[&str],
    has_after: bool,
    limit: u32,
    placeholder: &dyn Fn(usize) -> String,
) -> String {
    let from = derived(dialect, query);
    let Some(pred) = predicate(dialect, keys, placeholder) else {
        return match dialect {
            Dialect::Mssql => format!("SELECT TOP ({limit}) * FROM {from}"),
            _ => format!("SELECT * FROM {from} {}", limit_clause(dialect, limit)),
        };
    };
    let cap = limit_clause(dialect, limit);
    if has_after {
        format!("SELECT * FROM {from} WHERE {pred} {cap}")
    } else {
        let order: Vec<String> = keys.iter().map(|k| quote(dialect, k)).collect();
        format!("SELECT * FROM {from} ORDER BY {} {cap}", order.join(", "))
    }
}

/// The ODBC placeholder: one `?` per value, positional.
#[must_use]
pub fn question_mark(_: usize) -> String {
    "?".to_owned()
}

/// Which key value fills each `?` of [`predicate`], as indexes into the tuple.
///
/// The row-value form binds the tuple once; the expanded form repeats the
/// leading values for every disjunct.
#[must_use]
pub fn bind_order(dialect: Dialect, key_count: usize) -> Vec<usize> {
    if key_count == 0 {
        return Vec::new();
    }
    if dialect.supports_row_values() {
        return (0..key_count).collect();
    }
    let mut order = Vec::new();
    for i in 0..key_count {
        order.extend(0..=i);
    }
    order
}

#[cfg(test)]
mod tests {
    use super::*;

    fn odbc(dialect: Dialect, query: &str, keys: &[&str], has_after: bool, limit: u32) -> String {
        tail_sql(dialect, query, keys, has_after, limit, &question_mark)
    }

    #[test]
    fn row_value_dialects_compare_the_tuple_once() {
        assert_eq!(
            predicate(Dialect::Postgres, &["ts", "id"], &question_mark).unwrap(),
            "(ts, id) > (?, ?) ORDER BY ts, id"
        );
        assert_eq!(bind_order(Dialect::Postgres, 2), [0, 1]);
    }

    #[test]
    fn mssql_and_oracle_expand_to_the_disjunction() {
        assert_eq!(
            predicate(Dialect::Mssql, &["ts", "id"], &question_mark).unwrap(),
            "ts > ? OR (ts = ? AND id > ?) ORDER BY ts, id"
        );
        assert_eq!(bind_order(Dialect::Mssql, 2), [0, 0, 1]);
        assert_eq!(
            predicate(Dialect::Oracle, &["a", "b", "c"], &question_mark).unwrap(),
            "a > ? OR (a = ? AND b > ?) OR (a = ? AND b = ? AND c > ?) ORDER BY a, b, c"
        );
        assert_eq!(bind_order(Dialect::Oracle, 3), [0, 0, 1, 0, 1, 2]);
    }

    #[test]
    fn a_single_key_is_the_same_on_every_dialect_and_no_key_is_no_keyset() {
        assert_eq!(
            predicate(Dialect::Mssql, &["id"], &question_mark).unwrap(),
            "id > ? ORDER BY id"
        );
        assert_eq!(
            predicate(Dialect::Mysql, &["id"], &question_mark).unwrap(),
            "(id) > (?) ORDER BY id"
        );
        assert!(predicate(Dialect::Postgres, &[], &question_mark).is_none());
        assert_eq!(bind_order(Dialect::Postgres, 0), [] as [usize; 0]);
    }

    #[test]
    fn the_tail_statement_takes_the_dialect_cap_and_skips_the_predicate_on_the_first_tick() {
        assert_eq!(
            odbc(Dialect::Mysql, "SELECT * FROM t", &["id"], true, 10),
            "SELECT * FROM (SELECT * FROM t) AS dfe_tail WHERE (id) > (?) ORDER BY id LIMIT 10"
        );
        assert_eq!(
            odbc(Dialect::Mssql, "SELECT * FROM t", &["ts", "id"], false, 10),
            "SELECT * FROM (SELECT * FROM t) AS dfe_tail ORDER BY ts, id OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY"
        );
        assert_eq!(
            odbc(Dialect::Postgres, "SELECT 1", &[], true, 3),
            "SELECT * FROM (SELECT 1) AS dfe_tail LIMIT 3"
        );
    }

    #[test]
    fn mssql_offset_fetch_always_follows_an_order_by_and_top_takes_the_unordered_case() {
        // OFFSET ... FETCH is a sub-clause of ORDER BY on SQL Server 2012+ and
        // TOP cannot share a query scope with it, so the no-key form is TOP.
        assert_eq!(
            odbc(Dialect::Mssql, "SELECT * FROM t", &["ts", "id"], true, 10),
            "SELECT * FROM (SELECT * FROM t) AS dfe_tail WHERE ts > ? OR (ts = ? AND id > ?) \
             ORDER BY ts, id OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY"
        );
        assert_eq!(
            odbc(Dialect::Mssql, "SELECT * FROM t", &[], false, 10),
            "SELECT TOP (10) * FROM (SELECT * FROM t) AS dfe_tail"
        );
    }

    #[test]
    fn oracle_aliases_the_derived_table_without_as_and_caps_with_fetch_first() {
        // ORA-00933 on `AS` before a table alias; FETCH FIRST is 12c Release 1+.
        assert_eq!(
            odbc(Dialect::Oracle, "SELECT * FROM t", &["id"], true, 3),
            "SELECT * FROM (SELECT * FROM t) dfe_tail WHERE id > ? ORDER BY id FETCH FIRST 3 ROWS ONLY"
        );
        assert_eq!(
            odbc(Dialect::Oracle, "SELECT * FROM t", &["ts", "id"], false, 3),
            "SELECT * FROM (SELECT * FROM t) dfe_tail ORDER BY ts, id FETCH FIRST 3 ROWS ONLY"
        );
        assert_eq!(
            odbc(Dialect::Oracle, "SELECT * FROM t", &[], false, 3),
            "SELECT * FROM (SELECT * FROM t) dfe_tail FETCH FIRST 3 ROWS ONLY"
        );
        assert!(!Dialect::Oracle.aliases_with_as());
        assert!(Dialect::Mssql.aliases_with_as());
    }

    #[test]
    fn identifiers_are_delimited_in_the_dialect_form_only_when_not_plain() {
        assert_eq!(quote(Dialect::Mssql, "ts"), "ts");
        assert_eq!(quote(Dialect::Mssql, "Order Date"), "[Order Date]");
        assert_eq!(quote(Dialect::Mssql, "a]b"), "[a]]b]");
        assert_eq!(quote(Dialect::Oracle, "Order Date"), "\"Order Date\"");
        assert_eq!(quote(Dialect::Postgres, "Ts-1"), "\"Ts-1\"");
        assert_eq!(quote(Dialect::Postgres, "say \"hi\""), "\"say \"\"hi\"\"\"");
        assert_eq!(quote(Dialect::Mysql, "order date"), "`order date`");
        assert_eq!(quote(Dialect::Clickhouse, "x.y"), "`x.y`");
        assert_eq!(quote(Dialect::Bigquery, "x y"), "`x y`");
        assert_eq!(quote(Dialect::Snowflake, "x y"), "\"x y\"");
        assert_eq!(quote(Dialect::Postgres, "_ok9"), "_ok9");
        assert_eq!(quote(Dialect::Postgres, "9no"), "\"9no\"");
        assert_eq!(quote(Dialect::Postgres, ""), "\"\"");
    }

    #[test]
    fn a_key_the_operator_delimited_passes_through_and_names_its_bare_column() {
        assert_eq!(quote(Dialect::Postgres, "\"Ts\""), "\"Ts\"");
        assert_eq!(column(Dialect::Postgres, "\"Ts\""), "Ts");
        assert_eq!(quote(Dialect::Mssql, "[Order]"), "[Order]");
        assert_eq!(column(Dialect::Mssql, "[Order]"), "Order");
        assert_eq!(column(Dialect::Mssql, "[a]]b]"), "a]b");
        assert_eq!(column(Dialect::Mysql, "`x y`"), "x y");
        assert_eq!(column(Dialect::Postgres, "ts"), "ts");
        assert_eq!(
            column(Dialect::Postgres, "Order Date"),
            "Order Date",
            "a name we delimit lands under itself"
        );
        assert_eq!(
            column(Dialect::Postgres, "Ts"),
            "ts",
            "PostgreSQL folds a plain name to lower case"
        );
        assert_eq!(
            column(Dialect::Oracle, "seq"),
            "SEQ",
            "Oracle folds a plain name to upper case"
        );
        assert_eq!(column(Dialect::Snowflake, "seq"), "SEQ");
        assert_eq!(column(Dialect::Oracle, "\"seq\""), "seq");
        assert_eq!(column(Dialect::Mssql, "Seq"), "Seq", "SQL Server keeps it");
        assert_eq!(column(Dialect::Mysql, "Seq"), "Seq");
        assert_eq!(column(Dialect::Clickhouse, "Seq"), "Seq");
        assert_eq!(
            quote(Dialect::Mysql, "\"x\""),
            "`\"x\"`",
            "another dialect's delimiters are just characters"
        );
    }

    #[test]
    fn delimited_keys_are_delimited_in_the_predicate_and_the_ordering() {
        assert_eq!(
            odbc(
                Dialect::Mssql,
                "SELECT * FROM t",
                &["Order Date", "id"],
                true,
                5
            ),
            "SELECT * FROM (SELECT * FROM t) AS dfe_tail WHERE [Order Date] > ? OR ([Order Date] = ? AND id > ?) \
             ORDER BY [Order Date], id OFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY"
        );
        assert_eq!(
            odbc(
                Dialect::Postgres,
                "SELECT * FROM t",
                &["\"Ts\"", "id"],
                false,
                5
            ),
            "SELECT * FROM (SELECT * FROM t) AS dfe_tail ORDER BY \"Ts\", id LIMIT 5"
        );
    }

    #[test]
    fn a_typed_placeholder_is_named_per_key_and_repeats_where_the_disjunction_needs_it() {
        let typed = |i: usize| format!("{{k{i}:{}}}", ["DateTime64(3)", "UInt64"][i]);
        assert_eq!(
            tail_sql(
                Dialect::Clickhouse,
                "SELECT * FROM t",
                &["ts", "id"],
                true,
                7,
                &typed
            ),
            "SELECT * FROM (SELECT * FROM t) AS dfe_tail WHERE (ts, id) > ({k0:DateTime64(3)}, {k1:UInt64}) \
             ORDER BY ts, id LIMIT 7"
        );
        let named = |i: usize| format!(":k{i}");
        assert_eq!(
            predicate(Dialect::Oracle, &["a", "b"], &named).unwrap(),
            "a > :k0 OR (a = :k0 AND b > :k1) ORDER BY a, b"
        );
    }
}
