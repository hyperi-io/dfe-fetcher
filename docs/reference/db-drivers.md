<!-- Project:   dfe-fetcher                            -->
<!-- File:      docs/reference/db-drivers.md            -->
<!-- Purpose:   Reference for the database engines: driver, licence, image, connection form, tail key, checkpoint -->
<!-- Language:  Markdown                                 -->
<!--                                                     -->
<!-- License:   BUSL-1.1                                 -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED              -->

# Database drivers

A `sources.db` instance names an engine, and the engine decides what has to
be in the image: `odbc` speaks to any SQL engine through unixODBC and that
engine's own ODBC driver, `clickhouse` and `mongodb` are pure Rust clients
with nothing to install. The table below is one row per engine: the driver,
its licence, whether the runtime image ships it or the operator supplies it,
the connection form, what a tail needs of its key, and what the tail commits.
The grammar every row shares (`stores`, `batch`, `filter`, `accumulate`) is
annotated in `config.example.yaml`; the code is `crates/db/src/`.

- [What the image ships](#what-the-image-ships)
- [The matrix](#the-matrix)
- [Dialect notes](#dialect-notes)
- [What the tail commits](#what-the-tail-commits)
- [What is proven where](#what-is-proven-where)

## What the image ships

The runtime image the deployment contract generates (`Dockerfile`, from
`dfe-fetcher emit-dockerfile`) installs the transport's shared libraries and
no ODBC package. A binary built with `db-odbc` links unixODBC (`libodbc2` on
Debian) and loads each engine's driver by name from `odbcinst.ini` at
connect time, so an image for an ODBC deployment adds `libodbc2` plus one
driver package per engine it serves. The two LGPL drivers, psqlodbc and
MariaDB Connector/ODBC, are Debian packages and can ship in a HyperI image;
every proprietary driver is a click-through download the operator installs
under its own licence, in the image or in a driver layer. `db-clickhouse` and
`db-mongodb` need nothing from the image.

## The matrix

| Engine | `engine` / `dialect` | Driver | Licence | In the image | Connection form | Tail key | Checkpoint |
|--------|----------------------|--------|---------|--------------|-----------------|----------|------------|
| PostgreSQL, and the PostgreSQL wire (Redshift, CockroachDB, Cloud SQL, RDS) | `odbc` / `postgres` | psqlodbc (`odbc-postgresql`, `PostgreSQL Unicode`) | LGPL-2.1 | Shippable (LGPL, dynamic); not in the generated image today | `Driver={PostgreSQL Unicode};Server=..;Port=..;Database=..;Uid=..;Pwd=..;UseDeclareFetch=1;BoolsAsChar=0` -- `UseDeclareFetch=1` is required or the driver buffers the whole result set; `BoolsAsChar=0` makes booleans JSON booleans | NOT NULL columns of the query's result; a plain name folds to lower case, `"Name"` keeps its case | Key tuple of the last acknowledged row |
| MariaDB, MySQL, and their managed forms | `odbc` / `mysql` | MariaDB Connector/ODBC (`odbc-mariadb`, `MariaDB Unicode`); works against MySQL servers | LGPL-2.1 (MySQL Connector/ODBC is GPL and cannot ship) | Shippable; not in the generated image today | `Driver={MariaDB Unicode};Server=..;Port=..;Database=..;User=..;Password=..;NO_CACHE=1;FORWARDONLY=1` -- both knobs required or the driver buffers the result set | NOT NULL columns; names keep their case, `` `name` `` delimits | Key tuple of the last acknowledged row |
| ClickHouse, ClickHouse Cloud | `clickhouse` | The official HTTP client crate plus `clickhouse-dfe` (pure Rust) | Apache-2.0 | Nothing to install | `http[s]://user:password@host:8123/database` | Columns of the query's result; their types are read from `DESCRIBE (query)` at the first tail and bound as `{k<i>:Type}` placeholders | Key tuple of the last acknowledged row |
| SQL Server, Azure SQL, Synapse | `odbc` / `mssql` | Microsoft ODBC Driver 18 (`msodbcsql18`) from packages.microsoft.com | Microsoft EULA, click-through, redistributable under its terms | Operator-supplied | `Driver={ODBC Driver 18 for SQL Server};Server=host,1433;Database=..;Uid=..;Pwd=..;Encrypt=yes` | NOT NULL columns; names keep their case, `[name]` delimits | Key tuple of the last acknowledged row |
| Oracle, Autonomous, RDS Oracle | `odbc` / `oracle` | Oracle Instant Client ODBC | OTN licence, click-through | Operator-supplied | `Driver={Oracle ODBC driver};DBQ=host:1521/service;UID=..;PWD=..` | NOT NULL columns; a plain name folds to UPPER case, `"Name"` keeps its case; 12c Release 1 or later | Key tuple of the last acknowledged row |
| SQLite | `odbc` / `sqlite` | sqliteodbc (`libsqliteodbc`, `SQLite3`) | Permissive (the driver's own BSD-style licence) | Operator-supplied | `Driver={SQLite3};Database=/path/to/file.db` | NOT NULL columns; names keep their case | Key tuple of the last acknowledged row |
| Snowflake | `odbc` / `snowflake` | Snowflake ODBC driver | Snowflake client licence, click-through | Operator-supplied | `Driver={SnowflakeDSIIDriver};Server=<account>.snowflakecomputing.com;Database=..;Schema=..;Warehouse=..;Uid=..;Pwd=..` | NOT NULL columns; a plain name folds to UPPER case, `"Name"` keeps its case | Key tuple of the last acknowledged row |
| Databricks | `odbc` / `databricks` | Simba Spark ODBC (Databricks-distributed) | Proprietary, click-through | Operator-supplied | `Driver={Simba Spark ODBC Driver};Host=..;Port=443;HTTPPath=..;SSL=1;ThriftTransport=2;AuthMech=3;UID=token;PWD=<token>` | NOT NULL columns; names keep their case, `` `name` `` delimits | Key tuple of the last acknowledged row |
| BigQuery | `odbc` / `bigquery` | Simba Google BigQuery ODBC Connector (Google-distributed) | Proprietary, click-through | Operator-supplied | `Driver={Simba Google BigQuery ODBC Connector};Catalog=<project>;OAuthMechanism=0;Email=<service account>;KeyFilePath=<key.json>` | NOT NULL columns; names keep their case, `` `name` `` delimits | Key tuple of the last acknowledged row |
| Redshift with Amazon's driver | `odbc` / `postgres` | Amazon Redshift ODBC driver, or psqlodbc over the PostgreSQL wire | Apache-2.0 (Amazon's), LGPL-2.1 (psqlodbc) | Operator-supplied (Amazon's), shippable (psqlodbc) | As PostgreSQL, with Amazon's driver name when used | As PostgreSQL | Key tuple of the last acknowledged row |
| MongoDB, Atlas, and the MongoDB wire (DocumentDB, Cosmos DB for MongoDB) | `mongodb` | The official `mongodb` crate and `bson` (pure Rust) | Apache-2.0 (driver), MIT (bson) | Nothing to install | `mongodb://user:password@host:27017/?authSource=admin`, or `mongodb+srv://...` for Atlas | Change stream (the default): none, the server orders events, but the deployment must be a replica set; keyset: `_id` | Change stream: the resume token of the last acknowledged event; keyset: the `_id` of the last acknowledged document |
| DynamoDB, Cosmos DB SQL API, Elasticsearch, OpenSearch | not a `sources.db` engine | -- | -- | -- | A REST profile under `sources.rest` (see `docs/reference/profile-grammar.md`) | -- | -- |

The driver name in a connection string is whatever `odbcinst -q -d` lists on
the host; the names above are the packages' defaults.

## Dialect notes

The keyset SQL a tail runs is built per dialect in `crates/db/src/keyset.rs`,
and the unit tests there pin each of these:

- The predicate. Engines with row-value comparison take `(a, b) > (?, ?)`;
  SQL Server and Oracle do not, and take the expanded
  `a > ? OR (a = ? AND b > ?)` with the leading values bound once per
  disjunct. Values are always bound as ODBC `?` parameters, never written
  into the SQL text.
- The cap. `LIMIT n` everywhere except SQL Server, whose
  `OFFSET 0 ROWS FETCH NEXT n ROWS ONLY` is a sub-clause of `ORDER BY` (so
  the no-key form is `TOP (n)`), and Oracle's `FETCH FIRST n ROWS ONLY`,
  which needs 12c Release 1 or later.
- The alias. The operator's query becomes a derived table, `(query) AS
  dfe_tail`; Oracle refuses `AS` before a table alias (ORA-00933) and gets
  `(query) dfe_tail`.
- Delimiting. A key that is not a plain identifier (a space, a dash, a dot)
  is delimited in the dialect's form: `[name]` on SQL Server, `` `name` `` on
  MariaDB, MySQL, ClickHouse, BigQuery and Databricks, `"name"` elsewhere. A
  key the operator already delimited is written as given, and its bare name
  is what the row JSON is read under.
- Case. A plain (undelimited) key is written as given and the engine folds
  it: PostgreSQL to lower case, Oracle and Snowflake to upper, the rest keep
  it. The row JSON carries the column under the folded name, which is where
  the tail reads its checkpoint value; write a case-sensitive column
  delimited.
- Timestamps. Every engine reports timestamps as RFC 3339 UTC (the session
  is pinned to UTC on PostgreSQL and MariaDB) and the checkpoint binds that
  text back. ClickHouse's parameter parser takes the ISO form without the
  `Z`, so the store binds a `DateTime` key without it under
  `session_timezone=UTC`, whatever zone the column renders in.

## What the tail commits

A tail row carries its key as a mark; the driver folds the marks of a flush
and commits the last one only after the transport has acknowledged the
batch, under the cursor key `{instance}.{connection}.{unit}` in the cursor
store. The next tick binds the committed values back: as the keyset
predicate's parameters on a SQL engine, as `start_after` on a MongoDB change
stream, as `_id > value` on a MongoDB keyset. A tick that fails commits
nothing and re-fetches from the previous checkpoint, so a row is never lost
and may be seen twice after an aborted tick. A change stream sees every
insert, update, replace and delete as an event with the current document; a
keyset tail, on any engine, sees rows whose key sorts after the checkpoint,
which is inserts only unless the key is a change timestamp.

One tick reads pages of `limit` rows, each resuming past the last row of the
page before, and ends on the first short page or at `max_pages_per_tick`: a
store that is behind catches up over several ticks instead of holding one
open, and `tail_pages_full_total` counts the pages that came back full.

A change stream that saw no event still commits. The tick ends with the
stream's post-batch resume token, so an idle collection does not leave the
committed token trailing the oplog's window; once the oplog no longer reaches
that token the server answers code 286, which the tail reports naming the
unit whose checkpoint has to be cleared to restart from now. The stream binds
its token with `start_after` rather than `resume_after`, which is what lets it
resume past the `invalidate` a dropped collection ends a stream with.

## What is proven where

The integration tests under `crates/fetcher/tests/integration/` start their
own database container per test and run the dump and the tail through the
driver: PostgreSQL and MariaDB through their ODBC drivers, ClickHouse over
HTTP, MongoDB as a standalone server for the dump and the keyset tail and as
a one-member replica set for the change stream. Each tail proof inserts rows,
ticks, stops, ticks again on a fresh driver over the same cursor store, and
checks that every row landed once in key order and that the checkpoint after
the first tick was the last acknowledged row's. SQL Server and Oracle have
the same proof, gated on their operator-supplied driver being registered
with the host's driver manager: absent the driver the test skips saying so,
in CI too, because the driver is a click-through licence no shared runner
carries; the dialect each needs is pinned by unit test regardless.
