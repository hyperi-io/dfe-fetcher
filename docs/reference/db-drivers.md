<!-- Project:   dfe-fetcher                            -->
<!-- File:      docs/reference/db-drivers.md            -->
<!-- Purpose:   Reference for the database engines: driver, licence, image, connection form, authentication, tail key, checkpoint -->
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
- [Authentication](#authentication)
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

## Authentication

`connection_string` is ONE string, resolved once when the store first
connects. So an auth form works here only if the whole credential fits in that
string, or in files the container already carries. A form that needs a
short-lived token minted per connection does not work, because nothing
re-resolves the string between connects; those are listed as unsupported below
rather than left to fail at connect time.

Two limits apply to every row. The string is a credential spec, so it may be
`vault:`, `bao:`, `openbao:`, `env:` or `file:` and the plaintext never sits in
config; an `aws:` prefix needs a secrets feature the fetcher does not build and
is refused at load naming the prefix rather than reaching the driver as literal
text. And a
credential the driver reads from disk (a key file, a wallet, a certificate) has
to be mounted into the image; the spec mechanism does not fetch it.

| Engine | Expressible in the connection string | Needs a file mounted | Not expressible here |
|--------|--------------------------------------|----------------------|----------------------|
| PostgreSQL | User and password (`Uid`, `Pwd`). TLS client certificate through psqlodbc's `pqopt`, which forwards libpq's `sslmode`, `sslcert`, `sslkey`, `sslrootcert` | The certificate, key and root CA | Kerberos and GSSAPI: libpq has `krbsrvname`, `gssencmode` and `gsslib`, but psqlodbc documents no keyword for them and only demonstrates `pqopt` with the TLS set, so treat it as undocumented rather than available. Azure Database for PostgreSQL Entra auth -- see below |
| MariaDB, MySQL | User and password. TLS client certificate (`SSLCERT`, `SSLKEY`, `SSLCA`, `SSLVERIFY=1`); absolute paths are required | The certificate, key and CA | -- |
| ClickHouse | User and password in the URL (`http[s]://user:password@host/db`) | -- | -- |
| SQL Server, Azure SQL, Synapse | User and password. Entra service principal (`Authentication=ActiveDirectoryServicePrincipal`, client id in `UID`, secret in `PWD`). Managed identity (`Authentication=ActiveDirectoryMsi`, with `UID` for a user-assigned identity). Kerberos (`Trusted_Connection=yes`) | Nothing for the Entra forms. Kerberos needs an external ticket cache the driver does not create | An Entra ACCESS TOKEN. Microsoft documents no DSN or connection-string keyword for it: it is set through a connection attribute whose buffer must outlive the connection handle, so a config-driven caller cannot supply it. Client certificates are documented only for loopback connections on SQL Server on Linux, not as a general login |
| Oracle | User and password. Nothing else: the ODBC keyword set has no auth parameter beyond these | Wallet and TCPS, and Kerberos, are Oracle Net configuration in `tnsnames.ora` and `sqlnet.ora` found through `TNS_ADMIN`, so the credential is entirely out of band and the string carries none of it | -- |
| Snowflake | User and password. Key-pair JWT (`AUTHENTICATOR=SNOWFLAKE_JWT` with `PRIV_KEY_FILE`, plus `PRIV_KEY_FILE_PWD` for an encrypted key) -- the driver signs the assertion itself. OAuth client credentials (`AUTHENTICATOR=oauth_client_credentials` with `OAUTH_CLIENT_ID`, `OAUTH_CLIENT_SECRET`, `OAUTH_TOKEN_REQUEST_URL`) -- the driver performs the exchange. Workload identity (`AUTHENTICATOR=workload_identity` with `WORKLOAD_IDENTITY_PROVIDER`), which needs no key file at all. A programmatic access token, which the driver's parameter list takes as `AUTHENTICATOR=programmatic_access_token` with `token` while the feature's own guide describes putting it in the password | The private key for the key-pair form | An OAuth token minted elsewhere (`AUTHENTICATOR=oauth` with `token`) -- that is a caller-supplied token, not a static credential |
| Databricks | A personal access token (`AuthMech=3`, `UID=token`, the token in `PWD`). OAuth machine-to-machine (`AuthMech=11`, `Auth_Flow=1`, with `Auth_Client_ID`, `Auth_Client_Secret` and `Auth_Scope=all-apis`), where the driver performs the exchange | -- | Token pass-through (`Auth_Flow=0` with `Auth_AccessToken`), where the caller supplies an OAuth token |
| BigQuery | A service-account key (`OAuthMechanism=0` with `Email` and `KeyFilePath`, or `KeyFile` for the key inline). Application Default Credentials (`OAuthMechanism=3`), which falls back to the metadata server's default service account and so needs no key at all. Workload identity federation (`OAuthMechanism=4`). Service-account impersonation (`SAI_Email`, `SAI_Lifetime`, `SAI_Scopes`) | The key file, unless `KeyFile` carries it inline or `OAuthMechanism=3` is used | -- |
| Redshift | IAM with explicit keys (`IAM=1` with `AccessKeyID`, `SecretAccessKey`, optional `SessionToken`), a shared profile (`Profile`), or the instance role (`InstanceProfile=1`). The driver calls STS and requests cluster credentials itself | `Profile` reads a shared credentials file | The IdP token plugin, which the vendor states the calling application must generate |
| SQLite | Nothing: the file is the database and the driver takes no credential | The database file itself | -- |
| MongoDB | SCRAM (`authMechanism=SCRAM-SHA-256` or `SCRAM-SHA-1`, with `authSource`), which is the on-prem form and the one this engine is for. Workload identity (`authMechanism=MONGODB-OIDC` with `authMechanismProperties=ENVIRONMENT:k8s`, `azure` or `gcp`, the latter two with `TOKEN_RESOURCE`), where the driver mints the token from that environment itself | The projected service-account token for the `k8s` form, which Kubernetes mounts on its own | `MONGODB-AWS` and `GSSAPI`: driver build features this binary does not enable, the first because it links the AWS SDK's credential stack. `MONGODB-OIDC` with no `ENVIRONMENT`, which needs a token callback the driver expects the application to supply. Each is refused at load naming the reason. `MONGODB-X509` is URI-shaped and needs no auth feature, but the driver's own documentation demonstrates X.509 only through a programmatic credential, so it is unproven here rather than supported |

### Forms that need a token minted per connection

These are unsupported today, and the reason is the same in each case: the
credential is a token valid for minutes to an hour, and `connection_string`
resolves once. Supporting them means re-resolving the string per connect, which
#111 also asks for.

- **Azure Database for PostgreSQL** with Entra. The user name is the Entra
  principal and the PASSWORD is an access token, obtained out of band and valid
  for a matter of minutes.
- **Google Cloud SQL** with IAM database authentication. The password is a login
  token valid for about an hour; Google's own guidance is that long-lived or
  pooled processes use a connector rather than a pasted token, and the token can
  exceed a client's password field.
- **Caller-supplied OAuth tokens** on Snowflake, Databricks and Redshift, listed
  per engine above. The driver accepts a token but will not obtain one, so
  something outside the fetcher has to mint and rotate it.

Everything else in the matrix either fits in the string or sits in a file, which
is why the supported set is wider than user and password while still being a
static configuration.

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
