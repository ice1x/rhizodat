//! PostgreSQL for `rhizodat-core` (tokio-postgres), the port of the Python
//! reference's `rhizodat.adapters.postgres`.
//!
//! - [`PostgresStore`]: one database as a store.
//! - [`PostgresBranch`]: one Unit of Work's open transaction. `stage` checks
//!   deferred constraints and writes the applied marker; `prepare` runs
//!   `PREPARE TRANSACTION`.
//! - [`PostgresParticipant`]: `COMMIT PREPARED` / `ROLLBACK PREPARED` and
//!   `pg_prepared_xacts`, for the coordinator and recovery.
//! - [`PostgresJournal`]: the journal table, compare-and-set by
//!   `UPDATE … WHERE state = expected`. Same table and `branches` text as the
//!   Python journal, so either implementation recovers the other's records.
//!
//! Every branch inserts `(dat_txn, dat_tx_time)` into `dat_applied` inside its
//! own transaction, so "did this branch commit?" is one primary-key lookup.
//! The server needs `max_prepared_transactions > 0`. Connections are driven on
//! the tokio runtime of the caller.
//!
//! Two stores in one meta-transaction must not share a `dat_applied` (the same
//! server, database and schema): both branches would insert the same `dat_txn`
//! and the second would wait for the first forever. The Python facade refuses
//! such a configuration; until a Rust facade exists, give each store its own
//! schema.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use rhizodat_core::adapter::{Adapter, Presence, Scheme};
use rhizodat_core::dsn::{DsnError, unquote_plus};
use rhizodat_core::identity::system_ms;
use rhizodat_core::journal::{
    BranchRecord, Journal, JournalError, JournalRecord, State, check_transition, decode_branches,
    encode_branches,
};
use rhizodat_core::migrations::AppliedMigration;
use rhizodat_core::rows::RowValue;
use rhizodat_core::store::{BoxError, Branch, Participant};
use tokio::sync::Mutex;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, NoTls, Row};
use uuid::Uuid;

pub mod parts;
pub mod query;

use parts::{PostgresParts, apply_parts};

/// Columns rhizodat sets on every data row: the meta-transaction and its time.
/// A Unit of Work's own rows must leave them out; `stage` appends them.
const RESERVED: [&str; 2] = ["dat_txn", "dat_tx_time"];

/// A setting the adapter refuses before connecting.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigError(String);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for ConfigError {}

/// `heuristic outcome`: a prepared gid is gone but its marker was never written.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeuristicOutcome {
    /// The vanished gid.
    pub gid: String,
}

impl fmt::Display for HeuristicOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "heuristic outcome: {} is gone but was never applied",
            self.gid
        )
    }
}

impl Error for HeuristicOutcome {}

/// Transaction isolation for a branch (the server default when unset).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Isolation {
    /// `READ COMMITTED`
    ReadCommitted,
    /// `REPEATABLE READ`
    RepeatableRead,
    /// `SERIALIZABLE`
    Serializable,
}

impl Isolation {
    const fn sql(self) -> &'static str {
        match self {
            Isolation::ReadCommitted => "READ COMMITTED",
            Isolation::RepeatableRead => "REPEATABLE READ",
            Isolation::Serializable => "SERIALIZABLE",
        }
    }
}

fn check_identifier(kind: &str, value: &str) -> Result<(), ConfigError> {
    let mut chars = value.chars();
    let ok = value.len() <= 63
        && chars
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    if ok {
        Ok(())
    } else {
        Err(ConfigError(format!(
            "invalid {kind} name {value:?}: use letters, digits and underscores"
        )))
    }
}

/// SQL string literal for statements that take no parameters (gids).
fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn undefined_object(error: &tokio_postgres::Error) -> bool {
    error.code() == Some(&SqlState::UNDEFINED_OBJECT)
}

/// Serialization failures, deadlocks and lost connections; never constraint or
/// syntax errors.
#[must_use]
pub fn is_retryable(error: &(dyn Error + 'static)) -> bool {
    if let Some(pg) = error.downcast_ref::<tokio_postgres::Error>() {
        return match pg.code() {
            Some(code) => {
                *code == SqlState::T_R_SERIALIZATION_FAILURE
                    || *code == SqlState::T_R_DEADLOCK_DETECTED
            }
            None => pg.is_closed() || pg.source().is_some_and(<dyn Error>::is::<std::io::Error>),
        };
    }
    error.is::<std::io::Error>()
}

/// The PostgreSQL store kind: `postgresql://` (alias `postgres://`), port 5432.
pub const ADAPTER: Adapter = Adapter {
    kind: "postgres",
    schemes: &[
        Scheme {
            name: "postgresql",
            transport: None,
            default_port: Some(5432),
            host: Presence::Optional,
            path: Presence::Optional,
        },
        Scheme {
            name: "postgres",
            transport: None,
            default_port: Some(5432),
            host: Presence::Optional,
            path: Presence::Optional,
        },
    ],
    holds_journal: true,
    query_help: Some("SQL with $1, $2 placeholders"),
};

fn is_pg_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    value.len() <= 63
        && chars
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// The DSN for the driver and the schema of a PostgreSQL store or journal.
///
/// The schema is the DSN's `schema` query parameter, or `schema`; both given
/// must agree. The returned DSN is `raw` without that parameter.
///
/// # Errors
///
/// [`DsnError`] for a DSN that does not parse or is not PostgreSQL, an invalid
/// schema name, or two different schemas.
pub fn postgres_schema(
    raw: &str,
    schema: Option<&str>,
) -> Result<(String, Option<String>), DsnError> {
    let dsn = ADAPTER.parse(raw)?;
    let from_dsn = dsn
        .params()
        .iter()
        .find(|(k, _)| k == "schema")
        .map(|(_, v)| v.as_str());
    for value in [from_dsn, schema].into_iter().flatten() {
        if !is_pg_identifier(value) {
            return Err(DsnError::new(format!(
                "invalid schema name {value:?}: a letter or '_', then letters, digits and \
                 underscores, at most 63 characters"
            )));
        }
    }
    match (from_dsn, schema) {
        (Some(d), Some(s)) if d != s => Err(DsnError::new(format!(
            "the DSN names schema {d:?} but schema {s:?} was passed"
        ))),
        (None, _) => Ok((raw.to_owned(), schema.map(str::to_owned))),
        (Some(d), _) => {
            let (base, query) = raw.split_once('?').unwrap_or((raw, ""));
            let kept: Vec<&str> = query
                .split('&')
                .filter(|field| {
                    let key = field.split_once('=').map_or(*field, |(k, _)| k);
                    unquote_plus(key) != "schema"
                })
                .collect();
            let out = if kept.is_empty() {
                base.to_owned()
            } else {
                format!("{base}?{}", kept.join("&"))
            };
            Ok((out, Some(d.to_owned())))
        }
    }
}

#[derive(Clone)]
struct Connector {
    config: tokio_postgres::Config,
    schema: Option<String>,
}

impl Connector {
    fn new(dsn: &str, schema: Option<&str>) -> Result<Self, ConfigError> {
        let parsed = ADAPTER.parse(dsn).map_err(|e| ConfigError(e.to_string()))?;
        if parsed.store() != "postgres" {
            return Err(ConfigError(format!(
                "not a PostgreSQL connection string: {parsed}"
            )));
        }
        let (driver_dsn, schema) =
            postgres_schema(dsn, schema).map_err(|e| ConfigError(e.to_string()))?;
        let config = tokio_postgres::Config::from_str(&driver_dsn)
            .map_err(|_| ConfigError(format!("invalid PostgreSQL connection string: {parsed}")))?;
        Ok(Self { config, schema })
    }

    async fn connect(&self) -> Result<Client, tokio_postgres::Error> {
        let (client, connection) = self.config.connect(NoTls).await?;
        tokio::spawn(async move {
            // Ends when the client is dropped or the server goes away; errors surface on the client.
            let _ = connection.await;
        });
        if let Some(schema) = &self.schema {
            client
                .batch_execute(&format!("SET search_path TO \"{schema}\", public"))
                .await?;
        }
        Ok(client)
    }
}

/// A double-quoted SQL identifier (table or column). The name is checked by
/// [`check_identifier`] first, so this only guards against the odd quote.
fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

/// One value of a row as the text PostgreSQL parses per column (the row is
/// inserted as `$n::text::<column type>`, so the server's input function reads
/// the value exactly as it would from `COPY`/`INSERT` text). `None` is `NULL`.
///
/// # Errors
///
/// A [`BoxError`] for a list or map: nested values need an explicit array or
/// JSON column and are not carried across the boundary yet.
fn pg_text(value: &RowValue) -> Result<Option<String>, BoxError> {
    Ok(match value {
        RowValue::Null => None,
        RowValue::Bool(v) => Some(if *v { "true" } else { "false" }.to_owned()),
        RowValue::Int(v) => Some(v.to_string()),
        RowValue::Float(v) if v.is_finite() => Some(v.to_string()),
        RowValue::Float(v) if v.is_nan() => Some("NaN".to_owned()),
        RowValue::Float(v) => Some(if *v > 0.0 { "Infinity" } else { "-Infinity" }.to_owned()),
        RowValue::Str(v) | RowValue::Decimal(v) | RowValue::Date(v) | RowValue::DateTime(v) => {
            Some(v.clone())
        }
        RowValue::Uuid(v) => Some(v.hyphenated().to_string()),
        RowValue::Bytes(v) => {
            let mut hex = String::with_capacity(2 + v.len() * 2);
            hex.push_str("\\x");
            for byte in v {
                use std::fmt::Write as _;
                let _ = write!(hex, "{byte:02x}");
            }
            Some(hex)
        }
        RowValue::List(_) | RowValue::Map(_) => {
            return Err("lists and maps are not carried into PostgreSQL rows yet".into());
        }
    })
}

/// The `format_type` of each of `needed` columns of `table`, resolved through
/// the branch's `search_path`. Errors when the table lacks one of them.
async fn column_types(
    client: &Client,
    table: &str,
    needed: &[String],
) -> Result<BTreeMap<String, String>, BoxError> {
    let rows = client
        .query(
            "SELECT a.attname, format_type(a.atttypid, a.atttypmod) \
             FROM pg_attribute a \
             WHERE a.attrelid = $1::text::regclass AND a.attnum > 0 AND NOT a.attisdropped",
            &[&quote_ident(table)],
        )
        .await?;
    let types: BTreeMap<String, String> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    let missing: Vec<&str> = needed
        .iter()
        .map(String::as_str)
        .filter(|c| !types.contains_key(*c))
        .collect();
    if missing.is_empty() {
        Ok(types)
    } else {
        Err(format!("table {table:?} has no column(s) {}", missing.join(", ")).into())
    }
}

/// Insert every buffered table's rows into the open transaction, stamping each
/// row with `(dat_txn, dat_tx_time)`. One statement per table (chunked to stay
/// under PostgreSQL's parameter limit), values bound as `$n::text::<type>` so
/// the server parses them per the column's own type.
async fn apply_rows(
    client: &Client,
    rows: &PostgresRows,
    dat_txn: Uuid,
    dat_tx_time: i64,
) -> Result<(), BoxError> {
    for (table, (cols, data)) in &rows.buffer {
        let mut all: Vec<String> = cols.clone();
        all.extend(RESERVED.map(str::to_owned));
        let types = column_types(client, table, &all).await?;
        let names: Vec<String> = all.iter().map(|c| quote_ident(c)).collect();
        // 65535 is the server's bind-parameter limit; keep each statement under it.
        let per_row = all.len();
        let max_rows = (65535 / per_row.max(1)).max(1);
        for chunk in data.chunks(max_rows) {
            let mut params: Vec<Option<String>> = Vec::with_capacity(chunk.len() * per_row);
            let mut tuples: Vec<String> = Vec::with_capacity(chunk.len());
            for row in chunk {
                let mut values: Vec<Option<String>> =
                    row.iter().map(pg_text).collect::<Result<_, _>>()?;
                values.push(pg_text(&RowValue::Uuid(dat_txn))?);
                values.push(pg_text(&RowValue::Int(dat_tx_time))?);
                let mut placeholders = Vec::with_capacity(per_row);
                for (col, value) in all.iter().zip(values) {
                    placeholders.push(format!(
                        "${}::text::{}",
                        params.len() + 1,
                        types.get(col).expect("checked by column_types")
                    ));
                    params.push(value);
                }
                tuples.push(format!("({})", placeholders.join(", ")));
            }
            let sql = format!(
                "INSERT INTO {} ({}) VALUES {}",
                quote_ident(table),
                names.join(", "),
                tuples.join(", ")
            );
            let refs: Vec<&(dyn ToSql + Sync)> =
                params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
            client.execute(&sql, &refs).await?;
        }
    }
    Ok(())
}

/// A Unit of Work's own rows for a PostgreSQL store, buffered client-side and
/// written inside the branch's transaction at [`PostgresBranch::stage`] (so they
/// commit atomically with the meta-transaction, through the two-phase protocol).
///
/// The target table carries the user columns plus `dat_txn uuid` and
/// `dat_tx_time bigint`; those are reserved and [`stage`](PostgresBranch::stage)
/// appends them to every row. Lists and maps are not carried yet.
#[derive(Clone, Default)]
pub struct PostgresRows {
    buffer: BTreeMap<String, (Vec<String>, Vec<Vec<RowValue>>)>,
}

impl PostgresRows {
    /// An empty set of rows.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether no rows have been buffered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Buffer `rows` (one value per column of `columns`) for `table`.
    ///
    /// # Errors
    ///
    /// A [`ConfigError`] for a bad table or column name, a reserved column
    /// (`dat_txn`, `dat_tx_time`), different columns than earlier rows of the
    /// same table, a row of the wrong length, or a list/map value.
    pub fn insert(
        &mut self,
        table: &str,
        columns: &[&str],
        rows: Vec<Vec<RowValue>>,
    ) -> Result<(), ConfigError> {
        check_identifier("table", table)?;
        if rows.is_empty() {
            return Ok(());
        }
        if let Some(reserved) = RESERVED.iter().find(|r| columns.contains(r)) {
            return Err(ConfigError(format!(
                "{reserved} is set by rhizodat; leave it out of the rows"
            )));
        }
        for column in columns {
            check_identifier("column", column)?;
        }
        if let Some(row) = rows.iter().find(|r| r.len() != columns.len()) {
            return Err(ConfigError(format!(
                "a row has {} values for {} columns",
                row.len(),
                columns.len()
            )));
        }
        if rows
            .iter()
            .flatten()
            .any(|v| matches!(v, RowValue::List(_) | RowValue::Map(_)))
        {
            return Err(ConfigError(
                "lists and maps are not carried into PostgreSQL rows yet".to_owned(),
            ));
        }
        let columns: Vec<String> = columns.iter().map(ToString::to_string).collect();
        let (known, buffered) = self
            .buffer
            .entry(table.to_owned())
            .or_insert_with(|| (columns.clone(), Vec::new()));
        if *known != columns {
            return Err(ConfigError(format!(
                "rows for {table:?} must have the same columns: {known:?}"
            )));
        }
        buffered.extend(rows);
        Ok(())
    }
}

/// One PostgreSQL database as a rhizodat store.
#[derive(Clone)]
pub struct PostgresStore {
    name: String,
    connector: Connector,
    isolation: Option<Isolation>,
    participant: Arc<PostgresParticipant>,
}

impl fmt::Debug for PostgresStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresStore")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl PostgresStore {
    /// A store named `name` over `dsn`, optionally in `schema` (or the DSN's
    /// `schema=` parameter; both given must agree).
    ///
    /// # Errors
    ///
    /// [`ConfigError`] for a DSN of another store, an unparsable DSN, a bad
    /// schema name or two different schemas.
    pub fn new(dsn: &str, name: &str, schema: Option<&str>) -> Result<Self, ConfigError> {
        let connector = Connector::new(dsn, schema)?;
        let participant = Arc::new(PostgresParticipant {
            connector: connector.clone(),
            client: Mutex::new(None),
        });
        Ok(Self {
            name: name.to_owned(),
            connector,
            isolation: None,
            participant,
        })
    }

    /// The default isolation of new branches.
    #[must_use]
    pub fn with_isolation(mut self, isolation: Isolation) -> Self {
        self.isolation = Some(isolation);
        self
    }

    /// The store name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// A new autocommit connection for native access.
    ///
    /// # Errors
    ///
    /// The driver's error when the server cannot be reached.
    pub async fn connect(&self) -> Result<Client, tokio_postgres::Error> {
        self.connector.connect().await
    }

    /// Run `statements` in one transaction, the reference's `sql` migration step
    /// (PostgreSQL DDL is transactional, so a step applies whole or not at all).
    ///
    /// # Errors
    ///
    /// The driver's error; nothing of the step stays applied.
    pub async fn run_sql(&self, statements: &[String]) -> Result<(), tokio_postgres::Error> {
        let mut client = self.connect().await?;
        let tx = client.transaction().await?;
        for sql in statements {
            tx.batch_execute(sql).await?;
        }
        tx.commit().await
    }

    /// Create the service table `dat_applied` if it does not exist.
    ///
    /// # Errors
    ///
    /// The driver's error.
    pub async fn install(&self) -> Result<(), tokio_postgres::Error> {
        self.connect()
            .await?
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS dat_applied (\
                 dat_txn uuid PRIMARY KEY, \
                 dat_tx_time bigint NOT NULL, \
                 applied_at timestamptz NOT NULL DEFAULT now())",
            )
            .await
    }

    /// A new branch with an open transaction.
    ///
    /// # Errors
    ///
    /// The driver's error when the connection or `BEGIN` fails.
    pub async fn branch(&self) -> Result<PostgresBranch, tokio_postgres::Error> {
        let client = self.connector.connect().await?;
        let begin = match self.isolation {
            Some(level) => format!("BEGIN ISOLATION LEVEL {}", level.sql()),
            None => "BEGIN".to_owned(),
        };
        client.batch_execute(&begin).await?;
        Ok(PostgresBranch {
            store: self.name.clone(),
            connector: self.connector.clone(),
            client: Some(client),
            gid: None,
            rows: PostgresRows::new(),
            parts: PostgresParts::new(),
        })
    }

    /// The store's participant (one per store, shared).
    #[must_use]
    pub fn participant(&self) -> Arc<PostgresParticipant> {
        self.participant.clone()
    }
}

/// One Unit of Work's open transaction in PostgreSQL.
pub struct PostgresBranch {
    store: String,
    connector: Connector,
    client: Option<Client>,
    gid: Option<String>,
    rows: PostgresRows,
    parts: PostgresParts,
}

fn gone() -> BoxError {
    "the branch's transaction is no longer open".into()
}

impl PostgresBranch {
    /// The open transaction's client, for the Unit of Work's statements.
    ///
    /// # Errors
    ///
    /// When the branch is already prepared, committed or rolled back.
    pub fn client(&self) -> Result<&Client, BoxError> {
        self.client.as_ref().ok_or_else(gone)
    }

    /// Replace the branch's buffered rows (written at [`stage`](Self::stage)).
    pub fn load(&mut self, rows: PostgresRows) {
        self.rows = rows;
    }

    /// Replace the branch's buffered part writes (applied at [`stage`](Self::stage)).
    pub fn load_parts(&mut self, parts: PostgresParts) {
        self.parts = parts;
    }

    /// Buffer `rows` (one value per column of `columns`) for `table`, to be
    /// written inside this branch's transaction at [`stage`](Self::stage).
    ///
    /// # Errors
    ///
    /// As [`PostgresRows::insert`].
    pub fn insert(
        &mut self,
        table: &str,
        columns: &[&str],
        rows: Vec<Vec<RowValue>>,
    ) -> Result<(), ConfigError> {
        self.rows.insert(table, columns, rows)
    }
}

#[async_trait]
impl Branch for PostgresBranch {
    fn store(&self) -> &str {
        &self.store
    }

    fn two_phase(&self) -> bool {
        true
    }

    async fn stage(
        &mut self,
        dat_txn: Uuid,
        dat_tx_time: i64,
    ) -> Result<Option<Vec<u8>>, BoxError> {
        let client = self.client()?;
        client
            .batch_execute("SET CONSTRAINTS ALL IMMEDIATE")
            .await?; // fail here, not at prepare
        client
            .execute(
                "INSERT INTO dat_applied (dat_txn, dat_tx_time) VALUES ($1, $2)",
                &[&dat_txn, &dat_tx_time],
            )
            .await?;
        apply_rows(client, &self.rows, dat_txn, dat_tx_time).await?;
        apply_parts(client, &self.parts, dat_txn, dat_tx_time).await?;
        Ok(None)
    }

    async fn prepare(&mut self, gid: &str) -> Result<(), BoxError> {
        self.client()?
            .batch_execute(&format!("PREPARE TRANSACTION {}", literal(gid)))
            .await?;
        self.gid = Some(gid.to_owned());
        // The prepared transaction no longer belongs to this session: free the connection.
        self.client = None;
        Ok(())
    }

    async fn commit_one_phase(&mut self) -> Result<(), BoxError> {
        let client = self.client.take().ok_or_else(gone)?;
        client.batch_execute("COMMIT").await?;
        Ok(())
    }

    async fn rollback(&mut self) -> Result<(), BoxError> {
        let Some(gid) = &self.gid else {
            if let Some(client) = self.client.take()
                && !client.is_closed()
            {
                client.batch_execute("ROLLBACK").await?;
            }
            return Ok(());
        };
        let client = self.connector.connect().await?;
        match client
            .batch_execute(&format!("ROLLBACK PREPARED {}", literal(gid)))
            .await
        {
            Err(e) if !undefined_object(&e) => Err(e.into()),
            _ => Ok(()),
        }
    }

    fn is_retryable(&self, error: &(dyn Error + 'static)) -> bool {
        is_retryable(error)
    }
}

/// Resolves prepared gids. One connection, reopened after it is lost.
pub struct PostgresParticipant {
    connector: Connector,
    client: Mutex<Option<Client>>,
}

impl fmt::Debug for PostgresParticipant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresParticipant")
            .finish_non_exhaustive()
    }
}

impl PostgresParticipant {
    async fn query(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<Vec<Row>, tokio_postgres::Error> {
        let mut slot = self.client.lock().await;
        if slot.as_ref().is_none_or(Client::is_closed) {
            *slot = Some(self.connector.connect().await?);
        }
        let client = slot.as_ref().expect("connected above");
        let result = client.query(sql, params).await;
        if result.as_ref().is_err_and(|_| client.is_closed()) {
            *slot = None;
        }
        result
    }

    async fn simple(&self, sql: &str) -> Result<(), tokio_postgres::Error> {
        let mut slot = self.client.lock().await;
        if slot.as_ref().is_none_or(Client::is_closed) {
            *slot = Some(self.connector.connect().await?);
        }
        let client = slot.as_ref().expect("connected above");
        let result = client.batch_execute(sql).await;
        if result.is_err() && client.is_closed() {
            *slot = None;
        }
        result
    }
}

#[async_trait]
impl Participant for PostgresParticipant {
    async fn finish(
        &self,
        branch: &BranchRecord,
        dat_txn: Uuid,
        _dat_tx_time: i64,
    ) -> Result<(), BoxError> {
        match self
            .simple(&format!("COMMIT PREPARED {}", literal(&branch.gid)))
            .await
        {
            Ok(()) => Ok(()),
            Err(e) if undefined_object(&e) => {
                if self.is_applied(branch, dat_txn).await? {
                    Ok(())
                } else {
                    Err(Box::new(HeuristicOutcome {
                        gid: branch.gid.clone(),
                    }))
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn is_applied(&self, _branch: &BranchRecord, dat_txn: Uuid) -> Result<bool, BoxError> {
        Ok(!self
            .query("SELECT 1 FROM dat_applied WHERE dat_txn = $1", &[&dat_txn])
            .await?
            .is_empty())
    }

    async fn abort(&self, branch: &BranchRecord) -> Result<(), BoxError> {
        match self
            .simple(&format!("ROLLBACK PREPARED {}", literal(&branch.gid)))
            .await
        {
            Err(e) if !undefined_object(&e) => Err(e.into()),
            _ => Ok(()),
        }
    }

    async fn prepared_gids(&self) -> Result<Vec<String>, BoxError> {
        let rows = self
            .query("SELECT gid FROM pg_prepared_xacts WHERE database = current_database() ORDER BY prepared", &[])
            .await?;
        Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
    }

    fn is_retryable(&self, error: &(dyn Error + 'static)) -> bool {
        is_retryable(error)
    }
}

type WallFn = dyn Fn() -> i64 + Send + Sync;

const COLUMNS: &str = "dat_txn, state, dat_tx_time, branches, created_ms, updated_ms";

/// The journal in PostgreSQL: shared and durable, so recovery can run on any host.
pub struct PostgresJournal {
    connector: Connector,
    table: String,
    wall_ms: Box<WallFn>,
    client: Mutex<Option<Client>>,
}

impl fmt::Debug for PostgresJournal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresJournal")
            .field("table", &self.table)
            .finish_non_exhaustive()
    }
}

fn backend(e: impl Into<BoxError>) -> JournalError {
    JournalError::Backend(e.into())
}

impl PostgresJournal {
    /// Open (and create if needed) the journal table `table` in `schema` (or the
    /// DSN's `schema=` parameter; both given must agree).
    ///
    /// # Errors
    ///
    /// [`JournalError::Backend`] for a bad setting or an unreachable server.
    pub async fn open(dsn: &str, schema: Option<&str>, table: &str) -> Result<Self, JournalError> {
        check_identifier("table", table).map_err(backend)?;
        let connector = Connector::new(dsn, schema).map_err(backend)?;
        let journal = Self {
            connector,
            table: table.to_owned(),
            wall_ms: Box::new(system_ms),
            client: Mutex::new(None),
        };
        journal
            .batch(&format!(
                "CREATE TABLE IF NOT EXISTS {t} (\
                 dat_txn uuid PRIMARY KEY, state text NOT NULL, dat_tx_time bigint, \
                 branches text NOT NULL, created_ms bigint NOT NULL, updated_ms bigint NOT NULL); \
                 CREATE INDEX IF NOT EXISTS {t}_pending ON {t} (state, updated_ms); \
                 CREATE TABLE IF NOT EXISTS {t}_migrations (id bigint PRIMARY KEY, \
                 name text NOT NULL, checksum text NOT NULL, dat_tx_time bigint NOT NULL)",
                t = journal.table
            ))
            .await?;
        Ok(journal)
    }

    /// An injected wall clock (Unix ms) for `created_ms` / `updated_ms`.
    #[must_use]
    pub fn with_wall(mut self, wall_ms: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        self.wall_ms = Box::new(wall_ms);
        self
    }

    /// The schema migrations recorded in `<table>_migrations` (the reference's
    /// history table), by id.
    ///
    /// # Errors
    ///
    /// [`JournalError::Backend`] for the server's error.
    pub async fn applied_migrations(&self) -> Result<Vec<AppliedMigration>, JournalError> {
        let rows = self
            .query(
                &format!(
                    "SELECT id, name, checksum, dat_tx_time FROM {}_migrations ORDER BY id",
                    self.table
                ),
                &[],
            )
            .await?;
        Ok(rows
            .iter()
            .map(|r| AppliedMigration {
                id: r.get(0),
                name: r.get(1),
                checksum: r.get(2),
                dat_tx_time: r.get(3),
            })
            .collect())
    }

    /// Record `applied`: `Ok(true)`, or `Ok(false)` when its id is already recorded
    /// (another runner got there first).
    ///
    /// # Errors
    ///
    /// [`JournalError::Backend`] for the server's error.
    pub async fn record_migration(&self, applied: &AppliedMigration) -> Result<bool, JournalError> {
        let rows = self
            .query(
                &format!(
                    "INSERT INTO {}_migrations (id, name, checksum, dat_tx_time) \
                     VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING RETURNING id",
                    self.table
                ),
                &[
                    &applied.id,
                    &applied.name,
                    &applied.checksum,
                    &applied.dat_tx_time,
                ],
            )
            .await?;
        Ok(!rows.is_empty())
    }

    async fn batch(&self, sql: &str) -> Result<(), JournalError> {
        let mut slot = self.client.lock().await;
        let client = self.ensure(&mut slot).await?;
        let result = client.batch_execute(sql).await;
        Self::settle(&mut slot, result).map_err(backend)
    }

    async fn query(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<Vec<Row>, JournalError> {
        let mut slot = self.client.lock().await;
        let client = self.ensure(&mut slot).await?;
        let result = client.query(sql, params).await;
        Self::settle(&mut slot, result).map_err(backend)
    }

    async fn ensure<'a>(&self, slot: &'a mut Option<Client>) -> Result<&'a Client, JournalError> {
        if slot.as_ref().is_none_or(Client::is_closed) {
            *slot = Some(self.connector.connect().await.map_err(backend)?);
        }
        slot.as_ref()
            .ok_or_else(|| backend("journal connection lost"))
    }

    fn settle<T>(
        slot: &mut Option<Client>,
        result: Result<T, tokio_postgres::Error>,
    ) -> Result<T, tokio_postgres::Error> {
        if result.is_err() && slot.as_ref().is_some_and(Client::is_closed) {
            *slot = None;
        }
        result
    }

    fn record(row: &Row) -> Result<JournalRecord, JournalError> {
        let state: String = row.get(1);
        Ok(JournalRecord {
            dat_txn: row.get(0),
            state: state.parse().map_err(backend)?,
            dat_tx_time: row.get(2),
            branches: decode_branches(row.get(3))?,
            created_ms: row.get(4),
            updated_ms: row.get(5),
        })
    }
}

#[async_trait]
impl Journal for PostgresJournal {
    async fn begin(
        &self,
        dat_txn: Uuid,
        branches: &[BranchRecord],
    ) -> Result<JournalRecord, JournalError> {
        let now = (self.wall_ms)();
        let rows = self
            .query(
                &format!(
                    "INSERT INTO {} ({COLUMNS}) VALUES ($1, $2, NULL, $3, $4, $4) \
                     ON CONFLICT (dat_txn) DO NOTHING RETURNING dat_txn",
                    self.table
                ),
                &[
                    &dat_txn,
                    &State::Active.as_str(),
                    &encode_branches(branches),
                    &now,
                ],
            )
            .await?;
        if rows.is_empty() {
            return Err(JournalError::AlreadyJournaled(dat_txn));
        }
        Ok(JournalRecord {
            dat_txn,
            state: State::Active,
            branches: branches.to_vec(),
            dat_tx_time: None,
            created_ms: now,
            updated_ms: now,
        })
    }

    async fn transition(
        &self,
        dat_txn: Uuid,
        expected: State,
        new: State,
        dat_tx_time: Option<i64>,
    ) -> Result<JournalRecord, JournalError> {
        check_transition(expected, new, dat_tx_time)?;
        let rows = self
            .query(
                &format!(
                    "UPDATE {} SET state = $1, dat_tx_time = COALESCE($2, dat_tx_time), updated_ms = $3 \
                     WHERE dat_txn = $4 AND state = $5 RETURNING {COLUMNS}",
                    self.table
                ),
                &[&new.as_str(), &dat_tx_time, &(self.wall_ms)(), &dat_txn, &expected.as_str()],
            )
            .await?;
        if let Some(row) = rows.first() {
            return Self::record(row);
        }
        let actual = self.get(dat_txn).await?.map(|r| r.state);
        Err(JournalError::Conflict {
            dat_txn,
            expected,
            actual,
        })
    }

    async fn get(&self, dat_txn: Uuid) -> Result<Option<JournalRecord>, JournalError> {
        let rows = self
            .query(
                &format!("SELECT {COLUMNS} FROM {} WHERE dat_txn = $1", self.table),
                &[&dat_txn],
            )
            .await?;
        rows.first().map(Self::record).transpose()
    }

    async fn pending(
        &self,
        updated_before_ms: i64,
        limit: Option<usize>,
    ) -> Result<Vec<JournalRecord>, JournalError> {
        let limit = limit.map(|l| i64::try_from(l).unwrap_or(i64::MAX));
        let rows = self
            .query(
                &format!(
                    "SELECT {COLUMNS} FROM {} WHERE state IN ('ACTIVE', 'PREPARED', 'COMMITTED') \
                     AND updated_ms < $1 ORDER BY updated_ms, created_ms LIMIT $2",
                    self.table
                ),
                &[&updated_before_ms, &limit],
            )
            .await?;
        rows.iter().map(Self::record).collect()
    }

    async fn forget(&self, dat_txn: Uuid) -> Result<(), JournalError> {
        match self.get(dat_txn).await? {
            None => Ok(()),
            Some(r) if !r.state.is_final() => Err(JournalError::NotFinal {
                dat_txn,
                state: r.state,
            }),
            Some(_) => {
                self.query(
                    &format!(
                        "DELETE FROM {} WHERE dat_txn = $1 AND state IN ('ABORTED', 'DONE')",
                        self.table
                    ),
                    &[&dat_txn],
                )
                .await?;
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_are_checked() {
        assert!(check_identifier("schema", "t_1").is_ok());
        assert!(check_identifier("schema", "_x").is_ok());
        for bad in ["", "1a", "a-b", "a\"b", &"x".repeat(64)] {
            assert!(check_identifier("schema", bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn gids_are_quoted() {
        assert_eq!(literal("dat:x:o'k"), "'dat:x:o''k'");
    }

    #[test]
    fn only_postgres_dsns_are_accepted() {
        assert!(PostgresStore::new("clickhouse://h/db", "pg", None).is_err());
        assert!(PostgresStore::new("postgresql://h/db", "pg", Some("bad-name")).is_err());
        let err = PostgresStore::new("mysql://u:hunter2@h/db", "pg", None).unwrap_err();
        assert!(!err.to_string().contains("hunter2"));
        assert!(PostgresStore::new("postgresql://u:p@h:5432/db", "pg", Some("s")).is_ok());
    }

    #[test]
    fn buffered_rows_are_validated() {
        let mut rows = PostgresRows::new();
        assert!(rows.is_empty());
        // A reserved column is rejected.
        assert!(
            rows.insert("t", &["dat_txn"], vec![vec![RowValue::Null]])
                .is_err()
        );
        // Bad table and column names are rejected.
        assert!(
            rows.insert("bad name", &["a"], vec![vec![RowValue::Int(1)]])
                .is_err()
        );
        assert!(
            rows.insert("t", &["a b"], vec![vec![RowValue::Int(1)]])
                .is_err()
        );
        // Empty rows is a no-op that buffers nothing.
        rows.insert("t", &["a"], vec![]).unwrap();
        assert!(rows.is_empty());
        // A first good batch, then a mismatched one for the same table.
        rows.insert("t", &["a"], vec![vec![RowValue::Int(1)]])
            .unwrap();
        assert!(
            rows.insert("t", &["b"], vec![vec![RowValue::Int(2)]])
                .is_err()
        );
        // A row of the wrong length is rejected.
        assert!(
            rows.insert("t", &["a"], vec![vec![RowValue::Int(1), RowValue::Int(2)]])
                .is_err()
        );
        // Lists and maps are not carried yet.
        assert!(
            rows.insert("u", &["a"], vec![vec![RowValue::List(vec![])]])
                .is_err()
        );
        assert!(!rows.is_empty());
    }

    #[test]
    fn io_errors_are_retryable_and_others_are_not() {
        let io = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset");
        assert!(is_retryable(&io));
        assert!(!is_retryable(&ConfigError("x".into())));
    }
}
