//! PostgreSQL for `datorium-core` (tokio-postgres), the port of the Python
//! reference's `datorium.adapters.postgres`.
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

use std::error::Error;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use datorium_core::dsn::{parse_dsn, postgres_schema};
use datorium_core::identity::system_ms;
use datorium_core::journal::{
    BranchRecord, Journal, JournalError, JournalRecord, State, check_transition, decode_branches,
    encode_branches,
};
use datorium_core::store::{BoxError, Branch, Participant};
use tokio::sync::Mutex;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, NoTls, Row};
use uuid::Uuid;

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

#[derive(Clone)]
struct Connector {
    config: tokio_postgres::Config,
    schema: Option<String>,
}

impl Connector {
    fn new(dsn: &str, schema: Option<&str>) -> Result<Self, ConfigError> {
        let parsed = parse_dsn(dsn).map_err(|e| ConfigError(e.to_string()))?;
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

/// One PostgreSQL database as a datorium store.
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
                 CREATE INDEX IF NOT EXISTS {t}_pending ON {t} (state, updated_ms)",
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
    fn io_errors_are_retryable_and_others_are_not() {
        let io = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset");
        assert!(is_retryable(&io));
        assert!(!is_retryable(&ConfigError("x".into())));
    }
}
