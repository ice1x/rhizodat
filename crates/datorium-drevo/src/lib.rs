//! drevo for `datorium-core`, the port of the Python reference's
//! `datorium.adapters.drevo`: over Bolt (neo4rs) or embedded in the process
//! (the `drevo` crate's native engine, cargo feature `embedded`).
//!
//! - [`DrevoStore`]: one drevo graph as a store.
//! - [`DrevoBranch`]: one Unit of Work's transaction. `stage` writes the
//!   applied marker `(:DatApplied {dat_txn, dat_tx_time})`; `prepare` prepares
//!   the transaction under the gid (`drevo.tx.prepare` over Bolt).
//! - [`DrevoParticipant`]: commit / roll back a prepared gid, list them.
//! - [`DrevoJournal`]: one node per meta-transaction, the same nodes as the
//!   Python journal, so either implementation recovers the other's records.
//!
//! DSNs: `drevo+bolt://user:pass@host:7687`, `drevo:///abs/path.drevo`
//! (embedded, durable) and `drevo+mem://` (embedded, in memory, tests only).
//!
//! While a branch is prepared, drevo's fence rejects every other commit to that
//! graph (a retryable error), even of a read-only transaction. The journal
//! must therefore live in another graph than any drevo store of the same
//! deployment, and reads run in auto-commit mode, which the fence lets through.
//! Writes outside a branch run in their own explicit transaction: neo4rs
//! retries a failed auto-commit statement on transient errors for up to 60
//! seconds, which would hide the fence from datorium's retry policy, and it
//! never retries explicit transactions.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datorium_core::dsn::parse_dsn;
use datorium_core::identity::system_ms;
use datorium_core::journal::{
    BranchRecord, Journal, JournalError, JournalRecord, State, check_transition, decode_branches,
    encode_branches,
};
use datorium_core::store::{BoxError, Branch, Participant};
use uuid::Uuid;

mod bolt;
#[cfg(feature = "embedded")]
mod embedded;

const MARKER: &str = "CREATE (:DatApplied {dat_txn: $dat_txn, dat_tx_time: $dat_tx_time})";
const IS_APPLIED: &str = "MATCH (m:DatApplied {dat_txn: $dat_txn}) RETURN count(m) AS n";

// --- values ------------------------------------------------------------------------------

/// A Cypher parameter or result value.
#[derive(Clone, Debug, PartialEq, serde::Deserialize)]
#[serde(untagged)]
pub enum Value {
    /// `NULL`
    Null,
    /// A boolean.
    Bool(bool),
    /// A 64-bit integer.
    Int(i64),
    /// A 64-bit float.
    Float(f64),
    /// A string.
    Str(String),
    /// A list.
    List(Vec<Value>),
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::Str(v.to_owned())
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::Str(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int(v)
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}

impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(v: Option<T>) -> Self {
        v.map_or(Value::Null, Into::into)
    }
}

impl<T: Into<Value>> From<Vec<T>> for Value {
    fn from(v: Vec<T>) -> Self {
        Value::List(v.into_iter().map(Into::into).collect())
    }
}

/// Named Cypher parameters.
pub type Params = Vec<(String, Value)>;

/// Build [`Params`]: `params![("id", id.to_string()), ("n", 3_i64)]`.
#[macro_export]
macro_rules! params {
    ($(($name:expr, $value:expr)),* $(,)?) => {
        vec![$(($name.to_string(), $crate::Value::from($value))),*]
    };
}

/// A column the row does not have, or a value of another type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnError(String);

impl fmt::Display for ColumnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for ColumnError {}

/// One result row, by column name.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Row(pub BTreeMap<String, Value>);

impl Row {
    /// The value of `column`, `NULL` when the row has none.
    #[must_use]
    pub fn value(&self, column: &str) -> &Value {
        self.0.get(column).unwrap_or(&Value::Null)
    }

    /// `column` as a string.
    ///
    /// # Errors
    ///
    /// [`ColumnError`] when it is missing or not a string.
    pub fn str(&self, column: &str) -> Result<&str, ColumnError> {
        match self.value(column) {
            Value::Str(s) => Ok(s),
            other => Err(ColumnError(format!(
                "column {column:?} is not a string: {other:?}"
            ))),
        }
    }

    /// `column` as an integer, `None` for `NULL`.
    ///
    /// # Errors
    ///
    /// [`ColumnError`] for a value of another type.
    pub fn opt_int(&self, column: &str) -> Result<Option<i64>, ColumnError> {
        match self.value(column) {
            Value::Int(n) => Ok(Some(*n)),
            Value::Null => Ok(None),
            other => Err(ColumnError(format!(
                "column {column:?} is not an integer: {other:?}"
            ))),
        }
    }

    /// `column` as an integer.
    ///
    /// # Errors
    ///
    /// [`ColumnError`] when it is missing or not an integer.
    pub fn int(&self, column: &str) -> Result<i64, ColumnError> {
        self.opt_int(column)?
            .ok_or_else(|| ColumnError(format!("column {column:?} is NULL")))
    }
}

// --- backends ----------------------------------------------------------------------------

/// How one prepared gid was resolved.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Resolved {
    Done,
    /// No transaction is prepared under this gid (never prepared, or already resolved).
    UnknownGid,
}

/// One graph: Bolt or embedded.
#[async_trait]
trait Backend: Send + Sync {
    /// A read (or a `drevo.tx.*` call over Bolt) in auto-commit mode.
    async fn read(&self, cypher: &str, params: Params) -> Result<Vec<Row>, BoxError>;
    /// An explicit transaction.
    async fn begin(&self) -> Result<Box<dyn Tx>, BoxError>;
    /// Commit (`true`) or roll back a prepared gid.
    async fn resolve(&self, commit: bool, gid: &str) -> Result<Resolved, BoxError>;
    async fn list_prepared(&self) -> Result<Vec<String>, BoxError>;
    fn is_retryable(&self, error: &(dyn Error + 'static)) -> bool;
}

#[async_trait]
trait Tx: Send {
    async fn run(&mut self, cypher: &str, params: Params) -> Result<Vec<Row>, BoxError>;
    /// Prepare under `gid`; the transaction is closed either way.
    async fn prepare(self: Box<Self>, gid: &str) -> Result<(), BoxError>;
    async fn commit(self: Box<Self>) -> Result<(), BoxError>;
    async fn rollback(self: Box<Self>) -> Result<(), BoxError>;
}

/// A write in its own explicit transaction: committed once, never retried.
async fn write(backend: &dyn Backend, cypher: &str, params: Params) -> Result<Vec<Row>, BoxError> {
    let mut tx = backend.begin().await?;
    match tx.run(cypher, params).await {
        Ok(found) => {
            tx.commit().await?;
            Ok(found)
        }
        Err(e) => {
            tx.rollback().await.ok();
            Err(e)
        }
    }
}

/// A setting the adapter refuses before connecting.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigError(String);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for ConfigError {}

/// A prepared gid is gone but its marker was never written.
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

/// Which graph a DSN names, to keep the journal off every store's graph:
/// `["bolt", host, port]` or `["embedded", path]`; `None` for `drevo+mem://`,
/// which is always a graph of its own.
///
/// # Errors
///
/// [`ConfigError`] for a DSN of another store.
pub fn graph_location(dsn: &str) -> Result<Option<Vec<String>>, ConfigError> {
    let parsed = parse_dsn(dsn).map_err(|e| ConfigError(e.to_string()))?;
    if parsed.store() != "drevo" {
        return Err(ConfigError(format!(
            "not a drevo connection string: {parsed}"
        )));
    }
    Ok(match parsed.transport() {
        Some("bolt") => Some(vec![
            "bolt".into(),
            parsed.host().unwrap_or_default().into(),
            parsed.port().unwrap_or(7687).to_string(),
        ]),
        Some("embedded") => Some(vec![
            "embedded".into(),
            parsed.path().unwrap_or_default().into(),
        ]),
        _ => None,
    })
}

fn open_backend(dsn: &str) -> Result<Arc<dyn Backend>, ConfigError> {
    let parsed = parse_dsn(dsn).map_err(|e| ConfigError(e.to_string()))?;
    if parsed.store() != "drevo" {
        return Err(ConfigError(format!(
            "not a drevo connection string: {parsed}"
        )));
    }
    match parsed.transport() {
        Some("bolt") => Ok(Arc::new(bolt::Bolt::connect(&parsed)?)),
        #[cfg(feature = "embedded")]
        Some("embedded") => Ok(Arc::new(embedded::Embedded::open(parsed.path())?)),
        #[cfg(feature = "embedded")]
        Some("memory") => Ok(Arc::new(embedded::Embedded::open(None)?)),
        _ => Err(ConfigError(format!(
            "{parsed}: embedded drevo needs datorium-drevo built with the `embedded` feature"
        ))),
    }
}

// --- store, branch, participant ------------------------------------------------------------

/// One drevo graph as a datorium store.
#[derive(Clone)]
pub struct DrevoStore {
    name: String,
    backend: Arc<dyn Backend>,
    participant: Arc<DrevoParticipant>,
}

impl fmt::Debug for DrevoStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DrevoStore")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl DrevoStore {
    /// A store named `name` over a drevo DSN. An embedded graph is opened here;
    /// clone the store (not the DSN) to share it within the process.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] for another kind of DSN, or an embedded one that cannot
    /// be opened (or without the `embedded` feature).
    pub fn new(dsn: &str, name: &str) -> Result<Self, ConfigError> {
        let backend = open_backend(dsn)?;
        let participant = Arc::new(DrevoParticipant {
            backend: backend.clone(),
        });
        Ok(Self {
            name: name.to_owned(),
            backend,
            participant,
        })
    }

    /// The store name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// A native read in auto-commit mode, outside any meta-transaction.
    ///
    /// # Errors
    ///
    /// The engine's error.
    pub async fn read(&self, cypher: &str, params: Params) -> Result<Vec<Row>, BoxError> {
        self.backend.read(cypher, params).await
    }

    /// A native write in its own transaction, outside any meta-transaction.
    /// While a branch of this graph is prepared it fails with a retryable
    /// error (the fence) instead of waiting.
    ///
    /// # Errors
    ///
    /// The engine's error.
    pub async fn write(&self, cypher: &str, params: Params) -> Result<Vec<Row>, BoxError> {
        write(self.backend.as_ref(), cypher, params).await
    }

    /// Whether `error` (from this store) may go away on a retry: the fence,
    /// write conflicts, lost connections.
    #[must_use]
    pub fn is_retryable(&self, error: &(dyn Error + 'static)) -> bool {
        self.backend.is_retryable(error)
    }

    /// A new branch with an open transaction.
    ///
    /// # Errors
    ///
    /// The engine's error when no transaction can be started.
    pub async fn branch(&self) -> Result<DrevoBranch, BoxError> {
        let tx = self.backend.begin().await?;
        Ok(DrevoBranch {
            store: self.name.clone(),
            backend: self.backend.clone(),
            tx: Some(tx),
            gid: None,
        })
    }

    /// The store's participant (one per store, shared).
    #[must_use]
    pub fn participant(&self) -> Arc<DrevoParticipant> {
        self.participant.clone()
    }
}

/// One Unit of Work's transaction in a drevo graph.
pub struct DrevoBranch {
    store: String,
    backend: Arc<dyn Backend>,
    tx: Option<Box<dyn Tx>>,
    gid: Option<String>,
}

fn gone() -> BoxError {
    "the branch's transaction is no longer open".into()
}

impl DrevoBranch {
    /// Run Cypher inside this branch's transaction (it reads its own writes).
    ///
    /// # Errors
    ///
    /// The engine's error, or when the branch is no longer open.
    pub async fn run(&mut self, cypher: &str, params: Params) -> Result<Vec<Row>, BoxError> {
        self.tx.as_mut().ok_or_else(gone)?.run(cypher, params).await
    }
}

#[async_trait]
impl Branch for DrevoBranch {
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
        self.run(
            MARKER,
            params![
                ("dat_txn", dat_txn.to_string()),
                ("dat_tx_time", dat_tx_time)
            ],
        )
        .await?;
        Ok(None)
    }

    async fn prepare(&mut self, gid: &str) -> Result<(), BoxError> {
        self.tx.take().ok_or_else(gone)?.prepare(gid).await?;
        self.gid = Some(gid.to_owned());
        Ok(())
    }

    async fn commit_one_phase(&mut self) -> Result<(), BoxError> {
        self.tx.take().ok_or_else(gone)?.commit().await
    }

    async fn rollback(&mut self) -> Result<(), BoxError> {
        if let Some(tx) = self.tx.take() {
            return tx.rollback().await;
        }
        if let Some(gid) = &self.gid {
            self.backend.resolve(false, gid).await?; // an unknown gid is already resolved
        }
        Ok(())
    }

    fn is_retryable(&self, error: &(dyn Error + 'static)) -> bool {
        self.backend.is_retryable(error)
    }
}

/// Resolves prepared gids of one drevo graph.
pub struct DrevoParticipant {
    backend: Arc<dyn Backend>,
}

impl fmt::Debug for DrevoParticipant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DrevoParticipant").finish_non_exhaustive()
    }
}

#[async_trait]
impl Participant for DrevoParticipant {
    async fn finish(
        &self,
        branch: &BranchRecord,
        dat_txn: Uuid,
        _dat_tx_time: i64,
    ) -> Result<(), BoxError> {
        match self.backend.resolve(true, &branch.gid).await? {
            Resolved::Done => Ok(()),
            Resolved::UnknownGid if self.is_applied(branch, dat_txn).await? => Ok(()),
            Resolved::UnknownGid => Err(Box::new(HeuristicOutcome {
                gid: branch.gid.clone(),
            })),
        }
    }

    async fn is_applied(&self, _branch: &BranchRecord, dat_txn: Uuid) -> Result<bool, BoxError> {
        let found = self
            .backend
            .read(IS_APPLIED, params![("dat_txn", dat_txn.to_string())])
            .await?;
        Ok(found.first().map(|r| r.int("n")).transpose()?.unwrap_or(0) > 0)
    }

    async fn abort(&self, branch: &BranchRecord) -> Result<(), BoxError> {
        self.backend.resolve(false, &branch.gid).await.map(|_| ())
    }

    async fn prepared_gids(&self) -> Result<Vec<String>, BoxError> {
        self.backend.list_prepared().await
    }

    fn is_retryable(&self, error: &(dyn Error + 'static)) -> bool {
        self.backend.is_retryable(error)
    }
}

// --- journal -----------------------------------------------------------------------------

type WallFn = dyn Fn() -> i64 + Send + Sync;

const FIELDS: &str = "j.dat_txn AS dat_txn, j.state AS state, j.dat_tx_time AS dat_tx_time, \
                      j.branches AS branches, j.created_ms AS created_ms, j.updated_ms AS updated_ms";

/// The journal in a drevo graph: one node per meta-transaction.
///
/// Nodes are `(:<label> {title, dat_txn, state, dat_tx_time, branches,
/// created_ms, updated_ms})`; `title` (unique graph-wide in drevo) is
/// `<label>:<dat_txn>`, so a `dat_txn` is journaled once. Each compare-and-set
/// is one drevo transaction: of two concurrent writers of the same node, drevo
/// rejects the second commit, which surfaces as a conflict.
pub struct DrevoJournal {
    backend: Arc<dyn Backend>,
    label: String,
    wall_ms: Box<WallFn>,
}

impl fmt::Debug for DrevoJournal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DrevoJournal")
            .field("label", &self.label)
            .finish_non_exhaustive()
    }
}

fn backend_error(e: impl Into<BoxError>) -> JournalError {
    JournalError::Backend(e.into())
}

fn check_label(label: &str) -> Result<(), ConfigError> {
    let mut chars = label.chars();
    let ok = label.len() <= 64
        && chars.next().is_some_and(|c| c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    if ok {
        Ok(())
    } else {
        Err(ConfigError(format!(
            "invalid journal label {label:?}: a letter, then letters, digits, _"
        )))
    }
}

impl DrevoJournal {
    /// The journal in the graph `dsn` names, as nodes labelled `label`.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] for a bad label or DSN.
    pub fn new(dsn: &str, label: &str) -> Result<Self, ConfigError> {
        check_label(label)?;
        Ok(Self::over(open_backend(dsn)?, label))
    }

    /// The journal in the graph `store` opened (for an embedded graph, which
    /// one process opens once). The graph must hold no store's data.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] for a bad label.
    pub fn in_graph_of(store: &DrevoStore, label: &str) -> Result<Self, ConfigError> {
        check_label(label)?;
        Ok(Self::over(store.backend.clone(), label))
    }

    fn over(backend: Arc<dyn Backend>, label: &str) -> Self {
        Self {
            backend,
            label: label.to_owned(),
            wall_ms: Box::new(system_ms),
        }
    }

    /// An injected wall clock (Unix ms) for `created_ms` / `updated_ms`.
    #[must_use]
    pub fn with_wall(mut self, wall_ms: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        self.wall_ms = Box::new(wall_ms);
        self
    }

    fn record(row: &Row) -> Result<JournalRecord, JournalError> {
        Ok(JournalRecord {
            dat_txn: Uuid::parse_str(row.str("dat_txn").map_err(backend_error)?)
                .map_err(backend_error)?,
            state: row
                .str("state")
                .map_err(backend_error)?
                .parse()
                .map_err(backend_error)?,
            dat_tx_time: row.opt_int("dat_tx_time").map_err(backend_error)?,
            branches: decode_branches(row.str("branches").map_err(backend_error)?)?,
            created_ms: row.int("created_ms").map_err(backend_error)?,
            updated_ms: row.int("updated_ms").map_err(backend_error)?,
        })
    }

    async fn compare_and_set(
        &self,
        dat_txn: Uuid,
        expected: State,
        new: State,
        dat_tx_time: Option<i64>,
    ) -> Result<Option<JournalRecord>, BoxError> {
        let time_clause = if dat_tx_time.is_some() {
            ", j.dat_tx_time = $ts"
        } else {
            ""
        };
        let cypher = format!(
            "MATCH (j:{} {{dat_txn: $t}}) WHERE j.state = $expected \
             SET j.state = $new, j.updated_ms = $now{time_clause} RETURN {FIELDS}",
            self.label
        );
        let params = params![
            ("t", dat_txn.to_string()),
            ("expected", expected.as_str()),
            ("new", new.as_str()),
            ("now", (self.wall_ms)()),
            ("ts", dat_tx_time),
        ];
        let mut tx = self.backend.begin().await?;
        let found = match tx.run(&cypher, params).await {
            Ok(found) => found,
            Err(e) => {
                tx.rollback().await.ok();
                return Err(e);
            }
        };
        let Some(row) = found.first() else {
            tx.rollback().await?;
            return Ok(None);
        };
        let record = Self::record(row).map_err(|e| -> BoxError { Box::new(e) });
        tx.commit().await?;
        record.map(Some)
    }
}

#[async_trait]
impl Journal for DrevoJournal {
    async fn begin(
        &self,
        dat_txn: Uuid,
        branches: &[BranchRecord],
    ) -> Result<JournalRecord, JournalError> {
        let now = (self.wall_ms)();
        if self.get(dat_txn).await?.is_some() {
            return Err(JournalError::AlreadyJournaled(dat_txn));
        }
        let cypher = format!(
            "CREATE (j:{} {{title: $title, dat_txn: $t, state: $state, branches: $branches, \
             created_ms: $now, updated_ms: $now}})",
            self.label
        );
        let params = params![
            ("title", format!("{}:{dat_txn}", self.label)),
            ("t", dat_txn.to_string()),
            ("state", State::Active.as_str()),
            ("branches", encode_branches(branches)),
            ("now", now),
        ];
        if let Err(e) = write(self.backend.as_ref(), &cypher, params).await {
            if self.get(dat_txn).await?.is_some() {
                return Err(JournalError::AlreadyJournaled(dat_txn)); // lost a race on the title
            }
            return Err(backend_error(e));
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
        match self
            .compare_and_set(dat_txn, expected, new, dat_tx_time)
            .await
        {
            Ok(Some(record)) => return Ok(record),
            Ok(None) => {}
            // A concurrent writer committed first: report what it left.
            Err(e) if self.backend.is_retryable(e.as_ref()) => {}
            Err(e) => return Err(JournalError::Backend(e)),
        }
        let actual = self.get(dat_txn).await?.map(|r| r.state);
        Err(JournalError::Conflict {
            dat_txn,
            expected,
            actual,
        })
    }

    async fn get(&self, dat_txn: Uuid) -> Result<Option<JournalRecord>, JournalError> {
        let cypher = format!("MATCH (j:{} {{dat_txn: $t}}) RETURN {FIELDS}", self.label);
        let found = self
            .backend
            .read(&cypher, params![("t", dat_txn.to_string())])
            .await;
        found
            .map_err(JournalError::Backend)?
            .first()
            .map(Self::record)
            .transpose()
    }

    async fn pending(
        &self,
        updated_before_ms: i64,
        limit: Option<usize>,
    ) -> Result<Vec<JournalRecord>, JournalError> {
        let mut cypher = format!(
            "MATCH (j:{}) WHERE j.state IN ['ACTIVE', 'PREPARED', 'COMMITTED'] AND j.updated_ms < $cut \
             RETURN {FIELDS} ORDER BY updated_ms, created_ms",
            self.label
        );
        if let Some(limit) = limit {
            cypher += " LIMIT ";
            cypher += &limit.to_string();
        }
        let found = self
            .backend
            .read(&cypher, params![("cut", updated_before_ms)])
            .await;
        found
            .map_err(JournalError::Backend)?
            .iter()
            .map(Self::record)
            .collect()
    }

    async fn forget(&self, dat_txn: Uuid) -> Result<(), JournalError> {
        match self.get(dat_txn).await? {
            None => Ok(()),
            Some(r) if !r.state.is_final() => Err(JournalError::NotFinal {
                dat_txn,
                state: r.state,
            }),
            Some(_) => {
                let cypher = format!(
                    "MATCH (j:{} {{dat_txn: $t}}) WHERE j.state IN ['ABORTED', 'DONE'] DETACH DELETE j",
                    self.label
                );
                write(
                    self.backend.as_ref(),
                    &cypher,
                    params![("t", dat_txn.to_string())],
                )
                .await
                .map_err(JournalError::Backend)?;
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graphs_are_located_by_dsn() {
        assert_eq!(
            graph_location("drevo+bolt://u:p@Graph.Local:7000").unwrap(),
            Some(vec!["bolt".into(), "graph.local".into(), "7000".into()])
        );
        assert_eq!(
            graph_location("drevo+bolt://g").unwrap().unwrap()[2],
            "7687"
        );
        assert_eq!(
            graph_location("drevo:///var/g.drevo").unwrap(),
            Some(vec!["embedded".into(), "/var/g.drevo".into()])
        );
        assert_eq!(graph_location("drevo+mem://").unwrap(), None);
        assert!(graph_location("postgresql://h/d").is_err());
        let err = graph_location("mysql://u:hunter2@h/d").unwrap_err();
        assert!(!err.to_string().contains("hunter2"));
    }

    #[test]
    fn labels_are_checked() {
        assert!(check_label("DatJournal").is_ok());
        for bad in ["", "1x", "_x", "a-b", "a b", &"x".repeat(65)] {
            assert!(check_label(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn rows_are_read_by_column() {
        let row = Row(BTreeMap::from([
            ("s".to_owned(), Value::from("x")),
            ("n".to_owned(), Value::from(3_i64)),
            ("z".to_owned(), Value::Null),
        ]));
        assert_eq!(
            (row.str("s"), row.int("n"), row.opt_int("z")),
            (Ok("x"), Ok(3), Ok(None))
        );
        assert!(row.int("z").is_err() && row.str("n").is_err() && row.int("missing").is_err());
        assert_eq!(
            params![("a", 1_i64), ("b", None::<i64>)],
            vec![
                ("a".to_owned(), Value::Int(1)),
                ("b".to_owned(), Value::Null)
            ]
        );
    }

    #[cfg(not(feature = "embedded"))]
    #[test]
    fn embedded_dsns_need_the_feature() {
        let err = DrevoStore::new("drevo+mem://", "g").unwrap_err();
        assert!(err.to_string().contains("`embedded` feature"), "{err}");
    }
}
