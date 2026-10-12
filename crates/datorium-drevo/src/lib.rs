//! drevo for `datorium-core` over Bolt (neo4rs), the port of the Python
//! reference's `datorium.adapters.drevo` (its Bolt half; embedded drevo will use
//! `drevo-core` directly).
//!
//! - [`DrevoStore`]: one drevo graph as a store.
//! - [`DrevoBranch`]: one Unit of Work's explicit Bolt transaction. `stage`
//!   writes the applied marker `(:DatApplied {dat_txn, dat_tx_time})`;
//!   `prepare` calls `drevo.tx.prepare($gid)` inside the transaction and then
//!   commits the Bolt transaction, which the server keeps prepared.
//! - [`DrevoParticipant`]: `drevo.tx.commitPrepared` / `rollbackPrepared` /
//!   `listPrepared`.
//! - [`DrevoJournal`]: one node per meta-transaction, the same nodes as the
//!   Python journal, so either implementation recovers the other's records.
//!
//! While a branch is prepared, drevo's fence rejects every other commit to that
//! graph (a transient error): the journal must live in another graph than any
//! drevo store of the same deployment.
//!
//! Bolt has no distinct code for an unknown gid
//! ([ice1x/drevo#590](https://github.com/ice1x/drevo/issues/590)); the server's
//! message is the only signal. Result streams hold a pooled connection until
//! dropped, so every query here collects its rows first.
//!
//! neo4rs retries a failed `Graph::run` / `Graph::execute` on transient errors
//! (the prepared fence among them) for up to 60 seconds, which would hide the
//! fence from datorium's own retry policy and stall the coordinator. Writes here
//! run in an explicit transaction instead, which neo4rs never retries (the
//! Python driver does not retry auto-commit statements either). Reads and the
//! `drevo.tx.*` procedures stay in auto-commit mode: under the fence drevo
//! rejects the commit of any explicit transaction, even a read-only one, and
//! accepts the procedures only outside one; neither waits on the fence.

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
use neo4rs::{ConfigBuilder, Graph, Neo4jClientErrorKind, Neo4jErrorKind, Query, Row, Txn, query};
use uuid::Uuid;

const MARKER: &str = "CREATE (:DatApplied {dat_txn: $dat_txn, dat_tx_time: $dat_tx_time})";
const IS_APPLIED: &str = "MATCH (m:DatApplied {dat_txn: $dat_txn}) RETURN count(m) AS n";
const UNKNOWN_GID: &str = "no transaction is prepared as";

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

/// The prepared fence, lost connections and other transient server errors;
/// never Cypher or constraint errors.
#[must_use]
pub fn is_retryable(error: &(dyn Error + 'static)) -> bool {
    let Some(error) = error.downcast_ref::<neo4rs::Error>() else {
        return error.is::<std::io::Error>();
    };
    match error {
        neo4rs::Error::Neo4j(e) => matches!(
            e.kind(),
            Neo4jErrorKind::Transient
                | Neo4jErrorKind::Client(Neo4jClientErrorKind::SessionExpired)
        ),
        neo4rs::Error::IOError { .. }
        | neo4rs::Error::ConnectionError
        | neo4rs::Error::ConnectionTimedOut
        | neo4rs::Error::ServerUnavailableError(_)
        | neo4rs::Error::RequestIgnoredError => true,
        _ => false,
    }
}

fn is_unknown_gid(error: &neo4rs::Error) -> bool {
    matches!(error, neo4rs::Error::Neo4j(e) if e.message().contains(UNKNOWN_GID))
}

/// Which graph a DSN names, to keep the journal off every store's graph:
/// `("bolt", host, port)`.
///
/// # Errors
///
/// [`ConfigError`] for anything but a `drevo+bolt://` DSN.
pub fn graph_location(dsn: &str) -> Result<(String, String, u16), ConfigError> {
    let parsed = parse_dsn(dsn).map_err(|e| ConfigError(e.to_string()))?;
    if parsed.store() != "drevo" || parsed.transport() != Some("bolt") {
        return Err(ConfigError(format!(
            "not a drevo+bolt:// connection string: {parsed}"
        )));
    }
    let host = parsed.host().unwrap_or_default().to_owned();
    Ok(("bolt".to_owned(), host, parsed.port().unwrap_or(7687)))
}

fn connect(dsn: &str) -> Result<Graph, ConfigError> {
    let parsed = parse_dsn(dsn).map_err(|e| ConfigError(e.to_string()))?;
    let (_, host, port) = graph_location(dsn)?;
    let config = ConfigBuilder::new()
        .uri(format!("bolt://{host}:{port}"))
        .user(parsed.user().unwrap_or_default())
        .password(parsed.password().unwrap_or_default())
        .build()
        .map_err(|_| ConfigError(format!("invalid drevo connection string: {parsed}")))?;
    Graph::connect(config)
        .map_err(|_| ConfigError(format!("invalid drevo connection string: {parsed}")))
}

/// A read or a `drevo.tx.*` procedure, in auto-commit mode.
async fn read(graph: &Graph, q: Query) -> Result<Vec<Row>, neo4rs::Error> {
    let mut stream = graph.execute(q).await?;
    let mut out = Vec::new();
    while let Some(row) = stream.next().await? {
        out.push(row);
    }
    Ok(out) // the stream (and its pooled connection) is released here
}

/// A write in its own explicit transaction: committed once, never retried.
async fn write(graph: &Graph, q: Query) -> Result<Vec<Row>, neo4rs::Error> {
    let mut txn = graph.start_txn().await?;
    match txn_rows(&mut txn, q).await {
        Ok(found) => {
            txn.commit().await?;
            Ok(found)
        }
        Err(e) => {
            txn.rollback().await.ok();
            Err(e)
        }
    }
}

async fn txn_rows(txn: &mut Txn, q: Query) -> Result<Vec<Row>, neo4rs::Error> {
    let mut stream = txn.execute(q).await?;
    let mut out = Vec::new();
    while let Some(row) = stream.next(txn.handle()).await? {
        out.push(row);
    }
    Ok(out)
}

/// One drevo graph (over Bolt) as a datorium store.
#[derive(Clone)]
pub struct DrevoStore {
    name: String,
    graph: Graph,
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
    /// A store named `name` over a `drevo+bolt://` DSN.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] for another kind of DSN.
    pub fn new(dsn: &str, name: &str) -> Result<Self, ConfigError> {
        let graph = connect(dsn)?;
        let participant = Arc::new(DrevoParticipant {
            graph: graph.clone(),
        });
        Ok(Self {
            name: name.to_owned(),
            graph,
            participant,
        })
    }

    /// The store name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Native read access in auto-commit mode, outside any meta-transaction.
    ///
    /// # Errors
    ///
    /// The driver's error.
    pub async fn read(&self, q: Query) -> Result<Vec<Row>, neo4rs::Error> {
        read(&self.graph, q).await
    }

    /// A native write in its own transaction, outside any meta-transaction.
    /// While a branch of this graph is prepared it fails with a transient
    /// error (the fence) instead of waiting.
    ///
    /// # Errors
    ///
    /// The driver's error.
    pub async fn write(&self, q: Query) -> Result<Vec<Row>, neo4rs::Error> {
        write(&self.graph, q).await
    }

    /// A new branch with an open explicit transaction.
    ///
    /// # Errors
    ///
    /// The driver's error when no transaction can be started.
    pub async fn branch(&self) -> Result<DrevoBranch, neo4rs::Error> {
        let txn = self.graph.start_txn().await?;
        Ok(DrevoBranch {
            store: self.name.clone(),
            graph: self.graph.clone(),
            txn: Some(txn),
            gid: None,
        })
    }

    /// The store's participant (one per store, shared).
    #[must_use]
    pub fn participant(&self) -> Arc<DrevoParticipant> {
        self.participant.clone()
    }
}

/// One Unit of Work's explicit transaction in a drevo graph.
pub struct DrevoBranch {
    store: String,
    graph: Graph,
    txn: Option<Txn>,
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
    /// The driver's error, or when the branch is no longer open.
    pub async fn rows(&mut self, q: Query) -> Result<Vec<Row>, BoxError> {
        let txn = self.txn.as_mut().ok_or_else(gone)?;
        Ok(txn_rows(txn, q).await?)
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
        let txn = self.txn.as_mut().ok_or_else(gone)?;
        txn.run(
            query(MARKER)
                .param("dat_txn", dat_txn.to_string())
                .param("dat_tx_time", dat_tx_time),
        )
        .await?;
        Ok(None)
    }

    async fn prepare(&mut self, gid: &str) -> Result<(), BoxError> {
        let mut txn = self.txn.take().ok_or_else(gone)?;
        txn.run(query("CALL drevo.tx.prepare($gid)").param("gid", gid))
            .await?;
        txn.commit().await?; // closes the Bolt transaction; the server keeps it prepared
        self.gid = Some(gid.to_owned());
        Ok(())
    }

    async fn commit_one_phase(&mut self) -> Result<(), BoxError> {
        let txn = self.txn.take().ok_or_else(gone)?;
        txn.commit().await?;
        Ok(())
    }

    async fn rollback(&mut self) -> Result<(), BoxError> {
        if let Some(txn) = self.txn.take() {
            txn.rollback().await?;
            return Ok(());
        }
        let Some(gid) = &self.gid else {
            return Ok(());
        };
        resolve(&self.graph, "rollbackPrepared", gid).await
    }

    fn is_retryable(&self, error: &(dyn Error + 'static)) -> bool {
        is_retryable(error)
    }
}

/// `drevo.tx.<procedure>($gid)`; an unknown gid is already resolved.
async fn resolve(graph: &Graph, name: &str, gid: &str) -> Result<(), BoxError> {
    match read(
        graph,
        query(&format!("CALL drevo.tx.{name}($gid)")).param("gid", gid),
    )
    .await
    {
        Err(e) if !is_unknown_gid(&e) => Err(e.into()),
        _ => Ok(()),
    }
}

/// Resolves prepared gids of one drevo graph.
pub struct DrevoParticipant {
    graph: Graph,
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
        let q = query("CALL drevo.tx.commitPrepared($gid)").param("gid", branch.gid.as_str());
        match read(&self.graph, q).await {
            Ok(_) => Ok(()),
            Err(e) if is_unknown_gid(&e) => {
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
        let found = read(
            &self.graph,
            query(IS_APPLIED).param("dat_txn", dat_txn.to_string()),
        )
        .await?;
        let n: i64 = found.first().map(|r| r.get("n")).transpose()?.unwrap_or(0);
        Ok(n > 0)
    }

    async fn abort(&self, branch: &BranchRecord) -> Result<(), BoxError> {
        resolve(&self.graph, "rollbackPrepared", &branch.gid).await
    }

    async fn prepared_gids(&self) -> Result<Vec<String>, BoxError> {
        let found = read(&self.graph, query("CALL drevo.tx.listPrepared()")).await?;
        Ok(found
            .iter()
            .map(|r| r.get::<String>("gid"))
            .collect::<Result<_, _>>()?)
    }

    fn is_retryable(&self, error: &(dyn Error + 'static)) -> bool {
        is_retryable(error)
    }
}

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
    graph: Graph,
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

fn backend(e: impl Into<BoxError>) -> JournalError {
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
        Ok(Self {
            graph: connect(dsn)?,
            label: label.to_owned(),
            wall_ms: Box::new(system_ms),
        })
    }

    /// An injected wall clock (Unix ms) for `created_ms` / `updated_ms`.
    #[must_use]
    pub fn with_wall(mut self, wall_ms: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        self.wall_ms = Box::new(wall_ms);
        self
    }

    fn record(row: &Row) -> Result<JournalRecord, JournalError> {
        let txn: String = row.get("dat_txn").map_err(backend)?;
        let state: String = row.get("state").map_err(backend)?;
        let branches: String = row.get("branches").map_err(backend)?;
        Ok(JournalRecord {
            dat_txn: Uuid::parse_str(&txn).map_err(backend)?,
            state: state.parse().map_err(backend)?,
            dat_tx_time: row.get::<Option<i64>>("dat_tx_time").map_err(backend)?,
            branches: decode_branches(&branches)?,
            created_ms: row.get("created_ms").map_err(backend)?,
            updated_ms: row.get("updated_ms").map_err(backend)?,
        })
    }

    async fn create(
        &self,
        dat_txn: Uuid,
        branches: &[BranchRecord],
        now: i64,
    ) -> Result<(), neo4rs::Error> {
        let mut txn = self.graph.start_txn().await?;
        let q = query(&format!(
            "CREATE (j:{} {{title: $title, dat_txn: $t, state: $state, branches: $branches, \
             created_ms: $now, updated_ms: $now}})",
            self.label
        ))
        .param("title", format!("{}:{dat_txn}", self.label))
        .param("t", dat_txn.to_string())
        .param("state", State::Active.as_str())
        .param("branches", encode_branches(branches))
        .param("now", now);
        match txn.run(q).await {
            Ok(()) => txn.commit().await,
            Err(e) => {
                txn.rollback().await.ok();
                Err(e)
            }
        }
    }

    async fn compare_and_set(
        &self,
        dat_txn: Uuid,
        expected: State,
        new: State,
        dat_tx_time: Option<i64>,
    ) -> Result<Option<JournalRecord>, neo4rs::Error> {
        let time_clause = if dat_tx_time.is_some() {
            ", j.dat_tx_time = $ts"
        } else {
            ""
        };
        let mut q = query(&format!(
            "MATCH (j:{} {{dat_txn: $t}}) WHERE j.state = $expected \
             SET j.state = $new, j.updated_ms = $now{time_clause} RETURN {FIELDS}",
            self.label
        ))
        .param("t", dat_txn.to_string())
        .param("expected", expected.as_str())
        .param("new", new.as_str())
        .param("now", (self.wall_ms)());
        if let Some(ts) = dat_tx_time {
            q = q.param("ts", ts);
        }
        let mut txn = self.graph.start_txn().await?;
        let found = match txn_rows(&mut txn, q).await {
            Ok(found) => found,
            Err(e) => {
                txn.rollback().await.ok();
                return Err(e);
            }
        };
        let Some(row) = found.first() else {
            txn.rollback().await?;
            return Ok(None);
        };
        let record = Self::record(row).ok();
        txn.commit().await?;
        Ok(record)
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
        if let Err(e) = self.create(dat_txn, branches, now).await {
            if self.get(dat_txn).await?.is_some() {
                return Err(JournalError::AlreadyJournaled(dat_txn)); // lost a race on the title
            }
            return Err(backend(e));
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
            Err(e) if is_retryable(&e) => {}
            Err(e) => return Err(backend(e)),
        }
        let actual = self.get(dat_txn).await?.map(|r| r.state);
        Err(JournalError::Conflict {
            dat_txn,
            expected,
            actual,
        })
    }

    async fn get(&self, dat_txn: Uuid) -> Result<Option<JournalRecord>, JournalError> {
        let q = query(&format!(
            "MATCH (j:{} {{dat_txn: $t}}) RETURN {FIELDS}",
            self.label
        ))
        .param("t", dat_txn.to_string());
        let found = read(&self.graph, q).await.map_err(backend)?;
        found.first().map(Self::record).transpose()
    }

    async fn pending(
        &self,
        updated_before_ms: i64,
        limit: Option<usize>,
    ) -> Result<Vec<JournalRecord>, JournalError> {
        let mut text = format!(
            "MATCH (j:{}) WHERE j.state IN ['ACTIVE', 'PREPARED', 'COMMITTED'] AND j.updated_ms < $cut \
             RETURN {FIELDS} ORDER BY updated_ms, created_ms",
            self.label
        );
        if let Some(limit) = limit {
            text += " LIMIT ";
            text += &limit.to_string();
        }
        let found = read(&self.graph, query(&text).param("cut", updated_before_ms))
            .await
            .map_err(backend)?;
        found.iter().map(Self::record).collect()
    }

    async fn forget(&self, dat_txn: Uuid) -> Result<(), JournalError> {
        match self.get(dat_txn).await? {
            None => Ok(()),
            Some(r) if !r.state.is_final() => Err(JournalError::NotFinal {
                dat_txn,
                state: r.state,
            }),
            Some(_) => {
                let q = query(&format!(
                    "MATCH (j:{} {{dat_txn: $t}}) WHERE j.state IN ['ABORTED', 'DONE'] DETACH DELETE j",
                    self.label
                ))
                .param("t", dat_txn.to_string());
                write(&self.graph, q).await.map_err(backend)?;
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_bolt_dsns_name_a_graph() {
        assert_eq!(
            graph_location("drevo+bolt://u:p@Graph.Local:7000").unwrap(),
            ("bolt".into(), "graph.local".into(), 7000)
        );
        assert_eq!(graph_location("drevo+bolt://g").unwrap().2, 7687);
        for dsn in ["drevo:///x.drevo", "drevo+mem://", "postgresql://h/d"] {
            assert!(graph_location(dsn).is_err(), "{dsn}");
        }
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
    fn io_errors_are_retryable_and_config_errors_are_not() {
        assert!(is_retryable(&std::io::Error::other("reset")));
        assert!(!is_retryable(&ConfigError("x".into())));
        assert!(is_retryable(&neo4rs::Error::ConnectionError));
        assert!(!is_retryable(&neo4rs::Error::ConversionError));
    }
}
