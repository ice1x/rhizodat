//! The journal state machine of a meta-transaction.
//!
//! ```text
//! ACTIVE ──► PREPARED ──► COMMITTED ──► DONE
//!    │           │
//!    └───────────┴──► ABORTED
//! ```
//!
//! `COMMITTED` is the atomic commit point; `ABORTED` and `DONE` are final.
//! Every change is a compare-and-set from an expected state, so a coordinator
//! deciding `COMMITTED` and a recovery worker deciding `ABORTED` never both win.

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::sync::Mutex;

use async_trait::async_trait;
use uuid::Uuid;

use crate::identity::system_ms;
use crate::store::BoxError;

/// A journal state, stored as its upper-case name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum State {
    /// Branches are being staged; nothing is decided.
    Active,
    /// Every two-phase branch is prepared; `dat_tx_time` is assigned.
    Prepared,
    /// The decision to commit: the atomic commit point.
    Committed,
    /// Rolled back (final).
    Aborted,
    /// Every branch is finished (final).
    Done,
}

impl State {
    /// Every state, in the order of the conformance vectors.
    pub const ALL: [State; 5] = [
        State::Active,
        State::Prepared,
        State::Committed,
        State::Aborted,
        State::Done,
    ];

    /// The stored name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            State::Active => "ACTIVE",
            State::Prepared => "PREPARED",
            State::Committed => "COMMITTED",
            State::Aborted => "ABORTED",
            State::Done => "DONE",
        }
    }

    /// True for `ABORTED` and `DONE`, which never change again.
    #[must_use]
    pub const fn is_final(self) -> bool {
        matches!(self, State::Aborted | State::Done)
    }

    /// The states reachable from `self` in one compare-and-set.
    #[must_use]
    pub const fn successors(self) -> &'static [State] {
        match self {
            State::Active => &[State::Prepared, State::Aborted],
            State::Prepared => &[State::Committed, State::Aborted],
            State::Committed => &[State::Done],
            State::Aborted | State::Done => &[],
        }
    }
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A stored state name that is not one of [`State::ALL`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnknownState(pub String);

impl fmt::Display for UnknownState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown journal state {:?}", self.0)
    }
}

impl std::error::Error for UnknownState {}

impl FromStr for State {
    type Err = UnknownState;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        State::ALL
            .into_iter()
            .find(|state| state.as_str() == s)
            .ok_or_else(|| UnknownState(s.to_owned()))
    }
}

/// An edge that is never legal, whatever is stored.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransitionError {
    /// `to` is not a successor of `from`.
    Illegal {
        /// The expected (current) state.
        from: State,
        /// The requested state.
        to: State,
    },
    /// `PREPARED` without the `dat_tx_time` assigned at stage.
    MissingTxTime,
}

impl fmt::Display for TransitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransitionError::Illegal { from, to } => {
                write!(f, "illegal journal transition {from} -> {to}")
            }
            TransitionError::MissingTxTime => {
                f.write_str("PREPARED needs the dat_tx_time assigned at stage")
            }
        }
    }
}

impl std::error::Error for TransitionError {}

/// Check the edge `expected -> new` before any journal writes it.
///
/// # Errors
///
/// [`TransitionError::Illegal`] for an edge outside the state machine,
/// [`TransitionError::MissingTxTime`] for `PREPARED` without `dat_tx_time`.
pub fn check_transition(
    expected: State,
    new: State,
    dat_tx_time: Option<i64>,
) -> Result<(), TransitionError> {
    if !expected.successors().contains(&new) {
        return Err(TransitionError::Illegal {
            from: expected,
            to: new,
        });
    }
    if new == State::Prepared && dat_tx_time.is_none() {
        return Err(TransitionError::MissingTxTime);
    }
    Ok(())
}

/// One branch as the journal records it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BranchRecord {
    /// The store name the branch belongs to.
    pub store: String,
    /// The branch gid, `dat:<dat_txn>:<store>`.
    pub gid: String,
    /// Whether the store prepares (PostgreSQL, drevo) or applies after the decision.
    pub two_phase: bool,
    /// What a one-phase branch staged (rows, data file paths), if anything.
    pub payload: Option<Vec<u8>>,
}

impl BranchRecord {
    /// A branch record without a payload.
    #[must_use]
    pub fn new(store: impl Into<String>, gid: impl Into<String>, two_phase: bool) -> Self {
        Self {
            store: store.into(),
            gid: gid.into(),
            two_phase,
            payload: None,
        }
    }
}

/// The journal record of one meta-transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalRecord {
    /// The meta-transaction.
    pub dat_txn: Uuid,
    /// Its current state.
    pub state: State,
    /// Every branch, in the order the Unit of Work gave them.
    pub branches: Vec<BranchRecord>,
    /// The HLC transaction time, set with `PREPARED`.
    pub dat_tx_time: Option<i64>,
    /// Wall clock of the writer when the record was created (for scans only).
    pub created_ms: i64,
    /// Wall clock of the writer at the last change.
    pub updated_ms: i64,
}

/// A journal operation that did not happen.
#[derive(Debug)]
pub enum JournalError {
    /// The stored state is not the expected one: another actor got there first.
    Conflict {
        /// The meta-transaction.
        dat_txn: Uuid,
        /// The state the compare-and-set expected.
        expected: State,
        /// What is stored (`None`: no record).
        actual: Option<State>,
    },
    /// `begin` for a meta-transaction that already has a record.
    AlreadyJournaled(Uuid),
    /// An edge outside the state machine; nothing was written.
    Illegal(TransitionError),
    /// `forget` of a record that is not final.
    NotFinal {
        /// The meta-transaction.
        dat_txn: Uuid,
        /// Its (unfinished) state.
        state: State,
    },
    /// The journal's store failed; the write may or may not have happened.
    Backend(BoxError),
}

impl JournalError {
    /// True for [`Conflict`](Self::Conflict) and [`AlreadyJournaled`](Self::AlreadyJournaled):
    /// a definite answer that someone else owns the record.
    #[must_use]
    pub fn is_conflict(&self) -> bool {
        matches!(
            self,
            JournalError::Conflict { .. } | JournalError::AlreadyJournaled(_)
        )
    }
}

impl fmt::Display for JournalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JournalError::Conflict {
                dat_txn,
                actual: None,
                ..
            } => {
                write!(f, "meta-transaction {dat_txn} is unknown to the journal")
            }
            JournalError::Conflict {
                dat_txn,
                expected,
                actual: Some(actual),
            } => {
                write!(
                    f,
                    "meta-transaction {dat_txn} is {actual}, expected {expected}"
                )
            }
            JournalError::AlreadyJournaled(dat_txn) => {
                write!(f, "meta-transaction {dat_txn} is already journaled")
            }
            JournalError::Illegal(e) => e.fmt(f),
            JournalError::NotFinal { dat_txn, state } => {
                write!(
                    f,
                    "cannot forget meta-transaction {dat_txn} in state {state}"
                )
            }
            JournalError::Backend(e) => write!(f, "journal unavailable: {e}"),
        }
    }
}

impl std::error::Error for JournalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            JournalError::Illegal(e) => Some(e),
            JournalError::Backend(e) => Some(e.as_ref()),
            _ => None,
        }
    }
}

impl From<TransitionError> for JournalError {
    fn from(e: TransitionError) -> Self {
        JournalError::Illegal(e)
    }
}

/// The coordination record store. Every state change is a compare-and-set.
#[async_trait]
pub trait Journal: Send + Sync {
    /// Create the record in `ACTIVE`.
    async fn begin(
        &self,
        dat_txn: Uuid,
        branches: &[BranchRecord],
    ) -> Result<JournalRecord, JournalError>;

    /// Move from `expected` to `new`; `dat_tx_time` is required for `PREPARED`.
    async fn transition(
        &self,
        dat_txn: Uuid,
        expected: State,
        new: State,
        dat_tx_time: Option<i64>,
    ) -> Result<JournalRecord, JournalError>;

    /// The record, if any.
    async fn get(&self, dat_txn: Uuid) -> Result<Option<JournalRecord>, JournalError>;

    /// Unfinished records (`ACTIVE`, `PREPARED`, `COMMITTED`) last changed before
    /// the cutoff, oldest first.
    async fn pending(
        &self,
        updated_before_ms: i64,
        limit: Option<usize>,
    ) -> Result<Vec<JournalRecord>, JournalError>;

    /// Remove a final (`ABORTED` or `DONE`) record; a missing record is fine.
    async fn forget(&self, dat_txn: Uuid) -> Result<(), JournalError>;
}

#[derive(serde::Serialize, serde::Deserialize)]
struct BranchColumn {
    store: String,
    gid: String,
    two_phase: bool,
    payload: Option<String>,
}

/// The journal's `branches` column: a JSON list of `{store, gid, two_phase,
/// payload}`, payloads base64. Every journal (and every implementation) uses
/// this text, so records written by one are read by the others.
#[must_use]
pub fn encode_branches(branches: &[BranchRecord]) -> String {
    use base64::Engine as _;
    let column: Vec<BranchColumn> = branches
        .iter()
        .map(|b| BranchColumn {
            store: b.store.clone(),
            gid: b.gid.clone(),
            two_phase: b.two_phase,
            payload: b
                .payload
                .as_ref()
                .map(|p| base64::engine::general_purpose::STANDARD.encode(p)),
        })
        .collect();
    serde_json::to_string(&column).unwrap_or_else(|_| "[]".to_owned())
}

/// The branches in a `branches` column.
///
/// # Errors
///
/// [`JournalError::Backend`] for text that is not such a list.
pub fn decode_branches(column: &str) -> Result<Vec<BranchRecord>, JournalError> {
    use base64::Engine as _;
    let parsed: Vec<BranchColumn> =
        serde_json::from_str(column).map_err(|e| JournalError::Backend(Box::new(e)))?;
    parsed
        .into_iter()
        .map(|b| {
            let payload = b
                .payload
                .map(|p| base64::engine::general_purpose::STANDARD.decode(p))
                .transpose()
                .map_err(|e| JournalError::Backend(Box::new(e)))?;
            Ok(BranchRecord {
                store: b.store,
                gid: b.gid,
                two_phase: b.two_phase,
                payload,
            })
        })
        .collect()
}

type WallFn = dyn Fn() -> i64 + Send + Sync;

/// In-process journal for tests and single-process use. Not durable.
pub struct MemoryJournal {
    wall_ms: Box<WallFn>,
    records: Mutex<HashMap<Uuid, JournalRecord>>,
}

impl fmt::Debug for MemoryJournal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryJournal").finish_non_exhaustive()
    }
}

impl Default for MemoryJournal {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryJournal {
    /// A journal stamping records with the system clock.
    #[must_use]
    pub fn new() -> Self {
        Self::with_wall(system_ms)
    }

    /// A journal stamping records with an injected wall clock (Unix ms).
    pub fn with_wall(wall_ms: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        Self {
            wall_ms: Box::new(wall_ms),
            records: Mutex::default(),
        }
    }

    fn records(&self) -> std::sync::MutexGuard<'_, HashMap<Uuid, JournalRecord>> {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[async_trait]
impl Journal for MemoryJournal {
    async fn begin(
        &self,
        dat_txn: Uuid,
        branches: &[BranchRecord],
    ) -> Result<JournalRecord, JournalError> {
        let mut records = self.records();
        if records.contains_key(&dat_txn) {
            return Err(JournalError::AlreadyJournaled(dat_txn));
        }
        let now = (self.wall_ms)();
        let record = JournalRecord {
            dat_txn,
            state: State::Active,
            branches: branches.to_vec(),
            dat_tx_time: None,
            created_ms: now,
            updated_ms: now,
        };
        records.insert(dat_txn, record.clone());
        Ok(record)
    }

    async fn transition(
        &self,
        dat_txn: Uuid,
        expected: State,
        new: State,
        dat_tx_time: Option<i64>,
    ) -> Result<JournalRecord, JournalError> {
        check_transition(expected, new, dat_tx_time)?;
        let mut records = self.records();
        let current = records.get_mut(&dat_txn).filter(|r| r.state == expected);
        let Some(current) = current else {
            let actual = records.get(&dat_txn).map(|r| r.state);
            return Err(JournalError::Conflict {
                dat_txn,
                expected,
                actual,
            });
        };
        current.state = new;
        current.dat_tx_time = dat_tx_time.or(current.dat_tx_time);
        current.updated_ms = (self.wall_ms)();
        Ok(current.clone())
    }

    async fn get(&self, dat_txn: Uuid) -> Result<Option<JournalRecord>, JournalError> {
        Ok(self.records().get(&dat_txn).cloned())
    }

    async fn pending(
        &self,
        updated_before_ms: i64,
        limit: Option<usize>,
    ) -> Result<Vec<JournalRecord>, JournalError> {
        let mut found: Vec<JournalRecord> = self
            .records()
            .values()
            .filter(|r| !r.state.is_final() && r.updated_ms < updated_before_ms)
            .cloned()
            .collect();
        found.sort_by_key(|r| (r.updated_ms, r.created_ms));
        found.truncate(limit.unwrap_or(usize::MAX));
        Ok(found)
    }

    async fn forget(&self, dat_txn: Uuid) -> Result<(), JournalError> {
        let mut records = self.records();
        match records.get(&dat_txn) {
            None => Ok(()),
            Some(r) if !r.state.is_final() => Err(JournalError::NotFinal {
                dat_txn,
                state: r.state,
            }),
            Some(_) => {
                records.remove(&dat_txn);
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn final_states_have_no_successors() {
        for s in State::ALL {
            assert_eq!(s.is_final(), s.successors().is_empty(), "{s}");
        }
    }

    use futures::executor::block_on;

    fn txn(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    #[test]
    fn compare_and_set_follows_the_state_machine() {
        block_on(async {
            let j = MemoryJournal::with_wall(|| 7);
            let branches = [BranchRecord::new("pg", "dat:x:pg", true)];
            let r = j.begin(txn(1), &branches).await.unwrap();
            assert_eq!(
                (r.state, r.created_ms, r.dat_tx_time),
                (State::Active, 7, None)
            );
            assert!(j.begin(txn(1), &[]).await.unwrap_err().is_conflict());
            let r = j
                .transition(txn(1), State::Active, State::Prepared, Some(42))
                .await
                .unwrap();
            assert_eq!((r.state, r.dat_tx_time), (State::Prepared, Some(42)));
            let err = j
                .transition(txn(1), State::Active, State::Aborted, None)
                .await
                .unwrap_err();
            assert!(matches!(
                err,
                JournalError::Conflict {
                    actual: Some(State::Prepared),
                    ..
                }
            ));
            let r = j
                .transition(txn(1), State::Prepared, State::Committed, None)
                .await
                .unwrap();
            assert_eq!(
                r.dat_tx_time,
                Some(42),
                "a later transition keeps dat_tx_time"
            );
            assert!(matches!(
                j.transition(txn(1), State::Committed, State::Active, None)
                    .await,
                Err(JournalError::Illegal(_))
            ));
            let err = j
                .transition(txn(2), State::Active, State::Aborted, None)
                .await
                .unwrap_err();
            assert_eq!(
                err.to_string(),
                format!("meta-transaction {} is unknown to the journal", txn(2))
            );
        });
    }

    #[test]
    fn pending_lists_unfinished_records_oldest_first() {
        block_on(async {
            let wall = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(100));
            let j = {
                let wall = wall.clone();
                MemoryJournal::with_wall(move || wall.load(std::sync::atomic::Ordering::SeqCst))
            };
            for (n, at) in [(1, 300), (2, 100), (3, 200), (4, 50)] {
                wall.store(at, std::sync::atomic::Ordering::SeqCst);
                j.begin(txn(n), &[]).await.unwrap();
            }
            j.transition(txn(4), State::Active, State::Aborted, None)
                .await
                .unwrap();
            let ids =
                |rs: Vec<JournalRecord>| rs.into_iter().map(|r| r.dat_txn).collect::<Vec<_>>();
            assert_eq!(
                ids(j.pending(301, None).await.unwrap()),
                [txn(2), txn(3), txn(1)]
            );
            assert_eq!(ids(j.pending(201, Some(1)).await.unwrap()), [txn(2)]);
            assert_eq!(j.pending(100, None).await.unwrap(), []);
        });
    }

    #[test]
    fn only_final_records_are_forgotten() {
        block_on(async {
            let j = MemoryJournal::new();
            j.begin(txn(1), &[]).await.unwrap();
            assert!(matches!(
                j.forget(txn(1)).await,
                Err(JournalError::NotFinal { .. })
            ));
            j.transition(txn(1), State::Active, State::Aborted, None)
                .await
                .unwrap();
            j.forget(txn(1)).await.unwrap();
            assert_eq!(j.get(txn(1)).await.unwrap(), None);
            j.forget(txn(1)).await.unwrap();
        });
    }

    #[test]
    fn errors_name_the_edge() {
        let err = check_transition(State::Done, State::Active, Some(1)).unwrap_err();
        assert_eq!(err.to_string(), "illegal journal transition DONE -> ACTIVE");
    }
}
