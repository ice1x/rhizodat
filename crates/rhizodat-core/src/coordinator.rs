//! The commit protocol of meta-transactions.
//!
//! A Unit of Work hands the coordinator one [`Branch`] per store it touched.
//!
//! - No branch: nothing to do.
//! - One branch: the store's own one-phase commit, no journal.
//! - Several branches: stage → journal `ACTIVE` → prepare the two-phase
//!   branches → `PREPARED` → decide `COMMITTED` → finish every branch → `DONE`.
//!
//! Before the decision any failure rolls every branch back and returns
//! [`CommitError::Aborted`] (re-running the whole Unit of Work is then safe if
//! `retryable`). After the decision nothing is rolled back: each branch is
//! finished through its store's [`Participant`] with retries up to a short
//! deadline, and anything left is handed to recovery; the result is then
//! [`Outcome::Pending`], never an error, so callers never write the transaction
//! twice. When the decision write cannot be confirmed either way, the result is
//! [`CommitError::InDoubt`] and recovery decides from the journal.
//!
//! A panic (or a dropped future) leaves stores and journal exactly as they
//! are, which is what recovery is built for.

use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use crate::clock::HybridLogicalClock;
use crate::gid::gid_for;
use crate::journal::{BranchRecord, Journal, JournalError, JournalRecord, State};
use crate::retry::Retry;
use crate::store::{BoxError, Branch, Participants};
use crate::timer::Timer;

/// A successful commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Every branch is visible.
    Committed {
        /// The meta-transaction.
        dat_txn: Uuid,
        /// Its transaction time (`None` when there was nothing to commit).
        dat_tx_time: Option<i64>,
    },
    /// The decision is durable, but some branches are still being finished by
    /// recovery. Do not re-run the Unit of Work.
    Pending {
        /// The meta-transaction.
        dat_txn: Uuid,
        /// Its transaction time.
        dat_tx_time: i64,
    },
}

/// A commit that did not happen, or whose outcome is not known.
#[derive(Debug)]
pub enum CommitError {
    /// Nothing was committed. `retryable`: re-running the Unit of Work may succeed.
    Aborted {
        /// The meta-transaction.
        dat_txn: Uuid,
        /// Whether a re-run may succeed.
        retryable: bool,
        /// What went wrong.
        cause: BoxError,
    },
    /// The decision write could not be confirmed; recovery resolves it from the
    /// journal. Do not re-run the Unit of Work: it may have committed.
    InDoubt {
        /// The meta-transaction.
        dat_txn: Uuid,
        /// What went wrong.
        cause: BoxError,
    },
    /// The Unit of Work cannot be committed as given (rolled back, nothing written).
    Invalid(String),
}

impl fmt::Display for CommitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommitError::Aborted { dat_txn, cause, .. } => {
                write!(f, "meta-transaction {dat_txn} aborted: {cause}")
            }
            CommitError::InDoubt { dat_txn, .. } => {
                write!(
                    f,
                    "outcome of meta-transaction {dat_txn} is unknown; left to recovery"
                )
            }
            CommitError::Invalid(message) => f.write_str(message),
        }
    }
}

impl Error for CommitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            CommitError::Aborted { cause, .. } | CommitError::InDoubt { cause, .. } => {
                Some(cause.as_ref())
            }
            CommitError::Invalid(_) => None,
        }
    }
}

type RandFn = dyn Fn() -> f64 + Send + Sync;

fn os_uniform() -> f64 {
    // 53 random bits: every value is exact in an f64 and below 1.
    let bits = getrandom::u64().map_or(0, |v| v >> 11);
    #[allow(clippy::cast_precision_loss)]
    let unit = bits as f64 / (1_u64 << 53) as f64;
    unit
}

/// Runs the commit protocol. One coordinator serves many Units of Work.
pub struct Coordinator {
    journal: Option<Arc<dyn Journal>>,
    participants: Participants,
    clock: Arc<HybridLogicalClock>,
    timer: Arc<dyn Timer>,
    finish: Retry,
    rand: Box<RandFn>,
}

impl fmt::Debug for Coordinator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Coordinator")
            .field("participants", &self.participants)
            .field("journal", &self.journal.is_some())
            .field("finish", &self.finish)
            .finish_non_exhaustive()
    }
}

impl Coordinator {
    /// A coordinator. `journal` may be `None` when every Unit of Work touches one store.
    #[must_use]
    pub fn new(
        journal: Option<Arc<dyn Journal>>,
        participants: Participants,
        clock: Arc<HybridLogicalClock>,
        timer: Arc<dyn Timer>,
    ) -> Self {
        let finish = Retry::new(
            5,
            Duration::from_secs(2),
            Duration::from_millis(20),
            Duration::from_millis(500),
        )
        .unwrap_or_default();
        Self {
            journal,
            participants,
            clock,
            timer,
            finish,
            rand: Box::new(os_uniform),
        }
    }

    /// The retry policy for the decision write and for finishing branches.
    #[must_use]
    pub fn with_finish(mut self, finish: Retry) -> Self {
        self.finish = finish;
        self
    }

    /// The jitter source, uniform in [0, 1).
    #[must_use]
    pub fn with_rand(mut self, rand: impl Fn() -> f64 + Send + Sync + 'static) -> Self {
        self.rand = Box::new(rand);
        self
    }

    /// Commit the branches of `dat_txn` as one meta-transaction.
    ///
    /// # Errors
    ///
    /// [`CommitError::Aborted`] when nothing was committed,
    /// [`CommitError::InDoubt`] when the decision could not be confirmed, and
    /// [`CommitError::Invalid`] for branches that cannot be committed together.
    pub async fn commit(
        &self,
        dat_txn: Uuid,
        branches: &mut [Box<dyn Branch>],
    ) -> Result<Outcome, CommitError> {
        if branches.is_empty() {
            return Ok(Outcome::Committed {
                dat_txn,
                dat_tx_time: None,
            });
        }
        if let Err(e) = self.validate(branches) {
            rollback(branches).await;
            return Err(e);
        }
        if let [branch] = branches {
            return self.commit_one_phase(dat_txn, branch.as_mut()).await;
        }
        self.commit_two_phase(dat_txn, branches).await
    }

    /// Undo every branch before the decision; one failing branch does not stop the rest.
    pub async fn rollback(&self, branches: &mut [Box<dyn Branch>]) {
        rollback(branches).await;
    }

    // --- one branch -------------------------------------------------------------

    async fn commit_one_phase(
        &self,
        dat_txn: Uuid,
        branch: &mut dyn Branch,
    ) -> Result<Outcome, CommitError> {
        let ts = match self.clock.now() {
            Ok(ts) => ts.encode(),
            Err(e) => {
                branch.rollback().await.ok();
                return Err(CommitError::Aborted {
                    dat_txn,
                    retryable: true,
                    cause: Box::new(e),
                });
            }
        };
        let committed = Outcome::Committed {
            dat_txn,
            dat_tx_time: Some(ts),
        };
        let mut payload = None;
        let error = match branch.stage(dat_txn, ts).await {
            Ok(staged) => {
                payload = staged;
                match branch.commit_one_phase().await {
                    Ok(()) => return Ok(committed),
                    Err(e) => e,
                }
            }
            Err(e) => e,
        };
        let record = BranchRecord {
            store: branch.store().to_owned(),
            gid: gid_for(dat_txn, branch.store()),
            two_phase: branch.two_phase(),
            payload,
        };
        if self.applied(&record, dat_txn).await {
            return Ok(committed);
        }
        let retryable = branch.is_retryable(error.as_ref());
        branch.rollback().await.ok();
        Err(CommitError::Aborted {
            dat_txn,
            retryable,
            cause: error,
        })
    }

    // --- several branches -------------------------------------------------------

    async fn commit_two_phase(
        &self,
        dat_txn: Uuid,
        branches: &mut [Box<dyn Branch>],
    ) -> Result<Outcome, CommitError> {
        let Some(journal) = self.journal.as_deref() else {
            rollback(branches).await;
            return Err(CommitError::Invalid(
                "a meta-transaction over several stores needs a journal (PostgreSQL or drevo)"
                    .into(),
            ));
        };
        let mut journaled = false;
        let ts = match self
            .stage_and_prepare(journal, dat_txn, branches, &mut journaled)
            .await
        {
            Ok(ts) => ts,
            Err(error) => {
                self.abort(journal, dat_txn, branches, journaled).await;
                let retryable = branches.iter().any(|b| b.is_retryable(error.as_ref()))
                    || error
                        .downcast_ref::<JournalError>()
                        .is_some_and(|e| matches!(e, JournalError::Backend(_)));
                return Err(CommitError::Aborted {
                    dat_txn,
                    retryable,
                    cause: error,
                });
            }
        };
        let record = self.decide(journal, dat_txn, branches).await?;
        Ok(self.finish_all(journal, &record, ts).await)
    }

    async fn stage_and_prepare(
        &self,
        journal: &dyn Journal,
        dat_txn: Uuid,
        branches: &mut [Box<dyn Branch>],
        journaled: &mut bool,
    ) -> Result<i64, BoxError> {
        // Slow lock-free work first, lock holders last.
        let mut order: Vec<usize> = (0..branches.len()).collect();
        order.sort_by_key(|&i| branches[i].two_phase());
        let ts = self.clock.now()?.encode();
        let mut staged: Vec<Option<BranchRecord>> = vec![None; branches.len()];
        for &i in &order {
            let branch = &mut branches[i];
            let payload = branch.stage(dat_txn, ts).await?;
            staged[i] = Some(BranchRecord {
                store: branch.store().to_owned(),
                gid: gid_for(dat_txn, branch.store()),
                two_phase: branch.two_phase(),
                payload,
            });
        }
        let records: Vec<BranchRecord> = staged.into_iter().flatten().collect();
        *journaled = true; // from here a record may exist, even if begin reports an error
        journal.begin(dat_txn, &records).await?;
        for &i in &order {
            let branch = &mut branches[i];
            if branch.two_phase() {
                let gid = gid_for(dat_txn, branch.store());
                branch.prepare(&gid).await?;
            }
        }
        journal
            .transition(dat_txn, State::Active, State::Prepared, Some(ts))
            .await?;
        Ok(ts)
    }

    async fn decide(
        &self,
        journal: &dyn Journal,
        dat_txn: Uuid,
        branches: &mut [Box<dyn Branch>],
    ) -> Result<JournalRecord, CommitError> {
        let deadline = self.timer.monotonic() + self.finish.deadline();
        let mut attempt = 0;
        loop {
            let error = match journal
                .transition(dat_txn, State::Prepared, State::Committed, None)
                .await
            {
                Ok(record) => return Ok(record),
                Err(e) if e.is_conflict() => {
                    // Recovery (or anyone) moved the record; from PREPARED only ABORTED is possible.
                    rollback(branches).await;
                    return Err(CommitError::Aborted {
                        dat_txn,
                        retryable: true,
                        cause: Box::new(e),
                    });
                }
                Err(e) => e,
            };
            let current = match journal.get(dat_txn).await {
                Ok(current) => current,
                Err(read) => {
                    return Err(CommitError::InDoubt {
                        dat_txn,
                        cause: Box::new(read),
                    });
                }
            };
            match current {
                Some(record) if record.state == State::Committed => return Ok(record),
                Some(record) if record.state == State::Prepared => {}
                _ => {
                    rollback(branches).await;
                    return Err(CommitError::Aborted {
                        dat_txn,
                        retryable: true,
                        cause: Box::new(error),
                    });
                }
            }
            attempt += 1;
            if attempt >= self.finish.attempts() || self.timer.monotonic() >= deadline {
                return self
                    .give_up_deciding(journal, dat_txn, branches, error)
                    .await;
            }
            self.pause(attempt - 1, deadline).await;
        }
    }

    /// Still `PREPARED` after every attempt: settle it with one more compare-and-set.
    async fn give_up_deciding(
        &self,
        journal: &dyn Journal,
        dat_txn: Uuid,
        branches: &mut [Box<dyn Branch>],
        cause: JournalError,
    ) -> Result<JournalRecord, CommitError> {
        match journal
            .transition(dat_txn, State::Prepared, State::Aborted, None)
            .await
        {
            Ok(_) => {}
            Err(e) if e.is_conflict() => match journal.get(dat_txn).await {
                Ok(Some(record)) if record.state == State::Committed => return Ok(record),
                Ok(_) => {}
                Err(read) => {
                    return Err(CommitError::InDoubt {
                        dat_txn,
                        cause: Box::new(read),
                    });
                }
            },
            Err(e) => {
                return Err(CommitError::InDoubt {
                    dat_txn,
                    cause: Box::new(e),
                });
            }
        }
        rollback(branches).await;
        Err(CommitError::Aborted {
            dat_txn,
            retryable: true,
            cause: Box::new(cause),
        })
    }

    async fn abort(
        &self,
        journal: &dyn Journal,
        dat_txn: Uuid,
        branches: &mut [Box<dyn Branch>],
        journaled: bool,
    ) {
        if journaled {
            for expected in [State::Active, State::Prepared] {
                // No decision was written: presumed abort covers a failed write.
                if journal
                    .transition(dat_txn, expected, State::Aborted, None)
                    .await
                    .is_ok()
                {
                    break;
                }
            }
        }
        rollback(branches).await;
    }

    async fn finish_all(&self, journal: &dyn Journal, record: &JournalRecord, ts: i64) -> Outcome {
        let dat_txn = record.dat_txn;
        let deadline = self.timer.monotonic() + self.finish.deadline();
        let mut lock_holders_first: Vec<&BranchRecord> = record.branches.iter().collect();
        lock_holders_first.sort_by_key(|b| !b.two_phase);
        let mut done = true;
        for branch in lock_holders_first {
            done &= self.finish_one(branch, dat_txn, ts, deadline).await;
        }
        if done
            && journal
                .transition(dat_txn, State::Committed, State::Done, None)
                .await
                .is_ok()
        {
            return Outcome::Committed {
                dat_txn,
                dat_tx_time: Some(ts),
            };
        }
        // Recovery re-finishes idempotently and marks DONE.
        Outcome::Pending {
            dat_txn,
            dat_tx_time: ts,
        }
    }

    async fn finish_one(
        &self,
        branch: &BranchRecord,
        dat_txn: Uuid,
        ts: i64,
        deadline: Duration,
    ) -> bool {
        let Some(participant) = self.participants.get(&branch.store) else {
            return false;
        };
        for attempt in 0..self.finish.attempts() {
            let Err(error) = participant.finish(branch, dat_txn, ts).await else {
                return true;
            };
            if self.applied(branch, dat_txn).await {
                return true;
            }
            if !participant.is_retryable(error.as_ref())
                || attempt + 1 >= self.finish.attempts()
                || self.timer.monotonic() >= deadline
            {
                return false;
            }
            self.pause(attempt, deadline).await;
        }
        false
    }

    // --- helpers ----------------------------------------------------------------

    /// Unknown counts as not applied; never guess "done".
    async fn applied(&self, branch: &BranchRecord, dat_txn: Uuid) -> bool {
        match self.participants.get(&branch.store) {
            Some(p) => p.is_applied(branch, dat_txn).await.unwrap_or(false),
            None => false,
        }
    }

    async fn pause(&self, attempt: u32, deadline: Duration) {
        let remaining = deadline.saturating_sub(self.timer.monotonic());
        if !remaining.is_zero() {
            self.timer
                .sleep(self.finish.delay(attempt, (self.rand)()).min(remaining))
                .await;
        }
    }

    fn validate(&self, branches: &[Box<dyn Branch>]) -> Result<(), CommitError> {
        for (i, branch) in branches.iter().enumerate() {
            let store = branch.store();
            if branches[..i].iter().any(|b| b.store() == store) {
                return Err(CommitError::Invalid(format!(
                    "store {store:?} has more than one branch"
                )));
            }
            if self.participants.get(store).is_none() {
                return Err(CommitError::Invalid(format!(
                    "no participant is registered for store {store:?}"
                )));
            }
        }
        Ok(())
    }
}

async fn rollback(branches: &mut [Box<dyn Branch>]) {
    for branch in branches {
        // Best effort; recovery cleans up prepared leftovers.
        branch.rollback().await.ok();
    }
}
