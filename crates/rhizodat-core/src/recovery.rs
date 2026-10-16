//! Recovery: finishes what crashed coordinators left behind.
//!
//! Safe to run on any host, repeatedly. Each run first sweeps prepared gids held
//! by the stores (rolling back those whose journal record is missing or
//! `ABORTED`), then handles journal records unchanged for `stale_after_ms`: no
//! decision → `ABORTED` (compare-and-set first, so a live coordinator cannot
//! decide afterwards) and roll back; `COMMITTED` → finish every branch
//! idempotently and mark `DONE`.

use std::fmt;
use std::sync::Arc;

use uuid::Uuid;

use crate::gid::parse_gid;
use crate::identity::system_ms;
use crate::journal::{BranchRecord, Journal, JournalError, JournalRecord, State};
use crate::store::Participants;

/// What one recovery run did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    /// Undecided meta-transactions presumed aborted.
    pub aborted: Vec<Uuid>,
    /// Committed meta-transactions whose branches are now all finished.
    pub finished: Vec<Uuid>,
    /// Committed meta-transactions with a branch still not finished.
    pub unfinished: Vec<Uuid>,
    /// Unfinished for longer than `alert_after_ms`: raise an alert.
    pub stuck: Vec<Uuid>,
    /// Prepared gids rolled back because the journal has no decision for them.
    pub orphans: Vec<String>,
}

/// Recovery settings that would race live coordinators.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidRecovery(&'static str);

impl fmt::Display for InvalidRecovery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for InvalidRecovery {}

type WallFn = dyn Fn() -> i64 + Send + Sync;

/// A recovery worker over one journal and the participants of its stores.
pub struct Recovery {
    journal: Arc<dyn Journal>,
    participants: Participants,
    stale_after_ms: i64,
    alert_after_ms: i64,
    wall_ms: Box<WallFn>,
}

impl fmt::Debug for Recovery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Recovery")
            .field("participants", &self.participants)
            .field("stale_after_ms", &self.stale_after_ms)
            .field("alert_after_ms", &self.alert_after_ms)
            .finish_non_exhaustive()
    }
}

impl Recovery {
    /// A worker handling records unchanged for `stale_after_ms`.
    ///
    /// # Errors
    ///
    /// [`InvalidRecovery`] for `stale_after_ms <= 0`: recovery must not race
    /// live coordinators.
    pub fn new(
        journal: Arc<dyn Journal>,
        participants: Participants,
        stale_after_ms: i64,
    ) -> Result<Self, InvalidRecovery> {
        if stale_after_ms <= 0 {
            return Err(InvalidRecovery(
                "stale_after_ms must be > 0: recovery must not race live coordinators",
            ));
        }
        Ok(Self {
            journal,
            participants,
            stale_after_ms,
            alert_after_ms: 600_000,
            wall_ms: Box::new(system_ms),
        })
    }

    /// Report a committed meta-transaction as stuck after this long (default ten minutes).
    #[must_use]
    pub fn with_alert_after_ms(mut self, alert_after_ms: i64) -> Self {
        self.alert_after_ms = alert_after_ms;
        self
    }

    /// An injected wall clock (Unix ms), comparable with the journal's.
    #[must_use]
    pub fn with_wall(mut self, wall_ms: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        self.wall_ms = Box::new(wall_ms);
        self
    }

    /// One pass over at most `limit` stale records.
    ///
    /// # Errors
    ///
    /// A [`JournalError`] when the journal cannot be read; a later run retries.
    pub async fn run_once(&self, limit: usize) -> Result<RecoveryReport, JournalError> {
        let mut report = RecoveryReport::default();
        self.sweep_orphans(&mut report).await?;
        let now = (self.wall_ms)();
        let stale = self
            .journal
            .pending(now - self.stale_after_ms, Some(limit))
            .await?;
        for record in stale {
            if record.state == State::Committed {
                self.roll_forward(&record, now, &mut report).await;
            } else {
                self.presume_abort(&record, &mut report).await;
            }
        }
        Ok(report)
    }

    async fn presume_abort(&self, record: &JournalRecord, report: &mut RecoveryReport) {
        if self
            .journal
            .transition(record.dat_txn, record.state, State::Aborted, None)
            .await
            .is_err()
        {
            return; // it moved on since the scan (or the journal failed); the next run sees
        }
        report.aborted.push(record.dat_txn);
        for branch in &record.branches {
            if let Some(participant) = self.participants.get(&branch.store) {
                // Left prepared on failure: the orphan sweep of a later run retries.
                participant.abort(branch).await.ok();
            }
        }
    }

    async fn roll_forward(&self, record: &JournalRecord, now: i64, report: &mut RecoveryReport) {
        let mut done = record.dat_tx_time.is_some();
        if let Some(ts) = record.dat_tx_time {
            let mut lock_holders_first: Vec<&BranchRecord> = record.branches.iter().collect();
            lock_holders_first.sort_by_key(|b| !b.two_phase);
            for branch in lock_holders_first {
                let Some(participant) = self.participants.get(&branch.store) else {
                    done = false;
                    continue;
                };
                if participant
                    .finish(branch, record.dat_txn, ts)
                    .await
                    .is_err()
                {
                    let applied = participant.is_applied(branch, record.dat_txn).await;
                    done = done && applied.unwrap_or(false);
                }
            }
        }
        if done {
            // A conflict means another worker finished it.
            let marked = self
                .journal
                .transition(record.dat_txn, State::Committed, State::Done, None)
                .await;
            if marked.is_ok() || marked.is_err_and(|e| e.is_conflict()) {
                report.finished.push(record.dat_txn);
                return;
            }
        }
        report.unfinished.push(record.dat_txn);
        if now - record.created_ms > self.alert_after_ms {
            report.stuck.push(record.dat_txn);
        }
    }

    async fn sweep_orphans(&self, report: &mut RecoveryReport) -> Result<(), JournalError> {
        for (store, participant) in self.participants.iter() {
            let Ok(gids) = participant.prepared_gids().await else {
                continue;
            };
            for gid in gids {
                let Some((dat_txn, owner)) = parse_gid(&gid) else {
                    continue; // not ours
                };
                if owner != store {
                    continue;
                }
                let record = self.journal.get(dat_txn).await?;
                if record.is_some_and(|r| r.state != State::Aborted) {
                    continue; // undecided (handled when stale) or committed (rolled forward)
                }
                if participant
                    .abort(&BranchRecord::new(store, gid.as_str(), true))
                    .await
                    .is_ok()
                {
                    report.orphans.push(gid);
                }
            }
        }
        Ok(())
    }
}
