//! Commit protocol: one-phase, multi-branch 2PC through the journal, ambiguity.
//! The port of the Python reference's `tests/core/test_coordinator.py`.

mod fakes;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use datorium_core::clock::HybridLogicalClock;
use datorium_core::coordinator::{CommitError, Coordinator, Outcome};
use datorium_core::identity::IdGenerator;
use datorium_core::journal::{
    BranchRecord, Journal, JournalError, JournalRecord, MemoryJournal, State,
};
use datorium_core::retry::Retry;
use datorium_core::store::Branch;
use fakes::{FakeTimer, Fault, Permanent, Timeout, Transient, World, block, run};
use uuid::Uuid;

fn new_id() -> Uuid {
    IdGenerator::new().generate().unwrap()
}

fn world() -> World {
    let mut w = World::default();
    w.store("pg", true);
    w.store("graph", true);
    w.store("clickhouse", false);
    w
}

fn coordinator(
    world: &World,
    journal: Option<Arc<dyn Journal>>,
    timer: &Arc<FakeTimer>,
) -> Coordinator {
    Coordinator::new(
        journal,
        world.participants(),
        Arc::new(HybridLogicalClock::new()),
        timer.clone(),
    )
    .with_finish(
        Retry::new(
            5,
            Duration::from_secs(2),
            Duration::from_millis(10),
            Duration::from_millis(100),
        )
        .unwrap(),
    )
    .with_rand(|| 0.5)
}

fn memory() -> Arc<MemoryJournal> {
    Arc::new(MemoryJournal::new())
}

fn aborted(result: Result<Outcome, CommitError>) -> (Uuid, bool, datorium_core::store::BoxError) {
    match result {
        Err(CommitError::Aborted {
            dat_txn,
            retryable,
            cause,
        }) => (dat_txn, retryable, cause),
        other => panic!("expected Aborted, got {other:?}"),
    }
}

fn all_rolled_back(world: &World) -> bool {
    world.states().iter().all(|(_, s)| *s == "rolled back")
}

// --- empty and single branch -----------------------------------------------------

#[test]
fn nothing_to_commit() {
    let (w, timer) = (world(), Arc::default());
    let txn = new_id();
    let outcome = block(coordinator(&w, Some(memory()), &timer).commit(txn, &mut []));
    assert_eq!(
        outcome.unwrap(),
        Outcome::Committed {
            dat_txn: txn,
            dat_tx_time: None
        }
    );
}

#[test]
fn single_branch_commits_natively_without_the_journal() {
    let mut w = World::default();
    w.store("clickhouse", false);
    let txn = new_id();
    let outcome =
        block(coordinator(&w, None, &Arc::default()).commit(txn, &mut w.branches())).unwrap();
    assert!(matches!(
        outcome,
        Outcome::Committed {
            dat_tx_time: Some(_),
            ..
        }
    ));
    assert_eq!(w.applied(txn), [("clickhouse".into(), true)]);
    assert!(!w.faults.seen().contains(&"clickhouse.prepare".to_owned()));
}

#[test]
fn single_branch_failure_rolls_back_and_says_if_retry_helps() {
    let mut w = World::default();
    w.store("pg", true);
    w.faults.add("pg.commit_one_phase", Fault::Transient, 1);
    let txn = new_id();
    let (dat_txn, retryable, cause) = aborted(block(
        coordinator(&w, None, &Arc::default()).commit(txn, &mut w.branches()),
    ));
    assert_eq!((dat_txn, retryable), (txn, true));
    assert!(cause.is::<Transient>());
    assert!(all_rolled_back(&w));
}

#[test]
fn single_branch_lost_ack_is_resolved_by_asking_the_store() {
    let mut w = World::default();
    w.store("pg", true).set_lose_commit_ack(1);
    let txn = new_id();
    let outcome = block(coordinator(&w, None, &Arc::default()).commit(txn, &mut w.branches()));
    assert!(matches!(outcome, Ok(Outcome::Committed { .. })));
    assert_eq!(w.applied(txn), [("pg".into(), true)]);
}

#[test]
fn single_branch_lost_ack_check_sees_the_staged_payload() {
    // A one-phase store (Iceberg) needs its payload to know where to look.
    let mut w = World::default();
    let lake = w.store("lake", false);
    lake.set_lose_commit_ack(1);
    let txn = new_id();
    let outcome = block(coordinator(&w, None, &Arc::default()).commit(txn, &mut w.branches()));
    assert!(matches!(outcome, Ok(Outcome::Committed { .. })));
    let seen = lake.state.lock().unwrap().is_applied_payloads.clone();
    assert_eq!(seen.len(), 1);
    assert!(seen[0].is_some());
}

// --- several branches: happy path --------------------------------------------------

#[test]
fn multi_branch_commit_follows_the_protocol() {
    let (w, journal) = (world(), memory());
    let txn = new_id();
    let outcome = block(
        coordinator(&w, Some(journal.clone()), &Arc::default()).commit(txn, &mut w.branches()),
    )
    .unwrap();
    let Outcome::Committed { dat_tx_time, .. } = outcome else {
        panic!("{outcome:?}")
    };
    assert!(w.all_applied(txn, true));
    w.assert_nothing_prepared();
    let record = block(journal.get(txn)).unwrap().unwrap();
    assert_eq!(record.state, State::Done);
    assert_eq!(record.dat_tx_time, dat_tx_time);
    let stores: Vec<&str> = record.branches.iter().map(|b| b.store.as_str()).collect();
    assert_eq!(stores, ["pg", "graph", "clickhouse"]);

    let seen = w.faults.seen();
    let at = |p: &str| seen.iter().position(|s| s == p).unwrap();
    // Slow lock-free staging first, lock-holding prepare last, then commit 2PC first.
    assert!(at("clickhouse.stage") < at("pg.prepare"));
    assert!(at("pg.prepare").max(at("graph.prepare")) < at("pg.finish"));
    assert!(at("pg.finish").max(at("graph.finish")) < at("clickhouse.finish"));
}

#[test]
fn lock_holders_commit_first_whatever_order_the_branches_come_in() {
    let mut w = World::default();
    w.store("clickhouse", false);
    w.store("iceberg", false);
    w.store("pg", true);
    block(coordinator(&w, Some(memory()), &Arc::default()).commit(new_id(), &mut w.branches()))
        .unwrap();
    let first = w.faults.seen().into_iter().find(|p| p.ends_with(".finish"));
    assert_eq!(first.as_deref(), Some("pg.finish"));
}

#[test]
fn multi_branch_without_a_journal_is_refused_up_front() {
    let w = world();
    let txn = new_id();
    let result = block(coordinator(&w, None, &Arc::default()).commit(txn, &mut w.branches()));
    assert!(matches!(result, Err(CommitError::Invalid(m)) if m.contains("journal")));
    assert!(w.all_applied(txn, false));
    assert!(all_rolled_back(&w));
}

#[test]
fn duplicate_store_branches_are_rejected() {
    let w = world();
    let pg = w.get("pg").clone();
    let mut branches = vec![w.branch(&pg), w.branch(&pg)];
    let result =
        block(coordinator(&w, Some(memory()), &Arc::default()).commit(new_id(), &mut branches));
    assert!(matches!(result, Err(CommitError::Invalid(m)) if m.contains("\"pg\"")));
}

#[test]
fn branch_without_a_participant_is_rejected() {
    let w = world();
    let mut other = World::default();
    let lonely = other.store("iceberg", false);
    let mut branches = w.branches();
    branches.push(other.branch(&lonely));
    let result =
        block(coordinator(&w, Some(memory()), &Arc::default()).commit(new_id(), &mut branches));
    assert!(matches!(result, Err(CommitError::Invalid(m)) if m.contains("\"iceberg\"")));
}

// --- several branches: failures before the decision --------------------------------

#[test]
fn failure_before_the_decision_aborts_everything() {
    for point in [
        "clickhouse.stage",
        "pg.stage",
        "pg.prepare",
        "graph.prepare",
    ] {
        for (fault, retryable) in [(Fault::Transient, true), (Fault::Permanent, false)] {
            let (w, journal) = (world(), memory());
            w.faults.add(point, fault.clone(), 1);
            let txn = new_id();
            let (_, got, cause) = aborted(block(
                coordinator(&w, Some(journal.clone()), &Arc::default())
                    .commit(txn, &mut w.branches()),
            ));
            assert_eq!(got, retryable, "{point} {fault:?}");
            assert!(cause.is::<Transient>() || cause.is::<Permanent>());
            assert_eq!(cause.to_string().split(": ").nth(1), Some(point));
            assert!(w.all_applied(txn, false));
            w.assert_nothing_prepared();
            assert!(all_rolled_back(&w));
            let record = block(journal.get(txn)).unwrap();
            assert!(record.is_none_or(|r| r.state == State::Aborted), "{point}");
        }
    }
}

/// What a scripted journal does with one `transition` call.
enum Hook {
    Pass,
    /// Fail without writing.
    Fail(JournalError),
    /// Write, then report a lost acknowledgement.
    ApplyThenFail,
    /// Someone else first moves the record from the same expected state to this one.
    First(State),
}

type HookFn = dyn Fn(Uuid, State, State) -> Hook + Send + Sync;

/// A `MemoryJournal` whose calls a test can intercept.
struct Scripted {
    inner: MemoryJournal,
    transition: Box<HookFn>,
    get_fails: Arc<Mutex<bool>>,
    begin_fails: bool,
}

impl Scripted {
    fn new(transition: impl Fn(Uuid, State, State) -> Hook + Send + Sync + 'static) -> Self {
        Self {
            inner: MemoryJournal::new(),
            transition: Box::new(transition),
            get_fails: Arc::default(),
            begin_fails: false,
        }
    }
}

fn down(what: &str) -> JournalError {
    JournalError::Backend(Box::new(Timeout(what.to_owned())))
}

#[async_trait]
impl Journal for Scripted {
    async fn begin(
        &self,
        dat_txn: Uuid,
        branches: &[BranchRecord],
    ) -> Result<JournalRecord, JournalError> {
        if self.begin_fails {
            return Err(down("journal down"));
        }
        self.inner.begin(dat_txn, branches).await
    }

    async fn transition(
        &self,
        dat_txn: Uuid,
        expected: State,
        new: State,
        dat_tx_time: Option<i64>,
    ) -> Result<JournalRecord, JournalError> {
        match (self.transition)(dat_txn, expected, new) {
            Hook::Pass => {
                self.inner
                    .transition(dat_txn, expected, new, dat_tx_time)
                    .await
            }
            Hook::Fail(e) => Err(e),
            Hook::ApplyThenFail => {
                self.inner
                    .transition(dat_txn, expected, new, dat_tx_time)
                    .await?;
                Err(down("ack lost"))
            }
            Hook::First(other) => {
                self.inner
                    .transition(dat_txn, expected, other, None)
                    .await?;
                self.inner
                    .transition(dat_txn, expected, new, dat_tx_time)
                    .await
            }
        }
    }

    async fn get(&self, dat_txn: Uuid) -> Result<Option<JournalRecord>, JournalError> {
        if *self.get_fails.lock().unwrap() {
            return Err(down("journal unreachable"));
        }
        self.inner.get(dat_txn).await
    }

    async fn pending(
        &self,
        before: i64,
        limit: Option<usize>,
    ) -> Result<Vec<JournalRecord>, JournalError> {
        self.inner.pending(before, limit).await
    }

    async fn forget(&self, dat_txn: Uuid) -> Result<(), JournalError> {
        self.inner.forget(dat_txn).await
    }
}

#[test]
fn journal_unreachable_before_the_decision_is_a_retryable_abort() {
    let w = world();
    let mut journal = Scripted::new(|_, _, _| Hook::Pass);
    journal.begin_fails = true;
    let (_, retryable, _) = aborted(block(
        coordinator(&w, Some(Arc::new(journal)), &Arc::default())
            .commit(new_id(), &mut w.branches()),
    ));
    assert!(retryable);
    w.assert_nothing_prepared();
}

#[test]
fn recovery_won_the_race_so_the_late_decision_aborts() {
    let w = world();
    let journal = Scripted::new(|_, _, new| {
        if new == State::Committed {
            Hook::First(State::Aborted)
        } else {
            Hook::Pass
        }
    });
    let txn = new_id();
    let (_, _, cause) = aborted(block(
        coordinator(&w, Some(Arc::new(journal)), &Arc::default()).commit(txn, &mut w.branches()),
    ));
    assert!(
        cause
            .downcast_ref::<JournalError>()
            .is_some_and(JournalError::is_conflict)
    );
    assert!(w.all_applied(txn, false));
    w.assert_nothing_prepared();
}

// --- the decision write is ambiguous ----------------------------------------------

#[test]
fn decision_timeout_that_did_apply_is_read_back_and_finished() {
    let w = world();
    let lost = AtomicU32::new(1);
    let journal = Scripted::new(move |_, _, new| {
        if new == State::Committed && lost.fetch_sub(1, Ordering::SeqCst) == 1 {
            Hook::ApplyThenFail
        } else {
            Hook::Pass
        }
    });
    let txn = new_id();
    let outcome = block(
        coordinator(&w, Some(Arc::new(journal)), &Arc::default()).commit(txn, &mut w.branches()),
    );
    assert!(
        matches!(outcome, Ok(Outcome::Committed { .. })),
        "{outcome:?}"
    );
    assert!(w.all_applied(txn, true));
}

#[test]
fn decision_timeout_that_did_not_apply_is_retried() {
    let w = world();
    let failures = AtomicU32::new(1);
    let journal = Scripted::new(move |_, _, new| {
        if new == State::Committed && failures.fetch_sub(1, Ordering::SeqCst) == 1 {
            Hook::Fail(down("not written"))
        } else {
            Hook::Pass
        }
    });
    let txn = new_id();
    let outcome = block(
        coordinator(&w, Some(Arc::new(journal)), &Arc::default()).commit(txn, &mut w.branches()),
    );
    assert!(
        matches!(outcome, Ok(Outcome::Committed { .. })),
        "{outcome:?}"
    );
    assert!(w.all_applied(txn, true));
}

#[test]
fn unreadable_decision_is_in_doubt_and_left_to_recovery() {
    let w = world();
    let dark: Arc<Mutex<bool>> = Arc::default();
    let mut journal = {
        let dark = dark.clone();
        Scripted::new(move |_, _, new| {
            if new == State::Committed {
                *dark.lock().unwrap() = true;
                Hook::Fail(down("no answer"))
            } else {
                Hook::Pass
            }
        })
    };
    journal.get_fails = dark;
    let txn = new_id();
    let result = block(
        coordinator(&w, Some(Arc::new(journal)), &Arc::default()).commit(txn, &mut w.branches()),
    );
    assert!(matches!(result, Err(CommitError::InDoubt { dat_txn, .. }) if dat_txn == txn));
    // Nothing was rolled back: recovery decides from the journal later.
    let holders: Vec<String> = w.leftover_prepared().into_iter().map(|(s, _)| s).collect();
    assert_eq!(holders, ["pg", "graph"]);
}

#[test]
fn decision_still_unwritten_after_every_attempt_is_settled_as_aborted() {
    let w = world();
    let journal = Scripted::new(|_, _, new| {
        if new == State::Committed {
            Hook::Fail(down("slow"))
        } else {
            Hook::Pass
        }
    });
    let txn = new_id();
    let journal = Arc::new(journal);
    let timer = Arc::default();
    let (_, retryable, _) = aborted(block(
        coordinator(&w, Some(journal.clone()), &timer).commit(txn, &mut w.branches()),
    ));
    assert!(retryable);
    assert_eq!(
        block(journal.get(txn)).unwrap().unwrap().state,
        State::Aborted
    );
    w.assert_nothing_prepared();
    assert_eq!(timer.sleeps().len(), 4, "five attempts, four pauses");
}

#[test]
fn settling_an_undecidable_transaction_when_the_journal_goes_dark_is_in_doubt() {
    // The give-up compare-and-set conflicts, then the read-back fails: in doubt,
    // never an unclassified error.
    let w = world();
    let dark: Arc<Mutex<bool>> = Arc::default();
    let mut journal = {
        let dark = dark.clone();
        Scripted::new(move |_, _, new| match new {
            State::Committed => Hook::Fail(down("slow")),
            State::Aborted => {
                // Someone else decided in between; the record is no longer PREPARED.
                *dark.lock().unwrap() = true;
                Hook::First(State::Committed)
            }
            _ => Hook::Pass,
        })
    };
    journal.get_fails = dark;
    let txn = new_id();
    let result = block(
        coordinator(&w, Some(Arc::new(journal)), &Arc::default()).commit(txn, &mut w.branches()),
    );
    assert!(
        matches!(result, Err(CommitError::InDoubt { .. })),
        "{result:?}"
    );
}

// --- after the decision -------------------------------------------------------------

#[test]
fn finish_retries_a_transient_failure() {
    let w = world();
    w.faults.add("clickhouse.finish", Fault::Transient, 2);
    let timer = Arc::new(FakeTimer::default());
    let txn = new_id();
    let outcome = block(coordinator(&w, Some(memory()), &timer).commit(txn, &mut w.branches()));
    assert!(matches!(outcome, Ok(Outcome::Committed { .. })));
    assert!(w.get("clickhouse").applied(txn));
    assert_eq!(timer.sleeps().len(), 2);
}

#[test]
fn finish_with_a_lost_ack_checks_the_store_instead_of_reapplying() {
    let w = world();
    w.get("graph").set_lose_commit_ack(1);
    let outcome =
        block(coordinator(&w, Some(memory()), &Arc::default()).commit(new_id(), &mut w.branches()));
    assert!(matches!(outcome, Ok(Outcome::Committed { .. })));
    assert!(w.faults.seen().contains(&"graph.is_applied".to_owned()));
}

#[test]
fn finish_that_keeps_failing_returns_committed_pending_never_errors() {
    let (w, journal) = (world(), memory());
    w.faults.add("clickhouse.finish", Fault::Transient, 100);
    let timer = Arc::new(FakeTimer::default());
    let txn = new_id();
    let outcome =
        block(coordinator(&w, Some(journal.clone()), &timer).commit(txn, &mut w.branches()))
            .unwrap();
    assert!(matches!(outcome, Outcome::Pending { dat_txn, .. } if dat_txn == txn));
    assert_eq!(
        w.applied(txn),
        [
            ("pg".into(), true),
            ("graph".into(), true),
            ("clickhouse".into(), false)
        ]
    );
    assert_eq!(
        block(journal.get(txn)).unwrap().unwrap().state,
        State::Committed
    ); // handed to recovery
    assert!(timer.now() <= Duration::from_millis(2100));
}

#[test]
fn finish_stops_at_the_deadline_even_with_attempts_left() {
    let w = world();
    w.faults.add("clickhouse.finish", Fault::Transient, 1000);
    let timer = Arc::new(FakeTimer::default());
    let slow = Retry::new(
        100,
        Duration::from_secs(1),
        Duration::from_millis(400),
        Duration::from_millis(400),
    )
    .unwrap();
    let c = coordinator(&w, Some(memory()), &timer)
        .with_finish(slow)
        .with_rand(|| 0.999);
    let outcome = block(c.commit(new_id(), &mut w.branches()));
    assert!(matches!(outcome, Ok(Outcome::Pending { .. })));
    let tries = w
        .faults
        .seen()
        .iter()
        .filter(|p| *p == "clickhouse.finish")
        .count();
    assert!(
        (2..=4).contains(&tries),
        "{tries} attempts within a one-second deadline"
    );
    assert!(timer.now() <= Duration::from_secs(1));
}

#[test]
fn finish_failure_of_any_kind_after_the_decision_does_not_error() {
    let w = world();
    w.faults.add("pg.finish", Fault::Permanent, 100);
    let txn = new_id();
    let outcome =
        block(coordinator(&w, Some(memory()), &Arc::default()).commit(txn, &mut w.branches()));
    assert!(matches!(outcome, Ok(Outcome::Pending { .. })));
    assert!(w.get("clickhouse").applied(txn)); // the other branches still finished
}

#[test]
fn done_write_failure_still_reports_committed_pending() {
    let w = world();
    let journal = Scripted::new(|_, _, new| {
        if new == State::Done {
            Hook::Fail(down("journal down"))
        } else {
            Hook::Pass
        }
    });
    let txn = new_id();
    let outcome = block(
        coordinator(&w, Some(Arc::new(journal)), &Arc::default()).commit(txn, &mut w.branches()),
    );
    assert!(matches!(outcome, Ok(Outcome::Pending { .. })));
    assert!(w.all_applied(txn, true));
}

// --- crashes ------------------------------------------------------------------------

#[test]
fn a_crash_is_not_swallowed() {
    for point in ["pg.prepare", "graph.finish", "clickhouse.finish"] {
        let w = world();
        w.faults.add(point, Fault::Crash, 1);
        let result =
            run(coordinator(&w, Some(memory()), &Arc::default())
                .commit(new_id(), &mut w.branches()));
        assert!(result.is_err(), "{point}: the crash must propagate");
    }
}

#[test]
fn uow_rollback_releases_every_branch() {
    let w = world();
    let mut branches: Vec<Box<dyn Branch>> = w.branches();
    w.faults.add("graph.rollback", Fault::Transient, 1);
    block(coordinator(&w, Some(memory()), &Arc::default()).rollback(&mut branches));
    for (store, state) in w.states() {
        assert_eq!(state == "rolled back", store != "graph", "{store}");
    }
}

#[test]
fn faults_helper_counts_down() {
    let w = world();
    w.faults.add("x", Fault::Transient, 2);
    assert!(w.faults.hit("x").is_err());
    assert!(w.faults.hit("x").is_err());
    assert!(w.faults.hit("x").is_ok());
}
