//! Recovery: presumed abort, roll-forward, orphan prepared gids, crash anywhere.
//! The port of the Python reference's `tests/core/test_recovery.py`.

mod fakes;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use datorium_core::clock::HybridLogicalClock;
use datorium_core::coordinator::{CommitError, Coordinator, Outcome};
use datorium_core::gid::gid_for;
use datorium_core::identity::IdGenerator;
use datorium_core::journal::{BranchRecord, Journal, JournalError, JournalRecord, State};
use datorium_core::recovery::{Recovery, RecoveryReport};
use datorium_core::retry::Retry;
use datorium_core::timer::NoWait;
use fakes::{Fault, FaultyJournal, Wall, World, block, run};
use proptest::prelude::*;
use uuid::Uuid;

const STALE_MS: i64 = 30_000;

fn new_id() -> Uuid {
    IdGenerator::new().generate().unwrap()
}

fn make_world() -> World {
    let mut w = World::default();
    w.store("pg", true);
    w.store("graph", true);
    w.store("clickhouse", false);
    w
}

fn make_coordinator(world: &World, journal: Arc<dyn Journal>) -> Coordinator {
    Coordinator::new(
        Some(journal),
        world.participants(),
        Arc::new(HybridLogicalClock::new()),
        Arc::new(NoWait::default()),
    )
    .with_finish(Retry::new(2, Duration::from_secs(1), Duration::ZERO, Duration::ZERO).unwrap())
}

fn make_recovery(world: &World, journal: Arc<dyn Journal>, wall: &Wall) -> Recovery {
    let wall = wall.clone();
    Recovery::new(journal, world.participants(), STALE_MS)
        .unwrap()
        .with_wall(move || wall.get())
}

struct Fixture {
    world: World,
    wall: Wall,
    journal: Arc<FaultyJournal>,
}

fn fixture() -> Fixture {
    let world = make_world();
    let wall = Wall::default();
    let journal = Arc::new(FaultyJournal::new(world.faults.clone(), wall.clone()));
    Fixture {
        world,
        wall,
        journal,
    }
}

impl Fixture {
    fn coordinator(&self) -> Coordinator {
        make_coordinator(&self.world, self.journal.clone())
    }

    fn recovery(&self) -> Recovery {
        make_recovery(&self.world, self.journal.clone(), &self.wall)
    }

    fn crash_commit(&self, point: &str) -> Uuid {
        self.world.faults.add(point, Fault::Crash, 1);
        let txn = new_id();
        let result = run(self.coordinator().commit(txn, &mut self.world.branches()));
        assert!(result.is_err(), "{point} must crash");
        txn
    }

    fn state(&self, txn: Uuid) -> Option<State> {
        block(self.journal.get(txn)).unwrap().map(|r| r.state)
    }

    fn run_recovery(recovery: &Recovery) -> RecoveryReport {
        block(recovery.run_once(100)).unwrap()
    }
}

// --- presumed abort --------------------------------------------------------------

#[test]
fn crash_before_the_decision_is_rolled_back() {
    for point in [
        "pg.prepare.after",
        "graph.prepare",
        "journal.PREPARED.after",
        "journal.COMMITTED",
    ] {
        let f = fixture();
        let txn = f.crash_commit(point);
        let recovery = f.recovery();
        // Not stale yet: the coordinator may be alive.
        assert!(
            Fixture::run_recovery(&recovery).aborted.is_empty(),
            "{point}"
        );
        f.wall.advance(STALE_MS + 1);
        let report = Fixture::run_recovery(&recovery);
        assert_eq!(report.aborted, [txn], "{point}");
        assert!(f.world.all_applied(txn, false));
        f.world.assert_nothing_prepared();
        assert_eq!(f.state(txn), Some(State::Aborted));
    }
}

#[test]
fn abort_is_written_before_branches_are_rolled_back() {
    let f = fixture();
    let txn = f.crash_commit("journal.COMMITTED");
    f.wall.advance(STALE_MS + 1);
    Fixture::run_recovery(&f.recovery());
    let seen = f.world.faults.seen();
    let at = |p: &str| seen.iter().position(|s| s == p).unwrap();
    assert!(at("journal.ABORTED") < at("pg.abort"));
    assert!(f.state(txn).is_some());
}

// --- roll forward ----------------------------------------------------------------

#[test]
fn crash_after_the_decision_is_rolled_forward() {
    for point in [
        "journal.COMMITTED.after",
        "pg.finish",
        "graph.finish.after",
        "clickhouse.finish",
        "journal.DONE",
    ] {
        let f = fixture();
        let txn = f.crash_commit(point);
        f.wall.advance(STALE_MS + 1);
        let report = Fixture::run_recovery(&f.recovery());
        assert_eq!(report.finished, [txn], "{point}");
        assert!(f.world.all_applied(txn, true));
        f.world.assert_nothing_prepared();
        assert_eq!(f.state(txn), Some(State::Done));
    }
}

#[test]
fn recovery_rolls_forward_lock_holders_first() {
    let mut world = World::default();
    world.store("clickhouse", false);
    world.store("pg", true);
    let wall = Wall::default();
    let journal = Arc::new(FaultyJournal::new(world.faults.clone(), wall.clone()));
    let f = Fixture {
        world,
        wall,
        journal,
    };
    f.crash_commit("journal.COMMITTED.after");
    f.world.faults.clear_seen();
    f.wall.advance(STALE_MS + 1);
    Fixture::run_recovery(&f.recovery());
    let finishes: Vec<String> = f
        .world
        .faults
        .seen()
        .into_iter()
        .filter(|p| p.ends_with(".finish"))
        .collect();
    assert_eq!(finishes, ["pg.finish", "clickhouse.finish"]);
}

#[test]
fn committed_pending_is_finished_by_recovery() {
    let f = fixture();
    f.world.faults.add("clickhouse.finish", Fault::Transient, 2);
    let txn = new_id();
    let outcome = block(f.coordinator().commit(txn, &mut f.world.branches()));
    assert!(matches!(outcome, Ok(Outcome::Pending { .. })));
    f.wall.advance(STALE_MS + 1);
    assert_eq!(Fixture::run_recovery(&f.recovery()).finished, [txn]);
    assert!(f.world.all_applied(txn, true));
}

#[test]
fn unfinished_branch_stays_pending_and_alerts_when_stuck() {
    let f = fixture();
    f.world
        .faults
        .add("clickhouse.finish", Fault::Permanent, 1000);
    let txn = new_id();
    block(f.coordinator().commit(txn, &mut f.world.branches())).unwrap();
    let recovery = f.recovery().with_alert_after_ms(600_000);

    f.wall.advance(STALE_MS + 1);
    let first = Fixture::run_recovery(&recovery);
    assert_eq!(
        (first.finished, first.unfinished, first.stuck),
        (vec![], vec![txn], vec![])
    );

    f.wall.advance(600_000);
    let later = Fixture::run_recovery(&recovery);
    assert_eq!(later.unfinished, [txn]);
    assert_eq!(later.stuck, [txn]); // never silently dropped
    assert_eq!(f.state(txn), Some(State::Committed));
}

#[test]
fn stuck_is_measured_from_the_start_of_the_transaction_not_the_last_change() {
    let f = fixture();
    f.world.faults.add("pg.finish", Fault::Permanent, 1000);
    let txn = new_id();
    let branches = [BranchRecord::new("pg", gid_for(txn, "pg"), true)];
    block(f.journal.begin(txn, &branches)).unwrap();
    f.wall.advance(590_000); // a slow Unit of Work: decided long after it began
    block(
        f.journal
            .transition(txn, State::Active, State::Prepared, Some(1)),
    )
    .unwrap();
    block(
        f.journal
            .transition(txn, State::Prepared, State::Committed, None),
    )
    .unwrap();
    f.wall.advance(STALE_MS + 1);
    let report = Fixture::run_recovery(&f.recovery().with_alert_after_ms(600_000));
    assert_eq!((report.unfinished, report.stuck), (vec![txn], vec![txn]));
}

/// Recovery runs (on "another host") right before the coordinator's decision.
struct Interleaved {
    inner: Arc<FaultyJournal>,
    recovery: std::sync::Mutex<Option<Recovery>>,
    wall: Wall,
}

#[async_trait]
impl Journal for Interleaved {
    async fn begin(
        &self,
        dat_txn: Uuid,
        branches: &[BranchRecord],
    ) -> Result<JournalRecord, JournalError> {
        self.inner.begin(dat_txn, branches).await
    }

    async fn transition(
        &self,
        dat_txn: Uuid,
        expected: State,
        new: State,
        dat_tx_time: Option<i64>,
    ) -> Result<JournalRecord, JournalError> {
        if new == State::Committed {
            let recovery = self.recovery.lock().unwrap().take();
            if let Some(recovery) = recovery {
                self.wall.advance(STALE_MS + 1);
                recovery.run_once(100).await?;
            }
        }
        self.inner
            .transition(dat_txn, expected, new, dat_tx_time)
            .await
    }

    async fn get(&self, dat_txn: Uuid) -> Result<Option<JournalRecord>, JournalError> {
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
fn recovery_and_a_live_coordinator_cannot_both_decide() {
    let f = fixture();
    let journal = Arc::new(Interleaved {
        inner: f.journal.clone(),
        recovery: std::sync::Mutex::new(Some(f.recovery())),
        wall: f.wall.clone(),
    });
    let txn = new_id();
    let result = block(make_coordinator(&f.world, journal).commit(txn, &mut f.world.branches()));
    assert!(
        matches!(result, Err(CommitError::Aborted { .. })),
        "{result:?}"
    );
    assert!(f.world.all_applied(txn, false));
    f.world.assert_nothing_prepared();
}

// --- orphans in the stores --------------------------------------------------------

#[test]
fn prepared_gid_without_a_journal_record_is_rolled_back() {
    let f = fixture();
    let txn = new_id();
    let gid = gid_for(txn, "pg");
    let pg = f.world.get("pg");
    pg.put_prepared(&gid, txn, &["row"]);
    pg.put_prepared("someone-elses-xid", Uuid::nil(), &["theirs"]);
    pg.put_prepared(&gid_for(txn, "graph"), txn, &["not this store's"]);

    let report = Fixture::run_recovery(&f.recovery());
    assert_eq!(report.orphans, [gid]);
    assert_eq!(
        pg.prepared_gids(),
        ["someone-elses-xid".to_owned(), gid_for(txn, "graph")]
    );
}

#[test]
fn prepared_gid_left_after_a_failed_abort_is_swept() {
    let f = fixture();
    let txn = f.crash_commit("journal.COMMITTED");
    f.world.faults.add("graph.abort", Fault::Transient, 1);
    f.wall.advance(STALE_MS + 1);
    let recovery = f.recovery();

    Fixture::run_recovery(&recovery);
    assert_eq!(
        f.world.leftover_prepared(),
        [("graph".into(), vec![gid_for(txn, "graph")])]
    );
    assert_eq!(
        Fixture::run_recovery(&recovery).orphans,
        [gid_for(txn, "graph")]
    );
    f.world.assert_nothing_prepared();
}

#[test]
fn prepared_gid_of_a_live_transaction_is_left_alone() {
    let f = fixture();
    let txn = new_id();
    block(
        f.journal
            .begin(txn, &[BranchRecord::new("pg", gid_for(txn, "pg"), true)]),
    )
    .unwrap();
    f.world
        .get("pg")
        .put_prepared(&gid_for(txn, "pg"), txn, &["row"]);
    assert_eq!(
        Fixture::run_recovery(&f.recovery()).orphans,
        Vec::<String>::new()
    );
    assert_eq!(
        f.world.leftover_prepared(),
        [("pg".into(), vec![gid_for(txn, "pg")])]
    );
}

#[test]
fn recovery_survives_a_broken_participant() {
    let f = fixture();
    let txn = f.crash_commit("pg.finish");
    f.world.faults.add("pg.finish", Fault::Permanent, 1);
    f.wall.advance(STALE_MS + 1);
    let recovery = f.recovery();
    assert_eq!(Fixture::run_recovery(&recovery).unfinished, [txn]);
    assert_eq!(Fixture::run_recovery(&recovery).finished, [txn]);
}

#[test]
fn invalid_recovery_settings_are_rejected() {
    let f = fixture();
    let err = Recovery::new(f.journal.clone(), f.world.participants(), 0).unwrap_err();
    assert!(err.to_string().contains("stale_after_ms"));
}

// --- crash anywhere: all or nothing ----------------------------------------------

fn points() -> Vec<String> {
    let mut points: Vec<String> = ["pg", "graph", "clickhouse"]
        .iter()
        .map(|s| format!("{s}.stage"))
        .collect();
    for s in ["pg", "graph"] {
        for a in ["", ".after"] {
            points.push(format!("{s}.prepare{a}"));
        }
    }
    for st in ["ACTIVE", "PREPARED", "COMMITTED", "DONE"] {
        for a in ["", ".after"] {
            points.push(format!("journal.{st}{a}"));
        }
    }
    for s in ["pg", "graph", "clickhouse"] {
        for a in ["", ".after"] {
            points.push(format!("{s}.finish{a}"));
        }
    }
    points
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(300))]

    #[test]
    fn any_crash_or_failure_ends_all_or_nothing_after_recovery(
        crash in proptest::option::of(proptest::sample::select(points())),
        transient in proptest::collection::vec((proptest::sample::select(points()), 1_u32..=3), 0..=3),
        lost_acks in proptest::collection::vec(proptest::sample::select(vec!["pg", "graph", "clickhouse"]), 0..=2),
    ) {
        let f = fixture();
        for (point, times) in &transient {
            f.world.faults.add(point, Fault::Transient, *times);
        }
        if let Some(point) = &crash {
            f.world.faults.add(point, Fault::Crash, 1);
        }
        for store in &lost_acks {
            let s = f.world.get(store);
            let n = s.state.lock().unwrap().lose_commit_ack;
            s.set_lose_commit_ack(n + 1);
        }

        let txn = new_id();
        let outcome = match run(f.coordinator().commit(txn, &mut f.world.branches())) {
            Ok(Ok(_)) => "committed",
            Ok(Err(CommitError::Aborted { .. })) => "aborted",
            Ok(Err(CommitError::InDoubt { .. })) => "in doubt",
            Ok(Err(e @ CommitError::Invalid(_))) => panic!("{e}"),
            Err(()) => "crashed",
        };

        f.world.faults.clear_plan(); // the world heals; recovery runs until quiet
        for s in &f.world.stores {
            s.set_lose_commit_ack(0);
        }
        let recovery = f.recovery();
        for _ in 0..3 {
            f.wall.advance(STALE_MS + 1);
            Fixture::run_recovery(&recovery);
        }

        let applied = f.world.applied_stores(txn);
        prop_assert!(applied.is_empty() || applied.len() == 3, "partial commit after {}: {:?}", outcome, applied);
        prop_assert_eq!(f.world.leftover_prepared(), Vec::<(String, Vec<String>)>::new());
        if outcome == "committed" {
            prop_assert_eq!(applied.len(), 3);
        }
        if outcome == "aborted" {
            prop_assert_eq!(&applied, &Vec::<String>::new());
        }
        let state = f.state(txn);
        prop_assert!(matches!(state, None | Some(State::Done | State::Aborted)), "{:?}", state);
        prop_assert_eq!(state == Some(State::Done), applied.len() == 3);
    }
}
