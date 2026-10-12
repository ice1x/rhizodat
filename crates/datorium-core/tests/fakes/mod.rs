//! In-memory stores, branches and participants with fault injection; the port
//! of the Python reference's `tests/fakes.py`.
//!
//! A `FakeStore` behaves like one database: an open transaction buffers rows,
//! `prepare` moves them to a prepared slot under a gid (two-phase stores only),
//! `commit` makes them visible. One-phase stores apply a staged payload after
//! the decision, deduplicated by `dat_txn`. `Faults` fails at named points; a
//! crash is a panic with a `Crash` payload, which nothing in the core catches.

#![allow(dead_code, clippy::missing_panics_doc)]

use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;

use async_trait::async_trait;
use datorium_core::journal::{
    BranchRecord, Journal, JournalError, JournalRecord, MemoryJournal, State,
};
use datorium_core::store::{BoxError, Branch, Participant, Participants};
use datorium_core::timer::Timer;
use uuid::Uuid;

macro_rules! error_type {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub struct $name(pub String);

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}: {}", stringify!($name), self.0)
            }
        }

        impl Error for $name {}
    };
}

error_type!(Transient); // retryable: serialization failure, prepared fence, network
error_type!(Permanent); // not retryable: constraint violation
error_type!(Timeout); // an acknowledgement that never came

/// The panic payload of a simulated process death.
#[derive(Debug)]
pub struct Crash;

/// What a fault point does when it fires.
#[derive(Clone, Debug)]
pub enum Fault {
    Transient,
    Permanent,
    Crash,
}

pub fn retryable(error: &(dyn Error + 'static)) -> bool {
    error.is::<Transient>() || error.is::<Timeout>()
}

#[derive(Default)]
struct FaultState {
    plan: HashMap<String, (Fault, u32)>,
    seen: Vec<String>,
}

/// Fail the first `times` times a point is reached. Shared by every fake.
#[derive(Clone, Default)]
pub struct Faults(Arc<Mutex<FaultState>>);

impl Faults {
    pub fn add(&self, point: &str, fault: Fault, times: u32) {
        self.0
            .lock()
            .unwrap()
            .plan
            .insert(point.to_owned(), (fault, times));
    }

    pub fn clear_plan(&self) {
        self.0.lock().unwrap().plan.clear();
    }

    pub fn clear_seen(&self) {
        self.0.lock().unwrap().seen.clear();
    }

    pub fn seen(&self) -> Vec<String> {
        self.0.lock().unwrap().seen.clone()
    }

    pub fn hit(&self, point: &str) -> Result<(), BoxError> {
        let fault = {
            let mut state = self.0.lock().unwrap();
            state.seen.push(point.to_owned());
            match state.plan.get_mut(point) {
                Some((fault, times)) if *times > 0 => {
                    *times -= 1;
                    Some(fault.clone())
                }
                _ => None,
            }
        }; // the lock is released before a crash unwinds
        match fault {
            None => Ok(()),
            Some(Fault::Transient) => Err(Box::new(Transient(point.to_owned()))),
            Some(Fault::Permanent) => Err(Box::new(Permanent(point.to_owned()))),
            Some(Fault::Crash) => std::panic::panic_any(Crash),
        }
    }
}

#[derive(Default)]
pub struct StoreState {
    pub committed: HashMap<Uuid, Vec<String>>,
    pub prepared: Vec<(String, (Uuid, Vec<String>))>,
    /// Commits that apply but then report a timeout.
    pub lose_commit_ack: u32,
    /// Payloads `is_applied` was asked about.
    pub is_applied_payloads: Vec<Option<Vec<u8>>>,
}

#[derive(Clone)]
pub struct FakeStore {
    pub name: String,
    pub two_phase: bool,
    pub faults: Faults,
    pub state: Arc<Mutex<StoreState>>,
}

impl FakeStore {
    pub fn new(name: &str, two_phase: bool, faults: Faults) -> Self {
        Self {
            name: name.to_owned(),
            two_phase,
            faults,
            state: Arc::default(),
        }
    }

    fn apply(&self, dat_txn: Uuid, rows: &[String]) -> Result<(), BoxError> {
        let mut state = self.state.lock().unwrap();
        state
            .committed
            .entry(dat_txn)
            .or_insert_with(|| rows.to_vec()); // idempotent
        if state.lose_commit_ack > 0 {
            state.lose_commit_ack -= 1;
            return Err(Box::new(Timeout(format!(
                "{}: commit acknowledgement lost",
                self.name
            ))));
        }
        Ok(())
    }

    pub fn applied(&self, dat_txn: Uuid) -> bool {
        self.state.lock().unwrap().committed.contains_key(&dat_txn)
    }

    pub fn prepared_gids(&self) -> Vec<String> {
        self.state
            .lock()
            .unwrap()
            .prepared
            .iter()
            .map(|(g, _)| g.clone())
            .collect()
    }

    pub fn put_prepared(&self, gid: &str, dat_txn: Uuid, rows: &[&str]) {
        let rows = rows.iter().map(ToString::to_string).collect();
        self.state
            .lock()
            .unwrap()
            .prepared
            .push((gid.to_owned(), (dat_txn, rows)));
    }

    fn take_prepared(&self, gid: &str) -> Option<(Uuid, Vec<String>)> {
        let mut state = self.state.lock().unwrap();
        let at = state.prepared.iter().position(|(g, _)| g == gid)?;
        Some(state.prepared.remove(at).1)
    }

    pub fn set_lose_commit_ack(&self, n: u32) {
        self.state.lock().unwrap().lose_commit_ack = n;
    }
}

pub struct FakeBranch {
    db: FakeStore,
    rows: Vec<String>,
    state: Arc<Mutex<&'static str>>,
    gid: Option<String>,
    dat_txn: Option<Uuid>,
}

impl FakeBranch {
    pub fn new(db: &FakeStore, rows: Vec<String>) -> (Self, Arc<Mutex<&'static str>>) {
        let state = Arc::new(Mutex::new("open"));
        let branch = Self {
            db: db.clone(),
            rows,
            state: state.clone(),
            gid: None,
            dat_txn: None,
        };
        (branch, state)
    }

    fn point(&self, step: &str) -> String {
        format!("{}.{step}", self.db.name)
    }
}

#[async_trait]
impl Branch for FakeBranch {
    fn store(&self) -> &str {
        &self.db.name
    }

    fn two_phase(&self) -> bool {
        self.db.two_phase
    }

    async fn stage(
        &mut self,
        dat_txn: Uuid,
        _dat_tx_time: i64,
    ) -> Result<Option<Vec<u8>>, BoxError> {
        self.db.faults.hit(&self.point("stage"))?;
        self.dat_txn = Some(dat_txn);
        if self.db.two_phase {
            return Ok(None);
        }
        Ok(Some(self.rows.join("\n").into_bytes()))
    }

    async fn prepare(&mut self, gid: &str) -> Result<(), BoxError> {
        assert!(self.db.two_phase);
        let dat_txn = self.dat_txn.expect("staged before prepare");
        self.db.faults.hit(&self.point("prepare"))?;
        self.db
            .state
            .lock()
            .unwrap()
            .prepared
            .push((gid.to_owned(), (dat_txn, self.rows.clone())));
        self.gid = Some(gid.to_owned());
        *self.state.lock().unwrap() = "prepared";
        self.db.faults.hit(&self.point("prepare.after"))
    }

    async fn commit_one_phase(&mut self) -> Result<(), BoxError> {
        let dat_txn = self.dat_txn.expect("staged before commit");
        self.db.faults.hit(&self.point("commit_one_phase"))?;
        self.db.apply(dat_txn, &self.rows)?;
        *self.state.lock().unwrap() = "committed";
        Ok(())
    }

    async fn rollback(&mut self) -> Result<(), BoxError> {
        self.db.faults.hit(&self.point("rollback"))?;
        if let Some(gid) = &self.gid {
            self.db.take_prepared(gid);
        }
        *self.state.lock().unwrap() = "rolled back";
        Ok(())
    }

    fn is_retryable(&self, error: &(dyn Error + 'static)) -> bool {
        retryable(error)
    }
}

pub struct FakeParticipant {
    db: FakeStore,
}

impl FakeParticipant {
    fn point(&self, step: &str) -> String {
        format!("{}.{step}", self.db.name)
    }
}

#[async_trait]
impl Participant for FakeParticipant {
    async fn finish(&self, branch: &BranchRecord, dat_txn: Uuid, _ts: i64) -> Result<(), BoxError> {
        self.db.faults.hit(&self.point("finish"))?;
        if branch.two_phase {
            let Some((_, rows)) = self.db.take_prepared(&branch.gid) else {
                return Ok(()); // unknown gid: already resolved
            };
            self.db.apply(dat_txn, &rows)?;
        } else {
            let payload = branch
                .payload
                .as_deref()
                .expect("a one-phase branch has a payload");
            let rows: Vec<String> = String::from_utf8(payload.to_vec())?
                .lines()
                .map(String::from)
                .collect();
            self.db.apply(dat_txn, &rows)?;
        }
        self.db.faults.hit(&self.point("finish.after"))
    }

    async fn is_applied(&self, branch: &BranchRecord, dat_txn: Uuid) -> Result<bool, BoxError> {
        self.db.faults.hit(&self.point("is_applied"))?;
        self.db
            .state
            .lock()
            .unwrap()
            .is_applied_payloads
            .push(branch.payload.clone());
        Ok(self.db.applied(dat_txn))
    }

    async fn abort(&self, branch: &BranchRecord) -> Result<(), BoxError> {
        self.db.faults.hit(&self.point("abort"))?;
        self.db.take_prepared(&branch.gid);
        Ok(())
    }

    async fn prepared_gids(&self) -> Result<Vec<String>, BoxError> {
        Ok(self.db.prepared_gids())
    }

    fn is_retryable(&self, error: &(dyn Error + 'static)) -> bool {
        retryable(error)
    }
}

/// The observable state of one fake branch: open, prepared, committed, rolled back.
pub type BranchState = Arc<Mutex<&'static str>>;

/// Several fake stores sharing one fault plan.
#[derive(Default)]
pub struct World {
    pub faults: Faults,
    pub stores: Vec<FakeStore>,
    pub branch_states: Mutex<Vec<(String, BranchState)>>,
}

impl World {
    pub fn store(&mut self, name: &str, two_phase: bool) -> FakeStore {
        let store = FakeStore::new(name, two_phase, self.faults.clone());
        self.stores.push(store.clone());
        store
    }

    pub fn get(&self, name: &str) -> &FakeStore {
        self.stores
            .iter()
            .find(|s| s.name == name)
            .expect("a store of this world")
    }

    pub fn participants(&self) -> Participants {
        self.stores
            .iter()
            .map(|s| {
                (
                    s.name.clone(),
                    Arc::new(FakeParticipant { db: s.clone() }) as Arc<dyn Participant>,
                )
            })
            .collect()
    }

    pub fn branch(&self, store: &FakeStore) -> Box<dyn Branch> {
        let (branch, state) = FakeBranch::new(store, vec![format!("{}-row", store.name)]);
        self.branch_states
            .lock()
            .unwrap()
            .push((store.name.clone(), state));
        Box::new(branch)
    }

    pub fn branches(&self) -> Vec<Box<dyn Branch>> {
        self.stores.iter().map(|s| self.branch(s)).collect()
    }

    /// The state of every branch made so far, with its store.
    pub fn states(&self) -> Vec<(String, &'static str)> {
        let states = self.branch_states.lock().unwrap();
        states
            .iter()
            .map(|(s, st)| (s.clone(), *st.lock().unwrap()))
            .collect()
    }

    pub fn applied(&self, dat_txn: Uuid) -> Vec<(String, bool)> {
        self.stores
            .iter()
            .map(|s| (s.name.clone(), s.applied(dat_txn)))
            .collect()
    }

    pub fn applied_stores(&self, dat_txn: Uuid) -> Vec<String> {
        let mut names: Vec<String> = self
            .applied(dat_txn)
            .into_iter()
            .filter(|(_, a)| *a)
            .map(|(n, _)| n)
            .collect();
        names.sort();
        names
    }

    pub fn all_applied(&self, dat_txn: Uuid, value: bool) -> bool {
        self.applied(dat_txn).iter().all(|(_, a)| *a == value)
    }

    pub fn assert_nothing_prepared(&self) {
        assert_eq!(
            self.leftover_prepared(),
            Vec::<(String, Vec<String>)>::new()
        );
    }

    /// Stores still holding a prepared transaction, with their gids.
    pub fn leftover_prepared(&self) -> Vec<(String, Vec<String>)> {
        self.stores
            .iter()
            .map(|s| (s.name.clone(), s.prepared_gids()))
            .filter(|(_, g)| !g.is_empty())
            .collect()
    }
}

/// A `MemoryJournal` with fault points `journal.<STATE>` and `journal.<STATE>.after`.
pub struct FaultyJournal {
    pub inner: MemoryJournal,
    pub faults: Faults,
}

impl FaultyJournal {
    pub fn new(faults: Faults, wall: Wall) -> Self {
        Self {
            inner: MemoryJournal::with_wall(move || wall.get()),
            faults,
        }
    }
}

fn backend(error: BoxError) -> JournalError {
    JournalError::Backend(error)
}

#[async_trait]
impl Journal for FaultyJournal {
    async fn begin(
        &self,
        dat_txn: Uuid,
        branches: &[BranchRecord],
    ) -> Result<JournalRecord, JournalError> {
        self.faults.hit("journal.ACTIVE").map_err(backend)?;
        let record = self.inner.begin(dat_txn, branches).await?;
        self.faults.hit("journal.ACTIVE.after").map_err(backend)?;
        Ok(record)
    }

    async fn transition(
        &self,
        dat_txn: Uuid,
        expected: State,
        new: State,
        dat_tx_time: Option<i64>,
    ) -> Result<JournalRecord, JournalError> {
        self.faults
            .hit(&format!("journal.{new}"))
            .map_err(backend)?;
        let record = self
            .inner
            .transition(dat_txn, expected, new, dat_tx_time)
            .await?;
        self.faults
            .hit(&format!("journal.{new}.after"))
            .map_err(backend)?;
        Ok(record)
    }

    async fn get(&self, dat_txn: Uuid) -> Result<Option<JournalRecord>, JournalError> {
        self.inner.get(dat_txn).await
    }

    async fn pending(
        &self,
        updated_before_ms: i64,
        limit: Option<usize>,
    ) -> Result<Vec<JournalRecord>, JournalError> {
        self.inner.pending(updated_before_ms, limit).await
    }

    async fn forget(&self, dat_txn: Uuid) -> Result<(), JournalError> {
        self.inner.forget(dat_txn).await
    }
}

/// A settable wall clock (Unix ms), shared by the journal and recovery.
#[derive(Clone)]
pub struct Wall(Arc<Mutex<i64>>);

impl Default for Wall {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(1_000_000)))
    }
}

impl Wall {
    pub fn get(&self) -> i64 {
        *self.0.lock().unwrap()
    }

    pub fn advance(&self, ms: i64) {
        *self.0.lock().unwrap() += ms;
    }
}

/// A monotonic clock that only moves when the coordinator sleeps.
#[derive(Default)]
pub struct FakeTimer {
    pub state: Mutex<(Duration, Vec<Duration>)>,
}

impl FakeTimer {
    pub fn now(&self) -> Duration {
        self.state.lock().unwrap().0
    }

    pub fn sleeps(&self) -> Vec<Duration> {
        self.state.lock().unwrap().1.clone()
    }
}

#[async_trait]
impl Timer for FakeTimer {
    fn monotonic(&self) -> Duration {
        self.now()
    }

    async fn sleep(&self, duration: Duration) {
        let mut state = self.state.lock().unwrap();
        state.0 += duration;
        state.1.push(duration);
    }
}

/// Run `future` to completion; `Err(())` if it crashed (panicked with `Crash`).
pub fn run<T>(future: impl Future<Output = T>) -> Result<T, ()> {
    static QUIET: Once = Once::new();
    QUIET.call_once(|| {
        let default = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if !info.payload().is::<Crash>() {
                default(info);
            }
        }));
    });
    match catch_unwind(AssertUnwindSafe(|| futures::executor::block_on(future))) {
        Ok(value) => Ok(value),
        Err(payload) if payload.is::<Crash>() => Err(()),
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

/// Run a future that must not crash.
pub fn block<T>(future: impl Future<Output = T>) -> T {
    run(future).unwrap_or_else(|()| panic!("unexpected crash"))
}
