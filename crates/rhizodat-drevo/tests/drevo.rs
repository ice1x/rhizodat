//! rhizodat-drevo against real graphs: the compose `drevo` service over Bolt,
//! and (with the `embedded` feature) the engine embedded in the test process.
//! Every scenario runs on both.
//!
//! The Bolt DSN comes from `RHIZODAT_TEST_DREVO_BOLT`, else from `.env.test`;
//! one from `.env.test` whose server is not running skips the Bolt variants
//! with a hint. Bolt tests take turns (one lock for the binary): a prepared
//! branch raises drevo's fence, which rejects every other commit to the graph.
//! Each test labels its nodes and names its stores after a fresh tag, and
//! removes what it left; an embedded graph is the test's own.

use std::error::Error;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rhizodat_core::adapter::Adapter;
use rhizodat_core::clock::HybridLogicalClock;
use rhizodat_core::coordinator::{Coordinator, Outcome};
use rhizodat_core::gid::gid_for;
use rhizodat_core::identity::IdGenerator;
use rhizodat_core::journal::{BranchRecord, Journal, JournalError, State};
use rhizodat_core::recovery::{Recovery, RecoveryReport};
use rhizodat_core::store::{BoxError, Branch, Participant, Participants};
use rhizodat_core::timer::TokioTimer;
use rhizodat_drevo::{DrevoBranch, DrevoJournal, DrevoStore, HeuristicOutcome, params};
use rhizodat_postgres::{PostgresJournal, PostgresStore};
use tokio::sync::{Mutex, MutexGuard};
use uuid::Uuid;

static FENCE: Mutex<()> = Mutex::const_new(());

fn env_test(key: &str) -> Option<String> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.env.test");
    let text = std::fs::read_to_string(path).ok()?;
    let prefix = format!("{key}=");
    text.lines()
        .filter(|l| !l.starts_with('#'))
        .find_map(|l| l.strip_prefix(&prefix))
        .map(str::to_owned)
}

fn reachable(host: &str, port: u16) -> bool {
    (host, port)
        .to_socket_addrs()
        .ok()
        .and_then(|mut a| a.next())
        .is_some_and(|addr| TcpStream::connect_timeout(&addr, Duration::from_millis(500)).is_ok())
}

/// The DSN under `key` (of `adapter`'s store), or `None` (skip) when it comes from `.env.test` and nothing listens.
fn dsn(key: &str, adapter: &Adapter) -> Option<String> {
    if let Ok(dsn) = std::env::var(key) {
        return Some(dsn);
    }
    let dsn = env_test(key)?;
    let parsed = adapter.parse(&dsn).expect("a valid DSN in .env.test");
    if reachable(
        parsed.host().unwrap_or("127.0.0.1"),
        parsed.port().unwrap_or(0),
    ) {
        Some(dsn)
    } else {
        eprintln!("skipped: {key} from .env.test is not running (docker compose up -d --wait)");
        None
    }
}

fn new_id() -> Uuid {
    IdGenerator::new().generate().unwrap()
}

/// Which graph a scenario runs on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Target {
    Bolt,
    #[cfg_attr(not(feature = "embedded"), allow(dead_code))]
    Embedded,
}

struct Graph {
    target: Target,
    /// The DSN another host would use (Bolt only).
    dsn: Option<String>,
    tag: String,
    store: DrevoStore,
    txns: Vec<Uuid>,
    _turn: Option<MutexGuard<'static, ()>>,
}

impl Graph {
    async fn new(target: Target) -> Option<Self> {
        let tag = format!("RsT{}", &new_id().simple().to_string()[20..]);
        let name = format!("graph_{tag}");
        let (dsn, store, turn) = match target {
            Target::Bolt => {
                let dsn = dsn("RHIZODAT_TEST_DREVO_BOLT", &rhizodat_drevo::ADAPTER)?;
                let turn = FENCE.lock().await;
                let store = DrevoStore::new(&dsn, &name).unwrap();
                (Some(dsn), store, Some(turn))
            }
            Target::Embedded => (None, DrevoStore::new("drevo+mem://", &name).unwrap(), None),
        };
        Some(Self {
            target,
            dsn,
            tag,
            store,
            txns: Vec::new(),
            _turn: turn,
        })
    }

    /// The same graph as another host (Bolt) or another component of this
    /// process (embedded: one process opens a graph once) would see it.
    fn elsewhere(&self) -> DrevoStore {
        match &self.dsn {
            Some(dsn) => DrevoStore::new(dsn, self.store.name()).unwrap(),
            None => self.store.clone(),
        }
    }

    fn journal(&self) -> DrevoJournal {
        let label = format!("{}J", self.tag);
        match &self.dsn {
            Some(dsn) => DrevoJournal::new(dsn, &label).unwrap(),
            None => DrevoJournal::in_graph_of(&self.store, &label).unwrap(),
        }
    }

    fn txn(&mut self) -> Uuid {
        let txn = new_id();
        self.txns.push(txn);
        txn
    }

    fn gid(&self, txn: Uuid) -> String {
        gid_for(txn, self.store.name())
    }

    async fn count(&self, label: &str) -> i64 {
        let found = self
            .store
            .read(
                &format!("MATCH (n:{label}) RETURN count(n) AS n"),
                params![],
            )
            .await
            .unwrap();
        found[0].int("n").unwrap()
    }

    async fn ours_prepared(&self) -> Vec<String> {
        let gids = self.store.participant().prepared_gids().await.unwrap();
        gids.into_iter()
            .filter(|g| g.ends_with(&self.tag))
            .collect()
    }

    async fn branch_with_task(&self, title: &str) -> DrevoBranch {
        let mut branch = self.store.branch().await.unwrap();
        let cypher = format!("CREATE (:{} {{title: $title}})", self.tag);
        branch
            .run(&cypher, params![("title", format!("{}:{title}", self.tag))])
            .await
            .unwrap();
        branch
    }

    async fn drop(self) {
        if self.target == Target::Embedded {
            return; // the graph goes with the store
        }
        let participant = self.store.participant();
        for gid in self.ours_prepared().await {
            participant
                .abort(&BranchRecord::new(self.store.name(), gid, true))
                .await
                .unwrap();
        }
        for label in [self.tag.clone(), format!("{}J", self.tag)] {
            self.store
                .write(&format!("MATCH (n:{label}) DETACH DELETE n"), params![])
                .await
                .unwrap();
        }
        for txn in &self.txns {
            let cypher = "MATCH (m:DatApplied {dat_txn: $t}) DETACH DELETE m";
            self.store
                .write(cypher, params![("t", txn.to_string())])
                .await
                .unwrap();
        }
    }
}

/// Run `scenario` on Bolt and, with the `embedded` feature, on an embedded graph.
macro_rules! on_both {
    ($($scenario:ident),* $(,)?) => {$(
        mod $scenario {
            #[tokio::test(flavor = "multi_thread")]
            async fn bolt() {
                super::$scenario(super::Target::Bolt).await;
            }

            #[cfg(feature = "embedded")]
            #[tokio::test(flavor = "multi_thread")]
            async fn embedded() {
                super::$scenario(super::Target::Embedded).await;
            }
        }
    )*};
}

on_both!(
    journal_compare_and_set_and_scans,
    journal_reads_nodes_written_by_the_python_reference,
    prepared_branch_is_committed_by_the_participant_idempotently,
    aborted_gid_is_gone_and_finishing_it_is_a_heuristic_outcome,
    one_phase_commit_and_rollback_of_an_open_branch,
    a_units_buffered_writes_commit_with_the_meta_transaction,
    the_prepared_fence_rejects_other_writers_with_a_retryable_error,
    a_bug_and_its_graph_link_commit_together,
    a_crash_after_the_graph_prepared_is_presumed_aborted_elsewhere,
    a_crash_after_the_decision_is_rolled_forward_elsewhere,
);

// --- journal ---------------------------------------------------------------------------

async fn journal_compare_and_set_and_scans(target: Target) {
    let Some(mut g) = Graph::new(target).await else {
        return;
    };
    let wall = Arc::new(std::sync::atomic::AtomicI64::new(1_000));
    let journal = {
        let wall = wall.clone();
        g.journal()
            .with_wall(move || wall.load(std::sync::atomic::Ordering::SeqCst))
    };
    let txn = g.txn();
    let branches = vec![
        BranchRecord::new("pg", gid_for(txn, "pg"), true),
        BranchRecord {
            payload: Some(vec![1, 2, 250]),
            ..BranchRecord::new("ch", gid_for(txn, "ch"), false)
        },
    ];
    let r = journal.begin(txn, &branches).await.unwrap();
    assert_eq!((r.state, r.created_ms), (State::Active, 1_000));
    assert!(journal.begin(txn, &[]).await.unwrap_err().is_conflict());

    wall.store(2_000, std::sync::atomic::Ordering::SeqCst);
    let r = journal
        .transition(txn, State::Active, State::Prepared, Some(77))
        .await
        .unwrap();
    assert_eq!(
        (r.state, r.dat_tx_time, r.updated_ms),
        (State::Prepared, Some(77), 2_000)
    );
    let err = journal
        .transition(txn, State::Active, State::Aborted, None)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            JournalError::Conflict {
                actual: Some(State::Prepared),
                ..
            }
        ),
        "{err}"
    );
    let err = journal
        .transition(new_id(), State::Active, State::Aborted, None)
        .await
        .unwrap_err();
    assert!(
        matches!(err, JournalError::Conflict { actual: None, .. }),
        "{err}"
    );

    assert_eq!(journal.get(txn).await.unwrap().unwrap().branches, branches);
    assert_eq!(journal.pending(2_001, None).await.unwrap().len(), 1);
    assert_eq!(journal.pending(2_001, Some(0)).await.unwrap(), []);
    assert_eq!(journal.pending(2_000, None).await.unwrap(), []);
    assert!(matches!(
        journal.forget(txn).await,
        Err(JournalError::NotFinal { .. })
    ));

    journal
        .transition(txn, State::Prepared, State::Committed, None)
        .await
        .unwrap();
    let done = journal
        .transition(txn, State::Committed, State::Done, None)
        .await
        .unwrap();
    assert_eq!(
        done.dat_tx_time,
        Some(77),
        "later transitions keep dat_tx_time"
    );
    journal.forget(txn).await.unwrap();
    assert_eq!(journal.get(txn).await.unwrap(), None);
    g.drop().await;
}

async fn journal_reads_nodes_written_by_the_python_reference(target: Target) {
    let Some(mut g) = Graph::new(target).await else {
        return;
    };
    let label = format!("{}J", g.tag);
    let journal = g.journal();
    let txn = g.txn();
    // What rhizodat.adapters.drevo.DrevoJournal writes, in Python's JSON spelling.
    let branches = format!(
        r#"[{{"store": "pg", "gid": "dat:{txn}:pg", "two_phase": true, "payload": null}}]"#
    );
    let cypher = format!(
        "CREATE (:{label} {{title: $title, dat_txn: $t, state: 'COMMITTED', dat_tx_time: 5, branches: $b, \
         created_ms: 1, updated_ms: 2}})"
    );
    let p = params![
        ("title", format!("{label}:{txn}")),
        ("t", txn.to_string()),
        ("b", branches)
    ];
    g.store.write(&cypher, p).await.unwrap();
    let record = journal.get(txn).await.unwrap().unwrap();
    assert_eq!(
        (record.state, record.dat_tx_time, record.updated_ms),
        (State::Committed, Some(5), 2)
    );
    assert_eq!(
        record.branches,
        [BranchRecord::new("pg", gid_for(txn, "pg"), true)]
    );
    g.drop().await;
}

// --- branch and participant ------------------------------------------------------------

async fn prepared_branch_is_committed_by_the_participant_idempotently(target: Target) {
    let Some(mut g) = Graph::new(target).await else {
        return;
    };
    let txn = g.txn();
    let gid = g.gid(txn);
    let mut branch = g.branch_with_task("login fails").await;
    branch.stage(txn, 42).await.unwrap();
    branch.prepare(&gid).await.unwrap();
    assert_eq!(g.ours_prepared().await, std::slice::from_ref(&gid));
    assert_eq!(g.count(&g.tag).await, 0, "prepared is invisible");

    let participant = g.store.participant();
    let record = BranchRecord::new(g.store.name(), &gid, true);
    assert!(!participant.is_applied(&record, txn).await.unwrap());
    participant.finish(&record, txn, 42).await.unwrap();
    assert_eq!(g.count(&g.tag).await, 1);
    assert!(participant.is_applied(&record, txn).await.unwrap());
    participant.finish(&record, txn, 42).await.unwrap(); // unknown gid + marker: already done
    assert_eq!(g.ours_prepared().await, Vec::<String>::new());
    g.drop().await;
}

async fn aborted_gid_is_gone_and_finishing_it_is_a_heuristic_outcome(target: Target) {
    let Some(mut g) = Graph::new(target).await else {
        return;
    };
    let txn = g.txn();
    let gid = g.gid(txn);
    let mut branch = g.branch_with_task("crash on save").await;
    branch.stage(txn, 1).await.unwrap();
    branch.prepare(&gid).await.unwrap();
    let participant = g.store.participant();
    let record = BranchRecord::new(g.store.name(), &gid, true);
    participant.abort(&record).await.unwrap();
    participant.abort(&record).await.unwrap(); // idempotent
    branch.rollback().await.unwrap(); // also once the gid is gone
    let err = participant.finish(&record, txn, 1).await.unwrap_err();
    assert!(err.is::<HeuristicOutcome>(), "{err}");
    assert_eq!(g.count(&g.tag).await, 0);
    g.drop().await;
}

async fn one_phase_commit_and_rollback_of_an_open_branch(target: Target) {
    let Some(mut g) = Graph::new(target).await else {
        return;
    };
    let mut kept = g.branch_with_task("kept").await;
    kept.stage(g.txn(), 1).await.unwrap();
    kept.commit_one_phase().await.unwrap();
    let mut dropped = g.branch_with_task("dropped").await;
    dropped.rollback().await.unwrap();
    assert_eq!(g.count(&g.tag).await, 1);
    g.drop().await;
}

async fn a_units_buffered_writes_commit_with_the_meta_transaction(target: Target) {
    let Some(mut g) = Graph::new(target).await else {
        return;
    };
    let txn = g.txn();
    let mut branch = g.store.branch().await.unwrap();
    // Two buffered statements; each stamps its node with the injected $dat_txn.
    let create = format!("CREATE (:{} {{title: $title, dat_txn: $dat_txn}})", g.tag);
    branch
        .enqueue(&create, params![("title", "first")])
        .unwrap();
    branch
        .enqueue(&create, params![("title", "second")])
        .unwrap();
    branch.stage(txn, 7).await.unwrap();
    assert_eq!(
        g.count(&g.tag).await,
        0,
        "staged is invisible before commit"
    );
    branch.commit_one_phase().await.unwrap();

    assert_eq!(g.count(&g.tag).await, 2);
    // Both nodes carry the meta-transaction's stamp.
    let stamped = g
        .store
        .read(
            &format!("MATCH (x:{} {{dat_txn: $t}}) RETURN count(x) AS n", g.tag),
            params![("t", txn.to_string())],
        )
        .await
        .unwrap();
    assert_eq!(stamped[0].int("n").unwrap(), 2);
    g.drop().await;
}

async fn the_prepared_fence_rejects_other_writers_with_a_retryable_error(target: Target) {
    let Some(mut g) = Graph::new(target).await else {
        return;
    };
    let txn = g.txn();
    let gid = g.gid(txn);
    let mut branch = g.branch_with_task("holds the fence").await;
    branch.stage(txn, 1).await.unwrap();
    branch.prepare(&gid).await.unwrap();

    let started = std::time::Instant::now();
    let err = g
        .store
        .write(&format!("CREATE (:{} {{title: 'late'}})", g.tag), params![])
        .await
        .unwrap_err();
    assert!(g.store.is_retryable(err.as_ref()), "{err}");
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "the fence is reported, not waited on"
    );
    assert_eq!(g.count(&g.tag).await, 0, "reads pass the fence");

    g.store
        .participant()
        .abort(&BranchRecord::new(g.store.name(), &gid, true))
        .await
        .unwrap();
    g.store
        .write(
            &format!("CREATE (:{} {{title: 'after'}})", g.tag),
            params![],
        )
        .await
        .unwrap();
    g.drop().await;
}

#[cfg(feature = "embedded")]
#[tokio::test(flavor = "multi_thread")]
async fn an_embedded_prepared_transaction_survives_a_restart() {
    let dir = std::env::temp_dir().join(format!("rhizodat-drevo-{}", new_id().simple()));
    std::fs::create_dir_all(&dir).unwrap();
    let dsn = format!("drevo://{}", dir.join("graph.drevo").display());
    let txn = new_id();
    let gid = gid_for(txn, "graph");
    {
        let store = DrevoStore::new(&dsn, "graph").unwrap();
        let mut branch = store.branch().await.unwrap();
        branch
            .run("CREATE (:Task {title: 'survives'})", params![])
            .await
            .unwrap();
        branch.stage(txn, 1).await.unwrap();
        branch.prepare(&gid).await.unwrap();
    } // the process "dies": the graph is closed with the transaction prepared

    let store = DrevoStore::new(&dsn, "graph").unwrap();
    assert_eq!(
        store.participant().prepared_gids().await.unwrap(),
        std::slice::from_ref(&gid)
    );
    let record = BranchRecord::new("graph", &gid, true);
    store.participant().finish(&record, txn, 1).await.unwrap();
    let found = store
        .read("MATCH (t:Task) RETURN count(t) AS n", params![])
        .await
        .unwrap();
    assert_eq!(found[0].int("n").unwrap(), 1);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

// --- a bug tracker over PostgreSQL and drevo ---------------------------------------------

/// Delegates to a branch and dies (panics) right after `prepare`.
struct DiesAfterPrepare(Box<dyn Branch>);

#[async_trait]
impl Branch for DiesAfterPrepare {
    fn store(&self) -> &str {
        self.0.store()
    }
    fn two_phase(&self) -> bool {
        true
    }
    async fn stage(&mut self, t: Uuid, ts: i64) -> Result<Option<Vec<u8>>, BoxError> {
        self.0.stage(t, ts).await
    }
    async fn prepare(&mut self, gid: &str) -> Result<(), BoxError> {
        self.0.prepare(gid).await?;
        panic!("the coordinator process dies");
    }
    async fn commit_one_phase(&mut self) -> Result<(), BoxError> {
        self.0.commit_one_phase().await
    }
    async fn rollback(&mut self) -> Result<(), BoxError> {
        self.0.rollback().await
    }
    fn is_retryable(&self, e: &(dyn Error + 'static)) -> bool {
        self.0.is_retryable(e)
    }
}

struct Tracker {
    graph: Graph,
    pg_dsn: String,
    schema: String,
    admin: tokio_postgres::Client,
    bugs: PostgresStore,
    journal: Arc<PostgresJournal>,
}

impl Tracker {
    async fn new(target: Target) -> Option<Self> {
        let pg_dsn = dsn("RHIZODAT_TEST_PG_DSN", &rhizodat_postgres::ADAPTER)?;
        let graph = Graph::new(target).await?;
        let (admin, conn) = tokio_postgres::connect(&pg_dsn, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(conn);
        let schema = graph.tag.to_lowercase();
        admin
            .batch_execute(&format!(
                "CREATE SCHEMA {schema}; CREATE SCHEMA {schema}_bugs"
            ))
            .await
            .unwrap();
        let bugs = PostgresStore::new(
            &pg_dsn,
            &format!("bugs_{}", graph.tag),
            Some(&format!("{schema}_bugs")),
        )
        .unwrap();
        bugs.install().await.unwrap();
        bugs.connect()
            .await
            .unwrap()
            .batch_execute("CREATE TABLE bugs (id uuid PRIMARY KEY)")
            .await
            .unwrap();
        let journal = Arc::new(
            PostgresJournal::open(&pg_dsn, Some(&schema), "dat_journal")
                .await
                .unwrap(),
        );
        Some(Self {
            graph,
            pg_dsn,
            schema,
            admin,
            bugs,
            journal,
        })
    }

    fn participants(bugs: &PostgresStore, graph: &DrevoStore) -> Participants {
        Participants::new()
            .with(bugs.name(), bugs.participant() as Arc<dyn Participant>)
            .with(graph.name(), graph.participant() as Arc<dyn Participant>)
    }

    fn coordinator(&self) -> Coordinator {
        Coordinator::new(
            Some(self.journal.clone()),
            Self::participants(&self.bugs, &self.graph.store),
            Arc::new(HybridLogicalClock::new()),
            Arc::new(TokioTimer::default()),
        )
    }

    /// Recovery with its own connections.
    async fn recover(&self) -> RecoveryReport {
        let bugs = PostgresStore::new(
            &self.pg_dsn,
            self.bugs.name(),
            Some(&format!("{}_bugs", self.schema)),
        )
        .unwrap();
        let graph = self.graph.elsewhere();
        let journal = Arc::new(
            PostgresJournal::open(&self.pg_dsn, Some(&self.schema), "dat_journal")
                .await
                .unwrap(),
        );
        let later = rhizodat_core::identity::system_ms() + 60_000;
        Recovery::new(journal, Self::participants(&bugs, &graph), 30_000)
            .unwrap()
            .with_wall(move || later)
            .run_once(100)
            .await
            .unwrap()
    }

    async fn bug_and_duplicate_link(&self) -> Vec<Box<dyn Branch>> {
        let bug = self.bugs.branch().await.unwrap();
        bug.client()
            .unwrap()
            .execute("INSERT INTO bugs (id) VALUES ($1)", &[&new_id()])
            .await
            .unwrap();
        let link = self.graph.branch_with_task("duplicate of #1").await;
        vec![Box::new(bug), Box::new(link)]
    }

    async fn bugs(&self) -> i64 {
        let sql = format!("SELECT count(*) FROM {}_bugs.bugs", self.schema);
        self.admin.query_one(&sql, &[]).await.unwrap().get(0)
    }

    async fn drop(self) {
        let rows = self
            .admin
            .query("SELECT gid FROM pg_prepared_xacts", &[])
            .await
            .unwrap();
        for gid in rows
            .iter()
            .map(|r| r.get::<_, String>(0))
            .filter(|g| g.ends_with(&self.graph.tag))
        {
            self.admin
                .batch_execute(&format!("ROLLBACK PREPARED '{gid}'"))
                .await
                .ok();
        }
        self.admin
            .batch_execute(&format!(
                "DROP SCHEMA {0} CASCADE; DROP SCHEMA {0}_bugs CASCADE",
                self.schema
            ))
            .await
            .unwrap();
        self.graph.drop().await;
    }
}

async fn a_bug_and_its_graph_link_commit_together(target: Target) {
    let Some(mut t) = Tracker::new(target).await else {
        return;
    };
    let txn = t.graph.txn();
    let outcome = t
        .coordinator()
        .commit(txn, &mut t.bug_and_duplicate_link().await)
        .await
        .unwrap();
    assert!(matches!(outcome, Outcome::Committed { .. }), "{outcome:?}");
    assert_eq!((t.bugs().await, t.graph.count(&t.graph.tag).await), (1, 1));
    assert_eq!(
        t.journal.get(txn).await.unwrap().unwrap().state,
        State::Done
    );
    assert_eq!(t.graph.ours_prepared().await, Vec::<String>::new());
    t.drop().await;
}

async fn a_crash_after_the_graph_prepared_is_presumed_aborted_elsewhere(target: Target) {
    let Some(mut t) = Tracker::new(target).await else {
        return;
    };
    let mut branches = t.bug_and_duplicate_link().await;
    let link = branches.pop().unwrap();
    branches.push(Box::new(DiesAfterPrepare(link)));
    let coordinator = Arc::new(t.coordinator());
    let txn = t.graph.txn();
    let died = tokio::spawn(async move { coordinator.commit(txn, &mut branches).await }).await;
    assert!(died.is_err_and(|e| e.is_panic()));
    assert_eq!(
        t.graph.ours_prepared().await.len(),
        1,
        "the graph's fence is up"
    );

    let report = t.recover().await;
    assert_eq!(report.aborted, [txn]);
    assert_eq!(t.graph.ours_prepared().await, Vec::<String>::new());
    assert_eq!((t.bugs().await, t.graph.count(&t.graph.tag).await), (0, 0));
    t.drop().await;
}

async fn a_crash_after_the_decision_is_rolled_forward_elsewhere(target: Target) {
    let Some(mut t) = Tracker::new(target).await else {
        return;
    };
    let txn = t.graph.txn();
    let mut branches = t.bug_and_duplicate_link().await;
    // The protocol by hand up to the decision, then the process "dies".
    let mut records = Vec::new();
    for b in &mut branches {
        b.stage(txn, 9).await.unwrap();
        records.push(BranchRecord::new(b.store(), gid_for(txn, b.store()), true));
    }
    t.journal.begin(txn, &records).await.unwrap();
    for b in &mut branches {
        let gid = gid_for(txn, b.store());
        b.prepare(&gid).await.unwrap();
    }
    t.journal
        .transition(txn, State::Active, State::Prepared, Some(9))
        .await
        .unwrap();
    t.journal
        .transition(txn, State::Prepared, State::Committed, None)
        .await
        .unwrap();
    drop(branches);

    assert_eq!(t.recover().await.finished, [txn]);
    assert_eq!((t.bugs().await, t.graph.count(&t.graph.tag).await), (1, 1));
    assert_eq!(
        t.journal.get(txn).await.unwrap().unwrap().state,
        State::Done
    );
    assert_eq!(t.recover().await.finished, Vec::<Uuid>::new());
    t.drop().await;
}
