//! rhizodat-postgres against a real server: the compose `postgres` service.
//!
//! The DSN comes from `RHIZODAT_TEST_PG_DSN`, else from `.env.test`. A DSN from
//! the environment must work (CI sets it, so a server that is down fails the
//! run); one from `.env.test` whose server is not running skips the test with a
//! hint, like the Python suite.
//!
//! Each test works in its own schemas: the journal in `<base>`, each store in
//! `<base>_<name>`. Stores must not share a schema: their branches of one
//! meta-transaction would insert the same `dat_txn` into one `dat_applied` and
//! wait for each other. Store names end with `<base>`, so leftover prepared gids
//! are found and rolled back afterwards.

use std::error::Error;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rhizodat_core::clock::HybridLogicalClock;
use rhizodat_core::coordinator::{CommitError, Coordinator, Outcome};
use rhizodat_core::gid::gid_for;
use rhizodat_core::identity::IdGenerator;
use rhizodat_core::journal::{BranchRecord, Journal, JournalError, State, encode_branches};
use rhizodat_core::recovery::Recovery;
use rhizodat_core::rows::RowValue;
use rhizodat_core::store::{BoxError, Branch, Participant, Participants};
use rhizodat_core::timer::TokioTimer;
use rhizodat_postgres::query::{QueryResult, query};
use rhizodat_postgres::{
    HeuristicOutcome, Isolation, PostgresBranch, PostgresJournal, PostgresRows, PostgresStore,
    is_retryable,
};
use uuid::Uuid;

fn env_test_dsn() -> Option<String> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.env.test");
    let text = std::fs::read_to_string(path).ok()?;
    text.lines()
        .filter(|l| !l.starts_with('#'))
        .find_map(|l| l.strip_prefix("RHIZODAT_TEST_PG_DSN="))
        .map(str::to_owned)
}

fn reachable(dsn: &str) -> bool {
    let config: tokio_postgres::Config = dsn.parse().expect("a valid DSN");
    let host = match config.get_hosts().first() {
        Some(tokio_postgres::config::Host::Tcp(h)) => h.clone(),
        _ => return true, // a socket path: let the driver report it
    };
    let port = config.get_ports().first().copied().unwrap_or(5432);
    (host.as_str(), port)
        .to_socket_addrs()
        .ok()
        .and_then(|mut a| a.next())
        .is_some_and(|addr| TcpStream::connect_timeout(&addr, Duration::from_millis(500)).is_ok())
}

/// The test DSN, or `None` (skip) when it comes from `.env.test` and nothing listens.
fn pg_dsn() -> Option<String> {
    if let Ok(dsn) = std::env::var("RHIZODAT_TEST_PG_DSN") {
        return Some(dsn);
    }
    let dsn = env_test_dsn()?;
    if reachable(&dsn) {
        Some(dsn)
    } else {
        eprintln!(
            "skipped: PostgreSQL from .env.test is not running (docker compose up -d --wait)"
        );
        None
    }
}

fn new_id() -> Uuid {
    IdGenerator::new().generate().unwrap()
}

struct Db {
    dsn: String,
    schema: String,
    admin: tokio_postgres::Client,
}

impl Db {
    async fn new() -> Option<Self> {
        let dsn = pg_dsn()?;
        let schema = format!("dat_rs_{}", &new_id().simple().to_string()[20..]);
        let (admin, conn) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("PostgreSQL is up");
        tokio::spawn(conn);
        admin
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .unwrap();
        Some(Self { dsn, schema, admin })
    }

    /// A store named `<name>_<base>` in its own schema `<base>_<name>`, installed.
    async fn store(&self, name: &str) -> PostgresStore {
        let schema = format!("{}_{name}", self.schema);
        self.admin
            .batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
            .await
            .unwrap();
        let store =
            PostgresStore::new(&self.dsn, &format!("{name}_{}", self.schema), Some(&schema))
                .unwrap();
        store.install().await.unwrap();
        store
    }

    async fn journal(&self) -> PostgresJournal {
        PostgresJournal::open(&self.dsn, Some(&self.schema), "dat_journal")
            .await
            .unwrap()
    }

    /// Rows in `table` of the store `name`.
    async fn count(&self, name: &str, table: &str) -> i64 {
        let sql = format!("SELECT count(*) FROM {}_{name}.{table}", self.schema);
        self.admin
            .query_one(&sql, &[])
            .await
            .map_or(-1, |r| r.get(0))
    }

    async fn ours_prepared(&self) -> Vec<String> {
        let rows = self
            .admin
            .query("SELECT gid FROM pg_prepared_xacts ORDER BY prepared", &[])
            .await
            .unwrap();
        rows.iter()
            .map(|r| r.get::<_, String>(0))
            .filter(|g| g.ends_with(&self.schema))
            .collect()
    }

    async fn drop(self) {
        for gid in self.ours_prepared().await {
            self.admin
                .batch_execute(&format!("ROLLBACK PREPARED '{gid}'"))
                .await
                .ok();
        }
        let rows = self
            .admin
            .query(
                "SELECT nspname FROM pg_namespace WHERE nspname = $1 OR nspname LIKE $1 || '\\_%'",
                &[&self.schema],
            )
            .await
            .unwrap();
        let schemas: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
        self.admin
            .batch_execute(&format!("DROP SCHEMA {} CASCADE", schemas.join(", ")))
            .await
            .unwrap();
    }
}

macro_rules! db {
    () => {
        match Db::new().await {
            Some(db) => db,
            None => return,
        }
    };
}

fn with_schema(dsn: &str, schema: &str) -> String {
    let sep = if dsn.contains('?') { '&' } else { '?' };
    format!("{dsn}{sep}schema={schema}")
}

// --- configuration ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn the_dsn_names_the_schema_of_a_store_and_a_journal() {
    let db = db!();
    let schema = format!("{}_dsn", db.schema);
    db.admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let dsn = with_schema(&db.dsn, &schema);
    let store = PostgresStore::new(&dsn, &format!("dsn_{}", db.schema), None).unwrap();
    store.install().await.unwrap();
    assert_eq!(db.count("dsn", "dat_applied").await, 0);
    let current: String = store
        .connect()
        .await
        .unwrap()
        .query_one("SELECT current_schema()", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(current, schema);
    assert!(PostgresStore::new(&dsn, "x", Some(&schema)).is_ok());
    assert!(PostgresStore::new(&dsn, "x", Some("other")).is_err());
    assert!(PostgresStore::new(&with_schema(&db.dsn, "a-b"), "x", None).is_err());

    PostgresJournal::open(&dsn, None, "dat_journal")
        .await
        .unwrap();
    assert_eq!(db.count("dsn", "dat_journal").await, 0);
    assert!(
        PostgresJournal::open(&dsn, Some("other"), "dat_journal")
            .await
            .is_err()
    );
    db.drop().await;
}

// --- journal ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn journal_compare_and_set_and_scans() {
    let db = db!();
    let wall = Arc::new(std::sync::atomic::AtomicI64::new(1_000));
    let journal = {
        let wall = wall.clone();
        db.journal()
            .await
            .with_wall(move || wall.load(std::sync::atomic::Ordering::SeqCst))
    };
    let txn = new_id();
    let branches = vec![
        BranchRecord::new("pg", gid_for(txn, "pg"), true),
        BranchRecord {
            payload: Some(vec![0, 159, 255]),
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
    assert!(matches!(err, JournalError::Conflict { actual: None, .. }));

    let got = journal.get(txn).await.unwrap().unwrap();
    assert_eq!(got.branches, branches);
    assert_eq!(journal.pending(2_001, None).await.unwrap().len(), 1);
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
    assert_eq!(journal.pending(i64::MAX, None).await.unwrap(), []);
    journal.forget(txn).await.unwrap();
    assert_eq!(journal.get(txn).await.unwrap(), None);
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn journal_reads_records_written_by_the_python_reference() {
    let db = db!();
    let journal = db.journal().await;
    let txn = new_id();
    // The spelling Python's json.dumps writes (see conformance/journal.json).
    let column = format!(
        r#"[{{"store": "pg", "gid": "dat:{txn}:pg", "two_phase": true, "payload": null}}, {{"store": "ch", "gid": "dat:{txn}:ch", "two_phase": false, "payload": "eyJ2IjoxfQ=="}}]"#
    );
    db.admin
        .execute(
            &format!(
                "INSERT INTO {}.dat_journal VALUES ($1, 'COMMITTED', 5, $2, 1, 2)",
                db.schema
            ),
            &[&txn, &column],
        )
        .await
        .unwrap();
    let record = journal.get(txn).await.unwrap().unwrap();
    assert_eq!(record.state, State::Committed);
    assert_eq!(
        record.branches[1].payload.as_deref(),
        Some(&br#"{"v":1}"#[..])
    );
    assert_eq!(
        rhizodat_core::journal::decode_branches(&encode_branches(&record.branches)).unwrap(),
        record.branches
    );
    db.drop().await;
}

// --- branch and participant ------------------------------------------------------------

async fn with_rows(store: &PostgresStore) {
    store
        .connect()
        .await
        .unwrap()
        .batch_execute("CREATE TABLE IF NOT EXISTS rows (id uuid PRIMARY KEY, v text)")
        .await
        .unwrap();
}

async fn insert(branch: &PostgresBranch, id: Uuid) {
    branch
        .client()
        .unwrap()
        .execute("INSERT INTO rows (id, v) VALUES ($1, 'x')", &[&id])
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn prepared_branch_is_committed_by_the_participant_idempotently() {
    let db = db!();
    let store = db.store("orders").await;
    with_rows(&store).await;
    let (txn, id) = (new_id(), new_id());
    let gid = gid_for(txn, store.name());
    let mut branch = store.branch().await.unwrap();
    insert(&branch, id).await;
    assert_eq!(branch.stage(txn, 42).await.unwrap(), None);
    branch.prepare(&gid).await.unwrap();
    assert!(
        branch.client().is_err(),
        "the session is released after PREPARE"
    );
    assert_eq!(db.count("orders", "rows").await, 0, "prepared is invisible");

    let participant = store.participant();
    assert!(participant.prepared_gids().await.unwrap().contains(&gid));
    let record = BranchRecord::new(store.name(), &gid, true);
    assert!(!participant.is_applied(&record, txn).await.unwrap());
    participant.finish(&record, txn, 42).await.unwrap();
    assert_eq!(db.count("orders", "rows").await, 1);
    assert!(participant.is_applied(&record, txn).await.unwrap());
    participant.finish(&record, txn, 42).await.unwrap(); // unknown gid + marker: already done
    assert_eq!(db.ours_prepared().await, Vec::<String>::new());
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn aborted_gid_is_gone_and_finishing_it_is_a_heuristic_outcome() {
    let db = db!();
    let store = db.store("orders").await;
    with_rows(&store).await;
    let txn = new_id();
    let gid = gid_for(txn, store.name());
    let mut branch = store.branch().await.unwrap();
    insert(&branch, new_id()).await;
    branch.stage(txn, 1).await.unwrap();
    branch.prepare(&gid).await.unwrap();
    let participant = store.participant();
    let record = BranchRecord::new(store.name(), &gid, true);
    participant.abort(&record).await.unwrap();
    participant.abort(&record).await.unwrap(); // idempotent
    branch.rollback().await.unwrap(); // also idempotent once the gid is gone
    let err = participant.finish(&record, txn, 1).await.unwrap_err();
    assert!(err.is::<HeuristicOutcome>(), "{err}");
    assert_eq!(db.count("orders", "rows").await, 0);
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn one_phase_commit_and_rollback_of_an_open_branch() {
    let db = db!();
    let store = db.store("orders").await;
    with_rows(&store).await;
    let mut kept = store.branch().await.unwrap();
    insert(&kept, new_id()).await;
    kept.stage(new_id(), 1).await.unwrap();
    kept.commit_one_phase().await.unwrap();
    let mut dropped = store.branch().await.unwrap();
    insert(&dropped, new_id()).await;
    dropped.rollback().await.unwrap();
    assert_eq!(
        (
            db.count("orders", "rows").await,
            db.count("orders", "dat_applied").await
        ),
        (1, 1)
    );
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn buffered_rows_cross_the_boundary_and_commit_with_the_meta_transaction() {
    let db = db!();
    let store = db.store("orders").await;
    store
        .connect()
        .await
        .unwrap()
        .batch_execute(
            "CREATE TABLE items (\
             id uuid, qty int, price numeric(10,2), note text, ok bool, raw bytea, \
             when_ timestamptz, dat_txn uuid, dat_tx_time bigint)",
        )
        .await
        .unwrap();
    let (txn, id) = (new_id(), new_id());
    let gid = gid_for(txn, store.name());
    let mut rows = PostgresRows::new();
    rows.insert(
        "items",
        &["id", "qty", "price", "note", "ok", "raw", "when_"],
        vec![
            vec![
                RowValue::Uuid(id),
                RowValue::Int(3),
                RowValue::Decimal("12.34".into()),
                RowValue::Str("hi".into()),
                RowValue::Bool(true),
                RowValue::Bytes(vec![0xff, 0x00, 0x6f]),
                RowValue::DateTime("2026-10-20T09:30:00+00:00".into()),
            ],
            vec![
                RowValue::Null,
                RowValue::Null,
                RowValue::Null,
                RowValue::Null,
                RowValue::Null,
                RowValue::Null,
                RowValue::Null,
            ],
        ],
    )
    .unwrap();
    let mut branch = store.branch().await.unwrap();
    branch.load(rows);
    assert_eq!(branch.stage(txn, 42).await.unwrap(), None);
    branch.prepare(&gid).await.unwrap();
    assert_eq!(
        db.count("orders", "items").await,
        0,
        "prepared is invisible"
    );

    let participant = store.participant();
    let record = BranchRecord::new(store.name(), &gid, true);
    participant.finish(&record, txn, 42).await.unwrap();
    assert_eq!(db.count("orders", "items").await, 2);
    let row = db
        .admin
        .query_one(
            &format!(
                "SELECT qty, price::text, note, ok, raw, dat_txn, dat_tx_time \
                 FROM {}_orders.items WHERE id = $1",
                db.schema
            ),
            &[&id],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, i32>(0), 3);
    assert_eq!(row.get::<_, String>(1), "12.34");
    assert_eq!(row.get::<_, String>(2), "hi");
    assert!(row.get::<_, bool>(3));
    assert_eq!(row.get::<_, Vec<u8>>(4), vec![0xff, 0x00, 0x6f]);
    assert_eq!(row.get::<_, Uuid>(5), txn);
    assert_eq!(row.get::<_, i64>(6), 42);
    assert_eq!(db.ours_prepared().await, Vec::<String>::new());
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn rows_for_a_table_without_the_reserved_columns_fail_at_stage() {
    let db = db!();
    let store = db.store("orders").await;
    store
        .connect()
        .await
        .unwrap()
        .batch_execute("CREATE TABLE plain (a int)")
        .await
        .unwrap();
    let mut branch = store.branch().await.unwrap();
    branch
        .insert("plain", &["a"], vec![vec![RowValue::Int(1)]])
        .unwrap();
    let err = branch.stage(new_id(), 1).await.unwrap_err();
    assert!(err.to_string().contains("dat_txn"), "{err}");
    assert!(!branch.is_retryable(err.as_ref()), "{err}");
    branch.rollback().await.unwrap();
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn deferred_constraints_fail_at_stage_and_are_not_retryable() {
    let db = db!();
    let store = db.store("orders").await;
    store
        .connect()
        .await
        .unwrap()
        .batch_execute("CREATE TABLE uniq (k int, CONSTRAINT k_unique UNIQUE (k) DEFERRABLE INITIALLY DEFERRED)")
        .await
        .unwrap();
    let mut branch = store.branch().await.unwrap();
    branch
        .client()
        .unwrap()
        .batch_execute("INSERT INTO uniq VALUES (1), (1)")
        .await
        .unwrap();
    let err = branch.stage(new_id(), 1).await.unwrap_err();
    assert!(!branch.is_retryable(err.as_ref()), "{err}");
    branch.rollback().await.unwrap();
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn serialization_failure_is_retryable() {
    let db = db!();
    let store = db
        .store("orders")
        .await
        .with_isolation(Isolation::Serializable);
    store
        .connect()
        .await
        .unwrap()
        .batch_execute(
            "CREATE TABLE acct (id int PRIMARY KEY, n int); INSERT INTO acct VALUES (1, 0), (2, 0)",
        )
        .await
        .unwrap();
    let (a, b) = (store.branch().await.unwrap(), store.branch().await.unwrap());
    // Write skew: each reads the other's row, then writes its own.
    a.client()
        .unwrap()
        .query("SELECT n FROM acct WHERE id = 2", &[])
        .await
        .unwrap();
    b.client()
        .unwrap()
        .query("SELECT n FROM acct WHERE id = 1", &[])
        .await
        .unwrap();
    a.client()
        .unwrap()
        .execute("UPDATE acct SET n = 1 WHERE id = 1", &[])
        .await
        .unwrap();
    b.client()
        .unwrap()
        .execute("UPDATE acct SET n = 1 WHERE id = 2", &[])
        .await
        .unwrap();
    let (mut a, mut b) = (a, b);
    a.commit_one_phase().await.unwrap();
    let err = b.commit_one_phase().await.unwrap_err();
    assert!(is_retryable(err.as_ref()), "{err}");
    db.drop().await;
}

// --- meta-transactions over two PostgreSQL stores ---------------------------------------

/// Delegates to a PostgreSQL branch and dies (panics) right after `prepare`.
struct DiesAfterPrepare(PostgresBranch);

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

struct Erp {
    db: Db,
    orders: PostgresStore,
    ledger: PostgresStore,
    journal: Arc<PostgresJournal>,
}

impl Erp {
    async fn new() -> Option<Self> {
        let db = Db::new().await?;
        let orders = db.store("orders").await;
        let ledger = db.store("ledger").await;
        with_rows(&orders).await;
        let journal = Arc::new(db.journal().await);
        Some(Self {
            db,
            orders,
            ledger,
            journal,
        })
    }

    fn participants(&self) -> Participants {
        Participants::new()
            .with(
                self.orders.name(),
                self.orders.participant() as Arc<dyn Participant>,
            )
            .with(
                self.ledger.name(),
                self.ledger.participant() as Arc<dyn Participant>,
            )
    }

    fn coordinator(&self) -> Coordinator {
        Coordinator::new(
            Some(self.journal.clone()),
            self.participants(),
            Arc::new(HybridLogicalClock::new()),
            Arc::new(TokioTimer::default()),
        )
    }

    /// Recovery on "another host": its own participants and journal connection.
    async fn recover(&self) -> rhizodat_core::recovery::RecoveryReport {
        let schema = |name: &str| format!("{}_{name}", self.db.schema);
        let orders =
            PostgresStore::new(&self.db.dsn, self.orders.name(), Some(&schema("orders"))).unwrap();
        let ledger =
            PostgresStore::new(&self.db.dsn, self.ledger.name(), Some(&schema("ledger"))).unwrap();
        let participants = Participants::new()
            .with(orders.name(), orders.participant() as Arc<dyn Participant>)
            .with(ledger.name(), ledger.participant() as Arc<dyn Participant>);
        let journal = Arc::new(self.db.journal().await);
        let later = rhizodat_core::identity::system_ms() + 60_000;
        Recovery::new(journal, participants, 30_000)
            .unwrap()
            .with_wall(move || later)
            .run_once(100)
            .await
            .unwrap()
    }

    async fn order_and_entry(&self) -> Vec<Box<dyn Branch>> {
        let order = self.orders.branch().await.unwrap();
        insert(&order, new_id()).await;
        let entry = self.ledger.branch().await.unwrap();
        entry
            .client()
            .unwrap()
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS entries (n int); INSERT INTO entries VALUES (1)",
            )
            .await
            .unwrap();
        vec![Box::new(order), Box::new(entry)]
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn order_and_ledger_entry_commit_together() {
    let Some(erp) = Erp::new().await else { return };
    let txn = new_id();
    let outcome = erp
        .coordinator()
        .commit(txn, &mut erp.order_and_entry().await)
        .await
        .unwrap();
    assert!(
        matches!(
            outcome,
            Outcome::Committed {
                dat_tx_time: Some(_),
                ..
            }
        ),
        "{outcome:?}"
    );
    assert_eq!(
        (
            erp.db.count("orders", "rows").await,
            erp.db.count("ledger", "entries").await
        ),
        (1, 1)
    );
    assert_eq!(
        erp.journal.get(txn).await.unwrap().unwrap().state,
        State::Done
    );
    assert_eq!(erp.db.ours_prepared().await, Vec::<String>::new());
    erp.db.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failing_branch_aborts_both_stores() {
    let Some(erp) = Erp::new().await else { return };
    let mut branches = erp.order_and_entry().await;
    // A statement error poisons the ledger transaction: prepare fails, nothing commits.
    branches[1] = {
        let b = erp.ledger.branch().await.unwrap();
        b.client().unwrap().batch_execute("SELECT 1/0").await.ok();
        Box::new(b)
    };
    let txn = new_id();
    let err = erp
        .coordinator()
        .commit(txn, &mut branches)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            CommitError::Aborted {
                retryable: false,
                ..
            }
        ),
        "{err}"
    );
    assert_eq!(erp.db.count("orders", "rows").await, 0);
    assert_eq!(erp.db.ours_prepared().await, Vec::<String>::new());
    let state = erp.journal.get(txn).await.unwrap().map(|r| r.state);
    assert!(matches!(state, None | Some(State::Aborted)), "{state:?}");
    erp.db.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_crash_after_prepare_is_presumed_aborted_by_recovery_elsewhere() {
    let Some(erp) = Erp::new().await else { return };
    let mut branches = erp.order_and_entry().await;
    // The second lock holder dies after its prepare; both stores hold prepared gids.
    branches.reverse();
    let order = branches.pop().unwrap();
    drop(order); // closes its session: the open transaction is rolled back
    let order = erp.orders.branch().await.unwrap();
    insert(&order, new_id()).await;
    branches.push(Box::new(DiesAfterPrepare(order)));
    let coordinator = Arc::new(erp.coordinator());
    let txn = new_id();
    let died = tokio::spawn(async move { coordinator.commit(txn, &mut branches).await }).await;
    assert!(died.is_err_and(|e| e.is_panic()));
    assert_eq!(erp.db.ours_prepared().await.len(), 2);
    assert_eq!(
        erp.journal.get(txn).await.unwrap().unwrap().state,
        State::Active
    );

    let report = erp.recover().await;
    assert_eq!(report.aborted, [txn]);
    assert_eq!(erp.db.ours_prepared().await, Vec::<String>::new());
    assert_eq!(erp.db.count("orders", "rows").await, 0);
    erp.db.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_crash_after_the_decision_is_rolled_forward_by_recovery_elsewhere() {
    let Some(erp) = Erp::new().await else { return };
    let txn = new_id();
    let mut branches = erp.order_and_entry().await;
    // Run the protocol by hand up to the decision, then "die": nothing is finished.
    let ts = 9;
    let mut records = Vec::new();
    for b in &mut branches {
        b.stage(txn, ts).await.unwrap();
        records.push(BranchRecord::new(b.store(), gid_for(txn, b.store()), true));
    }
    erp.journal.begin(txn, &records).await.unwrap();
    for b in &mut branches {
        let gid = gid_for(txn, b.store());
        b.prepare(&gid).await.unwrap();
    }
    erp.journal
        .transition(txn, State::Active, State::Prepared, Some(ts))
        .await
        .unwrap();
    erp.journal
        .transition(txn, State::Prepared, State::Committed, None)
        .await
        .unwrap();
    drop(branches);

    let report = erp.recover().await;
    assert_eq!(report.finished, [txn]);
    assert_eq!(
        (
            erp.db.count("orders", "rows").await,
            erp.db.count("ledger", "entries").await
        ),
        (1, 1)
    );
    assert_eq!(
        erp.journal.get(txn).await.unwrap().unwrap().state,
        State::Done
    );
    assert_eq!(erp.recover().await.finished, Vec::<Uuid>::new());
    erp.db.drop().await;
}

#[tokio::test]
async fn native_sql_binds_by_type_and_decodes_by_column() {
    let Some(db) = Db::new().await else { return };
    let store = db.store("q").await;
    let client = store.connect().await.unwrap();
    let ddl = query(
        &client,
        "CREATE TABLE people (id bigint, small int, name text, ok boolean, tag uuid, \
         raw bytea, doc jsonb, at timestamptz, day date, price numeric)",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(ddl, QueryResult::default());
    let tag = new_id();
    query(
        &client,
        "INSERT INTO people VALUES ($1, $2, $3, $4, $5, $6, $7, \
         '2026-10-31T09:30:15.123456+00:00', '2026-10-31', 12.5)",
        &[
            RowValue::Int(1),
            RowValue::Int(7),
            RowValue::Str("ann".into()),
            RowValue::Bool(true),
            RowValue::Uuid(tag),
            RowValue::Bytes(vec![0, 255]),
            RowValue::Str(r#"{"n": 1}"#.into()),
        ],
    )
    .await
    .unwrap();
    query(
        &client,
        "INSERT INTO people (id, name) VALUES ($1, $2)",
        &[RowValue::Int(2), RowValue::Null],
    )
    .await
    .unwrap();
    let result = query(
        &client,
        "SELECT id, small, name, ok, tag, raw, doc, at, day FROM people \
         WHERE id = $1 OR name = $2 ORDER BY id",
        &[RowValue::Int(2), RowValue::Str("ann".into())],
    )
    .await
    .unwrap();
    assert_eq!(
        result.columns,
        [
            "id", "small", "name", "ok", "tag", "raw", "doc", "at", "day"
        ]
    );
    assert_eq!(
        result.rows[0],
        [
            RowValue::Int(1),
            RowValue::Int(7),
            RowValue::Str("ann".into()),
            RowValue::Bool(true),
            RowValue::Uuid(tag),
            RowValue::Bytes(vec![0, 255]),
            RowValue::Map(vec![("n".into(), RowValue::Int(1))]),
            RowValue::DateTime("2026-10-31T09:30:15.123456Z".into()),
            RowValue::Date("2026-10-31".into()),
        ]
    );
    assert_eq!(result.rows[1][0], RowValue::Int(2));
    assert!(result.rows[1][1..7].iter().all(|v| *v == RowValue::Null));

    let err = query(&client, "SELECT price FROM people", &[])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("price::text"), "{err}");
    let text = query(
        &client,
        "SELECT price::text AS p FROM people WHERE id = 1",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(text.rows, [[RowValue::Str("12.5".into())]]);
    let err = query(&client, "SELECT $1::int8", &[RowValue::Str("x".into())])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("cannot bind $1"), "{err}");
    let err = query(&client, "SELECT $1::int8", &[]).await.unwrap_err();
    assert!(err.to_string().contains("1 parameter"), "{err}");
    let err = query(&client, "SELECT * FROM nope", &[]).await.unwrap_err();
    assert!(err.to_string().contains("nope"), "{err}");
    db.drop().await;
}
