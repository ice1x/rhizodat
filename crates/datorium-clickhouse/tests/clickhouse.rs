//! datorium-clickhouse against a real server: the compose `clickhouse` service.
//!
//! The DSN comes from `DATORIUM_TEST_CLICKHOUSE_DSN`, else from `.env.test`;
//! one from `.env.test` whose server is not running skips the test with a
//! hint. Each test gets its own database, dropped afterwards.

use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use clickhouse::Client;
use datorium_clickhouse::{ClickHouseStore, is_retryable};
use datorium_core::clock::HybridLogicalClock;
use datorium_core::coordinator::{Coordinator, Outcome};
use datorium_core::dsn::parse_dsn;
use datorium_core::gid::gid_for;
use datorium_core::identity::IdGenerator;
use datorium_core::journal::{BranchRecord, Journal, State};
use datorium_core::recovery::Recovery;
use datorium_core::rows::{Batch, RowValue, encode_batches};
use datorium_core::store::{Branch, Participant, Participants};
use datorium_core::timer::TokioTimer;
use datorium_postgres::{PostgresJournal, PostgresStore};
use uuid::Uuid;

fn env_test(key: &str) -> Option<String> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.env.test");
    let text = std::fs::read_to_string(path).ok()?;
    let prefix = format!("{key}=");
    text.lines()
        .filter(|l| !l.starts_with('#'))
        .find_map(|l| l.strip_prefix(&prefix))
        .map(str::to_owned)
}

/// The DSN under `key`, or `None` (skip) when it comes from `.env.test` and nothing listens.
fn dsn(key: &str) -> Option<String> {
    if let Ok(dsn) = std::env::var(key) {
        return Some(dsn);
    }
    let dsn = env_test(key)?;
    let parsed = parse_dsn(&dsn).expect("a valid DSN in .env.test");
    let addr = (
        parsed.host().unwrap_or("127.0.0.1"),
        parsed.port().unwrap_or(0),
    )
        .to_socket_addrs()
        .ok()?
        .next()?;
    if TcpStream::connect_timeout(&addr, Duration::from_millis(500)).is_ok() {
        Some(dsn)
    } else {
        eprintln!(
            "skipped: {key} from .env.test is not running (docker compose --profile clickhouse up -d --wait)"
        );
        None
    }
}

fn new_id() -> Uuid {
    IdGenerator::new().generate().unwrap()
}

struct Db {
    name: String,
    admin: Client,
    store: ClickHouseStore,
}

impl Db {
    async fn new() -> Option<Self> {
        let server = dsn("DATORIUM_TEST_CLICKHOUSE_DSN")?;
        let parsed = parse_dsn(&server).unwrap();
        let name = format!("dat_rs_{}", &new_id().simple().to_string()[20..]);
        let admin = Client::default()
            .with_url(format!(
                "http://{}:{}",
                parsed.host().unwrap(),
                parsed.port().unwrap()
            ))
            .with_user(parsed.user().unwrap_or("default"))
            .with_password(parsed.password().unwrap_or_default());
        admin
            .query(&format!("CREATE DATABASE {name}"))
            .execute()
            .await
            .unwrap();
        let (base, _) = server.rsplit_once('/').unwrap();
        let store = ClickHouseStore::new(&format!("{base}/{name}"), "feed").unwrap();
        store.install().await.unwrap();
        let admin = admin.with_database(name.clone());
        Some(Self { name, admin, store })
    }

    async fn ddl(&self, sql: &str) {
        self.admin.query(sql).execute().await.unwrap();
    }

    async fn activity(&self) {
        self.ddl(
            "CREATE TABLE activity (task_id UUID, event String, dat_txn UUID, dat_tx_time Int64) \
             ENGINE = MergeTree ORDER BY task_id",
        )
        .await;
    }

    async fn count(&self, sql: &str) -> u64 {
        self.admin.query(sql).fetch_one().await.unwrap()
    }

    async fn drop(self) {
        self.admin
            .query(&format!("DROP DATABASE {} SYNC", self.name))
            .execute()
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

fn events(task: Uuid, names: &[&str]) -> Vec<Vec<RowValue>> {
    names
        .iter()
        .map(|n| vec![RowValue::Uuid(task), RowValue::Str((*n).to_owned())])
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn stage_checks_the_schema_and_the_size_limit() {
    let db = db!();
    db.activity().await;
    db.ddl("CREATE TABLE plain (x Int64) ENGINE = MergeTree ORDER BY x")
        .await;
    let task = new_id();

    let mut unknown_column = db.store.branch();
    unknown_column
        .insert(
            "activity",
            &["task_id", "colour"],
            vec![vec![RowValue::Uuid(task), RowValue::Null]],
        )
        .unwrap();
    let err = unknown_column.stage(new_id(), 1).await.unwrap_err();
    assert!(err.to_string().contains("colour"), "{err}");
    assert!(!unknown_column.is_retryable(err.as_ref()));

    let mut without_service_columns = db.store.branch();
    without_service_columns
        .insert("plain", &["x"], vec![vec![RowValue::Int(1)]])
        .unwrap();
    let err = without_service_columns
        .stage(new_id(), 1)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("dat_txn, dat_tx_time"), "{err}");

    let small = db.store.clone().with_max_rows_per_table(2);
    let mut too_many = small.branch();
    too_many
        .insert(
            "activity",
            &["task_id", "event"],
            events(task, &["a", "b", "c"]),
        )
        .unwrap();
    let err = too_many.stage(new_id(), 1).await.unwrap_err();
    assert!(err.to_string().contains("max_rows_per_table=2"), "{err}");
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_committed_branch_is_applied_once_with_its_marker() {
    let db = db!();
    db.activity().await;
    let (txn, task) = (new_id(), new_id());
    let mut branch = db.store.branch();
    branch
        .insert(
            "activity",
            &["task_id", "event"],
            events(task, &["opened", "assigned"]),
        )
        .unwrap();
    let payload = branch
        .stage(txn, 77)
        .await
        .unwrap()
        .expect("rows travel in the journal");
    branch.commit_one_phase().await.unwrap();
    branch.commit_one_phase().await.unwrap(); // a retried commit inserts nothing twice
    assert_eq!(db.count("SELECT count() FROM activity").await, 2);
    let ts: i64 = db
        .admin
        .query("SELECT any(dat_tx_time) FROM activity")
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(ts, 77);

    let participant = db.store.participant();
    let record = BranchRecord {
        payload: Some(payload),
        ..BranchRecord::new("feed", gid_for(txn, "feed"), false)
    };
    assert!(participant.is_applied(&record, txn).await.unwrap());
    participant.finish(&record, txn, 77).await.unwrap(); // recovery finishing it again
    assert_eq!(db.count("SELECT count() FROM activity").await, 2);
    assert_eq!(db.count("SELECT count() FROM dat_applied").await, 1);
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_partly_inserted_batch_is_replaced_not_duplicated() {
    let db = db!();
    db.activity().await;
    let (txn, task) = (new_id(), new_id());
    let mut branch = db.store.branch();
    branch
        .insert(
            "activity",
            &["task_id", "event"],
            events(task, &["a", "b", "c"]),
        )
        .unwrap();
    let payload = branch.stage(txn, 5).await.unwrap().unwrap();
    // A crash left one of the three rows (one partition of the insert, say).
    db.admin
        .query("INSERT INTO activity VALUES (?, 'a', ?, 5)")
        .bind(task)
        .bind(txn)
        .execute()
        .await
        .unwrap();
    let record = BranchRecord {
        payload: Some(payload),
        ..BranchRecord::new("feed", gid_for(txn, "feed"), false)
    };
    db.store
        .participant()
        .finish(&record, txn, 5)
        .await
        .unwrap();
    assert_eq!(db.count("SELECT count() FROM activity").await, 3);
    assert_eq!(db.count("SELECT uniqExact(event) FROM activity").await, 3);
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_values_reach_their_columns() {
    let db = db!();
    db.ddl(
        "CREATE TABLE ledger (id UUID, amount Decimal(14, 4), booked DateTime64(6, 'UTC'), day Date, \
         rate Float64, attrs Map(String, String), tags Array(String), note Nullable(String), \
         dat_txn UUID, dat_tx_time Int64) ENGINE = MergeTree ORDER BY id",
    )
    .await;
    let (txn, id) = (new_id(), new_id());
    let mut branch = db.store.branch();
    branch
        .insert(
            "ledger",
            &[
                "id", "amount", "booked", "day", "rate", "attrs", "tags", "note",
            ],
            vec![vec![
                RowValue::Uuid(id),
                RowValue::Decimal("12.3400".into()),
                RowValue::DateTime("2026-10-12T09:30:15.123456+03:00".into()),
                RowValue::Date("2026-10-12".into()),
                RowValue::Float(f64::INFINITY),
                RowValue::Map(vec![("k".into(), RowValue::Str("v".into()))]),
                RowValue::List(vec![RowValue::Str("a".into()), RowValue::Str("b".into())]),
                RowValue::Null,
            ]],
        )
        .unwrap();
    branch.stage(txn, 1).await.unwrap();
    branch.commit_one_phase().await.unwrap();
    let row: String = db
        .admin
        .query(
            "SELECT concat(toString(id), '|', toString(amount), '|', toString(booked), '|', toString(day), '|', \
             toString(rate), '|', toString(attrs), '|', toString(tags), '|', toString(isNull(note))) FROM ledger",
        )
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(
        row,
        format!("{id}|12.34|2026-10-12 06:30:15.123456|2026-10-12|inf|{{'k':'v'}}|['a','b']|1")
    );
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn recovery_applies_a_payload_written_by_the_python_reference() {
    let db = db!();
    db.activity().await;
    let (txn, task) = (new_id(), new_id());
    // datorium.core.rows.encode_batches output for one activity row.
    let payload = format!(
        r#"{{"v":1,"batches":[{{"table":"activity","columns":["task_id","event","dat_txn","dat_tx_time"],"rows":[[{{"$":"uuid","v":"{task}"}},"closed",{{"$":"uuid","v":"{txn}"}},9]]}}]}}"#
    );
    let record = BranchRecord {
        payload: Some(payload.into_bytes()),
        ..BranchRecord::new("feed", gid_for(txn, "feed"), false)
    };
    db.store
        .participant()
        .finish(&record, txn, 9)
        .await
        .unwrap();
    assert_eq!(
        db.count("SELECT count() FROM activity WHERE event = 'closed'")
            .await,
        1
    );
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn overload_codes_are_retryable_and_schema_errors_are_not() {
    let db = db!();
    let err = db
        .store
        .client()
        .query("SELECT throwIf(1, 'too many parts', toInt32(252))")
        .with_setting("allow_custom_error_code_in_throwif", "1")
        .fetch_one::<u8>()
        .await
        .unwrap_err();
    assert!(is_retryable(&err), "{err}");
    let err = db
        .store
        .client()
        .query("SELECT count() FROM no_such_table")
        .fetch_one::<u64>()
        .await
        .unwrap_err();
    assert!(!is_retryable(&err), "{err}");
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn bytes_that_are_not_utf8_are_refused_at_stage() {
    let db = db!();
    db.ddl("CREATE TABLE blobs (b String, dat_txn UUID, dat_tx_time Int64) ENGINE = MergeTree ORDER BY b").await;
    let mut branch = db.store.branch();
    branch
        .insert("blobs", &["b"], vec![vec![RowValue::Bytes(vec![0, 0xff])]])
        .unwrap();
    let err = branch.stage(new_id(), 1).await.unwrap_err();
    assert!(err.to_string().contains("UTF-8"), "{err}");
    db.drop().await;
}

// --- an IT task manager: the task in PostgreSQL, its activity feed in ClickHouse --------

struct Tracker {
    db: Db,
    pg_dsn: String,
    schema: String,
    admin: tokio_postgres::Client,
    tasks: PostgresStore,
    journal: Arc<PostgresJournal>,
}

impl Tracker {
    async fn new() -> Option<Self> {
        let pg_dsn = dsn("DATORIUM_TEST_PG_DSN")?;
        let db = Db::new().await?;
        db.activity().await;
        let (admin, conn) = tokio_postgres::connect(&pg_dsn, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(conn);
        let schema = db.name.clone();
        admin
            .batch_execute(&format!(
                "CREATE SCHEMA {schema}; CREATE SCHEMA {schema}_tasks"
            ))
            .await
            .unwrap();
        let tasks = PostgresStore::new(
            &pg_dsn,
            &format!("tasks_{schema}"),
            Some(&format!("{schema}_tasks")),
        )
        .unwrap();
        tasks.install().await.unwrap();
        tasks
            .connect()
            .await
            .unwrap()
            .batch_execute("CREATE TABLE tasks (id uuid PRIMARY KEY)")
            .await
            .unwrap();
        let journal = Arc::new(
            PostgresJournal::open(&pg_dsn, Some(&schema), "dat_journal")
                .await
                .unwrap(),
        );
        Some(Self {
            db,
            pg_dsn,
            schema,
            admin,
            tasks,
            journal,
        })
    }

    fn participants(tasks: &PostgresStore, feed: &ClickHouseStore) -> Participants {
        Participants::new()
            .with(tasks.name(), tasks.participant() as Arc<dyn Participant>)
            .with(feed.name(), feed.participant() as Arc<dyn Participant>)
    }

    async fn open_task(&self) -> Vec<Box<dyn Branch>> {
        let task = new_id();
        let pg = self.tasks.branch().await.unwrap();
        pg.client()
            .unwrap()
            .execute("INSERT INTO tasks (id) VALUES ($1)", &[&task])
            .await
            .unwrap();
        let mut feed = self.db.store.branch();
        feed.insert("activity", &["task_id", "event"], events(task, &["opened"]))
            .unwrap();
        vec![Box::new(pg), Box::new(feed)]
    }

    async fn counts(&self) -> (i64, u64) {
        let sql = format!("SELECT count(*) FROM {}_tasks.tasks", self.schema);
        let tasks: i64 = self.admin.query_one(&sql, &[]).await.unwrap().get(0);
        (tasks, self.db.count("SELECT count() FROM activity").await)
    }

    async fn drop(self) {
        self.admin
            .batch_execute(&format!(
                "DROP SCHEMA {0} CASCADE; DROP SCHEMA {0}_tasks CASCADE",
                self.schema
            ))
            .await
            .unwrap();
        self.db.drop().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_task_and_its_activity_commit_together() {
    let Some(t) = Tracker::new().await else {
        return;
    };
    let coordinator = Coordinator::new(
        Some(t.journal.clone()),
        Tracker::participants(&t.tasks, &t.db.store),
        Arc::new(HybridLogicalClock::new()),
        Arc::new(TokioTimer::default()),
    );
    let txn = new_id();
    let outcome = coordinator
        .commit(txn, &mut t.open_task().await)
        .await
        .unwrap();
    assert!(matches!(outcome, Outcome::Committed { .. }), "{outcome:?}");
    assert_eq!(t.counts().await, (1, 1));
    assert_eq!(
        t.journal.get(txn).await.unwrap().unwrap().state,
        State::Done
    );
    t.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn activity_left_unapplied_by_a_crash_is_inserted_by_recovery() {
    let Some(t) = Tracker::new().await else {
        return;
    };
    let txn = new_id();
    let mut branches = t.open_task().await;
    // The protocol by hand up to the decision, then the process "dies".
    let mut records = Vec::new();
    for b in &mut branches {
        let payload = b.stage(txn, 3).await.unwrap();
        records.push(BranchRecord {
            payload,
            ..BranchRecord::new(b.store(), gid_for(txn, b.store()), b.two_phase())
        });
    }
    t.journal.begin(txn, &records).await.unwrap();
    let gid = gid_for(txn, branches[0].store());
    branches[0].prepare(&gid).await.unwrap();
    t.journal
        .transition(txn, State::Active, State::Prepared, Some(3))
        .await
        .unwrap();
    t.journal
        .transition(txn, State::Prepared, State::Committed, None)
        .await
        .unwrap();
    drop(branches);
    assert_eq!(t.counts().await, (0, 0));

    // Recovery with its own connections.
    let tasks = PostgresStore::new(
        &t.pg_dsn,
        t.tasks.name(),
        Some(&format!("{}_tasks", t.schema)),
    )
    .unwrap();
    let journal = Arc::new(
        PostgresJournal::open(&t.pg_dsn, Some(&t.schema), "dat_journal")
            .await
            .unwrap(),
    );
    let later = datorium_core::identity::system_ms() + 60_000;
    let recovery = Recovery::new(journal, Tracker::participants(&tasks, &t.db.store), 30_000)
        .unwrap()
        .with_wall(move || later);
    assert_eq!(recovery.run_once(100).await.unwrap().finished, [txn]);
    assert_eq!(t.counts().await, (1, 1));
    assert_eq!(
        recovery.run_once(100).await.unwrap().finished,
        Vec::<Uuid>::new()
    );
    assert_eq!(t.counts().await, (1, 1));
    t.drop().await;
}

#[test]
fn payload_is_the_shared_row_format() {
    let batch = Batch {
        table: "activity".into(),
        columns: vec!["event".into()],
        rows: vec![vec![RowValue::Str("x".into())]],
    };
    let raw = String::from_utf8(encode_batches(&[batch]).unwrap()).unwrap();
    assert!(
        raw.contains(r#""v":1"#) && raw.contains(r#""table":"activity""#),
        "{raw}"
    );
}
