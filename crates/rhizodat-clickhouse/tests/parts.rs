//! Entity parts in ClickHouse (`rhizodat_clickhouse::parts`) against the compose
//! server: the Python reference's part tables, versions carried in the journal
//! payload and applied after the decision, committed reads as of a time.
//!
//! The DSN comes from `RHIZODAT_TEST_CLICKHOUSE_DSN`, else from `.env.test`
//! (skipped with a hint when that server is not running). Each test works in
//! its own database, dropped afterwards.

use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use clickhouse::Client;
use rhizodat_clickhouse::ClickHouseStore;
use rhizodat_clickhouse::parts::{ChColumn, ClickHousePartSpec, ClickHouseParts, PartVersion};
use rhizodat_core::identity::IdGenerator;
use rhizodat_core::rows::{RowValue, decode_batches};
use rhizodat_core::store::Branch;
use uuid::Uuid;

fn server() -> Option<String> {
    if let Ok(dsn) = std::env::var("RHIZODAT_TEST_CLICKHOUSE_DSN") {
        return Some(dsn);
    }
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.env.test");
    let dsn = std::fs::read_to_string(path)
        .ok()?
        .lines()
        .filter(|l| !l.starts_with('#'))
        .find_map(|l| l.strip_prefix("RHIZODAT_TEST_CLICKHOUSE_DSN="))
        .map(str::to_owned)?;
    let parsed = rhizodat_clickhouse::ADAPTER.parse(&dsn).ok()?;
    let up = (
        parsed.host().unwrap_or("127.0.0.1"),
        parsed.port().unwrap_or(8123),
    )
        .to_socket_addrs()
        .ok()
        .and_then(|mut a| a.next())
        .is_some_and(|a| TcpStream::connect_timeout(&a, Duration::from_millis(500)).is_ok());
    if up {
        Some(dsn)
    } else {
        eprintln!(
            "skipped: ClickHouse from .env.test is not running (docker compose up -d --wait)"
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
        let server = server()?;
        let parsed = rhizodat_clickhouse::ADAPTER.parse(&server).unwrap();
        let name = format!("dat_parts_{}", &new_id().simple().to_string()[20..]);
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
        let store = ClickHouseStore::new(&format!("{base}/{name}"), "lake").unwrap();
        store.install().await.unwrap();
        let admin = admin.with_database(name.clone());
        Some(Self { name, admin, store })
    }

    /// Commit `parts` in one branch at `ts` (stage, then apply one-phase).
    async fn commit(&self, parts: ClickHouseParts, ts: i64) -> Uuid {
        let txn = new_id();
        let mut branch = self.store.branch();
        branch.load_parts(parts);
        branch.stage(txn, ts).await.unwrap();
        branch.commit_one_phase().await.unwrap();
        txn
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

fn spec(columns: &[(&str, ChColumn)]) -> ClickHousePartSpec {
    ClickHousePartSpec::new(
        "tracker.Issue",
        "tracker_issue",
        columns.iter().map(|(n, t)| ((*n).to_owned(), *t)).collect(),
    )
    .unwrap()
}

fn issue() -> ClickHousePartSpec {
    spec(&[("title", ChColumn::String), ("points", ChColumn::Int64)])
}

fn text(v: &str) -> RowValue {
    RowValue::Str(v.to_owned())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_part_table_is_the_python_references_table() {
    let db = db!();
    let s = spec(&[
        ("title", ChColumn::String),
        ("done", ChColumn::Bool),
        ("labels", ChColumn::Json),
        ("at", ChColumn::DateTime),
        ("day", ChColumn::Date),
        ("owner", ChColumn::Uuid),
        ("weight", ChColumn::Float64),
    ]);
    db.store.install_part(&s).await.unwrap();
    db.store.install_part(&s).await.unwrap(); // idempotent
    let columns: Vec<(String, String)> = db
        .admin
        .query("SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'tracker_issue' ORDER BY position")
        .fetch_all()
        .await
        .unwrap();
    let expected = [
        ("dat_id", "UUID"),
        ("dat_created", "Int64"),
        ("dat_deleted", "Bool"),
        ("title", "Nullable(String)"),
        ("done", "Nullable(Bool)"),
        ("labels", "Nullable(String)"),
        ("at", "Nullable(DateTime64(6, 'UTC'))"),
        ("day", "Nullable(Date32)"),
        ("owner", "Nullable(UUID)"),
        ("weight", "Nullable(Float64)"),
        ("dat_txn", "UUID"),
        ("dat_tx_time", "Int64"),
    ];
    let expected: Vec<(String, String)> = expected
        .iter()
        .map(|(a, b)| ((*a).to_owned(), (*b).to_owned()))
        .collect();
    assert_eq!(columns, expected);
    let key: String = db
        .admin
        .query("SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 'tracker_issue'")
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(key, "dat_id, dat_tx_time");
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn versions_ride_the_payload_and_read_as_of_their_time() {
    let db = db!();
    let s = issue();
    db.store.install_part(&s).await.unwrap();
    let id = new_id();
    let mut first = ClickHouseParts::new();
    first
        .write(&s, id, &[("title".into(), text("draft"))], None, false)
        .unwrap();
    // The versions travel in the journal payload, as row batches.
    let mut branch = db.store.branch();
    branch.load_parts(first.clone());
    let payload = branch.stage(new_id(), 1).await.unwrap().unwrap();
    let batches = decode_batches(&payload).unwrap();
    assert_eq!(batches[0].table, "tracker_issue");
    assert_eq!(
        batches[0].columns,
        [
            "dat_id",
            "dat_created",
            "dat_deleted",
            "title",
            "points",
            "dat_txn",
            "dat_tx_time"
        ]
    );
    branch.rollback().await.unwrap();

    let t1 = db.commit(first, 100).await;
    let mut second = ClickHouseParts::new();
    second
        .write(
            &s,
            id,
            &[
                ("title".into(), text("final")),
                ("points".into(), RowValue::Int(5)),
            ],
            Some(100),
            false,
        )
        .unwrap();
    let t2 = db.commit(second, 200).await;
    let v1 = PartVersion {
        dat_txn: t1,
        dat_tx_time: Some(100),
        created: Some(100),
        deleted: false,
        values: vec![text("draft"), RowValue::Null],
    };
    let v2 = PartVersion {
        dat_txn: t2,
        dat_tx_time: Some(200),
        created: Some(100),
        deleted: false,
        values: vec![text("final"), RowValue::Int(5)],
    };
    assert_eq!(
        db.store.read_part(&s, id, None).await.unwrap(),
        Some(v2.clone())
    );
    assert_eq!(
        db.store.read_part(&s, id, Some(199)).await.unwrap(),
        Some(v1.clone())
    );
    assert_eq!(db.store.read_part(&s, id, Some(99)).await.unwrap(), None);
    assert_eq!(
        db.store.part_history(&s, id).await.unwrap(),
        vec![v1.clone(), v2.clone()]
    );
    assert_eq!(
        db.store.part_versions(&s, Some(150)).await.unwrap(),
        vec![(id, v1)]
    );
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn every_column_type_round_trips() {
    let db = db!();
    let s = spec(&[
        ("name", ChColumn::String),
        ("done", ChColumn::Bool),
        ("qty", ChColumn::Int64),
        ("weight", ChColumn::Float64),
        ("inf", ChColumn::Float64),
        ("at", ChColumn::DateTime),
        ("day", ChColumn::Date),
        ("owner", ChColumn::Uuid),
        ("tags", ChColumn::Json),
        ("nothing", ChColumn::Json),
    ]);
    db.store.install_part(&s).await.unwrap();
    let (id, owner) = (new_id(), new_id());
    let values = vec![
        text("Ада Лавлейс"),
        RowValue::Bool(true),
        RowValue::Int(i64::MIN),
        RowValue::Float(0.1),
        RowValue::Float(f64::NEG_INFINITY),
        RowValue::DateTime("2026-10-31T13:14:15.123456+00:00".into()),
        RowValue::Date("2026-10-31".into()),
        RowValue::Uuid(owner),
        RowValue::Map(vec![
            ("price".into(), RowValue::Decimal("12.3400".into())),
            ("raw".into(), RowValue::Bytes(vec![0, 255])),
            ("ids".into(), RowValue::List(vec![RowValue::Uuid(owner)])),
        ]),
        RowValue::Null,
    ];
    let named: Vec<(String, RowValue)> = s
        .fields()
        .map(str::to_owned)
        .zip(values.iter().cloned())
        .collect();
    let mut parts = ClickHouseParts::new();
    parts.write(&s, id, &named, None, false).unwrap();
    db.commit(parts, 7).await;
    let read = db.store.read_part(&s, id, None).await.unwrap().unwrap();
    assert_eq!(read.values, values);
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn erase_removes_older_versions_and_soft_deletes_expire() {
    let db = db!();
    let s = issue();
    db.store.install_part(&s).await.unwrap();
    let (kept, gone, again) = (new_id(), new_id(), new_id());
    let mut parts = ClickHouseParts::new();
    for id in [kept, gone, again] {
        parts
            .write(&s, id, &[("title".into(), text("x"))], None, false)
            .unwrap();
    }
    db.commit(parts, 10).await;
    let mut parts = ClickHouseParts::new();
    parts.write(&s, gone, &[], Some(10), true).unwrap();
    db.commit(parts, 20).await;
    assert_eq!(
        db.store.expired_parts(&s, 21, 10).await.unwrap(),
        vec![gone]
    );
    assert_eq!(
        db.store.expired_parts(&s, 20, 10).await.unwrap(),
        Vec::<Uuid>::new()
    );

    // One Unit of Work erases `gone` and erases then rewrites `again`.
    let mut parts = ClickHouseParts::new();
    parts.erase(&s, gone);
    parts.erase(&s, again);
    parts
        .write(&s, again, &[("title".into(), text("anew"))], None, false)
        .unwrap();
    assert_eq!(parts.latest("tracker_issue", gone), Some(None));
    assert_eq!(
        parts
            .latest("tracker_issue", again)
            .unwrap()
            .unwrap()
            .values,
        vec![text("anew"), RowValue::Null]
    );
    db.commit(parts, 30).await;
    assert_eq!(
        db.store.part_history(&s, gone).await.unwrap(),
        Vec::<PartVersion>::new()
    );
    let history = db.store.part_history(&s, again).await.unwrap();
    assert_eq!(history.len(), 1, "only the rewrite is left");
    assert_eq!(history[0].values[0], text("anew"));
    assert_eq!(db.store.part_history(&s, kept).await.unwrap().len(), 1);
    db.drop().await;
}
