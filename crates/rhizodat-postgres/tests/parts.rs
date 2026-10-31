//! Entity parts in PostgreSQL (`rhizodat_postgres::parts`) against a real
//! server: the same tables, columns and encoding as the Python reference's
//! `PostgresStore` parts, so either implementation reads the other's versions.
//!
//! The DSN comes from `RHIZODAT_TEST_PG_DSN`, else from `.env.test` (skipped
//! with a hint when that server is not running). Each test works in its own
//! schema, dropped afterwards.

use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use rhizodat_core::identity::IdGenerator;
use rhizodat_core::rows::RowValue;
use rhizodat_core::store::Branch;
use rhizodat_postgres::PostgresStore;
use rhizodat_postgres::parts::{ColumnType, PartSpec, PartVersion, PostgresParts};
use uuid::Uuid;

fn pg_dsn() -> Option<String> {
    if let Ok(dsn) = std::env::var("RHIZODAT_TEST_PG_DSN") {
        return Some(dsn);
    }
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.env.test");
    let dsn = std::fs::read_to_string(path)
        .ok()?
        .lines()
        .filter(|l| !l.starts_with('#'))
        .find_map(|l| l.strip_prefix("RHIZODAT_TEST_PG_DSN="))
        .map(str::to_owned)?;
    let config: tokio_postgres::Config = dsn.parse().ok()?;
    let host = match config.get_hosts().first() {
        Some(tokio_postgres::config::Host::Tcp(h)) => h.clone(),
        _ => return Some(dsn),
    };
    let port = config.get_ports().first().copied().unwrap_or(5432);
    let up = (host.as_str(), port)
        .to_socket_addrs()
        .ok()
        .and_then(|mut a| a.next())
        .is_some_and(|a| TcpStream::connect_timeout(&a, Duration::from_millis(500)).is_ok());
    if up {
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
    schema: String,
    admin: tokio_postgres::Client,
    store: PostgresStore,
}

impl Db {
    async fn new() -> Option<Self> {
        let dsn = pg_dsn()?;
        let schema = format!("dat_parts_{}", &new_id().simple().to_string()[20..]);
        let (admin, conn) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("PostgreSQL is up");
        tokio::spawn(conn);
        admin
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .unwrap();
        let store = PostgresStore::new(&dsn, "pg", Some(&schema)).unwrap();
        store.install().await.unwrap();
        Some(Self {
            schema,
            admin,
            store,
        })
    }

    /// Commit `parts` in one branch at `dat_tx_time`, one-phase.
    async fn commit(&self, parts: PostgresParts, dat_tx_time: i64) -> Uuid {
        let txn = new_id();
        let mut branch = self.store.branch().await.unwrap();
        branch.load_parts(parts);
        branch.stage(txn, dat_tx_time).await.unwrap();
        branch.commit_one_phase().await.unwrap();
        txn
    }

    async fn drop(self) {
        self.admin
            .batch_execute(&format!("DROP SCHEMA {} CASCADE", self.schema))
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

fn issue() -> PartSpec {
    PartSpec::new(
        "tracker.Issue",
        "tracker_issue",
        vec![
            ("title".into(), ColumnType::Text),
            ("points".into(), ColumnType::Bigint),
        ],
    )
    .unwrap()
}

fn text(v: &str) -> RowValue {
    RowValue::Str(v.to_owned())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_part_table_is_the_python_references_table() {
    let db = db!();
    let spec = PartSpec::new(
        "erp.Item",
        "erp_item",
        [
            ("name", ColumnType::Text),
            ("active", ColumnType::Boolean),
            ("qty", ColumnType::Bigint),
            ("weight", ColumnType::Double),
            ("price", ColumnType::Numeric),
            ("at", ColumnType::Timestamptz),
            ("day", ColumnType::Date),
            ("owner", ColumnType::Uuid),
            ("blob", ColumnType::Bytea),
            ("tags", ColumnType::Jsonb),
        ]
        .into_iter()
        .map(|(n, t)| (n.to_owned(), t))
        .collect(),
    )
    .unwrap();
    db.store.install_part(&spec).await.unwrap();
    db.store.install_part(&spec).await.unwrap(); // idempotent
    let rows = db
        .admin
        .query(
            "SELECT column_name::text, data_type::text, is_nullable::text \
             FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = 'erp_item' ORDER BY ordinal_position",
            &[&db.schema],
        )
        .await
        .unwrap();
    let columns: Vec<(String, String, String)> = rows
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    let expected = [
        ("dat_id", "uuid", "NO"),
        ("dat_txn", "uuid", "NO"),
        ("dat_tx_time", "bigint", "YES"),
        ("dat_created", "bigint", "YES"),
        ("dat_deleted", "boolean", "NO"),
        ("name", "text", "YES"),
        ("active", "boolean", "YES"),
        ("qty", "bigint", "YES"),
        ("weight", "double precision", "YES"),
        ("price", "numeric", "YES"),
        ("at", "timestamp with time zone", "YES"),
        ("day", "date", "YES"),
        ("owner", "uuid", "YES"),
        ("blob", "bytea", "YES"),
        ("tags", "jsonb", "YES"),
    ];
    let expected: Vec<(String, String, String)> = expected
        .iter()
        .map(|(a, b, c)| ((*a).to_owned(), (*b).to_owned(), (*c).to_owned()))
        .collect();
    assert_eq!(columns, expected);
    let index = db
        .admin
        .query_one(
            "SELECT indexdef FROM pg_indexes WHERE schemaname = $1 AND indexname = 'erp_item_dat_time'",
            &[&db.schema],
        )
        .await
        .unwrap();
    assert!(index.get::<_, String>(0).contains("(dat_id, dat_tx_time)"));
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn versions_commit_with_the_branch_and_read_as_of_their_time() {
    let db = db!();
    let spec = issue();
    db.store.install_part(&spec).await.unwrap();
    let id = new_id();
    let mut first = PostgresParts::new();
    first
        .write(&spec, id, &[("title".into(), text("draft"))], None, false)
        .unwrap();
    let t1 = db.commit(first, 100).await;
    let mut second = PostgresParts::new();
    second
        .write(
            &spec,
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
        db.store.read_part(&spec, id, None).await.unwrap(),
        Some(v2.clone())
    );
    assert_eq!(
        db.store.read_part(&spec, id, Some(199)).await.unwrap(),
        Some(v1.clone())
    );
    assert_eq!(db.store.read_part(&spec, id, Some(99)).await.unwrap(), None);
    assert_eq!(
        db.store.read_part(&spec, new_id(), None).await.unwrap(),
        None
    );
    assert_eq!(
        db.store.part_history(&spec, id).await.unwrap(),
        vec![v1.clone(), v2.clone()]
    );
    assert_eq!(
        db.store.part_versions(&spec, Some(150)).await.unwrap(),
        vec![(id, v1.clone())]
    );
    assert_eq!(
        db.store.part_versions(&spec, None).await.unwrap(),
        vec![(id, v1), (id, v2)]
    );
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn every_column_type_round_trips() {
    let db = db!();
    let spec = PartSpec::new(
        "erp.Item",
        "erp_item",
        [
            ("name", ColumnType::Text),
            ("active", ColumnType::Boolean),
            ("qty", ColumnType::Bigint),
            ("weight", ColumnType::Double),
            ("inf", ColumnType::Double),
            ("price", ColumnType::Numeric),
            ("at", ColumnType::Timestamptz),
            ("day", ColumnType::Date),
            ("owner", ColumnType::Uuid),
            ("blob", ColumnType::Bytea),
            ("tags", ColumnType::Jsonb),
            ("nothing", ColumnType::Jsonb),
        ]
        .into_iter()
        .map(|(n, t)| (n.to_owned(), t))
        .collect(),
    )
    .unwrap();
    db.store.install_part(&spec).await.unwrap();
    let id = new_id();
    let owner = new_id();
    let tags = RowValue::Map(vec![
        (
            "list".into(),
            RowValue::List(vec![RowValue::Int(1), text("a")]),
        ),
        ("id".into(), RowValue::Uuid(owner)),
        ("price".into(), RowValue::Decimal("12.3400".into())),
    ]);
    let values = vec![
        text("Ада Лавлейс"),
        RowValue::Bool(true),
        RowValue::Int(i64::MIN),
        RowValue::Float(0.1),
        RowValue::Float(f64::NEG_INFINITY),
        RowValue::Decimal("12.3400".into()),
        RowValue::DateTime("2026-10-31T13:14:15.123456+00:00".into()),
        RowValue::Date("2026-10-31".into()),
        RowValue::Uuid(owner),
        RowValue::Bytes(vec![0, 1, 254, 255]),
        tags,
        RowValue::Null,
    ];
    let named: Vec<(String, RowValue)> = spec
        .fields()
        .map(str::to_owned)
        .zip(values.iter().cloned())
        .collect();
    let mut parts = PostgresParts::new();
    parts.write(&spec, id, &named, None, false).unwrap();
    db.commit(parts, 7).await;
    let read = db.store.read_part(&spec, id, None).await.unwrap().unwrap();
    let mut expected = values;
    // A time with zone reads back in the session's zone: the compose server's is UTC.
    expected[6] = RowValue::DateTime("2026-10-31T13:14:15.123456+00:00".into());
    assert_eq!(read.values, expected);
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn erase_removes_every_version_and_soft_deletes_expire() {
    let db = db!();
    let spec = issue();
    db.store.install_part(&spec).await.unwrap();
    let (kept, gone, erased) = (new_id(), new_id(), new_id());
    let mut parts = PostgresParts::new();
    for id in [kept, gone, erased] {
        parts
            .write(&spec, id, &[("title".into(), text("x"))], None, false)
            .unwrap();
    }
    db.commit(parts, 10).await;
    let mut parts = PostgresParts::new();
    parts.write(&spec, gone, &[], Some(10), true).unwrap();
    parts.write(&spec, erased, &[], Some(10), true).unwrap();
    db.commit(parts, 20).await;

    assert_eq!(
        db.store.expired_parts(&spec, 21, 10).await.unwrap().len(),
        2
    );
    assert_eq!(
        db.store.expired_parts(&spec, 20, 10).await.unwrap(),
        Vec::<Uuid>::new()
    );
    assert_eq!(db.store.expired_parts(&spec, 21, 1).await.unwrap().len(), 1);

    let mut parts = PostgresParts::new();
    parts.erase(&spec, erased);
    db.commit(parts, 30).await;
    assert_eq!(
        db.store.part_history(&spec, erased).await.unwrap(),
        Vec::<PartVersion>::new()
    );
    assert_eq!(
        db.store.expired_parts(&spec, 21, 10).await.unwrap(),
        vec![gone]
    );
    assert!(
        db.store
            .read_part(&spec, gone, None)
            .await
            .unwrap()
            .unwrap()
            .deleted
    );
    assert_eq!(db.store.part_history(&spec, kept).await.unwrap().len(), 1);
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rolled_back_branch_leaves_no_version() {
    let db = db!();
    let spec = issue();
    db.store.install_part(&spec).await.unwrap();
    let id = new_id();
    let mut parts = PostgresParts::new();
    parts
        .write(&spec, id, &[("title".into(), text("x"))], None, false)
        .unwrap();
    let mut branch = db.store.branch().await.unwrap();
    branch.load_parts(parts);
    branch.stage(new_id(), 5).await.unwrap();
    branch.rollback().await.unwrap();
    assert_eq!(
        db.store.part_history(&spec, id).await.unwrap(),
        Vec::<PartVersion>::new()
    );
    db.drop().await;
}
