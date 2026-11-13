//! A unit of work of native stores against the compose PostgreSQL: one store
//! commits in one phase, two commit through the journal, and a store that
//! cannot be reached aborts before anything is written.
//!
//! The DSN comes from `RHIZODAT_TEST_PG_DSN`, else from `.env.test` (skipped
//! with a hint when nothing listens there).

use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use rhizodat_core::clock::HybridLogicalClock;
use rhizodat_core::coordinator::Outcome;
use rhizodat_core::identity::IdGenerator;
use rhizodat_core::journal::Journal;
use rhizodat_core::rows::RowValue;
use rhizodat_postgres::parts::PostgresParts;
use rhizodat_postgres::{PostgresJournal, PostgresRows, PostgresStore};
use rhizodat_units::{NativeStore, PostgresPending, UnitError, commit, runtime};
use uuid::Uuid;

fn pg_dsn() -> Option<String> {
    if let Ok(dsn) = std::env::var("RHIZODAT_TEST_PG_DSN") {
        return Some(dsn);
    }
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.env.test");
    let dsn = std::fs::read_to_string(path)
        .ok()?
        .lines()
        .find_map(|l| l.strip_prefix("RHIZODAT_TEST_PG_DSN="))?
        .to_owned();
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

/// A schema per store, `units_<base>_<name>`, with an `events` table; dropped after.
struct Schemas {
    dsn: String,
    base: String,
}

impl Schemas {
    fn new(dsn: &str) -> Self {
        let id = IdGenerator::new().generate().unwrap().simple().to_string();
        Self {
            dsn: dsn.to_owned(),
            base: format!("units_{}", &id[20..]),
        }
    }

    fn of(&self, name: &str) -> String {
        format!("{}_{name}", self.base)
    }

    async fn store(&self, name: &str) -> Arc<PostgresStore> {
        let schema = self.of(name);
        let store = PostgresStore::new(&self.dsn, &format!("{name}_{}", self.base), None).unwrap();
        store
            .run_sql(&[
                format!("CREATE SCHEMA {schema}"),
                format!(
                    "CREATE TABLE {schema}.events (host text, value bigint, dat_txn uuid, dat_tx_time bigint)"
                ),
            ])
            .await
            .unwrap();
        let store =
            PostgresStore::new(&self.dsn, &format!("{name}_{}", self.base), Some(&schema)).unwrap();
        store.install().await.unwrap();
        Arc::new(store)
    }

    async fn count(&self, name: &str) -> i64 {
        let store = PostgresStore::new(&self.dsn, "count", None).unwrap();
        let client = store.connect().await.unwrap();
        client
            .query_one(
                &format!("SELECT count(*) FROM {}.events", self.of(name)),
                &[],
            )
            .await
            .unwrap()
            .get(0)
    }

    async fn drop_all(&self) {
        let store = PostgresStore::new(&self.dsn, "cleanup", None).unwrap();
        let client = store.connect().await.unwrap();
        let rows = client
            .query(
                "SELECT nspname FROM pg_namespace WHERE nspname LIKE $1",
                &[&format!("{}%", self.base)],
            )
            .await
            .unwrap();
        for row in rows {
            let schema: String = row.get(0);
            client
                .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
                .await
                .unwrap();
        }
    }
}

fn pending(store: &Arc<PostgresStore>, host: &str, n: i64) -> Arc<dyn NativeStore> {
    let mut rows = PostgresRows::new();
    rows.insert(
        "events",
        &["host", "value"],
        (0..n)
            .map(|v| vec![RowValue::Str(host.to_owned()), RowValue::Int(v)])
            .collect(),
    )
    .unwrap();
    Arc::new(PostgresPending {
        store: store.clone(),
        rows,
        parts: PostgresParts::default(),
    })
}

fn txn() -> Uuid {
    IdGenerator::new().generate().unwrap()
}

#[test]
fn one_store_commits_its_rows() {
    let Some(dsn) = pg_dsn() else { return };
    let rt = runtime().unwrap();
    rt.block_on(async {
        let schemas = Schemas::new(&dsn);
        let inventory = schemas.store("inventory").await;
        let outcome = commit(
            None,
            Arc::new(HybridLogicalClock::new()),
            txn(),
            &[pending(&inventory, "web1", 3)],
        )
        .await;
        let count = schemas.count("inventory").await;
        schemas.drop_all().await;
        assert!(
            matches!(outcome, Ok(Outcome::Committed { .. })),
            "{outcome:?}"
        );
        assert_eq!(count, 3);
    });
}

#[test]
fn two_stores_commit_together_through_the_journal() {
    let Some(dsn) = pg_dsn() else { return };
    let rt = runtime().unwrap();
    rt.block_on(async {
        let schemas = Schemas::new(&dsn);
        let inventory = schemas.store("inventory").await;
        let billing = schemas.store("billing").await;
        let journal: Arc<dyn Journal> = Arc::new(
            PostgresJournal::open(&dsn, Some(&schemas.of("journal")), "dat_journal")
                .await
                .unwrap(),
        );
        let dat_txn = txn();
        let outcome = commit(
            Some(journal.clone()),
            Arc::new(HybridLogicalClock::new()),
            dat_txn,
            &[pending(&inventory, "web1", 2), pending(&billing, "web1", 5)],
        )
        .await;
        let counts = (
            schemas.count("inventory").await,
            schemas.count("billing").await,
        );
        let record = journal.get(dat_txn).await.unwrap();
        schemas.drop_all().await;
        assert!(
            matches!(outcome, Ok(Outcome::Committed { .. })),
            "{outcome:?}"
        );
        assert_eq!(counts, (2, 5));
        assert!(record.is_some(), "a two-store commit is journaled");
    });
}

#[test]
fn a_store_that_cannot_be_reached_aborts_before_writing() {
    let Some(dsn) = pg_dsn() else { return };
    let rt = runtime().unwrap();
    rt.block_on(async {
        let schemas = Schemas::new(&dsn);
        let inventory = schemas.store("inventory").await;
        let down = Arc::new(
            PostgresStore::new(
                "postgresql://postgres:x@127.0.0.1:1/none?connect_timeout=2",
                "down",
                None,
            )
            .unwrap(),
        );
        let outcome = commit(
            None,
            Arc::new(HybridLogicalClock::new()),
            txn(),
            &[pending(&inventory, "web1", 1), pending(&down, "web1", 1)],
        )
        .await;
        let count = schemas.count("inventory").await;
        schemas.drop_all().await;
        assert!(matches!(outcome, Err(UnitError::Connect(_))), "{outcome:?}");
        assert_eq!(count, 0, "nothing is written when a branch cannot open");
    });
}
