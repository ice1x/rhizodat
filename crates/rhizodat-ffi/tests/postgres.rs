//! PostgreSQL through the C ABI against the compose server: a store's own SQL,
//! a unit of work over one store and over two through the journal, a store that
//! is down, and the refusals. The DSN comes from `RHIZODAT_TEST_PG_DSN`, else
//! `.env.test` (skipped with a hint when nothing listens).

use std::ffi::{CStr, CString, c_char};
use std::net::{TcpStream, ToSocketAddrs};
use std::ptr;
use std::time::Duration;

use rhizodat_ffi::*;
use serde_json::{Value, json};

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
    let up = ("127.0.0.1", 55440)
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

fn c(text: &str) -> CString {
    CString::new(text).unwrap()
}

fn take(raw: *mut c_char) -> String {
    assert!(!raw.is_null());
    // SAFETY: a string from the library, freed once.
    let text = unsafe { CStr::from_ptr(raw) }.to_str().unwrap().to_owned();
    // SAFETY: as above.
    unsafe { rhizodat_string_free(raw) };
    text
}

/// A call's status and its error text (empty when none).
fn run(call: impl FnOnce(*mut *mut c_char) -> RhizodatStatus) -> (RhizodatStatus, String) {
    let mut error: *mut c_char = ptr::null_mut();
    let status = call(&raw mut error);
    let message = if error.is_null() {
        String::new()
    } else {
        take(error)
    };
    (status, message)
}

fn ok(call: impl FnOnce(*mut *mut c_char) -> RhizodatStatus) {
    let (status, message) = run(call);
    assert_eq!(status, RhizodatStatus::Ok, "{message}");
}

/// A schema per test: `ffi_<suffix>`, holding the stores' tables (each store
/// gets its own `ffi_<suffix>_<name>` for its marker table) and the journal.
struct Db {
    dsn: String,
    base: String,
    admin: *mut RhizodatPostgresStore,
}

impl Db {
    fn new(dsn: &str) -> Self {
        let mut id = ptr::null_mut();
        // SAFETY: writable locals.
        ok(|e| unsafe { rhizodat_new_id(&raw mut id, e) });
        let id = take(id).replace('-', "");
        let mut admin = ptr::null_mut();
        let (d, n) = (c(dsn), c("admin"));
        // SAFETY: valid strings and writable locals.
        ok(|e| unsafe {
            rhizodat_postgres_store_open(d.as_ptr(), n.as_ptr(), ptr::null(), &raw mut admin, e)
        });
        Self {
            dsn: dsn.to_owned(),
            base: format!("ffi_{}", &id[20..]),
            admin,
        }
    }

    fn sql(&self, sql: &str, params: Option<Value>) -> Value {
        let sql = c(sql);
        let params = params.map(|p| c(&p.to_string()));
        let mut out = ptr::null_mut();
        // SAFETY: a live handle, valid strings, writable locals.
        ok(|e| unsafe {
            rhizodat_postgres_store_execute(
                self.admin,
                sql.as_ptr(),
                params.as_ref().map_or(ptr::null(), |p| p.as_ptr()),
                &raw mut out,
                e,
            )
        });
        serde_json::from_str(&take(out)).unwrap()
    }

    /// A store `name` with an `events` table in its own schema.
    fn store(&self, name: &str) -> *mut RhizodatPostgresStore {
        let schema = format!("{}_{name}", self.base);
        self.sql(&format!("CREATE SCHEMA {schema}"), None);
        self.sql(
            &format!("CREATE TABLE {schema}.events (host text, value bigint, dat_txn uuid, dat_tx_time bigint)"),
            None,
        );
        let mut store = ptr::null_mut();
        let (d, n, s) = (
            c(&self.dsn),
            c(&format!("{name}_{}", self.base)),
            c(&schema),
        );
        // SAFETY: valid strings and writable locals.
        ok(|e| unsafe {
            rhizodat_postgres_store_open(d.as_ptr(), n.as_ptr(), s.as_ptr(), &raw mut store, e)
        });
        // SAFETY: a live handle.
        ok(|e| unsafe { rhizodat_postgres_store_install(store, e) });
        store
    }

    /// A journal in its own schema (created first: a missing schema would put
    /// the table in `public` through the search path).
    fn journal(&self) -> *mut RhizodatJournal {
        let schema = format!("{}_journal", self.base);
        self.sql(&format!("CREATE SCHEMA {schema}"), None);
        let mut journal = ptr::null_mut();
        let (d, s) = (c(&self.dsn), c(&schema));
        // SAFETY: valid strings and a writable local.
        ok(|e| unsafe {
            rhizodat_postgres_journal_open(d.as_ptr(), s.as_ptr(), &raw mut journal, e)
        });
        journal
    }

    fn count(&self, name: &str) -> i64 {
        let rows = self.sql(
            &format!("SELECT count(*) AS n FROM {}_{name}.events", self.base),
            None,
        );
        rows["rows"][0][0].as_i64().unwrap()
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        let schemas = self.sql(
            "SELECT nspname FROM pg_namespace WHERE nspname LIKE $1",
            Some(json!([format!("{}%", self.base)])),
        );
        for row in schemas["rows"].as_array().unwrap() {
            self.sql(
                &format!("DROP SCHEMA {} CASCADE", row[0].as_str().unwrap()),
                None,
            );
        }
        // SAFETY: freed once.
        unsafe { rhizodat_postgres_store_free(self.admin) };
    }
}

fn unit_with(store: *mut RhizodatPostgresStore, host: &str, n: i64) -> *mut RhizodatUnit {
    let mut unit = ptr::null_mut();
    // SAFETY: a writable local.
    ok(|e| unsafe { rhizodat_unit_new(&raw mut unit, e) });
    insert(unit, store, host, n);
    unit
}

fn insert(unit: *mut RhizodatUnit, store: *mut RhizodatPostgresStore, host: &str, n: i64) {
    let rows: Vec<Value> = (0..n).map(|v| json!([host, v])).collect();
    let (t, cols, rows) = (
        c("events"),
        c(r#"["host","value"]"#),
        c(&Value::from(rows).to_string()),
    );
    // SAFETY: live handles and valid strings.
    ok(|e| unsafe {
        rhizodat_unit_postgres_insert(unit, store, t.as_ptr(), cols.as_ptr(), rows.as_ptr(), e)
    });
}

fn coordinator(journal: *const RhizodatJournal) -> *mut RhizodatCoordinator {
    let mut out = ptr::null_mut();
    // SAFETY: a live journal or null, a writable local.
    ok(|e| unsafe { rhizodat_coordinator_new(journal, &raw mut out, e) });
    out
}

fn commit(
    coordinator: *mut RhizodatCoordinator,
    unit: *mut RhizodatUnit,
) -> (RhizodatStatus, Value) {
    let mut out = ptr::null_mut();
    // SAFETY: live handles and a writable local.
    let (status, _) =
        run(|e| unsafe { rhizodat_coordinator_commit(coordinator, unit, &raw mut out, e) });
    (status, serde_json::from_str(&take(out)).unwrap())
}

#[test]
fn execute_binds_tagged_values_and_returns_them_tagged() {
    let Some(dsn) = pg_dsn() else { return };
    let db = Db::new(&dsn);
    let result = db.sql(
        "SELECT $1::uuid AS id, $2::numeric AS price, $3::text AS name, $4::bigint AS n, NULL::int AS gone",
        Some(json!([
            {"$": "uuid", "v": "01920000-0000-7000-8000-000000000001"},
            {"$": "decimal", "v": "12.3400"},
            "web1",
            9_007_199_254_740_993_i64
        ])),
    );
    assert_eq!(
        result["columns"],
        json!(["id", "price", "name", "n", "gone"])
    );
    assert_eq!(
        result["rows"],
        json!([[
            {"$": "uuid", "v": "01920000-0000-7000-8000-000000000001"},
            {"$": "decimal", "v": "12.3400"},
            "web1",
            9_007_199_254_740_993_i64,
            null
        ]])
    );
}

#[test]
fn a_server_error_is_failed_with_the_servers_message() {
    let Some(dsn) = pg_dsn() else { return };
    let db = Db::new(&dsn);
    let sql = c("SELECT * FROM no_such_table");
    let mut out = ptr::null_mut();
    // SAFETY: a live handle, a valid string, writable locals.
    let (status, message) = run(|e| unsafe {
        rhizodat_postgres_store_execute(db.admin, sql.as_ptr(), ptr::null(), &raw mut out, e)
    });
    assert_eq!(status, RhizodatStatus::Failed);
    assert!(
        message.contains("42P01") && message.contains("no_such_table"),
        "{message}"
    );
    assert!(out.is_null(), "no result on failure");
}

#[test]
fn one_store_commits_in_one_phase() {
    let Some(dsn) = pg_dsn() else { return };
    let db = Db::new(&dsn);
    let store = db.store("inventory");
    let unit = unit_with(store, "web1", 3);
    let coordinator = coordinator(ptr::null());
    let (status, outcome) = commit(coordinator, unit);
    assert_eq!(status, RhizodatStatus::Ok, "{outcome}");
    assert_eq!(outcome["status"], "committed");
    assert!(outcome["dat_tx_time"].as_i64().unwrap() > 0);
    assert_eq!(db.count("inventory"), 3);
    let txns = db.sql(
        &format!(
            "SELECT DISTINCT dat_txn::text AS t FROM {}_inventory.events",
            db.base
        ),
        None,
    );
    assert_eq!(txns["rows"], json!([[outcome["dat_txn"]]]));
    // SAFETY: each freed once.
    unsafe {
        rhizodat_unit_free(unit);
        rhizodat_coordinator_free(coordinator);
        rhizodat_postgres_store_free(store);
    }
}

#[test]
fn two_stores_commit_together_through_the_journal() {
    let Some(dsn) = pg_dsn() else { return };
    let db = Db::new(&dsn);
    let (inventory, billing) = (db.store("inventory"), db.store("billing"));
    let journal = db.journal();
    let unit = unit_with(inventory, "web1", 2);
    insert(unit, billing, "web1", 5);
    let coordinator = coordinator(journal);
    // SAFETY: the coordinator keeps its own reference to the journal.
    unsafe { rhizodat_journal_free(journal) };
    let (status, outcome) = commit(coordinator, unit);
    assert_eq!(status, RhizodatStatus::Ok, "{outcome}");
    assert_eq!((db.count("inventory"), db.count("billing")), (2, 5));
    let journaled = db.sql(
        &format!(
            "SELECT state FROM {}_journal.dat_journal WHERE dat_txn = $1::uuid",
            db.base
        ),
        Some(json!([outcome["dat_txn"]])),
    );
    assert_eq!(journaled["rows"].as_array().unwrap().len(), 1);
    // SAFETY: each freed once.
    unsafe {
        rhizodat_unit_free(unit);
        rhizodat_coordinator_free(coordinator);
        rhizodat_postgres_store_free(inventory);
        rhizodat_postgres_store_free(billing);
    }
}

#[test]
fn a_store_that_is_down_aborts_retryably_and_writes_nothing() {
    let Some(dsn) = pg_dsn() else { return };
    let db = Db::new(&dsn);
    let store = db.store("inventory");
    let mut down = ptr::null_mut();
    let (d, n) = (
        c("postgresql://postgres:x@127.0.0.1:1/none?connect_timeout=2"),
        c("down"),
    );
    // SAFETY: valid strings and a writable local.
    ok(|e| unsafe {
        rhizodat_postgres_store_open(d.as_ptr(), n.as_ptr(), ptr::null(), &raw mut down, e)
    });
    let unit = unit_with(store, "web1", 1);
    insert(unit, down, "web1", 1);
    let journal = db.journal();
    let coordinator = coordinator(journal);
    let (status, outcome) = commit(coordinator, unit);
    assert_eq!(status, RhizodatStatus::Aborted, "{outcome}");
    assert_eq!(outcome["status"], "aborted");
    assert_eq!(outcome["retryable"], true);
    assert!(outcome["dat_txn"].is_string());
    assert_eq!(db.count("inventory"), 0);
    // SAFETY: each freed once.
    unsafe {
        rhizodat_unit_free(unit);
        rhizodat_coordinator_free(coordinator);
        rhizodat_journal_free(journal);
        rhizodat_postgres_store_free(store);
        rhizodat_postgres_store_free(down);
    }
}

#[test]
fn two_stores_without_a_journal_are_refused() {
    let Some(dsn) = pg_dsn() else { return };
    let db = Db::new(&dsn);
    let (a, b) = (db.store("a"), db.store("b"));
    let unit = unit_with(a, "web1", 1);
    insert(unit, b, "web1", 1);
    let coordinator = coordinator(ptr::null());
    let (status, outcome) = commit(coordinator, unit);
    assert_eq!(status, RhizodatStatus::InvalidArgument, "{outcome}");
    assert!(
        outcome["error"].as_str().unwrap().contains("journal"),
        "{outcome}"
    );
    assert_eq!((db.count("a"), db.count("b")), (0, 0));
    // SAFETY: each freed once.
    unsafe {
        rhizodat_unit_free(unit);
        rhizodat_coordinator_free(coordinator);
        rhizodat_postgres_store_free(a);
        rhizodat_postgres_store_free(b);
    }
}

#[test]
fn malformed_rows_are_refused_at_insert() {
    let Some(dsn) = pg_dsn() else { return };
    let db = Db::new(&dsn);
    let store = db.store("inventory");
    let mut unit = ptr::null_mut();
    // SAFETY: a writable local.
    ok(|e| unsafe { rhizodat_unit_new(&raw mut unit, e) });
    for (columns, rows, why) in [
        (
            r#"["host","value"]"#,
            r#"[["web1"]]"#,
            "values for 2 columns",
        ),
        (r#"["host","dat_txn"]"#, r#"[["web1", null]]"#, "dat_txn"),
        (r#"{"host": 1}"#, r#"[["web1"]]"#, "columns_json"),
        (r#"["host"]"#, r#"{"host": "web1"}"#, "rows_json"),
        (r#"["host"]"#, r#"["web1"]"#, "array of arrays"),
    ] {
        let (t, cols, rows) = (c("events"), c(columns), c(rows));
        // SAFETY: live handles and valid strings.
        let (status, message) = run(|e| unsafe {
            rhizodat_unit_postgres_insert(unit, store, t.as_ptr(), cols.as_ptr(), rows.as_ptr(), e)
        });
        assert_eq!(status, RhizodatStatus::InvalidArgument, "{why}");
        assert!(message.contains(why), "{message:?} does not name {why:?}");
    }
    // SAFETY: each freed once.
    unsafe {
        rhizodat_unit_free(unit);
        rhizodat_postgres_store_free(store);
    }
}

#[test]
fn a_bad_dsn_is_an_invalid_argument() {
    let mut store = ptr::null_mut();
    let (d, n) = (c("mysql://nope"), c("x"));
    // SAFETY: valid strings and a writable local.
    let (status, message) = run(|e| unsafe {
        rhizodat_postgres_store_open(d.as_ptr(), n.as_ptr(), ptr::null(), &raw mut store, e)
    });
    assert_eq!(status, RhizodatStatus::InvalidArgument, "{message}");
    assert!(store.is_null());
}
