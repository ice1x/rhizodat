//! ClickHouse and drevo through the C ABI against the compose services: their
//! own queries, and one meta-transaction over PostgreSQL, ClickHouse and drevo
//! (two-phase PostgreSQL and drevo, one-phase ClickHouse from the journal).
//! DSNs come from `RHIZODAT_TEST_*`, else `.env.test` (skipped with a hint when
//! nothing listens).

// Calling a C ABI by hand is long-winded: one test drives three stores.
#![allow(clippy::too_many_lines, clippy::many_single_char_names)]

use std::ffi::{CStr, CString, c_char};
use std::net::{TcpStream, ToSocketAddrs};
use std::ptr;
use std::time::Duration;

use rhizodat_ffi::*;
use serde_json::{Value, json};

/// The DSN in `var`, else `.env.test`'s when `port` on localhost answers.
fn dsn(var: &str, port: u16) -> Option<String> {
    if let Ok(dsn) = std::env::var(var) {
        return Some(dsn);
    }
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.env.test");
    let dsn = std::fs::read_to_string(path)
        .ok()?
        .lines()
        .find_map(|l| l.strip_prefix(&format!("{var}=")))?
        .to_owned();
    let up = ("127.0.0.1", port)
        .to_socket_addrs()
        .ok()
        .and_then(|mut a| a.next())
        .is_some_and(|a| TcpStream::connect_timeout(&a, Duration::from_millis(500)).is_ok());
    if up {
        Some(dsn)
    } else {
        eprintln!("skipped: {var} from .env.test is not running (docker compose up -d --wait)");
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

fn suffix() -> String {
    let mut id = ptr::null_mut();
    // SAFETY: writable locals.
    ok(|e| unsafe { rhizodat_new_id(&raw mut id, e) });
    take(id).replace('-', "")[20..].to_owned()
}

fn clickhouse(dsn: &str, name: &str) -> *mut RhizodatClickHouseStore {
    let mut store = ptr::null_mut();
    let (d, n) = (c(dsn), c(name));
    // SAFETY: valid strings and a writable local.
    ok(|e| unsafe { rhizodat_clickhouse_store_open(d.as_ptr(), n.as_ptr(), &raw mut store, e) });
    // SAFETY: a live handle.
    ok(|e| unsafe { rhizodat_clickhouse_store_install(store, e) });
    store
}

fn ch_sql(store: *mut RhizodatClickHouseStore, sql: &str, params: Option<Value>) -> Value {
    let sql = c(sql);
    let params = params.map(|p| c(&p.to_string()));
    let mut out = ptr::null_mut();
    // SAFETY: a live handle, valid strings, writable locals.
    ok(|e| unsafe {
        rhizodat_clickhouse_store_execute(
            store,
            sql.as_ptr(),
            params.as_ref().map_or(ptr::null(), |p| p.as_ptr()),
            &raw mut out,
            e,
        )
    });
    serde_json::from_str(&take(out)).unwrap()
}

fn drevo(dsn: &str, name: &str) -> *mut RhizodatDrevoStore {
    let mut store = ptr::null_mut();
    let (d, n) = (c(dsn), c(name));
    // SAFETY: valid strings and a writable local.
    ok(|e| unsafe { rhizodat_drevo_store_open(d.as_ptr(), n.as_ptr(), &raw mut store, e) });
    store
}

fn cypher(store: *mut RhizodatDrevoStore, query: &str, params: Option<Value>) -> Value {
    let query = c(query);
    let params = params.map(|p| c(&p.to_string()));
    let mut out = ptr::null_mut();
    // SAFETY: a live handle, valid strings, writable locals.
    ok(|e| unsafe {
        rhizodat_drevo_store_execute(
            store,
            query.as_ptr(),
            params.as_ref().map_or(ptr::null(), |p| p.as_ptr()),
            &raw mut out,
            e,
        )
    });
    serde_json::from_str(&take(out)).unwrap()
}

#[test]
fn clickhouse_execute_returns_typed_rows() {
    let Some(dsn) = dsn("RHIZODAT_TEST_CLICKHOUSE_DSN", 58123) else {
        return;
    };
    let store = clickhouse(&dsn, &format!("metrics_{}", suffix()));
    let result = ch_sql(
        store,
        "SELECT {host:String} AS host, toInt64(42) AS n, toFloat64(0.5) AS f, \
         toFixedString(toString(toUUID({id:String})), 36) AS id, toDate32('2026-11-14') AS d, \
         toDecimal64('12.34', 2) AS price, [1, 2] AS xs, CAST(NULL AS Nullable(Int64)) AS gone",
        Some(json!({"host": "web1", "id": "01920000-0000-7000-8000-000000000001"})),
    );
    assert_eq!(
        result["columns"],
        json!(["host", "n", "f", "id", "d", "price", "xs", "gone"])
    );
    assert_eq!(
        result["rows"],
        json!([[
            "web1",
            42,
            0.5,
            "01920000-0000-7000-8000-000000000001",
            {"$": "date", "v": "2026-11-14"},
            {"$": "decimal", "v": "12.34"},
            [1, 2],
            null
        ]])
    );
    // A statement that returns no rows answers an empty result.
    assert_eq!(ch_sql(store, "SELECT 1 WHERE 0", None)["rows"], json!([]));
    // SAFETY: freed once.
    unsafe { rhizodat_clickhouse_store_free(store) };
}

#[test]
fn drevo_execute_binds_and_returns_columns_in_name_order() {
    let Some(dsn) = dsn("RHIZODAT_TEST_DREVO_BOLT", 57687) else {
        return;
    };
    let store = drevo(&dsn, &format!("graph_{}", suffix()));
    let result = cypher(
        store,
        "RETURN $name AS name, $n AS n, $tags AS tags",
        Some(json!({"name": "web1", "n": 3, "tags": ["a", "b"]})),
    );
    assert_eq!(result["columns"], json!(["n", "name", "tags"]));
    assert_eq!(result["rows"], json!([[3, "web1", ["a", "b"]]]));
    // SAFETY: freed once.
    unsafe { rhizodat_drevo_store_free(store) };
}

#[test]
fn one_meta_transaction_over_postgres_clickhouse_and_drevo() {
    let (Some(pg), Some(ch), Some(graph)) = (
        dsn("RHIZODAT_TEST_PG_DSN", 55440),
        dsn("RHIZODAT_TEST_CLICKHOUSE_DSN", 58123),
        dsn("RHIZODAT_TEST_DREVO_BOLT", 57687),
    ) else {
        return;
    };
    let tag = suffix();
    // PostgreSQL: a schema of its own with the inventory table.
    let mut admin = ptr::null_mut();
    let (d, n) = (c(&pg), c("admin"));
    // SAFETY: valid strings and a writable local.
    ok(|e| unsafe {
        rhizodat_postgres_store_open(d.as_ptr(), n.as_ptr(), ptr::null(), &raw mut admin, e)
    });
    let pg_sql = |sql: &str| {
        let sql = c(sql);
        let mut out = ptr::null_mut();
        // SAFETY: a live handle, a valid string, a writable local.
        ok(|e| unsafe {
            rhizodat_postgres_store_execute(admin, sql.as_ptr(), ptr::null(), &raw mut out, e)
        });
        serde_json::from_str::<Value>(&take(out)).unwrap()
    };
    let schema = format!("ffi_{tag}");
    pg_sql(&format!("CREATE SCHEMA {schema}"));
    pg_sql(&format!(
        "CREATE TABLE {schema}.resource (name text, dat_txn uuid, dat_tx_time bigint)"
    ));
    let mut inventory = ptr::null_mut();
    let (s, n) = (c(&schema), c(&format!("inventory_{tag}")));
    // SAFETY: valid strings and a writable local.
    ok(|e| unsafe {
        rhizodat_postgres_store_open(d.as_ptr(), n.as_ptr(), s.as_ptr(), &raw mut inventory, e)
    });
    // SAFETY: a live handle.
    ok(|e| unsafe { rhizodat_postgres_store_install(inventory, e) });
    // ClickHouse: a metrics table.
    let metrics = clickhouse(&ch, &format!("metrics_{tag}"));
    let table = format!("metric_{tag}");
    ch_sql(
        metrics,
        &format!(
            "CREATE TABLE {table} (host String, cpu Float64, dat_txn UUID, dat_tx_time Int64) \
             ENGINE = MergeTree ORDER BY host"
        ),
        None,
    );
    // drevo: the topology graph. The journal goes to PostgreSQL: a drevo journal
    // may not share a graph with a store's data (its writes would outdate the
    // branch's transaction).
    let topology = drevo(&graph, &format!("topology_{tag}"));
    pg_sql(&format!("CREATE SCHEMA {schema}_journal"));
    let mut journal = ptr::null_mut();
    let js = c(&format!("{schema}_journal"));
    // SAFETY: valid strings and a writable local.
    ok(|e| unsafe { rhizodat_postgres_journal_open(d.as_ptr(), js.as_ptr(), &raw mut journal, e) });

    let mut unit = ptr::null_mut();
    // SAFETY: a writable local.
    ok(|e| unsafe { rhizodat_unit_new(&raw mut unit, e) });
    let (rt, cols, rows) = (c("resource"), c(r#"["name"]"#), c(r#"[["web1"]]"#));
    // SAFETY: live handles and valid strings.
    ok(|e| unsafe {
        rhizodat_unit_postgres_insert(
            unit,
            inventory,
            rt.as_ptr(),
            cols.as_ptr(),
            rows.as_ptr(),
            e,
        )
    });
    let (mt, cols, rows) = (
        c(&table),
        c(r#"["host","cpu"]"#),
        c(r#"[["web1",0.25],["web1",0.75]]"#),
    );
    // SAFETY: live handles and valid strings.
    ok(|e| unsafe {
        rhizodat_unit_clickhouse_insert(unit, metrics, mt.as_ptr(), cols.as_ptr(), rows.as_ptr(), e)
    });
    let label_name = format!("Host{tag}");
    let (q, p) = (
        c(&format!(
            "CREATE (:{label_name} {{name: $name, txn: $dat_txn}})"
        )),
        c(r#"{"name": "web1"}"#),
    );
    // SAFETY: live handles and valid strings.
    ok(|e| unsafe { rhizodat_unit_drevo_run(unit, topology, q.as_ptr(), p.as_ptr(), e) });

    let mut coordinator = ptr::null_mut();
    // SAFETY: a live journal and a writable local.
    ok(|e| unsafe { rhizodat_coordinator_new(journal, &raw mut coordinator, e) });
    // A concurrent write to the graph (another test) aborts the commit
    // retryably: re-run it with the same unit of work, as a caller would.
    let mut attempts = 0;
    let (status, message, outcome) = loop {
        attempts += 1;
        let mut out = ptr::null_mut();
        // SAFETY: live handles and a writable local.
        let (status, message) =
            run(|e| unsafe { rhizodat_coordinator_commit(coordinator, unit, &raw mut out, e) });
        let outcome: Value = serde_json::from_str(&take(out)).unwrap();
        if status != RhizodatStatus::Aborted || outcome["retryable"] != true || attempts == 5 {
            break (status, message, outcome);
        }
    };
    assert_eq!(status, RhizodatStatus::Ok, "{message} {outcome}");
    let dat_txn = outcome["dat_txn"].as_str().unwrap().to_owned();

    let pg_rows = pg_sql(&format!(
        "SELECT name, dat_txn::text AS t FROM {schema}.resource"
    ));
    let ch_rows = ch_sql(
        metrics,
        &format!("SELECT count() AS n, any(toString(dat_txn)) AS t FROM {table}"),
        None,
    );
    let graph_rows = cypher(
        topology,
        &format!("MATCH (h:{label_name}) RETURN h.name AS name, h.txn AS txn"),
        None,
    );
    // Clean up before asserting.
    ch_sql(metrics, &format!("DROP TABLE {table}"), None);
    cypher(
        topology,
        &format!("MATCH (h:{label_name}) DETACH DELETE h"),
        None,
    );
    pg_sql(&format!("DROP SCHEMA {schema}, {schema}_journal CASCADE"));
    assert_eq!(pg_rows["rows"], json!([["web1", dat_txn]]));
    assert_eq!(ch_rows["rows"], json!([[2, dat_txn]]));
    assert_eq!(graph_rows["rows"], json!([["web1", dat_txn]]));
    // SAFETY: each freed once.
    unsafe {
        rhizodat_unit_free(unit);
        rhizodat_coordinator_free(coordinator);
        rhizodat_journal_free(journal);
        rhizodat_postgres_store_free(inventory);
        rhizodat_postgres_store_free(admin);
        rhizodat_clickhouse_store_free(metrics);
        rhizodat_drevo_store_free(topology);
    }
}

#[test]
fn a_drevo_journal_opens_and_a_bad_label_is_refused() {
    let Some(graph) = dsn("RHIZODAT_TEST_DREVO_BOLT", 57687) else {
        return;
    };
    let g = c(&graph);
    let mut journal = ptr::null_mut();
    // SAFETY: a valid string, null label (the default), a writable local.
    ok(|e| unsafe { rhizodat_drevo_journal_open(g.as_ptr(), ptr::null(), &raw mut journal, e) });
    // SAFETY: freed once.
    unsafe { rhizodat_journal_free(journal) };
    let bad = c("no spaces allowed");
    let mut other = ptr::null_mut();
    // SAFETY: valid strings and a writable local.
    let (status, _) = run(|e| unsafe {
        rhizodat_drevo_journal_open(g.as_ptr(), bad.as_ptr(), &raw mut other, e)
    });
    assert_eq!(status, RhizodatStatus::InvalidArgument);
    assert!(other.is_null());
}

#[test]
fn bad_inserts_and_parameters_are_refused() {
    let Some(ch) = dsn("RHIZODAT_TEST_CLICKHOUSE_DSN", 58123) else {
        return;
    };
    let metrics = clickhouse(&ch, &format!("metrics_{}", suffix()));
    let mut unit = ptr::null_mut();
    // SAFETY: a writable local.
    ok(|e| unsafe { rhizodat_unit_new(&raw mut unit, e) });
    let (t, cols, rows) = (c("m"), c(r#"["host","dat_txn"]"#), c(r#"[["web1", null]]"#));
    // SAFETY: live handles and valid strings.
    let (status, message) = run(|e| unsafe {
        rhizodat_unit_clickhouse_insert(unit, metrics, t.as_ptr(), cols.as_ptr(), rows.as_ptr(), e)
    });
    assert_eq!(status, RhizodatStatus::InvalidArgument);
    assert!(message.contains("dat_txn"), "{message}");
    let (sql, params) = (
        c("SELECT {d:Date}"),
        c(r#"{"d": {"$": "date", "v": "2026-11-14"}}"#),
    );
    let mut out = ptr::null_mut();
    // SAFETY: a live handle, valid strings, writable locals.
    let (status, message) = run(|e| unsafe {
        rhizodat_clickhouse_store_execute(metrics, sql.as_ptr(), params.as_ptr(), &raw mut out, e)
    });
    assert_eq!(status, RhizodatStatus::InvalidArgument);
    assert!(message.contains("cannot bind \"d\""), "{message}");
    // SAFETY: each freed once.
    unsafe {
        rhizodat_unit_free(unit);
        rhizodat_clickhouse_store_free(metrics);
    }
}
