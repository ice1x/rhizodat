//! Iceberg through the C ABI against the compose REST catalog: schema
//! operations, a meta-transaction over PostgreSQL and Iceberg, scans with
//! columns, a filter and a limit, and the refusals. DSNs come from
//! `RHIZODAT_TEST_*`, else `.env.test` (skipped with a hint when nothing
//! listens).

// Calling a C ABI by hand is long-winded.
#![allow(clippy::too_many_lines, clippy::many_single_char_names)]

use std::ffi::{CStr, CString, c_char};
use std::net::{TcpStream, ToSocketAddrs};
use std::ptr;
use std::time::Duration;

use rhizodat_ffi::*;
use serde_json::{Value, json};

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

fn lake(dsn: &str) -> *mut RhizodatIcebergStore {
    let mut store = ptr::null_mut();
    let (d, n) = (c(dsn), c("lake"));
    // SAFETY: valid strings and a writable local.
    ok(|e| unsafe { rhizodat_iceberg_store_open(d.as_ptr(), n.as_ptr(), &raw mut store, e) });
    store
}

fn operations(store: *mut RhizodatIcebergStore, ops: &Value) -> (RhizodatStatus, String) {
    let ops = c(&ops.to_string());
    // SAFETY: a live handle and a valid string.
    run(|e| unsafe { rhizodat_iceberg_store_run_operations(store, ops.as_ptr(), e) })
}

fn scan(store: *mut RhizodatIcebergStore, table: &str, options: Option<Value>) -> Value {
    let table = c(table);
    let options = options.map(|o| c(&o.to_string()));
    let mut out = ptr::null_mut();
    // SAFETY: a live handle, valid strings, writable locals.
    ok(|e| unsafe {
        rhizodat_iceberg_store_scan(
            store,
            table.as_ptr(),
            options.as_ref().map_or(ptr::null(), |o| o.as_ptr()),
            &raw mut out,
            e,
        )
    });
    serde_json::from_str(&take(out)).unwrap()
}

fn requests_table(ns: &str) -> Value {
    json!([
        {"op": "create_namespace", "namespace": ns},
        {"op": "create_table", "table": format!("{ns}.api_request"), "columns": [
            {"name": "id", "type": "uuid"},
            {"name": "path", "type": "string"},
            {"name": "status", "type": "int"},
            {"name": "ms", "type": "double"},
            {"name": "at", "type": "timestamptz"},
            {"name": "cost", "type": "decimal(10, 2)"},
            {"name": "dat_txn", "type": "uuid"},
            {"name": "dat_tx_time", "type": "long"}
        ]}
    ])
}

#[test]
fn rows_go_into_iceberg_with_postgres_in_one_meta_transaction() {
    let (Some(rest), Some(pg)) = (
        dsn("RHIZODAT_TEST_ICEBERG_REST_DSN", 58181),
        dsn("RHIZODAT_TEST_PG_DSN", 55440),
    ) else {
        return;
    };
    let tag = suffix();
    let ns = format!("ffi_{tag}");
    let table = format!("{ns}.api_request");
    let store = lake(&rest);
    let ops = requests_table(&ns);
    let (status, message) = operations(store, &ops);
    assert_eq!(status, RhizodatStatus::Ok, "{message}");
    // Idempotent.
    assert_eq!(operations(store, &ops).0, RhizodatStatus::Ok);

    // PostgreSQL: the inventory, and the journal.
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
    pg_sql(&format!("CREATE SCHEMA {ns}"));
    pg_sql(&format!("CREATE SCHEMA {ns}_journal"));
    pg_sql(&format!(
        "CREATE TABLE {ns}.resource (name text, dat_txn uuid, dat_tx_time bigint)"
    ));
    let mut inventory = ptr::null_mut();
    let (s, n) = (c(&ns), c(&format!("inventory_{tag}")));
    // SAFETY: valid strings and a writable local.
    ok(|e| unsafe {
        rhizodat_postgres_store_open(d.as_ptr(), n.as_ptr(), s.as_ptr(), &raw mut inventory, e)
    });
    // SAFETY: a live handle.
    ok(|e| unsafe { rhizodat_postgres_store_install(inventory, e) });
    let mut journal = ptr::null_mut();
    let js = c(&format!("{ns}_journal"));
    // SAFETY: valid strings and a writable local.
    ok(|e| unsafe { rhizodat_postgres_journal_open(d.as_ptr(), js.as_ptr(), &raw mut journal, e) });

    let mut unit = ptr::null_mut();
    // SAFETY: a writable local.
    ok(|e| unsafe { rhizodat_unit_new(&raw mut unit, e) });
    let (rt, cols, rows) = (c("resource"), c(r#"["name"]"#), c(r#"[["api-gw"]]"#));
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
    let requests = json!([
        [{"$": "uuid", "v": "01920000-0000-7000-8000-000000000001"}, "/v1/vms", 200, 12.5,
         {"$": "datetime", "v": "2026-11-14T09:30:00+00:00"}, {"$": "decimal", "v": "0.10"}],
        [{"$": "uuid", "v": "01920000-0000-7000-8000-000000000002"}, "/v1/vms", 503, 830,
         {"$": "datetime", "v": "2026-11-14T09:31:00+00:00"}, null]
    ]);
    let (tt, cols, rows) = (
        c(&table),
        c(r#"["id","path","status","ms","at","cost"]"#),
        c(&requests.to_string()),
    );
    // SAFETY: live handles and valid strings.
    ok(|e| unsafe {
        rhizodat_unit_iceberg_append(unit, store, tt.as_ptr(), cols.as_ptr(), rows.as_ptr(), e)
    });
    let mut coordinator = ptr::null_mut();
    // SAFETY: a live journal and a writable local.
    ok(|e| unsafe { rhizodat_coordinator_new(journal, &raw mut coordinator, e) });
    let mut out = ptr::null_mut();
    // SAFETY: live handles and a writable local.
    let (status, message) =
        run(|e| unsafe { rhizodat_coordinator_commit(coordinator, unit, &raw mut out, e) });
    let outcome: Value = serde_json::from_str(&take(out)).unwrap();
    assert_eq!(status, RhizodatStatus::Ok, "{message} {outcome}");

    let all = scan(store, &table, None);
    let failed = scan(
        store,
        &table,
        Some(
            json!({"columns": ["path", "status", "at", "cost", "dat_txn"], "filter": "status >= 500"}),
        ),
    );
    let limited = scan(store, &table, Some(json!({"limit": 1})));
    let pg_rows = pg_sql(&format!("SELECT dat_txn::text AS t FROM {ns}.resource"));
    pg_sql(&format!("DROP SCHEMA {ns}, {ns}_journal CASCADE"));
    assert_eq!(all["rows"].as_array().unwrap().len(), 2);
    assert_eq!(
        failed["rows"],
        json!([[
            "/v1/vms",
            503,
            {"$": "datetime", "v": "2026-11-14T09:31:00.000000+00:00"},
            null,
            {"$": "uuid", "v": outcome["dat_txn"]}
        ]])
    );
    assert_eq!(limited["rows"].as_array().unwrap().len(), 1);
    assert_eq!(pg_rows["rows"], json!([[outcome["dat_txn"]]]));
    // SAFETY: each freed once.
    unsafe {
        rhizodat_unit_free(unit);
        rhizodat_coordinator_free(coordinator);
        rhizodat_journal_free(journal);
        rhizodat_postgres_store_free(inventory);
        rhizodat_postgres_store_free(admin);
        rhizodat_iceberg_store_free(store);
    }
}

#[test]
fn what_iceberg_cannot_take_is_refused() {
    let Some(rest) = dsn("RHIZODAT_TEST_ICEBERG_REST_DSN", 58181) else {
        return;
    };
    let store = lake(&rest);
    let (status, message) = operations(
        store,
        &json!([{"op": "add_column", "table": "a.b", "name": "c", "type": "long"}]),
    );
    assert_eq!(status, RhizodatStatus::InvalidArgument);
    assert!(message.contains("not yet in Rust"), "{message}");
    let mut unit = ptr::null_mut();
    // SAFETY: a writable local.
    ok(|e| unsafe { rhizodat_unit_new(&raw mut unit, e) });
    for (columns, rows, why) in [
        (r#"["a"]"#, r#"[[1], ["x"]]"#, "mixes values"),
        (r#"["a"]"#, "[[[1, 2]]]", "a list or a map"),
        (r#"["dat_txn"]"#, r#"[["x"]]"#, "dat_txn"),
    ] {
        let (t, cols, rows) = (c("ns.t"), c(columns), c(rows));
        // SAFETY: live handles and valid strings.
        let (status, message) = run(|e| unsafe {
            rhizodat_unit_iceberg_append(unit, store, t.as_ptr(), cols.as_ptr(), rows.as_ptr(), e)
        });
        assert_eq!(status, RhizodatStatus::InvalidArgument, "{why}");
        assert!(message.contains(why), "{message:?} does not name {why:?}");
    }
    let (t, o) = (c("ns.t"), c(r#"{"where": "x"}"#));
    let mut out = ptr::null_mut();
    // SAFETY: a live handle, valid strings, writable locals.
    let (status, message) = run(|e| unsafe {
        rhizodat_iceberg_store_scan(store, t.as_ptr(), o.as_ptr(), &raw mut out, e)
    });
    assert_eq!(status, RhizodatStatus::InvalidArgument);
    assert!(message.contains("unknown scan option"), "{message}");
    let mut bad = ptr::null_mut();
    let (d, n) = (c("iceberg+sql://nope"), c("x"));
    // SAFETY: valid strings and a writable local.
    let (status, _) =
        run(|e| unsafe { rhizodat_iceberg_store_open(d.as_ptr(), n.as_ptr(), &raw mut bad, e) });
    assert_eq!(status, RhizodatStatus::InvalidArgument);
    // SAFETY: each freed once.
    unsafe {
        rhizodat_unit_free(unit);
        rhizodat_iceberg_store_free(store);
    }
}
