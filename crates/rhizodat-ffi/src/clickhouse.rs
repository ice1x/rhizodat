//! ClickHouse through the C ABI: a store, its own SQL outside any
//! meta-transaction, and the target of a unit of work's rows (one-phase,
//! applied after the decision from the journal payload).

use std::ffi::c_char;
use std::sync::Arc;

use rhizodat_clickhouse::ClickHouseStore;
use rhizodat_core::rows::RowValue;
use serde_json::Value as Json;

use crate::arrow::ipc_rows;
use crate::{
    Failure, RhizodatStatus, boundary, free_handle, handle, into_handle, optional_text,
    owned_string, rows_json, runtime, text, writable,
};

/// One ClickHouse database as a rhizodat store.
pub struct RhizodatClickHouseStore(pub(crate) Arc<ClickHouseStore>);

fn failed(error: impl std::fmt::Display) -> Failure {
    (RhizodatStatus::Failed, error.to_string())
}

/// `params_json` (a JSON object of tagged values) as the server-side
/// parameters of `{name:Type}` placeholders: strings, integers, floats,
/// booleans, UUIDs and null.
fn params(json: Option<&str>) -> Result<Vec<(String, Json)>, Failure> {
    let Some(json) = json else {
        return Ok(Vec::new());
    };
    let object: serde_json::Map<String, Json> = serde_json::from_str(json).map_err(|e| {
        (
            RhizodatStatus::InvalidArgument,
            format!("params_json must be a JSON object: {e}"),
        )
    })?;
    object
        .into_iter()
        .map(|(name, raw)| {
            let value = rhizodat_core::rows::decode_value(&raw.to_string())
                .map_err(|e| (RhizodatStatus::InvalidArgument, format!("{name}: {e}")))?;
            let json = match value {
                RowValue::Null => Json::Null,
                RowValue::Bool(b) => Json::Bool(b),
                RowValue::Int(i) => Json::from(i),
                RowValue::Float(f) => Json::from(f),
                RowValue::Str(s) => Json::String(s),
                RowValue::Uuid(u) => Json::String(u.to_string()),
                other => {
                    return Err((
                        RhizodatStatus::InvalidArgument,
                        format!(
                            "cannot bind {name:?}: {other:?} is not a string, an integer, a \
                             float, a boolean, a UUID or null"
                        ),
                    ));
                }
            };
            Ok((name, json))
        })
        .collect()
}

/// A store named `name` over a ClickHouse DSN (`clickhouse://…`).
///
/// # Safety
///
/// `dsn` and `name` must be NUL-terminated strings; `out` writable for one
/// pointer; `error` null or writable. Free with [`rhizodat_clickhouse_store_free`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_clickhouse_store_open(
    dsn: *const c_char,
    name: *const c_char,
    out: *mut *mut RhizodatClickHouseStore,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            let out = writable(out, "out")?;
            let store = ClickHouseStore::new(text(dsn, "dsn")?, text(name, "name")?)
                .map_err(|e| (RhizodatStatus::InvalidArgument, e.to_string()))?;
            *out = into_handle(RhizodatClickHouseStore(Arc::new(store)));
            Ok(())
        })
    }
}

/// Create the store's service table `dat_applied` if it does not exist.
///
/// # Safety
///
/// `store` must be a live store handle; `error` null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_clickhouse_store_install(
    store: *const RhizodatClickHouseStore,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            let store = handle(store, "store")?.0.clone();
            runtime()?.block_on(store.install()).map_err(failed)
        })
    }
}

/// Run one statement outside any meta-transaction, with `params_json` (a JSON
/// object of tagged values, or null) bound server-side to its `{name:Type}`
/// placeholders. A statement that returns rows (`SELECT`, `WITH`, `SHOW`,
/// `DESCRIBE`, `EXISTS`, `EXPLAIN`) answers `{"columns", "rows"}` with tagged
/// values; any other runs for its effect and answers no columns and no rows.
/// The rows travel as Arrow, so a column ClickHouse cannot send as Arrow
/// (`UUID`, a bare `NULL`) is refused by the server: cast it (`toString(id)`).
/// A `Date` arrives as its day number (ClickHouse sends it as `UInt16`); a
/// `Date32` arrives as a date.
///
/// # Safety
///
/// `store` must be a live store handle; `sql` a NUL-terminated string,
/// `params_json` too or null; `out` writable for one pointer; `error` null or
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_clickhouse_store_execute(
    store: *const RhizodatClickHouseStore,
    sql: *const c_char,
    params_json: *const c_char,
    out: *mut *mut c_char,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            let out = writable(out, "out")?;
            let store = handle(store, "store")?.0.clone();
            let sql = text(sql, "sql")?;
            let params = params(optional_text(params_json, "params_json")?)?;
            let first = sql.split_whitespace().next().unwrap_or("").to_uppercase();
            let returns_rows = matches!(
                first.as_str(),
                "SELECT" | "WITH" | "SHOW" | "DESCRIBE" | "DESC" | "EXISTS" | "EXPLAIN"
            );
            let runtime = runtime()?;
            let (columns, rows) = if returns_rows {
                let stream = runtime
                    .block_on(store.fetch(sql, &params, "ArrowStream"))
                    .map_err(failed)?;
                ipc_rows(&stream).map_err(failed)?
            } else {
                runtime
                    .block_on(store.command(sql, &params))
                    .map_err(failed)?;
                (Vec::new(), Vec::new())
            };
            *out = owned_string(rows_json(&columns, rows)?);
            Ok(())
        })
    }
}

/// Frees a store. Null does nothing.
///
/// # Safety
///
/// `store` must be null or a store handle not freed before.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_clickhouse_store_free(store: *mut RhizodatClickHouseStore) {
    // SAFETY: the caller's contract.
    unsafe { free_handle(store) }
}
