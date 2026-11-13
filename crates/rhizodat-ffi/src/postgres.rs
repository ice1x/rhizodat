//! PostgreSQL through the C ABI: a store (its own SQL outside any
//! meta-transaction, and the target of a unit of work's rows) and the durable
//! journal several stores commit through.

use std::ffi::c_char;
use std::sync::Arc;

use rhizodat_core::journal::Journal;
use rhizodat_postgres::query::query;
use rhizodat_postgres::{PostgresJournal, PostgresStore};

use crate::{
    Failure, RhizodatStatus, boundary, free_handle, handle, into_handle, optional_text,
    owned_string, rows_json, runtime, text, values, writable,
};

/// One PostgreSQL database as a rhizodat store.
pub struct RhizodatPostgresStore(pub(crate) Arc<PostgresStore>);

/// A journal: the record of meta-transactions over several stores.
pub struct RhizodatJournal(pub(crate) Arc<dyn Journal>);

fn failed(error: impl std::fmt::Display) -> Failure {
    (RhizodatStatus::Failed, error.to_string())
}

/// The driver's error with the server's message (`tokio_postgres::Error`
/// displays as "db error" and keeps the message in its source).
fn server_message(error: &(dyn std::error::Error + 'static)) -> String {
    match error
        .downcast_ref::<tokio_postgres::Error>()
        .and_then(tokio_postgres::Error::as_db_error)
    {
        Some(db) => format!("{}: {}", db.code().code(), db.message()),
        None => error.to_string(),
    }
}

/// A store named `name` over `dsn`, confined to `schema` when it is not null
/// (no two stores may share one schema's marker table). Connects on first use.
///
/// # Safety
///
/// `dsn` and `name` must be NUL-terminated strings, `schema` too or null; `out`
/// writable for one pointer; `error` null or writable. Free the store with
/// [`rhizodat_postgres_store_free`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_postgres_store_open(
    dsn: *const c_char,
    name: *const c_char,
    schema: *const c_char,
    out: *mut *mut RhizodatPostgresStore,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            let out = writable(out, "out")?;
            let store = PostgresStore::new(
                text(dsn, "dsn")?,
                text(name, "name")?,
                optional_text(schema, "schema")?,
            )
            .map_err(|e| (RhizodatStatus::InvalidArgument, e.to_string()))?;
            *out = into_handle(RhizodatPostgresStore(Arc::new(store)));
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
pub unsafe extern "C" fn rhizodat_postgres_store_install(
    store: *const RhizodatPostgresStore,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            let store = handle(store, "store")?.0.clone();
            runtime()?
                .block_on(store.install())
                .map_err(|e| failed(server_message(&e)))
        })
    }
}

/// Run one statement in autocommit mode, outside any meta-transaction, with
/// `params_json` (a JSON array of tagged values, or null for none) bound to its
/// `$1..$n` by the statement's parameter types. The result is
/// `{"columns": [...], "rows": [[value, ...], ...]}` with tagged values.
///
/// # Safety
///
/// `store` must be a live store handle; `sql` a NUL-terminated string,
/// `params_json` too or null; `out` writable for one pointer; `error` null or
/// writable. Free the result with [`crate::rhizodat_string_free`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_postgres_store_execute(
    store: *const RhizodatPostgresStore,
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
            let params = match optional_text(params_json, "params_json")? {
                Some(json) => values(json, "params_json")?,
                None => Vec::new(),
            };
            let result = runtime()?
                .block_on(async {
                    let client = store.connect().await?;
                    query(&client, sql, &params).await
                })
                .map_err(|e| failed(server_message(e.as_ref())))?;
            *out = owned_string(rows_json(&result.columns, result.rows)?);
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
pub unsafe extern "C" fn rhizodat_postgres_store_free(store: *mut RhizodatPostgresStore) {
    // SAFETY: the caller's contract.
    unsafe { free_handle(store) }
}

/// The durable journal in PostgreSQL: the table `dat_journal` in `schema` (or
/// the DSN's `schema=` parameter; both given must agree), created if missing.
///
/// # Safety
///
/// `dsn` must be a NUL-terminated string, `schema` too or null; `out` writable
/// for one pointer; `error` null or writable. Free the journal with
/// [`rhizodat_journal_free`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_postgres_journal_open(
    dsn: *const c_char,
    schema: *const c_char,
    out: *mut *mut RhizodatJournal,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            let out = writable(out, "out")?;
            let (dsn, schema) = (text(dsn, "dsn")?, optional_text(schema, "schema")?);
            let journal = runtime()?
                .block_on(PostgresJournal::open(dsn, schema, "dat_journal"))
                .map_err(failed)?;
            *out = into_handle(RhizodatJournal(Arc::new(journal)));
            Ok(())
        })
    }
}

/// Frees a journal. Null does nothing.
///
/// # Safety
///
/// `journal` must be null or a journal handle not freed before.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_journal_free(journal: *mut RhizodatJournal) {
    // SAFETY: the caller's contract.
    unsafe { free_handle(journal) }
}
