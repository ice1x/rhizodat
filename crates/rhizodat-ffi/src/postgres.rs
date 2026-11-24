//! PostgreSQL through the C ABI: a store (its own SQL outside any
//! meta-transaction, and the target of a unit of work's rows) and the durable
//! journal several stores commit through.

use std::ffi::c_char;
use std::sync::Arc;

use rhizodat_core::journal::Journal;
use rhizodat_postgres::parts::{ColumnType, PartSpec, PartVersion};
use rhizodat_postgres::query::query;
use rhizodat_postgres::{ConfigError, PostgresJournal, PostgresStore};

use crate::{
    Failure, RhizodatStatus, boundary, free_handle, handle, into_handle, optional_text,
    owned_string, parts, rows_json, runtime, text, values, writable,
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

// --- entity parts ----------------------------------------------------------------------

/// `spec_json` as a PostgreSQL part spec (column types as SQL: `text`,
/// `boolean`, `bigint`, `double precision`, `numeric`, `timestamptz`, `date`,
/// `uuid`, `bytea`, `jsonb`).
///
/// # Safety
///
/// `spec_json` must be null or a NUL-terminated string.
pub(crate) unsafe fn part_spec(spec_json: *const c_char) -> Result<PartSpec, Failure> {
    // SAFETY: the caller's contract.
    let spec = unsafe { parts::spec(spec_json) }?;
    let columns = spec
        .columns
        .into_iter()
        .map(|column| {
            let (name, ty, _) = column.into_parts();
            Ok((name, ty.parse::<ColumnType>()?))
        })
        .collect::<Result<Vec<_>, ConfigError>>()
        .map_err(|e| (RhizodatStatus::InvalidArgument, e.to_string()))?;
    PartSpec::new(&spec.type_name, &spec.table, columns)
        .map_err(|e| (RhizodatStatus::InvalidArgument, e.to_string()))
}

fn version_json(version: &PartVersion) -> String {
    parts::version_json(
        Some(version.dat_txn),
        version.dat_tx_time,
        version.created,
        version.deleted,
        &version.values,
    )
}

/// Create the part's table and index (and `dat_applied`) if missing. Idempotent.
///
/// # Safety
///
/// `store` must be a live store handle; `spec_json` a NUL-terminated string;
/// `error` null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_postgres_store_install_part(
    store: *const RhizodatPostgresStore,
    spec_json: *const c_char,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            let store = handle(store, "store")?.0.clone();
            let spec = part_spec(spec_json)?;
            runtime()?
                .block_on(store.install_part(&spec))
                .map_err(|e| failed(server_message(&e)))
        })
    }
}

/// The latest committed version of `dat_id`'s part with `dat_tx_time <=
/// *known_at` (any, when `known_at` is null), as JSON (see the C ABI's parts),
/// or `null` when there is none.
///
/// # Safety
///
/// `store` must be a live store handle; `spec_json` and `dat_id`
/// NUL-terminated strings; `known_at` null or readable; `out` writable for one
/// pointer; `error` null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_postgres_store_read_part(
    store: *const RhizodatPostgresStore,
    spec_json: *const c_char,
    dat_id: *const c_char,
    known_at: *const i64,
    out: *mut *mut c_char,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            let out = writable(out, "out")?;
            let store = handle(store, "store")?.0.clone();
            let spec = part_spec(spec_json)?;
            let dat_id = parts::uuid_arg(dat_id, "dat_id")?;
            let version = runtime()?
                .block_on(store.read_part(&spec, dat_id, parts::optional_i64(known_at)))
                .map_err(|e| failed(server_message(e.as_ref())))?;
            *out = owned_string(
                version
                    .as_ref()
                    .map_or_else(|| "null".to_owned(), version_json),
            );
            Ok(())
        })
    }
}

/// Every committed version of `dat_id`'s part, oldest first, as a JSON array.
///
/// # Safety
///
/// `store` must be a live store handle; `spec_json` and `dat_id`
/// NUL-terminated strings; `out` writable for one pointer; `error` null or
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_postgres_store_part_history(
    store: *const RhizodatPostgresStore,
    spec_json: *const c_char,
    dat_id: *const c_char,
    out: *mut *mut c_char,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            let out = writable(out, "out")?;
            let store = handle(store, "store")?.0.clone();
            let spec = part_spec(spec_json)?;
            let dat_id = parts::uuid_arg(dat_id, "dat_id")?;
            let versions = runtime()?
                .block_on(store.part_history(&spec, dat_id))
                .map_err(|e| failed(server_message(e.as_ref())))?;
            *out = owned_string(parts::json_array(versions.iter().map(version_json)));
            Ok(())
        })
    }
}

/// Every committed version of every entity's part with `dat_tx_time <=
/// *known_at` (all, when `known_at` is null), oldest first, as a JSON array of
/// versions with their entity's `dat_id`.
///
/// # Safety
///
/// `store` must be a live store handle; `spec_json` a NUL-terminated string;
/// `known_at` null or readable; `out` writable for one pointer; `error` null or
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_postgres_store_part_versions(
    store: *const RhizodatPostgresStore,
    spec_json: *const c_char,
    known_at: *const i64,
    out: *mut *mut c_char,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            let out = writable(out, "out")?;
            let store = handle(store, "store")?.0.clone();
            let spec = part_spec(spec_json)?;
            let versions = runtime()?
                .block_on(store.part_versions(&spec, parts::optional_i64(known_at)))
                .map_err(|e| failed(server_message(e.as_ref())))?;
            *out = owned_string(parts::json_array(
                versions
                    .iter()
                    .map(|(id, v)| parts::entity_version_json(*id, &version_json(v))),
            ));
            Ok(())
        })
    }
}

/// The entities whose latest version is a soft delete with `dat_tx_time <
/// deleted_before`, at most `limit`, as a JSON array of ids.
///
/// # Safety
///
/// `store` must be a live store handle; `spec_json` a NUL-terminated string;
/// `out` writable for one pointer; `error` null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_postgres_store_expired_parts(
    store: *const RhizodatPostgresStore,
    spec_json: *const c_char,
    deleted_before: i64,
    limit: i64,
    out: *mut *mut c_char,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            let out = writable(out, "out")?;
            let store = handle(store, "store")?.0.clone();
            let spec = part_spec(spec_json)?;
            let ids = runtime()?
                .block_on(store.expired_parts(&spec, deleted_before, limit))
                .map_err(|e| failed(server_message(e.as_ref())))?;
            *out = owned_string(parts::json_array(ids.iter().map(|id| format!("\"{id}\""))));
            Ok(())
        })
    }
}
