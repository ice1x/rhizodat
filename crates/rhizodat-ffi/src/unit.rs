//! A unit of work and the coordinator that commits it.
//!
//! A unit of work buffers writes for native stores (no callbacks: the caller's
//! code runs before the commit, not inside it); the coordinator commits them as
//! one meta-transaction through the shared path of `rhizodat_units`, the same
//! one the Python binding's native stores take. A unit of work is kept after a
//! commit, so a caller may retry a retryable abort with the same writes.

use std::collections::BTreeMap;
use std::ffi::c_char;
use std::sync::{Arc, Mutex};

use rhizodat_core::clock::HybridLogicalClock;
use rhizodat_core::coordinator::{CommitError, Outcome};
use rhizodat_core::journal::Journal;
use rhizodat_postgres::parts::PostgresParts;
use rhizodat_postgres::{PostgresRows, PostgresStore};
use rhizodat_units::{NativeStore, PostgresPending, UnitError, commit};
use serde_json::json;
use uuid::Uuid;

use crate::postgres::{RhizodatJournal, RhizodatPostgresStore};
use crate::{
    Failure, IDS, RhizodatStatus, boundary, free_handle, handle, into_handle, owned_string,
    runtime, text, values, writable,
};

/// One store's buffered writes.
enum Pending {
    Postgres {
        store: Arc<PostgresStore>,
        rows: PostgresRows,
    },
}

impl Pending {
    fn native(&self) -> Arc<dyn NativeStore> {
        match self {
            Pending::Postgres { store, rows } => Arc::new(PostgresPending {
                store: store.clone(),
                rows: rows.clone(),
                parts: PostgresParts::default(),
            }),
        }
    }
}

/// A unit of work: writes buffered per store, committed together.
pub struct RhizodatUnit(Mutex<BTreeMap<String, Pending>>);

/// Commits units of work; keeps a hybrid logical clock across them.
pub struct RhizodatCoordinator {
    journal: Option<Arc<dyn Journal>>,
    clock: Arc<HybridLogicalClock>,
}

fn invalid(message: impl Into<String>) -> Failure {
    (RhizodatStatus::InvalidArgument, message.into())
}

/// An empty unit of work.
///
/// # Safety
///
/// `out` must be writable for one pointer; `error` null or writable. Free the
/// unit with [`rhizodat_unit_free`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_unit_new(
    out: *mut *mut RhizodatUnit,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            *writable(out, "out")? = into_handle(RhizodatUnit(Mutex::new(BTreeMap::new())));
            Ok(())
        })
    }
}

/// Buffer rows for `table` of a PostgreSQL store: `columns_json` is a JSON
/// array of column names, `rows_json` a JSON array of rows, each an array of
/// tagged values in column order. They are written at the commit, with the
/// meta-transaction's `dat_txn` and `dat_tx_time` (leave those columns out).
///
/// # Safety
///
/// `unit` and `store` must be live handles; `table`, `columns_json` and
/// `rows_json` NUL-terminated strings; `error` null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_unit_postgres_insert(
    unit: *const RhizodatUnit,
    store: *const RhizodatPostgresStore,
    table: *const c_char,
    columns_json: *const c_char,
    rows_json: *const c_char,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            let unit = handle(unit, "unit")?;
            let store = handle(store, "store")?.0.clone();
            let table = text(table, "table")?;
            let columns: Vec<String> = serde_json::from_str(text(columns_json, "columns_json")?)
                .map_err(|e| invalid(format!("columns_json must be an array of names: {e}")))?;
            let rows = values(text(rows_json, "rows_json")?, "rows_json")?
                .into_iter()
                .map(|row| match row {
                    rhizodat_core::rows::RowValue::List(values) => Ok(values),
                    _ => Err(invalid("rows_json must be an array of arrays")),
                })
                .collect::<Result<Vec<_>, _>>()?;
            let mut buffer = unit
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let name = store.name().to_owned();
            let entry = buffer
                .entry(name.clone())
                .or_insert_with(|| Pending::Postgres {
                    store: store.clone(),
                    rows: PostgresRows::new(),
                });
            let Pending::Postgres {
                store: known,
                rows: buffered,
            } = entry;
            if !Arc::ptr_eq(known, &store) {
                return Err(invalid(format!(
                    "two stores named {name:?} in one unit of work"
                )));
            }
            let column_refs: Vec<&str> = columns.iter().map(String::as_str).collect();
            buffered
                .insert(table, &column_refs, rows)
                .map_err(|e| invalid(e.to_string()))
        })
    }
}

/// Frees a unit of work. Null does nothing.
///
/// # Safety
///
/// `unit` must be null or a unit handle not freed before.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_unit_free(unit: *mut RhizodatUnit) {
    // SAFETY: the caller's contract.
    unsafe { free_handle(unit) }
}

/// A coordinator committing through `journal` (null for none: then a unit of
/// work may write to one store only). The journal stays the caller's to free.
///
/// # Safety
///
/// `journal` must be null or a live journal handle; `out` writable for one
/// pointer; `error` null or writable. Free the coordinator with
/// [`rhizodat_coordinator_free`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_coordinator_new(
    journal: *const RhizodatJournal,
    out: *mut *mut RhizodatCoordinator,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            let out = writable(out, "out")?;
            let journal = if journal.is_null() {
                None
            } else {
                Some(handle(journal, "journal")?.0.clone())
            };
            *out = into_handle(RhizodatCoordinator {
                journal,
                clock: Arc::new(HybridLogicalClock::new()),
            });
            Ok(())
        })
    }
}

/// Commit `unit` as one new meta-transaction. `out` receives the outcome as
/// JSON on `OK` (`{"status": "committed" | "pending", "dat_txn", "dat_tx_time"}`)
/// and also on `ABORTED` (`{"status": "aborted", "dat_txn", "retryable",
/// "error"}`) and `IN_DOUBT` (`{"status": "in_doubt", "dat_txn", "error"}`). A
/// unit of work the core refuses is `INVALID_ARGUMENT`. The unit keeps its
/// writes, so a retryable abort may be committed again.
///
/// # Safety
///
/// `coordinator` and `unit` must be live handles; `out` writable for one
/// pointer; `error` null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_coordinator_commit(
    coordinator: *const RhizodatCoordinator,
    unit: *const RhizodatUnit,
    out: *mut *mut c_char,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            let out = writable(out, "out")?;
            let coordinator = handle(coordinator, "coordinator")?;
            let stores: Vec<Arc<dyn NativeStore>> = handle(unit, "unit")?
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .values()
                .map(Pending::native)
                .collect();
            let dat_txn = IDS
                .get_or_init(rhizodat_core::identity::IdGenerator::new)
                .generate()
                .map_err(|e| (RhizodatStatus::Failed, e.to_string()))?;
            let result = runtime()?.block_on(commit(
                coordinator.journal.clone(),
                coordinator.clock.clone(),
                dat_txn,
                &stores,
            ));
            let (outcome, failure) = outcome_json(dat_txn, result);
            *out = owned_string(outcome.to_string());
            failure.map_or(Ok(()), Err)
        })
    }
}

/// The outcome as JSON, and the failure to report with it.
fn outcome_json(
    dat_txn: Uuid,
    result: Result<Outcome, UnitError>,
) -> (serde_json::Value, Option<Failure>) {
    match result {
        Ok(Outcome::Committed {
            dat_txn,
            dat_tx_time,
        }) => (
            json!({"status": "committed", "dat_txn": dat_txn.to_string(), "dat_tx_time": dat_tx_time}),
            None,
        ),
        Ok(Outcome::Pending {
            dat_txn,
            dat_tx_time,
        }) => (
            json!({"status": "pending", "dat_txn": dat_txn.to_string(), "dat_tx_time": dat_tx_time}),
            None,
        ),
        Err(UnitError::Connect(connect)) => {
            let message = connect.error.to_string();
            (
                json!({"status": "aborted", "dat_txn": dat_txn.to_string(),
                       "retryable": connect.retryable, "error": message}),
                Some((RhizodatStatus::Aborted, message)),
            )
        }
        Err(UnitError::Commit(CommitError::Aborted {
            dat_txn,
            retryable,
            cause,
        })) => {
            let message = cause.to_string();
            (
                json!({"status": "aborted", "dat_txn": dat_txn.to_string(),
                       "retryable": retryable, "error": message}),
                Some((RhizodatStatus::Aborted, message)),
            )
        }
        Err(UnitError::Commit(CommitError::InDoubt { dat_txn, cause })) => {
            let message = cause.to_string();
            (
                json!({"status": "in_doubt", "dat_txn": dat_txn.to_string(), "error": message}),
                Some((RhizodatStatus::InDoubt, message)),
            )
        }
        Err(UnitError::Commit(CommitError::Invalid(message))) => (
            json!({"status": "invalid", "dat_txn": dat_txn.to_string(), "error": message}),
            Some((RhizodatStatus::InvalidArgument, message)),
        ),
    }
}

/// Frees a coordinator. Null does nothing.
///
/// # Safety
///
/// `coordinator` must be null or a coordinator handle not freed before.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_coordinator_free(coordinator: *mut RhizodatCoordinator) {
    // SAFETY: the caller's contract.
    unsafe { free_handle(coordinator) }
}
