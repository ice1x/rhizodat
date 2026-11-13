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

use rhizodat_clickhouse::{ClickHouseBranch, ClickHouseStore};
use rhizodat_core::clock::HybridLogicalClock;
use rhizodat_core::coordinator::{CommitError, Outcome};
use rhizodat_core::journal::Journal;
use rhizodat_core::rows::RowValue;
use rhizodat_drevo::parts::DrevoParts;
use rhizodat_drevo::{DrevoStore, DrevoWrites};
use rhizodat_postgres::parts::PostgresParts;
use rhizodat_postgres::{PostgresRows, PostgresStore};
use rhizodat_units::{
    ClickHousePending, DrevoPending, NativeStore, PostgresPending, UnitError, commit,
};
use serde_json::json;
use uuid::Uuid;

use crate::clickhouse::RhizodatClickHouseStore;
use crate::drevo::{RhizodatDrevoStore, cypher_params};
use crate::postgres::{RhizodatJournal, RhizodatPostgresStore};
use crate::{
    Failure, IDS, RhizodatStatus, boundary, free_handle, handle, into_handle, optional_text,
    owned_string, runtime, text, values, writable,
};

/// One store's buffered writes.
enum Pending {
    Postgres {
        store: Arc<PostgresStore>,
        rows: PostgresRows,
    },
    ClickHouse {
        store: Arc<ClickHouseStore>,
        /// Checks every insert as the commit's branch will (columns, reserved
        /// names, row lengths), so a bad one is refused at insert.
        check: Box<ClickHouseBranch>,
        inserts: Vec<(String, Vec<String>, Vec<Vec<RowValue>>)>,
    },
    Drevo {
        store: Arc<DrevoStore>,
        writes: DrevoWrites,
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
            Pending::ClickHouse { store, inserts, .. } => Arc::new(ClickHousePending {
                store: store.clone(),
                inserts: inserts.clone(),
            }),
            Pending::Drevo { store, writes } => Arc::new(DrevoPending {
                store: store.clone(),
                writes: writes.clone(),
                parts: DrevoParts::default(),
            }),
        }
    }

    /// Whether this is the buffer of exactly `store` (two stores may not share a name).
    fn holds(&self, store: *const ()) -> bool {
        let held = match self {
            Pending::Postgres { store, .. } => Arc::as_ptr(store).cast::<()>(),
            Pending::ClickHouse { store, .. } => Arc::as_ptr(store).cast::<()>(),
            Pending::Drevo { store, .. } => Arc::as_ptr(store).cast::<()>(),
        };
        std::ptr::eq(held, store)
    }
}

/// The buffer of the store named `name`, made by `new` on first use; refused
/// when another store holds that name.
fn buffer_of<'a>(
    buffer: &'a mut BTreeMap<String, Pending>,
    name: &str,
    store: *const (),
    new: impl FnOnce() -> Pending,
) -> Result<&'a mut Pending, Failure> {
    let entry = buffer.entry(name.to_owned()).or_insert_with(new);
    if entry.holds(store) {
        Ok(entry)
    } else {
        Err(invalid(format!(
            "two stores named {name:?} in one unit of work"
        )))
    }
}

/// `columns_json` and `rows_json` as names and rows of values.
fn table_rows(
    columns_json: &str,
    rows_json: &str,
) -> Result<(Vec<String>, Vec<Vec<RowValue>>), Failure> {
    let columns: Vec<String> = serde_json::from_str(columns_json)
        .map_err(|e| invalid(format!("columns_json must be an array of names: {e}")))?;
    let rows = values(rows_json, "rows_json")?
        .into_iter()
        .map(|row| match row {
            RowValue::List(values) => Ok(values),
            _ => Err(invalid("rows_json must be an array of arrays")),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((columns, rows))
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
            let (columns, rows) = table_rows(
                text(columns_json, "columns_json")?,
                text(rows_json, "rows_json")?,
            )?;
            let mut buffer = unit
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let pending = buffer_of(
                &mut buffer,
                store.name(),
                Arc::as_ptr(&store).cast(),
                || Pending::Postgres {
                    store: store.clone(),
                    rows: PostgresRows::new(),
                },
            )?;
            let Pending::Postgres { rows: buffered, .. } = pending else {
                unreachable!("a buffer holds its own store's kind")
            };
            let column_refs: Vec<&str> = columns.iter().map(String::as_str).collect();
            buffered
                .insert(table, &column_refs, rows)
                .map_err(|e| invalid(e.to_string()))
        })
    }
}

/// Buffer rows for `table` of a ClickHouse store, as
/// [`rhizodat_unit_postgres_insert`]. ClickHouse is one-phase: the rows go
/// into the journal payload and are applied after the decision.
///
/// # Safety
///
/// `unit` and `store` must be live handles; `table`, `columns_json` and
/// `rows_json` NUL-terminated strings; `error` null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_unit_clickhouse_insert(
    unit: *const RhizodatUnit,
    store: *const RhizodatClickHouseStore,
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
            let (columns, rows) = table_rows(
                text(columns_json, "columns_json")?,
                text(rows_json, "rows_json")?,
            )?;
            let mut buffer = unit
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let pending = buffer_of(
                &mut buffer,
                store.name(),
                Arc::as_ptr(&store).cast(),
                || Pending::ClickHouse {
                    store: store.clone(),
                    check: Box::new(store.branch()),
                    inserts: Vec::new(),
                },
            )?;
            let Pending::ClickHouse { check, inserts, .. } = pending else {
                unreachable!("a buffer holds its own store's kind")
            };
            let column_refs: Vec<&str> = columns.iter().map(String::as_str).collect();
            check
                .insert(table, &column_refs, rows.clone())
                .map_err(|e| invalid(e.to_string()))?;
            inserts.push((table.to_owned(), columns, rows));
            Ok(())
        })
    }
}

/// Buffer a Cypher statement for a drevo store, with `params_json` (a JSON
/// object of tagged values, or null). It runs inside the store's branch at the
/// commit (two-phase), with the parameters `dat_txn` and `dat_tx_time` added,
/// so it may stamp what it writes with `$dat_txn`.
///
/// # Safety
///
/// `unit` and `store` must be live handles; `cypher` a NUL-terminated string,
/// `params_json` too or null; `error` null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_unit_drevo_run(
    unit: *const RhizodatUnit,
    store: *const RhizodatDrevoStore,
    cypher: *const c_char,
    params_json: *const c_char,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            let unit = handle(unit, "unit")?;
            let store = handle(store, "store")?.0.clone();
            let cypher = text(cypher, "cypher")?;
            let params = cypher_params(optional_text(params_json, "params_json")?)?;
            let mut buffer = unit
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let pending = buffer_of(
                &mut buffer,
                store.name(),
                Arc::as_ptr(&store).cast(),
                || Pending::Drevo {
                    store: store.clone(),
                    writes: DrevoWrites::new(),
                },
            )?;
            let Pending::Drevo { writes, .. } = pending else {
                unreachable!("a buffer holds its own store's kind")
            };
            writes
                .push(cypher, params)
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
