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
use rhizodat_iceberg::IcebergBatches;
use rhizodat_iceberg::parts::IcebergParts;
use rhizodat_postgres::parts::PostgresParts;
use rhizodat_postgres::{PostgresRows, PostgresStore};
use rhizodat_units::{
    ClickHousePending, DrevoPending, IcebergLake, IcebergPending, NativeStore, PostgresPending,
    UnitError, commit,
};
use serde_json::json;
use uuid::Uuid;

use crate::arrow::record_batch;
use crate::clickhouse::RhizodatClickHouseStore;
use crate::drevo::{RhizodatDrevoStore, cypher_params, drevo_spec};
use crate::iceberg::RhizodatIcebergStore;
use crate::postgres::{RhizodatJournal, RhizodatPostgresStore, part_spec};
use crate::{
    Failure, IDS, RhizodatStatus, boundary, free_handle, handle, into_handle, optional_text,
    owned_string, parts, runtime, text, values, writable,
};

/// One store's buffered writes.
enum Pending {
    Postgres {
        store: Arc<PostgresStore>,
        rows: PostgresRows,
        parts: PostgresParts,
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
        parts: DrevoParts,
    },
    Iceberg {
        /// The store handle's address, its identity (it holds no `Arc` of a
        /// core store); only compared.
        id: usize,
        dsn: Arc<str>,
        name: Arc<str>,
        batches: IcebergBatches,
    },
}

impl Pending {
    fn native(&self) -> Arc<dyn NativeStore> {
        match self {
            Pending::Postgres { store, rows, parts } => Arc::new(PostgresPending {
                store: store.clone(),
                rows: rows.clone(),
                parts: parts.clone(),
            }),
            Pending::ClickHouse { store, inserts, .. } => Arc::new(ClickHousePending {
                store: store.clone(),
                inserts: inserts.clone(),
            }),
            Pending::Drevo {
                store,
                writes,
                parts,
            } => Arc::new(DrevoPending {
                store: store.clone(),
                writes: writes.clone(),
                parts: parts.clone(),
            }),
            Pending::Iceberg {
                dsn, name, batches, ..
            } => Arc::new(IcebergPending {
                lake: IcebergLake::new(dsn.clone(), name.clone()),
                batches: batches.clone(),
                parts: IcebergParts::default(),
            }),
        }
    }

    /// Whether this is the buffer of exactly `store` (two stores may not share a name).
    fn holds(&self, store: *const ()) -> bool {
        let held = match self {
            Pending::Postgres { store, .. } => Arc::as_ptr(store).cast::<()>(),
            Pending::ClickHouse { store, .. } => Arc::as_ptr(store).cast::<()>(),
            Pending::Drevo { store, .. } => Arc::as_ptr(store).cast::<()>(),
            Pending::Iceberg { id, .. } => *id as *const (),
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

fn postgres_pending(store: &Arc<PostgresStore>) -> Pending {
    Pending::Postgres {
        store: store.clone(),
        rows: PostgresRows::new(),
        parts: PostgresParts::new(),
    }
}

/// The parts buffered for a PostgreSQL store, made on first use.
///
/// # Safety
///
/// `unit` and `store` must be null or live handles.
unsafe fn postgres_parts<R>(
    unit: *const RhizodatUnit,
    store: *const RhizodatPostgresStore,
    with: impl FnOnce(&mut PostgresParts) -> Result<R, Failure>,
) -> Result<R, Failure> {
    // SAFETY: the caller's contract.
    let (unit, store) = unsafe { (handle(unit, "unit")?, handle(store, "store")?.0.clone()) };
    let mut buffer = unit
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let pending = buffer_of(
        &mut buffer,
        store.name(),
        Arc::as_ptr(&store).cast(),
        || postgres_pending(&store),
    )?;
    let Pending::Postgres { parts, .. } = pending else {
        unreachable!("a buffer holds its own store's kind")
    };
    with(parts)
}

/// Buffer a version of `dat_id`'s part in a PostgreSQL store: `fields_json` a
/// JSON array of field names, `values_json` a JSON array of their tagged
/// values (fields left out are `NULL`); `created` the time of the entity's
/// first version, or null when this is the first; `deleted` for a soft delete.
/// It is written at the commit, stamped with the meta-transaction.
///
/// # Safety
///
/// `unit` and `store` must be live handles; `spec_json`, `dat_id`,
/// `fields_json` and `values_json` NUL-terminated strings; `created` null or
/// readable; `error` null or writable.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn rhizodat_unit_postgres_write_part(
    unit: *const RhizodatUnit,
    store: *const RhizodatPostgresStore,
    spec_json: *const c_char,
    dat_id: *const c_char,
    fields_json: *const c_char,
    values_json: *const c_char,
    created: *const i64,
    deleted: bool,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            let spec = part_spec(spec_json)?;
            let dat_id = parts::uuid_arg(dat_id, "dat_id")?;
            let values = parts::field_values(fields_json, values_json)?;
            let created = parts::optional_i64(created);
            postgres_parts(unit, store, |parts| {
                parts
                    .write(&spec, dat_id, &values, created, deleted)
                    .map_err(|e| invalid(e.to_string()))
            })
        })
    }
}

/// Buffer the physical removal of every version of `dat_id`'s part in a
/// PostgreSQL store (committed ones too), at the commit.
///
/// # Safety
///
/// `unit` and `store` must be live handles; `spec_json` and `dat_id`
/// NUL-terminated strings; `error` null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_unit_postgres_erase_part(
    unit: *const RhizodatUnit,
    store: *const RhizodatPostgresStore,
    spec_json: *const c_char,
    dat_id: *const c_char,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            let spec = part_spec(spec_json)?;
            let dat_id = parts::uuid_arg(dat_id, "dat_id")?;
            postgres_parts(unit, store, |parts| {
                parts.erase(&spec, dat_id);
                Ok(())
            })
        })
    }
}

/// This unit of work's own latest version of `dat_id`'s part in a PostgreSQL
/// store: `{"touched": false}` when it did not touch it, else `{"touched":
/// true, "version": null | {...}}` (null: it erased it last; a version has no
/// `dat_txn` and no `dat_tx_time` yet).
///
/// # Safety
///
/// `unit` and `store` must be live handles; `spec_json` and `dat_id`
/// NUL-terminated strings; `out` writable for one pointer; `error` null or
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_unit_postgres_own_part(
    unit: *const RhizodatUnit,
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
            let spec = part_spec(spec_json)?;
            let dat_id = parts::uuid_arg(dat_id, "dat_id")?;
            let (unit, store) = (handle(unit, "unit")?, handle(store, "store")?.0.clone());
            let buffer = unit
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // A read: a store this unit has not written to stays out of it.
            let own = match buffer.get(store.name()) {
                Some(Pending::Postgres {
                    store: held, parts, ..
                }) if Arc::ptr_eq(held, &store) => {
                    parts.latest(spec.table(), dat_id).map(|version| {
                        version.map(|v| {
                            parts::version_json(None, None, v.created, v.deleted, &v.values)
                        })
                    })
                }
                Some(_) => {
                    return Err(invalid(format!(
                        "two stores named {:?} in one unit of work",
                        store.name()
                    )));
                }
                None => None,
            };
            *out = owned_string(parts::own_json(own));
            Ok(())
        })
    }
}

fn drevo_pending(store: &Arc<DrevoStore>) -> Pending {
    Pending::Drevo {
        store: store.clone(),
        writes: DrevoWrites::new(),
        parts: DrevoParts::new(),
    }
}

/// The parts buffered for a drevo store, made on first use.
///
/// # Safety
///
/// `unit` and `store` must be null or live handles.
unsafe fn drevo_parts<R>(
    unit: *const RhizodatUnit,
    store: *const RhizodatDrevoStore,
    with: impl FnOnce(&mut DrevoParts) -> Result<R, Failure>,
) -> Result<R, Failure> {
    // SAFETY: the caller's contract.
    let (unit, store) = unsafe { (handle(unit, "unit")?, handle(store, "store")?.0.clone()) };
    let mut buffer = unit
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let pending = buffer_of(
        &mut buffer,
        store.name(),
        Arc::as_ptr(&store).cast(),
        || drevo_pending(&store),
    )?;
    let Pending::Drevo { parts, .. } = pending else {
        unreachable!("a buffer holds its own store's kind")
    };
    with(parts)
}

/// Buffer a version of `dat_id`'s part in a drevo store, as
/// [`rhizodat_unit_postgres_write_part`] (two-phase: written at stage).
///
/// # Safety
///
/// `unit` and `store` must be live handles; `spec_json`, `dat_id`,
/// `fields_json` and `values_json` NUL-terminated strings; `created` null or
/// readable; `error` null or writable.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn rhizodat_unit_drevo_write_part(
    unit: *const RhizodatUnit,
    store: *const RhizodatDrevoStore,
    spec_json: *const c_char,
    dat_id: *const c_char,
    fields_json: *const c_char,
    values_json: *const c_char,
    created: *const i64,
    deleted: bool,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            let spec = drevo_spec(parts::spec(spec_json)?)?;
            let dat_id = parts::uuid_arg(dat_id, "dat_id")?;
            let values = parts::field_values(fields_json, values_json)?;
            let created = parts::optional_i64(created);
            drevo_parts(unit, store, |parts| {
                parts
                    .write(&spec, dat_id, &values, created, deleted)
                    .map_err(|e| invalid(e.to_string()))
            })
        })
    }
}

/// Buffer the removal of every version of `dat_id`'s part in a drevo store.
///
/// # Safety
///
/// `unit` and `store` must be live handles; `spec_json` and `dat_id`
/// NUL-terminated strings; `error` null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_unit_drevo_erase_part(
    unit: *const RhizodatUnit,
    store: *const RhizodatDrevoStore,
    spec_json: *const c_char,
    dat_id: *const c_char,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            let spec = drevo_spec(parts::spec(spec_json)?)?;
            let dat_id = parts::uuid_arg(dat_id, "dat_id")?;
            drevo_parts(unit, store, |parts| {
                parts.erase(&spec, dat_id);
                Ok(())
            })
        })
    }
}

/// This unit of work's own latest version of `dat_id`'s part in a drevo store,
/// as [`rhizodat_unit_postgres_own_part`].
///
/// # Safety
///
/// `unit` and `store` must be live handles; `spec_json` and `dat_id`
/// NUL-terminated strings; `out` writable for one pointer; `error` null or
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_unit_drevo_own_part(
    unit: *const RhizodatUnit,
    store: *const RhizodatDrevoStore,
    spec_json: *const c_char,
    dat_id: *const c_char,
    out: *mut *mut c_char,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            let out = writable(out, "out")?;
            let spec = drevo_spec(parts::spec(spec_json)?)?;
            let dat_id = parts::uuid_arg(dat_id, "dat_id")?;
            let (unit, store) = (handle(unit, "unit")?, handle(store, "store")?.0.clone());
            let buffer = unit
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // A read: a store this unit has not written to stays out of it.
            let own = match buffer.get(store.name()) {
                Some(Pending::Drevo {
                    store: held, parts, ..
                }) if Arc::ptr_eq(held, &store) => {
                    parts.latest(spec.label(), dat_id).map(|version| {
                        version.map(|v| {
                            parts::version_json(None, None, v.created, v.deleted, &v.values)
                        })
                    })
                }
                Some(_) => {
                    return Err(invalid(format!(
                        "two stores named {:?} in one unit of work",
                        store.name()
                    )));
                }
                None => None,
            };
            *out = owned_string(parts::own_json(own));
            Ok(())
        })
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
                || postgres_pending(&store),
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
                || drevo_pending(&store),
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

/// Buffer rows for `table` (`namespace.table`) of an Iceberg store, as
/// [`rhizodat_unit_postgres_insert`]. The rows become one Arrow batch whose
/// columns have their values' types; at stage they are matched to the table's
/// columns by name and cast to their types (text to dates, timestamps and
/// decimals; table columns the rows lack are null). Iceberg is one-phase:
/// data files are written at stage and appended after the decision.
///
/// # Safety
///
/// `unit` and `store` must be live handles; `table`, `columns_json` and
/// `rows_json` NUL-terminated strings; `error` null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_unit_iceberg_append(
    unit: *const RhizodatUnit,
    store: *const RhizodatIcebergStore,
    table: *const c_char,
    columns_json: *const c_char,
    rows_json: *const c_char,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            let unit = handle(unit, "unit")?;
            let id = store as usize;
            let store = handle(store, "store")?;
            let table = text(table, "table")?;
            let (columns, rows) = table_rows(
                text(columns_json, "columns_json")?,
                text(rows_json, "rows_json")?,
            )?;
            let batch = record_batch(&columns, &rows).map_err(invalid)?;
            let mut buffer = unit
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let pending = buffer_of(&mut buffer, &store.name, id as *const (), || {
                Pending::Iceberg {
                    id,
                    dsn: store.dsn.clone(),
                    name: store.name.clone(),
                    batches: IcebergBatches::new(),
                }
            })?;
            let Pending::Iceberg { batches, .. } = pending else {
                unreachable!("a buffer holds its own store's kind")
            };
            batches
                .append(table, batch)
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
