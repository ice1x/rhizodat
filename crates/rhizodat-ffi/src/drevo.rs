//! drevo through the C ABI: a graph store (its own Cypher outside any
//! meta-transaction, and the target of a unit of work's statements, two-phase)
//! and the journal kept in a graph.

use std::ffi::c_char;
use std::sync::Arc;

use rhizodat_core::rows::RowValue;
use rhizodat_drevo::parts::{DrevoPartSpec, FieldKind, PartVersion};
use rhizodat_drevo::{ConfigError, DrevoJournal, DrevoStore, Params, Value};

use crate::postgres::RhizodatJournal;
use crate::{
    Failure, RhizodatStatus, boundary, free_handle, handle, into_handle, optional_text,
    owned_string, parts, rows_json, runtime, text, writable,
};

/// One drevo graph as a rhizodat store.
pub struct RhizodatDrevoStore(pub(crate) Arc<DrevoStore>);

fn invalid(message: impl Into<String>) -> Failure {
    (RhizodatStatus::InvalidArgument, message.into())
}

fn cypher_value(name: &str, value: RowValue) -> Result<Value, Failure> {
    Ok(match value {
        RowValue::Null => Value::Null,
        RowValue::Bool(v) => Value::Bool(v),
        RowValue::Int(v) => Value::Int(v),
        RowValue::Float(v) => Value::Float(v),
        RowValue::Str(v) => Value::Str(v),
        RowValue::Uuid(v) => Value::Str(v.to_string()),
        RowValue::List(items) => Value::List(
            items
                .into_iter()
                .map(|v| cypher_value(name, v))
                .collect::<Result<_, _>>()?,
        ),
        other => {
            return Err(invalid(format!(
                "cannot bind {name:?}: {other:?} is not a string, a number, a boolean, a UUID, \
                 a list or null"
            )));
        }
    })
}

/// `params_json` (a JSON object of tagged values, or null) as Cypher parameters.
pub(crate) fn cypher_params(json: Option<&str>) -> Result<Params, Failure> {
    let Some(json) = json else {
        return Ok(Params::new());
    };
    let object: serde_json::Map<String, serde_json::Value> = serde_json::from_str(json)
        .map_err(|e| invalid(format!("params_json must be a JSON object: {e}")))?;
    object
        .into_iter()
        .map(|(name, raw)| {
            let value = rhizodat_core::rows::decode_value(&raw.to_string())
                .map_err(|e| invalid(format!("{name}: {e}")))?;
            Ok((name.clone(), cypher_value(&name, value)?))
        })
        .collect()
}

fn row_value(value: &Value) -> RowValue {
    match value {
        Value::Null => RowValue::Null,
        Value::Bool(v) => RowValue::Bool(*v),
        Value::Int(v) => RowValue::Int(*v),
        Value::Float(v) => RowValue::Float(*v),
        Value::Str(v) => RowValue::Str(v.clone()),
        Value::List(items) => RowValue::List(items.iter().map(row_value).collect()),
    }
}

/// A graph store named `name` over a drevo DSN (`drevo+bolt://…`, or an
/// embedded `drevo+file://…` when the library is built with it).
///
/// # Safety
///
/// `dsn` and `name` must be NUL-terminated strings; `out` writable for one
/// pointer; `error` null or writable. Free with [`rhizodat_drevo_store_free`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_drevo_store_open(
    dsn: *const c_char,
    name: *const c_char,
    out: *mut *mut RhizodatDrevoStore,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            let out = writable(out, "out")?;
            let store = DrevoStore::new(text(dsn, "dsn")?, text(name, "name")?)
                .map_err(|e| invalid(e.to_string()))?;
            *out = into_handle(RhizodatDrevoStore(Arc::new(store)));
            Ok(())
        })
    }
}

/// Run one Cypher statement outside any meta-transaction, with `params_json`
/// (a JSON object of tagged values, or null) as its parameters. A write fails
/// retryably while a branch of this graph is prepared. The result is
/// `{"columns", "rows"}`, columns in name order (the graph's rows are maps).
///
/// # Safety
///
/// `store` must be a live store handle; `cypher` a NUL-terminated string,
/// `params_json` too or null; `out` writable for one pointer; `error` null or
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_drevo_store_execute(
    store: *const RhizodatDrevoStore,
    cypher: *const c_char,
    params_json: *const c_char,
    out: *mut *mut c_char,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            let out = writable(out, "out")?;
            let store = handle(store, "store")?.0.clone();
            let cypher = text(cypher, "cypher")?;
            let params = cypher_params(optional_text(params_json, "params_json")?)?;
            let rows = runtime()?
                .block_on(store.write(cypher, params))
                .map_err(|e| (RhizodatStatus::Failed, e.to_string()))?;
            let columns: Vec<String> = rows
                .first()
                .map(|r| r.0.keys().cloned().collect())
                .unwrap_or_default();
            let values = rows
                .iter()
                .map(|row| columns.iter().map(|c| row_value(row.value(c))).collect())
                .collect();
            *out = owned_string(rows_json(&columns, values)?);
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
pub unsafe extern "C" fn rhizodat_drevo_store_free(store: *mut RhizodatDrevoStore) {
    // SAFETY: the caller's contract.
    unsafe { free_handle(store) }
}

/// The journal in the graph of `dsn`, its records under `label` (null:
/// `DatJournal`). The graph must hold no store's data: a journal write would
/// outdate the transaction of that store's branch. Free with
/// [`crate::rhizodat_journal_free`].
///
/// # Safety
///
/// `dsn` must be a NUL-terminated string, `label` too or null; `out` writable
/// for one pointer; `error` null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_drevo_journal_open(
    dsn: *const c_char,
    label: *const c_char,
    out: *mut *mut RhizodatJournal,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            let out = writable(out, "out")?;
            let label = optional_text(label, "label")?.unwrap_or("DatJournal");
            let journal =
                DrevoJournal::new(text(dsn, "dsn")?, label).map_err(|e| invalid(e.to_string()))?;
            *out = into_handle(RhizodatJournal(Arc::new(journal)));
            Ok(())
        })
    }
}

// --- entity parts ----------------------------------------------------------------------

fn drevo_version(version: &PartVersion) -> String {
    parts::version_json(
        Some(version.dat_txn),
        version.dat_tx_time,
        version.created,
        version.deleted,
        &version.values,
    )
}

fn failed(error: impl std::fmt::Display) -> Failure {
    (RhizodatStatus::Failed, error.to_string())
}

/// `spec_json` as a drevo part spec: the `table` is the versions' label, and
/// each column `[field, kind]` or `[field, kind, is a reference]` with the kind
/// `native` (strings, numbers, booleans), `uuid` (its text) or `json` (tagged
/// JSON).
pub(crate) fn drevo_spec(spec: parts::Spec) -> Result<DrevoPartSpec, Failure> {
    let fields = spec
        .columns
        .into_iter()
        .map(|column| {
            let (name, kind, reference) = column.into_parts();
            Ok((name, kind.parse::<FieldKind>()?, reference))
        })
        .collect::<Result<Vec<_>, ConfigError>>()
        .map_err(|e| invalid(e.to_string()))?;
    DrevoPartSpec::new(&spec.type_name, &spec.table, fields).map_err(|e| invalid(e.to_string()))
}

impl parts::PartReads for DrevoStore {
    type Spec = DrevoPartSpec;

    fn spec(spec: parts::Spec) -> Result<DrevoPartSpec, Failure> {
        drevo_spec(spec)
    }

    async fn install(&self, spec: &DrevoPartSpec) -> Result<(), Failure> {
        self.install_part(spec).await.map_err(failed)
    }

    async fn read(
        &self,
        spec: &DrevoPartSpec,
        dat_id: uuid::Uuid,
        known_at: Option<i64>,
    ) -> Result<Option<String>, Failure> {
        let version = self
            .read_part(spec, dat_id, known_at)
            .await
            .map_err(failed)?;
        Ok(version.as_ref().map(drevo_version))
    }

    async fn history(
        &self,
        spec: &DrevoPartSpec,
        dat_id: uuid::Uuid,
    ) -> Result<Vec<String>, Failure> {
        let versions = self.part_history(spec, dat_id).await.map_err(failed)?;
        Ok(versions.iter().map(drevo_version).collect())
    }

    async fn versions(
        &self,
        spec: &DrevoPartSpec,
        known_at: Option<i64>,
    ) -> Result<Vec<(uuid::Uuid, String)>, Failure> {
        let versions = self.part_versions(spec, known_at).await.map_err(failed)?;
        Ok(versions
            .iter()
            .map(|(id, v)| (*id, drevo_version(v)))
            .collect())
    }

    async fn expired(
        &self,
        spec: &DrevoPartSpec,
        deleted_before: i64,
        limit: i64,
    ) -> Result<Vec<uuid::Uuid>, Failure> {
        self.expired_parts(spec, deleted_before, limit)
            .await
            .map_err(failed)
    }
}

/// Create the index the part's versions need if missing. Idempotent.
///
/// # Safety
///
/// `store` must be a live store handle; `spec_json` a NUL-terminated string;
/// `error` null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_drevo_store_install_part(
    store: *const RhizodatDrevoStore,
    spec_json: *const c_char,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            parts::install(handle(store, "store")?.0.as_ref(), spec_json)
        })
    }
}

/// The latest committed version of `dat_id`'s part known at `*known_at` (any,
/// when null), as JSON, or `null`.
///
/// # Safety
///
/// `store` must be a live store handle; `spec_json` and `dat_id`
/// NUL-terminated strings; `known_at` null or readable; `out` writable for one
/// pointer; `error` null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_drevo_store_read_part(
    store: *const RhizodatDrevoStore,
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
            let json = parts::read(
                handle(store, "store")?.0.as_ref(),
                spec_json,
                dat_id,
                known_at,
            )?;
            *out = owned_string(json);
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
pub unsafe extern "C" fn rhizodat_drevo_store_part_history(
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
            *out = owned_string(parts::history(
                handle(store, "store")?.0.as_ref(),
                spec_json,
                dat_id,
            )?);
            Ok(())
        })
    }
}

/// Every committed version of every entity's part known at `*known_at` (all,
/// when null), oldest first, as a JSON array of versions with their `dat_id`.
///
/// # Safety
///
/// `store` must be a live store handle; `spec_json` a NUL-terminated string;
/// `known_at` null or readable; `out` writable for one pointer; `error` null or
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_drevo_store_part_versions(
    store: *const RhizodatDrevoStore,
    spec_json: *const c_char,
    known_at: *const i64,
    out: *mut *mut c_char,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            let out = writable(out, "out")?;
            *out = owned_string(parts::versions(
                handle(store, "store")?.0.as_ref(),
                spec_json,
                known_at,
            )?);
            Ok(())
        })
    }
}

/// The entities whose latest version is a soft delete older than
/// `deleted_before`, at most `limit`, as a JSON array of ids.
///
/// # Safety
///
/// `store` must be a live store handle; `spec_json` a NUL-terminated string;
/// `out` writable for one pointer; `error` null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_drevo_store_expired_parts(
    store: *const RhizodatDrevoStore,
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
            let ids = parts::expired(
                handle(store, "store")?.0.as_ref(),
                spec_json,
                deleted_before,
                limit,
            )?;
            *out = owned_string(ids);
            Ok(())
        })
    }
}
