//! Iceberg through the C ABI: a catalog as a store — its schema operations,
//! its scans, and the target of a unit of work's rows (one-phase: data files
//! written at stage, appended after the decision).
//!
//! A REST catalog's HTTP client is bound to the runtime that made it, so the
//! store keeps its DSN and connects on each call's own runtime (and the commit
//! connects on its own), as the native Python facade does.

use std::ffi::c_char;
use std::sync::Arc;

use rhizodat_iceberg::filter::parse_filter;
use rhizodat_iceberg::parts::{FieldKind, IcebergPartSpec, PartVersion};
use rhizodat_iceberg::{IcebergError, IcebergStore, Scan};
use serde_json::Value as Json;

use crate::arrow::batch_rows;
use crate::{
    Failure, RhizodatStatus, boundary, free_handle, handle, into_handle, optional_text,
    owned_string, parts, rows_json, runtime, text, writable,
};

/// One Iceberg catalog as a rhizodat store.
pub struct RhizodatIcebergStore {
    pub(crate) dsn: Arc<str>,
    pub(crate) name: Arc<str>,
}

fn invalid(message: impl Into<String>) -> Failure {
    (RhizodatStatus::InvalidArgument, message.into())
}

fn failed(error: impl std::fmt::Display) -> Failure {
    (RhizodatStatus::Failed, error.to_string())
}

/// The scan described by `options_json`: `{"columns": [...], "limit": n,
/// "snapshot_id": n, "filter": "<pyiceberg row filter>"}`, each optional.
fn scan_options(json: Option<&str>) -> Result<Scan, Failure> {
    let mut scan = Scan::default();
    let Some(json) = json else {
        return Ok(scan);
    };
    let options: serde_json::Map<String, Json> = serde_json::from_str(json)
        .map_err(|e| invalid(format!("options_json must be a JSON object: {e}")))?;
    for (key, value) in options {
        match key.as_str() {
            "columns" => {
                scan.columns = Some(
                    serde_json::from_value(value)
                        .map_err(|_| invalid("columns must be a list of names"))?,
                );
            }
            "limit" => {
                scan.limit = Some(
                    value
                        .as_u64()
                        .and_then(|n| usize::try_from(n).ok())
                        .ok_or_else(|| invalid("limit must be a count"))?,
                );
            }
            "snapshot_id" => {
                scan.snapshot_id = Some(
                    value
                        .as_i64()
                        .ok_or_else(|| invalid("snapshot_id must be a number"))?,
                );
            }
            "filter" => {
                let text = value.as_str().ok_or_else(|| {
                    invalid("filter must be the filter's text (pyiceberg's syntax)")
                })?;
                scan.filter =
                    Some(parse_filter(text).map_err(|e| invalid(format!("filter {text:?}: {e}")))?);
            }
            other => {
                return Err(invalid(format!(
                    "unknown scan option {other:?}: columns, limit, snapshot_id, filter"
                )));
            }
        }
    }
    Ok(scan)
}

/// A store named `name` over an Iceberg DSN (`iceberg+rest://…` or
/// `iceberg+glue://…`). It connects on each call.
///
/// # Safety
///
/// `dsn` and `name` must be NUL-terminated strings; `out` writable for one
/// pointer; `error` null or writable. Free with [`rhizodat_iceberg_store_free`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_iceberg_store_open(
    dsn: *const c_char,
    name: *const c_char,
    out: *mut *mut RhizodatIcebergStore,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            let out = writable(out, "out")?;
            let (dsn, name) = (text(dsn, "dsn")?, text(name, "name")?);
            rhizodat_iceberg::catalog_properties(dsn).map_err(|e| invalid(e.to_string()))?;
            *out = into_handle(RhizodatIcebergStore {
                dsn: dsn.into(),
                name: name.into(),
            });
            Ok(())
        })
    }
}

/// Run the operations of an `iceberg` migration step (a JSON list): the
/// Rust port has `create_namespace` and `create_table`, each a no-op when its
/// result is there.
///
/// # Safety
///
/// `store` must be a live store handle; `operations_json` a NUL-terminated
/// string; `error` null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_iceberg_store_run_operations(
    store: *const RhizodatIcebergStore,
    operations_json: *const c_char,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            let store = handle(store, "store")?;
            let body: Json = serde_json::from_str(text(operations_json, "operations_json")?)
                .map_err(|e| invalid(format!("operations_json: {e}")))?;
            rhizodat_iceberg::schema::parse_operations(&body)
                .map_err(|e| invalid(e.to_string()))?;
            let (dsn, name) = (store.dsn.clone(), store.name.clone());
            runtime()?
                .block_on(async {
                    let lake = IcebergStore::connect(&dsn, &name).await?;
                    lake.run_operations(&body).await
                })
                .map_err(failed)
        })
    }
}

/// Scan `table` (`namespace.table`) outside any meta-transaction with
/// `options_json` (null, or `{"columns", "limit", "snapshot_id", "filter"}`).
/// The result is `{"columns", "rows"}` with tagged values.
///
/// # Safety
///
/// `store` must be a live store handle; `table` a NUL-terminated string,
/// `options_json` too or null; `out` writable for one pointer; `error` null or
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_iceberg_store_scan(
    store: *const RhizodatIcebergStore,
    table: *const c_char,
    options_json: *const c_char,
    out: *mut *mut c_char,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            let out = writable(out, "out")?;
            let store = handle(store, "store")?;
            let table = text(table, "table")?;
            if table.split('.').filter(|p| !p.is_empty()).count() < 2 {
                return Err(invalid(format!(
                    "invalid table name {table:?}: namespace.table"
                )));
            }
            let scan = scan_options(optional_text(options_json, "options_json")?)?;
            let (dsn, name) = (store.dsn.clone(), store.name.clone());
            let batches = runtime()?
                .block_on(async {
                    let lake = IcebergStore::connect(&dsn, &name).await?;
                    lake.scan_with(table, &scan).await
                })
                .map_err(failed)?;
            let columns: Vec<String> = batches
                .first()
                .map(|b| {
                    b.schema()
                        .fields()
                        .iter()
                        .map(|f| f.name().clone())
                        .collect()
                })
                .unwrap_or_default();
            *out = owned_string(rows_json(&columns, batch_rows(&batches).map_err(failed)?)?);
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
pub unsafe extern "C" fn rhizodat_iceberg_store_free(store: *mut RhizodatIcebergStore) {
    // SAFETY: the caller's contract.
    unsafe { free_handle(store) }
}

// --- entity parts ----------------------------------------------------------------------

fn iceberg_version(version: &PartVersion) -> String {
    parts::version_json(
        Some(version.dat_txn),
        version.dat_tx_time,
        version.created,
        version.deleted,
        &version.values,
    )
}

/// `spec_json` as an Iceberg part spec: the `table` is `namespace.table`, each
/// column `[field, kind]` with the kind `string`, `boolean`, `long`, `double`,
/// `binary`, `timestamptz`, `date`, `id` (a UUID as text) or `json` (tagged
/// JSON text).
pub(crate) fn iceberg_spec(spec: parts::Spec) -> Result<IcebergPartSpec, Failure> {
    let fields = spec
        .columns
        .into_iter()
        .map(|column| {
            let (name, kind, _) = column.into_parts();
            Ok((name, kind.parse::<FieldKind>()?))
        })
        .collect::<Result<Vec<_>, IcebergError>>()
        .map_err(|e| invalid(e.to_string()))?;
    IcebergPartSpec::new(&spec.type_name, &spec.table, fields).map_err(|e| invalid(e.to_string()))
}

impl RhizodatIcebergStore {
    async fn lake(&self) -> Result<IcebergStore, Failure> {
        IcebergStore::connect(&self.dsn, &self.name)
            .await
            .map_err(failed)
    }
}

impl parts::PartReads for RhizodatIcebergStore {
    type Spec = IcebergPartSpec;

    fn spec(spec: parts::Spec) -> Result<IcebergPartSpec, Failure> {
        iceberg_spec(spec)
    }

    async fn install(&self, spec: &IcebergPartSpec) -> Result<(), Failure> {
        self.lake().await?.install_part(spec).await.map_err(failed)
    }

    async fn read(
        &self,
        spec: &IcebergPartSpec,
        dat_id: uuid::Uuid,
        known_at: Option<i64>,
    ) -> Result<Option<String>, Failure> {
        let version = self
            .lake()
            .await?
            .read_part(spec, dat_id, known_at)
            .await
            .map_err(failed)?;
        Ok(version.as_ref().map(iceberg_version))
    }

    async fn history(
        &self,
        spec: &IcebergPartSpec,
        dat_id: uuid::Uuid,
    ) -> Result<Vec<String>, Failure> {
        let versions = self
            .lake()
            .await?
            .part_history(spec, dat_id)
            .await
            .map_err(failed)?;
        Ok(versions.iter().map(iceberg_version).collect())
    }

    async fn versions(
        &self,
        spec: &IcebergPartSpec,
        known_at: Option<i64>,
    ) -> Result<Vec<(uuid::Uuid, String)>, Failure> {
        let versions = self
            .lake()
            .await?
            .part_versions(spec, known_at)
            .await
            .map_err(failed)?;
        Ok(versions
            .iter()
            .map(|(id, v)| (*id, iceberg_version(v)))
            .collect())
    }

    async fn expired(
        &self,
        spec: &IcebergPartSpec,
        deleted_before: i64,
        limit: i64,
    ) -> Result<Vec<uuid::Uuid>, Failure> {
        self.lake()
            .await?
            .expired_parts(spec, deleted_before, limit)
            .await
            .map_err(failed)
    }
}

/// Create the part's namespace and table if missing. Idempotent.
///
/// # Safety
///
/// `store` must be a live store handle; `spec_json` a NUL-terminated string;
/// `error` null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_iceberg_store_install_part(
    store: *const RhizodatIcebergStore,
    spec_json: *const c_char,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe { boundary(error, || parts::install(handle(store, "store")?, spec_json)) }
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
pub unsafe extern "C" fn rhizodat_iceberg_store_read_part(
    store: *const RhizodatIcebergStore,
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
            let json = parts::read(handle(store, "store")?, spec_json, dat_id, known_at)?;
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
pub unsafe extern "C" fn rhizodat_iceberg_store_part_history(
    store: *const RhizodatIcebergStore,
    spec_json: *const c_char,
    dat_id: *const c_char,
    out: *mut *mut c_char,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract.
    unsafe {
        boundary(error, || {
            let out = writable(out, "out")?;
            *out = owned_string(parts::history(handle(store, "store")?, spec_json, dat_id)?);
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
pub unsafe extern "C" fn rhizodat_iceberg_store_part_versions(
    store: *const RhizodatIcebergStore,
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
                handle(store, "store")?,
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
pub unsafe extern "C" fn rhizodat_iceberg_store_expired_parts(
    store: *const RhizodatIcebergStore,
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
            let ids = parts::expired(handle(store, "store")?, spec_json, deleted_before, limit)?;
            *out = owned_string(ids);
            Ok(())
        })
    }
}
