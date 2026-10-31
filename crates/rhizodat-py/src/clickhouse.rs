//! The real Rust ClickHouse adapter behind the Python coordinator.
//!
//! [`ClickHouseStore`] wraps [`rhizodat_clickhouse::ClickHouseStore`] (its
//! one-phase branch and participant). A Python Unit of Work hands native
//! ClickHouse stores to [`crate::coordinator::Coordinator`] alongside any other
//! native store; the core applies the branch after the decision. ClickHouse has
//! no journal of its own, so a meta-transaction that includes it commits through
//! another store's journal (`PostgresJournal` / `DrevoJournal`).
//!
//! The Unit of Work's own rows cross the boundary through a [`ClickHouseUnit`]
//! (`store.unit()`): Python buffers rows into it with [`ClickHouseUnit::insert`]
//! and then commits it like a store. The rows are the core's row format
//! (`rhizodat_core::rows`), so a native ClickHouse commit writes real data, not
//! just the applied marker.

use std::sync::{Arc, Mutex};

use pyo3::prelude::*;
use pyo3::types::PyDict;
use rhizodat_clickhouse::parts::{
    ChColumn, ClickHousePartSpec as CoreSpec, ClickHouseParts, PartVersion, PendingVersion,
};
use rhizodat_clickhouse::{ClickHouseBranch as CoreBranch, ClickHouseStore as CoreStore};
use rhizodat_core::rows::RowValue;
use rhizodat_core::store::{Branch, Participant};
use uuid::Uuid;

use crate::native::{ClickHouse, NativeStore};
use crate::rows::{fields_dict, row_value};
use crate::{InvalidUnitOfWork, op_err, runtime};

/// One ClickHouse database as a rhizodat store, for a native Unit of Work.
#[pyclass(module = "rhizodat_native")]
pub struct ClickHouseStore(Arc<CoreStore>);

impl ClickHouseStore {
    /// The store as a [`NativeStore`] for the coordinator (a Unit of Work that
    /// writes no rows of its own).
    pub(crate) fn native(&self) -> Arc<dyn NativeStore> {
        Arc::new(ClickHouse(self.0.clone()))
    }
}

#[pymethods]
impl ClickHouseStore {
    /// A store named `name` over a ClickHouse DSN.
    #[new]
    fn new(dsn: &str, name: &str) -> PyResult<Self> {
        let store =
            CoreStore::new(dsn, name).map_err(|e| InvalidUnitOfWork::new_err(e.to_string()))?;
        Ok(Self(Arc::new(store)))
    }

    /// The store name.
    #[getter]
    fn name(&self) -> &str {
        self.0.name()
    }

    /// Create the service table `dat_applied` if it does not exist.
    fn install(&self, py: Python<'_>) -> PyResult<()> {
        let store = self.0.clone();
        py.detach(|| {
            let runtime = runtime().map_err(op_err)?;
            runtime.block_on(store.install()).map_err(op_err)
        })
    }

    /// A Unit of Work's branch of this store, to buffer rows and then commit.
    fn unit(&self) -> ClickHouseUnit {
        ClickHouseUnit {
            store: self.0.clone(),
            branch: Mutex::new(Some(self.0.branch())),
            parts: Mutex::new(ClickHouseParts::new()),
        }
    }

    /// Create the part's table if missing. Idempotent.
    fn install_part(&self, py: Python<'_>, spec: &ClickHousePartSpec) -> PyResult<()> {
        let (store, spec) = (self.0.clone(), spec.0.clone());
        py.detach(|| {
            let runtime = runtime().map_err(op_err)?;
            runtime.block_on(store.install_part(&spec)).map_err(op_err)
        })
    }

    /// The latest committed version of `dat_id`'s part with
    /// `dat_tx_time <= known_at` (any, if `None`), as a dict, or `None`.
    #[pyo3(signature = (spec, dat_id, *, known_at = None))]
    fn read_part<'py>(
        &self,
        py: Python<'py>,
        spec: &ClickHousePartSpec,
        dat_id: Uuid,
        known_at: Option<i64>,
    ) -> PyResult<Option<Bound<'py, PyDict>>> {
        let (store, core) = (self.0.clone(), spec.0.clone());
        let version = py.detach(|| {
            let runtime = runtime().map_err(op_err)?;
            runtime
                .block_on(store.read_part(&core, dat_id, known_at))
                .map_err(op_err)
        })?;
        version.map(|v| version_dict(py, &core, &v)).transpose()
    }

    /// Every committed version of `dat_id`'s part, oldest first.
    fn part_history<'py>(
        &self,
        py: Python<'py>,
        spec: &ClickHousePartSpec,
        dat_id: Uuid,
    ) -> PyResult<Vec<Bound<'py, PyDict>>> {
        let (store, core) = (self.0.clone(), spec.0.clone());
        let versions = py.detach(|| {
            let runtime = runtime().map_err(op_err)?;
            runtime
                .block_on(store.part_history(&core, dat_id))
                .map_err(op_err)
        })?;
        versions
            .iter()
            .map(|v| version_dict(py, &core, v))
            .collect()
    }

    /// `(dat_id, version)` for every committed version of every entity with
    /// `dat_tx_time <= known_at` (all, if `None`), oldest first.
    #[pyo3(signature = (spec, *, known_at = None))]
    fn part_versions<'py>(
        &self,
        py: Python<'py>,
        spec: &ClickHousePartSpec,
        known_at: Option<i64>,
    ) -> PyResult<Vec<(Uuid, Bound<'py, PyDict>)>> {
        let (store, core) = (self.0.clone(), spec.0.clone());
        let versions = py.detach(|| {
            let runtime = runtime().map_err(op_err)?;
            runtime
                .block_on(store.part_versions(&core, known_at))
                .map_err(op_err)
        })?;
        versions
            .iter()
            .map(|(id, v)| Ok((*id, version_dict(py, &core, v)?)))
            .collect()
    }

    /// Entities whose latest version is a soft delete older than `deleted_before`.
    #[pyo3(signature = (spec, *, deleted_before, limit))]
    fn expired_parts(
        &self,
        py: Python<'_>,
        spec: &ClickHousePartSpec,
        deleted_before: i64,
        limit: i64,
    ) -> PyResult<Vec<Uuid>> {
        let (store, core) = (self.0.clone(), spec.0.clone());
        py.detach(|| {
            let runtime = runtime().map_err(op_err)?;
            runtime
                .block_on(store.expired_parts(&core, deleted_before, limit))
                .map_err(op_err)
        })
    }
}

/// One ClickHouse store's share of one entity type:
/// `ClickHousePartSpec(type_name, table, columns)` with `columns` the
/// `(field, type)` pairs in order: `String`, `Bool`, `Int64`, `Float64`,
/// `DateTime64(6, 'UTC')`, `Date32`, `UUID`, or `json` (tagged JSON in a `String`).
#[pyclass(module = "rhizodat_native", frozen)]
pub struct ClickHousePartSpec(CoreSpec);

#[pymethods]
impl ClickHousePartSpec {
    #[new]
    fn new(type_name: &str, table: &str, columns: Vec<(String, String)>) -> PyResult<Self> {
        let columns = columns
            .into_iter()
            .map(|(name, ty)| Ok((name, ty.parse::<ChColumn>()?)))
            .collect::<Result<Vec<_>, rhizodat_clickhouse::ClickHouseError>>()
            .map_err(|e| InvalidUnitOfWork::new_err(e.to_string()))?;
        CoreSpec::new(type_name, table, columns)
            .map(Self)
            .map_err(|e| InvalidUnitOfWork::new_err(e.to_string()))
    }

    /// The table.
    #[getter]
    fn table(&self) -> &str {
        self.0.table()
    }

    /// The fields, in order.
    #[getter]
    fn fields(&self) -> Vec<String> {
        self.0.fields().map(str::to_owned).collect()
    }
}

/// A committed version as `{dat_txn, dat_tx_time, created, deleted, values}`.
fn version_dict<'py>(
    py: Python<'py>,
    spec: &CoreSpec,
    version: &PartVersion,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("dat_txn", version.dat_txn)?;
    dict.set_item("dat_tx_time", version.dat_tx_time)?;
    dict.set_item("created", version.created)?;
    dict.set_item("deleted", version.deleted)?;
    dict.set_item("values", fields_dict(py, spec.fields(), &version.values)?)?;
    Ok(dict)
}

fn pending_dict<'py>(
    py: Python<'py>,
    spec: &CoreSpec,
    version: &PendingVersion,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("created", version.created)?;
    dict.set_item("deleted", version.deleted)?;
    dict.set_item("values", fields_dict(py, spec.fields(), &version.values)?)?;
    Ok(dict)
}

fn spent() -> PyErr {
    InvalidUnitOfWork::new_err("this ClickHouse unit was already committed")
}

/// One Unit of Work's buffered rows for a ClickHouse store. Buffer with
/// [`ClickHouseUnit::insert`], then hand it to `Coordinator.commit` like a store.
/// A unit is spent by the commit and cannot be committed again.
#[pyclass(module = "rhizodat_native")]
pub struct ClickHouseUnit {
    store: Arc<CoreStore>,
    branch: Mutex<Option<CoreBranch>>,
    parts: Mutex<ClickHouseParts>,
}

/// A branch ready for the coordinator, with its store's participant and name.
pub(crate) type ReadyBranch = (Box<dyn Branch>, Arc<dyn Participant>, String);

impl ClickHouseUnit {
    /// Take the buffered branch, with the store's participant and name, for the
    /// coordinator; afterwards the unit is spent.
    pub(crate) fn take(&self) -> PyResult<ReadyBranch> {
        let mut branch = self
            .branch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or_else(spent)?;
        branch.load_parts(
            self.parts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        );
        Ok((
            Box::new(branch),
            self.store.participant(),
            self.store.name().to_owned(),
        ))
    }
}

#[pymethods]
impl ClickHouseUnit {
    /// Buffer `rows` (one value per column of `columns`) for `table`.
    // PyO3 extracts method arguments by value.
    #[allow(clippy::needless_pass_by_value)]
    fn insert(
        &self,
        py: Python<'_>,
        table: &str,
        columns: Vec<String>,
        rows: Vec<Vec<Py<PyAny>>>,
    ) -> PyResult<()> {
        let rows: Vec<Vec<RowValue>> = rows
            .into_iter()
            .map(|row| row.iter().map(|v| row_value(v.bind(py))).collect())
            .collect::<PyResult<_>>()?;
        let column_refs: Vec<&str> = columns.iter().map(String::as_str).collect();
        let mut slot = self
            .branch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let branch = slot.as_mut().ok_or_else(spent)?;
        branch
            .insert(table, &column_refs, rows)
            .map_err(|e| InvalidUnitOfWork::new_err(e.to_string()))
    }

    /// Set this unit's version of `dat_id`'s part (the last write wins): `values`
    /// by field (missing ones are `NULL`), `created` the time of the first
    /// version (`None`: this one).
    #[pyo3(signature = (spec, dat_id, values, *, created, deleted = false))]
    fn write_part(
        &self,
        spec: &ClickHousePartSpec,
        dat_id: Uuid,
        values: &Bound<'_, PyDict>,
        created: Option<i64>,
        deleted: bool,
    ) -> PyResult<()> {
        self.check_open()?;
        let mut named = Vec::with_capacity(values.len());
        for (key, value) in values {
            named.push((key.extract::<String>()?, row_value(&value)?));
        }
        self.parts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .write(&spec.0, dat_id, &named, created, deleted)
            .map_err(|e| InvalidUnitOfWork::new_err(e.to_string()))
    }

    /// Buffer the erase of every earlier version of `dat_id`'s part.
    fn erase_part(&self, spec: &ClickHousePartSpec, dat_id: Uuid) -> PyResult<()> {
        self.check_open()?;
        self.parts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .erase(&spec.0, dat_id);
        Ok(())
    }

    /// This unit's own version of `dat_id`'s part: `(False, None)` when it did
    /// not touch it, `(True, None)` when it erased it (and wrote nothing since),
    /// else `(True, {created, deleted, values})`.
    fn own_part<'py>(
        &self,
        py: Python<'py>,
        spec: &ClickHousePartSpec,
        dat_id: Uuid,
    ) -> PyResult<(bool, Option<Bound<'py, PyDict>>)> {
        let parts = self
            .parts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(match parts.latest(spec.0.table(), dat_id) {
            None => (false, None),
            Some(None) => (true, None),
            Some(Some(version)) => (true, Some(pending_dict(py, &spec.0, version)?)),
        })
    }
}

impl ClickHouseUnit {
    fn check_open(&self) -> PyResult<()> {
        let open = self
            .branch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some();
        if open { Ok(()) } else { Err(spent()) }
    }
}
