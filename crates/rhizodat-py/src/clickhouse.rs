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
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString};
use rhizodat_clickhouse::{ClickHouseBranch as CoreBranch, ClickHouseStore as CoreStore};
use rhizodat_core::rows::RowValue;
use rhizodat_core::store::{Branch, Participant};
use uuid::Uuid;

use crate::native::{ClickHouse, NativeStore};
use crate::{InvalidUnitOfWork, op_err, runtime};

/// One Python value as a [`RowValue`]. Scalars, bytes, UUIDs, lists and dicts
/// are carried exactly; temporal and decimal values travel as strings (the
/// server parses them per column type).
fn row_value(obj: &Bound<'_, PyAny>) -> PyResult<RowValue> {
    if obj.is_none() {
        return Ok(RowValue::Null);
    }
    if let Ok(flag) = obj.cast::<PyBool>() {
        return Ok(RowValue::Bool(flag.is_true()));
    }
    if let Ok(bytes) = obj.cast::<PyBytes>() {
        return Ok(RowValue::Bytes(bytes.as_bytes().to_vec()));
    }
    if obj.is_instance_of::<PyInt>() {
        return Ok(RowValue::Int(obj.extract::<i64>().map_err(|_| {
            InvalidUnitOfWork::new_err("integer out of range for a 64-bit column")
        })?));
    }
    if obj.is_instance_of::<PyFloat>() {
        return Ok(RowValue::Float(obj.extract::<f64>()?));
    }
    if let Ok(text) = obj.cast::<PyString>() {
        return Ok(RowValue::Str(text.to_cow()?.into_owned()));
    }
    if let Ok(uuid) = obj.extract::<Uuid>() {
        return Ok(RowValue::Uuid(uuid));
    }
    if let Ok(list) = obj.cast::<PyList>() {
        return Ok(RowValue::List(
            list.iter()
                .map(|v| row_value(&v))
                .collect::<PyResult<_>>()?,
        ));
    }
    if let Ok(dict) = obj.cast::<PyDict>() {
        let mut pairs = Vec::with_capacity(dict.len());
        for (key, value) in dict {
            pairs.push((
                key.cast::<PyString>()
                    .map_err(|_| InvalidUnitOfWork::new_err("a map key must be a string"))?
                    .to_cow()?
                    .into_owned(),
                row_value(&value)?,
            ));
        }
        return Ok(RowValue::Map(pairs));
    }
    Err(InvalidUnitOfWork::new_err(format!(
        "unsupported value type for a ClickHouse row: {}",
        obj.get_type().name()?
    )))
}

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
        }
    }
}

/// One Unit of Work's buffered rows for a ClickHouse store. Buffer with
/// [`ClickHouseUnit::insert`], then hand it to `Coordinator.commit` like a store.
/// A unit is spent by the commit and cannot be committed again.
#[pyclass(module = "rhizodat_native")]
pub struct ClickHouseUnit {
    store: Arc<CoreStore>,
    branch: Mutex<Option<CoreBranch>>,
}

/// A branch ready for the coordinator, with its store's participant and name.
pub(crate) type ReadyBranch = (Box<dyn Branch>, Arc<dyn Participant>, String);

impl ClickHouseUnit {
    /// Take the buffered branch, with the store's participant and name, for the
    /// coordinator; afterwards the unit is spent.
    pub(crate) fn take(&self) -> PyResult<ReadyBranch> {
        let branch = self
            .branch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or_else(|| {
                InvalidUnitOfWork::new_err("this ClickHouse unit was already committed")
            })?;
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
        let branch = slot.as_mut().ok_or_else(|| {
            InvalidUnitOfWork::new_err("this ClickHouse unit was already committed")
        })?;
        branch
            .insert(table, &column_refs, rows)
            .map_err(|e| InvalidUnitOfWork::new_err(e.to_string()))
    }
}
