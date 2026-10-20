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
use rhizodat_clickhouse::{ClickHouseBranch as CoreBranch, ClickHouseStore as CoreStore};
use rhizodat_core::rows::RowValue;
use rhizodat_core::store::{Branch, Participant};

use crate::native::{ClickHouse, NativeStore};
use crate::rows::row_value;
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
