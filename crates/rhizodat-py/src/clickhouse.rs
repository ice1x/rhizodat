//! The real Rust ClickHouse adapter behind the Python coordinator.
//!
//! [`ClickHouseStore`] wraps [`rhizodat_clickhouse::ClickHouseStore`] (its
//! one-phase branch and participant). A Python Unit of Work hands native
//! ClickHouse stores to [`crate::coordinator::Coordinator`] alongside any other
//! native store; the core applies the branch after the decision. ClickHouse has
//! no journal of its own, so a meta-transaction that includes it commits through
//! another store's journal (`PostgresJournal` / `DrevoJournal`).

use std::sync::Arc;

use pyo3::prelude::*;
use rhizodat_clickhouse::ClickHouseStore as CoreStore;

use crate::native::{ClickHouse, NativeStore};
use crate::{InvalidUnitOfWork, op_err, runtime};

/// One ClickHouse database as a rhizodat store, for a native Unit of Work.
#[pyclass(module = "rhizodat_native")]
pub struct ClickHouseStore(Arc<CoreStore>);

impl ClickHouseStore {
    /// The store as a [`NativeStore`] for the coordinator.
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
}
