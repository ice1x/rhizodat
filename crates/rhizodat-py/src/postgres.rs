//! The real Rust PostgreSQL adapter behind the Python coordinator.
//!
//! [`PostgresStore`] wraps [`rhizodat_postgres::PostgresStore`] (its branch and
//! participant) and [`PostgresJournal`] wraps the shared, durable journal. A
//! Python Unit of Work hands native stores to [`crate::coordinator::Coordinator`]
//! and the core runs the whole commit protocol against real PostgreSQL, with no
//! Python callbacks on the path. Each native call runs on its own current-thread
//! runtime ([`crate::runtime`]); a journal reconnects between calls as needed.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyDict;
use rhizodat_core::journal::Journal;
use rhizodat_postgres::{PostgresJournal as CoreJournal, PostgresStore as CoreStore};
use uuid::Uuid;

use crate::native::{NativeStore, Postgres};
use crate::{InvalidUnitOfWork, journal_record_dict, op_err, runtime};

/// One PostgreSQL database as a rhizodat store, for a native Unit of Work.
#[pyclass(module = "rhizodat_native")]
pub struct PostgresStore(Arc<CoreStore>);

impl PostgresStore {
    /// The store as a [`NativeStore`] for the coordinator.
    pub(crate) fn native(&self) -> Arc<dyn NativeStore> {
        Arc::new(Postgres(self.0.clone()))
    }
}

#[pymethods]
impl PostgresStore {
    /// A store named `name` over `dsn`, optionally confined to `schema` (so that
    /// no two stores share one schema's marker table).
    #[new]
    #[pyo3(signature = (dsn, name, schema = None))]
    fn new(dsn: &str, name: &str, schema: Option<&str>) -> PyResult<Self> {
        let store = CoreStore::new(dsn, name, schema)
            .map_err(|e| InvalidUnitOfWork::new_err(e.to_string()))?;
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

/// The journal in PostgreSQL: shared and durable, for Units of Work over several
/// stores and for recovery.
#[pyclass(module = "rhizodat_native")]
pub struct PostgresJournal(Arc<CoreJournal>);

impl PostgresJournal {
    /// The wrapped core journal as a trait object, for the coordinator.
    pub(crate) fn as_journal(&self) -> Arc<dyn Journal> {
        self.0.clone()
    }
}

#[pymethods]
impl PostgresJournal {
    /// Open (and create if needed) the journal table `table` in `schema` (or the
    /// DSN's `schema=` parameter).
    #[new]
    #[pyo3(signature = (dsn, schema = None, table = "dat_journal"))]
    fn new(py: Python<'_>, dsn: &str, schema: Option<&str>, table: &str) -> PyResult<Self> {
        let journal = py.detach(|| {
            runtime()
                .map_err(op_err)?
                .block_on(CoreJournal::open(dsn, schema, table))
                .map_err(op_err)
        })?;
        Ok(Self(Arc::new(journal)))
    }

    /// The record of `dat_txn` as a dict (`state`, `dat_txn`, `dat_tx_time`), or
    /// `None` when there is none.
    fn get<'py>(&self, py: Python<'py>, dat_txn: Uuid) -> PyResult<Option<Bound<'py, PyDict>>> {
        let journal = self.0.clone();
        let record = py.detach(|| {
            runtime()
                .map_err(op_err)?
                .block_on(journal.get(dat_txn))
                .map_err(op_err)
        })?;
        match record {
            Some(record) => Ok(Some(journal_record_dict(
                py,
                record.state.as_str(),
                record.dat_txn,
                record.dat_tx_time,
            )?)),
            None => Ok(None),
        }
    }
}
