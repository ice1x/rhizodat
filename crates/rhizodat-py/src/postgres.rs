//! The real Rust PostgreSQL adapter behind the Python coordinator.
//!
//! [`PostgresStore`] wraps [`rhizodat_postgres::PostgresStore`] (its branch and
//! participant) and [`PostgresJournal`] wraps the shared, durable journal. A
//! Python Unit of Work hands native stores to [`crate::coordinator::Coordinator`]
//! and the core runs the whole commit protocol against real PostgreSQL, with no
//! Python callbacks on the path. Each native call runs on its own current-thread
//! runtime ([`crate::runtime`]); a journal reconnects between calls as needed.

use std::sync::{Arc, Mutex};

use pyo3::prelude::*;
use pyo3::types::PyDict;
use rhizodat_core::journal::Journal;
use rhizodat_postgres::{
    PostgresJournal as CoreJournal, PostgresRows as CoreRows, PostgresStore as CoreStore,
};
use uuid::Uuid;

use crate::native::{NativeStore, Postgres, PostgresPending};
use crate::rows::row_value;
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

    /// A Unit of Work's branch of this store, to buffer rows and then commit.
    fn unit(&self) -> PostgresUnit {
        PostgresUnit {
            store: self.0.clone(),
            rows: Mutex::new(CoreRows::new()),
        }
    }
}

/// One Unit of Work's buffered rows for a PostgreSQL store. Buffer with
/// [`PostgresUnit::insert`], then hand it to `Coordinator.commit` like a store:
/// its rows are written inside the branch's transaction, so they commit
/// atomically with the meta-transaction through the two-phase protocol. The unit
/// may be reused and keeps its rows across a retry of the commit.
#[pyclass(module = "rhizodat_native")]
pub struct PostgresUnit {
    store: Arc<CoreStore>,
    rows: Mutex<CoreRows>,
}

impl PostgresUnit {
    /// A native store carrying a snapshot of the buffered rows, for the
    /// coordinator; the unit itself is left intact.
    pub(crate) fn pending(&self) -> Arc<dyn NativeStore> {
        let rows = self
            .rows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        Arc::new(PostgresPending {
            store: self.store.clone(),
            rows,
        })
    }
}

#[pymethods]
impl PostgresUnit {
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
        let rows: Vec<Vec<_>> = rows
            .into_iter()
            .map(|row| row.iter().map(|v| row_value(v.bind(py))).collect())
            .collect::<PyResult<_>>()?;
        let column_refs: Vec<&str> = columns.iter().map(String::as_str).collect();
        self.rows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(table, &column_refs, rows)
            .map_err(|e| InvalidUnitOfWork::new_err(e.to_string()))
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
