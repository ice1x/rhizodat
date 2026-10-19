//! The real Rust drevo adapter behind the Python coordinator.
//!
//! [`DrevoStore`] wraps [`rhizodat_drevo::DrevoStore`] (its branch and
//! participant) and [`DrevoJournal`] wraps the drevo journal (a separate graph).
//! A Python Unit of Work hands native drevo stores to
//! [`crate::coordinator::Coordinator`] alongside any other native store, and the
//! core runs the whole two-phase protocol over Bolt with no Python callbacks on
//! the path. This module uses Bolt (`drevo+bolt://`); embedded graphs would need
//! the adapter crate's `embedded` feature.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyDict;
use rhizodat_core::journal::Journal;
use rhizodat_drevo::{DrevoJournal as CoreJournal, DrevoStore as CoreStore};
use uuid::Uuid;

use crate::native::{Drevo, NativeStore};
use crate::{InvalidUnitOfWork, journal_record_dict, op_err, runtime};

/// One drevo graph as a rhizodat store, for a native Unit of Work.
#[pyclass(module = "rhizodat_native")]
pub struct DrevoStore(Arc<CoreStore>);

impl DrevoStore {
    /// The store as a [`NativeStore`] for the coordinator.
    pub(crate) fn native(&self) -> Arc<dyn NativeStore> {
        Arc::new(Drevo(self.0.clone()))
    }
}

#[pymethods]
impl DrevoStore {
    /// A store named `name` over a drevo Bolt DSN (`drevo+bolt://…`).
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
}

/// The drevo journal (a separate graph), for Units of Work over several stores.
#[pyclass(module = "rhizodat_native")]
pub struct DrevoJournal(Arc<CoreJournal>);

impl DrevoJournal {
    /// The wrapped core journal as a trait object, for the coordinator.
    pub(crate) fn as_journal(&self) -> Arc<dyn Journal> {
        self.0.clone()
    }
}

#[pymethods]
impl DrevoJournal {
    /// Open the journal in the graph of `dsn`, its records under `label`.
    #[new]
    #[pyo3(signature = (dsn, label = "DatJournal"))]
    fn new(dsn: &str, label: &str) -> PyResult<Self> {
        let journal =
            CoreJournal::new(dsn, label).map_err(|e| InvalidUnitOfWork::new_err(e.to_string()))?;
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
