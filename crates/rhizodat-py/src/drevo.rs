//! The real Rust drevo adapter behind the Python coordinator.
//!
//! [`DrevoStore`] wraps [`rhizodat_drevo::DrevoStore`] (its branch and
//! participant) and [`DrevoJournal`] wraps the drevo journal (a separate graph).
//! A Python Unit of Work hands native drevo stores to
//! [`crate::coordinator::Coordinator`] alongside any other native store, and the
//! core runs the whole two-phase protocol over Bolt with no Python callbacks on
//! the path. This module uses Bolt (`drevo+bolt://`); embedded graphs would need
//! the adapter crate's `embedded` feature.

use std::sync::{Arc, Mutex};

use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString};
use rhizodat_core::journal::Journal;
use rhizodat_drevo::{
    DrevoJournal as CoreJournal, DrevoStore as CoreStore, DrevoWrites, Params, Value,
};
use uuid::Uuid;

use crate::native::{Drevo, DrevoPending, NativeStore};
use crate::{InvalidUnitOfWork, journal_record_dict, op_err, runtime};

/// One Python value as a drevo Cypher [`Value`]. drevo carries scalars and lists;
/// a UUID travels as its string. Bytes and dicts (maps) are not Cypher values.
///
/// # Errors
///
/// [`InvalidUnitOfWork`] for an integer out of 64-bit range or an unsupported
/// type.
fn cypher_value(obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    if obj.is_none() {
        return Ok(Value::Null);
    }
    if let Ok(flag) = obj.cast::<PyBool>() {
        return Ok(Value::Bool(flag.is_true()));
    }
    if obj.is_instance_of::<PyInt>() {
        return Ok(Value::Int(obj.extract::<i64>().map_err(|_| {
            InvalidUnitOfWork::new_err("integer out of range for a 64-bit value")
        })?));
    }
    if obj.is_instance_of::<PyFloat>() {
        return Ok(Value::Float(obj.extract::<f64>()?));
    }
    if let Ok(text) = obj.cast::<PyString>() {
        return Ok(Value::Str(text.to_cow()?.into_owned()));
    }
    if let Ok(uuid) = obj.extract::<Uuid>() {
        return Ok(Value::Str(uuid.hyphenated().to_string()));
    }
    if let Ok(list) = obj.cast::<PyList>() {
        return Ok(Value::List(
            list.iter()
                .map(|v| cypher_value(&v))
                .collect::<PyResult<_>>()?,
        ));
    }
    Err(InvalidUnitOfWork::new_err(format!(
        "unsupported value type for a Cypher parameter: {}",
        obj.get_type().name()?
    )))
}

/// A Python mapping of parameter names to values as drevo [`Params`].
fn cypher_params(params: &Bound<'_, PyDict>) -> PyResult<Params> {
    let mut out: Params = Vec::with_capacity(params.len());
    for (key, value) in params {
        let name = key
            .cast::<PyString>()
            .map_err(|_| InvalidUnitOfWork::new_err("a parameter name must be a string"))?
            .to_cow()?
            .into_owned();
        out.push((name, cypher_value(&value)?));
    }
    Ok(out)
}

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

    /// A Unit of Work's branch of this store, to buffer Cypher writes and commit.
    fn unit(&self) -> DrevoUnit {
        DrevoUnit {
            store: self.0.clone(),
            writes: Mutex::new(DrevoWrites::new()),
        }
    }
}

/// One Unit of Work's buffered graph writes for a drevo store. Buffer with
/// [`DrevoUnit::run`], then hand it to `Coordinator.commit` like a store: the
/// statements run inside the branch's transaction, so they commit atomically
/// with the meta-transaction through the two-phase protocol. Each statement gets
/// `dat_txn` and `dat_tx_time` parameters injected, so it may stamp its nodes
/// with `$dat_txn`. The unit may be reused and keeps its writes across a retry.
#[pyclass(module = "rhizodat_native")]
pub struct DrevoUnit {
    store: Arc<CoreStore>,
    writes: Mutex<DrevoWrites>,
}

impl DrevoUnit {
    /// A native store carrying a snapshot of the buffered writes, for the
    /// coordinator; the unit itself is left intact.
    pub(crate) fn pending(&self) -> Arc<dyn NativeStore> {
        let writes = self
            .writes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        Arc::new(DrevoPending {
            store: self.store.clone(),
            writes,
        })
    }
}

#[pymethods]
impl DrevoUnit {
    /// Buffer a parameterized Cypher statement to run at commit.
    #[pyo3(signature = (cypher, params = None))]
    fn run(&self, cypher: &str, params: Option<&Bound<'_, PyDict>>) -> PyResult<()> {
        let params = params.map(cypher_params).transpose()?.unwrap_or_default();
        self.writes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(cypher, params)
            .map_err(|e| InvalidUnitOfWork::new_err(e.to_string()))
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
