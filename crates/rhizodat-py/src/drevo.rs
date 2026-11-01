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
use rhizodat_drevo::parts::{
    DrevoPartSpec as CoreSpec, DrevoParts, FieldKind, PartVersion, PendingVersion,
};
use rhizodat_drevo::{
    DrevoJournal as CoreJournal, DrevoStore as CoreStore, DrevoWrites, Params, Value,
};
use uuid::Uuid;

use crate::native::{Drevo, DrevoPending, NativeStore};
use crate::rows::{fields_dict, row_value};
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

/// A drevo [`Value`] as the Python value `cypher_value` reads it from.
fn py_cypher<'py>(py: Python<'py>, value: &Value) -> PyResult<Bound<'py, PyAny>> {
    Ok(match value {
        Value::Null => py.None().into_bound(py),
        Value::Bool(v) => PyBool::new(py, *v).to_owned().into_any(),
        Value::Int(v) => v.into_pyobject(py)?.into_any(),
        Value::Float(v) => PyFloat::new(py, *v).into_any(),
        Value::Str(v) => PyString::new(py, v).into_any(),
        Value::List(items) => PyList::new(
            py,
            items
                .iter()
                .map(|x| py_cypher(py, x))
                .collect::<PyResult<Vec<_>>>()?,
        )?
        .into_any(),
    })
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
    /// Run `cypher` in its own transaction outside any meta-transaction, as the
    /// Python adapter's `execute` (a write fails retryably while a branch of this
    /// graph is prepared). The result is a `rhizodat.core.result.Result` of dicts,
    /// columns in name order (the core's rows are maps).
    #[pyo3(signature = (cypher, params = None))]
    fn execute<'py>(
        &self,
        py: Python<'py>,
        cypher: &str,
        params: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let params = params.map_or_else(|| Ok(Params::new()), cypher_params)?;
        let (store, cypher) = (self.0.clone(), cypher.to_owned());
        let rows = py.detach(|| {
            let runtime = runtime().map_err(op_err)?;
            runtime
                .block_on(store.write(&cypher, params))
                .map_err(op_err)
        })?;
        let columns: Vec<String> = rows
            .first()
            .map(|r| r.0.keys().cloned().collect())
            .unwrap_or_default();
        let values = rows
            .iter()
            .map(|row| {
                PyList::new(
                    py,
                    columns
                        .iter()
                        .map(|c| py_cypher(py, row.value(c)))
                        .collect::<PyResult<Vec<_>>>()?,
                )
            })
            .collect::<PyResult<Vec<_>>>()?;
        py.import("rhizodat.core.native_result")?
            .getattr("from_rows")?
            .call1((PyList::new(py, &columns)?, PyList::new(py, values)?))
    }

    fn unit(&self) -> DrevoUnit {
        DrevoUnit {
            store: self.0.clone(),
            writes: Mutex::new(DrevoWrites::new()),
            parts: Mutex::new(DrevoParts::new()),
        }
    }

    /// Create the `dat_id` indexes of the part's versions and of anchors. Idempotent.
    fn install_part(&self, py: Python<'_>, spec: &DrevoPartSpec) -> PyResult<()> {
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
        spec: &DrevoPartSpec,
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
        spec: &DrevoPartSpec,
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
        spec: &DrevoPartSpec,
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
        spec: &DrevoPartSpec,
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

/// One drevo store's share of one entity type:
/// `DrevoPartSpec(type_name, label, fields)` with `fields` the
/// `(field, kind, is a reference)` triples in order, `kind` one of `native`,
/// `uuid`, `json`.
#[pyclass(module = "rhizodat_native", frozen)]
pub struct DrevoPartSpec(CoreSpec);

#[pymethods]
impl DrevoPartSpec {
    #[new]
    fn new(type_name: &str, label: &str, fields: Vec<(String, String, bool)>) -> PyResult<Self> {
        let fields = fields
            .into_iter()
            .map(|(name, kind, reference)| Ok((name, kind.parse::<FieldKind>()?, reference)))
            .collect::<Result<Vec<_>, rhizodat_drevo::ConfigError>>()
            .map_err(|e| InvalidUnitOfWork::new_err(e.to_string()))?;
        CoreSpec::new(type_name, label, fields)
            .map(Self)
            .map_err(|e| InvalidUnitOfWork::new_err(e.to_string()))
    }

    /// The label.
    #[getter]
    fn label(&self) -> &str {
        self.0.label()
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
    parts: Mutex<DrevoParts>,
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
        let parts = self
            .parts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        Arc::new(DrevoPending {
            store: self.store.clone(),
            writes,
            parts,
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

    /// Buffer a version of `dat_id`'s part: `values` by field (missing ones are
    /// `NULL`), `created` the time of the first version (`None`: this one).
    #[pyo3(signature = (spec, dat_id, values, *, created, deleted = false))]
    fn write_part(
        &self,
        spec: &DrevoPartSpec,
        dat_id: Uuid,
        values: &Bound<'_, PyDict>,
        created: Option<i64>,
        deleted: bool,
    ) -> PyResult<()> {
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

    /// Buffer the removal of every version of `dat_id`'s part.
    fn erase_part(&self, spec: &DrevoPartSpec, dat_id: Uuid) {
        self.parts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .erase(&spec.0, dat_id);
    }

    /// This unit's own latest version of `dat_id`'s part: `(False, None)` when it
    /// did not touch it, `(True, None)` when it erased it last, else
    /// `(True, {created, deleted, values})`.
    fn own_part<'py>(
        &self,
        py: Python<'py>,
        spec: &DrevoPartSpec,
        dat_id: Uuid,
    ) -> PyResult<(bool, Option<Bound<'py, PyDict>>)> {
        let parts = self
            .parts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(match parts.latest(spec.0.label(), dat_id) {
            None => (false, None),
            Some(None) => (true, None),
            Some(Some(version)) => (true, Some(pending_dict(py, &spec.0, version)?)),
        })
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
