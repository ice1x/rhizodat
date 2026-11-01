//! The real Rust Iceberg adapter behind the Python coordinator.
//!
//! [`IcebergStore`] stands for [`rhizodat_iceberg::IcebergStore`] (its one-phase
//! branch and participant) over a REST catalog. A Python Unit of Work hands
//! native Iceberg stores to [`crate::coordinator::Coordinator`] alongside any
//! other native store; stage writes Parquet files and the participant appends
//! them after the decision. Iceberg has no journal of its own, so a
//! meta-transaction that includes it commits through another store's journal.
//!
//! The Unit of Work's own data crosses the boundary as Arrow through an
//! [`IcebergUnit`] (`store.unit()`): [`IcebergUnit::append`] takes any object
//! with the Arrow PyCapsule stream interface (`__arrow_c_stream__`: a pyarrow
//! `Table` or `RecordBatchReader`, a polars frame, …) or the bytes of an Arrow
//! IPC stream. The data travels as an IPC stream (pyarrow writes it, one copy),
//! read in safe Rust: importing the C stream directly would need `unsafe`, which
//! the workspace forbids, and no Arrow binding for pyo3 0.30 matches the Arrow
//! version iceberg-rust 0.10 uses.

use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyBytes, PyDict, PyFrozenSet};
use rhizodat_iceberg::parts::{
    FieldKind, IcebergPartSpec as CoreSpec, IcebergParts, PartVersion, PendingVersion,
};
use rhizodat_iceberg::{IcebergBatches, IcebergStore as CoreStore, Scan, catalog_properties};
use uuid::Uuid;

use crate::migrations::kinds;
use crate::native::{IcebergLake, IcebergPending, NativeStore};
use crate::result::{ipc_bytes, py_result};
use crate::rows::{fields_dict, row_value};
use crate::{InvalidUnitOfWork, op_err, runtime};

/// `data` as the bytes of an Arrow IPC stream: the bytes themselves, or a
/// Python object with `__arrow_c_stream__` written as a stream by pyarrow.
fn ipc_stream(data: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(bytes) = data.cast::<PyBytes>() {
        return Ok(bytes.as_bytes().to_vec());
    }
    if !data.hasattr("__arrow_c_stream__")? {
        return Err(InvalidUnitOfWork::new_err(format!(
            "Iceberg rows must be Arrow data with __arrow_c_stream__ (a pyarrow Table, …) \
             or Arrow IPC stream bytes, not {}",
            data.get_type().name()?
        )));
    }
    let py = data.py();
    let pa = py.import("pyarrow")?;
    let ipc = py.import("pyarrow.ipc")?;
    let table = pa
        .getattr("RecordBatchReader")?
        .call_method1("from_stream", (data,))?
        .call_method0("read_all")?;
    let sink = pa.getattr("BufferOutputStream")?.call0()?;
    let writer = ipc.call_method1("new_stream", (&sink, table.getattr("schema")?))?;
    writer.call_method1("write_table", (&table,))?;
    writer.call_method0("close")?;
    sink.call_method0("getvalue")?
        .call_method0("to_pybytes")?
        .extract()
}

/// `execute`'s `params` as a scan: `selected_fields` (a sequence of names),
/// `snapshot_id` and `limit`; a `row_filter` is refused (the core has none yet),
/// as is any other key.
fn scan_params(params: Option<&Bound<'_, PyDict>>) -> PyResult<Scan> {
    let mut scan = Scan::default();
    let Some(params) = params else {
        return Ok(scan);
    };
    for (key, value) in params.iter() {
        match key.extract::<String>()?.as_str() {
            "selected_fields" => {
                scan.columns = Some(
                    value
                        .try_iter()?
                        .map(|f| f?.extract::<String>())
                        .collect::<PyResult<Vec<_>>>()?,
                );
            }
            "snapshot_id" => scan.snapshot_id = Some(value.extract()?),
            "limit" => scan.limit = Some(value.extract()?),
            "row_filter" => {
                return Err(InvalidUnitOfWork::new_err(
                    "row_filter is not on the native core path yet; scan with selected_fields, \
                     snapshot_id and limit, or use the Python adapter for a filtered scan",
                ));
            }
            other => {
                return Err(InvalidUnitOfWork::new_err(format!(
                    "unknown scan parameter {other:?}; the native store takes selected_fields, \
                     snapshot_id and limit"
                )));
            }
        }
    }
    Ok(scan)
}

/// The record batches of an Arrow IPC stream.
fn record_batches(stream: &[u8]) -> PyResult<Vec<RecordBatch>> {
    let invalid = |e: arrow_schema::ArrowError| {
        InvalidUnitOfWork::new_err(format!("not an Arrow IPC stream: {e}"))
    };
    StreamReader::try_new(stream, None)
        .map_err(invalid)?
        .collect::<Result<_, _>>()
        .map_err(invalid)
}

/// One Iceberg REST catalog as a rhizodat store, for a native Unit of Work.
///
/// Built without I/O: the catalog is reached when a commit runs.
#[pyclass(module = "rhizodat_native")]
pub struct IcebergStore {
    dsn: Arc<str>,
    name: Arc<str>,
    parts_namespace: Arc<str>,
}

impl IcebergStore {
    /// The store as a [`NativeStore`] for the coordinator (a Unit of Work that
    /// writes no rows of its own).
    pub(crate) fn native(&self) -> Arc<dyn NativeStore> {
        Arc::new(IcebergPending {
            lake: IcebergLake::new(self.dsn.clone(), self.name.clone()),
            batches: IcebergBatches::new(),
            parts: IcebergParts::new(),
        })
    }

    /// Run `read` against the catalog, connected for this call (a REST client is
    /// bound to the runtime that made it).
    fn read<T: Send>(
        &self,
        py: Python<'_>,
        read: impl AsyncFnOnce(&CoreStore) -> Result<T, rhizodat_core::store::BoxError> + Send,
    ) -> PyResult<T> {
        let (dsn, name) = (self.dsn.clone(), self.name.clone());
        py.detach(|| {
            let runtime = runtime().map_err(op_err)?;
            runtime
                .block_on(async {
                    let store = CoreStore::connect(&dsn, &name).await?;
                    read(&store).await
                })
                .map_err(op_err)
        })
    }
}

#[pymethods]
impl IcebergStore {
    /// A store named `name` over an `iceberg+rest://host:port/<warehouse>` DSN;
    /// entity parts live in `parts_namespace` (the Python adapter's default too).
    #[new]
    #[pyo3(signature = (dsn, name, *, parts_namespace = "dat"))]
    fn new(dsn: &str, name: &str, parts_namespace: &str) -> PyResult<Self> {
        catalog_properties(dsn).map_err(|e| InvalidUnitOfWork::new_err(e.to_string()))?;
        Ok(Self {
            dsn: dsn.into(),
            name: name.into(),
            parts_namespace: parts_namespace.into(),
        })
    }

    /// The namespace of the entity part tables.
    #[getter]
    fn parts_namespace(&self) -> &str {
        &self.parts_namespace
    }

    /// Create the part's namespace and table if missing. Idempotent.
    fn install_part(&self, py: Python<'_>, spec: &IcebergPartSpec) -> PyResult<()> {
        let spec = spec.0.clone();
        self.read(py, async move |store| store.install_part(&spec).await)
    }

    /// The latest committed version of `dat_id`'s part with
    /// `dat_tx_time <= known_at` (any, if `None`), as a dict, or `None`.
    #[pyo3(signature = (spec, dat_id, *, known_at = None))]
    fn read_part<'py>(
        &self,
        py: Python<'py>,
        spec: &IcebergPartSpec,
        dat_id: Uuid,
        known_at: Option<i64>,
    ) -> PyResult<Option<Bound<'py, PyDict>>> {
        let core = spec.0.clone();
        let read = core.clone();
        let version = self.read(py, async move |store| {
            store.read_part(&read, dat_id, known_at).await
        })?;
        version.map(|v| version_dict(py, &core, &v)).transpose()
    }

    /// Every committed version of `dat_id`'s part, oldest first.
    fn part_history<'py>(
        &self,
        py: Python<'py>,
        spec: &IcebergPartSpec,
        dat_id: Uuid,
    ) -> PyResult<Vec<Bound<'py, PyDict>>> {
        let core = spec.0.clone();
        let read = core.clone();
        let versions = self.read(py, async move |store| {
            store.part_history(&read, dat_id).await
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
        spec: &IcebergPartSpec,
        known_at: Option<i64>,
    ) -> PyResult<Vec<(Uuid, Bound<'py, PyDict>)>> {
        let core = spec.0.clone();
        let read = core.clone();
        let versions = self.read(py, async move |store| {
            store.part_versions(&read, known_at).await
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
        spec: &IcebergPartSpec,
        deleted_before: i64,
        limit: i64,
    ) -> PyResult<Vec<Uuid>> {
        let read = spec.0.clone();
        self.read(py, async move |store| {
            store.expired_parts(&read, deleted_before, limit).await
        })
    }

    /// Scan `table` (`namespace.table`) outside any meta-transaction, as the
    /// Python adapter's `execute`: `params` take `selected_fields`, `snapshot_id`
    /// and `limit`. The rows come back as a `rhizodat.core.result.Result` built
    /// from one Arrow IPC stream, so its `to_arrow` is the scan itself.
    #[pyo3(signature = (table, params = None))]
    fn execute<'py>(
        &self,
        py: Python<'py>,
        table: &str,
        params: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if table.split('.').filter(|p| !p.is_empty()).count() < 2 {
            return Err(InvalidUnitOfWork::new_err(format!(
                "invalid table name {table:?}: namespace.table"
            )));
        }
        let scan = scan_params(params)?;
        let table = table.to_owned();
        let batches = self.read(py, async move |store| store.scan_with(&table, &scan).await)?;
        let schema = batches
            .first()
            .map(RecordBatch::schema)
            .ok_or_else(|| op_err("the scan answered without a schema"))?;
        py_result(py, &ipc_bytes(&schema, &batches)?)
    }

    /// The store name.
    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    /// A Unit of Work's branch of this store, to buffer Arrow data and commit.
    /// The migration step kinds this store runs (`Migrates.migration_kinds`).
    #[getter]
    #[allow(clippy::unused_self)]
    fn migration_kinds<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyFrozenSet>> {
        kinds(py, &["iceberg"])
    }

    /// An `iceberg` step: the Python adapter's declarative schema operations
    /// (`rhizodat.adapters.iceberg_schema.run_operations`) against a pyiceberg
    /// catalog on this store's DSN. The step itself stays Python for now:
    /// iceberg-rust 0.10 has no column rename or partition update.
    fn run_migration_step(
        &self,
        py: Python<'_>,
        kind: &str,
        body: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        if kind != "iceberg" {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "this store runs iceberg steps, not {kind}"
            )));
        }
        let is_ops = body
            .cast::<pyo3::types::PyList>()
            .is_ok_and(|l| l.iter().all(|op| op.is_instance_of::<PyDict>()));
        if !is_ops {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "an iceberg step is a list of operations (tables), got {}",
                body.repr()?
            )));
        }
        let store = py
            .import("rhizodat.adapters.iceberg")?
            .getattr("IcebergStore")?
            .call(
                (&*self.dsn,),
                Some(&[("name", &*self.name)].into_py_dict(py)?),
            )?;
        let run = py
            .import("rhizodat.adapters.iceberg_schema")?
            .getattr("run_operations")?;
        let result = run.call1((store.getattr("catalog")?, body));
        store.call_method0("close")?;
        result.map(|_| ())
    }

    fn unit(&self) -> IcebergUnit {
        IcebergUnit {
            dsn: self.dsn.clone(),
            name: self.name.clone(),
            batches: Mutex::new(IcebergBatches::new()),
            parts: Mutex::new(IcebergParts::new()),
        }
    }
}

/// One Unit of Work's buffered record batches for an Iceberg store. Buffer with
/// [`IcebergUnit::append`], then hand it to `Coordinator.commit` like a store. A
/// unit may be committed again (a retry writes the same batches anew).
#[pyclass(module = "rhizodat_native")]
pub struct IcebergUnit {
    dsn: Arc<str>,
    name: Arc<str>,
    batches: Mutex<IcebergBatches>,
    parts: Mutex<IcebergParts>,
}

impl IcebergUnit {
    /// The store with this unit's batches, for the coordinator.
    pub(crate) fn pending(&self) -> Arc<dyn NativeStore> {
        Arc::new(IcebergPending {
            lake: IcebergLake::new(self.dsn.clone(), self.name.clone()),
            batches: self
                .batches
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
            parts: self
                .parts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        })
    }
}

#[pymethods]
impl IcebergUnit {
    /// Buffer `data` (Arrow with `__arrow_c_stream__`, or IPC stream bytes) to
    /// append to `table` (`namespace.table`). Columns are matched by name and cast to the table's
    /// types at commit; table columns the data lacks are null.
    fn append(&self, table: &str, data: &Bound<'_, PyAny>) -> PyResult<()> {
        let stream = ipc_stream(data)?;
        let batches = data.py().detach(|| record_batches(&stream))?;
        let mut buffer = self
            .batches
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Check every batch before keeping any, so a refused call buffers nothing.
        let mut staged = buffer.clone();
        for batch in batches {
            staged
                .append(table, batch)
                .map_err(|e| InvalidUnitOfWork::new_err(e.to_string()))?;
        }
        *buffer = staged;
        Ok(())
    }

    /// Set this unit's version of `dat_id`'s part (the last write wins): `values`
    /// by field (missing ones are `NULL`), `created` the time of the first
    /// version (`None`: this one).
    #[pyo3(signature = (spec, dat_id, values, *, created, deleted = false))]
    fn write_part(
        &self,
        spec: &IcebergPartSpec,
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

    /// Buffer the erase of every earlier version of `dat_id`'s part.
    fn erase_part(&self, spec: &IcebergPartSpec, dat_id: Uuid) {
        self.parts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .erase(&spec.0, dat_id);
    }

    /// This unit's own version of `dat_id`'s part: `(False, None)` when it did
    /// not touch it, `(True, None)` when it erased it (and wrote nothing since),
    /// else `(True, {created, deleted, values})`.
    fn own_part<'py>(
        &self,
        py: Python<'py>,
        spec: &IcebergPartSpec,
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

/// One Iceberg store's share of one entity type:
/// `IcebergPartSpec(type_name, table, fields)` with `table` as `namespace.table`
/// and `fields` the `(field, kind)` pairs in order: `string`, `boolean`, `long`,
/// `double`, `binary`, `timestamptz`, `date`, `id` (a UUID as text) or `json`.
#[pyclass(module = "rhizodat_native", frozen)]
pub struct IcebergPartSpec(CoreSpec);

#[pymethods]
impl IcebergPartSpec {
    #[new]
    fn new(type_name: &str, table: &str, fields: Vec<(String, String)>) -> PyResult<Self> {
        let fields = fields
            .into_iter()
            .map(|(name, kind)| Ok((name, kind.parse::<FieldKind>()?)))
            .collect::<Result<Vec<_>, rhizodat_iceberg::IcebergError>>()
            .map_err(|e| InvalidUnitOfWork::new_err(e.to_string()))?;
        CoreSpec::new(type_name, table, fields)
            .map(Self)
            .map_err(|e| InvalidUnitOfWork::new_err(e.to_string()))
    }

    /// The table (`namespace.table`).
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
