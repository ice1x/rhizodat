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
use pyo3::types::PyBytes;
use rhizodat_iceberg::{IcebergBatches, catalog_properties};

use crate::InvalidUnitOfWork;
use crate::native::{IcebergLake, IcebergPending, NativeStore};

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
}

impl IcebergStore {
    /// The store as a [`NativeStore`] for the coordinator (a Unit of Work that
    /// writes no rows of its own).
    pub(crate) fn native(&self) -> Arc<dyn NativeStore> {
        Arc::new(IcebergPending {
            lake: IcebergLake::new(self.dsn.clone(), self.name.clone()),
            batches: IcebergBatches::new(),
        })
    }
}

#[pymethods]
impl IcebergStore {
    /// A store named `name` over an `iceberg+rest://host:port/<warehouse>` DSN.
    #[new]
    fn new(dsn: &str, name: &str) -> PyResult<Self> {
        catalog_properties(dsn).map_err(|e| InvalidUnitOfWork::new_err(e.to_string()))?;
        Ok(Self {
            dsn: dsn.into(),
            name: name.into(),
        })
    }

    /// The store name.
    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    /// A Unit of Work's branch of this store, to buffer Arrow data and commit.
    fn unit(&self) -> IcebergUnit {
        IcebergUnit {
            dsn: self.dsn.clone(),
            name: self.name.clone(),
            batches: Mutex::new(IcebergBatches::new()),
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
}
