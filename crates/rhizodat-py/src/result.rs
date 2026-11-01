//! A query's result across the boundary: the core answers `execute` with the
//! rows as one Arrow IPC stream, and `rhizodat.core.native_result` makes the
//! Python facade's `Result` of it (its rows, its columns, and `to_arrow` as the
//! table itself). Arrow travels as IPC bytes for the same reason the Iceberg
//! unit takes them: importing or exporting the C stream needs `unsafe`, which
//! the workspace forbids.

use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::SchemaRef;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::op_err;

/// `batches` as the bytes of one Arrow IPC stream with `schema` (so an answer
/// with no batches still carries its columns).
pub(crate) fn ipc_bytes(schema: &SchemaRef, batches: &[RecordBatch]) -> PyResult<Vec<u8>> {
    let mut writer = StreamWriter::try_new(Vec::new(), schema).map_err(op_err)?;
    for batch in batches {
        writer.write(batch).map_err(op_err)?;
    }
    writer.finish().map_err(op_err)?;
    writer.into_inner().map_err(op_err)
}

/// A `rhizodat.core.result.Result` over the IPC stream `stream`.
pub(crate) fn py_result<'py>(py: Python<'py>, stream: &[u8]) -> PyResult<Bound<'py, PyAny>> {
    py.import("rhizodat.core.native_result")?
        .getattr("from_ipc")?
        .call1((PyBytes::new(py, stream),))
}

/// The `Result` of a statement that returns no rows.
pub(crate) fn empty_result(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    py.import("rhizodat.core.native_result")?
        .getattr("empty")?
        .call0()
}
