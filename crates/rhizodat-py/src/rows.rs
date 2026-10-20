//! One Python value as a core [`RowValue`], shared by the native stores that
//! carry a Unit of Work's own rows across the boundary (ClickHouse, PostgreSQL).

use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString};
use rhizodat_core::rows::RowValue;
use uuid::Uuid;

use crate::InvalidUnitOfWork;

/// One Python value as a [`RowValue`]. Scalars, bytes, UUIDs, lists and dicts
/// are carried exactly; temporal and decimal values travel as strings (each
/// store parses them per the column's own type).
///
/// # Errors
///
/// [`InvalidUnitOfWork`] for an integer out of 64-bit range, a non-string map
/// key, or a value of an unsupported type.
pub(crate) fn row_value(obj: &Bound<'_, PyAny>) -> PyResult<RowValue> {
    if obj.is_none() {
        return Ok(RowValue::Null);
    }
    if let Ok(flag) = obj.cast::<PyBool>() {
        return Ok(RowValue::Bool(flag.is_true()));
    }
    if let Ok(bytes) = obj.cast::<PyBytes>() {
        return Ok(RowValue::Bytes(bytes.as_bytes().to_vec()));
    }
    if obj.is_instance_of::<PyInt>() {
        return Ok(RowValue::Int(obj.extract::<i64>().map_err(|_| {
            InvalidUnitOfWork::new_err("integer out of range for a 64-bit column")
        })?));
    }
    if obj.is_instance_of::<PyFloat>() {
        return Ok(RowValue::Float(obj.extract::<f64>()?));
    }
    if let Ok(text) = obj.cast::<PyString>() {
        return Ok(RowValue::Str(text.to_cow()?.into_owned()));
    }
    if let Ok(uuid) = obj.extract::<Uuid>() {
        return Ok(RowValue::Uuid(uuid));
    }
    if let Ok(list) = obj.cast::<PyList>() {
        return Ok(RowValue::List(
            list.iter()
                .map(|v| row_value(&v))
                .collect::<PyResult<_>>()?,
        ));
    }
    if let Ok(dict) = obj.cast::<PyDict>() {
        let mut pairs = Vec::with_capacity(dict.len());
        for (key, value) in dict {
            pairs.push((
                key.cast::<PyString>()
                    .map_err(|_| InvalidUnitOfWork::new_err("a map key must be a string"))?
                    .to_cow()?
                    .into_owned(),
                row_value(&value)?,
            ));
        }
        return Ok(RowValue::Map(pairs));
    }
    Err(InvalidUnitOfWork::new_err(format!(
        "unsupported value type for a row: {}",
        obj.get_type().name()?
    )))
}
