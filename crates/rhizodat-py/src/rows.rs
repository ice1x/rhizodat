//! Python values and core [`RowValue`]s, both ways: shared by the native stores
//! that carry a Unit of Work's own rows across the boundary (ClickHouse,
//! PostgreSQL) and the entity parts read back from them.

use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};
use rhizodat_core::rows::RowValue;
use uuid::Uuid;

use crate::InvalidUnitOfWork;

/// One Python value as a [`RowValue`]. Scalars, bytes, UUIDs, lists and dicts
/// are carried exactly; `datetime`, `date` and `Decimal` as their ISO 8601 or
/// exact text (strings work too: each store parses them per the column's own
/// type), the way `rhizodat.core.rows.encode_value` tags them.
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
    let py = obj.py();
    let datetime = py.import("datetime")?;
    // A datetime is a date: check it first.
    if obj.is_instance(&datetime.getattr("datetime")?)? {
        return Ok(RowValue::DateTime(
            obj.call_method0("isoformat")?.extract()?,
        ));
    }
    if obj.is_instance(&datetime.getattr("date")?)? {
        return Ok(RowValue::Date(obj.call_method0("isoformat")?.extract()?));
    }
    if obj.is_instance(&py.import("decimal")?.getattr("Decimal")?)? {
        return Ok(RowValue::Decimal(obj.str()?.extract()?));
    }
    if let Ok(list) = obj.cast::<PyList>() {
        return Ok(RowValue::List(
            list.iter()
                .map(|v| row_value(&v))
                .collect::<PyResult<_>>()?,
        ));
    }
    if let Ok(tuple) = obj.cast::<PyTuple>() {
        return Ok(RowValue::List(
            tuple
                .iter()
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

/// A [`RowValue`] as the Python value `row_value` reads it from: `uuid.UUID`,
/// `decimal.Decimal`, `datetime.date` / `datetime.datetime` (from ISO 8601),
/// `bytes`, lists and dicts.
pub(crate) fn py_value<'py>(py: Python<'py>, value: &RowValue) -> PyResult<Bound<'py, PyAny>> {
    Ok(match value {
        RowValue::Null => py.None().into_bound(py),
        RowValue::Bool(v) => PyBool::new(py, *v).to_owned().into_any(),
        RowValue::Int(v) => v.into_pyobject(py)?.into_any(),
        RowValue::Float(v) => PyFloat::new(py, *v).into_any(),
        RowValue::Str(v) => PyString::new(py, v).into_any(),
        RowValue::Uuid(v) => v.into_pyobject(py)?.into_any(),
        RowValue::Decimal(v) => py.import("decimal")?.getattr("Decimal")?.call1((v,))?,
        RowValue::Bytes(v) => PyBytes::new(py, v).into_any(),
        RowValue::Date(v) => py
            .import("datetime")?
            .getattr("date")?
            .call_method1("fromisoformat", (v,))?,
        RowValue::DateTime(v) => py
            .import("datetime")?
            .getattr("datetime")?
            .call_method1("fromisoformat", (v,))?,
        RowValue::List(v) => PyList::new(
            py,
            v.iter()
                .map(|x| py_value(py, x))
                .collect::<PyResult<Vec<_>>>()?,
        )?
        .into_any(),
        RowValue::Map(v) => {
            let dict = PyDict::new(py);
            for (key, x) in v {
                dict.set_item(key, py_value(py, x)?)?;
            }
            dict.into_any()
        }
    })
}
