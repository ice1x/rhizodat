//! Migrations across the boundary: the pieces the Python runner
//! (`rhizodat.migrations.runner`) needs from a native journal (its history as
//! `rhizodat.core.journal.AppliedMigration`s) and from a native store (its
//! migration kinds and a step's statements), so `r.migrate(...)` runs on the
//! native facade with the same runner.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyFrozenSet, PyList, PyString};
use rhizodat_core::migrations::AppliedMigration;

/// `kinds` as the `frozenset` of `Migrates.migration_kinds`.
pub(crate) fn kinds<'py>(py: Python<'py>, kinds: &[&str]) -> PyResult<Bound<'py, PyFrozenSet>> {
    PyFrozenSet::new(py, kinds.iter().map(|k| PyString::new(py, k)))
}

/// A `sql` / `cypher`-like step body as its statements, like the reference's
/// `rhizodat.migrations.format.statements`: one statement or a non-empty list of
/// them; `kind` must be `expected`.
pub(crate) fn statements(
    kind: &str,
    expected: &str,
    body: &Bound<'_, PyAny>,
) -> PyResult<Vec<String>> {
    if kind != expected {
        return Err(PyValueError::new_err(format!(
            "this store runs {expected} steps, not {kind}"
        )));
    }
    if let Ok(one) = body.cast::<PyString>() {
        return Ok(vec![one.to_cow()?.into_owned()]);
    }
    if let Ok(list) = body.cast::<PyList>()
        && !list.is_empty()
        && let Ok(all) = list
            .iter()
            .map(|s| {
                s.cast::<PyString>()
                    .map_err(PyErr::from)
                    .and_then(|s| Ok(s.to_cow()?.into_owned()))
            })
            .collect::<PyResult<Vec<String>>>()
    {
        return Ok(all);
    }
    Err(PyValueError::new_err(format!(
        "a {kind} step is a statement or a list of statements, got {}",
        body.repr()?
    )))
}

/// `applied` as a `rhizodat.core.journal.AppliedMigration`.
pub(crate) fn applied_py<'py>(
    py: Python<'py>,
    applied: &AppliedMigration,
) -> PyResult<Bound<'py, PyAny>> {
    py.import("rhizodat.core.journal")?
        .getattr("AppliedMigration")?
        .call1((
            applied.id,
            applied.name.as_str(),
            applied.checksum.as_str(),
            applied.dat_tx_time,
        ))
}

/// A `rhizodat.core.journal.AppliedMigration` (or anything with its fields) as
/// the core's.
pub(crate) fn applied_from_py(obj: &Bound<'_, PyAny>) -> PyResult<AppliedMigration> {
    Ok(AppliedMigration {
        id: obj.getattr("id")?.extract()?,
        name: obj.getattr("name")?.extract()?,
        checksum: obj.getattr("checksum")?.extract()?,
        dat_tx_time: obj.getattr("dat_tx_time")?.extract()?,
    })
}

/// The reference's `JournalConflict` for a migration recorded twice.
pub(crate) fn recorded_twice(py: Python<'_>, id: i64) -> PyErr {
    match py
        .import("rhizodat.core.journal")
        .and_then(|m| m.getattr("migration_recorded"))
        .and_then(|f| f.call1((id,)))
    {
        Ok(exc) => PyErr::from_value(exc),
        Err(e) => e,
    }
}
