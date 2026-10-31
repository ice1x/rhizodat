//! Recovery across the boundary: [`Recovery`] runs the core's recovery worker
//! (`rhizodat_core::recovery`) over a native journal and the participants of
//! native stores (and of Python objects with the participant methods).
//!
//! It finishes what crashed coordinators left behind: prepared gids without a
//! decision are rolled back, undecided stale records are presumed aborted, and
//! committed ones are finished idempotently and marked `DONE`. Like a native
//! commit, each run drives the adapters on its own current-thread runtime, and
//! the participants are rebuilt for it, so no connection outlives the run.

use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use rhizodat_core::journal::Journal;
use rhizodat_core::recovery::{Recovery as CoreRecovery, RecoveryReport};
use rhizodat_core::store::Participants;

use crate::coordinator::{PyParticipant, native_store, resolve_journal};
use crate::{Failure, call_int, op_err};

/// A recovery worker over one journal and the participants of its stores.
#[pyclass(module = "rhizodat_native")]
pub struct Recovery {
    journal: Arc<dyn Journal>,
    stores: Vec<Py<PyAny>>,
    stale_after_ms: i64,
    alert_after_ms: i64,
    wall_ms: Option<Py<PyAny>>,
}

#[pymethods]
impl Recovery {
    /// A worker over `journal` (a native journal) for `stores`: native stores, or
    /// Python objects with `store()` and the participant methods. Records unchanged
    /// for `stale_after_ms` are handled; a committed one unfinished for
    /// `alert_after_ms` is reported as stuck. `wall_ms() -> int` (Unix ms)
    /// defaults to the system clock.
    #[new]
    #[pyo3(signature = (journal, stores, *, stale_after_ms = 30_000, alert_after_ms = 600_000, wall_ms = None))]
    fn new(
        journal: &Bound<'_, PyAny>,
        stores: Vec<Py<PyAny>>,
        stale_after_ms: i64,
        alert_after_ms: i64,
        wall_ms: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        if stale_after_ms <= 0 {
            return Err(PyValueError::new_err(
                "stale_after_ms must be > 0: recovery must not race live coordinators",
            ));
        }
        Ok(Self {
            journal: resolve_journal(journal)?,
            stores,
            stale_after_ms,
            alert_after_ms,
            wall_ms,
        })
    }

    /// One pass over at most `limit` stale records. Returns a dict of lists:
    /// `aborted`, `finished`, `unfinished`, `stuck` (meta-transaction ids) and
    /// `orphans` (prepared gids rolled back).
    #[pyo3(signature = (*, limit = 100))]
    fn run_once<'py>(&self, py: Python<'py>, limit: usize) -> PyResult<Bound<'py, PyDict>> {
        // A participant's error is part of the report (recovery keeps going, a
        // later run retries), so its kept exception is dropped; a failing wall
        // clock fails the run.
        let participant_failure = Failure::default();
        let wall_failure = Failure::default();
        let mut participants = Participants::new();
        for obj in &self.stores {
            let bound = obj.bind(py);
            if let Some(store) = native_store(bound) {
                participants = participants.with(store.name(), store.participant());
            } else {
                let name: String = bound.call_method0("store")?.extract()?;
                participants = participants.with(
                    name,
                    Arc::new(PyParticipant {
                        obj: obj.clone_ref(py),
                        failure: participant_failure.clone(),
                    }),
                );
            }
        }
        let mut recovery =
            CoreRecovery::new(self.journal.clone(), participants, self.stale_after_ms)
                .map_err(|e| PyValueError::new_err(e.to_string()))?
                .with_alert_after_ms(self.alert_after_ms);
        if let Some(wall) = &self.wall_ms {
            let (wall, failure) = (wall.clone_ref(py), wall_failure.clone());
            recovery = recovery.with_wall(move || call_int(&wall, &failure, 0_i64, None));
        }
        let runtime = crate::runtime().map_err(op_err)?;
        let report = py.detach(|| runtime.block_on(recovery.run_once(limit)));
        let report = wall_failure.check(report, op_err)?;
        report_dict(py, &report)
    }
}

fn report_dict<'py>(py: Python<'py>, report: &RecoveryReport) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("aborted", report.aborted.clone())?;
    dict.set_item("finished", report.finished.clone())?;
    dict.set_item("unfinished", report.unfinished.clone())?;
    dict.set_item("stuck", report.stuck.clone())?;
    dict.set_item("orphans", report.orphans.clone())?;
    Ok(dict)
}
