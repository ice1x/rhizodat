//! The commit protocol of the core, driven by a Python Unit of Work.
//!
//! A Python Unit of Work hands [`Coordinator::commit`] its branches: any Python
//! objects with the branch methods (`store`, `two_phase`, `stage`, `prepare`,
//! `commit_one_phase`, `rollback`, `finish`, `is_applied`, `abort`,
//! `prepared_gids`). The core runs the protocol on a single-threaded executor
//! and calls back into each object along the way; the result mirrors the Python
//! reference coordinator (`rhizodat.core.coordinator`).
//!
//! The GIL: [`Coordinator::commit`] releases it while the core runs
//! ([`Python::detach`]) and each callback re-takes it for the one call
//! ([`Python::attach`]), so a branch method runs with the GIL and the protocol
//! between calls runs without it. Everything stays on the calling thread (the
//! executor is single-threaded), so there is no cross-thread hazard and the
//! branch objects need not be shared between threads. The first exception a
//! callback raises is kept and, once the core has rolled everything back,
//! chained as the `__cause__` of [`AbortedError`] / [`InDoubtError`], exactly
//! as the Python reference chains the store's own error.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use futures::executor::block_on;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use rhizodat_core::clock::HybridLogicalClock;
use rhizodat_core::coordinator::{CommitError, Coordinator as CoreCoordinator, Outcome};
use rhizodat_core::journal::{BranchRecord, Journal, MemoryJournal as CoreJournal};
use rhizodat_core::store::{BoxError, Branch, Participant, Participants};
use rhizodat_core::timer::NoWait;
use uuid::Uuid;

use crate::{AbortedError, Failure, InDoubtError, InvalidUnitOfWork};

/// A Python branch callback raised; it is kept in the [`Failure`] cell.
#[derive(Debug)]
struct CallbackFailure {
    retryable: bool,
}

impl fmt::Display for CallbackFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a Python branch callback raised")
    }
}

impl Error for CallbackFailure {}

/// Call a Python branch method under the GIL; a raised exception is kept in
/// `failure` (first one wins) and reported as a [`CallbackFailure`] whose
/// `retryable` is read from the exception's `retryable` attribute.
fn guarded<R>(
    obj: &Py<PyAny>,
    failure: &Failure,
    f: impl FnOnce(Python<'_>, &Bound<'_, PyAny>) -> PyResult<R>,
) -> Result<R, BoxError> {
    Python::attach(|py| match f(py, obj.bind(py)) {
        Ok(value) => Ok(value),
        Err(err) => {
            let retryable = err
                .value(py)
                .getattr("retryable")
                .ok()
                .and_then(|v| v.is_truthy().ok())
                .unwrap_or(false);
            failure.keep(err);
            Err(Box::new(CallbackFailure { retryable }) as BoxError)
        }
    })
}

fn retryable(error: &(dyn Error + 'static)) -> bool {
    error
        .downcast_ref::<CallbackFailure>()
        .is_some_and(|f| f.retryable)
}

/// One Python object as one store's branch of a meta-transaction.
struct PyBranch {
    obj: Py<PyAny>,
    store: String,
    two_phase: bool,
    failure: Failure,
}

#[async_trait]
impl Branch for PyBranch {
    fn store(&self) -> &str {
        &self.store
    }

    fn two_phase(&self) -> bool {
        self.two_phase
    }

    async fn stage(
        &mut self,
        dat_txn: Uuid,
        dat_tx_time: i64,
    ) -> Result<Option<Vec<u8>>, BoxError> {
        guarded(&self.obj, &self.failure, |_py, b| {
            b.call_method1("stage", (dat_txn, dat_tx_time))?
                .extract::<Option<Vec<u8>>>()
        })
    }

    async fn prepare(&mut self, gid: &str) -> Result<(), BoxError> {
        guarded(&self.obj, &self.failure, |_py, b| {
            b.call_method1("prepare", (gid,))?;
            Ok(())
        })
    }

    async fn commit_one_phase(&mut self) -> Result<(), BoxError> {
        guarded(&self.obj, &self.failure, |_py, b| {
            b.call_method0("commit_one_phase")?;
            Ok(())
        })
    }

    async fn rollback(&mut self) -> Result<(), BoxError> {
        guarded(&self.obj, &self.failure, |_py, b| {
            b.call_method0("rollback")?;
            Ok(())
        })
    }

    fn is_retryable(&self, error: &(dyn Error + 'static)) -> bool {
        retryable(error)
    }
}

/// The same Python object as the store's participant after the decision.
struct PyParticipant {
    obj: Py<PyAny>,
    failure: Failure,
}

#[async_trait]
impl Participant for PyParticipant {
    async fn finish(
        &self,
        branch: &BranchRecord,
        dat_txn: Uuid,
        dat_tx_time: i64,
    ) -> Result<(), BoxError> {
        let payload = branch.payload.clone();
        guarded(&self.obj, &self.failure, |py, b| {
            let arg: Py<PyAny> = match &payload {
                Some(bytes) => PyBytes::new(py, bytes).into_any().unbind(),
                None => py.None(),
            };
            b.call_method1("finish", (arg, dat_txn, dat_tx_time))?;
            Ok(())
        })
    }

    async fn is_applied(&self, _branch: &BranchRecord, dat_txn: Uuid) -> Result<bool, BoxError> {
        guarded(&self.obj, &self.failure, |_py, b| {
            b.call_method1("is_applied", (dat_txn,))?.extract::<bool>()
        })
    }

    async fn abort(&self, _branch: &BranchRecord) -> Result<(), BoxError> {
        guarded(&self.obj, &self.failure, |_py, b| {
            b.call_method0("abort")?;
            Ok(())
        })
    }

    async fn prepared_gids(&self) -> Result<Vec<String>, BoxError> {
        guarded(&self.obj, &self.failure, |_py, b| {
            b.call_method0("prepared_gids")?.extract::<Vec<String>>()
        })
    }

    fn is_retryable(&self, error: &(dyn Error + 'static)) -> bool {
        retryable(error)
    }
}

fn outcome_dict(py: Python<'_>, outcome: Outcome) -> PyResult<Bound<'_, PyDict>> {
    let dict = PyDict::new(py);
    let (status, dat_txn, dat_tx_time) = match outcome {
        Outcome::Committed {
            dat_txn,
            dat_tx_time,
        } => ("committed", dat_txn, dat_tx_time),
        Outcome::Pending {
            dat_txn,
            dat_tx_time,
        } => ("pending", dat_txn, Some(dat_tx_time)),
    };
    dict.set_item("status", status)?;
    dict.set_item("dat_txn", dat_txn)?;
    dict.set_item("dat_tx_time", dat_tx_time)?;
    Ok(dict)
}

fn commit_error(py: Python<'_>, error: CommitError, failure: &Failure) -> PyErr {
    match error {
        CommitError::Aborted {
            dat_txn,
            retryable,
            cause,
        } => {
            let err = AbortedError::new_err(cause.to_string());
            {
                let value = err.value(py).as_any();
                value.setattr("dat_txn", dat_txn).ok();
                value.setattr("retryable", retryable).ok();
            }
            if let Some(cause) = failure.take() {
                err.set_cause(py, Some(cause));
            }
            err
        }
        CommitError::InDoubt { dat_txn, cause } => {
            let err = InDoubtError::new_err(cause.to_string());
            err.value(py).as_any().setattr("dat_txn", dat_txn).ok();
            if let Some(cause) = failure.take() {
                err.set_cause(py, Some(cause));
            }
            err
        }
        CommitError::Invalid(message) => InvalidUnitOfWork::new_err(message),
    }
}

/// The in-memory journal of the core, for Units of Work over several stores.
#[pyclass(module = "rhizodat_native")]
pub struct MemoryJournal(Arc<CoreJournal>);

#[pymethods]
impl MemoryJournal {
    #[new]
    fn new() -> Self {
        Self(Arc::new(CoreJournal::new()))
    }

    /// The record of `dat_txn` as a dict (`state`, `dat_txn`, `dat_tx_time`),
    /// or `None` when there is none.
    fn get<'py>(&self, py: Python<'py>, dat_txn: Uuid) -> PyResult<Option<Bound<'py, PyDict>>> {
        let record = py
            .detach(|| block_on(self.0.get(dat_txn)))
            .map_err(|e| InvalidUnitOfWork::new_err(e.to_string()))?;
        let Some(record) = record else {
            return Ok(None);
        };
        let dict = PyDict::new(py);
        dict.set_item("state", record.state.as_str())?;
        dict.set_item("dat_txn", record.dat_txn)?;
        dict.set_item("dat_tx_time", record.dat_tx_time)?;
        Ok(Some(dict))
    }
}

/// Runs the commit protocol for Python Units of Work. One coordinator serves
/// many of them and keeps a hybrid logical clock across them.
#[pyclass(module = "rhizodat_native")]
pub struct Coordinator {
    journal: Option<Arc<CoreJournal>>,
    clock: Arc<HybridLogicalClock>,
}

#[pymethods]
impl Coordinator {
    #[new]
    #[pyo3(signature = (journal = None))]
    fn new(journal: Option<PyRef<'_, MemoryJournal>>) -> Self {
        Self {
            journal: journal.map(|j| j.0.clone()),
            clock: Arc::new(HybridLogicalClock::new()),
        }
    }

    /// Commit `branches` as the meta-transaction `dat_txn`.
    ///
    /// Returns `{"status", "dat_txn", "dat_tx_time"}` (`status` is `committed`
    /// or `pending`). Raises `AbortedError` (with `dat_txn` and `retryable`),
    /// `InDoubtError` (with `dat_txn`), or `InvalidUnitOfWork`.
    fn commit<'py>(
        &self,
        py: Python<'py>,
        dat_txn: Uuid,
        branches: Vec<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let failure = Failure::default();
        let mut participants = Participants::new();
        let mut core_branches: Vec<Box<dyn Branch>> = Vec::with_capacity(branches.len());
        for obj in branches {
            let bound = obj.bind(py);
            let store: String = bound.call_method0("store")?.extract()?;
            let two_phase: bool = bound.call_method0("two_phase")?.extract()?;
            participants = participants.with(
                store.clone(),
                Arc::new(PyParticipant {
                    obj: obj.clone_ref(py),
                    failure: failure.clone(),
                }),
            );
            core_branches.push(Box::new(PyBranch {
                obj,
                store,
                two_phase,
                failure: failure.clone(),
            }));
        }
        let journal: Option<Arc<dyn Journal>> = self.journal.clone().map(|j| j as Arc<dyn Journal>);
        let coordinator = CoreCoordinator::new(
            journal,
            participants,
            self.clock.clone(),
            Arc::new(NoWait::default()),
        );
        let result = py.detach(|| block_on(coordinator.commit(dat_txn, &mut core_branches)));
        match result {
            Ok(outcome) => outcome_dict(py, outcome),
            Err(error) => Err(commit_error(py, error, &failure)),
        }
    }
}
