//! `rhizodat_native`: the rhizodat core in Rust, for the rhizodat Python package.
//!
//! The pure parts of the core: UUIDv7 ids, the hybrid logical clock, branch
//! gids and the migration file format. The Python package uses them when this
//! module is installed and keeps its own implementation otherwise; both pass
//! the vectors in `conformance/`.
//!
//! Python callbacks and the GIL: the core holds a mutex while it calls its
//! clock or random source. A generator or clock over the system sources calls
//! no Python, so it is shared between threads and releases the GIL while it
//! works. One over Python callables (tests inject them) is bound to the thread
//! that made it: a callback may let another Python thread run, and that thread
//! would wait for the mutex while holding the GIL the callback needs back.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};

use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use rhizodat_core::clock::{ClockError, HlcTimestamp, HybridLogicalClock};
use rhizodat_core::identity::{IdError, IdGenerator};
use rhizodat_core::migrations::{Body, canonical};
use uuid::Uuid;

create_exception!(
    rhizodat_native,
    ClockSkewError,
    PyException,
    "A peer's timestamp is too far ahead."
);
create_exception!(
    rhizodat_native,
    LogicalOverflowError,
    PyException,
    "The logical counter is exhausted within one millisecond."
);
create_exception!(
    rhizodat_native,
    MigrationFormatError,
    PyValueError,
    "A migration file that does not follow the format."
);
create_exception!(
    rhizodat_native,
    AbortedError,
    PyException,
    "A meta-transaction committed nothing; `retryable` says if a re-run may help."
);
create_exception!(
    rhizodat_native,
    InDoubtError,
    PyException,
    "A meta-transaction's outcome is unknown; recovery resolves it from the journal."
);
create_exception!(
    rhizodat_native,
    InvalidUnitOfWork,
    PyValueError,
    "A Unit of Work that cannot be committed as given."
);

mod clickhouse;
mod coordinator;
mod drevo;
mod native;
mod postgres;

/// A current-thread tokio runtime for one native call: it drives the async
/// adapter and the connection tasks it spawns on the calling thread, so a native
/// commit stays single-threaded, like the Python-callback path.
pub(crate) fn runtime() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
}

/// An operational failure of a native adapter (a connection or a setup error),
/// as a Python exception. Safe to build without the GIL.
pub(crate) fn op_err<E: std::fmt::Display>(error: E) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(error.to_string())
}

/// A journal record as a dict (`state`, `dat_txn`, `dat_tx_time`), the shape the
/// in-memory and the PostgreSQL journals both return.
pub(crate) fn journal_record_dict<'py>(
    py: Python<'py>,
    state: &str,
    dat_txn: Uuid,
    dat_tx_time: Option<i64>,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("state", state)?;
    dict.set_item("dat_txn", dat_txn)?;
    dict.set_item("dat_tx_time", dat_tx_time)?;
    Ok(dict)
}

/// The first error a Python callback raised while the core was running.
#[derive(Clone, Default)]
struct Failure(Arc<Mutex<Option<PyErr>>>);

impl Failure {
    fn keep(&self, err: PyErr) {
        let mut slot = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        slot.get_or_insert(err);
    }

    fn take(&self) -> Option<PyErr> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    /// `result`, unless a callback failed: then the callback's error.
    fn check<T, E>(&self, result: Result<T, E>, convert: impl FnOnce(E) -> PyErr) -> PyResult<T> {
        match (self.take(), result) {
            (Some(err), _) => Err(err),
            (None, result) => result.map_err(convert),
        }
    }
}

/// Call `f() -> int`; a failure is kept and `fallback` returned.
fn call_int<T: for<'a> FromPyObject<'a, 'a>>(
    f: &Py<PyAny>,
    failure: &Failure,
    fallback: T,
    arg: Option<u32>,
) -> T
where
    for<'a> <T as FromPyObject<'a, 'a>>::Error: Into<PyErr>,
{
    Python::attach(|py| {
        let called = match arg {
            Some(a) => f.call1(py, (a,)),
            None => f.call0(py),
        };
        match called.and_then(|v| v.extract::<T>(py).map_err(Into::into)) {
            Ok(v) => v,
            Err(err) => {
                failure.keep(err);
                fallback
            }
        }
    })
}

fn id_error(err: IdError) -> PyErr {
    PyValueError::new_err(err.to_string())
}

fn clock_error(err: ClockError) -> PyErr {
    match err {
        ClockError::ClockSkew { .. } => ClockSkewError::new_err(err.to_string()),
        ClockError::LogicalOverflow { .. } => LogicalOverflowError::new_err(err.to_string()),
        ClockError::OutOfRange(_) => PyValueError::new_err(err.to_string()),
    }
}

// --- ids ---------------------------------------------------------------------------------

/// UUIDv7 ids from the system clock and the operating system's random source.
#[pyclass(frozen, module = "rhizodat_native")]
struct SystemIds(IdGenerator);

#[pymethods]
impl SystemIds {
    fn __call__(&self, py: Python<'_>) -> PyResult<Uuid> {
        py.detach(|| self.0.generate()).map_err(id_error)
    }
}

/// UUIDv7 ids from Python callables; usable only on the thread that made it.
#[pyclass(unsendable, module = "rhizodat_native")]
struct InjectedIds {
    inner: IdGenerator,
    failure: Failure,
}

#[pymethods]
impl InjectedIds {
    fn __call__(&self) -> PyResult<Uuid> {
        self.failure.check(self.inner.generate(), id_error)
    }
}

/// A UUIDv7 generator: `clock_ms() -> int` (Unix milliseconds) and
/// `randbits(n) -> int` (below `2**n`) default to the system's.
#[pyfunction]
#[pyo3(signature = (clock_ms = None, randbits = None))]
fn id_generator(
    py: Python<'_>,
    clock_ms: Option<Py<PyAny>>,
    randbits: Option<Py<PyAny>>,
) -> PyResult<Py<PyAny>> {
    if clock_ms.is_none() && randbits.is_none() {
        return Ok(Py::new(py, SystemIds(IdGenerator::new()))?.into_any());
    }
    let failure = Failure::default();
    let (clock_failure, rand_failure) = (failure.clone(), failure.clone());
    let clock: Box<dyn Fn() -> i64 + Send + Sync> = match clock_ms {
        Some(f) => Box::new(move || call_int(&f, &clock_failure, -1_i64, None)),
        None => Box::new(rhizodat_core::identity::system_ms),
    };
    let rand: Box<dyn Fn(u32) -> u64 + Send + Sync> = match randbits {
        Some(f) => Box::new(move |bits| call_int(&f, &rand_failure, 0_u64, Some(bits))),
        None => Box::new(rhizodat_core::identity::os_randbits),
    };
    let inner = IdGenerator::with_sources(clock, rand);
    Ok(Py::new(py, InjectedIds { inner, failure })?.into_any())
}

static IDS: OnceLock<IdGenerator> = OnceLock::new();

/// A new UUIDv7 from the process-wide generator.
#[pyfunction]
fn new_id(py: Python<'_>) -> PyResult<Uuid> {
    py.detach(|| IDS.get_or_init(IdGenerator::new).generate())
        .map_err(id_error)
}

/// True for an RFC 9562 version 7 UUID.
#[pyfunction]
fn is_uuid7(value: Uuid) -> bool {
    rhizodat_core::identity::is_uuid7(value)
}

/// The Unix milliseconds in a UUIDv7, `None` for any other UUID.
#[pyfunction]
fn uuid7_ms(value: Uuid) -> Option<u64> {
    rhizodat_core::identity::timestamp_ms(value)
}

// --- clock -------------------------------------------------------------------------------

fn decode(raw: i64) -> PyResult<HlcTimestamp> {
    HlcTimestamp::decode(raw).map_err(clock_error)
}

/// An HLC over the system wall clock; timestamps are their wire form (`int`).
#[pyclass(frozen, module = "rhizodat_native")]
struct SystemClock(HybridLogicalClock);

#[pymethods]
impl SystemClock {
    fn now(&self, py: Python<'_>) -> PyResult<i64> {
        py.detach(|| self.0.now())
            .map(HlcTimestamp::encode)
            .map_err(clock_error)
    }

    fn update(&self, py: Python<'_>, remote: i64) -> PyResult<i64> {
        let remote = decode(remote)?;
        py.detach(|| self.0.update(remote))
            .map(HlcTimestamp::encode)
            .map_err(clock_error)
    }

    #[getter]
    fn last(&self) -> Option<i64> {
        self.0.last().map(HlcTimestamp::encode)
    }
}

/// An HLC over a Python wall clock; usable only on the thread that made it.
#[pyclass(unsendable, module = "rhizodat_native")]
struct InjectedClock {
    inner: HybridLogicalClock,
    failure: Failure,
}

#[pymethods]
impl InjectedClock {
    fn now(&self) -> PyResult<i64> {
        self.failure
            .check(self.inner.now(), clock_error)
            .map(HlcTimestamp::encode)
    }

    fn update(&self, remote: i64) -> PyResult<i64> {
        let remote = decode(remote)?;
        self.failure
            .check(self.inner.update(remote), clock_error)
            .map(HlcTimestamp::encode)
    }

    #[getter]
    fn last(&self) -> Option<i64> {
        self.inner.last().map(HlcTimestamp::encode)
    }
}

/// A hybrid logical clock: `wall_ms() -> int` (Unix milliseconds) defaults to
/// the system's; `max_offset_ms` is how far ahead a peer's timestamp may be.
#[pyfunction]
#[pyo3(signature = (wall_ms = None, max_offset_ms = HybridLogicalClock::DEFAULT_MAX_OFFSET_MS))]
fn hybrid_logical_clock(
    py: Python<'_>,
    wall_ms: Option<Py<PyAny>>,
    max_offset_ms: u64,
) -> PyResult<Py<PyAny>> {
    let Some(f) = wall_ms else {
        let clock =
            HybridLogicalClock::with_wall(rhizodat_core::identity::system_ms, max_offset_ms);
        return Ok(Py::new(py, SystemClock(clock))?.into_any());
    };
    let failure = Failure::default();
    let kept = failure.clone();
    let inner =
        HybridLogicalClock::with_wall(move || call_int(&f, &kept, -1_i64, None), max_offset_ms);
    Ok(Py::new(py, InjectedClock { inner, failure })?.into_any())
}

// --- gids --------------------------------------------------------------------------------

/// The gid of the branch of `dat_txn` in `store`: `dat:<dat_txn>:<store>`.
#[pyfunction]
fn gid_for(dat_txn: Uuid, store: &str) -> String {
    rhizodat_core::gid::gid_for(dat_txn, store)
}

/// `(dat_txn, store)` for a gid written by `gid_for`, `None` for anything else.
#[pyfunction]
fn parse_gid(gid: &str) -> Option<(Uuid, String)> {
    rhizodat_core::gid::parse_gid(gid)
}

// --- migrations --------------------------------------------------------------------------

fn body<'py>(py: Python<'py>, value: &Body) -> PyResult<Bound<'py, PyAny>> {
    Ok(match value {
        Body::Text(s) => s.into_pyobject(py)?.into_any(),
        Body::Int(i) => i.into_pyobject(py)?.into_any(),
        Body::Bool(b) => b.into_pyobject(py)?.to_owned().into_any(),
        Body::List(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(body(py, item)?)?;
            }
            list.into_any()
        }
        Body::Table(map) => table(py, map)?.into_any(),
    })
}

fn table<'py>(py: Python<'py>, map: &BTreeMap<String, Body>) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    for (k, v) in map {
        dict.set_item(k, body(py, v)?)?;
    }
    Ok(dict)
}

/// A migration file read from `filename`: a dict of `id`, `name`,
/// `description`, `steps` (`store`, `kind`, `body`), `canonical` and `checksum`.
#[pyfunction]
fn parse_migration<'py>(
    py: Python<'py>,
    filename: &str,
    text: &str,
) -> PyResult<Bound<'py, PyDict>> {
    let m = rhizodat_core::migrations::parse_migration(filename, text)
        .map_err(|e| MigrationFormatError::new_err(e.to_string()))?;
    let steps = PyList::empty(py);
    for step in &m.steps {
        let s = PyDict::new(py);
        s.set_item("store", &step.store)?;
        s.set_item("kind", &step.kind)?;
        s.set_item("body", body(py, &step.body)?)?;
        steps.append(s)?;
    }
    let out = PyDict::new(py);
    out.set_item("id", m.id)?;
    out.set_item("name", &m.name)?;
    out.set_item("description", &m.description)?;
    out.set_item("steps", steps)?;
    out.set_item("canonical", canonical(&m))?;
    out.set_item("checksum", &m.checksum)?;
    Ok(out)
}

#[pymodule]
fn rhizodat_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("ClockSkewError", py.get_type::<ClockSkewError>())?;
    m.add(
        "LogicalOverflowError",
        py.get_type::<LogicalOverflowError>(),
    )?;
    m.add(
        "MigrationFormatError",
        py.get_type::<MigrationFormatError>(),
    )?;
    m.add("AbortedError", py.get_type::<AbortedError>())?;
    m.add("InDoubtError", py.get_type::<InDoubtError>())?;
    m.add("InvalidUnitOfWork", py.get_type::<InvalidUnitOfWork>())?;
    m.add_class::<coordinator::MemoryJournal>()?;
    m.add_class::<coordinator::Coordinator>()?;
    m.add_class::<postgres::PostgresStore>()?;
    m.add_class::<postgres::PostgresJournal>()?;
    m.add_class::<drevo::DrevoStore>()?;
    m.add_class::<drevo::DrevoJournal>()?;
    m.add_class::<clickhouse::ClickHouseStore>()?;
    m.add_class::<clickhouse::ClickHouseUnit>()?;
    m.add_function(wrap_pyfunction!(id_generator, m)?)?;
    m.add_function(wrap_pyfunction!(new_id, m)?)?;
    m.add_function(wrap_pyfunction!(is_uuid7, m)?)?;
    m.add_function(wrap_pyfunction!(uuid7_ms, m)?)?;
    m.add_function(wrap_pyfunction!(hybrid_logical_clock, m)?)?;
    m.add_function(wrap_pyfunction!(gid_for, m)?)?;
    m.add_function(wrap_pyfunction!(parse_gid, m)?)?;
    m.add_function(wrap_pyfunction!(parse_migration, m)?)?;
    Ok(())
}
