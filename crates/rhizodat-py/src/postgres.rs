//! The real Rust PostgreSQL adapter behind the Python coordinator.
//!
//! [`PostgresStore`] wraps [`rhizodat_postgres::PostgresStore`] (its branch and
//! participant) and [`PostgresJournal`] wraps the shared, durable journal. A
//! Python Unit of Work hands native stores to [`crate::coordinator::Coordinator`]
//! and the core runs the whole commit protocol against real PostgreSQL, with no
//! Python callbacks on the path. Each native call runs on its own current-thread
//! runtime ([`crate::runtime`]); a journal reconnects between calls as needed.

use std::sync::{Arc, Mutex};

use pyo3::prelude::*;
use pyo3::types::PyDict;
use rhizodat_core::journal::Journal;
use rhizodat_core::rows::RowValue;
use rhizodat_postgres::parts::{
    ColumnType, PartSpec as CoreSpec, PartVersion, PendingVersion, PostgresParts as CoreParts,
};
use rhizodat_postgres::query::query;
use rhizodat_postgres::{
    PostgresJournal as CoreJournal, PostgresRows as CoreRows, PostgresStore as CoreStore,
};
use uuid::Uuid;

use crate::native::{NativeStore, Postgres, PostgresPending};
use crate::result::rows_result;
use crate::rows::{fields_dict, row_value};
use crate::{InvalidUnitOfWork, journal_record_dict, op_err, runtime};

/// `execute`'s `params` as the statement's `$1..$n` values: a sequence, in
/// order (the native path binds by position; a mapping names nothing here).
fn positional_params(params: Option<&Bound<'_, PyAny>>) -> PyResult<Vec<RowValue>> {
    let Some(params) = params else {
        return Ok(Vec::new());
    };
    if params.is_instance_of::<PyDict>() || params.hasattr("keys")? {
        return Err(InvalidUnitOfWork::new_err(
            "the native PostgreSQL store binds parameters by position: pass a sequence \
             for $1..$n, not a mapping (use the Python adapter for %(name)s placeholders)",
        ));
    }
    if params.is_instance_of::<pyo3::types::PyString>()
        || params.is_instance_of::<pyo3::types::PyBytes>()
    {
        return Err(InvalidUnitOfWork::new_err(
            "params must be a sequence of values for $1..$n",
        ));
    }
    params
        .try_iter()?
        .enumerate()
        .map(|(i, value)| {
            row_value(&value?).map_err(|e| {
                InvalidUnitOfWork::new_err(format!(
                    "cannot bind ${}: {}",
                    i + 1,
                    e.value(params.py())
                ))
            })
        })
        .collect()
}

/// One PostgreSQL database as a rhizodat store, for a native Unit of Work.
#[pyclass(module = "rhizodat_native")]
pub struct PostgresStore(Arc<CoreStore>);

impl PostgresStore {
    /// The store as a [`NativeStore`] for the coordinator.
    pub(crate) fn native(&self) -> Arc<dyn NativeStore> {
        Arc::new(Postgres(self.0.clone()))
    }
}

#[pymethods]
impl PostgresStore {
    /// A store named `name` over `dsn`, optionally confined to `schema` (so that
    /// no two stores share one schema's marker table).
    #[new]
    #[pyo3(signature = (dsn, name, schema = None))]
    fn new(dsn: &str, name: &str, schema: Option<&str>) -> PyResult<Self> {
        let store = CoreStore::new(dsn, name, schema)
            .map_err(|e| InvalidUnitOfWork::new_err(e.to_string()))?;
        Ok(Self(Arc::new(store)))
    }

    /// The store name.
    #[getter]
    fn name(&self) -> &str {
        self.0.name()
    }

    /// Create the service table `dat_applied` if it does not exist.
    fn install(&self, py: Python<'_>) -> PyResult<()> {
        let store = self.0.clone();
        py.detach(|| {
            let runtime = runtime().map_err(op_err)?;
            runtime.block_on(store.install()).map_err(op_err)
        })
    }

    /// A Unit of Work's branch of this store, to buffer rows and then commit.
    /// Run `sql` in autocommit mode outside any meta-transaction, as the Python
    /// adapter's `execute`, with `params` (a sequence) bound to `$1..$n` by the
    /// statement's parameter types. The result is a `rhizodat.core.result.Result`
    /// of dicts; a column type the native path does not decode is refused with
    /// the cast to use (`col::text`).
    #[pyo3(signature = (sql, params = None))]
    fn execute<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let params = positional_params(params)?;
        let (store, sql) = (self.0.clone(), sql.to_owned());
        let result = py.detach(|| {
            let runtime = runtime().map_err(op_err)?;
            runtime
                .block_on(async {
                    let client = store.connect().await?;
                    query(&client, &sql, &params).await
                })
                .map_err(op_err)
        })?;
        rows_result(py, &result.columns, &result.rows)
    }

    fn unit(&self) -> PostgresUnit {
        PostgresUnit {
            store: self.0.clone(),
            rows: Mutex::new(CoreRows::new()),
            parts: Mutex::new(CoreParts::new()),
        }
    }

    /// Create the part's table and index if missing. Idempotent.
    fn install_part(&self, py: Python<'_>, spec: &PartSpec) -> PyResult<()> {
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
        spec: &PartSpec,
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
        spec: &PartSpec,
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
        spec: &PartSpec,
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
        spec: &PartSpec,
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

/// One store's share of one entity type: `PartSpec(type_name, table, columns)`
/// with `columns` the `(field, PostgreSQL type)` pairs in order (`text`,
/// `boolean`, `bigint`, `double precision`, `numeric`, `timestamptz`, `date`,
/// `uuid`, `bytea`, `jsonb`).
#[pyclass(module = "rhizodat_native", frozen)]
pub struct PartSpec(CoreSpec);

#[pymethods]
impl PartSpec {
    #[new]
    fn new(type_name: &str, table: &str, columns: Vec<(String, String)>) -> PyResult<Self> {
        let columns = columns
            .into_iter()
            .map(|(name, ty)| Ok((name, ty.parse::<ColumnType>()?)))
            .collect::<Result<Vec<_>, rhizodat_postgres::ConfigError>>()
            .map_err(|e| InvalidUnitOfWork::new_err(e.to_string()))?;
        CoreSpec::new(type_name, table, columns)
            .map(Self)
            .map_err(|e| InvalidUnitOfWork::new_err(e.to_string()))
    }

    /// The table.
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

/// One Unit of Work's buffered rows for a PostgreSQL store. Buffer with
/// [`PostgresUnit::insert`], then hand it to `Coordinator.commit` like a store:
/// its rows are written inside the branch's transaction, so they commit
/// atomically with the meta-transaction through the two-phase protocol. The unit
/// may be reused and keeps its rows across a retry of the commit.
#[pyclass(module = "rhizodat_native")]
pub struct PostgresUnit {
    store: Arc<CoreStore>,
    rows: Mutex<CoreRows>,
    parts: Mutex<CoreParts>,
}

impl PostgresUnit {
    /// A native store carrying a snapshot of the buffered rows, for the
    /// coordinator; the unit itself is left intact.
    pub(crate) fn pending(&self) -> Arc<dyn NativeStore> {
        let rows = self
            .rows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let parts = self
            .parts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        Arc::new(PostgresPending {
            store: self.store.clone(),
            rows,
            parts,
        })
    }
}

#[pymethods]
impl PostgresUnit {
    /// Buffer `rows` (one value per column of `columns`) for `table`.
    // PyO3 extracts method arguments by value.
    #[allow(clippy::needless_pass_by_value)]
    fn insert(
        &self,
        py: Python<'_>,
        table: &str,
        columns: Vec<String>,
        rows: Vec<Vec<Py<PyAny>>>,
    ) -> PyResult<()> {
        let rows: Vec<Vec<_>> = rows
            .into_iter()
            .map(|row| row.iter().map(|v| row_value(v.bind(py))).collect())
            .collect::<PyResult<_>>()?;
        let column_refs: Vec<&str> = columns.iter().map(String::as_str).collect();
        self.rows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(table, &column_refs, rows)
            .map_err(|e| InvalidUnitOfWork::new_err(e.to_string()))
    }

    /// Buffer a version of `dat_id`'s part: `values` by field (missing ones are
    /// `NULL`), `created` the time of the first version (`None`: this one).
    #[pyo3(signature = (spec, dat_id, values, *, created, deleted = false))]
    fn write_part(
        &self,
        spec: &PartSpec,
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

    /// Buffer the physical removal of every version of `dat_id`'s part.
    fn erase_part(&self, spec: &PartSpec, dat_id: Uuid) {
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
        spec: &PartSpec,
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

/// The journal in PostgreSQL: shared and durable, for Units of Work over several
/// stores and for recovery.
#[pyclass(module = "rhizodat_native")]
pub struct PostgresJournal(Arc<CoreJournal>);

impl PostgresJournal {
    /// The wrapped core journal as a trait object, for the coordinator.
    pub(crate) fn as_journal(&self) -> Arc<dyn Journal> {
        self.0.clone()
    }
}

#[pymethods]
impl PostgresJournal {
    /// Open (and create if needed) the journal table `table` in `schema` (or the
    /// DSN's `schema=` parameter).
    #[new]
    #[pyo3(signature = (dsn, schema = None, table = "dat_journal"))]
    fn new(py: Python<'_>, dsn: &str, schema: Option<&str>, table: &str) -> PyResult<Self> {
        let journal = py.detach(|| {
            runtime()
                .map_err(op_err)?
                .block_on(CoreJournal::open(dsn, schema, table))
                .map_err(op_err)
        })?;
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
