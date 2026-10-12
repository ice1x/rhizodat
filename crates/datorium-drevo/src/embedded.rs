//! drevo embedded in the process: the `drevo` crate's native engine
//! (`NativeService`), durable (`drevo:///path`, a write-ahead log) or in memory
//! (`drevo+mem://`).
//!
//! The engine's calls are synchronous (a write is fsynced before it returns),
//! so each one runs on tokio's blocking pool. Its two-phase errors are typed:
//! an unknown gid is `ResolveError::UnknownGid`, the fence is
//! `PreparedPending`. One process opens a durable graph once: share the store
//! (or open the journal with `DrevoJournal::in_graph_of`), never the path.

use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datorium_core::store::BoxError;
use drevo::cypher::executor::{ExecError, ExecResult, Value as Native};
use drevo::error::DrevoError;
use drevo::native::{CommitError, NativeTxId, PrepareError, ResolveError};
use drevo::native_service::NativeService;

use crate::{Backend, ConfigError, Params, Resolved, Row, Tx, Value};

/// An embedded engine error, classified once.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EmbeddedError {
    message: String,
    retryable: bool,
}

impl fmt::Display for EmbeddedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for EmbeddedError {}

fn failure(message: String, retryable: bool) -> BoxError {
    Box::new(EmbeddedError { message, retryable })
}

fn exec_error(e: &ExecError) -> BoxError {
    let fence = matches!(
        e,
        ExecError::Storage(DrevoError::PreparedTransactionPending(_))
    );
    failure(e.to_string(), fence)
}

fn commit_error(e: &CommitError) -> BoxError {
    let retryable = matches!(e, CommitError::Conflict | CommitError::PreparedPending(_));
    failure(format!("commit failed: {e:?}"), retryable)
}

fn prepare_error(e: &PrepareError) -> BoxError {
    let retryable = matches!(e, PrepareError::Conflict | PrepareError::PreparedPending(_));
    failure(format!("prepare failed: {e:?}"), retryable)
}

fn native(value: Value) -> Native {
    match value {
        Value::Null => Native::Null,
        Value::Bool(v) => Native::Bool(v),
        Value::Int(v) => Native::Integer(v),
        Value::Float(v) => Native::Float(v),
        Value::Str(v) => Native::String(v),
        Value::List(v) => Native::List(v.into_iter().map(native).collect()),
    }
}

fn value(native: Native) -> Result<Value, BoxError> {
    Ok(match native {
        Native::Null => Value::Null,
        Native::Bool(v) => Value::Bool(v),
        Native::Integer(v) => Value::Int(v),
        Native::Float(v) => Value::Float(v),
        Native::String(v) => Value::Str(v),
        Native::List(v) => Value::List(v.into_iter().map(value).collect::<Result<_, _>>()?),
        other => {
            return Err(failure(
                format!("unsupported result value: {other:?}"),
                false,
            ));
        }
    })
}

fn rows(result: ExecResult) -> Result<Vec<Row>, BoxError> {
    result
        .rows
        .into_iter()
        .map(|values| {
            let pairs = result.columns.iter().cloned().zip(values);
            Ok(Row(pairs
                .map(|(c, v)| Ok((c, value(v)?)))
                .collect::<Result<BTreeMap<_, _>, BoxError>>()?))
        })
        .collect()
}

fn parameters(params: Params) -> HashMap<String, Native> {
    params.into_iter().map(|(k, v)| (k, native(v))).collect()
}

/// Run a synchronous engine call on tokio's blocking pool.
async fn blocking<T: Send + 'static>(
    call: impl FnOnce() -> Result<T, BoxError> + Send + 'static,
) -> Result<T, BoxError> {
    tokio::task::spawn_blocking(call)
        .await
        .map_err(|e| failure(format!("engine call failed: {e}"), false))?
}

pub(crate) struct Embedded {
    svc: Arc<NativeService>,
}

impl Embedded {
    pub(crate) fn open(path: Option<&str>) -> Result<Self, ConfigError> {
        let svc = match path {
            Some(path) => NativeService::open(path)
                .map_err(|e| ConfigError(format!("cannot open the drevo graph at {path}: {e}")))?,
            None => NativeService::in_memory(),
        };
        Ok(Self { svc: Arc::new(svc) })
    }
}

#[async_trait]
impl Backend for Embedded {
    async fn read(&self, cypher: &str, params: Params) -> Result<Vec<Row>, BoxError> {
        let (svc, cypher) = (self.svc.clone(), cypher.to_owned());
        blocking(move || {
            let ast =
                drevo::cypher::parser::parse(&cypher).map_err(|e| failure(e.to_string(), false))?;
            rows(
                svc.execute(&ast, parameters(params))
                    .map_err(|e| exec_error(&e))?,
            )
        })
        .await
    }

    async fn begin(&self) -> Result<Box<dyn Tx>, BoxError> {
        let tx = self.svc.begin_tx();
        Ok(Box::new(EmbeddedTx {
            svc: self.svc.clone(),
            tx,
        }))
    }

    async fn resolve(&self, commit: bool, gid: &str) -> Result<Resolved, BoxError> {
        let (svc, gid) = (self.svc.clone(), gid.to_owned());
        blocking(move || {
            let result = if commit {
                svc.commit_prepared(&gid)
            } else {
                svc.rollback_prepared(&gid)
            };
            match result {
                Ok(()) => Ok(Resolved::Done),
                Err(ResolveError::UnknownGid(_)) => Ok(Resolved::UnknownGid),
                // The resolution record could not be written: the gid stays prepared.
                Err(e @ ResolveError::Io(_)) => Err(failure(format!("{e}"), true)),
                Err(e) => Err(failure(format!("{e}"), false)),
            }
        })
        .await
    }

    async fn list_prepared(&self) -> Result<Vec<String>, BoxError> {
        Ok(self
            .svc
            .list_prepared()
            .into_iter()
            .map(|p| p.gid)
            .collect())
    }

    fn is_retryable(&self, error: &(dyn Error + 'static)) -> bool {
        error
            .downcast_ref::<EmbeddedError>()
            .is_some_and(|e| e.retryable)
    }
}

struct EmbeddedTx {
    svc: Arc<NativeService>,
    tx: NativeTxId,
}

#[async_trait]
impl Tx for EmbeddedTx {
    async fn run(&mut self, cypher: &str, params: Params) -> Result<Vec<Row>, BoxError> {
        let (svc, tx, cypher) = (self.svc.clone(), self.tx, cypher.to_owned());
        blocking(move || {
            let ast =
                drevo::cypher::parser::parse(&cypher).map_err(|e| failure(e.to_string(), false))?;
            rows(
                svc.execute_in_tx(tx, &ast, parameters(params))
                    .map_err(|e| exec_error(&e))?,
            )
        })
        .await
    }

    async fn prepare(self: Box<Self>, gid: &str) -> Result<(), BoxError> {
        let (svc, tx, gid) = (self.svc.clone(), self.tx, gid.to_owned());
        blocking(move || svc.prepare_tx(tx, &gid).map_err(|e| prepare_error(&e))).await
    }

    async fn commit(self: Box<Self>) -> Result<(), BoxError> {
        let (svc, tx) = (self.svc.clone(), self.tx);
        blocking(move || svc.commit_tx(tx).map_err(|e| commit_error(&e))).await
    }

    async fn rollback(self: Box<Self>) -> Result<(), BoxError> {
        self.svc.rollback_tx(self.tx); // false: already closed, which is fine
        Ok(())
    }
}
