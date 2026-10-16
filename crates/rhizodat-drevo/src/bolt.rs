//! drevo over Bolt (neo4rs): the `drevo.tx.*` procedures for two-phase commit.
//!
//! Bolt has no distinct code for an unknown gid
//! ([ice1x/drevo#590](https://github.com/ice1x/drevo/issues/590)); the server's
//! message is the only signal. drevo accepts the `drevo.tx.*` resolution
//! procedures only outside an explicit transaction
//! ([ice1x/drevo#608](https://github.com/ice1x/drevo/issues/608)). Result
//! streams hold a pooled connection until dropped, so every query collects its
//! rows first.

use std::collections::BTreeMap;
use std::error::Error;

use async_trait::async_trait;
use neo4rs::{
    BoltBoolean, BoltFloat, BoltInteger, BoltList, BoltNull, BoltString, BoltType, ConfigBuilder,
    Graph, Neo4jClientErrorKind, Neo4jErrorKind, Query, Txn, query,
};
use rhizodat_core::dsn::Dsn;
use rhizodat_core::store::BoxError;

use crate::{Backend, ConfigError, Params, Resolved, Row, Tx, Value};

const UNKNOWN_GID: &str = "no transaction is prepared as";

/// The prepared fence, lost connections and other transient server errors;
/// never Cypher or constraint errors.
pub(crate) fn is_retryable(error: &(dyn Error + 'static)) -> bool {
    let Some(error) = error.downcast_ref::<neo4rs::Error>() else {
        return error.is::<std::io::Error>();
    };
    match error {
        neo4rs::Error::Neo4j(e) => matches!(
            e.kind(),
            Neo4jErrorKind::Transient
                | Neo4jErrorKind::Client(Neo4jClientErrorKind::SessionExpired)
        ),
        neo4rs::Error::IOError { .. }
        | neo4rs::Error::ConnectionError
        | neo4rs::Error::ConnectionTimedOut
        | neo4rs::Error::ServerUnavailableError(_)
        | neo4rs::Error::RequestIgnoredError => true,
        _ => false,
    }
}

fn is_unknown_gid(error: &neo4rs::Error) -> bool {
    matches!(error, neo4rs::Error::Neo4j(e) if e.message().contains(UNKNOWN_GID))
}

fn bolt(value: Value) -> BoltType {
    match value {
        Value::Null => BoltType::Null(BoltNull),
        Value::Bool(v) => BoltType::Boolean(BoltBoolean::new(v)),
        Value::Int(v) => BoltType::Integer(BoltInteger::new(v)),
        Value::Float(v) => BoltType::Float(BoltFloat::new(v)),
        Value::Str(v) => BoltType::String(BoltString::from(v)),
        Value::List(v) => {
            BoltType::List(BoltList::from(v.into_iter().map(bolt).collect::<Vec<_>>()))
        }
    }
}

fn cypher(text: &str, params: Params) -> Query {
    params
        .into_iter()
        .fold(query(text), |q, (name, value)| q.param(&name, bolt(value)))
}

fn row(found: &neo4rs::Row) -> Result<Row, BoxError> {
    let mut out = BTreeMap::new();
    for key in found.keys() {
        out.insert(key.value.clone(), found.get::<Value>(&key.value)?);
    }
    Ok(Row(out))
}

pub(crate) struct Bolt {
    graph: Graph,
}

impl Bolt {
    pub(crate) fn connect(dsn: &Dsn) -> Result<Self, ConfigError> {
        let invalid = || ConfigError(format!("invalid drevo connection string: {dsn}"));
        let config = ConfigBuilder::new()
            .uri(format!(
                "bolt://{}:{}",
                dsn.host().unwrap_or_default(),
                dsn.port().unwrap_or(7687)
            ))
            .user(dsn.user().unwrap_or_default())
            .password(dsn.password().unwrap_or_default())
            .build()
            .map_err(|_| invalid())?;
        Ok(Self {
            graph: Graph::connect(config).map_err(|_| invalid())?,
        })
    }
}

#[async_trait]
impl Backend for Bolt {
    async fn read(&self, text: &str, params: Params) -> Result<Vec<Row>, BoxError> {
        let mut stream = self.graph.execute(cypher(text, params)).await?;
        let mut out = Vec::new();
        while let Some(found) = stream.next().await? {
            out.push(row(&found)?);
        }
        Ok(out) // the stream (and its pooled connection) is released here
    }

    async fn begin(&self) -> Result<Box<dyn Tx>, BoxError> {
        Ok(Box::new(BoltTx(self.graph.start_txn().await?)))
    }

    async fn resolve(&self, commit: bool, gid: &str) -> Result<Resolved, BoxError> {
        let name = if commit {
            "commitPrepared"
        } else {
            "rollbackPrepared"
        };
        let q = query(&format!("CALL drevo.tx.{name}($gid)")).param("gid", gid);
        let result = async {
            let mut stream = self.graph.execute(q).await?;
            while stream.next().await?.is_some() {}
            Ok::<(), neo4rs::Error>(())
        }
        .await;
        match result {
            Ok(()) => Ok(Resolved::Done),
            Err(e) if is_unknown_gid(&e) => Ok(Resolved::UnknownGid),
            Err(e) => Err(e.into()),
        }
    }

    async fn list_prepared(&self) -> Result<Vec<String>, BoxError> {
        let found = self
            .read("CALL drevo.tx.listPrepared()", Vec::new())
            .await?;
        Ok(found
            .iter()
            .map(|r| r.str("gid").map(str::to_owned))
            .collect::<Result<_, _>>()?)
    }

    fn is_retryable(&self, error: &(dyn Error + 'static)) -> bool {
        is_retryable(error)
    }
}

struct BoltTx(Txn);

#[async_trait]
impl Tx for BoltTx {
    async fn run(&mut self, text: &str, params: Params) -> Result<Vec<Row>, BoxError> {
        let mut stream = self.0.execute(cypher(text, params)).await?;
        let mut out = Vec::new();
        while let Some(found) = stream.next(self.0.handle()).await? {
            out.push(row(&found)?);
        }
        Ok(out)
    }

    async fn prepare(self: Box<Self>, gid: &str) -> Result<(), BoxError> {
        let mut txn = self.0;
        txn.run(query("CALL drevo.tx.prepare($gid)").param("gid", gid))
            .await?;
        txn.commit().await?; // closes the Bolt transaction; the server keeps it prepared
        Ok(())
    }

    async fn commit(self: Box<Self>) -> Result<(), BoxError> {
        Ok(self.0.commit().await?)
    }

    async fn rollback(self: Box<Self>) -> Result<(), BoxError> {
        Ok(self.0.rollback().await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_errors_are_retryable_and_others_are_not() {
        assert!(is_retryable(&std::io::Error::other("reset")));
        assert!(!is_retryable(&ConfigError("x".into())));
        assert!(is_retryable(&neo4rs::Error::ConnectionError));
        assert!(!is_retryable(&neo4rs::Error::ConversionError));
    }
}
