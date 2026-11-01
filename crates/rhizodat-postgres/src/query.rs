//! Native SQL outside any meta-transaction, the port of the Python adapter's
//! `execute`: one statement with `$n` parameters, its result as rows of
//! [`RowValue`]s (the driver gives rows, not Arrow).
//!
//! The statement is prepared first, so the server names the parameter and
//! column types: a parameter is bound by its target type (an integer to
//! `int2` / `int4` / `int8` / `float8`, a string to any text type, `uuid`,
//! `json` / `jsonb`, …), and a column is decoded by its type. A value or a
//! column type the port cannot carry is refused with the cast that would
//! (`$1::text::numeric`, `price::text`).

use std::error::Error;
use std::fmt;

use chrono::{DateTime, NaiveDate, NaiveDateTime, SecondsFormat, Utc};
use rhizodat_core::rows::RowValue;
use rhizodat_core::store::BoxError;
use tokio_postgres::types::{IsNull, ToSql, Type, to_sql_checked};
use tokio_postgres::{Client, Row};
use uuid::Uuid;

/// A parameter or a column the port cannot carry (not the server's error).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryError(pub String);

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for QueryError {}

/// A statement's result: its columns (also for a statement that returns no
/// rows) and its rows, values in column order.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueryResult {
    /// The column names, in order (empty for a statement without a result set).
    pub columns: Vec<String>,
    /// The rows, each in column order.
    pub rows: Vec<Vec<RowValue>>,
}

/// Run `sql` on `client` with `params` bound to its `$1..$n`.
///
/// # Errors
///
/// [`QueryError`] for a parameter or a column the port cannot carry, else the
/// server's error.
pub async fn query(
    client: &Client,
    sql: &str,
    params: &[RowValue],
) -> Result<QueryResult, BoxError> {
    let statement = client.prepare(sql).await.map_err(server_error)?;
    if statement.params().len() != params.len() {
        return Err(QueryError(format!(
            "the statement takes {} parameter(s) ($1..), {} given",
            statement.params().len(),
            params.len()
        ))
        .into());
    }
    let bound: Vec<Bind<'_>> = params
        .iter()
        .zip(statement.params())
        .enumerate()
        .map(|(i, (value, ty))| Bind::new(i + 1, value, ty))
        .collect::<Result<_, _>>()?;
    let refs: Vec<&(dyn ToSql + Sync)> = bound.iter().map(|b| b as &(dyn ToSql + Sync)).collect();
    let columns: Vec<String> = statement
        .columns()
        .iter()
        .map(|c| c.name().to_owned())
        .collect();
    let rows = client
        .query(&statement, &refs)
        .await
        .map_err(server_error)?;
    let rows = rows
        .iter()
        .map(|row| decode_row(row, &statement))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(QueryResult { columns, rows })
}

/// The driver's error with the server's message in front: `tokio_postgres::Error`
/// displays as "db error" and keeps the message in its source.
fn server_error(error: tokio_postgres::Error) -> BoxError {
    match error.as_db_error() {
        Some(db) => QueryError(format!("{}: {}", db.code().code(), db.message())).into(),
        None => error.into(),
    }
}

/// One parameter, checked against its target type when bound.
struct Bind<'a> {
    value: &'a RowValue,
}

impl<'a> Bind<'a> {
    fn new(n: usize, value: &'a RowValue, ty: &Type) -> Result<Self, QueryError> {
        let fits = match value {
            RowValue::Null => true,
            RowValue::Bool(_) => *ty == Type::BOOL,
            RowValue::Int(_) => {
                matches!(*ty, Type::INT2 | Type::INT4 | Type::INT8 | Type::FLOAT8)
            }
            RowValue::Float(_) => matches!(*ty, Type::FLOAT4 | Type::FLOAT8),
            RowValue::Str(_) => is_text(ty) || matches!(*ty, Type::UUID | Type::JSON | Type::JSONB),
            RowValue::Uuid(_) => *ty == Type::UUID || is_text(ty),
            RowValue::Bytes(_) => *ty == Type::BYTEA,
            RowValue::Decimal(_)
            | RowValue::Date(_)
            | RowValue::DateTime(_)
            | RowValue::List(_)
            | RowValue::Map(_) => false,
        };
        if fits {
            Ok(Self { value })
        } else {
            Err(QueryError(format!(
                "cannot bind ${n}: {value:?} does not fit the {ty} parameter; \
                 cast it in SQL (${n}::text::{ty}) or pass a value of that type"
            )))
        }
    }
}

fn is_text(ty: &Type) -> bool {
    matches!(
        *ty,
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN
    )
}

impl fmt::Debug for Bind<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.value.fmt(f)
    }
}

impl ToSql for Bind<'_> {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        match self.value {
            RowValue::Null => Ok(IsNull::Yes),
            RowValue::Bool(v) => v.to_sql(ty, out),
            RowValue::Int(v) => match *ty {
                Type::INT2 => i16::try_from(*v)?.to_sql(ty, out),
                Type::INT4 => i32::try_from(*v)?.to_sql(ty, out),
                #[allow(clippy::cast_precision_loss)]
                Type::FLOAT8 => (*v as f64).to_sql(ty, out),
                _ => v.to_sql(ty, out),
            },
            RowValue::Float(v) => match *ty {
                #[allow(clippy::cast_possible_truncation)]
                Type::FLOAT4 => (*v as f32).to_sql(ty, out),
                _ => v.to_sql(ty, out),
            },
            RowValue::Str(v) => match *ty {
                Type::UUID => v.parse::<Uuid>()?.to_sql(ty, out),
                Type::JSON | Type::JSONB => {
                    serde_json::from_str::<serde_json::Value>(v)?.to_sql(ty, out)
                }
                _ => v.as_str().to_sql(ty, out),
            },
            RowValue::Uuid(v) => match *ty {
                Type::UUID => v.to_sql(ty, out),
                _ => v.hyphenated().to_string().to_sql(ty, out),
            },
            RowValue::Bytes(v) => v.as_slice().to_sql(ty, out),
            other => Err(format!("{other:?} cannot be bound").into()),
        }
    }

    fn accepts(_ty: &Type) -> bool {
        true // checked by type in `Bind::new`
    }

    to_sql_checked!();
}

fn decode_row(row: &Row, statement: &tokio_postgres::Statement) -> Result<Vec<RowValue>, BoxError> {
    statement
        .columns()
        .iter()
        .enumerate()
        .map(|(i, column)| decode(row, i, column.name(), column.type_()))
        .collect()
}

fn decode(row: &Row, i: usize, name: &str, ty: &Type) -> Result<RowValue, BoxError> {
    Ok(match *ty {
        Type::BOOL => row
            .try_get::<_, Option<bool>>(i)?
            .map_or(RowValue::Null, RowValue::Bool),
        Type::INT2 => row
            .try_get::<_, Option<i16>>(i)?
            .map_or(RowValue::Null, |v| RowValue::Int(v.into())),
        Type::INT4 => row
            .try_get::<_, Option<i32>>(i)?
            .map_or(RowValue::Null, |v| RowValue::Int(v.into())),
        Type::INT8 => row
            .try_get::<_, Option<i64>>(i)?
            .map_or(RowValue::Null, RowValue::Int),
        Type::FLOAT4 => row
            .try_get::<_, Option<f32>>(i)?
            .map_or(RowValue::Null, |v| RowValue::Float(v.into())),
        Type::FLOAT8 => row
            .try_get::<_, Option<f64>>(i)?
            .map_or(RowValue::Null, RowValue::Float),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN => row
            .try_get::<_, Option<String>>(i)?
            .map_or(RowValue::Null, RowValue::Str),
        Type::UUID => row
            .try_get::<_, Option<Uuid>>(i)?
            .map_or(RowValue::Null, RowValue::Uuid),
        Type::BYTEA => row
            .try_get::<_, Option<Vec<u8>>>(i)?
            .map_or(RowValue::Null, RowValue::Bytes),
        Type::JSON | Type::JSONB => row
            .try_get::<_, Option<serde_json::Value>>(i)?
            .map_or(Ok(RowValue::Null), |v| json_value(&v))?,
        Type::TIMESTAMPTZ => row
            .try_get::<_, Option<DateTime<Utc>>>(i)?
            .map_or(RowValue::Null, |v| {
                RowValue::DateTime(v.to_rfc3339_opts(SecondsFormat::AutoSi, true))
            }),
        Type::TIMESTAMP => row
            .try_get::<_, Option<NaiveDateTime>>(i)?
            .map_or(RowValue::Null, |v| {
                RowValue::DateTime(v.format("%Y-%m-%dT%H:%M:%S%.f").to_string())
            }),
        Type::DATE => row
            .try_get::<_, Option<NaiveDate>>(i)?
            .map_or(RowValue::Null, |v| {
                RowValue::Date(v.format("%Y-%m-%d").to_string())
            }),
        _ => {
            return Err(QueryError(format!(
                "column {name:?} is {ty}, which the native path does not decode; \
                 select it as text ({name}::text)"
            ))
            .into());
        }
    })
}

/// A JSON document as the values a `jsonb` column's reader gives the Python
/// adapter: objects as maps, arrays as lists, numbers as integers when whole.
fn json_value(v: &serde_json::Value) -> Result<RowValue, BoxError> {
    Ok(match v {
        serde_json::Value::Null => RowValue::Null,
        serde_json::Value::Bool(b) => RowValue::Bool(*b),
        serde_json::Value::Number(n) => match (n.as_i64(), n.as_f64()) {
            (Some(i), _) => RowValue::Int(i),
            (None, Some(f)) => RowValue::Float(f),
            (None, None) => return Err(QueryError(format!("not a number: {n}")).into()),
        },
        serde_json::Value::String(s) => RowValue::Str(s.clone()),
        serde_json::Value::Array(items) => {
            RowValue::List(items.iter().map(json_value).collect::<Result<_, _>>()?)
        }
        serde_json::Value::Object(fields) => RowValue::Map(
            fields
                .iter()
                .map(|(k, x)| Ok((k.clone(), json_value(x)?)))
                .collect::<Result<_, BoxError>>()?,
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_value_is_checked_against_its_parameter_type() {
        assert!(Bind::new(1, &RowValue::Int(1), &Type::INT4).is_ok());
        assert!(Bind::new(1, &RowValue::Null, &Type::TIMESTAMPTZ).is_ok());
        assert!(Bind::new(1, &RowValue::Str("x".into()), &Type::JSONB).is_ok());
        assert!(Bind::new(1, &RowValue::Uuid(Uuid::nil()), &Type::TEXT).is_ok());
        let err = Bind::new(2, &RowValue::Decimal("1.5".into()), &Type::NUMERIC).unwrap_err();
        assert!(
            err.0.contains("$2") && err.0.contains("::text::numeric"),
            "{err}"
        );
        let err = Bind::new(1, &RowValue::Str("x".into()), &Type::INT8).unwrap_err();
        assert!(err.0.contains("int8"), "{err}");
    }

    #[test]
    fn json_documents_become_row_values() {
        let v: serde_json::Value =
            serde_json::from_str(r#"{"n": 1, "f": 1.5, "l": [true, null, "s"]}"#).unwrap();
        let RowValue::Map(fields) = json_value(&v).unwrap() else {
            panic!("not a map")
        };
        assert_eq!(fields.len(), 3);
        assert!(
            fields
                .iter()
                .any(|(k, v)| k == "n" && *v == RowValue::Int(1))
        );
        assert!(
            fields
                .iter()
                .any(|(k, v)| k == "f" && *v == RowValue::Float(1.5))
        );
    }
}
