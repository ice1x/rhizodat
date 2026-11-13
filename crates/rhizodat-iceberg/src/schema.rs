//! Declarative Iceberg schema operations, the body of an `iceberg` migration
//! step: the port of `rhizodat.adapters.iceberg_schema`, for the operations
//! that create (`create_namespace`, `create_table`).
//!
//! An operation is a JSON object with an `op` and its arguments:
//!
//! ```json
//! {"op": "create_namespace", "namespace": "ops"}
//! {"op": "create_table", "table": "ops.api_request", "columns": [
//!     {"name": "id", "type": "string", "required": true},
//!     {"name": "at", "type": "timestamptz"}]}
//! ```
//!
//! Types: `boolean`, `int`, `long`, `float`, `double`, `date`, `time`,
//! `timestamp`, `timestamptz`, `string`, `uuid`, `binary`, `decimal(P, S)`.
//! Every operation is a no-op when its result is already there, so a step can
//! run again. The operations that change a table (`add_column`,
//! `rename_column`, `drop_column`, `add_partition`) are refused: they are not
//! yet in Rust (the Python adapter runs them).

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{NamespaceIdent, TableCreation};
use rhizodat_core::store::BoxError;
use serde_json::{Map, Value as Json};

use crate::{IcebergError, IcebergStore, ident};

/// One checked operation.
#[derive(Debug, Clone, PartialEq)]
pub enum Operation {
    /// Create a namespace if missing.
    CreateNamespace(String),
    /// Create a table (`namespace.table`) with its columns if missing.
    CreateTable {
        /// `namespace.table`.
        table: String,
        /// Name, type and whether it is required, in order.
        columns: Vec<(String, PrimitiveType, bool)>,
    },
}

fn refuse(message: impl Into<String>) -> IcebergError {
    IcebergError(message.into())
}

/// An Iceberg primitive type from its name.
///
/// # Errors
///
/// [`IcebergError`] for an unknown name.
pub fn iceberg_type(name: &str) -> Result<PrimitiveType, IcebergError> {
    Ok(match name {
        "boolean" => PrimitiveType::Boolean,
        "int" => PrimitiveType::Int,
        "long" => PrimitiveType::Long,
        "float" => PrimitiveType::Float,
        "double" => PrimitiveType::Double,
        "date" => PrimitiveType::Date,
        "time" => PrimitiveType::Time,
        "timestamp" => PrimitiveType::Timestamp,
        "timestamptz" => PrimitiveType::Timestamptz,
        "string" => PrimitiveType::String,
        "uuid" => PrimitiveType::Uuid,
        "binary" => PrimitiveType::Binary,
        other => {
            let decimal = other
                .strip_prefix("decimal(")
                .and_then(|rest| rest.strip_suffix(')'))
                .and_then(|args| args.split_once(','))
                .and_then(|(p, s)| Some((p.trim().parse().ok()?, s.trim().parse().ok()?)));
            match decimal {
                Some((precision, scale)) => PrimitiveType::Decimal { precision, scale },
                None => {
                    return Err(refuse(format!(
                        "unknown iceberg type {other:?}: boolean, int, long, float, double, \
                         date, time, timestamp, timestamptz, string, uuid, binary or decimal(P, S)"
                    )));
                }
            }
        }
    })
}

fn arguments<'a>(
    op: &'a Map<String, Json>,
    kind: &str,
    required: &[&str],
) -> Result<&'a Map<String, Json>, IcebergError> {
    let missing: Vec<&str> = required
        .iter()
        .copied()
        .filter(|k| !op.contains_key(*k))
        .collect();
    if !missing.is_empty() {
        return Err(refuse(format!("{kind} needs {}", missing.join(", "))));
    }
    let mut extra: Vec<&str> = op
        .keys()
        .map(String::as_str)
        .filter(|k| *k != "op" && !required.contains(k))
        .collect();
    extra.sort_unstable();
    if !extra.is_empty() {
        return Err(refuse(format!("{kind} takes no {}", extra.join(", "))));
    }
    Ok(op)
}

fn text<'a>(op: &'a Map<String, Json>, key: &str) -> Result<&'a str, IcebergError> {
    op[key]
        .as_str()
        .ok_or_else(|| refuse(format!("{key} must be a string")))
}

/// The operations of one step, all checked before any runs.
///
/// # Errors
///
/// [`IcebergError`] for a body that is not a list of operations, an unknown or
/// not yet ported operation, a missing or extra argument, a bad type or table name.
pub fn parse_operations(body: &Json) -> Result<Vec<Operation>, IcebergError> {
    let ops = body
        .as_array()
        .ok_or_else(|| refuse("an iceberg step is a list of operations"))?;
    ops.iter()
        .map(|op| {
            let op = op
                .as_object()
                .ok_or_else(|| refuse("an iceberg operation is an object"))?;
            match op.get("op").and_then(Json::as_str) {
                Some("create_namespace") => {
                    let op = arguments(op, "create_namespace", &["namespace"])?;
                    Ok(Operation::CreateNamespace(
                        text(op, "namespace")?.to_owned(),
                    ))
                }
                Some("create_table") => {
                    let op = arguments(op, "create_table", &["table", "columns"])?;
                    let table = text(op, "table")?.to_owned();
                    ident(&table).map_err(|e| refuse(e.to_string()))?;
                    let columns = op["columns"]
                        .as_array()
                        .ok_or_else(|| refuse("columns must be a list"))?
                        .iter()
                        .map(column)
                        .collect::<Result<Vec<_>, _>>()?;
                    if columns.is_empty() {
                        return Err(refuse(format!("{table}: a table needs columns")));
                    }
                    Ok(Operation::CreateTable { table, columns })
                }
                Some(kind @ ("add_column" | "rename_column" | "drop_column" | "add_partition")) => {
                    Err(refuse(format!(
                        "{kind} is not yet in Rust: run it with the Python adapter"
                    )))
                }
                other => Err(refuse(format!(
                    "unknown iceberg operation {other:?}: create_namespace, create_table"
                ))),
            }
        })
        .collect()
}

fn column(raw: &Json) -> Result<(String, PrimitiveType, bool), IcebergError> {
    let c = raw
        .as_object()
        .ok_or_else(|| refuse("a column is an object with name and type"))?;
    let name = c
        .get("name")
        .and_then(Json::as_str)
        .ok_or_else(|| refuse("a column needs a name"))?;
    let ty = c
        .get("type")
        .and_then(Json::as_str)
        .ok_or_else(|| refuse(format!("column {name:?} needs a type")))?;
    let required = c.get("required").and_then(Json::as_bool).unwrap_or(false);
    if let Some(extra) = c
        .keys()
        .find(|k| !matches!(k.as_str(), "name" | "type" | "required"))
    {
        return Err(refuse(format!("column {name:?} takes no {extra}")));
    }
    Ok((name.to_owned(), iceberg_type(ty)?, required))
}

impl IcebergStore {
    /// Run the operations of one `iceberg` step, in order (all are checked
    /// first). Idempotent.
    ///
    /// # Errors
    ///
    /// As [`parse_operations`], else the catalog's error.
    pub async fn run_operations(&self, body: &Json) -> Result<(), BoxError> {
        for op in parse_operations(body)? {
            match op {
                Operation::CreateNamespace(namespace) => {
                    let namespace = NamespaceIdent::from_strs(namespace.split('.'))?;
                    self.create_namespace(&namespace).await?;
                }
                Operation::CreateTable { table, columns } => {
                    self.create_table(&table, &columns).await?;
                }
            }
        }
        Ok(())
    }

    async fn create_namespace(&self, namespace: &NamespaceIdent) -> Result<(), BoxError> {
        let catalog = self.catalog();
        // A concurrent step may win the race: what counts is that it exists.
        if !catalog.namespace_exists(namespace).await?
            && let Err(e) = catalog.create_namespace(namespace, HashMap::new()).await
            && !catalog.namespace_exists(namespace).await?
        {
            return Err(e.into());
        }
        Ok(())
    }

    async fn create_table(
        &self,
        table: &str,
        columns: &[(String, PrimitiveType, bool)],
    ) -> Result<(), BoxError> {
        let ident = ident(table)?;
        let catalog = self.catalog();
        if catalog.table_exists(&ident).await? {
            return Ok(());
        }
        let fields = columns.iter().zip(1..).map(|((name, ty, required), id)| {
            Arc::new(NestedField::new(
                id,
                name,
                Type::Primitive(ty.clone()),
                *required,
            ))
        });
        let schema = Schema::builder().with_fields(fields).build()?;
        let creation = TableCreation::builder()
            .name(ident.name().to_owned())
            .schema(schema)
            .build();
        if let Err(e) = catalog.create_table(ident.namespace(), creation).await
            && !catalog.table_exists(&ident).await?
        {
            return Err(e.into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn types_are_named_as_in_the_python_adapter() {
        assert_eq!(iceberg_type("long").unwrap(), PrimitiveType::Long);
        assert_eq!(
            iceberg_type("timestamptz").unwrap(),
            PrimitiveType::Timestamptz
        );
        assert_eq!(
            iceberg_type("decimal(10, 2)").unwrap(),
            PrimitiveType::Decimal {
                precision: 10,
                scale: 2
            }
        );
        assert!(iceberg_type("varchar").is_err());
        assert!(iceberg_type("decimal(10)").is_err());
    }

    #[test]
    fn a_step_is_checked_before_it_runs() {
        let ops = parse_operations(&json!([
            {"op": "create_namespace", "namespace": "ops"},
            {"op": "create_table", "table": "ops.req", "columns": [
                {"name": "id", "type": "string", "required": true},
                {"name": "ms", "type": "long"}
            ]}
        ]))
        .unwrap();
        assert_eq!(
            ops[1],
            Operation::CreateTable {
                table: "ops.req".into(),
                columns: vec![
                    ("id".into(), PrimitiveType::String, true),
                    ("ms".into(), PrimitiveType::Long, false)
                ]
            }
        );
        for (body, why) in [
            (json!({"op": "create_namespace"}), "a list"),
            (
                json!([{"op": "create_table", "table": "ops.req"}]),
                "needs columns",
            ),
            (
                json!([{"op": "create_namespace", "namespace": "a", "x": 1}]),
                "takes no x",
            ),
            (
                json!([{"op": "create_table", "table": "req", "columns": [{"name": "a", "type": "long"}]}]),
                "namespace",
            ),
            (
                json!([{"op": "create_table", "table": "ops.req", "columns": []}]),
                "needs columns",
            ),
            (
                json!([{"op": "create_table", "table": "ops.req", "columns": [{"name": "a", "type": "nope"}]}]),
                "unknown iceberg type",
            ),
            (
                json!([{"op": "add_column", "table": "ops.req", "name": "a", "type": "long"}]),
                "not yet in Rust",
            ),
            (json!([{"op": "drop"}]), "unknown iceberg operation"),
        ] {
            let err = parse_operations(&body).unwrap_err().to_string();
            assert!(err.contains(why), "{err:?} does not say {why:?}");
        }
    }
}
