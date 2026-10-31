//! Entity parts in drevo: the port of the Python reference's `DrevoStore`
//! parts (`rhizodat.core.parts`).
//!
//! Every version is one node labelled after the part, with the properties
//! `dat_id`, `dat_txn`, `dat_tx_time`, `dat_created`, `dat_deleted` and one per
//! field that has a value (`title` is stored as `dat_title`). A field is stored
//! natively (strings, numbers, booleans), as a UUID's string, or as the tagged
//! JSON text of [`encode_value`]. A reference field also links the version to an
//! anchor node (`DatEntity {dat_id}`) of every entity it names; an anchor no
//! version points to any more is removed with the last one. All of it is the
//! reference's layout, so either implementation reads the other's versions.
//!
//! A Unit of Work's writes are buffered in [`DrevoParts`] and run, in order,
//! inside the branch's transaction at stage, stamped with the meta-transaction's
//! time: they commit atomically with it. The store reads committed versions in
//! auto-commit mode ([`DrevoStore::read_part`] and friends), which keeps working
//! under a prepared fence.

use std::fmt::Write as _;

use rhizodat_core::rows::{RowValue, decode_value, encode_value};
use rhizodat_core::store::BoxError;
use uuid::Uuid;

use crate::{ConfigError, DrevoBranch, DrevoStore, Params, Row, Value, params};

/// The label of the node a reference edge points to: one per referenced `dat_id`.
pub const ANCHOR: &str = "DatEntity";

const SERVICE: [&str; 6] = [
    "dat_id",
    "dat_txn",
    "dat_tx_time",
    "dat_created",
    "dat_deleted",
    "dat_title",
];

const ORPHANS: &str = "MATCH (a:DatEntity) WHERE a.dat_id IN $ids AND NOT (a)<--() DELETE a";

/// How a field is stored (the reference's `_field_kind`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FieldKind {
    /// A string, number or boolean as itself.
    Native,
    /// A UUID (or reference) as its string.
    Uuid,
    /// Anything else as tagged JSON text.
    Json,
}

impl std::str::FromStr for FieldKind {
    type Err = ConfigError;

    fn from_str(s: &str) -> Result<Self, ConfigError> {
        match s {
            "native" => Ok(FieldKind::Native),
            "uuid" => Ok(FieldKind::Uuid),
            "json" => Ok(FieldKind::Json),
            other => Err(ConfigError(format!("not a part field kind: {other:?}"))),
        }
    }
}

fn check_name(kind: &str, name: &str) -> Result<(), ConfigError> {
    let mut chars = name.chars();
    let ok = name.len() <= 128
        && chars
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    if ok {
        Ok(())
    } else {
        Err(ConfigError(format!(
            "invalid {kind} {name:?}: letters, digits and _, not starting with a digit"
        )))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Field {
    name: String,
    property: String,
    kind: FieldKind,
    reference: bool,
}

/// One store's share of one entity type: its label and fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DrevoPartSpec {
    type_name: String,
    label: String,
    fields: Vec<Field>,
}

impl DrevoPartSpec {
    /// The part of `type_name` stored under `label`, with `fields`
    /// `(name, kind, is a reference)` in order.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] for a label or field that is not a plain name, a field
    /// named like a service property, or a field given twice.
    pub fn new(
        type_name: &str,
        label: &str,
        fields: Vec<(String, FieldKind, bool)>,
    ) -> Result<Self, ConfigError> {
        check_name("label", label)?;
        let mut out: Vec<Field> = Vec::with_capacity(fields.len());
        for (name, kind, reference) in fields {
            check_name("field", &name)?;
            if SERVICE.contains(&name.as_str()) {
                return Err(ConfigError(format!(
                    "{type_name}: {name} is a service property, not a field"
                )));
            }
            if out.iter().any(|f| f.name == name) {
                return Err(ConfigError(format!(
                    "{type_name}: field {name} is given twice"
                )));
            }
            let property = if name == "title" {
                "dat_title".to_owned()
            } else {
                name.clone()
            };
            out.push(Field {
                name,
                property,
                kind,
                reference,
            });
        }
        Ok(Self {
            type_name: type_name.to_owned(),
            label: label.to_owned(),
            fields: out,
        })
    }

    /// The label.
    #[must_use]
    pub fn label(&self) -> &str {
        &self.label
    }

    /// The fields, in order.
    pub fn fields(&self) -> impl Iterator<Item = &str> {
        self.fields.iter().map(|f| f.name.as_str())
    }

    fn select(&self, filter: &str, order: &str) -> String {
        let mut columns = String::from(
            "v.dat_id AS dat_id, v.dat_txn AS dat_txn, v.dat_tx_time AS dat_tx_time, \
             v.dat_created AS dat_created, v.dat_deleted AS dat_deleted",
        );
        for (i, f) in self.fields.iter().enumerate() {
            let _ = write!(columns, ", v.{} AS f{i}", f.property);
        }
        format!(
            "MATCH (v:{}) WHERE {filter} RETURN {columns} ORDER BY {order}",
            self.label
        )
    }

    fn version(&self, row: &Row) -> Result<PartVersion, BoxError> {
        let values = self
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| decode(f.kind, row.value(&format!("f{i}"))))
            .collect::<Result<_, _>>()?;
        Ok(PartVersion {
            dat_txn: row.str("dat_txn")?.parse()?,
            dat_tx_time: row.opt_int("dat_tx_time")?,
            created: row.opt_int("dat_created")?,
            deleted: matches!(row.value("dat_deleted"), Value::Bool(true)),
            values,
        })
    }

    /// `values` in field order, `NULL` for a field left out.
    fn ordered(&self, values: &[(String, RowValue)]) -> Result<Vec<RowValue>, ConfigError> {
        let mut unknown: Vec<&str> = values
            .iter()
            .map(|(n, _)| n.as_str())
            .filter(|n| !self.fields.iter().any(|f| f.name == *n))
            .collect();
        if !unknown.is_empty() {
            unknown.sort_unstable();
            return Err(ConfigError(format!(
                "{unknown:?} are not fields of {}",
                self.type_name
            )));
        }
        Ok(self
            .fields
            .iter()
            .map(|f| {
                values
                    .iter()
                    .rev()
                    .find(|(n, _)| *n == f.name)
                    .map_or(RowValue::Null, |(_, v)| v.clone())
            })
            .collect())
    }
}

/// A field's value as its stored property, `None` for `NULL` (not stored).
fn store(kind: FieldKind, value: &RowValue) -> Option<Value> {
    Some(match (kind, value) {
        (_, RowValue::Null) => return None,
        (FieldKind::Native, RowValue::Str(v)) => Value::Str(v.clone()),
        (FieldKind::Native, RowValue::Int(v)) => Value::Int(*v),
        (FieldKind::Native, RowValue::Float(v)) => Value::Float(*v),
        (FieldKind::Native, RowValue::Bool(v)) => Value::Bool(*v),
        (FieldKind::Uuid, RowValue::Uuid(v)) => Value::Str(v.hyphenated().to_string()),
        (_, other) => Value::Str(encode_value(other)),
    })
}

fn decode(kind: FieldKind, raw: &Value) -> Result<RowValue, BoxError> {
    Ok(match (kind, raw) {
        (_, Value::Null) => RowValue::Null,
        (FieldKind::Uuid, Value::Str(s)) => RowValue::Uuid(s.parse()?),
        (FieldKind::Json, Value::Str(s)) => decode_value(s)?,
        (_, Value::Str(s)) => RowValue::Str(s.clone()),
        (_, Value::Int(v)) => RowValue::Int(*v),
        (_, Value::Float(v)) => RowValue::Float(*v),
        (_, Value::Bool(v)) => RowValue::Bool(*v),
        (_, Value::List(_)) => return Err("a part property holds a list".into()),
    })
}

/// Every `dat_id` a reference field's value names, as strings.
fn targets(value: &RowValue, out: &mut Vec<String>) {
    match value {
        RowValue::Uuid(v) => out.push(v.hyphenated().to_string()),
        RowValue::List(items) => items.iter().for_each(|v| targets(v, out)),
        RowValue::Map(items) => items.iter().for_each(|(_, v)| targets(v, out)),
        _ => {}
    }
}

/// One stored version of one part.
#[derive(Clone, Debug, PartialEq)]
pub struct PartVersion {
    /// The meta-transaction that wrote it.
    pub dat_txn: Uuid,
    /// Its time (HLC); `None` while not staged.
    pub dat_tx_time: Option<i64>,
    /// The time of the entity's first version.
    pub created: Option<i64>,
    /// A soft delete.
    pub deleted: bool,
    /// The fields' values, in field order.
    pub values: Vec<RowValue>,
}

/// A buffered version, not yet staged.
#[derive(Clone, Debug, PartialEq)]
pub struct PendingVersion {
    /// The time of the entity's first version; `None`: this is the first.
    pub created: Option<i64>,
    /// A soft delete.
    pub deleted: bool,
    /// The fields' values, in field order.
    pub values: Vec<RowValue>,
}

#[derive(Clone, Debug)]
enum Op {
    Write {
        spec: DrevoPartSpec,
        dat_id: Uuid,
        version: PendingVersion,
    },
    Erase {
        spec: DrevoPartSpec,
        dat_id: Uuid,
    },
}

/// A Unit of Work's part writes for a drevo store, buffered client-side and
/// run in order inside the branch's transaction at stage.
#[derive(Clone, Debug, Default)]
pub struct DrevoParts {
    ops: Vec<Op>,
}

impl DrevoParts {
    /// No writes.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether nothing has been buffered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Add a version of `dat_id`'s part: `values` by field (missing ones are
    /// `NULL`), `created` the time of the first version (`None`: this one).
    ///
    /// # Errors
    ///
    /// [`ConfigError`] for a field the part does not have.
    pub fn write(
        &mut self,
        spec: &DrevoPartSpec,
        dat_id: Uuid,
        values: &[(String, RowValue)],
        created: Option<i64>,
        deleted: bool,
    ) -> Result<(), ConfigError> {
        let values = spec.ordered(values)?;
        self.ops.push(Op::Write {
            spec: spec.clone(),
            dat_id,
            version: PendingVersion {
                created,
                deleted,
                values,
            },
        });
        Ok(())
    }

    /// Remove every version of `dat_id`'s part (committed ones too).
    pub fn erase(&mut self, spec: &DrevoPartSpec, dat_id: Uuid) {
        self.ops.push(Op::Erase {
            spec: spec.clone(),
            dat_id,
        });
    }

    /// This Unit of Work's own latest version of `dat_id`'s part: `None` when it
    /// did not touch it, `Some(None)` when it erased it last.
    #[must_use]
    pub fn latest(&self, label: &str, dat_id: Uuid) -> Option<Option<&PendingVersion>> {
        self.ops.iter().rev().find_map(|op| match op {
            Op::Write {
                spec,
                dat_id: id,
                version,
            } if spec.label == label && *id == dat_id => Some(Some(version)),
            Op::Erase { spec, dat_id: id } if spec.label == label && *id == dat_id => Some(None),
            _ => None,
        })
    }
}

impl DrevoBranch {
    /// Replace the branch's buffered part writes (run at stage).
    pub fn load_parts(&mut self, parts: DrevoParts) {
        self.parts = parts;
    }

    /// Run the buffered part writes, stamped with the meta-transaction.
    pub(crate) async fn apply_parts(
        &mut self,
        dat_txn: Uuid,
        dat_tx_time: i64,
    ) -> Result<(), BoxError> {
        let txn = dat_txn.to_string();
        for op in std::mem::take(&mut self.parts).ops {
            match op {
                Op::Write {
                    spec,
                    dat_id,
                    version,
                } => {
                    let id = dat_id.to_string();
                    let key = || params![("id", id.clone()), ("txn", txn.clone())];
                    let matched =
                        format!("MATCH (v:{} {{dat_id: $id, dat_txn: $txn}})", spec.label);
                    self.remove(&matched, key()).await?;
                    let mut props = vec![
                        "dat_id: $id".to_owned(),
                        "dat_txn: $txn".to_owned(),
                        "dat_tx_time: $ts".to_owned(),
                        "dat_created: $created".to_owned(),
                        "dat_deleted: $deleted".to_owned(),
                    ];
                    let mut params = key();
                    params.extend(params![
                        ("ts", dat_tx_time),
                        ("created", version.created.unwrap_or(dat_tx_time)),
                        ("deleted", version.deleted),
                    ]);
                    for (i, (field, value)) in spec.fields.iter().zip(&version.values).enumerate() {
                        if let Some(stored) = store(field.kind, value) {
                            props.push(format!("{}: $p{i}", field.property));
                            params.push((format!("p{i}"), stored));
                        }
                    }
                    let create = format!("CREATE (v:{} {{{}}})", spec.label, props.join(", "));
                    self.run(&create, params).await?;
                    let mut references: Vec<(&Field, &RowValue)> = spec
                        .fields
                        .iter()
                        .zip(&version.values)
                        .filter(|(f, _)| f.reference)
                        .collect();
                    references.sort_by(|a, b| a.0.name.cmp(&b.0.name));
                    for (field, value) in references {
                        let mut found = Vec::new();
                        targets(value, &mut found);
                        if found.is_empty() {
                            continue;
                        }
                        let link = format!(
                            "{matched} UNWIND $targets AS t MERGE (a:{ANCHOR} {{dat_id: t}}) \
                             CREATE (v)-[:{}]->(a)",
                            field.name
                        );
                        let mut params = key();
                        params.push(("targets".to_owned(), Value::from(found)));
                        self.run(&link, params).await?;
                    }
                }
                Op::Erase { spec, dat_id } => {
                    let matched = format!("MATCH (v:{} {{dat_id: $id}})", spec.label);
                    self.remove(&matched, params![("id", dat_id.to_string())])
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// Delete the matched versions, then the anchors only they pointed to.
    async fn remove(&mut self, matched: &str, params: Params) -> Result<(), BoxError> {
        let rows = self
            .run(
                &format!("{matched}-->(a:{ANCHOR}) RETURN DISTINCT a.dat_id AS a"),
                params.clone(),
            )
            .await?;
        let anchors: Vec<String> = rows
            .iter()
            .map(|r| r.str("a").map(str::to_owned))
            .collect::<Result<_, _>>()?;
        self.run(&format!("{matched} DETACH DELETE v"), params)
            .await?;
        if !anchors.is_empty() {
            self.run(ORPHANS, params![("ids", anchors)]).await?;
        }
        Ok(())
    }
}

impl DrevoStore {
    /// Create the `dat_id` indexes of the part's versions and of anchors, if
    /// missing (in auto-commit mode: index calls do not run in a transaction).
    ///
    /// # Errors
    ///
    /// The engine's error.
    pub async fn install_part(&self, spec: &DrevoPartSpec) -> Result<(), BoxError> {
        for label in [spec.label.as_str(), ANCHOR] {
            let name = format!("dat_{}", label.to_lowercase());
            self.backend
                .read(
                    &format!("CREATE INDEX {name} IF NOT EXISTS FOR (v:{label}) ON (v.dat_id)"),
                    params![],
                )
                .await?;
        }
        Ok(())
    }

    /// The latest committed version with `dat_tx_time <= known_at` (any, if `None`).
    ///
    /// # Errors
    ///
    /// The engine's error, or a property the part's kinds cannot decode.
    pub async fn read_part(
        &self,
        spec: &DrevoPartSpec,
        dat_id: Uuid,
        known_at: Option<i64>,
    ) -> Result<Option<PartVersion>, BoxError> {
        let mut filter = "v.dat_id = $id AND v.dat_tx_time IS NOT NULL".to_owned();
        let mut params = params![("id", dat_id.to_string())];
        if let Some(k) = known_at {
            filter.push_str(" AND v.dat_tx_time <= $k");
            params.push(("k".to_owned(), Value::Int(k)));
        }
        let query = spec.select(&filter, "v.dat_tx_time DESC LIMIT 1");
        let rows = self.backend.read(&query, params).await?;
        rows.first().map(|r| spec.version(r)).transpose()
    }

    /// Every committed version of `dat_id`'s part, oldest first.
    ///
    /// # Errors
    ///
    /// As [`read_part`](Self::read_part).
    pub async fn part_history(
        &self,
        spec: &DrevoPartSpec,
        dat_id: Uuid,
    ) -> Result<Vec<PartVersion>, BoxError> {
        let query = spec.select(
            "v.dat_id = $id AND v.dat_tx_time IS NOT NULL",
            "v.dat_tx_time",
        );
        let rows = self
            .backend
            .read(&query, params![("id", dat_id.to_string())])
            .await?;
        rows.iter().map(|r| spec.version(r)).collect()
    }

    /// Every committed version of every entity with `dat_tx_time <= known_at`
    /// (all, if `None`), oldest first.
    ///
    /// # Errors
    ///
    /// As [`read_part`](Self::read_part).
    pub async fn part_versions(
        &self,
        spec: &DrevoPartSpec,
        known_at: Option<i64>,
    ) -> Result<Vec<(Uuid, PartVersion)>, BoxError> {
        let mut filter = "v.dat_tx_time IS NOT NULL".to_owned();
        let mut params = params![];
        if let Some(k) = known_at {
            filter.push_str(" AND v.dat_tx_time <= $k");
            params.push(("k".to_owned(), Value::Int(k)));
        }
        let query = spec.select(&filter, "v.dat_tx_time, v.dat_id");
        let rows = self.backend.read(&query, params).await?;
        rows.iter()
            .map(|r| Ok((r.str("dat_id")?.parse()?, spec.version(r)?)))
            .collect()
    }

    /// Entities whose latest version is a soft delete older than `deleted_before`.
    ///
    /// # Errors
    ///
    /// The engine's error.
    pub async fn expired_parts(
        &self,
        spec: &DrevoPartSpec,
        deleted_before: i64,
        limit: i64,
    ) -> Result<Vec<Uuid>, BoxError> {
        if limit <= 0 {
            return Ok(Vec::new());
        }
        let label = &spec.label;
        let rows = self
            .backend
            .read(
                &format!(
                    "MATCH (v:{label}) WHERE v.dat_tx_time IS NOT NULL \
                     WITH v.dat_id AS d, max(v.dat_tx_time) AS t \
                     MATCH (w:{label}) WHERE w.dat_id = d AND w.dat_tx_time = t \
                     AND w.dat_deleted AND t < $b RETURN d ORDER BY d LIMIT {limit}"
                ),
                params![("b", deleted_before)],
            )
            .await?;
        rows.iter().map(|r| Ok(r.str("d")?.parse()?)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> DrevoPartSpec {
        DrevoPartSpec::new(
            "t.T",
            "TT",
            vec![
                ("title".into(), FieldKind::Native, false),
                ("owner".into(), FieldKind::Uuid, true),
                ("tags".into(), FieldKind::Json, false),
            ],
        )
        .unwrap()
    }

    #[test]
    fn a_spec_takes_plain_names_and_no_service_properties() {
        assert!(DrevoPartSpec::new("t.T", "bad label", vec![]).is_err());
        assert!(DrevoPartSpec::new("t.T", "1x", vec![]).is_err());
        let err = DrevoPartSpec::new(
            "t.T",
            "L",
            vec![("dat_title".into(), FieldKind::Native, false)],
        )
        .unwrap_err();
        assert!(err.to_string().contains("service property"), "{err}");
        let twice = vec![
            ("a".into(), FieldKind::Native, false),
            ("a".into(), FieldKind::Json, false),
        ];
        assert!(DrevoPartSpec::new("t.T", "L", twice).is_err());
        assert_eq!("json".parse::<FieldKind>().unwrap(), FieldKind::Json);
        assert!("map".parse::<FieldKind>().is_err());
        assert_eq!(spec().fields[0].property, "dat_title");
    }

    #[test]
    fn values_are_stored_like_the_reference() {
        let id = Uuid::from_u128(7);
        assert_eq!(store(FieldKind::Native, &RowValue::Null), None);
        assert_eq!(
            store(FieldKind::Native, &RowValue::Int(3)),
            Some(Value::Int(3))
        );
        assert_eq!(
            store(FieldKind::Uuid, &RowValue::Uuid(id)),
            Some(Value::Str(id.hyphenated().to_string()))
        );
        // A value without a native form falls back to tagged JSON, as in Python.
        assert_eq!(
            store(FieldKind::Native, &RowValue::Decimal("1.50".into())),
            Some(Value::Str(r#"{"$":"decimal","v":"1.50"}"#.into()))
        );
        for (kind, value) in [
            (FieldKind::Native, RowValue::Bool(true)),
            (FieldKind::Uuid, RowValue::Uuid(id)),
            (
                FieldKind::Json,
                RowValue::List(vec![RowValue::Uuid(id), RowValue::Null]),
            ),
        ] {
            let stored = store(kind, &value).unwrap();
            assert_eq!(decode(kind, &stored).unwrap(), value);
        }
        assert!(decode(FieldKind::Uuid, &Value::Str("x".into())).is_err());
    }

    #[test]
    fn references_name_every_target() {
        let (a, b) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let mut found = Vec::new();
        targets(
            &RowValue::Map(vec![
                ("x".into(), RowValue::List(vec![RowValue::Uuid(a)])),
                ("y".into(), RowValue::Uuid(b)),
                ("z".into(), RowValue::Str("not one".into())),
            ]),
            &mut found,
        );
        assert_eq!(found, vec![a.to_string(), b.to_string()]);
    }

    #[test]
    fn own_writes_read_back_last_first() {
        let id = Uuid::from_u128(1);
        let mut parts = DrevoParts::new();
        assert!(parts.is_empty());
        let err = parts
            .write(
                &spec(),
                id,
                &[("nope".into(), RowValue::Int(1))],
                None,
                false,
            )
            .unwrap_err();
        assert!(err.to_string().contains("are not fields of t.T"), "{err}");
        parts
            .write(
                &spec(),
                id,
                &[("tags".into(), RowValue::Int(1))],
                Some(4),
                false,
            )
            .unwrap();
        let own = parts.latest("TT", id).unwrap().unwrap();
        assert_eq!(
            own.values,
            vec![RowValue::Null, RowValue::Null, RowValue::Int(1)]
        );
        parts.erase(&spec(), id);
        assert_eq!(parts.latest("TT", id), Some(None));
        assert_eq!(parts.latest("Other", id), None);
    }
}
