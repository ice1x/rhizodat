//! Entity parts in Iceberg: the port of the Python reference's `IcebergStore`
//! parts (`rhizodat.core.parts`).
//!
//! One table per part in the store's parts namespace, one row per version:
//! `dat_id` (text), `dat_created`, `dat_deleted`, one optional column per field
//! (string, boolean, long, double, binary, timestamptz, date; ids as text;
//! tagged JSON text for the rest), then `dat_txn` (text) and `dat_tx_time`. The
//! layout is the reference's, so either implementation reads the other's
//! versions.
//!
//! A Unit of Work's versions are buffered in [`IcebergParts`] (one per entity:
//! the last write wins) and staged as one more data file of the branch; an
//! erase travels in the journal payload and is committed with the files, in one
//! snapshot (copy-on-write, see the `erase` module). The store reads committed
//! versions ([`IcebergStore::read_part`] and friends).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::str::FromStr;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use iceberg::TableCreation;
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::{Datum, NestedField, PrimitiveType, Schema, Type};
use rhizodat_core::rows::{RowValue, decode_value, encode_value};
use rhizodat_core::staged::StagedTable;
use rhizodat_core::store::BoxError;
use uuid::Uuid;

use crate::{IcebergBranch, IcebergError, IcebergStore, ident, refuse};

const SERVICE: [&str; 5] = [
    "dat_id",
    "dat_created",
    "dat_deleted",
    "dat_txn",
    "dat_tx_time",
];

/// How a field is stored (the reference's `_field_kind`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FieldKind {
    /// `string`
    String,
    /// `boolean`
    Boolean,
    /// `long`
    Long,
    /// `double`
    Double,
    /// `binary`
    Binary,
    /// `timestamptz`
    Timestamptz,
    /// `date`
    Date,
    /// A UUID or reference, as text
    Id,
    /// Anything else, as tagged JSON text
    Json,
}

impl FieldKind {
    fn iceberg(self) -> PrimitiveType {
        match self {
            FieldKind::Boolean => PrimitiveType::Boolean,
            FieldKind::Long => PrimitiveType::Long,
            FieldKind::Double => PrimitiveType::Double,
            FieldKind::Binary => PrimitiveType::Binary,
            FieldKind::Timestamptz => PrimitiveType::Timestamptz,
            FieldKind::Date => PrimitiveType::Date,
            FieldKind::String | FieldKind::Id | FieldKind::Json => PrimitiveType::String,
        }
    }

    /// The Arrow type a version's column is built with (cast to the table's
    /// type when staged: dates and times travel as ISO 8601 text).
    fn arrow(self) -> DataType {
        match self {
            FieldKind::Boolean => DataType::Boolean,
            FieldKind::Long => DataType::Int64,
            FieldKind::Double => DataType::Float64,
            FieldKind::Binary => DataType::Binary,
            _ => DataType::Utf8,
        }
    }

    fn column(self, name: &str, values: &[&RowValue]) -> Result<ArrayRef, IcebergError> {
        let wrong =
            |v: &RowValue| IcebergError(format!("field {name} ({self:?}) cannot hold {v:?}"));
        Ok(match self {
            FieldKind::Boolean => Arc::new(
                values
                    .iter()
                    .map(|v| match v {
                        RowValue::Null => Ok(None),
                        RowValue::Bool(b) => Ok(Some(*b)),
                        other => Err(wrong(other)),
                    })
                    .collect::<Result<BooleanArray, _>>()?,
            ),
            FieldKind::Long => Arc::new(
                values
                    .iter()
                    .map(|v| match v {
                        RowValue::Null => Ok(None),
                        RowValue::Int(i) => Ok(Some(*i)),
                        other => Err(wrong(other)),
                    })
                    .collect::<Result<Int64Array, _>>()?,
            ),
            FieldKind::Double => Arc::new(
                values
                    .iter()
                    .map(|v| match v {
                        RowValue::Null => Ok(None),
                        RowValue::Float(f) => Ok(Some(*f)),
                        #[allow(clippy::cast_precision_loss)]
                        RowValue::Int(i) => Ok(Some(*i as f64)),
                        other => Err(wrong(other)),
                    })
                    .collect::<Result<Float64Array, _>>()?,
            ),
            FieldKind::Binary => {
                let bytes: Vec<Option<&[u8]>> = values
                    .iter()
                    .map(|v| match v {
                        RowValue::Null => Ok(None),
                        RowValue::Bytes(b) => Ok(Some(b.as_slice())),
                        other => Err(wrong(other)),
                    })
                    .collect::<Result<_, _>>()?;
                Arc::new(BinaryArray::from(bytes))
            }
            text => Arc::new(
                values
                    .iter()
                    .map(|v| text_of(text, v).map_err(|()| wrong(v)))
                    .collect::<Result<StringArray, _>>()?,
            ),
        })
    }

    fn decode(self, column: &ArrayRef, i: usize) -> Result<RowValue, BoxError> {
        if column.is_null(i) {
            return Ok(RowValue::Null);
        }
        let as_text = |c: &ArrayRef| -> Result<String, BoxError> {
            let text = arrow_cast::cast(c, &DataType::Utf8)?;
            let text = text
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("not text")?;
            Ok(text.value(i).to_owned())
        };
        Ok(match self {
            FieldKind::Boolean => {
                let c = arrow_cast::cast(column, &DataType::Boolean)?;
                RowValue::Bool(
                    c.as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or("not a boolean")?
                        .value(i),
                )
            }
            FieldKind::Long => {
                let c = arrow_cast::cast(column, &DataType::Int64)?;
                RowValue::Int(
                    c.as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or("not a long")?
                        .value(i),
                )
            }
            FieldKind::Double => {
                let c = arrow_cast::cast(column, &DataType::Float64)?;
                RowValue::Float(
                    c.as_any()
                        .downcast_ref::<Float64Array>()
                        .ok_or("not a double")?
                        .value(i),
                )
            }
            FieldKind::Binary => {
                let c = arrow_cast::cast(column, &DataType::Binary)?;
                RowValue::Bytes(
                    c.as_any()
                        .downcast_ref::<BinaryArray>()
                        .ok_or("not binary")?
                        .value(i)
                        .to_vec(),
                )
            }
            FieldKind::String => RowValue::Str(as_text(column)?),
            FieldKind::Id => RowValue::Uuid(as_text(column)?.parse()?),
            FieldKind::Json => decode_value(&as_text(column)?)?,
            FieldKind::Date => RowValue::Date(as_text(column)?),
            FieldKind::Timestamptz => {
                let t = as_text(column)?;
                RowValue::DateTime(match t.strip_suffix('Z') {
                    Some(base) => format!("{base}+00:00"),
                    None => t,
                })
            }
        })
    }
}

/// A value of a text-backed kind as its text; `Err` when it does not fit.
fn text_of(kind: FieldKind, value: &RowValue) -> Result<Option<String>, ()> {
    Ok(Some(match (kind, value) {
        (_, RowValue::Null) => return Ok(None),
        (FieldKind::Json, v) => encode_value(v),
        (FieldKind::String, RowValue::Str(s))
        | (FieldKind::Date, RowValue::Date(s) | RowValue::Str(s))
        | (FieldKind::Timestamptz, RowValue::DateTime(s) | RowValue::Str(s)) => s.clone(),
        (FieldKind::Id, RowValue::Uuid(id)) => id.hyphenated().to_string(),
        _ => return Err(()),
    }))
}

impl FromStr for FieldKind {
    type Err = IcebergError;

    fn from_str(s: &str) -> Result<Self, IcebergError> {
        Ok(match s {
            "string" => FieldKind::String,
            "boolean" => FieldKind::Boolean,
            "long" => FieldKind::Long,
            "double" => FieldKind::Double,
            "binary" => FieldKind::Binary,
            "timestamptz" => FieldKind::Timestamptz,
            "date" => FieldKind::Date,
            "id" => FieldKind::Id,
            "json" => FieldKind::Json,
            other => return Err(IcebergError(format!("not a part field kind: {other:?}"))),
        })
    }
}

/// One store's share of one entity type: its table and fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergPartSpec {
    type_name: String,
    table: String,
    fields: Vec<(String, FieldKind)>,
}

impl IcebergPartSpec {
    /// The part of `type_name` stored in `table` (`namespace.table`), with
    /// `fields` (name, kind) in order.
    ///
    /// # Errors
    ///
    /// [`IcebergError`] for a table without a namespace, a field named like a
    /// service column, or a field given twice.
    pub fn new(
        type_name: &str,
        table: &str,
        fields: Vec<(String, FieldKind)>,
    ) -> Result<Self, IcebergError> {
        ident(table).map_err(|e| IcebergError(e.to_string()))?;
        for (i, (name, _)) in fields.iter().enumerate() {
            if name.is_empty() || SERVICE.contains(&name.as_str()) {
                return Err(IcebergError(format!(
                    "{type_name}: {name:?} is a service column, not a field"
                )));
            }
            if fields[..i].iter().any(|(n, _)| n == name) {
                return Err(IcebergError(format!(
                    "{type_name}: field {name} is given twice"
                )));
            }
        }
        Ok(Self {
            type_name: type_name.to_owned(),
            table: table.to_owned(),
            fields,
        })
    }

    /// The table (`namespace.table`).
    #[must_use]
    pub fn table(&self) -> &str {
        &self.table
    }

    /// The fields, in order.
    pub fn fields(&self) -> impl Iterator<Item = &str> {
        self.fields.iter().map(|(n, _)| n.as_str())
    }

    /// The reference's schema: service columns around the optional fields.
    fn schema(&self) -> Result<Schema, BoxError> {
        let primitive = |t: PrimitiveType| Arc::new(Type::Primitive(t));
        let mut columns = vec![
            NestedField::required(1, "dat_id", Type::Primitive(PrimitiveType::String)),
            NestedField::required(2, "dat_created", Type::Primitive(PrimitiveType::Long)),
            NestedField::required(3, "dat_deleted", Type::Primitive(PrimitiveType::Boolean)),
        ];
        let mut id = 4;
        for (name, kind) in &self.fields {
            columns.push(NestedField::new(
                id,
                name,
                (*primitive(kind.iceberg())).clone(),
                false,
            ));
            id += 1;
        }
        columns.push(NestedField::required(
            id,
            "dat_txn",
            Type::Primitive(PrimitiveType::String),
        ));
        columns.push(NestedField::required(
            id + 1,
            "dat_tx_time",
            Type::Primitive(PrimitiveType::Long),
        ));
        Ok(Schema::builder()
            .with_fields(columns.into_iter().map(Arc::new))
            .build()?)
    }

    /// `values` in field order, `NULL` for a field left out.
    fn ordered(&self, values: &[(String, RowValue)]) -> Result<Vec<RowValue>, IcebergError> {
        let mut unknown: Vec<&str> = values
            .iter()
            .map(|(n, _)| n.as_str())
            .filter(|n| !self.fields.iter().any(|(f, _)| f == n))
            .collect();
        if !unknown.is_empty() {
            unknown.sort_unstable();
            return Err(IcebergError(format!(
                "{}: {unknown:?} are not fields of this part",
                self.type_name
            )));
        }
        Ok(self
            .fields
            .iter()
            .map(|(name, _)| {
                values
                    .iter()
                    .rev()
                    .find(|(n, _)| n == name)
                    .map_or(RowValue::Null, |(_, v)| v.clone())
            })
            .collect())
    }

    fn version(&self, batch: &RecordBatch, i: usize) -> Result<PartVersion, BoxError> {
        let column = |name: &str| {
            batch
                .column_by_name(name)
                .ok_or_else(|| refuse(format!("{}: no column {name}", self.table)))
        };
        let values = self
            .fields
            .iter()
            .map(|(name, kind)| kind.decode(column(name)?, i))
            .collect::<Result<_, _>>()?;
        let long = |name: &str| -> Result<Option<i64>, BoxError> {
            match FieldKind::Long.decode(column(name)?, i)? {
                RowValue::Int(v) => Ok(Some(v)),
                _ => Ok(None),
            }
        };
        let text = |name: &str| -> Result<String, BoxError> {
            match FieldKind::String.decode(column(name)?, i)? {
                RowValue::Str(s) => Ok(s),
                other => Err(refuse(format!("{name} is {other:?}"))),
            }
        };
        Ok(PartVersion {
            dat_txn: text("dat_txn")?.parse()?,
            dat_tx_time: long("dat_tx_time")?,
            created: long("dat_created")?,
            deleted: matches!(
                FieldKind::Boolean.decode(column("dat_deleted")?, i)?,
                RowValue::Bool(true)
            ),
            values,
        })
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
struct PartWrites {
    spec: IcebergPartSpec,
    versions: BTreeMap<Uuid, PendingVersion>,
    erased: BTreeSet<Uuid>,
}

/// A Unit of Work's part writes for an Iceberg store (one version per entity:
/// the last write wins), staged with the branch's own appends.
#[derive(Clone, Debug, Default)]
pub struct IcebergParts {
    parts: Vec<PartWrites>,
}

impl IcebergParts {
    /// No writes.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether nothing has been buffered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.parts.is_empty()
    }

    fn of(&mut self, spec: &IcebergPartSpec) -> &mut PartWrites {
        let i = self
            .parts
            .iter()
            .position(|p| p.spec.table == spec.table)
            .unwrap_or_else(|| {
                self.parts.push(PartWrites {
                    spec: spec.clone(),
                    versions: BTreeMap::new(),
                    erased: BTreeSet::new(),
                });
                self.parts.len() - 1
            });
        &mut self.parts[i]
    }

    /// Set the version of `dat_id`'s part this Unit of Work writes: `values` by
    /// field (missing ones are `NULL`), `created` the time of the first version
    /// (`None`: this one).
    ///
    /// # Errors
    ///
    /// [`IcebergError`] for a field the part does not have, or a value its
    /// kind cannot hold.
    pub fn write(
        &mut self,
        spec: &IcebergPartSpec,
        dat_id: Uuid,
        values: &[(String, RowValue)],
        created: Option<i64>,
        deleted: bool,
    ) -> Result<(), IcebergError> {
        let values = spec.ordered(values)?;
        for ((name, kind), value) in spec.fields.iter().zip(&values) {
            kind.column(name, &[value])?; // refuse now what staging would
        }
        self.of(spec).versions.insert(
            dat_id,
            PendingVersion {
                created,
                deleted,
                values,
            },
        );
        Ok(())
    }

    /// Erase every earlier version of `dat_id`'s part (and drop this Unit of
    /// Work's own).
    pub fn erase(&mut self, spec: &IcebergPartSpec, dat_id: Uuid) {
        let part = self.of(spec);
        part.versions.remove(&dat_id);
        part.erased.insert(dat_id);
    }

    /// This Unit of Work's own version of `dat_id`'s part: `None` when it did
    /// not touch it, `Some(None)` when it erased it (and wrote nothing since).
    #[must_use]
    pub fn latest(&self, table: &str, dat_id: Uuid) -> Option<Option<&PendingVersion>> {
        let part = self.parts.iter().find(|p| p.spec.table == table)?;
        match part.versions.get(&dat_id) {
            Some(version) => Some(Some(version)),
            None if part.erased.contains(&dat_id) => Some(None),
            None => None,
        }
    }

    /// Each part's versions as one record batch (without `dat_txn` /
    /// `dat_tx_time`, which staging adds), by table.
    fn batches(&self, dat_tx_time: i64) -> Result<Vec<(String, RecordBatch)>, BoxError> {
        let mut out = Vec::new();
        for part in self.parts.iter().filter(|p| !p.versions.is_empty()) {
            let spec = &part.spec;
            let mut fields = vec![
                Field::new("dat_id", DataType::Utf8, false),
                Field::new("dat_created", DataType::Int64, false),
                Field::new("dat_deleted", DataType::Boolean, false),
            ];
            let mut columns: Vec<ArrayRef> = vec![
                Arc::new(StringArray::from_iter_values(
                    part.versions.keys().map(|id| id.hyphenated().to_string()),
                )),
                Arc::new(Int64Array::from_iter_values(
                    part.versions
                        .values()
                        .map(|v| v.created.unwrap_or(dat_tx_time)),
                )),
                Arc::new(BooleanArray::from_iter(
                    part.versions.values().map(|v| Some(v.deleted)),
                )),
            ];
            for (i, (name, kind)) in spec.fields.iter().enumerate() {
                let values: Vec<&RowValue> = part.versions.values().map(|v| &v.values[i]).collect();
                fields.push(Field::new(name, kind.arrow(), true));
                columns.push(kind.column(name, &values)?);
            }
            let batch = RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns)?;
            out.push((spec.table.clone(), batch));
        }
        Ok(out)
    }

    /// The ids each part table erases, sorted.
    fn erased(&self) -> Vec<(String, Vec<String>)> {
        self.parts
            .iter()
            .filter(|p| !p.erased.is_empty())
            .map(|p| {
                (
                    p.spec.table.clone(),
                    p.erased
                        .iter()
                        .map(|id| id.hyphenated().to_string())
                        .collect(),
                )
            })
            .collect()
    }
}

impl IcebergBranch {
    /// Replace the branch's buffered part writes (staged with its appends).
    pub fn load_parts(&mut self, parts: IcebergParts) {
        self.parts = parts;
    }

    /// The part versions to append, by table, before the branch's own batches.
    pub(crate) fn part_batches(
        &self,
        dat_tx_time: i64,
    ) -> Result<Vec<(String, RecordBatch)>, BoxError> {
        self.parts.batches(dat_tx_time)
    }

    /// Add the parts' erases to the staged tables (an erase-only table gets
    /// an entry of its own).
    pub(crate) fn with_erases(&self, tables: &mut Vec<StagedTable>) {
        for (table, ids) in self.parts.erased() {
            match tables.iter_mut().find(|t| t.table == table) {
                Some(staged) => staged.erase = ids,
                None => tables.push(StagedTable {
                    table,
                    files: Vec::new(),
                    data_files: None,
                    erase: ids,
                }),
            }
        }
    }
}

impl IcebergStore {
    /// Create the part's namespace and table if missing. Idempotent.
    ///
    /// # Errors
    ///
    /// The catalog's error.
    pub async fn install_part(&self, spec: &IcebergPartSpec) -> Result<(), BoxError> {
        let ident = ident(&spec.table)?;
        let catalog = &self.lake.catalog;
        let namespace = ident.namespace().clone();
        // A concurrent install may win either race: what counts is that it exists.
        if !catalog.namespace_exists(&namespace).await?
            && let Err(e) = catalog.create_namespace(&namespace, HashMap::new()).await
            && !catalog.namespace_exists(&namespace).await?
        {
            return Err(e.into());
        }
        if !catalog.table_exists(&ident).await? {
            let creation = TableCreation::builder()
                .name(ident.name().to_owned())
                .schema(spec.schema()?)
                .build();
            if let Err(e) = catalog.create_table(&namespace, creation).await
                && !catalog.table_exists(&ident).await?
            {
                return Err(e.into());
            }
        }
        Ok(())
    }

    async fn versions(
        &self,
        spec: &IcebergPartSpec,
        filter: Option<Predicate>,
    ) -> Result<Vec<(Uuid, PartVersion)>, BoxError> {
        let table = self.lake.load(&spec.table).await?;
        if table.metadata().current_snapshot().is_none() {
            return Ok(Vec::new());
        }
        let mut scan = table.scan().select_all();
        if let Some(filter) = filter {
            scan = scan.with_filter(filter);
        }
        let batches: Vec<RecordBatch> = scan.build()?.to_arrow().await?.try_collect().await?;
        let mut out = Vec::new();
        for batch in &batches {
            let ids = batch.column_by_name("dat_id").ok_or("no dat_id")?;
            for i in 0..batch.num_rows() {
                let id = match FieldKind::String.decode(ids, i)? {
                    RowValue::Str(s) => s.parse()?,
                    other => return Err(refuse(format!("dat_id is {other:?}"))),
                };
                out.push((id, spec.version(batch, i)?));
            }
        }
        out.sort_by_key(|(id, v)| (v.dat_tx_time, *id));
        Ok(out)
    }

    fn of_entity(dat_id: Uuid, known_at: Option<i64>) -> Predicate {
        let by_id =
            Reference::new("dat_id").equal_to(Datum::string(dat_id.hyphenated().to_string()));
        match known_at {
            Some(k) => {
                by_id.and(Reference::new("dat_tx_time").less_than_or_equal_to(Datum::long(k)))
            }
            None => by_id,
        }
    }

    /// The latest committed version with `dat_tx_time <= known_at` (any, if `None`).
    ///
    /// # Errors
    ///
    /// The catalog's or the scan's error, or a value the part cannot decode.
    pub async fn read_part(
        &self,
        spec: &IcebergPartSpec,
        dat_id: Uuid,
        known_at: Option<i64>,
    ) -> Result<Option<PartVersion>, BoxError> {
        let versions = self
            .versions(spec, Some(Self::of_entity(dat_id, known_at)))
            .await?;
        Ok(versions.into_iter().last().map(|(_, v)| v))
    }

    /// Every committed version of `dat_id`'s part, oldest first.
    ///
    /// # Errors
    ///
    /// As [`read_part`](Self::read_part).
    pub async fn part_history(
        &self,
        spec: &IcebergPartSpec,
        dat_id: Uuid,
    ) -> Result<Vec<PartVersion>, BoxError> {
        let versions = self
            .versions(spec, Some(Self::of_entity(dat_id, None)))
            .await?;
        Ok(versions.into_iter().map(|(_, v)| v).collect())
    }

    /// Every committed version of every entity with `dat_tx_time <= known_at`
    /// (all, if `None`), oldest first.
    ///
    /// # Errors
    ///
    /// As [`read_part`](Self::read_part).
    pub async fn part_versions(
        &self,
        spec: &IcebergPartSpec,
        known_at: Option<i64>,
    ) -> Result<Vec<(Uuid, PartVersion)>, BoxError> {
        let filter =
            known_at.map(|k| Reference::new("dat_tx_time").less_than_or_equal_to(Datum::long(k)));
        self.versions(spec, filter).await
    }

    /// Entities whose latest version is a soft delete older than `deleted_before`.
    ///
    /// # Errors
    ///
    /// As [`read_part`](Self::read_part).
    pub async fn expired_parts(
        &self,
        spec: &IcebergPartSpec,
        deleted_before: i64,
        limit: i64,
    ) -> Result<Vec<Uuid>, BoxError> {
        let mut latest: BTreeMap<Uuid, (Option<i64>, bool)> = BTreeMap::new();
        for (id, v) in self.versions(spec, None).await? {
            latest.insert(id, (v.dat_tx_time, v.deleted)); // oldest first: the last wins
        }
        Ok(latest
            .into_iter()
            .filter(|(_, (ts, deleted))| *deleted && ts.is_some_and(|t| t < deleted_before))
            .map(|(id, _)| id)
            .take(usize::try_from(limit.max(0)).unwrap_or(usize::MAX))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> IcebergPartSpec {
        IcebergPartSpec::new(
            "t.T",
            "dat.t_t",
            vec![
                ("title".into(), FieldKind::String),
                ("owner".into(), FieldKind::Id),
                ("tags".into(), FieldKind::Json),
            ],
        )
        .unwrap()
    }

    #[test]
    fn a_spec_takes_a_namespaced_table_and_no_service_columns() {
        assert!(IcebergPartSpec::new("t.T", "no_namespace", vec![]).is_err());
        let err = IcebergPartSpec::new("t.T", "dat.t", vec![("dat_txn".into(), FieldKind::Id)])
            .unwrap_err();
        assert!(err.to_string().contains("service column"), "{err}");
        let twice = vec![
            ("a".into(), FieldKind::Long),
            ("a".into(), FieldKind::Double),
        ];
        assert!(IcebergPartSpec::new("t.T", "dat.t", twice).is_err());
        for name in [
            "string",
            "boolean",
            "long",
            "double",
            "binary",
            "timestamptz",
            "date",
            "id",
            "json",
        ] {
            assert!(name.parse::<FieldKind>().is_ok(), "{name}");
        }
        assert!("uuid".parse::<FieldKind>().is_err());
    }

    #[test]
    fn the_schema_is_the_references() {
        let schema = spec().schema().unwrap();
        let names: Vec<(String, bool)> = schema
            .as_struct()
            .fields()
            .iter()
            .map(|f| (f.name.clone(), f.required))
            .collect();
        let expected = [
            ("dat_id", true),
            ("dat_created", true),
            ("dat_deleted", true),
            ("title", false),
            ("owner", false),
            ("tags", false),
            ("dat_txn", true),
            ("dat_tx_time", true),
        ];
        let expected: Vec<(String, bool)> = expected
            .iter()
            .map(|(n, r)| ((*n).to_owned(), *r))
            .collect();
        assert_eq!(names, expected);
    }

    #[test]
    fn writes_are_checked_and_the_last_one_wins() {
        let (a, b) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let mut parts = IcebergParts::new();
        let err = parts
            .write(&spec(), a, &[("zz".into(), RowValue::Int(1))], None, false)
            .unwrap_err();
        assert!(err.to_string().contains("[\"zz\"] are not fields"), "{err}");
        assert!(
            parts
                .write(
                    &spec(),
                    a,
                    &[("title".into(), RowValue::Int(1))],
                    None,
                    false
                )
                .is_err(),
            "a long in a string field"
        );
        parts
            .write(
                &spec(),
                a,
                &[("title".into(), RowValue::Str("x".into()))],
                None,
                false,
            )
            .unwrap();
        parts
            .write(
                &spec(),
                a,
                &[("tags".into(), RowValue::List(vec![RowValue::Int(1)]))],
                Some(4),
                false,
            )
            .unwrap();
        parts.write(&spec(), b, &[], None, false).unwrap();
        parts.erase(&spec(), b);
        assert_eq!(parts.latest("dat.t_t", b), Some(None));
        let batches = parts.batches(9).unwrap();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0].1;
        assert_eq!(batch.num_rows(), 1);
        let tags = batch.column_by_name("tags").unwrap();
        assert_eq!(
            tags.as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "[1]"
        );
        assert_eq!(
            parts.erased(),
            vec![("dat.t_t".to_owned(), vec![b.to_string()])]
        );
    }
}
