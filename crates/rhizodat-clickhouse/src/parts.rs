//! Entity parts in ClickHouse: the port of the Python reference's
//! `ClickHouseStore` parts (`rhizodat.core.parts`).
//!
//! One `MergeTree` table per part, ordered by `(dat_id, dat_tx_time)`: the
//! service columns `dat_id`, `dat_created`, `dat_deleted`, one `Nullable`
//! column per field (its native type, or `String` holding [`encode_value`]'s
//! tagged JSON), then `dat_txn`, `dat_tx_time`. The DDL and the encoding are the
//! reference's, so either implementation reads the other's versions.
//!
//! ClickHouse has no transactions: a Unit of Work's versions are buffered in
//! [`ClickHouseParts`] and travel in the branch's journal payload as row
//! batches (an erase as an `erase` batch of ids), applied once the
//! meta-transaction has committed. The store reads committed versions
//! ([`ClickHouseStore::read_part`] and friends).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use rhizodat_core::rows::{Batch, RowValue, decode_value, encode_value};
use rhizodat_core::store::BoxError;
use serde_json::Value as Json;
use uuid::Uuid;

use crate::{ClickHouseBranch, ClickHouseError, ClickHouseStore, check_table, quote, refuse};

const SERVICE: [&str; 3] = ["dat_id", "dat_created", "dat_deleted"];
const RESERVED: [&str; 2] = ["dat_txn", "dat_tx_time"];

/// The ClickHouse type of a part's field (the reference's `_column_type`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChColumn {
    /// `String`
    String,
    /// `Bool`
    Bool,
    /// `Int64`
    Int64,
    /// `Float64`
    Float64,
    /// `DateTime64(6, 'UTC')`
    DateTime,
    /// `Date32`
    Date,
    /// `UUID`
    Uuid,
    /// `String` holding the value as tagged JSON
    Json,
}

impl ChColumn {
    /// The type as SQL (before `Nullable`).
    #[must_use]
    pub fn sql(self) -> &'static str {
        match self {
            ChColumn::String | ChColumn::Json => "String",
            ChColumn::Bool => "Bool",
            ChColumn::Int64 => "Int64",
            ChColumn::Float64 => "Float64",
            ChColumn::DateTime => "DateTime64(6, 'UTC')",
            ChColumn::Date => "Date32",
            ChColumn::Uuid => "UUID",
        }
    }

    /// A stored value as it goes into the payload: JSON fields as their tagged
    /// text, the rest as they are.
    fn stored(self, value: &RowValue) -> RowValue {
        match (self, value) {
            (_, RowValue::Null) => RowValue::Null,
            (ChColumn::Json, v) => RowValue::Str(encode_value(v)),
            (_, v) => v.clone(),
        }
    }

    fn decode(self, raw: &Json) -> Result<RowValue, BoxError> {
        if raw.is_null() {
            return Ok(RowValue::Null);
        }
        let text = || {
            raw.as_str()
                .map(str::to_owned)
                .ok_or_else(|| BoxError::from(format!("not text: {raw}")))
        };
        Ok(match self {
            ChColumn::String => RowValue::Str(text()?),
            ChColumn::Bool => RowValue::Bool(raw.as_bool().ok_or("not a boolean")?),
            ChColumn::Int64 => RowValue::Int(int(raw)?),
            ChColumn::Float64 => RowValue::Float(match raw {
                Json::String(s) => s.parse()?,
                n => n.as_f64().ok_or("not a number")?,
            }),
            ChColumn::DateTime => {
                let t = text()?;
                // `date_time_output_format = 'iso'` spells UTC as `Z`.
                RowValue::DateTime(match t.strip_suffix('Z') {
                    Some(base) => format!("{base}+00:00"),
                    None => t,
                })
            }
            ChColumn::Date => RowValue::Date(text()?),
            ChColumn::Uuid => RowValue::Uuid(text()?.parse()?),
            ChColumn::Json => decode_value(&text()?)?,
        })
    }
}

impl std::str::FromStr for ChColumn {
    type Err = ClickHouseError;

    fn from_str(s: &str) -> Result<Self, ClickHouseError> {
        Ok(match s {
            "String" => ChColumn::String,
            "Bool" => ChColumn::Bool,
            "Int64" => ChColumn::Int64,
            "Float64" => ChColumn::Float64,
            "DateTime64(6, 'UTC')" => ChColumn::DateTime,
            "Date32" => ChColumn::Date,
            "UUID" => ChColumn::Uuid,
            "json" => ChColumn::Json,
            other => {
                return Err(ClickHouseError(format!(
                    "not a part column type: {other:?}"
                )));
            }
        })
    }
}

fn int(raw: &Json) -> Result<i64, BoxError> {
    match raw {
        Json::String(s) => Ok(s.parse()?),
        n => n
            .as_i64()
            .ok_or_else(|| format!("not an integer: {n}").into()),
    }
}

/// One store's share of one entity type: its table and typed fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClickHousePartSpec {
    type_name: String,
    table: String,
    columns: Vec<(String, ChColumn)>,
}

impl ClickHousePartSpec {
    /// The part of `type_name` stored in `table`, with `columns` (field, type)
    /// in order.
    ///
    /// # Errors
    ///
    /// [`ClickHouseError`] for a table or field that is not a plain name, a
    /// field named like a service column, or a field given twice.
    pub fn new(
        type_name: &str,
        table: &str,
        columns: Vec<(String, ChColumn)>,
    ) -> Result<Self, ClickHouseError> {
        check_table(table)?;
        if table.contains('.') {
            return Err(ClickHouseError(format!(
                "a part table is in the store's database: {table:?}"
            )));
        }
        for (i, (name, _)) in columns.iter().enumerate() {
            check_table(name)
                .map_err(|_| ClickHouseError(format!("{type_name}: invalid field {name:?}")))?;
            if name.contains('.')
                || SERVICE.contains(&name.as_str())
                || RESERVED.contains(&name.as_str())
            {
                return Err(ClickHouseError(format!(
                    "{type_name}: {name} is a service column, not a field"
                )));
            }
            if columns[..i].iter().any(|(n, _)| n == name) {
                return Err(ClickHouseError(format!(
                    "{type_name}: field {name} is given twice"
                )));
            }
        }
        Ok(Self {
            type_name: type_name.to_owned(),
            table: table.to_owned(),
            columns,
        })
    }

    /// The table.
    #[must_use]
    pub fn table(&self) -> &str {
        &self.table
    }

    /// The fields, in order.
    pub fn fields(&self) -> impl Iterator<Item = &str> {
        self.columns.iter().map(|(n, _)| n.as_str())
    }

    fn create(&self) -> String {
        let mut fields = String::new();
        for (name, ty) in &self.columns {
            let _ = write!(fields, ", `{name}` Nullable({})", ty.sql());
        }
        format!(
            "CREATE TABLE IF NOT EXISTS `{}` ( dat_id UUID, dat_created Int64, dat_deleted Bool{fields}, \
             dat_txn UUID, dat_tx_time Int64, INDEX dat_txn_idx dat_txn TYPE bloom_filter GRANULARITY 4\
             ) ENGINE = MergeTree ORDER BY (dat_id, dat_tx_time)",
            self.table
        )
    }

    /// Every column of the table, in order.
    fn all_columns(&self) -> Vec<String> {
        SERVICE
            .iter()
            .map(|s| (*s).to_owned())
            .chain(self.columns.iter().map(|(n, _)| n.clone()))
            .chain(RESERVED.iter().map(|s| (*s).to_owned()))
            .collect()
    }

    fn select(&self, filter: &str, order: &str) -> String {
        let names: Vec<String> = self
            .all_columns()
            .iter()
            .map(|c| format!("`{c}`"))
            .collect();
        format!(
            "SELECT {} FROM {} WHERE {filter} ORDER BY {order} FORMAT JSONEachRow",
            names.join(", "),
            quote(&self.table)
        )
    }

    fn version(&self, row: &serde_json::Map<String, Json>) -> Result<PartVersion, BoxError> {
        let get = |c: &str| row.get(c).unwrap_or(&Json::Null);
        let values = self
            .columns
            .iter()
            .map(|(n, t)| t.decode(get(n)))
            .collect::<Result<_, _>>()?;
        Ok(PartVersion {
            dat_txn: get("dat_txn").as_str().ok_or("no dat_txn")?.parse()?,
            dat_tx_time: Some(int(get("dat_tx_time"))?),
            created: Some(int(get("dat_created"))?),
            deleted: get("dat_deleted").as_bool().unwrap_or(false),
            values,
        })
    }

    /// `values` in column order, `NULL` for a field left out.
    fn ordered(&self, values: &[(String, RowValue)]) -> Result<Vec<RowValue>, ClickHouseError> {
        let mut unknown: Vec<&str> = values
            .iter()
            .map(|(n, _)| n.as_str())
            .filter(|n| !self.columns.iter().any(|(c, _)| c == n))
            .collect();
        if !unknown.is_empty() {
            unknown.sort_unstable();
            return Err(ClickHouseError(format!(
                "{}: {unknown:?} are not fields of this part",
                self.type_name
            )));
        }
        Ok(self
            .columns
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
    /// The fields' values, in column order.
    pub values: Vec<RowValue>,
}

/// A buffered version, not yet staged.
#[derive(Clone, Debug, PartialEq)]
pub struct PendingVersion {
    /// The time of the entity's first version; `None`: this is the first.
    pub created: Option<i64>,
    /// A soft delete.
    pub deleted: bool,
    /// The fields' values, in column order.
    pub values: Vec<RowValue>,
}

#[derive(Clone, Debug)]
struct PartWrites {
    spec: ClickHousePartSpec,
    versions: BTreeMap<Uuid, PendingVersion>,
    erased: BTreeSet<Uuid>,
}

/// A Unit of Work's part writes for a ClickHouse store, buffered client-side
/// (one version per entity: the last write wins) and staged as row batches.
#[derive(Clone, Debug, Default)]
pub struct ClickHouseParts {
    parts: Vec<PartWrites>,
}

impl ClickHouseParts {
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

    fn of(&mut self, spec: &ClickHousePartSpec) -> &mut PartWrites {
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
    /// [`ClickHouseError`] for a field the part does not have.
    pub fn write(
        &mut self,
        spec: &ClickHousePartSpec,
        dat_id: Uuid,
        values: &[(String, RowValue)],
        created: Option<i64>,
        deleted: bool,
    ) -> Result<(), ClickHouseError> {
        let values = spec.ordered(values)?;
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
    pub fn erase(&mut self, spec: &ClickHousePartSpec, dat_id: Uuid) {
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

    /// The row batches of the payload: every erase first, then the versions.
    fn batches(
        &self,
        dat_txn: Uuid,
        dat_tx_time: i64,
        max_rows: usize,
    ) -> Result<Vec<Batch>, BoxError> {
        let mut out = Vec::new();
        for part in self.parts.iter().filter(|p| !p.erased.is_empty()) {
            out.push(Batch {
                table: part.spec.table.clone(),
                columns: vec!["dat_id".to_owned()],
                rows: part
                    .erased
                    .iter()
                    .map(|id| vec![RowValue::Uuid(*id)])
                    .collect(),
                erase: true,
            });
        }
        for part in self.parts.iter().filter(|p| !p.versions.is_empty()) {
            if part.versions.len() > max_rows {
                return Err(refuse(format!(
                    "{} rows for {:?} exceed max_rows_per_table={max_rows}",
                    part.versions.len(),
                    part.spec.table
                )));
            }
            let rows = part
                .versions
                .iter()
                .map(|(id, v)| {
                    let mut row = vec![
                        RowValue::Uuid(*id),
                        RowValue::Int(v.created.unwrap_or(dat_tx_time)),
                        RowValue::Bool(v.deleted),
                    ];
                    row.extend(
                        part.spec
                            .columns
                            .iter()
                            .zip(&v.values)
                            .map(|((_, t), x)| t.stored(x)),
                    );
                    row.extend([RowValue::Uuid(dat_txn), RowValue::Int(dat_tx_time)]);
                    row
                })
                .collect();
            out.push(Batch {
                table: part.spec.table.clone(),
                columns: part.spec.all_columns(),
                rows,
                erase: false,
            });
        }
        Ok(out)
    }
}

impl ClickHouseBranch {
    /// Replace the branch's buffered part writes (staged with its rows).
    pub fn load_parts(&mut self, parts: ClickHouseParts) {
        self.parts = parts;
    }

    /// The part batches of the payload, before the Unit of Work's own rows.
    pub(crate) fn part_batches(
        &self,
        dat_txn: Uuid,
        dat_tx_time: i64,
    ) -> Result<Vec<Batch>, BoxError> {
        self.parts
            .batches(dat_txn, dat_tx_time, self.max_rows_per_table)
    }
}

impl ClickHouseStore {
    /// Create the part's table if missing. Idempotent.
    ///
    /// # Errors
    ///
    /// The server's error.
    pub async fn install_part(
        &self,
        spec: &ClickHousePartSpec,
    ) -> Result<(), clickhouse::error::Error> {
        self.client.query(&spec.create()).execute().await
    }

    async fn rows(
        &self,
        spec: &ClickHousePartSpec,
        sql: String,
        binds: &[Json],
    ) -> Result<Vec<serde_json::Map<String, Json>>, BoxError> {
        let mut query = self.client.query(&sql);
        for b in binds {
            query = match b {
                Json::String(s) => query.bind(s.as_str()),
                Json::Number(n) => query.bind(n.as_i64().ok_or("not an integer")?),
                other => return Err(format!("cannot bind {other}").into()),
            };
        }
        let raw = query
            .with_setting("output_format_json_quote_64bit_integers", "0")
            .with_setting("output_format_json_quote_denormals", "1")
            .with_setting("date_time_output_format", "iso")
            .fetch_bytes("JSONEachRow")?
            .collect()
            .await?;
        let text = std::str::from_utf8(&raw)?;
        text.lines()
            .filter(|l| !l.is_empty())
            .map(|l| match serde_json::from_str(l)? {
                Json::Object(row) => Ok(row),
                other => Err(format!("{}: not a row: {other}", spec.table).into()),
            })
            .collect()
    }

    /// The latest committed version with `dat_tx_time <= known_at` (any, if `None`).
    ///
    /// # Errors
    ///
    /// The server's error, or a value the part's types cannot decode.
    pub async fn read_part(
        &self,
        spec: &ClickHousePartSpec,
        dat_id: Uuid,
        known_at: Option<i64>,
    ) -> Result<Option<PartVersion>, BoxError> {
        let mut binds = vec![Json::String(dat_id.to_string())];
        let mut filter = "dat_id = toUUID(?)".to_owned();
        if let Some(k) = known_at {
            filter.push_str(" AND dat_tx_time <= ?");
            binds.push(Json::from(k));
        }
        let sql = spec.select(&filter, "dat_tx_time DESC LIMIT 1");
        let rows = self.rows(spec, sql, &binds).await?;
        rows.first().map(|r| spec.version(r)).transpose()
    }

    /// Every committed version of `dat_id`'s part, oldest first.
    ///
    /// # Errors
    ///
    /// As [`read_part`](Self::read_part).
    pub async fn part_history(
        &self,
        spec: &ClickHousePartSpec,
        dat_id: Uuid,
    ) -> Result<Vec<PartVersion>, BoxError> {
        let sql = spec.select("dat_id = toUUID(?)", "dat_tx_time");
        let rows = self
            .rows(spec, sql, &[Json::String(dat_id.to_string())])
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
        spec: &ClickHousePartSpec,
        known_at: Option<i64>,
    ) -> Result<Vec<(Uuid, PartVersion)>, BoxError> {
        let (filter, binds) = match known_at {
            Some(k) => ("dat_tx_time <= ?", vec![Json::from(k)]),
            None => ("1", Vec::new()),
        };
        let sql = spec.select(filter, "dat_tx_time, dat_id");
        let rows = self.rows(spec, sql, &binds).await?;
        rows.iter()
            .map(|r| {
                let id = r.get("dat_id").and_then(Json::as_str).ok_or("no dat_id")?;
                Ok((id.parse()?, spec.version(r)?))
            })
            .collect()
    }

    /// Entities whose latest version is a soft delete older than `deleted_before`.
    ///
    /// # Errors
    ///
    /// The server's error.
    pub async fn expired_parts(
        &self,
        spec: &ClickHousePartSpec,
        deleted_before: i64,
        limit: i64,
    ) -> Result<Vec<Uuid>, BoxError> {
        let sql = format!(
            "SELECT dat_id FROM {} GROUP BY dat_id \
             HAVING argMax(dat_deleted, dat_tx_time) AND max(dat_tx_time) < ? \
             ORDER BY dat_id LIMIT ? FORMAT JSONEachRow",
            quote(&spec.table)
        );
        let rows = self
            .rows(
                spec,
                sql,
                &[Json::from(deleted_before), Json::from(limit.max(0))],
            )
            .await?;
        rows.iter()
            .map(|r| {
                let id = r.get("dat_id").and_then(Json::as_str).ok_or("no dat_id")?;
                Ok(id.parse()?)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> ClickHousePartSpec {
        ClickHousePartSpec::new(
            "t.T",
            "t_t",
            vec![("a".into(), ChColumn::String), ("j".into(), ChColumn::Json)],
        )
        .unwrap()
    }

    #[test]
    fn a_spec_takes_plain_names_and_no_service_columns() {
        assert!(ClickHousePartSpec::new("t.T", "bad name", vec![]).is_err());
        assert!(ClickHousePartSpec::new("t.T", "db.t", vec![]).is_err());
        for service in ["dat_id", "dat_tx_time"] {
            let err = ClickHousePartSpec::new("t.T", "t", vec![(service.into(), ChColumn::Uuid)])
                .unwrap_err();
            assert!(err.to_string().contains("service column"), "{err}");
        }
        let twice = vec![("a".into(), ChColumn::String), ("a".into(), ChColumn::Bool)];
        assert!(ClickHousePartSpec::new("t.T", "t", twice).is_err());
        for ty in [
            ChColumn::String,
            ChColumn::Bool,
            ChColumn::Int64,
            ChColumn::Float64,
            ChColumn::DateTime,
            ChColumn::Date,
            ChColumn::Uuid,
        ] {
            assert_eq!(ty.sql().parse::<ChColumn>().unwrap(), ty);
        }
        assert_eq!("json".parse::<ChColumn>().unwrap(), ChColumn::Json);
        assert!("Int32".parse::<ChColumn>().is_err());
    }

    #[test]
    fn the_ddl_is_the_references() {
        assert_eq!(
            spec().create(),
            "CREATE TABLE IF NOT EXISTS `t_t` ( dat_id UUID, dat_created Int64, dat_deleted Bool, \
             `a` Nullable(String), `j` Nullable(String), dat_txn UUID, dat_tx_time Int64, \
             INDEX dat_txn_idx dat_txn TYPE bloom_filter GRANULARITY 4) ENGINE = MergeTree \
             ORDER BY (dat_id, dat_tx_time)"
        );
    }

    #[test]
    fn the_payload_erases_first_and_keeps_the_last_write() {
        let (a, b, txn) = (Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(9));
        let mut parts = ClickHouseParts::new();
        let err = parts
            .write(&spec(), a, &[("zz".into(), RowValue::Int(1))], None, false)
            .unwrap_err();
        assert!(err.to_string().contains("[\"zz\"] are not fields"), "{err}");
        parts
            .write(
                &spec(),
                a,
                &[("a".into(), RowValue::Str("first".into()))],
                None,
                false,
            )
            .unwrap();
        parts
            .write(
                &spec(),
                a,
                &[("j".into(), RowValue::List(vec![RowValue::Int(1)]))],
                Some(3),
                false,
            )
            .unwrap();
        parts.write(&spec(), b, &[], None, false).unwrap();
        parts.erase(&spec(), b);
        assert_eq!(parts.latest("t_t", b), Some(None));
        assert_eq!(parts.latest("t_t", Uuid::nil()), None);
        let batches = parts.batches(txn, 50, 10).unwrap();
        assert_eq!(batches.len(), 2);
        assert!(batches[0].erase);
        assert_eq!(batches[0].rows, vec![vec![RowValue::Uuid(b)]]);
        assert_eq!(
            batches[1].rows,
            vec![vec![
                RowValue::Uuid(a),
                RowValue::Int(3),
                RowValue::Bool(false),
                RowValue::Null,
                RowValue::Str("[1]".into()),
                RowValue::Uuid(txn),
                RowValue::Int(50),
            ]]
        );
        assert!(
            parts.batches(txn, 50, 0).is_err(),
            "over max_rows_per_table"
        );
    }

    #[test]
    fn values_decode_per_column_type() {
        let decode = |t: ChColumn, j: Json| t.decode(&j).unwrap();
        assert_eq!(decode(ChColumn::Int64, Json::from("-5")), RowValue::Int(-5));
        assert_eq!(
            decode(ChColumn::Float64, Json::from("-inf")),
            RowValue::Float(f64::NEG_INFINITY)
        );
        assert!(
            matches!(decode(ChColumn::Float64, Json::from("nan")), RowValue::Float(f) if f.is_nan())
        );
        assert_eq!(
            decode(
                ChColumn::DateTime,
                Json::from("2026-10-31T01:02:03.000004Z")
            ),
            RowValue::DateTime("2026-10-31T01:02:03.000004+00:00".into())
        );
        assert_eq!(decode(ChColumn::Bool, Json::Null), RowValue::Null);
        assert!(ChColumn::Uuid.decode(&Json::from("x")).is_err());
        assert!(ChColumn::Bool.decode(&Json::from(1)).is_err());
    }
}
