//! Entity parts in PostgreSQL: the port of the Python reference's
//! `PostgresStore` parts (`rhizodat.core.parts`).
//!
//! One table per part, named after its type: the service columns `dat_id`,
//! `dat_txn`, `dat_tx_time`, `dat_created`, `dat_deleted`, then one column per
//! field (`jsonb`, holding [`encode_value`]'s tagged JSON, for values without a
//! native type), keyed by `(dat_id, dat_txn)`. The DDL and the encoding are the
//! reference's, so either implementation reads the other's versions.
//!
//! A Unit of Work's writes are buffered in [`PostgresParts`] and applied, in
//! order, inside the branch's transaction at stage, stamped with the
//! meta-transaction's time: they commit atomically with it. The store reads
//! committed versions ([`PostgresStore::read_part`] and friends).

use std::fmt::{self, Write as _};
use std::str::FromStr;

use rhizodat_core::rows::{RowValue, decode_value, encode_value};
use rhizodat_core::store::BoxError;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, Row};
use uuid::Uuid;

use crate::{ConfigError, PostgresStore, check_identifier, pg_text, quote_ident};

const SERVICE: [&str; 5] = [
    "dat_id",
    "dat_txn",
    "dat_tx_time",
    "dat_created",
    "dat_deleted",
];

/// A concurrent `CREATE TABLE IF NOT EXISTS` lost the race: duplicate table / type.
const DUPLICATE: [&str; 2] = ["42P07", "23505"];

/// The PostgreSQL type of a part's field (the reference's `_column_type`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnType {
    /// `text`
    Text,
    /// `boolean`
    Boolean,
    /// `bigint`
    Bigint,
    /// `double precision`
    Double,
    /// `numeric`
    Numeric,
    /// `timestamptz`
    Timestamptz,
    /// `date`
    Date,
    /// `uuid`
    Uuid,
    /// `bytea`
    Bytea,
    /// `jsonb`, holding a value as tagged JSON
    Jsonb,
}

impl ColumnType {
    /// The type as SQL.
    #[must_use]
    pub fn sql(self) -> &'static str {
        match self {
            ColumnType::Text => "text",
            ColumnType::Boolean => "boolean",
            ColumnType::Bigint => "bigint",
            ColumnType::Double => "double precision",
            ColumnType::Numeric => "numeric",
            ColumnType::Timestamptz => "timestamptz",
            ColumnType::Date => "date",
            ColumnType::Uuid => "uuid",
            ColumnType::Bytea => "bytea",
            ColumnType::Jsonb => "jsonb",
        }
    }

    /// The column as text the value is decoded from.
    fn select(self, column: &str) -> String {
        match self {
            ColumnType::Text => column.to_owned(),
            ColumnType::Timestamptz | ColumnType::Date => format!("to_json({column}) #>> '{{}}'"),
            ColumnType::Bytea => format!("encode({column}, 'hex')"),
            _ => format!("{column}::text"),
        }
    }

    /// The text of a value bound as `$n::text::<type>`.
    fn text(self, value: &RowValue) -> Result<Option<String>, BoxError> {
        if self == ColumnType::Jsonb {
            return Ok(Some(encode_value(value)));
        }
        pg_text(value)
    }

    fn decode(self, text: Option<String>) -> Result<RowValue, BoxError> {
        let Some(text) = text else {
            return Ok(RowValue::Null);
        };
        Ok(match self {
            ColumnType::Text => RowValue::Str(text),
            ColumnType::Boolean => RowValue::Bool(text == "true"),
            ColumnType::Bigint => RowValue::Int(text.parse()?),
            ColumnType::Double => RowValue::Float(text.parse()?),
            ColumnType::Numeric => RowValue::Decimal(text),
            ColumnType::Timestamptz => RowValue::DateTime(text),
            ColumnType::Date => RowValue::Date(text),
            ColumnType::Uuid => RowValue::Uuid(text.parse()?),
            ColumnType::Bytea => RowValue::Bytes(unhex(&text)?),
            ColumnType::Jsonb => decode_value(&text)?,
        })
    }
}

impl FromStr for ColumnType {
    type Err = ConfigError;

    fn from_str(s: &str) -> Result<Self, ConfigError> {
        Ok(match s {
            "text" => ColumnType::Text,
            "boolean" => ColumnType::Boolean,
            "bigint" => ColumnType::Bigint,
            "double precision" => ColumnType::Double,
            "numeric" => ColumnType::Numeric,
            "timestamptz" => ColumnType::Timestamptz,
            "date" => ColumnType::Date,
            "uuid" => ColumnType::Uuid,
            "bytea" => ColumnType::Bytea,
            "jsonb" => ColumnType::Jsonb,
            other => return Err(ConfigError(format!("not a part column type: {other:?}"))),
        })
    }
}

fn unhex(text: &str) -> Result<Vec<u8>, BoxError> {
    if !text.len().is_multiple_of(2) {
        return Err(format!("odd hex length {}", text.len()).into());
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).map_err(Into::into))
        .collect()
}

/// One store's share of one entity type: its table and typed fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartSpec {
    type_name: String,
    table: String,
    columns: Vec<(String, ColumnType)>,
}

impl PartSpec {
    /// The part of `type_name` stored in `table`, with `columns` (field, type)
    /// in order.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] for a table or field that is not a plain identifier, a
    /// field named like a service column, or a field given twice.
    pub fn new(
        type_name: &str,
        table: &str,
        columns: Vec<(String, ColumnType)>,
    ) -> Result<Self, ConfigError> {
        check_identifier("table", table)?;
        for (i, (name, _)) in columns.iter().enumerate() {
            check_identifier("field", name)?;
            if SERVICE.contains(&name.as_str()) {
                return Err(ConfigError(format!(
                    "{type_name}: {name} is a service column, not a field"
                )));
            }
            if columns[..i].iter().any(|(n, _)| n == name) {
                return Err(ConfigError(format!(
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

    fn create(&self) -> [String; 2] {
        let table = quote_ident(&self.table);
        let mut fields = String::new();
        for (name, ty) in &self.columns {
            let _ = write!(fields, ", {} {}", quote_ident(name), ty.sql());
        }
        // The table name is ASCII (checked): 50 bytes are 50 characters.
        let index = quote_ident(&format!(
            "{}_dat_time",
            &self.table[..self.table.len().min(50)]
        ));
        [
            format!(
                "CREATE TABLE IF NOT EXISTS {table} ( dat_id uuid NOT NULL, dat_txn uuid NOT NULL, \
                 dat_tx_time bigint, dat_created bigint, dat_deleted boolean NOT NULL DEFAULT \
                 false{fields}, PRIMARY KEY (dat_id, dat_txn))"
            ),
            format!("CREATE INDEX IF NOT EXISTS {index} ON {table} (dat_id, dat_tx_time)"),
        ]
    }

    fn select(&self, filter: &str, order: &str) -> String {
        let fields: Vec<String> = self
            .columns
            .iter()
            .map(|(n, t)| t.select(&quote_ident(n)))
            .collect();
        format!(
            "SELECT dat_id, dat_txn, dat_tx_time, dat_created, dat_deleted{}{} FROM {} \
             WHERE {filter} ORDER BY {order}",
            if fields.is_empty() { "" } else { ", " },
            fields.join(", "),
            quote_ident(&self.table)
        )
    }

    fn version(&self, row: &Row) -> Result<PartVersion, BoxError> {
        let values = self
            .columns
            .iter()
            .enumerate()
            .map(|(i, (_, t))| t.decode(row.try_get(SERVICE.len() + i)?))
            .collect::<Result<_, BoxError>>()?;
        Ok(PartVersion {
            dat_txn: row.try_get(1)?,
            dat_tx_time: row.try_get(2)?,
            created: row.try_get(3)?,
            deleted: row.try_get(4)?,
            values,
        })
    }

    /// `values` in column order, `NULL` for a field left out.
    fn ordered(&self, values: &[(String, RowValue)]) -> Result<Vec<RowValue>, ConfigError> {
        let mut unknown: Vec<&str> = values
            .iter()
            .map(|(n, _)| n.as_str())
            .filter(|n| !self.columns.iter().any(|(c, _)| c == n))
            .collect();
        if !unknown.is_empty() {
            unknown.sort_unstable();
            return Err(ConfigError(format!(
                "{}: {unknown:?} are not fields of this part",
                self.type_name
            )));
        }
        let mut out = Vec::with_capacity(self.columns.len());
        for (name, ty) in &self.columns {
            let value = values
                .iter()
                .rev()
                .find(|(n, _)| n == name)
                .map_or(RowValue::Null, |(_, v)| v.clone());
            if *ty != ColumnType::Jsonb && matches!(value, RowValue::List(_) | RowValue::Map(_)) {
                return Err(ConfigError(format!(
                    "{}: field {name} is {}, not a list or a map",
                    self.type_name,
                    ty.sql()
                )));
            }
            out.push(value);
        }
        Ok(out)
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

/// A buffered version, not yet staged: its own time is not known.
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
enum Op {
    Write {
        spec: PartSpec,
        dat_id: Uuid,
        version: PendingVersion,
    },
    Erase {
        spec: PartSpec,
        dat_id: Uuid,
    },
}

/// A Unit of Work's part writes for a PostgreSQL store, buffered client-side and
/// applied in order inside the branch's transaction at stage.
#[derive(Clone, Debug, Default)]
pub struct PostgresParts {
    ops: Vec<Op>,
}

impl PostgresParts {
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
    /// [`ConfigError`] for a field the part does not have, or a list or map in a
    /// field without a `jsonb` column.
    pub fn write(
        &mut self,
        spec: &PartSpec,
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

    /// Physically remove every version of `dat_id`'s part (committed ones too).
    pub fn erase(&mut self, spec: &PartSpec, dat_id: Uuid) {
        self.ops.push(Op::Erase {
            spec: spec.clone(),
            dat_id,
        });
    }

    /// This Unit of Work's own latest version of `dat_id`'s part: `None` when it
    /// did not touch it, `Some(None)` when it erased it last.
    #[must_use]
    pub fn latest(&self, table: &str, dat_id: Uuid) -> Option<Option<&PendingVersion>> {
        self.ops.iter().rev().find_map(|op| match op {
            Op::Write {
                spec,
                dat_id: id,
                version,
            } if spec.table == table && *id == dat_id => Some(Some(version)),
            Op::Erase { spec, dat_id: id } if spec.table == table && *id == dat_id => Some(None),
            _ => None,
        })
    }
}

/// Apply `parts` in the branch's open transaction, each version stamped with
/// the meta-transaction and its time (`created` defaults to that time).
pub(crate) async fn apply_parts(
    client: &Client,
    parts: &PostgresParts,
    dat_txn: Uuid,
    dat_tx_time: i64,
) -> Result<(), BoxError> {
    for op in &parts.ops {
        match op {
            Op::Write {
                spec,
                dat_id,
                version,
            } => {
                let names: Vec<String> = SERVICE
                    .iter()
                    .map(|s| (*s).to_owned())
                    .chain(spec.columns.iter().map(|(n, _)| quote_ident(n)))
                    .collect();
                let mut placeholders: Vec<String> =
                    ["$1", "$2", "$3", "$4", "$5"].map(str::to_owned).to_vec();
                let mut texts = Vec::with_capacity(spec.columns.len());
                for (i, ((_, ty), value)) in spec.columns.iter().zip(&version.values).enumerate() {
                    placeholders.push(format!("${}::text::{}", SERVICE.len() + i + 1, ty.sql()));
                    texts.push(ty.text(value)?);
                }
                let updates: Vec<String> = names[2..]
                    .iter()
                    .map(|c| format!("{c} = EXCLUDED.{c}"))
                    .collect();
                let sql = format!(
                    "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (dat_id, dat_txn) DO UPDATE SET {}",
                    quote_ident(&spec.table),
                    names.join(", "),
                    placeholders.join(", "),
                    updates.join(", ")
                );
                let created = version.created.unwrap_or(dat_tx_time);
                let mut params: Vec<&(dyn ToSql + Sync)> =
                    vec![dat_id, &dat_txn, &dat_tx_time, &created, &version.deleted];
                params.extend(texts.iter().map(|t| t as &(dyn ToSql + Sync)));
                client.execute(&sql, &params).await?;
            }
            Op::Erase { spec, dat_id } => {
                let sql = format!("DELETE FROM {} WHERE dat_id = $1", quote_ident(&spec.table));
                client.execute(&sql, &[dat_id]).await?;
            }
        }
    }
    Ok(())
}

impl PostgresStore {
    /// Create the part's table and index if missing (and `dat_applied`).
    /// Idempotent, also against a concurrent install.
    ///
    /// # Errors
    ///
    /// The driver's error.
    pub async fn install_part(&self, spec: &PartSpec) -> Result<(), tokio_postgres::Error> {
        self.install().await?;
        let client = self.connect().await?;
        for statement in spec.create() {
            if let Err(e) = client.batch_execute(&statement).await
                && !e.code().is_some_and(|c| DUPLICATE.contains(&c.code()))
            {
                return Err(e);
            }
        }
        Ok(())
    }

    /// The latest committed version with `dat_tx_time <= known_at` (any, if `None`).
    ///
    /// # Errors
    ///
    /// The driver's error, or a value the part's types cannot decode.
    pub async fn read_part(
        &self,
        spec: &PartSpec,
        dat_id: Uuid,
        known_at: Option<i64>,
    ) -> Result<Option<PartVersion>, BoxError> {
        let sql = spec.select(
            "dat_id = $1 AND dat_tx_time IS NOT NULL AND ($2::bigint IS NULL OR dat_tx_time <= $2)",
            "dat_tx_time DESC LIMIT 1",
        );
        let rows = self
            .connect()
            .await?
            .query(&sql, &[&dat_id, &known_at])
            .await?;
        rows.first().map(|r| spec.version(r)).transpose()
    }

    /// Every committed version of `dat_id`'s part, oldest first.
    ///
    /// # Errors
    ///
    /// As [`read_part`](Self::read_part).
    pub async fn part_history(
        &self,
        spec: &PartSpec,
        dat_id: Uuid,
    ) -> Result<Vec<PartVersion>, BoxError> {
        let sql = spec.select("dat_id = $1 AND dat_tx_time IS NOT NULL", "dat_tx_time");
        let rows = self.connect().await?.query(&sql, &[&dat_id]).await?;
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
        spec: &PartSpec,
        known_at: Option<i64>,
    ) -> Result<Vec<(Uuid, PartVersion)>, BoxError> {
        let sql = spec.select(
            "dat_tx_time IS NOT NULL AND ($1::bigint IS NULL OR dat_tx_time <= $1)",
            "dat_tx_time, dat_id",
        );
        let rows = self.connect().await?.query(&sql, &[&known_at]).await?;
        rows.iter()
            .map(|r| Ok((r.try_get(0)?, spec.version(r)?)))
            .collect()
    }

    /// Entities whose latest version is a soft delete older than `deleted_before`.
    ///
    /// # Errors
    ///
    /// The driver's error.
    pub async fn expired_parts(
        &self,
        spec: &PartSpec,
        deleted_before: i64,
        limit: i64,
    ) -> Result<Vec<Uuid>, BoxError> {
        let sql = format!(
            "SELECT dat_id FROM (SELECT DISTINCT ON (dat_id) dat_id, dat_deleted, dat_tx_time \
             FROM {} WHERE dat_tx_time IS NOT NULL ORDER BY dat_id, dat_tx_time DESC) latest \
             WHERE dat_deleted AND dat_tx_time < $1 ORDER BY dat_id LIMIT $2",
            quote_ident(&spec.table)
        );
        let rows = self
            .connect()
            .await?
            .query(&sql, &[&deleted_before, &limit.max(0)])
            .await?;
        rows.iter()
            .map(|r| r.try_get(0).map_err(Into::into))
            .collect()
    }
}

impl fmt::Display for PartSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} in {}", self.type_name, self.table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> PartSpec {
        PartSpec::new(
            "t.T",
            "t_t",
            vec![
                ("a".into(), ColumnType::Text),
                ("j".into(), ColumnType::Jsonb),
            ],
        )
        .unwrap()
    }

    #[test]
    fn a_spec_takes_plain_identifiers_and_no_service_columns() {
        assert!(PartSpec::new("t.T", "bad name", vec![]).is_err());
        assert!(PartSpec::new("t.T", "t", vec![("x y".into(), ColumnType::Text)]).is_err());
        let err =
            PartSpec::new("t.T", "t", vec![("dat_txn".into(), ColumnType::Uuid)]).unwrap_err();
        assert!(err.to_string().contains("service column"), "{err}");
        let twice = vec![
            ("a".into(), ColumnType::Text),
            ("a".into(), ColumnType::Bigint),
        ];
        assert!(PartSpec::new("t.T", "t", twice).is_err());
    }

    #[test]
    fn column_types_are_named_like_the_reference() {
        for ty in [
            ColumnType::Text,
            ColumnType::Boolean,
            ColumnType::Bigint,
            ColumnType::Double,
            ColumnType::Numeric,
            ColumnType::Timestamptz,
            ColumnType::Date,
            ColumnType::Uuid,
            ColumnType::Bytea,
            ColumnType::Jsonb,
        ] {
            assert_eq!(ty.sql().parse::<ColumnType>().unwrap(), ty);
        }
        assert!("integer".parse::<ColumnType>().is_err());
    }

    #[test]
    fn the_ddl_is_the_references() {
        let [table, index] = spec().create();
        assert_eq!(
            table,
            "CREATE TABLE IF NOT EXISTS \"t_t\" ( dat_id uuid NOT NULL, dat_txn uuid NOT NULL, \
             dat_tx_time bigint, dat_created bigint, dat_deleted boolean NOT NULL DEFAULT false, \
             \"a\" text, \"j\" jsonb, PRIMARY KEY (dat_id, dat_txn))"
        );
        assert_eq!(
            index,
            "CREATE INDEX IF NOT EXISTS \"t_t_dat_time\" ON \"t_t\" (dat_id, dat_tx_time)"
        );
        let long = PartSpec::new("t.T", &"x".repeat(63), vec![]).unwrap();
        assert!(long.create()[1].contains(&format!("\"{}_dat_time\"", "x".repeat(50))));
    }

    #[test]
    fn writes_are_checked_and_ordered_by_column() {
        let mut parts = PostgresParts::new();
        let err = parts
            .write(
                &spec(),
                Uuid::nil(),
                &[("zz".into(), RowValue::Int(1))],
                None,
                false,
            )
            .unwrap_err();
        assert!(err.to_string().contains("[\"zz\"] are not fields"), "{err}");
        let list = RowValue::List(vec![RowValue::Int(1)]);
        assert!(
            parts
                .write(
                    &spec(),
                    Uuid::nil(),
                    &[("a".into(), list.clone())],
                    None,
                    false
                )
                .is_err()
        );
        assert!(parts.is_empty(), "a refused write buffers nothing");
        parts
            .write(
                &spec(),
                Uuid::nil(),
                &[("j".into(), list.clone())],
                Some(3),
                false,
            )
            .unwrap();
        let own = parts.latest("t_t", Uuid::nil()).unwrap().unwrap();
        assert_eq!(own.values, vec![RowValue::Null, list]);
        assert_eq!(own.created, Some(3));
    }

    #[test]
    fn own_writes_read_back_last_first() {
        let (a, b) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let mut parts = PostgresParts::new();
        assert_eq!(parts.latest("t_t", a), None);
        parts.write(&spec(), a, &[], None, false).unwrap();
        parts.erase(&spec(), a);
        assert_eq!(parts.latest("t_t", a), Some(None));
        parts.write(&spec(), a, &[], Some(1), true).unwrap();
        assert!(parts.latest("t_t", a).unwrap().unwrap().deleted);
        assert_eq!(parts.latest("t_t", b), None);
        assert_eq!(parts.latest("other", a), None);
    }

    #[test]
    fn values_decode_per_column_type() {
        let decode = |t: ColumnType, s: &str| t.decode(Some(s.to_owned())).unwrap();
        assert_eq!(decode(ColumnType::Boolean, "false"), RowValue::Bool(false));
        assert!(matches!(decode(ColumnType::Double, "NaN"), RowValue::Float(f) if f.is_nan()));
        assert_eq!(
            decode(ColumnType::Double, "-Infinity"),
            RowValue::Float(f64::NEG_INFINITY)
        );
        assert_eq!(
            decode(ColumnType::Bytea, "00ff"),
            RowValue::Bytes(vec![0, 255])
        );
        assert_eq!(ColumnType::Text.decode(None).unwrap(), RowValue::Null);
        assert!(ColumnType::Bytea.decode(Some("0".into())).is_err());
        assert!(ColumnType::Bigint.decode(Some("x".into())).is_err());
        assert_eq!(
            ColumnType::Jsonb.text(&RowValue::Null).unwrap(),
            Some("null".to_owned()),
            "like the reference, a missing JSON field is JSON null"
        );
    }
}
