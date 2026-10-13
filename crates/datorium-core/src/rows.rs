//! Row batches as journal payloads (the port of `datorium.core.rows`).
//!
//! A one-phase branch (ClickHouse) is applied only after the decision,
//! possibly by recovery on another host, so its rows travel in the journal.
//! The encoding is versioned JSON; values JSON cannot carry exactly are tagged
//! `{"$": tag, "v": value}`:
//!
//! - `uuid`, `decimal` (as text, so `12.3400` stays `12.3400`), `bytes`
//!   (base64), `date`, `datetime` (ISO 8601; a zone is its UTC offset);
//! - `float` only when not finite (`inf`, `-inf`, `nan`);
//! - `map` for every object (`[[key, value], ...]`), so a plain object can
//!   never be mistaken for a tag.
//!
//! Decimals, dates and datetimes keep their text: the core moves them between
//! the journal and a store and never computes with them. The format is pinned
//! by `conformance/rows.json`.

use std::fmt;

use base64::Engine as _;
use serde_json::{Map, Number, Value as Json, json};
use uuid::Uuid;

const VERSION: i64 = 1;

/// One value of a row.
#[derive(Clone, Debug, PartialEq)]
pub enum RowValue {
    /// `NULL`
    Null,
    /// A boolean.
    Bool(bool),
    /// A 64-bit integer.
    Int(i64),
    /// A 64-bit float (any, including non-finite).
    Float(f64),
    /// A string.
    Str(String),
    /// A UUID.
    Uuid(Uuid),
    /// A decimal, as its exact text (`12.3400`, `-0.01`, `1E+3`).
    Decimal(String),
    /// Raw bytes.
    Bytes(Vec<u8>),
    /// A date, `YYYY-MM-DD`.
    Date(String),
    /// A date and time in ISO 8601, naive or with its UTC offset
    /// (`2026-10-12T09:30:15.123456+00:00`).
    DateTime(String),
    /// A list.
    List(Vec<RowValue>),
    /// A map with string keys, in order.
    Map(Vec<(String, RowValue)>),
}

/// Rows for one table, in column order.
#[derive(Clone, Debug, PartialEq)]
pub struct Batch {
    /// The table.
    pub table: String,
    /// Column names.
    pub columns: Vec<String>,
    /// Rows, each with one value per column.
    pub rows: Vec<Vec<RowValue>>,
}

/// A payload that is not a row payload, or rows that cannot become one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowsError(String);

impl fmt::Display for RowsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for RowsError {}

fn invalid(message: impl fmt::Display) -> RowsError {
    RowsError(format!("invalid row payload: {message}"))
}

fn tag(name: &str, value: Json) -> Json {
    let mut object = Map::new();
    object.insert("$".to_owned(), Json::String(name.to_owned()));
    object.insert("v".to_owned(), value);
    Json::Object(object)
}

fn encode(value: &RowValue) -> Json {
    match value {
        RowValue::Null => Json::Null,
        RowValue::Bool(v) => Json::Bool(*v),
        RowValue::Int(v) => Json::from(*v),
        RowValue::Float(v) => match Number::from_f64(*v) {
            Some(n) => Json::Number(n),
            None if v.is_nan() => tag("float", json!("nan")),
            None if *v > 0.0 => tag("float", json!("inf")),
            None => tag("float", json!("-inf")),
        },
        RowValue::Str(v) => Json::String(v.clone()),
        RowValue::Uuid(v) => tag("uuid", json!(v.hyphenated().to_string())),
        RowValue::Decimal(v) => tag("decimal", json!(v)),
        RowValue::Bytes(v) => tag(
            "bytes",
            json!(base64::engine::general_purpose::STANDARD.encode(v)),
        ),
        RowValue::Date(v) => tag("date", json!(v)),
        RowValue::DateTime(v) => tag("datetime", json!(v)),
        RowValue::List(v) => Json::Array(v.iter().map(encode).collect()),
        RowValue::Map(v) => tag(
            "map",
            Json::Array(v.iter().map(|(k, x)| json!([k, encode(x)])).collect()),
        ),
    }
}

/// The journal payload for `batches`.
///
/// # Errors
///
/// [`RowsError`] for a row whose length differs from the columns.
pub fn encode_batches(batches: &[Batch]) -> Result<Vec<u8>, RowsError> {
    let mut out = Vec::with_capacity(batches.len());
    for b in batches {
        let mut rows = Vec::with_capacity(b.rows.len());
        for row in &b.rows {
            if row.len() != b.columns.len() {
                return Err(RowsError(format!(
                    "table {:?}: a row has {} values for {} columns",
                    b.table,
                    row.len(),
                    b.columns.len()
                )));
            }
            rows.push(Json::Array(row.iter().map(encode).collect()));
        }
        out.push(json!({"table": b.table, "columns": b.columns, "rows": rows}));
    }
    let doc = json!({"v": VERSION, "batches": out});
    serde_json::to_vec(&doc).map_err(|e| RowsError(e.to_string()))
}

fn text(value: &Json, what: &str) -> Result<String, RowsError> {
    value
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| invalid(format!("{what} is not a string")))
}

fn is_date(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 10
        && b[4] == b'-'
        && b[7] == b'-'
        && b.iter()
            .enumerate()
            .all(|(i, c)| i == 4 || i == 7 || c.is_ascii_digit())
}

fn decode_tagged(object: &Map<String, Json>) -> Result<RowValue, RowsError> {
    if object.len() != 2 || !object.contains_key("$") || !object.contains_key("v") {
        let mut keys: Vec<&String> = object.keys().collect();
        keys.sort();
        return Err(invalid(format!("{keys:?} is not a tagged value")));
    }
    let inner = &object["v"];
    Ok(match object["$"].as_str() {
        Some("float") => match text(inner, "a float")?.as_str() {
            "inf" => RowValue::Float(f64::INFINITY),
            "-inf" => RowValue::Float(f64::NEG_INFINITY),
            "nan" => RowValue::Float(f64::NAN),
            other => RowValue::Float(
                other
                    .parse()
                    .map_err(|_| invalid(format!("float {other:?}")))?,
            ),
        },
        Some("uuid") => {
            let s = text(inner, "a uuid")?;
            RowValue::Uuid(Uuid::parse_str(&s).map_err(|_| invalid(format!("uuid {s:?}")))?)
        }
        Some("decimal") => RowValue::Decimal(text(inner, "a decimal")?),
        Some("bytes") => RowValue::Bytes(
            base64::engine::general_purpose::STANDARD
                .decode(text(inner, "bytes")?)
                .map_err(|e| invalid(format!("bytes: {e}")))?,
        ),
        Some("date") => {
            let s = text(inner, "a date")?;
            if !is_date(&s) {
                return Err(invalid(format!("date {s:?}")));
            }
            RowValue::Date(s)
        }
        Some("datetime") => {
            let s = text(inner, "a datetime")?;
            if !(s.len() >= 16 && is_date(&s[..10]) && s.as_bytes()[10] == b'T') {
                return Err(invalid(format!("datetime {s:?}")));
            }
            RowValue::DateTime(s)
        }
        Some("map") => {
            let pairs = inner
                .as_array()
                .ok_or_else(|| invalid("a map is not a list"))?;
            RowValue::Map(
                pairs
                    .iter()
                    .map(|pair| match pair.as_array().map(Vec::as_slice) {
                        Some([k, v]) => Ok((text(k, "a map key")?, decode(v)?)),
                        _ => Err(invalid("a map entry is not a [key, value] pair")),
                    })
                    .collect::<Result<_, _>>()?,
            )
        }
        Some(other) => return Err(invalid(format!("unknown tag {other:?}"))),
        None => return Err(invalid("a tag is not a string")),
    })
}

fn decode(value: &Json) -> Result<RowValue, RowsError> {
    Ok(match value {
        Json::Null => RowValue::Null,
        Json::Bool(v) => RowValue::Bool(*v),
        Json::Number(n) => match (n.as_i64(), n.as_f64()) {
            (Some(i), _) if !n.is_f64() => RowValue::Int(i),
            (_, Some(f)) if n.is_f64() => RowValue::Float(f),
            _ => return Err(invalid(format!("integer {n} does not fit 64 bits"))),
        },
        Json::String(v) => RowValue::Str(v.clone()),
        Json::Array(v) => RowValue::List(v.iter().map(decode).collect::<Result<_, _>>()?),
        Json::Object(o) => decode_tagged(o)?,
    })
}

/// The batches in a journal payload.
///
/// # Errors
///
/// [`RowsError`] for another version, malformed JSON, an unknown tag or a
/// plain object.
pub fn decode_batches(raw: &[u8]) -> Result<Vec<Batch>, RowsError> {
    let doc: Json = serde_json::from_slice(raw).map_err(invalid)?;
    match doc.get("v").and_then(Json::as_i64) {
        Some(VERSION) => {}
        other => {
            return Err(RowsError(format!(
                "unsupported row payload version {other:?}"
            )));
        }
    }
    let batches = doc
        .get("batches")
        .and_then(Json::as_array)
        .ok_or_else(|| invalid("no batches"))?;
    batches
        .iter()
        .map(|b| {
            let columns: Vec<String> = b
                .get("columns")
                .and_then(Json::as_array)
                .ok_or_else(|| invalid("no columns"))?
                .iter()
                .map(|c| text(c, "a column name"))
                .collect::<Result<_, _>>()?;
            let rows = b
                .get("rows")
                .and_then(Json::as_array)
                .ok_or_else(|| invalid("no rows"))?
                .iter()
                .map(|r| {
                    let values = r.as_array().ok_or_else(|| invalid("a row is not a list"))?;
                    values.iter().map(decode).collect::<Result<Vec<_>, _>>()
                })
                .collect::<Result<_, _>>()?;
            Ok(Batch {
                table: text(b.get("table").unwrap_or(&Json::Null), "the table")?,
                columns,
                rows,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_must_match_their_columns() {
        let batch = Batch {
            table: "t".into(),
            columns: vec!["a".into()],
            rows: vec![vec![]],
        };
        let err = encode_batches(&[batch]).unwrap_err();
        assert!(err.to_string().contains("0 values for 1 columns"), "{err}");
    }

    #[test]
    fn nan_survives_as_a_tag() {
        let batch = Batch {
            table: "t".into(),
            columns: vec!["x".into()],
            rows: vec![vec![RowValue::Float(f64::NAN)]],
        };
        let raw = encode_batches(&[batch]).unwrap();
        assert!(
            String::from_utf8(raw.clone())
                .unwrap()
                .contains(r#"{"$":"float","v":"nan"}"#)
        );
        let back = decode_batches(&raw).unwrap();
        assert!(matches!(back[0].rows[0][0], RowValue::Float(f) if f.is_nan()));
    }

    #[test]
    fn malformed_values_are_rejected() {
        for bad in [
            r#"{"v":1,"batches":[{"table":"t","columns":["a"],"rows":[[{"$":"date","v":"12/10/2026"}]]}]}"#,
            r#"{"v":1,"batches":[{"table":"t","columns":["a"],"rows":[[{"$":"uuid","v":"x"}]]}]}"#,
            r#"{"v":1,"batches":[{"table":"t","columns":["a"],"rows":[[18446744073709551615]]}]}"#,
            r#"{"v":1}"#,
            "not json",
        ] {
            assert!(decode_batches(bad.as_bytes()).is_err(), "{bad}");
        }
    }
}
