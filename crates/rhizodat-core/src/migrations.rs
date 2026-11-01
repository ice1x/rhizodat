//! Schema migration files (the port of `rhizodat.migrations.format`).
//!
//! `NNNN_name.toml` (the number is the id, at least 1; the name is lower-case
//! letters, digits and `_`) holds an optional `description` and a list of
//! `steps`. A step names the store it runs on and has exactly one body, keyed
//! by its kind (`sql`, `cypher`, `iceberg`, or a plug-in adapter's own):
//!
//! ```toml
//! [[steps]]
//! store = "pg"
//! sql = "CREATE TABLE IF NOT EXISTS bug (id uuid PRIMARY KEY)"
//! ```
//!
//! A body is strings, integers, booleans, arrays and tables; floats and dates
//! have no single text form across languages and are refused. The checksum
//! the journal records is the sha256 (lower-case hex) of [`canonical`]. The
//! format is pinned by `conformance/migrations.json`.

use std::collections::BTreeMap;
use std::fmt::{self, Write as _};

use serde_json::Value as Json;
use sha2::{Digest, Sha256};

/// The body of a step, as the file gives it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Body {
    /// A string.
    Text(String),
    /// A 64-bit integer.
    Int(i64),
    /// A boolean.
    Bool(bool),
    /// An array.
    List(Vec<Body>),
    /// A table; keys in byte (code point) order.
    Table(BTreeMap<String, Body>),
}

impl Body {
    /// The body as JSON.
    #[must_use]
    pub fn to_json(&self) -> Json {
        match self {
            Body::Text(s) => Json::from(s.as_str()),
            Body::Int(i) => Json::from(*i),
            Body::Bool(b) => Json::from(*b),
            Body::List(items) => Json::Array(items.iter().map(Body::to_json).collect()),
            Body::Table(map) => {
                Json::Object(map.iter().map(|(k, v)| (k.clone(), v.to_json())).collect())
            }
        }
    }
}

/// One step: the store it runs on, its kind and its body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Step {
    /// A store name of the configuration.
    pub store: String,
    /// The kind, which decides which store can run it.
    pub kind: String,
    /// What the store runs.
    pub body: Body,
}

/// One migration file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Migration {
    /// The number from the file name.
    pub id: u64,
    /// The name from the file name.
    pub name: String,
    /// The optional description (not part of the checksum).
    pub description: Option<String>,
    /// The steps, in order.
    pub steps: Vec<Step>,
    /// sha256 of [`canonical`], lower-case hex.
    pub checksum: String,
}

/// A file that does not follow the format.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationFormatError(String);

impl fmt::Display for MigrationFormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for MigrationFormatError {}

fn fail<T>(filename: &str, message: impl fmt::Display) -> Result<T, MigrationFormatError> {
    Err(MigrationFormatError(format!("{filename}: {message}")))
}

fn is_name(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

fn is_store(s: &str) -> bool {
    (1..=64).contains(&s.len())
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
}

fn is_kind(s: &str) -> bool {
    let mut bytes = s.bytes();
    bytes.next().is_some_and(|b| b.is_ascii_lowercase())
        && s.len() <= 32
        && bytes.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

/// The id and name of a file `NNNN_name.toml`.
fn split_filename(filename: &str) -> Option<(u64, &str)> {
    let stem = filename.strip_suffix(".toml")?;
    let (digits, name) = stem.split_once('_')?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) || !is_name(name) {
        return None;
    }
    Some((digits.parse().ok()?, name))
}

fn body(filename: &str, step: usize, value: &toml::Value) -> Result<Body, MigrationFormatError> {
    Ok(match value {
        toml::Value::String(s) => Body::Text(s.clone()),
        toml::Value::Integer(i) => Body::Int(*i),
        toml::Value::Boolean(b) => Body::Bool(*b),
        toml::Value::Float(_) => {
            return fail(
                filename,
                format!("step {step}: floats have no portable form"),
            );
        }
        toml::Value::Datetime(_) => {
            return fail(
                filename,
                format!("step {step}: dates and times are not allowed"),
            );
        }
        toml::Value::Array(items) => Body::List(
            items
                .iter()
                .map(|v| body(filename, step, v))
                .collect::<Result<_, _>>()?,
        ),
        toml::Value::Table(map) => Body::Table(
            map.iter()
                .map(|(k, v)| Ok((k.clone(), body(filename, step, v)?)))
                .collect::<Result<_, _>>()?,
        ),
    })
}

/// The migration in `text`, read from a file named `filename`.
///
/// # Errors
///
/// [`MigrationFormatError`] naming the file when it does not follow the format.
pub fn parse_migration(filename: &str, text: &str) -> Result<Migration, MigrationFormatError> {
    let Some((id, name)) = split_filename(filename) else {
        return fail(
            filename,
            "a migration file is named NNNN_name.toml (a number, then lower-case letters, digits and _)",
        );
    };
    if id < 1 {
        return fail(filename, "the id must be at least 1");
    }
    let doc: toml::Table = match text.parse() {
        Ok(doc) => doc,
        Err(e) => return fail(filename, format!("not valid TOML: {e}")),
    };
    let unknown: Vec<&str> = doc
        .keys()
        .map(String::as_str)
        .filter(|k| !matches!(*k, "description" | "steps"))
        .collect();
    if !unknown.is_empty() {
        return fail(filename, format!("unknown key {}", unknown.join(", ")));
    }
    let description = match doc.get("description") {
        None => None,
        Some(toml::Value::String(s)) => Some(s.clone()),
        Some(_) => return fail(filename, "description must be a string"),
    };
    let raw = match doc.get("steps") {
        None => return fail(filename, "no steps"),
        Some(toml::Value::Array(items)) if items.is_empty() => return fail(filename, "no steps"),
        Some(toml::Value::Array(items)) => items,
        Some(_) => return fail(filename, "steps must be an array of tables ([[steps]])"),
    };
    let mut steps = Vec::with_capacity(raw.len());
    for (i, entry) in raw.iter().enumerate() {
        let n = i + 1;
        let Some(entry) = entry.as_table() else {
            return fail(filename, "steps must be an array of tables ([[steps]])");
        };
        let bodies: Vec<(&String, &toml::Value)> = entry
            .iter()
            .filter(|(k, _)| k.as_str() != "store")
            .collect();
        let [(kind, value)] = bodies[..] else {
            return fail(
                filename,
                format!("step {n} needs exactly one body (sql, cypher, iceberg, ...)"),
            );
        };
        let Some(toml::Value::String(store)) = entry.get("store") else {
            return fail(filename, format!("step {n} needs a store (a string)"));
        };
        if !is_store(store) {
            return fail(filename, format!("step {n}: invalid store name {store:?}"));
        }
        if !is_kind(kind) {
            return fail(filename, format!("step {n}: invalid step kind {kind:?}"));
        }
        steps.push(Step {
            store: store.clone(),
            kind: kind.clone(),
            body: body(filename, n, value)?,
        });
    }
    let mut migration = Migration {
        id,
        name: name.to_owned(),
        description,
        steps,
        checksum: String::new(),
    };
    let digest = Sha256::digest(canonical(&migration).as_bytes());
    migration.checksum = digest.iter().fold(String::with_capacity(64), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    });
    Ok(migration)
}

fn write_text(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            c if u32::from(c) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", u32::from(c));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

fn write_body(out: &mut String, body: &Body) {
    match body {
        Body::Text(s) => write_text(out, s),
        Body::Int(i) => {
            let _ = write!(out, "{i}");
        }
        Body::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Body::List(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_body(out, item);
            }
            out.push(']');
        }
        Body::Table(map) => {
            out.push('{');
            for (i, (k, v)) in map.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_text(out, k);
                out.push(':');
                write_body(out, v);
            }
            out.push('}');
        }
    }
}

/// One applied migration as a journal's history records it (the Python
/// reference's `AppliedMigration`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppliedMigration {
    /// The migration's id.
    pub id: i64,
    /// Its name.
    pub name: String,
    /// The checksum of its canonical form when it was applied.
    pub checksum: String,
    /// The HLC timestamp of the record: `as_of` reads compare against it.
    pub dat_tx_time: i64,
}

/// The text the checksum is taken of: the JSON of
/// `{"id", "name", "steps": [{"body", "kind", "store"}]}` with keys sorted, no
/// whitespace, text as UTF-8 with only `\"`, `\\`, `\b`, `\f`, `\n`, `\r`, `\t`
/// and `\u00XX` (other control characters) escaped.
#[must_use]
pub fn canonical(migration: &Migration) -> String {
    let mut out = format!("{{\"id\":{},\"name\":", migration.id);
    write_text(&mut out, &migration.name);
    out.push_str(",\"steps\":[");
    for (i, step) in migration.steps.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str("{\"body\":");
        write_body(&mut out, &step.body);
        out.push_str(",\"kind\":");
        write_text(&mut out, &step.kind);
        out.push_str(",\"store\":");
        write_text(&mut out, &step.store);
        out.push('}');
    }
    out.push_str("]}");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_file_names_its_id_and_holds_steps() {
        let m = parse_migration(
            "0007_seed.toml",
            "[[steps]]\nstore = \"pg\"\nsql = [\"CREATE TABLE IF NOT EXISTS a (id int)\"]\n",
        )
        .unwrap();
        assert_eq!(
            (m.id, m.name.as_str(), m.description.as_deref()),
            (7, "seed", None)
        );
        assert_eq!(m.steps[0].kind, "sql");
        assert_eq!(
            canonical(&m),
            r#"{"id":7,"name":"seed","steps":[{"body":["CREATE TABLE IF NOT EXISTS a (id int)"],"kind":"sql","store":"pg"}]}"#
        );
        assert_eq!(m.checksum.len(), 64);
    }

    #[test]
    fn floats_and_two_bodies_are_refused_with_the_file_name() {
        for (text, what) in [
            ("[[steps]]\nstore = \"pg\"\nsql = 1.5\n", "floats"),
            (
                "[[steps]]\nstore = \"pg\"\nsql = \"x\"\ncypher = \"y\"\n",
                "exactly one body",
            ),
        ] {
            let err = parse_migration("0001_a.toml", text)
                .unwrap_err()
                .to_string();
            assert!(
                err.starts_with("0001_a.toml: ") && err.contains(what),
                "{err}"
            );
        }
    }
}
