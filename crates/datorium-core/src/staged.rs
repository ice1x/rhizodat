//! Staged data files as journal payloads (the port of `datorium.core.staged`).
//!
//! A one-phase branch over files (Iceberg) writes its data files before
//! prepare and commits them only after the decision, possibly by recovery on
//! another host, so the journal carries what was staged:
//!
//! ```json
//! {"v": 1, "tables": [{"table": "erp.sales", "files": ["s3://…"], "data_files": [{…}],
//!                      "erase": ["0199…"]}]}
//! ```
//!
//! `data_files` is optional: the Rust adapter adds one Iceberg `DataFile`
//! JSON object per file (column statistics kept), the Python adapter writes
//! paths only, and a reader without the objects derives each `DataFile` from
//! the Parquet footer. `erase` is optional too: entity parts erased by the
//! branch (a hard delete), whose versions older than the transaction are
//! deleted before the files are added. Entries of one table are merged in order of first
//! appearance. The format is pinned by `conformance/staged_files.json`.

use std::fmt;

use serde_json::{Map, Value as Json, json};

const VERSION: i64 = 1;

/// The files staged for one table, with their `DataFile` objects if known.
#[derive(Clone, Debug, PartialEq)]
pub struct StagedTable {
    /// The table, as the catalog names it (`namespace.table`).
    pub table: String,
    /// Paths of the staged data files.
    pub files: Vec<String>,
    /// One Iceberg `DataFile` JSON object per file, in the same order.
    pub data_files: Option<Vec<Map<String, Json>>>,
    /// `dat_id` of the entity parts erased from the table.
    pub erase: Vec<String>,
}

/// A payload that is not a staged files payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagedError(String);

impl fmt::Display for StagedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for StagedError {}

fn invalid(message: impl fmt::Display) -> StagedError {
    StagedError(format!("invalid staged files payload: {message}"))
}

/// The journal payload for `tables`.
#[must_use]
pub fn encode_staged(tables: &[StagedTable]) -> Vec<u8> {
    let entries: Vec<Json> = tables
        .iter()
        .map(|t| {
            let mut entry = json!({"table": t.table, "files": t.files});
            if let Some(data_files) = &t.data_files {
                entry["data_files"] =
                    Json::Array(data_files.iter().map(|d| Json::Object(d.clone())).collect());
            }
            if !t.erase.is_empty() {
                entry["erase"] = json!(t.erase);
            }
            entry
        })
        .collect();
    json!({"v": VERSION, "tables": entries})
        .to_string()
        .into_bytes()
}

fn entry(raw: &Json) -> Result<StagedTable, StagedError> {
    let table = raw
        .get("table")
        .and_then(Json::as_str)
        .ok_or_else(|| invalid("an entry has no table name"))?;
    let files: Vec<String> = raw
        .get("files")
        .and_then(Json::as_array)
        .and_then(|fs| fs.iter().map(|f| f.as_str().map(str::to_owned)).collect())
        .ok_or_else(|| invalid(format!("table {table:?}: files is not a list of paths")))?;
    let data_files = match raw.get("data_files") {
        None | Some(Json::Null) => None,
        Some(value) => {
            let objects: Vec<Map<String, Json>> = value
                .as_array()
                .and_then(|ds| ds.iter().map(|d| d.as_object().cloned()).collect())
                .ok_or_else(|| {
                    invalid(format!(
                        "table {table:?}: data_files is not a list of objects"
                    ))
                })?;
            if objects.len() != files.len() {
                return Err(invalid(format!(
                    "table {table:?}: {} data_files for {} files",
                    objects.len(),
                    files.len()
                )));
            }
            Some(objects)
        }
    };
    let erase: Vec<String> = match raw.get("erase") {
        None | Some(Json::Null) => Vec::new(),
        Some(value) => value
            .as_array()
            .and_then(|ids| ids.iter().map(|i| i.as_str().map(str::to_owned)).collect())
            .ok_or_else(|| invalid(format!("table {table:?}: erase is not a list of ids")))?,
    };
    Ok(StagedTable {
        table: table.to_owned(),
        files,
        data_files,
        erase,
    })
}

/// The tables in a journal payload; no payload (or an empty one) is none.
///
/// # Errors
///
/// [`StagedError`] for another version or a malformed payload.
pub fn decode_staged(raw: Option<&[u8]>) -> Result<Vec<StagedTable>, StagedError> {
    let Some(raw) = raw.filter(|r| !r.is_empty()) else {
        return Ok(Vec::new());
    };
    let doc: Json = serde_json::from_slice(raw).map_err(invalid)?;
    if !doc.is_object() {
        return Err(invalid("not an object"));
    }
    match doc.get("v").and_then(Json::as_i64) {
        Some(VERSION) => {}
        other => {
            return Err(StagedError(format!(
                "unsupported staged files payload version {other:?}"
            )));
        }
    }
    let entries = doc
        .get("tables")
        .and_then(Json::as_array)
        .ok_or_else(|| invalid("no tables"))?;
    let mut merged: Vec<StagedTable> = Vec::new();
    for e in entries {
        let e = entry(e)?;
        match merged.iter_mut().find(|m| m.table == e.table) {
            None => merged.push(e),
            Some(seen) => {
                seen.files.extend(e.files);
                seen.erase.extend(e.erase);
                seen.data_files = match (seen.data_files.take(), e.data_files) {
                    (Some(mut a), Some(b)) => {
                        a.extend(b);
                        Some(a)
                    }
                    _ => None,
                };
            }
        }
    }
    Ok(merged)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_data_files_key_without_objects() {
        let raw = encode_staged(&[StagedTable {
            table: "erp.sales".into(),
            files: vec!["f".into()],
            data_files: None,
            erase: Vec::new(),
        }]);
        let doc: Json = serde_json::from_slice(&raw).unwrap();
        assert_eq!(
            doc,
            json!({"v": 1, "tables": [{"table": "erp.sales", "files": ["f"]}]})
        );
    }
}
