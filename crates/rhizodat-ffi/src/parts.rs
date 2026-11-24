//! Entity parts through the C ABI: what the stores' part functions share.
//!
//! A part is one store's share of one entity type (see `rhizodat.core.parts`):
//! every version of it is kept, tagged with its meta-transaction. A caller
//! describes the part with a spec as JSON, `{"type": "tracker.Issue", "table":
//! "tracker_issue", "columns": [["title", "text"], ...]}`, the fields in order
//! with the store's types; the layout is the Python reference's, so either
//! facade reads the other's versions.
//!
//! A version comes back as JSON, `{"dat_txn": "…", "dat_tx_time": 123,
//! "created": 123, "deleted": false, "values": [...]}`, the values tagged and in
//! the spec's field order; `dat_tx_time` is null for a unit of work's own,
//! unstaged version (which has no `dat_txn` either).

use std::ffi::c_char;

use rhizodat_core::rows::{RowValue, encode_value};
use serde::Deserialize;
use uuid::Uuid;

use crate::{Failure, RhizodatStatus, text, values};

fn invalid(message: impl Into<String>) -> Failure {
    (RhizodatStatus::InvalidArgument, message.into())
}

/// A spec as the caller writes it.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Spec {
    #[serde(rename = "type")]
    pub(crate) type_name: String,
    pub(crate) table: String,
    pub(crate) columns: Vec<(String, String)>,
}

/// `spec_json` as a [`Spec`].
///
/// # Safety
///
/// `spec_json` must be null or a NUL-terminated string.
pub(crate) unsafe fn spec(spec_json: *const c_char) -> Result<Spec, Failure> {
    // SAFETY: the caller's contract.
    let raw = unsafe { text(spec_json, "spec_json") }?;
    serde_json::from_str(raw).map_err(|e| {
        invalid(format!(
            "spec_json must be {{\"type\", \"table\", \"columns\": [[field, type], ...]}}: {e}"
        ))
    })
}

/// The UUID in `ptr`.
///
/// # Safety
///
/// `ptr` must be null or a NUL-terminated string.
pub(crate) unsafe fn uuid_arg(ptr: *const c_char, name: &str) -> Result<Uuid, Failure> {
    // SAFETY: the caller's contract.
    let raw = unsafe { text(ptr, name) }?;
    raw.parse()
        .map_err(|e| invalid(format!("{name} is not a UUID ({raw:?}): {e}")))
}

/// The value `ptr` points to, or `None` for null.
///
/// # Safety
///
/// `ptr` must be null or point to a readable `i64`.
pub(crate) unsafe fn optional_i64(ptr: *const i64) -> Option<i64> {
    // SAFETY: the caller's contract.
    unsafe { ptr.as_ref() }.copied()
}

/// `fields_json` (an array of names) zipped with `values_json` (an array of
/// tagged values, one per field).
///
/// # Safety
///
/// Both must be null or NUL-terminated strings.
pub(crate) unsafe fn field_values(
    fields_json: *const c_char,
    values_json: *const c_char,
) -> Result<Vec<(String, RowValue)>, Failure> {
    // SAFETY: the caller's contract.
    let (fields, row) = unsafe {
        (
            text(fields_json, "fields_json")?,
            text(values_json, "values_json")?,
        )
    };
    let fields: Vec<String> = serde_json::from_str(fields)
        .map_err(|e| invalid(format!("fields_json must be an array of names: {e}")))?;
    let row = values(row, "values_json")?;
    if fields.len() != row.len() {
        return Err(invalid(format!(
            "values_json needs one value per field: {} fields, {} values",
            fields.len(),
            row.len()
        )));
    }
    Ok(fields.into_iter().zip(row).collect())
}

/// One version as JSON (see the module's documentation).
pub(crate) fn version_json(
    dat_txn: Option<Uuid>,
    dat_tx_time: Option<i64>,
    created: Option<i64>,
    deleted: bool,
    values: &[RowValue],
) -> String {
    let txn = dat_txn.map_or_else(|| "null".to_owned(), |t| format!("\"{t}\""));
    let time = |t: Option<i64>| t.map_or_else(|| "null".to_owned(), |t| t.to_string());
    format!(
        r#"{{"dat_txn":{txn},"dat_tx_time":{},"created":{},"deleted":{deleted},"values":{}}}"#,
        time(dat_tx_time),
        time(created),
        encode_value(&RowValue::List(values.to_vec()))
    )
}

/// `version_json` with the entity's id first, for a scan over every entity.
pub(crate) fn entity_version_json(dat_id: Uuid, version: &str) -> String {
    format!(r#"{{"dat_id":"{dat_id}",{}"#, &version[1..])
}

/// A unit of work's own latest version: `{"touched": false}` when it did not
/// touch the part, else `{"touched": true, "version": null | {...}}` (null: it
/// erased it last).
#[allow(clippy::option_option)] // the parts' `latest`: untouched, erased, or a version
pub(crate) fn own_json(latest: Option<Option<String>>) -> String {
    match latest {
        None => r#"{"touched":false}"#.to_owned(),
        Some(version) => format!(
            r#"{{"touched":true,"version":{}}}"#,
            version.as_deref().unwrap_or("null")
        ),
    }
}

/// JSON arrays of already-encoded items.
pub(crate) fn json_array(items: impl IntoIterator<Item = String>) -> String {
    let items: Vec<String> = items.into_iter().collect();
    format!("[{}]", items.join(","))
}
