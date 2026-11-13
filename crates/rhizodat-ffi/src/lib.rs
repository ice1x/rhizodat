//! `librhizodat_ffi`: the C ABI of the rhizodat core, for Go (cgo) and other C callers.
//!
//! The conventions every function follows:
//!
//! - A function returns a [`RhizodatStatus`]; its results go to out-pointers the
//!   caller passes. On anything but [`RhizodatStatus::Ok`] it writes nothing to
//!   them except the error.
//! - An error is text: when the caller passes a non-null `error` out-pointer, a
//!   failing call stores a message there. Pass null to ignore it.
//! - A string the library returns (a result or an error) belongs to the caller,
//!   who frees it with [`rhizodat_string_free`], never with `free(3)`.
//! - A panic never crosses the boundary: it becomes [`RhizodatStatus::Panic`].
//! - A handle (`RhizodatPostgresStore *`, …) is opaque; the caller frees it with
//!   its `*_free` function, once. Handles may be used from several threads.
//! - Values travel as the core's tagged JSON (`rhizodat_core::rows`): JSON
//!   scalars, arrays as lists, and `{"$": tag, "v": value}` for what JSON
//!   cannot carry exactly (`uuid`, `decimal`, `bytes` in base64, `date`,
//!   `datetime`, `map` as `[[key, value], …]`, non-finite `float`).
//! - [`rhizodat_coordinator_commit`] is the one exception to "results only on
//!   OK": it also writes its outcome (with the meta-transaction id) on
//!   [`RhizodatStatus::Aborted`] and [`RhizodatStatus::InDoubt`].
//!
//! The header, `include/rhizodat.h`, is generated from this crate by cbindgen;
//! a test fails when the checked-in copy is stale.

use std::ffi::{CString, c_char};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::OnceLock;

use rhizodat_core::identity::IdGenerator;

/// One id generator per process, so ids from all threads are unique.
pub(crate) static IDS: OnceLock<IdGenerator> = OnceLock::new();

/// What a call did. Results are valid only with `Ok`.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RhizodatStatus {
    /// It worked; the out-pointers hold the results.
    Ok = 0,
    /// An argument the caller passed is unusable (a null pointer, text that is not
    /// UTF-8, a malformed value); nothing was done.
    InvalidArgument = 1,
    /// The core failed; the error says why.
    Failed = 2,
    /// The library panicked: a bug in rhizodat. The error carries the panic's text.
    Panic = 3,
    /// A meta-transaction committed nothing; its outcome says whether a re-run
    /// may succeed (`retryable`).
    Aborted = 4,
    /// A meta-transaction's outcome is unknown: recovery resolves it from the
    /// journal. Do not re-run it, it may have committed.
    InDoubt = 5,
}

mod arrow;
mod clickhouse;
mod drevo;
mod iceberg;
mod postgres;
mod unit;

pub use clickhouse::*;
pub use drevo::*;
pub use iceberg::*;
pub use postgres::*;
pub use unit::*;

/// The library's version, a static string: do not free it.
#[unsafe(no_mangle)]
pub extern "C" fn rhizodat_version() -> *const c_char {
    static VERSION: &std::ffi::CStr = match std::ffi::CStr::from_bytes_with_nul(
        concat!(env!("CARGO_PKG_VERSION"), "\0").as_bytes(),
    ) {
        Ok(v) => v,
        Err(_) => panic!("the version holds a NUL"),
    };
    VERSION.as_ptr()
}

/// A new UUIDv7 as its 36-character text, from one generator per process (ids
/// from all threads are unique; those from one thread are ordered).
///
/// # Safety
///
/// `out` must be null or point to writable memory for one pointer; `error` too.
/// A string stored in either is freed with [`rhizodat_string_free`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_new_id(
    out: *mut *mut c_char,
    error: *mut *mut c_char,
) -> RhizodatStatus {
    // SAFETY: the caller's contract above.
    unsafe {
        boundary(error, || {
            let out = writable(out, "out")?;
            let id = IDS
                .get_or_init(IdGenerator::new)
                .generate()
                .map_err(|e| (RhizodatStatus::Failed, e.to_string()))?;
            *out = owned_string(id.to_string());
            Ok(())
        })
    }
}

/// Frees a string the library returned. Null is allowed and does nothing.
///
/// # Safety
///
/// `value` must be null or a string from this library not freed before.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rhizodat_string_free(value: *mut c_char) {
    if !value.is_null() {
        // SAFETY: made by `CString::into_raw` in `owned_string`, freed once.
        drop(unsafe { CString::from_raw(value) });
    }
}

/// A failed call: its status and message.
pub(crate) type Failure = (RhizodatStatus, String);

type Outcome = Result<(), Failure>;

/// Runs `body`, turning its error or a panic into a status and, when the caller
/// asked for one, an error string.
///
/// # Safety
///
/// `error` must be null or writable for one pointer.
pub(crate) unsafe fn boundary(
    error: *mut *mut c_char,
    body: impl FnOnce() -> Outcome,
) -> RhizodatStatus {
    let (status, message) = match catch_unwind(AssertUnwindSafe(body)) {
        Ok(Ok(())) => return RhizodatStatus::Ok,
        Ok(Err(failure)) => failure,
        Err(panic) => (RhizodatStatus::Panic, panic_text(panic.as_ref())),
    };
    if !error.is_null() {
        // SAFETY: the caller's contract.
        unsafe { *error = owned_string(message) };
    }
    status
}

/// `ptr` as a place to write, or an invalid-argument error naming it.
///
/// # Safety
///
/// `ptr` must be null or writable for one `T`.
pub(crate) unsafe fn writable<'a, T>(
    ptr: *mut T,
    name: &str,
) -> Result<&'a mut T, (RhizodatStatus, String)> {
    // SAFETY: the caller's contract.
    unsafe { ptr.as_mut() }
        .ok_or_else(|| (RhizodatStatus::InvalidArgument, format!("{name} is null")))
}

/// `text` as a C string the caller owns; an interior NUL is replaced, so
/// nothing is lost silently and nothing panics.
pub(crate) fn owned_string(text: String) -> *mut c_char {
    let text = if text.contains('\0') {
        text.replace('\0', "\u{fffd}")
    } else {
        text
    };
    CString::new(text).unwrap_or_default().into_raw()
}

/// `ptr` as UTF-8 text, or an invalid-argument error naming it.
///
/// # Safety
///
/// `ptr` must be null or a NUL-terminated string that outlives the call.
pub(crate) unsafe fn text<'a>(ptr: *const c_char, name: &str) -> Result<&'a str, Failure> {
    // SAFETY: the caller's contract.
    unsafe { optional_text(ptr, name) }?
        .ok_or_else(|| (RhizodatStatus::InvalidArgument, format!("{name} is null")))
}

/// Like [`text`], with null as `None`.
///
/// # Safety
///
/// As [`text`].
pub(crate) unsafe fn optional_text<'a>(
    ptr: *const c_char,
    name: &str,
) -> Result<Option<&'a str>, Failure> {
    if ptr.is_null() {
        return Ok(None);
    }
    // SAFETY: the caller's contract.
    unsafe { std::ffi::CStr::from_ptr(ptr) }
        .to_str()
        .map(Some)
        .map_err(|_| {
            (
                RhizodatStatus::InvalidArgument,
                format!("{name} is not UTF-8"),
            )
        })
}

/// A handle as a shared reference, or an invalid-argument error naming it.
///
/// # Safety
///
/// `ptr` must be null or a live handle of this type.
pub(crate) unsafe fn handle<'a, T>(ptr: *const T, name: &str) -> Result<&'a T, Failure> {
    // SAFETY: the caller's contract.
    unsafe { ptr.as_ref() }
        .ok_or_else(|| (RhizodatStatus::InvalidArgument, format!("{name} is null")))
}

/// A boxed handle for the caller, who frees it with its `*_free` function.
pub(crate) fn into_handle<T>(value: T) -> *mut T {
    Box::into_raw(Box::new(value))
}

/// Frees a handle made by [`into_handle`]; null does nothing.
///
/// # Safety
///
/// `ptr` must be null or a handle of this type not freed before.
pub(crate) unsafe fn free_handle<T>(ptr: *mut T) {
    if !ptr.is_null() {
        // SAFETY: made by `Box::into_raw` in `into_handle`, freed once.
        drop(unsafe { Box::from_raw(ptr) });
    }
}

/// A list of values from tagged JSON text, or an invalid-argument error.
pub(crate) fn values(
    json: &str,
    name: &str,
) -> Result<Vec<rhizodat_core::rows::RowValue>, Failure> {
    match rhizodat_core::rows::decode_value(json) {
        Ok(rhizodat_core::rows::RowValue::List(values)) => Ok(values),
        Ok(_) => Err((
            RhizodatStatus::InvalidArgument,
            format!("{name} must be a JSON array"),
        )),
        Err(e) => Err((RhizodatStatus::InvalidArgument, format!("{name}: {e}"))),
    }
}

/// A statement's result as `{"columns": [...], "rows": [[value, ...], ...]}`,
/// values tagged.
pub(crate) fn rows_json(
    columns: &[String],
    rows: Vec<Vec<rhizodat_core::rows::RowValue>>,
) -> Result<String, Failure> {
    let columns =
        serde_json::to_string(columns).map_err(|e| (RhizodatStatus::Failed, e.to_string()))?;
    let rows = rhizodat_core::rows::encode_value(&rhizodat_core::rows::RowValue::List(
        rows.into_iter()
            .map(rhizodat_core::rows::RowValue::List)
            .collect(),
    ));
    Ok(format!(r#"{{"columns":{columns},"rows":{rows}}}"#))
}

/// A runtime for one call, or a failure.
pub(crate) fn runtime() -> Result<tokio::runtime::Runtime, Failure> {
    rhizodat_units::runtime().map_err(|e| (RhizodatStatus::Failed, e.to_string()))
}

fn panic_text(panic: &(dyn std::any::Any + Send)) -> String {
    let what = panic
        .downcast_ref::<&str>()
        .map(ToString::to_string)
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "a panic with no message".to_owned());
    format!("rhizodat panicked (a bug, please report it): {what}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_panic_becomes_a_status_and_its_text() {
        let mut error: *mut c_char = std::ptr::null_mut();
        // SAFETY: `error` is a writable local.
        let status = unsafe { boundary(&raw mut error, || panic!("boom")) };
        assert_eq!(status, RhizodatStatus::Panic);
        // SAFETY: set by `boundary`, freed once.
        let text = unsafe { CString::from_raw(error) }.into_string().unwrap();
        assert!(text.contains("boom") && text.contains("a bug"));
    }

    #[test]
    fn a_nul_inside_a_message_is_kept_visible() {
        let raw = owned_string("a\0b".to_owned());
        // SAFETY: made just above, freed once.
        let back = unsafe { CString::from_raw(raw) }.into_string().unwrap();
        assert_eq!(back, "a\u{fffd}b");
    }
}
