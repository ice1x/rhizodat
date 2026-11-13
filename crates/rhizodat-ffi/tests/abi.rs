//! The C ABI as a C caller sees it: status codes, strings the library owns
//! until `rhizodat_string_free`, and errors as text.

use std::collections::HashSet;
use std::ffi::{CStr, c_char};
use std::ptr;

use rhizodat_ffi::{RhizodatStatus, rhizodat_new_id, rhizodat_string_free, rhizodat_version};
use uuid::Uuid;

/// Takes a string the library returned and frees it.
fn take(raw: *mut c_char) -> String {
    assert!(!raw.is_null());
    // SAFETY: `raw` came from the library and is freed exactly once, below.
    let text = unsafe { CStr::from_ptr(raw) }.to_str().unwrap().to_owned();
    // SAFETY: as above.
    unsafe { rhizodat_string_free(raw) };
    text
}

fn new_id() -> Uuid {
    let mut out: *mut c_char = ptr::null_mut();
    let mut error: *mut c_char = ptr::null_mut();
    // SAFETY: both out-pointers point to writable locals.
    let status = unsafe { rhizodat_new_id(&raw mut out, &raw mut error) };
    assert_eq!(status, RhizodatStatus::Ok);
    assert!(error.is_null());
    take(out).parse().unwrap()
}

#[test]
fn the_version_is_the_crate_version() {
    // SAFETY: the version is a static string; it is not freed.
    let version = unsafe { CStr::from_ptr(rhizodat_version()) };
    assert_eq!(version.to_str().unwrap(), env!("CARGO_PKG_VERSION"));
}

#[test]
fn a_new_id_is_a_uuid7() {
    let id = new_id();
    assert_eq!(id.get_version_num(), 7);
    assert_eq!(id.get_variant(), uuid::Variant::RFC4122);
}

#[test]
fn ids_from_one_process_are_unique_and_ordered() {
    let ids: Vec<Uuid> = (0..2000).map(|_| new_id()).collect();
    assert!(ids.windows(2).all(|w| w[0] < w[1]));
    assert_eq!(ids.iter().collect::<HashSet<_>>().len(), ids.len());
}

#[test]
fn ids_from_many_threads_are_unique() {
    let threads: Vec<_> = (0..8)
        .map(|_| std::thread::spawn(|| (0..500).map(|_| new_id()).collect::<Vec<_>>()))
        .collect();
    let all: Vec<Uuid> = threads
        .into_iter()
        .flat_map(|t| t.join().unwrap())
        .collect();
    assert_eq!(all.iter().collect::<HashSet<_>>().len(), 4000);
}

#[test]
fn a_null_out_pointer_is_an_invalid_argument_with_its_reason() {
    let mut error: *mut c_char = ptr::null_mut();
    // SAFETY: a null `out` is what is under test; `error` is writable.
    let status = unsafe { rhizodat_new_id(ptr::null_mut(), &raw mut error) };
    assert_eq!(status, RhizodatStatus::InvalidArgument);
    assert!(take(error).contains("out"));
}

#[test]
fn a_null_error_pointer_still_reports_the_status() {
    // SAFETY: both null: the status alone tells the caller.
    let status = unsafe { rhizodat_new_id(ptr::null_mut(), ptr::null_mut()) };
    assert_eq!(status, RhizodatStatus::InvalidArgument);
}

#[test]
fn freeing_null_does_nothing() {
    // SAFETY: null is allowed, as with free(3).
    unsafe { rhizodat_string_free(ptr::null_mut()) };
}

#[test]
fn the_checked_in_header_is_current() {
    let crate_dir = env!("CARGO_MANIFEST_DIR");
    let config = cbindgen::Config::from_file(format!("{crate_dir}/cbindgen.toml")).unwrap();
    let mut generated = Vec::new();
    cbindgen::generate_with_config(crate_dir, config)
        .unwrap()
        .write(&mut generated);
    let path = format!("{crate_dir}/include/rhizodat.h");
    let checked_in = std::fs::read(&path).unwrap_or_default();
    if checked_in != generated && std::env::var_os("RHIZODAT_BLESS").is_some() {
        std::fs::create_dir_all(format!("{crate_dir}/include")).unwrap();
        std::fs::write(&path, &generated).unwrap();
        return;
    }
    assert!(
        checked_in == generated,
        "include/rhizodat.h is stale: RHIZODAT_BLESS=1 cargo test -p rhizodat-ffi"
    );
}
