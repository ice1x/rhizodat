//! On macOS a Python extension leaves the interpreter's symbols undefined.

fn main() {
    pyo3_build_config::add_extension_module_link_args();
}
