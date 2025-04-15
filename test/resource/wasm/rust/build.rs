// The default stack size in Rust is 1MB, which causes oversized allocation warnings,
// because it's allocated in a single chunk as a part of a Wasm Linear Memory.
// We change the stack size to 128KB using the RUSTFLAGS environment variable
// in the command below.
fn main() {
    println!("cargo:rustc-link-arg=-zstack-size=131072");
    println!("cargo:rerun-if-changed=build.rs");
}
