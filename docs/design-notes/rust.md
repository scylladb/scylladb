# Rust and C++

Rust introduces many useful features that are missing in C++. This document
shows how to use them in Scylla.

## Using Rust in Scylla

To create a Rust package `new_pkg` and use it in a C++ source file:
1. Create a new package in the `rust` directory using `cargo new new_pkg --lib`
2. Modify crate type to a static library by adding
```
[lib]
crate-type = ["staticlib"]
```
to `new_pkg/Cargo.toml`
3. Add `"new_pkg",` to the workspace members list in `Cargo.toml`
4. Write your Rust code in `new_pkg/src/lib.rs`
5. To export a function `fn foo(x: i32) -> i32`, add its declaration as follows to the same file
```
#[cxx::bridge(namespace = "rust")]
mod ffi {
    extern "Rust" {
        fn inc(x: i32) -> i32;
    }
}
```
6. Add `"rust/new_pkg/src/lib.rs"` to the `rusts` list in `configure.py`
7. Include the `rust/new_pkg.hh` header and use `rust::foo()` in the selected c++ file.

## Rust interoperability implementation

Using Rust alongside C++ in scylla is made possible by the Rust crate CXX. The `cxx::bridge` macro,
together with `mod ffi` and `extern "Rust"` mark items to be exported to C++. During compilation,
a compatible version is generated with temporary names. It can be later linked with a header file, generated from the source code using `cxxbridge` command. The header exposed all items with original names and can be included like any other c++ header.

Compilation is managed by `cargo`. Like in any Rust project, modules added to Scylla can be fully
customized using corresponding `Cargo.toml` files. Currently, all modules are compiled to static
libraries (TODO: dynamic or flexible)
