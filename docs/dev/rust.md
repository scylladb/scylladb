# Rust and C++

Rust introduces many useful features that are missing in C++. This document
shows how to use them in Scylla.

## Using Rust in Scylla

To create a Rust package `new_pkg` and use it in a C++ source file:
1. Create a new package in the `rust` directory using `cargo new new_pkg --lib`
2. Add `new_pkg = { path = "new_pkg", version = "0.1.0" }` to the dependencies list in `rust/Cargo.toml`
3. Add `extern crate new_pkg;` to `rust/src/lib.rs`
4. Configure your package in `new_pkg/Cargo.toml` and write your Rust code in `new_pkg/src/lib.rs` (and other `new_pkg/src/*.rs` files)
5. To export a function `fn inc(x: i32) -> i32` in the namespace `xyz`, add its declaration to `new_pkg/src/lib.rs` as follows:
```
#[cxx::bridge(namespace = "xyz")]
mod ffi {
    extern "Rust" {
        fn inc(x: i32) -> i32;
    }
}
```
6. Add `new_pkg/src/lib.rs` to the `configure.py` dependencies where you'll need the Rust exports
7. Include the `rust/new_pkg.hh` header and use `xyz::foo()` in the selected c++ file.

#### cxx::bridge placement

You can put the `cxx::bridge` segment into some file other than `lib.rs` as well, for example `abc.rs`. If you do that, remember to add `mod abc` to `lib.rs` to make sure that the bridge is compiled with the entire package.
Additionally, the definitions of exported functions must be visible in `abc.rs`. You can achieve this writing them in the same file or using `mod` and `use` statements.
Then, use this file instead of `lib.rs` in the `configure.py` dependencies.

## Submitting changes

Additionally to the source code, Scylla tracks the Cargo.lock file that contains precise information about dependency
versions used in the last successful build. Dependency modification may include:
 * adding a new local package (it is used in Scylla as a dependency)
 * updating a dependency version in an existing package
 * adding a new dependency to a package

After each such modification, a new Cargo.lock file should be submitted. Cargo.lock can be generated
by the `cargo update` command.

## Rust interoperability implementation

Using Rust alongside C++ in scylla is made possible by the Rust crate CXX. The `cxx::bridge` macro,
together with `mod ffi` and `extern "Rust"` mark items to be exported to C++. During compilation
of Rust files, a static library using C++ ABI is generated. The library exports Rust methods under
special names. These names are used in the implementations of C++ methods with the original names.
The C++ methods are listed in *.hh files, and their implementations in *.cc files, both generated
from the Rust source code using `cxxbridge` command.
The header exposes all items with original names and can be included like any other C++ header.

Compilation is managed by `cargo`. Like in any Rust project, modules added to Scylla can be fully
customized using corresponding `Cargo.toml` files. All modules are compiled to single static
library, as this is the only officially supported way of linking Rust against C++.
In the future, more linking methods may become supported, possibly using `rlib` ("Rust library") files.
