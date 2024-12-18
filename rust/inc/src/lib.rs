/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#[cxx::bridge(namespace = "rust")]
mod ffi {
    extern "Rust" {
        fn inc(x: i32) -> i32;
    }
}
fn inc(x: i32) -> i32 {
    x + 1
}
