/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
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
