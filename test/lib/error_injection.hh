/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include "utils/error_injection.hh"

// Waits until enter_count for the named injection reaches the given threshold.
// A replacement for busy-loop polling in unit tests.
inline future<> wait_for_injection_enter(std::string_view injection_name, size_t threshold = 1) {
    while (utils::get_local_injector().enter_count(injection_name) < threshold) {
        co_await coroutine::maybe_yield();
    }
}

// Injects the error for the duration of the scope on all shards.
// Runs in seastar thread.
class scoped_error_injection {
    std::string_view _name;
public:
    // Add other overloads as needed
    explicit scoped_error_injection(std::string_view name) : _name(name) {
        smp::invoke_on_all([this] {
            utils::get_local_injector().enable(_name);
        }).get();
    }

    ~scoped_error_injection() {
        smp::invoke_on_all([this] {
            utils::get_local_injector().disable(_name);
        }).get();
    }
};
