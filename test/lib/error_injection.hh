/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <chrono>
#include <stdexcept>
#include <fmt/format.h>
#include <seastar/core/future.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include "utils/error_injection.hh"

// Waits until enter_count for the named injection reaches the given threshold.
// A replacement for busy-loop polling in unit tests. Throws if the threshold
// is not reached within the timeout, so a test that would otherwise hang
// forever fails with a clear error instead.
inline future<> wait_for_injection_enter(std::string_view injection_name, size_t threshold = 1,
        std::chrono::steady_clock::duration timeout = std::chrono::seconds(30)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (utils::get_local_injector().enter_count(injection_name) < threshold) {
        if (std::chrono::steady_clock::now() > deadline) {
            throw std::runtime_error(fmt::format(
                    "wait_for_injection_enter(\"{}\"): enter_count did not reach {} within {}s",
                    injection_name, threshold,
                    std::chrono::duration_cast<std::chrono::seconds>(timeout).count()));
        }
        co_await coroutine::maybe_yield();
    }
}

// Injects the error for the duration of the scope on all shards.
// Runs in seastar thread.
class scoped_error_injection {
    std::string_view _name;
    bool _one_shot;
    utils::error_injection_parameters _parameters;
public:
    // Add other overloads as needed
    explicit scoped_error_injection(std::string_view name) : scoped_error_injection{name, false, {}}
    {
    }

    scoped_error_injection(std::string_view name, bool one_shot, utils::error_injection_parameters parameters) 
        : _name(name)
        , _one_shot(one_shot)
        , _parameters(std::move(parameters))
    {
        smp::invoke_on_all([this] {
            utils::get_local_injector().enable(_name, _one_shot, _parameters);
        }).get();
    }

    ~scoped_error_injection() {
        smp::invoke_on_all([this] {
            utils::get_local_injector().disable(_name);
        }).get();
    }
};
