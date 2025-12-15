/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/error_injection.hh"

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
