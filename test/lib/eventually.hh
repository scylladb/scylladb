/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sleep.hh>
#include <seastar/util/noncopyable_function.hh>

#include "seastarx.hh"

inline
void eventually(noncopyable_function<void ()> f, size_t max_attempts = 17) {
    size_t attempts = 0;
    while (true) {
        try {
            f();
            break;
        } catch (...) {
            if (++attempts < max_attempts) {
                sleep(std::chrono::milliseconds(1 << attempts)).get();
            } else {
                throw;
            }
        }
    }
}

inline
bool eventually_true(noncopyable_function<bool ()> f) {
    const unsigned max_attempts = 15;
    unsigned attempts = 0;
    while (true) {
        if (f()) {
            return true;
        }

        if (++attempts < max_attempts) {
            seastar::sleep(std::chrono::milliseconds(1 << attempts)).get();
        } else {
            return false;
        }
    }

    return false;
}

#define REQUIRE_EVENTUALLY_EQUAL(a, b) BOOST_REQUIRE(eventually_true([&] { return a == b; }))
#define CHECK_EVENTUALLY_EQUAL(a, b) BOOST_CHECK(eventually_true([&] { return a == b; }))
