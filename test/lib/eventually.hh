/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <boost/test/unit_test.hpp>

#include <seastar/core/coroutine.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/noncopyable_function.hh>
#include <seastar/util/later.hh>

#include "seastarx.hh"

using sleep_fn = std::function<future<>(std::chrono::milliseconds)>;

extern sleep_fn seastar_sleep_fn;

extern sleep_fn manual_clock_sleep_fn;

inline
void eventually(noncopyable_function<void ()> f, size_t max_attempts = 17, sleep_fn sleep = seastar_sleep_fn) {
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
bool eventually_true(noncopyable_function<bool ()> f, sleep_fn sleep = seastar_sleep_fn) {
    const unsigned max_attempts = 15;
    unsigned attempts = 0;
    while (true) {
        if (f()) {
            return true;
        }

        if (++attempts < max_attempts) {
            sleep(std::chrono::milliseconds(1 << attempts)).get();
        } else {
            return false;
        }
    }

    return false;
}

// Must be called in a seastar thread
template <typename T>
void REQUIRE_EVENTUALLY_EQUAL(std::function<T()> a, T b, sleep_fn sleep = seastar_sleep_fn) {
    eventually_true([&] { return a() == b; }, sleep);
    BOOST_REQUIRE_EQUAL(a(), b);
}

// Must be called in a seastar thread
template <typename T>
void CHECK_EVENTUALLY_EQUAL(std::function<T()> a, T b, sleep_fn sleep = seastar_sleep_fn) {
    eventually_true([&] { return a() == b; }, sleep);
    BOOST_CHECK_EQUAL(a(), b);
}
