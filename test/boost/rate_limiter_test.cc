/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <chrono>
#include <cstdint>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/util/later.hh>
#include <seastar/testing/test_case.hh>

#include "db/rate_limiter.hh"

using namespace seastar;
using test_rate_limiter = db::generic_rate_limiter<seastar::manual_clock>;

future<> step_seconds(int seconds) {
    for (int i = 0; i < seconds; i++) {
        // The rate limiter's timer executes periodically every second
        // and we want the timer to run `seconds` times.
        // Because `manual_clock::advance` executes each timer only once
        // even if they reschedule, we cannot just advance by requested
        // number of seconds - instead, we must advance multiple times
        // by one second.
        manual_clock::advance(std::chrono::seconds(1));
        co_await yield();
    }
}

SEASTAR_TEST_CASE(test_rate_limiter_no_rejections_on_sequential) {
    const uint64_t token_count = 1000 * 1000;
    test_rate_limiter::label lbl;

    test_rate_limiter limiter;

    for (uint64_t token = 0; token < token_count; token++) {
        BOOST_REQUIRE_LE(limiter.increase_and_get_counter(lbl, token), 1);
        co_await maybe_yield();
    }
}

SEASTAR_TEST_CASE(test_rate_limiter_partition_label_separation) {
    const uint64_t token_count = 30;
    const uint64_t repeat_count = 10;
    std::vector<test_rate_limiter::label> labels{3};

    test_rate_limiter limiter;

    for (uint64_t i = 0; i < repeat_count; i++) {
        for (uint64_t token = 0; token < token_count; token++) {
            for (auto& l : labels) {
                BOOST_REQUIRE_EQUAL(limiter.increase_and_get_counter(l, token), i + 1);
                co_await maybe_yield();
            }
        }
    }
}

SEASTAR_TEST_CASE(test_rate_limiter_halving_over_time) {
    test_rate_limiter::label lbl;
    test_rate_limiter limiter;

    for (int i = 0; i < 16; i++) {
        limiter.increase_and_get_counter(lbl, 0);
    }

    // Should be cut in half
    co_await step_seconds(1);
    BOOST_REQUIRE_EQUAL(limiter.increase_and_get_counter(lbl, 0), (16 / 2) + 1);

    // Should decrease four times (9 -> 2)
    co_await step_seconds(2);
    BOOST_REQUIRE_EQUAL(limiter.increase_and_get_counter(lbl, 0), (9 / 4) + 1);

    // Should be reset
    co_await step_seconds(10);
    BOOST_REQUIRE_EQUAL(limiter.increase_and_get_counter(lbl, 0), 1);
}

SEASTAR_TEST_CASE(test_rate_limiter_time_window_wraparound_handling) {
    test_rate_limiter::label lbl;
    test_rate_limiter limiter;

    BOOST_REQUIRE_EQUAL(limiter.increase_and_get_counter(lbl, 0), 1);
    BOOST_REQUIRE_EQUAL(limiter.increase_and_get_counter(lbl, 0), 2);
    BOOST_REQUIRE_EQUAL(limiter.increase_and_get_counter(lbl, 0), 3);

    // Advance far into the future so that the time window wraps around
    co_await step_seconds(1 << test_rate_limiter::time_window_bits);

    BOOST_REQUIRE_EQUAL(limiter.increase_and_get_counter(lbl, 0), 1);
    BOOST_REQUIRE_EQUAL(limiter.increase_and_get_counter(lbl, 0), 2);
    BOOST_REQUIRE_EQUAL(limiter.increase_and_get_counter(lbl, 0), 3);

    // TODO: Workaround for seastar#1072. Calling `manual_clock::advance`
    // multiple times and then quitting the test immediately causes
    // the test framework to hang. I didn't have the time to debug it, but I
    // suspect there are some pending tasks which need to finish before exiting
    // from the main test task.
    co_await seastar::sleep(std::chrono::seconds(1));
}

SEASTAR_TEST_CASE(test_rate_limiter_account_operation) {
    const uint64_t limit = 1;
    const int ops_per_loop = 1000;
    test_rate_limiter::label lbl;

    test_rate_limiter limiter;

    // We use UINT_MAX as the random parameter so that we get rejected quickly
    db::per_partition_rate_limit::account_and_enforce info {
        .random_variable = UINT32_MAX,
    };

    bool encountered_rejection = false;
    for (int i = 0; i < ops_per_loop; i++) {
        if (limiter.account_operation(lbl, 0, limit, info) == test_rate_limiter::can_proceed::no) {
            encountered_rejection = true;
            break;
        }
        co_await maybe_yield();
    }
    BOOST_REQUIRE(encountered_rejection);
}
