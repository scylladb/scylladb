/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#define BOOST_TEST_MODULE growth_counter

#include <boost/test/unit_test.hpp>

#include "utils/growth_counter.hh"

static uint64_t fake_counter = 0;

static uint64_t read_fake_counter() {
    return fake_counter;
}

BOOST_AUTO_TEST_CASE(test_growth_counter_leaves_out_paused_growth) {
    fake_counter = 100;
    utils::growth_counter<read_fake_counter> counter;
    fake_counter = 130;
    BOOST_REQUIRE_EQUAL(counter.value(), 30u);

    counter.pause();
    // Other tasks run.
    fake_counter = 1000;
    counter.resume();
    fake_counter = 1005;
    BOOST_REQUIRE_EQUAL(counter.value(), 35u);
}
