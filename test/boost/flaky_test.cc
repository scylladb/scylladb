/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/testing/test_case.hh>
#include "test/lib/random_utils.hh"
#include <fmt/core.h>

SEASTAR_TEST_CASE(flaky_test) {
    BOOST_REQUIRE_EQUAL(tests::random::get_int(5), 0);
    return make_ready_future<>();
}
