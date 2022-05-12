/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>

#include "rust/inc.hh"

BOOST_AUTO_TEST_CASE(test_inc) {
    int k = 1;
    BOOST_REQUIRE(rust::inc(k) == 2);
}
