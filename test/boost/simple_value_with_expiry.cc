/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#define BOOST_TEST_MODULE test-simple-value-with-expiry
#include <boost/test/unit_test.hpp>

 #include "utils/simple_value_with_expiry.hh"

BOOST_AUTO_TEST_CASE(empty) {
    utils::simple_value_with_expiry<int> empty;
    BOOST_CHECK_EQUAL(empty.get(), std::nullopt);
}

BOOST_AUTO_TEST_CASE(set) {
    utils::simple_value_with_expiry<int> simple;
    BOOST_CHECK_EQUAL(simple.get(), std::nullopt);
    auto now = utils::simple_value_with_expiry<int>::now();
    simple.set(42, std::chrono::seconds{1000});
    BOOST_REQUIRE(simple.get());
    BOOST_CHECK_EQUAL(*simple.get(), 42);
}

BOOST_AUTO_TEST_CASE(timeout) {
    utils::simple_value_with_expiry<int> simple;
    simple.set(42, std::chrono::seconds{1000});
    auto now = utils::simple_value_with_expiry<int>::now();
    BOOST_REQUIRE(!simple.get(now + std::chrono::seconds{1001}));
}

BOOST_AUTO_TEST_CASE(set_now) {
    utils::simple_value_with_expiry<int> simple;
    auto now1 = utils::simple_value_with_expiry<int>::now();
    auto now2 = simple.set_now(42, std::chrono::seconds{1000});
    auto now3 = utils::simple_value_with_expiry<int>::now();
    BOOST_CHECK_LE(now1, now2);
    BOOST_CHECK_LE(now2, now3);
}

BOOST_AUTO_TEST_CASE(set_replace) {
    utils::simple_value_with_expiry<int> simple;
    simple.set(42, std::chrono::seconds{1000});
    simple.set(84, std::chrono::seconds{1002});
    BOOST_REQUIRE(simple.get());
    BOOST_CHECK_EQUAL(*simple.get(), 84);
}
