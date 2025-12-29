/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>

#include "utils/simple_value_with_expiry.hh"

BOOST_AUTO_TEST_SUITE(simple_value_with_expiry_test)

BOOST_AUTO_TEST_CASE(empty) {
    utils::simple_value_with_expiry<int> empty;
    BOOST_REQUIRE(!empty.get());
}

BOOST_AUTO_TEST_CASE(set) {
    utils::simple_value_with_expiry<int> simple;
    simple.set_if_longer_expiry(42, std::chrono::high_resolution_clock::now() + std::chrono::seconds{1000});
    BOOST_REQUIRE(simple.get());
    BOOST_CHECK_EQUAL(*simple.get(), 42);
}

BOOST_AUTO_TEST_CASE(timeout) {
    utils::simple_value_with_expiry<int> simple;
    auto now = std::chrono::high_resolution_clock::now();
    simple.set_if_longer_expiry(42, now + std::chrono::seconds{1000});
    BOOST_REQUIRE(simple.get(now + std::chrono::seconds{1000}));
    BOOST_REQUIRE(!simple.get(now + std::chrono::seconds{1001}));
}

BOOST_AUTO_TEST_CASE(set_replace) {
    utils::simple_value_with_expiry<int> simple;
    auto now = std::chrono::high_resolution_clock::now();
    simple.set_if_longer_expiry(42, now + std::chrono::seconds{1000});
    BOOST_REQUIRE(simple.get());
    BOOST_CHECK_EQUAL(*simple.get(), 42);

    simple.set_if_longer_expiry(63, now + std::chrono::seconds{1000});
    BOOST_REQUIRE(simple.get());
    BOOST_CHECK_EQUAL(*simple.get(), 42);

    simple.set_if_longer_expiry(84, now + std::chrono::seconds{1001});
    BOOST_REQUIRE(simple.get());
    BOOST_CHECK_EQUAL(*simple.get(), 84);
}

BOOST_AUTO_TEST_SUITE_END()