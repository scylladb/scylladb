/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <limits>

#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/test_case.hh>

#include "utils/tagged_integer.hh"
#include "gms/generation-number.hh"
#include "gms/version_generator.hh"

using namespace seastar;

using test_tagged_int = utils::tagged_integer<struct test_int_tag, int>;

SEASTAR_THREAD_TEST_CASE(test_tagged_integer_ops) {
    int i = 17;
    auto t = test_tagged_int(i);

    BOOST_REQUIRE_EQUAL(t.value(), i);
    BOOST_REQUIRE_EQUAL((++t).value(), ++i);
    BOOST_REQUIRE_EQUAL((--t).value(), --i);
    BOOST_REQUIRE_EQUAL((t++).value(), i++);
    BOOST_REQUIRE_EQUAL((t--).value(), i--);
    BOOST_REQUIRE_EQUAL((t + test_tagged_int(42)).value(), i + 42);
    BOOST_REQUIRE_EQUAL((t - test_tagged_int(42)).value(), i - 42);
    BOOST_REQUIRE_EQUAL((t += test_tagged_int(42)).value(), i += 42);
    BOOST_REQUIRE_EQUAL((t -= test_tagged_int(42)).value(), i -= 42);
}

SEASTAR_THREAD_TEST_CASE(test_tagged_integer_numeric_limits) {
    BOOST_REQUIRE(std::numeric_limits<test_tagged_int>::is_specialized);
    BOOST_REQUIRE_EQUAL(std::numeric_limits<test_tagged_int>::is_signed, std::numeric_limits<test_tagged_int::value_type>::is_signed);
    BOOST_REQUIRE_EQUAL(std::numeric_limits<test_tagged_int>::is_integer, std::numeric_limits<test_tagged_int::value_type>::is_integer);
    BOOST_REQUIRE_EQUAL(std::numeric_limits<test_tagged_int>::is_exact, std::numeric_limits<test_tagged_int::value_type>::is_exact);
    BOOST_REQUIRE_EQUAL(std::numeric_limits<test_tagged_int>::is_bounded, std::numeric_limits<test_tagged_int::value_type>::is_bounded);

    BOOST_REQUIRE_EQUAL(std::numeric_limits<test_tagged_int>::min().value(), std::numeric_limits<test_tagged_int::value_type>::min());
    BOOST_REQUIRE_EQUAL(std::numeric_limits<test_tagged_int>::max().value(), std::numeric_limits<test_tagged_int::value_type>::max());
}

SEASTAR_THREAD_TEST_CASE(test_gms_generation_type_numeric_limits) {
    BOOST_REQUIRE_EQUAL(std::numeric_limits<gms::generation_type>::min().value(), std::numeric_limits<gms::generation_type::value_type>::min());
    BOOST_REQUIRE_EQUAL(std::numeric_limits<gms::generation_type>::max().value(), std::numeric_limits<gms::generation_type::value_type>::max());
}

SEASTAR_THREAD_TEST_CASE(test_gms_version_type_numeric_limits) {
    BOOST_REQUIRE_EQUAL(std::numeric_limits<gms::version_type>::min().value(), std::numeric_limits<gms::version_type::value_type>::min());
    BOOST_REQUIRE_EQUAL(std::numeric_limits<gms::version_type>::max().value(), std::numeric_limits<gms::version_type::value_type>::max());
}
