/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#define BOOST_TEST_MODULE big_decimal

#include <boost/test/unit_test.hpp>
#include "utils/big_decimal.hh"
#include "marshal_exception.hh"

namespace {

void test_div(const char *r_cstr, const int64_t q, const char *expected_cstr) {
    big_decimal r{r_cstr};
    auto res = r.div(q, big_decimal::rounding_mode::HALF_EVEN);
    big_decimal expected{expected_cstr};
    BOOST_REQUIRE_EQUAL(res.unscaled_value(), expected.unscaled_value());
    BOOST_REQUIRE_EQUAL(res.scale(), expected.scale());
}

template<typename Op>
void test_op(const char* x_cstr, const char* y_cstr, const char* expected_cstr, Op&& op) {
    big_decimal x{x_cstr};
    big_decimal y{y_cstr};
    big_decimal expected{expected_cstr};
    auto ret = op(x, y);
    BOOST_REQUIRE_EQUAL(ret.unscaled_value(), expected.unscaled_value());
    BOOST_REQUIRE_EQUAL(ret.scale(), expected.scale());
}

void test_assignadd(const char *x_cstr, const char *y_cstr, const char *expected_cstr) {
    return test_op(x_cstr, y_cstr, expected_cstr, [](big_decimal& d1, big_decimal& d2) { return d1 += d2; });
}

void test_add(const char *x_cstr, const char *y_cstr, const char *expected_cstr) {
    return test_op(x_cstr, y_cstr, expected_cstr, [](big_decimal& d1, big_decimal& d2) { return d1 + d2; });
}

void test_assignsub(const char *x_cstr, const char *y_cstr, const char *expected_cstr) {
    return test_op(x_cstr, y_cstr, expected_cstr, [](big_decimal& d1, big_decimal& d2) { return d1 -= d2; });
}

void test_sub(const char *x_cstr, const char *y_cstr, const char *expected_cstr) {
    return test_op(x_cstr, y_cstr, expected_cstr, [](big_decimal& d1, big_decimal& d2) { return d1 - d2; });
}

} /* anonymous namespoce */

BOOST_AUTO_TEST_CASE(test_big_decimal_construct_from_string) {
    big_decimal x0{"0"};
    big_decimal x1{"0.0"};
    big_decimal x2{"0.00"};
    big_decimal x3{"0.000"};
    big_decimal x4{"0E3"};
    big_decimal x5{"0E10"};
    big_decimal x6{"+12.34e5"};
    big_decimal x7{"10e+3"};

    BOOST_REQUIRE_EQUAL(x0.unscaled_value(), 0);
    BOOST_REQUIRE_EQUAL(x0.scale(), 0);

    BOOST_REQUIRE_EQUAL(x1.unscaled_value(), 0);
    BOOST_REQUIRE_EQUAL(x1.scale(), 1);

    BOOST_REQUIRE_EQUAL(x2.unscaled_value(), 0);
    BOOST_REQUIRE_EQUAL(x2.scale(), 2);

    BOOST_REQUIRE_EQUAL(x3.unscaled_value(), 0);
    BOOST_REQUIRE_EQUAL(x3.scale(), 3);

    BOOST_REQUIRE_EQUAL(x4.unscaled_value(), 0);
    BOOST_REQUIRE_EQUAL(x4.scale(), -3);

    BOOST_REQUIRE_EQUAL(x5.unscaled_value(), 0);
    BOOST_REQUIRE_EQUAL(x5.scale(), -10);

    BOOST_REQUIRE_EQUAL(x6.unscaled_value(), 1234);
    BOOST_REQUIRE_EQUAL(x6.scale(), -3);

    BOOST_REQUIRE_EQUAL(x7.unscaled_value(), 10);
    BOOST_REQUIRE_EQUAL(x7.scale(), -3);

    BOOST_REQUIRE_THROW(big_decimal(""), marshal_exception);
    BOOST_REQUIRE_THROW(big_decimal("10.0.3"), marshal_exception);
    BOOST_REQUIRE_THROW(big_decimal("10.0e7.3"), marshal_exception);
    BOOST_REQUIRE_THROW(big_decimal("10.0e7e2"), marshal_exception);
    BOOST_REQUIRE_THROW(big_decimal("-"), marshal_exception);
    BOOST_REQUIRE_THROW(big_decimal("."), marshal_exception);
    BOOST_REQUIRE_THROW(big_decimal(".e"), marshal_exception);
    BOOST_REQUIRE_THROW(big_decimal(".E0"), marshal_exception);
    BOOST_REQUIRE_THROW(big_decimal("10e"), marshal_exception);
    BOOST_REQUIRE_THROW(big_decimal("10.3e"), marshal_exception);
    BOOST_REQUIRE_THROW(big_decimal("10.3e+"), marshal_exception);
    BOOST_REQUIRE_THROW(big_decimal("-+5"), marshal_exception);
    BOOST_REQUIRE_THROW(big_decimal("+-5"), marshal_exception);
    BOOST_REQUIRE_THROW(big_decimal("++5"), marshal_exception);
    BOOST_REQUIRE_THROW(big_decimal("--5"), marshal_exception);
}

BOOST_AUTO_TEST_CASE(test_big_decimal_div) {
    test_div("1", 4, "0");
    test_div("1.00", 4, "0.25");
    test_div("0.10", 4, "0.02");
    test_div("1.000", 4, "0.250");
    test_div("0.100", 4, "0.025");
    test_div("1", 3, "0");
    test_div("1.00", 3, "0.33");
    test_div("1.000", 3, "0.333");
    test_div("0.100", 3, "0.033");
    test_div("11", 10, "1");
    test_div("15", 10, "2");
    test_div("16", 10, "2");
    test_div("25", 10, "2");
    test_div("26", 10, "3");
    test_div("0.11", 10, "0.01");
    test_div("0.15", 10, "0.02");
    test_div("0.16", 10, "0.02");
    test_div("0.25", 10, "0.02");
    test_div("0.26", 10, "0.03");
    test_div("10E10", 3, "3E10");

    test_div("-1", 4, "0");
    test_div("-1.00", 4, "-0.25");
    test_div("-0.10", 4, "-0.02");
    test_div("-1.000", 4, "-0.250");
    test_div("-0.100", 4, "-0.025");
    test_div("-1", 3, "0");
    test_div("-1.00", 3, "-0.33");
    test_div("-1.000", 3, "-0.333");
    test_div("-0.100", 3, "-0.033");
    test_div("-11", 10, "-1");
    test_div("-15", 10, "-2");
    test_div("-16", 10, "-2");
    test_div("-25", 10, "-2");
    test_div("-26", 10, "-3");
    test_div("-0.11", 10, "-0.01");
    test_div("-0.15", 10, "-0.02");
    test_div("-0.16", 10, "-0.02");
    test_div("-0.25", 10, "-0.02");
    test_div("-0.26", 10, "-0.03");
    test_div("-10E10", 3, "-3E10");

    // Document a small oddity, 1e1 has -1 decimal places, so dividing
    // it by 2 produces 0. This is not the behavior in cassandra, but
    // scylla doesn't expose arithmetic operations, so this doesn't
    // seem to be visible from CQL.
    test_div("10", 2, "5");
    test_div("1e1", 2, "0e1");
}

BOOST_AUTO_TEST_CASE(test_big_decimal_assignadd) {
    test_assignadd("1", "4", "5");
    test_assignadd("1.00", "4.00", "5.00");
    test_assignadd("1.000", "4.000", "5.000");
    test_assignadd("1", "-1", "0");
    test_assignadd("1.00", "-1.00", "0.00");
    test_assignadd("1.000", "-1.000", "0.000");
    test_assignadd("0.0", "0.000", "0.000");
    test_assignadd("1.0", "1.000", "2.000");
    test_assignadd("-1.0", "-1.000", "-2.000");
}

BOOST_AUTO_TEST_CASE(test_big_decimal_add) {
    test_add("1", "4", "5");
    test_add("1.00", "4.00", "5.00");
    test_add("1.000", "4.000", "5.000");
    test_add("1", "-1", "0");
    test_add("1.00", "-1.00", "0.00");
    test_add("1.000", "-1.000", "0.000");
    test_add("0.0", "0.000", "0.000");
    test_add("1.0", "1.000", "2.000");
    test_add("-1.0", "-1.000", "-2.000");
    test_add("0000123456789012345678901234", "19e3", "00123456789012345678920234");
    test_add("000000000012345678901234.5678901234e-1", "0777.555555555555555555555e2", "1234567967879.0123445678955555555");
}

BOOST_AUTO_TEST_CASE(test_big_decimal_assignsub) {
    test_assignsub("1", "4", "-3");
    test_assignsub("1.00", "4.00", "-3.00");
    test_assignsub("1.000", "4.000", "-3.000");
    test_assignsub("1", "-1", "2");
    test_assignsub("1.00", "-1.00", "2.00");
    test_assignsub("1.000", "-1.000", "2.000");
    test_assignsub("0.0", "0.000", "0.000");
    test_assignsub("1.0", "1.000", "0.000");
    test_assignsub("-1.0", "1.000", "-2.000");
}

BOOST_AUTO_TEST_CASE(test_big_decimal_sub) {
    test_sub("1", "4", "-3");
    test_sub("1.00", "4.00", "-3.00");
    test_sub("1.000", "4.000", "-3.000");
    test_sub("1", "-1", "2");
    test_sub("1.00", "-1.00", "2.00");
    test_sub("1.000", "-1.000", "2.000");
    test_sub("0.0", "0.000", "0.000");
    test_sub("1.0", "1.000", "0.000");
    test_sub("-1.0", "1.000", "-2.000");
    test_sub("9999999999999999999999999999999999999", "-1.000e0", "10000000000000000000000000000000000000.000");
    test_sub("+10.", "1.e+1", "0");
}
