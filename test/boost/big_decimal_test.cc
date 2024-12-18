/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
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


BOOST_AUTO_TEST_CASE(test_boost_multiprecision_sign) {
    namespace bmp = boost::multiprecision;

    // Validate assumption made about boost::multiprecision::sign
    BOOST_REQUIRE_EQUAL(bmp::sign(bmp::cpp_int(-10000)), -1);
    BOOST_REQUIRE_EQUAL(bmp::sign(bmp::cpp_int(-55545456)), -1);
    BOOST_REQUIRE_EQUAL(bmp::sign(bmp::cpp_int(-1)), -1);
    BOOST_REQUIRE_EQUAL(bmp::sign(bmp::cpp_int(0)), 0);
    BOOST_REQUIRE_EQUAL(bmp::sign(bmp::cpp_int(1)), 1);
    BOOST_REQUIRE_EQUAL(bmp::sign(bmp::cpp_int(10000)), 1);
    BOOST_REQUIRE_EQUAL(bmp::sign(bmp::cpp_int(55545456)), 1);
}

BOOST_AUTO_TEST_CASE(test_big_decimal_cmp) {
    const auto fmt_ordering = [] (std::strong_ordering r) {
        if (r == std::strong_ordering::less) {
            return "<";
        } else if (r == std::strong_ordering::equal) {
            return "=";
        } else if (r == std::strong_ordering::greater) {
            return ">";
        } else {
            return "?";
        }
    };
    // Some of the numbers in this test are truly humongous, converting them to string will stress the machine and will make the test output unusable.
    // So avoid rendering the numbers using big_decimal::to_string(), just print the unscaled value and the scale instead.
    const auto fmt_decimal = [] (const big_decimal& x) {
        return fmt::format("{}**{}", x.unscaled_value().str(), -int64_t(x.scale()));
    };
    auto test_cmp = [&] (const big_decimal& a, const big_decimal& b, std::strong_ordering expected_result, std::source_location sl = std::source_location::current()) {
        fmt::print("checking {} {} {} @ {}:{}\n", fmt_decimal(a), fmt_ordering(expected_result), fmt_decimal(b), sl.file_name(), sl.line());
        auto res = a <=> b;
        if (res != expected_result) {
            BOOST_FAIL(fmt::format("{} <=> {}: expected result: {}, but got {}", fmt_decimal(a), fmt_decimal(b), fmt_ordering(expected_result), fmt_ordering(res)));
        }
    };

    // Test compare with same scale, should hit early return for same scale.
    test_cmp(big_decimal("1"), big_decimal("-1"), std::strong_ordering::greater);
    test_cmp(big_decimal("1.3345E3"), big_decimal("-1.3344E3"), std::strong_ordering::greater);
    test_cmp(big_decimal("18E2"), big_decimal("2E2"), std::strong_ordering::greater);

    // Test compare with different signs, should hit early return for different signs.
    test_cmp(big_decimal("-1.3345E3"), big_decimal("1"), std::strong_ordering::less);
    test_cmp(big_decimal("1.3345E3"), big_decimal("-1.3344E5"), std::strong_ordering::greater);

    // Test compare against 0, should hit early return for different zero.
    test_cmp(big_decimal("0"), big_decimal("0"), std::strong_ordering::equal);
    test_cmp(big_decimal("0E6"), big_decimal("0E1"), std::strong_ordering::equal);
    test_cmp(big_decimal("1"), big_decimal("0"), std::strong_ordering::greater);
    test_cmp(big_decimal("1"), big_decimal("0E7"), std::strong_ordering::greater);
    test_cmp(big_decimal("1.32432345E7"), big_decimal("0"), std::strong_ordering::greater);
    test_cmp(big_decimal("-1"), big_decimal("0"), std::strong_ordering::less);
    test_cmp(big_decimal("-1"), big_decimal("0E-4"), std::strong_ordering::less);
    test_cmp(big_decimal("-1.3345E3"), big_decimal("0"), std::strong_ordering::less);
    test_cmp(big_decimal("0"), big_decimal("1"), std::strong_ordering::less);
    test_cmp(big_decimal("0"), big_decimal("187687E3"), std::strong_ordering::less);
    test_cmp(big_decimal("0"), big_decimal("-187687E3"), std::strong_ordering::greater);

    // Test compare scale-compare algorithm.
    test_cmp(big_decimal("1.1221310000000023424234E9"), big_decimal("1.267687687968768686"), std::strong_ordering::greater);
    test_cmp(big_decimal("1.267687687968768686"), big_decimal("1.1221310000000023424234E9"), std::strong_ordering::less);
    test_cmp(big_decimal("134252342E9"), big_decimal("100023424234E19"), std::strong_ordering::less);
    test_cmp(big_decimal("-1.1221310000000023424234E9"), big_decimal("-1.267687687968768686"), std::strong_ordering::less);
    test_cmp(big_decimal("-1.267687687968768686"), big_decimal("-1.1221310000000023424234E9"), std::strong_ordering::greater);
    test_cmp(big_decimal("-134252342E9"), big_decimal("-100023424234E19"), std::strong_ordering::greater);

    const auto min_scale = std::numeric_limits<int32_t>::min();
    const auto max_scale = std::numeric_limits<int32_t>::max();
    const auto huge_unscaled_number = std::numeric_limits<uint64_t>::max();

    // Test compare numbers with huge differences, this used to cause the comparison to allocate large amount of memory and generate stalls.
    // Reproduces https://github.com/scylladb/scylladb/issues/21716
    test_cmp(big_decimal("1789679678670986897698780986986E99999999"), big_decimal("1789679678670986897698780986986E-99999999"), std::strong_ordering::greater);
    test_cmp(big_decimal(max_scale, 1), big_decimal("1"), std::strong_ordering::less);
    test_cmp(big_decimal(min_scale, 1), big_decimal("1"), std::strong_ordering::greater);
    test_cmp(big_decimal(max_scale, -1), big_decimal("-1"), std::strong_ordering::greater);
    test_cmp(big_decimal(min_scale, -1), big_decimal("-1"), std::strong_ordering::less);

    // Test the edges of the compare scale-compare algorithm.
    // Note, scale has implicit inverted sign.
    test_cmp(big_decimal(max_scale, huge_unscaled_number), big_decimal(min_scale, huge_unscaled_number), std::strong_ordering::less);
    test_cmp(big_decimal(min_scale, huge_unscaled_number), big_decimal(max_scale, huge_unscaled_number), std::strong_ordering::greater);
    test_cmp(big_decimal(max_scale - 3, huge_unscaled_number), big_decimal(max_scale, huge_unscaled_number), std::strong_ordering::greater);
    test_cmp(big_decimal(max_scale, huge_unscaled_number), big_decimal(max_scale - 3, huge_unscaled_number), std::strong_ordering::less);
    test_cmp(big_decimal(min_scale, huge_unscaled_number), big_decimal(min_scale + 3, huge_unscaled_number), std::strong_ordering::greater);
    test_cmp(big_decimal(min_scale + 3, huge_unscaled_number), big_decimal(min_scale, huge_unscaled_number), std::strong_ordering::less);

    // Test the comparison with numbers such that the difference is very close to the confidence window.
    test_cmp(big_decimal("17.921310000000023424234"), big_decimal("1"), std::strong_ordering::greater); // just below
    test_cmp(big_decimal("-18.891310000000023424234"), big_decimal("-1"), std::strong_ordering::less); // just above
    test_cmp(big_decimal("-1"), big_decimal("-17.921310000000023424234"), std::strong_ordering::greater); // just below
    test_cmp(big_decimal("1"), big_decimal("18.891310000000023424234"), std::strong_ordering::less); // just above

    // Test fall-back to slow, should hit fall-back to slow and precise tri-compare.
    test_cmp(big_decimal(-4, 1), big_decimal(-3, 15), std::strong_ordering::less);
    test_cmp(big_decimal("1.1221310000000023424234"), big_decimal("1"), std::strong_ordering::greater);
    test_cmp(big_decimal("1"), big_decimal("1.1221310000000023424234"), std::strong_ordering::less);
    test_cmp(big_decimal("1.88796E-4"), big_decimal("2.024E-4"), std::strong_ordering::less);
    // The two numbers used in the following tests will generate very different scales, so the scale-compare
    // algorithm will engage but at the end it should realize that the numbers are too close.
    test_cmp(big_decimal("1.0000000000000000000001"), big_decimal("1.00000000000000000000010000000000000007"), std::strong_ordering::less);
    test_cmp(big_decimal("1.00000000000000000000010000000000000007"), big_decimal("1.0000000000000000000001"), std::strong_ordering::greater);
    test_cmp(big_decimal("-1.0000000000000000000001"), big_decimal("-1.00000000000000000000010000000000000007"), std::strong_ordering::greater);
    test_cmp(big_decimal("-1.00000000000000000000010000000000000007"), big_decimal("-1.0000000000000000000001"), std::strong_ordering::less);
    test_cmp(big_decimal("1e1000000"), big_decimal("99e999998"), std::strong_ordering::greater);
    test_cmp(big_decimal("-99e999998"), big_decimal("-1e1000000"), std::strong_ordering::greater);

    // A test which is sensitive to the accuracy of log2 (will fail if using sqrt
    // instead of log2, as an earlier version of the code did by mistake).
    test_cmp(big_decimal("1"), big_decimal("2582249878086908589655919172003011874329705792829223512830659356540647622016841194629645353280137831435903171972747493376E-123"), std::strong_ordering::greater);
}
