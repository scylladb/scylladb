/*
 * Copyright 2015 Cloudius Systems
 */

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include "types.hh"
#include "compound.hh"

BOOST_AUTO_TEST_CASE(test_bytes_type_string_conversions) {
    BOOST_REQUIRE(bytes_type->equal(bytes_type->from_string("616263646566"), bytes_type->decompose(bytes{"abcdef"})));
}

BOOST_AUTO_TEST_CASE(test_int32_type_string_conversions) {
    BOOST_REQUIRE(int32_type->equal(int32_type->from_string("1234567890"), int32_type->decompose(1234567890)));
    BOOST_REQUIRE_EQUAL(int32_type->to_string(int32_type->decompose(1234567890)), "1234567890");

    BOOST_REQUIRE(int32_type->equal(int32_type->from_string("12"), int32_type->decompose(12)));
    BOOST_REQUIRE(int32_type->equal(int32_type->from_string("0012"), int32_type->decompose(12)));
    BOOST_REQUIRE(int32_type->equal(int32_type->from_string("+12"), int32_type->decompose(12)));
    BOOST_REQUIRE_EQUAL(int32_type->to_string(int32_type->decompose(12)), "12");
    BOOST_REQUIRE(int32_type->equal(int32_type->from_string("-12"), int32_type->decompose(-12)));
    BOOST_REQUIRE_EQUAL(int32_type->to_string(int32_type->decompose(-12)), "-12");

    BOOST_REQUIRE(int32_type->equal(int32_type->from_string("0"), int32_type->decompose(0)));
    BOOST_REQUIRE(int32_type->equal(int32_type->from_string("-0"), int32_type->decompose(0)));
    BOOST_REQUIRE(int32_type->equal(int32_type->from_string("+0"), int32_type->decompose(0)));
    BOOST_REQUIRE_EQUAL(int32_type->to_string(int32_type->decompose(0)), "0");

    BOOST_REQUIRE(int32_type->equal(int32_type->from_string("-2147483648"), int32_type->decompose((int32_t)-2147483648)));
    BOOST_REQUIRE_EQUAL(int32_type->to_string(int32_type->decompose((int32_t)-2147483648)), "-2147483648");

    BOOST_REQUIRE(int32_type->equal(int32_type->from_string("2147483647"), int32_type->decompose((int32_t)2147483647)));
    BOOST_REQUIRE_EQUAL(int32_type->to_string(int32_type->decompose((int32_t)-2147483647)), "-2147483647");

    auto test_parsing_fails = [] (sstring text) {
        try {
            int32_type->from_string(text);
            BOOST_FAIL(sprint("Parsing of '%s' should have failed", text));
        } catch (const marshal_exception& e) {
            // expected
        }
    };

    test_parsing_fails("asd");
    test_parsing_fails("-2147483649");
    test_parsing_fails("2147483648");
    test_parsing_fails("2147483648123");

    BOOST_REQUIRE_EQUAL(int32_type->to_string(bytes()), "");
}

BOOST_AUTO_TEST_CASE(test_compound_type_compare) {
    compound_type<> type({utf8_type, utf8_type, utf8_type});

    BOOST_REQUIRE(type.compare(
        type.serialize_value({bytes("a"), bytes("b"), bytes("c")}),
        type.serialize_value({bytes("a"), bytes("b"), bytes("c")})) == 0);

    BOOST_REQUIRE(type.compare(
        type.serialize_value({bytes("a"), bytes("b"), bytes("c")}),
        type.serialize_value({bytes("a"), bytes("b"), bytes("d")})) < 0);

    BOOST_REQUIRE(type.compare(
        type.serialize_value({bytes("a"), bytes("b"), bytes("d")}),
        type.serialize_value({bytes("a"), bytes("b"), bytes("c")})) > 0);

    BOOST_REQUIRE(type.compare(
        type.serialize_value({bytes("a"), bytes("b"), bytes("d")}),
        type.serialize_value({bytes("a"), bytes("d"), bytes("c")})) < 0);

    BOOST_REQUIRE(type.compare(
        type.serialize_value({bytes("a"), bytes("d"), bytes("c")}),
        type.serialize_value({bytes("c"), bytes("b"), bytes("c")})) < 0);
}

template <typename T>
std::experimental::optional<T>
extract(boost::any a) {
    if (a.empty()) {
        return std::experimental::nullopt;
    } else {
        return std::experimental::make_optional(boost::any_cast<T>(a));
    }
}

template <typename T>
boost::any
unextract(std::experimental::optional<T> v) {
    if (v) {
        return boost::any(*v);
    } else {
        return boost::any();
    }
}

template <typename T>
using opt = std::experimental::optional<T>;

BOOST_AUTO_TEST_CASE(test_tuple) {
    auto t = tuple_type_impl::get_instance({int32_type, long_type, utf8_type});
    using native_type = tuple_type_impl::native_type;
    using c_type = std::tuple<opt<int32_t>, opt<int64_t>, opt<sstring>>;
    auto native_to_c = [] (native_type v) {
        return std::make_tuple(extract<int32_t>(v[0]), extract<int64_t>(v[1]), extract<sstring>(v[2]));
    };
    auto c_to_native = [] (std::tuple<opt<int32_t>, opt<int64_t>, opt<sstring>> v) {
        return native_type({unextract(std::get<0>(v)), unextract(std::get<1>(v)), unextract(std::get<2>(v))});
    };
    auto native_to_bytes = [t] (native_type v) {
        return t->decompose(v);
    };
    auto bytes_to_native = [t] (bytes v) {
        return boost::any_cast<native_type>(t->deserialize(v));
    };
    auto c_to_bytes = [=] (c_type v) {
        return native_to_bytes(c_to_native(v));
    };
    auto bytes_to_c = [=] (bytes v) {
        return native_to_c(bytes_to_native(v));
    };
    auto round_trip = [=] (c_type v) {
        return bytes_to_c(c_to_bytes(v));
    };
    auto v1 = c_type(int32_t(1), int64_t(2), sstring("abc"));
    BOOST_REQUIRE(v1 == round_trip(v1));
    auto v2 = c_type(int32_t(1), int64_t(2), std::experimental::nullopt);
    BOOST_REQUIRE(v2 == round_trip(v2));
    auto b1 = c_to_bytes(v1);
    auto b2 = c_to_bytes(v2);
    BOOST_REQUIRE(t->compare(b1, b2) > 0);
    BOOST_REQUIRE(t->compare(b2, b2) == 0);
}
