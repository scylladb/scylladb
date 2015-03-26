/*
 * Copyright 2015 Cloudius Systems
 */

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE core

#include <boost/test/included/unit_test.hpp>
#include "types.hh"
#include "tuple.hh"

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

BOOST_AUTO_TEST_CASE(test_tuple_type_compare) {
    tuple_type<> type({utf8_type, utf8_type, utf8_type});

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
