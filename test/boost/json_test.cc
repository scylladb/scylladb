
/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#define BOOST_TEST_MODULE json

#include <boost/test/unit_test.hpp>

#include <seastar/core/sstring.hh>

#include "utils/rjson.hh"

using namespace seastar;

BOOST_AUTO_TEST_CASE(test_value_to_quoted_string) {
    std::vector<sstring> input = {
            "\"\\\b\f\n\r\t",
            sstring(1, '\0') + "\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f",
            "regular string",
            "mixed\t\t\t\ba\f \007 string \002 fgh",
            "chào mọi người 123!",
            "ყველას მოგესალმებით 456?;",
            "всем привет",
            "大家好",
            ""
    };

    std::vector<sstring> expected = {
            "\"\\\"\\\\\\b\\f\\n\\r\\t\"",
            "\"\\u0000\\u0001\\u0002\\u0003\\u0004\\u0005\\u0006\\u0007\\b\\t\\n\\u000B\\f\\r\\u000E\\u000F\\u0010\\u0011\\u0012\\u0013\\u0014\\u0015\\u0016\\u0017\\u0018\\u0019\\u001A\\u001B\\u001C\\u001D\\u001E\\u001F\"",
            "\"regular string\"",
            "\"mixed\\t\\t\\t\\ba\\f \\u0007 string \\u0002 fgh\"",
            "\"chào mọi người 123!\"",
            "\"ყველას მოგესალმებით 456?;\"",
            "\"всем привет\"",
            "\"大家好\"",
            "\"\""
    };

    for (size_t i = 0; i < input.size(); ++i) {
        BOOST_CHECK_EQUAL(rjson::quote_json_string(input[i]), expected[i]);
    }
}

BOOST_AUTO_TEST_CASE(test_parsing_map_from_null) {
    std::map<sstring, sstring> empty_map;
    auto map1 = rjson::parse_to_map<std::map<sstring, sstring>>("null");
    auto map2 = rjson::parse_to_map<std::map<sstring, sstring>>("{}");
    BOOST_REQUIRE(map1 == map2);
    BOOST_REQUIRE(map1 == empty_map);
}

BOOST_AUTO_TEST_CASE(test_try_parse) {
    auto ok = rjson::try_parse(R"({"a": [1, 2, 3]})");
    BOOST_REQUIRE(ok);
    BOOST_REQUIRE(ok->IsObject());
    BOOST_REQUIRE_EQUAL((*ok)["a"].Size(), 3);

    for (const std::string_view bad : {"", "{", "{\"a\": }", "[1, 2", "nul", "{} {}"}) {
        auto res = rjson::try_parse(bad);
        BOOST_REQUIRE_MESSAGE(!res, fmt::format("expected parse failure for '{}'", bad));
        BOOST_REQUIRE_MESSAGE(!res.error().empty(), fmt::format("expected an error message for '{}'", bad));
    }
}

namespace rjs = rjson::schema;

static void check_valid(const rjs::schema& schema, std::string_view json) {
    auto res = rjson::parse_and_validate(json, schema);
    BOOST_REQUIRE_MESSAGE(res, fmt::format("expected '{}' to be valid, got: {}", json, res ? sstring() : res.error()));
}

// Check that json fails validation and that the error message contains error_fragment.
static void check_invalid(const rjs::schema& schema, std::string_view json, std::string_view error_fragment) {
    auto res = rjson::parse_and_validate(json, schema);
    BOOST_REQUIRE_MESSAGE(!res, fmt::format("expected '{}' to be invalid", json));
    BOOST_REQUIRE_MESSAGE(res.error().find(error_fragment) != sstring::npos,
            fmt::format("expected error for '{}' to contain '{}', got: {}", json, error_fragment, res.error()));
}

BOOST_AUTO_TEST_CASE(test_schema_scalar) {
    check_valid(rjs::scalar::string(), R"("abc")");
    check_invalid(rjs::scalar::string(), "1", "Expected string at $root");
    check_invalid(rjs::scalar::string(), "null", "Expected string at $root");

    check_valid(rjs::scalar::integer(), "-12");
    check_invalid(rjs::scalar::integer(), "1.5", "Expected integer at $root");
    check_invalid(rjs::scalar::integer(), R"("1")", "Expected integer at $root");
    check_invalid(rjs::scalar::integer(), "4294967296", "Expected integer at $root");

    check_valid(rjs::scalar::boolean(), "true");
    check_valid(rjs::scalar::boolean(), "false");
    check_invalid(rjs::scalar::boolean(), "0", "Expected boolean at $root");
}

BOOST_AUTO_TEST_CASE(test_schema_optional) {
    const auto schema = rjs::schema(rjs::optional(rjs::scalar::integer()));
    check_valid(schema, "null");
    check_valid(schema, "1");
    check_invalid(schema, R"("a")", "Expected integer at $root");
}

BOOST_AUTO_TEST_CASE(test_schema_array) {
    const auto schema = rjs::schema(rjs::array(rjs::scalar::integer(), 1, 2));
    check_valid(schema, "[1]");
    check_valid(schema, "[1, 2]");
    check_invalid(schema, "[]", "Expected size of $root to be in interval [1, 2]");
    check_invalid(schema, "[1, 2, 3]", "Expected size of $root to be in interval [1, 2]");
    check_invalid(schema, R"([1, "a"])", "Expected integer at $root[1]");
    check_invalid(schema, "{}", "Expected array at $root");

    check_valid(rjs::array(rjs::scalar::string()), "[]");
}

BOOST_AUTO_TEST_CASE(test_schema_object) {
    const auto schema = rjs::schema(rjs::object({
        {"name", rjs::scalar::string()},
        {"count", rjs::optional(rjs::scalar::integer())},
        {"tags", rjs::optional(rjs::array(rjs::object({{"key", rjs::scalar::string()}})))},
    }));

    check_valid(schema, R"({"name": "a"})");
    check_valid(schema, R"({"name": "a", "count": null})");
    check_valid(schema, R"({"name": "a", "count": 3, "tags": [{"key": "k1"}, {"key": "k2"}]})");

    check_invalid(schema, "[]", "Expected object at $root");
    check_invalid(schema, "{}", "Required member $root.name is missing or null");
    check_invalid(schema, R"({"name": null})", "Expected string at $root.name");
    check_invalid(schema, R"({"name": 1})", "Expected string at $root.name");
    check_invalid(schema, R"({"name": "a", "count": "3"})", "Expected integer at $root.count");
    check_invalid(schema, R"({"name": "a", "tags": [{"key": "k1"}, {}]})", "Required member $root.tags[1].key is missing or null");
    check_invalid(schema, R"({"name": "a", "extra": 1})", "Unknown member $root.extra");
    check_invalid(schema, R"({"name": "a", "tags": [{"key": "k", "value": "v"}]})", "Unknown member $root.tags[0].value");

    const auto lenient_schema = rjs::schema(rjs::object({{"name", rjs::scalar::string()}}, rjs::object::unknown_members::allow));
    check_valid(lenient_schema, R"({"name": "a", "extra": 1})");
}

BOOST_AUTO_TEST_CASE(test_parse_and_validate_parse_error) {
    auto res = rjson::parse_and_validate("{", rjs::object({}));
    BOOST_REQUIRE(!res);
    BOOST_REQUIRE(!res.error().empty());
}
