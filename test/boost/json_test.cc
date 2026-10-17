
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

BOOST_AUTO_TEST_CASE(test_exact_print) {
    const std::vector<std::string_view> input = {
        R"({"plain":"abcdefghijklmnopqrstuvwxyz","number":123,"boolean":true})",
        R"({"escaped":"\"\\\b\f\n\r\t\u0001"})",
        R"({"unicode":"chào mọi người","array":[null,false,1.5]})",
        R"({"expanded":1e20})",
    };

    for (const auto json : input) {
        auto value = rjson::parse(json);
        auto serialized_size = rjson::measure_serialized_size(value);
        auto exact = rjson::print_exact(value, serialized_size);
        BOOST_CHECK_EQUAL(exact, rjson::print(value));
        BOOST_CHECK_EQUAL(exact, rjson::print_exact(value));
        BOOST_CHECK_EQUAL(serialized_size.value, exact.size());
        BOOST_CHECK_LE(serialized_size.value, json.size() * 8);
    }

    auto expanded = rjson::parse(R"({"expanded":1e20})");
    BOOST_CHECK_EQUAL(rjson::print_exact(expanded), R"({"expanded":100000000000000000000.0})");
}

BOOST_AUTO_TEST_CASE(test_parse_with_memory_usage) {
    std::string input = R"({"values":[)";
    constexpr size_t value_count = 10000;
    for (size_t i = 0; i < value_count; ++i) {
        if (i) {
            input += ',';
        }
        input += '0';
    }
    input += "]}";

    rjson::chunked_content content;
    content.emplace_back(temporary_buffer<char>::copy_of(input));
    auto parsed = rjson::parse_with_memory_usage(std::move(content));

    BOOST_CHECK_EQUAL(rjson::print_exact(parsed.value), input);
    BOOST_CHECK_GT(parsed.memory_usage, input.size() * 2);
}
