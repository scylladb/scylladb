
/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#define BOOST_TEST_MODULE string_format

#include <boost/test/unit_test.hpp>

#include <array>
#include <vector>
#include <list>
#include <deque>
#include <set>
#include <unordered_set>
#include <map>
#include <unordered_map>
#include <initializer_list>

#include <boost/range/adaptors.hpp>
#include <boost/range/adaptor/transformed.hpp>

#include <fmt/format.h>

#include <seastar/core/print.hh>

#include "utils/to_string.hh"
#include "utils/small_vector.hh"
#include "utils/chunked_vector.hh"
#include "bytes.hh"

// Test scylla's string formatters and printers defined in utils/to_string.hh

namespace {

std::string_view trim(std::string_view sv) {
    auto it = sv.begin();
    auto end = sv.end();
    while (it != end && *it == ' ') {
        ++it;
    }
    return std::string_view(it, end);
}

std::string_view cmp_and_remove_prefix(std::string_view sv, std::string_view expected) {
    BOOST_TEST_MESSAGE(fmt::format("cmp_and_remove_prefix: {} expected='{}'", sv, expected));
    trim(sv);
    BOOST_REQUIRE(sv.starts_with(expected));
    auto sz = expected.size();
    sv = sv.substr(sz, sv.size() - sz);
    return trim(sv);
}

// Verify that the formatted string begins and ends
// with a matching pair of supported perenthesis.
void verify_parenthesis(std::string_view sv) {
    static std::unordered_map<char, char> paren_map = {{'{', '}'}, {'[', ']'}, {'(', ')'}, {'<', '>'}};

    BOOST_REQUIRE(!sv.empty());
    char open = sv.front();
    char close = sv.back();
    auto it = paren_map.find(open);
    if (it == paren_map.end()) {
        BOOST_FAIL(fmt::format("Unexpected delimiters: '{}' '{}'", open, close));
    }
    BOOST_REQUIRE_EQUAL(close, it->second);
}

// Verify that formatting a range using seastar::format is compatible
// with formatting using fmt::format,
// And that it produces a valid list, separated
// by the `expected_delim` and has parenthesis iff `expect_parenthesis` is set.
// The output is compared the ordered vector of `expected_strings`.
template <std::ranges::range Range>
void test_format_range(const char* desc, Range x, std::vector<std::string> expected_strings, std::string expected_delim = ",", bool expect_parenthesis = true) {
    auto str = seastar::format("{}", x);
    BOOST_TEST_MESSAGE(fmt::format("{}: {}", desc, str));

    auto fmt_str = fmt::format("{}", x);
    BOOST_REQUIRE_EQUAL(str, fmt_str);

    auto fmt_to_string = fmt::to_string(x);
    BOOST_REQUIRE_EQUAL(str, fmt_to_string);

    size_t num_elements = expected_strings.size();

    size_t paren_size = expect_parenthesis ? 2 : 0;
    size_t min_size = paren_size + (x.begin() == x.end() ? 0 : (num_elements - 1));
    BOOST_REQUIRE_GE(str.size(), min_size);

    std::string_view sv = str;
    if (expect_parenthesis) {
        verify_parenthesis(sv);
        sv = sv.substr(1, sv.size() - 2);
    }

    bool first = true;
    while (!expected_strings.empty()) {
        sv = trim(sv);
        if (!std::exchange(first, false)) {
            sv = cmp_and_remove_prefix(sv, expected_delim);
        }

        auto s = *expected_strings.begin();
        sv = cmp_and_remove_prefix(sv, s);
        expected_strings.erase(expected_strings.begin());
    }

    BOOST_REQUIRE(sv.empty());
}

// Verify that formatting a range using seastar::format is compatible
// with formatting using fmt::format,
// And that it produces a valid list, separated
// by the `expected_delim` and has parenthesis iff `expect_parenthesis` is set.
// The output is compared the unordered set of `expected_strings`.
template <std::ranges::range Range>
void test_format_range(const char* desc, Range x, std::unordered_set<std::string> expected_strings, std::string expected_delim = ",", bool expect_parenthesis = true) {
    auto str = seastar::format("{}", x);
    BOOST_TEST_MESSAGE(fmt::format("{}: {}", desc, str));

    auto fmt_str = fmt::format("{}", x);
    BOOST_REQUIRE_EQUAL(str, fmt_str);

    auto fmt_to_string = fmt::to_string(x);
    BOOST_REQUIRE_EQUAL(str, fmt_to_string);

    size_t num_elements = expected_strings.size();

    size_t paren_size = expect_parenthesis ? 2 : 0;
    size_t min_size = paren_size + (x.empty() ? 0 : (num_elements - 1));
    BOOST_REQUIRE_GE(str.size(), min_size);

    std::string_view sv = str;
    if (expect_parenthesis) {
        verify_parenthesis(sv);
        sv = sv.substr(1, sv.size() - 2);
    }

    bool first = true;
    while (!expected_strings.empty()) {
        sv = trim(sv);
        if (!std::exchange(first, false)) {
            sv = cmp_and_remove_prefix(sv, expected_delim);
        }

        for (auto it = expected_strings.begin(); it != expected_strings.end(); ++it) {
            if (sv.starts_with(*it)) {
                sv = cmp_and_remove_prefix(sv, *it);
                expected_strings.erase(it);
                break;
            }
        }
    }

    BOOST_REQUIRE(sv.empty());
}

} // namespace

BOOST_AUTO_TEST_CASE(test_vector_format) {
    auto ints = {1, 2, 3};
    auto ordered_strings = std::vector<std::string>({"1", "2", "3"});
    auto unordered_strings = std::unordered_set<std::string>(ordered_strings.begin(), ordered_strings.end());

    auto vector = std::vector<int>(ints);
    test_format_range("vector", vector, ordered_strings);

    auto array = std::array<int, 3>({1, 2, 3});
    test_format_range("array", array, ordered_strings);

    auto list = std::list<int>(ints);
    test_format_range("list", list, ordered_strings);

    auto deque = std::deque<int>(ints);
    test_format_range("deque", deque, ordered_strings);

    auto set = std::set<int>(ints);
    test_format_range("set", set, ordered_strings);

    auto unordered_set = std::unordered_set<int>(ints);
    test_format_range("unordered_set", unordered_set, unordered_strings);

    auto small_vector = utils::small_vector<int, 1>(ints);
    test_format_range("small_vector", small_vector, ordered_strings);

    auto chunked_vector = boost::copy_range<utils::chunked_vector<int, 131072>>(ints);
    test_format_range("chunked_vector", chunked_vector, ordered_strings);

    test_format_range("initializer_list", std::initializer_list<std::string>{"1", "2", "3"}, ordered_strings);

    auto map = std::map<int, std::string>({{1, "one"}, {2, "two"}, {3, "three"}});
    auto ordered_map_strings = std::vector<std::string>({"{1, one}", "{2, two}", "{3, three}"});
    auto ordered_map_values = std::vector<std::string>({"one", "two", "three"});
    test_format_range("map", map, ordered_map_strings);
    test_format_range("map | boost::adaptors::map_keys", map | boost::adaptors::map_keys, ordered_strings);
    test_format_range("map | boost::adaptors::map_values", map | boost::adaptors::map_values, ordered_map_values);

    auto unordered_map = std::unordered_map<int, std::string>(map.begin(), map.end());
    // seastar has a specialized print function for unordered_map
    // See https://github.com/scylladb/seastar/issues/1544
    auto unordered_map_strings = std::unordered_set<std::string>({"{1 -> one}", "{2 -> two}", "{3 -> three}"});
    auto unordered_map_values = std::unordered_set<std::string>(ordered_map_values.begin(), ordered_map_values.end());
    test_format_range("unordered_map", unordered_map, unordered_map_strings);
    test_format_range("unordered_map | boost::adaptors::map_keys", unordered_map | boost::adaptors::map_keys, unordered_strings);
    test_format_range("unordered_map | boost::adaptors::map_values", unordered_map | boost::adaptors::map_values, unordered_map_values);
}

BOOST_AUTO_TEST_CASE(test_string_format) {
    seastar::sstring sstring = "foo";
    auto formatted = fmt::format("{}", sstring);
    BOOST_REQUIRE_EQUAL(formatted, sstring);

    std::string std_string = "foo";
    formatted = fmt::format("{}", std_string);
    BOOST_REQUIRE_EQUAL(formatted, std_string);

    std::string_view std_string_view = std_string;
    formatted = fmt::format("{}", std_string_view);
    BOOST_REQUIRE_EQUAL(formatted, std_string_view);
}

BOOST_AUTO_TEST_CASE(test_bytes_format) {
    auto b = to_bytes("f0");
    auto formatted = fmt::format("{}", b);
    auto expected = to_hex(b);
    BOOST_REQUIRE_EQUAL(formatted, expected);
}

BOOST_AUTO_TEST_CASE(test_optional_string_format) {
    std::optional<std::string> sopt;

    auto s = fmt::format("{}", sopt);
    BOOST_TEST_MESSAGE(fmt::format("Empty opt: {}", s));
    BOOST_REQUIRE_EQUAL(s.size(), 2);
    verify_parenthesis(s);

    sopt.emplace("foo");
    s = fmt::format("{}", sopt);
    BOOST_TEST_MESSAGE(fmt::format("Engaged opt: {}", s));
}

BOOST_AUTO_TEST_CASE(test_fs_path_format) {
    auto str_path = "/foo/bar";
    auto fs_path = std::filesystem::path(str_path);
    auto formatted = fmt::format("{}", fs_path);
    // fs::path printer quotes the path
    auto expected = fmt::format("\"{}\"", str_path);
    BOOST_REQUIRE_EQUAL(formatted, expected);
}

BOOST_AUTO_TEST_CASE(test_boost_transformed_range_format) {
    auto v = std::vector<int>({1, 2, 3});

    test_format_range("boost::adaptors::transformed", v | boost::adaptors::transformed([] (int i) { return fmt::format("{}", i * 11); }),
        std::vector<std::string>({"11", "22", "33"}));

    auto sv = utils::small_vector<int, 3>({1, 2, 3});
    test_format_range("transformed vector", sv | boost::adaptors::transformed([] (int i) { return fmt::format("/{}", i); }),
        std::vector<std::string>({"/1", "/2", "/3"}));
}
