
/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#define BOOST_TEST_MODULE string_format

#include <boost/test/unit_test.hpp>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <fmt/std.h>

#include "utils/to_string.hh"

// Test scylla's string formatters and printers defined in utils/to_string.hh

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

boost::test_tools::assertion_result use_homebrew_formatter_for_optional(boost::unit_test::test_unit_id) {
    return FMT_VERSION < 100000;
}

// {fmt} >= 10.0.0 provides formatter for optional, and its
// representation is different from our homebrew one:
//            {fmt}      homebrew
// nullopt    none       {}
// "hello"    hello      hello
//
// so ignore this test for {fmt} >= 10.0.0
BOOST_AUTO_TEST_CASE(test_optional_string_format,
                     *boost::unit_test::precondition(use_homebrew_formatter_for_optional)) {
    std::optional<std::string> sopt;

    auto s = fmt::format("{}", sopt);
    BOOST_TEST_MESSAGE(fmt::format("Empty opt: {}", s));
    BOOST_REQUIRE_EQUAL(s.size(), 2);
    verify_parenthesis(s);

    sopt.emplace("foo");
    s = fmt::format("{}", sopt);
    BOOST_TEST_MESSAGE(fmt::format("Engaged opt: {}", s));
}
