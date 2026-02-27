
/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
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
