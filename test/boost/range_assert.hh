/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <boost/test/unit_test.hpp>
#include <vector>

template <typename Iterator>
class range_assert {
    using value_type = typename Iterator::value_type;
    Iterator _begin;
    Iterator _end;
public:
    range_assert(Iterator begin, Iterator end)
        : _begin(begin)
        , _end(end)
    { }

    template <typename ValueType>
    range_assert equals(std::vector<ValueType> expected) {
        auto i = _begin;
        auto expected_i = expected.begin();
        while (i != _end && expected_i != expected.end()) {
            BOOST_REQUIRE(*i == *expected_i);
            ++i;
            ++expected_i;
        }
        if (i != _end) {
            BOOST_FAIL("Expected fewer elements");
        }
        if (expected_i != expected.end()) {
            BOOST_FAIL("Expected more elements");
        }
        return *this;
    }
};

template <typename Iterator>
static range_assert<Iterator> assert_that_range(Iterator begin, Iterator end) {
    return { begin, end };
}
