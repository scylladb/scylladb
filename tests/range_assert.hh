/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
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
