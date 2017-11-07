/*
 * Copyright (C) 2017 ScyllaDB
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
#include "range_tombstone_list.hh"

class range_tombstone_list_assertions {
    const schema& _s;
    const range_tombstone_list& _list;
public:
    range_tombstone_list_assertions(const schema& s, const range_tombstone_list& list)
        : _s(s), _list(list) {}

    range_tombstone_list_assertions& has_no_less_information_than(const range_tombstone_list& other) {
        auto cpy = _list;
        cpy.apply(_s, other);
        if (!cpy.equal(_s, _list)) {
            BOOST_FAIL(sprint("Expected to include at least what's in %s, but does not: %s", other, _list));
        }
        return *this;
    }

    range_tombstone_list_assertions& is_equal_to(const range_tombstone_list& other) {
        if (!_list.equal(_s, other)) {
            BOOST_FAIL(sprint("Lists differ, expected: %s\n ...but got: %s", other, _list));
        }
        return *this;
    }

    range_tombstone_list_assertions& is_equal_to_either(const range_tombstone_list& list1, const range_tombstone_list& list2) {
        if (!_list.equal(_s, list1) && !_list.equal(_s, list2)) {
            BOOST_FAIL(sprint("Expected to be either %s\n ...or %s\n ...but got: %s", list1, list2, _list));
        }
        return *this;
    }
};

inline
range_tombstone_list_assertions assert_that(const schema& s, const range_tombstone_list& list) {
    return {s, list};
}
