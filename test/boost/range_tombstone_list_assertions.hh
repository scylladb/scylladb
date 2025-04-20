/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <boost/test/unit_test.hpp>
#include "mutation/range_tombstone_list.hh"

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
            BOOST_FAIL(format("Expected to include at least what's in {}, but does not: {}", other, _list));
        }
        return *this;
    }

    range_tombstone_list_assertions& is_equal_to(const range_tombstone_list& other) {
        if (!_list.equal(_s, other)) {
            BOOST_FAIL(format("Lists differ, expected: {}\n ...but got: {}", other, _list));
        }
        return *this;
    }

    range_tombstone_list_assertions& is_equal_to_either(const range_tombstone_list& list1, const range_tombstone_list& list2) {
        if (!_list.equal(_s, list1) && !_list.equal(_s, list2)) {
            BOOST_FAIL(format("Expected to be either {}\n ...or {}\n ...but got: {}", list1, list2, _list));
        }
        return *this;
    }
};

inline
range_tombstone_list_assertions assert_that(const schema& s, const range_tombstone_list& list) {
    return {s, list};
}
