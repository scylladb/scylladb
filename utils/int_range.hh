/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/assert.hh"
#include "interval.hh"
#include <seastar/core/print.hh>

#include "seastarx.hh"

using int_range = interval<int>;

inline
unsigned cardinality(const int_range& r) {
    SCYLLA_ASSERT(r.start());
    SCYLLA_ASSERT(r.end());
    return r.end()->value() - r.start()->value() + r.start()->is_inclusive() + r.end()->is_inclusive() - 1;
}

inline
unsigned cardinality(const std::optional<int_range>& ropt) {
    return ropt ? cardinality(*ropt) : 0;
}

inline
std::optional<int_range> intersection(const int_range& a, const int_range& b) {
    auto int_tri_cmp = [] (int x, int y) {
        return x <=> y;
    };
    return a.intersection(b, int_tri_cmp);
}

inline
int_range make_int_range(int start_inclusive, int end_exclusive) {
    if (end_exclusive <= start_inclusive) {
        throw std::runtime_error(format("invalid range: [{:d}, {:d})", start_inclusive, end_exclusive));
    }
    return int_range({start_inclusive}, {end_exclusive - 1});
}
