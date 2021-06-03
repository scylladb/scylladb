/*
 * Copyright (C) 2017-present ScyllaDB
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

#include "range.hh"
#include <seastar/core/print.hh>

#include "seastarx.hh"

using int_range = nonwrapping_range<int>;

inline
unsigned cardinality(const int_range& r) {
    assert(r.start());
    assert(r.end());
    return r.end()->value() - r.start()->value() + r.start()->is_inclusive() + r.end()->is_inclusive() - 1;
}

inline
unsigned cardinality(const std::optional<int_range>& ropt) {
    return ropt ? cardinality(*ropt) : 0;
}

inline
std::optional<int_range> intersection(const int_range& a, const int_range& b) {
    auto int_tri_cmp = [] (int x, int y) {
        return x < y ? -1 : (x > y ? 1 : 0);
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
