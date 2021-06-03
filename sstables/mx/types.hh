/*
 * Copyright (C) 2018-present ScyllaDB
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

#include "clustering_bounds_comparator.hh"
#include <iosfwd>

namespace sstables {

// This enum corresponds to Origin's ClusteringPrefix.Kind.
// It is a superset of values of the bound_kind enum
// declared in clustering_bounds_comparator.hh
enum class bound_kind_m : uint8_t {
    excl_end = 0,
    incl_start = 1,
    excl_end_incl_start = 2,
    static_clustering = 3,
    clustering = 4,
    incl_end_excl_start = 5,
    incl_end = 6,
    excl_start = 7,
};

inline bool is_bound_kind(bound_kind_m kind) {
    switch (kind) {
    case bound_kind_m::incl_start:
    case bound_kind_m::incl_end:
    case bound_kind_m::excl_start:
    case bound_kind_m::excl_end:
        return true;
    default:
        return false;
    }
}

inline bool is_boundary_between_adjacent_intervals(bound_kind_m kind) {
    switch (kind) {
    case bound_kind_m::excl_end_incl_start:
    case bound_kind_m::incl_end_excl_start:
        return true;
    default:
        return false;
    }
}

inline bool is_start(bound_kind_m kind) {
    switch (kind) {
    case bound_kind_m::incl_start:
    case bound_kind_m::excl_end_incl_start:
    case bound_kind_m::incl_end_excl_start:
    case bound_kind_m::excl_start:
        return true;
    default:
        return false;
    }
}

inline bound_kind to_bound_kind(bound_kind_m kind) {
    assert(is_bound_kind(kind));
    using underlying_type = std::underlying_type_t<bound_kind_m>;
    return bound_kind{static_cast<underlying_type>(kind)};
}

inline bound_kind_m to_bound_kind_m(bound_kind kind) {
    using underlying_type = std::underlying_type_t<bound_kind>;
    return bound_kind_m{static_cast<underlying_type>(kind)};
}

inline bound_kind boundary_to_start_bound(bound_kind_m kind) {
    assert(is_boundary_between_adjacent_intervals(kind));
    return (kind == bound_kind_m::incl_end_excl_start) ? bound_kind::excl_start : bound_kind::incl_start;
}

inline bound_kind boundary_to_end_bound(bound_kind_m kind) {
    assert(is_boundary_between_adjacent_intervals(kind));
    return (kind == bound_kind_m::incl_end_excl_start) ? bound_kind::incl_end : bound_kind::excl_end;
}

std::ostream& operator<<(std::ostream& out, sstables::bound_kind_m kind);

}
