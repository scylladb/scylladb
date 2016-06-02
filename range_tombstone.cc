/*
 * Copyright (C) 2016 ScyllaDB
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

#include "range_tombstone.hh"

std::ostream& operator<<(std::ostream& out, const bound_kind k) {
    switch(k) {
    case bound_kind::excl_end:
        return out << "excl end";
    case bound_kind::incl_start:
        return out << "incl start";
    case bound_kind::incl_end:
        return out << "incl end";
    case bound_kind::excl_start:
        return out << "excl start";
    }
    abort();
}

bound_kind invert_kind(bound_kind k) {
    switch(k) {
    case bound_kind::excl_start: return bound_kind::incl_end;
    case bound_kind::incl_start: return bound_kind::excl_end;
    case bound_kind::excl_end:   return bound_kind::incl_start;
    case bound_kind::incl_end:   return bound_kind::excl_start;
    }
    abort();
}

int32_t weight(bound_kind k) {
    switch(k) {
    case bound_kind::excl_end:
    case bound_kind::incl_start:
        return -1;
    case bound_kind::incl_end:
    case bound_kind::excl_start:
        return 1;
    }
    abort();
}

const thread_local clustering_key_prefix bound_view::empty_prefix = clustering_key::make_empty();

std::ostream& operator<<(std::ostream& out, const range_tombstone& rt) {
    if (rt) {
        return out << "{range_tombstone: start=" << rt.start_bound() << ", end=" << rt.end_bound() << ", " << rt.tomb << "}";
    } else {
        return out << "{range_tombstone: none}";
    }
}
