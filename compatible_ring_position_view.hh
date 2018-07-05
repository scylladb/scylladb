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


#pragma once

#include "query-request.hh"
#include <optional>

// Wraps ring_position_view so it is compatible with old-style C++: default
// constructor, stateless comparators, yada yada.
class compatible_ring_position_view {
    const schema* _schema = nullptr;
    // Optional to supply a default constructor, no more.
    std::optional<dht::ring_position_view> _rpv;
public:
    constexpr compatible_ring_position_view() = default;
    compatible_ring_position_view(const schema& s, dht::ring_position_view rpv)
        : _schema(&s), _rpv(rpv) {
    }
    const dht::ring_position_view& position() const {
        return *_rpv;
    }
    friend int tri_compare(const compatible_ring_position_view& x, const compatible_ring_position_view& y) {
        return dht::ring_position_tri_compare(*x._schema, *x._rpv, *y._rpv);
    }
    friend bool operator<(const compatible_ring_position_view& x, const compatible_ring_position_view& y) {
        return tri_compare(x, y) < 0;
    }
    friend bool operator<=(const compatible_ring_position_view& x, const compatible_ring_position_view& y) {
        return tri_compare(x, y) <= 0;
    }
    friend bool operator>(const compatible_ring_position_view& x, const compatible_ring_position_view& y) {
        return tri_compare(x, y) > 0;
    }
    friend bool operator>=(const compatible_ring_position_view& x, const compatible_ring_position_view& y) {
        return tri_compare(x, y) >= 0;
    }
    friend bool operator==(const compatible_ring_position_view& x, const compatible_ring_position_view& y) {
        return tri_compare(x, y) == 0;
    }
    friend bool operator!=(const compatible_ring_position_view& x, const compatible_ring_position_view& y) {
        return tri_compare(x, y) != 0;
    }
};

