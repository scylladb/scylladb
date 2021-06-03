/*
 * Copyright (C) 2019-present ScyllaDB
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
#include <variant>

// Wraps ring_position_view so it is compatible with old-style C++: default
// constructor, stateless comparators, yada yada.
class compatible_ring_position_view {
    const ::schema* _schema = nullptr;
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
    const ::schema& schema() const {
        return *_schema;
    }
    friend std::strong_ordering tri_compare(const compatible_ring_position_view& x, const compatible_ring_position_view& y) {
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

// Wraps ring_position so it is compatible with old-style C++: default
// constructor, stateless comparators, yada yada.
class compatible_ring_position {
    schema_ptr _schema;
    // Optional to supply a default constructor, no more.
    std::optional<dht::ring_position> _rp;
public:
    constexpr compatible_ring_position() = default;
    compatible_ring_position(schema_ptr s, dht::ring_position rp)
        : _schema(std::move(s)), _rp(std::move(rp)) {
    }
    dht::ring_position_view position() const {
        return *_rp;
    }
    const ::schema& schema() const {
        return *_schema;
    }
    friend std::strong_ordering tri_compare(const compatible_ring_position& x, const compatible_ring_position& y) {
        return dht::ring_position_tri_compare(*x._schema, *x._rp, *y._rp);
    }
    friend bool operator<(const compatible_ring_position& x, const compatible_ring_position& y) {
        return tri_compare(x, y) < 0;
    }
    friend bool operator<=(const compatible_ring_position& x, const compatible_ring_position& y) {
        return tri_compare(x, y) <= 0;
    }
    friend bool operator>(const compatible_ring_position& x, const compatible_ring_position& y) {
        return tri_compare(x, y) > 0;
    }
    friend bool operator>=(const compatible_ring_position& x, const compatible_ring_position& y) {
        return tri_compare(x, y) >= 0;
    }
    friend bool operator==(const compatible_ring_position& x, const compatible_ring_position& y) {
        return tri_compare(x, y) == 0;
    }
    friend bool operator!=(const compatible_ring_position& x, const compatible_ring_position& y) {
        return tri_compare(x, y) != 0;
    }
};

// Wraps ring_position or ring_position_view so either is compatible with old-style C++: default
// constructor, stateless comparators, yada yada.
// The motivations for supporting both types are to make containers self-sufficient by not relying
// on callers to keep ring position alive, allow lookup on containers that don't support different
// key types, and also avoiding unnecessary copies.
class compatible_ring_position_or_view {
    // Optional to supply a default constructor, no more.
    std::optional<std::variant<compatible_ring_position, compatible_ring_position_view>> _crp_or_view;
public:
    constexpr compatible_ring_position_or_view() = default;
    explicit compatible_ring_position_or_view(schema_ptr s, dht::ring_position rp)
        : _crp_or_view(compatible_ring_position(std::move(s), std::move(rp))) {
    }
    explicit compatible_ring_position_or_view(const schema& s, dht::ring_position_view rpv)
        : _crp_or_view(compatible_ring_position_view(s, rpv)) {
    }
    dht::ring_position_view position() const {
        struct rpv_accessor {
            dht::ring_position_view operator()(const compatible_ring_position& crp) {
                return crp.position();
            }
            dht::ring_position_view operator()(const compatible_ring_position_view& crpv) {
                return crpv.position();
            }
        };
        return std::visit(rpv_accessor{}, *_crp_or_view);
    }
    friend std::strong_ordering tri_compare(const compatible_ring_position_or_view& x, const compatible_ring_position_or_view& y) {
        struct schema_accessor {
            const ::schema& operator()(const compatible_ring_position& crp) {
                return crp.schema();
            }
            const ::schema& operator()(const compatible_ring_position_view& crpv) {
                return crpv.schema();
            }
        };
        return dht::ring_position_tri_compare(std::visit(schema_accessor{}, *x._crp_or_view), x.position(), y.position());
    }
    friend bool operator<(const compatible_ring_position_or_view& x, const compatible_ring_position_or_view& y) {
        return tri_compare(x, y) < 0;
    }
    friend bool operator<=(const compatible_ring_position_or_view& x, const compatible_ring_position_or_view& y) {
        return tri_compare(x, y) <= 0;
    }
    friend bool operator>(const compatible_ring_position_or_view& x, const compatible_ring_position_or_view& y) {
        return tri_compare(x, y) > 0;
    }
    friend bool operator>=(const compatible_ring_position_or_view& x, const compatible_ring_position_or_view& y) {
        return tri_compare(x, y) >= 0;
    }
    friend bool operator==(const compatible_ring_position_or_view& x, const compatible_ring_position_or_view& y) {
        return tri_compare(x, y) == 0;
    }
    friend bool operator!=(const compatible_ring_position_or_view& x, const compatible_ring_position_or_view& y) {
        return tri_compare(x, y) != 0;
    }
};
