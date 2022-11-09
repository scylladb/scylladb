/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#pragma once

#include "dht/i_partitioner.hh"

// Wraps ring_position or ring_position_view so either is compatible with old-style C++: default
// constructor, stateless comparators, yada yada.
// The motivations for supporting both types are to make containers self-sufficient by not relying
// on callers to keep ring position alive, allow lookup on containers that don't support different
// key types, and also avoiding unnecessary copies.
class compatible_ring_position_or_view {
    schema_ptr _schema;
    lw_shared_ptr<dht::ring_position> _rp;
    dht::ring_position_view_opt _rpv; // optional only for default ctor, nothing more
public:
    compatible_ring_position_or_view() = default;
    explicit compatible_ring_position_or_view(schema_ptr s, dht::ring_position rp)
        : _schema(std::move(s)), _rp(make_lw_shared<dht::ring_position>(std::move(rp))), _rpv(dht::ring_position_view(*_rp)) {
    }
    explicit compatible_ring_position_or_view(const schema& s, dht::ring_position_view rpv)
        : _schema(s.shared_from_this()), _rpv(rpv) {
    }
    const dht::ring_position_view& position() const {
        return *_rpv;
    }
    friend std::strong_ordering tri_compare(const compatible_ring_position_or_view& x, const compatible_ring_position_or_view& y) {
        return dht::ring_position_tri_compare(*x._schema, x.position(), y.position());
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
