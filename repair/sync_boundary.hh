/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include "dht/i_partitioner.hh"
#include "mutation/position_in_partition.hh"
#include <fmt/ostream.h>

// Represent a position of a mutation_fragment read from a flat mutation
// reader. Repair nodes negotiate a small range identified by two
// repair_sync_boundary to work on in each round.
struct repair_sync_boundary {
    dht::decorated_key pk;
    position_in_partition position;
    class tri_compare {
        dht::ring_position_comparator _pk_cmp;
        position_in_partition::tri_compare _position_cmp;
    public:
        tri_compare(const schema& s) : _pk_cmp(s), _position_cmp(s) { }
        std::strong_ordering operator()(const repair_sync_boundary& a, const repair_sync_boundary& b) const {
            auto ret = _pk_cmp(a.pk, b.pk);
            if (ret == 0) {
                ret = _position_cmp(a.position, b.position);
            }
            return ret;
        }
    };
    friend std::ostream& operator<<(std::ostream& os, const repair_sync_boundary& x) {
        return os << "{ " << x.pk << "," <<  x.position << " }";
    }
};

template <> struct fmt::formatter<repair_sync_boundary> : fmt::ostream_formatter {};
