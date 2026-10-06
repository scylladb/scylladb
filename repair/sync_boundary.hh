/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once
#include "dht/decorated_key.hh"
#include "mutation/position_in_partition.hh"

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
        std::strong_ordering operator()(
                const dht::decorated_key& a_pk, const position_in_partition& a_pos, const dht::decorated_key& b_pk, const position_in_partition& b_pos) const {
            auto ret = _pk_cmp(a_pk, b_pk);
            if (ret == 0) {
                ret = _position_cmp(a_pos, b_pos);
            }
            return ret;
        }
        std::strong_ordering operator()(const repair_sync_boundary& a, const repair_sync_boundary& b) const {
            return (*this)(a.pk, a.position, b.pk, b.position);
        }
    };
};

template <> struct fmt::formatter<repair_sync_boundary> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const repair_sync_boundary& boundary, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{{ {}, {} }}", boundary.pk, boundary.position);
    }
};
