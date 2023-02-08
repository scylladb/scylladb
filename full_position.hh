/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "keys.hh"
#include "mutation/position_in_partition.hh"

struct full_position;

struct full_position_view {
    const partition_key_view partition;
    const position_in_partition_view position;

    full_position_view(const full_position&);
    full_position_view(const partition_key&, const position_in_partition_view);
};

struct full_position {
    partition_key partition;
    position_in_partition position;

    full_position(full_position_view);
    full_position(partition_key, position_in_partition);

    operator full_position_view() {
        return full_position_view(partition, position);
    }

    static std::strong_ordering cmp(const schema& s, const full_position& a, const full_position& b) {
        partition_key::tri_compare pk_cmp(s);
        if (auto res = pk_cmp(a.partition, b.partition); res != 0) {
            return res;
        }
        position_in_partition::tri_compare pos_cmp(s);
        return pos_cmp(a.position, b.position);
    }
};

inline full_position_view::full_position_view(const full_position& fp) : partition(fp.partition), position(fp.position) { }
inline full_position_view::full_position_view(const partition_key& pk, const position_in_partition_view pos) : partition(pk), position(pos) { }

inline full_position::full_position(full_position_view fpv) : partition(fpv.partition), position(fpv.position) { }
inline full_position::full_position(partition_key pk, position_in_partition pos) : partition(std::move(pk)), position(pos) { }
