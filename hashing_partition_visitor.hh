/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "mutation/mutation_partition_visitor.hh"
#include "utils/hashing.hh"
#include "schema/schema.hh"
#include "mutation/atomic_cell_hash.hh"
#include "mutation/position_in_partition.hh"

// Calculates a hash of a mutation_partition which is consistent with
// mutation equality. For any equal mutations, no matter which schema
// version they were generated under, the hash fed will be the same for both of them.
template<typename Hasher>
class hashing_partition_visitor : public mutation_partition_visitor {
    Hasher& _h;
    const schema& _s;
public:
    hashing_partition_visitor(Hasher& h, const schema& s)
        : _h(h)
        , _s(s)
    { }

    virtual void accept_partition_tombstone(tombstone t) override {
        feed_hash(_h, t);
    }

    virtual void accept_static_cell(column_id id, atomic_cell_view cell) override {
        auto&& col = _s.static_column_at(id);
        feed_hash(_h, col.name());
        feed_hash(_h, col.type->name());
        feed_hash(_h, cell, col);
    }

    virtual void accept_static_cell(column_id id, collection_mutation_view cell) override {
        auto&& col = _s.static_column_at(id);
        feed_hash(_h, col.name());
        feed_hash(_h, col.type->name());
        feed_hash(_h, cell, col);
    }

    virtual void accept_row_tombstone(const range_tombstone& rt) override {
        feed_hash(_h, rt, _s);
    }

    virtual void accept_row(position_in_partition_view pos, const row_tombstone& deleted_at, const row_marker& rm, is_dummy dummy, is_continuous continuous) override {
        if (dummy) {
            return;
        }
        feed_hash(_h, pos.key(), _s);
        feed_hash(_h, deleted_at);
        feed_hash(_h, rm);
    }

    virtual void accept_row_cell(column_id id, atomic_cell_view cell) override {
        auto&& col = _s.regular_column_at(id);
        feed_hash(_h, col.name());
        feed_hash(_h, col.type->name());
        feed_hash(_h, cell, col);
    }

    virtual void accept_row_cell(column_id id, collection_mutation_view cell) override {
        auto&& col = _s.regular_column_at(id);
        feed_hash(_h, col.name());
        feed_hash(_h, col.type->name());
        feed_hash(_h, cell, col);
    }
};
