/*
 * Copyright (C) 2015 ScyllaDB
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

#include "mutation_partition_visitor.hh"
#include "hashing.hh"
#include "schema.hh"
#include "atomic_cell_hash.hh"
#include "keys.hh"

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

    virtual void accept_partition_tombstone(tombstone t) {
        feed_hash(_h, t);
    }

    virtual void accept_static_cell(column_id id, atomic_cell_view cell) {
        auto&& col = _s.static_column_at(id);
        feed_hash(_h, col.name());
        feed_hash(_h, col.type->name());
        feed_hash(_h, cell);
    }

    virtual void accept_static_cell(column_id id, collection_mutation_view cell) {
        auto&& col = _s.static_column_at(id);
        feed_hash(_h, col.name());
        feed_hash(_h, col.type->name());
        feed_hash(cell, _h, col.type);
    }

    virtual void accept_row_tombstone(clustering_key_prefix_view prefix, tombstone t) {
        prefix.feed_hash(_h, _s);
        feed_hash(_h, t);
    }

    virtual void accept_row(clustering_key_view key, tombstone deleted_at, const row_marker& rm) {
        key.feed_hash(_h, _s);
        feed_hash(_h, deleted_at);
        feed_hash(_h, rm);
    }

    virtual void accept_row_cell(column_id id, atomic_cell_view cell) {
        auto&& col = _s.regular_column_at(id);
        feed_hash(_h, col.name());
        feed_hash(_h, col.type->name());
        feed_hash(_h, cell);
    }

    virtual void accept_row_cell(column_id id, collection_mutation_view cell) {
        auto&& col = _s.regular_column_at(id);
        feed_hash(_h, col.name());
        feed_hash(_h, col.type->name());
        feed_hash(cell, _h, col.type);
    }
};
