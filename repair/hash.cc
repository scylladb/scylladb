/*
 * Copyright (C) 2018 ScyllaDB
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

#include <stdexcept>
#include "repair/hash.hh"
#include "hashing.hh"
#include "mutation_fragment.hh"
#include "range_tombstone.hh"

decorated_key_with_hash::decorated_key_with_hash(const schema& s, dht::decorated_key key, uint64_t seed) : dk(key) {
    xx_hasher h(seed);
    feed_hash(h, dk.key(), s);
    hash = repair_hash(h.finalize_uint64());
}

void fragment_hasher::consume_cell(const column_definition& col, const atomic_cell_or_collection& cell) {
    feed_hash(_hasher, col.kind);
    feed_hash(_hasher, col.id);
    feed_hash(_hasher, cell, col);
}

void fragment_hasher::hash(const mutation_fragment& mf) {
    mf.visit(seastar::make_visitor(
        [&] (const clustering_row& cr) {
            consume(cr);
        },
        [&] (const static_row& sr) {
            consume(sr);
        },
        [&] (const range_tombstone& rt) {
            consume(rt);
        },
        [&] (const partition_start& ps) {
            consume(ps);
        },
        [&] (const partition_end& pe) {
            throw std::runtime_error("partition_end is not expected");
        }
    ));
}

void fragment_hasher::consume(const tombstone& t) {
    feed_hash(_hasher, t);
}

void fragment_hasher::consume(const static_row& sr) {
    sr.cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& cell) {
        auto&& col = _schema.static_column_at(id);
        consume_cell(col, cell);
    });
}

void fragment_hasher::consume(const clustering_row& cr) {
    feed_hash(_hasher, cr.key(), _schema);
    feed_hash(_hasher, cr.tomb());
    feed_hash(_hasher, cr.marker());
    cr.cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& cell) {
        auto&& col = _schema.regular_column_at(id);
        consume_cell(col, cell);
    });
}

void fragment_hasher::consume(const range_tombstone& rt) {
    feed_hash(_hasher, rt.start, _schema);
    feed_hash(_hasher, rt.start_kind);
    feed_hash(_hasher, rt.tomb);
    feed_hash(_hasher, rt.end, _schema);
    feed_hash(_hasher, rt.end_kind);
}

void fragment_hasher::consume(const partition_start& ps) {
    feed_hash(_hasher, ps.key().key(), _schema);
    if (ps.partition_tombstone()) {
        consume(ps.partition_tombstone());
    }
}
