/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
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

#include "mutation_partition_view.hh"
#include "schema.hh"
#include "atomic_cell.hh"
#include "db/serializer.hh"
#include "utils/data_input.hh"
#include "mutation_partition_serializer.hh"
#include "mutation_partition.hh"

//
// See mutation_partition_serializer.cc for representation layout.
//

using namespace db;

void
mutation_partition_view::accept(const schema& schema, mutation_partition_visitor& visitor) const {
    data_input in(_bytes);

    visitor.accept_partition_tombstone(tombstone_serializer::read(in));

    // Read static row
    auto n_columns = in.read<mutation_partition_serializer::count_type>();
    while (n_columns-- > 0) {
        auto id = in.read<column_id>();

        if (schema.static_column_at(id).is_atomic()) {
            auto&& v = atomic_cell_view_serializer::read(in);
            visitor.accept_static_cell(id, v);
        } else {
            auto&& v = collection_mutation_view_serializer::read(in);
            visitor.accept_static_cell(id, v);
        }
    }

    // Read row tombstones
    auto n_tombstones = in.read<mutation_partition_serializer::count_type>();
    while (n_tombstones-- > 0) {
        auto&& prefix = clustering_key_prefix_view_serializer::read(in);
        auto&& t = tombstone_serializer::read(in);
        visitor.accept_row_tombstone(prefix, t);
    }

    // Read clustered rows
    while (in.has_next()) {
        auto&& key = clustering_key_view_serializer::read(in);
        auto&& timestamp = in.read<api::timestamp_type>();
        gc_clock::duration ttl;
        gc_clock::time_point expiry;
        if (timestamp != api::missing_timestamp) {
            ttl = gc_clock::duration(in.read<gc_clock::rep>());
            if (ttl.count()) {
                expiry = gc_clock::time_point(gc_clock::duration(in.read<gc_clock::rep>()));
            }
        }
        auto&& deleted_at = tombstone_serializer::read(in);
        visitor.accept_row(key, deleted_at, row_marker(timestamp, ttl, expiry));

        auto n_columns = in.read<mutation_partition_serializer::count_type>();
        while (n_columns-- > 0) {
            auto id = in.read<column_id>();

            if (schema.regular_column_at(id).is_atomic()) {
                auto&& v = atomic_cell_view_serializer::read(in);
                visitor.accept_row_cell(id, v);
            } else {
                auto&& v = collection_mutation_view_serializer::read(in);
                visitor.accept_row_cell(id, v);
            }
        }
    }
}
