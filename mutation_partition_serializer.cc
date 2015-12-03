
/*
 * Copyright 2015 Cloudius Systems
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

#include "mutation_partition_serializer.hh"
#include "mutation_partition.hh"
#include "db/serializer.hh"

//
// Representation layout:
//
// <partition>          ::= <tombstone> <static-row> <range-tombstones> <row>*
// <range-tombstones>   ::= <n-tombstones> <range-tombstone>*
// <range-tombstone>    ::= <clustering-key-prefix> <tombstone>
// <row>                ::= <tombstone> <created-at-timestamp> <cells>
// <static-row>         ::= <cells>
// <cells>              ::= <n-cells> <cell>+
// <cell>               ::= <column-id> <value>
// <value>              ::= <atomic-cell> | <collection-mutation>
//
// Notes:
//  - <cell>s in <cells> are sorted by <column-id>
//  - The count of <row>s is inferred from framing.
//
// FIXME: We could make this format more compact by using bitfields
// to mark absence of elements like:
//   - range tombstones (they should be rare)
//   - static row (maybe rare)
//   - row's tombstone (present only when deleting rows) and timestamp
//
// Also atomic cells could be stored more compactly. Right now
// each cell has a one-byte flags field which tells if it's live or not
// and if it has ttl or not. We could condense these flags in a per-row bitmap.

using namespace db;

mutation_partition_serializer::mutation_partition_serializer(const schema& schema, const mutation_partition& p)
    : _schema(schema), _p(p), _size(size(schema, p))
{ }

size_t
mutation_partition_serializer::size(const schema& schema, const mutation_partition& p) {
    size_t size = 0;
    size += tombstone_serializer(tombstone()).size();

    // static row
    size += sizeof(count_type);
    p.static_row().for_each_cell([&] (column_id, const atomic_cell_or_collection& c) {
        size += sizeof(column_id);
        size += bytes_view_serializer(c.serialize()).size();
    });

    // row tombstones
    size += sizeof(count_type);
    for (const row_tombstones_entry& e : p.row_tombstones()) {
        size += clustering_key_prefix_view_serializer(e.prefix()).size();
        size += tombstone_serializer(e.t()).size();
    }

    // rows
    for (const rows_entry& e : p.clustered_rows()) {
        size += clustering_key_view_serializer(e.key()).size();
        size += sizeof(api::timestamp_type); // e.row().marker()._timestamp
        if (!e.row().marker().is_missing()) {
            size += sizeof(gc_clock::duration); // e.row().marker()._ttl
            if (e.row().marker().ttl().count()) {
                size += sizeof(gc_clock::time_point); // e.row().marker()._expiry
            }
        }
        size += tombstone_serializer(e.row().deleted_at()).size();
        size += sizeof(count_type); // e.row().cells.size()
        e.row().cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& c) {
            size += sizeof(column_id);
            const column_definition& def = schema.regular_column_at(id);
            if (def.is_atomic()) {
                size += atomic_cell_view_serializer(c.as_atomic_cell()).size();
            } else {
                size += collection_mutation_view_serializer(c.as_collection_mutation()).size();
            }
        });
    }

    return size;
}

void
mutation_partition_serializer::write(data_output& out) const {
    out.write<size_type>(_size);
    write_without_framing(out);
}

void
mutation_partition_serializer::write_without_framing(data_output& out) const {
    tombstone_serializer::write(out, _p.partition_tombstone());

    // static row
    auto n_static_columns = _p.static_row().size();
    assert(n_static_columns == (count_type)n_static_columns);
    out.write<count_type>(n_static_columns);

    _p.static_row().for_each_cell([&] (column_id id, const atomic_cell_or_collection& c) {
        out.write(id);
        bytes_view_serializer::write(out, c.serialize());
    });

    // row tombstones
    auto n_tombstones = _p.row_tombstones().size();
    assert(n_tombstones == (count_type)n_tombstones);
    out.write<count_type>(n_tombstones);

    for (const row_tombstones_entry& e : _p.row_tombstones()) {
        clustering_key_prefix_view_serializer::write(out, e.prefix());
        tombstone_serializer::write(out, e.t());
    }

    // rows
    for (const rows_entry& e : _p.clustered_rows()) {
        clustering_key_view_serializer::write(out, e.key());
        const auto& rm = e.row().marker();
        out.write(rm.timestamp());
        if (!rm.is_missing()) {
            out.write(rm.ttl().count());
            if (rm.ttl().count()) {
                out.write(rm.expiry().time_since_epoch().count());
            }
        }
        tombstone_serializer::write(out, e.row().deleted_at());
        out.write<count_type>(e.row().cells().size());
        e.row().cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& c) {
            out.write(id);
            const column_definition& def = _schema.regular_column_at(id);
            if (def.is_atomic()) {
                atomic_cell_view_serializer::write(out, c.as_atomic_cell());
            } else {
                collection_mutation_view_serializer::write(out, c.as_collection_mutation());
            }
        });
    }
}

void
mutation_partition_serializer::write(bytes_ostream& out) const {
    out.write<size_type>(_size);
    auto buf = out.write_place_holder(_size);
    data_output data_out((char*)buf, _size);
    write_without_framing(data_out);
}

mutation_partition_view
mutation_partition_serializer::read_as_view(data_input& in) {
    auto size = in.read<size_type>();
    return mutation_partition_view::from_bytes(in.read_view(size));
}

mutation_partition
mutation_partition_serializer::read(data_input& in, schema_ptr s) {
    mutation_partition p(s);
    p.apply(*s, read_as_view(in));
    return p;
}
