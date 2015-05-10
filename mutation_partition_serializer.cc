#include "mutation_partition_serializer.hh"
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


size_t
mutation_partition_serializer::size(const schema& schema, const mutation_partition& p) {
    size_t size = 0;
    size += tombstone_serializer(tombstone()).size();

    // static row
    size += sizeof(count_type);
    for (auto&& e : p.static_row()) {
        size += sizeof(column_id);
        size += bytes_view_serializer(e.second.serialize()).size();
    }

    // row tombstones
    size += sizeof(count_type);
    for (const row_tombstones_entry& e : p.row_tombstones()) {
        size += clustering_key_prefix_view_serializer(e.prefix()).size();
        size += tombstone_serializer(e.t()).size();
    }

    // rows
    for (const rows_entry& e : p.clustered_rows()) {
        size += clustering_key_view_serializer(e.key()).size();
        size += sizeof(api::timestamp_type); // e.row().created_at
        size += tombstone_serializer(e.row().t).size();
        size += sizeof(count_type); // e.row().cells.size()
        for (auto&& cell_entry : e.row().cells) {
            size += sizeof(column_id);
            const column_definition& def = schema.regular_column_at(cell_entry.first);
            if (def.is_atomic()) {
                size += atomic_cell_view_serializer(cell_entry.second.as_atomic_cell()).size();
            } else {
                size += collection_mutation_view_serializer(cell_entry.second.as_collection_mutation()).size();
            }
        }
    }

    return size;
}

void
mutation_partition_serializer::write(data_output& out, const schema& schema, const mutation_partition& p) {
    tombstone_serializer::write(out, p.partition_tombstone());

    // static row
    auto n_static_columns = p.static_row().size();
    assert(n_static_columns == (count_type)n_static_columns);
    out.write<count_type>(n_static_columns);

    for (auto&& e : p.static_row()) {
        out.write(e.first);
        bytes_view_serializer::write(out, e.second.serialize());
    }

    // row tombstones
    auto n_tombstones = p.row_tombstones().size();
    assert(n_tombstones == (count_type)n_tombstones);
    out.write<count_type>(n_tombstones);

    for (const row_tombstones_entry& e : p.row_tombstones()) {
        clustering_key_prefix_view_serializer::write(out, e.prefix());
        tombstone_serializer::write(out, e.t());
    }

    // rows
    for (const rows_entry& e : p.clustered_rows()) {
        clustering_key_view_serializer::write(out, e.key());
        out.write(e.row().created_at);
        tombstone_serializer::write(out, e.row().t);
        out.write<count_type>(e.row().cells.size());
        for (auto&& cell_entry : e.row().cells) {
            out.write(cell_entry.first);
            const column_definition& def = schema.regular_column_at(cell_entry.first);
            if (def.is_atomic()) {
                atomic_cell_view_serializer::write(out, cell_entry.second.as_atomic_cell());
            } else {
                collection_mutation_view_serializer::write(out, cell_entry.second.as_collection_mutation());
            }
        }
    }
}
