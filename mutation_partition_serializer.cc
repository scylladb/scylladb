
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

#include "utils/UUID.hh"
#include "serializer.hh"
#include "idl/uuid.dist.hh"
#include "idl/keys.dist.hh"
#include "idl/mutation.dist.hh"
#include "serializer_impl.hh"
#include "serialization_visitors.hh"
#include "idl/uuid.dist.impl.hh"
#include "idl/keys.dist.impl.hh"
#include "idl/mutation.dist.impl.hh"

using namespace db;

namespace {

template<typename Writer>
auto write_live_cell(Writer&& writer, const atomic_cell& c)
{
    return std::move(writer).write_created_at(c.timestamp())
                            .write_value(c.value())
                        .end_live_cell();
}

template<typename Writer>
auto write_expiring_cell(Writer&& writer, const atomic_cell& c)
{
    return std::move(writer).write_ttl(c.ttl())
                            .write_expiry(c.expiry())
                            .start_c()
                                .write_created_at(c.timestamp())
                                .write_value(c.value())
                            .end_c()
                        .end_expiring_cell();
}

template<typename Writer>
auto write_dead_cell(Writer&& writer, const atomic_cell& c)
{
    return std::move(writer).start_tomb()
                                .write_timestamp(c.timestamp())
                                .write_deletion_time(c.deletion_time())
                            .end_tomb()
                        .end_dead_cell();
}

template<typename Writer>
auto write_collection_cell(Writer&& collection_writer, collection_mutation_view cmv, const column_definition& def)
{
    auto&& ctype = static_pointer_cast<const collection_type_impl>(def.type);
    auto m_view = ctype->deserialize_mutation_form(cmv);
    auto cells_writer = std::move(collection_writer).write_tomb(m_view.tomb).start_elements();
    for (auto&& c : m_view.cells) {
        auto cell_writer = cells_writer.add().write_key(c.first);
        if (!c.second.is_live()) {
            write_dead_cell(std::move(cell_writer).start_value_dead_cell(), c.second).end_collection_element();
        } else if (c.second.is_live_and_has_ttl()) {
            write_expiring_cell(std::move(cell_writer).start_value_expiring_cell(), c.second).end_collection_element();
        } else {
            write_live_cell(std::move(cell_writer).start_value_live_cell(), c.second).end_collection_element();
        }
    }
    return std::move(cells_writer).end_elements().end_collection_cell();
}

template<typename Writer>
auto write_row_cells(Writer&& writer, const row& r, const schema& s, column_kind kind)
{
    auto column_writer = std::move(writer).start_columns();
    r.for_each_cell([&] (column_id id, const atomic_cell_or_collection& cell) {
        auto& def = s.column_at(kind, id);
        auto cell_or_collection_writer = column_writer.add().write_id(id);
        if (def.is_atomic()) {
            auto&& c = cell.as_atomic_cell();
            auto cell_writer = std::move(cell_or_collection_writer).start_c_variant();
            if (!c.is_live()) {
                write_dead_cell(std::move(cell_writer).start_variant_dead_cell(), c).end_variant().end_column();
            } else if (c.is_live_and_has_ttl()) {
                write_expiring_cell(std::move(cell_writer).start_variant_expiring_cell(), c).end_variant().end_column();
            } else {
                write_live_cell(std::move(cell_writer).start_variant_live_cell(), c).end_variant().end_column();
            }
        } else {
            write_collection_cell(std::move(cell_or_collection_writer).start_c_collection_cell(), cell.as_collection_mutation(), def).end_column();
        }
    });
    return std::move(column_writer).end_columns();
}

template<typename Writer>
auto write_row_marker(Writer&& writer, const row_marker& marker)
{
    if (marker.is_missing()) {
        return std::move(writer).start_marker_no_marker().end_no_marker();
    } else if (!marker.is_live()) {
        return std::move(writer).start_marker_dead_marker()
                                    .start_tomb()
                                        .write_timestamp(marker.timestamp())
                                        .write_deletion_time(marker.deletion_time())
                                    .end_tomb()
                                .end_dead_marker();
    } else if (marker.is_expiring()) {
        return std::move(writer).start_marker_expiring_marker()
                                    .start_lm()
                                        .write_created_at(marker.timestamp())
                                    .end_lm()
                                    .write_ttl(marker.ttl())
                                    .write_expiry(marker.expiry())
                                .end_expiring_marker();
    } else {
        return std::move(writer).start_marker_live_marker()
                                    .write_created_at(marker.timestamp())
                                .end_live_marker();
    }
}

}

template<typename Writer>
void mutation_partition_serializer::write_serialized(Writer&& writer, const schema& s, const mutation_partition& mp)
{
    auto srow_writer = std::move(writer).write_tomb(mp.partition_tombstone()).start_static_row();
    auto row_tombstones = write_row_cells(std::move(srow_writer), mp.static_row(), s, column_kind::static_column).end_static_row().start_range_tombstones();
    for (auto&& rt : mp.row_tombstones()) {
        row_tombstones.add().write_key(rt.prefix()).write_tomb(rt.t()).end_range_tombstone();
    }
    auto clustering_rows = std::move(row_tombstones).end_range_tombstones().start_rows();
    for (auto&& cr : mp.clustered_rows()) {
        auto marker_writer = clustering_rows.add().write_key(cr.key());
        auto deleted_at_writer = write_row_marker(std::move(marker_writer), cr.row().marker());
        auto&& dt = cr.row().deleted_at();
        auto row_writer = std::move(deleted_at_writer).start_deleted_at()
                                                          .write_timestamp(dt.timestamp)
                                                          .write_deletion_time(dt.deletion_time)
                                                      .end_deleted_at()
                                                      .start_cells();
        write_row_cells(std::move(row_writer), cr.row().cells(), s, column_kind::regular_column).end_cells().end_deletable_row();
    }
    std::move(clustering_rows).end_rows().end_mutation_partition();
}

mutation_partition_serializer::mutation_partition_serializer(const schema& schema, const mutation_partition& p)
    : _schema(schema), _p(p)
{ }

void
mutation_partition_serializer::write(bytes_ostream& out) const {
    write(ser::writer_of_mutation_partition(out));
}

void mutation_partition_serializer::write(ser::writer_of_mutation_partition&& wr) const
{
    write_serialized(std::move(wr), _schema, _p);
}
