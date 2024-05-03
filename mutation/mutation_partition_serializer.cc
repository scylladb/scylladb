
/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "mutation_partition_serializer.hh"
#include "mutation_partition.hh"

#include "counters.hh"
#include "idl/mutation.dist.hh"
#include "idl/mutation.dist.impl.hh"
#include "frozen_mutation.hh"
#include <seastar/coroutine/maybe_yield.hh>

using namespace db;

namespace {

template<typename Writer>
auto write_live_cell(Writer&& writer, atomic_cell_view c)
{
    return std::move(writer).write_created_at(c.timestamp())
                            .write_fragmented_value(fragment_range(c.value()))
                        .end_live_cell();
}

template<typename Writer>
auto write_counter_cell(Writer&& writer, atomic_cell_view c)
{
    auto value = std::move(writer).write_created_at(c.timestamp());
    return [&c, value = std::move(value)] () mutable {
        if (c.is_counter_update()) {
            auto delta = c.counter_update_value();
            return std::move(value).start_value_counter_cell_update()
                                   .write_delta(delta)
                                   .end_counter_cell_update();
        } else {
            auto ccv = counter_cell_view(c);
            auto shards = std::move(value).start_value_counter_cell_full()
                                          .start_shards();
            for (auto csv : ccv.shards()) {
                shards.add_shards(counter_shard(csv));
            }
            return std::move(shards).end_shards().end_counter_cell_full();
        }
    }().end_counter_cell();
}

template<typename Writer>
auto write_expiring_cell(Writer&& writer, atomic_cell_view c)
{
    return std::move(writer).write_ttl(c.ttl())
                            .write_expiry(c.expiry())
                            .start_c()
                                .write_created_at(c.timestamp())
                                .write_fragmented_value(fragment_range(c.value()))
                            .end_c()
                        .end_expiring_cell();
}

template<typename Writer>
auto write_dead_cell(Writer&& writer, atomic_cell_view c)
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
  return cmv.with_deserialized(*def.type, [&] (collection_mutation_view_description m_view) {
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
  });
}

template<typename Writer>
auto write_row_cells(Writer&& writer, const row& r, const schema& s, column_kind kind)
{
    auto column_writer = std::move(writer).start_columns();
    r.for_each_cell([&] (column_id id, const atomic_cell_or_collection& cell) {
        auto& def = s.column_at(kind, id);
        auto cell_or_collection_writer = column_writer.add().write_id(id);
        if (def.is_atomic()) {
            auto&& c = cell.as_atomic_cell(def);
            auto cell_writer = std::move(cell_or_collection_writer).start_c_variant();
            if (!c.is_live()) {
                write_dead_cell(std::move(cell_writer).start_variant_dead_cell(), c).end_variant().end_column();
            } else if (def.is_counter()) {
                write_counter_cell(std::move(cell_writer).start_variant_counter_cell(), c).end_variant().end_column();
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

template <typename RowTombstones>
static void write_tombstones(const schema& s, RowTombstones& row_tombstones, const range_tombstone_list& rt_list)
{
    for (auto&& rte : rt_list) {
        auto& rt = rte.tombstone();
        row_tombstones.add().write_start(rt.start).write_tomb(rt.tomb).write_start_kind(rt.start_kind)
            .write_end(rt.end).write_end_kind(rt.end_kind).end_range_tombstone();
    }
}

template<typename Writer>
static auto write_tombstone(Writer&& writer, const tombstone& t) {
    return std::move(writer).write_timestamp(t.timestamp).write_deletion_time(t.deletion_time);
}

template<typename Writer>
static auto write_row(Writer&& writer, const schema& s, const clustering_key_prefix& key, const row& cells, const row_marker& m, const row_tombstone& t) {
    auto marker_writer = std::move(writer).write_key(key);
    auto deleted_at_writer = write_row_marker(std::move(marker_writer), m).start_deleted_at();
    auto row_writer = write_tombstone(std::move(deleted_at_writer), t.regular()).end_deleted_at().start_cells();
    auto shadowable_deleted_at_writer = write_row_cells(std::move(row_writer), cells, s, column_kind::regular_column).end_cells().start_shadowable_deleted_at();
    return write_tombstone(std::move(shadowable_deleted_at_writer), t.shadowable().tomb()).end_shadowable_deleted_at();
}

template<typename Writer>
void mutation_partition_serializer::write_serialized(Writer&& writer, const schema& s, const mutation_partition& mp)
{
    auto srow_writer = std::move(writer).write_tomb(mp.partition_tombstone()).start_static_row();
    auto row_tombstones = write_row_cells(std::move(srow_writer), mp.static_row().get(), s, column_kind::static_column).end_static_row().start_range_tombstones();
    write_tombstones(s, row_tombstones, mp.row_tombstones());
    auto clustering_rows = std::move(row_tombstones).end_range_tombstones().start_rows();
    for (auto&& cr : mp.non_dummy_rows()) {
        write_row(clustering_rows.add(), s, cr.key(), cr.row().cells(), cr.row().marker(), cr.row().deleted_at()).end_deletable_row();
    }
    std::move(clustering_rows).end_rows().end_mutation_partition();
}

template<typename Writer>
future<> mutation_partition_serializer::write_serialized_gently(Writer&& writer, const schema& s, const mutation_partition& mp)
{
    auto srow_writer = std::move(writer).write_tomb(mp.partition_tombstone()).start_static_row();
    auto row_tombstones = write_row_cells(std::move(srow_writer), mp.static_row().get(), s, column_kind::static_column).end_static_row().start_range_tombstones();
    for (auto&& rte : mp.row_tombstones()) {
        co_await coroutine::maybe_yield();
        auto& rt = rte.tombstone();
        row_tombstones.add().write_start(rt.start).write_tomb(rt.tomb).write_start_kind(rt.start_kind)
            .write_end(rt.end).write_end_kind(rt.end_kind).end_range_tombstone();
    }
    auto clustering_rows = std::move(row_tombstones).end_range_tombstones().start_rows();
    for (auto&& cr : mp.non_dummy_rows()) {
        co_await coroutine::maybe_yield();
        write_row(clustering_rows.add(), s, cr.key(), cr.row().cells(), cr.row().marker(), cr.row().deleted_at()).end_deletable_row();
    }
    std::move(clustering_rows).end_rows().end_mutation_partition();
}

mutation_partition_serializer::mutation_partition_serializer(const schema& schema, const mutation_partition& p)
    : _schema(schema), _p(p)
{ }

void
mutation_partition_serializer::write(bytes_ostream& out) const {
    write(ser::writer_of_mutation_partition<bytes_ostream>(out));
}

void mutation_partition_serializer::write(ser::writer_of_mutation_partition<bytes_ostream>&& wr) const
{
    write_serialized(std::move(wr), _schema, _p);
}

future<> mutation_partition_serializer::write_gently(bytes_ostream& out) const {
    return write_gently(ser::writer_of_mutation_partition<bytes_ostream>(out));
}

future<> mutation_partition_serializer::write_gently(ser::writer_of_mutation_partition<bytes_ostream>&& wr) const
{
    return write_serialized_gently(std::move(wr), _schema, _p);
}

void serialize_mutation_fragments(const schema& s, tombstone partition_tombstone,
    std::optional<static_row> sr,  range_tombstone_list rts,
    std::deque<clustering_row> crs, ser::writer_of_mutation_partition<bytes_ostream>&& wr)
{
    auto srow_writer = std::move(wr).write_tomb(partition_tombstone).start_static_row();
    auto row_tombstones = [&] {
        if (sr) {
            return write_row_cells(std::move(srow_writer), sr->cells(), s, column_kind::static_column).end_static_row().start_range_tombstones();
        } else {
            return std::move(srow_writer).start_columns().end_columns().end_static_row().start_range_tombstones();
        }
    }();
    sr = { };

    write_tombstones(s, row_tombstones, rts);
    rts.clear();

    auto clustering_rows = std::move(row_tombstones).end_range_tombstones().start_rows();
    while (!crs.empty()) {
        auto& cr = crs.front();
        write_row(clustering_rows.add(), s, cr.key(), cr.cells(), cr.marker(), cr.tomb()).end_deletable_row();
        crs.pop_front();
    }
    std::move(clustering_rows).end_rows().end_mutation_partition();
}

frozen_mutation_fragment freeze(const schema& s, const mutation_fragment& mf)
{
    bytes_ostream out;
    ser::writer_of_mutation_fragment<bytes_ostream> writer(out);
    mf.visit(seastar::make_visitor(
        [&] (const clustering_row& cr) {
            return write_row(std::move(writer).start_fragment_clustering_row().start_row(), s, cr.key(), cr.cells(), cr.marker(), cr.tomb())
                    .end_row()
                .end_clustering_row();
        },
        [&] (const static_row& sr) {
            return write_row_cells(std::move(writer).start_fragment_static_row().start_cells(), sr.cells(), s, column_kind::static_column)
                    .end_cells()
                .end_static_row();
        },
        [&] (const range_tombstone& rt) {
            return std::move(writer).write_fragment_range_tombstone(rt);
        },
        [&] (const partition_start& ps) {
            return std::move(writer).start_fragment_partition_start()
                    .write_key(ps.key().key())
                    .write_partition_tombstone(ps.partition_tombstone())
                .end_partition_start();
        },
        [&] (const partition_end& pe) {
            return std::move(writer).write_fragment_partition_end(pe);
        }
    )).end_mutation_fragment();
    return frozen_mutation_fragment(std::move(out));
}
