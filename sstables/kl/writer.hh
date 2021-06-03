/*
 * Copyright (C) 2020-present ScyllaDB
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

#include "sstables/writer_impl.hh"
#include "sstables/writer.hh"

namespace sstables {

class sstable_writer_k_l : public sstable_writer::writer_impl {
    bool _backup;
    bool _leave_unsealed;
    bool _compression_enabled;
    std::unique_ptr<file_writer> _writer;
    shard_id _shard; // Specifies which shard new sstable will belong to.
    write_monitor* _monitor;
    utils::UUID _run_identifier;

    std::unique_ptr<file_writer> _index;
    uint64_t _max_sstable_size;
    bool _tombstone_written;
    // Remember first and last keys, which we need for the summary file.
    std::optional<key> _first_key, _last_key;
    std::optional<key> _partition_key;
    index_sampling_state _index_sampling_state;
    range_tombstone_stream _range_tombstones;

    sstable_version_types _version;

    // _pi_write is used temporarily for building the promoted
    // index (column sample) of one partition when writing a new sstable.
    struct {
        // Unfortunately we cannot output the promoted index directly to the
        // index file because it needs to be prepended by its size.
        bytes_ostream data;
        uint32_t numblocks;
        deletion_time deltime;
        uint64_t block_start_offset;
        uint64_t block_next_start_offset;
        bytes block_first_colname;
        bytes block_last_colname;
        std::optional<range_tombstone_accumulator> tombstone_accumulator;
        const schema* schemap;
        size_t desired_block_size;
    } _pi_write;
private:
    void prepare_file_writer();
    void finish_file_writer();

    void maybe_flush_pi_block(file_writer& out,
            const composite& clustering_key,
            const std::vector<bytes_view>& column_names,
            composite::eoc marker = composite::eoc::none);

    void maybe_flush_pi_block(file_writer& out,
            const composite& clustering_key,
            bytes colname);

    void maybe_write_row_marker(file_writer& out, const schema& schema, const row_marker& marker, const composite& clustering_key);
    void write_clustered_row(file_writer& out, const schema& schema, const clustering_row& clustered_row);
    void write_static_row(file_writer& out, const schema& schema, const row& static_row);
    void write_cell(file_writer& out, atomic_cell_view cell, const column_definition& cdef);
    void write_range_tombstone(file_writer& out, const composite& start, composite::eoc start_marker, const composite& end, composite::eoc end_marker,
                               std::vector<bytes_view> suffix, const tombstone t, const column_mask = column_mask::range_tombstone);
    void write_range_tombstone_bound(file_writer& out, const schema& s, const composite& clustering_element, const std::vector<bytes_view>& column_names, composite::eoc marker = composite::eoc::none);
    void index_tombstone(file_writer& out, const composite& key, range_tombstone&& rt, composite::eoc marker);
    void write_collection(file_writer& out, const composite& clustering_key, const column_definition& cdef, collection_mutation_view collection);
    void maybe_write_row_tombstone(file_writer& out, const composite& key, const clustering_row& clustered_row);
    void write_deletion_time(file_writer& out, const tombstone t);

    void index_and_write_column_name(file_writer& out,
            const composite& clustering,
            const std::vector<bytes_view>& column_names,
            composite::eoc marker = composite::eoc::none);

    void maybe_add_summary_entry(const dht::token& token, bytes_view key);
    uint64_t get_offset() const;
    file_writer index_file_writer(sstable& sst, const io_priority_class& pc);
    // Emits all tombstones which start before pos.
    void drain_tombstones(position_in_partition_view pos);
    void drain_tombstones();
    void write_tombstone(range_tombstone&&);
    void ensure_tombstone_is_written() {
        if (!_tombstone_written) {
            consume(tombstone());
        }
    }
public:
    sstable_writer_k_l(sstable& sst, const schema& s, uint64_t estimated_partitions,
            const sstable_writer_config&, const io_priority_class& pc, shard_id shard = this_shard_id());
    ~sstable_writer_k_l();
    sstable_writer_k_l(sstable_writer_k_l&& o) : writer_impl(o._sst, o._schema, o._pc, o._cfg), _backup(o._backup),
        _leave_unsealed(o._leave_unsealed), _compression_enabled(o._compression_enabled), _writer(std::move(o._writer)),
        _shard(o._shard), _monitor(o._monitor),
        _run_identifier(o._run_identifier),
        _index(std::move(o._index)),
        _max_sstable_size(o._max_sstable_size), _tombstone_written(o._tombstone_written),
        _first_key(std::move(o._first_key)), _last_key(std::move(o._last_key)), _partition_key(std::move(o._partition_key)),
        _index_sampling_state(std::move(o._index_sampling_state)), _range_tombstones(std::move(o._range_tombstones)),
        _version(o._version)
    { }
    void consume_new_partition(const dht::decorated_key& dk) override;
    void consume(tombstone t) override;
    stop_iteration consume(static_row&& sr) override;
    stop_iteration consume(clustering_row&& cr) override;
    stop_iteration consume(range_tombstone&& rt) override;
    stop_iteration consume_end_of_partition() override;
    void consume_end_of_stream() override;
};

} // namespace sstables
