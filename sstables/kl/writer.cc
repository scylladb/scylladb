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

#include "log.hh"
#include "sstables/writer.hh"
#include "sstables/kl/writer.hh"

namespace sstables {

namespace {

thread_local reader_concurrency_semaphore reader_semaphore(reader_concurrency_semaphore::no_limits{}, "kx/lx writer");

}

extern logging::logger sstlog;

static composite::eoc bound_kind_to_start_marker(bound_kind start_kind) {
    return start_kind == bound_kind::excl_start
         ? composite::eoc::end
         : composite::eoc::start;
}

static composite::eoc bound_kind_to_end_marker(bound_kind end_kind) {
    return end_kind == bound_kind::excl_end
         ? composite::eoc::start
         : composite::eoc::end;
}

void sstable_writer_k_l::write_range_tombstone_bound(file_writer& out,
        const schema& s,
        const composite& clustering_element,
        const std::vector<bytes_view>& column_names,
        composite::eoc marker) {
    write_column_name(_version, out, s, clustering_element, column_names, marker);
}

static void output_promoted_index_entry(bytes_ostream& promoted_index,
        const bytes& first_col,
        const bytes& last_col,
        uint64_t offset, uint64_t width) {
    char s[2];
    write_be(s, uint16_t(first_col.size()));
    promoted_index.write(s, 2);
    promoted_index.write(first_col);
    write_be(s, uint16_t(last_col.size()));
    promoted_index.write(s, 2);
    promoted_index.write(last_col);
    char q[8];
    write_be(q, uint64_t(offset));
    promoted_index.write(q, 8);
    write_be(q, uint64_t(width));
    promoted_index.write(q, 8);
}

// Call maybe_flush_pi_block() before writing the given sstable atom to the
// output. This may start a new promoted-index block depending on how much
// data we've already written since the start of the current block. Starting
// a new block involves both outputting the range of the old block to the
// index file, and outputting again the currently-open range tombstones to
// the data file.
// TODO: currently, maybe_flush_pi_block serializes the column name on every
// call, saving it in _pi_write.block_last_colname which we need for closing
// each block, as well as for closing the last block. We could instead save
// just the unprocessed arguments, and serialize them only when needed at the
// end of the block. For this we would need this function to take rvalue
// references (so data is moved in), and need not to use vector of byte_view
// (which might be gone later).
void sstable_writer_k_l::maybe_flush_pi_block(file_writer& out,
        const composite& clustering_key,
        const std::vector<bytes_view>& column_names,
        composite::eoc marker) {
    if (!_schema.clustering_key_size()) {
        return;
    }
    bytes_writer_for_column_name w;
    write_column_name(_version, w, _schema, clustering_key, column_names, marker);
    maybe_flush_pi_block(out, clustering_key, std::move(w).release());
}

// Overload can only be called if the schema has clustering keys.
void sstable_writer_k_l::maybe_flush_pi_block(file_writer& out,
        const composite& clustering_key,
        bytes colname) {
    if (_pi_write.block_first_colname.empty()) {
        // This is the first column in the partition, or first column since we
        // closed a promoted-index block. Remember its name and position -
        // we'll need to write it to the promoted index.
        _pi_write.block_start_offset = out.offset();
        _pi_write.block_next_start_offset = out.offset() + _pi_write.desired_block_size;
        _pi_write.block_first_colname = colname;
        _pi_write.block_last_colname = std::move(colname);
    } else if (out.offset() >= _pi_write.block_next_start_offset) {
        // If we wrote enough bytes to the partition since we output a sample
        // to the promoted index, output one now and start a new one.
        output_promoted_index_entry(_pi_write.data,
                _pi_write.block_first_colname,
                _pi_write.block_last_colname,
                _pi_write.block_start_offset - _c_stats.start_offset,
                out.offset() - _pi_write.block_start_offset);
        _pi_write.numblocks++;
        _pi_write.block_start_offset = out.offset();
        // Because the new block can be read without the previous blocks, we
        // need to repeat the range tombstones which are still open.
        // Note that block_start_offset is before outputting those (so the new
        // block includes them), but we set block_next_start_offset after - so
        // even if we wrote a lot of open tombstones, we still get a full
        // block size of new data.
        auto& rts = _pi_write.tombstone_accumulator->range_tombstones_for_row(
                clustering_key_prefix::from_range(clustering_key.values()));
        for (const auto& rt : rts) {
            auto start = composite::from_clustering_element(*_pi_write.schemap, rt.start);
            auto end = composite::from_clustering_element(*_pi_write.schemap, rt.end);
            write_range_tombstone(out,
                    start, bound_kind_to_start_marker(rt.start_kind),
                    end, bound_kind_to_end_marker(rt.end_kind),
                    {}, rt.tomb);
        }
        _pi_write.block_next_start_offset = out.offset() + _pi_write.desired_block_size;
        _pi_write.block_first_colname = colname;
        _pi_write.block_last_colname = std::move(colname);
    } else {
        // Keep track of the last column in the partition - we'll need it to close
        // the last block in the promoted index, unfortunately.
        _pi_write.block_last_colname = std::move(colname);
    }
}

static inline void update_cell_stats(column_stats& c_stats, api::timestamp_type timestamp) {
    c_stats.update_timestamp(timestamp);
    c_stats.cells_count++;
}

// Intended to write all cell components that follow column name.
void sstable_writer_k_l::write_cell(file_writer& out, atomic_cell_view cell, const column_definition& cdef) {
    api::timestamp_type timestamp = cell.timestamp();

    update_cell_stats(_c_stats, timestamp);

    if (cell.is_dead(_sst._now)) {
        // tombstone cell

        _sst.get_stats().on_cell_tombstone_write();
        column_mask mask = column_mask::deletion;
        uint32_t deletion_time_size = sizeof(uint32_t);
        uint32_t deletion_time = gc_clock::as_int32(cell.deletion_time());

        _c_stats.update_local_deletion_time_and_tombstone_histogram(deletion_time);

        write(_version, out, mask, timestamp, deletion_time_size, deletion_time);
        return;
    }

    _sst.get_stats().on_cell_write();
    if (cdef.is_counter()) {
        // counter cell
        assert(!cell.is_counter_update());

        column_mask mask = column_mask::counter;
        write(_version, out, mask, int64_t(0), timestamp);

        auto ccv = counter_cell_view(cell);
        write_counter_value(ccv, out, _version, [v = _version] (file_writer& out, uint32_t value) {
            return write(v, out, value);
        });

        _c_stats.update_local_deletion_time(std::numeric_limits<int>::max());
    } else if (cell.is_live_and_has_ttl()) {
        // expiring cell

        column_mask mask = column_mask::expiration;
        uint32_t ttl = gc_clock::as_int32(cell.ttl());
        uint32_t expiration = gc_clock::as_int32(cell.expiry());
        disk_data_value_view<uint32_t> cell_value { cell.value() };

        // tombstone histogram is updated with expiration time because if ttl is longer
        // than gc_grace_seconds for all data, sstable will be considered fully expired
        // when actually nothing is expired.
        _c_stats.update_local_deletion_time_and_tombstone_histogram(expiration);

        write(_version, out, mask, ttl, expiration, timestamp, cell_value);
    } else {
        // regular cell

        column_mask mask = column_mask::none;
        disk_data_value_view<uint32_t> cell_value { cell.value() };

        _c_stats.update_local_deletion_time(std::numeric_limits<int>::max());

        write(_version, out, mask, timestamp, cell_value);
    }
}

void sstable_writer_k_l::maybe_write_row_marker(file_writer& out, const schema& schema, const row_marker& marker, const composite& clustering_key) {
    if (!schema.is_compound() || schema.is_dense() || marker.is_missing()) {
        return;
    }
    // Write row mark cell to the beginning of clustered row.
    index_and_write_column_name(out, clustering_key, { bytes_view() });
    uint64_t timestamp = marker.timestamp();
    uint32_t value_length = 0;

    update_cell_stats(_c_stats, timestamp);

    if (marker.is_dead(_sst._now)) {
        column_mask mask = column_mask::deletion;
        uint32_t deletion_time_size = sizeof(uint32_t);
        uint32_t deletion_time = gc_clock::as_int32(marker.deletion_time());

        _c_stats.update_local_deletion_time_and_tombstone_histogram(deletion_time);

        write(_version, out, mask, timestamp, deletion_time_size, deletion_time);
    } else if (marker.is_expiring()) {
        column_mask mask = column_mask::expiration;
        uint32_t ttl = gc_clock::as_int32(marker.ttl());
        uint32_t expiration = gc_clock::as_int32(marker.expiry());

        _c_stats.update_local_deletion_time_and_tombstone_histogram(expiration);

        write(_version, out, mask, ttl, expiration, timestamp, value_length);
    } else {
        column_mask mask = column_mask::none;
        write(_version, out, mask, timestamp, value_length);
    }
}

void sstable_writer_k_l::write_deletion_time(file_writer& out, const tombstone t) {
    uint64_t timestamp = t.timestamp;
    uint32_t deletion_time = gc_clock::as_int32(t.deletion_time);

    update_cell_stats(_c_stats, timestamp);
    _c_stats.update_local_deletion_time_and_tombstone_histogram(deletion_time);

    write(_version, out, deletion_time, timestamp);
}

void sstable_writer_k_l::index_tombstone(file_writer& out, const composite& key, range_tombstone&& rt, composite::eoc marker) {
    maybe_flush_pi_block(out, key, {}, marker);
    // Remember the range tombstone so when we need to open a new promoted
    // index block, we can figure out which ranges are still open and need
    // to be repeated in the data file. Note that apply() also drops ranges
    // already closed by rt.start, so the accumulator doesn't grow boundless.
    _pi_write.tombstone_accumulator->apply(std::move(rt));
}

void sstable_writer_k_l::maybe_write_row_tombstone(file_writer& out, const composite& key, const clustering_row& clustered_row) {
    auto t = clustered_row.tomb();
    if (!t) {
        return;
    }
    auto rt = range_tombstone(clustered_row.key(), bound_kind::incl_start, clustered_row.key(), bound_kind::incl_end, t.tomb());
    index_tombstone(out, key, std::move(rt), composite::eoc::none);
    write_range_tombstone(out, key, composite::eoc::start, key, composite::eoc::end, {}, t.regular());
    if (t.is_shadowable()) {
        write_range_tombstone(out, key, composite::eoc::start, key, composite::eoc::end, {}, t.shadowable().tomb(), column_mask::shadowable);
    }
}

void sstable_writer_k_l::write_range_tombstone(file_writer& out,
        const composite& start,
        composite::eoc start_marker,
        const composite& end,
        composite::eoc end_marker,
        std::vector<bytes_view> suffix,
        const tombstone t,
        column_mask mask) {
    if (!_schema.is_compound() && (start_marker == composite::eoc::end || end_marker == composite::eoc::start)) {
        throw std::logic_error(format("Cannot represent marker type in range tombstone for non-compound schemas"));
    }
    write_range_tombstone_bound(out, _schema, start, suffix, start_marker);
    write(_version, out, mask);
    write_range_tombstone_bound(out, _schema, end, suffix, end_marker);
    write_deletion_time(out, t);
}

void sstable_writer_k_l::write_collection(file_writer& out, const composite& clustering_key, const column_definition& cdef, collection_mutation_view collection) {
  collection.with_deserialized(*cdef.type, [&] (collection_mutation_view_description mview) {
    const bytes& column_name = cdef.name();
    if (mview.tomb) {
        write_range_tombstone(out, clustering_key, composite::eoc::start, clustering_key, composite::eoc::end, { column_name }, mview.tomb);
    }
    for (auto& cp: mview.cells) {
        index_and_write_column_name(out, clustering_key, { column_name, cp.first });
        write_cell(out, cp.second, cdef);
    }
  });
}

// This function is about writing a clustered_row to data file according to SSTables format.
// clustered_row contains a set of cells sharing the same clustering key.
void sstable_writer_k_l::write_clustered_row(file_writer& out, const schema& schema, const clustering_row& clustered_row) {
    auto clustering_key = composite::from_clustering_element(schema, clustered_row.key());

    maybe_write_row_tombstone(out, clustering_key, clustered_row);
    maybe_write_row_marker(out, schema, clustered_row.marker(), clustering_key);

    _collector.update_min_max_components(clustered_row.key());

    // Write all cells of a partition's row.
    clustered_row.cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& c) {
        auto&& column_definition = schema.regular_column_at(id);
        // non atomic cell isn't supported yet. atomic cell maps to a single trift cell.
        // non atomic cell maps to multiple trift cell, e.g. collection.
        if (!column_definition.is_atomic()) {
            write_collection(out, clustering_key, column_definition, c.as_collection_mutation());
            return;
        }
        assert(column_definition.is_regular());
        atomic_cell_view cell = c.as_atomic_cell(column_definition);
        std::vector<bytes_view> column_name = { column_definition.name() };
        index_and_write_column_name(out, clustering_key, column_name);
        write_cell(out, cell, column_definition);
    });
}

void sstable_writer_k_l::write_static_row(file_writer& out, const schema& schema, const row& static_row) {
    assert(schema.is_compound());
    static_row.for_each_cell([&] (column_id id, const atomic_cell_or_collection& c) {
        auto&& column_definition = schema.static_column_at(id);
        if (!column_definition.is_atomic()) {
            auto sp = composite::static_prefix(schema);
            write_collection(out, sp, column_definition, c.as_collection_mutation());
            return;
        }
        assert(column_definition.is_static());
        const auto& column_name = column_definition.name();
        auto sp = composite::static_prefix(schema);
        index_and_write_column_name(out, sp, { bytes_view(column_name) });
        atomic_cell_view cell = c.as_atomic_cell(column_definition);
        write_cell(out, cell, column_definition);
    });
}

void sstable_writer_k_l::index_and_write_column_name(file_writer& out,
         const composite& clustering_element,
         const std::vector<bytes_view>& column_names,
         composite::eoc marker) {
    if (_schema.clustering_key_size()) {
        bytes_writer_for_column_name w;
        write_column_name(_version, w, _schema, clustering_element, column_names, marker);
        auto&& colname = std::move(w).release();
        maybe_flush_pi_block(out, clustering_element, colname);
        write_column_name(_version, out, colname);
    } else {
        write_column_name(_version, out, _schema, clustering_element, column_names, marker);
    }
}

static void write_index_header(sstable_version_types v, file_writer& out, disk_string_view<uint16_t>& key, uint64_t pos) {
    write(v, out, key, pos);
}

static void write_index_promoted(sstable_version_types v, file_writer& out, bytes_ostream& promoted_index,
        deletion_time deltime, uint32_t numblocks) {
    uint32_t promoted_index_size = promoted_index.size();
    if (promoted_index_size) {
        promoted_index_size += 16 /* deltime + numblocks */;
        write(v, out, promoted_index_size, deltime, numblocks, promoted_index);
    } else {
        write(v, out, promoted_index_size);
    }
}

void sstable_writer_k_l::maybe_add_summary_entry(const dht::token& token, bytes_view key) {
    return sstables::maybe_add_summary_entry(_sst._components->summary, token, key, get_offset(),
        _index->offset(), _index_sampling_state);
}

// Returns offset into data component.
uint64_t sstable_writer_k_l::get_offset() const {
    if (_sst.has_component(component_type::CompressionInfo)) {
        // Variable returned by compressed_file_length() is constantly updated by compressed output stream.
        return _sst._components->compression.compressed_file_length();
    } else {
        return _writer->offset();
    }
}

file_writer sstable_writer_k_l::index_file_writer(sstable& sst, const io_priority_class& pc) {
    file_output_stream_options options;
    options.buffer_size = sst.sstable_buffer_size;
    options.io_priority_class = pc;
    options.write_behind = 10;
    return file_writer::make(std::move(sst._index_file), std::move(options), sst.filename(component_type::Index)).get0();
}

void sstable_writer_k_l::prepare_file_writer()
{
    file_output_stream_options options;
    options.io_priority_class = _pc;
    options.buffer_size = _sst.sstable_buffer_size;
    options.write_behind = 10;

    if (!_compression_enabled) {
        auto out = make_file_data_sink(std::move(_sst._data_file), options).get0();
        _writer = std::make_unique<adler32_checksummed_file_writer>(std::move(out), options.buffer_size, _sst.get_filename());
    } else {
        auto out = make_file_output_stream(std::move(_sst._data_file), std::move(options)).get0();
        _writer = std::make_unique<file_writer>(make_compressed_file_k_l_format_output_stream(
                std::move(out), &_sst._components->compression, _schema.get_compressor_params()), _sst.get_filename());
    }
}

void sstable_writer_k_l::finish_file_writer()
{
    auto writer = std::move(_writer);
    writer->close();

    if (!_compression_enabled) {
        auto chksum_wr = static_cast<adler32_checksummed_file_writer*>(writer.get());
        _sst.write_digest(chksum_wr->full_checksum());
        _sst.write_crc(chksum_wr->finalize_checksum());
    } else {
        _sst.write_digest(_sst._components->compression.get_full_checksum());
    }
}

sstable_writer_k_l::~sstable_writer_k_l() {
    if (_writer) {
        try {
            _writer->close();
        } catch (...) {
            sstlog.error("sstable_writer failed to close file: {}", std::current_exception());
        }
    }
    if (_index) {
        try {
            _index->close();
        } catch (...) {
            sstlog.error("sstable_writer failed to close file: {}", std::current_exception());
        }
    }
}

sstable_writer_k_l::sstable_writer_k_l(sstable& sst, const schema& s, uint64_t estimated_partitions,
                               const sstable_writer_config& cfg, const io_priority_class& pc, shard_id shard)
    : writer_impl(sst, s, pc, cfg)
    , _backup(cfg.backup)
    , _leave_unsealed(cfg.leave_unsealed)
    , _shard(shard)
    , _monitor(cfg.monitor)
    , _run_identifier(cfg.run_identifier)
    , _max_sstable_size(cfg.max_sstable_size)
    , _tombstone_written(false)
    , _range_tombstones(s, reader_semaphore.make_permit(&_schema, "components-writer"))
    , _version(sst.get_version())
{
    _sst.generate_toc(_schema.get_compressor_params().get_compressor(), _schema.bloom_filter_fp_chance());
    _sst.write_toc(_pc);
    _sst.create_data().get();
    _compression_enabled = !_sst.has_component(component_type::CRC);
    prepare_file_writer();

    // This can be 0 in some cases, which is albeit benign, can wreak havoc
    // in lower-level writer code, so clamp it to [1, +inf) here, which is
    // exactly what callers used to do anyway.
    estimated_partitions = std::max(uint64_t(1), estimated_partitions);

    _sst._components->filter = utils::i_filter::get_filter(estimated_partitions, _schema.bloom_filter_fp_chance(), utils::filter_format::k_l_format);
    _pi_write.desired_block_size = cfg.promoted_index_block_size;
    _index_sampling_state.summary_byte_cost = cfg.summary_byte_cost;
    _index = std::make_unique<file_writer>(index_file_writer(sst, pc));

    prepare_summary(_sst._components->summary, estimated_partitions, _schema.min_index_interval());

    // FIXME: we may need to set repaired_at stats at this point.

    _monitor->on_write_started(_writer->offset_tracker());
    _sst._shards = { shard };
}

void sstable_writer_k_l::consume_new_partition(const dht::decorated_key& dk) {
    // Set current index of data to later compute row size.
    _c_stats.start_offset = _writer->offset();

    _partition_key = key::from_partition_key(_schema, dk.key());

    maybe_add_summary_entry(dk.token(), bytes_view(*_partition_key));
    _sst._components->filter->add(bytes_view(*_partition_key));
    _collector.add_key(bytes_view(*_partition_key));

    auto p_key = disk_string_view<uint16_t>();
    p_key.value = bytes_view(*_partition_key);

    // Write index file entry for partition key into index file.
    // Write an index entry minus the "promoted index" (sample of columns)
    // part. We can only write that after processing the entire partition
    // and collecting the sample of columns.
    write_index_header(_sst.get_version(), *_index, p_key, _writer->offset());
    _pi_write.data = {};
    _pi_write.numblocks = 0;
    _pi_write.deltime.local_deletion_time = std::numeric_limits<int32_t>::max();
    _pi_write.deltime.marked_for_delete_at = std::numeric_limits<int64_t>::min();
    _pi_write.block_start_offset = _writer->offset();
    _pi_write.tombstone_accumulator = range_tombstone_accumulator(_schema, false);
    _pi_write.schemap = &_schema; // sadly we need this

    // Write partition key into data file.
    write(_sst.get_version(), *_writer, p_key);

    _tombstone_written = false;
}

void sstable_writer_k_l::consume(tombstone t) {
    deletion_time d;

    if (t) {
        d.local_deletion_time = t.deletion_time.time_since_epoch().count();
        d.marked_for_delete_at = t.timestamp;

        _c_stats.update(d);
    } else {
        // Default values for live, undeleted rows.
        d.local_deletion_time = std::numeric_limits<int32_t>::max();
        d.marked_for_delete_at = std::numeric_limits<int64_t>::min();
    }
    write(_sst.get_version(), *_writer, d);
    _tombstone_written = true;
    // TODO: need to verify we don't do this twice?
    _pi_write.deltime = d;
}

stop_iteration sstable_writer_k_l::consume(static_row&& sr) {
    ensure_tombstone_is_written();
    write_static_row(*_writer, _schema, sr.cells());
    return stop_iteration::no;
}

stop_iteration sstable_writer_k_l::consume(clustering_row&& cr) {
    drain_tombstones(cr.position());
    write_clustered_row(*_writer, _schema, cr);
    return stop_iteration::no;
}

void sstable_writer_k_l::drain_tombstones(position_in_partition_view pos) {
    ensure_tombstone_is_written();
    while (auto mfo = _range_tombstones.get_next(pos)) {
        write_tombstone(std::move(*mfo).as_range_tombstone());
    }
}

void sstable_writer_k_l::drain_tombstones() {
    ensure_tombstone_is_written();
    while (auto mfo = _range_tombstones.get_next()) {
        write_tombstone(std::move(*mfo).as_range_tombstone());
    }
}

stop_iteration sstable_writer_k_l::consume(range_tombstone&& rt) {
    drain_tombstones(rt.position());
    _range_tombstones.apply(std::move(rt));
    return stop_iteration::no;
}

void sstable_writer_k_l::write_tombstone(range_tombstone&& rt) {
    auto start = composite::from_clustering_element(_schema, rt.start);
    auto start_marker = bound_kind_to_start_marker(rt.start_kind);
    auto end = composite::from_clustering_element(_schema, rt.end);
    auto end_marker = bound_kind_to_end_marker(rt.end_kind);
    auto tomb = rt.tomb;
    index_tombstone(*_writer, start, std::move(rt), start_marker);
    write_range_tombstone(*_writer, std::move(start), start_marker, std::move(end), end_marker, {}, tomb);
}

stop_iteration sstable_writer_k_l::consume_end_of_partition() {
    drain_tombstones();

    // If there is an incomplete block in the promoted index, write it too.
    // However, if the _promoted_index is still empty, don't add a single
    // chunk - better not output a promoted index at all in this case.
    if (!_pi_write.data.empty() && !_pi_write.block_first_colname.empty()) {
        output_promoted_index_entry(_pi_write.data,
            _pi_write.block_first_colname,
            _pi_write.block_last_colname,
            _pi_write.block_start_offset - _c_stats.start_offset,
            _writer->offset() - _pi_write.block_start_offset);
        _pi_write.numblocks++;
    }
    write_index_promoted(_sst.get_version(), *_index, _pi_write.data, _pi_write.deltime,
            _pi_write.numblocks);
    _pi_write.data = {};
    _pi_write.block_first_colname = {};

    int16_t end_of_row = 0;
    write(_sst.get_version(), *_writer, end_of_row);

    // compute size of the current row.
    _c_stats.partition_size = _writer->offset() - _c_stats.start_offset;

    _sst.get_large_data_handler().maybe_record_large_partitions(_sst, *_partition_key, _c_stats.partition_size).get();

    // update is about merging column_stats with the data being stored by collector.
    _collector.update(std::move(_c_stats));
    _c_stats.reset();

    if (!_first_key) {
        _first_key = *_partition_key;
    }
    _last_key = std::move(*_partition_key);
    _partition_key = std::nullopt;

    return get_offset() < _max_sstable_size ? stop_iteration::no : stop_iteration::yes;
}

static sstable_enabled_features all_features() {
    return sstable_enabled_features::all();
}

void sstable_writer_k_l::consume_end_of_stream()
{
    _monitor->on_data_write_completed();

    // what if there is only one partition? what if it is empty?
    seal_summary(_sst._components->summary, std::move(_first_key), std::move(_last_key), _index_sampling_state).get();

    _index->close();
    _index.reset();

    if (_sst.has_component(component_type::CompressionInfo)) {
        _collector.add_compression_ratio(_sst._components->compression.compressed_file_length(), _sst._components->compression.uncompressed_file_length());
    }

    _sst.set_first_and_last_keys();
    seal_statistics(_sst.get_version(), _sst._components->statistics, _collector, _sst.compaction_ancestors(), _schema.get_partitioner().name(), _schema.bloom_filter_fp_chance(),
            _sst._schema, _sst.get_first_decorated_key(), _sst.get_last_decorated_key());

    finish_file_writer();
    _sst.write_summary(_pc);
    _sst.write_filter(_pc);
    _sst.write_statistics(_pc);
    _sst.write_compression(_pc);
    auto features = all_features();
    run_identifier identifier{_run_identifier};
    _sst.write_scylla_metadata(_pc, _shard, std::move(features), std::move(identifier), {}, "");

    if (!_leave_unsealed) {
        _sst.seal_sstable(_backup).get();
    }
}

} // namespace sstables
