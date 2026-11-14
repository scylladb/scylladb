/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/sstable_writer.hh"

#include "sstables/parquet/mapping_writer.hh"
#include "sstables/compressor.hh"
#include "sstables/filter_writer.hh"
#include "sstables/large_data_tracker.hh"
#include "sstables/row_index_block_tracker.hh"
#include "sstables/serialization_header_builder.hh"
#include "sstables/sstable_sealer.hh"
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/trie_index_writer.hh"
#include "sstables/writer_stats.hh"
#include "mutation/mutation_fragment.hh"
#include "schema/schema.hh"

namespace sstables::parquet {

writer_config make_writer_config(const sstable_writer_config& cfg, const compression_parameters& compression) {
    writer_config wc;
    wc.row_group_size = cfg.parquet_row_group_size;
    wc.page_set_size = cfg.parquet_page_set_size;
    wc.min_page_size = cfg.parquet_min_page_size;
    wc.dictionary_page_limit = cfg.parquet_dictionary_page_limit;
    using algorithm = compression_parameters::algorithm;
    switch (compression.get_algorithm()) {
    case algorithm::none:
        wc.codec = compression_codec::uncompressed;
        break;
    case algorithm::lz4:
    case algorithm::lz4_with_dicts:
        wc.codec = compression_codec::lz4_raw;
        break;
    case algorithm::snappy:
        wc.codec = compression_codec::snappy;
        break;
    case algorithm::deflate:
        wc.codec = compression_codec::gzip;
        break;
    case algorithm::zstd:
    case algorithm::zstd_with_dicts:
        wc.codec = compression_codec::zstd;
        wc.compression_level = compression.zstd_compression_level();
        break;
    }
    return wc;
}

namespace {

// The weight of an index entry's position, as the trie index encodes it.
bound_weight weight_of(bound_kind_m kind) {
    switch (kind) {
    case bound_kind_m::excl_end:
    case bound_kind_m::incl_start:
    case bound_kind_m::excl_end_incl_start:
        return bound_weight::before_all_prefixed;
    case bound_kind_m::clustering:
    case bound_kind_m::static_clustering:
        return bound_weight::equal;
    case bound_kind_m::excl_start:
    case bound_kind_m::incl_end_excl_start:
    case bound_kind_m::incl_end:
        return bound_weight::after_all_prefixed;
    }
    std::unreachable();
}

class sstable_writer_impl final : public sstable_writer::writer_impl {
    const encoding_stats _enc_stats;
    const shard_id _shard;
    const run_id _run_identifier;
    serialization_header _header;
    // Compared with the on-disk estimate of the file size.
    const uint64_t _size_threshold;
    const bool _has_static_columns;
    large_data_tracker _large_data;
    sstable_sealer _sealer;
    // Engaged by the constructor. The mapping writer refers to the data
    // writer, so it is declared after it and destroyed before it.
    std::unique_ptr<crc32_digest_file_writer> _data_writer;
    std::optional<trie_index_writer> _trie_index;
    std::optional<filter_writer> _filter;
    std::optional<mapping_writer> _mapping;
    // Write progress, in raw size.
    writer_offset_tracker _progress;

    row_index_block_tracker _row_index;
    clustering_bounds_tracker _clustering_bounds;
    std::optional<key> _partition_key;
    std::optional<key> _first_key, _last_key;
    deletion_time _partition_tombstone;
    // The raw size of the file before the current partition.
    uint64_t _partition_raw_start = 0;
    // The range tombstone open after the last row written.
    tombstone _current_tombstone;
    // A page set ended after the last row of the open block. The block
    // ends before the next row, unless that row has the same position.
    bool _block_end_pending = false;
    // The static row of the partition was counted in the statistics.
    bool _static_row_counted = false;

    const writer& file() const noexcept {
        return _mapping->file();
    }

    void end_block(uint64_t end) {
        _row_index.end_block(end, _current_tombstone, [this] (const row_index_block& block) {
            _trie_index->add_row_index_block(_schema, block.first, block.last, block.offset,
                    to_deletion_time(block.preceding_range_tombstone, *_partition_key));
        });
    }

    // Called before a clustering row or a range tombstone bound row is written.
    void begin_entry(const clustering_info& info) {
        const uint64_t ordinal = file().rows();
        if (_block_end_pending) {
            _block_end_pending = false;
            // See "Row index blocks" in sstable_writer.hh.
            const clustering_info& last = *_row_index.last_clustering();
            if (weight_of(last.kind) != weight_of(info.kind) || !last.clustering.equal(_schema, info.clustering)) {
                end_block(ordinal);
            }
        }
        _row_index.on_entry_start(info, _current_tombstone, ordinal);
    }

    // Called after a clustering row or a range tombstone bound row is written.
    void end_entry(clustering_info info, const row_info& row) {
        _row_index.on_entry_end(std::move(info));
        _progress.offset = file().raw_size();
        _block_end_pending = row.page_set_ended;
    }

    // The `mx` writer writes a static row in every partition of a table
    // with static columns, and counts it as a row. The Parquet file has no
    // row for an empty static row, but the statistics count it, so that
    // they are equal to those of `mt` for the same data.
    void count_static_row_if_needed() {
        if (_has_static_columns && !_static_row_counted) {
            _static_row_counted = true;
            ++_c_stats.rows_count;
        }
    }

    // Collects the statistics of the cells of a row, and records large cells.
    void collect_cells(const row& cells, column_kind kind, const clustering_key_prefix* ck) {
        cells.for_each_cell([&] (column_id id, const atomic_cell_or_collection& c) {
            const column_definition& cdef = _schema.column_at(kind, id);
            if (cdef.is_atomic()) {
                const atomic_cell_view cell = c.as_atomic_cell(cdef);
                ++_c_stats.cells_count;
                ++_c_stats.column_count;
                collect_cell_stats(_c_stats, _sst.get_stats(), cell);
                _large_data.maybe_record_large_cell(*_partition_key, ck, cdef, cell.is_live() ? cell.value().size() : 0, 0);
                return;
            }
            const collection_mutation_view cmv = c.as_collection_mutation();
            _c_stats.update(cmv.tomb());
            uint64_t elements = 0;
            uint64_t size = 0;
            for (const auto& [path, cell] : cmv) {
                ++elements;
                size += path.size() + (cell.is_live() ? cell.value().size() : 0);
                collect_cell_stats(_c_stats, _sst.get_stats(), cell);
            }
            if (elements > 0) {
                ++_c_stats.column_count;
            }
            _c_stats.cells_count += elements;
            _large_data.maybe_record_large_cell(*_partition_key, ck, cdef, size, elements);
        });
    }

public:
    sstable_writer_impl(sstable& sst, const ::schema& s, uint64_t estimated_partitions,
            const sstable_writer_config& cfg, encoding_stats enc_stats, shard_id shard)
        : sstable_writer::writer_impl(sst, s, cfg)
        , _enc_stats(enc_stats)
        , _shard(shard)
        , _run_identifier(cfg.run_identifier)
        , _header(make_serialization_header(s, _enc_stats))
        , _size_threshold(partition_split_size_threshold(_cfg.max_sstable_size))
        , _has_static_columns(s.has_static_columns())
        , _large_data(sst, s, cfg.large_data_records_per_sstable)
        , _sealer(sst)
    {
        if (s.is_compact_table()) {
            throw std::invalid_argument(seastar::format("Cannot write sstable {} of format {}: compact storage table {}.{} is not supported",
                    sst.get_filename(), sst.get_format(), s.ks_name(), s.cf_name()));
        }
        // See the mx writer.
        estimated_partitions = std::max(uint64_t(1), estimated_partitions);

        _sealer.open(cfg.origin);
        _data_writer = _sealer.make_digest_data_writer();
        _trie_index.emplace(_sst);
        _filter.emplace(_sst, estimated_partitions);
        _sealer.set_owner_shard(shard);
        _cfg.monitor->on_write_started(_progress);
        _mapping.emplace(s, _header, *_data_writer, make_writer_config(_cfg, s.get_compressor_params()),
                _sst.manager().register_parquet_writer(_cfg.parquet_page_set_size, _cfg.parquet_row_group_size),
                _cfg.parquet_random_row_defaults_seed ? make_random_row_defaults_policy(*_cfg.parquet_random_row_defaults_seed) : nullptr);
    }

    ~sstable_writer_impl() {
        _mapping.reset();
        if (_data_writer) {
            try {
                _data_writer->close();
            } catch (...) {
                sstlog.error("writer failed to close file: {:t}", std::current_exception());
            }
        }
        _trie_index.reset();
        _filter.reset();
    }

    void consume_new_partition(const dht::decorated_key& dk) override {
        _partition_key = key::from_partition_key(_schema, dk.key());
        _filter->add(bytes_view(*_partition_key));
        _collector.add_key(bytes_view(*_partition_key));

        const uint64_t first_row = file().rows();
        _trie_index->start_partition(dk, first_row);
        _row_index.start_partition(first_row);
        _clustering_bounds.start_partition();
        _mapping->start_partition(dk.key());

        _partition_raw_start = file().raw_size();
        _partition_tombstone = to_deletion_time(tombstone(), *_partition_key);
        _current_tombstone = {};
        _block_end_pending = false;
        _static_row_counted = false;
    }

    void consume(tombstone t) override {
        _partition_tombstone = to_deletion_time(t, *_partition_key);
        _mapping->partition_tombstone(t);
        _c_stats.update(t);
        _clustering_bounds.on_partition_tombstone(_collector, t);
    }

    stop_iteration consume(static_row&& sr) override {
        _static_row_counted = true;
        ++_c_stats.rows_count;
        collect_cells(sr.cells(), column_kind::static_column, nullptr);
        if (const auto row = _mapping->write_static_row(sr)) {
            _progress.offset = file().raw_size();
            _large_data.maybe_record_large_row(*_partition_key, nullptr, row->raw_size);
        }
        return stop_iteration::no;
    }

    stop_iteration consume(clustering_row&& cr) override {
        if (!cr.key().is_full(_schema)) {
            record_corrupt_row(std::move(cr), *_partition_key);
            return stop_iteration::no;
        }
        count_static_row_if_needed();

        clustering_info info{cr.key(), bound_kind_m::clustering};
        begin_entry(info);
        const row_info row = _mapping->write_clustering_row(cr);
        end_entry(std::move(info), row);

        const row_marker& marker = cr.marker();
        collect_row_marker_stats(_c_stats, marker);
        if (cr.tomb().regular()) {
            _c_stats.do_update(cr.tomb().regular());
        }
        if (cr.tomb().is_shadowable()) {
            _c_stats.do_update(cr.tomb().tomb());
        }
        collect_cells(cr.cells(), column_kind::regular_column, &cr.key());
        _clustering_bounds.on_clustered(_collector, cr.position());
        ++_c_stats.rows_count;
        if (!cr.is_live(_schema, tombstone(), gc_clock::now())) {
            ++_c_stats.dead_rows_count;
        }
        _large_data.maybe_record_large_row(*_partition_key, &cr.key(), row.raw_size);

        // Split the partition only where enough row index blocks ended,
        // as the mx writer does.
        return stop_iteration(_row_index.block_count() >= 2 && file().estimated_size() > _size_threshold);
    }

    stop_iteration consume(range_tombstone_change&& rtc) override {
        count_static_row_if_needed();
        const auto bounds = range_tombstone_bounds(_current_tombstone, rtc);
        if (bounds.empty()) {
            return stop_iteration::no;
        }
        const clustering_key_prefix& prefix = rtc.position().key();
        bool first = true;
        for (const auto& bound : bounds) {
            clustering_info info{prefix, to_bound_kind_m(bound.kind)};
            begin_entry(info);
            const row_info row = _mapping->write_range_tombstone_bound(prefix, bound.kind, bound.tomb);
            _current_tombstone = is_start(info.kind) ? bound.tomb : tombstone();
            end_entry(std::move(info), row);
            // A boundary is one marker of the mx writer, which counts the
            // first tombstone with update() and the second with do_update().
            if (first) {
                _c_stats.update(bound.tomb);
            } else {
                _c_stats.do_update(bound.tomb);
            }
            first = false;
        }
        _clustering_bounds.on_clustered(_collector, rtc.position());
        // A boundary counts once, as in mt.
        ++_c_stats.rows_count;
        ++_c_stats.range_tombstones_count;
        return stop_iteration::no;
    }

    stop_iteration consume_end_of_partition() override {
        count_static_row_if_needed();
        if (_mapping->end_partition()) {
            _progress.offset = file().raw_size();
        }
        const uint64_t partition_end = file().rows();
        if (_row_index.block_count() && _row_index.has_open_block()) {
            end_block(partition_end);
        }
        _block_end_pending = false;

        _trie_index->end_partition(_schema, _filter->current_hash(), partition_end, *_partition_key, _partition_tombstone);

        _c_stats.partition_size = file().raw_size() - _partition_raw_start;
        _large_data.maybe_record_large_partition(*_partition_key, _c_stats.partition_size, _c_stats.rows_count,
                _c_stats.range_tombstones_count, _c_stats.dead_rows_count);
        if (const auto& last = _row_index.last_clustering()) {
            _clustering_bounds.end_partition(_collector, position_of(last->clustering, last->kind));
        }
        _collector.update(std::move(_c_stats));
        _c_stats.reset();

        if (!_first_key) {
            _first_key = *_partition_key;
        }
        _last_key = std::move(*_partition_key);
        _partition_key = std::nullopt;
        return file().estimated_size() < _cfg.max_sstable_size ? stop_iteration::no : stop_iteration::yes;
    }

    void consume_end_of_stream() override {
        _cfg.monitor->on_data_write_completed();

        const writer_result result = _mapping->finish();
        _mapping.reset();
        if (_schema.get_compressor_params().compression_enabled() && result.raw_size) {
            _collector.add_compression_ratio(result.file_size, result.raw_size);
        }
        const auto data_writer = std::exchange(_data_writer, nullptr);
        data_writer->close();

        // The order of the steps follows the mx writer. See sstable_sealer.
        _trie_index->finish(_first_key.value(), _last_key.value());
        _sealer.seal_statistics(std::move(_header), _collector, _enc_stats);
        _sealer.write_data_digest(data_writer->full_checksum());
        _filter->finish();
        _sealer.write_statistics();
        _sealer.write_scylla_metadata(_shard, _run_identifier, _large_data, _collector, scylla_metadata::parquet_data_file{
            .rows = result.rows,
            .raw_size = result.raw_size,
            .file_size = result.file_size,
            .footer_size = result.footer.size,
            .footer_crc32 = result.footer.crc32,
        });
        _sealer.seal(_cfg);
    }

    uint64_t data_file_position_for_tests() const override {
        return file().rows();
    }
};

} // anonymous namespace

std::unique_ptr<sstable_writer::writer_impl> make_sstable_writer(sstable& sst,
        const ::schema& s,
        uint64_t estimated_partitions,
        const sstable_writer_config& cfg,
        encoding_stats enc_stats,
        shard_id shard) {
    return std::make_unique<sstable_writer_impl>(sst, s, estimated_partitions, cfg, enc_stats, shard);
}

} // namespace sstables::parquet
