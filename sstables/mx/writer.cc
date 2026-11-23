/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/mx/writer.hh"
#include "sstables/mx/partition_encoder.hh"
#include "sstables/writer.hh"
#include "sstables/trie/bti_index.hh"
#include "encoding_stats.hh"
#include "schema/schema.hh"
#include "mutation/mutation_fragment.hh"
#include "vint-serialization.hh"
#include "sstables/types.hh"
#include "sstables/mx/types.hh"
#include "mutation/atomic_cell.hh"
#include "utils/assert.hh"
#include "utils/exceptions.hh"
#include "db/large_data_handler.hh"
#include "db/corrupt_data_handler.hh"
#include "keys/keys.hh"

#include <functional>
#include <iterator>
#include <queue>
#include <boost/container/static_vector.hpp>

logging::logger slogger("mc_writer");

namespace sstables {
namespace mc {

bool clustering_blocks_input_range::next() const {
    if (_offset == _serialization_limit_size) {
        // No more values to encode
        return false;
    }

    // Each block contains up to max_block_size values
    auto limit = std::min(_serialization_limit_size, _offset + clustering_block::max_block_size);

    _current_block = {};
    SCYLLA_ASSERT (_offset % clustering_block::max_block_size == 0);
    while (_offset < limit) {
        auto shift = _offset % clustering_block::max_block_size;
        if (_offset < _prefix.size(_schema)) {
            managed_bytes_view value = _prefix.get_component(_schema, _offset);
            if (value.empty()) {
                _current_block.header |= (uint64_t(1) << (shift * 2));
            } else {
                const auto& compound_type = _prefix.get_compound_type(_schema);
                const auto& types = compound_type->types();
                if (_offset < types.size()) {
                    const auto& type = *types[_offset];
                    _current_block.values.push_back({value, type});
                } else {
                    // FIXME: might happen due to bad thrift key.
                    // See https://github.com/scylladb/scylla/issues/7568
                    //
                    // Consider turning into exception when the issue is fixed
                    // and the key is rejected in thrift handler layer, and if
                    // the bad key could not find its way to existing sstables.
                    slogger.warn("prefix {} (size={}): offset {} >= types.size {}", _prefix, _prefix.size(_schema), _offset, types.size());
                    _current_block.header |= (uint64_t(1) << ((shift * 2) + 1));
                }
            }
        } else {
            // This (and all subsequent) values of the prefix are missing (null)
            // This branch is only ever taken for an ephemerally_full_prefix
            _current_block.header |= (uint64_t(1) << ((shift * 2) + 1));
        }
        ++_offset;
    }
    return true;
}

static bytes_array_vint_size to_bytes_array_vint_size(bytes b) {
    bytes_array_vint_size result;
    result.value = std::move(b);
    return result;
}

static bytes_array_vint_size to_bytes_array_vint_size(const sstring& s) {
    bytes_array_vint_size result;
    result.value = to_bytes(s);
    return result;
}

static sstring pk_type_to_string(const schema& s) {
    if (s.partition_key_size() == 1) {
        return s.partition_key_columns().begin()->type->name();
    } else {
        return seastar::format("org.apache.cassandra.db.marshal.CompositeType({})",
                               fmt::join(s.partition_key_columns()
                                          | std::views::transform(std::mem_fn(&column_definition::type))
                                          | std::views::transform(std::mem_fn(&abstract_type::name)),
                                        ","));
    }
}

sstable_schema make_sstable_schema(const schema& s, const encoding_stats& enc_stats) {
    sstable_schema sst_sch;
    serialization_header& header = sst_sch.header;
    // mc serialization header minimum values are delta-encoded based on the default timestamp epoch times
    // Note: We rely on implicit conversion to uint64_t when subtracting the signed epoch values below
    // for preventing signed integer overflow.
    header.min_timestamp_base.value = static_cast<uint64_t>(enc_stats.min_timestamp) - encoding_stats::timestamp_epoch;
    header.min_local_deletion_time_base.value = static_cast<uint64_t>(enc_stats.min_local_deletion_time.time_since_epoch().count()) - encoding_stats::deletion_time_epoch;
    header.min_ttl_base.value = static_cast<uint64_t>(enc_stats.min_ttl.count()) - encoding_stats::ttl_epoch;

    header.pk_type_name = to_bytes_array_vint_size(pk_type_to_string(s));

    header.clustering_key_types_names.elements.reserve(s.clustering_key_size());
    for (const auto& ck_column : s.clustering_key_columns()) {
        auto ck_type_name = to_bytes_array_vint_size(ck_column.type->name());
        header.clustering_key_types_names.elements.push_back(std::move(ck_type_name));
    }

    auto add_column = [&] (const column_definition& column) {
        serialization_header::column_desc cd;
        cd.name = to_bytes_array_vint_size(column.name());
        cd.type_name = to_bytes_array_vint_size(column.type->name());
        if (column.is_static()) {
            header.static_columns.elements.push_back(std::move(cd));
            sst_sch.static_columns.push_back(column);
        } else if (column.is_regular()) {
            header.regular_columns.elements.push_back(std::move(cd));
            sst_sch.regular_columns.push_back(column);
        }
    };

    for (const auto& column : s.v3().all_columns()) {
        add_column(column);
    }


    // For static and regular columns, we write all simple columns first followed by collections
    // These containers have columns partitioned by atomicity
    auto pred = [] (const std::reference_wrapper<const column_definition>& column) { return column.get().is_atomic(); };
    std::ranges::stable_partition(sst_sch.regular_columns, pred);
    std::ranges::stable_partition(sst_sch.static_columns, pred);

    return sst_sch;
}

// The sstable writer runs in a seastar thread, so the encoder may yield in long rows.
struct thread_yield {
    static void maybe_yield() {
        thread::maybe_yield();
    }
};

// Used for writing SSTables in 'mc' format.
class writer : public sstable_writer::writer_impl {
    friend class partition_encoder<writer, thread_yield>;
private:
    const encoding_stats _enc_stats;
    shard_id _shard; // Specifies which shard the new SStable will belong to.
    bool _compression_enabled = false;
    std::unique_ptr<file_writer> _data_writer;
    std::unique_ptr<file_writer> _index_writer;
    std::unique_ptr<file_writer> _rows_writer;
    std::unique_ptr<file_writer> _partitions_writer;
    optimized_optional<trie::bti_row_index_writer> _bti_row_index_writer;
    optimized_optional<trie::bti_partition_index_writer> _bti_partition_index_writer;
    // The key of the last `consume_new_partition` call.
    // A partition key can only be inserted into Partitions.db
    // after its intra-partition index is written to Rows.db.
    // So we need to hold onto the key until `consume_end_of_partition`.
    std::optional<dht::decorated_key> _current_dk_for_bti;
    // Position of the Data writer at the moment of the last
    // `consume_new_partition` call.
    uint64_t _current_partition_position = 0;
    // If true, we don't build the bloom filter during the main pass
    // (when Data.db is written), but we instead write the murmur hashes
    // of keys to a temporary file, and later (after Data.db is written,
    // but before the sstable is sealed) we build the bloom filter from that.
    //
    // (Ideally this mechanism should only be used if the optimal size of the
    // filter can't be well estimated in advance. As of this writing we use
    // this mechanism every time the Index component isn't being written).
    bool _delayed_filter = true;
    // The writer of the temporary file used when `_delayed_filter` is true.
    std::unique_ptr<file_writer> _hashes_writer;
    // Positions within a partition are weakly monotonic, so only its first
    // clustering position can lower the min. Reset in consume_new_partition().
    bool _partition_min_checked = false;
    // The length of partition header (partition key, partition deletion and static row, if present)
    // as written to the data file
    // Used for writing promoted index
    uint64_t _partition_header_length = 0;
    std::optional<key> _partition_key;
    utils::hashed_key _current_murmur_hash{{0, 0}};
    std::optional<key> _first_key, _last_key;
    index_sampling_state _index_sampling_state;
    uint64_t _num_partitions_consumed = 0;

    const sstable_schema _sst_schema;
    partition_encoder<writer, thread_yield> _encoder;

    struct pi_block {
        clustering_info first;
        clustering_info last;
        uint64_t offset;
        uint64_t width;
        std::optional<tombstone> open_marker;
        // The range tombstone active between this block
        // and its predecessor.
        // (If there's no predecessor, this is a "live" tombstone).
        tombstone preceding_range_tombstone;
    };
    // _pi_write_m is used temporarily for building the promoted
    // index (column sample) of one partition when writing a new sstable.
    struct {
        // Unfortunately we cannot output the promoted index directly to the
        // index file because it needs to be prepended by its size.
        // first_entry is used for deferring serialization into blocks for small partitions.
        std::optional<pi_block> first_entry;
        bytes_ostream blocks; // Serialized pi_blocks.
        bytes_ostream offsets; // Serialized block offsets (uint32_t) relative to the start of "blocks".
        uint64_t promoted_index_size = 0; // Number of pi_blocks inside blocks and first_entry;
        sstables::deletion_time partition_tombstone;
        tombstone range_tombstone_preceding_current_block;
        uint64_t block_start_offset;
        uint64_t block_next_start_offset;
        std::optional<clustering_info> first_clustering;
        std::optional<clustering_info> last_clustering;

        // for this partition
        size_t desired_block_size;
        size_t auto_scale_threshold;

        // from write config
        size_t promoted_index_block_size;
        size_t promoted_index_auto_scale_threshold;
    } _pi_write_m;
    run_id _run_identifier;
    // Size threshold used by can_split_partition_at_clustering_boundary() in consume(clustering_row&&)
    // to decide whether the current partition may be split at a clustering boundary.
    // Derived once from _cfg.max_sstable_size, which never changes for the lifetime of the writer.
    const uint64_t _size_threshold;
    large_data_stats_entry _partition_size_entry;
    large_data_stats_entry _rows_in_partition_entry;
    large_data_stats_entry _row_size_entry;
    large_data_stats_entry _cell_size_entry;
    large_data_stats_entry _elements_in_collection_entry;

    // Bounded min-heaps for top-N large data records, one per large_data_type.
    // Size-type heaps (partition_size, row_size, cell_size) compare by `value`;
    // element-count-type heaps (rows_in_partition, elements_in_collection)
    // compare by `elements_count`.
    struct large_data_record_cmp_by_value {
        bool operator()(const large_data_record& a, const large_data_record& b) const {
            return a.value > b.value; // min-heap: smallest value on top
        }
    };
    struct large_data_record_cmp_by_elements {
        bool operator()(const large_data_record& a, const large_data_record& b) const {
            return a.elements_count > b.elements_count; // min-heap: smallest elements_count on top
        }
    };
    using ld_size_heap = std::priority_queue<large_data_record, std::vector<large_data_record>, large_data_record_cmp_by_value>;
    using ld_elements_heap = std::priority_queue<large_data_record, std::vector<large_data_record>, large_data_record_cmp_by_elements>;
    ld_size_heap _ld_partition_size_records;
    ld_elements_heap _ld_rows_in_partition_records;
    ld_size_heap _ld_row_size_records;
    ld_size_heap _ld_cell_size_records;
    ld_elements_heap _ld_elements_in_collection_records;

    // Insert a record into a bounded min-heap, keeping at most N entries.
    // Uses the heap's own comparator to decide eviction: since the comparator
    // defines a min-heap (smallest on top), comp(rec, top) is true when rec
    // is "greater" than top in the heap's ordering, meaning it should replace it.
    template <typename Heap>
    void insert_into_ld_heap(Heap& heap, large_data_record rec) {
        auto max_records = _cfg.large_data_records_per_sstable;
        if (max_records == 0) {
            return;
        }
        if (heap.size() < max_records) {
            heap.push(std::move(rec));
        } else if (typename Heap::value_compare{}(rec, heap.top())) {
            heap.pop();
            heap.push(std::move(rec));
        }
    }

    void init_file_writers();

    // Returns the closed writer
    std::unique_ptr<file_writer> close_writer(std::unique_ptr<file_writer>& w);
    uint32_t close_digest_writer(std::unique_ptr<file_writer>& w);

    void close_data_writer();
    void close_index_writer();
    void close_rows_writer();
    void close_partitions_writer();

    void drain_tombstones(std::optional<position_in_partition_view> pos = {});

    void maybe_add_summary_entry(const dht::token& token, bytes_view key) {
      if (_index_writer) {
        return sstables::maybe_add_summary_entry(
            _sst._components->summary, token, key, get_data_offset(),
            _index_writer->offset(), _index_sampling_state);
      }
    }

    void maybe_set_pi_first_clustering(const clustering_info& info, tombstone range_tombstone, uint64_t pos);
    void maybe_add_pi_block();
    void add_pi_block();
    void write_pi_block(const pi_block&);

    uint64_t get_data_offset() const {
        if (_sst.has_component(component_type::CompressionInfo)) {
            // Variable returned by compressed_file_length() is constantly updated by compressed output stream.
            return _sst._components->compression.compressed_file_length();
        } else {
            return _data_writer->offset();
        }
    }

    void maybe_record_large_partitions(const sstables::sstable& sst, const sstables::key& partition_key, uint64_t partition_size, uint64_t rows, uint64_t range_tombstones, uint64_t dead_rows);
    void maybe_record_large_rows(const sstables::sstable& sst, const sstables::key& partition_key,
            const clustering_key_prefix* clustering_key, const uint64_t row_size);
    void maybe_record_large_cells(const sstables::sstable& sst, const sstables::key& partition_key,
            const clustering_key_prefix* clustering_key, const column_definition& cdef, uint64_t cell_size, uint64_t collection_elements);

    void record_corrupt_row(clustering_row&& clustered_row);

    void collect_row_stats(uint64_t row_size, const clustering_key_prefix* clustering_key, bool is_dead = false) {
        ++_c_stats.rows_count;
        if (is_dead) {
            ++_c_stats.dead_rows_count;
        }
        maybe_record_large_rows(_sst, *_partition_key, clustering_key, row_size);
    }
    void collect_range_tombstone_stats() {
        ++_c_stats.rows_count;
        ++_c_stats.range_tombstones_count;
    }

    void maybe_update_partition_min(position_in_partition_view pos) {
        if (!_partition_min_checked) {
            _collector.update_min(pos);
            _partition_min_checked = true;
        }
    }

    void write_promoted_index();

    // partition_encoder hooks, called from inside the data encoding of a partition.
    void on_partition_tombstone(tombstone t, const deletion_time& dt, uint64_t size);
    void on_static_row_written(uint64_t size);
    void on_row_written(const clustering_row& clustered_row, uint64_t size);
    void on_range_tombstone_written(const rt_marker& marker);
    // Called after a row or an RT marker starting at data offset pos is written.
    void on_clustered_written(const clustering_key_prefix& key, bound_kind_m kind, tombstone preceding_range_tombstone, uint64_t pos);
    void on_cell_written(const clustering_key_prefix* clustering_key, const column_definition& cdef, atomic_cell_view cell,
            gc_clock::duration ttl, gc_clock::time_point expiry, uint64_t size);
    void on_collection_written(const clustering_key_prefix* clustering_key, const column_definition& cdef,
            uint64_t collection_elements, uint64_t size);
    // ttl and ldt are the written values for a dead or an expiring marker.
    void on_liveness_info(const row_marker& marker, gc_clock::duration ttl, gc_clock::time_point ldt);
    void on_tombstone(const tombstone& t) {
        _c_stats.update(t);
    }
    void on_capped_deletion_time(tombstone t, int32_t ldt);
    void on_clamped_ttl(gc_clock::duration ttl);
public:

    writer(sstable& sst, const schema& s, uint64_t estimated_partitions,
        const sstable_writer_config& cfg, encoding_stats enc_stats,
        shard_id shard = this_shard_id())
        : sstable_writer::writer_impl(sst, s, cfg)
        , _enc_stats(partition_encoder<writer, thread_yield>::clamp_enc_stats_ttl(enc_stats))
        , _shard(shard)
        , _sst_schema(make_sstable_schema(s, _enc_stats))
        , _encoder(s, _sst_schema, _sst.get_version(), _enc_stats, *this, _sst.sstable_buffer_size)
        , _run_identifier(cfg.run_identifier)
        , _size_threshold([this] {
                    // will allow size limit to be exceeded for 10%, so we won't perform unnecessary split
                    // of a partition which crossed the limit by a small margin.
                    const uint64_t max_size = std::numeric_limits<uint64_t>::max();
                    if (_cfg.max_sstable_size == max_size) {
                        return max_size;
                    }
                    uint64_t threshold_goal = _cfg.max_sstable_size * 1.1;
                    // handle overflow.
                    return threshold_goal < _cfg.max_sstable_size ? max_size : threshold_goal;
                }())
        , _partition_size_entry(
                    large_data_stats_entry{
                        .threshold = _sst.get_large_data_handler().get_partition_threshold_bytes(),
                    }
                )
        , _rows_in_partition_entry(
                    large_data_stats_entry{
                        .threshold = _sst.get_large_data_handler().get_rows_count_threshold(),
                    }
                )
        , _row_size_entry(
                    large_data_stats_entry{
                        .threshold = _sst.get_large_data_handler().get_row_threshold_bytes(),
                    }
                )
        , _cell_size_entry(
                    large_data_stats_entry{
                        .threshold = _sst.get_large_data_handler().get_cell_threshold_bytes(),
                    }
                )
        , _elements_in_collection_entry(
                    large_data_stats_entry{
                        .threshold = _sst.get_large_data_handler().get_collection_elements_count_threshold(),
                    }
                )
    {
        // This can be 0 in some cases, which is albeit benign, can wreak havoc
        // in lower-level writer code, so clamp it to [1, +inf) here, which is
        // exactly what callers used to do anyway.
        estimated_partitions = std::max(uint64_t(1), estimated_partitions);

        _sst.open_sstable(cfg.origin);
        _sst.create_data().get();
        _compression_enabled = !_sst.has_component(component_type::CRC);
        _delayed_filter = _sst.has_component(component_type::Filter) && !_sst.has_component(component_type::Index);
        init_file_writers();
        _sst._shards = { shard };

        _cfg.monitor->on_write_started(_data_writer->offset_tracker());
        if (!_delayed_filter) {
            _sst._components->filter = utils::i_filter::get_filter(estimated_partitions, _sst._schema->bloom_filter_fp_chance(), utils::filter_format::m_format);
        }
        _pi_write_m.promoted_index_block_size = cfg.promoted_index_block_size;
        _pi_write_m.promoted_index_auto_scale_threshold = cfg.promoted_index_auto_scale_threshold;
        _index_sampling_state.summary_byte_cost = _cfg.summary_byte_cost;
        _index_sampling_state.max_partitions_per_page = _cfg.summary_max_partitions_per_page;
      if (_index_writer) {
        prepare_summary(_sst._components->summary, estimated_partitions, _schema.min_index_interval());
      }
    }

    ~writer();
    void consume_new_partition(const dht::decorated_key& dk) override;
    void consume(tombstone t) override;
    stop_iteration consume(static_row&& sr) override;
    stop_iteration consume(clustering_row&& cr) override;
    stop_iteration consume(range_tombstone_change&& rtc) override;
    stop_iteration consume_end_of_partition() override;
    void consume_end_of_stream() override;
    uint64_t data_file_position_for_tests() const override;
};

writer::~writer() {
    auto close_writer = [](auto& writer) {
        if (writer) {
            try {
                writer->close();
            } catch (...) {
                sstlog.error("writer failed to close file: {:t}", std::current_exception());
            }
        }
    };
    close_writer(_index_writer);
    close_writer(_data_writer);
    close_writer(_partitions_writer);
    close_writer(_rows_writer);
    close_writer(_hashes_writer);
}

void writer::maybe_set_pi_first_clustering(const clustering_info& info, tombstone preceding_range_tombstone, uint64_t pos) {
    if (!_pi_write_m.first_clustering) {
        _pi_write_m.first_clustering = info;
        _pi_write_m.range_tombstone_preceding_current_block = preceding_range_tombstone;
        _pi_write_m.block_start_offset = pos;
        _pi_write_m.block_next_start_offset = pos + _pi_write_m.desired_block_size;
    }
}

void writer::add_pi_block() {
    auto block = pi_block{
        *_pi_write_m.first_clustering,
        *_pi_write_m.last_clustering,
        _pi_write_m.block_start_offset - _c_stats.start_offset,
        _data_writer->offset() - _pi_write_m.block_start_offset,
        (_encoder.current_tombstone() ? std::make_optional(_encoder.current_tombstone()) : std::optional<tombstone>{}),
        _pi_write_m.range_tombstone_preceding_current_block,
    };

    // An index with only one block (that spans the entire partition)
    // would be relatively useless, and wouldn't carry its weight.
    // So if we are adding the first block, we don't write it out yet --
    // we only remember it in `first_entry`, and we wait until another
    // entry appears. (If it doesn't, then there will be no row index
    // for this partition).
    // If this is a second block, we write out the deferred `first_entry`
    // first, and then we proceed normally.
    if (_pi_write_m.promoted_index_size == 0) {
        _pi_write_m.first_entry.emplace(std::move(block));
        ++_pi_write_m.promoted_index_size;
        return;
    } else if (_pi_write_m.promoted_index_size == 1) {
        write_pi_block(*_pi_write_m.first_entry);
    }

    write_pi_block(block);
    ++_pi_write_m.promoted_index_size;

    // auto-scale?
    if (_pi_write_m.blocks.size() >= _pi_write_m.auto_scale_threshold) {
        _pi_write_m.desired_block_size *= 2;
        _pi_write_m.auto_scale_threshold += _pi_write_m.promoted_index_auto_scale_threshold;
        _sst.get_stats().on_promoted_index_auto_scale();
    }
}

void writer::maybe_add_pi_block() {
    uint64_t pos = _data_writer->offset();
    if (pos >= _pi_write_m.block_next_start_offset) {
        add_pi_block();
        _pi_write_m.first_clustering.reset();
        _pi_write_m.range_tombstone_preceding_current_block = tombstone();
        _pi_write_m.block_next_start_offset = pos + _pi_write_m.desired_block_size;
    }
}

void writer::init_file_writers() {
    auto out = _sst._storage->make_data_or_index_sink(_sst, component_type::Data).get();

    if (!_compression_enabled) {
        _data_writer = std::make_unique<crc32_checksummed_file_writer>(std::move(out), _sst.sstable_buffer_size, _sst.get_filename());
    } else {
        auto compressor = _sst.manager().get_compressor_factory().make_compressor_for_writing(_sst._schema).get();
        _data_writer = std::make_unique<file_writer>(
            make_compressed_file_m_format_output_stream(
                output_stream<char>(std::move(out)),
                &_sst._components->compression,
                _sst._schema->get_compressor_params(),
                std::move(compressor)), _sst.get_filename());
    }

    if (_sst.has_component(component_type::Index)) {
        out = _sst._storage->make_data_or_index_sink(_sst, component_type::Index).get();
        _index_writer = std::make_unique<crc32_digest_file_writer>(std::move(out), _sst.sstable_buffer_size, _sst.index_filename());
    }
    if (_sst.has_component(component_type::Partitions) && _sst.has_component(component_type::Rows)) {
        out = _sst._storage->make_data_or_index_sink(_sst, component_type::Rows).get();
        _rows_writer = std::make_unique<crc32_digest_file_writer>(std::move(out), _sst.sstable_buffer_size, component_name(_sst, component_type::Rows));
        _bti_row_index_writer = trie::bti_row_index_writer(*_rows_writer);
        out = _sst._storage->make_data_or_index_sink(_sst, component_type::Partitions).get();
        _partitions_writer = std::make_unique<crc32_digest_file_writer>(std::move(out), _sst.sstable_buffer_size, component_name(_sst, component_type::Partitions));
        _bti_partition_index_writer = trie::bti_partition_index_writer(_sst.get_version(), *_partitions_writer);
    }
    if (_delayed_filter) {
        file_output_stream_options options;
        options.buffer_size = 32 * 1024;
        _hashes_writer = std::make_unique<file_writer>(_sst.make_component_file_writer(component_type::TemporaryHashes, std::move(options),
            sstable_write_open_flags).get());
    }
}

std::unique_ptr<file_writer> writer::close_writer(std::unique_ptr<file_writer>& w) {
    auto writer = std::move(w);
    writer->close();
    return writer;
}

uint32_t writer::close_digest_writer(std::unique_ptr<file_writer>& w) {
    auto writer = close_writer(w);
    auto chksum_wr = static_cast<crc32_digest_file_writer*>(writer.get());
    return chksum_wr->full_checksum();
}

void writer::close_data_writer() {
    auto writer = close_writer(_data_writer);
    if (!_compression_enabled) {
        auto chksum_wr = static_cast<crc32_checksummed_file_writer*>(writer.get());
        _sst.write_digest(chksum_wr->full_checksum());
        _sst.write_crc(chksum_wr->finalize_checksum());
    } else {
        _sst.write_digest(_sst._components->compression.get_full_checksum());
    }
}

void writer::close_index_writer() {
    if (_index_writer) {
        _sst.get_components_digests().map[component_type::Index] = close_digest_writer(_index_writer);
    }
}

void writer::close_partitions_writer() {
    if (_partitions_writer) {
        _sst._partitions_db_footer = std::move(*_bti_partition_index_writer).finish(
            _first_key.value(),
            _last_key.value());
        _sst.get_components_digests().map[component_type::Partitions] = close_digest_writer(_partitions_writer);
    }
}

void writer::close_rows_writer() {
    if (_rows_writer) {
        _sst.get_components_digests().map[component_type::Rows] = close_digest_writer(_rows_writer);
    }
}

void writer::consume_new_partition(const dht::decorated_key& dk) {
    _c_stats.start_offset = _data_writer->offset();

    _partition_key = key::from_partition_key(_schema, dk.key());
    maybe_add_summary_entry(dk.token(), bytes_view(*_partition_key));

    _current_murmur_hash = utils::make_hashed_key(bytes_view(*_partition_key));
    if (_hashes_writer) {
        std::array<uint64_t, 2> hash = {
            seastar::cpu_to_le(_current_murmur_hash.hash()[0]),
            seastar::cpu_to_le(_current_murmur_hash.hash()[1])
        };
        _hashes_writer->write(reinterpret_cast<const char*>(hash.data()), sizeof(hash));
    } else {
        _sst._components->filter->add(_current_murmur_hash);
    }
    _collector.add_key(bytes_view(*_partition_key));
    _num_partitions_consumed++;

    auto p_key = disk_string_view<uint16_t>();
    p_key.value = bytes_view(*_partition_key);

  if (_index_writer) {
    // Write index file entry from partition key into index file.
    // Write an index entry minus the "promoted index" (sample of columns)
    // part. We can only write that after processing the entire partition
    // and collecting the sample of columns.
    write(_sst.get_version(), *_index_writer, p_key);
    write_vint(*_index_writer, _data_writer->offset());
  }
    _current_dk_for_bti = dk;
    _current_partition_position = _data_writer->offset();

    _pi_write_m.first_entry.reset();
    _pi_write_m.blocks.clear();
    _pi_write_m.offsets.clear();
    _pi_write_m.promoted_index_size = 0;
    _pi_write_m.partition_tombstone = {};
    _pi_write_m.first_clustering.reset();
    _pi_write_m.last_clustering.reset();
    _pi_write_m.desired_block_size = _pi_write_m.promoted_index_block_size;
    _pi_write_m.auto_scale_threshold = _pi_write_m.promoted_index_auto_scale_threshold;

    _encoder.consume_new_partition(*_data_writer, p_key.value);
    _partition_header_length = _data_writer->offset() - _c_stats.start_offset;

    _partition_min_checked = false;
}

void writer::consume(tombstone t) {
    _encoder.consume(*_data_writer, t);
}

void writer::on_partition_tombstone(tombstone t, const deletion_time& dt, uint64_t size) {
    _pi_write_m.partition_tombstone = dt;
    _partition_header_length += size;
    _c_stats.update(t);

    if (t) {
        // The tombstone covers the whole clustered region; feed the sentinels to the
        // collector directly - nothing in this partition can improve on them.
        _collector.update_min(position_in_partition_view::before_all_clustered_rows());
        _collector.update_max(position_in_partition_view::after_all_clustered_rows());
        _partition_min_checked = true;
    }
}

void writer::maybe_record_large_partitions(const sstables::sstable& sst, const sstables::key& partition_key,
                                           uint64_t partition_size, uint64_t rows, uint64_t range_rombstones, uint64_t dead_rows) {
    auto& size_entry = _partition_size_entry;
    auto& row_count_entry = _rows_in_partition_entry;
    size_entry.max_value = std::max(size_entry.max_value, partition_size);
    row_count_entry.max_value = std::max(row_count_entry.max_value, rows);
    auto ret = _sst.get_large_data_handler().maybe_record_large_partitions(sst, partition_key, partition_size, rows, range_rombstones, dead_rows).get();
    size_entry.above_threshold += unsigned(bool(ret.size));
    row_count_entry.above_threshold += unsigned(bool(ret.elements));

    auto trace_log = [&] (large_data_type type) {
        slogger.trace("Detected large partition: sstable={}, partition_key={}, record_type={}, partition_size={}, rows={}, range_tombstones={}, dead_rows={}",
            _sst.component_basename(component_type::Data),
            key_to_str(partition_key.to_partition_key(_schema), _schema),
            type,
            partition_size,
            rows,
            range_rombstones,
            dead_rows);
    };

    // Populate top-N large data records into separate per-type heaps.
    if (ret.size) {
        trace_log(large_data_type::partition_size);
        const auto& pk_bytes = partition_key.get_bytes();
        insert_into_ld_heap(_ld_partition_size_records, large_data_record{
            .type = large_data_type::partition_size,
            .partition_key = disk_string<uint32_t>{bytes(pk_bytes)},
            .clustering_key = disk_string<uint32_t>{bytes()},
            .column_name = disk_string<uint32_t>{bytes()},
            .value = partition_size,
            .elements_count = rows,
            .range_tombstones = range_rombstones,
            .dead_rows = dead_rows,
        });
    }
    if (ret.elements) {
        trace_log(large_data_type::rows_in_partition);
        const auto& pk_bytes = partition_key.get_bytes();
        insert_into_ld_heap(_ld_rows_in_partition_records, large_data_record{
            .type = large_data_type::rows_in_partition,
            .partition_key = disk_string<uint32_t>{bytes(pk_bytes)},
            .clustering_key = disk_string<uint32_t>{bytes()},
            .column_name = disk_string<uint32_t>{bytes()},
            .value = partition_size,
            .elements_count = rows,
            .range_tombstones = range_rombstones,
            .dead_rows = dead_rows,
        });
    }
}

void writer::maybe_record_large_rows(const sstables::sstable& sst, const sstables::key& partition_key,
        const clustering_key_prefix* clustering_key, uint64_t row_size) {
    auto& entry = _row_size_entry;
    if (entry.max_value < row_size) {
        entry.max_value = row_size;
    }
    if (_sst.get_large_data_handler().maybe_record_large_rows(sst, partition_key, clustering_key, row_size).get()) {
        entry.above_threshold++;

        slogger.trace("Detected large row: sstable={}, partition_key={}, clustering_key={}, record_type={}, row_size={}",
            _sst.component_basename(component_type::Data),
            key_to_str(partition_key.to_partition_key(_schema), _schema),
            clustering_key ? key_to_str(*clustering_key, _schema) : "",
            large_data_type::row_size,
            row_size);
        const auto& pk_bytes = partition_key.get_bytes();
        auto ck_bytes = clustering_key ? clustering_key->view().representation().linearize() : bytes();
        insert_into_ld_heap(_ld_row_size_records, large_data_record{
            .type = large_data_type::row_size,
            .partition_key = disk_string<uint32_t>{bytes(pk_bytes)},
            .clustering_key = disk_string<uint32_t>{std::move(ck_bytes)},
            .column_name = disk_string<uint32_t>{bytes()},
            .value = row_size,
        });
    };
}

void writer::maybe_record_large_cells(const sstables::sstable& sst, const sstables::key& partition_key,
        const clustering_key_prefix* clustering_key, const column_definition& cdef, uint64_t cell_size, uint64_t collection_elements) {
    auto& cell_size_entry = _cell_size_entry;
    if (cell_size_entry.max_value < cell_size) {
        cell_size_entry.max_value = cell_size;
    }
    auto& collection_elements_entry = _elements_in_collection_entry;
    if (collection_elements_entry.max_value < collection_elements) {
        collection_elements_entry.max_value = collection_elements;
    }
    auto ret = _sst.get_large_data_handler().maybe_record_large_cells(_sst, *_partition_key, clustering_key, cdef, cell_size, collection_elements).get();
    if (ret.size) {
        cell_size_entry.above_threshold++;
    }
    if (ret.elements) {
        collection_elements_entry.above_threshold++;
    }

    auto trace_log = [&] (large_data_type type) {
        slogger.trace("Detected large cell: sstable={}, partition_key={}, clustering_key={}, column={}, record_type={}, cell_size={}, collection_elements={}",
            _sst.component_basename(component_type::Data),
            key_to_str(partition_key.to_partition_key(_schema), _schema),
            clustering_key ? key_to_str(*clustering_key, _schema) : "",
            cdef.name_as_text(),
            type,
            cell_size,
            collection_elements);
    };

    // Populate top-N large data records into separate per-type heaps.
    if (ret.size) {
        trace_log(large_data_type::cell_size);
        const auto& pk_bytes = partition_key.get_bytes();
        auto ck_bytes = clustering_key ? clustering_key->view().representation().linearize() : bytes();
        auto col_name = cdef.name_as_text();
        insert_into_ld_heap(_ld_cell_size_records, large_data_record{
            .type = large_data_type::cell_size,
            .partition_key = disk_string<uint32_t>{bytes(pk_bytes)},
            .clustering_key = disk_string<uint32_t>{std::move(ck_bytes)},
            .column_name = disk_string<uint32_t>{to_bytes(col_name)},
            .value = cell_size,
            .elements_count = collection_elements,
        });
    }
    if (ret.elements) {
        trace_log(large_data_type::elements_in_collection);
        const auto& pk_bytes = partition_key.get_bytes();
        auto ck_bytes = clustering_key ? clustering_key->view().representation().linearize() : bytes();
        auto col_name = cdef.name_as_text();
        insert_into_ld_heap(_ld_elements_in_collection_records, large_data_record{
            .type = large_data_type::elements_in_collection,
            .partition_key = disk_string<uint32_t>{bytes(pk_bytes)},
            .clustering_key = disk_string<uint32_t>{std::move(ck_bytes)},
            .column_name = disk_string<uint32_t>{to_bytes(col_name)},
            .value = cell_size,
            .elements_count = collection_elements,
        });
    }
}

void writer::on_cell_written(const clustering_key_prefix* clustering_key, const column_definition& cdef, atomic_cell_view cell,
        gc_clock::duration ttl, gc_clock::time_point expiry, uint64_t size) {
    // Collect cell statistics
    // We record collections in on_collection_written, so ignore them here
    if (cdef.is_atomic()) {
        ++_c_stats.cells_count;
        ++_c_stats.column_count;
        maybe_record_large_cells(_sst, *_partition_key, clustering_key, cdef, size, 0);
    }

    auto timestamp = cell.timestamp();
    if (!cell.is_live()) {
        _c_stats.update_timestamp(timestamp, is_live::no);
        _c_stats.update_local_deletion_time_and_tombstone_histogram(cell.deletion_time());
        _sst.get_stats().on_cell_tombstone_write();
        return;
    }

    _c_stats.update_timestamp(timestamp, is_live::yes);

    if (cell.is_live_and_has_ttl()) {
        _c_stats.update_ttl(ttl);
        // tombstone histogram is updated with expiration time because if ttl is longer
        // than gc_grace_seconds for all data, sstable will be considered fully expired
        // when actually nothing is expired.
        _c_stats.update_local_deletion_time_and_tombstone_histogram(expiry);
    } else { // regular live cell
        _c_stats.update_local_deletion_time(std::numeric_limits<int>::max());
    }
    _sst.get_stats().on_cell_write();
}

void writer::on_liveness_info(const row_marker& marker, gc_clock::duration ttl, gc_clock::time_point ldt) {
    api::timestamp_type timestamp = marker.timestamp();
    if (marker.is_live()) {
        _c_stats.update_timestamp(timestamp, is_live::yes);
        _c_stats.update_live_row_marker_timestamp(timestamp);
    } else {
        _c_stats.update_timestamp(timestamp, is_live::no);
    }

    if (!marker.is_live() || marker.is_expiring()) {
        _c_stats.update_ttl(ttl);
        _c_stats.update_local_deletion_time_and_tombstone_histogram(ldt);
    } else {
        _c_stats.update_ttl(0);
        _c_stats.update_local_deletion_time(std::numeric_limits<int32_t>::max());
    }
}

void writer::on_collection_written(const clustering_key_prefix* clustering_key, const column_definition& cdef,
        uint64_t collection_elements, uint64_t size) {
    if (collection_elements > 0) {
        ++_c_stats.column_count;
    }
    _c_stats.cells_count += collection_elements;
    maybe_record_large_cells(_sst, *_partition_key, clustering_key, cdef, size, collection_elements);
}

void writer::on_static_row_written(uint64_t size) {
    _partition_header_length += size;

    collect_row_stats(size, nullptr);
}

stop_iteration writer::consume(static_row&& sr) {
    _encoder.consume(*_data_writer, sr);
    return stop_iteration::no;
}

void writer::on_row_written(const clustering_row& clustered_row, uint64_t size) {
    const bool is_dead = !clustered_row.is_live(_schema, tombstone(), gc_clock::now());

    // Collect statistics
    maybe_update_partition_min(clustered_row.position());
    collect_row_stats(size, &clustered_row.key(), is_dead);
}

void writer::on_clustered_written(const clustering_key_prefix& key, bound_kind_m kind, tombstone preceding_range_tombstone, uint64_t pos) {
    clustering_info info {key, kind};
    maybe_set_pi_first_clustering(info, preceding_range_tombstone, pos);
    _pi_write_m.last_clustering = std::move(info);
    maybe_add_pi_block();
}

void writer::on_capped_deletion_time(tombstone t, int32_t ldt) {
    slogger.warn("Capping tombstone local_deletion_time {} to max {}", t.deletion_time.time_since_epoch().count(), ldt);
    slogger.warn("Capping tombstone in sstable = {}, partition_key = {}", _sst.get_filename(), _partition_key->to_partition_key(_schema));
    _sst.get_stats().on_capped_tombstone_deletion_time();
}

void writer::on_clamped_ttl(gc_clock::duration ttl) {
    static thread_local logger::rate_limit rl(std::chrono::minutes(1));
    sstlog.log(log_level::warn, rl, "Clamping too big ttl {} to max_ttl ({}) while writing {}", ttl.count(), max_ttl.count(), _sst.get_filename());
}

void writer::record_corrupt_row(clustering_row&& clustered_row) {
    auto& handler = _sst.get_corrupt_data_handler();

    const auto pk = _partition_key->to_partition_key(_schema);
    const auto ck = clustered_row.key();

    db::corrupt_data_handler::entry_id corrupt_row_id;
    sstring result;
    try {
        corrupt_row_id = handler.record_corrupt_clustering_row(_schema, pk, std::move(clustered_row), "sstable-write", fmt::to_string(_sst.get_filename())).get();
        result = format("written corrupt row to {} with id {}", handler.storage_name(), corrupt_row_id);
    } catch (...) {
        result = format("failed to write corrupt row to {}: {:t}", handler.storage_name(), std::current_exception());
    }

    slogger.error("found non-full clustering key {} in partition {} while writing sstable {} for non-compact table {}.{}; {}",
            ck,
            pk,
            _sst.get_filename(),
            _schema.ks_name(),
            _schema.cf_name(),
            result);
}

stop_iteration writer::consume(clustering_row&& cr) {
    if (_encoder.writes_regular_as_static()) {
        _encoder.consume(*_data_writer, cr);
        return stop_iteration::no;
    }
    if (!_schema.is_compact_table() && !cr.key().is_full(_schema)) {
        record_corrupt_row(std::move(cr));
        return stop_iteration::no;
    }

    _encoder.consume(*_data_writer, cr);

    auto can_split_partition_at_clustering_boundary = [this] {
        // Check there are enough promoted index entries, meaning that current fragment won't
        // unnecessarily cut the current partition in the middle.
        bool has_enough_promoted_index_entries = _pi_write_m.promoted_index_size >= 2;

        return get_data_offset() > _size_threshold && has_enough_promoted_index_entries;
    };

    return stop_iteration(can_split_partition_at_clustering_boundary());
}

void writer::write_promoted_index() {
  if (_index_writer) {
    if (_pi_write_m.promoted_index_size < 2) {
        write_vint(*_index_writer, uint64_t(0));
        return;
    }
    bytes_ostream& tmp_bufs = _encoder.tmp_bufs();
    write_vint(tmp_bufs, _partition_header_length);
    write(_sst.get_version(), tmp_bufs, _pi_write_m.partition_tombstone);
    write_vint(tmp_bufs, _pi_write_m.promoted_index_size);
    uint64_t pi_size = tmp_bufs.size() + _pi_write_m.blocks.size() + _pi_write_m.offsets.size();
    write_vint(*_index_writer, pi_size);
    _encoder.flush_tmp_bufs(*_index_writer);
    write(_sst.get_version(), *_index_writer, _pi_write_m.blocks);
    write(_sst.get_version(), *_index_writer, _pi_write_m.offsets);
  }
}

void writer::write_pi_block(const pi_block& block) {
  if (_index_writer) {
    static constexpr size_t width_base = 65536;
    bytes_ostream& blocks = _pi_write_m.blocks;
    uint32_t offset = blocks.size();
    write(_sst.get_version(), _pi_write_m.offsets, offset);
    write_clustering_prefix(_sst.get_version(), blocks, block.first.kind, _schema, block.first.clustering);
    write_clustering_prefix(_sst.get_version(), blocks, block.last.kind, _schema, block.last.clustering);
    write_vint(blocks, block.offset);
    write_signed_vint(blocks, block.width - width_base);
    write(_sst.get_version(), blocks, static_cast<std::byte>(block.open_marker ? 1 : 0));
    if (block.open_marker) {
        write(_sst.get_version(), blocks, _encoder.to_deletion_time(*block.open_marker));
    }
  }
    if (_bti_row_index_writer) {
        _bti_row_index_writer->add(
            _schema,
            block.first,
            block.last,
            block.offset,
            _encoder.to_deletion_time(block.preceding_range_tombstone)
        );
    }
}

void writer::on_range_tombstone_written(const rt_marker& marker) {
    maybe_update_partition_min(marker.position());

    collect_range_tombstone_stats();
}

stop_iteration writer::consume(range_tombstone_change&& rtc) {
    _encoder.consume(*_data_writer, rtc);
    return stop_iteration::no;
}

stop_iteration writer::consume_end_of_partition() {
    _encoder.ensure_partition_header_is_written(*_data_writer);

    uint64_t end_of_partition_position = _data_writer->offset();

    auto write_end_of_partition = [&] {
        _encoder.write_end_of_partition(*_data_writer);
    };

    auto maybe_add_pi_block = [&] {
        if (_pi_write_m.promoted_index_size && _pi_write_m.first_clustering) {
            add_pi_block();
        }
    };

    if (_features.is_enabled(CorrectLastPiBlockWidth)) [[likely]] {
        maybe_add_pi_block();
        write_end_of_partition();
    } else {
        write_end_of_partition();
        maybe_add_pi_block();
    }

    write_promoted_index();

    if (_bti_partition_index_writer) {
        auto partitions_db_payload = _bti_row_index_writer->finish(
            _sst.get_version(),
            _schema,
            _current_partition_position,
            end_of_partition_position,
            *_partition_key,
            _pi_write_m.partition_tombstone
        );
        _bti_partition_index_writer->add(
            _schema,
            *std::exchange(_current_dk_for_bti, std::nullopt),
            _current_murmur_hash,
            partitions_db_payload
        );
    }

    // compute size of the current row.
    _c_stats.partition_size = _data_writer->offset() - _c_stats.start_offset;

    maybe_record_large_partitions(_sst, *_partition_key, _c_stats.partition_size, _c_stats.rows_count, _c_stats.range_tombstones_count, _c_stats.dead_rows_count);

    // Only the partition's last clustering position can raise the max; the promoted
    // index already holds an owning copy of it.
    if (const auto& last = _pi_write_m.last_clustering) {
        _collector.update_max(position_of(last->clustering, last->kind));
    }

    // update is about merging column_stats with the data being stored by collector.
    _collector.update(std::move(_c_stats));
    _c_stats.reset();

    if (!_first_key) {
        _first_key = *_partition_key;
    }
    _last_key = std::move(*_partition_key);
    _partition_key = std::nullopt;
    return get_data_offset() < _cfg.max_sstable_size ? stop_iteration::no : stop_iteration::yes;
}

void writer::consume_end_of_stream() {
    _cfg.monitor->on_data_write_completed();

  if (_sst._components->summary) {
    seal_summary(_sst._components->summary, std::optional<key>(_first_key), std::optional<key>(_last_key), _index_sampling_state).get();
  }

    if (_sst.has_component(component_type::CompressionInfo)) {
        _collector.add_compression_ratio(_sst._components->compression.compressed_file_length(), _sst._components->compression.uncompressed_file_length());
    }

    close_index_writer();

    close_partitions_writer();
    close_rows_writer();

    if (_hashes_writer) {
        close_writer(_hashes_writer);
    }

    _sst.set_first_and_last_keys();

    _sst._components->statistics.contents[metadata_type::Serialization] = std::make_unique<serialization_header>(std::move(_sst_schema.header));
    seal_statistics(_sst.get_version(), _sst._components->statistics, _collector,
        _sst._schema->get_partitioner().name(), _sst._schema->bloom_filter_fp_chance(),
        _sst._schema, _sst.get_first_decorated_key(), _sst.get_last_decorated_key(), _enc_stats);
    close_data_writer();

  if (_sst._components->summary) {
    _sst.write_summary();
  }

    if (_delayed_filter) {
        _sst.build_delayed_filter(_num_partitions_consumed);
    } else {
        _sst.maybe_rebuild_filter_from_index(_num_partitions_consumed);
    }
    _sst.write_filter();
    _sst.write_statistics();
    _sst.write_compression();
    // Note: during the SSTable write, the `compressor` object in `_sst._components->compression`
    // can only compress, not decompress. We have to create a decompressing `compressor` here.
    // (The reason we split the two is that we don't want to keep the compressor-specific compression
    // context after the write is over, because it hogs memory).
    auto decompressor = _sst.manager().get_compressor_factory().make_compressor_for_reading(_sst._components->compression).get();
    _sst._components->compression.set_compressor(std::move(decompressor));
    _sst._components->compression.discard_hidden_options();
    run_identifier identifier{_run_identifier};
    std::optional<scylla_metadata::large_data_stats> ld_stats(scylla_metadata::large_data_stats{
        .map = {
            { large_data_type::partition_size, std::move(_partition_size_entry) },
            { large_data_type::rows_in_partition, std::move(_rows_in_partition_entry) },
            { large_data_type::row_size, std::move(_row_size_entry) },
            { large_data_type::cell_size, std::move(_cell_size_entry) },
            { large_data_type::elements_in_collection, std::move(_elements_in_collection_entry) },
        }
    });
    std::optional<scylla_metadata::ext_timestamp_stats> ts_stats(scylla_metadata::ext_timestamp_stats{
        .map = _collector.get_ext_timestamp_stats()
    });
    // Drain all per-type min-heaps into a single large_data_records array.
    std::optional<scylla_metadata::large_data_records> ld_records;
    {
        utils::chunked_vector<large_data_record> records;
        auto drain_size_heap = [&records](ld_size_heap& heap) {
            while (!heap.empty()) {
                records.push_back(std::move(const_cast<large_data_record&>(heap.top())));
                heap.pop();
            }
        };
        auto drain_elements_heap = [&records](ld_elements_heap& heap) {
            while (!heap.empty()) {
                records.push_back(std::move(const_cast<large_data_record&>(heap.top())));
                heap.pop();
            }
        };
        drain_size_heap(_ld_partition_size_records);
        drain_elements_heap(_ld_rows_in_partition_records);
        drain_size_heap(_ld_row_size_records);
        drain_size_heap(_ld_cell_size_records);
        drain_elements_heap(_ld_elements_in_collection_records);
        if (!records.empty()) {
            ld_records = scylla_metadata::large_data_records{.elements = std::move(records)};
        }
    }
    _sst.write_scylla_metadata(_shard, std::move(identifier), std::move(ld_stats), std::move(ts_stats), std::move(ld_records));
    if (!_cfg.leave_unsealed) {
        _sst.seal_sstable(_cfg.backup).get();
    }
}

uint64_t writer::data_file_position_for_tests() const {
    return _data_writer->offset();
}

std::unique_ptr<sstable_writer::writer_impl> make_writer(sstable& sst,
        const schema& s,
        uint64_t estimated_partitions,
        const sstable_writer_config& cfg,
        encoding_stats enc_stats,
        shard_id shard) {
    return std::make_unique<writer>(sst, s, estimated_partitions, cfg, enc_stats, shard);
}

}
}
