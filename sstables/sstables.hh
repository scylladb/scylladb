/*
 * Copyright (C) 2015 ScyllaDB
 *
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

#pragma once

#include "version.hh"
#include "shared_sstable.hh"
#include "core/file.hh"
#include "core/fstream.hh"
#include "core/future.hh"
#include "core/sstring.hh"
#include "core/enum.hh"
#include "core/shared_ptr.hh"
#include "core/distributed.hh"
#include <seastar/core/shared_ptr_incomplete.hh>
#include <unordered_set>
#include <unordered_map>
#include "types.hh"
#include "clustering_key_filter.hh"
#include "core/enum.hh"
#include "compress.hh"
#include "row.hh"
#include "dht/i_partitioner.hh"
#include "schema.hh"
#include "mutation.hh"
#include "utils/i_filter.hh"
#include "utils/optimized_optional.hh"
#include "core/stream.hh"
#include "writer.hh"
#include "metadata_collector.hh"
#include "filter.hh"
#include "exceptions.hh"
#include "mutation_reader.hh"
#include "query-request.hh"
#include "compound_compat.hh"
#include "disk-error-handler.hh"
#include "atomic_deletion.hh"
#include "sstables/shared_index_lists.hh"
#include "sstables/progress_monitor.hh"
#include "db/commitlog/replay_position.hh"
#include "flat_mutation_reader.hh"

namespace seastar {
class thread_scheduling_group;
}

namespace sstables {

extern logging::logger sstlog;

class data_consume_rows_context;

// data_consume_context is an object returned by sstable::data_consume_rows()
// which allows knowing when the consumer stops reading, and starting it again
// (e.g., when the consumer wants to stop after every sstable row).
//
// The read() method initiates reading into the consumer, and continues to
// read and feed data into the consumer until one of the consumer's callbacks
// requests to stop,  or until we reach the end of the data range originally
// requested. read() returns a future which completes when reading stopped.
// If we're at the end-of-file, the read may complete without reading anything
// so it's the consumer class's task to check if anything was consumed.
// Note:
// The caller MUST ensure that between calling read() on this object,
// and the time the returned future is completed, the object lives on.
// Moreover, the sstable object used for the sstable::data_consume_rows()
// call which created this data_consume_context, must also be kept alive.
//
// data_consume_rows() and data_consume_rows_at_once() both can read just a
// single row or many rows. The difference is that data_consume_rows_at_once()
// is optimized to reading one or few rows (reading it all into memory), while
// data_consume_rows() uses a read buffer, so not all the rows need to fit
// memory in the same time (they are delivered to the consumer one by one).
class data_consume_context {
    shared_sstable _sst;
    std::unique_ptr<data_consume_rows_context> _ctx;
    // This object can only be constructed by sstable::data_consume_rows()
    data_consume_context(shared_sstable,row_consumer& consumer, input_stream<char>&& input, uint64_t start, uint64_t maxlen);
    friend class sstable;
    data_consume_context();
    explicit operator bool() const noexcept;
    friend class optimized_optional<data_consume_context>;
public:
    future<> read();
    future<> fast_forward_to(uint64_t begin, uint64_t end);
    future<> skip_to(indexable_element, uint64_t begin);
    uint64_t position() const;
    bool eof() const;
    // Define (as defaults) the destructor and move operations in the source
    // file, so here we don't need to know the incomplete impl type.
    ~data_consume_context();
    data_consume_context(data_consume_context&&) noexcept;
    data_consume_context& operator=(data_consume_context&&) noexcept;
};

using data_consume_context_opt = optimized_optional<data_consume_context>;

class key;
class sstable_writer;
struct foreign_sstable_open_info;
struct sstable_open_info;

class index_reader;

struct sstable_writer_config {
    std::experimental::optional<size_t> promoted_index_block_size;
    uint64_t max_sstable_size = std::numeric_limits<uint64_t>::max();
    bool backup = false;
    bool leave_unsealed = false;
    stdx::optional<db::replay_position> replay_position;
    seastar::thread_scheduling_group* thread_scheduling_group = nullptr;
    seastar::shared_ptr<write_monitor> monitor = default_write_monitor();
};

static constexpr inline size_t default_sstable_buffer_size() {
    return 128 * 1024;
}

shared_sstable make_sstable(schema_ptr schema, sstring dir, int64_t generation, sstable_version_types v, sstable_format_types f, gc_clock::time_point now = gc_clock::now(),
            io_error_handler_gen error_handler_gen = default_io_error_handler_gen(), size_t buffer_size = default_sstable_buffer_size());

class sstable : public enable_lw_shared_from_this<sstable> {
public:
    enum class component_type {
        Index,
        CompressionInfo,
        Data,
        TOC,
        Summary,
        Digest,
        CRC,
        Filter,
        Statistics,
        TemporaryTOC,
        TemporaryStatistics,
        Scylla,
        Unknown,
    };
    using version_types = sstable_version_types;
    using format_types = sstable_format_types;
    static const size_t default_buffer_size = default_sstable_buffer_size();
public:
    sstable(schema_ptr schema, sstring dir, int64_t generation, version_types v, format_types f, gc_clock::time_point now = gc_clock::now(),
            io_error_handler_gen error_handler_gen = default_io_error_handler_gen(), size_t buffer_size = default_buffer_size)
        : sstable_buffer_size(buffer_size)
        , _schema(std::move(schema))
        , _dir(std::move(dir))
        , _generation(generation)
        , _version(v)
        , _format(f)
        , _now(now)
        , _read_error_handler(error_handler_gen(sstable_read_error))
        , _write_error_handler(error_handler_gen(sstable_write_error))
    { }
    sstable& operator=(const sstable&) = delete;
    sstable(const sstable&) = delete;
    sstable(sstable&&) = default;

    ~sstable();

    // Read one or few rows at the given byte range from the data file,
    // feeding them into the consumer. This function reads the entire given
    // byte range at once into memory, so it should not be used for iterating
    // over all the rows in the data file (see the next function for that.
    // The function returns a future which completes after all the data has
    // been fed into the consumer. The caller needs to ensure the "consumer"
    // object lives until then (e.g., using the do_with() idiom).
    future<> data_consume_rows_at_once(row_consumer& consumer, uint64_t pos, uint64_t end);

    // disk_read_range describes a byte ranges covering part of an sstable
    // row that we need to read from disk. Usually this is the whole byte
    // range covering a single sstable row, but in very large rows we might
    // want to only read a subset of the atoms which we know contains the
    // columns we are looking for.
    struct disk_read_range {
        // TODO: this should become a vector of ranges
        uint64_t start;
        uint64_t end;

        disk_read_range() : start(0), end(0) {}
        disk_read_range(uint64_t start, uint64_t end) :
            start(start), end(end) { }
        explicit operator bool() const {
            return start != end;
        }
    };

    // data_consume_rows() iterates over rows in the data file from
    // a particular range, feeding them into the consumer. The iteration is
    // done as efficiently as possible - reading only the data file (not the
    // summary or index files) and reading data in batches.
    //
    // The consumer object may request the iteration to stop before reaching
    // the end of the requested data range (e.g. stop after each sstable row).
    // A context object is returned which allows to resume this consumption:
    // This context's read() method requests that consumption begins, and
    // returns a future which will be resolved when it ends (because the
    // consumer asked to stop, or the data range ended). Only after the
    // returned future is resolved, may read() be called again to consume
    // more.
    // The caller must ensure (e.g., using do_with()) that the context object,
    // as well as the sstable, remains alive as long as a read() is in
    // progress (i.e., returned a future which hasn't completed yet).
    //
    // The "toread" range specifies the range we want to read initially.
    // However, the object returned by the read, a data_consume_context, also
    // provides a fast_forward_to(start,end) method which allows resetting
    // the reader to a new range. To allow that, we also have a "last_end"
    // byte which should be the last end to which fast_forward_to is
    // eventually allowed. If last_end==end, fast_forward_to is not allowed
    // at all, if last_end==file_size fast_forward_to is allowed until the
    // end of the file, and it can be something in between if we know that we
    // are planning to skip parts, but eventually read until last_end.
    // When last_end==end, we guarantee that the read will only read the
    // desired byte range from disk. However, when last_end > end, we may
    // read beyond end in anticipation of a small skip via fast_foward_to.
    // The amount of this excessive read is controlled by read ahead
    // hueristics which learn from the usefulness of previous read aheads.
    data_consume_context data_consume_rows(row_consumer& consumer, disk_read_range toread, uint64_t last_end);

    data_consume_context data_consume_single_partition(row_consumer& consumer, disk_read_range toread);

    // Like data_consume_rows() with bounds, but iterates over whole range
    data_consume_context data_consume_rows(row_consumer& consumer);

    static component_type component_from_sstring(sstring& s);
    static version_types version_from_sstring(sstring& s);
    static format_types format_from_sstring(sstring& s);
    static const sstring filename(sstring dir, sstring ks, sstring cf, version_types version, int64_t generation,
                                  format_types format, component_type component);
    static const sstring filename(sstring dir, sstring ks, sstring cf, version_types version, int64_t generation,
                                  format_types format, sstring component);
    // WARNING: it should only be called to remove components of a sstable with
    // a temporary TOC file.
    static future<> remove_sstable_with_temp_toc(sstring ks, sstring cf, sstring dir, int64_t generation,
                                                 version_types v, format_types f);

    // load sstable using components shared by a shard
    future<> load(foreign_sstable_open_info info);
    // load all components from disk
    // this variant will be useful for testing purposes and also when loading
    // a new sstable from scratch for sharing its components.
    future<> load(const io_priority_class& pc = default_priority_class());
    future<> open_data();
    future<> update_info_for_opened_data();

    future<> set_generation(int64_t generation);

    int64_t generation() const {
        return _generation;
    }

    // read_row() reads the entire sstable row (partition) at a given
    // partition key k, or a subset of this row. The subset is defined by
    // a filter on the clustering keys which we want to read, which
    // additionally determines also if all the static columns will also be
    // returned in the result.
    future<streamed_mutation_opt> read_row(
        schema_ptr schema,
        dht::ring_position_view key,
        const query::partition_slice& slice,
        const io_priority_class& pc = default_priority_class(),
        reader_resource_tracker resource_tracker = no_resource_tracking(),
        streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no);

    future<streamed_mutation_opt> read_row(schema_ptr schema, dht::ring_position_view key) {
        auto& full_slice = schema->full_slice();
        return read_row(std::move(schema), std::move(key), full_slice);
    }

    future<streamed_mutation_opt> read_row(
        schema_ptr schema,
        const sstables::key& key,
        const query::partition_slice& slice,
        const io_priority_class& pc = default_priority_class(),
        reader_resource_tracker resource_tracker = no_resource_tracking(),
        streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no);

    future<streamed_mutation_opt> read_row(schema_ptr schema, const sstables::key& key) {
        auto& full_slice = schema->full_slice();
        return read_row(std::move(schema), key, full_slice);
    }

    flat_mutation_reader read_row_flat(
        schema_ptr schema,
        dht::ring_position_view key,
        const query::partition_slice& slice,
        const io_priority_class& pc = default_priority_class(),
        reader_resource_tracker resource_tracker = no_resource_tracking(),
        streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no);

    flat_mutation_reader read_row_flat(schema_ptr schema, dht::ring_position_view key) {
        auto& full_slice = schema->full_slice();
        return read_row_flat(std::move(schema), std::move(key), full_slice);
    }

    flat_mutation_reader read_row_flat(
        schema_ptr schema,
        const sstables::key& key,
        const query::partition_slice& slice,
        const io_priority_class& pc = default_priority_class(),
        reader_resource_tracker resource_tracker = no_resource_tracking(),
        streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no);

    flat_mutation_reader read_row_flat(schema_ptr schema, const sstables::key& key) {
        auto& full_slice = schema->full_slice();
        return read_row_flat(std::move(schema), key, full_slice);
    }

    // Returns a mutation_reader for given range of partitions
    mutation_reader read_range_rows(
        schema_ptr schema,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        const io_priority_class& pc = default_priority_class(),
        reader_resource_tracker resource_tracker = no_resource_tracking(),
        streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
        mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes);

    mutation_reader read_range_rows(schema_ptr schema, const dht::partition_range& range) {
        auto& full_slice = schema->full_slice();
        return read_range_rows(std::move(schema), range, full_slice);
    }

    // Returns a mutation_reader for given range of partitions
    flat_mutation_reader read_range_rows_flat(
        schema_ptr schema,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        const io_priority_class& pc = default_priority_class(),
        reader_resource_tracker resource_tracker = no_resource_tracking(),
        streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
        mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes);

    flat_mutation_reader read_range_rows_flat(schema_ptr schema, const dht::partition_range& range) {
        auto& full_slice = schema->full_slice();
        return read_range_rows_flat(std::move(schema), range, full_slice);
    }

    // read_rows_flat() returns each of the rows in the sstable, in sequence,
    // converted to a "mutation" data structure.
    // This function is implemented efficiently - doing buffered, sequential
    // read of the data file (no need to access the index file).
    // A "mutation_reader" object is returned with which the caller can
    // fetch mutations in sequence, and allows stop iteration any time
    // after getting each row.
    //
    // The caller must ensure (e.g., using do_with()) that the context object,
    // as well as the sstable, remains alive as long as a read() is in
    // progress (i.e., returned a future which hasn't completed yet).
    flat_mutation_reader read_rows_flat(schema_ptr schema,
                              const io_priority_class& pc = default_priority_class(),
                              streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no);

    // Returns mutation_source containing all writes contained in this sstable.
    // The mutation_source shares ownership of this sstable.
    mutation_source as_mutation_source();

    future<> write_components(mutation_reader mr,
            uint64_t estimated_partitions,
            schema_ptr schema,
            const sstable_writer_config&,
            const io_priority_class& pc = default_priority_class());

    sstable_writer get_writer(const schema& s,
        uint64_t estimated_partitions,
        const sstable_writer_config&,
        const io_priority_class& pc = default_priority_class(),
        shard_id shard = engine().cpu_id());

    future<> seal_sstable(bool backup);

    uint64_t get_estimated_key_count() const {
        return ((uint64_t)_components->summary.header.size_at_full_sampling + 1) *
                _components->summary.header.min_index_interval;
    }

    uint64_t estimated_keys_for_range(const dht::token_range& range);

    std::vector<dht::decorated_key> get_key_samples(const schema& s, const dht::token_range& range);

    // mark_for_deletion() specifies that a sstable isn't relevant to the
    // current shard, and thus can be deleted by the deletion manager, if
    // all shards sharing it agree. In case the sstable is unshared, it's
    // guaranteed that all of its on-disk files will be deleted as soon as
    // the in-memory object is destroyed.
    void mark_for_deletion() {
        _marked_for_deletion = true;
    }

    bool marked_for_deletion() const {
        return _marked_for_deletion;
    }

    void add_ancestor(int64_t generation) {
        _collector.add_ancestor(generation);
    }

    std::unordered_set<uint64_t> ancestors() const;

    // Returns true iff this sstable contains data which belongs to many shards.
    bool is_shared() const {
        return _shared;
    }

    void set_unshared() {
        _shared = false;
    }

    // Returns uncompressed size of data component.
    uint64_t data_size() const;
    // Returns on-disk size of data component.
    uint64_t ondisk_data_size() const;

    uint64_t index_size() const {
        return _index_file_size;
    }
    uint64_t filter_size() const {
        return _filter_file_size;
    }

    db_clock::time_point data_file_write_time() const {
        return _data_file_write_time;
    }

    uint64_t filter_memory_size() const {
        return _components->filter->memory_size();
    }

    // Returns the total bytes of all components.
    uint64_t bytes_on_disk();

    const partition_key& get_first_partition_key() const;
    const partition_key& get_last_partition_key() const;

    const dht::decorated_key& get_first_decorated_key() const;
    const dht::decorated_key& get_last_decorated_key() const;

    // SSTable comparator using the first key (decorated key).
    // Return values are those of a trichotomic comparison.
    int compare_by_first_key(const sstable& other) const;

    // SSTable comparator using the max timestamp.
    // Return values are those of a trichotomic comparison.
    int compare_by_max_timestamp(const sstable& other) const;

    const sstring get_filename() const {
        return filename(component_type::Data);
    }
    const sstring& get_dir() const {
        return _dir;
    }
    sstring toc_filename() const;

    metadata_collector& get_metadata_collector() {
        return _collector;
    }

    std::vector<std::pair<component_type, sstring>> all_components() const;

    future<> create_links(sstring dir, int64_t generation) const;

    future<> create_links(sstring dir) const {
        return create_links(dir, _generation);
    }

    /**
     * Note. This is using the Origin definition of
     * max_data_age, which is load time. This could maybe
     * be improved upon.
     */
    gc_clock::time_point max_data_age() const {
        return _now;
    }
    std::vector<sstring> component_filenames() const;

    template<typename Func, typename... Args>
    auto sstable_write_io_check(Func&& func, Args&&... args) const {
        return do_io_check(_write_error_handler, func, std::forward<Args>(args)...);
    }

    // Immutable components that can be shared among shards.
    struct shareable_components {
        sstables::compression compression;
        utils::filter_ptr filter;
        sstables::summary summary;
        sstables::statistics statistics;
        stdx::optional<sstables::scylla_metadata> scylla_metadata;
    };
private:
    size_t sstable_buffer_size = default_buffer_size;

    static std::unordered_map<version_types, sstring, enum_hash<version_types>> _version_string;
    static std::unordered_map<format_types, sstring, enum_hash<format_types>> _format_string;
    static std::unordered_map<component_type, sstring, enum_hash<component_type>> _component_map;

    std::unordered_set<component_type, enum_hash<component_type>> _recognized_components;
    std::vector<sstring> _unrecognized_components;

    foreign_ptr<lw_shared_ptr<shareable_components>> _components = make_foreign(make_lw_shared<shareable_components>());
    shared_index_lists _index_lists;
    bool _shared = true;  // across shards; safe default
    // NOTE: _collector and _c_stats are used to generation of statistics file
    // when writing a new sstable.
    metadata_collector _collector;
    column_stats _c_stats;
    file _index_file;
    file _data_file;
    uint64_t _data_file_size;
    uint64_t _index_file_size;
    uint64_t _filter_file_size = 0;
    uint64_t _bytes_on_disk = 0;
    db_clock::time_point _data_file_write_time;
    std::vector<nonwrapping_range<bytes_view>> _clustering_components_ranges;
    std::vector<unsigned> _shards;
    stdx::optional<dht::decorated_key> _first;
    stdx::optional<dht::decorated_key> _last;

    lw_shared_ptr<file_input_stream_history> _single_partition_history = make_lw_shared<file_input_stream_history>();
    lw_shared_ptr<file_input_stream_history> _partition_range_history = make_lw_shared<file_input_stream_history>();

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
        std::experimental::optional<range_tombstone_accumulator> tombstone_accumulator;
        const schema* schemap;
        size_t desired_block_size;
    } _pi_write;

    void maybe_flush_pi_block(file_writer& out,
            const composite& clustering_key,
            const std::vector<bytes_view>& column_names,
            composite::eoc marker = composite::eoc::none);

    schema_ptr _schema;
    sstring _dir;
    unsigned long _generation = 0;
    version_types _version;
    format_types _format;

    filter_tracker _filter_tracker;

    bool _marked_for_deletion = false;

    gc_clock::time_point _now;

    io_error_handler _read_error_handler;
    io_error_handler _write_error_handler;

    const bool has_component(component_type f) const;

    const sstring filename(component_type f) const;

    template <sstable::component_type Type, typename T>
    future<> read_simple(T& comp, const io_priority_class& pc);

    template <sstable::component_type Type, typename T>
    void write_simple(const T& comp, const io_priority_class& pc);

    void generate_toc(compressor c, double filter_fp_chance);
    void write_toc(const io_priority_class& pc);
    future<> seal_sstable();

    future<> read_compression(const io_priority_class& pc);
    void write_compression(const io_priority_class& pc);

    future<> read_scylla_metadata(const io_priority_class& pc);
    void write_scylla_metadata(const io_priority_class& pc, shard_id shard = engine().cpu_id());

    future<> read_filter(const io_priority_class& pc);

    void write_filter(const io_priority_class& pc);

    future<> read_summary(const io_priority_class& pc);

    void write_summary(const io_priority_class& pc) {
        write_simple<component_type::Summary>(_components->summary, pc);
    }

    // To be called when we try to load an SSTable that lacks a Summary. Could
    // happen if old tools are being used.
    future<> generate_summary(const io_priority_class& pc);

    future<> read_statistics(const io_priority_class& pc);
    void write_statistics(const io_priority_class& pc);
    // Rewrite statistics component by creating a temporary Statistics and
    // renaming it into place of existing one.
    void rewrite_statistics(const io_priority_class& pc);
    // Validate metadata that's used to optimize reads when user specifies
    // a clustering key range. If this specific metadata is incorrect, then
    // it should be cleared. Otherwise, it could lead to bad decisions.
    // Metadata is probably incorrect if generated by previous Scylla versions.
    void validate_min_max_metadata();

    void set_first_and_last_keys();

    // Create one range for each clustering component of this sstable.
    // Each range stores min and max value for that specific component.
    // It does nothing if schema defines no clustering key, and it's supposed
    // to be called when loading an existing sstable or after writing a new one.
    void set_clustering_components_ranges();

    future<> create_data();

    // Return an input_stream which reads exactly the specified byte range
    // from the data file (after uncompression, if the file is compressed).
    // Unlike data_read() below, this method does not read the entire byte
    // range into memory all at once. Rather, this method allows reading the
    // data incrementally as a stream. Knowing in advance the exact amount
    // of bytes to be read using this stream, we can make better choices
    // about the buffer size to read, and where exactly to stop reading
    // (even when a large buffer size is used).
    input_stream<char> data_stream(uint64_t pos, size_t len, const io_priority_class& pc,
                                   reader_resource_tracker resource_tracker, lw_shared_ptr<file_input_stream_history> history);

    // Read exactly the specific byte range from the data file (after
    // uncompression, if the file is compressed). This can be used to read
    // a specific row from the data file (its position and length can be
    // determined using the index file).
    // This function is intended (and optimized for) random access, not
    // for iteration through all the rows.
    future<temporary_buffer<char>> data_read(uint64_t pos, size_t len, const io_priority_class& pc);

    future<summary_entry&> read_summary_entry(size_t i);

    // FIXME: pending on Bloom filter implementation
    bool filter_has_key(const schema& s, const dht::decorated_key& dk) { return filter_has_key(key::from_partition_key(s, dk._key)); }

    // NOTE: functions used to generate sstable components.
    void write_row_marker(file_writer& out, const row_marker& marker, const composite& clustering_key);
    void write_clustered_row(file_writer& out, const schema& schema, const clustering_row& clustered_row);
    void write_static_row(file_writer& out, const schema& schema, const row& static_row);
    void write_cell(file_writer& out, atomic_cell_view cell, const column_definition& cdef);
    void write_column_name(file_writer& out, const composite& clustering_key, const std::vector<bytes_view>& column_names, composite::eoc marker = composite::eoc::none);
    void write_column_name(file_writer& out, bytes_view column_names);
    void write_range_tombstone(file_writer& out, const composite& start, composite::eoc start_marker, const composite& end, composite::eoc end_marker, std::vector<bytes_view> suffix, const tombstone t);
    void write_range_tombstone(file_writer& out, const composite& start, const composite& end, std::vector<bytes_view> suffix, const tombstone t) {
        write_range_tombstone(out, start, composite::eoc::start, end, composite::eoc::end, std::move(suffix), std::move(t));
    }
    void write_collection(file_writer& out, const composite& clustering_key, const column_definition& cdef, collection_mutation_view collection);
    void write_row_tombstone(file_writer& out, const composite& key, const row_tombstone t);
    void write_deletion_time(file_writer& out, const tombstone t);

    stdx::optional<std::pair<uint64_t, uint64_t>> get_sample_indexes_for_range(const dht::token_range& range);

    std::vector<unsigned> compute_shards_for_this_sstable() const;
public:
    std::unique_ptr<index_reader> get_index_reader(const io_priority_class& pc);

    future<> read_toc();

    bool has_scylla_component() const {
        return has_component(component_type::Scylla);
    }

    bool filter_has_key(const key& key) {
        return _components->filter->is_present(bytes_view(key));
    }

    bool filter_has_key(utils::hashed_key key) {
        return _components->filter->is_present(key);
    }

    bool filter_has_key(const schema& s, partition_key_view key) {
        return filter_has_key(key::from_partition_key(s, key));
    }

    static utils::hashed_key make_hashed_key(const schema& s, const partition_key& key);

    filter_tracker& get_filter_tracker() { return _filter_tracker; }

    uint64_t filter_get_false_positive() {
        return _filter_tracker.false_positive;
    }
    uint64_t filter_get_true_positive() {
        return _filter_tracker.true_positive;
    }
    uint64_t filter_get_recent_false_positive() {
        auto t = _filter_tracker.false_positive - _filter_tracker.last_false_positive;
        _filter_tracker.last_false_positive = _filter_tracker.false_positive;
        return t;
    }
    uint64_t filter_get_recent_true_positive() {
        auto t = _filter_tracker.true_positive - _filter_tracker.last_true_positive;
        _filter_tracker.last_true_positive = _filter_tracker.true_positive;
        return t;
    }

    const stats_metadata& get_stats_metadata() const {
        auto entry = _components->statistics.contents.find(metadata_type::Stats);
        if (entry == _components->statistics.contents.end()) {
            throw std::runtime_error("Stats metadata not available");
        }
        auto& p = entry->second;
        if (!p) {
            throw std::runtime_error("Statistics is malformed");
        }
        const stats_metadata& s = *static_cast<stats_metadata *>(p.get());
        return s;
    }
    const compaction_metadata& get_compaction_metadata() const {
        auto entry = _components->statistics.contents.find(metadata_type::Compaction);
        if (entry == _components->statistics.contents.end()) {
            throw std::runtime_error("Compaction metadata not available");
        }
        auto& p = entry->second;
        if (!p) {
            throw std::runtime_error("Statistics is malformed");
        }
        const compaction_metadata& s = *static_cast<compaction_metadata *>(p.get());
        return s;
    }
    const std::vector<unsigned>& get_shards_for_this_sstable() const {
        return _shards;
    }

    uint32_t get_sstable_level() const {
        return get_stats_metadata().sstable_level;
    }

    // This will change sstable level only in memory.
    void set_sstable_level(uint32_t);

    double get_compression_ratio() const;

    future<> mutate_sstable_level(uint32_t);

    const summary& get_summary() const {
        return _components->summary;
    }

    // Return sstable key range as range<partition_key> reading only the summary component.
    future<range<partition_key>>
    get_sstable_key_range(const schema& s);

    const std::vector<nonwrapping_range<bytes_view>>& clustering_components_ranges() const;

    // Gets ratio of droppable tombstone. A tombstone is considered droppable here
    // for cells expired before gc_before and regular tombstones older than gc_before.
    double estimate_droppable_tombstone_ratio(gc_clock::time_point gc_before) const;

    // get sstable open info from a loaded sstable, which can be used to quickly open a sstable
    // at another shard.
    future<foreign_sstable_open_info> get_open_info() &;

    // returns all info needed for a sstable to be shared with other shards.
    static future<sstable_open_info> load_shared_components(const schema_ptr& s, sstring dir, int generation, version_types v, format_types f,
        const io_priority_class& pc = default_priority_class());

    // Allow the test cases from sstable_test.cc to test private methods. We use
    // a placeholder to avoid cluttering this class too much. The sstable_test class
    // will then re-export as public every method it needs.
    friend class test;

    friend class components_writer;
    friend class sstable_writer;
    friend class index_reader;
};

struct entry_descriptor {
    sstring ks;
    sstring cf;
    sstable::version_types version;
    int64_t generation;
    sstable::format_types format;
    sstable::component_type component;

    static entry_descriptor make_descriptor(sstring fname);

    entry_descriptor(sstring ks, sstring cf, sstable::version_types version,
                     int64_t generation, sstable::format_types format,
                     sstable::component_type component)
        : ks(ks), cf(cf), version(version), generation(generation), format(format), component(component) {}
};

// Waits for all prior tasks started on current shard related to sstable management to finish.
//
// There may be asynchronous cleanup started from sstable destructor. Since we can't have blocking
// destructors in seastar, that cleanup is not waited for. It can be waited for using this function.
// It is also waited for when seastar exits.
future<> await_background_jobs();

// Invokes await_background_jobs() on all shards
future<> await_background_jobs_on_all_shards();

// When we compact sstables, we have to atomically instantiate the new
// sstable and delete the old ones.  Otherwise, if we compact A+B into C,
// and if A contained some data that was tombstoned by B, and if B was
// deleted but A survived, then data from A will be resurrected.
//
// There are two violators of the requirement to atomically delete
// sstables: first sstable instantiation and deletion on disk is atomic
// only wrt. itself, not other sstables, and second when an sstable is
// shared among shard, so actual on-disk deletion of an sstable is deferred
// until all shards agree it can be deleted.
//
// When shutting down, we will not be able to complete some deletions.
// In that case, an atomic_deletion_cancelled exception is returned instead.
//
// This function only solves the second problem for now.
future<> delete_atomically(std::vector<shared_sstable> ssts);
future<> delete_atomically(std::vector<sstable_to_delete> ssts);

// Cancel any deletions scheduled by delete_atomically() and make their
// futures complete (with an atomic_deletion_cancelled exception).
void cancel_prior_atomic_deletions();

// Like cancel_prior_atomic_deletions(), but will also cause any later deletion attempts to fail.
void cancel_atomic_deletions();

class components_writer {
    sstable& _sst;
    const schema& _schema;
    file_writer& _out;
    file_writer _index;
    bool _index_needs_close;
    uint64_t _max_sstable_size;
    bool _tombstone_written;
    // Remember first and last keys, which we need for the summary file.
    stdx::optional<key> _first_key, _last_key;
    stdx::optional<key> _partition_key;
    uint64_t _next_data_offset_to_write_summary = 0;
    // Enforces ratio of summary to data of 1 to N.
    size_t _summary_byte_cost = default_summary_byte_cost;
private:
    void maybe_add_summary_entry(const dht::token& token, bytes_view key);
    uint64_t get_offset() const;
    file_writer index_file_writer(sstable& sst, const io_priority_class& pc);
    void ensure_tombstone_is_written() {
        if (!_tombstone_written) {
            consume(tombstone());
        }
    }
public:
    components_writer(sstable& sst, const schema& s, file_writer& out, uint64_t estimated_partitions, const sstable_writer_config&, const io_priority_class& pc);
    ~components_writer();
    components_writer(components_writer&& o) : _sst(o._sst), _schema(o._schema), _out(o._out), _index(std::move(o._index)),
            _index_needs_close(o._index_needs_close), _max_sstable_size(o._max_sstable_size), _tombstone_written(o._tombstone_written),
            _first_key(std::move(o._first_key)), _last_key(std::move(o._last_key)), _partition_key(std::move(o._partition_key)),
            _next_data_offset_to_write_summary(o._next_data_offset_to_write_summary), _summary_byte_cost(o._summary_byte_cost) {
        o._index_needs_close = false;
    }

    void consume_new_partition(const dht::decorated_key& dk);
    void consume(tombstone t);
    stop_iteration consume(static_row&& sr);
    stop_iteration consume(clustering_row&& cr);
    stop_iteration consume(range_tombstone&& rt);
    stop_iteration consume_end_of_partition();
    void consume_end_of_stream();

    static constexpr size_t default_summary_byte_cost = 2000;
    static void maybe_add_summary_entry(summary& s, const dht::token& token, bytes_view key, uint64_t data_offset,
        uint64_t index_offset, uint64_t& next_data_offset_to_write_summary, size_t summary_byte_cost);
};

class sstable_writer {
    sstable& _sst;
    const schema& _schema;
    const io_priority_class& _pc;
    bool _backup;
    bool _leave_unsealed;
    bool _compression_enabled;
    std::unique_ptr<file_writer> _writer;
    stdx::optional<components_writer> _components_writer;
    shard_id _shard; // Specifies which shard new sstable will belong to.
    seastar::shared_ptr<write_monitor> _monitor;
private:
    void prepare_file_writer();
    void finish_file_writer();
public:
    sstable_writer(sstable& sst, const schema& s, uint64_t estimated_partitions,
            const sstable_writer_config&, const io_priority_class& pc, shard_id shard = engine().cpu_id());
    ~sstable_writer();
    sstable_writer(sstable_writer&& o) : _sst(o._sst), _schema(o._schema), _pc(o._pc), _backup(o._backup),
            _leave_unsealed(o._leave_unsealed), _compression_enabled(o._compression_enabled), _writer(std::move(o._writer)),
            _components_writer(std::move(o._components_writer)), _shard(o._shard), _monitor(std::move(o._monitor)) {}
    void consume_new_partition(const dht::decorated_key& dk) { return _components_writer->consume_new_partition(dk); }
    void consume(tombstone t) { _components_writer->consume(t); }
    stop_iteration consume(static_row&& sr) { return _components_writer->consume(std::move(sr)); }
    stop_iteration consume(clustering_row&& cr) { return _components_writer->consume(std::move(cr)); }
    stop_iteration consume(range_tombstone&& rt) { return _components_writer->consume(std::move(rt)); }
    stop_iteration consume_end_of_partition() { return _components_writer->consume_end_of_partition(); }
    void consume_end_of_stream();
};

// contains data for loading a sstable using components shared by a single shard;
// can be moved across shards
struct foreign_sstable_open_info {
    foreign_ptr<lw_shared_ptr<sstable::shareable_components>> components;
    std::vector<shard_id> owners;
    seastar::file_handle data;
    seastar::file_handle index;
    uint64_t generation;
    sstable::version_types version;
    sstable::format_types format;
};

// can only be used locally
struct sstable_open_info {
    lw_shared_ptr<sstable::shareable_components> components;
    std::vector<shard_id> owners;
    file data;
    file index;
};

future<> init_metrics();

}
