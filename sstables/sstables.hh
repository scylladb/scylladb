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

#include "core/file.hh"
#include "core/fstream.hh"
#include "core/future.hh"
#include "core/sstring.hh"
#include "core/enum.hh"
#include "core/shared_ptr.hh"
#include "core/distributed.hh"
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
#include "core/stream.hh"
#include "writer.hh"
#include "metadata_collector.hh"
#include "filter.hh"
#include "exceptions.hh"
#include "mutation_reader.hh"
#include "query-request.hh"
#include "key_reader.hh"
#include "compound_compat.hh"

namespace sstables {

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
class data_consume_context {
    class impl;
    std::unique_ptr<impl> _pimpl;
    // This object can only be constructed by sstable::data_consume_rows()
    data_consume_context(std::unique_ptr<impl>);
    friend class sstable;
public:
    future<> read();
    // Define (as defaults) the destructor and move operations in the source
    // file, so here we don't need to know the incomplete impl type.
    ~data_consume_context();
    data_consume_context(data_consume_context&&) noexcept;
    data_consume_context& operator=(data_consume_context&&) noexcept;
};

// mutation_reader is an object returned by sstable::read_rows() et al. which
// allows getting each sstable row in sequence, in mutation format.
//
// The read() method reads the next mutation, returning a disengaged optional
// on EOF. As usual for future-returning functions, a caller which starts a
// read() MUST ensure that the mutation_reader object continues to live until
// the returned future is fulfilled.  Moreover, the sstable whose read_rows()
// method was used to open this mutation_reader must also live between the
// time read() is called and its future ends.
// As soon as the future returned by read() completes, the object may safely
// be deleted. In other words, when the read() future is fulfilled, we can
// be sure there are no background tasks still scheduled.
class mutation_reader {
    class impl;
    std::unique_ptr<impl> _pimpl;
    // This object can only be constructed by sstable::read_rows() et al.
    mutation_reader(std::unique_ptr<impl>);
    friend class sstable;
public:
    future<streamed_mutation_opt> read();
    // Define (as defaults) the destructor and move operations in the source
    // file, so here we don't need to know the incomplete impl type.
    ~mutation_reader();
    mutation_reader(mutation_reader&&);
    mutation_reader& operator=(mutation_reader&&);
};

class key;
class sstable_writer;

using index_list = std::vector<index_entry>;

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
    };
    enum class version_types { ka, la };
    enum class format_types { big };
public:
    sstable(schema_ptr schema, sstring dir, int64_t generation, version_types v, format_types f, gc_clock::time_point now = gc_clock::now())
        : _schema(std::move(schema))
        , _dir(std::move(dir))
        , _generation(generation)
        , _version(v)
        , _format(f)
        , _now(now)
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
    // columns we are looking for. When the range to be read does NOT include
    // the entire row, the caller needs to supply the optional "row_info"
    // containing information about the entire row (key and deletion time)
    // which is normally read from the beginning of the row.
    struct disk_read_range {
        // TODO: this should become a vector of ranges
        uint64_t start;
        uint64_t end;
        // When the range above does not cover the beginning of the sstable
        // row, we need to supply information which is only available at the
        // beginning of the row - the row's key and its tombstone if any.
        struct row_info {
            key k;
            deletion_time deltime;
        };
        std::experimental::optional<row_info> ri;
        disk_read_range() : start(0), end(0) {}
        disk_read_range(uint64_t start, uint64_t end) :
            start(start), end(end) { }
        disk_read_range(uint64_t start, uint64_t end, const key& key, const deletion_time& deltime) :
            start(start), end(end), ri(row_info{key, deltime}) { }
        explicit operator bool() const {
            return start != end;
        }
        // found_row() is true if the row was found. This is not the same as
        // operator bool(): It is possible that found_row() but the promoted
        // index ruled out anything to read (in this case "ri" was set).
        bool found_row() const {
            return start != end || ri;
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
    data_consume_context data_consume_rows(row_consumer& consumer, disk_read_range toread);

    // Like data_consume_rows() with bounds, but iterates over whole range
    data_consume_context data_consume_rows(row_consumer& consumer);

    static component_type component_from_sstring(sstring& s);
    static version_types version_from_sstring(sstring& s);
    static format_types format_from_sstring(sstring& s);
    static const sstring filename(sstring dir, sstring ks, sstring cf, version_types version, int64_t generation,
                                  format_types format, component_type component);
    // WARNING: it should only be called to remove components of a sstable with
    // a temporary TOC file.
    static future<> remove_sstable_with_temp_toc(sstring ks, sstring cf, sstring dir, int64_t generation,
                                                 version_types v, format_types f);

    future<> load();
    future<> open_data();

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
        const key& k,
        const query::partition_slice& slice = query::full_slice,
        const io_priority_class& pc = default_priority_class());
    /**
     * @param schema a schema_ptr object describing this table
     * @param min the minimum token we want to search for (inclusive)
     * @param max the maximum token we want to search for (inclusive)
     * @return a mutation_reader object that can be used to iterate over
     * mutations.
     */
    mutation_reader read_range_rows(schema_ptr schema,
            const dht::token& min, const dht::token& max,
            const io_priority_class& pc = default_priority_class());

    // Returns a mutation_reader for given range of partitions
    mutation_reader read_range_rows(
        schema_ptr schema,
        const query::partition_range& range,
        const query::partition_slice& slice = query::full_slice,
        const io_priority_class& pc = default_priority_class());

    // read_rows() returns each of the rows in the sstable, in sequence,
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
    mutation_reader read_rows(schema_ptr schema, const io_priority_class& pc = default_priority_class());

    // Write sstable components from a memtable.
    future<> write_components(memtable& mt, bool backup = false,
                              const io_priority_class& pc = default_priority_class(), bool leave_unsealed = false);

    future<> write_components(::mutation_reader mr,
            uint64_t estimated_partitions, schema_ptr schema, uint64_t max_sstable_size, bool backup = false,
            const io_priority_class& pc = default_priority_class(), bool leave_unsealed = false);

    sstable_writer get_writer(const schema& s, uint64_t estimated_partitions, uint64_t max_sstable_size,
                              bool backup = false, const io_priority_class& pc = default_priority_class(),
                              bool leave_unsealed = false);

    future<> seal_sstable(bool backup);

    uint64_t get_estimated_key_count() const {
        return ((uint64_t)_summary.header.size_at_full_sampling + 1) *
                _summary.header.min_index_interval;
    }

    // mark_for_deletion() specifies that a sstable isn't relevant to the
    // current shard, and thus can be deleted by the deletion manager, if
    // all shards sharing it agree. In case the sstable is unshared, it's
    // guaranteed that all of its on-disk files will be deleted as soon as
    // the in-memory object is destroyed.
    void mark_for_deletion() {
        _marked_for_deletion = true;
    }

    future<> mark_for_deletion_on_disk();

    bool marked_for_deletion() const {
        return _marked_for_deletion;
    }

    void add_ancestor(int64_t generation) {
        _collector.add_ancestor(generation);
    }

    // Returns true iff this sstable contains data which belongs to many shards.
    bool is_shared() const {
        return _shared;
    }

    void set_unshared() {
        _shared = false;
    }

    uint64_t data_size() const;
    uint64_t index_size() const {
        return _index_file_size;
    }
    uint64_t filter_size() const {
        return _filter_file_size;
    }

    uint64_t filter_memory_size() const {
        return _filter->memory_size();
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

private:
    sstable(size_t wbuffer_size, schema_ptr schema, sstring dir, int64_t generation, version_types v, format_types f, gc_clock::time_point now = gc_clock::now())
        : sstable_buffer_size(wbuffer_size)
        , _schema(std::move(schema))
        , _dir(std::move(dir))
        , _generation(generation)
        , _version(v)
        , _format(f)
        , _now(now)
    { }

    size_t sstable_buffer_size = 128*1024;

    static std::unordered_map<version_types, sstring, enum_hash<version_types>> _version_string;
    static std::unordered_map<format_types, sstring, enum_hash<format_types>> _format_string;
    static std::unordered_map<component_type, sstring, enum_hash<component_type>> _component_map;

    std::unordered_set<component_type, enum_hash<component_type>> _components;

    bool _shared = true;  // across shards; safe default
    compression _compression;
    utils::filter_ptr _filter;
    summary _summary;
    statistics _statistics;
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
    std::vector<nonwrapping_range<bytes_view>> _clustering_components_ranges;
    stdx::optional<dht::decorated_key> _first;
    stdx::optional<dht::decorated_key> _last;

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
            const std::vector<bytes_view>& column_names);

    schema_ptr _schema;
    sstring _dir;
    unsigned long _generation = 0;
    version_types _version;
    format_types _format;

    filter_tracker _filter_tracker;

    bool _marked_for_deletion = false;

    gc_clock::time_point _now;

    const bool has_component(component_type f) const;

    const sstring filename(component_type f) const;

    template <sstable::component_type Type, typename T>
    future<> read_simple(T& comp, const io_priority_class& pc);

    template <sstable::component_type Type, typename T>
    void write_simple(T& comp, const io_priority_class& pc);

    void generate_toc(compressor c, double filter_fp_chance);
    void write_toc(const io_priority_class& pc);
    future<> seal_sstable();

    future<> read_compression(const io_priority_class& pc);
    void write_compression(const io_priority_class& pc);

    future<> read_filter(const io_priority_class& pc);

    void write_filter(const io_priority_class& pc);

    future<> read_summary(const io_priority_class& pc);

    void write_summary(const io_priority_class& pc) {
        write_simple<component_type::Summary>(_summary, pc);
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

    future<index_list> read_indexes(uint64_t summary_idx, const io_priority_class& pc);

    // Return an input_stream which reads exactly the specified byte range
    // from the data file (after uncompression, if the file is compressed).
    // Unlike data_read() below, this method does not read the entire byte
    // range into memory all at once. Rather, this method allows reading the
    // data incrementally as a stream. Knowing in advance the exact amount
    // of bytes to be read using this stream, we can make better choices
    // about the buffer size to read, and where exactly to stop reading
    // (even when a large buffer size is used).
    input_stream<char> data_stream(uint64_t pos, size_t len, const io_priority_class& pc);

    // Read exactly the specific byte range from the data file (after
    // uncompression, if the file is compressed). This can be used to read
    // a specific row from the data file (its position and length can be
    // determined using the index file).
    // This function is intended (and optimized for) random access, not
    // for iteration through all the rows.
    future<temporary_buffer<char>> data_read(uint64_t pos, size_t len, const io_priority_class& pc);

    future<uint64_t> data_end_position(uint64_t summary_idx, uint64_t index_idx, const index_list& il, const io_priority_class& pc);

    // Returns data file position for an entry right after all entries mapped by given summary page.
    future<uint64_t> data_end_position(uint64_t summary_idx, const io_priority_class& pc);

    template <typename T>
    int binary_search(const T& entries, const key& sk, const dht::token& token);

    template <typename T>
    int binary_search(const T& entries, const key& sk) {
        return binary_search(entries, sk, dht::global_partitioner().get_token(key_view(sk)));
    }

    // Returns position in the data file of the first entry which is not
    // smaller than the supplied ring_position. If no such entry exists, a
    // position right after all entries is returned.
    //
    // The ring_position doesn't have to survive deferring.
    future<uint64_t> lower_bound(schema_ptr, const dht::ring_position&, const io_priority_class& pc);

    // Returns position in the data file of the first partition which is
    // greater than the supplied ring_position. If no such entry exists, a
    // position right after all entries is returned.
    //
    // The ring_position doesn't have to survive deferring.
    future<uint64_t> upper_bound(schema_ptr, const dht::ring_position&, const io_priority_class& pc);

    // find_disk_ranges finds the ranges of bytes we need to read from the
    // sstable to read the desired columns out of the given key. This range
    // may be the entire byte range of the given partition - as found using
    // the summary and index files - but if the index contains a "promoted
    // index" (a sample of column positions for each key) it may be a smaller
    // range. The returned range may contain columns beyond those requested
    // in slice, so it is the reader's duty to use slice again
    // when parsing the data read from the returned range.
    future<disk_read_range> find_disk_ranges(schema_ptr schema,
            const sstables::key& key,
            const query::partition_slice& slice,
            const io_priority_class& pc);

    future<summary_entry&> read_summary_entry(size_t i);

    // FIXME: pending on Bloom filter implementation
    bool filter_has_key(const schema& s, const dht::decorated_key& dk) { return filter_has_key(key::from_partition_key(s, dk._key)); }

    // NOTE: functions used to generate sstable components.
    void write_row_marker(file_writer& out, const row_marker& marker, const composite& clustering_key);
    void write_clustered_row(file_writer& out, const schema& schema, const clustering_row& clustered_row);
    void write_static_row(file_writer& out, const schema& schema, const row& static_row);
    void write_cell(file_writer& out, atomic_cell_view cell);
    void write_column_name(file_writer& out, const composite& clustering_key, const std::vector<bytes_view>& column_names, composite::eoc marker = composite::eoc::none);
    void write_column_name(file_writer& out, bytes_view column_names);
    void write_range_tombstone(file_writer& out, const composite& start, bound_kind start_kind, const composite& end, bound_kind stop_kind, std::vector<bytes_view> suffix, const tombstone t);
    void write_range_tombstone(file_writer& out, const composite& start, const composite& end, std::vector<bytes_view> suffix, const tombstone t) {
        write_range_tombstone(out, start, bound_kind::incl_start, end, bound_kind::incl_end, std::move(suffix), std::move(t));
    }
    void write_collection(file_writer& out, const composite& clustering_key, const column_definition& cdef, collection_mutation_view collection);
public:
    future<> read_toc();

    bool filter_has_key(const key& key) {
        return _filter->is_present(bytes_view(key));
    }

    bool filter_has_key(const schema& s, partition_key_view key) {
        return filter_has_key(key::from_partition_key(s, key));
    }

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
        auto entry = _statistics.contents.find(metadata_type::Stats);
        if (entry == _statistics.contents.end()) {
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
        auto entry = _statistics.contents.find(metadata_type::Compaction);
        if (entry == _statistics.contents.end()) {
            throw std::runtime_error("Compaction metadata not available");
        }
        auto& p = entry->second;
        if (!p) {
            throw std::runtime_error("Statistics is malformed");
        }
        const compaction_metadata& s = *static_cast<compaction_metadata *>(p.get());
        return s;
    }

    uint32_t get_sstable_level() const {
        return get_stats_metadata().sstable_level;
    }

    // This will change sstable level only in memory.
    void set_sstable_level(uint32_t);

    double get_compression_ratio() const;

    future<> mutate_sstable_level(uint32_t);

    const summary& get_summary() const {
        return _summary;
    }

    // Return sstable key range as range<partition_key> reading only the summary component.
    future<range<partition_key>>
    get_sstable_key_range(const schema& s);

    const std::vector<nonwrapping_range<bytes_view>>& clustering_components_ranges() const;

    // Used to mark a sstable for deletion that is not relevant to the current shard.
    // It doesn't mean that the sstable will be deleted, but that the sstable is not
    // relevant to the current shard, thus can be deleted by the deletion manager.
    static void mark_sstable_for_deletion(const schema_ptr& schema, sstring dir, int64_t generation, version_types v, format_types f);

    // Allow the test cases from sstable_test.cc to test private methods. We use
    // a placeholder to avoid cluttering this class too much. The sstable_test class
    // will then re-export as public every method it needs.
    friend class test;

    friend class key_reader;
    friend class components_writer;
    friend class sstable_writer;
};

using shared_sstable = lw_shared_ptr<sstable>;
using sstable_list = std::unordered_set<shared_sstable>;

::key_reader make_key_reader(schema_ptr s, shared_sstable sst, const query::partition_range& range,
                             const io_priority_class& pc = default_priority_class());

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

struct sstable_to_delete {
    sstable_to_delete(sstring name, bool shared) : name(std::move(name)), shared(shared) {}
    sstring name;
    bool shared = false;
    friend std::ostream& operator<<(std::ostream& os, const sstable_to_delete& std);
};


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

class atomic_deletion_cancelled : public std::exception {
    std::string _msg;
public:
    explicit atomic_deletion_cancelled(std::vector<sstring> names);
    template <typename StringRange>
    explicit atomic_deletion_cancelled(StringRange range)
            : atomic_deletion_cancelled(std::vector<sstring>{range.begin(), range.end()}) {
    }
    const char* what() const noexcept override;
};

// Cancel any deletions scheduled by delete_atomically() and make their
// futures complete (with an atomic_deletion_cancelled exception).
void cancel_atomic_deletions();

// Read toc content and delete all components found in it.
future<> remove_by_toc_name(sstring sstable_toc_name);

class components_writer {
    sstable& _sst;
    const schema& _schema;
    file_writer& _out;
    file_writer _index;
    uint64_t _max_sstable_size;
    bool _tombstone_written;
    // Remember first and last keys, which we need for the summary file.
    stdx::optional<key> _first_key, _last_key;
    stdx::optional<key> _partition_key;
private:
    size_t get_offset();
    file_writer index_file_writer(sstable& sst, const io_priority_class& pc);
    void ensure_tombstone_is_written() {
        if (!_tombstone_written) {
            consume(tombstone());
        }
    }
public:
    components_writer(sstable& sst, const schema& s, file_writer& out, uint64_t estimated_partitions, uint64_t max_sstable_size, const io_priority_class& pc);

    void consume_new_partition(const dht::decorated_key& dk);
    void consume(tombstone t);
    stop_iteration consume(static_row&& sr);
    stop_iteration consume(clustering_row&& cr);
    stop_iteration consume(range_tombstone&& rt);
    stop_iteration consume_end_of_partition();
    void consume_end_of_stream();
};

class sstable_writer {
    sstable& _sst;
    const schema& _schema;
    const io_priority_class& _pc;
    bool _backup;
    bool _leave_unsealed;
    bool _compression_enabled;
    shared_ptr<file_writer> _writer;
    stdx::optional<components_writer> _components_writer;
private:
    void prepare_file_writer();
    void finish_file_writer();
public:
    sstable_writer(sstable& sst, const schema& s, uint64_t estimated_partitions,
                   uint64_t max_sstable_size, bool backup, bool leave_unsealed, const io_priority_class& pc);
    void consume_new_partition(const dht::decorated_key& dk) { return _components_writer->consume_new_partition(dk); }
    void consume(tombstone t) { _components_writer->consume(t); }
    stop_iteration consume(static_row&& sr) { return _components_writer->consume(std::move(sr)); }
    stop_iteration consume(clustering_row&& cr) { return _components_writer->consume(std::move(cr)); }
    stop_iteration consume(range_tombstone&& rt) { return _components_writer->consume(std::move(rt)); }
    stop_iteration consume_end_of_partition() { return _components_writer->consume_end_of_partition(); }
    void consume_end_of_stream();
};

}
