/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
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
    future<mutation_opt> read();
    // Define (as defaults) the destructor and move operations in the source
    // file, so here we don't need to know the incomplete impl type.
    ~mutation_reader();
    mutation_reader(mutation_reader&&);
    mutation_reader& operator=(mutation_reader&&);
};

class key;

using index_list = std::vector<index_entry>;

class sstable {
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
    };
    enum class version_types { ka, la };
    enum class format_types { big };
public:
    sstable(sstring ks, sstring cf, sstring dir, int64_t generation, version_types v, format_types f, gc_clock::time_point now = gc_clock::now())
        : _ks(std::move(ks))
        , _cf(std::move(cf))
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
    data_consume_context data_consume_rows(row_consumer& consumer, uint64_t start, uint64_t end);

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

    future<mutation_opt> read_row(schema_ptr schema, const key& k);
    /**
     * @param schema a schema_ptr object describing this table
     * @param min the minimum token we want to search for (inclusive)
     * @param max the maximum token we want to search for (inclusive)
     * @return a mutation_reader object that can be used to iterate over
     * mutations.
     */
    mutation_reader read_range_rows(schema_ptr schema,
            const dht::token& min, const dht::token& max);

    // Returns a mutation_reader for given range of partitions
    mutation_reader read_range_rows(schema_ptr schema, const query::partition_range& range);

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
    mutation_reader read_rows(schema_ptr schema);

    // Write sstable components from a memtable.
    future<> write_components(memtable& mt, bool backup = false);
    future<> write_components(::mutation_reader mr,
            uint64_t estimated_partitions, schema_ptr schema, uint64_t max_sstable_size, bool backup = false);

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

    bool marked_for_deletion() const {
        return _marked_for_deletion;
    }

    void add_ancestor(int64_t generation) {
        _collector.add_ancestor(generation);
    }

    // Returns true iff this sstable contains data which belongs to many shards.
    bool is_shared() {
        return _shared;
    }

    void set_unshared() {
        _shared = false;
    }

    uint64_t data_size();
    uint64_t index_size() {
        return _index_file_size;
    }
    uint64_t filter_size() {
        return _filter_file_size;
    }

    uint64_t filter_memory_size() {
        return _filter->memory_size();
    }

    // Returns the total bytes of all components.
    future<uint64_t> bytes_on_disk();

    partition_key get_first_partition_key(const schema& s) const;
    partition_key get_last_partition_key(const schema& s) const;

    dht::decorated_key get_first_decorated_key(const schema& s) const;
    dht::decorated_key get_last_decorated_key(const schema& s) const;

    // SSTable comparator using the first key (decorated key).
    // Return values are those of a trichotomic comparison.
    int compare_by_first_key(const schema& s, const sstable& other) const;

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
    sstable(size_t wbuffer_size, sstring ks, sstring cf, sstring dir, int64_t generation, version_types v, format_types f, gc_clock::time_point now = gc_clock::now())
        : sstable_buffer_size(wbuffer_size)
        , _ks(std::move(ks))
        , _cf(std::move(cf))
        , _dir(std::move(dir))
        , _generation(generation)
        , _version(v)
        , _format(f)
        , _now(now)
    { }

    size_t sstable_buffer_size = 128*1024;

    void do_write_components(::mutation_reader mr,
            uint64_t estimated_partitions, schema_ptr schema, uint64_t max_sstable_size, file_writer& out);
    void prepare_write_components(::mutation_reader mr,
            uint64_t estimated_partitions, schema_ptr schema, uint64_t max_sstable_size);
    static future<> shared_remove_by_toc_name(sstring toc_name, bool shared);
    static std::unordered_map<version_types, sstring, enum_hash<version_types>> _version_string;
    static std::unordered_map<format_types, sstring, enum_hash<format_types>> _format_string;
    static std::unordered_map<component_type, sstring, enum_hash<component_type>> _component_map;
    static thread_local std::unordered_map<sstring, unsigned> _shards_agreeing_to_remove_sstable;

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

    sstring _ks;
    sstring _cf;
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
    future<> read_simple(T& comp);

    template <sstable::component_type Type, typename T>
    void write_simple(T& comp);

    void generate_toc(compressor c, double filter_fp_chance);
    void write_toc();
    void seal_sstable();

    future<> read_compression();
    void write_compression();

    future<> read_filter();

    void write_filter();

    future<> read_summary() {
        return read_simple<component_type::Summary>(_summary);
    }
    void write_summary() {
        write_simple<component_type::Summary>(_summary);
    }

    future<> read_statistics();
    void write_statistics();

    future<> create_data();

    future<index_list> read_indexes(uint64_t summary_idx);

    input_stream<char> data_stream_at(uint64_t pos, uint64_t buf_size = 8192);

    // Read exactly the specific byte range from the data file (after
    // uncompression, if the file is compressed). This can be used to read
    // a specific row from the data file (its position and length can be
    // determined using the index file).
    // This function is intended (and optimized for) random access, not
    // for iteration through all the rows.
    future<temporary_buffer<char>> data_read(uint64_t pos, size_t len);

    future<uint64_t> data_end_position(uint64_t summary_idx, uint64_t index_idx, const index_list& il);

    // Returns data file position for an entry right after all entries mapped by given summary page.
    future<uint64_t> data_end_position(uint64_t summary_idx);

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
    future<uint64_t> lower_bound(schema_ptr, const dht::ring_position&);

    // Returns position in the data file of the first partition which is
    // greater than the supplied ring_position. If no such entry exists, a
    // position right after all entries is returned.
    //
    // The ring_position doesn't have to survive deferring.
    future<uint64_t> upper_bound(schema_ptr, const dht::ring_position&);

    future<summary_entry&> read_summary_entry(size_t i);

    // FIXME: pending on Bloom filter implementation
    bool filter_has_key(const key& key) { return _filter->is_present(bytes_view(key)); }
    bool filter_has_key(const schema& s, const dht::decorated_key& dk) { return filter_has_key(key::from_partition_key(s, dk._key)); }

    // NOTE: functions used to generate sstable components.
    void write_row_marker(file_writer& out, const rows_entry& clustered_row, const composite& clustering_key);
    void write_clustered_row(file_writer& out, const schema& schema, const rows_entry& clustered_row);
    void write_static_row(file_writer& out, const schema& schema, const row& static_row);
    void write_cell(file_writer& out, atomic_cell_view cell);
    void write_column_name(file_writer& out, const composite& clustering_key, const std::vector<bytes_view>& column_names, composite_marker m = composite_marker::none);
    void write_column_name(file_writer& out, bytes_view column_names);
    void write_range_tombstone(file_writer& out, const composite& clustering_prefix, std::vector<bytes_view> suffix, const tombstone t);
    void write_collection(file_writer& out, const composite& clustering_key, const column_definition& cdef, collection_mutation_view collection);
public:
    future<> read_toc();

    bool filter_has_key(const schema& s, const partition_key& key) {
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

    future<> mutate_sstable_level(uint32_t);

    const summary& get_summary() const {
        return _summary;
    }

    // Return sstable key range as range<partition_key> reading only the summary component.
    static future<range<partition_key>>
    get_sstable_key_range(const schema& s, sstring ks, sstring cf, sstring dir, int64_t generation, version_types v, format_types f);

    // Used to mark a sstable for deletion that is not relevant to the current shard.
    // It doesn't mean that the sstable will be deleted, but that the sstable is not
    // relevant to the current shard, thus can be deleted by the deletion manager.
    static void mark_sstable_for_deletion(sstring ks, sstring cf, sstring dir, int64_t generation, version_types v, format_types f);

    // Allow the test cases from sstable_test.cc to test private methods. We use
    // a placeholder to avoid cluttering this class too much. The sstable_test class
    // will then re-export as public every method it needs.
    friend class test;

    friend class key_reader;
};

using shared_sstable = lw_shared_ptr<sstable>;
using sstable_list = std::map<int64_t, shared_sstable>;

::key_reader make_key_reader(schema_ptr s, shared_sstable sst, const query::partition_range& range);

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

}
