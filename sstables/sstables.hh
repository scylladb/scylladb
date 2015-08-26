/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 *
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
#include "database.hh"
#include "dht/i_partitioner.hh"
#include "schema.hh"
#include "mutation.hh"
#include "utils/i_filter.hh"
#include "core/stream.hh"
#include "writer.hh"
#include "metadata_collector.hh"
#include "filter.hh"

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
    data_consume_context(data_consume_context&&);
    data_consume_context& operator=(data_consume_context&&);
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

class malformed_sstable_exception : public std::exception {
    sstring _msg;
public:
    malformed_sstable_exception(sstring s) : _msg(s) {}
    const char *what() const noexcept {
        return _msg.c_str();
    }
};

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
    };
    enum class version_types { ka, la };
    enum class format_types { big };
public:
    sstable(sstring ks, sstring cf, sstring dir, unsigned long generation, version_types v, format_types f, gc_clock::time_point now = gc_clock::now())
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
    static const sstring filename(sstring dir, sstring ks, sstring cf, version_types version, unsigned long generation,
                                  format_types format, component_type component, bool temporary = false);

    future<> load();

    void set_generation(unsigned long generation) {
        _generation = generation;
    }
    unsigned long generation() const {
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
    future<> write_components(const memtable& mt);
    future<> write_components(::mutation_reader mr,
            uint64_t estimated_partitions, schema_ptr schema);

    uint64_t get_estimated_key_count() const {
        return ((uint64_t)_summary.header.size_at_full_sampling + 1) *
                _summary.header.min_index_interval;
    }

    // mark_for_deletion() specifies that the on-disk files for this sstable
    // should be deleted as soon as the in-memory object is destructed.
    void mark_for_deletion() {
        _marked_for_deletion = true;
    }

    void add_ancestor(int generation) {
        _collector.add_ancestor(generation);
    }

    // Returns true iff this sstable contains data which belongs to many shards.
    bool is_shared() {
        return true; // FIXME: set to false for sstables created by compaction process
    }

    uint64_t data_size();
    uint64_t index_size() {
        return _index_file_size;
    }
    uint64_t filter_size() {
        return _filter_file_size;
    }

    // Returns the total bytes of all components.
    future<uint64_t> bytes_on_disk();

    partition_key get_first_partition_key(const schema& s) const;
    partition_key get_last_partition_key(const schema& s) const;

    const sstring get_filename() {
        return filename(component_type::Data);
    }
private:
    sstable(size_t wbuffer_size, sstring ks, sstring cf, sstring dir, unsigned long generation, version_types v, format_types f, gc_clock::time_point now = gc_clock::now())
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
            uint64_t estimated_partitions, schema_ptr schema, file_writer& out);
    void prepare_write_components(::mutation_reader mr,
            uint64_t estimated_partitions, schema_ptr schema);
    static std::unordered_map<version_types, sstring, enum_hash<version_types>> _version_string;
    static std::unordered_map<format_types, sstring, enum_hash<format_types>> _format_string;
    static std::unordered_map<component_type, sstring, enum_hash<component_type>> _component_map;

    std::unordered_set<component_type, enum_hash<component_type>> _components;

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

    const bool has_component(component_type f);

    const sstring filename(component_type f);
    const sstring temporary_filename(component_type f);

    template <sstable::component_type Type, typename T>
    future<> read_simple(T& comp);

    template <sstable::component_type Type, typename T>
    void write_simple(T& comp);

    future<> read_toc();
    void write_toc();

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

    future<> open_data();
    future<> create_data();

    future<index_list> read_indexes(uint64_t position, uint64_t quantity);

    future<index_list> read_indexes(uint64_t position) {
        return read_indexes(position, _summary.header.sampling_level);
    }

    input_stream<char> data_stream_at(uint64_t pos);

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
    void write_collection(file_writer& out, const composite& clustering_key, const column_definition& cdef, collection_mutation::view collection);
public:
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

    // Allow the test cases from sstable_test.cc to test private methods. We use
    // a placeholder to avoid cluttering this class too much. The sstable_test class
    // will then re-export as public every method it needs.
    friend class test;
};

using shared_sstable = lw_shared_ptr<sstable>;

struct entry_descriptor {
    sstring ks;
    sstring cf;
    sstable::version_types version;
    unsigned long generation;
    sstable::format_types format;
    sstable::component_type component;

    static entry_descriptor make_descriptor(sstring fname);

    entry_descriptor(sstring ks, sstring cf, sstable::version_types version,
                     unsigned long generation, sstable::format_types format,
                     sstable::component_type component)
        : ks(ks), cf(cf), version(version), generation(generation), format(format), component(component) {}
};
}
