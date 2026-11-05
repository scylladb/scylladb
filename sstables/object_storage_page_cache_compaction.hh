/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// This file is part of the object storage page cache (the 'disk_cache': 'pages'
// keyspace storage option): compaction-resistant caching, which keeps the
// cache of the data which compaction rewrites.
//
// Compaction replaces its input sstables with new ones, and when the inputs
// are deleted, their cached pages are evicted (see
// object_storage_base::destroy()). Without this, the new sstables would start
// with nothing cached, so every compaction would lose the cache of the data
// it compacts - and a workload which writes would keep losing its cache, and
// a major compaction would drop the whole cache. So while compaction writes
// a page of an output sstable, if the data in that page was cached in an
// input sstable, the page is cached too - so when compaction ends, and its
// inputs' pages are evicted, the output already has their data cached. The
// cache only grows, temporarily, by the pages which compaction copies, until
// its inputs are deleted - which incremental compaction does while it runs.
//
// The pages of the inputs and of the outputs don't correspond by their
// offsets - an output page holds the merged data of all the inputs - only by
// the keys of their data. The mapping must be accurate to about a page: when
// the cache holds a small part of the data, its pages are scattered (the
// pages which point reads read), so an output page next to the right one
// would be useless. So it can't be found from when compaction read the
// inputs' pages and wrote the output's pages - each input is read ahead of
// the merged data which is written, by many pages. Instead:
//
// * page_cache_compaction_input finds which fragments (partition starts,
//   rows, range tombstone changes, partition ends) of an input sstable were
//   in its cached pages ("hot"): the sstable reader tells it where each
//   fragment starts in the Data component, and it looks up whether that
//   position's page is cached. It doesn't learn this from the reads of the
//   pages: compaction reads the cached pages of an sstable only until the
//   first page which isn't cached, and then reads the rest from the object
//   store (see object_storage_page_cache_source.hh). It reads the sstable's
//   rows in pagecache_hits instead, alongside the reader, with
//   object_storage_page_cache::get_page_hits(), which also tells when each
//   page was last hit.
//
// * page_cache_compaction_output finds which pages of the output's components
//   to copy: the sstable writer tells it the key and offset of each fragment
//   it writes, which is hot if an input had a hot fragment with the same key
//   (partition and position in it) - and a page of the Data component is
//   copied if enough of it is hot data (see below). A partition's entries in
//   the index are copied if its hot data is in pages which are copied.
//
// * The sink which writes an output's component to the object store copies
//   those pages to the cache, as it writes them (see
//   make_page_cache_compaction_sink()).
//
// The merged data is written in the order of its keys, and every input is
// read past a key before the data at that key is written - the merge needs
// the next fragment of every input to choose which is first - so each input
// tells the output in order which of its fragments are hot, and the output
// asks it in order, keeping little in memory.
//
// Why only pages which are mostly hot: the cache knows which pages were read,
// not which of their rows. A page of an input which is dense in the output's
// keys - a large sstable - holds about the same data as a page of the
// output, so if it was cached, the output page which holds its data should
// be. But a page of a small input - such as an sstable which a memtable
// flush wrote, whose rows are spread thinly over the keys - holds a range of
// keys which fills many pages of the output (about as many as the output is
// larger than the input), each holding a few of its rows among many which
// weren't cached. Copying all of these pages would turn one cached page
// into tens or hundreds, for a read which probably needed only one of its
// rows - and we don't know which. So a page of the Data
// component is copied only if at least a 16th of its data is hot (see
// page_cache_compaction_output::min_hot_fraction_denominator).
// This keeps the pages which hold the data of dense cached pages - and the
// rows next to them, such as new rows which a write added between them -
// while a row which only a sparse page had cached is read again from the
// object store after the compaction. And because a read of a row reads the
// data around it - a forward read from the start of its block in the
// partition's row index (or of the partition, if it has no row index), a
// reversed read the whole block - and the output's blocks aren't the
// inputs', a copied page also copies all the blocks with hot data in it,
// whole.
//
// A copied page gets the latest last hit of the input pages which made it
// hot, so it is evicted as they would have been - if it got no last hit, it
// would be evicted first, and if it got the current time, cold data would
// look recently used after every compaction, and never be evicted.
//
// Reads go on during a compaction, and keep caching pages of its inputs,
// and recording hits of them. An input fetches its rows of pagecache_hits in
// batches, ahead of where the compaction reads it, so it doesn't see the
// pages which are cached, or hit, behind that - in data which the compaction
// has passed, or in a batch which it has fetched: such a page isn't copied,
// and the copy of a page doesn't get such a hit. This only costs a read of
// such a page a miss after the compaction, or makes a copy look older than
// it is - and with incremental compaction, only briefly: each output sstable
// replaces the input sstables which the compaction exhausted once it is
// written, and reads then cache, and hit, its pages directly.

#pragma once

#include <cstdint>
#include <deque>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <unordered_map>
#include <vector>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/noncopyable_function.hh>
#include "db_clock.hh"
#include "dht/decorated_key.hh"
#include "mutation/position_in_partition.hh"
#include "schema/schema_fwd.hh"
#include "sstables/component_type.hh"
#include "sstables/object_storage_page_cache.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/types.hh"
#include "seastarx.hh"

namespace sstables {

struct compression;
class sstable;

// Whether data is in a range of keys which an input had cached ("hot"), and
// if so, the latest last hit of the cached pages of that range (nothing if
// none of them was hit since it was cached).
struct page_cache_hotness {
    bool hot = false;
    std::optional<db_clock::time_point> last_hit;

    // Data which is hot here or there is hot, with the later last hit.
    void merge(const page_cache_hotness& other) noexcept;
    bool operator==(const page_cache_hotness&) const = default;
};

// The hot ranges of a component, in increasing order of its offsets - of
// its data as it is written, so before compression, for a Data component.
// Ranges are forgotten once they are passed.
class page_cache_hot_ranges {
    struct range {
        uint64_t start;
        uint64_t end;
        std::optional<db_clock::time_point> last_hit;
    };
    std::deque<range> _ranges;
    static constexpr uint64_t open_end = std::numeric_limits<uint64_t>::max();
    uint64_t _decided_until = open_end;
public:
    // Adds the hot range [start, end), after the existing ranges.
    void add(uint64_t start, uint64_t end, std::optional<db_clock::time_point> last_hit);
    // Whether all the hot ranges before "end" are known. Normally they
    // are, because the data is written before its hotness is needed - but
    // the writer may set_decided_until() an offset after which the data
    // written is still undecided.
    bool decided(uint64_t end) const noexcept {
        return end <= _decided_until;
    }
    void set_decided_until(uint64_t offset) noexcept {
        _decided_until = offset;
    }
    void set_all_decided() noexcept {
        _decided_until = open_end;
    }
    // The hotness of [from, to) - hot if any hot range overlaps it. Forgets
    // the ranges which end at or before "from", so "from" must not
    // decrease between calls.
    page_cache_hotness hotness(uint64_t from, uint64_t to);
    bool empty() const noexcept {
        return _ranges.empty();
    }
};

// What one input sstable of a compaction had cached (see above).
class page_cache_compaction_input {
    object_storage_page_cache& _cache;
    sstable_id _sst;
    schema_ptr _schema;
    // The compression of the sstable's Data component, or null if it isn't
    // compressed; and whether it is cached in 4 KiB pages even though it is
    // compressed, because a file I/O extension (encryption) transformed it
    // (see object_storage_page_cache_geometry.hh).
    const compression* _compression;
    bool _fixed_size_pages;
    // The rows of the sstable's Data component in pagecache_hits, which were
    // fetched and not yet passed, and the offset from which to fetch the
    // next ones (nothing if there are no more).
    std::deque<object_storage_page_cache::page_hits> _hits;
    std::optional<uint64_t> _next_hits;
    bool _hits_started = false;
    // The data offset up to which on_read() looked up the cache.
    uint64_t _read_until = 0;
    // The hot ranges of the Data component, in data offsets (before
    // compression), which the reader hasn't passed yet.
    page_cache_hot_ranges _hot;
    // Set after a failure to look up the cache, after which no more data is
    // hot (and the failure is logged, but doesn't fail the compaction).
    bool _failed = false;

    // The hot fragments which the reader found, until the output passes
    // them.
    struct hot_fragment {
        dht::decorated_key key;
        position_in_partition position;
        page_cache_hotness hotness;
    };
    std::deque<hot_fragment> _hot_fragments;
    // The reader may find hot fragments which the output never passes (for
    // example, when compaction drops their data), so at most this many are
    // kept, forgetting the oldest. The reader is ahead of the output by much
    // fewer.
    static constexpr size_t max_hot_fragments = 100000;

    // Calls func(data_start, data_end, stored_start, stored_end) for each
    // page-like unit of the Data component - a compressed chunk, or 4 KiB of
    // an uncompressed one - which overlaps the data range [start, end).
    template <typename Func>
    future<> for_each_unit(uint64_t start, uint64_t end, Func func);
    // Fetches the rows of pagecache_hits of the pages which start before
    // the given stored offset.
    future<> fetch_hits_until(uint64_t stored_end);
public:
    page_cache_compaction_input(object_storage_page_cache& cache, sstable_id sst, schema_ptr schema,
            const compression* compression, bool fixed_size_pages);

    // Called with each data range [start, end) of the Data component - before
    // compression - which the reader reads, in order, before it parses it.
    // Looks up which of its pages are cached. Never fails.
    future<> on_read(uint64_t start, uint64_t end) noexcept;
    // Called by the reader for each fragment it reads, with its partition,
    // its position, and the data offset where it starts.
    void on_fragment(const dht::decorated_key& key, position_in_partition_view pos, uint64_t offset);
    // The hotness of this input's fragment at the given key - not hot, if it
    // has no fragment there. The key must not decrease between calls.
    page_cache_hotness hotness_at(const dht::decorated_key& key, position_in_partition_view pos);
};

class page_cache_compaction_output;

// The inputs of one compaction, which uses the object storage page cache.
class page_cache_compaction : public enable_lw_shared_from_this<page_cache_compaction> {
    object_storage_page_cache& _cache;
    schema_ptr _schema;
    std::vector<std::unique_ptr<page_cache_compaction_input>> _inputs;
    bool _nothing_cached = false;
public:
    page_cache_compaction(object_storage_page_cache& cache, schema_ptr schema);
    object_storage_page_cache& cache() noexcept {
        return _cache;
    }
    // Looks up whether any of the input sstables has a page of its Data
    // component cached, before the compaction reads them. Most compactions
    // compact data which isn't cached at all, and then there is nothing to
    // copy - so add_input() and make_output() of an sstable return null,
    // and the compaction costs no more than if the sstables weren't cached.
    future<> start(std::vector<sstable_id> inputs);
    future<> start(const std::vector<shared_sstable>& inputs);
    bool nothing_cached() const noexcept {
        return _nothing_cached;
    }
    // Adds an input sstable, given the arguments of
    // page_cache_compaction_input's constructor.
    page_cache_compaction_input& add_input(sstable_id sst, const compression* compression, bool fixed_size_pages);
    // Adds an input sstable, or returns null if it doesn't use the cache.
    page_cache_compaction_input* add_input(const sstable& sst);
    // An output sstable with the given id, which asks the inputs which of
    // its data is hot.
    shared_ptr<page_cache_compaction_output> make_output(sstable_id sst);
    // The same, for an output sstable, or null if it has no id.
    shared_ptr<page_cache_compaction_output> make_output(const sstable& sst);
    // The hotness at the given key: hot if a fragment there is hot in any
    // input.
    page_cache_hotness hotness_at(const dht::decorated_key& key, position_in_partition_view pos);
};

// Which pages of one output sstable of a compaction to copy to the cache
// (see above).
//
// While it exists, find() finds it by its sstable's id - this is how the
// storage which writes the sstable's components to the object store finds
// it, without the sstable knowing about it.
class page_cache_compaction_output : public enable_shared_from_this<page_cache_compaction_output> {
public:
    // A page of the Data component is copied if at least this fraction of
    // its data is hot (see above).
    static constexpr uint64_t min_hot_fraction_denominator = 16;
private:
    lw_shared_ptr<page_cache_compaction> _compaction;
    sstable_id _sst;
    // The copied pages of each component.
    page_cache_hot_ranges _data;
    page_cache_hot_ranges _index;
    page_cache_hot_ranges _partitions;
    page_cache_hot_ranges _rows;

    // The Data component's pages, in data offsets (before compression): its
    // compressed chunks, or 4 KiB pages if it isn't compressed. (An
    // encrypted, compressed Data component is cached in 4 KiB pages of the
    // compressed data, which the sink maps to the chunks they hold.)
    uint64_t _data_page_size = 4096;
    // The page being written, its hot bytes so far, and their hotness; and
    // the number of the first block (see below) with hot data in the page.
    uint64_t _data_page = 0;
    uint64_t _data_page_hot_bytes = 0;
    page_cache_hotness _data_page_hotness;
    std::optional<uint64_t> _data_page_first_hot_block;
    // The fragment being written: where it starts, and its hotness; and
    // where the fragment before it started.
    uint64_t _fragment_start = 0;
    page_cache_hotness _fragment_hotness;
    uint64_t _last_fragment_offset = 0;
    uint64_t _previous_fragment_start = 0;

    // The blocks of the partitions' rows, as reads read them: a block of the
    // partition's row index, which reads read whole (reversed reads) or from
    // its start (forward reads) - or the partition, from its start, if it
    // has no row index. When a page is copied, so are all the blocks with
    // hot data in it, with the start of the row after them, which a forward
    // read of their last row also parses, and the row before them, which a
    // reversed read of their first row also parses. The blocks are kept
    // until their partition's index entries are decided. _first_block is
    // the number of the first.
    struct block {
        uint64_t start;
        // Where the row before the block starts.
        uint64_t previous_row_start;
        std::optional<uint64_t> end;
        // The hotness of its hot fragments.
        page_cache_hotness hotness;
        bool copied = false;
    };
    std::deque<block> _blocks;
    uint64_t _first_block = 0;
    // How many blocks of the current partition the writer reported (see
    // on_row_index_block()), and whether all of the data was written.
    uint64_t _partition_reported_blocks = 0;
    bool _data_ended = false;
    // How much of the row after a copied block is copied with it: a read of
    // the block's last row also parses the next row's header and key.
    static constexpr uint64_t next_row_bytes = 256;
    // The pages which were written, but may still be copied - because a
    // block with data in them may still be copied - in order.
    struct data_page {
        uint64_t start;
        uint64_t end;
        page_cache_hotness hotness;
        bool copy = false;
    };
    std::deque<data_page> _pending_pages;
    // The pages which start before this offset are copied, with this
    // hotness - they hold the end of a copied block.
    uint64_t _copied_until = 0;
    page_cache_hotness _copied_until_hotness;

    // The partitions whose index entries aren't decided yet - those which
    // haven't ended, or have blocks which may still be copied - in order.
    struct partition {
        uint64_t first_block;
        bool ended = false;
        // The Index interval of the partition (see on_index_interval()).
        uint64_t index_interval;
        // Adds the partition to the index, with the marks of its hotness
        // and of its blocks' (see on_partition_end()).
        noncopyable_function<void(uint32_t mark, std::span<const uint32_t> block_marks)> add_to_index;
    };
    std::deque<partition> _undecided_partitions;

    // The intervals of the Index component of a "me" sstable (see
    // on_index_interval()) which aren't decided yet: where they start and
    // end (when known), their hotness, and the hotness of their last
    // partition. The first one's number is _first_index_interval.
    struct index_interval {
        uint64_t start;
        std::optional<uint64_t> end;
        page_cache_hotness hotness;
        page_cache_hotness last_partition_hotness;
        size_t undecided_partitions = 0;
    };
    std::deque<index_interval> _index_intervals;
    uint64_t _first_index_interval = 0;
    // The hotness of the last partition of the last decided interval.
    page_cache_hotness _last_decided_partition_hotness;

    // Counts the data written since the start of the current fragment, up
    // to the given offset, ending each page which it completes.
    void count_data_until(uint64_t offset);
    // Ends the current page of the Data component, whose data ends at the
    // given offset, and starts the next one.
    void end_data_page(uint64_t end);
    block& current_block() noexcept {
        return _blocks.back();
    }
    uint64_t current_block_number() const noexcept {
        return _first_block + _blocks.size() - 1;
    }
    void start_block(uint64_t start, uint64_t previous_row_start);
    void end_block(uint64_t end);
    // Copies the block, if it isn't copied yet - the part of it written so
    // far, if it hasn't ended.
    void copy_block(block& b);
    // Copies the pages which overlap [start, end).
    void copy_data(uint64_t start, uint64_t end, const page_cache_hotness& hotness);
    // Decides the pages which no block can copy any more, and then the
    // partitions all of whose blocks are decided, and the Index intervals
    // all of whose partitions are.
    void decide();
    void decide_index_intervals();
public:
    page_cache_compaction_output(lw_shared_ptr<page_cache_compaction> compaction, sstable_id sst);
    ~page_cache_compaction_output();
    page_cache_compaction_output(const page_cache_compaction_output&) = delete;
    page_cache_compaction_output& operator=(const page_cache_compaction_output&) = delete;
    // The output of this shard for the given sstable, or null.
    static shared_ptr<page_cache_compaction_output> find(sstable_id sst) noexcept;
    object_storage_page_cache& cache() noexcept {
        return _compaction->cache();
    }
    sstable_id sstable() const noexcept {
        return _sst;
    }
    // Called by the writer before it writes, with the compression of the
    // Data component, or null if it isn't compressed.
    void set_data_compression(const compression* compression) noexcept;
    // Called by the writer for each fragment, before writing it, with its
    // partition, its position, and the data offset - before compression -
    // where it will start.
    void on_data_fragment(const dht::decorated_key& key, position_in_partition_view pos, uint64_t offset);
    // Called by the writer when it starts a block of rows of the
    // partition's row index - before writing its first row - with its data
    // offset, where a read of a row of the block starts reading. (The
    // first block of a partition is read from the partition's start.)
    void on_row_index_block(uint64_t offset);
    // Called by the writer after it wrote a partition, which ends at the
    // given data offset, and its entries in the Index of a "me" sstable. The
    // index of a BTI sstable is written by add_to_index(), once it is
    // decided whether the partition and its blocks are hot - with the marks
    // which encode their hotness (see mark()), for the partition and for
    // each of its blocks of rows, in order.
    void on_partition_end(uint64_t end,
            noncopyable_function<void(uint32_t mark, std::span<const uint32_t> block_marks)> add_to_index = {});
    // Called by the writer after it wrote all of the data, with its size.
    void on_data_end(uint64_t offset);
    // Called by the writer when it is destroyed: drops the partitions which
    // are waiting to be added to the index, whose add_to_index() refers to
    // the writer.
    void abandon() noexcept;

    // The Index component of a "me" sstable is read by intervals: a read
    // reads the whole range of the Index between two of the Summary's
    // entries, and the next range too if the entry it looks for is the
    // last in its range (which it needs to know where the partition ends).
    // So all of an interval is hot if any of its partitions is hot, and so
    // is the next interval, if the last partition of a hot interval is hot.
    // The writer calls on_index_interval() when it adds an entry to the
    // Summary, with the offset in the Index where the new interval starts,
    // and on_index_end() with the size of the Index.
    void on_index_interval(uint64_t offset);
    void on_index_end(uint64_t offset);

    // The Partitions and Rows components of a BTI sstable are tries, whose
    // writers mark the keys whose data is hot, with a mark which encodes
    // the hotness (see trie::trie_writer::add()), and tell on_trie_node()
    // where they write each node which leads to a marked key - which a read
    // of that key reads. The writer gives the keys the marks which
    // on_partition_end()'s add_to_index() gets, and calls
    // on_rows_entry_start() and on_rows_entry_end() before and after it
    // writes a partition's entry in the Rows component, with its offsets
    // and the partition's mark - the entry ends with a header, which a read
    // of the partition reads, and which is hot if the partition is.
    static uint32_t mark(const page_cache_hotness& hotness) noexcept;
    static page_cache_hotness hotness_of_mark(uint32_t mark) noexcept;
    void on_trie_node(component_type type, uint64_t pos, uint32_t mark);
    void on_rows_entry_start(uint64_t offset) noexcept;
    void on_rows_entry_end(std::optional<uint64_t> header_start, uint64_t end, uint32_t mark);
    // The ranges of the pages to copy of a component of the output, or null
    // for a component which isn't copied to the cache.
    page_cache_hot_ranges* hot_ranges(component_type type) noexcept;
};

// Returns what a compaction of the given input sstables needs to keep their
// cache, or null if they don't use the object storage page cache.
lw_shared_ptr<page_cache_compaction> make_page_cache_compaction(const std::vector<shared_sstable>& inputs, schema_ptr schema);

// A data_source which reads the given input stream - the Data component of
// an input sstable, before compression, from the given data offset - and
// passes the range of each buffer to input.on_read() before returning it.
data_source make_page_cache_compaction_source(input_stream<char> in, page_cache_compaction_input& input, uint64_t offset);

// A data_sink which writes an output sstable's component to the given sink,
// which writes it to the object store as it is stored (after compression
// and file I/O extensions), and copies to the cache, with
// object_storage_page_cache::put_copied_page(), its pages which hold data in
// the output's hot ranges of the component. The pages are those in which
// reads cache the component (see object_storage_page_cache_geometry.hh): the
// compressed chunks of a compressed Data component, given its compression -
// unless fixed_size_pages - and otherwise 4 KiB pages. A failure to copy a
// page is logged, and stops the copying, but doesn't fail the write.
data_sink make_page_cache_compaction_sink(data_sink sink, shared_ptr<page_cache_compaction_output> output,
        component_type type, const compression* compression, std::function<bool()> fixed_size_pages);

// For the storage of an sstable which uses the cache: wraps the given sink,
// which writes the sstable's component to the object store as it is stored,
// with make_page_cache_compaction_sink(), if the sstable is an output of a
// compaction which keeps its inputs' cache.
data_sink maybe_make_page_cache_compaction_sink(const sstable& sst, component_type type, data_sink sink);

} // namespace sstables
