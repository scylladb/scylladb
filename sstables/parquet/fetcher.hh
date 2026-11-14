/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// The fetcher decides which byte ranges of the file to read, and reads them.
//
// Every range is known before the read: the summary locates each row
// group's OffsetIndex block, the OffsetIndexes locate the pages, and a
// dictionary page spans from the start of its chunk to the first data page.
//
// Requests. The fetcher aligns every request to the file's
// disk_read_dma_alignment(), so the buffer that the file allocates for a
// read has the size of the request. A request is at most
// fetch_policy::max_request_size. Two ranges are read with one request when
// their aligned extents overlap or touch. So a request reads bytes outside
// the ranges that the read needs only in the blocks at the ends of those
// ranges, which the device reads anyway. A range whose first or last block
// is in a request that still exists uses that request for the block.
//
// Memory. With a permit, the fetcher charges each request to it with
// reader_permit::request_memory() before it reads, and the buffer holds the
// charge. So read-ahead waits for memory, as the read buffers of an `mx`
// reader do. The file must not be tracked by the permit as well. The
// fetcher asks for the memory of the requests that a cursor waits for
// before that of read-ahead, so read-ahead never takes memory that they
// need. The requests of one permit that wait for memory are granted
// together.
//
// Inactive reads. Between two calls of its consumer, a reader may be
// registered as an inactive read with the reader concurrency semaphore. An
// inactive permit must not ask for memory: the semaphore would queue it
// as a waiter. So the fetcher asks for memory only within a call of
// get_index(), get_page(), or get_dictionary_page(), or while the future
// of such a call has not resolved, because then the reader is not
// inactive. Work that completes in the background, such as an OffsetIndex
// load that read-ahead started, asks for no memory when no call is in
// progress, and the next call goes on with it. Registering a read as
// inactive fails the memory requests that wait with std::bad_alloc. Such
// a request may be read-ahead, which nothing waits for. So a page or an
// OffsetIndex block whose read failed with std::bad_alloc is fetched
// again, once, when a caller asks for it, and the second error reaches
// the caller.
//
// Read-ahead: the fetcher keeps a window of rows in the read direction. The
// window's frontier moves by page boundaries: each step adds, for each
// projected column with data in the row group, the page that holds the row
// at the frontier. The pages of a column are adjacent in the file, so the
// pages of one column in the window form one byte range. The window never
// moves past the end of the range set by set_range(), so it holds only
// pages with rows in the range.
//
// - A scan refills the window when the bytes fetched or in flight and not
//   yet taken fall below half the budget, and fills it up to the budget.
// - A single-partition read extends the window only when a cursor asks for
//   a page outside it. The first extension is one step, and each later one
//   is twice the previous.
// - When the window reaches a row group whose OffsetIndex block the fetcher
//   has not loaded, the fetcher starts loading it, from the cache or from
//   the file, and stops the window there. When the block is loaded, the
//   extension goes on where it stopped: at once if a call is in progress,
//   and otherwise at the next call. A failed load stops the window, and
//   the cursor that reaches the row group gets the error.
// - The budget is policy.budget_per_column per projected column with data in
//   the row group, at least policy.min_budget, and at most the size of the
//   row group's region.
//
// The OffsetIndex block of a row group is read whole, with the
// OffsetIndexes of the columns outside the projection. The summary keeps
// only the location of the block, and a chunk's start comes from the
// OffsetIndex of the chunk before it, so reading single OffsetIndexes would
// need a location per chunk in memory, where the summary keeps two bits.
// The sstable reader projects every column of the schema, so the block
// holds little else. The cache keeps the decoded OffsetIndexes of the
// whole block, and a reader copies those of its projected columns.
//
// Measured (test_read_ahead_counters in parquet_sstable_reader_test.cc,
// dev mode, default writer settings, files of 8 to 10 MiB, random int
// values, so no dictionaries):
//
// | Table | Full scan | Peak memory of a scan | Point read | Compaction of 4 |
// |---|---|---|---|---|
// | 5 int columns (9 leaves) | 21 requests per MiB | 1.6 MB | 7 requests, 81 KB | 6.4 MB |
// | 20 int columns (24 leaves) | 20 requests per MiB | 5.0 MB | 23 requests, 223 KB | 20 MB |
// | 100 int columns (104 leaves) | 30 requests per MiB | 10 MB | 109 requests, 890 KB | 41 MB |
//
// - A scan refills half the budget at a time, so a request is about
//   budget / 2 / columns, at most 128 KiB, and smaller at the cap: there,
//   the next row group is read half a chunk at a time.
// - At the cap, a scan holds about one row group.
// - A point read reads one page per leaf. A page has at least the minimum
//   page size (8 KiB), so with 100 leaves it reads about 900 KB. Pages of
//   different columns are adjacent only in small chunks, so a point read
//   makes about one request per leaf.
//
// perf-simple-query --flush --bypass-cache (dev mode, one shard, 10000
// partitions, five blob columns with constant values, so the value columns
// are dictionary pages and tiny data pages), with earlier merge rules:
//
// | Merge rule | Reads per op | KB read per op | tps |
// |---|---|---|---|
// | mt, for comparison | 3.0 | 13 | 13000 |
// | no merging | 10.0 | 47 | 1900 |
// | gaps of up to 4 KiB | 4.6 | 29 | 9700 |
// | gaps of up to 16 KiB | 3.8 | 34 | 8700 |
// | one request for all pages within 128 KiB | 3.0 | 52 | 5300 |
//
// About 2 reads per op are the trie index, as for mt. A point read then
// needs 8 pages in three places of the file: the key column's chunk, the
// value columns with `_scylla.kind`, and `_scylla.ts`, whose chunk is
// larger. They are 3 to 40 KiB apart, so the read makes 2 or 3 requests.
// Reading the gaps costs more than the requests it saves. With blocks of
// 4 KiB, the current rule merges every gap of less than 4 KiB too, and
// reads no block that separate requests would not read.
//
// The fetcher never reads a chunk that is entirely null, or a chunk of a
// column outside the projection, beyond the blocks that it shares with a
// range that the read needs. It reads only bytes of the row group regions,
// whose bounds come from the checksummed footer, so every read is within
// the file, apart from the alignment of its end. The policy is kept in this
// class and in fetch_policy, apart from the decoders, so it can be changed
// alone.

#include <cstdint>
#include <map>
#include <unordered_map>
#include <vector>

#include <boost/intrusive/set.hpp>

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/io_intent.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>

#include "sstables/parquet/file_summary.hh"
#include "sstables/parquet/page_decoder.hh"
#include "sstables/parquet/reader_cache.hh"
#include "reader_permit.hh"
#include "utils/chunked_vector.hh"
#include "utils/fragmented_temporary_buffer.hh"
#include "utils/small_vector.hh"

namespace sstables::parquet {

enum class read_direction {
    forward,
    backward,
};

/// What the caller expects to read. It selects the read-ahead policy.
enum class read_hint {
    /// Many rows: compaction, repair, streaming, range scans.
    scan,
    /// A few rows, typically of one partition.
    single_partition,
};

/// The tunables of read-ahead. The values are indicative, and not tuned
/// yet.
struct fetch_policy {
    uint64_t budget_per_column = 128 << 10;
    uint64_t min_budget = 640 << 10;
    /// The largest read request. The fetcher rounds it down to a multiple
    /// of the file's alignment, and up to one block.
    uint64_t max_request_size = 128 << 10;
};

/// A reader's copy of the OffsetIndexes of the projected columns of a row
/// group, and the charge of its memory to the reader's permit, if there is
/// one.
struct charged_row_group_index : row_group_index {
    std::optional<reader_permit::resource_units> memory;
};
/// The fetcher and the cursors of a reader share its copies.
using row_group_index_ptr = lw_shared_ptr<const charged_row_group_index>;

/// Counters of the fetcher's work, for tests and metrics.
struct fetch_stats {
    /// Read requests issued to the file, OffsetIndex blocks included.
    uint64_t reads = 0;
    /// Their aligned sizes.
    uint64_t bytes_read = 0;
    uint64_t index_reads = 0;
    uint64_t largest_request = 0;
    /// Pages that cursors asked for.
    uint64_t pages_taken = 0;
    /// Pages that cursors asked for and that were not fetched ahead.
    uint64_t page_misses = 0;
    /// The most bytes that were fetched or in flight and not yet taken,
    /// measured after each extension of the window.
    uint64_t max_pending_bytes = 0;
};

class fetcher {
    // A request is linked into _live_requests while it exists.
    struct io_request : public enable_lw_shared_from_this<io_request>,
            public boost::intrusive::set_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>> {
        // Aligned. The request is charged and allocated at this size.
        uint64_t offset = 0;
        uint64_t size = 0;
        // The end of the bytes that the read needs. Past the end of the file,
        // a read returns fewer bytes than the request.
        uint64_t needed_end = 0;
        // Whether the request reads an OffsetIndex block.
        bool index = false;
        // Whether a cursor waits for the request now. Such requests are
        // started first.
        bool now = false;
        // Whether start_read() was called.
        bool issued = false;
        temporary_buffer<char> data;
        std::exception_ptr error;
        shared_promise<> done;
    };
    using io_request_ptr = lw_shared_ptr<io_request>;
    struct io_request_offset {
        using type = uint64_t;
        const type& operator()(const io_request& r) const noexcept { return r.offset; }
    };
    using live_request_set = boost::intrusive::multiset<io_request,
            boost::intrusive::key_of_value<io_request_offset>,
            boost::intrusive::constant_time_size<false>>;

    // A page or a dictionary page. Slot 0 is the dictionary page, and slot
    // j + 1 is data page j.
    struct page_key {
        leaf_index column;
        uint32_t row_group;
        uint32_t slot;
        auto operator<=>(const page_key&) const = default;
    };
    struct pending_page {
        uint64_t offset;
        uint32_t size;
        utils::small_vector<io_request_ptr, 2> requests;
    };
    struct wanted_page {
        page_key key;
        uint64_t offset;
        uint32_t size;
        // A cursor asks for the page now, and not only ahead.
        bool now;
    };
    // A load that is done with neither an index nor an error was dropped:
    // the cached block was evicted while the load waited for memory, and
    // no call was in progress to read the block from the file.
    struct index_load {
        row_group_index_ptr index;
        std::exception_ptr error;
        shared_promise<> done;
    };
    // Marks a call of the consumer as in progress while it lives.
    class consumer_call {
        fetcher& _f;
    public:
        explicit consumer_call(fetcher& f) noexcept : _f(f) { ++_f._consumer_calls; }
        consumer_call(const consumer_call&) = delete;
        ~consumer_call() { --_f._consumer_calls; }
    };
    enum class step_result {
        stepped,
        // The window passed a row group in which no projected column has data.
        skipped,
        // The window reached the end of the file.
        end,
        // The window waits for the OffsetIndex block of its row group.
        waiting_for_index,
    };

    file _file;
    const file_summary& _summary;
    metadata_cache& _cache;
    std::optional<reader_permit> _permit;
    // The projected columns that are in the file, in increasing order.
    std::vector<leaf_index> _columns;
    read_direction _direction;
    read_hint _hint;
    fetch_policy _policy;
    // The file's read alignment, a power of two.
    uint64_t _alignment;
    uint64_t _range_begin = 0;
    uint64_t _range_end = 0;

    // The requests that exist, by offset. They do not own the requests. A
    // new range reuses a request that holds its first or last block, so
    // that the block is not read twice, for example at the boundary between
    // two refills of a scan's window.
    live_request_set _live_requests;
    std::map<page_key, pending_page> _pending;
    uint64_t _pending_bytes = 0;
    // Pages chosen by the window, to be read by issue_wanted().
    utils::chunked_vector<wanted_page> _wanted;
    uint64_t _wanted_bytes = 0;
    // The OffsetIndex blocks that are loading, loaded, or failed to load.
    // A loaded one stays while the window or a cursor uses it, so that the
    // reader holds one copy of it. drop_unused_indexes() drops the others.
    std::unordered_map<uint32_t, lw_shared_ptr<index_load>> _index_loads;

    // The read-ahead window. Forward, the rows below the frontier are
    // covered, and backward, the rows at or above it.
    bool _window_valid = false;
    uint64_t _frontier = 0;
    uint32_t _frontier_row_group = 0;
    row_group_index_ptr _frontier_index;
    uint64_t _frontier_budget = 0;
    // For each of _columns: the next page of the frontier's row group to
    // add in the read direction, or -1 if none is left.
    std::vector<int64_t> _next_page;
    // For each of _columns: the last page taken, which its cursor holds.
    std::vector<std::pair<uint32_t, uint32_t>> _taken;
    // The steps of the next extension of a single-partition read.
    uint64_t _window_steps = 1;
    // The steps left of an extension that stopped to wait for the
    // OffsetIndex block of the frontier's row group, or 0.
    uint64_t _stalled_steps = 0;
    // Whether advance() runs, so that a load that completes within it does
    // not resume the window.
    bool _advancing = false;
    bool _closing = false;
    // The calls of get_index(), get_page(), and get_dictionary_page() whose
    // futures have not resolved.
    unsigned _consumer_calls = 0;

    fetch_stats _stats;
    // Cancels the reads that wait in the I/O queue when the fetcher closes.
    io_intent _intent;
    gate _gate;

    std::optional<size_t> column_position(leaf_index c) const noexcept;
    bool has_data(uint32_t g, leaf_index c) const noexcept;
    uint64_t budget(uint32_t g) const noexcept;
    void want(page_key key, uint64_t offset, uint32_t size, bool now);
    void want_page(uint32_t g, leaf_index c, const row_group_index& idx, uint32_t j, bool now);
    void want_dictionary(uint32_t g, leaf_index c, const row_group_index& idx, bool now);
    void issue_wanted();
    io_request_ptr reusable_request(uint64_t block, uint64_t end) noexcept;
    void make_requests(uint64_t start, uint64_t end, bool index, utils::chunked_vector<io_request_ptr>& requests);
    void start_read(io_request_ptr r) noexcept;
    future<> read(io_request_ptr r, gate::holder);
    lw_shared_ptr<index_load> start_index_load(uint32_t g);
    future<> load_index(uint32_t g, lw_shared_ptr<index_load> load, gate::holder);
    future<row_group_index_ptr> read_index(uint32_t g);
    void check_open() const;
    void drop_unused_indexes() noexcept;
    bool may_request_memory() const noexcept { return !_permit || _consumer_calls; }
    future<fragmented_temporary_buffer> take(page_key key, bool retry = true);
    void drop_behind(page_key key);
    void enter_row_group(uint32_t g, row_group_index_ptr idx, uint64_t row_in_group, bool now);
    void reset_window(uint32_t g, row_group_index_ptr idx, uint64_t row);
    step_result step(bool now);
    void extend(bool on_miss);
    void advance(uint64_t steps, bool on_miss);
    void resume_window() noexcept;
public:
    /// `columns` are the projected columns that are present in the file.
    /// The read buffers and the OffsetIndexes that the fetcher copies from
    /// the cache or decodes are charged to `permit`. `f` must not charge
    /// its reads to the permit as well.
    fetcher(file f, const file_summary& s, metadata_cache& cache, std::vector<leaf_index> columns,
            read_direction direction, read_hint hint, fetch_policy policy, std::optional<reader_permit> permit = {});
    ~fetcher();

    /// Sets the rows to read, [begin, end). Drops the pages fetched ahead.
    void set_range(uint64_t begin, uint64_t end) noexcept;

    /// Returns the OffsetIndexes of the projected columns of row group `g`.
    /// It copies them from the cache, or reads and decodes the OffsetIndex
    /// block and adds it to the cache.
    future<row_group_index_ptr> get_index(uint32_t g);

    /// Returns the bytes of data page `j` of column `c` in row group `g`,
    /// and extends the read-ahead window. `row` is the row that the caller
    /// is after, which the page holds.
    future<fragmented_temporary_buffer> get_page(uint32_t g, leaf_index c, row_group_index_ptr idx, uint32_t j, uint64_t row);

    /// Returns the bytes of the dictionary page of column `c` in row group
    /// `g`. To have the dictionary page fetched with the data page, ask for
    /// the data page first.
    future<fragmented_temporary_buffer> get_dictionary_page(uint32_t g, leaf_index c, const row_group_index& idx);

    /// Stops issuing reads, cancels those that wait in the I/O queue, and
    /// waits for those in flight. A read that waits for memory is not
    /// issued once it gets it, but close() waits for it, because a memory
    /// request cannot be withdrawn. Must be called before destruction.
    future<> close() noexcept;

    const fetch_stats& stats() const noexcept { return _stats; }
    /// The fixed memory of the fetcher per projected column.
    static constexpr size_t memory_per_column() noexcept {
        return sizeof(leaf_index) + sizeof(int64_t) + sizeof(std::pair<uint32_t, uint32_t>);
    }
    uint64_t pending_bytes() const noexcept { return _pending_bytes; }
    read_direction direction() const noexcept { return _direction; }
};

} // namespace sstables::parquet
