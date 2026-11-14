/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// The read side of the Parquet codec: the reader and its column cursors.
//
// Use:
//
//     auto summary = co_await read_file_summary(f, size, footer_crc);   // once per file
//     metadata_cache cache(region, lru, cache_stats);                   // once per file and shard
//     std::vector<std::optional<leaf_index>> projection{summary.find_column({"a"}), summary.find_column({"b"})};
//     reader r(f, summary, cache, projection, reader_options{.begin = b, .end = e});
//     co_await r.seek_all(b);
//     for (uint64_t row = b; row < e; ++row) {
//         auto& c = r.cursor(0);
//         if (row > b && !c.try_next()) {
//             co_await c.next();
//         }
//         ... c.has_value(), c.value<int64_t>() ...
//     }
//     co_await r.close();
//
// The reader knows Parquet only, and supports only files that follow the
// layout of our writer (see writer.hh and file_summary.hh).

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <vector>

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>

#include "sstables/parquet/fetcher.hh"
#include "sstables/parquet/file_summary.hh"
#include "sstables/parquet/page_decoder.hh"
#include "sstables/parquet/reader_cache.hh"
#include "reader_permit.hh"

namespace sstables::parquet {

struct reader_options {
    /// The rows to read, [begin, end). They inform read-ahead. A cursor may
    /// still be moved to any row of the file.
    uint64_t begin = 0;
    uint64_t end = std::numeric_limits<uint64_t>::max();
    read_direction direction = read_direction::forward;
    read_hint hint = read_hint::scan;
    fetch_policy policy;
    /// The permit to charge the read buffers and the memory of decoded
    /// pages to. See reader.
    std::optional<reader_permit> permit;
};

class reader;

/// A cursor over the rows of one leaf column.
///
/// A cursor is positioned at a row ordinal. It decodes the whole page that
/// holds the row, and moves within it in either direction without I/O. A
/// cursor of a column that is absent from the file, or of a chunk that is
/// entirely null, does no I/O and decodes nothing: every row there is
/// null at level 0.
///
/// Moving has two forms. try_next(), try_previous(), and try_seek() move
/// synchronously when they need no I/O, and return false otherwise,
/// leaving the cursor where it was. next(), previous(), and seek() always
/// move, and wait for I/O when needed. A failed move leaves the cursor
/// unpositioned.
///
/// Byte array values are views of buffers that the cursor holds. They stay
/// valid until the cursor moves or is destroyed.
class column_cursor {
public:
    /// An entry of the current row: its levels, and if def equals the
    /// column's max_def, the index of its value for value_at().
    struct entry {
        uint8_t rep;
        uint8_t def;
        uint32_t value_index;
    };

    /// Iterates over the entries of the current row. A row has at least
    /// one entry. Only a repeated column has more than one. Valid until the
    /// cursor moves.
    class row_entries {
        const decoded_page* _page = nullptr;
        decoded_page::position _pos;
        bool _done = false;
    public:
        row_entries() = default;
        row_entries(const decoded_page* page, decoded_page::position pos) noexcept : _page(page), _pos(pos) {}
        std::optional<entry> next() noexcept {
            if (_done) {
                return std::nullopt;
            }
            if (!_page) {
                _done = true;
                return entry{0, 0, 0};
            }
            const entry e{_page->rep_at(_pos), _page->def_at(_pos), _pos.value};
            _done = !_page->next_entry_in_row(_pos);
            return e;
        }
    };

private:
    enum class state : uint8_t {
        unpositioned,
        // Every row of the region is null at level 0.
        null_region,
        page,
    };

    reader& _reader;
    // Null for a column that is absent from the file.
    std::optional<leaf_index> _column;
    state _state = state::unpositioned;
    uint64_t _row = 0;
    // The rows of the current page or null region, [_region_begin, _region_end).
    uint64_t _region_begin = 0;
    uint64_t _region_end = 0;
    // The row group of _index and _dictionary.
    uint32_t _row_group = std::numeric_limits<uint32_t>::max();
    row_group_index_ptr _index;
    dictionary_ptr _dictionary;
    // The memory of _dictionary, charged to the reader's permit, if it has one.
    std::optional<reader_permit::resource_units> _dictionary_memory;
    std::unique_ptr<decoded_page> _page;
    // The memory of _page, charged to the reader's permit, if it has one.
    std::optional<reader_permit::resource_units> _page_memory;
    decoded_page::position _pos;
    std::unique_ptr<page_compressor> _compressor;

    // A copy of a cached dictionary, in compact form, or the bytes of a
    // dictionary page as fetched.
    struct dictionary_page_bytes {
        metadata_cache::cached_bytes bytes;
        bool cached = false;
    };

    void drop_page() noexcept;
    void drop_row_group() noexcept;
    bool try_null_region(uint64_t row);
    bool try_seek_far(uint64_t row);
    future<dictionary_page_bytes> dictionary_page(uint32_t g);
    future<> load_dictionary(uint32_t g, dictionary_page_bytes page);
    future<> load(uint64_t row);
    void check_row(uint64_t row) const;
public:
    column_cursor(reader& r, std::optional<leaf_index> column);
    column_cursor(const column_cursor&) = delete;
    ~column_cursor();

    /// The column in the file, or nothing if it is absent.
    std::optional<leaf_index> column() const noexcept { return _column; }
    bool positioned() const noexcept { return _state != state::unpositioned; }
    uint64_t row() const noexcept { return _row; }

    // Moving. The target row must be below the file's row count.

    bool try_seek(uint64_t row) {
        // The step to the next row of the page or null region, which a
        // scan makes for every row.
        if (_state != state::unpositioned && row == _row + 1 && row < _region_end) [[likely]] {
            if (_state == state::page) {
                _page->next_row(_pos);
            }
            _row = row;
            return true;
        }
        return try_seek_far(row);
    }
    bool try_next();
    bool try_previous();
    future<> seek(uint64_t row);
    future<> next();
    future<> previous();

    /// Drops the page and unpositions the cursor.
    void release() noexcept;

    // The current row. The cursor must be positioned.

    /// The definition level of the row's first entry.
    uint8_t def() const noexcept {
        return _state == state::page ? _page->def_at(_pos) : 0;
    }
    /// Whether the row's first entry is a value. For a column that is not
    /// repeated, whether the row has a value.
    bool has_value() const noexcept {
        return _state == state::page && _page->has_value(_pos);
    }
    /// Whether the row is null, see leaf_column::null_threshold().
    bool is_null() const noexcept {
        return _state != state::page || _page->is_null_row(_pos);
    }
    /// The value of the row's first entry, which has_value() must report.
    /// T must match the column's physical type, see encoding.hh.
    template <decoded_value T>
    const T& value() const {
        return std::get<utils::chunked_vector<T>>(_page->values())[_pos.value];
    }
    /// The value with the given index, from row_entries.
    template <decoded_value T>
    const T& value_at(uint32_t value_index) const {
        return std::get<utils::chunked_vector<T>>(_page->values())[value_index];
    }
    row_entries entries() const noexcept {
        return _state == state::page ? row_entries(_page.get(), _pos) : row_entries();
    }

    /// If the current row is null, the number of null rows from it onward,
    /// itself included. Otherwise 0. It is a lower bound: the run may
    /// continue after it, for example in the next page. A caller can park
    /// the cursor for that many rows.
    uint64_t null_rows_forward() const noexcept;
    /// Like null_rows_forward(), toward the start of the file.
    uint64_t null_rows_backward() const noexcept;
};

/// Reads a projection of the columns of a file.
///
/// The reader holds one cursor per projected column. All cursors share a
/// fetcher, so the pages that they need are read together. The reader must
/// be closed before it is destroyed. The summary, the cache, and the file
/// must outlive it.
///
/// Memory accounting. With a permit in reader_options, the reader charges
/// to it everything that it allocates per read. The file must not be
/// tracked by the permit (make_tracked_file()), because the fetcher charges
/// the bytes that it reads. In detail:
///
/// - The fixed cost of the cursors and of the fetcher's state per column
///   (fixed_memory()). A caller that can wait asks for it with
///   reader_permit::request_memory() before it creates the reader, and
///   passes the units to the constructor. Otherwise the reader consumes it
///   when it is created.
/// - The read buffers, at their aligned size, before they are read. So
///   read-ahead waits for memory.
/// - The memory of each decoded page: the decompressed values, the decoded
///   values, and the levels. A cursor asks for it with
///   reader_permit::request_memory() after the page is fetched and checked,
///   and before it is decompressed. So under memory pressure the semaphore
///   makes the read wait there, as it does before an `mx` reader
///   decompresses a chunk, instead of killing it later. The charge is then
///   set to the page's real size, and lasts while the cursor holds the page.
/// - The OffsetIndexes and the dictionaries that the reader uses. The
///   cache holds them decoded, and they belong to no read. A reader uses a
///   private copy: the OffsetIndexes of its projected columns, or a whole
///   dictionary, which it copies from the cache, or decodes from the file
///   on a miss. It asks for the memory with request_memory() before it
///   allocates it: the size of the copy, or a bound on the memory of
///   decoding, which it lowers to the size of the result once it is
///   decoded. The charge lasts while the reader holds the copy. The
///   cursors share the OffsetIndexes of a row group.
class reader {
    file _file;
    const file_summary& _summary;
    metadata_cache& _cache;
    reader_options _options;
    fetcher _fetcher;
    std::vector<std::unique_ptr<column_cursor>> _cursors;
    std::optional<reader_permit::resource_units> _fixed_memory;

    friend class column_cursor;
public:
    /// `projection` lists the columns to read: each is a leaf of the file,
    /// or nothing for a column that is absent from the file. A leaf may
    /// appear only once. Columns of the file that are not listed are never read.
    /// `fixed_memory`, if set, holds at least fixed_memory(projection.size())
    /// of the permit. The reader keeps it instead of consuming that memory.
    reader(file f, const file_summary& summary, metadata_cache& cache, const std::vector<std::optional<leaf_index>>& projection,
            reader_options options = {}, std::optional<reader_permit::resource_units> fixed_memory = {});
    ~reader();

    const file_summary& summary() const noexcept { return _summary; }
    size_t columns() const noexcept { return _cursors.size(); }
    /// The cursor of the i-th projected column.
    column_cursor& cursor(size_t i) { return *_cursors.at(i); }

    uint64_t range_begin() const noexcept { return _options.begin; }
    uint64_t range_end() const noexcept { return _options.end; }

    /// Sets a new range of rows to read, [begin, end), for example the
    /// next partition or clustering range. Read-ahead restarts there. The
    /// cursors keep their positions and pages, and the caller seeks them.
    void fast_forward_to(uint64_t begin, uint64_t end);

    /// Seeks every cursor to `row`. The pages are read together.
    future<> seek_all(uint64_t row);

    /// Drops the pages that the cursors hold and the pages fetched ahead,
    /// and unpositions every cursor. A reader that has consumed its rows
    /// calls it so that it holds no page memory while it waits to be
    /// destroyed. The next move of a cursor reads its page again.
    void release_pages() noexcept;

    /// Stops the reads, and waits for those in flight (fetcher::close()).
    future<> close() noexcept;

    const fetch_stats& stats() const noexcept { return _fetcher.stats(); }

    /// The memory that the reader charges per projected column when it is
    /// created.
    static size_t fixed_memory_per_column() noexcept;
    /// The memory that the reader charges when it is created, for
    /// `columns` projected columns.
    static size_t fixed_memory(size_t columns) noexcept;
};

} // namespace sstables::parquet
