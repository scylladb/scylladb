/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Decoding of what the reader fetches from a file: the OffsetIndex block of
// a row group, dictionary pages, and data pages.
//
// These decoders work on bytes that are already in memory, and do no I/O.
// They treat their input as untrusted, because OffsetIndexes and page
// headers carry no checksum. Every offset and size is checked against what
// the footer says, which is checksummed, before it is used. Page bodies
// are checked against their CRC before they are decoded. Any inconsistency
// raises the malformed sstable error.

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <variant>

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include "sstables/parquet/bit_util.hh"
#include "sstables/parquet/compression.hh"
#include "sstables/parquet/encoding.hh"
#include "sstables/parquet/file_summary.hh"
#include "utils/chunked_vector.hh"
#include "utils/fragment_range.hh"
#include "utils/fragmented_temporary_buffer.hh"

namespace sstables::parquet {

/// The OffsetIndexes of the in-place chunks of one row group, in the compact
/// form that the metadata cache stores (reader_cache.hh):
///
/// - A table with one compact_chunk per leaf column of the file, in leaf order.
/// - One compact_page per page, chunk by chunk, in file order.
///
/// Offsets are relative to the start of the row group's region, and rows to
/// the row group's first row. decode_offset_index_block() checks that the
/// region size and the row count fit in 32 bits. The records are in the
/// byte order of the machine, because the form is never written to a file.
struct compact_chunk {
    /// Where the chunk starts: at its dictionary page if it has one,
    /// otherwise at its first data page.
    uint32_t start;
    /// The position of the chunk's first page in the page records.
    uint32_t first_page;
    /// 0 for a chunk that is not in place.
    uint32_t pages;
};
struct compact_page {
    uint32_t offset;
    uint32_t size;
    uint32_t first_row;
};
static_assert(sizeof(compact_chunk) == 12 && sizeof(compact_page) == 12);

/// A reader's copy of the OffsetIndexes of some columns of one row group,
/// which it takes out of the compact form.
///
/// Offsets are file positions. Rows are relative to the row group's first
/// row. The decoder checked that the pages of each chunk are adjacent, that
/// each chunk starts where the previous one ends (see writer.hh), and that
/// the last one ends at the OffsetIndex block. So every page lies in the
/// row group's region, whose bounds come from the footer.
///
/// Every column that a method takes must be one of the index's columns.
class row_group_index {
public:
    struct page {
        uint64_t offset;
        uint64_t first_row;
        uint32_t size;
    };
private:
    struct column {
        leaf_index leaf;
        compact_chunk chunk;
    };
    uint64_t _region_start = 0;
    uint64_t _rows = 0;
    // In increasing order of leaf.
    utils::chunked_vector<column> _columns;
    // The pages of _columns, column by column. A column's first_page is a
    // position here.
    utils::chunked_vector<compact_page> _pages;

    const compact_chunk& chunk(leaf_index c) const;
    page page_record(uint32_t i) const noexcept {
        const auto& p = _pages[i];
        return page{.offset = _region_start + p.offset, .first_row = p.first_row, .size = p.size};
    }
    friend class row_group_index_copier;
public:
    uint64_t rows() const noexcept { return _rows; }
    /// The pages of column `c`. 0 for a chunk that is not in place.
    uint32_t pages(leaf_index c) const { return chunk(c).pages; }
    /// Where the chunk of column `c` starts: at its dictionary page if it
    /// has one, otherwise at its first data page.
    uint64_t chunk_start(leaf_index c) const { return _region_start + chunk(c).start; }
    page page_at(leaf_index c, uint32_t j) const { return page_record(chunk(c).first_page + j); }
    /// The page of column `c` that holds row `row`. The chunk must be in place.
    uint32_t page_of(leaf_index c, uint64_t row) const;
    /// One past the last row of page `j` of column `c`.
    uint64_t page_end_row(leaf_index c, uint32_t j) const {
        const auto& ch = chunk(c);
        return j + 1 < ch.pages ? _pages[ch.first_page + j + 1].first_row : _rows;
    }
    /// The bytes of the dictionary page of column `c`: from the start of
    /// its chunk to its first data page.
    uint64_t dictionary_size(leaf_index c) const {
        const auto& ch = chunk(c);
        return _pages[ch.first_page].offset - ch.start;
    }

    /// The memory that the index holds.
    size_t memory_usage() const noexcept { return memory_usage(_columns.size(), _pages.size()); }
    /// The memory of an index of `columns` columns with `pages` pages.
    static size_t memory_usage(size_t columns, size_t pages) noexcept {
        return sizeof(row_group_index) + columns * sizeof(column) + pages * sizeof(compact_page);
    }
};

/// Copies the records of some columns out of the compact form of the
/// OffsetIndexes of row group `g` into a row_group_index. The copy goes in
/// steps, and the caller may yield between them. The compact form may move
/// between the calls, so each call takes a view of it, and the caller
/// keeps it from moving during the call. No call but allocate() allocates.
///
/// Use: construct, charge memory_usage(), allocate(), then copy() until it
/// returns true, and release().
class row_group_index_copier {
    row_group_index _index;
    // The columns, in increasing order, or all leaf columns of the file if
    // empty and _all is set. The caller keeps them alive.
    std::span<const leaf_index> _columns;
    bool _all;
    size_t _leaves;
    // The page records of the compact form.
    size_t _compact_pages;
    // The pages of the columns.
    size_t _pages = 0;
    // The progress of copy(): the column whose pages are copied next, the
    // pages of it that are copied, and where they start in the index.
    size_t _next_column = 0;
    uint32_t _next_page = 0;
    uint32_t _column_base = 0;

    size_t columns() const noexcept { return _all ? _leaves : _columns.size(); }
    leaf_index column_at(size_t i) const noexcept { return _all ? leaf_index(i) : _columns[i]; }
    template <FragmentedView View>
    void check(View compact);
public:
    /// `columns` are in increasing order. The copier reads their records
    /// in the column table of `compact`.
    template <FragmentedView View>
    row_group_index_copier(const file_summary& s, uint32_t g, std::span<const leaf_index> columns, View compact);
    /// The same, for all leaf columns of the file.
    struct all_columns_tag {};
    template <FragmentedView View>
    row_group_index_copier(const file_summary& s, uint32_t g, all_columns_tag, View compact);

    /// The memory of the index, which allocate() allocates.
    size_t memory_usage() const noexcept { return row_group_index::memory_usage(columns(), _pages); }
    void allocate();
    /// Copies at most about `max_records` records. Returns whether the copy is complete.
    template <FragmentedView View>
    bool copy(View compact, size_t max_records) noexcept;
    row_group_index release() && noexcept { return std::move(_index); }
};

/// Decodes the OffsetIndex block of row group `g`, whose location the
/// summary gives, into the compact form. Yields between OffsetIndexes.
future<fragmented_temporary_buffer> decode_offset_index_block(const file_summary& s, uint32_t g, fragmented_temporary_buffer block);

/// Copies the index of `columns`, in increasing order, out of the compact
/// form of row group `g`. Yields between steps.
future<row_group_index> make_row_group_index(const file_summary& s, uint32_t g, std::span<const leaf_index> columns,
        const fragmented_temporary_buffer& compact);

/// decode_offset_index_block() and an index of all leaf columns in one step.
future<row_group_index> decode_row_group_index(const file_summary& s, uint32_t g, fragmented_temporary_buffer block);

/// A bound on the memory that decode_row_group_index() allocates for row
/// group `g`, from the size of its OffsetIndex block, which the footer
/// gives. It covers decode_offset_index_block() and then
/// make_row_group_index() of any columns, while the compact form lives.
size_t row_group_index_decode_memory(const file_summary& s, uint32_t g) noexcept;

/// The sanity checks that the reader makes on a page header. A page has no
/// record of its own beyond its header, so these checks and the checksum are
/// all that guard it. Each raises the malformed sstable error.
///
/// check_page_sizes(): the sizes of a page whose bytes after the header are
/// `body_size`, and the presence of a checksum.
void check_page_sizes(const page_header& h, uint64_t body_size);
/// check_page_checksum(): `crc` is the CRC-32 of the bytes after the header.
/// check_page_sizes() must have passed.
void check_page_checksum(const page_header& h, uint32_t crc);
/// check_dictionary_page_header(): the dictionary page of column `c`.
/// check_page_sizes() must have passed.
void check_dictionary_page_header(const leaf_column& col, leaf_index c, const page_header& h);
/// check_data_page_header(): a data page of column `c` that the OffsetIndex
/// says covers `rows` rows. check_page_sizes() must have passed.
void check_data_page_header(const leaf_column& col, leaf_index c, const page_header& h, uint64_t rows);
/// check_uncompressed_size(): a page of column `c` declares at most
/// `max_uncompressed_size` bytes. The reader and the verifier pass the size
/// of the largest chunk of the row group (file_summary::row_group_entry).
void check_uncompressed_size(leaf_index c, const page_header& h, uint64_t max_uncompressed_size);

/// A decoded dictionary page. Booleans have no dictionary.
using any_dictionary = std::variant<dictionary<int32_t>, dictionary<int64_t>, dictionary<float>, dictionary<double>, dictionary<byte_array_view>>;
/// A reader's dictionary is shared by the pages that use it.
using dictionary_ptr = lw_shared_ptr<const any_dictionary>;

/// The dictionary of column `col` whose compact form (see dictionary) is
/// `compact`, which the metadata cache stores.
any_dictionary make_dictionary(const leaf_column& col, fragmented_temporary_buffer compact);
/// The compact form of a dictionary.
const fragmented_temporary_buffer& compact_dictionary(const any_dictionary& d) noexcept;
/// The memory that a dictionary holds.
size_t dictionary_memory_usage(const any_dictionary& d) noexcept;

/// A dictionary page whose header is parsed and checked, and whose checksum
/// is verified. Nothing is decompressed or decoded yet, so a reader can ask
/// for the memory that decoding takes before it decodes.
class checked_dictionary_page {
    fragmented_temporary_buffer _bytes;
    page_header _header;
    // The size of the header, at the start of _bytes.
    uint64_t _header_size = 0;
    size_t _decode_memory = 0;
    friend future<checked_dictionary_page> check_dictionary_page(const leaf_column&, leaf_index, fragmented_temporary_buffer, uint64_t);
    friend future<any_dictionary> decode_dictionary_page(const leaf_column&, leaf_index, const checked_dictionary_page&, page_compressor&);
public:
    /// The memory that decode_dictionary_page() allocates: the page body,
    /// uncompressed, if the page is compressed, and the dictionary.
    size_t decode_memory() const noexcept { return _decode_memory; }
    /// The page as it was checked.
    const fragmented_temporary_buffer& bytes() const noexcept { return _bytes; }
};

/// Checks the dictionary page of column `c`, which is the whole of `page`.
/// A page that declares more than `max_uncompressed_size` bytes is
/// malformed. Yields between the fragments of the page while it computes
/// the checksum.
future<checked_dictionary_page> check_dictionary_page(const leaf_column& col, leaf_index c, fragmented_temporary_buffer page,
        uint64_t max_uncompressed_size = std::numeric_limits<uint64_t>::max());

/// Decodes a checked dictionary page of column `c`. The dictionary owns a
/// new copy of the body, so it holds none of the buffers of `page`. Yields
/// between batches of entries, and decompresses as decoded_page does.
future<any_dictionary> decode_dictionary_page(const leaf_column& col, leaf_index c, const checked_dictionary_page& page, page_compressor& compressor);

/// A decoded data page: the levels of its entries and its non-null values.
///
/// Levels are kept as segments. An RLE run of at least 8 entries with the
/// same levels is one uniform segment, which costs the same whatever its
/// length. Shorter runs, and the bit-packed levels of a column that is not
/// repeated, are stored entry by entry in literal segments. So a long
/// run of nulls costs constant time to decode and to step over, and the
/// memory of the levels is bounded by the level bytes of the page.
///
/// Positions within the page are row starts: entries with repetition
/// level 0. A position can be moved forward and backward row by row.
///
/// The page owns the buffers that its byte array values are views of: the
/// page as fetched, the decompressed values, or the dictionary. The views
/// stay valid while the page lives.
class decoded_page {
public:
    using values_type = std::variant<
            utils::chunked_vector<bool>,
            utils::chunked_vector<int32_t>,
            utils::chunked_vector<int64_t>,
            utils::chunked_vector<float>,
            utils::chunked_vector<double>,
            utils::chunked_vector<byte_array_view>>;

    /// A position in the page.
    struct position {
        uint32_t segment = 0;
        /// The entry.
        uint32_t entry = 0;
        /// The row that the entry belongs to.
        uint32_t row = 0;
        /// The number of values before the entry.
        uint32_t value = 0;
    private:
        // A copy of the segment's record and of the entry where the
        // segment ends, so that a step within a segment does not look
        // the segment up. The page sets them when it sets `segment`.
        uint32_t segment_entry = 0;
        uint32_t segment_end = 0;
        uint32_t literal = 0;
        uint8_t rep = 0;
        uint8_t def = 0;
        friend class decoded_page;
    };

private:
    static constexpr uint32_t uniform = std::numeric_limits<uint32_t>::max();
    struct segment {
        uint32_t entry;
        /// The rows that start before `entry`.
        uint32_t row;
        uint32_t value;
        /// The index of the entry's levels in the literal arrays, or `uniform`.
        uint32_t literal;
        uint8_t rep;
        uint8_t def;
    };

    fragmented_temporary_buffer _page;
    fragmented_temporary_buffer _decompressed;
    dictionary_ptr _dictionary;
    // The segments, followed by a sentinel that holds the totals.
    utils::chunked_vector<segment> _segments;
    utils::chunked_vector<uint8_t> _literal_def;
    utils::chunked_vector<uint8_t> _literal_rep;
    values_type _values;
    uint32_t _entries = 0;
    uint32_t _rows = 0;
    uint8_t _max_def = 0;
    uint8_t _max_rep = 0;
    uint8_t _null_threshold = 0;

    struct private_tag {};
    class level_builder;

    // Sets the copy of the record of p.segment. The sentinel does not end.
    void enter_segment(position& p) const noexcept {
        const auto& s = _segments[p.segment];
        p.segment_entry = s.entry;
        p.segment_end = p.segment + 1 < _segments.size() ? _segments[p.segment + 1].entry : uniform;
        p.literal = s.literal;
        p.rep = s.rep;
        p.def = s.def;
    }
    static bool in_uniform(const position& p) noexcept { return p.literal == uniform; }
    void step(position& p) const noexcept {
        p.value += def_at(p) == _max_def;
        ++p.entry;
        if (p.entry == p.segment_end) {
            ++p.segment;
            enter_segment(p);
        }
    }
    void next_row_repeated(position& p) const noexcept;
    void step_back(position& p) const noexcept {
        --p.entry;
        if (p.entry < p.segment_entry) {
            --p.segment;
            enter_segment(p);
        }
        p.value -= def_at(p) == _max_def;
    }
public:
    explicit decoded_page(private_tag) {}

    /// A data page whose header is parsed and checked, and whose checksum
    /// is verified. Nothing is decoded or decompressed yet, so a reader can
    /// ask for the memory that decoding takes before it decodes.
    class checked_page {
        fragmented_temporary_buffer _bytes;
        page_header _header;
        // The size of the header, at the start of _bytes.
        uint64_t _header_size = 0;
        uint64_t _rows = 0;
        size_t _decode_memory = 0;
        friend class decoded_page;
    public:
        checked_page() = default;
        /// A bound on the memory that decode() allocates, beyond the
        /// bytes of the page: the decompressed values, the decoded values,
        /// and the levels. It is exact for the first two.
        size_t decode_memory() const noexcept { return _decode_memory; }
        size_t page_bytes() const noexcept { return _bytes.size_bytes(); }
    };

    // Preemption. check() and decode() yield between the fragments of the
    // page when they compute its checksum, between runs of levels, and
    // between batches of 256 values. Decompression goes through
    // page_compressor::decompress(), which yields between steps of a page
    // above sync_decompress_threshold.

    /// Checks a data page of column `c`, which is the whole of `page`.
    /// `rows` is the number of rows that the OffsetIndex says the page covers.
    /// A page that declares more than `max_uncompressed_size` bytes, or
    /// more than `max_values` entries, is malformed. The reader passes the
    /// size and the entries of the largest chunk of the row group
    /// (file_summary::row_group_entry), so that a damaged header cannot
    /// make it ask for more memory than the chunk needs. The header has no
    /// checksum, and decode_memory() follows from its entries.
    static future<checked_page> check(const leaf_column& col, leaf_index c, fragmented_temporary_buffer page, uint64_t rows,
            uint64_t max_uncompressed_size = std::numeric_limits<uint64_t>::max(),
            uint64_t max_values = std::numeric_limits<uint64_t>::max());

    /// Decodes a checked data page of column `c`. `dict` is the chunk's
    /// dictionary, if it has one.
    static future<std::unique_ptr<decoded_page>> decode(const leaf_column& col, leaf_index c, checked_page page,
            dictionary_ptr dict, page_compressor& compressor);

    /// check() and decode() in one step.
    static future<std::unique_ptr<decoded_page>> decode(const leaf_column& col, leaf_index c, fragmented_temporary_buffer page, uint64_t rows,
            dictionary_ptr dict, page_compressor& compressor) {
        auto checked = co_await check(col, c, std::move(page), rows);
        co_return co_await decode(col, c, std::move(checked), std::move(dict), compressor);
    }

    uint32_t rows() const noexcept { return _rows; }
    uint32_t entries() const noexcept { return _entries; }
    uint8_t max_def() const noexcept { return _max_def; }
    uint8_t max_rep() const noexcept { return _max_rep; }
    const values_type& values() const noexcept { return _values; }

    uint8_t rep_at(const position& p) const noexcept {
        return p.literal == uniform ? p.rep : (_max_rep ? _literal_rep[p.literal + p.entry - p.segment_entry] : 0);
    }
    uint8_t def_at(const position& p) const noexcept {
        return p.literal == uniform ? p.def : _literal_def[p.literal + p.entry - p.segment_entry];
    }
    bool has_value(const position& p) const noexcept { return def_at(p) == _max_def; }
    /// Whether the row at `p` is null, see leaf_column::null_threshold().
    bool is_null_row(const position& p) const noexcept { return rep_at(p) == 0 && def_at(p) < _null_threshold; }

    /// The start of row `row`, which must be below rows().
    position row_start(uint32_t row) const noexcept;
    /// Moves from the start of a row to the start of the next row, or to
    /// the end of the page (entry == entries()).
    void next_row(position& p) const noexcept {
        if (!_max_rep) {
            // One entry per row.
            step(p);
            ++p.row;
            return;
        }
        next_row_repeated(p);
    }
    /// Moves from the start of a row to the start of the previous row.
    /// The row must not be the first.
    void previous_row(position& p) const noexcept;
    /// Moves to the next entry of the same row. Returns false at the end of the row.
    bool next_entry_in_row(position& p) const noexcept {
        step(p);
        return p.entry < _entries && rep_at(p) > 0;
    }
    /// The number of null rows from the row at `p` onward, within the page.
    uint32_t null_rows_forward(position p) const noexcept;
    /// The number of null rows from the row at `p` backward, the row
    /// itself included, within the page.
    uint32_t null_rows_backward(position p) const noexcept;

    size_t memory_usage() const noexcept;
    /// The part of memory_usage() that holds the bytes of the page as
    /// fetched. A reader that fetches through a file tracked by a reader
    /// permit has them charged to the permit already.
    size_t fetched_bytes() const noexcept { return _page.size_bytes(); }
};

} // namespace sstables::parquet
