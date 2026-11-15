/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// The write side of the Parquet codec: column writers and the file writer.
//
// The caller declares a schema (schema.hh), appends entries to leaf
// columns, and ends each row. The writer knows Parquet only. It knows
// nothing about CQL types, mutation fragments, or sstables.
//
// File layout:
//
//     PAR1
//     row group 0: in-place chunks in schema order, then their OffsetIndexes in the same order
//     row group 1: the same
//     ...
//     late chunks (all null, no OffsetIndex)
//     footer (FileMetaData), footer length (4 bytes, little endian), PAR1
//
// Invariants that a reader may rely on:
//
// - Data pages are Data Page V2, with a CRC (the standard CRC-32 of the page
//   bytes after the header), repetition and definition levels before the
//   values and outside the compressed part. `is_compressed` is false when
//   compression did not shrink the values, or when the values are empty.
// - A dictionary page, if any, is the first page of its chunk, PLAIN, with
//   a CRC. It starts at the chunk's start, and the first data page follows
//   it directly. Every chunk has data pages.
// - Every page starts at a row boundary, and pages of all columns are cut
//   at the same rows (page set boundaries), though a column may skip a cut.
// - A chunk is "in place" if it lies in its row group's region. The
//   in-place chunks of a row group are adjacent and in schema order. The
//   first starts where the previous row group's OffsetIndex block ends
//   (after the 4 magic bytes for the first row group). Each next one starts
//   at the end of the previous one's last data page.
// - The OffsetIndex block of a row group directly follows its last in-place
//   chunk. It holds one OffsetIndex per in-place chunk, in the same order,
//   adjacent. A row group without in-place chunks has an empty region.
// - A column "has content" from its first entry that is a value or a null
//   with a definition level above 0. For each column, the chunks in place
//   are those of the row groups from the first one in which the column has
//   content to the last. Earlier chunks of the column are late: they are
//   entirely null (every entry at level 0), lie after the last row group,
//   and have no OffsetIndex. The late chunks are adjacent, in schema order
//   and then in row group order, from the end of the last row group's
//   OffsetIndex block to the footer.
// - A column without content in the whole file is not in the file's schema,
//   unless it is declared always present. An always-present column without
//   content has only late chunks.
// - Every chunk records its null count (entries with a definition level
//   below the maximum) in the footer.
// - The footer has no minimum, maximum, or sort order, and there is no
//   ColumnIndex.
//
// Bounds on a data page:
//
// - `num_values` (the entries, nulls included) is at most
//   max_page_values + max_page_set_rows, plus the entries of one row. A row
//   of a repeated column can have any number of entries, because a page
//   holds whole rows.
// - A page holds whole values, so a large value makes a large page. A page
//   above 2 GiB, which a header cannot describe, is an internal error.

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include <seastar/core/sstring.hh>

#include "bytes_ostream.hh"
#include "sstables/parquet/bit_util.hh"
#include "sstables/parquet/compression.hh"
#include "sstables/parquet/metadata.hh"
#include "sstables/parquet/schema.hh"
#include "utils/chunked_vector.hh"
#include "utils/managed_bytes.hh"

namespace sstables {
class file_writer;
}

namespace sstables::parquet {

/// The tunables of the writer. These are writer policies, not format
/// invariants, and the reader handles any values.
struct writer_config {
    /// The writer clamps the size tunables below to these maximums. Larger
    /// values would make pages, dictionaries, and row groups that take too
    /// much memory to write and to read.
    static constexpr uint64_t max_row_group_size = 1 << 30;
    static constexpr uint64_t max_page_size = 1 << 20;
    static constexpr uint64_t max_dictionary_page_limit = 1 << 20;

    /// The largest row group, in bytes of buffered data (see writer::buffered_size()).
    /// The memory handle may ask for less.
    uint64_t row_group_size = 8 << 20;
    /// A page set is cut at the first row boundary after this many bytes of
    /// raw size (see writer::raw_size()) have been appended since the last cut.
    uint64_t page_set_size = 128 << 10;
    /// A column skips a page set cut while its pending page is smaller than
    /// this. Its size is the PLAIN size of its values plus the size of its levels.
    uint64_t min_page_size = 8 << 10;
    /// The size limit of a dictionary page. A chunk whose dictionary would
    /// grow beyond it falls back to another encoding. 0 disables dictionaries.
    uint64_t dictionary_page_limit = 4 << 10;
    /// A page set is cut at the end of a row in which a column's pending page
    /// reached this many entries. A column also takes a cut, whatever its
    /// page size, once its page has half as many. A gap of nulls is split at
    /// page set boundaries to respect it. See the bounds at the top of this file.
    uint32_t max_page_values = 1 << 16;
    /// A page set is cut after this many rows. It is clamped to
    /// max_page_values / 2.
    uint32_t max_page_set_rows = 1 << 14;
    compression_codec codec = compression_codec::uncompressed;
    /// The level of ZSTD and GZIP. The library's default if unset.
    std::optional<int> compression_level;
    sstring created_by = "scylla";
};

/// The writer's link to the per-shard writer memory budget. The writer holds
/// it for its lifetime, and asks it for its row group target each time it
/// checks whether to end a row group, which is at each page set cut. The
/// writer caps the target at row_group_size. A row group ends only at a page
/// set cut, so it holds at least one page set whatever the target.
class writer_memory_handle {
public:
    virtual ~writer_memory_handle() = default;
    virtual uint64_t row_group_target() = 0;
    /// The writer reports its memory_usage() before each call to
    /// row_group_target() and after each row group. A report replaces the
    /// previous one. The handle keeps the last one until it is destroyed.
    virtual void report_memory_usage(uint64_t bytes) noexcept {}
};

/// A handle with a fixed target, for tests and tools.
class fixed_writer_memory_handle final : public writer_memory_handle {
    uint64_t _target;
public:
    explicit fixed_writer_memory_handle(uint64_t target) noexcept : _target(target) {}
    uint64_t row_group_target() override { return _target; }
};

/// Counters of the work that the writer did. Tests use them to check the
/// cost per row without measuring time.
struct writer_stats {
    /// Entries appended. Nulls at level 0 are not counted, because the
    /// writer ignores them.
    uint64_t entries = 0;
    /// Times a page set cut or the end of a row group visited a column.
    uint64_t column_visits = 0;
    uint64_t page_set_cuts = 0;
    uint64_t data_pages = 0;
    uint64_t dictionary_pages = 0;
    /// Pages that fell back from a dictionary to another encoding, at the
    /// start of the fallback.
    uint64_t dictionary_fallbacks = 0;
    uint64_t row_groups = 0;
    uint64_t late_chunks = 0;
};

/// What finish() reports about the file.
struct writer_result {
    uint64_t file_size;
    uint64_t rows;
    /// The total raw size of the rows. See writer::raw_size().
    uint64_t raw_size;
    uint64_t row_groups;
    /// The size and the checksum of the serialized FileMetaData. The
    /// checksum is the standard CRC-32 (crc32_utils).
    footer_info footer;
};

class column_writer;

/// Writes a Parquet file to a file_writer.
///
/// The writer buffers a whole row group in memory, in chains of contiguous
/// pieces of at most 128 KiB, then writes it out chunk by chunk, followed by
/// the OffsetIndexes. All methods that may write to the file_writer or compress
/// a page must run in a seastar thread: append(), append_null(), end_row(),
/// and finish(). The compression of a large page yields. The file_writer
/// must be at the start of the file, because Parquet offsets are file
/// positions.
///
/// Cost: an append costs constant amortized time, apart from encoding and
/// compressing the pages that it completes. Appends do no I/O. Ending a row costs
/// constant time, except when it cuts a page set, which visits only the
/// columns that received an entry since the previous cut, or ends a row
/// group, which visits every column that has state. A column has state once
/// it has content (see above). Before that, it costs nothing.
///
/// Nulls: a column records how many rows it has seen. A row that has no
/// entry for a column is null there (definition level 0). The column
/// records a gap of such rows as one run, when it next gets an entry or when
/// the row group ends.
class writer {
    sstables::file_writer& _out;
    schema _schema;
    writer_config _cfg;
    std::unique_ptr<writer_memory_handle> _memory;
    std::unique_ptr<page_compressor> _compressor;
    // Position of each column in schema order.
    utils::chunked_vector<uint32_t> _schema_position;
    // The state of each column, null until the column has content.
    utils::chunked_vector<std::unique_ptr<column_writer>> _columns;
    // Columns with state from earlier row groups, in schema order.
    utils::chunked_vector<column_id> _active;
    // Columns that got state in the current row group.
    utils::chunked_vector<column_id> _new;
    // Columns that received an entry since the last page set cut.
    utils::chunked_vector<column_id> _touched;
    // The rows of the current row group at which a page set started, after the first.
    utils::chunked_vector<uint64_t> _page_set_starts;

    struct row_group_record {
        int64_t num_rows;
        // The start of the first in-place chunk, if there is one.
        std::optional<int64_t> file_offset;
    };
    utils::chunked_vector<row_group_record> _row_groups;

    bool _magic_written = false;
    bool _row_open = false;
    bool _force_cut = false;
    bool _finished = false;
    uint64_t _rows_before = 0;      // rows in finished row groups
    uint64_t _rg_rows = 0;          // rows ended in the current row group
    uint64_t _page_set_rows = 0;
    uint64_t _page_set_raw = 0;     // raw size appended since the last page set cut
    uint64_t _row_raw = 0;
    uint64_t _total_raw = 0;        // raw size of the ended rows
    uint64_t _flushed_data = 0;     // bytes written for finished row groups
    // Buffered bytes of the current row group: encoded pages, and the raw size of pending pages.
    uint64_t _rg_encoded = 0;
    uint64_t _pending_raw = 0;
    // The footer records of the chunks written so far.
    uint64_t _footer_memory = 0;
    // The dictionaries of the current chunks, with the indices of their pending pages.
    uint64_t _dictionary_memory = 0;
    writer_stats _stats;

    friend class column_writer;
    template <typename Store, typename Dictionary> friend class value_column;

    column_writer& state(column_id c);
    template <typename T> T& typed(column_id c, physical_type t);
    void before_entry(column_id c, levels l, bool has_value);
    void after_entry(column_id c, column_writer& cw);
    template <typename Column, typename V>
    void append_value(column_id c, levels l, physical_type t, V v, uint64_t raw);
    void write_magic();
    void cut_page_set();
    void end_row_group();
    void write_late_chunk(column_writer& cw, uint64_t rows);
    void write(const bytes_ostream& b);
    void report_memory_usage() noexcept;
public:
    writer(sstables::file_writer& out, schema s, writer_config cfg, std::unique_ptr<writer_memory_handle> memory = nullptr);
    ~writer();

    const schema& get_schema() const noexcept { return _schema; }

    // Appending entries.
    //
    // Each entry has levels. `def` equal to the column's max_def appends a
    // value, and a lower `def` appends a null at that level. The first entry
    // of a column in a row has rep 0, and the others have rep above 0.
    // A row without an entry is null at level 0. An explicit entry with
    // def 0 is therefore redundant, and the writer ignores it.
    //
    // The value's type must match the column's physical type: bool for
    // BOOLEAN, int32_t, int64_t, float, double, and a byte view for
    // BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY. The raw size of a value is its
    // width (1 for a boolean) or its length.
    //
    // The writer checks the levels against the column's maximums and the
    // first entry of a row against rep 0. It does not check that the levels
    // of a row are consistent with each other. That is the caller's job.

    void append(column_id c, levels l, bool v);
    void append(column_id c, levels l, int32_t v);
    void append(column_id c, levels l, int64_t v);
    void append(column_id c, levels l, float v);
    void append(column_id c, levels l, double v);
    void append(column_id c, levels l, bytes_view v);
    void append(column_id c, levels l, managed_bytes_view v);
    void append(column_id c, levels l, byte_array_view v);
    void append_null(column_id c, levels l);

    /// Appends a value to a column that is not repeated, at its max_def.
    template <typename V>
    void append(column_id c, V v) {
        append(c, levels{.rep = 0, .def = _schema.get_column(c).max_def}, v);
    }

    /// Ends the current row. Returns true if a page set ended at this row,
    /// so that the next row starts a page set. A row group end is also a page
    /// set end. Must run in a seastar thread.
    bool end_row();

    /// Ends the file: writes the last row group, the late chunks, the
    /// footer with `key_value_metadata`, and the trailer. It does not close
    /// the file_writer. Must run in a seastar thread, at a row boundary.
    writer_result finish(std::vector<key_value> key_value_metadata = {});

    // Size measures (PLAN.md, "Size measures").

    /// The ordinal of the next row: the number of rows ended so far.
    uint64_t rows() const noexcept { return _rows_before + _rg_rows; }
    /// The raw size of the row in progress: the sum, over its values, of
    /// each value's raw size. Read it before end_row().
    uint64_t current_row_raw_size() const noexcept { return _row_raw; }
    /// The total raw size of the ended rows.
    uint64_t raw_size() const noexcept { return _total_raw; }
    /// The bytes written to the file_writer so far.
    uint64_t flushed_size() const noexcept;
    /// The estimated size of the file if it ended now: the bytes flushed,
    /// plus the encoded pages of the current row group, plus the raw size
    /// not yet encoded times the ratio of encoded bytes to raw size of the
    /// pages encoded so far (1 before the first page). The error is at most
    /// the values not yet encoded, which are less than a row group.
    /// Footer, dictionaries, OffsetIndexes, and late chunks are not counted.
    uint64_t estimated_size() const noexcept;
    /// The memory of the current row group that counts against the row
    /// group target: the encoded pages, plus the raw size of the pending
    /// pages. Dictionaries are not counted. Each is bounded by
    /// dictionary_page_limit.
    uint64_t buffered_size() const noexcept { return _rg_encoded + _pending_raw; }
    /// An estimate of the memory that the writer holds, which it reports to
    /// its memory handle: buffered_size(), plus the footer records of the
    /// chunks written so far, plus the dictionaries of the current chunks
    /// and the dictionary indices of the pending pages. It leaves out the
    /// levels of the pending pages, the second copy of a page while it is
    /// encoded or compressed, and the fixed state of the columns.
    uint64_t memory_usage() const noexcept { return buffered_size() + _footer_memory + _dictionary_memory; }

    const writer_stats& stats() const noexcept { return _stats; }
};

} // namespace sstables::parquet
