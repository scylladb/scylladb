/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// What the reader keeps from a Parquet footer.
//
// The reader parses the footer once, when the file is opened, and keeps a
// compact summary: the file's schema, the row counts, the location of each
// row group's OffsetIndex block, and two bits per column chunk. Everything
// else follows from the layout invariants of our writer (see writer.hh)
// and from the OffsetIndexes, which the reader fetches on demand.
//
// The reader supports only files that follow those invariants. Opening
// checks the footer against them and raises the malformed sstable error for
// any other file. It does not try to read such a file differently.
//
// A summary is immutable once built and holds no shard-local state, such
// as lw_shared_ptr. So one shard can own it, for example through a
// foreign_ptr, and other shards can read it.

#include <cstdint>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include "sstables/parquet/metadata.hh"
#include "utils/chunked_vector.hh"
#include "utils/small_vector.hh"
#include "seastarx.hh"

namespace sstables::parquet {

/// The index of a leaf column in the file's schema, in schema order.
/// It is the position of the column's chunk in each row group.
using leaf_index = uint32_t;

/// A leaf column of the file's schema.
struct leaf_column {
    /// The names from the root's child down to the leaf.
    std::vector<sstring> path;
    physical_type type;
    /// The length of a FIXED_LEN_BYTE_ARRAY value, 0 for the other types.
    int32_t type_length = 0;
    uint8_t max_def = 0;
    uint8_t max_rep = 0;
    /// For each repetition level r from 1 to max_rep, at index r - 1: the
    /// definition level at which the repeated node of level r has at least
    /// one element. An entry with repetition level r has at least this
    /// definition level. Increasing.
    utils::small_vector<uint8_t, 2> repeated_def;
    /// The compression codec of all chunks of the column.
    compression_codec codec = compression_codec::uncompressed;
    /// The index of the leaf's schema element in file_summary::elements().
    uint32_t element = 0;
    /// The first row group in which the column's chunk is in place.
    /// Earlier chunks are late, so entirely null. Equal to the row group
    /// count if the column has no chunk in place.
    uint32_t first_in_place = 0;

    /// A row is null when it has no value: for a column that is not
    /// repeated, its entry is below max_def; for a repeated column, its
    /// outermost repeated node has no element, so the row has a single
    /// entry with a definition level below this threshold.
    uint8_t null_threshold() const noexcept {
        return max_rep ? repeated_def[0] : max_def;
    }
};

/// The compact summary of a footer. See the top of this file.
///
/// Permanent memory: 40 bytes per row group, 2 bits per column chunk, and
/// per leaf column its path and about 64 bytes, plus the schema elements and
/// the key-value metadata.
class file_summary {
public:
    struct row_group_entry {
        /// The ordinal of the row group's first row.
        uint64_t first_row;
        /// The location of the OffsetIndex block. It directly follows the
        /// row group's last in-place chunk, so it is also the end of the
        /// row group's region.
        uint64_t offset_index_offset;
        uint32_t offset_index_length;
        /// The largest total_uncompressed_size of the row group's chunks. A
        /// page is part of a chunk, so this bounds the uncompressed size of
        /// any page of the row group, by values from the checksummed footer.
        uint64_t max_chunk_uncompressed_size;
        /// The largest num_values of the row group's chunks. It bounds the
        /// entries of any page of the row group, by values from the
        /// checksummed footer.
        uint64_t max_chunk_values;
    };
private:
    utils::chunked_vector<schema_element> _elements;
    utils::chunked_vector<leaf_column> _columns;
    utils::chunked_vector<row_group_entry> _row_groups;
    // Two bits per chunk, row group by row group: all null, has a dictionary.
    utils::chunked_vector<uint64_t> _chunk_bits;
    utils::chunked_vector<key_value> _key_value_metadata;
    uint64_t _rows = 0;
    uint64_t _footer_offset = 0;

    size_t bit_index(uint32_t row_group, leaf_index c) const noexcept {
        return (size_t(row_group) * _columns.size() + c) * 2;
    }
    bool bit(size_t i) const noexcept {
        return (_chunk_bits[i / 64] >> (i % 64)) & 1;
    }

    friend class summary_builder;
public:
    /// The schema elements of the footer, in schema order.
    const utils::chunked_vector<schema_element>& elements() const noexcept { return _elements; }
    /// The leaf columns, in schema order.
    const utils::chunked_vector<leaf_column>& columns() const noexcept { return _columns; }
    const leaf_column& column(leaf_index c) const { return _columns.at(c); }
    /// Finds a leaf column by its path.
    std::optional<leaf_index> find_column(std::span<const std::string_view> path) const noexcept;
    std::optional<leaf_index> find_column(std::initializer_list<std::string_view> path) const noexcept {
        return find_column(std::span<const std::string_view>(path.begin(), path.size()));
    }

    const utils::chunked_vector<key_value>& key_value_metadata() const noexcept { return _key_value_metadata; }

    /// The total number of rows.
    uint64_t rows() const noexcept { return _rows; }
    uint32_t row_groups() const noexcept { return _row_groups.size(); }
    uint64_t row_group_first_row(uint32_t g) const noexcept { return _row_groups[g].first_row; }
    /// One past the last row of row group `g`.
    uint64_t row_group_end_row(uint32_t g) const noexcept {
        return g + 1 < _row_groups.size() ? _row_groups[g + 1].first_row : _rows;
    }
    uint64_t row_group_rows(uint32_t g) const noexcept { return row_group_end_row(g) - row_group_first_row(g); }
    /// The row group that holds row `row`, which must be below rows().
    uint32_t row_group_of(uint64_t row) const noexcept;
    const row_group_entry& row_group(uint32_t g) const noexcept { return _row_groups[g]; }
    /// The start of row group `g`'s region: where its first in-place chunk
    /// starts, or where its OffsetIndex block starts if it has no chunk in place.
    uint64_t region_start(uint32_t g) const noexcept {
        return g ? _row_groups[g - 1].offset_index_offset + _row_groups[g - 1].offset_index_length : magic.size();
    }
    /// The bytes of the in-place chunks of row group `g`.
    uint64_t region_size(uint32_t g) const noexcept { return _row_groups[g].offset_index_offset - region_start(g); }

    /// Whether every entry of the chunk is null at level 0. The reader does
    /// no I/O for such a chunk.
    bool all_null(uint32_t row_group, leaf_index c) const noexcept { return bit(bit_index(row_group, c)); }
    /// Whether the chunk starts with a dictionary page.
    bool has_dictionary(uint32_t row_group, leaf_index c) const noexcept { return bit(bit_index(row_group, c) + 1); }
    /// Whether the chunk is in place, so its row group's OffsetIndex block covers it.
    bool in_place(uint32_t row_group, leaf_index c) const noexcept { return row_group >= _columns[c].first_in_place; }

    /// The offset of the serialized FileMetaData.
    uint64_t footer_offset() const noexcept { return _footer_offset; }

    /// An estimate of the memory that the summary holds.
    size_t memory_usage() const noexcept;
};

/// Reads the footer of a Parquet file of `file_size` bytes and builds its
/// summary. The footer is parsed from an input stream over the end of the
/// file, so it is never held in memory as a whole.
///
/// If `expected_crc32` is set, it must be the checksum of the serialized
/// FileMetaData (footer_info::crc32), and a mismatch raises the malformed
/// sstable error. So does a file that does not follow our layout.
future<file_summary> read_file_summary(file f, uint64_t file_size, std::optional<uint32_t> expected_crc32);

} // namespace sstables::parquet
