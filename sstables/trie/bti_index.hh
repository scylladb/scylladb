/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <memory>
#include <seastar/core/shared_ptr.hh>
#include "sstables/abstract_index_reader.hh"
#include "sstables/framed_chunk.hh"
#include "utils/assert.hh"

namespace sstables {
    class file_writer;
    class clustering_info;
    class deletion_time;
}

namespace dht {
    class decorated_key;
}

namespace seastar {
    class file;
}

namespace utils {
    class hashed_key;
}

class schema;
class cached_file;
class reader_permit;

using schema_ptr = seastar::lw_shared_ptr<const schema>;

namespace sstables::trie {

// IMPORTANT: the index reader is currently page-oriented and
// it assumes that BTI nodes don't cross page boundaries.
//
// Therefore, modifying the page size is a breaking change,
// because old sstables will violate the assumptions of new readers,
// and new sstables will violate the assumptions of old readers.
//
// If you want to change the page size, you must teach new readers how to handle both
// the old and the new page size, and new writers must keep writing with the old page size
// until a cluster feature guarding the new page size is set.
constexpr static uint64_t BTI_PAGE_SIZE = 4096;

struct bti_partitions_db_footer {
    sstables::key first_key;
    sstables::key last_key;
    uint64_t partition_count;
    uint64_t trie_root_position;
};

// The bit layout of the packed (block count, offset within chunk) fields in
// physical BTI index positions. The index stores the chunk's extent as a block
// count N, and the offset within the chunk as O. The widths of both fields depend
// on the sstable's uncompressed chunk length L and on its chunk frames, so the
// layout is computed once from the sstable's chunk codec. This is the only
// implementation of the layout: the partition payload, the row payload and the
// row index header all pack and unpack (N, O) here.
class bti_physical_layout {
    uint32_t _uncompressed_chunk_length;
    uint64_t _max_block_count;
    unsigned _block_count_bits;
    unsigned _offset_within_chunk_bits;
public:
    // Throws malformed_sstable_exception unless L is a power of two.
    // A physical position doesn't hold the chunk's length, so a reader finds
    // chunk boundaries from the chunk frames. Thus, the codec must be framed.
    explicit bti_physical_layout(const framed_chunk_codec& codec);

    uint32_t uncompressed_chunk_length() const noexcept {
        return _uncompressed_chunk_length;
    }
    // The upper bound on the block count N of a chunk. A chunk's compressed data
    // is at most compressed_chunk_length_limit() bytes. Its on-disk extent also
    // includes the header, the checksum and the footer. The chunk may start at any
    // offset within a block, so the count also covers up to block size - 1 bytes
    // of leading misalignment.
    uint64_t max_block_count() const noexcept {
        return _max_block_count;
    }
    // N is a 1-based value in [1, max_block_count()], stored zero-based
    // in exactly this many bits.
    unsigned block_count_bits() const noexcept {
        return _block_count_bits;
    }
    unsigned offset_within_chunk_bits() const noexcept {
        return _offset_within_chunk_bits;
    }
    unsigned packed_bits() const noexcept {
        return _block_count_bits + _offset_within_chunk_bits;
    }
    size_t packed_bytes() const noexcept {
        return (packed_bits() + 7) / 8;
    }

    // Packs N and O into the low packed_bits() bits of the result.
    uint64_t pack(uint64_t block_count, uint64_t offset_within_chunk) const {
        SCYLLA_ASSERT(block_count >= 1);
        SCYLLA_ASSERT(block_count <= _max_block_count);
        SCYLLA_ASSERT(offset_within_chunk < _uncompressed_chunk_length);
        return ((block_count - 1) << _offset_within_chunk_bits) | offset_within_chunk;
    }

    struct unpacked {
        uint64_t block_count;
        uint64_t offset_within_chunk;
    };
    // Inverse of pack(). Bits above packed_bits() are ignored.
    unpacked unpack(uint64_t packed) const noexcept {
        const auto encoded_length = (packed >> _offset_within_chunk_bits) & ((uint64_t{1} << _block_count_bits) - 1);
        return unpacked{
            .block_count = encoded_length + 1,
            .offset_within_chunk = packed & ((uint64_t{1} << _offset_within_chunk_bits) - 1),
        };
    }
};

// Transforms a stream of (partition key, offset in Data.db, hash bits) tuples
// into a stream of trie nodes fed into bti_trie_sink.
// Used to populate the Partitions.db file of the BTI format
// with the trie-based index of partition keys.
class bti_partition_index_writer {
    class impl;
    std::unique_ptr<impl> _impl;
private:
    friend class optimized_optional<bti_partition_index_writer>;
    bti_partition_index_writer() noexcept;
public:
    // The trie will be written to the given file writer.
    // Note: the file doesn't have to be empty,
    // but it mustn't be extended after `finish()`,
    // because `finish()` writes a footer which is used by the reader
    // to find the root of the trie.
    explicit bti_partition_index_writer(sstable_version_types, sstables::file_writer&);
    bti_partition_index_writer(bti_partition_index_writer&&) noexcept;
    bti_partition_index_writer& operator=(bti_partition_index_writer&&) noexcept;
    ~bti_partition_index_writer() noexcept;
    explicit operator bool() const noexcept { return bool(_impl); }
    // Add a new partition key to the index.
    void add(const schema&, dht::decorated_key, const utils::hashed_key&, int64_t data_or_rowsdb_file_pos);
    // Flushes all remaining contents, and returns the position of the root node in the output stream.
    // If add() was never called, returns -1.
    // The writer mustn't be used again after this.
    std::optional<bti_partitions_db_footer> finish(const sstables::key& first_key, const sstables::key& last_key) &&;
};

future<bti_partitions_db_footer> read_bti_partitions_db_footer(const schema& s, sstable_version_types v, const seastar::file& f, uint64_t file_size);

// Transforms a stream of clustering index block entries
// into a stream of trie nodes fed into bti_trie_sink.
// Used to populate the Partitions.db file of the BTI format
// with trie-based indexes of clustering keys.
class bti_row_index_writer {
    class impl;
    std::unique_ptr<impl> _impl;
private:
    friend class optimized_optional<bti_row_index_writer>;
    bti_row_index_writer() noexcept;
public:
    ~bti_row_index_writer() noexcept;
    // The trie will be written to the given file writer.
    // Note: the file doesn't have to be empty,
    // and it can be extended later.
    explicit bti_row_index_writer(sstables::file_writer&);
    bti_row_index_writer(bti_row_index_writer&&) noexcept;
    bti_row_index_writer& operator=(bti_row_index_writer&&) noexcept;
    explicit operator bool() const noexcept { return bool(_impl); }
    // Add a new row index entry.
    // Must be called in ascending order.
    // (`first_ck` must be strictly greater than the previous `last_ck`).
    void add(
        const schema& s,
        const sstables::clustering_info& first_ck,
        const sstables::clustering_info& last_ck,
        uint64_t offset_from_partition_start,
        const sstables::deletion_time& range_tombstone_before_first_ck);
    // Flushes all remaining contents, and returns the position of the root node in the output stream.
    // If add() was never called, returns -1.
    //
    // Note: the writer can be reused after this.
    // (This is unlike with the partition index writer, which can't be reused.
    // This is because reusing the row index writer is useful for performance,
    // (without an explicit reuse, it would have to be recreated for every partition)
    // while reusing the partition index writer isn't).
    //
    // Note: `partition_data_start` is the start of the entire partition in Data.db,
    // i.e. the position of the partition key.
    // `partition_data_end` is the position of the END_OF_PARTITION flag byte in Data.db,
    // which lies 1 byte before the start of the next partition.
    // So `partition_data_end` is *NOT* the start position of the next partition.
    int64_t finish(
        sstable_version_types,
        const schema&,
        int64_t partition_data_start,
        int64_t partition_data_end,
        const sstables::key&,
        const sstables::deletion_time& partition_tombstone);
};

// Creates a BTI index reader over the Partitions.db file and the matching Rows.db file.
// `partitions_db_root_pos` should have been read from the Partitions.db footer beforehand.
// (As we don't want to repeat that for every query).
std::unique_ptr<sstables::abstract_index_reader> make_bti_index_reader(
    seastar::shared_ptr<cached_file> partitions_db,
    seastar::shared_ptr<cached_file> rows_db,
    uint64_t partitions_db_root_pos,
    uint64_t total_data_db_file_size,
    sstable_version_types,
    schema_ptr,
    reader_permit,
    tracing::trace_state_ptr);

} // namespace sstables::trie
