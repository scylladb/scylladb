/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <bit>
#include <utility>
#include "bti_index.hh"
#include "common.hh"
#include "utils/div_ceil.hh"

// This file contains some declarations which aren't needed by users of BTI readers/writers,
// but are exposed here for testing, or because they are shared between readers and writers.

namespace sstables::trie {

// The compressed data file is read in aligned blocks of this size. To keep the
// on-disk index compact, a chunk's extent (its size in Data.db) is stored not as a
// byte count but as the number of these blocks the chunk touches. The two helpers
// below are the only place that block-size math lives: they convert between the
// byte count carried in a physical position's `extent_hint` and the on-disk
// block count. This detail is hidden from users of the index, who deal only in
// byte counts.
inline constexpr int64_t sstable_compressed_block_size = 512;

// Extent -> on-disk block count, for the writer. A chunk may begin at an
// arbitrary offset within a block, so the count accounts for the leading
// misalignment and rounds up.
inline int64_t bti_extent_to_block_count(int64_t chunk_position, int64_t extent) {
    const int64_t misalignment = chunk_position % sstable_compressed_block_size;
    return div_ceil(misalignment + extent, sstable_compressed_block_size);
}

// On-disk block count -> an upper bound on the chunk's extent, for the reader.
// This is lossy: the recovered value is >= the extent, because it rounds the
// chunk out to whole blocks.
inline int64_t bti_extent_hint_from_block_count(int64_t chunk_position, int64_t block_count) {
    const int64_t misalignment = chunk_position % sstable_compressed_block_size;
    return block_count * sstable_compressed_block_size - misalignment;
}

// Writes the `width` least-significant bytes of `v` to `it`, big-endian.
// Returns the end of the written bytes.
inline std::byte* write_fixed_be(std::byte* it, unsigned __int128 v, size_t width) {
    for (size_t i = 0; i < width; ++i) {
        it[i] = std::byte(v >> (8 * (width - 1 - i)));
    }
    return it + width;
}

// Reads a `width`-byte big-endian unsigned integer from `bytes`, starting at `offset`.
inline unsigned __int128 read_fixed_be_u128(const_bytes bytes, size_t offset, size_t width) {
    unsigned __int128 v = 0;
    for (size_t i = 0; i < width; ++i) {
        v = (v << 8) | std::to_integer<uint8_t>(bytes[offset + i]);
    }
    return v;
}

// FIXME: we calculate the murmur hash when inserting or reading keys
// from bloom filters.
// It's a waste of work to hash the key again.
// The hash should be passed to BTI writers and readers from above,
// and then this function should be eliminated.
std::byte hash_byte_from_key(const schema &s, const partition_key& x);

// Each row index trie in Rows.db is followed by a header containing some partition metadata.
// The partition's entry in Partitions.db points into that header.
struct row_index_header {
    // The partiition key, in BIG serialization format (i.e. the same as in Data.db or Index.db).
    sstables::key partition_key = bytes();
    // The global position of the root node of this partition's row index within Rows.db.
    uint64_t trie_root;
    // The position of the partition inside Data.db, in the sstable's position kind:
    // a logical (uncompressed) position, or a physical one carrying the full chunk
    // coordinates that locate the partition start in the compressed (on-disk) file
    // so the physical cursor can navigate to it directly.
    sstables::sstable_position data_file_position;
    // The partition tombstone of this partition.
    sstables::deletion_time partition_tombstone;
    uint64_t number_of_blocks = 0;
};

future<row_index_header> read_row_index_header(
    const bti_position_encoding& encoding,
    input_stream<char>&& input,
    uint64_t start,
    uint64_t maxlen,
    reader_permit rp
);

void write_row_index_header(
    sstable_version_types sst_ver,
    sstables::file_writer& fw,
    const sstables::key& pk,
    sstable_position partition_data_start,
    const bti_position_encoding& encoding,
    uint64_t added_blocks,
    uint64_t root_pos,
    const sstables::deletion_time& partition_tombstone
);

} // namespace sstables::trie
