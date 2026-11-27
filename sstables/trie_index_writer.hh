/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <memory>
#include <optional>
#include "dht/decorated_key.hh"
#include "sstables/trie/bti_index.hh"
#include <seastar/util/optimized_optional.hh>

class schema;

namespace utils {
class hashed_key;
}

namespace sstables {

class sstable;
class file_writer;
class key;
struct clustering_info;
struct deletion_time;

// Writes the trie index of an sstable: Partitions.db and Rows.db.
//
// Positions are opaque to this code, and the index reader returns them
// unchanged. The writer chooses their unit, for example a byte offset in
// the Data file or a row ordinal, and uses the same unit for all positions
// of one sstable.
//
// The caller must call, in a seastar thread:
//  1. The constructor, after the sstable is opened for writing. It creates
//     Rows.db and then Partitions.db.
//  2. For each partition, in order:
//     start_partition(),
//     add_row_index_block() for each block of the row index, in order (see
//     row_index_block_tracker; a partition has either no block or at least two),
//     end_partition().
//  3. finish(), once, after the last partition. It must not be called if
//     no partition was written.
//
// If the writer is destroyed before finish(), it closes the files and logs
// any error.
class trie_index_writer {
    sstable& _sst;
    std::unique_ptr<file_writer> _rows_writer;
    std::unique_ptr<file_writer> _partitions_writer;
    optimized_optional<trie::bti_row_index_writer> _row_index_writer;
    optimized_optional<trie::bti_partition_index_writer> _partition_index_writer;
    // The key of the last start_partition() call.
    // A partition key can only be inserted into Partitions.db
    // after its intra-partition index is written to Rows.db.
    // So we need to hold onto the key until end_partition().
    std::optional<dht::decorated_key> _current_dk;
    // The position of the current partition.
    uint64_t _current_partition_position = 0;
public:
    explicit trie_index_writer(sstable& sst);
    trie_index_writer(trie_index_writer&&) = delete;
    ~trie_index_writer();

    // `partition_position` is the position of the partition in the Data file.
    // The index reader returns it for a lookup of this partition.
    void start_partition(const dht::decorated_key& dk, uint64_t partition_position);

    // Adds a block of the row index of the current partition.
    // `offset_from_partition_start` is the position of the block's first
    // entry minus the partition's position (row_index_block::offset).
    // `preceding_range_tombstone` is the range tombstone open before the
    // block's first entry, in its on-disk form.
    void add_row_index_block(const schema& s, const clustering_info& first, const clustering_info& last,
            uint64_t offset_from_partition_start, const deletion_time& preceding_range_tombstone);

    // Ends the current partition.
    // `hash` is the murmur hash of the partition key, as in the bloom filter.
    // `partition_end` is the position where the partition's last entry ends.
    // The row index stores it as the end of the last block.
    // For the mx Data file, it is the position of the end-of-partition flag.
    // `partition_key` and `partition_tombstone` are stored in Rows.db if the
    // partition has a row index.
    void end_partition(const schema& s, const utils::hashed_key& hash, uint64_t partition_end,
            const key& partition_key, const deletion_time& partition_tombstone);

    // Writes the footer of Partitions.db, closes Partitions.db and then
    // Rows.db, and records their digests in the sstable. Stores the footer
    // in the sstable, so that sstable::set_first_and_last_keys() can use it.
    // `first_key` and `last_key` are the first and the last partition key
    // of the sstable.
    void finish(const key& first_key, const key& last_key);
};

} // namespace sstables
