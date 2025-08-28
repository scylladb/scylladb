/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <memory>
#include <seastar/core/shared_ptr.hh>
#include "sstables/abstract_index_reader.hh"

namespace sstables {
    class file_writer;
    class clustering_info;
    class deletion_time;
}

namespace dht {
    class decorated_key;
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

// Transforms a stream of (partition key, offset in Data.db, hash bits) tuples
// into a stream of trie nodes fed into bti_trie_sink.
// Used to populate the Partitions.db file of the BTI format
// with the trie-based index of partition keys.
class bti_partition_index_writer {
    class impl;
    std::unique_ptr<impl> _impl;
public:
    // The trie will be written to the given file writer.
    // Note: the file doesn't have to be empty,
    // but it mustn't be extended after `finish()`,
    // because `finish()` writes a footer which is used by the reader
    // to find the root of the trie.
    bti_partition_index_writer(sstables::file_writer&);
    ~bti_partition_index_writer();
    // Add a new partition key to the index.
    void add(const schema&, dht::decorated_key, int64_t data_or_rowsdb_file_pos);
    // Flushes all remaining contents, and returns the position of the root node in the output stream.
    // If add() was never called, returns -1.
    // The writer mustn't be used again after this.
    void finish(sstable_version_types ver, disk_string_view<uint16_t> first_key, disk_string_view<uint16_t> last_key) &&;
};

// Transforms a stream of clustering index block entries
// into a stream of trie nodes fed into bti_trie_sink.
// Used to populate the Partitions.db file of the BTI format
// with trie-based indexes of clustering keys.
class bti_row_index_writer {
    class impl;
    std::unique_ptr<impl> _impl;
public:
    bti_row_index_writer ();
    ~bti_row_index_writer ();
public:
    // The trie will be written to the given file writer.
    // Note: the file doesn't have to be empty,
    // and it can be extended later.
    bti_row_index_writer(sstables::file_writer&);
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
        const partition_key&,
        const sstables::deletion_time& partition_tombstone);
};

// Creates a BTI index reader over the Partitions.db file and the matching Rows.db file.
// `partitions_db_root_pos` should have been read from the Partitions.db footer beforehand.
// (As we don't want to repeat that for every query).
std::unique_ptr<sstables::abstract_index_reader> make_bti_index_reader(
    cached_file& partitions_db,
    cached_file& rows_db,
    uint64_t partitions_db_root_pos,
    uint64_t total_data_db_file_size,
    schema_ptr,
    reader_permit);

} // namespace sstables::trie
