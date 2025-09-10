/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "bti_index.hh"

// This file contains some declarations which aren't needed by users of BTI readers/writers,
// but are exposed here for testing, or because they are shared between readers and writers.

namespace sstables::trie {

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
    // The global position of the partition inside Data.db.
    uint64_t data_file_offset;
    // The partition tombstone of this partition.
    sstables::deletion_time partition_tombstone;
    uint64_t number_of_blocks = 0;
};

future<row_index_header> read_row_index_header(input_stream<char>&& input, uint64_t start, uint64_t maxlen, reader_permit rp);
void write_row_index_header(
    sstable_version_types sst_ver,
    sstables::file_writer& fw,
    const sstables::key& pk,
    int64_t partition_data_start,
    uint64_t added_blocks,
    uint64_t root_pos,
    const sstables::deletion_time& partition_tombstone
);

} // namespace sstables::trie
