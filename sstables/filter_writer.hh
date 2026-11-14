/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <memory>
#include "bytes.hh"
#include "utils/i_filter.hh"

namespace sstables {

class sstable;
class file_writer;

// Builds the bloom filter of the partition keys of an sstable, and writes
// Filter.db.
//
// If the sstable has a Filter.db and no Index.db, the filter is delayed:
// the writer stores the hash of each partition key in a temporary file,
// and builds a filter of the right size from it at the end. Otherwise it
// builds the filter in memory from `estimated_partitions`, and may rebuild
// it from Index.db at the end.
//
// The caller must call, in a seastar thread:
//  1. The constructor, after the sstable is opened for writing. For a
//     delayed filter, it creates the temporary file.
//  2. add() for each partition, in order.
//  3. finish(), once, after the Data file and Index.db (if any) are complete.
//
// If the writer is destroyed before finish(), it closes the temporary file
// and logs any error.
class filter_writer {
    sstable& _sst;
    // If true, we don't build the bloom filter during the main pass
    // (when Data.db is written), but we instead write the murmur hashes
    // of keys to a temporary file, and later (after Data.db is written,
    // but before the sstable is sealed) we build the bloom filter from that.
    //
    // (Ideally this mechanism should only be used if the optimal size of the
    // filter can't be well estimated in advance. As of this writing we use
    // this mechanism every time the Index component isn't being written).
    bool _delayed_filter;
    // The writer of the temporary file used when `_delayed_filter` is true.
    std::unique_ptr<file_writer> _hashes_writer;
    utils::hashed_key _current_hash{{0, 0}};
    uint64_t _num_partitions = 0;
public:
    // `estimated_partitions` must be at least 1.
    filter_writer(sstable& sst, uint64_t estimated_partitions);
    filter_writer(filter_writer&&) = delete;
    ~filter_writer();

    // Adds a partition key in its serialized form (sstables::key).
    void add(bytes_view partition_key);

    // The murmur hash of the last key passed to add().
    // The trie index stores some of its bits.
    const utils::hashed_key& current_hash() const noexcept {
        return _current_hash;
    }

    // Builds the final filter and writes Filter.db, if the sstable has one.
    void finish();
};

} // namespace sstables
