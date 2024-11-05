/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "schema/schema.hh" // IWYU pragma: keep
#include <span>
#include <memory>

extern seastar::logger trie_logger;


namespace sstables {
    class file_writer;
    class deletion_time;
}

namespace trie {

using const_bytes = std::span<const std::byte>;

template <typename T>
void append_to_vector(std::vector<T>& v, std::span<const T> s) {
    v.insert(v.end(), s.begin(), s.end());
}

struct bti_trie_sink_impl;

// Transforms a stream of trie nodes into a stream of bytes fed to sstables::file_writer.
struct bti_trie_sink {
    std::unique_ptr<bti_trie_sink_impl> _impl;
    bti_trie_sink();
    ~bti_trie_sink();
    bti_trie_sink& operator=(bti_trie_sink&&);
    bti_trie_sink(std::unique_ptr<bti_trie_sink_impl>);
};
bti_trie_sink make_bti_trie_sink(sstables::file_writer&, size_t page_size);

// Transforms a stream of (partition key, offset in Data.db, hash bits) tuples
// into a stream of trie nodes fed into bti_trie_sink.
// Used to populate the Partitions.db file of the BTI format
// with the trie-based index of partition keys.
class partition_trie_writer {
    class impl;
    std::unique_ptr<impl> _impl;
public:
    partition_trie_writer();
    ~partition_trie_writer();
    partition_trie_writer& operator=(partition_trie_writer&&);
public:
    partition_trie_writer(bti_trie_sink&);
    // Add a new partition key to the index.
    void add(const_bytes key, int64_t data_file_offset, uint8_t hash_bits);
    // Flushes all remaining contents, and returns the position of the root node in the output stream.
    // If add() was never called, returns -1.
    // The writer can't be used again after this. (FIXME: it would be better to reset it and allow reuse).
    int64_t finish();
    operator bool() const { return bool(_impl); }
};
partition_trie_writer make_partition_trie_writer(bti_trie_sink&);

// Transforms a stream of clustering index block entries
// into a stream of trie nodes fed into bti_trie_sink.
// Used to populate the Partitions.db file of the BTI format
// with trie-based indexes of clustering keys.
class row_trie_writer {
    class impl;
    std::unique_ptr<impl> _impl;
public:
    row_trie_writer();
    ~row_trie_writer();
    row_trie_writer& operator=(row_trie_writer&&);
public:
    row_trie_writer(bti_trie_sink&);
    // Add a new row index entry.
    void add(const_bytes first_ck, const_bytes last_ck, uint64_t data_file_pos, sstables::deletion_time);
    // Flushes all remaining contents, and returns the position of the root node in the output stream.
    // If add() was never called, returns -1.
    // The writer can't be used again after this. (FIXME: it would be better to reset it and allow reuse).
    int64_t finish();
    operator bool() const { return bool(_impl); }
};
row_trie_writer make_row_trie_writer(bti_trie_sink&);

} // namespace trie