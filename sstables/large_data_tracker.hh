/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <queue>
#include "sstables/types.hh"
#include "keys/keys.hh"

class schema;
class column_definition;

namespace sstables {

class sstable;
class key;

// Detects large partitions, rows, and cells while an sstable is written.
//
// The tracker reports each one to the sstable's large data handler.
// It also collects what the writer stores in Scylla.db: the large data
// statistics and the top-N large data records of each type.
//
// The meaning of a size is up to the caller. The tracker compares it with
// the thresholds of the large data handler, and stores it unchanged. The
// thresholds are in bytes, so a size should be close to the uncompressed
// size of the data that it measures.
//
// All the maybe_record_*() methods wait for the large data handler, so they
// must be called in a seastar thread.
//
// Usage:
//  - maybe_record_large_cell() for each cell, after it is written.
//  - maybe_record_large_row() for each static or clustering row, after it is written.
//  - maybe_record_large_partition() at the end of each partition.
//  - At the end of the stream, take_stats() and take_records(), once each.
//    The tracker must not be used after that.
class large_data_tracker {
    sstable& _sst;
    const schema& _schema;
    // The size of each top-N heap.
    const uint32_t _max_records;

    large_data_stats_entry _partition_size_entry;
    large_data_stats_entry _rows_in_partition_entry;
    large_data_stats_entry _row_size_entry;
    large_data_stats_entry _cell_size_entry;
    large_data_stats_entry _elements_in_collection_entry;

    // Bounded min-heaps for top-N large data records, one per large_data_type.
    // Size-type heaps (partition_size, row_size, cell_size) compare by `value`;
    // element-count-type heaps (rows_in_partition, elements_in_collection)
    // compare by `elements_count`.
    struct large_data_record_cmp_by_value {
        bool operator()(const large_data_record& a, const large_data_record& b) const {
            return a.value > b.value; // min-heap: smallest value on top
        }
    };
    struct large_data_record_cmp_by_elements {
        bool operator()(const large_data_record& a, const large_data_record& b) const {
            return a.elements_count > b.elements_count; // min-heap: smallest elements_count on top
        }
    };
    using ld_size_heap = std::priority_queue<large_data_record, std::vector<large_data_record>, large_data_record_cmp_by_value>;
    using ld_elements_heap = std::priority_queue<large_data_record, std::vector<large_data_record>, large_data_record_cmp_by_elements>;
    ld_size_heap _ld_partition_size_records;
    ld_elements_heap _ld_rows_in_partition_records;
    ld_size_heap _ld_row_size_records;
    ld_size_heap _ld_cell_size_records;
    ld_elements_heap _ld_elements_in_collection_records;

    // Insert a record into a bounded min-heap, keeping at most N entries.
    // Uses the heap's own comparator to decide eviction: since the comparator
    // defines a min-heap (smallest on top), comp(rec, top) is true when rec
    // is "greater" than top in the heap's ordering, meaning it should replace it.
    template <typename Heap>
    void insert_into_ld_heap(Heap& heap, large_data_record rec) {
        if (_max_records == 0) {
            return;
        }
        if (heap.size() < _max_records) {
            heap.push(std::move(rec));
        } else if (typename Heap::value_compare{}(rec, heap.top())) {
            heap.pop();
            heap.push(std::move(rec));
        }
    }
public:
    // Takes the thresholds from the sstable's large data handler.
    // `max_records_per_type` is the number of records of each type that
    // take_records() returns at most (sstable_writer_config::large_data_records_per_sstable).
    large_data_tracker(sstable& sst, const schema& s, uint32_t max_records_per_type);

    // `partition_size` is the size of the whole partition.
    // `rows` counts the rows and the range tombstone markers of the partition,
    // `range_tombstones` and `dead_rows` count those (see column_stats).
    void maybe_record_large_partition(const key& partition_key,
            uint64_t partition_size, uint64_t rows, uint64_t range_tombstones, uint64_t dead_rows);

    // `clustering_key` is null for the static row.
    void maybe_record_large_row(const key& partition_key,
            const clustering_key_prefix* clustering_key, uint64_t row_size);

    // `clustering_key` is null for a static cell.
    // `collection_elements` is the number of elements of a collection, and 0 for an atomic cell.
    void maybe_record_large_cell(const key& partition_key,
            const clustering_key_prefix* clustering_key, const column_definition& cdef,
            uint64_t cell_size, uint64_t collection_elements);

    // The statistics of each large data type, for Scylla.db.
    scylla_metadata::large_data_stats take_stats();

    // The top-N records of all types, for Scylla.db.
    // Returns nothing if no record exceeded its threshold.
    std::optional<scylla_metadata::large_data_records> take_records();
};

} // namespace sstables
