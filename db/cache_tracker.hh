/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "utils/lru.hh"
#include "utils/logalloc.hh"
#include "partition_version.hh"
#include "mutation_cleaner.hh"

#include <seastar/core/metrics_registration.hh>

#include <stdint.h>

class cache_entry;

namespace cache {

class autoupdating_underlying_reader;
class cache_streamed_mutation;
class cache_flat_mutation_reader;
class read_context;
class lsa_manager;

}

// Tracks accesses and performs eviction of cache entries.
class cache_tracker final {
public:
    friend class row_cache;
    friend class cache::read_context;
    friend class cache::autoupdating_underlying_reader;
    friend class cache::cache_flat_mutation_reader;
    struct stats {
        uint64_t partition_hits;
        uint64_t partition_misses;
        uint64_t row_hits;
        uint64_t dummy_row_hits;
        uint64_t row_misses;
        uint64_t partition_insertions;
        uint64_t row_insertions;
        uint64_t static_row_insertions;
        uint64_t concurrent_misses_same_key;
        uint64_t partition_merges;
        uint64_t rows_processed_from_memtable;
        uint64_t rows_dropped_from_memtable;
        uint64_t rows_merged_from_memtable;
        uint64_t partition_evictions;
        uint64_t partition_removals;
        uint64_t row_evictions;
        uint64_t row_removals;
        uint64_t partitions;
        uint64_t rows;
        uint64_t mispopulations;
        uint64_t underlying_recreations;
        uint64_t underlying_partition_skips;
        uint64_t underlying_row_skips;
        uint64_t reads;
        uint64_t reads_with_misses;
        uint64_t reads_done;
        uint64_t pinned_dirty_memory_overload;
        uint64_t range_tombstone_reads;

        uint64_t active_reads() const {
            return reads - reads_done;
        }
    };
private:
    stats _stats{};
    seastar::metrics::metric_groups _metrics;
    logalloc::region _region;
    lru _lru;
    mutation_cleaner _garbage;
    mutation_cleaner _memtable_cleaner;
private:
    void setup_metrics();
public:
    using register_metrics = bool_class<class register_metrics_tag>;
    cache_tracker(mutation_application_stats&, register_metrics);
    cache_tracker(register_metrics = register_metrics::no);
    ~cache_tracker();
    void clear();
    void touch(rows_entry&);
    void insert(cache_entry&);
    void insert(partition_entry&) noexcept;
    void insert(partition_version&) noexcept;
    void insert(rows_entry&) noexcept;
    void on_remove(rows_entry&) noexcept;
    void clear_continuity(cache_entry& ce) noexcept;
    void on_partition_erase() noexcept;
    void on_partition_merge() noexcept;
    void on_partition_hit() noexcept;
    void on_partition_miss() noexcept;
    void on_partition_eviction() noexcept;
    void on_row_eviction() noexcept;
    void on_row_hit() noexcept;
    void on_dummy_row_hit() noexcept;
    void on_row_miss() noexcept;
    void on_miss_already_populated() noexcept;
    void on_mispopulate() noexcept;
    void on_row_processed_from_memtable() noexcept { ++_stats.rows_processed_from_memtable; }
    void on_row_dropped_from_memtable() noexcept { ++_stats.rows_dropped_from_memtable; }
    void on_row_merged_from_memtable() noexcept { ++_stats.rows_merged_from_memtable; }
    void on_range_tombstone_read() noexcept { ++_stats.range_tombstone_reads; }
    void pinned_dirty_memory_overload(uint64_t bytes) noexcept;
    allocation_strategy& allocator() noexcept;
    logalloc::region& region() noexcept;
    const logalloc::region& region() const noexcept;
    mutation_cleaner& cleaner() noexcept { return _garbage; }
    mutation_cleaner& memtable_cleaner() noexcept { return _memtable_cleaner; }
    uint64_t partitions() const noexcept { return _stats.partitions; }
    const stats& get_stats() const noexcept { return _stats; }
    void set_compaction_scheduling_group(seastar::scheduling_group);
    lru& get_lru() { return _lru; }
};

inline
void cache_tracker::on_remove(rows_entry& row) noexcept {
    --_stats.rows;
    ++_stats.row_removals;
}

inline
void cache_tracker::insert(rows_entry& entry) noexcept {
    ++_stats.row_insertions;
    ++_stats.rows;
    _lru.add(entry);
}

inline
void cache_tracker::insert(partition_version& pv) noexcept {
    for (rows_entry& row : pv.partition().clustered_rows()) {
        insert(row);
    }
}

inline
void cache_tracker::insert(partition_entry& pe) noexcept {
    for (partition_version& pv : pe.versions_from_oldest()) {
        insert(pv);
    }
}
