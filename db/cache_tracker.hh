/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "dht/token.hh"
#include "utils/lru.hh"
#include "utils/logalloc.hh"
#include "utils/updateable_value.hh"
#include "mutation/partition_version.hh"
#include "mutation/mutation_cleaner.hh"
#include "utils/cached_file_stats.hh"
#include "sstables/partition_index_cache_stats.hh"

#include <seastar/core/metrics_registration.hh>

#include <stdint.h>

class cache_entry;

namespace cache {

class autoupdating_underlying_reader;
class cache_streamed_mutation;
class cache_mutation_reader;
class read_context;
class lsa_manager;

}

// Tracks accesses and performs eviction of cache entries.
class cache_tracker final {
public:
    friend class row_cache;
    friend class cache::read_context;
    friend class cache::autoupdating_underlying_reader;
    friend class cache::cache_mutation_reader;
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
        uint64_t dummy_processed_from_memtable;
        uint64_t rows_covered_by_range_tombstones_from_memtable;
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
        uint64_t row_tombstone_reads;
        uint64_t rows_compacted;
        uint64_t rows_compacted_away;

        uint64_t active_reads() const {
            return reads - reads_done;
        }
    };
private:
    stats _stats{};
    cached_file_stats _index_cached_file_stats{};
    partition_index_cache_stats _partition_index_cache_stats{};
    seastar::metrics::metric_groups _metrics;
    logalloc::region _region;
    lru _lru;
    mutation_cleaner _garbage;
    mutation_cleaner _memtable_cleaner;
    mutation_application_stats& _app_stats;
    utils::updateable_value<double> _index_cache_fraction;
    utils::updateable_value<double> _tinylfu_sketch_entries_per_mb;
    utils::updateable_value<double> _tinylfu_initial_window_fraction;
    utils::observer<double> _sketch_ratio_observer;
    utils::observer<double> _window_fraction_observer;
private:
    void setup_metrics();
public:
    using register_metrics = bool_class<class register_metrics_tag>;
    cache_tracker(utils::updateable_value<double> index_cache_fraction, mutation_application_stats&, register_metrics);
    cache_tracker(utils::updateable_value<double> index_cache_fraction, register_metrics);
    cache_tracker(utils::updateable_value<double> index_cache_fraction,
                  utils::updateable_value<double> tinylfu_sketch_entries_per_mb,
                  utils::updateable_value<double> tinylfu_initial_window_fraction,
                  mutation_application_stats&, register_metrics);
    cache_tracker(utils::updateable_value<double> index_cache_fraction,
                  utils::updateable_value<double> tinylfu_sketch_entries_per_mb,
                  utils::updateable_value<double> tinylfu_initial_window_fraction,
                  register_metrics);
    cache_tracker();
    ~cache_tracker();
    void clear();
    void touch(rows_entry&);
    void insert(cache_entry&);
    void insert(partition_entry&) noexcept;
    void insert(partition_version&) noexcept;
    void insert(mutation_partition_v2&) noexcept;
    void insert(rows_entry&) noexcept;
    void remove(rows_entry&) noexcept;
    // Inserts e such that it will be evicted right before more_recent in the absence of later touches.
    void insert(rows_entry& more_recent, rows_entry& e) noexcept;
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
    void on_row_tombstone_read() noexcept { ++_stats.row_tombstone_reads; }
    void on_row_compacted() noexcept { ++_stats.rows_compacted; }
    void on_row_compacted_away() noexcept { ++_stats.rows_compacted_away; }
    void pinned_dirty_memory_overload(uint64_t bytes) noexcept;
    allocation_strategy& allocator() noexcept;
    logalloc::region& region() noexcept;
    const logalloc::region& region() const noexcept;
    mutation_cleaner& cleaner() noexcept { return _garbage; }
    mutation_cleaner& memtable_cleaner() noexcept { return _memtable_cleaner; }
    uint64_t partitions() const noexcept { return _stats.partitions; }
    const stats& get_stats() const noexcept { return _stats; }
    stats& get_stats() noexcept { return _stats; }
    void set_compaction_scheduling_group(seastar::scheduling_group);
    lru& get_lru() { return _lru; }
    cached_file_stats& get_index_cached_file_stats() { return _index_cached_file_stats; }
    partition_index_cache_stats& get_partition_index_cache_stats() { return _partition_index_cache_stats; }
    seastar::memory::reclaiming_result evict_from_lru_shallow() noexcept;

    /// Resize the W-TinyLFU Count-Min Sketch based on the current cache region
    /// size and the configured tinylfu_sketch_entries_per_mb ratio.
    /// Safe to call at any time; called automatically when the config changes.
    void resize_sketch();

    /// Fully reset the W-TinyLFU sketch (zero all counters).
    /// Called by clear() to ensure a clean slate after full cache eviction.
    void reset_sketch() noexcept;

    /// Ensure the thread-local current_tracker pointer is set to this tracker.
    /// Must be called before lru::add() since add-time window draining may
    /// trigger on_evicted_shallow() which dereferences current_tracker.
    void set_current_tracker() noexcept;
};

// Compute a stable sketch key from a partition token.
// All rows in the same partition share this key — partition-level
// frequency tracking is sufficient because the SLRU handles
// intra-partition row-level eviction ordering.
//
// Design note: we use token-level (not row-level) frequency because
// (a) it avoids the frequency sketch mis-rejecting individual rows from
//     multi-row partitions (clustering key tables), and
// (b) within a partition, the SLRU recency ordering handles eviction.
// Trade-off: we don't get scan protection *within* large partitions.
// Partition-level sketch key — used for dummy/sentinel entries and
// tables without clustering keys.
inline uint64_t compute_sketch_key(const dht::token& tok) noexcept {
    // The token is already a hash (murmur3); use it directly.
    // No sentinel guard needed — evictable::has_sketch_key() distinguishes
    // "key set to 0" from "key not set" via a separate flag bit.
    return static_cast<uint64_t>(tok.raw());
}

// Row-level sketch key — mixes partition token with clustering key so each
// row tracks its own frequency independently.  This prevents cold rows in
// hot partitions from being over-protected by the partition's accumulated
// frequency (the "parasitic row" problem).
inline uint64_t compute_sketch_key(const dht::token& tok,
                                    const clustering_key& ck) {
    uint64_t h = static_cast<uint64_t>(tok.raw());
    ck.representation().with_linearized([&] (bytes_view bv) {
        for (auto byte : bv) {
            h ^= static_cast<uint64_t>(byte);
            h *= 0x9e3779b97f4a7c15ULL;
        }
    });
    return h;
}

inline
void cache_tracker::remove(rows_entry& entry) noexcept {
    --_stats.rows;
    ++_stats.row_removals;
    if (entry.is_linked()) {
        _lru.remove(entry);
    }
}

inline
void cache_tracker::insert(rows_entry& entry) noexcept {
    ++_stats.row_insertions;
    ++_stats.rows;
    set_current_tracker();
    _lru.add(entry);
}

inline
void cache_tracker::insert(rows_entry& more_recent, rows_entry& entry) noexcept {
    ++_stats.row_insertions;
    ++_stats.rows;
    entry.set_sketch_key(more_recent.sketch_key());
    _lru.add_before(more_recent, entry);
}

inline
void cache_tracker::insert(partition_version& pv) noexcept {
    insert(pv.partition());
}

inline
void cache_tracker::insert(mutation_partition_v2& p) noexcept {
    for (rows_entry& row : p.clustered_rows()) {
        insert(row);
    }
}

inline
void cache_tracker::insert(partition_entry& pe) noexcept {
    for (partition_version& pv : pe.versions_from_oldest()) {
        insert(pv);
    }
}
