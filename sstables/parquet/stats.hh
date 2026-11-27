/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Counters of the Parquet code on a shard, which the metrics of the
// `sstables` group expose (init_metrics() in sstables/sstables.cc).
//
// They cost nothing per row. The readers add their fetch_stats when they
// close, and the writers add their writer_stats when they finish a file.
// The metadata cache counts its lookups, which happen once per page.

#include <cstdint>
#include <utility>

namespace sstables::parquet {

struct shard_stats {
    // Reads, from the fetch_stats of the readers that closed.
    uint64_t read_requests = 0;
    uint64_t bytes_read = 0;
    uint64_t data_pages_read = 0;
    uint64_t dictionary_pages_read = 0;
    uint64_t offset_index_reads = 0;

    // The caches of the OffsetIndexes and the dictionaries.
    uint64_t offset_index_cache_hits = 0;
    uint64_t offset_index_cache_misses = 0;
    uint64_t dictionary_cache_hits = 0;
    uint64_t dictionary_cache_misses = 0;
    // The memory that the caches hold in the LSA region of the cache_tracker.
    uint64_t metadata_cache_bytes = 0;
    // Entries that memory reclaim evicted from the caches.
    uint64_t metadata_cache_evictions = 0;

    // The memory of the footer summaries of the open sstables that this
    // shard owns (shareable_components::parquet_summary).
    uint64_t footer_summary_bytes = 0;

    // Writes, from the writer_stats of the files that were finished.
    uint64_t data_pages_written = 0;
    uint64_t dictionary_pages_written = 0;
    uint64_t dictionary_fallbacks = 0;
    uint64_t row_groups_written = 0;
    uint64_t late_chunks_written = 0;
};

inline thread_local shard_stats local_shard_stats;

/// Counts `bytes` in `gauge`, a member of local_shard_stats, for its lifetime.
/// It must be destroyed on the shard that created it.
class accounted_memory {
    uint64_t shard_stats::* _gauge = nullptr;
    uint64_t _bytes = 0;
public:
    accounted_memory() noexcept = default;
    accounted_memory(uint64_t shard_stats::* gauge, uint64_t bytes) noexcept : _gauge(gauge), _bytes(bytes) {
        local_shard_stats.*_gauge += _bytes;
    }
    accounted_memory(accounted_memory&& o) noexcept : _gauge(std::exchange(o._gauge, nullptr)), _bytes(o._bytes) {}
    accounted_memory& operator=(accounted_memory&& o) noexcept {
        if (this != &o) {
            release();
            _gauge = std::exchange(o._gauge, nullptr);
            _bytes = o._bytes;
        }
        return *this;
    }
    ~accounted_memory() {
        release();
    }
    void release() noexcept {
        if (_gauge) {
            local_shard_stats.*_gauge -= _bytes;
            _gauge = nullptr;
        }
    }
};

} // namespace sstables::parquet
