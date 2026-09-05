/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// The parsed Parquet footer of one pq sstable, retained between reads.
//
// Why this exists: a cold point read on a pq sstable costs 1.11 us per row group (design doc
// 10.21), and the part that scales is fetching and Thrift-walking the footer -- 1.4 kB per row
// group (10.22) -- which is a pure function of an immutable file and therefore need happen only
// once per sstable rather than once per read.
//
// Why it is *evictable* rather than merely bounded: at 1.4 kB per row group a 1 601-group sstable
// costs ~2.3 MB, so a node holding a thousand of them would want ~2.3 GB. That is far too much to
// pin, so the entry registers with the reclaim machinery in sstables_manager that already drops
// bloom filters under pressure, rather than inventing a second policy (10.22).
//
// This header is deliberately free of Parquet types so that sstables.hh -- which owns the entry,
// sizes it and drops it -- does not have to pull in the format layer. The concrete entry lives in
// sstables/parquet/reader.cc, next to the code that parses a footer in the first place.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <seastar/core/shared_ptr.hh>

namespace sstables::parquet {

class cached_footer_base {
public:
    virtual ~cached_footer_base() = default;
    // Called when the entry stops being reachable for new reads, so that whatever it was holding
    // against the shard-wide read-cache budget is released. Not the destructor: a reader mid-read
    // keeps the entry alive past the drop, and the budget should free up at the drop -- the bytes
    // are no longer *reusable* even though they are briefly still allocated.
    virtual void on_dropped() const noexcept {}
    // Bytes retained by this entry. Measured from the capacities of the containers that hold it,
    // not estimated from the on-disk footer length -- see cached_footer::retained_bytes().
    virtual size_t memory_size() const noexcept = 0;
};

// Immutable and shared: a reader that is mid-read keeps its entry alive across an eviction, which
// is what makes eviction transparent rather than a use-after-free.
using cached_footer_ptr = seastar::shared_ptr<const cached_footer_base>;

// Shard-local counters, exported as the `sstables_pq_footer_cache_*` metrics.
struct footer_cache_stats {
    uint64_t hits = 0;
    uint64_t misses = 0;
    uint64_t populations = 0;
    uint64_t evictions = 0;      // dropped by the reclaimer, i.e. under memory pressure
    uint64_t bytes = 0;          // currently retained across this shard's sstables
};

inline footer_cache_stats& footer_cache_stats_local() noexcept {
    static thread_local footer_cache_stats s;
    return s;
}

inline void note_footer_cache_populated(size_t bytes) noexcept {
    auto& s = footer_cache_stats_local();
    ++s.populations;
    s.bytes += bytes;
}

// Bytes added to an entry that is already published. Deliberately not note_footer_cache_populated:
// that one also counts a population, and a page index filled in later is not a second footer.
inline void note_footer_cache_grew(size_t bytes) noexcept {
    footer_cache_stats_local().bytes += bytes;
}

// `evicted` distinguishes a drop under memory pressure from a drop because the sstable is going
// away; only the former is something an operator wants to see in the eviction counter.
inline void note_footer_cache_dropped(size_t bytes, bool evicted) noexcept {
    auto& s = footer_cache_stats_local();
    if (evicted) {
        ++s.evictions;
    }
    s.bytes -= std::min(s.bytes, uint64_t(bytes));
}

// The page-index half of the same entry, counted separately because it is filled in lazily per
// row group after the entry is published. Exported as `sstables_pq_offset_index_cache_*`.
//
// It has its own counters rather than sharing the footer's because the hit rates answer different
// questions: the footer is fetched once per sstable per read, the page index once per sstable per
// *row group* touched, so a workload can hit on one and miss on the other all day.
struct offset_index_cache_stats {
    uint64_t hits = 0;
    uint64_t misses = 0;
    uint64_t populations = 0;
};

inline offset_index_cache_stats& offset_index_cache_stats_local() noexcept {
    static thread_local offset_index_cache_stats s;
    return s;
}

// Bytes the read caches hold across *this shard*, not one sstable.
//
// The per-sstable cap these replaced was sized against a benchmark's working set and was wrong for
// a node: 32 MB each for pages and extents is unremarkable for one file and is 64 MB x N sstables
// for a node holding thousands. Reclaim would eventually take it back, but "eventually, under
// pressure, by dropping whole footer entries" is not a memory budget.
//
// Tracked per kind so the metrics can say which half is holding what, and spent from one shared
// cap so the two cannot each claim the whole budget.
struct read_cache_bytes {
    uint64_t pages = 0;
    uint64_t extents = 0;
    uint64_t total() const noexcept { return pages + extents; }
};

inline read_cache_bytes& read_cache_bytes_local() noexcept {
    static thread_local read_cache_bytes b;
    return b;
}

// Decompressed data pages. Separate counters again, for the same reason the page index has its
// own: a workload can hit on the index and miss on the pages, and the two say different things
// about what to change.
struct page_cache_stats {
    uint64_t hits = 0;
    uint64_t misses = 0;
    uint64_t populations = 0;
};

inline page_cache_stats& page_cache_stats_local() noexcept {
    static thread_local page_cache_stats s;
    return s;
}

// Compressed extents, i.e. the reads a paged point read would otherwise issue every time.
struct extent_cache_stats {
    uint64_t hits = 0;
    uint64_t misses = 0;
    uint64_t populations = 0;
};

inline extent_cache_stats& extent_cache_stats_local() noexcept {
    static thread_local extent_cache_stats s;
    return s;
}

// How often a query's projection could actually be applied. Exported so an operator can see the
// answer for their own data: a table written by INSERT projects, one written by UPDATE does not,
// and the difference is not visible from the query.
struct projection_stats {
    uint64_t groups_projected = 0;
    uint64_t groups_declined = 0;
};

inline projection_stats& projection_stats_local() noexcept {
    static thread_local projection_stats s;
    return s;
}

} // namespace sstables::parquet
