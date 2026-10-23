/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// This file is part of the disk page cache of sstables on object storage
// (the 'disk_cache': 'pages' keyspace storage option). It defines the
// cache's statistics, which object_storage_page_cache.hh counts and the
// cache's implementation reports as metrics - in a small header of their
// own, so that replica/database.hh, which reports the per-table statistics,
// doesn't need to include the cache's interface.

#pragma once

#include <cstdint>

namespace sstables {

// The statistics of reads through the cache, of one kind of read.
struct object_storage_page_cache_read_stats {
    // The pages read from the cache.
    uint64_t hits = 0;
    // The pages read from the object store - because they weren't cached,
    // or because they followed a page which wasn't cached, in the same
    // request (see object_storage_page_cache_source.hh).
    uint64_t misses = 0;
};

// The statistics of the cache, which both the sstables code that reads
// through it and the cache's implementation count, and the implementation
// reports as metrics.
struct object_storage_page_cache_stats {
    // Of reads which may populate the cache (see populate_disk_cache).
    object_storage_page_cache_read_stats populating;
    // Of reads which must not populate it: reads with BYPASS CACHE, and
    // background reads.
    object_storage_page_cache_read_stats non_populating;
    // The requests which reads through the cache sent to the object store,
    // and the number of bytes they requested.
    uint64_t object_reads = 0;
    uint64_t object_read_bytes = 0;
    // The pages evicted because the cache was full, and because their
    // sstable was deleted.
    uint64_t capacity_evictions = 0;
    uint64_t sstable_evictions = 0;
    // Failures of the cache, which don't fail any read (they are only
    // logged): failures to read pages, to cache them, to record their hits,
    // and to evict them.
    uint64_t read_errors = 0;
    uint64_t write_errors = 0;
    uint64_t hit_recording_errors = 0;
    uint64_t eviction_errors = 0;
    // Reads of a component of an sstable whose keyspace has the
    // 'disk_cache' option, which don't use the cache: through a duplicate of
    // an index file, which another shard reads when it reshards the sstable
    // (see make_page_cached_file()), and of an sstable which a file I/O
    // extension transformed in a way which can't be read from the middle of
    // a component.
    uint64_t dup_bypasses = 0;
    uint64_t transform_bypasses = 0;
};

// The statistics of the reads of one table through the cache, which its
// table reports as metrics - a subset of the cache's statistics, of the
// reads of that table.
struct object_storage_page_cache_table_stats {
    object_storage_page_cache_read_stats populating;
    object_storage_page_cache_read_stats non_populating;
    uint64_t object_reads = 0;
    uint64_t object_read_bytes = 0;
};

} // namespace sstables
