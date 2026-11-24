/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// This file is part of the object storage page cache (the 'disk_cache': 'pages'
// keyspace storage option). It defines the interface through which the sstables
// code uses the cache, which is implemented in
// db/object_storage_page_cache_table.hh. How the cache splits sstable
// components into pages is in object_storage_page_cache_geometry.hh.

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <unordered_set>
#include <vector>
#include <seastar/core/future.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/temporary_buffer.hh>
#include "sstables/component_type.hh"
#include "sstables/object_storage_page_cache_stats.hh"
#include "sstables/types.hh"
#include "sstables/types_fwd.hh"
#include "db_clock.hh"
#include "seastarx.hh"

namespace sstables {

// An abstract interface to the object storage page cache: byte ranges ("pages")
// of sstable components, which were read from the object store and are kept on
// the node's disk, so that reading them again does not need another request to
// the object store.
//
// The only implementation is db::object_storage_page_cache_table (in
// db/object_storage_page_cache_table.hh), which stores the pages in the
// system_cache.pagecache table. The sstables code uses only this interface,
// so it does not depend on the code that reads and writes tables, and can
// be tested with a fake cache.
//
// Each shard has its own cache, of the sstables that it owns, and all the
// methods work on the cache of the calling shard.
//
// All offsets are positions in the component's file as it is stored on the
// object store. In particular, a compressed Data component is cached in
// compressed chunks, and the offset of a page is the position of its chunk
// in the compressed file - never a position in the uncompressed data.
class object_storage_page_cache {
public:
    struct page {
        uint64_t offset;
        temporary_buffer<char> data;
    };

    // A cached page's offset and size, and when it was last hit (nothing if
    // it was never hit since it was cached). A size of 0 means that the page
    // isn't cached after all: its hit was recorded after it was evicted.
    struct page_hits {
        uint64_t offset;
        uint64_t size;
        std::optional<db_clock::time_point> last_hit;
    };

    virtual ~object_storage_page_cache() = default;

    // Caches a page of the sstable's component, which starts at the given
    // offset. A page already cached at the same offset is replaced. Caching
    // a page doesn't count as a hit of it (see read_pages()).
    virtual future<> put_page(sstable_id sst, component_type component,
            uint64_t offset, temporary_buffer<char> data) = 0;

    // Like put_page(), but for a page which compaction wrote with the data
    // of pages that were cached (see object_storage_page_cache_compaction.hh):
    // the page gets the last hit of those pages, so it is evicted when they
    // would have been.
    virtual future<> put_copied_page(sstable_id sst, component_type component,
            uint64_t offset, temporary_buffer<char> data, std::optional<db_clock::time_point> last_hit) = 0;

    // Returns which pages of the sstable's component are cached, and when
    // they were last hit: those whose start offset is at least "from", in
    // offset order - but at most max_pages of them, so fewer than max_pages
    // means that there are no more. Unlike read_pages(), it doesn't read the
    // pages themselves, and never records hits.
    virtual future<std::vector<page_hits>> get_page_hits(sstable_id sst, component_type component,
            uint64_t from, size_t max_pages) = 0;

    // A read of the cached pages of a range of an sstable component, which
    // returns the pages one by one, at increasing offsets (see read_pages()).
    // It keeps its position in the cache between pages, so reading a long
    // range of cached pages looks it up in the cache only once - and a read
    // which saves its sstable reader between pages of a query (in the
    // querier cache) keeps its cursor with it.
    class page_cursor {
    public:
        virtual ~page_cursor() = default;
        // Returns the cached page which starts at the given offset, or an
        // empty buffer if no page starts there. Each call must be for a
        // larger offset than the previous one. With populate_disk_cache::yes
        // (see read_pages()), it records that the page was hit.
        virtual future<temporary_buffer<char>> get_page(uint64_t offset) = 0;
        // Must be called, and waited for, before the cursor is destroyed.
        virtual future<> close() noexcept = 0;
    };

    // Returns a cursor over the cached pages of the sstable's component whose
    // start offset is in [from, to).
    //
    // "from" and "to" may be any offsets, but only the start offset of each
    // page is compared to them: a page that starts before "to" is returned
    // whole even if it extends beyond "to", while a page that starts before
    // "from" is not returned even if it extends into the range. So "from" is
    // normally the start offset of the first page the caller needs, and "to"
    // any offset after the start offset of the last page it needs.
    //
    // With populate_disk_cache::yes - for a user's read, which may populate
    // the cache - the cursor records that the pages it returns were hit, for
    // choosing which pages to evict when the cache is full. A read which
    // must not populate the cache, such as a scan with BYPASS CACHE, reads
    // whole sstables, and its hits would only make all of their pages look
    // recently used (see populate_disk_cache).
    //
    // The cursor doesn't see pages which are cached after it starts reading
    // the cache, so they look missing to it; and it may return a page which
    // was evicted after it started. Both are harmless: the object store has
    // all the pages, and a page of an sstable never changes.
    virtual std::unique_ptr<page_cursor> read_pages(sstable_id sst, component_type component,
            uint64_t from, uint64_t to, populate_disk_cache populate) = 0;

    // Drops all the cached pages of the sstable, of all its components.
    virtual future<> evict_sstable(sstable_id sst) = 0;

    // Drops all the cached pages of all the sstables except those in "live" -
    // the pages left in the cache by sstables which no longer exist on this
    // shard (see sstables_manager::evict_orphans_from_page_cache()).
    virtual future<> evict_other_sstables(const std::unordered_set<sstable_id>& live) = 0;

    // Copies to this shard's cache the pages which another shard cached for
    // the sstable - which moved from that shard to this one, keeping its
    // sstable_id (see sstables_manager::copy_cached_pages()).
    virtual future<> copy_sstable_from_shard(sstable_id sst, shard_id from) = 0;

    // Whether the pages of the sstables which memtable flushes write are
    // cached as they are written (see make_page_cache_flush_output()).
    virtual bool caches_flushes() const noexcept = 0;
    // The last hit with which the pages which a memtable flush writes are
    // cached, which places them in the order in which pages are evicted -
    // or nothing, to cache them as never hit, which are evicted first.
    virtual std::optional<db_clock::time_point> flushed_page_last_hit() const = 0;

    // The cache's statistics (see object_storage_page_cache_stats.hh).
    object_storage_page_cache_stats& get_stats() noexcept {
        return _stats;
    }
protected:
    object_storage_page_cache_stats _stats;
};

} // namespace sstables
