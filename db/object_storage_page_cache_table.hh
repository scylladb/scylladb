/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// This file is part of the object storage page cache (the 'disk_cache': 'pages'
// keyspace storage option). It defines the system_cache.pagecache table, which
// stores the cache, and implements the cache's interface
// (sstables/object_storage_page_cache.hh) over it.

#pragma once

#include "mutation/timestamp.hh"
#include "schema/schema_fwd.hh"
#include "sstables/object_storage_page_cache.hh"

class mutation;

namespace cql3 {
class query_processor;
}

namespace replica {
class database;
}

namespace db {

// The object storage page cache, which is stored in the system_cache.pagecache
// table - and in the system_cache.pagecache_hits table, which records when each
// cached page was last hit.
class object_storage_page_cache_table : public sstables::object_storage_page_cache {
    // The cursor which read_pages() returns.
    class page_cursor_impl;
    // The database, which holds the cache's tables: reads and writes of
    // pages go to it directly. The query processor executes the cache's
    // internal queries.
    replica::database& _db;
    cql3::query_processor& _qp;
public:
    // A node-local keyspace for caches. Unlike the system keyspace, writes
    // to it skip the commitlog, and it may grow very large.
    static constexpr auto SYSTEM_CACHE = "system_cache";
    static constexpr auto PAGECACHE = "pagecache";
    static constexpr auto PAGECACHE_HITS = "pagecache_hits";

    static schema_ptr schema();
    static schema_ptr hits_schema();
    // The timestamp of a write to the tables. The writes of a shard's cache -
    // of the same rows, which it writes, deletes and writes again - must be
    // ordered by their timestamps, but api::new_timestamp() may return the
    // same timestamp twice (in the same microsecond), or an earlier one (if
    // the clock is set back). So this returns timestamps which increase on
    // each shard - which writes only its own partitions.
    static api::timestamp_type next_timestamp() noexcept;

    object_storage_page_cache_table(replica::database& db, cql3::query_processor& qp) : _db(db), _qp(qp) {}

    virtual future<> put_page(sstables::sstable_id sst, sstables::component_type component,
            uint64_t offset, temporary_buffer<char> data) override;
    virtual std::unique_ptr<page_cursor> read_pages(sstables::sstable_id sst, sstables::component_type component,
            uint64_t from, uint64_t to, sstables::populate_disk_cache populate) override;
    // Returns the cached pages of the sstable's component whose start offset
    // is in [from, to), in offset order, but at most max_pages of them - and
    // records their hits, like a cursor of read_pages() which returned all of
    // them. For tests, which check which pages the cache holds.
    future<std::vector<page>> get_pages(sstables::sstable_id sst, sstables::component_type component,
            uint64_t from, uint64_t to, size_t max_pages, sstables::populate_disk_cache populate);
    virtual future<> evict_sstable(sstables::sstable_id sst) override;
    virtual future<> evict_other_sstables(const std::unordered_set<sstables::sstable_id>& live) override;
    virtual future<> copy_sstable_from_shard(sstables::sstable_id sst, shard_id from) override;
private:
    // Records that the given pages of an sstable's component were hit.
    future<> record_hits(sstables::sstable_id sst, const sstring& component_name, std::vector<int64_t> offsets);
    future<> apply(mutation m);
};

} // namespace db
