/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// This file is part of the disk page cache of sstables on object storage
// (the 'disk_cache': 'pages' keyspace storage option). It defines the
// system_cache.pagecache table, which stores the cache, and implements the
// cache's interface (sstables/object_storage_page_cache.hh) over it.

#pragma once

#include "mutation/timestamp.hh"
#include "schema/schema_fwd.hh"
#include "sstables/object_storage_page_cache.hh"

class mutation;

namespace cql3 {
class query_processor;
}

namespace db {

// The disk page cache of sstables on object storage, which is stored in
// the system_cache.pagecache table - and in the system_cache.pagecache_hits
// table, which records when each cached page was last hit.
class object_storage_page_cache_table : public sstables::object_storage_page_cache {
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

    explicit object_storage_page_cache_table(cql3::query_processor& qp) : _qp(qp) {}

    virtual future<> put_page(sstables::sstable_id sst, sstables::component_type component,
            uint64_t offset, temporary_buffer<char> data) override;
    virtual future<std::vector<page>> get_pages(sstables::sstable_id sst, sstables::component_type component,
            uint64_t from, uint64_t to, size_t max_pages, sstables::populate_disk_cache populate) override;
    virtual future<> evict_sstable(sstables::sstable_id sst) override;
    virtual future<> evict_other_sstables(const std::unordered_set<sstables::sstable_id>& live) override;
    virtual future<> copy_sstable_from_shard(sstables::sstable_id sst, shard_id from) override;
private:
    future<> apply(mutation m);
};

} // namespace db
