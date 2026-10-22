/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// This file is part of the disk page cache of sstables on object storage
// (the 'disk_cache': 'pages' keyspace storage option): the schema of the
// system_cache.pagecache table, which stores the cache, and the
// implementation of the cache's interface over it. See
// db/object_storage_page_cache_table.hh.

#include <seastar/core/coroutine.hh>
#include <seastar/core/deleter.hh>
#include <seastar/core/smp.hh>
#include "db/object_storage_page_cache_table.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "dht/fixed_shard.hh"
#include "mutation/frozen_mutation.hh"
#include "mutation/mutation.hh"
#include "partition_slice_builder.hh"
#include "query/query-result-set.hh"
#include "replica/database.hh"
#include "schema/caching_options.hh"
#include "schema/schema_builder.hh"
#include "service/storage_proxy.hh"
#include "sstables/compressor.hh"
#include "tombstone_gc_options.hh"
#include "types/types.hh"

namespace db {

// The disk page cache of sstables on object storage: byte ranges ("pages")
// of sstable components, which were read from the object store.
//
// The table is in the system_cache keyspace, not in the system keyspace:
// the cache may fill most of the node's disk, while the system keyspace
// holds small metadata tables. Moreover, the system_cache keyspace has
// durable_writes = false, so writes to this table skip the commitlog. A
// crash may then lose the most recently cached pages, which is fine: they
// are only copies of data that the object store holds.
schema_ptr object_storage_page_cache_table::schema() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(SYSTEM_CACHE, PAGECACHE);
        return schema_builder(this_smp_shard_count(), SYSTEM_CACHE, PAGECACHE, id)
            // The shard that owns the sstable. The fixed_shard_partitioner
            // (below) stores the partition on that shard, so a shard reads
            // and writes the cache of its sstables without crossing shards.
            .with_column("shard", short_type, column_kind::partition_key)
            .with_column("sstable", uuid_type, column_kind::partition_key)
            // Every lookup specifies the component, but it is a clustering
            // column, so all the pages of an sstable are in one partition,
            // and dropping the cache of an sstable takes a single partition
            // tombstone.
            .with_column("component", utf8_type, column_kind::clustering_key)
            // The pages of each component are sorted by offset, so reading a
            // range of consecutive pages is a sequential scan.
            .with_column("offset", long_type, column_kind::clustering_key)
            .with_column("page", bytes_type)
            .set_comment("Disk page cache of sstables on object storage")
            .with_hash_version()
            .with_partitioner(dht::fixed_shard_partitioner::classname)
            .with_sharder(dht::fixed_shard_sharder::instance())
            // A page read from this table is cached in memory anyway, by the
            // user table's caches (its row cache for Data pages, its index
            // cache for index pages), so the row cache would only hold a
            // second copy. Unlike BYPASS CACHE, disabling the row cache in the
            // schema keeps this table's own index cached.
            .set_caching_options(caching_options::get_disabled_caching_options())
            // Most pages are already compressed - the compressed chunks of
            // a Data component - or encrypted, so compressing them again
            // would cost CPU on every write, read and compaction, and save
            // almost nothing.
            .set_compressor_params(compression_parameters::no_compression())
            // The table is local, so it has no other replicas which a
            // tombstone needs to reach before it may be dropped.
            .with_tombstone_gc_options(tombstone_gc_options(tombstone_gc_mode::immediate))
            .build();
    }();
    return schema;
}

// The cache of each shard is in the partitions whose "shard" column is
// that shard, which the fixed_shard_partitioner stores on that shard. So
// the queries below, which always name the calling shard, never leave it.
// The shard column is a smallint, which the fixed_shard_partitioner
// requires, so the shard number is passed as an int16_t.
static int16_t this_shard() {
    return int16_t(this_shard_id());
}

// The key of the table: ((shard, sstable), component, offset).
static partition_key cache_partition_key(const schema& s, int16_t shard, utils::UUID sstable) {
    return partition_key::from_exploded(s, {data_value(shard).serialize_nonnull(), data_value(sstable).serialize_nonnull()});
}

static clustering_key cache_clustering_key(const schema& s, const sstring& component, int64_t offset) {
    return clustering_key::from_exploded(s, {data_value(component).serialize_nonnull(), data_value(offset).serialize_nonnull()});
}

api::timestamp_type object_storage_page_cache_table::next_timestamp() noexcept {
    static thread_local api::timestamp_type last = api::min_timestamp;
    last = std::max(api::new_timestamp(), last + 1);
    return last;
}

// A mutation which writes a cached page, like an INSERT of it.
static mutation page_mutation(int16_t shard, utils::UUID sstable, const sstring& component, int64_t offset, bytes page) {
    auto s = object_storage_page_cache_table::schema();
    auto ts = object_storage_page_cache_table::next_timestamp();
    auto ck = cache_clustering_key(*s, component, offset);
    mutation m(s, cache_partition_key(*s, shard, sstable));
    m.partition().clustered_row(*s, ck).apply(row_marker(ts));
    m.set_clustered_cell(ck, "page", data_value(std::move(page)), ts);
    return m;
}

// A mutation which deletes all the rows of an sstable in the given table.
static mutation sstable_deletion(schema_ptr s, int16_t shard, utils::UUID sstable) {
    mutation m(s, cache_partition_key(*s, shard, sstable));
    m.partition().apply(tombstone(object_storage_page_cache_table::next_timestamp(), gc_clock::now()));
    return m;
}

// The cache writes its table by applying mutations to it directly, not
// with internal CQL statements: a statement costs much more than the write
// itself - its execution, and the coordinator's write path - and the cache
// writes a page for each page which a read misses. The mutations are of
// this shard's partitions, so they are applied on this shard.
future<> object_storage_page_cache_table::apply(mutation m) {
    auto s = m.schema();
    co_await _qp.proxy().local_db().apply(std::move(s), freeze(m), {}, db::commitlog_force_sync::no, db::no_timeout);
}

future<> object_storage_page_cache_table::put_page(sstables::sstable_id sst, sstables::component_type component,
        uint64_t offset, temporary_buffer<char> data) {
    co_await apply(page_mutation(this_shard(), sst.uuid(), fmt::to_string(component), int64_t(offset),
            bytes(reinterpret_cast<const int8_t*>(data.get()), data.size())));
}

future<std::vector<sstables::object_storage_page_cache::page>>
object_storage_page_cache_table::get_pages(sstables::sstable_id sst, sstables::component_type component,
        uint64_t from, uint64_t to, size_t max_pages) {
    // The pages are read from the table directly, not with an internal CQL
    // query. The sstable read which looks them up holds a permit of the
    // reader concurrency semaphore of its scheduling group, and a CQL query
    // would wait for admission on the same semaphore, behind other reads -
    // which also hold their permits while waiting for their own lookups. So
    // with more concurrent reads than the semaphore admits, all of them
    // would wait until they time out. Instead, the lookup reads with a
    // tracking-only permit, which isn't admitted: it is part of a read which
    // was already admitted, and reads at most max_pages pages.
    //
    // But skipping admission isn't enough: while a read runs, it requests
    // memory from its permit's semaphore (through tracked files), which,
    // when its reads use too much memory, lets only one of them proceed,
    // and makes the others wait. If the read which proceeds were the one
    // waiting for this lookup, a permit of the same semaphore would wait
    // behind it, and so would all the reads. So the permit is of a semaphore
    // without limits, which never makes a read wait - for admission, or for
    // memory (database::object_storage_page_cache_semaphore()). The lookup's
    // memory is bounded anyway, by max_pages.
    auto& db = _qp.proxy().local_db();
    auto& table = db.find_column_family(SYSTEM_CACHE, PAGECACHE);
    auto s = table.schema();
    auto key = dht::decorate_key(*s, cache_partition_key(*s, this_shard(), sst.uuid()));
    auto component_name = fmt::to_string(component);
    auto cmd = query::read_command(s->id(), s->version(),
            partition_slice_builder(*s).with_range(query::clustering_range::make(
                    {cache_clustering_key(*s, component_name, int64_t(from)), true},
                    {cache_clustering_key(*s, component_name, int64_t(to)), false})).build(),
            query::max_result_size(query::result_memory_limiter::unlimited_result_size), query::tombstone_limit::max,
            query::row_limit(max_pages));
    auto ranges = dht::partition_range_vector{dht::partition_range::make_singular(key)};
    auto permit = db.object_storage_page_cache_semaphore().make_tracking_only_permit(s, "object_storage_page_cache_get_pages", db::no_timeout, {});
    auto result = co_await table.query(s, std::move(permit), cmd, query::result_options::only_result(), ranges, {},
            db.get_result_memory_limiter(), db::no_timeout);
    auto rs = query::result_set::from_raw_result(s, cmd.slice, *result);
    std::vector<page> pages;
    pages.reserve(rs.rows().size());
    for (const auto& row : rs.rows()) {
        // The page's buffer points into the blob, which its deleter keeps
        // alive. The blob is on the heap, because moving a short bytes object
        // would move its contents too (they are stored inline).
        auto blob = std::make_unique<bytes>(row.get_nonnull<bytes>("page"));
        auto data = reinterpret_cast<char*>(blob->data());
        auto size = blob->size();
        pages.push_back(page{
            .offset = uint64_t(row.get_nonnull<int64_t>("offset")),
            .data = temporary_buffer<char>(data, size, make_object_deleter(std::move(blob))),
        });
    }
    co_return pages;
}

future<> object_storage_page_cache_table::evict_sstable(sstables::sstable_id sst) {
    co_await apply(sstable_deletion(schema(), this_shard(), sst.uuid()));
}

} // namespace db
