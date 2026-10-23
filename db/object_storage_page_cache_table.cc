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

#include <algorithm>
#include <array>
#include <cmath>
#include <set>
#include <seastar/core/coroutine.hh>
#include <seastar/core/deleter.hh>
#include <seastar/core/smp.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/util/defer.hh>
#include "db/object_storage_page_cache_table.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "db_clock.hh"
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
#include "utils/error_injection.hh"
#include "utils/hash.hh"
#include "utils/log.hh"

namespace db {

static logging::logger pclogger("object_storage_page_cache");

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

// When each cached page was last hit - read from the cache by a read which
// may populate it - and its size, for choosing which pages to evict when
// the cache is full. Caching a page writes its row here without last_hit: so every
// cached page has a row here - choosing which pages to evict scans only
// this table - but a page which was read only once, when it was cached,
// has no hit, and so looks older than any page which was read again.
//
// This is a separate table from the pages, although it has the same key,
// because they are written very differently: a page is written once, and
// is large, while its row here is small, and is rewritten on every hit.
// Mixing them would make compaction rewrite the large pages again and
// again, because of the frequently rewritten hits. Also, choosing which
// pages to evict needs to scan only this small table.
schema_ptr object_storage_page_cache_table::hits_schema() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(SYSTEM_CACHE, PAGECACHE_HITS);
        return schema_builder(this_smp_shard_count(), SYSTEM_CACHE, PAGECACHE_HITS, id)
            // The same key as the pagecache table's, partitioned the same way.
            .with_column("shard", short_type, column_kind::partition_key)
            .with_column("sstable", uuid_type, column_kind::partition_key)
            .with_column("component", utf8_type, column_kind::clustering_key)
            .with_column("offset", long_type, column_kind::clustering_key)
            .with_column("last_hit", timestamp_type)
            .with_column("size", int32_type)
            .set_comment("When the pages of the disk page cache of sstables on object storage were last hit")
            .with_hash_version()
            .with_partitioner(dht::fixed_shard_partitioner::classname)
            .with_sharder(dht::fixed_shard_sharder::instance())
            // The table is written much more than it is read, so caching it
            // in memory would be a waste.
            .set_caching_options(caching_options::get_disabled_caching_options())
            // The table is flushed every minute. Without a commitlog, a crash
            // loses what the memtables hold, and when memtables take too much
            // memory, the largest is flushed - almost always pagecache's, so
            // the small rows of this table could stay in memory for long.
            // After a crash, a page whose row here was lost would stay in
            // pagecache - never counted, nor evicted - until its sstable is
            // deleted. Flushing every minute limits this to the pages cached
            // in the last minute before the crash.
            .set_memtable_flush_period(hits_flush_period_ms)
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

// The fixed_shard_partitioner puts the shard in the high bits of the token,
// so the partitions of this shard are a contiguous range of tokens, which a
// query reads without leaving this shard. The last shard also holds the
// partitions of shards which no longer exist, after the node restarted with
// fewer shards: their tokens are after its own.
static std::pair<int64_t, int64_t> this_shard_token_range() {
    auto first = dht::fixed_shard_partitioner::token_for_shard(this_shard_id(), 0);
    auto last = this_shard_id() == this_smp_shard_count() - 1
            ? dht::fixed_shard_partitioner::token_for_shard(dht::fixed_shard_partitioner::max_shard, dht::fixed_shard_partitioner::hash_mask)
            : dht::fixed_shard_partitioner::token_for_shard(this_shard_id(), dht::fixed_shard_partitioner::hash_mask);
    return {first.raw(), last.raw()};
}

// The keys of both tables: ((shard, sstable), component, offset).
static partition_key cache_partition_key(const schema& s, int16_t shard, utils::UUID sstable) {
    return partition_key::from_exploded(s, {data_value(shard).serialize_nonnull(), data_value(sstable).serialize_nonnull()});
}

static clustering_key cache_clustering_key(const schema& s, const sstring& component, int64_t offset) {
    return clustering_key::from_exploded(s, {data_value(component).serialize_nonnull(), data_value(offset).serialize_nonnull()});
}

// A mutation which writes a cached page, like an INSERT of it.
static mutation page_mutation(int16_t shard, utils::UUID sstable, const sstring& component, int64_t offset, bytes page) {
    auto s = object_storage_page_cache_table::schema();
    auto ts = api::new_timestamp();
    auto ck = cache_clustering_key(*s, component, offset);
    mutation m(s, cache_partition_key(*s, shard, sstable));
    m.partition().clustered_row(*s, ck).apply(row_marker(ts));
    m.set_clustered_cell(ck, "page", data_value(std::move(page)), ts);
    return m;
}

// A mutation which writes a page's row in the hits table, like an INSERT
// of the given columns: a column which isn't given keeps its value.
static mutation hits_mutation(int16_t shard, utils::UUID sstable, const sstring& component, int64_t offset,
        std::optional<int32_t> size, std::optional<db_clock::time_point> last_hit) {
    auto s = object_storage_page_cache_table::hits_schema();
    auto ts = api::new_timestamp();
    auto ck = cache_clustering_key(*s, component, offset);
    mutation m(s, cache_partition_key(*s, shard, sstable));
    m.partition().clustered_row(*s, ck).apply(row_marker(ts));
    if (size) {
        m.set_clustered_cell(ck, "size", data_value(*size), ts);
    }
    if (last_hit) {
        m.set_clustered_cell(ck, "last_hit", data_value(*last_hit), ts);
    }
    return m;
}

// A mutation which deletes all the rows of an sstable in the given table.
static mutation sstable_deletion(schema_ptr s, int16_t shard, utils::UUID sstable) {
    mutation m(s, cache_partition_key(*s, shard, sstable));
    m.partition().apply(tombstone(api::new_timestamp(), gc_clock::now()));
    return m;
}

// A mutation which deletes the row of a page in the given table.
static mutation page_deletion(schema_ptr s, int16_t shard, utils::UUID sstable, const sstring& component, int64_t offset) {
    mutation m(s, cache_partition_key(*s, shard, sstable));
    m.partition().apply_delete(*s, cache_clustering_key(*s, component, offset), tombstone(api::new_timestamp(), gc_clock::now()));
    return m;
}

object_storage_page_cache_table::object_storage_page_cache_table(cql3::query_processor& qp, utils::updateable_value<uint64_t> size_in_mb,
        utils::updateable_value<uint64_t> max_remembered_hits)
    : _qp(qp)
    , _size_in_mb(std::move(size_in_mb))
    , _max_remembered_hits(std::move(max_remembered_hits))
{}

size_t object_storage_page_cache_table::page_key_hash::operator()(const page_key& k) const noexcept {
    return utils::hash_combine(std::hash<utils::UUID>()(k.sstable), utils::hash_combine(size_t(k.component), std::hash<uint64_t>()(k.offset)));
}

size_t object_storage_page_cache_table::hand_position_hash::operator()(const hand_position& k) const noexcept {
    return utils::hash_combine(std::hash<utils::UUID>()(k.sstable),
            utils::hash_combine(std::hash<std::string_view>()(k.component), std::hash<int64_t>()(k.offset)));
}

// The cache writes its tables by applying mutations to them directly,
// not with internal CQL statements: a statement adds the cost of its
// execution, and of the coordinator's write path - in a benchmark, writing
// directly cut a write's instructions by a third - and the cache writes
// often: a page for each page which a read misses, a hit for each page
// which is hit (at most once in each pass of the hand), and a deletion for
// each evicted page. The mutations are of this shard's partitions (or of
// shards which no longer exist, which are stored on the last shard - see
// this_shard_token_range()), so they are applied on this shard.
future<> object_storage_page_cache_table::apply(mutation m) {
    auto s = m.schema();
    co_await _qp.proxy().local_db().apply(std::move(s), freeze(m), {}, db::commitlog_force_sync::no, db::no_timeout);
}

// The hand passes the partitions in the order of their tokens (the order of
// the queries in sweep()), and the pages of a partition in the order of
// their clustering keys: by component, as UTF-8 text, and then by offset.
bool object_storage_page_cache_table::at_or_before(const hand_position& pos, int16_t shard, utils::UUID sstable,
        std::string_view component, int64_t offset) {
    auto s = hits_schema();
    auto c = dht::decorate_key(*s, cache_partition_key(*s, shard, sstable)).tri_compare(*s,
            dht::decorate_key(*s, cache_partition_key(*s, pos.shard, pos.sstable)));
    if (c != 0) {
        return c < 0;
    }
    // UTF-8 text is ordered by its bytes, as std::string_view orders it.
    return std::pair(component, offset) <= std::pair(std::string_view(pos.component), pos.offset);
}

std::optional<object_storage_page_cache_table::hand_position> object_storage_page_cache_table::fetched_end() const {
    return _batch.empty() ? _hand : std::optional<hand_position>(_batch.back().key);
}

void object_storage_page_cache_table::count_written_page(int16_t shard, utils::UUID sstable, std::string_view component,
        int64_t offset, int32_t size, std::optional<db_clock::time_point> last_hit) {
    auto end = fetched_end();
    if (!end || !at_or_before(*end, shard, sstable, component, offset)) {
        // The hand will see the page on this pass, or no pass is ongoing.
        return;
    }
    _pass_bytes += size;
    add_to_histogram(_pass_ages, last_hit, size);
    if (!_hand || !at_or_before(*_hand, shard, sstable, component, offset)) {
        // The page is after the hand, within its batch.
        _window_bytes += size;
        add_to_histogram(_window_ages, last_hit, size);
    }
}

void object_storage_page_cache_table::subtract_histogram(age_histogram& from, const age_histogram& h) {
    if (from.counted_at != h.counted_at || from.buckets.size() != h.buckets.size()) {
        return;
    }
    from.never_hit -= std::min(from.never_hit, h.never_hit);
    for (size_t i = 0; i < h.buckets.size(); i++) {
        from.buckets[i] -= std::min(from.buckets[i], h.buckets[i]);
    }
}

void object_storage_page_cache_table::drop_batch() {
    _batch.clear();
    _evicted_in_batch.clear();
    // The pages cached within the batch, after the hand, are fetched again.
    _pass_bytes -= std::min(_pass_bytes, _window_bytes);
    subtract_histogram(_pass_ages, _window_ages);
    _window_bytes = 0;
    _window_ages = age_histogram{.counted_at = _pass_ages.counted_at, .buckets = std::vector<uint64_t>(_pass_ages.buckets.size())};
}

uint64_t object_storage_page_cache_table::limit() const noexcept {
    return _size_in_mb() * 1024 * 1024 / this_smp_shard_count();
}

future<> object_storage_page_cache_table::put_page(sstables::sstable_id sst, sstables::component_type component,
        uint64_t offset, temporary_buffer<char> data) {
    utils::get_local_injector().inject("object_storage_page_cache_put_page", [] {
        throw std::runtime_error("object_storage_page_cache_put_page: injected error");
    });
    auto component_name = fmt::to_string(component);
    hand_position key{this_shard(), sst.uuid(), component_name, int64_t(offset)};
    if (_page_being_evicted == key) {
        // See _pages_being_put. Not caching a page is harmless.
        co_return;
    }
    ++_pages_being_put[key];
    auto put_done = defer([this, &key] () noexcept {
        auto it = _pages_being_put.find(key);
        if (--it->second == 0) {
            _pages_being_put.erase(it);
        }
    });
    // The hits row is written first: eviction and the count of the cache's
    // bytes find pages only through their hits rows, so if the page's write
    // fails, a hits row without a page is evicted and counted harmlessly,
    // while a page without a hits row would stay until its sstable is
    // deleted. Not writing last_hit means that a page which is cached again
    // keeps its last hit.
    co_await apply(hits_mutation(this_shard(), sst.uuid(), component_name, int64_t(offset), int32_t(data.size()), std::nullopt));
    co_await utils::get_local_injector().inject("object_storage_page_cache_put_page_between_writes",
            utils::wait_for_message(std::chrono::minutes(5)), false);
    co_await apply(page_mutation(this_shard(), sst.uuid(), component_name, int64_t(offset),
            bytes(reinterpret_cast<const int8_t*>(data.get()), data.size())));
    // The page may have been evicted and cached again, so its next hit must
    // be recorded.
    _hit_in_pass.erase(page_key{sst.uuid(), component, offset});
    count_written_page(this_shard(), sst.uuid(), component_name, int64_t(offset), int32_t(data.size()), std::nullopt);
    // A page which was already cached is counted twice, until the bytes are
    // recounted.
    _bytes += data.size();
    _bytes_added += data.size();
    if (limit() && _bytes > limit()) {
        _eviction_needed.signal();
    }
}

future<std::vector<sstables::object_storage_page_cache::page>>
object_storage_page_cache_table::get_pages(sstables::sstable_id sst, sstables::component_type component,
        uint64_t from, uint64_t to, size_t max_pages, sstables::populate_disk_cache populate) {
    utils::get_local_injector().inject("object_storage_page_cache_get_pages", [] {
        throw std::runtime_error("object_storage_page_cache_get_pages: injected error");
    });
    // The pages are read from the table directly, not with an internal CQL
    // query. The sstable read which looks them up holds a permit of the
    // reader concurrency semaphore of its scheduling group, and a CQL query
    // would wait for admission on the same semaphore, behind other reads -
    // which also hold their permits while waiting for their own lookups. So
    // with more concurrent reads than the semaphore admits, all of them
    // would wait until they time out. Instead, the lookup reads with a
    // tracking-only permit, which isn't admitted: it is part of a read which
    // was already admitted, and reads at most max_pages pages.
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
    auto permit = db.get_reader_concurrency_semaphore().make_tracking_only_permit(s, "object_storage_page_cache_get_pages", db::no_timeout, {});
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
    // If the hand evicts a page after it was read here, recording its hit
    // leaves a row without a page and without a size, which the hand evicts
    // when it reaches it.
    if (populate && !pages.empty()) {
        auto now = db_clock::now();
        auto max_remembered = _max_remembered_hits();
        // Recording the hits is only for choosing which pages to evict, so a
        // failure to record them doesn't fail the read of the pages.
        try {
            co_await coroutine::parallel_for_each(pages, [&] (const page& p) -> future<> {
                page_key key{sst.uuid(), component, p.offset};
                if (max_remembered && _hit_in_pass.contains(key)) {
                    co_return;
                }
                if (_hit_in_pass.size() < max_remembered) {
                    _hit_in_pass.insert(key);
                }
                co_await apply(hits_mutation(this_shard(), sst.uuid(), component_name, int64_t(p.offset), std::nullopt, now));
            });
        } catch (...) {
            static thread_local logging::logger::rate_limit rate_limit{std::chrono::minutes(1)};
            pclogger.log(log_level::warn, rate_limit, "Failed to record hits in the disk page cache: {}", std::current_exception());
        }
    }
    co_return pages;
}

future<> object_storage_page_cache_table::evict_sstable(sstables::sstable_id sst) {
    static const auto rows_query = fmt::format("SELECT component, offset, size, last_hit FROM {}.{} WHERE shard = ? AND sstable = ?",
            SYSTEM_CACHE, PAGECACHE_HITS);
    std::vector<hits_row> rows;
    co_await _qp.query_internal(rows_query, db::consistency_level::ONE, {this_shard(), sst.uuid()},
            cql3::query_processor::default_internal_page_size, [&] (const cql3::untyped_result_set_row& row) {
        rows.push_back(hits_row{
            .key = hand_position{this_shard(), sst.uuid(), row.get_as<sstring>("component"), row.get_as<int64_t>("offset")},
            .size = row.get_or<int32_t>("size", 0),
            .last_hit = row.get_opt<db_clock::time_point>("last_hit"),
        });
        return make_ready_future<stop_iteration>(stop_iteration::no);
    });
    co_await apply(sstable_deletion(schema(), this_shard(), sst.uuid()));
    co_await apply(sstable_deletion(hits_schema(), this_shard(), sst.uuid()));

    // Stop counting the evicted pages which the hand counted on its current
    // pass (see _pass_bytes): those which it passed, and those cached within
    // its batch, after it. The hand skips the pages which are in its batch,
    // so it never counts them.
    uint64_t bytes = 0;
    std::set<std::pair<sstring, int64_t>> in_batch;
    for (const auto& row : _batch) {
        if (row.key.sstable == sst.uuid() && row.key.shard == this_shard()) {
            in_batch.emplace(row.key.component, row.key.offset);
        }
    }
    if (!in_batch.empty()) {
        _evicted_in_batch.insert(sst.uuid());
    }
    auto end = fetched_end();
    for (const auto& row : rows) {
        bytes += row.size;
        if (!end || !at_or_before(*end, row.key.shard, row.key.sstable, row.key.component, row.key.offset)
                || in_batch.contains({row.key.component, row.key.offset})) {
            continue;
        }
        _pass_bytes -= std::min<uint64_t>(_pass_bytes, row.size);
        add_to_histogram(_pass_ages, row.last_hit, -int64_t(row.size));
        if (!_hand || !at_or_before(*_hand, row.key.shard, row.key.sstable, row.key.component, row.key.offset)) {
            _window_bytes -= std::min<uint64_t>(_window_bytes, row.size);
            add_to_histogram(_window_ages, row.last_hit, -int64_t(row.size));
        }
    }
    _bytes -= std::min(_bytes, bytes);
}

future<> object_storage_page_cache_table::evict_other_sstables(const std::unordered_set<sstables::sstable_id>& live) {
    // The queries read only this shard's partitions - including, on the last
    // shard, those of shards which no longer exist, whose pages are of no
    // use to any shard (see this_shard_token_range()).
    auto [first, last] = this_shard_token_range();
    for (auto table : {PAGECACHE, PAGECACHE_HITS}) {
        auto select = fmt::format("SELECT DISTINCT shard, sstable FROM {}.{} "
                "WHERE token(shard, sstable) >= ? AND token(shard, sstable) <= ?", SYSTEM_CACHE, table);
        std::vector<std::pair<int16_t, utils::UUID>> orphans;
        co_await _qp.query_internal(select, db::consistency_level::ONE, {first, last},
                cql3::query_processor::default_internal_page_size, [&] (const cql3::untyped_result_set_row& row) {
            auto shard = row.get_as<int16_t>("shard");
            auto sst = row.get_as<utils::UUID>("sstable");
            if (shard != this_shard() || !live.contains(sstables::sstable_id(sst))) {
                orphans.emplace_back(shard, sst);
            }
            return make_ready_future<stop_iteration>(stop_iteration::no);
        });
        // The partition of another shard number, which no longer exists, is
        // on this shard too, so this deletion doesn't leave it either.
        auto s = table == PAGECACHE ? schema() : hits_schema();
        for (auto& [shard, sst] : orphans) {
            co_await apply(sstable_deletion(s, shard, sst));
        }
    }
    co_await count_bytes();
}

future<> object_storage_page_cache_table::copy_sstable_from_shard(sstables::sstable_id sst, shard_id from) {
    // Unlike the other queries, these read the partitions of another shard,
    // so they are executed on that shard.
    // The hits rows are copied first, as put_page() writes them first: if
    // the copy fails in the middle, a hits row without a page is harmless,
    // but a page without a hits row would stay, uncounted, until its sstable
    // is deleted. The hits rows also have when the pages were last hit, so
    // the new shard evicts them in the same order as the old one would.
    static const auto select_hits = fmt::format("SELECT component, offset, last_hit, size FROM {}.{} WHERE shard = ? AND sstable = ?",
            SYSTEM_CACHE, PAGECACHE_HITS);
    co_await _qp.query_internal(select_hits, db::consistency_level::ONE, {int16_t(from), sst.uuid()},
            cql3::query_processor::default_internal_page_size,
            [&] (const cql3::untyped_result_set_row& row) -> future<stop_iteration> {
        auto component = row.get_as<sstring>("component");
        auto offset = row.get_as<int64_t>("offset");
        auto size = row.get_or<int32_t>("size", 0);
        co_await apply(hits_mutation(this_shard(), sst.uuid(), component, offset, size, row.get_opt<db_clock::time_point>("last_hit")));
        _bytes += size;
        _bytes_added += size;
        count_written_page(this_shard(), sst.uuid(), component, offset, size, row.get_opt<db_clock::time_point>("last_hit"));
        co_return stop_iteration::no;
    });
    static const auto select = fmt::format("SELECT component, offset, page FROM {}.{} WHERE shard = ? AND sstable = ?",
            SYSTEM_CACHE, PAGECACHE);
    // A page may be as large as a compressed chunk, so read only a few at a
    // time.
    constexpr int32_t pages_per_read = 100;
    co_await _qp.query_internal(select, db::consistency_level::ONE, {int16_t(from), sst.uuid()}, pages_per_read,
            [&] (const cql3::untyped_result_set_row& row) -> future<stop_iteration> {
        co_await apply(page_mutation(this_shard(), sst.uuid(), row.get_as<sstring>("component"), row.get_as<int64_t>("offset"),
                row.get_blob_unfragmented("page")));
        co_return stop_iteration::no;
    });
    if (limit() && _bytes > limit()) {
        _eviction_needed.signal();
    }
}

// Bucket 0 of an age_histogram holds the ages below 1 ms. The following
// buckets start at every millisecond, until there are 4 buckets for each
// doubling of the age - so the ages in a bucket differ by at most 19% - up
// to 2^48 ms (almost 9000 years). The last bucket holds the older ages too.
static const auto age_bucket_starts = [] {
    constexpr auto age_buckets = object_storage_page_cache_table::age_buckets;
    std::array<int64_t, age_buckets> starts{};
    for (size_t i = 1; i < age_buckets; i++) {
        starts[i] = std::max<int64_t>(starts[i - 1] + 1, std::llround(std::exp2((i - 1) / 4.0)));
    }
    return starts;
}();

int64_t object_storage_page_cache_table::age_bucket_start(size_t i) {
    return age_bucket_starts[i];
}

size_t object_storage_page_cache_table::age_bucket(int64_t age_ms) {
    // A page hit after the histogram was started has a negative age.
    auto it = std::upper_bound(age_bucket_starts.begin(), age_bucket_starts.end(), std::max<int64_t>(age_ms, 0));
    return it - age_bucket_starts.begin() - 1;
}

void object_storage_page_cache_table::add_to_histogram(age_histogram& h, std::optional<db_clock::time_point> last_hit, int64_t size) {
    if (h.buckets.empty()) {
        // The histogram wasn't started.
        return;
    }
    auto& counter = last_hit
            ? h.buckets[age_bucket(std::chrono::duration_cast<std::chrono::milliseconds>(h.counted_at - *last_hit).count())]
            : h.never_hit;
    counter = size >= 0 ? counter + size : counter - std::min<uint64_t>(counter, -size);
}

future<> object_storage_page_cache_table::count_bytes() {
    auto [first, last] = this_shard_token_range();
    static const auto query = fmt::format("SELECT size, last_hit FROM {}.{} "
            "WHERE token(shard, sstable) >= ? AND token(shard, sstable) <= ?", SYSTEM_CACHE, PAGECACHE_HITS);
    auto added_before = _bytes_added;
    uint64_t bytes = 0;
    age_histogram ages{.counted_at = db_clock::now(), .buckets = std::vector<uint64_t>(age_buckets)};
    co_await _qp.query_internal(query, db::consistency_level::ONE, {first, last},
            cql3::query_processor::default_internal_page_size, [&] (const cql3::untyped_result_set_row& row) {
        auto size = row.get_or<int32_t>("size", 0);
        bytes += size;
        add_to_histogram(ages, row.get_opt<db_clock::time_point>("last_hit"), size);
        return make_ready_future<stop_iteration>(stop_iteration::no);
    });
    // The pages added while the query ran may or may not have been counted
    // by it, so count them again: counting a page twice only makes the cache
    // evict a little earlier, while not counting it at all could let the
    // cache grow beyond its limit. They were cached after the count started,
    // so they weren't hit since.
    _bytes = bytes + (_bytes_added - added_before);
    add_to_histogram(ages, std::nullopt, _bytes_added - added_before);
    _ages = std::move(ages);
}

db_clock::time_point object_storage_page_cache_table::eviction_cutoff(uint64_t to_free) const {
    // Evicting only the pages which were never hit is enough - or there is
    // no histogram yet, so evict only them, and the hand builds one.
    if (_ages.never_hit >= to_free || _ages.buckets.empty()) {
        return db_clock::time_point::min();
    }
    // Add the oldest buckets, until they are enough. The cutoff is at the
    // start of the last bucket added: a page which was hit then, or before,
    // is evicted.
    uint64_t bytes = _ages.never_hit;
    for (size_t i = _ages.buckets.size(); i-- > 0;) {
        bytes += _ages.buckets[i];
        if (bytes >= to_free) {
            return _ages.counted_at - std::chrono::milliseconds(age_bucket_start(i)) + std::chrono::milliseconds(1);
        }
    }
    // Not even all the pages are enough (they were counted before more were
    // cached), so evict them all.
    return db_clock::time_point::max();
}

void object_storage_page_cache_table::start(scheduling_group sg) {
    _evictor = with_scheduling_group(sg, [this] { return evict_in_background(); });
}

future<> object_storage_page_cache_table::stop() noexcept {
    _stopping = true;
    _eviction_needed.broken();
    if (_evictor) {
        co_await std::exchange(_evictor, std::nullopt).value();
    }
}

future<> object_storage_page_cache_table::evict_in_background() {
    // The cache is plugged before the system_cache keyspace is loaded, so
    // retry counting its bytes until it is.
    while (!_stopping) {
        bool counted = false;
        try {
            co_await count_bytes();
            counted = true;
        } catch (...) {
            pclogger.debug("Failed to count the bytes in the disk page cache, will retry: {}", std::current_exception());
        }
        if (counted) {
            break;
        }
        try {
            co_await _eviction_needed.wait(std::chrono::seconds(1));
        } catch (const condition_variable_timed_out&) {
        } catch (const broken_condition_variable&) {
            co_return;
        }
    }
    bool failed = false;
    while (!_stopping) {
        try {
            co_await _eviction_needed.wait([this] { return _stopping || (limit() && _bytes > limit()); });
            if (!_stopping) {
                co_await evict_if_needed();
            }
        } catch (const broken_condition_variable&) {
            break;
        } catch (...) {
            pclogger.warn("Failed to evict pages from the disk page cache: {}", std::current_exception());
            failed = true;
        }
        if (failed) {
            // Don't retry in a tight loop.
            failed = false;
            try {
                co_await _eviction_needed.wait(std::chrono::seconds(1));
            } catch (const condition_variable_timed_out&) {
            } catch (const broken_condition_variable&) {
                break;
            }
        }
    }
}

future<> object_storage_page_cache_table::evict_if_needed() {
    auto max = limit();
    if (!max || _bytes <= max) {
        co_return;
    }
    // Evict a little more than needed, so that the cache doesn't need to
    // evict again for every new page.
    co_await sweep(max / 10 * 9);
}

// Evicting the least recently hit pages: a hand moves over this shard's
// pages, in the order of their keys, and evicts the pages which were never
// hit since they were cached, and those which were last hit before a cutoff
// time - until the cache is small enough. When the hand starts a pass, it
// chooses the cutoff from a histogram of how long ago the cache's pages
// were last hit, so that these pages add up to at least a quarter of the
// cache (eviction_cutoff()) - first the pages which were never hit. So the
// pages are evicted in the order of their last hits - up to the precision
// of the hit times and of the histogram - not in the order of their keys,
// which only decides between pages hit at about the same time. Choosing a
// quarter of the cache, rather than only the bytes which need to be
// evicted now, lets one pass, with one cutoff, serve several evictions:
// the hand keeps its position, and its cutoff, between them.
//
// The hand builds the histogram for its next pass, and recounts the bytes
// in the cache, from the pages which it passes on this pass and keeps - so
// the cache is scanned only once per pass.
future<> object_storage_page_cache_table::sweep(uint64_t target) {
    // The hand reads the rows of pagecache_hits in batches of its own, with
    // LIMIT, rather than with the automatic paging of query_internal(),
    // because it needs to know how far it fetched. That paging fetches a
    // page of rows, and passes them to the hand one by one, without saying
    // where the page ends - and a page cached during the pass after the
    // hand's position, but within the rows which were already fetched, is
    // never seen by the hand on this pass, so it must be counted when it is
    // cached (see _pass_bytes). Only the end of the batch tells which pages
    // those are. (query_processor's interface which returns a page at a
    // time is private to it - and when the hand stops in the middle of a
    // batch, it must continue from its position anyway, with new queries.)
    //
    // A batch has as many rows as a page of the internal paging: a row of
    // pagecache_hits is small, so a batch takes some tens of KiB of memory;
    // each query has a fixed cost, so smaller batches would need more
    // queries for each pass; and larger batches would hold rows longer
    // before the hand visits them, so their hits would be staler.
    constexpr int32_t batch_rows = cql3::query_processor::default_internal_page_size;
    static const auto rest_of_partition = fmt::format("SELECT component, offset, size, last_hit FROM {}.{} "
            "WHERE shard = ? AND sstable = ? AND (component, offset) > (?, ?) LIMIT ?", SYSTEM_CACHE, PAGECACHE_HITS);
    static const auto next_partitions = fmt::format("SELECT shard, sstable, component, offset, size, last_hit FROM {}.{} "
            "WHERE token(shard, sstable) > token(?, ?) AND token(shard, sstable) <= ? LIMIT ?", SYSTEM_CACHE, PAGECACHE_HITS);
    static const auto all_partitions = fmt::format("SELECT shard, sstable, component, offset, size, last_hit FROM {}.{} "
            "WHERE token(shard, sstable) >= ? AND token(shard, sstable) <= ? LIMIT ?", SYSTEM_CACHE, PAGECACHE_HITS);
    auto [first, last] = this_shard_token_range();

    // Fetches the next batch of rows after the hand: the rest of the
    // partition in which it is, and the following partitions.
    auto fetch = [&] () -> future<> {
        auto add = [&] (const cql3::untyped_result_set& rs, std::optional<std::pair<int16_t, utils::UUID>> partition) {
            for (const auto& row : rs) {
                _batch.push_back(hits_row{
                    .key = hand_position{
                        partition ? partition->first : row.get_as<int16_t>("shard"),
                        partition ? partition->second : row.get_as<utils::UUID>("sstable"),
                        row.get_as<sstring>("component"), row.get_as<int64_t>("offset")},
                    .size = row.get_or<int32_t>("size", 0),
                    .last_hit = row.get_opt<db_clock::time_point>("last_hit"),
                });
            }
        };
        if (!_hand) {
            add(*co_await _qp.execute_internal(all_partitions, {first, last, batch_rows}, cql3::query_processor::cache_internal::yes),
                    std::nullopt);
            co_return;
        }
        auto hand = *_hand;
        add(*co_await _qp.execute_internal(rest_of_partition, {hand.shard, hand.sstable, hand.component, hand.offset, batch_rows},
                cql3::query_processor::cache_internal::yes), std::pair(hand.shard, hand.sstable));
        if (_batch.size() < size_t(batch_rows)) {
            add(*co_await _qp.execute_internal(next_partitions, {hand.shard, hand.sstable, last, int32_t(batch_rows - _batch.size())},
                    cql3::query_processor::cache_internal::yes), std::nullopt);
        }
    };

    // Visits a page: evicts it if it wasn't hit since the cutoff, and stops
    // the hand when the cache is small enough.
    auto visit = [&] (const hits_row& row) -> future<stop_iteration> {
        _hand = row.key;
        if (_pages_being_put.contains(row.key)) {
            // The page is being cached (see _pages_being_put), so it isn't
            // evicted - and put_page() counts it when it is written, because
            // the hand has reached it.
        } else if (!row.last_hit || *row.last_hit < _cutoff) {
            _page_being_evicted = row.key;
            auto evict_done = defer([this] () noexcept { _page_being_evicted.reset(); });
            co_await evict_page(row.key.shard, row.key.sstable, row.key.component, row.key.offset, row.size);
        } else {
            _pass_bytes += row.size;
            add_to_histogram(_pass_ages, row.last_hit, row.size);
        }
        co_return stop_iteration(_bytes <= target || _stopping);
    };

    // A pass usually evicts enough, because its cutoff was chosen for it.
    // But if pages are hit, or cached, during a pass - after the histogram
    // from which its cutoff was chosen - it may evict less. So if two passes
    // didn't evict enough, the third pass evicts pages regardless of their
    // hits, to keep the cache within its limit even if all of its pages are
    // hit all the time.
    for (int passes = 0; _bytes > target && !_stopping && passes < 3;) {
        if (!_hand) {
            // A hit of a page on this pass is recorded again.
            _hit_in_pass.clear();
            ++passes;
            // Lets tests hit pages after the histogram, from which this
            // pass's cutoff is chosen, was built.
            co_await utils::get_local_injector().inject("object_storage_page_cache_pass_start",
                    utils::wait_for_message(std::chrono::minutes(5)), false);
            _pass_bytes = 0;
            _pass_ages = age_histogram{.counted_at = db_clock::now(), .buckets = std::vector<uint64_t>(age_buckets)};
            _window_bytes = 0;
            _window_ages = age_histogram{.counted_at = _pass_ages.counted_at, .buckets = std::vector<uint64_t>(age_buckets)};
            _cutoff = passes == 3
                    ? db_clock::time_point::max()
                    : eviction_cutoff(std::max(_bytes - std::min(_bytes, target), _bytes / 4));
        }
        bool stopped = false;
        bool pass_done = false;
        while (!stopped) {
            if (_batch.empty()) {
                // The pages cached within the previous batch, after the
                // hand, are now behind it.
                _evicted_in_batch.clear();
                _window_bytes = 0;
                _window_ages = age_histogram{.counted_at = _pass_ages.counted_at, .buckets = std::vector<uint64_t>(age_buckets)};
                co_await fetch();
                // Lets tests cache pages within the hand's batch.
                co_await utils::get_local_injector().inject("object_storage_page_cache_batch_fetched",
                        utils::wait_for_message(std::chrono::minutes(5)), false);
                if (_batch.empty()) {
                    pass_done = true;
                    break;
                }
            }
            auto row = std::move(_batch.front());
            _batch.pop_front();
            if (_evicted_in_batch.contains(row.key.sstable)) {
                // The page was evicted with its sstable.
                _hand = std::move(row.key);
                continue;
            }
            stopped = bool(co_await visit(row));
        }
        if (stopped) {
            // The cache is small enough. Drop the rest of the batch, which the
            // hand fetches again when it continues - with their latest hits.
            drop_batch();
        } else if (pass_done) {
            // The hand reached the end of this shard's cache: it passed all
            // of its pages, except those cached during the pass where the
            // hand doesn't see them, which _pass_bytes counts too - so
            // _pass_bytes recounts the bytes in the cache, without another
            // scan of it. (A page which was cached again, while it was
            // cached, may be counted twice - until the next pass.)
            _bytes = _pass_bytes;
            // The histogram of the pages which the hand kept on this pass
            // chooses the cutoff of the next one.
            _ages = std::move(_pass_ages);
            _pass_ages = age_histogram{};
            _window_ages = age_histogram{};
            // Start a new pass.
            _hand.reset();
        }
    }
}

future<> object_storage_page_cache_table::evict_page(int16_t shard, utils::UUID sstable, const sstring& component, int64_t offset, int32_t size) {
    co_await apply(page_deletion(schema(), shard, sstable, component, offset));
    co_await utils::get_local_injector().inject("object_storage_page_cache_evict_page_between_deletions",
            utils::wait_for_message(std::chrono::minutes(5)), false);
    co_await apply(page_deletion(hits_schema(), shard, sstable, component, offset));
    _bytes -= std::min(_bytes, uint64_t(size));
}

} // namespace db
