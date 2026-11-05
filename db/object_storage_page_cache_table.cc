/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// This file is part of the object storage page cache (the 'disk_cache': 'pages'
// keyspace storage option): the schema of the system_cache.pagecache table,
// which stores the cache, and the implementation of the cache's interface over
// it. See db/object_storage_page_cache_table.hh.

#include <algorithm>
#include <array>
#include <cmath>
#include <set>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/core/smp.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/util/defer.hh>
#include <seastar/core/metrics.hh>
#include "db/object_storage_page_cache_table.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "db_clock.hh"
#include "dht/fixed_shard.hh"
#include "mutation/frozen_mutation.hh"
#include "mutation/mutation.hh"
#include "mutation/mutation_fragment_v2.hh"
#include "partition_slice_builder.hh"
#include "query/query-result-set.hh"
#include "readers/mutation_reader.hh"
#include "replica/database.hh"
#include "schema/caching_options.hh"
#include "schema/schema_builder.hh"
#include "service/storage_proxy.hh"
#include "sstables/compressor.hh"
#include "tombstone_gc_options.hh"
#include "types/types.hh"
#include "utils/chunked_vector.hh"
#include "utils/error_injection.hh"
#include "utils/fragment_range.hh"
#include "utils/hash.hh"
#include "utils/log.hh"
#include "utils/murmur_hash.hh"
#include "utils/stall_free.hh"

namespace db {

static logging::logger pclogger("object_storage_page_cache");

// The object storage page cache: byte ranges ("pages") of sstable components,
// which were read from the object store.
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
            // The sstable's sstable_id (sstable::sstable_identifier()), which
            // is also in the names of its objects on the object store.
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
            .set_comment("The object storage page cache: pages of sstables on object storage, cached on the local disk")
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
            // When the cache is full, each page cached is matched by a page
            // evicted, so the table's live data stays about the size of the
            // cache while all of it is replaced - as in an overwrite
            // workload. With size-tiered compaction, the largest tier, which
            // holds most of the cache, could become mostly deleted pages
            // before it is compacted, so the table could take several times
            // the cache's size on disk. With a space amplification goal,
            // incremental compaction compacts the two largest tiers together
            // when they are more than 1.5 times the largest one, which keeps
            // the table's disk usage near 1.5 times its live data - at the
            // cost of rewriting the largest tier about twice each time the
            // cache's contents are replaced.
            .set_compaction_strategy(compaction::compaction_strategy_type::incremental)
            .set_compaction_strategy_options({{"space_amplification_goal", "1.5"}})
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
            .set_comment("When the pages of the object storage page cache were last hit")
            .with_hash_version()
            .with_partitioner(dht::fixed_shard_partitioner::classname)
            .with_sharder(dht::fixed_shard_sharder::instance())
            // The table is written much more than it is read, so caching it
            // in memory would be a waste.
            .set_caching_options(caching_options::get_disabled_caching_options())
            // The table is small, so its space amplification costs little
            // disk space, and it is only read by scans of a shard's whole
            // table, on each pass of the hand which evicts pages - so its
            // space amplification costs only the reading of the garbage in
            // each scan. Reducing it would cost compaction, which reads the
            // garbage too, and writes the live rows again - and much of the
            // table changes in each pass, so compacting more, to scan less,
            // would cost at least as much as it saves. So the table uses the
            // default incremental compaction, without a space amplification
            // goal, which compacts the least; leveled compaction, which
            // compacts the most, would be the worst choice.
            .set_compaction_strategy(compaction::compaction_strategy_type::incremental)
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
// fewer shards: their tokens are after its own. Returns the range of the
// shard's tokens, which includes both of its ends.
static dht::token_range this_shard_token_range() {
    auto first = dht::fixed_shard_partitioner::token_for_shard(this_shard_id(), 0);
    auto last = this_shard_id() == this_smp_shard_count() - 1
            ? dht::fixed_shard_partitioner::token_for_shard(dht::fixed_shard_partitioner::max_shard, dht::fixed_shard_partitioner::hash_mask)
            : dht::fixed_shard_partitioner::token_for_shard(this_shard_id(), dht::fixed_shard_partitioner::hash_mask);
    return dht::token_range::make({first, true}, {last, true});
}

// The keys of both tables: ((shard, sstable), component, offset).
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

// A mutation which writes a page's row in the hits table, like an INSERT
// of the given columns: a column which isn't given keeps its value.
static mutation hits_mutation(int16_t shard, utils::UUID sstable, const sstring& component, int64_t offset,
        std::optional<int32_t> size, std::optional<db_clock::time_point> last_hit) {
    auto s = object_storage_page_cache_table::hits_schema();
    auto ts = object_storage_page_cache_table::next_timestamp();
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
    m.partition().apply(tombstone(object_storage_page_cache_table::next_timestamp(), gc_clock::now()));
    return m;
}

// A mutation which deletes the row of a page in the given table.
static mutation page_deletion(schema_ptr s, int16_t shard, utils::UUID sstable, const sstring& component, int64_t offset) {
    mutation m(s, cache_partition_key(*s, shard, sstable));
    m.partition().apply_delete(*s, cache_clustering_key(*s, component, offset), tombstone(object_storage_page_cache_table::next_timestamp(), gc_clock::now()));
    return m;
}

object_storage_page_cache_table::object_storage_page_cache_table(replica::database& db, cql3::query_processor& qp,
        utils::updateable_value<uint64_t> size_in_mb, utils::updateable_value<uint64_t> max_remembered_hits)
    : _db(db)
    , _qp(qp)
    , _size_in_mb(std::move(size_in_mb))
    , _max_remembered_hits(std::move(max_remembered_hits))
    , _size_observer(_size_in_mb.observe([this] (uint64_t) { _eviction_needed.signal(); }))
{
    // Before the hand's first pass, which may come long after the cache
    // started filling. Without yielding, but only once, as the node starts.
    reset_hit_filter_now();
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
    co_await _db.apply(std::move(s), freeze(m), {}, db::commitlog_force_sync::no, db::no_timeout);
}

// The hand passes the partitions in the order of their tokens (the order of
// the queries in sweep()), and the pages of a partition in the order of
// their clustering keys: by component, as UTF-8 text, and then by offset.
std::strong_ordering object_storage_page_cache_table::compare_partitions(int16_t shard, utils::UUID sstable, const hand_position& pos) {
    auto s = hits_schema();
    return dht::decorate_key(*s, cache_partition_key(*s, shard, sstable)).tri_compare(*s,
            dht::decorate_key(*s, cache_partition_key(*s, pos.shard, pos.sstable)));
}

bool object_storage_page_cache_table::at_or_before(const hand_position& pos, std::strong_ordering partitions,
        std::string_view component, int64_t offset) {
    if (partitions != 0) {
        return partitions < 0;
    }
    // UTF-8 text is ordered by its bytes, as std::string_view orders it.
    return std::pair(component, offset) <= std::pair(std::string_view(pos.component), pos.offset);
}

bool object_storage_page_cache_table::at_or_before(const hand_position& pos, int16_t shard, utils::UUID sstable,
        std::string_view component, int64_t offset) {
    return at_or_before(pos, compare_partitions(shard, sstable, pos), component, offset);
}

std::optional<object_storage_page_cache_table::hand_position> object_storage_page_cache_table::fetched_end() const {
    return _batch.empty() ? _hand : std::optional<hand_position>(_batch.back().key);
}

void object_storage_page_cache_table::count_written_page(int16_t shard, utils::UUID sstable, std::string_view component,
        int64_t offset, int32_t size, std::optional<db_clock::time_point> last_hit) {
    if (_fetching && (!_hand || !at_or_before(*_hand, shard, sstable, component, offset))) {
        // The batch which the hand is fetching may have the page, or not.
        _written_during_fetch.push_back(written_page{hand_position{shard, sstable, sstring(component), offset}, size, last_hit});
        return;
    }
    auto end = fetched_end();
    if (!end || !at_or_before(*end, shard, sstable, component, offset)) {
        // The hand will see the page on this pass, or no pass is ongoing.
        return;
    }
    _pass_bytes += size;
    add_to_histogram(_pass_ages, last_hit, size);
    if (!_hand || !at_or_before(*_hand, shard, sstable, component, offset)) {
        // The page is after the hand, within its batch.
        _window.push_back(written_page{hand_position{shard, sstable, sstring(component), offset}, size, last_hit});
    }
}

void object_storage_page_cache_table::count_pages_written_during_fetch() {
    std::unordered_set<hand_position, hand_position_hash> in_batch;
    for (const auto& row : _batch) {
        in_batch.insert(row.key);
    }
    // If the batch is empty, the hand reached the end of the cache, so all
    // the pages after it are counted.
    auto end = fetched_end();
    for (auto& page : std::exchange(_written_during_fetch, {})) {
        if (in_batch.contains(page.key)) {
            // The hand will see the page.
            continue;
        }
        if (!_batch.empty() && !at_or_before(*end, page.key.shard, page.key.sstable, page.key.component, page.key.offset)) {
            // The hand will fetch the page with a later batch.
            continue;
        }
        _pass_bytes += page.size;
        add_to_histogram(_pass_ages, page.last_hit, page.size);
        if (!_batch.empty()) {
            _window.push_back(std::move(page));
        }
    }
}

void object_storage_page_cache_table::drop_batch() {
    _batch.clear();
    _evicted_in_batch.clear();
    // The pages cached within the batch which are still after the hand are
    // fetched again; those which the hand already passed aren't.
    for (const auto& page : _window) {
        if (!_hand || !at_or_before(*_hand, page.key.shard, page.key.sstable, page.key.component, page.key.offset)) {
            _pass_bytes -= std::min<uint64_t>(_pass_bytes, page.size);
            add_to_histogram(_pass_ages, page.last_hit, -int64_t(page.size));
        }
    }
    _window.clear();
}

uint64_t object_storage_page_cache_table::limit() const noexcept {
    return _size_in_mb() * 1024 * 1024 / this_smp_shard_count();
}

future<> object_storage_page_cache_table::put_page(sstables::sstable_id sst, sstables::component_type component,
        uint64_t offset, temporary_buffer<char> data) {
    return do_put_page(sst, component, offset, std::move(data), std::nullopt);
}

future<> object_storage_page_cache_table::put_copied_page(sstables::sstable_id sst, sstables::component_type component,
        uint64_t offset, temporary_buffer<char> data, std::optional<db_clock::time_point> last_hit) {
    return do_put_page(sst, component, offset, std::move(data), last_hit);
}

future<> object_storage_page_cache_table::do_put_page(sstables::sstable_id sst, sstables::component_type component,
        uint64_t offset, temporary_buffer<char> data, std::optional<db_clock::time_point> last_hit) {
    utils::get_local_injector().inject("object_storage_page_cache_put_page", [] {
        throw std::runtime_error("object_storage_page_cache_put_page: injected error");
    });
    auto operation = _operations.try_hold();
    if (!operation) {
        // The cache was drained (see drain()). Not caching a page is harmless.
        co_return;
    }
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
    if (!last_hit && hit_in_pass(page_key{sst.uuid(), component, offset})) {
        // The page's hit was recorded in this pass, but the page may have
        // been evicted, with its hit, since - and the filter of pages hit in
        // the pass can't forget it, so its next hit in the pass wouldn't be
        // recorded. So the page is written with a hit now (see _hit_filter).
        last_hit = db_clock::now();
    }
    // The hits row is written first: eviction and the count of the cache's
    // bytes find pages only through their hits rows, so if the page's write
    // fails, a hits row without a page is evicted and counted harmlessly,
    // while a page without a hits row would stay until its sstable is
    // deleted. Not writing last_hit means that a page which is cached again
    // keeps its last hit.
    co_await apply(hits_mutation(this_shard(), sst.uuid(), component_name, int64_t(offset), int32_t(data.size()), last_hit));
    co_await utils::get_local_injector().inject("object_storage_page_cache_put_page_between_writes",
            utils::wait_for_message(std::chrono::minutes(5)), false);
    co_await apply(page_mutation(this_shard(), sst.uuid(), component_name, int64_t(offset),
            bytes(reinterpret_cast<const int8_t*>(data.get()), data.size())));
    _pages_written = true;
    count_written_page(this_shard(), sst.uuid(), component_name, int64_t(offset), int32_t(data.size()), last_hit);
    // A page which was already cached is counted twice, until the bytes are
    // recounted.
    _bytes += data.size();
    _bytes_added += data.size();
    if (limit() && _bytes > limit()) {
        _eviction_needed.signal();
    }
}

// Reads the rows of an sstable's component in the given table of the cache,
// whose offsets are in [from, to), but at most max_rows of them - directly
// from the table, with a permit of a semaphore which never makes it wait (see
// cached_pages_reader).
static future<query::result_set> query_component(replica::database& db, std::string_view table_name, sstables::sstable_id sst,
        const sstring& component_name, uint64_t from, uint64_t to, size_t max_rows, const char* op_name) {
    auto& table = db.find_column_family(object_storage_page_cache_table::SYSTEM_CACHE, table_name);
    auto s = table.schema();
    auto key = dht::decorate_key(*s, cache_partition_key(*s, this_shard(), sst.uuid()));
    auto cmd = query::read_command(s->id(), s->version(),
            partition_slice_builder(*s).with_range(query::clustering_range::make(
                    {cache_clustering_key(*s, component_name, int64_t(from)), true},
                    {cache_clustering_key(*s, component_name, int64_t(to)), false})).build(),
            query::max_result_size(query::result_memory_limiter::unlimited_result_size), query::tombstone_limit::max,
            query::row_limit(max_rows));
    auto ranges = dht::partition_range_vector{dht::partition_range::make_singular(key)};
    auto permit = db.object_storage_page_cache_semaphore().make_tracking_only_permit(s, op_name, db::no_timeout, {});
    auto result = co_await table.query(s, std::move(permit), cmd, query::result_options::only_result(), ranges, {},
            db.get_result_memory_limiter(), db::no_timeout);
    co_return query::result_set::from_raw_result(s, cmd.slice, *result);
}

// Reads the pages of a range of an sstable's component from the cache's
// table, in offset order, with a mutation reader of the table - which the
// reader keeps open between pages, so reading consecutive pages looks the
// range up in the table's sstables only once.
//
// The reader reads the table directly, not with an internal CQL query. The
// sstable read which looks up its pages holds a permit of the reader
// concurrency semaphore of its scheduling group, and a CQL query would wait
// for admission on the same semaphore, behind other reads - which also hold
// their permits while waiting for their own lookups. So with more concurrent
// reads than the semaphore admits, all of them would wait until they time
// out. Instead, the reader has a tracking-only permit, which isn't admitted:
// it is part of a read which was already admitted.
//
// But skipping admission isn't enough: while a read runs, it requests memory
// from its permit's semaphore (through tracked files), which, when its reads
// use too much memory, lets only one of them proceed, and makes the others
// wait. If the read which proceeds were the one waiting for this reader, a
// permit of the same semaphore would wait behind it, and so would all the
// reads. So the permit is of a semaphore without limits, which never makes a
// read wait - for admission, or for memory
// (database::object_storage_page_cache_semaphore()). The reader's memory,
// which its permit tracks, is bounded anyway, by the size of its buffers.
//
// The table's mutation reader returns the rows as they are stored, with the
// tombstones which delete them - the hand evicts a page with a row
// tombstone, and evict_sstable() evicts all of an sstable's pages with a
// partition tombstone - so the reader skips the pages which a tombstone
// deletes, as a query would. A page which is cached again after it was
// evicted is written with a later timestamp, so the tombstone doesn't delete
// it.
namespace {

class cached_pages_reader {
    replica::table& _table;
    schema_ptr _schema;
    const column_definition& _page_column;
    // The ranges which the mutation reader reads, which must live as long as
    // it does.
    dht::partition_range _range;
    query::partition_slice _slice;
    mutation_reader _reader;
    tombstone _partition_tombstone;
    tombstone _range_tombstone;
public:
    cached_pages_reader(replica::database& db, sstables::sstable_id sst, const sstring& component_name,
            uint64_t from, uint64_t to, const char* op_name)
        : _table(db.find_column_family(object_storage_page_cache_table::SYSTEM_CACHE, object_storage_page_cache_table::PAGECACHE))
        , _schema(_table.schema())
        , _page_column(*_schema->get_column_definition("page"))
        , _range(dht::partition_range::make_singular(dht::decorate_key(*_schema, cache_partition_key(*_schema, this_shard(), sst.uuid()))))
        , _slice(partition_slice_builder(*_schema).with_range(query::clustering_range::make(
                {cache_clustering_key(*_schema, component_name, int64_t(from)), true},
                {cache_clustering_key(*_schema, component_name, int64_t(to)), false})).build())
        , _reader(_table.make_mutation_reader(_schema,
                db.object_storage_page_cache_semaphore().make_tracking_only_permit(_schema, op_name, db::no_timeout, {}),
                _range, _slice, nullptr, streamed_mutation::forwarding::no, mutation_reader::forwarding::no))
    {}
    // The mutation reader refers to _range and _slice.
    cached_pages_reader(cached_pages_reader&&) = delete;

    // Returns the next page which no tombstone deletes, or nothing after the
    // last one.
    future<std::optional<sstables::object_storage_page_cache::page>> next() {
        while (auto mf = co_await _reader()) {
            if (mf->is_partition_start()) {
                _partition_tombstone = mf->as_partition_start().partition_tombstone();
            } else if (mf->is_range_tombstone_change()) {
                _range_tombstone = mf->as_range_tombstone_change().tombstone();
            } else if (mf->is_clustering_row()) {
                if (auto page = live_page(mf->as_clustering_row())) {
                    co_return page;
                }
            }
        }
        co_return std::nullopt;
    }

    future<> close() noexcept {
        return _reader.close();
    }
private:
    std::optional<sstables::object_storage_page_cache::page> live_page(const clustering_row& row) const {
        auto cell = row.cells().find_cell(_page_column.id);
        if (!cell) {
            return std::nullopt;
        }
        auto t = std::max({_partition_tombstone, _range_tombstone, row.tomb().tomb()});
        auto page = cell->as_atomic_cell(_page_column);
        if (!page.is_live(t, false)) {
            return std::nullopt;
        }
        auto value = page.value();
        temporary_buffer<char> data(value.size_bytes());
        auto out = data.get_write();
        for (bytes_view fragment : fragment_range(value)) {
            out = std::copy_n(reinterpret_cast<const char*>(fragment.data()), fragment.size(), out);
        }
        return sstables::object_storage_page_cache::page{
            .offset = uint64_t(value_cast<int64_t>(long_type->deserialize_value(row.key().get_component(*_schema, 1)))),
            .data = std::move(data),
        };
    }
};

} // anonymous namespace

// A cursor of read_pages(), which reads the pages with a cached_pages_reader.
class object_storage_page_cache_table::page_cursor_impl : public sstables::object_storage_page_cache::page_cursor {
    // The longest gap before the page which the cursor is asked for that it
    // reads through: reading through a gap reads the cached pages in it from
    // the table, while a new lookup reads only the indexes of the table's
    // sstables, so a longer gap is cheaper to look up anew. The limit is the
    // size of an sstable reader's buffer (default_sstable_buffer_size): the
    // pages of a shorter gap may already be in the buffer.
    static constexpr uint64_t max_read_through_gap = 128 * 1024;

    object_storage_page_cache_table& _cache;
    sstables::sstable_id _sst;
    sstables::component_type _component;
    sstring _component_name;
    uint64_t _from;
    uint64_t _to;
    sstables::populate_disk_cache _populate;
    std::unique_ptr<cached_pages_reader> _reader;
    // The offset after which _reader returns the next page: the start of its
    // range, or the offset of the last page which it returned.
    uint64_t _reader_pos = 0;
    // Whether the reader returned all the pages up to the end of the range.
    bool _exhausted = false;
    // The page which the reader returned after the offset that get_page()
    // was asked for, which a later get_page() may ask for.
    std::optional<page> _next;
public:
    page_cursor_impl(object_storage_page_cache_table& cache, sstables::sstable_id sst, sstables::component_type component,
            uint64_t from, uint64_t to, sstables::populate_disk_cache populate)
        : _cache(cache)
        , _sst(sst)
        , _component(component)
        , _component_name(fmt::to_string(component))
        , _from(from)
        , _to(to)
        , _populate(populate)
        , _reader_pos(from)
    {}

    virtual future<temporary_buffer<char>> get_page(uint64_t offset) override {
        utils::get_local_injector().inject("object_storage_page_cache_read_pages", [] {
            throw std::runtime_error("object_storage_page_cache_read_pages: injected error");
        });
        if (offset < _from || offset >= _to) {
            co_return temporary_buffer<char>();
        }
        if (!_next || _next->offset < offset) {
            _next.reset();
            if (!_exhausted) {
                if (!_reader || (offset > _reader_pos && offset - _reader_pos > max_read_through_gap)) {
                    co_await close_reader();
                    _reader = std::make_unique<cached_pages_reader>(_cache._db, _sst, _component_name,
                            offset, _to, "object_storage_page_cache_read_pages");
                    _reader_pos = offset;
                }
                while (auto p = co_await _reader->next()) {
                    _reader_pos = p->offset;
                    if (p->offset >= offset) {
                        _next = std::move(p);
                        break;
                    }
                }
                if (!_next) {
                    // No page is left in the range, so the reader isn't needed.
                    _exhausted = true;
                    co_await close_reader();
                }
            }
        }
        if (!_next || _next->offset != offset) {
            co_return temporary_buffer<char>();
        }
        auto data = std::move(_next->data);
        _next.reset();
        if (_populate) {
            co_await _cache.record_hits(_sst, _component, _component_name, {int64_t(offset)});
        }
        co_return data;
    }

    virtual future<> close() noexcept override {
        return close_reader();
    }
private:
    future<> close_reader() noexcept {
        if (_reader) {
            co_await _reader->close();
            _reader.reset();
        }
    }
};

std::unique_ptr<sstables::object_storage_page_cache::page_cursor>
object_storage_page_cache_table::read_pages(sstables::sstable_id sst, sstables::component_type component,
        uint64_t from, uint64_t to, sstables::populate_disk_cache populate) {
    return std::make_unique<page_cursor_impl>(*this, sst, component, from, to, populate);
}

future<std::vector<sstables::object_storage_page_cache::page>>
object_storage_page_cache_table::get_pages(sstables::sstable_id sst, sstables::component_type component,
        uint64_t from, uint64_t to, size_t max_pages, sstables::populate_disk_cache populate) {
    auto component_name = fmt::to_string(component);
    cached_pages_reader reader(_db, sst, component_name, from, to, "object_storage_page_cache_get_pages");
    std::vector<page> pages;
    std::exception_ptr ex;
    try {
        while (pages.size() < max_pages) {
            auto p = co_await reader.next();
            if (!p) {
                break;
            }
            pages.push_back(std::move(*p));
        }
    } catch (...) {
        ex = std::current_exception();
    }
    co_await reader.close();
    if (ex) {
        std::rethrow_exception(ex);
    }
    if (populate && !pages.empty()) {
        std::vector<int64_t> offsets;
        for (const auto& p : pages) {
            offsets.push_back(int64_t(p.offset));
        }
        co_await record_hits(sst, component, component_name, std::move(offsets));
    }
    co_return pages;
}

future<> object_storage_page_cache_table::record_hits(sstables::sstable_id sst, sstables::component_type component,
        const sstring& component_name, std::vector<int64_t> offsets) {
    auto operation = _operations.try_hold();
    if (!operation) {
        // The cache was drained (see drain()). Not recording a hit only
        // makes the choice of pages to evict less accurate.
        co_return;
    }
    // If the hand evicts a page after it was read, recording its hit leaves
    // a row without a page and without a size, which the hand evicts once
    // its hit is older than the hand's cutoff.
    auto now = db_clock::now();
    std::erase_if(offsets, [&] (int64_t offset) {
        return hit_in_pass(page_key{sst.uuid(), component, uint64_t(offset)});
    });
    if (_record_hits_in_background && (_hit_writes_in_progress + offsets.size() > max_hit_writes_in_progress
            || _hit_writes.is_closed())) {
        // Too many hits are being recorded: skip these, which only makes
        // the choice of pages to evict less accurate. They aren't
        // remembered as recorded, so their next hits are recorded.
        _stats.unrecorded_hits += offsets.size();
        offsets.clear();
    }
    for (auto offset : offsets) {
        remember_hit(page_key{sst.uuid(), component, uint64_t(offset)});
    }
    if (offsets.empty()) {
        // Nothing to record.
    } else if (_record_hits_in_background) {
        _hit_writes_in_progress += offsets.size();
        auto n = offsets.size();
        // The count is decremented inside the gate, because stop() may
        // destroy the cache as soon as the gate is closed.
        (void)with_gate(_hit_writes, [this, sst, component_name, offsets = std::move(offsets), now, n] () mutable {
            return write_hits(sst.uuid(), std::move(component_name), std::move(offsets), now).finally([this, n] {
                _hit_writes_in_progress -= n;
            });
        });
    } else {
        co_await write_hits(sst.uuid(), component_name, std::move(offsets), now);
    }
}

future<std::vector<sstables::object_storage_page_cache::page_hits>>
object_storage_page_cache_table::get_page_hits(sstables::sstable_id sst, sstables::component_type component,
        uint64_t from, size_t max_pages) {
    utils::get_local_injector().inject("object_storage_page_cache_get_page_hits", [] {
        throw std::runtime_error("object_storage_page_cache_get_page_hits: injected error");
    });
    // Read directly from the table, like cached_pages_reader: compaction asks for
    // the hits of its input sstables while it reads them, holding its permit.
    auto rs = co_await query_component(_db, PAGECACHE_HITS, sst, fmt::to_string(component),
            from, std::numeric_limits<int64_t>::max(), max_pages, "object_storage_page_cache_get_page_hits");
    std::vector<page_hits> ret;
    ret.reserve(rs.rows().size());
    for (const auto& row : rs.rows()) {
        ret.push_back(page_hits{
            .offset = uint64_t(row.get_nonnull<int64_t>("offset")),
            .size = uint64_t(std::max(0, row.get<int32_t>("size").value_or(0))),
            .last_hit = row.get<db_clock::time_point>("last_hit"),
        });
    }
    co_return ret;
}

future<> object_storage_page_cache_table::write_hits(utils::UUID sstable, sstring component, std::vector<int64_t> offsets,
        db_clock::time_point now) noexcept {
    // Recording the hits is only for choosing which pages to evict, so a
    // failure to record them doesn't fail anything.
    try {
        co_await utils::get_local_injector().inject("object_storage_page_cache_write_hits",
                utils::wait_for_message(std::chrono::minutes(5)), false);
        co_await coroutine::parallel_for_each(offsets, [&] (int64_t offset) -> future<> {
            co_await apply(hits_mutation(this_shard(), sstable, component, offset, std::nullopt, now));
        });
    } catch (...) {
        static thread_local logging::logger::rate_limit rate_limit{std::chrono::minutes(1)};
        _stats.hit_recording_errors++;
        pclogger.log(log_level::warn, rate_limit, "Failed to record hits in the object storage page cache: {}", std::current_exception());
    }
}

// The filter of pages hit in a pass (see _hit_filter) has 10 bits per page,
// and 7 hash functions, the best number for 10 bits per page: about 0.8% of
// the pages which it doesn't hold seem to be in it, when it holds as many
// pages as it was sized for.
static constexpr uint64_t hit_filter_bits_per_page = 10;
static constexpr unsigned hit_filter_hashes = 7;

static size_t hit_filter_words(uint64_t pages) {
    return (pages * hit_filter_bits_per_page + 63) / 64;
}

uint64_t object_storage_page_cache_table::hit_filter_pages() const noexcept {
    // A page is usually at most 4 KiB: a compressed chunk, 4 KiB by default
    // before compression, or a 4 KiB page of an uncompressed component. This
    // assumes that pages are 2 KiB on average - if they are smaller, the
    // filter fills before the cache does, and the hits of the pages beyond
    // it are recorded every time. A cache without a limit is sized only by
    // object_storage_page_cache_max_remembered_hits.
    //
    // It is sized for at least 64K pages: a smaller filter would save little
    // memory (64K pages take 80 KB), and a small cache, which would fill a
    // filter sized for it, hardly fills this one, which then misjudges almost
    // none of its pages.
    constexpr uint64_t average_page_size = 2048;
    constexpr uint64_t min_pages = 64 * 1024;
    auto pages = limit() ? std::max(limit() / average_page_size + 1, min_pages) : std::numeric_limits<uint64_t>::max();
    return std::min(pages, _max_remembered_hits());
}

void object_storage_page_cache_table::reset_hit_filter_now() {
    auto pages = hit_filter_pages();
    _hit_filter.clear();
    _hit_filter.resize(hit_filter_words(pages));
    _hit_filter_pages = pages;
    _hit_filter_size = 0;
    ++_hit_filter_seed;
}

future<> object_storage_page_cache_table::reset_hit_filter() {
    // The filter is emptied first: while the new one is filled, hits are
    // recorded, and the pages aren't remembered.
    _hit_filter.clear();
    _hit_filter_pages = 0;
    _hit_filter_size = 0;
    ++_hit_filter_seed;
    auto pages = hit_filter_pages();
    auto words = hit_filter_words(pages);
    utils::chunked_vector<uint64_t> filter;
    co_await utils::reserve_gently(filter, words);
    while (filter.size() < words) {
        filter.resize(std::min(words, filter.size() + filter.max_chunk_capacity()));
        co_await coroutine::maybe_yield();
    }
    _hit_filter = std::move(filter);
    _hit_filter_pages = pages;
}

// Calls func with each of the page's bits in a filter of the given number of
// bits, by double hashing - the i-th hash function is h1 + i * h2 - until
// func returns false. Returns whether it never did.
template <typename Func>
static bool for_each_hit_filter_bit(utils::UUID sstable, sstables::component_type component, uint64_t offset,
        uint64_t seed, size_t bits, Func func) {
    std::array<uint64_t, 4> key{uint64_t(sstable.get_most_significant_bits()), uint64_t(sstable.get_least_significant_bits()),
            uint64_t(component), offset};
    std::array<uint64_t, 2> h;
    utils::murmur_hash::hash3_x64_128(bytes_view(reinterpret_cast<const int8_t*>(key.data()), sizeof(key)), seed, h);
    // The number of bits is a multiple of 64, so an even h2 would reach
    // fewer of them.
    h[1] |= 1;
    for (unsigned i = 0; i < hit_filter_hashes; i++) {
        if (!func((h[0] + i * h[1]) % bits)) {
            return false;
        }
    }
    return true;
}

bool object_storage_page_cache_table::hit_in_pass(const page_key& key) const noexcept {
    if (!_hit_filter_size || !_max_remembered_hits()) {
        return false;
    }
    return for_each_hit_filter_bit(key.sstable, key.component, key.offset, _hit_filter_seed, _hit_filter.size() * 64,
            [this] (size_t bit) { return bool((_hit_filter[bit / 64] >> (bit % 64)) & 1); });
}

void object_storage_page_cache_table::remember_hit(const page_key& key) noexcept {
    if (_hit_filter_size >= _hit_filter_pages) {
        return;
    }
    for_each_hit_filter_bit(key.sstable, key.component, key.offset, _hit_filter_seed, _hit_filter.size() * 64,
            [this] (size_t bit) { _hit_filter[bit / 64] |= uint64_t(1) << (bit % 64); return true; });
    ++_hit_filter_size;
}

future<> object_storage_page_cache_table::evict_sstable(sstables::sstable_id sst) {
    static const auto rows_query = fmt::format("SELECT component, offset, size, last_hit FROM {}.{} WHERE shard = ? AND sstable = ?",
            SYSTEM_CACHE, PAGECACHE_HITS);
    // An sstable may have very many cached pages, so the rows are held in a
    // chunked_vector, which doesn't need one large contiguous allocation.
    utils::chunked_vector<hits_row> rows;
    auto operation = _operations.try_hold();
    if (!operation) {
        // The cache was drained (see drain()). The sstable's pages are
        // evicted on the next startup, with the other orphans (see
        // evict_other_sstables()).
        co_return;
    }
    auto count_lock = co_await get_shared_lock(_count_lock);
    _sstables_being_evicted[sst.uuid()];
    auto eviction_done = defer([this, sst] () noexcept { _sstables_being_evicted.erase(sst.uuid()); });
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
    // All the rows are of the same partition, so it is compared with the
    // hand's once, rather than for every row.
    auto end = fetched_end();
    auto end_partitions = end ? compare_partitions(this_shard(), sst.uuid(), *end) : std::strong_ordering::equal;
    // The pages which the hand evicted since the rows were read, and the
    // page which it is evicting, are subtracted by the hand - and it never
    // counted them on its current pass.
    const auto& evicted_by_hand = _sstables_being_evicted[sst.uuid()];
    size_t evicted_here = 0;
    for (const auto& row : rows) {
        if (evicted_by_hand.contains({row.key.component, row.key.offset}) || _page_being_evicted == row.key) {
            continue;
        }
        bytes += row.size;
        evicted_here++;
        if (!end || !at_or_before(*end, end_partitions, row.key.component, row.key.offset)
                || in_batch.contains({row.key.component, row.key.offset})) {
            continue;
        }
        _pass_bytes -= std::min<uint64_t>(_pass_bytes, row.size);
        add_to_histogram(_pass_ages, row.last_hit, -int64_t(row.size));
    }
    // The evicted pages are no longer in the window, nor written during the
    // fetch of a batch.
    auto evicted = [&] (const written_page& page) {
        return page.key.sstable == sst.uuid() && page.key.shard == this_shard();
    };
    std::erase_if(_window, evicted);
    std::erase_if(_written_during_fetch, evicted);
    _bytes -= std::min(_bytes, bytes);
    _stats.sstable_evictions += evicted_here;
}

future<> object_storage_page_cache_table::evict_other_sstables(const std::unordered_set<sstables::sstable_id>& live) {
    auto operation = _operations.try_hold();
    if (!operation) {
        // The cache was drained (see drain()). The orphans are evicted on
        // the next startup.
        co_return;
    }
    // The queries read only this shard's partitions - including, on the last
    // shard, those of shards which no longer exist, whose pages are of no
    // use to any shard (see this_shard_token_range()).
    auto range = this_shard_token_range();
    auto first = range.start()->value().raw();
    auto last = range.end()->value().raw();
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
    // The deletions don't subtract the orphans' pages from _bytes, so they
    // don't wait for a count which may be in progress (see _count_lock): the
    // background fiber recounts the cache after them, and its count replaces
    // any earlier one. It does so in the background because a count scans
    // all of the shard's pagecache_hits, and this function runs while the
    // node starts.
    _recount_needed = true;
    _eviction_needed.signal();
}

future<> object_storage_page_cache_table::copy_sstable_from_shard(sstables::sstable_id sst, shard_id from) {
    auto operation = _operations.try_hold();
    if (!operation) {
        // The cache was drained (see drain()). The pages which aren't copied
        // will be read again from the object store.
        co_return;
    }
    // See _sstables_being_copied. Only a page whose eviction already started
    // can be evicted during the copy, so it isn't copied - which is harmless.
    _sstables_being_copied.insert(sst.uuid());
    auto copy_done = defer([this, sst] () noexcept { _sstables_being_copied.erase(sst.uuid()); });
    auto evicting = _page_being_evicted;
    auto being_evicted = [&] (const sstring& component, int64_t offset) {
        return evicting && *evicting == hand_position{this_shard(), sst.uuid(), component, offset};
    };
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
        if (being_evicted(component, offset)) {
            co_return stop_iteration::no;
        }
        co_await apply(hits_mutation(this_shard(), sst.uuid(), component, offset, size, row.get_opt<db_clock::time_point>("last_hit")));
        _bytes += size;
        _bytes_added += size;
        count_written_page(this_shard(), sst.uuid(), component, offset, size, row.get_opt<db_clock::time_point>("last_hit"));
        co_return stop_iteration::no;
    });
    static const auto select = fmt::format("SELECT component, offset, page FROM {}.{} WHERE shard = ? AND sstable = ?",
            SYSTEM_CACHE, PAGECACHE);
    // A page may be as large as a compressed chunk, but the query's paging
    // limits each of its pages of results to the page size in bytes (1 MiB
    // by default), so many cached pages can be read at a time.
    co_await _qp.query_internal(select, db::consistency_level::ONE, {int16_t(from), sst.uuid()},
            cql3::query_processor::default_internal_page_size,
            [&] (const cql3::untyped_result_set_row& row) -> future<stop_iteration> {
        if (being_evicted(row.get_as<sstring>("component"), row.get_as<int64_t>("offset"))) {
            co_return stop_iteration::no;
        }
        co_await utils::get_local_injector().inject("object_storage_page_cache_copy_page",
                utils::wait_for_message(std::chrono::minutes(5)), false);
        co_await apply(page_mutation(this_shard(), sst.uuid(), row.get_as<sstring>("component"), row.get_as<int64_t>("offset"),
                row.get_blob_unfragmented("page")));
        _pages_written = true;
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

future<> object_storage_page_cache_table::recount_cache() {
    auto range = this_shard_token_range();
    auto first = range.start()->value().raw();
    auto last = range.end()->value().raw();
    static const auto query = fmt::format("SELECT size, last_hit FROM {}.{} "
            "WHERE token(shard, sstable) >= ? AND token(shard, sstable) <= ?", SYSTEM_CACHE, PAGECACHE_HITS);
    // Pages evicted during the count would be subtracted from _bytes, which
    // the count then replaces - with a count which may include them. So the
    // evictions wait for the count (see _count_lock). It only reads
    // pagecache_hits, so it doesn't wait for them in turn.
    auto count_lock = co_await get_unique_lock(_count_lock);
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
    co_await utils::get_local_injector().inject("object_storage_page_cache_counted",
            utils::wait_for_message(std::chrono::minutes(5)), false);
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
    // Not even all the counted pages are enough: more pages were cached since
    // they were counted - for example, when the shard started with an empty
    // cache, counted it, and then filled it. So evict all the counted pages,
    // which were last hit when they were counted or before, and the pages
    // which were never hit - but keep the pages which were hit since. (A page
    // is evicted if its last hit is before the cutoff, and the times have a
    // resolution of a millisecond.) A cutoff which evicted every page would
    // also evict the pages which were hit, until the hand passed all of the
    // cache.
    return _ages.counted_at + std::chrono::milliseconds(1);
}

void object_storage_page_cache_table::start(scheduling_group sg) {
    register_metrics();
    record_hits_in_background();
    _evictor = with_scheduling_group(sg, [this] { return evict_in_background(); });
}

void object_storage_page_cache_table::register_metrics() {
    namespace sm = seastar::metrics;
    auto reads = sm::label("reads");
    auto reason = sm::label("reason");
    auto operation = sm::label("operation");
    _metrics.add_group("object_storage", {
        sm::make_counter("page_cache_hits", [this] { return _stats.populating.hits; },
                sm::description("Pages read from the object storage page cache, by reads which may populate it"), {reads("populating")}),
        sm::make_counter("page_cache_hits", [this] { return _stats.non_populating.hits; },
                sm::description("Pages read from the object storage page cache, by reads with BYPASS CACHE and background reads"), {reads("non_populating")}),
        sm::make_counter("page_cache_misses", [this] { return _stats.populating.misses; },
                sm::description("Pages read from the object store through the object storage page cache, by reads which may populate it"), {reads("populating")}),
        sm::make_counter("page_cache_misses", [this] { return _stats.non_populating.misses; },
                sm::description("Pages read from the object store through the object storage page cache, by reads with BYPASS CACHE and background reads"), {reads("non_populating")}),
        sm::make_gauge("page_cache_bytes", [this] { return _bytes; },
                sm::description("Bytes in the object storage page cache")),
        sm::make_gauge("page_cache_capacity_bytes", [this] { return limit(); },
                sm::description("The size limit of the object storage page cache, or 0 if unlimited")),
        sm::make_counter("page_cache_object_reads", [this] { return _stats.object_reads; },
                sm::description("Requests which reads through the object storage page cache sent to the object store")),
        sm::make_counter("page_cache_object_read_bytes", [this] { return _stats.object_read_bytes; },
                sm::description("Bytes which reads through the object storage page cache requested from the object store")),
        sm::make_counter("page_cache_evictions", [this] { return _stats.capacity_evictions; },
                sm::description("Pages evicted from the object storage page cache because it was full"), {reason("capacity")}),
        sm::make_counter("page_cache_evictions", [this] { return _stats.sstable_evictions; },
                sm::description("Pages evicted from the object storage page cache because their sstable was deleted"), {reason("sstable_deleted")}),
        sm::make_counter("page_cache_errors", [this] { return _stats.read_errors; },
                sm::description("Failures to read the object storage page cache, after which the object store was read instead"), {operation("read")}),
        sm::make_counter("page_cache_errors", [this] { return _stats.write_errors; },
                sm::description("Failures to write pages to the object storage page cache"), {operation("write")}),
        sm::make_counter("page_cache_errors", [this] { return _stats.hit_recording_errors; },
                sm::description("Failures to record hits of pages of the object storage page cache"), {operation("record_hits")}),
        sm::make_counter("page_cache_unrecorded_hits", [this] { return _stats.unrecorded_hits; },
                sm::description("Hits of pages of the object storage page cache which weren't recorded, because too many recordings of hits were in progress")),
        sm::make_counter("page_cache_errors", [this] { return _stats.eviction_errors; },
                sm::description("Failures to evict pages from the object storage page cache"), {operation("evict")}),
        sm::make_counter("page_cache_bypasses", [this] { return _stats.dup_bypasses; },
                sm::description("Index files of sstables which use the object storage page cache, duplicated for resharding, which read without it"), {reason("dup")}),
        sm::make_counter("page_cache_bypasses", [this] { return _stats.transform_bypasses; },
                sm::description("Reads of sstables which use the object storage page cache, transformed by a file I/O extension in a way which prevents it"), {reason("transform")}),
        sm::make_counter("page_cache_compaction_copies", [this] { return _stats.compaction_copied_pages; },
                sm::description("Pages which compaction wrote and copied to the object storage page cache, because their data was cached in its input sstables")),
        sm::make_counter("page_cache_compaction_copied_bytes", [this] { return _stats.compaction_copied_bytes; },
                sm::description("Bytes of the pages which compaction wrote and copied to the object storage page cache")),
        sm::make_counter("page_cache_errors", [this] { return _stats.compaction_errors; },
                sm::description("Failures of compaction to find which data of its input sstables was in the object storage page cache, or to copy pages to it"), {operation("compaction")}),
    });
}

future<> object_storage_page_cache_table::drain() noexcept {
    if (_stopping) {
        // Drained, or being drained, by an earlier call.
        co_return;
    }
    _metrics.clear();
    _stopping = true;
    _eviction_needed.broken();
    if (_evictor) {
        co_await std::exchange(_evictor, std::nullopt).value();
    }
    co_await _hit_writes.close();
    co_await _operations.close();
}

future<> object_storage_page_cache_table::stop() noexcept {
    // Nothing is left to do after drain(): the readers which may have used
    // the cache after it are gone, and they wrote nothing.
    co_await drain();
}

future<> object_storage_page_cache_table::evict_in_background() {
    // The cache is plugged before the system_cache keyspace is loaded, so
    // retry counting its bytes until it is.
    while (!_stopping) {
        bool counted = false;
        try {
            // A recount requested during this count may have deleted pages
            // which it counted, so it is done again.
            _recount_needed = false;
            co_await recount_cache();
            counted = true;
        } catch (...) {
            pclogger.debug("Failed to count the bytes in the object storage page cache, will retry: {}", std::current_exception());
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
            // Wake up at least once per flush period, to flush the tables if
            // needed - also while the cache is not full.
            try {
                co_await _eviction_needed.wait(flush_period, [this] {
                    return _stopping || _recount_needed || (limit() && _bytes > limit());
                });
            } catch (const condition_variable_timed_out&) {
            }
            if (!_stopping && std::exchange(_recount_needed, false)) {
                try {
                    co_await recount_cache();
                } catch (...) {
                    _recount_needed = true;
                    throw;
                }
            }
            if (!_stopping) {
                co_await evict_if_needed();
                co_await flush_if_needed();
            }
        } catch (const broken_condition_variable&) {
            break;
        } catch (...) {
            _stats.eviction_errors++;
            pclogger.warn("Failed to evict pages from the object storage page cache: {}", std::current_exception());
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

// The tables skip the commitlog, so a crash loses what their memtables hold.
// But eviction and the count of the cache's bytes find pages only through
// their rows in pagecache_hits, so a crash must not leave a page in pagecache
// without its row in pagecache_hits:
//
// * A page's row in pagecache_hits is written before the page (put_page()),
//   but when memtables take too much memory, the largest one is flushed -
//   almost always pagecache's - so pagecache_hits' rows could stay in memory
//   for long, while their pages are flushed.
// * Evicting a page deletes it, and then its row in pagecache_hits - and a
//   flush of pagecache_hits could persist the second deletion while the
//   first is still in pagecache's memtable.
//
// So the background fiber flushes the tables every flush_period: after pages
// were evicted, pagecache and then pagecache_hits - between evictions, so
// that every deletion persisted in pagecache_hits is persisted in pagecache;
// and after pages were written, pagecache_hits. Then a crash leaves without
// rows in pagecache_hits only the pages written in about the last
// flush_period before it. If nothing was written or evicted, nothing is
// flushed: a cache which isn't used doesn't write tiny sstables every
// minute, and a cache which isn't full, which evicts nothing, doesn't flush
// the large memtable of pagecache - only that of pagecache_hits, whose rows
// are small. Recorded hits don't require a flush: losing them is harmless.
future<> object_storage_page_cache_table::flush_if_needed(bool now) {
    if (!now && lowres_clock::now() - _last_flush < flush_period) {
        co_return;
    }
    _last_flush = lowres_clock::now();
    auto& db = _db;
    if (std::exchange(_pages_evicted, false)) {
        _pages_written = false;
        co_await db.find_column_family(SYSTEM_CACHE, PAGECACHE).flush();
        co_await db.find_column_family(SYSTEM_CACHE, PAGECACHE_HITS).flush();
    } else if (std::exchange(_pages_written, false)) {
        co_await db.find_column_family(SYSTEM_CACHE, PAGECACHE_HITS).flush();
    }
}

future<> object_storage_page_cache_table::evict_if_needed() {
    auto max = limit();
    if (!max || _bytes <= max) {
        co_return;
    }
    // Evict a little more than needed, so that the cache doesn't need to
    // evict again for every new page.
    auto count_lock = co_await get_shared_lock(_count_lock);
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
    // next_partitions continues after the token of the hand's partition, so
    // if two of the shard's sstables had partitions with the same token, the
    // hand would skip the second, and never evict its pages - they would be
    // evicted only with their sstable. The token has 48 bits of hash, so
    // with N sstables on a shard, the chance of this is about N^2 / 2^49:
    // negligible.
    static const auto rest_of_partition = fmt::format("SELECT component, offset, size, last_hit FROM {}.{} "
            "WHERE shard = ? AND sstable = ? AND (component, offset) > (?, ?) LIMIT ?", SYSTEM_CACHE, PAGECACHE_HITS);
    static const auto next_partitions = fmt::format("SELECT shard, sstable, component, offset, size, last_hit FROM {}.{} "
            "WHERE token(shard, sstable) > token(?, ?) AND token(shard, sstable) <= ? LIMIT ?", SYSTEM_CACHE, PAGECACHE_HITS);
    static const auto all_partitions = fmt::format("SELECT shard, sstable, component, offset, size, last_hit FROM {}.{} "
            "WHERE token(shard, sstable) >= ? AND token(shard, sstable) <= ? LIMIT ?", SYSTEM_CACHE, PAGECACHE_HITS);
    auto range = this_shard_token_range();
    auto first = range.start()->value().raw();
    auto last = range.end()->value().raw();

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
        auto query = [&] (const sstring& q, query_data_params values) -> future<::shared_ptr<cql3::untyped_result_set>> {
            auto rs = co_await _qp.execute_internal(q, std::move(values), cql3::query_processor::cache_internal::yes);
            // Lets tests cache pages after a query read the rows, but before
            // they are in the batch.
            co_await utils::get_local_injector().inject("object_storage_page_cache_fetching",
                    utils::wait_for_message(std::chrono::minutes(5)), false);
            co_return rs;
        };
        if (!_hand) {
            add(*co_await query(all_partitions, {first, last, batch_rows}), std::nullopt);
            co_return;
        }
        auto hand = *_hand;
        add(*co_await query(rest_of_partition, {hand.shard, hand.sstable, hand.component, hand.offset, batch_rows}),
                std::pair(hand.shard, hand.sstable));
        if (_batch.size() < size_t(batch_rows)) {
            add(*co_await query(next_partitions, {hand.shard, hand.sstable, last, int32_t(batch_rows - _batch.size())}),
                    std::nullopt);
        }
    };

    // Visits a page: evicts it if it wasn't hit since the cutoff, and stops
    // the hand when the cache is small enough.
    auto visit = [&] (const hits_row& row) -> future<stop_iteration> {
        // A long sweep also flushes the tables once in a while, between
        // evictions (see flush_if_needed()).
        co_await flush_if_needed();
        _hand = row.key;
        if (_pages_being_put.contains(row.key)) {
            // The page is being cached (see _pages_being_put), so it isn't
            // evicted - and put_page() counts it when it is written, because
            // the hand has reached it.
        } else if (_sstables_being_copied.contains(row.key.sstable)) {
            // The page is being copied (see _sstables_being_copied), so it
            // isn't evicted. Its row was counted when it was copied only if
            // the hand had fetched beyond it (see count_written_page()), so
            // the hand counts it now.
            _pass_bytes += row.size;
            add_to_histogram(_pass_ages, row.last_hit, row.size);
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
            co_await reset_hit_filter();
            ++passes;
            // Lets tests hit pages after the histogram, from which this
            // pass's cutoff is chosen, was built.
            co_await utils::get_local_injector().inject("object_storage_page_cache_pass_start",
                    utils::wait_for_message(std::chrono::minutes(5)), false);
            _pass_bytes = 0;
            _pass_ages = age_histogram{.counted_at = db_clock::now(), .buckets = std::vector<uint64_t>(age_buckets)};
            _window.clear();
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
                _window.clear();
                _fetching = true;
                {
                    auto fetch_done = defer([this] () noexcept {
                        _fetching = false;
                        _written_during_fetch.clear();
                    });
                    co_await fetch();
                    _fetching = false;
                    count_pages_written_during_fetch();
                }
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
            _window.clear();
            // Start a new pass.
            _hand.reset();
        }
    }
}

future<> object_storage_page_cache_table::evict_page(int16_t shard, utils::UUID sstable, const sstring& component, int64_t offset, int32_t size) {
    _pages_evicted = true;
    co_await apply(page_deletion(schema(), shard, sstable, component, offset));
    co_await utils::get_local_injector().inject("object_storage_page_cache_evict_page_between_deletions",
            utils::wait_for_message(std::chrono::minutes(5)), false);
    co_await apply(page_deletion(hits_schema(), shard, sstable, component, offset));
    _bytes -= std::min(_bytes, uint64_t(size));
    _stats.capacity_evictions++;
    if (auto it = _sstables_being_evicted.find(sstable); it != _sstables_being_evicted.end() && shard == this_shard()) {
        it->second.emplace(component, offset);
    }
}

} // namespace db
