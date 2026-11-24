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

#include <compare>
#include <deque>
#include <optional>
#include <set>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/shared_mutex.hh>
#include "db/cluster_config_manager.hh"
#include "db_clock.hh"
#include "mutation/timestamp.hh"
#include "schema/schema_fwd.hh"
#include "sstables/object_storage_page_cache.hh"
#include "utils/UUID.hh"
#include "utils/chunked_vector.hh"
#include "utils/updateable_value.hh"

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
//
// Each shard's cache is limited to its share of the configured size of the
// node's cache. When it grows beyond it, a background fiber evicts the
// pages which were least recently hit, approximately (see sweep()).
class object_storage_page_cache_table : public sstables::object_storage_page_cache {
    // The cursor which read_pages() returns.
    class page_cursor_impl;
    // The database, which holds the cache's tables: reads and writes of
    // pages go to it directly. The query processor executes the internal
    // queries of eviction, and of the operations on whole sstables, which
    // drain() waits for and disables - because the query processor stops
    // before the database, and before the readers which use the cache are
    // gone (see drain()).
    replica::database& _db;
    cql3::query_processor& _qp;
    // The size of the node's cache, in MiB. 0 means unlimited.
    utils::updateable_value<uint64_t> _size_in_mb;
    // The number of bytes in this shard's cache: the sum of the sizes of its
    // pages. It is counted in memory, as pages are cached and evicted, and
    // recounted whenever the hand which evicts pages completes a pass -
    // because caching a page which was already cached counts it again.
    uint64_t _bytes = 0;
    // The number of bytes ever added to this shard's cache, for recounting
    // its bytes while pages are added (see recount_cache()).
    uint64_t _bytes_added = 0;
    // Held by recount_cache(), exclusively, and by the evictions which
    // subtract the pages they evict from _bytes - the hand's and
    // evict_sstable()'s - shared: a page which the count found, and which
    // was evicted before the count ended, would stay counted.
    shared_mutex _count_lock;

    // The position of the hand which evicts pages (see sweep()): the key of
    // the last page which it passed, or nothing if it starts a new pass, at
    // the beginning of this shard's cache.
    struct hand_position {
        int16_t shard;
        utils::UUID sstable;
        sstring component;
        int64_t offset;
        bool operator==(const hand_position&) const = default;
    };
    struct hand_position_hash {
        size_t operator()(const hand_position& k) const noexcept;
    };
    std::optional<hand_position> _hand;
    // The rows of pagecache_hits which the hand fetched, in a batch, and
    // didn't pass yet - and the sstables which were evicted since, whose
    // rows in the batch the hand skips.
    struct hits_row {
        hand_position key;
        int32_t size;
        std::optional<db_clock::time_point> last_hit;
    };
    std::deque<hits_row> _batch;
    std::unordered_set<utils::UUID> _evicted_in_batch;
    // The sstables which evict_sstable() is evicting, with the pages of each
    // which the hand evicted meanwhile - which evict_sstable() doesn't
    // subtract from the count of bytes again.
    std::unordered_map<utils::UUID, std::set<std::pair<sstring, int64_t>>> _sstables_being_evicted;
    // The bytes of the pages which the hand passed on its current pass and
    // didn't evict, and of the pages cached, during the pass, at keys which
    // the hand won't reach on this pass - because it already passed them,
    // or they are within its batch: when the hand completes the pass, they
    // are the bytes in the cache.
    uint64_t _pass_bytes = 0;
    // How many bytes of this shard's cache were last hit how long ago, for
    // choosing the cutoff of the hand's next pass. Bucket i holds the pages
    // whose age, when the histogram was started, was at least
    // age_bucket_start(i) milliseconds, and less than the start of the next
    // bucket. The buckets grow exponentially, so they need few counters for
    // all ages, with the same relative precision.
    struct age_histogram {
        db_clock::time_point counted_at;
        uint64_t never_hit = 0;
        std::vector<uint64_t> buckets;
    };
    // The histogram of the pages, as of the start of the hand's current
    // pass (or of the count when the node started), and the histogram of
    // the pages which the hand keeps on its current pass - which becomes
    // the former when the hand completes the pass.
    age_histogram _ages;
    age_histogram _pass_ages;
    // A page written during the hand's pass, which _pass_bytes counts,
    // because the hand won't see it.
    struct written_page {
        hand_position key;
        int32_t size;
        std::optional<db_clock::time_point> last_hit;
    };
    // The pages which were cached during the pass after the hand's position
    // but within its batch, which _pass_bytes and _pass_ages count: if the
    // hand drops the rest of its batch, it fetches again those which are
    // still after it, so it stops counting them.
    std::vector<written_page> _window;
    // Whether the hand is fetching a batch, and the pages which were cached,
    // after the hand, while it did: the batch may or may not have them, so
    // they are counted, or not, when the batch arrives (see
    // count_pages_written_during_fetch()).
    bool _fetching = false;
    std::vector<written_page> _written_during_fetch;
    // On its current pass, the hand evicts the pages which were never hit,
    // and those last hit before this time (see sweep()).
    db_clock::time_point _cutoff = db_clock::time_point::min();

    // The pages whose hits were recorded since the hand started its current
    // pass. Another hit of such a page isn't recorded, to save writes: a
    // page which is read many times is written at most once per pass of the
    // hand. So a page's recorded last hit is its first hit in the last pass
    // in which it was hit, which makes the order in which the hand evicts
    // pages accurate only to within a pass.
    //
    // They are remembered in a Bloom filter, of 10 bits per page with 7 hash
    // functions - about 1.2 bytes per page, rather than tens of bytes for a
    // set of the pages' keys - so it remembers all the pages hit in a pass,
    // not just a few of them. But about 1% of the pages which it doesn't
    // hold seem to be in it: the first hit of such a page in a pass isn't
    // recorded, so the page keeps the last hit it had - from an earlier
    // pass, or none - and the hand may evict it a pass earlier than it
    // should. The filter is emptied, with a new seed, when the hand starts a
    // pass, so the same pages aren't misjudged on every pass. Its bits are
    // in a chunked_vector, which allocates small chunks, not one large array
    // - which may fail or stall when memory is fragmented.
    //
    // The filter is sized for as many pages as the cache can hold (see
    // hit_filter_pages()), up to object_storage_page_cache_max_remembered_hits
    // pages. When it holds that many, more pages aren't added to it, which
    // would make it misjudge more pages: their hits are recorded every time.
    // A Bloom filter can't forget a page, so a page which is cached while it
    // is in the filter - cached again after it was evicted, say - is written
    // with a hit now (see do_put_page()), rather than with its next hit.
    struct page_key {
        utils::UUID sstable;
        sstables::component_type component;
        uint64_t offset;
    };
    utils::chunked_vector<uint64_t> _hit_filter;
    // How many pages the filter is sized for, how many it holds, and the
    // seed of its hash functions.
    uint64_t _hit_filter_pages = 0;
    uint64_t _hit_filter_size = 0;
    uint64_t _hit_filter_seed = 0;
    // The most pages which the filter remembers, to limit its memory. 0
    // means to record every hit.
    utils::updateable_value<uint64_t> _max_remembered_hits;
    // The cluster config manager, from which caches_flushes() reads the
    // object_storage_page_cache_populate_on_flush option of each table, while
    // it is plugged in (see plug_cluster_config()).
    cluster_config_manager* _cluster_config = nullptr;
    // Logs each table's changes of the option, on shard 0: the tables for
    // which it is true, by keyspace and name - so that a table which never
    // set it, as most don't, isn't logged.
    std::optional<cluster_config_manager::config_callback_registration> _populate_on_flush_logger;
    std::set<std::pair<sstring, sstring>> _tables_caching_flushes;

    // The keys of the pages which put_page() is writing, with how many writes
    // of each are in progress, and the key of the page which the hand is
    // evicting. put_page() writes a page's row in pagecache_hits and then the
    // page, and the hand deletes both, so if the two interleaved, the page
    // could be left without its row in pagecache_hits, through which eviction
    // and the count of the cache's bytes find it. So the hand doesn't evict a
    // page which is being written - it was just cached anyway - and
    // put_page() doesn't write a page which is being evicted.
    std::unordered_map<hand_position, size_t, hand_position_hash> _pages_being_put;
    std::optional<hand_position> _page_being_evicted;
    // The sstables whose pages copy_sstable_from_shard() is copying to this
    // shard. It copies all of an sstable's rows in pagecache_hits, and then
    // all of its pages, so the hand doesn't evict an sstable's pages while
    // they are being copied, for the same reason as above.
    std::unordered_set<utils::UUID> _sstables_being_copied;

    // Whether hits are recorded in the background (see
    // record_hits_in_background()), the background writes of hits, and how
    // many of them are in progress, which is limited.
    bool _record_hits_in_background = false;
    gate _hit_writes;
    size_t _hit_writes_in_progress = 0;
    static constexpr size_t max_hit_writes_in_progress = 1000;

    // The background fiber which evicts pages when the cache is too large.
    condition_variable _eviction_needed;
    std::optional<future<>> _evictor;
    bool _stopping = false;
    // Whether the background fiber should recount the cache (see
    // recount_cache()), because evict_other_sstables() deleted pages without
    // counting them.
    bool _recount_needed = false;
    // The writes of pages and hits, and the operations on whole sstables,
    // which drain() waits for - and after which they do nothing.
    gate _operations;
    // Wakes up the background fiber when the cache's size limit changes - if
    // it was lowered, the cache may need to evict pages now.
    utils::observer<uint64_t> _size_observer;

    // Whether pages were written to the tables, or evicted from them, since
    // the background fiber last flushed them (see flush_if_needed()), and
    // when it did.
    bool _pages_written = false;
    bool _pages_evicted = false;
    lowres_clock::time_point _last_flush = lowres_clock::now();

    seastar::metrics::metric_groups _metrics;
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
    // How often the background fiber flushes the tables, if pages were
    // written or evicted (see flush_if_needed()).
    static constexpr auto flush_period = std::chrono::minutes(1);

    object_storage_page_cache_table(replica::database& db, cql3::query_processor& qp, utils::updateable_value<uint64_t> size_in_mb,
            utils::updateable_value<uint64_t> max_remembered_hits);

    // The cache's life cycle: start() starts the background fiber which
    // evicts pages when this shard's cache is too large, which runs in the
    // given scheduling group, and makes the cache record hits in the
    // background. It is called before the cache is given to any sstable.
    //
    // On shutdown, drain() is called before the query processor stops. It
    // stops the background fiber, waits for the writes of pages and hits
    // and for the operations on whole sstables which are in progress, and
    // disables them: after it, the cache doesn't cache pages, record hits,
    // evict sstables or copy them between shards. But readers which still
    // hold the cache - the sstables are released only later - can read the
    // pages already in it, from the database.
    //
    // stop() is called after all the sstables, and so all their readers,
    // were released, and before the database stops - and before the cache
    // is destroyed. It drains the cache, if it wasn't drained.
    void start(scheduling_group sg);
    future<> drain() noexcept;
    future<> stop() noexcept;
    // Makes the cursors of read_pages() (and get_pages()) record the hits of
    // the pages which they return in the background, rather than wait for
    // their writes: a read which finds its pages in the cache shouldn't wait
    // for writes - which wait when memtables take too much memory. drain()
    // waits for the writes in progress. Called by start(), and by tests.
    void record_hits_in_background() noexcept {
        _record_hits_in_background = true;
    }

    virtual future<> put_page(sstables::sstable_id sst, sstables::component_type component,
            uint64_t offset, temporary_buffer<char> data) override;
    virtual future<> put_copied_page(sstables::sstable_id sst, sstables::component_type component,
            uint64_t offset, temporary_buffer<char> data, std::optional<db_clock::time_point> last_hit) override;
    virtual future<std::vector<page_hits>> get_page_hits(sstables::sstable_id sst, sstables::component_type component,
            uint64_t from, size_t max_pages) override;
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
    // Whether the table's object_storage_page_cache_populate_on_flush option,
    // which may be set for the table, its keyspace or the cluster, is true.
    // False while the cluster config manager isn't plugged in - while the
    // node starts, before the manager does, and while it stops.
    virtual bool caches_flushes(const ::schema& s) const noexcept override;
    // Plugs in the cluster config manager, after it starts, and unplugs it,
    // before it stops.
    void plug_cluster_config(cluster_config_manager& ccm);
    future<> unplug_cluster_config() noexcept;
    // In the middle of the order in which pages are evicted, nearer to its
    // end: just after the pages which were never hit, or last hit longest
    // ago, which make up a quarter of the cache - so the hand evicts the
    // flushed pages soon, unless they are hit, but not before the pages which
    // were never hit (see sweep()). If the pages which were never hit are a
    // quarter of the cache, the flushed pages join them.
    virtual std::optional<db_clock::time_point> flushed_page_last_hit() const override;

    // The number of bytes in this shard's cache.
    uint64_t cached_bytes() const noexcept {
        return _bytes;
    }
    // Recounts the bytes in this shard's cache, and rebuilds the histogram of
    // how long ago they were last hit, by scanning all of the shard's rows of
    // pagecache_hits - which may take minutes in a large cache. The
    // background fiber does it when the node starts, and when requested (see
    // _recount_needed).
    future<> recount_cache();
    // The cutoff for evicting at least "to_free" bytes from this shard's
    // cache - first the pages which were never hit, and then those which
    // were last hit longest ago - according to the histogram of the pages
    // as of the start of the hand's current pass.
    db_clock::time_point eviction_cutoff(uint64_t to_free) const;
    // The number of buckets of an age_histogram, the start of bucket i, in
    // milliseconds, and the bucket of the given age.
    static constexpr size_t age_buckets = 1 + 4 * 48;
    static int64_t age_bucket_start(size_t i);
    static size_t age_bucket(int64_t age_ms);
    // If this shard's cache is larger than its limit, evicts pages until it
    // is a little smaller than it, so that it doesn't need to evict again
    // immediately. Called by the background fiber, and by tests.
    future<> evict_if_needed();
    // Flushes this shard's memtables of the tables, if pages were written to
    // them or evicted from them since they were last flushed (see the
    // comment in the .cc file). Called by the background fiber, at most once
    // per flush_period - unless "now" - and by tests.
    future<> flush_if_needed(bool now = false);
private:
    // This shard's limit, in bytes, or 0 if the cache is unlimited.
    uint64_t limit() const noexcept;
    void register_metrics();
    future<> evict_in_background();
    // Moves the hand, evicting pages, until the cache holds at most "target"
    // bytes.
    future<> sweep(uint64_t target);
    // Adds a page to a histogram, or removes it.
    static void add_to_histogram(age_histogram& h, std::optional<db_clock::time_point> last_hit, int64_t size);
    future<> evict_page(int16_t shard, utils::UUID sstable, const sstring& component, int64_t offset, int32_t size);
    // Writes the hits, at the given time, of the given pages of an sstable's
    // component. Never fails: a failure is only logged and counted.
    future<> write_hits(utils::UUID sstable, sstring component, std::vector<int64_t> offsets, db_clock::time_point now) noexcept;
    // Records that the given pages of an sstable's component were hit, in
    // the background if record_hits_in_background() was called - except
    // those whose hits were already recorded in the hand's current pass.
    future<> record_hits(sstables::sstable_id sst, sstables::component_type component,
            const sstring& component_name, std::vector<int64_t> offsets);
    // How many pages the filter of pages hit in a pass should be sized for.
    uint64_t hit_filter_pages() const noexcept;
    // Empties the filter of pages hit in a pass, and sizes it again. The
    // former yields while it fills the filter's bits; the latter doesn't.
    future<> reset_hit_filter();
    void reset_hit_filter_now();
    // Whether the filter holds the page, and adds it to the filter.
    bool hit_in_pass(const page_key& key) const noexcept;
    void remember_hit(const page_key& key) noexcept;
    future<> apply(mutation m);
    // Implements put_page() and put_copied_page(): writes the page's last
    // hit only if one is given.
    future<> do_put_page(sstables::sstable_id sst, sstables::component_type component,
            uint64_t offset, temporary_buffer<char> data, std::optional<db_clock::time_point> last_hit);
    // Whether the given page's key is at the given position, or before it,
    // in the order in which the hand passes the pages.
    static bool at_or_before(const hand_position& pos, int16_t shard, utils::UUID sstable, std::string_view component, int64_t offset);
    // The same, given how the page's partition compares with the position's
    // (see compare_partitions()) - for comparing many pages of a partition
    // with the same position.
    static bool at_or_before(const hand_position& pos, std::strong_ordering partitions, std::string_view component, int64_t offset);
    // How the partition of the given shard and sstable compares with the
    // given position's partition, in the order in which the hand passes them.
    static std::strong_ordering compare_partitions(int16_t shard, utils::UUID sstable, const hand_position& pos);
    // The last position which the hand reached on its current pass, or
    // fetched in its batch - a page cached up to it, during the pass, isn't
    // seen by the hand on this pass.
    std::optional<hand_position> fetched_end() const;
    // Counts a page written during the hand's pass (cached, or copied from
    // another shard), if the hand won't see it on this pass.
    void count_written_page(int16_t shard, utils::UUID sstable, std::string_view component, int64_t offset, int32_t size,
            std::optional<db_clock::time_point> last_hit);
    // Counts the pages which were cached while the hand fetched its batch,
    // which the hand won't see: those up to the end of the batch, which
    // aren't in it.
    void count_pages_written_during_fetch();
    // Drops the rest of the hand's batch, which it will fetch again.
    void drop_batch();
};

} // namespace db
