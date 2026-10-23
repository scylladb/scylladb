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

#include <compare>
#include <deque>
#include <optional>
#include <set>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/shared_mutex.hh>
#include "db_clock.hh"
#include "mutation/timestamp.hh"
#include "schema/schema_fwd.hh"
#include "sstables/object_storage_page_cache.hh"
#include "utils/UUID.hh"
#include "utils/updateable_value.hh"

class mutation;

namespace cql3 {
class query_processor;
}

namespace db {

// The disk page cache of sstables on object storage, which is stored in
// the system_cache.pagecache table - and in the system_cache.pagecache_hits
// table, which records when each cached page was last hit.
//
// Each shard's cache is limited to its share of the configured size of the
// node's cache. When it grows beyond it, a background fiber evicts the
// pages which were least recently hit, approximately (see sweep()).
class object_storage_page_cache_table : public sstables::object_storage_page_cache {
    cql3::query_processor& _qp;
    // The size of the node's cache, in MiB. 0 means unlimited.
    utils::updateable_value<uint64_t> _size_in_mb;
    // The number of bytes in this shard's cache: the sum of the sizes of its
    // pages. It is counted in memory, as pages are cached and evicted, and
    // recounted whenever the hand which evicts pages completes a pass -
    // because caching a page which was already cached counts it again.
    uint64_t _bytes = 0;
    // The number of bytes ever added to this shard's cache, for recounting
    // its bytes while pages are added (see count_bytes()).
    uint64_t _bytes_added = 0;
    // Held by count_bytes(), exclusively, and by the evictions which
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

    // The background fiber which evicts pages when the cache is too large.
    condition_variable _eviction_needed;
    std::optional<future<>> _evictor;
    bool _stopping = false;
    // Wakes up the background fiber when the cache's size limit changes - if
    // it was lowered, the cache may need to evict pages now.
    utils::observer<uint64_t> _size_observer;

    // Whether pages were written to the tables, or evicted from them, since
    // the background fiber last flushed them (see flush_if_needed()), and
    // when it did.
    bool _pages_written = false;
    bool _pages_evicted = false;
    lowres_clock::time_point _last_flush = lowres_clock::now();
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

    object_storage_page_cache_table(cql3::query_processor& qp, utils::updateable_value<uint64_t> size_in_mb);

    // Starts and stops the background fiber which evicts pages when this
    // shard's cache is too large, which runs in the given scheduling group.
    void start(scheduling_group sg);
    future<> stop() noexcept;

    virtual future<> put_page(sstables::sstable_id sst, sstables::component_type component,
            uint64_t offset, temporary_buffer<char> data) override;
    virtual future<std::vector<page>> get_pages(sstables::sstable_id sst, sstables::component_type component,
            uint64_t from, uint64_t to, size_t max_pages, sstables::populate_disk_cache populate) override;
    virtual future<> evict_sstable(sstables::sstable_id sst) override;
    virtual future<> evict_other_sstables(const std::unordered_set<sstables::sstable_id>& live) override;
    virtual future<> copy_sstable_from_shard(sstables::sstable_id sst, shard_id from) override;

    // The number of bytes in this shard's cache.
    uint64_t cached_bytes() const noexcept {
        return _bytes;
    }
    // Recounts the bytes in this shard's cache, and how long ago they were
    // last hit.
    future<> count_bytes();
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
    future<> evict_in_background();
    // Moves the hand, evicting pages, until the cache holds at most "target"
    // bytes.
    future<> sweep(uint64_t target);
    // Adds a page to a histogram, or removes it.
    static void add_to_histogram(age_histogram& h, std::optional<db_clock::time_point> last_hit, int64_t size);
    future<> evict_page(int16_t shard, utils::UUID sstable, const sstring& component, int64_t offset, int32_t size);
    future<> apply(mutation m);
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
