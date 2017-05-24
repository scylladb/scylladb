/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>

#include "core/memory.hh"
#include <seastar/core/thread.hh>

#include "mutation_reader.hh"
#include "mutation_partition.hh"
#include "utils/logalloc.hh"
#include "utils/phased_barrier.hh"
#include "utils/histogram.hh"
#include "partition_version.hh"
#include "utils/estimated_histogram.hh"
#include "tracing/trace_state.hh"
#include <seastar/core/metrics_registration.hh>

namespace bi = boost::intrusive;

class row_cache;

namespace cache {

class read_context;

}

// Intrusive set entry which holds partition data.
//
// TODO: Make memtables use this format too.
class cache_entry {
    // We need auto_unlink<> option on the _cache_link because when entry is
    // evicted from cache via LRU we don't have a reference to the container
    // and don't want to store it with each entry. As for the _lru_link, we
    // have a global LRU, so technically we could not use auto_unlink<> on
    // _lru_link, but it's convenient to do so too. We may also want to have
    // multiple eviction spaces in the future and thus multiple LRUs.
    using lru_link_type = bi::list_member_hook<bi::link_mode<bi::auto_unlink>>;
    using cache_link_type = bi::set_member_hook<bi::link_mode<bi::auto_unlink>>;

    schema_ptr _schema;
    dht::decorated_key _key;
    partition_entry _pe;
    // True when we know that there is nothing between this entry and the next one in cache
    struct {
        bool _continuous : 1;
        bool _dummy_entry : 1;
    } _flags{};
    lru_link_type _lru_link;
    cache_link_type _cache_link;
    friend class size_calculator;
public:
    friend class row_cache;
    friend class cache_tracker;

    struct dummy_entry_tag{};
    cache_entry(dummy_entry_tag)
        : _key{dht::token(), partition_key::make_empty()}
    {
        _flags._dummy_entry = true;
    }

    cache_entry(schema_ptr s, const dht::decorated_key& key, const mutation_partition& p)
        : _schema(std::move(s))
        , _key(key)
        , _pe(p)
    { }

    cache_entry(schema_ptr s, dht::decorated_key&& key, mutation_partition&& p) noexcept
        : _schema(std::move(s))
        , _key(std::move(key))
        , _pe(std::move(p))
    { }

    cache_entry(schema_ptr s, dht::decorated_key&& key, partition_entry&& pe) noexcept
        : _schema(std::move(s))
        , _key(std::move(key))
        , _pe(std::move(pe))
    { }

    cache_entry(cache_entry&&) noexcept;

    bool is_evictable() { return _lru_link.is_linked(); }
    const dht::decorated_key& key() const { return _key; }
    const partition_entry& partition() const { return _pe; }
    partition_entry& partition() { return _pe; }
    const schema_ptr& schema() const { return _schema; }
    schema_ptr& schema() { return _schema; }
    streamed_mutation read(row_cache&, cache::read_context& reader);
    bool continuous() const { return _flags._continuous; }
    void set_continuous(bool value) { _flags._continuous = value; }

    bool is_dummy_entry() const { return _flags._dummy_entry; }

    struct compare {
        dht::decorated_key::less_comparator _c;

        compare(schema_ptr s)
            : _c(std::move(s))
        {}

        bool operator()(const dht::decorated_key& k1, const cache_entry& k2) const {
            if (k2.is_dummy_entry()) {
                return true;
            }
            return _c(k1, k2._key);
        }

        bool operator()(const dht::ring_position& k1, const cache_entry& k2) const {
            if (k2.is_dummy_entry()) {
                return true;
            }
            return _c(k1, k2._key);
        }

        bool operator()(const cache_entry& k1, const cache_entry& k2) const {
            if (k1.is_dummy_entry()) {
                return false;
            }
            if (k2.is_dummy_entry()) {
                return true;
            }
            return _c(k1._key, k2._key);
        }

        bool operator()(const cache_entry& k1, const dht::decorated_key& k2) const {
            if (k1.is_dummy_entry()) {
                return false;
            }
            return _c(k1._key, k2);
        }

        bool operator()(const cache_entry& k1, const dht::ring_position& k2) const {
            if (k1.is_dummy_entry()) {
                return false;
            }
            return _c(k1._key, k2);
        }
    };
};

// Tracks accesses and performs eviction of cache entries.
class cache_tracker final {
public:
    using lru_type = bi::list<cache_entry,
        bi::member_hook<cache_entry, cache_entry::lru_link_type, &cache_entry::_lru_link>,
        bi::constant_time_size<false>>; // we need this to have bi::auto_unlink on hooks.
private:
    // We will try to evict large partition after that many normal evictions
    const uint32_t _normal_large_eviction_ratio = 1000;
    // Number of normal evictions to perform before we try to evict large partition
    uint32_t _normal_eviction_count = _normal_large_eviction_ratio;
public:
    struct stats {
        uint64_t hits;
        uint64_t misses;
        uint64_t insertions;
        uint64_t concurrent_misses_same_key;
        uint64_t merges;
        uint64_t evictions;
        uint64_t removals;
        uint64_t partitions;
        uint64_t modification_count;
    };
private:
    stats _stats{};
    seastar::metrics::metric_groups _metrics;
    logalloc::region _region;
    lru_type _lru;
private:
    void setup_metrics();
public:
    cache_tracker();
    ~cache_tracker();
    void clear();
    void touch(cache_entry&);
    void insert(cache_entry&);
    void clear_continuity(cache_entry& ce);
    void on_erase();
    void on_merge();
    void on_hit();
    void on_miss();
    void on_miss_already_populated();
    allocation_strategy& allocator();
    logalloc::region& region();
    const logalloc::region& region() const;
    uint64_t modification_count() const { return _stats.modification_count; }
    uint64_t partitions() const { return _stats.partitions; }
    const stats& get_stats() const { return _stats; }
};

// Returns a reference to shard-wide cache_tracker.
cache_tracker& global_cache_tracker();

//
// A data source which wraps another data source such that data obtained from the underlying data source
// is cached in-memory in order to serve queries faster.
//
// To query the underlying data source through cache, use make_reader().
//
// Cache populates itself automatically during misses.
//
// Cache needs to be maintained externally so that it remains consistent with the underlying data source.
// Any incremental change to the underlying data source should result in update() being called on cache.
//
class row_cache final {
public:
    using partitions_type = bi::set<cache_entry,
        bi::member_hook<cache_entry, cache_entry::cache_link_type, &cache_entry::_cache_link>,
        bi::constant_time_size<false>, // we need this to have bi::auto_unlink on hooks
        bi::compare<cache_entry::compare>>;
    friend class autoupdating_underlying_reader;
    friend class single_partition_populating_reader;
    friend class cache_entry;
    friend class cache::read_context;
public:
    struct stats {
        utils::timed_rate_moving_average hits;
        utils::timed_rate_moving_average misses;
    };
private:
    cache_tracker& _tracker;
    stats _stats{};
    schema_ptr _schema;
    partitions_type _partitions; // Cached partitions are complete.

    // Represents all mutations behind the cache.
    // The set of mutations doesn't change within a single populate phase (it's a snapshot).
    mutation_source _underlying;
    snapshot_source _snapshot_source;

    // Synchronizes populating reads with updates of underlying data source to ensure that cache
    // remains consistent across flushes with the underlying data source.
    // Readers obtained from the underlying data source in earlier than
    // current phases must not be used to populate the cache, unless they hold
    // phaser::operation created in the reader's phase of origin. Readers
    // should hold to a phase only briefly because this inhibits progress of
    // updates. Phase changes occur in update()/clear(), which can be assumed to
    // be asynchronous wrt invoking of the underlying data source.
    utils::phased_barrier _populate_phaser;

    logalloc::allocating_section _update_section;
    logalloc::allocating_section _populate_section;
    logalloc::allocating_section _read_section;
    mutation_reader create_underlying_reader(cache::read_context&, const dht::partition_range&);
    mutation_reader make_scanning_reader(const dht::partition_range&, lw_shared_ptr<cache::read_context>);
    void on_hit();
    void on_miss();
    void upgrade_entry(cache_entry&);
    void invalidate_locked(const dht::decorated_key&);
    void invalidate_unwrapped(const dht::partition_range&);
    void clear_now() noexcept;
    static thread_local seastar::thread_scheduling_group _update_thread_scheduling_group;

    struct previous_entry_pointer {
        utils::phased_barrier::phase_type _populate_phase;
        stdx::optional<dht::decorated_key> _key;

        void reset(stdx::optional<dht::decorated_key> key, utils::phased_barrier::phase_type populate_phase) {
            _populate_phase = populate_phase;
            _key = std::move(key);
        }

        // TODO: Currently inserting an entry to the cache increases
        // modification counter. That doesn't seem to be necessary and if we
        // didn't do that we could store iterator here to avoid key comparison
        // (not to mention avoiding lookups in just_cache_scanning_reader.
    };

    template<typename CreateEntry, typename VisitEntry>
    //requires requires(CreateEntry create, VisitEntry visit, partitions_type::iterator it) {
    //        { create(it) } -> partitions_type::iterator;
    //        { visit(it) } -> void;
    //    }
    void do_find_or_create_entry(const dht::decorated_key& key, const previous_entry_pointer* previous,
                                 CreateEntry&& create_entry, VisitEntry&& visit_entry);

    partitions_type::iterator partitions_end() {
        return std::prev(_partitions.end());
    }
public:
    ~row_cache();
    row_cache(schema_ptr, snapshot_source, cache_tracker&);
    row_cache(row_cache&&) = default;
    row_cache(const row_cache&) = delete;
    row_cache& operator=(row_cache&&) = default;
public:
    // Implements mutation_source for this cache, see mutation_reader.hh
    // User needs to ensure that the row_cache object stays alive
    // as long as the reader is used.
    // The range must not wrap around.
    mutation_reader make_reader(schema_ptr,
                                const dht::partition_range& = query::full_partition_range,
                                const query::partition_slice& slice = query::full_slice,
                                const io_priority_class& = default_priority_class(),
                                tracing::trace_state_ptr trace_state = nullptr,
                                streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
                                mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::no);

    const stats& stats() const { return _stats; }
public:
    // Populate cache from given mutation. The mutation must contain all
    // information there is for its partition in the underlying data sources.
    void populate(const mutation& m, const previous_entry_pointer* previous = nullptr);

    // Synchronizes cache with the underlying data source from a memtable which
    // has just been flushed to the underlying data source.
    // The memtable can be queried during the process, but must not be written.
    // After the update is complete, memtable is empty.
    future<> update(memtable&, partition_presence_checker underlying_negative);

    // Moves given partition to the front of LRU if present in cache.
    void touch(const dht::decorated_key&);

    // Synchronizes cache with the underlying mutation source
    // by invalidating ranges which were modified. This will force
    // them to be re-read from the underlying mutation source
    // during next read overlapping with the invalidated ranges.
    //
    // Guarantees that readers created after invalidate()
    // completes will see all writes from the underlying
    // mutation source made prior to the call to invalidate().
    future<> invalidate(const dht::decorated_key&);
    future<> invalidate(const dht::partition_range& = query::full_partition_range);
    future<> invalidate(dht::partition_range_vector&&);

    auto num_entries() const {
        return _partitions.size();
    }
    const cache_tracker& get_cache_tracker() const {
        return _tracker;
    }

    void set_schema(schema_ptr) noexcept;
    const schema_ptr& schema() const;

    friend class just_cache_scanning_reader;
    friend class scanning_and_populating_reader;
    friend class range_populating_reader;
    friend class cache_tracker;
    friend class mark_end_as_continuous;
};
