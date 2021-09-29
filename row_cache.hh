/*
 * Copyright (C) 2015-present ScyllaDB
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
#include <boost/intrusive/parent_from_member.hpp>

#include <seastar/core/memory.hh>
#include <seastar/util/noncopyable_function.hh>

#include "mutation_reader.hh"
#include "mutation_partition.hh"
#include "utils/phased_barrier.hh"
#include "utils/histogram.hh"
#include "partition_version.hh"
#include "tracing/trace_state.hh"
#include <seastar/core/metrics_registration.hh>
#include "mutation_cleaner.hh"
#include "utils/double-decker.hh"
#include "db/cache_tracker.hh"

namespace bi = boost::intrusive;

class row_cache;
class memtable_entry;
class cache_tracker;
class flat_mutation_reader;

namespace cache {

class autoupdating_underlying_reader;
class cache_flat_mutation_reader;
class read_context;
class lsa_manager;

}

// Intrusive set entry which holds partition data.
//
// TODO: Make memtables use this format too.
class cache_entry {
    schema_ptr _schema;
    dht::decorated_key _key;
    partition_entry _pe;
    // True when we know that there is nothing between this entry and the previous one in cache
    struct {
        bool _continuous : 1;
        bool _dummy_entry : 1;
        bool _head : 1;
        bool _tail : 1;
        bool _train : 1;
    } _flags{};
    friend class size_calculator;

    flat_mutation_reader do_read(row_cache&, cache::read_context& ctx);
    flat_mutation_reader do_read(row_cache&, std::unique_ptr<cache::read_context> unique_ctx);
public:
    friend class row_cache;
    friend class cache_tracker;

    bool is_head() const noexcept { return _flags._head; }
    void set_head(bool v) noexcept { _flags._head = v; }
    bool is_tail() const noexcept { return _flags._tail; }
    void set_tail(bool v) noexcept { _flags._tail = v; }
    bool with_train() const noexcept { return _flags._train; }
    void set_train(bool v) noexcept { _flags._train = v; }

    struct dummy_entry_tag{};
    struct evictable_tag{};

    cache_entry(dummy_entry_tag)
        : _key{dht::token(), partition_key::make_empty()}
    {
        _flags._dummy_entry = true;
    }

    cache_entry(schema_ptr s, const dht::decorated_key& key, const mutation_partition& p)
        : _schema(std::move(s))
        , _key(key)
        , _pe(partition_entry::make_evictable(*_schema, mutation_partition(*_schema, p)))
    { }

    cache_entry(schema_ptr s, dht::decorated_key&& key, mutation_partition&& p)
        : cache_entry(evictable_tag(), s, std::move(key),
            partition_entry::make_evictable(*s, std::move(p)))
    { }

    // It is assumed that pe is fully continuous
    // pe must be evictable.
    cache_entry(evictable_tag, schema_ptr s, dht::decorated_key&& key, partition_entry&& pe) noexcept
        : _schema(std::move(s))
        , _key(std::move(key))
        , _pe(std::move(pe))
    { }

    cache_entry(cache_entry&&) noexcept;
    ~cache_entry();

    static cache_entry& container_of(partition_entry& pe) {
        return *boost::intrusive::get_parent_from_member(&pe, &cache_entry::_pe);
    }

    // Called when all contents have been evicted.
    // This object should unlink and destroy itself from the container.
    void on_evicted(cache_tracker&) noexcept;
    // Evicts contents of this entry.
    // The caller is still responsible for unlinking and destroying this entry.
    void evict(cache_tracker&) noexcept;

    const dht::decorated_key& key() const noexcept { return _key; }
    dht::ring_position_view position() const noexcept {
        if (is_dummy_entry()) {
            return dht::ring_position_view::max();
        }
        return _key;
    }

    friend dht::ring_position_view ring_position_view_to_compare(const cache_entry& ce) noexcept { return ce.position(); }

    const partition_entry& partition() const noexcept { return _pe; }
    partition_entry& partition() { return _pe; }
    const schema_ptr& schema() const noexcept { return _schema; }
    schema_ptr& schema() noexcept { return _schema; }
    flat_mutation_reader read(row_cache&, cache::read_context&);
    flat_mutation_reader read(row_cache&, std::unique_ptr<cache::read_context>);
    flat_mutation_reader read(row_cache&, cache::read_context&, utils::phased_barrier::phase_type);
    flat_mutation_reader read(row_cache&, std::unique_ptr<cache::read_context>, utils::phased_barrier::phase_type);
    bool continuous() const noexcept { return _flags._continuous; }
    void set_continuous(bool value) noexcept { _flags._continuous = value; }

    bool is_dummy_entry() const noexcept { return _flags._dummy_entry; }

    friend std::ostream& operator<<(std::ostream&, cache_entry&);
};

//
// A data source which wraps another data source such that data obtained from the underlying data source
// is cached in-memory in order to serve queries faster.
//
// Cache populates itself automatically during misses.
//
// All updates to the underlying mutation source must be performed through one of the synchronizing methods.
// Those are the methods which accept external_updater, e.g. update(), invalidate().
// All synchronizers have strong exception guarantees. If they fail, the set of writes represented by
// cache didn't change.
// Synchronizers can be invoked concurrently with each other and other operations on cache.
//
class row_cache final {
public:
    using phase_type = utils::phased_barrier::phase_type;
    using partitions_type = double_decker<int64_t, cache_entry,
                            dht::raw_token_less_comparator, dht::ring_position_comparator,
                            16, bplus::key_search::linear>;
    static_assert(bplus::SimpleLessCompare<int64_t, dht::raw_token_less_comparator>);
    friend class cache::autoupdating_underlying_reader;
    friend class single_partition_populating_reader;
    friend class cache_entry;
    friend class cache::cache_flat_mutation_reader;
    friend class cache::lsa_manager;
    friend class cache::read_context;
    friend class partition_range_cursor;
    friend class cache_tester;

    // A function which adds new writes to the underlying mutation source.
    // All invocations of external_updater on given cache instance are serialized internally.
    // Must have strong exception guarantees. If throws, the underlying mutation source
    // must be left in the state in which it was before the call.
    class external_updater_impl {
    public:
        virtual ~external_updater_impl() {}
        virtual future<> prepare() { return make_ready_future<>(); }
        // FIXME: make execute() noexcept, that will require every updater to make execution exception safe,
        // also change function signature.
        virtual void execute() = 0;
    };

    class external_updater {
        class non_prepared : public external_updater_impl {
            using Func = seastar::noncopyable_function<void()>;
            Func _func;
        public:
            explicit non_prepared(Func func) : _func(std::move(func)) {}
            virtual void execute() override {
                _func();
            }
        };
        std::unique_ptr<external_updater_impl> _impl;
    public:
        external_updater(seastar::noncopyable_function<void()> f) : _impl(std::make_unique<non_prepared>(std::move(f))) {}
        external_updater(std::unique_ptr<external_updater_impl> impl) : _impl(std::move(impl)) {}

        future<> prepare() { return _impl->prepare(); }
        void execute() { _impl->execute(); }
    };
public:
    struct stats {
        utils::timed_rate_moving_average hits;
        utils::timed_rate_moving_average misses;
        utils::timed_rate_moving_average reads_with_misses;
        utils::timed_rate_moving_average reads_with_no_misses;
    };
private:
    cache_tracker& _tracker;
    stats _stats{};
    schema_ptr _schema;
    partitions_type _partitions; // Cached partitions are complete.

    // The snapshots used by cache are versioned. The version number of a snapshot is
    // called the "population phase", or simply "phase". Between updates, cache
    // represents the same snapshot.
    //
    // Update doesn't happen atomically. Before it completes, some entries reflect
    // the old snapshot, while others reflect the new snapshot. After update
    // completes, all entries must reflect the new snapshot. There is a race between the
    // update process and populating reads. Since after the update all entries must
    // reflect the new snapshot, reads using the old snapshot cannot be allowed to
    // insert data which will no longer be reached by the update process. The whole
    // range can be therefore divided into two sub-ranges, one which was already
    // processed by the update and one which hasn't. Each key can be assigned a
    // population phase which determines to which range it belongs, as well as which
    // snapshot it reflects. The methods snapshot_of() and phase_of() can
    // be used to determine this.
    //
    // In general, reads are allowed to populate given range only if the phase
    // of the snapshot they use matches the phase of all keys in that range
    // when the population is committed. This guarantees that the range will
    // be reached by the update process or already has been in its entirety.
    // In case of phase conflict, current solution is to give up on
    // population. Since the update process is a scan, it's sufficient to
    // check when committing the population if the start and end of the range
    // have the same phases and that it's the same phase as that of the start
    // of the range at the time when reading began.

    mutation_source _underlying;
    phase_type _underlying_phase = partition_snapshot::min_phase;
    mutation_source_opt _prev_snapshot;

    // Positions >= than this are using _prev_snapshot, the rest is using _underlying.
    std::optional<dht::ring_position_ext> _prev_snapshot_pos;

    snapshot_source _snapshot_source;

    // There can be at most one update in progress.
    seastar::semaphore _update_sem = {1};

    logalloc::allocating_section _update_section;
    logalloc::allocating_section _populate_section;
    logalloc::allocating_section _read_section;
    flat_mutation_reader create_underlying_reader(cache::read_context&, mutation_source&, const dht::partition_range&);
    flat_mutation_reader make_scanning_reader(const dht::partition_range&, std::unique_ptr<cache::read_context>);
    void on_partition_hit();
    void on_partition_miss();
    void on_row_hit();
    void on_row_miss();
    void on_static_row_insert();
    void on_mispopulate();
    void upgrade_entry(cache_entry&);
    void invalidate_locked(const dht::decorated_key&);
    void clear_now() noexcept;

    struct previous_entry_pointer {
        std::optional<dht::decorated_key> _key;

        previous_entry_pointer() = default; // Represents dht::ring_position_view::min()
        previous_entry_pointer(dht::decorated_key key) : _key(std::move(key)) {};

        // TODO: store iterator here to avoid key comparison
    };

    template<typename CreateEntry, typename VisitEntry>
    requires requires(CreateEntry create, VisitEntry visit, partitions_type::iterator it, partitions_type::bound_hint hint) {
        { create(it, hint) } -> std::same_as<partitions_type::iterator>;
        { visit(it) } -> std::same_as<void>;
    }
    // Must be run under reclaim lock
    cache_entry& do_find_or_create_entry(const dht::decorated_key& key, const previous_entry_pointer* previous,
                                 CreateEntry&& create_entry, VisitEntry&& visit_entry);

    // Ensures that partition entry for given key exists in cache and returns a reference to it.
    // Prepares the entry for reading. "phase" must match the current phase of the entry.
    //
    // Since currently every entry has to have a complete tombstone, it has to be provided here.
    // The entry which is returned will have the tombstone applied to it.
    //
    // Must be run under reclaim lock
    cache_entry& find_or_create_incomplete(const partition_start& ps, row_cache::phase_type phase, const previous_entry_pointer* previous = nullptr);

    // Creates (or touches) a cache entry for missing partition so that sstables are not
    // poked again for it.
    cache_entry& find_or_create_missing(const dht::decorated_key& key);

    partitions_type::iterator partitions_end() {
        return std::prev(_partitions.end());
    }

    // Only active phases are accepted.
    // Reference valid only until next deferring point.
    mutation_source& snapshot_for_phase(phase_type);

    // Returns population phase for given position in the ring.
    // snapshot_for_phase() can be called to obtain mutation_source for given phase, but
    // only until the next deferring point.
    // Should be only called outside update().
    phase_type phase_of(dht::ring_position_view);

    struct snapshot_and_phase {
        mutation_source& snapshot;
        phase_type phase;
    };

    // Optimized version of:
    //
    //  { snapshot_for_phase(phase_of(pos)), phase_of(pos) };
    //
    snapshot_and_phase snapshot_of(dht::ring_position_view pos);

    // Merges the memtable into cache with configurable logic for handling memtable entries.
    // The Updater gets invoked for every entry in the memtable with a lower bound iterator
    // into _partitions (cache_i), and the memtable entry.
    // It is invoked inside allocating section and in the context of cache's allocator.
    // All memtable entries will be removed.
    template <typename Updater>
    future<> do_update(external_updater, memtable& m, Updater func);

    // Clears given memtable invalidating any affected cache elements.
    void invalidate_sync(memtable&) noexcept;

    // A function which updates cache to the current snapshot.
    // It's responsible for advancing _prev_snapshot_pos between deferring points.
    //
    // Must have strong failure guarantees. Upon failure, it should still leave the cache
    // in a state consistent with the update it is performing.
    using internal_updater = std::function<future<>()>;

    // Atomically updates the underlying mutation source and synchronizes the cache.
    //
    // Strong failure guarantees. If returns a failed future, the underlying mutation
    // source was and cache are not modified.
    //
    // internal_updater is only kept alive until its invocation returns.
    future<> do_update(external_updater eu, internal_updater iu) noexcept;

    flat_mutation_reader do_make_reader(schema_ptr, reader_permit permit, const dht::partition_range&, const query::partition_slice&,
            const io_priority_class&, tracing::trace_state_ptr, streamed_mutation::forwarding, mutation_reader::forwarding);
public:
    ~row_cache();
    row_cache(schema_ptr, snapshot_source, cache_tracker&, is_continuous = is_continuous::no);
    row_cache(row_cache&&) = default;
    row_cache(const row_cache&) = delete;
    row_cache& operator=(row_cache&&) = default;
public:
    // Implements mutation_source for this cache, see mutation_reader.hh
    // User needs to ensure that the row_cache object stays alive
    // as long as the reader is used.
    // The range must not wrap around.
    flat_mutation_reader make_reader(schema_ptr,
                                     reader_permit permit,
                                     const dht::partition_range&,
                                     const query::partition_slice&,
                                     const io_priority_class& = default_priority_class(),
                                     tracing::trace_state_ptr trace_state = nullptr,
                                     streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
                                     mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::no);

    flat_mutation_reader make_reader(schema_ptr s, reader_permit permit, const dht::partition_range& range = query::full_partition_range) {
        auto& full_slice = s->full_slice();
        return make_reader(std::move(s), std::move(permit), range, full_slice);
    }

    const stats& stats() const { return _stats; }
public:
    // Populate cache from given mutation, which must be fully continuous.
    // Intended to be used only in tests.
    // Can only be called prior to any reads.
    void populate(const mutation& m, const previous_entry_pointer* previous = nullptr);

    // Finds the entry in cache for a given key.
    // Intended to be used only in tests.
    cache_entry& lookup(const dht::decorated_key& key);

    // Synchronizes cache with the underlying data source from a memtable which
    // has just been flushed to the underlying data source.
    // The memtable can be queried during the process, but must not be written.
    // After the update is complete, memtable is empty.
    future<> update(external_updater, memtable&);

    // Like update(), synchronizes cache with an incremental change to the underlying
    // mutation source, but instead of inserting and merging data, invalidates affected ranges.
    // Can be thought of as a more fine-grained version of invalidate(), which invalidates
    // as few elements as possible.
    future<> update_invalidating(external_updater, memtable&);

    // Refreshes snapshot. Must only be used if logical state in the underlying data
    // source hasn't changed.
    void refresh_snapshot();

    // Moves given partition to the front of LRU if present in cache.
    void touch(const dht::decorated_key&);

    // Detaches current contents of given partition from LRU, so
    // that they are not evicted by memory reclaimer.
    void unlink_from_lru(const dht::decorated_key&);

    // Synchronizes cache with the underlying mutation source
    // by invalidating ranges which were modified. This will force
    // them to be re-read from the underlying mutation source
    // during next read overlapping with the invalidated ranges.
    //
    // The ranges passed to invalidate() must include all
    // data which changed since last synchronization. Failure
    // to do so may result in reads seeing partial writes,
    // which would violate write atomicity.
    //
    // Guarantees that readers created after invalidate()
    // completes will see all writes from the underlying
    // mutation source made prior to the call to invalidate().
    future<> invalidate(external_updater, const dht::decorated_key&);
    future<> invalidate(external_updater, const dht::partition_range& = query::full_partition_range);
    future<> invalidate(external_updater, dht::partition_range_vector&&);

    // Evicts entries from cache.
    //
    // Note that this does not synchronize with the underlying source,
    // it is assumed that the underlying source didn't change.
    // If it did, use invalidate() instead.
    void evict();

    const cache_tracker& get_cache_tracker() const {
        return _tracker;
    }
    cache_tracker& get_cache_tracker() {
        return _tracker;
    }

    void set_schema(schema_ptr) noexcept;
    const schema_ptr& schema() const;

    friend std::ostream& operator<<(std::ostream&, row_cache&);

    friend class just_cache_scanning_reader;
    friend class scanning_and_populating_reader;
    friend class range_populating_reader;
    friend class cache_tracker;
    friend class mark_end_as_continuous;
};

namespace cache {

class lsa_manager {
    row_cache &_cache;
public:
    lsa_manager(row_cache &cache) : _cache(cache) {}

    template<typename Func>
    decltype(auto) run_in_read_section(const Func &func) {
        return _cache._read_section(_cache._tracker.region(), [&func]() {
            return func();
        });
    }

    template<typename Func>
    decltype(auto) run_in_update_section(const Func &func) {
        return _cache._update_section(_cache._tracker.region(), [&func]() {
            return func();
        });
    }

    template<typename Func>
    void run_in_update_section_with_allocator(Func &&func) {
        return _cache._update_section(_cache._tracker.region(), [this, &func]() {
            return with_allocator(_cache._tracker.region().allocator(), [this, &func]() mutable {
                return func();
            });
        });
    }

    logalloc::region &region() { return _cache._tracker.region(); }

    logalloc::allocating_section &read_section() { return _cache._read_section; }
};

}
