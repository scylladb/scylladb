/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "partition_version.hh"
#include "partition_version_list.hh"

#include "utils/logalloc.hh"

class mutation_cleaner;

class mutation_cleaner_impl final {
    using snapshot_list = boost::intrusive::slist<partition_snapshot,
        boost::intrusive::member_hook<partition_snapshot, boost::intrusive::slist_member_hook<>, &partition_snapshot::_cleaner_hook>,
                boost::intrusive::cache_last<true>>;
    struct worker {
        condition_variable cv;
        snapshot_list snapshots;
        logalloc::allocating_section alloc_section;
        bool done = false; // true means the worker was abandoned and cannot access the mutation_cleaner_impl instance.
        int64_t merging_paused = 0; // Allows for pausing the background merging. Used only for testing purposes.
    };
private:
    logalloc::region& _region;
    cache_tracker* _tracker;
    mutation_cleaner* _cleaner;
    partition_version_list _versions;
    lw_shared_ptr<worker> _worker_state;
    mutation_application_stats& _app_stats;
    seastar::scheduling_group _scheduling_group;
    std::function<void(size_t)> _on_space_freed;
private:
    stop_iteration merge_some(partition_snapshot& snp) noexcept;
    stop_iteration merge_some() noexcept;
    void start_worker();
public:
    mutation_cleaner_impl(logalloc::region& r, cache_tracker* t, mutation_cleaner* cleaner,
            mutation_application_stats& app_stats,
            seastar::scheduling_group sg = seastar::current_scheduling_group(),
            std::function<void(size_t)> on_space_freed = nullptr)
        : _region(r)
        , _tracker(t)
        , _cleaner(cleaner)
        , _worker_state(make_lw_shared<worker>())
        , _app_stats(app_stats)
        , _scheduling_group(sg)
        , _on_space_freed(std::move(on_space_freed))
    {
        start_worker();
    }
    ~mutation_cleaner_impl();
    stop_iteration clear_gently() noexcept;
    memory::reclaiming_result clear_some() noexcept;
    void clear() noexcept;
    void destroy_later(partition_version& v) noexcept;
    void destroy_gently(partition_version& v) noexcept;
    void merge(mutation_cleaner_impl& other) noexcept;
    bool empty() const noexcept { return _versions.empty(); }
    future<> drain();
    void merge_and_destroy(partition_snapshot&) noexcept;
    void set_scheduling_group(seastar::scheduling_group sg) {
        _scheduling_group = sg;
        _worker_state->cv.broadcast();
    }
    auto pause() {
        _worker_state->merging_paused += 1;
        return defer([this] {
            _worker_state->merging_paused -= 1;
            _worker_state->cv.signal();
        });
    };
    auto make_region_space_guard() {
        return defer([&, dirty_before = _region.occupancy().total_space()] {
            auto dirty_after = _region.occupancy().total_space();
            if (_on_space_freed && dirty_before > dirty_after) {
                _on_space_freed(dirty_before - dirty_after);
            }
        });
    }
};

inline
void mutation_cleaner_impl::destroy_later(partition_version& v) noexcept {
    _versions.push_back(v);
}

inline
void mutation_cleaner_impl::destroy_gently(partition_version& v) noexcept {
    if (v.clear_gently(_tracker) == stop_iteration::no) {
        destroy_later(v);
    } else {
        current_allocator().destroy(&v);
    }
}

inline
void mutation_cleaner_impl::merge_and_destroy(partition_snapshot& ps) noexcept {
    if (ps.slide_to_oldest() == stop_iteration::yes || (!_worker_state->merging_paused && merge_some(ps) == stop_iteration::yes)) {
        lw_shared_ptr<partition_snapshot>::dispose(&ps);
    } else {
        // The snapshot must not be reachable by partitino_entry::read() after this,
        // which is ensured by slide_to_oldest() == stop_iteration::no.
        ps.migrate(&_region, _cleaner);
        _worker_state->snapshots.push_back(ps);
        _worker_state->cv.signal();
    }
}

// Container for garbage partition_version objects, used for freeing them incrementally.
//
// Mutation cleaner extends the lifetime of mutation_partition without doing
// the same for its schema. This means that the destruction of mutation_partition
// as well as any LSA migrators it may use cannot depend on the schema. Moreover,
// all used LSA migrators need remain alive and registered as long as
// mutation_cleaner is alive. In particular, this means that the instances of
// mutation_cleaner should not be thread local objects (or members of thread
// local objects).
class mutation_cleaner final {
    lw_shared_ptr<mutation_cleaner_impl> _impl;
public:
    mutation_cleaner(logalloc::region& r, cache_tracker* t, mutation_application_stats& app_stats,
            seastar::scheduling_group sg = seastar::current_scheduling_group(),
            std::function<void(size_t)> on_space_freed = nullptr)
        : _impl(make_lw_shared<mutation_cleaner_impl>(r, t, this, app_stats, sg, std::move(on_space_freed))) {
    }

    mutation_cleaner(mutation_cleaner&&) = delete;
    mutation_cleaner(const mutation_cleaner&) = delete;

    void set_scheduling_group(seastar::scheduling_group sg) {
        _impl->set_scheduling_group(sg);
    }

    // Frees some of the data. Returns stop_iteration::yes iff all was freed.
    // Must be invoked under owning allocator.
    stop_iteration clear_gently() noexcept {
        return _impl->clear_gently();
    }

    // Must be invoked under owning allocator.
    memory::reclaiming_result clear_some() noexcept {
        return _impl->clear_some();
    }

    // Must be invoked under owning allocator.
    void clear() noexcept {
        _impl->clear();
    }

    // Returns a guard object which when freed calls the on_space_freed callback with the amount
    // of memory freed in the region in terms of total_space() during the time the guard was
    // alive.
    // The guard must not outlive the cleaner.
    auto make_region_space_guard() {
        return _impl->make_region_space_guard();
    }

    // Enqueues v for destruction.
    // The object must not be part of any list, and must not be accessed externally any more.
    // In particular, it must not be attached, even indirectly, to any snapshot or partition_entry,
    // and must not be evicted from.
    // Must be invoked under owning allocator.
    void destroy_later(partition_version& v) noexcept {
        return _impl->destroy_later(v);
    }

    // Destroys v now or later.
    // Same requirements as destroy_later().
    // Must be invoked under owning allocator.
    void destroy_gently(partition_version& v) noexcept {
        return _impl->destroy_gently(v);
    }

    // Transfers objects from other to this.
    // This and other must belong to the same logalloc::region, and the same cache_tracker.
    // After the call other will refer to this cleaner.
    void merge(mutation_cleaner& other) noexcept {
        _impl->merge(*other._impl);
        other._impl = _impl;
    }

    // Returns true iff contains no unfreed objects
    bool empty() const noexcept {
        return _impl->empty();
    }

    // Forces cleaning and returns a future which resolves when there is nothing to clean.
    future<> drain() {
        return _impl->drain();
    }

    // Will merge given snapshot using partition_snapshot::merge_partition_versions() and then destroys it
    // using destroy_from_this(), possibly deferring in between.
    // This instance becomes the sole owner of the partition_snapshot object, the caller should not destroy it
    // nor access it after calling this.
    void merge_and_destroy(partition_snapshot& ps) {
        return _impl->merge_and_destroy(ps);
    }

    // Ensures the cleaner isn't doing any version merging while
    // the returned guard object is alive.
    //
    // Example usage:
    //
    // mutation_cleaner cleaner;
    // ...
    // {
    //     auto pause_guard = cleaner.pause();
    //     // Merging is paused here.
    //     ...
    // }
    // // Merging happens normally here.
    //
    // Meant only for use by unit tests.
    auto pause() {
        return _impl->pause();
    }
};
