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

#include <bits/unique_ptr.h>
#include <seastar/core/scollectd.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/circular_buffer.hh>
#include "allocation_strategy.hh"
#include <boost/heap/binomial_heap.hpp>

namespace logalloc {

struct occupancy_stats;
class region;
class region_impl;
class allocating_section;

constexpr int segment_size_shift = 18; // 256K; see #151, #152
constexpr size_t segment_size = 1 << segment_size_shift;

//
// Frees some amount of objects from the region to which it's attached.
//
// This should eventually stop given no new objects are added:
//
//     while (eviction_fn() == memory::reclaiming_result::reclaimed_something) ;
//
using eviction_fn = std::function<memory::reclaiming_result()>;

//
// Users of a region_group can pass an instance of the class region_group_reclaimer, and specialize
// its methods start_reclaiming() and stop_reclaiming(). Those methods will be called when the LSA
// see relevant changes in the memory pressure conditions for this region_group. By specializing
// those methods - which are a nop by default - the callers can take action to aid the LSA in
// alleviating pressure.
class region_group_reclaimer {
protected:
    size_t _threshold;
    bool _under_pressure = false;
    virtual void start_reclaiming() {}
    virtual void stop_reclaiming() {}
public:
    bool under_pressure() const {
        return _under_pressure;
    }

    void notify_pressure() {
        if (!_under_pressure) {
            _under_pressure = true;
            start_reclaiming();
        }
    }

    void notify_relief() {
        if (_under_pressure) {
            _under_pressure = false;
            stop_reclaiming();
        }
    }

    region_group_reclaimer(size_t threshold = std::numeric_limits<size_t>::max()) : _threshold(threshold) {}
    virtual ~region_group_reclaimer() {}

    size_t throttle_threshold() const {
        return _threshold;
    }
};

// Groups regions for the purpose of statistics.  Can be nested.
class region_group {
    static region_group_reclaimer no_reclaimer;

    struct region_evictable_occupancy_ascending_less_comparator {
        bool operator()(region_impl* r1, region_impl* r2) const;
    };

    // We want to sort the subgroups so that we can easily find the one that holds the biggest
    // region for freeing purposes. Please note that this is not the biggest of the region groups,
    // since a big region group can have a big collection of very small regions, and freeing them
    // won't achieve anything. An example of such scenario is a ScyllaDB region with a lot of very
    // small memtables that add up, versus one with a very big memtable. The small memtables are
    // likely still growing, and freeing the big memtable will guarantee that the most memory is
    // freed up, while maximizing disk throughput.
    //
    // As asynchronous reclaim will likely involve disk operation, and those tend to be more
    // efficient when bulk done, this behavior is not ScyllaDB memtable specific.
    //
    // The maximal score is recursively defined as:
    //
    //      max(our_biggest_region, our_subtree_biggest_region)
    struct subgroup_maximal_region_ascending_less_comparator {
        bool operator()(region_group* rg1, region_group* rg2) const {
            return rg1->maximal_score() < rg2->maximal_score();
        }
    };
    friend struct subgroup_maximal_region_ascending_less_comparator;

    using region_heap = boost::heap::binomial_heap<region_impl*,
          boost::heap::compare<region_evictable_occupancy_ascending_less_comparator>,
          boost::heap::allocator<std::allocator<region_impl*>>,
          //constant_time_size<true> causes corruption with boost < 1.60
          boost::heap::constant_time_size<false>>;

    using subgroup_heap = boost::heap::binomial_heap<region_group*,
          boost::heap::compare<subgroup_maximal_region_ascending_less_comparator>,
          boost::heap::allocator<std::allocator<region_group*>>,
          //constant_time_size<true> causes corruption with boost < 1.60
          boost::heap::constant_time_size<false>>;

    region_group* _parent = nullptr;
    size_t _total_memory = 0;
    region_group_reclaimer& _reclaimer;

    subgroup_heap _subgroups;
    subgroup_heap::handle_type _subgroup_heap_handle;
    region_heap _regions;
    region_group* _maximal_rg = nullptr;
    // We need to store the score separately, otherwise we'd have to have an extra pass
    // before we update the region occupancy.
    size_t _maximal_score = 0;

    struct allocating_function {
        virtual ~allocating_function() = default;
        virtual void allocate() = 0;
    };

    template <typename Func>
    struct concrete_allocating_function : public allocating_function {
        using futurator = futurize<std::result_of_t<Func()>>;
        typename futurator::promise_type pr;
        Func func;
    public:
        void allocate() override {
            futurator::apply(func).forward_to(std::move(pr));
        }
        concrete_allocating_function(Func&& func) : func(std::forward<Func>(func)) {}
        typename futurator::type get_future() {
            return pr.get_future();
        }
    };

    // It is a more common idiom to just hold the promises in the circular buffer and make them
    // ready. However, in the time between the promise being made ready and the function execution,
    // it could be that our memory usage went up again. To protect against that, we have to recheck
    // if memory is still available after the future resolves.
    //
    // But we can greatly simplify it if we store the function itself in the circular_buffer, and
    // execute it synchronously in release_requests() when we are sure memory is available.
    //
    // This allows us to easily provide strong execution guarantees while keeping all re-check
    // complication in release_requests and keep the main request execution path simpler.
    circular_buffer<std::unique_ptr<allocating_function>> _blocked_requests;

    // All requests waiting for execution are kept in _blocked_requests (explained above) in the
    // region_group they were executed against. However, it could be that they are blocked not due
    // to their region group but to an ancestor. To handle these cases we will keep a list of
    // descendant region_groups that have requests that are waiting on us.
    //
    // Please note that what we keep here are not requests, and can be thought as just messages. The
    // requests themselves are kept in the region_group in which they originated. When we see that
    // there are region_groups waiting on us, we broadcast these messages to the waiters and they
    // will then decide whether they can now run or if they have to wait on us again (or potentially
    // a different ancestor)
    std::experimental::optional<shared_promise<>> _descendant_blocked_requests = {};

    region_group* _waiting_on_ancestor = nullptr;
    seastar::gate _asynchronous_gate;
    bool _shutdown_requested = false;
public:
    // When creating a region_group, one can specify an optional throttle_threshold parameter. This
    // parameter won't affect normal allocations, but an API is provided, through the region_group's
    // method run_when_memory_available(), to make sure that a given function is only executed when
    // the total memory for the region group (and all of its parents) is lower or equal to the
    // region_group's throttle_treshold (and respectively for its parents).
    region_group(region_group_reclaimer& reclaimer = no_reclaimer) : region_group(nullptr, reclaimer) {}
    region_group(region_group* parent, region_group_reclaimer& reclaimer = no_reclaimer) : _parent(parent), _reclaimer(reclaimer) {
        if (_parent) {
            _parent->add(this);
        }
    }
    region_group(region_group&& o) = delete;
    region_group(const region_group&) = delete;
    ~region_group() {
        // If we set a throttle threshold, we'd be postponing many operations. So shutdown must be
        // called.
        if (_reclaimer.throttle_threshold() != std::numeric_limits<size_t>::max()) {
            assert(_shutdown_requested);
        }
        if (_parent) {
            _parent->del(this);
        }
    }
    region_group& operator=(const region_group&) = delete;
    region_group& operator=(region_group&&) = delete;
    size_t memory_used() const {
        return _total_memory;
    }
    void update(ssize_t delta) {
        do_for_each_parent(this, [delta] (auto rg) mutable {
            rg->update_maximal_rg();
            rg->_total_memory += delta;
            // It is okay to call release_requests for a region_group that can't allow execution.
            // But that can generate various spurious messages to groups waiting on us that will be
            // then woken up just so they can go to wait again. So let's filter that.
            if (rg->execution_permitted()) {
                rg->release_requests();
            }
            return stop_iteration::no;
        });
    }

    // It would be easier to call update, but it is unfortunately broken in boost versions up to at
    // least 1.59.
    //
    // One possibility would be to just test for delta sigdness, but we adopt an explicit call for
    // two reasons:
    //
    // 1) it save us a branch
    // 2) some callers would like to pass delta = 0. For instance, when we are making a region
    //    evictable / non-evictable. Because the evictable occupancy changes, we would like to call
    //    the full update cycle even then.
    void increase_usage(region_heap::handle_type& r_handle, ssize_t delta) {
        _regions.increase(r_handle);
        update(delta);
    }

    void decrease_usage(region_heap::handle_type& r_handle, ssize_t delta) {
        _regions.decrease(r_handle);
        update(delta);
    }

    //
    // Make sure that the function specified by the parameter func only runs when this region_group,
    // as well as each of its ancestors have a memory_used() amount of memory that is lesser or
    // equal the throttle_threshold, as specified in the region_group's constructor.
    //
    // region_groups that did not specify a throttle_threshold will always allow for execution.
    //
    // In case current memory_used() is over the threshold, a non-ready future is returned and it
    // will be made ready at some point in the future, at which memory usage in the offending
    // region_group (either this or an ancestor) falls below the threshold.
    //
    // Requests that are not allowed for execution are queued and released in FIFO order within the
    // same region_group, but no guarantees are made regarding release ordering across different
    // region_groups.
    template <typename Func>
    futurize_t<std::result_of_t<Func()>> run_when_memory_available(Func&& func) {
        // We disallow future-returning functions here, because otherwise memory may be available
        // when we start executing it, but no longer available in the middle of the execution.
        static_assert(!is_future<std::result_of_t<Func()>>::value, "future-returning functions are not permitted.");
        using futurator = futurize<std::result_of_t<Func()>>;

        auto blocked_at = do_for_each_parent(this, [] (auto rg) {
            return (rg->_blocked_requests.empty() && rg->execution_permitted()) ? stop_iteration::no : stop_iteration::yes;
        });

        if (!blocked_at) {
            return futurator::apply(func);
        }
        subscribe_for_ancestor_available_memory_notification(blocked_at);

        auto fn = std::make_unique<concrete_allocating_function<Func>>(std::forward<Func>(func));
        auto fut = fn->get_future();
        _blocked_requests.push_back(std::move(fn));

        // This is called here, and not at update(), for two reasons: the first, is that things that
        // are done during the free() path should be done carefuly, in the sense that they can
        // trigger another update call and put us in a loop. Not to mention we would like to keep
        // those from having exceptions. We solve that for release_requests by using later(), but in
        // here we can do away with that need altogether.
        //
        // Second and most important, until we actually block a request, the pressure condition may
        // very well be transient. There are opportunities for compactions, the condition can go
        // away on its own, etc.
        //
        // The reason we check execution permitted(), is that we'll still block requests if we have
        // free memory but existing requests in the queue. That is so we can keep our FIFO ordering
        // guarantee. So we need to distinguish here the case in which we're blocking merely to
        // serialize requests, so that the caller does not evict more than it should.
        if (!blocked_at->execution_permitted()) {
            blocked_at->_reclaimer.notify_pressure();
        }
        return fut;
    }

    // returns a pointer to the largest region (in terms of memory usage) that sits below this
    // region group. This includes the regions owned by this region group as well as all of its
    // children.
    region* get_largest_region();

    // Shutdown is mandatory for every user who has set a threshold
    future<> shutdown() {
        _shutdown_requested = true;
        return _asynchronous_gate.close();
    }

    size_t blocked_requests() {
        return _blocked_requests.size();
    }
private:
    // Make sure we get a notification and can call release_requests when one of our ancestors that
    // used to block us is no longer under memory pressure.
    void subscribe_for_ancestor_available_memory_notification(region_group *ancestor) {
        if ((this == ancestor) || (_waiting_on_ancestor)) {
            return; // already subscribed, or no need to
        }

        _waiting_on_ancestor = ancestor;

        with_gate(_asynchronous_gate, [this] {
            // We reevaluate _waiting_on_ancestor here so we make sure there is no deferring point
            // between determining the ancestor and registering with it for a notification. We start
            // with _waiting_on_ancestor set to the initial value, and after we are notified, we
            // will set _waiting_on_ancestor to nullptr to force this lambda to reevaluate it.
            auto evaluate_ancestor_and_stop = [this] {
                if (!_waiting_on_ancestor) {
                    auto new_blocking_point = do_for_each_parent(this, [] (auto rg) {
                        return (rg->execution_permitted()) ? stop_iteration::no : stop_iteration::yes;
                    });
                    if (!new_blocking_point) {
                        release_requests();
                    }
                    _waiting_on_ancestor = (new_blocking_point == this) ? nullptr : new_blocking_point;
                }
                return _waiting_on_ancestor == nullptr;
            };

            return do_until(evaluate_ancestor_and_stop, [this] {
                if (!_waiting_on_ancestor->_descendant_blocked_requests) {
                    _waiting_on_ancestor->_descendant_blocked_requests = shared_promise<>();
                }
                return _waiting_on_ancestor->_descendant_blocked_requests->get_shared_future().then([this] {
                    _waiting_on_ancestor = nullptr;
                });
            });
        });
    }

    // Executes the function func for each region_group upwards in the hierarchy, starting with the
    // parameter node. The function func may return stop_iteration::no, in which case it proceeds to
    // the next ancestor in the hierarchy, or stop_iteration::yes, in which case it stops at this
    // level.
    //
    // This method returns a pointer to the region_group that was processed last, or nullptr if the
    // root was reached.
    template <typename Func>
    static region_group* do_for_each_parent(region_group *node, Func&& func) {
        auto rg = node;
        while (rg) {
            if (func(rg) == stop_iteration::yes) {
                return rg;
            }
            rg = rg->_parent;
        }
        return nullptr;
    }
    inline bool execution_permitted() const {
        return _total_memory <= _reclaimer.throttle_threshold();
    }

    void release_requests() noexcept;

    uint64_t top_region_evictable_space() const;

    uint64_t maximal_score() const {
        return _maximal_score;
    }

    void update_maximal_rg() {
        auto my_score = top_region_evictable_space();
        auto children_score = _subgroups.empty() ? 0 : _subgroups.top()->maximal_score();
        auto old_maximal_score = _maximal_score;
        if (children_score > my_score) {
            _maximal_rg = _subgroups.top()->_maximal_rg;
        } else {
            _maximal_rg = this;
        }

        _maximal_score = _maximal_rg->top_region_evictable_space();
        if (_parent) {
            // binomial heap update boost bug.
            if (_maximal_score > old_maximal_score) {
                _parent->_subgroups.increase(_subgroup_heap_handle);
            } else if (_maximal_score < old_maximal_score) {
                _parent->_subgroups.decrease(_subgroup_heap_handle);
            }
        }
    }

    void add(region_group* child);
    void del(region_group* child);
    void add(region_impl* child);
    void del(region_impl* child);
    friend class region_impl;
};

// Controller for all LSA regions. There's one per shard.
class tracker {
public:
    class impl;
private:
    std::unique_ptr<impl> _impl;
    memory::reclaimer _reclaimer;
    friend class region;
    friend class region_impl;
    memory::reclaiming_result reclaim();
public:
    tracker();
    ~tracker();

    //
    // Tries to reclaim given amount of bytes in total using all compactible
    // and evictable regions. Returns the number of bytes actually reclaimed.
    // That value may be smaller than requested when evictable pools are empty
    // and compactible pools can't compact any more.
    //
    // Invalidates references to objects in all compactible and evictable regions.
    //
    size_t reclaim(size_t bytes);

    // Compacts one segment at a time from sparsest segment to least sparse until work_waiting_on_reactor returns true
    // or there are no more segments to compact.
    reactor::idle_cpu_handler_result compact_on_idle(reactor::work_waiting_on_reactor);

    // Compacts as much as possible. Very expensive, mainly for testing.
    // Invalidates references to objects in all compactible and evictable regions.
    void full_compaction();

    void reclaim_all_free_segments();

    // Returns aggregate statistics for all pools.
    occupancy_stats region_occupancy();

    // Returns statistics for all segments allocated by LSA on this shard.
    occupancy_stats occupancy();

    impl& get_impl() { return *_impl; }

    // Set the minimum number of segments reclaimed during single reclamation cycle.
    void set_reclamation_step(size_t step_in_segments);

    // Returns the minimum number of segments reclaimed during single reclamation cycle.
    size_t reclamation_step() const;

    // Abort on allocation failure from LSA
    void enable_abort_on_bad_alloc();

    bool should_abort_on_bad_alloc();
};

tracker& shard_tracker();

// Monoid representing pool occupancy statistics.
// Naturally ordered so that sparser pools come fist.
// All sizes in bytes.
class occupancy_stats {
    size_t _free_space;
    size_t _total_space;
public:
    occupancy_stats() : _free_space(0), _total_space(0) {}

    occupancy_stats(size_t free_space, size_t total_space)
        : _free_space(free_space), _total_space(total_space) { }

    bool operator<(const occupancy_stats& other) const {
        return used_fraction() < other.used_fraction();
    }

    friend occupancy_stats operator+(const occupancy_stats& s1, const occupancy_stats& s2) {
        occupancy_stats result(s1);
        result += s2;
        return result;
    }

    friend occupancy_stats operator-(const occupancy_stats& s1, const occupancy_stats& s2) {
        occupancy_stats result(s1);
        result -= s2;
        return result;
    }

    occupancy_stats& operator+=(const occupancy_stats& other) {
        _total_space += other._total_space;
        _free_space += other._free_space;
        return *this;
    }

    occupancy_stats& operator-=(const occupancy_stats& other) {
        _total_space -= other._total_space;
        _free_space -= other._free_space;
        return *this;
    }

    size_t used_space() const {
        return _total_space - _free_space;
    }

    size_t free_space() const {
        return _free_space;
    }

    size_t total_space() const {
        return _total_space;
    }

    float used_fraction() const {
        return _total_space ? float(used_space()) / total_space() : 0;
    }

    friend std::ostream& operator<<(std::ostream&, const occupancy_stats&);
};

//
// Log-structured allocator region.
//
// Objects allocated using this region are said to be owned by this region.
// Objects must be freed only using the region which owns them. Ownership can
// be transferred across regions using the merge() method. Region must be live
// as long as it owns any objects.
//
// Each region has separate memory accounting and can be compacted
// independently from other regions. To reclaim memory from all regions use
// shard_tracker().
//
// Region is automatically added to the set of
// compactible regions when constructed.
//
class region {
public:
    using impl = region_impl;
private:
    shared_ptr<impl> _impl;
public:
    region();
    explicit region(region_group& group);
    ~region();
    region(region&& other);
    region& operator=(region&& other);
    region(const region& other) = delete;

    occupancy_stats occupancy() const;

    allocation_strategy& allocator();

    // Merges another region into this region. The other region is left empty.
    // Doesn't invalidate references to allocated objects.
    void merge(region& other);

    // Compacts everything. Mainly for testing.
    // Invalidates references to allocated objects.
    void full_compaction();

    // Changes the reclaimability state of this region. When region is not
    // reclaimable, it won't be considered by tracker::reclaim(). By default region is
    // reclaimable after construction.
    void set_reclaiming_enabled(bool);

    // Returns the reclaimability state of this region.
    bool reclaiming_enabled() const;

    // Returns a value which is increased when this region is either compacted or
    // evicted from, which invalidates references into the region.
    // When the value returned by this method doesn't change, references remain valid.
    uint64_t reclaim_counter() const;

    // Makes this region an evictable region. Supplied function will be called
    // when data from this region needs to be evicted in order to reclaim space.
    // The function should free some space from this region.
    void make_evictable(eviction_fn);

    friend class region_group;
    friend class allocating_section;
};

// Forces references into the region to remain valid as long as this guard is
// live by disabling compaction and eviction.
// Can be nested.
struct reclaim_lock {
    region& _region;
    bool _prev;
    reclaim_lock(region& r)
        : _region(r)
        , _prev(r.reclaiming_enabled())
    {
        _region.set_reclaiming_enabled(false);
    }
    ~reclaim_lock() {
        _region.set_reclaiming_enabled(_prev);
    }
};

// Utility for running critical sections which need to lock some region and
// also allocate LSA memory. The object learns from failures how much it
// should reserve up front in order to not cause allocation failures.
class allocating_section {
    size_t _lsa_reserve = 10; // in segments
    size_t _std_reserve = 1024; // in bytes
private:
    struct guard {
        size_t _prev;
        guard();
        ~guard();
        void enter(allocating_section&);
    };
    void on_alloc_failure();
public:
    //
    // Invokes func with reclaim_lock on region r. If LSA allocation fails
    // inside func it is retried after increasing LSA segment reserve. The
    // memory reserves are increased with region lock off allowing for memory
    // reclamation to take place in the region.
    //
    // Throws std::bad_alloc when reserves can't be increased to a sufficient level.
    //
    template<typename Func>
    decltype(auto) operator()(logalloc::region& r, Func&& func) {
        auto prev_lsa_reserve = _lsa_reserve;
        auto prev_std_reserve = _std_reserve;
        try {
            while (true) {
                assert(r.reclaiming_enabled());
                guard g;
                g.enter(*this);
                try {
                    logalloc::reclaim_lock _(r);
                    return func();
                } catch (const std::bad_alloc&) {
                    on_alloc_failure();
                }
            }
        } catch (const std::bad_alloc&) {
            // roll-back limits to protect against pathological requests
            // preventing future requests from succeeding.
            _lsa_reserve = prev_lsa_reserve;
            _std_reserve = prev_std_reserve;
            throw;
        }
    }
};

}
