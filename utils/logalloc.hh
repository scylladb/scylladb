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

#include <memory>
#include <seastar/core/memory.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/expiring_fifo.hh>
#include "allocation_strategy.hh"
#include <boost/heap/binomial_heap.hpp>
#include "seastarx.hh"
#include "db/timeout_clock.hh"

namespace logalloc {

struct occupancy_stats;
class region;
class region_impl;
class allocating_section;

constexpr int segment_size_shift = 17; // 128K; see #151, #152
constexpr size_t segment_size = 1 << segment_size_shift;
constexpr size_t max_zone_segments = 256;

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
    size_t _soft_limit;
    bool _under_pressure = false;
    bool _under_soft_pressure = false;
    // The following restrictions apply to implementations of start_reclaiming() and stop_reclaiming():
    //
    //  - must not use any region or region_group objects, because they're invoked synchronously
    //    with operations on those.
    //
    //  - must be noexcept, because they're called on the free path.
    //
    //  - the implementation may be called synchronously with any operation
    //    which allocates memory, because these are called by memory reclaimer.
    //    In particular, the implementation should not depend on memory allocation
    //    because that may fail when in reclaiming context.
    //
    virtual void start_reclaiming() noexcept {}
    virtual void stop_reclaiming() noexcept {}
public:
    bool under_pressure() const {
        return _under_pressure;
    }

    bool over_soft_limit() const {
        return _under_soft_pressure;
    }

    void notify_soft_pressure() noexcept {
        if (!_under_soft_pressure) {
            _under_soft_pressure = true;
            start_reclaiming();
        }
    }

    void notify_soft_relief() noexcept {
        if (_under_soft_pressure) {
            _under_soft_pressure = false;
            stop_reclaiming();
        }
    }

    void notify_pressure() noexcept {
        _under_pressure = true;
    }

    void notify_relief() noexcept {
        _under_pressure = false;
    }

    region_group_reclaimer()
        : _threshold(std::numeric_limits<size_t>::max()), _soft_limit(std::numeric_limits<size_t>::max()) {}
    region_group_reclaimer(size_t threshold)
        : _threshold(threshold), _soft_limit(threshold) {}
    region_group_reclaimer(size_t threshold, size_t soft)
        : _threshold(threshold), _soft_limit(soft) {
        assert(_soft_limit <= _threshold);
    }

    virtual ~region_group_reclaimer() {}

    size_t throttle_threshold() const {
        return _threshold;
    }
    size_t soft_limit_threshold() const {
        return _soft_limit;
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
        virtual void fail(std::exception_ptr) = 0;
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
        void fail(std::exception_ptr e) override {
            pr.set_exception(e);
        }
        concrete_allocating_function(Func&& func) : func(std::forward<Func>(func)) {}
        typename futurator::type get_future() {
            return pr.get_future();
        }
    };

    struct on_request_expiry {
        void operator()(std::unique_ptr<allocating_function>&) noexcept;
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
    expiring_fifo<std::unique_ptr<allocating_function>, on_request_expiry, db::timeout_clock> _blocked_requests;

    uint64_t _blocked_requests_counter = 0;

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

    condition_variable _relief;
    future<> _releaser;
    bool _shutdown_requested = false;

    bool reclaimer_can_block() const;
    future<> start_releaser(scheduling_group deferered_work_sg);
    void notify_relief();
    friend void region_group_binomial_group_sanity_check(const region_group::region_heap& bh);
public:
    // When creating a region_group, one can specify an optional throttle_threshold parameter. This
    // parameter won't affect normal allocations, but an API is provided, through the region_group's
    // method run_when_memory_available(), to make sure that a given function is only executed when
    // the total memory for the region group (and all of its parents) is lower or equal to the
    // region_group's throttle_treshold (and respectively for its parents).
    //
    // The deferred_work_sg parameter specifies a scheduling group in which to run allocations
    // (given to run_when_memory_available()) when they must be deferred due to lack of memory
    // at the time the call to run_when_memory_available() was made.
    region_group(region_group_reclaimer& reclaimer = no_reclaimer,
            scheduling_group deferred_work_sg = default_scheduling_group())
            : region_group(nullptr, reclaimer, deferred_work_sg) {}
    region_group(region_group* parent, region_group_reclaimer& reclaimer = no_reclaimer,
            scheduling_group deferred_work_sg = default_scheduling_group());
    region_group(region_group&& o) = delete;
    region_group(const region_group&) = delete;
    ~region_group() {
        // If we set a throttle threshold, we'd be postponing many operations. So shutdown must be
        // called.
        if (reclaimer_can_block()) {
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
    void update(ssize_t delta);

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

    void decrease_evictable_usage(region_heap::handle_type& r_handle) {
        _regions.decrease(r_handle);
    }

    void decrease_usage(region_heap::handle_type& r_handle, ssize_t delta) {
        decrease_evictable_usage(r_handle);
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
    //
    // When timeout is reached first, the returned future is resolved with timed_out_error exception.
    template <typename Func>
    futurize_t<std::result_of_t<Func()>> run_when_memory_available(Func&& func, db::timeout_clock::time_point timeout = db::no_timeout) {
        // We disallow future-returning functions here, because otherwise memory may be available
        // when we start executing it, but no longer available in the middle of the execution.
        static_assert(!is_future<std::result_of_t<Func()>>::value, "future-returning functions are not permitted.");
        using futurator = futurize<std::result_of_t<Func()>>;

        auto blocked_at = do_for_each_parent(this, [] (auto rg) {
            return (rg->_blocked_requests.empty() && !rg->under_pressure()) ? stop_iteration::no : stop_iteration::yes;
        });

        if (!blocked_at) {
            return futurator::apply(func);
        }

        auto fn = std::make_unique<concrete_allocating_function<Func>>(std::forward<Func>(func));
        auto fut = fn->get_future();
        _blocked_requests.push_back(std::move(fn), timeout);
        ++_blocked_requests_counter;

        return fut;
    }

    // returns a pointer to the largest region (in terms of memory usage) that sits below this
    // region group. This includes the regions owned by this region group as well as all of its
    // children.
    region* get_largest_region();

    // Shutdown is mandatory for every user who has set a threshold
    // Can be called at most once.
    future<> shutdown() {
        _shutdown_requested = true;
        _relief.signal();
        return std::move(_releaser);
    }

    size_t blocked_requests() {
        return _blocked_requests.size();
    }

    uint64_t blocked_requests_counter() const {
        return _blocked_requests_counter;
    }
private:
    // Returns true if and only if constraints of this group are not violated.
    // That's taking into account any constraints imposed by enclosing (parent) groups.
    bool execution_permitted() noexcept;

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

    inline bool under_pressure() const {
        return _reclaimer.under_pressure();
    }

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
    // Guarantees that every live object from reclaimable regions will be moved.
    // Invalidates references to objects in all compactible and evictable regions.
    void full_compaction();

    void reclaim_all_free_segments();

    // Returns aggregate statistics for all pools.
    occupancy_stats region_occupancy();

    // Returns statistics for all segments allocated by LSA on this shard.
    occupancy_stats occupancy();

    // Returns amount of allocated memory not managed by LSA
    size_t non_lsa_used_space() const;

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

class basic_region_impl : public allocation_strategy {
protected:
    bool _reclaiming_enabled = true;
public:
    void set_reclaiming_enabled(bool enabled) {
        _reclaiming_enabled = enabled;
    }

    bool reclaiming_enabled() const {
        return _reclaiming_enabled;
    }
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
    shared_ptr<basic_region_impl> _impl;
private:
    region_impl& get_impl();
    const region_impl& get_impl() const;
public:
    region();
    explicit region(region_group& group);
    ~region();
    region(region&& other);
    region& operator=(region&& other);
    region(const region& other) = delete;

    occupancy_stats occupancy() const;

    allocation_strategy& allocator() {
        return *_impl;
    }
    const allocation_strategy& allocator() const {
        return *_impl;
    }

    region_group* group();

    // Merges another region into this region. The other region is left empty.
    // Doesn't invalidate references to allocated objects.
    void merge(region& other) noexcept;

    // Compacts everything. Mainly for testing.
    // Invalidates references to allocated objects.
    void full_compaction();

    // Runs eviction function once. Mainly for testing.
    memory::reclaiming_result evict_some();

    // Changes the reclaimability state of this region. When region is not
    // reclaimable, it won't be considered by tracker::reclaim(). By default region is
    // reclaimable after construction.
    void set_reclaiming_enabled(bool e) { _impl->set_reclaiming_enabled(e); }

    // Returns the reclaimability state of this region.
    bool reclaiming_enabled() const { return _impl->reclaiming_enabled(); }

    // Returns a value which is increased when this region is either compacted or
    // evicted from, which invalidates references into the region.
    // When the value returned by this method doesn't change, references remain valid.
    uint64_t reclaim_counter() const {
        return allocator().invalidate_counter();
    }

    // Will cause subsequent calls to evictable_occupancy() to report empty occupancy.
    void ground_evictable_occupancy();

    // Makes this region an evictable region. Supplied function will be called
    // when data from this region needs to be evicted in order to reclaim space.
    // The function should free some space from this region.
    void make_evictable(eviction_fn);

    const eviction_fn& evictor() const;

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
    size_t _minimum_lsa_emergency_reserve = 0;
private:
    struct guard {
        size_t _prev;
        guard();
        ~guard();
    };
    void reserve();
    void on_alloc_failure(logalloc::region&);
public:

    void set_lsa_reserve(size_t);
    void set_std_reserve(size_t);

    //
    // Reserves standard allocator and LSA memory for subsequent operations that
    // have to be performed with memory reclamation disabled.
    //
    // Throws std::bad_alloc when reserves can't be increased to a sufficient level.
    //
    template<typename Func>
    decltype(auto) with_reserve(Func&& fn) {
        auto prev_lsa_reserve = _lsa_reserve;
        auto prev_std_reserve = _std_reserve;
        try {
            guard g;
            _minimum_lsa_emergency_reserve = g._prev;
            reserve();
            return fn();
        } catch (const std::bad_alloc&) {
            // roll-back limits to protect against pathological requests
            // preventing future requests from succeeding.
            _lsa_reserve = prev_lsa_reserve;
            _std_reserve = prev_std_reserve;
            throw;
        }
    }

    //
    // Invokes func with reclaim_lock on region r. If LSA allocation fails
    // inside func it is retried after increasing LSA segment reserve. The
    // memory reserves are increased with region lock off allowing for memory
    // reclamation to take place in the region.
    //
    // References in the region are invalidated when allocating section is re-entered
    // on allocation failure.
    //
    // Throws std::bad_alloc when reserves can't be increased to a sufficient level.
    //
    template<typename Func>
    decltype(auto) with_reclaiming_disabled(logalloc::region& r, Func&& fn) {
        assert(r.reclaiming_enabled());
        while (true) {
            try {
                logalloc::reclaim_lock _(r);
                return fn();
            } catch (const std::bad_alloc&) {
                on_alloc_failure(r);
            }
        }
    }

    //
    // Reserves standard allocator and LSA memory and
    // invokes func with reclaim_lock on region r. If LSA allocation fails
    // inside func it is retried after increasing LSA segment reserve. The
    // memory reserves are increased with region lock off allowing for memory
    // reclamation to take place in the region.
    //
    // References in the region are invalidated when allocating section is re-entered
    // on allocation failure.
    //
    // Throws std::bad_alloc when reserves can't be increased to a sufficient level.
    //
    template<typename Func>
    decltype(auto) operator()(logalloc::region& r, Func&& func) {
        return with_reserve([this, &r, &func] {
            return with_reclaiming_disabled(r, func);
        });
    }
};

future<> prime_segment_pool(size_t available_memory, size_t min_free_memory);

uint64_t memory_allocated();
uint64_t memory_compacted();

}
