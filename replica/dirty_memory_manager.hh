/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <boost/intrusive/parent_from_member.hpp>
#include <boost/heap/binomial_heap.hpp>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/semaphore.hh>
#include "replica/database_fwd.hh"
#include "utils/assert.hh"
#include "utils/logalloc.hh"

class test_region_group;

namespace replica {

// Code previously under logalloc namespace
namespace dirty_memory_manager_logalloc {

class size_tracked_region;

struct region_evictable_occupancy_ascending_less_comparator {
    bool operator()(size_tracked_region* r1, size_tracked_region* r2) const;
};

using region_heap = boost::heap::binomial_heap<size_tracked_region*,
        boost::heap::compare<region_evictable_occupancy_ascending_less_comparator>,
        boost::heap::allocator<std::allocator<size_tracked_region*>>,
        //constant_time_size<true> causes corruption with boost < 1.60
        boost::heap::constant_time_size<false>>;

class size_tracked_region : public logalloc::region {
public:
    std::optional<region_heap::handle_type> _heap_handle;
};

// The region_group class keeps track of two memory use counts:
//
//  - real memory: this is LSA memory used by memtables, whether active or being
//            flushed. 
//  - spooled memory: this is LSA memory used by memtables undergoing a flush
//            that has been copied to an sstable. It a subset of
//            real memory. Once a flushing memtable is sealed, its spooled memory
//            drops to zero and real memory drops by the size of the memtable.
//
// Since the control loop is interested in (real memory) - (spooled memory), we keep
// track of the difference as unspooled memory.
//
// real memory and unspooled memory react to events in this way:
//   - data added to memtable: both real memory and unspooled memory increase by the same amount
//            (spooled memory does not change). Note the increase can actually be a decrease if
//            data was deleted or overwritten.
//   - part of the memtable was flushed to disk: unspooled memory decreases (spooled memory
//            increases)
//
// Users of a region_group configure reclaim with an unspooled memory soft limit (where
// sstable flush starts, but allocation can still continue), an unspooled memory hard limit (where
// allocation cannot proceed until sstable flush makes progress),
// and callbacks that are called when reclaiming is required and no longer necessary.
// There is also a real memory hard limit.

//
// These callbacks will be called when the dirty memory manager
// see relevant changes in the memory pressure conditions for this region_group. By specializing
// those methods - which are a nop by default - the callers initiate memtable flushing to
// free real and unspooled memory.

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

using reclaim_start_callback = noncopyable_function<void () noexcept>;
using reclaim_stop_callback = noncopyable_function<void () noexcept>;

struct reclaim_config {
    size_t unspooled_hard_limit = std::numeric_limits<size_t>::max();
    size_t unspooled_soft_limit = unspooled_hard_limit;
    size_t real_hard_limit = std::numeric_limits<size_t>::max();
    reclaim_start_callback start_reclaiming = [] () noexcept {};
    reclaim_stop_callback stop_reclaiming = [] () noexcept {};
};

// A container for memtables. Called "region_group" for historical
// reasons. Receives updates about memtable size change via the
// LSA region_listener interface.
class region_group : public logalloc::region_listener {
    using region_heap = dirty_memory_manager_logalloc::region_heap;
public:
    struct allocating_function {
        virtual ~allocating_function() = default;
        virtual void allocate() = 0;
        virtual void fail(std::exception_ptr) = 0;
    };
private:
    template <typename Func>
    struct concrete_allocating_function : public allocating_function {
        using futurator = futurize<std::invoke_result_t<Func>>;
        typename futurator::promise_type pr;
        Func func;
    public:
        void allocate() override {
            futurator::invoke(func).forward_to(std::move(pr));
        }
        void fail(std::exception_ptr e) override {
            pr.set_exception(e);
        }
        concrete_allocating_function(Func&& func) : func(std::forward<Func>(func)) {}
        typename futurator::type get_future() {
            return pr.get_future();
        }
    };

    class on_request_expiry {
        class blocked_requests_timed_out_error : public timed_out_error {
            const sstring _msg;
        public:
            explicit blocked_requests_timed_out_error(sstring name)
                : _msg(std::move(name) + ": timed out") {}
            virtual const char* what() const noexcept override {
                return _msg.c_str();
            }
        };

        sstring _name;
    public:
        explicit on_request_expiry(sstring name) : _name(std::move(name)) {}
        void operator()(std::unique_ptr<allocating_function>&) noexcept;
    };
private:
    reclaim_config _cfg;

    bool _under_unspooled_pressure = false;
    bool _under_unspooled_soft_pressure = false;

    region_group* _subgroup = nullptr;

    size_t _real_total_memory = 0;

    bool _under_real_pressure = false;

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

    size_t _unspooled_total_memory = 0;

    region_heap _regions;

    condition_variable _relief;
    bool _shutdown_requested = false;
    future<> _releaser;

private:
    size_t real_throttle_threshold() const noexcept {
        return _cfg.real_hard_limit;
    }
public:
    void update_real(ssize_t delta);

    size_t real_memory_used() const noexcept {
        return _real_total_memory;
    }

private:
    bool do_update_real_and_check_relief(ssize_t delta);

public:
    bool under_unspooled_pressure() const noexcept {
        return _under_unspooled_pressure;
    }

    bool over_unspooled_soft_limit() const noexcept {
        return _under_unspooled_soft_pressure;
    }

    void notify_unspooled_soft_pressure() noexcept {
        if (!_under_unspooled_soft_pressure) {
            _under_unspooled_soft_pressure = true;
            _cfg.start_reclaiming();
        }
    }

private:
    void notify_unspooled_soft_relief() noexcept {
        if (_under_unspooled_soft_pressure) {
            _under_unspooled_soft_pressure = false;
            _cfg.stop_reclaiming();
        }
    }

    void notify_unspooled_pressure() noexcept {
        _under_unspooled_pressure = true;
    }

    void notify_unspooled_relief() noexcept {
        _under_unspooled_pressure = false;
    }

    void execute_one();
public:
    size_t unspooled_throttle_threshold() const noexcept {
        return _cfg.unspooled_hard_limit;
    }
private:
    size_t unspooled_soft_limit_threshold() const noexcept {
        return _cfg.unspooled_soft_limit;
    }

    bool reclaimer_can_block() const;
    future<> start_releaser(scheduling_group deferered_work_sg);
    future<> release_queued_allocations();
    void notify_unspooled_pressure_relieved();
    friend void region_group_binomial_group_sanity_check(const region_group::region_heap& bh);
private: // from region_listener
    virtual void moved(logalloc::region* old_address, logalloc::region* new_address) override;
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
    region_group(sstring name = "(unnamed region group)", reclaim_config cfg = {},
            scheduling_group deferred_work_sg = default_scheduling_group());
    region_group(region_group&& o) = delete;
    region_group(const region_group&) = delete;
    ~region_group() {
        // If we set a throttle threshold, we'd be postponing many operations. So shutdown must be
        // called.
        if (reclaimer_can_block()) {
            SCYLLA_ASSERT(_shutdown_requested);
        }
    }
    region_group& operator=(const region_group&) = delete;
    region_group& operator=(region_group&&) = delete;
    size_t unspooled_memory_used() const noexcept {
        return _unspooled_total_memory;
    }
    void update_unspooled(ssize_t delta);

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
    virtual void increase_usage(logalloc::region* r, ssize_t delta) override { // From region_listener
        _regions.increase(*static_cast<size_tracked_region*>(r)->_heap_handle);
        update_unspooled(delta);
    }

    virtual void decrease_evictable_usage(logalloc::region* r) override { // From region_listener
        _regions.decrease(*static_cast<size_tracked_region*>(r)->_heap_handle);
    }

    virtual void decrease_usage(logalloc::region* r, ssize_t delta) override { // From region_listener
        decrease_evictable_usage(r);
        update_unspooled(delta);
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
    // We disallow future-returning functions here, because otherwise memory may be available
    // when we start executing it, but no longer available in the middle of the execution.
    requires (!is_future<std::invoke_result_t<Func>>::value)
    futurize_t<std::invoke_result_t<Func>> run_when_memory_available(Func&& func, db::timeout_clock::time_point timeout);

    // returns a pointer to the largest region (in terms of memory usage) that sits below this
    // region group. This includes the regions owned by this region group as well as all of its
    // children.
    size_tracked_region* get_largest_region() noexcept;

    // Shutdown is mandatory for every user who has set a threshold
    // Can be called at most once.
    future<> shutdown() noexcept;

    size_t blocked_requests() const noexcept;

    uint64_t blocked_requests_counter() const noexcept;
private:
    // Returns true if and only if constraints of this group are not violated.
    // That's taking into account any constraints imposed by enclosing (parent) groups.
    bool execution_permitted() noexcept;

    uint64_t top_region_evictable_space() const noexcept;

    virtual void add(logalloc::region* child) override; // from region_listener
    virtual void del(logalloc::region* child) override; // from region_listener

    friend class ::test_region_group;
};

}

class dirty_memory_manager;

class sstable_write_permit final {
    friend class dirty_memory_manager;
    std::optional<semaphore_units<>> _permit;

    sstable_write_permit() noexcept = default;
    explicit sstable_write_permit(semaphore_units<>&& units) noexcept
            : _permit(std::move(units)) {
    }

public:
    sstable_write_permit(sstable_write_permit&&) noexcept = default;
    sstable_write_permit& operator=(sstable_write_permit&&) noexcept = default;

    static sstable_write_permit unconditional() {
        return sstable_write_permit();
    }
};

class flush_permit {
    friend class dirty_memory_manager;
    dirty_memory_manager* _manager;
    std::optional<sstable_write_permit> _sstable_write_permit;
    semaphore_units<> _background_permit;

    flush_permit(dirty_memory_manager* manager, sstable_write_permit&& sstable_write_permit, semaphore_units<>&& background_permit)
            : _manager(manager)
            , _sstable_write_permit(std::move(sstable_write_permit))
            , _background_permit(std::move(background_permit)) {
    }
public:
    flush_permit(flush_permit&&) noexcept = default;
    flush_permit& operator=(flush_permit&&) noexcept = default;

    sstable_write_permit release_sstable_write_permit() noexcept {
        return std::exchange(_sstable_write_permit, std::nullopt).value();
    }

    bool has_sstable_write_permit() const noexcept {
        return _sstable_write_permit.has_value();
    }

    future<flush_permit> reacquire_sstable_write_permit() &&;
};

class dirty_memory_manager {
    // We need a separate boolean, because from the LSA point of view, pressure may still be
    // mounting, in which case the pressure flag could be set back on if we force it off.
    bool _db_shutdown_requested = false;

    replica::database* _db;
    // The _region_group accounts for unspooled memory usage. It is defined as the real dirty
    // memory usage minus bytes that were already written to disk.
    dirty_memory_manager_logalloc::region_group _region_group;

    // We would like to serialize the flushing of memtables. While flushing many memtables
    // simultaneously can sustain high levels of throughput, the memory is not freed until the
    // memtable is totally gone. That means that if we have throttled requests, they will stay
    // throttled for a long time. Even when we have unspooled dirty, that only provides a rough
    // estimate, and we can't release requests that early.
    semaphore _flush_serializer;
    // We will accept a new flush before another one ends, once it is done with the data write.
    // That is so we can keep the disk always busy. But there is still some background work that is
    // left to be done. Mostly, update the caches and seal the auxiliary components of the SSTable.
    // This semaphore will cap the amount of background work that we have. Note that we're not
    // overly concerned about memtable memory, because dirty memory will put a limit to that. This
    // is mostly about dangling continuations. So that doesn't have to be a small number.
    static constexpr unsigned _max_background_work = 20;
    semaphore _background_work_flush_serializer = { _max_background_work };
    condition_variable _should_flush;
    int64_t _dirty_bytes_released_pre_accounted = 0;

    future<> flush_when_needed();

    future<> _waiting_flush;
    void start_reclaiming() noexcept;

    bool has_pressure() const noexcept {
        return _region_group.over_unspooled_soft_limit();
    }

    unsigned _extraneous_flushes = 0;

    seastar::metrics::metric_groups _metrics;
public:
    void setup_collectd(sstring namestr);

    future<> shutdown();

    // Limits and pressure conditions:
    // ===============================
    //
    // Unspooled Dirty
    // -------------
    // We can't free memory until the whole memtable is flushed because we need to keep it in memory
    // until the end, but we can fake freeing memory. When we are done with an element of the
    // memtable, we will update the region group pretending memory just went down by that amount.
    //
    // Because the amount of memory that we pretend to free should be close enough to the actual
    // memory used by the memtables, that effectively creates two sub-regions inside the dirty
    // region group, of equal size. In the worst case, we will have <memtable_total_space> dirty
    // bytes used, and half of that already spooled.
    //
    // Hard Limit
    // ----------
    // The total space that can be used by memtables in each group is defined by the threshold, but
    // we will only allow the region_group to grow to half of that. This is because of unspooled_dirty
    // as explained above. Because unspooled dirty is implemented by reducing the usage in the
    // region_group directly on partition written, we want to throttle every time half of the memory
    // as seen by the region_group. To achieve that we need to set the hard limit (first parameter
    // of the region_group_reclaimer) to 1/2 of the user-supplied threshold
    //
    // Soft Limit
    // ----------
    // When the soft limit is hit, no throttle happens. The soft limit exists because we don't want
    // to start flushing only when the limit is hit, but a bit earlier instead. If we were to start
    // flushing only when the hard limit is hit, workloads in which the disk is fast enough to cope
    // would see latency added to some requests unnecessarily.
    //
    // We then set the soft limit to 80 % of the unspooled dirty hard limit, which is equal to 40 % of
    // the user-supplied threshold.
    dirty_memory_manager(replica::database& db, size_t threshold, double soft_limit, scheduling_group deferred_work_sg);
    dirty_memory_manager()
        : _db(nullptr)
        , _region_group("memtable (unspooled)",
                dirty_memory_manager_logalloc::reclaim_config{
                    .start_reclaiming = std::bind_front(&dirty_memory_manager::start_reclaiming, this),
                })
        , _flush_serializer(1)
        , _waiting_flush(make_ready_future<>()) {}

    static dirty_memory_manager& from_region_group(dirty_memory_manager_logalloc::region_group *rg) noexcept {
        return *(boost::intrusive::get_parent_from_member(rg, &dirty_memory_manager::_region_group));
    }

    dirty_memory_manager_logalloc::region_group& region_group() noexcept {
        return _region_group;
    }

    const dirty_memory_manager_logalloc::region_group& region_group() const noexcept {
        return _region_group;
    }

    void revert_potentially_cleaned_up_memory(logalloc::region* from, int64_t delta) {
        _region_group.update_real(-delta);
        _region_group.update_unspooled(delta);
        _dirty_bytes_released_pre_accounted -= delta;
    }

    void account_potentially_cleaned_up_memory(logalloc::region* from, int64_t delta) {
        _region_group.update_real(delta);
        _region_group.update_unspooled(-delta);
        _dirty_bytes_released_pre_accounted += delta;
    }

    void pin_real_dirty_memory(int64_t delta) {
        _region_group.update_real(delta);
    }

    void unpin_real_dirty_memory(int64_t delta) {
        _region_group.update_real(-delta);
    }

    size_t real_dirty_memory() const noexcept {
        return _region_group.real_memory_used();
    }

    size_t unspooled_dirty_memory() const noexcept {
        return _region_group.unspooled_memory_used();
    }

    void notify_soft_pressure() {
        _region_group.notify_unspooled_soft_pressure();
    }

    size_t throttle_threshold() const {
        return _region_group.unspooled_throttle_threshold();
    }

    future<> flush_one(replica::memtable_list& cf, flush_permit&& permit) noexcept;

    future<flush_permit> get_flush_permit() noexcept {
        return get_units(_background_work_flush_serializer, 1).then([this] (auto&& units) {
            return this->get_flush_permit(std::move(units));
        });
    }

    future<flush_permit> get_all_flush_permits() noexcept {
        return get_units(_background_work_flush_serializer, _max_background_work).then([this] (auto&& units) {
            return this->get_flush_permit(std::move(units));
        });
    }

    bool has_extraneous_flushes_requested() const noexcept {
        return _extraneous_flushes > 0;
    }

    void start_extraneous_flush() noexcept {
        ++_extraneous_flushes;
    }

    void finish_extraneous_flush() noexcept {
        --_extraneous_flushes;
    }
private:
    future<flush_permit> get_flush_permit(semaphore_units<>&& background_permit) noexcept {
        return get_units(_flush_serializer, 1).then([this, background_permit = std::move(background_permit)] (auto&& units) mutable {
            return flush_permit(this, sstable_write_permit(std::move(units)), std::move(background_permit));
        });
    }

    friend class flush_permit;
};

namespace dirty_memory_manager_logalloc {

template <typename Func>
// We disallow future-returning functions here, because otherwise memory may be available
// when we start executing it, but no longer available in the middle of the execution.
requires (!is_future<std::invoke_result_t<Func>>::value)
futurize_t<std::invoke_result_t<Func>>
region_group::run_when_memory_available(Func&& func, db::timeout_clock::time_point timeout) {
    bool blocked = 
        !_blocked_requests.empty()
        || under_unspooled_pressure()
        || _under_real_pressure;

    if (!blocked) {
        return futurize_invoke(func);
    }

    auto fn = std::make_unique<concrete_allocating_function<Func>>(std::forward<Func>(func));
    auto fut = fn->get_future();
    _blocked_requests.push_back(std::move(fn), timeout);
    ++_blocked_requests_counter;

    return fut;
}

inline
size_t
region_group::blocked_requests() const noexcept {
    return _blocked_requests.size();
}

inline
uint64_t
region_group::blocked_requests_counter() const noexcept {
    return _blocked_requests_counter;
}

}

extern thread_local dirty_memory_manager default_dirty_memory_manager;

}
