// Copyright (C) 2012-present ScyllaDB
// SPDX-License-Identifier: AGPL-3.0-or-later


#include "utils/assert.hh"
#include "dirty_memory_manager.hh"
#include "database.hh" // for memtable_list
#include <seastar/core/metrics_api.hh>
#include <seastar/util/later.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include "seastarx.hh"

extern logging::logger dblog;

using namespace std::chrono_literals;

namespace replica {

// Code previously under logalloc namespace
namespace dirty_memory_manager_logalloc {

inline void
region_group_binomial_group_sanity_check(const region_group::region_heap& bh) {
#ifdef SEASTAR_DEBUG
    bool failed = false;
    size_t last =  std::numeric_limits<size_t>::max();
    for (auto b = bh.ordered_begin(); b != bh.ordered_end(); b++) {
        auto t = (*b)->evictable_occupancy().total_space();
        if (!(t <= last)) {
            failed = true;
            break;
        }
        last = t;
    }
    if (!failed) {
        return;
    }

    fmt::print("Sanity checking FAILED, size {}\n", bh.size());
    for (auto b = bh.ordered_begin(); b != bh.ordered_end(); b++) {
        auto r = (*b);
        auto t = r->evictable_occupancy().total_space();
        fmt::print(" r = {} (id={}), occupancy = {}\n", fmt::ptr(r), r->id(), t);
    }
    SCYLLA_ASSERT(0);
#endif
}

bool
region_evictable_occupancy_ascending_less_comparator::operator()(size_tracked_region* r1, size_tracked_region* r2) const {
    return r1->evictable_occupancy().total_space() < r2->evictable_occupancy().total_space();
}

uint64_t region_group::top_region_evictable_space() const noexcept {
    return _regions.empty() ? 0 : _regions.top()->evictable_occupancy().total_space();
}

dirty_memory_manager_logalloc::size_tracked_region* region_group::get_largest_region() noexcept {
    return _regions.empty() ? nullptr : _regions.top();
}

void
region_group::add(logalloc::region* child_r) {
    auto child = static_cast<size_tracked_region*>(child_r);
    SCYLLA_ASSERT(!child->_heap_handle);
    child->_heap_handle = std::make_optional(_regions.push(child));
    region_group_binomial_group_sanity_check(_regions);
    update_unspooled(child_r->occupancy().total_space());
}

void
region_group::del(logalloc::region* child_r) {
    auto child = static_cast<size_tracked_region*>(child_r);
    if (child->_heap_handle) {
        _regions.erase(*std::exchange(child->_heap_handle, std::nullopt));
        region_group_binomial_group_sanity_check(_regions);
        update_unspooled(-child_r->occupancy().total_space());
    }
}

void
region_group::moved(logalloc::region* old_address, logalloc::region* new_address) {
    auto old_child = static_cast<size_tracked_region*>(old_address);
    if (old_child->_heap_handle) {
        _regions.erase(*std::exchange(old_child->_heap_handle, std::nullopt));
    }

    auto new_child = static_cast<size_tracked_region*>(new_address);

    // set the old child handle since it's going to be moved
    // to the new child's handle by the respective move constructor /
    // assignment operator.
    old_child->_heap_handle = std::make_optional(_regions.push(new_child));
    region_group_binomial_group_sanity_check(_regions);
}

bool
region_group::execution_permitted() noexcept {
    return !under_unspooled_pressure() && !_under_real_pressure;
}

void
region_group::execute_one() {
    auto req = std::move(_blocked_requests.front());
    _blocked_requests.pop_front();
    req->allocate();
}

future<>
region_group::start_releaser(scheduling_group deferred_work_sg) {
    return with_scheduling_group(deferred_work_sg, std::bind(&region_group::release_queued_allocations, this));
}

future<> region_group::release_queued_allocations() {
    while (!_shutdown_requested) {
        if (!_blocked_requests.empty() && execution_permitted()) {
            execute_one();
            co_await coroutine::maybe_yield();
        } else {
            // We want `rl` to hold for the call to _relief.wait(), but not to wait
            // for the future to resolve, hence the inner lambda.
            co_await std::invoke([&] {
                // Block reclaiming to prevent signal() from being called by reclaimer inside wait()
                // FIXME: handle allocation failures (not very likely) like allocating_section does
                logalloc::tracker_reclaimer_lock rl(logalloc::shard_tracker());
                return _relief.wait();
            });
        }
    }
}

region_group::region_group(sstring name,
        reclaim_config cfg, scheduling_group deferred_work_sg)
    : _cfg(std::move(cfg))
    , _blocked_requests(on_request_expiry{std::move(name)})
    , _releaser(reclaimer_can_block() ? start_releaser(deferred_work_sg) : make_ready_future<>())
{
}

bool region_group::reclaimer_can_block() const {
    return unspooled_throttle_threshold() != std::numeric_limits<size_t>::max();
}

void region_group::notify_unspooled_pressure_relieved() {
    _relief.signal();
}

bool region_group::do_update_real_and_check_relief(ssize_t delta) {
    _real_total_memory += delta;

    if (_real_total_memory > real_throttle_threshold()) {
        _under_real_pressure = true;
    } else if (_under_real_pressure) {
        _under_real_pressure = false;
        return true;
    }
    return false;
}

void region_group::update_real(ssize_t delta) {
    if (do_update_real_and_check_relief(delta)) {
        notify_unspooled_pressure_relieved();
    }
}

void region_group::update_unspooled(ssize_t delta) {
    // Most-enclosing group which was relieved.
    bool relief = false;

    _unspooled_total_memory += delta;

    if (_unspooled_total_memory > unspooled_soft_limit_threshold()) {
        notify_unspooled_soft_pressure();
    } else {
        notify_unspooled_soft_relief();
    }

    if (_unspooled_total_memory > unspooled_throttle_threshold()) {
        notify_unspooled_pressure();
    } else if (under_unspooled_pressure()) {
        notify_unspooled_relief();
        relief = true;
    }

    relief |= do_update_real_and_check_relief(delta);

    if (relief) {
        notify_unspooled_pressure_relieved();
    }
}

future<>
region_group::shutdown() noexcept {
    _shutdown_requested = true;
    _relief.signal();
    return std::move(_releaser);
}

void region_group::on_request_expiry::operator()(std::unique_ptr<allocating_function>& func) noexcept {
    func->fail(std::make_exception_ptr(blocked_requests_timed_out_error{_name}));
}

}

future<flush_permit> flush_permit::reacquire_sstable_write_permit() && {
    return _manager->get_flush_permit(std::move(_background_permit));
}

dirty_memory_manager::dirty_memory_manager(replica::database& db, size_t threshold, double soft_limit, scheduling_group deferred_work_sg)
    : _db(&db)
    , _region_group("memtable (unspooled)", dirty_memory_manager_logalloc::reclaim_config{
            .unspooled_hard_limit = threshold / 2,
            .unspooled_soft_limit = threshold * soft_limit / 2,
            .real_hard_limit = threshold,
            .start_reclaiming = std::bind_front(&dirty_memory_manager::start_reclaiming, this)
      }, deferred_work_sg)
    , _flush_serializer(1)
    , _waiting_flush(flush_when_needed()) {}

void
dirty_memory_manager::setup_collectd(sstring namestr) {
    namespace sm = seastar::metrics;

    _metrics.add_group("memory", {
        sm::make_gauge(namestr + "_dirty_bytes", [this] { return real_dirty_memory(); },
                       sm::description("Holds the current size of a all non-free memory in bytes: used memory + released memory that hasn't been returned to a free memory pool yet. "
                                       "Total memory size minus this value represents the amount of available memory. "
                                       "If this value minus unspooled_dirty_bytes is too high then this means that the dirty memory eviction lags behind.")),

        sm::make_gauge(namestr +"_unspooled_dirty_bytes", [this] { return unspooled_dirty_memory(); },
                       sm::description("Holds the size of used memory in bytes. Compare it to \"dirty_bytes\" to see how many memory is wasted (neither used nor available).")),
    });
}

future<> dirty_memory_manager::shutdown() {
    _db_shutdown_requested = true;
    _should_flush.signal();
    return std::move(_waiting_flush).then([this] {
        return _region_group.shutdown();
    });
}

future<> dirty_memory_manager::flush_one(replica::memtable_list& mtlist, flush_permit&& permit) noexcept {
    return mtlist.seal_active_memtable(std::move(permit)).handle_exception([schema = mtlist.back()->schema()] (std::exception_ptr ep) {
        dblog.error("Failed to flush memtable, {}:{} - {}", schema->ks_name(), schema->cf_name(), ep);
        return make_exception_future<>(ep);
    });
}

future<> dirty_memory_manager::flush_when_needed() {
    using namespace replica;
    if (!_db) {
        return make_ready_future<>();
    }
    // If there are explicit flushes requested, we must wait for them to finish before we stop.
    return do_until([this] { return _db_shutdown_requested; }, [this] {
        auto has_work = [this] { return has_pressure() || _db_shutdown_requested; };
        return _should_flush.wait(std::move(has_work)).then([this] {
            return get_flush_permit().then([this] (auto permit) {
                // We give priority to explicit flushes. They are mainly user-initiated flushes,
                // flushes coming from a DROP statement, or commitlog flushes.
                if (_flush_serializer.waiters()) {
                    return make_ready_future<>();
                }
                // condition abated while we waited for the semaphore
                if (!this->has_pressure() || _db_shutdown_requested) {
                    return make_ready_future<>();
                }
                // There are many criteria that can be used to select what is the best memtable to
                // flush. Most of the time we want some coordination with the commitlog to allow us to
                // release commitlog segments as early as we can.
                //
                // But during pressure condition, we'll just pick the CF that holds the largest
                // memtable. The advantage of doing this is that this is objectively the one that will
                // release the biggest amount of memory and is less likely to be generating tiny
                // SSTables.
                memtable& candidate_memtable = memtable::from_region(*(this->_region_group.get_largest_region()));
                memtable_list& mtlist = *(candidate_memtable.get_memtable_list());

                if (!candidate_memtable.region().evictable_occupancy()) {
                    // Soft pressure, but nothing to flush. It could be due to fsync, memtable_to_cache lagging,
                    // or candidate_memtable failed to flush.
                    // Back off to avoid OOMing with flush continuations.
                    return sleep(1ms);
                }

                // Do not wait. The semaphore will protect us against a concurrent flush. But we
                // want to start a new one as soon as the permits are destroyed and the semaphore is
                // made ready again, not when we are done with the current one.
                (void)this->flush_one(mtlist, std::move(permit)).handle_exception([] (std::exception_ptr ex) {
                    dblog.error("Flushing memtable returned unexpected error: {}", ex);
                });
                return make_ready_future<>();
            });
        });
    }).finally([this] {
        // We'll try to acquire the permit here to make sure we only really stop when there are no
        // in-flight flushes. Our stop condition checks for the presence of waiters, but it could be
        // that we have no waiters, but a flush still in flight. We wait for all background work to
        // stop. When that stops, we know that the foreground work in the _flush_serializer has
        // stopped as well.
        return get_units(_background_work_flush_serializer, _max_background_work).discard_result();
    });
}

void dirty_memory_manager::start_reclaiming() noexcept {
    _should_flush.signal();
}

}
