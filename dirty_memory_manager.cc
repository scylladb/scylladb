// Copyright (C) 2012-present ScyllaDB
// SPDX-License-Identifier: AGPL-3.0-or-later


#include "dirty_memory_manager.hh"
#include <seastar/util/later.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include "seastarx.hh"

// Code previously under logalloc namespace
namespace dirty_memory_manager_logalloc {

using namespace logalloc;

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
    assert(0);
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
region_group::add(region* child_r) {
    auto child = static_cast<size_tracked_region*>(child_r);
    assert(!child->_heap_handle);
    child->_heap_handle = std::make_optional(_regions.push(child));
    region_group_binomial_group_sanity_check(_regions);
    update_unspooled(child_r->occupancy().total_space());
}

void
region_group::del(region* child_r) {
    auto child = static_cast<size_tracked_region*>(child_r);
    if (child->_heap_handle) {
        _regions.erase(*std::exchange(child->_heap_handle, std::nullopt));
        region_group_binomial_group_sanity_check(_regions);
        update_unspooled(-child_r->occupancy().total_space());
    }
}

void
region_group::moved(region* old_address, region* new_address) {
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
    return !(this->under_unspooled_pressure()
                || (_under_real_pressure));
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
    {
        {
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
                    tracker_reclaimer_lock rl(logalloc::shard_tracker());
                    return _relief.wait();
                  });
                }
            }
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
