// Copyright (C) 2012-present ScyllaDB
// SPDX-License-Identifier: AGPL-3.0-or-later


#include "dirty_memory_manager.hh"
#include <seastar/util/later.hh>
#include <seastar/core/with_scheduling_group.hh>
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

region_group_reclaimer region_group::no_reclaimer;

uint64_t region_group::top_region_evictable_space() const noexcept {
    return _regions.empty() ? 0 : _regions.top()->evictable_occupancy().total_space();
}

dirty_memory_manager_logalloc::size_tracked_region* region_group::get_largest_region() noexcept {
    if (!_maximal_rg || _maximal_rg->_regions.empty()) {
        return nullptr;
    }
    return _maximal_rg->_regions.top();
}

void
region_group::add(region_group* child) {
    child->_subgroup_heap_handle = _subgroups.push(child);
    update(child->_total_memory);
}

void
region_group::del(region_group* child) {
    _subgroups.erase(child->_subgroup_heap_handle);
    update(-child->_total_memory);
}

void
region_group::add(region* child_r) {
    auto child = static_cast<size_tracked_region*>(child_r);
    child->_heap_handle = _regions.push(child);
    region_group_binomial_group_sanity_check(_regions);
    update(child_r->occupancy().total_space());
}

void
region_group::del(region* child_r) {
    auto child = static_cast<size_tracked_region*>(child_r);
    _regions.erase(child->_heap_handle);
    region_group_binomial_group_sanity_check(_regions);
    update(-child_r->occupancy().total_space());
}

void
region_group::moved(region* old_address, region* new_address) {
}

bool
region_group::execution_permitted() noexcept {
    return do_for_each_parent(this, [] (auto rg) noexcept {
        return rg->under_pressure() ? stop_iteration::yes : stop_iteration::no;
    }) == nullptr;
}

future<>
region_group::start_releaser(scheduling_group deferred_work_sg) {
    return with_scheduling_group(deferred_work_sg, [this] {
        return yield().then([this] {
            return repeat([this] () noexcept {
                if (_shutdown_requested) {
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }

                if (!_blocked_requests.empty() && execution_permitted()) {
                    auto req = std::move(_blocked_requests.front());
                    _blocked_requests.pop_front();
                    req->allocate();
                    return make_ready_future<stop_iteration>(stop_iteration::no);
                } else {
                    // Block reclaiming to prevent signal() from being called by reclaimer inside wait()
                    // FIXME: handle allocation failures (not very likely) like allocating_section does
                    tracker_reclaimer_lock rl;
                    return _relief.wait().then([] {
                        return stop_iteration::no;
                    });
                }
            });
        });
    });
}

region_group::region_group(sstring name, region_group *parent,
        region_group_reclaimer& reclaimer, scheduling_group deferred_work_sg)
    : _parent(parent)
    , _reclaimer(reclaimer)
    , _blocked_requests(on_request_expiry{std::move(name)})
    , _releaser(reclaimer_can_block() ? start_releaser(deferred_work_sg) : make_ready_future<>())
{
    if (_parent) {
        _parent->add(this);
    }
}

bool region_group::reclaimer_can_block() const {
    return _reclaimer.throttle_threshold() != std::numeric_limits<size_t>::max();
}

void region_group::notify_relief() {
    _relief.signal();
    for (region_group* child : _subgroups) {
        child->notify_relief();
    }
}

void region_group::update(ssize_t delta) {
    // Most-enclosing group which was relieved.
    region_group* top_relief = nullptr;

    do_for_each_parent(this, [&top_relief, delta] (region_group* rg) mutable {
        rg->update_maximal_rg();
        rg->_total_memory += delta;

        if (rg->_total_memory >= rg->_reclaimer.soft_limit_threshold()) {
            rg->_reclaimer.notify_soft_pressure();
        } else {
            rg->_reclaimer.notify_soft_relief();
        }

        if (rg->_total_memory > rg->_reclaimer.throttle_threshold()) {
            rg->_reclaimer.notify_pressure();
        } else if (rg->_reclaimer.under_pressure()) {
            rg->_reclaimer.notify_relief();
            top_relief = rg;
        }

        return stop_iteration::no;
    });

    if (top_relief) {
        top_relief->notify_relief();
    }
}

void region_group::on_request_expiry::operator()(std::unique_ptr<allocating_function>& func) noexcept {
    func->fail(std::make_exception_ptr(blocked_requests_timed_out_error{_name}));
}

}
