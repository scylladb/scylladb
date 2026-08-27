/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 *
 * Copyright (C) 2021-present ScyllaDB
 *
 */

#include "service/memory_limiter.hh"

#include <seastar/core/coroutine.hh>

#include <cmath>

namespace service {

memory_limiter::memory_limiter(size_t available_memory, double fraction,
        utils::updateable_value<double> shared_pool_fraction)
    : _max_request_size(budget(available_memory, fraction))
    , _total_memory(budget(available_memory, fraction))
    , _unlimited(false)
    , _shared_pool_fraction(std::move(shared_pool_fraction))
    , _fallback(make_lw_shared<request_memory_tenant>("unclassified", 0, _pool))
    , _shared_pool_fraction_observer(_shared_pool_fraction.observe([this] (const double&) { adjust(); }))
{
    adjust();
}

memory_limiter::memory_limiter(unlimited_tag, size_t available_memory, double fraction)
    : _max_request_size(budget(available_memory, fraction))
    , _total_memory(semaphore::max_counter() / 2)
    , _unlimited(true)
    // No source behind it, so observing it yields a dummy observer and adjust()
    // is never called; an unlimited limiter has nothing to re-split anyway.
    , _shared_pool_fraction(utils::updateable_value<double>(0.0))
    , _fallback(make_lw_shared<request_memory_tenant>("unlimited", 0, _pool))
    , _shared_pool_fraction_observer(_shared_pool_fraction.observe([this] (const double&) { adjust(); }))
{
    _fallback->set_capacity(_total_memory);
}

memory_limiter::~memory_limiter() = default;

future<> memory_limiter::stop() {
    // Waiting for the request memory to come back is deliberately not attempted:
    // a request holds its charge until its response has been written to a client
    // that may never read it, so this would not be bounded. main.cc does not stop
    // the CQL server's limiter for the same reason.
    return make_ready_future<>();
}

void memory_limiter::adjust() noexcept {
    if (_unlimited) {
        return;
    }

    reap_drained();

    if (_total_weight == 0) {
        // No service levels to divide the dedicated portion between yet, so let
        // the pool hold the whole budget; the fallback tenant, which has no
        // dedicated memory, then admits everything out of it.
        _pool.set_total_memory(_total_memory);
        return;
    }

    // Clamp out-of-range values rather than rejecting them, so a bad configured
    // value degrades gracefully.
    const double fraction = std::clamp(_shared_pool_fraction(), 0.0, 1.0);
    const ssize_t shared = _total_memory * fraction;
    const ssize_t dedicated = _total_memory - shared;

    const auto share_of = [&] (const request_memory_tenant& tenant) -> ssize_t {
        return std::floor((double(tenant.weight()) / double(_total_weight)) * dedicated);
    };
    ssize_t distributed = 0;
    for (const auto& [sg, tenant] : _tenants) {
        distributed += share_of(*tenant);
    }
    // The rounding remainder is a few bytes; it does not matter which tenant
    // gets it.
    const request_memory_tenant* remainder_owner = _tenants.begin()->second.get();
    const ssize_t remainder = dedicated - distributed;

    // Every resize wakes the tenants that are waiting for memory, so apply the
    // reductions before the increases: the memory moved into the shared pool has
    // to be taken off the dedicated shares before the pool offers it. Growing
    // the pool first funds a waiting tenant out of memory it still holds a share
    // of, and it keeps both.
    const auto apply = [&] (bool grow) {
        for (auto& [sg, tenant] : _tenants) {
            const ssize_t target = share_of(*tenant) + (tenant.get() == remainder_owner ? remainder : 0);
            const auto current = static_cast<ssize_t>(tenant->capacity());
            if (target != current && (target > current) == grow) {
                tenant->set_capacity(target);
            }
        }
    };
    apply(false);
    if (shared < _pool.total_memory()) {
        _pool.set_total_memory(shared);
    }
    apply(true);
    // A no-op if the pool was already resized above.
    _pool.set_total_memory(shared);
}

void memory_limiter::add_or_update(scheduling_group sg, size_t shares) {
    auto it = _tenants.find(sg);
    if (it == _tenants.end()) {
        it = _tenants.emplace(sg, make_lw_shared<request_memory_tenant>(sstring(sg.name()), 0, _pool)).first;
    }
    auto& tenant = *it->second;
    const ssize_t diff = static_cast<ssize_t>(shares) - static_cast<ssize_t>(tenant.weight());
    if (diff == 0) {
        return;
    }
    tenant.set_weight(shares);
    _total_weight = static_cast<size_t>(static_cast<ssize_t>(_total_weight) + diff);
    adjust();
}

void memory_limiter::remove(scheduling_group sg) {
    auto node = _tenants.extract(sg);
    if (node.empty()) {
        return;
    }
    auto tenant = std::move(node.mapped());
    _total_weight -= tenant->weight();
    tenant->set_weight(0);
    // Requests keep arriving in this scheduling group until the connections
    // using it are reclassified, which does not happen when the service level is
    // removed, so the tenant has to keep admitting. It gives up its dedicated
    // share and lives off the shared pool until it has drained.
    tenant->start_draining();
    // If this was the fallback - it should not be, the default service level is
    // not removed - leave it as the fallback. A draining tenant still admits out
    // of the shared pool, and reap_drained() will not drop one that is still
    // referenced.
    _draining.push_back(std::move(tenant));
    adjust();
}

void memory_limiter::set_fallback(scheduling_group sg) {
    auto it = _tenants.find(sg);
    if (it == _tenants.end()) {
        return;
    }
    auto previous = std::exchange(_fallback, it->second);
    _fallback_counted_elsewhere = true;
    if (previous.use_count() > 1) {
        // Connections opened before the service levels were known still point at
        // the tenant we started with, so keep it with the draining ones: it goes
        // on admitting out of the shared pool and is reaped once they are gone.
        previous->start_draining();
        _draining.push_back(std::move(previous));
    }
}

void memory_limiter::reap_drained() noexcept {
    // A draining tenant is dropped once it has no memory outstanding and nobody
    // else holds a reference to it; a connection can still be pointing at one.
    std::erase_if(_draining, [] (const tenant_ptr& tenant) {
        return tenant.use_count() == 1 && tenant->drained();
    });
}

} // namespace service
