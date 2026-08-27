/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 *
 * Copyright (C) 2021-present ScyllaDB
 *
 */

#pragma once

#include "seastarx.hh"
#include "service/qos/qos_configuration_change_subscriber.hh"
#include "service/request_memory_limiter.hh"
#include "utils/updateable_value.hh"
#include <seastar/core/sharded.hh>
#include <seastar/util/noncopyable_function.hh>

#include <seastar/core/scheduling.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>

#include <algorithm>
#include <unordered_map>
#include <vector>

namespace qos {
class service_level_controller;
}

namespace service {

// Reserves a share of shard memory for admitting client requests, and caps how
// large a single request may be.
//
// The budget is split between tenants, one per scheduling group, which in
// practice means one per service level:
//
//  - a dedicated portion, (1 - shared_pool_fraction) of the budget, divided
//    between the tenants in proportion to their service level shares and which
//    no other tenant can take;
//  - a shared pool, the remaining shared_pool_fraction, which any tenant may
//    borrow from once its own share runs out.
//
// So a flood of requests in one service level can never stop another service
// level from being served.
class memory_limiter final : public qos::qos_configuration_change_subscriber {
public:
    // Tag selecting a limiter that admits every request. The CQL maintenance
    // socket uses it: it is the operator's escape hatch, so it must stay usable
    // no matter how much memory user requests are holding. The request size
    // limit still applies.
    struct unlimited_tag {};

    using tenant_ptr = lw_shared_ptr<request_memory_tenant>;

private:
    size_t _max_request_size;
    size_t _total_memory;
    bool _unlimited;
    utils::updateable_value<double> _shared_pool_fraction;
    // Declared before the tenants so that it outlives them.
    request_memory_pool _pool;
    std::unordered_map<scheduling_group, tenant_ptr> _tenants;
    // Tenants of service levels that were removed. They keep admitting requests
    // out of the shared pool until the memory they handed out comes back.
    std::vector<tenant_ptr> _draining;
    // Used for a scheduling group that has no tenant of its own, e.g. a
    // connection that has not authenticated yet.
    tenant_ptr _fallback;
    // Set once _fallback aliases the default service level's tenant, which is
    // then already accounted for in _tenants or, if it is ever removed, in
    // _draining. Keeps the sums below from counting it twice.
    bool _fallback_counted_elsewhere = false;
    size_t _total_weight = 0;
    bool _use_metrics;
    noncopyable_function<future<>()> _unsubscribe_qos_configuration_change;
    utils::observer<double> _shared_pool_fraction_observer;

    // A fraction of 0 would block every request forever, and more than the whole
    // shard is meaningless, so out-of-range values are clamped rather than rejected.
    static size_t budget(size_t available_memory, double fraction) noexcept {
        return available_memory * std::clamp(fraction, 0.01, 1.0);
    }

    void reap_drained() noexcept;

public:
    // `with_metrics` should be set on exactly one limiter per shard, since the
    // per-service-level metrics are not namespaced by limiter.
    memory_limiter(size_t available_memory, double fraction,
            utils::updateable_value<double> shared_pool_fraction, bool with_metrics = false);
    memory_limiter(unlimited_tag, size_t available_memory, double fraction);
    ~memory_limiter();

    // Registers for service level changes and creates the default service
    // level's tenant, which doubles as the fallback. Must be called after the
    // service level controller has started; every other service level's tenant
    // is created from the subscription.
    future<> start(sharded<qos::service_level_controller>& sl_controller);

    // Stops following service level changes. Split out of stop() because the CQL
    // server's limiter is deliberately never stopped, see main.cc.
    future<> unsubscribe_qos();

    future<> stop();

    // qos::qos_configuration_change_subscriber
    future<> on_before_service_level_add(qos::service_level_options slo, qos::service_level_info sl_info) override;
    future<> on_after_service_level_remove(qos::service_level_info sl_info) override;
    future<> on_before_service_level_change(qos::service_level_options slo_before,
            qos::service_level_options slo_after, qos::service_level_info sl_info) override;
    future<> on_effective_service_levels_cache_reloaded() override;

    // Memory reserved for admitting requests.
    size_t total_memory() const noexcept { return _total_memory; }

    // Largest single request accepted. Kept apart from total_memory() so that an
    // unlimited limiter still rejects absurdly large frames.
    size_t max_request_size() const noexcept { return _max_request_size; }

    // Re-splits the budget between the dedicated shares and the shared pool.
    // Called whenever a tenant is added, removed or reweighted, and whenever the
    // shared pool fraction is updated at runtime.
    void adjust() noexcept;

    // Adds a tenant for `sg`, or updates the shares of the existing one.
    void add_or_update(scheduling_group sg, size_t shares);

    // Retires the tenant for `sg`. It keeps admitting requests out of the shared
    // pool until the memory it handed out is returned.
    void remove(scheduling_group sg);

    // Uses the tenant for `sg` as the fallback for scheduling groups that have
    // none of their own.
    void set_fallback(scheduling_group sg);

    request_memory_tenant& tenant_for(scheduling_group sg) noexcept {
        auto it = _tenants.find(sg);
        return it != _tenants.end() ? *it->second : *_fallback;
    }
    tenant_ptr shared_tenant_for(scheduling_group sg) noexcept {
        auto it = _tenants.find(sg);
        return it != _tenants.end() ? it->second : _fallback;
    }
    request_memory_tenant& tenant_for_current_scheduling_group() noexcept {
        return tenant_for(current_scheduling_group());
    }

    const request_memory_pool& pool() const noexcept { return _pool; }

    // Transitional, until the CQL server and Alternator admit against tenants:
    // everything goes through the fallback tenant, which is the only tenant
    // there is until service levels are wired up.
    semaphore& get_semaphore() noexcept { return _fallback->sem(); }

    // Memory available for admitting new requests: what is left of the tenants'
    // own shares, plus what the shared pool has not lent out. Clamped at zero,
    // because a tenant's share goes negative when a request larger than the
    // whole share is let through for forward progress.
    size_t available_memory() const noexcept {
        const ssize_t available = sum_over_tenants(&request_memory_tenant::available) + _pool.available_memory();
        return static_cast<size_t>(std::max<ssize_t>(available, 0));
    }

    // Sums a per-tenant stat over every tenant, for the shard-wide metrics that
    // predate the per-service-level split.
    template <typename Ret>
    Ret sum_over_tenants(Ret (request_memory_tenant::*stat)() const noexcept) const {
        Ret sum{};
        for (const auto& [sg, tenant] : _tenants) {
            sum += (tenant.get()->*stat)();
        }
        for (const auto& tenant : _draining) {
            sum += (tenant.get()->*stat)();
        }
        if (!_fallback_counted_elsewhere) {
            sum += (_fallback.get()->*stat)();
        }
        return sum;
    }
};

} // namespace service
