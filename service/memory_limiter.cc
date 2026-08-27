/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 *
 * Copyright (C) 2021-present ScyllaDB
 *
 */

#include "service/memory_limiter.hh"

#include "service/qos/service_level_controller.hh"
#include "utils/log.hh"

#include <seastar/core/coroutine.hh>

#include <cmath>

namespace service {

namespace {

// A service level only gets a tenant of its own when it has shares; the rest
// fall back to the default service level's tenant, the same way reads do.
std::optional<int32_t> shares_of(const qos::service_level_options& slo) {
    if (auto* shares = std::get_if<int32_t>(&slo.shares)) {
        return *shares;
    }
    return std::nullopt;
}

} // anonymous namespace

memory_limiter::memory_limiter(size_t available_memory, double fraction,
        utils::updateable_value<double> shared_pool_fraction, bool with_metrics)
    : _max_request_size(budget(available_memory, fraction))
    , _total_memory(budget(available_memory, fraction))
    , _unlimited(false)
    , _shared_pool_fraction(std::move(shared_pool_fraction))
    , _fallback(make_lw_shared<request_memory_tenant>("unclassified", 0, _pool, with_metrics))
    , _use_metrics(with_metrics)
    , _shared_pool_fraction_observer(_shared_pool_fraction.observe([this] (const double&) { adjust(); }))
{
    if (_use_metrics) {
        _pool.register_metrics();
    }
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
    , _use_metrics(false)
    , _shared_pool_fraction_observer(_shared_pool_fraction.observe([this] (const double&) { adjust(); }))
{
    _fallback->set_capacity(_total_memory);
}

memory_limiter::~memory_limiter() = default;

future<> memory_limiter::start(sharded<qos::service_level_controller>& sl_controller) {
    if (_unlimited) {
        return make_ready_future<>();
    }
    auto& controller = sl_controller.local();
    controller.register_subscriber(this);
    _unsubscribe_qos_configuration_change = [this, &sl_controller] {
        return sl_controller.local().unregister_subscriber(this);
    };

    // The default service level's tenant also serves every scheduling group that
    // has no tenant of its own. It has to be set up here, because the default
    // service level is created by the controller's own start() and its
    // notification was therefore missed.
    //
    // The other service levels are not enumerated here: they all arrive through
    // on_before_service_level_add(). At boot the controller seeds its cache from
    // the system tables (update_cache() in main.cc, after the distributed data
    // accessor is installed, which is after this point) and that notifies every
    // shard's subscribers - before the CQL server starts accepting connections.
    const auto& default_sl = controller.get_service_level(qos::service_level_controller::default_service_level_name);
    const auto default_shares = shares_of(default_sl.slo);
    if (!default_shares) {
        on_internal_error(rml_logger, "the default service level should always have a shares value");
    }
    add_or_update(default_sl.sg, *default_shares);
    set_fallback(default_sl.sg);
    return make_ready_future<>();
}

future<> memory_limiter::on_before_service_level_add(qos::service_level_options slo, qos::service_level_info sl_info) {
    if (auto shares = shares_of(slo)) {
        add_or_update(sl_info.sg, *shares);
    }
    return make_ready_future<>();
}

future<> memory_limiter::on_before_service_level_change(qos::service_level_options slo_before,
        qos::service_level_options slo_after, qos::service_level_info sl_info) {
    if (auto shares = shares_of(slo_after)) {
        add_or_update(sl_info.sg, *shares);
    }
    return make_ready_future<>();
}

future<> memory_limiter::on_after_service_level_remove(qos::service_level_info sl_info) {
    remove(sl_info.sg);
    return make_ready_future<>();
}

future<> memory_limiter::on_effective_service_levels_cache_reloaded() {
    // A tenant that finished draining after the last service level change would
    // otherwise sit in _draining until the next one, so reap here too: this runs
    // whenever roles or service levels change, and unlike the shared pool's
    // repayment path it is not inside a semaphore signal.
    reap_drained();
    return make_ready_future<>();
}

future<> memory_limiter::unsubscribe_qos() {
    if (_unsubscribe_qos_configuration_change) {
        co_await std::exchange(_unsubscribe_qos_configuration_change, {})();
    }
}

future<> memory_limiter::stop() {
    // Only the service level subscription is torn down. Waiting for the request
    // memory to come back is deliberately not attempted: a request holds its
    // charge until its response has been written to a client that may never read
    // it, so this would not be bounded. main.cc does not stop the CQL server's
    // limiter for the same reason.
    return unsubscribe_qos();
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
        it = _tenants.emplace(sg, make_lw_shared<request_memory_tenant>(sg.name(), 0, _pool, _use_metrics)).first;
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
