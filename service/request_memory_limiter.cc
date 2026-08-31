/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 *
 * Copyright (C) 2026-present ScyllaDB
 *
 */

#include "service/request_memory_limiter.hh"

#include "utils/log.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/format.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/on_internal_error.hh>

#include <algorithm>

namespace service {

logging::logger rml_logger("request_memory_limiter");

namespace sm = seastar::metrics;

static const sm::label scheduling_group_label("scheduling_group_name");

void request_memory_pool::register_metrics() {
    _metrics.add_group("transport", {
        sm::make_gauge("cql_requests_shared_pool_total_memory", [this] { return _total; },
                sm::description("Holds the size of the pool of request memory shared by all service levels.")),
        sm::make_gauge("cql_requests_shared_pool_available_memory", [this] { return available(); },
                sm::description("Holds the amount of memory in the shared request memory pool that is not lent out. "
                                "Zero means every service level that needs more than its own share is waiting.")),
        sm::make_gauge("cql_requests_shared_pool_waiting_service_levels", [this] { return _notify_list.size(); },
                sm::description("Holds the number of service levels waiting for memory from the shared request memory pool.")),
    });
}

void request_memory_tenant::register_metrics() {
    if (!_use_metrics) {
        return;
    }
    auto metrics = sm::metric_groups();
    metrics.add_group("transport", {
        sm::make_gauge("cql_requests_memory_total", [this] { return capacity(); },
                sm::description("Holds this service level's own share of the request memory budget, which no other "
                                "service level can take."),
                {scheduling_group_label(_name)}),
        sm::make_gauge("cql_requests_memory_available", [this] { return available(); },
                sm::description("Holds how much of this service level's own share is available for admitting new "
                                "requests. Negative when a single request larger than the whole share was admitted "
                                "to guarantee forward progress."),
                {scheduling_group_label(_name)}),
        sm::make_gauge("cql_requests_memory_borrowed_from_shared_pool", [this] { return borrowed(); },
                sm::description("Holds how much memory this service level has borrowed from the shared pool because "
                                "its own share was exhausted."),
                {scheduling_group_label(_name)}),
        sm::make_gauge("cql_requests_blocked_memory_current", [this] { return waiters(); },
                sm::description("Holds the number of requests in this service level currently waiting for memory. "
                                "Non-zero means this service level is the one being throttled."),
                {scheduling_group_label(_name)}),
        sm::make_counter("cql_requests_blocked_memory", [this] { return _blocked_total; },
                sm::description("Counts the requests in this service level that had to wait for memory."),
                {scheduling_group_label(_name)}),
    });
    _metrics = std::exchange(metrics, {});
}

ssize_t request_memory_pool::available() const noexcept {
    return std::max(_available, ssize_t(0));
}

void request_memory_pool::borrow(ssize_t amount) noexcept {
    // Borrowers only take up to available(), so this should never go negative
    // on its own. It is left negative rather than clamped when it does, so that
    // the pool's accounting stays consistent and settles as borrowers repay.
    _available -= amount;
    if (_available < 0) [[unlikely]] {
        static thread_local logger::rate_limit rate_limit(std::chrono::seconds(30));
        rml_logger.log(log_level::warn, rate_limit,
                "request memory pool over-borrowed: available={}, total={}, borrowed={}",
                _available, _total, amount);
    }
}

void request_memory_pool::repay(ssize_t amount) noexcept {
    _available += amount;
    if (_available > _total) [[unlikely]] {
        static thread_local logger::rate_limit rate_limit(std::chrono::seconds(30));
        rml_logger.log(log_level::warn, rate_limit,
                "request memory pool over-repaid: available={}, total={}, repaid={}",
                _available, _total, amount);
    }
}

void request_memory_pool::on_repaid() noexcept {
    notify_waiters();
}

void request_memory_pool::notify_waiters() noexcept {
    if (_dispatching) {
        // We are being called from inside a tenant we just woke. Let the
        // outermost pass pick the change up rather than recursing.
        _dispatch_again = true;
        return;
    }
    _dispatching = true;
    do {
        _dispatch_again = false;
        offer_to_waiters();
    } while (_dispatch_again);
    _dispatching = false;
}

void request_memory_pool::offer_to_waiters() noexcept {
    // Give every registered tenant a turn, bounded by how many were registered
    // when the pass started: one large repayment can then unblock several
    // tenants, while a tenant that re-registers does not get a second turn
    // ahead of the others.
    //
    // Every tenant is woken even when there is nothing to lend. A tenant may be
    // able to make progress precisely because the pool shrank: a request that
    // needs more than the tenant's share plus the whole pool is let through once
    // nothing else is outstanding, and only the tenant can work that out.
    for (size_t turns = _notify_list.size(); turns && !_notify_list.empty(); --turns) {
        auto& tenant = *_notify_list.front();
        _notify_list.pop_front();
        tenant._on_notify_list = false;
        if (tenant.waiters() == 0) {
            continue;
        }
        tenant.wake();
        if (tenant.waiters() != 0) {
            // Still blocked. Back of the queue, so it gets another turn without
            // starving the tenants behind it.
            request_wakeup(tenant);
        }
    }
}

void request_memory_pool::set_total_memory(ssize_t total) noexcept {
    const ssize_t diff = total - _total;
    if (diff == 0) {
        return;
    }
    _total = total;
    _available += diff;
    // Both directions matter. Growing gives the blocked tenants something to
    // borrow; shrinking lowers what they can ever reach, which can make an
    // already queued request eligible to be let through for forward progress.
    // Nothing else would look at it again.
    notify_waiters();
}

void request_memory_pool::request_wakeup(request_memory_tenant& tenant) noexcept {
    if (!tenant._on_notify_list) {
        tenant._on_notify_list = true;
        _notify_list.push_back(&tenant);
    }
}

void request_memory_pool::unregister_wakeup(request_memory_tenant& tenant) noexcept {
    if (tenant._on_notify_list) {
        tenant._on_notify_list = false;
        std::erase(_notify_list, &tenant);
    }
}

request_memory_tenant::request_memory_tenant(sstring name, size_t weight, request_memory_pool& pool,
        bool with_metrics)
    : _name(std::move(name))
    , _weight(weight)
    , _pool(pool)
    , _sem(0)
    , _use_metrics(with_metrics)
{
    register_metrics();
    _sem.set_borrow_source(&_pool);
    // A request needing more than this tenant's share plus the whole pool would
    // otherwise never be admitted. Let one such request through at a time, once
    // nothing else is outstanding, so every tenant keeps making progress even
    // when its share is resized underneath a queued request.
    _sem.set_admit_oversized_when_idle(true);
}

request_memory_tenant::~request_memory_tenant() {
    _pool.unregister_wakeup(*this);
    if (!drained()) {
        // This is not recoverable: the units handed out hold a raw pointer to
        // the semaphore below, so whoever releases one after this returns writes
        // to freed memory. The owner is expected to keep a tenant alive until it
        // has drained - hence an internal error rather than a mere log line.
        on_internal_error_noexcept(rml_logger, seastar::format(
                "request memory tenant {} destroyed with {} bytes borrowed and {} waiters",
                _name, _sem.borrowed(), _sem.waiters()));
        // Where internal errors are not fatal, at least keep the pool's
        // accounting from drifting: this repays what was borrowed and fails the
        // waiters.
        _sem.broken();
    }
    _sem.set_borrow_source(nullptr);
}

void request_memory_tenant::note_blocked() noexcept {
    ++_blocked_total;
    // Memory of our own may not come back in time, so ask the pool for a turn
    // when another tenant returns some.
    _pool.request_wakeup(*this);
}

future<semaphore_units<>> request_memory_tenant::get_units(size_t amount) {
    auto f = seastar::get_units(_sem, amount);
    if (!f.available()) {
        note_blocked();
    }
    return f;
}

future<semaphore_units<>> request_memory_tenant::get_units(size_t amount, semaphore::duration timeout) {
    auto f = seastar::get_units(_sem, amount, timeout);
    if (f.available()) {
        return f;
    }
    note_blocked();
    return f.then_wrapped([this] (future<semaphore_units<>> f) {
        if (f.failed()) {
            // A waiter that timed out leaves the queue without the admission
            // loop being re-run, so nudge the semaphore to let the next one in.
            _sem.adjust_capacity(0);
        }
        return f;
    });
}

void request_memory_tenant::start_draining() noexcept {
    _draining = true;
    // Stop reporting. A service level with the same name can be created again
    // while this tenant is still winding down, and registering the same metrics
    // under the same label twice is an error. The memory it still holds is
    // visible in the shard-wide totals.
    _use_metrics = false;
    _metrics = seastar::metrics::metric_groups();
    set_capacity(0);
}

bool request_memory_tenant::drained() const noexcept {
    return _sem.waiters() == 0 && _sem.borrowed() == 0
            && _sem.available_units() >= static_cast<ssize_t>(_sem.capacity());
}

} // namespace service
