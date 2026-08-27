/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 *
 * Copyright (C) 2026-present ScyllaDB
 *
 */

#pragma once

#include "seastarx.hh"

#include <seastar/core/semaphore.hh>
#include <seastar/core/sstring.hh>

#include <deque>

namespace service {

class request_memory_tenant;

// Memory shared between several request_memory_tenants.
//
// Each tenant gets a dedicated share of the request memory budget that no other
// tenant can take, and the rest of the budget is held here. A tenant that
// exhausts its own share borrows from the pool and repays as soon as its own
// share covers its consumption again, so the pool stays available for whoever
// needs it next.
//
// Tenants that could not be funded register for a wakeup. Whenever memory comes
// back, every registered tenant is offered a turn in registration order, so one
// large repayment can unblock several tenants and none is skipped.
class request_memory_pool final : public seastar::semaphore_borrow_source {
    ssize_t _total = 0;
    // _total minus what is lent out. Goes negative when the pool is shrunk
    // below what is already lent out; available() reports 0 until the borrowers
    // repay the difference.
    ssize_t _available = 0;
    std::deque<request_memory_tenant*> _notify_list;
    // A tenant woken from on_repaid() borrows and repays, which calls back in
    // here; coalesce those into the outermost pass instead of recursing.
    bool _dispatching = false;
    bool _dispatch_again = false;

    void notify_waiters() noexcept;
    void offer_to_waiters() noexcept;

public:
    // seastar::semaphore_borrow_source
    ssize_t available() const noexcept override;
    ssize_t lendable_total() const noexcept override { return _total; }
    void borrow(ssize_t amount) noexcept override;
    void repay(ssize_t amount) noexcept override;
    void on_repaid() noexcept override;

    ssize_t total_memory() const noexcept { return _total; }
    ssize_t available_memory() const noexcept { return available(); }

    // Resizes the pool.
    void set_total_memory(ssize_t total) noexcept;

    // Registering is idempotent. A tenant unregisters itself when destroyed.
    void request_wakeup(request_memory_tenant& tenant) noexcept;
    void unregister_wakeup(request_memory_tenant& tenant) noexcept;

    size_t waiting_tenants() const noexcept { return _notify_list.size(); }
};

// One tenant of the request memory budget: a scheduling group, which in
// practice means a service level.
//
// A request is admitted against the tenant's own dedicated memory first and
// against the shared pool once that runs out, so a flood in one service level
// can never take another service level's dedicated share.
class request_memory_tenant {
    friend class request_memory_pool;

    sstring _name;
    size_t _weight;
    request_memory_pool& _pool;
    semaphore _sem;
    uint64_t _blocked_total = 0;
    bool _on_notify_list = false;
    bool _draining = false;

    void note_blocked() noexcept;
    // Called by the pool when it has memory to offer. Re-runs admission, which
    // borrows from the pool if the head of the queue now fits.
    void wake() noexcept { _sem.adjust_capacity(0); }

public:
    request_memory_tenant(sstring name, size_t weight, request_memory_pool& pool);
    ~request_memory_tenant();

    request_memory_tenant(const request_memory_tenant&) = delete;
    request_memory_tenant& operator=(const request_memory_tenant&) = delete;

    // Reserves `amount` bytes for one request. Releasing the returned units
    // returns the memory to this tenant, and to the shared pool if it was
    // borrowed from there.
    future<semaphore_units<>> get_units(size_t amount);

    // As above, but gives up after `timeout`. Used for CQL load shedding.
    future<semaphore_units<>> get_units(size_t amount, semaphore::duration timeout);

    // Charges `amount` without waiting, for topping a reservation up once the
    // real cost is known. May take the tenant over its share.
    semaphore_units<> consume(size_t amount) noexcept {
        return seastar::consume_units(_sem, amount);
    }

    semaphore& sem() noexcept { return _sem; }

    const sstring& name() const noexcept { return _name; }
    void rename(sstring name) { _name = std::move(name); }

    size_t weight() const noexcept { return _weight; }
    void set_weight(size_t weight) noexcept { _weight = weight; }

    // The tenant's dedicated share of the budget.
    size_t capacity() const noexcept { return _sem.capacity(); }
    void set_capacity(size_t capacity) noexcept { _sem.set_capacity(capacity); }

    // Memory left in this tenant's own share. Negative when a request was
    // admitted over the share to guarantee forward progress.
    ssize_t available() const noexcept { return _sem.available_units(); }
    // Memory currently borrowed from the shared pool.
    ssize_t borrowed() const noexcept { return _sem.borrowed(); }
    size_t waiters() const noexcept { return _sem.waiters(); }
    // Requests that had to wait for memory.
    uint64_t blocked_total() const noexcept { return _blocked_total; }

    // A draining tenant belongs to a service level that was removed. It gives
    // up its dedicated share but keeps admitting requests out of the shared
    // pool, because connections are not reclassified the moment a service
    // level goes away.
    void start_draining() noexcept;
    bool draining() const noexcept { return _draining; }
    // True once nothing is outstanding, so the tenant can be destroyed.
    bool drained() const noexcept;
};

} // namespace service
