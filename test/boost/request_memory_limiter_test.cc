/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "service/memory_limiter.hh"
#include "service/request_memory_limiter.hh"
#include "utils/updateable_value.hh"

#undef SEASTAR_TESTING_MAIN
#include <seastar/core/reactor.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>
#include <boost/test/unit_test.hpp>

#include <chrono>

using service::request_memory_pool;
using service::request_memory_tenant;

BOOST_AUTO_TEST_SUITE(request_memory_limiter_test)

// A tenant is constructed with no dedicated share, so give it one right away.
struct test_tenant : request_memory_tenant {
    test_tenant(request_memory_pool& pool, sstring name, size_t capacity)
        : request_memory_tenant(std::move(name), 1, pool)
    {
        set_capacity(capacity);
    }
};

// Memory is handed back and dispatched synchronously, but the future returned by
// get_units() is resolved through a continuation, so let the reactor run its
// task queue before checking whether a request was admitted.
static void run_pending_tasks() {
    for (int i = 0; i < 4; ++i) {
        seastar::yield().get();
    }
}

// A request that fits in the tenant's own share is funded from there, leaving
// the shared pool alone.
SEASTAR_THREAD_TEST_CASE(test_request_memory_own_memory_first) {
    request_memory_pool pool;
    test_tenant tenant(pool, "t", 100);

    BOOST_REQUIRE_EQUAL(tenant.capacity(), 100u);
    BOOST_REQUIRE_EQUAL(tenant.available(), 100);

    auto f = tenant.get_units(60);
    BOOST_REQUIRE(f.available());
    auto units = f.get();

    BOOST_REQUIRE_EQUAL(tenant.available(), 40);
    BOOST_REQUIRE_EQUAL(tenant.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(tenant.blocked_total(), 0u);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 0);

    units.return_all();
    BOOST_REQUIRE_EQUAL(tenant.available(), 100);
    BOOST_REQUIRE(tenant.drained());
}

// What the tenant's own share cannot cover is borrowed from the pool.
SEASTAR_THREAD_TEST_CASE(test_request_memory_borrow_shortfall) {
    request_memory_pool pool;
    pool.set_total_memory(100);
    test_tenant tenant(pool, "t", 100);

    auto f = tenant.get_units(150);
    BOOST_REQUIRE(f.available());
    auto units = f.get();

    BOOST_REQUIRE_EQUAL(tenant.available(), 0);
    BOOST_REQUIRE_EQUAL(tenant.borrowed(), 50);
    BOOST_REQUIRE_EQUAL(tenant.blocked_total(), 0u);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 50);
    BOOST_REQUIRE_EQUAL(pool.total_memory(), 100);

    units.return_all();
    BOOST_REQUIRE(tenant.drained());
}

// Released memory repays the pool before the tenant keeps any of it.
SEASTAR_THREAD_TEST_CASE(test_request_memory_repay_on_release) {
    request_memory_pool pool;
    pool.set_total_memory(100);
    test_tenant tenant(pool, "t", 100);

    auto units = tenant.get_units(150).get();
    BOOST_REQUIRE_EQUAL(tenant.borrowed(), 50);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 50);

    units.return_all();

    BOOST_REQUIRE_EQUAL(tenant.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 100);
    BOOST_REQUIRE_EQUAL(tenant.available(), 100);
    BOOST_REQUIRE(tenant.drained());
}

// Borrowing is all or nothing: a partial borrow could not admit the request
// anyway, and would keep memory away from the tenants that can use it.
SEASTAR_THREAD_TEST_CASE(test_request_memory_borrow_is_all_or_nothing) {
    request_memory_pool pool;
    pool.set_total_memory(20);
    test_tenant tenant(pool, "t", 10);

    // Keep a request in flight: a tenant with nothing outstanding admits even a
    // request it cannot fund, to guarantee forward progress (see
    // test_request_memory_oversized_request_forward_progress).
    auto in_flight = tenant.get_units(10).get();
    BOOST_REQUIRE_EQUAL(tenant.available(), 0);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 20);

    // 100 is more than the tenant's share plus the whole pool.
    auto f = tenant.get_units(100);
    BOOST_REQUIRE(!f.available());
    BOOST_REQUIRE_EQUAL(tenant.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(tenant.waiters(), 1u);
    BOOST_REQUIRE_EQUAL(tenant.blocked_total(), 1u);
    // The pool was left untouched.
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 20);
    BOOST_REQUIRE_EQUAL(pool.total_memory(), 20);

    // Grow the pool so the queued request can be funded and the tenant drains.
    pool.set_total_memory(1000);
    run_pending_tasks();
    BOOST_REQUIRE(f.available());
    auto units = f.get();
    BOOST_REQUIRE_EQUAL(tenant.borrowed(), 100);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 900);

    units.return_all();
    in_flight.return_all();
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 1000);
    BOOST_REQUIRE(tenant.drained());
}

// The point of the whole feature: a flood in one tenant cannot reach into
// another tenant's dedicated share.
SEASTAR_THREAD_TEST_CASE(test_request_memory_tenant_isolation) {
    request_memory_pool pool;
    pool.set_total_memory(100);
    test_tenant a(pool, "a", 100);
    test_tenant b(pool, "b", 100);

    // b floods: its own share plus the whole pool.
    auto fb = b.get_units(200);
    BOOST_REQUIRE(fb.available());
    auto ub = fb.get();
    BOOST_REQUIRE_EQUAL(b.borrowed(), 100);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 0);

    // a's dedicated memory was never taken by b.
    BOOST_REQUIRE_EQUAL(a.available(), 100);
    auto fa = a.get_units(100);
    BOOST_REQUIRE(fa.available());
    auto ua = fa.get();
    BOOST_REQUIRE_EQUAL(a.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(a.blocked_total(), 0u);
    BOOST_REQUIRE_EQUAL(a.available(), 0);

    // Not one byte more, though: the pool is empty.
    auto fa2 = a.get_units(1);
    BOOST_REQUIRE(!fa2.available());
    BOOST_REQUIRE_EQUAL(a.blocked_total(), 1u);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 0);
    BOOST_REQUIRE_EQUAL(pool.waiting_tenants(), 1u);

    // b returns everything, and a's byte is funded out of the repaid pool.
    ub.return_all();
    run_pending_tasks();
    BOOST_REQUIRE(fa2.available());
    auto ua2 = fa2.get();
    BOOST_REQUIRE_EQUAL(a.borrowed(), 1);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 99);
    BOOST_REQUIRE(b.drained());

    ua2.return_all();
    ua.return_all();
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 100);
    BOOST_REQUIRE(a.drained());
}

// Memory returned by one tenant unblocks a request in another tenant.
SEASTAR_THREAD_TEST_CASE(test_request_memory_cross_tenant_wakeup) {
    request_memory_pool pool;
    pool.set_total_memory(100);
    // No dedicated share at all: everything comes from the pool.
    test_tenant a(pool, "a", 0);
    test_tenant b(pool, "b", 0);

    auto ub1 = b.get_units(50).get();
    auto ua = a.get_units(50).get();
    BOOST_REQUIRE_EQUAL(a.borrowed(), 50);
    BOOST_REQUIRE_EQUAL(b.borrowed(), 50);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 0);

    // b has a request in flight already, so this one waits for the pool instead
    // of being let through for forward progress.
    auto fb2 = b.get_units(50);
    BOOST_REQUIRE(!fb2.available());
    BOOST_REQUIRE_EQUAL(b.blocked_total(), 1u);
    BOOST_REQUIRE_EQUAL(pool.waiting_tenants(), 1u);

    // a's release repays the pool, which offers the memory to b.
    ua.return_all();
    BOOST_REQUIRE_EQUAL(a.borrowed(), 0);
    run_pending_tasks();
    BOOST_REQUIRE(fb2.available());
    auto ub2 = fb2.get();
    BOOST_REQUIRE_EQUAL(b.borrowed(), 100);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 0);
    // b is no longer waiting, so it left the notify list.
    BOOST_REQUIRE_EQUAL(pool.waiting_tenants(), 0u);
    BOOST_REQUIRE(a.drained());

    ub1.return_all();
    ub2.return_all();
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 100);
    BOOST_REQUIRE(b.drained());
}

// One repayment runs a complete pass over the notify list, so it can unblock
// several tenants instead of just the one at the head.
SEASTAR_THREAD_TEST_CASE(test_request_memory_complete_dispatch_pass) {
    request_memory_pool pool;
    pool.set_total_memory(100);
    test_tenant hog(pool, "hog", 0);
    test_tenant a(pool, "a", 40);
    test_tenant b(pool, "b", 40);
    test_tenant c(pool, "c", 40);

    // a, b and c have used up their own shares, so anything more has to come
    // from the pool - which the hog has emptied.
    auto ua = a.get_units(40).get();
    auto ub = b.get_units(40).get();
    auto uc = c.get_units(40).get();
    auto uhog = hog.get_units(100).get();
    BOOST_REQUIRE_EQUAL(hog.borrowed(), 100);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 0);

    auto fa = a.get_units(40);
    auto fb = b.get_units(40);
    auto fc = c.get_units(40);
    BOOST_REQUIRE(!fa.available());
    BOOST_REQUIRE(!fb.available());
    BOOST_REQUIRE(!fc.available());
    BOOST_REQUIRE_EQUAL(pool.waiting_tenants(), 3u);

    // The hog repays 100 in one go. The dispatch pass offers it to a, b and c in
    // registration order: a and b are funded with 40 each and c does not fit in
    // the remaining 20, so it goes to the back of the notify list. Waking only
    // the head of the list would have left b waiting with 60 bytes idle in the
    // pool.
    uhog.return_all();
    run_pending_tasks();
    BOOST_REQUIRE(fa.available());
    BOOST_REQUIRE(fb.available());
    BOOST_REQUIRE(!fc.available());
    auto ua2 = fa.get();
    auto ub2 = fb.get();
    BOOST_REQUIRE_EQUAL(a.borrowed(), 40);
    BOOST_REQUIRE_EQUAL(b.borrowed(), 40);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 20);
    BOOST_REQUIRE(hog.drained());
    // c re-registered, so it is not starved by the tenants ahead of it.
    BOOST_REQUIRE_EQUAL(pool.waiting_tenants(), 1u);

    // c gets its turn as soon as enough memory is repaid.
    ua2.return_all();
    run_pending_tasks();
    BOOST_REQUIRE(fc.available());
    auto uc2 = fc.get();
    BOOST_REQUIRE_EQUAL(c.borrowed(), 40);

    uc2.return_all();
    ub2.return_all();
    ua.return_all();
    ub.return_all();
    uc.return_all();
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 100);
    BOOST_REQUIRE(a.drained());
    BOOST_REQUIRE(b.drained());
    BOOST_REQUIRE(c.drained());
}

// A tenant whose request can never be funded sits at the head of the notify
// list without shadowing a smaller request in another tenant.
SEASTAR_THREAD_TEST_CASE(test_request_memory_oversized_head_does_not_block_other_tenants) {
    request_memory_pool pool;
    pool.set_total_memory(100);
    test_tenant a(pool, "a", 0);
    test_tenant b(pool, "b", 0);
    test_tenant hog(pool, "hog", 0);

    // One request in flight per tenant, so the forward-progress exception does
    // not apply to what follows.
    auto ua = a.get_units(1).get();
    auto ub = b.get_units(1).get();
    auto uhog = hog.get_units(98).get();
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 0);

    // a asks for more than the whole pool can ever provide.
    auto fa = a.get_units(500);
    BOOST_REQUIRE(!fa.available());
    // b's request fits, but registered behind a.
    auto fb = b.get_units(50);
    BOOST_REQUIRE(!fb.available());
    BOOST_REQUIRE_EQUAL(pool.waiting_tenants(), 2u);

    uhog.return_all();
    run_pending_tasks();
    // a stays blocked, but it did not shadow b.
    BOOST_REQUIRE(!fa.available());
    BOOST_REQUIRE(fb.available());
    auto ub2 = fb.get();
    BOOST_REQUIRE_EQUAL(b.borrowed(), 51);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 48);
    BOOST_REQUIRE(hog.drained());
    BOOST_REQUIRE_EQUAL(pool.waiting_tenants(), 1u);

    // a is admitted for forward progress once it has nothing else outstanding,
    // over its share and without raiding the pool.
    ua.return_all();
    run_pending_tasks();
    BOOST_REQUIRE(fa.available());
    auto ua2 = fa.get();
    BOOST_REQUIRE_EQUAL(a.available(), -499);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 48);

    ua2.return_all();
    ub2.return_all();
    ub.return_all();
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 100);
    BOOST_REQUIRE(a.drained());
    BOOST_REQUIRE(b.drained());
}

// A request can give up waiting, and doing so must not leave the requests
// behind it stuck.
SEASTAR_THREAD_TEST_CASE(test_request_memory_timeout) {
    request_memory_pool pool; // no shared memory at all
    test_tenant tenant(pool, "t", 100);

    auto u1 = tenant.get_units(90).get();
    auto u2 = tenant.get_units(10).get();
    BOOST_REQUIRE_EQUAL(tenant.available(), 0);

    // Can never be funded: more than the share plus the (empty) pool, and the
    // tenant has requests in flight.
    auto f_timeout = tenant.get_units(1000, std::chrono::milliseconds(200));
    BOOST_REQUIRE(!f_timeout.available());
    // Queued behind it, and small enough to be admitted right away once the
    // request ahead of it is gone.
    auto f_next = tenant.get_units(10);
    BOOST_REQUIRE(!f_next.available());
    BOOST_REQUIRE_EQUAL(tenant.waiters(), 2u);
    BOOST_REQUIRE_EQUAL(tenant.blocked_total(), 2u);

    // 90 bytes come back, but the head of the queue still cannot be funded, so
    // both requests keep waiting.
    u1.return_all();
    run_pending_tasks();
    BOOST_REQUIRE(!f_timeout.available());
    BOOST_REQUIRE(!f_next.available());
    BOOST_REQUIRE_EQUAL(tenant.available(), 90);

    BOOST_REQUIRE_THROW(f_timeout.get(), seastar::semaphore_timed_out);

    // The expired request left the queue without seastar re-running admission,
    // so request_memory_tenant::get_units() nudges the semaphore. Without that
    // the next waiter would sit behind a request that is already gone, with
    // enough memory available to serve it.
    run_pending_tasks();
    BOOST_REQUIRE(f_next.available());
    auto u3 = f_next.get();
    BOOST_REQUIRE_EQUAL(tenant.available(), 80);
    BOOST_REQUIRE_EQUAL(tenant.waiters(), 0u);
    BOOST_REQUIRE_EQUAL(tenant.blocked_total(), 2u);

    u2.return_all();
    u3.return_all();
    BOOST_REQUIRE(tenant.drained());
}

// blocked_total() counts requests that actually had to wait.
SEASTAR_THREAD_TEST_CASE(test_request_memory_blocked_total) {
    request_memory_pool pool;
    test_tenant tenant(pool, "t", 100);

    auto u1 = tenant.get_units(50).get();
    auto u2 = tenant.get_units(50).get();
    BOOST_REQUIRE_EQUAL(tenant.blocked_total(), 0u);

    auto f = tenant.get_units(10);
    BOOST_REQUIRE(!f.available());
    BOOST_REQUIRE_EQUAL(tenant.blocked_total(), 1u);

    // Admitting a waiter is not a new block.
    u1.return_all();
    run_pending_tasks();
    BOOST_REQUIRE(f.available());
    auto u3 = f.get();
    BOOST_REQUIRE_EQUAL(tenant.blocked_total(), 1u);

    auto u4 = tenant.get_units(10).get();
    BOOST_REQUIRE_EQUAL(tenant.blocked_total(), 1u);

    // consume() never waits.
    auto u5 = tenant.consume(1000);
    BOOST_REQUIRE_EQUAL(tenant.blocked_total(), 1u);

    u5.return_all();
    u2.return_all();
    u3.return_all();
    u4.return_all();
    BOOST_REQUIRE(tenant.drained());
}

// A request larger than the tenant's share plus the whole pool is still
// admitted, one at a time, so the tenant always makes progress.
SEASTAR_THREAD_TEST_CASE(test_request_memory_oversized_request_forward_progress) {
    request_memory_pool pool;
    pool.set_total_memory(10);
    test_tenant tenant(pool, "t", 10);

    auto f1 = tenant.get_units(1000);
    BOOST_REQUIRE(f1.available());
    auto u1 = f1.get();
    BOOST_REQUIRE_EQUAL(tenant.available(), -990);
    BOOST_REQUIRE_EQUAL(tenant.blocked_total(), 0u);
    // Admitted over the tenant's share, not out of the pool.
    BOOST_REQUIRE_EQUAL(tenant.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 10);

    // Only one such request at a time.
    auto f2 = tenant.get_units(1000);
    BOOST_REQUIRE(!f2.available());
    BOOST_REQUIRE_EQUAL(tenant.blocked_total(), 1u);

    u1.return_all();
    run_pending_tasks();
    BOOST_REQUIRE(f2.available());
    auto u2 = f2.get();
    BOOST_REQUIRE_EQUAL(tenant.available(), -990);

    u2.return_all();
    BOOST_REQUIRE_EQUAL(tenant.available(), 10);
    BOOST_REQUIRE(tenant.drained());
}

// The dedicated share can be resized under way, including below what the tenant
// is already using.
SEASTAR_THREAD_TEST_CASE(test_request_memory_capacity_resize) {
    request_memory_pool pool; // no shared memory: only the tenant's own share
    test_tenant tenant(pool, "t", 100);

    tenant.set_capacity(200);
    BOOST_REQUIRE_EQUAL(tenant.capacity(), 200u);
    BOOST_REQUIRE_EQUAL(tenant.available(), 200);

    // Shrinking below the current consumption drives available() negative; it
    // recovers as the units are released.
    auto u = tenant.get_units(150).get();
    BOOST_REQUIRE_EQUAL(tenant.available(), 50);
    tenant.set_capacity(100);
    BOOST_REQUIRE_EQUAL(tenant.capacity(), 100u);
    BOOST_REQUIRE_EQUAL(tenant.available(), -50);
    BOOST_REQUIRE(!tenant.drained());
    u.return_all();
    BOOST_REQUIRE_EQUAL(tenant.available(), 100);
    BOOST_REQUIRE(tenant.drained());

    // Shrinking while a request is queued whose demand no longer fits in the
    // share plus the pool: it must still be admitted eventually.
    auto u2 = tenant.get_units(100).get();
    auto f = tenant.get_units(80);
    BOOST_REQUIRE(!f.available());
    tenant.set_capacity(10);
    run_pending_tasks();
    BOOST_REQUIRE(!f.available());
    BOOST_REQUIRE_EQUAL(tenant.available(), -90);

    u2.return_all();
    run_pending_tasks();
    BOOST_REQUIRE(f.available());
    auto u3 = f.get();
    BOOST_REQUIRE_EQUAL(tenant.available(), -70);

    u3.return_all();
    BOOST_REQUIRE_EQUAL(tenant.available(), 10);
    BOOST_REQUIRE(tenant.drained());
}

// Growing the dedicated share repays what the tenant borrowed, without waiting
// for the request to finish.
SEASTAR_THREAD_TEST_CASE(test_request_memory_capacity_growth_repays_pool) {
    request_memory_pool pool;
    pool.set_total_memory(100);
    test_tenant tenant(pool, "t", 100);

    auto u = tenant.get_units(150).get();
    BOOST_REQUIRE_EQUAL(tenant.borrowed(), 50);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 50);

    // Partial growth repays what the share now covers.
    tenant.set_capacity(120);
    BOOST_REQUIRE_EQUAL(tenant.borrowed(), 30);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 70);
    BOOST_REQUIRE_EQUAL(tenant.available(), 0);

    // Growing past the consumption repays the rest.
    tenant.set_capacity(200);
    BOOST_REQUIRE_EQUAL(tenant.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 100);
    BOOST_REQUIRE_EQUAL(tenant.available(), 50);

    u.return_all();
    BOOST_REQUIRE_EQUAL(tenant.available(), 200);
    BOOST_REQUIRE(tenant.drained());
}

// The pool itself can be resized.
SEASTAR_THREAD_TEST_CASE(test_request_memory_pool_resize) {
    request_memory_pool pool;
    test_tenant tenant(pool, "t", 10);

    auto u1 = tenant.get_units(10).get();
    auto f = tenant.get_units(50);
    BOOST_REQUIRE(!f.available());
    BOOST_REQUIRE_EQUAL(pool.waiting_tenants(), 1u);

    // Growing the pool wakes the blocked tenant.
    pool.set_total_memory(100);
    run_pending_tasks();
    BOOST_REQUIRE(f.available());
    auto u2 = f.get();
    BOOST_REQUIRE_EQUAL(tenant.borrowed(), 50);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 50);

    // Shrinking below what is lent out cannot take the memory back, so the pool
    // reports nothing available until the borrower repays.
    pool.set_total_memory(20);
    BOOST_REQUIRE_EQUAL(pool.total_memory(), 20);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 0);

    u2.return_all();
    BOOST_REQUIRE_EQUAL(tenant.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 20);

    u1.return_all();
    BOOST_REQUIRE(tenant.drained());
}

// A draining tenant gives up its share but keeps serving requests out of the
// pool: connections are not reclassified the moment a service level goes away,
// so a draining tenant that stalled would hang the server.
SEASTAR_THREAD_TEST_CASE(test_request_memory_draining_tenant) {
    request_memory_pool pool;
    pool.set_total_memory(100);
    test_tenant tenant(pool, "t", 50);

    auto u1 = tenant.get_units(50).get();
    tenant.start_draining();
    BOOST_REQUIRE(tenant.draining());
    BOOST_REQUIRE_EQUAL(tenant.capacity(), 0u);
    BOOST_REQUIRE_EQUAL(tenant.available(), -50);
    BOOST_REQUIRE(!tenant.drained());

    auto f = tenant.get_units(30);
    BOOST_REQUIRE(f.available());
    auto u2 = f.get();
    BOOST_REQUIRE_EQUAL(tenant.borrowed(), 30);
    BOOST_REQUIRE_EQUAL(tenant.blocked_total(), 0u);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 70);

    u2.return_all();
    BOOST_REQUIRE(!tenant.drained());
    u1.return_all();
    BOOST_REQUIRE_EQUAL(tenant.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 100);
    BOOST_REQUIRE(tenant.drained());
}

// consume() charges the tenant without waiting, and may take it over its share.
SEASTAR_THREAD_TEST_CASE(test_request_memory_consume) {
    request_memory_pool pool;
    pool.set_total_memory(100);
    test_tenant tenant(pool, "t", 100);

    auto u1 = tenant.get_units(60).get();
    BOOST_REQUIRE_EQUAL(tenant.available(), 40);

    auto u2 = tenant.consume(100);
    BOOST_REQUIRE_EQUAL(tenant.available(), -60);
    BOOST_REQUIRE_EQUAL(tenant.waiters(), 0u);
    BOOST_REQUIRE_EQUAL(tenant.blocked_total(), 0u);
    // The pool is not touched.
    BOOST_REQUIRE_EQUAL(tenant.borrowed(), 0);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 100);

    u2.return_all();
    BOOST_REQUIRE_EQUAL(tenant.available(), 40);
    u1.return_all();
    BOOST_REQUIRE_EQUAL(tenant.available(), 100);
    BOOST_REQUIRE(tenant.drained());
}


// Shrinking the shared pool has to wake the tenants blocked on it. A request
// that was legitimately waiting for pool memory can become impossible to fund,
// and then only the forward-progress escape can let it through - and nothing
// else would ever look at that request again.
SEASTAR_THREAD_TEST_CASE(test_request_memory_pool_shrink_wakes_waiters) {
    request_memory_pool pool;
    pool.set_total_memory(5000);
    test_tenant hog(pool, "hog", 5000);
    // Capacity 0, as a tenant whose service level was dropped has.
    test_tenant retired(pool, "retired", 0);

    // The hog holds its own share and the whole pool.
    auto hog_units = hog.get_units(10000).get();
    BOOST_REQUIRE_EQUAL(hog.borrowed(), 5000);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 0);

    // 3000 is within reach of 0 + the pool's 5000, so this waits for the pool
    // rather than being let through.
    auto f = retired.get_units(3000);
    BOOST_REQUIRE(!f.available());
    BOOST_REQUIRE_EQUAL(pool.waiting_tenants(), 1u);

    // The shared pool fraction is set to 0 while that request is queued. It can
    // never be funded now, so it has to be let through.
    pool.set_total_memory(0);
    run_pending_tasks();
    BOOST_REQUIRE(f.available());
    auto units = f.get();
    BOOST_REQUIRE_EQUAL(retired.available(), -3000);
    BOOST_REQUIRE_EQUAL(retired.borrowed(), 0);

    units.return_all();
    BOOST_REQUIRE(retired.drained());
    hog_units.return_all();
}

// Moving memory from the dedicated shares into the shared pool must not hand the
// same bytes out twice. The pool has to be grown only after the shares it is
// being grown out of have been taken away: a tenant funded from the enlarged
// pool while it still holds the share it is about to lose ends up with both.
SEASTAR_THREAD_TEST_CASE(test_memory_limiter_resplit_does_not_hand_memory_out_twice) {
    constexpr size_t budget = 1000;
    utils::updateable_value_source<double> pool_fraction(0.0);
    service::memory_limiter limiter(budget, 1.0, utils::updateable_value<double>(pool_fraction));
    BOOST_REQUIRE_EQUAL(limiter.total_memory(), budget);

    auto sg_a = create_scheduling_group("rml_test_a", 100).get();
    auto sg_b = create_scheduling_group("rml_test_b", 100).get();
    auto destroy_sgs = defer([&] () noexcept {
        destroy_scheduling_group(sg_a).get();
        destroy_scheduling_group(sg_b).get();
    });

    // Equal shares and nothing in the pool, so each tenant owns half the budget.
    limiter.add_or_update(sg_a, 100);
    limiter.add_or_update(sg_b, 100);
    auto& a = limiter.tenant_for(sg_a);
    auto& b = limiter.tenant_for(sg_b);
    BOOST_REQUIRE_EQUAL(a.capacity(), budget / 2);
    BOOST_REQUIRE_EQUAL(b.capacity(), budget / 2);
    BOOST_REQUIRE_EQUAL(limiter.pool().total_memory(), 0);

    // Both tenants hold most of their own share and have a request queued that
    // the rest of it cannot cover.
    auto held_a = a.get_units(300).get();
    auto held_b = b.get_units(300).get();
    auto queued_a = a.get_units(400);
    auto queued_b = b.get_units(400);
    run_pending_tasks();
    BOOST_REQUIRE(!queued_a.available());
    BOOST_REQUIRE(!queued_b.available());

    // Half the budget goes into the shared pool, 200 bytes of it out of each
    // tenant's own share. The pool can then fund one of the two queued requests,
    // and only one.
    pool_fraction.set(0.5);
    run_pending_tasks();
    BOOST_REQUIRE_EQUAL(limiter.pool().total_memory(), budget / 2);
    BOOST_REQUIRE_EQUAL(a.capacity(), budget / 4);
    BOOST_REQUIRE_EQUAL(b.capacity(), budget / 4);

    // What a tenant has handed out: its own share and what it borrowed, less
    // what is left of them.
    const auto outstanding = [] (const request_memory_tenant& tenant) -> ssize_t {
        return static_cast<ssize_t>(tenant.capacity()) + tenant.borrowed() - tenant.available();
    };
    BOOST_TEST_MESSAGE(seastar::format("after the re-split: outstanding a={}, b={}",
            outstanding(a), outstanding(b)));
    BOOST_REQUIRE_LE(outstanding(a) + outstanding(b), static_cast<ssize_t>(budget));
    BOOST_REQUIRE(queued_a.available() != queued_b.available());

    // Drain, so that the tenants can be destroyed. The one that was funded has
    // to give its memory back before the other one can be.
    held_a.return_all();
    held_b.return_all();
    const auto finish = [] (future<semaphore_units<>> f) {
        f.get().return_all();
    };
    if (queued_a.available()) {
        finish(std::move(queued_a));
        finish(std::move(queued_b));
    } else {
        finish(std::move(queued_b));
        finish(std::move(queued_a));
    }
    BOOST_REQUIRE(a.drained());
    BOOST_REQUIRE(b.drained());
}

BOOST_AUTO_TEST_SUITE_END()
