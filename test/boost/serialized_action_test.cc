/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <boost/test/unit_test.hpp>
#include <seastar/core/coroutine.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/semaphore.hh>
#include "utils/serialized_action.hh"
#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include "utils/phased_barrier.hh"
#include <seastar/core/timer.hh>
#include <seastar/core/sleep.hh>

using namespace std::chrono_literals;

SEASTAR_TEST_CASE(test_serialized_action_triggering) {
    return seastar::async([] {
        int current = 0;
        std::vector<int> history;
        promise<> p;
        seastar::semaphore sem{0};

        serialized_action act([&] {
            sem.signal(1);
            auto val = current;
            return p.get_future().then([&, val] {
                history.push_back(val);
            });
        });

        auto release = [&] {
            std::exchange(p, promise<>()).set_value();
        };

        auto t1 = act.trigger();
        sem.wait().get(); // wait for t1 action to block
        current = 1;
        auto t2 = act.trigger();
        auto t3 = act.trigger();

        current = 2;
        release();

        t1.get();
        BOOST_REQUIRE(history.size() == 1);
        BOOST_REQUIRE(history.back() == 0);
        BOOST_REQUIRE(!t2.available());
        BOOST_REQUIRE(!t3.available());

        sem.wait().get(); // wait for t2 action to block
        current = 3;
        auto t4 = act.trigger();
        release();

        t2.get();
        t3.get();
        BOOST_REQUIRE(history.size() == 2);
        BOOST_REQUIRE(history.back() == 2);
        BOOST_REQUIRE(!t4.available());

        sem.wait().get(); // wait for t4 action to block
        current = 4;
        release();

        t4.get();
        BOOST_REQUIRE(history.size() == 3);
        BOOST_REQUIRE(history.back() == 3);

        current = 5;
        auto t5 = act.trigger();
        sem.wait().get(); // wait for t5 action to block
        release();
        t5.get();

        BOOST_REQUIRE(history.size() == 4);
        BOOST_REQUIRE(history.back() == 5);
    });
}

SEASTAR_THREAD_TEST_CASE(test_serialized_action_exception) {
    class expected_exception : public std::exception {
    public:
        virtual const char* what() const noexcept override {
            return "expected_exception";
        }
    };

    serialized_action simple_action([&] {
        return make_exception_future<>(expected_exception());
    });

    // test that the exception returned by the serialized action
    // is propageted to the caller of trigger().
    BOOST_REQUIRE_THROW(simple_action.trigger(false).get(), expected_exception);
    BOOST_REQUIRE_THROW(simple_action.trigger(true).get(), expected_exception);

    int count = 0;
    promise<> p;
    seastar::semaphore sem{0};

    serialized_action triggered_action([&] {
        sem.signal(1);
        return p.get_future().then([&] {
            count++;
            return make_exception_future<>(expected_exception());
        });
    });

    auto release = [&] {
        std::exchange(p, promise<>()).set_value();
    };

    // test that the exception returned by the serialized action
    // is propageted to pending callers of trigger().
    auto t1 = triggered_action.trigger();   // launch the action in the background.
    sem.wait().get();                       // wait for t1 to block on `p`.
    auto t2 = triggered_action.trigger();   // trigger the action again.
    auto t3 = triggered_action.trigger();   // trigger the action again. t3 and t2 should share the same future.

    release();                              // signal t1 to proceed (and return the exception).
    BOOST_REQUIRE_THROW(t1.get(), expected_exception);
    BOOST_REQUIRE_EQUAL(count, 1);

    sem.wait().get();                       // wait for t2 to block on `p`.
    release();                              // signal t2 to proceed (and return the exception).
    BOOST_REQUIRE_THROW(t2.get(), expected_exception);
    BOOST_REQUIRE_THROW(t3.get(), expected_exception);
    BOOST_REQUIRE_EQUAL(count, 2);          // verify that `triggered_action` was called only once for t2 and t3.
}

SEASTAR_THREAD_TEST_CASE(test_serialized_action_abort) {
    uint8_t c = 0;
    promise<> p, pp;
    future<> f = p.get_future();
    serialized_action simple_action([&] {
        BOOST_TEST_MESSAGE("action is running");
        if (c++ == 0) {
            p.set_value();
            return pp.get_future();
        }
        return make_ready_future();
    });

    abort_source as;
    auto f1 = simple_action.trigger(as);
    f.get(); // wait for serialized action to trigger
    auto f2 = simple_action.trigger(as);
    auto f3 = simple_action.trigger(as);
    auto f4 = simple_action.trigger();

    as.request_abort();
    pp.set_value(); // release first invocation
    BOOST_TEST_MESSAGE("abort");
    simple_action.join().get();

    BOOST_REQUIRE_THROW(f1.get(), abort_requested_exception);
    BOOST_REQUIRE_THROW(f2.get(), abort_requested_exception);
    BOOST_REQUIRE_THROW(f3.get(), abort_requested_exception);
    BOOST_REQUIRE_NO_THROW(f4.get());
    BOOST_REQUIRE(c == 2);
}

SEASTAR_THREAD_TEST_CASE(test_phased_barrier_reassignment) {
    utils::phased_barrier bar1("bar1");
    utils::phased_barrier bar2("bar2");
    {
        auto op1 = bar1.start();
        auto op2 = bar2.start();
        op1 = std::move(op2);
    }
    timer<> completion_timer;
    completion_timer.set_callback([&] {
        BOOST_ERROR("phased_barrier::advance_and_await timed out");
        _exit(1);
    });
    completion_timer.arm(1s);
    bar1.advance_and_await().get();
    bar2.advance_and_await().get();
    completion_timer.cancel();
}

SEASTAR_THREAD_TEST_CASE(test_phased_barrier_stop) {
    utils::phased_barrier bar("bar");
    semaphore sem(0);
    auto task = [&] () -> future<> {
        auto op = bar.start();
        co_await get_units(sem, 1);
    };
    auto task_fut = task();
    BOOST_REQUIRE(!task_fut.available());

    auto close_fut = bar.close();
    BOOST_REQUIRE(!close_fut.available());
    BOOST_REQUIRE(bar.is_closed());

    BOOST_REQUIRE_THROW(task().get(), seastar::gate_closed_exception);

    sem.signal();
    BOOST_REQUIRE_NO_THROW(task_fut.get());
    BOOST_REQUIRE_NO_THROW(close_fut.get());

    BOOST_REQUIRE(bar.is_closed());
    BOOST_REQUIRE_THROW(task().get(), seastar::gate_closed_exception);
}

SEASTAR_THREAD_TEST_CASE(test_phased_barrier_stop_while_awaiting) {
    utils::phased_barrier bar("bar");
    semaphore sem(0);
    auto task = [&] () -> future<> {
        auto op = bar.start();
        co_await get_units(sem, 1);
    };
    auto task_fut = task();
    BOOST_REQUIRE(!task_fut.available());

    auto wait_fut = bar.advance_and_await();
    BOOST_REQUIRE(!wait_fut.available());
    BOOST_REQUIRE(!bar.is_closed());

    auto close_fut = bar.close();
    BOOST_REQUIRE(!close_fut.available());
    BOOST_REQUIRE(bar.is_closed());

    BOOST_REQUIRE_THROW(task().get(), seastar::gate_closed_exception);

    sem.signal();
    BOOST_REQUIRE_NO_THROW(task_fut.get());
    BOOST_REQUIRE_NO_THROW(wait_fut.get());
    BOOST_REQUIRE_NO_THROW(close_fut.get());

    BOOST_REQUIRE(bar.is_closed());
    BOOST_REQUIRE_THROW(task().get(), seastar::gate_closed_exception);
}
