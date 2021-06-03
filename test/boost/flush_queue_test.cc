/*
 * Copyright 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */


#include <random>
#include <bitset>
#include <boost/test/unit_test.hpp>
#include <boost/range/irange.hpp>
#include <seastar/core/semaphore.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>

#include "seastarx.hh"
#include <seastar/testing/test_case.hh>
#include <seastar/core/print.hh>
#include "utils/flush_queue.hh"
#include "log.hh"
#include "test/lib/random_utils.hh"

SEASTAR_TEST_CASE(test_queue_ordering_random_ops) {
    struct env {
        env(size_t n) : promises(n) {}

        utils::flush_queue<int> queue;
        std::vector<promise<>> promises;
        std::vector<int> result;
    };

    auto r = boost::irange(0, 100);

    return do_for_each(r, [](int) {
        constexpr size_t num_ops = 1000;

        auto e = make_lw_shared<env>(num_ops);

        int i = 0;
        for (auto& p : e->promises) {
            // FIXME: discarded future.
            (void)e->queue.run_with_ordered_post_op(i, [&p, i] {
                return p.get_future().then([i] {
                    return make_ready_future<int>(i);
                });
            }, [e](int i) {
                e->result.emplace_back(i);
            });
            ++i;
        }

        auto res = e->queue.wait_for_pending();

        std::uniform_int_distribution<size_t> dist(0, num_ops - 1);
        std::bitset<num_ops> set;

        while (!set.all()) {
            auto& e1 = seastar::testing::local_random_engine;
            size_t i = dist(e1);
            if (!set.test(i)) {
                set[i] = true;
                e->promises[i].set_value();
            }
        }

        return res.then([e] {
            BOOST_CHECK_EQUAL(e->result.size(), e->promises.size());
            BOOST_REQUIRE(std::is_sorted(e->result.begin(), e->result.end()));
        }).finally([e] {
            return e->queue.close().finally([e] { });
        });
    });
}

SEASTAR_TEST_CASE(test_queue_ordering_multi_ops) {
    struct env {
        env() : sem(0) {}

        utils::flush_queue<int> queue;
        std::vector<int> result;
        semaphore sem;
        size_t n = 0;
    };

    auto r = boost::irange(0, 100);

    return do_for_each(r, [](int) {
        constexpr size_t num_ops = 1000;

        auto e = make_lw_shared<env>();

        std::uniform_int_distribution<size_t> dist(0, num_ops - 1);

        for (size_t k = 0; k < num_ops*10; ++k) {
            auto& e1 = seastar::testing::local_random_engine;
            int i = dist(e1);

            if (e->queue.has_operation(i) || (!e->queue.empty() && e->queue.highest_key() < i)) {
                // FIXME: discarded future.
                (void)e->queue.run_with_ordered_post_op(i, [e, i] {
                    return e->sem.wait().then([i] {
                        return make_ready_future<int>(i);
                    });
                }, [e](int i) {
                    e->result.emplace_back(i);
                });
                ++e->n;
            }
        }

        auto res = e->queue.wait_for_pending();

        e->sem.signal(e->n);

        return res.then([e] {
            BOOST_CHECK_EQUAL(e->result.size(), e->n);
            BOOST_REQUIRE(std::is_sorted(e->result.begin(), e->result.end()));
        }).finally([e] {
            return e->queue.close().finally([e] { });
        });
    });
}

static future<> test_propagation(bool propagate,
        noncopyable_function<future<> ()> func,
        noncopyable_function<future<> ()> post,
        noncopyable_function<void ()> thn,
        bool want_except_in_run, bool want_except_in_wait) {
    auto queue = ::make_shared<utils::flush_queue<int>>(propagate);
    auto sem = ::make_shared<semaphore>(0);
    auto xr = ::make_shared<bool>(false);
    auto xw = ::make_shared<bool>(false);

    auto f1 = queue->run_with_ordered_post_op(0, [sem, func = std::move(func)]() mutable {
        return sem->wait().then(std::move(func));
    }, std::move(post)).handle_exception([xr](auto p) {
        *xr = true;
    }).discard_result();

    auto f2 = queue->wait_for_pending(0).then(std::move(thn)).handle_exception([xw](auto p) {
        *xw = true;
    }).discard_result();

    sem->signal();

    return seastar::when_all_succeed(std::move(f1), std::move(f2)).finally([sem, queue, want_except_in_run, want_except_in_wait, xr, xw] {
        BOOST_CHECK_EQUAL(want_except_in_run, *xr);
        BOOST_CHECK_EQUAL(want_except_in_wait, *xw);
    }).discard_result().finally([queue] {
        return queue->close().finally([queue] { });
    });
}

SEASTAR_TEST_CASE(test_propagate_exception_in_op) {
    return test_propagation(true, // propagate exception to waiter
                    [] { return make_exception_future(std::runtime_error("hej")); }, // ex in op
                    [] { BOOST_FAIL("should not reach (1)"); return make_ready_future(); }, // should not reach post
                    [] { BOOST_FAIL("should not reach (2)"); }, // should not reach waiter "then"
                    true,
                    true
                    );
}

SEASTAR_TEST_CASE(test_propagate_exception_in_post) {
    return test_propagation(true, // propagate exception to waiter
                    [] { return make_ready_future(); }, // ok func
                    [] { return make_exception_future(std::runtime_error("hej")); }, // ex in post
                    [] { BOOST_FAIL("should not reach"); }, // should not reach waiter "then"
                    true,
                    true
    );
}

SEASTAR_TEST_CASE(test_no_propagate_exception_in_op) {
    return test_propagation(false, // do not propagate exception to waiter
                    [] { return make_exception_future(std::runtime_error("hej")); }, // ex in op
                    [] { BOOST_FAIL("should not reach"); return make_ready_future(); }, // should not reach post
                    [] {}, // should reach waiter "then"
                    true,
                    false
                    );
}

SEASTAR_TEST_CASE(test_no_propagate_exception_in_post) {
    return test_propagation(false, // do not propagate exception to waiter
                    [] { return make_ready_future(); }, // ok func
                    [] { return make_exception_future(std::runtime_error("hej")); }, // ex in post
                    [] {}, // should reach waiter "then"
                    true,
                    false
                    );
}
