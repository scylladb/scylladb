/*
 * Copyright 2015 ScyllaDB
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

#define BOOST_TEST_DYN_LINK

#include <random>
#include <bitset>
#include <boost/test/unit_test.hpp>
#include <boost/range/irange.hpp>
#include <seastar/core/semaphore.hh>
#include <seastar/core/reactor.hh>

#include "tests/test-utils.hh"
#include "utils/flush_queue.hh"
#include "log.hh"

std::random_device rd;
std::default_random_engine e1(rd());

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
            e->queue.run_with_ordered_post_op(i, [&p, i] {
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
            size_t i = dist(e1);
            if (!set.test(i)) {
                set[i] = true;
                e->promises[i].set_value();
            }
        }

        return res.then([e] {
            BOOST_CHECK_EQUAL(e->result.size(), e->promises.size());
            BOOST_REQUIRE(std::is_sorted(e->result.begin(), e->result.end()));
        }).finally([e] {});
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
            int i = dist(e1);

            if (e->queue.has_operation(i) || (!e->queue.empty() && e->queue.highest_key() < i)) {
                e->queue.run_with_ordered_post_op(i, [e, i] {
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
        }).finally([e] {});
    });
}
