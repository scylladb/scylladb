/*
 * Copyright (C) 2017 ScyllaDB
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

#include <boost/test/unit_test.hpp>
#include <seastar/core/thread.hh>
#include <seastar/core/semaphore.hh>
#include "utils/serialized_action.hh"
#include "tests/test-utils.hh"
#include "utils/phased_barrier.hh"

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
