/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "core/thread.hh"
#include "core/do_with.hh"
#include "test-utils.hh"
#include "core/sstring.hh"
#include "core/reactor.hh"
#include "core/semaphore.hh"
#include "core/do_with.hh"
#include "core/future-util.hh"
#include "core/sleep.hh"

using namespace seastar;
using namespace std::chrono_literals;

SEASTAR_TEST_CASE(test_thread_1) {
    return do_with(sstring(), [] (sstring& x) {
        auto t1 = new thread([&x] {
            x = "abc";
        });
        return t1->join().then([&x] {
            BOOST_REQUIRE_EQUAL(x, "abc");
        });
    });
}

SEASTAR_TEST_CASE(test_thread_2) {
    struct tmp {
        std::vector<thread> threads;
        semaphore sem1{0};
        semaphore sem2{0};
        int counter = 0;
        void thread_fn() {
            sem1.wait(1).get();
            ++counter;
            sem2.signal(1);
        }
    };
    return do_with(tmp(), [] (tmp& x) {
        auto n = 10;
        for (int i = 0; i < n; ++i) {
            x.threads.emplace_back(std::bind(&tmp::thread_fn, &x));
        }
        BOOST_REQUIRE_EQUAL(x.counter, 0);
        x.sem1.signal(n);
        return x.sem2.wait(n).then([&x, n] {
            BOOST_REQUIRE_EQUAL(x.counter, n);
            return parallel_for_each(x.threads.begin(), x.threads.end(), std::mem_fn(&thread::join));
        });
    });
}

SEASTAR_TEST_CASE(test_thread_async) {
    sstring x = "x";
    sstring y = "y";
    auto concat = [] (sstring x, sstring y) {
        sleep(10ms).get();
        return x + y;
    };
    return async(concat, x, y).then([] (sstring xy) {
        BOOST_REQUIRE_EQUAL(xy, "xy");
    });
}

SEASTAR_TEST_CASE(test_thread_async_immed) {
    return async([] { return 3; }).then([] (int three) {
        BOOST_REQUIRE_EQUAL(three, 3);
    });
}

SEASTAR_TEST_CASE(test_thread_async_nested) {
    return async([] {
        return async([] {
            return 3;
        }).get0();
    }).then([] (int three) {
        BOOST_REQUIRE_EQUAL(three, 3);
    });
}

