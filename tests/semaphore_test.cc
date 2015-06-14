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

SEASTAR_TEST_CASE(test_semaphore_1) {
    return do_with(std::make_pair(semaphore(0), 0), [] (std::pair<semaphore, int>& x) {
        x.first.wait().then([&x] {
            x.second++;
        });
        x.first.signal();
        return sleep(10ms).then([&x] {
            BOOST_REQUIRE_EQUAL(x.second, 1);
        });
    });
}

SEASTAR_TEST_CASE(test_semaphore_2) {
    return do_with(std::make_pair(semaphore(0), 0), [] (std::pair<semaphore, int>& x) {
        x.first.wait().then([&x] {
            x.second++;
        });
        return sleep(10ms).then([&x] {
            BOOST_REQUIRE_EQUAL(x.second, 0);
        });
    });
}

SEASTAR_TEST_CASE(test_semaphore_timeout_1) {
    return do_with(std::make_pair(semaphore(0), 0), [] (std::pair<semaphore, int>& x) {
        x.first.wait(10ms).then([&x] {
            x.second++;
        });
        sleep(3ms).then([&x] {
            x.first.signal();
        });
        return sleep(20ms).then([&x] {
            BOOST_REQUIRE_EQUAL(x.second, 1);
        });
    });
}

SEASTAR_TEST_CASE(test_semaphore_timeout_2) {
    return do_with(std::make_pair(semaphore(0), 0), [] (std::pair<semaphore, int>& x) {
        x.first.wait(3ms).then([&x] {
            x.second++;
        });
        sleep(10ms).then([&x] {
            x.first.signal();
        });
        return sleep(20ms).then([&x] {
            BOOST_REQUIRE_EQUAL(x.second, 0);
        });
    });
}

