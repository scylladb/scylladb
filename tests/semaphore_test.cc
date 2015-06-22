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
#include "core/shared_mutex.hh"
#include <boost/range/irange.hpp>

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

SEASTAR_TEST_CASE(test_semaphore_mix_1) {
    return do_with(std::make_pair(semaphore(0), 0), [] (std::pair<semaphore, int>& x) {
        x.first.wait(3ms).then([&x] {
            x.second++;
        });
        x.first.wait().then([&x] {
            x.second = 10;
        });
        sleep(10ms).then([&x] {
            x.first.signal();
        });
        return sleep(20ms).then([&x] {
            BOOST_REQUIRE_EQUAL(x.second, 10);
        });
    });
}

SEASTAR_TEST_CASE(test_broken_semaphore) {
    auto sem = make_lw_shared<semaphore>(0);
    struct oops {};
    auto ret = sem->wait().then_wrapped([sem] (future<> f) {
        try {
            f.get();
            BOOST_FAIL("expecting exception");
        } catch (oops& x) {
            // ok
            return make_ready_future<>();
        } catch (...) {
            BOOST_FAIL("wrong exception seen");
        }
        BOOST_FAIL("unreachable");
        return make_ready_future<>();
    });
    sem->broken(oops());
    return ret;
}

SEASTAR_TEST_CASE(test_shared_mutex_exclusive) {
    return do_with(shared_mutex(), unsigned(0), [] (shared_mutex& sm, unsigned& counter) {
        return parallel_for_each(boost::irange(0, 10), [&sm, &counter] (int idx) {
            return with_lock(sm, [&counter] {
                BOOST_REQUIRE_EQUAL(counter, 0);
                ++counter;
                return sleep(10ms).then([&counter] {
                    --counter;
                    BOOST_REQUIRE_EQUAL(counter, 0);
                });
            });
        });
    });
}

SEASTAR_TEST_CASE(test_shared_mutex_shared) {
    return do_with(shared_mutex(), unsigned(0), [] (shared_mutex& sm, unsigned& counter) {
        auto running_in_parallel = [&sm, &counter] (int instance) {
            return with_shared(sm, [&counter] {
                ++counter;
                return sleep(10ms).then([&counter] {
                    bool was_parallel = counter != 0;
                    --counter;
                    return was_parallel;
                });
            });
        };
        return map_reduce(boost::irange(0, 100), running_in_parallel, false, std::bit_or<bool>()).then([&counter] (bool result) {
            BOOST_REQUIRE_EQUAL(result, true);
            BOOST_REQUIRE_EQUAL(counter, 0);
        });
    });
}

SEASTAR_TEST_CASE(test_shared_mutex_mixed) {
    return do_with(shared_mutex(), unsigned(0), [] (shared_mutex& sm, unsigned& counter) {
        auto running_in_parallel = [&sm, &counter] (int instance) {
            return with_shared(sm, [&counter] {
                ++counter;
                return sleep(10ms).then([&counter] {
                    bool was_parallel = counter != 0;
                    --counter;
                    return was_parallel;
                });
            });
        };
        auto running_alone = [&sm, &counter] (int instance) {
            return with_lock(sm, [&counter] {
                BOOST_REQUIRE_EQUAL(counter, 0);
                ++counter;
                return sleep(10ms).then([&counter] {
                    --counter;
                    BOOST_REQUIRE_EQUAL(counter, 0);
                    return true;
                });
            });
        };
        auto run = [running_in_parallel, running_alone] (int instance) {
            if (instance % 9 == 0) {
                return running_alone(instance);
            } else {
                return running_in_parallel(instance);
            }
        };
        return map_reduce(boost::irange(0, 100), run, false, std::bit_or<bool>()).then([&counter] (bool result) {
            BOOST_REQUIRE_EQUAL(result, true);
            BOOST_REQUIRE_EQUAL(counter, 0);
        });
    });
}

