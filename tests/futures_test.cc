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
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "tests/test-utils.hh"

#include "core/shared_ptr.hh"
#include "core/future-util.hh"
#include "core/sleep.hh"
#include "core/do_with.hh"
#include <boost/iterator/counting_iterator.hpp>

class expected_exception : std::runtime_error {
public:
    expected_exception() : runtime_error("expected") {}
};

// Returns a future which is not ready but is scheduled to resolve soon.
static
future<> later() {
    promise<> p;
    auto f = p.get_future();
    schedule(make_task([p = std::move(p)] () mutable {
        p.set_value();
    }));
    return f;
}

SEASTAR_TEST_CASE(test_finally_is_called_on_success_and_failure) {
    auto finally1 = make_shared<bool>();
    auto finally2 = make_shared<bool>();

    return make_ready_future().then([] {
    }).finally([=] {
        *finally1 = true;
    }).then([] {
        throw std::runtime_error("");
    }).finally([=] {
        *finally2 = true;
    }).then_wrapped([=] (auto&& f) {
        BOOST_REQUIRE(*finally1);
        BOOST_REQUIRE(*finally2);

        // Should be failed.
        try {
            f.get();
            BOOST_REQUIRE(false);
        } catch (...) {}
    });
}

SEASTAR_TEST_CASE(test_finally_is_called_on_success_and_failure__not_ready_to_armed) {
    auto finally1 = make_shared<bool>();
    auto finally2 = make_shared<bool>();

    promise<> p;
    auto f = p.get_future().finally([=] {
        *finally1 = true;
    }).then([] {
        throw std::runtime_error("");
    }).finally([=] {
        *finally2 = true;
    }).then_wrapped([=] (auto &&f) {
        BOOST_REQUIRE(*finally1);
        BOOST_REQUIRE(*finally2);
    });

    p.set_value();
    return f;
}

SEASTAR_TEST_CASE(test_exception_from_finally_fails_the_target) {
    promise<> pr;
    auto f = pr.get_future().finally([=] {
        throw std::runtime_error("");
    }).then([] {
        BOOST_REQUIRE(false);
    }).then_wrapped([] (auto&& f) {});

    pr.set_value();
    return f;
}

SEASTAR_TEST_CASE(test_exception_from_finally_fails_the_target_on_already_resolved) {
    return make_ready_future().finally([=] {
        throw std::runtime_error("");
    }).then([] {
        BOOST_REQUIRE(false);
    }).then_wrapped([] (auto&& f) {});
}

SEASTAR_TEST_CASE(test_exception_thrown_from_then_wrapped_causes_future_to_fail) {
    return make_ready_future().then_wrapped([] (auto&& f) {
        throw std::runtime_error("");
    }).then_wrapped([] (auto&& f) {
        try {
            f.get();
            BOOST_REQUIRE(false);
        } catch (...) {}
    });
}

SEASTAR_TEST_CASE(test_exception_thrown_from_then_wrapped_causes_future_to_fail__async_case) {
    promise<> p;

    auto f = p.get_future().then_wrapped([] (auto&& f) {
        throw std::runtime_error("");
    }).then_wrapped([] (auto&& f) {
        try {
            f.get();
            BOOST_REQUIRE(false);
        } catch (...) {}
    });

    p.set_value();

    return f;
}

SEASTAR_TEST_CASE(test_failing_intermediate_promise_should_fail_the_master_future) {
    promise<> p1;
    promise<> p2;

    auto f = p1.get_future().then([f = std::move(p2.get_future())] () mutable {
        return std::move(f);
    }).then([] {
        BOOST_REQUIRE(false);
    });

    p1.set_value();
    p2.set_exception(std::runtime_error("boom"));

    return std::move(f).then_wrapped([](auto&& f) {
        try {
            f.get();
            BOOST_REQUIRE(false);
        } catch (...) {}
    });
}

SEASTAR_TEST_CASE(test_future_forwarding__not_ready_to_unarmed) {
    promise<> p1;
    promise<> p2;

    auto f1 = p1.get_future();
    auto f2 = p2.get_future();

    f1.forward_to(std::move(p2));

    BOOST_REQUIRE(!f2.available());

    auto called = f2.then([] {});

    p1.set_value();
    return called;
}

SEASTAR_TEST_CASE(test_future_forwarding__not_ready_to_armed) {
    promise<> p1;
    promise<> p2;

    auto f1 = p1.get_future();
    auto f2 = p2.get_future();

    auto called = f2.then([] {});

    f1.forward_to(std::move(p2));

    BOOST_REQUIRE(!f2.available());

    p1.set_value();

    return called;
}

SEASTAR_TEST_CASE(test_future_forwarding__ready_to_unarmed) {
    promise<> p2;

    auto f1 = make_ready_future<>();
    auto f2 = p2.get_future();

    std::move(f1).forward_to(std::move(p2));
    BOOST_REQUIRE(f2.available());

    return std::move(f2).then_wrapped([] (future<> f) {
        BOOST_REQUIRE(!f.failed());
    });
}

SEASTAR_TEST_CASE(test_future_forwarding__ready_to_armed) {
    promise<> p2;

    auto f1 = make_ready_future<>();
    auto f2 = p2.get_future();

    auto called = std::move(f2).then([] {});

    BOOST_REQUIRE(f1.available());

    f1.forward_to(std::move(p2));
    return called;
}

static void forward_dead_unarmed_promise_with_dead_future_to(promise<>& p) {
    promise<> p2;
    p.get_future().forward_to(std::move(p2));
}

SEASTAR_TEST_CASE(test_future_forwarding__ready_to_unarmed_soon_to_be_dead) {
    promise<> p1;
    forward_dead_unarmed_promise_with_dead_future_to(p1);
    make_ready_future<>().forward_to(std::move(p1));
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_exception_can_be_thrown_from_do_until_body) {
    return do_until([] { return false; }, [] {
        throw expected_exception();
        return now();
    }).then_wrapped([] (auto&& f) {
       try {
           f.get();
           BOOST_FAIL("should have failed");
       } catch (const expected_exception& e) {
           // expected
       }
    });
}

SEASTAR_TEST_CASE(test_bare_value_can_be_returned_from_callback) {
    return now().then([] {
        return 3;
    }).then([] (int x) {
        BOOST_REQUIRE(x == 3);
    });
}

SEASTAR_TEST_CASE(test_when_all_iterator_range) {
    std::vector<future<size_t>> futures;
    for (size_t i = 0; i != 1000000; ++i) {
        // .then() usually returns a ready future, but sometimes it
        // doesn't, so call it a million times.  This exercises both
        // available and unavailable paths in when_all().
        futures.push_back(make_ready_future<>().then([i] { return i; }));
    }
    // Verify the above statement is correct
    BOOST_REQUIRE(!std::all_of(futures.begin(), futures.end(),
            [] (auto& f) { return f.available(); }));
    auto p = make_shared(std::move(futures));
    return when_all(p->begin(), p->end()).then([p] (std::vector<future<size_t>> ret) {
        BOOST_REQUIRE(std::all_of(ret.begin(), ret.end(), [] (auto& f) { return f.available(); }));
        BOOST_REQUIRE(std::all_of(ret.begin(), ret.end(), [&ret] (auto& f) { return std::get<0>(f.get()) == size_t(&f - ret.data()); }));
    });
}

SEASTAR_TEST_CASE(test_map_reduce) {
    auto square = [] (long x) { return make_ready_future<long>(x*x); };
    long n = 1000;
    return map_reduce(boost::make_counting_iterator<long>(0), boost::make_counting_iterator<long>(n),
            square, long(0), std::plus<long>()).then([n] (auto result) {
        auto m = n - 1; // counting does not include upper bound
        BOOST_REQUIRE_EQUAL(result, (m * (m + 1) * (2*m + 1)) / 6);
    });
}

// This test doesn't actually test anything - it just waits for the future
// returned by sleep to complete. However, a bug we had in sleep() caused
// this test to fail the sanitizer in the debug build, so this is a useful
// regression test.
SEASTAR_TEST_CASE(test_sleep) {
    return sleep(std::chrono::milliseconds(100));
}

SEASTAR_TEST_CASE(test_do_with_1) {
    return do_with(1, [] (int& one) {
       BOOST_REQUIRE_EQUAL(one, 1);
       return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_do_with_2) {
    return do_with(1, 2L, [] (int& one, long two) {
        BOOST_REQUIRE_EQUAL(one, 1);
        BOOST_REQUIRE_EQUAL(two, 2);
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_do_with_3) {
    return do_with(1, 2L, 3, [] (int& one, long two, int three) {
        BOOST_REQUIRE_EQUAL(one, 1);
        BOOST_REQUIRE_EQUAL(two, 2);
        BOOST_REQUIRE_EQUAL(three, 3);
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_do_with_4) {
    return do_with(1, 2L, 3, 4, [] (int& one, long two, int three, int four) {
        BOOST_REQUIRE_EQUAL(one, 1);
        BOOST_REQUIRE_EQUAL(two, 2);
        BOOST_REQUIRE_EQUAL(three, 3);
        BOOST_REQUIRE_EQUAL(four, 4);
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_do_while_stopping_immediately) {
    return do_with(int(0), [] (int& count) {
        return repeat([&count] {
            ++count;
            return stop_iteration::yes;
        }).then([&count] {
            BOOST_REQUIRE(count == 1);
        });
    });
}

SEASTAR_TEST_CASE(test_do_while_stopping_after_two_iterations) {
    return do_with(int(0), [] (int& count) {
        return repeat([&count] {
            ++count;
            return count == 2 ? stop_iteration::yes : stop_iteration::no;
        }).then([&count] {
            BOOST_REQUIRE(count == 2);
        });
    });
}

SEASTAR_TEST_CASE(test_do_while_failing_in_the_first_step) {
    return repeat([] {
        throw expected_exception();
        return stop_iteration::no;
    }).then_wrapped([](auto&& f) {
        try {
            f.get();
            BOOST_FAIL("should not happen");
        } catch (const expected_exception&) {
            // expected
        }
    });
}

SEASTAR_TEST_CASE(test_do_while_failing_in_the_second_step) {
    return do_with(int(0), [] (int& count) {
        return repeat([&count] {
            ++count;
            if (count > 1) {
                throw expected_exception();
            }
            return later().then([] { return stop_iteration::no; });
        }).then_wrapped([&count](auto&& f) {
            try {
                f.get();
                BOOST_FAIL("should not happen");
            } catch (const expected_exception&) {
                BOOST_REQUIRE(count == 2);
            }
        });
    });
}
