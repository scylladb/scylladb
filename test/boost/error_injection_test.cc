/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/testing/on_internal_error.hh>
#include "test/lib/cql_test_env.hh"
#include <seastar/core/manual_clock.hh>
#include "test/lib/scylla_test_case.hh"
#include <seastar/rpc/rpc_types.hh>
#include "utils/error_injection.hh"
#include "db/timeout_clock.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/test_utils.hh"
#include "types/list.hh"
#include "log.hh"
#include <chrono>

using namespace std::literals::chrono_literals;

static logging::logger flogger("error_injection_test");

using milliseconds = std::chrono::milliseconds;
using minutes = std::chrono::minutes;
using steady_clock = std::chrono::steady_clock;

constexpr milliseconds sleep_msec(10); // Injection time sleep 10 msec

SEASTAR_TEST_CASE(test_inject_noop) {
    utils::error_injection<false> errinj;

    BOOST_REQUIRE_NO_THROW(errinj.inject("noop1",
            [] () { throw std::runtime_error("shouldn't happen"); }));

    errinj.enable("error");
    BOOST_ASSERT(errinj.enabled_injections().empty());
    BOOST_ASSERT(errinj.enter("error") == false);

    auto f = errinj.inject("noop2", sleep_msec);
    BOOST_REQUIRE(f.available() && !f.failed());

    errinj.enable("noop3");
    f = errinj.inject("noop3", [] (auto& handler) -> future<> {
        throw std::runtime_error("shouldn't happen");
    });

    BOOST_REQUIRE(f.available() && !f.failed());

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_is_enabled) {
    utils::error_injection<true> errinj;

    // Test enable and disable
    errinj.enable("is_enabled_test", false);
    errinj.disable("is_enabled_test");
    BOOST_ASSERT(errinj.enabled_injections().size() == 0);

    // Test enable with one_shot=true and enter
    errinj.enable("is_enabled_test", true);
    BOOST_ASSERT(errinj.enabled_injections().size() == 1);
    BOOST_ASSERT(errinj.enter("is_enabled_test"));
    BOOST_ASSERT(errinj.enabled_injections().size() == 0);

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_inject_lambda) {
    utils::error_injection<true> errinj;

    errinj.enable("lambda");
    BOOST_REQUIRE_THROW(errinj.inject("lambda",
            [] () -> void { throw std::runtime_error("test"); }),
            std::runtime_error);
    errinj.disable("lambda");
    BOOST_REQUIRE_NO_THROW(errinj.inject("lambda",
            [] () -> void { throw std::runtime_error("test"); }));
    errinj.enable("lambda");
    BOOST_REQUIRE_THROW(errinj.inject("lambda",
            [] () -> void { throw std::runtime_error("test"); }),
            std::runtime_error);
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_inject_sleep_duration) {
    utils::error_injection<true> errinj;

    auto start_time = steady_clock::now();
    errinj.enable("future_sleep");
    return errinj.inject("future_sleep", sleep_msec).then([start_time] {
        auto wait_time = std::chrono::duration_cast<milliseconds>(steady_clock::now() - start_time);
        BOOST_REQUIRE_GE(wait_time, sleep_msec);
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_inject_sleep_deadline_steady_clock) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        utils::error_injection<true> errinj;

        // Inject sleep, deadline short-circuit
        auto deadline = steady_clock::now() + sleep_msec;
        errinj.enable("future_deadline");
        errinj.inject("future_deadline", deadline).then([deadline] {
            BOOST_REQUIRE_GE(steady_clock::now() - deadline,
                             steady_clock::duration::zero());
            return make_ready_future<>();
        }).get();
    });
}

SEASTAR_TEST_CASE(test_inject_sleep_deadline_manual_clock) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        utils::error_injection<true> errinj;

        // Inject sleep, deadline short-circuit
        auto deadline = seastar::manual_clock::now() + sleep_msec;
        errinj.enable("future_deadline");
        auto f = errinj.inject("future_deadline", deadline).then([deadline] {
            BOOST_REQUIRE_GE(seastar::manual_clock::now() - deadline,
                             seastar::manual_clock::duration::zero());
            return make_ready_future<>();
        });
        manual_clock::advance(sleep_msec);
        f.get();
    });
}

SEASTAR_TEST_CASE(test_inject_sleep_deadline_manual_clock_lambda) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        utils::error_injection<true> errinj;

        // Inject sleep, deadline short-circuit
        auto deadline = seastar::manual_clock::now() + sleep_msec;
        errinj.enable("future_deadline");
        auto f = errinj.inject("future_deadline", deadline, [deadline] {
            BOOST_REQUIRE_GE(seastar::manual_clock::now() - deadline,
                             seastar::manual_clock::duration::zero());
            return make_ready_future<>();
        });
        manual_clock::advance(sleep_msec);
        f.get();
    });
}

SEASTAR_TEST_CASE(test_inject_sleep_deadline_db_clock) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        utils::error_injection<true> errinj;

        // Inject sleep, deadline short-circuit
        auto deadline = db::timeout_clock::now() + sleep_msec;
        errinj.enable("future_deadline");
        errinj.inject("future_deadline", deadline).then([deadline] {
            BOOST_REQUIRE_GE(db::timeout_clock::now() - deadline,
                             db::timeout_clock::duration::zero());
            return make_ready_future<>();
        }).get();
    });
}

SEASTAR_TEST_CASE(test_inject_future_disabled) {
    utils::error_injection<true> errinj;

    auto start_time = steady_clock::now();
    return errinj.inject("futid", sleep_msec).then([start_time] {
        auto wait_time = steady_clock::now() - start_time;
        BOOST_REQUIRE_LT(wait_time, sleep_msec);
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_error_exceptions) {

    auto exc = std::make_exception_ptr(utils::injected_error("test"));
    BOOST_TEST(!is_timeout_exception(exc));

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_is_timeout_exception) {
    for (auto ep : {
        std::make_exception_ptr(seastar::rpc::timeout_error()),
        std::make_exception_ptr(seastar::semaphore_timed_out()),
        std::make_exception_ptr(seastar::timed_out_error()),
    })
    {
         BOOST_TEST(is_timeout_exception(ep));
         try {
             std::rethrow_exception(ep);
         } catch (...) {
             try {
                 std::throw_with_nested(std::runtime_error("Hello"));
             } catch (...) {
                BOOST_TEST(is_timeout_exception(std::current_exception()));
             }
         }
    }
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_inject_exception) {
    utils::error_injection<true> errinj;

    errinj.enable("exc");
    return errinj.inject("exc", [] () -> std::exception_ptr {
        return std::make_exception_ptr(std::runtime_error("test"));
    }).then_wrapped([] (auto f) {
        BOOST_REQUIRE_THROW(f.get(), std::runtime_error);
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_inject_two) {
    utils::error_injection<true> errinj;

    auto f = make_ready_future<>();
    errinj.enable("one");
    errinj.enable("two");

    std::vector<sstring> expected = { "one", "two" };
    auto enabled_injections = errinj.enabled_injections();
    std::sort(enabled_injections.begin(), enabled_injections.end());
    BOOST_TEST(enabled_injections == expected);

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_disable_all) {
    utils::error_injection<true> errinj;

    auto f = make_ready_future<>();
    errinj.enable("one");
    errinj.enable("two");
    errinj.disable_all();
    auto enabled_injections = errinj.enabled_injections();
    BOOST_TEST(enabled_injections == std::vector<sstring>());

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_inject_once) {
    utils::error_injection<true> errinj;

    errinj.enable("first", true);

    std::vector<sstring> expected1 = { "first" };
    auto enabled_injections1 = errinj.enabled_injections();
    BOOST_TEST(enabled_injections1 == expected1);
    BOOST_REQUIRE_THROW(errinj.inject("first", [] { throw std::runtime_error("test"); }),
        std::runtime_error);

    std::vector<sstring> expected_empty;
    auto enabled_injections2 = errinj.enabled_injections();
    BOOST_TEST(enabled_injections2 == expected_empty);
    BOOST_REQUIRE_NO_THROW(errinj.inject("first", [] { throw std::runtime_error("test"); }));

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_inject_message) {
    testing::scoped_no_abort_on_internal_error abort_guard;
    utils::error_injection<true> errinj;

    auto timeout = db::timeout_clock::now() + 5s;

    errinj.enable("injection1");
    {
        // Test timeout
        auto f = errinj.inject("injection1", [] (auto& handler) {
            return handler.wait_for_message(db::timeout_clock::now());
        });

        BOOST_REQUIRE_THROW(co_await std::move(f), std::runtime_error);
    }
    {
        // Test receiving multiple messages
        auto f = errinj.inject("injection1", std::bind_front([] (auto timeout, auto& handler) -> future<> {
            for (size_t i = 0; i < 3; ++i) {
                co_await handler.wait_for_message(timeout);
            }
        }, timeout));

        for (size_t i = 0; i < 3; ++i) {
            errinj.receive_message("injection1");
        }

        BOOST_REQUIRE_NO_THROW(co_await std::move(f));
    }
    errinj.disable("injection1");

    errinj.enable("injection2");
    {
        // Test receiving message before waiting for it
        errinj.receive_message("injection2");

        auto f = errinj.inject("injection2", [] (auto& handler) {
            return handler.wait_for_message(db::timeout_clock::now());
        });
        BOOST_REQUIRE_NO_THROW(co_await std::move(f));
    }
    errinj.disable("injection2");

    errinj.enable("multiple_injections");
    {
        // Test concurrent injections
        auto f1 = errinj.inject("multiple_injections", [timeout] (auto& handler) {
            return handler.wait_for_message(timeout);
        });
        auto f2 = errinj.inject("multiple_injections", [timeout] (auto& handler) {
            return handler.wait_for_message(timeout);
        });
        errinj.receive_message("multiple_injections");
        BOOST_REQUIRE_NO_THROW(co_await std::move(f1));
        BOOST_REQUIRE_NO_THROW(co_await std::move(f2));
    }
    errinj.disable("multiple_injections");

    errinj.enable("one_shot", true);
    {
        // Test concurrent one shot injections
        auto f1 = errinj.inject("one_shot", [timeout] (auto& handler) {
            return handler.wait_for_message(timeout);
        });
        auto f2 = errinj.inject("one_shot", std::bind_front([] (auto timeout, auto& handler) -> future<> {
            co_await handler.wait_for_message(timeout);
            co_await handler.wait_for_message(timeout);
        }, timeout));
        auto injections = errinj.enabled_injections();
        BOOST_REQUIRE(injections.empty());
        errinj.receive_message("one_shot");
        BOOST_REQUIRE_NO_THROW(co_await std::move(f1));
        BOOST_REQUIRE_NO_THROW(co_await std::move(f2)); // Disabled after first injection
    }
}

SEASTAR_TEST_CASE(test_inject_unshared_message) {
    testing::scoped_no_abort_on_internal_error abort_guard;
    utils::error_injection<true> errinj;

    auto timeout = db::timeout_clock::now() + 5s;

    errinj.enable("injection1");
    {
        // Test receiving enough unshared messages
        auto f1 = errinj.inject("injection1", std::bind_front([] (auto timeout, auto& handler) -> future<> {
            co_await handler.wait_for_message(timeout);
            co_await handler.wait_for_message(timeout);
        }, timeout), false);
        auto f2 = errinj.inject("injection1", std::bind_front([] (auto timeout, auto& handler) -> future<> {
            co_await handler.wait_for_message(timeout);
            co_await handler.wait_for_message(timeout);
        }, timeout), false);

        for (size_t i = 0; i < 4; ++i) {
            errinj.receive_message("injection1");
        }

        BOOST_REQUIRE_NO_THROW(co_await std::move(f1));
        BOOST_REQUIRE_NO_THROW(co_await std::move(f2));
    }
    errinj.disable("injection1");

    errinj.enable("injection2");
    {
        // Test receiving enough unshared messages before waiting for them
        errinj.receive_message("injection2");
        errinj.receive_message("injection2");

        auto f1 = errinj.inject("injection2", [timeout] (auto& handler) {
            return handler.wait_for_message(timeout);
        }, false);
        auto f2 = errinj.inject("injection2", [timeout] (auto& handler) {
            return handler.wait_for_message(timeout);
        }, false);

        BOOST_REQUIRE_NO_THROW(co_await std::move(f1));
        BOOST_REQUIRE_NO_THROW(co_await std::move(f2));
    }
    errinj.disable("injection2");

    errinj.enable("injection3");
    {
        // Test receiving not enough unshared messages
        auto timeout_1s = db::timeout_clock::now() + 1s;

        auto f1 = errinj.inject("injection3", std::bind_front([] (auto timeout, auto& handler) -> future<> {
            co_await handler.wait_for_message(timeout);
            co_await handler.wait_for_message(timeout);
        }, timeout_1s), false);
        auto f2 = errinj.inject("injection3", std::bind_front([] (auto timeout, auto& handler) -> future<> {
            co_await handler.wait_for_message(timeout);
            co_await handler.wait_for_message(timeout);
        }, timeout_1s), false);

        for (size_t i = 0; i < 3; ++i) {
            errinj.receive_message("injection3");
        }

        BOOST_REQUIRE_THROW(co_await when_all_succeed(std::move(f1), std::move(f2)).discard_result(), std::runtime_error);
    }
    errinj.disable("injection3");

    errinj.enable("injection4");
    {
        // Test handlers sharing messages are independent of the not sharing ones
        auto f1 = errinj.inject("injection4", std::bind_front([] (auto timeout, auto& handler) -> future<> {
            co_await handler.wait_for_message(timeout);
            co_await handler.wait_for_message(timeout);
        }, timeout), true);
        auto f2 = errinj.inject("injection4", std::bind_front([] (auto timeout, auto& handler) -> future<> {
            co_await handler.wait_for_message(timeout);
            co_await handler.wait_for_message(timeout);
        }, timeout), true);
        auto f3 = errinj.inject("injection4", [timeout] (auto& handler) {
            return handler.wait_for_message(timeout);
        }, false);
        auto f4 = errinj.inject("injection4", [timeout] (auto& handler) {
            return handler.wait_for_message(timeout);
        }, false);

        errinj.receive_message("injection4");
        errinj.receive_message("injection4");

        BOOST_REQUIRE_NO_THROW(co_await std::move(f1));
        BOOST_REQUIRE_NO_THROW(co_await std::move(f2));
        BOOST_REQUIRE_NO_THROW(co_await std::move(f3));
        BOOST_REQUIRE_NO_THROW(co_await std::move(f4));
    }
    errinj.disable("injection4");
}

SEASTAR_TEST_CASE(test_inject_with_parameters) {
    utils::error_injection<true> errinj;

    errinj.enable("injection", false, { { "x", "42" } });

    auto f = errinj.inject("injection", [] (auto& handler) {
        auto x = handler.get("x");
        auto y = handler.get("y");
        BOOST_REQUIRE(x && *x == "42");
        BOOST_REQUIRE(!y);
        return make_ready_future<>();
    });

    BOOST_REQUIRE_NO_THROW(co_await std::move(f));
}

// Test error injection CQL API
// NOTE: currently since functions can't get terminals an auxiliary table
//       with error injection names and one shot parameters
SEASTAR_TEST_CASE(test_inject_cql) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&e] {
            // Type of returned list of error injections cql3/functions/error_injcetion_fcts.cc
            const auto my_list_type = list_type_impl::get_instance(ascii_type, false);
#ifdef SCYLLA_ENABLE_ERROR_INJECTION
            auto row_empty = my_list_type->decompose(make_list_value(my_list_type, list_type_impl::native_type{{}}));
            auto row_test1 = my_list_type->decompose(make_list_value(my_list_type, list_type_impl::native_type{{"test1"}}));
            auto row_test2 = my_list_type->decompose(make_list_value(my_list_type, list_type_impl::native_type{{"test2"}}));
#else
            auto row_empty = my_list_type->decompose(make_list_value(my_list_type, list_type_impl::native_type{{}}));
            auto row_test1 = row_empty;
            auto row_test2 = row_empty;
#endif

            // Auxiliary table with terminals
            cquery_nofail(e, "create table error_name (name ascii primary key, one_shot ascii)");

            // Enable (test1,one_shot=true)
            cquery_nofail(e, "insert into  error_name (name, one_shot) values ('test1', 'true')");

            // Check no error injections before injecting
            auto ret0 = e.execute_cql("select enabled_injections() from error_name limit 1").get();
            assert_that(ret0).is_rows().with_rows({
                {row_empty}
            });

            cquery_nofail(e, "select enable_injection(name, one_shot)  from error_name where name = 'test1'");
            // enabled_injections() returns a list all injections in one call, so limit 1
            auto ret1 = e.execute_cql("select enabled_injections() from error_name limit 1").get();
            assert_that(ret1).is_rows().with_rows({
                {row_test1}
            });
            utils::get_local_injector().inject("test1", [] {}); // Noop one-shot injection
            auto ret2 = e.execute_cql("select enabled_injections() from error_name limit 1").get();
            assert_that(ret2).is_rows().with_rows({
                // Empty list after one shot executed
                {row_empty}
            });

            // Again (test1,one_shot=true) but disable with CQL API
            cquery_nofail(e, "select enable_injection(name, one_shot)  from error_name where name = 'test1'");
            // enabled_injections() returns a list all injections in one call, so limit 1
            auto ret3 = e.execute_cql("select enabled_injections() from error_name limit 1").get();
            assert_that(ret3).is_rows().with_rows({
                {row_test1}
            });
            // Disable
            cquery_nofail(e, "select disable_injection(name) from error_name where name = 'test1'");
            auto ret4 = e.execute_cql("select enabled_injections() from error_name limit 1").get();
            assert_that(ret4).is_rows().with_rows({
                // Empty list after one shot disabled
                {row_empty}
            });

            cquery_nofail(e, "insert into  error_name (name, one_shot) values ('test2', 'false')");
            cquery_nofail(e, "select enable_injection(name, one_shot)  from error_name where name = 'test2'");
            utils::get_local_injector().inject("test2", [] {}); // Noop injection, doesn't disable
            auto ret5 = e.execute_cql("select enabled_injections() from error_name limit 1").get();
            assert_that(ret5).is_rows().with_rows({
                {row_test2}
            });
        });
    });
}
