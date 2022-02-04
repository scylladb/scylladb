/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <vector>
#include <stdexcept>
#include "utils/exception_container.hh"
#include "utils/result.hh"
#include "utils/result_try.hh"

#include <seastar/testing/test_case.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/map_reduce.hh>
#include <seastar/util/later.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace seastar;

class foo_exception : public std::exception {
public:
    const char* what() const noexcept override {
        return "foo";
    }
};

class bar_exception : public std::exception {
public:
    const char* what() const noexcept override {
        return "bar";
    }
};

using exc_container = utils::exception_container<foo_exception, bar_exception>;

template<typename T = void>
using result = bo::result<T, exc_container,utils::exception_container_throw_policy>;

SEASTAR_TEST_CASE(test_exception_container_throw_policy) {
    result<> r_ok = bo::success();
    BOOST_REQUIRE_NO_THROW(r_ok.value());
    BOOST_REQUIRE_THROW(r_ok.error(), bo::bad_result_access);

    result<> r_err_foo = bo::failure(foo_exception());
    BOOST_REQUIRE_NO_THROW(r_err_foo.error());
    BOOST_REQUIRE_THROW(r_err_foo.value(), foo_exception);

    return make_ready_future<>();
}

SEASTAR_THREAD_TEST_CASE(test_result_into_future) {
    // T == void

    result<> r_ok = bo::success();
    auto f_ok = utils::result_into_future(std::move(r_ok));
    BOOST_REQUIRE_NO_THROW(f_ok.get());

    result<> r_err_foo = bo::failure(foo_exception());
    auto f_err_foo = utils::result_into_future(std::move(r_err_foo));
    BOOST_REQUIRE_THROW(f_err_foo.get(), foo_exception);

    // T != void

    result<int> r_ok_int = bo::success();
    auto f_ok_int = utils::result_into_future(std::move(r_ok_int));
    BOOST_REQUIRE_NO_THROW(f_ok_int.get());

    result<int> r_err_foo_int = bo::failure(foo_exception());
    auto f_err_foo_int = utils::result_into_future(std::move(r_err_foo_int));
    BOOST_REQUIRE_THROW(f_err_foo_int.get(), foo_exception);
}

SEASTAR_THREAD_TEST_CASE(test_then_ok_result) {
    auto f_void = utils::then_ok_result<result<>>(make_ready_future<>());
    BOOST_REQUIRE_NO_THROW(f_void.get().value());

    auto f_int = utils::then_ok_result<result<int>>(make_ready_future<int>(123));
    BOOST_REQUIRE_EQUAL(f_int.get().value(), 123);
}

SEASTAR_THREAD_TEST_CASE(test_result_wrap) {
    int run_count = 0;

    // T == void
    auto fun_void = utils::result_wrap([&run_count] {
        ++run_count;
        return result<>(bo::success());
    });

    BOOST_REQUIRE_NO_THROW(fun_void(result<>(bo::success())).get().value());
    BOOST_REQUIRE_EQUAL(run_count, 1);

    BOOST_REQUIRE_THROW(fun_void(result<>(bo::failure(foo_exception()))).get().value(), foo_exception);
    BOOST_REQUIRE_EQUAL(run_count, 1);

    // T != void
    auto fun_int = utils::result_wrap([&run_count] (int i) {
        ++run_count;
        return result<int>(bo::success(i));
    });

    BOOST_REQUIRE_EQUAL(fun_int(result<int>(bo::success(123))).get().value(), 123);
    BOOST_REQUIRE_EQUAL(run_count, 2);

    BOOST_REQUIRE_THROW(fun_int(result<int>(bo::failure(foo_exception()))).get().value(), foo_exception);
    BOOST_REQUIRE_EQUAL(run_count, 2);
}

SEASTAR_THREAD_TEST_CASE(test_result_parallel_for_each) {
    auto reduce = [] (auto... params) {
        std::vector<result<>> v;
        (v.push_back(std::move(params)), ...);
        utils::result_parallel_for_each<result<>>(std::move(v), [] (result<>& r) {
            return make_ready_future<result<>>(std::move(r));
        }).get().value(); // <- trying to access the value throws in case of error
    };

    auto foo_exc = [] () { return result<>(bo::failure(foo_exception())); };
    auto bar_exc = [] () { return result<>(bo::failure(bar_exception())); };

    BOOST_REQUIRE_NO_THROW(reduce(bo::success(), bo::success()));
    BOOST_REQUIRE_THROW(reduce(foo_exc(), bo::success()), foo_exception);
    BOOST_REQUIRE_THROW(reduce(bo::success(), foo_exc()), foo_exception);
    BOOST_REQUIRE_THROW(reduce(foo_exc(), bar_exc()), foo_exception);
    BOOST_REQUIRE_THROW(reduce(bar_exc(), foo_exc()), bar_exception);
}

namespace test_policies {

struct result_try {
    template<typename... Args>
    static auto do_try(Args&&... args) {
        return utils::result_try(std::forward<Args>(args)...);
    }

    static result<sstring> value(sstring value)   { return bo::success(std::move(value)); }
    static result<sstring> error(auto ex)         { return bo::failure(std::move(ex)); }
    static result<sstring> exception(auto ex)     { throw ex; }
    static result<sstring> rethrow(auto&& handle) { return handle.into_result(); }
};

struct result_futurize_try_exceptional_futures {
    template<typename... Args>
    static auto do_try(Args&&... args) {
        return utils::result_futurize_try(std::forward<Args>(args)...).get();
    }

    static future<result<sstring>> value(sstring value)   { return make_ready_future<result<sstring>>(bo::success(std::move(value))); }
    static future<result<sstring>> error(auto ex)         { return make_ready_future<result<sstring>>(bo::failure(std::move(ex))); }
    static future<result<sstring>> exception(auto ex)     { return make_exception_future<result<sstring>>(std::move(ex)); }
    static future<result<sstring>> rethrow(auto&& handle) { return handle.into_future(); }
};

struct result_futurize_try_exceptions {
    template<typename... Args>
    static auto do_try(Args&&... args) {
        return utils::result_futurize_try(std::forward<Args>(args)...).get();
    }

    static future<result<sstring>> value(sstring value)   { return make_ready_future<result<sstring>>(bo::success(std::move(value))); }
    static future<result<sstring>> error(auto ex)         { return make_ready_future<result<sstring>>(bo::failure(std::move(ex))); }
    static future<result<sstring>> exception(auto ex)     { throw ex; }
    static future<result<sstring>> rethrow(auto&& handle) { return make_ready_future<result<sstring>>(handle.into_result()); }
};

template<typename Base>
struct with_delay : public Base {
    using base = Base;

private:
    static future<result<sstring>> with_yield(future<result<sstring>> f) {
        return seastar::yield().then([f = std::move(f)] () mutable {
            return std::move(f);
        });
    }

public:
    static future<result<sstring>> value(sstring value)   { return with_yield(base::value(std::move(value))); }
    static future<result<sstring>> error(auto ex)         { return with_yield(base::error(std::move(ex))); }
    static future<result<sstring>> exception(auto ex)     { return with_yield(base::exception(std::move(ex))); }
    static future<result<sstring>> rethrow(auto&& handle) { return with_yield(base::rethrow(std::move(handle))); }
};

}

template<typename TestTemplate>
auto test_result_try_with_policies(TestTemplate&& template_fn) {
    BOOST_TEST_MESSAGE("Running test for result_try");
    template_fn(test_policies::result_try());

    BOOST_TEST_MESSAGE("Running test for result_futurize_try (exceptions as futures)");
    template_fn(test_policies::result_futurize_try_exceptional_futures());

    BOOST_TEST_MESSAGE("Running test for result_futurize_try (exceptions as futures, yielded futures)");
    template_fn(test_policies::with_delay<test_policies::result_futurize_try_exceptional_futures>());

    BOOST_TEST_MESSAGE("Running test for result_futurize_try (thrown exceptions)");
    template_fn(test_policies::result_futurize_try_exceptions());

    BOOST_TEST_MESSAGE("Running test for result_futurize_try (thrown exceptions, yielded futures)");
    template_fn(test_policies::with_delay<test_policies::result_futurize_try_exceptions>());
}

SEASTAR_THREAD_TEST_CASE(test_result_try_success_on_exception) {
    test_result_try_with_policies([] (auto policy) {
        auto handle = [&] (auto f) {
            return policy.do_try(
                std::move(f),
                utils::result_catch<foo_exception>([&] (const auto& ex) {
                    return policy.value("foo");
                }),
                utils::result_catch<bar_exception>([&] (const auto& ex) {
                    return policy.value("bar");
                })
            );
        };

        BOOST_CHECK_EQUAL(handle([&] { return policy.value("success"); }).value(), "success");
        BOOST_CHECK_EQUAL(handle([&] { return policy.error(foo_exception()); }).value(), "foo");
        BOOST_CHECK_EQUAL(handle([&] { return policy.error(bar_exception()); }).value(), "bar");
        BOOST_CHECK_EQUAL(handle([&] { return policy.exception(foo_exception()); }).value(), "foo");
        BOOST_CHECK_EQUAL(handle([&] { return policy.exception(bar_exception()); }).value(), "bar");
    });
}

SEASTAR_THREAD_TEST_CASE(test_result_try_rethrow_exception) {
    test_result_try_with_policies([] (auto policy) {
        bool was_handled = false;

        auto handle = [&] (auto f) {
            was_handled = false;
            return policy.do_try(
                std::move(f),
                utils::result_catch<foo_exception>([&] (const auto& ex, auto&& handle) {
                    was_handled = true;
                    return policy.rethrow(std::move(handle));
                }),
                utils::result_catch<bar_exception>([&] (const auto& ex, auto&& handle) {
                    was_handled = true;
                    return policy.rethrow(std::move(handle));
                })
            );
        };

        BOOST_CHECK_EQUAL(handle([&] { return policy.value("success"); }).value(), "success");
        BOOST_CHECK(!was_handled);
        BOOST_CHECK_EQUAL(handle([&] { return policy.error(foo_exception()); }).error(), exc_container(foo_exception()));
        BOOST_CHECK(was_handled);
        BOOST_CHECK_EQUAL(handle([&] { return policy.error(bar_exception()); }).error(), exc_container(bar_exception()));
        BOOST_CHECK(was_handled);
        BOOST_CHECK_THROW((void)handle([&] { return policy.exception(foo_exception()); }), foo_exception);
        BOOST_CHECK(was_handled);
        BOOST_CHECK_THROW((void)handle([&] { return policy.exception(bar_exception()); }), bar_exception);
        BOOST_CHECK(was_handled);
    });
}

SEASTAR_THREAD_TEST_CASE(test_result_try_handler_precedence) {
    test_result_try_with_policies([] (auto policy) {
        auto handle = [&] (auto f) {
            return policy.do_try(
                std::move(f),
                utils::result_catch<foo_exception>([&] (const auto& ex) {
                    return policy.value("foo");
                }),
                utils::result_catch<std::exception>([&] (const auto& ex) {
                    return policy.value("std::exception");
                })
            );
        };

        BOOST_CHECK_EQUAL(handle([&] { return policy.value("success"); }).value(), "success");
        BOOST_CHECK_EQUAL(handle([&] { return policy.error(foo_exception()); }).value(), "foo");
        BOOST_CHECK_EQUAL(handle([&] { return policy.error(bar_exception()); }).value(), "std::exception");
        BOOST_CHECK_EQUAL(handle([&] { return policy.exception(foo_exception()); }).value(), "foo");
        BOOST_CHECK_EQUAL(handle([&] { return policy.exception(bar_exception()); }).value(), "std::exception");
    });
}

SEASTAR_THREAD_TEST_CASE(test_result_try_unhandled_exception) {
    test_result_try_with_policies([] (auto policy) {
        auto handle = [&] (auto f) {
            return policy.do_try(
                std::move(f),
                utils::result_catch<foo_exception>([&] (const auto& ex) {
                    return policy.value("foo");
                })
            );
        };

        BOOST_CHECK_EQUAL(handle([&] { return policy.value("success"); }).value(), "success");
        BOOST_CHECK_EQUAL(handle([&] { return policy.error(foo_exception()); }).value(), "foo");
        BOOST_CHECK_EQUAL(handle([&] { return policy.error(bar_exception()); }).error(), exc_container(bar_exception()));
        BOOST_CHECK_EQUAL(handle([&] { return policy.exception(foo_exception()); }).value(), "foo");
        BOOST_CHECK_THROW((void)handle([&] { return policy.exception(bar_exception()); }), bar_exception);
    });
}

SEASTAR_THREAD_TEST_CASE(test_result_try_catch_dots) {
    test_result_try_with_policies([] (auto policy) {
        auto handle = [&] (auto f) {
            return policy.do_try(
                std::move(f),
                utils::result_catch<foo_exception>([&] (const auto& ex) {
                    return policy.value("foo");
                }),
                utils::result_catch_dots([&] () {
                    return policy.value("...");
                })
            );
        };

        BOOST_CHECK_EQUAL(handle([&] { return policy.value("success"); }).value(), "success");
        BOOST_CHECK_EQUAL(handle([&] { return policy.error(foo_exception()); }).value(), "foo");
        BOOST_CHECK_EQUAL(handle([&] { return policy.error(bar_exception()); }).value(), "...");
        BOOST_CHECK_EQUAL(handle([&] { return policy.exception(foo_exception()); }).value(), "foo");
        BOOST_CHECK_EQUAL(handle([&] { return policy.exception(bar_exception()); }).value(), "...");
    });
}

SEASTAR_THREAD_TEST_CASE(test_result_try_catch_forward_to_promise) {
    test_result_try_with_policies([] (auto policy) {
        auto handle = [&] (auto f) -> result<int> {
            // Handle error/failed result in catch handler and forward it to
            // promise of a different type
            promise<result<int>> prom;
            bool promise_set = false;
            try {
                policy.do_try(
                    std::move(f),
                    utils::result_catch_dots([&] (auto&& result) {
                        result.forward_to_promise(prom);
                        promise_set = true;
                        return policy.value("should not see that");
                    })
                ).value();
            } catch (...) {
                BOOST_CHECK_MESSAGE(false, "Exception or failed result was not forwarded to promise");
                throw;
            }

            // If not set by a handler, set the promise manually
            if (!promise_set) {
                prom.set_value(bo::success(123));
            }

            return prom.get_future().get();
        };

        BOOST_CHECK_EQUAL(handle([&] { return policy.value("success"); }).value(), 123);
        BOOST_CHECK_EQUAL(handle([&] { return policy.error(foo_exception()); }).error(), exc_container(foo_exception()));
        BOOST_CHECK_EQUAL(handle([&] { return policy.error(bar_exception()); }).error(), exc_container(bar_exception()));
        BOOST_CHECK_THROW((void)handle([&] { return policy.exception(foo_exception()); }), foo_exception);
        BOOST_CHECK_THROW((void)handle([&] { return policy.exception(bar_exception()); }), bar_exception);
    });
}
