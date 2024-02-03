/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/exception_container.hh"

#include "test/lib/scylla_test_case.hh"
#include <seastar/core/sstring.hh>

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

using foo_bar_container = utils::exception_container<foo_exception, bar_exception>;

static sstring foo_bar_what(const foo_bar_container& fbc) {
    return fbc.accept([] (const auto& ex) { return ex.what(); });
}

SEASTAR_TEST_CASE(test_exception_container) {
    auto empty = foo_bar_container();
    auto foo = foo_bar_container(foo_exception());
    auto bar = foo_bar_container(bar_exception());

    BOOST_REQUIRE(empty.empty());
    BOOST_REQUIRE(!foo.empty());
    BOOST_REQUIRE(!bar.empty());

    BOOST_REQUIRE(!empty);
    BOOST_REQUIRE(foo);
    BOOST_REQUIRE(bar);

    BOOST_REQUIRE_EQUAL(foo_bar_what(empty), sstring("bad exception container access"));
    BOOST_REQUIRE_EQUAL(foo_bar_what(foo), sstring("foo"));
    BOOST_REQUIRE_EQUAL(foo_bar_what(bar), sstring("bar"));

    BOOST_REQUIRE_THROW(empty.throw_me(), utils::bad_exception_container_access);
    BOOST_REQUIRE_THROW(foo.throw_me(), foo_exception);
    BOOST_REQUIRE_THROW(bar.throw_me(), bar_exception);

    // Construct the futures outside BOOST_REQUIRE_THROW
    // otherwise the checks would pass if as_exception_future throws
    // and we don't want that
    auto f_empty = empty.as_exception_future();
    auto f_foo = foo.as_exception_future();
    auto f_bar = bar.as_exception_future();
    BOOST_REQUIRE_THROW(f_empty.get(), utils::bad_exception_container_access);
    BOOST_REQUIRE_THROW(f_foo.get(), foo_exception);
    BOOST_REQUIRE_THROW(f_bar.get(), bar_exception);

    // Same reasoning as with as_exception_future
    f_empty = std::move(empty).into_exception_future();
    f_foo = std::move(foo).into_exception_future();
    f_bar = std::move(bar).into_exception_future();
    BOOST_REQUIRE_THROW(f_empty.get(), utils::bad_exception_container_access);
    BOOST_REQUIRE_THROW(f_foo.get(), foo_exception);
    BOOST_REQUIRE_THROW(f_bar.get(), bar_exception);

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_exception_container_empty_accept) {
    auto empty = foo_bar_container();

    struct visitor {
        sstring operator()(const foo_exception&) {
            return "had foo exception";
        }
        sstring operator()(const bar_exception&) {
            return "had bar exception";
        }
        sstring operator()(const utils::bad_exception_container_access&) {
            return "was empty";
        }
    };

    BOOST_REQUIRE_EQUAL(empty.accept(visitor{}), "was empty");

    return make_ready_future<>();
}
