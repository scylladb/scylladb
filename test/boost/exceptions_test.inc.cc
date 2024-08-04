/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

// Common definitions of test cases used in
//   handle_exception_optimized_test.cc
//   handle_exception_fallback_test.cc

#include <exception>
#include <stdexcept>
#include <type_traits>
#include <cxxabi.h>
#include <boost/test/unit_test_log.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include "seastarx.hh"
#include <seastar/core/future.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/log.hh>

#include "utils/assert.hh"
#include "utils/exceptions.hh"

class base_exception : public std::exception {};
class derived_exception : public base_exception {};

static void dummy_fn(void*) {
    //
}

template<typename T>
static std::exception_ptr maybe_wrap_eptr(T&& t) {
    if constexpr (std::is_same_v<T, std::exception_ptr>) {
        return std::move(t);
    } else {
        return std::make_exception_ptr(std::move(t));
    }
}

static const std::type_info& eptr_typeid(std::exception_ptr eptr) {
    try {
        std::rethrow_exception(eptr);
    } catch (...) {
        return *abi::__cxa_current_exception_type();
    }
}

template<typename Capture, typename Throw>
static void check_catch(Throw&& ex) {
    auto eptr = maybe_wrap_eptr(std::move(ex));
    BOOST_TEST_MESSAGE("Checking if " << seastar::pretty_type_name(eptr_typeid(eptr))
            << " is caught as " << seastar::pretty_type_name(typeid(Capture)));

    auto typed_eptr = try_catch<Capture>(eptr);
    BOOST_CHECK_NE(typed_eptr, nullptr);

    // Verify that it's the same as what the usual throwing gives
    // TODO: Does this check make sense? Does the standard guarantee
    // that this will give the same pointer?
    try {
        std::rethrow_exception(eptr);
    } catch (Capture& t) {
        BOOST_CHECK_EQUAL(typed_eptr, &t);
    } catch (...) {
        // Can happen if the first check fails, just skip
        SCYLLA_ASSERT(typed_eptr == nullptr);
    }
}

template<typename Capture, typename Throw>
static void check_no_catch(Throw&& ex) {
    auto eptr = maybe_wrap_eptr(std::move(ex));
    BOOST_TEST_MESSAGE("Checking if " << seastar::pretty_type_name(eptr_typeid(eptr))
            << " is NOT caught as " << seastar::pretty_type_name(typeid(Capture)));

    auto typed_eptr = try_catch<Capture>(eptr);
    BOOST_CHECK_EQUAL(typed_eptr, nullptr);
}

template<typename A, typename B>
static std::exception_ptr make_nested_eptr(A&& a, B&& b) {
    try {
        throw std::move(b);
    } catch (...) {
        try {
            std::throw_with_nested(std::move(a));
        } catch (...) {
            return std::current_exception();
        }
    }
}

SEASTAR_TEST_CASE(test_try_catch) {
    // Some standard examples, throwing exceptions derived from std::exception
    // and catching them through their base types

    check_catch<derived_exception>(derived_exception());
    check_catch<base_exception>(derived_exception());
    check_catch<std::exception>(derived_exception());
    check_no_catch<std::runtime_error>(derived_exception());

    check_no_catch<derived_exception>(base_exception());
    check_catch<base_exception>(base_exception());
    check_catch<std::exception>(base_exception());
    check_no_catch<std::runtime_error>(base_exception());

    // Catching nested exceptions
    check_catch<base_exception>(make_nested_eptr(base_exception(), derived_exception()));
    check_catch<std::nested_exception>(make_nested_eptr(base_exception(), derived_exception()));
    check_no_catch<derived_exception>(make_nested_eptr(base_exception(), derived_exception()));

    // Check that everything works if we throw some crazy stuff
    check_catch<int>(int(1));
    check_no_catch<std::exception>(int(1));

    check_no_catch<int>(nullptr);
    check_no_catch<std::exception>(nullptr);

    // Catching pointers is not supported, but nothing should break if they are
    // being thrown
    derived_exception exc;
    check_no_catch<int>(&exc);
    check_no_catch<std::exception>(&exc);

    check_no_catch<int>(&dummy_fn);
    check_no_catch<std::exception>(&dummy_fn);

    check_no_catch<int>(&std::exception::what);
    check_no_catch<std::exception>(&std::exception::what);

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_make_nested_exception_ptr) {
    auto inner = std::make_exception_ptr(std::runtime_error("inner"));
    auto outer = make_nested_exception_ptr(std::runtime_error("outer"), inner);

    try {
        std::rethrow_exception(outer);
    } catch (const std::runtime_error& ex) {
        BOOST_REQUIRE_EQUAL(std::string(ex.what()), "outer");
        auto* nested = dynamic_cast<const std::nested_exception*>(&ex);
        BOOST_REQUIRE_NE(nested, nullptr);
        BOOST_REQUIRE_EQUAL(nested->nested_ptr(), inner);
    }

    try {
        std::rethrow_exception(outer);
    } catch (const std::nested_exception& ex) {
        BOOST_REQUIRE_EQUAL(ex.nested_ptr(), inner);
    }

    // Not a class
    BOOST_REQUIRE_THROW(std::rethrow_exception(make_nested_exception_ptr(int(123), inner)), int);

    // Final, so cannot add the std::nested_exception mixin to it
    struct ultimate_exception final : public std::exception {};
    BOOST_REQUIRE_THROW(std::rethrow_exception(make_nested_exception_ptr(ultimate_exception(), inner)), ultimate_exception);

    // Already derived from nested exception, so cannot put more to it
    struct already_nested_exception : public std::exception, public std::nested_exception {};
    auto inner2 = std::make_exception_ptr(std::runtime_error("inner2"));
    try {
        std::rethrow_exception(inner2);
    } catch (const std::runtime_error&) {
        try {
            std::rethrow_exception(make_nested_exception_ptr(already_nested_exception(), inner));
        } catch (const already_nested_exception& ex) {
            BOOST_REQUIRE_EQUAL(ex.nested_ptr(), inner2);
        }
    }

    return make_ready_future<>();
}
