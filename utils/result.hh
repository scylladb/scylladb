/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

// Basic utilities which allow to start working with boost::outcome::result
// in conjunction with our exception_container.

#include <boost/outcome/success_failure.hpp>
#include <boost/outcome/trait.hpp>
#include <boost/outcome/policy/base.hpp>
#include <boost/outcome/result.hpp>
#include "utils/exception_container.hh"

namespace bo = BOOST_OUTCOME_V2_NAMESPACE;

namespace utils {

// A policy which throws the container_error associated with the result
// if there was an attempt to access value while it was not present.
struct exception_container_throw_policy : bo::policy::base {
    template<class Impl> static constexpr void wide_value_check(Impl&& self) {
        if (!base::_has_value(self)) {
            base::_error(self).throw_me();
        }
    }

    template<class Impl> static constexpr void wide_error_check(Impl&& self) {
        if (!base::_has_error(self)) {
            throw bo::bad_result_access("no error");
        }
    }
};

template<typename T, typename... Exs>
using result_with_exception = bo::result<T, exception_container<Exs...>, exception_container_throw_policy>;

template<typename R>
concept ExceptionContainerResult = bo::is_basic_result<R>::value && ExceptionContainer<typename R::error_type>;

template<typename F>
concept ExceptionContainerResultFuture = seastar::is_future<F>::value && ExceptionContainerResult<typename F::value_type>;

template<typename L, typename R>
concept ResultRebindableTo =
    bo::is_basic_result<L>::value &&
    bo::is_basic_result<R>::value &&
    std::same_as<typename L::error_type, typename R::error_type> &&
    std::same_as<typename L::no_value_policy_type, typename R::no_value_policy_type>;

// Creates a result type which has the same error type as R, but has a different value type.
// The name was inspired by std::allocator::rebind.
template<typename T, ExceptionContainerResult R>
using rebind_result = bo::result<T, typename R::error_type, exception_container_throw_policy>;

}
