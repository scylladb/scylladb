/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "exceptions.hh"
#include <boost/outcome/result.hpp>
#include "utils/exception_container.hh"
#include "utils/result.hh"

namespace exceptions {

// Allows to pass a coordinator exception as a value. With coordinator_result,
// it is possible to handle exceptions and inspect their type/value without
// resorting to costly rethrows. On the other hand, using them is more
// cumbersome than just using exceptions and exception futures.
//
// Not all exceptions are passed in this way, therefore the container
// does not allow all types of coordinator exceptions. On the other hand,
// an exception being listed here does not mean it is _always_ passed
// in an exception_container - it can be thrown in a regular fashion
// as well.
//
// It is advised to use this mechanism mainly for exceptions which can
// happen frequently, e.g. signalling timeouts, overloads or rate limits.
using coordinator_exception_container = utils::exception_container<
    mutation_write_timeout_exception,
    read_timeout_exception,
    read_failure_exception,
    rate_limit_exception,
    overloaded_exception
>;

template<typename T = void>
using coordinator_result = bo::result<T,
    coordinator_exception_container,
    utils::exception_container_throw_policy
>;

}
