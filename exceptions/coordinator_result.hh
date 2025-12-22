/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include "exceptions.hh"
#include <boost/outcome/result.hpp>
#include "utils/exception_container.hh"
#include "utils/result.hh"
#include <variant>

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
    overloaded_exception,
    read_failure_exception_with_timeout
>;

template<typename T = void>
using coordinator_result = bo::result<T,
    coordinator_exception_container,
    utils::exception_container_throw_policy
>;

// Serializable representation of mutation_write_timeout_exception for RPC
struct mutation_write_timeout_exception_serialized {
    sstring message;
    db::consistency_level consistency;
    int32_t received;
    int32_t block_for;
    db::write_type type;
};

// Serializable representation of read_timeout_exception for RPC
struct read_timeout_exception_serialized {
    sstring message;
    db::consistency_level consistency;
    int32_t received;
    int32_t block_for;
    bool data_present;
};

// Serializable representation of read_failure_exception for RPC
struct read_failure_exception_serialized {
    sstring message;
    db::consistency_level consistency;
    int32_t received;
    int32_t failures;
    int32_t block_for;
    bool data_present;
};

// Serializable representation of rate_limit_exception for RPC
struct rate_limit_exception_serialized {
    sstring ks_name;
    sstring cf_name;
    db::operation_type op_type;
    bool rejected_by_coordinator;
};

// Serializable representation of overloaded_exception for RPC
struct overloaded_exception_serialized {
    sstring message;
};

// Variant to hold any serialized coordinator exception
struct coordinator_exception_serialized {
    std::variant<
        mutation_write_timeout_exception_serialized,
        read_timeout_exception_serialized,
        read_failure_exception_serialized,
        rate_limit_exception_serialized,
        overloaded_exception_serialized
    > exception;
};

// Convert coordinator_exception_container to its serialized form
coordinator_exception_serialized serialize_coordinator_exception(const coordinator_exception_container& ex);

// Reconstruct the exception from its serialized form
coordinator_exception_container deserialize_coordinator_exception(const coordinator_exception_serialized& ex);

}
