/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "exceptions.hh"
#include <seastar/core/print.hh>
#include <seastar/util/log.hh>

namespace exceptions {

truncate_exception::truncate_exception(std::exception_ptr ep)
    : request_execution_exception(exceptions::exception_code::TRUNCATE_ERROR, format("Error during truncate: {}", ep))
{}

const std::unordered_map<exception_code, sstring>& exception_map() {
    static const std::unordered_map<exception_code, sstring> map {
        {exception_code::SERVER_ERROR, "server_error"},
        {exception_code::PROTOCOL_ERROR, "protocol_error"},
        {exception_code::BAD_CREDENTIALS, "authentication"},
        {exception_code::UNAVAILABLE, "unavailable"},
        {exception_code::OVERLOADED, "overloaded"},
        {exception_code::IS_BOOTSTRAPPING, "is_bootstrapping"},
        {exception_code::TRUNCATE_ERROR, "truncate_error"},
        {exception_code::WRITE_TIMEOUT, "write_timeout"},
        {exception_code::READ_TIMEOUT, "read_timeout"},
        {exception_code::READ_FAILURE, "read_failure"},
        {exception_code::FUNCTION_FAILURE, "function_failure"},
        {exception_code::WRITE_FAILURE, "write_failure"},
        {exception_code::CDC_WRITE_FAILURE, "cdc_write_failure"},
        {exception_code::SYNTAX_ERROR, "syntax_error"},
        {exception_code::UNAUTHORIZED, "unauthorized"},
        {exception_code::INVALID, "invalid"},
        {exception_code::CONFIG_ERROR, "config_error"},
        {exception_code::ALREADY_EXISTS, "already_exists"},
        {exception_code::UNPREPARED, "unprepared"},
        {exception_code::RATE_LIMIT_ERROR, "rate_limit_error"}
    };
    return map;
}

template<typename... Args>
static inline sstring prepare_message(const char* fmt, Args&&... args) noexcept {
    try {
        return format(fmt, std::forward<Args>(args)...);
    } catch (...) {
        return sstring();
    }
}

unavailable_exception::unavailable_exception(db::consistency_level cl, int32_t required, int32_t alive) noexcept
    : unavailable_exception(prepare_message("Cannot achieve consistency level for cl {}. Requires {}, alive {}", cl, required, alive),
        cl, required, alive)
    {}

read_write_timeout_exception::read_write_timeout_exception(exception_code code, const sstring& ks, const sstring& cf, db::consistency_level consistency, int32_t received, int32_t block_for) noexcept
    : request_timeout_exception{code, prepare_message("Operation timed out for {}.{} - received only {} responses from {} CL={}.", ks, cf, received, block_for, consistency)}
    , consistency{consistency}
    , received{received}
    , block_for{block_for}
    { }

request_failure_exception::request_failure_exception(exception_code code, const sstring& ks, const sstring& cf, db::consistency_level consistency_, int32_t received_, int32_t failures_, int32_t block_for_) noexcept
    : cassandra_exception{code, prepare_message("Operation failed for {}.{} - received {} responses and {} failures from {} CL={}.", ks, cf, received_, failures_, block_for_, consistency_)}
    , consistency{consistency_}
    , received{received_}
    , failures{failures_}
    , block_for{block_for_}
    {}

overloaded_exception::overloaded_exception(size_t c) noexcept
    : cassandra_exception(exception_code::OVERLOADED, prepare_message("Too many in flight hints: {}", c))
    {}

rate_limit_exception::rate_limit_exception(const sstring& ks, const sstring& cf, db::operation_type op_type_, bool rejected_by_coordinator_) noexcept
    : cassandra_exception(exception_code::RATE_LIMIT_ERROR, prepare_message("Per-partition rate limit reached for {} in table {}.{}, rejected by {}", op_type_, ks, cf, rejected_by_coordinator_ ? "coordinator" : "replicas"))
    , op_type(op_type_)
    , rejected_by_coordinator(rejected_by_coordinator_)
    { }

prepared_query_not_found_exception::prepared_query_not_found_exception(bytes id) noexcept
    : request_validation_exception{exception_code::UNPREPARED, prepare_message("No prepared statement with ID {} found.", id)}
    , id{id}
    { }

already_exists_exception::already_exists_exception(sstring ks_name_, sstring cf_name_)
    : already_exists_exception{ks_name_, cf_name_, format("Cannot add already existing table \"{}\" to keyspace \"{}\"", cf_name_, ks_name_)}
    { }

already_exists_exception::already_exists_exception(sstring ks_name_)
    : already_exists_exception{ks_name_, "", format("Cannot add existing keyspace \"{}\"", ks_name_)}
    { }

function_execution_exception::function_execution_exception(sstring func_name_, sstring detail, sstring ks_name_, std::vector<sstring> args_) noexcept
    : cassandra_exception{exception_code::FUNCTION_FAILURE,
        format("execution of {} failed: {}", func_name_, detail)}
    , ks_name(std::move(ks_name_))
    , func_name(std::move(func_name_))
    , args(std::move(args_))
    { }

}
