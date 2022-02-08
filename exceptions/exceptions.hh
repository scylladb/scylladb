/*
 */

/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "db/consistency_level_type.hh"
#include "db/write_type.hh"
#include <stdexcept>
#include <seastar/core/sstring.hh>
#include <seastar/core/print.hh>
#include "bytes.hh"
#include <boost/outcome/result.hpp>
#include "utils/exception_container.hh"
#include "utils/result.hh"

namespace exceptions {

enum class exception_code : int32_t {
    SERVER_ERROR    = 0x0000,
    PROTOCOL_ERROR  = 0x000A,

    BAD_CREDENTIALS = 0x0100,

    // 1xx: problem during request execution
    UNAVAILABLE     = 0x1000,
    OVERLOADED      = 0x1001,
    IS_BOOTSTRAPPING= 0x1002,
    TRUNCATE_ERROR  = 0x1003,
    WRITE_TIMEOUT   = 0x1100,
    READ_TIMEOUT    = 0x1200,
    READ_FAILURE    = 0x1300,
    FUNCTION_FAILURE= 0x1400,
    WRITE_FAILURE   = 0x1500,
    CDC_WRITE_FAILURE = 0x1600,

    // 2xx: problem validating the request
    SYNTAX_ERROR    = 0x2000,
    UNAUTHORIZED    = 0x2100,
    INVALID         = 0x2200,
    CONFIG_ERROR    = 0x2300,
    ALREADY_EXISTS  = 0x2400,
    UNPREPARED      = 0x2500
};

const std::unordered_map<exception_code, sstring>& exception_map();

class cassandra_exception : public std::exception {
private:
    exception_code _code;
    sstring _msg;
protected:
    template<typename... Args>
    static inline sstring prepare_message(const char* fmt, Args&&... args) noexcept {
        try {
            return format(fmt, std::forward<Args>(args)...);
        } catch (...) {
            return sstring();
        }
    }
public:
    cassandra_exception(exception_code code, sstring msg) noexcept
        : _code(code)
        , _msg(std::move(msg))
    { }
    virtual const char* what() const noexcept override { return _msg.c_str(); }
    exception_code code() const { return _code; }
    sstring get_message() const { return what(); }
};

class server_exception : public cassandra_exception {
public:
    server_exception(sstring msg) noexcept
        : exceptions::cassandra_exception{exceptions::exception_code::SERVER_ERROR, std::move(msg)}
    { }
};

class protocol_exception : public cassandra_exception {
public:
    protocol_exception(sstring msg) noexcept
        : exceptions::cassandra_exception{exceptions::exception_code::PROTOCOL_ERROR, std::move(msg)}
    { }
};

struct unavailable_exception : cassandra_exception {
    db::consistency_level consistency;
    int32_t required;
    int32_t alive;


    unavailable_exception(sstring msg, db::consistency_level cl, int32_t required, int32_t alive) noexcept
        : exceptions::cassandra_exception(exceptions::exception_code::UNAVAILABLE, std::move(msg))
        , consistency(cl)
        , required(required)
        , alive(alive)
    {}

    unavailable_exception(db::consistency_level cl, int32_t required, int32_t alive) noexcept
        : unavailable_exception(prepare_message("Cannot achieve consistency level for cl {}. Requires {}, alive {}", cl, required, alive),
                cl, required, alive)
    {}
};

class request_execution_exception : public cassandra_exception {
public:
    request_execution_exception(exception_code code, sstring msg) noexcept
        : cassandra_exception(code, std::move(msg))
    { }
};

class truncate_exception : public request_execution_exception
{
public:
    truncate_exception(std::exception_ptr ep);
};

class request_timeout_exception : public cassandra_exception {
public:
    db::consistency_level consistency;
    int32_t received;
    int32_t block_for;

    request_timeout_exception(exception_code code, const sstring& ks, const sstring& cf, db::consistency_level consistency, int32_t received, int32_t block_for) noexcept
        : cassandra_exception{code, prepare_message("Operation timed out for {}.{} - received only {} responses from {} CL={}.", ks, cf, received, block_for, consistency)}
        , consistency{consistency}
        , received{received}
        , block_for{block_for}
    { }
};

class read_timeout_exception : public request_timeout_exception {
public:
    bool data_present;

    read_timeout_exception(const sstring& ks, const sstring& cf, db::consistency_level consistency, int32_t received, int32_t block_for, bool data_present) noexcept
        : request_timeout_exception{exception_code::READ_TIMEOUT, ks, cf, consistency, received, block_for}
        , data_present{data_present}
    { }
};

struct mutation_write_timeout_exception : public request_timeout_exception {
    db::write_type type;
    mutation_write_timeout_exception(const sstring& ks, const sstring& cf, db::consistency_level consistency, int32_t received, int32_t block_for, db::write_type type) noexcept :
        request_timeout_exception(exception_code::WRITE_TIMEOUT, ks, cf, consistency, received, block_for)
        , type{std::move(type)}
    { }
};

class request_failure_exception : public cassandra_exception {
public:
    db::consistency_level consistency;
    int32_t received;
    int32_t failures;
    int32_t block_for;

protected:
    request_failure_exception(exception_code code, const sstring& ks, const sstring& cf, db::consistency_level consistency_, int32_t received_, int32_t failures_, int32_t block_for_) noexcept
        : cassandra_exception{code, prepare_message("Operation failed for {}.{} - received {} responses and {} failures from {} CL={}.", ks, cf, received_, failures_, block_for_, consistency_)}
        , consistency{consistency_}
        , received{received_}
        , failures{failures_}
        , block_for{block_for_}
    {}

    request_failure_exception(exception_code code, const sstring& msg, db::consistency_level consistency_, int32_t received_, int32_t failures_, int32_t block_for_) noexcept
        : cassandra_exception{code, msg}
        , consistency{consistency_}
        , received{received_}
        , failures{failures_}
        , block_for{block_for_}
    {}
};

struct mutation_write_failure_exception : public request_failure_exception {
    db::write_type type;
    mutation_write_failure_exception(const sstring& ks, const sstring& cf, db::consistency_level consistency_, int32_t received_, int32_t failures_, int32_t block_for_, db::write_type type_) noexcept :
        request_failure_exception(exception_code::WRITE_FAILURE, ks, cf, consistency_, received_, failures_, block_for_)
        , type{std::move(type_)}
    { }

    mutation_write_failure_exception(const sstring& msg, db::consistency_level consistency_, int32_t received_, int32_t failures_, int32_t block_for_, db::write_type type_) noexcept :
        request_failure_exception(exception_code::WRITE_FAILURE, msg, consistency_, received_, failures_, block_for_)
        , type{std::move(type_)}
    { }
};

struct read_failure_exception : public request_failure_exception {
    bool data_present;

    read_failure_exception(const sstring& ks, const sstring& cf, db::consistency_level consistency_, int32_t received_, int32_t failures_, int32_t block_for_, bool data_present_) noexcept
        : request_failure_exception{exception_code::READ_FAILURE, ks, cf, consistency_, received_, failures_, block_for_}
        , data_present{data_present_}
    { }

    read_failure_exception(const sstring& msg, db::consistency_level consistency_, int32_t received_, int32_t failures_, int32_t block_for_, bool data_present_) noexcept
        : request_failure_exception{exception_code::READ_FAILURE, msg, consistency_, received_, failures_, block_for_}
        , data_present{data_present_}
    { }
};

struct overloaded_exception : public cassandra_exception {
    explicit overloaded_exception(size_t c) noexcept :
        cassandra_exception(exception_code::OVERLOADED, prepare_message("Too many in flight hints: {}", c)) {}
    explicit overloaded_exception(sstring msg) noexcept :
        cassandra_exception(exception_code::OVERLOADED, std::move(msg)) {}
};

class request_validation_exception : public cassandra_exception {
public:
    using cassandra_exception::cassandra_exception;
};

class invalidated_prepared_usage_attempt_exception : public exceptions::request_validation_exception {
public:
    invalidated_prepared_usage_attempt_exception() : request_validation_exception{exception_code::UNPREPARED, "Attempt to execute the invalidated prepared statement."} {}
};

class unauthorized_exception: public request_validation_exception {
public:
    unauthorized_exception(sstring msg) noexcept
                    : request_validation_exception(exception_code::UNAUTHORIZED,
                                    std::move(msg)) {
    }
};

class authentication_exception: public request_validation_exception {
public:
    authentication_exception(sstring msg) noexcept
                    : request_validation_exception(exception_code::BAD_CREDENTIALS,
                                    std::move(msg)) {
    }
};

class invalid_request_exception : public request_validation_exception {
public:
    invalid_request_exception(sstring cause) noexcept
        : request_validation_exception(exception_code::INVALID, std::move(cause))
    { }
};

class keyspace_not_defined_exception : public invalid_request_exception {
public:
    keyspace_not_defined_exception(std::string cause) noexcept
        : invalid_request_exception(std::move(cause))
    { }
};

class overflow_error_exception : public invalid_request_exception {
public:
    overflow_error_exception(std::string msg) noexcept
        : invalid_request_exception(std::move(msg))
    { }
};

class prepared_query_not_found_exception : public request_validation_exception {
public:
    bytes id;

    prepared_query_not_found_exception(bytes id) noexcept
        : request_validation_exception{exception_code::UNPREPARED, prepare_message("No prepared statement with ID {} found.", id)}
        , id{id}
    { }
};

class syntax_exception : public request_validation_exception {
public:
    syntax_exception(sstring msg) noexcept
        : request_validation_exception(exception_code::SYNTAX_ERROR, std::move(msg))
    { }
};

class configuration_exception : public request_validation_exception {
public:
    configuration_exception(sstring msg) noexcept
        : request_validation_exception{exception_code::CONFIG_ERROR, std::move(msg)}
    { }

    configuration_exception(exception_code code, sstring msg) noexcept
        : request_validation_exception{code, std::move(msg)}
    { }
};

class already_exists_exception : public configuration_exception {
public:
    const sstring ks_name;
    const sstring cf_name;
private:
    already_exists_exception(sstring ks_name_, sstring cf_name_, sstring msg)
        : configuration_exception{exception_code::ALREADY_EXISTS, msg}
        , ks_name{ks_name_}
        , cf_name{cf_name_}
    { }
public:
    already_exists_exception(sstring ks_name_, sstring cf_name_)
        : already_exists_exception{ks_name_, cf_name_, format("Cannot add already existing table \"{}\" to keyspace \"{}\"", cf_name_, ks_name_)}
    { }

    already_exists_exception(sstring ks_name_)
        : already_exists_exception{ks_name_, "", format("Cannot add existing keyspace \"{}\"", ks_name_)}
    { }
};

class recognition_exception : public std::runtime_error {
public:
    recognition_exception(const std::string& msg) : std::runtime_error(msg) {};
};

class unsupported_operation_exception : public std::runtime_error {
public:
    unsupported_operation_exception() : std::runtime_error("unsupported operation") {}
    unsupported_operation_exception(const sstring& msg) : std::runtime_error("unsupported operation: " + msg) {}
};

class function_execution_exception : public cassandra_exception {
public:
    const sstring ks_name;
    const sstring func_name;
    const std::vector<sstring> args;
    function_execution_exception(sstring func_name_, sstring detail, sstring ks_name_, std::vector<sstring> args_) noexcept
        : cassandra_exception{exception_code::FUNCTION_FAILURE,
            format("execution of {} failed: {}", func_name_, detail)}
        , ks_name(std::move(ks_name_))
        , func_name(std::move(func_name_))
        , args(std::move(args_))
    { }
};

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
    read_failure_exception
>;

template<typename T = void>
using coordinator_result = bo::result<T,
    coordinator_exception_container,
    utils::exception_container_throw_policy
>;

}
