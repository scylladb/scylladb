/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

#include "db/consistency_level_type.hh"
#include "db/write_type.hh"
#include "db/operation_type.hh"
#include "locator/host_id.hh"
#include "utils/small_vector.hh"
#include <memory>
#include <stdexcept>
#include <seastar/core/sstring.hh>
#include <seastar/core/lowres_clock.hh>
#include "bytes_fwd.hh"

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
    UNPREPARED      = 0x2500,

    // Scylla-specific error codes
    // The error codes below are advertised to the drivers during connection
    // handshake using the protocol extension negotiation, and are only
    // enabled if the drivers explicitly enable them. Therefore it's perfectly
    // fine to change them in case some new error codes are introduced
    // in Cassandra.
    // NOTE TO DRIVER DEVELOPERS: These constants must not be relied upon,
    // they must be learned from protocol extensions instead.
    RATE_LIMIT_ERROR = 0xF000
};

inline auto format_as(exception_code ec) {
    return fmt::underlying(ec);
}

const std::unordered_map<exception_code, sstring>& exception_map();

class cassandra_exception : public std::exception {
private:
    exception_code _code;
    sstring _msg;
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

    unavailable_exception(db::consistency_level cl, int32_t required, int32_t alive) noexcept;
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

// Base class for various request timeout exceptions
class request_timeout_exception : public cassandra_exception {
public:
    request_timeout_exception(exception_code code, sstring msg) : cassandra_exception(code, std::move(msg)) {}
};

// Timeout during read/write - didn't receive enough responses from remote nodes to fulfill the consistency requirement.
class read_write_timeout_exception : public request_timeout_exception {
public:
    db::consistency_level consistency;
    int32_t received;
    int32_t block_for;

    read_write_timeout_exception(exception_code code, const sstring& ks, const sstring& cf, db::consistency_level consistency, int32_t received, int32_t block_for) noexcept;
    read_write_timeout_exception(exception_code code, sstring message, db::consistency_level consistency, int32_t received, int32_t block_for) noexcept;
};

class read_timeout_exception : public read_write_timeout_exception {
public:
    bool data_present;

    read_timeout_exception(const sstring& ks, const sstring& cf, db::consistency_level consistency, int32_t received, int32_t block_for, bool data_present) noexcept
        : read_write_timeout_exception{exception_code::READ_TIMEOUT, ks, cf, consistency, received, block_for}
        , data_present{data_present}
    { }

    read_timeout_exception(sstring message, db::consistency_level consistency, int32_t received, int32_t block_for, bool data_present) noexcept
        : read_write_timeout_exception{exception_code::READ_TIMEOUT, std::move(message), consistency, received, block_for}
        , data_present{data_present}
    { }
};

struct mutation_write_timeout_exception : public read_write_timeout_exception {
    db::write_type type;
    mutation_write_timeout_exception(const sstring& ks, const sstring& cf, db::consistency_level consistency, int32_t received, int32_t block_for, db::write_type type) noexcept :
        read_write_timeout_exception(exception_code::WRITE_TIMEOUT, ks, cf, consistency, received, block_for)
        , type{std::move(type)}
    { }
    mutation_write_timeout_exception(sstring message, db::consistency_level consistency, int32_t received, int32_t block_for, db::write_type type) noexcept :
        read_write_timeout_exception(exception_code::WRITE_TIMEOUT, std::move(message), consistency, received, block_for)
        , type{std::move(type)}
    { }
};

// The reason a replica failed to execute a request.
// The values are the <failure_code> values of the <reason_map> in the
// Read_failure and Write_failure errors of the CQL native protocol v5.
// Values starting at 0xF000 are Scylla-specific. They are advertised to the
// drivers by the SCYLLA_FAILURE_REASON_MAP protocol extension.
enum class request_failure_reason : uint16_t {
    UNKNOWN                  = 0x0000,
    READ_TOO_MANY_TOMBSTONES = 0x0001,
    TIMEOUT                  = 0x0002,
    INCOMPATIBLE_SCHEMA      = 0x0003,
    READ_SIZE                = 0x0004,
    NODE_DOWN                = 0x0005,
    INDEX_NOT_AVAILABLE      = 0x0006,
    READ_TOO_MANY_INDEXES    = 0x0007,
    NOT_CMS                  = 0x0008,
    INVALID_ROUTING          = 0x0009,
    COORDINATOR_BEHIND       = 0x000A,

    // Scylla-specific failure reasons.
    // NOTE TO DRIVER DEVELOPERS: These constants must not be relied upon,
    // they must be learned from protocol extensions instead.
    SCYLLA_RATE_LIMITED              = 0xF000,
    SCYLLA_LARGE_DATA_REJECTED       = 0xF001,
    SCYLLA_CRITICAL_DISK_UTILIZATION = 0xF002,
    SCYLLA_ABORTED                   = 0xF003,
    SCYLLA_DISCONNECTED              = 0xF004,
};

struct replica_failure {
    locator::host_id replica;
    request_failure_reason reason;
};

// The replicas that failed a request, each with the reason of its failure.
// Keeps one entry per replica, the one added first, in the order of addition.
// The entries are allocated on the first addition, so an empty map takes
// the size of a pointer.
// The map is informational: an entry that cannot be allocated is dropped.
class replica_failure_map {
    using entries = utils::small_vector<replica_failure, 3>;
    std::unique_ptr<entries> _entries;
public:
    replica_failure_map() noexcept = default;
    replica_failure_map(replica_failure_map&&) noexcept = default;
    replica_failure_map& operator=(replica_failure_map&&) noexcept = default;
    replica_failure_map(const replica_failure_map& o);
    replica_failure_map& operator=(const replica_failure_map& o);

    void add(locator::host_id replica, request_failure_reason reason) noexcept;

    bool empty() const noexcept { return !_entries || _entries->empty(); }
    size_t size() const noexcept { return _entries ? _entries->size() : 0; }
    const replica_failure* begin() const noexcept { return _entries ? _entries->begin() : nullptr; }
    const replica_failure* end() const noexcept { return _entries ? _entries->end() : nullptr; }
};

class request_failure_exception : public cassandra_exception {
public:
    db::consistency_level consistency;
    int32_t received;
    int32_t failures;
    int32_t block_for;
    replica_failure_map failed_replicas;

protected:
    request_failure_exception(exception_code code, const sstring& ks, const sstring& cf, db::consistency_level consistency_, int32_t received_, int32_t failures_, int32_t block_for_,
            replica_failure_map failed_replicas_ = {}) noexcept;

    request_failure_exception(exception_code code, sstring msg, db::consistency_level consistency_, int32_t received_, int32_t failures_, int32_t block_for_,
            replica_failure_map failed_replicas_ = {}) noexcept
        : cassandra_exception{code, std::move(msg)}
        , consistency{consistency_}
        , received{received_}
        , failures{failures_}
        , block_for{block_for_}
        , failed_replicas{std::move(failed_replicas_)}
    {}
};

struct mutation_write_failure_exception : public request_failure_exception {
    db::write_type type;
    mutation_write_failure_exception(const sstring& ks, const sstring& cf, db::consistency_level consistency_, int32_t received_, int32_t failures_, int32_t block_for_, db::write_type type_,
            replica_failure_map failed_replicas_ = {}) noexcept :
        request_failure_exception(exception_code::WRITE_FAILURE, ks, cf, consistency_, received_, failures_, block_for_, std::move(failed_replicas_))
        , type{std::move(type_)}
    { }

    mutation_write_failure_exception(sstring msg, db::consistency_level consistency_, int32_t received_, int32_t failures_, int32_t block_for_, db::write_type type_,
            replica_failure_map failed_replicas_ = {}) noexcept :
        request_failure_exception(exception_code::WRITE_FAILURE, std::move(msg), consistency_, received_, failures_, block_for_, std::move(failed_replicas_))
        , type{std::move(type_)}
    { }
};

struct read_failure_exception : public request_failure_exception {
    bool data_present;

    read_failure_exception(const sstring& ks, const sstring& cf, db::consistency_level consistency_, int32_t received_, int32_t failures_, int32_t block_for_, bool data_present_,
            replica_failure_map failed_replicas_ = {}) noexcept
        : request_failure_exception{exception_code::READ_FAILURE, ks, cf, consistency_, received_, failures_, block_for_, std::move(failed_replicas_)}
        , data_present{data_present_}
    { }

    read_failure_exception(sstring msg, db::consistency_level consistency_, int32_t received_, int32_t failures_, int32_t block_for_, bool data_present_,
            replica_failure_map failed_replicas_ = {}) noexcept
        : request_failure_exception{exception_code::READ_FAILURE, std::move(msg), consistency_, received_, failures_, block_for_, std::move(failed_replicas_)}
        , data_present{data_present_}
    { }
};

struct read_failure_exception_with_timeout : public read_failure_exception {
    const seastar::lowres_clock::time_point _timeout;
    read_timeout_exception _timeout_exception;

    read_failure_exception_with_timeout(const sstring& ks, const sstring& cf, db::consistency_level consistency_, int32_t received_, int32_t failures_, int32_t block_for_, bool data_present_, seastar::lowres_clock::time_point timeout_,
            replica_failure_map failed_replicas_ = {}) noexcept
        : read_failure_exception{ks, cf, consistency_, received_, failures_, block_for_, data_present_, std::move(failed_replicas_)}
        , _timeout(timeout_)
        , _timeout_exception(ks, cf, consistency_, received_, block_for_, data_present_)
    { }
};

struct overloaded_exception : public cassandra_exception {
    explicit overloaded_exception(size_t c) noexcept;
    explicit overloaded_exception(sstring msg) noexcept :
        cassandra_exception(exception_code::OVERLOADED, std::move(msg)) {}
};

struct rate_limit_exception : public cassandra_exception {
    db::operation_type op_type;
    bool rejected_by_coordinator;

    rate_limit_exception(const sstring& ks, const sstring& cf, db::operation_type op_type_, bool rejected_by_coordinator_) noexcept;
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

    prepared_query_not_found_exception(bytes id) noexcept;
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
    // Take ks_name and cf_name by rvalue reference so the public constructors
    // can compute the message from the original objects before
    // they transition to 'moved-from' state.
    already_exists_exception(sstring&& ks_name_, sstring&& cf_name_, sstring msg)
        : configuration_exception{exception_code::ALREADY_EXISTS, std::move(msg)}
        , ks_name{ks_name_}
        , cf_name{cf_name_}
    { }
public:
    already_exists_exception(sstring ks_name_, sstring cf_name_);
    already_exists_exception(sstring ks_name_);
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
    function_execution_exception(sstring func_name_, sstring detail, sstring ks_name_, std::vector<sstring> args_) noexcept;
};

} // namespace exceptions
