/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#pragma once

#include "db/consistency_level_type.hh"
#include <stdexcept>
#include "core/sstring.hh"
#include "core/print.hh"
#include "bytes.hh"

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

    // 2xx: problem validating the request
    SYNTAX_ERROR    = 0x2000,
    UNAUTHORIZED    = 0x2100,
    INVALID         = 0x2200,
    CONFIG_ERROR    = 0x2300,
    ALREADY_EXISTS  = 0x2400,
    UNPREPARED      = 0x2500
};

class cassandra_exception : public std::exception {
private:
    exception_code _code;
    sstring _msg;
public:
    cassandra_exception(exception_code code, sstring msg)
        : _code(code)
        , _msg(std::move(msg))
    { }
    virtual const char* what() const noexcept override { return _msg.begin(); }
    exception_code code() const { return _code; }
    sstring get_message() const { return what(); }
};

struct unavailable_exception : exceptions::cassandra_exception {
    db::consistency_level consistency;
    int32_t required;
    int32_t alive;

    unavailable_exception(db::consistency_level cl, int32_t required, int32_t alive)
        : exceptions::cassandra_exception(exceptions::exception_code::UNAVAILABLE, sprint("Cannot achieve consistency level for cl %s. Requires %ld, alive %ld", cl, required, alive))
        , consistency(cl)
        , required(required)
        , alive(alive)
    {}
};

class request_timeout_exception : public cassandra_exception {
public:
    db::consistency_level consistency;
    int32_t received;
    int32_t block_for;

    request_timeout_exception(exception_code code, db::consistency_level consistency, int32_t received, int32_t block_for)
        : cassandra_exception{code, sprint("Operation timed out - received only %d responses.", received)}
        , consistency{consistency}
        , received{received}
        , block_for{block_for}
    { }
};

class read_timeout_exception : public request_timeout_exception {
public:
    bool data_present;

    read_timeout_exception(db::consistency_level consistency, int32_t received, int32_t block_for, bool data_present)
        : request_timeout_exception{exception_code::READ_TIMEOUT, consistency, received, block_for}
        , data_present{data_present}
    { }
};

class request_validation_exception : public cassandra_exception {
public:
    using cassandra_exception::cassandra_exception;
};

class invalid_request_exception : public request_validation_exception {
public:
    invalid_request_exception(sstring cause)
        : request_validation_exception(exception_code::INVALID, cause)
    { }
};

class keyspace_not_defined_exception : public invalid_request_exception {
public:
    keyspace_not_defined_exception(std::string cause)
        : invalid_request_exception(cause)
    { }
};

class prepared_query_not_found_exception : public request_validation_exception {
public:
    prepared_query_not_found_exception(bytes id)
        : request_validation_exception{exception_code::UNPREPARED, std::move(make_message(id))}
    { }
private:
    static sstring make_message(bytes id) {
        std::stringstream msg;
        msg << "No prepared statement with ID " << id << " found.";
        return msg.str();
    }
};

class syntax_exception : public request_validation_exception {
public:
    syntax_exception(sstring msg)
        : request_validation_exception(exception_code::SYNTAX_ERROR, std::move(msg))
    { }
};

class configuration_exception : public request_validation_exception {
public:
    configuration_exception(sstring msg)
        : request_validation_exception{exception_code::CONFIG_ERROR, std::move(msg)}
    { }

    configuration_exception(exception_code code, sstring msg)
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
        : already_exists_exception{ks_name_, cf_name_, sprint("Cannot add already existing table \"%s\" to keyspace \"%s\"", cf_name_, ks_name_)}
    { }

    already_exists_exception(sstring ks_name_)
        : already_exists_exception{ks_name_, "", sprint("Cannot add existing keyspace \"%s\"", ks_name_)}
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

}
