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

#ifndef EXCEPTIONS_HH
#define EXCEPTIONS_HH

#include <stdexcept>
#include "core/sstring.hh"

namespace exceptions {

class invalid_request_exception : public std::logic_error {
public:
    invalid_request_exception(std::string cause)
        : logic_error(cause)
    { }
};

class keyspace_not_defined_exception : public invalid_request_exception {
public:
    keyspace_not_defined_exception(std::string cause)
        : invalid_request_exception(cause)
    { }
};

class marshal_exception : public std::logic_error {
public:
    marshal_exception(std::string cause)
        : logic_error(cause)
    { }
};

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

class transport_exception {
public:
    virtual exception_code code() const = 0;
    virtual sstring get_message() const = 0;
};

class cassandra_exception : public std::exception, public transport_exception {
private:
    exception_code _code;
    sstring _msg;
public:
    cassandra_exception(exception_code code, sstring msg)
        : _code(code)
        , _msg(std::move(msg))
    { }
    virtual const char* what() const noexcept override { return _msg.begin(); }
    virtual exception_code code() const override { return _code; }
    virtual sstring get_message() const override { return what(); }
};

class request_validation_exception : public cassandra_exception {
public:
    using cassandra_exception::cassandra_exception;
};

class syntax_exception : public request_validation_exception {
public:
    syntax_exception(sstring msg)
        : request_validation_exception(exception_code::SYNTAX_ERROR, std::move(msg))
    { }
};

class recognition_exception : public std::runtime_error {
public:
    recognition_exception(const std::string& msg) : std::runtime_error(msg) {};
};

class unsupported_operation_exception : public std::runtime_error {
public:
    unsupported_operation_exception() : std::runtime_error("unsupported operation") {}
};

}

#endif
