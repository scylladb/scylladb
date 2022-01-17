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

#include <sstream>

#include "exceptions.hh"
#include "log.hh"

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
        {exception_code::UNAUTHORIZED, "unathorized"},
        {exception_code::INVALID, "invalid"},
        {exception_code::CONFIG_ERROR, "config_error"},
        {exception_code::ALREADY_EXISTS, "already_exists"},
        {exception_code::UNPREPARED, "unprepared"}
    };
    return map;
}

}
