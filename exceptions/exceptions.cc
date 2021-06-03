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
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
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
