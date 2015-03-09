/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright 2015 Cloudius Systems
 */

//
// response.cpp
// ~~~~~~~~~
//
// Copyright (c) 2003-2013 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#include "reply.hh"

namespace httpd {

namespace status_strings {

const sstring ok = " 200 OK\r\n";
const sstring created = " 201 Created\r\n";
const sstring accepted = " 202 Accepted\r\n";
const sstring no_content = " 204 No Content\r\n";
const sstring multiple_choices = " 300 Multiple Choices\r\n";
const sstring moved_permanently = " 301 Moved Permanently\r\n";
const sstring moved_temporarily = " 302 Moved Temporarily\r\n";
const sstring not_modified = " 304 Not Modified\r\n";
const sstring bad_request = " 400 Bad Request\r\n";
const sstring unauthorized = " 401 Unauthorized\r\n";
const sstring forbidden = " 403 Forbidden\r\n";
const sstring not_found = " 404 Not Found\r\n";
const sstring internal_server_error = " 500 Internal Server Error\r\n";
const sstring not_implemented = " 501 Not Implemented\r\n";
const sstring bad_gateway = " 502 Bad Gateway\r\n";
const sstring service_unavailable = " 503 Service Unavailable\r\n";

static const sstring& to_string(reply::status_type status) {
    switch (status) {
    case reply::status_type::ok:
        return ok;
    case reply::status_type::created:
        return created;
    case reply::status_type::accepted:
        return accepted;
    case reply::status_type::no_content:
        return no_content;
    case reply::status_type::multiple_choices:
        return multiple_choices;
    case reply::status_type::moved_permanently:
        return moved_permanently;
    case reply::status_type::moved_temporarily:
        return moved_temporarily;
    case reply::status_type::not_modified:
        return not_modified;
    case reply::status_type::bad_request:
        return bad_request;
    case reply::status_type::unauthorized:
        return unauthorized;
    case reply::status_type::forbidden:
        return forbidden;
    case reply::status_type::not_found:
        return not_found;
    case reply::status_type::internal_server_error:
        return internal_server_error;
    case reply::status_type::not_implemented:
        return not_implemented;
    case reply::status_type::bad_gateway:
        return bad_gateway;
    case reply::status_type::service_unavailable:
        return service_unavailable;
    default:
        return internal_server_error;
    }
}
} // namespace status_strings

namespace misc_strings {

const char name_value_separator[] = { ':', ' ' };
const char crlf[] = { '\r', '\n' };

} // namespace misc_strings

sstring reply::response_line() {
    return "HTTP/" + _version + status_strings::to_string(_status);
}

} // namespace server
