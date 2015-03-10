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
// request.hpp
// ~~~~~~~~~~~
//
// Copyright (c) 2003-2013 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#ifndef HTTP_REQUEST_HPP
#define HTTP_REQUEST_HPP

#include "core/sstring.hh"
#include <string>
#include <vector>
#include <strings.h>
#include "common.hh"

namespace httpd {
class connection;

/**
 * A request received from a client.
 */
struct request {
    enum class ctclass
        : char {
            other, multipart, app_x_www_urlencoded,
    };

    sstring _method;
    sstring _url;
    sstring _version;
    int http_version_major;
    int http_version_minor;
    ctclass content_type_class;
    size_t content_length = 0;
    std::unordered_map<sstring, sstring> _headers;
    std::unordered_map<sstring, sstring> query_parameters;
    connection* connection_ptr;
    parameters param;
    sstring content;
    sstring protocol_name;

    /**
     * Search for the first header of a given name
     * @param name the header name
     * @return a pointer to the header value, if it exists or empty string
     */
    sstring get_header(const sstring& name) const {
        auto res = _headers.find(name);
        if (res == _headers.end()) {
            return "";
        }
        return res->second;
    }

    /**
     * Search for the first header of a given name
     * @param name the header name
     * @return a pointer to the header value, if it exists or empty string
     */
    sstring get_query_param(const sstring& name) const {
        auto res = query_parameters.find(name);
        if (res == query_parameters.end()) {
            return "";
        }
        return res->second;
    }

    /**
     * Get the request protocol name. Can be either "http" or "https".
     */
    sstring get_protocol_name() const {
        return protocol_name;
    }

    /**
     * Get the request url.
     * @return the request url
     */
    sstring get_url() const {
        return get_protocol_name() + "://" + get_header("Host") + _url;
    }

    bool is_multi_part() const {
        return content_type_class == ctclass::multipart;
    }

    bool is_form_post() const {
        return content_type_class == ctclass::app_x_www_urlencoded;
    }

};

} // namespace httpd

#endif // HTTP_REQUEST_HPP
