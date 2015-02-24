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

// This file was modified from boost http example
//
// reply.hpp
// ~~~~~~~~~
//
// Copyright (c) 2003-2013 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#pragma once

#include "core/sstring.hh"
#include <unordered_map>
#include "http/mime_types.hh"

namespace httpd {
/**
 * A reply to be sent to a client.
 */
struct reply {
    /**
     * The status of the reply.
     */
    enum class status_type {
        ok = 200, //!< ok
        created = 201, //!< created
        accepted = 202, //!< accepted
        no_content = 204, //!< no_content
        multiple_choices = 300, //!< multiple_choices
        moved_permanently = 301, //!< moved_permanently
        moved_temporarily = 302, //!< moved_temporarily
        not_modified = 304, //!< not_modified
        bad_request = 400, //!< bad_request
        unauthorized = 401, //!< unauthorized
        forbidden = 403, //!< forbidden
        not_found = 404, //!< not_found
        internal_server_error = 500, //!< internal_server_error
        not_implemented = 501, //!< not_implemented
        bad_gateway = 502, //!< bad_gateway
        service_unavailable = 503  //!< service_unavailable
    } _status;

    /**
     * The headers to be included in the reply.
     */
    std::unordered_map<sstring, sstring> _headers;

    sstring _version;
    /**
     * The content to be sent in the reply.
     */
    sstring _content;

    sstring _response_line;
    reply()
            : _status(status_type::ok) {
    }

    reply& add_header(const sstring& h, const sstring& value) {
        _headers[h] = value;
        return *this;
    }

    reply& set_version(const sstring& version) {
        _version = version;
        return *this;
    }

    reply& set_status(status_type status, const sstring& content = "") {
        _status = status;
        if (content != "") {
            _content = content;
        }
        return *this;
    }

    /**
     * Set the content type mime type.
     * Used when the mime type is known.
     * For most cases, use the set_content_type
     */
    reply& set_mime_type(const sstring& mime) {
        _headers["Content-Type"] = mime;
        return *this;
    }

    /**
     * Set the content type mime type according to the file extension
     * that would have been used if it was a file: e.g. html, txt, json etc'
     */
    reply& set_content_type(const sstring& content_type = "html") {
        set_mime_type(httpd::mime_types::extension_to_type(content_type));
        return *this;
    }

    reply& done(const sstring& content_type) {
        return set_content_type(content_type).done();
    }
    /**
     * Done should be called before using the reply.
     * It would set the response line
     */
    reply& done() {
        _response_line = response_line();
        return *this;
    }
    sstring response_line();
};

} // namespace httpd
