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

#pragma once

#include "handlers.hh"
#include <functional>
#include "json/json_elements.hh"

namespace httpd {

/**
 * A request function is a lambda expression that gets only the request
 * as its parameter
 */
typedef std::function<sstring(const_req req)> request_function;

/**
 * A handle function is a lambda expression that gets request and reply
 */
typedef std::function<sstring(const_req req, reply&)> handle_function;

/**
 * A json request function is a lambda expression that gets only the request
 * as its parameter and return a json response.
 * Using the json response is done implicitly.
 */
typedef std::function<json::json_return_type(const_req req)> json_request_function;

/**
 * The function handler get a lambda expression in the constructor.
 * it will call that expression to get the result
 * This is suited for very simple handlers
 *
 */
class function_handler : public handler_base {
public:

    function_handler(const handle_function & f_handle, const sstring& type)
            : _f_handle(f_handle), _type(type) {
    }

    function_handler(const request_function & _handle, const sstring& type)
            : _f_handle([_handle](const_req req, reply& rep) {
                return _handle(req);
            }), _type(type) {
    }

    function_handler(const json_request_function& _handle)
            : _f_handle([_handle](const_req req, reply& rep) {
                json::json_return_type res = _handle(req);
                return res._res;
            }), _type("json") {

    }
    future<std::unique_ptr<reply>> handle(const sstring& path,
            std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        rep->_content += _f_handle(*req.get(), *rep.get());
        rep->done(_type);
        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
    }

protected:
    handle_function _f_handle;
    sstring _type;
};

}
