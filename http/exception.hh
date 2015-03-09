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

#ifndef EXCEPTION_HH_
#define EXCEPTION_HH_

#include "reply.hh"
#include "json/json_elements.hh"

namespace httpd {

/**
 * The base_exception is a base for all http exception.
 * It contains a message that will be return as the message content
 * and a status that will be return as a status code.
 */
class base_exception : public std::exception {
public:
    base_exception(const std::string& msg, reply::status_type status)
            : _msg(msg), _status(status) {
    }

    virtual const char* what() const throw () {
        return _msg.c_str();
    }

    reply::status_type status() const {
        return _status;
    }

    virtual const std::string& str() const {
        return _msg;
    }
private:
    std::string _msg;
    reply::status_type _status;

};

/**
 * Throwing this exception will result in a redirect to the given url
 */
class redirect_exception : public base_exception {
public:
    redirect_exception(const std::string& url)
            : base_exception("", reply::status_type::moved_permanently), url(
                    url) {
    }
    std::string url;
};

/**
 * Throwing this exception will result in a 404 not found result
 */
class not_found_exception : public base_exception {
public:
    not_found_exception(const std::string& msg = "Not found")
            : base_exception(msg, reply::status_type::not_found) {
    }
};

/**
 * Throwing this exception will result in a 400 bad request result
 */

class bad_request_exception : public base_exception {
public:
    bad_request_exception(const std::string& msg)
            : base_exception(msg, reply::status_type::bad_request) {
    }
};

class bad_param_exception : public bad_request_exception {
public:
    bad_param_exception(const std::string& msg)
            : bad_request_exception(msg) {
    }
};

class missing_param_exception : public bad_request_exception {
public:
    missing_param_exception(const std::string& param)
            : bad_request_exception(
                    std::string("Missing mandatory parameter '") + param + "'") {
    }
};

class server_error_exception : public base_exception {
public:
    server_error_exception(const std::string& msg)
            : base_exception(msg, reply::status_type::internal_server_error) {
    }
};

class json_exception : public json::json_base {
public:
    json::json_element<std::string> _msg;
    json::json_element<int> _code;
    void register_params() {
        add(&_msg, "message");
        add(&_code, "code");
    }

    json_exception(const base_exception & e) {
        set(e.str(), e.status());
    }

    json_exception(const std::exception& e) {
        set(e.what(), reply::status_type::internal_server_error);
    }
private:
    void set(const std::string& msg, reply::status_type code) {
        register_params();
        _msg = msg;
        _code = (int) code;
    }
};

}

#endif /* EXCEPTION_HH_ */
