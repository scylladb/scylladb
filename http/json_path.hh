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

#ifndef JSON_PATH_HH_
#define JSON_PATH_HH_

#include <vector>
#include <unordered_map>
#include <tuple>
#include "common.hh"
#include "core/sstring.hh"
#include "routes.hh"
#include "function_handlers.hh"

namespace httpd {

/**
 * A json_operation contain a method and a nickname.
 * operation are associated to a path, that can
 * have multiple methods
 */
struct json_operation {
    /**
     * default constructor
     */
    json_operation()
            : method(GET) {
    }

    /**
     * Construct with assignment
     * @param method the http method type
     * @param nickname the http nickname
     */
    json_operation(operation_type method, const sstring& nickname)
            : method(method), nickname(nickname) {
    }

    operation_type method;
    sstring nickname;

};

/**
 * path description holds the path in the system.
 * It maps a nickname to an operation, which allows
 * defining the operation (path and method) by its
 * nickname.
 *
 * the description are taken from the json swagger
 * definition file, during auto code generation in the
 * compilation.
 */
struct path_description {
    /**
     * default empty constructor
     */
    path_description() = default;

    /**
     * constructor for path with parameters
     * The constructor is used by
     * @param path the url path
     * @param method the http method
     * @param nickname the nickname
     */
    path_description(const sstring& path, operation_type method,
            const sstring& nickname,
            const std::vector<std::pair<sstring, bool>>& path_parameters,
            const std::vector<sstring>& mandatory_params);

    /**
     * Add a parameter to the path definition
     * for example, if the url should match /file/{path}
     * The constructor would be followed by a call to
     * pushparam("path")
     *
     * @param param the name of the parameters, this name will
     * be used by the handler to identify the parameters.
     * A name can appear at most once in a description
     * @param all_path when set to true the parameter will assume to match
     * until the end of the url.
     * This is useful for situation like file path with
     * a rule like /file/{path} and a url /file/etc/hosts.
     * path should be equal to /ets/hosts and not only /etc
     * @return the current path description
     */
    path_description* pushparam(const sstring& param,
    bool all_path = false) {
        params.push_back( { param, all_path });
        return this;
    }

    /**
     * adds a mandatory query parameter to the path
     * this parameter will be check before calling a handler
     * @param param the parameter to head
     * @return a pointer to the current path description
     */
    path_description* pushmandatory_param(const sstring& param) {
        mandatory_queryparams.push_back(param);
        return this;
    }

    std::vector<std::pair<sstring, bool>> params;
    sstring path;
    json_operation operations;

    std::vector<sstring> mandatory_queryparams;

    void set(routes& _routes, handler_base* handler) const;

    void set(routes& _routes, const json_request_function& f) const;

    void set(routes& _routes, const future_json_function& f) const;
};

}
#endif /* JSON_PATH_HH_ */
