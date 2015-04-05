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

#include "json_path.hh"

namespace httpd {

using namespace std;

void path_description::set(routes& _routes, handler_base* handler) const {
    for (auto& i : mandatory_queryparams) {
        handler->mandatory(i);
    }

    if (params.size() == 0)
        _routes.put(operations.method, path, handler);
    else {
        match_rule* rule = new match_rule(handler);
        rule->add_str(path);
        for (auto i = params.begin(); i != params.end(); ++i) {
            rule->add_param(std::get<0>(*i), std::get<1>(*i));
        }
        _routes.add(rule, operations.method);
    }
}

void path_description::set(routes& _routes,
        const json_request_function& f) const {
    set(_routes, new function_handler(f));
}

void path_description::set(routes& _routes, const future_json_function& f) const {
    set(_routes, new function_handler(f));
}
path_description::path_description(const sstring& path, operation_type method,
        const sstring& nickname,
        const std::vector<std::pair<sstring, bool>>& path_parameters,
        const std::vector<sstring>& mandatory_params)
        : path(path), operations(method, nickname) {

    for (auto man : mandatory_params) {
        pushmandatory_param(man);
    }
    for (auto param : path_parameters) {
        params.push_back(param);
    }

}

}
