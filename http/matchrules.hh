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

#ifndef MATCH_RULES_HH_
#define MATCH_RULES_HH_

#include "handlers.hh"
#include "matcher.hh"
#include "common.hh"

#include "core/sstring.hh"
#include <vector>

namespace httpd {

/**
 * match_rule check if a url matches criteria, that can contains
 * parameters.
 * the routes object would call the get method with a url and if
 * it matches, the method will return a handler
 * during the matching process, the method fill the parameters object.
 */
class match_rule {
public:
    /**
     * The destructor deletes matchers.
     */
    ~match_rule() {
        for (auto m : _match_list) {
            delete m;
        }
        delete _handler;
    }

    /**
     * Constructor with a handler
     * @param handler the handler to return when this match rule is met
     */
    explicit match_rule(handler_base* handler)
            : _handler(handler) {
    }

    /**
     * Check if url match the rule and return a handler if it does
     * @param url a url to compare against the rule
     * @param params the parameters object, matches parameters will fill
     * the object during the matching process
     * @return a handler if there is a full match or nullptr if not
     */
    handler_base* get(const sstring& url, parameters& params) {
        size_t ind = 0;
        for (unsigned int i = 0; i < _match_list.size(); i++) {
            ind = _match_list.at(i)->match(url, ind, params);
            if (ind == sstring::npos) {
                return nullptr;
            }
        }
        return (ind + 1 >= url.length()) ? _handler : nullptr;
    }

    /**
     * Add a matcher to the rule
     * @param match the matcher to add
     * @return this
     */
    match_rule& add_matcher(matcher* match) {
        _match_list.push_back(match);
        return *this;
    }

    /**
     * Add a static url matcher
     * @param str the string to search for
     * @return this
     */
    match_rule& add_str(const sstring& str) {
        add_matcher(new str_matcher(str));
        return *this;
    }

    /**
     * add a parameter matcher to the rule
     * @param str the parameter name
     * @param fullpath when set to true, parameter will included all the
     * remaining url until its end
     * @return this
     */
    match_rule& add_param(const sstring& str, bool fullpath = false) {
        add_matcher(new param_matcher(str, fullpath));
        return *this;
    }

private:
    std::vector<matcher*> _match_list;
    handler_base* _handler;
};

}

#endif /* MATCH_RULES_HH_ */
