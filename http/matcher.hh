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

#ifndef MATCHER_HH_
#define MATCHER_HH_

#include "common.hh"

#include "core/sstring.hh"

namespace httpd {

/**
 * a base class for the url matching.
 * Each implementation check if the given url matches a criteria
 */
class matcher {
public:

    virtual ~matcher() = default;

    /**
     * check if the given url matches the rule
     * @param url the url to check
     * @param ind the position to start from
     * @param fill the parameters hash
     * @return the end of of the matched part, or sstring::npos if not matched
     */
    virtual size_t match(const sstring& url, size_t ind, parameters& param) = 0;
};

/**
 * Check if the url match a parameter and fill the parameters object
 *
 * Note that a non empty url will always return true with the parameters
 * object filled
 *
 * Assume that the rule is /file/{path}/ and the param_matcher identify
 * the /{path}
 *
 * For all non empty values, match will return true.
 * If the entire url is /file/etc/hosts, and the part that is passed to
 * param_matcher is /etc/hosts, if entire_path is true, the match will be
 * '/etc/hosts' If entire_path is false, the match will be '/etc'
 */
class param_matcher : public matcher {
public:
    /**
     * Constructor
     * @param name the name of the parameter, will be used as the key
     * in the parameters object
     * @param entire_path when set to true, the matched parameters will
     * include all the remaining url until the end of it.
     * when set to false the match will terminate at the next slash
     */
    explicit param_matcher(const sstring& name, bool entire_path = false)
            : _name(name), _entire_path(entire_path) {
    }

    virtual size_t match(const sstring& url, size_t ind, parameters& param)
            override;
private:
    sstring _name;
    bool _entire_path;
};

/**
 * Check if the url match a predefine string.
 *
 * When parsing a match rule such as '/file/{path}' the str_match would parse
 * the '/file' part
 */
class str_matcher : public matcher {
public:
    /**
     * Constructor
     * @param cmp the string to match
     */
    explicit str_matcher(const sstring& cmp)
            : _cmp(cmp), _len(cmp.size()) {
    }

    virtual size_t match(const sstring& url, size_t ind, parameters& param)
            override;
private:
    sstring _cmp;
    unsigned _len;
};

}

#endif /* MATCHER_HH_ */
