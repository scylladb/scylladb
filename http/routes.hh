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

#ifndef ROUTES_HH_
#define ROUTES_HH_

#include "matchrules.hh"
#include "handlers.hh"
#include "common.hh"
#include "reply.hh"

#include <boost/program_options/variables_map.hpp>
#include <unordered_map>
#include <vector>

namespace httpd {

/**
 * The url helps defining a route.
 */
class url {
public:
    /**
     * Move constructor
     */
    url(url&&) = default;

    /**
     * Construct with a url path as it's parameter
     * @param path the url path to be used
     */
    url(const sstring& path)
            : _path(path) {
    }

    /**
     * Adds a parameter that matches untill the end of the URL.
     * @param param the parmaeter name
     * @return the current url
     */
    url& remainder(const sstring& param) {
        this->_param = param;
        return *this;
    }

    sstring _path;
    sstring _param;
};

/**
 * routes object do the request dispatching according to the url.
 * It uses two decision mechanism exact match, if a url matches exactly
 * (an optional leading slash is permitted) it is choosen
 * If not, the matching rules are used.
 * matching rules are evaluated by their insertion order
 */
class routes {
public:
    /**
     * The destructor deletes the match rules and handlers
     */
    ~routes();

    /**
     * adding a handler as an exact match
     * @param url the url to match (note that url should start with /)
     * @param handler the desire handler
     * @return it self
     */
    routes& put(operation_type type, const sstring& url,
            handler_base* handler) {
        _map[type][url] = handler;
        return *this;
    }

    /**
     * add a rule to be used.
     * rules are search only if an exact match was not found.
     * rules are search by the order they were added.
     * First in higher priority
     * @param rule a rule to add
     * @param type the operation type
     * @return it self
     */
    routes& add(match_rule* rule, operation_type type = GET) {
        _rules[type].push_back(rule);
        return *this;
    }

    /**
     * Add a url match to a handler:
     * Example  routes.add(GET, url("/api").remainder("path"), handler);
     * @param type
     * @param url
     * @param handler
     * @return
     */
    routes& add(operation_type type, const url& url, handler_base* handler);

    /**
     * the main entry point.
     * the general handler calls this method with the request
     * the method takes the headers from the request and find the
     * right handler.
     * It then call the handler with the parameters (if they exists) found in the url
     * @param path the url path found
     * @param req the http request
     * @param rep the http reply
     */
    void handle(const sstring& path, httpd::request& req, httpd::reply& rep);

private:

    /**
     * Search and return an exact match
     * @param url the request url
     * @return the handler if exists or nullptr if it does not
     */
    handler_base* get_exact_match(operation_type type, const sstring& url) {
        return (_map[type].find(url) == _map[type].end()) ?
                nullptr : _map[type][url];
    }

    /**
     * Search and return a handler by the operation type and url
     * @param type the http operation type
     * @param url the request url
     * @param params a parameter object that will be filled during the match
     * @return a handler based on the type/url match
     */
    handler_base* get_handler(operation_type type, const sstring& url,
            parameters& params);

    /**
     * Normalize the url to remove the last / if exists
     * and get the parameter part
     * @param url the full url path
     * @param param_part will hold the string with the parameters
     * @return the url from the request without the last /
     */
    sstring normalize_url(const sstring& url);

    std::unordered_map<sstring, handler_base*> _map[NUM_OPERATION];
    std::vector<match_rule*> _rules[NUM_OPERATION];
};

/**
 * A helper function that check if a parameter is found in the params object
 * if it does not the function would throw a parameter not found exception
 * @param params the parameters object
 * @param param the parameter to look for
 */
void verify_param(const httpd::request& req, const sstring& param);

}

#endif /* ROUTES_HH_ */
