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

#include "matcher.hh"

#include <iostream>

namespace httpd {

using namespace std;

/**
 * Search for the end of the url parameter.
 * @param url the url to search
 * @param ind the position in the url
 * @param entire_path when set to true, take all the reminaing url
 * when set to false, search for the next slash
 * @return the position in the url of the end of the parameter
 */
static size_t find_end_param(const sstring& url, size_t ind, bool entire_path) {
    size_t pos = (entire_path) ? url.length() : url.find('/', ind + 1);
    if (pos == sstring::npos) {
        return url.length();
    }
    return pos;
}

size_t param_matcher::match(const sstring& url, size_t ind, parameters& param) {
    size_t last = find_end_param(url, ind, _entire_path);
    if (last == ind) {
        /*
         * empty parameter allows only for the case of entire_path
         */
        if (_entire_path) {
            param.set(_name, "");
            return ind;
        }
        return sstring::npos;
    }
    param.set(_name, url.substr(ind, last - ind));
    return last;
}

size_t str_matcher::match(const sstring& url, size_t ind, parameters& param) {
    if (url.length() >= _len + ind && (url.find(_cmp, ind) == ind)
            && (url.length() == _len + ind || url.at(_len + ind) == '/')) {
        return _len + ind;
    }
    return sstring::npos;
}

}
