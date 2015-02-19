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
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef CONVERSIONS_CC_
#define CONVERSIONS_CC_

#include "conversions.hh"
#include "core/print.hh"
#include <boost/lexical_cast.hpp>

size_t parse_memory_size(std::string s) {
    size_t factor = 1;
    if (s.size()) {
        auto c = s[s.size() - 1];
        static std::string suffixes = "kMGT";
        auto pos = suffixes.find(c);
        if (pos == suffixes.npos) {
            throw std::runtime_error(sprint("Cannot parse memory size '%s'", s));
        }
        factor <<= (pos + 1) * 10;
        s = s.substr(0, s.size() - 1);
    }
    return boost::lexical_cast<size_t>(s) * factor;
}


#endif /* CONVERSIONS_CC_ */
