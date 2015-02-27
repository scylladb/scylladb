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

#ifndef COMMON_HH_
#define COMMON_HH_

#include <unordered_map>
#include "core/sstring.hh"

namespace httpd {

typedef std::unordered_map<sstring, sstring> parameters;

enum operation_type {
    GET, POST, PUT, DELETE, NUM_OPERATION
};

/**
 * Translate the string command to operation type
 * @param type the string "GET" or "POST"
 * @return the operation_type
 */
operation_type str2type(const sstring& type);

}

#endif /* COMMON_HH_ */
