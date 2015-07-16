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
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "net/api.hh"
#include <stdexcept>
#include <string>

namespace rpc {

struct stats {
    using counter_type = uint64_t;
    counter_type replied = 0;
    counter_type pending = 0;
    counter_type exception_received = 0;
    counter_type sent_messages = 0;
    counter_type wait_reply = 0;
};


struct client_info {
    socket_address addr;
};

class error : public std::runtime_error {
public:
    error(const std::string& msg) : std::runtime_error(msg) {}
};

class closed_error : public error {
public:
    closed_error() : error("connection is closed") {}
};

struct no_wait_type {};

// return this from a callback if client does not want to waiting for a reply
extern no_wait_type no_wait;

} // namespace rpc
