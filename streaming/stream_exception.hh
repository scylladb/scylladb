/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified by Cloudius Systems.
 * Copyright 2015 Cloudius Systems.
 */

#pragma once

#include "streaming/stream_state.hh"
#include <seastar/core/sstring.hh>
#include <exception>

namespace streaming {

class stream_exception : public std::exception {
public:
    stream_state state;
    sstring msg;
    stream_exception(stream_state s, sstring m)
        : state(std::move(s))
        , msg(std::move(m)) {
    }
    virtual const char* what() const noexcept override {
        return msg.c_str();
    }
};

} // namespace streaming
