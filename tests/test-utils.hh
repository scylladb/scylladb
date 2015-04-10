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

#pragma once

#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>

#include "core/future.hh"
#include "test_runner.hh"

class seastar_test {
public:
    seastar_test();
    virtual ~seastar_test() {}
    virtual const char* get_test_file() = 0;
    virtual const char* get_name() = 0;
    virtual future<> run_test_case() = 0;
    void run();
};

#define SEASTAR_TEST_CASE(name) \
    struct name : public seastar_test { \
        const char* get_test_file() override { return __FILE__; } \
        const char* get_name() override { return #name; } \
        future<> run_test_case() override; \
    }; \
    static name name ## _instance; \
    future<> name::run_test_case()

