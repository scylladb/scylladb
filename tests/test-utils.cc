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

#include <thread>

#include "tests/test-utils.hh"
#include "core/future.hh"
#include "core/app-template.hh"

void seastar_test::run() {
    // HACK: please see https://github.com/cloudius-systems/seastar/issues/10
    BOOST_REQUIRE(true);

    // HACK: please see https://github.com/cloudius-systems/seastar/issues/10
    boost::program_options::variables_map()["dummy"];

    global_test_runner().run_sync([this] {
        return run_test_case();
    });
}

// We store a pointer because tests are registered from dynamic initializers,
// so we must ensure that 'tests' is initialized before any dynamic initializer.
// I use a primitive type, which is guaranteed to be initialized before any
// dynamic initializer and lazily allocate the factor.

static std::vector<seastar_test*>* tests;

seastar_test::seastar_test() {
    if (!tests) {
        tests = new std::vector<seastar_test*>();
    }
    tests->push_back(this);
}

bool init_unit_test_suite() {
    auto&& ts = boost::unit_test::framework::master_test_suite();
    ts.p_name.set(tests->size() ? (*tests)[0]->get_test_file() : "seastar-tests");

    for (seastar_test* test : *tests) {
        ts.add(boost::unit_test::make_test_case([test] { test->run(); }, test->get_name()), 0, 0);
    }

    global_test_runner().start(ts.argc, ts.argv);
    return true;
}

int main(int ac, char** av) {
    return ::boost::unit_test::unit_test_main(&init_unit_test_suite, ac, av);
}
