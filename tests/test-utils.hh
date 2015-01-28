/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef _TEST_UTILS_HH
#define _TEST_UTILS_HH

#include <iostream>
#include <boost/test/included/unit_test.hpp>
#include "core/future.hh"
#include "core/reactor.hh"
#include "core/app-template.hh"
#include "test_runner.hh"

using namespace boost::unit_test;

class seastar_test {
public:
    seastar_test();
    virtual ~seastar_test() {}
    virtual const char* get_name() = 0;
    virtual future<> run_test_case() = 0;

    void run() {
        test_runner::launch_or_get([] {
            // HACK: please see https://github.com/cloudius-systems/seastar/issues/10
            BOOST_REQUIRE(true);

            // HACK: please see https://github.com/cloudius-systems/seastar/issues/10
            boost::program_options::variables_map()["dummy"];
        }).run_sync([this] {
            return run_test_case();
        });
    }
};

static std::vector<seastar_test*> tests;

seastar_test::seastar_test() {
    tests.push_back(this);
}

test_suite* init_unit_test_suite(int argc, char* argv[]) {
    test_suite* ts = BOOST_TEST_SUITE("seastar-tests");
    for (seastar_test* test : tests) {
        ts->add(boost::unit_test::make_test_case([test] { test->run(); }, test->get_name()));
    }
    return ts;
}

#define SEASTAR_TEST_CASE(name) \
    struct name : public seastar_test { \
        const char* get_name() override { return #name; } \
        future<> run_test_case() override; \
    }; \
    static name name ## _instance; \
    future<> name::run_test_case()

#endif
