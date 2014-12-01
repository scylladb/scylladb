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

using namespace boost::unit_test;

class seastar_test {
public:
    seastar_test();
    virtual ~seastar_test() {}
    virtual const char* get_name() = 0;
    virtual future<> run_test_case() = 0;

    void run() {
        posix_thread t([this] {
            namespace bpo = boost::program_options;
            boost::program_options::variables_map configuration;
            auto opts = reactor::get_options_description();
            bpo::store(bpo::command_line_parser(0, nullptr).options(opts).run(), configuration);
            engine.configure(configuration);
            engine.when_started().then([this] {
                return run_test_case();
            }).rescue([] (auto get) {
                try {
                    get();
                    engine.exit(0);
                } catch (...) {
                    std::terminate();
                }
            });
            engine.run();
        });
        t.join();
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
