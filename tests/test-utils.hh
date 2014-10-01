/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef _TEST_UTILS_HH
#define _TEST_UTILS_HH

#include <iostream>
#include <core/future.hh>

#define BUG() do { \
        std::cerr << "ERROR @ " << __FILE__ << ":" << __LINE__ << std::endl; \
        throw std::runtime_error("test failed"); \
    } while (0)

#define OK() { \
        std::cerr << "OK @ " << __FILE__ << ":" << __LINE__ << std::endl; \
    } while (0)

static inline
void run_tests(future<> tests_done) {
    tests_done.rescue([] (auto get) {
        try {
            get();
            exit(0);
        } catch(...) {
            std::terminate();
        }
    });
}

#endif
