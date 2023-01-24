/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

/* That's to define a new entry point that process scylla tests specific options */
#undef SEASTAR_TESTING_MAIN

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/testing/entry_point.hh>
#include "test/lib/scylla_tests_cmdline_options.hh"

int main(int argc, char** argv) {
    scylla_tests_cmdline_options_processor processor;
    auto [new_argc, new_argv] = processor.process_cmdline_options(argc, argv);
    return seastar::testing::entry_point(new_argc, new_argv);
}
