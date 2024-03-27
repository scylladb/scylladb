/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

// Test Raft library with many candidates
//
// Using slower but precise clock

#include "replication.hh"
#include "utils/to_string.hh"

#ifdef SEASTAR_DEBUG
// Increase tick time to allow debug to process messages
const auto tick_delay = 200ms;
#else
const auto tick_delay = 100ms;
#endif

SEASTAR_THREAD_TEST_CASE(test_many_100) {
    replication_test<steady_clock_type>(
        {.nodes = 100, .total_values = 10,
         .updates = {entries{1},
                     isolate{0},    // drop leader, free election
                     entries{2},
                     }}
    , true, tick_delay,
    rpc_config{ .network_delay = 20ms, .local_delay = 1ms });
}

#ifndef SEASTAR_DEBUG
SEASTAR_THREAD_TEST_CASE(test_many_400) {
    replication_test<steady_clock_type>(
        {.nodes = 400, .total_values = 10,
         .updates = {entries{1},
                     isolate{0},    // drop leader, free election
                     entries{2},
                     }}
    , true, tick_delay,
    rpc_config{ .network_delay = 20ms, .local_delay = 1ms });
}

// Expected to work for release and dev builds
SEASTAR_THREAD_TEST_CASE(test_many_1000) {
    replication_test<steady_clock_type>(
        {.nodes = 1000, .total_values = 10,
         .updates = {entries{1},
                     isolate{0},              // drop leader, free election
                     entries{1},
                     new_leader{1},
                     set_config{1,2,3,4,5},   // Set configuration to smaller size
                     }}
    , true, tick_delay,
    rpc_config{ .network_delay = 20ms, .local_delay = 1ms });
}
#endif
