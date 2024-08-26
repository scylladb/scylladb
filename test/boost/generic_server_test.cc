/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <chrono>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/log.hh>

#include "generic_server.hh"
#include "utils/assert.hh"

using namespace generic_server;
using namespace logging;
using namespace seastar;
using namespace std::literals::chrono_literals;

static logger test_logger("test_server");

class test_server : public server {
public:
    test_server() : server("test_server", test_logger) {};
protected:
    [[noreturn]] shared_ptr<connection> make_connection(socket_address, connected_socket&&, socket_address) {
        SCYLLA_ASSERT(false);
    }
};

SEASTAR_TEST_CASE(stop_without_listening) {
    test_server srv;
    co_await with_timeout(lowres_clock::now() + 5min, srv.stop());
}
