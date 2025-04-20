/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
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
    test_server(const db::config& cfg) : server("test_server", test_logger, config(cfg)) {};
protected:
    [[noreturn]] shared_ptr<connection> make_connection(socket_address, connected_socket&&, socket_address, named_semaphore& sem, semaphore_units<named_semaphore_exception_factory> initial_sem_units) override {
        SCYLLA_ASSERT(false);
    }
};

SEASTAR_TEST_CASE(stop_without_listening) {
    db::config cfg;
    test_server srv(cfg);
    co_await with_timeout(lowres_clock::now() + 5min, srv.stop());
    co_return;
}
