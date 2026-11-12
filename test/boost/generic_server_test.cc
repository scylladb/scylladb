/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <chrono>
#include <optional>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/log.hh>

#include "transport/generic_server.hh"
#include "utils/assert.hh"

using namespace generic_server;
using namespace logging;
using namespace seastar;
using namespace std::literals::chrono_literals;

static logger test_logger("test_server");

class test_server : public server {
public:
    test_server(const utils::updateable_value_source<uint32_t>& c) : server("test_server", test_logger, config{utils::updateable_value<uint32_t>(c)}) {};
    test_server(const utils::updateable_value_source<uint32_t>& c, const utils::updateable_value_source<uint32_t>& timeout)
        : server("test_server", test_logger, config{utils::updateable_value<uint32_t>(c), utils::updateable_value<uint32_t>(timeout)}) {}
    gate::holder hold_connection() { return gate::holder(_gate); }
protected:
    [[noreturn]] shared_ptr<connection> make_connection(socket_address, connected_socket&&, socket_address, named_semaphore& sem, semaphore_units<named_semaphore_exception_factory> initial_sem_units) override {
        SCYLLA_ASSERT(false);
    }
    scheduling_group get_scheduling_group_for_new_connection() const override { return current_scheduling_group(); }
};

SEASTAR_TEST_CASE(stop_without_listening) {
    utils::updateable_value_source<uint32_t> concurrency(1);
    test_server srv(concurrency);
    co_await with_timeout(lowres_clock::now() + 5min, srv.stop());
    co_return;
}

SEASTAR_TEST_CASE(stop_admission_preserves_admitted_requests) {
    // Regression for #15899: closing admission must not wait for a request
    // whose reply still depends on the later peer-coordinator drain stage.
    utils::updateable_value_source<uint32_t> concurrency(1);
    utils::updateable_value_source<uint32_t> timeout(60);
    test_server srv(concurrency, timeout);
    std::optional<gate::holder> admitted(srv.hold_connection());
    auto check_admission = [&] () -> future<> {
        co_await with_timeout(lowres_clock::now() + 5s, srv.stop_accepting_requests());
        auto shutdown = srv.shutdown();
        BOOST_REQUIRE(!shutdown.available());
        co_await with_timeout(lowres_clock::now() + 5s, srv.stop_accepting_requests());
        auto stopped = srv.stop();
        BOOST_REQUIRE(!stopped.available());
        admitted.reset();
        co_await with_timeout(lowres_clock::now() + 5s,
                when_all_succeed(std::move(shutdown), std::move(stopped)).discard_result());
    };
    co_await check_admission().finally([&] {
        admitted.reset();
        return with_timeout(lowres_clock::now() + 5s, srv.stop());
    });
}
