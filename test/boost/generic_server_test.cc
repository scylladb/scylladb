/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <chrono>
#include <seastar/core/lowres_clock.hh>
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

SEASTAR_TEST_CASE(intern_ssl_info_shares_entries) {
    auto a = connection::intern_ssl_info("TLS1.3", "TLS_AES_256_GCM_SHA384");
    auto same = connection::intern_ssl_info("TLS1.3", "TLS_AES_256_GCM_SHA384");
    auto other_cipher = connection::intern_ssl_info("TLS1.3", "TLS_CHACHA20_POLY1305_SHA256");
    auto other_protocol = connection::intern_ssl_info("TLS1.2", "TLS_AES_256_GCM_SHA384");
    auto empty = connection::intern_ssl_info({}, {});

    BOOST_REQUIRE_EQUAL(a, same);
    BOOST_REQUIRE_NE(a, other_cipher);
    BOOST_REQUIRE_NE(a, other_protocol);
    BOOST_REQUIRE_NE(other_cipher, other_protocol);
    BOOST_REQUIRE_NE(a, empty);

    BOOST_REQUIRE_EQUAL(a->protocol, "TLS1.3");
    BOOST_REQUIRE_EQUAL(a->cipher_suite, "TLS_AES_256_GCM_SHA384");
    BOOST_REQUIRE_EQUAL(other_cipher->protocol, "TLS1.3");
    BOOST_REQUIRE_EQUAL(other_cipher->cipher_suite, "TLS_CHACHA20_POLY1305_SHA256");
    BOOST_REQUIRE_EQUAL(other_protocol->protocol, "TLS1.2");
    BOOST_REQUIRE_EQUAL(other_protocol->cipher_suite, "TLS_AES_256_GCM_SHA384");
    BOOST_REQUIRE(empty->protocol.empty());
    BOOST_REQUIRE(empty->cipher_suite.empty());
    // Entries are stable: a later insertion must not move an earlier one.
    BOOST_REQUIRE_EQUAL(a, connection::intern_ssl_info("TLS1.3", "TLS_AES_256_GCM_SHA384"));
    co_return;
}
