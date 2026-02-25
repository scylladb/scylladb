/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/http/common.hh>
#include "vector_search/client.hh"
#include "vector_search/clients.hh"
#include "vector_search/truststore.hh"
#include "vector_search/utils.hh"
#include "vs_mock_server.hh"
#include "unavailable_server.hh"
#include "utils.hh"
#include "utils/rjson.hh"
#include <boost/test/tools/old/interface.hpp>
#include <seastar/testing/test_case.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/as_future.hh>
#include <optional>
#include <variant>

using namespace seastar;
using namespace vector_search;
using namespace test::vector_search;
using namespace seastar::httpd;
using namespace std::chrono_literals;

namespace {

logging::logger client_test_logger("client_test");

const auto REQUEST_TIMEOUT = utils::updateable_value<uint32_t>{100};
constexpr auto PATH = "/api/v1/indexes/ks/idx/ann";
constexpr auto CONTENT = R"({"vector": [0.1, 0.2, 0.3], "limit": 10})";

template <typename Server>
client::endpoint_type make_endpoint(const std::unique_ptr<Server>& server) {
    return client::endpoint_type{server->host(), server->port(), seastar::net::inet_address(server->host())};
}

future<std::unique_ptr<vs_mock_server>> make_available(std::unique_ptr<unavailable_server>& down_server) {
    // Replace the unavailable server with an available one.
    auto server = std::make_unique<vs_mock_server>();
    co_await server->start(co_await down_server->take_socket());
    co_return server;
}

/// Create a truststore with empty options (sufficient for HTTP-only tests).
truststore make_test_truststore() {
    return truststore(
            client_test_logger,
            utils::updateable_value(std::unordered_map<sstring, sstring>{}),
            [](auto) { return make_ready_future<>(); });
}

} // namespace

SEASTAR_TEST_CASE(is_up_after_construction) {
    auto server = co_await make_vs_mock_server();
    client client{client_test_logger, make_endpoint(server), REQUEST_TIMEOUT, shared_ptr<seastar::tls::certificate_credentials>{}};

    BOOST_CHECK(client.is_up());

    co_await client.close();
    co_await server->stop();
}

SEASTAR_TEST_CASE(is_up_when_server_returned_ok_status) {
    abort_source_timeout as;
    auto server = co_await make_vs_mock_server();
    client client{client_test_logger, make_endpoint(server), REQUEST_TIMEOUT, shared_ptr<seastar::tls::certificate_credentials>{}};

    auto res = co_await client.request(operation_type::POST, PATH, CONTENT, as.reset());

    BOOST_CHECK(client.is_up());
    BOOST_CHECK(res);

    co_await client.close();
    co_await server->stop();
}

SEASTAR_TEST_CASE(is_up_when_server_returned_client_error_status) {
    abort_source_timeout as;
    auto server = co_await make_vs_mock_server();
    server->next_ann_response(vs_mock_server::response{seastar::http::reply::status_type::bad_request, "Bad request"});
    client client{client_test_logger, make_endpoint(server), REQUEST_TIMEOUT, shared_ptr<seastar::tls::certificate_credentials>{}};

    auto res = co_await client.request(operation_type::POST, PATH, CONTENT, as.reset());

    BOOST_CHECK(client.is_up());
    BOOST_CHECK(res);
    BOOST_CHECK_EQUAL(res.value().status, seastar::http::reply::status_type::bad_request);

    co_await client.close();
    co_await server->stop();
}

SEASTAR_TEST_CASE(is_up_when_request_is_aborted) {
    abort_source as;
    auto server = co_await make_vs_mock_server();
    server->next_ann_response(vs_mock_server::response{seastar::http::reply::status_type::ok, "{}"});
    client client{client_test_logger, make_endpoint(server), REQUEST_TIMEOUT, shared_ptr<seastar::tls::certificate_credentials>{}};

    as.request_abort();
    auto res = co_await client.request(operation_type::POST, PATH, CONTENT, as);

    BOOST_CHECK(client.is_up());
    BOOST_CHECK(!res);
    BOOST_CHECK(std::holds_alternative<aborted_error>(res.error()));

    co_await client.close();
    co_await server->stop();
}

SEASTAR_TEST_CASE(is_up_when_server_returned_server_error_status) {
    abort_source_timeout as;
    auto server = co_await make_vs_mock_server();
    server->next_ann_response(vs_mock_server::response{seastar::http::reply::status_type::internal_server_error, "Internal Server Error"});

    client client{client_test_logger, make_endpoint(server), REQUEST_TIMEOUT, shared_ptr<seastar::tls::certificate_credentials>{}};

    auto res = co_await client.request(operation_type::POST, PATH, CONTENT, as.reset());

    BOOST_CHECK(client.is_up());
    BOOST_CHECK(res);
    BOOST_CHECK(res->status == seastar::http::reply::status_type::internal_server_error);

    co_await client.close();
    co_await server->stop();
}

SEASTAR_TEST_CASE(is_up_when_server_returned_service_unavailable_status) {
    abort_source_timeout as;
    auto server = co_await make_vs_mock_server();
    server->next_ann_response(vs_mock_server::response{seastar::http::reply::status_type::service_unavailable, "Service Unavailable"});

    client client{client_test_logger, make_endpoint(server), REQUEST_TIMEOUT, shared_ptr<seastar::tls::certificate_credentials>{}};

    auto res = co_await client.request(operation_type::POST, PATH, CONTENT, as.reset());

    BOOST_CHECK(client.is_up());
    BOOST_CHECK(res);
    BOOST_CHECK(res->status == seastar::http::reply::status_type::service_unavailable);

    co_await client.close();
    co_await server->stop();
}

SEASTAR_TEST_CASE(is_down_when_server_is_not_available) {
    abort_source_timeout as;
    auto down_server = co_await make_unavailable_server();
    client client{client_test_logger, make_endpoint(down_server), REQUEST_TIMEOUT, shared_ptr<seastar::tls::certificate_credentials>{}};

    auto res = co_await client.request(operation_type::POST, PATH, CONTENT, as.reset());

    BOOST_CHECK(!client.is_up());
    BOOST_CHECK(!res);
    BOOST_CHECK(std::holds_alternative<service_unavailable_error>(res.error()));

    co_await client.close();
    co_await down_server->stop();
}

SEASTAR_TEST_CASE(becomes_up_when_server_status_is_serving) {
    abort_source_timeout as;
    auto down_server = co_await make_unavailable_server();
    client client{client_test_logger, make_endpoint(down_server), REQUEST_TIMEOUT, shared_ptr<seastar::tls::certificate_credentials>{}};

    auto res = co_await client.request(operation_type::POST, PATH, CONTENT, as.reset());
    auto server = co_await make_available(down_server);
    server->next_status_response(vs_mock_server::response{seastar::http::reply::status_type::ok, rjson::quote_json_string("SERVING")});

    auto became_up = co_await repeat_until([&client]() -> future<bool> {
        co_return client.is_up();
    });
    BOOST_CHECK(became_up);

    co_await client.close();
    co_await server->stop();
    co_await down_server->stop();
}

SEASTAR_TEST_CASE(remains_down_when_server_status_is_not_serving) {
    abort_source_timeout as;
    std::vector<sstring> non_serving_statuses{
            "INITIALIZING",
            "CONNECTING_TO_DB",
            "BOOTSTRAPPING",
    };
    for (auto const& status : non_serving_statuses) {
        auto down_server = co_await make_unavailable_server();
        client client{client_test_logger, make_endpoint(down_server), REQUEST_TIMEOUT, shared_ptr<seastar::tls::certificate_credentials>{}};

        co_await client.request(operation_type::POST, PATH, CONTENT, as.reset());
        auto server = co_await make_available(down_server);
        server->next_status_response(vs_mock_server::response{seastar::http::reply::status_type::ok, rjson::quote_json_string(status)});

        auto got_2_status_requests = co_await repeat_until([&]() -> future<bool> {
            // waiting for 2 status requests to be sure that node had a chance to become up
            co_return server->status_requests().size() >= 2;
        });
        BOOST_CHECK(got_2_status_requests);
        BOOST_CHECK(!client.is_up());

        co_await client.close();
        co_await server->stop();
        co_await down_server->stop();
    }
}

SEASTAR_TEST_CASE(is_down_when_connection_times_out) {
    abort_source_timeout as;
    auto unreachable = co_await make_unreachable_socket();
    client client{client_test_logger, client::endpoint_type{unreachable.host, unreachable.port, seastar::net::inet_address(unreachable.host)},
            utils::updateable_value<uint32_t>{5000}, shared_ptr<seastar::tls::certificate_credentials>{}};

    auto res = co_await client.request(operation_type::POST, PATH, CONTENT, as.reset());

    BOOST_CHECK(!client.is_up());
    BOOST_CHECK(!res);
    BOOST_CHECK(std::holds_alternative<service_unavailable_error>(res.error()));

    co_await unreachable.close();
    co_await client.close();
}

SEASTAR_TEST_CASE(connection_timeout_cannot_be_smaller_than_5s) {
    abort_source_timeout as;
    auto unreachable = co_await make_unreachable_socket();
    client client{client_test_logger, client::endpoint_type{unreachable.host, unreachable.port, seastar::net::inet_address(unreachable.host)},
            utils::updateable_value<uint32_t>{1000}, shared_ptr<seastar::tls::certificate_credentials>{}};


    auto start = std::chrono::steady_clock::now();
    auto res = co_await client.request(operation_type::POST, PATH, CONTENT, as.reset());
    auto duration = std::chrono::steady_clock::now() - start;

    BOOST_CHECK(duration >= 5s);

    co_await unreachable.close();
    co_await client.close();
}

BOOST_AUTO_TEST_CASE(test_get_keepalive_parameters) {

    auto params1 = get_keepalive_parameters(10s);
    BOOST_CHECK_EQUAL(params1.idle.count(), 4);
    BOOST_CHECK_EQUAL(params1.interval.count(), 2);
    BOOST_CHECK_EQUAL(params1.count, 3);

    auto params2 = get_keepalive_parameters(5s);
    BOOST_CHECK_EQUAL(params2.idle.count(), 2);
    BOOST_CHECK_EQUAL(params2.interval.count(), 1);
    BOOST_CHECK_EQUAL(params2.count, 3);
}

/// Validates fix for SCYLLADB-802: handle_changed() must not disrupt ongoing
/// client availability.
///
/// Before the fix, handle_changed() called clear() (emptying _clients) and then
/// created new clients one-by-one with co_await. During this async gap, any
/// concurrent call to get_clients() would observe an empty _clients and return
/// addr_unavailable_error after timing out.
///
/// The fix builds the new client list into a local vector and then swaps it in
/// atomically.
///
/// This test verifies that after initial population, repeated handle_changed()
/// calls never leave get_clients() returning addr_unavailable_error.
SEASTAR_TEST_CASE(handle_changed_does_not_empty_clients) {
    auto ts = make_test_truststore();

    // Create the mock server before clients so that on stack destruction,
    // clients (with their HTTP connections) are destroyed before the server.
    auto server = co_await make_vs_mock_server();
    auto host = server->host();
    auto port = server->port();
    auto addr = net::inet_address(host);

    auto request_timeout = utils::updateable_value<uint32_t>(100);
    clients c(client_test_logger, [] {}, request_timeout, ts);
    c.timeout(100ms);

    uri u{uri::schema_type::http, host, port};
    dns::host_address_map addrs{{host, {addr}}};

    abort_source as;

    // Initial population — after this, _clients has one entry.
    co_await c.handle_changed({u}, addrs);

    // Verify clients are available after initial population.
    auto result = co_await c.get_clients(as);
    BOOST_REQUIRE(result.has_value());
    BOOST_CHECK_EQUAL(result->size(), 1u);

    // Repeatedly call handle_changed() and verify that get_clients() still
    // returns clients each time. Before the fix, clear() inside handle_changed()
    // would transiently empty _clients; after the fix the swap is atomic so
    // get_clients() always sees a populated vector.
    for (int i = 0; i < 10; ++i) {
        co_await c.handle_changed({u}, addrs);
        result = co_await c.get_clients(as);
        BOOST_CHECK_MESSAGE(result.has_value(),
                "get_clients() returned error after handle_changed() iteration " + std::to_string(i));
        if (result) {
            BOOST_CHECK_EQUAL(result->size(), 1u);
        }
    }

    co_await c.stop();
    co_await server->stop();
    co_await ts.stop();
}

/// Validates fix for SCYLLADB-802: request() must retry when get_clients()
/// returns addr_unavailable_error.
///
/// Before the fix, addr_unavailable_error from get_clients() (e.g. because TLS
/// credential initialization via truststore::get() hadn't completed within the
/// wait_for_client_timeout) caused request() to fail immediately.
///
/// The fix makes request() trigger a DNS refresh and retry, giving the async
/// setup time to complete.
SEASTAR_TEST_CASE(request_retries_on_addr_unavailable) {
    auto ts = make_test_truststore();
    int refresh_count = 0;
    auto request_timeout = utils::updateable_value<uint32_t>(1000);
    auto server = co_await make_vs_mock_server();
    auto host = server->host();
    auto port = server->port();
    auto addr = net::inet_address(host);

    uri u{uri::schema_type::http, host, port};
    dns::host_address_map addrs{{host, {addr}}};

    // Supply clients on the 2nd refresh trigger (simulating the TLS setup delay).
    std::optional<future<>> pending_handle_changed;
    clients* clients_ptr = nullptr;
    clients c(client_test_logger, [&] {
        refresh_count++;
        // On the 2nd trigger, populate the clients (simulating DNS/TLS readiness).
        if (refresh_count == 2 && clients_ptr) {
            pending_handle_changed = clients_ptr->handle_changed({u}, addrs);
        }
    }, request_timeout, ts);
    clients_ptr = &c;
    c.timeout(50ms);

    abort_source as;

    // Initially _clients is empty, so get_clients() will return addr_unavailable_error.
    // The first trigger fires from the producer factory (sequential_producer).
    // Before the fix, request() would immediately return the error.
    // After the fix, it retries and the 2nd refresh populates clients.
    auto result = co_await c.request(httpd::operation_type::POST, "/api/v1/indexes/ks/idx/ann", CONTENT, as);

    // The request should succeed because the retry logic gives the clients time
    // to become available.
    BOOST_CHECK(result.has_value());
    BOOST_CHECK(refresh_count >= 2);

    if (pending_handle_changed) {
        co_await std::move(*pending_handle_changed);
    }
    co_await c.stop();
    co_await server->stop();
    co_await ts.stop();
}

/// Validates that request() returns addr_unavailable_error (not
/// service_unavailable_error) when all retries are exhausted due to no clients
/// being available.
SEASTAR_TEST_CASE(request_returns_addr_unavailable_after_retries_exhausted) {
    auto ts = make_test_truststore();
    auto request_timeout = utils::updateable_value<uint32_t>(100);
    clients c(client_test_logger, [] {}, request_timeout, ts);
    c.timeout(50ms);

    abort_source as;

    auto result = co_await c.request(httpd::operation_type::POST, "/api/v1/indexes/ks/idx/ann", "{}", as);

    BOOST_REQUIRE(!result.has_value());
    BOOST_CHECK(std::holds_alternative<addr_unavailable_error>(result.error()));

    co_await c.stop();
    co_await ts.stop();
}
