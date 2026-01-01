/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/http/common.hh>
#include "vector_search/client.hh"
#include "vector_search/utils.hh"
#include "vs_mock_server.hh"
#include "unavailable_server.hh"
#include "utils.hh"
#include "utils/rjson.hh"
#include <boost/test/tools/old/interface.hpp>
#include <seastar/testing/test_case.hh>
#include <seastar/coroutine/as_future.hh>

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
