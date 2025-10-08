/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "seastar/core/abort_source.hh"
#include "utils/exceptions.hh"
#include "vector_search/node.hh"
#include "vs_mock_server.hh"
#include "util.hh"
#include <boost/test/tools/old/interface.hpp>
#include <seastar/testing/test_case.hh>
#include <seastar/coroutine/as_future.hh>

using namespace seastar;
using namespace vector_search;
using namespace test::vector_search;

SEASTAR_TEST_CASE(is_up_after_construction) {
    auto server = co_await vs_mock_server::create(vs_mock_server::response{seastar::http::reply::status_type::bad_request, "Bad request"});
    node n{client::endpoint_type{server->host(), server->port(), seastar::net::inet_address(server->host())}};

    BOOST_CHECK(n.is_up());

    co_await n.close();
    co_await server->stop();
}

SEASTAR_TEST_CASE(is_up_when_server_returned_client_return_ok_status) {
    abort_source_timeout as;
    auto server = co_await vs_mock_server::create(vs_mock_server::response{seastar::http::reply::status_type::ok, "{}"});
    node n{client::endpoint_type{server->host(), server->port(), seastar::net::inet_address(server->host())}};

    auto res = co_await n.ann("ks", "name", {0.1f, 0.2f, 0.3f}, 10, as.reset());

    BOOST_CHECK(n.is_up());

    co_await n.close();
    co_await server->stop();
}

SEASTAR_TEST_CASE(is_up_when_server_returned_client_error_status) {
    abort_source_timeout as;
    auto server = co_await vs_mock_server::create(vs_mock_server::response{seastar::http::reply::status_type::bad_request, "Bad request"});
    node n{client::endpoint_type{server->host(), server->port(), seastar::net::inet_address(server->host())}};

    auto res = co_await n.ann("ks", "name", {0.1f, 0.2f, 0.3f}, 10, as.reset());

    BOOST_CHECK(n.is_up());

    co_await n.close();
    co_await server->stop();
}

SEASTAR_TEST_CASE(is_up_when_request_is_aborted) {
    abort_source as;
    as.request_abort();
    auto server = co_await vs_mock_server::create(vs_mock_server::response{seastar::http::reply::status_type::ok, "{}"});
    node n{client::endpoint_type{server->host(), server->port(), seastar::net::inet_address(server->host())}};

    auto f = co_await coroutine::as_future(n.ann("ks", "name", {0.1f, 0.2f, 0.3f}, 10, as));

    BOOST_CHECK(n.is_up());
    BOOST_CHECK(f.failed());
    auto e = f.get_exception();
    BOOST_CHECK(try_catch<abort_requested_exception>(e));

    co_await n.close();
    co_await server->stop();
}

SEASTAR_TEST_CASE(is_down_when_server_returned_server_error_status) {
    abort_source_timeout as;
    auto server = co_await vs_mock_server::create(vs_mock_server::response{seastar::http::reply::status_type::internal_server_error, "Internal Server Error"});
    // The node might attempt to recover in the background by making a status request.
    // To prevent a race condition where the node recovers before we check its status,
    // we ensure the next status request also fails.
    server->next_status_response(vs_mock_server::response{seastar::http::reply::status_type::internal_server_error, "Internal Server Error"});
    node n{client::endpoint_type{server->host(), server->port(), seastar::net::inet_address(server->host())}};

    auto res = co_await n.ann("ks", "name", {0.1f, 0.2f, 0.3f}, 10, as.reset());

    BOOST_CHECK(!n.is_up());

    co_await n.close();
    co_await server->stop();
}

SEASTAR_TEST_CASE(becomes_up_when_server_status_is_serving) {
    abort_source_timeout as;
    auto server = co_await vs_mock_server::create(vs_mock_server::response{seastar::http::reply::status_type::internal_server_error, "Internal Server Error"});
    server->next_status_response(vs_mock_server::response{seastar::http::reply::status_type::ok, "SERVING"});
    node n{client::endpoint_type{server->host(), server->port(), seastar::net::inet_address(server->host())}};

    auto res = co_await n.ann("ks", "name", {0.1f, 0.2f, 0.3f}, 10, as.reset());
    auto became_up = co_await repeat_until([&n]() -> future<bool> {
        co_return n.is_up();
    });

    BOOST_CHECK(became_up);

    co_await n.close();
    co_await server->stop();
}

SEASTAR_TEST_CASE(remains_down_when_server_status_is_not_serving) {
    std::vector<sstring> non_serving_statuses{
            "INITIALIZING",
            "CONNECTING_TO_DB",
            "BOOTSTRAPPING",
    };
    for (auto const& status : non_serving_statuses) {
        abort_source_timeout as;
        auto server =
                co_await vs_mock_server::create(vs_mock_server::response{seastar::http::reply::status_type::internal_server_error, "Internal Server Error"});
        server->next_status_response(vs_mock_server::response{seastar::http::reply::status_type::ok, status});
        node n{client::endpoint_type{server->host(), server->port(), seastar::net::inet_address(server->host())}};

        auto res = co_await n.ann("ks", "name", {0.1f, 0.2f, 0.3f}, 10, as.reset());
        auto got_2_status_requests = co_await repeat_until([&]() -> future<bool> {
            // waiting for 2 status requests to be sure that node had a chance to become up
            co_return server->status_requests().size() >= 2;
        });

        BOOST_CHECK(got_2_status_requests);
        BOOST_CHECK(!n.is_up());

        co_await n.close();
        co_await server->stop();
    }
}
