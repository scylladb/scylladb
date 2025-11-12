/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "seastar/http/common.hh"
#include "vector_search/client.hh"
#include "vs_mock_server.hh"
#include "utils.hh"
#include <boost/test/tools/old/interface.hpp>
#include <seastar/testing/test_case.hh>
#include <seastar/coroutine/as_future.hh>

using namespace seastar;
using namespace vector_search;
using namespace test::vector_search;
using namespace seastar::httpd;

namespace {

logging::logger client_test_logger("client_test");

const auto REQUEST_TIMEOUT = utils::updateable_value<uint32_t>{100};
constexpr auto PATH = "/api/v1/indexes/ks/idx/ann";
constexpr auto CONTENT = R"({"vector": [0.1, 0.2, 0.3], "limit": 10})";

client::endpoint_type make_endpoint(const std::unique_ptr<vs_mock_server>& server) {
    return client::endpoint_type{server->host(), server->port(), seastar::net::inet_address(server->host())};
}

} // namespace

SEASTAR_TEST_CASE(is_up_after_construction) {
    auto server = co_await make_vs_mock_server();
    client client{client_test_logger, make_endpoint(server), REQUEST_TIMEOUT};

    BOOST_CHECK(client.is_up());

    co_await client.close();
    co_await server->stop();
}

SEASTAR_TEST_CASE(is_up_when_server_returned_ok_status) {
    abort_source_timeout as;
    auto server = co_await make_vs_mock_server();
    client client{client_test_logger, make_endpoint(server), REQUEST_TIMEOUT};

    auto res = co_await client.request(operation_type::POST, PATH, CONTENT, as.reset());

    BOOST_CHECK(client.is_up());

    co_await client.close();
    co_await server->stop();
}

SEASTAR_TEST_CASE(is_up_when_server_returned_client_error_status) {
    abort_source_timeout as;
    auto server = co_await make_vs_mock_server();
    server->next_ann_response(vs_mock_server::response{seastar::http::reply::status_type::bad_request, "Bad request"});
    client client{client_test_logger, make_endpoint(server), REQUEST_TIMEOUT};

    auto res = co_await client.request(operation_type::POST, PATH, CONTENT, as.reset());

    BOOST_CHECK(client.is_up());

    co_await client.close();
    co_await server->stop();
}

SEASTAR_TEST_CASE(is_up_when_request_is_aborted) {
    abort_source as;
    auto server = co_await make_vs_mock_server();
    server->next_ann_response(vs_mock_server::response{seastar::http::reply::status_type::ok, "{}"});
    client client{client_test_logger, make_endpoint(server), REQUEST_TIMEOUT};

    as.request_abort();
    auto res = co_await client.request(operation_type::POST, PATH, CONTENT, as);

    BOOST_CHECK(client.is_up());
    BOOST_CHECK(!res);
    BOOST_CHECK(std::holds_alternative<aborted_error>(res.error()));

    co_await client.close();
    co_await server->stop();
}
