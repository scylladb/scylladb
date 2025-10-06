/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "vs_mock_server.hh"
#include "util.h"

using namespace seastar;
using namespace seastar::httpd;

namespace test::vector_search {
namespace {

constexpr auto const* LOCALHOST = "127.0.0.1";

auto listen_on_port(std::unique_ptr<http_server> server, sstring host, uint16_t port) -> future<std::tuple<std::unique_ptr<http_server>, socket_address>> {
    auto inaddr = net::inet_address(host);
    auto const addr = socket_address(inaddr, port);
    ::listen_options opts;
    opts.set_fixed_cpu(this_shard_id());
    co_await server->listen(addr, opts);
    auto const& listeners = http_server_tester::listeners(*server);
    co_return std::make_tuple(std::move(server), listeners.at(0).local_address().port());
}

auto make_http_server(std::function<void(routes& r)> set_routes) {
    static unsigned id = 0;
    auto server = std::make_unique<http_server>(fmt::format("test_vector_store_client_{}", id++));
    set_routes(server->_routes);
    server->set_content_streaming(true);
    return server;
}

auto new_http_server(std::function<void(routes& r)> set_routes, sstring host = LOCALHOST, uint16_t port = 0)
        -> future<std::tuple<std::unique_ptr<http_server>, socket_address>> {
    co_return co_await listen_on_port(make_http_server(set_routes), std::move(host), port);
}

auto new_http_server(std::function<void(routes& r)> set_routes, server_socket socket) -> future<std::tuple<std::unique_ptr<http_server>, socket_address>> {
    auto server = make_http_server(set_routes);
    auto& listeners = http_server_tester::listeners(*server);
    listeners.push_back(std::move(socket));
    co_await server->do_accepts(listeners.size() - 1);
    co_return std::make_tuple(std::move(server), listeners.back().local_address().port());
}

} // namespace

vs_mock_server::vs_mock_server(uint16_t port)
    : _port(port) {
}

vs_mock_server::vs_mock_server(ann_resp next_ann_response)
    : _next_ann_response{std::move(next_ann_response)} {
}

seastar::future<> vs_mock_server::start() {
    co_await listen();
}

seastar::future<> vs_mock_server::start(seastar::server_socket socket) {
    auto [server, addr] = co_await new_http_server(
            [this](auto& r) {
                set_routes(r);
            },
            std::move(socket));
    _http_server = std::move(server);
    _port = addr.port();
}

seastar::future<> vs_mock_server::stop() {
    co_await _http_server->stop();
}

seastar::sstring vs_mock_server::host() const {
    return _host;
}

uint16_t vs_mock_server::port() const {
    return _port;
}

const std::vector<vs_mock_server::ann_req>& vs_mock_server::requests() const {
    return _ann_requests;
}

void vs_mock_server::next_ann_response(ann_resp response) {
    _next_ann_response = std::move(response);
}

seastar::future<> vs_mock_server::listen() {
    co_await try_on_loopback_address([this](auto host) -> seastar::future<> {
        auto [s, addr] = co_await new_http_server(
                [this](auto& r) {
                    set_routes(r);
                },
                host.c_str(), _port);
        _http_server = std::move(s);
        _port = addr.port();
        _host = std::move(host);
    });
}

seastar::future<std::unique_ptr<seastar::http::reply>> vs_mock_server::handle_ann_request(
        std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    ann_req r{.path = INDEXES_PATH + "/" + req->get_path_param("path"), .body = co_await seastar::util::read_entire_stream_contiguous(*req->content_stream)};
    _ann_requests.push_back(std::move(r));
    rep->set_status(_next_ann_response.status);
    rep->write_body("json", _next_ann_response.body);
    co_return rep;
}

seastar::future<std::unique_ptr<seastar::http::reply>> vs_mock_server::handle_status_request(
        std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    rep->set_status(seastar::http::reply::status_type::ok);
    rep->write_body("json", "SERVING");
    co_return rep;
}

void vs_mock_server::set_routes(seastar::httpd::routes& r) {
    r.add(seastar::httpd::operation_type::POST, seastar::httpd::url(INDEXES_PATH).remainder("path"),
            new seastar::httpd::function_handler(
                    [this](std::unique_ptr<seastar::http::request> req,
                            std::unique_ptr<seastar::http::reply> rep) -> seastar::future<std::unique_ptr<seastar::http::reply>> {
                        return handle_ann_request(std::move(req), std::move(rep));
                    },
                    "json"));
    r.add(seastar::httpd::operation_type::GET, seastar::httpd::url("/api/v1/status").remainder("status"),
            new seastar::httpd::function_handler(
                    [this](std::unique_ptr<seastar::http::request> req,
                            std::unique_ptr<seastar::http::reply> rep) -> seastar::future<std::unique_ptr<seastar::http::reply>> {
                        return handle_status_request(std::move(req), std::move(rep));
                    },
                    "json"));
}

} // namespace test::vector_search
