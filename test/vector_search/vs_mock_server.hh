/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils.hh"
#include "utils/rjson.hh"
#include "seastar/http/request.hh"
#include <seastar/core/future.hh>
#include <seastar/core/seastar.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/util/short_streams.hh>
#include <seastar/net/api.hh>
#include <cstdint>
#include <memory>
#include <vector>

namespace test::vector_search {

class vs_mock_server {
public:
    struct request {
        seastar::sstring path;
        seastar::sstring body;
    };

    struct response {
        seastar::http::reply::status_type status;
        seastar::sstring body;
    };

    explicit vs_mock_server(uint16_t port)
        : _port(port) {
    }

    explicit vs_mock_server(response next_ann_response)
        : _next_ann_response(std::move(next_ann_response)) {
    }

    explicit vs_mock_server(seastar::httpd::http_server::server_credentials_ptr credentials)
        : _credentials(credentials) {
    }

    vs_mock_server() = default;

    seastar::future<> start() {
        co_await listen();
    }

    seastar::future<> start(seastar::server_socket socket) {
        auto [server, addr] = co_await new_http_server(
                [this](auto& r) {
                    set_routes(r);
                },
                std::move(socket));
        _http_server = std::move(server);
        _port = addr.port();
    }

    seastar::future<> stop() {
        co_await _http_server->stop();
    }

    seastar::sstring host() const {
        return _host;
    }

    uint16_t port() const {
        return _port;
    }

    const std::vector<request>& ann_requests() const {
        return _ann_requests;
    }

    const std::vector<request>& status_requests() const {
        return _status_requests;
    }

    void next_ann_response(response response) {
        _next_ann_response = std::move(response);
    }

    void next_status_response(response response) {
        _next_status_response = std::move(response);
    }

private:
    seastar::future<> listen() {
        co_await try_on_loopback_address([this](auto host) -> seastar::future<> {
            auto [s, addr] = co_await new_http_server(
                    [this](auto& r) {
                        set_routes(r);
                    },
                    host.c_str(), _port, _credentials);
            _http_server = std::move(s);
            _port = addr.port();
            _host = std::move(host);
        });
    }

    seastar::future<std::unique_ptr<seastar::http::reply>> handle_ann_request(
            std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
        request r{.path = INDEXES_PATH + "/" + req->get_path_param("path"), .body = co_await util::read_entire_stream_contiguous(*req->content_stream)};
        _ann_requests.push_back(std::move(r));
        rep->set_status(_next_ann_response.status);
        rep->write_body("json", _next_ann_response.body);
        co_return rep;
    }

    seastar::future<std::unique_ptr<seastar::http::reply>> handle_status_request(
            std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
        _status_requests.push_back(request{.path = "/api/v1/status", .body = co_await seastar::util::read_entire_stream_contiguous(*req->content_stream)});
        rep->set_status(_next_status_response.status);
        rep->write_body("json", _next_status_response.body);
        co_return rep;
    }

    void set_routes(seastar::httpd::routes& r) {
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

    uint16_t _port = 0;
    seastar::sstring _host;
    std::unique_ptr<seastar::httpd::http_server> _http_server;
    std::vector<request> _ann_requests;
    std::vector<request> _status_requests;
    response _next_ann_response{seastar::http::reply::status_type::ok, CORRECT_RESPONSE_FOR_TEST_TABLE};
    response _next_status_response{seastar::http::reply::status_type::ok, rjson::quote_json_string("SERVING")};
    const seastar::sstring INDEXES_PATH = "/api/v1/indexes";
    seastar::httpd::http_server::server_credentials_ptr _credentials;
};

template <typename... Args>
auto make_vs_mock_server(Args&&... args) -> seastar::future<std::unique_ptr<vs_mock_server>> {
    auto server = std::make_unique<vs_mock_server>(std::forward<Args>(args)...);
    co_await server->start();
    co_return server;
}

} // namespace test::vector_search
