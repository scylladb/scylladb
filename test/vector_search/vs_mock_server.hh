/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

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

    seastar::future<> stop();

    seastar::sstring host() const;

    uint16_t port() const;

    const std::vector<request>& ann_requests() const;

    const std::vector<request>& status_requests() const;

    void next_ann_response(response response);

    void next_status_response(response response);

    template <typename... Args>
    static auto create(Args&&... args) -> seastar::future<std::unique_ptr<vs_mock_server>> {
        auto server = std::unique_ptr<vs_mock_server>(new vs_mock_server(std::forward<Args>(args)...));
        co_await server->start();
        co_return server;
    }

    template <typename... Args>
    static auto create(seastar::server_socket socket, Args&&... args) -> seastar::future<std::unique_ptr<vs_mock_server>> {
        auto server = std::unique_ptr<vs_mock_server>(new vs_mock_server(std::forward<Args>(args)...));
        co_await server->start(std::move(socket));
        co_return server;
    }

private:
    explicit vs_mock_server(uint16_t port);

    explicit vs_mock_server(response next_ann_response);

    vs_mock_server() = default;

    seastar::future<> start();

    seastar::future<> start(seastar::server_socket socket);

    seastar::future<> listen();

    seastar::future<std::unique_ptr<seastar::http::reply>> handle_ann_request(
            std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep);

    seastar::future<std::unique_ptr<seastar::http::reply>> handle_status_request(
            std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep);
    void set_routes(seastar::httpd::routes& r);

    std::uint16_t _port = 0;
    seastar::sstring _host;
    std::unique_ptr<seastar::httpd::http_server> _http_server;
    std::vector<request> _ann_requests;
    std::vector<request> _status_requests;
    response _next_ann_response{seastar::http::reply::status_type::ok, ""};
    response _next_status_response{seastar::http::reply::status_type::ok, "SERVING"};
    const seastar::sstring INDEXES_PATH = "/api/v1/indexes";
};

} // namespace test::vector_search
