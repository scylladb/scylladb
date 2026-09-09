/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "utils.hh"
#include "utils/assert.hh"
#include "utils/rjson.hh"
#include <seastar/http/request.hh>
#include <chrono>
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
    enum class mode { ann, bm25 };

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

    explicit vs_mock_server(response next_search_response)
        : _next_search_response(std::move(next_search_response)) {
    }

    explicit vs_mock_server(seastar::httpd::http_server::server_credentials_ptr credentials)
        : _credentials(credentials) {
    }

    explicit vs_mock_server(mode m)
        : _mode(m)
        , _next_search_response(default_response(m)) {
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

    const std::vector<request>& search_requests() const {
        return _search_requests;
    }

    const std::vector<request>& status_requests() const {
        return _status_requests;
    }

    void next_search_response(response r) {
        _next_search_response = std::move(r);
    }

    void search_response_delay(std::chrono::seconds delay) {
        _search_response_delay = delay;
    }

    void next_status_response(response response) {
        _next_status_response = std::move(response);
    }

    /// Compatibility alias for mode::ann — use search_requests() instead.
    const std::vector<request>& ann_requests() const {
        SCYLLA_ASSERT(_mode == mode::ann);
        return search_requests();
    }

    /// Compatibility alias for mode::ann — use next_search_response() instead.
    void next_ann_response(response r) {
        SCYLLA_ASSERT(_mode == mode::ann);
        next_search_response(std::move(r));
    }

    /// Compatibility alias for mode::ann — use search_response_delay() instead.
    void ann_response_delay(std::chrono::seconds delay) {
        SCYLLA_ASSERT(_mode == mode::ann);
        search_response_delay(delay);
    }

    const std::vector<request>& index_status_requests() const {
        return _index_status_requests;
    }

    void next_index_status_response(response response) {
        _next_index_status_response = std::move(response);
    }

private:
    static response default_response(mode m) {
        if (m == mode::bm25) {
            return {seastar::http::reply::status_type::ok, CORRECT_BM25_RESPONSE_FOR_TEST_TABLE};
        }
        return {seastar::http::reply::status_type::ok, CORRECT_RESPONSE_FOR_TEST_TABLE};
    }

    static seastar::sstring mode_suffix(mode m) {
        return m == mode::bm25 ? "/bm25" : "/ann";
    }

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

    seastar::future<std::unique_ptr<seastar::http::reply>> handle_search_request(
            std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
        auto full_path = req->get_path_param("path");
        // Only handle requests matching our configured mode suffix; return 404 for others.
        if (!full_path.ends_with(mode_suffix(_mode))) {
            rep->set_status(seastar::http::reply::status_type::not_found);
            rep->write_body("json", R"("not found")");
            co_return rep;
        }
        request r{.path = INDEXES_PATH + "/" + full_path, .body = co_await util::read_entire_stream_contiguous(*req->content_stream)};
        _search_requests.push_back(std::move(r));
        rep->set_status(_next_search_response.status);
        rep->write_body("json", _next_search_response.body);
        if (_search_response_delay > 0s) {
            co_await seastar::sleep(_search_response_delay);
        }
        co_return rep;
    }

    seastar::future<std::unique_ptr<seastar::http::reply>> handle_status_request(
            std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
        _status_requests.push_back(request{.path = "/api/v1/status", .body = co_await seastar::util::read_entire_stream_contiguous(*req->content_stream)});
        rep->set_status(_next_status_response.status);
        rep->write_body("json", _next_status_response.body);
        co_return rep;
    }

    seastar::future<std::unique_ptr<seastar::http::reply>> handle_index_status_request(
            std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
        request r{.path = INDEXES_PATH + "/" + req->get_path_param("path"),
                .body = co_await util::read_entire_stream_contiguous(*req->content_stream)};
        _index_status_requests.push_back(std::move(r));
        rep->set_status(_next_index_status_response.status);
        rep->write_body("json", _next_index_status_response.body);
        co_return rep;
    }

    void set_routes(seastar::httpd::routes& r) {
        r.add(seastar::httpd::operation_type::POST, seastar::httpd::url(INDEXES_PATH).remainder("path"),
                new seastar::httpd::function_handler(
                        [this](std::unique_ptr<seastar::http::request> req,
                                std::unique_ptr<seastar::http::reply> rep) -> seastar::future<std::unique_ptr<seastar::http::reply>> {
                            return handle_search_request(std::move(req), std::move(rep));
                        },
                        "json"));
        r.add(seastar::httpd::operation_type::GET, seastar::httpd::url(INDEXES_PATH).remainder("path"),
                new seastar::httpd::function_handler(
                        [this](std::unique_ptr<seastar::http::request> req,
                                std::unique_ptr<seastar::http::reply> rep) -> seastar::future<std::unique_ptr<seastar::http::reply>> {
                            return handle_index_status_request(std::move(req), std::move(rep));
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

    mode _mode = mode::ann;
    uint16_t _port = 0;
    seastar::sstring _host;
    std::unique_ptr<seastar::httpd::http_server> _http_server;
    std::vector<request> _search_requests;
    std::vector<request> _status_requests;
    std::vector<request> _index_status_requests;
    response _next_search_response = default_response(_mode);
    std::chrono::seconds _search_response_delay = std::chrono::seconds(0);
    response _next_status_response{seastar::http::reply::status_type::ok, rjson::quote_json_string("SERVING")};
    response _next_index_status_response{seastar::http::reply::status_type::ok, R"({"status":"SERVING","count":0,"build_progress":100.0})"};
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
