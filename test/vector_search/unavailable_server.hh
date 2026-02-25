/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include "utils.hh"
#include <seastar/core/seastar.hh>
#include <seastar/net/api.hh>
#include <cstdint>
#include <vector>
#include <memory>

namespace test::vector_search {

class unavailable_server {
    struct Connection {
        seastar::lowres_clock::time_point timestamp;
        seastar::connected_socket socket;
    };

public:
    explicit unavailable_server(std::uint16_t port)
        : _port(port) {
    }

    seastar::future<> start() {
        co_await listen();
        (void)seastar::try_with_gate(_gate, [this] {
            return run();
        });
    }

    seastar::future<> stop() {
        if (_socket) {
            _socket.abort_accept();
            co_await _gate.close();
        }
    }

    seastar::sstring host() const {
        return _host;
    }

    std::uint16_t port() const {
        return _port;
    }

    const std::vector<Connection>& connections() const {
        return _connections;
    }

    seastar::future<seastar::server_socket> take_socket() {
        _running = false;
        // Make a connection to unblock accept() in run loop.
        co_await seastar::connect(seastar::socket_address(seastar::net::inet_address(_host), _port));
        co_await _gate.close();
        co_return std::move(_socket);
    }

    void auto_shutdown_off() {
        _auto_shutdown = false;
    }

    seastar::future<> shutdown_all_and_clear() {
        std::vector<Connection> tmp;
        std::swap(tmp, _connections);
        for (auto& conn : tmp) {
            co_await shutdown(conn.socket);
        }
    }

private:
    seastar::future<> listen() {
        co_await try_on_loopback_address([this](auto host) -> seastar::future<> {
            seastar::listen_options opts;
            opts.set_fixed_cpu(seastar::this_shard_id());
            _socket = seastar::listen(seastar::socket_address(seastar::net::inet_address(host), _port), opts);
            _port = _socket.local_address().port();
            _host = std::move(host);
            return seastar::make_ready_future<>();
        });
    }

    seastar::future<> run() {
        while (_running) {
            try {
                auto result = co_await _socket.accept();
                _connections.push_back(Connection{.timestamp = seastar::lowres_clock::now(), .socket = std::move(result.connection)});
                if (_auto_shutdown) {
                    co_await shutdown(_connections.back().socket);
                }
            } catch (...) {
                break;
            }
        }
    }

    seastar::future<> shutdown(seastar::connected_socket& cs) {
        cs.shutdown_output();
        cs.shutdown_input();
        co_await cs.wait_input_shutdown();
    }


    seastar::server_socket _socket;
    seastar::gate _gate;
    std::uint16_t _port;
    seastar::sstring _host;
    std::vector<Connection> _connections;
    bool _running = true;
    bool _auto_shutdown = true;
};

inline auto make_unavailable_server(std::uint16_t port = 0) -> seastar::future<std::unique_ptr<unavailable_server>> {
    auto ret = std::make_unique<unavailable_server>(port);
    co_await ret->start();
    co_return ret;
}

} // namespace test::vector_search
