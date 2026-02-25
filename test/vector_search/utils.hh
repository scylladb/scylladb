/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "test/lib/cql_test_env.hh"
#include "db/config.hh"
#include <seastar/core/future.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/http/httpd.hh>
#include <seastar/net/api.hh>
#include <seastar/util/tmp_file.hh>
#include <functional>
#include <chrono>

namespace test::vector_search {

constexpr auto STANDARD_WAIT = std::chrono::seconds(10);

class abort_source_timeout {
    abort_source as;
    timer<> t;

public:
    explicit abort_source_timeout(std::chrono::milliseconds timeout = STANDARD_WAIT)
        : t(timer([&]() {
            as.request_abort();
        })) {
        t.arm(timeout);
    }

    abort_source& get() {
        return as;
    }

    abort_source& reset(std::chrono::milliseconds timeout = STANDARD_WAIT) {
        t.cancel();
        as = abort_source();
        t.arm(timeout);
        return as;
    }
};

inline auto repeat_until(std::chrono::milliseconds timeout, std::function<seastar::future<bool>()> func) -> seastar::future<bool> {
    auto begin = seastar::lowres_clock::now();
    while (!co_await func()) {
        if (seastar::lowres_clock::now() - begin > timeout) {
            co_return false;
        }
        co_await seastar::yield();
    }
    co_return true;
}

inline auto repeat_until(std::function<seastar::future<bool>()> func) -> seastar::future<bool> {
    return repeat_until(STANDARD_WAIT, std::move(func));
}

inline seastar::future<> try_on_loopback_address(std::function<seastar::future<>(seastar::sstring)> func) {
    constexpr size_t MAX_LOCALHOST_ADDR_TO_TRY = 127;
    for (size_t i = 1; i < MAX_LOCALHOST_ADDR_TO_TRY; i++) {
        auto host = fmt::format("127.0.0.{}", i);
        try {
            co_await func(std::move(host));
            co_return;
        } catch (...) {
        }
    }
    throw std::runtime_error("unable to perform action on any 127.0.0.x address");
}

constexpr auto const* LOCALHOST = "127.0.0.1";

inline auto listen_on_port(std::unique_ptr<seastar::httpd::http_server> server, seastar::sstring host, uint16_t port,
        seastar::httpd::http_server::server_credentials_ptr credentials)
        -> seastar::future<std::tuple<std::unique_ptr<seastar::httpd::http_server>, seastar::socket_address>> {
    auto inaddr = seastar::net::inet_address(host);
    auto const addr = seastar::socket_address(inaddr, port);
    seastar::listen_options opts;
    opts.set_fixed_cpu(seastar::this_shard_id());
    co_await server->listen(addr, opts, credentials);
    auto const& listeners = seastar::httpd::http_server_tester::listeners(*server);
    co_return std::make_tuple(std::move(server), listeners.at(0).local_address().port());
}

inline auto make_http_server(std::function<void(seastar::httpd::routes& r)> set_routes) {
    static unsigned id = 0;
    auto server = std::make_unique<seastar::httpd::http_server>(fmt::format("test_vector_store_client_{}", id++));
    set_routes(server->_routes);
    server->set_content_streaming(true);
    return server;
}

inline auto new_http_server(std::function<void(seastar::httpd::routes& r)> set_routes, seastar::sstring host = LOCALHOST, uint16_t port = 0,
        seastar::httpd::http_server::server_credentials_ptr credentials = nullptr)
        -> seastar::future<std::tuple<std::unique_ptr<seastar::httpd::http_server>, seastar::socket_address>> {
    co_return co_await listen_on_port(make_http_server(set_routes), std::move(host), port, credentials);
}

inline auto new_http_server(std::function<void(seastar::httpd::routes& r)> set_routes, seastar::server_socket socket)
        -> seastar::future<std::tuple<std::unique_ptr<seastar::httpd::http_server>, seastar::socket_address>> {
    auto server = make_http_server(set_routes);
    auto& listeners = seastar::httpd::http_server_tester::listeners(*server);
    listeners.push_back(std::move(socket));
    co_await server->do_accepts(listeners.size() - 1);
    co_return std::make_tuple(std::move(server), listeners.back().local_address().port());
}

// A sample correct ANN response for the test table created in create_test_table().
constexpr auto CORRECT_RESPONSE_FOR_TEST_TABLE = R"({
    "primary_keys": {
        "pk1": [5, 6],
        "pk2": [7, 8],
        "ck1": [9, 1],
        "ck2": [2, 3]
    },
    "distances": [0.1, 0.2]
})";

inline auto create_test_table(cql_test_env& env, const seastar::sstring& ks, const seastar::sstring& cf) -> future<schema_ptr> {
    co_await env.execute_cql(fmt::format(R"(
        create table {}.{} (
            pk1 tinyint, pk2 tinyint,
            ck1 tinyint, ck2 tinyint,
            embedding vector<float, 3>,
            primary key ((pk1, pk2), ck1, ck2))
    )",
            ks, cf));
    co_return env.local_db().find_schema(ks, cf);
}

inline seastar::future<> remove(seastar::tmp_file& f) {
    co_await f.close().finally([&f] {
        return f.remove();
    });
}

struct unreachable_socket {
    seastar::server_socket socket;
    std::vector<connected_socket> connections;
    sstring host = "127.0.0.1";
    std::uint16_t port;

    seastar::future<> close() {
        socket.abort_accept();
        for (auto& conn : connections) {
            conn.shutdown_input();
            conn.shutdown_output();
            co_await conn.wait_input_shutdown();
        }
        // There is currently no effective way to abort an ongoing connect in Seastar.
        // Timing out connect by with_timeout, remains pending coroutine in the reactor.
        // To prevent resource leaks, we close the unreachable socket and sleep,
        // allowing the pending connect coroutines to fail and release their resources.
        co_await seastar::sleep(3s);
    }
};

inline seastar::future<unreachable_socket> make_unreachable_socket() {
    unreachable_socket ret;
    seastar::listen_options opts;
    opts.listen_backlog = 1;
    opts.set_fixed_cpu(seastar::this_shard_id());
    ret.socket = seastar::listen(seastar::socket_address(seastar::net::inet_address(ret.host), 0), opts);
    ret.port = ret.socket.local_address().port();
    // Make two (backlog + 1) connections to occupy the backlog.
    ret.connections.push_back(co_await seastar::connect(seastar::socket_address(seastar::net::inet_address(ret.host), ret.port)));
    ret.connections.push_back(co_await seastar::connect(seastar::socket_address(seastar::net::inet_address(ret.host), ret.port)));
    co_return std::move(ret);
}

inline cql_test_config make_config(const seastar::sstring& primary_uri = "") {
    cql_test_config cfg;
    cfg.initial_tablets = 1;
    cfg.db_config->vector_store_primary_uri.set(primary_uri.c_str());
    return cfg;
}

} // namespace test::vector_search
