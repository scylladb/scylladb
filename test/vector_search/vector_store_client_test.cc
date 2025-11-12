/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "vector_search/vector_store_client.hh"
#include "utils.hh"
#include "vs_mock_server.hh"
#include "seastar/core/future.hh"
#include "seastar/core/when_all.hh"
#include "db/config.hh"
#include "exceptions/exceptions.hh"
#include "cql3/statements/select_statement.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/log.hh"
#include <functional>
#include <chrono>
#include <iostream>
#include <memory>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/metrics_api.hh>
#include <seastar/net/api.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/json/json_elements.hh>
#include <seastar/net/dns.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/short_streams.hh>
#include <seastar/net/tcp.hh>
#include <variant>
#include <vector>


namespace {

using namespace seastar;
using namespace test::vector_search;

using vector_store_client = vector_search::vector_store_client;
using vector_store_client_tester = vector_search::vector_store_client_tester;
using config = vector_store_client::config;
using configuration_exception = exceptions::configuration_exception;
using inet_address = seastar::net::inet_address;
using milliseconds = std::chrono::milliseconds;
using seconds = std::chrono::seconds;
using status_type = http::reply::status_type;

auto repeat_until(milliseconds timeout, std::function<future<bool>()> func) -> future<bool> {
    auto begin = lowres_clock::now();
    while (!co_await func()) {
        if (lowres_clock::now() - begin > timeout) {
            co_return false;
        }
        co_await seastar::yield();
    }
    co_return true;
}

constexpr auto STANDARD_WAIT = std::chrono::seconds(10);

auto repeat_until(std::function<future<bool>()> func) -> future<bool> {
    return repeat_until(STANDARD_WAIT, std::move(func));
}


class abort_source_timeout {
    abort_source as;
    timer<> t;

public:
    explicit abort_source_timeout(milliseconds timeout = STANDARD_WAIT)
        : t(timer([&]() {
            as.request_abort();
        })) {
        t.arm(timeout);
    }

    abort_source& get() {
        return as;
    }

    abort_source& reset(milliseconds timeout = STANDARD_WAIT) {
        t.cancel();
        as = abort_source();
        t.arm(timeout);
        return as;
    }
};

auto print_addr(const inet_address& addr) -> sstring {
    return format("{}", addr);
}

auto get_metrics_value(sstring metric_name, const auto& all_metrics) {
    const auto& all_metadata = *all_metrics->metadata;
    const auto m = find_if(cbegin(all_metadata), cend(all_metadata), [&metric_name](const auto& x) {
        return x.mf.name == metric_name;
    });
    return all_metrics->values[distance(cbegin(all_metadata), m)].cbegin();
}

class configure {
    std::reference_wrapper<vector_search::vector_store_client> vs_ref;

public:
    explicit configure(vector_search::vector_store_client& vs)
        : vs_ref(vs) {
        with_dns_refresh_interval(seconds(2));
        with_wait_for_client_timeout(milliseconds(100));
        with_dns_resolver([](auto const& host) -> future<std::optional<inet_address>> {
            co_return inet_address("127.0.0.1");
        });
    }

    configure& with_dns_refresh_interval(milliseconds interval) {
        vector_store_client_tester::set_dns_refresh_interval(vs_ref.get(), interval);
        return *this;
    }

    configure& with_wait_for_client_timeout(milliseconds timeout) {
        vector_store_client_tester::set_wait_for_client_timeout(vs_ref.get(), timeout);
        return *this;
    }

    configure& with_dns(std::map<std::string, std::optional<std::string>> dns_) {
        vector_store_client_tester::set_dns_resolver(vs_ref.get(), [dns = std::move(dns_)](auto const& host) -> future<std::vector<inet_address>> {
            auto value = dns.at(host);
            if (value) {
                co_return std::vector<inet_address>{inet_address(*value)};
            }
            co_return std::vector<inet_address>{};
        });
        return *this;
    }

    configure& with_dns(std::map<std::string, std::vector<std::string>> dns) {
        vector_store_client_tester::set_dns_resolver(vs_ref.get(), [dns = std::move(dns)](auto const& host) -> future<std::vector<inet_address>> {
            std::vector<inet_address> ret;
            for (auto const& ip : dns.at(host)) {
                ret.push_back(inet_address(ip));
            }
            co_return ret;
        });
        return *this;
    }

    configure& with_dns_resolver(std::function<future<std::optional<inet_address>>(sstring const&)> resolver) {
        vector_store_client_tester::set_dns_resolver(vs_ref.get(), [r = std::move(resolver)](auto host) -> future<std::vector<inet_address>> {
            auto addr = co_await r(host);
            if (addr) {
                co_return std::vector<inet_address>{*addr};
            }
            co_return std::vector<inet_address>{};
        });
        return *this;
    }
};

class unavailable_server {
    struct Connection {
        lowres_clock::time_point timestamp;
        connected_socket socket;
    };

public:
    explicit unavailable_server(uint16_t port)
        : _port(port) {
    }

    future<> start() {
        co_await listen();
        (void)try_with_gate(_gate, [this] {
            return run();
        });
    }

    future<> stop() {
        if (_socket) {
            _socket.abort_accept();
            co_await _gate.close();
        }
    }

    sstring host() const {
        return _host;
    }

    uint16_t port() const {
        return _port;
    }

    const std::vector<Connection>& connections() const {
        return _connections;
    }

    future<seastar::server_socket> take_socket() {
        _running = false;
        // Make a connection to unblock accept() in run loop.
        co_await seastar::connect(socket_address(net::inet_address(_host), _port));
        co_await _gate.close();
        co_return std::move(_socket);
    }

    void auto_shutdown_off() {
        _auto_shutdown = false;
    }

    future<> shutdown_all_and_clear() {
        std::vector<Connection> tmp;
        std::swap(tmp, _connections);
        for (auto& conn : tmp) {
            co_await shutdown(conn.socket);
        }
    }

private:
    future<> listen() {
        co_await try_on_loopback_address([this](auto host) -> future<> {
            ::listen_options opts;
            opts.set_fixed_cpu(this_shard_id());
            _socket = seastar::listen(socket_address(net::inet_address(host), _port), opts);
            _port = _socket.local_address().port();
            _host = std::move(host);
            return make_ready_future<>();
        });
    }

    future<> run() {
        while (_running) {
            try {
                auto result = co_await _socket.accept();
                _connections.push_back(Connection{.timestamp = lowres_clock::now(), .socket = std::move(result.connection)});
                if (_auto_shutdown) {
                    co_await shutdown(_connections.back().socket);
                }
            } catch (...) {
                break;
            }
        }
    }

    future<> shutdown(connected_socket& cs) {
        cs.shutdown_output();
        cs.shutdown_input();
        co_await cs.wait_input_shutdown();
    }


    seastar::server_socket _socket;
    seastar::gate _gate;
    uint16_t _port;
    sstring _host;
    std::vector<Connection> _connections;
    bool _running = true;
    bool _auto_shutdown = true;
};

auto make_unavailable_server(uint16_t port = 0) -> future<std::unique_ptr<unavailable_server>> {
    auto ret = std::make_unique<unavailable_server>(port);
    co_await ret->start();
    co_return ret;
}

} // namespace

BOOST_AUTO_TEST_CASE(vector_store_client_test_ctor) {
    auto cfg = config();
    cfg.vector_store_primary_uri.set("http://bad,authority.com:6080");
    BOOST_CHECK_THROW(vector_store_client{cfg}, configuration_exception);
    cfg.vector_store_primary_uri.set("bad-schema://authority.com:6080");
    BOOST_CHECK_THROW(vector_store_client{cfg}, configuration_exception);
    cfg.vector_store_primary_uri.set("http://bad.port.com:a6080");
    BOOST_CHECK_THROW(vector_store_client{cfg}, configuration_exception);
    cfg.vector_store_primary_uri.set("http://bad.port.com:60806080");
    BOOST_CHECK_THROW(vector_store_client{cfg}, configuration_exception);
    cfg.vector_store_primary_uri.set("http://bad.format.com:60:80");
    BOOST_CHECK_THROW(vector_store_client{cfg}, configuration_exception);
    cfg.vector_store_primary_uri.set("http://authority.com:6080/bad/path");
    BOOST_CHECK_THROW(vector_store_client{cfg}, configuration_exception);
}

/// Resolving of the hostname is started in start_background_tasks()
SEASTAR_TEST_CASE(vector_store_client_test_dns_started) {
    auto cfg = config();
    cfg.vector_store_primary_uri.set("http://good.authority.here:6080");
    auto vs = vector_store_client{cfg};
    configure(vs).with_dns({{"good.authority.here", "127.0.0.1"}});
    auto as = abort_source_timeout();

    vs.start_background_tasks();

    auto addr = co_await vector_store_client_tester::resolve_hostname(vs, as.reset());
    BOOST_REQUIRE(!addr.empty());
    BOOST_CHECK_EQUAL(print_addr(addr[0]), "127.0.0.1");

    co_await vs.stop();
}

/// Unable to resolve the hostname
SEASTAR_TEST_CASE(vector_store_client_test_dns_resolve_failure) {
    auto cfg = config();
    cfg.vector_store_primary_uri.set("http://good.authority.here:6080");
    auto as = abort_source_timeout();
    auto vs = vector_store_client{cfg};
    configure(vs).with_dns({{"good.authority.here", std::nullopt}});

    vs.start_background_tasks();

    auto addrs = co_await vector_store_client_tester::resolve_hostname(vs, as.reset());

    BOOST_CHECK(addrs.empty());

    co_await vs.stop();
}

/// Resolving of the hostname is repeated after errors
SEASTAR_TEST_CASE(vector_store_client_test_dns_resolving_repeated) {
    auto cfg = config();
    cfg.vector_store_primary_uri.set("http://good.authority.here:6080");
    auto vs = vector_store_client{cfg};
    auto count = 0;
    auto as = abort_source_timeout();
    configure(vs)
            .with_dns_refresh_interval(milliseconds(10))
            .with_wait_for_client_timeout(milliseconds(20))
            .with_dns_resolver([&count](auto const& host) -> future<std::optional<inet_address>> {
                BOOST_CHECK_EQUAL(host, "good.authority.here");
                count++;
                if (count % 3 != 0) {
                    co_return std::nullopt;
                }
                co_return inet_address(format("127.0.0.{}", count));
            });

    vs.start_background_tasks();

    BOOST_CHECK(co_await repeat_until(seconds(1), [&vs, &as]() -> future<bool> {
        auto addrs = co_await vector_store_client_tester::resolve_hostname(vs, as.reset());
        co_return addrs.size() == 1;
    }));
    BOOST_CHECK_EQUAL(count, 3);
    auto addrs = co_await vector_store_client_tester::resolve_hostname(vs, as.reset());
    BOOST_REQUIRE_EQUAL(addrs.size(), 1);
    BOOST_CHECK_EQUAL(print_addr(addrs[0]), "127.0.0.3");

    vector_store_client_tester::trigger_dns_resolver(vs);

    BOOST_CHECK(co_await repeat_until(seconds(1), [&vs, &as]() -> future<bool> {
        auto addrs = co_await vector_store_client_tester::resolve_hostname(vs, as.reset());
        co_return addrs.empty();
    }));

    BOOST_CHECK(co_await repeat_until(seconds(1), [&vs, &as]() -> future<bool> {
        auto addrs = co_await vector_store_client_tester::resolve_hostname(vs, as.reset());
        co_return addrs.size() == 1;
    }));
    BOOST_CHECK_EQUAL(count, 6);
    addrs = co_await vector_store_client_tester::resolve_hostname(vs, as.reset());

    BOOST_REQUIRE_EQUAL(addrs.size(), 1);
    BOOST_CHECK_EQUAL(print_addr(addrs[0]), "127.0.0.6");

    co_await vs.stop();
}

/// Minimal interval between DNS refreshes is respected
SEASTAR_TEST_CASE(vector_store_client_test_dns_refresh_respects_interval) {
    auto cfg = config();
    cfg.vector_store_primary_uri.set("http://good.authority.here:6080");
    auto vs = vector_store_client{cfg};
    auto count = 0;
    auto as = abort_source_timeout();
    configure(vs).with_dns_refresh_interval(milliseconds(10)).with_dns_resolver([&count](auto const& host) -> future<std::optional<inet_address>> {
        BOOST_CHECK_EQUAL(host, "good.authority.here");
        count++;
        co_return inet_address("127.0.0.1");
    });

    vs.start_background_tasks();
    co_await sleep(milliseconds(20)); // wait for the first DNS refresh

    auto addrs = co_await vector_store_client_tester::resolve_hostname(vs, as.reset());
    BOOST_REQUIRE_EQUAL(addrs.size(), 1);
    BOOST_CHECK_EQUAL(print_addr(addrs[0]), "127.0.0.1");
    BOOST_CHECK_EQUAL(count, 1);
    count = 0;
    vector_store_client_tester::trigger_dns_resolver(vs);
    vector_store_client_tester::trigger_dns_resolver(vs);
    vector_store_client_tester::trigger_dns_resolver(vs);
    vector_store_client_tester::trigger_dns_resolver(vs);
    vector_store_client_tester::trigger_dns_resolver(vs);
    co_await sleep(milliseconds(100)); // wait for the next DNS refresh

    addrs = co_await vector_store_client_tester::resolve_hostname(vs, as.reset());
    BOOST_REQUIRE_EQUAL(addrs.size(), 1);
    BOOST_CHECK_EQUAL(print_addr(addrs[0]), "127.0.0.1");
    BOOST_CHECK_GE(count, 1);
    BOOST_CHECK_LE(count, 2);

    co_await vs.stop();
}

/// DNS refresh could be aborted
SEASTAR_TEST_CASE(vector_store_client_test_dns_refresh_aborted) {
    auto cfg = config();
    cfg.vector_store_primary_uri.set("http://good.authority.here:6080");
    seastar::condition_variable wait_for_abort;
    auto as = abort_source_timeout(milliseconds(10));
    auto vs = vector_store_client{cfg};
    configure(vs).with_dns_refresh_interval(milliseconds(10)).with_dns_resolver([&](auto const& host) -> future<std::optional<inet_address>> {
        BOOST_CHECK_EQUAL(host, "good.authority.here");
        co_await wait_for_abort.when();
        co_return inet_address("127.0.0.1");
    });

    vs.start_background_tasks();

    auto addrs = co_await vector_store_client_tester::resolve_hostname(vs, as.get());
    BOOST_CHECK(addrs.empty());
    wait_for_abort.signal();

    co_await vs.stop();
}

SEASTAR_TEST_CASE(vector_store_client_ann_test_disabled) {
    co_await do_with_cql_env([](cql_test_env& env) -> future<> {
        auto as = abort_source_timeout();
        auto schema = co_await create_test_table(env, "ks", "vs");
        auto& vs = env.local_qp().vector_store_client();

        auto keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.reset());
        BOOST_REQUIRE(!keys);
        BOOST_CHECK(std::holds_alternative<vector_store_client::disabled>(keys.error()));
    });
}

SEASTAR_TEST_CASE(vector_store_client_test_ann_addr_unavailable) {
    auto cfg = cql_test_config();
    cfg.db_config->vector_store_primary_uri.set("http://bad.authority.here:6080");
    co_await do_with_cql_env(
            [](cql_test_env& env) -> future<> {
                auto schema = co_await create_test_table(env, "ks", "vs");
                auto as = abort_source_timeout();
                auto& vs = env.local_qp().vector_store_client();
                configure(vs).with_dns_refresh_interval(seconds(1)).with_dns({{"bad.authority.here", std::nullopt}});

                vs.start_background_tasks();

                auto keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.reset());
                BOOST_REQUIRE(!keys);
                BOOST_CHECK(std::holds_alternative<vector_store_client::addr_unavailable>(keys.error()));
            },
            cfg);
}

SEASTAR_TEST_CASE(vector_store_client_test_ann_service_unavailable) {
    auto cfg = cql_test_config();
    auto server = co_await make_unavailable_server();
    cfg.db_config->vector_store_primary_uri.set(format("http://good.authority.here:{}", server->port()));
    co_await do_with_cql_env(
            [&server](cql_test_env& env) -> future<> {
                auto schema = co_await create_test_table(env, "ks", "vs");
                auto as = abort_source_timeout();
                auto& vs = env.local_qp().vector_store_client();
                configure(vs).with_dns_refresh_interval(seconds(1)).with_dns({{"good.authority.here", server->host()}});

                vs.start_background_tasks();

                auto keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.reset());
                BOOST_REQUIRE(!keys);
                BOOST_CHECK(std::holds_alternative<vector_store_client::service_unavailable>(keys.error()));
            },
            cfg)
            .finally([&server] {
                return server->stop();
            });
}

SEASTAR_TEST_CASE(vector_store_client_test_ann_service_aborted) {
    auto cfg = cql_test_config();
    auto server = co_await make_unavailable_server();
    cfg.db_config->vector_store_primary_uri.set(format("http://good.authority.here:{}", server->port()));
    co_await do_with_cql_env(
            [&server](cql_test_env& env) -> future<> {
                auto schema = co_await create_test_table(env, "ks", "vs");
                auto as = abort_source_timeout();
                auto& vs = env.local_qp().vector_store_client();
                configure(vs).with_dns_refresh_interval(milliseconds(10)).with_dns_resolver([&server](auto const& host) -> future<std::optional<inet_address>> {
                    BOOST_CHECK_EQUAL(host, "good.authority.here");
                    co_await sleep(milliseconds(100));
                    co_return inet_address(server->host());
                });

                vs.start_background_tasks();

                auto keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.reset(milliseconds(10)));

                BOOST_REQUIRE(!keys);
                BOOST_CHECK(std::holds_alternative<vector_store_client::aborted>(keys.error()));
            },
            cfg)
            .finally([&server] {
                return server->stop();
            });
}

SEASTAR_TEST_CASE(vector_store_client_test_ann_request) {
    auto server = co_await make_vs_mock_server();
    auto cfg = cql_test_config();
    cfg.db_config->vector_store_primary_uri.set(format("http://good.authority.here:{}", server->port()));
    co_await do_with_cql_env(
            [&server](cql_test_env& env) -> future<> {
                auto schema = co_await create_test_table(env, "ks", "idx");
                auto as = abort_source_timeout();
                auto& vs = env.local_qp().vector_store_client();
                configure(vs).with_dns_refresh_interval(seconds(1)).with_dns({{"good.authority.here", "127.0.0.1"}});

                vs.start_background_tasks();

                // server responds with 404 - client should return service_error
                server->next_ann_response({status_type::not_found, "idx2 not found"});
                auto keys = co_await vs.ann("ks", "idx2", schema, std::vector<float>{0.3, 0.2, 0.1}, 1, as.reset());
                BOOST_REQUIRE(!server->requests().empty());
                BOOST_REQUIRE_EQUAL(server->requests().back().body, R"({"vector":[0.3,0.2,0.1],"limit":1})");
                BOOST_REQUIRE_EQUAL(server->requests().back().path, "/api/v1/indexes/ks/idx2/ann");
                BOOST_REQUIRE(!keys);
                auto* err = std::get_if<vector_store_client::service_error>(&keys.error());
                BOOST_CHECK(err != nullptr);
                BOOST_CHECK_EQUAL(err->status, status_type::not_found);

                // missing primary_keys in the reply - service should return format error
                server->next_ann_response({status_type::ok, R"({"primary_keys1":{"pk1":[5,6],"pk2":[7,8],"ck1":[9,1],"ck2":[2,3]},"distances":[0.1,0.2]})"});
                keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.reset());
                BOOST_REQUIRE(!server->requests().empty());
                BOOST_REQUIRE_EQUAL(server->requests().back().body, R"({"vector":[0.1,0.2,0.3],"limit":2})");
                BOOST_REQUIRE_EQUAL(server->requests().back().path, "/api/v1/indexes/ks/idx/ann");
                BOOST_REQUIRE(!keys);
                BOOST_CHECK(std::holds_alternative<vector_store_client::service_reply_format_error>(keys.error()));

                // missing distances in the reply - service should return format error
                server->next_ann_response({status_type::ok, R"({"primary_keys":{"pk1":[5,6],"pk2":[7,8],"ck1":[9,1],"ck2":[2,3]},"distances1":[0.1,0.2]})"});
                keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.reset());
                BOOST_REQUIRE(!keys);
                BOOST_CHECK(std::holds_alternative<vector_store_client::service_reply_format_error>(keys.error()));

                // missing pk1 key in the reply - service should return format error
                server->next_ann_response({status_type::ok, R"({"primary_keys":{"pk11":[5,6],"pk2":[7,8],"ck1":[9,1],"ck2":[2,3]},"distances":[0.1,0.2]})"});
                keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.reset());
                BOOST_REQUIRE(!keys);
                BOOST_CHECK(std::holds_alternative<vector_store_client::service_reply_format_error>(keys.error()));

                // missing ck1 key in the reply - service should return format error
                server->next_ann_response({status_type::ok, R"({"primary_keys":{"pk1":[5,6],"pk2":[7,8],"ck11":[9,1],"ck2":[2,3]},"distances":[0.1,0.2]})"});
                keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.reset());
                BOOST_REQUIRE(!keys);
                BOOST_CHECK(std::holds_alternative<vector_store_client::service_reply_format_error>(keys.error()));

                // wrong size of pk2 key in the reply - service should return format error
                server->next_ann_response({status_type::ok, R"({"primary_keys":{"pk1":[5,6],"pk2":[7],"ck1":[9,1],"ck2":[2,3]},"distances":[0.1,0.2]})"});
                keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.reset());
                BOOST_REQUIRE(!keys);
                BOOST_CHECK(std::holds_alternative<vector_store_client::service_reply_format_error>(keys.error()));

                // wrong size of ck2 key in the reply - service should return format error
                server->next_ann_response({status_type::ok, R"({"primary_keys":{"pk1":[5,6],"pk2":[7,8],"ck1":[9,1],"ck2":[2]},"distances":[0.1,0.2]})"});
                keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.reset());
                BOOST_REQUIRE(!keys);
                BOOST_CHECK(std::holds_alternative<vector_store_client::service_reply_format_error>(keys.error()));

                // correct reply - service should return keys
                server->next_ann_response({status_type::ok, CORRECT_RESPONSE_FOR_TEST_TABLE});
                keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.reset());
                BOOST_REQUIRE(keys);
                BOOST_REQUIRE_EQUAL(keys->size(), 2);
                BOOST_CHECK_EQUAL(seastar::format("{}", keys->at(0).partition.key().explode()), "[05, 07]");
                BOOST_CHECK_EQUAL(seastar::format("{}", keys->at(0).clustering.explode()), "[09, 02]");
                BOOST_CHECK_EQUAL(seastar::format("{}", keys->at(1).partition.key().explode()), "[06, 08]");
                BOOST_CHECK_EQUAL(seastar::format("{}", keys->at(1).clustering.explode()), "[01, 03]");
            },
            cfg)
            .finally([&server] {
                return server->stop();
            });
}

SEASTAR_TEST_CASE(vector_store_client_uri_update_to_empty) {
    auto cfg = config();
    auto count = 0;
    cfg.vector_store_primary_uri.set("http://good.authority.here:6080");
    auto vs = vector_store_client{cfg};
    auto DNS_REFRESH_INTERVAL = milliseconds(5);
    configure(vs).with_dns_refresh_interval(DNS_REFRESH_INTERVAL).with_dns_resolver([&count](auto const& host) -> future<std::optional<inet_address>> {
        count++;
        co_return inet_address(format("127.0.0.0"));
    });
    vs.start_background_tasks();

    // Wait for initial DNS resolution
    BOOST_CHECK(co_await repeat_until([&]() -> future<bool> {
        co_return count > 0;
    }));

    cfg.vector_store_primary_uri.set("");
    vector_store_client_tester::trigger_dns_resolver(vs);
    co_await sleep(DNS_REFRESH_INTERVAL * 2); // wait for the next DNS refresh

    BOOST_CHECK(vs.is_disabled());
    // DNS is not resolved again, after URI is set to empty
    BOOST_CHECK_EQUAL(count, 1);

    co_await vs.stop();
}

SEASTAR_TEST_CASE(vector_store_client_uri_update_to_non_empty) {
    auto cfg = config();
    std::vector<std::string> resolved;
    auto vs = vector_store_client{cfg};
    configure(vs).with_dns_refresh_interval(milliseconds(10)).with_dns_resolver([&resolved](auto const& host) -> future<std::optional<inet_address>> {
        resolved.push_back(host);
        co_return inet_address("127.0.0.1");
    });

    vs.start_background_tasks();

    cfg.vector_store_primary_uri.set("http://good.authority.here:6080");

    BOOST_CHECK(!vs.is_disabled());
    // Wait for the DNS resolver to be called
    BOOST_CHECK(co_await repeat_until(std::chrono::seconds(1), [&]() -> future<bool> {
        co_return resolved.size() > 0;
    }));
    BOOST_CHECK_EQUAL(resolved.back(), "good.authority.here");
    co_await vs.stop();
}

SEASTAR_TEST_CASE(vector_store_client_uri_update_to_invalid) {
    auto cfg = config();
    cfg.vector_store_primary_uri.set("http://good.authority.here:6080");
    auto vs = vector_store_client{cfg};
    configure{vs};

    vs.start_background_tasks();

    cfg.vector_store_primary_uri.set("invalid-uri");

    // vs becomes disabled
    BOOST_CHECK(vs.is_disabled());
    co_await vs.stop();
}

SEASTAR_TEST_CASE(vector_store_client_uri_update) {
    // Test verifies that when vector store uri is update, the client
    // will switch to the new uri within the DNS refresh interval.
    // To avoid race condition we wait twice long as DNS refresh interval before checking the result.
    auto s1 = co_await make_vs_mock_server(vs_mock_server::ann_resp(status_type::not_found, "Not found"));
    auto s2 = co_await make_vs_mock_server(vs_mock_server::ann_resp(status_type::service_unavailable, "Service unavailable"));

    constexpr auto is_s2_response = [](const auto& keys) -> bool {
        return !keys && std::holds_alternative<vector_store_client::service_error>(keys.error()) &&
               std::get<vector_store_client::service_error>(keys.error()).status == status_type::service_unavailable;
    };

    auto cfg = cql_test_config();
    cfg.db_config->vector_store_primary_uri.set(format("http://good.authority.here:{}", s1->port()));
    co_await do_with_cql_env(
            [&](cql_test_env& env) -> future<> {
                auto as = abort_source_timeout();
                auto schema = co_await create_test_table(env, "ks", "idx");
                auto& vs = env.local_qp().vector_store_client();
                constexpr auto DNS_REFRESH_INTERVAL = std::chrono::milliseconds(10);
                configure(vs).with_dns_refresh_interval(DNS_REFRESH_INTERVAL).with_dns({{"good.authority.here", "127.0.0.1"}});

                vs.start_background_tasks();

                env.db_config().vector_store_primary_uri.set(format("http://good.authority.here:{}", s2->port()));

                // Wait until requests are handled by s2
                BOOST_CHECK(co_await repeat_until(DNS_REFRESH_INTERVAL * 2, [&]() -> future<bool> {
                    co_return is_s2_response(co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.reset()));
                }));
            },
            cfg)
            .finally(seastar::coroutine::lambda([&s1, &s2] -> future<> {
                co_await s1->stop();
                co_await s2->stop();
            }));
}

SEASTAR_TEST_CASE(vector_store_client_multiple_ips_high_availability) {

    auto responding_s = co_await make_vs_mock_server();
    auto unavail_s = co_await make_unavailable_server(responding_s->port());

    auto cfg = cql_test_config();
    cfg.db_config->vector_store_primary_uri.set(format("http://good.authority.here:{}", responding_s->port()));
    co_await do_with_cql_env(
            [&](cql_test_env& env) -> future<> {
                auto as = abort_source_timeout();
                auto schema = co_await create_test_table(env, "ks", "idx");
                auto& vs = env.local_qp().vector_store_client();
                configure(vs).with_dns({{"good.authority.here", std::vector<std::string>{unavail_s->host(), responding_s->host()}}});
                vs.start_background_tasks();
                std::expected<vector_store_client::primary_keys, vector_store_client::ann_error> keys;

                // Because requests are distributed in random order due to load balancing,
                // repeat the ANN query until the unavailable server is queried.
                BOOST_CHECK(co_await repeat_until([&]() -> future<bool> {
                    keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.reset());
                    co_return unavail_s->connections().size() > 1;
                }));

                // The query is successful because the client falls back to the available server
                // when the attempt to connect to the unavailable one fails.
                BOOST_CHECK(keys);
            },
            cfg)
            .finally(seastar::coroutine::lambda([&responding_s, &unavail_s] -> future<> {
                co_await responding_s->stop();
                co_await unavail_s->stop();
            }));
}

SEASTAR_TEST_CASE(vector_store_client_multiple_ips_load_balancing) {

    auto s1 = co_await make_vs_mock_server();
    auto s2 = co_await make_vs_mock_server(s1->port());

    auto cfg = cql_test_config();
    cfg.db_config->vector_store_primary_uri.set(format("http://good.authority.here:{}", s1->port()));
    co_await do_with_cql_env(
            [&](cql_test_env& env) -> future<> {
                auto as = abort_source_timeout();
                auto schema = co_await create_test_table(env, "ks", "idx");
                auto& vs = env.local_qp().vector_store_client();
                configure(vs).with_dns({{"good.authority.here", std::vector<std::string>{s1->host(), s2->host()}}});
                vs.start_background_tasks();

                // Wait until requests are handled by both servers.
                // The load balancing algorithm is random, so we send requests in a loop
                // until both servers have received at least one, verifying that load is distributed.
                BOOST_CHECK(co_await repeat_until([&]() -> future<bool> {
                    co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.reset());
                    co_return !s1->requests().empty() && !s2->requests().empty();
                }));
            },
            cfg)
            .finally(seastar::coroutine::lambda([&s1, &s2] -> future<> {
                co_await s1->stop();
                co_await s2->stop();
            }));
}

SEASTAR_TEST_CASE(vector_store_client_multiple_uris_high_availability) {

    auto responding_s = co_await make_vs_mock_server();
    auto unavail_s = co_await make_unavailable_server();

    auto cfg = cql_test_config();
    cfg.db_config->vector_store_primary_uri.set(format("http://s1.node:{},http://s2.node:{}", unavail_s->port(), responding_s->port()));
    co_await do_with_cql_env(
            [&](cql_test_env& env) -> future<> {
                auto as = abort_source_timeout();
                auto schema = co_await create_test_table(env, "ks", "idx");
                auto& vs = env.local_qp().vector_store_client();
                configure(vs).with_dns({{"s1.node", std::vector<std::string>{unavail_s->host()}}, {"s2.node", std::vector<std::string>{responding_s->host()}}});
                vs.start_background_tasks();
                std::expected<vector_store_client::primary_keys, vector_store_client::ann_error> keys;

                // Because requests are distributed in random order due to load balancing,
                // repeat the ANN query until the unavailable server is queried.
                BOOST_CHECK(co_await repeat_until([&]() -> future<bool> {
                    keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.reset());
                    co_return unavail_s->connections().size() > 1;
                }));

                // The query is successful because the client falls back to the available server
                // when the attempt to connect to the unavailable one fails.
                BOOST_CHECK(keys);
            },
            cfg)
            .finally(seastar::coroutine::lambda([&responding_s, &unavail_s] -> future<> {
                co_await responding_s->stop();
                co_await unavail_s->stop();
            }));
}

SEASTAR_TEST_CASE(vector_store_client_multiple_uris_load_balancing) {

    auto s1 = co_await make_vs_mock_server();
    auto s2 = co_await make_vs_mock_server();

    auto cfg = cql_test_config();
    cfg.db_config->vector_store_primary_uri.set(format("http://s1.node:{},http://s2.node:{}", s1->port(), s2->port()));
    co_await do_with_cql_env(
            [&](cql_test_env& env) -> future<> {
                auto as = abort_source_timeout();
                auto schema = co_await create_test_table(env, "ks", "idx");
                auto& vs = env.local_qp().vector_store_client();
                configure(vs).with_dns({{"s1.node", std::vector<std::string>{s1->host()}}, {"s2.node", std::vector<std::string>{s2->host()}}});
                vs.start_background_tasks();

                // Wait until requests are handled by both servers.
                // The load balancing algorithm is random, so we send requests in a loop
                // until both servers have received at least one, verifying that load is distributed.
                BOOST_CHECK(co_await repeat_until([&]() -> future<bool> {
                    co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.reset());
                    co_return !s1->requests().empty() && !s2->requests().empty();
                }));
            },
            cfg)
            .finally(seastar::coroutine::lambda([&s1, &s2] -> future<> {
                co_await s1->stop();
                co_await s2->stop();
            }));
}

SEASTAR_TEST_CASE(vector_search_metrics_test) {

    auto cfg = cql_test_config();
    cfg.db_config->vector_store_primary_uri.set("http://good.authority.here:6080");
    co_await do_with_cql_env(
            [](cql_test_env& env) -> future<> {
                auto as = abort_source_timeout();
                auto schema = co_await create_test_table(env, "ks", "test");
                auto result = co_await env.execute_cql("CREATE CUSTOM INDEX idx ON ks.test (embedding) USING 'vector_index'");
                result.get()->throw_if_exception();
                auto& vs = env.local_qp().vector_store_client();
                configure{vs};
                vs.start_background_tasks();

                co_await vector_store_client_tester::resolve_hostname(vs, as.reset());

                auto metrics = seastar::metrics::impl::get_values();
                BOOST_CHECK_EQUAL(get_metrics_value("vector_store_dns_refreshes", metrics)->i(), 1);
            },
            cfg);
}

SEASTAR_TEST_CASE(vector_store_client_test_paging_warning) {
    auto s1 = co_await make_vs_mock_server();

    auto cfg = cql_test_config();
    cfg.db_config->vector_store_primary_uri.set(format("http://s1.node:{}", s1->port()));
    co_await do_with_cql_env(
            [&s1](cql_test_env& env) -> future<> {
                auto schema = co_await create_test_table(env, "ks", "test");
                auto& vs = env.local_qp().vector_store_client();
                configure(vs).with_dns({{"s1.node", std::vector<std::string>{s1->host()}}});

                vs.start_background_tasks();
                auto result = co_await env.execute_cql("CREATE CUSTOM INDEX idx ON ks.test (embedding) USING 'vector_index'");
                auto qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                        cql3::query_options::specific_options{5, nullptr, {}, api::new_timestamp()});
                auto msg = co_await env.execute_cql("SELECT * FROM ks.test ORDER BY embedding ANN OF [0.1, 0.2, 0.3] LIMIT 100;", std::move(qo));
                auto warns = msg->warnings();
                BOOST_REQUIRE_EQUAL(warns.size(), 1);
                BOOST_CHECK(warns[0] == "Paging is not supported for Vector Search queries. The entire result set has been returned.");
            },
            cfg)
            .finally([&s1] {
                return s1->stop();
            });
}

SEASTAR_TEST_CASE(vector_store_client_test_paging_warning_doesnt_show_when_paging_disabled) {
    auto s1 = co_await make_vs_mock_server();

    auto cfg = cql_test_config();
    cfg.db_config->vector_store_primary_uri.set(format("http://s1.node:{}", s1->port()));
    co_await do_with_cql_env(
            [&s1](cql_test_env& env) -> future<> {
                auto schema = co_await create_test_table(env, "ks", "test");
                auto& vs = env.local_qp().vector_store_client();
                configure(vs).with_dns({{"s1.node", std::vector<std::string>{s1->host()}}});

                vs.start_background_tasks();
                auto result = co_await env.execute_cql("CREATE CUSTOM INDEX idx ON ks.test (embedding) USING 'vector_index'");
                auto qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                        cql3::query_options::specific_options{0, nullptr, {}, api::new_timestamp()});
                auto msg = co_await env.execute_cql("SELECT * FROM ks.test ORDER BY embedding ANN OF [0.1, 0.2, 0.3] LIMIT 100;", std::move(qo));
                auto warns = msg->warnings();
                BOOST_REQUIRE_EQUAL(warns.size(), 0);
            },
            cfg)
            .finally([&s1] {
                return s1->stop();
            });
}

SEASTAR_TEST_CASE(vector_store_client_test_paging_warning_doesnt_show_when_limit_less_than_page_size) {
    auto s1 = co_await make_vs_mock_server();

    auto cfg = cql_test_config();
    cfg.db_config->vector_store_primary_uri.set(format("http://s1.node:{}", s1->port()));
    co_await do_with_cql_env(
            [&s1](cql_test_env& env) -> future<> {
                auto schema = co_await create_test_table(env, "ks", "test");
                auto& vs = env.local_qp().vector_store_client();
                configure(vs).with_dns({{"s1.node", std::vector<std::string>{s1->host()}}});

                vs.start_background_tasks();
                auto result = co_await env.execute_cql("CREATE CUSTOM INDEX idx ON ks.test (embedding) USING 'vector_index'");
                auto qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                        cql3::query_options::specific_options{100, nullptr, {}, api::new_timestamp()});
                auto msg = co_await env.execute_cql("SELECT * FROM ks.test ORDER BY embedding ANN OF [0.1, 0.2, 0.3] LIMIT 5;", std::move(qo));
                auto warns = msg->warnings();
                BOOST_REQUIRE_EQUAL(warns.size(), 0);
            },
            cfg)
            .finally([&s1] {
                return s1->stop();
            });
}

SEASTAR_TEST_CASE(vector_store_client_node_recovery_after_backoff) {
    auto unavail_server = co_await make_unavailable_server();
    std::unique_ptr<vs_mock_server> avail_server;
    constexpr auto HOSTNAME = "server.node";

    auto cfg = cql_test_config();
    cfg.db_config->vector_store_primary_uri.set(format("http://{}:{}", HOSTNAME, unavail_server->port()));
    co_await do_with_cql_env(
            [&](cql_test_env& env) -> future<> {
                auto as = abort_source_timeout();
                auto schema = co_await create_test_table(env, "ks", "idx");
                auto& vs = env.local_qp().vector_store_client();
                configure(vs).with_dns({{HOSTNAME, std::vector<std::string>{unavail_server->host()}}});
                vs.start_background_tasks();

                // Send request to unavailable node - this will put the node to backoff.
                auto result = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.reset());

                BOOST_CHECK(!result);
                BOOST_CHECK(std::holds_alternative<vector_store_client::service_unavailable>(result.error()));

                // Replace the unavailable server with an available one.
                avail_server = std::make_unique<vs_mock_server>();
                co_await avail_server->start(co_await unavail_server->take_socket());

                // Wait until node is taken out of the backoff state and used for requests again.
                BOOST_CHECK(co_await repeat_until([&]() -> future<bool> {
                    auto result = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.reset());
                    co_return result.has_value();
                }));
            },
            cfg)
            .finally(coroutine::lambda([&] -> future<> {
                co_await unavail_server->stop();
                if (avail_server) {
                    co_await avail_server->stop();
                }
            }));
}

SEASTAR_TEST_CASE(vector_store_client_single_status_check_after_concurrent_failures) {
    using keys = std::expected<vector_store_client::primary_keys, vector_store_client::ann_error>;

    auto unavail_s = co_await make_unavailable_server();
    auto cfg = cql_test_config();
    cfg.db_config->vector_store_primary_uri.set(format("http://unavail.node:{}", unavail_s->port()));
    co_await do_with_cql_env(
            [&](cql_test_env& env) -> future<> {
                std::vector<future<keys>> requests;
                unavail_s->auto_shutdown_off();
                constexpr auto NUM_OF_PARALLEL_REQUESTS = 50;
                auto as = abort_source_timeout();
                auto schema = co_await create_test_table(env, "ks", "idx");
                auto& vs = env.local_qp().vector_store_client();
                configure(vs).with_dns({{"unavail.node", std::vector<std::string>{unavail_s->host()}}});
                vs.start_background_tasks();

                for (int i = 0; i < NUM_OF_PARALLEL_REQUESTS; ++i) {
                    requests.push_back(vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.reset()));
                }
                // Wait for all requests to establish a connection with the server.
                co_await repeat_until([&unavail_s]() -> future<bool> {
                    co_return unavail_s->connections().size() == NUM_OF_PARALLEL_REQUESTS;
                });
                // Shutdown all connections, causing all requests to fail.
                // The number of connections will drop to zero.
                co_await unavail_s->shutdown_all_and_clear();
                // Wait for all requests to complete.
                co_await when_all(requests.begin(), requests.end());

                // After the backoff period, a single status check is expected to verify node recovery.
                // The test server keeps the subsequent status check connection open (auto_shutdown_off()).
                // This prevents the client's backoff mechanism from sending another status request
                // while the first one is pending, ensuring that exactly one new connection is made.
                // This makes the test assertion deterministic.
                BOOST_CHECK(co_await repeat_until([&]() -> future<bool> {
                    co_return unavail_s->connections().size() == 1;
                }));
            },
            cfg)
            .finally(coroutine::lambda([&] -> future<> {
                co_await unavail_s->stop();
            }));
}

SEASTAR_TEST_CASE(vector_store_client_updates_backoff_max_time_from_read_request_timeout_cfg) {
    auto unavail_s = co_await make_unavailable_server();
    auto cfg = cql_test_config();
    cfg.db_config->vector_store_primary_uri.set(format("http://unavail.node:{}", unavail_s->port()));
    co_await do_with_cql_env(
            [&](cql_test_env& env) -> future<> {
                auto as = abort_source_timeout();
                auto schema = co_await create_test_table(env, "ks", "idx");
                auto& vs = env.local_qp().vector_store_client();
                configure(vs).with_dns({{"unavail.node", std::vector<std::string>{unavail_s->host()}}});
                vs.start_background_tasks();

                // Set request timeout to 100ms, hence max backoff time is 2x100ms = 200ms.
                cfg.db_config->read_request_timeout_in_ms.set(100);
                // Trigger status checking by making ANN request to unavailable server.
                co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.reset());
                co_await repeat_until([&unavail_s]() -> future<bool> {
                    // Wait for 1 ANN request + 4 status check connections (5 total)
                    co_return unavail_s->connections().size() > 4;
                });

                // Verify backoff timing between status check connections.
                // Skip the first connection (ANN request) and analyze status check intervals.
                auto duration_between_1st_and_2nd_status_check = std::chrono::duration_cast<std::chrono::milliseconds>(
                        unavail_s->connections().at(2).timestamp - unavail_s->connections().at(1).timestamp);
                BOOST_CHECK_GE(duration_between_1st_and_2nd_status_check, std::chrono::milliseconds(100));
                BOOST_CHECK_LT(duration_between_1st_and_2nd_status_check, std::chrono::milliseconds(200));
                auto duration_between_2nd_and_3rd_status_check = std::chrono::duration_cast<std::chrono::milliseconds>(
                        unavail_s->connections().at(3).timestamp - unavail_s->connections().at(2).timestamp);
                // Max backoff time reached at 200ms, so subsequent status checks use fixed 200ms intervals.
                BOOST_CHECK_GE(duration_between_2nd_and_3rd_status_check, std::chrono::milliseconds(200)); // 200ms = 100ms * 2
                BOOST_CHECK_LT(duration_between_2nd_and_3rd_status_check, std::chrono::milliseconds(400));
                auto duration_between_3rd_and_4th_status_check = std::chrono::duration_cast<std::chrono::milliseconds>(
                        unavail_s->connections().at(4).timestamp - unavail_s->connections().at(3).timestamp);
                BOOST_CHECK_GE(duration_between_3rd_and_4th_status_check, std::chrono::milliseconds(200));
                BOOST_CHECK_LT(duration_between_3rd_and_4th_status_check, std::chrono::milliseconds(400));
            },
            cfg)
            .finally(coroutine::lambda([&] -> future<> {
                co_await unavail_s->stop();
            }));
}
