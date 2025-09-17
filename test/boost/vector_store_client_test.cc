/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "service/vector_store_client.hh"
#include "db/config.hh"
#include "exceptions/exceptions.hh"
#include "cql3/statements/select_statement.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/log.hh"
#include <functional>
#include <chrono>
#include <memory>
#include <seastar/core/shared_ptr.hh>
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
#include <variant>


namespace {

using namespace seastar;

using vector_store_client = service::vector_store_client;
using vector_store_client_tester = service::vector_store_client_tester;
using config = vector_store_client::config;
using configuration_exception = exceptions::configuration_exception;
using inet_address = seastar::net::inet_address;
using function_handler = httpd::function_handler;
using http_server = httpd::http_server;
using http_server_tester = httpd::http_server_tester;
using milliseconds = std::chrono::milliseconds;
using seconds = std::chrono::seconds;
using operation_type = httpd::operation_type;
using port_number = vector_store_client::port_number;
using reply = http::reply;
using request = http::request;
using routes = httpd::routes;
using status_type = http::reply::status_type;
using url = httpd::url;

constexpr auto const* LOCALHOST = "127.0.0.1";

/// Generate an ephemeral port number for listening on localhost.
/// After closing this socket, the port should be not listened on for a while.
/// This is not guaranteed to be a robust solution, but it should work for most tests.
auto generate_unavailable_localhost_port() -> port_number {
    auto inaddr = net::inet_address(LOCALHOST);
    auto server = listen(socket_address(inaddr, 0));
    auto port = server.local_address().port();
    server.abort_accept();
    return port;
}

auto listen_on_ephemeral_port(std::unique_ptr<http_server> server) -> future<std::tuple<std::unique_ptr<http_server>, socket_address>> {
    auto inaddr = net::inet_address(LOCALHOST);
    auto const addr = socket_address(inaddr, 0);
    ::listen_options opts;
    opts.set_fixed_cpu(this_shard_id());
    co_await server->listen(addr, opts);
    auto const& listeners = http_server_tester::listeners(*server);
    BOOST_CHECK_EQUAL(listeners.size(), 1);
    co_return std::make_tuple(std::move(server), listeners[0].local_address().port());
}

auto new_http_server(std::function<void(routes& r)> set_routes) -> future<std::tuple<std::unique_ptr<http_server>, socket_address>> {
    static unsigned id = 0;
    auto server = std::make_unique<http_server>(fmt::format("test_vector_store_client_{}", id++));
    set_routes(server->_routes);
    server->set_content_streaming(true);
    co_return co_await listen_on_ephemeral_port(std::move(server));
}

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

constexpr auto STANDARD_WAIT = std::chrono::seconds(5);

struct abort_source_timeout {
    abort_source as;
    timer<> t;

    explicit abort_source_timeout(milliseconds timeout = STANDARD_WAIT)
        : t(timer([&]() {
            as.request_abort();
        })) {
        t.arm(timeout);
    }

    void reset(milliseconds timeout = STANDARD_WAIT) {
        t.cancel();
        as = abort_source();
        t.arm(timeout);
    }
};

auto print_addr(const inet_address& addr) -> sstring {
    return format("{}", addr);
}


auto create_test_table(cql_test_env& env, const sstring& ks, const sstring& cf) -> future<schema_ptr> {
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

class configure {
    std::reference_wrapper<service::vector_store_client> vs_ref;

public:
    explicit configure(service::vector_store_client& vs)
        : vs_ref(vs) {
        with_dns_refresh_interval(seconds(2));
        with_wait_for_client_timeout(milliseconds(100));
        with_http_request_retries(3);
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

    configure& with_http_request_retries(int retries) {
        vector_store_client_tester::set_http_request_retries(vs_ref.get(), retries);
        return *this;
    }

    configure& with_dns(std::map<std::string, std::optional<std::string>> dns_) {
        vector_store_client_tester::set_dns_resolver(vs_ref.get(), [dns = std::move(dns_)](auto const& host) -> future<std::optional<inet_address>> {
            auto value = dns.at(host);
            if (value) {
                co_return inet_address(*value);
            }
            co_return std::nullopt;
        });
        return *this;
    }

    configure& with_dns_resolver(std::function<future<std::optional<inet_address>>(sstring const&)> resolver) {
        vector_store_client_tester::set_dns_resolver(vs_ref.get(), std::move(resolver));
        return *this;
    }
};

} // namespace

BOOST_AUTO_TEST_CASE(vector_store_client_test_ctor) {
    {
        auto cfg = config();
        auto vs = vector_store_client{cfg};
        BOOST_CHECK(vs.is_disabled());
        BOOST_CHECK(!vs.host());
        BOOST_CHECK(!vs.port());
    }
    {
        auto cfg = config();
        cfg.vector_store_uri.set("http://good.authority.com:6080");
        auto vs = vector_store_client{cfg};
        BOOST_CHECK(!vs.is_disabled());
        BOOST_CHECK_EQUAL(*vs.host(), "good.authority.com");
        BOOST_CHECK_EQUAL(*vs.port(), 6080);
    }
    {
        auto cfg = config();
        cfg.vector_store_uri.set("http://bad,authority.com:6080");
        BOOST_CHECK_THROW(vector_store_client{cfg}, configuration_exception);
        cfg.vector_store_uri.set("bad-schema://authority.com:6080");
        BOOST_CHECK_THROW(vector_store_client{cfg}, configuration_exception);
        cfg.vector_store_uri.set("http://bad.port.com:a6080");
        BOOST_CHECK_THROW(vector_store_client{cfg}, configuration_exception);
        cfg.vector_store_uri.set("http://bad.port.com:60806080");
        BOOST_CHECK_THROW(vector_store_client{cfg}, configuration_exception);
        cfg.vector_store_uri.set("http://bad.format.com:60:80");
        BOOST_CHECK_THROW(vector_store_client{cfg}, configuration_exception);
        cfg.vector_store_uri.set("http://authority.com:6080/bad/path");
        BOOST_CHECK_THROW(vector_store_client{cfg}, configuration_exception);
    }
}

/// Resolving of the hostname is started in start_background_tasks()
SEASTAR_TEST_CASE(vector_store_client_test_dns_started) {
    auto cfg = config();
    cfg.vector_store_uri.set("http://good.authority.here:6080");
    auto vs = vector_store_client{cfg};
    configure(vs).with_dns({{"good.authority.here", "127.0.0.1"}});

    vs.start_background_tasks();

    auto as = abort_source_timeout();
    auto addr = co_await vector_store_client_tester::resolve_hostname(vs, as.as);
    BOOST_REQUIRE(addr);
    BOOST_CHECK_EQUAL(print_addr(*addr), "127.0.0.1");

    co_await vs.stop();
}

/// Unable to resolve the hostname
SEASTAR_TEST_CASE(vector_store_client_test_dns_resolve_failure) {
    auto cfg = config();
    cfg.vector_store_uri.set("http://good.authority.here:6080");
    auto vs = vector_store_client{cfg};
    configure(vs).with_dns({{"good.authority.here", std::nullopt}});

    vs.start_background_tasks();

    auto as = abort_source_timeout();
    BOOST_CHECK(!co_await vector_store_client_tester::resolve_hostname(vs, as.as));

    co_await vs.stop();
}

/// Resolving of the hostname is repeated after errors
SEASTAR_TEST_CASE(vector_store_client_test_dns_resolving_repeated) {
    auto cfg = config();
    cfg.vector_store_uri.set("http://good.authority.here:6080");
    auto vs = vector_store_client{cfg};
    auto count = 0;
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

    auto as = abort_source_timeout();
    BOOST_CHECK(co_await repeat_until(seconds(1), [&vs, &as]() -> future<bool> {
        co_return co_await vector_store_client_tester::resolve_hostname(vs, as.as);
    }));
    BOOST_CHECK_EQUAL(count, 3);
    as.reset();
    auto addr = co_await vector_store_client_tester::resolve_hostname(vs, as.as);
    BOOST_REQUIRE(addr);
    BOOST_CHECK_EQUAL(print_addr(*addr), "127.0.0.3");

    vector_store_client_tester::trigger_dns_resolver(vs);

    BOOST_CHECK(co_await repeat_until(seconds(1), [&vs, &as]() -> future<bool> {
        as.reset();
        co_return !co_await vector_store_client_tester::resolve_hostname(vs, as.as);
    }));

    BOOST_CHECK(co_await repeat_until(seconds(1), [&vs, &as]() -> future<bool> {
        as.reset();
        co_return co_await vector_store_client_tester::resolve_hostname(vs, as.as);
    }));
    BOOST_CHECK_EQUAL(count, 6);
    as.reset();
    addr = co_await vector_store_client_tester::resolve_hostname(vs, as.as);
    BOOST_REQUIRE(addr);
    BOOST_CHECK_EQUAL(print_addr(*addr), "127.0.0.6");

    co_await vs.stop();
}

/// Minimal interval between DNS refreshes is respected
SEASTAR_TEST_CASE(vector_store_client_test_dns_refresh_respects_interval) {
    auto cfg = config();
    cfg.vector_store_uri.set("http://good.authority.here:6080");
    auto vs = vector_store_client{cfg};
    auto count = 0;
    configure(vs).with_dns_refresh_interval(milliseconds(10)).with_dns_resolver([&count](auto const& host) -> future<std::optional<inet_address>> {
        BOOST_CHECK_EQUAL(host, "good.authority.here");
        count++;
        co_return inet_address("127.0.0.1");
    });

    vs.start_background_tasks();
    co_await sleep(milliseconds(20)); // wait for the first DNS refresh

    auto as = abort_source_timeout();
    auto addr = co_await vector_store_client_tester::resolve_hostname(vs, as.as);
    BOOST_REQUIRE(addr);
    BOOST_CHECK_EQUAL(print_addr(*addr), "127.0.0.1");
    BOOST_CHECK_EQUAL(count, 1);
    count = 0;
    vector_store_client_tester::trigger_dns_resolver(vs);
    vector_store_client_tester::trigger_dns_resolver(vs);
    vector_store_client_tester::trigger_dns_resolver(vs);
    vector_store_client_tester::trigger_dns_resolver(vs);
    vector_store_client_tester::trigger_dns_resolver(vs);
    co_await sleep(milliseconds(100)); // wait for the next DNS refresh

    as.reset();
    addr = co_await vector_store_client_tester::resolve_hostname(vs, as.as);
    BOOST_REQUIRE(addr);
    BOOST_CHECK_EQUAL(print_addr(*addr), "127.0.0.1");
    BOOST_CHECK_GE(count, 1);
    BOOST_CHECK_LE(count, 2);

    co_await vs.stop();
}

/// DNS refresh could be aborted
SEASTAR_TEST_CASE(vector_store_client_test_dns_refresh_aborted) {
    auto cfg = config();
    cfg.vector_store_uri.set("http://good.authority.here:6080");
    auto vs = vector_store_client{cfg};
    configure(vs).with_dns_refresh_interval(milliseconds(10)).with_dns_resolver([&](auto const& host) -> future<std::optional<inet_address>> {
        BOOST_CHECK_EQUAL(host, "good.authority.here");
        co_await sleep(milliseconds(100));
        co_return inet_address("127.0.0.1");
    });

    vs.start_background_tasks();

    auto as = abort_source_timeout(milliseconds(10));
    auto addr = co_await vector_store_client_tester::resolve_hostname(vs, as.as);
    BOOST_CHECK(!addr);

    co_await vs.stop();
}

SEASTAR_TEST_CASE(vector_store_client_ann_test_disabled) {
    co_await do_with_cql_env([](cql_test_env& env) -> future<> {
        auto schema = co_await create_test_table(env, "ks", "vs");
        auto& vs = env.local_qp().vector_store_client();

        auto as = abort_source_timeout();
        auto keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.as);
        BOOST_REQUIRE(!keys);
        BOOST_CHECK(std::holds_alternative<vector_store_client::disabled>(keys.error()));
    });
}

SEASTAR_TEST_CASE(vector_store_client_test_ann_addr_unavailable) {
    auto cfg = cql_test_config();
    cfg.db_config->vector_store_uri.set("http://bad.authority.here:6080");
    co_await do_with_cql_env(
            [](cql_test_env& env) -> future<> {
                auto schema = co_await create_test_table(env, "ks", "vs");
                auto& vs = env.local_qp().vector_store_client();
                configure(vs).with_dns_refresh_interval(seconds(1)).with_dns({{"bad.authority.here", std::nullopt}});

                vs.start_background_tasks();

                auto as = abort_source_timeout();
                auto keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.as);
                BOOST_REQUIRE(!keys);
                BOOST_CHECK(std::holds_alternative<vector_store_client::addr_unavailable>(keys.error()));
            },
            cfg);
}

SEASTAR_TEST_CASE(vector_store_client_test_ann_service_unavailable) {
    auto cfg = cql_test_config();
    cfg.db_config->vector_store_uri.set(format("http://good.authority.here:{}", generate_unavailable_localhost_port()));
    co_await do_with_cql_env(
            [](cql_test_env& env) -> future<> {
                auto schema = co_await create_test_table(env, "ks", "vs");
                auto& vs = env.local_qp().vector_store_client();
                configure(vs).with_dns_refresh_interval(seconds(1)).with_dns({{"good.authority.here", "127.0.0.1"}});

                vs.start_background_tasks();

                auto as = abort_source_timeout();
                auto keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.as);
                BOOST_REQUIRE(!keys);
                BOOST_CHECK(std::holds_alternative<vector_store_client::service_unavailable>(keys.error()));
            },
            cfg);
}

SEASTAR_TEST_CASE(vector_store_client_test_ann_service_aborted) {
    auto cfg = cql_test_config();
    cfg.db_config->vector_store_uri.set(format("http://good.authority.here:{}", generate_unavailable_localhost_port()));
    co_await do_with_cql_env(
            [](cql_test_env& env) -> future<> {
                auto schema = co_await create_test_table(env, "ks", "vs");
                auto& vs = env.local_qp().vector_store_client();
                configure(vs).with_dns_refresh_interval(milliseconds(10)).with_dns_resolver([](auto const& host) -> future<std::optional<inet_address>> {
                    BOOST_CHECK_EQUAL(host, "good.authority.here");
                    co_await sleep(milliseconds(100));
                    co_return inet_address("127.0.0.1");
                });

                vs.start_background_tasks();

                auto as = abort_source_timeout(milliseconds(10));
                auto keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.as);
                BOOST_REQUIRE(!keys);
                BOOST_CHECK(std::holds_alternative<vector_store_client::aborted>(keys.error()));
            },
            cfg);
}


SEASTAR_TEST_CASE(vector_store_client_test_ann_request) {
    auto ann_replies = make_lw_shared<std::queue<std::tuple<sstring, sstring>>>();
    auto [server, addr] = co_await new_http_server([ann_replies](routes& r) {
        auto ann = [ann_replies](std::unique_ptr<request> req, std::unique_ptr<reply> rep) -> future<std::unique_ptr<reply>> {
            BOOST_REQUIRE(!ann_replies->empty());
            auto [req_exp, rep_inp] = ann_replies->front();
            auto const req_inp = co_await util::read_entire_stream_contiguous(*req->content_stream);
            BOOST_CHECK_EQUAL(req_inp, req_exp);
            ann_replies->pop();
            rep->set_status(status_type::ok);
            rep->write_body("json", rep_inp);
            co_return rep;
        };
        r.add(operation_type::POST, url("/api/v1/indexes/ks/idx").remainder("ann"), new function_handler(ann, "json"));
    });

    auto cfg = cql_test_config();
    cfg.db_config->vector_store_uri.set(format("http://good.authority.here:{}", addr.port()));
    co_await do_with_cql_env(
            [&ann_replies](cql_test_env& env) -> future<> {
                auto schema = co_await create_test_table(env, "ks", "idx");
                auto& vs = env.local_qp().vector_store_client();
                configure(vs).with_dns_refresh_interval(seconds(1)).with_dns({{"good.authority.here", "127.0.0.1"}});

                vs.start_background_tasks();

                // set the wrong idx (wrong endpoint) - service should return 404
                auto as = abort_source_timeout();
                auto keys = co_await vs.ann("ks", "idx2", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.as);
                BOOST_REQUIRE(!keys);
                auto* err = std::get_if<vector_store_client::service_error>(&keys.error());
                BOOST_CHECK(err != nullptr);
                BOOST_CHECK_EQUAL(err->status, status_type::not_found);

                // missing primary_keys in the reply - service should return format error
                ann_replies->emplace(std::make_tuple(R"({"vector":[0.1,0.2,0.3],"limit":2})",
                            R"({"primary_keys1":{"pk1":[5,6],"pk2":[7,8],"ck1":[9,1],"ck2":[2,3]},"distances":[0.1,0.2]})"));
                auto const now = lowres_clock::now();
                for (;;) {
                    as.reset();
                    keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.as);
                    BOOST_REQUIRE(lowres_clock::now() - now < STANDARD_WAIT);
                    BOOST_REQUIRE(!keys);

                    // if the service is unavailable or 400, retry, seems http server is not ready yet
                    auto* const unavailable = std::get_if<vector_store_client::service_unavailable>(&keys.error());
                    auto* const service_error = std::get_if<vector_store_client::service_error>(&keys.error());
                    if ((unavailable == nullptr && service_error == nullptr) ||
                            (service_error != nullptr && service_error->status != status_type::bad_request)) {
                        break;
                    }
                }
                BOOST_CHECK(std::holds_alternative<vector_store_client::service_reply_format_error>(keys.error()));

                // missing distances in the reply - service should return format error
                ann_replies->emplace(std::make_tuple(R"({"vector":[0.1,0.2,0.3],"limit":2})",
                        R"({"primary_keys":{"pk1":[5,6],"pk2":[7,8],"ck1":[9,1],"ck2":[2,3]},"distances1":[0.1,0.2]})"));
                as.reset();
                keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.as);
                BOOST_REQUIRE(!keys);
                BOOST_CHECK(std::holds_alternative<vector_store_client::service_reply_format_error>(keys.error()));

                // missing pk1 key in the reply - service should return format error
                ann_replies->emplace(std::make_tuple(R"({"vector":[0.1,0.2,0.3],"limit":2})",
                        R"({"primary_keys":{"pk11":[5,6],"pk2":[7,8],"ck1":[9,1],"ck2":[2,3]},"distances":[0.1,0.2]})"));
                as.reset();
                keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.as);
                BOOST_REQUIRE(!keys);
                BOOST_CHECK(std::holds_alternative<vector_store_client::service_reply_format_error>(keys.error()));

                // missing ck1 key in the reply - service should return format error
                ann_replies->emplace(std::make_tuple(R"({"vector":[0.1,0.2,0.3],"limit":2})",
                        R"({"primary_keys":{"pk1":[5,6],"pk2":[7,8],"ck11":[9,1],"ck2":[2,3]},"distances":[0.1,0.2]})"));
                as.reset();
                keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.as);
                BOOST_REQUIRE(!keys);
                BOOST_CHECK(std::holds_alternative<vector_store_client::service_reply_format_error>(keys.error()));

                // wrong size of pk2 key in the reply - service should return format error
                ann_replies->emplace(std::make_tuple(R"({"vector":[0.1,0.2,0.3],"limit":2})",
                        R"({"primary_keys":{"pk1":[5,6],"pk2":[78],"ck1":[9,1],"ck2":[2,3]},"distances":[0.1,0.2]})"));
                as.reset();
                keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.as);
                BOOST_REQUIRE(!keys);
                BOOST_CHECK(std::holds_alternative<vector_store_client::service_reply_format_error>(keys.error()));

                // wrong size of ck2 key in the reply - service should return format error
                ann_replies->emplace(std::make_tuple(R"({"vector":[0.1,0.2,0.3],"limit":2})",
                        R"({"primary_keys":{"pk1":[5,6],"pk2":[7,8],"ck1":[9,1],"ck2":[23]},"distances":[0.1,0.2]})"));
                as.reset();
                keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.as);
                BOOST_REQUIRE(!keys);
                BOOST_CHECK(std::holds_alternative<vector_store_client::service_reply_format_error>(keys.error()));

                // correct reply - service should return keys
                ann_replies->emplace(std::make_tuple(R"({"vector":[0.1,0.2,0.3],"limit":2})",
                        R"({"primary_keys":{"pk1":[5,6],"pk2":[7,8],"ck1":[9,1],"ck2":[2,3]},"distances":[0.1,0.2]})"));
                as.reset();
                keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.as);
                BOOST_REQUIRE(keys);
                BOOST_REQUIRE_EQUAL(keys->size(), 2);
                BOOST_CHECK_EQUAL(seastar::format("{}", keys->at(0).partition.key().explode()), "[05, 07]");
                BOOST_CHECK_EQUAL(seastar::format("{}", keys->at(0).clustering.explode()), "[09, 02]");
                BOOST_CHECK_EQUAL(seastar::format("{}", keys->at(1).partition.key().explode()), "[06, 08]");
                BOOST_CHECK_EQUAL(seastar::format("{}", keys->at(1).clustering.explode()), "[01, 03]");
            },
            cfg);
    co_await server->stop();
}

SEASTAR_TEST_CASE(vector_store_client_uri_update_to_empty) {
    auto cfg = config();
    cfg.vector_store_uri.set("http://good.authority.here:6080");
    auto vs = vector_store_client{cfg};

    cfg.vector_store_uri.set("");

    BOOST_CHECK(vs.is_disabled());
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

    cfg.vector_store_uri.set("http://good.authority.here:6080");

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
    cfg.vector_store_uri.set("http://good.authority.here:6080");
    auto vs = vector_store_client{cfg};

    vs.start_background_tasks();

    cfg.vector_store_uri.set("invalid-uri");

    // vs becomes disabled
    BOOST_CHECK(vs.is_disabled());
    co_await vs.stop();
}

SEASTAR_TEST_CASE(vector_store_client_uri_update) {
    // Test verifies that when vector store uri is update, the client
    // will switch to the new uri within the DNS refresh interval.
    // To avoid race condition we wait twice long as DNS refresh interval before checking the result.
    auto make_vs_server = [](status_type status) -> future<std::tuple<std::unique_ptr<http_server>, socket_address>> {
        return new_http_server([status](routes& r) {
            auto ann = [status](std::unique_ptr<request> req, std::unique_ptr<reply> rep) -> future<std::unique_ptr<reply>> {
                rep->set_status(status);
                co_return rep;
            };
            r.add(operation_type::POST, url("/api/v1/indexes/ks/idx").remainder("ann"), new function_handler(ann, "json"));
        });
    };
    auto [s1, addr_s1] = co_await make_vs_server(status_type::not_found);
    auto [s2, addr_s2] = co_await make_vs_server(status_type::service_unavailable);

    constexpr auto is_s2_response = [](const auto& keys) -> bool {
        return !keys && std::holds_alternative<vector_store_client::service_error>(keys.error()) &&
               std::get<vector_store_client::service_error>(keys.error()).status == status_type::service_unavailable;
    };

    auto cfg = cql_test_config();
    cfg.db_config->vector_store_uri.set(format("http://good.authority.here:{}", addr_s1.port()));
    co_await do_with_cql_env(
            [&](cql_test_env& env) -> future<> {
                auto as = abort_source_timeout();
                auto schema = co_await create_test_table(env, "ks", "idx");
                auto& vs = env.local_qp().vector_store_client();
                constexpr auto DNS_REFRESH_INTERVAL = std::chrono::milliseconds(10);
                configure(vs).with_dns_refresh_interval(DNS_REFRESH_INTERVAL).with_dns({{"good.authority.here", "127.0.0.1"}});

                vs.start_background_tasks();

                env.db_config().vector_store_uri.set(format("http://good.authority.here:{}", addr_s2.port()));

                // Wait until requests are handled by s2
                BOOST_CHECK(co_await repeat_until(DNS_REFRESH_INTERVAL * 2, [&]() -> future<bool> {
                    as.reset();
                    co_return is_s2_response(co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2, as.as));
                }));
            },
            cfg);
    co_await s1->stop();
    co_await s2->stop();
}
