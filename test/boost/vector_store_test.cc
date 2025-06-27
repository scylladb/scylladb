/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "service/vector_store.hh"
#include "db/config.hh"
#include "exceptions/exceptions.hh"
#include "cql3/statements/select_statement.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/log.hh"
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


namespace {

using namespace seastar;

using vector_store = service::vector_store;
using config = vector_store::config;
using configuration_exception = exceptions::configuration_exception;
using function_handler = httpd::function_handler;
using http_server = httpd::http_server;
using http_server_tester = httpd::http_server_tester;
using operation_type = httpd::operation_type;
using port_number = vector_store::port_number;
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
    co_await server->listen(addr);
    auto const& listeners = http_server_tester::listeners(*server);
    BOOST_CHECK_EQUAL(listeners.size(), 1);
    co_return std::make_tuple(std::move(server), listeners[0].local_address().port());
}

auto new_http_server(std::function<void(routes& r)> set_routes) -> future<std::tuple<std::unique_ptr<http_server>, socket_address>> {
    auto server = std::make_unique<http_server>("test_vector_store");
    set_routes(server->_routes);
    server->set_content_streaming(true);
    co_return co_await listen_on_ephemeral_port(std::move(server));
}

} // namespace

BOOST_AUTO_TEST_CASE(vector_store_test_ctor) {
    {
        auto cfg = config();
        auto vs = vector_store{cfg};
        BOOST_CHECK(vs.is_disabled());
        BOOST_CHECK_EQUAL(vs.host(), "");
        BOOST_CHECK_EQUAL(vs.port(), 0);
    }
    {
        auto cfg = config();
        cfg.vector_store_uri.set("http://good.authority.com:6080");
        auto vs = vector_store{cfg};
        BOOST_CHECK(!vs.is_disabled());
        BOOST_CHECK_EQUAL(vs.host(), "good.authority.com");
        BOOST_CHECK_EQUAL(vs.port(), 6080);
    }
    {
        auto cfg = config();
        cfg.vector_store_uri.set("http://bad,authority.com:6080");
        BOOST_CHECK_THROW(vector_store{cfg}, configuration_exception);
        cfg.vector_store_uri.set("bad-schema://authority.com:6080");
        BOOST_CHECK_THROW(vector_store{cfg}, configuration_exception);
        cfg.vector_store_uri.set("http://bad.port.com:a6080");
        BOOST_CHECK_THROW(vector_store{cfg}, configuration_exception);
        cfg.vector_store_uri.set("http://bad.port.com:60806080");
        BOOST_CHECK_THROW(vector_store{cfg}, configuration_exception);
        cfg.vector_store_uri.set("http://bad.format.com:60:80");
        BOOST_CHECK_THROW(vector_store{cfg}, configuration_exception);
        cfg.vector_store_uri.set("http://authority.com:6080/bad/path");
        BOOST_CHECK_THROW(vector_store{cfg}, configuration_exception);
    }
}

SEASTAR_TEST_CASE(vector_store_test_disabled) {
    co_await do_with_cql_env([](cql_test_env& env) -> future<> {
        co_await env.execute_cql(R"(
            create table ks.vs (
                pk1 tinyint, pk2 tinyint,
                ck1 tinyint, ck2 tinyint,
                embedding vector<float, 3>,
                primary key ((pk1, pk2), ck1, ck2))
        )");

        auto schema = env.local_db().find_schema("ks", "vs");
        auto& vs = env.local_qp().vector_store();

        auto keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2);
        BOOST_REQUIRE(!keys);
        BOOST_CHECK(std::get_if<vector_store::disabled>(&keys.error()) != nullptr);
    });
}

SEASTAR_TEST_CASE(vector_store_test_addr_unavailable) {
    auto cfg = cql_test_config();
    cfg.db_config->vector_store_uri.set("http://bad.authority.here:6080");
    co_await do_with_cql_env(
            [](cql_test_env& env) -> future<> {
                co_await env.execute_cql(R"(
                    create table ks.vs (
                        pk1 tinyint, pk2 tinyint,
                        ck1 tinyint, ck2 tinyint,
                        embedding vector<float, 3>,
                        primary key ((pk1, pk2), ck1, ck2))
                )");

                auto schema = env.local_db().find_schema("ks", "vs");
                auto& vs = env.local_qp().vector_store();

                auto keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2);
                BOOST_REQUIRE(!keys);
                BOOST_CHECK(std::get_if<vector_store::addr_unavailable>(&keys.error()) != nullptr);
            },
            cfg);
}

SEASTAR_TEST_CASE(vector_store_test_service_unavailable) {
    auto inaddr = net::inet_address(LOCALHOST);
    auto local_name = co_await net::dns::resolve_addr(inaddr);
    auto cfg = cql_test_config();
    cfg.db_config->vector_store_uri.set(format("http://{}:{}", local_name, generate_unavailable_localhost_port()));
    co_await do_with_cql_env(
            [](cql_test_env& env) -> future<> {
                co_await env.execute_cql(R"(
                    create table ks.vs (
                        pk1 tinyint, pk2 tinyint,
                        ck1 tinyint, ck2 tinyint,
                        embedding vector<float, 3>,
                        primary key ((pk1, pk2), ck1, ck2))
                )");

                auto schema = env.local_db().find_schema("ks", "vs");
                auto& vs = env.local_qp().vector_store();

                auto keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2);
                BOOST_REQUIRE(!keys);
                BOOST_CHECK(std::get_if<vector_store::service_unavailable>(&keys.error()) != nullptr);
            },
            cfg);
}


SEASTAR_TEST_CASE(vector_store_test_api_ann) {
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
    cfg.db_config->vector_store_uri.set(format("http://localhost:{}", addr.port()));
    co_await do_with_cql_env(
            [&ann_replies](cql_test_env& env) -> future<> {
                co_await env.execute_cql(R"(
                    create table ks.vs (
                        pk1 tinyint, pk2 tinyint,
                        ck1 tinyint, ck2 tinyint,
                        embedding vector<float, 3>,
                        primary key ((pk1, pk2), ck1, ck2))
                )");

                auto schema = env.local_db().find_schema("ks", "vs");
                auto& vs = env.local_qp().vector_store();

                // set the wrong idx (wrong endpoint) - service should return 404
                auto keys = co_await vs.ann("ks", "idx2", schema, std::vector<float>{0.1, 0.2, 0.3}, 2);
                BOOST_REQUIRE(!keys);
                auto err = std::get_if<vector_store::service_error>(&keys.error());
                BOOST_CHECK(err != nullptr);
                BOOST_CHECK_EQUAL(err->status, status_type::not_found);

                // missing primary_keys in the reply - service should return format error
                ann_replies->emplace(std::make_tuple(R"({"embedding":[0.1,0.2,0.3],"limit":2})",
                        R"({"primary_keys1":{"pk1":[5,6],"pk2":[7,8],"ck1":[9,1],"ck2":[2,3]},"distances":[0.1,0.2]})"));
                auto const now = lowres_system_clock::now();
                for (;;) {
                    keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2);
                    BOOST_REQUIRE(!keys);

                    // if the service is unavailable or 400, retry, seems http server is not ready yet
                    auto* const unavailable = std::get_if<vector_store::service_unavailable>(&keys.error());
                    auto* const service_error = std::get_if<vector_store::service_error>(&keys.error());
                    if ((unavailable == nullptr && service_error == nullptr) ||
                            (service_error != nullptr && service_error->status != status_type::bad_request)) {
                        constexpr auto MAX_WAIT = std::chrono::seconds(5);
                        BOOST_REQUIRE(lowres_system_clock::now() - now < MAX_WAIT);
                        break;
                    }
                }
                BOOST_CHECK(std::get_if<vector_store::service_reply_format_error>(&keys.error()) != nullptr);

                // missing distances in the reply - service should return format error
                ann_replies->emplace(std::make_tuple(R"({"embedding":[0.1,0.2,0.3],"limit":2})",
                        R"({"primary_keys":{"pk1":[5,6],"pk2":[7,8],"ck1":[9,1],"ck2":[2,3]},"distances1":[0.1,0.2]})"));
                keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2);
                BOOST_REQUIRE(!keys);
                BOOST_CHECK(std::get_if<vector_store::service_reply_format_error>(&keys.error()) != nullptr);

                // missing pk1 key in the reply - service should return format error
                ann_replies->emplace(std::make_tuple(R"({"embedding":[0.1,0.2,0.3],"limit":2})",
                        R"({"primary_keys":{"pk11":[5,6],"pk2":[7,8],"ck1":[9,1],"ck2":[2,3]},"distances":[0.1,0.2]})"));
                keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2);
                BOOST_REQUIRE(!keys);
                BOOST_CHECK(std::get_if<vector_store::service_reply_format_error>(&keys.error()) != nullptr);

                // missing ck1 key in the reply - service should return format error
                ann_replies->emplace(std::make_tuple(R"({"embedding":[0.1,0.2,0.3],"limit":2})",
                        R"({"primary_keys":{"pk1":[5,6],"pk2":[7,8],"ck11":[9,1],"ck2":[2,3]},"distances":[0.1,0.2]})"));
                keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2);
                BOOST_REQUIRE(!keys);
                BOOST_CHECK(std::get_if<vector_store::service_reply_format_error>(&keys.error()) != nullptr);

                // wrong size of pk2 key in the reply - service should return format error
                ann_replies->emplace(std::make_tuple(R"({"embedding":[0.1,0.2,0.3],"limit":2})",
                        R"({"primary_keys":{"pk1":[5,6],"pk2":[78],"ck1":[9,1],"ck2":[2,3]},"distances":[0.1,0.2]})"));
                keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2);
                BOOST_REQUIRE(!keys);
                BOOST_CHECK(std::get_if<vector_store::service_reply_format_error>(&keys.error()) != nullptr);

                // wrong size of ck2 key in the reply - service should return format error
                ann_replies->emplace(std::make_tuple(R"({"embedding":[0.1,0.2,0.3],"limit":2})",
                        R"({"primary_keys":{"pk1":[5,6],"pk2":[7,8],"ck1":[9,1],"ck2":[23]},"distances":[0.1,0.2]})"));
                keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2);
                BOOST_REQUIRE(!keys);
                BOOST_CHECK(std::get_if<vector_store::service_reply_format_error>(&keys.error()) != nullptr);

                // correct reply - service should return keys
                ann_replies->emplace(std::make_tuple(R"({"embedding":[0.1,0.2,0.3],"limit":2})",
                        R"({"primary_keys":{"pk1":[5,6],"pk2":[7,8],"ck1":[9,1],"ck2":[2,3]},"distances":[0.1,0.2]})"));
                keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2);
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

