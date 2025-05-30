/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "service/vector_store.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/log.hh"
#include <cstdint>
#include <memory>
#include <random>
#include <seastar/core/shared_ptr.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/json/json_elements.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/short_streams.hh>


namespace {

using namespace seastar;

using vector_store = service::vector_store;
using function_handler = httpd::function_handler;
using http_server = httpd::http_server;
using operation_type = httpd::operation_type;
using reply = http::reply;
using request = http::request;
using routes = httpd::routes;
using status_type = http::reply::status_type;
using url = httpd::url;

auto new_ephemeral_port() {
    constexpr auto MIN_PORT = 49152;
    constexpr auto MAX_PORT = 65535;

    auto rd = std::random_device{};
    return std::uniform_int_distribution<uint16_t>(MIN_PORT, MAX_PORT)(rd);
}

auto listen_on_ephemeral_port(std::unique_ptr<http_server> server) -> future<std::tuple<std::unique_ptr<http_server>, socket_address>> {
    constexpr auto const* LOCALHOST = "127.0.0.1";
    auto inaddr = net::inet_address(LOCALHOST);

    while (true) {
        auto addr = socket_address(inaddr, new_ephemeral_port());
        try {
            co_await server->listen(addr);
            co_return std::make_tuple(std::move(server), addr);
        } catch (const std::system_error& e) {
            continue;
        }
    }
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
        auto vs = vector_store(nullptr, nullptr);
        BOOST_CHECK_EQUAL(vs.host(), "");
    }
    {
        auto vs = vector_store("192.168.100.101", nullptr);
        BOOST_CHECK_EQUAL(vs.host(), "");
    }
    {
        auto vs = vector_store(nullptr, "6080");
        BOOST_CHECK_EQUAL(vs.host(), "");
    }
    {
        auto vs = vector_store("192.168.100,101", "6080");
        BOOST_CHECK_EQUAL(vs.host(), "");
    }
    {
        auto vs = vector_store("192.168.100.101", "6080a");
        BOOST_CHECK_EQUAL(vs.host(), "");
    }
    {
        auto vs = vector_store("192.168.100.101", "6080");
        BOOST_CHECK_EQUAL(vs.host(), "192.168.100.101:6080");
    }
}

SEASTAR_TEST_CASE(vector_store_test_api) {
    return do_with_cql_env([](cql_test_env& env) -> future<> {
        auto ann_replies = make_lw_shared<std::queue<std::tuple<sstring, sstring>>>();
        auto [server, addr] = co_await new_http_server([ann_replies](routes& r) {
            auto ann = [ann_replies](std::unique_ptr<request> req, std::unique_ptr<reply> rep) -> future<std::unique_ptr<reply>> {
                BOOST_ASSERT(!ann_replies->empty());
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

        co_await env.execute_cql(R"(
                    create table ks.vs (
                        pk1 tinyint, pk2 tinyint,
                        ck1 tinyint, ck2 tinyint,
                        embedding vector<float, 3>,
                        primary key ((pk1, pk2), ck1, ck2))
                )");
        auto& db = env.local_db();
        auto schema = db.find_schema("ks", "vs");

        auto& vs = env.local_qp().vector_store();

        // service should be disabled
        auto keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2);
        BOOST_CHECK(!keys);
        BOOST_CHECK(std::get_if<vector_store::disabled>(&keys.error()) != nullptr);

        // set the wrong service address - service should be unavailable
        vs.set_service(socket_address(addr.addr(), addr.port() - 1));
        keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2);
        BOOST_CHECK(!keys);
        BOOST_CHECK(std::get_if<vector_store::service_unavailable>(&keys.error()) != nullptr);

        vs.set_service(addr);

        // set the wrong idx (wrong endpoint) - service should return 404
        keys = co_await vs.ann("ks", "idx2", schema, std::vector<float>{0.1, 0.2, 0.3}, 2);
        BOOST_CHECK(!keys);
        auto err = std::get_if<vector_store::service_error>(&keys.error());
        BOOST_CHECK(err != nullptr);
        BOOST_CHECK_EQUAL(err->_status, status_type::not_found);

        // missing primary_keys in the reply - service should return format error
        ann_replies->emplace(std::make_tuple(
                R"({"embedding":[0.1,0.2,0.3],"limit":2})", R"({"primary_keys1":{"pk1":[5,6],"pk2":[7,8],"ck1":[9,1],"ck2":[2,3]},"distances":[0.1,0.2]})"));
        do {
            keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2);
            BOOST_CHECK(!keys);
            // TODO: sometimes the service is not ready here, check why
        } while (std::get_if<vector_store::service_unavailable>(&keys.error()) != nullptr);
        BOOST_CHECK(std::get_if<vector_store::service_reply_format_error>(&keys.error()) != nullptr);

        // missing distances in the reply - service should return format error
        ann_replies->emplace(std::make_tuple(
                R"({"embedding":[0.1,0.2,0.3],"limit":2})", R"({"primary_keys":{"pk1":[5,6],"pk2":[7,8],"ck1":[9,1],"ck2":[2,3]},"distances1":[0.1,0.2]})"));
        keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2);
        BOOST_CHECK(!keys);
        BOOST_CHECK(std::get_if<vector_store::service_reply_format_error>(&keys.error()) != nullptr);

        // missing pk1 key in the reply - service should return format error
        ann_replies->emplace(std::make_tuple(
                R"({"embedding":[0.1,0.2,0.3],"limit":2})", R"({"primary_keys":{"pk11":[5,6],"pk2":[7,8],"ck1":[9,1],"ck2":[2,3]},"distances":[0.1,0.2]})"));
        keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2);
        BOOST_CHECK(!keys);
        BOOST_CHECK(std::get_if<vector_store::service_reply_format_error>(&keys.error()) != nullptr);

        // missing ck1 key in the reply - service should return format error
        ann_replies->emplace(std::make_tuple(
                R"({"embedding":[0.1,0.2,0.3],"limit":2})", R"({"primary_keys":{"pk1":[5,6],"pk2":[7,8],"ck11":[9,1],"ck2":[2,3]},"distances":[0.1,0.2]})"));
        keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2);
        BOOST_CHECK(!keys);
        BOOST_CHECK(std::get_if<vector_store::service_reply_format_error>(&keys.error()) != nullptr);

        // wrong size of pk2 key in the reply - service should return format error
        ann_replies->emplace(std::make_tuple(
                R"({"embedding":[0.1,0.2,0.3],"limit":2})", R"({"primary_keys":{"pk1":[5,6],"pk2":[78],"ck1":[9,1],"ck2":[2,3]},"distances":[0.1,0.2]})"));
        keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2);
        BOOST_CHECK(!keys);
        BOOST_CHECK(std::get_if<vector_store::service_reply_format_error>(&keys.error()) != nullptr);

        // wrong size of ck2 key in the reply - service should return format error
        ann_replies->emplace(std::make_tuple(
                R"({"embedding":[0.1,0.2,0.3],"limit":2})", R"({"primary_keys":{"pk1":[5,6],"pk2":[7,8],"ck1":[9,1],"ck2":[23]},"distances":[0.1,0.2]})"));
        keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2);
        BOOST_CHECK(!keys);
        BOOST_CHECK(std::get_if<vector_store::service_reply_format_error>(&keys.error()) != nullptr);

        ann_replies->emplace(std::make_tuple(
                R"({"embedding":[0.1,0.2,0.3],"limit":2})", R"({"primary_keys":{"pk1":[5,6],"pk2":[7,8],"ck1":[9,1],"ck2":[2,3]},"distances":[0.1,0.2]})"));
        keys = co_await vs.ann("ks", "idx", schema, std::vector<float>{0.1, 0.2, 0.3}, 2);
        BOOST_CHECK(keys);
        BOOST_CHECK_EQUAL(keys->size(), 2);
        BOOST_CHECK_EQUAL(seastar::format("{}", keys->at(0).partition.key().explode()), "[05, 07]");
        BOOST_CHECK_EQUAL(seastar::format("{}", keys->at(0).clustering.explode()), "[09, 02]");
        BOOST_CHECK_EQUAL(seastar::format("{}", keys->at(1).partition.key().explode()), "[06, 08]");
        BOOST_CHECK_EQUAL(seastar::format("{}", keys->at(1).clustering.explode()), "[01, 03]");
        co_await server->stop();
    });
}

