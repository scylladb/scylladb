/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "utils.hh"
#include "configure.hh"
#include "vs_mock_server.hh"
#include "test/lib/cql_test_env.hh"
#include "utils/rjson.hh"
#include <boost/test/tools/old/interface.hpp>
#include <seastar/core/seastar.hh>
#include <seastar/testing/test_case.hh>

using namespace seastar;
using namespace vector_search;
using namespace test::vector_search;

namespace {

size_t parse_limit(const seastar::sstring& body) {
    auto request = rjson::parse(body);
    return request["limit"].GetUint();
}

} // namespace

SEASTAR_TEST_CASE(index_option_quantization_valid_values) {
    std::vector<sstring> supported_quantizations = {"f32", "f16", "bf16", "i8", "b1", "F32", "F16", "BF16", "I8", "B1"};
    for (const auto& quantization : supported_quantizations) {
        co_await do_with_cql_env(
                [&quantization](cql_test_env& env) -> future<> {
                    auto schema = co_await create_test_table(env, "ks", "cf");

                    BOOST_REQUIRE_NO_THROW(co_await env.execute_cql(
                            fmt::format("CREATE INDEX idx ON ks.cf (embedding) USING 'vector_index' WITH OPTIONS={{'quantization': '{}'}};", quantization)));
                },
                make_config());
    }
}

SEASTAR_TEST_CASE(index_option_quantization_invalid_value) {
    co_await do_with_cql_env(
            [](cql_test_env& env) -> future<> {
                auto schema = co_await create_test_table(env, "ks", "cf");

                BOOST_REQUIRE_THROW(
                        co_await env.execute_cql("CREATE INDEX idx ON ks.cf (embedding) USING 'vector_index' WITH OPTIONS={'quantization': 'invalid_value'};"),
                        exceptions::invalid_request_exception);
            },
            make_config());
}

SEASTAR_TEST_CASE(index_option_oversampling_valid_values) {
    std::vector<float> valid_factors = {1.0, 50.5, 100.0};
    for (const auto& factor : valid_factors) {
        co_await do_with_cql_env(
                [factor](cql_test_env& env) -> future<> {
                    auto schema = co_await create_test_table(env, "ks", "cf");

                    co_await env.execute_cql(
                            fmt::format("CREATE INDEX idx ON ks.cf (embedding) USING 'vector_index' WITH OPTIONS={{'oversampling': {}}};", factor));
                },
                make_config());
    }
}

SEASTAR_TEST_CASE(index_option_oversampling_invalid_value_below_range) {
    co_await do_with_cql_env(
            [](cql_test_env& env) -> future<> {
                auto schema = co_await create_test_table(env, "ks", "cf");

                BOOST_REQUIRE_THROW(
                        co_await env.execute_cql("CREATE INDEX idx ON ks.cf (embedding) USING 'vector_index' WITH OPTIONS={'oversampling': 0.9};"),
                        exceptions::invalid_request_exception);
            },
            make_config());
}

SEASTAR_TEST_CASE(index_option_oversampling_invalid_value_above_range) {
    co_await do_with_cql_env(
            [](cql_test_env& env) -> future<> {
                auto schema = co_await create_test_table(env, "ks", "cf");

                BOOST_REQUIRE_THROW(
                        co_await env.execute_cql("CREATE INDEX idx ON ks.cf (embedding) USING 'vector_index' WITH OPTIONS={'oversampling': 100.1};"),
                        exceptions::invalid_request_exception);
            },
            make_config());
}

SEASTAR_TEST_CASE(oversampling_multiplies_limit_for_vector_store_query) {
    auto server = co_await make_vs_mock_server();
    co_await do_with_cql_env(
            [&](cql_test_env& env) -> future<> {
                auto schema = co_await create_test_table(env, "ks", "cf");
                configure(env.local_qp().vector_store_client()).with_dns({{"server.node", std::vector<std::string>{server->host()}}});
                env.local_qp().vector_store_client().start_background_tasks();
                co_await env.execute_cql("CREATE INDEX idx ON ks.cf (embedding) USING 'vector_index' WITH OPTIONS={'oversampling': 3.4}");

                auto msg = co_await env.execute_cql("SELECT * FROM ks.cf ORDER BY embedding ANN OF [0.1, 0.2, 0.3] LIMIT 3;");

                BOOST_REQUIRE(!server->ann_requests().empty());
                // The expected limit is ceil(oversampling * limit) = ceil(3.4 * 3) = ceil(10.2) = 11.
                BOOST_CHECK_EQUAL(parse_limit(server->ann_requests().back().body), 11);
            },
            make_config(format("http://server.node:{}", server->port())))
            .finally(seastar::coroutine::lambda([&] -> future<> {
                co_await server->stop();
            }));
}

SEASTAR_TEST_CASE(oversampled_vector_store_results_are_limited_to_cql_limit) {
    auto server = co_await make_vs_mock_server();
    co_await do_with_cql_env(
            [&](cql_test_env& env) -> future<> {
                configure(env.local_qp().vector_store_client()).with_dns({{"server.node", std::vector<std::string>{server->host()}}});
                env.local_qp().vector_store_client().start_background_tasks();
                co_await env.execute_cql("CREATE TABLE ks.cf (id int primary key, embedding vector<float, 3>)");
                co_await env.execute_cql("CREATE INDEX idx ON ks.cf (embedding) USING 'vector_index' WITH OPTIONS={'oversampling': 2}");
                co_await env.execute_cql("INSERT INTO ks.cf (id, embedding) VALUES (1, [0, 0, 0])");
                co_await env.execute_cql("INSERT INTO ks.cf (id, embedding) VALUES (2, [0, 0, 0])");

                server->next_ann_response({http::reply::status_type::ok, R"({
                    "primary_keys": {
                        "id": [1, 2]
                    },
                    "distances": [0, 0]
                })"});
                auto msg = co_await env.execute_cql("SELECT id FROM ks.cf ORDER BY embedding ANN OF [0, 0, 0] LIMIT 1;");

                auto rms = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
                BOOST_REQUIRE(rms);
                const auto& rows = rms->rs().result_set().rows();
                BOOST_REQUIRE_EQUAL(rows.size(), 1);
            },
            make_config(format("http://server.node:{}", server->port())))
            .finally(seastar::coroutine::lambda([&] -> future<> {
                co_await server->stop();
            }));
}
