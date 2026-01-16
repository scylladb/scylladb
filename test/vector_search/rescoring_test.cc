/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "types/types.hh"
#include "types/vector.hh"
#include "cql3/util.hh"
#include "utils.hh"
#include "configure.hh"
#include "vector_search/vector_store_client.hh"
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

bool is_similarity_eq(float a, float b) {
    return std::abs(a - b) <= 0.01f;
}
struct test_data_type {
    sstring function_name;
    std::vector<std::vector<float>> vectors;
    std::vector<float> expected_similarity;
};

std::vector<int> test_ids = {1, 2, 3, 4};


std::vector<test_data_type> test_data = {
        test_data_type{
                "cosine",
                {{0.1, 0.1}, {0.1, 0.2}, {0.1, 1.4}, {0.1, 0.8}},
                {1.0, 0.97, 0.92, 0.89},
        },
        test_data_type{
                "euclidean",
                {{0.1, 0.2}, {0.1, 0.4}, {0.1, 0.8}, {0.1, 1.6}},
                {0.99, 0.91, 0.67, 0.30},
        },
        test_data_type{
                "dot_product",
                {{0.1, 1.6}, {0.1, 0.8}, {0.2, 0.4}, {0.1, 0.1}},
                {0.585, 0.545, 0.53, 0.51},
        },
};

future<> create_index_and_insert_data(cql_test_env& env, const test_data_type& data) {
    co_await env.execute_cql("CREATE TABLE ks.cf (id int primary key, embedding vector<float, 2>)");
    co_await env.execute_cql(fmt::format("CREATE INDEX idx ON ks.cf (embedding) USING 'vector_index' WITH OPTIONS={{'quantization': 'b1', "
                                         "'oversampling': '2.0', 'rescoring': 'true', 'similarity_function': '{}'}}",
            data.function_name));
    for (const auto& [id, vector] : std::views::zip(test_ids, data.vectors)) {
        co_await env.execute_cql(fmt::format("INSERT INTO ks.cf (id, embedding) VALUES ({}, [{}])", id, fmt::join(vector, ",")));
    }
}

int get_id_col_value(const auto& row, int index = 0) {
    return value_cast<int>(int32_type->deserialize(row.at(index).value()));
}

float get_similarity_col_value(const auto& row, int index = 1) {
    return value_cast<float>(float_type->deserialize(row.at(index).value()));
}

std::vector<float> get_embedding_col_value(const auto& row, int index = 1) {
    auto bytes = row.at(index).value();
    auto vec_type = vector_type_impl::get_instance(float_type, 2);
    auto values = value_cast<vector_type_impl::native_type>(vec_type->deserialize(bytes));
    return cql3::util::to_vector<float>(values);
}

} // namespace

namespace std {

ostream& operator<<(ostream& os, const vector<float>& vec) {
    return os << fmt::format("[{}]", fmt::join(vec, ", "));
}

} // namespace std


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

SEASTAR_TEST_CASE(index_option_rescoring_valid_values) {
    std::vector<sstring> valid_rescoring = {"True", "False"};
    for (const auto& rescoring : valid_rescoring) {
        co_await do_with_cql_env(
                [&rescoring](cql_test_env& env) -> future<> {
                    auto schema = co_await create_test_table(env, "ks", "cf");

                    BOOST_REQUIRE_NO_THROW(co_await env.execute_cql(
                            fmt::format("CREATE INDEX idx ON ks.cf (embedding) USING 'vector_index' WITH OPTIONS={{'rescoring': '{}'}};", rescoring)));
                },
                make_config());
    }
}

SEASTAR_TEST_CASE(index_option_rescoring_invalid_value) {
    co_await do_with_cql_env(
            [](cql_test_env& env) -> future<> {
                auto schema = co_await create_test_table(env, "ks", "cf");

                BOOST_REQUIRE_THROW(
                        co_await env.execute_cql("CREATE INDEX idx ON ks.cf (embedding) USING 'vector_index' WITH OPTIONS={'rescoring': 'invalid_value'};"),
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

// Test is failing until rescoring is implemented (see https://scylladb.atlassian.net/browse/SCYLLADB-83)
SEASTAR_TEST_CASE(result_returned_by_vector_store_is_rescored, *boost::unit_test::expected_failures(6)) {

    for (const auto& params : test_data) {
        auto server = co_await make_vs_mock_server();
        co_await do_with_cql_env(
                [&](cql_test_env& env) -> future<> {
                    configure(env.local_qp().vector_store_client()).with_dns({{"server.node", std::vector<std::string>{server->host()}}});
                    env.local_qp().vector_store_client().start_background_tasks();
                    co_await create_index_and_insert_data(env, params);

                    // Mock Response: Return all keys but in REVERSE similarity order.
                    server->next_ann_response({http::reply::status_type::ok, R"({
                        "primary_keys": { "id": [4, 3, 2, 1] },
                        "distances": [0, 0, 0, 0]
                    })"});
                    auto msg = co_await env.execute_cql("SELECT id FROM ks.cf ORDER BY embedding ANN OF [0.1, 0.1] LIMIT 2;");

                    auto rms = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
                    BOOST_REQUIRE(rms);
                    const auto& rows = rms->rs().result_set().rows();
                    BOOST_REQUIRE(rows.size() >= 2);
                    BOOST_CHECK_EQUAL(rows.size(), 2);
                    BOOST_CHECK_EQUAL(rms->rs().result_set().get_metadata().column_count(), 1);
                    BOOST_CHECK_EQUAL(get_id_col_value(rows.at(0)), 1);
                    BOOST_CHECK_EQUAL(get_id_col_value(rows.at(1)), 2);
                },
                make_config(format("http://server.node:{}", server->port())))
                .finally(seastar::coroutine::lambda([&] -> future<> {
                    co_await server->stop();
                }));
    }
}

// Test is failing until rescoring is implemented (see https://scylladb.atlassian.net/browse/SCYLLADB-83)
SEASTAR_TEST_CASE(similarity_function_returns_correctly_rescored_results, *boost::unit_test::expected_failures(24)) {
    // This is a dedicated test that uses a similarity function in the SELECT clause.
    // We want to keep two tests, one with and one without (see `result_returned_by_vector_store_is_rescored`)\
    // a similarity function in the SELECT clause, to ensure both code paths are covered.

    for (const auto& params : test_data) {
        auto server = co_await make_vs_mock_server();
        co_await do_with_cql_env(
                [&](cql_test_env& env) -> future<> {
                    configure(env.local_qp().vector_store_client()).with_dns({{"server.node", std::vector<std::string>{server->host()}}});
                    env.local_qp().vector_store_client().start_background_tasks();
                    co_await create_index_and_insert_data(env, params);

                    for (auto func_args : {"embedding, [0.1, 0.1]", "[0.1, 0.1], embedding"}) {
                        // Mock Response: Return all keys but in REVERSE similarity order.
                        server->next_ann_response({http::reply::status_type::ok, R"({
                            "primary_keys": { "id": [4, 3, 2, 1] },
                            "distances": [0, 0, 0, 0]
                        })"});
                        auto msg = co_await env.execute_cql(fmt::format(
                                "SELECT id, similarity_{}({}) FROM ks.cf ORDER BY embedding ANN OF [0.1, 0.1] LIMIT 2;", params.function_name, func_args));

                        auto rms = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
                        BOOST_REQUIRE(rms);
                        const auto& rows = rms->rs().result_set().rows();
                        BOOST_REQUIRE(rows.size() >= 2);
                        BOOST_CHECK_EQUAL(rows.size(), 2);
                        BOOST_CHECK_EQUAL(rms->rs().result_set().get_metadata().column_count(), 2);
                        BOOST_CHECK_EQUAL(get_id_col_value(rows.at(0)), 1);
                        BOOST_CHECK(is_similarity_eq(get_similarity_col_value(rows.at(0)), params.expected_similarity[0]));
                        BOOST_CHECK_EQUAL(get_id_col_value(rows.at(1)), 2);
                        BOOST_CHECK(is_similarity_eq(get_similarity_col_value(rows.at(1)), params.expected_similarity[1]));
                    }
                },
                make_config(format("http://server.node:{}", server->port())))
                .finally(seastar::coroutine::lambda([&] -> future<> {
                    co_await server->stop();
                }));
    }
}

// Test is failing until rescoring is implemented (see https://scylladb.atlassian.net/browse/SCYLLADB-83)
SEASTAR_TEST_CASE(wildcard_select_is_correctly_rescored, *boost::unit_test::expected_failures(12)) {
    // Another case with slightly different path of processing is "SELECT * ...".
    // We want to confirm that it is handled correctly. 

    for (const auto& params : test_data) {
        auto server = co_await make_vs_mock_server();
        co_await do_with_cql_env(
                [&](cql_test_env& env) -> future<> {
                    configure(env.local_qp().vector_store_client()).with_dns({{"server.node", std::vector<std::string>{server->host()}}});
                    env.local_qp().vector_store_client().start_background_tasks();
                    co_await create_index_and_insert_data(env, params);

                    // Mock Response: Return all keys but in REVERSE similarity order.
                    server->next_ann_response({http::reply::status_type::ok, R"({
                        "primary_keys": { "id": [4, 3, 2, 1] },
                        "distances": [0, 0, 0, 0]
                    })"});
                    auto msg = co_await env.execute_cql("SELECT * FROM ks.cf ORDER BY embedding ANN OF [0.1, 0.1] LIMIT 2;");

                    auto rms = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
                    BOOST_REQUIRE(rms);
                    const auto& rows = rms->rs().result_set().rows();
                    BOOST_REQUIRE(rows.size() >= 2);
                    BOOST_CHECK_EQUAL(rows.size(), 2);
                    BOOST_CHECK_EQUAL(rms->rs().result_set().get_metadata().column_count(), 2);
                    BOOST_CHECK_EQUAL(get_id_col_value(rows.at(0)), test_ids[0]);
                    BOOST_CHECK_EQUAL(get_embedding_col_value(rows.at(0)), params.vectors[0]);
                    BOOST_CHECK_EQUAL(get_id_col_value(rows.at(1)), 2);
                    BOOST_CHECK_EQUAL(get_embedding_col_value(rows.at(1)), params.vectors[1]);
                },
                make_config(format("http://server.node:{}", server->port())))
                .finally(seastar::coroutine::lambda([&] -> future<> {
                    co_await server->stop();
                }));
    }
}
