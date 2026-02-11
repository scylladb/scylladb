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
                {{0.1, 0.1}, {0.1, 0.2}, {-0.1, 0.8}, {-0.1, 0.4}},
                {1.0, 0.97, 0.81, 0.76},
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

future<> create_index_and_insert_data(cql_test_env& env, const test_data_type& data, const sstring& quantization = "b1") {
    co_await env.execute_cql("CREATE TABLE ks.cf (id int primary key, embedding vector<float, 2>)");
    co_await env.execute_cql(fmt::format("CREATE INDEX idx ON ks.cf (embedding) USING 'vector_index' WITH OPTIONS={{'quantization': '{}', "
                                         "'oversampling': '2.0', 'rescoring': 'true', 'similarity_function': '{}'}}",
            quantization, data.function_name));
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

auto to_embedding(const std::vector<float>& vec) {
    auto vector_type = vector_type_impl::get_instance(float_type, vec.size());
    std::vector<data_value> vector_data_values;
    for (float f : vec) {
        vector_data_values.push_back(data_value(f));
    }
    return vector_type->decompose(make_vector_value(vector_type, vector_data_values));
}

} // namespace

// Enable BOOST_CHECK_EQUAL to print std::vector<float> values
namespace boost::test_tools::tt_detail {
template<>
struct print_log_value<std::vector<float>> {
    void operator()(std::ostream& os, const std::vector<float>& vec) const {
        os << fmt::format("[{}]", fmt::join(vec, ", "));
    }
};
}

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
    std::vector<sstring> valid_factors = {"1.0", "50.5", "100.0", "10", "1e1", "1000000e-4", "0x10", "  10  "};
    for (const auto& factor : valid_factors) {
        co_await do_with_cql_env(
                [&factor](cql_test_env& env) -> future<> {
                    auto schema = co_await create_test_table(env, "ks", "cf");

                    BOOST_REQUIRE_NO_THROW(co_await env.execute_cql(
                            fmt::format("CREATE INDEX idx ON ks.cf (embedding) USING 'vector_index' WITH OPTIONS={{'oversampling': {}}};", factor)));
                },
                make_config());
    }
}

SEASTAR_TEST_CASE(index_option_oversampling_invalid_values) {
    std::vector<sstring> invalid_factors = {"0.9", "100.1", "0", "-5", "abc", "   ", "NaN", "inf"};
    for (const auto& factor : invalid_factors) {
        co_await do_with_cql_env(
                [&factor](cql_test_env& env) -> future<> {
                    auto schema = co_await create_test_table(env, "ks", "cf");

                    BOOST_REQUIRE_THROW(
                        co_await env.execute_cql(
                            fmt::format("CREATE INDEX idx ON ks.cf (embedding) USING 'vector_index' WITH OPTIONS={{'oversampling': '{}'}};", factor)),
                        exceptions::invalid_request_exception);
                },
                make_config());
    }
}

SEASTAR_TEST_CASE(index_option_rescoring_valid_values) {
    std::vector<sstring> valid_rescoring = {"true", "false", "True", "False", "TRUE", "FALSE"};
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
    std::vector<sstring> invalid_rescoring = {"invalid_value", "0", "1", " true", "false "};
    for (const auto& rescoring : invalid_rescoring) {
        co_await do_with_cql_env(
                [&rescoring](cql_test_env& env) -> future<> {
                    auto schema = co_await create_test_table(env, "ks", "cf");

                    BOOST_REQUIRE_THROW(
                        co_await env.execute_cql(
                            fmt::format("CREATE INDEX idx ON ks.cf (embedding) USING 'vector_index' WITH OPTIONS={{'rescoring': '{}'}};", rescoring)),
                        exceptions::invalid_request_exception);
                },
                make_config());
    }
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

SEASTAR_TEST_CASE(result_returned_by_vector_store_is_rescored) {

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

SEASTAR_TEST_CASE(f32_quantization_disables_rescoring) {

    auto server = co_await make_vs_mock_server();
    co_await do_with_cql_env(
            [&](cql_test_env& env) -> future<> {
                configure(env.local_qp().vector_store_client()).with_dns({{"server.node", std::vector<std::string>{server->host()}}});
                env.local_qp().vector_store_client().start_background_tasks();
                co_await create_index_and_insert_data(env, test_data[0], "f32");

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
                BOOST_CHECK_EQUAL(get_id_col_value(rows.at(0)), 4);
                BOOST_CHECK_EQUAL(get_id_col_value(rows.at(1)), 3);
            },
            make_config(format("http://server.node:{}", server->port())))
            .finally(seastar::coroutine::lambda([&] -> future<> {
                co_await server->stop();
            }));
}

SEASTAR_TEST_CASE(similarity_function_returns_correctly_rescored_results) {
    // This is a dedicated test that uses a similarity function in the SELECT clause.
    // We want to keep two tests, one with and one without (see `result_returned_by_vector_store_is_rescored`)
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

SEASTAR_TEST_CASE(wildcard_select_is_correctly_rescored) {
    // Another case with slightly different path of processing is "SELECT * ...".

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

SEASTAR_TEST_CASE(select_similarity_function_other_than_ann_ordering) {
    // Another tricky case with similarity column with argument different from ANN ordering vector.
    // Especially if we use prepared statement and the difference is only seen at execution time.
    const auto& params = test_data[0];
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
                auto prep = co_await env.prepare(fmt::format(
                        "SELECT id, similarity_{}(embedding, ?) FROM ks.cf ORDER BY embedding ANN OF ? LIMIT 2;", params.function_name));
                auto msg = co_await env.execute_prepared(prep, cql3::raw_value_vector_with_unset{
                    cql3::raw_value::make_value(to_embedding(params.vectors[1])),
                    cql3::raw_value::make_value(to_embedding({0.1f, 0.1f}))});

                auto rms = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
                BOOST_REQUIRE(rms);
                const auto& rows = rms->rs().result_set().rows();
                BOOST_REQUIRE(rows.size() >= 2);
                BOOST_CHECK_EQUAL(rows.size(), 2);
                BOOST_CHECK_EQUAL(rms->rs().result_set().get_metadata().column_count(), 2);
                BOOST_CHECK_EQUAL(get_id_col_value(rows.at(0)), 1);
                BOOST_CHECK(is_similarity_eq(get_similarity_col_value(rows.at(0)), params.expected_similarity[1]));
                BOOST_CHECK_EQUAL(get_id_col_value(rows.at(1)), 2);
                BOOST_CHECK(is_similarity_eq(get_similarity_col_value(rows.at(1)), params.expected_similarity[0]));
            },
            make_config(format("http://server.node:{}", server->port())))
            .finally(seastar::coroutine::lambda([&] -> future<> {
                co_await server->stop();
            }));
}

// Rescoring does not filter out NULL embeddings yet, but they should be sorted as last.
// So this test is expected to report error on result set size, but passes if the first element is correct.
SEASTAR_TEST_CASE(no_nulls_in_rescored_results, *boost::unit_test::expected_failures(3)) {

    for (const auto& params : test_data) {
        auto server = co_await make_vs_mock_server();
        co_await do_with_cql_env(
                [&](cql_test_env& env) -> future<> {
                    configure(env.local_qp().vector_store_client()).with_dns({{"server.node", std::vector<std::string>{server->host()}}});
                    env.local_qp().vector_store_client().start_background_tasks();
                    co_await create_index_and_insert_data(env, params);
                    co_await env.execute_cql(fmt::format("INSERT INTO ks.cf (id, embedding) VALUES (17, NULL)"));
                    co_await env.execute_cql(fmt::format("INSERT INTO ks.cf (id, embedding) VALUES (16, [0.1, NaN])"));
                    co_await env.execute_cql(fmt::format("INSERT INTO ks.cf (id, embedding) VALUES (15, [INFINITY, 0.1])"));

                    // Mock Response: Return all keys but in REVERSE similarity order.
                    server->next_ann_response({http::reply::status_type::ok, R"({
                        "primary_keys": { "id": [55, 17, 16, 15, 2] },
                        "distances": [0, 0, 0, 0, 0]
                    })"});
                    auto msg = co_await env.execute_cql("SELECT id FROM ks.cf ORDER BY embedding ANN OF [0.1, 0.1] LIMIT 3;");

                    auto rms = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
                    BOOST_REQUIRE(rms);
                    const auto& rows = rms->rs().result_set().rows();
                    BOOST_REQUIRE(rows.size() >= 1);
                    BOOST_CHECK_EQUAL(rows.size(), 1);
                    BOOST_CHECK_EQUAL(rms->rs().result_set().get_metadata().column_count(), 1);
                    BOOST_CHECK_EQUAL(get_id_col_value(rows.at(0)), 2);
                },
                make_config(format("http://server.node:{}", server->port())))
                .finally(seastar::coroutine::lambda([&] -> future<> {
                    co_await server->stop();
                }));
    }
}

// Reproducer for SCYLLADB-456
SEASTAR_TEST_CASE(rescoring_with_zerovector_query) {
    for (const auto& params : test_data) {
        auto server = co_await make_vs_mock_server();
        co_await do_with_cql_env(
                [&](cql_test_env& env) -> future<> {
                    configure(env.local_qp().vector_store_client()).with_dns({{"server.node", std::vector<std::string>{server->host()}}});
                    env.local_qp().vector_store_client().start_background_tasks();

                    co_await create_index_and_insert_data(env, params);

                    server->next_ann_response({http::reply::status_type::ok, R"({
                        "primary_keys": { "id": [4, 3, 2, 1] },
                        "distances": [0, 0, 0, 0]
                    })"});

                    // For cosine similarity the ANN vector query would fail as `similarity_cosine` function did not support zero vectors.
                    try {
                        auto msg = co_await env.execute_cql("SELECT id FROM ks.cf ORDER BY embedding ANN OF [0, 0] LIMIT 3;");

                        auto rms = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
                        BOOST_REQUIRE(rms);
                        const auto& rows = rms->rs().result_set().rows();
                        BOOST_REQUIRE_EQUAL(rows.size(), 3);
                    } catch (const std::exception& e) {
                        BOOST_FAIL(e.what());
                    }
                },
                make_config(format("http://server.node:{}", server->port())))
                .finally(seastar::coroutine::lambda([&] -> future<> {
                    co_await server->stop();
                }));
    }
}
