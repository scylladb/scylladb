/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "utils.hh"
#include "test/lib/cql_test_env.hh"
#include <seastar/core/seastar.hh>
#include <seastar/testing/test_case.hh>

using namespace seastar;
using namespace vector_search;
using namespace test::vector_search;

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
