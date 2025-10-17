/*
 * Copyright (C) 2025 ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <boost/test/unit_test.hpp>
#include <seastar/core/format.hh>
#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>

#include "db/config.hh"
#include "test/lib/log.hh"
#include "test/lib/tmpdir.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"
#include "transport/messages/result_message.hh"

BOOST_AUTO_TEST_SUITE(sstable_compression_config_test)

// Helper to retrieve the compression options of a table
static compression_parameters get_table_compression_options(cql_test_env& env, const sstring& keyspace, const sstring& table) {
    auto query = seastar::format("SELECT compression FROM system_schema.tables WHERE keyspace_name='{}' AND table_name='{}'", keyspace, table);
    auto result = cquery_nofail(env, query);
    assert_that(result).is_rows().with_size(1);

    auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(result);
    auto& row = rows->rs().result_set().rows()[0];
    BOOST_REQUIRE_EQUAL(row.size(), 1);
    auto& compression_bytes = row[0];
    BOOST_REQUIRE(compression_bytes.has_value());

    auto compression_map = partially_deserialize_map(managed_bytes_view(*compression_bytes));
    std::map<sstring, sstring> compression_options;
    for (const auto& [k, v] : compression_map) {
        auto key = value_cast<sstring>(utf8_type->deserialize(k));
        auto value = value_cast<sstring>(utf8_type->deserialize(v));
        compression_options[key] = value;
    }
    return compression_parameters{compression_options};
}

// Test iteraction of SSTable compression configuration with CREATE TABLE and
// ALTER TABLE statements.
SEASTAR_TEST_CASE(test_compression_with_yaml_config) {
    tmpdir tmp;
    sstring alg = "SnappyCompressor";
    sstring chunk_kb = "32";

    auto cfg = seastar::make_shared<db::config>();
    auto yaml = seastar::format(R"foo(
        sstable_compression_user_table_options:
            sstable_compression: {}
            chunk_length_in_kb: {}
        )foo", alg, chunk_kb);
    cfg->read_from_yaml(yaml);

    co_await do_with_cql_env_thread([&] (cql_test_env& env) {
        testlog.info("Testing that CREATE TABLE inherits compression options from configuration");
        compression_parameters expected_options = std::map<sstring, sstring>{{"sstable_compression", alg}, {"chunk_length_in_kb", chunk_kb}};
        cquery_nofail(env, "CREATE TABLE ks.t1 (pk int PRIMARY KEY)");
        BOOST_REQUIRE(expected_options == get_table_compression_options(env, "ks", "t1"));

        testlog.info("Testing that ALTER TABLE's compression properties override config settings");
        expected_options = std::map<sstring, sstring>{{"sstable_compression", "DeflateCompressor"}, {"chunk_length_in_kb", "64"}};
        cquery_nofail(env, "ALTER TABLE ks.t1 WITH compression = {'sstable_compression': 'DeflateCompressor', 'chunk_length_in_kb': '64'}");
        BOOST_REQUIRE(expected_options == get_table_compression_options(env, "ks", "t1"));

        testlog.info("Testing that a table retains its compression properties after ALTER TABLE on a different property");
        cquery_nofail(env, "ALTER TABLE ks.t1 WITH comment = 'Test comment'");
        BOOST_REQUIRE(expected_options == get_table_compression_options(env, "ks", "t1"));

        testlog.info("Testing that ALTER TABLE can disable compression despite config settings");
        expected_options = compression_parameters::no_compression();
        cquery_nofail(env, "ALTER TABLE ks.t1 WITH compression = {'sstable_compression': ''}");
        BOOST_REQUIRE(expected_options == get_table_compression_options(env, "ks", "t1"));

        testlog.info("Testing that CREATE TABLE's compression properties override config settings");
        expected_options = std::map<sstring, sstring>{{"sstable_compression", "LZ4Compressor"}, {"chunk_length_in_kb", "128"}};
        cquery_nofail(env, "CREATE TABLE ks.t2 (pk int PRIMARY KEY) WITH compression = {'sstable_compression': 'LZ4Compressor', 'chunk_length_in_kb': '128'}");
        BOOST_REQUIRE(expected_options == get_table_compression_options(env, "ks", "t2"));
    }, cfg);
}

// Test that syntax errors are properly detected.
// (based on sanity checks in the `compression_parameters` constructor)
//
// NOTE: Ideally, the following tests should be conducted with a full ScyllaDB
// instance, in Python. However, ScyllaDB currently does not exit on such
// errors; it only emits error logs and falls back to the defaults. That's
// because of the suppressive error handler passed from `scylla_main()` to `read_from_file()`.
// There is an open issue about this: https://github.com/scylladb/scylladb/issues/9469
SEASTAR_TEST_CASE(test_syntax_errors_in_yaml_config) {
    auto cfg = seastar::make_shared<db::config>();

    testlog.info("Testing non-empty compression options with absent sstable_compression");
    BOOST_REQUIRE_THROW(cfg->read_from_yaml(R"foo(
        sstable_compression_user_table_options:
            chunk_length_in_kb: 4
        )foo"), std::invalid_argument);

    testlog.info("Testing invalid sstable_compression");
    BOOST_REQUIRE_THROW(cfg->read_from_yaml(R"foo(
        sstable_compression_user_table_options:
            sstable_compression: InvalidCompressor
        )foo"), std::invalid_argument);

    testlog.info("Testing invalid chunk_length_in_kb");
    BOOST_REQUIRE_THROW(cfg->read_from_yaml(R"foo(
        sstable_compression_user_table_options:
            sstable_compression: LZ4Compressor
            chunk_length_in_kb: four
        )foo"), std::invalid_argument);

    testlog.info("Testing invalid crc_check_chance");
    BOOST_REQUIRE_THROW(cfg->read_from_yaml(R"foo(
        sstable_compression_user_table_options:
            sstable_compression: LZ4Compressor
            crc_check_chance: zero
        )foo"), std::invalid_argument);

    testlog.info("Testing invalid compression_level");
    BOOST_REQUIRE_THROW(cfg->read_from_yaml(R"foo(
        sstable_compression_user_table_options:
            sstable_compression: ZstdCompressor
            compression_level: ten
        )foo"), std::invalid_argument);

    testlog.info("Testing invalid option name");
    BOOST_REQUIRE_THROW(cfg->read_from_yaml(R"foo(
        sstable_compression_user_table_options:
            sstable_compression: ZstdCompressor
            invalid_option_name: foo
        )foo"), std::invalid_argument);

    testlog.info("Testing compression level with non-ZSTD algorithm");
    BOOST_REQUIRE_THROW(cfg->read_from_yaml(R"foo(
        sstable_compression_user_table_options:
            sstable_compression: LZ4Compressor
            compression_level: 10
        )foo"), std::invalid_argument);

    return make_ready_future<>();
}

BOOST_AUTO_TEST_SUITE_END()