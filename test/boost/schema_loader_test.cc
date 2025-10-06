/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/util/short_streams.hh>

#include "test/lib/log.hh"
#include "test/lib/scylla_test_case.hh"
#include "test/lib/random_schema.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/sstable_test_env.hh"
#include "test/lib/mutation_reader_assertions.hh"

#include "db/config.hh"
#include "index/secondary_index_manager.hh"
#include "sstables/sstable_writer.hh"
#include "readers/from_mutations.hh"
#include "tools/schema_loader.hh"
#include "types/list.hh"
#include "view_info.hh"

SEASTAR_THREAD_TEST_CASE(test_empty) {
    db::config dbcfg;
    dbcfg.rf_rack_valid_keyspaces(true);

    BOOST_REQUIRE_THROW(tools::load_schemas(dbcfg, "").get(), std::exception);
    BOOST_REQUIRE_THROW(tools::load_schemas(dbcfg, ";").get(), std::exception);
}

SEASTAR_THREAD_TEST_CASE(test_keyspace_only) {
    db::config dbcfg;
    dbcfg.rf_rack_valid_keyspaces(true);

    BOOST_REQUIRE_EQUAL(tools::load_schemas(dbcfg, "CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};").get().size(), 0);
}

SEASTAR_THREAD_TEST_CASE(test_single_table) {
    db::config dbcfg;
    dbcfg.rf_rack_valid_keyspaces(true);

    BOOST_REQUIRE_EQUAL(tools::load_schemas(dbcfg, "CREATE TABLE ks.cf (pk int PRIMARY KEY, v int)").get().size(), 1);
    BOOST_REQUIRE_EQUAL(tools::load_schemas(dbcfg, "CREATE TABLE ks.cf (pk int PRIMARY KEY, v map<int, int>)").get().size(), 1);
    BOOST_REQUIRE_EQUAL(tools::load_schemas(dbcfg, "CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}; CREATE TABLE ks.cf (pk int PRIMARY KEY, v int);").get().size(), 1);
}

SEASTAR_THREAD_TEST_CASE(test_keyspace_replication_strategy) {
    db::config dbcfg;
    dbcfg.rf_rack_valid_keyspaces(true);

    BOOST_REQUIRE_EQUAL(tools::load_schemas(dbcfg, "CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}; CREATE TABLE ks.cf (pk int PRIMARY KEY, v int);").get().size(), 1);
    BOOST_REQUIRE_EQUAL(tools::load_schemas(dbcfg, "CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}; CREATE TABLE ks.cf (pk int PRIMARY KEY, v int);").get().size(), 1);
    BOOST_REQUIRE_EQUAL(tools::load_schemas(dbcfg, "CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'mydc1': 1, 'mydc2': 4}; CREATE TABLE ks.cf (pk int PRIMARY KEY, v int);").get().size(), 1);
}

SEASTAR_THREAD_TEST_CASE(test_multiple_tables) {
    db::config dbcfg;
    dbcfg.rf_rack_valid_keyspaces(true);

    BOOST_REQUIRE_EQUAL(tools::load_schemas(dbcfg, "CREATE TABLE ks.cf1 (pk int PRIMARY KEY, v int); CREATE TABLE ks.cf2 (pk int PRIMARY KEY, v int)").get().size(), 2);
    BOOST_REQUIRE_EQUAL(tools::load_schemas(dbcfg, "CREATE TABLE ks.cf1 (pk int PRIMARY KEY, v int); CREATE TABLE ks.cf2 (pk int PRIMARY KEY, v int);").get().size(), 2);
    BOOST_REQUIRE_EQUAL(tools::load_schemas(
                dbcfg,
                "CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}; "
                "CREATE TABLE ks.cf1 (pk int PRIMARY KEY, v int); "
                "CREATE TABLE ks.cf2 (pk int PRIMARY KEY, v int); "
                "CREATE TABLE ks.cf3 (pk int PRIMARY KEY, v int); "
    ).get().size(), 3);
    BOOST_REQUIRE_EQUAL(tools::load_schemas(
                dbcfg,
                "CREATE KEYSPACE ks1 WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}; "
                "CREATE TABLE ks1.cf (pk int PRIMARY KEY, v int); "
                "CREATE KEYSPACE ks2 WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}; "
                "CREATE TABLE ks2.cf (pk int PRIMARY KEY, v int); "
    ).get().size(), 2);
}

SEASTAR_THREAD_TEST_CASE(test_udts) {
    db::config dbcfg;
    dbcfg.rf_rack_valid_keyspaces(true);

    BOOST_REQUIRE_EQUAL(tools::load_schemas(
                dbcfg,
                "CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}; "
                "CREATE TYPE ks.type1 (f1 int, f2 text); "
                "CREATE TABLE ks.cf (pk int PRIMARY KEY, v frozen<type1>); "
    ).get().size(), 1);
    BOOST_REQUIRE_EQUAL(tools::load_schemas(
                dbcfg,
                "CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}; "
                "CREATE TYPE ks.type1 (f1 int, f2 text); "
                "CREATE TABLE ks.cf (pk int PRIMARY KEY, v type1); "
    ).get().size(), 1);
    BOOST_REQUIRE_EQUAL(tools::load_schemas(
                dbcfg,
                "CREATE KEYSPACE ks1 WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}; "
                "CREATE TYPE ks1.type1 (f1 int, f2 text); "
                "CREATE TABLE ks1.cf (pk int PRIMARY KEY, v frozen<type1>); "
                "CREATE KEYSPACE ks2 WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}; "
                "CREATE TYPE ks2.type1 (f1 int, f2 text); "
                "CREATE TABLE ks2.cf (pk int PRIMARY KEY, v frozen<type1>); "
    ).get().size(), 2);
    BOOST_REQUIRE_EQUAL(tools::load_schemas(
                dbcfg,
                "CREATE TYPE ks.type1 (f1 int, f2 text); "
                "CREATE TABLE ks.cf (pk int PRIMARY KEY, v frozen<type1>); "
    ).get().size(), 1);
    BOOST_REQUIRE_EQUAL(tools::load_schemas(
                dbcfg,
                "CREATE TYPE ks1.type1 (f1 int, f2 text); "
                "CREATE TABLE ks1.cf (pk int PRIMARY KEY, v frozen<type1>); "
                "CREATE TYPE ks2.type1 (f1 int, f2 text); "
                "CREATE TABLE ks2.cf (pk int PRIMARY KEY, v frozen<type1>); "
    ).get().size(), 2);
}

SEASTAR_THREAD_TEST_CASE(test_dropped_columns) {
    db::config dbcfg;
    dbcfg.rf_rack_valid_keyspaces(true);

    BOOST_REQUIRE_EQUAL(tools::load_schemas(
                dbcfg,
                "CREATE TABLE ks.cf (pk int PRIMARY KEY, v1 int); "
                "INSERT INTO system_schema.dropped_columns (keyspace_name, table_name, column_name, dropped_time, type) VALUES ('ks', 'cf', 'v2', 1631011979170675, 'int'); "
    ).get().size(), 1);
    BOOST_REQUIRE_THROW(tools::load_schemas(
                dbcfg,
                "CREATE TABLE ks.cf (pk int PRIMARY KEY, v1 int); "
                "INSERT INTO ks.cf (pk, v1) VALUES (0, 0); "
    ).get(), std::exception);
    BOOST_REQUIRE_THROW(tools::load_schemas(
                dbcfg,
                "INSERT INTO system_schema.dropped_columns (keyspace_name, table_name, column_name, dropped_time, type) VALUES ('ks', 'cf', 'v2', 1631011979170675, 'int'); "
                "CREATE TABLE ks.cf (pk int PRIMARY KEY, v1 int); "
    ).get(), std::exception);
    BOOST_REQUIRE_THROW(tools::load_schemas(
                dbcfg,
                "CREATE TABLE ks.cf (pk int PRIMARY KEY, v1 int); "
                "INSERT INTO system_schema.dropped_columns (keyspace_name, table_name, column_name, dropped_time, type) VALUES ('unknown_ks', 'unknown_cf', 'v2', 1631011979170675, 'int'); "
    ).get(), std::exception);
    BOOST_REQUIRE_THROW(tools::load_schemas(
                dbcfg,
                "CREATE TABLE ks.cf (pk int PRIMARY KEY, v1 int); "
                "INSERT INTO system_schema.dropped_columns (keyspace_name, table_name, column_name, dropped_time, type) VALUES ('unknown_ks', 'cf', 'v2', 1631011979170675, 'int'); "
    ).get(), std::exception);
    BOOST_REQUIRE_THROW(tools::load_schemas(
                dbcfg,
                "CREATE TABLE ks.cf (pk int PRIMARY KEY, v1 int); "
                "INSERT INTO system_schema.dropped_columns (keyspace_name, table_name, column_name, dropped_time, type) VALUES ('ks', 'unknown_cf', 'v2', 1631011979170675, 'int'); "
    ).get(), std::exception);
}

/// Check that:
/// * schemas[0] is a base schema (it is not a view)
/// * schemas[1]..schemas.back() are views
/// * schemas[0] is the base of each view
/// * the type of schemas[i] matches views_type[i - 1]
enum class view_type {
    index,
    view
};
void check_views(std::vector<schema_ptr> schemas, std::vector<view_type> views_type, std::source_location sl = std::source_location::current()) {
    testlog.info("Checking views built at {}:{}", sl.file_name(), sl.line());
    BOOST_REQUIRE_EQUAL(schemas.size(), views_type.size() + 1);

    const auto base_schema = schemas.front();
    testlog.info("Base table is {}.{}", base_schema->ks_name(), base_schema->cf_name());
    BOOST_REQUIRE(!base_schema->is_view());

    auto schema_it = schemas.begin() + 1;
    auto view_type_it = views_type.begin();
    while (schema_it != schemas.end() && view_type_it != views_type.end()) {
        auto schema = *schema_it;
        auto type = *view_type_it;
        testlog.info("Checking view {}.{} is_index={}", schema->ks_name(), schema->cf_name(), type == view_type::index);
        BOOST_REQUIRE(schema->is_view());
        BOOST_REQUIRE_EQUAL(base_schema->id(), schema->view_info()->base_id());
        if (type == view_type::index) {
            BOOST_REQUIRE(base_schema->has_index(secondary_index::index_name_from_table_name(schema->cf_name())));
        }

        ++schema_it;
        ++view_type_it;
    }
    BOOST_REQUIRE(schema_it == schemas.end());
    BOOST_REQUIRE(view_type_it == views_type.end());
}

SEASTAR_THREAD_TEST_CASE(test_materialized_view) {
    db::config dbcfg;
    dbcfg.rf_rack_valid_keyspaces(true);

    check_views(
            tools::load_schemas(
                    dbcfg,
                    "CREATE TABLE ks.cf (pk int PRIMARY KEY, v int); "
                    "CREATE MATERIALIZED VIEW ks.cf_by_v AS"
                    "    SELECT * FROM ks.cf"
                    "    WHERE v IS NOT NULL"
                    "    PRIMARY KEY (v, pk);").get(),
            {view_type::view});

    check_views(
            tools::load_schemas(
                    dbcfg,
                    "CREATE TABLE ks.cf (pk int PRIMARY KEY, v1 int, v2 int); "
                    "CREATE MATERIALIZED VIEW ks.cf_by_v1 AS"
                    "    SELECT * FROM ks.cf"
                    "    WHERE v1 IS NOT NULL"
                    "    PRIMARY KEY (v1, pk);"
                    "CREATE MATERIALIZED VIEW ks.cf_by_v2 AS"
                    "    SELECT * FROM ks.cf"
                    "    WHERE v2 IS NOT NULL"
                    "    PRIMARY KEY (v2, pk);").get(),
            {view_type::view, view_type::view});

    check_views(
            tools::load_schemas(
                    dbcfg,
                    "CREATE TABLE ks.cf (pk int PRIMARY KEY, v int); "
                    "CREATE MATERIALIZED VIEW IF NOT EXISTS ks.cf_by_v AS"
                    "    SELECT * FROM ks.cf"
                    "    WHERE v IS NOT NULL"
                    "    PRIMARY KEY (v, pk);"
                    "CREATE MATERIALIZED VIEW IF NOT EXISTS ks.cf_by_v AS"
                    "    SELECT * FROM ks.cf"
                    "    WHERE v IS NOT NULL"
                    "    PRIMARY KEY (v, pk);").get(),
            {view_type::view});
};

SEASTAR_THREAD_TEST_CASE(test_index) {
    db::config dbcfg;
    dbcfg.rf_rack_valid_keyspaces(true);

    check_views(
            tools::load_schemas(
                dbcfg,
                "CREATE TABLE ks.cf (pk int PRIMARY KEY, v int); "
                "CREATE INDEX cf_by_v ON ks.cf (v);").get(),
            {view_type::index});

    check_views(
            tools::load_schemas(
                dbcfg,
                "CREATE TABLE ks.cf (pk int PRIMARY KEY, v int); "
                "CREATE INDEX ON ks.cf (v);").get(),
            {view_type::index});

    check_views(
            tools::load_schemas(
                dbcfg,
                "CREATE TABLE ks.cf (pk int PRIMARY KEY, v1 int, v2 int); "
                "CREATE INDEX cf_by_v1 ON ks.cf (v1);"
                "CREATE INDEX cf_by_v2 ON ks.cf (v2);").get(),
            {view_type::index, view_type::index});

    check_views(
            tools::load_schemas(
                dbcfg,
                "CREATE TABLE ks.cf (pk int PRIMARY KEY, v1 int, v2 int); "
                "CREATE INDEX ON ks.cf (v1);"
                "CREATE INDEX ON ks.cf (v2);").get(),
            {view_type::index, view_type::index});

    check_views(
            tools::load_schemas(
                dbcfg,
                "CREATE TABLE ks.cf (pk int PRIMARY KEY, v int); "
                "CREATE INDEX IF NOT EXISTS cf_by_v ON ks.cf (v);"
                "CREATE INDEX IF NOT EXISTS cf_by_v ON ks.cf (v);").get(),
            {view_type::index});

    check_views(
            tools::load_schemas(
                dbcfg,
                "CREATE TABLE ks.cf (pk int PRIMARY KEY, v int); "
                "CREATE INDEX IF NOT EXISTS ON ks.cf (v);"
                "CREATE INDEX IF NOT EXISTS ON ks.cf (v);").get(),
            {view_type::index});
}

SEASTAR_THREAD_TEST_CASE(test_mv_index) {
    db::config dbcfg;
    dbcfg.rf_rack_valid_keyspaces(true);

    check_views(
            tools::load_schemas(
                    dbcfg,
                    "CREATE TABLE ks.cf (pk int PRIMARY KEY, v1 int, v2 int); "
                    "CREATE MATERIALIZED VIEW ks.cf_by_v1 AS"
                    "    SELECT * FROM ks.cf"
                    "    WHERE v1 IS NOT NULL"
                    "    PRIMARY KEY (v1, pk);"
                    "CREATE INDEX ON ks.cf (v2);"
                    "CREATE MATERIALIZED VIEW ks.cf_by_v2 AS"
                    "    SELECT * FROM ks.cf"
                    "    WHERE v2 IS NOT NULL"
                    "    PRIMARY KEY (v2, pk);"
                    "CREATE INDEX ON ks.cf (v1);").get(),
            {view_type::view, view_type::index, view_type::view, view_type::index});
}

void check_schema_columns(schema::const_iterator_range_type a, schema::const_iterator_range_type b, bool check_names) {
    BOOST_REQUIRE_EQUAL(std::ranges::distance(a), std::ranges::distance(b));

    for (const auto& [col_a, col_b] : std::views::zip(a, b)) {
        if (check_names) {
            BOOST_REQUIRE_EQUAL(col_a.name(), col_b.name());
        }
        // Type instances can be different for collections and user-types, so compare names instead.
        BOOST_REQUIRE_EQUAL(col_a.type->name(), col_b.type->name());
    }
}

void check_schema_columns(const schema& a, const schema& b, bool check_key_column_names) {
    check_schema_columns(a.columns(column_kind::partition_key), b.columns(column_kind::partition_key), check_key_column_names);
    check_schema_columns(a.columns(column_kind::clustering_key), b.columns(column_kind::clustering_key), check_key_column_names);
    check_schema_columns(a.columns(column_kind::static_column), b.columns(column_kind::static_column), true);
    check_schema_columns(a.columns(column_kind::regular_column), b.columns(column_kind::regular_column), true);
}

void check_sstable_schema(sstables::test_env& env, std::filesystem::path sst_path, const utils::chunked_vector<mutation>& mutations, bool has_scylla_metadata) {
    db::config dbcfg;
    dbcfg.rf_rack_valid_keyspaces(true);

    auto schema = tools::load_schema_from_sstable(dbcfg, sst_path).get();

    check_schema_columns(*schema, *mutations.front().schema(), has_scylla_metadata);

    const auto ed = sstables::parse_path(sst_path, "ks", "tbl");
    const auto dir_path = sst_path.parent_path();
    auto sst = env.make_sstable(schema, dir_path.c_str(), ed.generation, ed.version, ed.format);

    sst->load(schema->get_sharder()).get();

    const auto scylla_metadata = sst->get_scylla_metadata();
    const bool has_schema_metadata = scylla_metadata && scylla_metadata->data.get<sstables::scylla_metadata_type::Schema, sstables::scylla_metadata::sstable_schema>();
    BOOST_REQUIRE_EQUAL(has_schema_metadata, has_scylla_metadata);

    auto rd = assert_that(sst->make_reader(schema, env.make_reader_permit(), query::full_partition_range, schema->full_slice()));
    for (const auto& m : mutations) {
        rd.produces(m);
    }
    rd.produces_end_of_stream();
}

future<> do_test_load_schema_from_sstable(schema_ptr schema, const utils::chunked_vector<mutation>& mutations) {
    return sstables::test_env::do_with_async([&] (sstables::test_env& env) {

        for (const auto& version : sstables::writable_sstable_versions) {
            auto sst = env.make_sstable(schema, version);

            {
                auto mr = make_mutation_reader_from_mutations(schema, env.make_reader_permit(), mutations);
                auto close_mr = deferred_close(mr);

                const auto cfg = env.manager().configure_writer();
                auto wr = sst->get_writer(*schema, mutations.size(), cfg, encoding_stats{});
                mr.consume_in_thread(std::move(wr));
            }

            const auto sst_path = std::filesystem::path(fmt::to_string(sst->get_filename()));

            // Do the check in a separate method to ensure we don't accidentally
            // re-use the original schema.
            check_sstable_schema(env, sst_path, mutations, true);

            // Drop the scylla-metadata to also test the fall-back schema loader from Statistics
            {
                // rm Scylla.db
                const auto scylla_metadata_path = fmt::to_string(sstables::component_name(*sst, component_type::Scylla));
                remove_file(scylla_metadata_path).get();

                // drop Scylla.db from TOC.txt

                const auto toc_path = fmt::to_string(sstables::component_name(*sst, component_type::TOC));
                auto toc_file = open_file_dma(toc_path, open_flags::rw).get();
                auto ifstream = make_file_input_stream(toc_file);

                auto toc = util::read_entire_stream_contiguous(ifstream).get();
                const char scylla_component_name[] = "Scylla.db\n";
                const auto scylla_component_name_pos = toc.find(scylla_component_name, 0, strlen(scylla_component_name));
                BOOST_REQUIRE_NE(scylla_component_name_pos, sstring::npos);
                toc.replace(scylla_component_name_pos, strlen(scylla_component_name), "", 0);

                toc_file.truncate(0).get();

                auto ofstream = make_file_output_stream(toc_file).get();
                ofstream.write(toc).get();
                ofstream.close().get();
            }

            check_sstable_schema(env, sst_path, mutations, false);

        }
    });
}

SEASTAR_TEST_CASE(test_load_schema_from_sstable_random_schema) {
    auto random_spec = tests::make_random_schema_specification(
            get_name(),
            std::uniform_int_distribution<size_t>(1, 4), // partition-key
            std::uniform_int_distribution<size_t>(0, 4), // clustering-key
            std::uniform_int_distribution<size_t>(0, 8), // static
            std::uniform_int_distribution<size_t>(0, 8)); // regular
    auto random_schema = tests::random_schema{tests::random::get_int<uint32_t>(), *random_spec};
    auto schema = random_schema.schema();

    const auto mutations = co_await tests::generate_random_mutations(random_schema, 10);

    co_await do_test_load_schema_from_sstable(std::move(schema), std::move(mutations));
}

SEASTAR_TEST_CASE(test_load_schema_from_sstable_interesting_schema) {
    const sstring ks = get_name();

    const auto string_list = list_type_impl::get_instance(utf8_type, true);
    const auto frozen_string_list = list_type_impl::get_instance(utf8_type, false);
    const auto udt_type = user_type_impl::get_instance(ks, "udt", {"f1", "f2"}, {int32_type, frozen_string_list}, true);
    const auto frozen_udt_type = user_type_impl::get_instance(ks, "frozen_udt", {"f1", "f2"}, {int32_type, frozen_string_list}, false);

    auto schema = schema_builder(ks, "interesting_table")
        .with_column("pk", int32_type, column_kind::partition_key)
        .with_column("ck", reversed_type_impl::get_instance(int32_type), column_kind::clustering_key)
        .with_column("list", string_list, column_kind::regular_column)
        .with_column("frozen_list", frozen_string_list, column_kind::regular_column)
        .with_column("udt", udt_type, column_kind::regular_column)
        .with_column("frozen_udt", frozen_udt_type, column_kind::regular_column)
        .build();

    tests::random_schema random_schema{schema};
    const auto mutations = co_await tests::generate_random_mutations(random_schema, 10);

    co_await do_test_load_schema_from_sstable(std::move(schema), std::move(mutations));
}
