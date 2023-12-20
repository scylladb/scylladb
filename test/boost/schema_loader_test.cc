/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "test/lib/log.hh"
#include "test/lib/scylla_test_case.hh"

#include "db/config.hh"
#include "index/secondary_index_manager.hh"
#include "tools/schema_loader.hh"
#include "view_info.hh"

SEASTAR_THREAD_TEST_CASE(test_empty) {
    db::config dbcfg;
    BOOST_REQUIRE_THROW(tools::load_schemas(dbcfg, "").get(), std::exception);
    BOOST_REQUIRE_THROW(tools::load_schemas(dbcfg, ";").get(), std::exception);
}

SEASTAR_THREAD_TEST_CASE(test_keyspace_only) {
    db::config dbcfg;
    BOOST_REQUIRE_EQUAL(tools::load_schemas(dbcfg, "CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};").get().size(), 0);
}

SEASTAR_THREAD_TEST_CASE(test_single_table) {
    db::config dbcfg;
    BOOST_REQUIRE_EQUAL(tools::load_schemas(dbcfg, "CREATE TABLE ks.cf (pk int PRIMARY KEY, v int)").get().size(), 1);
    BOOST_REQUIRE_EQUAL(tools::load_schemas(dbcfg, "CREATE TABLE ks.cf (pk int PRIMARY KEY, v map<int, int>)").get().size(), 1);
    BOOST_REQUIRE_EQUAL(tools::load_schemas(dbcfg, "CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}; CREATE TABLE ks.cf (pk int PRIMARY KEY, v int);").get().size(), 1);
}

SEASTAR_THREAD_TEST_CASE(test_keyspace_replication_strategy) {
    db::config dbcfg;
    BOOST_REQUIRE_EQUAL(tools::load_schemas(dbcfg, "CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}; CREATE TABLE ks.cf (pk int PRIMARY KEY, v int);").get().size(), 1);
    BOOST_REQUIRE_EQUAL(tools::load_schemas(dbcfg, "CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}; CREATE TABLE ks.cf (pk int PRIMARY KEY, v int);").get().size(), 1);
    BOOST_REQUIRE_EQUAL(tools::load_schemas(dbcfg, "CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'mydc1': 1, 'mydc2': 4}; CREATE TABLE ks.cf (pk int PRIMARY KEY, v int);").get().size(), 1);
}

SEASTAR_THREAD_TEST_CASE(test_multiple_tables) {
    db::config dbcfg;
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
