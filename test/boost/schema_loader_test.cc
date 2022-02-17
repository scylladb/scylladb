/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/testing/thread_test_case.hh>

#include "tools/schema_loader.hh"

SEASTAR_THREAD_TEST_CASE(test_empty) {
    BOOST_REQUIRE_THROW(tools::load_schemas("").get(), std::exception);
    BOOST_REQUIRE_THROW(tools::load_schemas(";").get(), std::exception);
}

SEASTAR_THREAD_TEST_CASE(test_keyspace_only) {
    BOOST_REQUIRE_EQUAL(tools::load_schemas("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};").get().size(), 0);
}

SEASTAR_THREAD_TEST_CASE(test_single_table) {
    BOOST_REQUIRE_EQUAL(tools::load_schemas("CREATE TABLE ks.cf (pk int PRIMARY KEY, v int)").get().size(), 1);
    BOOST_REQUIRE_EQUAL(tools::load_schemas("CREATE TABLE ks.cf (pk int PRIMARY KEY, v map<int, int>)").get().size(), 1);
    BOOST_REQUIRE_EQUAL(tools::load_schemas("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}; CREATE TABLE ks.cf (pk int PRIMARY KEY, v int);").get().size(), 1);
}

SEASTAR_THREAD_TEST_CASE(test_keyspace_replication_strategy) {
    BOOST_REQUIRE_EQUAL(tools::load_schemas("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}; CREATE TABLE ks.cf (pk int PRIMARY KEY, v int);").get().size(), 1);
    BOOST_REQUIRE_EQUAL(tools::load_schemas("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}; CREATE TABLE ks.cf (pk int PRIMARY KEY, v int);").get().size(), 1);
    BOOST_REQUIRE_EQUAL(tools::load_schemas("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'mydc1': 1, 'mydc2': 4}; CREATE TABLE ks.cf (pk int PRIMARY KEY, v int);").get().size(), 1);
}

SEASTAR_THREAD_TEST_CASE(test_multiple_tables) {
    BOOST_REQUIRE_EQUAL(tools::load_schemas("CREATE TABLE ks.cf1 (pk int PRIMARY KEY, v int); CREATE TABLE ks.cf2 (pk int PRIMARY KEY, v int)").get().size(), 2);
    BOOST_REQUIRE_EQUAL(tools::load_schemas("CREATE TABLE ks.cf1 (pk int PRIMARY KEY, v int); CREATE TABLE ks.cf2 (pk int PRIMARY KEY, v int);").get().size(), 2);
    BOOST_REQUIRE_EQUAL(tools::load_schemas(
                "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}; "
                "CREATE TABLE ks.cf1 (pk int PRIMARY KEY, v int); "
                "CREATE TABLE ks.cf2 (pk int PRIMARY KEY, v int); "
                "CREATE TABLE ks.cf3 (pk int PRIMARY KEY, v int); "
    ).get().size(), 3);
    BOOST_REQUIRE_EQUAL(tools::load_schemas(
                "CREATE KEYSPACE ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}; "
                "CREATE TABLE ks1.cf (pk int PRIMARY KEY, v int); "
                "CREATE KEYSPACE ks2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}; "
                "CREATE TABLE ks2.cf (pk int PRIMARY KEY, v int); "
    ).get().size(), 2);
}

SEASTAR_THREAD_TEST_CASE(test_udts) {
    BOOST_REQUIRE_EQUAL(tools::load_schemas(
                "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}; "
                "CREATE TYPE ks.type1 (f1 int, f2 text); "
                "CREATE TABLE ks.cf (pk int PRIMARY KEY, v frozen<type1>); "
    ).get().size(), 1);
    BOOST_REQUIRE_EQUAL(tools::load_schemas(
                "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}; "
                "CREATE TYPE ks.type1 (f1 int, f2 text); "
                "CREATE TABLE ks.cf (pk int PRIMARY KEY, v type1); "
    ).get().size(), 1);
    BOOST_REQUIRE_EQUAL(tools::load_schemas(
                "CREATE KEYSPACE ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}; "
                "CREATE TYPE ks1.type1 (f1 int, f2 text); "
                "CREATE TABLE ks1.cf (pk int PRIMARY KEY, v frozen<type1>); "
                "CREATE KEYSPACE ks2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}; "
                "CREATE TYPE ks2.type1 (f1 int, f2 text); "
                "CREATE TABLE ks2.cf (pk int PRIMARY KEY, v frozen<type1>); "
    ).get().size(), 2);
    BOOST_REQUIRE_EQUAL(tools::load_schemas(
                "CREATE TYPE ks.type1 (f1 int, f2 text); "
                "CREATE TABLE ks.cf (pk int PRIMARY KEY, v frozen<type1>); "
    ).get().size(), 1);
    BOOST_REQUIRE_EQUAL(tools::load_schemas(
                "CREATE TYPE ks1.type1 (f1 int, f2 text); "
                "CREATE TABLE ks1.cf (pk int PRIMARY KEY, v frozen<type1>); "
                "CREATE TYPE ks2.type1 (f1 int, f2 text); "
                "CREATE TABLE ks2.cf (pk int PRIMARY KEY, v frozen<type1>); "
    ).get().size(), 2);
}

SEASTAR_THREAD_TEST_CASE(test_dropped_columns) {
    BOOST_REQUIRE_EQUAL(tools::load_schemas(
                "CREATE TABLE ks.cf (pk int PRIMARY KEY, v1 int); "
                "INSERT INTO system_schema.dropped_columns (keyspace_name, table_name, column_name, dropped_time, type) VALUES ('ks', 'cf', 'v2', 1631011979170675, 'int'); "
    ).get().size(), 1);
    BOOST_REQUIRE_THROW(tools::load_schemas(
                "CREATE TABLE ks.cf (pk int PRIMARY KEY, v1 int); "
                "INSERT INTO ks.cf (pk, v1) VALUES (0, 0); "
    ).get(), std::exception);
    BOOST_REQUIRE_THROW(tools::load_schemas(
                "INSERT INTO system_schema.dropped_columns (keyspace_name, table_name, column_name, dropped_time, type) VALUES ('ks', 'cf', 'v2', 1631011979170675, 'int'); "
                "CREATE TABLE ks.cf (pk int PRIMARY KEY, v1 int); "
    ).get(), std::exception);
    BOOST_REQUIRE_THROW(tools::load_schemas(
                "CREATE TABLE ks.cf (pk int PRIMARY KEY, v1 int); "
                "INSERT INTO system_schema.dropped_columns (keyspace_name, table_name, column_name, dropped_time, type) VALUES ('unknown_ks', 'unknown_cf', 'v2', 1631011979170675, 'int'); "
    ).get(), std::exception);
    BOOST_REQUIRE_THROW(tools::load_schemas(
                "CREATE TABLE ks.cf (pk int PRIMARY KEY, v1 int); "
                "INSERT INTO system_schema.dropped_columns (keyspace_name, table_name, column_name, dropped_time, type) VALUES ('unknown_ks', 'cf', 'v2', 1631011979170675, 'int'); "
    ).get(), std::exception);
    BOOST_REQUIRE_THROW(tools::load_schemas(
                "CREATE TABLE ks.cf (pk int PRIMARY KEY, v1 int); "
                "INSERT INTO system_schema.dropped_columns (keyspace_name, table_name, column_name, dropped_time, type) VALUES ('ks', 'unknown_cf', 'v2', 1631011979170675, 'int'); "
    ).get(), std::exception);
}
