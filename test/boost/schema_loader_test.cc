/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "test/lib/scylla_test_case.hh"

#include "db/config.hh"
#include "tools/schema_loader.hh"

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
