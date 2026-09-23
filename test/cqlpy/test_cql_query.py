# -*- coding: utf-8 -*-
# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for various CQL statements, converted from test/boost/cql_query_test.cc.
#############################################################################

from contextlib import contextmanager

from cassandra import InvalidRequest
from cassandra.protocol import ConfigurationException
import pytest

from .util import unique_name


def test_create_keyspace_statement(cql):
    ks = unique_name()
    cql.execute(f"create keyspace {ks} with replication = {{ 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }};")
    try:
        assert list(cql.execute(f"SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = '{ks}'")) == [(ks,)]
    finally:
        cql.execute(f"DROP KEYSPACE {ks}")


def table_exists(cql, keyspace, table):
    return len(list(cql.execute(
        f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{keyspace}' AND table_name = '{table}'"))) == 1


def test_create_table_statement(cql, test_keyspace):
    users = unique_name()
    cf = unique_name()
    cql.execute(f"create table {test_keyspace}.{users} (user_name varchar PRIMARY KEY, birth_year bigint);")
    try:
        assert table_exists(cql, test_keyspace, users)
        cql.execute(f"create table {test_keyspace}.{cf} (id int primary key, m map<int, int>, s set<text>, l list<uuid>);")
        try:
            assert table_exists(cql, test_keyspace, cf)
        finally:
            cql.execute(f"DROP TABLE {test_keyspace}.{cf}")
    finally:
        cql.execute(f"DROP TABLE {test_keyspace}.{users}")


def test_create_table_with_id_statement(cql, test_keyspace, scylla_only):
    tbl = f"{test_keyspace}.{unique_name()}"
    tbl2 = f"{test_keyspace}.{unique_name()}"
    cql.execute(f"CREATE TABLE {tbl} (a int, b int, PRIMARY KEY (a))")
    ks, name = tbl.split(".")
    id = cql.execute(f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = '{name}'").one().id
    cql.execute(f"DROP TABLE {tbl}")
    with pytest.raises(InvalidRequest):
        cql.execute(f"SELECT * FROM {tbl}")
    cql.execute(f"CREATE TABLE {tbl} (a int, b int, PRIMARY KEY (a)) WITH id='{id}'")
    try:
        assert list(cql.execute(f"SELECT * FROM {tbl}")) == []
        with pytest.raises(InvalidRequest):
            cql.execute(f"CREATE TABLE {tbl2} (a int, b int, PRIMARY KEY (a)) WITH id='{id}'")
        with pytest.raises(ConfigurationException):
            cql.execute(f"CREATE TABLE {tbl2} (a int, b int, PRIMARY KEY (a)) WITH id='55'")
        with pytest.raises(ConfigurationException):
            cql.execute(f"ALTER TABLE {tbl} WITH id='f2a8c099-e723-48cb-8cd9-53e647a011a3'")
    finally:
        cql.execute(f"DROP TABLE {tbl}")


# The cluster-scope config overrides (ALTER CLUSTER) are global state of the
# shared test server, so tests that set them must remove them when done.
@contextmanager
def cluster_config_cleanup(cql):
    try:
        yield
    finally:
        cql.execute("ALTER CLUSTER WITH auto_repair_enabled = null")


# The key of the single row in system_schema.scylla_clusters
# (schema_tables::CLUSTER_CONFIG_SINGLETON_KEY).
cluster_configs_query = "SELECT configs FROM system_schema.scylla_clusters WHERE cluster_name = 'cluster'"


# Returns the configs column of each row returned by the query, as dicts
# (None for a null/empty configs map).
def fetch_configs(cql, query):
    return [dict(row.configs) if row.configs is not None else None for row in cql.execute(query)]


def test_alter_cluster_with_persists_cluster_config_override(cql, scylla_only):
    with cluster_config_cleanup(cql):
        cql.execute("ALTER CLUSTER WITH auto_repair_enabled = true")

        assert fetch_configs(cql, cluster_configs_query) == [{'auto_repair_enabled': 'true'}]

        # The string literal 'null' is a value, not the removal keyword: for a boolean
        # option it is rejected as an invalid value and the stored override stays intact.
        with pytest.raises(InvalidRequest):
            cql.execute("ALTER CLUSTER WITH auto_repair_enabled = 'null'")

        cql.execute("ALTER CLUSTER WITH auto_repair_enabled = null")

        assert fetch_configs(cql, cluster_configs_query) == []
