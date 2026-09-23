# -*- coding: utf-8 -*-
# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for various CQL statements, converted from test/boost/cql_query_test.cc.
#############################################################################

from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
import json
import re
import struct
from uuid import UUID

from cassandra import InvalidRequest, Unauthorized
from cassandra.cluster import NoHostAvailable
from cassandra.concurrent import execute_concurrent_with_args
import cassandra.cqltypes
from cassandra.protocol import ConfigurationException, SyntaxException
from cassandra.query import PreparedStatement, SimpleStatement, UNSET_VALUE
from cassandra.util import Date, Duration, Time
import pytest

from . import nodetool
from .util import config_value_context, new_session, new_test_keyspace, new_test_table, new_type, new_user, unique_name


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


# A fresh keyspace per test, so keyspace-scope overrides don't leak.
def new_config_test_keyspace(cql):
    return new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")


def keyspace_configs_query(ks):
    return f"SELECT configs FROM system_schema.scylla_keyspaces WHERE keyspace_name = '{ks}'"


def table_configs_query(ks, table):
    return f"SELECT configs FROM system_schema.scylla_tables WHERE keyspace_name = '{ks}' AND table_name = '{table}'"


# The CQL BOOLEAN token is case-insensitive but case-preserving, so `= TRUE` reaches the
# registry as "TRUE". Every scope must accept it and persist the canonical lowercase form,
# so that consumers can compare the stored text without re-normalizing it.
def test_cluster_config_boolean_value_is_case_insensitive_and_stored_canonically(cql, scylla_only):
    with new_config_test_keyspace(cql) as ks, cluster_config_cleanup(cql):
        cql.execute(f"CREATE TABLE {ks}.tbl (pk int PRIMARY KEY)")

        cql.execute("ALTER CLUSTER WITH auto_repair_enabled = TRUE")
        assert fetch_configs(cql, cluster_configs_query) == [{'auto_repair_enabled': 'true'}]

        cql.execute(f"ALTER KEYSPACE {ks} WITH auto_repair_enabled = False")
        assert fetch_configs(cql, keyspace_configs_query(ks)) == [{'auto_repair_enabled': 'false'}]

        cql.execute(f"ALTER TABLE {ks}.tbl WITH auto_repair_enabled = TrUe")
        assert fetch_configs(cql, table_configs_query(ks, 'tbl')) == [{'auto_repair_enabled': 'true'}]

        # NULL removal stays case-insensitive too.
        cql.execute(f"ALTER TABLE {ks}.tbl WITH auto_repair_enabled = NULL")
        assert fetch_configs(cql, table_configs_query(ks, 'tbl')) == [None]


# The grammar accepts `<ident> = <mapLiteral>` for any property name, so a map value must
# surface as a CQL error rather than escaping as std::bad_variant_access (which the client
# would see as a generic ServerError).
def test_cluster_config_rejects_map_value_with_cql_error(cql, scylla_only):
    with new_config_test_keyspace(cql) as ks:
        cql.execute(f"CREATE TABLE {ks}.tbl (pk int PRIMARY KEY)")

        with pytest.raises(SyntaxException):
            cql.execute(f"ALTER TABLE {ks}.tbl WITH auto_repair_enabled = {{'a': 'b'}}")
        with pytest.raises(SyntaxException):
            cql.execute(f"ALTER KEYSPACE {ks} WITH auto_repair_enabled = {{'a': 'b'}}")
        with pytest.raises(SyntaxException):
            cql.execute(f"CREATE TABLE {ks}.tbl2 (pk int PRIMARY KEY) WITH auto_repair_enabled = {{'a': 'b'}}")


# A non-boolean value for a boolean-typed option must be rejected at every scope.
def test_cluster_config_rejects_invalid_boolean_value(cql, scylla_only):
    with new_config_test_keyspace(cql) as ks, cluster_config_cleanup(cql):
        cql.execute(f"CREATE TABLE {ks}.tbl (pk int PRIMARY KEY)")

        with pytest.raises(InvalidRequest):
            cql.execute("ALTER CLUSTER WITH auto_repair_enabled = 'yes'")
        with pytest.raises(ConfigurationException):
            cql.execute(f"ALTER KEYSPACE {ks} WITH auto_repair_enabled = 'yes'")
        with pytest.raises(ConfigurationException):
            cql.execute(f"ALTER TABLE {ks}.tbl WITH auto_repair_enabled = 'yes'")


# `= NULL` removes a stored override; the string literal `= 'null'` is an ordinary value.
# propertyValue renders both as the text "null", so only the parser's null-keyword flag
# tells them apart - without it a quoted 'null' would silently erase the override at the
# table and keyspace scopes instead of being rejected as an invalid boolean (ALTER CLUSTER
# goes through configPropertyValue and has always distinguished the two).
def test_cluster_config_quoted_null_is_a_value_not_the_removal_keyword(cql, scylla_only):
    with new_config_test_keyspace(cql) as ks:
        keyspace_configs = keyspace_configs_query(ks)
        table_configs = table_configs_query(ks, 'tbl')

        cql.execute(f"CREATE TABLE {ks}.tbl (pk int PRIMARY KEY)")
        cql.execute(f"ALTER KEYSPACE {ks} WITH auto_repair_enabled = true")
        cql.execute(f"ALTER TABLE {ks}.tbl WITH auto_repair_enabled = true")

        with pytest.raises(ConfigurationException):
            cql.execute(f"ALTER KEYSPACE {ks} WITH auto_repair_enabled = 'null'")
        with pytest.raises(ConfigurationException):
            cql.execute(f"ALTER TABLE {ks}.tbl WITH auto_repair_enabled = 'null'")
        with pytest.raises(ConfigurationException):
            cql.execute(f"CREATE TABLE {ks}.tbl2 (pk int PRIMARY KEY) WITH auto_repair_enabled = 'null'")

        # The rejected statements left both overrides in place.
        assert fetch_configs(cql, keyspace_configs) == [{'auto_repair_enabled': 'true'}]
        assert fetch_configs(cql, table_configs) == [{'auto_repair_enabled': 'true'}]

        # The bare keyword still removes.
        cql.execute(f"ALTER TABLE {ks}.tbl WITH auto_repair_enabled = null")
        assert fetch_configs(cql, table_configs) == [None]
        assert fetch_configs(cql, keyspace_configs) == [{'auto_repair_enabled': 'true'}]


# CREATE TABLE ... WITH <config_key> = ... must persist the override, not accept it and
# silently drop it. cf_prop_defs::validate() allow-lists registry keys for every statement
# that uses cf_prop_defs, but only ALTER TABLE used to write them, so DESCRIBE output
# (which folds the effective value into CREATE TABLE) could not be replayed without losing
# every table-scope override.
def test_create_table_persists_cluster_config_property(cql, scylla_only):
    with new_config_test_keyspace(cql) as ks:
        cql.execute(f"CREATE TABLE {ks}.tbl (pk int PRIMARY KEY) WITH auto_repair_enabled = TRUE")

        # Stored in the out-of-band configs column, canonicalized to lowercase.
        assert list(cql.execute(f"SELECT configs['auto_repair_enabled'] FROM system_schema.scylla_tables "
                                f"WHERE keyspace_name = '{ks}' AND table_name = 'tbl'")) == [('true',)]

        # A table created without the property stores nothing.
        cql.execute(f"CREATE TABLE {ks}.plain (pk int PRIMARY KEY)")
        assert list(cql.execute(f"SELECT configs['auto_repair_enabled'] FROM system_schema.scylla_tables "
                                f"WHERE keyspace_name = '{ks}' AND table_name = 'plain'")) == [(None,)]


# Regression test for the superuser check on node-oriented ALTER statements: it must be
# awaited, not resolved with a blocking future::get(). With authentication enabled and the
# permissions cache disabled, has_superuser() returns a non-ready future, so a blocking get()
# outside a seastar::thread would abort the node. Exercises both the superuser-allowed path
# (default cassandra user) and the non-superuser-rejected path.
# cqlpy's Scylla already runs with PasswordAuthenticator and CassandraAuthorizer; the
# permissions cache is disabled here via the live-updatable permissions_validity_in_ms.
def test_alter_cluster_superuser_check_is_async_with_auth_enabled(cql, scylla_only):
    with config_value_context(cql, 'permissions_validity_in_ms', '0'), cluster_config_cleanup(cql):
        # Default logged-in user is a superuser: allowed, and must not crash on the async
        # has_superuser() lookup.
        cql.execute("ALTER CLUSTER WITH auto_repair_enabled = true")

        # A non-superuser is rejected with Unauthorized (also via the async path).
        with new_user(cql) as username:
            with new_session(cql, username) as user_session:
                with pytest.raises(Unauthorized):
                    user_session.execute("ALTER CLUSTER WITH auto_repair_enabled = false")


def test_alter_schema_with_persists_scope_configs(cql, scylla_only):
    with new_config_test_keyspace(cql) as ks:
        cql.execute(f"CREATE TABLE {ks}.tbl (pk int PRIMARY KEY, v int)")

        cql.execute(f"ALTER KEYSPACE {ks} WITH auto_repair_enabled = true")
        cql.execute(f"ALTER TABLE {ks}.tbl WITH auto_repair_enabled = false")

        assert fetch_configs(cql, keyspace_configs_query(ks)) == [{'auto_repair_enabled': 'true'}]
        assert fetch_configs(cql, table_configs_query(ks, 'tbl')) == [{'auto_repair_enabled': 'false'}]

        cql.execute(f"ALTER TABLE {ks}.tbl WITH auto_repair_enabled = null")

        assert fetch_configs(cql, keyspace_configs_query(ks)) == [{'auto_repair_enabled': 'true'}]
        assert fetch_configs(cql, table_configs_query(ks, 'tbl')) == [None]


# The create_statement of a single-row DESCRIBE result.
def describe_create_statement(cql, query):
    rows = list(cql.execute(query))
    assert len(rows) == 1
    assert rows[0].create_statement is not None
    return rows[0].create_statement


def test_describe_schema_with_inherited_auto_repair_scope_config(cql, scylla_only):
    with new_config_test_keyspace(cql) as ks, cluster_config_cleanup(cql):
        cql.execute(f"CREATE TABLE {ks}.tbl (pk int PRIMARY KEY)")
        cql.execute("ALTER CLUSTER WITH auto_repair_enabled = true")

        # A live property is emitted only for what is stored at the described object's own
        # scope: nothing is stored at the keyspace or table yet, so the option appears as a
        # commented-out property carrying the effective value, with a trailing provenance
        # comment. The terminating ';' sits on its own line so the trailing comment lexes.
        keyspace_desc = describe_create_statement(cql, f"DESCRIBE ONLY KEYSPACE {ks}")
        assert "\n    AND auto_repair_enabled" not in keyspace_desc
        assert "\n    -- AND auto_repair_enabled = true  -- from cluster (keyspace=NULL, cluster=true)\n;" in keyspace_desc
        assert keyspace_desc.endswith(';')

        table_desc = describe_create_statement(cql, f"DESCRIBE TABLE {ks}.tbl")
        assert "\n    AND auto_repair_enabled" not in table_desc
        assert "\n    -- AND auto_repair_enabled = true  -- from cluster (table=NULL, keyspace=NULL, cluster=true)\n;" in table_desc
        assert table_desc.endswith(';')

        # WITH INTERNALS output is pure replayable CQL: no comment block, and no property
        # either - nothing is stored at table scope, and the cluster-scope override is
        # carried by the ALTER CLUSTER block of DESC SCHEMA instead.
        internals_desc = describe_create_statement(cql, f"DESCRIBE TABLE {ks}.tbl WITH INTERNALS")
        assert "auto_repair_enabled" not in internals_desc

        cql.execute(f"ALTER KEYSPACE {ks} WITH auto_repair_enabled = false")
        keyspace_desc = describe_create_statement(cql, f"DESCRIBE ONLY KEYSPACE {ks}")
        # Now stored at the keyspace itself: live property, annotated with the chain.
        assert "\n    AND auto_repair_enabled = false  -- from keyspace (keyspace=false, cluster=true)\n;" in keyspace_desc
        assert "-- AND auto_repair_enabled" not in keyspace_desc

        table_desc = describe_create_statement(cql, f"DESCRIBE TABLE {ks}.tbl")
        # The table only inherits the keyspace override: commented-out property.
        assert "\n    AND auto_repair_enabled" not in table_desc
        assert "\n    -- AND auto_repair_enabled = false  -- from keyspace (table=NULL, keyspace=false, cluster=true)\n;" in table_desc

        cql.execute(f"ALTER TABLE {ks}.tbl WITH auto_repair_enabled = true")
        table_desc = describe_create_statement(cql, f"DESCRIBE TABLE {ks}.tbl")
        # Now stored at the table itself: live property, annotated with the chain.
        assert ";\n AND auto_repair_enabled" not in table_desc
        assert "\n    AND auto_repair_enabled = true  -- from table (table=true, keyspace=false, cluster=true)\n;" in table_desc
        assert "-- AND auto_repair_enabled" not in table_desc

        # WITH INTERNALS keeps the stored override as a property, still without comments.
        internals_desc = describe_create_statement(cql, f"DESCRIBE TABLE {ks}.tbl WITH INTERNALS")
        assert "auto_repair_enabled = true" in internals_desc
        assert "-- AND auto_repair_enabled" not in internals_desc
        assert "-- from" not in internals_desc

        cql.execute(f"ALTER TABLE {ks}.tbl WITH auto_repair_enabled = null")
        table_desc = describe_create_statement(cql, f"DESCRIBE TABLE {ks}.tbl")
        assert "\n    AND auto_repair_enabled" not in table_desc
        assert "\n    -- AND auto_repair_enabled = false  -- from keyspace (table=NULL, keyspace=false, cluster=true)\n;" in table_desc

        cql.execute(f"ALTER KEYSPACE {ks} WITH auto_repair_enabled = null")
        keyspace_desc = describe_create_statement(cql, f"DESCRIBE ONLY KEYSPACE {ks}")
        assert "\n    AND auto_repair_enabled" not in keyspace_desc
        assert "\n    -- AND auto_repair_enabled = true  -- from cluster (keyspace=NULL, cluster=true)\n;" in keyspace_desc

        table_desc = describe_create_statement(cql, f"DESCRIBE TABLE {ks}.tbl")
        assert "\n    AND auto_repair_enabled" not in table_desc
        assert "\n    -- AND auto_repair_enabled = true  -- from cluster (table=NULL, keyspace=NULL, cluster=true)\n;" in table_desc


# All create_statement cells of a multi-row describe (e.g. DESC SCHEMA), in row order.
def describe_create_statements(cql, query):
    return [row.create_statement for row in cql.execute(query) if row.create_statement is not None]


# The DESC SCHEMA cluster-config block: stored cluster-scope overrides lead the dump,
# before any keyspace, in all tiers - the executable slot is stored-only, so without this
# block a schema dump would silently lose cluster scope. The plain tier annotates each
# statement with a trailing provenance comment (and so keeps a final newline, which the
# COMMENT lexer token requires); WITH INTERNALS stays pure CQL.
def test_describe_schema_emits_cluster_config_block(cql, scylla_only):
    with new_config_test_keyspace(cql) as ks, cluster_config_cleanup(cql):
        cql.execute(f"CREATE TABLE {ks}.tbl (pk int PRIMARY KEY)")

        # Nothing stored at cluster scope: no cluster block at all.
        for stmt in describe_create_statements(cql, "DESCRIBE SCHEMA"):
            assert "ALTER CLUSTER" not in stmt

        cql.execute("ALTER CLUSTER WITH auto_repair_enabled = false")

        schema_stmts = describe_create_statements(cql, "DESCRIBE SCHEMA")
        assert schema_stmts
        assert schema_stmts[0] == "ALTER CLUSTER WITH auto_repair_enabled = false;  -- from cluster (cluster=false)\n"

        internals_stmts = describe_create_statements(cql, "DESCRIBE SCHEMA WITH INTERNALS")
        assert internals_stmts
        assert internals_stmts[0] == "ALTER CLUSTER WITH auto_repair_enabled = false;"


# Every emitted create_statement must replay verbatim as a single request, comment lines
# included. This is what moving the terminating ';' to its own line buys: the CQL COMMENT
# lexer token needs a newline terminator, so a trailing comment with no final newline
# fails to parse.
def test_describe_config_output_replays_verbatim(cql, scylla_only):
    with new_config_test_keyspace(cql) as ks, cluster_config_cleanup(cql):
        cql.execute(f"CREATE TABLE {ks}.tbl (pk int PRIMARY KEY)")
        cql.execute("ALTER CLUSTER WITH auto_repair_enabled = false")
        cql.execute(f"ALTER TABLE {ks}.tbl WITH auto_repair_enabled = true")

        table_desc = describe_create_statement(cql, f"DESCRIBE TABLE {ks}.tbl")
        assert "\n    AND auto_repair_enabled = true  -- from table (table=true, keyspace=NULL, cluster=false)\n;" in table_desc

        cql.execute(f"DROP TABLE {ks}.tbl")
        cql.execute(table_desc)

        # The replayed CREATE stored the table-scope override again.
        assert list(cql.execute(f"SELECT configs['auto_repair_enabled'] FROM system_schema.scylla_tables "
                                f"WHERE keyspace_name = '{ks}' AND table_name = 'tbl'")) == [('true',)]


# Changes cluster-scope configuration with ALTER CLUSTER while active, and
# restores the original stored cluster-scope value (or its absence) at exit,
# so that the cluster-wide state doesn't leak into other tests.
@contextmanager
def cluster_config_context(cql, option, value):
    rows = list(cql.execute("SELECT configs FROM system_schema.scylla_clusters"))
    original = None
    if rows and rows[0].configs:
        original = rows[0].configs.get(option)
    cql.execute(f"ALTER CLUSTER WITH {option} = {value}")
    try:
        yield
    finally:
        cql.execute(f"ALTER CLUSTER WITH {option} = {original if original is not None else 'null'}")


# The commented-out property is a real, executable property behind its comment marker:
# replaying the describe output as-is stores nothing at the described scope (inheritance
# is preserved), while erasing just the leading "-- " pins the effective value there.
def test_describe_config_uncomment_pins_inherited_value(cql, this_dc, scylla_only):
    with new_test_keyspace(cql, f"WITH replication = {{'class': 'NetworkTopologyStrategy', '{this_dc}': 1}}") as ks:
        table_name = unique_name()
        table = f"{ks}.{table_name}"
        cql.execute(f"CREATE TABLE {table} (pk int PRIMARY KEY)")
        try:
            with cluster_config_context(cql, "auto_repair_enabled", "true"):
                table_desc = describe_create_statement(cql, f"DESCRIBE TABLE {table}")
                commented = "\n    -- AND auto_repair_enabled = true  -- from cluster (table=NULL, keyspace=NULL, cluster=true)"
                pos = table_desc.find(commented)
                assert pos != -1

                # Replaying as-is keeps the table purely inheriting: no stored override.
                cql.execute(f"DROP TABLE {table}")
                cql.execute(table_desc)
                rows = list(cql.execute(f"SELECT configs FROM system_schema.scylla_tables "
                                        f"WHERE keyspace_name = '{ks}' AND table_name = '{table_name}'"))
                assert len(rows) == 1 and not rows[0].configs

                # Erasing the comment marker turns the line into a live property; the trailing
                # provenance stays a valid inline comment. Replaying now pins the value.
                commented_marker = "\n    -- AND"
                pinned_desc = table_desc[:pos] + "\n    AND" + table_desc[pos + len(commented_marker):]
                cql.execute(f"DROP TABLE {table}")
                cql.execute(pinned_desc)
                rows = list(cql.execute(f"SELECT configs['auto_repair_enabled'] FROM system_schema.scylla_tables "
                                        f"WHERE keyspace_name = '{ks}' AND table_name = '{table_name}'"))
                assert [tuple(r) for r in rows] == [("true",)]
        finally:
            cql.execute(f"DROP TABLE IF EXISTS {table}")


TWCS_1_MINUTE = ("compaction = {'class': 'TimeWindowCompactionStrategy', "
                 "'compaction_window_size': '1', 'compaction_window_unit': 'MINUTES'}")

def test_create_twcs_table_no_ttl(cql, test_keyspace, scylla_only):
    tbl = f"{test_keyspace}.{unique_name()}"
    tbl2 = f"{test_keyspace}.{unique_name()}"
    tbl3 = f"{test_keyspace}.{unique_name()}"
    try:
        # Create a TWCS table with no TTL defined
        with config_value_context(cql, 'restrict_twcs_without_default_ttl', 'warn'):
            cql.execute(f"CREATE TABLE {tbl} (a int, b int, PRIMARY KEY (a)) WITH {TWCS_1_MINUTE}")
            # Ensure ALTER TABLE works
            cql.execute(f"ALTER TABLE {tbl} WITH default_time_to_live=60")
            # LiveUpdate and enforce TTL to be defined
            cql.execute("UPDATE system.config SET value='true' WHERE name='restrict_twcs_without_default_ttl'")
            # default_time_to_live option is required
            with pytest.raises(ConfigurationException):
                cql.execute(f"CREATE TABLE {tbl2} (a int, b int, PRIMARY KEY (a)) WITH {TWCS_1_MINUTE}")
            cql.execute(f"CREATE TABLE {tbl2} (a int, b int, PRIMARY KEY (a)) WITH {TWCS_1_MINUTE} AND default_time_to_live=60")
            # default_time_to_live option must not be set to 0.
            with pytest.raises(ConfigurationException):
                cql.execute(f"ALTER TABLE {tbl} WITH default_time_to_live=0")
            # LiveUpdate and disable the check, then try table creation again
            cql.execute("UPDATE system.config SET value='false' WHERE name='restrict_twcs_without_default_ttl'")
            cql.execute(f"CREATE TABLE {tbl3} (a int, b int, PRIMARY KEY (a)) WITH {TWCS_1_MINUTE}")
            # LiveUpdate back, and ensure that unrelated CQL requests are able to get through
            cql.execute("UPDATE system.config SET value='true' WHERE name='restrict_twcs_without_default_ttl'")
            cql.execute(f"ALTER TABLE {tbl3} WITH gc_grace_seconds=0")
    finally:
        for t in [tbl, tbl2, tbl3]:
            cql.execute(f"DROP TABLE IF EXISTS {t}")


TWCS_1_HOUR = ("compaction = {'class': 'TimeWindowCompactionStrategy', "
               "'compaction_window_size': '1', 'compaction_window_unit': 'HOURS'}")

def test_twcs_max_window(cql, test_keyspace, scylla_only):
    tbl = f"{test_keyspace}.{unique_name()}"
    tbl2 = f"{test_keyspace}.{unique_name()}"
    try:
        # Hardcode restriction to max 10 windows
        with config_value_context(cql, 'twcs_max_window_count', '10'):
            # Creating a TWCS table with a large number of windows/buckets should fail
            with pytest.raises(ConfigurationException):
                cql.execute(f"CREATE TABLE {tbl} (a int, b int, PRIMARY KEY (a)) WITH {TWCS_1_HOUR} AND default_time_to_live=86400")
            # However the creation of a table within bounds should succeed
            cql.execute(f"CREATE TABLE {tbl} (a int, b int, PRIMARY KEY (a)) WITH {TWCS_1_HOUR} AND default_time_to_live=36000")
            # LiveUpdate - Disable check
            cql.execute("UPDATE system.config SET value='0' WHERE name='twcs_max_window_count'")
            cql.execute(f"CREATE TABLE {tbl2} (a int, b int, PRIMARY KEY (a)) WITH {TWCS_1_HOUR} AND default_time_to_live=864000000")
    finally:
        for t in [tbl, tbl2]:
            cql.execute(f"DROP TABLE IF EXISTS {t}")


def test_twcs_restrictions_mixed(cql, test_keyspace, scylla_only):
    tables = {i: f"{test_keyspace}.{unique_name()}" for i in range(1, 13)}
    def set_max_windows(n):
        cql.execute(f"UPDATE system.config SET value='{n}' WHERE name='twcs_max_window_count'")
    twcs = "{'class': 'TimeWindowCompactionStrategy'}"
    stcs = "{'class': 'SizeTieredCompactionStrategy'}"
    try:
        # Hardcode restriction to max 10 windows
        with config_value_context(cql, 'twcs_max_window_count', '10'):
            # Scenario 1: STCS->TWCS with no TTL defined
            cql.execute(f"CREATE TABLE {tables[1]} (a int PRIMARY KEY, b int)")
            cql.execute(f"ALTER TABLE {tables[1]} WITH compaction = {twcs}")

            # Scenario 2: STCS->TWCS with small TTL. Note: TWCS default window size is 1 day (86400s)
            cql.execute(f"CREATE TABLE {tables[2]} (a int PRIMARY KEY, b int) WITH default_time_to_live = 60")
            cql.execute(f"ALTER TABLE {tables[2]} WITH compaction = {twcs}")

            # Scenario 3: STCS->TWCS with large TTL value
            cql.execute(f"CREATE TABLE {tables[3]} (a int PRIMARY KEY, b int) WITH default_time_to_live = 8640000")
            with pytest.raises(ConfigurationException):
                cql.execute(f"ALTER TABLE {tables[3]} WITH compaction = {twcs}")

            # Scenario 4: TWCS table with small to large TTL
            cql.execute(f"CREATE TABLE {tables[4]} (a int PRIMARY KEY, b int) WITH compaction = {twcs} AND default_time_to_live = 60")
            with pytest.raises(ConfigurationException):
                cql.execute(f"ALTER TABLE {tables[4]} WITH default_time_to_live = 86400000")

            # Scenario 5: No TTL TWCS to large TTL and then small TTL
            cql.execute(f"CREATE TABLE {tables[5]} (a int PRIMARY KEY, b int) WITH compaction = {twcs}")
            with pytest.raises(ConfigurationException):
                cql.execute(f"ALTER TABLE {tables[5]} WITH default_time_to_live = 86400000")
            cql.execute(f"ALTER TABLE {tables[5]} WITH default_time_to_live = 60")

            # Scenario 6: twcs_max_window_count LiveUpdate - Decrease TTL
            set_max_windows(0)
            cql.execute(f"CREATE TABLE {tables[6]} (a int PRIMARY KEY, b int) WITH compaction = {twcs} AND default_time_to_live = 86400000")
            set_max_windows(50)
            cql.execute(f"ALTER TABLE {tables[6]} WITH default_time_to_live = 60")

            # Scenario 7: twcs_max_window_count LiveUpdate - Switch CompactionStrategy
            set_max_windows(0)
            cql.execute(f"CREATE TABLE {tables[7]} (a int PRIMARY KEY, b int) WITH compaction = {twcs} AND default_time_to_live = 86400000")
            set_max_windows(50)
            cql.execute(f"ALTER TABLE {tables[7]} WITH compaction = {stcs}")

            # Scenario 8: No TTL TWCS table to STCS
            cql.execute(f"CREATE TABLE {tables[8]} (a int PRIMARY KEY, b int) WITH compaction = {twcs}")
            cql.execute(f"ALTER TABLE {tables[8]} WITH compaction = {stcs}")

            # Scenario 9: Large TTL TWCS table, modify attribute other than compaction and default_time_to_live
            set_max_windows(0)
            cql.execute(f"CREATE TABLE {tables[9]} (a int PRIMARY KEY, b int) WITH compaction = {twcs} AND default_time_to_live = 86400000")
            set_max_windows(50)
            cql.execute(f"ALTER TABLE {tables[9]} WITH gc_grace_seconds = 0")

            # Scenario 10: Large TTL STCS table, fail to switch to TWCS with no TTL
            cql.execute(f"CREATE TABLE {tables[10]} (a int PRIMARY KEY, b int) WITH default_time_to_live = 8640000")
            with pytest.raises(ConfigurationException):
                cql.execute(f"ALTER TABLE {tables[10]} WITH compaction = {twcs}")
            cql.execute(f"ALTER TABLE {tables[10]} WITH compaction = {twcs} AND default_time_to_live = 0")

            # Scenario 11: Ensure default_time_to_live updates reference existing table properties
            cql.execute(f"CREATE TABLE {tables[11]} (a int PRIMARY KEY, b int) WITH compaction = "
                        "{'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '1', "
                        "'compaction_window_unit': 'MINUTES'} AND default_time_to_live=3000")
            with pytest.raises(ConfigurationException):
                cql.execute(f"ALTER TABLE {tables[11]} WITH default_time_to_live=3600")
            cql.execute(f"ALTER TABLE {tables[11]} WITH compaction = {{'class': 'TimeWindowCompactionStrategy', "
                        "'compaction_window_size': '2', 'compaction_window_unit': 'MINUTES'}")
            cql.execute(f"ALTER TABLE {tables[11]} WITH default_time_to_live=3600")

            # Scenario 12: Ensure that window sizes <= 0 are forbidden
            with pytest.raises(ConfigurationException):
                cql.execute(f"CREATE TABLE {tables[12]} (a int PRIMARY KEY, b int) WITH compaction = "
                            "{'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '0'}")
            with pytest.raises(ConfigurationException):
                cql.execute(f"CREATE TABLE {tables[12]} (a int PRIMARY KEY, b int) WITH compaction = "
                            "{'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': -65535}")
            cql.execute(f"CREATE TABLE {tables[12]} (a int PRIMARY KEY, b int) WITH compaction = "
                        "{'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': 1}")
            with pytest.raises(ConfigurationException):
                cql.execute(f"ALTER TABLE {tables[12]} WITH compaction = {{'class': 'TimeWindowCompactionStrategy', "
                            "'compaction_window_size': 0}")
    finally:
        for t in tables.values():
            cql.execute(f"DROP TABLE IF EXISTS {t}")


def test_drop_table_with_si_and_mv(cql, this_dc):
    ks = unique_name()
    cql.execute(f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', '{this_dc}': 1}}")
    try:
        tbl = f"{ks}.tbl"
        cql.execute(f"CREATE TABLE {tbl} (a int, b int, c float, PRIMARY KEY (a))")
        cql.execute(f"CREATE INDEX idx1 ON {tbl} (b)")
        cql.execute(f"CREATE INDEX idx2 ON {tbl} (c)")
        cql.execute(f"CREATE MATERIALIZED VIEW {ks}.tbl_view AS SELECT c FROM {tbl} WHERE c IS NOT NULL PRIMARY KEY (c, a)")
        # dropping a table with materialized views is prohibited
        with pytest.raises(InvalidRequest):
            cql.execute(f"DROP TABLE {tbl}")
        cql.execute(f"DROP MATERIALIZED VIEW {ks}.tbl_view")
        # dropping a table with secondary indexes is fine
        cql.execute(f"DROP TABLE {tbl}")

        cql.execute(f"CREATE TABLE {tbl} (a int, b int, c float, PRIMARY KEY (a))")
        cql.execute(f"CREATE INDEX idx1 ON {tbl} (b)")
        cql.execute(f"CREATE INDEX idx2 ON {tbl} (c)")
        cql.execute(f"CREATE MATERIALIZED VIEW {ks}.tbl_view AS SELECT c FROM {tbl} WHERE c IS NOT NULL PRIMARY KEY (c, a)")
        # dropping whole keyspace with MV and SI is fine too
        cql.execute(f"DROP KEYSPACE {ks}")
    finally:
        cql.execute(f"DROP KEYSPACE IF EXISTS {ks}")


# The Python driver refuses to send invalid UTF-8 in a text value. This
# fixture monkey-patches the driver's text serializer so that a string
# containing "surrogateescape"-wrapped bytes is sent as those raw bytes,
# allowing tests to bind invalid UTF-8 to text values (including inside
# collections and tuples). See also test_validation.py.
@pytest.fixture
def raw_utf8_serialization(monkeypatch):
    def serialize(ustr, protocol_version):
        return ustr.encode('utf-8', errors='surrogateescape')
    monkeypatch.setattr(cassandra.cqltypes.UTF8Type, 'serialize', staticmethod(serialize))

# A single byte 0xAD - a UTF-8 continuation byte, which is invalid as the
# first byte of a UTF-8 sequence - wrapped so it can be bound using the
# raw_utf8_serialization fixture.
bad_utf8_string = b'\xad'.decode('utf-8', errors='surrogateescape')

def test_list_elements_validation(cql, test_keyspace, raw_utf8_serialization):
    with new_test_table(cql, test_keyspace, "a int, b list<date>, PRIMARY KEY (a)") as tbl:
        with pytest.raises(InvalidRequest):
            cql.execute(f"INSERT INTO {tbl} (a, b) VALUES(1, ['definitely not a date value'])")
        cql.execute(f"INSERT INTO {tbl} (a, b) VALUES(1, ['2015-05-03'])")
    with new_test_table(cql, test_keyspace, "a int, b list<text>, PRIMARY KEY (a)") as tbl2:
        stmt = cql.prepare(f"INSERT INTO {tbl2} (a, b) VALUES(?, ?)")
        with pytest.raises(InvalidRequest, match='UTF8'):
            cql.execute(stmt, [1, [bad_utf8_string]])
        cql.execute(stmt, [1, ["proper utf8 string"]])


def test_set_elements_validation(cql, test_keyspace, raw_utf8_serialization):
    with new_test_table(cql, test_keyspace, "a int, b set<date>, PRIMARY KEY (a)") as tbl:
        with pytest.raises(InvalidRequest):
            cql.execute(f"INSERT INTO {tbl} (a, b) VALUES(1, {{'definitely not a date value'}})")
        cql.execute(f"INSERT INTO {tbl} (a, b) VALUES(1, {{'2015-05-03'}})")
    with new_test_table(cql, test_keyspace, "a int, b set<text>, PRIMARY KEY (a)") as tbl2:
        stmt = cql.prepare(f"INSERT INTO {tbl2} (a, b) VALUES(?, ?)")
        with pytest.raises(InvalidRequest, match='UTF8'):
            cql.execute(stmt, [1, {bad_utf8_string}])
        cql.execute(stmt, [1, {"proper utf8 string"}])


def test_map_elements_validation(cql, test_keyspace, raw_utf8_serialization):
    with new_test_table(cql, test_keyspace, "a int, b map<date, date>, PRIMARY KEY (a)") as tbl:
        def test_inline(value, should_throw):
            cql1 = f"INSERT INTO {tbl} (a, b) VALUES(1, {{'10-10-2010' : '{value}'}})"
            cql2 = f"INSERT INTO {tbl} (a, b) VALUES(1, {{'{value}' : '10-10-2010'}})"
            if should_throw:
                with pytest.raises(InvalidRequest):
                    cql.execute(cql1)
                with pytest.raises(InvalidRequest):
                    cql.execute(cql2)
            else:
                cql.execute(cql1)
                cql.execute(cql2)
        test_inline("definitely not a date value", True)
        test_inline("2015-05-03", False)
    with new_test_table(cql, test_keyspace, "a int, b map<text, text>, PRIMARY KEY (a)") as tbl2:
        stmt = cql.prepare(f"INSERT INTO {tbl2} (a, b) VALUES(?, ?)")
        def test_bind(value, should_throw):
            for m in [{value: "foo"}, {"foo": value}]:
                if should_throw:
                    with pytest.raises(InvalidRequest, match='UTF8'):
                        cql.execute(stmt, [1, m])
                else:
                    cql.execute(stmt, [1, m])
        test_bind(bad_utf8_string, True)
        test_bind("proper utf8 string", False)


def test_in_clause_validation(cql, test_keyspace, raw_utf8_serialization):
    with new_test_table(cql, test_keyspace, "p1 int, c1 int, r1 date, PRIMARY KEY (p1, c1, r1)") as tbl:
        with pytest.raises(InvalidRequest):
            cql.execute(f"SELECT r1 FROM {tbl} WHERE (c1,r1) IN ((1, 'definitely not a date value')) ALLOW FILTERING")
        cql.execute(f"SELECT r1 FROM {tbl} WHERE (c1,r1) IN ((1, '2015-05-03')) ALLOW FILTERING")
    with new_test_table(cql, test_keyspace, "p1 int, c1 int, r1 text, PRIMARY KEY (p1, c1, r1)") as tbl2:
        stmt = cql.prepare(f"SELECT r1 FROM {tbl2} WHERE (c1,r1) IN ? ALLOW FILTERING")
        with pytest.raises(InvalidRequest, match='UTF8'):
            cql.execute(stmt, [[(2, bad_utf8_string)]])
        cql.execute(stmt, [[(2, "proper utf8 string")]])


def test_in_clause_cartesian_product_limits(cql, test_keyspace):
    # These limits are the defaults of max_partition_key_restrictions_per_query
    # and max_clustering_key_restrictions_per_query (100). Scylla reports
    # exceeding them as a generic server error (the C++ test just expects
    # std::runtime_error), which the driver, after trying all hosts, reports
    # as NoHostAvailable.
    with new_test_table(cql, test_keyspace, "pk1 int, pk2 int, PRIMARY KEY ((pk1, pk2))") as tab1:
        # 100 partitions, should pass
        cql.execute(f"SELECT * FROM {tab1} WHERE pk1 IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)"
                    "                           AND pk2 IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)")
        # 110 partitions, should fail
        with pytest.raises(NoHostAvailable, match="is greater than maximum 100"):
            cql.execute(f"SELECT * FROM {tab1} WHERE pk1 IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)"
                        "                          AND pk2 IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)")

    with new_test_table(cql, test_keyspace, "pk1 int, ck1 int, ck2 int, PRIMARY KEY (pk1, ck1, ck2)") as tab2:
        # 100 clustering rows, should pass
        cql.execute(f"SELECT * FROM {tab2} WHERE pk1 = 1"
                    "                          AND ck1 IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)"
                    "                          AND ck2 IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)")
        # 110 clustering rows, should fail
        with pytest.raises(NoHostAvailable, match="is greater than maximum 100"):
            cql.execute(f"SELECT * FROM {tab2} WHERE pk1 = 1"
                        "                           AND ck1 IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)"
                        "                           AND ck2 IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)")

    def make_tuple(count):
        return "(" + ",".join(str(i) for i in range(count)) + ")"
    with new_test_table(cql, test_keyspace, "pk1 int, ck1 int, PRIMARY KEY (pk1, ck1)") as tab3:
        # tuple with 100 keys, should pass
        cql.execute(f"SELECT * FROM {tab3} WHERE pk1 IN {make_tuple(100)}")
        cql.execute(f"SELECT * FROM {tab3} WHERE pk1 = 1 AND ck1 IN {make_tuple(100)}")
        # tuple with 101 keys, should fail
        with pytest.raises(NoHostAvailable, match="is greater than maximum 100"):
            cql.execute(f"SELECT * FROM {tab3} WHERE pk1 IN {make_tuple(101)}")
        with pytest.raises(NoHostAvailable, match="is greater than maximum 100"):
            cql.execute(f"SELECT * FROM {tab3} WHERE pk1 = 3 AND ck1 IN {make_tuple(101)}")


def test_tuple_elements_validation(cql, test_keyspace, raw_utf8_serialization):
    with new_test_table(cql, test_keyspace, "a int, b tuple<int, date>, PRIMARY KEY (a)") as tbl:
        with pytest.raises(InvalidRequest):
            cql.execute(f"INSERT INTO {tbl} (a, b) VALUES(1, (1, 'definitely not a date value'))")
        cql.execute(f"INSERT INTO {tbl} (a, b) VALUES(1, (1, '2015-05-03'))")
    with new_test_table(cql, test_keyspace, "a int, b tuple<int, text>, PRIMARY KEY (a)") as tbl2:
        stmt = cql.prepare(f"INSERT INTO {tbl2} (a, b) VALUES(?, ?)")
        with pytest.raises(InvalidRequest, match='UTF8'):
            cql.execute(stmt, [1, (2, bad_utf8_string)])
        cql.execute(stmt, [1, (2, "proper utf8 string")])


def test_vector_elements_validation(cql, test_keyspace, raw_utf8_serialization):
    with new_test_table(cql, test_keyspace, "a int, b vector<date, 1>, PRIMARY KEY (a)") as tbl:
        with pytest.raises(InvalidRequest):
            cql.execute(f"INSERT INTO {tbl} (a, b) VALUES(1, ['definitely not a date value'])")
        cql.execute(f"INSERT INTO {tbl} (a, b) VALUES(1, ['2015-05-03'])")
    with new_test_table(cql, test_keyspace, "a int, b vector<text, 1>, PRIMARY KEY (a)") as tbl2:
        stmt = cql.prepare(f"INSERT INTO {tbl2} (a, b) VALUES(?, ?)")
        with pytest.raises(InvalidRequest, match='UTF8'):
            cql.execute(stmt, [1, [bad_utf8_string]])
        cql.execute(stmt, [1, ["proper utf8 string"]])


# Reproduces #4209
def test_list_of_tuples_with_bound_var(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, c1 list<frozen<tuple<int,int>>>") as cf:
        cql.prepare(f"update {cf} SET c1 = c1 + [(?,9999)] where pk = 999")


def test_bound_var_in_collection_literal(cql, test_keyspace, monkeypatch):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, c1 list<int>") as list_t, \
         new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, c1 set<int>") as set_t, \
         new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, c1 map<int, int>") as map_t:
        insert_list = cql.prepare(f"insert into {list_t} (pk, c1) values (112, [997, ?])")
        insert_set = cql.prepare(f"insert into {set_t} (pk, c1) values (112, {{997, ?}})")
        insert_map_key = cql.prepare(f"insert into {map_t} (pk, c1) values (112, {{997: 112, ?: 112}})")
        insert_map_value = cql.prepare(f"insert into {map_t} (pk, c1) values (112, {{997: 112, 112: ?}})")
        with pytest.raises(SyntaxException):
            cql.prepare(f"insert into {map_t} (pk, c1) values (112, {{997: 112, ?}})")

        for stmt in [insert_list, insert_set, insert_map_key, insert_map_value]:
            # Null value is not allowed as a collections element
            with pytest.raises(InvalidRequest):
                cql.execute(stmt, [None])

            # Check if types mismatch is detected: send a 2-byte smallint
            # where a 4-byte int is expected. The driver serializes according
            # to the prepared metadata, so we override the int serializer.
            with monkeypatch.context() as m:
                m.setattr(cassandra.cqltypes.Int32Type, 'serialize',
                          staticmethod(lambda val, protocol_version: struct.pack('>h', val)))
                with pytest.raises(InvalidRequest):
                    cql.execute(stmt, [1])

            with pytest.raises(InvalidRequest):
                cql.execute(stmt, [UNSET_VALUE])

            # Inserting a valid value has to be successful
            cql.execute(stmt, [2])


# The number of distinct values in a list is limited. Test the limit.
def test_list_append_limit(cql, test_keyspace, scylla_only):
    # utils::UUID_gen::SUBMICRO_LIMIT
    SUBMICRO_LIMIT = 1 << 17
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, l list<int>") as t:
        value_list = ",".join(["0"] * (SUBMICRO_LIMIT + 1))
        with pytest.raises(InvalidRequest):
            cql.execute(f"UPDATE {t} SET l = l + [{value_list}] WHERE pk = 0")


def test_insert_statement(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p1 varchar, c1 int, r1 int, PRIMARY KEY (p1, c1)") as cf:
        cql.execute(f"insert into {cf} (p1, c1, r1) values ('key1', 1, 100)")
        assert list(cql.execute(f"select r1 from {cf} where p1 = 'key1' and c1 = 1")) == [(100,)]
        cql.execute(f"update {cf} set r1 = 66 where p1 = 'key1' and c1 = 1")
        assert list(cql.execute(f"select r1 from {cf} where p1 = 'key1' and c1 = 1")) == [(66,)]


def test_select_statement(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p1 varchar, c1 int, c2 int, r1 int, PRIMARY KEY (p1, c1, c2)") as cf:
        cql.execute(f"insert into {cf} (p1, c1, c2, r1) values ('key1', 1, 2, 3)")
        cql.execute(f"insert into {cf} (p1, c1, c2, r1) values ('key2', 1, 2, 13)")
        cql.execute(f"insert into {cf} (p1, c1, c2, r1) values ('key3', 1, 2, 23)")
        # Test wildcard
        assert list(cql.execute(f"select * from {cf} where p1 = 'key1' and c2 = 2 and c1 = 1")) == [('key1', 1, 2, 3)]
        # Test with only regular column
        assert list(cql.execute(f"select r1 from {cf} where p1 = 'key1' and c2 = 2 and c1 = 1")) == [(3,)]
        # Test full partition range, singular clustering range
        assert sorted(cql.execute(f"select * from {cf} where c1 = 1 and c2 = 2 allow filtering")) == [
            ('key1', 1, 2, 3), ('key2', 1, 2, 13), ('key3', 1, 2, 23)]


def test_cassandra_stress_like_write_and_read(cql, test_keyspace):
    values = [
        "8f75da6b3dcec90c8a404fb9a5f6b0621e62d39c69ba5758e5f41b78311fbb26cc7a",
        "a8761a2127160003033a8f4f3d1069b7833ebe24ef56b3beee728c2b686ca516fa51",
        "583449ce81bfebc2e1a695eb59aad5fcc74d6d7311fc6197b10693e1a161ca2e1c64",
        "62bcb1dbc0ff953abc703bcb63ea954f437064c0c45366799658bd6b91d0f92908d7",
        "222fcbe31ffa1e689540e1499b87fa3f9c781065fccd10e4772b4c7039c2efd0fb27",
    ]
    with new_test_table(cql, test_keyspace, '"KEY" blob PRIMARY KEY, "C0" blob, "C1" blob, "C2" blob, "C3" blob, "C4" blob') as cf:
        keys = [f"0xdeadbeefcafebabe{suffix:02d}" for suffix in range(10)]
        # The C++ test issues the writes (and then the reads) in parallel
        futures = [cql.execute_async(
            f'UPDATE {cf} SET "C0" = 0x{values[0]}, "C1" = 0x{values[1]}, "C2" = 0x{values[2]}, '
            f'"C3" = 0x{values[3]}, "C4" = 0x{values[4]} WHERE "KEY"={key}') for key in keys]
        for f in futures:
            f.result()
        futures = [cql.execute_async(f'select "C0", "C1", "C2", "C3", "C4" from {cf} where "KEY" = {key}') for key in keys]
        for f in futures:
            assert list(f.result()) == [tuple(bytes.fromhex(v) for v in values)]


def test_range_queries(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "k blob, c0 blob, c1 blob, v blob, PRIMARY KEY (k, c0, c1)") as cf:
        for v, c0, c1 in [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 2, 2), (5, 2, 3), (6, 2, 4), (7, 3, 4), (8, 3, 5)]:
            cql.execute(f"update {cf} set v = 0x{v:02x} where k = 0x00 and c0 = 0x{c0:02x} and c1 = 0x{c1:02x}")
        def check(where, expected):
            assert list(cql.execute(f"select v from {cf} where k = 0x00{where}")) == [(bytes([v]),) for v in expected]
        check("", [1, 2, 3, 4, 5, 6, 7, 8])
        check(" and c0 = 0x02 allow filtering", [4, 5, 6])
        check(" and c0 > 0x02 allow filtering", [7, 8])
        check(" and c0 >= 0x02 allow filtering", [4, 5, 6, 7, 8])
        check(" and c0 >= 0x02 and c0 < 0x03 allow filtering", [4, 5, 6])
        check(" and c0 > 0x02 and c0 <= 0x03 allow filtering", [7, 8])
        check(" and c0 >= 0x02 and c0 <= 0x02 allow filtering", [4, 5, 6])
        check(" and c0 < 0x02 allow filtering", [1, 2, 3])
        check(" and c0 = 0x02 and c1 > 0x02 allow filtering", [5, 6])
        check(" and c0 = 0x02 and c1 >= 0x02 and c1 <= 0x02 allow filtering", [4])


def test_ordering_of_composites_with_variable_length_components(cql, test_keyspace):
    # We need more than one clustering column so that the single-element tuple format optimisation doesn't kick in
    with new_test_table(cql, test_keyspace, "k blob, c0 blob, c1 blob, v blob, PRIMARY KEY (k, c0, c1)") as cf:
        cql.execute(f"update {cf} set v = 0x01 where k = 0x00 and c0 = 0x0001 and c1 = 0x00")
        cql.execute(f"update {cf} set v = 0x02 where k = 0x00 and c0 = 0x03 and c1 = 0x00")
        cql.execute(f"update {cf} set v = 0x03 where k = 0x00 and c0 = 0x035555 and c1 = 0x00")
        cql.execute(f"update {cf} set v = 0x04 where k = 0x00 and c0 = 0x05 and c1 = 0x00")
        assert list(cql.execute(f"select v from {cf} where k = 0x00 allow filtering")) == [
            (b'\x01',), (b'\x02',), (b'\x03',), (b'\x04',)]


def test_query_with_static_columns(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "k blob, c blob, v blob, s1 blob static, s2 blob static, primary key (k, c)") as cf:
        cql.execute(f"update {cf} set s1 = 0x01 where k = 0x00")
        cql.execute(f"update {cf} set v = 0x02 where k = 0x00 and c = 0x01")
        cql.execute(f"update {cf} set v = 0x03 where k = 0x00 and c = 0x02")
        assert list(cql.execute(f"select s1, v from {cf}")) == [(b'\x01', b'\x02'), (b'\x01', b'\x03')]
        assert list(cql.execute(f"select s1 from {cf}")) == [(b'\x01',), (b'\x01',)]
        assert list(cql.execute(f"select s1 from {cf} limit 1")) == [(b'\x01',)]
        assert list(cql.execute(f"select s1, v from {cf} limit 1")) == [(b'\x01', b'\x02')]
        cql.execute(f"update {cf} set v = null where k = 0x00 and c = 0x02")
        assert list(cql.execute(f"select s1 from {cf}")) == [(b'\x01',)]
        cql.execute(f"insert into {cf} (k, c) values (0x00, 0x02)")
        assert list(cql.execute(f"select s1 from {cf}")) == [(b'\x01',), (b'\x01',)]
        # Try 'in' restriction out
        assert list(cql.execute(f"select s1, v from {cf} where k = 0x00 and c in (0x01, 0x02)")) == [
            (b'\x01', b'\x02'), (b'\x01', None)]
        # Verify that limit is respected for multiple clustering ranges and that static columns
        # are populated when limit kicks in.
        assert list(cql.execute(f"select s1, v from {cf} where k = 0x00 and c in (0x01, 0x02) limit 1")) == [
            (b'\x01', b'\x02')]


def test_insert_without_clustering_key(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "k blob, v blob, primary key (k)") as cf:
        cql.execute(f"insert into {cf} (k) values (0x01)")
        assert list(cql.execute(f"select * from {cf}")) == [(b'\x01', None)]
        assert list(cql.execute(f"select k from {cf}")) == [(b'\x01',)]


def test_limit_is_respected_across_partitions(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "k blob, c blob, v blob, s1 blob static, primary key (k, c)") as cf:
        cql.execute(f"update {cf} set s1 = 0x01 where k = 0x01")
        cql.execute(f"update {cf} set s1 = 0x02 where k = 0x02")
        # Determine partition order
        keys = [row.k for row in cql.execute(f"select k from {cf}")]
        assert len(keys) == 2
        k1, k2 = keys
        # Note that s1 happens to have the same value as k in every partition
        assert list(cql.execute(f"select s1 from {cf} limit 1")) == [(k1,)]
        assert list(cql.execute(f"select s1 from {cf} limit 2")) == [(k1,), (k2,)]
        cql.execute(f"update {cf} set s1 = null where k = 0x{k1.hex()}")
        assert list(cql.execute(f"select s1 from {cf} limit 1")) == [(k2,)]
        cql.execute(f"update {cf} set s1 = null where k = 0x{k2.hex()}")
        assert list(cql.execute(f"select s1 from {cf} limit 1")) == []


def test_partitions_have_consistent_ordering_in_range_query(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "k blob, v int, primary key (k)") as cf:
        cql.execute("begin unlogged batch \n" +
                    "".join(f"  insert into {cf} (k, v) values (0x0{i}, 0); \n" for i in range(1, 7)) +
                    "apply batch;")
        # Determine partition order
        keys = [row.k for row in cql.execute(f"select k from {cf}")]
        assert len(keys) == 6
        for limit in range(1, 7):
            assert list(cql.execute(f"select k from {cf} limit {limit}")) == [(k,) for k in keys[:limit]]


def test_partition_range_queries_with_bounds(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "k blob, v int, primary key (k)") as cf:
        cql.execute("begin unlogged batch \n" +
                    "".join(f"  insert into {cf} (k, v) values (0x0{i}, 0); \n" for i in range(1, 6)) +
                    "apply batch;")
        # Determine partition order
        rows = list(cql.execute(f"select k, token(k) from {cf}"))
        keys = [r[0] for r in rows]
        tokens = [r[1] for r in rows]
        assert len(keys) == 5
        def check(where, expected):
            assert list(cql.execute(f"select k from {cf} where {where}")) == [(k,) for k in expected]
        check(f"token(k) > {tokens[1]}", keys[2:5])
        check(f"token(k) >= {tokens[1]}", keys[1:5])
        check(f"token(k) > {tokens[1]} and token(k) < {tokens[4]}", keys[2:4])
        check(f"token(k) < {tokens[3]}", keys[0:3])
        check(f"token(k) = {tokens[3]}", [keys[3]])
        check(f"token(k) < {tokens[3]} and token(k) > {tokens[3]}", [])
        check(f"token(k) >= {tokens[4]} and token(k) <= {tokens[2]}", [])
        min_token = -2**63
        check(f"token(k) > {min_token} and token (k) < {min_token}", keys)


def test_deletion_scenarios(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "k blob, c blob, v blob, primary key (k, c)") as cf:
        def select_v():
            return list(cql.execute(f"select v from {cf}"))
        cql.execute(f"insert into {cf} (k, c, v) values (0x00, 0x05, 0x01) using timestamp 1")
        assert select_v() == [(b'\x01',)]
        cql.execute(f"update {cf} using timestamp 2 set v = null where k = 0x00 and c = 0x05")
        assert select_v() == [(None,)]
        # same tampstamp, dead cell wins
        cql.execute(f"update {cf} using timestamp 2 set v = 0x02 where k = 0x00 and c = 0x05")
        assert select_v() == [(None,)]
        cql.execute(f"update {cf} using timestamp 3 set v = 0x02 where k = 0x00 and c = 0x05")
        assert select_v() == [(b'\x02',)]
        # same timestamp, greater value wins
        cql.execute(f"update {cf} using timestamp 3 set v = 0x03 where k = 0x00 and c = 0x05")
        assert select_v() == [(b'\x03',)]
        # same tampstamp, delete whole row, delete should win
        cql.execute(f"delete from {cf} using timestamp 3 where k = 0x00 and c = 0x05")
        assert select_v() == []
        # same timestamp, update should be shadowed by range tombstone
        cql.execute(f"update {cf} using timestamp 3 set v = 0x04 where k = 0x00 and c = 0x05")
        assert select_v() == []
        cql.execute(f"update {cf} using timestamp 4 set v = 0x04 where k = 0x00 and c = 0x05")
        assert select_v() == [(b'\x04',)]
        # deleting an orphan cell (row is considered as deleted) yields no row
        cql.execute(f"update {cf} using timestamp 5 set v = null where k = 0x00 and c = 0x05")
        assert select_v() == []


def test_range_deletion_scenarios(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, v text, primary key (p, c)") as cf:
        for i in range(10):
            cql.execute(f"insert into {cf} (p, c, v) values (1, {i}, 'abc')")

        cql.execute(f"delete from {cf} where p = 1 and c <= 3")
        cql.execute(f"delete from {cf} where p = 1 and c >= 8")

        cql.execute(f"delete from {cf} where p = 1 and c >= 0 and c <= 5")
        assert len(list(cql.execute(f"select * from {cf}"))) == 2
        cql.execute(f"delete from {cf} where p = 1 and c > 3 and c < 10")
        assert len(list(cql.execute(f"select * from {cf}"))) == 0

        cql.execute(f"insert into {cf} (p, c, v) values (1, 1, '1')")
        cql.execute(f"insert into {cf} (p, c, v) values (1, 3, '3')")
        cql.execute(f"delete from {cf} where p = 1 and c >= 2 and c <= 3")
        cql.execute(f"insert into {cf} (p, c, v) values (1, 2, '2')")
        assert len(list(cql.execute(f"select * from {cf}"))) == 2
        cql.execute(f"delete from {cf} where p = 1 and c >= 2 and c <= 3")
        assert list(cql.execute(f"select * from {cf}")) == [(1, 1, '1')]


def test_range_deletion_scenarios_with_compact_storage(cql, test_keyspace, compact_storage):
    with new_test_table(cql, test_keyspace, "p int, c int, v text, primary key (p, c)", "with compact storage") as table:
        for i in range(10):
            cql.execute(f"insert into {table} (p, c, v) values (1, {i}, 'abc')")
        # Range deletions are not allowed on compact storage tables
        for where in ["c <= 3", "c >= 0", "c > 0 and c <= 3", "c >= 0 and c < 3", "c > 0 and c < 3", "c >= 0 and c <= 3"]:
            with pytest.raises(InvalidRequest):
                cql.execute(f"delete from {table} where p = 1 and {where}")


def test_map_insert_update(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p1 varchar primary key, map1 map<int, int>") as table:
        def check(expected):
            assert list(cql.execute(f"select map1 from {table} where p1 = 'key1'")) == [(expected,)]
        cql.execute(f"insert into {table} (p1, map1) values ('key1', {{ 1001: 2001 }})")
        check({1001: 2001})
        cql.execute(f"update {table} set map1[1002] = 2002 where p1 = 'key1'")
        check({1001: 2001, 1002: 2002})
        # overwrite an element
        cql.execute(f"update {table} set map1[1001] = 3001 where p1 = 'key1'")
        check({1001: 3001, 1002: 2002})
        # overwrite whole map
        cql.execute(f"update {table} set map1 = {{1003: 4003}} where p1 = 'key1'")
        check({1003: 4003})
        # overwrite whole map, but bad syntax
        with pytest.raises(InvalidRequest):
            cql.execute(f"update {table} set map1 = {{1003, 4003}} where p1 = 'key1'")
        # overwrite whole map
        cql.execute(f"update {table} set map1 = {{1001: 5001, 1002: 5002, 1003: 5003}} where p1 = 'key1'")
        check({1001: 5001, 1002: 5002, 1003: 5003})
        # discard some keys
        cql.execute(f"update {table} set map1 = map1 - {{1001, 1003, 1005}} where p1 = 'key1'")
        check({1002: 5002})
        assert list(cql.execute(f"select * from {table} where p1 = 'key1'")) == [('key1', {1002: 5002})]
        # overwrite an element
        cql.execute(f"update {table} set map1[1009] = 5009 where p1 = 'key1'")
        # delete a key
        cql.execute(f"delete map1[1002] from {table} where p1 = 'key1'")
        check({1009: 5009})
        cql.execute(f"insert into {table} (p1, map1) values ('key1', null)")
        # An empty non-frozen map is returned as null
        check(None)


def test_set_insert_update(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p1 varchar primary key, set1 set<int>") as table:
        def check(expected):
            assert list(cql.execute(f"select set1 from {table} where p1 = 'key1'")) == [(expected,)]
        cql.execute(f"insert into {table} (p1, set1) values ('key1', {{ 1001 }})")
        check({1001})
        cql.execute(f"update {table} set set1 = set1 + {{ 1002 }} where p1 = 'key1'")
        check({1001, 1002})
        # overwrite an element
        cql.execute(f"update {table} set set1 = set1 + {{ 1001 }} where p1 = 'key1'")
        check({1001, 1002})
        # overwrite entire set
        cql.execute(f"update {table} set set1 = {{ 1007, 1019 }} where p1 = 'key1'")
        check({1007, 1019})
        # discard keys
        cql.execute(f"update {table} set set1 = set1 - {{ 1007, 1008 }} where p1 = 'key1'")
        check({1019})
        assert list(cql.execute(f"select * from {table} where p1 = 'key1'")) == [('key1', {1019})]
        cql.execute(f"update {table} set set1 = set1 + {{ 1009 }} where p1 = 'key1'")
        cql.execute(f"delete set1[1019] from {table} where p1 = 'key1'")
        check({1009})
        cql.execute(f"insert into {table} (p1, set1) values ('key1', null)")
        check(None)
        cql.execute(f"insert into {table} (p1, set1) values ('key1', {{}})")
        # Empty non-frozen set is indistinguishable from NULL
        check(None)


def test_list_insert_update(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p1 varchar primary key, list1 list<int>") as table:
        def check(expected):
            assert list(cql.execute(f"select list1 from {table} where p1 = 'key1'")) == [(expected,)]
        cql.execute(f"insert into {table} (p1, list1) values ('key1', [ 1001 ])")
        check([1001])
        cql.execute(f"update {table} set list1 = [ 1002, 1003 ] where p1 = 'key1'")
        check([1002, 1003])
        cql.execute(f"update {table} set list1[1] = 2003 where p1 = 'key1'")
        check([1002, 2003])
        cql.execute(f"update {table} set list1 = list1 - [1002, 2004] where p1 = 'key1'")
        check([2003])
        assert list(cql.execute(f"select * from {table} where p1 = 'key1'")) == [('key1', [2003])]
        cql.execute(f"update {table} set list1 = [2008, 2009, 2010] where p1 = 'key1'")
        cql.execute(f"delete list1[1] from {table} where p1 = 'key1'")
        check([2008, 2010])
        cql.execute(f"update {table} set list1 = list1 + [2012, 2019] where p1 = 'key1'")
        check([2008, 2010, 2012, 2019])
        cql.execute(f"update {table} set list1 = [2001, 2002] + list1 where p1 = 'key1'")
        check([2001, 2002, 2008, 2010, 2012, 2019])
        cql.execute(f"insert into {table} (p1, list1) values ('key1', null)")
        check(None)
        cql.execute(f"insert into {table} (p1, list1) values ('key1', [])")
        # Empty non-frozen list is indistinguishable from NULL
        check(None)


def test_writetime_and_ttl(cql, test_keyspace):
    the_timestamp = 123456789
    with new_test_table(cql, test_keyspace, "p1 varchar primary key, i int, fc frozen<set<int>>, c set<int>") as table:
        cql.execute(f"insert into {table} (p1, i) values ('key1', 1) using timestamp {the_timestamp}")
        assert list(cql.execute(f"select writetime(i) from {table} where p1 in ('key1')")) == [(the_timestamp,)]
        ts1 = the_timestamp + 1
        cql.execute(f"UPDATE {table} USING TIMESTAMP {ts1} SET fc = {{1}}, c = {{2}} WHERE p1 = 'key1'")
        assert list(cql.execute(f"SELECT writetime(fc) FROM {table}")) == [(ts1,)]
        # writetime() of a non-frozen collection is not allowed
        with pytest.raises(InvalidRequest):
            cql.execute(f"SELECT writetime(c) FROM {table}")


# max_ttl in Scylla is 20 years (gc_clock.hh)
max_ttl = 20 * 365 * 24 * 60 * 60


def test_time_overflow_with_default_ttl(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "p1 varchar primary key, i int", f"with default_time_to_live = {max_ttl}") as table:
        def verify(value, bypass_cache):
            bypass = "bypass cache" if bypass_cache else ""
            assert list(cql.execute(f"select i from {table} where p1 = 'key1' {bypass}")) == [(value,)]
        cql.execute(f"insert into {table} (p1, i) values ('key1', 1)")
        verify(1, False)
        verify(1, True)
        cql.execute(f"update {table} set i = 2 where p1 = 'key1'")
        verify(2, True)
        verify(2, False)


def test_time_overflow_using_ttl(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "p1 varchar primary key, i int") as table:
        def verify(key, value, bypass_cache):
            bypass = "bypass cache" if bypass_cache else ""
            assert list(cql.execute(f"select i from {table} where p1 = '{key}' {bypass}")) == [(value,)]
        cql.execute(f"insert into {table} (p1, i) values ('key1', 1) using ttl {max_ttl}")
        verify('key1', 1, False)
        verify('key1', 1, True)
        cql.execute(f"insert into {table} (p1, i) values ('key2', 0)")
        cql.execute(f"update {table} using ttl {max_ttl} set i = 2 where p1 = 'key2'")
        verify('key2', 2, False)
        verify('key1', 1, False)
        verify('key1', 1, True)


def test_batch(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p1 varchar, c1 int, r1 int, PRIMARY KEY (p1, c1)") as table:
        cql.execute(f"""begin unlogged batch
              insert into {table} (p1, c1, r1) values ('key1', 1, 100);
              insert into {table} (p1, c1, r1) values ('key1', 2, 200);
            apply batch;""")
        assert list(cql.execute(f"select r1 from {table} where p1 = 'key1' and c1 = 1")) == [(100,)]
        assert list(cql.execute(f"select r1 from {table} where p1 = 'key1' and c1 = 2")) == [(200,)]


def test_tuples(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "id int primary key, t tuple<int, bigint, text>") as table:
        cql.execute(f"insert into {table} (id, t) values (1, (1001, 2001, 'abc1'))")
        assert list(cql.execute(f"select t from {table} where id = 1")) == [((1001, 2001, 'abc1'),)]
    with new_test_table(cql, test_keyspace, "p1 int PRIMARY KEY, r1 tuple<int, bigint, text>") as table:
        cql.execute(f"insert into {table} (p1, r1) values (1, (1, 2, 'abc'))")
        assert list(cql.execute(f"select * from {table} where p1 = 1")) == [(1, (1, 2, 'abc'))]


def test_vectors(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "id int primary key, v vector<int, 3>") as table:
        cql.execute(f"insert into {table} (id, v) values (1, [1001, 2001, 3001])")
        assert list(cql.execute(f"select v from {table} where id = 1")) == [([1001, 2001, 3001],)]
    with new_test_table(cql, test_keyspace, "p1 int PRIMARY KEY, r1 vector<int, 3>") as table:
        cql.execute(f"insert into {table} (p1, r1) values (1, [1, 2, 3])")
        assert list(cql.execute(f"select * from {table} where p1 = 1")) == [(1, [1, 2, 3])]


def test_vectors_variable_length_elements(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "id int PRIMARY KEY, v vector<text, 2>") as table:
        cql.execute(f"INSERT INTO {table} (id, v) VALUES (1, ['abc', ''])")
        assert list(cql.execute(f"SELECT * FROM {table}")) == [(1, ['abc', ''])]
    with new_test_table(cql, test_keyspace, "id int PRIMARY KEY, v vector<list<int>, 2>") as table:
        cql.execute(f"INSERT INTO {table} (id, v) VALUES (1, [[1, 2], [3, 4, 5]])")
        assert list(cql.execute(f"SELECT * FROM {table}")) == [(1, [[1, 2], [3, 4, 5]])]
    with new_test_table(cql, test_keyspace, "id int PRIMARY KEY, v vector<tuple<int, text>, 2>") as table:
        cql.execute(f"INSERT INTO {table} (id, v) VALUES (1, [(123, 'abc'), (456, '')])")
        assert list(cql.execute(f"SELECT * FROM {table}")) == [(1, [(123, 'abc'), (456, '')])]


# Since durations don't have a well-defined ordering on their semantic value,
# a number of restrictions exist on their use.
def test_duration_restrictions(cql, test_keyspace):
    def validate_request_failure(request, expected_message):
        with pytest.raises(InvalidRequest, match=re.escape(expected_message)):
            cql.execute(request)

    # Disallow "direct" use of durations in ordered collection types to avoid
    # user confusion when their ordering doesn't match expectations.
    my_type = f"{test_keyspace}.{unique_name()}"
    validate_request_failure(f"create type {my_type} (a set<duration>)",
        "Durations are not allowed inside sets: set<duration>")
    validate_request_failure(f"create type {my_type} (a map<duration, int>)",
        "Durations are not allowed as map keys: map<duration, int>")

    # Disallow any type referring to a duration from being used in a primary
    # key of a table or a materialized view.
    my_table = f"{test_keyspace}.{unique_name()}"
    validate_request_failure(f"create table {my_table} (direct_key duration PRIMARY KEY)",
        "duration type is not supported for PRIMARY KEY part direct_key")
    validate_request_failure(f"create table {my_table} (collection_key frozen<list<duration>> PRIMARY KEY)",
        "duration type is not supported for PRIMARY KEY part collection_key")
    with new_type(cql, test_keyspace, "(span duration)") as my_type0:
        validate_request_failure(f"create table {my_table} (udt_key frozen<{my_type0}> PRIMARY KEY)",
            "duration type is not supported for PRIMARY KEY part udt_key")
    validate_request_failure(f"create table {my_table} (tuple_key tuple<int, duration, int> PRIMARY KEY)",
        "duration type is not supported for PRIMARY KEY part tuple_key")
    validate_request_failure(f"create table {my_table} (a int, b duration, PRIMARY KEY ((a), b)) WITH CLUSTERING ORDER BY (b DESC)",
        "duration type is not supported for PRIMARY KEY part b")
    with new_test_table(cql, test_keyspace, "key int PRIMARY KEY, name text, span duration") as my_table0:
        my_mv = f"{test_keyspace}.{unique_name()}"
        validate_request_failure(f"create materialized view {my_mv} as select * from {my_table0} primary key (key, span)",
            "Cannot use Duration column 'span' in PRIMARY KEY of materialized view")

        # Disallow creating secondary indexes on durations.
        validate_request_failure(f"create index {unique_name()} on {my_table0} (span)",
            "Secondary indexes are not supported on duration columns")

        # Disallow slice-based restrictions and conditions on durations.
        #
        # Note that multi-column restrictions are only supported on clustering
        # columns (which cannot be `duration`) and that multi-column conditions
        # are not supported in the grammar.
        validate_request_failure(f"select * from {my_table0} where key = 0 and span < 3d",
            "Duration type is unordered for span")
        validate_request_failure(f"update {my_table0} set name = 'joe' where key = 0 if span >= 5m",
            "Duration type is unordered for span")


def test_select_multiple_ranges(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p1 varchar, r1 int, PRIMARY KEY (p1)") as table:
        cql.execute(f"""begin unlogged batch
              insert into {table} (p1, r1) values ('key1', 100);
              insert into {table} (p1, r1) values ('key2', 200);
            apply batch;""")
        assert sorted(cql.execute(f"select r1 from {table} where p1 in ('key1', 'key2')")) == [(100,), (200,)]


def test_validate_keyspace(cql):
    # Keyspace name too long (schema::NAME_LENGTH is 192)
    keyspace_name = 'k' * 193
    with pytest.raises(InvalidRequest):
        cql.execute(f"create keyspace {keyspace_name} with replication = {{ 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }}")
    with pytest.raises(SyntaxException):
        cql.execute("create keyspace ks3-1 with replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }")
    # A replication strategy class is not mandatory
    with new_test_keyspace(cql, "with replication = { 'replication_factor' : 1 }") as ks3:
        with pytest.raises(SyntaxException):
            cql.execute(f"create keyspace {ks3} with rreplication = {{ 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }}")
    with pytest.raises(InvalidRequest, match="not user-modifiable"):
        cql.execute("create keyspace SyStEm with replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }")


def test_validate_table(cql, test_keyspace):
    # Table name too long (schema::NAME_LENGTH is 192)
    table_name = 't' * 193
    with pytest.raises(InvalidRequest):
        cql.execute(f"create table {test_keyspace}.{table_name} (foo text PRIMARY KEY, bar text)")
    tb = f"{test_keyspace}.{unique_name()}"
    with pytest.raises(InvalidRequest):
        cql.execute(f"create table {tb} (foo text PRIMARY KEY, foo text)")
    with pytest.raises(SyntaxException):
        cql.execute(f"create table {test_keyspace}.tb-1 (foo text PRIMARY KEY, bar text)")
    with pytest.raises(InvalidRequest):
        cql.execute(f"create table {tb} (foo text, bar text)")
    with pytest.raises(InvalidRequest):
        cql.execute(f"create table {tb} (foo text PRIMARY KEY, bar text PRIMARY KEY)")
    with pytest.raises(SyntaxException):
        cql.execute(f"create table {tb} (foo text PRIMARY KEY, bar text) with commment = 'aaaa'")
    with pytest.raises(ConfigurationException):
        cql.execute(f"create table {tb} (foo text PRIMARY KEY, bar text) with min_index_interval = -1")
    with pytest.raises(ConfigurationException):
        cql.execute(f"create table {tb} (foo text PRIMARY KEY, bar text) with min_index_interval = 1024 and max_index_interval = 128")


def get_sstable_compression(cql, table):
    ks, cf = table.split('.')
    return cql.execute(f"SELECT compression FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = '{cf}'").one().compression


def test_table_compression(cql, test_keyspace):
    # Compression disabled: the compression options map doesn't have a
    # sstable_compression class.
    with new_test_table(cql, test_keyspace, "foo text PRIMARY KEY, bar text", "with compression = { }") as tb1:
        assert 'sstable_compression' not in get_sstable_compression(cql, tb1)
    with new_test_table(cql, test_keyspace, "foo text PRIMARY KEY, bar text", "with compression = { 'sstable_compression' : '' }") as tb5:
        assert 'sstable_compression' not in get_sstable_compression(cql, tb5)

    tb2 = f"{test_keyspace}.{unique_name()}"
    # An unknown compressor class is rejected. Scylla currently reports it
    # as a generic server error (std::runtime_error thrown from
    # sstables/compressor.cc), which the driver surfaces as NoHostAvailable,
    # so we only check that the request fails with the expected message.
    with pytest.raises(Exception, match="Unknown sstable_compression: LossyCompressor"):
        cql.execute(f"create table {tb2} (foo text PRIMARY KEY, bar text) with compression = {{ 'sstable_compression' : 'LossyCompressor' }}")
    with pytest.raises(ConfigurationException):
        cql.execute(f"create table {tb2} (foo text PRIMARY KEY, bar text) with compression = {{ 'sstable_compression' : 'LZ4Compressor', 'chunk_length_kb' : -1 }}")
    with pytest.raises(ConfigurationException):
        cql.execute(f"create table {tb2} (foo text PRIMARY KEY, bar text) with compression = {{ 'sstable_compression' : 'LZ4Compressor', 'chunk_length_kb' : 3 }}")

    with new_test_table(cql, test_keyspace, "foo text PRIMARY KEY, bar text",
            "with compression = { 'sstable_compression' : 'LZ4Compressor', 'chunk_length_kb' : 2 }") as tb2:
        compression = get_sstable_compression(cql, tb2)
        assert compression['sstable_compression'] == 'org.apache.cassandra.io.compress.LZ4Compressor'
        assert compression['chunk_length_in_kb'] == '2'
    with new_test_table(cql, test_keyspace, "foo text PRIMARY KEY, bar text",
            "with compression = { 'sstable_compression' : 'DeflateCompressor' }") as tb3:
        assert get_sstable_compression(cql, tb3)['sstable_compression'] == 'org.apache.cassandra.io.compress.DeflateCompressor'
    with new_test_table(cql, test_keyspace, "foo text PRIMARY KEY, bar text",
            "with compression = { 'sstable_compression' : 'org.apache.cassandra.io.compress.DeflateCompressor' }") as tb4:
        assert get_sstable_compression(cql, tb4)['sstable_compression'] == 'org.apache.cassandra.io.compress.DeflateCompressor'
    # Default compression comes from the sstable_compression_user_table_options
    # config. (The C++ test also accounted for the sstable_compression_dicts
    # cluster feature, which may downgrade a dictionary compressor to its
    # non-dictionary variant; it is enabled in a normal cluster.)
    with new_test_table(cql, test_keyspace, "foo text PRIMARY KEY, bar text") as tb6:
        default = json.loads(cql.execute("SELECT value FROM system.config WHERE name = 'sstable_compression_user_table_options'").one().value)
        strip_prefix = lambda name: name.removeprefix('org.apache.cassandra.io.compress.')
        assert strip_prefix(get_sstable_compression(cql, tb6)['sstable_compression']) == strip_prefix(default['sstable_compression'])


def test_types(cql, test_keyspace):
    with new_test_table(cql, test_keyspace,
            "a ascii PRIMARY KEY, b bigint, c blob, d boolean, e double, f float, g inet, h int, i text, "
            "j timestamp, k timeuuid, l uuid, m varchar, n varint, o decimal, p tinyint, q smallint, "
            "r date, s time, u duration") as table:
        cql.execute(f"""INSERT INTO {table} (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, u) VALUES (
            'ascii',
            123456789,
            0xdeadbeef,
            true,
            3.14,
            3.14,
            '127.0.0.1',
            3,
            'zażółć gęślą jaźń',
            '2001-10-18 14:15:55.134+0000',
            d2177dd0-eaa2-11de-a572-001b779c76e3,
            d2177dd0-eaa2-11de-a572-001b779c76e3,
            'varchar',
            123,
            1.23,
            3,
            3,
            '1970-01-02',
            '00:00:00.000000001',
            1y2mo3w4d5h6m7s8ms9us10ns
            )""")

        float_3_14 = struct.unpack('f', struct.pack('f', 3.14))[0]
        ts = datetime(2001, 10, 18, 14, 15, 55, 134000)
        uuid = UUID('d2177dd0-eaa2-11de-a572-001b779c76e3')
        ns = 1
        us = 1000 * ns
        ms = 1000 * us
        s = 1000 * ms
        m = 60 * s
        h = 60 * m
        assert list(cql.execute(f"SELECT * FROM {table} WHERE a = 'ascii'")) == [(
            'ascii', 123456789, b'\xde\xad\xbe\xef', True, 3.14, float_3_14, '127.0.0.1', 3,
            'zażółć gęślą jaźń', ts, uuid, uuid, 'varchar', 123, Decimal('1.23'), 3, 3,
            Date(1), Time(1),
            Duration(1 * 12 + 2, 3 * 7 + 4, 5 * h + 6 * m + 7 * s + 8 * ms + 9 * us + 10 * ns))]

        cql.execute(f"""INSERT INTO {table} (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, u) VALUES (
            blobAsAscii(asciiAsBlob('ascii2')),
            blobAsBigint(bigintAsBlob(123456789)),
            bigintAsBlob(12),
            blobAsBoolean(booleanAsBlob(true)),
            blobAsDouble(doubleAsBlob(3.14)),
            blobAsFloat(floatAsBlob(3.14)),
            blobAsInet(inetAsBlob('127.0.0.1')),
            blobAsInt(intAsBlob(3)),
            blobAsText(textAsBlob('zażółć gęślą jaźń')),
            blobAsTimestamp(timestampAsBlob('2001-10-18 14:15:55.134+0000')),
            blobAsTimeuuid(timeuuidAsBlob(d2177dd0-eaa2-11de-a572-001b779c76e3)),
            blobAsUuid(uuidAsBlob(d2177dd0-eaa2-11de-a572-001b779c76e3)),
            blobAsVarchar(varcharAsBlob('varchar')), blobAsVarint(varintAsBlob(123)),
            blobAsDecimal(decimalAsBlob(1.23)),
            blobAsTinyint(tinyintAsBlob(3)),
            blobAsSmallint(smallintAsBlob(3)),
            blobAsDate(dateAsBlob('1970-01-02')),
            blobAsTime(timeAsBlob('00:00:00.000000001')),
            blobAsDuration(durationAsBlob(10y9mo8w7d6h5m4s3ms2us1ns))
            )""")
        assert list(cql.execute(f"SELECT * FROM {table} WHERE a = 'ascii2'")) == [(
            'ascii2', 123456789, bytes.fromhex('000000000000000c'), True, 3.14, float_3_14, '127.0.0.1', 3,
            'zażółć gęślą jaźń', ts, uuid, uuid, 'varchar', 123, Decimal('1.23'), 3, 3,
            Date(1), Time(1),
            Duration(10 * 12 + 9, 8 * 7 + 7, 6 * h + 5 * m + 4 * s + 3 * ms + 2 * us + 1 * ns))]


def test_order_by(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p1 int, c1 int, c2 int, r1 int, r2 int, PRIMARY KEY(p1, c1, c2)") as table:
        def check(where_and_order, expected):
            # ORDER BY with IN on the partition key cannot be paged, so disable
            # paging for all queries here.
            stmt = SimpleStatement(f"select c1, c2, r1 from {table} where {where_and_order}", fetch_size=None)
            assert list(cql.execute(stmt)) == expected

        cql.execute(f"insert into {table} (p1, c1, c2, r1) values (0, 1, 2, 3)")
        cql.execute(f"insert into {table} (p1, c1, c2, r1) values (0, 2, 1, 0)")

        check("p1 = 0 order by c1 asc", [(1, 2, 3), (2, 1, 0)])
        check("p1 = 0 order by c1 desc", [(2, 1, 0), (1, 2, 3)])

        cql.execute(f"insert into {table} (p1, c1, c2, r1) values (0, 1, 1, 4)")
        cql.execute(f"insert into {table} (p1, c1, c2, r1) values (0, 2, 2, 5)")
        check("p1 = 0 order by c1 desc, c2 desc", [(2, 2, 5), (2, 1, 0), (1, 2, 3), (1, 1, 4)])

        cql.execute(f"insert into {table} (p1, c1, c2, r1) values (1, 1, 0, 6)")
        cql.execute(f"insert into {table} (p1, c1, c2, r1) values (1, 2, 3, 7)")

        check("p1 in (0, 1) order by c1 desc, c2 desc",
            [(2, 3, 7), (2, 2, 5), (2, 1, 0), (1, 2, 3), (1, 1, 4), (1, 0, 6)])
        check("p1 in (0, 1) order by c1 asc, c2 asc",
            [(1, 0, 6), (1, 1, 4), (1, 2, 3), (2, 1, 0), (2, 2, 5), (2, 3, 7)])
        check("p1 in (0, 1) and c1 < 2 order by c1 desc, c2 desc limit 1", [(1, 2, 3)])
        check("p1 in (0, 1) and c1 >= 2 order by c1 asc, c2 asc limit 1", [(2, 1, 0)])
        check("p1 in (0, 1) order by c1 desc, c2 desc limit 1", [(2, 3, 7)])
        check("p1 in (0, 1) order by c1 asc, c2 asc limit 1", [(1, 0, 6)])
        check("p1 = 0 and c1 > 1 order by c1 desc, c2 desc", [(2, 2, 5), (2, 1, 0)])
        check("p1 = 0 and c1 >= 2 order by c1 desc, c2 desc", [(2, 2, 5), (2, 1, 0)])
        check("p1 = 0 and c1 >= 2 order by c1 desc, c2 desc limit 1", [(2, 2, 5)])
        check("p1 = 0 order by c1 desc, c2 desc limit 1", [(2, 2, 5)])
        check("p1 = 0 and c1 > 1 order by c1 asc, c2 asc", [(2, 1, 0), (2, 2, 5)])
        check("p1 = 0 and c1 >= 2 order by c1 asc, c2 asc", [(2, 1, 0), (2, 2, 5)])
        check("p1 = 0 and c1 >= 2 order by c1 asc, c2 asc limit 1", [(2, 1, 0)])
        check("p1 = 0 order by c1 asc, c2 asc limit 1", [(1, 1, 4)])


def test_order_by_validate(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p1 int, c1 int, c2 int, r1 int, r2 int, PRIMARY KEY(p1, c1, c2)") as table:
        with pytest.raises(InvalidRequest):
            cql.execute(f"select c2, r1 from {table} where p1 = 0 order by c desc")
        with pytest.raises(InvalidRequest):
            cql.execute(f"select c2, r1 from {table} where p1 = 0 order by c2 desc")
        with pytest.raises(InvalidRequest):
            cql.execute(f"select c2, r1 from {table} where p1 = 0 order by c1 desc, c2 asc")
        with pytest.raises(InvalidRequest):
            cql.execute(f"select c2, r1 from {table} order by c1 asc")


def test_multi_column_restrictions(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p1 int, c1 int, c2 int, c3 int, r1 int, PRIMARY KEY (p1, c1, c2, c3)") as table:
        r1 = 0
        for c1 in range(2):
            for c2 in range(2):
                for c3 in range(2):
                    cql.execute(f"insert into {table} (p1, c1, c2, c3, r1) values (0, {c1}, {c2}, {c3}, {r1})")
                    r1 += 1
        def check(where, expected):
            assert list(cql.execute(f"select r1 from {table} where p1 = 0 and {where}")) == [(x,) for x in expected]
        check("(c1, c2, c3) = (0, 1, 1)", [3])
        check("(c1, c2) = (0, 1)", [2, 3])
        check("(c1, c2, c3) in ((0, 1, 0), (1, 0, 1), (0, 1, 0))", [2, 5])
        check("(c1, c2) in ((0, 1), (1, 0), (0, 1))", [2, 3, 4, 5])
        check("(c1, c2, c3) >= (1, 0, 1)", [5, 6, 7])
        check("(c1, c2, c3) >= (0, 1, 1) and (c1, c2, c3) < (1, 1, 0)", [3, 4, 5])
        check("(c1, c2) >= (0, 1) and (c1, c2, c3) < (1, 0, 1)", [2, 3, 4])
        check("(c1, c2, c3) > (0, 1, 0) and (c1, c2) <= (0, 1)", [3])


def test_select_distinct(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p1 int, c1 int, r1 int, PRIMARY KEY (p1, c1)") as table:
        cql.execute(f"insert into {table} (p1, c1, r1) values (0, 0, 0)")
        cql.execute(f"insert into {table} (p1, c1, r1) values (1, 1, 1)")
        cql.execute(f"insert into {table} (p1, c1, r1) values (1, 1, 2)")
        cql.execute(f"insert into {table} (p1, c1, r1) values (2, 2, 2)")
        cql.execute(f"insert into {table} (p1, c1, r1) values (2, 3, 3)")
        assert sorted(cql.execute(f"select distinct p1 from {table}")) == [(0,), (1,), (2,)]
        assert sorted(cql.execute(f"select distinct p1 from {table} limit 3")) == [(0,), (1,), (2,)]

    with new_test_table(cql, test_keyspace, "p1 int, p2 int, c1 int, r1 int, PRIMARY KEY ((p1, p2), c1)") as table:
        for p in range(3):
            for c in range(2):
                cql.execute(f"insert into {table} (p1, p2, c1, r1) values ({p}, {p}, {c}, {c})")
        assert sorted(cql.execute(f"select distinct p1, p2 from {table}")) == [(0, 0), (1, 1), (2, 2)]
        assert sorted(cql.execute(f"select distinct p1, p2 from {table} limit 3")) == [(0, 0), (1, 1), (2, 2)]

    with new_test_table(cql, test_keyspace, "p1 int, r1 int, PRIMARY KEY (p1)") as table:
        cql.execute(f"insert into {table} (p1, r1) values (0, 0)")
        cql.execute(f"insert into {table} (p1, r1) values (1, 1)")
        cql.execute(f"insert into {table} (p1, r1) values (1, 2)")
        cql.execute(f"insert into {table} (p1, r1) values (2, 2)")
        assert sorted(cql.execute(f"select distinct p1 from {table}")) == [(0,), (1,), (2,)]

    with new_test_table(cql, test_keyspace, "p1 int, c1 int, s1 int static, r1 int, PRIMARY KEY (p1, c1)") as table:
        cql.execute(f"insert into {table} (p1, c1, s1, r1) values (0, 0, 0, 0)")
        cql.execute(f"insert into {table} (p1, c1, r1) values (0, 1, 1)")
        cql.execute(f"insert into {table} (p1, s1) values (2, 1)")
        cql.execute(f"insert into {table} (p1, s1) values (3, 2)")
        assert sorted(cql.execute(f"select distinct p1, s1 from {table}")) == [(0, 0), (2, 1), (3, 2)]


def test_select_distinct_with_where_clause(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "k int, a int, b int, PRIMARY KEY (k, a)") as table:
        for i in range(10):
            cql.execute(f"INSERT INTO {table} (k, a, b) VALUES ({i}, {i}, {i})")
            cql.execute(f"INSERT INTO {table} (k, a, b) VALUES ({i}, {i * 10}, {i * 10})")
        with pytest.raises(InvalidRequest):
            cql.execute(f"SELECT DISTINCT k FROM {table} WHERE a >= 80 ALLOW FILTERING")
        with pytest.raises(InvalidRequest):
            cql.execute(f"SELECT DISTINCT k FROM {table} WHERE k IN (1, 2, 3) AND a = 10")
        with pytest.raises(InvalidRequest):
            cql.execute(f"SELECT DISTINCT k FROM {table} WHERE b = 5")
        assert list(cql.execute(f"SELECT DISTINCT k FROM {table} WHERE k = 1")) == [(1,)]
        assert sorted(cql.execute(f"SELECT DISTINCT k FROM {table} WHERE k IN (5, 6, 7)")) == [(5,), (6,), (7,)]

    # static columns
    with new_test_table(cql, test_keyspace, "k int, a int, s int static, b int, PRIMARY KEY (k, a)") as table:
        for i in range(10):
            cql.execute(f"INSERT INTO {table} (k, a, b, s) VALUES ({i}, {i}, {i}, {i})")
            cql.execute(f"INSERT INTO {table} (k, a, b, s) VALUES ({i}, {i * 10}, {i * 10}, {i * 10})")
        assert list(cql.execute(f"SELECT DISTINCT s FROM {table} WHERE k = 5")) == [(50,)]
        assert sorted(cql.execute(f"SELECT DISTINCT s FROM {table} WHERE k IN (5, 6, 7)")) == [(50,), (60,), (70,)]


def test_batch_insert_statement(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p1 varchar, c1 int, r1 int, PRIMARY KEY (p1, c1)") as table:
        cql.execute(f"""BEGIN BATCH
insert into {table} (p1, c1, r1) values ('key1', 1, 100);
insert into {table} (p1, c1, r1) values ('key2', 2, 200);
APPLY BATCH;""")
        cql.execute(f"""BEGIN BATCH
update {table} set r1 = 66 where p1 = 'key1' and c1 = 1;
update {table} set r1 = 33 where p1 = 'key2' and c1 = 2;
APPLY BATCH;""")
        assert list(cql.execute(f"select r1 from {table} where p1 = 'key1' and c1 = 1")) == [(66,)]
        assert list(cql.execute(f"select r1 from {table} where p1 = 'key2' and c1 = 2")) == [(33,)]


# Regression test for SCYLLADB-2474: have_multiple_cfs misclassification in
# batch_statement::prepare(): the flag was assigned with = instead of |=, so a
# batch whose first and last sub-statements target the same table (e.g.
# [ta, tb, ta]) had the flag cleared on the last sub-statement and was
# misclassified as targeting a single table. A single-table batch is given a
# routing key (partition_key_bind_indices) computed from its first
# sub-statement, while a multi-table batch has no single partition key and must
# have none. The misclassification therefore makes the prepared statement
# advertise a bogus routing key.
def test_batch_multi_table_has_no_partition_key_bind_indices(cql, test_keyspace, scylla_only, monkeypatch):
    # The Python driver, when the server returns no partition key indexes
    # for a prepared statement, computes its own routing_key_indexes from
    # the bound column names, so we can't check routing_key_indexes. Instead,
    # capture the pk_indexes the server returned in the PREPARE response.
    captured_pk_indexes = []
    original_from_message = PreparedStatement.from_message
    def capturing_from_message(cls, query_id, column_metadata, pk_indexes, *args, **kwargs):
        captured_pk_indexes.append(pk_indexes)
        return original_from_message(query_id, column_metadata, pk_indexes, *args, **kwargs)
    monkeypatch.setattr(PreparedStatement, 'from_message', classmethod(capturing_from_message))
    def prepare_and_get_pk_indexes(query):
        captured_pk_indexes.clear()
        cql.prepare(query)
        assert len(captured_pk_indexes) == 1
        return captured_pk_indexes[0]

    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as ta, \
         new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as tb:
        # A multi-table batch whose first and last sub-statements target the
        # same table must not advertise a single routing key.
        assert not prepare_and_get_pk_indexes(
            "BEGIN BATCH "
            f"INSERT INTO {ta} (pk, v) VALUES (?, ?); "
            f"INSERT INTO {tb} (pk, v) VALUES (?, ?); "
            f"INSERT INTO {ta} (pk, v) VALUES (?, ?); "
            "APPLY BATCH")
        # A single-table batch, on the other hand, must keep its routing key.
        # This guards against the fix accidentally breaking the single-table
        # case.
        assert prepare_and_get_pk_indexes(
            "BEGIN BATCH "
            f"INSERT INTO {ta} (pk, v) VALUES (?, ?); "
            f"INSERT INTO {ta} (pk, v) VALUES (?, ?); "
            f"INSERT INTO {ta} (pk, v) VALUES (?, ?); "
            "APPLY BATCH")


def test_in_restriction(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p1 int, c1 int, r1 int, PRIMARY KEY (p1, c1)") as table:
        cql.execute(f"insert into {table} (p1, c1, r1) values (0, 0, 0)")
        cql.execute(f"insert into {table} (p1, c1, r1) values (1, 0, 1)")
        cql.execute(f"insert into {table} (p1, c1, r1) values (1, 1, 2)")
        cql.execute(f"insert into {table} (p1, c1, r1) values (1, 2, 3)")
        cql.execute(f"insert into {table} (p1, c1, r1) values (2, 3, 4)")
        assert list(cql.execute(f"select * from {table} where p1 in ()")) == []
        assert sorted(cql.execute(f"select r1 from {table} where p1 in (2, 0, 2, 1)")) == [(0,), (1,), (2,), (3,), (4,)]
        assert list(cql.execute(f"select r1 from {table} where p1 = 1 and c1 in ()")) == []
        assert list(cql.execute(f"select r1 from {table} where p1 = 1 and c1 in (2, 0, 2, 1)")) == [(1,), (2,), (3,)]
        assert list(cql.execute(f"select r1 from {table} where p1 = 1 and c1 in (2, 0, 2, 1) order by c1 desc")) == [(3,), (2,), (1,)]
        stmt = cql.prepare(f"select r1 from {table} where p1 in ?")
        assert sorted(cql.execute(stmt, [[2, 0, 2, 1]])) == [(0,), (1,), (2,), (3,), (4,)]

    with new_test_table(cql, test_keyspace, "p1 int, c1 int, r1 int, PRIMARY KEY (p1, c1, r1)") as table:
        cql.execute(f"insert into {table} (p1, c1, r1) values (0, 0, 0)")
        cql.execute(f"insert into {table} (p1, c1, r1) values (1, 0, 1)")
        cql.execute(f"insert into {table} (p1, c1, r1) values (1, 1, 2)")
        cql.execute(f"insert into {table} (p1, c1, r1) values (1, 2, 3)")
        cql.execute(f"insert into {table} (p1, c1, r1) values (2, 3, 4)")
        assert list(cql.execute(f"select r1 from {table} where (c1,r1) in ((0, 1),(1,2),(0,1),(1,2),(3,3)) allow filtering")) == [(1,), (2,)]
        stmt = cql.prepare(f"select r1 from {table} where (c1,r1) in ? allow filtering")
        assert list(cql.execute(stmt, [[(0, 1), (1, 2), (0, 1), (1, 2), (3, 3)]])) == [(1,), (2,)]


def test_compact_storage(cql, test_keyspace, compact_storage):
    with new_test_table(cql, test_keyspace, "p1 int, c1 int, r1 int, PRIMARY KEY (p1, c1)", "with compact storage") as table:
        cql.execute(f"insert into {table} (p1, c1, r1) values (1, 2, 3)")
        assert list(cql.execute(f"select r1 from {table} where p1 = 1 and c1 = 2")) == [(3,)]
        cql.execute(f"update {table} set r1 = 4 where p1 = 1 and c1 = 2")
        assert list(cql.execute(f"select r1 from {table} where p1 = 1 and c1 = 2")) == [(4,)]
        assert list(cql.execute(f"select * from {table} where p1 = 1")) == [(1, 2, 4)]
    with new_test_table(cql, test_keyspace, "p1 int, c1 int, PRIMARY KEY (p1, c1)", "with compact storage") as table:
        cql.execute(f"insert into {table} (p1, c1) values (1, 2)")
        assert list(cql.execute(f"select * from {table} where p1 = 1")) == [(1, 2)]
    with new_test_table(cql, test_keyspace, "p1 int, c1 int, c2 int, r1 int, PRIMARY KEY (p1, c1, c2)", "with compact storage") as table:
        cql.execute(f"insert into {table} (p1, c1, c2, r1) values (1, 2, 3, 4)")
        cql.execute(f"insert into {table} (p1, c1, r1) values (1, 2, 5)")
        cql.execute(f"insert into {table} (p1, c1, r1) values (1, 3, 6)")
        cql.execute(f"insert into {table} (p1, c1, c2, r1) values (1, 3, 5, 7)")
        cql.execute(f"insert into {table} (p1, c1, c2, r1) values (1, 3, blobasint(0x), 8)")
        # A missing c2 (null) sorts before an empty c2, which sorts before any
        # other value. Use intasblob() to distinguish null from empty values.
        def select():
            return list(cql.execute(f"select p1, c1, intasblob(c2), r1 from {table} where p1 = 1"))
        i = lambda v: struct.pack('>i', v)
        assert select() == [
            (1, 2, None, 5),
            (1, 2, i(3), 4),
            (1, 3, None, 6),
            (1, 3, b'', 8),
            (1, 3, i(5), 7),
        ]
        cql.execute(f"delete from {table} where p1 = 1 and c1 = 2")
        assert select() == [
            (1, 3, None, 6),
            (1, 3, b'', 8),
            (1, 3, i(5), 7),
        ]
        cql.execute(f"delete from {table} where p1 = 1 and c1 = 3 and c2 = 5")
        assert select() == [
            (1, 3, None, 6),
            (1, 3, b'', 8),
        ]
        cql.execute(f"delete from {table} where p1 = 1 and c1 = 3 and c2 = blobasint(0x)")
        assert select() == [
            (1, 3, None, 6),
        ]
    with new_test_table(cql, test_keyspace, "p1 int PRIMARY KEY, c1 int, c2 int", "with compact storage") as table:
        cql.execute(f"insert into {table} (p1) values (1)")
        assert list(cql.execute(f"select * from {table}")) == []


def test_collections_of_collections(cql, test_keyspace):
    # The driver returns frozen sets nested inside a set or used as map keys
    # as SortedSet objects; normalize them to frozensets for comparison.
    with new_test_table(cql, test_keyspace, "p1 int PRIMARY KEY, v set<frozen<set<int>>>") as table:
        def check(expected):
            rows = list(cql.execute(f"select v from {table} where p1 = 1"))
            assert len(rows) == 1
            assert {frozenset(s) for s in rows[0].v} == expected
        cql.execute(f"insert into {table} (p1, v) values (1, {{{{1, 2}}, {{3, 4}}, {{5, 6}}}})")
        check({frozenset({1, 2}), frozenset({3, 4}), frozenset({5, 6})})
        cql.execute(f"delete v[{{3, 4}}] from {table} where p1 = 1")
        check({frozenset({1, 2}), frozenset({5, 6})})
        cql.execute(f"update {table} set v = v - {{{{1, 2}}, {{5}}}} where p1 = 1")
        check({frozenset({5, 6})})
    with new_test_table(cql, test_keyspace, "p1 int PRIMARY KEY, v map<frozen<set<int>>, int>") as table:
        def check(expected):
            rows = list(cql.execute(f"select v from {table} where p1 = 1"))
            assert len(rows) == 1
            assert {frozenset(k): v for k, v in rows[0].v.items()} == expected
        cql.execute(f"insert into {table} (p1, v) values (1, {{{{1, 2}}: 7, {{3, 4}}: 8, {{5, 6}}: 9}})")
        check({frozenset({1, 2}): 7, frozenset({3, 4}): 8, frozenset({5, 6}): 9})
        cql.execute(f"delete v[{{3, 4}}] from {table} where p1 = 1")
        check({frozenset({1, 2}): 7, frozenset({5, 6}): 9})
        cql.execute(f"update {table} set v = v - {{{{1, 2}}, {{5}}}} where p1 = 1")
        check({frozenset({5, 6}): 9})


def test_result_order(cql, test_keyspace, compact_storage):
    with new_test_table(cql, test_keyspace, "p1 int, c1 text, r1 int, PRIMARY KEY (p1, c1)", "with compact storage") as table:
        cql.execute(f"insert into {table} (p1, c1, r1) values (1, 'z', 1)")
        cql.execute(f"insert into {table} (p1, c1, r1) values (1, 'bbbb', 2)")
        cql.execute(f"insert into {table} (p1, c1, r1) values (1, 'a', 3)")
        cql.execute(f"insert into {table} (p1, c1, r1) values (1, 'aaa', 4)")
        cql.execute(f"insert into {table} (p1, c1, r1) values (1, 'bb', 5)")
        cql.execute(f"insert into {table} (p1, c1, r1) values (1, 'cccc', 6)")
        assert list(cql.execute(f"select * from {table} where p1 = 1")) == [
            (1, 'a', 3),
            (1, 'aaa', 4),
            (1, 'bb', 5),
            (1, 'bbbb', 2),
            (1, 'cccc', 6),
            (1, 'z', 1),
        ]


def test_frozen_collections(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "a int, b int, c frozen<map<set<int>, list<int>>> static, d int, PRIMARY KEY (a, b)") as table:
        cql.execute(f"INSERT INTO {table} (a, b, c, d) VALUES (0, 0, {{}}, 0)")
        # An empty frozen map is a value, not null
        assert list(cql.execute(f"SELECT * FROM {table}")) == [(0, 0, {}, 0)]


def test_alter_table(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk1 int, c1 int, ck2 int, r1 int, r2 int, PRIMARY KEY (pk1, c1, ck2)") as table:
        ks, cf = table.split('.')
        cql.execute(f"insert into {table} (pk1, c1, ck2, r1, r2) values (1, 2, 3, 4, 5)")
        cql.execute(f"alter table {table} with comment = 'This is a comment.'")
        assert cql.execute(f"SELECT comment FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = '{cf}'").one().comment == 'This is a comment.'
        cql.execute(f"alter table {table} alter r2 type blob")
        assert list(cql.execute(f"select pk1, c1, ck2, r1, r2 from {table}")) == [(1, 2, 3, 4, struct.pack('>i', 5))]
        cql.execute(f"insert into {table} (pk1, c1, ck2, r2) values (1, 2, 3, 0x1234567812345678)")
        blob = bytes.fromhex('1234567812345678')
        assert list(cql.execute(f"select pk1, c1, ck2, r1, r2 from {table}")) == [(1, 2, 3, 4, blob)]
        cql.execute(f"alter table {table} rename pk1 to p1 and ck2 to c2")
        assert list(cql.execute(f"select p1, c1, c2, r1, r2 from {table}")) == [(1, 2, 3, 4, blob)]
        cql.execute(f"alter table {table} add r1_2 int")
        cql.execute(f"insert into {table} (p1, c1, c2, r1_2) values (1, 2, 3, 6)")
        assert list(cql.execute(f"select * from {table}")) == [(1, 2, 3, 4, 6, blob)]
        cql.execute(f"alter table {table} drop r1")
        assert list(cql.execute(f"select * from {table}")) == [(1, 2, 3, 6, blob)]
        cql.execute(f"alter table {table} add r1 int")
        assert list(cql.execute(f"select * from {table}")) == [(1, 2, 3, None, 6, blob)]
        cql.execute(f"alter table {table} drop r2")
        cql.execute(f"alter table {table} add r2 int")
        assert list(cql.execute(f"select * from {table}")) == [(1, 2, 3, None, 6, None)]


def test_map_query(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "k int PRIMARY KEY, m map<text, int>") as table:
        cql.execute(f"insert into {table} (k, m) values (0, {{'v2': 1}})")
        assert list(cql.execute(f"select m from {table} where k = 0")) == [({'v2': 1},)]
        cql.execute(f"delete m['v2'] from {table} where k = 0")
        assert list(cql.execute(f"select m from {table} where k = 0")) == [(None,)]


def test_drop_table(cql, test_keyspace):
    tmp = f"{test_keyspace}.{unique_name()}"
    cql.execute(f"create table {tmp} (pk int, v int, PRIMARY KEY (pk))")
    cql.execute(f"drop columnfamily {tmp}")
    cql.execute(f"create table {tmp} (pk int, v int, PRIMARY KEY (pk))")
    cql.execute(f"drop columnfamily {tmp}")


def test_reversed_slice_with_empty_range_before_all_rows(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "a int, b int, c int, s1 int static, s2 int static, PRIMARY KEY (a, b)") as table:
        for i in range(16):
            cql.execute(f"INSERT INTO {table} (a, b, c, s1, s2) VALUES (99, {i}, {i}, 17, 42)")
        assert list(cql.execute(f"select * from {table} WHERE a = 99 and b < 0 ORDER BY b DESC limit 2")) == []
        assert len(list(cql.execute(f"select * from {table} WHERE a = 99 order by b desc"))) == 16
        assert len(list(cql.execute(f"select * from {table}"))) == 16


# Test that the sstable layer correctly handles reversed slices, in particular
# slices that read many clustering ranges, such that there is a large enough gap
# between the ranges for the reader to attempt to use the promoted index for
# skipping between them.
# For this reason, the test writes a large partition (10MB), then issues a
# reverse query which reads 4 singular clustering ranges from it. The ranges are
# constructed such that there is many clustering rows between them: roughly 20%
# which is ~2MB.
# See #6171
def test_reversed_slice_with_many_clustering_ranges(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "pk int, ck int, v text, PRIMARY KEY (pk, ck)") as table:
        stmt = cql.prepare(f"INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)")
        pk = 0
        value = 'a' * 1024
        num_rows = 10 * 1024
        execute_concurrent_with_args(cql, stmt, [(pk, i, value) for i in range(num_rows)], concurrency=100)

        nodetool.flush(cql, table)

        selected_cks = [2 * (num_rows // 10), 4 * (num_rows // 10), 6 * (num_rows // 10), 8 * (num_rows // 10)]

        # Many singular ranges - to check that the right range is used for
        # determining the disk read-range upper bound.
        cks = ', '.join(str(ck) for ck in selected_cks)
        rows = list(cql.execute(f"SELECT * FROM {table} WHERE pk = {pk} and ck IN ({cks}) ORDER BY ck DESC BYPASS CACHE"))
        assert rows == [(pk, ck, value) for ck in reversed(selected_cks)]

        # A single wide range - to check that the right range bound is used for
        # determining the disk read-range upper bound.
        rows = list(cql.execute(f"SELECT * FROM {table} WHERE pk = {pk} and ck >= {selected_cks[0]} and ck <= {selected_cks[1]} ORDER BY ck DESC BYPASS CACHE"))
        assert rows == [(pk, ck, value) for ck in reversed(range(selected_cks[0], selected_cks[1] + 1))]


def test_query_with_range_tombstones(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int, ck int, v int, PRIMARY KEY (pk, ck)") as table:
        for i in [0, 2, 4, 5, 6]:
            cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (0, {i}, {i})")
        cql.execute(f"DELETE FROM {table} WHERE pk = 0 AND ck >= 1 AND ck <= 3")
        cql.execute(f"DELETE FROM {table} WHERE pk = 0 AND ck > 4 AND ck <= 8")
        cql.execute(f"DELETE FROM {table} WHERE pk = 0 AND ck > 0 AND ck <= 1")
        assert list(cql.execute(f"SELECT v FROM {table} WHERE pk = 0 ORDER BY ck DESC")) == [(4,), (0,)]
        assert list(cql.execute(f"SELECT v FROM {table} WHERE pk = 0")) == [(0,), (4,)]
