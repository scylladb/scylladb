# -*- coding: utf-8 -*-
# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for various CQL statements, converted from test/boost/cql_query_test.cc.
#############################################################################

from contextlib import contextmanager

from cassandra import InvalidRequest, Unauthorized
from cassandra.protocol import ConfigurationException, SyntaxException
import pytest

from .util import config_value_context, new_session, new_test_keyspace, new_user, unique_name


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
