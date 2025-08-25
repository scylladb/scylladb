
# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

###############################################################################
# Tests for server-side describe
###############################################################################

import pytest
import random
import re
import textwrap
from contextlib import contextmanager, ExitStack
from .util import new_type, unique_name, new_test_table, new_test_keyspace, new_function, new_aggregate, \
    new_cql, keyspace_has_tablets, unique_name_prefix, new_session, new_user, new_materialized_view, \
    new_secondary_index
from cassandra.protocol import InvalidRequest, Unauthorized
from collections.abc import Iterable
from typing import Any

# Type of the row returned by `DESC` statements. It's of form
#   (keyspace_name, type, name, create_statement)
DescRowType = Any

DEFAULT_SUPERUSER = "cassandra"
# The prefix of the create statement returned by `DESC SCHEMA` and corresponding to a CDC log table.
CDC_LOG_TABLE_DESC_PREFIX =                                                                \
    "/* Do NOT execute this statement! It's only for informational purposes.\n"            \
    "   A CDC log table is created automatically when creating the base with CDC\n"        \
    "   enabled option or creating the vector index on the base table's vector column.\n"  \
    "\n"
CDC_LOG_TABLE_DESC_SUFFIX = "\n*/"

def filter_non_default_user(desc_result_iter: Iterable[DescRowType]) -> Iterable[DescRowType]:
    return filter(lambda result: result.name != DEFAULT_SUPERUSER, desc_result_iter)

###

def filter_roles(desc_result_iter: Iterable[DescRowType]) -> Iterable[DescRowType]:
    return filter(lambda result: result.type == "role", desc_result_iter)

def filter_grant_roles(desc_result_iter: Iterable[DescRowType]) -> Iterable[DescRowType]:
    return filter(lambda result: result.type == "grant_role", desc_result_iter)

def filter_grant_permissions(desc_result_iter: Iterable[DescRowType]) -> Iterable[DescRowType]:
    return filter(lambda result: result.type == "grant_permission", desc_result_iter)

def filter_service_levels(desc_result_iter: Iterable[DescRowType]) -> Iterable[DescRowType]:
    return filter(lambda result: result.type == "service_level", desc_result_iter)

def filter_attached_service_levels(desc_result_iter: Iterable[DescRowType]) -> Iterable[DescRowType]:
    return filter(lambda result: result.type == "service_level_attachment", desc_result_iter)

###

def extract_names(desc_result_iter: Iterable[DescRowType]) -> Iterable[str]:
    return map(lambda result: result.name, desc_result_iter)

def extract_create_statements(desc_result_iter: Iterable[DescRowType]) -> Iterable[str]:
    return map(lambda result: result.create_statement, desc_result_iter)

# (`element` refers to keyspace or keyspace's element(table, type, function, aggregate))
# There are 2 main types of tests:
# - tests for listings (DESC TABLES/DESC KEYSPACES/DESC TYPES/...)
#   Those tests lists elements in two ways. First the elements are listed
#   using describe statement, second the proper table from `system_schema`
#   is queried. Each row from the table should be present in result from
#   describe statement.
#   e.g.: Result of `DESC TABLE` is compared to 
#   `SELECT keyspace_name, table_name FROM system_schema.tables`
#
# - tests for element's description (aka create statement)
#   Those tests are expected to validate the create statement of an element,
#   in other words, element created by copy-pasting description should be
#   identical to the original one.
#   The similarity is validated by comparing rows from tables
#   in `system_scehma` keyspaces (excluding elements' name).
#   In order to cover all possibilities, the elements are created randomly.
#
# There are also single tests for DESC CLUSTER and DESC SCHEMA statements and also
# for some corner cases.

# Test `DESC {elements}` listing. Assert if all rows from `system_schema.{elements}`
# are in describe result
def assert_element_listing(cql, elements, name_column, name_f):
    # Create new session to ensure no `USE keyspace` statement was executed before
    with new_cql(cql) as ncql:
        table_result = ncql.execute(f"SELECT keyspace_name, {name_column} FROM system_schema.{elements}")
        desc_result = ncql.execute(f"DESC {elements}")

        desc_listing = [(r.keyspace_name, r.name) for r in desc_result]

        for row in table_result:
            assert (maybe_quote(row.keyspace_name), maybe_quote(name_f(row))) in desc_listing

# Test that `DESC KEYSPACES` contains all keyspaces
def test_keyspaces(cql, test_keyspace):
    assert_element_listing(cql, "keyspaces", "keyspace_name", lambda r: r.keyspace_name)

# Test that `DESC TABLES` contains all tables
def test_tables(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "id int PRIMARY KEY"):
        assert_element_listing(cql, "tables", "table_name", lambda r: r.table_name)

# Test that `DESC TYPES` contains all user types
def test_desc_types(cql, test_keyspace):
    with new_random_type(cql, test_keyspace), new_random_type(cql, test_keyspace):
        assert_element_listing(cql, "types", "type_name", lambda r: r.type_name)

# Test that `DESC FUNCTIONS` contains all user functions
def test_desc_functions(scylla_only, cql, test_keyspace):
    fn_schema = "(val int)"
    fn_null = "RETURNS NULL ON NULL INPUT"
    fn_return = "RETURNS int"
    fn_language = "LANGUAGE lua"
    fn_body = "AS 'return -val'"
    with new_function(cql, test_keyspace, f"{fn_schema} {fn_null} {fn_return} {fn_language} {fn_body}"):
        assert_element_listing(cql, "functions", "function_name", lambda r: r.function_name)
        
# Test that `DESC AGGREGATES` contains all user aggregates
def test_desc_aggregates(scylla_only, cql, test_keyspace):
    fn_schema = "(val1 int, val2 int)"
    fn_null = "RETURNS NULL ON NULL INPUT"
    fn_return = "RETURNS int"
    fn_language = "LANGUAGE lua"
    fn_body = "return val1 + val2"

    with new_function(cql, test_keyspace, f"{fn_schema} {fn_null} {fn_return} {fn_language} AS '{fn_body}'") as fn:
        agg_body = f"(int) SFUNC {fn} STYPE int INITCOND 0"
        with new_aggregate(cql, test_keyspace, agg_body):
            assert_element_listing(cql, "aggregates", "aggregate_name", lambda r: r.aggregate_name)

# Test that `DESC ONLY KEYSPACE {ks}` contains appropriate create statement for keyspace
# This test compares the content of `system_schema.keyspaces` table.
def test_desc_keyspace(cql, random_seed):
    with new_random_keyspace(cql) as ks:
        desc = cql.execute(f"DESC ONLY KEYSPACE {ks}")
        desc_stmt = desc.one().create_statement

        new_ks = unique_name()
        new_desc_stmt = desc_stmt.replace(ks, new_ks)

        try:
            cql.execute(new_desc_stmt)
            ks_row = cql.execute(f"""
                SELECT durable_writes, replication
                FROM system_schema.keyspaces
                WHERE keyspace_name='{ks}'
            """).one()
            new_ks_row = cql.execute(f"""
                SELECT durable_writes, replication
                FROM system_schema.keyspaces
                WHERE keyspace_name='{new_ks}'
            """).one()
            assert ks_row == new_ks_row
        finally:
            cql.execute(f"DROP KEYSPACE {new_ks}")
# Test that `DESC ONLY KEYSPACE {ks}` contains appropriate create statement for keyspace
# This test compares the content of `system_schema.scylla_keyspaces` table, thus the test
# is `scylla_only`.
def test_desc_scylla_keyspace(scylla_only, cql, random_seed):
    with new_random_keyspace(cql) as ks:
        desc = cql.execute(f"DESC ONLY KEYSPACE {ks}")
        desc_stmt = desc.one().create_statement

        new_ks = unique_name()
        new_desc_stmt = desc_stmt.replace(ks, new_ks)

        try:
            cql.execute(new_desc_stmt)
            ks_scylla_row = cql.execute(f"""
                SELECT storage_options, storage_type
                FROM system_schema.scylla_keyspaces
                WHERE keyspace_name='{ks}'
            """).one()
            new_ks_scylla_row = cql.execute(f"""
                SELECT storage_options, storage_type
                FROM system_schema.scylla_keyspaces
                WHERE keyspace_name='{new_ks}'
            """).one()
            assert ks_scylla_row == new_ks_scylla_row
        finally:
            cql.execute(f"DROP KEYSPACE {new_ks}")

# Test that `DESC TABLE {tbl}` contains appropriate create statement for table
# This test compares the content of `system_schema.tables` and `system_schema.columns` tables.
def test_desc_table(cql, test_keyspace, random_seed, has_tablets):
    if has_tablets:  # issue #18180
        global counter_table_chance
        counter_table_chance = 0
    with new_random_table(cql, test_keyspace) as tbl:
        desc = cql.execute(f"DESC TABLE {tbl}")
        desc_stmt = desc.one().create_statement

        new_tbl = f"{test_keyspace}.{unique_name()}"
        new_desc_stmt = desc_stmt.replace(tbl, new_tbl)

        try:
            cql.execute(new_desc_stmt)
            tbl_row = cql.execute(f"""
                SELECT keyspace_name, bloom_filter_fp_chance, caching,
                comment, compaction, compression, crc_check_chance,
                default_time_to_live, extensions,
                flags, gc_grace_seconds, max_index_interval,
                memtable_flush_period_in_ms, min_index_interval,
                speculative_retry
                FROM system_schema.tables
                WHERE keyspace_name='{test_keyspace}' AND table_name='{get_name(tbl)}'
            """).one()
            new_tbl_row = cql.execute(f"""
                SELECT keyspace_name, bloom_filter_fp_chance, caching,
                comment, compaction, compression, crc_check_chance,
                default_time_to_live, extensions,
                flags, gc_grace_seconds, max_index_interval,
                memtable_flush_period_in_ms, min_index_interval,
                speculative_retry
                FROM system_schema.tables
                WHERE keyspace_name='{test_keyspace}' AND table_name='{get_name(new_tbl)}'
            """).one()
            assert tbl_row == new_tbl_row

            col_rows = cql.execute(f"""
                SELECT keyspace_name, column_name, clustering_order, column_name_bytes,
                kind, position, type
                FROM system_schema.columns
                WHERE keyspace_name='{test_keyspace}' AND table_name='{get_name(tbl)}'
            """)
            new_col_rows = cql.execute(f"""
                SELECT keyspace_name, column_name, clustering_order, column_name_bytes,
                kind, position, type
                FROM system_schema.columns
                WHERE keyspace_name='{test_keyspace}' AND table_name='{get_name(new_tbl)}'
            """)
            assert col_rows == new_col_rows
        finally:
            cql.execute(f"DROP TABLE {new_tbl}")

# This test compares the content of `system_schema.tables` and `system_schema.columns` tables
# when providing tablet options to CREATE TABLE.
def test_desc_table_with_tablet_options(cql, test_keyspace, random_seed, has_tablets):
    if has_tablets:  # issue #18180
        global counter_table_chance
        counter_table_chance = 0
    tablet_options = {
        'min_tablet_count': '100',
        'min_per_shard_tablet_count': '0.8',   # Verify that a floating point value works for this hint
        'expected_data_size_in_gb': '50',
    }
    with new_random_table(cql, test_keyspace, tablet_options=tablet_options) as tbl:
        desc = cql.execute(f"DESC TABLE {tbl}")
        desc_create_stmt = desc.one().create_statement

        try:
            new_tbl = f"{test_keyspace}.{unique_name()}"
            new_create_stmt = desc_create_stmt.replace(tbl, new_tbl)
            cql.execute(new_create_stmt)
            new_desc_stmt = cql.execute(f"DESC TABLE {new_tbl}")
            new_desc_create_stmt = new_desc_stmt.one().create_statement
            assert new_desc_create_stmt == new_create_stmt
        finally:
            cql.execute(f"DROP TABLE {new_tbl}")

# Test that `DESC TABLE {tbl}` contains appropriate create statement for table
# This test compares the content of `system_schema.scylla_tables` tables, thus the test
# is `scylla_only`.
def test_desc_scylla_table(scylla_only, cql, test_keyspace, random_seed, has_tablets):
    if has_tablets:  # issue #18180
        global counter_table_chance
        counter_table_chance = 0
    with new_random_table(cql, test_keyspace) as tbl:
        desc = cql.execute(f"DESC TABLE {tbl}")
        desc_stmt = desc.one().create_statement

        new_tbl = f"{test_keyspace}.{unique_name()}"
        new_desc_stmt = desc_stmt.replace(tbl, new_tbl)

        try:
            cql.execute(new_desc_stmt)
            tbl_scylla_row = cql.execute(f"""
                SELECT cdc, partitioner
                FROM system_schema.scylla_tables
                WHERE keyspace_name='{test_keyspace}' AND table_name='{get_name(tbl)}'
            """).one()
            new_tbl_scylla_row = cql.execute(f"""
                SELECT cdc, partitioner
                FROM system_schema.scylla_tables
                WHERE keyspace_name='{test_keyspace}' AND table_name='{get_name(new_tbl)}'
            """).one()
            assert tbl_scylla_row == new_tbl_scylla_row
        finally:
            cql.execute(f"DROP TABLE {new_tbl}")

# Test that `DESC TYPE {udt}` contains appropriate create statement for user-defined type
def test_desc_type(cql, test_keyspace, random_seed):
    with new_random_type(cql, test_keyspace) as udt:
        desc = cql.execute(f"DESC TYPE {udt}")
        desc_stmt = desc.one().create_statement

        new_udt = f"{test_keyspace}.{unique_name()}"
        new_desc_stmt = desc_stmt.replace(udt, new_udt)

        try:
            cql.execute(new_desc_stmt)
            udt_row = cql.execute(f"""
                SELECT keyspace_name, field_names, field_types
                FROM system_schema.types
                WHERE keyspace_name='{test_keyspace}' AND type_name='{get_name(udt)}'
            """).one()
            new_udt_row = cql.execute(f"""
                SELECT keyspace_name, field_names, field_types
                FROM system_schema.types
                WHERE keyspace_name='{test_keyspace}' AND type_name='{get_name(new_udt)}'
            """).one()
            assert udt_row == new_udt_row
        finally:
            cql.execute(f"DROP TYPE {new_udt}")

# Test that `DESC FUNCTION {udf}` contains appropriate create statement for user-defined function
# The test is `scylla_only` because Scylla's UDF is written in Lua, which is not supported by Cassandra
def test_desc_function(scylla_only, cql, test_keyspace, random_seed):
    fn_schema = "(val int)"
    fn_null = "RETURNS NULL ON NULL INPUT"
    fn_return = "RETURNS int"
    fn_language = "LANGUAGE lua"
    fn_body = "AS $$return -val$$"
    with new_function(cql, test_keyspace, f"{fn_schema} {fn_null} {fn_return} {fn_language} {fn_body}") as fn:
        desc = cql.execute(f"DESC FUNCTION {test_keyspace}.{fn}")
        desc_stmt = desc.one().create_statement

        new_fn = unique_name()
        new_desc_stmt = desc_stmt.replace(fn, new_fn)

        try:
            cql.execute(new_desc_stmt)
            fn_row = cql.execute(f"""
                SELECT argument_types, argument_names, body, called_on_null_input, language, return_type
                FROM system_schema.functions
                WHERE keyspace_name='{test_keyspace}' AND function_name='{fn}'
            """).one()
            new_fn_row = cql.execute(f"""
                SELECT argument_types, argument_names, body, called_on_null_input, language, return_type
                FROM system_schema.functions
                WHERE keyspace_name='{test_keyspace}' AND function_name='{new_fn}'
            """).one()

            # The columns are tested one by one to be able to trim function's body.
            assert fn_row.argument_types == new_fn_row.argument_types
            assert fn_row.argument_names == new_fn_row.argument_names
            assert fn_row.body.strip() == new_fn_row.body.strip()
            assert fn_row.called_on_null_input == new_fn_row.called_on_null_input
            assert fn_row.language == new_fn_row.language
            assert fn_row.return_type == new_fn_row.return_type
        finally:
            cql.execute(f"DROP FUNCTION {test_keyspace}.{new_fn}")

# Test that `DESC AGGREGATE {uda}` contains appropriate create statement for user-defined aggregate
# The test is `scylla_only` because Scylla's UDA is written in Lua, which is not supported by Cassandra
def test_desc_aggregate(scylla_only, cql, test_keyspace, random_seed):
    fn_schema = "(val1 int, val2 int)"
    fn_null = "RETURNS NULL ON NULL INPUT"
    fn_return = "RETURNS int"
    fn_language = "LANGUAGE lua"
    fn_body = "return val1 + val2"

    with new_function(cql, test_keyspace, f"{fn_schema} {fn_null} {fn_return} {fn_language} AS '{fn_body}'") as fn:
        agg_body = f"(int) SFUNC {fn} STYPE int INITCOND 0"
        with new_aggregate(cql, test_keyspace, agg_body) as aggr: 
            desc = cql.execute(f"DESC AGGREGATE {test_keyspace}.{aggr}")
            desc_stmt = desc.one().create_statement

            new_aggr = unique_name()
            new_desc_stmt = desc_stmt.replace(f"{test_keyspace}.{aggr}", f"{test_keyspace}.{new_aggr}")

            try:
                cql.execute(new_desc_stmt)
                aggr_row = cql.execute(f"""
                    SELECT argument_types, final_func, initcond, return_type, state_func, state_type
                    FROM system_schema.aggregates
                    WHERE keyspace_name='{test_keyspace}' AND aggregate_name='{aggr}'
                """).one()
                new_aggr_row = cql.execute(f"""
                    SELECT argument_types, final_func, initcond, return_type, state_func, state_type
                    FROM system_schema.aggregates
                    WHERE keyspace_name='{test_keyspace}' AND aggregate_name='{new_aggr}'
                """).one()
                assert aggr_row == new_aggr_row

                aggr_scylla_row = cql.execute(f"""
                    SELECT argument_types, reduce_func, state_type
                    FROM system_schema.scylla_aggregates
                    WHERE keyspace_name='{test_keyspace}' AND aggregate_name='{aggr}'
                """).one()
                new_aggr_scylla_row = cql.execute(f"""
                    SELECT argument_types, reduce_func, state_type
                    FROM system_schema.scylla_aggregates
                    WHERE keyspace_name='{test_keyspace}' AND aggregate_name='{new_aggr}'
                """).one()
                assert aggr_scylla_row == new_aggr_scylla_row
            finally:
                cql.execute(f"DROP AGGREGATE {test_keyspace}.{new_aggr}")

# Test that `DESC TABLE {tbl} WITH INTERNALS` contains additional information for added/dropped columns
def test_desc_table_internals(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "a int primary key, b int, c int") as tbl:
        cql.execute(f"ALTER TABLE {tbl} DROP b")
        desc = cql.execute(f"DESC TABLE {tbl}").one().create_statement
        desc_internals = cql.execute(f"DESC TABLE {tbl} WITH INTERNALS").one().create_statement

        assert "b int" not in desc
        assert "b int" in desc_internals
        assert f"ALTER TABLE {tbl} DROP b" in desc_internals

        cql.execute(f"ALTER TABLE {tbl} ADD b int")
        desc = cql.execute(f"DESC TABLE {tbl}").one().create_statement
        desc_internals = cql.execute(f"DESC TABLE {tbl} WITH INTERNALS").one().create_statement

        assert "b int" in desc
        assert "b int" in desc_internals
        assert f"ALTER TABLE {tbl} DROP b" in desc_internals
        assert f"ALTER TABLE {tbl} ADD b int" in desc_internals

# Test that `DESC KEYSPACE {ks}` contains not only keyspace create statement but also for its elements
def test_desc_keyspace_elements(cql, random_seed, has_tablets):
    if has_tablets:  # issue #18180
        global counter_table_chance
        counter_table_chance = 0
    with new_random_keyspace(cql) as ks:
        with new_random_type(cql, ks) as udt:
            with new_random_table(cql, ks, [udt]) as tbl:
                desc = cql.execute(f"DESC KEYSPACE {ks}")
                ks_desc = " ".join([r.create_statement for r in desc])

                assert f"CREATE KEYSPACE {ks}" in ks_desc
                assert f"CREATE TYPE {udt}" in ks_desc
                assert f"CREATE TABLE {tbl}" in ks_desc

                only_desc = cql.execute(f"DESC ONLY KEYSPACE {ks}")
                ks_only_desc = " ".join([r.create_statement for r in only_desc])

                assert f"CREATE KEYSPACE {ks}" in ks_only_desc
                assert f"CREATE TYPE {udt}" not in ks_only_desc
                assert f"CREATE TABLE {tbl}" not in ks_only_desc

# Test that `DESC SCHEMA` contains all information for user created keyspaces
# and `DESC FULL SCHEMA` contains also information for system keyspaces
def test_desc_schema(cql, test_keyspace, random_seed, has_tablets):
    if has_tablets:  # issue #18180
        global counter_table_chance
        counter_table_chance = 0
    with new_random_keyspace(cql) as ks:
        with new_random_table(cql, test_keyspace) as tbl1, new_random_table(cql, ks) as tbl2:
            desc = cql.execute("DESC SCHEMA")
            schema_desc = " ".join([r.create_statement for r in desc])

            assert f"CREATE KEYSPACE {test_keyspace}" in schema_desc
            assert f"CREATE TABLE {tbl1}" in schema_desc
            assert f"CREATE KEYSPACE {ks}" in schema_desc
            assert f"CREATE TABLE {tbl2}" in schema_desc

            full_desc = cql.execute("DESC FULL SCHEMA")
            schema_full_desc = " ".join([r.create_statement for r in full_desc])

            kss = cql.execute("SELECT keyspace_name FROM system_schema.keyspaces")
            tbls = cql.execute("SELECT keyspace_name, table_name FROM system_schema.tables")

            for ks_row in kss:
                assert f"CREATE KEYSPACE {maybe_quote(ks_row.keyspace_name)}" in schema_full_desc
            for tbl_row in tbls:
                assert f"CREATE TABLE {maybe_quote(tbl_row.keyspace_name)}.{maybe_quote(tbl_row.table_name)}"

# Test that `DESC CLUSTER` contains token ranges to endpoints map
# The test is `scylla_only` because there is no `system.token_ring` table in Cassandra
@pytest.mark.parametrize("test_keyspace", ["tablets", "vnodes"], indirect=True)
def test_desc_cluster(scylla_only, cql, test_keyspace):
    cql.execute(f"USE {test_keyspace}")
    desc = cql.execute("DESC CLUSTER").one()

    if keyspace_has_tablets(cql, test_keyspace):
        # FIXME: there is no endpoint content for tablet keyspaces yet
        # See https://github.com/scylladb/scylladb/issues/16789
        # When run with tablets, this test currently only validates that `DESC CLUSTER` doesn't fail with tablets.
        return

    desc_endpoints = []
    for (end_token, endpoints) in desc.range_ownership.items():
        for endpoint in endpoints:
            desc_endpoints.append((end_token, endpoint))

    result = cql.execute(f"SELECT end_token, endpoint FROM system.token_ring WHERE keyspace_name='{test_keyspace}' ORDER BY table_name, start_token")
    ring_endpoints = [(r.end_token, r.endpoint) for r in result]
    
    assert sorted(desc_endpoints) == sorted(ring_endpoints)

def is_scylla(cql):
    return any('scylla' in name for name in [row.table_name for row in cql.execute("SELECT * FROM system_schema.tables WHERE keyspace_name = 'system'")])

# Test that 'DESC INDEX' contains create statement of index
def test_desc_index(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "a int primary key, b int, c int") as tbl:
        tbl_name = get_name(tbl)
        create_idx_b = f"CREATE INDEX ON {tbl}(b)"
        create_idx_c = f"CREATE INDEX named_index ON {tbl}(c)"
        # Only Scylla supports local indexes
        has_local = is_scylla(cql)
        if has_local:
            create_idx_ab = f"CREATE INDEX ON {tbl}((a), b)"

        cql.execute(create_idx_b)
        cql.execute(create_idx_c)
        if has_local:
            cql.execute(create_idx_ab)

        b_desc = cql.execute(f"DESC INDEX {test_keyspace}.{tbl_name}_b_idx").one().create_statement
        if has_local:
            ab_desc = cql.execute(f"DESC INDEX {test_keyspace}.{tbl_name}_b_idx_1").one().create_statement
        c_desc = cql.execute(f"DESC INDEX {test_keyspace}.named_index").one().create_statement

        # Cassandra inserts a space between the table name and parentheses,
        # Scylla doesn't. This difference doesn't matter because both are
        # valid CQL commands
        if is_scylla(cql):
            maybe_space = ''
        else:
            maybe_space = ' '
        assert f"CREATE INDEX {tbl_name}_b_idx ON {tbl}{maybe_space}(b)" in b_desc
        if has_local:
            assert f"CREATE INDEX {tbl_name}_b_idx_1 ON {tbl}((a), b)" in ab_desc
        
        assert f"CREATE INDEX named_index ON {tbl}{maybe_space}(c)" in c_desc

def test_desc_index_on_collections(cql, test_keyspace):
    # In this test, all assertions are in form of
    # `assert create_stmt in desc or create_stmt.replace("(", " (", 1) in desc`
    # This is because Scylla and Cassandra have slightly different (whitespace-wise)
    # output of `DESC INDEX`. 
    #   Scylla:     `CREATE INDEX ON table_name(column_name)`
    #   Cassandra:  `CREATE INDEX ON table_name (column_name)`
    #
    # This helper function asserts index create statement is present in the description
    # and it matches both Scylla and Cassandra description output style.
    def assert_desc_index(create_stmt, description):
        cassandra_format_create = create_stmt.replace("(", " (", 1)
        assert create_stmt in description or cassandra_format_create in description

    
    with new_test_table(cql, test_keyspace, "a int primary key, b int, c frozen<set<int>>, d map<int, int>, e list<int>, f set<int>, g list<int>, h set<int>, akeysb set<int>") as tbl:
        indexes = [
            ("idx_b",           f"CREATE INDEX idx_b ON {tbl}(b)"),
            ("idx_c",           f"CREATE INDEX idx_c ON {tbl}(full(c))"),
            ("idx_d_entries",   f"CREATE INDEX idx_d_entries ON {tbl}(entries(d))"),
            ("idx_d_keys",      f"CREATE INDEX idx_d_keys ON {tbl}(keys(d))"),
            ("idx_d_values",    f"CREATE INDEX idx_d_values ON {tbl}(values(d))"),
            ("idx_e",           f"CREATE INDEX idx_e ON {tbl}(values(e))"),
            ("idx_f",           f"CREATE INDEX idx_f ON {tbl}(values(f))"),
            ("idx_akeysb",      f"CREATE INDEX idx_akeysb ON {tbl}(values(akeysb))"),
        ]

        for name, create_stmt in indexes:
            cql.execute(create_stmt)
            desc = cql.execute(f"DESC INDEX {test_keyspace}.{name}").one().create_statement
            assert_desc_index(create_stmt, desc)
        
        # When an index is created on list column with `CREATE INDEX ON tbl(list_column)`,
        # its target is `values(list_column)` and describe will print create statement with that
        # values() function added.
        cql.execute(f"CREATE INDEX idx_g ON {tbl}(g)")
        idx_g_desc = cql.execute(f"DESC INDEX {test_keyspace}.idx_g").one().create_statement
        idx_g_create_stmt = f"CREATE INDEX idx_g ON {tbl}(values(g))"
        assert_desc_index(idx_g_create_stmt, idx_g_desc)

        # The same situation but with set
        cql.execute(f"CREATE INDEX idx_h ON {tbl}(h)")
        idx_h_desc = cql.execute(f"DESC INDEX {test_keyspace}.idx_h").one().create_statement
        idx_h_create_stmt = f"CREATE INDEX idx_h ON {tbl}(values(h))"
        assert_desc_index(idx_h_create_stmt, idx_h_desc)

        with pytest.raises(InvalidRequest):
            cql.execute(f"DESC INDEX {test_keyspace}.nosuchindex")
        with pytest.raises(InvalidRequest):
            cql.execute(f"DESC INDEX {tbl}")

def test_desc_index_column_quoting(cql, test_keyspace):
    # In this test, all assertions are in form of
    # `assert create_stmt in desc or create_stmt.replace("(", " (", 1)`
    # This is because Scylla and Cassandra have slightly different (whitespace-wise)
    # output of `DESC INDEX`. 
    #   Scylla:     `CREATE INDEX ON table_name(column_name)`
    #   Cassandra:  `CREATE INDEX ON table_name (column_name)`

    quoted_names = ['"select"', '"hEllo"', '"x y"', '"hi""hello""yo"', '"""hi"""']
    with new_test_table(cql, test_keyspace, "a int primary key, \"keys(m)\" map<int, int>, \"values(m)\" set<int>, \"entries(m)\" map<int, int>, " + ', '.join([name + " int" for name in quoted_names])) as tbl:
        indexes = [
            ("idx_1", f"CREATE INDEX idx_1 ON {tbl}(keys(\"keys(m)\"))"),
            ("idx_2", f"CREATE INDEX idx_2 ON {tbl}(values(\"values(m)\"))"),
            ("idx_3", f"CREATE INDEX idx_3 ON {tbl}(entries(\"entries(m)\"))"),
        ] + [(f"idx_{n}", f"CREATE INDEX idx_{n} ON {tbl}({name})") for n, name in enumerate(quoted_names, 4)]

        for name, create_stmt in indexes:
            cql.execute(create_stmt)
            desc = cql.execute(f"DESC INDEX {test_keyspace}.{name}").one().create_statement
            assert create_stmt in desc or create_stmt.replace("(", " (", 1)

def test_desc_local_secondary_index(scylla_only, cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "a int primary key, b int, c frozen<list<int>>") as tbl:
        indexes = [
            ("idx_1", f"CREATE INDEX idx_1 ON {tbl}((a), b)"),
            ("idx_2", f"CREATE INDEX idx_2 ON {tbl}((a), full(c))"),
        ]
        
        for name, create_stmt in indexes:
            cql.execute(create_stmt)
            desc = cql.execute(f"DESC INDEX {test_keyspace}.{name}").one().create_statement
            assert create_stmt in desc

# Test that 'DESC TABLE' contains description of indexes
def test_index_desc_in_table_desc(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "a int primary key, b int, c int") as tbl:
        create_idx_b = f"CREATE INDEX ON {tbl}(b)"
        create_idx_c = f"CREATE INDEX named_index ON {tbl}(c)"
        has_local = is_scylla(cql)
        if is_scylla(cql):
            maybe_space = ''
        else:
            maybe_space = ' '
        if has_local:
            create_idx_ab = f"CREATE INDEX ON {tbl}((a), b)"

        cql.execute(create_idx_b)
        cql.execute(create_idx_c)
        if has_local:
            cql.execute(create_idx_ab)

        tbl_name = get_name(tbl)
        desc = "\n".join([d.create_statement for d in cql.execute(f"DESC TABLE {tbl}")])
        print(desc)
        
        assert f"CREATE INDEX {tbl_name}_b_idx ON {tbl}{maybe_space}(b)" in desc
        assert f"CREATE INDEX named_index ON {tbl}{maybe_space}(c)" in desc
        if has_local:
            assert f"CREATE INDEX {tbl_name}_b_idx_1 ON {tbl}((a), b)" in desc

# Test that while 'DESC TABLE' and 'DESC KEYSPACE' include creation of a
# secondary index (CREATE INDEX command), neither includes a CREATE
# MATERIALIZED VIEW for the materialized view that Scylla uses internally
# to back the index. This test confirms that issue #6058 might have existed
# in cqlsh, but no longer exist for server-side describe.
def test_index_not_view_in_desc(cql):
    with new_test_keyspace(cql, "WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        with new_test_table(cql, ks, "a int primary key, b int, c int") as table:
            with new_secondary_index(cql, table, 'c'):
                desc = [d.create_statement for d in cql.execute(f"DESC TABLE {table}")]
                assert 1 == sum(1 for line in desc if 'CREATE INDEX' in line)
                assert 0 == sum(1 for line in desc if 'CREATE MATERIALIZED VIEW' in line)
                desc = [d.create_statement for d in cql.execute(f"DESC KEYSPACE {ks}")]
                assert 1 == sum(1 for line in desc if 'CREATE INDEX' in line)
                assert 0 == sum(1 for line in desc if 'CREATE MATERIALIZED VIEW' in line)

# Test that 'DESC KEYSPACE' contains description of a materialized view in
# that keyspace.
def test_view_desc_in_keyspace_desc(cql):
    with new_test_keyspace(cql, "WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        with new_test_table(cql, ks, "a int primary key, b int, c int") as table:
            with new_materialized_view(cql, table, '*', 'b, a', 'b is not null and a is not null') as mv:
                desc = [d.create_statement for d in cql.execute(f"DESC KEYSPACE {ks}")]
                assert 1 == sum(1 for line in desc if 'CREATE MATERIALIZED VIEW' in line)

# Test that 'DESC TABLE' contains description of a materialized view on that
# table. This is in fact missing in Cassandra, and I consider this a Cassandra
# but so opened https://issues.apache.org/jira/browse/CASSANDRA-20365.
# See also discussion in #23014.
def test_view_desc_in_table_desc(cql, test_keyspace, cassandra_bug):
    with new_test_table(cql, test_keyspace, "a int primary key, b int, c int") as table:
        with new_materialized_view(cql, table, '*', 'b, a', 'b is not null and a is not null') as mv:
            desc = [d.create_statement for d in cql.execute(f"DESC TABLE {table}")]
            assert 1 == sum(1 for line in desc if 'CREATE MATERIALIZED VIEW' in line)

# -----------------------------------------------------------------------------
# "Generic describe" is a describe statement without specifying what kind of object 
# you want to describe, so it only requires keyspace name(optionally) and object name.
# Generic describe should be exactly the same as normal describe, so for instannce:
# `DESC TABLE <table_name> == DESC <table_name>`.
#
# ScyllaDB looks for describing object in a following order: 
# keyspace, table, view, index, UDT, UDF, UDA

# Cassandra compatibility require us to be able generic describe: keyspace, table, view, index.
def test_generic_desc(cql, random_seed, has_tablets):
    if has_tablets:  # issue #18180
        global counter_table_chance
        counter_table_chance = 0
    with new_random_keyspace(cql) as ks:
        with new_random_table(cql, ks) as t1, new_test_table(cql, ks, "a int primary key, b int, c int") as tbl:
            cql.execute(f"CREATE INDEX idx ON {tbl}(b)")
            
            generic_ks = cql.execute(f"DESC {ks}")
            generic_t1 = cql.execute(f"DESC {t1}")
            generic_tbl = cql.execute(f"DESC {tbl}")
            generic_idx = cql.execute(f"DESC {ks}.idx")

            desc_ks = cql.execute(f"DESC KEYSPACE {ks}")
            desc_t1 = cql.execute(f"DESC TABLE {t1}")
            desc_tbl = cql.execute(f"DESC TABLE {tbl}")
            desc_idx = cql.execute(f"DESC INDEX {ks}.idx")

            assert generic_ks == desc_ks
            assert generic_t1 == desc_t1
            assert generic_tbl == desc_tbl
            assert generic_idx == desc_idx

# We've extended generic describe to include user-defined objects: UDTs, UDFs and UDAs.
# Since Cassandra doesn't generic description of support user-defined objects,
# the test is `scylla_only`.
# Reproduces #14170
def test_generic_desc_user_defined(scylla_only, cql, test_keyspace):
    with new_random_type(cql, test_keyspace) as udt:
        assert cql.execute(f"DESC {udt}") == cql.execute(f"DESC TYPE {udt}")

    with new_function(cql, test_keyspace, """
        (val1 int, val2 int)
        RETURNS NULL ON NULL INPUT
        RETURNS int
        LANGUAGE lua
        AS 'return val1 + val2'
    """) as fn:
        assert cql.execute(f"DESC {test_keyspace}.{fn}") == cql.execute(f"DESC FUNCTION {test_keyspace}.{fn}")

        with new_aggregate(cql, test_keyspace, f"(int) SFUNC {fn} STYPE int INITCOND 0") as aggr:
            assert cql.execute(f"DESC {test_keyspace}.{aggr}") == cql.execute(f"DESC AGGREGATE {test_keyspace}.{aggr}")


# Test that 'DESC FUNCTION'/'DESC AGGREGATE' doesn't show UDA/UDF and doesn't crash Scylla
# The test is marked scylla_only because it uses Lua as UDF language.
def test_desc_udf_uda(cql, test_keyspace, scylla_only):
    with new_function(cql, test_keyspace, "(a int, b int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE LUA AS 'return a+b'") as fn:
        with new_aggregate(cql, test_keyspace, f"(int) SFUNC {fn} STYPE int") as aggr:

            with pytest.raises(InvalidRequest):
                cql.execute(f"DESC FUNCTION {test_keyspace}.{aggr}")
            with pytest.raises(InvalidRequest):
                cql.execute(f"DESC AGGREGATE {test_keyspace}.{fn}")

# Test that JSON objects in DESC TABLE contains whitespaces.
# In the old client-side describe there was a whitespace after 
# each colon(:) and comma(,).
# The new server-side describe was missing it.
# Example: caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
# Reproduces #14895
# The test is marked scylla_only because it uses a Scylla-only property "cdc".
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
def test_whitespaces_in_table_options(cql, test_keyspace, scylla_only):
    regex = "\\{[^}]*[:,][^\\s][^}]*\\}" # looks for any colon or comma without space after it inside a { }
    
    with new_test_table(cql, test_keyspace, "a int primary key", "WITH cdc = {'enabled': true}") as tbl:
        desc = "\n".join([d.create_statement for d in cql.execute(f"DESC TABLE {tbl}")])
        assert re.search(regex, desc) == None

# Randomly create many UDTs, with dependencies between them 
# and validate if describe displays them in correct order.
# UDTs should be sorted topologically, meaning if UDT `a`
# is used to create UDT `b`, then `a` should be before `b`.
def test_udt_sorting(scylla_only, cql, test_keyspace, random_seed):
    repeat = 10 # test it randomized, so repeat it inside the test to increase coverage
    max_selected_udts = 5 # max number of UDTs that can be used to create a new UDT

    # Keyspace for recreating UDTs
    with new_test_keyspace(cql, "WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as keyspace:
        for _ in range(repeat):
            created_udts = []
            udts_cnt = 10
            udt_backup = []

            with ExitStack() as stack:
                # Create `udts_cnt` many UDTs
                while len(created_udts) < udts_cnt:
                    select_cnt = random.randint(0, min(max_selected_udts, len(created_udts))) # how many UDTs we want to use to create a new one
                    udt_body = "(a int)"
                    if select_cnt != 0:
                        selected_udts = random.sample(created_udts, select_cnt) # randomly select `select_cnt` already created UDTs
                        udt_body = create_udt_body(selected_udts)
                    
                    udt = stack.enter_context(new_type(cql, test_keyspace, udt_body))
                    created_udts.append(udt)
                
                # Describe all UDTs, iterate over all of them and verify the order is correct.
                # (If UDT `u` is used create UDT `w`, `u` has to be described before `w`)
                desc = cql.execute(f"DESC KEYSPACE {test_keyspace}").all()
                visited_udts = set()
                for row in desc:
                    if row.type == "type":
                        # Extract UDTs used in current UDT
                        # Names of all UDTs begin with `unique_name_prefix`
                        for it in re.finditer(f"<({unique_name_prefix}[^\s\>]+)>", row.create_statement):
                            assert f"{test_keyspace}.{it.group(1)}" in visited_udts
                        visited_udts.add(f"{row.keyspace_name}.{row.name}")
                
                assert visited_udts == set(created_udts)
                udt_backup = desc

            with ExitStack() as stack:
                drop_type = lambda cql, keyspace_name, type_name: cql.execute(f"DROP TYPE {keyspace_name}.{type_name}")

                # Recreate all UDTs from the description in a designated keyspace.
                for row in desc:
                    if row.type == "type":
                        cql.execute(row.create_statement.replace(test_keyspace, keyspace))
                        stack.callback(drop_type, cql=cql, keyspace_name=keyspace, type_name=row.name)

# -----------------------------------------------------------------------------
# Following tests `test_*_quoting` check if names inside elements' descriptions are quoted if needed.
# The tests don't check if create statements are correct, but only assert if the name inside is quoted.
# Names which are testes are extracted to separate variable at the beginning of each test.

def test_keyspaces_quoting(cql):
    # Keyspace name must match [a-zA-Z0-9_]+, so the only names which require quoting
    # are the ones with capital letters.
    name = "Quoted_KS"

    cql.execute(f"CREATE KEYSPACE \"{name}\" WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}")
    try:
        desc = cql.execute(f"DESC KEYSPACE \"{name}\"").one().create_statement

        # Check if name in create statement is quoted
        assert f"CREATE KEYSPACE \"{name}\" WITH" in desc
        assert f"CREATE KEYSPACE {name} WITH" not in desc

        # Check if name if keyspaces listing is quoted
        with new_cql(cql) as ncql:
            keyspaces_list = [(r.keyspace_name, r.name) for r in ncql.execute("DESC KEYSPACES")]

            assert (f"\"{name}\"", f"\"{name}\"") in keyspaces_list
            assert (f"\"{name}\"", name) not in keyspaces_list
            assert (name, f"\"{name}\"") not in keyspaces_list
    finally:
        cql.execute(f"DROP KEYSPACE \"{name}\"")

def test_table_quoting(cql):
    # Table name must match [a-zA-Z0-9_]+, so the only names which require quoting
    # are the ones with capital letters.
    ks_name = "Quoted_KS"
    name = "Quoted_TABLE"
    col_name = "!@#$%"

    cql.execute(f"CREATE KEYSPACE \"{ks_name}\" WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}")
    try:
        cql.execute(f"CREATE TABLE \"{ks_name}\".\"{name}\"(a int primary key, \"{col_name}\" int)")
        desc = cql.execute(f"DESC TABLE \"{ks_name}\".\"{name}\"").one().create_statement

        assert f"CREATE TABLE \"{ks_name}\".\"{name}\"" in desc
        assert f"CREATE TABLE {ks_name}.\"{name}\"" not in desc
        assert f"CREATE TABLE \"{ks_name}\".{name}" not in desc

        assert f"\"{col_name}\" int" in desc
        assert f"{col_name} int" not in desc

        cql.execute(f"ALTER TABLE \"{ks_name}\".\"{name}\" DROP \"!@#$%\"")
        cql.execute(f"ALTER TABLE \"{ks_name}\".\"{name}\" ADD \"!@#$%\" int")
        desc_alter = cql.execute(f"DESC TABLE \"{ks_name}\".\"{name}\" WITH INTERNALS").one().create_statement

        assert f"ALTER TABLE \"{ks_name}\".\"{name}\" DROP \"!@#$%\"" in desc_alter
        assert f"ALTER TABLE \"{ks_name}\".\"{name}\" ADD \"!@#$%\"" in desc_alter

        with new_cql(cql) as ncql:
            tables_list = [(r.keyspace_name, r.name) for r in ncql.execute("DESC TABLES")]

            assert (f"\"{ks_name}\"", f"\"{name}\"") in tables_list
            assert (f"\"{ks_name}\"", name) not in tables_list
            assert (ks_name, f"\"{name}\"") not in tables_list
    finally:
        cql.execute(f"DROP TABLE \"{ks_name}\".\"{name}\"")
        cql.execute(f"DROP KEYSPACE \"{ks_name}\"")

def test_type_quoting(cql):
    ks_name = "Quoted_KS"
    name = "udt_@@@"
    field_name = "field_!!!"

    cql.execute(f"CREATE KEYSPACE \"{ks_name}\" WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}")
    try:
        cql.execute(f"CREATE TYPE \"{ks_name}\".\"{name}\" (a int, \"{field_name}\" text)")
        desc = cql.execute(f"DESC TYPE \"{ks_name}\".\"{name}\"").one().create_statement

        assert f"CREATE TYPE \"{ks_name}\".\"{name}\"" in desc
        assert f"CREATE TYPE \"{ks_name}\".{name}" not in desc
        assert f"CREATE TYPE {ks_name}.\"{name}\"" not in desc
        # The following used to be buggy in Cassandra, but fixed in
        # CASSANDRA-17918 (in Cassandra 4.1.2):
        assert f"\"{field_name}\" text" in desc
        assert f"{field_name} text" not in desc

        with new_cql(cql) as ncql:
            types_list = [(r.keyspace_name, r.name) for r in ncql.execute("DESC TYPES")]

            assert (f"\"{ks_name}\"", f"\"{name}\"") in types_list
            assert (f"\"{ks_name}\"", name) not in types_list
            assert (ks_name, f"\"{name}\"") not in types_list
    finally:
        cql.execute(f"DROP TYPE \"{ks_name}\".\"{name}\"")
        cql.execute(f"DROP KEYSPACE \"{ks_name}\"")

def test_function_quoting(scylla_only, cql, test_keyspace):
    ks_name = "Quoted_KS"
    name = "!udf!"

    cql.execute(f"CREATE KEYSPACE \"{ks_name}\" WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}")
    try:
        cql.execute(f"""
            CREATE FUNCTION \"{ks_name}\".\"{name}\"(val int)
            RETURNS NULL ON NULL INPUT
            RETURNS int
            LANGUAGE lua
            AS $$ return val $$
        """)
        desc = cql.execute(f"DESC FUNCTION \"{ks_name}\".\"{name}\"").one().create_statement

        assert f"CREATE FUNCTION \"{ks_name}\".\"{name}\"" in desc
        assert f"CREATE FUNCTION \"{ks_name}\".{name}" not in desc
        assert f"CREATE FUNCTION {ks_name}.\"{name}\"" not in desc

        with new_cql(cql) as ncql:
            udf_list = [(r.keyspace_name, r.name) for r in ncql.execute("DESC FUNCTIONS")]

            assert (f"\"{ks_name}\"", f"\"{name}\"") in udf_list
            assert (f"\"{ks_name}\"", name) not in udf_list
            assert (ks_name, f"\"{name}\"") not in udf_list
    finally:
        cql.execute(f"DROP FUNCTION \"{ks_name}\".\"{name}\"")
        cql.execute(f"DROP KEYSPACE \"{ks_name}\"")

def test_aggregate_quoting(scylla_only, cql, test_keyspace):
    ks_name = "Quoted_KS"
    sfunc_name = "!udf!"
    name = "'uda'!"

    cql.execute(f"CREATE KEYSPACE \"{ks_name}\" WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}")
    try:
        cql.execute(f"""
            CREATE FUNCTION \"{ks_name}\".\"{sfunc_name}\"(val1 int, val2 int)
            RETURNS NULL ON NULL INPUT
            RETURNS int
            LANGUAGE lua
            AS $$ return val1 + val2 $$
        """)
        cql.execute(f"""
            CREATE AGGREGATE \"{ks_name}\".\"{name}\"(int)
            SFUNC \"{sfunc_name}\"
            STYPE int
        """)
        desc = cql.execute(f"DESC AGGREGATE \"{ks_name}\".\"{name}\"").one().create_statement

        assert f"CREATE AGGREGATE \"{ks_name}\".\"{name}\"" in desc
        assert f"SFUNC \"{sfunc_name}\"" in desc

        with new_cql(cql) as ncql:
            uda_list = [(r.keyspace_name, r.name) for r in ncql.execute("DESC AGGREGATES")]

            assert (f"\"{ks_name}\"", f"\"{name}\"") in uda_list
            assert (f"\"{ks_name}\"", name) not in uda_list
            assert (ks_name, f"\"{name}\"") not in uda_list
    finally:
        cql.execute(f"DROP AGGREGATE \"{ks_name}\".\"{name}\"")
        cql.execute(f"DROP FUNCTION \"{ks_name}\".\"{sfunc_name}\"")
        cql.execute(f"DROP KEYSPACE \"{ks_name}\"")

# Test if fields and options (column names, column types, comment) inside table's description are quoted properly
def test_table_options_quoting(cql, test_keyspace):
    type_name = f"some_udt; DROP KEYSPACE {test_keyspace}"
    column_name = "col''umn -- @quoting test!!"
    comment = "table''s comment test!\"; DESC TABLES --quoting test"
    comment_plain = "table's comment test!\"; DESC TABLES --quoting test" #without doubling "'" inside comment

    cql.execute(f"CREATE TYPE {test_keyspace}.\"{type_name}\" (a int)")
    try:
        with new_test_table(cql, test_keyspace, f"a int primary key, b list<frozen<\"{type_name}\">>, \"{column_name}\" int", 
            f"with comment = '{comment}'") as tbl:
            desc = cql.execute(f"DESC TABLE {tbl}").one().create_statement

            assert f"b list<frozen<\"{type_name}\">>" in desc
            assert f"\"{column_name}\" int" in desc
            assert f"comment = '{comment}'" in desc

            assert f"b list<frozen<{type_name}>>" not in desc
            assert f"{column_name} int" not in desc
            assert f"comment = \"{comment}\"" not in desc
            assert f"comment = {comment}" not in desc
            assert f"comment = '{comment_plain}'" not in desc
    finally:
        cql.execute(f"DROP TYPE {test_keyspace}.\"{type_name}\"")

# We need to hide cdc log tables (more precisely, their CREATE statemtns and/or names)
# but we are attaching `ALTER TABLE <cdc log table name> WITH <all table's properties>` to description of base table
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
@pytest.mark.parametrize("cdc_enablement_query",
                         ["ALTER TABLE {t} WITH cdc = {{'enabled': true}}",
                          "CREATE INDEX ON {t}(b) USING 'vector_index'"],
                         ids=["alter", "create_index"])
def test_hide_cdc_table(scylla_only, cql, test_keyspace, cdc_enablement_query):
    cdc_table_suffix = "_scylla_cdc_log"
    with new_test_table(cql, test_keyspace, "a int primary key, b vector<float, 3>") as t:
        t_name = t.split('.')[1]
        cdc_log_name = t_name + cdc_table_suffix

        # Enable CDC log
        cql.execute(cdc_enablement_query.format(t=t))

        # Check if the log table exists
        cdc_log_table_entry = cql.execute(f"SELECT * FROM system_schema.tables WHERE keyspace_name='{test_keyspace}' AND table_name='{cdc_log_name}'").all()
        assert len(cdc_log_table_entry) == 1

        # DESC TABLES
        desc_tables = cql.execute("DESC TABLES")
        assert cdc_log_name not in [r.name for r in desc_tables]

        # DESC KEYSPACE ks
        desc_keyspace = cql.execute(f"DESC KEYSPACE {test_keyspace}").all()
        for row in desc_keyspace:
            if row.name == cdc_log_name:
                assert f"ALTER TABLE {test_keyspace}.{cdc_log_name} WITH" in row.create_statement

        #  DESC SCHEMA
        desc_schema = cql.execute("DESC SCHEMA")
        for row in desc_schema:
            if row.name == cdc_log_name:
                assert f"ALTER TABLE {test_keyspace}.{cdc_log_name} WITH" in row.create_statement

        # Check base table description contains ALTER TABLE statement for cdc log table
        desc_base_table = cql.execute(f"DESC TABLE {t}").all()
        assert any(f"ALTER TABLE {test_keyspace}.{cdc_log_name} WITH" in row.create_statement for row in desc_base_table)

        # Drop current cdc base table and try to recreate it with describe output
        cql.execute(f"DROP TABLE {t}")
        for row in desc_keyspace[1:]: # [1:] because we want to skip first row (keyspace's CREATE STATEMENT)
            cql.execute(row.create_statement)

        # Check if base and log tables were recreated
        ks_tables = cql.execute(f"SELECT * FROM system_schema.tables WHERE keyspace_name='{test_keyspace}'").all()
        assert len(ks_tables) == 2
        for row in ks_tables:
            assert row.table_name == t_name or row.table_name == cdc_log_name

# Verify that the format of the result of `DESC TABLE` targeting a CDC log table
# has the expected format.
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
@pytest.mark.parametrize("cdc_enablement_query",
                         ["ALTER TABLE {t} WITH cdc = {{'enabled': true}}",
                          "CREATE INDEX ON {t}(v) USING 'vector_index'"],
                         ids=["alter", "create_index"])
def test_describe_cdc_log_table_format(scylla_only, cql, test_keyspace, cdc_enablement_query):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v vector<float, 3>") as table:
        log_table = f"{table}_scylla_cdc_log"
        _, log_table_name = log_table.split(".")

        # Enable CDC log
        cql.execute(cdc_enablement_query.format(t=table))

        [row] = cql.execute(f"DESC TABLE {log_table}")

        assert row.keyspace_name == test_keyspace
        assert row.type == "table"
        assert row.name == log_table_name

        create_statement = row.create_statement

        # The actual content of `row.create_statement` is tested separately in `test_describe_cdc_log_table_create_statement`.
        # We only want to confirm here that the statement is wrapped in CQL comment markers and that it informs
        # the user that they shouldn't attempt to restore a CDC log table with the returned statement.
        assert create_statement.startswith(CDC_LOG_TABLE_DESC_PREFIX)
        assert create_statement.endswith(CDC_LOG_TABLE_DESC_SUFFIX)

# Verify that the create statement returned by `DESC TABLE` targeting a CDC log table
# is correct and as expected.
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
@pytest.mark.parametrize("cdc_enablement_query",
                         ["ALTER TABLE {t} WITH cdc = {{'enabled': true}}",
                          "CREATE INDEX ON {t}(v) USING 'vector_index'"],
                         ids=["alter", "create_index"])
def test_describe_cdc_log_table_create_statement(scylla_only, cql, test_keyspace, cdc_enablement_query):
    def format_create_statement(stmt: str) -> str:
        stmt = " ".join(stmt.split("\n"))
        stmt = " ".join(stmt.split())
        stmt = stmt.strip()
        return stmt

    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v vector<float, 3>") as table:
        log_table = f"{table}_scylla_cdc_log"

        # Enable CDC log
        cql.execute(cdc_enablement_query.format(t=table))

        [row] = cql.execute(f"DESC TABLE {log_table}")
        create_statement = row.create_statement

        # We want to get rid of the CQL comment markers: "/*" and "*/", as well as the warning
        # for the user that informs them that they shouldn't restore a CDC log table using the
        # returned statement.
        create_statement = create_statement.removeprefix(CDC_LOG_TABLE_DESC_PREFIX)
        create_statement = create_statement.removesuffix(CDC_LOG_TABLE_DESC_SUFFIX)

        create_statement = format_create_statement(create_statement)

        expected = f"""\
            CREATE TABLE {log_table} (
                "cdc$stream_id" blob,
                "cdc$time" timeuuid,
                "cdc$batch_seq_no" int,
                "cdc$deleted_v" boolean,
                "cdc$end_of_batch" boolean,
                "cdc$operation" tinyint,
                "cdc$ttl" bigint,
                p int,
                v vector<float, 3>,
                PRIMARY KEY ("cdc$stream_id", "cdc$time", "cdc$batch_seq_no")
            ) WITH CLUSTERING ORDER BY ("cdc$time" ASC, "cdc$batch_seq_no" ASC)
                AND bloom_filter_fp_chance = 0.01
                AND caching = {{'enabled': 'false', 'keys': 'NONE', 'rows_per_partition': 'NONE'}}
                AND comment = 'CDC log for {table}'
                AND compaction = {{'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '60', 'compaction_window_unit': 'MINUTES', 'expired_sstable_check_frequency_seconds': '1800'}}
                AND compression = {{'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}}
                AND crc_check_chance = 1
                AND default_time_to_live = 0
                AND gc_grace_seconds = 0
                AND max_index_interval = 2048
                AND memtable_flush_period_in_ms = 0
                AND min_index_interval = 128
                AND speculative_retry = '99.0PERCENTILE';
            """
        expected = format_create_statement(expected)

        assert create_statement == expected

# Verify that the options of a CDC log table specified by the create statement
# returned by `DESC TABLE` reflect the reality and are present.
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #16317")]), "vnodes"],
                         indirect=True)
@pytest.mark.parametrize("cdc_enablement_query",
                         ["ALTER TABLE {t} WITH cdc = {{'enabled': true}}",
                          "CREATE INDEX ON {t}(v) USING 'vector_index'"],
                         ids=["alter", "create_index"])
def test_describe_cdc_log_table_opts(scylla_only, cql, test_keyspace, cdc_enablement_query):
    def test_config(altered_cdc_log_table_opt):
        with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v vector<float, 3>") as table:
            log_table = f"{table}_scylla_cdc_log"

            # Enable CDC log
            cql.execute(cdc_enablement_query.format(t=table))

            cql.execute(f"ALTER TABLE {log_table} WITH {altered_cdc_log_table_opt}")

            [row] = list(cql.execute(f"DESCRIBE TABLE {log_table} WITH INTERNALS"))
            create_statement = row.create_statement

            # We extract the options of the log table, e.g.
            #   ["bloom_filter_fp_chance = 0.01", "caching = {'enabled': 'false', 'keys': 'NONE', 'rows_per_partition': 'NONE'}", ...]
            # We want to get rid of unnecessary lines as well as the keywords "WITH" and "AND".
            # All of that for the convenience of comparing the strings later on.

            # We want to get rid of the CQL comment markers: "/*" and "*/", as well as the warning
            # for the user that informs them that they shouldn't restore a CDC log table using the
            # returned statement.
            create_statement = create_statement.removeprefix(CDC_LOG_TABLE_DESC_PREFIX)
            create_statement = create_statement.removesuffix(CDC_LOG_TABLE_DESC_SUFFIX)

            # We get rid of the trailing semicolon to not interfere with the last option.
            create_statement = create_statement.replace(";", "")

            # We rely on the assumption that `WITH CLUSTERING ORDER` is the first option.
            # That means that all relevant options for this tests start with "AND".
            # We discard the prefix and take all of those options here.
            opts = create_statement.split("AND")[1:]
            opts = [" ".join(opt.strip().split()) for opt in opts]

            assert altered_cdc_log_table_opt in opts

    test_config("bloom_filter_fp_chance = 0.5")
    test_config("caching = {'keys': 'NONE', 'rows_per_partition': 'NONE'}")
    test_config("comment = 'some custom comment haha'")
    test_config("compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '30', 'compaction_window_unit': 'HOURS', 'expired_sstable_check_frequency_seconds': '1200'}")
    test_config("compression = {'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}")

    # FIXME: Once scylladb/scylladb#2431 is resolved, change this to a custom value.
    test_config("crc_check_chance = 1")

    test_config("default_time_to_live = 11")
    test_config("gc_grace_seconds = 31")
    test_config("max_index_interval = 1234")
    test_config("memtable_flush_period_in_ms = 61234")
    test_config("min_index_interval = 17")
    test_config("speculative_retry = '17.0PERCENTILE'")
    test_config("tombstone_gc = {'mode': 'immediate', 'propagation_delay_in_seconds': '17'}")


### =========================== UTILITY FUNCTIONS =============================

# Create random body of UDT using all UDTs from `udts`.
# The functions tries to create complex field types,
# by mixing lists, tuples, sets, etc...
def create_udt_body(udts):
    two_udts_type_probability = 0.3
    nest_probability = 0.1

    one_udt_type = lambda u: random.choice([
        f"frozen<{u}>",
        f"frozen<list<{u}>>",
        f"list<frozen<{u}>>",
        f"frozen<set<{u}>>",
        f"set<frozen<{u}>>",
        f"tuple<frozen<{u}>, int>",
        f"frozen<tuple<{u}, int>>",
        f"map<frozen<{u}>, int>",
        f"map<int, frozen<{u}>>",
        f"frozen<map<{u}, int>>",
        f"frozen<map<int, {u}>>",
    ])
    two_udts_type = lambda u, w: random.choice([
        f"frozen<tuple<{u}, {w}>>",
        f"tuple<frozen<{u}>, frozen<{w}>>",
        f"frozen<map<{u}, {w}>>",
        f"map<frozen<{u}>, frozen<{w}>>"
    ])
    
    fields = []
    field_idx = 1
    while udts:
        # Select to create one_udt_type or two_udts_type
        two_udts = False
        if len(udts) > 1 and random.random() < two_udts_type_probability:
            two_udts = True
        
        field_type = None
        if two_udts: # Create a type based on two UDTs (two_udts_type)
            u1 = udts.pop()
            u2 = udts.pop()
            field_type = two_udts_type(u1, u2)
        else:        # Create a type based on one UDT (one_udt_type)
            field_type = one_udt_type(udts.pop())
        
        # Maybe create nested type
        if random.random() < nest_probability:
            field_type = one_udt_type(field_type)

        fields.append(f"f_{field_idx} {field_type}")
        field_idx = field_idx + 1
    
    fields_joined = ", ".join(fields)
    return f"({fields_joined})"

def get_name(name_with_ks):
    return name_with_ks.split(".")[1]

# Takes an identifier and quotes it when needed.
# This is a copy of `maybe_quote` from cql3's utils.
# (see 'cql3/cql3_type.cc:430')
# NOTE: This function is missing a functionality of running cql's grammar function
def maybe_quote(ident):
    if not len(ident):
        return "\"\""

    need_quotes = False
    num_quotes = 0
    for c in ident:
        if not (('a' <= c and c <= 'z') or ('0' <= c and c <= '9') or c =='_'):
            need_quotes = True
            num_quotes += (c == '"')

    if not need_quotes:
        # TODO: Here is missing part from C++ implementation (see cql3/cql3_type.cc:448)
        # Here function from Cql.g (cident) is used to ensure the ident doesn't need quoting.
        return ident
    
    if num_quotes == 0:
        return '"' + ident + '"'
    
    return '"' + ident.replace('"', "\"\"") + '"'

### ---------------------------------------------------------------------------

# Configuration parameters for randomly created elements (keyspaces/tables/types)

# If depth reaches 0, random type will return native type
max_depth = 2
# How many types can be in collection/tuple/UDT
max_types = 5
# How many rows can be in partition key
max_pk = 3
# How many rows can be in clustering key
max_ck = 5
# How many regular rows can be in table
max_regular = 4
# How many static rows can be in table
max_static = 3
# Chance of counter table instead of normal one
counter_table_chance = 0.1

# A utility function for obtaining random native type
def get_random_native_type():
    types = ["text", "bigint", "boolean", "decimal", "double", "float", "int"]
    return random.choice(types)

# A utility function for obtaining random collection type.
# `udts` argument represents user-defined types that can be used for
# creating the collection type. If `frozen` is true, then the function will
# return `frozen<collection_type>`
def get_random_collection_type(cql, keyspace, depth, udts=[], frozen=False):
    types = ["set", "list", "map"]
    collection_type = random.choice(types)

    if collection_type == "set":
        return_type = f"set<{get_random_type(cql, keyspace, depth-1, udts, True)}>"
    elif collection_type == "list":
        return_type = f"list<{get_random_type(cql, keyspace, depth-1, udts, True)}>"
    elif collection_type == "map":
        return_type = f"map<{get_random_type(cql, keyspace, depth-1, udts, True)}, {get_random_type(cql, keyspace, depth-1, udts, True)}>"
    return f"frozen<{return_type}>" if frozen else return_type

# A utility function for obtaining random tuple type.
# `udts` argument represents user-defined types that can be used for
# creating the tuple type. If `frozen` is true, then the function will
# return `frozen<tuple<some_fields>>`
def get_random_tuple_type(cql, keyspace, depth, udts=[], frozen=False):
    n = random.randrange(1, max_types+1)
    types = [get_random_type(cql, keyspace, depth-1, udts, False) for _ in range(n)]
    fields = ", ".join(types)
    return_type = f"tuple<{fields}>"

    return f"frozen<{return_type}>" if frozen else return_type

# A utility function for obtaining random type. The function can return
# native types, collections, tuples and UDTs. `udts` argument represents
# user-defined types that can be returned or used for creation of other types
# If `frozen` is true, then the function will return `frozen<some_type>`
def get_random_type(cql, keyspace, depth, udts=[], frozen=False):
    types = ["native", "collection", "tuple"]
    if len(udts) != 0:
        types.append("UDT")
    type_type = random.choice(types)

    if type_type == "native" or depth <= 0: # native type
        return get_random_native_type()
    elif type_type == "collection":
        return get_random_collection_type(cql, keyspace, depth, udts, frozen)
    elif type_type == "tuple":
        return get_random_tuple_type(cql, keyspace, depth, udts, frozen)
    elif type_type == "UDT":
        udt = random.choice(udts)
        return f"frozen<{udt}>" if frozen else udt

def new_random_keyspace(cql):
    strategies = ["SimpleStrategy", "NetworkTopologyStrategy"]
    writes = ["true", "false"]

    options = {}
    options["class"] = random.choice(strategies)
    options["replication_factor"] = random.randrange(1, 6)
    options_str = ", ".join([f"'{k}': '{v}'" for (k, v) in options.items()])
    extra = ""
    if options["class"] == "NetworkTopologyStrategy" and options["replication_factor"] != 1:
        extra = " and tablets = { 'enabled': false }"

    write = random.choice(writes)
    return new_test_keyspace(cql, f"with replication = {{{options_str}}} and durable_writes = {write}{extra}")

# A utility function for creating random table. `udts` argument represents
# UDTs that can be used to create the table. The function uses `new_test_table`
# from util.py, so it can be used in a "with", as:
#   with new_random_table(cql, test_keyspace) as table:
def new_random_table(cql, keyspace, udts=[], tablet_options={}):
    pk_n = random.randrange(1, max_pk)
    ck_n = random.randrange(max_ck)
    regular_n = random.randrange(1, max_regular)
    static_n = random.randrange(max_static if ck_n > 0 else 1)

    pk = [(unique_name(), get_random_type(cql, keyspace, max_depth, udts, True)) for _ in range(pk_n)]
    ck = [(unique_name(), get_random_type(cql, keyspace, max_depth, udts, True)) for _ in range(ck_n)]
    pk_fields = ", ".join([f"{n} {t}" for (n, t) in pk])
    ck_fields = ", ".join([f"{n} {t}" for (n, t) in ck])

    partition_key = ", ".join([n for (n, _) in pk])
    clustering_fields = ", ".join([n for (n, _) in ck])
    clustering_key = f", {clustering_fields}" if ck_n > 0 else ""
    primary_def = f"PRIMARY KEY(({partition_key}){clustering_key})"


    schema = ""
    extras = {}
    if random.random() <= counter_table_chance:
        counter_name = unique_name()
        schema = f"{pk_fields}, {ck_fields}, {counter_name} counter, {primary_def}"
    else:
        regular = [(unique_name(), get_random_type(cql, keyspace, max_depth, udts)) for _ in range(regular_n)]
        static = [(unique_name(), get_random_type(cql, keyspace, max_depth, udts)) for _ in range(static_n)]
        regular_fields = ", ".join([f"{n} {t}" for (n, t) in regular])
        static_fields = ", ".join([f"{n} {t} static" for (n, t) in static])
        schema = f"{pk_fields}, {ck_fields}, {regular_fields}, {static_fields}, {primary_def}"
        
        extras["default_time_to_live"] = random.randrange(1000000)


    caching_keys = ["ALL", "NONE"]
    caching_rows = ["ALL", "NONE", random.randrange(1000000)]
    caching_k = random.choice(caching_keys)
    caching_r = random.choice(caching_rows)
    extras["caching"] = f"{{'keys':'{caching_k}', 'rows_per_partition':'{caching_r}'}}"

    compactions = ["SizeTieredCompactionStrategy", "TimeWindowCompactionStrategy", "LeveledCompactionStrategy"]
    extras["compaction"] = f"{{'class': '{random.choice(compactions)}'}}"

    speculative_retries = ["ALWAYS", "NONE", "99.0PERCENTILE", "50.00ms"]
    extras["speculative_retry"] = f"'{random.choice(speculative_retries)}'"

    compressions = ["LZ4Compressor", "SnappyCompressor", "DeflateCompressor"]
    extras["compression"] = f"{{'sstable_compression': '{random.choice(compressions)}'}}"

    # see the last element of `probs` defined by scylladb/utils/bloom_calculation.cc,
    # the minimum false positive rate supported by the bloom filter is determined by
    # prob's element with the greatest number of buckets. also, because random.random()
    # could return 0.0, not to mention round() could round a small enough float to 0,
    # let's add an epislon to ensure that the randomly picked chance is not equal to the
    # minimum.
    min_supported_bloom_filter_fp_chance = 6.71e-5 + 1e-5
    extras["bloom_filter_fp_chance"] = max(min_supported_bloom_filter_fp_chance, round(random.random(), 5))
    extras["crc_check_chance"] = round(random.random(), 5)
    extras["gc_grace_seconds"] = random.randrange(100000, 1000000)
    min_idx_interval = random.randrange(100, 1000)
    extras["min_index_interval"] = min_idx_interval
    extras["max_index_interval"] = random.randrange(min_idx_interval, 10000)

    memtable_flush_period = [0, random.randrange(60000, 200000)]
    extras["memtable_flush_period_in_ms"] = random.choice(memtable_flush_period)

    if is_scylla(cql):
        # Extra properties which ScyllaDB supports but Cassandra doesn't
        extras["paxos_grace_seconds"] = random.randrange(1000, 100000)
        extras["tombstone_gc"] = f"{{'mode': 'timeout', 'propagation_delay_in_seconds': '{random.randrange(100, 100000)}'}}"
        if tablet_options:
            extras["tablets"] = str(tablet_options)

    extra_options = [f"{k} = {v}" for (k, v) in extras.items()]
    extra_str = " AND ".join(extra_options)
    extra = f" WITH {extra_str}"
    return new_test_table(cql, keyspace, schema, extra)

# A utility function for creating random UDT. `udts` argument represents
# other UDTs that can be used to create the new one. The function uses `new_type`
# from util.py, so it can be used in a "with", as:
#   with new_random_type(cql, test_keyspace) as udt:
def new_random_type(cql, keyspace, udts=[]):
    n = random.randrange(1, max_types+1)
    types = [get_random_type(cql, keyspace, max_depth, udts, True) for _ in range(n)]
    fields = ", ".join([f"{unique_name()} {t}" for t in types])
    
    return new_type(cql, keyspace, f"({fields})")

### ===========================================================================

################################################################################
# ............................................................................ #
# ------------------------------- DESCRIPTION -------------------------------- #
# ............................................................................ #
# ============================================================================ #
#                                                                              #
# The tests below correspond to the task `scylladb/scylladb#18750`:            #
#     "auth on raft: safe backup and restore"                                  #
#                                                                              #
# We want to test the following features related to the issue:                 #
#                                                                              #
# 1. Creating roles when providing `HASHED PASSWORD`,                          #
# 2. The behavior of `DESC SCHEMA WITH INTERNALS (AND PASSWORDS)` and          #
#    the correctness of statements that it produces and which are related      #
#    to the referenced issue: auth and service levels.                         #
#                                                                              #
# To do that, we use the following pattern for most of the cases we should     #
# cover in this file:                                                          #
#                                                                              #
# 1. Format of the returned result by the query:                               #
#    - Are all of the values in the columns present and correct?               #
# 2. Formatting of identifiers with quotation marks.                           #
# 3. Formatting of identifiers with uppercase characters.                      #
# 4. Formatting of identifiers with unicode characters.                        #
# 5. Test(s) verifying that all of the cases are handled and `DESC SCHEMA`     #
#    prints them properly.                                                     #
#                                                                              #
################################################################################

################################################################################
# ............................................................................ #
# ---------------------------------- NOTES ----------------------------------- #
# ............................................................................ #
# ============================================================================ #
#                                                                              #
# 1. Every create statement corresponding to auth and service levels returned  #
#    by                                                                        #
#        `DESC SCHEMA WITH INTERNALS (AND PASSWORDS)`                          #
#    is termined with a semicolon.                                             #
#                                                                              #
# 2. `CREATE ROLE` statements always preserve the following order of options:  #
#        `HASHED PASSWORD`, `LOGIN`, `SUPERUSER`                               #
#    Aside from `HASHED PASSWORD`, which only appears when executing           #
#        `DESC SCHEMA WITH INTERNALS AND PASSWORDS`                            #
#    the parameters are always present.                                        #
#                                                                              #
# 3. *ALL* create statements returned by                                       #
#        `DESC SCHEMA WITH INTERNALS (AND PASSWORDS)`                          #
#    and related to AUTH/service levels use capital letters for CQL syntax.    #
#                                                                              #
# 4. If an identifier needs to be quoted, e.g. because it contains whitespace  #
#    characters, it will be wrapped with double quoatation marks.              #
#    There are three exceptions to that rule:                                  #
#                                                                              #
#    (i)   the `WORKLOAD_TYPE` option when creating a service level,           #
#    (ii)  the `PASSWORD` option when creating a role,                         #
#    (iii) the `HASHED PASSWORD` option when creating a role.                  #
#                                                                              #
#    The exceptions are enforced by the CQL grammar used in Scylla.            #
#                                                                              #
# 5. Statements for creating service levels always have options listed in the  #
#    following order:                                                          #
#        `TIMEOUT`, `WORKLOAD_TYPE`, `SHARES`                                  #
#    If an option is unnecessary (e.g. there's no timeout or the workload      #
#    type is unspecified -- default!), it's not present in the create          #
#    statement of the result of `DESC SCHEMA WITH INTERNALS`.                  #
#                                                                              #
# 6. The `TIMEOUT` option in `CREATE SERVICE LEVEL` statements returned        #
#    by `DESC SCHEMA WITH INTERNALS` always uses milliseconds as its           #
#    resolution.                                                               #
#                                                                              #
# 7. We create test keyspaces manually here. The rationale for that is         #
#    the fact that the creator of a resource automatically obtains all         #
#    permissions on the resource. Since in these tests we verify permission    #
#    grants, we want to have full control over who creates what.               #
#                                                                              #
################################################################################

def sanitize_identifier(identifier: str, quotation_mark: str) -> str:
    doubled_quotation_mark = quotation_mark + quotation_mark
    return identifier.replace(quotation_mark, doubled_quotation_mark)

def sanitize_password(password: str) -> str:
    return sanitize_identifier(password, "'")

def make_identifier(identifier: str, quotation_mark: str) -> str:
    return quotation_mark + sanitize_identifier(identifier, quotation_mark) + quotation_mark

###

KS_AND_TABLE_PERMISSIONS = ["CREATE", "ALTER", "DROP", "MODIFY", "SELECT", "AUTHORIZE"]

###

class AuthSLContext:
    def __init__(self, cql, ks=None):
        self.cql = cql
        self.ks = ks

    def __enter__(self):
        if self.ks:
            self.cql.execute(f"CREATE KEYSPACE {self.ks} WITH REPLICATION = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.ks:
            self.cql.execute(f"DROP KEYSPACE {self.ks}")

        roles_iter = self.cql.execute("LIST ROLES")
        roles_iter = filter(lambda record: record.role != DEFAULT_SUPERUSER, roles_iter)
        roles = [record.role for record in roles_iter]
        for role in roles:
            self.cql.execute(f"DROP ROLE {make_identifier(role, quotation_mark='"')}")

        if is_scylla(self.cql):
            service_levels_iter = self.cql.execute("LIST ALL SERVICE LEVELS")
            service_levels = [record.service_level for record in service_levels_iter]
            for sl in service_levels:
                self.cql.execute(f"DROP SERVICE LEVEL {make_identifier(sl, quotation_mark='"')}")

class ServiceLevel:
    default_shares_value = 1000
    
    def __init__(self, name: str, timeout: int|None = None, wl_type: str|None = None, shares: int|None = None):
        self.name = name
        self.timeout = timeout
        self.wl_type = wl_type
        self.shares = shares

    # replace_default_shares - Scylla automatically assigns default value of shares it they are not
    #                          specified. Set this argument to True to include the default shares in create statement
    #                          to match describe result.
    def get_create_stmt(self, replace_default_shares = False) -> str:
        # Note: `CREATE SERVICE LEVEL` statements returned by `DESC SCHEMA WITH INTERNALS` always uses
        #       `std::chrono::milliseconds` as its resolution. For that reason, we use milliseconds in
        #       create statements too so that they're easy to compare with Scylla's output.
        timeout = None if not self.timeout else f"TIMEOUT = {self.timeout}ms"
        wl_type = None if not self.wl_type else f"WORKLOAD_TYPE = '{self.wl_type}'"
        shares = None if not self.shares else f"SHARES = {self.shares}"
        if shares is None and replace_default_shares:
            shares = f"SHARES = {self.default_shares_value}"
        
        opts = [opt for opt in [timeout, wl_type, shares] if opt is not None]
        if opts:
            return f"CREATE SERVICE LEVEL {self.name} WITH {" AND ".join(opts)};"

        return f"CREATE SERVICE LEVEL {self.name};"

###

def test_create_role_with_hashed_password(cql):
    """
    Verify that creating a role with a hashed password works correctly, i.e. that the hashed password
    present in `system.roles` is the same as the one we provide.
    """

    with AuthSLContext(cql):
        role = "andrew"
        # This could be anything, but we stick to the jBCrypt format to be compliant with Cassandra's policy
        # (it only accepts hashed passwords in that format). We don't use characters that won't be generated
        # by Scylla, i.e.:
        #    `:`, `;`, `*`, `!`, and `\`,
        # but in practice it should accept them too.
        hashed_password = "$2a$10$JSJEMFm6GeaW9XxT5JIheuEtPvat6i7uKbnTcxX3c1wshIIsGyUtG"
        cql.execute(f"CREATE ROLE {role} WITH HASHED PASSWORD = '{sanitize_password(hashed_password)}'")

        table = "system.roles" if is_scylla(cql) else "system_auth.roles"
        [result] = cql.execute(f"SELECT salted_hash FROM {table} WHERE role = '{role}'")
        assert hashed_password == result.salted_hash


def test_create_role_with_hashed_password_authorization(cql):
    """
    Verify that roles that aren't superusers cannot perform `CREATE ROLE WITH HASHED PASSWORD`.
    """

    with AuthSLContext(cql):
        def try_create_role_with_hashed_password(role):
            with new_session(cql, role) as ncql:
                with pytest.raises(Unauthorized):
                    ncql.execute("CREATE ROLE some_unused_name WITH HASHED PASSWORD = '$2a$10$JSJEMFm6GeaW9XxT5JIheuEtPvat6i7uKbnTcxX3c1wshIIsGyUtG'")

        # List of form (role name, list of permission grants to the role)
        r1 = "andrew"
        r2 = "jane"
        r3 = "bob"

        for r in [r1, r2]:
            cql.execute(f"CREATE ROLE {r} WITH LOGIN = true AND PASSWORD = '{r}'")
        cql.execute(f"CREATE ROLE {r3} WITH LOGIN = true AND PASSWORD = '{r3}' AND SUPERUSER = true")

        # This also grants access to system tables.
        cql.execute(f"GRANT ALL ON ALL KEYSPACES TO {r2}")

        try_create_role_with_hashed_password(r1)
        try_create_role_with_hashed_password(r2)

        with new_session(cql, r3) as ncql:
            ncql.execute("CREATE ROLE some_unused_name WITH HASHED PASSWORD = '$2a$10$JSJEMFm6GeaW9XxT5JIheuEtPvat6i7uKbnTcxX3c1wshIIsGyUtG'")

###

# Marked as `scylla_only` because we verify `DESCRIBE SCHEMA WITH INTERNALS AND PASSWORDS`,
# which is not present on Cassandra.
def test_desc_authorization(cql, scylla_only):
    """
    Verify that Scylla rejects performing `DESC SCHEMA WITH INTERNALS AND PASSWORDS` if the user
    sending the request is not a superuser, even if they have all permissions to relevant system tables.
    """

    with AuthSLContext(cql):
        def try_describe_with_passwords(role):
            with new_session(cql, role) as ncql:
                with pytest.raises(Unauthorized):
                    ncql.execute("DESCRIBE SCHEMA WITH INTERNALS AND PASSWORDS")

        # List of form (role name, list of permission grants to the role)
        r1 = "andrew"
        r2 = "jane"
        
        with new_user(cql, r1), new_user(cql, r2):
            # This also grants access to system tables.
            cql.execute(f"GRANT ALL ON ALL KEYSPACES TO {r2}")
            
            try_describe_with_passwords(r1)
            try_describe_with_passwords(r2)

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about auth. That's not the case in Cassandra.
def test_desc_roles_format(cql, scylla_only):
    """
    Verify that the format of the output of `DESC SCHEMA WITH INTERNALS` corresponding to
    creating roles is of the expected form.
    """

    with AuthSLContext(cql):
        role_name = "andrew"
        stmt = f"CREATE ROLE {role_name} WITH LOGIN = false AND SUPERUSER = false;"
        cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_roles(desc_iter)
        desc_iter = filter_non_default_user(desc_iter)
        [result] = list(desc_iter)

        assert result.keyspace_name == None
        assert result.type == "role"
        assert result.name == role_name
        assert result.create_statement == stmt

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about auth. That's not the case in Cassandra.
def test_desc_roles_quotation_marks(cql, scylla_only):
    """
    Verify that statements corresponding to creating roles correctly format quotation marks.
    """

    with AuthSLContext(cql):
        andrew_raw = "andrew \" 'the great'"
        jane_raw = "jane ' \"the wise\""

        andrew_hashed_password_raw = "my \" 'hashed password'"
        jane_hashed_password_raw = "my ' \"other hashed password\""

        andrew_single_quote = make_identifier(andrew_raw, quotation_mark="'")
        andrew_double_quote = make_identifier(andrew_raw, quotation_mark='"')
        jane_double_quote = make_identifier(jane_raw, quotation_mark='"')

        andrew_hashed_password = make_identifier(andrew_hashed_password_raw, quotation_mark="'")
        jane_hashed_password = make_identifier(jane_hashed_password_raw, quotation_mark="'")

        cql.execute(f"CREATE ROLE {andrew_single_quote} WITH HASHED PASSWORD = {andrew_hashed_password}")
        cql.execute(f"CREATE ROLE {jane_double_quote} WITH HASHED PASSWORD = {jane_hashed_password}")

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS AND PASSWORDS")
        desc_iter = filter_roles(desc_iter)
        desc_iter = filter_non_default_user(desc_iter)

        desc_elements = list(desc_iter)
        element_names_iter = extract_names(desc_elements)

        assert set(element_names_iter) == {andrew_double_quote, jane_double_quote}

        desc_iter = extract_create_statements(desc_elements)

        expected_result = {
            f"CREATE ROLE {andrew_double_quote} WITH HASHED PASSWORD = {andrew_hashed_password} AND LOGIN = false AND SUPERUSER = false;",
            f"CREATE ROLE {jane_double_quote} WITH HASHED PASSWORD = {jane_hashed_password} AND LOGIN = false AND SUPERUSER = false;"
        }

        assert set(desc_iter) == expected_result

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about auth. That's not the case in Cassandra.
def test_desc_roles_uppercase(cql, scylla_only):
    """
    Verify that statements corresponding to creating roles correctly format uppercase characters,
    i.e. identifiers like that should be wrapped in quotation marks.
    """

    with AuthSLContext(cql):
        role = '"myRole"'
        cql.execute(f"CREATE ROLE {role}")

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_roles(desc_iter)
        desc_iter = filter_non_default_user(desc_iter)

        desc_elements = list(desc_iter)

        role_iter = map(lambda row: row.name, desc_elements)
        assert list(role_iter) == [role]

        desc_iter = extract_create_statements(desc_elements)

        assert list(desc_iter) == [f"CREATE ROLE {role} WITH LOGIN = false AND SUPERUSER = false;"]

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about auth. That's not the case in Cassandra.
def test_desc_roles_unicode(cql, scylla_only):
    """
    Verify that statements to creating roles can contain unicode characters.
    """

    with AuthSLContext(cql):
        role = '"ユーザー"'
        cql.execute(f"CREATE ROLE {role}")

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_roles(desc_iter)
        desc_iter = filter_non_default_user(desc_iter)

        desc_elements = list(desc_iter)

        role_iter = map(lambda row: row.name, desc_elements)
        assert list(role_iter) == [role]

        desc_iter = extract_create_statements(desc_elements)

        assert list(desc_iter) == [f"CREATE ROLE {role} WITH LOGIN = false AND SUPERUSER = false;"]

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about auth. That's not the case in Cassandra.
def test_desc_roles(cql, scylla_only):
    """
    Verify that the output of `DESC SCHEMA WITH INTERNALS` corresponding to creating roles
    is as expected for various different cases.
    """

    with AuthSLContext(cql):
        roles = ["andrew", "alice", '"m!ch@el j4cks0n"']
        roles_password = ["fred", "julie", '"hi, I like sunsets"']
        roles_can_login = ["bob", "jane", '"very weird nam3 Fu!! of character$"']
        roles_superuser = ["gustang", "devon", '"my h4ppy c0mp@n!0n!"']
        roles_can_login_and_superuser = ["peter", "susan", '"the k!ng 0f 3vryth!ng th4t 3x!$t$ @"']

        for idx, role in enumerate(roles):
            cql.execute(f"CREATE ROLE {role}")
        for idx, role in enumerate(roles_password):
            cql.execute(f"CREATE ROLE {role} WITH PASSWORD = 'my_password{idx}'")
        for role in roles_can_login:
            cql.execute(f"CREATE ROLE {role} WITH LOGIN = true")
        for role in roles_superuser:
            cql.execute(f"CREATE ROLE {role} WITH SUPERUSER = true")
        for role in roles_can_login_and_superuser:
            cql.execute(f"CREATE ROLE {role} WITH SUPERUSER = true AND LOGIN = true")

        create_role_stmts = [
            [f"CREATE ROLE {role} WITH LOGIN = false AND SUPERUSER = false;" for role in roles],
            [f"CREATE ROLE {role} WITH LOGIN = false AND SUPERUSER = false;" for role in roles_password],
            [f"CREATE ROLE {role} WITH LOGIN = true AND SUPERUSER = false;" for role in roles_can_login],
            [f"CREATE ROLE {role} WITH LOGIN = false AND SUPERUSER = true;" for role in roles_superuser],
            [f"CREATE ROLE {role} WITH LOGIN = true AND SUPERUSER = true;" for role in roles_can_login_and_superuser],
            [f"CREATE ROLE IF NOT EXISTS {DEFAULT_SUPERUSER} WITH LOGIN = true AND SUPERUSER = true;"]
        ]
        # Flatten the list of lists to a list.
        create_role_stmts = sum(create_role_stmts, [])
        create_role_stmts = set(create_role_stmts)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_roles(desc_iter)
        desc_create_role_stmts = set(extract_create_statements(desc_iter))

        assert create_role_stmts == desc_create_role_stmts

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about auth. That's not the case in Cassandra.
def test_desc_roles_with_passwords(cql, scylla_only):
    """
    Verify that the output of `DESC SCHEMA WITH INTERNALS AND PASSWORDS` corresponding to creating roles
    is as expected for various different cases.
    """

    with AuthSLContext(cql):
        role_without_pass = "bob"
        role_with_pass = "alice"

        create_stmt_without_pass = f"CREATE ROLE {role_without_pass} WITH LOGIN = false AND SUPERUSER = false;"
        create_stmt_with_pass = f"CREATE ROLE {role_with_pass} WITH PASSWORD = 'some_password'"

        cql.execute(create_stmt_without_pass)
        cql.execute(create_stmt_with_pass)

        [salted_hash_result] = cql.execute(f"SELECT salted_hash FROM system.roles WHERE role = '{role_with_pass}'")
        hashed_password = salted_hash_result.salted_hash
        create_stmt_with_hashed_password = f"CREATE ROLE {role_with_pass} WITH HASHED PASSWORD = '{sanitize_password(hashed_password)}' AND LOGIN = false AND SUPERUSER = false;"

        stmts = [create_stmt_without_pass, create_stmt_with_hashed_password]

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS AND PASSWORDS")
        desc_iter = filter_roles(desc_iter)
        desc_iter = filter_non_default_user(desc_iter)
        desc_iter = extract_create_statements(desc_iter)

        assert set(stmts) == set(desc_iter)

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about auth. That's not the case in Cassandra.
def test_desc_role_grants_format(cql, scylla_only):
    """
    Verify that the format of the output of `DESC SCHEMA WITH INTERNALS` corresponding to
    granting roles is of the expected form.
    """

    with AuthSLContext(cql):
        [r1, r2] = ["andrew", "jane"]

        cql.execute(f"CREATE ROLE {r1}")
        cql.execute(f"CREATE ROLE {r2}")

        stmt = f"GRANT {r1} TO {r2};"
        cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_roles(desc_iter)

        [result] = list(desc_iter)

        assert result.keyspace_name == None
        assert result.type == "grant_role"
        assert result.name == r1
        assert result.create_statement == stmt

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about auth. That's not the case in Cassandra.
def test_desc_role_grants_quotation_marks(cql, scylla_only):
    """
    Verify that statements corresponding to granting roles correctly format quotation marks.
    """

    with AuthSLContext(cql):
        andrew_raw = "andrew \" 'the great'"
        jane_raw = "jane ' \"the wise\""

        andrew_single_quote = make_identifier(andrew_raw, quotation_mark="'")
        andrew_double_quote = make_identifier(andrew_raw, quotation_mark='"')
        jane_double_quote = make_identifier(jane_raw, quotation_mark='"')

        cql.execute(f"CREATE ROLE {andrew_single_quote}")
        cql.execute(f"CREATE ROLE {jane_double_quote}")

        cql.execute(f"GRANT {andrew_single_quote} TO {jane_double_quote}")

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_roles(desc_iter)

        desc_elements = [*desc_iter]
        element_names_iter = extract_names(desc_elements)

        assert set(element_names_iter) == {andrew_double_quote}

        desc_iter = extract_create_statements(desc_elements)

        expected_result = f"GRANT {andrew_double_quote} TO {jane_double_quote};"
        assert [expected_result] == list(desc_iter)

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about auth. That's not the case in Cassandra.
def test_desc_role_grants_uppercase(cql, scylla_only):
    """
    Verify that statements corresponding to granting roles correctly format uppercase characters,
    i.e. identifiers like that should be wrapped in quotation marks.
    """

    with AuthSLContext(cql):
        r1 = '"myRole"'
        r2 = '"otherRole"'

        cql.execute(f"CREATE ROLE {r1}")
        cql.execute(f"CREATE ROLE {r2}")
        cql.execute(f"GRANT {r1} TO {r2}")

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_roles(desc_iter)

        desc_elements = list(desc_iter)

        role_iter = map(lambda row: row.name, desc_elements)
        assert list(role_iter) == [r1]

        desc_iter = extract_create_statements(desc_elements)

        assert list(desc_iter) == [f"GRANT {r1} TO {r2};"]

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about auth. That's not the case in Cassandra.
def test_desc_role_grants_unicode(cql, scylla_only):
    """
    Verify that statements corresponding to granting roles correctly format unicode characters,
    i.e. identifiers like that should be wrapped in quotation marks.
    """

    with AuthSLContext(cql):
        r1 = '"ユーザー"'
        r2 = '"私の猫"'

        cql.execute(f"CREATE ROLE {r1}")
        cql.execute(f"CREATE ROLE {r2}")
        cql.execute(f"GRANT {r1} TO {r2}")

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_roles(desc_iter)

        desc_elements = list(desc_iter)

        role_iter = map(lambda row: row.name, desc_elements)
        assert list(role_iter) == [r1]

        desc_iter = extract_create_statements(desc_elements)

        assert list(desc_iter) == [f"GRANT {r1} TO {r2};"]

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about auth. That's not the case in Cassandra.
def test_desc_role_grants(cql, scylla_only):
    """
    Verify that the output of `DESC SCHEMA WITH INTERNALS` corresponding to granting roles
    is as expected for various different cases.
    """

    with AuthSLContext(cql):
        [r1, r2, r3, r4] = ["andrew", '"b0b, my f@vor!t3 fr!3nd :)"', "jessica", "kate"]
        # List whose each element is a pair of form:
        #     (role, list of granted roles)
        roles = [
            (r1, []),
            (r2, [r1]),
            (r3, [r2]),
            (r4, [r1, r3])
        ]

        for role, _ in roles:
            cql.execute(f"CREATE ROLE {role}")
        for role, grants in roles:
            for grant in grants:
                cql.execute(f"GRANT {grant} TO {role}")

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_roles(desc_iter)
        desc_grants = list(extract_create_statements(desc_iter))

        expected_grants = [[f"GRANT {grant} TO {role};" for grant in grants] for role, grants in roles]
        # Flatten the list of lists to a list.
        expected_grants = sum(expected_grants, [])

        assert set(expected_grants) == set(desc_grants)

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about auth. That's not the case in Cassandra.
def test_desc_grant_permission_format(cql, scylla_only):
    """
    Verify that the format of the output of `DESC SCHEMA WITH INTERNALS` corresponding to
    granting permissions is of the expected form.
    """

    with AuthSLContext(cql):
        role_name = "kate"
        cql.execute(f"CREATE ROLE {role_name}")

        stmt = f"GRANT SELECT ON ALL KEYSPACES TO {role_name};"
        cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_permissions(desc_iter)
        # We need to fliter out the default superuser because when it creates a role,
        # it automatically obtains all of permissions to manipulate it.
        desc_iter = filter_non_default_user(desc_iter)

        [result] = list(desc_iter)

        assert result.keyspace_name == None
        assert result.type == "grant_permission"
        assert result.name == role_name
        assert result.create_statement == stmt

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about auth. That's not the case in Cassandra.
def test_desc_grant_permission_quotation_marks(cql, scylla_only):
    """
    Verify that statements corresponding to granting permissions correctly format quotation marks.
    """

    with AuthSLContext(cql):
        andrew_raw = "andrew \" 'the great'"
        jane_raw = "jane ' \"the wise\""

        andrew_single_quote = make_identifier(andrew_raw, quotation_mark="'")
        andrew_double_quote = make_identifier(andrew_raw, quotation_mark='"')
        jane_double_quote = make_identifier(jane_raw, quotation_mark='"')

        cql.execute(f"CREATE ROLE {andrew_single_quote}")
        cql.execute(f"CREATE ROLE {jane_double_quote}")

        cql.execute(f"GRANT SELECT ON ALL KEYSPACES TO {andrew_single_quote}")
        cql.execute(f"GRANT ALTER ON ALL KEYSPACES TO {jane_double_quote}")

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_permissions(desc_iter)
        desc_iter = filter_non_default_user(desc_iter)

        desc_elements = list(desc_iter)
        element_names_iter = extract_names(desc_elements)

        assert set(element_names_iter) == {andrew_double_quote, jane_double_quote}

        desc_iter = extract_create_statements(desc_elements)

        expected_result = {
            f"GRANT SELECT ON ALL KEYSPACES TO {andrew_double_quote};",
            f"GRANT ALTER ON ALL KEYSPACES TO {jane_double_quote};"
        }

        assert set(desc_iter) == expected_result

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about auth. That's not the case in Cassandra.
def test_desc_auth_different_permissions(cql, scylla_only):
    """
    Verify that the output of `DESC SCHEMA WITH INTERNALS` corresponding to granting permissions
    is as expected for various different cases. Here we test different kinds of permissions specifically.
    """

    with AuthSLContext(cql, ks="my_ks") as ctx:
        all_ks_grants = [f"GRANT {permission} ON ALL KEYSPACES TO {{}};" for permission in KS_AND_TABLE_PERMISSIONS]
        specific_ks_grants = [f"GRANT {permission} ON KEYSPACE {ctx.ks} TO {{}};" for permission in KS_AND_TABLE_PERMISSIONS]

        grants = [*all_ks_grants, *specific_ks_grants, r"GRANT DESCRIBE ON ALL ROLES TO {};"]

        roles = [f"my_role_{idx}" for idx in range(len(grants))]
        grants = [grant.format(roles[idx]) for idx, grant in enumerate(grants)]

        for role in roles:
            cql.execute(f"CREATE ROLE {role}")

        for grant in grants:
            cql.execute(grant)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_permissions(desc_iter)
        # Creating a resource automatically grants all permissioners on it to the creator.
        # That's why we need to filter them out here. Normally, we could check them too,
        # but it's unpredictable how many keyspaces there are when this test case is being
        # executed, and we'd need to take them into consideration too. So we skip them.
        desc_iter = filter_non_default_user(desc_iter)
        desc_iter = extract_create_statements(desc_iter)

        assert set(grants) == set(desc_iter)

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about auth. That's not the case in Cassandra.
def test_desc_data_permissions_uppercase(cql, scylla_only):
    """
    Verify that statements corresponding to granting data permissions correctly format uppercase characters,
    i.e. identifiers like that should be wrapped in quotation marks.
    """

    ks = '"myKs"'
    with AuthSLContext(cql, ks=ks):
        r1 = '"myRole"'
        r2 = '"someOtherRole"'
        r3 = '"YetANOTHERrole"'

        roles = {r1, r2, r3}

        for role in roles:
            cql.execute(f"CREATE ROLE {role}")

        table = '"myTable"'
        cql.execute(f"CREATE TABLE {ks}.{table} (pk int PRIMARY KEY, t int)")

        stmts = {
            f"GRANT SELECT ON ALL KEYSPACES TO {r1};",
            f"GRANT SELECT ON KEYSPACE {ks} TO {r2};",
            f"GRANT SELECT ON {ks}.{table} TO {r3};"
        }

        for stmt in stmts:
            cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_permissions(desc_iter)
        desc_iter = filter_non_default_user(desc_iter)
        desc_elements = list(desc_iter)

        roles_iter = map(lambda row: row.name, desc_elements)
        assert set(roles_iter) == roles

        desc_iter = extract_create_statements(desc_elements)
        assert set(desc_iter) == stmts

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about auth. That's not the case in Cassandra.
def test_desc_data_permissions_unicode(cql, scylla_only):
    """
    Verify that statements corresponding to granting permissions to data resources correctly format
    unicode characters, i.e. identifiers like that should be wrapped in quotation marks.
    """

    with AuthSLContext(cql, ks="my_ks") as ctx:
        r1 = '"ユーザー"'
        r2 = '"私の猫"'
        r3 = '"山羊"'

        roles = {r1, r2, r3}

        for role in roles:
            cql.execute(f"CREATE ROLE {role}")

        table = "my_table"
        cql.execute(f"CREATE TABLE {ctx.ks}.{table} (pk int PRIMARY KEY, t int)")

        stmts = {
            f"GRANT SELECT ON ALL KEYSPACES TO {r1};",
            f"GRANT SELECT ON KEYSPACE {ctx.ks} TO {r2};",
            f"GRANT SELECT ON {ctx.ks}.{table} TO {r3};"
        }

        for stmt in stmts:
            cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_permissions(desc_iter)
        desc_iter = filter_non_default_user(desc_iter)
        desc_elements = list(desc_iter)

        roles_iter = map(lambda row: row.name, desc_elements)
        assert set(roles_iter) == roles

        desc_iter = extract_create_statements(desc_elements)
        assert set(desc_iter) == stmts

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about auth. That's not the case in Cassandra.
def test_desc_data_permissions(cql, scylla_only):
    """
    Verify that the output of `DESC SCHEMA WITH INTERNALS` corresponding to granting permissions
    is as expected for various different cases. Here we test data resources specifically.
    """

    with AuthSLContext(cql, ks="my_ks") as ctx:
        all_ks_role = "mary"
        spec_ks_role = '"r0bb, my gr3@t3st fr!3nd :)"'
        spec_table_role = "scarlet"

        for role in [all_ks_role, spec_ks_role, spec_table_role]:
            cql.execute(f"CREATE ROLE {role}")

        table_name = "my_table"
        # Note: When the keyspace `ctx.ks` is dropped, the table will be removed as well.
        #       That's why there's no need to clean up this table later.
        cql.execute(f"CREATE TABLE {ctx.ks}.{table_name} (a int PRIMARY KEY, b int)")

        all_ks_stmt = f"GRANT CREATE ON ALL KEYSPACES TO {all_ks_role};"
        spec_ks_stmt = f"GRANT ALTER ON KEYSPACE {ctx.ks} TO {spec_ks_role};"
        spec_table_stmt = f"GRANT MODIFY ON {ctx.ks}.{table_name} TO {spec_table_role};"

        stmts = [all_ks_stmt, spec_ks_stmt, spec_table_stmt]
        for stmt in stmts:
            cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_permissions(desc_iter)
        desc_iter = filter_non_default_user(desc_iter)
        desc_iter = extract_create_statements(desc_iter)

        assert set(stmts) == set(desc_iter)

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about auth. That's not the case in Cassandra.
def test_desc_role_permissions_uppercase(cql, scylla_only):
    """
    Verify that statements corresponding to granting role permissions correctly format uppercase characters,
    i.e. identifiers like that should be wrapped in quotation marks.
    """

    with AuthSLContext(cql):
        r1 = '"myRole"'
        r2 = '"MyOtherRole"'

        roles = {r1, r2}
        for role in roles:
            cql.execute(f"CREATE ROLE {role}")

        stmts = {
            f"GRANT AUTHORIZE ON ALL ROLES TO {r1};",
            f"GRANT ALTER ON ROLE {r1} TO {r2};"
        }

        for stmt in stmts:
            cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_permissions(desc_iter)
        desc_iter = filter_non_default_user(desc_iter)

        desc_elements = list(desc_iter)

        role_iter = map(lambda row: row.name, desc_elements)
        assert set(role_iter) == roles

        desc_iter = extract_create_statements(desc_elements)
        assert set(desc_iter) == stmts

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about auth. That's not the case in Cassandra.
def test_desc_role_permissions_unicode(cql, scylla_only):
    """
    Verify that statements corresponding to granting permissions to role resources correctly format
    unicode characters, i.e. identifiers like that should be wrapped in quotation marks.
    """

    with AuthSLContext(cql):
        r1 = '"ユーザー"'
        r2 = '"私の猫"'

        roles = {r1, r2}
        for role in roles:
            cql.execute(f"CREATE ROLE {role}")

        stmts = {
            f"GRANT AUTHORIZE ON ALL ROLES TO {r1};",
            f"GRANT ALTER ON ROLE {r1} TO {r2};"
        }

        for stmt in stmts:
            cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_permissions(desc_iter)
        desc_iter = filter_non_default_user(desc_iter)

        desc_elements = list(desc_iter)

        role_iter = map(lambda row: row.name, desc_elements)
        assert set(role_iter) == roles

        desc_iter = extract_create_statements(desc_elements)
        assert set(desc_iter) == stmts

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about auth. That's not the case in Cassandra.
def test_desc_role_permissions(cql, scylla_only):
    """
    Verify that the output of `DESC SCHEMA WITH INTERNALS` corresponding to granting permissions
    is as expected for various different cases. Here we test role permissions specifically.
    """

    with AuthSLContext(cql):
        all_roles_role = "howard"
        specific_role_role = '"h! th3r3 str@ng3r :)"'

        for role in [all_roles_role, specific_role_role]:
            cql.execute(f"CREATE ROLE {role}")

        all_roles_stmt = f"GRANT AUTHORIZE ON ALL ROLES TO {all_roles_role};"
        specific_role_stmt = f"GRANT ALTER ON ROLE {all_roles_role} TO {specific_role_role};"

        stmts = [all_roles_stmt, specific_role_stmt]
        for stmt in stmts:
            cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_permissions(desc_iter)
        desc_iter = filter_non_default_user(desc_iter)
        desc_iter = extract_create_statements(desc_iter)

        assert set(stmts) == set(desc_iter)

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about auth. That's not the case in Cassandra.
def test_desc_udf_permissions_uppercase(cql, scylla_only):
    """
    Verify that statements corresponding to granting permissions to UDFs correctly format uppercase characters,
    i.e. identifiers like that should be wrapped in quotation marks.
    """

    ks = '"myKs"'
    with AuthSLContext(cql, ks=ks):
        all_funcs_role = '"myRole"'
        all_funcs_in_ks_role = '"MyOtherRole"'
        specific_func_role = '"ROLE"'

        for role in [all_funcs_role, all_funcs_in_ks_role, specific_func_role]:
            cql.execute(f"CREATE ROLE {role}")

        func_name = '"functionName"'
        type_name = '"TypeName"'

        cql.execute(f"CREATE TYPE {ks}.{type_name} (value int)")
        cql.execute(f"""CREATE FUNCTION {ks}.{func_name}(val1 int, val2 {type_name})
                        RETURNS NULL ON NULL INPUT
                        RETURNS int
                        LANGUAGE lua
                        AS $$ return val1 + val2.value $$""")

        stmts = {
            f"GRANT ALTER ON ALL FUNCTIONS TO {all_funcs_role};",
            f"GRANT EXECUTE ON ALL FUNCTIONS IN KEYSPACE {ks} TO {all_funcs_in_ks_role};",
            f"GRANT DROP ON FUNCTION {ks}.{func_name}(int, {type_name}) TO {specific_func_role};"
        }

        for stmt in stmts:
            cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_permissions(desc_iter)
        desc_iter = filter_non_default_user(desc_iter)

        desc_elements = list(desc_iter)

        role_iter = map(lambda row: row.name, desc_elements)
        assert set(role_iter) == {all_funcs_role, all_funcs_in_ks_role, specific_func_role}

        desc_iter = extract_create_statements(desc_elements)
        assert set(desc_iter) == stmts

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about auth. That's not the case in Cassandra.
def test_desc_udf_permissions_unicode(cql, scylla_only):
    """
    Verify that statements corresponding to granting permissions to UDFs correctly format
    unicode characters, i.e. identifiers like that should be wrapped in quotation marks.
    """

    with AuthSLContext(cql, ks="my_ks") as ctx:
        all_funcs_role = '"ユーザー"'
        all_funcs_in_ks_role = '"私の猫"'
        specific_func_role = '"山羊"'

        for role in [all_funcs_role, all_funcs_in_ks_role, specific_func_role]:
            cql.execute(f"CREATE ROLE {role}")

        func_name = '"関数"'
        type_name = '"データ型"'

        cql.execute(f"CREATE TYPE {ctx.ks}.{type_name} (value int)")
        cql.execute(f"""CREATE FUNCTION {ctx.ks}.{func_name}(val1 int, val2 {type_name})
                        RETURNS NULL ON NULL INPUT
                        RETURNS int
                        LANGUAGE lua
                        AS $$ return val1 + val2.value $$""")

        stmts = {
            f"GRANT ALTER ON ALL FUNCTIONS TO {all_funcs_role};",
            f"GRANT EXECUTE ON ALL FUNCTIONS IN KEYSPACE {ctx.ks} TO {all_funcs_in_ks_role};",
            f"GRANT DROP ON FUNCTION {ctx.ks}.{func_name}(int, {type_name}) TO {specific_func_role};"
        }

        for stmt in stmts:
            cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_permissions(desc_iter)
        desc_iter = filter_non_default_user(desc_iter)

        desc_elements = list(desc_iter)

        role_iter = map(lambda row: row.name, desc_elements)
        assert set(role_iter) == {all_funcs_role, all_funcs_in_ks_role, specific_func_role}

        desc_iter = extract_create_statements(desc_elements)
        assert set(desc_iter) == stmts

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about auth. That's not the case in Cassandra.
def test_desc_udf_permissions(cql, scylla_only):
    """
    Verify that the output of `DESC SCHEMA WITH INTERNALS` corresponding to granting permissions
    is as expected for various different cases. Here we test UDFs specifically.
    """

    with AuthSLContext(cql, ks="my_ks") as ctx:
        all_funcs_role = "miley"
        all_funcs_in_ks_role = '"v3ry w3!rd n@m3!"'
        specific_func_role = "ricard"

        for role in [all_funcs_role, all_funcs_in_ks_role, specific_func_role]:
            cql.execute(f"CREATE ROLE {role}")

        func_name = '"funct!0n n@m3"'
        type_name = '"qu!t3 int3r3st!ng typ3 :)"'

        cql.execute(f"CREATE TYPE {ctx.ks}.{type_name} (value int)")
        cql.execute(f"""CREATE FUNCTION {ctx.ks}.{func_name}(val1 int, val2 {type_name})
                        RETURNS NULL ON NULL INPUT
                        RETURNS int
                        LANGUAGE lua
                        AS $$ return val1 + val2.value $$""")

        stmts = {
            f"GRANT ALTER ON ALL FUNCTIONS TO {all_funcs_role};",
            f"GRANT EXECUTE ON ALL FUNCTIONS IN KEYSPACE {ctx.ks} TO {all_funcs_in_ks_role};",
            f"GRANT DROP ON FUNCTION {ctx.ks}.{func_name}(int, {type_name}) TO {specific_func_role};"
        }

        for stmt in stmts:
            cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_permissions(desc_iter)
        desc_iter = filter_non_default_user(desc_iter)

        desc_elements = list(desc_iter)

        role_iter = map(lambda row: row.name, desc_elements)
        assert set(role_iter) == {all_funcs_role, all_funcs_in_ks_role, specific_func_role}

        desc_iter = extract_create_statements(desc_elements)
        assert set(desc_iter) == stmts

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about service levels. That's not the case in Cassandra.
def test_desc_service_levels_format(cql, scylla_only):
    """
    Verify that the format of the output of `DESC SCHEMA WITH INTERNALS` corresponding to
    creating service levels is of the expected form.
    """

    with AuthSLContext(cql):
        sl = ServiceLevel("my_service_level")
        cql.execute(sl.get_create_stmt())

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_service_levels(desc_iter)

        [result] = list(desc_iter)

        assert result.keyspace_name == None
        assert result.type == "service_level"
        assert result.name == sl.name
        assert result.create_statement == sl.get_create_stmt(replace_default_shares=True)

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about service levels. That's not the case in Cassandra.
def test_desc_service_levels_quotation_marks(cql, scylla_only):
    """
    Verify that statements corresponding to creating service levels correctly format quotation marks.
    """

    with AuthSLContext(cql):
        sl1_raw = "service \" 'level maybe'"
        sl2_raw = "service ' \"level perhaps\""

        sl1_single_quote = ServiceLevel(make_identifier(sl1_raw, quotation_mark="'"))
        sl1_double_quote = ServiceLevel(make_identifier(sl1_raw, quotation_mark='"'))
        sl2_double_quote = ServiceLevel(make_identifier(sl2_raw, quotation_mark='"'))

        cql.execute(sl1_single_quote.get_create_stmt())
        cql.execute(sl2_double_quote.get_create_stmt())

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_service_levels(desc_iter)

        desc_elements = list(desc_iter)
        element_names_iter = extract_names(desc_elements)

        assert set(element_names_iter) == {sl1_double_quote.name, sl2_double_quote.name}

        desc_iter = extract_create_statements(desc_elements)

        expected_result = {
            sl1_double_quote.get_create_stmt(replace_default_shares=True),
            sl2_double_quote.get_create_stmt(replace_default_shares=True)
        }

        assert set(desc_iter) == expected_result

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about service levels. That's not the case in Cassandra.
def test_desc_service_levels_uppercase(cql, scylla_only):
    """
    Verify that statements corresponding to creating service levels correctly format uppercase characters,
    i.e. identifiers like that should be wrapped in quotation marks.
    """

    with AuthSLContext(cql):
        sl = ServiceLevel('"myServiceLevel"')
        cql.execute(sl.get_create_stmt())

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_service_levels(desc_iter)

        desc_elements = list(desc_iter)

        sl_iter = map(lambda row: row.name, desc_elements)
        assert list(sl_iter) == [sl.name]

        desc_iter = extract_create_statements(desc_elements)
        assert list(desc_iter) == [sl.get_create_stmt(replace_default_shares=True)]

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about service levels. That's not the case in Cassandra.
def test_desc_service_levels_unicode(cql, scylla_only):
    """
    Verify that statements corresponding to creating service levels correctly format
    unicode characters, i.e. identifiers like that should be wrapped in quotation marks.
    """

    with AuthSLContext(cql):
        sl = ServiceLevel('"レベル"')
        cql.execute(sl.get_create_stmt())

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_service_levels(desc_iter)

        desc_elements = list(desc_iter)

        sl_iter = map(lambda row: row.name, desc_elements)
        assert list(sl_iter) == [sl.name]

        desc_iter = extract_create_statements(desc_elements)
        assert list(desc_iter) == [sl.get_create_stmt(replace_default_shares=True)]

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about service levels. That's not the case in Cassandra.
def test_desc_auth_service_levels(cql, scylla_only):
    """
    Verify that the output of `DESC SCHEMA WITH INTERNALS` corresponding to creating service levels
    is as expected for various different cases.
    """

    with AuthSLContext(cql):
        service_levels = {
            # No additional parameters.
            ServiceLevel("sl1"),
            # Timeout parameter.
            ServiceLevel("sl2", timeout=10), ServiceLevel("sl3", timeout=350), ServiceLevel("sl4", 20000),
            # Workload type parameter.
            ServiceLevel("sl5", wl_type="interactive"), ServiceLevel("sl6", wl_type="batch"),
            # Timeout and workload parameter.
            ServiceLevel("sl7", timeout=25000, wl_type="interactive")
        }
        service_levels |= { ServiceLevel(sl.name + 's', wl_type=sl.wl_type, timeout=sl.timeout, shares=400) for sl in service_levels }

        sl_create_stmts = set(map(lambda sl: sl.get_create_stmt(replace_default_shares=True), service_levels))

        # Enterprise is limited in the number of service levels it supports
        sl_create_stmts = set(random.sample(list(sl_create_stmts), k=5))

        for stmt in sl_create_stmts:
            cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_service_levels(desc_iter)
        desc_iter = extract_create_statements(desc_iter)

        assert sl_create_stmts == set(desc_iter)

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about service levels. That's not the case in Cassandra.
def test_desc_service_levels_default_shares(cql, scylla_only):
    """
    Verify that DESCRIBE handles the default value of shares correctly:
    (a) when a service level is created without specifying the number of shares,
        we should get a create statement with the default number of shares,
    (b) when a service level is created with the default number of shares but specified explicitly,
        we should get a create statement with that number of shares too.
    """

    with AuthSLContext(cql):
        default_share_count = 1000

        stmts = [
            "CREATE SERVICE LEVEL sl_default;",
            f"CREATE SERVICE LEVEL sl_set WITH SHARES = {default_share_count};",
        ]

        for stmt in stmts:
            cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_service_levels(desc_iter)
        desc_iter = extract_create_statements(desc_iter)

        stmts[0] = f"CREATE SERVICE LEVEL sl_default WITH SHARES = {default_share_count};"
        assert stmts == list(desc_iter)

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about service levels. That's not the case in Cassandra.
def test_desc_attach_service_level_format(cql, scylla_only):
    """
    Verify that the format of the output of `DESC SCHEMA WITH INTERNALS` corresponding to
    attaching service levels is of the expected form.
    """

    with AuthSLContext(cql):
        role_name = "jasmine"
        cql.execute(f"CREATE ROLE {role_name}")

        sl_name = "some_service_level"
        cql.execute(f"CREATE SERVICE LEVEL {sl_name}")

        stmt = f"ATTACH SERVICE LEVEL {sl_name} TO {role_name};"
        cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_attached_service_levels(desc_iter)

        [result] = list(desc_iter)

        assert result.keyspace_name == None
        assert result.type == "service_level_attachment"
        assert result.name == sl_name
        assert result.create_statement == stmt

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about service levels. That's not the case in Cassandra.
def test_desc_auth_attach_service_levels_quotation_marks(cql, scylla_only):
    """
    Verify that statements corresponding to attaching service levels correctly format quotation marks.
    """

    with AuthSLContext(cql):
        andrew_raw = "andrew \" 'the great'"
        jane_raw = "jane ' \"the wise\""

        andrew_single_quote = make_identifier(andrew_raw, quotation_mark="'")
        andrew_double_quote = make_identifier(andrew_raw, quotation_mark='"')
        jane_double_quote = make_identifier(jane_raw, quotation_mark='"')

        cql.execute(f"CREATE ROLE {andrew_single_quote}")
        cql.execute(f"CREATE ROLE {jane_double_quote}")

        sl1_raw = "service \" 'level maybe'"
        sl2_raw = "service ' \"level perhaps\""

        sl1_single_quote = make_identifier(sl1_raw, quotation_mark="'")
        sl1_double_quote = make_identifier(sl1_raw, quotation_mark='"')
        sl2_double_quote = make_identifier(sl2_raw, quotation_mark='"')

        cql.execute(f"CREATE SERVICE LEVEL {sl1_single_quote}")
        cql.execute(f"CREATE SERVICE LEVEL {sl2_double_quote}")

        cql.execute(f"ATTACH SERVICE LEVEL {sl1_single_quote} TO {andrew_single_quote}")
        cql.execute(f"ATTACH SERVICE LEVEL {sl2_double_quote} TO {jane_double_quote}")

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_attached_service_levels(desc_iter)

        desc_elements = list(desc_iter)
        element_names_iter = extract_names(desc_elements)

        assert set(element_names_iter) == {sl1_double_quote, sl2_double_quote}

        desc_iter = extract_create_statements(desc_elements)

        expected_result = {
            f"ATTACH SERVICE LEVEL {sl1_double_quote} TO {andrew_double_quote};",
            f"ATTACH SERVICE LEVEL {sl2_double_quote} TO {jane_double_quote};"
        }

        assert set(desc_iter) == expected_result

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about service levels. That's not the case in Cassandra.
def test_desc_auth_attach_service_levels_uppercase(cql, scylla_only):
    """
    Verify that statements corresponding to attaching service levels correctly format uppercase characters,
    i.e. identifiers like that should be wrapped in quotation marks.
    """

    with AuthSLContext(cql):
        role = '"myRole"'
        sl = '"MyServiceLevel"'

        cql.execute(f"CREATE ROLE {role}")
        cql.execute(f"CREATE SERVICE LEVEL {sl}")
        cql.execute(f"ATTACH SERVICE LEVEL {sl} TO {role}")

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_attached_service_levels(desc_iter)

        desc_elements = list(desc_iter)

        sl_iter = map(lambda row: row.name, desc_elements)
        assert list(sl_iter) == [sl]

        desc_iter = extract_create_statements(desc_elements)
        assert list(desc_iter) == [f"ATTACH SERVICE LEVEL {sl} TO {role};"]

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about service levels. That's not the case in Cassandra.
def test_desc_attach_service_levels_unicode(cql, scylla_only):
    """
    Verify that statements corresponding to attaching service levels correctly format
    unicode characters, i.e. identifiers like that should be wrapped in quotation marks.
    """

    with AuthSLContext(cql):
        role = '"私の猫"'
        sl = '"サービスレベル"'

        cql.execute(f"CREATE ROLE {role}")
        cql.execute(f"CREATE SERVICE LEVEL {sl}")
        cql.execute(f"ATTACH SERVICE LEVEL {sl} TO {role}")

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_attached_service_levels(desc_iter)

        desc_elements = list(desc_iter)

        sl_iter = map(lambda row: row.name, desc_elements)
        assert list(sl_iter) == [sl]

        desc_iter = extract_create_statements(desc_elements)
        assert list(desc_iter) == [f"ATTACH SERVICE LEVEL {sl} TO {role};"]

# Marked as `scylla_only` because we verify that the output of `DESCRIBE SCHEMA`
# contains information about service levels. That's not the case in Cassandra.
def test_desc_auth_attach_service_levels(cql, scylla_only):
    """
    Verify that the output of `DESC SCHEMA WITH INTERNALS` corresponding to attaching service levels
    is as expected for various different cases.
    """

    with AuthSLContext(cql):
        [r1, r2] = ["andrew", '"j@n3 is my fr!3nd"']
        cql.execute(f"CREATE ROLE {r1}")
        cql.execute(f"CREATE ROLE {r2}")
        cql.execute(f"GRANT {r1} TO {r2}")

        [sl1, sl2] = ["my_service_level", '"s0m3 0th3r s3rv!c3 l3v3l!"']
        # Note: The smaller timeout, the better. We also want to verify in this test
        #       that the service level statement returned by `DESC SCHEMA` that corresponds
        #       to `r1` is the actual service level it was granted, not its effective service level.
        cql.execute(f"CREATE SERVICE LEVEL {sl1} WITH TIMEOUT = 1ms")
        cql.execute(f"CREATE SERVICE LEVEL {sl2} WITH TIMEOUT = 10ms")

        sl_stmt1 = f"ATTACH SERVICE LEVEL {sl1} TO {r1};"
        sl_stmt2 = f"ATTACH SERVICE LEVEL {sl2} TO {r2};"
        sl_stmts = [sl_stmt1, sl_stmt2]

        for stmt in sl_stmts:
            cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_attached_service_levels(desc_iter)
        desc_iter = extract_create_statements(desc_iter)

        assert set(sl_stmts) == set(desc_iter)

@pytest.mark.xfail(reason="issue #25187")
def test_desc_restore(cql):
    """
    Verify that restoring the schema, auth and service levels works correctly. We create entities
    of each relevant kind, describe the schema, and drop everything. Then we restore it by
    executing the obtained create statement, describe the schema again, and verify that we
    have obtained the same description as before.
    """

    restore_stmts = None
    ks = "my_ks"

    with AuthSLContext(cql, ks=ks):
        cql.execute(f"CREATE TABLE {ks}.my_table (pk int PRIMARY KEY, sth int)")
        cql.execute(f"CREATE TYPE {ks}.my_type (value int)")
        cql.execute(f"""CREATE TABLE {ks}.some_other_table (c1 frozen<my_type>, c2 double, c3 int, c4 set<int>,
                        PRIMARY KEY ((c1, c2), c3)) WITH comment = 'some comment'""")
        cql.execute(f"CREATE TABLE {ks}.vector_table (pk int PRIMARY KEY, v vector<float, 3>)")

        cql.execute(f"""CREATE MATERIALIZED VIEW {ks}.mv AS
                            SELECT pk FROM {ks}.my_table
                            WHERE pk IS NOT NULL
                            PRIMARY KEY (pk)
                            WITH comment='some other comment'""")

        cql.execute(f"CREATE INDEX myindex ON {ks}.some_other_table (c1)")

        # Scylla doesn't support `sai`, while Cassandra doesn't support `vector_index`.
        vec_class = "vector_index" if is_scylla(cql) else "sai"
        cql.execute(f"CREATE INDEX custom_index ON {ks}.vector_table (v) USING '{vec_class}'")

        # Scylla supports UDFs with Lua. Cassandra supports UDFs with Java.
        if is_scylla(cql):
            cql.execute(f"""CREATE FUNCTION {ks}.my_udf(val1 int, val2 int)
                            RETURNS NULL ON NULL INPUT
                            RETURNS int
                            LANGUAGE lua
                            AS $$ return val1 + val2 $$""")
        else:
            cql.execute(f"""CREATE FUNCTION {ks}.my_udf(val1 int, val2 int)
                            RETURNS NULL ON NULL INPUT
                            RETURNS int
                            LANGUAGE java
                            AS $$ return val1 + val2; $$""")

        cql.execute(f"""CREATE AGGREGATE {ks}.my_aggregate(int)
                            SFUNC my_udf
                            STYPE int
                            INITCOND 0""")

        [r1, r2, r3] = ["jack", "'b0b @nd d0b!'", "jane"]
        cql.execute(f"CREATE ROLE {r1} WITH PASSWORD = 'pass1'")
        cql.execute(f"CREATE ROLE {r2} WITH PASSWORD = 'pass2'")
        cql.execute(f"CREATE ROLE {r3}")

        cql.execute(f"GRANT {r1} TO {r3}")
        cql.execute(f"GRANT {r3} TO {r2}")

        cql.execute(f"GRANT ALL ON ALL KEYSPACES TO {r2}")
        cql.execute(f"GRANT SELECT ON KEYSPACE {ks} TO {r3}")
        cql.execute(f"GRANT MODIFY ON TABLE {ks}.my_table TO {r1}")
        cql.execute(f"GRANT AUTHORIZE ON ALL ROLES TO {r1}")
        cql.execute(f"GRANT DESCRIBE ON ALL ROLES TO {r1}")

        # Only Scylla supports service levels.
        if is_scylla(cql):
            [sl1, sl2] = ["my_service_level", "'s3rv!c3 l3v3l !!!'"]
            cql.execute(f"CREATE SERVICE LEVEL {sl1} WITH TIMEOUT = 10ms AND WORKLOAD_TYPE = 'batch'")
            cql.execute(f"CREATE SERVICE LEVEL {sl2} WITH TIMEOUT = 100s")

            cql.execute(f"ATTACH SERVICE LEVEL {sl1} TO {r1}")
            cql.execute(f"ATTACH SERVICE LEVEL {sl1} TO {r2}")
            cql.execute(f"ATTACH SERVICE LEVEL {sl2} TO {r3}")

            # Only Scylla supports the `... WITH PASSWORDS` form of `DESC SCHEMA`.
        if is_scylla(cql):
            restore_stmts = list(cql.execute("DESC SCHEMA WITH INTERNALS AND PASSWORDS"))
        else:
            restore_stmts = list(cql.execute("DESC SCHEMA WITH INTERNALS"))

    def remove_other_keyspaces(rows: Iterable[DescRowType]) -> Iterable[DescRowType]:
        return filter(lambda row: row.keyspace_name == ks or row.keyspace_name == None, rows)

    # Other test cases might've created keyspaces that would be included in the result
    # of `DESC SCHEMA`, so we need to filter them out.
    restore_stmts = list(remove_other_keyspaces(restore_stmts))

    with AuthSLContext(cql):
        try:
            for stmt in extract_create_statements(restore_stmts):
                cql.execute(stmt)

            # Only Scylla supports the `... WITH PASSWORDS` form of `DESC SCHEMA`.
            if is_scylla(cql):
                res = list(cql.execute("DESC SCHEMA WITH INTERNALS AND PASSWORDS"))
            else:
                res = list(cql.execute("DESC SCHEMA WITH INTERNALS"))

            # Other test cases might've created keyspaces that would be included in the result
            # of `DESC SCHEMA`, so we need to filter them out.
            res = list(remove_other_keyspaces(res))

            assert restore_stmts == res
        finally:
            cql.execute(f"DROP KEYSPACE IF EXISTS {ks}")

### ===========================================================================

# Verifies that describing a UDF with built-in types works correctly.
# Scylla-only as the UDF uses Lua.
def test_describe_udf_with_builtin_types(cql, test_keyspace, scylla_only):
    fn_content = """\
        (free_arg int, a list<boolean>, b set<time>, c map<varint, text>, d tuple<uuid, inet>)
        RETURNS NULL ON NULL INPUT
        RETURNS bigint
        LANGUAGE lua
        AS $$
        return 3
        $$;"""
    # Get rid of indentation in the string.
    fn_content = textwrap.dedent(fn_content)

    with new_function(cql, test_keyspace, fn_content) as fn:
        udf_row = cql.execute(f"DESC FUNCTION {test_keyspace}.{fn}").one()
        udf_stmt = udf_row.create_statement

        expected_stmt = f"CREATE FUNCTION {test_keyspace}.{fn}{fn_content}"
        assert udf_stmt == expected_stmt

# Verifies that describing a UDF with UDTs as its arguments works correctly.
# Reproduces: scylladb/scylladb#20256.
# Scylla-only as the UDf uses Lua.
def test_describe_udf_with_udt(cql, test_keyspace, scylla_only):
    with new_type(cql, test_keyspace, "(value int)") as udt:
        _, udt_name = udt.split(".")

        fn_content = f"""\
            (free_arg {udt_name}, a list<frozen<{udt_name}>>, b set<frozen<{udt_name}>>, c map<frozen<{udt_name}>, frozen<{udt_name}>>, d tuple<frozen<{udt_name}>, int>)
            RETURNS NULL ON NULL INPUT
            RETURNS {udt_name}
            LANGUAGE lua
            AS $$
            rhs.value = lhs + rhs.value
            return rhs
            $$;"""
        # Get rid of indentation in the string.
        fn_content = textwrap.dedent(fn_content)

        with new_function(cql, test_keyspace, fn_content) as fn:
            udf_row = cql.execute(f"DESC FUNCTION {test_keyspace}.{fn}").one()
            udf_stmt = udf_row.create_statement

            expected_stmt = f"CREATE FUNCTION {test_keyspace}.{fn}{fn_content}"
            assert udf_stmt == expected_stmt

# Test that the Scylla-only "tombstone_gc" option appears in the output of
# "desc table". Reproduces issue #14390.
# The test is marked scylla_only because Cassandra doesn't have this
# "tombstone_gc" option.
def test_desc_table_tombstone_gc(cql, test_keyspace, scylla_only):
        with_clause = "tombstone_gc = {'mode': 'timeout', 'propagation_delay_in_seconds': '73'}"
        with new_test_table(cql, test_keyspace, "p int PRIMARY KEY", "WITH " + with_clause) as table:
            desc = cql.execute(f"DESCRIBE TABLE {table} WITH INTERNALS").one()
            # ignore spaces in comparison, as different versions of Scylla
            # add spaces in different places
            assert with_clause.replace(' ','') in desc.create_statement.replace(' ','')
