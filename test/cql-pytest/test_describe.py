
# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

###############################################################################
# Tests for server-side describe
###############################################################################

import pytest
import random
from pytest import fixture
from contextlib import contextmanager, ExitStack
from util import new_type, unique_name, new_test_table, new_test_keyspace, new_function, new_aggregate, new_cql, keyspace_has_tablets, unique_name_prefix
from cassandra.protocol import InvalidRequest
import re

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

# Test that 'DESC INDEX' contains create statement od index
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
        
        assert f"CREATE INDEX {tbl_name}_b_idx ON {tbl}{maybe_space}(b)" in desc
        assert f"CREATE INDEX named_index ON {tbl}{maybe_space}(c)" in desc
        if has_local:
            assert f"CREATE INDEX {tbl_name}_b_idx_1 ON {tbl}((a), b)" in desc

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
def test_hide_cdc_table(scylla_only, cql, test_keyspace):
    cdc_table_suffix = "_scylla_cdc_log"
    
    with new_test_table(cql, test_keyspace, "a int primary key, b int", "WITH cdc = {'enabled': true}") as t:
        t_name = t.split('.')[1]
        cdc_log_name = t_name + cdc_table_suffix

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

        # Check 't_scylla_cdc_log' cannot be described directly
        with pytest.raises(InvalidRequest, match=f"{test_keyspace}.{cdc_log_name} is a cdc log table and it cannot be described directly. Try `DESC TABLE {test_keyspace}.{t_name}` to describe cdc base table and it's log table."):
            desc_cdc_table = cql.execute(f"DESC TABLE {test_keyspace}.{cdc_log_name}")
        
        # Check base table description contains ALTER TABLE statement for cdc log table
        desc_base_table = cql.execute(f"DESC TABLE {t}").all()
        assert f"ALTER TABLE {test_keyspace}.{cdc_log_name} WITH" in desc_base_table[1].create_statement
        
        # Drop current cdc base table and try to recreate it with describe output
        cql.execute(f"DROP TABLE {t}")
        for row in desc_keyspace[1:]: # [1:] because we want to skip first row (keyspace's CREATE STATEMENT)
            cql.execute(row.create_statement)
        
        # Check if base and log tables were recreated
        ks_tables = cql.execute(f"SELECT * FROM system_schema.tables WHERE keyspace_name='{test_keyspace}'").all()
        assert len(ks_tables) == 2
        for row in ks_tables:
            assert row.table_name == t_name or row.table_name == cdc_log_name


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
def new_random_table(cql, keyspace, udts=[]):
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
    extras["memtable_flush_period_in_ms"] = random.randrange(0, 10000)

    if is_scylla(cql):
        # Extra properties which ScyllaDB supports but Cassandra doesn't
        extras["paxos_grace_seconds"] = random.randrange(1000, 100000)
        extras["tombstone_gc"] = f"{{'mode': 'timeout', 'propagation_delay_in_seconds': '{random.randrange(100, 100000)}'}}"

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
