# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Various tests for static-column support in Scylla.
# Note that we have many more tests for static columns in other test
# frameworks (C++, cql, dtest) but the benefit of these tests is that they
# allow running tests with debatable behavior on Cassandra as well.
#############################################################################

import pytest
from cassandra.protocol import SyntaxException, InvalidRequest
from cassandra.query import SimpleStatement
from .util import new_named_table, new_test_table, new_secondary_index, unique_key_int
from .nodetool import flush

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    schema='p int, c int, r int, s int static, PRIMARY KEY(p, c)'
    with new_test_table(cql, test_keyspace, schema) as table:
        yield table

# Test what happens when we SELECT a partition which has a static column
# set but no clustering row, but the static column is *not* selected.
# Reproduces issue #10091.
#
# Marking this issue "xfail" because Scylla and Cassandra differ in behavior
# and we have no good explanation why Scylla's behavior is more correct than
# Cassandra. If we later decide that we consider *both* behaviors equally
# correct, we can change the test to accept both and make it pass.
@pytest.mark.xfail(reason="issue #10091")
def test_static_not_selected(cql, table1):
    p = unique_key_int()
    # The partition p doesn't exist, so the following select yields nothing:
    assert list(cql.execute(f'SELECT * FROM {table1} WHERE p={p}')) == []
    # Insert just the static column, and no clustering row:
    cql.execute(f'INSERT INTO {table1} (p, s) values ({p}, 1)')
    # If we select all the columns, including the static column s, SELECTing
    # the partition gives us one "row" with the static column set and all
    # other columns set to null - otherwise the static column cannot be
    # returned.
    assert list(cql.execute(f'SELECT * FROM {table1} WHERE p={p}')) == [(p,None,1,None)]
    # But what happens if we SELECT just regular (non-static) columns?
    # Should the SELECT return nothing (since there is no clustering row),
    # or return one "row" with null columns (basically just like in the
    # previous SELECT, just intersected with the desired column)?
    # Currently, Cassandra does the former, Scylla does the latter,
    # so the following assert fails on Scylla:
    assert list(cql.execute(f'SELECT r FROM {table1} WHERE p={p}')) == []

# Verify that if a partition has a static column set, reading an *existing*
# clustering row will return it, but reading a *non-existing* row will not
# return anything - not even the static column. 
#
# Contrast this with issue #10081 (test_lwt.py::test_lwt_missing_row_with_static)
# where we suggested that in an UPDATE with IF condition (LWT), the static
# columns should be readable in the condition expression, whether or not the
# given row exists.
def test_missing_row_with_static(cql, table1):
    p = unique_key_int()
    # Insert into partition p just static column and once clustering row c=2
    cql.execute(f'INSERT INTO {table1}(p, s, c, r) values ({p}, 1, 2, 3)')
    # If we SELECT row c=2, we get it and the static column:
    assert list(cql.execute(f'SELECT p, s, c, r FROM {table1} WHERE p={p} AND c=2')) == [(p, 1, 2, 3)]
    # If we SELECT row c=1 (which doesn't exist), we get nothing - not even
    # the static column
    assert list(cql.execute(f'SELECT p, s, c, r FROM {table1} WHERE p={p} AND c=1')) == []

# Verify that if a partition has a static column set, filtering it (with
# ALLOW FILTERING) based on regular columns will need to find clustering
# rows which actually match this filter - and can't return only a static
# row (which doesn't match the filter). This is similar to the previous test
# (test_missing_row_with_static) except that the filter here is on a regular
# column, not a clustering column.
def test_filter_with_static(cql, table1):
    p = unique_key_int()
    # Insert into partition p just static column and one clustering row
    # with its regular column r set to 3:
    cql.execute(f'INSERT INTO {table1}(p, s, c, r) values ({p}, 1, 2, 3)')
    # If we filter for r=3, we expect to get the clustering row and the
    # static column:
    assert list(cql.execute(f'SELECT p, s, c, r FROM {table1} WHERE p={p} AND r=3 ALLOW FILTERING')) == [(p, 1, 2, 3)]
    # If we filter for r=4, we expect to get nothing - not even the static
    # column. Getting back just the static row, with r=null, would be wrong
    # because it does *not* match the filter r=4.
    assert list(cql.execute(f'SELECT p, s, c, r FROM {table1} WHERE p={p} AND r=4 ALLOW FILTERING')) == []

# Reproduce issue #10357. The query is identical to the one in the above
# test (test_filter_with_static), but here the partition has only a static
# column value, and no clustering row at all. We expect to get back nothing
# when using a filtering on a regular column.
def test_filter_with_only_static(cql, table1):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table1}(p, s) values ({p}, 1)')
    # There are no clustering rows or r column set for anything, so filtering
    # for r=4 should return nothing. But issue #10357 caused Scylla to wrongly
    # return a row with only the static value - which doesn't match the filter.
    assert list(cql.execute(f'SELECT p, s, c, r FROM {table1} WHERE p={p} AND r=4 ALLOW FILTERING')) == []

# The CREATE TABLE syntax allows either "PRIMARY KEY" or "STATIC" to follow
# a column's definition, but it's forbidden to use both at once!
def test_syntax_primary_key_and_static(cql, test_keyspace):
    # Because of implementation details of the parser, "STATIC PRIMARY KEY"
    # is considered valid syntax but rejected as InvalidRequest with a message
    # "Static column p cannot be part of the PRIMARY KEY", while the reverse
    # "PRIMARY KEY STATIC" is rejected as a SyntaxException. We do not insist
    # on this bizarre distinction, and allow both types of errors for both
    # cases.
    with pytest.raises((SyntaxException, InvalidRequest)):
        with new_test_table(cql, test_keyspace, 'p int STATIC PRIMARY KEY, v int') as table:
            pass
    with pytest.raises((SyntaxException, InvalidRequest)):
        with new_test_table(cql, test_keyspace, 'p int PRIMARY KEY STATIC, v int') as table:
            pass

# Test reading a static column through a secondary index on a *regular*
# column, when the index lookup matches rows in several partitions that have
# different static values - including when paging splits the results.
# The case where all matches are in one partition is already tested in
# cassandra_tests/validation/entities/static_columns_test.py
# (testStaticColumnsWithSecondaryIndex).
def test_static_column_via_regular_index_multiple_partitions(cql, test_keyspace):
    schema = 'p int, c int, s int static, v int, PRIMARY KEY (p, c)'
    with new_test_table(cql, test_keyspace, schema) as table:
        with new_secondary_index(cql, table, 'v'):
            insert = cql.prepare(f'INSERT INTO {table} (p, c, s, v) VALUES (?, ?, ?, ?)')
            cql.execute(insert, [1, 1, 11, 7])
            cql.execute(insert, [1, 2, 11, 8])
            cql.execute(insert, [2, 1, 22, 7])
            cql.execute(insert, [2, 2, 22, 7])
            for fetch_size in [None, 1, 2]:
                stmt = SimpleStatement(f'SELECT s FROM {table} WHERE v = 7', fetch_size=fetch_size)
                assert sorted(cql.execute(stmt)) == [(11,), (22,), (22,)]
                stmt = SimpleStatement(f'SELECT p, s FROM {table} WHERE v = 7', fetch_size=fetch_size)
                assert sorted(cql.execute(stmt)) == [(1, 11), (2, 22), (2, 22)]


# Tests ported from scylla-dtest's cql_additional_tests.py. Like the dtests,
# they run in a keyspace with vnodes, and keep the dtests' table names.

# A static column: written without rows, with writetime (#7081), shared by
# all rows of the partition, read with slices, IN (#6769), DISTINCT and with
# or without other columns; not deleted with a row, deleted on its own; a
# static column added and dropped by ALTER TABLE.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_static_column_operations(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "test", "k int, p int, s int static, v int, PRIMARY KEY (k, p)") as table:
        def check(stmt, expected):
            assert [tuple(r) for r in cql.execute(stmt)] == [tuple(e) for e in expected]
        cql.execute(f"INSERT INTO {table}(k, s) VALUES (0, 42)")
        check(f"SELECT * FROM {table}", [[0, None, 42, None]])
        [row] = list(cql.execute(f"SELECT s, writetime(s) FROM {table} WHERE k=0"))
        assert row[0] == 42 and row[1] > 0
        cql.execute(f"INSERT INTO {table}(k, p, s, v) VALUES (0, 0, 12, 0)")
        cql.execute(f"INSERT INTO {table}(k, p, s, v) VALUES (0, 1, 24, 1)")
        check(f"SELECT * FROM {table}", [[0, 0, 24, 0], [0, 1, 24, 1]])
        check(f"SELECT * FROM {table} WHERE k=0 AND p=0", [[0, 0, 24, 0]])
        check(f"SELECT * FROM {table} WHERE k=0 AND p=1", [[0, 1, 24, 1]])
        check(f"SELECT * FROM {table} WHERE k=0 AND p IN (0, 1)", [[0, 0, 24, 0], [0, 1, 24, 1]])
        check(f"SELECT p, v FROM {table} WHERE k=0 AND p=1", [[1, 1]])
        check(f"SELECT DISTINCT s FROM {table} WHERE k=0", [[24]])
        check(f"SELECT s FROM {table} WHERE k=0", [[24], [24]])
        check(f"SELECT s, v FROM {table} WHERE k=0", [[24, 0], [24, 1]])
        check(f"SELECT s, v FROM {table} WHERE k=0 AND p=1", [[24, 1]])
        check(f"SELECT p, s FROM {table} WHERE k=0 AND p=1", [[1, 24]])
        check(f"SELECT k, p, s FROM {table} WHERE k=0 AND p=1", [[0, 1, 24]])
        cql.execute(f"DELETE FROM {table} WHERE k=0 AND p=0")
        check(f"SELECT * FROM {table}", [[0, 1, 24, 1]])
        cql.execute(f"DELETE s FROM {table} WHERE k=0")
        check(f"SELECT * FROM {table}", [[0, 1, None, 1]])
        cql.execute(f"ALTER TABLE {table} ADD s2 int static")
        check(f"SELECT * FROM {table}", [[0, 1, None, None, 1]])
        cql.execute(f"INSERT INTO {table.upper()} (k, p, s2, v) VALUES(0, 2, 42, 2)")
        check(f"SELECT * FROM {table}", [[0, 1, None, 42, 1], [0, 2, None, 42, 2]])
        cql.execute(f"ALTER TABLE {table} DROP s2")
        check(f"SELECT * FROM {table}", [[0, 1, None, 1], [0, 2, None, 2]])

# LIMIT on a partition with a static column counts rows, not the static row
# (CASSANDRA-6956).
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_static_column_with_limit(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "test", "k int, s int static, v int, PRIMARY KEY (k, v)") as table:
        cql.execute(f"INSERT INTO {table}(k, s) VALUES(0, 42)")
        for i in range(4):
            cql.execute(f"INSERT INTO {table}(k, v) VALUES(0, {i})")
        for limit in [1, 2, 3]:
            assert list(cql.execute(f"SELECT * FROM {table} WHERE k = 0 LIMIT {limit}")) == [(0, i, 42) for i in range(limit)]

# A row with an empty clustering key in a partition with a static column
# (CASSANDRA-7455), read from a memtable and from an sstable.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
def test_static_column_with_empty_clustering_key(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "test", "pkey text, ckey text, value text, static_value text static, PRIMARY KEY(pkey, ckey)") as table:
        cql.execute(f"INSERT INTO {table}(pkey, static_value) VALUES ('partition1', 'static value')")
        cql.execute(f"INSERT INTO {table}(pkey, ckey, value) VALUES('partition1', '', 'value')")
        assert list(cql.execute(f"SELECT * FROM {table}")) == [("partition1", "", "static value", "value")]
        # The same read from an sstable; the dtest's node flushed at shutdown.
        flush(cql, table)
        assert list(cql.execute(f"SELECT * FROM {table}")) == [("partition1", "", "static value", "value")]
