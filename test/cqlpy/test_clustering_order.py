# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for clustering key ordering, namely the WITH CLUSTERING ORDER BY
# setting in the table schema, and ORDER BY in select.
#
# We have many other tests for this feature - in C++ tests, in translated
# unit tests from Cassandra (cassandra_tests), and its interaction with
# other features (filtering, secondary indexes, etc.) in other test files.

import itertools
import time
from contextlib import contextmanager

import pytest
from cassandra.query import SimpleStatement

from .util import new_test_table, unique_key_int, sleep_till_whole_second
from . import nodetool

@pytest.fixture(scope="module")
def table_int_desc(cql, test_keyspace):
    schema="k INT, c INT, PRIMARY KEY (k, c)"
    order="WITH CLUSTERING ORDER BY (c DESC)"
    with new_test_table(cql, test_keyspace, schema, order) as table:
        yield table

@pytest.fixture(scope="module")
def table2(cql, test_keyspace):
    schema="p int, c1 int, c2 int, v int, PRIMARY KEY (p, c1, c2)"
    order="WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC)"
    with new_test_table(cql, test_keyspace, schema, order) as table:
        yield table

# Verify that if a table is created with descending order for its
# clustering key, the default ordering of SELECT is changed to descending
# order. This was contrary to our documentation which used to suggest
# that SELECT always defaults to ascending order.
def test_select_default_order(cql, table_int_desc):
    k = unique_key_int()
    stmt = cql.prepare(f'INSERT INTO {table_int_desc} (k, c) VALUES ({k}, ?)')
    numbers = range(5)
    for i in numbers:
        cql.execute(stmt, [i])
    # In a table created with descending sort order, the default select
    # order is descending:
    rows = [(i,) for i in numbers]
    reverse_rows = [(i,) for i in reversed(numbers)]
    assert reverse_rows == list(cql.execute(f'SELECT c FROM {table_int_desc} WHERE k = {k}'))
    # Confirm that when specifying the order explicitly, both work:
    assert rows == list(cql.execute(f'SELECT c FROM {table_int_desc} WHERE k = {k} ORDER BY c ASC'))
    assert reverse_rows == list(cql.execute(f'SELECT c FROM {table_int_desc} WHERE k = {k} ORDER BY c DESC'))
    # Repeat the same three assertions as above, adding a "limit" of N=3:
    N=3
    rows = rows[0:N]
    reverse_rows = reverse_rows[0:N]
    assert reverse_rows == list(cql.execute(f'SELECT c FROM {table_int_desc} WHERE k = {k} LIMIT {N}'))
    assert rows == list(cql.execute(f'SELECT c FROM {table_int_desc} WHERE k = {k} ORDER BY c ASC LIMIT {N}'))
    assert reverse_rows == list(cql.execute(f'SELECT c FROM {table_int_desc} WHERE k = {k} ORDER BY c DESC LIMIT {N}'))

# Reproduce issue #65: Test SELECT with a compound clustering key c1,c2
# and a multi-column relation (c1, c2) >= (1,1) when one of the clustering
# column has reversed order. We need to uphold the correct inequality (not
# reversed), just return results in a the reversed order.
def test_multi_column_relation_desc(cql, table2):
    k = unique_key_int()
    stmt = cql.prepare(f'INSERT INTO {table2} (p, c1, c2) VALUES (?, ?, ?)')
    cql.execute(stmt, [k, 1, 0])
    cql.execute(stmt, [k, 1, 1])
    cql.execute(stmt, [k, 1, 2])
    assert [(1, 2), (1, 1)] == list(cql.execute(f'SELECT c1,c2 FROM {table2} WHERE p = {k} AND (c1, c2) >= (1, 1)'))

@pytest.fixture(scope="module")
def table_all_asc(cql, test_keyspace):
    schema="p int, c1 int, c2 int, v int, PRIMARY KEY (p, c1, c2)"
    with new_test_table(cql, test_keyspace, schema) as table:
        yield table

@pytest.fixture(scope="module")
def table_all_desc(cql, test_keyspace):
    schema="p int, c1 int, c2 int, v int, PRIMARY KEY (p, c1, c2)"
    order="WITH CLUSTERING ORDER BY (c1 DESC, c2 DESC)"
    with new_test_table(cql, test_keyspace, schema, order) as table:
        yield table

# A multi-column equality names a single row, and UPDATE and DELETE are
# allowed to use it. The clustering bounds of a table whose clustering
# columns are all descending are computed in the natural column order and
# then reversed, and it is easy for the reversal to lose the fact that the
# bounds name a single row rather than a range of them - which is all these
# statements have to go by when they decide to accept the WHERE clause.
def test_multi_column_eq_write_all_desc(cql, table_all_desc):
    p = unique_key_int()
    cql.execute(f'UPDATE {table_all_desc} SET v = 1 WHERE p = {p} AND (c1, c2) = (1, 2)')
    assert [(1, 2, 1)] == list(cql.execute(f'SELECT c1, c2, v FROM {table_all_desc} WHERE p = {p}'))
    cql.execute(f'DELETE FROM {table_all_desc} WHERE p = {p} AND (c1, c2) = (1, 2)')
    assert [] == list(cql.execute(f'SELECT c1, c2, v FROM {table_all_desc} WHERE p = {p}'))

# Same as test_multi_column_eq_write_all_desc(), for a multi-column IN
# naming several single rows.
def test_multi_column_in_write_all_desc(cql, table_all_desc):
    p = unique_key_int()
    cql.execute(f'UPDATE {table_all_desc} SET v = 1 WHERE p = {p} AND (c1, c2) IN ((1, 2), (3, 4))')
    assert [(3, 4, 1), (1, 2, 1)] == list(cql.execute(f'SELECT c1, c2, v FROM {table_all_desc} WHERE p = {p}'))
    cql.execute(f'DELETE FROM {table_all_desc} WHERE p = {p} AND (c1, c2) IN ((1, 2), (3, 4))')
    assert [] == list(cql.execute(f'SELECT c1, c2, v FROM {table_all_desc} WHERE p = {p}'))

# Same as test_multi_column_eq_write_all_desc(), for a table that mixes
# ascending and descending clustering columns, and so computes its bounds
# in yet another way.
def test_multi_column_eq_write_mixed_order(cql, table2):
    p = unique_key_int()
    cql.execute(f'UPDATE {table2} SET v = 1 WHERE p = {p} AND (c1, c2) = (1, 2)')
    assert [(1, 2, 1)] == list(cql.execute(f'SELECT c1, c2, v FROM {table2} WHERE p = {p}'))
    cql.execute(f'DELETE FROM {table2} WHERE p = {p} AND (c1, c2) = (1, 2)')
    assert [] == list(cql.execute(f'SELECT c1, c2, v FROM {table2} WHERE p = {p}'))

# Same as test_multi_column_eq_write_all_desc(), for a table whose
# clustering columns are all ascending, so that no reordering of the
# bounds is needed at all.
def test_multi_column_eq_write_all_asc(cql, table_all_asc):
    p = unique_key_int()
    cql.execute(f'UPDATE {table_all_asc} SET v = 1 WHERE p = {p} AND (c1, c2) = (1, 2)')
    assert [(1, 2, 1)] == list(cql.execute(f'SELECT c1, c2, v FROM {table_all_asc} WHERE p = {p}'))
    cql.execute(f'DELETE FROM {table_all_asc} WHERE p = {p} AND (c1, c2) = (1, 2)')
    assert [] == list(cql.execute(f'SELECT c1, c2, v FROM {table_all_asc} WHERE p = {p}'))

# Test a reversed query (ORDER BY DESC) with LIMIT that reads rows and a
# range tombstone deleting some of them, where the range tombstone is already
# old enough to be garbage-collected. The query should not return the deleted
# rows, and neither should later reads of the same partition. Scylla's row
# cache may drop such a purgeable tombstone when it is read, but it must then
# drop the rows that the tombstone deleted as well.
# Reproduces SCYLLADB-5187 (reversed read with LIMIT drops a purgeable range
# tombstone from the row cache, resurrecting the deleted rows).
def test_reversed_read_purgeable_range_tombstone(cql, test_keyspace):
    schema = "p int, c int, PRIMARY KEY (p, c)"
    with new_test_table(cql, test_keyspace, schema, "WITH gc_grace_seconds = 0") as table:
        for c in range(5):
            cql.execute(f"INSERT INTO {table} (p, c) VALUES (0, {c})")
        # Flush the rows before deleting some of them, so that the deleted rows
        # aren't simply dropped when the deletion is flushed. In Scylla, both
        # flushes also add their data to the row cache, so the cache holds
        # both the rows and the range tombstone deleting some of them.
        nodetool.flush(cql, table)
        cql.execute(f"DELETE FROM {table} WHERE p = 0 AND c >= 1 AND c <= 3")
        nodetool.flush(cql, table)
        # A tombstone's deletion time has a resolution of one second, so the
        # tombstone becomes purgeable (with gc_grace_seconds = 0, or Scylla's
        # tombstone_gc "repair" mode with RF=1) only in the next second.
        sleep_till_whole_second()
        assert list(cql.execute(f"SELECT c FROM {table} WHERE p = 0 AND c <= 2 ORDER BY c DESC LIMIT 1")) == [(0,)]
        assert list(cql.execute(f"SELECT c FROM {table} WHERE p = 0")) == [(0,), (4,)]

#############################################################################
# The following tests were translated from scylla-dtest's
# reversed_queries_test.py.
#############################################################################

# A table with partitions 1 to 5, where partition 1 has range tombstones
# covering some of its rows, for the tests of reversed (ORDER BY DESC)
# queries below.
@contextmanager
def table_for_reversed_selects(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'bucket int, id int, id2 int, value text, PRIMARY KEY (bucket, id, id2)') as table:
        insert = cql.prepare(f'INSERT INTO {table} (bucket, id, id2, value) VALUES (?, ?, ?, ?)')
        rows = [(1, 4), (2, 1), (2, 5), (2, 9), (2, 10), (3, 3), (4, 2), (4, 5), (4, 6), (5, 4), (6, 1), (6, 3), (7, 1), (8, 1)]
        for bucket in [1, 2]:
            for id, id2 in rows:
                cql.execute(insert, [bucket, id, id2, 'testing'])
        for bucket in [3, 4, 5]:
            cql.execute(insert, [bucket, 1, 1, 'testing'])
        cql.execute(f'DELETE FROM {table} WHERE bucket = 1 AND id < 0')
        cql.execute(f'DELETE FROM {table} WHERE bucket = 1 AND id = 2 AND id2 < 4')
        cql.execute(f'DELETE FROM {table} WHERE bucket = 1 AND id = 2 AND id2 > 9')
        cql.execute(f'DELETE FROM {table} WHERE bucket = 1 AND (id, id2) >= (4, 6) AND (id, id2) <= (6, 2)')
        cql.execute(f'DELETE FROM {table} WHERE bucket = 1 AND id >= 8')
        yield table

# Check reversed queries with various restrictions on the clustering key,
# on partition 1 of table_for_reversed_selects. "suffix" is appended to
# each query.
def check_reversed_single_partition_selects(cql, table, suffix=''):
    def select(where):
        return list(cql.execute(f'SELECT id, id2 FROM {table} WHERE bucket = 1 {where} ORDER BY id DESC {suffix}'))
    assert select('') == [(7, 1), (6, 3), (4, 5), (4, 2), (3, 3), (2, 9), (2, 5), (1, 4)]
    # Single column restrictions
    assert select('AND id = 2') == [(2, 9), (2, 5)]
    assert select('AND id IN (4, 2, 20)') == [(4, 5), (4, 2), (2, 9), (2, 5)]
    assert select('AND id < 3') == [(2, 9), (2, 5), (1, 4)]
    assert select('AND id > 4') == [(7, 1), (6, 3)]
    assert select('AND id > 3 AND id < 7') == [(6, 3), (4, 5), (4, 2)]
    # Multi-column restrictions
    assert select('AND (id, id2) IN ((2, 9), (7, 1))') == [(7, 1), (2, 9)]
    assert select('AND (id, id2) < (2, 8)') == [(2, 5), (1, 4)]
    assert select('AND (id, id2) > (3, 3)') == [(7, 1), (6, 3), (4, 5), (4, 2)]
    assert select('AND (id, id2) < (3, 5) AND (id, id2) > (1, 10)') == [(3, 3), (2, 9), (2, 5)]
    # Independent restrictions on both clustering columns
    assert select('AND id IN (3, 6) AND id2 IN (3, 4)') == [(6, 3), (3, 3)]

# Check reversed queries on partitions 1 and 2 of table_for_reversed_selects.
# Cassandra only allows ORDER BY with an IN restriction on the partition key
# when paging is disabled. CQL doesn't specify the order of "ties" - rows
# equal in all the ORDER BY columns - and Scylla and Cassandra order them
# differently (see #15099). Ties can come from different partitions, and
# when ORDER BY lists only "id", also from the same partition: for
# "ORDER BY id DESC", Scylla returns a partition's rows with the same id in
# ascending id2 order (e.g., (2, 6, 1) before (2, 6, 3)), while Cassandra
# returns them in descending id2 order, the reverse of the clustering order.
# The dtest expected Scylla's exact order. Here, each expected result is a
# list of groups of tied rows, and the order of rows within each group isn't
# checked.
def check_reversed_multi_partition_selects(cql, table, suffix=''):
    def select(order, nties):
        rows = [tuple(r) for r in cql.execute(SimpleStatement(
            f'SELECT bucket, id, id2 FROM {table} WHERE bucket IN (1, 2) AND id > 3 AND id < 7 ORDER BY {order} {suffix}',
            fetch_size=None))]
        # The ORDER BY columns are the last nties columns
        return [sorted(group) for _, group in itertools.groupby(rows, key=lambda r: r[1:1 + nties])]
    assert select('id DESC', 1) == [[(1, 6, 3), (2, 6, 1), (2, 6, 3)], [(2, 5, 4)], [(1, 4, 2), (1, 4, 5), (2, 4, 2), (2, 4, 5), (2, 4, 6)]]
    assert select('id DESC, id2 DESC', 2) == [[(1, 6, 3), (2, 6, 3)], [(2, 6, 1)], [(2, 5, 4)], [(2, 4, 6)], [(1, 4, 5), (2, 4, 5)], [(1, 4, 2), (2, 4, 2)]]
    assert select('id DESC, id2 DESC PER PARTITION LIMIT 3', 2) == [[(1, 6, 3), (2, 6, 3)], [(2, 6, 1)], [(2, 5, 4)], [(1, 4, 5)], [(1, 4, 2)]]

# Test reversed queries on a single partition with range tombstones, reading
# from the memtable and then, after a flush, from the sstable (and cache).
# Translated from scylla-dtest's reversed_queries_test.py::TestSinglePartitionReversedQueriesSelectors::test_reverse_selectors,
# except its reads with BYPASS CACHE, which are in
# test_reversed_selects_single_partition_bypass_cache below.
def test_reversed_selects_single_partition(cql, test_keyspace):
    with table_for_reversed_selects(cql, test_keyspace) as table:
        check_reversed_single_partition_selects(cql, table)
        nodetool.flush(cql, table)
        check_reversed_single_partition_selects(cql, table)

# Like test_reversed_selects_single_partition, reading the sstable with
# BYPASS CACHE, which is a Scylla extension.
# Translated from scylla-dtest's reversed_queries_test.py::TestSinglePartitionReversedQueriesSelectors::test_reverse_selectors.
def test_reversed_selects_single_partition_bypass_cache(cql, test_keyspace, scylla_only):
    with table_for_reversed_selects(cql, test_keyspace) as table:
        nodetool.flush(cql, table)
        check_reversed_single_partition_selects(cql, table, 'BYPASS CACHE')

# Test reversed queries on two partitions, one with range tombstones,
# reading from the memtable and then, after a flush, from the sstable (and
# cache).
# Translated from scylla-dtest's reversed_queries_test.py::TestMultiPartitionReversedQueriesSelectors::test_reverse_selectors,
# except its reads with BYPASS CACHE, which are in
# test_reversed_selects_multi_partition_bypass_cache below.
def test_reversed_selects_multi_partition(cql, test_keyspace):
    with table_for_reversed_selects(cql, test_keyspace) as table:
        check_reversed_multi_partition_selects(cql, table)
        nodetool.flush(cql, table)
        check_reversed_multi_partition_selects(cql, table)

# Like test_reversed_selects_multi_partition, reading the sstable with
# BYPASS CACHE, which is a Scylla extension.
# Translated from scylla-dtest's reversed_queries_test.py::TestMultiPartitionReversedQueriesSelectors::test_reverse_selectors.
def test_reversed_selects_multi_partition_bypass_cache(cql, test_keyspace, scylla_only):
    with table_for_reversed_selects(cql, test_keyspace) as table:
        nodetool.flush(cql, table)
        check_reversed_multi_partition_selects(cql, table, 'BYPASS CACHE')

# Test a reversed query (ORDER BY ASC on a table with CLUSTERING ORDER BY
# DESC) on a table using TimeWindowCompactionStrategy, whose rows are spread
# over five sstables in five different time windows.
# Translated from scylla-dtest's reversed_queries_test.py::TestReversedQueriesWithTimeWindowCompactionStrategy::test_reversed_queries_with_time_window_compaction_strategy.
# The dtest wrote 300 rows, one per second, and flushed every 60 rows. It
# meant to spread the rows over five one-minute windows, but its USING
# TIMESTAMP values were in seconds instead of microseconds, so all the rows
# were in the same window. This test writes 10 rows with timestamps in
# microseconds, two rows in each of five consecutive one-minute windows,
# and flushes after each window's rows. The dtest also checked that there
# are five sstables; with autocompaction disabled, this follows from the
# five flushes.
def test_reversed_query_twcs(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'pk int, ck int, v int, PRIMARY KEY (pk, ck)',
            "WITH CLUSTERING ORDER BY (ck DESC) AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'MINUTES', 'compaction_window_size': 1}") as table:
        with nodetool.no_autocompaction_context(cql, table):
            insert = cql.prepare(f'INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?')
            # The start of the one-minute window five minutes ago
            start = (int(time.time()) // 60 - 5) * 60
            for i in range(1, 11):
                window = (i - 1) // 2
                cql.execute(insert, [0, i, -i, (start + 60 * window + i % 2) * 1000000])
                if i % 2 == 0:
                    nodetool.flush(cql, table)
            assert list(cql.execute(f'SELECT ck, v FROM {table} WHERE pk = 0 ORDER BY ck ASC')) == [(i, -i) for i in range(1, 11)]
