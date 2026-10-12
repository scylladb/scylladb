# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Various tests for Scylla's ttl feature - USING TTL and DEFAULT_TIME_TO_LIVE
#############################################################################

from test import DEBUG_MODES
from .util import new_named_table, new_test_keyspace, new_test_table, unique_key_int, config_value_context
from cassandra.protocol import InvalidRequest
from cassandra.query import UNSET_VALUE, SimpleStatement
from cassandra.util import sortedset
from .nodetool import flush
from cassandra import ConsistencyLevel
from collections import OrderedDict
import contextlib
import math
import pytest
import time

# Fixture with a table with a default TTL set to 1, so new data would be
# inserted by default with TTL of 1.
@pytest.fixture(scope="module")
def table_ttl_1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int primary key, v int', 'with default_time_to_live = 1') as table:
        yield table

@pytest.fixture(scope="module")
def table_ttl_100(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int primary key, v int', 'with default_time_to_live = 100') as table:
        yield table


# Basic test that data inserted *without* an explicit ttl into a table with
# default TTL inherits this TTL, but an explicit TTL overrides it.
def test_basic_default_ttl(cql, table_ttl_1):
    p1 = unique_key_int()
    p2 = unique_key_int()
    cql.execute(f'INSERT INTO {table_ttl_1} (p, v) VALUES ({p1}, 1) USING TTL 1000')
    cql.execute(f'INSERT INTO {table_ttl_1} (p, v) VALUES ({p2}, 1)')
    # p2 item should expire in *less* than one second (it will expire
    # in the next whole second), 
    start = time.time()
    while len(list(cql.execute(f'SELECT * from {table_ttl_1} where p={p2}'))):
        assert time.time() < start + 2
        time.sleep(0.1)
    # p1 should not have expired yet. By the way, its current ttl(v) would
    # normally be exactly 999 now, but theoretically could be a bit lower in
    # case of delays in the test.
    assert len(list(cql.execute(f'SELECT *from {table_ttl_1} where p={p1}'))) == 1

# Above we tested that explicitly setting "using ttl" overrides the default
# ttl set for the table. Here we check that also in the special case of
# "using ttl 0" (which means the item should never expire) it also overrides
# the default TTL. Reproduces issue #9842.
def test_default_ttl_0_override(cql, table_ttl_100):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table_ttl_100} (p, v) VALUES ({p}, 1) USING TTL 0')
    # We can immediately check that this item's TTL is "null", meaning it
    # will never expire. There's no need to have any sleeps.
    assert list(cql.execute(f'SELECT ttl(v) from {table_ttl_100} where p={p}')) == [(None,)]

# Whereas a zero TTL overrides the default TTL, an unset TTL doesn't - and
# leaves the default TTL. This means that the text in Cassandra's NEWS.txt
# which claims that "an unset bind ttl is treated as 'unlimited'" is not
# accurate - it's only unlimited if there is no default TTL for the table,
# and if there is, this default TTL is used instead of unlimited. In other
# words, an UNSET_VALUE TTL behaves exactly like not specifying a TTL at all.
# See also test_unset.py::test_unset_ttl()
def test_default_ttl_unset_override(cql, table_ttl_100):
    p = unique_key_int()
    stmt = cql.prepare(f'INSERT INTO {table_ttl_100} (p, v) VALUES ({p}, 1) USING TTL ?')
    cql.execute(stmt, [UNSET_VALUE])
    # At this point, reading ttl(v) would normally return 99, but it's
    # possible in case of delays to be somewhat lower. But it wouldn't
    # be None - which would mean that the item will never expire.
    assert list(cql.execute(f'SELECT ttl(v) from {table_ttl_100} where p={p}')) != [(None,)]


# Tests ported from scylla-dtest's cql_additional_tests.py. Like the dtests,
# they run in a keyspace with vnodes, and keep the dtests' table names.

# writetime() and ttl() of a column with and without TTL, also wrapped in
# other functions (CASSANDRA-8451); writetime() of a key column is rejected,
# and of a missing value is null.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_writetime_and_ttl_functions(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "test", "k int PRIMARY KEY, c text, d text") as table:
        cql.execute(f"INSERT INTO {table} (k, c) VALUES (1, 'test')")
        cql.execute(f"INSERT INTO {table} (k, c) VALUES (2, 'test') USING TTL 400")
        for select in [f"SELECT k, c, writetime(c), ttl(c) FROM {table}",
                       f"SELECT k, c, blobAsBigint(bigintAsBlob(writetime(c))), ttl(c) FROM {table}",
                       f"SELECT k, c, writetime(c), blobAsInt(intAsBlob(ttl(c))) FROM {table}"]:
            res = list(cql.execute(select))
            assert len(res) == 2
            for r in res:
                assert isinstance(r[2], int)
                if r[0] == 1:
                    assert r[3] is None
                else:
                    assert isinstance(r[3], int)
        with pytest.raises(InvalidRequest):
            cql.execute(f"SELECT k, c, writetime(k) FROM {table}")
        assert list(cql.execute(f"SELECT k, d, writetime(d) FROM {table} WHERE k = 1")) == [(1, None, None)]


#############################################################################
# Tests ported from scylla-dtest ttl_test.py. As in the dtests, the tables
# are in a vnodes keyspace, rows are counted with SELECT COUNT(*), and in
# the end the table is flushed (as a stopping dtest node does), so the
# expired data is also written to sstables, and checked again.
#############################################################################

def ttl_table(cql, keyspace, default_ttl=None, schema='key int primary key, col1 int, col2 int, col3 int', extra=''):
    if default_ttl:
        extra = f' WITH default_time_to_live = {default_ttl}' + (f' AND {extra}' if extra else '')
    elif extra:
        extra = f' WITH {extra}'
    return new_test_table(cql, keyspace, schema, extra)

def row_count(cql, table):
    return cql.execute(f'SELECT COUNT(*) FROM {table}').one()[0]

def rows(cql, table):
    return [list(r) for r in cql.execute(f'SELECT * FROM {table}')]

# Waits until cond() is true and returns the time it took since `start`.
def wait_until(cond, start, timeout=60):
    while not cond():
        assert time.time() < start + timeout, 'Timed out waiting'
        time.sleep(0.1)
    return time.time() - start

def flush_and_check(cql, table, expected_rows):
    flush(cql, table)
    assert rows(cql, table) == expected_rows
    assert row_count(cql, table) == len(expected_rows)

# Rows inserted into a table with a default TTL expire.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_default_ttl_expires_rows(cql, test_keyspace_vnodes):
    with ttl_table(cql, test_keyspace_vnodes, default_ttl=1) as table:
        start = time.time()
        for i in range(1, 4):
            cql.execute(f'INSERT INTO {table} (key, col1) VALUES ({i}, {i})')
        wait_until(lambda: row_count(cql, table) == 0, start)
        flush_and_check(cql, table, [])

# A TTL set on some list elements applies to them only; the rest of the
# list expires by the table's default TTL.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_collection_list_ttl(cql, test_keyspace_vnodes):
    with ttl_table(cql, test_keyspace_vnodes, default_ttl=5, schema='key int primary key, col1 int, col2 int, col3 int, mylist list<int>') as table:
        start = time.time()
        cql.execute(f'INSERT INTO {table} (key, col1, mylist) VALUES (1, 1, [1, 2, 3, 4, 5])')
        cql.execute(f'UPDATE {table} USING TTL 2 SET mylist[0] = 42, mylist[4] = 42 WHERE key=1')
        assert rows(cql, table) == [[1, 1, None, None, [42, 2, 3, 4, 42]]]
        wait_until(lambda: rows(cql, table) != [[1, 1, None, None, [42, 2, 3, 4, 42]]], start)
        assert rows(cql, table) == [[1, 1, None, None, [2, 3, 4]]]
        wait_until(lambda: row_count(cql, table) == 0, start)
        flush_and_check(cql, table, [])

# Rows overwritten with different TTLs expire by the TTL of their latest
# write, for each compaction strategy. Like the dtest, the test first drops
# the default table, with auto_snapshot on as on a dtest node.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_overlapped_rows_ttls(cql, test_keyspace_vnodes):
    strategies = ['LeveledCompactionStrategy', 'SizeTieredCompactionStrategy', 'TimeWindowCompactionStrategy', 'IncrementalCompactionStrategy']
    with config_value_context(cql, 'auto_snapshot', 'true'):
        with ttl_table(cql, test_keyspace_vnodes) as table:
            cql.execute(f'INSERT INTO {table} (key, col1) VALUES (1, 1)')
    with contextlib.ExitStack() as stack:
        tables = [stack.enter_context(new_test_table(cql, test_keyspace_vnodes, 'key int, col1 int, col2 int, col3 int, PRIMARY KEY (key, col1)',
                                                     f"WITH compaction = {{'class': '{s}'}}")) for s in strategies]
        def insert(start, end, ttl=None):
            for table in tables:
                for i in range(start, end + 1):
                    cql.execute(f'INSERT INTO {table} (key, col1, col2, col3) VALUES ({i}, {i}, {i}, {i})' + (f' USING TTL {ttl}' if ttl else ''))
            return time.time()
        insert(1, 20)
        for table in tables:
            assert row_count(cql, table) == 20
        # (TTL, write time, keys left once it passed), for the writes in order
        steps = [(12, insert(5, 10, 12), [i for i in range(1, 21) if i not in [5, 6, 7, 8, 10]]),
                 (17, insert(9, 13, 17), [i for i in range(1, 21) if i < 5 or i > 10]),
                 (5, insert(10, 11, 5), [i for i in range(1, 21) if i != 10]),
                 (22, insert(11, 15, 22), [i for i in range(1, 21) if i < 5 or i > 15])]
        for ttl, written, keys in sorted(steps):
            while time.time() < written + ttl + 1:
                time.sleep(0.1)
            for table in tables:
                actual = sorted(r.key for r in cql.execute(SimpleStatement(f'SELECT key FROM {table}', consistency_level=ConsistencyLevel.QUORUM)))
                assert actual == keys
        for table in tables:
            flush(cql, table)
            assert sorted(r.key for r in cql.execute(f'SELECT key FROM {table}')) == steps[3][2]


#############################################################################
# Tests ported one per dtest from scylla-dtest ttl_test.py::TestTTL. Like the
# dtests' prepare(), each test creates its own keyspace (RF=1, vnodes as on
# the dtest nodes), drops ttl_table if it exists and creates it
# with the dtest's statement - the default one ends its column list with a
# comma - and the dtest's default_time_to_live. Rows are counted with SELECT
# count(*) at CL=ONE, as the dtests' assert_row_count() does, and the waits
# are the dtests' smart_sleep(): until the given time since a start passed.
#############################################################################

DTEST_TTL_TABLE = 'CREATE TABLE {table} (key int primary key, col1 int, col2 int, col3 int, )'

@contextlib.contextmanager
def dtest_ttl_prepare(cql, has_tablets, default_time_to_live=None, create_table_statement=DTEST_TTL_TABLE):
    """The dtests' TestTTL.prepare(): yields the keyspace, with ttl_table in it."""
    tablets = " AND tablets = {'enabled': false}" if has_tablets else ''
    with new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}" + tablets) as ks:
        table = f'{ks}.ttl_table'
        cql.execute(f'DROP TABLE IF EXISTS {table}')
        query = create_table_statement.format(table=table)
        if default_time_to_live:
            query += f' WITH default_time_to_live = {default_time_to_live};'
        cql.execute(query)
        yield ks

def smart_sleep(start_time, time_to_wait):
    real_time_to_wait = time_to_wait - (time.time() - start_time)
    if real_time_to_wait > 0:
        time.sleep(real_time_to_wait)

def dtest_rows(cql, query, cl=ConsistencyLevel.ONE):
    return [list(r) for r in cql.execute(SimpleStatement(query, consistency_level=cl))]

def assert_all(cql, query, expected):
    res = dtest_rows(cql, query)
    assert res == expected, f'Expected {expected} from {query}, but got {res}'

def assert_row_count(cql, table, expected):
    count = dtest_rows(cql, f'SELECT count(*) FROM {table}')[0][0]
    assert count == expected, f"Expected a row count of {expected} in table '{table}', but got {count}"

def eventually_expires(cql, expected, before, after, ttl, table):
    """The dtests' eventually_expires(): TTLed data expires within +/- 1
    second of the TTL, and keeps its values until then (#5290)."""
    smart_sleep(before, ttl - 2)
    query = f'SELECT * FROM {table}'
    list_res = dtest_rows(cql, query)
    assert list_res == expected, f'Expected {expected} from {query}, but got {list_res}'
    while time.time() - math.ceil(after) < ttl + 1:
        list_res = dtest_rows(cql, query)
        if not list_res:
            assert time.time() - math.floor(before) >= ttl - 1, 'Data has expired prematurely'
            break
        assert list_res == expected, f'Expected {expected} from {query}, but got {list_res}'
        time.sleep(0.1)
    if list_res:
        list_res = dtest_rows(cql, query)
        assert not list_res, f'Expected [] from {query}, but got {list_res}'
    assert_row_count(cql, table, 0)

# Ported from scylla-dtest ttl_test.py::TestTTL::test_insert_ttl_has_priority_on_defaut_ttl.
# A TTL given in an INSERT has priority over the table's default TTL.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_insert_ttl_has_priority_on_default_ttl(cql, has_tablets):
    with dtest_ttl_prepare(cql, has_tablets, default_time_to_live=1) as ks:
        # The dtest's release-mode TTL (10 s in debug mode, where it does not run).
        ttl = 4
        before = time.time()
        cql.execute(f'INSERT INTO {ks}.ttl_table (key, col1, col2, col3) VALUES (1, 1, 1, 1) USING TTL {ttl};')
        after = time.time()
        eventually_expires(cql, [[1, 1, 1, 1]], before, after, ttl, f'{ks}.ttl_table')

# Ported from scylla-dtest ttl_test.py::TestTTL::test_insert_ttl_works_without_default_ttl.
# A TTL given in an INSERT works in a table without a default TTL.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_insert_ttl_works_without_default_ttl(cql, has_tablets):
    with dtest_ttl_prepare(cql, has_tablets) as ks:
        # The dtest's release-mode TTL (10 s in debug mode, where it does not run).
        ttl = 4
        before = time.time()
        cql.execute(f'INSERT INTO {ks}.ttl_table (key, col1, col2, col3) VALUES (1, 1, 1, 1) USING TTL {ttl};')
        after = time.time()
        eventually_expires(cql, [[1, 1, 1, 1]], before, after, ttl, f'{ks}.ttl_table')

# Ported from scylla-dtest ttl_test.py::TestTTL::test_default_ttl_can_be_removed.
# After the table's default TTL is set to 0, new rows do not expire.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_default_ttl_can_be_removed(cql, has_tablets):
    with dtest_ttl_prepare(cql, has_tablets, default_time_to_live=1) as ks:
        table = f'{ks}.ttl_table'
        start = time.time()
        cql.execute(f'ALTER TABLE {table} WITH default_time_to_live = 0;')
        cql.execute(f'INSERT INTO {table} (key, col1) VALUES (1, 1);')
        smart_sleep(start, 1.5)
        assert_row_count(cql, table, 1)

# Ported from scylla-dtest ttl_test.py::TestTTL::test_removing_default_ttl_does_not_affect_existing_rows.
# Rows written with the default TTL of 10 or with USING TTL 30 still expire
# by them after the default TTL was removed; a row written afterwards stays.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_removing_default_ttl_does_not_affect_existing_rows(cql, has_tablets):
    with dtest_ttl_prepare(cql, has_tablets, default_time_to_live=1) as ks:
        table = f'{ks}.ttl_table'
        ttl1 = 10
        cql.execute(f'ALTER TABLE {table} WITH default_time_to_live = {ttl1};')
        start1 = time.time()
        cql.execute(f'INSERT INTO {table} (key, col1) VALUES (1, 1);')
        ttl2 = 30
        start2 = time.time()
        cql.execute(f'INSERT INTO {table} (key, col1) VALUES (2, 1) USING TTL {ttl2};')
        cql.execute(f'ALTER TABLE {table} WITH default_time_to_live = 0;')
        cql.execute(f'INSERT INTO {table} (key, col1) VALUES (3, 1);')
        smart_sleep(start1, ttl1 // 2)
        assert_row_count(cql, table, 3)
        smart_sleep(start1, ttl1 + 2)
        assert_row_count(cql, table, 2)
        smart_sleep(start2, ttl2 + 2)
        assert_row_count(cql, table, 1)

# Ported from scylla-dtest ttl_test.py::TestTTL::test_update_single_column_ttl.
# A TTL given in an UPDATE of one column applies to that column only.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_update_single_column_ttl(cql, has_tablets):
    with dtest_ttl_prepare(cql, has_tablets) as ks:
        table = f'{ks}.ttl_table'
        cql.execute(f'INSERT INTO {table} (key, col1, col2, col3) VALUES (1, 1, 1, 1);')
        start = time.time()
        cql.execute(f'UPDATE {table} USING TTL 3 set col1=42 where key=1;')
        assert_all(cql, f'SELECT * FROM {table};', [[1, 42, 1, 1]])
        smart_sleep(start, 5)
        assert_all(cql, f'SELECT * FROM {table};', [[1, None, 1, 1]])

# Ported from scylla-dtest ttl_test.py::TestTTL::test_update_multiple_columns_ttl.
# A TTL given in an UPDATE of several columns applies to all of them.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_update_multiple_columns_ttl(cql, has_tablets):
    with dtest_ttl_prepare(cql, has_tablets) as ks:
        table = f'{ks}.ttl_table'
        cql.execute(f'INSERT INTO {table} (key, col1, col2, col3) VALUES (1, 1, 1, 1);')
        start = time.time()
        cql.execute(f'UPDATE {table} USING TTL 2 set col1=42, col2=42, col3=42 where key=1;')
        assert_all(cql, f'SELECT * FROM {table};', [[1, 42, 42, 42]])
        smart_sleep(start, 4)
        assert_all(cql, f'SELECT * FROM {table};', [[1, None, None, None]])

# Ported from scylla-dtest ttl_test.py::TestTTL::test_update_column_ttl_with_default_ttl.
# A column TTL lower than the table's default TTL: the column expires first,
# then the rest of the row by the default TTL.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_update_column_ttl_with_default_ttl(cql, has_tablets):
    with dtest_ttl_prepare(cql, has_tablets, default_time_to_live=8) as ks:
        table = f'{ks}.ttl_table'
        start = time.time()
        cql.execute(f'INSERT INTO {table} (key, col1, col2, col3) VALUES (1, 1, 1, 1);')
        cql.execute(f'UPDATE {table} USING TTL 3 set col1=42 where key=1;')
        assert_all(cql, f'SELECT * FROM {table};', [[1, 42, 1, 1]])
        smart_sleep(start, 5)
        assert_all(cql, f'SELECT * FROM {table};', [[1, None, 1, 1]])
        smart_sleep(start, 10)
        assert_row_count(cql, table, 0)

# Ported from scylla-dtest ttl_test.py::TestTTL::test_update_column_ttl_with_default_ttl_2.
# A column TTL higher than the table's default TTL: the rest of the row
# expires first, then the column.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_update_column_ttl_with_default_ttl_2(cql, has_tablets):
    with dtest_ttl_prepare(cql, has_tablets, default_time_to_live=2) as ks:
        table = f'{ks}.ttl_table'
        start = time.time()
        cql.execute(f'INSERT INTO {table} (key, col1, col2, col3) VALUES (1, 1, 1, 1);')
        cql.execute(f'UPDATE {table} USING TTL 6 set col1=42 where key=1;')
        smart_sleep(start, 4)
        assert_all(cql, f'SELECT * FROM {table};', [[1, 42, None, None]])
        smart_sleep(start, 8)
        assert_row_count(cql, table, 0)

# Ported from scylla-dtest ttl_test.py::TestTTL::test_remove_column_ttl.
# An UPDATE without TTL removes the TTL of the column it sets.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_remove_column_ttl(cql, has_tablets):
    with dtest_ttl_prepare(cql, has_tablets) as ks:
        table = f'{ks}.ttl_table'
        start = time.time()
        cql.execute(f'INSERT INTO {table} (key, col1, col2, col3) VALUES (1, 1, 1, 1) USING TTL 2;')
        cql.execute(f'UPDATE {table} set col1=42 where key=1;')
        smart_sleep(start, 4)
        assert_all(cql, f'SELECT * FROM {table};', [[1, 42, None, None]])

# Ported from scylla-dtest ttl_test.py::TestTTL::test_collection_set_ttl.
# A TTL given to an element added to a set applies to that element only.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_collection_set_ttl(cql, has_tablets):
    cts = 'CREATE TABLE {table} (key int primary key, col1 int, col2 int, col3 int, myset set<int>)'
    with dtest_ttl_prepare(cql, has_tablets, default_time_to_live=10, create_table_statement=cts) as ks:
        table = f'{ks}.ttl_table'
        start = time.time()
        cql.execute(f'INSERT INTO {table} (key, col1, myset) VALUES (1, 1, {{1,2,3,4,5}});')
        cql.execute(f'UPDATE {table} USING TTL 3 SET myset = myset + {{42}} WHERE key=1;')
        assert_all(cql, f'SELECT * FROM {table};', [[1, 1, None, None, sortedset([1, 2, 3, 4, 5, 42])]])
        smart_sleep(start, 5)
        assert_all(cql, f'SELECT * FROM {table};', [[1, 1, None, None, sortedset([1, 2, 3, 4, 5])]])
        smart_sleep(start, 12)
        assert_row_count(cql, table, 0)

# Ported from scylla-dtest ttl_test.py::TestTTL::test_collection_map_ttl.
# A TTL given to overwritten map entries applies to these entries only.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_collection_map_ttl(cql, has_tablets):
    cts = 'CREATE TABLE {table} (key int primary key, col1 int, col2 int, col3 int, mymap map<int, int>)'
    with dtest_ttl_prepare(cql, has_tablets, default_time_to_live=6, create_table_statement=cts) as ks:
        table = f'{ks}.ttl_table'
        start = time.time()
        cql.execute(f'INSERT INTO {table} (key, col1, mymap) VALUES (1, 1, {{1:1,2:2,3:3,4:4,5:5}});')
        cql.execute(f'UPDATE {table} USING TTL 2 SET mymap[1] = 42, mymap[5] = 42 WHERE key=1;')
        assert_all(cql, f'SELECT * FROM {table};', [[1, 1, None, None, OrderedDict([(1, 42), (2, 2), (3, 3), (4, 4), (5, 42)])]])
        smart_sleep(start, 4)
        assert_all(cql, f'SELECT * FROM {table};', [[1, 1, None, None, OrderedDict([(2, 2), (3, 3), (4, 4)])]])
        smart_sleep(start, 8)
        assert_row_count(cql, table, 0)

# Ported from scylla-dtest ttl_test.py::TestTTL::test_delete_with_ttl_expired.
# A row with an expired column can be deleted (CASSANDRA-6363). As in the
# dtest, the DELETE is not conditional.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_delete_with_ttl_expired(cql, has_tablets):
    with dtest_ttl_prepare(cql, has_tablets) as ks:
        table = f'{ks}.session'
        cql.execute(f'CREATE TABLE {table} (id text, usr text, valid int, PRIMARY KEY (id))')
        cql.execute(f"insert into {table} (id, usr) values ('abc', 'abc')")
        cql.execute(f"update {table} using ttl 1 set valid = 1 where id = 'abc'")
        smart_sleep(time.time(), 2)
        cql.execute(f"delete from {table} where id = 'abc'")
        assert_row_count(cql, table, 0)

# Ported from scylla-dtest ttl_test.py::TestTTL::test_boundary_ttl.
# TTLs out of range are rejected: more than a signed int, more than the
# maximum of 20 years; the maximum, the largest TTL whose expiration still
# fits in the deletion time and one more are accepted.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_boundary_ttl(cql, has_tablets):
    with dtest_ttl_prepare(cql, has_tablets) as ks:
        table = f'{ks}.session'
        cql.execute(f'CREATE TABLE {table} (id text, usr text, valid int, PRIMARY KEY (id))')
        with pytest.raises(InvalidRequest):
            cql.execute(f"insert into {table} (id, usr) values ('abc', 'abc') USING TTL 2147483648")
        with pytest.raises(InvalidRequest):
            cql.execute(f"insert into {table} (id, usr) values ('abc', 'abc') USING TTL 2147483647")
        max_ttl = 630720000
        with pytest.raises(InvalidRequest, match='ttl is too large'):
            cql.execute(f"insert into {table} (id, usr) values ('abc', 'abc') USING TTL {max_ttl + 1}")
        assert_row_count(cql, table, 0)
        cql.execute(f"insert into {table} (id, usr) values ('abc', 'abc') USING TTL {max_ttl}")
        assert_row_count(cql, table, 1)

        max_delete_time = 2**31 - 1
        start_time = time.time()
        boundary_ttl = max_delete_time - int(start_time)
        cql.execute(f"insert into {table} (id, usr) values ('abc', 'abc') USING TTL {boundary_ttl}")
        assert_row_count(cql, table, 1)
        smart_sleep(start_time, 10)
        assert_row_count(cql, table, 1)

        start_time = time.time()
        boundary_ttl = max_delete_time - int(start_time)
        cql.execute(f"insert into {table} (id, usr) values ('def', 'def') USING TTL {boundary_ttl + 1}")
        assert_row_count(cql, table, 2)

        start_time = time.time()
        cql.execute(f"insert into {table} (id, usr) values ('abc', 'abc') USING TTL 5")
        smart_sleep(start_time, 10)
        assert_row_count(cql, table, 1)
