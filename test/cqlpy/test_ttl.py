# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Various tests for Scylla's ttl feature - USING TTL and DEFAULT_TIME_TO_LIVE
#############################################################################

from .util import new_test_table, unique_key_int, is_scylla, is_cassandra_older_than
from test.pylib.skip_types import skip_env
from cassandra.protocol import InvalidRequest
from cassandra.query import UNSET_VALUE
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

#############################################################################
# The following tests were translated from scylla-dtest's ttl_test.py::TestTTL
# (which originally came from Cassandra's dtest). The dtests waited for TTLs
# to expire with real sleeps of up to 32 seconds; these tests use the "clock"
# fixture to move the server's clock forward instead, so on Scylla they don't
# sleep at all (on Cassandra, the clock fixture still really sleeps).
# Real time passes while a test runs, in addition to the clock's jumps - and
# a slow (e.g., debug) build may take a few seconds. So each check that data
# is still alive is done at least 5 seconds before its TTL. A check that data
# expired can be done right after the TTL, because the real time which
# passes only brings the expiration closer.
#############################################################################

# Moves the server's clock, using the clock fixture, to given times - in
# seconds since the timeline was created, which is right before the test's
# writes.
class Timeline:
    def __init__(self, clock):
        self.clock = clock
        self.now = 0

    def at(self, t):
        assert t >= self.now
        self.clock.jump(t - self.now)
        self.now = t

def count_rows(cql, table):
    return cql.execute(f'SELECT count(*) FROM {table}').one()[0]

def rows(cql, table):
    return list(cql.execute(f'SELECT * FROM {table}'))

TTL_TABLE_SCHEMA = 'key int PRIMARY KEY, col1 int, col2 int, col3 int'

def ttl_table(cql, test_keyspace, default_ttl=None, schema=TTL_TABLE_SCHEMA):
    return new_test_table(cql, test_keyspace, schema, f'WITH default_time_to_live = {default_ttl}' if default_ttl else '')

# Test that rows written to a table with default_time_to_live expire.
def test_default_ttl_expires(cql, test_keyspace, clock):
    with ttl_table(cql, test_keyspace, default_ttl=1) as table:
        t = Timeline(clock)
        for i in range(1, 4):
            cql.execute(f'INSERT INTO {table} (key, col1) VALUES ({i}, {i})')
        t.at(3)
        assert count_rows(cql, table) == 0

# Check that the row inserted with USING TTL ttl right after the timeline was
# created is still there, with all its values, at half its TTL, and is gone a
# second after it. Reproduces issue #5290, where data expired by TTL was
# returned with null values for a short time before it disappeared - so a
# second before the TTL, the row must be either whole or gone. It may already
# be gone then, because real time passes while the test runs, in addition to
# the clock's jumps, and the server's clock has a resolution of one second.
def check_eventually_expires(cql, table, t, expected, ttl):
    t.at(ttl // 2)
    assert rows(cql, table) == expected
    t.at(ttl - 1)
    assert rows(cql, table) in (expected, [])
    t.at(ttl + 1)
    assert rows(cql, table) == []

# Test that a USING TTL in an INSERT takes priority over the table's
# default_time_to_live.
def test_insert_ttl_has_priority_on_default_ttl(cql, test_keyspace, clock):
    with ttl_table(cql, test_keyspace, default_ttl=1) as table:
        t = Timeline(clock)
        cql.execute(f'INSERT INTO {table} (key, col1, col2, col3) VALUES (1, 1, 1, 1) USING TTL 10')
        check_eventually_expires(cql, table, t, [(1, 1, 1, 1)], 10)

# Test that a USING TTL in an INSERT works when the table has no
# default_time_to_live.
def test_insert_ttl_works_without_default_ttl(cql, test_keyspace, clock):
    with ttl_table(cql, test_keyspace) as table:
        t = Timeline(clock)
        cql.execute(f'INSERT INTO {table} (key, col1, col2, col3) VALUES (1, 1, 1, 1) USING TTL 10')
        check_eventually_expires(cql, table, t, [(1, 1, 1, 1)], 10)

# Test that default_time_to_live can be removed (set to 0), after which new
# rows don't expire.
def test_default_ttl_can_be_removed(cql, test_keyspace, clock):
    with ttl_table(cql, test_keyspace, default_ttl=1) as table:
        t = Timeline(clock)
        cql.execute(f'ALTER TABLE {table} WITH default_time_to_live = 0')
        cql.execute(f'INSERT INTO {table} (key, col1) VALUES (1, 1)')
        t.at(2)
        assert count_rows(cql, table) == 1

# Test that changing or removing default_time_to_live doesn't affect the
# expiration of rows written before the change: a row written with the old
# default TTL and a row written with an explicit TTL still expire on time,
# and a row written after the default TTL was removed doesn't expire.
def test_removing_default_ttl_does_not_affect_existing_rows(cql, test_keyspace, clock):
    with ttl_table(cql, test_keyspace, default_ttl=1) as table:
        cql.execute(f'ALTER TABLE {table} WITH default_time_to_live = 10')
        t = Timeline(clock)
        cql.execute(f'INSERT INTO {table} (key, col1) VALUES (1, 1)')
        cql.execute(f'INSERT INTO {table} (key, col1) VALUES (2, 1) USING TTL 30')
        cql.execute(f'ALTER TABLE {table} WITH default_time_to_live = 0')
        cql.execute(f'INSERT INTO {table} (key, col1) VALUES (3, 1)')
        t.at(5)
        assert count_rows(cql, table) == 3
        t.at(12)
        assert count_rows(cql, table) == 2
        t.at(32)
        assert count_rows(cql, table) == 1

# Test that a USING TTL on an UPDATE of a single column expires only that
# column's value.
def test_update_single_column_ttl(cql, test_keyspace, clock):
    with ttl_table(cql, test_keyspace) as table:
        cql.execute(f'INSERT INTO {table} (key, col1, col2, col3) VALUES (1, 1, 1, 1)')
        t = Timeline(clock)
        cql.execute(f'UPDATE {table} USING TTL 5 SET col1 = 42 WHERE key = 1')
        assert rows(cql, table) == [(1, 42, 1, 1)]
        t.at(6)
        assert rows(cql, table) == [(1, None, 1, 1)]

# Test that a USING TTL on an UPDATE of several columns expires their values.
def test_update_multiple_columns_ttl(cql, test_keyspace, clock):
    with ttl_table(cql, test_keyspace) as table:
        cql.execute(f'INSERT INTO {table} (key, col1, col2, col3) VALUES (1, 1, 1, 1)')
        t = Timeline(clock)
        cql.execute(f'UPDATE {table} USING TTL 5 SET col1 = 42, col2 = 42, col3 = 42 WHERE key = 1')
        assert rows(cql, table) == [(1, 42, 42, 42)]
        t.at(6)
        assert rows(cql, table) == [(1, None, None, None)]

# Test a USING TTL on an UPDATE of a column which is lower than the table's
# default_time_to_live: the column expires first, and the rest of the row
# expires with the default TTL.
def test_update_column_ttl_with_default_ttl(cql, test_keyspace, clock):
    with ttl_table(cql, test_keyspace, default_ttl=12) as table:
        t = Timeline(clock)
        cql.execute(f'INSERT INTO {table} (key, col1, col2, col3) VALUES (1, 1, 1, 1)')
        cql.execute(f'UPDATE {table} USING TTL 5 SET col1 = 42 WHERE key = 1')
        assert rows(cql, table) == [(1, 42, 1, 1)]
        t.at(6)
        assert rows(cql, table) == [(1, None, 1, 1)]
        t.at(13)
        assert count_rows(cql, table) == 0

# Test a USING TTL on an UPDATE of a column which is higher than the table's
# default_time_to_live: the rest of the row expires first, and the column -
# and with it the row - expires later.
def test_update_column_ttl_with_default_ttl_2(cql, test_keyspace, clock):
    with ttl_table(cql, test_keyspace, default_ttl=5) as table:
        t = Timeline(clock)
        cql.execute(f'INSERT INTO {table} (key, col1, col2, col3) VALUES (1, 1, 1, 1)')
        cql.execute(f'UPDATE {table} USING TTL 12 SET col1 = 42 WHERE key = 1')
        t.at(6)
        assert rows(cql, table) == [(1, 42, None, None)]
        t.at(13)
        assert count_rows(cql, table) == 0

# Test that an UPDATE without a TTL of a column which was written with a TTL
# removes the TTL from that column.
def test_remove_column_ttl(cql, test_keyspace, clock):
    with ttl_table(cql, test_keyspace) as table:
        t = Timeline(clock)
        cql.execute(f'INSERT INTO {table} (key, col1, col2, col3) VALUES (1, 1, 1, 1) USING TTL 2')
        cql.execute(f'UPDATE {table} SET col1 = 42 WHERE key = 1')
        t.at(4)
        assert rows(cql, table) == [(1, 42, None, None)]

# The following three tests check that TTL has the granularity of collection
# elements: elements set with a USING TTL expire alone, while the rest of the
# collection expires with the table's default_time_to_live.
def test_collection_list_ttl(cql, test_keyspace, clock):
    with ttl_table(cql, test_keyspace, default_ttl=12, schema=TTL_TABLE_SCHEMA + ', mylist list<int>') as table:
        t = Timeline(clock)
        cql.execute(f'INSERT INTO {table} (key, col1, mylist) VALUES (1, 1, [1, 2, 3, 4, 5])')
        cql.execute(f'UPDATE {table} USING TTL 5 SET mylist[0] = 42, mylist[4] = 42 WHERE key = 1')
        assert rows(cql, table) == [(1, 1, None, None, [42, 2, 3, 4, 42])]
        t.at(6)
        assert rows(cql, table) == [(1, 1, None, None, [2, 3, 4])]
        t.at(13)
        assert count_rows(cql, table) == 0

def test_collection_set_ttl(cql, test_keyspace, clock):
    with ttl_table(cql, test_keyspace, default_ttl=12, schema=TTL_TABLE_SCHEMA + ', myset set<int>') as table:
        t = Timeline(clock)
        cql.execute(f'INSERT INTO {table} (key, col1, myset) VALUES (1, 1, {{1, 2, 3, 4, 5}})')
        cql.execute(f'UPDATE {table} USING TTL 5 SET myset = myset + {{42}} WHERE key = 1')
        assert rows(cql, table) == [(1, 1, None, None, {1, 2, 3, 4, 5, 42})]
        t.at(6)
        assert rows(cql, table) == [(1, 1, None, None, {1, 2, 3, 4, 5})]
        t.at(13)
        assert count_rows(cql, table) == 0

def test_collection_map_ttl(cql, test_keyspace, clock):
    with ttl_table(cql, test_keyspace, default_ttl=12, schema=TTL_TABLE_SCHEMA + ', mymap map<int, int>') as table:
        t = Timeline(clock)
        cql.execute(f'INSERT INTO {table} (key, col1, mymap) VALUES (1, 1, {{1: 1, 2: 2, 3: 3, 4: 4, 5: 5}})')
        cql.execute(f'UPDATE {table} USING TTL 5 SET mymap[1] = 42, mymap[5] = 42 WHERE key = 1')
        assert rows(cql, table) == [(1, 1, None, None, {1: 42, 2: 2, 3: 3, 4: 4, 5: 42})]
        t.at(6)
        assert rows(cql, table) == [(1, 1, None, None, {2: 2, 3: 3, 4: 4})]
        t.at(13)
        assert count_rows(cql, table) == 0

# Test that a row with an expired column can be deleted (CASSANDRA-6363).
def test_delete_with_ttl_expired(cql, test_keyspace, clock):
    with new_test_table(cql, test_keyspace, 'id text PRIMARY KEY, usr text, valid int') as table:
        t = Timeline(clock)
        cql.execute(f"INSERT INTO {table} (id, usr) VALUES ('abc', 'abc')")
        cql.execute(f"UPDATE {table} USING TTL 1 SET valid = 1 WHERE id = 'abc'")
        t.at(2)
        cql.execute(f"DELETE FROM {table} WHERE id = 'abc'")
        assert count_rows(cql, table) == 0

# Test that TTLs at the boundaries are rejected: a TTL which doesn't fit in
# a 32-bit signed integer, or which is above the maximum of 20 years.
# Translated, together with test_boundary_ttl_beyond_2038 below, from the
# dtest test_boundary_ttl.
def test_boundary_ttl_invalid(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'id text PRIMARY KEY, usr text, valid int') as table:
        for ttl in [2**31, 2**31 - 1]:
            with pytest.raises(InvalidRequest):
                cql.execute(f"INSERT INTO {table} (id, usr) VALUES ('abc', 'abc') USING TTL {ttl}")
        max_ttl = 20 * 365 * 24 * 3600
        with pytest.raises(InvalidRequest, match='ttl is too large'):
            cql.execute(f"INSERT INTO {table} (id, usr) VALUES ('abc', 'abc') USING TTL {max_ttl + 1}")
        assert count_rows(cql, table) == 0

# Test that the maximum TTL of 20 years is accepted, and that a TTL which
# makes the expiration time reach - or even pass - the maximum 32-bit signed
# number of seconds since the epoch (in the year 2038) is accepted, and the
# row doesn't expire soon. Scylla has supported expiration times beyond the
# year 2038 since its gc_clock became 64-bit (#3353). Cassandra doesn't
# support them in its storage compatibility mode CASSANDRA_4 (the default in
# Cassandra 5.0), where it rejects such TTLs (CASSANDRA-14092), so this test
# is skipped on Cassandra in that mode.
# Translated, together with test_boundary_ttl_invalid above, from the dtest
# test_boundary_ttl.
def test_boundary_ttl_beyond_2038(cql, test_keyspace, clock):
    if not is_scylla(cql):
        # When storage_compatibility_mode isn't set in the configuration file,
        # system_views.settings shows it as "None", and the actual default is
        # CASSANDRA_4 in Cassandra 5.0 (and NONE in Cassandra 6).
        mode = cql.execute("SELECT value FROM system_views.settings WHERE name = 'storage_compatibility_mode'").one()
        mode = mode.value if mode else None
        if mode in (None, 'None'):
            mode = 'CASSANDRA_4' if is_cassandra_older_than(cql, (6, 0)) else 'NONE'
        if mode == 'CASSANDRA_4':
            skip_env("Cassandra's storage_compatibility_mode is CASSANDRA_4")
    with new_test_table(cql, test_keyspace, 'id text PRIMARY KEY, usr text, valid int') as table:
        t = Timeline(clock)
        max_ttl = 20 * 365 * 24 * 3600
        cql.execute(f"INSERT INTO {table} (id, usr) VALUES ('abc', 'abc') USING TTL {max_ttl}")
        assert count_rows(cql, table) == 1
        max_delete_time = 2**31 - 1
        cql.execute(f"INSERT INTO {table} (id, usr) VALUES ('abc', 'abc') USING TTL {max_delete_time - int(time.time())}")
        assert count_rows(cql, table) == 1
        t.at(10)
        assert count_rows(cql, table) == 1
        cql.execute(f"INSERT INTO {table} (id, usr) VALUES ('def', 'def') USING TTL {max_delete_time - int(time.time()) + 1}")
        assert count_rows(cql, table) == 2
        cql.execute(f"INSERT INTO {table} (id, usr) VALUES ('abc', 'abc') USING TTL 5")
        t.at(20)
        assert count_rows(cql, table) == 1

# Test rows overwritten with different TTLs - each row expires according to
# the TTL of its last write - with different compaction strategies.
def check_overlapped_rows_ttls(cql, test_keyspace, clock, strategy):
    schema = 'key int, col1 int, col2 int, col3 int, PRIMARY KEY (key, col1)'
    with new_test_table(cql, test_keyspace, schema, f"WITH compaction = {{'class': '{strategy}'}}") as table:
        def insert(keys, ttl=None):
            using = f' USING TTL {ttl}' if ttl else ''
            for i in keys:
                cql.execute(f'INSERT INTO {table} (key, col1, col2, col3) VALUES ({i}, {i}, {i}, {i}){using}')
        insert(range(1, 21))
        assert count_rows(cql, table) == 20
        t = Timeline(clock)
        insert(range(5, 11), 20)
        insert(range(9, 14), 30)
        insert(range(10, 12), 10)
        insert(range(11, 16), 40)
        # Check after each TTL (plus a second) which keys are left.
        for when, expected in [(11, [i for i in range(1, 21) if i != 10]),
                               (21, [i for i in range(1, 21) if i not in [5, 6, 7, 8, 10]]),
                               (31, [i for i in range(1, 21) if i < 5 or i > 10]),
                               (41, [i for i in range(1, 21) if i < 5 or i > 15])]:
            t.at(when)
            assert sorted(row.key for row in cql.execute(f'SELECT key FROM {table}')) == expected

@pytest.mark.parametrize('strategy', ['LeveledCompactionStrategy', 'SizeTieredCompactionStrategy', 'TimeWindowCompactionStrategy'])
def test_overlapped_rows_ttls(cql, test_keyspace, clock, strategy):
    check_overlapped_rows_ttls(cql, test_keyspace, clock, strategy)

# IncrementalCompactionStrategy is a Scylla-only compaction strategy.
def test_overlapped_rows_ttls_ics(cql, test_keyspace, clock, scylla_only):
    check_overlapped_rows_ttls(cql, test_keyspace, clock, 'IncrementalCompactionStrategy')
