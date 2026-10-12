# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Various tests for Scylla's ttl feature - USING TTL and DEFAULT_TIME_TO_LIVE
#############################################################################

from test import DEBUG_MODES
from .util import new_named_table, new_test_table, unique_key_int
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
