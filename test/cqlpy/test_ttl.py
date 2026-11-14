# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Various tests for Scylla's ttl feature - USING TTL and DEFAULT_TIME_TO_LIVE
#############################################################################

from .util import new_test_table, unique_key_int
from cassandra.query import UNSET_VALUE
from cassandra.protocol import ConfigurationException
import pytest
import time

# The maximum TTL Scylla accepts, 20 years in seconds (max_ttl in gc_clock.hh).
MAX_TTL = 20 * 365 * 24 * 60 * 60

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

# The table properties which end up as the TTL of cells must respect the
# same maximum as USING TTL does, otherwise the sstable reader rejects the
# written cells ("Too big ttl"). Reproduces SCYLLADB-6065.
def check_ttl_property_limit(cql, test_keyspace, prop):
    too_large = rf"{prop} must be less than or equal to {MAX_TTL} \(got {MAX_TTL + 1}\)"
    with pytest.raises(ConfigurationException, match=too_large):
        with new_test_table(cql, test_keyspace, 'p int primary key, v int', f'with {prop} = {MAX_TTL + 1}'):
            pass
    # The maximum itself is accepted, on CREATE and ALTER TABLE.
    with new_test_table(cql, test_keyspace, 'p int primary key, v int', f'with {prop} = {MAX_TTL}') as table:
        cql.execute(f'ALTER TABLE {table} WITH {prop} = {MAX_TTL}')
        with pytest.raises(ConfigurationException, match=too_large):
            cql.execute(f'ALTER TABLE {table} WITH {prop} = {MAX_TTL + 1}')
        # ALTER TABLE of an unrelated property is not affected.
        cql.execute(f'ALTER TABLE {table} WITH comment = \'ttl\'')

def test_default_time_to_live_limit(cql, test_keyspace):
    check_ttl_property_limit(cql, test_keyspace, 'default_time_to_live')

# Cassandra has no paxos_grace_seconds property.
def test_paxos_grace_seconds_limit(scylla_only, cql, test_keyspace):
    check_ttl_property_limit(cql, test_keyspace, 'paxos_grace_seconds')
