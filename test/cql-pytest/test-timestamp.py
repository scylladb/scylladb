# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Various tests for USING TIMESTAMP support in Scylla. Note that Cassandra
# also had tests for timestamps, which we ported in
# cassandra_tests/validation/entities/json_timestamp.py. The tests here are
# either additional ones, or focusing on more esoteric issues or small tests
# aiming to reproduce bugs discovered by bigger Cassandra tests.
#############################################################################

from util import unique_name, new_test_table, unique_key_int
from cassandra.protocol import FunctionFailure, InvalidRequest
import pytest
import time

@pytest.fixture(scope="session")
def table1(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (k int PRIMARY KEY, v int)")
    yield table
    cql.execute("DROP TABLE " + table)

# In Cassandra, timestamps can be any *signed* 64-bit integer, not including
# the most negative 64-bit integer (-2^63) which for deletion times is
# reserved for marking *not deleted* cells.
# As proposed in issue #5619, Scylla forbids timestamps higher than the
# current time in microseconds plus three days. Still, any negative is
# timestamp is still allowed in Scylla. If we ever choose to expand #5619
# and also forbid negative timestamps, we will need to remove this test -
# but for now, while they are allowed, let's test that they are.
def test_negative_timestamp(cql, table1):
    p = unique_key_int()
    write = cql.prepare(f"INSERT INTO {table1} (k, v) VALUES (?, ?) USING TIMESTAMP ?")
    read = cql.prepare(f"SELECT writetime(v) FROM {table1} where k = ?")
    # Note we need to order the loop in increasing timestamp if we want
    # the read to see the latest value:
    for ts in [-2**63+1, -100, -1]:
        print(ts)
        cql.execute(write, [p, 1, ts])
        assert ts == cql.execute(read, [p]).one()[0]
    # The specific value -2**63 is not allowed as a timestamp - although it
    # is a legal signed 64-bit integer, it is reserved to mean "not deleted"
    # in the deletion time of cells.
    with pytest.raises(InvalidRequest, match='bound'):
        cql.execute(write, [p, 1, -2**63])

# As explained above, after issue #5619 Scylla can forbid timestamps higher
# than the current time in microseconds plus three days. This test will
# check that it actually does. Starting with #12527 this restriction can
# be turned on or off, so this test checks which mode we're in that this
# mode does the right thing. On Cassandra, this checking is always disabled.
def test_futuristic_timestamp(cql, table1):
    # The USING TIMESTAMP checking assumes the timestamp is in *microseconds*
    # since the UNIX epoch. If we take the number of *nanoseconds* since the
    # epoch, this will be thousands of years into the future, and if USING
    # TIMESTAMP rejects overly-futuristic timestamps, it should surely reject
    # this one.
    futuristic_ts = int(time.time()*1e9)
    p = unique_key_int()
    # In Cassandra and in Scylla with restrict_future_timestamp=false,
    # futuristic_ts can be successfully written and then read as-is. In
    # Scylla with restrict_future_timestamp=true, it can't be written.
    def restrict_future_timestamp():
        # If not running on Scylla, futuristic timestamp is not restricted
        names = [row.table_name for row in cql.execute("SELECT * FROM system_schema.tables WHERE keyspace_name = 'system'")]
        if not any('scylla' in name for name in names):
            return False
        # In Scylla, we check the configuration via CQL.
        v = list(cql.execute("SELECT value FROM system.config WHERE name = 'restrict_future_timestamp'"))
        return v[0].value == "true"
    if restrict_future_timestamp():
        print('checking with restrict_future_timestamp=true')
        with pytest.raises(InvalidRequest, match='into the future'):
            cql.execute(f'INSERT INTO {table1} (k, v) VALUES ({p}, 1) USING TIMESTAMP {futuristic_ts}')
    else:
        print('checking with restrict_future_timestamp=false')
        cql.execute(f'INSERT INTO {table1} (k, v) VALUES ({p}, 1) USING TIMESTAMP {futuristic_ts}')
        assert [(futuristic_ts,)] == cql.execute(f'SELECT writetime(v) FROM {table1} where k = {p}')

# Currently, writetime(k) is not allowed for a key column. Neither is ttl(k).
# Scylla issue #14019 and CASSANDRA-9312 consider allowing it - with the
# meaning that it should return the timestamp and ttl of a row marker.
# If this issue is ever implemented in Scylla or Cassandra, the following
# test will need to be replaced by a test for the new feature instead of
# expecting an error message.
def test_key_writetime(cql, table1):
    with pytest.raises(InvalidRequest, match='PRIMARY KEY part k|WRITETIME is not legal on partition key component k'):
        cql.execute(f'SELECT writetime(k) FROM {table1}')
    with pytest.raises(InvalidRequest, match='PRIMARY KEY part k|TTL is not legal on partition key component k'):
        cql.execute(f'SELECT ttl(k) FROM {table1}')
