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

from .util import unique_name, new_test_table, unique_key_int
from cassandra.protocol import FunctionFailure, InvalidRequest # type: ignore
import pytest

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
