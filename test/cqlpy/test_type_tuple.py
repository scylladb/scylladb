# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#############################################################################
# Tests involving the "tuple" column type.
#
# There are additional tests involving tuples in the context of other features
# (describe, aggregates, filtering, json, etc.) in other files. We also have
# many tests for tuples ported from Cassandra in cassandra_tests/. Here we only
# have a few tests that didn't fit elsewhere.
#############################################################################

import time
import pytest
from cassandra.protocol import InvalidRequest
from .util import new_test_table, unique_key_int

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int PRIMARY KEY, t tuple<int, int>') as table:
        yield table

# Unlike UDTs, tuples are always frozen, meaning that individual fields cannot
# be updated separately. So there is no point in allowing writetime() or ttl()
# on individual fields of a tuple - so we do reject them, and we'll test this
# here. Additionally, we check that writetime() and ttl() on the entire tuple
# column is allowed.
def test_writetime_on_tuple(cql, table1):
    # Subscript on a tuple is not valid for WRITETIME, since tuple is not
    # a map or set.
    with pytest.raises(InvalidRequest, match=' t '):
        cql.execute(f"SELECT WRITETIME(t[0]) FROM {table1}")
    # Field selection on a tuple is also not valid: tuples are not user
    # types, so the field_selection preparation itself rejects it.
    with pytest.raises(InvalidRequest, match='user type'):
        cql.execute(f"SELECT WRITETIME(t.a) FROM {table1}")
    # But WRITETIME() on the entire tuple column is allowed: it returns
    # the single timestamp of the whole (frozen) tuple cell.
    p = unique_key_int()
    timestamp = int(time.time() * 1000000) - 1234
    cql.execute(f"INSERT INTO {table1}(p, t) VALUES ({p}, (1, 2)) USING TIMESTAMP {timestamp}")
    assert list(cql.execute(f"SELECT WRITETIME(t) FROM {table1} WHERE p={p}")) == [(timestamp,)]

def test_ttl_on_tuple(cql, table1):
    # Subscript on a tuple is not valid for TTL, since tuple is not
    # a map or set.
    with pytest.raises(InvalidRequest, match=' t '):
        cql.execute(f"SELECT TTL(t[0]) FROM {table1}")
    # Field selection on a tuple is also not valid: tuples are not user
    # types, so the field_selection preparation itself rejects it.
    with pytest.raises(InvalidRequest, match='user type'):
        cql.execute(f"SELECT TTL(t.a) FROM {table1}")
    # But TTL() on the entire tuple column is allowed: it returns
    # the single timestamp of the whole (frozen) tuple cell.
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1}(p, t) VALUES ({p}, (1, 2)) USING TTL 1000")
    ret = list(cql.execute(f"SELECT TTL(t) FROM {table1} WHERE p={p}"))
    # TTL() returns the remaining TTL, which may be slightly less than 1000 by the time we read it, so we check that it's between 900 and 1000.
    assert len(ret) == 1 and len(ret[0]) == 1 and 900 <= ret[0][0] <= 1000
