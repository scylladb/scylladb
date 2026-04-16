# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#############################################################################
# Various tests for User-Defined Type (UDT) support in Scylla.
#
# There are additional tests involving UDTs in the context of other features
# (describe, aggregates, filtering, json, etc.) in other files. We also have
# many tests for UDTs ported from Cassandra in cassandra_tests/. Here we only
# have a few tests that didn't fit elsewhere.
#############################################################################

import time
import pytest
from cassandra.protocol import InvalidRequest
from .util import new_test_table, unique_key_int, new_type

# In test_select_collection_element.py we test that we can select
# WRITETIME(col[key]) and TTL(col[key]) for an element in a map and a set,
# reproducing issue #15427.
# Here we check that we can also select WRITETIME(a.field) for a field of
# an UDT:
def test_writetime_udt_field(cql, test_keyspace):
    with new_type(cql, test_keyspace, '(a int, b int)') as typ:
        schema = f"p int PRIMARY KEY, x {typ}"
        with new_test_table(cql, test_keyspace, schema) as table:
            p = unique_key_int()
            timestamp = int(time.time() * 1000000)
            timestamp1 = timestamp - 1234
            timestamp2 = timestamp - 1217
            cql.execute(f"UPDATE {table} USING TIMESTAMP {timestamp1} SET x.a = 42 WHERE p={p}")
            cql.execute(f"UPDATE {table} USING TIMESTAMP {timestamp2} SET x.b = 17 WHERE p={p}")
            assert list(cql.execute(f"SELECT WRITETIME(x.a) FROM {table} WHERE p={p}")) == [(timestamp1,)]
            assert list(cql.execute(f"SELECT WRITETIME(x.b) FROM {table} WHERE p={p}")) == [(timestamp2,)]

# And TTL(a.field) for a field of an UDT:
def test_ttl_udt_field(cql, test_keyspace):
    with new_type(cql, test_keyspace, '(a int, b int)') as typ:
        schema = f"p int PRIMARY KEY, x {typ}"
        with new_test_table(cql, test_keyspace, schema) as table:
            p = unique_key_int()
            ttl1 = 3600
            ttl2 = 7200
            cql.execute(f"UPDATE {table} USING TTL {ttl1} SET x.a = 42 WHERE p={p}")
            cql.execute(f"UPDATE {table} USING TTL {ttl2} SET x.b = 17 WHERE p={p}")
            # TTL() on a field returns the remaining TTL for that field, which may be slightly less than the original TTL by the time we read it, so we check that it's between ttl and ttl-100.
            [(t,)] = cql.execute(f"SELECT TTL(x.a) FROM {table} WHERE p={p}")
            assert t is not None and t <= ttl1 and t > ttl1 - 100
            [(t,)] = cql.execute(f"SELECT TTL(x.b) FROM {table} WHERE p={p}")
            assert t is not None and t <= ttl2 and t > ttl2 - 100
            # A field written without a TTL returns null TTL.
            cql.execute(f"UPDATE {table} SET x.a = 99 WHERE p={p}")
            [(t,)] = cql.execute(f"SELECT TTL(x.a) FROM {table} WHERE p={p}")
            assert t is None

# Test that WRITETIME(col.field) and TTL(col.field) fail with a clear error
# when col is not a UDT, e.g., an int.
def test_writetime_field_on_wrong_type(cql, test_keyspace):
    schema = f'p int PRIMARY KEY, v int'
    with new_test_table(cql, test_keyspace, schema) as table:
        # Subscript on a non-map/set column should be rejected.
        # Cassandra says "Invalid element selection: v is of type int is not a
        # collection". Scylla says "Column v is not a map/set/list, cannot be
        # subscripted"
        with pytest.raises(InvalidRequest, match=' v '):
            cql.execute(f"SELECT WRITETIME(v[3]) FROM {table}")
        with pytest.raises(InvalidRequest, match=' v '):
            cql.execute(f"SELECT TTL(v[3]) FROM {table}")
        # Field selection on a non-UDT column should be rejected.
        # Cassandra and Scylla both say "Invalid field selection:
        # v of type int is not a user type".
        with pytest.raises(InvalidRequest, match='user type'):
            cql.execute(f"SELECT WRITETIME(v.a) FROM {table}")
        with pytest.raises(InvalidRequest, match='user type'):
            cql.execute(f"SELECT TTL(v.a) FROM {table}")

# If a WRITETIME() of individual fields of a tuple is not supported (we have
# test for this - test_type_tuple.py::test_writetime_on_tuple), then it's
# reasonable that it is also not supported on a *frozen* UDT, since just like
# in a tuple, a frozen UDT does not have timestamps for individual fields.
# But, surprisingly, Cassandra does allow WRITETIME() on individual fields of
# a frozen UDT, and returns the timestamp of the whole UDT. Because of this
# difference from Cassandra, we mark this test as xfail. In the future we
# can consider if Cassandra's support for field timestamps in frozen UDTs
# but not tuples is a mistake, and replace the xfail by cassandra_bug.
@pytest.mark.xfail(reason="Cassandra allows WRITETIME on fields of frozen UDTs, but Scylla does not")
def test_writetime_field_on_frozen_udt(cql, test_keyspace):
    with new_type(cql, test_keyspace, '(a int, b int)') as typ:
        schema = f'p int PRIMARY KEY, x frozen<{typ}>'
        with new_test_table(cql, test_keyspace, schema) as table:
            p = unique_key_int()
            timestamp = int(time.time() * 1000000) - 1234
            cql.execute(f'INSERT INTO {table}(p, x) VALUES ({p}, {{a: 1, b: 2}}) USING TIMESTAMP {timestamp}')
            # Scylla's error message: "WRITETIME on a field selection is
            # only valid for non-frozen UDT columns". Cassandra allows this.
            assert list(cql.execute(f'SELECT WRITETIME(x.a) FROM {table} WHERE p={p}')) == [(timestamp,)]

@pytest.mark.xfail(reason="Cassandra allows TTL on fields of frozen UDTs, but Scylla does not")
def test_ttl_field_on_frozen_udt(cql, test_keyspace):
    with new_type(cql, test_keyspace, '(a int, b int)') as typ:
        schema = f'p int PRIMARY KEY, x frozen<{typ}>'
        with new_test_table(cql, test_keyspace, schema) as table:
            p = unique_key_int()
            ttl = 3600
            cql.execute(f'INSERT INTO {table}(p, x) VALUES ({p}, {{a: 1, b: 2}}) USING TTL {ttl}')
            # Scylla's error message: "TTL on a field selection is
            # only valid for non-frozen UDT columns". Cassandra allows this.
            [(t,)] = cql.execute(f'SELECT TTL(x.a) FROM {table} WHERE p={p}')
            assert t is not None and t <= ttl and t > ttl - 100

# This is a variant of the previous test (test_writetime_field_on_frozen_udt)
# with the frozen udt also being a primary key column (which is allowed for
# a *frozen* udt). Primary key columns don't have a timestamp or TTL at
# all, so this case should be rejected even if we generally allow WRITETIME
# or TTL on frozen udt members.
def test_writetime_ttl_frozen_udt_elements_pk(cql, test_keyspace):
    with new_type(cql, test_keyspace, '(a int, b int)') as typ:
        schema = f'pk frozen<{typ}> PRIMARY KEY'
        with new_test_table(cql, test_keyspace, schema) as table:
            # Scylla's error message is "WRITETIME is not legal on primary key
            # component pk". Cassandra's is "Cannot use selection function
            # writetime on PRIMARY KEY part pk".
            with pytest.raises(InvalidRequest, match='primary key|PRIMARY KEY'):
                cql.execute(f"SELECT WRITETIME(pk.a) FROM {table}")
            with pytest.raises(InvalidRequest, match='primary key|PRIMARY KEY'):
                cql.execute(f"SELECT TTL(pk.a) FROM {table}")

# Test WRITETIME() on an entire frozen UDT column. A frozen UDT is stored as
# a single atomic cell, so it has a single timestamp like any atomic column.
def test_writetime_frozen_udt(cql, test_keyspace):
    with new_type(cql, test_keyspace, '(a int, b int)') as typ:
        schema = f"p int PRIMARY KEY, x frozen<{typ}>"
        with new_test_table(cql, test_keyspace, schema) as table:
            p = unique_key_int()
            timestamp = int(time.time() * 1000000) - 1234
            cql.execute(f"INSERT INTO {table}(p, x) VALUES ({p}, {{a: 1, b: 2}}) USING TIMESTAMP {timestamp}")
            assert list(cql.execute(f"SELECT WRITETIME(x) FROM {table} WHERE p={p}")) == [(timestamp,)]

# Test TTL() on an entire frozen UDT column. A frozen UDT is stored as
# a single atomic cell, so it has a single TTL like any atomic column.
def test_ttl_frozen_udt(cql, test_keyspace):
    with new_type(cql, test_keyspace, '(a int, b int)') as typ:
        schema = f"p int PRIMARY KEY, x frozen<{typ}>"
        with new_test_table(cql, test_keyspace, schema) as table:
            p = unique_key_int()
            ttl = 3600
            cql.execute(f"INSERT INTO {table}(p, x) VALUES ({p}, {{a: 1, b: 2}}) USING TTL {ttl}")
            [(t,)] = cql.execute(f"SELECT TTL(x) FROM {table} WHERE p={p}")
            assert t is not None and t <= ttl and t > ttl - 100