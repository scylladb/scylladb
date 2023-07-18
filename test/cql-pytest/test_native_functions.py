# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

###############################################################################
# Tests for various native (built-in) scalar functions that can be used in
# various SELECT, INSERT or UPDATE requests. Note we also have tests for
# some of these functions in many other test files. For example, the tests
# for the cast() function are in test_cast_data.py.
###############################################################################

import pytest
from util import new_test_table, unique_key_int
from cassandra.protocol import InvalidRequest

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, i int, g bigint, b blob, s text, t timestamp, u timeuuid, PRIMARY KEY (p)") as table:
        yield table

# Check that a function that can take a column name as a parameter, can also
# take a constant. This feature is barely useful for WHERE clauses, and
# even less useful for selectors, but should be allowed for both.
# Reproduces #12607.
@pytest.mark.xfail(reason="issue #12607")
def test_constant_function_parameter(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, b) VALUES ({p}, 0x03)")
    assert [(p,)] == list(cql.execute(f"SELECT p FROM {table1} WHERE p={p} AND b=tinyintAsBlob(3) ALLOW FILTERING"))
    assert [(b'\x04',)] == list(cql.execute(f"SELECT tinyintAsBlob(4) FROM {table1} WHERE p={p}"))

# According to the documentation, "The `minTimeuuid` function takes a
# `timestamp` value t, either a timestamp or a date string.". But although
# both cases are supported with constant parameters in WHERE restrictions,
# in a *selector* (the first part of the SELECT, saying what to select), it
# turns out that ONLY a timestamp column is allowed. Although this is
# undocumented behavior, both Cassandra and Scylla share it so we deem it
# correct.
def test_selector_mintimeuuid(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, s, t, i) VALUES ({p}, '2013-02-02 10:00+0000', 123, 456)")
    # We just check this works, not what the value is:
    cql.execute(f"SELECT mintimeuuid(t) FROM {table1} WHERE p={p}")
    # This doesn't work - despite the documentation, in a selector a
    # date string is not supported by mintimeuuid.
    with pytest.raises(InvalidRequest, match='of type timestamp'):
        cql.execute(f"SELECT mintimeuuid(s) FROM {table1} WHERE p={p}")
    # Other integer types also don't work, it must be a timestamp:
    with pytest.raises(InvalidRequest, match='of type timestamp'):
        cql.execute(f"SELECT mintimeuuid(i) FROM {table1} WHERE p={p}")

# Cassandra allows the implicit (and wrong!) casting of a bigint returned
# by writetime() to the timestamp type required by mintimeuuid(). Scylla
# doesn't. I'm not sure which behavior we should consider correct, but it's
# useful to have a test that demonstrates this incompatibility.
# Reproduces #14319.
@pytest.mark.xfail(reason="issue #14319")
def test_selector_mintimeuuid_64bit(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, g) VALUES ({p}, 123)")
    cql.execute(f"SELECT mintimeuuid(g) FROM {table1} WHERE p={p}")
    cql.execute(f"SELECT mintimeuuid(writetime(g)) FROM {table1} WHERE p={p}")

# blobasbigint() must insist to receive a properly-sized (8-byte) blob.
# If it accepts a shorter blob (e.g., 4 bytes) and returns that to the driver,
# it will confuse the driver (the driver will expect to read 8 bytes for the
# bigint but will get only 4).
def test_blobas_wrong_size(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, i) VALUES ({p}, 123)")
    # Cassandra and Scylla print: "In call to function system.blobasbigint,
    # value 0x0000007b is not a valid binary representation for type bigint".
    with pytest.raises(InvalidRequest, match='blobasbigint'):
        cql.execute(f"SELECT blobasbigint(intasblob(i)) FROM {table1} WHERE p={p}")
