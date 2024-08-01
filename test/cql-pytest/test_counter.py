# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

###############################################################################
# Tests for various operations on COUNTER columns.
# See also tests for casting involving counter columns in test_cast_data.py
###############################################################################

import pytest
from util import new_test_table, unique_key_int
from cassandra.protocol import InvalidRequest
from rest_api import scylla_inject_error

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, i bigint, v int") as table:
        yield table

@pytest.fixture(scope="module")
def table2(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, c counter") as table:
        yield table

# Test that the function counterasblob() exists and works as expected -
# same as bigintasblob on the same number (a counter is a 64-bit number).
# Reproduces #14742
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18180")]), "vnodes"],
                         indirect=True)
def test_counter_to_blob(cql, table1, table2):
    p = unique_key_int()
    cql.execute(f'UPDATE {table1} SET i = 1000 WHERE p = {p}')
    cql.execute(f'UPDATE {table2} SET c = c + 1000 WHERE p = {p}')
    expected = b'\x00\x00\x00\x00\x00\x00\x03\xe8'
    assert [(expected,)] == list(cql.execute(f"SELECT bigintasblob(i) FROM {table1} WHERE p={p}"))
    assert [(expected,)] == list(cql.execute(f"SELECT counterasblob(c) FROM {table2} WHERE p={p}"))
    # Although the representation of the counter and bigint types are the
    # same (64-bit), you can't use the wrong "*asblob()" function:
    with pytest.raises(InvalidRequest, match='counterasblob'):
        cql.execute(f"SELECT counterasblob(i) FROM {table1} WHERE p={p}")
    # The opposite order is allowed in Scylla because of #14319, so let's
    # split it into a second test test_counter_to_blob2:
@pytest.mark.xfail(reason="issue #14319")
def test_counter_to_blob2(cql, table1, table2):
    p = unique_key_int()
    cql.execute(f'UPDATE {table2} SET c = c + 1000 WHERE p = {p}')
    # Reproduces #14319:
    with pytest.raises(InvalidRequest, match='bigintasblob'):
        cql.execute(f"SELECT bigintasblob(c) FROM {table2} WHERE p={p}")

# Test that the function blobascounter() exists and works as expected.
# Reproduces #14742
def test_counter_from_blob(cql, table1):
    p = unique_key_int()
    cql.execute(f'UPDATE {table1} SET i = 1000 WHERE p = {p}')
    assert [(1000,)] == list(cql.execute(f"SELECT blobascounter(bigintasblob(i)) FROM {table1} WHERE p={p}"))

# blobascounter() must insist to receive a properly-sized (8-byte) blob.
# If it accepts a shorter blob (e.g., 4 bytes) and returns that to the driver,
# it will confuse the driver (the driver will expect to read 8 bytes for the
# bigint but will get only 4).
# We have test_native_functions.py::test_blobas_wrong_size() that verifies
# that this protection works for the bigint type, but it turns out it also
# needs to be separately enforced for the counter type.
def test_blobascounter_wrong_size(cql, table1):
    p = unique_key_int()
    cql.execute(f'UPDATE {table1} SET v = 1000 WHERE p = {p}')
    with pytest.raises(InvalidRequest, match='blobascounter'):
        cql.execute(f"SELECT blobascounter(intasblob(v)) FROM {table1} WHERE p={p}")

# Drop a table while there is a counter update operation in progress.
# Verify the table waits for the operation to complete before it's destroyed.
# Reproduces scylladb/scylla-enterprise#4475
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18180")]), "vnodes"],
                         indirect=True)
def test_counter_update_while_table_dropped(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, c counter") as table, \
         scylla_inject_error(cql, "apply_counter_update_delay_5s", one_shot=True):
        cql.execute_async(f'UPDATE {table} SET c = c + 1 WHERE p = 0')
