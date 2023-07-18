# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

###############################################################################
# Tests for data casts, e.g `SELECT CAST (... AS int) FROM tbl`
#
# Note that this is a different feature from the query casts `(int)123`
# which are tested separately in test_cast.py. Whereas the query casts
# can only reinterpret data losslessly (e.g., cast an integer to a wider
# integer), the data casts tested here can actually convert data from one
# type to another in sometimes surprising ways. For example, a very large
# arbitrary-precision number (varint) can be cast into an integer and result
# in a wraparound, or be cast into a float and result an infinity. In
# this test file we'll check some of these surprising conversions, and
# verify that they are the same in Scylla and Cassandra and don't regress.
#
# See also cql_cast_test.py in dtest.
###############################################################################

import pytest
import math
from util import new_test_table, unique_key_int
from cassandra.protocol import InvalidRequest

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, cVarint varint") as table:
        yield table

@pytest.fixture(scope="module")
def table2(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, c counter") as table:
        yield table

# Utility function for emulating a wrapping cast of a big positive number
# into a smaller signed integer of given number of bits. For example,
# casting 511 to 8 bits results in -1.
def signed(number, bits):
    p = 2**bits
    ret = number % p
    if ret > p/2:
        return ret - p
    else:
        return ret

# Test casting a very large varint number to various other types, resulting
# in various non-trivial conversions including truncation or wrap-around of
# numbers.
def test_cast_from_large_varint(cql, table1):
    p = unique_key_int()
    v = 32767456456456456456545678943512357658768763546575675
    cql.execute(f'INSERT INTO {table1} (p, cVarint) VALUES ({p}, {v})')
    # We can read back the original number without a cast, or with a cast
    # to the same type. The "decimal" type can also hold a varint and return
    # the same number.
    assert [(v,)] == list(cql.execute(f"SELECT cVarint FROM {table1} WHERE p={p}"))
    assert [(v,)] == list(cql.execute(f"SELECT CAST(cVarint AS varint) FROM {table1} WHERE p={p}"))
    assert [(v,)] == list(cql.execute(f"SELECT CAST(cVarint AS decimal) FROM {table1} WHERE p={p}"))
    # Casting into smaller integer types results in wraparound
    assert [(signed(v,8),)] == list(cql.execute(f"SELECT CAST(cVarint AS tinyint) FROM {table1} WHERE p={p}"))
    assert [(signed(v,16),)] == list(cql.execute(f"SELECT CAST(cVarint AS smallint) FROM {table1} WHERE p={p}"))
    assert [(signed(v,32),)] == list(cql.execute(f"SELECT CAST(cVarint AS int) FROM {table1} WHERE p={p}"))
    assert [(signed(v,64),)] == list(cql.execute(f"SELECT CAST(cVarint AS bigint) FROM {table1} WHERE p={p}"))
    # Casting to a 32-bit floating point, which only supports numbers up
    # to 1e38, results in infinity
    assert [(math.inf,)] == list(cql.execute(f"SELECT CAST(cVarint AS float) FROM {table1} WHERE p={p}"))
    # Casting to a 64-bit floating point, which supports the range of our
    # given number (though not its full precision!) is allowed, and some
    # precision is lost. Confusingly, Python's 64-bit floating point is
    # called "float", and we can use Python's float() function to convert
    # the large number into a 64-bit double to compare the expected result.
    assert [(float(v),)] == list(cql.execute(f"SELECT CAST(cVarint AS double) FROM {table1} WHERE p={p}"))
    # Casting the number to string types results in printing the string in
    # decimal, as expected - same as Python's str() function:
    assert [(str(v),)] == list(cql.execute(f"SELECT CAST(cVarint AS ascii) FROM {table1} WHERE p={p}"))
    assert [(str(v),)] == list(cql.execute(f"SELECT CAST(cVarint AS text) FROM {table1} WHERE p={p}"))
    # "varchar" is supposed to be an alias to "text" and worked just as well,
    # but suprisingly casting to varchar doesn't work on Cassandra, so let's
    # test it in a separate test below, test_cast_from_large_varint_to_varchar
    # Casting a number to all other types is NOT allowed:
    for t in ['blob', 'boolean', 'counter', 'date', 'duration', 'inet',
              'timestamp', 'timeuuid', 'uuid']:
        with pytest.raises(InvalidRequest, match='cast'):
            cql.execute(f"SELECT CAST(cVarint AS {t}) FROM {table1} WHERE p={p}")

# In test_cast_from_large_varint we checked that a varint can be cast
# to the "text" type. Since "varchar" is just an alias for "text", casting
# to varchar should work too, but in Cassandra it doesn't so this test
# is marked a Cassandra bug.
def test_cast_from_large_varint_to_varchar(cql, table1, cassandra_bug):
    p = unique_key_int()
    v = 32767456456456456456545678943512357658768763546575675
    cql.execute(f'INSERT INTO {table1} (p, cVarint) VALUES ({p}, {v})')
    assert [(str(v),)] == list(cql.execute(f"SELECT CAST(cVarint AS varchar) FROM {table1} WHERE p={p}"))

# Test casting a counter to various other types. Reproduces #14501.
def test_cast_from_counter(cql, table2):
    p = unique_key_int()
    # Set the counter to 1000 in two increments, to make it less trivial to
    # read correctly.
    cql.execute(f'UPDATE {table2} SET c = c + 230 WHERE p = {p}')
    cql.execute(f'UPDATE {table2} SET c = c + 770 WHERE p = {p}')
    # We can read back the original number without a cast, or with a silly
    # cast to the same type "counter".
    assert [(1000,)] == list(cql.execute(f"SELECT c FROM {table2} WHERE p={p}"))
    assert [(1000,)] == list(cql.execute(f"SELECT CAST(c AS counter) FROM {table2} WHERE p={p}"))
    # Casting into smaller integer types results in wraparound
    assert [(signed(1000,8),)] == list(cql.execute(f"SELECT CAST(c AS tinyint) FROM {table2} WHERE p={p}"))
    assert [(1000,)] == list(cql.execute(f"SELECT CAST(c AS smallint) FROM {table2} WHERE p={p}"))
    assert [(1000,)] == list(cql.execute(f"SELECT CAST(c AS int) FROM {table2} WHERE p={p}"))
    assert [(1000,)] == list(cql.execute(f"SELECT CAST(c AS bigint) FROM {table2} WHERE p={p}"))
    assert [(1000.0,)] == list(cql.execute(f"SELECT CAST(c AS float) FROM {table2} WHERE p={p}"))
    assert [(1000.0,)] == list(cql.execute(f"SELECT CAST(c AS double) FROM {table2} WHERE p={p}"))
    # Casting the counter to string types results in printing the number in
    # decimal, as expected
    assert [("1000",)] == list(cql.execute(f"SELECT CAST(c AS ascii) FROM {table2} WHERE p={p}"))
    assert [("1000",)] == list(cql.execute(f"SELECT CAST(c AS text) FROM {table2} WHERE p={p}"))
    # "varchar" is supposed to be an alias to "text" and should work, but
    # suprisingly casting to varchar doesn't work on Cassandra, so we
    # test it in a separate test below, test_cast_from_counter_to_varchar.
    # Casting a counter to all other types is NOT allowed:
    for t in ['blob', 'boolean', 'date', 'duration', 'inet',
              'timestamp', 'timeuuid', 'uuid']:
        with pytest.raises(InvalidRequest, match='cast'):
            cql.execute(f"SELECT CAST(c AS {t}) FROM {table2} WHERE p={p}")

# In test_cast_from_counter we checked that a counter can be cast to the
# "text" type. Since "varchar" is just an alias for "text", casting
# to varchar should work too, but in Cassandra it doesn't so this test
# is marked a Cassandra bug.
def test_cast_from_counter_to_varchar(cql, table2, cassandra_bug):
    p = unique_key_int()
    cql.execute(f'UPDATE {table2} SET c = c + 1000 WHERE p = {p}')
    assert [("1000",)] == list(cql.execute(f"SELECT CAST(c AS varchar) FROM {table2} WHERE p={p}"))

# TODO: test casts from more types.
