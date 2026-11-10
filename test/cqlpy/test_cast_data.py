# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

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

from test import DEBUG_MODES
import pytest
import math
from .util import new_test_table, unique_key_int, run_cqlsh
from cassandra.protocol import InvalidRequest
from ctypes import c_float

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, cVarint varint") as table:
        yield table

@pytest.fixture(scope="module")
def table2(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, c counter") as table:
        yield table

@pytest.fixture(scope="module")
def table3(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, i int, a ascii, bi bigint, b blob, bool boolean, d date, dec decimal, db double, dur duration, f float, addr inet, si smallint, t text, tim time, ts timestamp, tu timeuuid, ti tinyint, u uuid, vc varchar, vi varint") as table:
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
    p_negative = unique_key_int()
    v = 32767456456456456456545678943512357658768763546575675
    cql.execute(f'INSERT INTO {table1} (p, cVarint) VALUES ({p}, {v})')
    cql.execute(f'INSERT INTO {table1} (p, cVarint) VALUES ({p_negative}, {-v})')
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
    assert [(-math.inf,)] == list(cql.execute(f"SELECT CAST(cVarint AS float) FROM {table1} WHERE p={p_negative}"))
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
    # but surprisingly casting to varchar doesn't work on Cassandra, so let's
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

# In test_cast_column_names we are checking that columns are named in Cassandra style
def test_cast_column_names(cql, table1):
    p = unique_key_int()
    v = 123
    for t in ['int', 'double', 'text', 'tinyint', 'smallint']:
        cql.execute(f'INSERT INTO {table1} (p, cVarint) VALUES ({p}, {v})')
        assert cql.execute(f"SELECT CAST(cVarint as {t}) FROM {table1} WHERE p={p}").one()._fields[0] == f"cast_cvarint_as_{t}"

#In test_ignore_cast_to_the_same_type we are checking that query with casting of column to the same type is optimized
def test_ignore_cast_to_the_same_type(cql, table1):
    p = unique_key_int()
    v = 456
    cql.execute(f'INSERT INTO {table1} (p, cVarint) VALUES ({p}, {v})')
    assert cql.execute(f"SELECT CAST(p as int) FROM {table1} WHERE p={p}").one()._fields[0] == "p"

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
    # surprisingly casting to varchar doesn't work on Cassandra, so we
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

# Test casts from various types *to* counter type. This is a rather silly
# operation - casting to "counter" doesn't make a real counter. It could
# have been supported the same as casting to bigint, but Cassandra chose not
# to support it and neither do we.
# The only case that works is the do-nothing casting of a counter to counter,
# and that case is already checked in test_cast_from_counter().
def test_cast_to_counter(cql, table3):
    p = unique_key_int()
    for col in ['i', 'a', 'bi', 'b', 'bool', 'd', 'dec', 'db', 'dur', 'f', 'addr', 'si', 't', 'tim', 'ts', 'tu', 'ti', 'u', 'vc', 'vi']:
        with pytest.raises(InvalidRequest, match='cannot be cast'):
            cql.execute(f"SELECT CAST({col} AS counter) FROM {table3} WHERE p={p}")

# Test casting 32-bit floating-point to text. The output string should have
# enough digits to faithfully reproduce the original floating-point value
# when read back. We don't check in this test whether maybe it outputs too
# many digits that don't make any difference when read back.
# Reproduces issue #15127.
def test_cast_float_to_text(cql, table3):
    p = unique_key_int()
    # The numbers below (based on pi) have excessive precision, they are
    # truncated to Python's 64-bit precision, and later further truncated
    # to 32-bit floats when assigned to a Scylla "float" column.
    numbers = [ 3.141592653589793238462643383279502884197,
               -3.141592653589793238462643383279502884197,
               -31415926535897932384.62643383279502884197,
               0.00003141592653589793238462643383279502884197]
    for n in numbers:
        cql.execute(f'INSERT INTO {table3} (p, f) VALUES ({p}, {n})')
        # Sanity check: If we read "f" back, we get n truncated to 32-bit
        # precision. We use c_float to truncate n to 32-bit precision.
        expected_f = c_float(n)
        read_f = c_float(list(cql.execute(f"SELECT f FROM {table3} WHERE p={p}"))[0][0])
        assert expected_f.value == read_f.value  # that's how to compare c_float objects
        # Finally test CAST(f AS text): test that the resulting textual
        # representation has enough digits to restore f without loss of
        # precision. If we ask Scylla to cast f to text, and then convert
        # this text back to c_float, the result should be equal again to
        # expected_f. If the cast printed too few digits, it would not be
        # equal.
        ftext = list(cql.execute(f"SELECT CAST(f AS text) FROM {table3} WHERE p={p}"))[0][0]
        read_f = c_float(float(ftext))
        assert expected_f.value == read_f.value

# Same test as test_cast_float_to_text() above, just for 64-bit floats.
# Reproduces issue #15127.
def test_cast_double_to_text(cql, table3):
    p = unique_key_int()
    numbers = [ 3.141592653589793238462643383279502884197,
               -3.141592653589793238462643383279502884197,
               -31415926535897932384.62643383279502884197,
               0.00003141592653589793238462643383279502884197]
    for n in numbers:
        cql.execute(f'INSERT INTO {table3} (p, db) VALUES ({p}, {n})')
        # If we read "db" back, we get n truncated to 64-bit precision.
        assert n == list(cql.execute(f"SELECT db FROM {table3} WHERE p={p}"))[0][0]
        # If we ask Scylla to cast db to text, and then convert this text back
        # to 64-bit float, the result should be equal again to n. If the cast
        # printed too few digits, it would not be equal.
        ftext = list(cql.execute(f"SELECT CAST(db AS text) FROM {table3} WHERE p={p}"))[0][0]
        assert n == float(ftext)

# TODO: test casts from more types.


# The casting tests above use one value at a time. The following tests,
# ported from scylla-dtest cql_cast_test.py, create a table of a few values of
# one type, and check through cqlsh, as cqlsh prints them, the casts of each
# value to other types (some narrow or truncate it: bigint 7657139128428828644
# cast to tinyint wraps around to -28), aggregates of such casts
# (sum(cast(c as tinyint)) sums the wrapped-around values), casts of
# aggregates (cast(avg(c) as tinyint)) and casts of casts.

# The values and expected results of dtest cql_cast_test.py's
# TestData.POSITIVE_VALUES: for each source type, the rows
# [<value inserted>, {<test>: {<cast to type>: <expected result>}}], where
# <test> is "cast" (of that row), an aggregate of the cast ("avg", "sum",
# "min", "max"), a cast of the aggregate ("cast_avg"...) or "cast_cast" (of
# that row, to "<type1>/<type2>"). "udt" holds such rows for each field of a
# UDT. As in the dtest, tests and types starting with "#" and falsy expected
# results are skipped.
POSITIVE_VALUES = {
    "bigint": [
        [
            7657139128428828644,
            {
                "cast": {
                    "tinyint": -28,
                    "smallint": -26652,
                    "int": -776038428,
                    "float": "7.6571e+18",
                    "double": "7.6571e+18",
                    "decimal": "7657139128428828644",
                    "varint": 7657139128428828644,
                    "text": "7657139128428828644",
                    "varchar": "7657139128428828644",
                },
                "avg": {"tinyint": 10, "smallint": -10741, "int": -258681333, "float": "2.5524e+18", "double": "2.5524e+18", "decimal": "2552379709476274357", "varint": 2552379709476274357, "text": None, "varchar": None},
                "sum": {"tinyint": 31, "smallint": -32225, "int": -776044001, "float": "7.6571e+18", "double": "7.6571e+18", "decimal": "7657139128428823071", "varint": 7657139128428823071, "text": None, "varchar": None},
                "min": {"tinyint": -66, "smallint": -26652, "int": -776038428, "float": -5698, "double": -5698, "decimal": -5698.0, "varint": -5698, "text": "-5698", "varchar": "-5698"},
                "max": {
                    "tinyint": 125,
                    "smallint": 125,
                    "int": 125,
                    "float": "7.6571e+18",
                    "double": "7.6571e+18",
                    "decimal": "7657139128428828644",
                    "varint": 7657139128428828644,
                    "text": "7657139128428828644",
                    "varchar": "7657139128428828644",
                },
            },
        ],
        [125, {"cast": {"tinyint": 125, "smallint": 125, "int": 125, "float": 125, "double": 125, "decimal": 125.0, "varint": 125, "text": "125", "varchar": "125"}}],
        [-5698, {"cast": {"tinyint": -66, "smallint": -5698, "int": -5698, "double": -5698, "decimal": -5698.0, "varint": -5698, "text": "-5698", "varchar": "-5698"}}],
    ],
    "boolean": [[True, {"cast": {"text": "true", "varchar": "true"}}], [False, {"cast": {"text": "false", "varchar": "false"}}]],
    "decimal": [
        [
            76571.345,
            {
                "cast": {"tinyint": 27, "smallint": 11035, "int": 76571, "float": 76571.34375, "double": 76571.345, "bigint": 76571, "varint": 76571, "text": "76571.345", "varchar": "76571.345"},
                "avg": {"tinyint": 14, "smallint": 5390, "int": 38158, "float": 38158.54297, "double": 38158.54401, "bigint": 38158, "varint": 38158, "text": None, "varchar": None},
                "sum": {"tinyint": 29, "smallint": 10781, "int": 76317, "float": 76317.08594, "double": 76317.08801, "bigint": 76317, "varint": 76317, "text": None, "varchar": None},
                "min": {"tinyint": 2, "smallint": -254, "int": -254, "float": -254.25699, "double": -254.25699, "bigint": -254, "varint": -254, "text": "-254.256987", "varchar": "-254.256987"},
                "max": {"tinyint": 27, "smallint": 11035, "int": 76571, "float": 76571.34375, "double": 76571.345, "bigint": 76571, "varint": 76571, "text": "76571.345", "varchar": "76571.345"},
                "cast_avg": {"tinyint": 14, "smallint": -27378, "int": 38158, "float": "38158.54297", "double": "38158.54401", "bigint": 38158, "varint": 38158, "text": None, "varchar": None},
                "cast_sum": {"tinyint": 29, "smallint": 10781, "int": 76317, "float": "76317.08594", "double": "76317.08801", "bigint": 76317, "varint": 76317, "text": None, "varchar": None},
                "cast_min": {"tinyint": 2, "smallint": -254, "int": -254, "float": "-254.25699", "double": "-254.25699", "bigint": -254, "varint": -254, "text": "-254.256987", "varchar": "-254.256987"},
                "cast_max": {"tinyint": 27, "smallint": 11035, "int": 76571, "float": 76571.34375, "double": "76571.345", "bigint": 76571, "varint": 76571, "text": "76571.345", "varchar": "76571.345"},
                "cast_cast": {"double/bigint": "76571"},
            },
        ],
        [-254.256987, {"cast": {"tinyint": 2, "smallint": -254, "int": -254, "float": -254.25699, "double": -254.25699, "bigint": -254, "varint": -254, "text": "-254.256987", "varchar": "-254.256987"}}],
    ],
    "double": [
        [
            76571.345,
            {
                "cast": {"tinyint": 27, "smallint": 11035, "int": 76571, "float": 76571.34375, "decimal": "76571.345000000001", "bigint": 76571, "varint": 76571, "text": "76571.345", "varchar": "76571.345"},
                "avg": {"tinyint": 14, "smallint": 5390, "int": 38158, "float": 38158.54297, "decimal": "38158.54400650000050", "bigint": 38158, "varint": 38158, "text": None, "varchar": None},
                "sum": {"tinyint": 29, "smallint": 10781, "int": 76317, "float": 76317.08594, "decimal": "76317.08801300000099", "bigint": 76317, "varint": 76317, "text": None, "varchar": None},
                "min": {"tinyint": 2, "smallint": -254, "int": -254, "float": -254.25699, "decimal": "-254.25698700000001", "bigint": -254, "varint": -254, "text": "-254.256987", "varchar": "-254.256987"},
                "max": {"tinyint": 27, "smallint": 11035, "int": 76571, "float": 76571.34375, "decimal": "76571.345000000001", "bigint": 76571, "varint": 76571, "text": "76571.345", "varchar": "76571.345"},
            },
        ],
        [-254.256987, {"cast": {"tinyint": 2, "smallint": -254, "int": -254, "float": -254.25699, "decimal": "-254.25698700000001", "bigint": -254, "varint": -254, "text": "-254.256987", "varchar": "-254.256987"}}],
    ],
    "float": [
        [
            -564,
            {
                "cast": {"tinyint": -52, "smallint": -564, "int": -564, "double": -564, "decimal": "-564.0", "bigint": -564, "varint": -564, "text": "-564.0", "varchar": "-564.0"},
                "avg": {"tinyint": 10, "smallint": 1034, "int": 1034, "double": "1034.23883", "decimal": "1034.238824", "bigint": 1034, "varint": 1034, "text": None, "varchar": None},
                "sum": {"tinyint": 30, "smallint": 3102, "int": 3102, "double": "3102.71648", "decimal": "3102.716473", "bigint": 3102, "varint": 3102, "text": None, "varchar": None},
                "min": {"tinyint": -52, "smallint": -564, "int": -564, "double": -564, "decimal": "-564.0", "bigint": -564, "varint": -564, "text": "-564.0", "varchar": "-564.0"},
                "max": {"tinyint": 123, "smallint": 3543, "int": 3543, "double": "3543.45654", "decimal": "3543.45654", "bigint": 3543, "varint": 3543, "text": "3543.4565", "varchar": "3543.4565"},
            },
        ],
        [3543.4565429688, {"cast": {"tinyint": -41, "smallint": 3543, "int": 3543, "double": 3543.45654, "decimal": "3543.45654", "bigint": 3543, "varint": 3543, "text": "3543.4565", "varchar": "3543.4565"}}],
        ["123.25993624886543132316548678", {"cast": {"tinyint": 123, "smallint": 123, "int": 123, "double": "123.25993", "decimal": "123.259933", "bigint": 123, "varint": 123, "text": "123.25993", "varchar": "123.25993"}}],
    ],
    "int": [
        [
            0,
            {
                "cast": {"tinyint": 0, "smallint": 0, "float": 0, "double": 0, "decimal": "0.0", "bigint": 0, "varint": 0, "text": "0", "varchar": "0"},
                "avg": {"tinyint": 3, "smallint": -2044, "float": -2044.40002, "double": -2044.4, "decimal": "-2044.4", "bigint": -2044, "varint": -2044, "text": None, "varchar": None},
                "sum": {"tinyint": 18, "smallint": -10222, "float": -10222, "double": -10222, "decimal": "-10222.0", "bigint": -10222, "varint": -10222, "text": None, "varchar": None},
                "min": {"tinyint": -76, "smallint": -32767, "float": -32767, "double": -32767, "decimal": "-32767.0", "bigint": -32767, "varint": -32767, "text": "-12876", "varchar": "-12876"},
                "max": {"tinyint": 94, "smallint": 32767, "float": 32767, "double": 32767, "decimal": "32767.0", "bigint": 32767, "varint": 32767, "text": "32767", "varchar": "32767"},
            },
        ],
        [32767, {"cast": {"tinyint": -1, "smallint": 32767, "float": 32767, "double": 32767, "decimal": "32767.0", "bigint": 32767, "varint": 32767, "text": "32767", "varchar": "32767"}}],
        [-32767, {"cast": {"tinyint": 1, "smallint": -32767, "float": -32767, "double": -32767, "decimal": "-32767.0", "bigint": -32767, "varint": -32767, "text": "-32767", "varchar": "-32767"}}],
        [-12876, {}],
        [2654, {}],
    ],
    "smallint": [
        [
            32767,
            {
                "cast": {"tinyint": -1, "int": 32767, "float": 32767, "double": 32767, "decimal": "32767.0", "bigint": 32767, "varint": 32767, "text": "32767", "varchar": "32767"},
                "avg": {"tinyint": 4, "int": -2555, "float": -2555.5, "double": -2555.5, "decimal": "-2555.5", "bigint": -2555, "varint": -2555, "text": None, "varchar": None},
                "sum": {"tinyint": 18, "int": -10222, "float": -10222, "double": -10222, "decimal": "-10222.0", "bigint": -10222, "varint": -10222, "text": None, "varchar": None},
                "min": {"tinyint": -76, "int": -32767, "float": -32767, "double": -32767, "decimal": "-32767.0", "bigint": -32767, "varint": -32767, "text": "-12876", "varchar": "-12876"},
                "max": {"tinyint": 94, "int": 32767, "float": 32767, "double": 32767, "decimal": "32767.0", "bigint": 32767, "varint": 32767, "text": "32767", "varchar": "32767"},
            },
        ],
        [-32767, {"cast": {"tinyint": 1, "int": -32767, "float": -32767, "double": -32767, "decimal": "-32767.0", "bigint": -32767, "varint": -32767, "text": "-32767", "varchar": "-32767"}}],
        [-12876, {"cast": {"tinyint": -76, "int": -12876, "float": -12876, "double": -12876, "decimal": "-12876.0", "bigint": -12876, "varint": -12876, "text": "-12876", "varchar": "-12876"}}],
        [2654, {"cast": {"tinyint": 94, "int": 2654, "float": 2654, "double": 2654, "decimal": "2654.0", "bigint": 2654, "varint": 2654, "text": "2654", "varchar": "2654"}}],
    ],
    "tinyint": [
        [
            127,
            {
                "cast": {"smallint": 127, "int": 127, "float": 127, "double": 127, "decimal": "127.0", "bigint": 127, "varint": 127, "text": "127", "varchar": "127"},
                "avg": {"smallint": 11, "int": 11, "float": 11.33333, "double": 11.33333, "decimal": "11.3", "bigint": 11, "varint": 11, "text": None, "varchar": None},
                "sum": {"smallint": 34, "int": 34, "float": 34, "double": 34, "decimal": "34.0", "bigint": 34, "varint": 34, "text": None, "varchar": None},
                "min": {"smallint": -128, "int": -128, "float": -128, "double": -128, "decimal": "-128.0", "bigint": -128, "varint": -128, "text": "-128", "varchar": "-128"},
                "max": {"smallint": 127, "int": 127, "float": 127, "double": 127, "decimal": "127.0", "bigint": 127, "varint": 127, "text": "35", "varchar": "35"},
            },
        ],
        [-128, {"cast": {"smallint": -128, "int": -128, "float": -128, "double": -128, "decimal": "-128.0", "bigint": -128, "varint": -128, "text": "-128", "varchar": "-128"}}],
        [35, {"cast": {"smallint": 35, "int": 35, "float": 35, "double": 35, "decimal": "35.0", "bigint": 35, "varint": 35, "text": "35", "varchar": "35"}}],
    ],
    "varint": [
        [
            32767456456456456456545678943512357658768763546575675,
            {
                "cast": {
                    "tinyint": 59,
                    "smallint": 9019,
                    "int": -1446698181,
                    "float": "Infinity",
                    "double": 3.2767e52,
                    "decimal": "32767456456456456456545678943512357658768763546575675",
                    "bigint": -4832518953173179589,
                    "text": "32767456456456456456545678943512357658768763546575675",
                    "varchar": "32767456456456456456545678943512357658768763546575675",
                },
                "avg": {
                    "tinyint": -9,
                    "smallint": 8011,
                    "int": -678398815,
                    "float": "NaN",
                    "double": "1419.33333",
                    "#decimal": "-2110054581",  # commented cause cassandra is rounding the result
                    "bigint": -2110054580,
                    "text": None,
                    "varchar": None,
                },
                "sum": {"tinyint": -29, "smallint": 24035, "int": -2035196445, "float": "NaN", "double": 4258, "decimal": "-6330163741", "bigint": -6330163741, "text": None, "varchar": None},
                "min": {
                    "tinyint": -94,
                    "smallint": 4258,
                    "int": -1446698181,
                    "float": "-Infinity",
                    "double": -3.2767e52,
                    "decimal": "-32767456456456456456545678943512357658768769876743674",
                    "bigint": -4832518953173179589,
                    "text": "-32767456456456456456545678943512357658768769876743674",
                    "varchar": "-32767456456456456456545678943512357658768769876743674",
                },
                "max": {
                    "tinyint": 59,
                    "smallint": 10758,
                    "int": 4258,
                    "float": "Infinity",
                    "double": 3.2767e52,
                    "decimal": "32767456456456456456545678943512357658768763546575675",
                    "bigint": 4832518946843011590,
                    "text": "4258",
                    "varchar": "4258",
                },
                "cast_avg": {"tinyint": 76, "smallint": 8012, "int": -2110054580, "float": "-2.1101e+09", "double": "-2.1101e+09", "decimal": "-2110054580", "bigint": -2110054580, "text": None, "varchar": None},
                "cast_sum": {"tinyint": -29, "smallint": 24035, "int": -2035196445, "float": "-6.3302e+09", "double": "-6.3302e+09", "decimal": "-6330163741", "bigint": -6330163741, "text": None, "varchar": None},
                "cast_min": {
                    "tinyint": 6,
                    "smallint": 10758,
                    "int": -588502522,
                    "float": "-Infinity",
                    "double": "-3.2767e+52",
                    "decimal": "-32767456456456456456545678943512357658768769876743674",
                    "bigint": 4832518946843011590,
                    "text": "-32767456456456456456545678943512357658768769876743674",
                    "varchar": "-32767456456456456456545678943512357658768769876743674",
                },
                "cast_max": {
                    "tinyint": 59,
                    "smallint": 9019,
                    "int": -1446698181,
                    "float": "Infinity",
                    "double": "3.2767e+52",
                    "decimal": "32767456456456456456545678943512357658768763546575675",
                    "bigint": -4832518953173179589,
                    "text": "32767456456456456456545678943512357658768763546575675",
                    "varchar": "32767456456456456456545678943512357658768763546575675",
                },
            },
        ],
        [
            -32767456456456456456545678943512357658768769876743674,
            {
                "cast": {
                    "tinyint": 6,
                    "smallint": 10758,
                    "int": -588502522,
                    "float": "-Infinity",
                    "double": -3.2767e52,
                    "decimal": "-32767456456456456456545678943512357658768769876743674",
                    "bigint": 4832518946843011590,
                    "text": "-32767456456456456456545678943512357658768769876743674",
                    "varchar": "-32767456456456456456545678943512357658768769876743674",
                }
            },
        ],
        [4258, {"cast": {"tinyint": -94, "smallint": 4258, "int": 4258, "float": 4258, "double": 4258, "decimal": "4258.0", "bigint": 4258, "text": "4258", "varchar": "4258"}}],
    ],
    "ascii": [
        ["'045asciitext'", {"cast": {"text": "045asciitext", "varchar": "045asciitext"}, "min": {"text": "045asciitext", "varchar": "045asciitext"}, "max": {"text": "abcdefj", "varchar": "abcdefj"}}],
        ["'abcdefj'", {"cast": {"text": "abcdefj", "varchar": "abcdefj"}}],
    ],
    "date": [
        ["'2017-11-25'", {"cast": {"timestamp": "2017-11-25 00:00:00.000000+0000"}, "min": {"timestamp": "1900-01-01 00:00:00.000000+0000"}, "max": {"timestamp": "2458-12-13 00:00:00.000000+0000"}}],
        ["'1900-01-01'", {"cast": {"timestamp": "1900-01-01 00:00:00.000000+0000"}}],
        ["'2458-12-13'", {"cast": {"timestamp": "2458-12-13 00:00:00.000000+0000"}}],
    ],
    "time": [
        [
            "'13:07:45.089'",
            {"cast": {"text": "13:07:45.089000000", "varchar": "13:07:45.089000000"}, "min": {"text": "00:45:25.123000000", "varchar": "00:45:25.123000000"}, "max": {"text": "13:07:45.089000000", "varchar": "13:07:45.089000000"}},
        ],
        ["'00:45:25.123'", {"cast": {"text": "00:45:25.123000000", "varchar": "00:45:25.123000000"}}],
        ["'10:14:36.0002312'", {"cast": {"text": "10:14:36.000231200", "varchar": "10:14:36.000231200"}}],
    ],
    "timestamp": [
        [
            "'2017-12-27 11:57:42+0000'",
            {
                "cast": {"date": "2017-12-27", "text": "2017-12-27T11:57:42.000Z", "varchar": "2017-12-27T11:57:42.000Z"},
                "min": {"date": "1900-01-01", "text": "1900-01-01T00:00:00.000Z", "varchar": "1900-01-01T00:00:00.000Z"},
                "max": {"date": "2018-02-01", "text": "2018-02-01T01:00:42.000Z", "varchar": "2018-02-01T01:00:42.000Z"},
            },
        ],
        [
            "'2017-12-27 11:57:42+0100'",  # timezone offset
            {
                "cast": {"date": "2017-12-27", "text": "2017-12-27T10:57:42.000Z", "varchar": "2017-12-27T10:57:42.000Z"},
            },
        ],
        [
            "'2017-12-27 11:57:42.500+0100'",  # with milliseconds, scylla is returning with 6 digit vs. cassandra with 3 digits
            {
                "cast": {"date": "2017-12-27", "text": "2017-12-27T10:57:42.500Z", "varchar": "2017-12-27T10:57:42.500Z"},
            },
        ],
        ["'2018-02-01 01:00:42+0000'", {"cast": {"date": "2018-02-01", "text": "2018-02-01T01:00:42.000Z", "varchar": "2018-02-01T01:00:42.000Z"}}],
        ["'1900-01-01 00:00:00+0000'", {"cast": {"date": "1900-01-01", "text": "1900-01-01T00:00:00.000Z", "varchar": "1900-01-01T00:00:00.000Z"}}],
    ],
    "timeuuid": [
        [
            "minTimeuuid('2018-01-01 17:01:07+0000')",
            {
                "cast": {"timestamp": "2018-01-01 17:01:07.000000+0000", "date": "2018-01-01", "text": "5bb48b80-ef15-11e7-8080-808080808080", "varchar": "5bb48b80-ef15-11e7-8080-808080808080"},
                "min": {"timestamp": "1900-01-01 00:00:00.000000+0000", "date": "1900-01-01", "text": "3c230000-a32f-1163-8080-808080808080", "varchar": "3c230000-a32f-1163-8080-808080808080"},
                "max": {"timestamp": "2018-01-01 17:01:07.000000+0000", "date": "2018-01-01", "text": "7a73e600-eb01-11e7-8080-808080808080", "varchar": "7a73e600-eb01-11e7-8080-808080808080"},
            },
        ],
        ["minTimeuuid('2017-12-27 12:28:44+0000')", {"cast": {"timestamp": "2017-12-27 12:28:44.000000+0000", "date": "2017-12-27", "text": "7a73e600-eb01-11e7-8080-808080808080", "varchar": "7a73e600-eb01-11e7-8080-808080808080"}}],
        ["minTimeuuid('1900-01-01 00:00:00+0000')", {"cast": {"timestamp": "1900-01-01 00:00:00.000000+0000", "date": "1900-01-01", "text": "3c230000-a32f-1163-8080-808080808080", "varchar": "3c230000-a32f-1163-8080-808080808080"}}],
    ],
    "uuid": [
        [
            "de5cba0d-41a2-4f39-8834-35130d8b5d86",
            {
                "cast": {"text": "de5cba0d-41a2-4f39-8834-35130d8b5d86", "varchar": "de5cba0d-41a2-4f39-8834-35130d8b5d86"},
                "min": {"text": "de5cba0d-41a2-4f39-8834-35130d8b5d86", "varchar": "de5cba0d-41a2-4f39-8834-35130d8b5d86"},
                "max": {"text": "fa80080c-a4c5-46d6-afe4-5e184fec35ae", "varchar": "fa80080c-a4c5-46d6-afe4-5e184fec35ae"},
            },
        ],
        ["f5f576e7-efb6-41b5-804e-fa3205548137", {"cast": {"text": "f5f576e7-efb6-41b5-804e-fa3205548137", "varchar": "f5f576e7-efb6-41b5-804e-fa3205548137"}}],
        ["fa80080c-a4c5-46d6-afe4-5e184fec35ae", {"cast": {"text": "fa80080c-a4c5-46d6-afe4-5e184fec35ae", "varchar": "fa80080c-a4c5-46d6-afe4-5e184fec35ae"}}],
    ],
    "inet": [
        ["'205.118.5.3'", {"cast": {"text": "205.118.5.3", "varchar": "205.118.5.3"}, "min": {"text": "192.168.122.255", "varchar": "192.168.122.255"}, "max": {"text": "205.118.5.3", "varchar": "205.118.5.3"}}],
        ["'192.168.2.1'", {"cast": {"text": "192.168.2.1", "varchar": "192.168.2.1"}}],
        ["'192.168.122.255'", {"cast": {"text": "192.168.122.255", "varchar": "192.168.122.255"}}],
    ],
    "udt": {
        "bigint": [
            [
                4561313648948,
                {
                    "cast": {"decimal": "4561313648948.0", "text": "4561313648948"},
                    "avg": {"decimal": "2280656551235.0", "text": None},
                    "sum": {"decimal": "4561313102470.0", "text": None},
                    "min": {"decimal": "-546478.0", "text": "-546478"},
                    "max": {"decimal": "4561313648948.0", "text": "4561313648948"},
                    "cast_avg": {"decimal": "2280656551235.0", "text": "2280656551235"},
                    "cast_sum": {"decimal": "4561313102470.0", "text": "4561313102470"},
                    "cast_min": {"decimal": "-546478.0", "text": "-546478"},
                    "cast_max": {"decimal": "4561313648948.0", "text": "4561313648948"},
                    "cast_cast": {"decimal/float": "4.5613e+12", "smallint/text": "-11980"},
                },
            ],
            [-546478, {"cast": {"decimal": "-546478.0", "text": "-546478"}}],
        ],
        "float": [[55646.234556984, {"cast": {"int": 55646, "double": "55646.23438"}}]],
        "timestamp": [["'2017-12-27 11:57:42+0000'", {"cast": {"date": "2017-12-27", "text": "2017-12-27T11:57:42.000Z"}}]],
    },
}

COLUMN_NAME_TEMPLATE = "{}_clmn"

skip_dev_debug = pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")

# The query of one check, like the dtest's TESTS functions.
def cast_query(test, column_name, to_type, row_id, table_name):
    if test == "cast":
        return f"SELECT cast({column_name} as {to_type}) FROM {table_name} WHERE k={row_id}"
    if test == "cast_cast":
        to_types = to_type.split("/")
        return f"SELECT cast(cast({column_name} as {to_types[0]}) as {to_types[1]}) FROM {table_name} where k={row_id}"
    if test.startswith("cast_"):
        return f"SELECT cast({test.replace('cast_', '')}({column_name}) as {to_type}) FROM {table_name}"
    return f"SELECT {test}(cast({column_name} as {to_type})) FROM {table_name}"

# The checks of one source type, (query, from_type, to_type, expected), in
# the order of the dtest's _test_one_type() (with exclude=True).
def cast_checks(from_type, rows, column_name, table_name):
    checks = []
    for row_id, exp_results in enumerate(rows):
        for test, test_exp_result in exp_results[1].items():
            if test.startswith("#"):
                continue
            for to_type, exp_result in test_exp_result.items():
                if to_type.startswith("#"):
                    continue
                if exp_result:
                    checks.append((cast_query(test, column_name, to_type, row_id, table_name), from_type, to_type, exp_result))
    return checks

# Compare a value printed by cqlsh with the expected one, as the dtest does.
def check_cast_result(actual_result, from_type, to_type, exp_result, query):
    if to_type in ["text", "varchar"] and from_type == "timestamp":
        # scylla omit zero milliseconds
        actual_result = actual_result.removesuffix(".000Z")
        exp_result = str(exp_result).removesuffix(".000Z")
    if to_type in ["decimal", "text", "varchar"]:
        # in cassandra decimal omits .0, so we compare without it
        # https://github.com/scylladb/scylladb/issues/3109
        actual_result = actual_result.removesuffix(".0")
        exp_result = str(exp_result).removesuffix(".0")
    if to_type in ["decimal"] and from_type in ["float", "double"]:
        assert float(exp_result) == pytest.approx(float(actual_result), rel=1e-6), f"failed casting from type [{from_type}] to type [{to_type}]\n\tquery=[{query}]"
        return
    assert actual_result == str(exp_result), f"failed casting from type [{from_type}] to type [{to_type}]\n\tquery=[{query}]"

# Run the statements creating and filling the table, then the checks' queries,
# then the cleanup statements, in one cqlsh run, and compare each query's
# single printed value with the expected one.
def run_cast_checks(cql, keyspace, stmts, checks, cleanup):
    res = run_cqlsh(cql, "; ".join([f"USE {keyspace}", *stmts, *(query for query, *_ in checks), *cleanup]) + ";")
    assert res.returncode == 0, res.stderr
    lines = res.stdout.split("\n")
    values = [lines[i - 2].strip() for i, line in enumerate(lines) if line.strip() == "(1 rows)"]
    assert len(values) == len(checks), res.stdout
    for value, (query, from_type, to_type, exp_result) in zip(values, checks):
        check_cast_result(value, from_type, to_type, exp_result, query)

# Like the dtest's _test_run(): a table cast_<type>_test with a column
# <type>_clmn, filled with the type's values in rows k=0,1,..., and the
# checks of the type.
def cast_test_run(cql, keyspace, from_type):
    rows = POSITIVE_VALUES[from_type]
    table_name = f"cast_{from_type}_test"
    column_name = COLUMN_NAME_TEMPLATE.format(from_type)
    stmts = [f"CREATE TABLE IF NOT EXISTS {table_name} (k int PRIMARY KEY, {column_name} {from_type})"]
    stmts += [f"INSERT INTO {table_name} (k, {column_name}) VALUES ({_id}, {value[0]})" for _id, value in enumerate(rows)]
    run_cast_checks(cql, keyspace, stmts, cast_checks(from_type, rows, column_name, table_name), [f"DROP TABLE {table_name}"])

# Like the dtest's _udt_test_run(): a UDT <fromt>_for_cast_test with a field
# <type>_clmn per source type, a table <fromt>_table with a frozen column of
# it, row k=N holding the N-th value of each field, and the checks of each
# field (cast(<fromt>_clmn.<type>_clmn as ...)).
def udt_cast_test_run(cql, keyspace, fromt):
    data = POSITIVE_VALUES[fromt]
    type_name = f"{fromt}_for_cast_test"
    table_name = f"{fromt}_table"
    column_name = COLUMN_NAME_TEMPLATE.format(fromt)
    fields = ", ".join(f"{COLUMN_NAME_TEMPLATE.format(from_type)} {from_type}" for from_type in data)
    stmts = [f"CREATE TYPE {type_name} ({fields})",
             f"CREATE TABLE IF NOT EXISTS {table_name} (k int PRIMARY KEY, {column_name} frozen <{type_name}>)"]
    for _id in range(max(len(values) for values in data.values())):
        row = [f"{COLUMN_NAME_TEMPLATE.format(from_type)}: {values[_id][0]}" for from_type, values in data.items() if len(values) > _id]
        if row:
            stmts.append(f"INSERT INTO {table_name} (k, {column_name}) VALUES ({_id}, {{{', '.join(row)}}})")
    checks = []
    for from_type, rows in data.items():
        udt_column_name = f"{column_name}.{COLUMN_NAME_TEMPLATE.format(from_type)}"
        checks += cast_checks(from_type, rows, udt_column_name, table_name)
    run_cast_checks(cql, keyspace, stmts, checks, [f"DROP TABLE {table_name}", f"DROP TYPE {type_name}"])

# Ported from scylla-dtest cql_cast_test.py::TestCQLCast::test_cast_udt.
# Casts of the bigint, float and timestamp fields of a UDT column, aggregates
# of casts of the bigint field, casts of its aggregates and casts of casts.
@skip_dev_debug
def test_cast_udt(cql, test_keyspace_vnodes):
    udt_cast_test_run(cql, test_keyspace_vnodes, "udt")

# Ported from scylla-dtest cql_cast_test.py::TestCQLCast::test_cast_varint.
# Casts of huge varints to narrower numeric types (wrap around, infinite
# floats) and to text, with aggregates and casts of aggregates.
@skip_dev_debug
def test_cast_varint(cql, test_keyspace_vnodes):
    cast_test_run(cql, test_keyspace_vnodes, "varint")

# Ported from scylla-dtest cql_cast_test.py::TestCQLCast::test_cast_tinyint.
# Casts of tinyints to wider numeric types and to text, with aggregates.
@skip_dev_debug
def test_cast_tinyint(cql, test_keyspace_vnodes):
    cast_test_run(cql, test_keyspace_vnodes, "tinyint")

# Ported from scylla-dtest cql_cast_test.py::TestCQLCast::test_cast_smallint.
# Casts of smallints to other numeric types and to text, with aggregates.
@skip_dev_debug
def test_cast_smallint(cql, test_keyspace_vnodes):
    cast_test_run(cql, test_keyspace_vnodes, "smallint")

# Ported from scylla-dtest cql_cast_test.py::TestCQLCast::test_cast_int.
# Casts of ints to other numeric types and to text, with aggregates.
@skip_dev_debug
def test_cast_int(cql, test_keyspace_vnodes):
    cast_test_run(cql, test_keyspace_vnodes, "int")

# Ported from scylla-dtest cql_cast_test.py::TestCQLCast::test_cast_bigint.
# Casts of bigints to other numeric types (wrapping around) and to text,
# with aggregates.
@skip_dev_debug
def test_cast_bigint(cql, test_keyspace_vnodes):
    cast_test_run(cql, test_keyspace_vnodes, "bigint")

# Ported from scylla-dtest cql_cast_test.py::TestCQLCast::test_cast_float.
# Casts of floats to integer types (truncating), double, decimal and text,
# with aggregates.
@skip_dev_debug
def test_cast_float(cql, test_keyspace_vnodes):
    cast_test_run(cql, test_keyspace_vnodes, "float")

# Ported from scylla-dtest cql_cast_test.py::TestCQLCast::test_cast_double.
# Casts of doubles to integer types (truncating), float, decimal and text,
# with aggregates.
@skip_dev_debug
def test_cast_double(cql, test_keyspace_vnodes):
    cast_test_run(cql, test_keyspace_vnodes, "double")

# Ported from scylla-dtest cql_cast_test.py::TestCQLCast::test_cast_decimal.
# Casts of decimals to the other numeric types and to text, with aggregates,
# casts of aggregates and a cast of a cast.
@skip_dev_debug
def test_cast_decimal(cql, test_keyspace_vnodes):
    cast_test_run(cql, test_keyspace_vnodes, "decimal")

# Ported from scylla-dtest cql_cast_test.py::TestCQLCast::test_cast_date.
# Casts of dates to timestamp, with min() and max().
@skip_dev_debug
def test_cast_date(cql, test_keyspace_vnodes):
    cast_test_run(cql, test_keyspace_vnodes, "date")

# Ported from scylla-dtest cql_cast_test.py::TestCQLCast::test_cast_time.
# Casts of times to text, with min() and max().
@skip_dev_debug
def test_cast_time(cql, test_keyspace_vnodes):
    cast_test_run(cql, test_keyspace_vnodes, "time")

# Ported from scylla-dtest cql_cast_test.py::TestCQLCast::test_cast_timestamp.
# Casts of timestamps (with timezone offsets and milliseconds) to date and
# text, with min() and max(). @unmark.next_gating in a dtest_full class.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
def test_cast_timestamp(cql, test_keyspace_vnodes):
    cast_test_run(cql, test_keyspace_vnodes, "timestamp")

# Ported from scylla-dtest cql_cast_test.py::TestCQLCast::test_cast_uuid.
# Casts of uuids to text, with min() and max().
@skip_dev_debug
def test_cast_uuid(cql, test_keyspace_vnodes):
    cast_test_run(cql, test_keyspace_vnodes, "uuid")

# Ported from scylla-dtest cql_cast_test.py::TestCQLCast::test_cast_boolean.
# Casts of booleans to text.
@skip_dev_debug
def test_cast_boolean(cql, test_keyspace_vnodes):
    cast_test_run(cql, test_keyspace_vnodes, "boolean")

# Ported from scylla-dtest cql_cast_test.py::TestCQLCast::test_cast_ascii.
# Casts of ascii values to text, with min() and max().
@skip_dev_debug
def test_cast_ascii(cql, test_keyspace_vnodes):
    cast_test_run(cql, test_keyspace_vnodes, "ascii")

# Ported from scylla-dtest cql_cast_test.py::TestCQLCast::test_cast_inet.
# Casts of inet addresses to text, with min() and max().
@skip_dev_debug
def test_cast_inet(cql, test_keyspace_vnodes):
    cast_test_run(cql, test_keyspace_vnodes, "inet")

# Ported from scylla-dtest cql_cast_test.py::TestCQLCast::test_cast_timeuuid.
# Casts of timeuuids to timestamp, date and text, with min() and max().
@skip_dev_debug
def test_cast_timeuuid(cql, test_keyspace_vnodes):
    cast_test_run(cql, test_keyspace_vnodes, "timeuuid")
