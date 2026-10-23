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
# type to another in sometimes surprising ways. For example, a decimal cast
# into an integer is truncated, and a number that does not fit the target type
# fails the cast (where Cassandra wraps around or gives an infinity). In this
# test file we'll check some of these conversions, and verify that they are
# the same in Scylla and Cassandra, where they should be, and don't regress.
#
# See also test_cast_all_types.py.
###############################################################################

import pytest
import math
from .util import new_test_table, unique_key_int
from cassandra.protocol import InvalidRequest, FunctionFailure
from ctypes import c_float
from decimal import Decimal

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

# Test casting a very large varint number to various other types, resulting
# in various non-trivial conversions including truncation of numbers.
# Casting it to integer types or float fails; see test_cast_to_narrower_integer
# and test_cast_to_float_out_of_range.
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
    # Casting into smaller integer types works when the number fits. When it
    # does not, as for tinyint, see test_cast_to_narrower_integer.
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

# Casting a float or double to an integer type truncates toward zero, and
# fails if the result does not fit the type, as in SQL. Scylla's conversion
# used to be undefined behavior there, which aborted debug builds (e.g. when
# casting -564.0 to tinyint). Cassandra wraps around instead, as Java does,
# so this test is Scylla-only.
def test_cast_float_to_integer(cql, table3, scylla_only):
    p = unique_key_int()
    # (value, tinyint, smallint, int, bigint), None where the cast fails
    cases = [(2.7, 2, 2, 2, 2),
             (-1.9, -1, -1, -1, -1),
             (-0.5, 0, 0, 0, 0),
             (127.9, 127, 127, 127, 127),
             (-128.9, -128, -128, -128, -128),
             (128.0, None, 128, 128, 128),
             (-564.0, None, -564, -564, -564),
             (1e10, None, None, None, 10000000000),
             (-2147483648.0, None, None, -2147483648, -2147483648),
             (2147483648.0, None, None, None, 2147483648),
             (-9.223372036854776e+18, None, None, None, -9223372036854775808),
             (9.223372036854776e+18, None, None, None, None),
             (1e30, None, None, None, None)]
    for v, *expected in cases:
        cql.execute(f'INSERT INTO {table3} (p, f, db) VALUES ({p}, {v}, {v})')
        for col in ['f', 'db']:
            for t, e in zip(['tinyint', 'smallint', 'int', 'bigint'], expected):
                q = f"SELECT CAST({col} AS {t}) FROM {table3} WHERE p={p}"
                if e is None:
                    with pytest.raises(FunctionFailure, match='out of range'):
                        cql.execute(q)
                else:
                    assert [(e,)] == list(cql.execute(q))
    for lit in ['NaN', 'Infinity', '-Infinity']:
        cql.execute(f'INSERT INTO {table3} (p, f, db) VALUES ({p}, {lit}, {lit})')
        for col in ['f', 'db']:
            for t in ['tinyint', 'smallint', 'int', 'bigint']:
                with pytest.raises(FunctionFailure, match='out of range'):
                    cql.execute(f"SELECT CAST({col} AS {t}) FROM {table3} WHERE p={p}")

# Casting a number to a narrower integer type fails if it does not fit, as in
# SQL. Cassandra wraps around instead, as Java does, so this test is
# Scylla-only. A decimal is truncated toward zero first.
def test_cast_to_narrower_integer(cql, table2, table3, scylla_only):
    p = unique_key_int()
    targets = {'tinyint': 8, 'smallint': 16, 'int': 32, 'bigint': 64}
    for col, bits in [('si', 16), ('i', 32), ('bi', 64), ('vi', None), ('dec', None)]:
        for t, tbits in targets.items():
            if bits and bits <= tbits:
                continue
            lo, hi = -2**(tbits-1), 2**(tbits-1)-1
            for v in [lo - 1, lo, hi, hi + 1]:
                cql.execute(f'INSERT INTO {table3} (p, {col}) VALUES ({p}, {v})')
                q = f"SELECT CAST({col} AS {t}) FROM {table3} WHERE p={p}"
                if lo <= v <= hi:
                    assert [(v,)] == list(cql.execute(q))
                else:
                    with pytest.raises(FunctionFailure, match='out of range'):
                        cql.execute(q)
    for v, expected in [('127.9', 127), ('-128.9', -128), ('128.0', None), ('-129.5', None)]:
        cql.execute(f'INSERT INTO {table3} (p, dec) VALUES ({p}, {v})')
        q = f"SELECT CAST(dec AS tinyint) FROM {table3} WHERE p={p}"
        if expected is None:
            with pytest.raises(FunctionFailure, match='out of range'):
                cql.execute(q)
        else:
            assert [(expected,)] == list(cql.execute(q))
    cql.execute(f'UPDATE {table2} SET c = c + 1000 WHERE p = {p}')
    with pytest.raises(FunctionFailure, match='out of range'):
        cql.execute(f"SELECT CAST(c AS tinyint) FROM {table2} WHERE p={p}")

# Casting a float or double to varint is exact, truncating toward zero. NaN
# and infinities have no integer value, so the cast fails; Scylla used to
# report that as a server error. Cassandra instead casts through a bigint,
# saturating at its limits and taking NaN as 0, so there 1e30 becomes
# 9223372036854775807. Hence this test is Scylla-only.
def test_cast_float_to_varint(cql, table3, scylla_only):
    p = unique_key_int()
    for v in [2.7, -1.9, 1e30, -1e30]:
        cql.execute(f'INSERT INTO {table3} (p, f, db) VALUES ({p}, {v}, {v})')
        assert [(int(c_float(v).value), int(v))] == list(cql.execute(
            f"SELECT CAST(f AS varint), CAST(db AS varint) FROM {table3} WHERE p={p}"))
    for lit in ['NaN', 'Infinity', '-Infinity']:
        cql.execute(f'INSERT INTO {table3} (p, f, db) VALUES ({p}, {lit}, {lit})')
        for col in ['f', 'db']:
            with pytest.raises(FunctionFailure, match='out of range'):
                cql.execute(f"SELECT CAST({col} AS varint) FROM {table3} WHERE p={p}")

# Casting a double to decimal keeps its shortest exact representation.
def test_cast_float_to_decimal(cql, table3):
    p = unique_key_int()
    for v in ['2.7', '-1.9', '1e+30']:
        cql.execute(f'INSERT INTO {table3} (p, db) VALUES ({p}, {v})')
        assert [(Decimal(v),)] == list(cql.execute(
            f"SELECT CAST(db AS decimal) FROM {table3} WHERE p={p}"))

# NaN and infinities have no decimal value, so casting them to decimal fails.
# Cassandra fails it too, but with a server error (NumberFormatException).
def test_cast_nan_to_decimal(cql, table3, cassandra_bug):
    p = unique_key_int()
    for lit in ['NaN', 'Infinity', '-Infinity']:
        cql.execute(f'INSERT INTO {table3} (p, f, db) VALUES ({p}, {lit}, {lit})')
        for col in ['f', 'db']:
            with pytest.raises(FunctionFailure):
                cql.execute(f"SELECT CAST({col} AS decimal) FROM {table3} WHERE p={p}")

# Same for a float, which keeps the shortest representation of the float.
# Cassandra widens it to double first (CASSANDRA-18647), so there 2.7 becomes
# 2.700000047683716.
def test_cast_float_column_to_decimal(cql, table3, cassandra_bug):
    p = unique_key_int()
    for v in ['2.7', '-1.9', '1e+30']:
        cql.execute(f'INSERT INTO {table3} (p, f) VALUES ({p}, {v})')
        assert [(Decimal(v),)] == list(cql.execute(
            f"SELECT CAST(f AS decimal) FROM {table3} WHERE p={p}"))

# Casting a number beyond the range of float or double to it fails, as in
# SQL, rather than giving an infinity as in Cassandra (Java). Hence this test
# is Scylla-only. "Beyond" means rounding to nearest would give an infinity,
# whatever the source type. Infinities and NaN cast as they are.
def test_cast_to_float_out_of_range(cql, table3, scylla_only):
    p = unique_key_int()
    float_max = 3.4028234663852886e+38
    # From here on, rounding to float gives an infinity: float_max plus half
    # a unit in its last place, 2**103.
    float_overflow = 340282356779733661637539395458142568448
    # (column, value, float, double), None where the cast fails
    cases = [('db', float_max, float_max, float_max),
             ('db', -float_max, -float_max, -float_max),
             ('db', '3.4028235e38', float_max, 3.4028235e38),
             ('db', float(float_overflow), None, float(float_overflow)),
             ('vi', 340282350000000000000000000000000000000, float_max, 3.4028235e38),
             ('vi', float_overflow - 1, float_max, float(float_overflow)),
             ('vi', float_overflow, None, float(float_overflow)),
             ('dec', '3.4028235e38', float_max, 3.4028235e38),
             ('db', '1e300', None, 1e300),
             ('db', '-1e300', None, -1e300),
             ('db', 'Infinity', math.inf, math.inf),
             ('db', '-Infinity', -math.inf, -math.inf),
             ('vi', 10**38, 1e38, 1e38),
             ('vi', 10**39, None, 1e39),
             ('vi', -10**39, None, -1e39),
             ('vi', 10**309, None, None),
             ('dec', '1e38', 1e38, 1e38),
             ('dec', '1e39', None, 1e39),
             ('dec', '-1e39', None, -1e39),
             ('dec', '1e309', None, None)]
    for col, v, *expected in cases:
        cql.execute(f'INSERT INTO {table3} (p, {col}) VALUES ({p}, {v})')
        for t, e in zip(['float', 'double'], expected):
            q = f"SELECT CAST({col} AS {t}) FROM {table3} WHERE p={p}"
            if e is None:
                with pytest.raises(FunctionFailure, match='out of range'):
                    cql.execute(q)
            elif t == 'float':
                assert [(c_float(e).value,)] == list(cql.execute(q))
            else:
                assert [(e,)] == list(cql.execute(q))
    cql.execute(f'INSERT INTO {table3} (p, db) VALUES ({p}, NaN)')
    assert math.isnan(list(cql.execute(f"SELECT CAST(db AS float) FROM {table3} WHERE p={p}"))[0][0])

# TODO: test casts from more types.
