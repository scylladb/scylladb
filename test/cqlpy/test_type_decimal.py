# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#############################################################################
# Test involving the "decimal" column type.
# There are additional tests involving decimals in specific contexts in other
# files - e.g., aggregating decimals in test_aggregate.py, casting decimals
# in test_cast_data.py, and decimals in JSON in test_json.py
#############################################################################

from decimal import Decimal
import pytest
from cassandra.protocol import InvalidRequest

from . import nodetool
from .util import new_test_table, unique_key_int

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c decimal, PRIMARY KEY (p, c)") as table:
        yield table

# Test that if we have clustering keys of wildly different scales, sorting
# them works. Reproduces issue #21716 where comparing two decimals of
# very different scales needed to expand the digits of one of them and
# would take huge amounts of CPU and also run out of memory.
# See also test_json.py::test_json_decimal_high_mantissa where printing
# a decimal runs into a similar problem.
def test_decimal_clustering_key_high_exponent(cql, table1):
    # The two numbers "low" and "high" have wildly different exponents.
    # Any algorithm that attempts to expand the digits of one to have the
    # same exponent as the other will run out of memory (#21716).
    low = Decimal('19866597869857659876855e-1000000000')
    high = Decimal('19866597869857659876855e1000000000')
    minuslow = Decimal('-19866597869857659876855e-1000000000')
    minushigh = Decimal('-19866597869857659876855e1000000000')
    p = unique_key_int()
    stmt = cql.prepare(f"INSERT INTO {table1} (p, c) VALUES ({p}, ?)")
    cql.execute(stmt, [high])
    cql.execute(stmt, [low])
    cql.execute(stmt, [minushigh])
    cql.execute(stmt, [minuslow])
    assert list(cql.execute(f"SELECT c from {table1} where p = {p}")) == [(minushigh,),(minuslow,),(low,),(high,)]
    # Do a memtable flush, just to make sure the memtable-flushing or sstable
    # writing code also doesn't use problematic decimal-handling algorithms.
    nodetool.flush(cql, table1)

# Test initializing "decimal" columns using inline literals in the CQL
# commands instead of using prepared statements as in the previous test
# (test_decimal_clustering_key_high_exponent). We check both very high
# exponents, and many significant digits, both of which cannot be properly
# parsed as double-precision and must be parsed correctly as a decimal.
# Both tests worked well in Cassandra 3, but the high exponent one regressed
# in Cassandra 4 and 5 (CASSANDRA-20723).
def test_decimal_clustering_key_inline_high_exponent(cql, table1, cassandra_bug):
    p = unique_key_int()
    sorted_numbers = ['1e309', '19866597869857659876855e1000000000']
    for n in sorted_numbers:
        cql.execute(f"INSERT INTO {table1} (p, c) VALUES ({p}, {n})")
    assert list(cql.execute(f"SELECT c from {table1} where p = {p}")) == [
            (Decimal(n),) for n in sorted_numbers]

def test_decimal_clustering_key_inline_high_precision(cql, table1):
    p = unique_key_int()
    sorted_numbers = ['3.14159265358979323846264338327950288419716939937510',
               '314159265358979323846264338327950288419716939937510']
    for n in sorted_numbers:
        cql.execute(f"INSERT INTO {table1} (p, c) VALUES ({p}, {n})")
    assert list(cql.execute(f"SELECT c from {table1} where p = {p}")) == [
            (Decimal(n),) for n in sorted_numbers]

# Test that exponents that are too large generate an error when parsing
# a CQL inline literal (not prepared statement) instead of being incorrectly
# parsed. This test reproduces issue #24581.
# Cassandra's documentation specifies that the "decimal" type is implemented
# by Java's java.math.BigDecimal class. The documentation of that class
# explains about its concept of "scale" and its limitation:
#    "The scale of the returned BigDecimal will be the number of digits in
#     the fraction, or zero if the string contains no decimal point, subject
#     to adjustment for any exponent; if the string contains an exponent,
#     the exponent is subtracted from the scale. The value of the resulting
#     scale must lie between Integer.MIN_VALUE and Integer.MAX_VALUE,
#     inclusive."
#     [note: Integer.MIN_VALUE=-2147483648, Integer.MAX_VALUE=2147483647]
# This test passes on Cassandra 3 but fails on Cassandra 4 and 5 due to
# CASSANDRA-20723 which limits the exponent to 309.
def test_decimal_clustering_key_inline_overflow_exponent(cql, table1):
    p = unique_key_int()
    # The following numbers all have an exponent that is itself (without
    # any fractional part) already above the scale limit.
    # Trying to use such a number in inline CQL should result in an
    # InvalidRequest and the text "unable to make BigDecimal from
    #'1e2147483649'." in both Scylla and Cassandra 3,
    for n in ['1e2147483649','-1e2147483649', '1e-2147483648', '-1e-2147483648']:
        with pytest.raises(InvalidRequest, match="BigDecimal"):
            cql.execute(f"INSERT INTO {table1} (p, c) VALUES ({p}, {n})")
    # The number 1e-2147483647 has (according to the documentation quoted
    # above) a "scale" of 2147483647 which is allowed.
    n = '1e2147483647'
    cql.execute(f"INSERT INTO {table1} (p, c) VALUES ({p}, {n})")
    assert list(cql.execute(f"SELECT c from {table1} where p = {p}")) == [(Decimal(n),)]
    # The number 1.1e+2147483647 has, according to the same documentation,
    # a "scale" of 1 (digits in the fraction) minus 2147483647(minus of
    # exponent) so -2147483646 which is allowed
    p = unique_key_int()
    n = '1.1e2147483647'
    cql.execute(f"INSERT INTO {table1} (p, c) VALUES ({p}, {n})")
    # However the number 1.1e-2147483647 has a scale of 1 (digits in the
    # fraction) + 2147483647(minus of exponent) so 2147483648 which, is not
    # supposed to not be allowed.
    p = unique_key_int()
    n = '1.1e-2147483647'
    with pytest.raises(InvalidRequest, match="BigDecimal"):
        cql.execute(f"INSERT INTO {table1} (p, c) VALUES ({p}, {n})")
        # If for some reason the INSERT doesn't fail, let's check if somehow
        # we managed to save the parsed number. If the INSERT succeeded but
        # the read reads something different, it's an even worse bug
        assert list(cql.execute(f"SELECT c from {table1} where p = {p}")) == [(Decimal(n),)]
    # Here is an even more obvious reproducer of issue #24581. The number
    # 1.1234e-2147483647 has a scale of 4 (digits in the fraction) +
    # 2147483647 (minus of exponent) so 2147483651 which, is not allowed.
    p = unique_key_int()
    n = '1.1234e-2147483647'
    with pytest.raises(InvalidRequest, match="BigDecimal"):
        cql.execute(f"INSERT INTO {table1} (p, c) VALUES ({p}, {n})")
        # In issue #24581 the following assert fails - we tried to save
        # 1.1234e-2147483647 but what we read is 1.1234E+2147483649 - the
        # "scale" got wrapped around the 32 bit integer.
        assert list(cql.execute(f"SELECT c from {table1} where p = {p}")) == [(Decimal(n),)]
