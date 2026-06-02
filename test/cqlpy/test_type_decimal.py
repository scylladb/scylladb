# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Test involving the "decimal" column type.
# There are additional tests involving decimals in specific contexts in other
# files - e.g., aggregating decimals in test_aggregate.py, casting decimals
# in test_cast_data.py, and decimals in JSON in test_json.py
#############################################################################

from decimal import Decimal
import sys
from contextlib import contextmanager
import pytest
from cassandra.protocol import InvalidRequest, FunctionFailure

from . import nodetool
from .util import new_test_table, unique_key_int, new_materialized_view, new_secondary_index
from .test_materialized_view import wait_for_view_built
from .test_secondary_index import wait_for_index

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
def test_decimal_clustering_key_inline_overflow_exponent(cql, table1, cassandra_bug):
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

# Verify that decimal partition keys and clustering keys have fundamentally
# different identity semantics:
#
#   - Partition key: raw-bytes identity. Two different (scale, unscaled)
#     representations of the same numeric value serialize to different bytes,
#     produce different tokens, and live in different partitions.
#
#   - Clustering key: value-based comparison (DecimalType.compare() delegates
#     to BigDecimal.compareTo()). Different representations of the same value
#     are the same clustering key - a second INSERT overwrites the first.
#     The stored clustering key bytes are NOT normalized; the last write's
#     representation is preserved.
#
# This is consistent with Cassandra.
#
# NOTE: Python's Decimal normalizes some string forms - e.g. Decimal('123E+1')
# and Decimal('1.23E+3') produce the same as_tuple() - so the CQL driver
# would serialize them identically.  We use the Decimal tuple constructor
# where needed to get genuinely distinct wire representations of the same
# numeric value.
def test_decimal_key_representation(cql, test_keyspace):
    # Three representations of the numeric value 1230, each with a distinct
    # (unscaled, scale) pair and therefore distinct wire bytes:
    d1 = Decimal('1230')                   # as_tuple: (0, (1,2,3,0), 0)
    d2 = Decimal('1.23E+3')               # as_tuple: (0, (1,2,3), 1)
    d3 = Decimal((0, (1,2,3,0,0), -1))    # as_tuple: (0, (1,2,3,0,0), -1)
    d_other = Decimal('456')
    # Sanity: all three are numerically equal but have distinct as_tuple()
    # (i.e., distinct (sign, digits, exponent) triples), meaning the CQL
    # driver will serialize them to different wire bytes.
    assert d1 == d2 == d3
    assert len({d1.as_tuple(), d2.as_tuple(), d3.as_tuple()}) == 3

    with new_test_table(cql, test_keyspace,
            "p decimal, c decimal, v text, PRIMARY KEY (p, c)") as table:
        stmt = cql.prepare(
            f"INSERT INTO {table} (p, c, v) VALUES (?, ?, ?)")

        # Insert 1: (p=d1, c=d1, v='a') - new row in partition d1.
        cql.execute(stmt, [d1, d1, 'a'])
        # Insert 2: (p=d1, c=d2, v='b') - same PK bytes as insert 1, same CK
        # *value* but different CK representation -> overwrites insert 1.
        cql.execute(stmt, [d1, d2, 'b'])
        # Insert 3: (p=d1, c=d_other, v='c') - same partition, different CK
        # value -> new row alongside the overwritten one.
        cql.execute(stmt, [d1, d_other, 'c'])
        # Insert 4: (p=d2, c=d1, v='d') - different PK representation -> different
        # partition entirely, despite the same numeric PK value.
        cql.execute(stmt, [d2, d1, 'd'])

        # --- Partition d1: should have exactly 2 rows ---
        rows_d1 = list(cql.execute(
            cql.prepare(f"SELECT c, v FROM {table} WHERE p = ?"),
            [d1]))
        assert len(rows_d1) == 2
        # One row for CK~=1230 (overwritten to 'b') and one for CK=456 ('c').
        by_v = {r.v: r for r in rows_d1}
        assert set(by_v.keys()) == {'b', 'c'}
        # The CK of the overwritten row carries the first writer's
        # representation (d1), proving the CK bytes are not normalized.
        assert by_v['b'].c.as_tuple() == d1.as_tuple()
        # The other row's CK should be exactly as written.
        assert by_v['c'].c.as_tuple() == d_other.as_tuple()

        # --- Partition d2: should have exactly 1 row ---
        rows_d2 = list(cql.execute(
            cql.prepare(f"SELECT c, v FROM {table} WHERE p = ?"),
            [d2]))
        assert len(rows_d2) == 1
        assert rows_d2[0].v == 'd'
        # CK bytes are preserved as written (d1's representation).
        assert rows_d2[0].c.as_tuple() == d1.as_tuple()

        # --- Partition d3: should be empty ---
        # d3 is yet another representation of 1230 that was never used as a PK
        # in any insert.  Because PK identity is byte-based, this is a distinct
        # (and empty) partition.
        rows_d3 = list(cql.execute(
            cql.prepare(f"SELECT c, v FROM {table} WHERE p = ?"),
            [d3]))
        assert len(rows_d3) == 0

        # --- Cross-representation CK lookup ---
        # Look up a row in partition d2 using d3 as the CK - a representation
        # that was never used for writing.  Because CK comparison is value-based,
        # this should find the row (v='d').
        rows_cross = list(cql.execute(
            cql.prepare(
                f"SELECT v FROM {table} WHERE p = ? AND c = ?"),
            [d2, d3]))
        assert len(rows_cross) == 1
        assert rows_cross[0].v == 'd'

# Decimal values with different (unscaled, scale) representations that
# are numerically equal have different serialized bytes. This test
# documents how that byte-vs-value distinction manifests across
# secondary indexes and materialized views:
#
# - ALLOW FILTERING: value-based comparison - finds all matching rows
#   regardless of representation.
# - Secondary index: the indexed column becomes a partition key in the
#   SI backing table, so lookup is byte-based - only finds rows whose
#   stored bytes match the query value exactly.
# - MV with decimal as partition key: byte-based identity - different
#   representations create different MV partitions.
# - MV with decimal as clustering key: value-based comparison -
#   different representations resolve to the same clustering position.
#
# All behaviors are Cassandra-compatible.
def test_decimal_si_and_mv_representation(cql, test_keyspace):
    d1 = Decimal((0, (1,2,3,0), 0))    # unscaled=1230, scale=0
    d2 = Decimal((0, (1,2,3), 1))       # unscaled=123, scale=-1, value=1230
    assert d1 == d2
    assert d1.as_tuple() != d2.as_tuple()

    with new_test_table(cql, test_keyspace,
            "p int PRIMARY KEY, v decimal, data text") as table:
        stmt = cql.prepare(
            f"INSERT INTO {table} (p, v, data) VALUES (?, ?, ?)")
        cql.execute(stmt, [1, d1, 'a'])
        cql.execute(stmt, [2, d2, 'b'])

        # --- ALLOW FILTERING: value-based - finds both rows ---
        rows = list(cql.execute(cql.prepare(
            f"SELECT p FROM {table} WHERE v = ? ALLOW FILTERING"), [d1]))
        assert sorted(r.p for r in rows) == [1, 2]
        rows = list(cql.execute(cql.prepare(
            f"SELECT p FROM {table} WHERE v = ? ALLOW FILTERING"), [d2]))
        assert sorted(r.p for r in rows) == [1, 2]

        # --- Secondary index: byte-based - each repr finds only its row ---
        with new_secondary_index(cql, table, 'v') as idx:
            ks, idx_name = idx.split('.')
            wait_for_index(cql, ks, idx_name)
            rows = list(cql.execute(cql.prepare(
                f"SELECT p FROM {table} WHERE v = ?"), [d1]))
            assert [r.p for r in rows] == [1]
            rows = list(cql.execute(cql.prepare(
                f"SELECT p FROM {table} WHERE v = ?"), [d2]))
            assert [r.p for r in rows] == [2]

        # --- MV with decimal as partition key: byte-based ---
        with new_materialized_view(cql, table, '*', 'v, p',
                'v is not null and p is not null') as mv:
            wait_for_view_built(cql, mv)
            rows = list(cql.execute(cql.prepare(
                f"SELECT p, data FROM {mv} WHERE v = ?"), [d1]))
            assert len(rows) == 1 and rows[0].p == 1
            rows = list(cql.execute(cql.prepare(
                f"SELECT p, data FROM {mv} WHERE v = ?"), [d2]))
            assert len(rows) == 1 and rows[0].p == 2

        # --- MV with decimal as clustering key: value-based ---
        with new_materialized_view(cql, table, '*', 'p, v',
                'v is not null and p is not null') as mv:
            wait_for_view_built(cql, mv)
            # Query p=1 using d2 (different repr) - finds it.
            rows = list(cql.execute(cql.prepare(
                f"SELECT data FROM {mv} WHERE p = ? AND v = ?"),
                [1, d2]))
            assert len(rows) == 1 and rows[0].data == 'a'
            # Query p=2 using d1 (different repr) - finds it.
            rows = list(cql.execute(cql.prepare(
                f"SELECT data FROM {mv} WHERE p = ? AND v = ?"),
                [2, d1]))
            assert len(rows) == 1 and rows[0].data == 'b'

# Decimal operator+= OOM on extreme scale difference.
# Adding two decimals with very different scales in operator+= rescales
# both operands to the larger scale via pow(10, scale_diff). CQL's
# decimal type allows arbitrary scales (int32 range), so a table with
# values like 1e500000000 and 1 forces pow(10, 500000000) when
# computing SUM() or AVG(), allocating a number with ~500 million digits.
# This is the same class of DoS as issue #8002 (to_string OOM)
# but in arithmetic.
# Cassandra has the same bug - whereas its arithmetic operations limit
# the precision of the result to 10,000 digits (see tests below), for
# aggregation sum() that we test here, it does not (CASSANDRA-21401).
# We use the number 500 million and not higher because in Cassandra's
# implementation, BitInteger (used to store the mantissa) has a hard
# limit of around 537 million decimal digits (537M = 2^31/4 - where
# 4=bitlength(10) was used by the Java code - incorrectly - instead of
# log2(10) - to limit the number of binary digits to 2^31). So using 500
# million digits can allow us to run this test on Cassandra - while still
# timing out or OOMing if this bug exists.
# This test is skipped because it would hang or crash the tests.
# Reproduces SCYLLADB-1576
@pytest.mark.skip_bug(reason="SCYLLADB-1576")
def test_decimal_sum_extreme_scale_oom(cql, test_keyspace, cassandra_bug):
    with new_test_table(cql, test_keyspace,
            "p int, c decimal, PRIMARY KEY (p)") as table:
        stmt = cql.prepare(f"INSERT INTO {table} (p, c) VALUES (?, ?)")
        cql.execute(stmt, [1, Decimal('1e500000000')])
        cql.execute(stmt, [2, Decimal('1')])
        result = list(cql.execute(f"SELECT sum(c) FROM {table}"))
        assert len(result) == 1

# The following context manager is needed to work around a bug in how the
# Python driver parses very long "decimal" values returned from the server.
# By default, the driver fails to parse "decimal" values with more than
# 4300 digits of precision. The reason is as follows:
#
# The driver parses decimal responses using DecimalType.deserialize().
# This function does:
#
#    scale = int32_unpack(byts[:4])
#    unscaled = varint_unpack(byts[4:])
#    return Decimal('%de%d' % (unscaled, -scale))
#
# The problem is in the third line: The "%d" in the format string converts the
# unscaled integer to a decimal string. And due to CVE-2020-10735 Python
# limits that conversion to 4300 digits by default.
# The following context manager can be used to temporarily increase this
# limit beyond the default 4300.
@contextmanager
def int_max_str_digits(limit):
    old_limit = sys.get_int_max_str_digits()
    sys.set_int_max_str_digits(limit)
    try:
        yield
    finally:
        sys.set_int_max_str_digits(old_limit)

# Check the precision addition.
# As explained above in test_decimal_sum_extreme_scale_oom(), we can't allow
# addition to generate huge precision when adding two "decimal"s with wildly
# different scales. Cassandra chose (commit d60e798) to limit the precision
# of the result of addition to MAX_PRECISION = 10,000 digits, Scylla should
# do the same as a solution for #13601. Let's check that this is indeed what
# happens using arithmetic operators in SELECT.
MAX_PRECISION = 10000
@pytest.mark.xfail(reason="#2693 - arithmetic operators not yet implemented")
def test_decimal_add_precision(cql, test_keyspace):
    with new_test_table(cql, test_keyspace,
            "p int, a decimal, b decimal, PRIMARY KEY (p)") as table, \
            int_max_str_digits(MAX_PRECISION * 2):
        stmt = cql.prepare(f"INSERT INTO {table} (p, a, b) VALUES (?, ?, ?)")
        # Test that 1e9999 + 1 produces the correct 10,000-digit result.
        # 10,000 digits is exactly MAX_PRECISION, so no truncation happens.
        cql.execute(stmt, [1, Decimal('1e' + str(MAX_PRECISION - 1)), Decimal('1')])
        result = list(cql.execute(f"SELECT a + b FROM {table} WHERE p = 1"))
        assert result == [(Decimal('1' + '0' * (MAX_PRECISION - 2) + '1'),)]
        # 1e10000 + 1 would have 10,001 digits of precision, but Cassandra
        # limits the precision to just 10,000 digits so the last digit will
        # be zeroed out, and the result of the addition will be just 1e10000.
        # Note that this truncation of the result is silent, it is not an error.
        cql.execute(stmt, [2, Decimal('1e' + str(MAX_PRECISION)), Decimal('1')])
        result = list(cql.execute(f"SELECT a + b FROM {table} WHERE p = 2"))
        assert result == [(Decimal('1e' + str(MAX_PRECISION)),)]
        # Check that the "truncation" of extra digits is actually HALF_UP
        # rounding: 1e10000 + 5 has 10001 digits (10000...005), the 10001st
        # digit is 5, so HALF_UP rounds the 10000th digit (0) up to 1:
        # result is 10000...010 (= 10^10000 + 10), not 10000...000 (= 10^10000).
        # Note: HALF_EVEN would give 10^10000 here (0 is even), so this
        # assertion specifically verifies Java's HALF_UP rounding mode.
        cql.execute(stmt, [3, Decimal('1e' + str(MAX_PRECISION)), Decimal('5')])
        result = list(cql.execute(f"SELECT a + b FROM {table} WHERE p = 3"))
        assert result == [(Decimal('1' + '0' * (MAX_PRECISION - 2) + '10'),)]
        # For negative numbers, HALF_UP rounds away from zero (towards -inf),
        # so -1e10000 + (-5) = -(10^10000 + 5) rounds to -(10^10000 + 10),
        # not -(10^10000).
        cql.execute(stmt, [4, Decimal('-1e' + str(MAX_PRECISION)), Decimal('-5')])
        result = list(cql.execute(f"SELECT a + b FROM {table} WHERE p = 4"))
        assert result == [(Decimal('-1' + '0' * (MAX_PRECISION - 2) + '10'),)]

# Check the precision of division.
# How many digits of precision do we expect when dividing 1.0 by 3.0? There is
# no obvious answer. Cassandra decided in commit d60e798 (for CASSANDRA-15232)
# to use the following rules:
#  * expected scale is set to minimum precision (32) minus estimated position
#    of first digit in quotient
#  * scale should be at least as big as maximum scale of operands
#  * scale should not be less than 32
#  * scale should not be bigger than 1000
#  * if actual quotient scale is bigger than calculated scale then result is
#    rounded using HALF_UP mode
#  * trailing zeros are stripped
# For example, 1/3: the quotient has 0 digits before the decimal point, so
# estimated position = 0, giving scale = 32 - 0 = 32. In general, the
# estimated position (= integer digit count of the quotient) is computed from
# operand precision and scale as (directly from DecimalType.java):
#   position = (l.precision() - l.scale()) - (r.precision() - r.scale())
@pytest.mark.xfail(reason="#2693 - arithmetic operators not yet implemented")
def test_decimal_division_precision(cql, test_keyspace):
    with new_test_table(cql, test_keyspace,
            "p int, a decimal, b decimal, PRIMARY KEY (p)") as table, \
            int_max_str_digits(1001 + 32):
        stmt = cql.prepare(f"INSERT INTO {table} (p, a, b) VALUES (?, ?, ?)")
        # 1 / 3: quotient ~0.333 has 0 integer digits, estimated position = 0,
        # scale = 32 - 0 = 32. The 33rd digit of 1/3 is 3 (< 5), so
        # HALF_UP does not round up. Result: 0.333...3 with 32 threes.
        cql.execute(stmt, [1, Decimal('1'), Decimal('3')])
        result = list(cql.execute(f"SELECT a / b FROM {table} WHERE p = 1"))
        assert result == [(Decimal('0.' + '3' * 32),)]
        # 1 / 6: same scale 32. The 33rd digit of 1/6 is 6 (>= 5), so
        # HALF_UP rounds the 32nd digit up: 0.1666...6 -> 0.1666...7.
        # Result: 0.1 followed by 30 sixes then 7 (32 decimal places total).
        cql.execute(stmt, [2, Decimal('1'), Decimal('6')])
        result = list(cql.execute(f"SELECT a / b FROM {table} WHERE p = 2"))
        assert result == [(Decimal('0.1' + '6' * 30 + '7'),)]
        # 1 / 30: quotient ~0.0333 has -1 integer digits (position = (1-0)-(2-0) = -1),
        # scale = 32 - (-1) = 33. The 34th digit is 3 (< 5), so no rounding.
        # Result: 0.0 followed by 32 threes (33 decimal places total).
        cql.execute(stmt, [3, Decimal('1'), Decimal('30')])
        result = list(cql.execute(f"SELECT a / b FROM {table} WHERE p = 3"))
        assert result == [(Decimal('0.0' + '3' * 32),)]
        # 10 / 3: position = (2-0)-(1-0) = 1, so formula gives scale = 31,
        # but the "not less than 32" floor kicks in, so scale = 32.
        # Result: 3.333...3 with 32 threes after the decimal point.
        cql.execute(stmt, [4, Decimal('10'), Decimal('3')])
        result = list(cql.execute(f"SELECT a / b FROM {table} WHERE p = 4"))
        assert result == [(Decimal('3.' + '3' * 32),)]
        # When max(operand scale) exceeds the computed scale, the result
        # scale is bumped up. Here a has scale 40 > 32, so result has scale 40
        # (40 3's after the decimal point).
        cql.execute(stmt, [5, Decimal('1.' + '0' * 40), Decimal('3')])
        result = list(cql.execute(f"SELECT a / b FROM {table} WHERE p = 5"))
        assert result == [(Decimal('0.' + '3' * 40),)]
        # The scale is hard-capped at 1000. An operand with scale 1001 would
        # push result scale to 1001, but the cap reduces it to 1000.
        cql.execute(stmt, [6, Decimal('1.' + '0' * 1001), Decimal('3')])
        result = list(cql.execute(f"SELECT a / b FROM {table} WHERE p = 6"))
        assert result == [(Decimal('0.' + '3' * 1000),)]
        # Exact divisions: trailing zeros are stripped.
        # 1 / 4 = 0.25 exactly; computed scale = 33, but the trailing zeros
        # are removed, leaving scale 2.
        cql.execute(stmt, [7, Decimal('1'), Decimal('4')])
        result = list(cql.execute(f"SELECT a / b FROM {table} WHERE p = 7"))
        assert len(result) == 1
        assert result[0][0] == Decimal('0.25')
        assert result[0][0].as_tuple().exponent == Decimal('0.25').as_tuple().exponent

# Test modulo operator for decimals. Check how it works for different
# interesting cases, and also that we don't have CASSANDRA-15232 - if the
# division produced a number with precision bigger than 34, it used to fail.
@pytest.mark.xfail(reason="#2693 - arithmetic operators not yet implemented")
def test_decimal_modulo(cql, test_keyspace):
    with new_test_table(cql, test_keyspace,
            "p int, a decimal, b decimal, PRIMARY KEY (p)") as table:
        stmt = cql.prepare(f"INSERT INTO {table} (p, a, b) VALUES (?, ?, ?)")
        # Basic integer modulo.
        cql.execute(stmt, [1, Decimal('7'), Decimal('3')])
        result = list(cql.execute(f"SELECT a % b FROM {table} WHERE p = 1"))
        assert result == [(Decimal('1'),)]
        # Fractional result: scale of result = max(scale(a), scale(b)) = 1.
        cql.execute(stmt, [2, Decimal('7.5'), Decimal('2')])
        result = list(cql.execute(f"SELECT a % b FROM {table} WHERE p = 2"))
        assert result == [(Decimal('1.5'),)]
        # Negative dividend: sign of remainder follows the dividend (Java
        # BigDecimal.remainder() semantics, i.e. truncation toward zero).
        cql.execute(stmt, [3, Decimal('-7'), Decimal('3')])
        result = list(cql.execute(f"SELECT a % b FROM {table} WHERE p = 3"))
        assert result == [(Decimal('-1'),)]
        # Negative divisor: sign of remainder still follows the dividend.
        cql.execute(stmt, [4, Decimal('7'), Decimal('-3')])
        result = list(cql.execute(f"SELECT a % b FROM {table} WHERE p = 4"))
        assert result == [(Decimal('1'),)]
        # Reproduces CASSANDRA-15232: before the fix, a % b would fail when the
        # integer quotient (divideToIntegralValue) had more than 34 digits.
        # 1e35 / 3 has a 35-digit integer part (33333...3 with 35 digits), and
        # 1e35 % 3 = 1e35 - 3 * 33333...3 = 1.
        cql.execute(stmt, [5, Decimal('1e35'), Decimal('3')])
        result = list(cql.execute(f"SELECT a % b FROM {table} WHERE p = 5"))
        assert result == [(Decimal('1'),)]
        # Non-integer operands: 1.7 % 0.5.
        # divideToIntegralValue(1.7/0.5) = 3, so 1.7 - 0.5*3 = 0.2.
        # Result scale = max(scale(1.7), scale(0.5)) = max(1,1) = 1.
        cql.execute(stmt, [6, Decimal('1.7'), Decimal('0.5')])
        result = list(cql.execute(f"SELECT a % b FROM {table} WHERE p = 6"))
        assert result == [(Decimal('0.2'),)]
        # Division by zero raises a FunctionFailure with a "Division by zero" message.
        cql.execute(stmt, [7, Decimal('1'), Decimal('0')])
        with pytest.raises(FunctionFailure, match="Division by zero"):
            list(cql.execute(f"SELECT a % b FROM {table} WHERE p = 7"))

# Verify that toJson() on decimal values produces output consistent with
# Java's BigDecimal.toString() specification.
# See: https://docs.oracle.com/javase/8/docs/api/java/math/BigDecimal.html#toString--
#
# The toJson() function in CQL calls the type's toJSONString(), which
# for DecimalType delegates to BigDecimal.toString(). This test can be
# run on Cassandra to validate the expected output strings.
#
# Scylla's to_string() currently diverges from Java in several cases:
# - Zero with scale (e.g. 0.0, 0.00) loses the scale
# - Negative scale values are expanded instead of using exponential form
# Once to_string() is fixed (SCYLLADB-1574), this test should pass and
# the xfail can be removed.
@pytest.mark.xfail(reason="SCYLLADB-1574")
def test_decimal_tojson_representation(cql, test_keyspace):
    with new_test_table(cql, test_keyspace,
            "p int PRIMARY KEY, v decimal") as table:
        stmt = cql.prepare(f"INSERT INTO {table} (p, v) VALUES (?, ?)")
        cases = [
            # (input, expected toJson output per Java BigDecimal.toString())

            # Plain integers -- scale=0, no surprises
            (Decimal('0'), '0'),
            (Decimal('123'), '123'),
            (Decimal('-45'), '-45'),

            # Plain fractional -- scale>0, adjusted_exp >= -6
            (Decimal('1.23'), '1.23'),
            (Decimal('0.001'), '0.001'),
            (Decimal('-1.23'), '-1.23'),

            # Zero with scale -- Scylla's to_string() loses scale info
            (Decimal('0.0'), '0.0'),
            (Decimal('0.00'), '0.00'),

            # adjusted_exp boundary: -6 is plain, -7 switches to exponential
            # adj_exp = num_digits - 1 - scale
            # 0.00000123: digits=123, adj_exp = 2 - 8 = -6 -> plain
            (Decimal('0.00000123'), '0.00000123'),
            # 0.000000123: digits=123, adj_exp = 2 - 9 = -7 -> exponential
            (Decimal('0.000000123'), '1.23E-7'),

            # Negative scale boundary: scale=0 is plain, scale<0 is exponential.
            # Scylla's to_string() expands these to plain form instead.
            # scale=0 -> plain
            (Decimal('1230'), '1230'),
            (Decimal('10'), '10'),
            (Decimal('100'), '100'),
            # scale=-1 -> exponential
            (Decimal((0, (1, 2, 3), 1)), '1.23E+3'),
            (Decimal((0, (1,), 1)), '1E+1'),
            # scale=-2 -> exponential
            (Decimal((0, (1,), 2)), '1E+2'),
        ]
        for i, (val, expected) in enumerate(cases):
            cql.execute(stmt, [i, val])
            rows = list(cql.execute(
                f"SELECT toJson(v) FROM {table} WHERE p = {i}"))
            assert len(rows) == 1
            assert rows[0][0] == expected, \
                f"For input {val} (as_tuple={val.as_tuple()}): " \
                f"expected toJson={expected!r}, got {rows[0][0]!r}"
