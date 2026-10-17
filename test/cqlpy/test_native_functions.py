# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

###############################################################################
# Tests for various native (built-in) scalar functions that can be used in
# various SELECT, INSERT or UPDATE requests. Note we also have tests for
# some of these functions in many other test files. For example, the tests
# for the cast() function are in test_cast_data.py.
###############################################################################

import pytest
import re
from .util import new_test_table, unique_key_int
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP, ROUND_HALF_EVEN
from cassandra.protocol import InvalidRequest

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, i int, g bigint, b blob, s text, t timestamp, u timeuuid, d date, PRIMARY KEY (p)") as table:
        yield table

@pytest.fixture(scope="module")
def tbl_set(cql, test_keyspace):
    with new_test_table(cql, test_keyspace,
                        "p int PRIMARY KEY, s1 set<int>, s2 set<int>,"
                        "s3 set<tinyint>, m map<int, int>, s4 frozen<set<int>>") as table:
        yield table

# Check that a function that can take a column name as a parameter, can also
# take a constant. This feature is barely useful for WHERE clauses, and
# even less useful for selectors, but should be allowed for both.
# Reproduces #12607.
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

# The mintimeuuid() function works with valid "timestamp"-type values, which
# are 64-bit *signed* integers representing milliseconds since the epoch.
# In particular, negative timestamps are allowed and should not be rejected.
# The function tounixtimestamp can be used to extract the timestamp back from
# the timeuuid. In this test we use "reasonably"-low timestamps (referring
# to dates in this and the previous century - not to the age of the
# dinosaurs). The test that follows will look at extreme, not-useful-in-
# practice, timestamp values.
@pytest.mark.parametrize("timestamp", [-1706638779000, -123, 123, 1706638779000])
def test_mintimeuuid_reasonable(cql, table1, timestamp):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, t) VALUES ({p}, {timestamp})")
    assert [(timestamp,)] == list(cql.execute(f"SELECT tounixtimestamp(mintimeuuid(t)) FROM {table1} WHERE p={p}"))

# The "timestamp" column type stores a 64-bit number of milliseconds, but
# Scylla's and Cassandra's implementation of timeuuids cannot store this
# entire range. This test expects mintimeuuid() to either return the *right*
# thing (converted back to the original timestamp via tounixtimestamp),
# or to throw an exception. Crashing is not acceptable (reproduces #17035),
# and so is returning a value, but it having the wrong timestamp (this
# happens in Cassandra, so this test is marked a cassandra_bug)
@pytest.mark.parametrize("timestamp", [-2**63+123, -2**62, -2**58, 2**62, 2**63-123])
def test_mintimeuuid_extreme(cql, table1, timestamp, cassandra_bug):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, t) VALUES ({p}, {timestamp})")
    try:
        res = list(cql.execute(f"SELECT tounixtimestamp(mintimeuuid(t)) FROM {table1} WHERE p={p}"))
    except:
        # If mintimeuuid() refuses these extreme values, it's fine.
        # The real bug is to return something, which is wrong (the
        # assert below), or to crash the server (issue #17035).
        pass
    else:
        assert [(timestamp,)] == res

# An example of an extreme negative timestamp is what totimestamp(123)
# returns: As shown in test_type_date.py, the number "123" is converted to
# a date very long in the past (2**31 is the day of the epoch), and results
# in a very negative timestamp.
# We don't expect much from this test - it may report strange errors (this
# happens in both Cassandra and Scylla), but it mustn't crash as it did in
# issue #17035.
def test_mintimeuuid_extreme_from_totimestamp(cql, table1):
    p = unique_key_int()
    try:
        cql.execute(f"SELECT * FROM {table1} WHERE p={p} and u < mintimeuuid(totimestamp(123)) ALLOW FILTERING")
    except:
        pass

# According to the documentation, the toTimestamp() function can take either
# a "timeuuid" or a "date" value and convert it to a "timestamp" type
# (64-bit signed integer representing number of milliseconds since the UNIX
# epoch). Let's test these conversions, and their error cases (especially,
# the range dates covered by the different types isn't identical).
# In test_type_date.py we have test coverage on the different ways a "date"
# value can initialized, so we don't need to cover all of these here.
def test_totimestamp_date(cql, table1):
    p = unique_key_int()
    # The date 2**31 is the day of the epoch, so 2**31+1 is one day later,
    # Midnight January 2nd, 1970:
    cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, {2**31+1})")
    assert [(datetime(1970,1,2,0,0),)] == list(cql.execute(f"SELECT totimestamp(d) FROM {table1} WHERE p={p}"))
    # The day 2**31-1 is one day before the epoch, and this (negative
    # timestamps) is allowed:
    cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, {2**31-1})")
    assert [(datetime(1969,12,31,0,0),)] == list(cql.execute(f"SELECT totimestamp(d) FROM {table1} WHERE p={p}"))

# Same as above test test_totimestame_date(), but use extreme dates
# millions of years in the past. These tests cannot be run with the
# current Python driver because of bugs it has in converting extreme
# timestamps - https://github.com/scylladb/python-driver/issues/255 -
# so the test is skipped.
@pytest.mark.skip_bug(
    link="https://github.com/scylladb/python-driver/issues/255",
    reason="Python driver bug",
)
def test_totimestamp_date_extreme(cql, table1):
    p = unique_key_int()
    # The day 2**31-2**29 is 2**29 days before the epoch - it's a useless date
    # (more than a million years before our time), but at 4e16 milliseconds
    # before the epoch, it should still fit nicely into 63 bits (9e18
    # milliseconds), and should work fine.
    cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, {2**31-2**29})")
    cql.execute(f"SELECT totimestamp(d) FROM {table1} WHERE p={p}")
    # The day 2**30 is 2**30 days before the epoch - it's a useless date
    # (almost 3 million years before our time), but at 10^17 milliseconds
    # before the epoch, it should still fit 63 bits (10^19 milliseconds)
    # and work fine.
    cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, {2**30})")
    cql.execute(f"SELECT totimestamp(d) FROM {table1} WHERE p={p}")

# Test set_intersection() function. Not supported in Cassandra.
def test_set_intersection_fn(cql, tbl_set, scylla_only):
    p1 = unique_key_int()
    p2 = unique_key_int()
    cql.execute(f"INSERT INTO {tbl_set} (p, s1, s2, s3, m, s4) VALUES ({p1}, {{1,2,3}}, {{2,3,4}}, {{1}}, {{1:2, 2:3}}, {{-1,2}})")
    cql.execute(f"INSERT INTO {tbl_set} (p, s1, s2, s3, m, s4) VALUES ({p2}, {{1,2,3}}, NULL, {{1}}, {{1:2, 2:3}}, {{-1,2}})")
    # Normal intersection.
    assert [(set([2,3]),)] == list(cql.execute(f"SELECT set_intersection(s1, s2) FROM {tbl_set} WHERE p={p1}"))
    # Intersecting with NULL.
    assert [(None,)] == list(cql.execute(f"SELECT set_intersection(s1, s2) FROM {tbl_set} WHERE p={p2}"))
    # Frozen and non-frozen.
    assert [(set([2]),)] == list(cql.execute(f"SELECT set_intersection(s1, s4) FROM {tbl_set} WHERE p={p1}"))
    # Nesting
    assert [(set([2]),)] == list(cql.execute(f"SELECT set_intersection(s1, set_intersection(s2, s4)) FROM {tbl_set} WHERE p={p1}"))
    # Some error cases
    with pytest.raises(InvalidRequest, match='accepts 2 arguments'):
        cql.execute(f"SELECT set_intersection(s1, s2, s3) FROM {tbl_set} WHERE p={p1}")
    with pytest.raises(InvalidRequest, match='accepts 2 arguments'):
        cql.execute(f"SELECT set_intersection(s1) FROM {tbl_set} WHERE p={p1}")
    with pytest.raises(InvalidRequest, match='both arguments are of set type'):
        cql.execute(f"SELECT set_intersection(s1, p) FROM {tbl_set} WHERE p={p1}")
    with pytest.raises(InvalidRequest, match='both arguments are of the same set type'):
        cql.execute(f"SELECT set_intersection(s1, s3) FROM {tbl_set} WHERE p={p1}")

# An unqualified native function (e.g. intAsBlob) called against a `system`
# keyspace table used to be added as a candidate twice. It was registered once
# under the native "system" name, and once under the current keyspace, which is
# also "system". A weakly-assignable argument then matched both copies and
# raised a spurious "Ambiguous call to function" InvalidRequest error.
# Reproduces SCYLLADB-1682.
#
# "Ambiguous call to function" is a hard error, so the regression check is
# simply that these queries execute without raising: if the duplicate candidate
# came back, the call would throw InvalidRequest and fail the test.
def test_native_function_in_system_keyspace_query(cql):
    # Literal: weakly assignable to int under the current type inference.
    list(cql.execute("SELECT intAsBlob(4) FROM system.local"))
    # Bind marker: untyped by construction, so the canonical reproducer.
    stmt = cql.prepare("SELECT intAsBlob(?) FROM system.local")
    list(cql.execute(stmt, [4]))
    # Nested unqualified calls.
    stmt = cql.prepare("SELECT blobAsInt(intAsBlob(?)) FROM system.local")
    list(cql.execute(stmt, [4]))
    # Fully-qualified call takes the single-candidate path; regression guard.
    list(cql.execute("SELECT system.intAsBlob(4) FROM system.local"))

# The following tests are for the format_bytes() and format_time() functions,
# added in Cassandra 6. The translated tests in
# cassandra_tests/functions/format_*_fct_test.py test them thoroughly, but
# always pass them table columns, so these tests check what those don't.

# The value argument of format_bytes() and format_time() can also be a
# literal - an integer, also one too big for an int, or a string holding an
# integer - or a bind marker.
def test_format_functions_literal_and_bound_values(cql, table1, new_to_cassandra_6):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table1} (p) VALUES ({p})')
    def one(expr):
        return cql.execute(f'SELECT {expr} FROM {table1} WHERE p = {p}').one()[0]
    # An integer literal, also one too big for an int, or a string literal:
    assert one('format_bytes(102)') == '102 B'
    assert one('format_bytes(1563401650)') == '1.46 GiB'
    assert one('format_bytes(4294967296)') == '4 GiB'
    assert one("format_bytes('2048')") == '2 KiB'
    assert one('format_time(9000000)') == '2.5 h'
    # A bound value:
    stmt = cql.prepare(f'SELECT format_bytes(?), format_time(?) FROM {table1} WHERE p = {p}')
    assert tuple(cql.execute(stmt, [2048, 90000]).one()) == ('2 KiB', '1.5 m')

# When the value argument of format_bytes() is a string, it must hold an
# integer: an optional sign followed by decimal digits, and nothing else - no
# spaces, exponent or hexadecimal - which fits in a bigint. format_time()
# parses a string value in the same way.
def test_format_bytes_string_value(cql, table1, new_to_cassandra_6):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table1} (p) VALUES ({p})')
    def one(expr):
        return cql.execute(f'SELECT {expr} FROM {table1} WHERE p = {p}').one()[0]
    assert one("format_bytes('+100')") == '100 B'
    assert one("format_bytes('007')") == '7 B'
    assert one("format_bytes('-0')") == '0 B'
    assert one("format_bytes('9223372036854775807')") == '8589934592 GiB'
    for s in ['', ' 100', '100 ', '+-1', '1e3', '0x10', '9223372036854775808']:
        with pytest.raises(InvalidRequest, match=re.escape(f"unable to convert string '{s}' to a value of type long")):
            one(f"format_bytes('{s}')")

# The units of format_bytes() and format_time() are case-insensitive.
# format_bytes() prints its unit in the standard case, but format_time()
# prints the target unit as the user wrote it.
def test_format_functions_unit_case(cql, table1, new_to_cassandra_6):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table1} (p, i) VALUES ({p}, 1024)')
    def one(expr):
        return cql.execute(f'SELECT {expr} FROM {table1} WHERE p = {p}').one()[0]
    assert one("format_bytes(i, 'kib')") == '1 KiB'
    assert one("format_bytes(i, 'KIB', 'b')") == '1048576 B'
    assert one("format_time(i, 'MS', 'S')") == '1.02 S'

# format_time()'s microseconds unit can be written as "us" or as "µs", with
# the micro sign. Cassandra's documentation lists "µs" too, but Cassandra
# rejects it, because its units are ascii and "µs" isn't ascii
# (CASSANDRA-21758).
def test_format_time_micro_sign(cql, table1, new_to_cassandra_6, cassandra_bug):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table1} (p) VALUES ({p})')
    def one(expr):
        return cql.execute(f'SELECT {expr} FROM {table1} WHERE p = {p}').one()[0]
    assert one("format_time(5, 'ms', 'us')") == '5000 us'
    assert one("format_time(5, 'ms', 'µs')") == '5000 µs'
    assert one("format_time(5000, 'µs', 'ms')") == '5 ms'

# The result of format_time() and format_bytes() is rounded to two decimal
# places, according to the exact binary value of the double. A value which
# looks like a decimal tie usually isn't one: 1005 ms is
# 1.00500000000000000444 s in a double, so it is rounded up, but 1025 ms is
# 1.02499999999999991118 s, so it is rounded down. An exact tie is only
# possible for the fractions .125, .375, .625 and .875, and there Cassandra
# rounds up and Scylla rounds to even - which gives the same result for .375
# and .875 but not for .125 and .625, so the test accepts both.
def test_format_functions_rounding(cql, table1, new_to_cassandra_6):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table1} (p) VALUES ({p})')
    def one(expr):
        return cql.execute(f'SELECT {expr} FROM {table1} WHERE p = {p}').one()[0]
    assert one("format_time(1005, 'ms', 's')") == '1.01 s'
    assert one("format_time(1025, 'ms', 's')") == '1.02 s'
    assert one("format_time(1, 'ns', 'd')") == '0 d'
    assert one("format_time(125, 'ms', 's')") in ['0.12 s', '0.13 s']
    assert one("format_time(375, 'ms', 's')") == '0.38 s'
    # An exact tie in a large number, 2^44 + 0.125 KiB:
    assert one("format_bytes(18014398509482112, 'B', 'KiB')") in ['17592186044416.12 KiB', '17592186044416.13 KiB']

# format_bytes() prints a result of 10^16 or more - always an integer - with
# the shortest digits that identify the double, padded with zeros, not with
# the exact value of the double. A result just below 10^16 is printed
# exactly.
def test_format_bytes_large_numbers(cql, table1, new_to_cassandra_6):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table1} (p) VALUES ({p})')
    def one(expr):
        return cql.execute(f'SELECT {expr} FROM {table1} WHERE p = {p}').one()[0]
    assert one("format_bytes(9999999999999998, 'B', 'B')") == '9999999999999998 B'
    assert one("format_bytes(10000000000000000, 'B', 'B')") == '10000000000000000 B'
    assert one("format_bytes(9223372036854775807, 'B', 'B')") == '9223372036854776000 B'

# The acceptable ways to format a number with at most two decimal places,
# rounding according to the exact binary value of the double, as Decimal(value)
# does: on an exact tie, Cassandra rounds up and Scylla rounds to even.
def acceptable_formats(value):
    ret = set()
    for rounding in [ROUND_HALF_UP, ROUND_HALF_EVEN]:
        s = format(Decimal(value).quantize(Decimal('0.01'), rounding=rounding), 'f')
        ret.add(s.rstrip('0').rstrip('.') if '.' in s else s)
    return ret

# Check the rounding of many results of format_bytes() and format_time().
# n bytes is n/1024 KiB, so n from 0 to 2047 gives every multiple of 1/1024
# below 2, including all four kinds of exact ties (.125, .375, .625 and
# .875). n milliseconds is n * 0.001 seconds, which is never an exact tie,
# but often looks like a decimal tie.
def test_format_functions_rounding_many(cql, table1, new_to_cassandra_6):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table1} (p) VALUES ({p})')
    stmt = cql.prepare(f"SELECT format_bytes(?, 'B', 'KiB'), format_time(?, 'ms', 's') FROM {table1} WHERE p = {p}")
    for n in range(2048):
        kib, s = cql.execute(stmt, [n, n]).one()
        assert kib in {f + ' KiB' for f in acceptable_formats(n / 1024)}
        assert s in {f + ' s' for f in acceptable_formats(n * (1e6 / 1e9))}

# format_bytes() and format_time() also accept a column whose type is
# reversed - a clustering column with descending order.
def test_format_functions_desc_column(cql, test_keyspace, new_to_cassandra_6):
    with new_test_table(cql, test_keyspace, 'p int, c int, PRIMARY KEY (p, c)', 'WITH CLUSTERING ORDER BY (c DESC)') as table:
        cql.execute(f'INSERT INTO {table} (p, c) VALUES (1, 2048)')
        assert tuple(cql.execute(f"SELECT format_bytes(c), format_time(c, 's') FROM {table} WHERE p = 1").one()) == ('2 KiB', '2.05 s')
