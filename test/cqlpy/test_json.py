# -*- coding: utf-8 -*-
# Copyright 2020-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Various tests for JSON support in Scylla. Note that Cassandra also had
# extensive tests for JSON, which we ported in
# cassandra_tests/validation/entities/json_test.py. The tests here are either
# additional ones, or focusing on more esoteric issues or small tests aiming
# to reproduce bugs discovered by bigger Cassandra tests.
#############################################################################

from util import unique_name, new_test_table, unique_key_int

from cassandra.protocol import FunctionFailure, InvalidRequest
from cassandra.util import Date, Time

import pytest
import json
from decimal import Decimal
from datetime import datetime
from uuid import UUID

@pytest.fixture(scope="module")
def type1(cql, test_keyspace):
    type_name = test_keyspace + "." + unique_name()
    cql.execute("CREATE TYPE " + type_name + " (t text, b boolean)")
    yield type_name
    cql.execute("DROP TYPE " + type_name)

@pytest.fixture(scope="module")
def table1(cql, test_keyspace, type1):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"""CREATE TABLE {table} (p int PRIMARY KEY,
        v int,
        bigv bigint,
        a ascii,
        b boolean,
        vi varint,
        mai map<ascii, int>,
        tup frozen<tuple<text, int>>,
        l list<text>,
        d double,
        t time,
        dec decimal,
        tupmap map<frozen<tuple<text, int>>, int>,
        t1 frozen<{type1}>,
        \"CaseSensitive\" int,
        ts timestamp,
        timeuuidmap map<timeuuid, int>,
        uuidmap map<uuid, int>,
        bigintmap map<bigint, int>,
        decimalmap map<decimal, int>,
        doublemap map<double, int>,
        floatmap map<float, int>,
        intmap map<int, int>,
        tinyintmap map<tinyint, int>,
        smallintmap map<smallint, int>,
        varintmap map<varint, int>,
        blobmap map<blob, int>,
        booleanmap map<boolean, int>,
        datemap map<date, int>,
        inetmap map<inet, int>,
        timemap map<time, int>,
        timestampmap map<timestamp, int>)
        """)
    yield table
    cql.execute("DROP TABLE " + table)

# The EquivalentJson class wraps a JSON string, and compare equal to other
# strings if both are valid JSON strings which decode to the same object.
# EquivalentJson("....") can be used in equality assertions below, to check
# whether functionally-equivalent JSON is returned instead of checking for
# identical strings.
class EquivalentJson:
    def __init__(self, s):
        self.obj = json.loads(s)
    def __eq__(self, other):
        if isinstance(other, EquivalentJson):
            return self.obj == other.obj
        elif isinstance(other, str):
            return self.obj == json.loads(other)
        return NotImplemented
    # Implementing __repr__ is useful because when a comparison fails, pytest
    # helpfully prints what it tried to compare, and uses __repr__ for that.
    def __repr__(self):
        return f'EquivalentJson("{self.obj}")'

# Test that failed fromJson() parsing an invalid JSON results in the expected
# error - FunctionFailure - and not some weird internal error.
# Reproduces issue #7911.
def test_failed_json_parsing_unprepared(cql, table1):
    p = unique_key_int()
    with pytest.raises(FunctionFailure):
        cql.execute(f"INSERT INTO {table1} (p, v) VALUES ({p}, fromJson('dog'))")
def test_failed_json_parsing_prepared(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"INSERT INTO {table1} (p, v) VALUES (?, fromJson(?))")
    with pytest.raises(FunctionFailure):
        cql.execute(stmt, [p, 'dog'])

# Similarly, if the JSON parsing did not fail, but yielded a type which is
# incompatible with the type we want it to yield, we should get a clean
# FunctionFailure, not some internal server error.
# We have here examples of returning a string where a number was expected,
# and returning a unicode string where ASCII was expected, and returning
# a number of the wrong type
# Reproduces issue #7911.
def test_fromjson_wrong_type_unprepared(cql, table1):
    p = unique_key_int()
    with pytest.raises(FunctionFailure):
        cql.execute(f"INSERT INTO {table1} (p, v) VALUES ({p}, fromJson('\"dog\"'))")
    with pytest.raises(FunctionFailure):
        cql.execute(f"INSERT INTO {table1} (p, a) VALUES ({p}, fromJson('3'))")
def test_fromjson_wrong_type_prepared(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"INSERT INTO {table1} (p, v) VALUES (?, fromJson(?))")
    with pytest.raises(FunctionFailure):
        cql.execute(stmt, [p, '"dog"'])
    stmt = cql.prepare(f"INSERT INTO {table1} (p, a) VALUES (?, fromJson(?))")
    with pytest.raises(FunctionFailure):
        cql.execute(stmt, [p, '3'])
def test_fromjson_bad_ascii_unprepared(cql, table1):
    p = unique_key_int()
    with pytest.raises(FunctionFailure):
        cql.execute(f"INSERT INTO {table1} (p, a) VALUES ({p}, fromJson('\"שלום\"'))")
def test_fromjson_bad_ascii_prepared(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"INSERT INTO {table1} (p, a) VALUES (?, fromJson(?))")
    with pytest.raises(FunctionFailure):
        cql.execute(stmt, [p, '"שלום"'])
def test_fromjson_nonint_unprepared(cql, table1):
    p = unique_key_int()
    with pytest.raises(FunctionFailure):
        cql.execute(f"INSERT INTO {table1} (p, v) VALUES ({p}, fromJson('1.2'))")
def test_fromjson_nonint_prepared(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"INSERT INTO {table1} (p, v) VALUES (?, fromJson(?))")
    with pytest.raises(FunctionFailure):
        cql.execute(stmt, [p, '1.2'])

# In test_fromjson_nonint_*() above we noted that the floating point number 1.2
# cannot be assigned into an integer column v. In contrast, the numbers 1e6
# or 1.23456789E+9, despite appearing to C programmers like a floating-point
# constant, are perfectly valid integers - whole numbers and fitting the range
# of int and bigint respectively - so they should be assignable into an int or
# bigint. This test checks that.
# Reproduces issue #10100.
# This test is marked with "cassandra_bug" because it fails in Cassandra as
# well and we consider this failure a bug.
def test_fromjson_int_scientific_notation_unprepared(cql, table1, cassandra_bug):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, bigv) VALUES ({p}, fromJson('1.23456789E+9'))")
    assert list(cql.execute(f"SELECT p, bigv from {table1} where p = {p}")) == [(p, 1234567890)]
    cql.execute(f"INSERT INTO {table1} (p, v) VALUES ({p}, fromJson('1e6'))")
    assert list(cql.execute(f"SELECT p, v from {table1} where p = {p}")) == [(p, 1000000)]
def test_fromjson_int_scientific_notation_prepared(cql, table1, cassandra_bug):
    p = unique_key_int()
    stmt = cql.prepare(f"INSERT INTO {table1} (p, bigv) VALUES (?, fromJson(?))")
    cql.execute(stmt, [p, '1.23456789E+9'])
    assert list(cql.execute(f"SELECT p, bigv from {table1} where p = {p}")) == [(p, 1234567890)]
    stmt = cql.prepare(f"INSERT INTO {table1} (p, v) VALUES (?, fromJson(?))")
    cql.execute(stmt, [p, '1e6'])
    assert list(cql.execute(f"SELECT p, v from {table1} where p = {p}")) == [(p, 1000000)]

# The JSON standard does not define or limit the range or precision of
# numbers. However, if a number is assigned to a Scylla number type, the
# assignment can overflow and should result in an error - not be silently
# wrapped around.
# Reproduces issue #7914
def test_fromjson_int_overflow_unprepared(cql, table1):
    p = unique_key_int()
    # The highest legal int is 2147483647 (2^31-1).2147483648 is not a legal
    # int, so trying to insert it should result in an error - not silent
    # wraparound to -2147483648 as happened in Scylla.
    with pytest.raises(FunctionFailure):
        cql.execute(f"INSERT INTO {table1} (p, v) VALUES ({p}, fromJson('2147483648'))")
def test_fromjson_bigint_overflow_unprepared(cql, table1):
    p = unique_key_int()
    with pytest.raises(FunctionFailure):
        cql.execute(f"INSERT INTO {table1} (p, bigv) VALUES ({p}, fromJson('9223372036854775808'))")
def test_fromjson_int_overflow_prepared(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"INSERT INTO {table1} (p, v) VALUES (?, fromJson(?))")
    with pytest.raises(FunctionFailure):
        cql.execute(stmt, [p, '2147483648'])
def test_fromjson_bigint_overflow_prepared(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"INSERT INTO {table1} (p, bigv) VALUES (?, fromJson(?))")
    with pytest.raises(FunctionFailure):
        cql.execute(stmt, [p, '9223372036854775808'])

# On the other hand, let's check a case of the biggest bigint (64-bit
# integer) which should *not* overflow. Let's check that we handle it
# correctly.
def test_fromjson_bigint_nonoverflow(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"INSERT INTO {table1} (p, bigv) VALUES (?, fromJson(?))")
    cql.execute(stmt, [p, '9223372036854775807'])
    assert list(cql.execute(f"SELECT bigv from {table1} where p = {p}")) == [(9223372036854775807,)]

# Test the same non-overflowing integer with scientific notation. This is the
# same test as test_fromjson_int_scientific_notation_prepared above (so
# reproduces #10100), just with a number higher than 2^53. This presents
# difficult problem for a parser like the RapidJSON one we use, that decides
# to read scientific notation numbers through a "double" variable: a double
# only had 53 significant bits of mantissa, so may not preserve numbers higher
# than 2^53 accurately.
# Note that the JSON standard (RFC 8259) explains that because implementations
# may use double-precision representation (as the Scylla-used RapidJSON does),
# "numbers that are integers and are in the range [-(2**53)+1, (2**53)-1] are
# interoperable in the sense that implementations will agree exactly on their
# numeric values.". Because the number in this test is higher, the JSON
# standard suggests it may be fine to botch it up, so it might be acceptable
# to fail this test.
# Reproduces #10100 and #10137.
@pytest.mark.xfail(reason="issue #10137")
def test_fromjson_bigint_nonoverflow_scientific(cql, table1, cassandra_bug):
    p = unique_key_int()
    stmt = cql.prepare(f"INSERT INTO {table1} (p, bigv) VALUES (?, fromJson(?))")
    # 1152921504606846975 is 2^60-1, more than 2^53 but less than what a
    # bigint can store (2^63-1). We do not use 2^63-1 in this test because
    # an inaccuracy there in the up direction can lead us to overflowing
    # the signed integer and UBSAN errors - while we want to detect the
    # inaccuracy cleanly, here.
    cql.execute(stmt, [p, '115292150460684697.5e1'])
    assert list(cql.execute(f"SELECT bigv from {table1} where p = {p}")) == [(1152921504606846975,)]

# When writing to an integer column, Cassandra's fromJson() function allows
# not just JSON number constants, it also allows a string containing a number.
# Strings which do not hold a number fail with a FunctionFailure. In
# particular, the empty string "" is not a valid number, and should report an
# error, but both Scylla and Cassandra have bugs that allow it for some types
# and not for others. The following tests reproduce #7944. Where Cassandra
# has (what we consider to be) a bug, it is marked with "cassandra_bug"
# which causes it to xfail when testing against Cassandra.
def test_fromjson_int_empty_string_unprepared(cql, table1, cassandra_bug):
    p = unique_key_int()
    with pytest.raises(FunctionFailure):
        cql.execute(f"INSERT INTO {table1} (p, v) VALUES ({p}, fromJson('\"\"'))")
def test_fromjson_int_empty_string_prepared(cql, table1, cassandra_bug):
    p = unique_key_int()
    stmt = cql.prepare(f"INSERT INTO {table1} (p, v) VALUES (?, fromJson(?))")
    with pytest.raises(FunctionFailure):
        cql.execute(stmt, [p, '""'])
@pytest.mark.xfail(reason="issue #7944")
def test_fromjson_varint_empty_string_unprepared(cql, table1):
    p = unique_key_int()
    with pytest.raises(FunctionFailure):
        cql.execute(f"INSERT INTO {table1} (p, vi) VALUES ({p}, fromJson('\"\"'))")
@pytest.mark.xfail(reason="issue #7944")
def test_fromjson_varint_empty_string_prepared(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"INSERT INTO {table1} (p, vi) VALUES (?, fromJson(?))")
    with pytest.raises(FunctionFailure):
        cql.execute(stmt, [p, '""'])

# Cassandra allows the strings "true" and "false", not just the JSON constants
# true and false, to be assigned to a boolean column.
# Reproduces issue #7915
def test_fromjson_boolean_string_unprepared(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, b) VALUES ({p}, fromJson('\"true\"'))")
    assert list(cql.execute(f"SELECT p, b from {table1} where p = {p}")) == [(p, True)]
    cql.execute(f"INSERT INTO {table1} (p, b) VALUES ({p}, fromJson('\"false\"'))")
    assert list(cql.execute(f"SELECT p, b from {table1} where p = {p}")) == [(p, False)]
# Reproduces issue #7915
def test_fromjson_boolean_string_prepared(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"INSERT INTO {table1} (p, b) VALUES (?, fromJson(?))")
    cql.execute(stmt, [p, '"true"'])
    assert list(cql.execute(f"SELECT p, b from {table1} where p = {p}")) == [(p, True)]
    cql.execute(stmt, [p, '"false"'])
    assert list(cql.execute(f"SELECT p, b from {table1} where p = {p}")) == [(p, False)]
    cql.execute(stmt, [p, '"fALSe"'])
    assert list(cql.execute(f"SELECT p, b from {table1} where p = {p}")) == [(p, False)]

# Test that null argument is allowed for fromJson(), with unprepared statement
# Reproduces issue #7912.
def test_fromjson_null_unprepared(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, v) VALUES ({p}, fromJson(null))")
    assert list(cql.execute(f"SELECT p, v from {table1} where p = {p}")) == [(p, None)]

# Test that null argument is allowed for fromJson(), with prepared statement
# Reproduces issue #7912.
def test_fromjson_null_prepared(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"INSERT INTO {table1} (p, v) VALUES (?, fromJson(?))")
    cql.execute(stmt, [p, None])
    assert list(cql.execute(f"SELECT p, v from {table1} where p = {p}")) == [(p, None)]

# Test that fromJson can parse a map<ascii,int>. Strangely Scylla had a bug
# setting a map<ascii,int> with fromJson(), while map<text,int> worked well.
# Reproduces #7949.
def test_fromjson_map_ascii_unprepared(cql, table1):
    p = unique_key_int()
    cql.execute("INSERT INTO " + table1 + " (p, mai) VALUES (" + str(p) + ", fromJson('{\"a\": 1, \"b\": 2}'))")
    assert list(cql.execute(f"SELECT p, mai from {table1} where p = {p}")) == [(p, {'a': 1, 'b': 2})]
def test_fromjson_map_ascii_prepared(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"INSERT INTO {table1} (p, mai) VALUES (?, fromJson(?))")
    cql.execute(stmt, [p, '{"a": 1, "b": 2}'])
    assert list(cql.execute(f"SELECT p, mai from {table1} where p = {p}")) == [(p, {'a': 1, 'b': 2})]

# After the above test for map<ascii,int> was fixed, it turned out (see
# issue #18477) that we have the same bug for fromJson() or INSERT JSON
# of map<timeuuid,int>. Let's test that type, and in more tests below -
# all other types as map keys (except "duration" which isn't allowed as
# a map key) - so we don't discover these bugs one by one...
# Test for fromJson() for map<timeuuid, int>:
# Reproduces #18477:
def test_fromjson_map_key_timeuuid(cql, table1):
    p = unique_key_int()
    cql.execute("INSERT INTO " + table1 + " (p, timeuuidmap) VALUES (" + str(p) + ", fromJson('{\"a4a70900-24e1-11df-8924-001ff3591711\": 3}'))")
    assert list(cql.execute(f"SELECT p, timeuuidmap from {table1} where p = {p}")) == [(p, {UUID('a4a70900-24e1-11df-8924-001ff3591711'): 3})]

def test_fromjson_map_key_uuid(cql, table1):
    p = unique_key_int()
    cql.execute("INSERT INTO " + table1 + " (p, uuidmap) VALUES (" + str(p) + ", fromJson('{\"a4a70900-24e1-11df-8924-001ff3591711\": 3}'))")
    assert list(cql.execute(f"SELECT p, uuidmap from {table1} where p = {p}")) == [(p, {UUID('a4a70900-24e1-11df-8924-001ff3591711'): 3})]

def test_fromjson_map_key_number(cql, table1):
    # For all number types, the JSON should contain as a map key a quoted
    # version of the number, e.g, "17" with the quotes. An unquoted version
    # is not supported (even though it looks like legal JSON), and generates
    # a FunctionFailure error:
    for t in ["bigint", "decimal", "double", "float", "int", "smallint", "tinyint", "varint"]:
        p = unique_key_int()
        with pytest.raises(FunctionFailure):
            cql.execute("INSERT INTO " + table1 + f" (p, {t}map) VALUES (" + str(p) + ", fromJson('{17: 3}'))")
        cql.execute("INSERT INTO " + table1 + f" (p, {t}map) VALUES (" + str(p) + ", fromJson('{\"17\": 3}'))")
        assert list(cql.execute(f"SELECT p, {t}map from {table1} where p = {p}")) == [(p, {17: 3})]

# Reproduces #18477:
def test_fromjson_map_key_blob(cql, table1):
    p = unique_key_int()
    # A blob is encoded in JSON as string containing 0x and then hex, e.g.,
    # "0x12ab3e".
    cql.execute("INSERT INTO " + table1 + f" (p, blobmap) VALUES (" + str(p) + ", fromJson('{\"0x12ab3e\": 3}'))")
    assert list(cql.execute(f"SELECT p, blobmap from {table1} where p = {p}")) == [(p, {b'\x12\xab\x3e': 3})]

def test_fromjson_map_key_boolean(cql, table1):
    # While in general, Cassandra and Scylla allow boolean to be encoded in
    # JSON as either the unquoted Json constants true/false or as quoted
    # strings "true"/"false", for map keys only the quoted version is
    # supported.
    p = unique_key_int()
    cql.execute("INSERT INTO " + table1 + f" (p, booleanmap) VALUES (" + str(p) + ", fromJson('{\"true\": 3}'))")
    assert list(cql.execute(f"SELECT p, booleanmap from {table1} where p = {p}")) == [(p, {True: 3})]

# Reproduces #18477:
def test_fromjson_map_key_date(cql, table1):
    p = unique_key_int()
    cql.execute("INSERT INTO " + table1 + f" (p, datemap) VALUES (" + str(p) + ", fromJson('{\"2011-02-03\": 3}'))")
    assert list(cql.execute(f"SELECT p, datemap from {table1} where p = {p}")) == [(p, {Date('2011-02-03'): 3})]

# Reproduces #18477:
def test_fromjson_map_key_inet(cql, table1):
    p = unique_key_int()
    cql.execute("INSERT INTO " + table1 + f" (p, inetmap) VALUES (" + str(p) + ", fromJson('{\"1.2.3.4\": 3}'))")
    assert list(cql.execute(f"SELECT p, inetmap from {table1} where p = {p}")) == [(p, {'1.2.3.4': 3})]

# Reproduces #18477:
def test_fromjson_map_key_time(cql, table1):
    p = unique_key_int()
    cql.execute("INSERT INTO " + table1 + f" (p, timemap) VALUES (" + str(p) + ", fromJson('{\"07:35:07.000111222\": 3}'))")
    assert list(cql.execute(f"SELECT p, timemap from {table1} where p = {p}")) == [(p, {Time('07:35:07.000111222'): 3})]

# Reproduces #18477:
def test_fromjson_map_key_timestamp(cql, table1):
    p = unique_key_int()
    # The following is just one example of a recognized timestamp format.
    # We have other tests above for the other allowed formats - here our
    # goal isn't to check all of them, but just check on in the context
    # of a map's key.
    cql.execute("INSERT INTO " + table1 + f" (p, timestampmap) VALUES (" + str(p) + ", fromJson('{\"2014-01-01 12:15:45+0000\": 3}'))")
    assert list(cql.execute(f"SELECT p, timestampmap from {table1} where p = {p}")) == [(p, {datetime(2014, 1, 1, 12, 15, 45): 3})]


# With fromJson() the JSON "null" constant can be used to unset a column,
# but can also be used to unset a part of a tuple column. In both cases,
# in addition to fromJson() allowing the expected type, the "null" constant
# should also be allowed. But it's not like a null is allowed *everywhere*
# that a normal value is allowed. For example, it cannot be given as an
# element of a list.
# Reproduces #7954.
@pytest.mark.xfail(reason="issue #7954")
def test_fromjson_null_constant(cql, table1):
    p = unique_key_int()
    # Check that a "null" JSON constant can be used to unset a column
    stmt = cql.prepare(f"INSERT INTO {table1} (p, v) VALUES (?, fromJson(?))")
    cql.execute(stmt, [p, '1'])
    assert list(cql.execute(f"SELECT p, v from {table1} where p = {p}")) == [(p, 1)]
    cql.execute(stmt, [p, 'null'])
    assert list(cql.execute(f"SELECT p, v from {table1} where p = {p}")) == [(p, None)]
    # Check that a "null" JSON constant can be used to unset part of a tuple
    stmt = cql.prepare(f"INSERT INTO {table1} (p, tup) VALUES (?, fromJson(?))")
    cql.execute(stmt, [p, '["a", 1]'])
    assert list(cql.execute(f"SELECT p, tup from {table1} where p = {p}")) == [(p, ('a', 1))]
    cql.execute(stmt, [p, '["a", null]'])
    assert list(cql.execute(f"SELECT p, tup from {table1} where p = {p}")) == [(p, ('a', None))]
    cql.execute(stmt, [p, '[null, 2]'])
    assert list(cql.execute(f"SELECT p, tup from {table1} where p = {p}")) == [(p, (None, 2))]
    # However, a "null" JSON constant is not just allowed everywhere that a
    # normal value is allowed. E.g, it cannot be part of a list. Let's
    # verify that we didn't overdo the fix.
    stmt = cql.prepare(f"INSERT INTO {table1} (p, l) VALUES (?, fromJson(?))")
    with pytest.raises(FunctionFailure):
        cql.execute(stmt, [p, '["a", null]'])

# Check that toJson() correctly formats double values. Strangely, we had a bug`
# (issue #7972) where the double value 123.456 was correctly formatted, but
# the value 123123.123123 was truncated to an integer. This test reproduces
# this.
@pytest.mark.xfail(reason="issue #7972")
def test_tojson_double(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"INSERT INTO {table1} (p, d) VALUES (?, ?)")
    cql.execute(stmt, [p, 123.456])
    assert list(cql.execute(f"SELECT d, toJson(d) from {table1} where p = {p}")) == [(123.456, "123.456")]
    # While 123.456 above worked, in issue #7972 we note that 123123.123123
    # does not work.
    cql.execute(stmt, [p, 123123.123123])
    assert list(cql.execute(f"SELECT d, toJson(d) from {table1} where p = {p}")) == [(123123.123123, "123123.123123")]

# Check that toJson() correctly formats "time" values. The JSON translation
# is a string containing the time (there is no time type in JSON), and of
# course, a string needs to be wrapped in quotes.
# Reproduces issue #7988.
def test_tojson_time(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"INSERT INTO {table1} (p, t) VALUES (?, ?)")
    cql.execute(stmt, [p, 123])
    assert list(cql.execute(f"SELECT toJson(t) from {table1} where p = {p}")) == [('"00:00:00.000000123"',)]

# Test the same thing in test_tojson_time above, with SELECT JSON instead
# of SELECT toJson(). Also reproduces issue #7988.
def test_select_json_time(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"INSERT INTO {table1} (p, t) VALUES (?, ?)")
    cql.execute(stmt, [p, 123])
    assert list(cql.execute(f"SELECT JSON t from {table1} where p = {p}")) == [(EquivalentJson('{"t": "00:00:00.000000123"}'),)]

# Check that toJson() returns timestamp string in correct cassandra compatible format (issue #7997)
# with milliseconds and timezone specification
def test_tojson_timestamp(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"INSERT INTO {table1} (p, ts) VALUES (?, ?)")
    cql.execute(stmt, [p, datetime(2014, 1, 1, 12, 15, 45)])
    assert list(cql.execute(f"SELECT toJson(ts) from {table1} where p = {p}")) == [('"2014-01-01 12:15:45.000Z"',)]

# Although toJson() converts a timestamp to one specific textual format (as we
# saw in the previous test), following Postel's law several other alternative
# formats are recognized by fromJson():
def test_fromjson_timestamp(cql, table1):
    stmt = cql.prepare(f"INSERT INTO {table1} (p, ts) VALUES (?, fromJson(?))")
    for json in [
        '"2014-01-01 12:15:45.000Z"',
        '"2014-01-01T12:15:45.000Z"',
        '"2014-01-01 12:15:45.000+0000"',
        '"2014-01-01 12:15:45+0000"',
    ]:
        p = unique_key_int()
        cql.execute(stmt, [p, json])
        assert list(cql.execute(f"SELECT ts from {table1} where p = {p}")) == [(datetime(2014, 1, 1, 12, 15, 45),)]

# However, there is one variation on the timestamp format that Scylla refuses
# to accept, while Cassandra allows: Scylla allows the *fractional* second
# to only have millisecond precision because that's the precision that
# timestamps actually have. So Scylla allows "2014-01-01 12:15:45.000Z"
# but forbids "2014-01-01 12:15:45.000000Z" - whereas Cassandra allows both,
# and this is what this test shows.
# The same difference is reproduced in the test
#    test_type_timestamp.py::test_type_timestamp_from_string_overprecise
# and there we decided to accept Scylla's error handling as the better
# approach, and mark this test a Cassandra bug, so we do the same here.
#
# See also issue #16575 which is about Scylla mistakenly using this
# forbidden format when outputting JSON - which was a problem because then
# we can't read the JSON we wrote.
def test_fromjson_timestamp_submilli(cql, table1, cassandra_bug):
    stmt = cql.prepare(f"INSERT INTO {table1} (p, ts) VALUES (?, fromJson(?))")
    p = unique_key_int()
    json = '"2014-01-01 12:15:45.0000000Z"'
    with pytest.raises(FunctionFailure, match='Milliseconds'):
        cql.execute(stmt, [p, json])
        assert list(cql.execute(f"SELECT ts from {table1} where p = {p}")) == [(datetime(2014, 1, 1, 12, 15, 45),)]

# Test that toJson() can prints a decimal type with a very high mantissa.
# Reproduces issue #8002, where it was written as 1 and a billion zeroes,
# running out of memory.
# We need to skip this test because in debug mode memory allocation is not
# bounded, and this test can hang or crash instead of failing immediately.
# We also have a smaller xfailing test below, test_tojson_decimal_high_mantissa2.
@pytest.mark.skip(reason="issue #8002")
def test_tojson_decimal_high_mantissa(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"INSERT INTO {table1} (p, dec) VALUES ({p}, ?)")
    high = '1e1000000000'
    cql.execute(stmt, [Decimal(high)])
    assert list(cql.execute(f"SELECT toJson(dec) from {table1} where p = {p}")) == [(EquivalentJson(high),)]

# This is a smaller version of test_tojson_decimal_high_mantissa, showing
# that a much smaller exponent, 1e1000 works (this is not surprising) but
# results in 1000 digits of output. This hints that 1e1000000000 will not
# work at all, without testing it directly as above.
@pytest.mark.xfail(reason="issue #8002")
def test_tojson_decimal_high_mantissa2(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"INSERT INTO {table1} (p, dec) VALUES ({p}, ?)")
    # Although 1e1000 is higher than a normal double, it should be fine for
    # Scylla's "decimal" type:
    high = '1e1000'
    cql.execute(stmt, [Decimal(high)])
    result = cql.execute(f"SELECT toJson(dec) from {table1} where p = {p}").one()[0]
    # We expect the "result" JSON string to be 1E+1000 - not 100000000....000000.
    assert len(result) < 10

# Reproducers for issue #8077: SELECT JSON on a function call should result
# in the same JSON strings as it does on Cassandra.
@pytest.mark.xfail(reason="issue #8077")
def test_select_json_function_call(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, v) VALUES ({p}, 17) USING TIMESTAMP 1234")
    input_and_output = {
        'v':                       '{"v": 17}',
        'count(*)':                '{"count": 1}',
        'ttl(v)':                  '{"ttl(v)": null}',
        'writetime(v)':            '{"writetime(v)": 1234}',
        'intAsBlob(v)':            '{"system.intasblob(v)": "0x00000011"}',
        'blobasInt(intAsBlob(v))': '{"system.blobasint(system.intasblob(v))": 17}',
        'tojson(v)':               '{"system.tojson(v)": "17"}',
        'CAST(v AS FLOAT)':        '{"cast(v as float)": 17.0}',
    }
    for input, output in input_and_output.items():
        assert list(cql.execute(f"SELECT JSON {input} from {table1} where p = {p}")) == [(EquivalentJson(output),)]

# Whereas in CQL map keys might be of many types, in JSON map keys must always
# be strings. So when SELECT JSON prints a map value with a non-string key to
# JSON, it needs to format this key as a string. When the map key *contains* a
# string, e.g., tuple<int, text>, we must not forget to *quote* that string
# before inserting into the key's string representation. But we forgot :-)
# This is issue #8087.
# This issue is also reproduced by the much more comprehensive test
# cassandra_tests/validation/entities/json_test.py::testInsertJsonSyntaxWithNonNativeMapKeys
@pytest.mark.xfail(reason="issue #8087")
def test_select_json_string_in_nonstring_map_key(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"INSERT INTO {table1} (p, tupmap) VALUES ({p}, ?)")
    cql.execute(stmt, [{('hello', 3): 7}])
    expected = '{"tupmap": {"[\\"hello\\", 3]": 7}}'
    assert list(cql.execute(f"SELECT JSON tupmap from {table1} where p = {p}")) == [(expected,)]

# Test that SELECT JSON correctly prints unset components of a UDT or tuple
# as "null". This test passes which demonstrates that issue #8092 is specific
# to altering a UDT, and doesn't just happen for every null component of a
# UDT or tuple.
def test_select_json_null_component(cql, table1, type1):
    p = unique_key_int()
    stmt = cql.prepare(f"INSERT INTO {table1} (p, tup) VALUES ({p}, ?)")
    cql.execute(stmt, [('hello', None)])
    assert list(cql.execute(f"SELECT JSON tup from {table1} where p = {p}")) == [('{"tup": ["hello", null]}',)]

    stmt = cql.prepare(f"INSERT INTO {table1} (p, t1) VALUES ({p}, ?)")
    cql.execute(stmt, [('hello', None)])
    assert list(cql.execute(f"SELECT JSON t1 from {table1} where p = {p}")) == [('{"t1": {"t": "hello", "b": null}}',)]

# Reproducer for issue #8078: Test that the "AS" clause (alias) in
# a SELECT JSON is honored, and the returned JSON string contains the
# given alias instead of the original column name or function call.
#
# This issue is also reproduced (together with additional issues) by the
# translated Cassandra unit tests:
# cassandra_tests/validation/entities/json_test.py::testSelectJsonSyntax
# cassandra_tests/validation/entities/json_test.py::testCaseSensitivity
def test_select_json_with_alias(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, v, bigv, a, \"CaseSensitive\") VALUES ({p}, 17, 34, 'dog', 99)")
    # We aim here to cover many interesting cases: alias for one column,
    # aliases for several columns, some columns with alias and some without,
    # aliasing a function call, case-sensitive aliases and case-sensitive
    # column names.
    input_and_output = {
        'v':                       '{"v": 17}',
        'v as hello':              '{"hello": 17}',
        'v as Hello':              '{"hello": 17}',
        'v as "Hello"':            '{"\\"Hello\\"": 17}',
        'v as "Hello World!"':     '{"\\"Hello World!\\"": 17}',
        'ttl(v)':                  '{"ttl(v)": null}',
        'ttl(v) as hi':            '{"hi": null}',
        '"CaseSensitive"':         '{"\\"CaseSensitive\\"": 99}',
        '"CaseSensitive" as cs':   '{"cs": 99}',
        'v, p':                    '{"v": 17, "p": ' + str(p) + '}',
        'v, p as xyz':             '{"v": 17, "xyz": ' + str(p) + '}',
        'v, bigv, a':              '{"v": 17, "bigv": 34, "a": "dog"}',
        'v, bigv as xyz, a':       '{"v": 17, "xyz": 34, "a": "dog"}',
        'v as qwe, bigv as xyz, a' :'{"qwe": 17, "xyz": 34, "a": "dog"}',
        'v as q, bigv as x, a as z':'{"q": 17, "x": 34, "z": "dog"}',
        # Although it's not useful, it's allowed to use the same alias
        # for multiple columns...
        'v as q, bigv as q, a as q':'{"q": 17, "q": 34, "q": "dog"}',
    }
    for input, output in input_and_output.items():
        assert list(cql.execute(f"SELECT JSON {input} from {table1} where p = {p}")) == [(EquivalentJson(output),)]
