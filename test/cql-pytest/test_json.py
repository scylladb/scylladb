# -*- coding: utf-8 -*-
# Copyright 2020-present ScyllaDB
#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

#############################################################################
# Various tests for JSON support in Scylla. Note that Cassandra also had
# extensive tests for JSON, which we ported in
# cassandra_tests/validation/entities/json_test.py. The tests here are either
# additional ones, or focusing on more esoteric issues or small tests aiming
# to reproduce bugs discovered by bigger Cassandra tests.
#############################################################################

from util import unique_name, new_test_table

from cassandra.protocol import FunctionFailure, InvalidRequest

import pytest
import random
import json
from decimal import Decimal

@pytest.fixture(scope="session")
def type1(cql, test_keyspace):
    type_name = test_keyspace + "." + unique_name()
    cql.execute("CREATE TYPE " + type_name + " (t text, b boolean)")
    yield type_name
    cql.execute("DROP TYPE " + type_name)

@pytest.fixture(scope="session")
def table1(cql, test_keyspace, type1):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (p int PRIMARY KEY, v int, a ascii, b boolean, vi varint, mai map<ascii, int>, tup frozen<tuple<text, int>>, l list<text>, d double, t time, dec decimal, tupmap map<frozen<tuple<text, int>>, int>, t1 frozen<{type1}>)")
    yield table
    cql.execute("DROP TABLE " + table)

# Test that failed fromJson() parsing an invalid JSON results in the expected
# error - FunctionFailure - and not some weird internal error.
# Reproduces issue #7911.
def test_failed_json_parsing_unprepared(cql, table1):
    p = random.randint(1,1000000000)
    with pytest.raises(FunctionFailure):
        cql.execute(f"INSERT INTO {table1} (p, v) VALUES ({p}, fromJson('dog'))")
def test_failed_json_parsing_prepared(cql, table1):
    p = random.randint(1,1000000000)
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
    p = random.randint(1,1000000000)
    with pytest.raises(FunctionFailure):
        cql.execute(f"INSERT INTO {table1} (p, v) VALUES ({p}, fromJson('\"dog\"'))")
    with pytest.raises(FunctionFailure):
        cql.execute(f"INSERT INTO {table1} (p, a) VALUES ({p}, fromJson('3'))")
def test_fromjson_wrong_type_prepared(cql, table1):
    p = random.randint(1,1000000000)
    stmt = cql.prepare(f"INSERT INTO {table1} (p, v) VALUES (?, fromJson(?))")
    with pytest.raises(FunctionFailure):
        cql.execute(stmt, [p, '"dog"'])
    stmt = cql.prepare(f"INSERT INTO {table1} (p, a) VALUES (?, fromJson(?))")
    with pytest.raises(FunctionFailure):
        cql.execute(stmt, [p, '3'])
def test_fromjson_bad_ascii_unprepared(cql, table1):
    p = random.randint(1,1000000000)
    with pytest.raises(FunctionFailure):
        cql.execute(f"INSERT INTO {table1} (p, a) VALUES ({p}, fromJson('\"שלום\"'))")
def test_fromjson_bad_ascii_prepared(cql, table1):
    p = random.randint(1,1000000000)
    stmt = cql.prepare(f"INSERT INTO {table1} (p, a) VALUES (?, fromJson(?))")
    with pytest.raises(FunctionFailure):
        cql.execute(stmt, [p, '"שלום"'])
def test_fromjson_nonint_unprepared(cql, table1):
    p = random.randint(1,1000000000)
    with pytest.raises(FunctionFailure):
        cql.execute(f"INSERT INTO {table1} (p, v) VALUES ({p}, fromJson('1.2'))")
def test_fromjson_nonint_prepared(cql, table1):
    p = random.randint(1,1000000000)
    stmt = cql.prepare(f"INSERT INTO {table1} (p, v) VALUES (?, fromJson(?))")
    with pytest.raises(FunctionFailure):
        cql.execute(stmt, [p, '1.2'])

# The JSON standard does not define or limit the range or precision of
# numbers. However, if a number is assigned to a Scylla number type, the
# assignment can overflow and should result in an error - not be silently
# wrapped around.
# Reproduces issue #7914
@pytest.mark.xfail(reason="issue #7914")
def test_fromjson_int_overflow_unprepared(cql, table1):
    p = random.randint(1,1000000000)
    # The highest legal int is 2147483647 (2^31-1).2147483648 is not a legal
    # int, so trying to insert it should result in an error - not silent
    # wraparound to -2147483648 as happened in Scylla.
    with pytest.raises(FunctionFailure):
        cql.execute(f"INSERT INTO {table1} (p, v) VALUES ({p}, fromJson('2147483648'))")
@pytest.mark.xfail(reason="issue #7914")
def test_fromjson_int_overflow_prepared(cql, table1):
    p = random.randint(1,1000000000)
    stmt = cql.prepare(f"INSERT INTO {table1} (p, v) VALUES (?, fromJson(?))")
    with pytest.raises(FunctionFailure):
        cql.execute(stmt, [p, '2147483648'])

# When writing to an integer column, Cassandra's fromJson() function allows
# not just JSON number constants, it also allows a string containing a number.
# Strings which do not hold a number fail with a FunctionFailure. In
# particular, the empty string "" is not a valid number, and should report an
# error, but both Scylla and Cassandra have bugs that allow it for some types
# and not for others. The following tests reproduce #7944. Where Cassandra
# has (what we consider to be) a bug, it is marked with "cassandra_bug"
# which causes it to xfail when testing against Cassandra.
def test_fromjson_int_empty_string_unprepared(cql, table1, cassandra_bug):
    p = random.randint(1,1000000000)
    with pytest.raises(FunctionFailure):
        cql.execute(f"INSERT INTO {table1} (p, v) VALUES ({p}, fromJson('\"\"'))")
def test_fromjson_int_empty_string_prepared(cql, table1, cassandra_bug):
    p = random.randint(1,1000000000)
    stmt = cql.prepare(f"INSERT INTO {table1} (p, v) VALUES (?, fromJson(?))")
    with pytest.raises(FunctionFailure):
        cql.execute(stmt, [p, '""'])
@pytest.mark.xfail(reason="issue #7944")
def test_fromjson_varint_empty_string_unprepared(cql, table1):
    p = random.randint(1,1000000000)
    with pytest.raises(FunctionFailure):
        cql.execute(f"INSERT INTO {table1} (p, vi) VALUES ({p}, fromJson('\"\"'))")
@pytest.mark.xfail(reason="issue #7944")
def test_fromjson_varint_empty_string_prepared(cql, table1):
    p = random.randint(1,1000000000)
    stmt = cql.prepare(f"INSERT INTO {table1} (p, vi) VALUES (?, fromJson(?))")
    with pytest.raises(FunctionFailure):
        cql.execute(stmt, [p, '""'])

# Cassandra allows the strings "true" and "false", not just the JSON constants
# true and false, to be assigned to a boolean column. However, very strangely,
# it only allows this for prepared statements, and *not* for unprepared
# statements - which result in an InvalidRequest!
# Reproduces #7915.
def test_fromjson_boolean_string_unprepared(cql, table1):
    p = random.randint(1,1000000000)
    with pytest.raises(InvalidRequest):
        cql.execute(f"INSERT INTO {table1} (p, b) VALUES ({p}, '\"true\"')")
    with pytest.raises(InvalidRequest):
        cql.execute(f"INSERT INTO {table1} (p, b) VALUES ({p}, '\"false\"')")
@pytest.mark.xfail(reason="issue #7915")
def test_fromjson_boolean_string_prepared(cql, table1):
    p = random.randint(1,1000000000)
    stmt = cql.prepare(f"INSERT INTO {table1} (p, b) VALUES (?, fromJson(?))")
    cql.execute(stmt, [p, '"true"'])
    assert list(cql.execute(f"SELECT p, b from {table1} where p = {p}")) == [(p, True)]
    cql.execute(stmt, [p, '"false"'])
    assert list(cql.execute(f"SELECT p, b from {table1} where p = {p}")) == [(p, False)]

# Test that null argument is allowed for fromJson(), with unprepared statement
# Reproduces issue #7912.
@pytest.mark.xfail(reason="issue #7912")
def test_fromjson_null_unprepared(cql, table1):
    p = random.randint(1,1000000000)
    cql.execute(f"INSERT INTO {table1} (p, v) VALUES ({p}, fromJson(null))")
    assert list(cql.execute(f"SELECT p, v from {table1} where p = {p}")) == [(p, None)]

# Test that null argument is allowed for fromJson(), with prepared statement
# Reproduces issue #7912.
@pytest.mark.xfail(reason="issue #7912")
def test_fromjson_null_prepared(cql, table1):
    p = random.randint(1,1000000000)
    stmt = cql.prepare(f"INSERT INTO {table1} (p, v) VALUES (?, fromJson(?))")
    cql.execute(stmt, [p, None])
    assert list(cql.execute(f"SELECT p, v from {table1} where p = {p}")) == [(p, None)]

# Test that fromJson can parse a map<ascii,int>. Strangely Scylla had a bug
# setting a map<ascii,int> with fromJson(), while map<text,int> worked well.
# Reproduces #7949.
@pytest.mark.xfail(reason="issue #7949")
def test_fromjson_map_ascii_unprepared(cql, table1):
    p = random.randint(1,1000000000)
    cql.execute("INSERT INTO " + table1 + " (p, mai) VALUES (" + str(p) + ", fromJson('{\"a\": 1, \"b\": 2}'))")
    assert list(cql.execute(f"SELECT p, mai from {table1} where p = {p}")) == [(p, {'a': 1, 'b': 2})]
@pytest.mark.xfail(reason="issue #7949")
def test_fromjson_map_ascii_prepared(cql, table1):
    p = random.randint(1,1000000000)
    stmt = cql.prepare(f"INSERT INTO {table1} (p, mai) VALUES (?, fromJson(?))")
    cql.execute(stmt, [p, '{"a": 1, "b": 2}'])
    assert list(cql.execute(f"SELECT p, mai from {table1} where p = {p}")) == [(p, {'a': 1, 'b': 2})]

# With fromJson() the JSON "null" constant can be used to unset a column,
# but can also be used to unset a part of a tuple column. In both cases,
# in addition to fromJson() allowing the expected type, the "null" constant
# should also be allowed. But it's not like a null is allowed *everywhere*
# that a normal value is allowed. For example, it cannot be given as an
# element of a list.
# Reproduces #7954.
@pytest.mark.xfail(reason="issue #7954")
def test_fromjson_null_constant(cql, table1):
    p = random.randint(1,1000000000)
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
    p = random.randint(1,1000000000)
    stmt = cql.prepare(f"INSERT INTO {table1} (p, d) VALUES (?, ?)")
    cql.execute(stmt, [p, 123.456])
    assert list(cql.execute(f"SELECT d, toJson(d) from {table1} where p = {p}")) == [(123.456, "123.456")]
    # While 123.456 above worked, in issue #7972 we note that 123123.123123
    # does not work.
    cql.execute(stmt, [p, 123123.123123])
    assert list(cql.execute(f"SELECT d, toJson(d) from {table1} where p = {p}")) == [(123123.123123, "123123.123123")]

# Check that toJson() correctly formats "time" values. The JSON translation
# is a string containing the time (there is no time type in JSON), and of
# course, a string needs to be wrapped in quotes. (issue #7988
@pytest.mark.xfail(reason="issue #7988")
def test_tojson_time(cql, table1):
    p = random.randint(1,1000000000)
    stmt = cql.prepare(f"INSERT INTO {table1} (p, t) VALUES (?, ?)")
    cql.execute(stmt, [p, 123])
    assert list(cql.execute(f"SELECT toJson(t) from {table1} where p = {p}")) == [('"00:00:00.000000123"',)]

# The EquivalentJson class wraps a JSON string, and compare equal to other
# strings if both are valid JSON strings which decode to the same object.
# EquivalentJson("....") can be used in assert_rows() checks below, to check
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

# Test that toJson() can prints a decimal type with a very high mantissa.
# Reproduces issue #8002, where it was written as 1 and a billion zeroes,
# running out of memory.
# We need to skip this test because in debug mode memory allocation is not
# bounded, and this test can hang or crash instead of failing immediately.
# We also have a smaller xfailing test below, test_tojson_decimal_high_mantissa2.
@pytest.mark.skip(reason="issue #8002")
def test_tojson_decimal_high_mantissa(cql, table1):
    p = random.randint(1,1000000000)
    stmt = cql.prepare(f"INSERT INTO {table1} (p, dec) VALUES ({p}, ?)")
    high = '1e1000000000'
    cql.execute(stmt, [Decimal(high)])
    assert list(cql.execute(f"SELECT toJson(dec) from {table1} where p = {p}")) == [(EquivalentJson(high),)]

# This is a smaller version of test_tojson_decimal_high_mantissa, showing
# that a much smaller exponent, 1e1000 works (this is not surprising) but
# results in 1000 digits of output. This hints that 1e1000000000 willl not
# work at all, without testing it directly as above.
@pytest.mark.xfail(reason="issue #8002")
def test_tojson_decimal_high_mantissa2(cql, table1):
    p = random.randint(1,1000000000)
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
    p = random.randint(1,1000000000)
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
    p = random.randint(1,1000000000)
    stmt = cql.prepare(f"INSERT INTO {table1} (p, tupmap) VALUES ({p}, ?)")
    cql.execute(stmt, [{('hello', 3): 7}])
    expected = '{"tupmap": {"[\\"hello\\", 3]": 7}}'
    assert list(cql.execute(f"SELECT JSON tupmap from {table1} where p = {p}")) == [(expected,)]

# Test that SELECT JSON correctly prints unset components of a UDT or tuple
# as "null". This test passes which demonstrates that issue #8092 is specific
# to altering a UDT, and doesn't just happen for every null component of a
# UDT or tuple.
def test_select_json_null_component(cql, table1, type1):
    p = random.randint(1,1000000000)
    stmt = cql.prepare(f"INSERT INTO {table1} (p, tup) VALUES ({p}, ?)")
    cql.execute(stmt, [('hello', None)])
    assert list(cql.execute(f"SELECT JSON tup from {table1} where p = {p}")) == [('{"tup": ["hello", null]}',)]

    stmt = cql.prepare(f"INSERT INTO {table1} (p, t1) VALUES ({p}, ?)")
    cql.execute(stmt, [('hello', None)])
    assert list(cql.execute(f"SELECT JSON t1 from {table1} where p = {p}")) == [('{"t1": {"t": "hello", "b": null}}',)]
