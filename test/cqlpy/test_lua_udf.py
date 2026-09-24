# -*- coding: utf-8 -*-
# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for user-defined functions (UDF) written in Lua. Lua is a
# Scylla-only UDF language, so all tests here are Scylla-only.
# Ported from test/boost/user_function_test.cc.
#############################################################################

import math
import re
from decimal import Decimal
from uuid import UUID

import pytest
from cassandra.cluster import NoHostAvailable
from cassandra.protocol import InvalidRequest, SyntaxException, Unauthorized
from cassandra.util import Date, Duration

from .util import new_test_table, new_type, new_function, unique_name

# Some errors (e.g., marshaling errors) are returned by the server as a
# SERVER_ERROR, which the driver converts to NoHostAvailable.

# Create a Lua function with the given signature (argument list, null
# handling and return type) and body, call it with the given arguments
# on all rows of the table, and return the resulting values.
def call_lua(cql, keyspace, table, signature, body, args="val"):
    with new_function(cql, keyspace, f"{signature} LANGUAGE lua AS '{body}'") as f:
        return [row[0] for row in cql.execute(f"SELECT {keyspace}.{f}({args}) FROM {table}")]

def test_lua_out_of_memory(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val int") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', null)")
        with pytest.raises(InvalidRequest, match="lua execution failed: not enough memory"):
            call_lua(cql, test_keyspace, table, "(val int) CALLED ON NULL INPUT RETURNS int",
                     'a = "foo" while true do a = a .. a end')

def test_lua_use_null(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val int") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', null)")
        with pytest.raises(InvalidRequest, match="attempt to perform arithmetic on a nil value"):
            call_lua(cql, test_keyspace, table, "(val int) CALLED ON NULL INPUT RETURNS int", "return val + 1")
        assert call_lua(cql, test_keyspace, table, "(val int) CALLED ON NULL INPUT RETURNS int", "return val") == [None]
        assert [row.val for row in cql.execute(f"SELECT val FROM {table}")] == [None]

def test_lua_wrong_return_type(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val int") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', null)")
        with pytest.raises(InvalidRequest, match="value is not an integer"):
            call_lua(cql, test_keyspace, table, "(val int) CALLED ON NULL INPUT RETURNS int", "return 1.2")

def test_lua_too_many_return_values(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val int") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', null)")
        with pytest.raises(InvalidRequest, match="2 values returned, expected 1"):
            call_lua(cql, test_keyspace, table, "(val int) CALLED ON NULL INPUT RETURNS int", "return 1,2")

def test_lua_reversed_argument(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text, val int, PRIMARY KEY ((key), val)",
                        "WITH CLUSTERING ORDER BY (val DESC)") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 1)")
        assert call_lua(cql, test_keyspace, table, "(val int) CALLED ON NULL INPUT RETURNS int", "return 2 * val") == [2]

def test_lua_boolean_argument(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val boolean") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', true)")
        assert call_lua(cql, test_keyspace, table, "(val boolean) CALLED ON NULL INPUT RETURNS int", "return val and 1 or 0") == [1]

def test_lua_time_argument(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val time") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', '01:23:45.6789')")
        assert call_lua(cql, test_keyspace, table, "(val time) CALLED ON NULL INPUT RETURNS bigint", "return val") == [
            (((60 + 23)*60 + 45)*10000 + 6789)*100000]

def test_lua_timestamp_argument(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val timestamp") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', '2011-03-02 04:05+0000')")
        assert call_lua(cql, test_keyspace, table, "(val timestamp) CALLED ON NULL INPUT RETURNS bigint", "return val") == [1299038700000]

def test_lua_date_argument(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val date") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', '2019-08-26')")
        assert call_lua(cql, test_keyspace, table, "(val date) CALLED ON NULL INPUT RETURNS int", "return val - 2^31") == [18134]

def test_lua_counter_argument(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val counter") as table:
        cql.execute(f"UPDATE {table} SET val = val + 1 WHERE key = 'foo'")
        assert call_lua(cql, test_keyspace, table, "(val counter) CALLED ON NULL INPUT RETURNS int", "return val * 2") == [2]

def test_lua_duration_argument(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val duration") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 1mo2d3ns)")
        assert call_lua(cql, test_keyspace, table, "(val duration) CALLED ON NULL INPUT RETURNS int",
                        "return 100 * val.months + 10 * val.days + val.nanoseconds") == [123]

def test_lua_inet_argument(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val inet") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', '127.0.0.1')")
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('bar', '::1')")
        assert sorted(call_lua(cql, test_keyspace, table, "(val inet) CALLED ON NULL INPUT RETURNS text", "return val")) == [
            "127.0.0.1", "::1"]

def test_lua_uuid_argument(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val uuid") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 5375ddb6-d5a5-4cce-9aa1-b10c3fea36a3)")
        assert call_lua(cql, test_keyspace, table, "(val uuid) CALLED ON NULL INPUT RETURNS text", "return val") == [
            "5375ddb6-d5a5-4cce-9aa1-b10c3fea36a3"]

def test_lua_utf8_argument(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val text") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 'bár')")
        assert call_lua(cql, test_keyspace, table, "(val text) CALLED ON NULL INPUT RETURNS int", "return val:byte(2)") == [0xc3]

def test_lua_blob_argument(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val blob") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 0x123456)")
        assert call_lua(cql, test_keyspace, table, "(val blob) CALLED ON NULL INPUT RETURNS int", "return val:byte(2)") == [0x34]

def test_lua_tuple_argument(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val tuple<int, bigint, int>") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', (1, 2, 3))")
        assert call_lua(cql, test_keyspace, table, "(val tuple<int, bigint, int>) CALLED ON NULL INPUT RETURNS bigint",
                        "return val[1] + val[2] + val[3]") == [6]

def test_lua_udt_argument(cql, test_keyspace, scylla_only):
    with new_type(cql, test_keyspace, "(my_int int)") as udt:
        with new_test_table(cql, test_keyspace, f"key text PRIMARY KEY, val frozen<{udt}>") as table:
            cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', {{my_int : 42}})")
            assert call_lua(cql, test_keyspace, table, f"(val {udt}) CALLED ON NULL INPUT RETURNS int", "return val.my_int") == [42]

def test_lua_set_argument(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val set<int>") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', {{1, 2, 3}})")
        assert call_lua(cql, test_keyspace, table, "(val set<int>) CALLED ON NULL INPUT RETURNS int",
                        "local ret = 0; for k in pairs(val) do ret = ret + k; end return ret") == [6]

def test_lua_map_argument(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val map<int, int>") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', {{1 : 2, 3 : 4, 5: 6}})")
        sig = "(val map<int, int>) CALLED ON NULL INPUT RETURNS int"
        sum_keys = "local ret = 0; for k, v in pairs(val) do ret = ret + k; end return ret"
        sum_values = "local ret = 0; for k, v in pairs(val) do ret = ret + v; end return ret"
        with new_function(cql, test_keyspace, f"{sig} LANGUAGE lua AS '{sum_keys}'") as f1, \
             new_function(cql, test_keyspace, f"{sig} LANGUAGE lua AS '{sum_values}'") as f2:
            assert list(cql.execute(f"SELECT {test_keyspace}.{f1}(val), {test_keyspace}.{f2}(val) FROM {table}").one()) == [9, 12]

def test_lua_decimal_argument(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val decimal") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 3)")
        assert call_lua(cql, test_keyspace, table, "(val decimal) CALLED ON NULL INPUT RETURNS bigint", "return 42") == [42]

def test_lua_decimal_add(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val1 decimal") as table:
        cql.execute(f"INSERT INTO {table} (key, val1) VALUES ('foo', 1.5)")
        assert call_lua(cql, test_keyspace, table, "(a decimal) CALLED ON NULL INPUT RETURNS decimal",
                        "return a + 1", args="val1") == [Decimal("2.5")]
        assert call_lua(cql, test_keyspace, table, "(a decimal) CALLED ON NULL INPUT RETURNS double",
                        "return 42.2 + a", args="val1") == [43.7]

def test_lua_decimal_sub(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val1 decimal, val2 decimal") as table:
        cql.execute(f"INSERT INTO {table} (key, val1, val2) VALUES ('foo', 4, 1)")
        assert call_lua(cql, test_keyspace, table, "(a decimal, b decimal) CALLED ON NULL INPUT RETURNS decimal",
                        "return a - b", args="val1, val2") == [Decimal(3)]

def test_lua_decimal_return(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val1 varint, val2 decimal") as table:
        cql.execute(f"INSERT INTO {table} (key, val1, val2) VALUES ('foo', 42, 42.2)")
        assert call_lua(cql, test_keyspace, table, "(a varint) CALLED ON NULL INPUT RETURNS decimal",
                        "return a", args="val1") == [Decimal(42)]
        assert call_lua(cql, test_keyspace, table, "(a decimal) CALLED ON NULL INPUT RETURNS decimal",
                        "return a", args="val2") == [Decimal("42.2")]
        with pytest.raises(InvalidRequest, match="value is not a decimal"):
            call_lua(cql, test_keyspace, table, "(a varint) CALLED ON NULL INPUT RETURNS decimal",
                     "return 4.2", args="val1")
        assert call_lua(cql, test_keyspace, table, "(a varint) CALLED ON NULL INPUT RETURNS decimal",
                        'return "18446744073709551616.1"', args="val1") == [Decimal("18446744073709551616.1")]

def test_lua_varint_return(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val int") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 42)")
        assert call_lua(cql, test_keyspace, table, "(a int) CALLED ON NULL INPUT RETURNS varint", "return a") == [42]

def test_lua_double_return(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val varint") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 3)")
        sig = "(val varint) CALLED ON NULL INPUT RETURNS double"
        assert call_lua(cql, test_keyspace, table, sig, "return val") == [3.0]
        assert call_lua(cql, test_keyspace, table, sig, "return 1/0") == [math.inf]
        assert call_lua(cql, test_keyspace, table, sig, "return -1/0") == [-math.inf]
        [res] = call_lua(cql, test_keyspace, table, sig, "return 0/0")
        assert math.isnan(res)
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val decimal") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 5.1)")
        assert call_lua(cql, test_keyspace, table, "(val decimal) CALLED ON NULL INPUT RETURNS double", "return val") == [5.1]

def test_lua_sum_of_udf(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "val int PRIMARY KEY") as table:
        cql.execute(f"INSERT INTO {table} (val) VALUES (1)")
        cql.execute(f"INSERT INTO {table} (val) VALUES (2)")
        with new_function(cql, test_keyspace, "(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE lua AS 'return val'") as f:
            assert cql.execute(f"SELECT sum({test_keyspace}.{f}(val)) FROM {table}").one()[0] == 3

def test_lua_tinyint_return(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val1 int, val2 int, val3 int, val4 varint") as table:
        cql.execute(f"INSERT INTO {table} (key, val1, val2, val3, val4) VALUES ('foo', 3, -1, 128, 9223372036854775808)")
        with new_function(cql, test_keyspace, "(val varint) CALLED ON NULL INPUT RETURNS tinyint LANGUAGE lua AS 'return val'") as f:
            def call(col):
                return cql.execute(f"SELECT {test_keyspace}.{f}({col}) FROM {table}").one()[0]
            assert call("val1") == 3
            assert call("val2") == -1
            assert call("val3") == -128
            assert call("val4") == 0

def test_lua_int_return(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val int") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 3)")
        with new_function(cql, test_keyspace, "(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE lua AS 'return 2 * val'") as f:
            assert cql.execute(f"SELECT {test_keyspace}.{f}(val) FROM {table}").one()[0] == 6
            cql.execute(f"CREATE OR REPLACE FUNCTION {test_keyspace}.{f}(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE lua AS 'return val'")
            assert cql.execute(f"SELECT {test_keyspace}.{f}(val) FROM {table}").one()[0] == 3
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val tinyint") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 4)")
        assert call_lua(cql, test_keyspace, table, "(val tinyint) CALLED ON NULL INPUT RETURNS int", "return val") == [4]
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val varint") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 4)")
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('bar', 2147483648)")
        assert sorted(call_lua(cql, test_keyspace, table, "(val varint) CALLED ON NULL INPUT RETURNS int", "return val")) == [
            -2147483648, 4]
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val double") as table:
        sig = "(val double) CALLED ON NULL INPUT RETURNS int"
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 4)")
        assert call_lua(cql, test_keyspace, table, sig, "return val") == [4]
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 4.2)")
        with pytest.raises(InvalidRequest, match="value is not an integer"):
            call_lua(cql, test_keyspace, table, sig, "return val")
        with pytest.raises(InvalidRequest, match="value is not a number"):
            call_lua(cql, test_keyspace, table, sig, 'return "foo"')
        assert call_lua(cql, test_keyspace, table, sig, 'return "123"') == [123]
        assert call_lua(cql, test_keyspace, table, sig, 'return "0x123p+1"') == [0x246]
        with pytest.raises(InvalidRequest, match="unexpected value"):
            call_lua(cql, test_keyspace, table, sig, "return false")
        with pytest.raises(InvalidRequest, match="value is not a number"):
            call_lua(cql, test_keyspace, table, sig, 'return ""')

def test_lua_date_return(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val int") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 3)")
        sig = "(val int) CALLED ON NULL INPUT RETURNS date"
        # A date is represented as the number of days since the epoch
        # plus 2^31, so an integer 3 is a date long before the epoch.
        assert call_lua(cql, test_keyspace, table, sig, "return val") == [Date(3 - 2**31)]
        assert call_lua(cql, test_keyspace, table, sig, 'return "2019-10-01"') == [Date("2019-10-01")]
        with pytest.raises(InvalidRequest, match="date value must fit in 32 bits"):
            call_lua(cql, test_keyspace, table, sig, "return 4294967296")
        assert call_lua(cql, test_keyspace, table, sig, "return {year = 2019, month = 10, day = 1}") == [Date("2019-10-01")]
        with pytest.raises(InvalidRequest, match="year is too large: '2147483648'"):
            call_lua(cql, test_keyspace, table, sig, "return {year = 2147483648, month = 10, day = 1}")
        with pytest.raises(InvalidRequest, match="month is too large: '256'"):
            call_lua(cql, test_keyspace, table, sig, "return {year = 2019, month = 256, day = 1}")
        with pytest.raises(InvalidRequest, match="day is too large: '256'"):
            call_lua(cql, test_keyspace, table, sig, "return {year = 2019, month = 10, day = 256}")
        with pytest.raises(InvalidRequest, match="invalid date table field: 'abc'"):
            call_lua(cql, test_keyspace, table, sig, "return {year = 2019, month = 10, day = 1, abc = 42}")
        with pytest.raises(InvalidRequest, match="date table must have year, month and day"):
            call_lua(cql, test_keyspace, table, sig, "return {year = 2019, month = 10}")
        with pytest.raises(InvalidRequest, match="date value must fit in 32 bits"):
            call_lua(cql, test_keyspace, table, sig, "return {year = 2147483647, month = 10, day = 1}")
        with pytest.raises(InvalidRequest, match="date must be a string, integer or date table"):
            call_lua(cql, test_keyspace, table, sig, "return 42.2")
        with pytest.raises(InvalidRequest, match="date type has no hour, minute or second"):
            call_lua(cql, test_keyspace, table, sig, "return {year = 2019, month = 10, day = 1, hour = 4}")

def test_lua_inet_return(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val int") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 3)")
        sig = "(val int) CALLED ON NULL INPUT RETURNS inet"
        assert call_lua(cql, test_keyspace, table, sig, 'return "1.2.3.4"') == ["1.2.3.4"]
        with pytest.raises(NoHostAvailable, match="marshaling error: Failed to parse inet_addr from 'abc'"):
            call_lua(cql, test_keyspace, table, sig, 'return "abc"')
        with pytest.raises(NoHostAvailable, match="marshaling error: Failed to parse inet_addr from ''"):
            call_lua(cql, test_keyspace, table, sig, 'return ""')

def test_lua_boolean_return(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val int") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 3)")
        assert call_lua(cql, test_keyspace, table, "(val int) CALLED ON NULL INPUT RETURNS boolean", "return val > 4") == [False]

def test_lua_ascii_return(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val int") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 3)")
        sig = "(val int) CALLED ON NULL INPUT RETURNS ascii"
        assert call_lua(cql, test_keyspace, table, sig, 'return "foo"') == ["foo"]
        with pytest.raises(InvalidRequest, match="value is not valid ascii"):
            call_lua(cql, test_keyspace, table, sig, 'return "foó"')

def test_lua_utf8_return(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val varint") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 3)")
        sig = "(val varint) CALLED ON NULL INPUT RETURNS text"
        assert call_lua(cql, test_keyspace, table, sig, 'return "foó"') == ["foó"]
        assert call_lua(cql, test_keyspace, table, sig, "return val") == ["3"]
        with pytest.raises(InvalidRequest, match="value is not valid utf8, invalid character at byte offset 0"):
            call_lua(cql, test_keyspace, table, sig, r'return "\xFF"')
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val decimal") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 4.2)")
        assert call_lua(cql, test_keyspace, table, "(val decimal) CALLED ON NULL INPUT RETURNS text", "return val") == ["4.2"]

def test_lua_blob_return(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val int") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 3)")
        assert call_lua(cql, test_keyspace, table, "(val int) CALLED ON NULL INPUT RETURNS blob", 'return "foó"') == [
            "foó".encode()]

def test_lua_counter_return(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val int") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 3)")
        assert call_lua(cql, test_keyspace, table, "(val int) CALLED ON NULL INPUT RETURNS counter", "return 42") == [42]

# The Python driver can't represent time values outside a single day, so
# we convert the function's result to a bigint on the server.
def test_lua_time_return(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val varint") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 9223372036854775807)")
        sig = "(val varint) CALLED ON NULL INPUT RETURNS time"
        with new_function(cql, test_keyspace, f"{sig} LANGUAGE lua AS 'return val'") as f:
            query = f"SELECT blobasbigint(timeasblob({test_keyspace}.{f}(val))) FROM {table}"
            assert cql.execute(query).one()[0] == 9223372036854775807
            with new_function(cql, test_keyspace, f"""{sig} LANGUAGE lua AS 'return "08:12:54.123"'""") as f2:
                assert cql.execute(f"SELECT blobasbigint(timeasblob({test_keyspace}.{f2}(val))) FROM {table}").one()[0] == 29574123000000
            with pytest.raises(NoHostAvailable, match=re.escape("marshaling error: Timestamp format must be hh:mm:ss[.fffffffff]")):
                call_lua(cql, test_keyspace, table, sig, 'return "abc"')
            cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 9223372036854775808)")
            with pytest.raises(InvalidRequest, match="time value must fit in signed 64 bits"):
                cql.execute(query)
        with pytest.raises(InvalidRequest, match="time must be a string or an integer"):
            call_lua(cql, test_keyspace, table, sig, "return 42.2")

# The Python driver can't represent all timestamp values, so we convert
# the function's result to a bigint on the server.
def test_lua_timestamp_return(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val varint") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 9223372036854775807)")
        sig = "(val varint) CALLED ON NULL INPUT RETURNS timestamp"
        def call(body):
            with new_function(cql, test_keyspace, f"{sig} LANGUAGE lua AS '{body}'") as f:
                return cql.execute(f"SELECT blobasbigint(timestampasblob({test_keyspace}.{f}(val))) FROM {table}").one()[0]
        with new_function(cql, test_keyspace, f"{sig} LANGUAGE lua AS 'return val'") as f:
            query = f"SELECT blobasbigint(timestampasblob({test_keyspace}.{f}(val))) FROM {table}"
            assert cql.execute(query).one()[0] == 9223372036854775807
            assert call('return "2011-02-03 04:05:06+0000"') == 0x12de9b1e550
            assert call("return {year = 2011, month = 2, day = 3, hour = 4, min = 5, sec = 6 }") == 0x12de9b1e550
            # Different boost versions support different year ranges, but no version supports year 10001.
            with pytest.raises(NoHostAvailable, match="Year is out of valid range:"):
                call("return {year = 10001, month = 2, day = 3, hour = 4, min = 5, sec = 6 }")
            # FIXME: the exception message is redundant.
            with pytest.raises(NoHostAvailable, match="marshaling error: unable to parse date 'abc': marshaling error: Unable to parse timestamp from 'abc'"):
                call('return "abc"')
            cql.execute(f"INSERT INTO {table} (key, val) VALUES ('bar', 9223372036854775808)")
            with pytest.raises(InvalidRequest, match="timestamp value must fit in signed 64 bits"):
                cql.execute(query)
        with pytest.raises(InvalidRequest, match="timestamp must be a string, integer or date table"):
            call("return 42.2")

def test_lua_uuid_return(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val int") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 3)")
        assert call_lua(cql, test_keyspace, table, "(val int) CALLED ON NULL INPUT RETURNS uuid",
                        'return "982e9b0f-1df7-4425-ba04-e99d808b8940"') == [UUID("982e9b0f-1df7-4425-ba04-e99d808b8940")]

def test_lua_timeuuid_return(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val int") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 3)")
        sig = "(val int) CALLED ON NULL INPUT RETURNS timeuuid"
        assert call_lua(cql, test_keyspace, table, sig, 'return "d18648bc-cf83-11e9-9820-107b4493b787"') == [
            UUID("d18648bc-cf83-11e9-9820-107b4493b787")]
        with pytest.raises(NoHostAvailable, match=re.escape("marshaling error: Unsupported UUID version (2)")):
            call_lua(cql, test_keyspace, table, sig, 'return "d18648bc-cf83-21e9-9820-107b4493b787"')

def test_lua_tuple_return(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val int") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 3)")
        sig = "(val int) CALLED ON NULL INPUT RETURNS tuple<int, double, text>"
        assert call_lua(cql, test_keyspace, table, sig, 'return {1,2.4,"foo"}') == [(1, 2.4, "foo")]
        with pytest.raises(InvalidRequest, match="value is not an integer"):
            call_lua(cql, test_keyspace, table, sig, 'return {1.2, 1.2, "foo"}')
        with pytest.raises(InvalidRequest, match="key 4 is not valid for a sequence of size 3"):
            call_lua(cql, test_keyspace, table, sig, 'return {1,2.4,"foo", 42}')
        with pytest.raises(InvalidRequest, match="key 2 missing in sequence of size 3"):
            call_lua(cql, test_keyspace, table, sig, 'return {[1] = 1, [3] = "foo"}')

def test_lua_vector_return(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val int") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 3)")
        sig = "(val int) CALLED ON NULL INPUT RETURNS vector<int, 3>"
        assert call_lua(cql, test_keyspace, table, sig, "return {1,2,3}") == [[1, 2, 3]]
        with pytest.raises(InvalidRequest, match="value is not an integer"):
            call_lua(cql, test_keyspace, table, sig, "return {1, 2.1, 3}")
        with pytest.raises(InvalidRequest, match="key 4 is not valid for a sequence of size 3"):
            call_lua(cql, test_keyspace, table, sig, "return {1, 2, 3, 4}")
        with pytest.raises(InvalidRequest, match="key 2 missing in sequence of size 3"):
            call_lua(cql, test_keyspace, table, sig, "return {[1] = 1, [3] = 3}")

def test_lua_list_return(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val int") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 3)")
        sig = "(val int) CALLED ON NULL INPUT RETURNS list<int>"
        assert call_lua(cql, test_keyspace, table, sig, "return {1,2,3}") == [[1, 2, 3]]
        with pytest.raises(InvalidRequest, match="value is not an integer"):
            call_lua(cql, test_keyspace, table, sig, "return {1.2}")
        with pytest.raises(InvalidRequest, match="value is not a table"):
            call_lua(cql, test_keyspace, table, sig, 'return "foo"')
        with pytest.raises(InvalidRequest, match="value is not a number"):
            call_lua(cql, test_keyspace, table, sig, "return {foo = 42}")
        with pytest.raises(InvalidRequest, match="table is not a sequence"):
            call_lua(cql, test_keyspace, table, sig, "return {[1] = 42, [3] = 43}")

def test_lua_set_return(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val int") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 3)")
        sig = "(val int) CALLED ON NULL INPUT RETURNS set<int>"
        assert call_lua(cql, test_keyspace, table, sig, "return {[1] = true, [42] = true}") == [{1, 42}]
        with pytest.raises(InvalidRequest, match="sets are represented with tables with true values"):
            call_lua(cql, test_keyspace, table, sig, "return {[1] = false}")

# The Python driver's representation of nested collections is awkward
# to compare, so we compare the JSON representation of the result.
def test_lua_nested_types(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val int") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 3)")
        sig = "(val int) CALLED ON NULL INPUT RETURNS map<int, frozen<set<frozen<list<frozen<tuple<text, vector<bigint, 2>>>>>>>>"
        body = 'return {[42] = {[{{"foo", {41, 43}}, {"bar", {40, 44}}}] = true, [{{"bar", {40, 44}}}] = true}, [39] = {}}'
        with new_function(cql, test_keyspace, f"{sig} LANGUAGE lua AS '{body}'") as f:
            res = cql.execute(f"SELECT toJson({test_keyspace}.{f}(val)) FROM {table}").one()[0]
            assert res == '{"39": [], "42": [[["bar", [40, 44]]], [["foo", [41, 43]], ["bar", [40, 44]]]]}'

def test_lua_duration_return(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "key text PRIMARY KEY, val int") as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ('foo', 3)")
        sig = "(val int) CALLED ON NULL INPUT RETURNS duration"
        assert call_lua(cql, test_keyspace, table, sig, "return {months = 1, days = 2147483647, nanoseconds = 3}") == [
            Duration(1, 2147483647, 3)]
        with pytest.raises(InvalidRequest, match="2147483648 days doesn't fit in a 32 bit integer"):
            call_lua(cql, test_keyspace, table, sig, "return {months = 1, days = 2147483648, nanoseconds = 3}")
        with pytest.raises(InvalidRequest, match="2147483648 months doesn't fit in a 32 bit integer"):
            call_lua(cql, test_keyspace, table, sig, "return {months = 2147483648, days = 2, nanoseconds = 3}")
        with pytest.raises(InvalidRequest, match="9223372036854775808 nanoseconds doesn't fit in a 64 bit integer"):
            call_lua(cql, test_keyspace, table, sig, 'return {months = 1, days = 2, nanoseconds = "9223372036854775808"}')
        assert call_lua(cql, test_keyspace, table, sig, 'return "1mo2d3ns"') == [Duration(1, 2, 3)]
        with pytest.raises(InvalidRequest, match=re.escape("a duration must be of the form { months = v1, days = v2, nanoseconds = v3 }")):
            call_lua(cql, test_keyspace, table, sig, "return 42.2")
        with pytest.raises(InvalidRequest, match="invalid duration field: 'foo'"):
            call_lua(cql, test_keyspace, table, sig, "return {foo = 42}")
