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
