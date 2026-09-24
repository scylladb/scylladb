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
