# -*- coding: utf-8 -*-
# Copyright 2020 ScyllaDB
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

from cassandra.protocol import FunctionFailure

import pytest
import random

@pytest.fixture(scope="session")
def table1(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (p int PRIMARY KEY, v int, a ascii)")
    yield table
    cql.execute("DROP TABLE " + table)

# Test that failed fromJson() parsing an invalid JSON results in the expected
# error - FunctionFailure - and not some weird internal error.
# Reproduces issue #7911.
@pytest.mark.xfail(reason="issue #7911")
def test_failed_json_parsing_unprepared(cql, table1):
    p = random.randint(1,1000000000)
    with pytest.raises(FunctionFailure):
        cql.execute(f"INSERT INTO {table1} (p, v) VALUES ({p}, fromJson('dog'))")
@pytest.mark.xfail(reason="issue #7911")
def test_failed_json_parsing_prepared(cql, table1):
    p = random.randint(1,1000000000)
    stmt = cql.prepare(f"INSERT INTO {table1} (p, v) VALUES (?, fromJson(?))")
    with pytest.raises(FunctionFailure):
        cql.execute(stmt, [0, 'dog'])

# Similarly, if the JSON parsing did not fail, but yielded a type which is
# incompatible with the type we want it to yield, we should get a clean
# FunctionFailure, not some internal server error.
# We have here examples of returning a string where a number was expected,
# and returning a unicode string where ASCII was expected.
# Reproduces issue #7911.
@pytest.mark.xfail(reason="issue #7911")
def test_fromjson_wrong_type_unprepared(cql, table1):
    p = random.randint(1,1000000000)
    with pytest.raises(FunctionFailure):
        cql.execute(f"INSERT INTO {table1} (p, v) VALUES ({p}, fromJson('\"dog\"'))")
    with pytest.raises(FunctionFailure):
        cql.execute(f"INSERT INTO {table1} (p, a) VALUES ({p}, fromJson('3'))")
@pytest.mark.xfail(reason="issue #7911")
def test_fromjson_wrong_type_prepared(cql, table1):
    p = random.randint(1,1000000000)
    stmt = cql.prepare(f"INSERT INTO {table1} (p, v) VALUES (?, fromJson(?))")
    with pytest.raises(FunctionFailure):
        cql.execute(stmt, [0, '"dog"'])
    stmt = cql.prepare(f"INSERT INTO {table1} (p, a) VALUES (?, fromJson(?))")
    with pytest.raises(FunctionFailure):
        cql.execute(stmt, [0, '3'])
@pytest.mark.xfail(reason="issue #7911")
def test_fromjson_bad_ascii_unprepared(cql, table1):
    p = random.randint(1,1000000000)
    with pytest.raises(FunctionFailure):
        cql.execute(f"INSERT INTO {table1} (p, a) VALUES ({p}, fromJson('\"שלום\"'))")
@pytest.mark.xfail(reason="issue #7911")
def test_fromjson_bad_ascii_prepared(cql, table1):
    p = random.randint(1,1000000000)
    stmt = cql.prepare(f"INSERT INTO {table1} (p, a) VALUES (?, fromJson(?))")
    with pytest.raises(FunctionFailure):
        cql.execute(stmt, [0, '"שלום"'])

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
        cql.execute(stmt, [0, '2147483648'])

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
