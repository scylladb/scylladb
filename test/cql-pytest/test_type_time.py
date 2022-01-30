# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Test involving the "time" column type.
#############################################################################

from util import unique_name, unique_key_int

import pytest

from cassandra.util import Time

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (p int PRIMARY KEY, t time)")
    yield table
    cql.execute("DROP TABLE " + table)

# According to the Cassandra documentation, a time "can be input either as an
# integer or using a string representing the time. In the later case, the
# format should be hh:mm:ss[.fffffffff] (where the sub-second precision is
# optional and if provided, can be less than the nanosecond). The following
# tests verify that these different initialization options actually work.
# Reproduces issue #7987
@pytest.mark.xfail(reason="issue #7987")
def test_type_time_from_int_unprepared(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, t) VALUES ({p}, 123)")
    assert list(cql.execute(f"SELECT t from {table1} where p = {p}")) == [(Time(123),)]

def test_type_time_from_int_prepared(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"INSERT INTO {table1} (p, t) VALUES (?, ?)")
    cql.execute(stmt, [p, 123])
    assert list(cql.execute(f"SELECT t from {table1} where p = {p}")) == [(Time(123),)]

def test_type_time_from_string_unprepared(cql, table1):
    p = unique_key_int()
    for t in ["08:12:54", "08:12:54.123", "08:12:54.123456", "08:12:54.123456789"]:
        cql.execute(f"INSERT INTO {table1} (p, t) VALUES ({p}, '{t}')")
        assert list(cql.execute(f"SELECT t from {table1} where p = {p}")) == [(Time(t),)]

def test_type_time_from_string_prepared(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"INSERT INTO {table1} (p, t) VALUES (?, ?)")
    for t in ["08:12:54", "08:12:54.123", "08:12:54.123456", "08:12:54.123456789"]:
        cql.execute(stmt, [p, t])
        assert list(cql.execute(f"SELECT t from {table1} where p = {p}")) == [(Time(t),)]
