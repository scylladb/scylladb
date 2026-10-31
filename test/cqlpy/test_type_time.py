# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Test involving the "time" column type.
#############################################################################

from .util import unique_name, unique_key_int

import pytest

from cassandra.util import Time
from cassandra.protocol import InvalidRequest

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
def test_type_time_from_int_unprepared(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, t) VALUES ({p}, 123)")
    assert list(cql.execute(f"SELECT t from {table1} where p = {p}")) == [(Time(123),)]

def test_type_time_from_int_prepared(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"INSERT INTO {table1} (p, t) VALUES (?, ?)")
    cql.execute(stmt, [p, 123])
    assert list(cql.execute(f"SELECT t from {table1} where p = {p}")) == [(Time(123),)]

# A time given as an integer is the number of nanoseconds since midnight,
# so it must be at least 0 and less than the number of nanoseconds in a day.
# Such an integer can also be given in a string - in a CQL string literal or
# in JSON.
# Reproduces issue #7987
def test_type_time_from_int_more(cql, table1):
    p = unique_key_int()
    nanoseconds_per_day = 24 * 60 * 60 * 1000000000
    for n in [0, nanoseconds_per_day - 1]:
        cql.execute(f"INSERT INTO {table1} (p, t) VALUES ({p}, {n})")
        assert list(cql.execute(f"SELECT t from {table1} where p = {p}")) == [(Time(n),)]
    for n in [-1, nanoseconds_per_day]:
        with pytest.raises(InvalidRequest):
            cql.execute(f"INSERT INTO {table1} (p, t) VALUES ({p}, {n})")
        with pytest.raises(InvalidRequest):
            cql.execute(f"INSERT INTO {table1} (p, t) VALUES ({p}, '{n}')")
    # Check that the integer really means nanoseconds since midnight: it is
    # the same time as the equivalent hh:mm:ss.fffffffff string, and toJson()
    # converts it to that string.
    p2 = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, t) VALUES ({p2}, '08:12:54.123456789')")
    for n in ["29574123456789", "'29574123456789'"]:
        cql.execute(f"INSERT INTO {table1} (p, t) VALUES ({p}, {n})")
        assert list(cql.execute(f"SELECT t from {table1} where p = {p}")) == list(cql.execute(f"SELECT t from {table1} where p = {p2}"))
        assert list(cql.execute(f"SELECT toJson(t) from {table1} where p = {p}")) == [('"08:12:54.123456789"',)]
    cql.execute(f"INSERT INTO {table1} (p, t) VALUES ({p}, '456')")
    assert list(cql.execute(f"SELECT t from {table1} where p = {p}")) == [(Time(456),)]
    cql.execute(f"""INSERT INTO {table1} JSON '{{"p": {p}, "t": "789"}}'""")
    assert list(cql.execute(f"SELECT t from {table1} where p = {p}")) == [(Time(789),)]
    cql.execute(f"""INSERT INTO {table1} (p, t) VALUES ({p}, fromJson('"321"'))""")
    assert list(cql.execute(f"SELECT t from {table1} where p = {p}")) == [(Time(321),)]

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

# A malformed time string, e.g., one with a non-numeric or out-of-range
# component, should be rejected with an InvalidRequest error - both as a CQL
# string literal and as a JSON value in INSERT JSON - not cause some
# internal server error.
def test_type_time_from_bad_string(cql, table1):
    p = unique_key_int()
    for bad in ['x:00:00', '01:x:00', '01:00:x', '01:00:00.x', ':00:00',
                '99999999999999999999:00:00', '01:00:00.99999999999999999999']:
        with pytest.raises(InvalidRequest):
            cql.execute(f"INSERT INTO {table1} (p, t) VALUES ({p}, '{bad}')")
        with pytest.raises(InvalidRequest):
            cql.execute(f"""INSERT INTO {table1} JSON '{{"p": {p}, "t": "{bad}"}}'""")
