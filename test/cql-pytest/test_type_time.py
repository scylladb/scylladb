# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Test involving the "time" column type.
#############################################################################

from util import unique_name, unique_key_int
from cassandra.protocol import InvalidRequest

import pytest

from cassandra.util import Time

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (p int PRIMARY KEY, t time, lis list<time>, tup frozen<tuple<int, time>>)")
    yield table
    cql.execute("DROP TABLE " + table)

# Time values are limited to 24 hours, but it's possible to bypass that limit
# by setting the number of nanoseconds manually.
def make_invalid_time(nanoseconds):
    t = Time(0)
    t.nanosecond_time = nanoseconds
    return t

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

# According to the CQL binary protocol spec, `time` values must be between 0 and 24 hours.
# Test that Scylla rejects integer values outside of this range
# https://github.com/apache/cassandra/blob/f5df4b219e063cb24b9cc0c22b6e614506b8d903/doc/native_protocol_v4.spec#L941
# An 8 byte two's complement long representing nanoseconds since midnight.
# Valid values are in the range 0 to 86399999999999
@pytest.mark.xfail(reason="issue #14667")
def test_invalid_time_int_values(cql, table1):
    p = unique_key_int()
    insert_t = cql.prepare(f"INSERT INTO {table1} (p, t) VALUES ({p}, ?)")
    update_t = cql.prepare(f"UPDATE {table1} SET t = ? WHERE p = {p}")

    # Values within the range are accepted
    cql.execute(insert_t, (Time(0),))
    cql.execute(update_t, (Time(123456),))
    cql.execute(insert_t, (Time(86399999999999),))

    # Values outside of the range are rejected
    with pytest.raises(InvalidRequest):
        cql.execute(insert_t, (make_invalid_time(-1),))

    with pytest.raises(InvalidRequest):
        cql.execute(update_t, (make_invalid_time(-1),))

    with pytest.raises(InvalidRequest):
        cql.execute(insert_t, (make_invalid_time(86400000000000),))

    with pytest.raises(InvalidRequest):
        cql.execute(insert_t, (make_invalid_time(-12345),))

    with pytest.raises(InvalidRequest):
        int64_min = -9223372036854775808
        cql.execute(insert_t, (make_invalid_time(int64_min),))

    with pytest.raises(InvalidRequest):
        int64_max = 9223372036854775807
        cql.execute(insert_t, (make_invalid_time(int64_max),))

    # Validation also works when time value is inside of another structure
    insert_list = cql.prepare(f"INSERT INTO {table1} (p, lis) VALUES ({p}, ?)")
    insert_tuple = cql.prepare(f"INSERT INTO {table1} (p, tup) VALUES ({p}, ?)")
    update_list = cql.prepare( f"UPDATE {table1} SET lis = ? WHERE p = {p}")
    update_tuple = cql.prepare(f"UPDATE {table1} SET tup = ? WHERE p = {p}")

    # Valid values
    cql.execute(insert_list, ([Time(1234)],))
    cql.execute(insert_tuple, ((123, Time(1234),),))
    cql.execute(update_list, ([Time(1234)],))
    cql.execute(update_tuple, ((123, Time(1234),),))

    invalid_time = make_invalid_time(86400000000000999)

    # Invalid values
    with pytest.raises(InvalidRequest):
        cql.execute(insert_list, ([invalid_time],))

    with pytest.raises(InvalidRequest):
        cql.execute(insert_tuple, ((123, invalid_time,),))

    with pytest.raises(InvalidRequest):
        cql.execute(update_list, ([invalid_time],))

    with pytest.raises(InvalidRequest):
        cql.execute(update_tuple, ((123, invalid_time,),))
