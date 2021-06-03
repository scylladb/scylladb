# Copyright 2021-present ScyllaDB
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
# Test involving the "time" column type.
#############################################################################

from util import unique_name

import pytest
import random

from cassandra.util import Time

@pytest.fixture(scope="session")
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
    p = random.randint(1,1000000000)
    cql.execute(f"INSERT INTO {table1} (p, t) VALUES ({p}, 123)")
    assert list(cql.execute(f"SELECT t from {table1} where p = {p}")) == [(Time(123),)]

def test_type_time_from_int_prepared(cql, table1):
    p = random.randint(1,1000000000)
    stmt = cql.prepare(f"INSERT INTO {table1} (p, t) VALUES (?, ?)")
    cql.execute(stmt, [p, 123])
    assert list(cql.execute(f"SELECT t from {table1} where p = {p}")) == [(Time(123),)]

def test_type_time_from_string_unprepared(cql, table1):
    p = random.randint(1,1000000000)
    for t in ["08:12:54", "08:12:54.123", "08:12:54.123456", "08:12:54.123456789"]:
        cql.execute(f"INSERT INTO {table1} (p, t) VALUES ({p}, '{t}')")
        assert list(cql.execute(f"SELECT t from {table1} where p = {p}")) == [(Time(t),)]

def test_type_time_from_string_prepared(cql, table1):
    p = random.randint(1,1000000000)
    stmt = cql.prepare(f"INSERT INTO {table1} (p, t) VALUES (?, ?)")
    for t in ["08:12:54", "08:12:54.123", "08:12:54.123456", "08:12:54.123456789"]:
        cql.execute(stmt, [p, t])
        assert list(cql.execute(f"SELECT t from {table1} where p = {p}")) == [(Time(t),)]
