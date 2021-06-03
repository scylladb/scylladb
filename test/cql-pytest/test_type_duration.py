# -*- coding: utf-8 -*-
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
# Test involving the "duration" column type.
#############################################################################

from util import unique_name

import pytest
import random

from cassandra.util import Duration

@pytest.fixture(scope="session")
def table1(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (p int PRIMARY KEY, d duration)")
    yield table
    cql.execute("DROP TABLE " + table)

# The Cassandra documentation
# https://cassandra.apache.org/doc/latest/cql/types.html#durations
# or our copy 
# https://docs.scylladb.com/getting-started/types/#working-with-durations
# Specify three ways in which a duration can be input in CQL syntax:
# a human-readable combination of units (e.g., 12h30m17us), or two ISO 8601
# formats. Let's begin by testing the human-readable format:

# Test each of the units which are supposed to be allowed, separately -
# y, mo, w, d, h, m, s, ms, us *or* µs, ns:
# Reproduces issue #8001
@pytest.mark.xfail(reason="issue #8001")
def test_type_duration_human_readable_input_units(cql, table1):
    # Map of allowed units and their expected meaning.
    units = {
        'y': Duration(12, 0, 0),
        'mo': Duration(1, 0, 0),
        'w': Duration(0, 7, 0),
        'd': Duration(0, 1, 0),
        'h': Duration(0, 0, 3600000000000),
        'm': Duration(0, 0, 60000000000),
        's': Duration(0, 0, 1000000000),
        'ms': Duration(0, 0, 1000000),
        'us': Duration(0, 0, 1000),
        'ns': Duration(0, 0, 1),
        # An alias for "us" which should be supported, but wasn't (issue #8001)
        'µs': Duration(0, 0, 1000),
    }
    p = random.randint(1,1000000000)
    for (unit, duration) in units.items():
        print(unit)
        cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, 1{unit})")
        assert list(cql.execute(f"SELECT d FROM {table1} where p = {p}")) == [(duration,)]
