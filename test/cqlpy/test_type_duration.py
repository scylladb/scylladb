# -*- coding: utf-8 -*-
# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Test involving the "duration" column type.
#############################################################################

from util import unique_name, unique_key_int

import pytest

from cassandra.util import Duration

@pytest.fixture(scope="module")
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
    p = unique_key_int()
    for (unit, duration) in units.items():
        print(unit)
        cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, 1{unit})")
        assert list(cql.execute(f"SELECT d FROM {table1} where p = {p}")) == [(duration,)]
