# -*- coding: utf-8 -*-
# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#############################################################################
# Test involving the "duration" column type.
#############################################################################

from .util import unique_name, unique_key_int

import pytest

from cassandra.util import Duration
from cassandra.protocol import SyntaxException, InvalidRequest

# The "duration" type is composed of three separate integers, counting
# months, days and nanoseconds. Other units are composed from these basic
# units - e.g., a year is 12 months, and a week is 7 days. Composing units
# like seconds, minutes and hours, from nanoseconds is long and ugly, so
# the following shortcuts are useful for many tests below:
s = 1000000000  # nanoseconds per second
m = 60 * s
h = 60 * m

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
        'h': Duration(0, 0, 1*h),
        'm': Duration(0, 0, 1*m),
        's': Duration(0, 0, 1*s),
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

# Test that various combinations of units (e.g., 1d12h30m) work
def test_type_duration_combine_units(cql, table1):
    # Map of example duration strings and their expected meaning.
    examples = {
        '1d12h30m': Duration(0, 1, 12*h + 30*m),
        '1y2mo': Duration(14, 0, 0),
        # A negative duration - with a prefix "-", is supported. We'll have
        # a separate test below showing you can't put the "-" anywhere else.
        '-1y2mo': Duration(-14, 0, 0),
        '-1y2h': Duration(-12, 0, -2*h),
        # Unit names are case insensitive
        '1Y2mO': Duration(14, 0, 0),
        '2d10h': Duration(0, 2, 10*h),
        # Units of months, days and nanoseconds are inherently separate and
        # do not "overflow" into larger units: 40 days is 40 days, not one
        # month and 10 (or 9 or 12?) days. 30 hours is 30 hours, not
        # a day and 6 hours. The thinking behind this is that you don't know
        # how many days make a month (it can be 28, 30 or 31), and don't even
        # know how many hours make a day - when daylight saving time starts
        # or stops in a certain day.
        '40d': Duration(0, 40, 0),
        '30h': Duration(0, 0, 30*h),
        # The "human readable" format allows to mix weeks and other units.
        # We'll see below that the ISO 8601 format doesn't.
        '2y3w': Duration(24, 21, 0),
    }
    p = unique_key_int()
    for (string, duration) in examples.items():
        cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, {string})")
        assert list(cql.execute(f"SELECT d FROM {table1} where p = {p}")) == [(duration,)]

# We tested above that the duration "-1y2mo" is supported, meaning a negative
# duration of one year and two months (i.e., a negative 14 months). In this
# test we see that you can't put the minus sign anywhere else - "1y-2mo" is
# NOT supported, and does NOT mean 10 months.
def test_type_duration_negative_part(cql, table1):
    p = unique_key_int()
    with pytest.raises(SyntaxException, match="-2mo"):
        cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, 1y-2mo)")

# While 1y means "one year", you can't just write "y" without a multiplier.
# In math or physics lingo, "y" is a dimension, not a unit vector.
def test_type_duration_missing_multiplier(cql, table1):
    p = unique_key_int()
    with pytest.raises(SyntaxException):
        cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, y)")

# Only integer multipliers of each units is allowed. If you want half a year,
# you'll need to ask for six months, not half a year.
def test_type_duration_integer_only(cql, table1):
    p = unique_key_int()
    with pytest.raises(SyntaxException):
        cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, 0.5y)")
    with pytest.raises(SyntaxException):
        cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, 2m12.5s)")

# You cannot specify the same unit twice: 2m3m is not allowed for 5m.
# The error here is InvalidRequest, not SyntaxException as more obvious
# format errors in the duration.
def test_type_duration_twice_same_unit(cql, table1):
    p = unique_key_int()
    with pytest.raises(InvalidRequest, match='multiple times'):
        cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, 2m3m)")

# The order of the size of the units should be decreasing - 12s3m is not
# allowed.
def test_type_duration_wrong_unit_order(cql, table1):
    p = unique_key_int()
    with pytest.raises(InvalidRequest, match='The seconds should be after minutes'):
        cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, 12s3m)")
    with pytest.raises(InvalidRequest, match='The minutes should be after days'):
        cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, 1y3m4d)")

# Now tests for the ISO 8601 format, starting with the letter "P":
# There are two variants of this format:
# 1. P[n]Y[n]M[n]DT[n]H[n]M[n]S or P[n]W
# 2. P[YYYY]-[MM]-[DD]T[hh]:[mm]:[ss]

# Test the "regular" ISO 8601 format,
# P[n]Y[n]M[n]DT[n]H[n]M[n]S or P[n]W
def test_type_duration_iso_8601(cql, table1):
    examples = {
        'P1Y2D': Duration(12, 2, 0),
        'P1Y2M': Duration(14, 0, 0),
        'P2W': Duration(0, 14, 0),
        'P1YT2H': Duration(12, 0, 2*h),
        '-P1YT2H': Duration(-12, 0, -2*h),
        'P2D': Duration(0, 2, 0),
        'PT30H': Duration(0, 0, 30*h),
        'PT30H20M': Duration(0, 0, 30*h + 20*m),
        'PT20M': Duration(0, 0, 20*m),
        'PT56S': Duration(0, 0, 56*s),
        'P1Y3MT2H10M': Duration(15, 0, 130*m),
    }
    p = unique_key_int()
    for (string, duration) in examples.items():
        cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, {string})")
        assert list(cql.execute(f"SELECT d FROM {table1} where p = {p}")) == [(duration,)]

# The ISO 8601 format can specify either only weeks, or no weeks. It cannot
# mix weeks and other units: We saw above that '2y3w' is a valid human-format
# duration, but here we see it's not accepted in ISO 8601 format:
def test_type_duration_iso_8601_mix_w(cql, table1):
    p = unique_key_int()
    with pytest.raises(SyntaxException):
        cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, P2Y3W)")

# Test the "alternative" ISO 8601 format,
# P[YYYY]-[MM]-[DD]T[hh]:[mm]:[ss]
def test_type_duration_iso_8601_alternative(cql, table1):
    examples = {
        'P0001-00-02T00:00:00': Duration(12, 2, 0),
        'P0001-02-00T00:00:00': Duration(14, 0, 0),
        'P0001-00-00T02:00:00': Duration(12, 0, 2*h),
        '-P0001-02-00T00:00:00': Duration(-14, 0, 0),
        'P0000-00-02T00:00:00': Duration(0, 2, 0),
        'P0000-00-00T30:00:00': Duration(0, 0, 30*h),
        'P0000-00-00T30:20:00': Duration(0, 0, 30*h + 20*m),
        'P0000-00-00T00:20:00': Duration(0, 0, 20*m),
        'P0000-00-00T00:00:56': Duration(0, 0, 56*s),
        'P0001-03-00T02:10:00': Duration(15, 0, 130*m),
    }
    p = unique_key_int()
    for (string, duration) in examples.items():
        cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, {string})")
        assert list(cql.execute(f"SELECT d FROM {table1} where p = {p}")) == [(duration,)]

# In the ISO 8601 alternative format, you cannot just drop parts like the "T"
def test_type_duration_iso_8601_alternative_missing_t(cql, table1):
    p = unique_key_int()
    with pytest.raises(SyntaxException):
        cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, P0002-00-20)")

# Unlike the "human readable" formats, the ISO 8601 formats are case
# sensitive, and lowercase letters won't work:
def test_type_duration_iso_8601_case_sensitive(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, P1Y2D)") # works
    with pytest.raises(SyntaxException):
        cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, P1y2D)")
    with pytest.raises(SyntaxException):
        cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, p1Y2D)")
    with pytest.raises(SyntaxException):
        cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, P0001-03-00t02:10:00)")
    with pytest.raises(SyntaxException):
        cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, p0001-03-00T02:10:00)")
