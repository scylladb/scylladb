# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Test involving the "date" column type.
#############################################################################

from util import unique_name, unique_key_int
from cassandra.util import Date
from cassandra.protocol import InvalidRequest

import pytest

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (p int PRIMARY KEY, d date)")
    yield table
    cql.execute("DROP TABLE " + table)

# According to the Cassandra documentation, a date "can be input either as an
# integer or using a date string."
# The documentation further explains the (surprising) encoding of that
# integer:
#    "Values of the date type are encoded as 32-bit unsigned integers
#     representing a number of days with “the epoch” at the center of the
#     range (2^31). Epoch is January 1st, 1970"
# So the number "123" doesn't mean 123 days since the epoch - it actually
# means 2^31-123 days before the epoch.
# The date string should be yyyy-mm-dd, and nothing else is allowed.
# The following tests verify that these different initialization options
# actually work.
def test_type_date_from_int_unprepared(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, 123)")
    ds = list(cql.execute(f"SELECT d from {table1} where p = {p}"))
    assert len(ds) == 1
    # The Python driver returns a "date" type as a cassandra.util.Date
    # type, and when that is converted into a number, it strangely results
    # in the number of days since the epoch, where the epoch is 0.
    # So 123 is converted into "-2^31+123" days (a negative number) since
    # the epoch.
    assert ds[0].d == -2**31 + 123

def test_type_date_from_int_prepared(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"INSERT INTO {table1} (p, d) VALUES (?, ?)")
    cql.execute(stmt, [p, 123])
    ds = list(cql.execute(f"SELECT d from {table1} where p = {p}"))
    assert len(ds) == 1
    assert ds[0].d == -2**31 + 123

# As explained above, the integer used to create the date must be a
# a 32-bit unsigned integer. An attempt to pass a negative number, or
# one that doesn't fit 32 bits, is an error. Cassandra says:
# "Unable to make unsigned int (for date) from: '-123'".
# Reproduces #17066.
def test_type_date_from_int_unprepared_underflow(cql, table1):
    p = unique_key_int()
    with pytest.raises(InvalidRequest, match="unsigned int"):
        cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, -123)")

def test_type_date_from_int_unprepared_overflow(cql, table1):
    p = unique_key_int()
    for val in ["50000000000", "50000000000000000000"]:
        with pytest.raises(InvalidRequest, match="unsigned int"):
            cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, {val})")

def test_type_date_from_string_unprepared(cql, table1):
    p = unique_key_int()
    for d in ["2024-01-30"]:
        cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, '{d}')")
        assert list(cql.execute(f"SELECT d from {table1} where p = {p}")) == [(Date(d),)]

# Although the documentation suggests that yyyy-mm-dd is the only *string*
# format allowed for creating a date, it is actually allowed to also use
# a number string, which is treated just like the integer case above.
# This can only be tested for the *unprepared* case - in the prepared-
# statement case, the Python driver needs to build the integer to pass
# to Scylla itself, and whatever it supports or doesn't support (in this
# case, it doesn't) is its own fault - not Scylla's.
def test_type_date_from_string_number_unprepared(cql, table1):
    p = unique_key_int()
    # Note the string '123', not integer 123
    cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, '123')")
    ds = list(cql.execute(f"SELECT d from {table1} where p = {p}"))
    assert len(ds) == 1
    assert ds[0].d == -2**31 + 123
