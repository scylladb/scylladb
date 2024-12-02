# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

###############################################################################
# Tests for various native (built-in) scalar functions that can be used in
# various SELECT, INSERT or UPDATE requests. Note we also have tests for
# some of these functions in many other test files. For example, the tests
# for the cast() function are in test_cast_data.py.
###############################################################################

import pytest
from util import new_test_table, unique_key_int
from datetime import datetime
from cassandra.protocol import InvalidRequest

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, i int, g bigint, b blob, s text, t timestamp, u timeuuid, d date, PRIMARY KEY (p)") as table:
        yield table

# Check that a function that can take a column name as a parameter, can also
# take a constant. This feature is barely useful for WHERE clauses, and
# even less useful for selectors, but should be allowed for both.
# Reproduces #12607.
@pytest.mark.xfail(reason="issue #12607")
def test_constant_function_parameter(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, b) VALUES ({p}, 0x03)")
    assert [(p,)] == list(cql.execute(f"SELECT p FROM {table1} WHERE p={p} AND b=tinyintAsBlob(3) ALLOW FILTERING"))
    assert [(b'\x04',)] == list(cql.execute(f"SELECT tinyintAsBlob(4) FROM {table1} WHERE p={p}"))

# According to the documentation, "The `minTimeuuid` function takes a
# `timestamp` value t, either a timestamp or a date string.". But although
# both cases are supported with constant parameters in WHERE restrictions,
# in a *selector* (the first part of the SELECT, saying what to select), it
# turns out that ONLY a timestamp column is allowed. Although this is
# undocumented behavior, both Cassandra and Scylla share it so we deem it
# correct.
def test_selector_mintimeuuid(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, s, t, i) VALUES ({p}, '2013-02-02 10:00+0000', 123, 456)")
    # We just check this works, not what the value is:
    cql.execute(f"SELECT mintimeuuid(t) FROM {table1} WHERE p={p}")
    # This doesn't work - despite the documentation, in a selector a
    # date string is not supported by mintimeuuid.
    with pytest.raises(InvalidRequest, match='of type timestamp'):
        cql.execute(f"SELECT mintimeuuid(s) FROM {table1} WHERE p={p}")
    # Other integer types also don't work, it must be a timestamp:
    with pytest.raises(InvalidRequest, match='of type timestamp'):
        cql.execute(f"SELECT mintimeuuid(i) FROM {table1} WHERE p={p}")

# Cassandra allows the implicit (and wrong!) casting of a bigint returned
# by writetime() to the timestamp type required by mintimeuuid(). Scylla
# doesn't. I'm not sure which behavior we should consider correct, but it's
# useful to have a test that demonstrates this incompatibility.
# Reproduces #14319.
@pytest.mark.xfail(reason="issue #14319")
def test_selector_mintimeuuid_64bit(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, g) VALUES ({p}, 123)")
    cql.execute(f"SELECT mintimeuuid(g) FROM {table1} WHERE p={p}")
    cql.execute(f"SELECT mintimeuuid(writetime(g)) FROM {table1} WHERE p={p}")

# blobasbigint() must insist to receive a properly-sized (8-byte) blob.
# If it accepts a shorter blob (e.g., 4 bytes) and returns that to the driver,
# it will confuse the driver (the driver will expect to read 8 bytes for the
# bigint but will get only 4).
def test_blobas_wrong_size(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, i) VALUES ({p}, 123)")
    # Cassandra and Scylla print: "In call to function system.blobasbigint,
    # value 0x0000007b is not a valid binary representation for type bigint".
    with pytest.raises(InvalidRequest, match='blobasbigint'):
        cql.execute(f"SELECT blobasbigint(intasblob(i)) FROM {table1} WHERE p={p}")

# The mintimeuuid() function works with valid "timestamp"-type values, which
# are 64-bit *signed* integers representing milliseconds since the epoch.
# In particular, negative timestamps are allowed and should not be rejected.
# The function tounixtimestamp can be used to extract the timestamp back from
# the timeuuid. In this test we use "reasonably"-low timestamps (referring
# to dates in this and the previous century - not to the age of the
# dinosaurs). The test that follows will look at extreme, not-useful-in-
# practice, timestamp values.
@pytest.mark.parametrize("timestamp", [-1706638779000, -123, 123, 1706638779000])
def test_mintimeuuid_reasonable(cql, table1, timestamp):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, t) VALUES ({p}, {timestamp})")
    assert [(timestamp,)] == list(cql.execute(f"SELECT tounixtimestamp(mintimeuuid(t)) FROM {table1} WHERE p={p}"))

# The "timestamp" column type stores a 64-bit number of milliseconds, but
# Scylla's and Cassandra's implementation of timeuuids cannot store this
# entire range. This test expects mintimeuuid() to either return the *right*
# thing (converted back to the original timestamp via tounixtimestamp),
# or to throw an exception. Crashing is not acceptable (reproduces #17035),
# and so is returning a value, but it having the wrong timestamp (this
# happens in Cassandra, so this test is marked a cassandra_bug)
@pytest.mark.parametrize("timestamp", [-2**63+123, -2**62, -2**58, 2**62, 2**63-123])
def test_mintimeuuid_extreme(cql, table1, timestamp, cassandra_bug):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, t) VALUES ({p}, {timestamp})")
    try:
        res = list(cql.execute(f"SELECT tounixtimestamp(mintimeuuid(t)) FROM {table1} WHERE p={p}"))
    except:
        # If mintimeuuid() refuses these extreme values, it's fine.
        # The real bug is to return something, which is wrong (the
        # assert below), or to crash the server (issue #17035).
        pass
    else:
        assert [(timestamp,)] == res

# An example of an extreme negative timestamp is what totimestamp(123)
# returns: As shown in test_type_date.py, the number "123" is converted to
# a date very long in the past (2**31 is the day of the epoch), and results
# in a very negative timestamp.
# We don't expect much from this test - it may report strange errors (this
# happens in both Cassandra and Scylla), but it mustn't crash as it did in
# issue #17035.
def test_mintimeuuid_extreme_from_totimestamp(cql, table1):
    p = unique_key_int()
    try:
        cql.execute(f"SELECT * FROM {table1} WHERE p={p} and u < mintimeuuid(totimestamp(123)) ALLOW FILTERING")
    except:
        pass

# According to the documentation, the toTimestamp() function can take either
# a "timeuuid" or a "date" value and convert it to a "timestamp" type
# (64-bit signed integer representing number of milliseconds since the UNIX
# epoch). Let's test these conversions, and their error cases (especially,
# the range dates covered by the different types isn't identical).
# In test_type_date.py we have test coverage on the different ways a "date"
# value can initialized, so we don't need to cover all of these here.
def test_totimestamp_date(cql, table1):
    p = unique_key_int()
    # The date 2**31 is the day of the epoch, so 2**31+1 is one day later,
    # Midnight January 2nd, 1970:
    cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, {2**31+1})")
    assert [(datetime(1970,1,2,0,0),)] == list(cql.execute(f"SELECT totimestamp(d) FROM {table1} WHERE p={p}"))
    # The day 2**31-1 is one day before the epoch, and this (negative
    # timestamps) is allowed:
    cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, {2**31-1})")
    assert [(datetime(1969,12,31,0,0),)] == list(cql.execute(f"SELECT totimestamp(d) FROM {table1} WHERE p={p}"))

# Same as above test test_totimestame_date(), but use extreme dates
# millions of years in the past. These tests cannot be run with the
# current Python driver because of bugs it has in converting extreme
# timestamps - https://github.com/scylladb/python-driver/issues/255 -
# so the test is skipped.
@pytest.mark.skip(reason="Python driver bug")
def test_totimestamp_date_extreme(cql, table1):
    p = unique_key_int()
    # The day 2**31-2**29 is 2**29 days before the epoch - it's a useless date
    # (more than a million years before our time), but at 4e16 milliseconds
    # before the epoch, it should still fit nicely into 63 bits (9e18
    # milliseconds), and should work fine.
    cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, {2**31-2**29})")
    cql.execute(f"SELECT totimestamp(d) FROM {table1} WHERE p={p}")
    # The day 2**30 is 2**30 days before the epoch - it's a useless date
    # (almost 3 million years before our time), but at 10^17 milliseconds
    # before the epoch, it should still fit 63 bits (10^19 milliseconds)
    # and work fine.
    cql.execute(f"INSERT INTO {table1} (p, d) VALUES ({p}, {2**30})")
    cql.execute(f"SELECT totimestamp(d) FROM {table1} WHERE p={p}")
