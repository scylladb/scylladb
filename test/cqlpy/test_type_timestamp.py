# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests involving the "timestamp" column type.
#
# Please do not confuse the "timestamp" column type tested here, with the
# unrelated concept of the "timestamp" of a write (the "USING TIMESTAMP").
# That latter concept is tested in test_timestamp.py, not here.
#############################################################################

from util import new_test_table, unique_key_int
from datetime import datetime
from cassandra.protocol import SyntaxException, InvalidRequest
import pytest

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int primary key, t timestamp") as table:
        yield table

@pytest.fixture(scope="module")
def table2(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, t timestamp, primary key (p,c)") as table:
        yield table

# According to the Cassandra documentation, a timestamp is a 64-bit integer
# "representing a number of milliseconds since the standard base time known
# as the epoch". This underlying "number" can be stored into a timestamp
# column by writing an integer directly to it, and later the number can be
# extracted during select with the built-in tounixtimestamp() function.
#
# In issue #11588, a user who mistakenly used a microsecond count reported
# that the write was successful, but then the read failed reporting that
# "timestamp is out of range. Must be in milliseconds since epoch".
# This is wrong and unhelpful beahvior, and the following test reproduces
# it. Cassandra currently allows the out-of-range number to be both written
# and then read - so that is the Cassandra-compatible approach - but
# arguably a more correct and useful behavior would be to fail the write
# up-front, even before reaching the read.
@pytest.mark.xfail(reason="issue #11588")
def test_type_timestamp_overflow(cql, table1):
    p = unique_key_int()
    t = 1667215862 * 1000000
    # t is out of the normal range for milliseconds since the epoch (it
    # was calculated as microseconds since the epoch), but Cassandra allows
    # to write it. Scylla may decide in the future to fail this write (in
    # which case this test should be changed to accept both options, or
    # be marked scylla_only), but passing the write and failing the read
    # (as in #11588) is a bug.
    cql.execute(f"INSERT INTO {table1} (p, t) VALUES ({p}, {t})")
    assert list(cql.execute(f"SELECT tounixtimestamp(t) from {table1} where p = {p}")) == [(t,)]

# Check that setting a timestamp to a reasonable number of milliseconds
# since the epoch, we can read it back properly: We can read the same
# number back using tounixtimestamp(), and also the Python driver can
# convert the timestamp to a Python datetime object as expect.
def test_type_timestamp_select(cql, table1):
    p = unique_key_int()
    t = 1667215862123
    cql.execute(f"INSERT INTO {table1} (p, t) VALUES ({p}, {t})")
    assert list(cql.execute(f"SELECT tounixtimestamp(t) from {table1} where p = {p}")) == [(t,)]
    assert list(cql.execute(f"SELECT t from {table1} where p = {p}")) == [(datetime(2022, 10, 31, 11, 31, 2, 123000),)]

# Above we created a timestamp value from an integer. The documentation also
# says that it can be assigned a string, and that this string uses a ISO 8601
# date, with an RFC 8222 4-digit time zone specification.
# Check that the various variations of this format indeed work.
def test_type_timestamp_from_string(cql, table1):
    p = unique_key_int()
    # Some example valid timestamp formats, taken from the documentation
    # https://docs.scylladb.com/stable/cql/types.html#timestamps:
    cql.execute(f"INSERT INTO {table1} (p, t) VALUES ({p}, '2011-02-03 04:05+0000')")
    assert list(cql.execute(f"SELECT t from {table1} where p = {p}")) == [(datetime(2011, 2, 3, 4, 5, 0, 0),)]
    cql.execute(f"INSERT INTO {table1} (p, t) VALUES ({p}, '2011-02-03 04:05:12+0000')")
    assert list(cql.execute(f"SELECT t from {table1} where p = {p}")) == [(datetime(2011, 2, 3, 4, 5, 12, 0),)]
    cql.execute(f"INSERT INTO {table1} (p, t) VALUES ({p}, '2011-02-03 04:05:12.345+0000')")
    assert list(cql.execute(f"SELECT t from {table1} where p = {p}")) == [(datetime(2011, 2, 3, 4, 5, 12, 345000),)]
    cql.execute(f"INSERT INTO {table1} (p, t) VALUES ({p}, '2011-02-03T05:06:17.123+0000')")
    assert list(cql.execute(f"SELECT t from {table1} where p = {p}")) == [(datetime(2011, 2, 3, 5, 6, 17, 123000),)]
    cql.execute(f"INSERT INTO {table1} (p, t) VALUES ({p}, '2011-02-03+0000')")
    assert list(cql.execute(f"SELECT t from {table1} where p = {p}")) == [(datetime(2011, 2, 3, 0, 0, 0, 0),)]
    # Dropping the timezone string +0000 is allowed, but results in an unknown
    # timezone (the on the machine running Scylla), so we don't know how to
    # check the correctness of the result. But at least the insert should
    # succeed and not result in InvalidRequest.
    cql.execute(f"INSERT INTO {table1} (p, t) VALUES ({p}, '2011-02-03 04:05:12.345')")
    # An invalid format for the timestamp should result in InvalidRequest:
    with pytest.raises(InvalidRequest):
        cql.execute(f"INSERT INTO {table1} (p, t) VALUES ({p}, 'an invalid timestamp')")

# The timestamp column type has millisecond resolution. What happens if we
# assign a string to a timestamp column which attempts to specify a time
# with too many digits of precision? In Cassandra, the extra digits are
# silently truncated. In Scylla, it's an error with somewhat obscure wording:
#  "marshaling error: unable to parse date '2011-02-03 04:05:12.345678+0000':
#   marshaling error: Milliseconds length exceeds expected (6)"
# For now, let's accept Scylla's error handling as the better approach, so
# mark this test cassandra_bug.
def test_type_timestamp_from_string_overprecise(cql, table1, cassandra_bug):
    p = unique_key_int()
    with pytest.raises(InvalidRequest):
        cql.execute(f"INSERT INTO {table1} (p, t) VALUES ({p}, '2011-02-03 04:05:12.345678+0000')")
        assert list(cql.execute(f"SELECT t from {table1} where p = {p}")) == [(datetime(2011, 2, 3, 4, 5, 12, 345000),)]

# Check that filtering expressions of timestamps - equality and inequality
# checks - work as expected
def test_type_timestamp_comparison(cql, table2):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table2} (p, c, t) VALUES ({p}, 1, '2011-02-03 04:05:12.345+0000')")
    assert list(cql.execute(f"SELECT c from {table2} where p = {p} and t = '2011-02-03 04:05:12.345+0000' ALLOW FILTERING")) == [(1,)]
    assert list(cql.execute(f"SELECT c from {table2} where p = {p} and t = '2012-02-03 04:05:12.345+0000' ALLOW FILTERING")) == []
    assert list(cql.execute(f"SELECT c from {table2} where p = {p} and t < '2011-02-04+0000' ALLOW FILTERING")) == [(1,)]
    assert list(cql.execute(f"SELECT c from {table2} where p = {p} and t < '2011-02-03+0000' ALLOW FILTERING")) == []
    assert list(cql.execute(f"SELECT c from {table2} where p = {p} and t > '2011-02-03+0000' ALLOW FILTERING")) == [(1,)]
    assert list(cql.execute(f"SELECT c from {table2} where p = {p} and t > '2011-02-04+0000' ALLOW FILTERING")) == []
    assert list(cql.execute(f"SELECT c from {table2} where p = {p} and t <= '2011-02-03 04:05:12.345+0000' ALLOW FILTERING")) == [(1,)]
    assert list(cql.execute(f"SELECT c from {table2} where p = {p} and t < '2011-02-03 04:05:12.345+0000' ALLOW FILTERING")) == []
    assert list(cql.execute(f"SELECT c from {table2} where p = {p} and t >= '2011-02-03 04:05:12.345+0000' ALLOW FILTERING")) == [(1,)]
    assert list(cql.execute(f"SELECT c from {table2} where p = {p} and t > '2011-02-03 04:05:12.345+0000' ALLOW FILTERING")) == []

# Cassandra 4 added the feature of arithmetic between values in general, and
# timestamps in particular (to which durations can be added). Scylla used to
# be missing this feature - see #2693, #2694 and their many duplicates.
@pytest.mark.xfail(reason="issue #2693, #2694")
def test_type_timestamp_arithmetic(cql, table2):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table2} (p, c, t) VALUES ({p}, 1, '2011-02-03 04:05:12.345+0000')")
    assert list(cql.execute(f"SELECT c from {table2} where p = {p} and t > '2011-02-03 04:05:12.345+0000' - 1d ALLOW FILTERING")) == [(1,)]
    assert list(cql.execute(f"SELECT c from {table2} where p = {p} and t = '2011-02-03 05:05:12.345+0000' - 1h ALLOW FILTERING")) == [(1,)]
