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
import pytest

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int primary key, t timestamp") as table:
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
