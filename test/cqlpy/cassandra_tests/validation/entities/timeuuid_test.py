# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 6ca34f81386dc8f6020cdf2ea4246bca2a0896c5
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0

from cassandra_tests.porting import *

from cassandra.protocol import SyntaxException
from cassandra.util import datetime_from_uuid1
from datetime import timezone

# Migrated from cql_tests.py:TestCQL.timeuuid_test()
def testTimeuuid(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, t timeuuid, PRIMARY KEY(k, t))") as table:
        assert_invalid_throw(cql, table, SyntaxException, "INSERT INTO %s (k, t) VALUES (0, 2012-11-07 18:18:22-0800)")

        for i in range(4):
            execute(cql, table, "INSERT INTO %s (k, t) VALUES (0, now())")

        rows = list(execute(cql, table, "SELECT * FROM %s"))
        assert 4 == len(rows)

        assert_row_count(execute(cql, table, "SELECT * FROM %s WHERE k = 0 AND t >= ?", rows[0][1]), 4)

        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE k = 0 AND t < ?", rows[0][1]))

        assert_row_count(execute(cql, table, "SELECT * FROM %s WHERE k = 0 AND t > ? AND t <= ?", rows[0][1], rows[2][1]), 2)

        assert_row_count(execute(cql, table, "SELECT * FROM %s WHERE k = 0 AND t = ?", rows[0][1]), 1)

        assert_invalid(cql, table, "SELECT dateOf(k) FROM %s WHERE k = 0 AND t = ?", rows[0][1])

        for i in range(4):
            uuid = rows[i][1]
            datetime = datetime_from_uuid1(uuid)
            # Before comparing this datetime to the result of dateOf(), we
            # must truncate the resolution of datetime to milliseconds.
            # he problem is that the dateOf(timeuuid) CQL function converts a
            # timeuuid to CQL's "timestamp" type, which has millisecond
            # resolution, but datetime *may* have finer resolution. It will
            # usually be whole milliseconds, because this is what the now()
            # implementation usually does, but when now() is called more than
            # once per millisecond, it *may* start incrementing the sub-
            # millisecond part.
            datetime = datetime.replace(microsecond=datetime.microsecond//1000*1000)
            timestamp = round(datetime.replace(tzinfo=timezone.utc).timestamp() * 1000)
            assert_rows(execute(cql, table, "SELECT dateOf(t), unixTimestampOf(t) FROM %s WHERE k = 0 AND t = ?", rows[i][1]),
                       [datetime, timestamp])

        assert_empty(execute(cql, table, "SELECT t FROM %s WHERE k = 0 AND t > maxTimeuuid(1234567) AND t < minTimeuuid('2012-11-07 18:18:22-0800')"))

# Test for 5386,
# migrated from cql_tests.py:TestCQL.function_and_reverse_type_test()
def testDescClusteringOnTimeuuid(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, c timeuuid, v int, PRIMARY KEY (k, c)) WITH CLUSTERING ORDER BY (c DESC)") as table:
        execute(cql, table, "INSERT INTO %s (k, c, v) VALUES (0, now(), 0)")
