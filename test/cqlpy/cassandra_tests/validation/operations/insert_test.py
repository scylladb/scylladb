# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit a87055d56a33a9b17606f14535f48eb461965b82
#
# The original Apache Cassandra license:
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from cassandra_tests.porting import *

def testInsertWithUnset(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, s text, i int)") as table:
        # insert using nulls
        execute(cql, table, "INSERT INTO %s (k, s, i) VALUES (10, ?, ?)", "text", 10)
        execute(cql, table, "INSERT INTO %s (k, s, i) VALUES (10, ?, ?)", null, null)
        assertRows(execute(cql, table, "SELECT s, i FROM %s WHERE k = 10"),
                   row(null, null) # sending null deletes the data
        )
        # insert using UNSET
        execute(cql, table, "INSERT INTO %s (k, s, i) VALUES (11, ?, ?)", "text", 10)
        execute(cql, table, "INSERT INTO %s (k, s, i) VALUES (11, ?, ?)", unset(), unset())
        assertRows(execute(cql, table, "SELECT s, i FROM %s WHERE k=11"),
                   row("text", 10) # unset columns does not delete the existing data
        )

        # The Python driver doesn't allow unset() as partition key (needed
        # for selecting the coordinator), so we can't test this:
        #assertInvalidMessage(cql, table, "Invalid unset value for column k", "UPDATE %s SET i = 0 WHERE k = ?", unset())
        #assertInvalidMessage(cql, table, "Invalid unset value for column k", "DELETE FROM %s WHERE k = ?", unset())
        # Scylla and Cassandra have slightly different messages here. Cassandra
        # has "Invalid unset value for argument in call to function blobasint"
        # Scylla has "Invalid null or unset value for argument to
        # system.blobasint : (blob) -> int"
        assertInvalidMessageRE(cql, table, "unset", "SELECT * FROM %s WHERE k = blobAsInt(?)", unset())

# Both Scylla and Cassandra define MAX_TTL or max_ttl with the same formula,
# 20 years in seconds. In both systems, it is not configurable.
MAX_TTL = 20 * 365 * 24 * 60 * 60

# Reproduces #12243:
@pytest.mark.xfail(reason="Issue #12243")
def testInsertWithTtl(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, v int)") as table:
        # test with unset
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (1, 1) USING TTL ?", unset()); # treat as 'unlimited'
        assertRows(execute(cql, table, "SELECT ttl(v) FROM %s"), row(null))

        # test with null
        # Reproduces #12243:
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?) USING TTL ?", 1, 1, null)
        assertRows(execute(cql, table, "SELECT k, v, TTL(v) FROM %s"), row(1, 1, null))

        # test error handling
        assertInvalidMessage(cql, table, "TTL must be greater or equal to 0",
                             "INSERT INTO %s (k, v) VALUES (?, ?) USING TTL ?", 1, 1, -5)

        assertInvalidMessage(cql, table, "ttl is too large.",
                             "INSERT INTO %s (k, v) VALUES (?, ?) USING TTL ?", 1, 1, MAX_TTL + 1)

@pytest.mark.parametrize("forceFlush", [False, True])
def testInsert(cql, test_keyspace, forceFlush):
    with create_table(cql, test_keyspace, "(partitionKey int, clustering int, value int, PRIMARY KEY (partitionKey, clustering))") as table:
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering) VALUES (0, 0)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering, value) VALUES (0, 1, 1)")
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row(0, 0, null),
                   row(0, 1, 1))

        # Missing primary key columns
        assertInvalidMessageRE(cql, table, "[Mm]issing.*partitionkey",
                             "INSERT INTO %s (clustering, value) VALUES (0, 1)")
        assertInvalidMessageRE(cql, table, "[Mm]issing.*clustering",
                             "INSERT INTO %s (partitionKey, value) VALUES (0, 2)")

        # multiple time the same value
        assertInvalidMessageRE(cql, table, "Multiple|duplicates",
                             "INSERT INTO %s (partitionKey, clustering, value, value) VALUES (0, 0, 2, 2)")

        # multiple time same primary key element in WHERE clause
        assertInvalidMessageRE(cql, table, "Multiple|duplicates",
                             "INSERT INTO %s (partitionKey, clustering, clustering, value) VALUES (0, 0, 0, 2)")

        # unknown identifiers
        assertInvalidMessageRE(cql, table, "(Undefined|Unknown).*clusteringx",
                             "INSERT INTO %s (partitionKey, clusteringx, value) VALUES (0, 0, 2)")

        assertInvalidMessageRE(cql, table, "(Undefined|Unknown).*valuex",
                             "INSERT INTO %s (partitionKey, clustering, valuex) VALUES (0, 0, 2)")

@pytest.mark.parametrize("forceFlush", [False, True])
def testInsertWithTwoClusteringColumns(cql, test_keyspace, forceFlush):
    with create_table(cql, test_keyspace, "(partitionKey int, clustering_1 int, clustering_2 int, value int, PRIMARY KEY (partitionKey, clustering_1, clustering_2))") as table:
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, clustering_2) VALUES (0, 0, 0)")
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 0, 1, 1)")
        if forceFlush:
            flush(cql, table)

        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row(0, 0, 0, null),
                   row(0, 0, 1, 1))

        # Missing primary key columns
        assertInvalidMessageRE(cql, table, "[Mm]issing.*partitionkey",
                             "INSERT INTO %s (clustering_1, clustering_2, value) VALUES (0, 0, 1)")
        assertInvalidMessageRE(cql, table, "clustering_1",
                             "INSERT INTO %s (partitionKey, clustering_2, value) VALUES (0, 0, 2)")

        # multiple time the same value
        assertInvalidMessageRE(cql, table, "Multiple|duplicates",
                             "INSERT INTO %s (partitionKey, clustering_1, value, clustering_2, value) VALUES (0, 0, 2, 0, 2)")

        # multiple time same primary key element in WHERE clause
        assertInvalidMessageRE(cql, table, "Multiple|duplicates",
                             "INSERT INTO %s (partitionKey, clustering_1, clustering_1, clustering_2, value) VALUES (0, 0, 0, 0, 2)")

        # unknown identifiers
        assertInvalidMessageRE(cql, table, "(Undefined|Unknown).*clustering_1x",
                             "INSERT INTO %s (partitionKey, clustering_1x, clustering_2, value) VALUES (0, 0, 0, 2)")

        assertInvalidMessageRE(cql, table, "(Undefined|Unknown).*valuex",
                             "INSERT INTO %s (partitionKey, clustering_1, clustering_2, valuex) VALUES (0, 0, 0, 2)")

@pytest.mark.parametrize("forceFlush", [False, True])
def testInsertWithAStaticColumn(cql, test_keyspace, forceFlush):
    with create_table(cql, test_keyspace, "(partitionKey int, clustering_1 int, clustering_2 int, value int, staticValue text static, PRIMARY KEY (partitionKey, clustering_1, clustering_2))") as table:
        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, clustering_2, staticValue) VALUES (0, 0, 0, 'A')")
        execute(cql, table, "INSERT INTO %s (partitionKey, staticValue) VALUES (1, 'B')")
        if forceFlush:
            flush(cql, table)

        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row(1, null, null, "B", null),
                   row(0, 0, 0, "A", null))

        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (1, 0, 0, 0)")
        if forceFlush:
            flush(cql, table)
        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row(1, 0, 0, "B", 0),
                   row(0, 0, 0, "A", null))

        # Missing primary key columns
        assertInvalidMessageRE(cql, table, "[Mm]issing.*partitionkey",
                             "INSERT INTO %s (clustering_1, clustering_2, staticValue) VALUES (0, 0, 'A')")
        assertInvalidMessageRE(cql, table, "clustering_1",
                             "INSERT INTO %s (partitionKey, clustering_2, staticValue) VALUES (0, 0, 'A')")

# Reproduces #6447 and #12243:
@pytest.mark.xfail(reason="Issue #12243")
def testInsertWithDefaultTtl(cql, test_keyspace):
    secondsPerMinute = 60
    with create_table(cql, test_keyspace, f"(a int PRIMARY KEY, b int) WITH default_time_to_live = {10*secondsPerMinute}") as table:
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (1, 1)")
        results = list(execute(cql, table, "SELECT ttl(b) FROM %s WHERE a = 1"))
        assert len(results) == 1
        assert getattr(results[0], 'ttl_b') >= 9 * secondsPerMinute

        execute(cql, table, "INSERT INTO %s (a, b) VALUES (2, 2) USING TTL ?", (5 * secondsPerMinute))
        results = list(execute(cql, table, "SELECT ttl(b) FROM %s WHERE a = 2"))
        assert len(results) == 1
        assert getattr(results[0], 'ttl_b') <= 5 * secondsPerMinute

        # Reproduces #6447:
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (3, 3) USING TTL ?", 0)
        assertRows(execute(cql, table, "SELECT ttl(b) FROM %s WHERE a = 3"), row(null))

        execute(cql, table, "INSERT INTO %s (a, b) VALUES (4, 4) USING TTL ?", unset())
        results = list(execute(cql, table, "SELECT ttl(b) FROM %s WHERE a = 4"))
        assert len(results) == 1
        assert getattr(results[0], 'ttl_b') >= 9 * secondsPerMinute

        # Reproduces #12243:
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?) USING TTL ?", 4, 4, null)
        assertRows(execute(cql, table, "SELECT ttl(b) FROM %s WHERE a = 4"), row(null))


TOO_BIG = 1024 * 65

# Reproduces #12247:
@pytest.mark.xfail(reason="Issue #12247")
def testPKInsertWithValueOver64K(cql, test_keyspace):
    with create_table(cql, test_keyspace, f"(a text, b text, PRIMARY KEY (a, b))") as table:
        assertInvalidThrow(cql, table, InvalidRequest,
                           "INSERT INTO %s (a, b) VALUES (?, 'foo')", 'x'*TOO_BIG)

# Reproduces #12247:
@pytest.mark.xfail(reason="Issue #12247")
def testCKInsertWithValueOver64K(cql, test_keyspace):
    with create_table(cql, test_keyspace, f"(a text, b text, PRIMARY KEY (a, b))") as table:
        assertInvalidThrow(cql, table, InvalidRequest,
                           "INSERT INTO %s (a, b) VALUES ('foo', ?)", 'x'*TOO_BIG)
