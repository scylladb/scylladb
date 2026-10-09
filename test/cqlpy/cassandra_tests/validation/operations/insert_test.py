# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
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
#
# Modifications: Copyright 2022-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

from ...porting import *
from cassandra.query import BoundStatement
from cassandra.util import Duration
import struct

def testInsertZeroDuration(cql, test_keyspace):
    expectedDuration = Duration(0, 0, 0)
    with create_table(cql, test_keyspace, "(a INT PRIMARY KEY, b DURATION);") as table:
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (1, P0Y)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (2, P0M)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (3, P0W)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (4, P0D)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (5, P0Y0M0D)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (6, PT0H)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (7, PT0M)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (8, PT0S)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (9, PT0H0M0S)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (10, P0YT0H)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (11, P0MT0M)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (12, P0DT0S)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (13, P0M0DT0H0S)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (14, P0Y0M0DT0H0M0S)")
        # The Python driver's Duration objects aren't hashable, so we can't
        # use assertRowsIgnoringOrder(), and sort the rows ourselves.
        assertRows(sorted(execute(cql, table, "SELECT * FROM %s"), key=lambda r: r.a),
                                row(1, expectedDuration),
                                row(2, expectedDuration),
                                row(3, expectedDuration),
                                row(4, expectedDuration),
                                row(5, expectedDuration),
                                row(6, expectedDuration),
                                row(7, expectedDuration),
                                row(8, expectedDuration),
                                row(9, expectedDuration),
                                row(10, expectedDuration),
                                row(11, expectedDuration),
                                row(12, expectedDuration),
                                row(13, expectedDuration),
                                row(14, expectedDuration))

# The end of the Java testInsertZeroDuration checks that a bare "P", without
# any designator, is not a valid duration literal. We split it into a
# separate test, so that the rest of testInsertZeroDuration keeps running on
# Scylla. The Java test calls assertInvalid() with the expected error message
# as its first argument, but assertInvalid()'s first argument is the query -
# so the Java test runs the error message as a query, which of course fails,
# and never runs the INSERT it meant to check. We check what the test
# evidently intended: that this INSERT is a syntax error. Cassandra 5's
# message is "no viable alternative at input ')' (... b) VALUES (15, [P]))",
# Cassandra 6's is "rule insertValue failed predicate: {isParsingTxn}?", so
# we don't check the message.
# Reproduces SCYLLADB-5200 (Scylla accepts a bare P as a zero duration).
@pytest.mark.xfail(reason="SCYLLADB-5200")
def testInsertZeroDurationWithoutDesignator(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a INT PRIMARY KEY, b DURATION);") as table:
        assertInvalidThrow(cql, table, SyntaxException, "INSERT INTO %s (a, b) VALUES (15, P)")

# Reproduces #12243 (Scylla rejects a null TTL) and SCYLLADB-5208 (an empty
# TTL causes a server error).
@pytest.mark.xfail(reason="#12243, SCYLLADB-5208")
def testEmptyTTL(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, v int)") as table:
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (0, 0) USING TTL ?", None)
        # The Python driver can't send an empty value for an int bind
        # variable, so we send the raw serialized values ourselves.
        bound = BoundStatement(cql.prepare(f"INSERT INTO {table} (k, v) VALUES (1, 1) USING TTL ?"))
        bound.values = [b'']
        cql.execute(bound)
        assertRowsIgnoringOrder(execute(cql, table, "SELECT k, v, ttl(v) FROM %s"), row(1, 1, None), row(0, 0, None))

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
        # has "Invalid unset value for argument in call to function
        # blob_as_int", Scylla has "Invalid null or unset value for argument
        # to system.blobasint : (blob) -> int".
        # Cassandra's test now uses the new function name blob_as_int(),
        # which Scylla doesn't support yet (SCYLLADB-5141). Cassandra still
        # supports the old name blobAsInt(), so we use it, to keep testing
        # this on Scylla.
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

        assertRowsIgnoringOrder(execute(cql, table, "SELECT * FROM %s"),
                   row(1, null, null, "B", null),
                   row(0, 0, 0, "A", null))

        execute(cql, table, "INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (1, 0, 0, 0)")
        if forceFlush:
            flush(cql, table)
        assertRowsIgnoringOrder(execute(cql, table, "SELECT * FROM %s"),
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
def testPKInsertWithValueOver64K(cql, test_keyspace):
    with create_table(cql, test_keyspace, f"(a text, b text, PRIMARY KEY (a, b))") as table:
        assertInvalidThrow(cql, table, InvalidRequest,
                           "INSERT INTO %s (a, b) VALUES (?, 'foo')", 'x'*TOO_BIG)

# Reproduces #12247:
def testCKInsertWithValueOver64K(cql, test_keyspace):
    with create_table(cql, test_keyspace, f"(a text, b text, PRIMARY KEY (a, b))") as table:
        assertInvalidThrow(cql, table, InvalidRequest,
                           "INSERT INTO %s (a, b) VALUES ('foo', ?)", 'x'*TOO_BIG)

# The following three tests insert a collection with an empty (zero-length)
# element. The Python driver can't serialize such a collection, so we
# serialize it ourselves: a 4-byte count of elements (for a map, of
# key-value pairs), followed by each element (for a map, each key and value)
# as a 4-byte length and its bytes.
# Cassandra 5 fails these tests - it was fixed in Cassandra 6 by
# CASSANDRA-20667, which also added these tests - so we skip them on older
# Cassandra (new_to_cassandra_6).
def serialize_collection(count, elements):
    return struct.pack('>i', count) + b''.join(struct.pack('>i', len(e)) + e for e in elements)

def insertRaw(cql, table, value):
    bound = BoundStatement(cql.prepare(f"INSERT INTO {table}(pk, v1, v2) VALUES (?, ?, ?)"))
    bound.values = [struct.pack('>i', 0), value, value]
    cql.execute(bound)

def testMapEmptyValueMeaningless(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(pk int primary key, v1 map<int, int>, v2 frozen<map<int, int>>)") as table:
        # A map with one entry, whose key and value are both empty
        insertRaw(cql, table, serialize_collection(1, [b'', b'']))

        expected = {None: None}
        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row(0, expected, expected))

def testListEmptyValueMeaningless(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(pk int primary key, v1 list<int>, v2 frozen<list<int>>)") as table:
        insertRaw(cql, table, serialize_collection(1, [b'']))

        expected = [None]
        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row(0, expected, expected))

def testSetEmptyValueMeaningless(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(pk int primary key, v1 set<int>, v2 frozen<set<int>>)") as table:
        insertRaw(cql, table, serialize_collection(1, [b'']))

        expected = {None}
        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row(0, expected, expected))
