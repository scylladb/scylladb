# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# This is a translation of CreateTableValidationTest.java from Cassandra's
# test/unit/org/apache/cassandra/schema directory.

import re
from ..porting import *
from ...util import unique_name
from cassandra.protocol import InvalidRequest, ConfigurationException

# Cassandra's expectedFailure(): CREATE TABLE fails with the given exception
# type, and a message matching the given regular expression.
def expectedFailure(cql, keyspace, exceptionType, statement, errorMsg):
    table = keyspace + "." + unique_name()
    with pytest.raises(exceptionType, match=errorMsg):
        cql.execute(statement.replace("%s", table))
    cql.execute("DROP TABLE IF EXISTS " + table)

# Cassandra's assertInvalidMessage(), which accepts any exception type -
# Cassandra reports some of these errors as ConfigurationException, and
# Scylla as InvalidRequest. The message is a regular expression.
def assertInvalidMessage(cql, message, statement):
    with pytest.raises((InvalidRequest, ConfigurationException), match=message):
        cql.execute(statement)

def testInvalidBloomFilterFPRatio(cql, test_keyspace):
    expectedFailure(cql, test_keyspace, ConfigurationException, "CREATE TABLE %s (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 0.0000001",
                    "bloom_filter_fp_chance must be larger than ")
    expectedFailure(cql, test_keyspace, ConfigurationException, "CREATE TABLE %s (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 1.1",
                    "bloom_filter_fp_chance must be larger than ")
    # sanity check
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 0.1"):
        pass

def testCreateTableOnSelectedClusteringColumn(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);"):
        pass

def testCreateTableOnAllClusteringColumns(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC, ck2 DESC);"):
        pass

def testCreateTableErrorOnNonClusteringKey(cql, test_keyspace):
    # Cassandra's message ends with a list of the wrong columns (e.g.,
    # ": [v]"), and Scylla's doesn't, so we only check the beginning. Also,
    # most of these CLUSTERING ORDER clauses have more than one problem - a
    # non-clustering column, but also a missing clustering column or columns
    # in the wrong order - and Scylla checks for these problems in a
    # different order than Cassandra, so it may report one of the other
    # problems, which is also correct.
    expectedMessage = "Only clustering key columns can be defined in CLUSTERING ORDER directive|Missing CLUSTERING ORDER for column|The order of columns in the CLUSTERING ORDER directive must be the one of the clustering key"
    expectedFailure(cql, test_keyspace, InvalidRequest, "CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC, ck2 DESC, v ASC);",
                    expectedMessage)
    expectedFailure(cql, test_keyspace, InvalidRequest, "CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (v ASC);",
                    expectedMessage)
    expectedFailure(cql, test_keyspace, InvalidRequest, "CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (pk ASC);",
                    expectedMessage)
    expectedFailure(cql, test_keyspace, InvalidRequest, "CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (pk ASC, ck1 DESC);",
                    expectedMessage)
    expectedFailure(cql, test_keyspace, InvalidRequest, "CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC, ck2 DESC, pk DESC);",
                    expectedMessage)
    expectedFailure(cql, test_keyspace, InvalidRequest, "CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (pk DESC, v DESC);",
                    expectedMessage)
    expectedFailure(cql, test_keyspace, InvalidRequest, "CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (pk DESC, v DESC, ck1 DESC);",
                    expectedMessage)
    expectedFailure(cql, test_keyspace, InvalidRequest, "CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC, v ASC);",
                    expectedMessage)
    expectedFailure(cql, test_keyspace, InvalidRequest, "CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (v ASC, ck1 DESC);",
                    expectedMessage)

def testCreateTableInWrongOrder(cql, test_keyspace):
    expectedFailure(cql, test_keyspace, InvalidRequest, "CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck2 ASC, ck1 DESC);",
                    # Cassandra and Scylla word this error differently
                    "The order of columns in the CLUSTERING ORDER directive must (match that of the clustering columns|be the one of the clustering key)")

def testCreateTableWithMissingClusteringColumn(cql, test_keyspace):
    expectedFailure(cql, test_keyspace, InvalidRequest, "CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck2 ASC);",
                    "Missing CLUSTERING ORDER for column ck1")

# The test testCreatingTableWithLongName was not translated, because it
# checks Cassandra's exact limit on the length of table names (222
# characters), which is deliberately different from Scylla's (192
# characters, which Scylla's own tests check).

# Cassandra and Scylla word this error differently, so we accept both.
def testNonAlphanummericTableName(cql, test_keyspace):
    assertInvalidMessage(cql, re.escape("%s.d-3: Table name must not be empty or not contain non-alphanumeric-underscore characters (got \"d-3\")" % test_keyspace) +
                              "|" + re.escape("\"d-3\" is not a valid table name"),
                         "CREATE TABLE %s.\"d-3\" (key int PRIMARY KEY, val int)" % test_keyspace)
    assertInvalidMessage(cql, re.escape("%s.    : Table name must not be empty or not contain non-alphanumeric-underscore characters (got \"    \")" % test_keyspace) +
                              "|" + re.escape("\"    \" is not a valid table name"),
                         "CREATE TABLE %s.\"    \" (key int PRIMARY KEY, val int)" % test_keyspace)

# Reproduces SCYLLADB-5142: LeveledCompactionStrategy doesn't support the
# fanout_size option.
@pytest.mark.xfail(reason="SCYLLADB-5142")
def testInvalidCompactionOptions(cql, test_keyspace):
    expectedFailure(cql, test_keyspace, ConfigurationException, "CREATE TABLE %s (k int PRIMARY KEY, v int) WITH compaction = {'class': 'LeveledCompactionStrategy', 'fanout_size': '90', 'sstable_size_in_mb': '1089'}",
                    "your maxSSTableSize must be absurdly high to compute")
