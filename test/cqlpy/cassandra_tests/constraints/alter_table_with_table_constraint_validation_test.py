# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# This is a translation of AlterTableWithTableConstraintValidationTest.java
# from Cassandra's test/unit/org/apache/cassandra/constraints directory.
#
# Column constraints (CHECK, NOT NULL) were added in Cassandra 6
# (CEP-42, CASSANDRA-19947), so these tests are marked new_to_cassandra_6.
# Scylla doesn't support constraints yet, so all of them are xfail.

from ..porting import *
from .cql_constraint_validation_tester import *

pytestmark = pytest.mark.xfail(reason="SCYLLADB-5234")

def testCreateTableWithColumnNamedConstraintDescribeTableNonFunction(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(pk int, ck1 int CHECK ck1 < 100, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC)") as table:
        execute(cql, table, "ALTER TABLE %s ALTER ck1 DROP CHECK")

        tableCreateStatement = ("CREATE TABLE " + table + " (\n" +
                                "    pk int,\n" +
                                "    ck1 int,\n" +
                                "    ck2 int,\n" +
                                "    v int,\n" +
                                "    PRIMARY KEY (pk, ck1, ck2)\n" +
                                ") WITH CLUSTERING ORDER BY (ck1 ASC, ck2 ASC)\n" +
                                "    AND ")
        assert_describe_starts_with(cql, table, tableCreateStatement)

def testCreateTableAddConstraint(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC)") as table:
        execute(cql, table, "ALTER TABLE %s ALTER ck1 CHECK ck1 < 100 AND ck1 > 10")

        tableCreateStatement = ("CREATE TABLE " + table + " (\n" +
                                "    pk int,\n" +
                                "    ck1 int CHECK ck1 < 100 AND ck1 > 10,\n" +
                                "    ck2 int,\n" +
                                "    v int,\n" +
                                "    PRIMARY KEY (pk, ck1, ck2)\n" +
                                ") WITH CLUSTERING ORDER BY (ck1 ASC, ck2 ASC)\n" +
                                "    AND ")
        assert_describe_starts_with(cql, table, tableCreateStatement)

def testCreateTableAddMultipleConstraints(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC)") as table:
        execute(cql, table, "ALTER TABLE %s ALTER ck1 CHECK ck1 < 100")
        execute(cql, table, "ALTER TABLE %s ALTER ck2 CHECK ck2 > 10")

        tableCreateStatement = ("CREATE TABLE " + table + " (\n" +
                                "    pk int,\n" +
                                "    ck1 int CHECK ck1 < 100,\n" +
                                "    ck2 int CHECK ck2 > 10,\n" +
                                "    v int,\n" +
                                "    PRIMARY KEY (pk, ck1, ck2)\n" +
                                ") WITH CLUSTERING ORDER BY (ck1 ASC, ck2 ASC)\n" +
                                "    AND ")
        assert_describe_starts_with(cql, table, tableCreateStatement)

def testCreateTableAddMultipleMixedConstraints(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(pk int, ck1 int, ck2 text, v int, PRIMARY KEY ((pk), ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC)") as table:
        execute(cql, table, "ALTER TABLE %s ALTER ck1 CHECK ck1 < 100")

        tableCreateStatement = ("CREATE TABLE " + table + " (\n" +
                                "    pk int,\n" +
                                "    ck1 int CHECK ck1 < 100,\n" +
                                "    ck2 text,\n" +
                                "    v int,\n" +
                                "    PRIMARY KEY (pk, ck1, ck2)\n" +
                                ") WITH CLUSTERING ORDER BY (ck1 ASC, ck2 ASC)\n" +
                                "    AND ")
        assert_describe_starts_with(cql, table, tableCreateStatement)

        execute(cql, table, "ALTER TABLE %s ALTER ck2 CHECK LENGTH() = 4")

        tableCreateStatement = ("CREATE TABLE " + table + " (\n" +
                                "    pk int,\n" +
                                "    ck1 int CHECK ck1 < 100,\n" +
                                "    ck2 text CHECK LENGTH() = 4,\n" +
                                "    v int,\n" +
                                "    PRIMARY KEY (pk, ck1, ck2)\n" +
                                ") WITH CLUSTERING ORDER BY (ck1 ASC, ck2 ASC)\n" +
                                "    AND ")
        assert_describe_starts_with(cql, table, tableCreateStatement)

        execute(cql, table, "ALTER TABLE %s ALTER v CHECK NOT NULL")

        tableCreateStatement = ("CREATE TABLE " + table + " (\n" +
                                "    pk int,\n" +
                                "    ck1 int CHECK ck1 < 100,\n" +
                                "    ck2 text CHECK LENGTH() = 4,\n" +
                                "    v int CHECK NOT NULL,\n" +
                                "    PRIMARY KEY (pk, ck1, ck2)\n" +
                                ") WITH CLUSTERING ORDER BY (ck1 ASC, ck2 ASC)\n" +
                                "    AND ")
        assert_describe_starts_with(cql, table, tableCreateStatement)

def testCreateTableAddAndRemoveConstraint(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(pk int, ck1 int, ck2 text, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC)") as table:
        execute(cql, table, "ALTER TABLE %s ALTER ck1 CHECK ck1 < 100")

        tableCreateStatement = ("CREATE TABLE " + table + " (\n" +
                                "    pk int,\n" +
                                "    ck1 int CHECK ck1 < 100,\n" +
                                "    ck2 text,\n" +
                                "    v int,\n" +
                                "    PRIMARY KEY (pk, ck1, ck2)\n" +
                                ") WITH CLUSTERING ORDER BY (ck1 ASC, ck2 ASC)\n" +
                                "    AND ")
        assert_describe_starts_with(cql, table, tableCreateStatement)

        execute(cql, table, "ALTER TABLE %s ALTER ck1 DROP CHECK")

        tableCreateStatement2 = ("CREATE TABLE " + table + " (\n" +
                                 "    pk int,\n" +
                                 "    ck1 int,\n" +
                                 "    ck2 text,\n" +
                                 "    v int,\n" +
                                 "    PRIMARY KEY (pk, ck1, ck2)\n" +
                                 ") WITH CLUSTERING ORDER BY (ck1 ASC, ck2 ASC)\n" +
                                 "    AND ")
        assert_describe_starts_with(cql, table, tableCreateStatement2)

# Note that Scylla's CDC is enabled with "cdc = {'enabled': true}", not
# Cassandra's "cdc = true", so even after Scylla supports constraints, these
# four tests will need to be changed to pass on Scylla.
def testAlterWithConstraintsAndCdcEnabled(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(pk text, ck1 int, ck2 int, PRIMARY KEY ((pk),ck1, ck2)) WITH cdc = true") as table:
        # It works
        execute(cql, table, "ALTER TABLE %s ALTER ck1 CHECK ck1 < 100")

def testAlterWithCdcAndPKConstraintsEnabled(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(pk text CHECK length() = 100, ck1 int, ck2 int, PRIMARY KEY ((pk), ck1, ck2))") as table:
        # It works
        execute(cql, table, "ALTER TABLE %s WITH cdc = true")

def testAlterWithCdcAndRegularConstraintsEnabled(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(pk text, ck1 int CHECK ck1 < 100, ck2 int, PRIMARY KEY (pk))") as table:
        # It works
        execute(cql, table, "ALTER TABLE %s WITH cdc = true")

def testAlterWithCdcAndClusteringConstraintsEnabled(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(pk text, ck1 int CHECK ck1 < 100, ck2 int, PRIMARY KEY ((pk), ck1, ck2))") as table:
        # It works
        execute(cql, table, "ALTER TABLE %s WITH cdc = true")

def testCreateTableAddConstraintWithIfExists(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC)") as table:
        execute(cql, table, "ALTER TABLE %s ALTER IF EXISTS foo CHECK foo < 100")

def testCreateTableAddConstraintWithNonExistingColumn(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC)") as table:
        expectedErrorMessage = "Column 'foo' doesn't exist"
        assert_invalid_message(cql, table, expectedErrorMessage, "ALTER TABLE %s ALTER foo CHECK foo < 100")

def testAlterTableAlterExistingColumnWithCheckOnNonExistingColumn(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(pk int, ck1 text, ck2 text, v int, PRIMARY KEY ((pk),ck1, ck2))") as table:
        assert_invalid_message(cql, table, "Constraint ck3 < 100 was not specified on a column it operates on: ck1 but on: ck3",
                               "ALTER TABLE %s ALTER ck1 CHECK ck3 < 100")

def testAlterTableAddNewColumnWithCheckOnNonExistingColumn(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(pk int, ck1 text, ck2 text, v int, PRIMARY KEY ((pk),ck1, ck2))") as table:
        assert_invalid_message(cql, table, "Constraint v3 < 100 was not specified on a column it operates on: v2 but on: v3",
                               "ALTER TABLE %s ADD v2 int CHECK v3 < 100")

def testAlterTableAddColumnWithCheck(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(pk text, col1 int, primary key (pk))") as table:
        execute(cql, table, "ALTER TABLE %s ADD col2 int CHECK col2 > 0")

def testNotNullSyntax(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(pk text, col1 int NOT NULL, primary key (pk))"):
        pass
    with create_table(cql, test_keyspace, "(pk text, col1 int CHECK NOT NULL, primary key (pk))"):
        pass
    with create_table(cql, test_keyspace, "(pk text, col1 int NOT NULL CHECK col1 > 0, primary key (pk))") as table:
        execute(cql, table, "ALTER TABLE %s ALTER col1 CHECK col1 > 100")
        execute(cql, table, "ALTER TABLE %s ALTER col1 CHECK NOT NULL AND col1 > 100")

    # The original test uses the name of the last table created above, but
    # the error is about the duplicate constraint, not the existing table,
    # so we use a new table name.
    assert_invalid_message(cql, test_keyspace + "." + unique_name(), "Duplicate definition of NOT NULL constraint",
                           "CREATE TABLE %s (pk text, col1 int NOT NULL CHECK NOT NULL, primary key (pk))")
