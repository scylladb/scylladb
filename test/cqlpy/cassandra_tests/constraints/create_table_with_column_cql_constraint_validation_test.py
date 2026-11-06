# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# This is a translation of CreateTableWithColumnCqlConstraintValidationTest.java
# from Cassandra's test/unit/org/apache/cassandra/constraints directory.
#
# Column constraints (CHECK, NOT NULL) were added in Cassandra 6
# (CEP-42, CASSANDRA-19947), so these tests are marked new_to_cassandra_6.
# Scylla doesn't support constraints yet, so all of them are xfail.

from ..porting import *
from .cql_constraint_validation_tester import *

pytestmark = pytest.mark.xfail(reason="SCYLLADB-5234")

def testCreateTableWithColumnNotNamedConstraintDescribeTableNonFunction(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(pk int, ck1 int CHECK ck1 < 100, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC)") as table:
        tableCreateStatement = ("CREATE TABLE " + table + " (\n" +
                                "    pk int,\n" +
                                "    ck1 int CHECK ck1 < 100,\n" +
                                "    ck2 int,\n" +
                                "    v int,\n" +
                                "    PRIMARY KEY (pk, ck1, ck2)\n" +
                                ") WITH CLUSTERING ORDER BY (ck1 ASC, ck2 ASC)\n" +
                                "    AND ")
        assert_describe_starts_with(cql, table, tableCreateStatement)

def testCreateTableWithColumnMultipleConstraintsDescribeTableNonFunction(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(pk int, ck1 int CHECK ck1 < 100 AND ck1 > 10, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC)") as table:
        tableCreateStatement = ("CREATE TABLE " + table + " (\n" +
                                "    pk int,\n" +
                                "    ck1 int CHECK ck1 < 100 AND ck1 > 10,\n" +
                                "    ck2 int,\n" +
                                "    v int,\n" +
                                "    PRIMARY KEY (pk, ck1, ck2)\n" +
                                ") WITH CLUSTERING ORDER BY (ck1 ASC, ck2 ASC)\n" +
                                "    AND ")
        assert_describe_starts_with(cql, table, tableCreateStatement)

def testCreateTableWithColumnNotNamedConstraintDescribeTableFunction(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(pk int, ck1 text CHECK LENGTH() = 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC)") as table:
        tableCreateStatement = ("CREATE TABLE " + table + " (\n" +
                                "    pk int,\n" +
                                "    ck1 text CHECK LENGTH() = 4,\n" +
                                "    ck2 int,\n" +
                                "    v int,\n" +
                                "    PRIMARY KEY (pk, ck1, ck2)\n" +
                                ") WITH CLUSTERING ORDER BY (ck1 ASC, ck2 ASC)\n" +
                                "    AND ")
        assert_describe_starts_with(cql, table, tableCreateStatement)

def testCreateTableWithColumnNotNullConstraintDescribe(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(pk int, ck1 int, ck2 int, v int CHECK NOT NULL, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC)") as table:
        tableCreateStatement = ("CREATE TABLE " + table + " (\n" +
                                "    pk int,\n" +
                                "    ck1 int,\n" +
                                "    ck2 int,\n" +
                                "    v int CHECK NOT NULL,\n" +
                                "    PRIMARY KEY (pk, ck1, ck2)\n" +
                                ") WITH CLUSTERING ORDER BY (ck1 ASC, ck2 ASC)\n" +
                                "    AND ")
        assert_describe_starts_with(cql, table, tableCreateStatement)

# SCALAR

# Most of the scalar tests below have the same structure: create a table
# with a constraint on ck1 of the given type, check that inserting ck1
# values from the "valid" ranges works, and that inserting ck1 values from
# the "invalid" ranges fails with the given message.
def check_scalar_constraint(cql, test_keyspace, typ, constraint, valid, invalid, initial_valid=[]):
    for order in ORDERS:
        with create_table(cql, test_keyspace, f"(pk int, ck1 {typ} CHECK {constraint}, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})") as table:
            # Valid
            for d in initial_valid:
                execute(cql, table, f"INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, {d}, 2, 3)")
            for values in valid:
                for d in values:
                    execute(cql, table, f"INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, {d}, 3, 4)")
            # Invalid
            for expectedErrorMessage, values in invalid:
                for d in values:
                    assert_invalid_message(cql, table, expectedErrorMessage, f"INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, {d}, 3, 4)")

def testCreateTableWithColumnWithClusteringColumnLessThanScalarConstraintInteger(cql, test_keyspace, new_to_cassandra_6):
    expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 < 4"
    check_scalar_constraint(cql, test_keyspace, "int", "ck1 < 4",
        [integers(0, 3)],
        [(expectedErrorMessage, integers(4, 100))])

def testCreateTableWithColumnWithClusteringColumnBiggerThanScalarConstraint(cql, test_keyspace, new_to_cassandra_6):
    expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 > 4"
    check_scalar_constraint(cql, test_keyspace, "int", "ck1 > 4",
        [integers(5, 100)],
        [(expectedErrorMessage, integers(0, 4))])

def testCreateTableWithColumnWithClusteringColumnBiggerOrEqualThanScalarConstraint(cql, test_keyspace, new_to_cassandra_6):
    expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 >= 4"
    check_scalar_constraint(cql, test_keyspace, "int", "ck1 >= 4",
        [integers(4, 100)],
        [(expectedErrorMessage, integers(0, 3))])

def testCreateTableWithColumnWithClusteringColumnLessOrEqualThanScalarConstraint(cql, test_keyspace, new_to_cassandra_6):
    expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 <= 4"
    check_scalar_constraint(cql, test_keyspace, "int", "ck1 <= 4",
        [integers(0, 4)],
        [(expectedErrorMessage, integers(5, 100))])

def testCreateTableWithColumnWithClusteringColumnDifferentThanScalarConstraint(cql, test_keyspace, new_to_cassandra_6):
    expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 != 4"
    check_scalar_constraint(cql, test_keyspace, "int", "ck1 != 4",
        [integers(0, 3), integers(5, 100)],
        [(expectedErrorMessage, [4])])

def testCreateTableWithColumnWithClusteringColumnMultipleScalarConstraints(cql, test_keyspace, new_to_cassandra_6):
    expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 >= 2"
    expectedErrorMessage2 = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 < 4"
    check_scalar_constraint(cql, test_keyspace, "int", "ck1 < 4 AND ck1 >= 2",
        [integers(2, 3)],
        [(expectedErrorMessage, integers(-100, 1)), (expectedErrorMessage2, integers(4, 100))])

def testCreateTableWithColumnWithClusteringColumnLessThanScalarSmallIntConstraint(cql, test_keyspace, new_to_cassandra_6):
    expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 < 4"
    check_scalar_constraint(cql, test_keyspace, "smallint", "ck1 < 4",
        [integers(0, 3)],
        [(expectedErrorMessage, integers(4, 100))])

def testCreateTableWithColumnWithClusteringColumnBiggerThanScalarSmallIntConstraint(cql, test_keyspace, new_to_cassandra_6):
    expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 > 4"
    check_scalar_constraint(cql, test_keyspace, "smallint", "ck1 > 4",
        [integers(5, 100)],
        [(expectedErrorMessage, integers(0, 4))])

def testCreateTableWithColumnWithClusteringColumnBiggerOrEqualThanScalarSmallIntConstraint(cql, test_keyspace, new_to_cassandra_6):
    expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 >= 4"
    check_scalar_constraint(cql, test_keyspace, "smallint", "ck1 >= 4",
        [integers(4, 100)],
        [(expectedErrorMessage, integers(0, 3))])

def testCreateTableWithColumnWithClusteringColumnLessOrEqualThanScalarSmallIntConstraint(cql, test_keyspace, new_to_cassandra_6):
    expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 <= 4"
    check_scalar_constraint(cql, test_keyspace, "smallint", "ck1 <= 4",
        [integers(0, 4)],
        [(expectedErrorMessage, integers(5, 100))])

def testCreateTableWithColumnWithClusteringColumnDifferentThanScalarSmallIntConstraint(cql, test_keyspace, new_to_cassandra_6):
    expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 != 4"
    check_scalar_constraint(cql, test_keyspace, "smallint", "ck1 != 4",
        [integers(0, 3), integers(5, 100)],
        [(expectedErrorMessage, [4])])

def testCreateTableWithColumnWithClusteringColumnMultipleScalarSmallIntConstraints(cql, test_keyspace, new_to_cassandra_6):
    expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 >= 2"
    expectedErrorMessage2 = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 < 4"
    check_scalar_constraint(cql, test_keyspace, "smallint", "ck1 < 4 AND ck1 >= 2",
        [integers(2, 3)],
        [(expectedErrorMessage, integers(-100, 1)), (expectedErrorMessage2, integers(4, 100))])

def testCreateTableWithColumnWithClusteringColumnLessThanScalarDecimalConstraint(cql, test_keyspace, new_to_cassandra_6):
    expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 < 4.2"
    check_scalar_constraint(cql, test_keyspace, "decimal", "ck1 < 4.2",
        [doubles(0, 4.1)],
        [(expectedErrorMessage, doubles(4.3, 100))],
        initial_valid=[2])

def testCreateTableWithColumnWithClusteringColumnBiggerThanScalarDecimalConstraint(cql, test_keyspace, new_to_cassandra_6):
    expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 > 4.2"
    check_scalar_constraint(cql, test_keyspace, "decimal", "ck1 > 4.2",
        [doubles(4.3, 100)],
        [(expectedErrorMessage, doubles(0, 4.2))])

def testCreateTableWithColumnWithClusteringColumnBiggerOrEqualThanScalarDecimalConstraint(cql, test_keyspace, new_to_cassandra_6):
    expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 >= 4.2"
    check_scalar_constraint(cql, test_keyspace, "decimal", "ck1 >= 4.2",
        [doubles(4.2, 100)],
        [(expectedErrorMessage, doubles(0, 4.1))])

def testCreateTableWithColumnWithClusteringColumnLessOrEqualThanScalarDecimalConstraint(cql, test_keyspace, new_to_cassandra_6):
    expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 <= 4.2"
    check_scalar_constraint(cql, test_keyspace, "decimal", "ck1 <= 4.2",
        [doubles(0, 4.2)],
        [(expectedErrorMessage, doubles(4.3, 100))])

def testCreateTableWithColumnWithClusteringColumnDifferentThanScalarDecimalConstraint(cql, test_keyspace, new_to_cassandra_6):
    expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 != 4.2"
    check_scalar_constraint(cql, test_keyspace, "decimal", "ck1 != 4.2",
        [doubles(0, 4.1), doubles(4.3, 100)],
        [(expectedErrorMessage, [4.2])])

def testCreateTableWithColumnWithClusteringColumnMultipleScalarDecimalConstraints(cql, test_keyspace, new_to_cassandra_6):
    expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 >= 2.1"
    expectedErrorMessage2 = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 < 4.2"
    check_scalar_constraint(cql, test_keyspace, "decimal", "ck1 < 4.2 AND ck1 >= 2.1",
        [doubles(2.1, 4.1)],
        [(expectedErrorMessage, doubles(-100, 2)), (expectedErrorMessage2, doubles(4.2, 100))])

def testCreateTableWithColumnWithClusteringColumnLessThanScalarDoubleConstraint(cql, test_keyspace, new_to_cassandra_6):
    expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 < 4.2"
    check_scalar_constraint(cql, test_keyspace, "double", "ck1 < 4.2",
        [doubles(0, 4.1)],
        [(expectedErrorMessage, doubles(4.3, 100))],
        initial_valid=[2])

def testCreateTableWithColumnWithClusteringColumnBiggerThanScalarDoubleConstraint(cql, test_keyspace, new_to_cassandra_6):
    expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 > 4.2"
    check_scalar_constraint(cql, test_keyspace, "double", "ck1 > 4.2",
        [doubles(4.3, 100)],
        [(expectedErrorMessage, doubles(0, 4.2))])

def testCreateTableWithColumnWithClusteringColumnBiggerOrEqualThanScalarDoubleConstraint(cql, test_keyspace, new_to_cassandra_6):
    expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 >= 4.2"
    check_scalar_constraint(cql, test_keyspace, "double", "ck1 >= 4.2",
        [doubles(4.2, 100)],
        [(expectedErrorMessage, doubles(0, 4.1))])

def testCreateTableWithColumnWithClusteringColumnLessOrEqualThanScalarDoubleConstraint(cql, test_keyspace, new_to_cassandra_6):
    expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 <= 4.2"
    check_scalar_constraint(cql, test_keyspace, "double", "ck1 <= 4.2",
        [doubles(0, 4.2)],
        [(expectedErrorMessage, doubles(4.3, 100))])

def testCreateTableWithColumnWithClusteringColumnDifferentThanScalarDoubleConstraint(cql, test_keyspace, new_to_cassandra_6):
    expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 != 4.2"
    check_scalar_constraint(cql, test_keyspace, "double", "ck1 != 4.2",
        [doubles(0, 4.1), doubles(4.3, 100)],
        [(expectedErrorMessage, [4.2])])

def testCreateTableWithColumnWithClusteringColumnMultipleScalarDoubleConstraints(cql, test_keyspace, new_to_cassandra_6):
    expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 >= 2.1"
    expectedErrorMessage2 = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 < 4.2"
    check_scalar_constraint(cql, test_keyspace, "double", "ck1 < 4.2 AND ck1 >= 2.1",
        [doubles(2.1, 4.1)],
        [(expectedErrorMessage, doubles(-100, 2)), (expectedErrorMessage2, doubles(4.2, 100))])

def testCreateTableWithColumnWithClusteringColumnLessThanScalarFloatConstraint(cql, test_keyspace, new_to_cassandra_6):
    expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 < 4.2"
    check_scalar_constraint(cql, test_keyspace, "float", "ck1 < 4.2",
        [doubles(0, 4.1)],
        [(expectedErrorMessage, doubles(4.3, 100))],
        initial_valid=[2])

def testCreateTableWithColumnWithClusteringColumnBiggerThanScalarFloatConstraint(cql, test_keyspace, new_to_cassandra_6):
    expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 > 4.2"
    check_scalar_constraint(cql, test_keyspace, "float", "ck1 > 4.2",
        [doubles(4.3, 100)],
        [(expectedErrorMessage, doubles(0, 4.2))])

def testCreateTableWithColumnWithClusteringColumnBiggerOrEqualThanScalarFloatConstraint(cql, test_keyspace, new_to_cassandra_6):
    expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 >= 4.2"
    check_scalar_constraint(cql, test_keyspace, "float", "ck1 >= 4.2",
        [doubles(4.2, 100)],
        [(expectedErrorMessage, doubles(0, 4.1))])

def testCreateTableWithColumnWithClusteringColumnLessOrEqualThanScalarFloatConstraint(cql, test_keyspace, new_to_cassandra_6):
    expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 <= 4.2"
    check_scalar_constraint(cql, test_keyspace, "float", "ck1 <= 4.2",
        [doubles(0, 4.2)],
        [(expectedErrorMessage, doubles(4.3, 100))])

def testCreateTableWithColumnWithClusteringColumnDifferentThanScalarFloatConstraint(cql, test_keyspace, new_to_cassandra_6):
    expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 != 4.2"
    check_scalar_constraint(cql, test_keyspace, "float", "ck1 != 4.2",
        [doubles(0, 4.1), doubles(4.3, 100)],
        [(expectedErrorMessage, [4.2])])

def testCreateTableWithColumnWithClusteringColumnMultipleScalarFloatConstraints(cql, test_keyspace, new_to_cassandra_6):
    expectedErrorMessage = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 >= 2.1"
    expectedErrorMessage2 = "Column value does not satisfy value constraint for column 'ck1'. It should be ck1 < 4.2"
    check_scalar_constraint(cql, test_keyspace, "float", "ck1 < 4.2 AND ck1 >= 2.1",
        [doubles(2.1, 4.1)],
        [(expectedErrorMessage, doubles(-100, 2)), (expectedErrorMessage2, doubles(4.2, 100))])

# A constraint on a column makes it implicitly NOT NULL
def check_not_null_scalar_constraint(cql, test_keyspace, typ):
    for order in ORDERS:
        with create_table(cql, test_keyspace, f"(pk int, ck1 int, ck2 int, v {typ} CHECK v < 4 AND v >= 2, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})") as table:
            expectedErrorMessage = "Column value does not satisfy value constraint for column 'v' as it is null."
            assert_invalid_message(cql, table, expectedErrorMessage, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, null)")

def testCreateTableWithColumnWithNotNullCheckScalarIntConstraints(cql, test_keyspace, new_to_cassandra_6):
    check_not_null_scalar_constraint(cql, test_keyspace, "int")

def testCreateTableWithColumnWithNotNullCheckScalarSmallintConstraints(cql, test_keyspace, new_to_cassandra_6):
    check_not_null_scalar_constraint(cql, test_keyspace, "smallint")

def testCreateTableWithColumnWithNotNullCheckScalarDecimalConstraints(cql, test_keyspace, new_to_cassandra_6):
    check_not_null_scalar_constraint(cql, test_keyspace, "decimal")

def testCreateTableWithColumnWithNotNullCheckScalarDoubleConstraints(cql, test_keyspace, new_to_cassandra_6):
    check_not_null_scalar_constraint(cql, test_keyspace, "double")

def testCreateTableWithColumnWithNotNullCheckScalarFloatConstraints(cql, test_keyspace, new_to_cassandra_6):
    check_not_null_scalar_constraint(cql, test_keyspace, "float")

# FUNCTION

def testCreateTableWithColumnWithClusteringColumnLengthEqualToConstraint(cql, test_keyspace, new_to_cassandra_6):
    check_length_constraint(cql, test_keyspace, "(pk int, ck1 text CHECK LENGTH() = 4, ck2 int, v int, PRIMARY KEY ((pk), ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC)",
        CK1_INSERT, "ck1", ["'fooo'"], ["'foo'", "'foooo'"])

def testCreateTableWithColumnWithClusteringColumnLengthDifferentThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    check_length_constraint(cql, test_keyspace, "(pk int, ck1 text CHECK LENGTH() != 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC)",
        CK1_INSERT, "ck1", ["'foo'", "'foooo'"], ["'fooo'"])

def testCreateTableWithColumnWithClusteringColumnLengthBiggerThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    check_length_constraint(cql, test_keyspace, "(pk int, ck1 text CHECK LENGTH() > 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC)",
        CK1_INSERT, "ck1", ["'foooo'"], ["'foo'", "'fooo'"])

def testCreateTableWithColumnWithClusteringColumnLengthBiggerOrEqualThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    check_length_constraint(cql, test_keyspace, "(pk int, ck1 text CHECK LENGTH() >= 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC)",
        CK1_INSERT, "ck1", ["'foooo'", "'fooo'"], ["'foo'"])

def testCreateTableWithColumnWithClusteringColumnLengthSmallerThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    check_length_constraint(cql, test_keyspace, "(pk int, ck1 text CHECK LENGTH() < 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC)",
        CK1_INSERT, "ck1", ["'foo'"], ["'fooo'", "'foooo'"])

def testCreateTableWithColumnWithClusteringColumnLengthSmallerOrEqualThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    check_length_constraint(cql, test_keyspace, "(pk int, ck1 text CHECK LENGTH() <= 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC)",
        CK1_INSERT, "ck1", ["'foo'", "'fooo'"], ["'foooo'"])

def testCreateTableWithColumnWithClusteringBlobColumnLengthEqualToConstraint(cql, test_keyspace, new_to_cassandra_6):
    check_length_constraint(cql, test_keyspace, "(pk int, ck1 blob CHECK LENGTH() = 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC)",
        CK1_INSERT, "ck1", ["textAsBlob('fooo')"], ["textAsBlob('foo')", "textAsBlob('foooo')"])

def testCreateTableWithColumnWithClusteringBlobColumnLengthDifferentThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    check_length_constraint(cql, test_keyspace, "(pk int, ck1 blob CHECK LENGTH() != 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC)",
        CK1_INSERT, "ck1", ["textAsBlob('foo')", "textAsBlob('foooo')"], ["textAsBlob('fooo')"])

def testCreateTableWithColumnWithClusteringBlobColumnLengthBiggerThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    check_length_constraint(cql, test_keyspace, "(pk int, ck1 blob CHECK LENGTH() > 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC)",
        CK1_INSERT, "ck1", ["textAsBlob('foooo')"], ["textAsBlob('foo')", "textAsBlob('fooo')"])

def testCreateTableWithColumnWithClusteringBlobColumnLengthBiggerOrEqualThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    check_length_constraint(cql, test_keyspace, "(pk int, ck1 blob CHECK LENGTH() >= 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC)",
        CK1_INSERT, "ck1", ["textAsBlob('foooo')", "textAsBlob('fooo')"], ["textAsBlob('foo')"])

def testCreateTableWithColumnWithClusteringBlobColumnLengthSmallerThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    check_length_constraint(cql, test_keyspace, "(pk int, ck1 blob CHECK LENGTH() < 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC)",
        CK1_INSERT, "ck1", ["textAsBlob('foo')"], ["textAsBlob('fooo')", "textAsBlob('foooo')"])

def testCreateTableWithColumnWithClusteringBlobColumnLengthSmallerOrEqualThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    check_length_constraint(cql, test_keyspace, "(pk int, ck1 blob CHECK LENGTH() <= 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC)",
        CK1_INSERT, "ck1", ["textAsBlob('foo')", "textAsBlob('fooo')"], ["textAsBlob('foooo')"])

def testCreateTableWithColumnWithPkColumnLengthEqualToConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk text CHECK LENGTH() = 4, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            PK_INSERT, "pk", ["'fooo'"], ["'foo'", "'foooo'"])

def testCreateTableWithColumnWithPkColumnLengthDifferentThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk text CHECK LENGTH() != 4, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            PK_INSERT, "pk", ["'foo'", "'foooo'"], ["'fooo'"])

def testCreateTableWithColumnWithPkColumnLengthBiggerThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk text CHECK LENGTH() > 4, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            PK_INSERT, "pk", ["'foooo'"], ["'foo'", "'fooo'"])

def testCreateTableWithColumnWithPkColumnLengthBiggerOrEqualThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk text CHECK LENGTH() >= 4, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            PK_INSERT, "pk", ["'foooo'", "'fooo'"], ["'foo'"])

def testCreateTableWithColumnWithPkColumnLengthSmallerThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk text CHECK LENGTH() < 4, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            PK_INSERT, "pk", ["'foo'"], ["'fooo'", "'foooo'"])

def testCreateTableWithColumnWithPkColumnLengthSmallerOrEqualThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk text CHECK LENGTH() <= 4, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            PK_INSERT, "pk", ["'foo'", "'fooo'"], ["'foooo'"])

def testCreateTableWithColumnWithRegularColumnLengthEqualToConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk int, ck1 int, ck2 int, v text CHECK LENGTH() = 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            V_INSERT, "v", ["'fooo'"], ["'foo'", "'foooo'"])

def testCreateTableWithColumnWithRegularColumnLengthDifferentThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk int, ck1 int, ck2 int, v text CHECK LENGTH() != 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            V_INSERT, "v", ["'foo'", "'foooo'"], ["'fooo'"])

def testCreateTableWithColumnWithRegularColumnLengthBiggerThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk int, ck1 int, ck2 int, v text CHECK LENGTH() > 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            V_INSERT, "v", ["'foooo'"], ["'foo'", "'fooo'"])

def testCreateTableWithColumnWithRegularColumnLengthBiggerOrEqualThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk int, ck1 int, ck2 int, v text CHECK LENGTH() >= 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            V_INSERT, "v", ["'foooo'", "'fooo'"], ["'foo'"])

def testCreateTableWithColumnWithRegularColumnLengthSmallerThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk int, ck1 int, ck2 int, v text CHECK LENGTH() < 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            V_INSERT, "v", ["'foo'"], ["'fooo'", "'foooo'"])

def testCreateTableWithColumnWithRegularColumnLengthSmallerOrEqualThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk int, ck1 int, ck2 int, v text CHECK LENGTH() <= 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            V_INSERT, "v", ["'foo'", "'fooo'"], ["'foooo'"])

def testCreateTableWithColumnWithRegularColumnLengthCheckNullTextConstraint(cql, test_keyspace, new_to_cassandra_6):
    check_length_null_constraint(cql, test_keyspace, "LENGTH()", "text")

def testCreateTableWithColumnWithRegularColumnLengthCheckNullVarcharConstraint(cql, test_keyspace, new_to_cassandra_6):
    check_length_null_constraint(cql, test_keyspace, "LENGTH()", "varchar")

def testCreateTableWithColumnWithRegularColumnLengthCheckNullAsciiConstraint(cql, test_keyspace, new_to_cassandra_6):
    check_length_null_constraint(cql, test_keyspace, "LENGTH()", "ascii")

def testCreateTableWithColumnWithRegularColumnLengthCheckNullBlobConstraint(cql, test_keyspace, new_to_cassandra_6):
    check_length_null_constraint(cql, test_keyspace, "LENGTH()", "blob")

def testCreateTableWithColumnMixedColumnsLengthConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        with create_table(cql, test_keyspace, f"(pk text CHECK LENGTH() = 4, ck1 int, ck2 int, v text CHECK LENGTH() = 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})") as table:
            # Valid
            execute(cql, table, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fooo', 2, 3, 'fooo')")

            expectedErrorMessage = "Column value does not satisfy value constraint for column 'pk'. It has a length of"
            expectedErrorMessage2 = "Column value does not satisfy value constraint for column 'v'. It has a length of"
            # Invalid
            assert_invalid_message(cql, table, expectedErrorMessage, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('foo', 2, 3, 'foo')")
            assert_invalid_message(cql, table, expectedErrorMessage2, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fooo', 2, 3, 'foo')")
            assert_invalid_message(cql, table, expectedErrorMessage, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('foo', 2, 3, 'fooo')")
            assert_invalid_message(cql, table, expectedErrorMessage, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('foooo', 2, 3, 'fooo')")
            assert_invalid_message(cql, table, expectedErrorMessage2, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fooo', 2, 3, 'foooo')")
            assert_invalid_message(cql, table, expectedErrorMessage, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('foooo', 2, 3, 'foooo')")

def testCreateTableWithWrongColumnConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        assert_invalid_create_table(cql, test_keyspace, f"(pk text, ck1 int CHECK LENGTH() = 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})")

def testCreateTableWithWrongColumnMultipleConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        assert_invalid_create_table(cql, test_keyspace, f"(pk text, ck1 int CHECK LENGTH() = 4 AND ck1 < 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})")

def testCreateTableWithColumnWithClusteringColumnInvalidTypeConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        assert_invalid_create_table(cql, test_keyspace, f"(pk int, ck1 int CHECK LENGTH() = 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})")

def testCreateTableWithColumnWithClusteringColumnInvalidScalarTypeConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        assert_invalid_create_table(cql, test_keyspace, f"(pk text CHECK pk = 4, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            "Constraint 'pk =' can be used only for columns of type")

def testCreateTableInvalidFunction(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        assert_invalid_create_table(cql, test_keyspace, f"(pk text CHECK not_a_function() = 4, ck1 int, ck2 int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})")

# Note that Scylla's CDC is enabled with "cdc = {'enabled': true}", not
# Cassandra's "cdc = true", so even after Scylla supports constraints, these
# three tests will need to be changed to pass on Scylla.
def testCreateTableWithPKConstraintsAndCDCEnabled(cql, test_keyspace, new_to_cassandra_6):
    # It works
    with create_table(cql, test_keyspace, "(pk text CHECK length() = 4, ck1 int, ck2 int, PRIMARY KEY ((pk), ck1, ck2)) WITH cdc = true"):
        pass

def testCreateTableWithClusteringConstraintsAndCDCEnabled(cql, test_keyspace, new_to_cassandra_6):
    # It works
    with create_table(cql, test_keyspace, "(pk text, ck1 int CHECK ck1 < 100, ck2 int, PRIMARY KEY ((pk), ck1, ck2)) WITH cdc = true"):
        pass

def testCreateTableWithRegularConstraintsAndCDCEnabled(cql, test_keyspace, new_to_cassandra_6):
    # It works
    with create_table(cql, test_keyspace, "(pk text, ck1 int CHECK ck1 < 100, ck2 int, PRIMARY KEY (pk)) WITH cdc = true"):
        pass

# Copy table with like
# This test also needs CREATE TABLE LIKE, which Scylla doesn't support
# either (SCYLLADB-5147).
@pytest.mark.xfail(reason="SCYLLADB-5234, SCYLLADB-5147")
def testCreateTableWithColumnWithClusteringColumnLessThanScalarConstraintIntegerOnLikeTable(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        with create_table(cql, test_keyspace, f"(pk int, ck1 int CHECK ck1 < 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})") as table:
            tb_copy = test_keyspace + "." + unique_name()
            execute(cql, table, f"create table {tb_copy} like %s")
            try:
                # Valid
                for d in integers(0, 3):
                    execute(cql, tb_copy, f"INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, {d}, 3, 4)")

                # Invalid
                for d in integers(4, 100):
                    assert_invalid_throw(cql, tb_copy, InvalidRequest, f"INSERT INTO %s(pk, ck1, ck2, v) VALUES (1, {d}, 3, 4)")
            finally:
                cql.execute(f"DROP TABLE {tb_copy}")

def testCreateTableAddConstraintWithCheckOnNonExistingColumn(cql, test_keyspace, new_to_cassandra_6):
    assert_invalid_create_table(cql, test_keyspace, "(pk int, ck1 int CHECK ck3 > 5, ck2 text, v int, PRIMARY KEY ((pk),ck1, ck2))",
        "Constraint ck3 > 5 was not specified on a column it operates on: ck1 but on: ck3")
