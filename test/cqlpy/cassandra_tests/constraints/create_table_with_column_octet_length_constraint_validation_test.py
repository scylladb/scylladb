# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# This is a translation of CreateTableWithColumnOctetLengthConstraintValidationTest.java
# from Cassandra's test/unit/org/apache/cassandra/constraints directory.
#
# Column constraints (CHECK, NOT NULL) were added in Cassandra 6
# (CEP-42, CASSANDRA-19947), so these tests are marked new_to_cassandra_6.
# Scylla doesn't support constraints yet, so all of them are xfail.
#
# The tests below use the character 'ñ', which takes two bytes in UTF-8,
# so for example 'fño' has an octet length of 4 (but a length of 3).

from ..porting import *
from .cql_constraint_validation_tester import *

pytestmark = pytest.mark.xfail(reason="SCYLLADB-5234")

def testCreateTableWithColumnWithClusteringColumnSerializedSizeEqualToConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk int, ck1 text CHECK OCTET_LENGTH() = 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            CK1_INSERT, "ck1", ["'fooo'", "'fño'"], ["'foo'", "'fooñ'", "'foooo'"])

def testCreateTableWithColumnWithClusteringColumnSerializedSizeDifferentThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk int, ck1 text CHECK OCTET_LENGTH() != 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            CK1_INSERT, "ck1", ["'fñ'", "'fñoo'"], ["'fño'"])

def testCreateTableWithColumnWithClusteringColumnSerializedSizeBiggerThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk int, ck1 text CHECK OCTET_LENGTH() > 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            CK1_INSERT, "ck1", ["'fñoo'"], ["'fñ'", "'fño'"])

def testCreateTableWithColumnWithClusteringColumnSerializedSizeBiggerOrEqualThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk int, ck1 text CHECK OCTET_LENGTH() >= 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            CK1_INSERT, "ck1", ["'fñoo'", "'fño'"], ["'fñ'"])

def testCreateTableWithColumnWithClusteringColumnSerializedSizeSmallerThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk int, ck1 text CHECK OCTET_LENGTH() < 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            CK1_INSERT, "ck1", ["'fñ'"], ["'fño'", "'fñoo'"])

def testCreateTableWithColumnWithClusteringColumnSerializedSizeSmallerOrEqualThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk int, ck1 text CHECK OCTET_LENGTH() <= 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            CK1_INSERT, "ck1", ["'fñ'", "'fño'"], ["'fñoo'"])

def testCreateTableWithColumnWithClusteringBlobColumnSerializedSizeEqualToConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk int, ck1 blob CHECK OCTET_LENGTH() = 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            CK1_INSERT, "ck1", ["textAsBlob('fño')"], ["textAsBlob('fñ')", "textAsBlob('fñoo')"])

def testCreateTableWithColumnWithClusteringBlobColumnSerializedSizeDifferentThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk int, ck1 blob CHECK OCTET_LENGTH() != 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            CK1_INSERT, "ck1", ["textAsBlob('fñ')", "textAsBlob('fñoo')"], ["textAsBlob('fño')"])

def testCreateTableWithColumnWithClusteringBlobColumnSerializedSizeBiggerThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk int, ck1 blob CHECK OCTET_LENGTH() > 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            CK1_INSERT, "ck1", ["textAsBlob('fñoo')"], ["textAsBlob('fñ')", "textAsBlob('fño')"])

def testCreateTableWithColumnWithClusteringBlobColumnSerializedSizeBiggerOrEqualThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk int, ck1 blob CHECK OCTET_LENGTH() >= 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            CK1_INSERT, "ck1", ["textAsBlob('fñoo')", "textAsBlob('fño')"], ["textAsBlob('fñ')"])

def testCreateTableWithColumnWithClusteringBlobColumnSerializedSizeSmallerThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk int, ck1 blob CHECK OCTET_LENGTH() < 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            CK1_INSERT, "ck1", ["textAsBlob('fñ')"], ["textAsBlob('fño')", "textAsBlob('fñoo')"])

def testCreateTableWithColumnWithClusteringBlobColumnSerializedSizeSmallerOrEqualThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk int, ck1 blob CHECK OCTET_LENGTH() <= 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            CK1_INSERT, "ck1", ["textAsBlob('fñ')", "textAsBlob('fño')"], ["textAsBlob('fñoo')"])

def testCreateTableWithColumnWithPkColumnSerializedSizeEqualToConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk text CHECK OCTET_LENGTH() = 4, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            PK_INSERT, "pk", ["'fño'"], ["'fñ'", "'fñoo'"])

def testCreateTableWithColumnWithPkColumnSerializedSizeDifferentThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk text CHECK OCTET_LENGTH() != 4, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            PK_INSERT, "pk", ["'fñ'", "'fñoo'"], ["'fño'"])

def testCreateTableWithColumnWithPkColumnSerializedSizeBiggerThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk text CHECK OCTET_LENGTH() > 4, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            PK_INSERT, "pk", ["'fñoo'"], ["'fñ'", "'fño'"])

def testCreateTableWithColumnWithPkColumnSerializedSizeBiggerOrEqualThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk text CHECK OCTET_LENGTH() >= 4, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            PK_INSERT, "pk", ["'fñoo'", "'fño'"], ["'fñ'"])

def testCreateTableWithColumnWithPkColumnSerializedSizeSmallerThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk text CHECK OCTET_LENGTH() < 4, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            PK_INSERT, "pk", ["'fñ'"], ["'fño'", "'fñoo'"])

def testCreateTableWithColumnWithPkColumnSerializedSizeSmallerOrEqualThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk text CHECK OCTET_LENGTH() <= 4, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            PK_INSERT, "pk", ["'fñ'", "'fño'"], ["'fñoo'"])

def testCreateTableWithColumnWithRegularColumnSerializedSizeEqualToConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk int, ck1 int, ck2 int, v text CHECK OCTET_LENGTH() = 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            V_INSERT, "v", ["'fño'"], ["'fñ'", "'fñoo'"])

def testCreateTableWithColumnWithRegularColumnSerializedSizeDifferentThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk int, ck1 int, ck2 int, v text CHECK OCTET_LENGTH() != 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            V_INSERT, "v", ["'fñ'", "'fñoo'"], ["'fño'"])

def testCreateTableWithColumnWithRegularColumnSerializedSizeBiggerThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk int, ck1 int, ck2 int, v text CHECK OCTET_LENGTH() > 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            V_INSERT, "v", ["'fñoo'"], ["'fñ'", "'fño'"])

def testCreateTableWithColumnWithRegularColumnSerializedSizeBiggerOrEqualThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk int, ck1 int, ck2 int, v text CHECK OCTET_LENGTH() >= 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            V_INSERT, "v", ["'fñoo'", "'fño'"], ["'fñ'"])

def testCreateTableWithColumnWithRegularColumnSerializedSizeSmallerThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk int, ck1 int, ck2 int, v text CHECK OCTET_LENGTH() < 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            V_INSERT, "v", ["'fñ'"], ["'fño'", "'fñoo'"])

def testCreateTableWithColumnWithRegularColumnSerializedSizeSmallerOrEqualThanConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        check_length_constraint(cql, test_keyspace, f"(pk int, ck1 int, ck2 int, v text CHECK OCTET_LENGTH() <= 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})",
            V_INSERT, "v", ["'fñ'", "'fño'"], ["'fñoo'"])

def testCreateTableWithColumnWithRegularColumnSerializedSizeCheckNullTextConstraint(cql, test_keyspace, new_to_cassandra_6):
    check_length_null_constraint(cql, test_keyspace, "OCTET_LENGTH()", "text")

def testCreateTableWithColumnWithRegularColumnSerializedSizeCheckNullVarcharConstraint(cql, test_keyspace, new_to_cassandra_6):
    check_length_null_constraint(cql, test_keyspace, "OCTET_LENGTH()", "varchar")

def testCreateTableWithColumnWithRegularColumnSerializedSizeCheckNullAsciiConstraint(cql, test_keyspace, new_to_cassandra_6):
    check_length_null_constraint(cql, test_keyspace, "OCTET_LENGTH()", "ascii")

def testCreateTableWithColumnWithRegularColumnSerializedSizeCheckNullBlobConstraint(cql, test_keyspace, new_to_cassandra_6):
    check_length_null_constraint(cql, test_keyspace, "OCTET_LENGTH()", "blob")

def testCreateTableWithColumnMixedColumnsSerializedSizeConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        with create_table(cql, test_keyspace, f"(pk text CHECK OCTET_LENGTH() = 4, ck1 int, ck2 int, v text CHECK OCTET_LENGTH() = 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})") as table:
            # Valid
            execute(cql, table, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fño', 2, 3, 'fño')")

            expectedErrorMessage = "Column value does not satisfy value constraint for column 'pk'. It has a length of"
            expectedErrorMessage2 = "Column value does not satisfy value constraint for column 'v'. It has a length of"
            # Invalid
            assert_invalid_message(cql, table, expectedErrorMessage, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fñ', 2, 3, 'fñ')")
            assert_invalid_message(cql, table, expectedErrorMessage2, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fño', 2, 3, 'fñ')")
            assert_invalid_message(cql, table, expectedErrorMessage, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fñ', 2, 3, 'fño')")
            assert_invalid_message(cql, table, expectedErrorMessage, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fñoo', 2, 3, 'fño')")
            assert_invalid_message(cql, table, expectedErrorMessage2, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fño', 2, 3, 'fñoo')")
            assert_invalid_message(cql, table, expectedErrorMessage, "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('fñoo', 2, 3, 'fñoo')")

def testCreateTableWithWrongColumnConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        assert_invalid_create_table(cql, test_keyspace, f"(pk text, ck1 int CHECK OCTET_LENGTH() = 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})")

def testCreateTableWithWrongColumnMultipleConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        assert_invalid_create_table(cql, test_keyspace, f"(pk text, ck1 int CHECK OCTET_LENGTH() = 4 AND ck1 < 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})")

def testCreateTableWithColumnWithClusteringColumnInvalidTypeConstraint(cql, test_keyspace, new_to_cassandra_6):
    for order in ORDERS:
        assert_invalid_create_table(cql, test_keyspace, f"(pk int, ck1 int CHECK OCTET_LENGTH() = 4, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})")

# The original Java test class ends with six more tests -
# testCreateTableWithColumnWithClusteringColumnInvalidScalarTypeConstraint,
# testCreateTableInvalidFunction, the three tests *ConstraintsAndCDCEnabled
# and testCreateTableWithColumnWithClusteringColumnLessThanScalarConstraintIntegerOnLikeTable.
# They don't use OCTET_LENGTH(), and are copies of the tests with the same
# names in CreateTableWithColumnCqlConstraintValidationTest, which we already
# translated in create_table_with_column_cql_constraint_validation_test.py,
# so we don't translate them again here.
