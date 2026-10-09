# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# This is a translation of CreateTableWithColumnNotNullConstraintInvalidTest.java
# from Cassandra's test/unit/org/apache/cassandra/constraints directory.
#
# Column constraints (CHECK, NOT NULL) were added in Cassandra 6
# (CEP-42, CASSANDRA-19947), so these tests are marked new_to_cassandra_6.
# Scylla doesn't support constraints yet, so all of them are xfail.

from ..porting import *
from .cql_constraint_validation_tester import *

pytestmark = pytest.mark.xfail(reason="SCYLLADB-5234")

# The original Java test is parameterized by all the native types except
# counter, empty and duration. We loop over them instead.
TYPES = ["ascii", "bigint", "blob", "boolean", "date", "decimal", "double",
         "float", "inet", "int", "smallint", "text", "time", "timestamp",
         "timeuuid", "tinyint", "uuid", "varchar", "varint"]

def testCreateTableWithColumnNotNullCheckNonExisting(cql, test_keyspace, new_to_cassandra_6):
    for typeString in TYPES:
        with create_table(cql, test_keyspace, f"(pk int, ck1 {typeString} CHECK NOT NULL, ck2 int, v int, PRIMARY KEY (pk))") as table:
            # Invalid
            assert_invalid_message(cql, table, "Column 'ck1' has to be specified as part of this query.", "INSERT INTO %s (pk, ck2, v) VALUES (1, 2, 3)")

            assert_invalid_message(cql, table, "Column value does not satisfy value constraint for column 'ck1' as it is null.", "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, null, 2, 3)")
            assert_invalid_message(cql, table, "Column 'ck1' can not be set to null.", "DELETE ck1 FROM %s WHERE pk = 1")

def testInvalidSpecificationOfNotNullConstraintOnPrimaryKeys(cql, test_keyspace, new_to_cassandra_6):
    for typeString in TYPES:
        assert_invalid_create_table(cql, test_keyspace, f"(pk {typeString} CHECK NOT NULL PRIMARY KEY)",
            "NOT_NULL constraint can not be specified on a partition key column 'pk'")

        assert_invalid_create_table(cql, test_keyspace, f"(pk int, cl {typeString} CHECK NOT NULL, PRIMARY KEY (pk, cl))",
            "NOT_NULL constraint can not be specified on a clustering key column 'cl'")
