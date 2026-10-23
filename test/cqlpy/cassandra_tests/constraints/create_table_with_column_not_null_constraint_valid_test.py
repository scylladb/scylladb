# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# This is a translation of CreateTableWithColumnNotNullConstraintValidTest.java
# from Cassandra's test/unit/org/apache/cassandra/constraints directory.
#
# Column constraints (CHECK, NOT NULL) were added in Cassandra 6
# (CEP-42, CASSANDRA-19947), so these tests are marked new_to_cassandra_6.
# Scylla doesn't support constraints yet, so all of them are xfail.

from ..porting import *
from .cql_constraint_validation_tester import *

pytestmark = pytest.mark.xfail(reason="SCYLLADB-5234")

# The original Java test is parameterized by all the native numeric types
# (except counter), with the value 123, and all the native string types,
# with the value 'fooo'. We loop over them instead.
TYPES_AND_VALUES = [(t, 123) for t in ["bigint", "decimal", "double", "float", "int", "smallint", "tinyint", "varint"]] + \
                   [(t, "'fooo'") for t in ["ascii", "text", "varchar"]]

def testCreateTableWithColumnNotNullCheckValid(cql, test_keyspace, new_to_cassandra_6):
    for typeString, value in TYPES_AND_VALUES:
        with create_table(cql, test_keyspace, f"(pk int, ck1 {typeString} CHECK NOT NULL, ck2 int, v int, PRIMARY KEY (pk))") as table:
            # Valid
            execute(cql, table, f"INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, {value}, 2, 3)")

            # Invalid
            assert_invalid_message(cql, table, "Column 'ck1' has to be specified as part of this query.", "INSERT INTO %s (pk, ck2, v) VALUES (1, 2, 3)")
