# This file was translated from the original Java code from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# This is a translation of CqlConstraintValidationTester.java from Cassandra's
# test/unit/org/apache/cassandra/constraints directory, along with some
# helper functions shared by the tests in this directory.

import random
from ..porting import *

# Several of the original Java test classes are parameterized by the
# clustering order of ck1, ASC or DESC. We loop over both orders in each
# test that uses this order. Some tests don't use the order parameter at
# all (they hard-code ASC), so we run them just once.
ORDERS = ["ASC", "DESC"]

# The original tests use QuickTheories to check random values from a range.
# To keep the tests fast, we check both ends of the range and just a few
# random values between them.
def integers(lo, hi):
    return [lo, hi] + random.sample(range(lo + 1, hi), min(5, hi - lo - 1))

def doubles(lo, hi):
    return [lo, hi] + [random.uniform(lo, hi) for _ in range(5)]

# The original tests compare the full DESCRIBE TABLE output, including all
# the table's options with their default values (CqlConstraintValidationTester's
# tableParametersCql()). These options differ
# between Cassandra versions and between Cassandra and Scylla, and are not
# relevant to constraints, so we only compare the beginning of the output -
# the column definitions, primary key and clustering order.
def assert_describe_starts_with(cql, table, expected):
    keyspace, name = table.split(".")
    res = list(cql.execute(f"DESCRIBE TABLE {table}"))
    assert len(res) == 1
    assert (res[0].keyspace_name, res[0].type, res[0].name) == (keyspace, "table", name)
    assert res[0].create_statement.startswith(expected)

# Many LENGTH() and OCTET_LENGTH() tests have the same structure: create a
# table with such a constraint on one column, check that inserting the
# "valid" values into that column works, and inserting the "invalid" ones
# fails.
# "schema" is the table's schema, and "insert" is an INSERT statement with
# a {} where the tested value goes.
def check_length_constraint(cql, test_keyspace, schema, insert, column, valid, invalid):
    with create_table(cql, test_keyspace, schema) as table:
        # Valid
        for value in valid:
            execute(cql, table, insert.format(value))
        expectedErrorMessage = f"Column value does not satisfy value constraint for column '{column}'. It has a length of"
        # Invalid
        for value in invalid:
            assert_invalid_message(cql, table, expectedErrorMessage, insert.format(value))

CK1_INSERT = "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, {}, 2, 3)"

PK_INSERT = "INSERT INTO %s (pk, ck1, ck2, v) VALUES ({}, 1, 2, 3)"

V_INSERT = "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, {})"

# A LENGTH() or OCTET_LENGTH() constraint makes the column implicitly NOT NULL
def check_length_null_constraint(cql, test_keyspace, function, typ):
    for order in ORDERS:
        with create_table(cql, test_keyspace, f"(pk int, ck1 int, ck2 int, v {typ} CHECK {function} <= 4, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 {order})") as table:
            expectedErrorMessage = "Column value does not satisfy value constraint for column 'v' as it is null."
            assert_invalid_message(cql, table, expectedErrorMessage, "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 3, null)")

# In the original tests, CREATE TABLE failures are wrapped by the test
# framework in an exception with "Error setting schema for test",
# and the tests check that message, which is not interesting to us. We only
# check that an InvalidRequest is returned (and when the original test also
# checks the cause's message, we check that message).
def assert_invalid_create_table(cql, test_keyspace, schema, message=None):
    table = test_keyspace + "." + unique_name()
    if message:
        assert_invalid_message(cql, table, message, "CREATE TABLE %s " + schema)
    else:
        assert_invalid_throw(cql, table, InvalidRequest, "CREATE TABLE %s " + schema)
