# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1


# SELECT statement tests that require a ByteOrderedPartitioner
#
# Scylla doesn't support ByteOrderedPartitioner, and the Cassandra node we
# test against uses Murmur3Partitioner, and neither can be changed through
# CQL. So the tests below were translated only where their results don't
# depend on the order of tokens: Where a result only depends on the order of
# rows from different partitions, we check it ignoring the order; Checks
# whose results depend on the order of tokens are commented out; And tests
# where all checks depend on the order of tokens were not translated.

from ...porting import *

# For a token() call with the wrong number of arguments, Scylla's error
# message is about the token() function's number of arguments, e.g.,
# "Invalid number of arguments in call to function system.token: 1 required
# but 2 provided", instead of Cassandra's messages below.
SCYLLA_TOKEN_ARGUMENTS_MESSAGE = "Invalid number of arguments in call to function system.token"
ONLY_PARTITION_KEY_MESSAGE = re.escape("The token() function must contains only partition key components") + "|" + SCYLLA_TOKEN_ARGUMENTS_MESSAGE
ALL_OR_NONE_MESSAGE = re.escape("The token() function must be applied to all partition key components or none of them") + "|" + SCYLLA_TOKEN_ARGUMENTS_MESSAGE
# For token(b, a), where a is an int and b is a text, Scylla's error message
# is about the type mismatch ("b cannot be passed as argument 0 of function
# system.token of type int") instead of the argument order. When the types
# match, Scylla gives the same message as Cassandra.
ARGUMENTS_ORDER_MESSAGE = re.escape("The token function arguments must be in the partition key order: a, b") + "|" + re.escape("b cannot be passed as argument 0 of function system.token")

# The tests testTokenAndIndex, testFilteringOnAllPartitionKeysWithTokenRestriction,
# testFilteringOnPartitionKeyWithToken and testTokenAndCollections were not
# translated, because their results depend on the order of tokens of a
# ByteOrderedPartitioner.

def testTokenFunctionWithSingleColumnPartitionKey(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int PRIMARY KEY, b text)") as table:
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (0, 'a')")

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE token(a) >= token(?)", 0), row(0, "a"))
        # The following two checks depend on the order of tokens:
        #assert_rows(execute(cql, table, "SELECT * FROM %s WHERE token(a) >= token(?) and token(a) < token(?)", 0, 1), row(0, "a"))
        #assert_rows(execute(cql, table, "SELECT * FROM %s WHERE token(a) BETWEEN token(?) and token(?)", 0, 1), row(0, "a"))
        # Not translated because the Python driver refuses to send incorrect
        # parameters for prepared statements:
        #assert_invalid(cql, table, "SELECT * FROM %s WHERE token(a) > token(?)", "a")
        assert_invalid_message_re(cql, table, ONLY_PARTITION_KEY_MESSAGE,
                             "SELECT * FROM %s WHERE token(a, b) >= token(?, ?)", "b", 0)
        # Scylla does allow multiple restrictions on the same column, including
        # token(a), so the following checks are commented out. The correctness
        # of such queries is tested in
        # test_filtering.py::test_multiple_restrictions_on_same_column
        #assert_invalid_message(cql, table, "More than one restriction was found for the start bound on a",
        #                     "SELECT * FROM %s WHERE token(a) >= token(?) and token(a) >= token(?)", 0, 1)
        #assert_invalid_message(cql, table, "a cannot be restricted by more than one relation if it includes an Equal",
        #                     "SELECT * FROM %s WHERE token(a) >= token(?) and token(a) = token(?)", 0, 1)
        assert_invalid_syntax(cql, table, "SELECT * FROM %s WHERE token(a) = token(?) and token(a) IN (token(?))", 0, 1)

        #assert_invalid_message(cql, table, "More than one restriction was found for the start bound on a",
        #                     "SELECT * FROM %s WHERE token(a) > token(?) AND token(a) > token(?)", 1, 2)
        #assert_invalid_message(cql, table, "More than one restriction was found for the start bound on a",
        #                     "SELECT * FROM %s WHERE token(a) > token(?) AND token(a) BETWEEN token(?) AND token(?)", 1, 2, 3)
        #assert_invalid_message(cql, table, "More than one restriction was found for the end bound on a",
        #                     "SELECT * FROM %s WHERE token(a) <= token(?) AND token(a) < token(?)", 1, 2)
        #assert_invalid_message(cql, table, "More than one restriction was found for the end bound on a",
        #                     "SELECT * FROM %s WHERE token(a) <= token(?) AND token(a) BETWEEN token(?) AND token(?)", 1, 2, 3)
        #assert_invalid_message(cql, table, "a cannot be restricted by more than one relation if it includes an Equal",
        #                     "SELECT * FROM %s WHERE token(a) > token(?) AND token(a) = token(?)", 1, 2)
        #assert_invalid_message(cql, table, "a cannot be restricted by more than one relation if it includes an Equal",
        #                     "SELECT * FROM %s WHERE  token(a) = token(?) AND token(a) > token(?)", 1, 2)
        #assert_invalid_message(cql, table, "a cannot be restricted by more than one relation if it includes an Equal",
        #                     "SELECT * FROM %s WHERE  token(a) = token(?) AND token(a) BETWEEN token(?) AND token(?)", 1, 2, 3)

def testTokenFunctionWithPartitionKeyAndClusteringKeyArguments(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b text, PRIMARY KEY (a, b))") as table:
        assert_invalid_message_re(cql, table, ONLY_PARTITION_KEY_MESSAGE,
                             "SELECT * FROM %s WHERE token(a, b) > token(0, 'c')")
