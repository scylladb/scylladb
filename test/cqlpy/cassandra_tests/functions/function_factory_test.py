# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

from ..porting import *

# Most of the tests in the original Java test (testInvalidNumberOfArguments,
# testSimpleTypes, testSets, testLists, testMaps, testTuples, testUDTs and
# testNestedCalls) were not translated, because they test two functions,
# identity() and tostring(), which accept arguments of any type. The Java test
# adds these functions through Cassandra's internal NativeFunctions API, and
# they don't exist in a normal Cassandra (or Scylla) build. They can't be
# replaced by user-defined functions, because a UDF's arguments have fixed
# types.

def testUnknownFunction(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY)") as table:
        # Cassandra's message is "Unknown function 'unknown'", Scylla's is
        # "Unknown function unknown called".
        assert_invalid_message_re(cql, table, "Unknown function '?unknown'?", "SELECT unknown() FROM %s")
