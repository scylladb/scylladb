# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2020-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

from ...porting import create_table, execute, assert_invalid

# Check dates are correctly recognized and validated,
# migrated from cql_tests.py:TestCQL.date_test()
def testDate(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, t timestamp)") as table:
        execute(cql, table, "INSERT INTO %s (k, t) VALUES (0, '2011-02-03')");
        assert_invalid(cql, table, "INSERT INTO %s (k, t) VALUES (0, '2011-42-42')");
