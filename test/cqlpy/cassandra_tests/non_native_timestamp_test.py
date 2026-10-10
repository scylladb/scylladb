# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

from .porting import *

# The original Java test runs the statements through Cassandra's internal
# (non-CQL-native) query path, which, unlike the native protocol, doesn't get
# timestamps from the client. Through the Python driver we can only test the
# native protocol, but the test's expectations still hold.
def testsetServerTimestampForNonCqlNativeStatements(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, v int)") as table:
        execute(cql, table, "INSERT INTO %s (k, v) values (1, ?)", 2)

        row = execute(cql, table, "SELECT v, writetime(v) AS wt FROM %s WHERE k = 1").one()
        assert row.v == 2
        timestamp1 = row.wt
        assert timestamp1 != -1

        # per CASSANDRA-8246 the two updates will have the same (incorrect)
        # timestamp, so reconcilliation is by value and the "older" update wins
        execute(cql, table, "INSERT INTO %s (k, v) values (1, ?)", 1)
        row = execute(cql, table, "SELECT v, writetime(v) AS wt FROM %s WHERE k = 1").one()
        assert row.v == 1
        assert row.wt > timestamp1
