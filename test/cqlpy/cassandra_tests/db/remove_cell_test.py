# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# This is a translation of RemoveCellTest.java from Cassandra's
# test/unit/org/apache/cassandra/db directory.

from ..porting import *

def testDeleteCell(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, PRIMARY KEY (a, b))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP ?", 0, 0, 0, 0)
        flush(cql, table)
        execute(cql, table, "DELETE c FROM %s USING TIMESTAMP ? WHERE a = ? AND b = ?", 1, 0, 0)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND b = ?", 0, 0), row(0, 0, None))
        assertRows(execute(cql, table, "SELECT c FROM %s WHERE a = ? AND b = ?", 0, 0), row(None))
