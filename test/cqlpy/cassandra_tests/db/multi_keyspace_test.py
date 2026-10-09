# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# This is a translation of MultiKeyspaceTest.java from Cassandra's
# test/unit/org/apache/cassandra/db directory.

from ..porting import *
from ...util import new_test_keyspace

# The Java test creates two keyspaces with SimpleStrategy, which Scylla
# doesn't allow with tablets, so we use NetworkTopologyStrategy. It also uses
# fixed keyspace names, and we use unique ones.
def testSameTableNames(cql):
    replication = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
    with new_test_keyspace(cql, replication) as multikstest1, new_test_keyspace(cql, replication) as multikstest2:
        cql.execute(f"CREATE TABLE {multikstest1}.standard1 (a int PRIMARY KEY, b int)")
        cql.execute(f"CREATE TABLE {multikstest2}.standard1 (a int PRIMARY KEY, b int)")

        cql.execute(f"INSERT INTO {multikstest1}.standard1 (a, b) VALUES (0, 0)")
        cql.execute(f"INSERT INTO {multikstest2}.standard1 (a, b) VALUES (0, 0)")

        flush(cql, f"{multikstest1}.standard1")
        flush(cql, f"{multikstest2}.standard1")

        assertRows(cql.execute(f"SELECT * FROM {multikstest1}.standard1"),
                   row(0, 0))
        assertRows(cql.execute(f"SELECT * FROM {multikstest2}.standard1"),
                   row(0, 0))
