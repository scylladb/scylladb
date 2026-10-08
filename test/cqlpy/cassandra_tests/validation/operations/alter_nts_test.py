# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1


from ...porting import *

def testDropColumnAsPreparedStatement(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(key int PRIMARY KEY, value int)") as table:
        prepared = cql.prepare(f"ALTER TABLE {table} DROP value")
        cql.execute(f"INSERT INTO {table} (key, value) VALUES (1, 1)")
        assert_rows(cql.execute(f"SELECT * FROM {table}"), [1, 1])
        cql.execute(prepared)
        cql.execute(f"ALTER TABLE {table} ADD value int")
        assert_rows(cql.execute(f"SELECT * FROM {table}"), [1, None])
