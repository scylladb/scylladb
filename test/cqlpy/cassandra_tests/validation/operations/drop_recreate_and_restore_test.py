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
from cassandra.protocol import AlreadyExists, ConfigurationException

def tableId(cql, table):
    keyspace, name = table.split(".")
    return cql.execute("SELECT id FROM system_schema.tables WHERE keyspace_name = %s AND table_name = %s", [keyspace, name]).one().id

# The test testCreateWithIdRestore was not translated, because it restores
# the table's data by replaying saved commitlog segments, through internal
# Cassandra APIs.

def testCreateWithIdDuplicate(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, PRIMARY KEY(a, b))") as table:
        id = tableId(cql, table)
        with pytest.raises(AlreadyExists):
            execute(cql, table, f"CREATE TABLE %s (a int, b int, c int, PRIMARY KEY(a, b)) WITH ID = {id}")

def testCreateWithIdInvalid(cql, test_keyspace):
    with pytest.raises(ConfigurationException):
        with create_table(cql, test_keyspace, f"(a int, b int, c int, PRIMARY KEY(a, b)) WITH ID = {55}"):
            pass

def testAlterWithId(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, PRIMARY KEY(a, b))") as table:
        id = tableId(cql, table)
        with pytest.raises(ConfigurationException):
            execute(cql, table, f"ALTER TABLE %s WITH ID = {id}")
