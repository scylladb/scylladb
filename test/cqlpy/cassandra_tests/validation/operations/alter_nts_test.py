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
from ....util import is_scylla
from test.pylib.skip_types import skip_env

def testDropColumnAsPreparedStatement(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(key int PRIMARY KEY, value int)") as table:
        prepared = cql.prepare(f"ALTER TABLE {table} DROP value")
        cql.execute(f"INSERT INTO {table} (key, value) VALUES (1, 1)")
        assert_rows(cql.execute(f"SELECT * FROM {table}"), [1, 1])
        cql.execute(prepared)
        cql.execute(f"ALTER TABLE {table} ADD value int")
        assert_rows(cql.execute(f"SELECT * FROM {table}"), [1, None])

def getWarnings(result):
    return result.response_future.warnings

# This test checks that Cassandra warns when a keyspace's replication factor
# is higher than the number of nodes. Scylla behaves differently, on purpose:
# With tablets (the default), it refuses to create such a keyspace - a
# replication factor higher than the number of racks is an error, not a
# warning - and it doesn't allow SimpleStrategy at all. Without tablets,
# Scylla allows such a keyspace, but doesn't warn about it. So this test is
# skipped on Scylla.
def testCreateAlterKeyspacesRFWarnings(cql, this_dc):
    if is_scylla(cql):
        skip_env("Scylla rejects, rather than warns about, a replication factor higher than the number of nodes")
    # NTS
    ks = unique_name()
    warnings = getWarnings(cql.execute("CREATE KEYSPACE " + ks + " WITH replication = {'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 3 }"))
    try:
        assert len(warnings) == 1
        assert "Your replication factor 3 for keyspace " + ks + " is higher than the number of nodes 1 for datacenter " + this_dc in warnings[0]

        warnings = getWarnings(cql.execute("CREATE TABLE " + ks + ".t (k int PRIMARY KEY, v int)"))
        assert not warnings

        warnings = getWarnings(cql.execute("ALTER KEYSPACE " + ks + " WITH replication = {'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 2 }"))
        assert len(warnings) == 1
        assert "Your replication factor 2 for keyspace " + ks + " is higher than the number of nodes 1 for datacenter " + this_dc in warnings[0]

        warnings = getWarnings(cql.execute("ALTER KEYSPACE " + ks + " WITH replication = {'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 1 }"))
        assert not warnings
    finally:
        cql.execute("DROP KEYSPACE " + ks)

    # SimpleStrategy
    ks = unique_name()
    warnings = getWarnings(cql.execute("CREATE KEYSPACE " + ks + " WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }"))
    try:
        assert len(warnings) == 1
        assert "Your replication factor 3 for keyspace " + ks + " is higher than the number of nodes 1" in warnings[0]

        warnings = getWarnings(cql.execute("CREATE TABLE " + ks + ".t (k int PRIMARY KEY, v int)"))
        assert not warnings

        warnings = getWarnings(cql.execute("ALTER KEYSPACE " + ks + " WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 }"))
        assert len(warnings) == 1
        assert "Your replication factor 2 for keyspace " + ks + " is higher than the number of nodes 1" in warnings[0]

        warnings = getWarnings(cql.execute("ALTER KEYSPACE " + ks + " WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"))
        assert not warnings
    finally:
        cql.execute("DROP KEYSPACE " + ks)

# The test testAlterKeyspaceSystem_AuthWithNTSOnlyAcceptsConfiguredDataCenterNames
# was not translated, because it registers a fake node in a second data
# center through internal Cassandra APIs.
