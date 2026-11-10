#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Dropping and re-adding a column on a one-node cluster, ported from
# scylla-dtest schema_test.py: the values of the dropped column are purged
# by "nodetool flush" and "nodetool compact" rather than resurrected.

from collections import Counter

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import EXEC_PROFILE_DEFAULT

from test import DEBUG_MODES
from test.cluster.util import run_nodetool, stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


def connect(manager: ScyllaClusterManager, server):
    """A session to the node, like the dtests' patient_cql_connection():
    statements default to consistency level ONE."""
    cql = manager.con_gen([server.ip_addr]).connect()
    cql.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.ONE
    return cql


async def prepare(manager: ScyllaClusterManager):
    """The dtests' TestSchema.prepare(): one node in datacenter1/rack1 and
    keyspace ks with RF 1, on vnodes."""
    server = await manager.server_add(config={"tablets_mode_for_new_keyspaces": "disabled"},
                                      property_file={"dc": "datacenter1", "rack": "rack1"})
    cql = connect(manager, server)
    # It is forbidden to re-add a column with client-side timestamps
    cql.use_client_timestamp = False
    cql.execute("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':1}")
    cql.execute("USE ks")
    return server, cql


# Ported from scylla-dtest schema_test.py::TestSchema::test_drop_column_compaction.
# Values of a column dropped and re-added while still in the memtable stay
# hidden after a flush and a major compaction of all keyspaces.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_drop_column_compaction(manager: ScyllaClusterManager):
    server, cql = await prepare(manager)
    cql.execute("CREATE TABLE cf (key int PRIMARY KEY, c1 int, c2 int)")

    # insert some data.
    cql.execute("INSERT INTO cf (key, c1, c2) VALUES (0, 1, 2)")
    cql.execute("INSERT INTO cf (key, c1, c2) VALUES (1, 2, 3)")
    cql.execute("INSERT INTO cf (key, c1, c2) VALUES (2, 3, 4)")

    # drop and readd c1.
    cql.execute("ALTER TABLE cf DROP c1")
    cql.execute("ALTER TABLE cf ADD c1 int")

    # add another row.
    cql.execute("INSERT INTO cf (key, c1, c2) VALUES (3, 4, 5)")

    await run_nodetool(manager, server, "flush")
    await run_nodetool(manager, server, "compact")

    # test that c1 values have been compacted away.
    cql.shutdown()
    cql = connect(manager, server)
    assert Counter(cql.execute("SELECT c1 FROM ks.cf")) == Counter([(None,), (None,), (None,), (4,)])
    cql.shutdown()
