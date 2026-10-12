#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Rebuild of many keyspaces in the same DC, ported from scylla-dtest
rebuild_test.py: 50 vnodes keyspaces of two tables each are written on
one node, a second node joins the DC, the keyspaces' RF goes up to 2, the
new node rebuilds (nodetool rebuild), and once the first node is killed
the new node alone returns every key.
"""
import pytest
from cassandra import ConsistencyLevel

from test.cluster.dtest_helpers import exclusive_one, DTEST_CONFIG, C1C2, create_cf, create_ks, insert_c1c2, nodetool, query_c1c2
from test.cluster.util import stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

CONFIG = {**DTEST_CONFIG, "endpoint_snitch": "GossipingPropertyFileSnitch"}
LOCATION = {"dc": "dc1", "rack": "rack1"}


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)



def check_data(cql, keyspaces, tables, keys, cl=ConsistencyLevel.ALL):
    errors = 0
    for ks in keyspaces:
        for cf in tables:
            for i in keys:
                try:
                    query_c1c2(cql, i, ks=ks, cf=cf, consistency=cl)
                except AssertionError:
                    errors += 1
    assert errors == 0, f"Found {errors} errors"


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_rebuild_many_keyspaces(manager: ScyllaClusterManager):
    num_keys, num_keyspaces, num_tables = 100, 50, 2
    node1 = await manager.server_add(config=CONFIG, property_file=LOCATION)
    cql = await exclusive_one(manager, node1)
    keyspaces = [f"ks_{i:04d}" for i in range(num_keyspaces)]
    tables = [f"cf_{i:04d}" for i in range(num_tables)]
    for ks in keyspaces:
        create_ks(cql, ks, {"dc1": 1})
    for ks in keyspaces:
        for cf in tables:
            create_cf(cql, f"{ks}.{cf}", C1C2)
            insert_c1c2(cql, keys=range(num_keys), ks=ks, cf=cf, consistency=ConsistencyLevel.ALL)
    keys = range(num_keys)
    check_data(cql, keyspaces, tables, keys)

    node2 = await manager.server_add(config=CONFIG, property_file=LOCATION)
    cql = await exclusive_one(manager, node2)
    for ks in keyspaces:
        cql.execute(f"ALTER KEYSPACE {ks} WITH REPLICATION = {{ 'class':'NetworkTopologyStrategy', 'dc1':2 }};")
    await nodetool(manager, node2, "rebuild")
    # Read right after the kill, as the dtest does: until node2 sees node1
    # down, its reads still send digest requests to node1.
    await manager.server_stop(node1.server_id, convict=False)
    check_data(cql, keyspaces, tables, keys, cl=ConsistencyLevel.ONE)
