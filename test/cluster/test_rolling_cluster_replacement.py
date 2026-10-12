#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Rolling replacement of every node of a cluster, one node at a time, ported
from scylla-dtest cluster_replacement_test.py: by adding a node and
decommissioning the old one (then cleanup on the other nodes), by replacing
a stopped node, and by removenode of a killed node then adding a new one.
In the single-DC tests the nodes are replaced in the order they were added;
in the two-DC tests the nodes of the second DC are replaced first. Every key
is then read at CL=ONE. The keyspaces use vnodes, and nodetool is used as in
the dtests.
"""
import logging
import time

import pytest
from cassandra import ConsistencyLevel, OperationTimedOut
from cassandra.cluster import EXEC_PROFILE_DEFAULT, NoHostAvailable

from test import DEBUG_MODES
from test.cluster.dtest_helpers import exclusive_one, DTEST_CONFIG, C1C2, create_cf, create_ks, insert_c1c2, nodetool, query_c1c2, \
    topology, wait_for_nodes_status
from test.pylib.scylla_cluster import ReplaceConfig
from test.cluster.util import stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

NUM_KEYS = 10000

# ccm's populate(n), with a node count instead of a topology, configures the
# SimpleSnitch, with every node in datacenter1/rack1.
SIMPLE_SNITCH = {**DTEST_CONFIG, "endpoint_snitch": "SimpleSnitch"}


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


async def insert_data(manager: ScyllaClusterManager, server, rf) -> None:
    """The dtests' _insert_data(): keyspace ks and table cf with NUM_KEYS rows
    written at CL=ALL, through a session to the server alone."""
    cql = await exclusive_one(manager, server)
    create_ks(cql, "ks", rf)
    create_cf(cql, "ks.cf", C1C2)
    insert_c1c2(cql, keys=range(NUM_KEYS), consistency=ConsistencyLevel.ALL)


async def verify_data_integrity(manager: ScyllaClusterManager) -> None:
    """The dtests' _verify_data_integrity(): once `nodetool status` of the last
    node shows every live node UN, every key is read at CL=ONE through a
    session to that node with a 120 s request timeout, retrying timeouts."""
    servers = await manager.running_servers()
    node = servers[-1]
    await wait_for_nodes_status(manager, node, ["UN"] * len(servers), timeout=300)
    cql = manager.con_gen([node.ip_addr]).connect()
    cql.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.ONE
    cql.get_execution_profile(EXEC_PROFILE_DEFAULT).request_timeout = 120
    for key in range(NUM_KEYS):
        for attempt in range(5):
            try:
                query_c1c2(cql, key, consistency=ConsistencyLevel.ONE)
                break
            except (OperationTimedOut, NoHostAvailable):
                if attempt == 4:
                    raise
                time.sleep(2)


async def replace_by_add_and_decommission(manager: ScyllaClusterManager, old, location, config) -> None:
    """The dtests' replace_node_by_add_and_decommission(): a new node is added
    in the old node's location, the old node is decommissioned, then
    `nodetool cleanup` runs on every remaining node."""
    await manager.server_add(config=config, property_file=location)
    await manager.decommission_node(old.server_id)
    for s in await manager.running_servers():
        await nodetool(manager, s, "cleanup")


async def replace_dead_node(manager: ScyllaClusterManager, old, location, config) -> None:
    """RepairBasedNodeOperationsScenarios.replace_node(): the old node is
    stopped, and seen down by the others, then replaced by a new node in its
    location, which logs that the old node was removed."""
    host_id = await manager.get_host_id(old.server_id)
    await manager.server_stop_gracefully(old.server_id)
    await manager.others_not_see_server(old.ip_addr)
    new = await manager.server_add(ReplaceConfig(replaced_id=old.server_id, reuse_ip_addr=False, use_host_id=True),
                                   config=config, property_file=location)
    log = await manager.server_open_log(new.server_id)
    await log.wait_for(rf"gossip - Finished to force remove node ({old.ip_addr}|{host_id})")


async def remove_dead_node_and_add(manager: ScyllaClusterManager, old, location, config) -> None:
    """The dtests' replace_dead_node_by_remove_and_add(): the old node is
    killed, and seen down by the others, `nodetool removenode` of it runs on
    the last running node, then a new node is added in its location."""
    host_id = await manager.get_host_id(old.server_id)
    await manager.server_stop(old.server_id, convict=False)
    await manager.others_not_see_server(old.ip_addr)
    running = await manager.running_servers()
    await nodetool(manager, running[-1], "removenode", host_id)
    await manager.server_add(config=config, property_file=location)


async def rolling_replacement(manager: ScyllaClusterManager, replace, locations, rf, config=DTEST_CONFIG,
                              reverse=False) -> None:
    """Starts nodes in the given locations, writes the data through the first,
    replaces every initial node with `replace` (the last one first if
    `reverse`), then verifies the data."""
    servers = await manager.servers_add(len(locations), config=config, property_file=locations)
    await insert_data(manager, servers[0], rf)
    nodes = list(zip(servers, locations))
    if reverse:
        nodes.reverse()
    for server, location in nodes:
        logger.info("Replacing %s with %s", server, replace.__name__)
        await replace(manager, server, location, config)
    await verify_data_integrity(manager)


# Ported from scylla-dtest cluster_replacement_test.py::TestClusterReplacement::test_rolling_cluster_replacement_sequentially_live_nodes.
# Three nodes (SimpleSnitch, datacenter1/rack1), RF=1: each node is replaced
# by adding a new node and decommissioning it.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_rolling_cluster_replacement_sequentially_live_nodes(manager: ScyllaClusterManager):
    await rolling_replacement(manager, replace_by_add_and_decommission, topology({"rack1": 3}), 1, config=SIMPLE_SNITCH)


# Ported from scylla-dtest cluster_replacement_test.py::TestClusterReplacement::test_rolling_cluster_replacement_sequentially_live_nodes_multi_dc.
# Three nodes in dc1 and three in dc2 (rack RAC1, as ccm's populate([3, 3])
# and new_node() without a rack), RF=1 in each DC: each node is replaced by
# adding a new node and decommissioning it, dc2 first.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_rolling_cluster_replacement_sequentially_live_nodes_multi_dc(manager: ScyllaClusterManager):
    await rolling_replacement(manager, replace_by_add_and_decommission,
                              topology({"RAC1": 3}, dc="dc1") + topology({"RAC1": 3}, dc="dc2"),
                              {"dc1": 1, "dc2": 1}, reverse=True)


# Ported from scylla-dtest cluster_replacement_test.py::TestClusterReplacement::test_rolling_cluster_replacement_sequentially_dead_nodes.
# Three nodes in dc1 (two in rack r1, one in r2), RF=2: each node is stopped
# and replaced.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_rolling_cluster_replacement_sequentially_dead_nodes(manager: ScyllaClusterManager):
    await rolling_replacement(manager, replace_dead_node, topology({"r1": 2, "r2": 1}, dc="dc1"), 2)


# Ported from scylla-dtest cluster_replacement_test.py::TestClusterReplacement::test_rolling_cluster_replacement_sequentially_dead_nodes_multi_dc.
# Two DCs of one node in each of racks r1 and r2, RF=2 in each DC: each node
# is stopped and replaced, dc2 first.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_rolling_cluster_replacement_sequentially_dead_nodes_multi_dc(manager: ScyllaClusterManager):
    await rolling_replacement(manager, replace_dead_node,
                              topology({"r1": 1, "r2": 1}, dc="dc1") + topology({"r1": 1, "r2": 1}, dc="dc2"),
                              {"dc1": 2, "dc2": 2}, reverse=True)


# Ported from scylla-dtest cluster_replacement_test.py::TestClusterReplacement::test_rolling_cluster_replacement_sequentially_dead_nodes_remove_and_add.
# Four nodes in dc1 (two in each of racks r1 and r2), RF=2: each node is
# killed, removed with nodetool removenode, and a new node is added in its
# rack.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_rolling_cluster_replacement_sequentially_dead_nodes_remove_and_add(manager: ScyllaClusterManager):
    await rolling_replacement(manager, remove_dead_node_and_add, topology({"r1": 2, "r2": 2}, dc="dc1"), 2)


# Ported from scylla-dtest cluster_replacement_test.py::TestClusterReplacement::test_rolling_cluster_replacement_sequentially_dead_nodes_remove_and_add_multi_dc.
# Two DCs of two nodes in each of racks r1 and r2, RF=2 in each DC: each
# node is killed, removed with nodetool removenode, and a new node is added
# in its rack, dc2 first.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_rolling_cluster_replacement_sequentially_dead_nodes_remove_and_add_multi_dc(manager: ScyllaClusterManager):
    await rolling_replacement(manager, remove_dead_node_and_add,
                              topology({"r1": 2, "r2": 2}, dc="dc1") + topology({"r1": 2, "r2": 2}, dc="dc2"),
                              {"dc1": 2, "dc2": 2}, reverse=True)
