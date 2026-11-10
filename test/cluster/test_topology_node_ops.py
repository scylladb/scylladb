#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Decommission and removenode on a cluster of three nodes in three racks,
ported from scylla-dtest topology_test.py: a decommissioned node cannot
rejoin, a node restarted again and again while another node decommissions
leaves no null entry in nodetool status, and removenode of a node that is
alive, or was killed a moment ago, is rejected. Node operations go through
nodetool, as in the dtests.
"""
import asyncio
import logging
import re
import time

import pytest
from cassandra import ConsistencyLevel

from test import DEBUG_MODES
from test.cluster.dtest_helpers import cql_one, exclusive_one, DTEST_CONFIG, C1C2, create_cf, create_ks, flush_all, insert_c1c2, nodetool, \
    topology, wait_for_nodes_status
from test.cluster.util import stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

REMOVENODE_REJECT_MSG = r"Rejected removenode operation.*the node being removed is alive, maybe you should use decommission instead"
THREE_RACKS = topology({"rack1": 1, "rack2": 1, "rack3": 1})


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)



@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_decommissioned_node_cant_rejoin(manager: ScyllaClusterManager):
    """A decommissioned node refuses to start again. CASSANDRA-8801."""
    servers = await manager.servers_add(3, config=DTEST_CONFIG, property_file=THREE_RACKS)
    node3 = servers[2]
    await manager.decommission_node(node3.server_id)
    await manager.server_start(node3.server_id, expected_error="This node was decommissioned and will not rejoin the ring")


@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_crash_during_decommission(manager: ScyllaClusterManager, build_mode: str):
    """
    A node killed and restarted, again and again, while another node is being
    decommissioned converges in nodetool status: the decommissioned node is
    gone if the decommission succeeded, else UN, and every other node is UN,
    with no node in an unknown state. CASSANDRA-10231. Decommission uses
    repair-based node operations without the small table optimization,
    which makes it slower, as in the dtest.
    """
    config = {**DTEST_CONFIG, "enable_repair_based_node_ops": True,
              "allowed_repair_based_node_ops": "replace,removenode,rebuild,bootstrap,decommission",
              "enable_small_table_optimization_for_rbno": False}
    manager.ignore_log_patterns += [
        r"decommission.*failed",
        r"raft_topology - Decommission failed",
        r"raft_topology - .* failed with seastar::rpc::closed_error",
        r"raft::request_aborted",
    ]
    node1, node2, node3 = await manager.servers_add(3, config=config, property_file=THREE_RACKS)
    log1 = await manager.server_open_log(node1.server_id)
    mark1 = await log1.mark()
    decommission = asyncio.create_task(nodetool(manager, node1, "decommission", check=False))

    async def status(node):
        return (await nodetool(manager, node, "status")).stdout

    counter = 0
    while not decommission.done():
        counter += 1
        if " null " in await status(node2):
            break
        await manager.server_stop(node2.server_id, convict=False)
        if counter < 10:
            await manager.server_start(node2.server_id)
        else:
            # After 10 kills, wait for the decommission to fail before restarting.
            await wait_for_nodes_status(manager, node3, ["UN", "DN", "UN"])
            await manager.server_start(node2.server_id)
            break
    # Wait for the decommission on both loop exits: whether it succeeded is
    # final only once it ended.
    await asyncio.wait([decommission], timeout=600 if build_mode in DEBUG_MODES else 300)
    assert decommission.done(), "decommission still running"
    # The dtest's DecommissionInParallel: nodetool decommission succeeded and
    # node1 logged DECOMMISSIONED.
    succeeded = decommission.result().returncode == 0
    if succeeded:
        await log1.wait_for("DECOMMISSIONED", from_mark=mark1)
    logger.info("decommission succeeded: %s", succeeded)
    await status(node2)

    # node2 must converge: node1 gone if the decommission succeeded, else UN;
    # no row in an unknown ("?") state, so the rows are parsed from the raw
    # output.
    timeout = 300 if build_mode in DEBUG_MODES else 120
    deadline = time.time() + timeout
    while True:
        out = await status(node2)
        statuses = {addr: st for st, addr in re.findall(r"^(\S+)\s+(\d+\.\d+\.\d+\.\d+)\s", out, re.M)}
        node1_ok = node1.ip_addr not in statuses if succeeded else statuses.get(node1.ip_addr) == "UN"
        others_ok = {node2.ip_addr, node3.ip_addr} <= statuses.keys() and all(st == "UN" for st in statuses.values())
        if node1_ok and others_ok:
            break
        assert time.time() < deadline, f"node2 status did not converge in {timeout}s (decommission succeeded: {succeeded}):\n{out}"
        await asyncio.sleep(0.25)


async def prepare_cluster(manager: ScyllaClusterManager, property_file=THREE_RACKS, rf=3):
    """The dtests' prepare_cluster(): the nodes (by default three in three
    racks), a keyspace with the RF (3) and 1000 rows written at QUORUM,
    flushed on every node."""
    servers = await manager.servers_add(len(property_file), config=DTEST_CONFIG, property_file=property_file)
    cql = cql_one(manager)
    create_ks(cql, "ks", rf)
    create_cf(cql, "ks.cf", C1C2)
    insert_c1c2(await exclusive_one(manager, servers[0]), keys=range(1000), consistency=ConsistencyLevel.QUORUM)
    await flush_all(manager, servers)
    return servers


async def removenode_rejected(manager: ScyllaClusterManager, node, remove_host_id):
    log = await manager.server_open_log(node.server_id)
    mark = await log.mark()
    res = await nodetool(manager, node, "removenode", remove_host_id, check=False)
    assert res.returncode != 0, res.stdout
    assert await log.grep(REMOVENODE_REJECT_MSG, from_mark=mark), "Removenode reject message was not found in logs"


@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_remove_node_alive(manager: ScyllaClusterManager):
    """removenode of a live node is rejected."""
    servers = await prepare_cluster(manager)
    manager.ignore_log_patterns += [REMOVENODE_REJECT_MSG]
    await removenode_rejected(manager, servers[0], await manager.get_host_id(servers[-1].server_id))


@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_remove_node_alive_in_gossip(manager: ScyllaClusterManager):
    """removenode of a node killed a moment ago, which the other nodes still
    consider alive, is rejected."""
    servers = await prepare_cluster(manager)
    manager.ignore_log_patterns += [REMOVENODE_REJECT_MSG]
    remove = servers[-1]
    host_id = await manager.get_host_id(remove.server_id)
    await manager.server_stop(remove.server_id, convict=False)
    await removenode_rejected(manager, servers[0], host_id)


# Ported from scylla-dtest topology_test.py::TestTopology::test_removenode_rejected_before_decommision_node.
# Four nodes (two in rack1, two in rack2), RF=2: removenode of the live last
# node is rejected (logged on the initiator); after the node is
# decommissioned, removenode of its host ID fails too.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_removenode_rejected_before_decommision_node(manager: ScyllaClusterManager):
    servers = await prepare_cluster(manager, topology({"rack1": 2, "rack2": 2}), rf=2)
    manager.ignore_log_patterns += [REMOVENODE_REJECT_MSG]
    remove = servers[-1]
    host_id = await manager.get_host_id(remove.server_id)
    node = servers[0]
    log = await manager.server_open_log(node.server_id)
    mark = await log.mark()
    res = await nodetool(manager, node, "removenode", host_id, check=False)
    assert res.returncode != 0, res.stdout
    assert await log.grep(REMOVENODE_REJECT_MSG, from_mark=mark), "Removenode error message was not found in logs"
    await nodetool(manager, remove, "decommission")
    res = await nodetool(manager, node, "removenode", host_id, check=False)
    assert res.returncode != 0, res.stdout
