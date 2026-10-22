#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging
import random
import sys
import time

import pytest

from test.cluster.util import new_test_keyspace, wait_for_token_ring_and_group0_consistency
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

INITIAL_NODES = 100
BATCH_SIZE = 10

MIN_CLUSTER_NODES = 50
MAX_CLUSTER_NODES = 300
RANDOM_OPERATION_MIN = 50
RANDOM_OPERATION_MAX = 150
TOPOLOGY_ITERATIONS = 5

CMDLINE = [
    '--auto-repair-enabled-default', '1',
    '--auto-repair-threshold-default-in-seconds', '1',
    '--smp', '2',
    '-m', '10G',
    '--failure-detector-timeout-in-ms', '120000',
    '--direct-failure-detector-ping-timeout-in-ms', '20000',
    '--read-request-timeout-in-ms', '100000',
    '--write-request-timeout-in-ms', '100000',
    '--request-timeout-in-ms', '300000',
]


def init_random_seed() -> None:
    """Initialize and log the random seed so a failing run can be reproduced."""
    seed = random.randrange(sys.maxsize)
    random.seed(seed)
    logger.info("Random seed: %s", seed)


async def add_nodes(manager: ScyllaClusterManager, count: int):
    """Bootstrap count nodes in parallel batches of BATCH_SIZE.

    Bootstrapping many nodes at once can overwhelm the host.
    """
    servers = []
    while len(servers) < count:
        batch = min(BATCH_SIZE, count - len(servers))
        logger.info(f"Starting concurrent bootstrap of {batch} nodes")
        new_servers = await manager.servers_add(batch, cmdline=CMDLINE)
        logger.info(f"All nodes joined: {[s.server_id for s in new_servers]}")
        servers += new_servers
    return servers


async def grow_cluster(manager: ScyllaClusterManager, target_nodes: int):
    """Bootstrap a cluster of target_nodes: INITIAL_NODES at once, then the rest in batches of BATCH_SIZE."""
    initial = min(INITIAL_NODES, target_nodes)
    logger.info(f"Starting concurrent bootstrap of {initial} nodes")
    servers = await manager.servers_add(initial, cmdline=CMDLINE)
    return servers + await add_nodes(manager, target_nodes - initial)


async def check_cluster_health(manager: ScyllaClusterManager) -> None:
    """Fail unless all nodes agree on the token ring and group 0 members, and the cluster serves a write and a read."""
    servers = await manager.running_servers()
    # The check polls the nodes one by one under a single deadline, so scale it with the cluster size.
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 10 * len(servers))

    cql = manager.get_cql()
    # All nodes are in one rack, so RF=1 is the only RF-rack-valid replication factor.
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int)")
        await cql.run_async(f"INSERT INTO {ks}.t (pk, v) VALUES (0, 1)")
        rows = await cql.run_async(f"SELECT v FROM {ks}.t WHERE pk = 0")
        assert [row.v for row in rows] == [1]


@pytest.mark.non_gating
async def test_scale(manager: ScyllaClusterManager):
    """Reproducer: segfault on joining nodes when tablets and automatic
    incremental repair are both enabled on a fresh cluster.

    Start a small cluster, then repeatedly add nodes in batches until the
    cluster reaches the target size.
    """
    await grow_cluster(manager, MAX_CLUSTER_NODES)
    await check_cluster_health(manager)


def _pick_operation_size(room: int) -> int:
    """Pick a random size in [RANDOM_OPERATION_MIN, RANDOM_OPERATION_MAX], capped by the room left within the cluster size bounds."""
    high = min(RANDOM_OPERATION_MAX, room)
    return random.randint(min(RANDOM_OPERATION_MIN, high), high)


@pytest.mark.non_gating
async def test_scale_decommission_then_bootstrap_cycle(manager: ScyllaClusterManager):
    """Cycle topology operations under scale: sequential decommission, then batched bootstrap.

    Grow the cluster to MAX_CLUSTER_NODES (300), then repeat TOPOLOGY_ITERATIONS times:
      1. Pick a random number of random nodes and decommission them one by one.
      2. Pick a random number of nodes and bootstrap them in parallel batches of
         BATCH_SIZE (10).

    The chosen counts keep the cluster size within [MIN_CLUSTER_NODES, MAX_CLUSTER_NODES]
    (50..300). Finally, restore the cluster to MAX_CLUSTER_NODES and check its health.
    """
    init_random_seed()

    servers = await grow_cluster(manager, MAX_CLUSTER_NODES)
    logger.info(f"Initial cluster ready with {len(servers)} nodes")

    for iteration in range(1, TOPOLOGY_ITERATIONS + 1):
        decommission_count = _pick_operation_size(len(servers) - MIN_CLUSTER_NODES)
        logger.info(
            f"Iteration {iteration}/{TOPOLOGY_ITERATIONS}: decommission {decommission_count} nodes one-by-one "
            f"(cluster size before operation: {len(servers)})"
        )
        for _ in range(decommission_count):
            server = servers.pop(random.randrange(len(servers)))
            await manager.decommission_node(server.server_id)

        bootstrap_count = _pick_operation_size(MAX_CLUSTER_NODES - len(servers))
        logger.info(
            f"Iteration {iteration}/{TOPOLOGY_ITERATIONS}: bootstrap {bootstrap_count} nodes in parallel "
            f"(batch={BATCH_SIZE}, cluster size before operation: {len(servers)})"
        )
        servers += await add_nodes(manager, bootstrap_count)

    logger.info(f"Restoring cluster to initial size (cluster size before operation: {len(servers)})")
    await add_nodes(manager, MAX_CLUSTER_NODES - len(servers))
    await check_cluster_health(manager)
