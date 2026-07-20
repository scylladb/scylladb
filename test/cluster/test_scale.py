#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging
import random
import sys

import pytest

from test.pylib.manager_client import ManagerClient

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
    '--direct_failure_detector_ping_timeout_in_ms', '20000',
    '--read-request-timeout-in-ms', '100000',
    '--write-request-timeout-in-ms', '100000',
    '--request-timeout-in-ms', '300000',
    # '--max-networking-io-control-blocks', '100',
]


def init_random_seed() -> None:
    """Initialize and log the random seed so a failing run can be reproduced."""
    seed = random.randrange(sys.maxsize)
    random.seed(seed)
    logger.info("Random seed: %s", seed)


async def grow_cluster(manager: ManagerClient, target_nodes: int):
    """Bootstrap a cluster up to target_nodes, growing in batches of BATCH_SIZE.

    Bootstrapping all nodes at once can overwhelm the host, so the cluster is
    grown incrementally: start with INITIAL_NODES and then add nodes in batches
    of BATCH_SIZE until reaching target_nodes.
    """
    initial = min(INITIAL_NODES, target_nodes)
    logger.info(f"Starting concurrent bootstrap of {initial} nodes")
    servers = await manager.servers_add(initial, cmdline=CMDLINE)
    logger.info(f"All nodes joined: {[s.server_id for s in servers]}")

    while len(servers) < target_nodes:
        batch = min(BATCH_SIZE, target_nodes - len(servers))
        logger.info(f"Starting concurrent bootstrap of {batch} nodes (current size: {len(servers)})")
        new_servers = await manager.servers_add(batch, cmdline=CMDLINE)
        logger.info(f"All nodes joined: {[s.server_id for s in new_servers]}")
        servers += new_servers

    return servers


@pytest.mark.check_nodes_for_errors
async def test_scale(manager: ManagerClient):
    """Reproducer: segfault on joining nodes when tablets and automatic
    incremental repair are both enabled on a fresh cluster.

    Start a small cluster, then repeatedly add nodes in batches until the
    cluster reaches the target size.
    """
    # Arrange & Act: bootstrap and grow the cluster to the target size.
    servers = await grow_cluster(manager, MAX_CLUSTER_NODES)

    # Assert: the cluster reached exactly the target size.
    assert len(servers) == MAX_CLUSTER_NODES


def _pick_safe_operation_size(current_size: int, *, is_bootstrap: bool) -> int:
    """Pick a random operation size while keeping the cluster within bounds."""
    requested = random.randint(RANDOM_OPERATION_MIN, RANDOM_OPERATION_MAX)
    projected_size = current_size + requested if is_bootstrap else current_size - requested
    if MIN_CLUSTER_NODES <= projected_size <= MAX_CLUSTER_NODES:
        return requested

    if is_bootstrap:
        min_allowed = max(1, MIN_CLUSTER_NODES - current_size)
        max_allowed = MAX_CLUSTER_NODES - current_size
    else:
        min_allowed = max(1, current_size - MAX_CLUSTER_NODES)
        max_allowed = current_size - MIN_CLUSTER_NODES

    if max_allowed < min_allowed:
        return 0

    bounded_low = max(min_allowed, RANDOM_OPERATION_MIN)
    bounded_high = min(max_allowed, RANDOM_OPERATION_MAX)
    if bounded_low <= bounded_high:
        return random.randint(bounded_low, bounded_high)

    return random.randint(min_allowed, max_allowed)


@pytest.mark.check_nodes_for_errors
async def test_scale_decommission_then_bootstrap_cycle(manager: ManagerClient):
    """Cycle topology operations under scale: sequential decommission, then batched bootstrap.

    Grow the cluster to MAX_CLUSTER_NODES (300), then repeat TOPOLOGY_ITERATIONS times:
      1. Pick a random number of nodes and decommission them one by one.
      2. Pick a random number of nodes and bootstrap them in parallel batches of
         BATCH_SIZE (10).

    Before each operation the chosen count is clamped so the cluster size always
    stays within [MIN_CLUSTER_NODES, MAX_CLUSTER_NODES] (50..300).
    """
    init_random_seed()

    # Grow the initial cluster to MAX_CLUSTER_NODES before starting the cycle.
    servers = await grow_cluster(manager, MAX_CLUSTER_NODES)
    assert len(servers) == MAX_CLUSTER_NODES, \
        f"Initial bootstrap failed: expected {MAX_CLUSTER_NODES} nodes, got {len(servers)}"
    logger.info(f"Initial cluster ready with {len(servers)} nodes")

    for iteration in range(1, TOPOLOGY_ITERATIONS + 1):
        current_size = len(servers)
        decommission_count = _pick_safe_operation_size(current_size, is_bootstrap=False)
        logger.info(
            f"Iteration {iteration}/{TOPOLOGY_ITERATIONS}: decommission {decommission_count} nodes one-by-one "
            f"(cluster size before operation: {current_size})"
        )

        # The selected nodes are decommissioned one by one (sequentially).
        for _ in range(decommission_count):
            server = servers.pop()
            await manager.decommission_node(server.server_id)

        assert MIN_CLUSTER_NODES <= len(servers) <= MAX_CLUSTER_NODES, \
            f"Cluster size {len(servers)} out of bounds [{MIN_CLUSTER_NODES}, {MAX_CLUSTER_NODES}] " \
            f"after decommissioning {decommission_count} nodes"
        logger.info(
            f"Iteration {iteration}/{TOPOLOGY_ITERATIONS}: decommissioned {decommission_count} nodes "
            f"(cluster size after operation: {len(servers)})"
        )

        current_size = len(servers)
        bootstrap_count = _pick_safe_operation_size(current_size, is_bootstrap=True)
        logger.info(
            f"Iteration {iteration}/{TOPOLOGY_ITERATIONS}: bootstrap {bootstrap_count} nodes in parallel "
            f"(batch={BATCH_SIZE}, cluster size before operation: {current_size})"
        )

        remaining = bootstrap_count
        while remaining > 0:
            batch = min(BATCH_SIZE, remaining)
            new_servers = await manager.servers_add(batch, cmdline=CMDLINE)
            servers += new_servers
            remaining -= batch

        assert MIN_CLUSTER_NODES <= len(servers) <= MAX_CLUSTER_NODES, \
            f"Cluster size {len(servers)} out of bounds [{MIN_CLUSTER_NODES}, {MAX_CLUSTER_NODES}] " \
            f"after bootstrapping {bootstrap_count} nodes"
        logger.info(
            f"Iteration {iteration}/{TOPOLOGY_ITERATIONS}: bootstrapped {bootstrap_count} nodes "
            f"(cluster size after operation: {len(servers)})"
        )

    # Final step: restore the cluster to its initial size by bootstrapping the
    # missing nodes in parallel batches of BATCH_SIZE.
    restore_count = MAX_CLUSTER_NODES - len(servers)
    logger.info(
        f"Restoring cluster to initial size: bootstrap {restore_count} nodes "
        f"(cluster size before operation: {len(servers)})"
    )
    while len(servers) < MAX_CLUSTER_NODES:
        batch = min(BATCH_SIZE, MAX_CLUSTER_NODES - len(servers))
        servers += await manager.servers_add(batch, cmdline=CMDLINE)

    assert len(servers) == MAX_CLUSTER_NODES, \
        f"Failed to restore cluster: expected {MAX_CLUSTER_NODES} nodes, got {len(servers)}"
    logger.info(f"Cluster restored to initial size with {len(servers)} nodes")
