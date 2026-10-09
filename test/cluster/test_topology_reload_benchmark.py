#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# Ad hoc before/after benchmark for the tablet-migration topology-reload fix.
# Not part of the regular suite assertions beyond a sanity check; the useful
# output is the printed reload/apply counts and wall-clock time.
#
# Scenario: RF=3 cluster grown from 3 to 6 nodes, with a real (non-empty)
# table and the actual tablet load balancer driving migrations via
# quiesce_topology — not a hand-picked single-tablet move loop.
import asyncio
import logging
import time

import pytest

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_replica_count_by_host
from test.cluster.util import new_test_keyspace

logger = logging.getLogger(__name__)

INITIAL_NODES = 3
ADDED_NODES = 3
NUM_TABLETS = 512
NUM_ROWS = 5000

DECOMM_INITIAL_NODES = 4
NUM_TABLETS_DECOMM = 512
NUM_ROWS_DECOMM = 5000

CFG = {'tablet_load_stats_refresh_interval_in_seconds': 1}

# Ceilings, not targets: a fast-path bug that stalls coordinator progress
# (e.g. a stale topology.paused_requests entry) doesn't change the full/fast
# reload mix at all, so the reload-count metric below can't catch it - only
# wall-clock can. Measured on this scenario: master (no fast path, 100% full
# reloads) ~19s/~5s; a real regression that was caught this way took ~50s/~24s.
MAX_ELAPSED_S_GROW = 30
MAX_ELAPSED_S_DECOMM = 12


def _report(log_reloads, log_applies, elapsed, description):
    print(f"\nBENCHMARK RESULT: {description}, in {elapsed:.3f}s wall-clock; "
          f"{len(log_reloads)} full system.topology reloads out of {len(log_applies)} topology_state_load calls "
          f"({100.0 * len(log_reloads) / max(len(log_applies), 1):.1f}% full reloads)")


@pytest.mark.perf
async def test_topology_reload_benchmark(manager: ScyllaClusterManager):
    # RF=3 with tablets requires at least 3 distinct racks in the DC.
    servers = await manager.servers_add(INITIAL_NODES, config=CFG, auto_rack_dc="datacenter1")
    cql = manager.get_cql()

    await manager.disable_tablet_balancing()

    async with new_test_keyspace(manager,
            f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}} "
            f"AND tablets = {{'initial': {NUM_TABLETS}}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int);")

        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({i}, {i})")
                                for i in range(NUM_ROWS)])
        await manager.api.flush_keyspace(servers[0].ip_addr, ks)

        log = await manager.server_open_log(servers[0].server_id)
        mark = await log.mark()

        t0 = time.monotonic()
        for i in range(ADDED_NODES):
            # Reuse the same racks as the initial nodes (2 nodes/rack) rather than
            # adding new racks: RF=3 stays satisfied by 3 racks either way, but this
            # lets the balancer freely spread load onto the new nodes without
            # being constrained by rack placement.
            servers.append(await manager.server_add(config=CFG,
                    property_file={"dc": "datacenter1", "rack": f"rack{i + 1}"}))

        await manager.enable_tablet_balancing()
        await manager.api.quiesce_topology(servers[0].ip_addr)
        elapsed = time.monotonic() - t0

        replicas = await get_replica_count_by_host(manager, servers[0], ks, "test")
        logger.info(f"Replica distribution after rebalance: {replicas}")
        assert len(replicas) == INITIAL_NODES + ADDED_NODES
        assert elapsed < MAX_ELAPSED_S_GROW, \
                f"rebalance took {elapsed:.3f}s (ceiling {MAX_ELAPSED_S_GROW}s) - likely a stalled reload path"

        reloads = await log.grep("topology_state_load: loading topology state", from_mark=mark)
        applies = await log.grep("topology_state_load: waiting for token metadata lock", from_mark=mark)

        _report(reloads, applies, elapsed,
                f"grew {INITIAL_NODES}->{INITIAL_NODES + ADDED_NODES} nodes, RF=3, {NUM_TABLETS} tablets, rebalance")


@pytest.mark.perf
async def test_topology_reload_benchmark_decommission_while_draining(manager: ScyllaClusterManager):
    # Grows to DECOMM_INITIAL_NODES, then decommissions one under tablet load,
    # exercising the ordinary tablet-migration fast path during a busy
    # decommission (the tablet-rebuild-drain scope gap fix targets this).
    # Not a left_nodes_rs scenario: decommission asserts tablets are fully
    # drained before the node reaches 'left' state, so left_nodes_rs stays
    # empty here; only node replace populates it with a still-draining node.
    # RF=3 requires exactly 3 racks, so the 4th node reuses rack1 rather than
    # getting its own rack: decommissioning it then leaves the rack count at 3.
    servers = await manager.servers_add(DECOMM_INITIAL_NODES - 1, config=CFG, auto_rack_dc="datacenter1")
    servers.append(await manager.server_add(config=CFG,
            property_file={"dc": "datacenter1", "rack": "rack1"}))
    cql = manager.get_cql()

    async with new_test_keyspace(manager,
            f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}} "
            f"AND tablets = {{'initial': {NUM_TABLETS_DECOMM}}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int);")

        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({i}, {i})")
                                for i in range(NUM_ROWS_DECOMM)])
        await manager.api.flush_keyspace(servers[0].ip_addr, ks)

        log = await manager.server_open_log(servers[0].server_id)
        mark = await log.mark()

        t0 = time.monotonic()
        await manager.decommission_node(servers[-1].server_id)
        elapsed = time.monotonic() - t0

        replicas = await get_replica_count_by_host(manager, servers[0], ks, "test")
        logger.info(f"Replica distribution after decommission: {replicas}")
        assert len(replicas) == DECOMM_INITIAL_NODES - 1
        assert elapsed < MAX_ELAPSED_S_DECOMM, \
                f"decommission took {elapsed:.3f}s (ceiling {MAX_ELAPSED_S_DECOMM}s) - likely a stalled reload path"

        reloads = await log.grep("topology_state_load: loading topology state", from_mark=mark)
        applies = await log.grep("topology_state_load: waiting for token metadata lock", from_mark=mark)

        _report(reloads, applies, elapsed,
                f"decommissioned 1 of {DECOMM_INITIAL_NODES} nodes, RF=3, {NUM_TABLETS_DECOMM} tablets, "
                f"while draining")
