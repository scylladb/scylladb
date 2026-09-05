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

CFG = {'tablet_load_stats_refresh_interval_in_seconds': 1}


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

        reloads = await log.grep("topology_state_load: loading topology state", from_mark=mark)
        applies = await log.grep("topology_state_load: waiting for token metadata lock", from_mark=mark)

        print(f"\nBENCHMARK RESULT: grew {INITIAL_NODES}->{INITIAL_NODES + ADDED_NODES} nodes, "
              f"RF=3, {NUM_TABLETS} tablets, rebalance in {elapsed:.3f}s wall-clock; "
              f"{len(reloads)} full system.topology reloads out of {len(applies)} topology_state_load calls "
              f"({100.0 * len(reloads) / max(len(applies), 1):.1f}% full reloads)")
