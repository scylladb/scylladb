#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.repair import create_table_insert_data_for_repair

import asyncio
import logging
import pytest

logger = logging.getLogger(__name__)

# The cluster config cache is refreshed in the background after an ALTER; the refresh logs
# the cached values of every node-oriented scope at debug level.
CMDLINE = ["--logger-log-level", "cluster_config_manager=debug"]


async def alter_and_sync(manager, cql, servers, stmt, pattern):
    """Run the ALTER and wait until every node has refreshed its cluster config cache with it.

    pattern is a regex matched against the rest of the refresh log line, which lists the
    cached values as `{name=value, ...}` per scope.
    """
    logs = [await manager.server_open_log(s.server_id) for s in servers]
    marks = [await log.mark() for log in logs]
    await cql.run_async(stmt)
    await asyncio.gather(*[log.wait_for(f"Refreshed cluster config cache{pattern}", from_mark=mark, timeout=120)
                           for log, mark in zip(logs, marks)])


async def delay_tablet_repairs(manager, servers, ms=1000):
    """Make every tablet repair take at least ms milliseconds."""
    await asyncio.gather(*[manager.api.enable_injection(s.ip_addr, "repair_tablet_repair_task_delay", False, {'value': str(ms)})
                           for s in servers])


async def repair_and_sample_concurrency(manager, cql, servers, ks, table_id):
    """Repair all tablets of ks.test and return the largest number of tablets seen in the repair stage at once."""
    done = asyncio.Event()

    async def sample():
        max_seen = 0
        while not done.is_set():
            rows = await cql.run_async(f"SELECT stage FROM system.tablets WHERE table_id = {table_id}")
            max_seen = max(max_seen, sum(1 for r in rows if r.stage == 'repair'))
            await asyncio.sleep(0.1)
        return max_seen

    sampler = asyncio.create_task(sample())
    try:
        await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token="all", await_completion=True)
    finally:
        done.set()
    return await sampler


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_tablet_repair_max_per_node(manager: ScyllaClusterManager):
    """Check that tablet_repair_max_per_node bounds the number of tablet repairs a node takes part in.

    With three nodes and RF=3 every tablet repair involves every node, so a limit of 1 allows
    one tablet repair in the cluster at a time. Each tablet repair is delayed so that the
    concurrency can be sampled from system.tablets. Without the limit, two shards per node
    allow two tablet repairs at once.
    """
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager, tablets=8, cmdline=CMDLINE)
    await delay_tablet_repairs(manager, servers)

    unlimited = await repair_and_sample_concurrency(manager, cql, servers, ks, table_id)
    logger.info(f"Concurrent tablet repairs without the limit: {unlimited}")
    assert unlimited > 1

    await alter_and_sync(manager, cql, servers, "ALTER CLUSTER WITH tablet_repair_max_per_node = 1",
                         r".*tablet_repair_max_per_node=1[,}]")
    limited = await repair_and_sample_concurrency(manager, cql, servers, ks, table_id)
    logger.info(f"Concurrent tablet repairs with tablet_repair_max_per_node = 1: {limited}")
    assert limited == 1


