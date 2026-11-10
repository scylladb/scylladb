#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.repair import create_table_insert_data_for_repair
from test.pylib.util import wait_for

import asyncio
import logging
import pytest
import time

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



@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_tablet_repair_intensity(manager: ScyllaClusterManager):
    """Check that tablet_repair_intensity makes a node rest between tablet repairs and that a change applies at once.

    Steps: repair all tablets at the default and check that the scheduler skipped no candidate;
    set the cluster value to 1, start a repair of all tablets and wait until the scheduler skips
    candidates because a node rests; remove the value and check that the repair finishes. Then
    the same with the value set on one node only. Every tablet repair takes at least a second,
    so at 1 the rest after the first tablets is more than a minute.
    """
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager, tablets=8, cmdline=CMDLINE)
    await delay_tablet_repairs(manager, servers)

    async def skipped():
        metrics = await asyncio.gather(*[manager.metrics.query(s.ip_addr) for s in servers])
        return sum(m.get("scylla_load_balancer_repairs_skipped_cooldown") or 0 for m in metrics)

    async def repair_all():
        await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token="all", await_completion=True, timeout=600)

    await repair_all()
    assert await skipped() == 0

    async def repair_resting(stmt, pattern, reset_stmt, reset_pattern):
        before = await skipped()
        await alter_and_sync(manager, cql, servers, stmt, pattern)
        repair = asyncio.create_task(repair_all())
        await wait_for(lambda: skipped_more(before), deadline=time.time() + 120)
        assert not repair.done()
        await alter_and_sync(manager, cql, servers, reset_stmt, reset_pattern)
        await repair

    async def skipped_more(before):
        return await skipped() > before or None

    await repair_resting("ALTER CLUSTER WITH tablet_repair_intensity = 1", r".*tablet_repair_intensity=1[,}]",
                         "ALTER CLUSTER WITH tablet_repair_intensity = NULL", r"(?!.*tablet_repair_intensity)")

    host_id = await manager.get_host_id(servers[1].server_id)
    await repair_resting(f"ALTER NODE {host_id} WITH tablet_repair_intensity = 1",
                         rf".*node scope: .*{host_id}.*tablet_repair_intensity=1[,}}]",
                         f"ALTER NODE {host_id} WITH tablet_repair_intensity = NULL", r"(?!.*tablet_repair_intensity)")
