#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Tablet repairs of tables with tombstone_gc = {'mode': 'repair'} need hints
# and batchlog flushed on all nodes first. The topology coordinator does that
# flush once for all the tablet repairs it starts and passes the flush time
# to the replica, instead of each repair asking every node itself.

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.repair import load_tablet_repair_time, create_table_insert_data_for_repair
from test.pylib.util import wait_for_cql_and_get_hosts, wait_for
from test.cluster.util import new_test_keyspace, new_test_table

import pytest
import asyncio
import logging
import time

logger = logging.getLogger(__name__)

# One line per flush round on the coordinator.
COORDINATOR_ROUND = r"hints_batchlog_flusher - Flushing hints and batchlog on nodes="
# One line per flush request served on behalf of the coordinator.
GLOBAL_FLUSH_SERVED = r"global flush: Finished to flush batchlog for repair_flush_hints_batchlog_request"
# One line per flush request served on behalf of a repair that flushes itself.
REPAIR_FLUSH_SERVED = r"repair\[[0-9a-f-]+\]: Finished to flush batchlog for repair_flush_hints_batchlog_request"
INITIATING_REPAIR = r"raft_topology - Initiating tablet repair "
FINISHED_REPAIR = r"raft_topology - Finished tablet repair "

# Long enough that no cached flush time expires during a test.
LONG_CACHE_TIME_MS = 600 * 1000


async def count_in_logs(logs, expr):
    return [len(await log.grep(expr)) for log in logs]


async def open_logs(manager, servers):
    return [await manager.server_open_log(s.server_id) for s in servers]


async def test_concurrent_repairs_share_one_flush(manager: ScyllaClusterManager):
    """
    Repairing many tablets at once costs one flush round: one request per node,
    however many tablets are repaired, and none of the repairs flushes itself.
    """
    nr_tablets = 8
    cmdline = ["--repair-hints-batchlog-flush-cache-time-in-ms", str(LONG_CACHE_TIME_MS)]
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager, tablets=nr_tablets, cmdline=cmdline)
    logs = await open_logs(manager, servers)

    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", "all")

    initiated = sum(await count_in_logs(logs, INITIATING_REPAIR))
    rounds = sum(await count_in_logs(logs, COORDINATOR_ROUND))
    served = await count_in_logs(logs, GLOBAL_FLUSH_SERVED)
    self_flushes = sum(await count_in_logs(logs, REPAIR_FLUSH_SERVED))
    logger.info(f"{initiated=} {rounds=} {served=} {self_flushes=}")

    assert initiated == nr_tablets
    assert rounds == 1
    assert served == [1] * len(servers)
    assert self_flushes == 0


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_repair_fails_and_recovers_when_flush_fails(manager: ScyllaClusterManager):
    """
    When the coordinator cannot flush a node, the repair request carries no
    flush time. The replica still repairs the tablet but reports the repair
    as failed, as it does when its own flush fails, so no repair time is
    recorded and the coordinator reschedules the tablet. Once the node
    flushes again the rescheduled repair completes and the time is recorded.
    """
    injection = "repair_flush_hints_batchlog_handler_bm_uninitialized"
    nr_tablets = 4
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager, tablets=nr_tablets, disable_flush_cache_time=True)
    logs = await open_logs(manager, servers)

    before = await load_tablet_repair_time(cql, hosts[0:1], table_id)
    assert all(v is None for v in before.values()), f"{before=}"

    await manager.api.enable_injection(servers[2].ip_addr, injection, one_shot=False)
    try:
        # The coordinator keeps rescheduling the failed repair, so the request never completes.
        with pytest.raises(asyncio.TimeoutError):
            await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", "all", timeout=30)

        failed_rounds = sum(await count_in_logs(logs, r"hints_batchlog_flusher - Flushing hints and batchlog failed on node="))
        no_time_requests = sum(await count_in_logs(logs, INITIATING_REPAIR + r".*flush_time=none"))
        failed_repairs = sum(await count_in_logs(logs, r"repair for tablet .* failed: .*Flush is needed"))
        logger.info(f"{failed_rounds=} {no_time_requests=} {failed_repairs=}")
        assert failed_rounds > 0
        assert no_time_requests > 0
        assert failed_repairs > 0
        during = await load_tablet_repair_time(cql, hosts[0:1], table_id)
        assert all(v is None for v in during.values()), f"{during=}"
    finally:
        await manager.api.disable_injection(servers[2].ip_addr, injection)

    async def all_repaired():
        times = await load_tablet_repair_time(cql, hosts[0:1], table_id)
        return True if all(v is not None for v in times.values()) else None
    await wait_for(all_repaired, time.time() + 300)


async def test_no_flush_for_table_without_repair_tombstone_gc(manager: ScyllaClusterManager):
    """
    A table that does not use repair mode tombstone GC needs no flush, so the
    coordinator neither runs a round nor asks any node to flush.
    """
    nr_tablets = 8
    servers = await manager.servers_add(3, property_file=[{"dc": "dc1", "rack": f"r{i}"} for i in range(3)])
    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    logs = await open_logs(manager, servers)

    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}} AND tablets = {{'initial': {nr_tablets}}}") as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, c int", " WITH tombstone_gc = {'mode': 'timeout'}") as table:
            await asyncio.gather(*[cql.run_async(f"INSERT INTO {table} (pk, c) VALUES ({k}, {k});") for k in range(256)])

            await manager.api.tablet_repair(servers[0].ip_addr, ks, table.split(".")[1], "all")

            finished = sum(await count_in_logs(logs, FINISHED_REPAIR))
            rounds = sum(await count_in_logs(logs, COORDINATOR_ROUND))
            served = sum(await count_in_logs(logs, GLOBAL_FLUSH_SERVED))
            self_flushes = sum(await count_in_logs(logs, REPAIR_FLUSH_SERVED))
            logger.info(f"{finished=} {rounds=} {served=} {self_flushes=}")
            assert finished == nr_tablets
            assert rounds == 0
            assert served == 0
            assert self_flushes == 0


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_repair_flushes_itself_for_request_without_flush_mode(manager: ScyllaClusterManager):
    """
    A request from a coordinator too old to say what to do about the flush
    makes the repair flush on all nodes itself, as every tablet repair used
    to. The injection drops the parameter on the receiving side to stand in
    for such a coordinator.
    """
    injection = "tablet_repair_ignore_flush_mode"
    nr_tablets = 4
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager, tablets=nr_tablets, disable_flush_cache_time=True)
    logs = await open_logs(manager, servers)

    await asyncio.gather(*[manager.api.enable_injection(s.ip_addr, injection, one_shot=False) for s in servers])
    try:
        await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", "all")
    finally:
        await asyncio.gather(*[manager.api.disable_injection(s.ip_addr, injection) for s in servers])

    finished = sum(await count_in_logs(logs, FINISHED_REPAIR))
    self_flushes = sum(await count_in_logs(logs, REPAIR_FLUSH_SERVED))
    logger.info(f"{finished=} {self_flushes=}")
    assert finished == nr_tablets
    # Each repair flushes every node, and the cache is off.
    assert self_flushes == nr_tablets * len(servers)
    times = await load_tablet_repair_time(cql, hosts[0:1], table_id)
    assert all(v is not None for v in times.values()), f"{times=}"
