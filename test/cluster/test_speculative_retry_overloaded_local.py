#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
import time

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

PK_ROWS = 10
COORD = 'scylla_storage_proxy_coordinator_'


async def coordinator_metric(manager, server, name):
    metrics = await manager.metrics.query(server.ip_addr)
    return metrics.get(COORD + name) or 0


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_speculative_retry_redirected_from_overloaded_local_replica(manager: ScyllaClusterManager):
    """
    Page 2 is coordinated by B with A (page 1's replica) preferred, so B is the speculative extra.
    With A stalled and reads queued for admission on B, B redirects the retry to the third replica.
    """
    nodes = 3
    cfg = {'cache_hit_rate_read_balancing': False}
    servers = await manager.servers_add(nodes, config=cfg, cmdline=['--smp', '1'], auto_rack_dc='dc1')
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    a, b = servers[0], servers[1]

    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {nodes}}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int, ck int, PRIMARY KEY (pk, ck)) WITH speculative_retry = 'NONE'")
        await cql.run_async(f"CREATE TABLE {ks}.blocker (pk int PRIMARY KEY) WITH speculative_retry = 'NONE'")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, ck) VALUES (0, {ck})") for ck in range(PK_ROWS)])
        await cql.run_async(f"INSERT INTO {ks}.blocker (pk) VALUES (0)")

        stmt = SimpleStatement(f"SELECT ck FROM {ks}.test WHERE pk = 0 USING TIMEOUT 60s", fetch_size=2,
                               consistency_level=ConsistencyLevel.ONE)
        page1 = await asyncio.to_thread(cql.execute, stmt, host=hosts[0])
        assert page1.has_more_pages
        await cql.run_async(f"ALTER TABLE {ks}.test WITH speculative_retry = '200ms'")

        # Fill B's user read queue: two reads park inside the read, taking both
        # CPU-concurrency slots, a third one queues for admission.
        await manager.api.enable_injection(b.ip_addr, "replica_query_wait", one_shot=False, parameters={"table": "blocker"})
        blocker_stmt = SimpleStatement(f"SELECT * FROM {ks}.blocker WHERE pk = 0 USING TIMEOUT 60s",
                                       consistency_level=ConsistencyLevel.ONE)
        blockers = [cql.run_async(blocker_stmt, host=hosts[1]) for _ in range(3)]
        # The injection's enter count includes non-matching reads, so count the log line printed only for blocker.
        b_log = await manager.server_open_log(b.server_id)

        async def two_blockers_parked():
            return len(await b_log.grep("replica_query_wait: waiting")) >= 2 or None
        await wait_for(two_blockers_parked, time.time() + 30)

        # The gauge also counts ready-list waiters, but with both slots parked a nonzero value is the third read in the admission queue.
        async def b_has_queued_reads():
            metrics = await manager.metrics.query(b.ip_addr)
            return (metrics.get('scylla_database_queued_reads', {'class': 'sl:default'}) or 0) > 0 or None
        await wait_for(b_has_queued_reads, time.time() + 30)

        redirected_before = await coordinator_metric(manager, b, 'speculative_retries_redirected')
        data_before = await coordinator_metric(manager, b, 'speculative_data_reads')

        await manager.api.enable_injection(a.ip_addr, "replica_query_wait", one_shot=False, parameters={"table": "test"})
        page2 = asyncio.create_task(asyncio.to_thread(cql.execute, stmt, host=hosts[1], paging_state=page1.paging_state))
        failed = True
        try:
            # Served by the spare replica while A is still stalled.
            rows = (await page2).current_rows
            assert [r.ck for r in rows] == [2, 3]
            assert await coordinator_metric(manager, b, 'speculative_retries_redirected') == redirected_before + 1
            assert await coordinator_metric(manager, b, 'speculative_data_reads') == data_before + 1
            failed = False
        finally:
            # Release the stalled reads; on failure don't let their errors or a hang mask the original one.
            for srv in (a, b):
                await manager.api.disable_injection(srv.ip_addr, "replica_query_wait")
                await manager.api.message_injection(srv.ip_addr, "replica_query_wait")
            done = asyncio.gather(page2, *blockers, return_exceptions=failed)
            if failed:
                await asyncio.wait({done}, timeout=10)
            else:
                await done
