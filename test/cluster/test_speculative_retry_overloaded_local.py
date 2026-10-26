#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

PK_ROWS = 10
COORD = 'scylla_storage_proxy_coordinator_'

pytestmark = pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')


async def coordinator_metric(manager, server, name):
    metrics = await manager.metrics.query(server.ip_addr)
    return metrics.get(COORD + name) or 0


async def release_injections(s):
    if s.released:
        return
    s.released = True
    for srv in s.servers:
        await s.manager.api.disable_injection(srv.ip_addr, "replica_query_wait")
        await s.manager.api.message_injection(srv.ip_addr, "replica_query_wait")


@asynccontextmanager
async def paged_read_scenario(manager, nodes):
    """
    Creates the cluster, a table with a speculative_retry-able paged read whose page 1 is served
    by A (servers[0]); page 2 is coordinated by B (servers[1]) with A preferred, so B is the
    speculative extra. Releases all injections and pending reads on exit.
    """
    # One parked read takes the only CPU-concurrency slot, so the next one queues for admission.
    cfg = {'cache_hit_rate_read_balancing': False, 'reader_concurrency_semaphore_cpu_concurrency': 1}
    servers = await manager.servers_add(nodes, config=cfg, cmdline=['--smp', '1'], auto_rack_dc='dc1')
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    s = SimpleNamespace(manager=manager, cql=cql, hosts=hosts, servers=servers, a=servers[0], b=servers[1], tasks=[], released=False)

    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {nodes}}}") as ks:
        s.ks = ks
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int, ck int, PRIMARY KEY (pk, ck)) WITH speculative_retry = 'NONE'")
        await cql.run_async(f"CREATE TABLE {ks}.blocker (pk int PRIMARY KEY) WITH speculative_retry = 'NONE'")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, ck) VALUES (0, {ck})") for ck in range(PK_ROWS)])
        await cql.run_async(f"INSERT INTO {ks}.blocker (pk) VALUES (0)")

        s.stmt = SimpleStatement(f"SELECT ck FROM {ks}.test WHERE pk = 0 USING TIMEOUT 60s", fetch_size=2,
                                 consistency_level=ConsistencyLevel.ONE)
        s.page1 = await asyncio.to_thread(cql.execute, s.stmt, host=hosts[0])
        assert s.page1.has_more_pages
        await cql.run_async(f"ALTER TABLE {ks}.test WITH speculative_retry = '200ms'")

        failed = True
        try:
            yield s
            failed = False
        finally:
            # Release the stalled reads; on failure don't let their errors or a hang mask the original one.
            await release_injections(s)
            done = asyncio.gather(*s.tasks, return_exceptions=failed)
            if failed:
                await asyncio.wait({done}, timeout=10)
            else:
                await done


async def backlog_b(s):
    """Park one blocker read on B and queue a second one behind it for admission."""
    await s.manager.api.enable_injection(s.b.ip_addr, "replica_query_wait", one_shot=False, parameters={"table": "blocker"})
    blocker_stmt = SimpleStatement(f"SELECT * FROM {s.ks}.blocker WHERE pk = 0 USING TIMEOUT 60s",
                                   consistency_level=ConsistencyLevel.ONE)
    s.tasks += [s.cql.run_async(blocker_stmt, host=s.hosts[1]) for _ in range(2)]
    # The injection's enter count includes non-matching reads, so count the log line printed only for blocker.
    b_log = await s.manager.server_open_log(s.b.server_id)

    async def blocker_parked():
        return len(await b_log.grep("replica_query_wait: waiting")) >= 1 or None
    await wait_for(blocker_parked, time.time() + 30)

    # The gauge also counts ready-list waiters, but with the only slot parked a nonzero value is the second read in the admission queue.
    async def b_has_queued_reads():
        metrics = await s.manager.metrics.query(s.b.ip_addr)
        return (metrics.get('scylla_database_queued_reads', {'class': 'sl:default'}) or 0) > 0 or None
    await wait_for(b_has_queued_reads, time.time() + 30)


async def stall_a_and_read_page2(s):
    """Stall A's read of the table and start page 2 via B; returns the page 2 task."""
    await s.manager.api.enable_injection(s.a.ip_addr, "replica_query_wait", one_shot=False, parameters={"table": "test"})
    page2 = asyncio.create_task(asyncio.to_thread(s.cql.execute, s.stmt, host=s.hosts[1], paging_state=s.page1.paging_state))
    s.tasks.append(page2)
    return page2


async def speculative_metrics(s):
    return (await coordinator_metric(s.manager, s.b, 'speculative_retries_redirected'),
            await coordinator_metric(s.manager, s.b, 'speculative_data_reads'))


async def test_speculative_retry_redirected_from_overloaded_local_replica(manager: ScyllaClusterManager):
    """With A stalled and reads queued for admission on B, B redirects the retry to the third replica."""
    async with paged_read_scenario(manager, 3) as s:
        await backlog_b(s)
        redirected_before, data_before = await speculative_metrics(s)
        page2 = await stall_a_and_read_page2(s)

        # Served by the spare replica while A is still stalled.
        assert [r.ck for r in (await page2).current_rows] == [2, 3]
        assert await coordinator_metric(manager, s.b, 'speculative_retries_redirected') == redirected_before + 1
        assert await coordinator_metric(manager, s.b, 'speculative_data_reads') == data_before + 1


async def test_speculative_retry_stays_local_without_backlog(manager: ScyllaClusterManager):
    """With A stalled and nothing queued on B, the retry runs locally on B and is not redirected."""
    async with paged_read_scenario(manager, 3) as s:
        redirected_before, data_before = await speculative_metrics(s)
        page2 = await stall_a_and_read_page2(s)

        assert [r.ck for r in (await page2).current_rows] == [2, 3]
        assert await coordinator_metric(manager, s.b, 'speculative_retries_redirected') == redirected_before
        assert await coordinator_metric(manager, s.b, 'speculative_data_reads') == data_before + 1


async def test_speculative_retry_without_spare_queues_locally(manager: ScyllaClusterManager):
    """With RF=2 there is no spare replica, so the retry still queues locally on the backlogged B."""
    async with paged_read_scenario(manager, 2) as s:
        await backlog_b(s)
        redirected_before, data_before = await speculative_metrics(s)
        page2 = await stall_a_and_read_page2(s)

        async def retry_sent():
            return await coordinator_metric(manager, s.b, 'speculative_data_reads') == data_before + 1 or None
        await wait_for(retry_sent, time.time() + 30)
        assert await coordinator_metric(manager, s.b, 'speculative_retries_redirected') == redirected_before

        # Release everything; the queued local retry then completes page 2.
        await release_injections(s)
        assert [r.ck for r in (await page2).current_rows] == [2, 3]
