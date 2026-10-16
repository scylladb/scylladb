#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
import re
from collections import Counter

import pytest

from test.cluster.tasks import extra_scylla_cmdline_options
from test.cluster.tasks.task_manager_client import TaskManagerClient
from test.cluster.util import get_topology_coordinator, new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.rest_client import HTTPError, read_barrier

logger = logging.getLogger(__name__)

TABLETS = 8
KEYS = 200
FLUSHES = 3


# A tablet compaction counts with a weight of 2, like repair, against the per-shard tablet
# streaming limits, so with limits of 2 a shard compacts a single tablet at a time.
SINGLE_TABLET_PER_SHARD = ('--tablet-streaming-read-concurrency-per-shard', '2',
                           '--tablet-streaming-write-concurrency-per-shard', '2')


async def create_cluster(manager: ScyllaClusterManager, extra_cmdline: tuple[str, ...] = ()):
    cmdline = ['--smp', '2', *extra_scylla_cmdline_options, *extra_cmdline]
    property_files = [{"dc": "dc1", "rack": f"r{i}"} for i in range(2)]
    return await manager.servers_add(2, cmdline=cmdline, property_file=property_files)


async def populate(manager: ScyllaClusterManager, servers, ks: str):
    """Writes the table in several flushes, so each tablet replica has several sstables."""
    cql = manager.get_cql()
    await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int)")
    for s in servers:
        await manager.api.disable_autocompaction(s.ip_addr, ks, "test")
    for v in range(FLUSHES):
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({k}, {v})") for k in range(KEYS)])
        for s in servers:
            await manager.api.keyspace_flush(s.ip_addr, ks, "test")


async def live_sstable_count(manager: ScyllaClusterManager, server, ks: str) -> int:
    return await manager.api.client.get_json(f"/column_family/metrics/live_ss_table_count/{ks}:test", host=server.ip_addr)


async def start_cluster_compaction(manager: ScyllaClusterManager, server, ks: str) -> str:
    return await manager.api.client.post_json("/storage_service/tablets/compaction", host=server.ip_addr,
                                              params={"ks": ks, "table": "test"})


async def tablet_compaction_transitions(manager: ScyllaClusterManager, server, ks: str):
    """Returns the replicas of the tablets of the table which are in the tablet_compaction transition."""
    # Query the node which did the read barrier.
    host = manager.get_cql().cluster.metadata.get_host(server.ip_addr)
    await read_barrier(manager.api, server.ip_addr)
    table_id = await manager.get_table_id(ks, "test")
    rows = await manager.get_cql().run_async(f"SELECT replicas, transition FROM system.tablets WHERE table_id = {table_id}", host=host)
    return [r.replicas for r in rows if r.transition == "tablet_compaction"]


async def ongoing_requests(manager: ScyllaClusterManager, server):
    host = manager.get_cql().cluster.metadata.get_host(server.ip_addr)
    await read_barrier(manager.api, server.ip_addr)
    rows = await manager.get_cql().run_async("SELECT ongoing_cluster_compaction_requests FROM system.topology", host=host)
    return rows[0].ongoing_cluster_compaction_requests or set() if rows else set()


@pytest.mark.asyncio
async def test_tablet_compaction(manager: ScyllaClusterManager):
    """A cluster-wide major compaction compacts every tablet replica into a single sstable,
    keeping the data, and is tracked by a task whose progress counts the compacted tablets.
    The default per-shard tablet streaming limits apply."""
    servers = await create_cluster(manager)
    tm = TaskManagerClient(manager.api)
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} "
                                          f"AND tablets = {{'initial': {TABLETS}}}") as ks:
        await populate(manager, servers, ks)
        for s in servers:
            assert await live_sstable_count(manager, s, ks) > TABLETS

        task_id = await start_cluster_compaction(manager, servers[0], ks)
        status = await tm.wait_for_task(servers[0].ip_addr, task_id)
        assert status.state == "done", status.error
        assert status.type == "compact_tablets"
        assert status.kind == "cluster"
        assert status.scope == "table"
        assert status.keyspace == ks
        assert status.table == "test"
        assert status.progress_units == "tablets"
        assert status.progress_total == TABLETS
        assert status.progress_completed == TABLETS

        # Every node holds a replica of every tablet, each in a single sstable now.
        for s in servers:
            assert await live_sstable_count(manager, s, ks) == TABLETS
        assert not await tablet_compaction_transitions(manager, servers[0], ks)
        assert not await ongoing_requests(manager, servers[0])

        rows = await manager.get_cql().run_async(f"SELECT pk, v FROM {ks}.test")
        assert sorted((r.pk, r.v) for r in rows) == [(k, FLUSHES - 1) for k in range(KEYS)]


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_tablet_compaction_concurrency_per_shard(manager: ScyllaClusterManager):
    """A major compaction counts against the per-shard tablet streaming limits, with a weight
    of 2, so with limits of 2 no shard major compacts more than one tablet at a time on behalf
    of a cluster-wide major compaction."""
    servers = await create_cluster(manager, SINGLE_TABLET_PER_SHARD)
    tm = TaskManagerClient(manager.api)
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} "
                                          f"AND tablets = {{'initial': {TABLETS}}}") as ks:
        await populate(manager, servers, ks)
        for s in servers:
            await manager.api.enable_injection(s.ip_addr, "tablet_compaction_wait", one_shot=False)

        task_id = await start_cluster_compaction(manager, servers[0], ks)
        max_seen = 0
        while True:
            status = await tm.get_task_status(servers[0].ip_addr, task_id)
            if status.state in ("done", "failed"):
                break
            transitions = await tablet_compaction_transitions(manager, servers[0], ks)
            per_shard = Counter((str(host), shard) for replicas in transitions for host, shard in replicas)
            if per_shard:
                max_seen = max(max_seen, max(per_shard.values()))
            assert all(n <= 1 for n in per_shard.values()), f"More than one tablet compacted on a shard: {per_shard}"
            # Release the held compactions only once some were observed: a node's wait passes for
            # any message it received before, so a message sent before the first compaction waits
            # would release all of them before they could be observed.
            if per_shard:
                for s in servers:
                    await manager.api.message_injection(s.ip_addr, "tablet_compaction_wait")
            await asyncio.sleep(0.2)
        assert status.state == "done", status.error
        assert max_seen == 1
        for s in servers:
            await manager.api.disable_injection(s.ip_addr, "tablet_compaction_wait")
            assert await live_sstable_count(manager, s, ks) == TABLETS


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_tablet_compaction_abort(manager: ScyllaClusterManager):
    """Aborting the task of a cluster-wide major compaction fails it, cancels the compaction of
    the tablets which are being compacted, and stops starting the compaction of more tablets."""
    servers = await create_cluster(manager, SINGLE_TABLET_PER_SHARD)
    tm = TaskManagerClient(manager.api)
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} "
                                          f"AND tablets = {{'initial': {TABLETS}}}") as ks:
        await populate(manager, servers, ks)
        for s in servers:
            await manager.api.enable_injection(s.ip_addr, "tablet_compaction_wait", one_shot=False)

        coordinator = await manager.find_server_by_host_id(servers, await get_topology_coordinator(manager))
        log = await manager.server_open_log(coordinator.server_id)
        mark = await log.mark()
        server_logs = [await manager.server_open_log(s.server_id) for s in servers]
        server_marks = [await server_log.mark() for server_log in server_logs]

        task_id = await start_cluster_compaction(manager, servers[0], ks)
        # Wait for the first tablets to be held by the injection on every node, before they are
        # compacted. Each tablet has a replica on every node.
        for server_log, server_mark in zip(server_logs, server_marks):
            await server_log.wait_for("tablet_compaction_wait: waiting for message", from_mark=server_mark)
        sstables = [await live_sstable_count(manager, s, ks) for s in servers]

        await tm.abort_task(servers[0].ip_addr, task_id)
        status = await tm.wait_for_task(servers[0].ip_addr, task_id)
        assert status.state == "failed"
        assert "Aborted" in status.error
        assert not await ongoing_requests(manager, servers[0])

        # The compaction of the tablets which were being compacted is canceled while the injection
        # still holds it, and no other tablets start.
        while await tablet_compaction_transitions(manager, servers[0], ks):
            await asyncio.sleep(0.1)
        cancel_pattern = r"Canceling compaction of tablet (\S+), as its request was aborted"
        await log.wait_for(cancel_pattern, from_mark=mark)
        canceled = [m[1].group(1) for m in await log.grep(cancel_pattern, from_mark=mark)]
        # Let every node see that the transitions are gone before releasing the compactions,
        # which then fail rather than compact the tablets.
        for s in servers:
            await read_barrier(manager.api, s.ip_addr)
        mark = await log.mark()
        for s in servers:
            await manager.api.message_injection(s.ip_addr, "tablet_compaction_wait")
            await manager.api.disable_injection(s.ip_addr, "tablet_compaction_wait")
        await log.wait_for(*[f"tablet_compaction for tablet {re.escape(gid)} failed" for gid in canceled], from_mark=mark)

        status = await tm.get_task_status(servers[0].ip_addr, task_id)
        assert status.progress_completed == 0
        assert [await live_sstable_count(manager, s, ks) for s in servers] == sstables
        assert all(request_id is None for request_id, _ in await tablet_markers(manager, servers[0], ks, "test"))


async def tablet_markers(manager: ScyllaClusterManager, server, ks: str, table: str):
    """Returns the (cluster_compaction_request_id, cluster_compaction_failed) of each tablet of the table."""
    host = manager.get_cql().cluster.metadata.get_host(server.ip_addr)
    await read_barrier(manager.api, server.ip_addr)
    table_id = await manager.get_table_or_view_id(ks, table)
    rows = await manager.get_cql().run_async(
        f"SELECT cluster_compaction_request_id, cluster_compaction_failed FROM system.tablets WHERE table_id = {table_id}", host=host)
    return [(str(r.cluster_compaction_request_id) if r.cluster_compaction_request_id else None, bool(r.cluster_compaction_failed)) for r in rows]


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_tablet_compaction_failure(manager: ScyllaClusterManager):
    """A tablet which fails to compact isn't retried, the other tablets are compacted all the same,
    and the cluster-wide major compaction then fails, reporting the failed tablet."""
    servers = await create_cluster(manager)
    tm = TaskManagerClient(manager.api)
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} "
                                          f"AND tablets = {{'initial': {TABLETS}}}") as ks:
        await populate(manager, servers, ks)
        await manager.api.enable_injection(servers[1].ip_addr, "tablet_compaction_fail", one_shot=True)

        task_id = await start_cluster_compaction(manager, servers[0], ks)
        status = await tm.wait_for_task(servers[0].ip_addr, task_id)
        assert status.state == "failed"
        assert "major compaction failed for 1 tablet(s)" in status.error
        assert "tablet_compaction_fail injection" in status.error
        # Every tablet was processed, compacted or failed.
        assert status.progress_total == TABLETS
        assert status.progress_completed == TABLETS

        markers = await tablet_markers(manager, servers[0], ks, "test")
        assert all(request_id == task_id for request_id, _ in markers)
        assert sum(failed for _, failed in markers) == 1
        assert not await tablet_compaction_transitions(manager, servers[0], ks)
        assert not await ongoing_requests(manager, servers[0])


@pytest.mark.asyncio
async def test_tablet_compaction_params(manager: ScyllaClusterManager):
    """The table can be given by its id, without its keyspace, and conflicting or missing parameters
    are rejected, as are compaction types other than major and options which major doesn't take."""
    servers = await create_cluster(manager)
    tm = TaskManagerClient(manager.api)
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} "
                                          f"AND tablets = {{'initial': {TABLETS}}}") as ks:
        await populate(manager, servers, ks)
        table_id = str(await manager.get_table_id(ks, "test"))
        api = manager.api.client
        ip = servers[0].ip_addr

        for params, error in [
                ({"ks": ks, "table": "test", "table_id": table_id}, "Specify either table or table_id, not both"),
                ({"ks": ks}, "Both ks and table are required, unless table_id is given"),
                ({"table_id": table_id, "ks": "other_ks"}, f"belongs to keyspace {ks}, not to other_ks"),
                ({"table_id": "5e1bd3a0-4a9e-11f0-9f7a-0123456789ab"}, "does not exist"),
                ({"table_id": table_id, "type": "scrub"}, "Unsupported tablet compaction type 'scrub': only 'major' is supported"),
                ({"table_id": table_id, "flush": "true"}, "Unsupported option 'flush' for major compaction"),
                ({"table_id": table_id, "force_flush": "maybe"}, "Invalid value 'maybe' of option force_flush: expected true or false"),
        ]:
            with pytest.raises(HTTPError, match=error):
                await api.post_json("/storage_service/tablets/compaction", host=ip, params=params)

        task_id = await api.post_json("/storage_service/tablets/compaction", host=ip, params={"table_id": table_id, "type": "major"})
        status = await tm.wait_for_task(ip, task_id)
        assert status.state == "done", status.error
        assert status.table == "test"
        for s in servers:
            assert await live_sstable_count(manager, s, ks) == TABLETS


@pytest.mark.asyncio
async def test_tablet_compaction_recursive(manager: ScyllaClusterManager):
    """With recursive, a single request compacts the table's materialized views and secondary
    indexes too. Without it, only the table is compacted."""
    servers = await create_cluster(manager)
    tm = TaskManagerClient(manager.api)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} "
                                          f"AND tablets = {{'initial': {TABLETS}}}") as ks:
        await populate(manager, servers, ks)
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv AS SELECT * FROM {ks}.test WHERE v IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (v, pk)")
        await cql.run_async(f"CREATE INDEX test_v_idx ON {ks}.test (v)")
        derived = ["mv", "test_v_idx_index"]

        api = manager.api.client
        ip = servers[0].ip_addr
        task_id = await api.post_json("/storage_service/tablets/compaction", host=ip, params={"ks": ks, "table": "test"})
        assert (await tm.wait_for_task(ip, task_id)).state == "done"
        for table in derived:
            assert all(request_id is None for request_id, _ in await tablet_markers(manager, servers[0], ks, table))

        task_id = await api.post_json("/storage_service/tablets/compaction", host=ip, params={"ks": ks, "table": "test", "recursive": "true"})
        status = await tm.wait_for_task(ip, task_id)
        assert status.state == "done", status.error
        assert status.table == "test"
        total = 0
        for table in ["test", *derived]:
            markers = await tablet_markers(manager, servers[0], ks, table)
            assert markers and all(m == (task_id, False) for m in markers), f"{table}: {markers}"
            total += len(markers)
        assert status.progress_total == total
        assert status.progress_completed == total


@pytest.mark.asyncio
async def test_tablet_compaction_flushes_all_tables(manager: ScyllaClusterManager):
    """Each node flushes all of its tables, forcing new commitlog segments, once per cluster-wide
    compaction, before compacting its tablets: unless force_flush is set, only if all tables
    weren't flushed within compaction_flush_all_tables_before_major_seconds."""
    servers = await create_cluster(manager)
    tm = TaskManagerClient(manager.api)
    api = manager.api.client
    ip = servers[0].ip_addr
    logs = [await manager.server_open_log(s.server_id) for s in servers]
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} "
                                          f"AND tablets = {{'initial': {TABLETS}}}") as ks:
        await populate(manager, servers, ks)

        async def compact(**params):
            marks = [await log.mark() for log in logs]
            task_id = await api.post_json("/storage_service/tablets/compaction", host=ip, params={"ks": ks, "table": "test", **params})
            status = await tm.wait_for_task(ip, task_id)
            assert status.state == "done", status.error
            return task_id, marks

        async def count(pattern, marks):
            return [len(await log.grep(pattern, from_mark=mark)) for log, mark in zip(logs, marks)]

        # Every node holds replicas, so it decides once whether to flush, whatever it did before.
        task_id, marks = await compact()
        assert await count(rf"(Flushed|Skipped flushing) all tables before cluster-wide compaction request {task_id}", marks) == [1, 1]

        # All tables were just flushed, or recently enough.
        task_id, marks = await compact()
        assert await count(rf"Skipped flushing all tables before cluster-wide compaction request {task_id}", marks) == [1, 1]

        # Forced, every shard of every node flushes all tables, once.
        task_id, marks = await compact(force_flush="true")
        assert await count(rf"Flushed all tables before cluster-wide compaction request {task_id}", marks) == [1, 1]
        assert await count("Forcing new commitlog segment and flushing all tables complete", marks) == [2, 2]
