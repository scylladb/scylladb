#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import re

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

# One shard, so all statements land in the same cache and its metrics are one shard's.
CMDLINE = ["--smp", "1"]
TRACE_CMDLINE = CMDLINE + ["--logger-log-level", "prepared_statements_cache=trace"]
VNODES = {"tablets_mode_for_new_keyspaces": "disabled"}


async def evictions(manager: ScyllaClusterManager, server) -> tuple[int, int]:
    """The prepared statements cache's evictions and its unprivileged evictions on size."""
    metrics = await manager.metrics.query(server.ip_addr)
    return (int(metrics.get("scylla_cql_prepared_cache_evictions") or 0),
            int(metrics.get("scylla_cql_unprivileged_entries_evictions_on_size") or 0))


async def prepared_ids(log, pattern: str, mark: int) -> list[str]:
    """The cql_ids of the prepared_statements_cache trace lines matching pattern since mark."""
    await log.wait_for(pattern, from_mark=mark)
    ids = []
    for line, _ in await log.grep(pattern, from_mark=mark):
        m = re.search(r"cql_id: ([0-9a-f]+)", line)
        assert m, f"Bad format in a prepared_statements_cache log line: {line}"
        ids.append(m.group(1))
    return ids


async def test_prepared_statement_cache_unprivileged_eviction(manager: ScyllaClusterManager):
    """Statements prepared and executed once are evicted from the unprivileged section, and counted there."""
    server = await manager.server_add(cmdline=CMDLINE, config=VNODES)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (k int PRIMARY KEY, a int)")
        i = 0
        while True:
            await cql.run_async(cql.prepare(f"SELECT k, a FROM {ks}.test where k = {i}"))
            i += 1
            evicted, unprivileged = await evictions(manager, server)
            assert evicted == unprivileged
            if evicted > 100:
                break


async def test_prepared_statement_cache_privileged_eviction(manager: ScyllaClusterManager):
    """Statements executed twice move to the privileged section, so nothing is evicted from the unprivileged one."""
    server = await manager.server_add(cmdline=CMDLINE, config=VNODES)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (k int PRIMARY KEY, a int)")
        i = 0
        while True:
            prepared = cql.prepare(f"SELECT k, a FROM {ks}.test where k = {i}")
            await cql.run_async(prepared)
            await cql.run_async(prepared)
            i += 1
            evicted, unprivileged = await evictions(manager, server)
            assert unprivileged == 0
            if evicted > 100:
                break


async def check_lru_eviction(manager: ScyllaClusterManager, executions: int):
    """Fill the cache with statements executed `executions` times each, while a hot statement is executed in
    every round, then keep adding statements: each one must evict exactly one entry, the least recently used,
    and never the hot statement. The cache's trace log gives the ids stored and evicted."""
    server = await manager.server_add(cmdline=TRACE_CMDLINE, config=VNODES)
    cql = manager.get_cql()
    log = await manager.server_open_log(server.server_id)
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (k int PRIMARY KEY, a int)")
        mark = await log.mark()
        hot = cql.prepare(f"SELECT k, a FROM {ks}.test where k = 0")
        for _ in range(executions):
            await cql.run_async(hot)
        hot_id = (await prepared_ids(log, "storing the value for the first time", mark))[0]

        i = 1
        ids = []
        while True:
            mark = await log.mark()
            prepared = cql.prepare(f"SELECT k, a FROM {ks}.test where k = {i}")
            ids.append((await prepared_ids(log, "storing the value for the first time", mark))[0])
            for _ in range(executions):
                await cql.run_async(prepared)
            for _ in range(executions):
                await cql.run_async(hot)
            i += 1
            evicted, unprivileged = await evictions(manager, server)
            if evicted > 0:
                if executions == 1:
                    assert evicted == unprivileged, "Privileged entries got evicted"
                break

        last_evicted = evicted
        lru = 0
        for j in range(i, 2 * i - 1):
            for evicted_id in await prepared_ids(log, r"prepared_statements_cache - shrink\(\)", mark):
                assert evicted_id != hot_id, "MRU entry got evicted!"
                assert evicted_id == ids[lru], "LRU entry hasn't got evicted!"
                lru += 1
            mark = await log.mark()
            prepared = cql.prepare(f"SELECT k, a FROM {ks}.test where k = {j}")
            for _ in range(executions):
                await cql.run_async(prepared)
            # The hot statement is already privileged: once is enough to keep it the most recently used.
            await cql.run_async(hot)
            evicted, unprivileged = await evictions(manager, server)
            assert last_evicted + 1 == evicted, "No eviction! Must have been!"
            if executions == 1:
                assert evicted == unprivileged, "Privileged entries got evicted"
            last_evicted = evicted


async def test_prepared_statement_cache_lru_eviction_privileged(manager: ScyllaClusterManager):
    await check_lru_eviction(manager, 2)


async def test_prepared_statement_cache_lru_eviction_unprivileged(manager: ScyllaClusterManager):
    await check_lru_eviction(manager, 1)
