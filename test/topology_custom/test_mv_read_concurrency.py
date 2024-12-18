#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from test.pylib.manager_client import ManagerClient

import asyncio
import pytest
import logging

from test.topology.conftest import skip_mode
from test.pylib.util import wait_for_view
from cassandra import ReadTimeout, WriteTimeout

logger = logging.getLogger(__name__)

# This test verifies that the writes causing view updates don't impact the latency of regular reads
# due to contention on the read concurrency semaphore.
# The test creates a table and a table with a materialized view, and then runs a large number of writes causing view updates
# while concurrently running a small read workload on the other table.
# The test fails if any of the reads times out.
# Reproduces https://github.com/scylladb/scylladb/issues/8873
@pytest.mark.asyncio
@skip_mode('release', "error injections aren't enabled in release mode")
async def test_mv_read_concurrency(manager: ManagerClient) -> None:
    node_count = 1
    # Disable cache to make reads use the read concurrency semaphore.
    # Tests remove the rcs multiplier by default, here we use a slightly smaller one (1 instead of default 2) to hit the issue faster.
    cfg = {
        'enable_tablets': True,
        'enable_cache': False,
        'reader_concurrency_semaphore_serialize_limit_multiplier': 1,
        'view_update_reader_concurrency_semaphore_serialize_limit_multiplier': 1,
    }
    servers = await manager.servers_add(node_count, config=cfg)

    cql, _ = await manager.get_ready_cql(servers)
    await cql.run_async(f"CREATE KEYSPACE ks WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}")
    await cql.run_async(f"CREATE TABLE ks.tab (p int PRIMARY KEY, mvp int, v text)")
    await cql.run_async(f"CREATE TABLE ks.tab2 (p int PRIMARY KEY, mvp int)")
    await cql.run_async(f"CREATE MATERIALIZED VIEW IF NOT EXISTS ks.mv AS SELECT p, mvp FROM ks.tab \
        WHERE p IS NOT NULL AND mvp IS NOT NULL PRIMARY KEY (mvp, p)")
    await wait_for_view(cql, 'mv', node_count)

    row_count = 300
    for i in range(10):
        await cql.run_async(f"INSERT INTO ks.tab2 (p, mvp) VALUES ({i}, {i})")

    # The injection prolongs the time we hold the read concurrency semaphore resources during the rbw during a view update
    await manager.api.enable_injection(servers[0].ip_addr, "keep_mv_read_semaphore_units_10ms_longer", one_shot=False)

    failed = None
    stop_event = asyncio.Event()
    async def do_read(i: int):
        read_stmt = cql.prepare(f"SELECT mvp FROM ks.tab2 WHERE p=? USING TIMEOUT 10s")
        while not stop_event.is_set():
            try:
                await manager.cql.run_async(read_stmt, [i])
                await asyncio.sleep(0.1)
            except ReadTimeout as err:
                stop_event.set()
                # Fail the test after waiting for the other tasks to finish to avoid clogging the test logs with 100000*'a'
                nonlocal failed
                failed = err

    async def do_mv_inserts(i: int):
        insert_stmt = cql.prepare(f"INSERT INTO ks.tab(p, mvp, v) VALUES (?, ?, '{100000*'a'}') USING TIMEOUT 10s")
        reps = 0
        while not stop_event.is_set() and reps < 50:
            try:
                await manager.cql.run_async(insert_stmt, [i, i])
                reps += 1
            except WriteTimeout:
                # The writes may timeout for the same reason as the reads, but this test is focused on the reads specifically, so don't fail
                logger.info(f"Write timeout on {i}")

    read_tasks = [asyncio.create_task(do_read(i)) for i in range(10)]
    insert_tasks = [asyncio.create_task(do_mv_inserts(i)) for i in range(row_count)]

    await asyncio.gather(*insert_tasks)
    stop_event.set()
    await asyncio.gather(*read_tasks)

    if failed:
        raise failed

# This test verifies that the writes causing view updates don't make Scylla use excessive memory.
# Similarly to the read timeout test, we create a table with a materialized view, and then run
# an even larger number of writes causing view updates.
# The test fails if Scylla aborts due to using too much memory.
# Reproduces https://github.com/scylladb/scylladb/issues/15805
@pytest.mark.asyncio
@skip_mode('release', "error injections aren't enabled in release mode")
async def test_mv_read_memory(manager: ManagerClient) -> None:
    node_count = 1
    # Disable cache to make reads use the read concurrency semaphore.
    # Tests remove the rcs multipliers by default, here we set the serialize limit back back to the default used outside tests
    # and we increase the kill limit. Without the view update read before write admission, the test exceeds even the increased limit.
    # With the admission, the memory usage should stay within the limits and cause no errors.
    cfg = {
        'enable_tablets': True,
        'enable_cache': False,
        'view_update_reader_concurrency_semaphore_serialize_limit_multiplier': 2,
        'view_update_reader_concurrency_semaphore_kill_limit_multiplier': 10
    }
    servers = await manager.servers_add(node_count, config=cfg)

    cql, _ = await manager.get_ready_cql(servers)
    # Use just 1 tablet to make the test more predictable by running all view updates on the same shard
    await cql.run_async(f"CREATE KEYSPACE ks WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}"
                        "AND tablets = {'initial': 1}")
    await cql.run_async(f"CREATE TABLE ks.tab (p int PRIMARY KEY, mvp int, v text)")
    await cql.run_async(f"CREATE MATERIALIZED VIEW IF NOT EXISTS ks.mv AS SELECT p, mvp FROM ks.tab \
        WHERE p IS NOT NULL AND mvp IS NOT NULL PRIMARY KEY (mvp, p)")
    await wait_for_view(cql, 'mv', node_count)

    row_count = 500

    # The injection prolongs the time we hold the read concurrency semaphore resources during the rbw during a view update
    await manager.api.enable_injection(servers[0].ip_addr, "keep_mv_read_semaphore_units_10ms_longer", one_shot=False)

    stop_event = asyncio.Event()
    async def do_mv_inserts(i: int):
        insert_stmt = cql.prepare(f"INSERT INTO ks.tab(p, mvp, v) VALUES (?, ?, '{100000*'a'}') USING TIMEOUT 30s")
        reps = 0
        while not stop_event.is_set() and reps < 10:
            try:
                await manager.cql.run_async(insert_stmt, [i, i])
                reps += 1
            except WriteTimeout:
                # A write timeout doesn't necessarily show that we run out of memory - the read queueing
                # might just have done its job, so don't fail the test to avoid false negatives
                logger.info(f"Write timeout on {i}")

    insert_tasks = [asyncio.create_task(do_mv_inserts(i)) for i in range(row_count)]

    await asyncio.gather(*insert_tasks)
