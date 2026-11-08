#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import logging
import psutil
import pytest
from typing import Callable

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.cluster.storage.conftest import space_limited_servers
from test.cluster.tasks.task_manager_client import TaskManagerClient
from test.cluster.storage.test_out_of_space_prevention import DISK_FILL_TARGET_RATIO, global_cmdline, random_content_file

logger = logging.getLogger(__name__)


async def populate_vnodes_table(manager: ScyllaClusterManager, cql, server) -> tuple[str, str]:
    """Create a vnodes table, write data into it and flush it. Return the
    names of its keyspace and table."""
    # Create the keyspace and table without the helper context managers:
    # they drop the table on exit, but by then the node is down.
    ks = "test_ks"
    table = "test"
    await cql.run_async(f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'enabled': false}}")
    await cql.run_async(f"CREATE TABLE {ks}.{table} (pk int PRIMARY KEY, v int)")
    for pk in range(100):
        await cql.run_async(f"INSERT INTO {ks}.{table} (pk, v) VALUES ({pk}, {pk})")
    await manager.api.flush_keyspace(server.ip_addr, ks)
    return ks, table


async def restart_with_paused_resharding(manager: ScyllaClusterManager, server, log, table: str, shards: int,
                                         **start_args) -> tuple[asyncio.Task, int]:
    """Restart the node, which runs with the given number of shards, with one
    more shard in a background task, so that it reshards the given table of
    populate_vnodes_table() on boot, and pause the reshard jobs of that
    table. Return the start task and a log mark from before the restart."""
    await manager.server_stop_gracefully(server.server_id)
    await manager.server_update_cmdline(server.server_id, ["--smp", str(shards + 1)])
    await manager.server_update_config(server.server_id, "error_injections_at_startup",
                                       [{"name": "reshard_compaction_wait", "table": table}])
    mark = await log.mark()
    return asyncio.create_task(manager.server_start(server.server_id, **start_args)), mark


async def wait_for_paused_reshard_jobs(log, mark: int, node_done: asyncio.Task, jobs: int) -> int:
    """Wait until the given number of reshard jobs of the table pause, and
    return the log mark after that. Fail early if the node_done task
    finishes first.

    Resharding runs one job per shared sstable, so after a single flush the
    number of jobs is the number of shards the node had before the restart."""
    for _ in range(jobs):
        wait_task = asyncio.create_task(log.wait_for("reshard_compaction_wait: waiting", from_mark=mark, timeout=120))
        done, _ = await asyncio.wait({wait_task, node_done}, return_when=asyncio.FIRST_COMPLETED)
        if wait_task not in done:
            wait_task.cancel()
            await asyncio.gather(wait_task, return_exceptions=True)
            await node_done
            pytest.fail("The node stopped starting before the reshard jobs paused")
        mark, _ = wait_task.result()
    return mark


async def test_boot_fails_on_critical_disk_before_resharding(manager: ScyllaClusterManager, volumes_factory: Callable) -> None:
    """
    Test that a node fails to start, with an error, if it must reshard while
    the disk utilization is above the critical level.

    Out-of-space prevention disables compaction before sstable loading
    starts, so resharding cannot run. Startup must then fail ("Startup
    failed", non-zero exit) instead of the node booting without part of its
    data.

    Scenario:
      - Start a node with 2 shards and write data into a vnodes table.
      - Stop the node and create a big file to push the disk over the
        critical utilization level.
      - Start the node with 3 shards and expect startup to fail with an
        error.
    """
    async with space_limited_servers(manager, volumes_factory, ["20M"], cmdline=[*global_cmdline, "--smp", "2"]) as servers:
        server = servers[0]
        cql, _ = await manager.get_ready_cql(servers)
        workdir = await manager.server_get_workdir(server.server_id)
        await populate_vnodes_table(manager, cql, server)

        await manager.server_stop_gracefully(server.server_id)
        await manager.server_update_cmdline(server.server_id, ["--smp", "3"])

        logger.info("Create a big file on the node to reach critical disk utilization level")
        disk_info = psutil.disk_usage(workdir)
        with random_content_file(workdir, int(disk_info.total*DISK_FILL_TARGET_RATIO) - disk_info.used):
            logger.info("Start the node with 3 shards and expect the boot to fail")
            await manager.server_start(server.server_id,
                                       expected_error="Startup failed: .*populating keyspace.*critical disk utilization")


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_boot_fails_when_resharding_is_drained(manager: ScyllaClusterManager, volumes_factory: Callable) -> None:
    """
    Test that a node fails to start, with an error, if out-of-space
    prevention drains the compaction manager while boot-time resharding
    runs.

    Resharding must process every shared sstable before the table becomes
    writable. If the drain stops it halfway, the sstables that were not yet
    resharded stay on disk, and the table never loads them. Startup must
    then fail ("Startup failed", non-zero exit) instead of the node booting
    without them.

    Scenario:
      - Start a node with 2 shards and write data into a vnodes table.
      - Restart the node with 3 shards to trigger resharding, and pause the
        reshard jobs of the table.
      - Create a big file to push the node over the critical disk utilization
        level, so that the disk space monitor drains the compaction manager
        and stops the paused jobs.
      - Expect startup to fail with an error.

    Reproduces https://scylladb.atlassian.net/browse/SCYLLADB-5324.
    """
    shards = 2
    async with space_limited_servers(manager, volumes_factory, ["20M"], cmdline=[*global_cmdline, "--smp", str(shards)]) as servers:
        server = servers[0]
        cql, _ = await manager.get_ready_cql(servers)
        workdir = await manager.server_get_workdir(server.server_id)
        log = await manager.server_open_log(server.server_id)
        ks, table = await populate_vnodes_table(manager, cql, server)

        logger.info(f"Restart the node with {shards + 1} shards and pause the reshard jobs of the table")
        start_task, mark = await restart_with_paused_resharding(
            manager, server, log, table, shards, expected_error=f"Exception while populating keyspace '{ks}' with column family '{table}'")
        try:
            mark = await wait_for_paused_reshard_jobs(log, mark, start_task, jobs=shards)

            logger.info("Create a big file on the node to reach critical disk utilization level")
            disk_info = psutil.disk_usage(workdir)
            with random_content_file(workdir, int(disk_info.total*DISK_FILL_TARGET_RATIO) - disk_info.used):
                # The drain stops the paused jobs, and startup must fail.
                await start_task
        finally:
            if not start_task.done():
                start_task.cancel()

        assert await log.grep("Startup failed", from_mark=mark)
        assert await log.grep(f"Compaction for {ks}/{table} was stopped due to: drain", from_mark=mark)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_boot_fails_when_resharding_is_aborted(manager: ScyllaClusterManager) -> None:
    """
    Test that a node fails to start, with an error, if a user aborts
    boot-time resharding through the task manager.

    Same as test_boot_fails_when_resharding_is_drained, but a user stops the
    reshard jobs by aborting the resharding task of the table through the
    task manager, instead of out-of-space prevention draining the compaction
    manager.

    Scenario:
      - Start a node with 2 shards and write data into a vnodes table.
      - Restart the node with 3 shards to trigger resharding, and pause the
        reshard jobs of the table.
      - Abort the resharding task of the table through the task manager,
        which stops the paused jobs.
      - Expect startup to fail with an error.

    Reproduces https://scylladb.atlassian.net/browse/SCYLLADB-5324.
    """
    shards = 2
    server = await manager.server_add(cmdline=["--smp", str(shards)])
    cql, _ = await manager.get_ready_cql([server])
    log = await manager.server_open_log(server.server_id)
    ks, table = await populate_vnodes_table(manager, cql, server)

    logger.info(f"Restart the node with {shards + 1} shards and pause the reshard jobs of the table")
    start_task, mark = await restart_with_paused_resharding(
        manager, server, log, table, shards, expected_error=f"Exception while populating keyspace '{ks}' with column family '{table}'")
    try:
        mark = await wait_for_paused_reshard_jobs(log, mark, start_task, jobs=shards)

        logger.info("Abort the resharding task of the table and expect the boot to fail")
        tm = TaskManagerClient(manager.api)
        tasks = await tm.list_tasks(server.ip_addr, "compaction", internal=True, keyspace=ks, table=table)
        [task] = [t for t in tasks if t.type == "resharding compaction" and t.scope == "table"]
        await tm.abort_task(server.ip_addr, task.task_id)
        await start_task
    finally:
        if not start_task.done():
            start_task.cancel()

    assert await log.grep("Startup failed", from_mark=mark)
    assert await log.grep(f"Compaction for {ks}/{table} was stopped due to: user requested abort", from_mark=mark)
