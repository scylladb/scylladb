#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import glob
import os
import time

import pytest

from test.pylib.internal_types import ServerInfo
from test.pylib.rest_client import read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for

PAUSE_DROP = "pause_legacy_large_data_tables_drop"

# The production default, in seconds. The test suite uses 5 minutes, so a
# group 0 operation that waits for the paused drop would fail slowly.
GROUP0_TIMEOUT = 60


async def wait_for_legacy_tables_dropped(manager: ScyllaClusterManager, server: ServerInfo) -> None:
    workdir = await manager.server_get_workdir(server.server_id)
    legacy_table_dirs = os.path.join(workdir, "data", "system", "large_*-*")

    async def dropped():
        return None if glob.glob(legacy_table_dirs) else True

    await wait_for(dropped, time.time() + 60)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.xfail(reason="SCYLLADB-4940")
async def test_join_does_not_wait_for_legacy_tables_drop(manager: ScyllaClusterManager):
    """
    Reproduces SCYLLADB-4940. A joining node replaces the legacy system.large_*
    tables with virtual tables when it loads the group 0 snapshot. The join
    must not wait until the legacy tables are dropped from disk.
    """
    await manager.server_add()
    config = {
        'error_injections_at_startup': [PAUSE_DROP],
        'group0_raft_op_timeout_in_ms': GROUP0_TIMEOUT * 1000,
    }
    server = await manager.server_add(start=False, config=config)
    log = await manager.server_open_log(server.server_id)
    start = asyncio.create_task(manager.server_start(server.server_id))
    try:
        await log.wait_for(f"{PAUSE_DROP}: waiting for message")
        # Only the join must not wait for the drop.
        joined = asyncio.create_task(log.wait_for("join: success"))
        join_failed = asyncio.create_task(log.wait_for("will not join the cluster"))
        done, _ = await asyncio.wait([joined, join_failed], return_when=asyncio.FIRST_COMPLETED)
        joined.cancel()
        join_failed.cancel()
        assert joined in done, "The join waits until the legacy tables are dropped"
    finally:
        await manager.api.message_injection(server.ip_addr, PAUSE_DROP)
        await asyncio.gather(start, return_exceptions=True)
    await start
    cql, hosts = await manager.get_ready_cql([server])
    await cql.run_async("SELECT * FROM system.large_partitions", host=hosts[0])
    await wait_for_legacy_tables_dropped(manager, server)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.xfail(reason="SCYLLADB-4940")
async def test_group0_does_not_wait_for_legacy_tables_drop(manager: ScyllaClusterManager):
    """
    Reproduces SCYLLADB-4940. When LARGE_DATA_VIRTUAL_TABLES gets enabled in a
    running cluster, each node replaces the legacy system.large_* tables while
    it applies the group 0 command. Group 0 must keep working until the legacy
    tables are dropped from disk.
    """
    suppress = {'name': 'suppress_features', 'value': 'LARGE_DATA_VIRTUAL_TABLES'}
    servers = await manager.servers_add(2, config={'error_injections_at_startup': [suppress]})
    for server in servers:
        await manager.server_update_config(server.server_id, 'error_injections_at_startup', [PAUSE_DROP])
    await manager.server_restart(servers[0].server_id)
    # The feature gets enabled once the second node supports it too.
    restart = asyncio.create_task(manager.server_restart(servers[1].server_id))
    try:
        for server in servers:
            await manager.api.wait_for_injection_enter(server.ip_addr, PAUSE_DROP)
            await read_barrier(manager.api, server.ip_addr, timeout=GROUP0_TIMEOUT)
        await restart
        cql, hosts = await manager.get_ready_cql(servers)
        for host in hosts:
            await cql.run_async("SELECT * FROM system.large_partitions", host=host)
    finally:
        for server in servers:
            await manager.api.message_injection(server.ip_addr, PAUSE_DROP)
        await asyncio.gather(restart, return_exceptions=True)
    for server in servers:
        await wait_for_legacy_tables_dropped(manager, server)
