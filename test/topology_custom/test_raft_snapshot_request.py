#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import asyncio
import pytest
import time
import logging

from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts, read_barrier
from test.topology.util import reconnect_driver


logger = logging.getLogger(__name__)


async def get_raft_log_size(cql, host) -> int:
    query = "select count(\"index\") from system.raft"
    return (await cql.run_async(query, host=host))[0][0]


async def get_raft_snap_id(cql, host) -> str:
    query = "select snapshot_id from system.raft_snapshots"
    return (await cql.run_async(query, host=host))[0].snapshot_id


async def trigger_snapshot(manager: ManagerClient, group0_id: str, ip_addr) -> None:
    await manager.api.client.post(f"/raft/trigger_snapshot/{group0_id}", host=ip_addr)


@pytest.mark.asyncio
async def test_raft_snapshot_request(manager: ManagerClient):
    cmdline = [
        '--logger-log-level', 'raft=trace',
        ]
    servers = [await manager.server_add(cmdline=cmdline) for _ in range(3)]
    cql = manager.get_cql()

    s1 = servers[0]
    h1 = (await wait_for_cql_and_get_hosts(cql, [s1], time.time() + 60))[0]
    group0_id = (await cql.run_async(
        "select value from system.scylla_local where key = 'raft_group0_id'",
        host=h1))[0].value

    # Verify that snapshotting updates the snapshot ID and truncates the log.
    log_size = await get_raft_log_size(cql, h1)
    logger.info(f"Log size on {s1}: {log_size}")
    snap_id = await get_raft_snap_id(cql, h1)
    logger.info(f"Snapshot ID on {s1}: {snap_id}")
    assert log_size > 0
    await trigger_snapshot(manager, group0_id, s1.ip_addr)
    new_log_size = await get_raft_log_size(cql, h1)
    logger.info(f"New log size on {s1}: {new_log_size}")
    new_snap_id = await get_raft_snap_id(cql, h1)
    logger.info(f"New snapshot ID on {s1}: {new_snap_id}")
    assert new_log_size == 0
    assert new_snap_id != snap_id

    # If a server misses a command and a snapshot is created on the leader,
    # the server once alive should eventually receive that snapshot.
    s2 = servers[2]
    h2 = (await wait_for_cql_and_get_hosts(cql, [s2], time.time() + 60))[0]
    s2_log_size = await get_raft_log_size(cql, h2)
    logger.info(f"Log size on {s2}: {s2_log_size}")
    s2_snap_id = await get_raft_snap_id(cql, h2)
    logger.info(f"Snapshot ID on {s2}: {s2_snap_id}")
    await manager.server_stop_gracefully(s2.server_id)
    logger.info(f"Stopped {s2}")
    # Restarting the two servers will cause a newly elected leader to append a dummy command.
    await asyncio.gather(*(manager.server_restart(s.server_id) for s in servers[:2]))
    logger.info(f"Restarted {servers[:2]}")
    cql = await reconnect_driver(manager)
    # Wait for one server to append the command and do a read_barrier on the other
    # to make sure both appended
    async def appended_command() -> int | None:
        await wait_for_cql_and_get_hosts(cql, [s1], time.time() + 60)
        s = await get_raft_log_size(cql, h1)
        if s > 0:
            return s
        return None
    log_size = await wait_for(appended_command, time.time() + 60)
    logger.info(f"{servers[0]} appended new command")
    h = (await wait_for_cql_and_get_hosts(cql, [servers[1]], time.time() + 60))[0]
    await read_barrier(cql, h)
    logger.info(f"Read barrier done on {servers[1]}")
    # We don't know who the leader is, so trigger a snapshot on both servers.
    for s in servers[:2]:
        await trigger_snapshot(manager, group0_id, s.ip_addr)
        h = (await wait_for_cql_and_get_hosts(cql, [s], time.time() + 60))[0]
        snap = await get_raft_snap_id(cql, h)
        logger.info(f"New snapshot ID on {s}: {snap}")
    await manager.server_start(s2.server_id)
    logger.info(f"Server {s2} restarted")
    cql = await reconnect_driver(manager)
    await wait_for_cql_and_get_hosts(cql, [s2], time.time() + 60)
    async def received_snapshot() -> str | None:
        new_s2_snap_id = await get_raft_snap_id(cql, h2)
        if s2_snap_id != new_s2_snap_id:
            return new_s2_snap_id
        return None
    new_s2_snap_id = await wait_for(received_snapshot, time.time() + 60)
    logger.info(f"{s2} received new snapshot: {new_s2_snap_id}")
    new_s2_log_size = await get_raft_log_size(cql, h2)
    # Log size may be 1 because topology coordinator fiber may start and commit an entry
    # after that last snapshot was created (the two events race with each other).
    assert new_s2_log_size <= 1
