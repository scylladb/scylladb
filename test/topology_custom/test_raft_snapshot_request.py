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
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts
from test.topology.util import reconnect_driver, trigger_snapshot, get_topology_coordinator, get_raft_log_size, get_raft_snap_id


logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_raft_snapshot_request(manager: ManagerClient):
    cmdline = [
        '--logger-log-level', 'raft=trace',
        ]
    servers = await manager.servers_add(3, cmdline=cmdline)
    cql = manager.get_cql()

    s1 = servers[0]
    h1 = (await wait_for_cql_and_get_hosts(cql, [s1], time.time() + 60))[0]

    # Verify that snapshotting updates the snapshot ID and truncates the log.
    log_size = await get_raft_log_size(cql, h1)
    logger.info(f"Log size on {s1}: {log_size}")
    snap_id = await get_raft_snap_id(cql, h1)
    logger.info(f"Snapshot ID on {s1}: {snap_id}")
    assert log_size > 0
    await trigger_snapshot(manager, s1)
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
    # Any of the two restarted servers could have become the leader.
    # Find the leader and let s1 point to it for simplicity.
    leader_host_id = await get_topology_coordinator(manager)
    if leader_host_id != await manager.get_host_id(s1.server_id):
        s1 = servers[1]
    h1 = (await wait_for_cql_and_get_hosts(cql, [s1], time.time() + 60))[0]
    # Wait for the leader to append the command.
    async def appended_command() -> bool | None:
        s = await get_raft_log_size(cql, h1)
        return s > 0 or None
    await wait_for(appended_command, time.time() + 60)
    logger.info(f"{s1} appended new command")
    await trigger_snapshot(manager, s1)
    snap = await get_raft_snap_id(cql, h1)
    logger.info(f"New snapshot ID on {s1}: {snap}")
    await manager.server_start(s2.server_id)
    logger.info(f"Server {s2} restarted")
    cql = await reconnect_driver(manager)
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    async def received_snapshot() -> str | None:
        new_s2_snap_id = await get_raft_snap_id(cql, h2)
        if s2_snap_id != new_s2_snap_id:
            return new_s2_snap_id
        return None
    new_s2_snap_id = await wait_for(received_snapshot, time.time() + 60)
    logger.info(f"{s2} received new snapshot: {new_s2_snap_id}")
    new_s2_log_size = await get_raft_log_size(cql, h2)
    # The log size of s1 may be greater than 0 because topology coordinator fiber
    # may start and commit a few entries after the last snapshot was created.
    # However, the log size of s1 cannot be greater than the current leader's log
    # size. The leader's log was truncated, so the assertion below would fail if
    # s2 didn't truncate its log.
    current_s1_log_size = await get_raft_log_size(cql, h1)
    assert new_s2_log_size <= current_s1_log_size
