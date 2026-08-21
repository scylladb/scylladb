#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
import pytest

from test.pylib.manager_client import ManagerClient
from test.pylib.repair import create_table_insert_data_for_repair


logger = logging.getLogger(__name__)


async def wait_for_cleanup_log(logs, marks, timeout: float = 15) -> list[tuple[str, object]]:
    pattern = r"Session .* (closed, removing repair_meta_id|already closed, removing late repair_meta_id) .* for node"
    tasks = [
        asyncio.create_task(log.wait_for(pattern, from_mark=mark, timeout=timeout))
        for log, mark in zip(logs, marks)
    ]
    try:
        for task in asyncio.as_completed(tasks):
            try:
                _, matches = await task
            except TimeoutError:
                continue
            if matches:
                return matches
        return []
    finally:
        for task in tasks:
            task.cancel()


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_session_abort_cleans_orphan_repair_meta(manager: ManagerClient):
    """Verifies that orphaned repair_meta on a follower does not block
    drain_closing_sessions() thanks to the session abort_source mechanism.

    Reproduces the scenario from SCYLLADB-1805:
    1. A tablet repair creates repair_meta on a follower with a session guard.
    2. The follower's repair_row_level_stop handler skips cleanup (simulating
       the stop RPC never properly cleaning up, leaving an orphan repair_meta
       holding the session gate).
    3. The topology coordinator advances past repair → end_repair, closing the
       session and issuing a barrier_and_drain.
    4. Without the fix, drain_closing_sessions() would hang because the orphan
       repair_meta holds the session gate. With the fix, the session's
       abort_source fires on close, which cleans up the orphan repair_meta,
       allowing drain to complete and the tablet repair to finish.
    """
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(
        manager, rf=3, tablets=1, nr_keys=10,
        cmdline=['--logger-log-level', 'repair=info', '--logger-log-level', 'session=info'])

    # token=-1 means repair all tablets in the table
    token = -1

    # Enable injection on ALL nodes to skip repair_meta cleanup in
    # repair_row_level_stop_handler. This simulates an orphan repair_meta
    # remaining on the follower after repair completes.
    for s in servers:
        await manager.api.enable_injection(s.ip_addr, "repair_row_level_stop_skip_cleanup", one_shot=True)

    # Open logs to verify the cleanup via abort_source
    logs = [await manager.server_open_log(s.server_id) for s in servers]
    marks = [await log.mark() for log in logs]

    # Trigger tablet repair. This will:
    # 1. Run row-level repair (repair_meta created on follower with session guard)
    # 2. Stop handler skips cleanup (orphan repair_meta left on follower)
    # 3. Coordinator advances to end_repair → barrier_and_drain
    # 4. Session close fires abort_source → orphan cleaned up
    # 5. drain_closing_sessions completes → repair finishes
    # Trigger tablet repair with a timeout to avoid hanging if the fix regresses.
    await asyncio.wait_for(
        manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token),
        timeout=120)

    # Verify the session abort mechanism fired by checking logs.
    # Cleanup can happen either from the abort_source callback or immediately
    # when the follower races with a session that already started closing.
    cleanup_msgs = await wait_for_cleanup_log(logs, marks)
    assert len(cleanup_msgs) > 0, \
        "Expected session abort_source cleanup log message for orphaned repair_meta"
    logger.info(f"Found {len(cleanup_msgs)} abort_source cleanup message(s)")
