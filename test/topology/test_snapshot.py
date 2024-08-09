#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test snapshot transfer by forcing threshold and performing schema changes
"""
import asyncio
import logging
from test.pylib.rest_client import inject_error_one_shot, inject_error
import pytest
from cassandra.query import SimpleStatement              # type: ignore # pylint: disable=no-name-in-module


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_snapshot(manager, random_tables):
    """
        Cluster A, B, C
        with reduced snapshot threshold create table, do several schema changes.
        Start a new server D and it should get a snapshot on bootstrap.
        Then stop A B C and query D to check it sees the correct table schema (verify_schema).
    """
    server_a, server_b, server_c = await manager.running_servers()
    await manager.mark_dirty()
    # Reduce the snapshot thresholds
    errs = [inject_error_one_shot(manager.api, s.ip_addr, "raft_server_set_snapshot_thresholds",
                                  parameters={'snapshot_threshold': '3', 'snapshot_trailing': '1'})
            for s in [server_a, server_b, server_c]]
    await asyncio.gather(*errs)

    t = await random_tables.add_table(ncolumns=5, pks=1)

    for i in range(3):
        await t.add_column()

    manager.driver_close()
    server_d = await manager.server_add()
    logger.info("Started D %s", server_d)

    logger.info("Stopping A %s, B %s, and C %s", server_a, server_b, server_c)
    await asyncio.gather(*[manager.server_stop_gracefully(s.server_id)
                           for s in [server_a, server_b, server_c]])

    logger.info("Driver connecting to D %s", server_d)
    await manager.driver_connect()

    await random_tables.verify_schema(do_read_barrier=False)

    # Start servers to have quorum for post-test checkup
    # TODO: remove once there's a way to disable post-test checkup
    manager.driver_close()
    logger.info("Starting A %s", server_a)
    await manager.server_start(server_a.server_id)
    logger.info("Starting B %s", server_b)
    await manager.server_start(server_b.server_id)
    await manager.driver_connect()
    logger.info("Test DONE")
