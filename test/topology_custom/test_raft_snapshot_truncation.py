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
from test.pylib.rest_client import inject_error_one_shot

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
@pytest.mark.xfail(reason="issue #16817")
async def test_raft_snapshot_truncation(manager: ManagerClient):
    """
    Check that after snapshot creation, snapshot_trailing_size is taken into consideration,
      issue https://github.com/scylladb/scylladb/issues/16817

     1. Start 3 nodes.
     2. Set snapshot thresholds to 0 to trigger snapshot creation on the next schema change
        also set snapshot_trailing to 5 and snapshot_trailing_size to 0, practially setting
        desired number of log entries after log trancation to 0
     3. Generate a schema change event.
     4. Check that truncated log contains 0 entries.
    """

    cmdline = [
        '--logger-log-level', 'raft=trace',
    ]
    servers = await manager.servers_add(3, cmdline=cmdline)
    cql = manager.get_cql()

    s1 = servers[0]
    h1 = (await wait_for_cql_and_get_hosts(cql, [s1], time.time() + 60))[0]

    log_size = await get_raft_log_size(cql, h1)
    logger.info(f"Log size on {s1}: {log_size}")
    assert (log_size > 0)

    await cql.run_async("create keyspace ks with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")

    log_size = await get_raft_log_size(cql, h1)
    logger.info(f"After add keyspace Log size on {s1}: {log_size}")

    # Set up snapshot creation thresholds and trailing items
    errs = [inject_error_one_shot(manager.api, s.ip_addr, "raft_server_set_snapshot_thresholds",
                                  parameters={'snapshot_threshold': '0', 'snapshot_threshold_log_size': '0',
                                              'snapshot_trailing': '5', 'snapshot_trailing_size': '0'})
            for s in servers]
    await asyncio.gather(*errs)

    # Change schema - trigger log truncation.
    await cql.run_async("drop keyspace ks")

    log_size = await get_raft_log_size(cql, h1)
    logger.info(f"After drop keyspace Log size on {s1}: {log_size}")

    # Verify that after the snapshot was created, log size is 0 due to setting 'snapshot_trailing_size': '0'.
    assert (log_size == 0)
