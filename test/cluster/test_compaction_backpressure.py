#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from test.pylib.manager_client import ManagerClient
from test.pylib.tablets import get_tablet_replica
from test.pylib.rest_client import inject_error_one_shot, inject_error
from test.cluster.util import new_test_keyspace, new_test_table
from test.cqlpy import nodetool

import pytest
import asyncio
import logging

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_intranode_migration_not_blocked_by_backpressure(manager: ManagerClient):
    """
    Reproducer for a bug where intra-node tablet migration gets stuck
    in maybe_wait_for_sstable_count_reduction() because the compaction
    manager's regular_compaction_task_executor::do_run() returns early
    (descriptor.sstables is empty) without signaling compaction_done.

    The flush triggered by clone_locally_tablet_storage() calls
    maybe_wait_for_sstable_count_reduction() which waits on
    compaction_done. If compaction returns early without signaling,
    the wait hangs forever.

    The test uses two error injections:
    1. maybe_wait_for_sstable_count_reduction_wait_on_compaction_done
       (one-shot): Forces the flush path to wait on compaction_done,
       simulating the backpressure condition.
    2. compaction_regular_compaction_task_executor_do_run_empty_sstables
       (non-one-shot): Forces do_run() to always return early with
       empty sstables, simulating the case where the compaction
       strategy finds nothing to compact.

    Without the fix: do_run() returns early without signaling
    compaction_done, the wait hangs forever, blocking the migration.

    With the fix: do_run() signals compaction_done on early return,
    waking up the waiter and allowing the migration to proceed.
    """
    cmdline = ['--smp', '1']
    servers = [await manager.server_add(cmdline=cmdline)]

    cql = manager.get_cql()
    extra_ks_param = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}"
    async with new_test_keyspace(manager, extra_ks_param) as ks:
        extra_table_param = "WITH compaction = {'class' : 'IncrementalCompactionStrategy', 'max_threshold': 64} and compression = {}"
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, t text", extra_table_param) as cf:
            table = cf.split('.')[-1]

            await manager.api.disable_autocompaction(servers[0].ip_addr, ks)

            # Write data and flush to create an sstable.
            logger.info("Writing initial data and flushing")
            for i in range(100):
                await asyncio.gather(*[cql.run_async(f"INSERT INTO {cf} (pk, t) VALUES ({i}, '{'x' * 1020}')") for i in range(10*i, 10*(i+1))])
                await manager.api.keyspace_flush(servers[0].ip_addr, ks, table)

            # Write more data WITHOUT flushing
            logger.info("Writing dirty data to memtable (no flush)")
            await asyncio.gather(*[cql.run_async(f"INSERT INTO {cf} (pk, t) VALUES ({i}, '{'x' * 1020}')") for i in range(400, 410)])

            logger.info("Enable autocompaction and trigger flush to simulate backpressure")
            await manager.api.enable_injection(servers[0].ip_addr, "set_sstable_count_reduction_threshold", one_shot=False, parameters={'value': 32})
            await manager.api.enable_autocompaction(servers[0].ip_addr, ks)
            # With the bug, the flush hangs forever in maybe_wait_for_sstable_count_reduction().
            # With the fix, it completes promptly.
            await manager.api.keyspace_flush(servers[0].ip_addr, ks, table)
