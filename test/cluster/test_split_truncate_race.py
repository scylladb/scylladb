#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
import random

import pytest

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)


# A tablet split rewrites a compaction group's sstables in a task that first
# captures the sstables, and only then is registered with the compaction
# manager. A TRUNCATE in between finds no task to stop, discards all the
# table's sstables - including the captured ones - and then re-enables
# compaction, so the split task goes on to rewrite sstables which are no longer
# in the table. When it replaced them, the node aborted with "Unable to remove
# input SSTable" (SCYLLADB-6095, seen with a DROP TABLE, which truncates the
# table). Check that the node survives this, and that the truncated data stays
# deleted.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_truncate_while_split_captured_sstables(manager: ScyllaClusterManager):
    server = await manager.server_add(cmdline=['--target-tablet-size-in-bytes', '30000'],
                                      config={'tablet_load_stats_refresh_interval_in_seconds': 1})
    await manager.disable_tablet_balancing()
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        table = f'{ks}.test'
        await cql.run_async(f'CREATE TABLE {table} (pk int PRIMARY KEY, c blob)')
        # 60 KB of data in one tablet is above the 30 KB target tablet size,
        # so the load balancer decides to split it.
        insert = cql.prepare(f'INSERT INTO {table} (pk, c) VALUES (?, ?)')
        await asyncio.gather(*[cql.run_async(insert, [pk, random.randbytes(1000)]) for pk in range(60)])
        await manager.api.flush_keyspace(server.ip_addr, ks)

        injection = 'split_compaction_after_capturing_sstables'
        await manager.api.enable_injection(server.ip_addr, injection, one_shot=True)
        await manager.enable_tablet_balancing()
        await manager.api.wait_for_injection_enter(server.ip_addr, injection)

        await cql.run_async(f'TRUNCATE TABLE {table}')

        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()
        await manager.api.message_injection(server.ip_addr, injection)

        # Check that the split skipped the sstable that the truncate had
        # already deleted, instead of trying to replace it (which aborted the
        # node), and that it didn't bring back the truncated data.
        await log.wait_for('which was already deleted', from_mark=mark, timeout=60)
        assert not await log.grep('Unable to remove input SSTable', from_mark=mark)
        assert [] == list(await cql.run_async(f'SELECT * FROM {table}'))
