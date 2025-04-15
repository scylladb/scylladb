#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from test.pylib.manager_client import ManagerClient
from test.cluster.util import new_test_keyspace

import pytest
import asyncio
import logging
import time

logger = logging.getLogger(__name__)

# Reproducer for https://github.com/scylladb/scylladb/issues/20916.
@pytest.mark.asyncio
async def test_compacting_reader_tombstone_gc_with_data_in_memtable(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'table=debug',
        '--logger-log-level', 'mutation_compactor=debug',
    ]
    servers = [await manager.server_add(cmdline=cmdline)]

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};") as ks:
        table = f"{ks}.test"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, c int) WITH gc_grace_seconds = 0;")

        await manager.api.disable_autocompaction(servers[0].ip_addr, ks)

        key = 7 # Whatever

        # Simulates scenario where node missed tombstone and has it written to sstable directly
        # after repair, whereas the deleted data remains on memtable due to low write activity.

        # write a expiring tombstone into a sstable (flushed below)
        await cql.run_async(f'DELETE FROM {table} USING timestamp 10 WHERE pk = {key}')

        # waits for tombstone to expire
        time.sleep(1)

        # system-wide flush to prevent CL segment from blocking tombstone GC in the read path.
        await manager.api.flush_all_keyspaces(servers[0].ip_addr)

        # write into memtable data shadowed by the tombstone now living in the sstable
        await cql.run_async(f'INSERT INTO {table} (pk, c) VALUES ({key}, 0) USING timestamp 9')

        await manager.api.drop_sstable_caches(servers[0].ip_addr)

        # Without cache, the compacting reader is bypassed; Verify that the data in memtable is discarded
        bypass_cache_rows = cql.execute(f'SELECT pk, c FROM {table} WHERE pk = {key} BYPASS CACHE;')
        assert len(list(bypass_cache_rows)) == 0

        # With the cache, the compacting reader is involved;
        # Verify that the tombstone is not purged, allowing it to shadow the data in memtable
        through_cache_rows = cql.execute(f'SELECT pk, c FROM {table} WHERE pk = {key};')
        assert len(list(through_cache_rows)) == 0
