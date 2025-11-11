#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from test.pylib.manager_client import ManagerClient
from test.cluster.util import new_test_keyspace
from test.cluster.object_store.test_backup import take_snapshot

import pytest
import asyncio
import logging
import time

logger = logging.getLogger(__name__)
@pytest.mark.asyncio
async def test_cleanup_retry(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'compaction_manager=debug',
    ]
    servers = [await manager.server_add(cmdline=cmdline)]

    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'enabled': 'false'};") as ks:
        table = f"{ks}.test"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, c int);")

        keys = range(100)
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {table} (pk, c) VALUES ({k}, {k});") for k in keys])

        await manager.api.disable_autocompaction(servers[0].ip_addr, ks, "test")

        async def check(expected_keys):
            logger.info("Checking table")
            cql = manager.get_cql()
            rows = await cql.run_async(f"SELECT * FROM {table};")
            assert len(rows) == len(expected_keys)
            for r in rows:
                assert r.c == r.pk

        await manager.api.flush_keyspace(servers[0].ip_addr, ks)
        _, base_sstables = await take_snapshot(ks, servers[:1], manager, logger)
        await check(keys)

        await manager.api.cleanup_keyspace(servers[0].ip_addr, ks)
        _, sstables_after_cleanup = await take_snapshot(ks, servers[:1], manager, logger)
        await check(keys)

        assert set(sstables_after_cleanup) == set(base_sstables)

        servers.append(await manager.server_add(cmdline=cmdline))

        await manager.api.cleanup_keyspace(servers[0].ip_addr, ks)
        _, sstables_after_bootstrap = await take_snapshot(ks, servers[:1], manager, logger)
        await check(keys)

        assert set(sstables_after_bootstrap) != set(base_sstables)

        await manager.api.cleanup_keyspace(servers[0].ip_addr, ks)
        _, sstables_after_cleanup_retry = await take_snapshot(ks, servers[:1], manager, logger)
        await check(keys)

        assert set(sstables_after_cleanup_retry) == set(sstables_after_bootstrap)
