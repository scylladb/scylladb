#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error_one_shot
from test.cluster.conftest import skip_mode
from test.cluster.util import check_token_ring_and_group0_consistency, new_test_keyspace

import pytest
import asyncio
import logging
import time

logger = logging.getLogger(__name__)
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_cleanup_stop(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'compaction_manager=debug',
        '--smp', '1',
    ]
    servers = [await manager.server_add(cmdline=cmdline)]

    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'enabled': 'false'};") as ks:
        table = f"{ks}.test"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, c int);")

        keys = range(100)
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {table} (pk, c) VALUES ({k}, {k});") for k in keys])
        async def check(expected_keys):
            logger.info("Checking table")
            cql = manager.get_cql()
            rows = await cql.run_async(f"SELECT * FROM {table};")
            assert len(rows) == len(expected_keys)
            for r in rows:
                assert r.c == r.pk

        await manager.api.flush_keyspace(servers[0].ip_addr, ks)

        await check(keys)

        s0_log = await manager.server_open_log(servers[0].server_id)
        s0_mark = await s0_log.mark()

        await inject_error_one_shot(manager.api, servers[0].ip_addr, "sstable_cleanup_wait")
        cleanup_task = asyncio.create_task(manager.api.cleanup_keyspace(servers[0].ip_addr, ks))

        await s0_log.wait_for('sstable_cleanup_wait: waiting', from_mark=s0_mark)

        stop_cleanup = asyncio.create_task(manager.api.stop_compaction(servers[0].ip_addr, "CLEANUP"))
        time.sleep(1)

        await manager.api.message_injection(servers[0].ip_addr, "sstable_cleanup_wait")
        await stop_cleanup
        caught_exception = False
        try:
            await cleanup_task
        except Exception as e:
            caught_exception = True
            logger.info(f"Exception: {e}")

        await check(keys)
        assert caught_exception == True

