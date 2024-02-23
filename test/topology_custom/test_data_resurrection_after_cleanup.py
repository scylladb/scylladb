#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error_one_shot
from test.topology.conftest import skip_mode
from test.topology.util import check_token_ring_and_group0_consistency

import pytest
import asyncio
import logging
import time

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_data_resurrection_after_cleanup(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'table=debug',
        '--smp', '1',
    ]
    servers = [await manager.server_add(cmdline=cmdline)]

    cql = manager.get_cql()

    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int) WITH gc_grace_seconds=0;")

    keys = range(256)
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, {k});") for k in keys])

    async def check(expected_keys):
        logger.info("Checking table")
        cql = manager.get_cql()
        rows = await cql.run_async("SELECT * FROM test.test;")
        assert len(rows) == len(expected_keys)
        for r in rows:
            assert r.c == r.pk

    await manager.api.flush_keyspace(servers[0].ip_addr, "test")

    await check(keys)

    logger.info("Adding new server")
    servers.append(await manager.server_add(cmdline=cmdline))

    time.sleep(1)
    await check(keys)

    await inject_error_one_shot(manager.api, servers[0].ip_addr, "major_compaction_before_cleanup")
    await manager.api.cleanup_keyspace(servers[0].ip_addr, "test")

    deleted_keys = range(128)
    await asyncio.gather(*[cql.run_async(f"DELETE FROM test.test WHERE pk={k};") for k in deleted_keys])
    # Make sures tombstones are gone
    await manager.api.flush_keyspace(servers[1].ip_addr, "test")
    time.sleep(1)
    await manager.api.keyspace_compaction(servers[1].ip_addr, "test")

    # Regains ownership of deleted data

    logger.info(f"Decommissioning node {servers[1]}")
    await manager.decommission_node(servers[1].server_id)
    await check_token_ring_and_group0_consistency(manager)

    time.sleep(1)
    await check(range(128))
