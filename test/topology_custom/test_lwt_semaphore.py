#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import asyncio
import time
from test.pylib.rest_client import inject_error
from test.pylib.util import wait_for_cql_and_get_hosts
import pytest
from cassandra.protocol import WriteTimeout

@pytest.mark.asyncio
async def test_cas_semaphore(manager):
    """ This is a regression test for scylladb/scylladb#19698 """
    servers = await manager.servers_add(1, cmdline=['--smp', '1', '--write-request-timeout-in-ms', '500'])

    host = await wait_for_cql_and_get_hosts(manager.cql, {servers[0]}, time.time() + 60)

    await manager.cql.run_async("CREATE KEYSPACE test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    await manager.cql.run_async("CREATE TABLE test.test (a int PRIMARY KEY, b int)")

    async with inject_error(manager.api, servers[0].ip_addr, 'cas_timeout_after_lock'):
        res = [manager.cql.run_async(f"INSERT INTO test.test (a) VALUES (0) IF NOT EXISTS", host=host[0]) for r in range(10)]
        try:
            await asyncio.gather(*res)
        except WriteTimeout:
            pass

    res = [manager.cql.run_async(f"INSERT INTO test.test (a) VALUES (0) IF NOT EXISTS", host=host[0]) for r in range(10)]
    await asyncio.gather(*res)

    metrics = await manager.metrics.query(servers[0].ip_addr)
    contention = metrics.get(name="scylla_storage_proxy_coordinator_cas_write_contention_count")

    assert contention == None
