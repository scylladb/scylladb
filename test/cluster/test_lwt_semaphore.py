#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import time
from test.pylib.rest_client import inject_error
from test.pylib.util import wait_for_cql
import pytest
from cassandra.protocol import WriteTimeout
from test.cluster.util import new_test_keyspace


@pytest.mark.skip_mode(mode='debug', reason='aarch64/debug is unpredictably slow', platform_key='aarch64')
async def test_cas_semaphore(manager):
    """ This is a regression test for scylladb/scylladb#19698 """
    servers = await manager.servers_add(1, cmdline=['--smp', '1'])

    await wait_for_cql(manager.cql, time.time() + 60)

    async with new_test_keyspace(manager, "WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        table = f"{ks}.test"
        await manager.cql.run_async(f"CREATE TABLE {table} (a int PRIMARY KEY, b int)")

        # Lower write timeout only for the CAS phase, after paxos state table
        # creation (which goes through raft) has had time to complete with the
        # default timeout.
        #
        # write_request_timeout_in_ms is only read when a CQL connection is
        # established (it's snapshotted into that connection's client_state),
        # so a fresh, exclusive connection is required for the new value to
        # actually apply: manager.cql's existing connection would keep using
        # whatever timeout was in effect when it first connected.
        await manager.server_update_config(servers[0].server_id, 'write_request_timeout_in_ms', 500)
        cql = await manager.get_cql_exclusive(servers[0])

        async with inject_error(manager.api, servers[0].ip_addr, 'cas_timeout_after_lock'):
            res = [cql.run_async(f"INSERT INTO {table} (a) VALUES (0) IF NOT EXISTS") for r in range(10)]
            try:
                await asyncio.gather(*res)
            except WriteTimeout:
                pass

        # Restore a generous timeout so the second batch isn't affected by
        # lingering raft apply latency. Again requires a fresh connection to
        # take effect, for the same reason as above.
        await manager.server_update_config(servers[0].server_id, 'write_request_timeout_in_ms', 10000)
        cql = await manager.get_cql_exclusive(servers[0])

        res = [cql.run_async(f"INSERT INTO {table} (a) VALUES (0) IF NOT EXISTS") for r in range(10)]
        await asyncio.gather(*res)

        metrics = await manager.metrics.query(servers[0].ip_addr)
        contention = metrics.get(name="scylla_storage_proxy_coordinator_cas_write_contention_count")

        assert contention == None
