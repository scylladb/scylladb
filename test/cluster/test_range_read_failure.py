# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import asyncio

import pytest
from cassandra import ConsistencyLevel, ReadFailure, ReadTimeout
from cassandra.policies import FallthroughRetryPolicy
from cassandra.query import SimpleStatement

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.parametrize('tablets', [False, True])
@pytest.mark.parametrize('cl', [ConsistencyLevel.QUORUM, ConsistencyLevel.ALL])
@pytest.mark.parametrize('failure', ['abort', 'disconnect'])
async def test_range_read_replica_failure(manager: ScyllaClusterManager, tablets: bool, cl: int, failure: str):
    """A range read must use a spare replica after a shutdown error (#15899)."""
    servers = await manager.servers_add(3, auto_rack_dc='dc1')
    cql = await manager.get_cql_exclusive(servers[0])
    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} "
            f"AND tablets = {{'enabled': {str(tablets).lower()}}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int)")
        insert = SimpleStatement(f"INSERT INTO {ks}.t (pk, v) VALUES (0, 42)", consistency_level=ConsistencyLevel.ALL)
        await cql.run_async(insert)

        # Hold every possible remote target so that shutdown interrupts an
        # outstanding request, rather than happening before replica selection.
        for server in servers[1:]:
            await manager.api.enable_injection(server.ip_addr, 'storage_proxy::handle_read', one_shot=False,
                                               parameters={'cf_name': 't'})
        query = cql.run_async(SimpleStatement(f"SELECT * FROM {ks}.t USING TIMEOUT 10s", consistency_level=cl,
                                             retry_policy=FallthroughRetryPolicy()), timeout=30)

        async def wait_for_read(server):
            await manager.api.wait_for_injection_enter(server.ip_addr, 'storage_proxy::handle_read')
            return server

        tasks = [asyncio.create_task(wait_for_read(server)) for server in servers[1:]]
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            failed_server = next(iter(done)).result()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        for server in servers[1:]:
            if server != failed_server:
                await manager.api.message_injection(server.ip_addr, 'storage_proxy::handle_read')
                await manager.api.disable_injection(server.ip_addr, 'storage_proxy::handle_read')

        if failure == 'disconnect':
            await manager.server_stop(failed_server.server_id, convict=False)
        else:
            await manager.api.enable_injection(failed_server.ip_addr, 'storage_proxy::handle_read_abort', one_shot=True)
            await manager.api.message_injection(failed_server.ip_addr, 'storage_proxy::handle_read')
            await manager.api.disable_injection(failed_server.ip_addr, 'storage_proxy::handle_read')

        if cl == ConsistencyLevel.ALL:
            with pytest.raises(ReadTimeout if failure == 'disconnect' else ReadFailure):
                await query
        else:
            assert list(await query) == [(0, 42)]
