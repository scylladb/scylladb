#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from test.pylib.manager_client import ManagerClient
from test.pylib.util import gather_safely, wait_for
from test.cluster.util import new_test_keyspace
from test.pylib.internal_types import ServerInfo
from cassandra.protocol import InvalidRequest
from test.cluster.test_strong_consistency import wait_for_leader

import pytest
import logging
import time
import uuid

@pytest.mark.asyncio
async def test_cdc(manager: ManagerClient):
    config = {
        'experimental_features': ['strongly-consistent-tables']
    }
    cmdline = [
        '--logger-log-level', 'sc_groups_manager=debug',
        '--logger-log-level', 'sc_coordinator=debug'
    ]
    servers = await manager.servers_add(3, config=config, cmdline=cmdline, auto_rack_dc='my_dc')
    (cql, hosts) = await manager.get_ready_cql(servers)
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])

    def host_by_host_id(host_id):
        for hid, host in zip(host_ids, hosts):
            if hid == host_id:
                return host
        raise RuntimeError(f"Can't find host for host_id {host_id}")

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1} AND consistency = 'local'") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int) WITH cdc = {{'enabled': true}};")

        table_id = await manager.get_table_id(ks, 'test')
        rows = await cql.run_async(f"SELECT raft_group_id FROM system.tablets where table_id = {table_id}")
        group_id = str(rows[0].raft_group_id)
        leader_host_id = await wait_for_leader(manager, servers[0], group_id)
        leader_host = host_by_host_id(leader_host_id)

        for c in range(5):
            await cql.run_async(f"INSERT INTO {ks}.test(pk, c) VALUES (1, {c})", host=leader_host)
        cdc_stream = await cql.run_async(f"SELECT stream_id, stream_state FROM system.cdc_streams WHERE keyspace_name = '{ks}' AND table_name = 'test'")
        changes = await cql.run_async(f"SELECT * FROM {ks}.test_scylla_cdc_log WHERE \"cdc$stream_id\" = 0x{cdc_stream[0].stream_id.hex()}", host=leader_host)

        c_value = 0
        for row in changes:
            assert row.pk == 1
            assert row.c == c_value
            c_value += 1
        
@pytest.mark.asyncio
async def test_cdc_preimage(manager: ManagerClient):
    config = {
        'experimental_features': ['strongly-consistent-tables']
    }
    cmdline = [
        '--logger-log-level', 'sc_groups_manager=debug',
        '--logger-log-level', 'sc_coordinator=debug'
    ]
    servers = await manager.servers_add(3, config=config, cmdline=cmdline, auto_rack_dc='my_dc')
    (cql, hosts) = await manager.get_ready_cql(servers)
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])

    def host_by_host_id(host_id):
        for hid, host in zip(host_ids, hosts):
            if hid == host_id:
                return host
        raise RuntimeError(f"Can't find host for host_id {host_id}")

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1} AND consistency = 'local'") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int) WITH cdc = {{'enabled': true, 'preimage': true}};")

        table_id = await manager.get_table_id(ks, 'test')
        rows = await cql.run_async(f"SELECT raft_group_id FROM system.tablets where table_id = {table_id}")
        group_id = str(rows[0].raft_group_id)
        leader_host_id = await wait_for_leader(manager, servers[0], group_id)
        leader_host = host_by_host_id(leader_host_id)

        for c in range(5):
            await cql.run_async(f"INSERT INTO {ks}.test(pk, c) VALUES (1, {c})", host=leader_host)
        cdc_stream = await cql.run_async(f"SELECT stream_id, stream_state FROM system.cdc_streams WHERE keyspace_name = '{ks}' AND table_name = 'test'")
        changes = await cql.run_async(f"SELECT * FROM {ks}.test_scylla_cdc_log WHERE \"cdc$stream_id\" = 0x{cdc_stream[0].stream_id.hex()}", host=leader_host)

        c_value = 0
        is_preimage = False
        for row in changes:
            assert row.pk == 1
            if is_preimage:
                assert row.cdc_operation == 0
                assert row.c == c_value - 1
            else:
                assert row.cdc_operation == 2
                assert row.c == c_value
                c_value += 1
            is_preimage = not is_preimage
