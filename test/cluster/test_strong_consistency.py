#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from test.pylib.manager_client import ManagerClient
from test.pylib.util import gather_safely, wait_for
from test.cluster.util import new_test_keyspace
from test.pylib.internal_types import ServerInfo
from cassandra.protocol import InvalidRequest

import pytest
import logging
import time
import uuid

logger = logging.getLogger(__name__)


async def wait_for_leader(manager: ManagerClient, s: ServerInfo, group_id: str):
    async def get_leader_host_id():
        result = await manager.api.get_raft_leader(s.ip_addr, group_id)
        return None if uuid.UUID(result).int == 0 else result
    return await wait_for(get_leader_host_id, time.time() + 60)

async def get_table_raft_group_id(manager: ManagerClient, ks: str, table: str):
    table_id = await manager.get_table_id(ks, table)
    rows = await manager.get_cql().run_async(f"SELECT raft_group_id FROM system.tablets where table_id = {table_id}")
    return str(rows[0].raft_group_id)

@pytest.mark.asyncio
async def test_basic_write_read(manager: ManagerClient):

    logger.info("Bootstrapping cluster")
    config = {
        'experimental_features': ['strongly-consistent-tables']
    }
    cmdline = [
        '--logger-log-level', 'sc_groups_manager=debug',
        '--logger-log-level', 'sc_coordinator=debug'
    ]
    servers = await manager.servers_add(3, config=config, cmdline=cmdline, auto_rack_dc='my_dc')
    (cql, hosts) = await manager.get_ready_cql(servers)

    logger.info("Load host_id-s for servers")
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])

    def host_by_host_id(host_id):
        for hid, host in zip(host_ids, hosts):
            if hid == host_id:
                return host
        raise RuntimeError(f"Can't find host for host_id {host_id}")

    logger.info("Creating a strongly-consistent keyspace")
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1} AND consistency = 'local'") as ks:
        logger.info("Creating a table")
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        logger.info("Select raft group id for the tablet")
        group_id = await get_table_raft_group_id(manager, ks, 'test')

        logger.info(f"Get current leader for the group {group_id}")
        leader_host_id = await wait_for_leader(manager, servers[0], group_id)
        leader_host = host_by_host_id(leader_host_id)
        non_leader_host = next((host_by_host_id(hid) for hid in host_ids if hid != leader_host_id), None)
        assert non_leader_host is not None

        logger.info(f"Run INSERT statement on leader {leader_host}")
        await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES (10, 20)", host=leader_host)
        logger.info(f"Run INSERT statement on non-leader {non_leader_host}")
        with pytest.raises(InvalidRequest, match="Strongly consistent writes can be executed only on the leader node"):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES (10, 30)", host=non_leader_host)

        logger.info(f"Run SELECT statement on leader {leader_host}")
        rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = 10;", host=leader_host)
        assert len(rows) == 1
        row = rows[0]
        assert row.pk == 10
        assert row.c == 20

        logger.info(f"Run SELECT statement on non-leader {non_leader_host}")
        rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = 10;", host=non_leader_host)
        assert len(rows) == 1
        row = rows[0]
        assert row.pk == 10
        assert row.c == 20

        # Check that we can restart a server with an active tablets raft group
        await manager.server_restart(servers[2].server_id)

    # To check that the servers can be stopped gracefully. By default the test runner just kills them.
    await gather_safely(*[manager.server_stop_gracefully(s.server_id) for s in servers])

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_no_schema_when_apply_write(manager: ManagerClient):
    config = {
        'experimental_features': ['strongly-consistent-tables']
    }
    cmdline = [
        '--logger-log-level', 'sc_groups_manager=debug',
        '--logger-log-level', 'sc_coordinator=debug'
    ]
    servers = await manager.servers_add(2, config=config, cmdline=cmdline, auto_rack_dc='my_dc')
    # We don't want `servers[2]` to be a Raft leader (for both group0 and strong consistency groups),
    # because we want `servers[2]` to receive Raft commands from others.
    servers += [await manager.server_add(config=config | {'error_injections_at_startup': ['avoid_being_raft_leader']}, cmdline=cmdline, property_file={'dc':'my_dc', 'rack': 'rack3'})]
    (cql, hosts) = await manager.get_ready_cql(servers)
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])

    def host_by_host_id(host_id):
        for hid, host in zip(host_ids, hosts):
            if hid == host_id:
                return host
        raise RuntimeError(f"Can't find host for host_id {host_id}")

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1} AND consistency = 'local'") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        # Drop incomming append entries from group0 (schema changes) on `servers[2]` after the table is created,
        # so strong consistency raft groups are creates on the node but it won't receive next alter table mutations.
        group0_id = (await cql.run_async("SELECT value FROM system.scylla_local WHERE key = 'raft_group0_id'"))[0].value
        await manager.api.enable_injection(servers[2].ip_addr, "raft_drop_incoming_append_entries_for_specified_group", one_shot=False, parameters={'value': group0_id})
        await cql.run_async(f"ALTER TABLE {ks}.test ADD new_col int;", host=hosts[0])

        group_id = await get_table_raft_group_id(manager, ks, 'test')
        leader_host_id = await wait_for_leader(manager, servers[0], group_id)
        assert leader_host_id != host_ids[2]
        leader_host = host_by_host_id(leader_host_id)

        s2_log = await manager.server_open_log(servers[2].server_id)
        s2_mark = await s2_log.mark()

        await cql.run_async(f"INSERT INTO {ks}.test (pk, c, new_col) VALUES (10, 20, 30)", host=leader_host)
        await manager.api.disable_injection(servers[2].ip_addr, "raft_drop_incoming_append_entries_for_specified_group")

        await s2_log.wait_for(f"Column definitions for {ks}.test changed", timeout=60, from_mark=s2_mark)
        rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = 10;", host=hosts[2])
        assert len(rows) == 1
        row = rows[0]
        assert row.pk == 10
        assert row.c == 20
        assert row.new_col == 30

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_old_schema_when_apply_write(manager: ManagerClient):
    config = {
        'experimental_features': ['strongly-consistent-tables']
    }
    cmdline = [
        '--logger-log-level', 'sc_groups_manager=debug',
        '--logger-log-level', 'sc_coordinator=debug'
    ]
    servers = await manager.servers_add(2, config=config, cmdline=cmdline, auto_rack_dc='my_dc')
    # We don't want `servers[2]` to be a Raft leader (for both group0 and strong consistency groups),
    # because we want `servers[2]` to receive Raft commands from others.
    servers += [await manager.server_add(config=config | {'error_injections_at_startup': ['avoid_being_raft_leader']}, cmdline=cmdline, property_file={'dc':'my_dc', 'rack': 'rack3'})]
    (cql, hosts) = await manager.get_ready_cql(servers)
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])

    def host_by_host_id(host_id):
        for hid, host in zip(host_ids, hosts):
            if hid == host_id:
                return host
        raise RuntimeError(f"Can't find host for host_id {host_id}")

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1} AND consistency = 'local'") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        group_id = await get_table_raft_group_id(manager, ks, 'test')
        leader_host_id = await wait_for_leader(manager, servers[0], group_id)
        assert leader_host_id != host_ids[2]
        leader_host = host_by_host_id(leader_host_id)

        table_schema_version = (await cql.run_async(f"SELECT version FROM system_schema.scylla_tables WHERE keyspace_name = '{ks}' AND table_name = 'test'"))[0].version

        await manager.api.enable_injection(servers[2].ip_addr, "strong_consistency_state_machine_wait_before_apply", one_shot=False)
        await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES (10, 20)", host=leader_host)

        await cql.run_async(f"ALTER TABLE {ks}.test ADD new_col int;")
        # Following injection simulates that old schema version was already removed from the memory
        await manager.api.enable_injection(servers[2].ip_addr, "schema_registry_ignore_version", one_shot=False, parameters={'value': table_schema_version})
        await manager.api.message_injection(servers[2].ip_addr, "strong_consistency_state_machine_wait_before_apply")
        await manager.api.disable_injection(servers[2].ip_addr, "strong_consistency_state_machine_wait_before_apply")

        rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = 10;", host=hosts[2])
        assert len(rows) == 1
        row = rows[0]
        assert row.pk == 10
        assert row.c == 20
        assert row.new_col == None
