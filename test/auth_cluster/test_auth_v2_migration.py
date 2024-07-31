#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import asyncio
import logging
import pytest
import time

from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import get_host_api_address, read_barrier
from test.pylib.util import wait_for_cql_and_get_hosts, unique_name
from cassandra.cluster import ConsistencyLevel
from test.topology.util import wait_until_topology_upgrade_finishes, enter_recovery_state, reconnect_driver, \
        delete_raft_topology_state, delete_raft_data_and_upgrade_state, wait_until_upgrade_finishes

def auth_data():
    return [
        {
            "statement": "INSERT INTO system_distributed.service_levels (service_level, timeout, workload_type) VALUES (?, ?, ?)",
            "rows": [
                ("sl1", None, None),
            ]
        },
        {
            "statement": "INSERT INTO system_auth.roles (role, can_login, is_superuser, member_of, salted_hash) VALUES (?, ?, ?, ?, ?)",
            "rows": [
                ("user 1", True, False, frozenset({'users'}), "salt1?"),
                ("user 2", True, False, frozenset({'users'}), "salt2#"),
                ("users", False, False, None, None),
            ]
        },
        {
            "statement": "INSERT INTO system_auth.role_members (role, member) VALUES (?, ?)",
            "rows": [
                ("users", "user 1"),
                ("users", "user 2"),
            ]
        },
        {
            "statement": "INSERT INTO system_auth.role_attributes (role, name, value) VALUES (?, ?, ?)",
            "rows": [
                ("users", "service_level", "sl1"),
            ]
        },
    ]


async def populate_test_data(manager: ManagerClient, data):
    cql = manager.get_cql()
    for d in data:
        stmt = cql.prepare(d["statement"])
        stmt.consistency_level = ConsistencyLevel.ALL
        await asyncio.gather(*(
            cql.run_async(stmt.bind(row_data)) for row_data in d["rows"]))


async def populate_auth_v1_data(manager: ManagerClient):
    await populate_test_data(manager, auth_data())
    # test also absence of deleted data
    username = unique_name("deleted_user_")
    logging.info(f"Creating deleted auth-v1 user: {username}")
    await populate_test_data(manager, [
        {
            "statement": "INSERT INTO system_auth.roles (role, can_login, is_superuser, member_of, salted_hash) VALUES (?, ?, ?, ?, ?)",
            "rows": [
                (username, True, False, None, "fefe"),
            ]
        },
        {
            "statement": "DELETE FROM system_auth.roles WHERE role = ?",
            "rows": [
                (username,),
            ]
        },
    ])


async def warmup_v1_static_values(manager: ManagerClient, hosts):
    # auth-v1 was using statics to cache internal queries
    # in auth-v2 those statics were removed but we want to
    # verify that it was effective so trigger here internal
    # call to potentially populate static storage and we'll
    # verify later that after migration properly formed query
    # executes (query has to change because keyspace name changes)
    cql = manager.get_cql()
    await asyncio.gather(*(cql.run_async("LIST ROLES", host=host) for host in hosts))


async def check_auth_v2_data_migration(manager: ManagerClient, hosts):
    cql = manager.get_cql()
    # auth reads are eventually consistent so we need to make sure hosts are up-to-date
    assert hosts
    await asyncio.gather(*(read_barrier(manager.api, get_host_api_address(host)) for host in hosts))

    data = auth_data()

    roles = set()
    for row in await cql.run_async("SELECT * FROM system.roles"):
        if row.role == "cassandra":
            # Skip default role, its creation in auth-v1
            # is asynchronous and all nodes race to create it
            # so we'd need to delay the test and wait.
            # Checking this particular role doesn't bring much value
            # to the test as we check other roles to demonstrate correctness
            continue
        member_of = frozenset(row.member_of) if row.member_of else None
        roles.add((row.role, row.can_login, row.is_superuser, member_of, row.salted_hash))
    assert roles == set(data[1]["rows"])

    role_members = set()
    for row in await cql.run_async("SELECT * FROM system.role_members"):
        role_members.add((row.role, row.member))
    assert role_members == set(data[2]["rows"])

    role_attributes = set()
    for row in await cql.run_async("SELECT * FROM system.role_attributes"):
        role_attributes.add((row.role, row.name, row.value))
    assert role_attributes == set(data[3]["rows"])


async def check_auth_v2_works(manager: ManagerClient, hosts):
    cql = manager.get_cql()
    roles = set()
    for row in await cql.run_async("LIST ROLES"):
        roles.add(row.role)
    assert roles == set(["cassandra", "user 1", "user 2", "users"])

    user1_roles = await cql.run_async("LIST ROLES OF 'user 1'")
    assert len(user1_roles) == 2
    assert set([user1_roles[0].role, user1_roles[1].role]) == set(["users",  "user 1"])

    username = unique_name("user_after_migration_")
    logging.info(f"Create role after migration: {username}")
    await cql.run_async(f"CREATE ROLE {username}")
    await asyncio.gather(*(read_barrier(manager.api, get_host_api_address(host)) for host in hosts))
    # see warmup_v1_static_values for background about checks below
    # check if it was added to a new table
    assert len(await cql.run_async(f"SELECT role FROM system.roles WHERE role = '{username}'")) == 1
    # check whether list roles statement sees it also via new table (on all nodes)
    await asyncio.gather(*(cql.run_async(f"LIST ROLES OF {username}", host=host) for host in hosts))
    await cql.run_async(f"DROP ROLE {username}")


@pytest.mark.asyncio
async def test_auth_v2_migration(request, manager: ManagerClient):
    # First, force the first node to start in legacy mode
    cfg = {'force_gossip_topology_changes': True}

    servers = [await manager.server_add(config=cfg)]
    # Enable raft-based node operations for subsequent nodes - they should fall back to
    # using gossiper-based node operations
    del cfg['force_gossip_topology_changes']

    servers += [await manager.server_add(config=cfg) for _ in range(2)]
    cql = manager.cql
    assert(cql)

    logging.info("Waiting until driver connects to every server")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info("Checking the upgrade state on all nodes")
    for host in hosts:
        status = await manager.api.raft_topology_upgrade_status(host.address)
        assert status == "not_upgraded"

    await populate_auth_v1_data(manager)
    await warmup_v1_static_values(manager, hosts)

    logging.info("Triggering upgrade to raft topology")
    await manager.api.upgrade_to_raft_topology(hosts[0].address)

    logging.info("Waiting until upgrade finishes")
    await asyncio.gather(*(wait_until_topology_upgrade_finishes(manager, h.address, time.time() + 60) for h in hosts))

    logging.info("Checking migrated data in system")
    await check_auth_v2_data_migration(manager, hosts)

    logging.info("Checking auth statements after migration")
    await check_auth_v2_works(manager, hosts)


@pytest.mark.asyncio
async def test_auth_v2_during_recovery(manager: ManagerClient):
    servers = await manager.servers_add(3)
    cql, hosts = await manager.get_ready_cql(servers)

    logging.info("Checking auth version before recovery")
    auth_version = await cql.run_async(f"SELECT value FROM system.scylla_local WHERE key = 'auth_version'")
    assert auth_version[0].value == "2"

    logging.info("Creating role before recovery")
    role_name = "ro" + unique_name()
    await cql.run_async(f"CREATE ROLE {role_name}")
    # auth reads are eventually consistent so we need to sync all nodes
    await asyncio.gather(*(read_barrier(manager.api, get_host_api_address(host)) for host in hosts))

    logging.info("Read roles before recovery")
    roles = [row.role for row in await cql.run_async(f"LIST ROLES")]
    assert set(roles) == set([role_name, "cassandra"])

    logging.info("Poison with auth_v1 look a like data")
    # this will verify that old roles are not brought back during recovery
    # as it runs very similar code path as during v1->v2 migration
    await cql.run_async(f"CREATE TABLE system_auth.roles (role text PRIMARY KEY, can_login boolean, is_superuser boolean, member_of set<text>, salted_hash text)")
    v1_ro_name = "v1_ro" + unique_name()
    await cql.run_async(f"INSERT INTO system_auth.roles (role) VALUES ('{v1_ro_name}')")

    logging.info(f"Restarting hosts {hosts} in recovery mode")
    await asyncio.gather(*(enter_recovery_state(cql, h) for h in hosts))
    await manager.rolling_restart(servers)

    logging.info("Cluster restarted, waiting until driver reconnects to every server")
    await reconnect_driver(manager)
    cql, hosts = await manager.get_ready_cql(servers)

    logging.info("Checking auth version during recovery")
    auth_version = await cql.run_async(f"SELECT value FROM system.scylla_local WHERE key = 'auth_version'")
    assert auth_version[0].value == "2"

    logging.info("Reading roles during recovery")
    roles = [row.role for row in await cql.run_async(f"LIST ROLES")]
    assert set(roles) == set([role_name, "cassandra"])

    logging.info("Restoring cluster to normal status")
    await asyncio.gather(*(delete_raft_topology_state(cql, h) for h in hosts))
    await asyncio.gather(*(delete_raft_data_and_upgrade_state(cql, h) for h in hosts))
    await manager.rolling_restart(servers)

    await reconnect_driver(manager)
    cql, hosts = await manager.get_ready_cql(servers)

    await asyncio.gather(*(wait_until_upgrade_finishes(cql, h, time.time() + 60) for h in hosts))
    for host in hosts:
        status = await manager.api.raft_topology_upgrade_status(host.address)
        assert status == "not_upgraded"

    await manager.api.upgrade_to_raft_topology(hosts[0].address)
    await asyncio.gather(*(wait_until_topology_upgrade_finishes(manager, h.address, time.time() + 60) for h in hosts))

    logging.info("Checking auth version after recovery")
    auth_version = await cql.run_async(f"SELECT value FROM system.scylla_local WHERE key = 'auth_version'")
    assert auth_version[0].value == "2"

    logging.info("Reading roles after recovery")
    roles = [row.role for row in await manager.get_cql().run_async(f"LIST ROLES")]
    assert set(roles) == set([role_name, "cassandra"])
