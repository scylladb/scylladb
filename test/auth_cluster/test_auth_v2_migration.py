#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import asyncio
import logging
import pytest
import time

from test.pylib.manager_client import ManagerClient
from test.pylib.util import read_barrier, wait_for_cql_and_get_hosts
from test.topology.util import wait_until_topology_upgrade_finishes
from cassandra.cluster import ConsistencyLevel


def auth_data():
    return [
        {
            "statement": "INSERT INTO system_auth.roles (role, can_login, is_superuser, member_of, salted_hash) VALUES (?, ?, ?, ?, ?)",
            "rows": [
                ("cassandra", False, True, None, None),
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
                ("users", "service_level", "sl:fefe"),
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
    await populate_test_data(manager, [
        {
            "statement": "INSERT INTO system_auth.roles (role, can_login, is_superuser, member_of, salted_hash) VALUES (?, ?, ?, ?, ?)",
            "rows": [
                ("deleted_user", True, False, None, "fefe"),
            ]
        },
        {
            "statement": "DELETE FROM system_auth.roles WHERE role = ?",
            "rows": [
                ("deleted_user",),
            ]
        },
    ])


async def check_auth_v2_data_migration(manager: ManagerClient, hosts):
    cql = manager.get_cql()
    # auth reads are eventually consistent so we need to make sure hosts are up-to-date
    assert hosts
    await asyncio.gather(*(read_barrier(cql, host) for host in hosts))

    data = auth_data()

    roles = set()
    for row in await cql.run_async("SELECT * FROM system_auth_v2.roles"):
        member_of = frozenset(row.member_of) if row.member_of else None
        roles.add((row.role, row.can_login, row.is_superuser, member_of, row.salted_hash))
    assert roles == set(data[0]["rows"])

    role_members = set()
    for row in await cql.run_async("SELECT * FROM system_auth_v2.role_members"):
        role_members.add((row.role, row.member))
    assert role_members == set(data[1]["rows"])

    role_attributes = set()
    for row in await cql.run_async("SELECT * FROM system_auth_v2.role_attributes"):
        role_attributes.add((row.role, row.name, row.value))
    assert role_attributes == set(data[2]["rows"])


async def check_auth_v2_works(manager: ManagerClient, hosts):
    cql = manager.get_cql()
    roles = set()
    for row in await cql.run_async("LIST ROLES"):
        roles.add(row.role)
    assert roles == set(["cassandra", "user 1", "user 2", "users"])

    user1_roles = await cql.run_async("LIST ROLES OF 'user 1'")
    assert len(user1_roles) == 2
    assert set([user1_roles[0].role, user1_roles[1].role]) == set(["users",  "user 1"])

    await cql.run_async("CREATE ROLE IF NOT EXISTS user_after_migration")
    await cql.run_async("DROP ROLE user_after_migration")


@pytest.mark.asyncio
async def test_auth_v2_migration(request, manager: ManagerClient):
    # First, force the first node to start in legacy mode due to the error injection
    cfg = {'error_injections_at_startup': ['force_gossip_based_join']}

    servers = [await manager.server_add(config=cfg)]
    # Disable injections for the subsequent nodes - they should fall back to
    # using gossiper-based node operations
    del cfg['error_injections_at_startup']

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

    logging.info("Triggering upgrade to raft topology")
    await manager.api.upgrade_to_raft_topology(hosts[0].address)

    logging.info("Waiting until upgrade finishes")
    await asyncio.gather(*(wait_until_topology_upgrade_finishes(manager, h.address, time.time() + 60) for h in hosts))

    logging.info("Checking migrated data in system_auth_v2")
    await check_auth_v2_data_migration(manager, hosts)

    logging.info("Checking auth statements after migration")
    await check_auth_v2_works(manager, hosts)
