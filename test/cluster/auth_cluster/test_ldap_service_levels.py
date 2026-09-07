#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
import os
import time

from cassandra.auth import PlainTextAuthProvider

from test.cluster.auth_cluster import extra_scylla_config_options as auth_config
from test.pylib.driver_utils import safe_driver_shutdown
from test.pylib.manager_client import ManagerClient
from test.pylib.skip_types import skip_env
from test.pylib.util import unique_name, wait_for, wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

# The slapd fixture data (test/pylib/ldap_server.py) defines people jsmith
# (member of group role1) and jdoe (member of group role3).
LDAP_GROUPS = ["role1", "role2", "role3"]

BATCH_QUERIES = 10


def ldap_config():
    host = os.environ.get("SEASTAR_LDAP_HOST")
    port = os.environ.get("SEASTAR_LDAP_PORT")
    if not host or not port:
        skip_env("needs the LDAP server started by the test.py harness")
    return {
        **auth_config,
        "role_manager": "com.scylladb.auth.LDAPRoleManager",
        "ldap_url_template":
            f"ldap://{host}:{port}/dc=example,dc=com?cn?sub"
            "?(uniqueMember=uid={USER},ou=People,dc=example,dc=com)",
        "ldap_attr_role": "cn",
        "ldap_bind_dn": "cn=root,dc=example,dc=com",
        "ldap_bind_passwd": "secret",
    }


async def start_server(manager: ManagerClient):
    server = await manager.server_add(config=ldap_config())
    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)

    for group in LDAP_GROUPS:
        await cql.run_async(f"CREATE ROLE {group}")

    # A connection moves from the sl:driver group to its user's service
    # level group when it runs a query on a user table, such as demo.t.
    await cql.run_async("CREATE KEYSPACE demo WITH replication = "
                        "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    await cql.run_async("CREATE TABLE demo.t (id int PRIMARY KEY)")

    return server, cql


def run_query_batch(session):
    for _ in range(BATCH_QUERIES):
        session.execute("SELECT * FROM demo.t")


async def requests_served_in_group(manager: ManagerClient, server, group):
    metrics = await manager.metrics.query(server.ip_addr)
    count = metrics.get("scylla_transport_cql_requests_count",
                        {"scheduling_group_name": group})
    return count or 0


async def wait_until_served_in_group(manager, server, user_session, group, deadline):
    # Nothing else maps to `group`, so the counter growing by a full batch
    # proves the user's requests execute under the attached service level.
    async def full_batch_lands_in_group():
        before = await requests_served_in_group(manager, server, group)
        await asyncio.to_thread(run_query_batch, user_session)
        after = await requests_served_in_group(manager, server, group)
        if after - before >= BATCH_QUERIES:
            return True
        logger.info("%s served %d of %d requests", group, after - before, BATCH_QUERIES)
        return None
    await wait_for(full_batch_lands_in_group, deadline, label=f"requests in {group}")


async def wait_until_not_served_in_group(manager, server, user_session, group, deadline):
    async def batch_avoids_group():
        before = await requests_served_in_group(manager, server, group)
        await asyncio.to_thread(run_query_batch, user_session)
        after = await requests_served_in_group(manager, server, group)
        if after <= before:
            return True
        logger.info("%s still served %d requests", group, after - before)
        return None
    await wait_for(batch_avoids_group, deadline, label=f"no requests in {group}")


def connect_as(manager: ManagerClient, server, username, password):
    cluster = manager.con_gen([server.ip_addr], manager.port, manager.use_ssl,
                              PlainTextAuthProvider(username=username, password=password))
    return cluster, cluster.connect()


async def test_ldap_attach_service_level(manager: ManagerClient):
    """ATTACH, LIST and DROP SERVICE LEVEL for a user whose roles come from
    LDAP."""
    server, cql = await start_server(manager)

    sl = "sl_" + unique_name()
    await cql.run_async(f"CREATE SERVICE LEVEL {sl}")
    await cql.run_async("CREATE ROLE jsmith WITH password='jsmithpass' AND login=true")
    await cql.run_async("GRANT SELECT ON demo.t TO jsmith")

    await cql.run_async(f"ATTACH SERVICE LEVEL {sl} TO jsmith")

    attached = await cql.run_async("LIST ATTACHED SERVICE LEVEL OF jsmith")
    assert [(row.role, row.service_level) for row in attached] == [("jsmith", sl)]
    attached = await cql.run_async("LIST ALL ATTACHED SERVICE LEVELS")
    assert ("jsmith", sl) in [(row.role, row.service_level) for row in attached]

    listed = await cql.run_async(f"LIST SERVICE LEVEL {sl}")
    assert [row.service_level for row in listed] == [sl]
    assert sl in [row.service_level for row in await cql.run_async("LIST ALL SERVICE LEVELS")]

    cluster, session = connect_as(manager, server, "jsmith", "jsmithpass")
    try:
        await wait_until_served_in_group(manager, server, session, f"sl:{sl}",
                                         time.time() + 60)

        await cql.run_async("DETACH SERVICE LEVEL FROM jsmith")
        assert len(await cql.run_async("LIST ATTACHED SERVICE LEVEL OF jsmith")) == 0
        await wait_until_not_served_in_group(manager, server, session, f"sl:{sl}",
                                             time.time() + 60)

        await cql.run_async(f"ATTACH SERVICE LEVEL {sl} TO jsmith")
        await wait_until_served_in_group(manager, server, session, f"sl:{sl}",
                                         time.time() + 60)

        # DROP also detaches the level from every role.
        await cql.run_async(f"DROP SERVICE LEVEL {sl}")
        assert sl not in [row.service_level for row in await cql.run_async("LIST ALL SERVICE LEVELS")]
        assert len(await cql.run_async("LIST ATTACHED SERVICE LEVEL OF jsmith")) == 0
        await wait_until_not_served_in_group(manager, server, session, f"sl:{sl}",
                                             time.time() + 60)
    finally:
        safe_driver_shutdown(cluster)


async def test_ldap_granted_role_service_level(manager: ManagerClient):
    """A service level attached to a group role reaches the group's members.
    jdoe is a member of role3 in the LDAP directory only. No GRANT statement
    runs, the grant comes from LDAP."""
    server, cql = await start_server(manager)

    sl = "sl_" + unique_name()
    await cql.run_async(f"CREATE SERVICE LEVEL {sl}")
    await cql.run_async("CREATE ROLE jdoe WITH password='jdoepass' AND login=true")
    await cql.run_async("GRANT SELECT ON demo.t TO jdoe")

    await cql.run_async(f"ATTACH SERVICE LEVEL {sl} TO role3")

    effective = await cql.run_async("LIST EFFECTIVE SERVICE LEVEL OF jdoe")
    assert effective and all(row.effective_service_level == sl for row in effective)

    cluster, session = connect_as(manager, server, "jdoe", "jdoepass")
    try:
        await wait_until_served_in_group(manager, server, session, f"sl:{sl}",
                                         time.time() + 60)

        await cql.run_async(f"DROP SERVICE LEVEL {sl}")
        await wait_until_not_served_in_group(manager, server, session, f"sl:{sl}",
                                             time.time() + 60)
    finally:
        safe_driver_shutdown(cluster)
