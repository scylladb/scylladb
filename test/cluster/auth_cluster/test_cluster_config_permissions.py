#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging
import time
from pathlib import Path

import pytest
from cassandra.auth import PlainTextAuthProvider
from cassandra.protocol import InvalidRequest, Unauthorized

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.scylla_server import ScyllaVersionDescription, get_current_version_description
from test.pylib.skip_types import skip_env
from test.pylib.util import wait_for_feature
from test.cluster.auth_cluster import extra_scylla_config_options as auth_config

logger = logging.getLogger(__name__)

KS = "CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
TABLE = "CREATE TABLE ks.t (pk int PRIMARY KEY)"


async def connect_as(manager: ScyllaClusterManager, user: str, password: str):
    await manager.driver_connect(auth_provider=PlainTextAuthProvider(username=user, password=password))
    return manager.get_cql()


async def check_data_scopes_open_and_cluster_scope_superuser_only(manager: ScyllaClusterManager) -> None:
    cql = manager.get_cql()
    await cql.run_async("CREATE ROLE app WITH PASSWORD = 'app' AND LOGIN = true")
    await cql.run_async(KS)
    await cql.run_async(TABLE)

    cql = await connect_as(manager, "app", "app")
    await cql.run_async("ALTER TABLE ks.t WITH auto_repair_enabled = true")
    await cql.run_async("ALTER KEYSPACE ks WITH auto_repair_enabled = true")
    with pytest.raises(Unauthorized):
        await cql.run_async("ALTER CLUSTER WITH auto_repair_enabled = true")

    cql = await connect_as(manager, "cassandra", "cassandra")
    await cql.run_async("ALTER CLUSTER WITH auto_repair_enabled = true")


async def test_allow_all_authorizer_with_authentication(manager: ScyllaClusterManager) -> None:
    """With authentication on and authorization off, every role can set options on keyspaces
    and tables, and ALTER CLUSTER needs a superuser."""
    servers = await manager.servers_add(1, config={
        "authenticator": "PasswordAuthenticator",
        "authorizer": "AllowAllAuthorizer",
    })
    await manager.get_ready_cql(servers)
    await check_data_scopes_open_and_cluster_scope_superuser_only(manager)


async def test_transitional_authorizer(manager: ScyllaClusterManager) -> None:
    """In transitional mode every role can set options on keyspaces and tables, as it can
    ALTER them, and ALTER CLUSTER needs a superuser."""
    servers = await manager.servers_add(1, config={
        "authenticator": "com.scylladb.auth.TransitionalAuthenticator",
        "authorizer": "com.scylladb.auth.TransitionalAuthorizer",
    })
    await manager.get_ready_cql(servers)
    await check_data_scopes_open_and_cluster_scope_superuser_only(manager)


async def test_revoke_grant_on_departed_topology_target(manager: ScyllaClusterManager) -> None:
    """A grant on a datacenter, rack or node that is no longer in the topology can be revoked.

    The grants are written straight into system.role_permissions, as they are left behind
    when their target leaves: GRANT refuses a target that is not in the topology."""
    servers = await manager.servers_add(1, config=auth_config)
    cql, _ = await manager.get_ready_cql(servers)
    node = "7f4d8b1e-1c2a-4f3b-9e6d-0a1b2c3d4e5f"

    await cql.run_async("CREATE ROLE ops WITH PASSWORD = 'ops' AND LOGIN = true")
    for resource, permissions in [
        (f"cluster/gone_dc/gone_rack/{node}", "{'CONFIGURE'}"),
        (f"cluster/gone_dc/gone_rack/{node}/@config:auto_repair_enabled", "{'CONFIGURE'}"),
        ("cluster/gone_dc/gone_rack", "{'CONFIGURE'}"),
        ("cluster/gone_dc", "{'CONFIGURE', 'AUTHORIZE'}"),
    ]:
        await cql.run_async(
            f"INSERT INTO system.role_permissions (role, resource, permissions) VALUES ('ops', '{resource}', {permissions})")

    async def listed():
        rows = await cql.run_async("LIST ALL PERMISSIONS OF ops")
        return sorted((row.resource, row.permission) for row in rows)

    assert await listed() == [
        ("<datacenter gone_dc>", "AUTHORIZE"),
        ("<datacenter gone_dc>", "CONFIGURE"),
        (f"<node {node}>", "CONFIGURE"),
        (f"<node {node}>", "CONFIGURE (auto_repair_enabled)"),
        ("<rack gone_dc gone_rack>", "CONFIGURE"),
    ]

    # The node's datacenter and rack come from the stored grant; the host ID case does not matter.
    await cql.run_async(f"REVOKE CONFIGURE ON NODE {node.upper()} FROM ops")
    await cql.run_async("REVOKE CONFIGURE ON RACK gone_dc gone_rack FROM ops")
    assert await listed() == [("<datacenter gone_dc>", "AUTHORIZE"), ("<datacenter gone_dc>", "CONFIGURE")]
    await cql.run_async("REVOKE ALL ON DATACENTER gone_dc FROM ops")
    assert await listed() == []


async def test_configure_during_rolling_upgrade(manager: ScyllaClusterManager, scylla_binary: Path) -> None:
    """Until every node knows CONFIGURE, it cannot be granted and the rules of the previous
    version apply on every node: ALTER authorizes an option of a table, and ALTER CLUSTER
    needs a superuser. Once the feature is enabled, CONFIGURE is required and can be granted.

    Needs a binary that has cluster config but not the permission, next to the built one."""
    scylla_binary = Path(scylla_binary)
    old_binary = scylla_binary.with_name("scylla.no.cluster.config.rbac")
    if not old_binary.exists():
        skip_env(f"{old_binary} not found")
    old_version = ScyllaVersionDescription(path=str(old_binary), config={}, argv=[])
    new_version = get_current_version_description(str(scylla_binary))

    servers = await manager.servers_add(2, config=auth_config, version=old_version)
    cql, _ = await manager.get_ready_cql(servers)
    await cql.run_async("CREATE ROLE app WITH PASSWORD = 'app' AND LOGIN = true")
    await cql.run_async(KS)
    await cql.run_async(TABLE)
    await cql.run_async("GRANT ALTER ON TABLE ks.t TO app")

    async def session(user: str, password: str):
        await manager.driver_connect(auth_provider=PlainTextAuthProvider(username=user, password=password))
        return await manager.get_ready_cql(servers)

    async def check_previous_rules():
        # Checked on every node: the upgraded one must behave like the old one.
        cql, hosts = await session("app", "app")
        for host in hosts:
            await cql.run_async("ALTER TABLE ks.t WITH auto_repair_enabled = true", host=host)
            with pytest.raises(Unauthorized):
                await cql.run_async("ALTER CLUSTER WITH auto_repair_enabled = true", host=host)
        cql, hosts = await session("cassandra", "cassandra")
        for host in hosts:
            await cql.run_async("ALTER CLUSTER WITH auto_repair_enabled = true", host=host)
        return cql, hosts

    await check_previous_rules()

    logger.info("Upgrading the first node")
    await manager.server_change_version(servers[0].server_id, str(scylla_binary), new_version)
    cql, hosts = await check_previous_rules()
    with pytest.raises(InvalidRequest, match="not yet supported"):
        await cql.run_async("GRANT CONFIGURE ON KEYSPACE ks TO app", host=hosts[0])
    with pytest.raises(InvalidRequest, match="not yet supported"):
        await cql.run_async("GRANT AUTHORIZE ON ALL SCOPES TO app", host=hosts[0])
    # The old node must keep reading the permissions table.
    await cql.run_async("LIST ALL PERMISSIONS", host=hosts[1])

    logger.info("Upgrading the second node")
    await manager.server_change_version(servers[1].server_id, str(scylla_binary), new_version)
    cql, hosts = await session("cassandra", "cassandra")
    for host in hosts:
        await wait_for_feature("CLUSTER_CONFIG_RBAC", cql, host, time.time() + 60)

    cql, hosts = await session("app", "app")
    for host in hosts:
        with pytest.raises(Unauthorized, match="CONFIGURE"):
            await cql.run_async("ALTER TABLE ks.t WITH auto_repair_enabled = false", host=host)

    cql, hosts = await session("cassandra", "cassandra")
    await cql.run_async("GRANT CONFIGURE (auto_repair_enabled) ON KEYSPACE ks TO app")
    await cql.run_async("ALTER CLUSTER WITH auto_repair_enabled = NULL")

    cql, hosts = await session("app", "app")
    for host in hosts:
        await cql.run_async("ALTER TABLE ks.t WITH auto_repair_enabled = false", host=host)
        with pytest.raises(Unauthorized, match="CONFIGURE"):
            await cql.run_async("ALTER CLUSTER WITH auto_repair_enabled = true", host=host)
