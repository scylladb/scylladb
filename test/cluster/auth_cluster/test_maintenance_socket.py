#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra import Unauthorized, InvalidRequest
from cassandra.connection import UnixSocketEndPoint
from cassandra.policies import WhiteListRoundRobinPolicy
from test.cluster.conftest import cluster_con
from test.pylib.driver_utils import safe_driver_shutdown
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for

import logging
import pytest
import time
from collections.abc import Generator
from test.cluster.auth_cluster import extra_scylla_config_options as auth_config

logger = logging.getLogger(__name__)

CqlClusters = list[Cluster]

@pytest.fixture
def cql_clusters() -> Generator[CqlClusters, None, None]:
    """Tracks CQL driver Cluster objects for automatic shutdown after test completion."""
    clusters: CqlClusters = []
    yield clusters
    for c in reversed(clusters):
        safe_driver_shutdown(c)


async def get_ready_maintenance_session(socket_path: str, timeout: int = 60):
    """Connect to maintenance socket, retrying until the role manager is ready.

    Uses a two-phase approach:
    1. Establish a session using a simple SELECT (query processor, always ready).
    2. Probe role manager readiness on the same session via DROP ROLE IF EXISTS.

    Creating a new Cluster per retry in phase 2 doesn't work because the Python
    driver's control connection topology refresh discovers the node's real IP,
    removes the unix socket host, and the query fails client-side before reaching
    the server.
    """
    deadline = time.time() + timeout

    # Phase 1: establish a live session to the maintenance socket.
    async def try_connect():
        c = cluster_con([UnixSocketEndPoint(socket_path)],
                        load_balancing_policy=WhiteListRoundRobinPolicy([UnixSocketEndPoint(socket_path)]))
        try:
            session = c.connect()
            # A lightweight query that goes through the query processor only,
            # not the role manager, so it succeeds as soon as CQL is up.
            session.execute("SELECT key FROM system.local LIMIT 1")
            return session
        except Exception:
            c.shutdown()
            return None

    session = await wait_for(try_connect, deadline)

    # Phase 2: wait for the role manager to become ready on the same session.
    # DROP ROLE IF EXISTS goes through the role manager (via exists()), unlike
    # plain SELECT queries which only use the query processor.
    async def check_role_manager():
        try:
            session.execute("DROP ROLE IF EXISTS readiness_probe")
            return True
        except Exception:
            return None

    await wait_for(check_role_manager, deadline)
    return session


async def connect_with_credentials(ip: str, username: str, password: str, timeout: int = 60):
    """Connect with auth credentials, retrying until accepted.

    Uses WhiteListRoundRobinPolicy to prevent the driver from discovering
    other cluster nodes via topology refresh and routing queries there.
    """
    async def try_connect():
        c = cluster_con([ip],
                        auth_provider=PlainTextAuthProvider(username=username, password=password),
                        load_balancing_policy=WhiteListRoundRobinPolicy([ip]))
        try:
            return c.connect()
        except NoHostAvailable:
            c.shutdown()
            return None
    return await wait_for(try_connect, time.time() + timeout)


@pytest.mark.asyncio
async def test_maintenance_socket(manager: ManagerClient, cql_clusters: CqlClusters):
    """
    Test that when connecting to the maintenance socket, the user has superuser permissions,
    even if the authentication is enabled on the regular port.
    """
    logger.info("Starting server with auth enabled")
    server = await manager.server_add(config=auth_config)
    socket = await manager.server_get_maintenance_socket_path(server.server_id)

    logger.info("Verifying unauthenticated connection is rejected")
    cluster = Cluster([server.ip_addr])
    cql_clusters.append(cluster)
    try:
        cluster.connect()
        pytest.fail("Client should not be able to connect if auth provider is not specified")
    except NoHostAvailable:
        pass

    logger.info("Connecting as superuser to set up roles and keyspaces")
    superuser_cluster = cluster_con([server.ip_addr],
                                    auth_provider=PlainTextAuthProvider(username="cassandra", password="cassandra"))
    cql_clusters.append(superuser_cluster)
    session = superuser_cluster.connect()

    session.execute("CREATE ROLE john WITH PASSWORD = 'password' AND LOGIN = true;")
    session.execute("CREATE KEYSPACE ks1 WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};")
    session.execute("CREATE KEYSPACE ks2 WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};")
    session.execute("CREATE TABLE ks1.t1 (pk int PRIMARY KEY, val int);")
    session.execute("CREATE TABLE ks2.t1 (pk int PRIMARY KEY, val int);")
    session.execute("GRANT SELECT ON ks1.t1 TO john;")

    logger.info("Verifying user 'john' cannot access ks2.t1")
    john_cluster = cluster_con([server.ip_addr], auth_provider=PlainTextAuthProvider(username="john", password="password"))
    cql_clusters.append(john_cluster)
    john_session = john_cluster.connect()
    try:
        john_session.execute("SELECT * FROM ks2.t1")
    except Unauthorized:
        pass
    else:
        pytest.fail("User 'john' has no permissions to access ks2.t1")

    logger.info("Connecting via maintenance socket")
    maintenance_cluster = cluster_con([UnixSocketEndPoint(socket)], load_balancing_policy=WhiteListRoundRobinPolicy([UnixSocketEndPoint(socket)]))
    cql_clusters.append(maintenance_cluster)
    maintenance_session = maintenance_cluster.connect()

    logger.info("Verifying maintenance session has superuser permissions")
    maintenance_session.execute("SELECT * FROM ks1.t1")
    maintenance_session.execute("SELECT * FROM ks2.t1")
    maintenance_session.execute("INSERT INTO ks1.t1 (pk, val) VALUES (1, 1);")
    maintenance_session.execute("CREATE KEYSPACE ks3 WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};")
    maintenance_session.execute("CREATE TABLE ks1.t2 (pk int PRIMARY KEY, val int);")


@pytest.mark.asyncio
async def test_no_default_superuser_exists_by_default(manager: ManagerClient, cql_clusters: CqlClusters):
    """
    Test that no 'cassandra' user exists when no default superuser is configured.
    """
    config = {
        **auth_config,
        "auth_superuser_name": "",
        "auth_superuser_salted_password": "",
    }

    logger.info("Starting new server without default superuser")
    server = await manager.server_add(config=config, connect_driver=False)

    logger.info("Verifying default credentials are rejected")
    cluster = Cluster([server.ip_addr], auth_provider=PlainTextAuthProvider(username="cassandra", password="cassandra"))
    cql_clusters.append(cluster)
    try:
        cluster.connect()
        pytest.fail("Should not be able to connect with default credentials when they are not seeded")
    except Exception:
        pass


@pytest.mark.asyncio
async def test_no_default_superuser_maintenance_socket_ops(manager: ManagerClient, cql_clusters: CqlClusters):
    """
    Test that we can manage user roles via the maintenance socket.
    """
    config = {
        **auth_config,
        "auth_superuser_name": "",
        "auth_superuser_salted_password": "",
    }

    logger.info("Starting new server without default superuser")
    server = await manager.server_add(config=config, connect_driver=False)

    logger.info("Connecting via maintenance socket")
    socket_path = await manager.server_get_maintenance_socket_path(server.server_id)
    session = await get_ready_maintenance_session(socket_path)
    cql_clusters.append(session.cluster)

    logger.info("Verifying system.roles is empty before operations")
    rows = list(session.execute("SELECT role, is_superuser FROM system.roles"))
    assert len(rows) == 0, f"Expected no roles, found: {rows}"

    logger.info("Creating superuser role via maintenance socket")
    new_role = "admin_user"
    new_role_password = "password"
    session.execute(f"CREATE ROLE {new_role} WITH PASSWORD = '{new_role_password}' AND SUPERUSER = true AND LOGIN = true")

    rows = list(session.execute(f"SELECT role, is_superuser, can_login, salted_hash FROM system.roles WHERE role = '{new_role}'"))
    assert len(rows) == 1
    assert rows[0].role == new_role
    assert rows[0].is_superuser == True
    assert rows[0].can_login == True
    assert rows[0].salted_hash
    # Verify SHA-512 salted hash format: $6$<salt>$<hash>
    assert rows[0].salted_hash.startswith("$6$")
    assert len(rows[0].salted_hash.split('$')) == 4

    logger.info("Verifying the new role can log in via the normal CQL port")
    admin_session = await connect_with_credentials(server.ip_addr, new_role, new_role_password)
    cql_clusters.append(admin_session.cluster)

    logger.info("Verifying superuser can create a keyspace")
    admin_session.execute("CREATE KEYSPACE ks1 WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")

    logger.info("Altering role to remove superuser via maintenance socket")
    session.execute(f"ALTER ROLE {new_role} WITH SUPERUSER = false")

    rows = list(session.execute(f"SELECT role, is_superuser, can_login FROM system.roles WHERE role = '{new_role}'"))
    assert len(rows) == 1
    assert rows[0].is_superuser == False
    assert rows[0].can_login == True

    logger.info("Verifying superuser privileges were revoked")
    # The server caches superuser status, so we need to retry until the cache refreshes.
    async def check_superuser_revoked():
        c = cluster_con([server.ip_addr],
                        auth_provider=PlainTextAuthProvider(username=new_role, password=new_role_password))
        try:
            s = c.connect()
            s.execute("CREATE KEYSPACE ks2 WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
            return None  # Still cached as superuser, retry
        except Unauthorized:
            return True
        finally:
            c.shutdown()

    await wait_for(check_superuser_revoked, time.time() + 60)

    logger.info("Dropping role via maintenance socket")
    session.execute(f"DROP ROLE {new_role}")

    rows = list(session.execute(f"SELECT role FROM system.roles WHERE role = '{new_role}'"))
    assert len(rows) == 0

    logger.info("Verifying dropped role can no longer log in")
    # The server caches credentials, so we need to retry until the cache refreshes.
    async def check_role_dropped():
        c = cluster_con([server.ip_addr],
                        auth_provider=PlainTextAuthProvider(username=new_role, password=new_role_password))
        try:
            c.connect()
            c.shutdown()
            return None  # Still cached, retry
        except NoHostAvailable:
            c.shutdown()
            return True

    await wait_for(check_role_dropped, time.time() + 60)


@pytest.mark.asyncio
async def test_maintenance_socket_grant_revoke(manager: ManagerClient, cql_clusters: CqlClusters):
    """
    Test that GRANT, REVOKE, and REVOKE ALL via the maintenance socket work correctly.

    The maintenance socket uses maintenance_socket_authorizer, which extends
    CassandraAuthorizer so that authorization-altering statements (GRANT, REVOKE)
    are persisted, while the maintenance socket user itself always has full access.
    """
    config = {
        **auth_config,
        "auth_superuser_name": "",
        "auth_superuser_salted_password": "",
    }

    logger.info("Starting server without default superuser")
    server = await manager.server_add(config=config, connect_driver=False)

    logger.info("Connecting via maintenance socket")
    socket_path = await manager.server_get_maintenance_socket_path(server.server_id)
    session = await get_ready_maintenance_session(socket_path)
    cql_clusters.append(session.cluster)

    session.execute("CREATE KEYSPACE ks WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    session.execute("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")
    session.execute("CREATE ROLE role1 WITH PASSWORD = 'pass' AND LOGIN = true")

    # GRANT SELECT via maintenance socket, verify it is persisted
    logger.info("Testing GRANT via maintenance socket")
    session.execute("GRANT SELECT ON ks.t TO role1")

    rows = list(session.execute("LIST ALL PERMISSIONS OF role1"))
    assert len(rows) == 1
    assert rows[0].permission == "SELECT"

    role1_session = await connect_with_credentials(server.ip_addr, "role1", "pass")
    cql_clusters.append(role1_session.cluster)
    role1_session.execute("SELECT * FROM ks.t")

    # REVOKE SELECT via maintenance socket
    logger.info("Testing REVOKE via maintenance socket")
    session.execute("REVOKE SELECT ON ks.t FROM role1")

    rows = list(session.execute("LIST ALL PERMISSIONS OF role1"))
    assert len(rows) == 0

    # GRANT multiple permissions, then REVOKE ALL
    logger.info("Testing REVOKE ALL via maintenance socket")
    session.execute("GRANT SELECT ON ks.t TO role1")
    session.execute("GRANT MODIFY ON ks.t TO role1")

    rows = list(session.execute("LIST ALL PERMISSIONS OF role1"))
    assert len(rows) == 2

    session.execute("REVOKE ALL ON ks.t FROM role1")

    rows = list(session.execute("LIST ALL PERMISSIONS OF role1"))
    assert len(rows) == 0
