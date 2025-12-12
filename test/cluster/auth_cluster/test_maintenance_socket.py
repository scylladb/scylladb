#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra import Unauthorized, InvalidRequest
from cassandra.connection import UnixSocketEndPoint
from cassandra.policies import WhiteListRoundRobinPolicy
from test.cluster.conftest import cluster_con
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for

import pytest
import time
from test.cluster.auth_cluster import extra_scylla_config_options as auth_config


@pytest.mark.asyncio
async def test_maintenance_socket(manager: ManagerClient):
    """
    Test that when connecting to the maintenance socket, the user has superuser permissions,
    even if the authentication is enabled on the regular port.
    """
    config = {
        **auth_config,
        "authenticator": "PasswordAuthenticator",
        "authorizer": "CassandraAuthorizer",
    }

    server = await manager.server_add(config=config)
    socket = await manager.server_get_maintenance_socket_path(server.server_id)

    try:
        cluster = Cluster([server.ip_addr])
        cluster.connect()
    except NoHostAvailable:
        pass
    else:
        pytest.fail("Client should not be able to connect if auth provider is not specified")

    cluster = cluster_con([server.ip_addr],
                          auth_provider=PlainTextAuthProvider(username="cassandra", password="cassandra"))
    session = cluster.connect()

    session.execute("CREATE ROLE john WITH PASSWORD = 'password' AND LOGIN = true;")
    session.execute("CREATE KEYSPACE ks1 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};")
    session.execute("CREATE KEYSPACE ks2 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};")
    session.execute("CREATE TABLE ks1.t1 (pk int PRIMARY KEY, val int);")
    session.execute("CREATE TABLE ks2.t1 (pk int PRIMARY KEY, val int);")
    session.execute("GRANT SELECT ON ks1.t1 TO john;")

    cluster = cluster_con([server.ip_addr], auth_provider=PlainTextAuthProvider(username="john", password="password"))
    session = cluster.connect()
    try:
        session.execute("SELECT * FROM ks2.t1")
    except Unauthorized:
        pass
    else:
        pytest.fail("User 'john' has no permissions to access ks2.t1")

    maintenance_cluster = cluster_con([UnixSocketEndPoint(socket)])
    maintenance_session = maintenance_cluster.connect()

    # check that the maintenance session has superuser permissions
    maintenance_session.execute("SELECT * FROM ks1.t1")
    maintenance_session.execute("SELECT * FROM ks2.t1")
    maintenance_session.execute("INSERT INTO ks1.t1 (pk, val) VALUES (1, 1);")
    maintenance_session.execute("CREATE KEYSPACE ks3 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};")
    maintenance_session.execute("CREATE TABLE ks1.t2 (pk int PRIMARY KEY, val int);")

    maintenance_cluster.shutdown()
    cluster.shutdown()


@pytest.mark.asyncio
async def test_no_default_superuser_socket_ops(manager: ManagerClient):
    """
    Test that when no default superuser is configured:
    1. No 'cassandra' user exists.
    2. We can manage roles via the maintenance socket.
    """
    config = {
        **auth_config,
        "authenticator": "PasswordAuthenticator",
        "authorizer": "CassandraAuthorizer",
    }

    # Add server but don't start it yet
    server = await manager.server_add(config=config, start=False)

    # Remove the default superuser configuration to simulate a clean start without seeding
    await manager.server_remove_config_option(server.server_id, 'auth_superuser_name')
    await manager.server_remove_config_option(server.server_id, 'auth_superuser_salted_password')

    # Now start the server
    await manager.server_start(server.server_id, connect_driver=False)

    # 1. Verify we cannot connect with default credentials
    try:
        cluster = Cluster([server.ip_addr], auth_provider=PlainTextAuthProvider(username="cassandra", password="cassandra"))
        cluster.connect()
        pytest.fail("Should not be able to connect with default credentials when they are not seeded")
    except Exception:
        pass

    # 2. Connect via Maintenance Socket
    socket_path = await manager.server_get_maintenance_socket_path(server.server_id)
    
    async def get_maintenance_session():
        maintenance_cluster = cluster_con(
            [UnixSocketEndPoint(socket_path)],
            load_balancing_policy=WhiteListRoundRobinPolicy([UnixSocketEndPoint(socket_path)])
        )
        try:
            session = maintenance_cluster.connect()
        except NoHostAvailable:
            maintenance_cluster.shutdown()
            return None

        try:
            session.execute("SELECT role FROM system.roles LIMIT 1")
            return session
        except InvalidRequest:
            maintenance_cluster.shutdown()
            return None
        except Exception:
            maintenance_cluster.shutdown()
            raise

    session = await wait_for(get_maintenance_session, time.time() + 60)

    # 3. Check system.roles content BEFORE operations
    rows = list(session.execute("SELECT role, is_superuser FROM system.roles"))
    assert len(rows) == 0, f"Expected no roles, found: {rows}"

    # 4. Test CREATE ROLE
    # Wait for role manager to be ready by probing with DROP ROLE
    async def wait_for_role_manager():
        try:
            session.execute("DROP ROLE IF EXISTS this_role_should_not_exist")
            return True
        except NoHostAvailable as e:
            for err in e.errors.values():
                if "role operations not enabled" in str(err):
                    return None
            raise e

    await wait_for(wait_for_role_manager, time.time() + 60)

    new_role = "admin_user"
    session.execute(f"CREATE ROLE {new_role} WITH PASSWORD = 'password' AND SUPERUSER = true AND LOGIN = true")

    rows = list(session.execute(f"SELECT role, is_superuser, can_login, salted_hash FROM system.roles WHERE role = '{new_role}'"))
    assert len(rows) == 1
    assert rows[0].role == new_role
    assert rows[0].is_superuser == True
    assert rows[0].can_login == True
    assert rows[0].salted_hash
    # Verify SHA-512 salted hash format: $6$<salt>$<hash>
    assert rows[0].salted_hash.startswith("$6$")
    assert len(rows[0].salted_hash.split('$')) == 4

    # 5. Test ALTER ROLE
    session.execute(f"ALTER ROLE {new_role} WITH SUPERUSER = false")

    rows = list(session.execute(f"SELECT role, is_superuser, can_login FROM system.roles WHERE role = '{new_role}'"))
    assert len(rows) == 1
    assert rows[0].is_superuser == False
    assert rows[0].can_login == True

    # 6. Test DROP ROLE
    session.execute(f"DROP ROLE {new_role}")

    rows = list(session.execute(f"SELECT role FROM system.roles WHERE role = '{new_role}'"))
    assert len(rows) == 0

    # 7. Test CREATE ROLE WITH HASHED PASSWORD
    hashed_role = "hashed_user"
    known_hash = '$6$x7IFjiX5VCpvNiFk$2IfjTvSyGL7zerpV.wbY7mJjaRCrJ/68dtT3UpT.sSmNYz1bPjtn3mH.kJKFvaZ2T4SbVeBijjmwGjcb83LlV/'

    session.execute(f"CREATE ROLE {hashed_role} WITH HASHED PASSWORD = '{known_hash}' AND LOGIN = true")

    rows = list(session.execute(f"SELECT role, salted_hash FROM system.roles WHERE role = '{hashed_role}'"))
    assert len(rows) == 1
    assert rows[0].role == hashed_role
    assert rows[0].salted_hash == known_hash

    session.cluster.shutdown()


