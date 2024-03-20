#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra import Unauthorized
from cassandra.connection import UnixSocketEndPoint
from test.pylib.manager_client import ManagerClient

import pytest

@pytest.mark.asyncio
async def test_maintenance_socket(manager: ManagerClient):
    """
    Test that when connecting to the maintenance socket, the user has superuser permissions,
    even if the authentication is enabled on the regular port.
    """
    config = {
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

    cluster = Cluster([server.ip_addr], auth_provider=PlainTextAuthProvider(username="cassandra", password="cassandra"))
    session = cluster.connect()

    session.execute("CREATE ROLE john WITH PASSWORD = 'password' AND LOGIN = true;")
    session.execute("CREATE KEYSPACE ks1 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};")
    session.execute("CREATE KEYSPACE ks2 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};")
    session.execute("CREATE TABLE ks1.t1 (pk int PRIMARY KEY, val int);")
    session.execute("CREATE TABLE ks2.t1 (pk int PRIMARY KEY, val int);")
    session.execute("GRANT SELECT ON ks1.t1 TO john;")

    cluster = Cluster([server.ip_addr], auth_provider=PlainTextAuthProvider(username="john", password="password"))
    session = cluster.connect()
    try:
        session.execute("SELECT * FROM ks2.t1")
    except Unauthorized:
        pass
    else:
        pytest.fail("User 'john' has no permissions to access ks2.t1")

    maintenance_cluster = Cluster([UnixSocketEndPoint(socket)])
    maintenance_session = maintenance_cluster.connect()

    # check that the maintenance session has superuser permissions
    maintenance_session.execute("SELECT * FROM ks1.t1")
    maintenance_session.execute("SELECT * FROM ks2.t1")
    maintenance_session.execute("INSERT INTO ks1.t1 (pk, val) VALUES (1, 1);")
    maintenance_session.execute("CREATE KEYSPACE ks3 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};")
    maintenance_session.execute("CREATE TABLE ks1.t2 (pk int PRIMARY KEY, val int);")
