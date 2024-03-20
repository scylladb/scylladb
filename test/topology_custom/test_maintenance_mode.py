#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from cassandra.protocol import ConfigurationException
from cassandra.connection import UnixSocketEndPoint
from cassandra.policies import WhiteListRoundRobinPolicy

from test.pylib.manager_client import ManagerClient
from test.topology.conftest import cluster_con
from test.pylib.util import wait_for_cql_and_get_hosts

import pytest
import logging
import socket
import time

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_maintenance_mode(manager: ManagerClient):
    """
    The test checks that in maintenance mode server A is not available for other nodes and for clients.
    It is possible to connect by the maintenance socket to server A and perform local CQL operations.
    """

    server_a, server_b = await manager.server_add(), await manager.server_add()
    socket_endpoint = UnixSocketEndPoint(await manager.server_get_maintenance_socket_path(server_a.server_id))

    cluster = cluster_con([server_b.ip_addr], 9042, False)
    cql = cluster.connect()

    await cql.run_async("CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    await cql.run_async("CREATE TABLE ks.t (k int PRIMARY KEY, v int)")

    # Token ranges of the server A
    # [(start_token, end_token)]
    ranges = [(int(row[0]), int(row[1])) for row in await cql.run_async(f"""SELECT start_token, end_token, endpoint
                                                                            FROM system.token_ring WHERE keyspace_name = 'ks'
                                                                            AND endpoint = '{server_a.ip_addr}' ALLOW FILTERING""")]

    # Insert data to the cluster and find a key that is stored on server A.
    for i in range(256):
        await cql.run_async(f"INSERT INTO ks.t (k, v) VALUES ({i}, {i})")

    # [(key, token of this key)]
    keys_with_tokens = [(int(row[0]), int(row[1])) for row in await cql.run_async("SELECT k, token(k) FROM ks.t")]
    key_on_server_a = None

    for key, token in keys_with_tokens:
        for start, end in ranges:
            if (start < end and start < token <= end) or (start >= end and (token <= end or start < token)):
                key_on_server_a = key

    if key_on_server_a is None:
        # There is only a chance ~(1/2)^256 that all keys are stored on the server B
        # In this case we skip the test
        pytest.skip("All keys are stored on the server B")

    # Start server A in maintenance mode
    await manager.server_stop_gracefully(server_a.server_id)
    await manager.server_update_config(server_a.server_id, "maintenance_mode", "true")
    await manager.server_start(server_a.server_id)

    # Check that the regular CQL port is not available
    assert socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect_ex((server_a.ip_addr, 9042)) != 0

    maintenance_cluster = cluster_con([socket_endpoint], 9042, False,
                                      load_balancing_policy=WhiteListRoundRobinPolicy([socket_endpoint]))
    maintenance_cql = maintenance_cluster.connect()

    # Check that local data is available in maintenance mode
    res = await maintenance_cql.run_async(f"SELECT v FROM ks.t WHERE k = {key_on_server_a}")
    assert res[0][0] == key_on_server_a

    # Check that group0 operations are disabled
    with pytest.raises(ConfigurationException):
        await maintenance_cql.run_async(f"CREATE TABLE ks.t2 (k int PRIMARY KEY, v int)")

    await maintenance_cql.run_async(f"UPDATE ks.t SET v = {key_on_server_a + 1} WHERE k = {key_on_server_a}")

    # Ensure that server B recognizes server A as being shutdown, not as being alive.
    res = await cql.run_async(f"SELECT status FROM system.cluster_status WHERE peer = '{server_a.ip_addr}'")
    assert res[0][0] == "shutdown"

    await manager.server_stop_gracefully(server_a.server_id)

    # Restart in normal mode to see if the changes made in maintenance mode are persisted
    await manager.server_update_config(server_a.server_id, "maintenance_mode", "false")
    await manager.server_start(server_a.server_id, wait_others=1)
    await wait_for_cql_and_get_hosts(cql, [server_a], time.time() + 60)
    await manager.servers_see_each_other([server_a, server_b])

    res = await cql.run_async(f"SELECT v FROM ks.t WHERE k = {key_on_server_a}")
    assert res[0][0] == key_on_server_a + 1


