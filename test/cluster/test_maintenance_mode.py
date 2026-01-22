#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from cassandra.protocol import ConfigurationException
from cassandra.connection import UnixSocketEndPoint
from cassandra.policies import WhiteListRoundRobinPolicy

from test.pylib.manager_client import ManagerClient
from test.cluster.conftest import cluster_con
from test.pylib.util import wait_for_cql_and_get_hosts
from test.cluster.util import new_test_keyspace

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

    cluster = cluster_con([server_b.ip_addr])
    cql = cluster.connect()

    async with new_test_keyspace(manager, "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}") as ks:
        table = f"{ks}.t"
        await cql.run_async(f"CREATE TABLE {table} (k int PRIMARY KEY, v int)")

        # Token ranges of the server A
        # [(start_token, end_token)]
        ranges = [(int(row[0]), int(row[1])) for row in await cql.run_async(f"""SELECT start_token, end_token
                                                                                FROM system.token_ring WHERE keyspace_name = '{ks}'
                                                                                AND endpoint = '{server_a.ip_addr}' ALLOW FILTERING""")]
        key_on_server_a = None

        # Insert data to the cluster until a key is stored on server A.
        new_key = 0
        while key_on_server_a is None:
            if new_key == 1000:
                # The probability of reaching this code is (1/2)^1000 for RF=1. This is much
                # less than, for example, the probability of a UUID collision, so worrying about this would be silly.
                # It could still happen due to a bug, and then we want to know about it, so we fail the test.
                pytest.fail(f"Could not find a key on server {server_a} after inserting 1000 keys")
            new_key += 1

            await cql.run_async(f"INSERT INTO {table} (k, v) VALUES ({new_key}, {new_key})")

            res = await cql.run_async(f"SELECT token(k) FROM {table} WHERE k = {new_key}")
            assert len(res) == 1
            token = res[0][0]
            for start, end in ranges:
                if (start < end and start < token <= end) or (start >= end and (token <= end or start < token)):
                    logger.info(f"Found key {new_key} with token {token} on server {server_a} for table {table}")
                    key_on_server_a = new_key

        # Start server A in maintenance mode
        await manager.server_stop_gracefully(server_a.server_id)
        await manager.server_update_config(server_a.server_id, "maintenance_mode", "true")
        await manager.server_start(server_a.server_id)

        log = await manager.server_open_log(server_a.server_id)
        await log.wait_for(r"initialization completed \(maintenance mode\)")

        # Check that the regular CQL port is not available
        assert socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect_ex((server_a.ip_addr, 9042)) != 0

        maintenance_cluster = cluster_con([socket_endpoint],
                                        load_balancing_policy=WhiteListRoundRobinPolicy([socket_endpoint]))
        maintenance_cql = maintenance_cluster.connect()

        # Check that local data is available in maintenance mode
        res = await maintenance_cql.run_async(f"SELECT v FROM {table} WHERE k = {key_on_server_a}")
        assert res[0][0] == key_on_server_a

        # Check that group0 operations are disabled
        with pytest.raises(ConfigurationException):
            await maintenance_cql.run_async(f"CREATE TABLE {ks}.t2 (k int PRIMARY KEY, v int)")

        await maintenance_cql.run_async(f"UPDATE {table} SET v = {key_on_server_a + 1} WHERE k = {key_on_server_a}")

        # Ensure that server B recognizes server A as being shutdown, not as being alive.
        res = await cql.run_async(f"SELECT status FROM system.cluster_status WHERE peer = '{server_a.ip_addr}'")
        assert res[0][0] == "shutdown"

        await manager.server_stop_gracefully(server_a.server_id)

        # Restart in normal mode to see if the changes made in maintenance mode are persisted
        await manager.server_update_config(server_a.server_id, "maintenance_mode", False)
        await manager.server_start(server_a.server_id, wait_others=1)
        await wait_for_cql_and_get_hosts(cql, [server_a], time.time() + 60)
        await manager.servers_see_each_other([server_a, server_b])

        res = await cql.run_async(f"SELECT v FROM {table} WHERE k = {key_on_server_a}")
        assert res[0][0] == key_on_server_a + 1


