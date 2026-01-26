#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from cassandra.protocol import ConfigurationException
from cassandra.connection import UnixSocketEndPoint
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.query import SimpleStatement, ConsistencyLevel

from test.pylib.manager_client import ManagerClient
from test.pylib.tablets import get_all_tablet_replicas
from test.cluster.conftest import cluster_con
from test.pylib.util import gather_safely, wait_for_cql_and_get_hosts
from test.cluster.util import create_new_test_keyspace

import pytest
import logging
import socket
import time
from typing import TypeAlias

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_maintenance_mode(manager: ManagerClient):
    """
    The test checks that in maintenance mode server A is not available for other nodes and for clients.
    It is possible to connect by the maintenance socket to server A and perform local CQL operations.

    The test is run with multiple keyspaces with different configurations (replication strategy, RF, tablets enabled).
    It initially used only SimpleStrategy and RF=1, which hid https://github.com/scylladb/scylladb/issues/27988. To keep
    the test fast, the tasks for different keyspaces are performed concurrently, and server A is started in maintenance
    mode only once.
    """
    max_rf = 3
    servers = await manager.servers_add(max_rf, auto_rack_dc='dc1', config={"rf_rack_valid_keyspaces": False})
    server_a = servers[0]
    host_id_a = await manager.get_host_id(server_a.server_id)
    socket_endpoint = UnixSocketEndPoint(await manager.server_get_maintenance_socket_path(server_a.server_id))

    # For the move_tablet API.
    await manager.api.disable_tablet_balancing(server_a.ip_addr)

    # An exclusive connection to server A is needed for requests with LocalStrategy.
    cluster = cluster_con([server_a.ip_addr], 9042, False,
                          load_balancing_policy=WhiteListRoundRobinPolicy([server_a.ip_addr]))
    cql = cluster.connect()

    # (replication strategy, Optional[replication factor], tablets enabled)
    KeyspaceOptions: TypeAlias = tuple[str, int | None, bool]
    keyspace_options: list[KeyspaceOptions] = []
    keyspace_options.append(('EverywhereStrategy', None, False))
    keyspace_options.append(('LocalStrategy', None, False))
    for rf in range(1, max_rf + 1):
        keyspace_options.append(('SimpleStrategy', rf, False))
        for tablets_enabled in [True, False]:
            keyspace_options.append(('NetworkTopologyStrategy', rf, tablets_enabled))

    key_on_server_a_per_table: dict[str, int] = dict()

    async def prepare_table(options: KeyspaceOptions):
        replication_strategy, rf, tablets_enabled = options
        rf_string = "" if rf is None else f", 'replication_factor': {rf}"
        ks = await create_new_test_keyspace(cql,
                f"""WITH REPLICATION = {{'class': '{replication_strategy}'{rf_string}}}
                AND tablets = {{'enabled': {str(tablets_enabled).lower()}, 'initial': 1}}""")
        rf_tag = "" if rf is None else f"rf{rf}"
        tablets_tag = "tablets" if tablets_enabled else "vnodes"
        table_suffix = f"{replication_strategy.lower()}_{rf_tag}_{tablets_tag}"
        table = f"{ks}.{table_suffix}"
        await cql.run_async(f"CREATE TABLE {table} (k int PRIMARY KEY, v int)")
        logger.info(f"Created table {table}")

        async def insert_one(cl: ConsistencyLevel):
            key = 1
            insert_stmt = SimpleStatement(f"INSERT INTO {table} (k, v) VALUES ({key}, {key})",
                                          consistency_level=cl)
            await cql.run_async(insert_stmt)
            key_on_server_a_per_table[table] = key

        if replication_strategy == 'LocalStrategy':
            await insert_one(ConsistencyLevel.ONE)
            return

        if tablets_enabled:
            await insert_one(ConsistencyLevel.ALL)

            logger.info(f"Ensuring that a tablet replica is on {server_a} for table {table}")
            [tablet] = await get_all_tablet_replicas(manager, server_a, ks, table_suffix)
            if host_id_a not in [r[0] for r in tablet.replicas]:
                assert rf < max_rf
                any_replica = tablet.replicas[0]
                logger.info(f"Moving tablet from {any_replica} to {server_a} for table {table}")
                await manager.api.move_tablet(server_a.ip_addr, ks, table_suffix,
                                              any_replica[0], any_replica[1],
                                              host_id_a, 0,
                                              tablet.last_token)
            return

        # This path is executed only for vnodes-based keyspaces.

        # Token ranges of the server A
        # [(start_token, end_token)]
        ranges = [(int(row[0]), int(row[1])) for row in await cql.run_async(f"""SELECT start_token, end_token
                                                                                FROM system.token_ring WHERE keyspace_name = '{ks}'
                                                                                AND endpoint = '{server_a.ip_addr}' ALLOW FILTERING""")]

        # Insert data to the cluster until a key is stored on server A.
        new_key = 0
        while table not in key_on_server_a_per_table:
            if new_key == 1000:
                # The probability of reaching this code is (2/3)^1000 for RF=1 and lower for greater RFs. This is much
                # less than, for example, the probability of a UUID collision, so worrying about this would be silly.
                # It could still happen due to a bug, and then we want to know about it, so we fail the test.
                pytest.fail(f"Could not find a key on server {server_a} after inserting 1000 keys")
            new_key += 1

            insert_stmt = SimpleStatement(f"INSERT INTO {table} (k, v) VALUES ({new_key}, {new_key})",
                                          consistency_level=ConsistencyLevel.ALL)
            await cql.run_async(insert_stmt)

            res = await cql.run_async(f"SELECT token(k) FROM {table} WHERE k = {new_key}")
            assert len(res) == 1
            token = res[0][0]
            for start, end in ranges:
                if (start < end and start < token <= end) or (start >= end and (token <= end or start < token)):
                    logger.info(f"Found key {new_key} with token {token} on server {server_a} for table {table}")
                    key_on_server_a_per_table[table] = new_key

    logger.info("Preparing tables")
    await gather_safely(*(prepare_table(options) for options in keyspace_options))

    # Start server A in maintenance mode
    await manager.server_stop_gracefully(server_a.server_id)
    await manager.server_update_config(server_a.server_id, "maintenance_mode", True)
    await manager.server_start(server_a.server_id)

    log = await manager.server_open_log(server_a.server_id)
    await log.wait_for(r"initialization completed \(maintenance mode\)")

    # Check that the regular CQL port is not available
    assert socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect_ex((server_a.ip_addr, 9042)) != 0

    maintenance_cluster = cluster_con([socket_endpoint], 9042, False,
                                      load_balancing_policy=WhiteListRoundRobinPolicy([socket_endpoint]))
    maintenance_cql = maintenance_cluster.connect()

    async def update_table_in_maintenance_mode(table: str, key: int):
        # Check that local data is available in maintenance mode
        select_stm = SimpleStatement(f"SELECT v FROM {table} WHERE k = {key}", consistency_level=ConsistencyLevel.ONE)
        res = await maintenance_cql.run_async(select_stm)
        assert len(res) == 1 and res[0][0] == key, f"Expected {key} for table {table}"

        update_stm = SimpleStatement(f"UPDATE {table} SET v = {key + 1} WHERE k = {key}",
                                     consistency_level=ConsistencyLevel.ONE)
        await maintenance_cql.run_async(update_stm)

    logger.info("Updating tables in maintenance mode")
    await gather_safely(*(update_table_in_maintenance_mode(table, key)
                        for table, key in key_on_server_a_per_table.items()))

    # Check that group0 operations are disabled
    with pytest.raises(ConfigurationException, match="cannot start group0 operation in the maintenance mode"):
        await create_new_test_keyspace(
                maintenance_cql, "WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")

    # Ensure that another server recognizes server A as being shutdown, not as being alive.
    cql_b, [host_b] = await manager.get_ready_cql([servers[1]])
    res = await cql_b.run_async(f"SELECT status FROM system.cluster_status WHERE peer = '{server_a.ip_addr}'",
                                host=host_b)
    assert len(res) == 1
    assert res[0][0] == "shutdown"

    await manager.server_stop_gracefully(server_a.server_id)

    # Restart in normal mode
    await manager.server_update_config(server_a.server_id, "maintenance_mode", False)
    await manager.server_start(server_a.server_id, wait_others=1)
    await wait_for_cql_and_get_hosts(cql, [server_a], time.time() + 60)
    await manager.servers_see_each_other(servers)

    async def check_table_in_normal_mode(table: str, key: int):
        # Check if the changes made in maintenance mode are persisted
        select_stm = SimpleStatement(f"SELECT v FROM {table} WHERE k = {key}", consistency_level=ConsistencyLevel.ALL)
        res = await cql.run_async(select_stm)
        assert len(res) == 1 and res[0][0] == key + 1, f"Expected {key + 1} for table {table}"

    logger.info("Checking tables in normal mode")
    await gather_safely(*(check_table_in_normal_mode(table, key) for table, key in key_on_server_a_per_table.items()))

    cluster.shutdown()
    maintenance_cluster.shutdown()
