#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test replacing node in different scenarios
"""
import time
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.manager_client import ManagerClient
from test.topology.util import wait_for_token_ring_and_group0_consistency, wait_for_cql_and_get_hosts, wait_for
import pytest
import logging
import asyncio
from cassandra.query import SimpleStatement, ConsistencyLevel
from test.pylib.random_tables import RandomTables, Column, TextType


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_replace_different_ip(manager: ManagerClient) -> None:
    """Replace an existing node with new node using a different IP address"""
    servers = await manager.servers_add(3, config={'failure_detector_timeout_in_ms': 2000})
    logger.info(f"cluster started, servers {servers}")

    logger.info(f"replacing server {servers[0]}")
    await manager.server_stop(servers[0].server_id)
    replaced_server = servers[0]
    replace_cfg = ReplaceConfig(replaced_id = replaced_server.server_id, reuse_ip_addr = False, use_host_id = False)
    new_server = await manager.server_add(replace_cfg)
    cql = manager.get_cql()
    servers = await manager.running_servers()
    all_ips = set([s.rpc_address for s in servers])
    logger.info(f"new server {new_server} started, all ips {all_ips}, "
                "waiting for token ring and group0 consistency")
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

    for s in servers:
        peers_to_see = all_ips - {s.rpc_address}

        logger.info(f'waiting for cql and get hosts for {s}')
        h = (await wait_for_cql_and_get_hosts(cql, [s], time.time() + 60))[0]

        logger.info(f"waiting for {s} to see its peers {peers_to_see}")
        async def check_peers_and_gossiper():
            peers = set([r.peer for r in await cql.run_async("select peer from system.peers", host=h)])
            remaining = peers_to_see - peers
            if remaining:
                logger.info(f"server {h} doesn't see its peers, all_ips {all_ips}, peers_to_see {peers_to_see}, remaining {remaining}, continue waiting")
                return None

            alive_eps = await manager.api.get_alive_endpoints(s.ip_addr)
            if replaced_server.ip_addr in alive_eps:
                logger.info(f"server {h}, replaced ip {replaced_server.ip_addr} is contained in alive eps {alive_eps}, continue waiting")
                return None

            down_eps = await manager.api.get_down_endpoints(s.ip_addr)
            if replaced_server.ip_addr in down_eps:
                logger.info(f"server {h}, replaced ip {replaced_server.ip_addr} is contained in down eps {down_eps}, continue waiting")
                return None

            return True
        await wait_for(check_peers_and_gossiper, time.time() + 60)
        logger.info(f"server {s} system.peers and gossiper state is valid")

@pytest.mark.asyncio
async def test_replace_different_ip_using_host_id(manager: ManagerClient) -> None:
    """Replace an existing node with new node reusing the replaced node host id"""
    servers = await manager.servers_add(3, config={'failure_detector_timeout_in_ms': 2000})
    await manager.server_stop(servers[0].server_id)
    replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = False, use_host_id = True)
    await manager.server_add(replace_cfg)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

@pytest.mark.asyncio
async def test_replace_reuse_ip(request, manager: ManagerClient) -> None:
    """Replace an existing node with new node using the same IP address"""
    servers = await manager.servers_add(3, config={'failure_detector_timeout_in_ms': 2000})
    host2 = (await wait_for_cql_and_get_hosts(manager.get_cql(), [servers[2]], time.time() + 60))[0]

    logger.info(f"creating test table")
    random_tables = RandomTables(request.node.name, manager, "ks", 3)
    await random_tables.add_table(name='test_table', pks=1, columns=[
        Column(name="key", ctype=TextType),
        Column(name="value", ctype=TextType)
    ])

    await manager.server_stop_gracefully(servers[0].server_id)
    replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = True, use_host_id = False)
    replace_future = asyncio.create_task(manager.server_add(replace_cfg))
    start_time = time.time()
    next_id = 0
    logger.info(f"running write requests in a loop while the replacing node is starting")
    expected_data = []
    while not replace_future.done():
        i = next_id
        next_id += 1
        k = f'key_{i}'
        v = f'value_{i}'
        expected_data.append((k, v))
        await manager.get_cql().run_async(SimpleStatement("insert into ks.test_table(key, value) values (%s, %s)",
                                                          consistency_level=ConsistencyLevel.QUORUM),
                                          parameters=[k, v],
                                          host=host2)
    finish_time = time.time()
    await replace_future
    logger.info(f"done, writes count {next_id}, took {finish_time - start_time} seconds")

    result_set = await manager.get_cql().run_async(SimpleStatement("select * from ks.test_table",
                                                                   consistency_level=ConsistencyLevel.QUORUM),
                                                   host=host2, all_pages=True)
    read_data = [(row.key, row.value) for row in result_set]
    expected_data.sort()
    read_data.sort()
    logger.info(f"expected data:\n{expected_data}\nread_data:\n{read_data}")
    assert read_data == expected_data
    logger.info("the data is correct")

    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)
    await manager.server_sees_other_server(servers[1].ip_addr, servers[0].ip_addr)
    await manager.server_sees_other_server(servers[2].ip_addr, servers[0].ip_addr)

@pytest.mark.asyncio
async def test_replace_reuse_ip_using_host_id(manager: ManagerClient) -> None:
    """Replace an existing node with new node using the same IP address and same host id"""
    servers = await manager.servers_add(3, config={'failure_detector_timeout_in_ms': 2000})
    await manager.server_stop(servers[0].server_id)
    replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = True, use_host_id = True)
    await manager.server_add(replace_cfg)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)
