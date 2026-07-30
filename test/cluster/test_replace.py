#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Test replacing node in different scenarios
"""
import time
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.manager_client import ManagerClient
from test.cluster.util import wait_for_token_ring_and_group0_consistency, wait_for_cql_and_get_hosts, \
    wait_for_replacement_propagation
import logging
import asyncio
from cassandra.query import SimpleStatement, ConsistencyLevel
from test.pylib.random_tables import RandomTables, Column, TextType


logger = logging.getLogger(__name__)


async def test_replace_different_ip(manager: ManagerClient) -> None:
    """Replace an existing node with new node using a different IP address"""
    servers = await manager.servers_add(3)
    logger.info(f"cluster started, servers {servers}")

    logger.info(f"replacing server {servers[0]}")
    replaced_server = servers[0]
    old_host_id = await manager.get_host_id(replaced_server.server_id)
    await manager.server_stop(replaced_server.server_id, convict=True)
    replace_cfg = ReplaceConfig(replaced_id = replaced_server.server_id, reuse_ip_addr = False, use_host_id = False)
    new_server = await manager.server_add(replace_cfg)
    logger.info(f"new server {new_server} started, "
                "waiting for token ring and group0 consistency")
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

    await wait_for_replacement_propagation(
        manager, new_server, old_host_id, replaced_server.ip_addr, time.time() + 60)

async def test_replace_different_ip_using_host_id(manager: ManagerClient) -> None:
    """Replace an existing node with new node reusing the replaced node host id"""
    servers = await manager.servers_add(3)
    await manager.server_stop(servers[0].server_id, convict=True)
    replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = False, use_host_id = True)
    await manager.server_add(replace_cfg)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

async def test_replace_reuse_ip(request, manager: ManagerClient) -> None:
    """Replace an existing node with new node using the same IP address"""
    servers = await manager.servers_add(3, auto_rack_dc="dc1")
    host2 = (await wait_for_cql_and_get_hosts(manager.get_cql(), [servers[2]], time.time() + 60))[0]

    logger.info(f"creating test table")
    random_tables = RandomTables(request.node.name, manager, "ks", 3)
    await random_tables.add_table(name='test_table', pks=1, columns=[
        Column(name="key", ctype=TextType),
        Column(name="value", ctype=TextType)
    ])

    old_host_id = await manager.get_host_id(servers[0].server_id)
    await manager.server_stop_gracefully(servers[0].server_id)
    replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = True, use_host_id = False)
    replace_future = asyncio.create_task(manager.server_add(replace_cfg, property_file=servers[0].property_file()))
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
    s = await replace_future
    logger.info(f"done, writes count {next_id}, took {finish_time - start_time} seconds")

    # make sure that after we start snapshot transfer we no longer have stale writes
    log = await manager.server_open_log(s.server_id)
    m, _ = await log.wait_for("group0_raft_sm - transfer snapshot from ")
    errs = await log.grep("storage_proxy - Failed to apply mutation from", from_mark=m)
    assert len(errs) == 0

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
    await wait_for_replacement_propagation(
        manager, s, old_host_id, servers[0].ip_addr, time.time() + 60)

async def test_replace_reuse_ip_using_host_id(manager: ManagerClient) -> None:
    """Replace an existing node with new node using the same IP address and same host id"""
    servers = await manager.servers_add(3)
    await manager.server_stop(servers[0].server_id, convict=True)
    replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = True, use_host_id = True)
    await manager.server_add(replace_cfg)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)
