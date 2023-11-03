#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.manager_client import ManagerClient
from test.pylib.internal_types import ServerInfo
from test.pylib.util import unique_name, wait_for_cql_and_get_hosts
from test.topology.util import check_token_ring_and_group0_consistency, reconnect_driver

from cassandra.cluster import Session, ConsistencyLevel
from cassandra.query import SimpleStatement

import asyncio
import time
import pytest
import logging

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_topology_ops(request, manager: ManagerClient):
    """Test basic topology operations using the topology coordinator."""
    logger.info("Bootstrapping first node")
    servers = [await manager.server_add()]

    logger.info(f"Restarting node {servers[0]}")
    await manager.server_stop_gracefully(servers[0].server_id)
    await manager.server_start(servers[0].server_id)

    await wait_for_cql_and_get_hosts(manager.cql, await manager.running_servers(), time.time() + 60)
    cql = await reconnect_driver(manager)
    # FIXME: disabled as a workaround for #15935, #15924
    # We need to reenable once these issues are fixed.
    #finish_writes = await start_writes(cql)

    logger.info("Bootstrapping other nodes")
    servers += [await manager.server_add(), await manager.server_add()]

    logger.info(f"Restarting node {servers[0]} when other nodes have bootstrapped")
    await manager.server_stop_gracefully(servers[0].server_id)
    await manager.server_start(servers[0].server_id)

    logger.info(f"Stopping node {servers[0]}")
    await manager.server_stop_gracefully(servers[0].server_id)
    await check_node_log_for_failed_mutations(manager, servers[0])

    logger.info(f"Replacing node {servers[0]}")
    replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = False, use_host_id = False)
    servers = servers[1:] + [await manager.server_add(replace_cfg)]
    await check_token_ring_and_group0_consistency(manager)

    logger.info(f"Stopping node {servers[0]}")
    await manager.server_stop_gracefully(servers[0].server_id)
    await check_node_log_for_failed_mutations(manager, servers[0])

    logger.info(f"Removing node {servers[0]} using {servers[1]}")
    await manager.remove_node(servers[1].server_id, servers[0].server_id)
    await check_token_ring_and_group0_consistency(manager)
    servers = servers[1:]

    logger.info(f"Decommissioning node {servers[0]}")
    await manager.decommission_node(servers[0].server_id)
    await check_token_ring_and_group0_consistency(manager)

    logger.info("Checking results of the background writes")
    #await finish_writes()

    for server in servers:
        await check_node_log_for_failed_mutations(manager, server)


async def check_node_log_for_failed_mutations(manager: ManagerClient, server: ServerInfo):
    logger.info(f"Checking that node {server} had no failed mutations")
    log = await manager.server_open_log(server.server_id)
    occurrences = await log.grep(expr="Failed to apply mutation from", \
                                 filter_expr="replica::stale_topology_exception") # Disabled due to #15804
    assert len(occurrences) == 0


async def start_writes(cql: Session, concurrency: int = 3):
    logger.info(f"Starting to asynchronously write, concurrency = {concurrency}")

    stop_event = asyncio.Event()

    ks_name = unique_name()
    await cql.run_async(f"CREATE KEYSPACE {ks_name} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}}")
    await cql.run_async(f"USE {ks_name}")
    await cql.run_async(f"CREATE TABLE tbl (pk int PRIMARY KEY, v int)")

    # In the test we only care about whether operations report success or not
    # and whether they trigger errors in the nodes' logs. Inserting the same
    # value repeatedly is enough for our purposes.
    stmt = SimpleStatement("INSERT INTO tbl (pk, v) VALUES (0, 0)", consistency_level=ConsistencyLevel.ONE)

    async def do_writes(worker_id: int):
        write_count = 0
        last_error = None
        while not stop_event.is_set():
            start_time = time.time()
            try:
                await cql.run_async(stmt)
                write_count += 1
            except Exception as e:
                logger.error(f"Write started {time.time() - start_time}s ago failed: {e}")
                last_error = e
        logger.info(f"Worker #{worker_id} did {write_count} successful writes")
        if last_error is not None:
            raise last_error

    tasks = [asyncio.create_task(do_writes(worker_id)) for worker_id in range(concurrency)]

    async def finish():
        logger.info("Stopping write workers")
        stop_event.set()
        await asyncio.gather(*tasks)

    return finish
