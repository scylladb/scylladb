# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables
from test.cluster.conftest import skip_mode, cluster_con
from test.pylib.random_tables import Column, IntType
from cassandra.policies import WhiteListRoundRobinPolicy
from test.pylib.rest_client import inject_error_one_shot

import asyncio
import pytest
import logging

logger = logging.getLogger(__name__)

@skip_mode('release', 'error injections are not supported in release mode')
@pytest.mark.parametrize("query_timeout, external_timeout, expected_error", [
    (1800, 10000, "Operation failed for"),
    (10, 1800000, "Operation timed out")
])
@pytest.mark.asyncio
async def test_external_speculative_retry(request, manager: ManagerClient, query_timeout, external_timeout, expected_error):
    """
    This test verifies system behavior if one node is removed during read.
    The expected behaviour, is that after detection of node disconnection,
    system should either wait for external_speculative_retry_timeout or
    wait for query timeout, whichever is first.

    There are two parametrized versions of the test:
      1) query_timeout > external_timeout: user receives "Operation failed" error after external_timeout.
      2) query_timeout < external_timeout: user receives "Operation timed out" error after the query timeouts.
    """
    logger.info("Start four nodes cluster, use large number of tablets to ensure distribution of reads")
    servers = await manager.servers_add(
        4,
        config={'tablets_initial_scale_factor': 32, 'external_speculative_retry_timeout_in_ms': external_timeout}
    )

    selected_server = servers[0]
    logger.info(f"Creating client with selected_server: {selected_server}")
    cql = cluster_con([selected_server.ip_addr], 9042, False,
        load_balancing_policy=WhiteListRoundRobinPolicy([selected_server.ip_addr])).connect()

    logger.info("Create table, write some rows")
    random_tables = RandomTables(request.node.name, manager, "ks", replication_factor=3, enable_tablets=True)
    table = await random_tables.add_table(pks=1, columns=[
        Column(name="key", ctype=IntType),
        Column(name="value", ctype=IntType)
    ])
    await asyncio.gather(*[cql.run_async(f"INSERT INTO {table} (key, value) VALUES ({i}, {i})") for i in range(256)])

    logger.info("Enable injected errors that stop reads")
    injected_handlers = {}
    for server in servers:
        injected_handlers[server.ip_addr] = await inject_error_one_shot(
            manager.api, server.ip_addr, 'storage_proxy::handle_read', parameters={'cf_name': str(table).split(".")[1]})


    logger.info("Start executing an aggregate query")
    query_future = cql.run_async(f"SELECT COUNT(*) FROM {table} BYPASS CACHE USING TIMEOUT {query_timeout}s", timeout=3600)

    logger.info("Confirm reads are waiting on the injected error")
    waiting_servers = []
    server_to_kill = None
    for server in servers:
        server_log = await manager.server_open_log(server.server_id)
        try:
            await server_log.wait_for("storage_proxy::handle_read injection hit", timeout=10)
            waiting_servers.append(server)
            if server != selected_server:
                server_to_kill = server
        except TimeoutError:
            # Some nodes might not receive any reads
            logger.info(f"Node not handling any reads: {server.ip_addr}")

    logger.info(f"Kill node: {server_to_kill.ip_addr}")
    await manager.server_stop(server_to_kill.server_id)

    logger.info("Unblock reads")
    for server in waiting_servers:
        if server != server_to_kill:
            await injected_handlers[server.ip_addr].message()

    with pytest.raises(Exception, match=expected_error):
        await query_future
