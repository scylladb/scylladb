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
from datetime import datetime, timedelta
import pytest
import logging

logger = logging.getLogger(__name__)
@skip_mode('release', 'error injections are not supported in release mode')
@pytest.mark.asyncio
@pytest.mark.parametrize("query,should_wait_for_timeout,force_disable_speculative_retries", [
    ("SELECT * FROM {}", True, False),
    ("SELECT COUNT(*) FROM {}", False, False),
    ("SELECT * FROM {} WHERE key = 0", True, True),
    ("SELECT COUNT(*) FROM {} WHERE key = 0", True, True),
])
async def test_long_query_timeout_erm(request, manager: ManagerClient, query, should_wait_for_timeout, force_disable_speculative_retries):
    """
    Test verifies that a query with long timeout doesn't block ERM on failure.

    The test is parametrized ("SELECT * ..." and "SELECT COUNT(*) ...") to verify both
    regular query execution and aggregate query that goes through mapreduce service.

    After an initial preparation, the test kills a node during the execution of
    a query with long timeout. The expectation is that:
      1) Due to scylladb#21831 fix, ERM will not be blocked for the timeout period.
      2) For non-mapreduce queries, due to scylladb#3699 fix, the query will not finish
         before the timeout passes.
      3) For mapreduce query, the query will fail promptly, because waiting with a hope for
         driver-side speculative retry (scylladb#3699 fix) is not needed for an aggregate query

    Please note that "SELECT COUNT(*) FROM {} WHERE key = 0" is not a mapreduce query.
    For non-ranged queries, Scylladb-side speculative retries are disabled to enforce
    conditions of scylladb#3699 fix.
    """
    logger.info("Start four nodes cluster, use large number of tablets to ensure distribution of reads")
    servers = await manager.servers_add(4, config={'failure_detector_timeout_in_ms': 2000})

    selected_server = servers[0]
    logger.info(f"Creating a client with selected_server: {selected_server}")
    cql = cluster_con([selected_server.ip_addr], 9042, False,
        load_balancing_policy=WhiteListRoundRobinPolicy([selected_server.ip_addr])).connect()

    logger.info("Create a table")
    random_tables = RandomTables(request.node.name, manager, "ks", replication_factor=3, enable_tablets=True)
    table = await random_tables.add_table(pks=1, columns=[
        Column(name="key", ctype=IntType),
        Column(name="value", ctype=IntType)
    ])

    if force_disable_speculative_retries:
        logger.info("Disabling speculative retries")
        cql.execute(f"ALTER TABLE {table} WITH speculative_retry = 'NONE'")

    logger.info(f"Created {table}, write some rows")
    await asyncio.gather(*[cql.run_async(f"INSERT INTO {table} (key, value) VALUES ({i}, {i})") for i in range(256)])

    logger.info("Enable injected errors that stop reads")
    injected_handlers = {}
    for server in servers:
        injected_handlers[server.ip_addr] = await inject_error_one_shot(
            manager.api, server.ip_addr, 'storage_proxy::handle_read', parameters={'cf_name': table.name})

    query_timeout = 60
    before_query = datetime.now()

    final_query = query.format(table)
    logger.info("Starting to execute query={final_query} with query_timeout={query_timeout}")
    query_future = cql.run_async(f"{final_query} USING TIMEOUT {query_timeout}s", timeout=2*query_timeout)

    logger.info("Confirm reads are waiting on the injected error")
    server_to_kill = None
    for server in servers:
        if server != selected_server:
            server_log = await manager.server_open_log(server.server_id)
            try:
                await server_log.wait_for("storage_proxy::handle_read injection hit", timeout=5)
                server_to_kill = server
            except TimeoutError:
                # Some nodes might not receive any reads
                logger.info(f"Node not handling any reads: {server.ip_addr}")

    logger.info(f"Kill a node: {server_to_kill.ip_addr}")
    await manager.server_stop(server_to_kill.server_id)

    logger.info("Unblock reads")
    for server in servers:
        if server != server_to_kill:
            await injected_handlers[server.ip_addr].message()

    logger.info("Remove the killed node, add a new one - ERM should not be blocked")
    await manager.server_not_sees_other_server(selected_server.ip_addr, server_to_kill.ip_addr, interval=10)
    await manager.remove_node(selected_server.server_id, server_to_kill.server_id, timeout=10)
    await manager.server_add(timeout=10)

    logger.info("Confirm the current time and wait for the query to finish")
    match = "Operation failed for"
    if should_wait_for_timeout:
        match = "Operation time"

    expected_timeout_time = before_query + timedelta(seconds=query_timeout)
    assert datetime.now() < expected_timeout_time

    with pytest.raises(Exception, match=match):
        await query_future

    if should_wait_for_timeout:
        assert datetime.now() > expected_timeout_time
    else:
        assert datetime.now() < expected_timeout_time


