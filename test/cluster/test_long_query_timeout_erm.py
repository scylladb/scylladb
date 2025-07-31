# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
from cassandra.policies import WhiteListRoundRobinPolicy
from test.cluster.conftest import skip_mode
from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables, Column, IntType
from test.pylib.rest_client import inject_error_one_shot
from test.pylib.util import wait_for_cql_and_get_hosts

import asyncio
from datetime import datetime, timedelta
import pytest
import logging
import time

logger = logging.getLogger(__name__)
@skip_mode('release', 'error injections are not supported in release mode')
@pytest.mark.asyncio
@pytest.mark.parametrize("query_type,should_wait_for_timeout,shutdown_nodes", [
    ("SELECT", True, False),
    ("SELECT", True, True),
    ("SELECT_COUNT", False, False),
    ("SELECT_WHERE", True, False),
    ("SELECT_COUNT_WHERE", True, False),
])
async def test_long_query_timeout_erm(request, manager: ManagerClient, query_type, should_wait_for_timeout, shutdown_nodes):
    """
    Test verifies that a query with long timeout doesn't block ERM on failure.

    The test is parametrized ("SELECT * ..." and "SELECT COUNT(*) ...") to verify both
    regular query execution and aggregate query that goes through mapreduce service.

    After an initial preparation, the test kills a node during the execution of
    a query with long timeout. The expectation is that:
      1) Due to scylladb#21831 fix, ERM will not be blocked for the timeout period.
      2) For non-mapreduce queries, due to scylladb#3699 fix, the query will not finish
         before the timeout passes.
      3) For mapreduce query, the query will fail promptly with "Operation failed for ..." error,
         because waiting with a hope for driver-side speculative retry (scylladb#3699 fix)
         is not needed for an aggregate query.

    Please note that "SELECT COUNT(*) FROM {} WHERE key = 0" is not a mapreduce query.

    One of the test scenarios sets shutdown_nodes=True, to verify that we are able to
    quickly shutdown nodes, even if query timeout is extremely long.
    """

    if query_type == "SELECT":
        query = "SELECT * FROM {}"
    elif query_type == "SELECT_COUNT":
        query = "SELECT COUNT(*) FROM {}"
    elif query_type == "SELECT_WHERE":
        query = "SELECT * FROM {} WHERE key = 0"
    elif query_type == "SELECT_COUNT_WHERE":
        query = "SELECT COUNT(*) FROM {} WHERE key = 0"
    else:
        assert False # Invalid query type

    logger.info("Start four nodes cluster")
    # FIXME: Adjust this test to run with `rf_rack_valid_keyspaces` set to `True`.
    if should_wait_for_timeout and shutdown_nodes:
        # Overriding the `request_timeout_on_shutdown_in_seconds` for this test to avoid excessive delays, since after PR 24499
        # all queries (non-completed) are always waited for `request_timeout_on_shutdown_in_seconds` time.
        servers = await manager.servers_add(4, config={"rf_rack_valid_keyspaces": False, "request_timeout_on_shutdown_in_seconds": 30})
    else:
        servers = await manager.servers_add(4, config={"rf_rack_valid_keyspaces": False})

    selected_server = servers[0]
    logger.info(f"Creating a client with selected_server: {selected_server}")
    cql = await manager.get_cql_exclusive(selected_server)

    logger.info("Create a table")
    random_tables = RandomTables(request.node.name, manager, "ks", replication_factor=3, enable_tablets=True)
    table = await random_tables.add_table(pks=1, columns=[
        Column(name="key", ctype=IntType),
        Column(name="value", ctype=IntType)
    ])

    if "WHERE" in query:
        # For non-ranged queries, Scylladb-side speculative retries are disabled to enforce
        # conditions of scylladb#3699 fix.
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
    if shutdown_nodes:
        query_timeout *= 100 # Will end test by shutting down nodes instead of finishing the query
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
    await manager.server_not_sees_other_server(selected_server.ip_addr, server_to_kill.ip_addr)
    await manager.remove_node(selected_server.server_id, server_to_kill.server_id)

    expected_timeout_time = before_query + timedelta(seconds=query_timeout)
    if datetime.now() > expected_timeout_time:
        logger.warning("Too much time passed and query already might be timed-out")

    if shutdown_nodes:
        for server in servers:
            await manager.server_stop_gracefully(server.server_id)
    else:
        if should_wait_for_timeout:
            with pytest.raises(Exception, match="Operation time"):
                await query_future
            assert datetime.now() > expected_timeout_time
        else:
            with pytest.raises(Exception, match="Operation failed for"):
                await query_future

@skip_mode('release', 'error injections are not supported in release mode')
@pytest.mark.asyncio
@pytest.mark.parametrize("enable_tablets", [True, False])
async def test_long_query_timeout_without_failure_erm(request, manager: ManagerClient, enable_tablets):
    """
    Test verifies that a long mapreduce query does not block ERM.
    The query is long because it is blocked by an injected error
    that pauses reads during dispatching. When enable_tablet is False,
    old mapreduce algorithm blocks ERM, so topology changes are also blocked.
    When enable_tablets is True, the ERM is not blocked.
    """
    servers = await manager.servers_add(3)
    cql, hosts = await manager.get_ready_cql(servers)

    logger.info("Create a table with enable_tablets=%s", enable_tablets)
    random_tables = RandomTables(request.node.name, manager, "ks", replication_factor=1, enable_tablets=enable_tablets)
    table = await random_tables.add_table(pks=1, columns=[
        Column(name="key", ctype=IntType),
        Column(name="value", ctype=IntType)
    ])

    logger.info(f"Created {table}, write some rows")
    num_rows = 256
    await asyncio.gather(*[cql.run_async(f"INSERT INTO {table} (key, value) VALUES ({i}, {i})") for i in range(num_rows)])

    logger.info("Enable injected errors that stop read before storage_proxy takes ERM")
    injected_handlers = {}
    for server in servers:
        injected_handlers[server.ip_addr] = await inject_error_one_shot(
            manager.api, server.ip_addr, 'mapreduce_pause_parallel_dispatch')

    query = "SELECT count(*) FROM {}".format(table)
    query_timeout = 300
    logger.info("Starting to execute query={query} with query_timeout={query_timeout}")
    query_future = cql.run_async(f"{query} USING TIMEOUT {query_timeout}s", timeout=2*query_timeout)

    logger.info("Confirm reads are waiting on the injected error")

    async def wait_for_log_on_any_node(server):
        server_log = await manager.server_open_log(server.server_id)
        await server_log.wait_for("mapreduce_pause_parallel_dispatch: waiting for message")

    async with asyncio.TaskGroup() as tg:
        log_watch_tasks = [tg.create_task(wait_for_log_on_any_node(server)) for server in servers]
        _, pending = await asyncio.wait(log_watch_tasks, return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()

    if enable_tablets:
        logger.info("Add new node - ERM should not be blocked")
        await manager.server_add()

    logger.info("Unblock reads")
    for server in servers:
        await injected_handlers[server.ip_addr].message()

    res = await query_future
    assert res[0][0] == num_rows
