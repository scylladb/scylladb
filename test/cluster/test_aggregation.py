#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import asyncio
import pytest
import time
import logging
import random

from cassandra.cluster import NoHostAvailable, OperationTimedOut  # type: ignore

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts
from test.cluster.conftest import skip_mode
from test.cluster.util import new_test_keyspace, new_test_table

logger = logging.getLogger(__name__)
pytestmark = pytest.mark.prepare_3_racks_cluster


@pytest.mark.asyncio
@skip_mode("release", "error injections are not supported in release mode")
@pytest.mark.parametrize("stop_node", ['sender', 'receiver'])
async def test_cancel_mapreduce(manager: ManagerClient, stop_node):
    """
    Reproduces: scylladb/scylladb#22337
    This test verifies that stopping a node which is a supercoordinator of mapreduce request (sender)
    or is executing a mapreduce subquery (receiver) cancels
    outgoing operations, which would otherwise prevent the shutdown.
    """

    running_servers = await manager.running_servers()
    assert len(running_servers) >= 2

    s1, s2 = running_servers[0], running_servers[1]
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, [s1, s2], time.time() + 30)

    await manager.api.set_logger_level(s1.ip_addr, "forward_service", "debug")

    [host1] = filter(lambda host: host.address == s1.ip_addr, hosts)
    host_id2 = await manager.get_host_id(s2.server_id)

    async with new_test_keyspace(manager, "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}") as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, v int") as t:
            # Distribute data across the nodes.
            for _ in range(250):
                # Note: CQL int is a 32-bit integer.
                pk = random.randint(-2**30, 2**30)
                v = random.randint(-2**30, 2**30)
                await cql.run_async(f"INSERT INTO {t} (pk, v) VALUES ({pk}, {v})")

            s1_log = await manager.server_open_log(s1.server_id)
            s2_log = await manager.server_open_log(s2.server_id)

            s1_mark = await s1_log.mark()
            s2_mark = await s2_log.mark()

            # Prevent finishing local mapreduce tasks on node 2.
            await manager.api.enable_injection(s2.ip_addr, "mapreduce_pause_dispatch_to_shards", one_shot=False)

            async def do_select():
                # Make node 1 the supercoordinator of the mapreduce task corresponding to aggregation.
                # We use this timeout because it's longer than the cumulative timeout of the following
                # steps. For the test to be reliable, the query cannot end on its own.
                try:
                    await cql.run_async(f"SELECT count(*) FROM {t} BYPASS CACHE USING TIMEOUT 600s", host=host1)
                    pytest.fail(f"Query finished, but it wasn't supposed to")
                except NoHostAvailable:
                    pass
                except OperationTimedOut:
                    pass

            async def wait_and_shutdown():
                # Make sure node 1 is the supercoordinator and sends a mapreduce task to node 2.
                await s1_log.wait_for(f"dispatching mapreduce_request=.* to address={host_id2}", from_mark=s1_mark, timeout=60)
                # Make sure that node 2 is preventing its local mapreduce task from finishing.
                await s2_log.wait_for("mapreduce_pause_dispatch_to_shards: waiting for message", from_mark=s2_mark, timeout=60)
                
                # Verify that the designated node stops without an issue despite the ongoing mapreduce task.
                if stop_node == 'sender':
                    await manager.server_stop_gracefully(s1.server_id, timeout=120)
                else:
                    stop_mark = await s2_log.mark()
                    loop = asyncio.get_event_loop()
                    shutdown_task = loop.create_task(manager.server_stop_gracefully(s2.server_id, timeout=120))
                    await s2_log.wait_for("Signal received; shutting down", from_mark=stop_mark, timeout=60)
                    await manager.api.message_injection(s2.ip_addr, 'mapreduce_pause_dispatch_to_shards')
                    await shutdown_task

            async with asyncio.TaskGroup() as tg:
                _ = tg.create_task(do_select())
                _ = tg.create_task(wait_and_shutdown())
