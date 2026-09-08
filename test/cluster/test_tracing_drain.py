#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
import time

import pytest
from cassandra import ConsistencyLevel  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.rest_client import inject_error
from test.pylib.util import wait_for_cql_and_get_hosts
from test.cluster.util import new_test_keyspace, new_test_table, reconnect_driver

logger = logging.getLogger(__name__)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_traced_read_racing_with_drain(manager: ScyllaClusterManager) -> None:
    """
    Reproduce the interleaving where a traced read lands on a replica that is
    being drained. The replica-side read verb handler is paused by error
    injection right before it clones the tracing session from the received
    trace_info, then drain is started on the replica, and only then the
    handler is released. The replica must not crash and the drain must run to
    completion: messaging service shutdown waits for the in-flight verb
    handler and tracing is shut down only after that, so the session is
    cloned on a still-running tracing service.
    """
    coord, replica = await manager.servers_add(2, auto_rack_dc="dc1")
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, [coord, replica], time.time() + 30)
    [coord_host] = [h for h in hosts if h.address == coord.ip_addr]

    async with new_test_keyspace(manager, "WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, v int") as t:
            await cql.run_async(f"INSERT INTO {t} (pk, v) VALUES (1, 1)")

            replica_log = await manager.server_open_log(replica.server_id)
            replica_mark = await replica_log.mark()

            async with inject_error(manager.api, replica.ip_addr, "storage_proxy_pause_traced_read"):
                async def do_traced_read():
                    # CL=ALL makes the coordinator read from the replica too.
                    # The read is allowed to fail and its outcome doesn't
                    # matter: once the replica-side handler is paused, it
                    # stays paused regardless of the coordinator timing out,
                    # so the timeout is short not to delay the test. The
                    # point is that the replica survives the whole exercise.
                    stmt = SimpleStatement(f"SELECT * FROM {t} WHERE pk = 1 USING TIMEOUT 10s",
                                           consistency_level=ConsistencyLevel.ALL)
                    try:
                        await cql.run_async(stmt, host=coord_host, trace=True, timeout=60)
                        logger.info("Traced read completed")
                    except Exception as e:
                        logger.info(f"Traced read failed (which is allowed): {e}")

                async def drain_replica():
                    # Wait for the replica-side read handler to pause right
                    # before creating the tracing session.
                    await manager.api.wait_for_injection_enter(replica.ip_addr, "storage_proxy_pause_traced_read")
                    # Drain the replica. The drain will block shutting down
                    # the messaging service until the paused handler is done.
                    drain = asyncio.create_task(manager.api.drain(replica.ip_addr))
                    await replica_log.wait_for("Stop transport: starts", from_mark=replica_mark, timeout=60)
                    # Release the handler -- it creates the tracing session
                    # on the draining node now.
                    await manager.api.message_injection(replica.ip_addr, "storage_proxy_pause_traced_read")
                    await drain

                async with asyncio.TaskGroup() as tg:
                    tg.create_task(do_traced_read())
                    tg.create_task(drain_replica())

            # The drain must have gone all the way through tracing shutdown
            await replica_log.wait_for("Tracing is down", from_mark=replica_mark, timeout=60)

            # Bring the replica back so that the cluster is fully functional
            # and the keyspace can be dropped (DDL needs group0 majority)
            await manager.server_stop_gracefully(replica.server_id)
            await manager.server_start(replica.server_id)
            cql = await reconnect_driver(manager)
            await wait_for_cql_and_get_hosts(cql, [coord, replica], time.time() + 30)
