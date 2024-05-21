#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import asyncio
import pytest
import time
import logging

from cassandra.cluster import ConnectionException, NoHostAvailable  # type: ignore

from test.pylib.manager_client import ManagerClient
from test.topology.conftest import skip_mode


logger = logging.getLogger(__name__)

# This test reproduces issues #17786 and #18709
# In the test, we create a keyspace with a table and a materialized view.
# We then start writing to the table, causing the materialized view to be updated.
# While the writes are in progress, we add then decommission a node in the cluster.
# The test verifies that no node crashes as a result of the topology change combined
# with the writes.
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_mv_topology_change(manager: ManagerClient):
    cfg = {'force_gossip_topology_changes': True, 'error_injections_at_startup': ['delay_before_get_view_natural_endpoint']}

    servers = [await manager.server_add(config=cfg, timeout=60) for _ in range(3)]

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3};")
    await cql.run_async("CREATE TABLE ks.t (pk int primary key, v int)")
    await cql.run_async("CREATE materialized view ks.t_view AS select pk, v from ks.t where v is not null primary key (v, pk)")

    stop_event = asyncio.Event()
    concurrency = 10
    async def do_writes(start_it, repeat) -> int:
        iteration = start_it
        while not stop_event.is_set():
            start_time = time.time()
            try:
                await cql.run_async(f"insert into ks.t (pk, v) values ({iteration}, {iteration})")
            except NoHostAvailable as e:
                for _, err in e.errors.items():
                    # ConnectionException can be raised when the node is shutting down.
                    if not isinstance(err, ConnectionException):
                        logger.error(f"Write started {time.time() - start_time}s ago failed: {e}")
                        raise
            except Exception as e:
                logger.error(f"Write started {time.time() - start_time}s ago failed: {e}")
                raise
            iteration += concurrency
            if not repeat:
                break
            await asyncio.sleep(0.01)
        return iteration


    # to hit the issue #18709 it's enough to start one batch of writes, the effective
    # replication maps for base and view will change after the writes start but before they finish
    tasks = [asyncio.create_task(do_writes(i, repeat=False)) for i in range(concurrency)]

    server = await manager.server_add()

    await asyncio.gather(*tasks)

    [await manager.api.disable_injection(s.ip_addr, "delay_before_get_view_natural_endpoint") for s in servers]
    [await manager.api.enable_injection(s.ip_addr, "delay_after_erm_update", False, parameters={'ks_name': 'ks', 'cf_name': 't'}) for s in servers]

    # to hit the issue #17786 we need to run multiple batches of writes, so that some write is processed while the 
    # effective replication maps for base and view are different
    tasks = [asyncio.create_task(do_writes(i, repeat=True)) for i in range(concurrency)]
    await manager.decommission_node(server.server_id)

    stop_event.set()
    await asyncio.gather(*tasks)

