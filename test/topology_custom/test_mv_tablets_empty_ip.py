
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

from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.manager_client import ManagerClient
from test.topology.conftest import skip_mode


logger = logging.getLogger(__name__)

# Regression test for https://github.com/scylladb/scylladb/issues/19439.
# Creates a tabled-enabled keyspace with a base table and a materialized view,
# continuously writes to it and replaces one of the nodes. During replacement,
# nodes should not crash.
#
# RF needs to be smaller than the cluster size in order ensure appearance of
# remote view updates.
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_mv_tablets_empty_ip(manager: ManagerClient):
    cfg = {'enable_tablets': True}
    servers = await manager.servers_add(4, config = cfg)

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}")
    await cql.run_async("CREATE TABLE ks.t (pk int primary key, v int)")
    await cql.run_async("CREATE materialized view ks.t_view AS select pk, v from ks.t where v is not null primary key (v, pk)")

    stop_event = asyncio.Event()
    concurrency = 10
    async def do_writes(start_it) -> int:
        iteration = start_it
        while not stop_event.is_set():
            start_time = time.time()
            try:
                await cql.run_async(f"insert into ks.t (pk, v) values ({iteration}, {iteration+1})")
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
            await asyncio.sleep(0.01)
        return iteration

    logger.info("Starting to write")
    tasks = [asyncio.create_task(do_writes(i)) for i in range(concurrency)]

    logger.info("Stopping the last node")
    await manager.server_stop_gracefully(servers[-1].server_id)
    replace_cfg = ReplaceConfig(replaced_id = servers[-1].server_id, reuse_ip_addr = False, use_host_id = True)

    logger.info("Replacing the last node")
    await manager.server_add(replace_cfg=replace_cfg, config = cfg)

    logger.info("Stopping writes")
    stop_event.set()
    await asyncio.gather(*tasks)
