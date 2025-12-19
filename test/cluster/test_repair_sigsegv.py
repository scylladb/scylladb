#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from cassandra.query import ConsistencyLevel
from test.pylib.manager_client import ManagerClient
from test.cluster.util import reconnect_driver
from test.cluster.util import new_test_keyspace
import asyncio
import logging
import pytest

logger = logging.getLogger(__name__)

# This is a reproducer for memory corruption issue https://github.com/scylladb/scylladb/issues/27666.
# The problem happened because lw_shared_ptr was copied into another cpu, into streaming consumer,
# and it caused use-after-free, corrupting the heap.
@pytest.mark.asyncio
async def test_repair_sigsegv(manager: ManagerClient):
    logger.info('Bootstrapping cluster')
    cfg = { 'force_gossip_topology_changes': True,
            'tablets_mode_for_new_keyspaces': 'disabled' }
    cmdline_s0 = [
        '--smp', '2',
    ]
    cmdline_s1 = [
        '--smp', '3',
    ]
    servers = await manager.servers_add(1, cmdline=cmdline_s0, config=cfg, auto_rack_dc="dc1")
    servers.append(await manager.server_add(cmdline=cmdline_s1, config=cfg, property_file={'dc': 'dc1', 'rack': 'rack2'}))

    cql = manager.get_cql()

    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 2}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        async def write_with_cl_one(range_start, range_end):
            insert_stmt = cql.prepare(f"INSERT INTO {ks}.test (pk, c) VALUES (?, ?)")
            insert_stmt.consistency_level = ConsistencyLevel.ONE
            pks = range(range_start, range_end)
            await asyncio.gather(*[cql.run_async(insert_stmt, (k, k)) for k in pks])

        logger.info("Adding data only on first node")
        await manager.api.flush_keyspace(servers[1].ip_addr, ks)
        await manager.server_stop(servers[1].server_id)
        manager.driver_close()
        cql = await reconnect_driver(manager)
        await write_with_cl_one(0, 1000)
        await manager.api.flush_keyspace(servers[0].ip_addr, ks)

        logger.info("Adding data only on second node")
        await manager.server_start(servers[1].server_id)
        await manager.api.flush_keyspace(servers[0].ip_addr, ks)
        await manager.server_stop(servers[0].server_id)
        manager.driver_close()
        cql = await reconnect_driver(manager)
        await write_with_cl_one(1000, 2000)

        await manager.server_start(servers[0].server_id)
        await manager.servers_see_each_other(servers)

        s0_log = await manager.server_open_log(servers[0].server_id)
        s0_mark = await s0_log.mark()

        await s0_log.wait_for('Skipping due to empty group0', from_mark=s0_mark)

        logger.info("Starting repair")
        await manager.api.repair(servers[1].ip_addr, ks, "test")
