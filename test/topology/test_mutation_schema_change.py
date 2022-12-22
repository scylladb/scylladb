#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Reproducer for a failure during lwt operation due to missing of a column mapping in schema history table.
"""
import asyncio
import logging
import time
from functools import partial
from test.pylib.rest_client import inject_error_one_shot, inject_error
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts
from test.pylib.manager_client import ManagerClient
from test.pylib.internal_types import IPAddress, ServerInfo
import pytest
from cassandra.cluster import Cluster, ConsistencyLevel  # type: ignore # pylint: disable=no-name-in-module
from cassandra.query import SimpleStatement              # type: ignore # pylint: disable=no-name-in-module


logger = logging.getLogger(__name__)


async def server_sees_another_server(server: ServerInfo, manager: ManagerClient):
    alive_nodes = await manager.api.get_alive_endpoints(server.ip_addr)
    if len(alive_nodes) > 1:
        return True

@pytest.mark.asyncio
async def test_mutation_schema_change(manager, random_tables):
    """
        Cluster A, B, C
        create table
        stop C
        change schema + do lwt write + change schema
        stop B
        start C
        do lwt write to the same key through C
    """
    server_a, server_b, server_c = await manager.running_servers()
    t = await random_tables.add_table(ncolumns=5, pks=1)
    manager.driver_close()
    # Reduce the snapshot thresholds
    await manager.mark_dirty()
    errs = [inject_error_one_shot(manager.api, s.ip_addr, 'raft_server_snapshot_reduce_threshold')
            for s in [server_a, server_b, server_c]]
    await asyncio.gather(*errs)


    logger.info("Stopping C %s", server_c)
    await manager.server_stop_gracefully(server_c.server_id)
    await manager.driver_connect()

    async with inject_error(manager.api, server_b.ip_addr, 'paxos_error_before_learn'):
        await t.add_column()
        ROWS = 1
        seeds = [t.next_seq() for _ in range(ROWS)]
        stmt = f"INSERT INTO {t} ({','.join(c.name for c in t.columns)}) " \
               f"VALUES ({', '.join(['%s'] * len(t.columns))}) "           \
               f"IF NOT EXISTS"
        query = SimpleStatement(stmt, consistency_level=ConsistencyLevel.ONE)
        for seed in seeds:
            logger.info("INSERT row seed %s", seed)
            await manager.cql.run_async(query, parameters=[c.val(seed) for c in t.columns])
        await t.add_column()

    logger.info("Stopping B %s", server_b)
    await manager.server_stop_gracefully(server_b.server_id)
    logger.info("Starting C %s", server_c)
    await manager.server_start(server_c.server_id)

    await wait_for(partial(server_sees_another_server, server_c, manager), time.time() + 45, period=.1)

    logger.info("Driver connecting to C %s", server_c)
    await manager.driver_connect(server=server_c)
    await wait_for_cql_and_get_hosts(manager.cql, [server_a, server_c], time.time() + 60)

    stmt = f"UPDATE {t} "                        \
           f"SET   {t.columns[3].name} = %s "  \
           f"WHERE {t.columns[0].name} = %s "  \
           f"IF    {t.columns[3].name} = %s"
    query = SimpleStatement(stmt, consistency_level=ConsistencyLevel.ONE)
    for seed in seeds:
        logger.info("UPDATE with seed %s", seed)
        await manager.cql.run_async(query, parameters=[t.columns[3].val(seed + 1), # v_01 = seed + 1
                                                       t.columns[0].val(seed),     # pk = seed
                                                       t.columns[3].val(seed)],    # v_01 == seed
                                    execution_profile='whitelist')


@pytest.mark.asyncio
async def test_mutation_schema_change_restart(manager, random_tables):
    """
        Cluster A, B, C
        create table
        stop C
        change schema + do lwt write + change schema
        stop B
        restart A
        start C
        do lwt write to the same key through A
    """
    server_a, server_b, server_c = await manager.running_servers()
    t = await random_tables.add_table(ncolumns=5, pks=1)
    manager.driver_close()
    # Reduce the snapshot thresholds
    await manager.mark_dirty()
    errs = [inject_error_one_shot(manager.api, s.ip_addr, 'raft_server_snapshot_reduce_threshold')
            for s in [server_a, server_b, server_c]]
    await asyncio.gather(*errs)

    logger.info("Stopping C %s", server_c)
    await manager.server_stop_gracefully(server_c.server_id)
    await manager.driver_connect()

    await inject_error_one_shot(manager.api, server_a.ip_addr,
                                'raft_server_reduce_threshold')
    async with inject_error(manager.api, server_b.ip_addr, 'paxos_error_before_learn'):
        await t.add_column()
        ROWS = 1
        seeds = [t.next_seq() for _ in range(ROWS)]
        stmt = f"INSERT INTO {t} ({','.join(c.name for c in t.columns)}) " \
               f"VALUES ({', '.join(['%s'] * len(t.columns))}) "           \
               f"IF NOT EXISTS"
        query = SimpleStatement(stmt, consistency_level=ConsistencyLevel.ONE)
        for seed in seeds:
            logger.info("INSERT row seed %s", seed)
            await manager.cql.run_async(query, parameters=[c.val(seed) for c in t.columns])
        await t.add_column()

    manager.driver_close()

    logger.info("Stopping B %s", server_b)
    await manager.server_stop_gracefully(server_b.server_id)
    logger.info("Restarting A %s", server_a)
    await manager.server_restart(server_a.server_id)
    logger.info("Starting C %s", server_c)
    await manager.server_start(server_c.server_id)

    # Wait for C and A to see each other
    await wait_for(partial(server_sees_another_server, server_c, manager), time.time() + 45, period=.1)
    await wait_for(partial(server_sees_another_server, server_a, manager), time.time() + 45, period=.1)

    logger.info("Driver connecting to A %s", server_a)
    await manager.driver_connect(server=server_a)

    await wait_for_cql_and_get_hosts(manager.cql, [server_a, server_c], time.time() + 60)
    stmt = f"UPDATE {t} "                        \
           f"SET   {t.columns[3].name} = %s "  \
           f"WHERE {t.columns[0].name} = %s "  \
           f"IF    {t.columns[3].name} = %s"
    query = SimpleStatement(stmt, consistency_level=ConsistencyLevel.ONE)
    for seed in seeds:
        logger.info("UPDATE with seed %s", seed)
        await manager.cql.run_async(query, parameters=[t.columns[3].val(seed + 1), # v_01 = seed + 1
                                                       t.columns[0].val(seed),     # pk = seed
                                                       t.columns[3].val(seed)],    # v_01 == seed
                                    execution_profile='whitelist')
