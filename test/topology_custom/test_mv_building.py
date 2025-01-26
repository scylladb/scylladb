#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import asyncio
import pytest
import logging
from test.pylib.manager_client import ManagerClient
from test.pylib.tablets import get_tablet_replica
from test.pylib.util import unique_name, wait_for_view

logger = logging.getLogger(__name__)


# Build multiple views of one base table, and while view building is running move
# some of the base tablets to another node. Verify the view build is completed.
# More specifically, we move all tablets except the first one to reproduce issue #21829.
# The issue happens when we start building a view at a token F and then all partitions
# with tokens >=F are moved, and it causes the view builder to enter an infinite loop
# building the same token ranges repeatedly because it doesn't reach F.
@pytest.mark.asyncio
async def test_view_building_with_tablet_move(manager: ManagerClient, mode: str):
    cfg = {'enable_tablets': True}
    servers = [await manager.server_add(config=cfg)]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    ks = unique_name()
    table = 'test'

    view_count = 4
    views = [f"{table}_view_{i}" for i in range(view_count)]

    cql = manager.get_cql()
    await cql.run_async(f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'initial': 4}}")
    await cql.run_async(f"CREATE TABLE {ks}.{table} (pk int PRIMARY KEY, c int)")

    # prefill the base table with enough rows so that view building takes some time
    # and runs during the tablet move
    keys = 200000 if mode != 'debug' else 10000
    batch_size = 50
    for k in range(0, keys, batch_size):
        inserts = [f"INSERT INTO {ks}.{table}(pk, c) VALUES ({i}, {i})" for i in range(k, k+batch_size)]
        batch = "BEGIN UNLOGGED BATCH\n" + "\n".join(inserts) + "\nAPPLY BATCH\n"
        await manager.cql.run_async(batch)

    logger.info("Adding new server")
    servers.append(await manager.server_add(config=cfg))

    # create some views so they are built together but starting at different tokens
    for view in views:
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.{view} AS SELECT * FROM {ks}.{table} WHERE c IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (c, pk)")
        await asyncio.sleep(1)

    s0_host_id = await manager.get_host_id(servers[0].server_id)
    s1_host_id = await manager.get_host_id(servers[1].server_id)
    dst_shard = 0

    # move all tablets except the first one (with lowest token range) to the other node.
    table_id = await manager.get_table_id(ks, table)
    rows = await manager.cql.run_async(f"SELECT last_token FROM system.tablets where table_id = {table_id}")
    move_tablets_tasks = []
    for r in rows[1:]:
        tablet_token = r.last_token
        replica = await get_tablet_replica(manager, servers[0], ks, table, tablet_token)
        move_tablets_tasks.append(asyncio.create_task(manager.api.move_tablet(servers[0].ip_addr, ks, table, replica[0], replica[1], s1_host_id, dst_shard, tablet_token)))
    await asyncio.gather(*move_tablets_tasks)

    for view in views:
        await wait_for_view(cql, view, len(servers))
