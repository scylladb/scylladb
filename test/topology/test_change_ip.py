#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test clusters can restart fine after an IP address change.
"""

import logging
import pytest
from test.pylib.util import wait_for_cql_and_get_hosts, wait_for
from test.pylib.random_tables import Column, IntType, TextType
import time
logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_change_two(manager, random_tables):
    """Stop two nodes, change their IPs and start, check the cluster is
    functional"""
    servers = await manager.running_servers()
    table = await random_tables.add_table(name='t1', pks=1, columns=[
        Column("pk", IntType),
        Column('int_c', IntType)
    ])
    s_1 = servers[1].server_id
    s_2 = servers[2].server_id
    logger.info("Gracefully stopping servers %s and %s to change ips", s_1, s_2)
    await manager.server_stop_gracefully(s_1)
    await manager.server_stop_gracefully(s_2)
    await manager.server_change_ip(s_1)
    await manager.server_change_ip(s_2)
    await manager.server_start(s_1)
    await manager.server_start(s_2)

    manager.driver_close()
    await manager.driver_connect()
    cql = manager.get_cql()
    servers = await manager.running_servers()

    await table.add_column(column=Column("str_c", TextType))
    await random_tables.verify_schema()

    host0 = (await wait_for_cql_and_get_hosts(cql, [servers[0]], time.time() + 60))[0]
    all_ips = set([s.rpc_address for s in servers])
    logger.info(f"waiting for {servers[0]} to see its peers, all ips {all_ips}")
    async def see_peers():
        peers = set()
        for r in await cql.run_async("select peer from system.peers", host=host0):
            peers.add(r.peer)
        remaining = all_ips - {servers[0].rpc_address} - peers
        if not remaining:
            return True
        logger.info(f"waiting for {host0} to see its peers, all_ips {all_ips}, peers {peers}, remaining {remaining}")
    await wait_for(see_peers, time.time() + 60)

    await cql.run_async(f"USE {random_tables.keyspace}")
    await cql.run_async("insert into t1(pk, int_c, str_c) values (1, 2, 'test-val')", host=host0)
    rows = list(await cql.run_async("select * from t1", host=host0))
    assert len(rows) == 1
    row = rows[0]
    assert row.pk == 1
    assert row.int_c == 2
    assert row.str_c == 'test-val'
