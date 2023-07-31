#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables, Column, IntType, CounterType
from test.pylib.util import unique_name, wait_for_cql_and_get_hosts
from cassandra import WriteFailure

import pytest
import logging
import time


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_fence_writes(request, manager: ManagerClient):
    logger.info("Bootstrapping first two nodes")
    servers = [await manager.server_add(), await manager.server_add()]

    # The third node is started as the last one, so we can be sure that is has
    # the latest topology version
    logger.info("Bootstrapping the last node")
    servers += [await manager.server_add()]

    logger.info(f'Creating new tables')
    random_tables = RandomTables(request.node.name, manager, unique_name(), 3)
    table1 = await random_tables.add_table(name='t1', pks=1, columns=[
        Column("pk", IntType),
        Column('int_c', IntType)
    ])
    table2 = await random_tables.add_table(name='t2', pks=1, columns=[
        Column("pk", IntType),
        Column('counter_c', CounterType)
    ])
    await manager.cql.run_async(f"USE {random_tables.keyspace}")

    logger.info(f'Waiting for cql and hosts')
    host2 = (await wait_for_cql_and_get_hosts(manager.cql, [servers[2]], time.time() + 60))[0]

    version = (await manager.cql.run_async(
        "select version from system.topology where key = 'topology'",
        host=host2))[0].version
    logger.info(f"version on host2 {version}")

    await manager.cql.run_async(f"update system.topology set version={version - 1} where key = 'topology'",
                                host=host2)
    logger.info(f"decremented version on host2")

    await manager.server_restart(servers[2].server_id, wait_others=2)
    logger.info(f"host2 restarted")

    host2 = (await wait_for_cql_and_get_hosts(manager.cql, [servers[2]], time.time() + 60))[0]

    logger.info(f"trying to write through host2 to regular column [{host2}]")
    with pytest.raises(WriteFailure, match="stale topology exception"):
        await manager.cql.run_async("insert into t1(pk, int_c) values (1, 1)", host=host2)

    logger.info(f"trying to write through host2 to counter column [{host2}]")
    with pytest.raises(WriteFailure, match="stale topology exception"):
        await manager.cql.run_async("update t2 set counter_c=counter_c+1 where pk=1", host=host2)

    random_tables.drop_all()
