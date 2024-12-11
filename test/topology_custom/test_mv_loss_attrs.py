#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.pylib.manager_client import ManagerClient

import pytest
import time
import logging

from test.topology.conftest import skip_mode
from test.pylib.util import wait_for_view

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_mv_loss_attrs_exception(manager: ManagerClient) -> None:
    node_count = 2
    config = {'error_injections_at_startup': ['never_finish_remote_view_updates'], 'enable_tablets': True}
    servers = await manager.servers_add(node_count, config=config)
    cql, _ = await manager.get_ready_cql(servers)
    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}"
                         "AND tablets = {'initial': 1}")
    await cql.run_async(f"CREATE TABLE ks.tab (base_key int, view_key int, v text, PRIMARY KEY (base_key, view_key))"
                         "WITH gc_grace_seconds = 10")
    await cql.run_async(f"CREATE MATERIALIZED VIEW ks.mv_cf_view AS SELECT * FROM ks.tab "
                    "WHERE view_key IS NOT NULL and base_key IS NOT NULL PRIMARY KEY (view_key, base_key) ")

    await wait_for_view(cql, 'mv_cf_view', node_count)
    for v in [1000]:
        # Simulate Alternator to perform the operation of deleting first and then writing
        await cql.run_async(f"DELETE FROM ks.tab WHERE pk = {v} AND sk = {v};")
        await cql.run_async(f"INSERT INTO ks.tab (base_key, view_key, v) VALUES ({v}, {v}, '{v*'a'}')")

    await manager.api.keyspace_flush(servers[0].ip_addr, 'ks')

    # waite for gc_grace_seconds
    time.sleep(15)

    # when compacted, the row inlcue liveness_info and deletion_info
    await manager.api.keyspace_compaction(servers[0].ip_addr, 'ks', 'tab')

    # restart to avoid read cache
    await manager.server_restart(servers[0].ip_addr)

    # repair base table will generate MV data
    await manager.api.repair(servers[0].ip_addr, 'ks', 'tab')

    # query the MV will find MV loss v
    for v in [1000]:
        assert [(None,)] == list(cql.execute(f"SELECT v FROM ks.mv_cf_view WHERE pk = {v} AND sk = {v};"))

    await cql.run_async(f"DROP KEYSPACE ks")

