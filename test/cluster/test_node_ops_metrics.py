#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import asyncio
import pytest
import logging

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_bootstrap_removenode_metrics(manager):
    cfg = {'enable_repair_based_node_ops': True}
    servers = [await manager.server_add(config=cfg),
               await manager.server_add(config=cfg),
               await manager.server_add(config=cfg)]
    await manager.server_stop_gracefully(servers[2].server_id)
    await manager.remove_node(servers[0].server_id, servers[2].server_id)

    def check_ops(metrics, ops):
        metric_name = "scylla_node_ops_finished_percentage"
        shard = 0
        while True:
            cnt = metrics.get(name=metric_name, labels={'ops': ops}, shard=str(shard))
            if cnt == None:
                break
            logger.info(f"Checking {shard=} {cnt=}")
            assert int(cnt) == 1
            shard = shard + 1

    for s in servers[:2]:
        metrics = await manager.metrics.query(s.ip_addr)
        check_ops(metrics, 'bootstrap')
        check_ops(metrics, 'removenode')
