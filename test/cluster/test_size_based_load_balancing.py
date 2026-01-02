#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from test.pylib.manager_client import ManagerClient
from test.cluster.conftest import skip_mode
from test.cluster.util import new_test_keyspace
from collections import defaultdict
import pytest
import logging
import asyncio

logger = logging.getLogger(__name__)

GB = 1024 * 1024 * 1024

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_balance_empty_tablets(manager: ManagerClient):

    # This test checks that size-based load balancing migrates empty tablets of a newly created
    # table after a scale-out. The number of tablets on a node must be proportional to the disk
    # capacity of that node.

    logger.info('Bootstrapping cluster')

    cfg = { 'error_injections_at_startup': ['short_tablet_stats_refresh_interval'] }

    cfg_small = cfg | { 'data_file_capacity': 50 * GB }
    cfg_large = cfg | { 'data_file_capacity': 100 * GB }

    cmdline = [
        '--smp', '2',
        '--logger-log-level', 'load_balancer=debug',
        '--logger-log-level', 'raft_topology=debug',
    ]

    servers = [await manager.server_add(config=cfg_large, cmdline=cmdline, property_file={'dc': 'dc1', 'rack': 'r1'})]
    large_host_id = await manager.get_host_id(servers[0].server_id)

    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 16}") as ks:
        for table in ('t1', 't2', 't3'):
            await cql.run_async(f'CREATE TABLE {ks}.{table} (pk int PRIMARY KEY, val text);')

        servers.append(await manager.server_add(config=cfg_small, cmdline=cmdline, property_file={'dc': 'dc1', 'rack': 'r1'}))
        small_host_id = await manager.get_host_id(servers[1].server_id)
        logger.debug(f'Large node: {large_host_id}')
        logger.debug(f'Small node: {small_host_id}')

        s0_log = await manager.server_open_log(servers[0].server_id)
        s0_mark = await s0_log.mark()
        await s0_log.wait_for('Refreshed table load stats for all DC', from_mark=s0_mark)

        await manager.api.quiesce_topology(servers[0].ip_addr)

        replicas_per_node = defaultdict(int)
        tablets_per_shard = {}
        for row in await cql.run_async('SELECT * FROM system.tablets'):
            if row.keyspace_name == ks:
                table_id = row.table_id
                for r in row.replicas:
                    host_id = str(r[0])
                    shard = r[1]
                    replicas_per_node[host_id] += 1
                    if host_id not in tablets_per_shard:
                        tablets_per_shard[host_id] = {}
                    if shard not in tablets_per_shard[host_id]:
                        tablets_per_shard[host_id][shard] = defaultdict(int)

                    tablets_per_shard[host_id][shard][table_id] += 1

        # check replica distribution per node
        for host_id, tablets in replicas_per_node.items():
            logger.debug(f'Node: {host_id} tablet count: {tablets}')
            if host_id == large_host_id:
                assert tablets == 32, f'Larger node {host_id} must have 32 replicas'
            else:
                assert tablets == 16, f'Smaller node {host_id} must have 16 replicas'

        # check replica distribution per shard
        for host_id, shard_dist in tablets_per_shard.items():
            for shard, table_dist in shard_dist.items():
                shard_sum = sum(table_dist.values())
                if host_id == large_host_id:
                    assert shard_sum == 16, f'A shard of the larger host {host_id} must have 16 tablets'
                else:
                    assert shard_sum == 8, f'A shard of the smaller host {host_id} must have 8 tablets'

                for table_id, count in table_dist.items():
                    logger.debug(f'replica: {host_id}:{shard} of table_id: {table_id} tablet count: {count}')
                    if host_id == large_host_id:
                        assert count == 5 or count == 6, f'A shard of the larger host {host_id} must have either 5 or 6 tablets for table {table_id}'
                    else:
                        assert count == 2 or count == 3, f'A shard of the smaller host {host_id} must have either 2 or 3 tablets for table {table_id}'
