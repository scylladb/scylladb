#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import asyncio
import logging
import pytest
import time
from cassandra.cluster import ConsistencyLevel

from test.cluster.dtest.alternator_utils import random_string
from test.cluster.util import new_test_keyspace
from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_streaming_deadlock_removenode(request, manager: ManagerClient):
    # Force removenode to exercise range_streamer and not repair.
    # The bug is in the streaming, and when senders are on different nodes,
    # and receivers are cross-located (B->C, C->B).
    cfg = {
        'rf_rack_valid_keyspaces': False,
        'tablets_mode_for_new_keyspaces': 'disabled',
        'maintenance_reader_concurrency_semaphore_count_limit': 1,
        'enable_repair_based_node_ops': False,
        'enable_cache': False, # Force IO
    }

    cmdline = [
        '--logger-log-level', 'stream_session=trace',
        '--logger-log-level', 'query_processor=trace'
    ]

    servers = await manager.servers_add(3, config=cfg, cmdline=cmdline)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int, v text);")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv AS SELECT * FROM {ks}.test "
                            "WHERE c IS NOT NULL and pk IS NOT NULL PRIMARY KEY (c, pk)")

        keys = range(10240)
        val = random_string(10240)
        stmt = cql.prepare(f"INSERT INTO {ks}.test (pk, c, v) VALUES (?, ?, '{val}')")
        await asyncio.gather(*[cql.run_async(stmt, [k, k]) for k in keys])

        await manager.server_stop_gracefully(servers[0].server_id)
        await manager.remove_node(servers[1].server_id, servers[0].server_id)
