#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
import time

from cassandra.query import ConsistencyLevel

from test.pylib.manager_client import ManagerClient
from test.pylib.repair import create_table_insert_data_for_repair
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)


async def repair_rounds_metrics(manager: ManagerClient, servers) -> tuple[int, int, int]:
    """Sum the repair round counters over all nodes and shards."""
    rounds = in_sync = out_of_sync = 0
    for server in servers:
        metrics = await manager.metrics.query(server.ip_addr)
        rounds += metrics.get("scylla_repair_rounds_total") or 0
        in_sync += metrics.get("scylla_repair_rounds_in_sync_total") or 0
        out_of_sync += metrics.get("scylla_repair_rounds_out_of_sync_total") or 0
    return int(rounds), int(in_sync), int(out_of_sync)


async def test_repair_rounds_metrics(manager: ManagerClient):
    """Test that repair reports whether it actually found inconsistencies.

    A repair over diverged replicas must increase rounds_out_of_sync_total, while
    a second repair over the now-synced replicas must only increase
    rounds_in_sync_total.
    """
    nr_keys = 128
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(
        manager, nr_keys=nr_keys, cmdline=["--hinted-handoff-enabled", "0"])

    # Make the replicas diverge: write new rows while one node is down.
    await manager.server_stop_gracefully(servers[2].server_id)
    insert_stmt = cql.prepare(f"INSERT INTO {ks}.test (pk, c) VALUES (?, ?)")
    insert_stmt.consistency_level = ConsistencyLevel.ONE
    await asyncio.gather(*[cql.run_async(insert_stmt, (k, k)) for k in range(nr_keys, 2 * nr_keys)])
    await manager.server_start(servers[2].server_id, wait_others=2)
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    rounds_before, in_sync_before, out_of_sync_before = await repair_rounds_metrics(manager, servers)

    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", "all")

    rounds_after, in_sync_after, out_of_sync_after = await repair_rounds_metrics(manager, servers)
    logger.info(f"First repair round deltas: rounds={rounds_after - rounds_before} "
                f"in_sync={in_sync_after - in_sync_before} out_of_sync={out_of_sync_after - out_of_sync_before}")
    assert rounds_after > rounds_before
    assert out_of_sync_after > out_of_sync_before

    # The replicas are in sync now, so a second repair must not find anything to fix.
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", "all")

    rounds_final, in_sync_final, out_of_sync_final = await repair_rounds_metrics(manager, servers)
    logger.info(f"Second repair round deltas: rounds={rounds_final - rounds_after} "
                f"in_sync={in_sync_final - in_sync_after} out_of_sync={out_of_sync_final - out_of_sync_after}")
    assert rounds_final > rounds_after
    assert in_sync_final > in_sync_after
    assert out_of_sync_final == out_of_sync_after
