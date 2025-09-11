#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import logging
import random

import pytest
from test.cluster.conftest import skip_mode
from test.cluster.lwt.lwt_common import (
    BaseLWTTester,
    get_token_for_pk,
    get_host_map,
    pick_non_replica_server,
    DEFAULT_WORKERS,
    DEFAULT_NUM_KEYS,
)
from test.cluster.util import new_test_keyspace
from test.pylib.manager_client import ManagerClient
from test.pylib.tablets import get_tablet_replicas

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Test constants
WORKLOAD_SEC = 30


async def continuous_tablet_migrations(stop_event: asyncio.Event,
        manager: ManagerClient, servers, tester, duration_sec: int, pause_range=(0.5, 2.0)
        ):

    """
    Continuously migrate random tablets between hosts/shards for duration_sec.
    """
    logger.info("Starting continuous tablet migrations for %s seconds", duration_sec)
    start = asyncio.get_event_loop().time()
    migration_count = 0

    host_map = await get_host_map(manager, servers)
    while not stop_event.is_set() and asyncio.get_event_loop().time() - start < duration_sec:

        sample_pk = random.choice(tester.pks)
        token = tester.pk_to_token[sample_pk]

        # pick any server as the query endpoint for replicas list
        replicas = await get_tablet_replicas(manager, servers[0], tester.ks, tester.tbl, token)
        if random.random() < 0.3:
            # Intranode migration (same host, different shard)
            src_host_id, src_shard = random.choice(replicas)
            src_server = host_map.get(src_host_id)

            # Choose a different shard on the same node
            dst_hid = src_host_id
            dst_shard = 1 if src_shard == 0 else 0
            dst_server = src_server

            logger.info(
                "Attempting intranode migration: token=%s, host=%s, shard %d -> %d",
                token,
                src_server.ip_addr,
                src_shard,
                dst_shard,
            )
        else:
            # Internode migration (move to a non-replica host)
            src_host_id, src_shard = random.choice(replicas)
            src_server = host_map.get(src_host_id)

            replica_hids = {h for (h, _shard) in replicas}
            dst_server = await pick_non_replica_server(manager, servers, replica_hids)

            dst_hid = await manager.get_host_id(dst_server.server_id)
            # pick shard 0 on the destination by default for internode case
            dst_shard = 0

        await manager.api.move_tablet(src_server.ip_addr, tester.ks, tester.tbl, src_host_id, src_shard, dst_hid, dst_shard, token)
        migration_count += 1
        logger.info(
            "Completed migration #%d (token=%s -> %s:%d)",
            migration_count,
            token,
            dst_server.ip_addr,
            dst_shard,
        )

        await asyncio.sleep(random.uniform(*pause_range))

    logger.info("Completed %d tablet migrations in %s seconds", migration_count, duration_sec)


@pytest.mark.asyncio
@skip_mode("release", "error injections are not supported in release mode")
@skip_mode("debug", "debug mode is too slow for this test")
async def test_multi_column_lwt_during_migration(manager: ManagerClient):
    """
    Test scenario:
      1. Start N servers with tablets enabled
      2. Disable auto-balancing
      3. Create keyspace/table
      4. Insert rows, precompute pk->token
      5. Start LWT workers
      6. Run tablet migrations in parallel
      7. Stop workers and verify consistency
    """

    # Setup cluster
    cfg = {
        "tablets_mode_for_new_keyspaces": "enabled",
        "rf_rack_valid_keyspaces": False,
    }

    servers = await manager.servers_add(6, config=cfg)
    for server in servers:
        await manager.api.disable_tablet_balancing(server.ip_addr)

    rf_max = len(servers) - 1
    rf = random.randint(2, rf_max)
    logger.info("Using replication_factor=%d (servers=%d)", rf, len(servers))

    async with new_test_keyspace(
        manager,
        f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}} "
        f"AND tablets = {{'initial': 5}}",
    ) as ks:
        stop_event_ = asyncio.Event()
        tester = BaseLWTTester(
            manager,
            ks,
            "lwt_table",
            num_workers=DEFAULT_WORKERS,
            num_keys=DEFAULT_NUM_KEYS,
        )
        await tester.create_schema()
        await tester.initialize_rows()
        await tester.start_workers(stop_event_)

        try:
            # Run continuous tablet migrations concurrently with the LWT workload
            logger.info(
                "Starting concurrent LWT workload and tablet migrations for %s seconds",
                WORKLOAD_SEC,
            )
            migration_task = asyncio.create_task(
                continuous_tablet_migrations(stop_event_, manager, servers, tester, WORKLOAD_SEC)
            )
            await asyncio.wait_for(migration_task, timeout=WORKLOAD_SEC + 5)


        finally:
            await tester.stop_workers()

        await tester.verify_consistency()
        logger.info("Multi-column LWT during continuous migrations test completed successfully")
