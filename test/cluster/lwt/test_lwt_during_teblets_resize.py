#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import logging
import os
import random
from typing import Optional

import pytest
from cassandra import ConsistencyLevel
from cassandra import WriteTimeout, OperationTimedOut
from cassandra.query import PreparedStatement, SimpleStatement

from test.cluster.conftest import skip_mode
from test.cluster.util import new_test_keyspace
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error_one_shot

from test.cluster.lwt.lwt_common import (
    BaseLWTTester,
    wait_for_tablet_count,
    DEFAULT_WORKERS,
    DEFAULT_NUM_KEYS,
)

from test.pylib.tablets import get_tablet_count

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@pytest.mark.asyncio
@skip_mode("release", "error injections are not supported in release mode")
@skip_mode("debug", "debug mode is too slow for this test")
async def test_multi_column_lwt_during_split_merge(manager: ManagerClient):
    """Test multi-column LWT pattern during tablet split and merge operations"""

    cfg = {
        "enable_tablets": True,
        "tablet_load_stats_refresh_interval_in_seconds": 1,
        "enable_tablet_merging": True,
        "target-tablet-size-in-bytes": 1024 * 16,
        "rf_rack_valid_keyspaces": False,
    }

    servers = await manager.servers_add(6, config=cfg)
    # Disable balancer initially
    for s in servers:
        await manager.api.disable_tablet_balancing(s.ip_addr)

    rf_max = len(servers) - 1
    rf = random.randint(2, rf_max)
    logger.info("Using replication_factor=%d (servers=%d)", rf, len(servers))

    async with new_test_keyspace(
        manager,
        f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}} "
        f"AND tablets = {{'initial': 1}}",
    ) as ks:
        stop_event_ = asyncio.Event()
        table = "lwt_split_merge_table"
        tester = BaseLWTTester(
            manager,
            ks,
            table,
            num_workers=DEFAULT_WORKERS,
            num_keys=DEFAULT_NUM_KEYS,
        )

        await tester.create_schema()
        await tester.initialize_rows()
        await tester.start_workers(stop_event_)

        try:
            # Let memtables accumulate
            await asyncio.sleep(10)

            initial_cnt = await get_tablet_count(manager, servers[0], ks, table)
            logger.info("Initial tablet_count=%d", initial_cnt)

            # Flush to disk to trigger size-based split
            for s in servers:
                await manager.api.flush_keyspace(s.ip_addr, ks)

            # Force load stats refresh before enabling balancing
            await asyncio.gather(
                *[
                    inject_error_one_shot(
                        manager.api, s.ip_addr, "tablet_load_stats_refresh_before_rebalancing"
                    )
                    for s in servers
                ]
            )

            # Enable balancer everywhere
            for s in servers:
                await manager.api.enable_tablet_balancing(s.ip_addr)

            # Request split by raising min_tablet_count
            split_target = 4
            await tester.cql.run_async(
                f"ALTER TABLE {ks}.{table} WITH tablets = {{'min_tablet_count': {split_target}}}"
            )

            split_cnt = await wait_for_tablet_count(
                manager,
                servers[0],
                ks,
                table,
                predicate=lambda c: c >= split_target,
                target=split_target,
                timeout_s=240,
            )

            logger.info("Observed split: tablet_count=%d", split_cnt)
            assert split_cnt >= split_target, f"Expected >= {split_target}, got {split_cnt}"
            assert split_cnt > initial_cnt, (
                f"Expected tablet count to increase after split (was {initial_cnt}, now {split_cnt})"
            )

            # Compact to shrink and help merge
            for s in servers:
                await manager.api.flush_keyspace(s.ip_addr, ks)
                await manager.api.keyspace_compaction(s.ip_addr, ks)

            # Request merge by lowering min_tablet_count
            merge_target = 1
            await tester.cql.run_async(
                f"ALTER TABLE {ks}.{table} WITH tablets = {{'min_tablet_count': {merge_target}}}"
            )

            merge_cnt = await wait_for_tablet_count(
                manager,
                servers[0],
                ks,
                table,
                predicate=lambda c: c <= merge_target,
                target=merge_target,
                timeout_s=300,
            )

            logger.info("Observed merge: tablet_count=%d", merge_cnt)
            assert merge_cnt <= merge_target, f"Expected <= {merge_target}, got {merge_cnt}"
            assert merge_cnt < split_cnt, (
                f"Expected tablet count to decrease after merge (was {split_cnt}, now {merge_cnt})"
            )
            await asyncio.sleep(5)

        finally:
            await tester.stop_workers()

        await tester.verify_consistency()
        logger.info("Multi-column LWT during split/merge test completed successfully")
