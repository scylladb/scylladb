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
    wait_for_tablet_count,
    DEFAULT_WORKERS,
    DEFAULT_NUM_KEYS,
)
from test.cluster.util import new_test_keyspace
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error_one_shot
from test.pylib.tablets import get_tablet_count

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# Test constants
NUM_MIGRATIONS = 20
WARMUP_LWT_CNT = 100
POST_LWT_CNT = 100
PHASE_WARMUP = 'warmup'
PHASE_POST = 'post'
PHASE_RESIZE = 'resize'


@pytest.mark.asyncio
@skip_mode("release", "error injections are not supported in release mode")
@skip_mode("debug", "debug mode is too slow for this test")
async def test_multi_column_lwt_during_split_merge(manager: ManagerClient):
    """
    Test scenario:
      1. Start N servers with tablets enabled
      2. Disable auto-balancing
      3. Create keyspace/table
      4. Insert rows, precompute pk->token
      5. Start LWT workers
      6. Run tablet resizing in parallel
      7. Stop workers and verify consistency
    """

    cfg = {
        "enable_tablets": True,
        "tablet_load_stats_refresh_interval_in_seconds": 1,
        "target-tablet-size-in-bytes": 1024 * 16,
    }
    properties = [
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"},
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"},
    ]
    servers = await manager.servers_add(6, config=cfg, property_file=properties)
    target_server = servers[0]
    logger.info(f"Disabling tablet balancing on random server: {target_server.ip_addr}")
    await manager.api.disable_tablet_balancing(target_server.ip_addr)

    async with new_test_keyspace(
        manager,
        f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}} "
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
            initial_cnt = await get_tablet_count(manager, servers[0], ks, table)
            logger.info("Initial tablet_count=%d", initial_cnt)

            # Phase 1: warmup LWT (100 applied CAS)
            tester.set_phase(PHASE_WARMUP)
            logger.info("LWT warmup: waiting for %d applied CAS", WARMUP_LWT_CNT)
            await tester.wait_for_phase_ops(stop_event_, PHASE_WARMUP, WARMUP_LWT_CNT, timeout=180, poll=0.2)
            logger.info("LWT warmup complete: %d ops", tester.get_phase_ops(PHASE_WARMUP))

            await asyncio.gather(
                *[
                    inject_error_one_shot(
                        manager.api, s.ip_addr, "tablet_load_stats_refresh_before_rebalancing"
                    )
                    for s in servers
                ]
            )

            logger.info(f"Enabling tablet balancing on random server: {target_server.ip_addr}")
            await manager.api.enable_tablet_balancing(target_server.ip_addr)

            # Phase 2: tablets resizing with LWT running
            logger.info(f"LWT during split/merge phase starting")
            tester.set_phase(PHASE_RESIZE)

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
            logger.info("LWT resize complete: %d ops", tester.get_phase_ops(PHASE_RESIZE))

            # Phase 3: post resize LWT (100 applied CAS)
            tester.set_phase(PHASE_POST)
            logger.info("LWT post resize: waiting for %d applied CAS", POST_LWT_CNT)
            await tester.wait_for_phase_ops(stop_event_, PHASE_POST, POST_LWT_CNT, timeout=180,poll=0.2)
            logger.info("LWT post resize complete: %d ops", tester.get_phase_ops(PHASE_POST))

            assert sum(tester.phase_ops.values()) > WARMUP_LWT_CNT + POST_LWT_CNT, (
                f"Expected more than {WARMUP_LWT_CNT + POST_LWT_CNT}, got {sum(tester.phase_ops.values())}"
            )

        finally:
            await tester.stop_workers()

        await tester.verify_consistency()
        logger.info("Multi-column LWT during split/merge test completed successfully")
