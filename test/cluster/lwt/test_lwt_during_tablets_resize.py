#
# Copyright (C) 2025-present ScyllaDB
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
from test.pylib.tablets import get_tablet_count

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Test constants
TARGET_RESIZE_COUNT = 20
WARMUP_LWT_CNT = 100
POST_LWT_CNT = 100
PHASE_WARMUP = "warmup"
PHASE_POST = "post"
PHASE_RESIZE = "resize"
MIN_TABLETS = 1
MAX_TABLETS = 20
RESIZE_TIMEOUT = 240


def powers_of_two_in_range(lo: int, hi: int):
    if lo > hi or hi < 1:
        return []
    lo = max(1, lo)
    start_e = (lo - 1).bit_length()
    end_e = hi.bit_length()
    return [1 << e for e in range(start_e, end_e + 1) if (1 << e) <= hi]


async def run_random_resizes(
    stop_event_: asyncio.Event,
    manager: ManagerClient,
    servers,
    tester: BaseLWTTester,
    ks: str,
    table: str,
    target_steps: int = TARGET_RESIZE_COUNT,
    pause_range=(0.5, 2.0)
):
    """
    Perform randomized tablet count changes (splits/merges) until target resize count is reached
    or stop_event_ is set. Returns a dict with simple stats.
    """
    split_count = 0
    merge_count = 0
    current_resize_count = 0
    pow2_targets = powers_of_two_in_range(MIN_TABLETS, MAX_TABLETS)

    while not stop_event_.is_set() and current_resize_count < target_steps:
        current_count = await get_tablet_count(manager, servers[0], ks, table)
        candidates = [t for t in pow2_targets if t != current_count]
        target_cnt = random.choice(candidates)

        direction = "split" if target_cnt > current_count else "merge"
        logger.info(
            "[%s] starting: %s.%s tablet_count %d -> target %d",
            direction.upper(),
            ks,
            table,
            current_count,
            target_cnt,
        )

        # Apply resize
        await tester.cql.run_async(
            f"ALTER TABLE {ks}.{table} WITH tablets = {{'min_tablet_count': {target_cnt}}}"
        )

        count_after_resize = await wait_for_tablet_count(
            manager, servers[0], tester.ks, tester.tbl,
            predicate=(
                (lambda c, tgt=target_cnt: c >= tgt)
                if direction == "split"
                else (lambda c, tgt=target_cnt: c <= tgt)
            ),
            target=target_cnt,
            timeout_s=RESIZE_TIMEOUT
        )

        if direction == "split":
            logger.info(
                "[SPLIT] converged: %s.%s tablet_count %d -> %d (target %d)",
                ks,
                table,
                current_count,
                count_after_resize,
                target_cnt,
            )
            assert count_after_resize >= current_count, (
                f"Tablet count expected to be increased during split (was {current_count}, now {count_after_resize})"
            )
            split_count += 1
        else:
            logger.info(
                "[MERGE] converged: %s.%s tablet_count %d -> %d (target %d)",
                ks,
                table,
                current_count,
                count_after_resize,
                target_cnt,
            )
            assert count_after_resize <= current_count, (
                f"Tablet count expected to be decreased during merge (was {current_count}, now {count_after_resize})"
            )
            merge_count += 1

        current_resize_count += 1
        await asyncio.sleep(random.uniform(*pause_range))

    return {
        "steps_done": current_resize_count,
        "seen_split": split_count,
        "seen_merge": merge_count,
    }


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.skip_mode(mode='debug', reason='debug mode is too slow for this test')
async def test_multi_column_lwt_during_split_merge(manager: ManagerClient):
    """
    Test scenario:
      1. Start N servers with tablets enabled
      2. Create keyspace/table
      3. Insert rows, precompute pk->token
      4. Start LWT workers
      5. Run randomized tablet resizing in parallel
      6. Stop workers and verify consistency
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
    cmdline = [
        '--logger-log-level', 'paxos=trace'
    ]
    servers = await manager.servers_add(6, config=cfg, cmdline=cmdline, property_file=properties)

    async with new_test_keyspace(
        manager,
        "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} "
        "AND tablets = {'initial': 1}",
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
            # Phase 1: warmup LWT (100 applied CAS)
            tester.set_phase(PHASE_WARMUP)
            logger.info("LWT warmup: waiting for %d applied CAS", WARMUP_LWT_CNT)
            await tester.wait_for_phase_ops(stop_event_, PHASE_WARMUP, WARMUP_LWT_CNT, timeout=180, poll=0.2)
            logger.info("LWT warmup complete: %d ops", tester.get_phase_ops(PHASE_WARMUP))

            # Phase 2: randomized resizes with LWT running
            logger.info(f"LWT during split/merge phase starting")
            tester.set_phase(PHASE_RESIZE)

            resize_stats = await run_random_resizes(
                stop_event_=stop_event_,
                manager=manager,
                servers=servers,
                tester=tester,
                ks=ks,
                table=table,
                target_steps=TARGET_RESIZE_COUNT,
            )
            logger.info("LWT resize complete: %d ops", tester.get_phase_ops(PHASE_RESIZE))

            # Phase 3: post resize LWT (100 applied CAS)
            tester.set_phase(PHASE_POST)
            logger.info("LWT post resize: waiting for %d applied CAS", POST_LWT_CNT)
            await tester.wait_for_phase_ops(stop_event_, PHASE_POST, POST_LWT_CNT, timeout=180, poll=0.2)
            logger.info("LWT post resize complete: %d ops", tester.get_phase_ops(PHASE_POST))

            logger.info(
                "Randomized resize complete: steps_done=%d, seen_split=%s, seen_merge=%s, ops=%d",
                resize_stats["steps_done"],
                resize_stats["seen_split"],
                resize_stats["seen_merge"],
                sum(tester.phase_ops.values()),
            )

        finally:
            await tester.stop_workers()

        await tester.verify_consistency()
        logger.info("Multi-column LWT during randomized split/merge test completed successfully")
