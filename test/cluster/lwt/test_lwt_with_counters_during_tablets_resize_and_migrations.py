# Copyright (C) 2025-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import asyncio
import logging
import random
import re
import time
from typing import Dict

import pytest
from test.cluster.conftest import skip_mode
from test.cluster.lwt.lwt_common import (
    BaseLWTTester,
    DEFAULT_WORKERS,
    DEFAULT_NUM_KEYS,
    wait_for_tablet_count
)
from test.cluster.util import new_test_keyspace
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import HTTPError
from test.pylib.tablets import get_tablet_count
from test.pylib.tablets import get_tablet_replicas

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


TARGET_RESIZE_COUNT = 20
NUM_MIGRATIONS = 20
WARMUP_LWT_CNT = 100
POST_LWT_CNT = 100

PHASE_WARMUP = "warmup"
PHASE_RESIZE = "resize"
PHASE_POST = "post"

MIN_TABLETS = 1
MAX_TABLETS = 20
RESIZE_TIMEOUT = 240
MIGRATE_ONE_TIMEOUT_S = 60
NO_REPLICA_RE = re.compile(r"has no replica on", re.IGNORECASE)
DST_REPLICA_RE = re.compile(r"has replica on", re.IGNORECASE)


def _err_code(e: Exception):
    return getattr(e, "code", None)

def _err_text(e: Exception):
    return getattr(e, "text", "") or str(e)

def _is_tablet_in_transition_http_error(e: Exception) -> bool:
    return isinstance(e, HTTPError) and _err_code(e) == 500 and "in transition" in _err_text(e).lower()

def _is_no_replica_on_src_error(e: Exception) -> bool:
    return isinstance(e, HTTPError) and _err_code(e) == 500  and NO_REPLICA_RE.search(_err_text(e)) is not None

def _is_dst_already_replica_error(e: Exception) -> bool:
    return isinstance(e, HTTPError) and _err_code(e) == 500 and DST_REPLICA_RE.search(_err_text(e)) is not None


async def _move_tablet_with_retry(manager, src_server, ks, tbl,
                                  src_host_id, src_shard, dst_host_id, dst_shard, token,
                                  *, timeout_s=MIGRATE_ONE_TIMEOUT_S, base_sleep=0.1, max_sleep=2.0):
    deadline = time.time() + timeout_s
    sleep = base_sleep
    while True:
        try:
            await manager.api.move_tablet(
                src_server.ip_addr, ks, tbl,
                src_host_id, src_shard, dst_host_id, dst_shard, token
            )
            return
        except Exception as e:
            if _is_tablet_in_transition_http_error(e) and time.time() + sleep < deadline:
                logger.info("Token %s in transition, retry in %.2fs", token, sleep)
                await asyncio.sleep(sleep + random.uniform(0, sleep))
                sleep = min(sleep * 1.7, max_sleep)
                continue
            raise


async def tablet_migration_ops(
    stop_event: asyncio.Event,
    manager: ManagerClient,
    servers,
    tester: BaseLWTTester,
    table: str,
    num_ops: int,
    pause_range=(0.5, 2.0),
    *,
    server_properties,
) -> None:
    logger.info("Starting tablet migration ops for %s.%s: target=%d", tester.ks, table, num_ops)
    migration_count = 0
    intranode_ratio = 0.3

    # server_id -> rack
    server_id_to_rack: Dict[str, str] = {
        s.server_id: prop["rack"] for s, prop in zip(servers, server_properties)
    }
    host_ids = await asyncio.gather(
        *(manager.get_host_id(s.server_id) for s in servers)
    )
    # server_id -> host_id и host_id -> server
    server_id_to_host_id: Dict[str, str] = {
        s.server_id: hid for s, hid in zip(servers, host_ids)
    }
    host_id_to_server = {
        hid: s for s, hid in zip(servers, host_ids)
    }

    attempt = 0
    while not stop_event.is_set() and migration_count < num_ops:
        attempt += 1
        sample_pk = random.choice(tester.pks)
        token = tester.pk_to_token[sample_pk]

        replicas = await get_tablet_replicas(
            manager, servers[0], tester.ks, table, token
        )
        src_host_id, src_shard = random.choice(replicas)
        src_server = host_id_to_server.get(src_host_id)
        assert src_server is not None, (
            f"Source host_id {src_host_id} for token {token} not found in host_id_to_server (attempt {attempt})"
        )

        if random.random() < intranode_ratio:
            dst_host_id = src_host_id
            dst_server = src_server
            dst_shard = 0 if src_shard != 0 else 1
        else:
            replica_hids = {h for (h, _sh) in replicas}
            src_rack = server_id_to_rack[src_server.server_id]

            same_rack_candidates = [
                s for s in servers if server_id_to_rack[s.server_id] == src_rack
                and server_id_to_host_id[s.server_id] not in replica_hids
            ]

            assert same_rack_candidates, (
                f"No same-rack non-replica candidate for token {token} (attempt {attempt})"
            )

            dst_server = random.choice(same_rack_candidates)
            dst_host_id = server_id_to_host_id[dst_server.server_id]
            dst_shard = 0

        try:
            await _move_tablet_with_retry(
                manager, src_server, tester.ks, table,
                src_host_id, src_shard, dst_host_id, dst_shard, token,
                timeout_s=60,
            )

            migration_count += 1
            logger.info(
                "Completed migration #%d (token=%s -> %s:%d) for %s.%s",
                migration_count, token, dst_server.ip_addr, dst_shard, tester.ks, table,
            )
            await asyncio.sleep(random.uniform(*pause_range))
            continue
        except Exception as e:
            if _is_tablet_in_transition_http_error(e):
                logger.info("Token %s in transition, switching token (attempt %d)",
                            token, attempt)
                continue
            if _is_no_replica_on_src_error(e) or _is_dst_already_replica_error(e):
                logger.info("Src replica vanished for token %s, re-pick (attempt %d)",
                            token, attempt)
                continue
            raise

    assert migration_count == num_ops, f"Only completed {migration_count}/{num_ops} migrations for {tester.ks}.{table}"
    logger.info("Completed tablet migration ops for %s.%s: %d/%d", tester.ks, table, migration_count, num_ops)


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
    counter_table: str,
    target_steps: int = TARGET_RESIZE_COUNT,
    pause_range=(0.5, 2.0),
):
    """
    Perform randomized tablet count changes (splits/merges) on the main LWT table
    and its counter table. Runs until target resize count is reached or stop_event_
    is set. Returns a dict with simple stats.
    """
    split_count = 0
    merge_count = 0
    current_resize_count = 0
    pow2_targets = powers_of_two_in_range(MIN_TABLETS, MAX_TABLETS)

    while not stop_event_.is_set() and current_resize_count < target_steps:
        # Drive resize direction from the main table.
        current_main = await get_tablet_count(manager, servers[0], ks, table)

        candidates = [t for t in pow2_targets if t != current_main]
        target_cnt = random.choice(candidates)

        direction = "split" if target_cnt > current_main else "merge"
        logger.info(
            "[%s] starting: %s.%s=%d, %s.%s -> target %d",
            direction.upper(), ks, table, current_main, ks,
            counter_table, target_cnt
        )
        tables = [table, counter_table]
        # Apply ALTER TABLE to both tables.
        for tbl in tables:
            await tester.cql.run_async(
                f"ALTER TABLE {ks}.{tbl} "
                f"WITH tablets = {{'min_tablet_count': {target_cnt}}}"
            )

        if direction == "split":
            predicate = lambda c, tgt=target_cnt: c >= tgt
        else:
            predicate = lambda c, tgt=target_cnt: c <= tgt

        # Wait for both tables to converge.
        main_after, counter_after = await asyncio.gather(
            wait_for_tablet_count(
                manager,
                servers[0],
                tester.ks,
                table,
                predicate=predicate,
                target=target_cnt,
                timeout_s=RESIZE_TIMEOUT,
            ),
            wait_for_tablet_count(
                manager,
                servers[0],
                tester.ks,
                counter_table,
                predicate=predicate,
                target=target_cnt,
                timeout_s=RESIZE_TIMEOUT,
            ),
        )

        # Sanity: both tables should end up with the same tablet count.
        assert main_after == counter_after, (
            f"Tablet counts diverged: {ks}.{table}={main_after}, "
            f"{ks}.{counter_table}={counter_after}"
        )

        if direction == "split":
            logger.info(
                "[SPLIT] converged: %s.%s %d -> %d, %s.%s -> %d (target %d)",
                ks, table, current_main, main_after, ks, counter_table,
                counter_after, target_cnt
            )
            assert main_after >= current_main, (
                f"Tablet count expected to increase during split "
                f"(was {current_main}, now {main_after})"
            )
            split_count += 1
        else:
            logger.info(
                "[MERGE] converged: %s.%s %d -> %d, %s.%s -> %d (target %d)",
                ks, table, current_main, main_after, ks, counter_table,
                counter_after, target_cnt
            )
            assert main_after <= current_main, (
                f"Tablet count expected to decrease during merge "
                f"(was {current_main}, now {main_after})"
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
@skip_mode("debug", "debug mode is too slow for this test")
async def test_multi_column_lwt_migrate_and_random_resizes(manager: ManagerClient):

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
        '--logger-log-level', 'paxos=trace', '--smp=2',
    ]

    servers = await manager.servers_add(6, config=cfg, property_file=properties, cmdline=cmdline)
    
    async with new_test_keyspace(
        manager,
        "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} "
        "AND tablets = {'initial': 1}",
    ) as ks:
        stop_event_ = asyncio.Event()
        table = "lwt_split_merge_table"
        cnt_table = "lwt_split_merge_counters"
        tester = BaseLWTTester(
            manager,
            ks,
            table,
            num_workers=DEFAULT_WORKERS,
            num_keys=DEFAULT_NUM_KEYS,
            use_counters=True,
            counters_random_delta=True,
            counters_max_delta=5,
            counter_tbl=cnt_table,
        )

        await tester.create_schema()
        await tester.initialize_rows()
        await tester.start_workers(stop_event_)

        try:
            # PHASE: warmup
            tester.set_phase(PHASE_WARMUP)
            logger.info("LWT warmup: waiting for %d applied CAS", WARMUP_LWT_CNT)
            await tester.wait_for_phase_ops(stop_event_, PHASE_WARMUP, WARMUP_LWT_CNT, timeout=180, poll=0.2)
            logger.info("LWT warmup complete: %d ops", tester.get_phase_ops(PHASE_WARMUP))

            # PHASE: resize + migrate
            tester.set_phase(PHASE_RESIZE)
            logger.info("Starting RESIZE (random powers-of-two) + %d migrations per table", NUM_MIGRATIONS)

            resize_task = asyncio.create_task(
                run_random_resizes(
                    stop_event_=stop_event_,
                    manager=manager,
                    servers=servers,
                    tester=tester,
                    ks=ks,
                    table=table,
                    target_steps=TARGET_RESIZE_COUNT,
                    counter_table=cnt_table,
                )
            )
            migrate_task = asyncio.create_task(
                tablet_migration_ops(
                    stop_event_,
                    manager, servers, tester,
                    num_ops=NUM_MIGRATIONS,
                    pause_range=(0.3, 1.0),
                    server_properties=properties,
                    table=table,
                )
            )
            migrate_cnt_task = asyncio.create_task(
                tablet_migration_ops(
                    stop_event_,
                    manager, servers, tester,
                    num_ops=NUM_MIGRATIONS,
                    pause_range=(0.3, 1.0),
                    server_properties=properties,
                    table=cnt_table
                )
            )

            resize_stats = await resize_task
            await asyncio.gather(migrate_task, migrate_cnt_task)

            logger.info(
                "Randomized resize stats: steps_done=%d, split=%d, merge=%d; LWT ops during resize=%d",
                resize_stats["steps_done"], resize_stats["seen_split"], resize_stats["seen_merge"],
                tester.get_phase_ops(PHASE_RESIZE),
            )
            assert resize_stats["steps_done"] >= 1, "Resize phase performed 0 steps"
            assert tester.get_phase_ops(PHASE_RESIZE) > 0, "Expected LWT ops during RESIZE phase"

            # PHASE: post
            tester.set_phase(PHASE_POST)
            logger.info("LWT post resize: waiting for %d applied CAS", POST_LWT_CNT)
            await tester.wait_for_phase_ops(stop_event_, PHASE_POST, POST_LWT_CNT, timeout=180, poll=0.2)
            logger.info("LWT post resize complete: %d ops", tester.get_phase_ops(PHASE_POST))

            total_ops = sum(tester.phase_ops.values())
            assert total_ops >= (WARMUP_LWT_CNT + POST_LWT_CNT), f"Too few total LWT ops: {total_ops}"

        finally:
            await tester.stop_workers()

        await tester.verify_consistency()
        logger.info("Combined LWT during random split/merge + migrations test completed successfully")
