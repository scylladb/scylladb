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
    get_host_map,
)
from test.cluster.util import new_test_keyspace
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import HTTPError
from test.pylib.tablets import get_tablet_replicas

from .test_lwt_during_tablets_resize import run_random_resizes

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


async def tablet_migration_ops(stop_event: asyncio.Event,
        manager: ManagerClient, servers, tester, num_ops: int, pause_range=(0.5, 2.0),
        *, rack_map: Dict[str,str], host_id_map: Dict[str,str]
        ):
    logger.info("Starting tablet migration ops: target=%d", num_ops)
    migration_count = 0
    intranode_ratio = 0.3

    host_map = {host_id_map[s.server_id]: s for s in servers}
    attempt = 0
    while not stop_event.is_set() and migration_count < num_ops:
        attempt += 1
        sample_pk = random.choice(tester.pks)
        token = tester.pk_to_token[sample_pk]

        replicas = await get_tablet_replicas(manager, servers[0], tester.ks, tester.tbl, token)
        src_host_id, src_shard = random.choice(replicas)
        src_server = host_map.get(src_host_id)

        if random.random() < intranode_ratio:
            dst_host_id = src_host_id
            dst_shard = 0 if src_shard != 0 else 1
            dst_server = src_server
        else:
            replica_hids = {h for (h, _sh) in replicas}
            src_rack = rack_map[src_server.server_id]
            same_rack_candidates = []
            for s in servers:
                if rack_map[s.server_id] != src_rack:
                    continue
                hid = host_id_map[s.server_id]
                if hid in replica_hids:
                    continue
                same_rack_candidates.append(s)
            dst_server = random.choice(same_rack_candidates)
            dst_host_id = host_id_map[dst_server.server_id]
            dst_shard = 0

        try:
            await _move_tablet_with_retry(
                manager, src_server, tester.ks, tester.tbl,
                src_host_id, src_shard, dst_host_id, dst_shard, token,
                timeout_s=60
            )
            migration_count += 1
            logger.info("Completed migration #%d (token=%s -> %s:%d)",
                        migration_count, token, dst_server.ip_addr, dst_shard)
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

    assert migration_count == num_ops, f"Only completed {migration_count}/{num_ops} migrations"
    logger.info("Completed tablet migration ops: %d/%d", migration_count, num_ops)


@pytest.mark.asyncio
@skip_mode("release", "error injections are not supported in release mode")
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
        '--logger-log-level', 'paxos=trace'
    ]

    servers = await manager.servers_add(6, config=cfg, property_file=properties, cmdline=cmdline)
    rack_map = {s.server_id: prop["rack"] for s, prop in zip(servers, properties)}
    host_id_map = {}
    for s in servers:
        host_id_map[s.server_id] = await manager.get_host_id(s.server_id)

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
            # PHASE: warmup
            tester.set_phase(PHASE_WARMUP)
            logger.info("LWT warmup: waiting for %d applied CAS", WARMUP_LWT_CNT)
            await tester.wait_for_phase_ops(stop_event_, PHASE_WARMUP, WARMUP_LWT_CNT, timeout=180, poll=0.2)
            logger.info("LWT warmup complete: %d ops", tester.get_phase_ops(PHASE_WARMUP))

            # PHASE: resize + migrate
            tester.set_phase(PHASE_RESIZE)
            logger.info("Starting RESIZE (random powers-of-two) + %d migrations", NUM_MIGRATIONS)

            resize_task = asyncio.create_task(
                run_random_resizes(
                    stop_event_=stop_event_,
                    manager=manager,
                    servers=servers,
                    tester=tester,
                    ks=ks,
                    table=table,
                    target_steps=TARGET_RESIZE_COUNT,
                )
            )
            migrate_task = asyncio.create_task(
                tablet_migration_ops(
                    stop_event_,
                    manager, servers, tester,
                    num_ops=NUM_MIGRATIONS,
                    pause_range=(0.3, 1.0),
                    rack_map=rack_map,
                    host_id_map=host_id_map,
                )
            )

            resize_stats = await resize_task
            await migrate_task

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
