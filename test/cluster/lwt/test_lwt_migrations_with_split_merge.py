# Copyright (C) 2024-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import asyncio
import logging
import random
import re
import time

import pytest
from test.cluster.conftest import skip_mode
from test.cluster.lwt.lwt_common import (
    BaseLWTTester,
    wait_for_tablet_count,
    DEFAULT_WORKERS,
    DEFAULT_NUM_KEYS,
    get_host_map,
    pick_non_replica_server,
)
from test.cluster.util import new_test_keyspace
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import HTTPError
from test.pylib.rest_client import inject_error_one_shot
from test.pylib.tablets import get_tablet_count
from test.pylib.tablets import get_tablet_replicas


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

NUM_MIGRATIONS = 15
WARMUP_LWT_CNT = 100
POST_LWT_CNT = 100
PHASE_WARMUP = "warmup"
PHASE_RESIZE = "resize"
PHASE_POST = "post"


NO_REPLICA_RE = re.compile(r"has no replica on", re.IGNORECASE)
DST_REPLICA_RE = re.compile(r"has replica on", re.IGNORECASE)


def _err_code(e: Exception):
    return getattr(e, "code", None)

def _err_text(e: Exception):
    return getattr(e, "text", "") or str(e)

def _is_tablet_in_transition_http_error(e: Exception) -> bool:
    return isinstance(e, HTTPError) and _err_code(e) in (409, 500) and "in transition" in _err_text(e).lower()

def _is_no_replica_on_src_error(e: Exception) -> bool:
    return isinstance(e, HTTPError) and _err_code(e) in (409, 500) and NO_REPLICA_RE.search(_err_text(e)) is not None

def _is_dst_already_replica_error(e: Exception) -> bool:
    return isinstance(e, HTTPError) and _err_code(e) in (409, 500) and DST_REPLICA_RE.search(_err_text(e)) is not None


async def _move_tablet_with_retry(manager, src_server, ks, tbl,
                                  src_host_id, src_shard, dst_host_id, dst_shard, token,
                                  *, timeout_s=90, base_sleep=0.1, max_sleep=2.0):
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
                logger.info("Token %s in transition, retrying after %.1s", token, sleep)
                await asyncio.sleep(sleep + random.uniform(0, sleep))
                sleep = min(sleep * 1.7, max_sleep)
                continue
            raise


async def split_then_merge(
    manager: ManagerClient,
    servers,
    ks: str,
    table: str,
    split_target: int,
    merge_target: int,
    timeout_split_s: int = 240,
    timeout_merge_s: int = 300,
):
    # SPLIT
    await manager.get_cql().run_async(
        f"ALTER TABLE {ks}.{table} WITH tablets = {{'min_tablet_count': {split_target}}}"
    )
    logger.info("Waiting for split")
    split_cnt = await wait_for_tablet_count(
        manager,
        servers[0],
        ks,
        table,
        predicate=lambda c: c >= split_target,
        target=split_target,
        timeout_s=timeout_split_s,
    )
    logger.info("Observed split: tablet_count=%d (target=%d)", split_cnt, split_target)
    assert split_cnt >= split_target, f"Expected >= {split_target}, got {split_cnt}"

    # MERGE
    await manager.get_cql().run_async(
        f"ALTER TABLE {ks}.{table} WITH tablets = {{'min_tablet_count': {merge_target}}}"
    )
    logger.info("Waiting for merge")
    merge_cnt = await wait_for_tablet_count(
        manager,
        servers[0],
        ks,
        table,
        predicate=lambda c: c <= merge_target,
        target=merge_target,
        timeout_s=timeout_merge_s,
    )
    logger.info("Observed merge: tablet_count=%d (target=%d)", merge_cnt, merge_target)
    assert merge_cnt <= merge_target, f"Expected <= {merge_target}, got {merge_cnt}"


async def tablet_migration_ops(stop_event: asyncio.Event,
        manager: ManagerClient, servers, tester, num_ops: int, pause_range=(0.5, 2.0)
        ):
    logger.info("Starting tablet migration ops: target=%d", num_ops)
    migration_count = 0
    intranode_ratio = 0.3

    host_map = await get_host_map(manager, servers)
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
            dst_server = await pick_non_replica_server(manager, servers, replica_hids)
            dst_host_id = await manager.get_host_id(dst_server.server_id)
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
async def test_multi_column_lwt_migrate_and_split_merge(manager: ManagerClient):
    """
    Combined scenario:
      1) Start N servers (tablets enabled), disable balancer initially.
      2) Create KS/table, init rows + pk->token.
      3) Start LWT workers.
      4) Warmup: wait for WARMUP_LWT_CNT applied CAS.
      5) Force load-stats refresh, enable balancer.
      6) Phase RESIZE: run split->merge while CONCURRENTLY performing ~15 tablet migrations.
      7) Post: wait for POST_LWT_CNT applied CAS.
      8) Stop workers; verify consistency.
    """

    cfg = {
        "enable_tablets": True,
        "tablet_load_stats_refresh_interval_in_seconds": 1,
        "target-tablet-size-in-bytes": 1024 * 16,
        "rf_rack_valid_keyspaces": False,
    }

    servers = await manager.servers_add(6, config=cfg)
    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    rf_max = len(servers) - 1
    rf = random.randint(2, rf_max)
    logger.info("Using replication_factor=%d (servers=%d)", rf, len(servers))

    async with new_test_keyspace(
        manager,
        f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}} "
        f"AND tablets = {{'initial': 5}}",
    ) as ks:
        stop_event_ = asyncio.Event()
        table = "lwt_migrate_resize_table"
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
            tester.set_phase(PHASE_WARMUP)
            logger.info("Warmup LWT: waiting for %d applied CAS", WARMUP_LWT_CNT)
            await tester.wait_for_phase_ops(stop_event_, PHASE_WARMUP, WARMUP_LWT_CNT, timeout=180, poll=0.2)
            logger.info("Warmup complete: %d ops", tester.get_phase_ops(PHASE_WARMUP))

            # Force load stats refresh to make rebalancing decisions current before enabling balancer.
            await asyncio.gather(
                *[
                    inject_error_one_shot(
                        manager.api, s.ip_addr, "tablet_load_stats_refresh_before_rebalancing"
                    )
                    for s in servers
                ]
            )

            await manager.api.enable_tablet_balancing(servers[0].ip_addr)

            tester.set_phase(PHASE_RESIZE)
            logger.info("Starting RESIZE (split->merge) with concurrent migrations: %d", NUM_MIGRATIONS)

            cur_before_split = await get_tablet_count(manager, servers[0], ks, table)
            logger.info("Tablet count before split: %d", cur_before_split)
            split_target = cur_before_split * 2
            merge_target = split_target // 2
            logger.info("Split target: %d, merge target: %d", split_target, merge_target)

            resize_task = asyncio.create_task(
                split_then_merge(manager, servers, ks, table, split_target=split_target, merge_target=merge_target)
            )
            migration_task = asyncio.create_task(
                tablet_migration_ops(stop_event_, manager, servers, tester, NUM_MIGRATIONS, pause_range=(0.2, 1.0))
            )

            await asyncio.wait_for(asyncio.gather(resize_task, migration_task), timeout=split_target * 60 / 3)

            logger.info("RESIZE phase LWT ops: %d", tester.get_phase_ops(PHASE_RESIZE))

            tester.set_phase(PHASE_POST)
            logger.info("Post phase: waiting for %d applied CAS", POST_LWT_CNT)
            await tester.wait_for_phase_ops(stop_event_, PHASE_POST, POST_LWT_CNT, timeout=180, poll=0.2)
            logger.info("Post complete: %d ops", tester.get_phase_ops(PHASE_POST))

            total_ops = sum(tester.phase_ops.values())
            assert total_ops > (WARMUP_LWT_CNT + POST_LWT_CNT), (
                f"Expected > {WARMUP_LWT_CNT + POST_LWT_CNT} total ops, got {total_ops}"
            )

        finally:
            await tester.stop_workers()

        await tester.verify_consistency()
        logger.info("Combined LWT during migrate + split/merge test completed successfully")
