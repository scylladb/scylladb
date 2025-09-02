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

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Flooder params
FLOODER_BLOB_SIZE = 64 * 1024
FLOODER_QPS = 1000


class HeavyWriter:
    """Continuously inserts wide-row payloads to grow tablet sizes for triggering splits/merges."""

    def __init__(
        self,
        manager: ManagerClient,
        ks: str,
        tbl: str,
        num_keys: int,
        blob_size: int = FLOODER_BLOB_SIZE,
        qps: int = FLOODER_QPS,
    ):
        self.cql = manager.get_cql()
        self.ks = ks
        self.tbl = tbl
        self.num_keys = num_keys
        self.blob_size = blob_size
        self.period = 1.0 / max(1, qps)
        self.stop_event = asyncio.Event()
        self.task: Optional[asyncio.Task] = None
        self.ps_insert: Optional[PreparedStatement] = None
        self.rng = random.Random(0xFEED)

    def prepare(self):
        self.ps_insert = self.cql.prepare(
            f"INSERT INTO {self.ks}.{self.tbl} (pk, ck, payload) VALUES (?, now(), ?)"
        )

    async def _run(self):
        payload = os.urandom(self.blob_size)
        while not self.stop_event.is_set():
            pk = self.rng.randint(1, self.num_keys)
            stmt = self.ps_insert.bind([pk, payload])
            stmt.consistency_level = ConsistencyLevel.LOCAL_ONE
            try:
                await self.cql.run_async(stmt)
            except (WriteTimeout, OperationTimedOut):
                pass
            await asyncio.sleep(self.period)

    async def start(self):
        if not self.ps_insert:
            self.prepare()
        self.task = asyncio.create_task(self._run())

    async def stop(self):
        self.stop_event.set()
        if self.task:
            await self.task


class ExtendedLWTTester(BaseLWTTester):
    """
    Create STATIC+wide-row schema and use LIMIT 1 for reading static values.
    """

    async def create_schema(self):
        statics = ", ".join(f"s{i} int STATIC" for i in range(self.num_workers))
        await self.cql.run_async(
            f"""
            CREATE TABLE {self.ks}.{self.tbl} (
              pk int,
              ck timeuuid,
              {statics},
              payload blob,
              PRIMARY KEY (pk, ck)
            ) WITH compression = {{'sstable_compression': ''}}
              AND tablets = {{'min_tablet_count': 1}}
            """
        )
        logger.info(
            "Created table %s.%s with %d STATIC columns", self.ks, self.tbl, self.num_workers
        )

    async def initialize_rows(self):
        zeros = ", ".join("0" for _ in range(self.num_workers))
        ps = self.cql.prepare(
            f"INSERT INTO {self.ks}.{self.tbl} (pk, ck, {self.select_cols}) VALUES (?, now(), {zeros})"
        )
        for pk in self.pks:
            await self.cql.run_async(ps.bind([pk]))

    # Override SELECT to add LIMIT 1 for static read
    @property
    def select_statement(self):  # type: ignore[override]
        if not hasattr(self, "_select_ps"):
            self._select_ps = self.cql.prepare(
                f"SELECT {self.select_cols} FROM {self.ks}.{self.tbl} WHERE pk = ? LIMIT 1"
            )
        return self._select_ps

    async def verify_consistency(self):
        mismatches = []
        for pk in self.pks:
            stmt = SimpleStatement(
                f"SELECT {self.select_cols} FROM {self.ks}.{self.tbl} WHERE pk = %s LIMIT 1",
                consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            )
            row = (await self.cql.run_async(stmt, [pk]))[0]
            for i, worker in enumerate(self.workers):
                actual = getattr(row, f"s{i}")
                expected = worker.success_counts[pk]
                if actual != expected:
                    mismatches.append(f"pk={pk} s{i}={actual}, expected {expected}")
        assert not mismatches, "Consistency violations: " + "; ".join(mismatches) if mismatches else ""
        total_ops = sum(sum(w.success_counts.values()) for w in self.workers)
        logger.info("Consistency verified – %d total successful CAS operations", total_ops)


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
        tester = ExtendedLWTTester(
            manager,
            ks,
            table,
            num_workers=DEFAULT_WORKERS,
            num_keys=DEFAULT_NUM_KEYS,
        )
        await tester.create_schema()
        await tester.initialize_rows()

        # Start flooder + LWT workload
        flood = HeavyWriter(manager, ks, table, DEFAULT_NUM_KEYS, blob_size=FLOODER_BLOB_SIZE, qps=FLOODER_QPS)
        flood.prepare()

        await tester.start_workers(stop_event_)
        await flood.start()

        try:
            # Let memtables accumulate
            await asyncio.sleep(10)

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

            # Stop heavy writes before merge
            await flood.stop()

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

            await asyncio.sleep(5)

        finally:
            await tester.stop_workers()

        await tester.verify_consistency()
        logger.info("Multi-column LWT during split/merge test completed successfully")
