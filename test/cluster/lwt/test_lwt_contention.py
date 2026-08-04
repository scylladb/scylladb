#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Test LWT (Paxos) contention with many concurrent workers.

Re-implementation of test_contention_test_many_threads and
test_contention_test_multi_iterations from scylla-dtest/paxos_tests.py
using asyncio instead of threads.

Both scenarios spawn async workers that contend on the same row via a
conditional batch statement (UPDATE IF v=? + INSERT IF NOT EXISTS). Each
worker tries to increment a shared static column `v` exactly `iterations`
times. At the end we verify that the final value equals workers * iterations,
confirming linearizability under contention.
"""

import asyncio
import logging
import time

import pytest
from cassandra import ConsistencyLevel, WriteTimeout, ReadTimeout, OperationTimedOut
from cassandra.query import SimpleStatement, PreparedStatement

from test.cluster.lwt.lwt_common import BaseLWTTester, is_uncertainty_timeout
from test.cluster.util import new_test_keyspace
from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)


class ContentionWorker:
    """
    A single async worker that contends on a shared row via batch LWT.

    Uses:
      BEGIN BATCH
        UPDATE test SET v = ? WHERE k = 0 IF v = ?;
        INSERT INTO test (k, id) VALUES (0, ?) IF NOT EXISTS;
      APPLY BATCH

    On success (applied=True), increments prev and moves to next iteration.
    On CAS failure, checks if the INSERT was applied (meaning our previous
    attempt succeeded despite a timeout) or retries with the updated value.
    On WriteTimeout, simply retries.
    """

    def __init__(
        self,
        worker_id: int,
        cql,
        batch_stmt: PreparedStatement,
        delete_stmt: PreparedStatement,
        iterations: int,
        stop_event: asyncio.Event,
        max_retries_per_iteration: int,
    ):
        self.worker_id = worker_id
        self.cql = cql
        self.batch_stmt = batch_stmt
        self.delete_stmt = delete_stmt
        self.iterations = iterations
        self.stop_event = stop_event
        self.max_retries_per_iteration = max_retries_per_iteration
        self.retries = 0
        self.uncertainty_timeouts = 0

    def stop(self) -> None:
        self.stop_event.set()

    async def __call__(self):
        prev = 0
        for i in range(self.iterations):
            attempt = 0
            while not self.stop_event.is_set():
                if attempt >= self.max_retries_per_iteration:
                    self.stop()
                    raise RuntimeError(
                        f"[worker {self.worker_id}] Exceeded max retries "
                        f"({self.max_retries_per_iteration}) for iteration (prev={prev})"
                    )
                attempt += 1
                try:
                    res = await self.cql.run_async(
                        self.batch_stmt, [prev + 1, prev, self.worker_id]
                    )
                    row = res[0]
                    if row.applied:
                        prev += 1
                        break
                    self.retries += 1
                    prev = row.v
                    if any(getattr(r, 'id', None) == self.worker_id for r in res):
                        logger.debug(
                            "[%d] Update was inserted on previous try (v=%d)",
                            self.worker_id, prev,
                        )
                        break
                except (WriteTimeout, ReadTimeout, OperationTimedOut) as e:
                    self.retries += 1
                    if is_uncertainty_timeout(e):
                        self.uncertainty_timeouts += 1
                        logger.warning(
                            "[worker %d] Uncertainty timeout (prev=%d, attempt=%d): %s",
                            self.worker_id, prev, attempt, e,
                        )
                except Exception as e:
                    logger.error(
                        "[worker %d] CAS error (prev=%d): %r",
                        self.worker_id, prev, e,
                    )
                    self.stop()
                    raise

            if i < self.iterations - 1:
                while not self.stop_event.is_set():
                    try:
                        await self.cql.run_async(self.delete_stmt, [self.worker_id])
                        break
                    except (WriteTimeout, ReadTimeout, OperationTimedOut):
                        pass

        logger.info("ContentionWorker %d finished", self.worker_id)


class ContentionLWTTester(BaseLWTTester):
    """
    Inherits from BaseLWTTester, overriding schema/workers/verification
    to implement the contention test pattern: many workers contend on a
    single shared static column via batch LWT.
    """

    def __init__(self, manager: ManagerClient, ks: str, tbl: str,
                 num_workers: int, iterations: int, max_retries_per_iteration: int, *,
                 scale_timeout):
        super().__init__(
            manager, ks, tbl,
            num_workers=num_workers,
            num_keys=1,  # single partition key (k=0)
            scale_timeout=scale_timeout,
        )
        self.iterations = iterations
        self.max_retries_per_iteration = max_retries_per_iteration

    async def create_schema(self):
        await self.cql.run_async(
            f"CREATE TABLE {self.ks}.{self.tbl} "
            f"(k int, v int static, id int, PRIMARY KEY (k, id))"
        )
        logger.info("Created contention table %s.%s", self.ks, self.tbl)

    async def initialize_rows(self):
        await self.cql.run_async(
            f"INSERT INTO {self.ks}.{self.tbl} (k, v) VALUES (0, 0)"
        )

    def create_workers(self, stop_event) -> list:
        batch_stmt = self.cql.prepare(f"""
            BEGIN BATCH
                UPDATE {self.ks}.{self.tbl} SET v = ? WHERE k = 0 IF v = ?;
                INSERT INTO {self.ks}.{self.tbl} (k, id) VALUES (0, ?) IF NOT EXISTS;
            APPLY BATCH
        """)
        delete_stmt = self.cql.prepare(
            f"DELETE FROM {self.ks}.{self.tbl} WHERE k = 0 AND id = ? IF EXISTS"
        )
        return [
            ContentionWorker(
                worker_id=i,
                cql=self.cql,
                batch_stmt=batch_stmt,
                delete_stmt=delete_stmt,
                iterations=self.iterations,
                stop_event=stop_event,
                max_retries_per_iteration=self.max_retries_per_iteration,
            )
            for i in range(self.num_workers)
        ]

    async def verify_consistency(self):
        query = SimpleStatement(
            f"SELECT v FROM {self.ks}.{self.tbl} WHERE k = 0",
            consistency_level=ConsistencyLevel.ALL,
        )
        rows = await self.cql.run_async(query)
        value = rows[0].v

        total_retries = sum(w.retries for w in self.workers)
        total_uncertainty = sum(w.uncertainty_timeouts for w in self.workers)
        expected = self.num_workers * self.iterations

        logger.info(
            "Final value=%d, expected=%d, retries=%d, uncertainty_timeouts=%d",
            value, expected, total_retries, total_uncertainty,
        )
        assert value == expected, (
            f"value={value}, expected={expected}, "
            f"retries={total_retries}, "
            f"uncertainty_timeouts={total_uncertainty}"
        )


def _cluster_config(tablets_enabled: bool) -> tuple[dict, str]:
    """Return (server config, keyspace options) for the contention scenario."""
    replication = "{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
    if tablets_enabled:
        return (
            {"tablets_mode_for_new_keyspaces": "enabled"},
            f"WITH replication = {replication} AND tablets = {{'initial': 1}}",
        )
    return (
        {"tablets_mode_for_new_keyspaces": "disabled"},
        f"WITH replication = {replication}",
    )


@pytest.fixture(params=[True, False], ids=["tablets", "vnodes"])
async def contention_cluster(request, manager: ManagerClient) -> str:
    """
    Boot a 3-node cluster (dc1/rack1..3, --smp=2, paxos=trace) for LWT contention.

    Parametrized over tablets and vnodes so any test depending on this fixture
    automatically runs against both storage backends.

    Returns the keyspace options string ready for `new_test_keyspace(...)`.
    """
    tablets_enabled = request.param
    cfg, ks_opts = _cluster_config(tablets_enabled)
    cmdline = ['--logger-log-level', 'paxos=trace']
    await manager.servers_add(3, config=cfg, auto_rack_dc="dc1", cmdline=cmdline)
    return ks_opts


async def _run_workload(
    manager: ManagerClient,
    scale_timeout,
    ks_opts: str,
    *,
    num_workers: int,
    iterations: int,
    max_retries_per_iteration: int,
    test_timeout_s: int = 300,
):
    """
    Run the contention workload against an already-booted cluster:
    create keyspace/table, spawn workers, wait for completion, verify.
    """
    async with new_test_keyspace(manager, ks_opts) as ks:
        stop_event = asyncio.Event()
        tester = ContentionLWTTester(
            manager, ks, "test",
            num_workers=num_workers,
            iterations=iterations,
            max_retries_per_iteration=max_retries_per_iteration,
            scale_timeout=scale_timeout,
        )

        await tester.create_schema()
        await tester.initialize_rows()

        start = time.time()
        logger.info(
            "Starting contention scenario: %d workers, %d iterations",
            num_workers, iterations,
        )

        try:
            await tester.start_workers(stop_event)
            await asyncio.wait_for(
                asyncio.gather(*tester._tasks, return_exceptions=False),
                timeout=scale_timeout(test_timeout_s),
            )
        finally:
            await tester.stop_workers()

        logger.info("Contention scenario completed in %.2fs", time.time() - start)
        await tester.verify_consistency()


@pytest.mark.tier2
async def test_lwt_contention_many_workers(manager: ManagerClient, scale_timeout, build_mode, contention_cluster):
    """
    Test many async workers repeatedly contending on the same row via LWT.

    Verifies that under heavy contention (300 workers, 1 iteration each),
    Paxos correctly serializes all conditional updates and the final value
    equals the total number of successful increments (300).

    Runs for both tablets and vnodes to ensure contention handling
    is correct regardless of the storage backend.

    This is a tier2-only test due to the high contention load.
    """
    num_workers = 100 if build_mode == "debug" else 300
    await _run_workload(
        manager, scale_timeout, contention_cluster,
        num_workers=num_workers,
        iterations=1,
        max_retries_per_iteration=500,
    )


@pytest.mark.tier2
async def test_lwt_contention_multi_iterations(manager: ManagerClient, scale_timeout, contention_cluster):
    """
    8 workers × 100 iterations — sustained contention over many CAS rounds.

    Each worker performs 100 CAS+DELETE cycles on the same row. Verifies
    the final value equals num_workers * iterations (800).

    This is a tier2-only test due to the high contention load.
    """
    await _run_workload(
        manager, scale_timeout, contention_cluster,
        num_workers=8,
        iterations=100,
        max_retries_per_iteration=50,
    )
