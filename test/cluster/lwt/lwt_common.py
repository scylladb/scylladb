# Copyright (C) 2024-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
"""
Common functionality for LWT (Lightweight Transaction)
This module provides shared components for testing LWT behavior during various tablet operations
like migrations, splits, merges, etc.
"""

import asyncio
import logging
import random
import re
import time
from functools import cached_property
from functools import wraps
from typing import List, Dict, Callable

from cassandra import ConsistencyLevel
from cassandra import WriteTimeout, ReadTimeout, OperationTimedOut
from cassandra.query import SimpleStatement, PreparedStatement
from test.pylib.manager_client import ManagerClient
from test.pylib.tablets import get_tablet_count

logger = logging.getLogger(__name__)

# Test constants (arbitrary values)
DEFAULT_WORKERS = 20
DEFAULT_BACKOFF_BASE = 0.02
DEFAULT_NUM_KEYS = 50
UNCERTAINTY_RE = re.compile(r"write timeout due to uncertainty", re.IGNORECASE)




def backoff_on_exception(timeout, between_sleep, retry_exceptions, should_retry):
    """
    Decorator for retrying async functions with backoff on specified exceptions.
    """
    def decorator(target):
        @wraps(target)
        async def wrapper(*args, **kwargs):
            event_loop = asyncio.get_event_loop()
            start_time = event_loop.time()
            attempt = 1
            while True:
                try:
                    return await target(*args, **kwargs)
                except Exception as e:
                    if retry_exceptions and not isinstance(e, retry_exceptions):
                        raise
                    if should_retry and not should_retry(e):
                        raise
                    elapsed = event_loop.time() - start_time
                    if elapsed > timeout:
                        raise TimeoutError(f"Operation timed out after {timeout} seconds and {attempt} attempts") from e
                    await asyncio.sleep(between_sleep)
                    attempt += 1
        return wrapper
    return decorator


class Worker:
    """
    A single worker increments its dedicated column `s{i}` via LWT:
      UPDATE .. SET s{i}=? WHERE pk=? IF <guards on other cols> AND s{i}=?
    It checks for applied state and retries on "uncertainty" timeouts.
    """
    def __init__(
        self,
        worker_id: int,
        cql,
        pks: List[int],
        select_statement: PreparedStatement,
        update_statement: PreparedStatement,
        other_columns: List[int],
        get_lower_bound: Callable[[int, int], int],
        on_applied: Callable[[int, int, int], None],
        stop_event: asyncio.Event

    ):
        self.stop_event = stop_event
        self.success_counts: Dict[int, int] = {pk: 0 for pk in pks}
        self.worker_id = worker_id
        # seed for reproducibility per worker
        self.rng = random.Random(worker_id)
        self.select_statement = select_statement
        self.update_statement = update_statement
        self.other_columns = other_columns
        self.pks = pks
        self.cql = cql
        self.get_lower_bound = get_lower_bound
        self.on_applied = on_applied


    async def verify_update_through_select(self, pk, new_val, prev_val):
        """
        When CAS returns "uncertainty timeout", it's unclear if the update was applied.
        Re-read the row at LOCAL_SERIAL consistency and check the current value.
        """
        verify_stmt = self.select_statement.bind([pk])
        verify_stmt.consistency_level = ConsistencyLevel.LOCAL_SERIAL
        rows = await self.cql.run_async(verify_stmt)
        vrow = rows[0]
        current_val = getattr(vrow, f"s{self.worker_id}")
        assert current_val == new_val or current_val == prev_val
        return current_val == new_val


    def stop(self) -> None:
        self.stop_event.set()

    async def __call__(self):
        """
        Worker loop:
          - pick random pk
          - read current row
          - compute guards
          - execute CAS update
          - handle applied/uncertainty/timeout cases
        """
        while not self.stop_event.is_set():
            try:
                pk = self.rng.choice(self.pks)

                # Read current values
                verify_query = self.select_statement.bind([pk])
                verify_query.consistency_level = ConsistencyLevel.LOCAL_QUORUM
                rows = await self.cql.run_async(verify_query)

                row = rows[0]
                prev_val = getattr(row, f"s{self.worker_id}")
                expected = self.success_counts[pk]
                new_val = expected + 1

                # Verify consistency before update
                assert prev_val == expected, (
                    f"Consistency mismatch: pk={pk} s{self.worker_id} row={prev_val} tracker={expected}"
                )

                guard_vals = [
                    max(getattr(row, f"s{col_idx}"), self.get_lower_bound(pk, col_idx))
                    for col_idx in self.other_columns
                ]
                # Prepare conditional update
                update = self.update_statement.bind([new_val, pk, *guard_vals, prev_val])
                update.consistency_level = ConsistencyLevel.LOCAL_QUORUM
                update.serial_consistency_level = ConsistencyLevel.LOCAL_SERIAL
                try:
                    res = await self.cql.run_async(update)
                    applied = bool(res and res[0].applied)
                    assert applied, f"LWT not applied: pk={pk} s{self.worker_id} new={new_val} guard={guard_vals} prev={prev_val}"
                except (WriteTimeout, OperationTimedOut, ReadTimeout) as e:
                    if not is_uncertainty_timeout(e):
                        raise
                    applied = await self.verify_update_through_select(pk, new_val, prev_val)

                if applied:
                    self.on_applied(pk, self.worker_id, new_val)
                    self.success_counts[pk] += 1

                await asyncio.sleep(0.1)

            except Exception:
                self.stop()
                raise

        logger.info("Worker finished")


class BaseLWTTester:
    """
    Orchestrates a multi-column LWT workload.
    Creates schema, spawns workers, collects results, verifies consistency.
    """

    def __init__(
            self, manager: ManagerClient, ks: str, tbl: str,
            num_workers: int = DEFAULT_WORKERS, num_keys: int = DEFAULT_NUM_KEYS
    ):
        self.ks = ks
        self.tbl = tbl
        self.cql = manager.get_cql()
        self.num_workers = num_workers
        self.pks = list(range(1, num_keys + 1))
        self.select_cols = ", ".join(f"s{i}" for i in range(self.num_workers))
        self.workers: List[Worker] = []
        self._tasks: List[asyncio.Task] = []
        self.lb_counts: Dict[int, List[int]] = {pk: [0] * self.num_workers for pk in self.pks}
        self.pk_to_token: Dict[int, int] = {}
        self.migrations = 0
        self.phase = "warmup"  # "warmup" -> "migrating" -> "post"
        self.phase_ops = {"warmup": 0, "migrating": 0, "post": 0}

    def _get_lower_bound(self, pk: int, col_idx: int) -> int:
        return self.lb_counts[pk][col_idx]

    def _on_applied(self, pk: int, col_idx: int, new_val: int) -> None:
        self.lb_counts[pk][col_idx] = max(self.lb_counts[pk][col_idx], new_val)
        self.phase_ops[self.phase] += 1

    def set_phase(self, phase: str) -> None:
        self.phase = phase

    def get_phase_ops(self, phase: str) -> int:
        return self.phase_ops.get(phase, 0)

    async def wait_for_phase_ops(self, stop_event, phase: str, target: int, timeout: float = 120.0, poll: float = 0.2):
        deadline = time.time() + timeout
        while time.time() < deadline and not stop_event.is_set():
            if self.get_phase_ops(phase) >= target:
                return
            await asyncio.sleep(poll)
        raise asyncio.TimeoutError(
            f"phase '{phase}' did not reach {target} ops in time (have {self.get_phase_ops(phase)})")

    @cached_property
    def select_statement(self) -> PreparedStatement:
        return self.cql.prepare(
            f"SELECT {self.select_cols} FROM {self.ks}.{self.tbl} WHERE pk = ?"
        )

    def create_workers(self, stop_event) -> List[Worker]:
        workers: List[Worker] = []
        for i in range(self.num_workers):
            other_columns = [j for j in range(self.num_workers) if j != i]
            cond = " AND ".join([*(f"s{j} >= ?" for j in other_columns), f"s{i} = ?"])
            query = f"UPDATE {self.ks}.{self.tbl} SET s{i} = ? WHERE pk = ? IF {cond}"
            worker = Worker(
                stop_event=stop_event,
                worker_id=i,
                cql=self.cql,
                pks=self.pks,
                select_statement=self.select_statement,
                update_statement=self.cql.prepare(query),
                other_columns=other_columns,
                get_lower_bound=self._get_lower_bound,
                on_applied=self._on_applied,
            )
            workers.append(worker)
        return workers

    async def create_schema(self):
        """Create table with multiple counter columns"""
        cols_def = ", ".join(f"s{i} int" for i in range(self.num_workers))
        await self.cql.run_async(
            f"CREATE TABLE {self.ks}.{self.tbl} (pk int PRIMARY KEY, {cols_def})"
        )
        logger.info("Created table %s.%s with %d columns", self.ks, self.tbl, self.num_workers)

    async def initialize_rows(self):
        """
        Insert initial rows with all columns set to 0.
        Then precompute and cache pk->token mapping.
        Both inserts and selects are run in parallel.
        """
        zeros = ", ".join("0" for _ in range(self.num_workers))
        ps = self.cql.prepare(
            f"INSERT INTO {self.ks}.{self.tbl} (pk, {self.select_cols}) VALUES (?, {zeros})"
        )

        insert_tasks = [self.cql.run_async(ps.bind([pk])) for pk in self.pks]
        await asyncio.gather(*insert_tasks)

        token_tasks = [get_token_for_pk(self.cql, self.ks, self.tbl, pk) for pk in self.pks]
        tokens = await asyncio.gather(*token_tasks)

        for pk, token in zip(self.pks, tokens):
            self.pk_to_token[pk] = token

    async def start_workers(self, stop_event):
        """Start workload workers"""
        self.workers = self.create_workers(stop_event)
        self._tasks = [asyncio.create_task(worker()) for worker in self.workers]
        logger.info("Started %d LWT workers", len(self._tasks))

    async def stop_workers(self):
        """Stop workload workers and surface exceptions"""
        for worker in self.workers:
            worker.stop()
        if self._tasks:
            results = await asyncio.gather(*self._tasks, return_exceptions=True)
            errs = [e for e in results if isinstance(e, Exception)]
            self._tasks.clear()
            assert not errs, f"worker errors: {errs}"
        logger.info("All workers stopped")

    async def verify_consistency(self):
        """Ensure every (pk, column) reflects the number of successful CAS writes."""
        # Run SELECTs for all PKs in parallel using prepared statement
        tasks = []
        for pk in self.pks:
            stmt = self.select_statement.bind([pk])
            stmt.consistency_level = ConsistencyLevel.LOCAL_QUORUM
            tasks.append(self.cql.run_async(stmt))

        results = await asyncio.gather(*tasks)

        mismatches = []
        for pk, rows in zip(self.pks, results):
            row = rows[0]
            for i, worker in enumerate(self.workers):
                actual = getattr(row, f"s{i}")
                expected = worker.success_counts[pk]
                if actual != expected:
                    mismatches.append(f"pk={pk} s{i}={actual}, expected {expected}")

        assert not mismatches, "Consistency violations: " + "; ".join(mismatches)
        total_ops = sum(sum(w.success_counts.values()) for w in self.workers)
        logger.info("Consistency verified – %d total successful CAS operations", total_ops)


async def get_token_for_pk(cql, ks: str, tbl: str, pk: int) -> int:
    """Get the token for a given primary key"""
    stmt = SimpleStatement(
        f"SELECT token(pk) AS tk FROM {ks}.{tbl} WHERE pk = %s",
        consistency_level=ConsistencyLevel.QUORUM,
    )
    row = (await cql.run_async(stmt, [pk]))[0]
    return row.tk


async def get_host_map(manager: ManagerClient, servers):
    """Create a mapping from host IDs to server info"""
    ids = await asyncio.gather(*[manager.get_host_id(s.server_id) for s in servers])
    return {hid: srv for hid, srv in zip(ids, servers)}


async def pick_non_replica_server(manager: ManagerClient, servers, replica_host_ids):
    """Find a random server that is not a replica for the given tablet"""
    host_map = await get_host_map(manager, servers)
    non_replicas = [srv for hid, srv in host_map.items() if hid not in replica_host_ids]
    return random.choice(non_replicas)


async def wait_for_tablet_count(
        manager: ManagerClient, server, ks: str, tbl: str,
        predicate, target: int, timeout_s: int = 180, poll_s: float = 1.0
    ):
    """
    Wait for tablet count to match predicate.
    predicate: callable like lambda c: c >= target (for split) or lambda c: c <= target (for merge)
    """
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        count = await get_tablet_count(manager, server, ks, tbl)
        last = count
        if predicate(count):
            return count
        await asyncio.sleep(poll_s)
    raise TimeoutError(f"tablet count wait timed out (last={last}, target={target})")


def is_uncertainty_timeout(exc: Exception) -> bool:
    return isinstance(exc, (WriteTimeout, ReadTimeout)) and UNCERTAINTY_RE.search(str(exc)) is not None
