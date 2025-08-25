"""
Common functionality for LWT (Lightweight Transaction)
This module provides shared components for testing LWT behavior during various tablet operations
like migrations, splits, merges, etc.
"""

import asyncio
import json
import logging
import random
import re
import time
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict

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


@dataclass
class LogEntry:
    """Represents a single operation log entry for tracking LWT operations"""
    ts: int
    pid: int
    phase: str
    key: int
    col: int
    applied: Optional[bool]
    prev: Optional[int]
    new: Optional[int]
    err: Optional[str]
    operation_id: int = 0


@dataclass
class LWTNonApplyError(RuntimeError):
    pk: int
    worker_id: int
    prev_val: object
    new_val: object
    operation_id: str
    payload: dict

    def __str__(self) -> str:
        return (f"Unexpected CAS non-apply without timeout: "
                f"pk={self.pk} worker={self.worker_id} prev={self.prev_val}, new={self.new_val}")


class ResultTracker:
    """Tracks operation results and maintains consistency counters for LWT operations"""

    def __init__(self, num_workers: int, num_keys: int):
        self.num_workers = num_workers
        self.num_keys = num_keys
        self.pks: List[int] = list(range(1, num_keys + 1))
        self.success_counts: Dict[int, List[int]] = {
            pk: [0] * self.num_workers for pk in self.pks
        }
        self.history: List[dict] = []

    def increment_success(self, pk: int, worker_id: int):
        """Increment success count for a specific worker and primary key"""
        self.success_counts[pk][worker_id] += 1

    def get_success_count(self, pk: int, worker_id: int) -> int:
        """Get current success count for a worker and primary key"""
        return self.success_counts[pk][worker_id]

    def log_entry(self, worker_id: int, phase: str, prev_val: int = None,
                  new_val: int = None, applied: bool = None, error: str = None,
                  pk: int = None, operation_id: int = None):
        """Log an operation entry"""
        entry = LogEntry(
            operation_id=operation_id or 0,
            ts=time.time_ns(),
            pid=worker_id,
            phase=phase,
            key=pk,
            col=worker_id,
            applied=applied,
            prev=prev_val,
            new=new_val,
            err=error
        )
        return asdict(entry)


class SchemaManager:
    """Manages database schema creation and initialization for LWT tests"""

    def __init__(self, manager: ManagerClient, ks: str, tbl: str, num_workers: int, num_keys: int = DEFAULT_NUM_KEYS):
        self.manager = manager
        self.ks = ks
        self.tbl = tbl
        self.num_workers = num_workers
        self.num_keys = num_keys
        self.columns = [f"s{i}" for i in range(self.num_workers)]
        self.select_cols = ", ".join(self.columns)
        self.cql = manager.get_cql()
        self.pks: List[int] = list(range(1, num_keys + 1))
        # Marker for static layout in subclasses
        self.uses_static = False

    async def create_schema(self):
        """Create table with multiple counter columns"""
        cols_def = ", ".join(f"s{i} int" for i in range(self.num_workers))
        await self.cql.run_async(
            f"CREATE TABLE {self.ks}.{self.tbl} (pk int PRIMARY KEY, {cols_def})"
        )
        logger.info("Created table %s.%s with %d columns", self.ks, self.tbl, self.num_workers)

    async def initialize_rows(self):
        """Initialize the test rows with all columns set to 0"""
        zeros = ", ".join("0" for _ in range(self.num_workers))
        ps = self.cql.prepare(
            f"INSERT INTO {self.ks}.{self.tbl} (pk, {self.select_cols}) VALUES (?, {zeros})"
        )
        for pk in self.pks:
            await self.cql.run_async(ps.bind([pk]))


class WorkloadManager:
    """Manages LWT workload execution with multiple concurrent workers"""

    def __init__(self, manager: ManagerClient, schema: SchemaManager, tracker: ResultTracker):
        self.manager = manager
        self.schema = schema
        self.tracker = tracker
        self.cql = manager.get_cql()
        self.workers: List[asyncio.Task] = []
        self.stop_event = asyncio.Event()

        # Prepared statements
        self.ps_select_row: Optional[PreparedStatement] = None
        self.ps_update: List[Optional[PreparedStatement]] = [None] * schema.num_workers

        # Worker configuration
        self.others_per_worker: List[List[int]] = [
            [col_index for col_index in range(schema.num_workers) if col_index != worker_index]
            for worker_index in range(schema.num_workers)
        ]
        self.rngs = [random.Random(i) for i in range(schema.num_workers)]

    def prepare_statements(self):
        """Prepare CQL statements for row selection and updates"""
        self.ps_select_row = self.cql.prepare(
            f"SELECT {self.schema.select_cols} FROM {self.schema.ks}.{self.schema.tbl} WHERE pk = ?"
        )

        for i in range(self.schema.num_workers):
            others = self.others_per_worker[i]
            cond = " AND ".join([f"s{j} >= ?" for j in others] + [f"s{i} = ?"])
            query = (
                f"UPDATE {self.schema.ks}.{self.schema.tbl} SET s{i} = ? "
                f"WHERE pk = ? IF {cond}"
            )
            self.ps_update[i] = self.cql.prepare(query)

    async def _worker(self, worker_id: int):
        """Worker that performs LWT operations"""
        rng = self.rngs[worker_id]
        operation_id = 0
        while not self.stop_event.is_set():
            operation_id += 1
            pk = rng.choice(self.schema.pks)

            # Read current values
            verify_query = self.ps_select_row.bind([pk])
            verify_query.consistency_level = ConsistencyLevel.LOCAL_QUORUM
            rows = await self.cql.run_async(verify_query)

            row = rows[0]
            prev_val = getattr(row, f"s{worker_id}")
            expected = self.tracker.get_success_count(pk, worker_id)
            new_val = expected + 1

            # Verify consistency before update
            assert prev_val == expected, (
                f"tracker mismatch: pk={pk} s{worker_id} row={prev_val} tracker={expected}"
            )

            # Prepare conditional update
            others = self.others_per_worker[worker_id]
            if_vals = [getattr(row, f"s{col_idx}") for col_idx in others] + [prev_val]
            params = [new_val, pk] + if_vals

            update = self.ps_update[worker_id].bind(params)
            update.consistency_level = ConsistencyLevel.LOCAL_QUORUM
            update.serial_consistency_level = ConsistencyLevel.LOCAL_SERIAL
            applied = False
            try:
                res = await self.cql.run_async(update)
                applied = bool(res and res[0].applied)
            except (WriteTimeout, OperationTimedOut, ReadTimeout) as e:
                if not is_uncertainty_timeout(e):
                    raise
                verify_stmt = self.ps_select_row.bind([pk])
                verify_stmt.consistency_level = ConsistencyLevel.LOCAL_SERIAL
                # TODO: not sure how to simplify this retry logic
                while True:
                    try:
                        rows = await self.cql.run_async(verify_stmt)
                        vrow = rows[0]
                        current_val = getattr(vrow, f"s{worker_id}")
                        assert current_val == new_val or current_val == prev_val
                        applied = (current_val == new_val)
                        break
                    except (WriteTimeout, OperationTimedOut, ReadTimeout):
                        await asyncio.sleep(0.05)
                        continue

            if applied:
                self.tracker.increment_success(pk, worker_id)
            else:
                entry = self.tracker.log_entry(
                    worker_id, phase="non_apply_no_timeout", prev_val=prev_val, new_val=new_val,
                    applied = applied, error = None, pk=pk, operation_id=operation_id
                )
                raise LWTNonApplyError(pk=pk, worker_id=worker_id, prev_val=prev_val,
                                       new_val=new_val, operation_id=operation_id, payload=entry)

            if self.stop_event.is_set():
                break

    async def start_workers(self):
        """Start all worker tasks"""
        self.workers = [
            asyncio.create_task(self._worker(i))
            for i in range(self.schema.num_workers)
        ]
        logger.info("Started %d LWT workers", self.schema.num_workers)

    async def stop_workers(self):
        """Stop all worker tasks"""
        self.stop_event.set()
        results = await asyncio.gather(*self.workers, return_exceptions=True)
        errs = [e for e in results if isinstance(e, Exception)]
        assert not errs, f"worker errors: {errs}"
        logger.info("All workers stopped")

    async def verify_consistency(self):
        """Ensure every (pk, column) reflects the number of successful CAS writes."""
        mismatches = []
        limit_clause = " LIMIT 1" if getattr(self.schema, "uses_static", False) else ""
        for pk in self.schema.pks:
            stmt = SimpleStatement(
                f"SELECT {self.schema.select_cols} FROM {self.schema.ks}.{self.schema.tbl} WHERE pk = %s{limit_clause}",
                consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            )
            row = (await self.cql.run_async(stmt, [pk]))[0]
            for col_idx in range(self.schema.num_workers):
                actual = getattr(row, f"s{col_idx}")
                expected = self.tracker.get_success_count(pk, col_idx)
                if actual != expected:
                    mismatches.append(
                        f"pk={pk} s{col_idx}={actual}, expected {expected}"
                    )

        assert not mismatches, "Consistency violations: " + "; ".join(mismatches) if mismatches else ""
        total_ops = sum(sum(v) for v in self.tracker.success_counts.values())
        logger.info("Consistency verified – %d total successful CAS operations", total_ops)


class BaseLWTTester:
    """
    Base class for coordinating multi-column LWT testing using dedicated components.
    """
    def __init__(self, manager: ManagerClient, ks: str, tbl: str,
                 num_workers: int = DEFAULT_WORKERS, num_keys: int = DEFAULT_NUM_KEYS):
        self.schema = SchemaManager(manager, ks, tbl, num_workers, num_keys)
        self.tracker = ResultTracker(num_workers, num_keys)
        self.workload = WorkloadManager(manager, self.schema, self.tracker)
        self.ks = ks
        self.tbl = tbl
        self.cql = manager.get_cql()
        self.success_counts = self.tracker.success_counts

    async def create_schema(self):
        """Create database schema"""
        await self.schema.create_schema()

    async def initialize_rows(self):
        """Initialize table rows"""
        await self.schema.initialize_rows()

    def prepare_statements(self):
        """Prepare CQL statements"""
        self.workload.prepare_statements()

    async def start_workers(self):
        """Start workload workers"""
        await self.workload.start_workers()

    async def stop_workers(self):
        """Stop workload workers"""
        await self.workload.stop_workers()

    async def verify_consistency(self):
        """Verify data consistency"""
        await self.workload.verify_consistency()


# Utility functions for tablet operations
async def get_token_for_pk(cql, ks: str, tbl: str, pk: int) -> int:
    """Get the token for a given primary key"""
    stmt = SimpleStatement(
        f"SELECT token(pk) AS tk FROM {ks}.{tbl} WHERE pk = %s",
        consistency_level=ConsistencyLevel.ONE,
    )
    row = (await cql.run_async(stmt, [pk]))[0]
    return row.tk


async def get_host_map(manager: ManagerClient, servers):
    """Create a mapping from host IDs to server info"""
    ids = await asyncio.gather(*[manager.get_host_id(s.server_id) for s in servers])
    return {hid: srv for hid, srv in zip(ids, servers)}


async def pick_non_replica_server(manager: ManagerClient, servers, replica_host_ids):
    """Find a server that is not a replica for the given tablet"""
    for s in servers:
        hid = await manager.get_host_id(s.server_id)
        if hid not in replica_host_ids:
            return s
    return None


async def wait_for_tablet_count(manager: ManagerClient, server, ks: str, tbl: str,
                                predicate, target: int, timeout_s: int = 180, poll_s: float = 1.0):
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
