import asyncio
import logging
import random
import time
import json
from dataclasses import dataclass, asdict
from typing import List, Optional

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.pylib.manager_client import ManagerClient
from test.pylib.tablets import get_tablet_replicas
from test.pylib.rest_client import read_barrier
from test.cluster.util import new_test_keyspace
from test.cluster.conftest import skip_mode

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


THREADS = 10
BACKOFF_BASE = 0.02
WORKLOAD_SEC = 30
POST_MIGRATION_SEC = 5
MIGRATION_TIMEOUT = 120

@dataclass
class LogEntry:
    ts: int
    pid: int
    phase: str
    key: int
    col: int
    applied: Optional[bool]
    prev: Optional[int]
    new: Optional[int]
    err: Optional[str]

# TODO lots of responsibility, maybe somehow split? There are few more cases for split/merge. Somehow prepare for reuse
class MultiColumnLWTTester:
    """
    Encapsulates multi-column LWT testing with comprehensive IF conditions.
    Each worker updates its own column while ensuring global consistency.
    """

    def __init__(self, manager: ManagerClient, ks: str, tbl: str, num_threads: int = THREADS):
        self.manager = manager
        self.ks = ks
        self.tbl = tbl
        self.num_threads = num_threads
        self.cql = manager.get_cql()

        self.history: List[dict] = []
        self.success_counts = [0] * self.num_threads
        self.workers: List[asyncio.Task] = []
        self.stop_event = asyncio.Event()

        self.columns = [f"s{i}" for i in range(self.num_threads)]
        self.select_cols = ", ".join(self.columns)

    async def create_schema(self):
        """Create table with multiple counter columns"""
        cols_def = ", ".join(f"s{i} int" for i in range(self.num_threads))
        await self.cql.run_async(
            f"CREATE TABLE {self.ks}.{self.tbl} (pk int PRIMARY KEY, {cols_def})"
        )
        logger.info(f"Created table {self.ks}.{self.tbl} with {self.num_threads} columns")

    async def initialize_row(self, pk: int = 1):
        """Initialize the test row with all columns set to 0"""
        zeros = ", ".join("0" for _ in range(self.num_threads))
        await self.cql.run_async(
            f"INSERT INTO {self.ks}.{self.tbl} (pk, {self.select_cols}) VALUES ({pk}, {zeros})"
        )
        logger.info(f"Initialized row with pk={pk}")

    async def _worker(self, worker_id: int):
        """
        One LWT worker that:
          - reads the entire row to build its IF-condition
          - tries to CAS-increment its own column
          - handles coordinator timeouts, fast-forwards missed successes,
            and backs off on contention
          - exits only after finishing the current loop when stop_event is set
        """
        rng = random.Random(worker_id)
        backoff = BACKOFF_BASE
        cql = self.cql

        # Statement that fetches the whole row (used on every iteration) ie snapshot of the row
        select_stmt = SimpleStatement(
            f"SELECT {self.select_cols} FROM {self.ks}.{self.tbl} WHERE pk = 1",
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        )

        while True:
            try:
                rows = await cql.run_async(select_stmt)
                row = rows[0] if rows else None
                if row is None:
                    await asyncio.sleep(backoff)
                    continue
                # Fast-forward local counter if a previous timeout was actually committed
                cur_val = getattr(row, f"s{worker_id}") or 0
                if cur_val > self.success_counts[worker_id]:
                    self.success_counts[worker_id] = cur_val

                prev_val = cur_val
                new_val = prev_val + 1

                conds = [
                    f"s{j} >= {getattr(row, f's{j}') or 0}"
                    for j in range(self.num_threads) if j != worker_id    # all other columns
                ]
                conds.append(f"s{worker_id} = {prev_val}")  # current worker column condition
                cond_str = " AND ".join(conds)

                self._log_entry(worker_id, "invoke", prev_val, new_val)

                # CAS request
                update_stmt = SimpleStatement(
                    f"UPDATE {self.ks}.{self.tbl} SET s{worker_id} = {new_val} "
                    f"WHERE pk = 1 IF {cond_str}",
                    consistency_level=ConsistencyLevel.LOCAL_QUORUM,
                    serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
                )

                res = await asyncio.shield(cql.run_async(update_stmt))   # Shield from cancellation

                if res:  # Coordinator replied normally
                    applied = res[0].applied
                    if applied:
                        self.success_counts[worker_id] += 1
                else:
                    # Coordinator timed out, outcome unknown
                    verify_stmt = SimpleStatement(
                        f"SELECT s{worker_id} FROM {self.ks}.{self.tbl} WHERE pk = 1",
                        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
                    )
                    v_row = (await cql.run_async(verify_stmt))[0]
                    cur_val = getattr(v_row, f"s{worker_id}") or 0
                    delta = cur_val - prev_val  # 0 (not applied) or 1 (applied)
                    applied = delta > 0
                    if applied:
                        self.success_counts[worker_id] += delta

                self._log_entry(worker_id, "ok",
                                prev_val,
                                new_val if applied else prev_val,
                                applied=applied)

                if applied:
                    backoff = BACKOFF_BASE
                else:
                    await asyncio.sleep(backoff + rng.random() * backoff)
                    backoff = min(backoff * 2, 0.5)

            except asyncio.CancelledError:
                # shield() guarantees the CAS is finished; re-raise to exit gracefully
                raise
            except Exception as e:
                self._log_entry(worker_id, "fail", error=str(e))
                await asyncio.sleep(backoff)

            if self.stop_event.is_set():
                break

    def _log_entry(self, worker_id: int, phase: str, prev_val: int = None,
                   new_val: int = None, applied: bool = None, error: str = None):
        entry = LogEntry(
            ts=time.time_ns(),
            pid=worker_id,
            phase=phase,
            key=1,  # always pk=1
            col=worker_id,
            applied=applied,
            prev=prev_val,
            new=new_val,
            err=error
        )
        self.history.append(asdict(entry))

    async def start_workers(self):
        self.workers = [
            asyncio.create_task(self._worker(i))
            for i in range(self.num_threads)
        ]
        logger.info(f"Started {self.num_threads} LWT workers")

    async def stop_workers(self):
        self.stop_event.set()
        await asyncio.gather(*self.workers)
        logger.info("All workers stopped")

    async def verify_consistency(self):
        """Verify final state matches successful CAS operations"""
        stmt = SimpleStatement(
            f"SELECT {self.select_cols} FROM {self.ks}.{self.tbl} WHERE pk = 1",
            consistency_level=ConsistencyLevel.LOCAL_QUORUM
        )
        result = await self.cql.run_async(stmt)
        row = result[0] if result else None

        assert row, "Final row not found"

        mismatches = []
        for i in range(self.num_threads):
            actual = getattr(row, f"s{i}") or 0
            expected = self.success_counts[i]
            if actual != expected:
                mismatches.append(f"s{i}={actual}, expected {expected}")

        assert not mismatches, f"Consistency violations: {'; '.join(mismatches)}"

        total_ops = sum(self.success_counts)
        logger.info(f"Consistency verified: {total_ops} total successful operations")
        for i, count in enumerate(self.success_counts):
            logger.info(f"Worker {i}: {count} successful increments")

    def save_history(self, filename: str = "lwt_history.json"):
        # TODO do we need to save hist?
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(self.history, f, indent=2)
        logger.info(f"Saved {len(self.history)} history entries to {filename}")


async def get_non_replica_server(manager: ManagerClient, servers, replicas):
    replica_host_ids = {replica[0] for replica in replicas}
    for server in servers:
        host_id = await manager.get_host_id(server.server_id)
        if host_id not in replica_host_ids:
            return server
    raise ValueError("No non-replica servers found")


async def migrate_tablet(manager: ManagerClient, src_server, dst_server, ks: str, tbl: str, token: int, replicas):
    logger.info(f"Starting tablet migration to {dst_server.ip_addr}")

    dst_host_id = await manager.get_host_id(dst_server.server_id)
    src_replica = replicas[0]

    await manager.api.move_tablet(
        src_server.ip_addr, ks, tbl,
        src_replica[0], src_replica[1],
        dst_host_id, 0, token
    )

    # Wait for migration completion
    start_time = time.time()
    while time.time() - start_time < MIGRATION_TIMEOUT:
        current_replicas = await get_tablet_replicas(manager, src_server, ks, tbl, token)
        if any(repl[0] == dst_host_id for repl in current_replicas):
            logger.info(f"Tablet migration completed to {dst_server.ip_addr}")
            return
        await asyncio.sleep(1)

    raise TimeoutError(f"Tablet migration failed after {MIGRATION_TIMEOUT}s")


@pytest.mark.asyncio
#@skip_mode() # is this needed?
async def test_multi_column_lwt_during_migration(manager: ManagerClient):
    """Test multi-column LWT pattern during tablet migration"""

    # Setup cluster
    cfg = {
        "enable_user_defined_functions": False,
        "tablets_mode_for_new_keyspaces": "enabled"
    }
    property_files = [{"dc": "dc1", "rack": f"rack{(i % 2) + 1}"} for i in range(6)]
    servers = await manager.servers_add(6, config=cfg, property_file=property_files)

    for server in servers:
        await manager.api.disable_tablet_balancing(server.ip_addr)

    async with new_test_keyspace(
        manager,
        "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} "
        "AND tablets = {'initial': 10}"
    ) as ks:

        tester = MultiColumnLWTTester(manager, ks, "lwt_table")
        await tester.create_schema()
        await tester.initialize_row()

        token = 0
        replicas = await get_tablet_replicas(manager, servers[0], ks, tester.tbl, token)
        dst_server = await get_non_replica_server(manager, servers, replicas)

        await tester.start_workers()

        try:
            # Phase 1: Pre-migration workload
            await asyncio.sleep(WORKLOAD_SEC / 3)

            # Phase 2: Migrate during workload
            await migrate_tablet(manager, servers[0], dst_server, ks, tester.tbl, token, replicas)
            await read_barrier(manager.api, servers[0].ip_addr)
            await asyncio.sleep(POST_MIGRATION_SEC)

            # Phase 3: Post-migration workload
            await asyncio.sleep(WORKLOAD_SEC * 2 / 3)

        finally:
            await tester.stop_workers()

        await tester.verify_consistency()
        tester.save_history()

        logger.info("Multi-column LWT during migration test completed successfully")
