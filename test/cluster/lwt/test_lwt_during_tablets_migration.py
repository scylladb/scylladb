import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict

import pytest
from cassandra import ConsistencyLevel
from cassandra import WriteTimeout, OperationTimedOut
from cassandra.query import SimpleStatement, PreparedStatement
from test.cluster.conftest import skip_mode
from test.cluster.util import new_test_keyspace
from test.pylib.manager_client import ManagerClient
from test.pylib.tablets import get_tablet_replicas

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# Arbitrary constants for the test
WORKERS = 20
BACKOFF_BASE = 0.02
WORKLOAD_SEC = 30
POST_MIGRATION_SEC = 5
NUM_KEYS = 50


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

    def __init__(self, manager: ManagerClient, ks: str, tbl: str, num_workers: int = WORKERS, num_keys: int = NUM_KEYS,):
        self.manager = manager
        self.ks = ks
        self.tbl = tbl
        self.num_workers = num_workers
        self.cql = manager.get_cql()

        self.history: List[dict] = []

        self.pks: List[int] = list(range(1, num_keys + 1))

        self.success_counts: Dict[int, List[int]] = {
            pk: [0] * self.num_workers for pk in self.pks
        }

        self.workers: List[asyncio.Task] = []
        self.stop_event = asyncio.Event()

        self.columns = [f"s{i}" for i in range(self.num_workers)]
        self.select_cols = ", ".join(self.columns)

        self.ps_select_row: Optional[PreparedStatement] = None
        self.ps_update: List[Optional[PreparedStatement]] = [None] * self.num_workers
        self.others_per_worker: List[List[int]] = [
            [col_index for col_index in range(self.num_workers) if col_index != worker_index] for worker_index in range(self.num_workers)
        ]
        self.rngs = [random.Random(i) for i in range(self.num_workers)]

    async def prepare_statements(self):
        """Prepare CQL statements for row selection and updates"""
        self.ps_select_row = self.cql.prepare(
            f"SELECT {self.select_cols} FROM {self.ks}.{self.tbl} WHERE pk = ?"
        )

        for i in range(self.num_workers):
            others = self.others_per_worker[i]
            cond = " AND ".join([f"s{j} >= ?" for j in others] + [f"s{i} = ?"])
            query = (
                f"UPDATE {self.ks}.{self.tbl} SET s{i} = ? "
                f"WHERE pk = ? IF {cond}"
            )
            self.ps_update[i] = self.cql.prepare(query)

    async def create_schema(self):
        """Create table with multiple counter columns"""
        cols_def = ", ".join(f"s{i} int" for i in range(self.num_workers))
        await self.cql.run_async(
            f"CREATE TABLE {self.ks}.{self.tbl} (pk int PRIMARY KEY, {cols_def})"
        )
        logger.info(f"Created table {self.ks}.{self.tbl} with {self.num_workers} columns")

    async def initialize_rows(self):
        """Initialize the test row with all columns set to 0"""
        zeros = ", ".join("0" for _ in range(self.num_workers))
        for pk in self.pks:  # self.pks = [1..NUM_KEYS]
            await self.cql.run_async(
                f"INSERT INTO {self.ks}.{self.tbl} "
                f"(pk, {self.select_cols}) VALUES ({pk}, {zeros})"
            )

    async def _worker(self, worker_id: int):

        rng = self.rngs[worker_id]
        backoff = BACKOFF_BASE

        while not self.stop_event.is_set():
            pk = rng.choice(self.pks)
            if self.stop_event.is_set():
                break
            verify_query = self.ps_select_row.bind([pk])
            verify_query.consistency_level = ConsistencyLevel.LOCAL_QUORUM
            rows = await self.cql.run_async(verify_query)
            if not rows:
                # do we need to handle this case?
                await asyncio.sleep(backoff); backoff = min(backoff * 2, 0.5); continue

            row = rows[0]
            prev_val = getattr(row, f"s{worker_id}")
            new_val = prev_val + 1

            others = self.others_per_worker[worker_id]
            if_vals = [getattr(row, f"s{col_idx}") for col_idx in others] + [prev_val]
            params = [new_val, pk] + if_vals

            update = self.ps_update[worker_id].bind(params)
            update.consistency_level = ConsistencyLevel.LOCAL_QUORUM
            update.serial_consistency_level = ConsistencyLevel.LOCAL_SERIAL

            try:
                res = await self.cql.run_async(update)
                applied = bool(res and res[0].applied)
            except (WriteTimeout, OperationTimedOut) as e:
                self._log_entry(worker_id, "fail", error=str(e), pk=pk)
                verify_after_cas_error = self.ps_select_row.bind([pk])
                verify_after_cas_error.consistency_level = ConsistencyLevel.LOCAL_QUORUM
                vrow = (await self.cql.run_async(verify_after_cas_error))[0]
                applied = (getattr(vrow, f"s{worker_id}") == new_val)
            except asyncio.CancelledError:
                raise

            if applied:
                self.success_counts[pk][worker_id] += 1
                backoff = BACKOFF_BASE
            else:

                sleep = backoff + rng.random() * backoff
                await asyncio.sleep(sleep)
                backoff = min(backoff * 2, 0.5)

            if self.stop_event.is_set():
                break

    def _log_entry(self, worker_id: int, phase: str, prev_val: int = None,
                   new_val: int = None, applied: bool = None, error: str = None, pk: int = None):
            entry = LogEntry(
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
            self.history.append(asdict(entry))

    async def start_workers(self):
        self.workers = [
            asyncio.create_task(self._worker(i))
            for i in range(self.num_workers)
        ]
        logger.info(f"Started {self.num_workers} LWT workers")

    async def stop_workers(self):
        self.stop_event.set()
        done, pending = await asyncio.wait(self.workers, timeout=5)
        for t in pending:
            t.cancel()
        await asyncio.gather(*self.workers, return_exceptions=True)
        logger.info("All workers stopped")

    async def verify_consistency(self):
        """Ensure every (pk, column) reflects the number of successful CAS writes."""
        mismatches = []
        for pk in self.pks:
            stmt = SimpleStatement(
                f"SELECT {self.select_cols} FROM {self.ks}.{self.tbl} WHERE pk = %s",
                consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            )
            row = (await self.cql.run_async(stmt, [pk]))[0]
            for col_idx in range(self.num_workers):
                actual = getattr(row, f"s{col_idx}")
                expected = self.success_counts[pk][col_idx]
                if actual != expected:
                    mismatches.append(
                        f"pk={pk} s{col_idx}={actual}, expected {expected}"
                    )

        assert not mismatches, "Consistency violations: " + "; ".join(mismatches)
        total_ops = sum(sum(v) for v in self.success_counts.values())
        logger.info("Consistency verified – %d total successful CAS operations", total_ops)

    def save_history(self, filename: str = "lwt_history.json"):
        # TODO do we need to save hist? By now using for debugging only
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

    current_replicas = await get_tablet_replicas(manager, src_server, ks, tbl, token)
    assert any(repl[0] == dst_host_id for repl in current_replicas), "Tablet migration failed"
    logger.info(f"Tablet migration completed to {dst_server.ip_addr}")


async def get_token_for_pk(cql, ks: str, tbl: str, pk: int) -> int:
    stmt = SimpleStatement(
        f"SELECT token(pk) AS tk FROM {ks}.{tbl} WHERE pk = %s",
        consistency_level=ConsistencyLevel.ONE,
    )
    row = (await cql.run_async(stmt, [pk]))[0]
    return row.tk


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
@skip_mode('debug', 'debug mode is too slow for this test')
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
        "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND tablets = {'initial': 5}"
    ) as ks:

        tester = MultiColumnLWTTester(manager, ks, "lwt_table")
        await tester.create_schema()
        await tester.initialize_rows()
        await tester.prepare_statements()
        await tester.start_workers()

        try:
            # Phase 1: Pre-migration workload
            await asyncio.sleep(WORKLOAD_SEC / 3)
            hot_pk = max(tester.success_counts, key=lambda k: sum(tester.success_counts[k]))
            token = await get_token_for_pk(tester.cql, ks, tester.tbl, hot_pk)
            replicas = await get_tablet_replicas(manager, servers[0], ks, tester.tbl, token)
            dst_server = await get_non_replica_server(manager, servers, replicas)

            # Phase 2: Migrate during workload
            await migrate_tablet(manager, servers[0], dst_server, ks, tester.tbl, token, replicas)
            await asyncio.sleep(POST_MIGRATION_SEC)

            # Phase 3: Post-migration workload
            await asyncio.sleep(WORKLOAD_SEC * 2 / 3)

        finally:
            await tester.stop_workers()

        await tester.verify_consistency()
        tester.save_history()

        logger.info("Multi-column LWT during migration test completed successfully")
