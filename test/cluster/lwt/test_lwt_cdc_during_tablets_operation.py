import asyncio
import contextlib
import logging
import random
from dataclasses import dataclass
from enum import IntEnum
from typing import Dict, List, Tuple, Optional

import pytest
from cassandra.query import SimpleStatement
from cassandra.util import datetime_from_uuid1

from test.cluster.conftest import skip_mode
from test.cluster.lwt.lwt_common import (
    BaseLWTTester,
    DEFAULT_WORKERS,
    DEFAULT_NUM_KEYS,
)
#from test.cluster.lwt.test_lwt_during_tablets_migration import tablet_migration_ops
from test.cluster.lwt.test_lwt_with_counters_during_tablets_resize_and_migrations import tablet_migration_ops
from test.cluster.lwt.test_lwt_during_tablets_resize import run_random_resizes
from test.cluster.util import new_test_keyspace
from test.pylib.manager_client import ManagerClient
from test.pylib.tablets import get_tablet_count, get_tablet_replicas


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

PHASE_WARMUP = "warmup"
PHASE_CHAOS = "chaos"
PHASE_POST = "post"

WARMUP_LWT_CNT = 100
POST_LWT_CNT = 100
TARGET_RESIZE_COUNT = 12
TARGET_MIGRATIONS = 20

MIN_TABLETS = 1
MAX_TABLETS = 20
RESIZE_TIMEOUT = 240


class CdcOperation(IntEnum):
    """
    CDC operation types from cdc$operation column
    https://github.com/scylladb/scylladb/blob/master/docs/features/cdc/cdc-log-table.rst#operation-column
    """
    PREIMAGE = 0
    UPDATE = 1
    INSERT = 2
    ROW_DELETE = 3
    PARTITION_DELETE = 4
    RANGE_DELETE_INCLUSIVE_LEFT_BOUND = 5
    RANGE_DELETE_EXCLUSIVE_LEFT_BOUND = 6
    RANGE_DELETE_INCLUSIVE_RIGHT_BOUND = 7
    RANGE_DELETE_EXCLUSIVE_RIGHT_BOUND = 8
    POSTIMAGE = 9


def powers_of_two_in_range(lo: int, hi: int):
    if lo > hi or hi < 1:
        return []
    lo = max(1, lo)
    start_e = (lo - 1).bit_length()
    end_e = hi.bit_length()
    return [1 << e for e in range(start_e, end_e + 1) if (1 << e) <= hi]


class CdcLWTTester(BaseLWTTester):
    async def create_schema(self):
        cols_def = ", ".join(f"s{i} int" for i in range(self.num_workers))

        await self.cql.run_async(
            f"CREATE TABLE {self.ks}.{self.tbl} (pk int PRIMARY KEY, {cols_def}) "
            "WITH cdc = {'enabled': true, 'preimage': true, 'postimage': true}"
        )

        # if self.use_counters:
        #     await self.cql.run_async(
        #         f"CREATE TABLE {self.ks}.{self.counter_tbl} (pk int PRIMARY KEY, c counter)"
        #     )

    async def truncate_cdc_log(self):
        await self.cql.run_async(f"TRUNCATE TABLE {self.ks}.{self.tbl}_scylla_cdc_log")


@dataclass(frozen=True)
class CdcDeltaEvent:
    pk: int
    stream_id: bytes
    cdc_time: object  # uuid.UUID
    batch_seq_no: int
    col_idx: int
    new_val: int


async def verify_cdc(manager: ManagerClient, servers, tester: CdcLWTTester):
    ks, tbl = tester.ks, tester.tbl
    log_tbl = f"{tbl}_scylla_cdc_log"

    total_expected = sum(sum(w.success_counts.values()) for w in tester.workers)
    logger.info("CDC verify: expected delta updates=%d", total_expected)

    # collect stream_ids from system.cdc_streams_state and system.cdc_streams_history
    log_table_id = await manager.get_table_id(ks, log_tbl)
    stream_rows = await tester.cql.run_async(
        f"SELECT stream_id FROM system.cdc_streams_state WHERE table_id={log_table_id}"
    )
    hist_rows = await tester.cql.run_async(
        f"SELECT stream_id FROM system.cdc_streams_history WHERE table_id={log_table_id}"
    )
    all_streams = list({r.stream_id for r in (stream_rows + hist_rows)})

    # read CDC log entries for all streams
    select_stmt = tester.cql.prepare(
        f'SELECT * FROM {ks}.{log_tbl} WHERE "cdc$stream_id"=?'
    )

    rows_nested = await asyncio.gather(
        *[tester.cql.run_async(select_stmt.bind([sid])) for sid in all_streams]
    )
    rows = [r for part in rows_nested for r in part]

    # filter pks, not sure if needed
    pk_set = set(tester.pks)
    rows = [r for r in rows if r.pk in pk_set]

    # stream sets
    ts_rows = await tester.cql.run_async(
        f"SELECT timestamp FROM system.cdc_timestamps "
        f"WHERE keyspace_name='{ks}' AND table_name='{tbl}' ORDER BY timestamp ASC"
    )
    timestamps = [r.timestamp for r in ts_rows]
    assert timestamps, "no cdc_timestamps found for table"
    # We'll validate the tablet co-location invariant only for the latest (current) stream set
    # to avoid false failures after splits/merges/migrations change tablet geometry.
    # should we validate all stream sets instead?
    last_ts = timestamps[-1]

    # CURRENT streams for each timestamp
    streams_by_ts: Dict[object, set] = {}
    for ts in timestamps:
        rs = await tester.cql.run_async(
            f"SELECT stream_id FROM system.cdc_streams "
            f"WHERE keyspace_name='{ks}' AND table_name='{tbl}' AND timestamp=%s AND stream_state=0",
            [ts],
        )
        streams_by_ts[ts] = set(r.stream_id for r in rs)

    def active_ts_for(date_time):
        # max(ts <= date_time)
        cur = None
        for ts in timestamps:
            if ts <= date_time:
                cur = ts
            else:
                break
        return cur

    groups: Dict[Tuple[bytes, object, int], List[dict]] = {}
    for r in rows:
        d = r._asdict()
        #logger.info("row fields: %s", r._fields)
        key = (d['cdc_stream_id'], d['cdc_time'], d['pk'])
        groups.setdefault(key, []).append(d)

    deltas: List[CdcDeltaEvent] = []
    bad = []

    # will uncomment in final version
    # def find_changed_col(row: dict) -> Optional[int]:
    #     for i in range(tester.num_workers):
    #         if row.get(f"s{i}") is not None:
    #             return i
    #         # do we need to check deleted_* too?
    #         if row.get(f"cdc_deleted_s{i}") is True:
    #             return i
    #     return None

    # func for debug
    def changed_cols(delta_row: dict) -> list[int]:
        changed = []
        for i in range(tester.num_workers):
            if delta_row.get(f"s{i}") is not None or delta_row.get(f"cdc_deleted_s{i}") is True:
                changed.append(i)
        return changed

    for (sid, ctime, pk), lst in groups.items():
        by_op: Dict[CdcOperation, List[dict]] = {}
        for cdc_row in lst:
            operation = CdcOperation(cdc_row["cdc_operation"])
            by_op.setdefault(operation, []).append(cdc_row)

        # if 1 not in by_op:
        #     continue  # not sure if this is possible

        if len(by_op[CdcOperation.UPDATE]) != 1:
            bad.append(f"pk={pk} time={ctime}: expected 1 delta row, got {len(by_op[CdcOperation.UPDATE])}")
            continue

        delta = by_op[CdcOperation.UPDATE][0]

        changed = changed_cols(delta)
        if len(changed) != 1:
            bad.append(
                f"pk={pk} time={ctime}: delta changes {len(changed)} columns {changed}, "
                f"cannot attribute to a single worker column"
            )
            continue
        col = changed[0]
        # will uncomment in final version
        # col = find_changed_col(delta)
        # # logger.info(f'pk={pk} time={ctime}: found changed column s{col}')
        # if col is None:
        #     bad.append(f"pk={pk} time={ctime}: cannot detect changed column in delta")
        #     continue

        new_val = delta[f"s{col}"]
        if new_val is None:
            bad.append(f"pk={pk} time={ctime}: delta has NULL new value for s{col}")
            continue

        # preimage
        if CdcOperation.PREIMAGE not in by_op or len(by_op[CdcOperation.PREIMAGE]) != 1:
            bad.append(f"pk={pk} time={ctime}: expected 1 preimage row")
            continue

        preimage = by_op[CdcOperation.PREIMAGE][0]
        pre_val = preimage.get(f"s{col}")
        if pre_val != new_val - 1:
            try:
                # Dump details for debugging
                logger.info(
                    "MISMATCH pk=%s col=s%s time=%s stream=%s new=%s preimage=%s",
                    pk, col, ctime, sid.hex(), new_val, pre_val
                )
                cols_to_show = [col, (col + 1) % tester.num_workers]

                for dd in sorted(lst, key=lambda x: (x.get("cdc_batch_seq_no", -1), x.get("cdc_operation", -1))):
                    parts = [
                        f"op={dd.get('cdc_operation')}",
                        f"bseq={dd.get('cdc_batch_seq_no')}",
                        f"eob={dd.get('cdc_end_of_batch')}",
                    ]
                    for j in cols_to_show:
                        parts.append(f"s{j}={dd.get(f's{j}')!r}")
                        parts.append(f"del_s{j}={dd.get(f'cdc_deleted_s{j}')!r}")
                    logger.info("  " + " ".join(parts))
            except Exception:
                logger.exception("Failed to dump CDC mismatch details")
            bad.append(f"pk={pk} time={ctime}: preimage s{col}={pre_val}, expected {new_val-1}")

        # postimage
        if CdcOperation.POSTIMAGE not in by_op or len(by_op[CdcOperation.POSTIMAGE]) != 1:
            bad.append(f"pk={pk} time={ctime}: expected 1 postimage row")
            continue
        post = by_op[CdcOperation.POSTIMAGE][0]
        post_val = post.get(f"s{col}")
        if post_val != new_val:
            bad.append(f"pk={pk} time={ctime}: postimage s{col}={post_val}, expected {new_val}")

        # stream correctness vs active stream set
        dt = datetime_from_uuid1(ctime)
        ats = active_ts_for(dt)
        if ats is None:
            bad.append(f"pk={pk} time={ctime}: cannot map event time to stream-set timestamp")
        else:
            if sid not in streams_by_ts[ats]:
                bad.append(f"pk={pk} time={ctime}: stream_id not in CURRENT streams for ts={ats}")

        deltas.append(CdcDeltaEvent(
            pk=pk, stream_id=sid, cdc_time=ctime, batch_seq_no=delta["cdc_batch_seq_no"],
            col_idx=col, new_val=int(new_val),
        ))

    # if comment this assertion test will fail later due to duplicates
    assert not bad, "CDC verification errors:\n" + "\n".join(bad[:50])

    deltas_by_pk_col: Dict[Tuple[int,int], List[CdcDeltaEvent]] = {}
    for e in deltas:
        deltas_by_pk_col.setdefault((e.pk, e.col_idx), []).append(e)

    for (pk, col), evs in deltas_by_pk_col.items():
        evs.sort(key=lambda x: x.cdc_time.time)  # timeuuid v1 timestamp
        vals = [e.new_val for e in evs]
        final = tester.workers[col].success_counts[pk]

        # debug output
        if len(vals) != final:
            # show duplicates and a compact event list
            from collections import Counter
            c = Counter(vals)
            dups = [v for v, n in c.items() if n > 1]
            logger.error(
                "CDC mismatch pk=%s col=%s: delta_count=%s expected=%s dups=%s",
                pk, col, len(vals), final, sorted(dups)[:20],
            )
            for ev in evs:
                logger.error(
                    "  time=%s stream=%s bseq=%s val=%s",
                    ev.cdc_time, ev.stream_id.hex(), ev.batch_seq_no, ev.new_val,
                )

            expected_vals = set(range(1, final + 1))
            seen_vals = set(vals)

            missing = sorted(expected_vals - seen_vals)
            dups = sorted(v for v, n in Counter(vals).items() if n > 1)

            assert not missing, f"pk={pk} s{col}: missing values {missing[:20]}"
            if dups:
                logger.error("pk=%s s%s: duplicate vals=%s", pk, col, dups[:20])
        # end of debug output
        assert len(vals) == final, f"pk={pk} s{col}: delta_count={len(vals)} expected={final}"
        assert vals == list(range(1, final + 1)), f"pk={pk} s{col}: delta vals mismatch: {vals[:20]}..."

    # Validate ONLY for events that belong to the latest/current stream set (last_ts),
    # otherwise the check can become invalid after later splits/merges.
    token_stmt = tester.cql.prepare("SELECT token(?) AS tk FROM system.local")
    stream_token_cache: Dict[bytes, int] = {}
    pk_replica_cache: Dict[int, Tuple[Tuple[str,int],...]] = {}
    stream_replica_cache: Dict[int, Tuple[Tuple[str,int],...]] = {}

    async def tablet_replicas_cached(token: int) -> Tuple[Tuple[str,int],...]:
        if token in stream_replica_cache:
            return stream_replica_cache[token]
        reps = await get_tablet_replicas(manager, servers[0], ks, tbl, token)
        reps_norm = tuple(sorted((str(host), int(shard)) for (host, shard) in reps))
        stream_replica_cache[token] = reps_norm
        return reps_norm

    current_streams_last = streams_by_ts.get(last_ts, set())
    for e in deltas:
        # Filter to events from the latest/current stream set
        dt = datetime_from_uuid1(e.cdc_time)
        ats = active_ts_for(dt)
        if ats != last_ts:
            continue
        # Optional: ensure the stream_id is one of the CURRENT streams for last_ts
        if current_streams_last and e.stream_id not in current_streams_last:
            continue
        pk_token = tester.pk_to_token[e.pk]
        if pk_token not in pk_replica_cache:
            reps = await get_tablet_replicas(manager, servers[0], ks, tbl, pk_token)
            pk_replica_cache[pk_token] = tuple(sorted((str(host), int(shard)) for (host, shard) in reps))

        if e.stream_id not in stream_token_cache:
            r = (await tester.cql.run_async(token_stmt.bind([e.stream_id])))[0]
            stream_token_cache[e.stream_id] = int(r.tk)

        stream_token = stream_token_cache[e.stream_id]
        pk_reps = pk_replica_cache[pk_token]
        stream_reps = await tablet_replicas_cached(stream_token)
        assert pk_reps == stream_reps, f"pk={e.pk}: tablet mismatch between token(pk) and token(stream_id)"

    assert len(deltas) == total_expected, f"delta rows={len(deltas)} expected={total_expected}"


@pytest.mark.asyncio
@skip_mode("release", "error injections are not supported in release mode")
@skip_mode("debug", "debug mode is too slow for this test")
async def test_lwt_cdc_during_tablets_resize_and_migration(manager: ManagerClient):
    cfg = {
        "enable_tablets": True,
        "target-tablet-size-in-bytes": 1024 * 16,
        "rf_rack_valid_keyspaces": False,
        "tablet_load_stats_refresh_interval_in_seconds": 1,
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
        '--logger-log-level', 'paxos=trace', '--logger-log-level', 'cdc=debug', '--smp=2',
    ]

    servers = await manager.servers_add(6, config=cfg, cmdline=cmdline, property_file=properties)

    async with new_test_keyspace(
        manager,
        "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} "
        "AND tablets = {'initial': 2}",
    ) as ks:
        stop = asyncio.Event()
        tester = CdcLWTTester(manager, ks, "lwt_cdc_table", num_workers=DEFAULT_WORKERS, num_keys=DEFAULT_NUM_KEYS)

        await tester.create_schema()
        await tester.initialize_rows()
        await tester.truncate_cdc_log()
        await tester.start_workers(stop)

        try:
            tester.set_phase(PHASE_WARMUP)
            await tester.wait_for_phase_ops(stop, PHASE_WARMUP, WARMUP_LWT_CNT, timeout=180, poll=0.2)
            #tester.set_phase(PHASE_CHAOS)
            resize_task = asyncio.create_task(run_random_resizes(stop, manager, servers, tester, ks, tester.tbl, 20))
            mig_task = asyncio.create_task(tablet_migration_ops(
                stop,
                manager, servers, tester,
                num_ops=20,
                pause_range=(0.3, 1.0),
                server_properties=properties,
                table="lwt_cdc_table",
            ))
            await asyncio.gather(resize_task, mig_task)
            logger.info('Completed chaos phase: resizing and migrations')
            #tester.set_phase(PHASE_POST)
            #await tester.wait_for_phase_ops(stop, PHASE_POST, POST_LWT_CNT, timeout=180, poll=0.2)
            logger.info('Completed chaos phase: post-migration')
        finally:
            await tester.stop_workers()

        await tester.verify_consistency()
        await asyncio.sleep(random.uniform(*(1, 2.0)))
        await verify_cdc(manager, servers, tester)
