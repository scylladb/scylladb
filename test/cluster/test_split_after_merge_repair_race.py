#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Deterministic regression guard for SCYLLADB-2531, the unlinked-sstable
abort originally observed in test_tablet_repair_wait
(test/cluster/tasks/test_tablet_tasks.py).

The split bypass and repair's component rewrite are forced to overlap on
tablet 0 by parking repair after its prepare phase, entering split mode,
and only then releasing it.  Both injections cannot park on the same
tablet at once (repair's prepare lock excludes the bypass).

Sequence:

  1. 3-node cluster, RF=3, tablets=4, tombstone_gc='repair'.
  2. Insert 256 rows + flush + run one incremental repair so the
     baseline sstables have repaired_at>0.
  3. Insert 128 more rows + flush on all nodes -> fresh sstables with
     repaired_at=0, one per tablet, confined to that tablet's range.
     Disable autocompaction: a minor compaction of the merged group
     would combine them into one sstable spanning the split point, which
     needs a real split and never takes the bypass.
  4. Lower min_tablet_count to 2.  The load balancer merges 4 -> 2 and,
     unlike with the old count, does not emit the split on its own the
     moment the merge finalizes; the split stays under the test's
     control.
  5. Enable the three injections on all nodes.
  6. Incremental repair of tablet 0 only.  Wait for it to park after
     its prepare phase, with the fresh sstables captured and the
     unrepaired-view write lock held, on every node.
  7. Raise min_tablet_count to 4 and wait, on every node, for tablet 0's
     storage group to enter split mode.  Split cannot pass the lock, so
     the group's sstables stay in the main compaction group.
  8. Release the prepare park and wait for repair to park at the
     component rewrite: pre-fix it has now bound the sstables to the
     split-ready group, post-fix it captures them from the main group.
  9. Release the rewrite.  Pre-fix the guard trips; post-fix end_repair
     drops the lock, split's bypass parks and is released, repeatedly,
     until the split finalizes.
 10. Grep for the guards, then require the repair to have succeeded and
     the bypass to have run.

Timeouts of the orchestration waits skip the test (environment); a guard,
a failed repair, a split that never finalizes or one that finalizes without
a bypass park fail it.
abort_on_internal_error is set to false so the guard is logged instead of
crashing the node.
"""

import asyncio
import contextlib
import logging
import re
import time

import pytest

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.skip_types import skip_env
from test.pylib.util import wait_for
from test.pylib.tablets import get_all_tablet_replicas, get_tablet_count

logger = logging.getLogger(__name__)

# The two sstable_list_updater::prepare() guards SCYLLADB-2531 trips (see the
# module docstring).  Not the generic "on_internal_error" string: that would
# risk false positives from unrelated internal errors in the race window.
ABORT_MARKER = "was already unlinked before being added to table"
MISROUTE_MARKER = "Unable to remove input SSTable"
MARKERS_RE = "|".join(re.escape(m) for m in (ABORT_MARKER, MISROUTE_MARKER))

PARKED = "split_bypass_before_completion: parked"
SPLIT_DONE = "Detected tablet split for table"
# Tablet 0's sstables are the only ones repair's prepare phase logs; a park of
# one of them identifies tablet 0's bypass.
PREPARED_SST_RE = r"(?:Added|Skipped adding) sst=(?:\S*/)?(\S+?)-big-TOC\.txt"
PARKED_SST_RE = r"parked sst=(?:\S*/)?(\S+?)-big-Data\.db"


async def _enable(manager, servers, name, one_shot=False):
    await asyncio.gather(*[
        manager.api.enable_injection(s.ip_addr, name, one_shot=one_shot)
        for s in servers
    ])


async def _disable(manager, servers, name):
    for s in servers:
        try:
            await manager.api.disable_injection(s.ip_addr, name)
        except Exception as e:
            logger.debug("disable_injection(%s) on %s ignored: %s",
                         name, s.server_id, e)


async def _message(manager, servers, name):
    await asyncio.gather(*[
        manager.api.message_injection(s.ip_addr, name) for s in servers
    ])


async def _wait_for_pattern_any(logs, marks, pattern, timeout, task=None,
                                task_may_finish=False):
    """Wait for *pattern* in any log; return (server, line, position) or raise.

    If *task* finishes first RuntimeError is raised from its exception;
    finishing without one is an error too unless *task_may_finish*.
    """
    waiters = {s: asyncio.create_task(log.wait_for(pattern, from_mark=marks[s]))
               for s, log in logs.items()}
    pending = set(waiters.values())
    if task is not None:
        pending.add(task)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    try:
        while True:
            done, _ = await asyncio.wait(
                pending, timeout=deadline - loop.time(),
                return_when=asyncio.FIRST_COMPLETED)
            if not done:
                raise asyncio.TimeoutError(
                    f"pattern {pattern!r} not seen on any node within {timeout}s")
            for s, t in waiters.items():
                if t in done:
                    pos, matches = t.result()
                    return s, matches[0][0], pos
            pending.discard(task)
            exc = None if task.cancelled() else task.exception()
            if task.cancelled() or exc is not None or not task_may_finish:
                raise RuntimeError(
                    f"repair task finished before {pattern!r} was seen") from exc
    finally:
        for t in waiters.values():
            t.cancel()


async def _wait_for_pattern_all(logs, marks, pattern, timeout, task=None):
    """Wait for *pattern* in every log, or raise.

    If *task* finishes before every log matched, RuntimeError is raised.
    """
    waiters = [asyncio.create_task(
                   log.wait_for(pattern, from_mark=marks[s], timeout=timeout))
               for s, log in logs.items()]
    pending = set(waiters)
    if task is not None:
        pending.add(task)
    try:
        while True:
            done, _ = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                if t is not task:
                    t.result()
            pending -= done
            if not (pending - {task}):
                return
            if task in done:
                raise RuntimeError(
                    f"repair task finished before {pattern!r} was seen on every node"
                ) from (None if task.cancelled() else task.exception())
    finally:
        for t in waiters:
            t.cancel()


@pytest.mark.asyncio
@pytest.mark.skip_mode('release', 'error injections are not supported in release mode')
async def test_split_bypass_race_with_repair_after_merge(manager: ScyllaClusterManager):
    """
    Deterministic reproducer of the split-bypass vs. repair component
    rewrite race on the merge→split→repair timeline observed in CI.
    """
    cfg = {
        'enable_tablets': True,
        'tablet_load_stats_refresh_interval_in_seconds': 1,
        'abort_on_internal_error': False,
    }
    cmdline = [
        '--logger-log-level', 'compaction=debug',
        '--logger-log-level', 'table=debug',
        '--logger-log-level', 'repair=debug',
        '--logger-log-level', 'load_balancer=info',
    ]
    servers = await manager.servers_add(
        3, cmdline=cmdline, config=cfg,
        property_file=[{"dc": "dc1", "rack": f"r{i % 3}"} for i in range(3)])
    cql = manager.get_cql()

    initial_tablets = 4
    merged_tablets = initial_tablets // 2
    split_target = merged_tablets * 2
    async with new_test_keyspace(
            manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', "
            "'replication_factor': 3}") as ks:
        await cql.run_async(
            f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int) "
            f"WITH tablets = {{'min_tablet_count': {initial_tablets}}} "
            f"AND tombstone_gc = {{'mode':'repair'}};")

        # ---- Step 2: baseline data + first repair so sstables_repaired_at > 0.
        logger.info("Inserting baseline data and flushing")
        await asyncio.gather(*[
            cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});")
            for k in range(256)])
        for s in servers:
            await manager.api.flush_keyspace(s.ip_addr, ks)
        logger.info("Running baseline incremental repair")
        await manager.api.tablet_repair(
            servers[0].ip_addr, ks, "test", "all",
            incremental_mode='incremental')

        # ---- Step 3: fresh data → memtable-origin sstables with
        # repaired_at=0.  These are the race target.
        logger.info("Inserting fresh data and flushing")
        await asyncio.gather(*[
            cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});")
            for k in range(256, 384)])
        for s in servers:
            await manager.api.flush_keyspace(s.ip_addr, ks)
        # Keep the per-tablet sstables as they are across the merge; a
        # minor compaction of the merged group would leave nothing for
        # the split bypass.
        await asyncio.gather(*[
            manager.api.disable_autocompaction(s.ip_addr, ks, "test")
            for s in servers])

        # Mark logs AFTER all setup so we only consider events from the
        # actual race window onward.
        logs = {s: await manager.server_open_log(s.server_id) for s in servers}
        marks = {s: await logs[s].mark() for s in servers}

        injections = ("wait_after_prepare_sstables_for_incremental_repair",
                      "repair_before_component_rewrite",
                      "split_bypass_before_completion")

        # ---- Step 4: merge 4 -> 2 by lowering min_tablet_count.  With
        # the count at 2 the load balancer has no reason to split again
        # until step 7 raises it.
        logger.info("Triggering tablet merge (lower min_tablet_count)")
        await cql.run_async(
            f"ALTER TABLE {ks}.test WITH tablets = "
            f"{{'min_tablet_count': {merged_tablets}}}")
        try:
            await _wait_for_pattern_any(
                logs, marks, "Detected tablet merge for table", timeout=120)
            logger.info("Merge detected")
        except asyncio.TimeoutError:
            skip_env("Could not trigger merge in this run")

        # Merge must land before the split is triggered, otherwise the
        # orchestration no longer matches the merge -> split -> repair timeline.
        async def merged():
            if await get_tablet_count(manager, servers[0], ks, 'test') <= merged_tablets:
                return True
        try:
            await wait_for(merged, time.time() + 60, period=1)
        except AssertionError:
            current = await get_tablet_count(
                manager, servers[0], ks, 'test')
            skip_env(
                f"Merge did not reduce tablet count within 60s "
                f"(current={current}); race window not exercised.")

        # Tablet 0 is the race target: its main compaction group is group
        # 0, which the split-mode wait in step 7 refers to.
        tablets = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
        repair_token = min(t.last_token for t in tablets)

        repair_task = None
        repair_error = None
        repair_hung = False
        parks = 0
        tablet0_ssts = {}
        try:
            # ---- Step 5: enable injections (after the merge, so a merge
            # timeout cannot leave them armed).
            logger.info("Enabling %s", ", ".join(injections))
            for name in injections:
                await _enable(manager, servers, name)

            # ---- Step 6: repair tablet 0 and wait for it to park right
            # after its prepare phase on every node: sstables captured,
            # unrepaired-view write lock held.
            logger.info("Starting background incremental repair of tablet 0")
            repair_task = asyncio.create_task(
                manager.api.tablet_repair(
                    servers[0].ip_addr, ks, "test", repair_token,
                    incremental_mode='incremental', timeout=600))
            logger.info("Waiting for repair to park after prepare")
            try:
                await _wait_for_pattern_all(
                    logs, marks,
                    "wait_after_prepare_sstables_for_incremental_repair: waiting for message",
                    timeout=120, task=repair_task)
                logger.info("Repair parked after prepare")
                for s in servers:
                    tablet0_ssts[s] = {m.group(1) for _, m in await logs[s].grep(
                        PREPARED_SST_RE, from_mark=marks[s])}
                    if not tablet0_ssts[s]:
                        pytest.fail(f"Repair's prepare phase logged no sstables on {s.server_id}")
            except asyncio.TimeoutError:
                skip_env("Repair never parked after prepare within 120s; "
                         "race window not exercised.")

            # ---- Step 7: trigger the split and wait for tablet 0's storage
            # group to enter split mode on every node.  Split queues behind
            # the repair lock; by-token routing now names a split-ready group.
            marks_split = {s: await logs[s].mark() for s in servers}
            logger.info("Triggering tablet split (raise min_tablet_count)")
            await cql.run_async(
                f"ALTER TABLE {ks}.test WITH tablets = "
                f"{{'min_tablet_count': {split_target}}}")
            logger.info("Waiting for split mode on tablet 0")
            try:
                await _wait_for_pattern_all(
                    logs, marks_split,
                    r"set_split_mode: Set sstables_repaired_at=\d+ for split old_group=0 old_range=",
                    timeout=120, task=repair_task)
                logger.info("Split mode set on tablet 0 on all nodes")
            except TimeoutError:
                skip_env("Split decision never reached tablet 0 within 120s; "
                         "race window not exercised.")

            # ---- Step 8: let repair reach mark_sstable_as_repaired and
            # park at the component rewrite.
            logger.info("Releasing repair from the prepare park")
            await _message(manager, servers,
                           "wait_after_prepare_sstables_for_incremental_repair")
            logger.info("Waiting for repair to park at component rewrite")
            try:
                await _wait_for_pattern_any(
                    logs, marks,
                    "repair_before_component_rewrite: waiting for message",
                    timeout=120, task=repair_task)
                logger.info("Repair parked at component rewrite")
            except asyncio.TimeoutError:
                skip_env("Repair never parked at component rewrite "
                         "within 120s; race window not exercised.")
            logger.info("Releasing repair from component rewrite")
            await _message(manager, servers, "repair_before_component_rewrite")

            # ---- Step 9: split proceeds once end_repair drops the lock.  Release
            # every bypass park, counting tablet 0's sstables, until the split
            # is detected on every node or a guard trips.
            split_done = set()
            while len(split_done) < len(servers):
                try:
                    s, line, pos = await _wait_for_pattern_any(
                        logs, marks_split, f"{MARKERS_RE}|{PARKED}|{SPLIT_DONE}",
                        timeout=120, task=repair_task, task_may_finish=True)
                except asyncio.TimeoutError:
                    pytest.fail("Split neither parked, finalized on every node nor tripped a guard within 120s")
                except RuntimeError as e:
                    repair_error = e.__cause__ or e
                    break
                marks_split[s] = pos
                if SPLIT_DONE in line:
                    split_done.add(s)
                    continue
                if PARKED not in line:
                    break
                parks += re.search(PARKED_SST_RE, line).group(1) in tablet0_ssts[s]
                try:
                    await manager.api.message_injection(
                        s.ip_addr, "split_bypass_before_completion")
                except Exception as e:
                    # A node that tripped the guard may already be gone.
                    logger.info("message_injection on %s failed: %s", s.server_id, e)
                    break
            logger.info("Released %d bypass park(s)", parks)
        finally:
            # Disabling releases every still-parked handler, so the split
            # can run to completion whatever happened above.
            for name in injections:
                await _disable(manager, servers, name)
            if repair_task is not None:
                try:
                    await asyncio.wait_for(repair_task, timeout=120)
                    logger.info("Repair task completed")
                except asyncio.TimeoutError:
                    repair_hung = True
                    repair_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await repair_task
                except Exception as e:
                    repair_error = repair_error or e

        # ---- Check for the race signal first.
        logger.info("Checking for guard markers")
        hit = None
        for s in servers:
            matches = await logs[s].grep(MARKERS_RE, from_mark=marks[s])
            if matches:
                hit = (s.server_id, matches[0][0])
                break

        if hit:
            where, line = hit
            pytest.fail(
                f"Race reproduced on server {where}: {line!r}. "
                "Repair's component rewrite and split's bypass disagreed about "
                "the compaction group holding the sstable. See SCYLLADB-2531.")
        if repair_hung:
            pytest.fail("Repair task did not complete within 120s after the injections were disabled")
        if repair_error is not None:
            pytest.fail(f"Repair task raised: {repair_error!r}")
        if not parks:
            pytest.fail("Split finalized without a bypass park of a tablet 0 sstable; "
                        "the setup no longer exercises the bypass path")
        logger.info("No guard marker observed - fix appears effective")
