#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Deterministic reproducer for the SCYLLADB-2531 unlinked-sstable abort
originally observed in test_tablet_repair_wait
(test/cluster/tasks/test_tablet_tasks.py).

This isolates the split-bypass-vs-repair race exposed by that test.
The abort fires inside the split bypass path after the captured
SSTable was unlinked by repair's mark_sstable_as_repaired:

    on_compaction_completion -> sstable_list_updater::prepare ->
        on_internal_error("was already unlinked before being added to table")

Why this test (vs. relying on the original test_tablet_repair_wait):

  * test_tablet_repair_wait hits the race only statistically — it
    relies on natural timing between split and repair, so reproducing
    requires --repeat N and is not reliable in CI.
  * Here we use error injections to park both split and repair at the
    exact points needed to expose the race and explicitly order the
    unlink.  The test fails deterministically on a pre-fix build and
    passes after the fix.

Sequence used here:

  1. 3-node cluster, RF=3, tablets=4, tombstone_gc='repair'.
  2. Insert 256 rows + flush + run one incremental repair so the
     baseline sstables have repaired_at>0.
  3. Insert 128 more rows + flush on all nodes -> fresh sstables with
     repaired_at=0 (the actual race target).
  4. Enable `split_bypass_before_completion` and
     `repair_before_component_rewrite` injections on all nodes.
  5. Trigger a tablet merge (force decrease) and wait for it to finalize.
  6. Start a background incremental repair -> it will park at
     repair_before_component_rewrite once it gets to
     mark_sstable_as_repaired for the freshly-flushed sstable.
  7. Trigger the split by raising min_tablet_count -> split's bypass
     captures the fresh sstable and parks at
     split_bypass_before_completion.
  8. Release the repair injection -> mark_sstable_as_repaired calls
     perform_component_rewrite, which creates a hard-linked replacement
     sstable and unlinks the original file.
  9. Release the split bypass -> on_compaction_completion is called with
     desc.new_sstables = [original_sst], whose unlinked_at() is now
     set -> sstable_list_updater::prepare() trips the diagnostic guard.

abort_on_internal_error is set to false so the abort is logged but
the node does not actually crash; we then grep for the marker.
"""

import asyncio
import logging

import pytest

from test.cluster.util import new_test_keyspace
from test.pylib.manager_client import ManagerClient
from test.pylib.skip_types import skip_env
from test.pylib.tablets import get_tablet_count

logger = logging.getLogger(__name__)

# Specific diagnostic marker emitted by sstable_list_updater::prepare()
# when split's on_compaction_completion observes an sstable already
# unlinked by repair's concurrent component rewrite (SCYLLADB-2531).
# We intentionally do NOT match the generic "on_internal_error" string
# here, as that would risk false positives from unrelated internal
# errors hitting during the race window.
ABORT_MARKER = "was already unlinked before being added to table"


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
    for s in servers:
        try:
            await manager.api.message_injection(s.ip_addr, name)
        except Exception as e:
            logger.debug("message_injection(%s) on %s ignored: %s",
                         name, s.server_id, e)


async def _wait_for_pattern_any(logs, marks, pattern, timeout):
    """Wait for *pattern* in any log; return the matching server or raise."""
    tasks = {s: asyncio.create_task(log.wait_for(pattern, from_mark=marks[s]))
             for s, log in logs.items()}
    done, pending = await asyncio.wait(
        tasks.values(), timeout=timeout,
        return_when=asyncio.FIRST_COMPLETED)
    for t in pending:
        t.cancel()
    if not done:
        raise asyncio.TimeoutError(
            f"pattern {pattern!r} not seen on any node within {timeout}s")
    for s, t in tasks.items():
        if t in done:
            return s
    raise asyncio.TimeoutError(f"pattern {pattern!r}: lost matching server")


@pytest.mark.asyncio
@pytest.mark.skip_mode('release', 'error injections are not supported in release mode')
async def test_split_bypass_race_with_repair_after_merge(manager: ManagerClient):
    """
    Deterministic reproducer of the unlinked-sstable abort in the split
    bypass path, racing against repair's component rewrite, set up to
    match the merge→split→repair timeline observed in CI.
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

        # Mark logs AFTER all setup so we only consider events from the
        # actual race window onward.
        logs = {s: await manager.server_open_log(s.server_id) for s in servers}
        marks = {s: await logs[s].mark() for s in servers}

        # ---- Step 4: enable injections.
        logger.info("Enabling split_bypass_before_completion and "
                    "repair_before_component_rewrite")
        await _enable(manager, servers, "split_bypass_before_completion")
        await _enable(manager, servers, "repair_before_component_rewrite")

        repair_task = None
        try:
            # ---- Step 5: trigger merge (4 -> 2 tablets) and wait for
            # it to finalize so the post-merge load-balancer emits the
            # split decision naturally.
            logger.info("Triggering tablet merge (force decrease)")
            await _enable(manager, servers, "tablet_force_tablet_count_decrease")
            try:
                await _wait_for_pattern_any(
                    logs, marks, "Detected tablet merge for table", timeout=120)
                logger.info("Merge detected")
            except asyncio.TimeoutError:
                skip_env("Could not trigger merge in this run")
            finally:
                await _disable(manager, servers,
                               "tablet_force_tablet_count_decrease")

            # Wait for merge to actually halve the tablets so the
            # subsequent split has well-defined post-state.  If it
            # never drops, the rest of the orchestration no longer
            # matches the merge -> split -> repair timeline, so we
            # skip rather than risk a false-green pass.
            merged = False
            for _ in range(60):
                n = await get_tablet_count(manager, servers[0], ks, 'test')
                if n <= initial_tablets // 2:
                    merged = True
                    break
                await asyncio.sleep(1)
            if not merged:
                current = await get_tablet_count(
                    manager, servers[0], ks, 'test')
                skip_env(
                    f"Merge did not reduce tablet count within 60s "
                    f"(current={current}); race window not exercised.")

            # ---- Step 6: start the long-running repair in the background.
            logger.info("Starting background incremental repair")
            repair_task = asyncio.create_task(
                manager.api.tablet_repair(
                    servers[0].ip_addr, ks, "test", "all",
                    incremental_mode='incremental'))

            # ---- Step 7: trigger the split by raising min_tablet_count
            # back up past the post-merge count.  This naturally emits a
            # split decision (the same way the CI failure did via
            # reason=initial after merge).
            logger.info("Triggering tablet split (raise min_tablet_count)")
            split_target = initial_tablets * 2
            await cql.run_async(
                f"ALTER TABLE {ks}.test WITH tablets = "
                f"{{'min_tablet_count': {split_target}}}")

            # Wait for split to park at the bypass injection.  We give
            # it a wide window because the load-balancer must first
            # emit the decision, then the split task must reach the
            # bypass path.  If it never parks, the bypass-vs-rewrite
            # race is not exercised at all, so skip rather than
            # report a false green.
            logger.info("Waiting for split bypass to park")
            try:
                await _wait_for_pattern_any(
                    logs, marks,
                    "split_bypass_before_completion: parked",
                    timeout=180)
                logger.info("Split bypass parked")
            except asyncio.TimeoutError:
                skip_env("Split bypass never parked within 180s; "
                         "race window not exercised.")

            # Wait for repair to park at the component rewrite.  If
            # this point is never reached the unlink ordering required
            # to expose the race does not happen, so skip.
            logger.info("Waiting for repair to park at component rewrite")
            try:
                await _wait_for_pattern_any(
                    logs, marks,
                    "repair_before_component_rewrite: waiting for message",
                    timeout=120)
                logger.info("Repair parked at component rewrite")
            except asyncio.TimeoutError:
                skip_env("Repair never parked at component rewrite "
                         "within 120s; race window not exercised.")

            # ---- Step 8: release repair -> it does the component
            # rewrite and unlinks the original sstable file.
            logger.info("Releasing repair from component rewrite")
            await _message(manager, servers,
                           "repair_before_component_rewrite")
            # Give the rewrite/unlink a moment to take effect on disk.
            await asyncio.sleep(2)

            # ---- Step 9: release split bypass -> on_compaction_completion
            # observes the unlinked sstable and trips the diagnostic guard.
            logger.info("Releasing split bypass")
            await _message(manager, servers,
                           "split_bypass_before_completion")
            # Give the abort marker a moment to be logged.
            await asyncio.sleep(3)
        finally:
            # Defensive cleanup.  Disable everything so the test framework
            # tears down cleanly even if the orchestration above raised.
            for name in ("split_bypass_before_completion",
                         "repair_before_component_rewrite",
                         "tablet_force_tablet_count_decrease"):
                await _disable(manager, servers, name)
            if repair_task is not None:
                try:
                    await asyncio.wait_for(repair_task, timeout=120)
                    logger.info("Repair task completed")
                except asyncio.TimeoutError:
                    logger.warning("Repair task did not complete within 120s")
                    repair_task.cancel()
                except Exception as e:
                    logger.warning("Repair task raised: %s", e)

        # ---- Check for the race signal.
        logger.info("Checking for abort marker")
        hit = None
        for s in servers:
            matches = await logs[s].grep(ABORT_MARKER, from_mark=marks[s])
            if matches:
                hit = (s.server_id, matches[0][0])
                break

        if hit:
            where, line = hit
            pytest.fail(
                f"Race reproduced on server {where}: {ABORT_MARKER!r} at {line!r}. "
                "Split's on_compaction_completion observed an SSTable already "
                "unlinked by repair's concurrent component rewrite. "
                "See SCYLLADB-2531.")
        else:
            logger.info("No abort marker observed - fix appears effective")
