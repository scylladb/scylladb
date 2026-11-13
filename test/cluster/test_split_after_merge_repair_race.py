#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Deterministic reproducer for the SCYLLADB-2531 unlinked-sstable abort
originally observed in test_tablet_repair_wait
(test/cluster/tasks/test_tablet_tasks.py).

Split's bypass path hands an sstable to on_compaction_completion() after
repair's mark_sstable_as_repaired() already replaced and unlinked it:

    on_compaction_completion -> sstable_list_updater::prepare ->
        on_internal_error("was already unlinked before being added to table")

Constraints that shape the sequence:

  * Split never processes the repairing view (its compaction is always
    disabled), and holds the unrepaired view's incremental-repair read lock,
    which repair needs before marking. So the shared sstable must be in the
    repaired view, and only a 'full' incremental repair rewrites those.
  * Split's executor holds the maintenance semaphore that repair's component
    rewrite also needs, so split must be parked after capturing its inputs
    but before taking that semaphore (split_before_maintenance_permit).

Sequence:

  1. 3 nodes, RF=3, 4 tablets, autocompaction off (so repaired sstables
     stay within one child tablet and split bypasses them).
  2. Insert + flush + incremental repair -> repaired sstables.
  3. Merge 4 -> 2 tablets.
  4. Enable split_before_maintenance_permit and trigger split 2 -> 4.
     Split captures the repaired sstables and parks on every node.
  5. Run a 'full' incremental repair to completion.
     - Pre-fix: repair rewrites the captured sstables, unlinking them.
     - Fixed: repair's capture stops the parked split first, so split
       never sees the unlinked sstables.
  6. Release split: pre-fix trips the marker; fixed code finishes the split.

abort_on_internal_error is set to false so the error is logged rather than
crashing the node; we then grep for the marker.
"""

import asyncio
import logging
import re
import time

import pytest

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_tablet_count

logger = logging.getLogger(__name__)

# Specific diagnostic emitted by sstable_list_updater::prepare(); the generic
# on_internal_error string could match unrelated errors.
ABORT_MARKER = "was already unlinked before being added to table"
INJECTION = "split_before_maintenance_permit"


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
            logger.warning("disable_injection(%s) on %s ignored: %s",
                           name, s.server_id, e)


async def _message(manager, servers, name):
    # Pre-fix, one node may abort on release; don't let that mask the marker check.
    results = await asyncio.gather(
        *(manager.api.message_injection(s.ip_addr, name) for s in servers),
        return_exceptions=True)
    failed = []
    for s, r in zip(servers, results):
        if isinstance(r, BaseException):
            logger.warning("message_injection(%s) on %s failed: %s", name, s.server_id, r)
            failed.append(s.server_id)
    return failed


async def _wait_for_tablet_count(manager, server, ks, expected, timeout):
    deadline = time.monotonic() + timeout
    n = last_err = None
    while True:
        try:
            n = await get_tablet_count(manager, server, ks, 'test')
            if n == expected:
                return
        except Exception as e:
            last_err = e
            logger.warning("get_tablet_count failed: %s", e)
        if time.monotonic() > deadline:
            pytest.fail(f"Tablet count did not reach {expected} within {timeout}s "
                        f"(current={n}, last error={last_err})")
        await asyncio.sleep(1)


async def _grep_marker(logs, marks):
    for s, log in logs.items():
        matches = await log.grep(ABORT_MARKER, from_mark=marks[s])
        if matches:
            return s.server_id, matches[0][0]
    return None


async def _wait_for_parked(logs, marks, servers, expected, timeout):
    pattern = f"{INJECTION}: waiting for message"
    deadline = time.monotonic() + timeout
    while True:
        counts = {s.server_id: len(await logs[s].grep(pattern, from_mark=marks[s])) for s in servers}
        if all(c >= expected for c in counts.values()):
            return
        if time.monotonic() > deadline:
            pytest.fail(f"Split did not park {expected}x on every node within {timeout}s: {counts}")
        await asyncio.sleep(1)


@pytest.mark.asyncio
@pytest.mark.skip_mode('release', 'error injections are not supported in release mode')
async def test_split_bypass_race_with_repair_after_merge(manager: ScyllaClusterManager):
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

    async with new_test_keyspace(
            manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', "
            "'replication_factor': 3}") as ks:
        await cql.run_async(
            f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int) "
            f"WITH tablets = {{'min_tablet_count': 4}} "
            f"AND tombstone_gc = {{'mode':'repair'}};")
        # Post-merge compaction would join repaired sstables across child
        # boundaries, forcing a real split rewrite instead of the bypass.
        for s in servers:
            await manager.api.disable_autocompaction(s.ip_addr, ks, 'test')

        logger.info("Inserting data, flushing and repairing")
        await asyncio.gather(*[
            cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});")
            for k in range(256)])
        for s in servers:
            await manager.api.flush_keyspace(s.ip_addr, ks)
        await manager.api.tablet_repair(
            servers[0].ip_addr, ks, "test", "all",
            incremental_mode='incremental')

        logger.info("Merging 4 -> 2 tablets")
        await cql.run_async(
            f"ALTER TABLE {ks}.test WITH tablets = "
            f"{{'min_tablet_count': 2, 'max_tablet_count': 2}}")
        await _wait_for_tablet_count(manager, servers[0], ks, 2, timeout=120)

        logs = {s: await manager.server_open_log(s.server_id) for s in servers}
        marks = {s: await logs[s].mark() for s in servers}

        # Precondition: baseline repair repaired all data and nothing was written since;
        # unrepaired data would make split park holding the lock repair needs.
        await _enable(manager, servers, INJECTION)
        try:
            logger.info("Triggering split 2 -> 4 tablets")
            await cql.run_async(
                f"ALTER TABLE {ks}.test WITH tablets = {{'min_tablet_count': 4}}")
            # RF=3 on 3 nodes with 2 tablets: 2 replicas per node, each parks once.
            await _wait_for_parked(logs, marks, servers, expected=2, timeout=180)
            logger.info("Split parked on all nodes")

            logger.info("Running full incremental repair")
            await asyncio.wait_for(
                manager.api.tablet_repair(servers[0].ip_addr, ks, "test", "all",
                                          incremental_mode='full'),
                timeout=180)

            logger.info("Releasing split")
            failed_releases = await _message(manager, servers, INJECTION)

            # Pre-fix trips the marker; fixed code completes the split.
            deadline = time.monotonic() + 180
            split_done = False
            hit = None
            while time.monotonic() < deadline:
                hit = await _grep_marker(logs, marks)
                if hit:
                    break
                try:
                    if await get_tablet_count(manager, servers[0], ks, 'test') == 4:
                        split_done = True
                        break
                except Exception as e:
                    logger.warning("get_tablet_count failed: %s", e)
                await asyncio.sleep(1)
        finally:
            await _disable(manager, servers, INJECTION)

        if hit:
            where, line = hit
            pytest.fail(
                f"Race reproduced on server {where}: {ABORT_MARKER!r} at {line!r}. "
                "Split's on_compaction_completion observed an SSTable already "
                "unlinked by repair's concurrent component rewrite. "
                "See SCYLLADB-2531.")
        assert split_done, \
            f"Split did not complete within 180s (failed releases: {failed_releases})"

        # A real split of a pre-merge repaired sstable means the bypass was not exercised.
        split_rewrite = re.compile(rf"\[Split {re.escape(ks)}\.test .*Splitting .*repaired_at=[1-9]\d*\b")
        for s in servers:
            assert not await logs[s].grep(split_rewrite, from_mark=marks[s]), \
                f"Repaired sstable needed a real split on server {s.server_id}"
