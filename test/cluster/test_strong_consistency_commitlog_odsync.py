#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Crash recovery of strongly consistent tables with commitlog_use_o_dsync on.

Every test tier runs Scylla with --commitlog-use-o-dsync 0, so the two paths
by which an intact commitlog segment gets reported as corruption after an
ordinary crash (SCYLLADB-4853) never occur in CI:

  A. a recycled segment keeps the trailers of its previous incarnation, written
     at a smaller alignment, so each new-size sector ends with an old trailer;
  B. a brand-new segment's tail is pre-written from an uninitialised buffer,
     so the bytes past the write frontier are not zero.

Both need the O_DSYNC pre-write in segment_manager::allocate_segment_ex(), and
the reader checks the sector CRC before the sector id, so both report
segment_data_corruption_error. That used to be cosmetic. Now that the raft log
of a strongly consistent group lives in the commitlog, replay refuses to start a
node whose damaged segments may have held acknowledged raft batches
(raft_commitlog_replay_buffer::finish_replay), so a healthy node can refuse to
boot after a plain crash with the production default of O_DSYNC.

The first two tests are the O_DSYNC counterparts of test_strong_consistency_commitlog.py:
a strongly consistent single-node table under write load, a crash, and the
expectation that the node starts again on its own and keeps every acknowledged
row. The last test damages a segment on purpose, to show that the refusal does
fire in this setup, so a clean start in the other tests means a clean replay
and not a detection that never ran.

Path A is fixed for files this binary recycled itself (commit ffe442641f writes
every O_DSYNC segment with the overwrite alignment), so its test guards the fix;
path B is open, and its test fails until it is fixed. Path A depends on the
filesystem: seastar lowers disk_overwrite_dma_alignment below
disk_write_dma_alignment only on XFS with a kernel of 5.12 or newer and a
device whose logical sector is smaller than the filesystem block. On other
filesystems the recycled-segment test exercises the recycling but not the
alignment change, and passes.
"""

import asyncio
import logging
import os
import re
import time
from typing import Awaitable, Callable

import pytest

from test.cluster.strong_consistency.config import sc_keyspace_opts
from test.cluster.util import commitlog_sector_has_data, commitlog_segments, corrupt_commitlog_segment, new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

SC_KEYSPACE_OPTS = sc_keyspace_opts(replication_factor=1, initial_tablets=1)

# Every test here runs one shard, so every commitlog segment on disk belongs to
# the shard that hosts the strongly consistent group. The refusal in
# finish_replay() is per shard: with more shards, a damaged segment of a shard
# that hosts no group starts the node regardless, and the control test below
# would have to pick its victim by the shard bits of the segment id.
ODSYNC_CMDLINE = [
    '--smp', '1',
    '--commitlog-use-o-dsync', '1',
    '--logger-log-level', 'commitlog=debug',
    '--logger-log-level', 'commitlog_replayer=debug',
]


class Writer:
    """
    Writes rows one after another until stopped, and remembers the ones the
    cluster acknowledged. The write in flight at the crash is neither
    acknowledged nor expected back.
    """

    def __init__(self, cql, ks: str, padding: int):
        self.insert = cql.prepare(f"INSERT INTO {ks}.test (pk, c, padding) VALUES (?, ?, ?)")
        self.cql = cql
        self.padding = 'x' * padding
        self.acked: list[int] = []
        # Set right before the node is killed: from then on a failed write
        # ends the writer instead of failing the test.
        self.crash_expected = False
        self.task: asyncio.Task | None = None

    async def run(self) -> None:
        pk = 0
        while True:
            try:
                await self.cql.run_async(self.insert, (pk, pk * 10, self.padding))
            except Exception as e:
                # Only the crash may fail a write: the one in flight, or the
                # next one, which finds no host. Either way the writer is done.
                assert self.crash_expected, f"write of pk={pk} failed before the crash: {e}"
                return
            self.acked.append(pk)
            pk += 1

    def start(self) -> None:
        self.task = asyncio.create_task(self.run())

    async def stop(self) -> list[int]:
        """Waits for the writer to end on the crash and returns the acknowledged rows."""
        assert self.crash_expected, "the writer ends only on a crash"
        assert self.task is not None
        await self.task
        return self.acked

    async def wait_for_rows(self, count: int, timeout: float = 120) -> None:
        deadline = time.time() + timeout
        while len(self.acked) < count:
            assert time.time() < deadline, f"only {len(self.acked)} rows acknowledged in {timeout}s"
            assert not self.task.done(), "the writer stopped early"
            await asyncio.sleep(0.1)


async def crash_and_restart(manager: ScyllaClusterManager, server, log, writer: Writer,
                            while_down: Callable[[], Awaitable[None]] | None = None) -> int:
    """
    Kills the node under the writer's load, starts it again and waits for the
    user-table replay to finish. Returns the log mark taken before the restart.
    `while_down` runs between the kill and the start.
    """
    mark = await log.mark()
    writer.crash_expected = True
    await manager.server_stop(server.server_id, convict=False)
    if while_down:
        await while_down()
    await manager.server_start(server.server_id)
    await log.wait_for('Log replay complete for user tables', from_mark=mark, timeout=120)
    return mark


async def assert_clean_replay(log, mark: int) -> None:
    """
    The replay met the write frontier of the active segment, which every crash
    leaves behind, and reported it as a truncation and not as damage.
    """
    truncated = await log.grep('Truncated file:', from_mark=mark)
    assert truncated, "the replay met no truncated tail, so the classification under test never ran"
    corrupted = await log.grep(r'Corrupted file( header)?:', from_mark=mark)
    assert not corrupted, f"an intact segment was reported as damaged (SCYLLADB-4853): {[line for line, _ in corrupted]}"
    overridden = await log.grep('strongly_consistent_tables_start_on_damaged_commitlog is set', from_mark=mark)
    assert not overridden, "the node started on the override, which this test does not set"


async def assert_acked_rows_survived(cql, ks: str, acked: list[int]) -> None:
    # One partition at a time: a strongly consistent query cannot span them.
    # The first, the last and a few in between keep the check short; a lost
    # tail of the log would take the last ones first.
    probe = sorted({acked[0], acked[len(acked) // 2], *acked[-5:]})
    for pk in probe:
        got = await cql.run_async(f"SELECT c FROM {ks}.test WHERE pk = {pk}")
        assert len(got) == 1 and got[0].c == pk * 10, f"acknowledged pk={pk} did not survive the crash"


@pytest.mark.asyncio
async def test_crash_on_a_fresh_segment_with_o_dsync(manager: ScyllaClusterManager):
    """
    Path B of SCYLLADB-4853: the node crashes while writing into a segment it
    created in this run, whose tail was pre-written from an uninitialised buffer.
    Replay must read the tail as the end of the log, not as damage, and the node
    must start without the override.

    Large segments and a large budget keep the writer in the first segments the
    node ever allocated, which are the fresh ones.

    The pre-write buffer (allocate_single_buffer() in allocate_segment_ex()) is
    not zeroed, so past the write frontier every 16 KiB holds a non-zero sector.
    The reader skips only all-zero sectors, so that sector fails the CRC check
    and the replay refuses the boot ("segments [...] could not be read in full").
    """
    config = {
        'commitlog_total_space_in_mb': 10000,
    }
    server = await manager.server_add(config=config, cmdline=ODSYNC_CMDLINE)
    cql, _ = await manager.get_ready_cql([server])
    log = await manager.server_open_log(server.server_id)

    async with new_test_keyspace(manager, SC_KEYSPACE_OPTS) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int, padding text);")

        writer = Writer(cql, ks, padding=4096)
        writer.start()
        # Past the first sector of the segment, so that the crash leaves a
        # frontier inside the segment and not at its header.
        await writer.wait_for_rows(50)

        mark = await crash_and_restart(manager, server, log, writer)
        acked = await writer.stop()

        cql = manager.get_cql()
        await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)
        await assert_clean_replay(log, mark)
        await assert_acked_rows_survived(cql, ks, acked)


@pytest.mark.asyncio
async def test_crash_on_a_recycled_segment_with_o_dsync(manager: ScyllaClusterManager):
    """
    Path A of SCYLLADB-4853: the node crashes while writing into a recycled
    segment. A recycled file is already max_size, so it skips the pre-write and
    keeps disk_write_dma_alignment, while its first incarnation was written at
    disk_overwrite_dma_alignment. Where the two differ (XFS, kernel 5.12+,
    512-byte logical sectors), every new sector past the frontier ends with an
    old trailer that fails the new CRC, and replay reports damage.

    The counterpart of test_crash_after_recycling_a_segment with O_DSYNC on: the
    same small segments and budget.  The segment the node writes at the crash
    must be a recycled one, or the test exercises nothing -- and a fresh one
    would meet path B instead.  It is the newest segment whose header names it
    (a recycled spare in the reserve still holds its old header), and the
    commitlog logs every recycled file it takes under the new name.
    """
    config = {
        'commitlog_segment_size_in_mb': 1,
        'commitlog_total_space_in_mb': 5,
    }
    server = await manager.server_add(config=config, cmdline=ODSYNC_CMDLINE)
    cql, _ = await manager.get_ready_cql([server])
    log = await manager.server_open_log(server.server_id)
    workdir = await manager.server_get_workdir(server.server_id)

    async def active_is_recycled() -> str | None:
        """The segment the node writes, if it is a recycled file; None otherwise."""
        segments = commitlog_segments(workdir)
        if not segments:
            return None
        name = os.path.basename(segments[0])
        reused = await log.grep(rf"Using recycled segment file \S+ -> \S*{re.escape(name)} ")
        return segments[0] if reused else None

    async with new_test_keyspace(manager, SC_KEYSPACE_OPTS) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int, padding text);")

        writer = Writer(cql, ks, padding=4096)
        writer.start()
        await log.wait_for('Using recycled segment file', timeout=120)
        # Into a recycled segment: a few sectors past the switch to it, so that
        # the frontier is inside a reused file and not at the switch.
        deadline = time.time() + 120
        while not await active_is_recycled():
            assert time.time() < deadline, "the node never wrote into a recycled segment"
            await writer.wait_for_rows(len(writer.acked) + 10)
        await writer.wait_for_rows(len(writer.acked) + 20)

        async def assert_crashed_on_a_recycled_segment() -> None:
            assert await active_is_recycled(), (
                f"the segment the node wrote at the crash, {commitlog_segments(workdir)[:1]}, is not a recycled one")

        mark = await crash_and_restart(manager, server, log, writer, while_down=assert_crashed_on_a_recycled_segment)
        acked = await writer.stop()

        cql = manager.get_cql()
        await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)
        await assert_clean_replay(log, mark)
        await assert_acked_rows_survived(cql, ks, acked)


@pytest.mark.asyncio
async def test_damaged_segment_refuses_the_boot_and_the_override_starts_it(manager: ScyllaClusterManager):
    """
    The control for the two tests above: a segment that really is damaged. After
    a crash the newest segment with data gets the CRC of its second sector
    flipped. The node must refuse to start, naming the segment and the strongly
    consistent group, and must start once
    strongly_consistent_tables_start_on_damaged_commitlog is set, saying so.

    This is the test/cluster counterpart of the boost test
    test_replay_starts_on_a_damaged_segment_when_the_override_is_set, which
    feeds note_unreadable_segment() by hand; here the damage travels through
    commitlog_replayer. With a single replica the rows past the damaged sector
    are gone for good, so the test asserts only that the table is served again;
    a replica that resyncs from a quorum after the override is a separate test.

    The control runs without O_DSYNC, as the rest of test.py does: with it on,
    the pre-written tail of the segment would be reported as damage too
    (SCYLLADB-4853), and the refusal could not be attributed to the damage this
    test inflicts. Without the pre-write the tail is what the writer left, and
    the only damage in the file is ours.
    """
    config = {
        'commitlog_total_space_in_mb': 10000,
    }
    cmdline = ['--smp', '1', '--logger-log-level', 'commitlog_replayer=debug']
    server = await manager.server_add(config=config, cmdline=cmdline)
    cql, _ = await manager.get_ready_cql([server])
    log = await manager.server_open_log(server.server_id)
    workdir = await manager.server_get_workdir(server.server_id)

    async with new_test_keyspace(manager, SC_KEYSPACE_OPTS) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int, padding text);")

        writer = Writer(cql, ks, padding=4096)
        writer.start()
        await writer.wait_for_rows(50)

        writer.crash_expected = True
        await manager.server_stop(server.server_id, convict=False)
        acked = await writer.stop()

        victims = [s for s in commitlog_segments(workdir) if commitlog_sector_has_data(s, 1)]
        assert victims, "no segment holds data past its first sector"
        victim = victims[0]
        logger.info(f"damaging sector 1 of {victim}, {len(acked)} rows acknowledged")
        corrupt_commitlog_segment(victim, sector=1)

        mark = await log.mark()
        await manager.server_start(server.server_id, expected_error="cannot replay the raft log: segments")
        corrupted = await log.grep('Corrupted file:', from_mark=mark)
        assert corrupted, "the damaged segment was not reported by the replayer"
        assert os.path.basename(victim) in corrupted[0][0], f"the replayer reported another file: {corrupted[0][0]}"

        await manager.server_update_config(server.server_id, 'strongly_consistent_tables_start_on_damaged_commitlog', True)
        mark = await log.mark()
        await manager.server_start(server.server_id)
        await log.wait_for('Starting because strongly_consistent_tables_start_on_damaged_commitlog is set',
                           from_mark=mark, timeout=120)
        await log.wait_for('Log replay complete for user tables', from_mark=mark, timeout=120)

        cql = manager.get_cql()
        await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)
        # Served again; what the damaged sector took is lost by design.
        await cql.run_async(f"SELECT c FROM {ks}.test WHERE pk = {acked[0]}")
        await cql.run_async(f"INSERT INTO {ks}.test (pk, c, padding) VALUES (-1, -10, '')")
        got = await cql.run_async(f"SELECT c FROM {ks}.test WHERE pk = -1")
        assert [r.c for r in got] == [-10]
