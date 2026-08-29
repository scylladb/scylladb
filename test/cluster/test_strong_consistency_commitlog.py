#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for
from test.pylib.tablets import get_tablet_replica
from test.cluster.util import new_test_keyspace, reconnect_driver

import asyncio
import logging
import pytest
import time
import uuid

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_data_survives_crash(manager: ScyllaClusterManager):
    """Verify that SC table data survives a non-graceful crash and is recovered
    from commitlog replay. After a crash, committed raft entries in the commitlog
    must be re-applied to memtables even if they were already snapshotted, because
    the snapshot data may not have been flushed to sstables."""
    config = {
        'experimental_features': ['strongly-consistent-tables'],
        # Prevent automatic memtable flushes so data stays in the commitlog
        # and is not persisted to sstables before the crash.
        'commitlog_total_space_in_mb': 10000,
    }
    cmdline = [
        '--logger-log-level', 'sc_groups_manager=debug',
        '--logger-log-level', 'sc_coordinator=debug',
    ]
    server = await manager.server_add(config=config, cmdline=cmdline)
    (cql, hosts) = await manager.get_ready_cql([server])

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        for pk in range(5):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({pk}, {pk * 10})")

        # Crash the node (non-graceful stop — no flush)
        await manager.server_stop(server.server_id, convict=False)
        await manager.server_start(server.server_id)
        (cql, hosts) = await manager.get_ready_cql([server])

        for pk in range(5):
            rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = {pk};")
            assert len(rows) == 1, f"Expected 1 row for pk={pk}, got {len(rows)}"
            assert rows[0].c == pk * 10, f"pk={pk}: expected c={pk * 10}, got c={rows[0].c}"

        # Verify that the recovered descriptor was persisted during replay.
        # The one-pass replay writes each group's snapshot index and term into
        # its system.raft_groups row from the floor the batch headers carried.
        table_id = await manager.get_table_id(ks.replace('"', ''), "test")
        tablet_rows = await cql.run_async(f"SELECT raft_group_id FROM system.tablets WHERE table_id = {table_id}")
        assert len(tablet_rows) == 1
        group_id = tablet_rows[0].raft_group_id
        snp_rows = await cql.run_async(
            f"SELECT snapshot_idx, snapshot_term FROM system.raft_groups WHERE shard = 0 AND group_id = {group_id}")
        assert len(snp_rows) == 1, f"Expected a row for group {group_id}"
        assert snp_rows[0].snapshot_idx > 0, f"Expected snapshot_idx > 0 after replay, got {snp_rows[0].snapshot_idx}"
        assert snp_rows[0].snapshot_term > 0, f"Expected an exact term with the index, got {snp_rows[0].snapshot_term}"

    await manager.server_stop_gracefully(server.server_id)


@pytest.mark.asyncio
async def test_schema_upgrade_during_replay(manager: ScyllaClusterManager):
    """Verify that SC table data survives a crash even when the schema was altered
    between writes. During commitlog replay, mutations written under the old schema
    must be upgraded to the current schema before being applied to memtables."""
    config = {
        'experimental_features': ['strongly-consistent-tables'],
        # Prevent automatic memtable flushes so data stays in the commitlog
        # and is not persisted to sstables before the crash.
        'commitlog_total_space_in_mb': 10000,
    }
    cmdline = [
        '--logger-log-level', 'sc_groups_manager=debug',
        '--logger-log-level', 'sc_coordinator=debug',
    ]
    server = await manager.server_add(config=config, cmdline=cmdline)
    (cql, hosts) = await manager.get_ready_cql([server])

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        # Write under the original schema
        for pk in range(5):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({pk}, {pk * 10})")

        # ALTER TABLE — adds a new column, changing the schema version
        await cql.run_async(f"ALTER TABLE {ks}.test ADD v text;")

        # Write under the new schema
        for pk in range(5, 10):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, c, v) VALUES ({pk}, {pk * 10}, 'hello')")

        # Crash the node (non-graceful stop — no flush)
        await manager.server_stop(server.server_id, convict=False)
        await manager.server_start(server.server_id)
        (cql, hosts) = await manager.get_ready_cql([server])

        # Verify rows written under the old schema
        for pk in range(5):
            rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = {pk};")
            assert len(rows) == 1, f"Expected 1 row for pk={pk}, got {len(rows)}"
            assert rows[0].c == pk * 10, f"pk={pk}: expected c={pk * 10}, got c={rows[0].c}"
            assert rows[0].v is None, f"pk={pk}: expected v=None, got v={rows[0].v}"

        # Verify rows written under the new schema
        for pk in range(5, 10):
            rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = {pk};")
            assert len(rows) == 1, f"Expected 1 row for pk={pk}, got {len(rows)}"
            assert rows[0].c == pk * 10, f"pk={pk}: expected c={pk * 10}, got c={rows[0].c}"
            assert rows[0].v == 'hello', f"pk={pk}: expected v='hello', got v={rows[0].v}"

    await manager.server_stop_gracefully(server.server_id)


@pytest.mark.asyncio
async def test_double_crash_recovery(manager: ScyllaClusterManager):
    """Verify that SC table data survives two consecutive crashes.
    Write data, crash, restart (commitlog replay restores data), write more data,
    crash again, restart, and verify all data (from both write phases) is present."""
    config = {
        'experimental_features': ['strongly-consistent-tables'],
        'commitlog_total_space_in_mb': 10000,
    }
    cmdline = [
        '--logger-log-level', 'sc_groups_manager=debug',
        '--logger-log-level', 'sc_coordinator=debug',
    ]
    server = await manager.server_add(config=config, cmdline=cmdline)
    (cql, hosts) = await manager.get_ready_cql([server])

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        # Phase 1: Write initial data
        for pk in range(10):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({pk}, {pk * 10})")

        # First crash
        await manager.server_stop(server.server_id, convict=False)
        await manager.server_start(server.server_id)
        (cql, hosts) = await manager.get_ready_cql([server])

        # Verify phase 1 data survived
        for pk in range(10):
            rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = {pk};")
            assert len(rows) == 1, f"After 1st crash: expected 1 row for pk={pk}, got {len(rows)}"
            assert rows[0].c == pk * 10

        # Phase 2: Write more data
        for pk in range(10, 20):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({pk}, {pk * 10})")

        # Second crash
        await manager.server_stop(server.server_id, convict=False)
        await manager.server_start(server.server_id)
        (cql, hosts) = await manager.get_ready_cql([server])

        # Verify all data (both phases) survived
        for pk in range(20):
            rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = {pk};")
            assert len(rows) == 1, f"After 2nd crash: expected 1 row for pk={pk}, got {len(rows)}"
            assert rows[0].c == pk * 10

    await manager.server_stop_gracefully(server.server_id)


@pytest.mark.asyncio
async def test_crash_with_multiple_commitlog_segments(manager: ScyllaClusterManager):
    """Verify crash recovery when data spans multiple commitlog segments.
    Uses a small commitlog segment size to force segment rotation, writes
    enough rows to span multiple segments, crashes, and verifies all data
    is recovered from replaying multiple segment files."""
    config = {
        'experimental_features': ['strongly-consistent-tables'],
        # Small segment size to force multiple segments. The minimum effective
        # segment size is clamped internally, but a low value triggers rotation.
        'commitlog_segment_size_in_mb': 1,
        'commitlog_total_space_in_mb': 10000,
    }
    cmdline = [
        '--logger-log-level', 'sc_groups_manager=debug',
        '--logger-log-level', 'sc_coordinator=debug',
    ]
    server = await manager.server_add(config=config, cmdline=cmdline)
    (cql, hosts) = await manager.get_ready_cql([server])

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int, padding text);")

        # Write enough rows with padding to span multiple segments.
        num_rows = 200
        padding = 'x' * 4096  # 4KB padding per row to fill segments faster
        for pk in range(num_rows):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, c, padding) VALUES ({pk}, {pk * 10}, '{padding}')")

        # Crash
        await manager.server_stop(server.server_id, convict=False)
        await manager.server_start(server.server_id)
        (cql, hosts) = await manager.get_ready_cql([server])

        # Verify all rows survived
        rows_by_pk = {}
        for pk in range(num_rows):
            rows = await cql.run_async(f"SELECT pk, c FROM {ks}.test WHERE pk = {pk};")
            assert len(rows) == 1, f"Missing row for pk={pk}"
            rows_by_pk[pk] = rows[0].c
        assert len(rows_by_pk) == num_rows, f"Expected {num_rows} rows, got {len(rows_by_pk)}"
        for pk in range(num_rows):
            assert rows_by_pk[pk] == pk * 10, f"pk={pk}: expected c={pk * 10}, got c={rows_by_pk[pk]}"

    await manager.server_stop_gracefully(server.server_id)


@pytest.mark.asyncio
async def test_crash_recovery_multi_tablet(manager: ScyllaClusterManager):
    """Verify crash recovery with multiple tablets (independent raft groups).
    Creates a table with 4 tablets, writes data distributed across all tablets,
    crashes, and verifies all data is recovered — testing that commitlog replay
    correctly routes entries to multiple independent raft groups."""
    config = {
        'experimental_features': ['strongly-consistent-tables'],
        'commitlog_total_space_in_mb': 10000,
    }
    cmdline = [
        '--logger-log-level', 'sc_groups_manager=debug',
        '--logger-log-level', 'sc_coordinator=debug',
    ]
    server = await manager.server_add(config=config, cmdline=cmdline)
    (cql, hosts) = await manager.get_ready_cql([server])

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 4} AND consistency = 'global'") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        # Write enough rows to ensure data lands on different tablets.
        num_rows = 40
        for pk in range(num_rows):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({pk}, {pk * 10})")

        # Crash
        await manager.server_stop(server.server_id, convict=False)
        await manager.server_start(server.server_id)
        (cql, hosts) = await manager.get_ready_cql([server])

        # Verify all rows survived
        rows_by_pk = {}
        for pk in range(num_rows):
            rows = await cql.run_async(f"SELECT pk, c FROM {ks}.test WHERE pk = {pk};")
            assert len(rows) == 1, f"Missing row for pk={pk}"
            rows_by_pk[pk] = rows[0].c
        assert len(rows_by_pk) == num_rows, f"Expected {num_rows} rows, got {len(rows_by_pk)}"
        for pk in range(num_rows):
            assert rows_by_pk[pk] == pk * 10, f"pk={pk}: expected c={pk * 10}, got c={rows_by_pk[pk]}"

    await manager.server_stop_gracefully(server.server_id)


@pytest.mark.asyncio
async def test_crash_recovery_after_flush(manager: ScyllaClusterManager):
    """Verify crash recovery when some data was flushed to sstables before the crash.
    Write data, flush it to sstables (so it is persisted on disk), then write
    more data (which exists only in the commitlog), crash, and verify both the
    flushed and unflushed data are present after recovery."""
    config = {
        'experimental_features': ['strongly-consistent-tables'],
        'commitlog_total_space_in_mb': 10000,
    }
    cmdline = [
        '--logger-log-level', 'sc_groups_manager=debug',
        '--logger-log-level', 'sc_coordinator=debug',
    ]
    server = await manager.server_add(config=config, cmdline=cmdline)
    (cql, hosts) = await manager.get_ready_cql([server])

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        ks_name = ks.replace('"', '')  # strip quotes for API call
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        # Phase 1: Write data and flush to sstables
        for pk in range(10):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({pk}, {pk * 10})")
        await manager.api.keyspace_flush(server.ip_addr, ks_name)

        # Phase 2: Write more data (only in commitlog, not flushed)
        for pk in range(10, 20):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({pk}, {pk * 10})")

        # Crash (non-graceful — phase 2 data is only in commitlog)
        await manager.server_stop(server.server_id, convict=False)
        await manager.server_start(server.server_id)
        (cql, hosts) = await manager.get_ready_cql([server])

        # Verify all data: flushed (phase 1) + replayed from commitlog (phase 2)
        for pk in range(20):
            rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = {pk};")
            assert len(rows) == 1, f"Expected 1 row for pk={pk}, got {len(rows)}"
            assert rows[0].c == pk * 10, f"pk={pk}: expected c={pk * 10}, got c={rows[0].c}"

    await manager.server_stop_gracefully(server.server_id)


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_truncated_copies_are_dropped_on_replay(manager: ScyllaClusterManager):
    """A leader change discards uncommitted entries, but their bytes stay in the
    old leader's commitlog — the commitlog is append-only. The group persists a
    truncation record saying which indexes of which segment were superseded, and
    that record is the only thing that tells a later replay which copies to drop.

    Drive the whole path: isolate the leader so its writes cannot commit, let a
    new leader take over and reuse those indexes, bring the old leader back so it
    truncates, then crash it so replay has to use the records."""
    config = {
        'experimental_features': ['strongly-consistent-tables'],
        # Keep the entries in the commitlog rather than letting a flush move them
        # to sstables before the crash.
        'commitlog_total_space_in_mb': 10000,
    }
    cmdline = [
        '--logger-log-level', 'raft_groups_storage=debug',
        '--logger-log-level', 'raft_commitlog=debug',
        '--logger-log-level', 'raft_commitlog_replay=debug',
        # "Dropping append request" is logged here, and the test waits for it.
        '--logger-log-level', 'raft_group_registry=debug',
    ]
    # Distinct racks, so replication_factor 3 is allowed.
    servers = await manager.servers_add(3, config=config, cmdline=cmdline, property_file=[
        {"dc": "dc1", "rack": "r1"}, {"dc": "dc1", "rack": "r2"}, {"dc": "dc1", "rack": "r3"}])
    cql, hosts = await manager.get_ready_cql(servers)
    host_by_id = {str(await manager.get_host_id(s.server_id)): (s, h) for s, h in zip(servers, hosts)}

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                 "'replication_factor': 3} AND tablets = {'initial': 1} "
                                 "AND consistency = 'global'") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        # Committed baseline, so the group has a floor and a released record.
        for pk in range(5):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({pk}, {pk * 10})")

        table_id = await manager.get_table_id(ks, "test")
        rows = await cql.run_async(f"SELECT raft_group_id FROM system.tablets WHERE table_id = {table_id}")
        group_id = str(rows[0].raft_group_id)

        async def leader_of(group: str) -> str:
            for s in servers:
                try:
                    result = await manager.api.get_raft_leader(s.ip_addr, group)
                except Exception:
                    continue
                if uuid.UUID(result).int != 0:
                    return result
            return None

        leader_id = await wait_for(lambda: leader_of(group_id), time.time() + 60)
        old_leader, old_leader_host = host_by_id[leader_id]
        followers = [s for s in servers if s.server_id != old_leader.server_id]
        logger.info(f"group {group_id}: leader is {old_leader.ip_addr}")

        # Cut the leader off from a quorum for this group: its next entries are
        # appended locally, and to its commitlog, but can never commit.
        for f in followers:
            await manager.api.enable_injection(f.ip_addr, "raft_drop_incoming_append_entries_for_specified_group",
                                               one_shot=False, parameters={"value": group_id})
        follower_logs = [await manager.server_open_log(f.server_id) for f in followers]
        marks = [await log.mark() for log in follower_logs]

        # These writes never commit. Fire them without waiting.
        doomed = [cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({100 + i}, {999})",
                                host=old_leader_host) for i in range(3)]
        for log, mark in zip(follower_logs, marks):
            await log.wait_for(rf"Dropping append request .* for group {group_id}",
                               from_mark=mark, timeout=120)

        # Take the old leader out so the followers can elect among themselves,
        # and let the doomed writes fail.
        await manager.server_stop(old_leader.server_id, convict=False)
        for task in doomed:
            try:
                await asyncio.wait_for(task, timeout=60)
            except Exception as e:
                logger.debug(f"doomed write failed as expected: {e}")
        for f in followers:
            await manager.api.disable_injection(f.ip_addr, "raft_drop_incoming_append_entries_for_specified_group")

        # The new leader reuses the indexes the old leader had used, with its own
        # entries, and commits them.
        cql, _ = await manager.get_ready_cql(followers)

        # A surviving follower keeps reporting the stopped leader until an
        # election actually happens, so wait for a leader that is a different
        # node rather than for any leader at all.
        async def elected_new_leader():
            for s in followers:
                try:
                    result = await manager.api.get_raft_leader(s.ip_addr, group_id)
                except Exception:
                    continue
                if uuid.UUID(result).int != 0 and result != leader_id:
                    return result
            return None

        new_leader_id = await wait_for(elected_new_leader, time.time() + 120)
        logger.info(f"group {group_id}: new leader is {new_leader_id}")
        # The coordinator's leader cache can still point at the stopped node for a
        # moment, so a write may be forwarded to it and fail; retry until the
        # cache catches up.
        async def write_with_retry(stmt: str, deadline: float):
            while True:
                try:
                    await cql.run_async(stmt)
                    return
                except Exception as e:
                    if time.time() > deadline:
                        raise
                    logger.debug(f"retrying write after: {e}")
                    await asyncio.sleep(0.5)

        for pk in range(5, 10):
            await write_with_retry(f"INSERT INTO {ks}.test (pk, c) VALUES ({pk}, {pk * 10})",
                                   time.time() + 120)

        # Bring the old leader back. Its log conflicts with the new leader's, so
        # it truncates — and records what it discarded.
        old_log = await manager.server_open_log(old_leader.server_id)
        old_mark = await old_log.mark()
        await manager.server_start(old_leader.server_id)
        await manager.servers_see_each_other(servers)
        await old_log.wait_for(rf"truncate_log: group_id={group_id}", from_mark=old_mark, timeout=180)
        logger.info("old leader truncated its conflicting tail")

        # Crash it, so its commitlog holds both copies of those indexes and the
        # only way to tell them apart is the truncation records it persisted.
        await manager.server_stop(old_leader.server_id, convict=False)
        replay_mark = await old_log.mark()
        await manager.server_start(old_leader.server_id)
        await manager.servers_see_each_other(servers)

        # Replay must have got rid of the superseded copies rather than keeping
        # them. Two mechanisms can do it, and which one fires depends on whether
        # the truncation records had been persisted yet: they only reach the row
        # when a record is released, and with a dataset this small no release
        # happens between the truncation and the crash. So the copies here are
        # dropped by the supersede rule — a later write at an index replaces
        # anything buffered at or above it — while the records cover the indexes
        # that were truncated and never written again.
        await old_log.wait_for(r"dropped_stale=[1-9]|superseded=[1-9]",
                               from_mark=replay_mark, timeout=180)

        # And the data must be the new leader's: the doomed rows were never
        # committed by anyone, so they must not appear on any replica.
        cql = await reconnect_driver(manager)
        cql, hosts = await manager.get_ready_cql(servers)
        # Strongly consistent reads target one partition at a time.
        for host in hosts:
            for pk in range(10):
                rows = await cql.run_async(f"SELECT pk, c FROM {ks}.test WHERE pk = {pk}", host=host)
                assert len(rows) == 1, f"host {host}: expected pk={pk} to be present"
                assert rows[0].c == pk * 10, f"host {host}: pk={pk} has c={rows[0].c}"
            # The doomed writes were never committed by any leader, so they must
            # not have come back with the truncated copies.
            for pk in range(100, 103):
                rows = await cql.run_async(f"SELECT pk FROM {ks}.test WHERE pk = {pk}", host=host)
                assert len(rows) == 0, f"host {host}: never-committed pk={pk} was resurrected"


@pytest.mark.asyncio
async def test_migrated_group_is_not_resurrected_on_replay(manager: ScyllaClusterManager):
    """A tablet that migrates to another shard leaves its raft entries behind in
    the old shard's commitlog, and on the way out the group gives up its segment
    references — so the segments still holding those entries can be reclaimed
    while the group's row keeps the index it reached.

    Replay on that shard must notice it no longer hosts the tablet and discard
    the entries. Resolving the group instead would apply them into a range the
    shard no longer owns, and — once the older segments are gone and only the
    tail survives — would leave a hole below the row's floor, which replay
    treats as an internal error and refuses to start on."""
    config = {
        'experimental_features': ['strongly-consistent-tables'],
        # Keep the entries in the commitlog rather than in sstables.
        'commitlog_total_space_in_mb': 10000,
    }
    cmdline = [
        '--smp=2',
        '--logger-log-level', 'raft_commitlog_replay=debug',
        '--logger-log-level', 'sc_groups_manager=debug',
    ]
    server = await manager.server_add(config=config, cmdline=cmdline)
    (cql, hosts) = await manager.get_ready_cql([server])
    await manager.disable_tablet_balancing()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                 "'replication_factor': 1} AND tablets = {'initial': 1} "
                                 "AND consistency = 'global'") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        for pk in range(5):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({pk}, {pk * 10})")

        table_id = await manager.get_table_id(ks.replace('"', ''), "test")
        tablet_rows = await cql.run_async(f"SELECT raft_group_id FROM system.tablets WHERE table_id = {table_id}")
        assert len(tablet_rows) == 1
        group_id = str(tablet_rows[0].raft_group_id)

        # Move the tablet to the other shard of the same node. The old shard's
        # commitlog keeps the entries it already wrote for this group.
        tablet_token = 0  # only one tablet
        host_id, src_shard = await get_tablet_replica(manager, server, ks, 'test', tablet_token)
        dst_shard = src_shard ^ 1
        await manager.api.move_tablet(server.ip_addr, ks, "test", host_id, src_shard, host_id, dst_shard, tablet_token)
        logger.info(f"group {group_id}: moved tablet from shard {src_shard} to {dst_shard}")

        # Crash, so those entries have to go through replay.
        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()
        await manager.server_stop(server.server_id, convict=False)
        await manager.server_start(server.server_id)
        await reconnect_driver(manager)
        cql = manager.get_cql()

        # The shard that no longer hosts the tablet must have dropped them.
        await log.wait_for(rf"group {group_id} is not hosted on this shard", from_mark=mark, timeout=120)

        # And the data must be intact, served from the shard that now owns it.
        for pk in range(5):
            rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = {pk};")
            assert len(rows) == 1, f"Expected 1 row for pk={pk}, got {len(rows)}"
            assert rows[0].c == pk * 10, f"pk={pk}: expected c={pk * 10}, got c={rows[0].c}"
