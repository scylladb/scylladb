#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Backup and restore of strongly consistent (SC) tables.

A backup captures sstables and a restore loads them, both around the Raft log of an SC
tablet: a write is durable once committed to that log, the sstables only hold what was
applied, and a restore hands the data to the replicas itself. These tests check where that
meets the SC guarantees: a snapshot taken right after acked writes, the tablet transition
stage a tablet-aware restore runs in, a restore into a table with another tablet count,
a restore that hands the data to one replica only, reads while load-and-stream is still
filling the replicas, and a cluster backup of replicas that were not in step.
"""
import asyncio
import logging
import os
import time

import pytest
from cassandra import InvalidRequest
from cassandra.cluster import ConsistencyLevel

from test.cluster.object_store.test_backup import (topo, create_cluster, take_snapshot, do_backup, do_restore_server,
                                                   sc_wait_applied, table_raft_group_ids, run_cluster_backup)
from test.cluster.test_refresh import SSTablesOnLocalStorage
from test.cluster.test_strong_consistency import DEFAULT_CONFIG, DEFAULT_CMDLINE, get_table_raft_group_id
from test.cluster.util import new_test_keyspace, ensure_raft_group_leader_on, wait_for_cql_and_get_hosts
from test.pylib.rest_client import HTTPError
from test.pylib.tablets import get_all_tablet_replicas
from test.pylib.util import unique_name, wait_for
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

# SC needs RF 3, and rf_rack_valid_keyspaces (on by default) wants one rack per replica.
TOPOLOGY = topo(rf=3, nodes=3, racks=3, dcs=1)
NUM_KEYS = 20


def ks_opts(tablets: int) -> str:
    return ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
            f" AND tablets = {{'initial': {tablets}}} AND consistency = 'global'")


async def start_cluster(manager: ScyllaClusterManager, object_storage):
    servers, _ = await create_cluster(TOPOLOGY, manager, logger, object_storage,
                                      extra_config=DEFAULT_CONFIG, extra_cmdline=DEFAULT_CMDLINE)
    return servers


async def populate(cql, ks: str, cf: str, keys, table_opts: str = '') -> dict[str, int]:
    """Create ks.cf and write keys at QUORUM, the only CL an SC write accepts."""
    await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk text PRIMARY KEY, value int){table_opts}")
    insert = cql.prepare(f"INSERT INTO {ks}.{cf} (pk, value) VALUES (?, ?)")
    insert.consistency_level = ConsistencyLevel.QUORUM
    await asyncio.gather(*(cql.run_async(insert, (str(k), k)) for k in keys))
    return {str(k): k for k in keys}


async def read_rows(cql, ks: str, cf: str, keys) -> dict[str, int]:
    """Read every key at QUORUM, one partition at a time: SC serves single-partition reads."""
    select = cql.prepare(f"SELECT pk, value FROM {ks}.{cf} WHERE pk = ?")
    select.consistency_level = ConsistencyLevel.QUORUM
    results = await asyncio.gather(*(cql.run_async(select, (str(k),)) for k in keys))
    return {row.pk: row.value for rows in results for row in rows}


async def replicas_with_sstables(manager: ScyllaClusterManager, servers, ks: str, cf: str) -> set:
    """The servers whose data directory holds at least one sstable of ks.cf."""
    found = set()
    for s in servers:
        workdir = await manager.server_get_workdir(s.server_id)
        cf_dir = next(d for d in os.listdir(f'{workdir}/data/{ks}') if d.startswith(f'{cf}-'))
        if any(f.name.endswith('-Data.db') for f in os.scandir(f'{workdir}/data/{ks}/{cf_dir}')):
            found.add(s.server_id)
    return found


@pytest.mark.skip_bug(
    link="https://scylladb.atlassian.net/browse/SCYLLADB-5951",
    reason="the SC replica selector has no case for the restore transition stage: every request aborts its coordinator",
)
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_requests_during_tablet_restore(manager: ScyllaClusterManager, object_storage):
    """Idea: a tablet-aware restore keeps every tablet of the destination table in the
    `restore` transition stage until the last replica has downloaded its sstables. An SC table
    must stay available meanwhile: a request routed to such a tablet is served or refused with
    a client error, never an internal one, and the restored rows are visible at QUORUM
    afterwards and survive a replica restart.

    Setup: 3 nodes in 3 racks, SC keyspace with RF 3 and 2 tablets per table. `src` is written
    at QUORUM, snapshotted and backed up from every node to the object store; `dst` is an empty
    table of the same schema that the backup is restored into.

    The restore is parked on every replica with the `pause_tablet_restore` injection, so the
    tablets stay in the `restore` stage while the test writes one new row to `dst` and reads
    one key from it and one from `src`. Errors are collected rather than raised, so that the
    restore is released before any assertion: a parked restore would also hang the keyspace
    drop. Test clusters run with --abort-on-internal-error, so an internal error shows up as
    a dead coordinator and a connection error on the probe. Once the restore is done the test
    checks that `dst` holds the backed-up rows plus the row written meanwhile, that every
    replica holds sstables of `dst`, and that a graceful restart of one replica brings it back
    with the same rows readable at QUORUM. Linearizability is not checked here, only
    availability and read-after-write visibility.
    """
    servers = await start_cluster(manager, object_storage)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, ks_opts(tablets=2)) as ks:
        expected = await populate(cql, ks, 'src', range(NUM_KEYS))
        await sc_wait_applied(manager, servers, ks, 'src')
        # Snapshot before dst exists: take_snapshot() expects one table directory in the keyspace.
        snap_name, _ = await take_snapshot(ks, servers, manager, logger)
        await asyncio.gather(*(do_backup(s, snap_name, f'{s.server_id}/{snap_name}', ks, 'src', object_storage, manager, logger)
                               for s in servers))
        manifests = [f'{s.server_id}/{snap_name}/manifest.json' for s in servers]
        await cql.run_async(f"CREATE TABLE {ks}.dst (pk text PRIMARY KEY, value int)")

        # Park the restore on every replica once it has picked the sstables of its tablet.
        await asyncio.gather(*(manager.api.enable_injection(s.ip_addr, 'pause_tablet_restore', False) for s in servers))
        log = await manager.server_open_log(servers[0].server_id)
        mark = await log.mark()
        tid = await manager.api.restore_tablets(servers[0].ip_addr, ks, 'dst', snap_name, servers[0].datacenter,
                                                object_storage.address, object_storage.bucket_name, manifests)
        await log.wait_for('pause_tablet_restore: waiting for message', from_mark=mark, timeout=60)

        # Requests while the tablets of dst sit in the restore stage.
        errors, refused = {}, {}

        async def probe(name, awaitable):
            try:
                await awaitable
            except InvalidRequest as e:
                refused[name] = repr(e)
            except Exception as e:
                errors[name] = repr(e)

        insert = cql.prepare(f"INSERT INTO {ks}.dst (pk, value) VALUES (?, ?)")
        insert.consistency_level = ConsistencyLevel.QUORUM
        await asyncio.gather(probe('write dst', cql.run_async(insert, ('during', -1))),
                             probe('read dst', read_rows(cql, ks, 'dst', ['0'])),
                             probe('read src', read_rows(cql, ks, 'src', ['0'])))

        # Release the restore before asserting anything, then let it finish. A node that died on
        # a probe refuses the REST call, so the probe errors are reported first.
        released = await asyncio.gather(*(manager.api.disable_injection(s.ip_addr, 'pause_tablet_restore') for s in servers),
                                        return_exceptions=True)
        assert not errors, f'requests failed while dst was being restored: {errors}'
        assert not any(isinstance(r, Exception) for r in released), f'releasing the restore failed: {released}'
        status = await manager.api.wait_task(servers[0].ip_addr, tid)
        assert status is not None and status['state'] == 'done', f'restore ended as {status}'

        expected_dst = expected if 'write dst' in refused else expected | {'during': -1}
        assert await read_rows(cql, ks, 'dst', expected_dst) == expected_dst
        assert await replicas_with_sstables(manager, servers, ks, 'dst') == {s.server_id for s in servers}

        # A replica restarted after the restore comes back with the rows.
        await manager.server_restart(servers[1].server_id, wait_others=2)
        await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
        assert await read_rows(cql, ks, 'dst', expected_dst) == expected_dst


@pytest.mark.skip_bug(
    link="https://scylladb.atlassian.net/browse/SCYLLADB-5952",
    reason="the data lands on one replica and nothing brings it to the others: tablet repair is unsupported for SC",
)
@pytest.mark.parametrize("via", ['restore', 'refresh'])
async def test_primary_replica_only_restore(manager: ScyllaClusterManager, object_storage, via):
    """Idea: `primary_replica_only` hands the restored sstables to one replica per tablet and
    relies on a later repair to reach the others. An SC tablet has no such repair, and Raft
    never replicates data that bypassed its log, so either the option is refused for an SC
    table or every replica must end up with the data. This test accepts either: a refused
    restore that loads nothing, or a restore after which the rows are readable at QUORUM
    whichever replica leads the tablet's Raft group.

    Setup: 3 nodes in 3 racks, two SC keyspaces with RF 3 and one tablet per table, so that
    `dst` is a single Raft group whose leader the test can place. `src` is written at QUORUM,
    snapshotted and backed up from every node; the backup is restored into the empty `dst`
    from every node with primary_replica_only and scope 'all', through the object storage
    restore API or through `nodetool refresh --load-and-stream` from the upload directory.

    The test then makes each replica in turn the leader of dst's group and reads every key at
    QUORUM. The assertion is that each leader serves the whole backed-up set; the failure
    message names the leaders that miss rows and the replicas that actually hold sstables.
    """
    servers = await start_cluster(manager, object_storage)
    cql = manager.get_cql()
    local = SSTablesOnLocalStorage()
    async with new_test_keyspace(manager, ks_opts(tablets=1)) as src_ks:
        expected = await populate(cql, src_ks, 't', range(NUM_KEYS))
        await sc_wait_applied(manager, servers, src_ks, 't')
        snap_name, tocs = await take_snapshot(src_ks, servers, manager, logger)
        prefix = f't/{snap_name}'
        if via == 'restore':
            await asyncio.gather(*(do_backup(s, snap_name, prefix, src_ks, 't', object_storage, manager, logger) for s in servers))
        else:
            await local.save(manager, servers, snap_name, prefix, src_ks, 't', logger)

    # The refresh helper locates the table directory as the keyspace's only one.
    async with new_test_keyspace(manager, ks_opts(tablets=1)) as dst_ks:
        await cql.run_async(f"CREATE TABLE {dst_ks}.t (pk text PRIMARY KEY, value int)")
        if via == 'restore':
            restores = [do_restore_server(manager, logger, dst_ks, 't', s, tocs[s], 'all', True, prefix, object_storage)
                        for s in servers]
        else:
            restores = [local.refresh_one(manager, s, dst_ks, 't', tocs[s], 'all', True) for s in servers]
        # Let every restore end before looking at the sstables: a refusal on one node must not
        # be checked while the others are still loading.
        results = await asyncio.gather(*restores, return_exceptions=True)
        failures = [r for r in results if isinstance(r, BaseException)]
        for f in failures:
            if not isinstance(f, HTTPError):
                raise f
        if failures:
            # Refused up front: then nothing may have been loaded either.
            holders = await replicas_with_sstables(manager, servers, dst_ks, 't')
            assert not holders, f'restore was refused ({failures[0]}) but {sorted(holders)} hold sstables of dst'
            return

        holders = await replicas_with_sstables(manager, servers, dst_ks, 't')
        group_id = await get_table_raft_group_id(manager, dst_ks, 't')
        missing = {}
        for s in servers:
            await ensure_raft_group_leader_on(manager, s, group_id)
            rows = await read_rows(cql, dst_ks, 't', expected)
            if rows != expected:
                missing[s.server_id] = sorted(set(expected) - set(rows), key=int)
        assert not missing, (f'leaders missing restored rows: {missing}; replicas holding sstables of dst: '
                             f'{sorted(holders)} of {sorted(s.server_id for s in servers)}')


@pytest.mark.skip_bug(
    link="https://scylladb.atlassian.net/browse/SCYLLADB-5954",
    reason="the restore task waits for a tablet resize that an SC table never does",
)
async def test_restore_into_different_tablet_count(manager: ScyllaClusterManager, object_storage):
    """Idea: a tablet-aware restore pins the destination's tablet hints to the backup's tablet
    count and waits for the balancer to resize the table. An SC tablet can't be split or
    merged yet, so a destination with another tablet count has to be refused up front; what
    the restore must not do is wait for that resize without end.

    Setup: 3 nodes in 3 racks, SC keyspace with RF 3. `src` is created with 8 tablets, written
    at QUORUM and backed up from every node; `dst` gets the keyspace default of 4 tablets.

    The test asks for the restore of `src` into `dst` and gives it a minute. It passes when the
    request is refused, or when the task it returns ends: a task that ends done must have
    restored every row. A task still running after the minute is aborted and fails the test.
    """
    servers = await start_cluster(manager, object_storage)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, ks_opts(tablets=4)) as ks:
        expected = await populate(cql, ks, 'src', range(NUM_KEYS), " WITH tablets = {'min_tablet_count': 8}")
        await sc_wait_applied(manager, servers, ks, 'src')
        # Snapshot before dst exists: take_snapshot() expects one table directory in the keyspace.
        snap_name, _ = await take_snapshot(ks, servers, manager, logger)
        await asyncio.gather(*(do_backup(s, snap_name, f'{s.server_id}/{snap_name}', ks, 'src', object_storage, manager, logger)
                               for s in servers))
        manifests = [f'{s.server_id}/{snap_name}/manifest.json' for s in servers]
        await cql.run_async(f"CREATE TABLE {ks}.dst (pk text PRIMARY KEY, value int)")
        src_tablets = len(await get_all_tablet_replicas(manager, servers[0], ks, 'src'))
        dst_tablets = len(await get_all_tablet_replicas(manager, servers[0], ks, 'dst'))
        assert src_tablets != dst_tablets, f'src and dst both have {src_tablets} tablets, the test needs them to differ'

        try:
            tid = await manager.api.restore_tablets(servers[0].ip_addr, ks, 'dst', snap_name, servers[0].datacenter,
                                                    object_storage.address, object_storage.bucket_name, manifests)
        except HTTPError as e:
            logger.info(f'restore into a table with {dst_tablets} tablets from a backup of {src_tablets} was refused: {e}')
            return

        # The request returns a task at once; the wait for the resize happens inside the task.
        deadline = time.time() + 60
        while (status := await manager.api.get_task_status(servers[0].ip_addr, tid))['state'] not in ('done', 'failed'):
            if time.time() > deadline:
                await manager.api.abort_task(servers[0].ip_addr, tid)
                pytest.fail(f'restore of a {src_tablets}-tablet backup into a {dst_tablets}-tablet SC table is still '
                            f'{status["state"]} after 60 s: it waits for a resize an SC table never does')
            await asyncio.sleep(1)
        if status['state'] == 'done':
            assert await read_rows(cql, ks, 'dst', expected) == expected


@pytest.mark.skip_bug(
    link="https://scylladb.atlassian.net/browse/SCYLLADB-5955",
    reason="a snapshot flushes the memtables without waiting for committed writes to be applied",
)
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_backup_right_after_acked_writes(manager: ScyllaClusterManager, object_storage):
    """Idea: a backup must hold every write the client was told is durable before the backup
    began. An EC write at QUORUM sits in the memtables of a quorum of replicas before it is
    acked, so a snapshot of every node captures it. An SC write is acked once its Raft entry
    is committed and applied to the memtables only afterwards, and a snapshot flushes the
    memtables without waiting for that: between commit and apply an acked write can be missing
    from every replica's snapshot, and the Raft log that still holds it is not part of any
    backup.

    Setup: 3 nodes in 3 racks, SC keyspace with RF 3 and 2 tablets per table. `src` gets one
    warm-up row, applied everywhere, so that its Raft groups are up and led. Then the
    `strong_consistency_state_machine_wait_before_apply` injection parks apply() on every
    replica and the test writes NUM_KEYS rows at QUORUM: all of them are acked, none is
    applied anywhere.

    With apply still parked the test snapshots and backs `src` up from every node, then
    releases apply, checks that `src` serves the rows (they were committed) and restores the
    backup into the empty `dst`. The assertion is that `dst` holds the warm-up row and every
    acked row; the failure message lists the acked rows the backup missed.
    """
    servers = await start_cluster(manager, object_storage)
    cql = manager.get_cql()
    injection = 'strong_consistency_state_machine_wait_before_apply'
    async with new_test_keyspace(manager, ks_opts(tablets=2)) as ks:
        expected = await populate(cql, ks, 'src', [-1])
        await sc_wait_applied(manager, servers, ks, 'src')

        # Every replica parks apply(): the writes below commit and are acked, nothing lands in a memtable.
        await asyncio.gather(*(manager.api.enable_injection(s.ip_addr, injection, False) for s in servers))
        insert = cql.prepare(f"INSERT INTO {ks}.src (pk, value) VALUES (?, ?)")
        insert.consistency_level = ConsistencyLevel.QUORUM
        await asyncio.gather(*(cql.run_async(insert, (str(k), k)) for k in range(NUM_KEYS)))
        expected |= {str(k): k for k in range(NUM_KEYS)}

        # The backup is taken while the acked writes are still unapplied.
        snap_name, _ = await take_snapshot(ks, servers, manager, logger)
        await asyncio.gather(*(do_backup(s, snap_name, f'{s.server_id}/{snap_name}', ks, 'src', object_storage, manager, logger)
                               for s in servers))
        manifests = [f'{s.server_id}/{snap_name}/manifest.json' for s in servers]

        await asyncio.gather(*(manager.api.disable_injection(s.ip_addr, injection) for s in servers))
        assert await read_rows(cql, ks, 'src', expected) == expected, 'acked writes are not readable once apply resumes'

        await cql.run_async(f"CREATE TABLE {ks}.dst (pk text PRIMARY KEY, value int)")
        tid = await manager.api.restore_tablets(servers[0].ip_addr, ks, 'dst', snap_name, servers[0].datacenter,
                                                object_storage.address, object_storage.bucket_name, manifests)
        status = await manager.api.wait_task(servers[0].ip_addr, tid)
        assert status is not None and status['state'] == 'done', f'restore ended as {status}'
        restored = await read_rows(cql, ks, 'dst', expected)
        missing = sorted(set(expected) - set(restored), key=int)
        assert not missing, f'the backup taken after {NUM_KEYS} acked writes misses {len(missing)} of them: {missing}'


@pytest.mark.skip_bug(
    link="https://scylladb.atlassian.net/browse/SCYLLADB-5958",
    reason="load-and-stream fills the replicas one by one and a leader change exposes the difference",
)
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_reads_during_load_and_stream_restore(manager: ScyllaClusterManager, object_storage):
    """Idea: load-and-stream fills each replica on its own, so for a while some replicas of an
    SC tablet hold the restored rows and some do not. A linearizable read is served by the
    leader from its own data, so a row served once must stay served when the leadership moves
    to a replica the stream has not reached yet; otherwise the table's history goes back in
    time, which no sequence of acked writes and reads of an SC table may do.

    Setup: 3 nodes in 3 racks, SC keyspace with RF 3 and one tablet per table. `src` is
    written at QUORUM, applied everywhere, snapshotted and backed up from every node; `dst` is
    empty. The `stream_mutation_fragments` injection parks the stream receiver on one replica
    (`late`), and the object storage restore (load-and-stream, scope all) is started from
    every node, so the two other replicas receive every row while `late` receives nothing.

    With the leadership of dst's group on a replica that has the rows, the test waits until a
    QUORUM read serves all of them, then moves the leadership to `late` and reads again. The
    stream is released and the restores complete before the assertion: the rows the new leader
    served must be the rows the previous leader served. Linearizability of the whole history
    is not checked, only this one monotonic pair of reads.
    """
    servers = await start_cluster(manager, object_storage)
    cql = manager.get_cql()
    injection = 'stream_mutation_fragments'
    async with new_test_keyspace(manager, ks_opts(tablets=1)) as ks:
        expected = await populate(cql, ks, 'src', range(NUM_KEYS))
        await sc_wait_applied(manager, servers, ks, 'src')
        # Snapshot before dst exists: take_snapshot() expects one table directory in the keyspace.
        snap_name, tocs = await take_snapshot(ks, servers, manager, logger)
        prefix = f'src/{snap_name}'
        await asyncio.gather(*(do_backup(s, snap_name, prefix, ks, 'src', object_storage, manager, logger) for s in servers))
        await cql.run_async(f"CREATE TABLE {ks}.dst (pk text PRIMARY KEY, value int)")

        late = servers[2]
        await manager.api.enable_injection(late.ip_addr, injection, False)
        restores = [asyncio.create_task(do_restore_server(manager, logger, ks, 'dst', s, tocs[s], 'all', False, prefix, object_storage))
                    for s in servers]
        await manager.api.wait_for_injection_enter(late.ip_addr, injection)

        group_id = await get_table_raft_group_id(manager, ks, 'dst')
        await ensure_raft_group_leader_on(manager, servers[0], group_id)

        async def all_rows_served():
            rows = await read_rows(cql, ks, 'dst', expected)
            return rows if rows == expected else None
        served_first = await wait_for(all_rows_served, time.time() + 60)

        await ensure_raft_group_leader_on(manager, late, group_id)
        served_by_late = await read_rows(cql, ks, 'dst', expected)

        # Let the stream reach `late` and the restores finish before judging the reads.
        await manager.api.message_injection(late.ip_addr, injection)
        await manager.api.disable_injection(late.ip_addr, injection)
        await asyncio.gather(*restores)
        assert await read_rows(cql, ks, 'dst', expected) == expected
        assert served_by_late == served_first, (f'a leader served {len(served_first)} restored rows, then the next leader '
                                                f'served {len(served_by_late)} of them: the reads went back in time')


@pytest.mark.skip_bug(
    link="https://scylladb.atlassian.net/browse/SCYLLADB-5968",
    reason="the cluster backup keeps each node's own sstables and the restore gives them back per replica",
)
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_cluster_backup_of_lagging_replica(manager: ScyllaClusterManager, object_storage):
    """Idea: the replicas of an SC tablet are never exactly in step: a write is applied on each
    of them at its own time, and a backup snapshots the nodes one after another. A cluster
    backup keeps what each node's snapshot holds, de-duplicated only by the repaired set, which
    an SC table does not have, and a tablet-aware restore gives each replica back its own node's
    sstables. For an EC table a repair after the restore evens the replicas out; an SC table has
    no repair, and Raft only replicates what went through its log. So a cluster backup restored
    into an SC table must still leave every replica with every acked row, or the table's
    content depends on which replica leads.

    Setup: 3 nodes in 3 racks, SC keyspace with RF 3 and 2 tablets per table. `src` gets one
    warm-up row applied everywhere; then apply() is parked on one replica (`lagging`) with the
    `strong_consistency_state_machine_wait_before_apply` injection and NUM_KEYS rows are written
    at QUORUM, so the two other replicas hold them and `lagging` does not.

    A cluster snapshot and a cluster backup are taken in that state, apply is released, `src` is
    checked to serve every row, truncated, and restored from the cluster backup. The test then
    makes each replica in turn the leader of every group of `src` and reads every key at QUORUM.
    The assertion is that each leader serves the whole set; the failure message names the
    leaders that miss rows.
    """
    servers = await start_cluster(manager, object_storage)
    cql = manager.get_cql()
    injection = 'strong_consistency_state_machine_wait_before_apply'
    async with new_test_keyspace(manager, ks_opts(tablets=2)) as ks:
        expected = await populate(cql, ks, 'src', [-1])
        await sc_wait_applied(manager, servers, ks, 'src')

        lagging = servers[2]
        await manager.api.enable_injection(lagging.ip_addr, injection, False)
        insert = cql.prepare(f"INSERT INTO {ks}.src (pk, value) VALUES (?, ?)")
        insert.consistency_level = ConsistencyLevel.QUORUM
        await asyncio.gather(*(cql.run_async(insert, (str(k), k)) for k in range(NUM_KEYS)))
        expected |= {str(k): k for k in range(NUM_KEYS)}
        # The two applying replicas hold every row, the lagging one only the warm-up row.
        await sc_wait_applied(manager, servers[:2], ks, 'src')

        snapshot_name = unique_name('snap_')
        await manager.api.take_cluster_snapshot(servers[0].ip_addr, ks, tag=snapshot_name, tables=['src'])
        prefix = unique_name('lagging_')
        await run_cluster_backup(object_storage, prefix, manager, snapshot_name, ks, 'src', servers)

        await manager.api.disable_injection(lagging.ip_addr, injection)
        assert await read_rows(cql, ks, 'src', expected) == expected, 'acked writes are not readable once apply resumes'

        await cql.run_async(f"TRUNCATE {ks}.src")
        manifests = [f'{prefix}/snapshots/{ks}/src/{snapshot_name}/manifest.json']
        tid = await manager.api.restore_tablets(servers[0].ip_addr, ks, 'src', snapshot_name, servers[0].datacenter,
                                                object_storage.address, object_storage.bucket_name, manifests)
        status = await manager.api.wait_task(servers[0].ip_addr, tid)
        assert status is not None and status['state'] == 'done', f'restore ended as {status}'

        group_ids = await table_raft_group_ids(manager, ks, 'src')
        missing = {}
        for s in servers:
            for gid in group_ids:
                await ensure_raft_group_leader_on(manager, s, gid)
            rows = await read_rows(cql, ks, 'src', expected)
            if rows != expected:
                missing[s.server_id] = sorted(set(expected) - set(rows), key=int)
        assert not missing, f'leaders missing rows after restoring the cluster backup of a lagging replica: {missing}'
