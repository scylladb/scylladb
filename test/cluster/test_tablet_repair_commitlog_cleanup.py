#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
import struct
import time

from cassandra.cluster import ConsistencyLevel  # type: ignore
from cassandra.metadata import Murmur3Token  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore

from test.cluster.util import new_test_keyspace, reconnect_driver
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_all_tablet_replicas
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)


async def get_cleanup_records(cql, host, table_id) -> list[tuple[tuple[int, int], int, int]]:
    """Returns the system.commitlog_cleanups rows of a table on one node,
    as (position, start_token_exclusive, end_token_inclusive) tuples."""
    rows = await cql.run_async("SELECT position, table_uuid, start_token_exclusive, end_token_inclusive FROM system.commitlog_cleanups", host=host)
    return [(tuple(r.position), r.start_token_exclusive, r.end_token_inclusive) for r in rows if str(r.table_uuid) == str(table_id)]


async def test_tablet_repair_cleanup_record_keeps_unflushed_writes(manager: ScyllaClusterManager):
    """Reproducer for SCYLLADB-6071.

    After a tablet repair, a commitlog cleanup record is saved for the
    repaired token range with the table's highest flushed replay position.
    That position is table-wide per shard, so when another tablet of the
    same table flushes on the same shard and the records are saved again
    (e.g. on the next reload of system.tablets), the record of the earlier
    repaired tablet moves past writes to it which are still only in the
    memtable and the commitlog. After an unclean restart, commitlog replay
    skips those writes and acked data is lost.

    Steps:
    1. 2 nodes, --smp 1 (so both tablets share a shard), commitlog_sync=batch.
    2. Table with 2 tablets A and B, RF=2, writes to both.
    3. Repair tablet A only.
    4. Write victim rows to A, then rows to B, at CL=ALL.
    5. Repair tablet B only. A's cleanup record moves to B's flush position.
    6. Write control rows to A.
    7. kill -9 all nodes and restart them.
    8. Read each replica on its own: the victim rows must still be there.
    """
    config = {'commitlog_sync': 'batch'}
    # Two replicas are the minimum: a single-replica tablet repair is skipped
    # and writes no repair time, so no cleanup record would be saved.
    property_files = [{"dc": "dc1", "rack": f"r{i}"} for i in range(2)]
    servers = await manager.servers_add(2, config=config, cmdline=['--smp', '1'], property_file=property_files)
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND tablets = {'initial': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int)")
        table_id = await manager.get_table_id(ks, "test")

        tablets = sorted(await get_all_tablet_replicas(manager, servers[0], ks, "test"), key=lambda t: t.last_token)
        assert len(tablets) == 2
        token_a, token_b = tablets[0].last_token, tablets[1].last_token
        logger.info(f"tablet A last_token={token_a} tablet B last_token={token_b}")

        # Bucket candidate keys by tablet using the driver's partitioner.
        keys_a, keys_b = [], []
        for pk in range(1000):
            t = Murmur3Token.from_key(struct.pack('>i', pk)).value
            (keys_a if t <= token_a else keys_b).append(pk)
        assert len(keys_a) >= 60 and len(keys_b) >= 40

        async def insert(keys, cl=ConsistencyLevel.ALL):
            stmts = [SimpleStatement(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k})", consistency_level=cl) for k in keys]
            await asyncio.gather(*[cql.run_async(s) for s in stmts])

        initial_a, victims_a, control_a = keys_a[0:20], keys_a[20:40], keys_a[40:60]
        initial_b, later_b = keys_b[0:20], keys_b[20:40]

        # Step 2: writes to both tablets.
        await insert(initial_a + initial_b)

        # Step 3: repair tablet A only. This flushes A's memtable and saves
        # A's cleanup record at the current flushed position.
        await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token_a)

        # The record is saved when a node reloads the tablet metadata, which may be
        # after the repair request returned.
        async def record_saved(h, token):
            recs = await get_cleanup_records(cql, h, table_id)
            return recs if any(r[2] == token for r in recs) else None

        records_after_a = {}
        for h in hosts:
            records_after_a[h] = await wait_for(lambda: record_saved(h, token_a), time.time() + 60)
            logger.info(f"cleanup records on {h} after repair of A: {records_after_a[h]}")

        # Step 4: victim writes to A (only in memtable + commitlog from now on),
        # then writes to B.
        await insert(victims_a)
        await insert(later_b)

        # Step 5: repair tablet B only. B's flush advances the table-wide
        # highest flushed replay position, and A's record gets re-saved at
        # that position when system.tablets is reloaded.
        await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token_b)

        # The pending updates of A and B are flushed together, A first, so once
        # B's record is there, A's has already been re-saved (pre-fix) or kept.
        for h in hosts:
            recs = await wait_for(lambda: record_saved(h, token_b), time.time() + 60)
            a_positions_before = [r[0] for r in records_after_a[h] if r[2] == token_a]
            a_positions_after = [r[0] for r in recs if r[2] == token_a]
            b_positions = [r[0] for r in recs if r[2] == token_b]
            logger.info(f"cleanup records on {h} after repair of B: {recs}; tablet A positions {a_positions_before} -> {a_positions_after}")
            # A did not flush since its repair, so its record must not have moved,
            # while B's record is at B's later flush.
            assert a_positions_after == a_positions_before, f"tablet A cleanup record moved on {h}"
            assert max(b_positions) > max(a_positions_after)

        # Step 6: control writes to A, after the last flush on the shard.
        await insert(control_a)

        async def read_keys(h):
            # Reads only the replica on h, so a replica which lost the rows is
            # not covered by the other one, as it would be by read repair at CL=ALL.
            rows = await cql.run_async(f"SELECT pk, mutation_fragment_kind FROM MUTATION_FRAGMENTS({ks}.test)", host=h)
            return {r.pk for r in rows if r.mutation_fragment_kind == "clustering row"}

        for h in hosts:
            before = await read_keys(h)
            assert set(victims_a) <= before and set(control_a) <= before

        # Step 7: kill -9 all nodes and restart them.
        for s in servers:
            await manager.server_stop(s.server_id, convict=False)
        for s in servers:
            await manager.server_start(s.server_id)
        cql = await reconnect_driver(manager)
        hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 120)

        # Step 8: everything acked before the kill must still be on every replica.
        for h in hosts:
            after = await read_keys(h)
            missing_control = set(control_a) - after
            assert not missing_control, f"control rows lost on {h}: {sorted(missing_control)}"
            missing_b = set(initial_b + later_b) - after
            assert not missing_b, f"tablet B rows lost on {h}: {sorted(missing_b)}"
            missing_victims = set(victims_a) - after
            assert not missing_victims, f"writes to tablet A after its repair were lost on commitlog replay on {h}: {sorted(missing_victims)}"
