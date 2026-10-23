#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

#############################################################################
# Cluster-level tests for clients that use "synthetic" write timestamps:
# every write carries USING TIMESTAMP with a value that has no relationship
# to real time. Replica convergence (read repair, row-level repair, full and
# incremental tablet repair) and repair-based tombstone GC must depend only
# on comparing write timestamps with each other, never with the wall clock.
#
# The replicas are made to diverge by failing writes on chosen replicas with
# an error injection (hinted handoff is disabled, so nothing heals them
# behind the test's back). The typical divergence is a minority replica that
# holds the winning (higher timestamp) write, while the majority received a
# losing (lower timestamp) write *later* in real time.
#
# Single-node properties are tested in test/cqlpy/test_synthetic_timestamps.py.
#############################################################################

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager

import pytest
from cassandra.query import SimpleStatement, ConsistencyLevel

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts
from test.cluster.util import new_test_keyspace

logger = logging.getLogger(__name__)

pytestmark = [
    pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode'),
    pytest.mark.skip_mode(mode='debug', reason='dev is enough'),
]

timestamp_eras = {
    # Control: ordinary wall-clock timestamps.
    'wall_clock': None,
    # A version counter starting at 1 - looks like a write from 1970.
    'counter': 1,
    # Negative timestamps are legal (only -2**63 is reserved).
    'negative': -2**62,
    # Two days into the future: still accepted with the default
    # restrict_future_timestamp=true. A placeholder; computed per test.
    'near_future': None,
    # Thousands of years into the future (needs restrict_future_timestamp=false).
    'far_future': 2**62,
}


def era_base(era):
    if era == 'wall_clock':
        return int(time.time() * 1_000_000)
    if era == 'near_future':
        return int((time.time() + 2 * 24 * 3600) * 1_000_000)
    return timestamp_eras[era]


# How the replicas diverge: the winning write is an INSERT, an UPDATE (no
# row marker) or a row deletion.
divergence_kinds = ['insert', 'update', 'deletion']


repair_modes = ['vnodes', 'tablets-full', 'tablets-incremental', 'tablets-disabled']


class Cluster:
    def __init__(self, manager, servers, hosts):
        self.manager = manager
        self.servers = servers
        self.hosts = hosts
        self.cql = manager.get_cql()

    async def execute(self, stmt, cl, host_index=0):
        return list(await self.cql.run_async(SimpleStatement(stmt, consistency_level=cl), host=self.hosts[host_index]) or [])

    @asynccontextmanager
    async def writes_fail_on(self, table, replicas):
        """Make writes to `table` fail on the given replicas (indexes into servers)."""
        ks, cf = table.split('.')
        await asyncio.gather(*(self.manager.api.enable_injection(self.servers[i].ip_addr, "database_apply", False,
                                                                 {"ks_name": ks, "cf_name": cf, "what": "throw"})
                               for i in replicas))
        try:
            yield
        finally:
            await asyncio.gather(*(self.manager.api.disable_injection(self.servers[i].ip_addr, "database_apply")
                                   for i in replicas))

    async def write_only_to(self, table, stmt, replicas):
        """Execute a write that is applied only on the given replicas."""
        others = [i for i in range(len(self.servers)) if i not in replicas]
        cl = {1: ConsistencyLevel.ONE, 2: ConsistencyLevel.TWO, 3: ConsistencyLevel.ALL}[len(replicas)]
        async with self.writes_fail_on(table, others):
            await self.execute(stmt, cl, host_index=replicas[0])

    async def flush_and_compact(self, ks):
        for s in self.servers:
            await self.manager.api.keyspace_flush(s.ip_addr, ks)
            await self.manager.api.keyspace_compaction(s.ip_addr, ks)

    async def local_fragments(self, table, pk, host_index, sstables_only=False):
        """The mutation fragments of partition pk held by one replica, as
        (fragment kind, metadata) pairs."""
        where = f"pk = {pk}"
        if sstables_only:
            where += " AND mutation_source > 'sstable:' AND mutation_source < 'sstable;'"
        rows = await self.execute(f"SELECT mutation_fragment_kind, metadata FROM MUTATION_FRAGMENTS({table}) WHERE {where}",
                                  ConsistencyLevel.ONE, host_index)
        return [(r.mutation_fragment_kind, json.loads(r.metadata) if r.metadata else None) for r in rows]

    async def repair(self, ks, table, mode):
        cf = table.split('.')[1]
        if mode == 'vnodes':
            await self.manager.api.repair(self.servers[0].ip_addr, ks, cf)
        else:
            incremental_mode = mode.removeprefix('tablets-')
            await self.manager.api.tablet_repair(self.servers[0].ip_addr, ks, cf, 'all', incremental_mode=incremental_mode)


@asynccontextmanager
async def new_cluster_keyspace(manager, uses_tablets, durable_writes=True):
    tablets = "true" if uses_tablets else "false"
    opts = (f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}}"
            f" AND tablets = {{'enabled': {tablets}}} AND durable_writes = {str(durable_writes).lower()}")
    async with new_test_keyspace(manager, opts) as ks:
        yield ks


async def make_cluster(manager: ScyllaClusterManager) -> Cluster:
    config = {
        'hinted_handoff_enabled': False,
        'restrict_future_timestamp': False,
        # Repair takes its repair time (which drives repair-mode tombstone
        # GC) from the hints and batchlog flush, which is otherwise cached.
        'repair_hints_batchlog_flush_cache_time_in_ms': 0,
    }
    servers = await manager.servers_add(3, config=config, auto_rack_dc="dc1")
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    hosts_by_ip = {h.address: h for h in hosts}
    hosts = [hosts_by_ip[s.ip_addr] for s in servers]
    return Cluster(manager, servers, hosts)


def has_cell(fragments, timestamp):
    return any(kind == 'clustering row' and m.get('columns', {}).get('v', {}).get('timestamp') == timestamp
               for kind, m in fragments)


def has_row_tombstone(fragments, timestamp):
    return any(kind == 'clustering row' and m.get('tombstone', {}).get('timestamp') == timestamp
               for kind, m in fragments)


async def diverge(cluster, table, base, pk, kind):
    """Replica 0 gets the winning write, at base+10; replicas 1 and 2 get a
    losing write, at base+5, later in real time."""
    if kind == 'insert':
        winner = f"INSERT INTO {table} (pk, ck, v) VALUES ({pk}, 0, 10) USING TIMESTAMP {base + 10}"
    elif kind == 'update':
        winner = f"UPDATE {table} USING TIMESTAMP {base + 10} SET v = 10 WHERE pk = {pk} AND ck = 0"
    else:
        winner = f"DELETE FROM {table} USING TIMESTAMP {base + 10} WHERE pk = {pk} AND ck = 0"
    loser = f"UPDATE {table} USING TIMESTAMP {base + 5} SET v = 5 WHERE pk = {pk} AND ck = 0"
    await cluster.write_only_to(table, winner, [0])
    await cluster.write_only_to(table, loser, [1, 2])
    for i, expected in [(0, False), (1, True), (2, True)]:
        assert has_cell(await cluster.local_fragments(table, pk, i), base + 5) == expected


async def check_converged(cluster, table, base, pk, kind):
    expected = [] if kind == 'deletion' else [(pk, 0, 10)]
    assert await cluster.execute(f"SELECT pk, ck, v FROM {table} WHERE pk = {pk}", ConsistencyLevel.ALL) == expected
    for i in range(len(cluster.servers)):
        fragments = await cluster.local_fragments(table, pk, i)
        if kind != 'deletion':
            assert has_cell(fragments, base + 10), f"replica {i}: {fragments}"
        else:
            assert has_row_tombstone(fragments, base + 10), f"replica {i}: {fragments}"
        # Each replica, read alone, sees the winner.
        rows = await cluster.execute(f"SELECT pk, ck, v FROM {table} WHERE pk = {pk} AND ck = 0", ConsistencyLevel.ONE, i)
        assert rows == expected, f"replica {i}"


@pytest.mark.parametrize("uses_tablets", [False, True], ids=["vnodes", "tablets"])
async def test_read_repair(manager: ScyllaClusterManager, uses_tablets):
    """A CL=ALL read reconciles the replicas by timestamp, and read repair
    propagates the winner - the minority write with the higher timestamp -
    to the replicas that received a lower-timestamp write later."""
    cluster = await make_cluster(manager)
    async with new_cluster_keyspace(manager, uses_tablets) as ks:
        table = f"{ks}.t"
        await cluster.execute(f"CREATE TABLE {table} (pk int, ck int, v int, PRIMARY KEY (pk, ck))"
                              " WITH tombstone_gc = {'mode': 'disabled'}", ConsistencyLevel.ALL)
        pk = 0
        for era in timestamp_eras:
            for kind in divergence_kinds:
                pk += 1
                base = era_base(era)
                logger.info(f"era={era} kind={kind} pk={pk} base={base}")
                await diverge(cluster, table, base, pk, kind)
                # Read at ALL, so that replica 0 surely participates.
                expected = [] if kind == 'deletion' else [(pk, 0, 10)]
                assert await cluster.execute(f"SELECT pk, ck, v FROM {table} WHERE pk = {pk}", ConsistencyLevel.ALL) == expected
                await check_converged(cluster, table, base, pk, kind)


@pytest.mark.parametrize("mode", repair_modes)
async def test_repair(manager: ScyllaClusterManager, mode):
    """Row-level repair - vnode repair and full, incremental and
    non-incremental tablet repair - converges the replicas on the write with
    the highest timestamp, regardless of arrival order."""
    cluster = await make_cluster(manager)
    async with new_cluster_keyspace(manager, mode != 'vnodes') as ks:
        table = f"{ks}.t"
        await cluster.execute(f"CREATE TABLE {table} (pk int, ck int, v int, PRIMARY KEY (pk, ck))"
                              " WITH tombstone_gc = {'mode': 'disabled'}", ConsistencyLevel.ALL)
        cases = []
        pk = 0
        for era in timestamp_eras:
            for kind in divergence_kinds:
                pk += 1
                base = era_base(era)
                await diverge(cluster, table, base, pk, kind)
                cases.append((base, pk, kind))
        # Some of the divergent data in sstables, some in memtables.
        for s in cluster.servers[:2]:
            await manager.api.keyspace_flush(s.ip_addr, ks)
        await cluster.repair(ks, table, mode)
        for base, pk, kind in cases:
            await check_converged(cluster, table, base, pk, kind)
        await cluster.flush_and_compact(ks)
        for base, pk, kind in cases:
            await check_converged(cluster, table, base, pk, kind)


async def test_incremental_repair_does_not_regress_repaired_data(manager: ScyllaClusterManager):
    """Incremental repair only repairs unrepaired sstables, but those may hold
    writes with lower timestamps than already-repaired data. Repairing them
    must not undo the repaired data, and a higher-timestamp write arriving
    after a repair must win over it."""
    cluster = await make_cluster(manager)
    async with new_cluster_keyspace(manager, True) as ks:
        table = f"{ks}.t"
        await cluster.execute(f"CREATE TABLE {table} (pk int, ck int, v int, PRIMARY KEY (pk, ck))"
                              " WITH tombstone_gc = {'mode': 'disabled'}", ConsistencyLevel.ALL)
        cases = []
        pk = 0
        for era in timestamp_eras:
            base = era_base(era)
            pk += 1
            await diverge(cluster, table, base, pk, 'insert')
            cases.append((base, pk))
        await cluster.repair(ks, table, 'tablets-incremental')
        for base, pk in cases:
            await check_converged(cluster, table, base, pk, 'insert')
        for base, pk in cases:
            # Older than the repaired winner, on one replica only.
            await cluster.write_only_to(table, f"INSERT INTO {table} (pk, ck, v) VALUES ({pk}, 0, 7) USING TIMESTAMP {base + 7}", [1])
            # A newer deletion of another row, on one replica only.
            await cluster.write_only_to(table, f"INSERT INTO {table} (pk, ck, v) VALUES ({pk}, 1, 1) USING TIMESTAMP {base + 1}", [0, 1, 2])
            await cluster.write_only_to(table, f"DELETE FROM {table} USING TIMESTAMP {base + 20} WHERE pk = {pk} AND ck = 1", [2])
        await cluster.repair(ks, table, 'tablets-incremental')
        await cluster.flush_and_compact(ks)
        await cluster.repair(ks, table, 'tablets-incremental')
        for base, pk in cases:
            await check_converged(cluster, table, base, pk, 'insert')
            for i in range(len(cluster.servers)):
                fragments = await cluster.local_fragments(table, pk, i)
                assert has_row_tombstone(fragments, base + 20), f"replica {i}: {fragments}"


@pytest.mark.parametrize("mode", repair_modes)
async def test_tombstone_gc_after_repair(manager: ScyllaClusterManager, mode):
    """With tombstone_gc mode 'repair', a tombstone that did not reach all
    replicas is kept until repair propagates it, and then purged together
    with the data it shadows on all replicas - without resurrecting it.
    Purgeability is decided by the tombstone's deletion time (wall clock,
    assigned on arrival) against the repair time, never by its timestamp."""
    cluster = await make_cluster(manager)
    # Without the commitlog, purging does not also wait for commitlog
    # segments (kept alive by other tables) to be recycled.
    async with new_cluster_keyspace(manager, mode != 'vnodes', durable_writes=False) as ks:
        table = f"{ks}.t"
        await cluster.execute(f"CREATE TABLE {table} (pk int, ck int, v int, PRIMARY KEY (pk, ck))"
                              " WITH tombstone_gc = {'mode': 'repair', 'propagation_delay_in_seconds': '1'}",
                              ConsistencyLevel.ALL)
        cases = []
        pk = 0
        for era in timestamp_eras:
            base = era_base(era)
            pk += 1
            await cluster.execute(f"INSERT INTO {table} (pk, ck, v) VALUES ({pk}, 0, 1) USING TIMESTAMP {base + 1}",
                                  ConsistencyLevel.ALL)
            cases.append((base, pk))
        for s in cluster.servers:
            await manager.api.keyspace_flush(s.ip_addr, ks)
        for base, pk in cases:
            await cluster.write_only_to(table, f"DELETE FROM {table} USING TIMESTAMP {base + 10} WHERE pk = {pk} AND ck = 0", [0, 1])

        # Not repaired yet: compaction keeps the tombstones, and the data
        # on replica 2 stays shadowed.
        time.sleep(2.1)
        await cluster.flush_and_compact(ks)
        for base, pk in cases:
            assert await cluster.execute(f"SELECT * FROM {table} WHERE pk = {pk}", ConsistencyLevel.ALL) == []
            for i in (0, 1):
                fragments = await cluster.local_fragments(table, pk, i, sstables_only=True)
                assert has_row_tombstone(fragments, base + 10), f"replica {i}: {fragments}"

        # The deletion time must be before the repair time minus the
        # propagation delay (at one-second resolution).
        await cluster.repair(ks, table, mode)
        time.sleep(2.1)
        await cluster.flush_and_compact(ks)
        for base, pk in cases:
            assert await cluster.execute(f"SELECT * FROM {table} WHERE pk = {pk}", ConsistencyLevel.ALL) == []
            for i in range(len(cluster.servers)):
                fragments = await cluster.local_fragments(table, pk, i, sstables_only=True)
                assert fragments == [], f"replica {i}: {fragments}"
