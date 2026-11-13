#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
#############################################################################
# The leader of a strongly consistent tablet seals its data now and then, and
# promotes a seal once every replica has applied it. Compaction never merges
# sstables from both sides of a seal that is not promoted, and merges them
# once it is.
#############################################################################

import asyncio
import logging
import time

import pytest

from test.cluster.test_incremental_repair import get_keys_from_sst, get_sstables
from test.cluster.test_strong_consistency import DEFAULT_CMDLINE, DEFAULT_CONFIG, get_table_raft_group_id
from test.cluster.util import ensure_raft_group_leader_on, new_test_keyspace, new_test_table
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')

SEAL_INTERVAL = 'sc_seal_interval_in_ms'
HOUR_MS = 3600 * 1000
WAIT_TIMEOUT_S = 60


async def read_seals(cql, host, group_id: str) -> tuple[list[int], int | None]:
    """The indexes of the pending seals of the group on the host, and of its promoted seal."""
    rows = await cql.run_async("SELECT group_id, seals, promoted_seal FROM system.raft_groups", host=host)
    [row] = [r for r in rows if str(r.group_id) == group_id]
    return sorted((row.seals or {}).keys()), row.promoted_seal[0] if row.promoted_seal else None


async def seal(manager: ScyllaClusterManager, cql, servers, host, group_id: str) -> int:
    """Make the leader seal the group now, and return the seal's index."""
    pending, promoted = await read_seals(cql, host, group_id)
    newest = max(pending + [promoted or 0])
    await set_seal_interval(manager, servers, 0)

    async def sealed():
        pending, promoted = await read_seals(cql, host, group_id)
        new = [idx for idx in pending + [promoted or 0] if idx > newest]
        return max(new) if new else None
    idx = await wait_for(sealed, time.time() + WAIT_TIMEOUT_S)
    await set_seal_interval(manager, servers, HOUR_MS)
    return idx


async def set_seal_interval(manager: ScyllaClusterManager, servers, interval_ms: int) -> None:
    # Enabling an injection that is already enabled keeps its old parameters.
    for s in servers:
        await manager.api.disable_injection(s.ip_addr, SEAL_INTERVAL)
        await manager.api.enable_injection(s.ip_addr, SEAL_INTERVAL, one_shot=False, parameters={'value': str(interval_ms)})


async def wait_promoted(cql, host, group_id: str, idx: int) -> None:
    async def promoted():
        _, promoted = await read_seals(cql, host, group_id)
        return True if promoted is not None and promoted >= idx else None
    await wait_for(promoted, time.time() + WAIT_TIMEOUT_S)


async def sstable_count(manager: ScyllaClusterManager, server, ks: str, table: str) -> int:
    info = await manager.api.get_sstable_info(server.ip_addr, ks, table)
    return len(info[0]['sstables']) if info else 0


async def test_seals_are_promoted_and_compacted(manager: ScyllaClusterManager):
    """Writes two sstables below a seal, which gets promoted, two between it and a second seal
    held back by a stopped replica, and two above. A major compaction merges only the two
    tiers on either side of the pending seal. Once the replica is back the seal is promoted,
    and the next compaction merges the middle tier into the repaired one."""
    config = DEFAULT_CONFIG | {'error_injections_at_startup': [
        {'name': 'sc_seal_proposer_tick_in_ms', 'value': '500'},
        {'name': SEAL_INTERVAL, 'value': str(HOUR_MS)},
    ]}
    servers = await manager.servers_add(3, config=config, cmdline=DEFAULT_CMDLINE, auto_rack_dc='dc1')
    leader, _, follower = servers
    cql, [leader_host, _, _] = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
                                          " AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, v int") as table:
            table_name = table.split('.')[-1]
            group_id = await get_table_raft_group_id(manager, ks, table_name)
            await ensure_raft_group_leader_on(manager, leader, group_id, timeout_seconds=WAIT_TIMEOUT_S)
            await manager.api.disable_autocompaction(leader.ip_addr, ks, table_name)

            async def write_and_flush(keys: range) -> None:
                await asyncio.gather(*[cql.run_async(f"INSERT INTO {table} (pk, v) VALUES ({k}, {k})", host=leader_host)
                                       for k in keys])
                await manager.api.keyspace_flush(leader.ip_addr, ks, table_name)

            await write_and_flush(range(0, 10))
            await write_and_flush(range(10, 20))
            first = await seal(manager, cql, servers, leader_host, group_id)
            await wait_promoted(cql, leader_host, group_id, first)

            await manager.server_stop(follower.server_id, convict=False)
            await write_and_flush(range(20, 30))
            await write_and_flush(range(30, 40))
            second = await seal(manager, cql, servers[:2], leader_host, group_id)
            await write_and_flush(range(40, 50))
            await write_and_flush(range(50, 60))
            assert await sstable_count(manager, leader, ks, table_name) == 6

            await manager.api.keyspace_compaction(leader.ip_addr, ks, table_name)
            # The two below the promoted seal merged, the two above the pending one merged,
            # the two between them did not.
            assert await sstable_count(manager, leader, ks, table_name) == 4
            _, promoted = await read_seals(cql, leader_host, group_id)
            assert promoted == first

            await manager.server_start(follower.server_id)
            await wait_promoted(cql, leader_host, group_id, second)
            _, promoted = await read_seals(cql, leader_host, group_id)
            assert promoted == second
            await manager.api.keyspace_compaction(leader.ip_addr, ks, table_name)
            # The middle two joined the repaired tier; the two above the seal stay apart from it.
            assert await sstable_count(manager, leader, ks, table_name) == 2


async def test_promoted_seal_purges_tombstones(manager: ScyllaClusterManager):
    """A promoted seal is the repair time of the tablet: a tombstone below it is purged by a
    compaction, with its data, while one above it is kept."""
    config = DEFAULT_CONFIG | {
        'error_injections_at_startup': [
            {'name': 'sc_seal_proposer_tick_in_ms', 'value': '500'},
            {'name': SEAL_INTERVAL, 'value': str(HOUR_MS)},
        ],
        # A live commitlog segment holds gc_before down to its oldest write of the table, so
        # the segment holding the writes below the seal has to be filled and retired.
        'commitlog_segment_size_in_mb': 1,
    }
    servers = await manager.servers_add(3, config=config, cmdline=DEFAULT_CMDLINE, auto_rack_dc='dc1')
    leader = servers[0]
    cql, [leader_host, _, _] = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
                                          " AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, v text",
                                  "WITH tombstone_gc = {'mode': 'repair', 'propagation_delay_in_seconds': '0'}") as table:
            table_name = table.split('.')[-1]
            group_id = await get_table_raft_group_id(manager, ks, table_name)
            await ensure_raft_group_leader_on(manager, leader, group_id, timeout_seconds=WAIT_TIMEOUT_S)
            await manager.api.disable_autocompaction(leader.ip_addr, ks, table_name)

            async def run_and_flush(query: str) -> None:
                await cql.run_async(query, host=leader_host)
                await manager.api.keyspace_flush(leader.ip_addr, ks, table_name)

            await run_and_flush(f"INSERT INTO {table} (pk, v) VALUES (0, '0')")
            await run_and_flush(f"INSERT INTO {table} (pk, v) VALUES (1, '1')")
            await run_and_flush(f"DELETE FROM {table} WHERE pk = 0")
            # A tombstone expires once its deletion time, in seconds, is strictly below the seal's.
            await asyncio.sleep(1.1)
            idx = await seal(manager, cql, servers, leader_host, group_id)
            await wait_promoted(cql, leader_host, group_id, idx)
            await run_and_flush(f"DELETE FROM {table} WHERE pk = 1")

            # Fill a few segments and flush everything, so that the segment of the writes
            # below the seal can go.
            filler = 'x' * 4000
            for start in range(100, 1100, 100):
                await asyncio.gather(*[cql.run_async(f"INSERT INTO {table} (pk, v) VALUES ({k}, '{filler}')", host=leader_host)
                                       for k in range(start, start + 100)])
            await manager.api.flush_all_keyspaces(leader.ip_addr)

            workdir = await manager.server_get_workdir(leader.server_id)
            exe = await manager.server_get_exe(leader.server_id)

            async def purged():
                await manager.api.keyspace_compaction(leader.ip_addr, ks, table_name)
                keys = await get_keys_from_sst(get_sstables(workdir, ks, table_name), exe)
                present = {k for ks_ in keys.values() for k in ks_}
                assert 1 in present
                return True if 0 not in present else None
            await wait_for(purged, time.time() + WAIT_TIMEOUT_S)
