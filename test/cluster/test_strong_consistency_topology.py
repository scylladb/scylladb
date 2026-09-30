#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Topology operations on strongly consistent tables."""

import asyncio
import logging

import pytest
from cassandra.cluster import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.internal_types import ServerInfo
from test.pylib.rest_client import read_barrier
from test.pylib.tablets import get_all_tablet_replicas
from test.pylib.util import gather_safely, start_writes
from test.cluster.util import DEFAULT_CMDLINE, FeatureConfigurations, get_table_raft_group_id, new_test_keyspace, \
    new_test_table, wait_for_leader

logger = logging.getLogger(__name__)

CMDLINE = DEFAULT_CMDLINE + ['--logger-log-level', 'raft_topology=debug']
SC_CONFIG = FeatureConfigurations.STRONG_CONSISTENCY.value
KS_OPTS = SC_CONFIG.get_keyspace_opts(
    "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1}")


async def boot_sc_cluster(manager: ScyllaClusterManager, racks: list[str], follower_only_racks: set[str]) -> list[ServerInfo]:
    """Boot one dc1 node per entry of `racks`; returns them in boot order.

    Nodes of `follower_only_racks` get avoid_being_raft_leader: they vote but never
    campaign, in every raft group including group0, so the layout decides who leads.
    They boot last because the first node has to elect itself as the only group0 voter.
    """
    eligible = [r for r in racks if r not in follower_only_racks]
    follower_only = [r for r in racks if r in follower_only_racks]
    assert eligible, "at least one rack must be able to lead"
    servers = await manager.servers_add(len(eligible), config=SC_CONFIG.get_cluster_cfg(), cmdline=CMDLINE,
                                        property_file=[{'dc': 'dc1', 'rack': r} for r in eligible])
    if follower_only:
        config = SC_CONFIG.get_cluster_cfg({'error_injections_at_startup': ['avoid_being_raft_leader']})
        servers += await manager.servers_add(len(follower_only), config=config, cmdline=CMDLINE,
                                             property_file=[{'dc': 'dc1', 'rack': r} for r in follower_only])
    await manager.disable_tablet_balancing()
    return servers


async def read_back(cql, host, table: str, key_count: int, cl: ConsistencyLevel) -> None:
    """Read every pk in range(key_count) back through `host`, expecting c == pk.

    At QUORUM the leader serves the read; at ONE the replica serves it from its own data.
    """
    stmt = SimpleStatement(f"SELECT pk, c FROM {table} WHERE pk = %s", consistency_level=cl)
    semaphore = asyncio.Semaphore(100)

    async def read_one(pk: int):
        async with semaphore:
            rows = await cql.run_async(stmt, [pk], host=host)
        assert len(rows) == 1 and rows[0].c == pk, f"pk={pk}: {rows}"

    await gather_safely(*[read_one(pk) for pk in range(key_count)])


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
@pytest.mark.parametrize("leader_on_src", [
    pytest.param(False, id="leader_away"),
    pytest.param(True, id="leader_on_src"),
])
async def test_tablet_migration_under_writes(manager: ScyllaClusterManager, leader_on_src: bool):
    """
    Moving a replica must not fail the clients' writes, whether it is a
    follower (leader_away) or the leader (leader_on_src).
    """
    follower_only = {'rack1', 'rack2'} if leader_on_src else {'rack1', 'rack3'}
    logger.info(f"Bootstrapping cluster, follower-only racks {sorted(follower_only)}")
    servers = await boot_sc_cluster(manager, ['rack1', 'rack2', 'rack3', 'rack3'], follower_only)
    cql, hosts = await manager.get_ready_cql(servers)
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])
    query = next(s for s in servers if s.rack == 'rack1')

    async with new_test_keyspace(manager, KS_OPTS) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, c int") as table:
            table_name = table.split('.')[1]
            group_id = await get_table_raft_group_id(manager, ks, table_name)

            tablets = await get_all_tablet_replicas(manager, query, ks, table_name)
            assert len(tablets) == 1, f"Expected 1 tablet, got {tablets}"
            token = tablets[0].last_token
            original = {h for h, _ in tablets[0].replicas}
            replica_of = {servers[host_ids.index(h)].rack: h for h in original}
            assert replica_of.keys() == {'rack1', 'rack2', 'rack3'}, f"Expected one replica per rack, got {tablets[0].replicas}"

            src_host_id, src_shard = next((h, s) for h, s in tablets[0].replicas if h == replica_of['rack3'])
            src = servers[host_ids.index(src_host_id)]
            dst = next(s for s in servers if s.rack == 'rack3' and s.server_id != src.server_id)
            dst_host_id = host_ids[servers.index(dst)]
            leader_before = src_host_id if leader_on_src else replica_of['rack2']
            leader_after = dst_host_id if leader_on_src else replica_of['rack2']
            await wait_for_leader(manager, servers[host_ids.index(leader_before)], group_id, expected_host_id=leader_before)
            logger.info(f"group_id={group_id} src={src_host_id}:{src_shard} ({src}) dst={dst_host_id} ({dst}) "
                        f"leader={leader_before} query={query}")

            logger.info("Starting strict writes")
            finish = await start_writes(cql, ks, table_name, concurrency=1)
            try:
                logger.info(f"Migrating {src_host_id}:{src_shard} to {dst_host_id}:0")
                await manager.api.move_tablet(query.ip_addr, ks, table_name, src_host_id, src_shard, dst_host_id, 0, token)
                logger.info("Migration finished")
            finally:
                # finish() re-raises the first failed write or read-back.
                key_count = await asyncio.wait_for(finish(), 300)
            logger.info(f"key_count={key_count}")

            await manager.api.quiesce_topology(query.ip_addr)
            tablets = await get_all_tablet_replicas(manager, query, ks, table_name)
            assert {h for h, _ in tablets[0].replicas} == (original - {src_host_id}) | {dst_host_id}, \
                f"Expected {src_host_id} replaced by {dst_host_id}, got {tablets[0].replicas}"
            await wait_for_leader(manager, dst, group_id, expected_host_id=leader_after)
            await read_barrier(manager.api, dst.ip_addr, group_id, timeout=60)
            await read_back(cql, hosts[servers.index(query)], table, key_count, ConsistencyLevel.QUORUM)
            await read_back(cql, hosts[servers.index(dst)], table, key_count, ConsistencyLevel.ONE)
