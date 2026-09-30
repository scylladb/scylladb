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
from test.pylib.rest_client import HTTPError
from test.pylib.tablets import get_all_tablet_replicas, get_tablet_info
from test.pylib.util import gather_safely
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
@pytest.mark.parametrize("park", [pytest.param(False, id="no_park"), pytest.param(True, id="park")])
async def test_tablet_migration_away_from_leader(manager: ScyllaClusterManager, park: bool):
    """
    Migrating the leading replica makes it drive its own removal from the raft
    group and hand leadership to the destination. With `park` the move is held
    while dst is still a non-voter.
    """
    logger.info("Bootstrapping cluster")
    servers = await boot_sc_cluster(manager, ['rack3', 'rack3', 'rack1', 'rack2'], {'rack1', 'rack2'})
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
            assert len(original) == 3, f"Expected 3 replicas, got {tablets[0].replicas}"

            # Under rf_rack_valid_keyspaces the only legal destination is the rack partner.
            src_host_id, src_shard = next((h, s) for h, s in tablets[0].replicas if servers[host_ids.index(h)].rack == 'rack3')
            src = servers[host_ids.index(src_host_id)]
            dst = next(s for s in servers if s.rack == 'rack3' and s.server_id != src.server_id)
            dst_host_id = host_ids[servers.index(dst)]
            logger.info(f"group_id={group_id} replicas={tablets[0].replicas} "
                        f"src={src_host_id}:{src_shard} ({src}) dst={dst_host_id}:0 ({dst}) query={query}")

            assert await wait_for_leader(manager, src, group_id, expected_host_id=src_host_id) == src_host_id
            logger.info(f"src {src_host_id} leads group {group_id}")

            logger.info("Seeding 10 rows")
            for pk in range(10):
                await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES ({pk}, {pk})")
            await read_back(cql, hosts[servers.index(query)], table, 10, ConsistencyLevel.QUORUM)

            src_log = await manager.server_open_log(src.server_id)
            src_mark = await src_log.mark()

            logger.info(f"Migrating the leading replica {src_host_id}:{src_shard} to {dst_host_id}:0 (park={park})")
            if park:
                # Mark before arming: a message emitted in between would be invisible to the wait.
                dst_log = await manager.server_open_log(dst.server_id)
                dst_mark = await dst_log.mark()
                await manager.api.enable_injection(dst.ip_addr, "sc_wait_for_snapshot_transfer", one_shot=True)
                move_task = asyncio.create_task(manager.api.move_tablet(query.ip_addr, ks, table_name,
                                                                        src_host_id, src_shard, dst_host_id, 0, token))
                try:
                    _, matches = await dst_log.wait_for("sc_wait_for_snapshot_transfer: waiting for message",
                                                        from_mark=dst_mark, timeout=60)
                    logger.info(f"dst parked: {matches[0][0].strip()}")
                    parked_leader = await manager.api.get_raft_leader(src.ip_addr, group_id)
                    stage = (await get_tablet_info(manager, query, ks, table_name, token)).stage
                    logger.info(f"While parked: leader={parked_leader} stage={stage}")
                    assert parked_leader == src_host_id, f"Leader moved to {parked_leader} while the migration is parked"
                    assert stage == "streaming", f"Expected stage streaming while parked, got {stage}"
                finally:
                    # Release on every path, or a failed assertion leaves dst parked and the move pending.
                    await manager.api.message_injection(dst.ip_addr, "sc_wait_for_snapshot_transfer")
                    await asyncio.wait_for(move_task, 300)
            else:
                await manager.api.move_tablet(query.ip_addr, ks, table_name, src_host_id, src_shard, dst_host_id, 0, token)
            await manager.api.quiesce_topology(query.ip_addr)
            logger.info("Migration finished")

            tablets = await get_all_tablet_replicas(manager, query, ks, table_name)
            assert len(tablets) == 1, f"Expected 1 tablet, got {tablets}"
            assert {h for h, _ in tablets[0].replicas} == (original - {src_host_id}) | {dst_host_id}, \
                f"Expected {src_host_id} replaced by {dst_host_id}, got {tablets[0].replicas}"

            # Takes an election timeout: src may hand over to a follower-only voter first.
            new_leader = await wait_for_leader(manager, dst, group_id, expected_host_id=dst_host_id)
            assert new_leader == dst_host_id, f"Expected dst {dst_host_id} to lead, got {new_leader}"
            logger.info(f"dst {dst_host_id} leads group {group_id}")

            await src_log.wait_for(f"raft server for group id {group_id} is destroyed", from_mark=src_mark, timeout=60)
            logger.info(f"src {src_host_id} destroyed its raft server for group {group_id}")
            with pytest.raises(HTTPError) as e:
                await manager.api.get_raft_leader(src.ip_addr, group_id)
            assert e.value.code == 400, f"Expected 400 for the torn-down group on src, got {e.value}"
            logger.info(f"get_raft_leader on src failed with HTTP {e.value.code} as expected")

            dst_host = hosts[servers.index(dst)]
            src_host = hosts[servers.index(src)]
            logger.info("Reading the rows back through dst (the leader) and through src (now a non-replica)")
            await read_back(cql, dst_host, table, 10, ConsistencyLevel.QUORUM)
            await read_back(cql, src_host, table, 10, ConsistencyLevel.QUORUM)
            await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES (10, 10)", host=dst_host)
            await read_back(cql, src_host, table, 11, ConsistencyLevel.QUORUM)
