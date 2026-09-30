#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Topology operations on strongly consistent tables."""

import asyncio
import logging
import time

import pytest
from cassandra import WriteFailure
from cassandra.cluster import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.internal_types import ServerInfo
from test.pylib.rest_client import HTTPError, read_barrier
from test.pylib.tablets import get_all_tablet_replicas, get_tablet_info
from test.pylib.util import gather_safely, start_writes, wait_for
from test.cluster.util import DEFAULT_CMDLINE, FeatureConfigurations, get_table_raft_group_id, new_test_keyspace, \
    new_test_table, reconnect_driver, wait_for_leader, wait_for_token_ring_and_group0_consistency

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


async def check_replica_left(manager: ScyllaClusterManager, query: ServerInfo, live_servers: list[ServerInfo],
                             ks: str, table_name: str, group_id: str, original: set[str], victim_host_id: str,
                             partner: ServerInfo, partner_host_id: str, expected_leaders: set[str], key_count: int):
    """Checks shared by the replica-removal tests, once the victim is gone."""
    await manager.api.quiesce_topology(query.ip_addr)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 60)
    # The hosts the caller derived before the removal still list the victim.
    cql, hosts = await manager.get_ready_cql(live_servers)
    table = f"{ks}.{table_name}"

    tablets = await get_all_tablet_replicas(manager, query, ks, table_name)
    assert len(tablets) == 1, f"Expected 1 tablet, got {tablets}"
    assert {h for h, _ in tablets[0].replicas} == (original - {victim_host_id}) | {partner_host_id}, \
        f"Expected {victim_host_id} replaced by {partner_host_id}, got {tablets[0].replicas}"

    async def allowed_leader():
        leader = await manager.api.get_raft_leader(partner.ip_addr, group_id)
        return leader if leader in expected_leaders else None
    # A follower may name a stale leader until the new term reaches it, so poll.
    leader = await wait_for(allowed_leader, time.time() + 60, label=f"a leader in {sorted(expected_leaders)}")
    logger.info(f"{leader} leads group {group_id} after the removal")

    # After the barrier the CL=ONE read-back proves the partner holds every acked write.
    # timeout= is required: without it the node hits on_internal_error (SCYLLADB-4759).
    await read_barrier(manager.api, partner.ip_addr, group_id, timeout=60)
    logger.info(f"Reading {key_count} keys back through {query} at QUORUM and through the partner {partner} at ONE")
    await read_back(cql, hosts[live_servers.index(query)], table, key_count, ConsistencyLevel.QUORUM)
    await read_back(cql, hosts[live_servers.index(partner)], table, key_count, ConsistencyLevel.ONE)


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


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
@pytest.mark.parametrize("leader_on_src", [
    pytest.param(False, id="leader_away", marks=pytest.mark.skip_bug(
        link="https://scylladb.atlassian.net/browse/SCYLLADB-4703",
        reason="a write racing the migration can abort the leaving replica; "
               "fixed by https://github.com/scylladb/scylladb/pull/31955")),
    pytest.param(True, id="leader_on_src", marks=pytest.mark.skip_bug(
        link="https://scylladb.atlassian.net/browse/SCYLLADB-4933",
        reason="a write waiting on the leaving ex-leader fails when its raft group is torn down at use_new")),
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


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
@pytest.mark.parametrize("leader_on_victim", [
    pytest.param(False, id="leader_away"),
    pytest.param(True, id="leader_on_victim", marks=pytest.mark.skip_bug(
        link="https://scylladb.atlassian.net/browse/SCYLLADB-4933",
        reason="a write waiting on the leaving ex-leader fails when its raft group is torn down at use_new"))])
async def test_decommission_sc_replica_under_writes(manager: ScyllaClusterManager, leader_on_victim: bool):
    """
    Decommissioning a replica must not fail the clients' writes; its rack
    partner takes the replica over. In leader_on_victim the leaving replica
    leads and has to hand leadership over while it leaves.
    """
    follower_only = {'rack1', 'rack2'} if leader_on_victim else {'rack1', 'rack3'}
    logger.info(f"Bootstrapping cluster, follower-only racks {sorted(follower_only)}")
    servers = await boot_sc_cluster(manager, ['rack1', 'rack1', 'rack2', 'rack2', 'rack3', 'rack3'], follower_only)
    cql, _ = await manager.get_ready_cql(servers)
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])
    query = next(s for s in servers if s.rack == 'rack1')

    async with new_test_keyspace(manager, KS_OPTS) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, c int") as table:
            table_name = table.split('.')[1]
            group_id = await get_table_raft_group_id(manager, ks, table_name)

            tablets = await get_all_tablet_replicas(manager, query, ks, table_name)
            assert len(tablets) == 1, f"Expected 1 tablet, got {tablets}"
            original = {h for h, _ in tablets[0].replicas}
            replica_of = {servers[host_ids.index(h)].rack: h for h in original}
            assert replica_of.keys() == {'rack1', 'rack2', 'rack3'}, f"Expected one replica per rack, got {tablets[0].replicas}"

            # Under rf_rack_valid_keyspaces the only legal successor is the rack partner.
            victim_host_id, rack2_host_id = replica_of['rack3'], replica_of['rack2']
            victim, rack2_server = servers[host_ids.index(victim_host_id)], servers[host_ids.index(rack2_host_id)]
            partner = next(s for s in servers if s.rack == 'rack3' and s.server_id != victim.server_id)
            partner_host_id = host_ids[servers.index(partner)]
            expected_leader_host_id, leader_server = (victim_host_id, victim) if leader_on_victim else (rack2_host_id, rack2_server)
            logger.info(f"group_id={group_id} replicas={tablets[0].replicas} victim={victim_host_id} ({victim}, rack3) "
                        f"partner={partner_host_id} ({partner}) rack2 replica={rack2_host_id} ({rack2_server}) query={query}")

            assert await wait_for_leader(manager, leader_server, group_id, expected_host_id=expected_leader_host_id) == expected_leader_host_id
            logger.info(f"{expected_leader_host_id} leads group {group_id} (leader_on_victim={leader_on_victim})")

            logger.info("Starting strict writes")
            finish = await start_writes(cql, ks, table_name, concurrency=1)
            try:
                logger.info(f"Decommissioning the victim {victim_host_id} ({victim})")
                await manager.decommission_node(victim.server_id)
                logger.info("Decommission finished")
            finally:
                # finish() re-raises the first failed write or read-back.
                key_count = await asyncio.wait_for(finish(), 300)
            logger.info(f"key_count={key_count}")

            live_servers = [s for s in servers if s.server_id != victim.server_id]
            # In leader_on_victim the partner is the only eligible voter left once C_new commits.
            after_leader = partner_host_id if leader_on_victim else rack2_host_id
            await check_replica_left(manager, query, live_servers, ks, table_name, group_id, original,
                                     victim_host_id, partner, partner_host_id, {after_leader}, key_count)
            await reconnect_driver(manager)


async def write_through_failover(cql, table: str, stop: asyncio.Event) -> int:
    """Write c = pk for pk = 0, 1, ... until `stop`; returns the number of keys written.

    A key whose write was forwarded to a dead leader and failed fast is retried, so the
    written keys stay range(n). Any other error fails the test.
    """
    stmt = cql.prepare(f"INSERT INTO {table} (pk, c) VALUES (?, ?)")
    pk = 0
    while not stop.is_set():
        try:
            await cql.run_async(stmt, [pk, pk])
            pk += 1
        except WriteFailure as e:
            assert "failed while forwarding" in str(e), e
            logger.info(f"pk={pk} failed fast, retrying: {e}")
            await asyncio.sleep(0.1)
    return pk


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
@pytest.mark.parametrize("leader_on_victim", [
    pytest.param(False, id="leader_away"),
    pytest.param(True, id="leader_on_victim")])
async def test_removenode_sc_replica_under_writes(manager: ScyllaClusterManager, leader_on_victim: bool):
    """
    Removing a dead replica must not fail the clients' writes; its rack partner
    takes the replica over.

    In leader_on_victim only rack1 is follower-only: after a hard kill nobody
    steps down and only a live leader can add the partner, so an eligible
    replica has to survive. The victim is the leader the product elected.
    Until a new leader is elected, writes forwarded to the dead one fail fast,
    which is by design (SCYLLADB-4758): they are retried, and writes must
    succeed again once the replica is removed.
    """
    follower_only = {'rack1', 'rack3'} if not leader_on_victim else {'rack1'}
    logger.info(f"Bootstrapping cluster, follower-only racks {sorted(follower_only)}")
    servers = await boot_sc_cluster(manager, ['rack1', 'rack1', 'rack2', 'rack2', 'rack3', 'rack3'], follower_only)
    cql, _ = await manager.get_ready_cql(servers)
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])
    query = next(s for s in servers if s.rack == 'rack1')

    async with new_test_keyspace(manager, KS_OPTS) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, c int") as table:
            table_name = table.split('.')[1]
            group_id = await get_table_raft_group_id(manager, ks, table_name)

            tablets = await get_all_tablet_replicas(manager, query, ks, table_name)
            assert len(tablets) == 1, f"Expected 1 tablet, got {tablets}"
            original = {h for h, _ in tablets[0].replicas}
            replica_of = {servers[host_ids.index(h)].rack: h for h in original}
            assert replica_of.keys() == {'rack1', 'rack2', 'rack3'}, f"Expected one replica per rack, got {tablets[0].replicas}"

            if leader_on_victim:
                # Ask the rack1 replica: the other rack1 node does not host the group.
                victim_host_id = await wait_for_leader(manager, servers[host_ids.index(replica_of['rack1'])], group_id)
                assert victim_host_id != replica_of['rack1'], f"The follower-only rack1 replica {victim_host_id} leads"
                victim = servers[host_ids.index(victim_host_id)]
                survivor = next(h for r, h in replica_of.items() if r not in ('rack1', victim.rack))
            else:
                victim_host_id = replica_of['rack3']
                victim = servers[host_ids.index(victim_host_id)]
                await wait_for_leader(manager, servers[host_ids.index(replica_of['rack2'])], group_id,
                                      expected_host_id=replica_of['rack2'])
            partner = next(s for s in servers if s.rack == victim.rack and s.server_id != victim.server_id)
            partner_host_id = host_ids[servers.index(partner)]
            # Once it is a voter, the partner is eligible too.
            after_leaders = {survivor, partner_host_id} if leader_on_victim else {replica_of['rack2']}
            logger.info(f"group_id={group_id} replicas={tablets[0].replicas} victim={victim_host_id} ({victim}, {victim.rack}) "
                        f"partner={partner_host_id} ({partner}) leader after one of {sorted(after_leaders)} query={query}")

            if leader_on_victim:
                logger.info("Starting writes that retry fast failures")
                stop = asyncio.Event()
                writer = asyncio.ensure_future(write_through_failover(cql, table, stop))

                async def finish():
                    stop.set()
                    return await writer
            else:
                logger.info("Starting strict writes")
                finish = await start_writes(cql, ks, table_name, concurrency=1)
            try:
                logger.info(f"Killing {victim_host_id} ({victim}) and removing it through {query}")
                await manager.server_stop(victim.server_id, convict=True)
                await manager.remove_node(query.server_id, victim.server_id)
                logger.info("Removenode finished")
            finally:
                # finish() re-raises the first failed write or read-back.
                key_count = await asyncio.wait_for(finish(), 300)
            logger.info(f"key_count={key_count}")

            live_servers = [s for s in servers if s.server_id != victim.server_id]
            await check_replica_left(manager, query, live_servers, ks, table_name, group_id, original,
                                     victim_host_id, partner, partner_host_id, after_leaders, key_count)
            if leader_on_victim:
                logger.info("Checking that strict writes succeed after the failover")
                cql, _ = await manager.get_ready_cql(live_servers)
                stmt = cql.prepare(f"INSERT INTO {table} (pk, c) VALUES (?, ?)")
                for pk in range(key_count, key_count + 100):
                    await cql.run_async(stmt, [pk, pk])
            await reconnect_driver(manager)


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
@pytest.mark.skip_bug(link="https://scylladb.atlassian.net/browse/SCYLLADB-4753",
                      reason="a replica that needs a raft snapshot never catches up: transfer_snapshot() is not "
                             "implemented (SCYLLADB-2565); lands after the basic replace test of "
                             "https://github.com/scylladb/scylladb/pull/31887")
async def test_replace_sc_replica_after_log_truncation(manager: ScyllaClusterManager):
    """
    Replacing a dead replica must work after the raft log was truncated, when
    the new replica can only catch up through a snapshot.
    """
    racks = ['rack1', 'rack1', 'rack2', 'rack2', 'rack3', 'rack3']
    servers = await boot_sc_cluster(manager, racks, {'rack1', 'rack3'})
    cql, _ = await manager.get_ready_cql(servers)
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])
    query = next(s for s in servers if s.rack == 'rack1')

    async with new_test_keyspace(manager, KS_OPTS) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, c int") as table:
            table_name = table.split('.')[1]
            group_id = await get_table_raft_group_id(manager, ks, table_name)
            tablets = await get_all_tablet_replicas(manager, query, ks, table_name)
            original = {h for h, _ in tablets[0].replicas}
            replica_of = {servers[host_ids.index(h)].rack: h for h in original}
            assert replica_of.keys() == {'rack1', 'rack2', 'rack3'}, f"Expected one replica per rack, got {tablets[0].replicas}"
            await wait_for_leader(manager, servers[host_ids.index(replica_of['rack2'])], group_id,
                                  expected_host_id=replica_of['rack2'])

            logger.info("Truncating the raft logs: snapshot after every entry, keep 5")
            thresholds = {'snapshot_threshold': '0', 'snapshot_threshold_log_size': '0',
                          'snapshot_trailing': '5', 'snapshot_trailing_size': '0'}
            await gather_safely(*[manager.api.enable_injection(s.ip_addr, "raft_server_set_snapshot_thresholds",
                                                               one_shot=False, parameters=thresholds) for s in servers])
            key_count = 50
            for pk in range(key_count):
                await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES ({pk}, {pk})")
            await gather_safely(*[manager.api.disable_injection(s.ip_addr, "raft_server_set_snapshot_thresholds")
                                  for s in servers])

            victim = servers[host_ids.index(replica_of['rack3'])]
            logger.info(f"Killing the rack3 replica {replica_of['rack3']} ({victim}) and replacing it")
            await manager.server_stop(victim.server_id, convict=True)
            replacement = await manager.server_add(
                replace_cfg=ReplaceConfig(replaced_id=victim.server_id, reuse_ip_addr=False, use_host_id=True),
                config=SC_CONFIG.get_cluster_cfg({'error_injections_at_startup': ['avoid_being_raft_leader']}),
                cmdline=CMDLINE, property_file={'dc': 'dc1', 'rack': 'rack3'},
                # While the bug is open the replace never completes; the default timeout is 17 minutes.
                timeout=300)
            live_servers = [s for s in servers if s.server_id != victim.server_id] + [replacement]
            live_host_ids = [h for h in host_ids if h != replica_of['rack3']] + [await manager.get_host_id(replacement.server_id)]

            async def rebuilt_replica():
                tablets = await get_all_tablet_replicas(manager, query, ks, table_name)
                replicas = {h for h, _ in tablets[0].replicas}
                if replica_of['rack3'] in replicas or len(replicas) != 3:
                    return None
                (new,) = replicas - original
                return new
            new_host_id = await wait_for(rebuilt_replica, time.time() + 120, label="the rack3 replica rebuilt")
            new_replica = live_servers[live_host_ids.index(new_host_id)]
            assert new_replica.rack == 'rack3', f"Replica rebuilt outside rack3: {new_replica}"

            cql, hosts = await manager.get_ready_cql(live_servers)
            await read_barrier(manager.api, new_replica.ip_addr, group_id, timeout=60)
            await read_back(cql, hosts[live_servers.index(query)], table, key_count, ConsistencyLevel.QUORUM)
            await read_back(cql, hosts[live_servers.index(new_replica)], table, key_count, ConsistencyLevel.ONE)
            await reconnect_driver(manager)
