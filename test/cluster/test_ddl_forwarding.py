#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import logging
import pytest
import time
from cassandra.cluster import Cluster
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra import InvalidRequest
from test.pylib.internal_types import HostID, ServerInfo
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.cluster.util import new_test_keyspace
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)

FORWARDED_METRIC = 'scylla_transport_requests_forwarded_successfully'
FORWARD_ATTEMPTS_METRIC = 'scylla_cql_forwarded_requests'


async def forwarded(manager: ScyllaClusterManager, ip: str) -> int:
    return (await manager.metrics.query(ip)).get(FORWARDED_METRIC) or 0


async def forward_attempts(manager: ScyllaClusterManager, ip: str) -> int:
    return (await manager.metrics.query(ip)).get(FORWARD_ATTEMPTS_METRIC) or 0


async def leader_and_follower(manager: ScyllaClusterManager, servers: list[ServerInfo], hosts):
    """The group 0 leader, one follower, and the driver host of each server by ip."""
    leader_id = await manager.api.get_raft_leader(servers[0].ip_addr)
    leader = (await manager.all_servers_by_host_id())[leader_id]
    follower = next(s for s in servers if s.server_id != leader.server_id)
    host_of = {s.ip_addr: h for s in servers for h in hosts if h.address == s.ip_addr}
    logger.info(f"group0 leader {leader.ip_addr}, follower {follower.ip_addr}")
    return leader, follower, host_of


@pytest.mark.asyncio
async def test_ddl_forwarded_to_group0_leader(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, auto_rack_dc='dc1')
    cql, hosts = await manager.get_ready_cql(servers)
    leader, follower, host_of = await leader_and_follower(manager, servers, hosts)

    ks_opts = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
    async with new_test_keyspace(manager, ks_opts) as ks:
        # DDL on a follower is forwarded; the reply and the schema change reach the client.
        before = await forwarded(manager, follower.ip_addr)
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int)", host=host_of[follower.ip_addr])
        await cql.run_async(f"ALTER TABLE {ks}.t ADD w int", host=host_of[follower.ip_addr])
        assert await forwarded(manager, follower.ip_addr) == before + 2
        rows = await cql.run_async(f"SELECT column_name FROM system_schema.columns WHERE keyspace_name = '{ks}' AND table_name = 't'",
                                   host=host_of[follower.ip_addr])
        assert {r.column_name for r in rows} == {'pk', 'v', 'w'}

        # Prepared DDL with bind values goes through the same path.
        stmt = cql.prepare(f"INSERT INTO {ks}.t (pk, v) VALUES (?, ?)")
        await cql.run_async(stmt, [1, 2], host=host_of[follower.ip_addr])
        drop = cql.prepare(f"DROP TABLE {ks}.t")
        await cql.run_async(drop, [], host=host_of[follower.ip_addr])
        assert await forwarded(manager, follower.ip_addr) == before + 3

        # Errors from the leader are passed back as-is.
        with pytest.raises(InvalidRequest, match="Cannot drop non existing table"):
            await cql.run_async(f"DROP TABLE {ks}.t", host=host_of[follower.ip_addr])

        # The follower applies the change before replying, like local execution does,
        # so a client that does not wait for schema agreement sees it right away.
        pinned = Cluster([follower.ip_addr], load_balancing_policy=WhiteListRoundRobinPolicy([follower.ip_addr]),
                         max_schema_agreement_wait=0)
        try:
            session = pinned.connect()
            session.execute(f"CREATE TABLE {ks}.t3 (pk int PRIMARY KEY)")
            rows = session.execute(f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = 't3'")
            assert [r.table_name for r in rows] == ['t3']
        finally:
            pinned.shutdown()

        # DDL on the leader runs locally.
        before = await forwarded(manager, leader.ip_addr)
        await cql.run_async(f"CREATE TABLE {ks}.t2 (pk int PRIMARY KEY)", host=host_of[leader.ip_addr])
        assert await forwarded(manager, leader.ip_addr) == before


@pytest.mark.asyncio
async def test_service_levels_forwarded_to_group0_leader(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, auto_rack_dc='dc1')
    cql, hosts = await manager.get_ready_cql(servers)
    leader, follower, host_of = await leader_and_follower(manager, servers, hosts)
    follower_host = host_of[follower.ip_addr]

    before = await forwarded(manager, follower.ip_addr)
    await cql.run_async("CREATE SERVICE LEVEL sl WITH shares = 500", host=follower_host)
    await cql.run_async("ALTER SERVICE LEVEL sl WITH shares = 700", host=follower_host)
    assert await forwarded(manager, follower.ip_addr) == before + 2

    # The change is applied on the follower when the statement returns.
    rows = await cql.run_async("LIST SERVICE LEVEL sl", host=follower_host)
    assert [(r.service_level, r.shares) for r in rows] == [('sl', 700)]


@pytest.mark.asyncio
async def test_ddl_without_group0_leader_runs_locally(manager: ScyllaClusterManager):
    """With no leader the DDL takes the local path, which waits for the election."""
    # The local path waits for a leader in each group 0 step; give the restart time.
    servers = await manager.servers_add(3, config={'group0_raft_op_timeout_in_ms': 300000}, auto_rack_dc='dc1')
    cql, hosts = await manager.get_ready_cql(servers)
    leader, follower, host_of = await leader_and_follower(manager, servers, hosts)
    other = next(s for s in servers if s.server_id not in (leader.server_id, follower.server_id))

    ks_opts = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
    async with new_test_keyspace(manager, ks_opts) as ks:
        await asyncio.gather(manager.server_stop(leader.server_id, convict=True),
                             manager.server_stop(other.server_id, convict=True))
        no_leader = HostID('00000000-0000-0000-0000-000000000000')
        async def leader_gone():
            return await manager.api.get_raft_leader(follower.ip_addr) == no_leader or None
        await wait_for(leader_gone, time.time() + 60)

        before = await forwarded(manager, follower.ip_addr)
        ddl = cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY)", host=host_of[follower.ip_addr])
        await asyncio.sleep(3)
        assert not ddl.done()

        await manager.server_start(other.server_id)
        await ddl
        assert await forwarded(manager, follower.ip_addr) == before
        await manager.server_start(leader.server_id)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.asyncio
async def test_ddl_runs_locally_when_leader_dies_during_forward(manager: ScyllaClusterManager):
    """A forward that fails falls back to the local path instead of failing the client."""
    servers = await manager.servers_add(3, auto_rack_dc='dc1')
    cql, hosts = await manager.get_ready_cql(servers)
    leader, follower, host_of = await leader_and_follower(manager, servers, hosts)

    ks_opts = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
    async with new_test_keyspace(manager, ks_opts) as ks:
        # Hold the forwarded statement on the leader, then kill the leader under it.
        await manager.api.enable_injection(leader.ip_addr, "wait_before_handling_forwarded_request", one_shot=True)
        attempts = await forward_attempts(manager, follower.ip_addr)
        successes = await forwarded(manager, follower.ip_addr)
        ddl = cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY)", host=host_of[follower.ip_addr])
        async def forward_sent():
            return await forward_attempts(manager, follower.ip_addr) > attempts or None
        await wait_for(forward_sent, time.time() + 60)
        await manager.server_stop(leader.server_id, convict=True)

        await ddl
        assert await forwarded(manager, follower.ip_addr) == successes
        rows = await cql.run_async(f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = 't'",
                                   host=host_of[follower.ip_addr])
        assert [r.table_name for r in rows] == ['t']
        await manager.server_start(leader.server_id)
