#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import logging

import pytest
from cassandra import ConsistencyLevel, WriteTimeout
from cassandra.query import SimpleStatement

from test.cluster.test_strong_consistency import DEFAULT_CMDLINE, DEFAULT_CONFIG, get_table_raft_group_id, wait_for_leader
from test.cluster.util import new_test_keyspace, new_test_table
from test.pylib.internal_types import ServerInfo
from test.pylib.rest_client import read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_tablet_replicas

logger = logging.getLogger(__name__)

INJECTION = 'sc_modification_statement_timeout'


async def pick_roles(manager: ScyllaClusterManager, servers: list[ServerInfo], ks: str, table_name: str) -> tuple[str, int, list[int], int]:
    """Return (raft group id, leader, followers, non-replica) of the table's single tablet, as indices into `servers`."""
    group_id = await get_table_raft_group_id(manager, ks, table_name)
    host_ids = [str(await manager.get_host_id(s.server_id)) for s in servers]
    replica_host_ids = [str(r[0]) for r in await get_tablet_replicas(manager, servers[0], ks, table_name, 0)]
    leader_host_id = str(await wait_for_leader(manager, servers[host_ids.index(replica_host_ids[0])], group_id))
    leader = host_ids.index(leader_host_id)
    followers = [host_ids.index(h) for h in replica_host_ids if h != leader_host_id]
    non_replica = next(i for i, h in enumerate(host_ids) if h not in replica_host_ids)
    return group_id, leader, followers, non_replica


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_write_timeout_after_success_is_stable(manager: ScyllaClusterManager):
    """
    Verify that a strongly consistent write which the client sees time out AFTER
    it was committed through Raft is applied exactly once and in order: it is
    visible to a linearizable read through every node, is applied locally on
    every replica, and a later write orders after it.

    The `sc_modification_statement_timeout` injection throws a write timeout on
    the leader right after the mutation has been committed, i.e. the client is
    told nothing useful about the fate of its write. The scenario is exercised
    once with the write sent to the leader and once with it forwarded from a
    non-replica, since the timeout must pass through the forwarding path too.

    Every write appends to a list rather than overwriting a value: each append
    gets its own cell keyed by the write's timestamp, so a write that was lost,
    applied twice (e.g. re-executed by a retry) or reordered shows up in the list.
    """
    servers = await manager.servers_add(4, config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE, auto_rack_dc='dc1')
    cql, hosts = await manager.get_ready_cql(servers)

    ks_opts = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1} AND consistency = 'global'"
    async with new_test_keyspace(manager, ks_opts) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, l list<int>") as table:
            group_id, leader, followers, non_replica = await pick_roles(manager, servers, ks, table.split('.')[-1])
            leader_ip = servers[leader].ip_addr
            logger.info(f"group {group_id}: leader={leader} followers={followers} non_replica={non_replica}")

            async def read(cl: ConsistencyLevel, via: int) -> list[int]:
                rows = await cql.run_async(SimpleStatement(f"SELECT l FROM {table} WHERE pk = 0", consistency_level=cl), host=hosts[via])
                return rows[0].l

            async def append(value: int, via: int) -> None:
                await cql.run_async(f"UPDATE {table} SET l = l + [{value}] WHERE pk = 0", host=hosts[via])

            expected = []
            # Round A: the write goes straight to the leader. Round B: it is forwarded there by a non-replica.
            for timeout_via, timeout_value, ok_via, ok_value in ((leader, 1, followers[0], 2), (non_replica, 3, leader, 4)):
                logger.info(f"Append {timeout_value} via {timeout_via}, expecting a timeout after the mutation was committed")
                await manager.api.enable_injection(leader_ip, INJECTION, one_shot=True)
                with pytest.raises(WriteTimeout):
                    await append(timeout_value, timeout_via)
                # The one-shot is armed on every shard but consumed only on the one that ran the write: clear the rest.
                await manager.api.disable_injection(leader_ip, INJECTION)

                expected.append(timeout_value)
                for via in (leader, *followers, non_replica):
                    assert await read(ConsistencyLevel.QUORUM, via) == expected, f"QUORUM read via {via}"

                logger.info(f"Append {ok_value} via {ok_via}, expecting it to order after {timeout_value}")
                await append(ok_value, ok_via)
                expected.append(ok_value)
                assert await read(ConsistencyLevel.QUORUM, non_replica) == expected
                for via in (leader, *followers):
                    await read_barrier(manager.api, servers[via].ip_addr, group_id, timeout=60)
                    assert await read(ConsistencyLevel.ONE, via) == expected, f"local read on replica {via}"
