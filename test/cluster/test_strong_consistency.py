#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from typing import Tuple
import re

from test.pylib.manager_client import ManagerClient
from test.pylib.util import gather_safely, wait_for, Host
from test.cluster.util import new_test_keyspace, new_test_table, reconnect_driver
from test.pylib.internal_types import HostID, ServerInfo
from cassandra import InvalidRequest, ReadTimeout, WriteTimeout
from cassandra.cluster import ConsistencyLevel
from cassandra.policies import FallthroughRetryPolicy
from cassandra.protocol import InvalidRequest
from cassandra.query import SimpleStatement, BoundStatement
from test.pylib.tablets import get_all_tablet_replicas, get_tablet_replicas
from test.pylib.rest_client import read_barrier

import asyncio
import pytest
import logging
import time
import uuid
import random
import asyncio


logger = logging.getLogger(__name__)


DEFAULT_CONFIG = {'experimental_features': ['strongly-consistent-tables']}
DEFAULT_CMDLINE = [
        '--logger-log-level', 'sc_groups_manager=debug',
        '--logger-log-level', 'sc_coordinator=debug'
    ]


async def wait_for_leader(manager: ManagerClient, s: ServerInfo, group_id: str):
    async def get_leader_host_id():
        result = await manager.api.get_raft_leader(s.ip_addr, group_id)
        return None if uuid.UUID(result).int == 0 else result
    return await wait_for(get_leader_host_id, time.time() + 60)

async def collect_all_raft_state(cql, host):
    state = {}
    rows = await cql.run_async(f"SELECT DISTINCT shard, group_id, vote_term, commit_idx FROM system.raft_groups", host=host)
    logger.info(f"Collected raft state from host {host}:")
    for row in rows:
        assert str(row.group_id) not in state, f"Duplicate raft state for group {row.group_id} shard {row.shard}"
        state[str(row.group_id)] = (row.shard, row.vote_term, row.commit_idx)
    return state


# Verify that:
# - All raft groups present before an event are still present after the event.
# - For each raft group, the shard did not change.
# - For each raft group, vote_term did not decrease (if present).
# - For each raft group, commit_idx did not decrease (if present).
def assert_raft_state_continuity(state_before: dict, state_after: dict, context: str):
    for group_id, (shard_before, vote_term_before, commit_idx_before) in state_before.items():
        assert group_id in state_after, (
            f"Raft state for group {group_id} shard {shard_before} missing after {context}."
        )
        shard_after, vote_term_after, commit_idx_after = state_after[group_id]

        assert shard_before == shard_after, (
            f"Raft state for group {group_id} changed shard from {shard_before} to {shard_after} after {context}."
        )

        if vote_term_before is not None:
            assert vote_term_after is not None and vote_term_after >= vote_term_before, (
                f"vote_term decreased for group {group_id} shard {shard_before} after {context}: "
                f"{vote_term_before} -> {vote_term_after}."
            )

        if commit_idx_before is not None:
            assert commit_idx_after is not None and commit_idx_after >= commit_idx_before, (
                f"commit_idx decreased for group {group_id} shard {shard_before} after {context}: "
                f"{commit_idx_before} -> {commit_idx_after}."
            )

# Verify that reads and writes to raft tables for strongly consistent tablets
# are always routed to the expected shards, according to the tablet's
# assignment.
async def assert_no_cross_shard_routing(manager: ManagerClient, server: ServerInfo):
    log = await manager.server_open_log(server.server_id)

    # Check partitioner logs
    partitioner_logs = await log.grep(r"fixed_shard.*get_token: shard=(\d+), token=(\d+)")
    computed_tokens = {}
    for _, match in partitioner_logs:
        shard = int(match.group(1))
        token = int(match.group(2))
        computed_tokens[token] = shard

    # Check sharder logs
    sharder_logs = await log.grep(r"fixed_shard.*shard_of\((\d+)\) = (\d+)")
    for _, match in sharder_logs:
        token = int(match.group(1))
        shard = int(match.group(2))
        if token not in computed_tokens:
            # This can happen when we read the raft tables as a client, through CQL.
            # In that case, we may try to route a token that does not correspond
            # to any existing raft group.
            continue
        shard_from_partitioner = computed_tokens[token]
        assert shard == shard_from_partitioner, (
            f"Sharder routed token {token} to shard {shard}, "
            f"but partitioner computed shard {shard_from_partitioner}."
        )

async def get_table_raft_group_id(manager: ManagerClient, ks: str, table: str):
    table_id = await manager.get_table_id(ks, table)
    rows = await manager.get_cql().run_async(f"SELECT raft_group_id FROM system.tablets where table_id = {table_id}")
    return str(rows[0].raft_group_id)

async def test_basic_write_read(manager: ManagerClient):

    logger.info("Bootstrapping cluster")
    servers = await manager.servers_add(3, config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE, auto_rack_dc='my_dc')
    (cql, hosts) = await manager.get_ready_cql(servers)

    logger.info("Load host_id-s for servers")
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])

    def host_by_host_id(host_id):
        for hid, host in zip(host_ids, hosts):
            if hid == host_id:
                return host
        raise RuntimeError(f"Can't find host for host_id {host_id}")

    logger.info("Creating a strongly-consistent keyspace")
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        logger.info("Creating a table")
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        logger.info("Select raft group id for the tablet")
        group_id = await get_table_raft_group_id(manager, ks, 'test')

        tablet_replicas = await get_tablet_replicas(manager, servers[0], ks, "test", 0)
        assert len(tablet_replicas) == 2
        replica_host_ids = [replica[0] for replica in tablet_replicas]

        logger.info(f"Get current leader for the group {group_id}")
        if host_ids[0] in replica_host_ids:
            leader_host_id = await wait_for_leader(manager, servers[0], group_id)
        else:
            leader_host_id = await wait_for_leader(manager, servers[1], group_id)
        leader_host = host_by_host_id(leader_host_id)

        logger.info(f"Get the non-leader replica for the group {group_id}")
        non_leader_replica_host_id = [host_id for host_id in replica_host_ids if str(host_id) != str(leader_host_id)][0]
        non_leader_replica_host = host_by_host_id(non_leader_replica_host_id)

        logger.info(f"Get the non-replica for the group {group_id}")
        non_replica_host_id = [host_id for host_id in host_ids if str(host_id) not in replica_host_ids][0]
        non_replica_host = host_by_host_id(non_replica_host_id)

        async def collect_metrics():
            leader_metrics_query = await manager.metrics.query(leader_host.address)
            leader_metrics = {
                'scylla_strong_consistency_coordinator_write_latency_count': leader_metrics_query.get('scylla_strong_consistency_coordinator_write_latency_count') or 0,
                'scylla_strong_consistency_coordinator_read_latency_count':  leader_metrics_query.get('scylla_strong_consistency_coordinator_read_latency_count', {'read_type': 'linearizable'}) or 0,
            }
            non_leader_metrics_query = await manager.metrics.query(non_leader_replica_host.address)
            non_leader_metrics = {
                'scylla_strong_consistency_coordinator_write_node_bounces': non_leader_metrics_query.get('scylla_strong_consistency_coordinator_write_node_bounces') or 0,
                'scylla_strong_consistency_coordinator_read_latency_count': non_leader_metrics_query.get('scylla_strong_consistency_coordinator_read_latency_count', {'read_type': 'linearizable'}) or 0,
            }
            non_replica_metrics_query = await manager.metrics.query(non_replica_host.address)
            non_replica_metrics = {
                'scylla_strong_consistency_coordinator_write_node_bounces': non_replica_metrics_query.get('scylla_strong_consistency_coordinator_write_node_bounces') or 0,
                'scylla_strong_consistency_coordinator_read_node_bounces':  non_replica_metrics_query.get('scylla_strong_consistency_coordinator_read_node_bounces') or 0,
            }

            return {
                'leader': leader_metrics,
                'replica': non_leader_metrics,
                'non_replica': non_replica_metrics,
            }     
        metrics_before = await collect_metrics()

        logger.info(f"Run INSERT statement on leader {leader_host}")
        await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES (10, 20)", host=leader_host)

        logger.info(f"Run SELECT statement on leader {leader_host}")
        rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = 10;", host=leader_host)
        assert len(rows) == 1
        row = rows[0]
        assert row.pk == 10
        assert row.c == 20

        logger.info(f"Run INSERT statement on non-leader replica {non_leader_replica_host}")
        await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES (10, 30)", host=non_leader_replica_host)

        logger.info(f"Run SELECT statement on non-leader replica {non_leader_replica_host}")
        rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = 10;", host=non_leader_replica_host)
        assert len(rows) == 1
        row = rows[0]
        assert row.pk == 10
        assert row.c == 30

        logger.info(f"Run INSERT statement on non-replica {non_replica_host}")
        await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES (10, 40)", host=non_replica_host)

        logger.info(f"Run SELECT statement on non-replica {non_replica_host}")
        rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = 10;", host=non_replica_host)
        assert len(rows) == 1
        row = rows[0]
        assert row.pk == 10
        assert row.c == 40

        # Test with prepared statements as well
        insert_stmt = cql.prepare(f"INSERT INTO {ks}.test (pk, c) VALUES (?, ?)")
        bound_insert_stmt = BoundStatement(insert_stmt)
        select_stmt = cql.prepare(f"SELECT * FROM {ks}.test WHERE pk = ?")
        bound_select_stmt = BoundStatement(select_stmt, consistency_level=ConsistencyLevel.QUORUM)
        bound_select_stmt.bind([10])

        logger.info(f"Run prepared INSERT statement on leader {leader_host}")
        bound_insert_stmt.bind([10, 50])
        await cql.run_async(bound_insert_stmt, host=leader_host)

        logger.info(f"Run prepared SELECT statement on leader {leader_host}")
        rows = await cql.run_async(bound_select_stmt, host=leader_host)
        assert len(rows) == 1
        row = rows[0]
        assert row.pk == 10
        assert row.c == 50

        logger.info(f"Run prepared INSERT statement on non-leader replica {non_leader_replica_host}")
        bound_insert_stmt.bind([10, 60])
        await cql.run_async(bound_insert_stmt, host=non_leader_replica_host)

        logger.info(f"Run prepared SELECT statement on non-leader replica {non_leader_replica_host}")
        rows = await cql.run_async(bound_select_stmt, host=non_leader_replica_host)
        assert len(rows) == 1
        row = rows[0]
        assert row.pk == 10
        assert row.c == 60

        logger.info(f"Run prepared INSERT statement on non-replica {non_replica_host}")
        bound_insert_stmt.bind([10, 70])
        await cql.run_async(bound_insert_stmt, host=non_replica_host)

        logger.info(f"Run prepared SELECT statement on non-replica {non_replica_host}")
        rows = await cql.run_async(bound_select_stmt, host=non_replica_host)
        assert len(rows) == 1
        row = rows[0]
        assert row.pk == 10
        assert row.c == 70

        metrics_after = await collect_metrics()
        # Validate that all matrics were increased after read/write operations
        for host in metrics_after.keys():
            for metric_name in metrics_after[host].keys():
                assert metrics_after[host][metric_name] > metrics_before[host][metric_name]

        # Check that we can restart a server with an active tablets raft group
        await manager.server_restart(servers[2].server_id)

    # To check that the servers can be stopped gracefully. By default the test runner just kills them.
    await gather_safely(*[manager.server_stop_gracefully(s.server_id) for s in servers])

async def test_multi_shard_write_read(manager: ManagerClient):
    """
    Verify that strongly consistent tables work correctly on non-shard-0.

    We create a strongly consistent table with 4 tablets and RF=3
    and a cluster with 3 nodes and 4 shards per node.
    Assuming a uniform distribution of tablets to shards,
    each tablet should be assigned to a different shard.
    We have 3 "shard zeros", so at least one tablet should be
    replicated on non-shard-0 nodes only, including its leader.

    We then perform multiple writes to all nodes, some of which
    should go to non-shard-0 leaders, and verify that all writes
    succeed and that we can read back all data correctly.
    """
    logger.info("Bootstrapping cluster with 4 shards per node")
    cmdline = DEFAULT_CMDLINE + ['--smp=4']
    servers = await manager.servers_add(3, config=DEFAULT_CONFIG, cmdline=cmdline, auto_rack_dc='my_dc')
    (cql, hosts) = await manager.get_ready_cql(servers)

    logger.info("Creating a strongly-consistent keyspace with 4 tablets")
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 4} AND consistency = 'global'") as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, c int") as table:
            for j in range(50):
                await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES ({j}, {j})")

            # Read all data
            for j in range(50):
                rows = await cql.run_async(f"SELECT * FROM {table} WHERE pk = {j};")
                assert len(rows) == 1, f"Expected 1 row for pk={j}, got {len(rows)}"
                row = rows[0]
                assert row.c == j, f"Data integrity check failed for pk={j}: expected c={j}, got c={row.c}"

    await gather_safely(*[manager.server_stop_gracefully(s.server_id) for s in servers])

async def test_sc_multishard_metadata_reads(manager: ManagerClient):
    """
    Verify that multi-shard reads of raft metadata for strongly-consistent tables work correctly.
    """
    cmdline = DEFAULT_CMDLINE + [
        '--smp=4',
        '--logger-log-level', 'fixed_shard=trace',
    ]
    server = await manager.server_add(config=DEFAULT_CONFIG, cmdline=cmdline)
    (cql, hosts) = await manager.get_ready_cql([server])

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 8} AND consistency = 'global'") as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, c int") as table:
            table_name = table.split('.')[-1]
            table_id = await manager.get_table_id(ks, table_name)

            # Write some rows to trigger raft table updates
            for pk in range(10):
                await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES ({pk}, {pk})")

            # Verify that tablets are allocated also on non-0 shards
            tablets = await get_all_tablet_replicas(manager, server, ks, table_name)
            logger.info(f"Tablet distribution: {tablets}")
            assert set(shard for tablet in tablets for _, shard in tablet.replicas) != set([0]), "Strongly consistent tables shoud be allocated also on non-0 shards"

            # Verify we have non-empty state
            state = await collect_all_raft_state(cql, hosts[0])
            has_nonempty_state = any(
                commit_idx is not None and commit_idx > 0
                for _, commit_idx, _ in state.values()
            )
            assert has_nonempty_state, "Expected at least one group to have commit_idx > 0 before crash"

            # Prepare the list of (shard, group_id) pairs for all tablets of the table.
            raft_partition_keys = []
            for row in await cql.run_async(f"SELECT raft_group_id, replicas FROM system.tablets where table_id = {table_id}"):
                assert len(row.replicas) == 1, "Expected RF=1 for the test table"
                (_, shard) = row.replicas[0]
                raft_partition_keys.append((shard, row.raft_group_id))
            assert len(raft_partition_keys) == 8, f"Expected 8 tablets, got {len(raft_partition_keys)}"

            # Collect raft metadata for all tablets using single-shard queries
            raft_data = {}
            for (shard, group_id) in raft_partition_keys:
                raft_data[(shard, group_id)] = set(await cql.run_async(f"SELECT * FROM system.raft_groups WHERE shard = {shard} AND group_id = {group_id}"))
                # Verify that we can also obtain the same data without knowing the shard using ALLOW FILTERING
                filtered_data = set(await cql.run_async(f"SELECT * FROM system.raft_groups WHERE group_id = {group_id} ALLOW FILTERING"))
                assert raft_data[(shard, group_id)] == filtered_data, f"Data mismatch for group {group_id} when read by shard vs ALLOW FILTERING: {raft_data[(shard, group_id)]} vs {filtered_data}"

            # Now read raft metadata using multi-shard queries and verify correctness
            for k in range(2, 9):
                sample_keys = random.sample(raft_partition_keys, k)
                shards = tuple(shard for (shard, _) in sample_keys)
                group_ids = ", ".join(f"{group_id}" for group_id in set(group_id for (_, group_id) in sample_keys))
                logger.info(f"Testing multi-shard read with {k} keys: {sample_keys}")
                # We can't specify (shard, group_id) partition key pairs in the IN clause because multi-column restriction are only allowed for clustering keys.
                # Instead, we read using a product of all involved shards and group_ids. This should return the same rows because every group_id
                # is only assigned to one shard.
                rows = await cql.run_async(f"SELECT * FROM system.raft_groups WHERE shard IN {shards} AND group_id IN ({group_ids})")
                for key in sample_keys:
                    for row_data in raft_data[key]:
                        assert row_data in rows, f"Missing data for raft group {key} in multi-shard read: {row_data}"
                for row in rows:
                    assert row in raft_data[(row.shard, row.group_id)], f"Unexpected data for raft group {(row.shard, row.group_id)} in multi-shard read: {row}"

            # Read ranges of partitions using TOKEN()
            for (boundary_shard, boundary_group_id) in raft_partition_keys:
                ith_token = await cql.run_async(f"SELECT TOKEN(shard, group_id) as t FROM system.raft_groups WHERE shard = {boundary_shard} AND group_id = {boundary_group_id} LIMIT 1")
                token_value = ith_token[0].t
                range_below = await cql.run_async(f"SELECT * FROM system.raft_groups WHERE TOKEN(shard, group_id) <= {token_value}")
                range_above = await cql.run_async(f"SELECT * FROM system.raft_groups WHERE TOKEN(shard, group_id) > {token_value}")
                assert set(range_below) & set(range_above) == set(), f"Overlapping data in range reads up to and above token {token_value}"
                for (shard, group_id) in raft_partition_keys:
                    for row_data in raft_data[(shard, group_id)]:
                        if shard < boundary_shard:
                            assert row_data in range_below, f"Missing data for raft group {(shard, group_id)} in range read up to token {token_value}: {row_data}"
                        elif shard > boundary_shard:
                            assert row_data in range_above, f"Missing data for raft group {(shard, group_id)} in range read above token {token_value}: {row_data}"
                        else:
                            if group_id == boundary_group_id:
                                assert row_data in range_below, f"Missing data for raft group {(shard, group_id)} in range read up to token {token_value}: {row_data}"
                            else:
                                assert row_data in range_below or row_data in range_above, f"Missing data for raft group {(shard, group_id)} in range read up to token {token_value}: {row_data}"

    await manager.server_stop_gracefully(server.server_id)

async def test_sc_persistence_restart_with_smp_increase(manager: ManagerClient):
    """
    Verify that the metadata for strongly-consistent tables
    is preserved after increasing shard count (--smp).

    The raft tables for strongly consistent tables use a custom partitioner
    and sharder which should not change the shard assignments when the
    number of shards changes. This test verifies that after increasing the SMP
    on a single-node cluster, we don't start reading/writing raft metadata
    from/to incorrect shards.
    """
    cmdline = DEFAULT_CMDLINE + [
        '--smp=2',
        '--logger-log-level', 'fixed_shard=trace',
    ]
    server = await manager.server_add(config=DEFAULT_CONFIG, cmdline=cmdline)
    (cql, hosts) = await manager.get_ready_cql([server])

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 2} AND consistency = 'global'") as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, c int") as table:

            # Write some rows to trigger raft table updates
            for pk in range(10):
                await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES ({pk}, {pk})")

            state_before = await collect_all_raft_state(cql, hosts[0])

            await manager.server_update_cmdline(server.server_id, ['--smp=4'])
            await manager.server_restart(server.server_id)
            await reconnect_driver(manager)
            cql = manager.get_cql()

            # We can't read the internal raft state directly, so we perform extra writes
            # which should cause raft table updates based on the loaded state after restart.
            for pk in range(10, 20):
                await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES ({pk}, {pk})")

            state_after = await collect_all_raft_state(cql, hosts[0])
            assert_raft_state_continuity(state_before, state_after, "SMP increase")

            await assert_no_cross_shard_routing(manager, server)

            for pk in range(20):
                rows = await cql.run_async(f"SELECT * FROM {table} WHERE pk = {pk};")
                assert len(rows) == 1, f"Expected 1 row for pk={pk}, got {len(rows)}"
                row = rows[0]
                assert row.c == pk, f"Incorrect row read for pk={pk}: expected c={pk}, got c={row.c}"

    await manager.server_stop_gracefully(server.server_id)


async def test_sc_persistence_with_compaction(manager: ManagerClient):
    """
    Verify that compaction of system.raft_groups works correctly.

    Regular (non-reshard) compaction does not use the custom partitioner/sharder
    directly, bit it does compact SSTables written with them, so in this test we
    verify that after compaction the raft metadata is still readable and correct.
    """
    cmdline = DEFAULT_CMDLINE + ['--logger-log-level', 'fixed_shard=trace']
    server = await manager.server_add(config=DEFAULT_CONFIG, cmdline=cmdline)
    (cql, hosts) = await manager.get_ready_cql([server])

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 4} AND consistency = 'global'") as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, c int") as table:

            # Create multiple SSTables by doing writes with flushes in between
            for batch in range(3):
                for pk in range(batch * 10, (batch + 1) * 10):
                    await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES ({pk}, {pk})")
                await manager.api.keyspace_flush(server.ip_addr, "system", "raft_groups")

            state_before_compaction = await collect_all_raft_state(cql, hosts[0])

            await manager.api.keyspace_compaction(server.ip_addr, "system", "raft_groups")

            state_after_compaction = await collect_all_raft_state(cql, hosts[0])
            assert_raft_state_continuity(state_before_compaction, state_after_compaction, "compaction")

            # Restart to verify compacted SSTables are correctly readable by raft server
            await manager.server_restart(server.server_id)
            await reconnect_driver(manager)
            cql = manager.get_cql()

            # We can't read the internal raft state directly, so we perform extra writes
            # which should cause raft table updates based on the loaded state after restart.
            for pk in range(30, 40):
                await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES ({pk}, {pk})")

            state_after_restart = await collect_all_raft_state(cql, hosts[0])
            assert_raft_state_continuity(state_after_compaction, state_after_restart, "compaction + restart")

            await assert_no_cross_shard_routing(manager, server)

    await manager.server_stop_gracefully(server.server_id)


async def test_sc_persistence_after_crash(manager: ManagerClient):
    """
    Verify that metadata for strongly-consistent tables is recovered
    after a non-graceful stop (crash simulation).
    """
    cmdline = DEFAULT_CMDLINE + ['--logger-log-level', 'fixed_shard=trace']
    server = await manager.server_add(config=DEFAULT_CONFIG, cmdline=cmdline)
    (cql, hosts) = await manager.get_ready_cql([server])

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 4} AND consistency = 'global'") as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, c int") as table:
            # Write some rows to trigger raft table updates
            for pk in range(20):
                await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES ({pk}, {pk})")

            # Collect raft state and log entry counts before crash
            state_before_crash = await collect_all_raft_state(cql, hosts[0])

            await manager.server_stop(server.server_id, convict=False)

            await manager.server_start(server.server_id)
            await reconnect_driver(manager)
            cql = manager.get_cql()

            # We can't read the internal raft state directly, so we perform extra writes
            # which should cause raft table updates based on the loaded state after restart.
            for pk in range(20, 30):
                await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES ({pk}, {pk})")

            state_after_crash = await collect_all_raft_state(cql, hosts[0])
            assert_raft_state_continuity(state_before_crash, state_after_crash, "crash recovery")

            await assert_no_cross_shard_routing(manager, server)

    await manager.server_stop_gracefully(server.server_id)

@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_no_schema_when_apply_write(manager: ManagerClient):
    servers = await manager.servers_add(2, config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE, auto_rack_dc='my_dc')
    # We don't want `servers[2]` to be a Raft leader (for both group0 and strong consistency groups),
    # because we want `servers[2]` to receive Raft commands from others.
    config = DEFAULT_CONFIG | {'error_injections_at_startup': ['avoid_being_raft_leader']}
    servers += [await manager.server_add(config=config, cmdline=DEFAULT_CMDLINE, property_file={'dc':'my_dc', 'rack': 'rack3'})]
    (cql, hosts) = await manager.get_ready_cql(servers)
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])

    def host_by_host_id(host_id):
        for hid, host in zip(host_ids, hosts):
            if hid == host_id:
                return host
        raise RuntimeError(f"Can't find host for host_id {host_id}")

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        # Drop incoming append entries from group0 (schema changes) on `servers[2]` after the table is created,
        # so strong consistency raft groups are created on the node but it won't receive next alter table mutations.
        group0_id = (await cql.run_async("SELECT value FROM system.scylla_local WHERE key = 'raft_group0_id'"))[0].value
        await manager.api.enable_injection(servers[2].ip_addr, "raft_drop_incoming_append_entries_for_specified_group", one_shot=False, parameters={'value': group0_id})
        await cql.run_async(f"ALTER TABLE {ks}.test ADD new_col int;", host=hosts[0])

        group_id = await get_table_raft_group_id(manager, ks, 'test')
        leader_host_id = await wait_for_leader(manager, servers[0], group_id)
        assert leader_host_id != host_ids[2]
        leader_host = host_by_host_id(leader_host_id)

        s2_log = await manager.server_open_log(servers[2].server_id)
        s2_mark = await s2_log.mark()

        await manager.api.enable_injection(servers[2].ip_addr, "disable_raft_drop_append_entries_for_specified_group", one_shot=True)
        await cql.run_async(f"INSERT INTO {ks}.test (pk, c, new_col) VALUES (10, 20, 30)", host=leader_host)

        await s2_log.wait_for(f"Column definitions for {ks}.test changed", timeout=60, from_mark=s2_mark)
        rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = 10;", host=hosts[2])
        assert len(rows) == 1
        row = rows[0]
        assert row.pk == 10
        assert row.c == 20
        assert row.new_col == 30

@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_old_schema_when_apply_write(manager: ManagerClient):
    servers = await manager.servers_add(2, config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE, auto_rack_dc='my_dc')
    # We don't want `servers[2]` to be a Raft leader (for both group0 and strong consistency groups),
    # because we want `servers[2]` to receive Raft commands from others.
    config = DEFAULT_CONFIG | {'error_injections_at_startup': ['avoid_being_raft_leader']}
    servers += [await manager.server_add(config=config, cmdline=DEFAULT_CMDLINE, property_file={'dc':'my_dc', 'rack': 'rack3'})]
    (cql, hosts) = await manager.get_ready_cql(servers)
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])

    def host_by_host_id(host_id):
        for hid, host in zip(host_ids, hosts):
            if hid == host_id:
                return host
        raise RuntimeError(f"Can't find host for host_id {host_id}")

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        group_id = await get_table_raft_group_id(manager, ks, 'test')
        leader_host_id = await wait_for_leader(manager, servers[0], group_id)
        assert leader_host_id != host_ids[2]
        leader_host = host_by_host_id(leader_host_id)

        table_schema_version = (await cql.run_async(f"SELECT version FROM system_schema.scylla_tables WHERE keyspace_name = '{ks}' AND table_name = 'test'"))[0].version

        await manager.api.enable_injection(servers[2].ip_addr, "strong_consistency_state_machine_wait_before_apply", one_shot=False)
        await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES (10, 20)", host=leader_host)

        await cql.run_async(f"ALTER TABLE {ks}.test ADD new_col int;", host=leader_host)
        # Following injection simulates that old schema version was already removed from the memory
        await manager.api.enable_injection(servers[2].ip_addr, "schema_registry_ignore_version", one_shot=False, parameters={'value': table_schema_version})
        await manager.api.message_injection(servers[2].ip_addr, "strong_consistency_state_machine_wait_before_apply")
        await manager.api.disable_injection(servers[2].ip_addr, "strong_consistency_state_machine_wait_before_apply")

        rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = 10;", host=hosts[2])
        assert len(rows) == 1
        row = rows[0]
        assert row.pk == 10
        assert row.c == 20
        assert row.new_col is None

async def test_reject_user_provided_timestamps(manager: ManagerClient):
    """
    A simple validation test that makes sure that we don't accept
    user-provided timestamps in queries to strongly consistent tables.
    """

    server = await manager.server_add(config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE)
    cql, _ = await manager.get_ready_cql([server])

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, v int") as table:
            error_msg = "Strongly consistent queries don't support user-provided timestamps"
            with pytest.raises(InvalidRequest, match=error_msg):
                await cql.run_async(f"INSERT INTO {table} (pk, v) VALUES (0, 13) USING TIMESTAMP 23")
            with pytest.raises(InvalidRequest, match=error_msg):
                await cql.run_async(f"UPDATE {table} USING TIMESTAMP 23 SET v = 13 WHERE pk = 0")
            with pytest.raises(InvalidRequest, match=error_msg):
                await cql.run_async(f"DELETE FROM {table} USING TIMESTAMP 23 WHERE pk = 0")
            # FIXME(SCYLLADB-977):
            # Add test cases for batches with timestamps. Remember to
            # handle both whole-batch timestamps, e.g.
            #   BEGIN BATCH USING TIMESTAMP ts
            #     ...
            #   APPLY BATCH
            # as well as timestamps for individual items, e.g.
            #   BEGIN BATCH
            #     INSERT INTO ... USING TIMESTAMP st;
            #     ...
            #   APPLY BATCH

async def test_forward_cql_prepared_with_bound_values(manager: ManagerClient):
    """
    When we prepare an statement not on the leader, we should
    still be able to forward it to the leader with bound values.
    In this test we prepare a statement that should be forwarded to
    the leader (so an INSERT statement) and then execute it.
    It should correctly insert the value.
    Then, we execute it again and check that we used the cache
    on the leader side and that it updated the value correctly again.
    """
    cmdline = [
        '--logger-log-level', 'cql_server=trace',
        '--experimental-features', 'strongly-consistent-tables',
    ]
    servers = await manager.servers_add(2, cmdline=cmdline, auto_rack_dc='dc1')
    (cql, hosts) = await manager.get_ready_cql(servers)

    ks_opts = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND consistency = 'global'")
    async with new_test_keyspace(manager, ks_opts) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, value int") as table:
            table_name = table.split('.')[-1]
            group_id = await get_table_raft_group_id(manager, ks, table_name)
            leader_host_id = await wait_for_leader(manager, servers[0], group_id)
            non_leader_host = [host for host in hosts if str(host.host_id) != str(leader_host_id)][0]

            # Prepare statement
            stmt = cql.prepare(f"INSERT INTO {table} (pk, value) VALUES (0, ?)")

            # Execute prepared INSERT once to verify results and populate the cache on the leader
            await cql.run_async(stmt, [0], host=non_leader_host)
            res = await cql.run_async(f"SELECT value FROM {table} WHERE pk = 0")
            assert len(res) == 1 and res[0].value == 0

            # Execute prepared INSERT again to verify that we use the cache on the leader
            traced_execute = cql.execute(stmt, (0,), host=non_leader_host, trace=True)
            res = await cql.run_async(f"SELECT value FROM {table} WHERE pk = 0")
            assert len(res) == 1 and res[0].value == 0
            trace = traced_execute.get_query_trace()
            for event in trace.events:
                logger.info(f"Trace event: {event.description}")
                assert "Prepared statement not found on target" not in event.description

async def test_forward_cql_cache_invalidation(manager: ManagerClient):
    """
    Test that cql forwarding works after invalidation of prepared statement cache on schema changes.
    """
    cmdline = [
        '--logger-log-level', 'cql_server=trace',
        '--experimental-features', 'strongly-consistent-tables',
    ]
    servers = await manager.servers_add(2, cmdline=cmdline, auto_rack_dc='dc1')
    (cql, hosts) = await manager.get_ready_cql(servers)

    ks_opts = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND tablets = {'initial': 1} AND consistency = 'global'")
    async with new_test_keyspace(manager, ks_opts) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, value int") as table:
            table_name = table.split('.')[-1]
            group_id = await get_table_raft_group_id(manager, ks, table_name)
            leader_host_id = await wait_for_leader(manager, servers[0], group_id)
            non_leader_host = [host for host in hosts if str(host.host_id) != str(leader_host_id)][0]

            insert_stmt = cql.prepare(f"INSERT INTO {table} (pk, value) VALUES (?, ?)")
            select_stmt = cql.prepare(f"SELECT pk, value FROM {table} WHERE pk = ?")

            await cql.run_async(insert_stmt, [1, 100])
            rows = await cql.run_async(select_stmt, [1])
            assert len(rows) == 1
            assert rows[0].value == 100

            metrics_before = await manager.metrics.query(non_leader_host.address)
            prepared_not_found_before = metrics_before.get('scylla_transport_requests_forwarded_prepared_not_found') or 0

            # Altering schema invalidates prepared statement cache
            await cql.run_async(f"ALTER TABLE {table} ADD extra_column text")
            await cql.run_async(insert_stmt, [2, 200], host=non_leader_host)
            rows = await cql.run_async(select_stmt, [2], host=non_leader_host)
            assert len(rows) == 1
            assert rows[0].value == 200

            metrics_after = await manager.metrics.query(non_leader_host.address)
            prepared_not_found_after = metrics_after.get('scylla_transport_requests_forwarded_prepared_not_found') or 0
            assert prepared_not_found_after > prepared_not_found_before

@pytest.mark.skip_mode('release', "error injections aren't enabled in release mode")
async def test_forward_cql_exception_passthrough(manager: ManagerClient):
    """
    Verify that coordinator exception returned on the target replica is correctly returned to the client.
    """
    cmdline = [
        '--logger-log-level', 'cql_server=trace',
        '--experimental-features', 'strongly-consistent-tables',
    ]
    servers = await manager.servers_add(3, cmdline=cmdline, auto_rack_dc='dc1')
    (cql, hosts) = await manager.get_ready_cql(servers)
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])

    ks_opts = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND tablets = {'initial': 1} AND consistency = 'global'")
    async with new_test_keyspace(manager, ks_opts) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, value int") as table:
            table_name = table.split('.')[-1]
            group_id = await get_table_raft_group_id(manager, ks, table_name)


            tablet_replicas = await get_tablet_replicas(manager, servers[0], ks, table_name, 0)
            assert len(tablet_replicas) == 2
            replica_host_ids = [replica[0] for replica in tablet_replicas]

            logger.info(f"Get current leader for the group {group_id}")
            if host_ids[0] in replica_host_ids:
                leader_host_id = await wait_for_leader(manager, servers[0], group_id)
            else:
                leader_host_id = await wait_for_leader(manager, servers[1], group_id)
            leader_host = [host for host in hosts if str(host.host_id) == str(leader_host_id)][0]

            logger.info(f"Get the non-leader replica for the group {group_id}")
            non_leader_replica_host_id = [host_id for host_id in replica_host_ids if str(host_id) != str(leader_host_id)][0]
            non_leader_replica_host = [host for host in hosts if str(host.host_id) == str(non_leader_replica_host_id)][0]

            logger.info(f"Get the non-replica for the group {group_id}")
            non_replica_host_id = [host_id for host_id in host_ids if str(host_id) not in replica_host_ids][0]
            non_replica_host = [host for host in hosts if str(host.host_id) == str(non_replica_host_id)][0]


            logger.info(f"Verify that timeout on the target node is returned to the client and fail metric is incremented")
            await manager.api.enable_injection(leader_host.address, "sc_modification_statement_timeout", one_shot=False)
            metrics = await manager.metrics.query(non_leader_replica_host.address)
            errors_before = metrics.get('scylla_transport_requests_forwarded_failed') or 0

            with pytest.raises(WriteTimeout):
                await cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, value) VALUES (1, 1)", retry_policy=FallthroughRetryPolicy()), host=non_leader_replica_host)

            metrics = await manager.metrics.query(non_leader_replica_host.address)
            errors_after = metrics.get('scylla_transport_requests_forwarded_failed') or 0
            assert errors_after > errors_before
            await manager.api.disable_injection(leader_host.address, "sc_modification_statement_timeout")

            # Now test that we get correct exception if the cross-node forwarding RPC times out.
            logger.info("Verify that timeout of the forwarding RPC is returned as the correct exception to the client")
            await manager.api.enable_injection(leader_host.address, "wait_before_handling_forwarded_request", one_shot=False)
            with pytest.raises(WriteTimeout):
                await cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, value) VALUES (1, 1) USING TIMEOUT 500ms", retry_policy=FallthroughRetryPolicy()), host=non_leader_replica_host)

            await manager.api.enable_injection(non_leader_replica_host.address, "wait_before_handling_forwarded_request", one_shot=False)
            with pytest.raises(ReadTimeout):
                await cql.run_async(SimpleStatement(f"SELECT * FROM {table} WHERE pk = 1 USING TIMEOUT 500ms", retry_policy=FallthroughRetryPolicy()), host=non_replica_host)

            await manager.api.message_injection(leader_host.address, "wait_before_handling_forwarded_request")
            await manager.api.message_injection(non_leader_replica_host.address, "wait_before_handling_forwarded_request")


@pytest.mark.skip_mode("release", "error injections aren't enabled in release mode")
async def test_drop_table_during_insert(manager: ManagerClient):
    """Regression test for SCYLLADB-1450: node crashes when DROP TABLE races with
    an in-flight DML on a strongly-consistent table.

    An error injection pauses INSERT inside create_operation_ctx (after obtaining
    the ERM but before acquire_server).  While it is paused the table is dropped,
    which erases the raft group.  Resuming the INSERT used to hit on_internal_error
    in acquire_server and abort the node."""

    config = {"experimental_features": ["strongly-consistent-tables"]}
    cmdline = ["--logger-log-level", "sc_groups_manager=debug"]
    server = await manager.server_add(config=config, cmdline=cmdline)
    (cql, hosts) = await manager.get_ready_cql([server])

    async with new_test_keyspace(
        manager,
        "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
        " AND tablets = {'initial': 1} AND consistency = 'global'",
    ) as ks:
        table = f"{ks}.tbl"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, v int)")

        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()

        await manager.api.enable_injection(server.ip_addr, "sc_coordinator_wait_before_acquire_server", one_shot=False)

        # Fire INSERT in the background – it will pause at the injection point.
        insert_fut = cql.run_async(f"INSERT INTO {table} (pk, v) VALUES (0, 0)")

        # Wait until the injection is actually hit.
        await manager.api.wait_for_injection_enter(server.ip_addr, "sc_coordinator_wait_before_acquire_server")

        # Drop the table while INSERT is paused.
        await cql.run_async(f"DROP TABLE {table}")

        # Wait for the raft group to be destroyed.
        await log.wait_for("schedule_raft_group_deletion.*raft server.*is destroyed", from_mark=mark, timeout=30)

        # Resume the INSERT – with the bug present, the node will abort here.
        await manager.api.message_injection(server.ip_addr, "sc_coordinator_wait_before_acquire_server")

        # The INSERT should fail gracefully, not crash the node.
        try:
            await insert_fut
        except Exception as e:
            logger.info(f"INSERT failed with expected error: {e}")

        # Verify the node is still alive.
        await manager.api.get_host_id(server.ip_addr)


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_timed_out_queries(manager: ManagerClient):
    """
    A simple test verifying that we don't get stuck for an indefinite amount
    of time while reading from or writing to a strongly consistent table.
    As soon as the deadline for a query ends, the operation should be canceled\
    and a time-out exception should be returned.

    This test focuses on a Raft operation being the potential reason for
    getting stuck. It should be aborted when we reach the deadline.
    """

    s1 = await manager.server_add(config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE)
    cql, _ = await manager.get_ready_cql([s1])

    log = await manager.server_open_log(s1.server_id)

    async def try_query_with_timeout(exception_type, stmt: str, error_injection_name: str, timeout_sec: float):
        await manager.api.enable_injection(s1.ip_addr, error_injection_name, one_shot=True)

        mark = await log.mark()
        request_timeout_ms = 100

        stmt_fut = cql.run_async(f"{stmt} USING TIMEOUT {request_timeout_ms}ms")
        await log.wait_for(error_injection_name, from_mark=mark)

        sleep_length = timeout_sec + (request_timeout_ms / 1000)
        await asyncio.sleep(sleep_length)

        await manager.api.message_injection(s1.ip_addr, error_injection_name)

        try:
            await stmt_fut
            return False
        except Exception as e:
            if isinstance(e, exception_type):
                assert f"Query timed out for {table}" in str(e)
                return True
            pytest.fail(f"Unexpected exception: {e}")

    async def try_query(exception_type, stmt: str, error_injection_name: str):
        # We cannot predict if the relevant timer will be triggered in time.
        # Even in not-really-extreme situations, it can take more time
        # than we expect. To avoid flakiness, we're going to attempt to
        # observe a timeout with an ever increasing margin of error.
        #
        # Most of the time, this will succeed during the first try,
        # so it shouldn't have a relevant impact on the length of the test.
        timeout_sec = 0.1
        while True:
            result = await try_query_with_timeout(exception_type, stmt, error_injection_name, timeout_sec)
            if result:
                break
            if timeout_sec > 60:
                pytest.fail("Reached the maximum number of attempts")
            timeout_sec *= 2

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, v int") as table:
            await cql.run_async(f"INSERT INTO {table} (pk, v) VALUES (0, 13)")


            async def try_read(error_injection_name: str):
                await try_query(ReadTimeout, f"SELECT * FROM {table} WHERE pk = 0", error_injection_name)

            async def try_write(error_injection_name: str):
                await try_query(WriteTimeout, f"INSERT INTO {table} (pk, v) VALUES (7, 23)", error_injection_name)

            # Case 1: Reads.
            read_error_injecitons = [
                "sc_coordinator_wait_before_acquire_server",
                "sc_coordinator_wait_before_query_read_barrier"
            ]
            for error_injection_name in read_error_injecitons:
                await try_read(error_injection_name)

            # Sanity check: Nothing broke and we can still read from the table.
            res = await cql.run_async(f"SELECT * FROM {table} WHERE pk = 0")
            assert res[0].v == 13

            # Case 2: Writes.
            write_error_injections = [
                "sc_coordinator_wait_before_acquire_server",
                "sc_coordinator_wait_before_begin_mutate",
                "sc_coordinator_wait_before_add_entry"
            ]
            for error_injection_name in write_error_injections:
                await try_write(error_injection_name)

            # Sanity check: Nothing broke and we can still write to the table.
            await cql.run_async(f"INSERT INTO {table} (pk, v) VALUES (17, 7)")

    # Case 3: Waiting for the leader during a write.

    # To trigger the timeout we want, we need to make sure that
    # groups_manager::begin_mutate will want to return need_wait_for_leader,
    # and that it will never succeed. This will do the job.
    await manager.api.enable_injection(s1.ip_addr, "sc_leader_info_updater_wait_before_setting_leader_info", one_shot=True)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, v int") as table:
            with pytest.raises(WriteTimeout, match=f"Query timed out for {table}"):
                await cql.run_async(f"INSERT INTO {table} (pk, v) VALUES (11, 13) USING TIMEOUT 100ms")


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_queries_while_dropping_table(manager: ManagerClient):
    """Verify that in-flight reads and writes are promptly aborted when
    a strongly consistent table is dropped.

    Setup: 2 nodes, RF=2, 1 tablet (raft quorum = 2).

    We pause a read (before read_barrier) and a write (before add_entry)
    on the leader, then drop the table. The follower destroys its raft group
    immediately (no in-flight ops). On the leader, the raft server is aborted
    as part of group deletion (SCYLLADB-2080 fix), causing the paused
    operations to fail with raft::stopped_error which the coordinator converts
    to "no such column family" / "unconfigured table".

    We also verify that new reads/writes after the drop are rejected
    immediately.
    """

    servers = await manager.servers_add(2, config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE, auto_rack_dc='my_dc')
    cql, hosts = await manager.get_ready_cql(servers)
    host_ids = [await manager.get_host_id(s.server_id) for s in servers]

    async with new_test_keyspace(manager,
        "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}"
        " AND tablets = {'initial': 1} AND consistency = 'global'"
    ) as ks:
        table_name = "tbl"
        table = f"{ks}.{table_name}"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, v int)")
        await cql.run_async(f"INSERT INTO {table} (pk, v) VALUES (0, 13)")

        # Identify the leader node for this tablet's raft group.
        group_id = await get_table_raft_group_id(manager, ks, table_name)
        leader_host_id = await wait_for_leader(manager, servers[0], group_id)

        leader_idx = next(i for i, hid in enumerate(host_ids) if str(hid) == leader_host_id)
        leader_server = servers[leader_idx]
        leader_host = hosts[leader_idx]
        follower_server = servers[1 - leader_idx]

        leader_log = await manager.server_open_log(leader_server.server_id)
        follower_log = await manager.server_open_log(follower_server.server_id)

        mark_leader = await leader_log.mark()

        # Pause both a read and a write on the leader.
        await asyncio.gather(
            manager.api.enable_injection(leader_server.ip_addr,
                "sc_coordinator_wait_before_query_read_barrier", one_shot=True),
            manager.api.enable_injection(leader_server.ip_addr,
                "sc_coordinator_wait_before_add_entry", one_shot=True))

        read_fut = asyncio.ensure_future(
            cql.run_async(f"SELECT * FROM {table} WHERE pk = 0", host=leader_host))
        write_fut = asyncio.ensure_future(
            cql.run_async(f"INSERT INTO {table} (pk, v) VALUES (7, 17)", host=leader_host))

        # Wait for both to hit their injection points.
        await asyncio.gather(
            leader_log.wait_for("sc_coordinator_wait_before_query_read_barrier: waiting",
                from_mark=mark_leader, timeout=30),
            leader_log.wait_for("sc_coordinator_wait_before_add_entry: waiting",
                from_mark=mark_leader, timeout=30))

        mark_leader = await leader_log.mark()
        mark_follower = await follower_log.mark()

        # Drop the table.
        await cql.run_async(f"DROP TABLE {table}")

        # Sanity check: new queries are rejected immediately.
        with pytest.raises(InvalidRequest, match="unconfigured table"):
            await cql.run_async(f"SELECT * FROM {table} WHERE pk = 0")
        with pytest.raises(InvalidRequest, match="unconfigured table"):
            await cql.run_async(f"INSERT INTO {table} (pk, v) VALUES (13, 11)")

        # Confirm the follower destroyed its raft group.
        await follower_log.wait_for("schedule_raft_group_deletion.*raft server.*is destroyed",
            from_mark=mark_follower, timeout=30)

        # Resume both operations.
        await asyncio.gather(
            manager.api.message_injection(leader_server.ip_addr,
                "sc_coordinator_wait_before_query_read_barrier"),
            manager.api.message_injection(leader_server.ip_addr,
                "sc_coordinator_wait_before_add_entry"))

        # Both should fail with "no such column family" / "unconfigured table".
        # The raft server is aborted as part of group deletion, causing
        # stopped_error which the coordinator converts to no_such_column_family.
        # Use a timeout to detect the old buggy behavior (write stuck forever).
        try:
            await asyncio.wait_for(asyncio.shield(write_fut), timeout=15)
        except asyncio.TimeoutError:
            cql.cluster.shutdown()
            for s in servers:
                await manager.server_stop(s.server_id, convict=True)
            pytest.fail("SCYLLADB-2080: write is stuck waiting for quorum that can never "
                        "be reached because the follower's raft group was already destroyed. "
                        "The leader should abort the raft server to unblock the write.")
        except InvalidRequest as e:
            assert re.search(r"[Uu]nconfigured table|[Nn]o such column family", str(e)), \
                f"Expected 'unconfigured table' or 'no such column family', got: {e}"

        with pytest.raises((InvalidRequest, Exception), match="[Uu]nconfigured table|[Cc]an't find a column family|[Nn]o such column family"):
            await read_fut


@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
@pytest.mark.parametrize("target", ["leader", "follower"])
async def test_queries_when_shutting_down(manager: ManagerClient, target: str):
    """
    Verify that Scylla has be stopped despite hanging opeartions
    to strongly consistent tables. The test drops AppendEntries
    requests on the followers to simulate a hanging call to
    raft_server::add_entry.

    The read path is more difficult to verify and it's under active
    work, so we skip it. It uses the same abortion mechanisms
    as the write path, though.

    We test writes sent to both the coordinator and a follower.
    """

    smp = 2
    config = DEFAULT_CONFIG | {"request_timeout_on_shutdown_in_seconds": 1}
    cmdline = DEFAULT_CMDLINE + [
        f"--smp={smp}",
        "--logger-log-level", "cql_server=debug",
        "--logger-log-level", "sc_coordinator=trace",
        "--logger-log-level", "raft_group_registry=debug"
    ]
    leader = await manager.server_add(config=config, cmdline=cmdline, property_file={"dc": "dc1", "rack": "r1"})

    follower_config = config | {"error_injections_at_startup": ["avoid_being_raft_leader"]}
    followers = await manager.servers_add(2, config=follower_config, cmdline=cmdline, property_file=[
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"}
    ])

    cql, hosts = await manager.get_ready_cql([leader, *followers])

    leader_host = hosts[0]
    nonleader_host = hosts[1]

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, v int") as table:
            _, table_name = table.split(".")

            logger.info(f"Select raft group id for the tablet of {ks}.{table_name}")
            group_id = await get_table_raft_group_id(manager, ks, table_name)

            await asyncio.gather(*[asyncio.create_task(manager.api.enable_injection(
                follower.ip_addr,
                "raft_drop_incoming_append_entries_for_specified_group",
                one_shot=False,
                parameters={"value": group_id}
            )) for follower in followers])

            logs = [await manager.server_open_log(follower.server_id) for follower in followers]
            marks = [await log.mark() for log in logs]

            target_host = None
            if target == "leader":
                target_host = leader_host
            elif target == "follower":
                target_host = nonleader_host
            write_fut = cql.run_async(f"INSERT INTO {table} (pk, v) VALUES (0, 13)", host=target_host)

            error_log = rf"Dropping append request .* for group {group_id}"
            await gather_safely(*[
                asyncio.create_task(logs[0].wait_for(error_log, from_mark=marks[0])),
                asyncio.create_task(logs[1].wait_for(error_log, from_mark=marks[1]))
            ])

            await manager.server_stop_gracefully(leader.server_id)

            try:
                await asyncio.wait_for(write_fut, timeout=60)
                logger.debug(f"Write awaited successfully")
            except Exception as e:
                # Acceptable. It doesn't matter what the result is.
                logger.debug(f"Exception when awaiting write: {e}")

            # We need to disable the injections to be able to drop the keyspace and table.
            await asyncio.gather(*[asyncio.create_task(manager.api.disable_injection(
                follower.ip_addr,
                "raft_drop_incoming_append_entries_for_specified_group"
            )) for follower in followers])
            await asyncio.gather(*[asyncio.create_task(manager.api.disable_injection(
                follower.ip_addr,
                "avoid_being_raft_leader"
            )) for follower in followers])


@pytest.mark.skip_bug(
    link="https://scylladb.atlassian.net/browse/SCYLLADB-1056",
    reason="Speed up abortion of applier fiber in raft::server_impl::abort",
)
@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_abort_state_machine_apply_after_dropping_table(manager: ManagerClient):
    """
    This test verifies that ongoing executions of state_machine::apply are
    aborted when their corresponding Raft group is being removed. We test
    that by dropping the table, but it should also correspond to other cases
    like tablet migration.

    For a similar scenario during a node shutdown, see test_abort_state_machine_apply_during_shutdown.
    """

    cmdline = DEFAULT_CMDLINE + ["--logger-log-level", "sc_state_machine=debug:raft=debug"]
    leader_server = await manager.server_add(config=DEFAULT_CONFIG, cmdline=cmdline,
                                             property_file={"dc": "dc1", "rack": "rack1"})

    # We want to prevent target_server from becoming the leader of either group0
    # or the strongly consistent Raft group so it might not have the latest
    # schema version and is forced to perform a read barrier.
    config = DEFAULT_CONFIG | {"error_injections_at_startup": ["avoid_being_raft_leader"]}
    target_server = await manager.server_add(config=config, cmdline=cmdline, property_file={"dc": "dc1", "rack": "rack2"})

    cql, [leader_host, _] = await manager.get_ready_cql([leader_server, target_server])

    leader_host_id, target_host_id = await gather_safely(*[
        manager.get_host_id(leader_server.server_id),
        manager.get_host_id(target_server.server_id)
    ])

    wait_before_apply_injection = "strong_consistency_state_machine_wait_before_apply"

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        table_name = "my_table"
        table = f"{ks}.{table_name}"

        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, v int)")

        group_id = await get_table_raft_group_id(manager, ks, table_name)
        leader_host_id = await wait_for_leader(manager, leader_server, group_id)
        assert leader_host_id != target_host_id

        await gather_safely(*[
            manager.api.enable_injection(target_server.ip_addr, wait_before_apply_injection, one_shot=True),
            manager.api.enable_injection(target_server.ip_addr, "sc_state_machine_return_empty_schema", one_shot=True)
        ])

        log = await manager.server_open_log(target_server.server_id)
        mark = await log.mark()

        # We won't wait for the follower to apply the state, so we can await this right away.
        await cql.run_async(f"INSERT INTO {table} (pk, v) VALUES (0, 13)", host=leader_host)

        await log.wait_for(wait_before_apply_injection, from_mark=mark)
        mark = await log.mark()

        await cql.run_async(f"DROP TABLE {table}")
        # Wait until the Raft group has started being removed.
        await log.wait_for(rf"schedule_raft_group_deletion\(\): starting aborting raft server for group id {group_id}", from_mark=mark)
        mark = await log.mark()

        # At this point, the Raft server should already be getting aborted,
        # so we can resume state_machine::apply.
        await manager.api.message_injection(target_server.ip_addr, wait_before_apply_injection)
        # Verify that state_machine::apply was really aborted.
        await log.wait_for(rf"apply\(\): execution for tablet \S+, group_id={group_id} aborted", from_mark=mark)


@pytest.mark.skip_bug(
    link="https://scylladb.atlassian.net/browse/SCYLLADB-1056",
    reason="Speed up abortion of applier fiber in raft::server_impl::abort",
)
@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_abort_state_machine_apply_during_shutdown(manager: ManagerClient):
    """
    This test verifies that ongoing executions of state_machine::apply are
    aborted when a node is shutting down.

    For a similar scenario after dropping a table, see test_abort_state_machine_apply_after_dropping_table.
    """

    cmdline = DEFAULT_CMDLINE + ["--logger-log-level", "sc_state_machine=debug:raft=debug"]
    leader_server = await manager.server_add(config=DEFAULT_CONFIG, cmdline=cmdline,
                                             property_file={"dc": "dc1", "rack": "rack1"})

    # We want to prevent target_server from becoming the leader of either group0
    # or the strongly consistent Raft group so it might not have the latest
    # schema version and is forced to perform a read barrier.
    config = DEFAULT_CONFIG | {"error_injections_at_startup": ["avoid_being_raft_leader"]}
    target_server = await manager.server_add(config=config, cmdline=cmdline, property_file={"dc": "dc1", "rack": "rack2"})

    cql, [leader_host, target_host] = await manager.get_ready_cql([leader_server, target_server])

    leader_host_id, target_host_id = await gather_safely(*[
        manager.get_host_id(leader_server.server_id),
        manager.get_host_id(target_server.server_id)
    ])

    wait_before_apply_injection = "strong_consistency_state_machine_wait_before_apply"

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        table_name = "my_table"
        table = f"{ks}.{table_name}"

        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, v int)")

        group_id = await get_table_raft_group_id(manager, ks, table_name)
        leader_host_id = await wait_for_leader(manager, leader_server, group_id)
        assert leader_host_id != target_host_id

        await gather_safely(*[
            manager.api.enable_injection(target_server.ip_addr, wait_before_apply_injection, one_shot=True),
            manager.api.enable_injection(target_server.ip_addr, "sc_state_machine_return_empty_schema", one_shot=True)
        ])

        log = await manager.server_open_log(target_server.server_id)
        mark = await log.mark()

        # We won't wait for the follower to apply the state, so we can await this right away.
        await cql.run_async(f"INSERT INTO {table} (pk, v) VALUES (0, 13)", host=leader_host)

        await log.wait_for(wait_before_apply_injection, from_mark=mark)
        mark = await log.mark()

        stop_task = asyncio.create_task(manager.server_stop_gracefully(target_server.server_id))
        # Wait until the Raft group has started being removed.
        await log.wait_for(rf"schedule_raft_group_deletion\(\): starting aborting raft server for group id {group_id}", from_mark=mark)
        mark = await log.mark()

        # At this point, the Raft server should already be getting aborted,
        # so we can resume state_machine::apply.
        await manager.api.message_injection(target_server.ip_addr, wait_before_apply_injection)
        # Verify that state_machine::apply was really aborted.
        await log.wait_for(rf"apply\(\): execution for tablet \S+, group_id={group_id} aborted", from_mark=mark)

        # The test framework should verify that we haven't observed any errors
        # during the stopping procedure.
        await stop_task

        await manager.server_start(target_server.server_id)
        cql, [target_host] = await manager.get_ready_cql([target_server])

        rows = await cql.run_async(f"SELECT * FROM {table} WHERE pk = 0", host=target_host)
        assert len(rows) == 1
        assert rows[0].v == 13

async def test_leader_cache_eliminates_redirect(manager: ManagerClient):
    """
    Verify that after a non-replica node learns the leader location via a redirect,
    subsequent write requests from that node go directly to the leader without a redirect.

    Uses 4 nodes in 2 racks (2 per rack) with RF=2 and 1 tablet.
    Tablet-aware replication places one replica per rack.
    The non-replica node in the non-leader rack always picks the same-rack
    (non-leader) replica as closest, causing a redirect to the leader.
    After the first write populates the cache, subsequent writes skip the redirect.

    We verify this via the scylla_transport_requests_forwarded_redirected metric
    on the non-replica node.
    """
    cmdline = [
        '--logger-log-level', 'cql_server=trace',
        '--experimental-features', 'strongly-consistent-tables',
    ]
    # 2 racks with 2 nodes each
    property_file = [
        {"dc": "dc1", "rack": "rack1"},
        {"dc": "dc1", "rack": "rack1"},
        {"dc": "dc1", "rack": "rack2"},
        {"dc": "dc1", "rack": "rack2"},
    ]
    servers = await manager.servers_add(4, cmdline=cmdline, property_file=property_file)
    (cql, hosts) = await manager.get_ready_cql(servers)
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])

    def host_by_host_id(host_id):
        for hid, host in zip(host_ids, hosts):
            if hid == host_id:
                return host
        raise RuntimeError(f"Can't find host for host_id {host_id}")

    ks_opts = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}"
               " AND tablets = {'initial': 1} AND consistency = 'global'")
    async with new_test_keyspace(manager, ks_opts) as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, value int") as table:
            table_name = table.split('.')[-1]
            group_id = await get_table_raft_group_id(manager, ks, table_name)

            tablet_replicas = await get_tablet_replicas(manager, servers[0], ks, table_name, 0)
            assert len(tablet_replicas) == 2
            replica_host_ids = [replica[0] for replica in tablet_replicas]

            if host_ids[0] in replica_host_ids:
                leader_host_id = await wait_for_leader(manager, servers[0], group_id)
            else:
                leader_host_id = await wait_for_leader(manager, servers[1], group_id)
            # Find the rack of the leader
            leader_server = next(s for hid, s in zip(host_ids, servers) if str(hid) == leader_host_id)
            leader_rack = leader_server.rack

            # Pick the non-replica in the non-leader rack.
            # This node's closest replica is the same-rack (non-leader) replica,
            # so writes always cause a redirect on the first attempt.
            non_replica_host_id = None
            for hid, s in zip(host_ids, servers):
                if str(hid) not in replica_host_ids and s.rack != leader_rack:
                    non_replica_host_id = hid
                    break
            assert non_replica_host_id is not None, "Could not find non-replica in non-leader rack"
            non_replica_server = next(s for hid, s in zip(host_ids, servers) if hid == non_replica_host_id)
            non_replica_host = host_by_host_id(non_replica_host_id)

            logger.info(f"Non-replica node: {non_replica_host} (rack {non_replica_server.rack}), leader: {leader_host_id} (rack {leader_rack})")

            # Warmup phase: run enough requests to populate the leader cache
            # on all shards of the non-replica node. The driver may distribute
            # requests across multiple connections (shards), so we need to
            # warm them all up. 20 requests should be enough to cover all shards.
            for i in range(20):
                await cql.run_async(f"INSERT INTO {table} (pk, value) VALUES ({i}, {i})", host=non_replica_host)

            # Measure redirect counter after warmup (all shards should be warm).
            metrics = await manager.metrics.query(non_replica_host.address)
            redirects_before = metrics.get('scylla_transport_requests_forwarded_redirected') or 0

            # Run another batch of requests; all should use the cached leader
            # and NOT cause any redirects.
            num_requests = 20
            for i in range(20, 20 + num_requests):
                await cql.run_async(f"INSERT INTO {table} (pk, value) VALUES ({i}, {i})", host=non_replica_host)

            metrics = await manager.metrics.query(non_replica_host.address)
            redirects_after = metrics.get('scylla_transport_requests_forwarded_redirected') or 0

            new_redirects = redirects_after - redirects_before
            logger.info(f"Redirects before: {redirects_before}, after: {redirects_after}, new: {new_redirects}")
            assert new_redirects == 0, \
                f"Expected no new redirects after cache warmup, but got {new_redirects}"

            # Verify data correctness
            rows = await cql.run_async(f"SELECT * FROM {table} WHERE pk = 25", host=non_replica_host)
            assert len(rows) == 1
            assert rows[0].value == 25

@pytest.mark.asyncio
async def test_read_forwarding(manager: ManagerClient):
    """
    Verify read forwarding behavior for strongly consistent tables:
    - CL=QUORUM reads (linearizable) are forwarded to the raft leader
    - CL=ONE reads (non-linearizable) work on any replica without forwarding
    - Linearizability: a CL=QUORUM read after a write always sees the write
    Use a 4-node cluster with RF=3 to have:
    - a leader replica
    - 2 follower replicas, one of which is not required for quorum
    - a non-replica node
    """

    logger.info("Bootstrapping cluster")
    servers = await manager.servers_add(4, config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE, auto_rack_dc='my_dc')
    (cql, hosts) = await manager.get_ready_cql(servers)
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])

    def host_by_host_id(host_id):
        for hid, host in zip(host_ids, hosts):
            if hid == host_id:
                return host
        raise RuntimeError(f"Can't find host for host_id {host_id}")

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, c int") as table:
            table_name = table.split('.')[-1]
            group_id = await get_table_raft_group_id(manager, ks, table_name)
            tablet_replicas = await get_tablet_replicas(manager, servers[0], ks, table_name, 0)
            replica_host_ids = [replica[0] for replica in tablet_replicas]

            for i in range(4):
                if host_ids[i] in replica_host_ids:
                    leader_host_id = await wait_for_leader(manager, servers[i], group_id)
                    break
            leader_host = host_by_host_id(leader_host_id)

            non_leader_replica_host_id = [hid for hid in replica_host_ids if str(hid) != str(leader_host_id)][0]
            non_leader_replica_host = host_by_host_id(non_leader_replica_host_id)
            non_replica_host_id = [hid for hid in host_ids if str(hid) not in [str(r) for r in replica_host_ids]][0]
            non_replica_host = host_by_host_id(non_replica_host_id)

            async def get_metric(host, name, labels={}):
                metrics = await manager.metrics.query(host.address)
                return metrics.get(name, labels) or 0

            async def check_read(cl, send_to, expect_fwd, read_type=None, sc_metric_host=None):
                """Execute a read and verify forwarding behavior and coordinator metrics."""
                label = f"CL={ConsistencyLevel.value_to_name[cl]} on {send_to.address}"
                logger.info(f"Testing read: {label}")

                stmt = SimpleStatement(f"SELECT * FROM {table} WHERE pk = 1", consistency_level=cl)

                fwd_before = await get_metric(send_to, 'scylla_transport_requests_forwarded_successfully')
                sc_before = await get_metric(sc_metric_host, f'scylla_strong_consistency_coordinator_read_latency_count', {'read_type': read_type}) if read_type else None

                rows = await cql.run_async(stmt, host=send_to)
                assert len(rows) == 1
                assert rows[0].c == 100

                fwd_after = await get_metric(send_to, 'scylla_transport_requests_forwarded_successfully')
                if expect_fwd:
                    assert fwd_after > fwd_before, f"{label}: expected forwarding"
                else:
                    assert fwd_after == fwd_before, f"{label}: unexpected forwarding"

                if read_type:
                    sc_after = await get_metric(sc_metric_host, f'scylla_strong_consistency_coordinator_read_latency_count', {'read_type': read_type})
                    assert sc_after > sc_before, f"{label}: expected {read_type} scylla_strong_consistency_coordinator_read_latency_count to increment on {sc_metric_host.address}"

            await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES (1, 100)", host=leader_host)

            # CL=QUORUM: linearizable, forwarded to leader
            await check_read(ConsistencyLevel.QUORUM, leader_host, expect_fwd=False,
                             read_type='linearizable', sc_metric_host=leader_host)
            await check_read(ConsistencyLevel.QUORUM, non_leader_replica_host, expect_fwd=True,
                             read_type='linearizable', sc_metric_host=leader_host)
            await check_read(ConsistencyLevel.QUORUM, non_replica_host, expect_fwd=True,
                             read_type='linearizable', sc_metric_host=leader_host)

            # CL=ONE: no forwarding, executed on the local replica
            await read_barrier(manager.api, non_leader_replica_host.address, group_id=group_id, timeout=30)
            await check_read(ConsistencyLevel.ONE, non_leader_replica_host, expect_fwd=False,
                             read_type='non_linearizable', sc_metric_host=non_leader_replica_host)

            # --- Linearizability: write then CL=QUORUM read from follower ---
            logger.info("Testing linearizability: write + CL=QUORUM read from follower (10 iterations)")
            for i in range(10):
                await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES ({100 + i}, {i * 10})")
                read_stmt = SimpleStatement(f"SELECT * FROM {table} WHERE pk = {100 + i}", consistency_level=ConsistencyLevel.QUORUM)
                rows = await cql.run_async(read_stmt, host=non_leader_replica_host)
                assert len(rows) == 1, f"Expected 1 row for pk={100 + i}, got {len(rows)}"
                assert rows[0].c == i * 10, f"Linearizability violation: pk={100 + i}, expected c={i * 10}, got c={rows[0].c}"
@pytest.mark.asyncio
async def test_data_survives_crash(manager: ManagerClient):
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
        await reconnect_driver(manager)
        cql = manager.get_cql()

        for pk in range(5):
            rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = {pk};")
            assert len(rows) == 1, f"Expected 1 row for pk={pk}, got {len(rows)}"
            assert rows[0].c == pk * 10, f"pk={pk}: expected c={pk * 10}, got c={rows[0].c}"

        # Verify that the snapshot index was advanced during replay.
        # After commitlog replay, store_snapshot_index should have bumped
        # snapshot.idx to commit_idx for the tablet's raft group.
        table_id = await manager.get_table_id(ks.replace('"', ''), "test")
        tablet_rows = await cql.run_async(f"SELECT raft_group_id FROM system.tablets WHERE table_id = {table_id}")
        assert len(tablet_rows) == 1
        group_id = tablet_rows[0].raft_group_id
        snp_rows = await cql.run_async(f"SELECT idx FROM system.raft_groups_snapshots WHERE shard = 0 AND group_id = {group_id}")
        assert len(snp_rows) == 1, f"Expected snapshot row for group {group_id}"
        assert snp_rows[0].idx > 0, f"Expected snapshot idx > 0 after replay, got {snp_rows[0].idx}"

    await manager.server_stop_gracefully(server.server_id)


@pytest.mark.asyncio
async def test_schema_upgrade_during_replay(manager: ManagerClient):
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
        await reconnect_driver(manager)
        cql = manager.get_cql()

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
async def test_double_crash_recovery(manager: ManagerClient):
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
        await reconnect_driver(manager)
        cql = manager.get_cql()

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
        await reconnect_driver(manager)
        cql = manager.get_cql()

        # Verify all data (both phases) survived
        for pk in range(20):
            rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = {pk};")
            assert len(rows) == 1, f"After 2nd crash: expected 1 row for pk={pk}, got {len(rows)}"
            assert rows[0].c == pk * 10

    await manager.server_stop_gracefully(server.server_id)


@pytest.mark.asyncio
async def test_crash_with_multiple_commitlog_segments(manager: ManagerClient):
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
        await reconnect_driver(manager)
        cql = manager.get_cql()

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
async def test_crash_recovery_multi_tablet(manager: ManagerClient):
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
        await reconnect_driver(manager)
        cql = manager.get_cql()

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
async def test_crash_recovery_after_flush(manager: ManagerClient):
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
        await reconnect_driver(manager)
        cql = manager.get_cql()

        # Verify all data: flushed (phase 1) + replayed from commitlog (phase 2)
        for pk in range(20):
            rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = {pk};")
            assert len(rows) == 1, f"Expected 1 row for pk={pk}, got {len(rows)}"
            assert rows[0].c == pk * 10, f"pk={pk}: expected c={pk * 10}, got c={rows[0].c}"

    await manager.server_stop_gracefully(server.server_id)
