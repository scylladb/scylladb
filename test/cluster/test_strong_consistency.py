#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from test.pylib.manager_client import ManagerClient
from test.pylib.util import gather_safely, wait_for
from test.cluster.util import new_test_keyspace, new_test_table, reconnect_driver
from test.pylib.internal_types import ServerInfo
from cassandra.protocol import InvalidRequest
from test.pylib.tablets import get_all_tablet_replicas

import pytest
import logging
import time
import uuid
import random


logger = logging.getLogger(__name__)


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

@pytest.mark.asyncio
async def test_basic_write_read(manager: ManagerClient):

    logger.info("Bootstrapping cluster")
    config = {
        'experimental_features': ['strongly-consistent-tables']
    }
    cmdline = [
        '--logger-log-level', 'sc_groups_manager=debug',
        '--logger-log-level', 'sc_coordinator=debug'
    ]
    servers = await manager.servers_add(3, config=config, cmdline=cmdline, auto_rack_dc='my_dc')
    (cql, hosts) = await manager.get_ready_cql(servers)

    logger.info("Load host_id-s for servers")
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])

    def host_by_host_id(host_id):
        for hid, host in zip(host_ids, hosts):
            if hid == host_id:
                return host
        raise RuntimeError(f"Can't find host for host_id {host_id}")

    logger.info("Creating a strongly-consistent keyspace")
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1} AND consistency = 'local'") as ks:
        logger.info("Creating a table")
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        logger.info("Select raft group id for the tablet")
        table_id = await manager.get_table_id(ks, 'test')
        rows = await cql.run_async(f"SELECT raft_group_id FROM system.tablets where table_id = {table_id}")
        group_id = str(rows[0].raft_group_id)

        logger.info(f"Get current leader for the group {group_id}")
        leader_host_id = await wait_for_leader(manager, servers[0], group_id)
        leader_host = host_by_host_id(leader_host_id)
        non_leader_host = next((host_by_host_id(hid) for hid in host_ids if hid != leader_host_id), None)
        assert non_leader_host is not None

        logger.info(f"Run INSERT statement on leader {leader_host}")
        await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES (10, 20)", host=leader_host)
        logger.info(f"Run INSERT statement on non-leader {non_leader_host}")
        with pytest.raises(InvalidRequest, match="Strongly consistent writes can be executed only on the leader node"):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES (10, 30)", host=non_leader_host)

        logger.info(f"Run SELECT statement on leader {leader_host}")
        rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = 10;", host=leader_host)
        assert len(rows) == 1
        row = rows[0]
        assert row.pk == 10
        assert row.c == 20

        logger.info(f"Run SELECT statement on non-leader {non_leader_host}")
        rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = 10;", host=non_leader_host)
        assert len(rows) == 1
        row = rows[0]
        assert row.pk == 10
        assert row.c == 20

        # Check that we can restart a server with an active tablets raft group
        await manager.server_restart(servers[2].server_id)

    # To check that the servers can be stopped gracefully. By default the test runner just kills them.
    await gather_safely(*[manager.server_stop_gracefully(s.server_id) for s in servers])

@pytest.mark.asyncio
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
    config = {
        'experimental_features': ['strongly-consistent-tables']
    }
    cmdline = [
        '--smp=4',
        '--logger-log-level', 'sc_groups_manager=debug',
        '--logger-log-level', 'sc_coordinator=debug'
    ]
    servers = await manager.servers_add(3, config=config, cmdline=cmdline, auto_rack_dc='my_dc')
    (cql, hosts) = await manager.get_ready_cql(servers)

    logger.info("Creating a strongly-consistent keyspace with 4 tablets")
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 4} AND consistency = 'local'") as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, c int") as table:
            successful_writes = 0
            for host in hosts:
                for j in range(50):
                    try:
                        await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES ({j}, {j})", host=host)
                        successful_writes += 1
                    except InvalidRequest:
                        # Leader might be different for this specific pk
                        pass
            assert successful_writes == 50, f"Expected 50 successful writes, got {successful_writes}"

            # Read all data
            for j in range(50):
                rows = await cql.run_async(f"SELECT * FROM {table} WHERE pk = {j};")
                assert len(rows) == 1, f"Expected 1 row for pk={j}, got {len(rows)}"
                row = rows[0]
                assert row.c == j, f"Data integrity check failed for pk={j}: expected c={j}, got c={row.c}"

    await gather_safely(*[manager.server_stop_gracefully(s.server_id) for s in servers])

@pytest.mark.asyncio
async def test_sc_multishard_metadata_reads(manager: ManagerClient):
    """
    Verify that multi-shard reads of raft metadata for strongly-consistent tables work correctly.
    """
    config = {
        'experimental_features': ['strongly-consistent-tables']
    }
    cmdline = [
        '--smp=4',
        '--logger-log-level', 'sc_groups_manager=debug',
        '--logger-log-level', 'sc_coordinator=debug',
        '--logger-log-level', 'fixed_shard=trace',
    ]
    server = await manager.server_add(config=config, cmdline=cmdline)
    (cql, hosts) = await manager.get_ready_cql([server])

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 8} AND consistency = 'local'") as ks:
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

@pytest.mark.asyncio
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
    config = {
        'experimental_features': ['strongly-consistent-tables']
    }
    cmdline = [
        '--smp=2',
        '--logger-log-level', 'sc_groups_manager=debug',
        '--logger-log-level', 'sc_coordinator=debug',
        '--logger-log-level', 'fixed_shard=trace',
    ]
    server = await manager.server_add(config=config, cmdline=cmdline)
    (cql, hosts) = await manager.get_ready_cql([server])

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 2} AND consistency = 'local'") as ks:
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


@pytest.mark.asyncio
async def test_sc_persistence_with_compaction(manager: ManagerClient):
    """
    Verify that compaction of system.raft_groups works correctly.

    Regular (non-reshard) compaction does not use the custom partitioner/sharder
    directly, bit it does compact SSTables written with them, so in this test we
    verify that after compaction the raft metadata is still readable and correct.
    """
    config = {
        'experimental_features': ['strongly-consistent-tables']
    }
    cmdline = [
        '--logger-log-level', 'sc_groups_manager=debug',
        '--logger-log-level', 'sc_coordinator=debug',
        '--logger-log-level', 'fixed_shard=trace',
    ]
    server = await manager.server_add(config=config, cmdline=cmdline)
    (cql, hosts) = await manager.get_ready_cql([server])

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 4} AND consistency = 'local'") as ks:
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


@pytest.mark.asyncio
async def test_sc_persistence_after_crash(manager: ManagerClient):
    """
    Verify that metadata for strongly-consistent tables is recovered
    after a non-graceful stop (crash simulation).
    """
    config = {
        'experimental_features': ['strongly-consistent-tables']
    }
    cmdline = [
        '--logger-log-level', 'sc_groups_manager=debug',
        '--logger-log-level', 'sc_coordinator=debug',
        '--logger-log-level', 'fixed_shard=trace',
    ]
    server = await manager.server_add(config=config, cmdline=cmdline)
    (cql, hosts) = await manager.get_ready_cql([server])

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 4} AND consistency = 'local'") as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, c int") as table:
            # Write some rows to trigger raft table updates
            for pk in range(20):
                await cql.run_async(f"INSERT INTO {table} (pk, c) VALUES ({pk}, {pk})")

            # Collect raft state and log entry counts before crash
            state_before_crash = await collect_all_raft_state(cql, hosts[0])

            await manager.server_stop(server.server_id)

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
