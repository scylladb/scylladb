#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import os
import glob
import json
import pytest
import asyncio
import logging
import subprocess
from collections import defaultdict
from cassandra.query import SimpleStatement, ConsistencyLevel

from test.pylib.tablets import get_tablet_count, get_all_tablet_replicas
from test.pylib.rest_client import HTTPError, read_barrier
from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.cluster.util import new_test_keyspace, reconnect_driver

logger = logging.getLogger(__name__)

def calculate_powof2_tokens(num_nodes: int, tokens_per_node: int) -> dict[int, list[int]]:
    exp = 0
    while 2**exp < num_nodes * tokens_per_node:
        exp += 1
    n = 2**exp
    new_tokens_combined = [int(i * 2**64 // n - 2**63 - 1) for i in range(1, n+1)]
    calculated_new_tokens = defaultdict(list)
    new_server_id = 1
    for i, t in enumerate(new_tokens_combined):
        calculated_new_tokens[new_server_id  + (i % num_nodes)].append(t)
    for s, tokens in calculated_new_tokens.items():
        calculated_new_tokens[s] = sorted(tokens)
    logger.debug(f"{calculated_new_tokens=}")
    return calculated_new_tokens


def get_sstable_token_ranges(scylla_path: str, scylla_yaml: str, sstable_data_files: list[str]) -> list[tuple[int, int]]:
    """Run 'scylla sstable dump-scylla-metadata' and return a list of (first_token, last_token) per SSTable.

    Extracts the first and last token from the sharding metadata in the Scylla.db component.

    Args:
        scylla_path: Path to the scylla executable.
        scylla_yaml: Path to the scylla.yaml config file.
        sstable_data_files: List of SSTable Data.db file paths.

    Returns:
        A list of (first_token, last_token) tuples, one per SSTable.
    """
    try:
        result = subprocess.check_output(
            [scylla_path, "sstable", "dump-scylla-metadata",
             "--scylla-yaml-file", scylla_yaml,
             "--sstables"] + sstable_data_files,
            stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        logger.error(f"scylla sstable dump-scylla-metadata failed with exit code {e.returncode}")
        logger.error(f"stdout: {e.output.decode('utf-8', 'ignore')}")
        logger.error(f"stderr: {e.stderr.decode('utf-8', 'ignore')}")
        raise
    metadata = json.loads(result.decode('utf-8', 'ignore'))
    ranges = []
    for sstable_name, info in metadata["sstables"].items():
        sharding = info["sharding"]
        assert not sharding[0]["left"]["exclusive"], f"SSTable {sstable_name}: Expected the left bound of the first sharding range to be inclusive"
        assert not sharding[-1]["right"]["exclusive"], f"SSTable {sstable_name}: Expected the right bound of the last sharding range to be inclusive"
        first_token = int(sharding[0]["left"]["token"])
        last_token = int(sharding[-1]["right"]["token"])
        ranges.append((first_token, last_token))
    return ranges


def sstable_range_within_vnode(first_token: int, last_token: int, vnode_boundaries: list[int]) -> bool:
    """Check whether an SSTable's token range falls entirely within a single vnode range.

    Args:
        first_token: The first token in the SSTable.
        last_token: The last token in the SSTable.
        vnode_boundaries: Sorted list of vnode token boundaries.

    Returns:
        True if both first_token and last_token fall within the same vnode range.
    """
    def find_owning_vnode(token: int) -> int:
        """Return the index of the vnode that owns this token."""
        for i, boundary in enumerate(vnode_boundaries):
            if token <= boundary:
                return i
        # Token is above the last boundary, wraps to vnode 0
        return 0

    return find_owning_vnode(first_token) == find_owning_vnode(last_token)


async def verify_data_integrity(cql, ks, table, num_keys, cl=ConsistencyLevel.QUORUM):
    stmt = SimpleStatement(f"SELECT * FROM {ks}.{table}")
    stmt.consistency_level = cl
    rows = await cql.run_async(stmt)
    data = {r.pk: r.c for r in rows}
    expected = {k: k for k in range(num_keys)}
    if data != expected:
        missing = expected.keys() - data.keys()
        extra = data.keys() - expected.keys()
        wrong = {k: (data[k], expected[k]) for k in data.keys() & expected.keys() if data[k] != expected[k]}
        assert False, f"Data mismatch: missing keys {missing}, extra keys {extra}, wrong values {wrong}"


async def verify_migration_status(manager: ManagerClient, server: ServerInfo,
                                  ks: str, expected_status: str,
                                  expected_node_statuses: dict[str, tuple[str, str]],
                                  retries: int = 0, retry_interval: float = 0):
    async def _check():
        status = await manager.api.get_vnode_tablet_migration_status(server.ip_addr, ks)
        actual_node_statuses = {n['host_id']: (n['current_mode'], n['intended_mode']) for n in status['nodes']}
        assert status['status'] == expected_status, f"Expected migration status '{expected_status}', got '{status['status']}'"
        assert actual_node_statuses == expected_node_statuses, f"Expected node statuses {expected_node_statuses}, got {actual_node_statuses}"

    for attempt in range(retries + 1):
        try:
            await _check()
            return
        except AssertionError:
            if attempt < retries and retry_interval > 0:
                await asyncio.sleep(retry_interval)
            else:
                raise


@pytest.mark.asyncio
async def test_migration(manager: ManagerClient):
    """Verify vnodes-to-tablets migration for a single table on a single-node cluster.

    Steps:
    1. Start a single node with multiple shards and power-of-2 aligned vnodes.
    2. Create a vnode table and inject data.
    3. Start the migration by creating a tablet map for the table.
       - Verify that the tablet map was created and tablet tokens match vnode tokens.
    4. Mark the node for upgrade on next restart.
    5. Restart the node (triggers resharding on vnode boundaries).
       - Verify that the new SSTables are properly segregated within vnode boundaries.
       - Verify data integrity.
    6. Finalize the migration.
       - Verify that the keyspace schema has tablets enabled.
    """
    num_shards = 3
    tokens_per_node = 16
    num_keys = 5000

    logger.info(f"Starting a node with {num_shards} shards and {tokens_per_node} power-of-2 aligned tokens")
    tokens = calculate_powof2_tokens(num_nodes=1, tokens_per_node=tokens_per_node)
    token_list = tokens[1]  # server_id 1
    initial_token = ','.join([str(t) for t in token_list])
    servers = (await manager.servers_add(1, cmdline=['--smp', str(num_shards), '--initial-token', initial_token, '--logger-log-level', 'compaction=debug']))
    server = servers[0]
    host_id = await manager.get_host_id(server.server_id)

    cql, _ = await manager.get_ready_cql(servers)

    logger.info("Creating keyspace and table with vnodes")
    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'enabled': false}}") as ks:
        # Create the table with minor compaction disabled to ensure that Scylla
        # won't delete SSTables while we're checking them.
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int) WITH compaction = {{'class': 'IncrementalCompactionStrategy', 'enabled': false}}")

        logger.info("Populating table in batches, flushing after each batch to produce multiple SSTables")
        stmt = cql.prepare(f"INSERT INTO {ks}.test (pk, c) VALUES (?, ?)")
        num_flushes = 5
        keys_per_sstable = num_keys // num_flushes
        for batch_start in range(0, num_keys, keys_per_sstable):
            batch_end = min(batch_start + keys_per_sstable, num_keys)
            await asyncio.gather(*(cql.run_async(stmt, [k, k]) for k in range(batch_start, batch_end)))
            await manager.api.keyspace_flush(server.ip_addr, ks, "test")

        logger.info("Collecting pre-migration SSTable info")
        node_workdir = await manager.server_get_workdir(server.server_id)
        scylla_path = await manager.server_get_exe(server.server_id)
        scylla_yaml = os.path.join(node_workdir, "conf", "scylla.yaml")
        table_data_dir = glob.glob(os.path.join(node_workdir, "data", ks, "test-*"))[0]
        pre_migration_sstables = glob.glob(os.path.join(table_data_dir, "*-Data.db"))
        logger.info(f"Pre-migration SSTable count: {len(pre_migration_sstables)}")
        assert len(pre_migration_sstables) == num_shards * num_flushes

        logger.info("Verifying that at least one pre-migration SSTable spans multiple vnode ranges (ensures that resharding will have work to do)")
        vnode_boundaries = sorted(token_list)
        pre_migration_ranges = get_sstable_token_ranges(scylla_path, scylla_yaml, pre_migration_sstables)
        cross_vnode_count = sum(1 for first, last in pre_migration_ranges
                                if not sstable_range_within_vnode(first, last, vnode_boundaries))
        logger.info(f"Pre-migration: {cross_vnode_count}/{len(pre_migration_ranges)} SSTables span multiple vnodes")
        assert cross_vnode_count >= 1, \
            "Expected at least one pre-migration SSTable to span multiple vnode ranges, but none was found. The test is malformed."

        logger.info("Verifying migration status before starting migration")
        await verify_migration_status(manager, server, ks, expected_status='vnodes', expected_node_statuses={})

        logger.info("Starting vnodes-to-tablets migration (creating a tablet map)")
        await manager.api.create_vnode_tablet_migration(server.ip_addr, ks)

        logger.info("Verifying migration status after creating tablet map")
        await verify_migration_status(manager, server, ks,
            expected_status='migrating_to_tablets',
            expected_node_statuses={host_id: ('vnodes', 'vnodes')})

        logger.info("Verifying that the tablet map was created")
        tablet_count = await get_tablet_count(manager, server, ks, 'test')
        assert tablet_count == tokens_per_node, \
            f"Expected {tokens_per_node} tablet(s), got {tablet_count}"

        tablet_replicas = await get_all_tablet_replicas(manager, server, ks, 'test')
        assert len(tablet_replicas) == tokens_per_node, \
            f"Expected {tokens_per_node} tablet replica entries, got {len(tablet_replicas)}"

        logger.info("Verifying that tablet tokens match vnode tokens")
        tablet_tokens = sorted([tr.last_token for tr in tablet_replicas])
        assert tablet_tokens == vnode_boundaries, \
            f"Tablet tokens {tablet_tokens} do not match vnode tokens {vnode_boundaries}"

        logger.info("Verifying data integrity after building tablet map and before resharding")
        await verify_data_integrity(cql, ks, "test", num_keys)

        logger.info("Marking node for tablets migration")
        await manager.api.upgrade_node_to_tablets(server.ip_addr)

        logger.info("Verifying migration status after marking node for upgrade")
        await verify_migration_status(manager, server, ks,
            expected_status='migrating_to_tablets',
            expected_node_statuses={host_id: ('vnodes', 'tablets')})

        logger.info("Verifying data integrity after marking the node for upgrade and before resharding (ensures that the node is still using the vnode-based ERM)")
        await verify_data_integrity(cql, ks, "test", num_keys)

        logger.info("Restarting the node to trigger resharding")
        await manager.server_restart(server.server_id)
        await reconnect_driver(manager)
        cql, _ = await manager.get_ready_cql(servers)

        # After restart, resharding should have produced new SSTables
        post_restart_sstables = glob.glob(os.path.join(table_data_dir, "*-Data.db"))
        logger.info(f"Post-restart SSTable count: {len(post_restart_sstables)}")

        post_restart_ranges = get_sstable_token_ranges(scylla_path, scylla_yaml, post_restart_sstables)
        logger.info(f"Post-restart SSTable token ranges:")
        for i, (first, last) in enumerate(post_restart_ranges):
            within = sstable_range_within_vnode(first, last, vnode_boundaries)
            logger.info(f"  SSTable {i}: tokens [{first}, {last}], within single vnode: {within}")

        logger.info("Verifying that every post-restart SSTable falls within a single vnode range")
        for i, (first, last) in enumerate(post_restart_ranges):
            assert sstable_range_within_vnode(first, last, vnode_boundaries), \
                f"Post-restart SSTable {i} with token range [{first}, {last}] " \
                f"spans multiple vnode ranges (boundaries: {vnode_boundaries})"

        logger.info("Verifying data integrity after restart")
        await verify_data_integrity(cql, ks, "test", num_keys)

        logger.info("Verifying migration status after node restart (node should now use tablets)")
        await verify_migration_status(manager, server, ks,
            expected_status='migrating_to_tablets',
            expected_node_statuses={host_id: ('tablets', 'tablets')})

        logger.info("Finalizing tablets migration")
        await manager.api.finalize_vnode_tablet_migration(server.ip_addr, ks)

        logger.info("Verifying that the keyspace schema has tablets enabled")
        res = await cql.run_async(f"SELECT * FROM system_schema.scylla_keyspaces WHERE keyspace_name = '{ks}'")
        assert len(res) == 1 and res[0].initial_tablets is not None, "keyspace is still using vnodes after migration finalization"

        logger.info("Verifying migration status after finalization")
        await verify_migration_status(manager, server, ks, expected_status='tablets', expected_node_statuses={})


@pytest.mark.asyncio
async def test_migration_rollback(manager: ManagerClient):
    """Verify rollback of vnodes-to-tablets migration on a single-node cluster.

    Same as test_migration(), but after the first restart (which reshards on
    vnode boundaries), the node is marked for downgrade back to vnodes and
    restarted again. After finalization the keyspace must still use vnodes, and
    the group0 state (tablet map, intended_storage_mode) must have been
    cleared.

    Steps:
    1. Start a single node with multiple shards and power-of-2 aligned vnodes.
    2. Create a vnode table and inject data.
    3. Start the migration by creating a tablet map for the table.
    4. Mark the node for upgrade and restart (triggers resharding).
    5. Mark the node for downgrade back to vnodes and restart again.
    6. Finalize the migration.
       - Verify that the keyspace schema still uses vnodes.
       - Verify that the tablet map has been cleared.
       - Verify that intended_storage_mode is cleared.
       - Verify data integrity.
    """
    num_shards = 3
    tokens_per_node = 16
    num_keys = 5000

    logger.info(f"Starting a node with {num_shards} shards and {tokens_per_node} power-of-2 aligned tokens")
    tokens = calculate_powof2_tokens(num_nodes=1, tokens_per_node=tokens_per_node)
    token_list = tokens[1]  # server_id 1
    initial_token = ','.join([str(t) for t in token_list])
    servers = (await manager.servers_add(1, cmdline=['--smp', str(num_shards), '--initial-token', initial_token, '--logger-log-level', 'compaction=debug']))
    server = servers[0]
    host_id = await manager.get_host_id(server.server_id)

    cql, _ = await manager.get_ready_cql(servers)

    logger.info("Creating keyspace and table with vnodes")
    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'enabled': false}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int) WITH compaction = {{'class': 'IncrementalCompactionStrategy', 'enabled': false}}")

        logger.info("Populating table in batches, flushing after each batch to produce multiple SSTables")
        stmt = cql.prepare(f"INSERT INTO {ks}.test (pk, c) VALUES (?, ?)")
        num_flushes = 5
        keys_per_sstable = num_keys // num_flushes
        for batch_start in range(0, num_keys, keys_per_sstable):
            batch_end = min(batch_start + keys_per_sstable, num_keys)
            await asyncio.gather(*(cql.run_async(stmt, [k, k]) for k in range(batch_start, batch_end)))
            await manager.api.keyspace_flush(server.ip_addr, ks, "test")

        logger.info("Starting vnodes-to-tablets migration (creating a tablet map)")
        await manager.api.create_vnode_tablet_migration(server.ip_addr, ks)

        logger.info("Verifying that the tablet map was created")
        tablet_count = await get_tablet_count(manager, server, ks, 'test')
        assert tablet_count == tokens_per_node, \
            f"Expected {tokens_per_node} tablet(s), got {tablet_count}"

        logger.info("Marking node for tablets migration")
        await manager.api.upgrade_node_to_tablets(server.ip_addr)

        logger.info("Restarting the node to trigger resharding")
        await manager.server_restart(server.server_id)
        await reconnect_driver(manager)
        cql, _ = await manager.get_ready_cql(servers)

        logger.info("Verifying data integrity after first restart")
        await verify_data_integrity(cql, ks, "test", num_keys)

        logger.info("Verifying migration status after first restart (node should now use tablets)")
        await verify_migration_status(manager, server, ks,
            expected_status='migrating_to_tablets',
            expected_node_statuses={host_id: ('tablets', 'tablets')})

        logger.info("Marking node for downgrade back to vnodes")
        await manager.api.downgrade_node_to_vnodes(server.ip_addr)

        logger.info("Verifying migration status after marking node for downgrade")
        await verify_migration_status(manager, server, ks,
            expected_status='migrating_to_tablets',
            expected_node_statuses={host_id: ('tablets', 'vnodes')})

        logger.info("Restarting the node to trigger resharding back to vnodes")
        await manager.server_restart(server.server_id)
        await reconnect_driver(manager)
        cql, _ = await manager.get_ready_cql(servers)

        logger.info("Verifying data integrity after second restart (downgrade)")
        await verify_data_integrity(cql, ks, "test", num_keys)

        logger.info("Verifying migration status after second restart (node should be back on vnodes)")
        await verify_migration_status(manager, server, ks,
            expected_status='migrating_to_tablets',
            expected_node_statuses={host_id: ('vnodes', 'vnodes')})

        logger.info("Finalizing migration (rollback path)")
        await manager.api.finalize_vnode_tablet_migration(server.ip_addr, ks)

        logger.info("Verifying that the keyspace schema still uses vnodes")
        res = await cql.run_async(f"SELECT * FROM system_schema.scylla_keyspaces WHERE keyspace_name = '{ks}'")
        assert len(res) == 0, \
            f"Expected keyspace to still use vnodes after rollback"

        logger.info("Verifying that the tablet map has been cleared")
        tablet_count = await get_tablet_count(manager, server, ks, 'test')
        assert tablet_count == 0, \
            f"Expected 0 tablets after rollback, got {tablet_count}"

        logger.info("Verifying that intended_storage_mode is cleared after finalization")
        rows = await cql.run_async("SELECT host_id, intended_storage_mode FROM system.topology WHERE key = 'topology'")
        for row in rows:
            assert row.intended_storage_mode is None, \
                f"Expected intended_storage_mode=None for node {row.host_id} after rollback finalization, got '{row.intended_storage_mode}'"

        logger.info("Verifying migration status after rollback finalization")
        await verify_migration_status(manager, server, ks, expected_status='vnodes', expected_node_statuses={})

        logger.info("Final data integrity check")
        await verify_data_integrity(cql, ks, "test", num_keys)


@pytest.mark.asyncio
async def test_migration_multinode(manager: ManagerClient):
    """Verify vnodes-to-tablets migration for a single table on a multi-node cluster with rolling restarts.

    Steps:
    1. Start a 2-node cluster with RF=2, multiple shards, and power-of-2 aligned vnodes.
    2. Create a vnode table and inject data at CL=QUORUM.
    3. Create tablet map — verify one tablet replica per node, tablet tokens match vnode tokens.
    4. Rolling restart — for each node: mark for upgrade, verify intended_storage_mode
       in system.topology, restart, then verify reads/writes at CL=QUORUM from every node.
    5. Finalize — verify keyspace schema has tablets enabled, intended_storage_mode is cleared
       for all nodes.
    """
    num_nodes = 2
    num_shards = 3
    tokens_per_node = 64
    num_keys = 1000

    logger.info(f"Starting {num_nodes} nodes with {num_shards} shards each, ~{tokens_per_node} power-of-2 aligned tokens per node")
    calculated_tokens = calculate_powof2_tokens(num_nodes=num_nodes, tokens_per_node=tokens_per_node)
    total_vnodes = sum(len(t) for t in calculated_tokens.values())
    logger.info(f"Total vnodes: {total_vnodes}")

    servers = []
    cfg = {'tablet_load_stats_refresh_interval_in_seconds': 1}
    for i, tokens in calculated_tokens.items():
        cmdline = [
            '--smp', str(num_shards),
            '--logger-log-level', 'compaction=debug',
            f"--initial-token={','.join([str(t) for t in tokens])}",
        ]
        servers.append(await manager.server_add(cmdline=cmdline, property_file={"dc": "dc1", "rack": f"rack{i}"}, config=cfg))

    cql, _ = await manager.get_ready_cql(servers)

    server_to_host_map = {s.server_id: await manager.get_host_id(s.server_id) for s in servers}
    host_ids = set(server_to_host_map.values())

    logger.info(f"Creating keyspace and table with vnodes (RF={num_nodes})")
    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {num_nodes}}} AND tablets = {{'enabled': false}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int)")

        logger.info(f"Populating table with {num_keys} rows at CL=QUORUM")
        insert_stmt = cql.prepare(f"INSERT INTO {ks}.test (pk, c) VALUES (?, ?)")
        insert_stmt.consistency_level = ConsistencyLevel.QUORUM
        await asyncio.gather(*(cql.run_async(insert_stmt, [k, k]) for k in range(num_keys)))

        logger.info("Starting vnodes-to-tablets migration (creating tablet map)")
        await manager.api.create_vnode_tablet_migration(servers[0].ip_addr, ks)

        logger.info("Verifying migration status after creating tablet map")
        await verify_migration_status(manager, servers[0], ks,
            expected_status='migrating_to_tablets',
            expected_node_statuses={host_id: ('vnodes', 'vnodes') for host_id in host_ids})

        logger.info("Verifying tablet map")
        tablet_replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
        assert len(tablet_replicas) == total_vnodes, \
            f"Expected {total_vnodes} tablets, got {len(tablet_replicas)}"

        all_vnode_tokens = sorted([t for tokens in calculated_tokens.values() for t in tokens])
        tablet_tokens = sorted([tr.last_token for tr in tablet_replicas])
        assert tablet_tokens == all_vnode_tokens, \
            f"Tablet tokens do not match vnode tokens"

        for tr in tablet_replicas:
            replica_hosts = set(r[0] for r in tr.replicas)
            assert replica_hosts == host_ids, \
                f"Tablet at token {tr.last_token}: replica hosts {replica_hosts} != expected {host_ids}"

        logger.info("Verifying data integrity before rolling restart")
        await verify_data_integrity(cql, ks, "test", num_keys)

        logger.info("Starting rolling restarts: mark each node for upgrade, restart it, verify reads/writes at CL=QUORUM from every node")
        for restart_idx, s in enumerate(servers):
            logger.info(f"Marking node {s.server_id} for tablets migration")
            await manager.api.upgrade_node_to_tablets(s.ip_addr)

            logger.info(f"Verifying intended_storage_mode='tablets' on node {s.server_id}")
            # It is important to verify the intended_storage_mode value on the
            # local group0 copy of the node that is about to be restarted
            # because the upgrade is an offline operation happening on startup,
            # so the node's group0 copy must be up-to-date.
            host_obj = cql.cluster.metadata.get_host(s.ip_addr)
            node_host_id = server_to_host_map[s.server_id]
            rows = await cql.run_async(f"SELECT intended_storage_mode FROM system.topology WHERE key = 'topology' AND host_id = {node_host_id}", host=host_obj)
            assert len(rows) == 1, f"Expected 1 row for host_id {node_host_id}, got {len(rows)}"
            assert rows[0].intended_storage_mode == "tablets", \
                f"Expected intended_storage_mode='tablets' for node {s.server_id}, got '{rows[0].intended_storage_mode}'"

            logger.info(f"Restarting node {s.server_id}")
            await manager.server_restart(s.server_id)
            await reconnect_driver(manager)
            cql, _ = await manager.get_ready_cql(servers)

            # Run 10 read/write queries from each node at CL=QUORUM to verify
            # correctness. The cluster is in a semi-migrated state, so some
            # nodes use a tablet ERM and some nodes use a vnode ERM.
            logger.info(f"Running read/write verification from all nodes after restarting node {s.server_id}")
            insert_stmt = cql.prepare(f"INSERT INTO {ks}.test (pk, c) VALUES (?, ?)")
            insert_stmt.consistency_level = ConsistencyLevel.QUORUM
            select_stmt = cql.prepare(f"SELECT c FROM {ks}.test WHERE pk = ?")
            select_stmt.consistency_level = ConsistencyLevel.QUORUM
            for node_idx, node in enumerate(servers):
                host_obj = cql.cluster.metadata.get_host(node.ip_addr)
                base_key = num_keys + restart_idx * num_nodes * 10 + node_idx * 10
                for i in range(10):
                    key = base_key + i
                    await cql.run_async(insert_stmt, [key, key], host=host_obj)
                    rows = await cql.run_async(select_stmt, [key], host=host_obj)
                    assert len(rows) == 1 and rows[0].c == key, \
                        f"Read/write verification failed: node {node.server_id}, key {key}"

        logger.info("Verifying migration status after rolling restarts")
        await verify_migration_status(manager, servers[0], ks,
            expected_status='migrating_to_tablets',
            expected_node_statuses={host_id: ('tablets', 'tablets') for host_id in host_ids},
            retries=1, retry_interval=1) # the status is derived from the topology coordinator's load stats which are refreshed every second; give it one retry after 1 second

        logger.info("Finalizing tablets migration")
        await manager.api.finalize_vnode_tablet_migration(servers[0].ip_addr, ks)

        # Barrier on an arbitrary node to ensure we can observe the latest group0 state from it.
        await read_barrier(manager.api, servers[1].ip_addr)
        host1 = cql.cluster.metadata.get_host(servers[1].ip_addr)

        logger.info("Verifying that the keyspace schema has tablets enabled")
        res = await cql.run_async(f"SELECT * FROM system_schema.scylla_keyspaces WHERE keyspace_name = '{ks}'", host=host1)
        assert len(res) == 1 and res[0].initial_tablets is not None, \
            "Keyspace is still using vnodes after migration finalization"

        logger.info("Verifying intended_storage_mode is cleared for all nodes after finalization")
        rows = await cql.run_async(f"SELECT host_id, intended_storage_mode FROM system.topology WHERE key = 'topology'", host=host1)
        assert len(rows) == num_nodes, f"Expected {num_nodes} rows, got {len(rows)}"
        for row in rows:
            assert row.intended_storage_mode is None, \
                f"Expected intended_storage_mode=None for node {row.host_id} after finalization, got '{row.intended_storage_mode}'"

        logger.info("Final data integrity check")
        total_keys = num_keys + num_nodes * num_nodes * 10 # original keys + 10 keys per node inserted during rolling restarts
        await verify_data_integrity(cql, ks, "test", total_keys)


async def setup_single_node_with_powof2_tokens(manager: ManagerClient):
    """Start a single node with power-of-2 aligned tokens for migration error tests."""
    tokens_per_node = 16
    tokens = calculate_powof2_tokens(num_nodes=1, tokens_per_node=tokens_per_node)
    token_list = tokens[1]
    initial_token = ','.join([str(t) for t in token_list])
    server = await manager.server_add(cmdline=['--smp', '2', '--initial-token', initial_token])
    cql, _ = await manager.get_ready_cql([server])
    return server, cql


@pytest.mark.asyncio
async def test_migration_nonexistent_keyspace(manager: ManagerClient):
    """Verify that migration APIs fail on a non-existent keyspace."""
    server, cql = await setup_single_node_with_powof2_tokens(manager)

    with pytest.raises(HTTPError, match="Can't find a keyspace"):
        await manager.api.create_vnode_tablet_migration(server.ip_addr, "ks")
    with pytest.raises(HTTPError, match="Can't find a keyspace"):
        await manager.api.get_vnode_tablet_migration_status(server.ip_addr, "ks")
    with pytest.raises(HTTPError, match="Can't find a keyspace"):
        await manager.api.finalize_vnode_tablet_migration(server.ip_addr, "ks")


@pytest.mark.asyncio
async def test_migration_already_tablets(manager: ManagerClient):
    """Verify that starting migration on a keyspace that already uses tablets fails."""
    server, cql = await setup_single_node_with_powof2_tokens(manager)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks_tablets:
        with pytest.raises(HTTPError, match="already uses tablets"):
            await manager.api.create_vnode_tablet_migration(server.ip_addr, ks_tablets)


@pytest.mark.asyncio
async def test_migration_empty_keyspace(manager: ManagerClient):
    """Verify that starting migration on a keyspace with no tables fails."""
    server, cql = await setup_single_node_with_powof2_tokens(manager)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'enabled': false}") as ks_empty:
        with pytest.raises(HTTPError, match="has no tables to migrate"):
            await manager.api.create_vnode_tablet_migration(server.ip_addr, ks_empty)


@pytest.mark.asyncio
async def test_migration_finalize_without_migration(manager: ManagerClient):
    """Verify that finalizing migration without starting one first fails."""
    server, cql = await setup_single_node_with_powof2_tokens(manager)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'enabled': false}") as ks_vnodes:
        await cql.run_async(f"CREATE TABLE {ks_vnodes}.t (pk int PRIMARY KEY)")
        with pytest.raises(HTTPError, match="does not have a tablet map"):
            await manager.api.finalize_vnode_tablet_migration(server.ip_addr, ks_vnodes)


@pytest.mark.asyncio
async def test_migration_upgrade_without_migration(manager: ManagerClient):
    """Verify that upgrading a node to tablets without an active migration fails."""
    server, cql = await setup_single_node_with_powof2_tokens(manager)

    with pytest.raises(HTTPError, match="no migration is in progress"):
        await manager.api.upgrade_node_to_tablets(server.ip_addr)


@pytest.mark.asyncio
async def test_migration_overlapping_migrations(manager: ManagerClient):
    """Verify that starting a second migration while one is already in progress fails."""
    server, cql = await setup_single_node_with_powof2_tokens(manager)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'enabled': false}") as ks1:
        await cql.run_async(f"CREATE TABLE {ks1}.t (pk int PRIMARY KEY)")
        await manager.api.create_vnode_tablet_migration(server.ip_addr, ks1)
        await manager.api.upgrade_node_to_tablets(server.ip_addr)

        async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'enabled': false}") as ks2:
            await cql.run_async(f"CREATE TABLE {ks2}.t (pk int PRIMARY KEY)")
            with pytest.raises(HTTPError, match="Another migration is in progress"):
                await manager.api.create_vnode_tablet_migration(server.ip_addr, ks2)

        # Rollback: schema changes are not yet supported for migrating keyspaces.
        # TODO: Remove this once support is added.
        await manager.api.downgrade_node_to_vnodes(server.ip_addr)
        await manager.api.finalize_vnode_tablet_migration(server.ip_addr, ks1)


@pytest.mark.asyncio
async def test_migration_finalize_before_upgrade(manager: ManagerClient):
    """Verify that finalizing migration before the node has finished upgrading fails."""
    server, cql = await setup_single_node_with_powof2_tokens(manager)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'enabled': false}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY)")
        await manager.api.create_vnode_tablet_migration(server.ip_addr, ks)
        await manager.api.upgrade_node_to_tablets(server.ip_addr)

        with pytest.raises(HTTPError, match=fr"Migration finalization failed for keyspace '{ks}': Node .* has not yet migrated table {ks}.t to tablets"):
            await manager.api.finalize_vnode_tablet_migration(server.ip_addr, ks)

        # Rollback: schema changes are not yet supported for migrating keyspaces.
        # TODO: Remove this once support is added.
        await manager.api.downgrade_node_to_vnodes(server.ip_addr)
        await manager.api.finalize_vnode_tablet_migration(server.ip_addr, ks)


@pytest.mark.asyncio
async def test_migration_multiple_keyspaces(manager: ManagerClient):
    """Verify that two keyspaces can be migrated from vnodes to tablets simultaneously."""
    num_shards = 3
    tokens_per_node = 16

    logger.info(f"Starting a node with {num_shards} shards and {tokens_per_node} power-of-2 aligned tokens")
    tokens = calculate_powof2_tokens(num_nodes=1, tokens_per_node=tokens_per_node)
    token_list = tokens[1]
    initial_token = ','.join([str(t) for t in token_list])
    servers = await manager.servers_add(1, cmdline=['--smp', str(num_shards), '--initial-token', initial_token])
    server = servers[0]
    host_id = await manager.get_host_id(server.server_id)

    cql, _ = await manager.get_ready_cql(servers)

    ks_opts = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'enabled': false}"

    logger.info("Creating two vnode keyspaces with tables")
    async with new_test_keyspace(manager, ks_opts) as ks1:
        async with new_test_keyspace(manager, ks_opts) as ks2:
            await cql.run_async(f"CREATE TABLE {ks1}.t (pk int PRIMARY KEY, c int)")
            await cql.run_async(f"CREATE TABLE {ks2}.t (pk int PRIMARY KEY, c int)")

            logger.info("Preparing both keyspaces for migration (creating tablet maps)")
            await manager.api.create_vnode_tablet_migration(server.ip_addr, ks1)
            await manager.api.create_vnode_tablet_migration(server.ip_addr, ks2)

            logger.info("Marking node for tablets migration and restarting")
            await manager.api.upgrade_node_to_tablets(server.ip_addr)
            await manager.server_restart(server.server_id)
            await reconnect_driver(manager)
            cql, _ = await manager.get_ready_cql(servers)

            logger.info("Verifying both keyspaces show as migrating")
            await verify_migration_status(manager, server, ks1,
                expected_status='migrating_to_tablets',
                expected_node_statuses={host_id: ('tablets', 'tablets')},
                retries=1, retry_interval=1)
            await verify_migration_status(manager, server, ks2,
                expected_status='migrating_to_tablets',
                expected_node_statuses={host_id: ('tablets', 'tablets')},
                retries=1, retry_interval=1)

            logger.info("Finalizing migration for ks1")
            await manager.api.finalize_vnode_tablet_migration(server.ip_addr, ks1)

            logger.info("Verifying ks1 has tablets enabled")
            res = await cql.run_async(f"SELECT * FROM system_schema.scylla_keyspaces WHERE keyspace_name = '{ks1}'")
            assert len(res) == 1 and res[0].initial_tablets is not None, \
                f"ks1 should use tablets after finalization"

            logger.info("Verifying intended_storage_mode is preserved (ks2 still migrating)")
            rows = await cql.run_async("SELECT host_id, intended_storage_mode FROM system.topology WHERE key = 'topology'")
            for row in rows:
                assert row.intended_storage_mode is not None, \
                    f"intended_storage_mode should be preserved for node {row.host_id} while ks2 is still migrating"

            logger.info("Finalizing migration for ks2")
            await manager.api.finalize_vnode_tablet_migration(server.ip_addr, ks2)

            logger.info("Verifying ks2 has tablets enabled")
            res = await cql.run_async(f"SELECT * FROM system_schema.scylla_keyspaces WHERE keyspace_name = '{ks2}'")
            assert len(res) == 1 and res[0].initial_tablets is not None, \
                f"ks2 should use tablets after finalization"

            logger.info("Verifying intended_storage_mode is cleared (no more migrating keyspaces)")
            rows = await cql.run_async("SELECT host_id, intended_storage_mode FROM system.topology WHERE key = 'topology'")
            for row in rows:
                assert row.intended_storage_mode is None, \
                    f"intended_storage_mode should be cleared for node {row.host_id} after all migrations are done, got '{row.intended_storage_mode}'"