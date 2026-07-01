#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import os
import glob
import json
import time
import pytest
import asyncio
import logging
import subprocess
from uuid import UUID
from cassandra.query import SimpleStatement, ConsistencyLevel

from test.pylib.tablets import get_tablet_count, get_all_tablet_replicas
from test.pylib.rest_client import HTTPError, read_barrier
from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.scylla_cluster import ReplaceConfig
from test.cluster.util import new_test_keyspace, reconnect_driver, ensure_group0_leader_on
from test.cluster.tasks.task_manager_client import TaskManagerClient

logger = logging.getLogger(__name__)

# Largest token that can be associated with a partition key (int64 max).
# The tablet map always ends at this token to cover the full ring.
MAX_TOKEN = 2**63 - 1


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


def sstable_within_single_range(first_token: int, last_token: int, boundaries: list[int]) -> bool:
    """Check whether an SSTable's token range falls entirely within a single range.

    Args:
        first_token: The first token in the SSTable.
        last_token: The last token in the SSTable.
        boundaries: Sorted list of token boundaries (e.g., vnode or tablet boundaries).

    Returns:
        True if both first_token and last_token fall within the same range.
    """
    for token in boundaries:
        if first_token < token <= last_token:
            return False
    return True


async def get_all_vnode_tokens(cql) -> list[int]:
    """Return a sorted list of all vnode token boundaries across all nodes.

    Queries the tokens column in system.topology, which contains the tokens
    for every node in the cluster.
    """
    rows = await cql.run_async("SELECT tokens FROM system.topology WHERE key = 'topology'")
    tokens = []
    for row in rows:
        if row.tokens:
            tokens.extend(int(t) for t in row.tokens)
    return sorted(tokens)


async def verify_data_integrity(cql, ks, table, num_keys, cl=ConsistencyLevel.QUORUM, server=None):
    stmt = SimpleStatement(f"SELECT * FROM {ks}.{table}")
    stmt.consistency_level = cl
    if server:
        host = cql.cluster.metadata.get_host(server.ip_addr)
        rows = await cql.run_async(stmt, host=host)
    else:
        rows = await cql.run_async(stmt)
    data = {r.pk: r.c for r in rows}
    expected = {k: k for k in range(num_keys)}
    if data != expected:
        missing = expected.keys() - data.keys()
        extra = data.keys() - expected.keys()
        wrong = {k: (data[k], expected[k]) for k in data.keys() & expected.keys() if data[k] != expected[k]}
        assert False, f"Data mismatch: missing keys {missing}, extra keys {extra}, wrong values {wrong}"


def compute_pow2_boundaries(target_pow2: int) -> list[int]:
    """Compute pow2 tablet boundaries.

    Mirrors the C++ dht::get_uniform_tokens() function:
        for i in [1, count]: n = i * UINT64_MAX / count; bias(n)
    where bias(n) converts from unsigned to signed by subtracting 2^63.
    """
    UINT64_MAX = (1 << 64) - 1
    INT64_MIN = -(1 << 63)
    return [(i * UINT64_MAX) // target_pow2 + INT64_MIN for i in range(1, target_pow2 + 1)]


async def get_target_pow2_tablet_count(manager: ManagerClient, server: ServerInfo, ks: str, table_name: str) -> int:
    """Read target_pow2_tablet_count from system.tablets for a given table."""
    host = manager.get_cql().cluster.metadata.get_host(server.ip_addr)
    await read_barrier(manager.api, server.ip_addr)
    table_id = await manager.get_table_or_view_id(ks, table_name)
    rows = await manager.get_cql().run_async(
        f"SELECT DISTINCT target_pow2_tablet_count FROM system.tablets WHERE table_id = {table_id}",
        host=host)
    assert len(rows) > 0, f"No rows found in system.tablets for table {ks}.{table_name}"
    return rows[0].target_pow2_tablet_count


async def verify_tablet_map_boundaries(manager: ManagerClient, server: ServerInfo,
                                       ks: str, table_name: str,
                                       vnode_boundaries: list[int]) -> list:
    """Verify the initial tablet map contains exactly the union of vnode boundaries, max token, and pow2 boundaries.

    Returns the tablet_replicas list for further checks by the caller.
    """
    tablet_replicas = await get_all_tablet_replicas(manager, server, ks, table_name)
    tablet_tokens = set(tr.last_token for tr in tablet_replicas)

    vnode_set = set(vnode_boundaries)
    missing_vnodes = vnode_set - tablet_tokens
    assert not missing_vnodes, f"Vnode boundaries missing from tablet map: {sorted(missing_vnodes)}"

    assert MAX_TOKEN in tablet_tokens, "MAX_TOKEN missing from tablet map"

    target_pow2 = await get_target_pow2_tablet_count(manager, server, ks, table_name)
    assert target_pow2 and target_pow2 & (target_pow2 - 1) == 0, \
        f"target_pow2_tablet_count {target_pow2} is not a power of two"

    pow2_set = set(compute_pow2_boundaries(target_pow2))
    missing_pow2 = pow2_set - tablet_tokens
    assert not missing_pow2, f"Pow2 boundaries missing from tablet map: {sorted(missing_pow2)}"

    expected_all = vnode_set | pow2_set | {MAX_TOKEN}
    extra = tablet_tokens - expected_all
    assert not extra, f"Unexpected boundaries in tablet map: {sorted(extra)}"

    return tablet_replicas


async def verify_pow2_layout(manager: ManagerClient, server: ServerInfo,
                             ks: str, table_name: str):
    """Verify that the tablet map has an exact uniform pow2 layout."""
    tablet_replicas = await get_all_tablet_replicas(manager, server, ks, table_name)
    tablet_count = len(tablet_replicas)
    assert tablet_count > 0 and tablet_count & (tablet_count - 1) == 0, \
        f"Tablet count {tablet_count} is not a power of two"

    tablet_tokens = sorted(tr.last_token for tr in tablet_replicas)
    expected_tokens = compute_pow2_boundaries(tablet_count)

    assert tablet_tokens == expected_tokens


async def wait_for_pow2_convergence(manager: ManagerClient, server: ServerInfo,
                                    ks: str, table_name: str, timeout: float = 120):
    """Wait until pow2 convergence completes and verify the result.

    Polls target_pow2_tablet_count in system.tablets until it is cleared,
    indicating that the tablet map has converged to a power-of-two layout.
    Once cleared, verifies that the resulting tablet map has a pow2 layout.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        target = await get_target_pow2_tablet_count(manager, server, ks, table_name)
        if target is None or target == 0:
            await verify_pow2_layout(manager, server, ks, table_name)
            return
        await asyncio.sleep(1)
    assert False, f"Pow2 convergence for {ks}.{table_name} did not complete within {timeout}s"


async def verify_migration_status(manager: ManagerClient, server: ServerInfo,
                                  ks: str, expected_status: str,
                                  expected_node_statuses: dict[str, tuple[str, str]],
                                  retries: int = 0, retry_interval: float = 0):
    async def _check():
        # Verify migration status via the migration status API
        status = await manager.api.get_vnode_tablet_migration_status(server.ip_addr, ks)
        actual_node_statuses = {n['host_id']: (n['current_mode'], n['intended_mode']) for n in status['nodes']}
        assert status['status'] == expected_status, f"Expected migration status '{expected_status}', got '{status['status']}'"
        assert actual_node_statuses == expected_node_statuses, f"Expected node statuses {expected_node_statuses}, got {actual_node_statuses}"

        # Verify migration status via the tasks API
        tm = TaskManagerClient(manager.api)
        tasks = await tm.list_tasks(server.ip_addr, "vnodes_to_tablets_migration")
        ks_tasks = [t for t in tasks if t.keyspace == ks and t.type == "vnodes_to_tablets_migration"]

        if expected_status == "migrating_to_tablets":
            assert len(ks_tasks) == 1, f"Expected 1 virtual task for keyspace '{ks}', got {len(ks_tasks)}"
            task = ks_tasks[0]
            assert task.state == "running", f"Expected task state 'running', got '{task.state}'"
            assert task.type == "vnodes_to_tablets_migration", f"Expected task type 'vnodes_to_tablets_migration', got '{task.type}'"

            task_status = await tm.get_task_status(server.ip_addr, task.task_id)
            expected_total = len(expected_node_statuses)
            expected_completed = sum(1 for cm, _ in expected_node_statuses.values() if cm == 'tablets')
            assert task_status.progress_total == expected_total, f"Expected progress_total {expected_total}, got {task_status.progress_total}"
            assert task_status.progress_completed == expected_completed, f"Expected progress_completed {expected_completed}, got {task_status.progress_completed}"
        else:
            assert len(ks_tasks) == 0, f"Expected no virtual tasks for keyspace '{ks}' when status is '{expected_status}', got {len(ks_tasks)}"

    for attempt in range(retries + 1):
        try:
            await _check()
            return
        except AssertionError:
            if attempt < retries and retry_interval > 0:
                await asyncio.sleep(retry_interval)
            else:
                raise


async def test_migration(manager: ManagerClient):
    """Verify vnodes-to-tablets migration for a single table on a single-node cluster.

    Steps:
    1. Start a single node with multiple shards and random vnodes.
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

    logger.info(f"Starting a node with {num_shards} shards and {tokens_per_node} random tokens")
    cfg = {'num_tokens': tokens_per_node}
    servers = await manager.servers_add(1, cmdline=['--smp', str(num_shards), '--logger-log-level', 'compaction=debug'], config=cfg)
    server = servers[0]
    host_id = await manager.get_host_id(server.server_id)

    cql, _ = await manager.get_ready_cql(servers)

    vnode_boundaries = await get_all_vnode_tokens(cql)
    logger.info(f"Vnode boundaries ({len(vnode_boundaries)} tokens): {vnode_boundaries}")

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
        assert len(pre_migration_sstables) == num_shards * num_flushes
        pre_migration_ranges = get_sstable_token_ranges(scylla_path, scylla_yaml, pre_migration_sstables)
        cross_vnode_count = sum(1 for first, last in pre_migration_ranges
                                if not sstable_within_single_range(first, last, vnode_boundaries))
        logger.info(f"Pre-migration: {cross_vnode_count}/{len(pre_migration_ranges)} SSTables span multiple vnodes")

        logger.info("Verifying migration status before starting migration")
        await verify_migration_status(manager, server, ks, expected_status='vnodes', expected_node_statuses={})

        logger.info("Starting vnodes-to-tablets migration (creating a tablet map)")
        await manager.api.create_vnode_tablet_migration(server.ip_addr, ks)

        logger.info("Verifying migration status after creating tablet map")
        await verify_migration_status(manager, server, ks,
            expected_status='migrating_to_tablets',
            expected_node_statuses={host_id: ('vnodes', 'vnodes')})

        logger.info("Verifying that the tablet map was created")
        tablet_replicas = await verify_tablet_map_boundaries(manager, server, ks, 'test', vnode_boundaries)
        tablet_boundaries = sorted([tr.last_token for tr in tablet_replicas])

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
            within = sstable_within_single_range(first, last, tablet_boundaries)
            logger.info(f"  SSTable {i}: tokens [{first}, {last}], within single tablet: {within}")

        logger.info("Verifying that every post-restart SSTable falls within a single tablet range")
        for i, (first, last) in enumerate(post_restart_ranges):
            assert sstable_within_single_range(first, last, tablet_boundaries), \
                f"Post-restart SSTable {i} with token range [{first}, {last}] " \
                f"spans multiple tablet ranges (boundaries: {tablet_boundaries})"

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

        logger.info("Waiting for pow2 convergence to complete")
        await wait_for_pow2_convergence(manager, server, ks, 'test')


async def test_migration_rollback(manager: ManagerClient):
    """Verify rollback of vnodes-to-tablets migration on a single-node cluster.

    Same as test_migration(), but after the first restart (which reshards on
    vnode boundaries), the node is marked for downgrade back to vnodes and
    restarted again. After finalization the keyspace must still use vnodes, and
    the group0 state (tablet map, intended_storage_mode) must have been
    cleared.

    Steps:
    1. Start a single node with multiple shards and random vnodes.
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

    logger.info(f"Starting a node with {num_shards} shards and {tokens_per_node} random tokens")
    cfg = {'num_tokens': tokens_per_node}
    servers = await manager.servers_add(1, cmdline=['--smp', str(num_shards), '--logger-log-level', 'compaction=debug'], config=cfg)
    server = servers[0]
    host_id = await manager.get_host_id(server.server_id)

    cql, _ = await manager.get_ready_cql(servers)

    vnode_boundaries = await get_all_vnode_tokens(cql)
    logger.info(f"Vnode boundaries ({len(vnode_boundaries)} tokens): {vnode_boundaries}")

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
        await verify_tablet_map_boundaries(manager, server, ks, 'test', vnode_boundaries)

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


async def test_migration_multinode(manager: ManagerClient):
    """Verify vnodes-to-tablets migration for a single table on a multi-node cluster with rolling restarts.

    Steps:
    1. Start a 2-node cluster with RF=2, multiple shards, and random vnodes.
    2. Create a vnode table and inject data at CL=QUORUM.
    3. Create tablet map — verify one tablet replica per node, tablet tokens match vnode tokens.
    4. Rolling restart — for each node: mark for upgrade, verify intended_storage_mode
       in system.topology, restart, then verify reads/writes at CL=QUORUM from every node.
    5. Finalize — verify keyspace schema has tablets enabled, intended_storage_mode is cleared
       for all nodes.
    """
    num_nodes = 2
    num_shards = 3
    tokens_per_node = 16
    num_keys = 1000

    logger.info(f"Starting {num_nodes} nodes with {num_shards} shards each, {tokens_per_node} random tokens per node")

    servers = []
    cfg = {'tablet_load_stats_refresh_interval_in_seconds': 1, 'num_tokens': tokens_per_node}
    for i in range(1, num_nodes + 1):
        cmdline = [
            '--smp', str(num_shards),
            '--logger-log-level', 'compaction=debug',
        ]
        servers.append(await manager.server_add(cmdline=cmdline, property_file={"dc": "dc1", "rack": f"rack{i}"}, config=cfg))

    cql, _ = await manager.get_ready_cql(servers)

    server_to_host_map = {s.server_id: await manager.get_host_id(s.server_id) for s in servers}
    host_ids = set(server_to_host_map.values())

    all_vnode_tokens = await get_all_vnode_tokens(cql)
    total_vnodes = len(all_vnode_tokens)
    logger.info(f"Total vnodes: {total_vnodes}")

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
        tablet_replicas = await verify_tablet_map_boundaries(manager, servers[0], ks, 'test', all_vnode_tokens)

        for tr in tablet_replicas:
            replica_hosts = set(r[0] for r in tr.replicas)
            assert replica_hosts == host_ids, \
                f"Tablet at token {tr.last_token}: replica hosts {replica_hosts} != expected {host_ids}"

        logger.info("Verifying data integrity before rolling restart")
        await verify_data_integrity(cql, ks, "test", num_keys)

        logger.info("Starting rolling restarts: mark each node for upgrade, restart it, verify reads/writes at CL=QUORUM from every node")
        expected_keys = num_keys
        for s in servers:
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

            # Check for SCYLLADB-2207: issue a full-table scan from each node
            # while the cluster is in semi-migrated state.
            range_scan_stmt = SimpleStatement(f"SELECT * FROM {ks}.test")
            range_scan_stmt.consistency_level = ConsistencyLevel.QUORUM
            for node in servers:
                logger.info(f"Running full-table scan with node {node.server_id} as coordinator after restarting node {s.server_id}")
                host_obj = cql.cluster.metadata.get_host(node.ip_addr)
                rows = await cql.run_async(range_scan_stmt, host=host_obj)
                assert len(rows) == expected_keys, \
                    f"Full-table scan verification failed: node {node.server_id}, expected {expected_keys} rows, got {len(rows)}"

            # Run 10 read/write queries from each node at CL=QUORUM to verify
            # correctness. The cluster is in a semi-migrated state, so some
            # nodes use a tablet ERM and some nodes use a vnode ERM.
            insert_stmt = cql.prepare(f"INSERT INTO {ks}.test (pk, c) VALUES (?, ?)")
            insert_stmt.consistency_level = ConsistencyLevel.QUORUM
            select_stmt = cql.prepare(f"SELECT c FROM {ks}.test WHERE pk = ?")
            select_stmt.consistency_level = ConsistencyLevel.QUORUM
            for node_idx, node in enumerate(servers):
                logger.info(f"Running read/write verification with node {node.server_id} as coordinator after restarting node {s.server_id}")
                host_obj = cql.cluster.metadata.get_host(node.ip_addr)
                base_key = expected_keys + node_idx * 10
                for i in range(10):
                    key = base_key + i
                    await cql.run_async(insert_stmt, [key, key], host=host_obj)
                    rows = await cql.run_async(select_stmt, [key], host=host_obj)
                    assert len(rows) == 1 and rows[0].c == key, \
                        f"Read/write verification failed: node {node.server_id}, key {key}"
            expected_keys += num_nodes * 10

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
        await verify_data_integrity(cql, ks, "test", expected_keys)

        logger.info("Waiting for pow2 convergence to complete")
        await wait_for_pow2_convergence(manager, servers[0], ks, 'test')


async def setup_single_node(manager: ManagerClient):
    """Start a single node with random tokens for migration error tests."""
    cfg = {'num_tokens': 16}
    server = await manager.server_add(cmdline=['--smp', '2'], config=cfg)
    cql, _ = await manager.get_ready_cql([server])
    return server, cql


async def test_migration_nonexistent_keyspace(manager: ManagerClient):
    """Verify that migration APIs fail on a non-existent keyspace."""
    server, cql = await setup_single_node(manager)

    with pytest.raises(HTTPError, match="Can't find a keyspace"):
        await manager.api.create_vnode_tablet_migration(server.ip_addr, "ks")
    with pytest.raises(HTTPError, match="Can't find a keyspace"):
        await manager.api.get_vnode_tablet_migration_status(server.ip_addr, "ks")
    with pytest.raises(HTTPError, match="Can't find a keyspace"):
        await manager.api.finalize_vnode_tablet_migration(server.ip_addr, "ks")


async def test_migration_already_tablets(manager: ManagerClient):
    """Verify that starting migration on a keyspace that already uses tablets fails."""
    server, cql = await setup_single_node(manager)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks_tablets:
        with pytest.raises(HTTPError, match="already uses tablets"):
            await manager.api.create_vnode_tablet_migration(server.ip_addr, ks_tablets)


async def test_migration_empty_keyspace(manager: ManagerClient):
    """Verify that starting migration on a keyspace with no tables fails."""
    server, cql = await setup_single_node(manager)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'enabled': false}") as ks_empty:
        with pytest.raises(HTTPError, match="has no tables to migrate"):
            await manager.api.create_vnode_tablet_migration(server.ip_addr, ks_empty)


async def test_migration_finalize_without_migration(manager: ManagerClient):
    """Verify that finalizing migration without starting one first fails."""
    server, cql = await setup_single_node(manager)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'enabled': false}") as ks_vnodes:
        await cql.run_async(f"CREATE TABLE {ks_vnodes}.t (pk int PRIMARY KEY)")
        with pytest.raises(HTTPError, match="does not have a tablet map"):
            await manager.api.finalize_vnode_tablet_migration(server.ip_addr, ks_vnodes)


async def test_migration_upgrade_without_migration(manager: ManagerClient):
    """Verify that upgrading a node to tablets without an active migration fails."""
    server, cql = await setup_single_node(manager)

    with pytest.raises(HTTPError, match="no migration is in progress"):
        await manager.api.upgrade_node_to_tablets(server.ip_addr)


async def test_migration_overlapping_migrations(manager: ManagerClient):
    """Verify that starting a second migration while one is already in progress fails."""
    server, cql = await setup_single_node(manager)

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


async def test_migration_finalize_before_upgrade(manager: ManagerClient):
    """Verify that finalizing migration before the node has finished upgrading fails."""
    server, cql = await setup_single_node(manager)

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


async def test_migration_task_not_abortable(manager: ManagerClient):
    """Verify that aborting a vnodes-to-tablets migration task via the task manager fails."""
    server, cql = await setup_single_node(manager)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'enabled': false}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY)")
        await manager.api.create_vnode_tablet_migration(server.ip_addr, ks)

        tm = TaskManagerClient(manager.api)
        tasks = await tm.list_tasks(server.ip_addr, "vnodes_to_tablets_migration")
        ks_tasks = [t for t in tasks if t.keyspace == ks and t.type == "vnodes_to_tablets_migration"]
        assert len(ks_tasks) == 1, f"Expected 1 migration task for keyspace '{ks}', got {len(ks_tasks)}"

        task = ks_tasks[0]
        status = await tm.get_task_status(server.ip_addr, task.task_id)
        assert status.is_abortable is False, f"Expected task to be non-abortable, got is_abortable={status.is_abortable}"

        with pytest.raises(HTTPError, match="cannot be aborted"):
            await tm.abort_task(server.ip_addr, task.task_id)

        # Rollback: schema changes are not yet supported for migrating keyspaces.
        # TODO: Remove this once support is added.
        await manager.api.finalize_vnode_tablet_migration(server.ip_addr, ks)


async def test_migration_wait_task(manager: ManagerClient):
    """Verify that the task manager "wait" API works for vnodes-to-tablets migration tasks.

    Exercises two scenarios:
    1. Wait on a migration task that is rolled back — expect "suspended" state.
    2. Wait on a migration task that completes successfully — expect "done" state.
    """
    server, cql = await setup_single_node(manager)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'enabled': false}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY)")

        tm = TaskManagerClient(manager.api)

        # Scenario 1: wait + rollback

        logger.info("Starting migration")
        await manager.api.create_vnode_tablet_migration(server.ip_addr, ks)

        tasks = await tm.list_tasks(server.ip_addr, "vnodes_to_tablets_migration")
        migration_tasks = [t for t in tasks if t.type == "vnodes_to_tablets_migration"]
        assert len(migration_tasks) == 1
        assert migration_tasks[0].keyspace == ks
        task_id = migration_tasks[0].task_id

        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()

        logger.info(f"Starting wait on the migration task '{task_id}'")
        wait_task = asyncio.create_task(tm.wait_for_task(server.ip_addr, task_id))

        await log.wait_for('migration_virtual_task: waiting for vnodes-to-tablets migration to finish', from_mark=mark)

        logger.info("Rolling back migration")
        await manager.api.finalize_vnode_tablet_migration(server.ip_addr, ks)

        logger.info("Expecting wait to finish with 'suspended' state")
        wait_status = await wait_task
        assert wait_status.state == "suspended", f"Expected 'suspended' after rollback, got '{wait_status.state}'"
        assert wait_status.progress_completed == 0, f"Expected 0 upgraded nods for rolled back migration, got {wait_status.progress_completed}"

        # Scenario 2: wait + full migration

        logger.info("Starting migration again")
        await manager.api.create_vnode_tablet_migration(server.ip_addr, ks)

        tasks = await tm.list_tasks(server.ip_addr, "vnodes_to_tablets_migration")
        migration_tasks = [t for t in tasks if t.type == "vnodes_to_tablets_migration"]
        assert len(migration_tasks) == 1
        assert migration_tasks[0].keyspace == ks
        task_id = migration_tasks[0].task_id

        logger.info("Marking the node for tablets migration and restarting")
        await manager.api.upgrade_node_to_tablets(server.ip_addr)
        await manager.server_restart(server.server_id)
        await reconnect_driver(manager)
        cql, _ = await manager.get_ready_cql([server])

        mark = await log.mark()

        logger.info(f"Starting wait on the migration task '{task_id}'")
        wait_task = asyncio.create_task(tm.wait_for_task(server.ip_addr, task_id))

        await log.wait_for('migration_virtual_task: waiting for vnodes-to-tablets migration to finish', from_mark=mark)

        logger.info("Finalizing migration")
        await manager.api.finalize_vnode_tablet_migration(server.ip_addr, ks)

        logger.info("Expecting wait to finish with 'done' state")
        wait_status = await wait_task
        assert wait_status.state == "done", f"Expected 'done' after finalization, got '{wait_status.state}'"
        assert wait_status.progress_completed == 1, f"Expected 1 upgraded node for completed migration, got {wait_status.progress_completed}"


async def test_migration_multiple_keyspaces(manager: ManagerClient):
    """Verify that two keyspaces can be migrated from vnodes to tablets simultaneously."""
    num_shards = 3
    tokens_per_node = 16

    logger.info(f"Starting a node with {num_shards} shards and {tokens_per_node} random tokens")
    cfg = {'num_tokens': tokens_per_node}
    servers = await manager.servers_add(1, cmdline=['--smp', str(num_shards)], config=cfg)
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


async def test_migration_multiple_tables(manager: ManagerClient):
    """Verify vnodes-to-tablets migration on keyspace with multiple tables.

    The test verifies that all tables get correct tablet maps, that resharding
    and finalization work for all tables, and that pow2 convergence completes
    for every table.
    """
    num_shards = 3
    tokens_per_node = 16

    logger.info(f"Starting a node with {num_shards} shards and {tokens_per_node} random tokens")
    cfg = {'num_tokens': tokens_per_node}
    servers = await manager.servers_add(1, cmdline=['--smp', str(num_shards)], config=cfg)
    server = servers[0]

    cql, _ = await manager.get_ready_cql(servers)

    vnode_boundaries = await get_all_vnode_tokens(cql)
    logger.info(f"Vnode boundaries ({len(vnode_boundaries)} tokens): {vnode_boundaries}")

    logger.info("Creating keyspace with two vnode tables")
    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'enabled': false}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t1 (pk int PRIMARY KEY, c int)")
        await cql.run_async(f"CREATE TABLE {ks}.t2 (pk int PRIMARY KEY, c int)")

        logger.info("Starting vnodes-to-tablets migration (creating tablet maps)")
        await manager.api.create_vnode_tablet_migration(server.ip_addr, ks)

        logger.info("Verifying tablet map boundaries for both tables")
        await verify_tablet_map_boundaries(manager, server, ks, 't1', vnode_boundaries)
        await verify_tablet_map_boundaries(manager, server, ks, 't2', vnode_boundaries)

        logger.info("Marking node for tablets migration")
        await manager.api.upgrade_node_to_tablets(server.ip_addr)

        logger.info("Restarting the node to trigger resharding")
        await manager.server_restart(server.server_id)
        await reconnect_driver(manager)
        cql, _ = await manager.get_ready_cql(servers)

        logger.info("Finalizing tablets migration")
        await manager.api.finalize_vnode_tablet_migration(server.ip_addr, ks)

        logger.info("Verifying that the keyspace schema has tablets enabled")
        res = await cql.run_async(f"SELECT * FROM system_schema.scylla_keyspaces WHERE keyspace_name = '{ks}'")
        assert len(res) == 1 and res[0].initial_tablets is not None, \
            "keyspace is still using vnodes after migration finalization"

        logger.info("Waiting for pow2 convergence on both tables")
        await wait_for_pow2_convergence(manager, server, ks, 't1')
        await wait_for_pow2_convergence(manager, server, ks, 't2')


async def test_tablet_status_in_migration_api(manager: ManagerClient):
    """"Verify the ?include=tablet_status query parameter in the migration API.

    When set, the migration API response is extended with a 'tablets' field that
    contains per-table pow2 convergence status. Intended for use after migration
    finalization to track convergence progress via the same API.

    Steps:
    1. Before migration: no-op; no 'tablets' field in response.
    2. During migration: no-op; no 'tablets' field in response.
    3. After finalization: 'tablets' field is present and its fields have correct values.
    4. Without query param: no 'tablets' field in response.
    5. After pow2 convergence: convergence is reported as completed for all tables.
    """
    num_shards = 3
    tokens_per_node = 16

    logger.info(f"Starting a node with {num_shards} shards and {tokens_per_node} random tokens")
    cfg = {'num_tokens': tokens_per_node}
    servers = await manager.servers_add(1, cmdline=['--smp', str(num_shards)], config=cfg)
    server = servers[0]

    cql, _ = await manager.get_ready_cql(servers)

    logger.info("Creating keyspace with two empty vnode-based tables")
    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'enabled': false}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t1 (pk int PRIMARY KEY, c int)")
        await cql.run_async(f"CREATE TABLE {ks}.t2 (pk int PRIMARY KEY, c int)")

        logger.info("Before migration - verify no tablet status in response")
        status = await manager.api.get_vnode_tablet_migration_status(server.ip_addr, ks, with_tablet_status=True)
        assert status['status'] == 'vnodes'
        assert 'tablets' not in status, "tablets field should not be present before migration"

        logger.info("Starting vnodes-to-tablets migration")
        await manager.api.create_vnode_tablet_migration(server.ip_addr, ks)

        logger.info("Marking node for tablets migration")
        await manager.api.upgrade_node_to_tablets(server.ip_addr)

        logger.info("Restarting node to trigger resharding")
        await manager.server_restart(server.server_id)
        await reconnect_driver(manager)
        cql, _ = await manager.get_ready_cql(servers)

        logger.info("During migration - verify no tablet status in response")
        status = await manager.api.get_vnode_tablet_migration_status(server.ip_addr, ks, with_tablet_status=True)
        assert status['status'] == 'migrating_to_tablets'
        assert 'tablets' not in status, "tablets field should not be present during migration"

        # Disable tablet balancing to prevent pow2 convergence from running
        # before we can check the post-finalization status.
        await manager.api.disable_tablet_balancing(server.ip_addr)

        logger.info("Finalizing migration")
        await manager.api.finalize_vnode_tablet_migration(server.ip_addr, ks)

        logger.info("After finalization - verify tablet status is present with in_progress convergence")
        status = await manager.api.get_vnode_tablet_migration_status(server.ip_addr, ks, with_tablet_status=True)
        assert status['status'] == 'tablets'
        assert 'tablets' in status, "tablets field should be present after finalization"

        pow2 = status['tablets']['pow2_convergence']
        assert pow2['status'] == 'in_progress'
        assert pow2['tables_total'] == 2
        assert pow2['tables_converging'] == 2
        assert len(pow2['tables']) == 2

        table_names = {t['table'] for t in pow2['tables']}
        assert table_names == {'t1', 't2'}, f"Expected tables t1 and t2, got {table_names}"

        for t in pow2['tables']:
            assert 'converging' in t
            assert 'current_tablet_count' in t
            assert 'target_pow2_tablet_count' in t
            assert t['converging'] is True

            # Verify current/target tablet counts match system.tablets
            table_name = t['table']
            expected_count = await get_tablet_count(manager, server, ks, table_name)
            expected_target = await get_target_pow2_tablet_count(manager, server, ks, table_name)
            assert t['current_tablet_count'] == expected_count, \
                f"Table {table_name}: API reports {t['current_tablet_count']} tablets, system.tablets has {expected_count}"
            assert t['target_pow2_tablet_count'] == expected_target, \
                f"Table {table_name}: API reports target {t['target_pow2_tablet_count']}, system.tablets has {expected_target}"

        logger.info("Without query param - verify no tablet status")
        status = await manager.api.get_vnode_tablet_migration_status(server.ip_addr, ks, with_tablet_status=False)
        assert 'tablets' not in status, "tablets field should not be present without query param"

        # Re-enable tablet balancing to allow pow2 convergence to proceed.
        await manager.api.enable_tablet_balancing(server.ip_addr)

        logger.info("After pow2 convergence - verify all tables have converged")
        await wait_for_pow2_convergence(manager, server, ks, 't1')
        await wait_for_pow2_convergence(manager, server, ks, 't2')

        status = await manager.api.get_vnode_tablet_migration_status(server.ip_addr, ks, with_tablet_status=True)
        pow2 = status['tablets']['pow2_convergence']
        assert pow2['status'] == 'complete'
        assert pow2['tables_converging'] == 0
        assert pow2['tables_total'] == 2

        for t in pow2['tables']:
            assert t['converging'] is False
            assert t['target_pow2_tablet_count'] == 0
            tablet_count = t['current_tablet_count']
            assert tablet_count > 0 and (tablet_count & (tablet_count - 1)) == 0, \
                f"Tablet count {tablet_count} for table {t['table']} is not a power of two"


async def test_pow2_convergence_virtual_task(manager: ManagerClient):
    """Verify that pow2 convergence is tracked via a virtual task.

    Coverage:
    - Task visibility: keyspace-level and table-level tasks appear during convergence.
    - Progress: keyspace task reports tables converged/total; table tasks report percent complete.
    - Parent-child: table tasks reference keyspace task as parent; keyspace task lists
      table tasks as children.
    - Not abortable: abort attempt is rejected.
    - Wait API: wait on keyspace-level task returns "done" when convergence finishes.
    - Lifetime: tasks disappear after convergence completes.

    Edge cases not covered (but could be added in the future):
    - Drop converging keyspace during task wait.
    - Drop converging table during task wait.
    - Drop all converging tables during task wait.
    """
    num_shards = 3
    tokens_per_node = 16

    logger.info(f"Starting a node with {num_shards} shards and {tokens_per_node} random tokens")
    cfg = {'num_tokens': tokens_per_node}
    servers = await manager.servers_add(1, cmdline=['--smp', str(num_shards)], config=cfg)
    server = servers[0]

    cql, _ = await manager.get_ready_cql(servers)

    logger.info("Creating keyspace with two empty vnode-based tables")
    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'enabled': false}}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t1 (pk int PRIMARY KEY, c int)")
        await cql.run_async(f"CREATE TABLE {ks}.t2 (pk int PRIMARY KEY, c int)")

        # Disable tablet balancing so convergence doesn't start after finalization.
        await manager.api.disable_tablet_balancing(server.ip_addr)

        logger.info("Starting vnodes-to-tablets migration")
        await manager.api.create_vnode_tablet_migration(server.ip_addr, ks)

        logger.info("Marking node for tablets migration")
        await manager.api.upgrade_node_to_tablets(server.ip_addr)

        logger.info("Restarting node to trigger resharding")
        await manager.server_restart(server.server_id)
        await reconnect_driver(manager)
        cql, _ = await manager.get_ready_cql(servers)

        logger.info("Finalizing migration")
        await manager.api.finalize_vnode_tablet_migration(server.ip_addr, ks)

        # At this point: migration is done, tables have target_pow2 != 0,
        # but load balancing is disabled so no merges will run.

        tm = TaskManagerClient(manager.api)

        logger.info("Verifying pow2 convergence tasks appear in task manager")
        tasks = await tm.list_tasks(server.ip_addr, "vnodes_to_tablets_migration")

        # Filter pow2_convergence tasks.
        convergence_tasks = [t for t in tasks if t.type == "pow2_convergence"]
        ks_tasks = [t for t in convergence_tasks if t.scope == "keyspace" and t.keyspace == ks]
        table_tasks = [t for t in convergence_tasks if t.scope == "table" and t.keyspace == ks]

        assert len(ks_tasks) == 1, f"Expected 1 keyspace-level convergence task, got {len(ks_tasks)}"
        assert len(table_tasks) == 2, f"Expected 2 table-level convergence tasks, got {len(table_tasks)}"

        ks_task = ks_tasks[0]
        assert ks_task.kind == "cluster"
        assert ks_task.state == "running"

        table_names = {t.table for t in table_tasks}
        assert table_names == {'t1', 't2'}, f"Expected tables t1 and t2, got {table_names}"
        for t in table_tasks:
            assert t.kind == "cluster"
            assert t.state == "running"

        logger.info("Verifying keyspace-level task status (progress and children)")
        ks_status = await tm.get_task_status(server.ip_addr, ks_task.task_id)
        assert ks_status.kind == "cluster"
        assert ks_status.state == "running"
        assert ks_status.is_abortable is False
        assert ks_status.progress_units == "tables"
        assert ks_status.progress_total == 2, f"Expected total=2, got {ks_status.progress_total}"
        assert ks_status.progress_completed == 0, f"Expected completed=0, got {ks_status.progress_completed}"

        # Children should be the two table-level task IDs.
        children_task_ids = {c['task_id'] for c in ks_status.children_ids}
        expected_children = {t.task_id for t in table_tasks}
        assert children_task_ids == expected_children, \
            f"Keyspace task children mismatch: got {children_task_ids}, expected {expected_children}"

        logger.info("Verifying table-level task status (progress, entity, parent)")
        for t_task in table_tasks:
            t_status = await tm.get_task_status(server.ip_addr, t_task.task_id)
            assert t_status.kind == "cluster"
            assert t_status.state == "running"
            assert t_status.is_abortable is False
            assert t_status.progress_units == "percent"
            assert t_status.progress_total == 100
            # Validate progress against the authoritative values in system.tablets.
            current_tablet_count = await get_tablet_count(manager, server, ks, t_task.table)
            target_pow2_tablet_count = await get_target_pow2_tablet_count(manager, server, ks, t_task.table)
            assert current_tablet_count > 0, f"Table {t_task.table}: expected current_tablet_count > 0, got {current_tablet_count}"
            expected_progress = int((target_pow2_tablet_count * 100.0 / current_tablet_count) + 0.5)
            assert t_status.progress_completed == expected_progress, \
                f"Table {t_task.table}: expected progress {expected_progress} (target={target_pow2_tablet_count}, current={current_tablet_count}), got {t_status.progress_completed}"
            expected_entity = f"{current_tablet_count} tablets (target: {target_pow2_tablet_count})"
            assert t_status.entity == expected_entity, \
                f"Table {t_task.table}: expected entity '{expected_entity}', got '{t_status.entity}'"
            # Parent should be the keyspace-level task.
            assert t_status.parent_id == ks_task.task_id, \
                f"Table {t_task.table}: expected parent_id={ks_task.task_id}, got {t_status.parent_id}"

        logger.info("Verifying convergence tasks are not abortable")
        with pytest.raises(HTTPError):
            await tm.abort_task(server.ip_addr, ks_task.task_id)

        with pytest.raises(HTTPError):
            await tm.abort_task(server.ip_addr, table_tasks[0].task_id)

        logger.info("Starting wait on keyspace-level convergence task")
        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()

        wait_task = asyncio.create_task(tm.wait_for_task(server.ip_addr, ks_task.task_id))

        await log.wait_for('pow2_convergence_virtual_task: waiting for pow2 convergence to finish', from_mark=mark)

        logger.info("Enabling tablet load balancing to let convergence proceed")
        await manager.api.enable_tablet_balancing(server.ip_addr)

        logger.info("Waiting for convergence to complete via task manager")
        wait_status = await asyncio.wait_for(wait_task, timeout=120)
        assert wait_status.state == "done", f"Expected 'done' state, got '{wait_status.state}'"
        assert wait_status.progress_completed == wait_status.progress_total, \
            f"Expected progress_completed == progress_total, got {wait_status.progress_completed}/{wait_status.progress_total}"

        logger.info("Verifying convergence tasks have disappeared")
        tasks = await tm.list_tasks(server.ip_addr, "vnodes_to_tablets_migration")
        convergence_tasks = [t for t in tasks if t.type == "pow2_convergence"]
        assert len(convergence_tasks) == 0, \
            f"Expected no convergence tasks after completion, got {len(convergence_tasks)}"


@pytest.mark.parametrize("rbno_enabled", [
    pytest.param(True, id="repair"),
    pytest.param(False, id="range_streamer"),
])
async def test_replace_during_migration(manager: ManagerClient, rbno_enabled: bool):
    """Replace a dead node when a vnodes-to-tablets migration is in progress.

    The test verifies that replace on a semi-migrated cluster succeeds, the
    tablet map is updated to reference the new node as replica, and the
    migration can resume after replace. A 3-node cluster is used so that Raft
    quorum is preserved with a dead node (required to run the node replacement).
    """
    num_nodes = 3
    num_shards = 2
    tokens_per_node = 16
    num_keys = 1000

    logger.info(f"Starting a cluster with {num_nodes} nodes, {num_shards} shards, "
                f"{tokens_per_node} tokens per node, rbno_enabled={rbno_enabled}")
    cfg = {
        'tablet_load_stats_refresh_interval_in_seconds': 1,
        'num_tokens': tokens_per_node,
        'enable_repair_based_node_ops': rbno_enabled,
    }
    cmdline = ['--smp', str(num_shards)]
    property_file = [{"dc": "dc1", "rack": f"rack{i}"} for i in range(1, num_nodes + 1)]
    servers = await manager.servers_add(num_nodes, cmdline=cmdline, config=cfg, property_file=property_file)

    cql, _ = await manager.get_ready_cql(servers)

    async def upgrade_and_restart_node(server: ServerInfo):
        logger.info(f"Marking node {server.server_id} for tablets migration")
        await manager.api.upgrade_node_to_tablets(server.ip_addr)
        logger.info(f"Restarting node {server.server_id} for resharding")
        await manager.server_restart(server.server_id)

    server_to_host_map = {s.server_id: await manager.get_host_id(s.server_id) for s in servers}
    host_ids = set(server_to_host_map.values())

    logger.info(f"Creating keyspace and table with RF={num_nodes} using vnodes")
    async with new_test_keyspace(manager,
            f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {num_nodes}}} "
            f"AND tablets = {{'enabled': false}}") as ks:
        table_name = "test"
        await cql.run_async(f"CREATE TABLE {ks}.{table_name} (pk int PRIMARY KEY, c int)")

        logger.info(f"Populating table with {num_keys} rows at CL=ALL")
        insert_stmt = cql.prepare(f"INSERT INTO {ks}.{table_name} (pk, c) VALUES (?, ?)")
        insert_stmt.consistency_level = ConsistencyLevel.ALL
        await asyncio.gather(*(cql.run_async(insert_stmt, [k, k]) for k in range(num_keys)))

        logger.info("Starting vnodes-to-tablets migration (creating tablet map)")
        await manager.api.create_vnode_tablet_migration(servers[0].ip_addr, ks)

        logger.info("Partially migrating cluster: upgrading 2 nodes to tablets")
        upgraded_servers = servers[:2]
        nonupgraded_servers = servers[2:]
        for s in upgraded_servers:
            await upgrade_and_restart_node(s)

        # Pick one of the upgraded servers to replace mid-migration.
        replaced_server = upgraded_servers[0]
        replaced_host_id = server_to_host_map[replaced_server.server_id]
        replaced_tokens = await manager.api.get_tokens(replaced_server.ip_addr)

        logger.info(f"Stopping node {replaced_server.server_id} to replace mid-migration")
        await manager.server_stop(replaced_server.server_id, convict=True)

        logger.info(f"Adding replacement node (same shard count {num_shards})")
        replace_cfg = ReplaceConfig(replaced_id=replaced_server.server_id, reuse_ip_addr=False, use_host_id=True)
        new_server = await manager.server_add(replace_cfg,
                                              cmdline=cmdline,
                                              property_file=replaced_server.property_file(),
                                              config=cfg)
        new_host_id = await manager.get_host_id(new_server.server_id)
        logger.info(f"Replacement node started: server_id={new_server.server_id}, host_id={new_host_id}")

        await reconnect_driver(manager)
        surviving_servers = [s for s in servers if s.server_id != replaced_server.server_id] + [new_server]
        cql, _ = await manager.get_ready_cql(surviving_servers)

        logger.info("Verifying node statuses after replacement")
        rs = await cql.run_async("SELECT host_id, node_state FROM system.topology WHERE key = 'topology'")
        for row in rs:
            if row.host_id == UUID(replaced_host_id):
                assert row.node_state == "left", f"Replaced node {row.host_id} should be left, got {row.node_state}"
            else:
                assert row.node_state == "normal", f"Surviving node {row.host_id} should be normal, got {row.node_state}"

        logger.info("Verifying that the new node has the same token ranges as the replaced node")
        new_tokens = await manager.api.get_tokens(new_server.ip_addr)
        assert replaced_tokens == new_tokens, \
            f"Replacement node tokens {new_tokens} do not match replaced node tokens {replaced_tokens}"

        # Due to RF=num_nodes, the new node should be a replica for all tablets.
        logger.info("Verifying tablet map after replacement")
        expected_host_ids = (host_ids - {server_to_host_map[replaced_server.server_id]}) | {new_host_id}

        check_server = servers[1]
        await read_barrier(manager.api, check_server.ip_addr)
        host = manager.get_cql().cluster.metadata.get_host(check_server.ip_addr)
        table_id = await manager.get_table_or_view_id(ks, table_name)
        tablet_rows = await manager.get_cql().run_async(
            f"SELECT last_token, replicas, new_replicas, stage, transition FROM system.tablets WHERE table_id = {table_id}", host=host)

        for row in tablet_rows:
            replica_hosts = {str(host_id) for host_id, _ in row.replicas}
            assert replica_hosts == expected_host_ids, f"Tablet at token {row.last_token}: expected replicas {expected_host_ids}, got {replica_hosts}"
            assert row.new_replicas is None, f"Tablet at token {row.last_token}: expected new_replicas to be empty, got {row.new_replicas}"
            assert row.stage is None, f"Tablet at token {row.last_token}: expected stage to be None, got {row.stage}"
            assert row.transition is None, f"Tablet at token {row.last_token}: expected transition to be None, got {row.transition}"

        logger.info("Verifying migration status after replacement (new node should be on vnodes)")
        upgraded_servers.remove(replaced_server)
        nonupgraded_servers.append(new_server)
        server_to_host_map[new_server.server_id] = new_host_id
        expected_node_statuses = {
            str(server_to_host_map[s.server_id]): (status, status)
            for servers, status in ((upgraded_servers, "tablets"), (nonupgraded_servers, "vnodes"))
            for s in servers
        }
        await verify_migration_status(manager, surviving_servers[0], ks,
            expected_status='migrating_to_tablets',
            expected_node_statuses=expected_node_statuses,
            retries=1,
            retry_interval=1)

        logger.info("Verifying data set on new node")
        await verify_data_integrity(cql, ks, table_name, num_keys, cl=ConsistencyLevel.LOCAL_ONE, server=new_server)

        logger.info("Resuming migration: upgrading the two vnode-based nodes")
        for s in nonupgraded_servers:
            await upgrade_and_restart_node(s)
        await reconnect_driver(manager)
        cql, _ = await manager.get_ready_cql(surviving_servers)

        logger.info("Finalizing tablets migration")
        await manager.api.finalize_vnode_tablet_migration(check_server.ip_addr, ks)

        # Barrier on an arbitrary node to ensure we can observe the latest group0 state from it.
        await read_barrier(manager.api, check_server.ip_addr)
        host1 = cql.cluster.metadata.get_host(check_server.ip_addr)

        logger.info("Verifying keyspace uses tablets after finalization")
        res = await cql.run_async(
            f"SELECT * FROM system_schema.scylla_keyspaces WHERE keyspace_name = '{ks}'", host=host1)
        assert len(res) == 1 and res[0].initial_tablets is not None, \
            "Keyspace is still using vnodes after migration finalization"


async def _check_migrating_tablet_map(manager: ManagerClient, server: ServerInfo, ks: str, table_name: str,
                                      expected_stage: str, expected_replicas_host_ids: set,
                                      expected_new_replicas_host_ids: set) -> None:
    """Read all rows for this table from system.tablets on `server` (after a read barrier) and assert,
    for every row:
      - stage == expected_stage
      - transition == 'rebuild'
      - {str(host) for (host, _) in replicas} == expected_replicas_host_ids
      - {str(host) for (host, _) in new_replicas} == expected_new_replicas_host_ids
    """
    await read_barrier(manager.api, server.ip_addr)
    host = manager.get_cql().cluster.metadata.get_host(server.ip_addr)
    table_id = await manager.get_table_or_view_id(ks, table_name)
    tablet_rows = await manager.get_cql().run_async(
        f"SELECT last_token, replicas, new_replicas, stage, transition FROM system.tablets WHERE table_id = {table_id}",
        host=host)
    assert tablet_rows, f"No tablet rows found for {ks}.{table_name} — tablet map is empty"
    for row in tablet_rows:
        replica_hosts = {str(h) for h, _ in row.replicas}
        new_replica_hosts = {str(h) for h, _ in row.new_replicas} if row.new_replicas is not None else set()
        assert row.stage == expected_stage, \
            f"Tablet at {row.last_token}: expected stage={expected_stage!r}, got {row.stage!r}"
        assert row.transition == 'rebuild', \
            f"Tablet at {row.last_token}: expected transition='rebuild', got {row.transition!r}"
        assert replica_hosts == expected_replicas_host_ids, \
            f"Tablet at {row.last_token}: expected replicas {expected_replicas_host_ids}, got {replica_hosts}"
        assert new_replica_hosts == expected_new_replicas_host_ids, \
            f"Tablet at {row.last_token}: expected new_replicas {expected_new_replicas_host_ids}, got {new_replica_hosts}"


async def _read_key(cql, ks: str, key: int, host) -> None:
    """Read a single row at CL=ONE via `host` (a Cassandra driver Host object) and assert the value.

    Verifies that a write that was supposed to reach `host` as a replica is actually readable.
    """
    stmt = cql.prepare(f"SELECT c FROM {ks}.test WHERE pk = ?")
    stmt.consistency_level = ConsistencyLevel.ONE
    rows = await cql.run_async(stmt, [key], host=host)
    assert len(rows) == 1 and rows[0].c == key, \
        f"CL=ONE read via {host} for pk={key} returned {rows}"


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_request_routing_during_replace(manager: ManagerClient):
    """Verify that CQL coordinators correctly route reads and writes to the
    replacing node during the write_both_read_old and write_both_read_new
    topology states if the table is undergoing vnodes-to-tablets migration.

    The difference between a normal vnode/tablet-based table and a migrating
    table is that, in the latter case, the cluster is in a mixed state with
    some nodes using a vnode ERM and others using a tablet ERM. To ensure that
    the replacement process remains transparent to user queries while the
    cluster is in this mixed state, the topology coordinator must update both
    ERMs in lockstep as part of the replacement process.

    Note that this test focuses only on the coordinator-to-replica request
    routing logic during the topology transitions. The end-to-end correctness
    of the replacement process is covered by test_replace_during_migration.

    Steps:
    1. Start a 3-node cluster {s0, s1, s2}, one node per rack.
    2. Create a vnode-based table, RF=3.
    3. Start vnodes-to-tablets migration and upgrade node s0 to tablets.
    4. Ensure the group0 leader runs on s1 and enable error injections for
       write_both_read_old and write_both_read_new on that node.
    5. Start replacing s2 with s2'. Wait for the replacement process to pause in write_both_read_old.
    6. Write keys K1 and K2 to s0 and s1 respectively.
       In one case the coordinator uses a tablet ERM, in the other it uses a vnode ERM.
       Read the keys to verify they are readable from both nodes.
       We also expect the write to be routed to s2', but s2' is not yet ready to serve CQL requests as coordinator, only as replica.
    7. Progress to write_both_read_new.
       Again, write and read new keys K3 and K4 to s0 and s1.
    8. Finish the replacement process and verify that K1..K4 are readable from s2'.
    """
    num_shards = 2
    num_tokens = 16
    K1, K2, K3, K4 = 1, 2, 3, 4
    INJECTION_WBRO = 'topology_coordinator/write_both_read_old/before_version_increment'
    INJECTION_WBRN = 'topology_coordinator/write_both_read_new/after_barrier'

    cfg = {
        'num_tokens': num_tokens,
        'tablet_load_stats_refresh_interval_in_seconds': 1,
    }
    cmdline = ['--smp', str(num_shards)]

    logger.info("Starting a 3-node cluster (one rack per node)")
    property_files = [{"dc": "dc1", "rack": f"rack{i}"} for i in range(1, 4)]
    servers = await manager.servers_add(3, cmdline=cmdline, config=cfg, property_file=property_files)
    s0, s1, s2 = servers[0], servers[1], servers[2]

    logger.info(f"Pinning raft group0 leader on s1 ({s1.server_id})")
    await ensure_group0_leader_on(manager, s1)

    cql, _ = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager,
            f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}} "
            f"AND tablets = {{'enabled': false}}") as ks:
        table_name = 'test'
        await cql.run_async(f"CREATE TABLE {ks}.{table_name} (pk int PRIMARY KEY, c int)")

        s0_id = str(await manager.get_host_id(s0.server_id))
        s1_id = str(await manager.get_host_id(s1.server_id))
        s2_id = str(await manager.get_host_id(s2.server_id))

        logger.info("Starting vnodes-to-tablets migration (creating tablet map)")
        await manager.api.create_vnode_tablet_migration(s1.ip_addr, ks)

        logger.info(f"Upgrading s0 ({s0.server_id}) to tablets")
        await manager.api.upgrade_node_to_tablets(s0.ip_addr)
        await manager.server_restart(s0.server_id)

        await reconnect_driver(manager)
        cql, _ = await manager.get_ready_cql(servers)

        logger.info(f"Enabling injection {INJECTION_WBRO!r} on s1")
        await manager.api.enable_injection(s1.ip_addr, INJECTION_WBRO, one_shot=True)
        logger.info(f"Enabling injection {INJECTION_WBRN!r} on s1")
        await manager.api.enable_injection(s1.ip_addr, INJECTION_WBRN, one_shot=True)

        logger.info(f"Stopping s2 ({s2.server_id}) — to be replaced")
        await manager.server_stop(s2.server_id, convict=True)

        logger.info("Starting replacement for s2 in background")
        replace_cfg = ReplaceConfig(replaced_id=s2.server_id, reuse_ip_addr=False, use_host_id=True)
        replace_task = asyncio.create_task(manager.server_add(replace_cfg, cmdline=cmdline, property_file=s2.property_file(), config=cfg))

        # PAUSE 1: write_both_read_old
        # The replacing node s2' must be in the tablet write set.
        # When this injection is hit, streaming has finished.
        # The following writes can reach s2' only via the CQL write path.
        logger.info(f"Waiting for injection {INJECTION_WBRO!r}")
        await manager.api.wait_for_injection_enter(s1.ip_addr, INJECTION_WBRO)

        # FIXME: AI argues that we cannot derive the host ID with get_host_id().
        # Not sure why. Need to investigate.
        # Discover the replacement node's host ID from system.topology.
        # At this point s2' has been accepted into the topology.
        await read_barrier(manager.api, s1.ip_addr)
        s1_cql_host = cql.cluster.metadata.get_host(s1.ip_addr)
        topology_rows = await cql.run_async(
            "SELECT host_id FROM system.topology WHERE key = 'topology'",
            host=s1_cql_host)
        old_ids = {s0_id, s1_id, s2_id}
        s2p_candidates = [str(r.host_id) for r in topology_rows if str(r.host_id) not in old_ids]
        # FIXME: If this assertion fires the replacement node has not yet appeared in
        # system.topology at this injection point.  Check whether the injection fires
        # before or after the new node is added to group0.
        assert len(s2p_candidates) == 1, \
            f"Expected exactly one replacement node in system.topology, got {s2p_candidates}"
        s2p_id = s2p_candidates[0]
        logger.info(f"Replacement node (s2') host_id: {s2p_id}")

        logger.info("Checking tablet map: stage=write_both_read_old (before K1 write)")
        await _check_migrating_tablet_map(
            manager, s0, ks, table_name,
            expected_stage='write_both_read_old',
            expected_replicas_host_ids={s0_id, s1_id, s2_id},
            expected_new_replicas_host_ids={s0_id, s1_id, s2p_id})

        s0_host = cql.cluster.metadata.get_host(s0.ip_addr)
        s1_host = cql.cluster.metadata.get_host(s1.ip_addr)
        write_stmt = cql.prepare(f"INSERT INTO {ks}.{table_name} (pk, c) VALUES (?, ?)")
        write_stmt.consistency_level = ConsistencyLevel.QUORUM

        logger.info(f"Writing K1={K1} via s0 (tablets-based coordinator)")
        await cql.run_async(write_stmt, [K1, K1], host=s0_host)
        await _read_key(cql, ks, K1, s0_host)
        await _read_key(cql, ks, K1, s1_host)

        logger.info(f"Writing K2={K2} via s1 (vnodes-based coordinator)")
        await cql.run_async(write_stmt, [K2, K2], host=s1_host)
        await _read_key(cql, ks, K2, s0_host)
        await _read_key(cql, ks, K2, s1_host)

        logger.info(f"Releasing injection {INJECTION_WBRO!r}")
        await manager.api.message_injection(s1.ip_addr, INJECTION_WBRO)

        # PAUSE 2: write_both_read_new
        # The replacing node s2' must be in the tablet read and write set.
        logger.info(f"Waiting for injection {INJECTION_WBRN!r}")
        await manager.api.wait_for_injection_enter(s1.ip_addr, INJECTION_WBRN)

        logger.info("Checking tablet map: stage=write_both_read_new (before K3 write)")
        await _check_migrating_tablet_map(
            manager, s0, ks, table_name,
            expected_stage='write_both_read_new',
            expected_replicas_host_ids={s0_id, s1_id, s2_id},
            expected_new_replicas_host_ids={s0_id, s1_id, s2p_id})

        logger.info(f"Writing K3={K3} via s0 (tablets-based coordinator)")
        await cql.run_async(write_stmt, [K3, K3], host=s0_host)
        await _read_key(cql, ks, K3, s0_host)
        await _read_key(cql, ks, K3, s1_host)

        logger.info(f"Writing K4={K4} via s1 (vnodes-based coordinator)")
        await cql.run_async(write_stmt, [K4, K4], host=s1_host)
        await _read_key(cql, ks, K4, s0_host)
        await _read_key(cql, ks, K4, s1_host)

        logger.info(f"Releasing injection {INJECTION_WBRN!r}")
        await manager.api.message_injection(s1.ip_addr, INJECTION_WBRN)

        # REPLACE COMPLETES
        logger.info("Waiting for replace to complete")
        new_server = await replace_task
        logger.info(f"Replacement node started: server_id={new_server.server_id}")

        await reconnect_driver(manager)
        live_servers = [s0, s1, new_server]
        cql, _ = await manager.get_ready_cql(live_servers)

        # POST-REPLACE VERIFICATION
        logger.info("Verifying final tablet map: replicas swapped, no pending transitions")
        await read_barrier(manager.api, s0.ip_addr)
        s0_host = cql.cluster.metadata.get_host(s0.ip_addr)
        table_id = await manager.get_table_or_view_id(ks, table_name)
        tablet_rows = await cql.run_async(
            f"SELECT last_token, replicas, new_replicas, stage, transition FROM system.tablets WHERE table_id = {table_id}",
            host=s0_host)
        assert tablet_rows, f"No tablet rows found for {ks}.{table_name} after replace"
        for row in tablet_rows:
            replica_hosts = {str(h) for h, _ in row.replicas}
            assert s2p_id in replica_hosts, \
                f"Tablet at {row.last_token}: replacement node {s2p_id} not in replicas {replica_hosts}"
            assert s2_id not in replica_hosts, \
                f"Tablet at {row.last_token}: dead node {s2_id} still in replicas {replica_hosts}"
            assert row.new_replicas is None, \
                f"Tablet at {row.last_token}: expected new_replicas=None, got {row.new_replicas}"
            assert row.stage is None, \
                f"Tablet at {row.last_token}: expected stage=None, got {row.stage}"
            assert row.transition is None, \
                f"Tablet at {row.last_token}: expected transition=None, got {row.transition}"

        logger.info("Verifying K1..K4 are present in s2'")
        new_server_host = cql.cluster.metadata.get_host(new_server.ip_addr)
        for k in [K1, K2, K3, K4]:
            await _read_key(cql, ks, k, new_server_host)
