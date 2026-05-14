#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import pytest
import logging
import uuid

from test.pylib.manager_client import ManagerClient
from test.pylib.internal_types import HostID
from test.pylib.util import wait_for_cql_and_get_hosts
from test.pylib.tablets import get_all_tablet_replicas
from test.pylib.object_storage import GSFront
from test.pylib.skip_types import skip_bug
from test.pylib.rest_client import read_barrier
from test.cluster.object_store.conftest import keyspace_options
from test.cluster.util import new_test_keyspace, wait_for_token_ring_and_group0_consistency
from cassandra.query import SimpleStatement, ConsistencyLevel
from time import time

logger = logging.getLogger(__name__)


async def get_host_id(cql, host=None):
    """Get the host_id of a node from system.local."""
    rows = await cql.run_async("SELECT host_id FROM system.local", host=host)
    return rows[0].host_id

async def get_table_id(cql, ks, table_name):
    """Get the table UUID from system_schema."""
    rows = await cql.run_async(
        f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = '{table_name}'")
    return rows[0].id

async def get_registry_entries(cql, table_id, node_owner, host=None):
    """Get sstables registry entries for a specific (table_id, node_owner) partition."""
    if isinstance(node_owner, str):
        node_owner = uuid.UUID(node_owner)
    rows = await cql.run_async(
        SimpleStatement(
            "SELECT generation, status FROM system_distributed.sstables"
            " WHERE table_id = %s AND node_owner = %s",
            consistency_level=ConsistencyLevel.ONE),
        parameters=[table_id, node_owner], host=host)
    return rows

async def get_all_registry_entries(cql, table_id, host=None):
    """Get all sstables registry entries for a given table_id"""
    rows = await cql.run_async(
        SimpleStatement(
            "SELECT generation, status, node_owner FROM system_distributed.sstables"
            " WHERE table_id = %s ALLOW FILTERING",
            consistency_level=ConsistencyLevel.ONE),
        parameters=[table_id], host=host)
    return rows


@pytest.mark.asyncio
async def test_sstables_registry_replicated_across_nodes(manager: ManagerClient, object_storage):
    """
    Verify that sstables registry entries in system_distributed.sstables
    are visible on ALL nodes in the cluster, not just the node that
    performed the flush.

    Starts a 2-node cluster with RF=1 object-storage-backed keyspace,
    inserts data, flushes on both nodes (only the replica owner will
    actually create SSTables), then verifies that both nodes see the
    same set of registry entries when querying all node_owner partitions.

    This proves that system_distributed.sstables is shared across the
    cluster rather than being node-local.
    """
    cfg = {
        'object_storage_endpoints': object_storage.create_endpoint_conf(),
        'experimental_features': ['keyspace-storage-options']
    }
    servers = await manager.servers_add(2, config=cfg)
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time() + 30)

    # Get host_ids for both nodes
    host_ids = []
    for h in hosts:
        host_ids.append(await get_host_id(cql, host=h))

    async with new_test_keyspace(manager, keyspace_options(object_storage, rf=1)) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t1 (pk int PRIMARY KEY, v int)")
        for i in range(10):
            await cql.run_async(f"INSERT INTO {ks}.t1 (pk, v) VALUES ({i}, {i})")

        # Flush on both nodes -- only the RF=1 replica owner creates SSTables,
        # but flushing both ensures we don't need to figure out which node owns data.
        for srv in servers:
            await manager.api.flush_keyspace(srv.ip_addr, ks)

        table_id = await get_table_id(cql, ks, 't1')

        # Query both node_owner partitions from each node to collect all generations.
        # Each node should be able to see entries written by any node since
        # system_distributed is replicated.
        def collect_gens(rows):
            return sorted(str(r.generation) for r in rows)

        all_gens_from_node0 = []
        all_gens_from_node1 = []
        for hid in host_ids:
            rows0 = await cql.run_async(
                SimpleStatement(
                    "SELECT generation FROM system_distributed.sstables WHERE table_id = %s AND node_owner = %s",
                    consistency_level=ConsistencyLevel.ONE),
                parameters=[table_id, hid], host=hosts[0])
            rows1 = await cql.run_async(
                SimpleStatement(
                    "SELECT generation FROM system_distributed.sstables WHERE table_id = %s AND node_owner = %s",
                    consistency_level=ConsistencyLevel.ONE),
                parameters=[table_id, hid], host=hosts[1])
            all_gens_from_node0.extend(rows0)
            all_gens_from_node1.extend(rows1)

        gens_node0 = collect_gens(all_gens_from_node0)
        gens_node1 = collect_gens(all_gens_from_node1)

        assert len(gens_node0) > 0 and len(gens_node1) > 0, \
            "Expected at least one node to have registry entries after flush"
        assert gens_node0 == gens_node1, \
            f"Registry entries differ between nodes: node0={gens_node0}, node1={gens_node1}"


@pytest.mark.asyncio
async def test_tablet_move_updates_registry(manager: ManagerClient, object_storage):
    """
    Verify that moving a tablet from one node to another correctly
    updates the sstables registry: the destination node creates new
    entries (status=sealed) and the source node's entries are cleaned up.

    Uses a 2-node cluster with RF=1 and a single tablet to ensure
    deterministic placement and movement.
    """
    if isinstance(object_storage, GSFront):
        skip_bug("https://scylladb.atlassian.net/browse/SCYLLADB-2044")
    cfg = {
        'object_storage_endpoints': object_storage.create_endpoint_conf(),
        'experimental_features': ['keyspace-storage-options']
    }
    servers = await manager.servers_add(2, config=cfg)
    await manager.disable_tablet_balancing()
    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, servers, time() + 30)

    host_ids = {}
    for s in servers:
        host_ids[s] = await manager.get_host_id(s.server_id)

    ks_opts = keyspace_options(object_storage, rf=1) + " AND tablets = {'initial': 1}"
    async with new_test_keyspace(manager, ks_opts) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t1 (pk int PRIMARY KEY, v int)")
        for i in range(10):
            await cql.run_async(f"INSERT INTO {ks}.t1 (pk, v) VALUES ({i}, {i})")

        for srv in servers:
            await manager.api.flush_keyspace(srv.ip_addr, ks)

        table_id = await get_table_id(cql, ks, 't1')

        # Find which node owns the tablet
        replicas = await get_all_tablet_replicas(manager, servers[0], ks, 't1')
        assert len(replicas) == 1, f"Expected 1 tablet, got {len(replicas)}"
        assert len(replicas[0].replicas) == 1, f"Expected RF=1, got {len(replicas[0].replicas)}"
        src_host_id, src_shard = replicas[0].replicas[0]
        token = replicas[0].last_token

        # Determine source and destination servers
        src_server = None
        dst_server = None
        for s in servers:
            if host_ids[s] == src_host_id:
                src_server = s
            else:
                dst_server = s
        assert src_server and dst_server, f"Could not match host_ids: tablet src={src_host_id}, servers={host_ids}"

        dst_host_id = host_ids[dst_server]

        # Verify source has registry entries before move
        src_entries = await get_registry_entries(cql, table_id, src_host_id)
        assert len(src_entries) > 0, "Source should have registry entries before move"
        logger.info(f"Source {src_host_id} has {len(src_entries)} registry entries before move")

        # Move the tablet
        logger.info(f"Moving tablet from {src_host_id} (shard {src_shard}) to {dst_host_id} (shard 0)")
        await manager.api.move_tablet(servers[0].ip_addr, ks, "t1",
                                      src_host_id, src_shard,
                                      dst_host_id, 0, token)

        # Verify data is still readable
        rows = await cql.run_async(f"SELECT * FROM {ks}.t1")
        assert len(rows) == 10, f"Expected 10 rows after move, got {len(rows)}"

        # Read barrier to make sure we see latest registry state
        for srv in servers:
            await read_barrier(manager.api, srv.ip_addr)

        # Verify destination has registry entries (status=sealed)
        dst_entries = await get_registry_entries(cql, table_id, dst_host_id)
        assert len(dst_entries) > 0, \
            f"Destination {dst_host_id} should have registry entries after move"
        for entry in dst_entries:
            assert entry.status == 'sealed', \
                f"Destination entry {entry.generation} has status '{entry.status}', expected 'sealed'"
        logger.info(f"Destination {dst_host_id} has {len(dst_entries)} sealed registry entries")

        # Verify source entries are cleaned up
        src_entries_after = await get_registry_entries(cql, table_id, src_host_id)
        assert len(src_entries_after) == 0, \
            f"Source {src_host_id} should have no registry entries after move, got {len(src_entries_after)}"
        logger.info("Source registry entries cleaned up successfully")


@pytest.mark.asyncio
async def test_decommission_cleans_registry(manager: ManagerClient, object_storage):
    """
    Verify that decommissioning a node removes all its sstables
    registry entries. Tablets are migrated to remaining nodes, which
    create their own registry entries, and the decommissioned node's
    entries are cleaned up.

    Uses a 2-node cluster with RF=1, 1 rack (each node in a separate rack)
    so that after decommission, the balancer moves the tablets to the remaining node.
    """
    if isinstance(object_storage, GSFront):
        skip_bug("https://scylladb.atlassian.net/browse/SCYLLADB-2044")
    cfg = {
        'object_storage_endpoints': object_storage.create_endpoint_conf(),
        'experimental_features': ['keyspace-storage-options']
    }
    servers = await manager.servers_add(2, config=cfg)

    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, servers, time() + 30)

    host_ids = {}
    for s in servers:
        host_ids[s] = await manager.get_host_id(s.server_id)

    ks_opts = keyspace_options(object_storage, rf=1) + " AND tablets = {'initial': 1}"
    async with new_test_keyspace(manager, ks_opts) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t1 (pk int PRIMARY KEY, v int)")
        for i in range(10):
            await cql.run_async(f"INSERT INTO {ks}.t1 (pk, v) VALUES ({i}, {i})")

        for srv in servers:
            await manager.api.flush_keyspace(srv.ip_addr, ks)

        table_id = await get_table_id(cql, ks, 't1')

        # Record host_id of the node we'll decommission
        decom_server = servers[1]
        decom_host_id = host_ids[decom_server]

        # Verify there are some registry entries before decommission
        all_entries = await get_all_registry_entries(cql, table_id)
        assert len(all_entries) > 0, "Should have registry entries after flush"
        logger.info(f"Total registry entries before decommission: {len(all_entries)}")

        # Decommission the node
        logger.info(f"Decommissioning server {decom_server} (host_id={decom_host_id})")
        await manager.decommission_node(decom_server.server_id)
        await wait_for_token_ring_and_group0_consistency(manager, time() + 30)

        remaining_server = servers[0]

        # Verify no registry entries remain for the decommissioned node
        decom_entries = await get_registry_entries(cql, table_id, decom_host_id)
        assert len(decom_entries) == 0, \
            (f"Decommissioned node {decom_host_id} should have no "
             f"registry entries, got {len(decom_entries)}")
        logger.info("Decommissioned node's registry entries cleaned up")

        # Verify data is still readable from remaining nodes
        rows = await cql.run_async(f"SELECT * FROM {ks}.t1")
        assert len(rows) == 10, f"Expected 10 rows, got {len(rows)}"

        # Verify remaining nodes have registry entries
        hid = host_ids[remaining_server]
        entries = await get_registry_entries(cql, table_id, hid)
        logger.info(f"Node {hid} has {len(entries)} registry entries after decommission")



@pytest.mark.asyncio
async def test_repair_creates_registry_entries(manager: ManagerClient, object_storage):
    """
    Verify that repair on object-storage keyspaces correctly creates
    sstables registry entries. Uses 2 nodes with RF=2. Both nodes
    receive writes (they are both replicas), but only the first node
    is flushed, so only it has SSTables and registry entries. Repair
    on the second node streams data and creates new registry entries.

    Note: incremental repair is not supported on object-storage keyspaces
    because object_storage_base does not implement link_with_excluded_components().
    Regular (tablet) repair uses streaming which works correctly.
    """
    if isinstance(object_storage, GSFront):
        skip_bug("https://scylladb.atlassian.net/browse/SCYLLADB-2044")
    cfg = {
        'object_storage_endpoints': object_storage.create_endpoint_conf(),
        'experimental_features': ['keyspace-storage-options'],
        'rf_rack_valid_keyspaces': False
    }

    servers = await manager.servers_add(2, config=cfg)

    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, servers, time() + 30)

    host_ids = {}
    for s in servers:
        host_ids[s] = await manager.get_host_id(s.server_id)

    ks_opts = keyspace_options(object_storage, rf=2)
    async with new_test_keyspace(manager, ks_opts) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t1 (pk int PRIMARY KEY, v int)")
        table_id = await get_table_id(cql, ks, 't1')

        # Insert data and flush only one node. With RF=2 both nodes
        # are replicas, so both have the data in memtables. But only
        # the flushed node creates SSTables and registry entries.
        src_node = servers[0]
        dst_node = servers[1]
        src_host_id = host_ids[src_node]
        dst_host_id = host_ids[dst_node]
        for i in range(10):
            await cql.run_async(f"INSERT INTO {ks}.t1 (pk, v) VALUES ({i}, {i})")

        await manager.api.flush_keyspace(src_node.ip_addr, ks)

        src_entries = await get_registry_entries(cql, table_id, src_host_id)
        assert len(src_entries) > 0, "Source should have registry entries after flush"
        logger.info(f"Source {src_host_id} has {len(src_entries)} registry entries")

        # Destination should have no registry entries yet (not flushed)
        dst_entries_before = await get_registry_entries(cql, table_id, dst_host_id)
        assert len(dst_entries_before) == 0, \
            f"Destination should have no registry entries before repair, got {len(dst_entries_before)}"

        # Run non-incremental repair on the empty node — triggers
        # streaming which creates new SSTables and registry entries.
        # Must use incremental_mode=disabled because object storage does
        # not implement link_with_excluded_components() needed by
        # incremental repair.
        logger.info(f"Running repair on {dst_host_id} ({dst_node.ip_addr})")
        params = {
            "ks": ks,
            "table": "t1",
            "tokens": "all",
            "await_completion": "true",
            "incremental_mode": "disabled",
        }
        await manager.api.client.post_json(
            "/storage_service/tablets/repair", host=dst_node.ip_addr, params=params)

        # Flush and read barrier to ensure registry entries are visible
        await manager.api.flush_keyspace(dst_node.ip_addr, ks)
        for s in servers:
            await read_barrier(manager.api, s.ip_addr)

        # After repair, the destination node should have registry entries
        dst_entries = await get_registry_entries(cql, table_id, dst_host_id)
        assert len(dst_entries) > 0, \
            f"Destination {dst_host_id} should have registry entries after repair"
        for e in dst_entries:
            assert e.status == 'sealed', \
                f"Entry {e.generation} has status '{e.status}', expected 'sealed'"
        logger.info(f"Destination {dst_host_id} has {len(dst_entries)} sealed entries after repair")

        # Verify data consistency
        rows = await cql.run_async(f"SELECT * FROM {ks}.t1")
        assert len(rows) == 10, f"Expected 10 rows, got {len(rows)}"
