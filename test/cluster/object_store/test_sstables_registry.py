#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import pytest
import logging

from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts
from test.cluster.object_store.conftest import keyspace_options
from cassandra.query import SimpleStatement, ConsistencyLevel
from test.cluster.util import new_test_keyspace
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
        'experimental_features': ['keyspace-storage-options'],
    }
    servers = await manager.servers_add(2, config=cfg)
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time() + 60)

    # Get host_ids for both nodes
    host_ids = []
    for h in hosts:
        hid = await get_host_id(cql, host=h)
        host_ids.append(hid)

    async with new_test_keyspace(manager, keyspace_options(object_storage, rf=1)) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t1 (pk int PRIMARY KEY, v int)")
        for i in range(10):
            await cql.run_async(f"INSERT INTO {ks}.t1 (pk, v) VALUES ({i}, {i})")

        # Flush on both nodes — only the RF=1 replica owner creates SSTables,
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
