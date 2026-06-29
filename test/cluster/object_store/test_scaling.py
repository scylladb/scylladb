#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import time
import logging
import pytest

from test.pylib.manager_client import ManagerClient
from test.pylib.object_storage import keyspace_options
from test.cluster.util import new_test_keyspace, wait_for_no_pending_topology_transition
from test.pylib.tablets import get_all_tablet_replicas
from cassandra.query import SimpleStatement, ConsistencyLevel

logger = logging.getLogger(__name__)

async def test_scaling(manager: ManagerClient, object_storage):
    """Test cluster scaling (add/remove nodes) with tablets on object storage.

    Creates a 3-node cluster (one per rack) with a keyspace on object storage,
    populates a table, adds a node to each rack, waits for tablet rebalancing,
    verifies data, then decommissions a node from each rack and verifies again.
    """
    num_racks = 3
    rf = 3

    objconf = object_storage.create_endpoint_conf()
    cfg = {
        'enable_user_defined_functions': False,
        'object_storage_endpoints': objconf,
        'experimental_features': ['keyspace-storage-options'],
    }

    # Create initial 3-node cluster, one node per rack
    logger.info("Creating initial 3-node cluster")
    servers = []
    for rack in range(num_racks):
        server = await manager.server_add(config=cfg, property_file={"dc": "dc1", "rack": f"r{rack}"})
        servers.append(server)

    cql = manager.get_cql()

    async def populate_table(cql, ks, num_rows):
        """Insert test data into the table with CL=ALL to ensure all replicas are consistent."""
        insert_stmt = cql.prepare(f"INSERT INTO {ks}.test (pk, value) VALUES (?, ?)")
        insert_stmt.consistency_level = ConsistencyLevel.ALL
        await asyncio.gather(*[
            cql.run_async(insert_stmt, [i, i * 10])
            for i in range(num_rows)
        ])

    async def verify_data(cql, ks, num_rows):
        """Verify all test data is readable and correct."""
        stmt = SimpleStatement(f"SELECT pk, value FROM {ks}.test;", consistency_level=ConsistencyLevel.ONE)
        res = await cql.run_async(stmt)
        rows = {r.pk: r.value for r in res}
        assert len(rows) == num_rows, f"Expected {num_rows} rows, got {len(rows)}"
        for i in range(num_rows):
            assert i in rows, f"Missing row with pk={i}"
            assert rows[i] == i * 10, f"Wrong value for pk={i}: expected {i * 10}, got {rows[i]}"

    async with new_test_keyspace(manager, keyspace_options(object_storage, rf=rf)) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, value int);")

        NUM_ROWS = 100

        # Populate test data
        logger.info("Populating table with %d rows", NUM_ROWS)
        await populate_table(cql, ks, NUM_ROWS)

        # Flush to ensure data is on object storage
        for server in servers:
            await manager.api.flush_keyspace(server.ip_addr, ks)

        # Verify initial data
        logger.info("Verifying initial data")
        await verify_data(cql, ks, NUM_ROWS)

        # Add one node to each rack
        logger.info("Adding 3 new nodes (one per rack)")
        new_servers = []
        for rack in range(num_racks):
            server = await manager.server_add(config=cfg, property_file={"dc": "dc1", "rack": f"r{rack}"})
            new_servers.append(server)

        # Wait for tablet load balancer to finish migrating tablets
        logger.info("Waiting for tablet rebalancing to complete")
        deadline = time.time() + 120
        await wait_for_no_pending_topology_transition(manager, deadline)

        # Verify data after scale-up
        logger.info("Verifying data after adding nodes")
        await verify_data(cql, ks, NUM_ROWS)

        # Verify tablets are distributed across all 6 nodes
        tablet_replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
        all_servers = servers + new_servers
        all_host_ids = set()
        for s in all_servers:
            host_id = await manager.get_host_id(s.server_id)
            all_host_ids.add(host_id)
        replica_hosts = set()
        for t in tablet_replicas:
            for host_id, shard in t.replicas:
                replica_hosts.add(host_id)
        logger.info("Tablets distributed across %d hosts (total %d)", len(replica_hosts), len(all_host_ids))

        # Decommission one node from each rack (the original nodes)
        logger.info("Decommissioning 3 original nodes (one per rack)")
        for server in servers:
            logger.info("Decommissioning %s", server.server_id)
            await manager.decommission_node(server.server_id)

        # Wait for topology to stabilize
        deadline = time.time() + 120
        await wait_for_no_pending_topology_transition(manager, deadline)

        # Verify data after scale-down
        logger.info("Verifying data after decommissioning nodes")
        await verify_data(cql, ks, NUM_ROWS)
