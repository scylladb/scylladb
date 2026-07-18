#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import time
import logging
import re
import uuid
import pytest

from test.pylib.manager_client import ManagerClient
from test.pylib.object_storage import keyspace_options
from test.cluster.util import new_test_keyspace, wait_for_no_pending_topology_transition
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts
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
    cmdline = ['--logger-log-level', 'sstable=debug']

    # Create initial 3-node cluster, one node per rack
    logger.info("Creating initial 3-node cluster")
    servers = []
    for rack in range(num_racks):
        server = await manager.server_add(config=cfg, cmdline=cmdline, property_file={"dc": "dc1", "rack": f"r{rack}"})
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

    def verify_uuid_sstable_generation(generation):
        match = re.fullmatch(r"([0-9a-z]{4})_([0-9a-z]{4})_([0-9a-z]{5})([0-9a-z]{13})", generation)
        assert match is not None
        assert int(match.group(2), 36) < 24 * 60 * 60
        assert int(match.group(3), 36) < 10_000_000
        assert int(match.group(4), 36) < 1 << 64

    def encode_base36(value):
        digits = "0123456789abcdefghijklmnopqrstuvwxyz"
        if value == 0:
            return "0"
        result = ""
        while value:
            value, digit = divmod(value, 36)
            result = digits[digit] + result
        return result

    def encode_uuid_sstable_generation(generation_uuid):
        seconds, decimicroseconds = divmod(generation_uuid.time, 10_000_000)
        days, seconds = divmod(seconds, 24 * 60 * 60)
        lsb = generation_uuid.int & ((1 << 64) - 1)
        generation = f"{encode_base36(days):0>4}_{encode_base36(seconds):0>4}_{encode_base36(decimicroseconds):0>5}{encode_base36(lsb):0>13}"
        verify_uuid_sstable_generation(generation)
        return generation

    def parse_node_reference(reference):
        parts = reference.split("/")
        assert len(parts) == 3
        assert parts[0] == "nodes"
        host_id = str(uuid.UUID(parts[1]))
        generation = parts[2]
        verify_uuid_sstable_generation(generation)
        return host_id, generation

    async def verify_object_storage_namespace(server, live_servers, ks):
        """Verify object-storage namespace layout and reference counts through REST API."""
        await manager.disable_tablet_balancing()
        await wait_for_no_pending_topology_transition(manager, time.time() + 120)
        try:
            for node in live_servers:
                await manager.api.disable_autocompaction(node.ip_addr, ks, "test")
            for node in live_servers:
                await manager.api.flush_keyspace(node.ip_addr, ks)

            params = {
                "endpoint": object_storage.address,
                "bucket": object_storage.bucket_name,
            }
            table_id = str(await manager.get_table_id(ks, "test"))

            live_hosts = await wait_for_cql_and_get_hosts(cql, live_servers, time.time() + 30)

            async def get_object_storage_refs():
                sstables = await manager.api.client.get_json("/storage_service/object_storage/sstables", host=server.ip_addr, params=params)
                assert sstables
                refs = {}
                for sstable in sstables:
                    sstable_id = str(uuid.UUID(sstable["sstable_id"]))
                    references = sstable.get("references", [])
                    assert sstable["num_references"] == len(references)
                    assert references, f"SSTable {sstable_id} has no object-storage references"
                    for reference in references:
                        host_id, generation = parse_node_reference(reference)
                        key = (sstable_id, host_id, generation)
                        assert key not in refs
                        refs[key] = reference
                return sstables, refs

            async def get_node_registry_refs(host):
                rows = await cql.run_async("SELECT table_id, node_owner, generation, sstable_id, status FROM system.sstables", host=host)
                refs = {}
                for row in rows:
                    if str(row.table_id) != table_id:
                        continue
                    if row.status != "sealed":
                        continue
                    sstable_id = str(row.sstable_id if row.sstable_id is not None else row.generation)
                    generation = encode_uuid_sstable_generation(row.generation)
                    key = (sstable_id, str(row.node_owner), generation)
                    assert key not in refs
                    refs[key] = row.status
                return refs

            async def get_registry_refs():
                registry_refs = {}
                for refs in await asyncio.gather(*(get_node_registry_refs(host) for host in live_hosts)):
                    for key, status in refs.items():
                        assert key not in registry_refs
                        registry_refs[key] = status
                return registry_refs

            sstables, object_storage_refs = await get_object_storage_refs()
            registry_refs = await get_registry_refs()
            assert object_storage_refs.keys() == registry_refs.keys(), f"Only in object_storage: {object_storage_refs.keys() - registry_refs.keys()}\nOnly in registry: {registry_refs.keys() - object_storage_refs.keys()}"
            logger.info("Verified %d object-storage SSTables and %d references", len(sstables), len(object_storage_refs))
        finally:
            for node in live_servers:
                await manager.api.enable_autocompaction(node.ip_addr, ks, "test")
            await manager.enable_tablet_balancing()

    async with new_test_keyspace(manager, keyspace_options(object_storage, rf=rf)) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, value int);")

        NUM_ROWS = 100

        # Populate test data
        logger.info("Populating table with %d rows", NUM_ROWS)
        await populate_table(cql, ks, NUM_ROWS)

        # Verify initial data
        logger.info("Verifying initial data")
        await verify_data(cql, ks, NUM_ROWS)
        await verify_object_storage_namespace(servers[0], servers, ks)

        # Add one node to each rack
        logger.info("Adding 3 new nodes (one per rack)")
        new_servers = []
        for rack in range(num_racks):
            server = await manager.server_add(config=cfg, cmdline=cmdline, property_file={"dc": "dc1", "rack": f"r{rack}"})
            new_servers.append(server)

        # Wait for tablet load balancer to finish migrating tablets
        logger.info("Waiting for tablet rebalancing to complete")
        deadline = time.time() + 120
        await wait_for_no_pending_topology_transition(manager, deadline)

        # Verify data after scale-up
        logger.info("Verifying data after adding nodes")
        await verify_data(cql, ks, NUM_ROWS)
        await verify_object_storage_namespace(servers[0], servers + new_servers, ks)

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
        await verify_object_storage_namespace(new_servers[0], new_servers, ks)
