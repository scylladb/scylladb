#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import os
import time
import pytest
import asyncio
import logging
import subprocess
from pathlib import Path

from test.pylib.util import wait_for_feature, wait_for_cql_and_get_hosts
from test.pylib.scylla_cluster import ScyllaVersionDescription
from test.pylib.manager_client import ManagerClient
from test.cluster.util import wait_for, create_new_test_keyspace


logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def scylla_path():
    return os.getenv("SCYLLA")


async def create_system_key(scylla_path, system_key_file):
    """
    Create a system key using the scylla local-file-key-generator command.
    """
    command = [
        scylla_path,
        'local-file-key-generator',
        'generate',
        '-a', 'AES',
        '-b', 'CBC',
        '-p', 'PKCS5',
        '-l', '128',
        system_key_file
    ]
    subprocess.run(command, check=True)


async def get_replicated_key_provider_version(cql, host=None):
    query = f"SELECT value FROM system.scylla_local WHERE key='replicated_key_provider_version'"
    result = await cql.run_async(query, host=host)
    if len(result) == 0:
        return 1
    else:
        return int(result[0].value) // 10

async def replicated_key_provider_version_is_v2(cql, host=None):
    version = await get_replicated_key_provider_version(cql, host)
    return version == 2

async def create_keyspace(cql):
    ks_options = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
    return await create_new_test_keyspace(cql, ks_options)


async def table_exists(cql, keyspace, table_name, host=None):
    query = f"SELECT table_name FROM system_schema.tables WHERE keyspace_name='{keyspace}' AND table_name='{table_name}'"
    result = await cql.run_async(query, host=host)
    return len(result) > 0


async def create_encrypted_table(cql, keyspace, table_name, schema, secret_key_dir, key_strength=128, host=None):
    """
    Create a table that is encrypted with the Replicated Key Provider.
    """
    query = f"""CREATE TABLE {keyspace}.{table_name} ({schema}) WITH
                scylla_encryption_options = {{
                    'cipher_algorithm' :  'AES/CBC/PKCS5Padding',
                    'secret_key_strength' : {key_strength},
                    'key_provider': 'ReplicatedKeyProviderFactory',
                    'system_key_file': 'system_key',
                    'secret_key_file': '{str(secret_key_dir / 'data_encryption_keys')}'
                }}
            """
    await cql.run_async(query, host=host)


@pytest.mark.asyncio
async def test_encrypted_keys_v2_table(manager: ManagerClient, scylla_path, tmpdir):
    """
    Test that the Replicated Key Provider uses the group0 table on a fresh cluster.
    Specifically:
    - Verify that the `replicated_key_provider_version` is v2.
    - Verify that the v2 table exists.
    - Create an encrypted table, insert some data and flush the SSTables on all nodes.
    - Verify that a key has been created in the v2 table.
    - Verify that the v1 table is empty.
    - Verify that we can read from the SSTables.
    - Restart all nodes and verify that we can still read from the encrypted table.
    """

    v1_keyspace, v1_table = 'system_replicated_keys', 'encrypted_keys'
    v2_keyspace, v2_table = 'system', 'encrypted_keys'
    node_count = 3

    logger.info("Creating system key")
    system_key_dir = tmpdir / 'system_keys'
    system_key_dir.mkdir()
    system_key_file = system_key_dir / 'system_key'
    await create_system_key(scylla_path, str(system_key_file))

    logger.info(f"Creating cluster with {node_count} nodes")
    cfg = {'system_key_directory': str(system_key_dir)}
    servers = await manager.servers_add(node_count, config=cfg)
    cql, hosts = await manager.get_ready_cql(servers)

    logger.info("Verifying `replicated_key_provider_version` is v2 on all nodes")
    for host in hosts:
        version = await get_replicated_key_provider_version(cql, host)
        assert version == 2, f'Expected replicated_key_provider_version to be 2, got {version} on host {host}'

    logger.info(f"Verifying existence of {v2_keyspace}.{v2_table}")
    await table_exists(cql, v2_keyspace, v2_table, hosts[0])

    logger.info("Creating a table encrypted with Replicated Key Provider")
    ks = await create_keyspace(cql)
    table_name = 't'
    await create_encrypted_table(cql, ks, table_name, secret_key_dir=tmpdir, schema='pk int PRIMARY KEY, c int', host=hosts[0])

    logger.info("Inserting data into the encrypted table")
    query = f'INSERT INTO {ks}.{table_name} (pk, c) VALUES (1, 1)'
    await cql.run_async(query)

    logger.info("Flushing table")
    await asyncio.gather(*[manager.api.keyspace_flush(host.address, ks, table_name) for host in hosts])

    logger.info(f"Validating that a key has been created in {v2_keyspace}.{v2_table}")
    query = f'SELECT COUNT(*) FROM {v2_keyspace}.{v2_table}'
    result = await cql.run_async(query)
    assert result[0].count == 1, f"Expected 1 key in {v2_keyspace}.{v2_table}, got {result[0].count}"

    logger.info(f"Validating that {v1_keyspace}.{v1_table} is empty")
    query = f'SELECT COUNT(*) FROM {v1_keyspace}.{v1_table}'
    result = await cql.run_async(query)
    assert result[0].count == 0

    logger.info("Verifying that reading from the encrypted table succeeds")
    query = f'SELECT * FROM {ks}.{table_name} WHERE pk=1'
    result = await cql.run_async(query)
    assert len(result) == 1

    logger.info("Restarting all nodes in the cluster")
    await asyncio.gather(*[manager.server_restart(server.server_id) for server in servers])

    logger.info("Verifying that reading from the encrypted table still works after restart")
    query = f'SELECT * FROM {ks}.{table_name} WHERE pk=1'
    result = await cql.run_async(query)
    assert len(result) == 1


@pytest.mark.asyncio
async def test_encrypted_keys_migration_to_v2(manager: ManagerClient, scylla_path: str, scylla_2025_1: ScyllaVersionDescription, tmpdir: Path):
    """
    Test that encrypted keys are migrated from v1 to v2 table when upgrading.
    Specifically:
    - Start a cluster with an older Scylla version (2025.1) that uses v1 table.
    - Create an encrypted table and verify a key is created in the v1 table.
    - Verify that `replicated_key_provider_version` is v1 on all nodes.
    - Upgrade the cluster to the latest Scylla version.
    - Wait for the ENCRYPTED_KEYS_ON_GROUP0 feature to be enabled.
    - Wait for `replicated_key_provider_version` to become v2 on all nodes.
    - Verify that keys have been migrated to the v2 table.
    - Verify that reading from the encrypted table succeeds after the migration.
    - Create a second encrypted table to verify that a new key is created in v2.
    """
    v1_keyspace, v1_table = 'system_replicated_keys', 'encrypted_keys'
    v2_keyspace, v2_table = 'system', 'encrypted_keys'
    node_count = 3

    logger.info("Creating system key")
    system_key_dir = tmpdir / 'system_keys'
    system_key_dir.mkdir()
    system_key_file = system_key_dir / 'system_key'
    await create_system_key(scylla_path, str(system_key_file))

    logger.info(f"Creating cluster with {node_count} nodes using Scylla 2025.1")
    cfg = {'system_key_directory': str(system_key_dir)}
    servers = await manager.servers_add(node_count, config=cfg, version=scylla_2025_1)
    cql, hosts = await manager.get_ready_cql(servers)

    logger.info("Creating a table encrypted with Replicated Key Provider")
    ks = await create_keyspace(cql)
    table_name = 't1'
    await create_encrypted_table(cql, ks, table_name, secret_key_dir=tmpdir, schema='pk int PRIMARY KEY, c int', key_strength=128, host=hosts[0])

    logger.info("Inserting data into the encrypted table")
    query = f'INSERT INTO {ks}.{table_name} (pk, c) VALUES (1, 1)'
    await cql.run_async(query)

    logger.info("Flushing table")
    await asyncio.gather(*[manager.api.keyspace_flush(host.address, ks, table_name) for host in hosts])

    logger.info(f"Validating that a key has been created in {v1_keyspace}.{v1_table}")
    query = f'SELECT * FROM {v1_keyspace}.{v1_table}'
    v1_keys = await cql.run_async(query)
    assert len(v1_keys) == 1, f"Expected 1 key in {v1_keyspace}.{v1_table}, got {len(v1_keys)}"

    logger.info("Verifying `replicated_key_provider_version` is v1 on all nodes")
    for host in hosts:
        version = await get_replicated_key_provider_version(cql, host)
        assert version == 1, f'Expected replicated_key_provider_version to be 1, got {version} on host {host}'

    logger.info("Upgrading cluster to latest Scylla version")
    await asyncio.gather(*[manager.server_change_version(server.server_id, scylla_path) for server in servers])
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logger.info("Waiting for ENCRYPTED_KEYS_ON_GROUP0 cluster feature to be enabled on all nodes")
    await asyncio.gather(*(wait_for_feature("ENCRYPTED_KEYS_ON_GROUP0", cql, host, time.time() + 60) for host in hosts))

    logger.info("Waiting for `replicated_key_provider_version` to become v2 on all nodes")
    await asyncio.gather(*(wait_for(lambda: replicated_key_provider_version_is_v2(cql, host=h), time.time() + 60) for h in hosts))

    logger.info(f"Validating that keys have been migrated to {v2_keyspace}.{v2_table}")
    query = f'SELECT * FROM {v2_keyspace}.{v2_table}'
    v2_keys = await cql.run_async(query)
    assert len(v2_keys) == 1, f"Expected {len(v1_keys)} keys in {v2_keyspace}.{v2_table}, got {len(v2_keys)}"
    assert v2_keys[0].key_file == v1_keys[0].key_file, "Key files do not match between v1 and v2 tables"
    assert v2_keys[0].cipher == v1_keys[0].cipher, "Key ciphers do not match between v1 and v2 tables"
    assert v2_keys[0].strength == v1_keys[0].strength, "Key strengths do not match between v1 and v2 tables"
    assert v2_keys[0].key_id == v1_keys[0].key_id, "Key IDs do not match between v1 and v2 tables"
    assert v2_keys[0].key == v1_keys[0].key, "Key materials do not match between v1 and v2 tables"

    logger.info(f"Verifying that reading from the encrypted table succeeds after the migration")
    query = f'SELECT * FROM {ks}.{table_name} WHERE pk=1'
    result = await cql.run_async(query)
    assert len(result) == 1, f"Expected to read 1 row from encrypted table, got {len(result)}"

    # Create a second encrypted table (different key strength) to generate a new key in the v2 table
    logger.info("Creating second encrypted table with different key strength (256)")
    table_name = 't2'
    await create_encrypted_table(cql, ks, table_name, secret_key_dir=tmpdir, schema='pk int PRIMARY KEY, c int', key_strength=256, host=hosts[0])

    logger.info("Inserting data into the second encrypted table")
    await cql.run_async(f'INSERT INTO {ks}.{table_name} (pk, c) VALUES (1, 1)')

    logger.info("Flushing second encrypted table")
    await asyncio.gather(*[manager.api.keyspace_flush(h.address, ks, table_name) for h in hosts])

    logger.info(f"Validating that a second key has been created in {v2_keyspace}.{v2_table}")
    v2_keys = await cql.run_async(f'SELECT * FROM {v2_keyspace}.{v2_table}')
    assert len(v2_keys) == 2, f"Expected 2 keys in {v2_keyspace}.{v2_table}, got {len(v2_keys)}"
    strengths = {k.strength for k in v2_keys}
    assert strengths == {128, 256}, f"Expected key strengths 128 and 256, got {strengths}"