#!/usr/bin/env python3

import asyncio
import os
import pytest
import shutil
import logging
import json
import time
import uuid

from test.pylib.minio_server import MinioServer
from cassandra.protocol import ConfigurationException
from cassandra.query import SimpleStatement, ConsistencyLevel
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts
from test.cluster.util import reconnect_driver
from test.pylib.object_storage import format_tuples, keyspace_options, GSFront
from test.cqlpy.rest_api import scylla_inject_error
from test.cluster.test_config import wait_for_config
from test.cluster.util import new_test_keyspace, wait_for_token_ring_and_group0_consistency
from test.pylib.tablets import get_all_tablet_replicas
from test.pylib.skip_types import skip_bug

logger = logging.getLogger(__name__)


async def assert_registry_empty_on_all_nodes(cql, hosts, table_id, action):
    """Verify that sstables registry has no entries for the given table_id on all nodes.

    system.sstables is a node-local table, so each node only stores entries for
    the tablets it owns. Querying every node verifies each one cleaned up its own
    registry after the schema change.
    """
    for h in hosts:
        res = await cql.run_async(
            SimpleStatement(f"SELECT * FROM system.sstables WHERE table_id = {table_id} ALLOW FILTERING",
                            consistency_level=ConsistencyLevel.ONE), host=h)
        assert not res, f'Unexpected entries in registry on {h.address} after {action}'


async def get_table_id(cql, ks, table_name):
    """Get the table UUID from system_schema."""
    rows = await cql.run_async(
        f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = '{table_name}'")
    return rows[0].id


async def get_registry_entries(cql, table_id, node_owner, host):
    """Get sstables registry entries for a specific (table_id, node_owner) partition.

    system.sstables is a node-local table: entries with node_owner=N only exist in
    node N's own system.sstables. The query must therefore be routed to that node
    via the `host` parameter, otherwise it hits the coordinator's local table and
    returns nothing.
    """
    if isinstance(node_owner, str):
        node_owner = uuid.UUID(node_owner)
    rows = await cql.run_async(
        SimpleStatement(
            "SELECT generation, status FROM system.sstables"
            " WHERE table_id = %s AND node_owner = %s",
            consistency_level=ConsistencyLevel.ONE),
        parameters=[table_id, node_owner], host=host)
    return rows


@pytest.mark.parametrize('replication_factor', [1, 3])
@pytest.mark.parametrize('mode', ['normal', 'encrypted'])
async def test_basic(manager: ManagerClient, object_storage, tmp_path, mode, replication_factor):
    '''verify ownership table is updated, and tables written to object storage can be read after scylla restarts.
    Parametrized over replication_factor to also verify RF=3 with multiple servers.'''

    objconf = object_storage.create_endpoint_conf()
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options']}
    if mode == 'encrypted':
        d = tmp_path / "system_keys"
        d.mkdir()
        cfg = cfg | {
            'system_key_directory': str(d),
            'user_info_encryption': { 'enabled': True, 'key_provider': 'LocalFileSystemKeyProviderFactory' }
        }

    servers = []
    for rack_num in range(replication_factor):
        property_file = {"dc": "dc1", "rack": f"r{rack_num}"}
        server = await manager.server_add(config=cfg, property_file=property_file)
        servers.append(server)

    cql = manager.get_cql()
    workdir = await manager.server_get_workdir(servers[0].server_id)
    print(f'Create keyspace (storage server listening at {object_storage.address})')
    async with new_test_keyspace(manager, keyspace_options(object_storage, rf=replication_factor)) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (name text PRIMARY KEY, value int);")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (name, value) VALUES ('{k}', {k});") for k in range(4)])

        assert not os.path.exists(os.path.join(workdir, f'data/{ks}')), "object storage backed keyspace has local directory created"
        # Sanity check that the path is constructed correctly
        assert os.path.exists(os.path.join(workdir, 'data/system')), "Datadir is elsewhere"

        desc = cql.execute(f"DESCRIBE KEYSPACE {ks}").one().create_statement
        # The storage_opts wraps options with '{ <options> }' while the DESCRIBE
        # does it like '{<options>}' so strip the corner branches and spaces for check
        assert f"{{'type': '{object_storage.type}', 'bucket': '{object_storage.bucket_name}', 'endpoint': '{object_storage.address}'}}" in desc, "DESCRIBE generates unexpected storage options"

        res = cql.execute(f"SELECT * FROM {ks}.test;")
        rows = {x.name: x.value for x in res}
        assert len(rows) > 0, 'Test table is empty'

        print('Flush keyspace on all servers')
        for server in servers:
            await manager.api.flush_keyspace(server.ip_addr, ks)

        # Check that the ownership table is populated properly
        tid = cql.execute(f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = 'test'").one()
        res = cql.execute("SELECT * FROM system.sstables;")
        for row in res:
            assert row.table_id == tid.id, \
                f'Unexpected entry table_id in registry: {row.table_id}'
            assert row.status == 'sealed', f'Unexpected entry status in registry: {row.status}'

        if replication_factor > 1:
            tablet_replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
            print(f'Found {len(tablet_replicas)} tablets')
            hosts = {await manager.get_host_id(s.server_id): s.ip_addr for s in servers}
            for idx, tablet in enumerate(tablet_replicas):
                replica_ips = [hosts[r[0]] for r in tablet.replicas]
                print(f'Tablet {idx}: last_token={tablet.last_token}, replicas={replica_ips}')
                assert len(replica_ips) == replication_factor, f'Expected RF={replication_factor} replicas, got {len(replica_ips)}'

        print('Restart scylla')
        for server in servers:
            await manager.server_restart(server.server_id)
        cql = await reconnect_driver(manager)

        # Shouldn't be recreated by populator code
        assert not os.path.exists(os.path.join(workdir, f'data/{ks}')), "object storage backed keyspace has local directory resurrected"

        stmt = SimpleStatement(f"SELECT * FROM {ks}.test;", consistency_level=ConsistencyLevel.ALL)
        res = cql.execute(stmt)
        have_res = {x.name: x.value for x in res}
        assert have_res == rows, f'Unexpected table content: {have_res}'

async def test_garbage_collect(manager: ManagerClient, object_storage):
    '''verify ownership table is garbage-collected on boot'''

    sstable_entries = []

    objconf = object_storage.create_endpoint_conf()
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options']}
    cmd = ['--logger-log-level', 's3=trace:http=debug:gcp_storage=trace']
    server = await manager.server_add(config=cfg, cmdline=cmd)

    cql = manager.get_cql()

    print(f'Create keyspace (storage server listening at {object_storage.address})')
    async with new_test_keyspace(manager, keyspace_options(object_storage)) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (name text PRIMARY KEY, value int);")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (name, value) VALUES ('{k}', {k});") for k in range(4)])

        await manager.api.flush_keyspace(server.ip_addr, ks)
        # Mark the sstables as "removing" to simulate the problem
        res = cql.execute("SELECT * FROM system.sstables;")
        for row in res:
            sstable_entries.append((row.table_id, row.node_owner, row.generation))
        print(f'Found entries: {[ str(ent[2]) for ent in sstable_entries ]}')
        for table_id, node_owner, gen in sstable_entries:
            cql.execute("UPDATE system.sstables SET status = 'removing'"
                         f" WHERE table_id = {table_id} AND node_owner = {node_owner} AND generation = {gen};")

        print('Restart scylla')
        await manager.server_restart(server.server_id)
        cql = await reconnect_driver(manager)

        res = cql.execute(f"SELECT * FROM {ks}.test;")
        have_res = {x.name: x.value for x in res}
        # Must be empty as no sstables should have been picked up
        assert not have_res, f'Sstables not cleaned, got {have_res}'
        # Make sure objects also disappeared
        objects = object_storage.get_resource().Bucket(object_storage.bucket_name).objects.all()
        print(f'Found objects: {[ objects ]}')
        for o in objects:
            for ent in sstable_entries:
                assert not o.key.startswith(str(ent[2])), f'Sstable object not cleaned, found {o.key}'


async def test_populate_from_quarantine(manager: ManagerClient, object_storage):
    '''verify sstables are populated from quarantine state'''

    objconf = object_storage.create_endpoint_conf()
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options']}
    server = await manager.server_add(config=cfg)

    cql = manager.get_cql()

    print(f'Create keyspace (storage server listening at {object_storage.address})')
    async with new_test_keyspace(manager, keyspace_options(object_storage)) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (name text PRIMARY KEY, value int);")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (name, value) VALUES ('{k}', {k});") for k in range(4)])

        res = cql.execute(f"SELECT * FROM {ks}.test;")
        rows = {x.name: x.value for x in res}
        assert len(rows) > 0, 'Test table is empty'

        await manager.api.flush_keyspace(server.ip_addr, ks)
        # Move the sstables into "quarantine"
        res = cql.execute("SELECT * FROM system.sstables;")
        assert len(list(res)) > 0, 'No entries in registry'
        for row in res:
            cql.execute("UPDATE system.sstables SET state = 'quarantine'"
                         f" WHERE table_id = {row.table_id} AND node_owner = {row.node_owner} AND generation = {row.generation};")

        print('Restart scylla')
        await manager.server_restart(server.server_id)
        cql = await reconnect_driver(manager)

        res = cql.execute(f"SELECT * FROM {ks}.test;")
        have_res = {x.name: x.value for x in res}
        # Quarantine entries must have been processed normally
        assert have_res == rows, f'Unexpected table content: {have_res}'


async def test_misconfigured_storage(manager: ManagerClient, object_storage):
    '''creating keyspace with unknown endpoint is not allowed'''
    # scylladb/scylladb#15074
    objconf = object_storage.create_endpoint_conf()
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options']}
    server = await manager.server_add(config=cfg)

    cql = manager.get_cql()
    print(f'Create keyspace (storage server listening at {object_storage.address})')
    replication_opts = format_tuples({'class': 'NetworkTopologyStrategy',
                                      'replication_factor': '1'})
    storage_opts = format_tuples(type=f'{object_storage.type}',
                                 endpoint='unknown_endpoint',
                                 bucket=object_storage.bucket_name)

    with pytest.raises(ConfigurationException):
        cql.execute((f"CREATE KEYSPACE test_ks WITH"
                      f" REPLICATION = {replication_opts} AND STORAGE = {storage_opts};"))


async def test_memtable_flush_retries(manager: ManagerClient, tmpdir, object_storage):
    '''verify that memtable flush doesn't crash in case storage access keys are incorrect'''

    print('Spoof the object-store config')
    objconf = object_storage.create_endpoint_conf()

    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options']}
    server = await manager.server_add(config=cfg)

    cql = manager.get_cql()
    print(f'Create keyspace (storage server listening at {object_storage.address})')

    async with new_test_keyspace(manager, keyspace_options(object_storage)) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (name text PRIMARY KEY, value int);")
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (name, value) VALUES ('{k}', {k});") for k in range(4)])

        res = cql.execute(f"SELECT * FROM {ks}.test;")
        rows = {x.name: x.value for x in res}

        with scylla_inject_error(cql, "s3_client_fail_authorization"):
            print(f'Flush keyspace')
            flush = asyncio.create_task(manager.api.flush_keyspace(server.ip_addr, ks))
            print(f'Wait few seconds')
            await asyncio.sleep(8)

        print(f'Wait for flush to finish')
        await flush

        print(f'Check the sstables table')
        res = cql.execute("SELECT * FROM system.sstables;")
        ssts = "\n".join(f"{row.table_id} {row.generation} {row.status}" for row in res)
        print(f'sstables:\n{ssts}')

        print('Restart scylla')
        await manager.server_restart(server.server_id)
        cql = await reconnect_driver(manager)

        res = cql.execute(f"SELECT * FROM {ks}.test;")
        have_res = { x.name: x.value for x in res }
        assert have_res == dict(rows), f'Unexpected table content: {have_res}'

@pytest.mark.parametrize('config_with_full_url', [True, False])
async def test_get_object_store_endpoints(manager: ManagerClient, config_with_full_url):
    if config_with_full_url:
        objconf = MinioServer.create_conf('http://a:123', 'region')
    else:
        objconf = MinioServer.create_conf('a', 'region')
        objconf[0]["port"] = 123
        objconf[0]["use_https"] = False
        del objconf[0]["type"]

    cfg = {'object_storage_endpoints': objconf}

    print('Scylla returns the object storage endpoints')
    server = await manager.server_add(config=cfg)
    endpoints = await manager.api.get_config(server.ip_addr, 'object_storage_endpoints')

    name = objconf[0]['name']
    del objconf[0]['name']

    print('Also check the returned string is valid JSON')
    assert name in endpoints
    assert json.loads(endpoints[name]) == objconf[0]

    print('Check that system.config contains the object storage endpoints')
    cql = manager.get_cql()
    res = json.loads(cql.execute("SELECT value FROM system.config WHERE name = 'object_storage_endpoints';").one().value)
    assert name in res
    assert json.loads(res[name]) == objconf[0]


async def test_create_keyspace_after_config_update(manager: ManagerClient, object_storage):
    print('Trying to create a keyspace with an endpoint not configured in object_storage_endpoints should trip storage_manager::is_known_endpoint()')
    server = await manager.server_add()
    cql = manager.get_cql()
    endpoint = object_storage.address  
    replication_opts = format_tuples({'class': 'NetworkTopologyStrategy',
                                      'replication_factor': '1'})
    storage_opts = format_tuples(type=f'{object_storage.type}',
                                 endpoint=endpoint,
                                 bucket=object_storage.bucket_name)

    with pytest.raises(ConfigurationException):
        cql.execute((f'CREATE KEYSPACE random_ks WITH'
                      f' REPLICATION = {replication_opts} AND STORAGE = {storage_opts};'))

    print('Update config with a new endpoint and SIGHUP Scylla to reload configuration')
    objconf = object_storage.create_endpoint_conf()
    await manager.server_update_config(server.server_id, 'object_storage_endpoints', objconf)
    ep = objconf[0]
    if ep['type'] == 's3':
        expected_conf = f'{{ "type": "s3", "aws_region": "{ep["aws_region"]}", "iam_role_arn": "{ep["iam_role_arn"]}" }}'
    else:
        expected_conf = f'{{ "type": "gs", "credentials_file": "{ep["credentials_file"]}" }}'
    await wait_for_config(manager, server, 'object_storage_endpoints', {ep['name']: expected_conf})

    print('Passing a known endpoint will make the CREATE KEYSPACE stmt to succeed')
    cql.execute((f'CREATE KEYSPACE random_ks WITH'
                    f' REPLICATION = {replication_opts} AND STORAGE = {storage_opts};'))

    print('Create a table, insert data and flush the keyspace to force object_storage_client creation')
    cql.execute(f'CREATE TABLE random_ks.test (name text PRIMARY KEY, value int);')
    await cql.run_async(f"INSERT INTO random_ks.test (name, value) VALUES ('test_key', 123);")
    await manager.api.flush_keyspace(server.ip_addr, 'random_ks')
    res = cql.execute(f"SELECT value FROM random_ks.test WHERE name = 'test_key';")
    assert res.one().value == 123, f'Unexpected value after flush: {res.one().value}'

    # Now that a live object_storage_client exists for this endpoint, push a
    # config update that modifies the endpoint parameters.  This exercises the
    # update_config_sync path on an already-instantiated client
    print('Push a config update to reconfigure the live object_storage_client')
    updated_objconf = object_storage.create_endpoint_conf()
    updated_ep = updated_objconf[0]
    if updated_ep['type'] == 's3':
        updated_ep['aws_region'] = 'updated-region'
        updated_expected_conf = f'{{ "type": "s3", "aws_region": "{updated_ep["aws_region"]}", "iam_role_arn": "{updated_ep["iam_role_arn"]}" }}'
    else:
        updated_ep['credentials_file'] = ''
        updated_expected_conf = f'{{ "type": "gs", "credentials_file": "{updated_ep["credentials_file"]}" }}'
        skip_bug(
            link="https://scylladb.atlassian.net/browse/SCYLLADB-1559",
            reason="Flaky test due to race condition closing the GCS object storage client while operations are in flight",
        )

    await manager.server_update_config(server.server_id, 'object_storage_endpoints', updated_objconf)
    await wait_for_config(manager, server, 'object_storage_endpoints', {updated_ep['name']: updated_expected_conf})

    print('Verify the reconfigured client still works: insert more data and flush')
    await cql.run_async(f"INSERT INTO random_ks.test (name, value) VALUES ('after_reconfig', 456);")
    await manager.api.flush_keyspace(server.ip_addr, 'random_ks')
    res = cql.execute(f"SELECT value FROM random_ks.test WHERE name = 'after_reconfig';")
    assert res.one().value == 456, f'Unexpected value after reconfiguration flush: {res.one().value}'

    print('Verify all data is intact')
    rows = {r.name: r.value for r in cql.execute(f'SELECT * FROM random_ks.test;')}
    assert rows == {'test_key': 123, 'after_reconfig': 456}, f'Unexpected table content: {rows}'


async def test_tablet_move_updates_registry(manager: ManagerClient, object_storage):
    """
    Verify that moving a tablet from one node to another correctly
    updates the (node-local) sstables registry: the destination node
    creates new entries (status=sealed) in its own system.sstables and
    the source node's entries are cleaned up.

    Uses a 2-node cluster with RF=1 and a single tablet to ensure
    deterministic placement and movement.
    """
    if isinstance(object_storage, GSFront):
        skip_bug(link="https://scylladb.atlassian.net/browse/SCYLLADB-2044",
                 reason="fake-gcs-server ignores ifGenerationMatch, causing HTTP 416 during tablet streaming")
    cfg = {
        'object_storage_endpoints': object_storage.create_endpoint_conf(),
        'experimental_features': ['keyspace-storage-options']
    }
    servers = await manager.servers_add(2, config=cfg)
    await manager.disable_tablet_balancing()
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 30)
    host_by_ip = {h.address: h for h in hosts}

    host_ids = {}
    driver_host = {}
    for s in servers:
        host_ids[s] = await manager.get_host_id(s.server_id)
        driver_host[s] = host_by_ip[str(s.rpc_address)]

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
        src_entries = await get_registry_entries(cql, table_id, src_host_id, host=driver_host[src_server])
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

        # Verify destination's local registry has entries (status=sealed)
        dst_entries = await get_registry_entries(cql, table_id, dst_host_id, host=driver_host[dst_server])
        assert len(dst_entries) > 0, \
            f"Destination {dst_host_id} should have registry entries after move"
        for entry in dst_entries:
            assert entry.status == 'sealed', \
                f"Destination entry {entry.generation} has status '{entry.status}', expected 'sealed'"
        logger.info(f"Destination {dst_host_id} has {len(dst_entries)} sealed registry entries")

        # Verify source entries are cleaned up
        src_entries_after = await get_registry_entries(cql, table_id, src_host_id, host=driver_host[src_server])
        assert len(src_entries_after) == 0, \
            f"Source {src_host_id} should have no registry entries after move, got {len(src_entries_after)}"
        logger.info("Source registry entries cleaned up successfully")


async def test_decommission_migrates_registry(manager: ManagerClient, object_storage):
    """
    Verify registry behavior around decommission.
    This test checks that the tablet owned by the decommissioned node is migrated to the surviving node,
    which then builds its own local registry entries (status=sealed).

    Uses a 2-node cluster with RF=1 and a single tablet so the owner is unambiguous; the
    owner is decommissioned to force a migration.
    """
    if isinstance(object_storage, GSFront):
        skip_bug(link="https://scylladb.atlassian.net/browse/SCYLLADB-2044",
                 reason="fake-gcs-server ignores ifGenerationMatch, causing HTTP 416 during tablet streaming")
    cfg = {
        'object_storage_endpoints': object_storage.create_endpoint_conf(),
        'experimental_features': ['keyspace-storage-options']
    }
    servers = await manager.servers_add(2, config=cfg)

    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 30)
    host_by_ip = {h.address: h for h in hosts}

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

        # Identify the tablet owner and decommission it, forcing the tablet to migrate
        # to the surviving node.
        replicas = await get_all_tablet_replicas(manager, servers[0], ks, 't1')
        assert len(replicas) == 1, f"Expected 1 tablet, got {len(replicas)}"
        assert len(replicas[0].replicas) == 1, f"Expected RF=1, got {len(replicas[0].replicas)}"
        owner_host_id = replicas[0].replicas[0][0]
        decom_server = next(s for s in servers if host_ids[s] == owner_host_id)
        remaining_server = next(s for s in servers if s is not decom_server)
        remaining_host_id = host_ids[remaining_server]

        # The owner has local registry entries before decommission.
        owner_entries = await get_registry_entries(cql, table_id, owner_host_id,
                                                    host=host_by_ip[str(decom_server.rpc_address)])
        assert len(owner_entries) > 0, "Owner should have registry entries before decommission"
        logger.info(f"Owner {owner_host_id} has {len(owner_entries)} registry entries before decommission")

        # Decommission the owner node
        logger.info(f"Decommissioning owner {decom_server} (host_id={owner_host_id})")
        await manager.decommission_node(decom_server.server_id)
        await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

        # Re-resolve the surviving node's CQL host after the topology change.
        remaining_hosts = await wait_for_cql_and_get_hosts(cql, [remaining_server], time.time() + 30)
        remaining_host = remaining_hosts[0]

        # The tablet migrated to the surviving node, which must have built its own
        # local registry entries (status=sealed).
        entries = await get_registry_entries(cql, table_id, remaining_host_id, host=remaining_host)
        assert len(entries) > 0, \
            f"Surviving node {remaining_host_id} should have registry entries after migration"
        for e in entries:
            assert e.status == 'sealed', \
                f"Entry {e.generation} has status '{e.status}', expected 'sealed'"
        logger.info(f"Surviving node {remaining_host_id} has {len(entries)} sealed registry entries")

        # Data remains readable from the surviving node.
        rows = await cql.run_async(
            SimpleStatement(f"SELECT * FROM {ks}.t1", consistency_level=ConsistencyLevel.ONE),
            host=remaining_host)
        assert len(rows) == 10, f"Expected 10 rows, got {len(rows)}"


async def test_repair_creates_registry_entries(manager: ManagerClient, object_storage):
    """
    Verify that non-incremental (tablet) repair on an object-storage keyspace
    creates sstables registry entries on the repaired node via streaming.

    To attribute the new entries to repair -- and not to a memtable flush of
    data the node already had -- the destination must genuinely lack the data
    before repair:

      * dst is stopped while the rows are written (at CL=ONE to src) and src
        is flushed, so the data lives only in src's SSTables;
      * hinted handoff is disabled so dst is not silently repopulated when it
        restarts;
      * after dst restarts, its local system.sstables is empty.

    Repair on dst then streams the data from src, writing a new sealed SSTable
    on dst, which must appear in dst's registry *without any flush*. If repair
    were a no-op, dst's registry would stay empty.

    FIXME: incremental repair is not supported on object-storage keyspaces
    because object_storage_base does not implement link_with_excluded_components().
    Regular tablet repair uses streaming which works correctly.
    """
    if isinstance(object_storage, GSFront):
        skip_bug(link="https://scylladb.atlassian.net/browse/SCYLLADB-2044",
                 reason="fake-gcs-server ignores ifGenerationMatch, causing HTTP 416 during tablet streaming")
    cfg = {
        'object_storage_endpoints': object_storage.create_endpoint_conf(),
        'experimental_features': ['keyspace-storage-options'],
        'rf_rack_valid_keyspaces': False,
        'hinted_handoff_enabled': False,
    }

    servers = await manager.servers_add(2, config=cfg)
    # Keep tablet placement deterministic so dst's missing replica is only
    # filled in by the explicit repair below, not by background balancing.
    await manager.disable_tablet_balancing()

    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 30)
    host_by_ip = {h.address: h for h in hosts}

    src_node = servers[0]
    dst_node = servers[1]
    src_host_id = await manager.get_host_id(src_node.server_id)
    dst_host_id = await manager.get_host_id(dst_node.server_id)
    src_host = host_by_ip[str(src_node.rpc_address)]

    ks_opts = keyspace_options(object_storage, rf=2)
    async with new_test_keyspace(manager, ks_opts) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t1 (pk int PRIMARY KEY, v int)")
        table_id = await get_table_id(cql, ks, 't1')

        # Stop dst so the writes land only on src. With hinted handoff disabled,
        # dst will not be repopulated on restart, so the only way it can later
        # obtain the data (and a registry entry) is through repair streaming.
        await manager.server_stop_gracefully(dst_node.server_id)

        for i in range(10):
            await cql.run_async(
                SimpleStatement(f"INSERT INTO {ks}.t1 (pk, v) VALUES ({i}, {i})",
                                consistency_level=ConsistencyLevel.ONE),
                host=src_host)
        await manager.api.flush_keyspace(src_node.ip_addr, ks)

        # Bring dst back and refresh its CQL host handle.
        await manager.server_start(dst_node.server_id, wait_others=1)
        hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
        host_by_ip = {h.address: h for h in hosts}
        dst_host = host_by_ip[str(dst_node.rpc_address)]

        src_entries = await get_registry_entries(cql, table_id, src_host_id, host=src_host)
        assert len(src_entries) > 0, "Source should have registry entries after flush"
        logger.info(f"Source {src_host_id} has {len(src_entries)} registry entries")

        # dst was down during the writes (hints disabled), so it has no SSTables
        # and therefore no local registry entries.
        dst_entries_before = await get_registry_entries(cql, table_id, dst_host_id, host=dst_host)
        assert len(dst_entries_before) == 0, \
            f"Destination should have no registry entries before repair, got {len(dst_entries_before)}"

        # Run non-incremental tablet repair on dst. This streams the missing data
        # from src and writes a new sealed SSTable on dst.
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

        # Repair streams a complete sealed SSTable, so the registry entry must
        # appear on dst.
        dst_entries = await get_registry_entries(cql, table_id, dst_host_id, host=dst_host)
        assert len(dst_entries) > 0, \
            f"Destination {dst_host_id} should have registry entries created by repair"
        for e in dst_entries:
            assert e.status == 'sealed', \
                f"Entry {e.generation} has status '{e.status}', expected 'sealed'"
        logger.info(f"Destination {dst_host_id} has {len(dst_entries)} sealed entries after repair")

        # Verify data consistency
        rows = await cql.run_async(f"SELECT * FROM {ks}.t1")
        assert len(rows) == 10, f"Expected 10 rows, got {len(rows)}"


@pytest.mark.parametrize('operation', ['truncate', 'drop_table', 'drop_keyspace'])
async def test_registry_cleanup_on_all_nodes(manager: ManagerClient, object_storage, operation):
    """
    Verify that TRUNCATE, DROP TABLE and DROP KEYSPACE on an object-storage
    backed table clean up the sstables registry entries on all nodes.

    Uses a 2-node RF=2 cluster so both nodes own the data
    and therefore both have local system.sstables entries that the operation
    must remove.
    """
    cfg = {
        'object_storage_endpoints': object_storage.create_endpoint_conf(),
        'experimental_features': ['keyspace-storage-options'],
    }
    servers = await manager.servers_add(2, config=cfg,
                                        property_file=[{"dc": "dc1", "rack": "r0"},
                                                       {"dc": "dc1", "rack": "r1"}])

    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    async with new_test_keyspace(manager, keyspace_options(object_storage, rf=2)) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int)")
        for k in range(4):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({k}, {k})")
        for server in servers:
            await manager.api.flush_keyspace(server.ip_addr, ks)

        table_id = await get_table_id(cql, ks, 'test')

        for h in hosts:
            res = await cql.run_async(
                SimpleStatement(f"SELECT * FROM system.sstables WHERE table_id = {table_id} ALLOW FILTERING",
                                consistency_level=ConsistencyLevel.ONE), host=h)
            assert res, f"Expected registry entries on {h.address} before {operation}"

        if operation == 'truncate':
            await cql.run_async(f"TRUNCATE {ks}.test")
        elif operation == 'drop_table':
            await cql.run_async(f"DROP TABLE {ks}.test")
        else:
            await cql.run_async(f"DROP KEYSPACE {ks}")

        await assert_registry_empty_on_all_nodes(cql, hosts, table_id, operation)

