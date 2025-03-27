#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from test.pylib.manager_client import ManagerClient
from test.pylib.tablets import get_tablet_replica
from test.cluster.conftest import skip_mode
import pytest
import logging
import asyncio
import os
import glob
import requests

logger = logging.getLogger(__name__)

def disable_auto_compaction(ip_addr, ks_name, cf_name):
    api_path = f'/column_family/autocompaction/{ks_name}:{cf_name}'
    ret = requests.delete(f'http://{ip_addr}:10000{api_path}')
    if not ret.ok:
        raise RuntimeError(f'failed to disable autocompaction using {api_path}: {ret.text}')

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_resurrection_while_file_streaming(manager: ManagerClient):
    '''
    When tablets are migrated with file-based streaming, we can have a situation where a tombstone is garbage
    collected before the data it shadows lands. For instance, if we have a tablet replica with 3 sstables:
    1: sstable containing an expired tombstone
    2: sstable with additional data
    3: sstable containing data which is shadowed by the expired tombstone in sstable 1
    If this tablet is migrated, and the sstables are streamed in the order listed above, the first two
    sstables can be compacted before the third sstable arrives. In that case, the expired tombstone will
    be garbage collected, and data in the third sstable will be resurrected after it arrives to the
    pending replica.
    The fix for this problem is to disables tombstone garbage collection for pending replicas. 
    '''
    logger.info('Bootstrapping cluster')
    cfg = {'enable_tablets': True}
    servers = [await manager.server_add(config=cfg)]
    servers.append(await manager.server_add(config=cfg))

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    s0_host_id = await manager.get_host_id(servers[0].server_id)
    s1_host_id = await manager.get_host_id(servers[1].server_id)

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int) WITH gc_grace_seconds = 0;")

    # select source and destination replica
    src_replica = await get_tablet_replica(manager, servers[0], 'test', 'test', 0)
    if src_replica[0] == s1_host_id:
        dst_replica = (s0_host_id, 0)
        src_server = servers[1]
        dst_server = servers[0]
    else:
        dst_replica = (s1_host_id, 0)
        src_server = servers[0]
        dst_server = servers[1]

    disable_auto_compaction(src_server.ip_addr, 'test', 'test')

    logger.info(f'src replica {src_replica}')
    logger.info(f'dst replica {dst_replica}')
    logger.info(f'src server ID {src_server.server_id}')
    logger.info(f'dst server ID {dst_server.server_id}')

    async def get_sstable_data_files(server_id):
        files = set()
        node_workdir = await manager.server_get_workdir(server_id)
        table_dir = glob.glob(os.path.join(node_workdir, 'data', 'test', 'test-*'))[0]
        for f in glob.glob(os.path.join(table_dir, '*-Data.db')):
            files.add(os.path.basename(f))
        return files

    # add data which will be deleted below, then flush to make an sstable
    await cql.run_async('INSERT INTO test.test (pk, c) VALUES (1, 1);')
    await manager.api.flush_keyspace(src_server.ip_addr, 'test')

    sst_files = await get_sstable_data_files(src_server.server_id)
    [shadowed_file] = sst_files
    
    # add the tombstone and flush
    await cql.run_async('DELETE FROM test.test WHERE pk = 1;')
    await manager.api.flush_keyspace(src_server.ip_addr, 'test')

    sst_with_tombstone = await get_sstable_data_files(src_server.server_id)
    [tombstone_file] = sst_with_tombstone.difference(sst_files)

    # add another record for the third sstable, which will be
    # compacted with the sstable containing the tombstone
    await cql.run_async(f'INSERT INTO test.test (pk, c) VALUES (100, 100);')
    await manager.api.flush_keyspace(src_server.ip_addr, 'test')

    all_files = await get_sstable_data_files(src_server.server_id)
    [additional_file] = all_files.difference(sst_with_tombstone)

    logger.info(f'tombstone file:       {tombstone_file}')
    logger.info(f'additional data file: {additional_file}')
    logger.info(f'shadowed file:        {shadowed_file}')

    # make sure gc grace has elapsed, so that the tombstone can be garbage collected
    await asyncio.sleep(1)

    # send the name of the file which needs to be streamed last
    await manager.api.enable_injection(src_server.ip_addr, 'order_sstables_for_streaming', one_shot=False, parameters={'shadowed_file': shadowed_file})

    logger.info(f'moving tablet: {src_replica} to {dst_replica}')
    await manager.api.move_tablet(src_server.ip_addr, 'test', 'test', src_replica[0], src_replica[1],
                                                                      dst_replica[0], dst_replica[1], 0)

    rows = await cql.run_async(f'SELECT pk FROM test.test WHERE pk = 1;')
    assert(len(rows) == 0)
