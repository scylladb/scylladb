#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.pylib.manager_client import ManagerClient

import pytest
import time
import logging
import glob
import os

from test.topology.util import reconnect_driver
from test.pylib.util import wait_for_view

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_mv_loss_attrs_exception(manager: ManagerClient) -> None:
    node_count = 2
    servers = await manager.servers_add(node_count)
    cql, _ = await manager.get_ready_cql(servers)
    # `durable_writes = false` to avoid replay commitlog
    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND durable_writes = false AND tablets = {'enabled': false};")
    # Create a table with non-primary key columns as collections
    await cql.run_async("""CREATE TABLE ks.tab (base_key int, view_key int, ":attrs" map<text, text>, PRIMARY KEY (base_key, view_key)) WITH gc_grace_seconds = 1 AND tombstone_gc = {'mode': 'timeout'};""")
    # Create a MV
    await cql.run_async("CREATE MATERIALIZED VIEW ks.mv_cf_view AS SELECT * FROM ks.tab WHERE view_key IS NOT NULL and base_key IS NOT NULL PRIMARY KEY (view_key, base_key);")

    await wait_for_view(cql, 'mv_cf_view', node_count)

    # Simulate Alternator to perform the operation of deleting first and then writing
    await cql.run_async("DELETE FROM ks.tab WHERE base_key = 5 AND view_key = 5;")
    await cql.run_async('INSERT INTO ks.tab (base_key, view_key, ":attrs") VALUES (5, 5, {\'attr1\': \'aaaaa\'});')

    # waite for gc_grace_seconds
    # Sometimes tombstones are not cleaned up occasionally, 
    # which may be due to the system clock problem in the test environment. 
    # Here, to ensure that tombstones are cleaned up, set 5s. 
    time.sleep(5)

    for server in servers:
        await manager.api.keyspace_flush(server.ip_addr, 'ks')

    # when node A compacted, the row inlcue liveness_info and deletion_info
    # Node A is compacted first, but node B has not yet been compacted.
    await manager.api.keyspace_compaction(servers[0].ip_addr, 'ks', 'tab')

    # query the MV will be found the item
    for row in cql.execute(f"SELECT * FROM ks.mv_cf_view WHERE base_key = 5 AND view_key = 5;"):
        assert row.base_key == 5
        assert row.view_key == 5
        assert row.attrs['attr1'] == 'aaaaa'

    def delete_files_with_prefix(directory, prefix='me-'):
        for filename in os.listdir(directory):
            if filename.startswith(prefix):
                file_path = os.path.join(directory, filename)
                try:
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                        print(f"Deleted: {file_path}")
                except Exception as e:
                    print(f"Error deleting {file_path}: {e}")

    # stop cluster to delete MV sstables
    for server in servers:
        await manager.server_stop(server.server_id)

    # Simulate MV asynchronous write failure
    for server in servers:
        node_workdir = await manager.server_get_workdir(server.server_id)
        mv_dir = glob.glob(os.path.join(node_workdir, "data", "ks", "mv_cf_view-*"))[0]
        delete_files_with_prefix(mv_dir)
    
    # start scylla, If `durable_writes = true`, then the commitlog will be replayed at startup, 
    # causing the sstables of the deleted MV to be regenerated.
    # So we specify `durable_writes = false` when creating ks.
    for server in servers:
        await manager.server_start(server.server_id)

    cql = await reconnect_driver(manager)

    # At this time, the MV should be empty.
    assert len(list(cql.execute(f"SELECT * FROM ks.mv_cf_view WHERE base_key = 5 AND view_key = 5;"))) == 0

    # repair node A base table will generate MV data
    await manager.api.repair(servers[0].ip_addr, 'ks', 'tab')

    # After flushing, you can parse the MV's sstable locally and find that the attrs attribute is missing.
    for server in servers:
        await manager.api.keyspace_flush(server.ip_addr, 'ks')

    # At this time, the MV should have one row.
    assert len(list(cql.execute(f"SELECT * FROM ks.mv_cf_view WHERE base_key = 5 AND view_key = 5;"))) == 1

    # When querying MV, you will find that the value of attrs is missing
    # by `assert row.attrs == None`. But, it should be ('attr1', 'aaaaa'),
    # so we need to fix this bug by compare_atomic_cell_for_merge(c.second, it->second) >= 0
    for row in cql.execute(f"SELECT * FROM ks.mv_cf_view WHERE base_key = 5 AND view_key = 5;"):
        assert row.base_key == 5
        assert row.view_key == 5
        assert row.attrs['attr1'] == 'aaaaa'

    await cql.run_async(f"DROP KEYSPACE ks")
