#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import time

from cassandra import ConsistencyLevel, ReadFailure, Unavailable

from test import DEBUG_MODES
from test.cluster.dtest_helpers import create_ks
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts
import pytest


# Ported from scylla-dtest cql_session_test.py::TestCqlSession::test_data_integrity_with_down_nodes.
# Four nodes in dc1 (rack1 x2, rack2, rack3), RF 3, the dtests' create_cf
# table (key, c, v text): 100 partitions of 100 rows written at QUORUM and
# read back at QUORUM; then each node in turn is killed, the rows read
# (retrying Unavailable/ReadFailure up to 5 times, 1 s apart), one more row
# written to every partition plus 25 new partitions, and after the node is
# back every partition holds the expected number of rows.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_data_integrity_with_down_nodes(manager: ScyllaClusterManager):
    rf, num_keys, additional_keys, num_rows, additional_rows = 3, 100, 25, 100, 1
    keyspace_name, table_name = "test_keyspace", "test_table"
    expected_rows = {}
    servers = await manager.servers_add(4, config={"tablets_mode_for_new_keyspaces": "disabled", "num_tokens": 256},
                                        property_file=[{"dc": "dc1", "rack": "rack1"}, {"dc": "dc1", "rack": "rack1"},
                                                       {"dc": "dc1", "rack": "rack2"}, {"dc": "dc1", "rack": "rack3"}])
    cluster = manager.con_gen([s.ip_addr for s in servers])
    session = cluster.connect()
    try:
        create_ks(session, keyspace_name, rf)
        session.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
        session.execute(f"CREATE COLUMNFAMILY {keyspace_name}.{table_name} (key varchar, c varchar, v varchar, PRIMARY KEY(key, c)) "
                        "WITH comment='test cf' AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'} AND compression = {}")

        def insert_data(start_key, num_keys, start_row, num_rows):
            query = session.prepare(f"INSERT INTO {keyspace_name}.{table_name} (key, c, v) VALUES (?, ?, ?)")
            query.consistency_level = ConsistencyLevel.QUORUM
            futures = {k: [session.execute_async(query, [f"k{k}", f"c{i}", f"v{i}"]) for i in range(start_row, start_row + num_rows)]
                       for k in range(start_key, start_key + num_keys)}
            for k, fs in futures.items():
                for f in fs:
                    assert len(list(f.result())) == 0
                expected_rows[k] = expected_rows.get(k, 0) + num_rows

        def verify_data(start_key, num_keys):
            query = session.prepare(f"SELECT * FROM {keyspace_name}.{table_name} WHERE key = ?")
            query.consistency_level = ConsistencyLevel.QUORUM
            for attempt in range(5):
                try:
                    futures = {k: session.execute_async(query, [f"k{k}"]) for k in range(start_key, start_key + num_keys)}
                    for k, f in futures.items():
                        res = list(f.result())
                        assert len(res) == expected_rows[k], res
                    return
                except (Unavailable, ReadFailure):
                    if attempt == 4:
                        raise
                    time.sleep(1)

        await asyncio.to_thread(insert_data, 0, num_keys, 0, num_rows)
        await asyncio.to_thread(verify_data, 0, num_keys)
        for server in servers:
            await manager.server_stop(server.server_id, convict=False)
            await asyncio.to_thread(verify_data, 0, num_keys)
            await asyncio.to_thread(insert_data, 0, num_keys + additional_keys, num_rows, additional_rows)
            num_keys += additional_keys
            num_rows += additional_keys
            await manager.server_start(server.server_id)
            await manager.servers_see_each_other(servers)
            await wait_for_cql_and_get_hosts(session, servers, time.time() + 60)
            await asyncio.to_thread(verify_data, 0, num_keys)
    finally:
        cluster.shutdown()
