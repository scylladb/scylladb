#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import pytest
import time
import logging

from cassandra import ConsistencyLevel  # type: ignore

from test import DEBUG_MODES
from test.cluster.dtest_helpers import (DTEST_CONFIG, NO_HINTS_BATCH_COMMITLOG, create_cf, create_ks, dtest_session,
                                        insert_c1c2, nodetool, query_c1c2, topology)
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

async def test_boot(manager):
    rbno = True
    cfg = {'enable_repair_based_node_ops': rbno, 'num_tokens': 256}
    nr_nodes = 3
    for i in range(nr_nodes):
        logger.info(f"booting node {i+1}")
        start = time.time()
        await manager.server_add(config=cfg)
        end = time.time()
        logger.info(f"node {i+1} took {end - start}ms to boot")


# Ported from scylla-dtest consistent_bootstrap_test.py::TestBootstrapConsistency::test_consistent_reads_after_bootstrap.
# A bootstrapping node must stream each range from the replica which is
# about to lose it, as that one is guaranteed to have the data, and not from
# an arbitrary replica. With hints disabled, keys 30..999 are written while
# node1 is down, so only node2 has them; node3 then bootstraps in rack2 and
# every key must still be readable from it at CL=ALL.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_bootstrap_streams_from_replica_with_data(manager: ScyllaClusterManager):
    cfg = {**DTEST_CONFIG, **NO_HINTS_BATCH_COMMITLOG, "write_request_timeout_in_ms": 60000,
           "read_request_timeout_in_ms": 60000, "dynamic_snitch_badness_threshold": 0.0}
    node1, node2 = await manager.servers_add(2, config=cfg, property_file=topology({"rack1": 1, "rack2": 1}))

    n2session = dtest_session(await manager.get_cql_exclusive(node2))
    create_ks(n2session, "ks", 2)
    n2session.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    n2session.execute("USE ks")
    create_cf(n2session, "cf", {"c1": "text", "c2": "text"})
    insert_c1c2(n2session, keys=range(10, 20), consistency=ConsistencyLevel.ALL)

    await nodetool(manager, node1, "flush")
    await manager.server_stop_gracefully(node1.server_id)
    await manager.others_not_see_server(node1.ip_addr)

    insert_c1c2(n2session, keys=range(30, 1000), consistency=ConsistencyLevel.ONE)
    await nodetool(manager, node2, "flush")

    await manager.server_start(node1.server_id)
    await manager.servers_see_each_other([node1, node2])

    # In rack2: the keyspace must stay RF-rack-valid.
    node3 = await manager.server_add(config=cfg, property_file={"dc": "datacenter1", "rack": "rack2"})
    n3session = dtest_session(await manager.get_cql_exclusive(node3))
    n3session.execute("USE ks")
    for n in range(10, 20):
        query_c1c2(n3session, n, ConsistencyLevel.ALL)
    for n in range(30, 1000):
        query_c1c2(n3session, n, ConsistencyLevel.ALL)
