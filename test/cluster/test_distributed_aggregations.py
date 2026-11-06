#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import os
import re
import time

import pytest
from cassandra import ConsistencyLevel

from test.cluster.util import cassandra_stress, run_nodetool
from test.cluster.dtest_helpers import start_node_wait_cql
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts

CS_KEYSPACE, CS_TABLE = "ks", "counter_cf"
CS_PROFILE_PATH = os.path.join(os.path.dirname(__file__), "..", "resource", "cassandra-stress", "cassandra-stress-custom-counters-1.yaml")
OPS = 100_000
CONFIG = {"murmur3_ignore_msb_bits": 1, "num_tokens": 3, "tablets_mode_for_new_keyspaces": "disabled"}
CMDLINE = ["--smp", "4", "-m", "4G"]


def is_regex_in_query_traces(result, regex_pattern) -> bool:
    # Scylla writes the trace to system_traces (RF=2) at CL=ANY, so one
    # replica may miss some of its events: read it at CL=ALL, not the
    # driver's default LOCAL_ONE.
    query_trace = result.response_future.get_query_trace(query_cl=ConsistencyLevel.ALL)
    return any(regex_pattern.search(str(event)) for event in query_trace.events)


def execute_timed_aggregate_query(session, aggregate_query: str):
    start = time.perf_counter()
    row = session.execute(aggregate_query).one()
    return row, time.perf_counter() - start


# Ported from scylla-dtest distributed_queries_test.py::TestDistributedAggregations::test_distributed_count_all.
# Three nodes (racks by RF 2: rack1 x2, rack2) of 4 shards and 4 GB, 3
# tokens each with murmur3_ignore_msb_bits 1, filled with 100000 counter
# updates by a cassandra-stress user profile and flushed, with trace
# probability 1 on node1: SELECT count(*) gives the same result, the number
# of operations, with enable_parallelized_aggregation off and, after the
# cluster restarts with it, on; the traced query dispatches mapreduce
# requests only when it is on.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_distributed_count_all(manager: ScyllaClusterManager):
    aggregate_query = f"SELECT count(*) FROM {CS_KEYSPACE}.{CS_TABLE} USING TIMEOUT 180s"
    servers = await manager.servers_add(3, config={**CONFIG, "enable_parallelized_aggregation": False}, cmdline=CMDLINE,
                                        property_file=[{"dc": "datacenter1", "rack": r} for r in ["rack1", "rack1", "rack2"]])
    node1 = servers[0]
    await cassandra_stress(manager, node1, "user", f"profile={CS_PROFILE_PATH}", "ops(insert=1)", f"n={OPS}", "cl=LOCAL_QUORUM",
                           "-rate", "threads=4", create_keyspace=False)
    await run_nodetool(manager, node1, "settraceprobability", "1.0")
    for s in servers:
        await run_nodetool(manager, s, "flush")
    session = manager.get_cql()

    no_parallelization_result, _ = await asyncio.to_thread(execute_timed_aggregate_query, session, aggregate_query)
    no_parallelization_trace = await asyncio.to_thread(session.execute, aggregate_query, trace=True)
    mapreduce_request_log_line = re.compile(r"Dispatching (forward|mapreduce)_request to 3 endpoints|Dispatching (\d+) ranges")
    # Read the trace before the restart: the session reading it is closed then.
    no_parallelization_forwarded = await asyncio.to_thread(is_regex_in_query_traces, no_parallelization_trace, mapreduce_request_log_line)

    # restart_cluster() with the new option: ccm's cluster.stop() stops the
    # nodes gracefully in parallel (ScyllaCluster.stop()), and the nodes are
    # started together.
    await asyncio.gather(*[manager.server_stop_gracefully(s.server_id) for s in servers])
    for s in servers:
        await manager.server_update_config(s.server_id, "enable_parallelized_aggregation", True, reload=False)
    await asyncio.gather(*[start_node_wait_cql(manager, s) for s in servers])
    await manager.driver_connect()
    session = manager.get_cql()
    await wait_for_cql_and_get_hosts(session, servers, time.time() + 60)

    parallelization_result, _ = await asyncio.to_thread(execute_timed_aggregate_query, session, aggregate_query)
    parallelization_trace = await asyncio.to_thread(session.execute, aggregate_query, trace=True)

    assert parallelization_result[0] == no_parallelization_result[0] == OPS, \
        "Aggregation query results where different with and without parallelized query flag."
    assert not no_parallelization_forwarded, \
        "Tracing record showed forwarding the aggregate requests with 'enable_parallelized_aggregation' set to false."
    assert is_regex_in_query_traces(parallelization_trace, mapreduce_request_log_line), \
        "Tracing record did not include forwarding the aggregate requests with 'enable_parallelized_aggregation' set to true."
