#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging
import os
import re

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import EXEC_PROFILE_DEFAULT

from test import DEBUG_MODES
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.cluster.util import cassandra_stress, stop_servers_gracefully

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


# The dtests' TestCountersStress cluster: two nodes, in rack1 and rack2 of
# datacenter1, with cache_hit_rate_read_balancing off; vnodes keyspaces.
STRESS_CONFIG = {"cache_hit_rate_read_balancing": False, "tablets_mode_for_new_keyspaces": "disabled"}
STRESS_TOPOLOGY = [{"dc": "datacenter1", "rack": "rack1"}, {"dc": "datacenter1", "rack": "rack2"}]


def stress_results(output: str) -> dict[str, float]:
    """The values of cassandra-stress's "Results:" section, like ccm's
    node.stress_object(): "total partitions", and with the WRITE or READ
    count in brackets, "total partitions:write" or "total partitions:read"."""
    res = {}
    started = False
    for line in (s.strip() for s in output.splitlines()):
        if not started:
            started = line == "Results:"
            continue
        m = re.match(r"^\s*([^:]+)\s*:\s*(\S.*)\s*$", line)
        if not m:
            continue
        key, val = m.group(1).strip().lower(), m.group(2).strip()
        if "[" in val:
            if m2 := re.match(r"^\s*([\d\.\,]+\d?)\s*\[.*", val):
                res[key] = float(m2.group(1).replace(",", ""))
            if m2 := re.match(r"^.*READ:\s*([\d\.\,]+\d?)[^\d].*", val):
                res[key + ":read"] = float(m2.group(1).replace(",", ""))
            if m2 := re.match(r".*WRITE:\s*([\d\.\,]+\d?)[^\d].*", val):
                res[key + ":write"] = float(m2.group(1).replace(",", ""))
        else:
            try:
                res[key] = float(val.replace(",", ""))
            except ValueError:
                res[key] = val
    return res


# Ported from scylla-dtest counter_tests.py::TestCountersStress::test_counter_stress.
# cassandra-stress counter_write updates 100000 counter rows (10000 in debug
# mode) of keyspace1.counter1 (RF=2, the dtest's schema) through the first
# node; all the rows are counted, then cassandra-stress counter_read reads
# them.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_counter_stress(manager: ScyllaClusterManager, build_mode):
    servers = await manager.servers_add(2, config=STRESS_CONFIG, property_file=STRESS_TOPOLOGY)
    node = servers[0]
    op_cnt = 10000 if build_mode in DEBUG_MODES else 100000
    cql = manager.con_gen([node.ip_addr]).connect()
    # Statements default to consistency level ONE, as in dtest sessions.
    cql.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.ONE
    cql.execute("""
        CREATE KEYSPACE keyspace1
        WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '2'} AND durable_writes = true;
    """)
    cql.execute("""
        CREATE TABLE keyspace1.counter1 (
            key blob PRIMARY KEY,
            "C0" counter,
            "C1" counter,
            "C2" counter,
            "C3" counter,
            "C4" counter
        ) WITH comment = ''
            AND bloom_filter_fp_chance = 0.01
            AND caching = '{"keys":"ALL","rows_per_partition":"ALL"}'
            AND compaction = {'class': 'SizeTieredCompactionStrategy'}
            AND compression = {}
            AND dclocal_read_repair_chance = 0.1
            AND default_time_to_live = 0
            AND gc_grace_seconds = 864000
            AND max_index_interval = 2048
            AND memtable_flush_period_in_ms = 0
            AND min_index_interval = 128
            AND read_repair_chance = 0.0
            AND speculative_retry = '99.0PERCENTILE';
    """)

    resp = stress_results(await cassandra_stress(manager, node, "counter_write", f"n={op_cnt}", "-rate", "threads=4",
                                                 create_keyspace=False))
    assert "total partitions:write" in resp, f"Error running stress test: {resp}"
    assert resp["total partitions:write"] >= op_cnt
    rows = list(cql.execute("SELECT count(*) FROM keyspace1.counter1;"))
    assert rows[0][0] == op_cnt

    resp = stress_results(await cassandra_stress(manager, node, "counter_read", f"n={op_cnt}", "-rate", "threads=4",
                                                 create_keyspace=False))
    assert "total partitions:read" in resp, f"Error running stress test: {resp}"
    assert resp["total partitions:read"] >= op_cnt


@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_counter_user_profile_workload(manager: ScyllaClusterManager, build_mode):
    """Ported from dtest counter_tests.py's test_counter_stress_user_profile:
    cassandra-stress, with the dtest's counter user profile (keyspace ks with
    RF=2, table counter_cf of two counters), updates 100000 rows (10000 in
    debug mode) through the first of two nodes in two racks, with vnodes
    (256 per node) and cache_hit_rate_read_balancing off as in the dtest;
    all the rows are counted, then cassandra-stress reads them with the
    profile's read1 query. The nodes are stopped gracefully at the end, as
    the dtests' teardown did."""
    config = {"cache_hit_rate_read_balancing": False, "tablets_mode_for_new_keyspaces": "disabled", "num_tokens": 256}
    servers = await manager.servers_add(2, config=config, auto_rack_dc="dc1")
    op_cnt = 10000 if build_mode in DEBUG_MODES else 100000
    profile = os.path.join(os.path.dirname(__file__), "..", "resource", "cassandra-stress", "cassandra-stress-custom-counters-1.yaml")

    def total_partitions(output: str) -> int:
        m = re.search(r"^\s*Total partitions\s*:\s*([\d,]+)", output, re.MULTILINE | re.IGNORECASE)
        assert m, f"no total partitions in cassandra-stress output: {output}"
        return int(m.group(1).replace(",", ""))

    try:
        out = await cassandra_stress(manager, servers[0], "user", f"profile={profile}", "ops(insert=1)", f"n={op_cnt}",
                                     "-rate", "threads=4", create_keyspace=False)
        assert total_partitions(out) >= op_cnt
        cql = manager.get_cql()
        assert list(await cql.run_async("SELECT count(*) FROM ks.counter_cf;"))[0][0] == op_cnt
        out = await cassandra_stress(manager, servers[0], "user", f"profile={profile}", "ops(read1=1)", f"n={op_cnt}",
                                     "-rate", "threads=4", create_keyspace=False)
        assert total_partitions(out) >= op_cnt
    finally:
        await stop_servers_gracefully(manager)
