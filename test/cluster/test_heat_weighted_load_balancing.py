#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Heat-weighted load balancing of CL=QUORUM reads: with warm caches the three
replicas serve similar shares of a cassandra-stress read load; after one of
them restarts with a cold cache, it serves less, and more as its cache hit
rate grows.

Ported from scylla-dtest heat_weighted_load_balancing_test.py. Like the
dtests: three nodes (one per rack) with vnodes (256 per node) and keyspace
and table metrics, the cassandra-stress table created up front (LCS, RF=3),
5000 rows written at the tested CL, every node flushed, and 60 one-second
samples of the nodes' metrics under a one-minute read load before and after
the restart. As in the dtests, the statistical checks are skipped in debug
mode. The nodes are stopped gracefully at the end of each test, as the
dtests' teardown did.
"""

import asyncio
import logging
import re
import time

import pytest

from test import DEBUG_MODES
from test.cluster.test_counters import stress_results
from test.cluster.util import cassandra_stress, run_nodetool, stop_servers_gracefully
from test.cqlpy.util import run_cqlsh_at
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

CONFIG = {'tablets_mode_for_new_keyspaces': 'disabled', 'num_tokens': 256, 'enable_keyspace_column_family_metrics': True}
READS = "scylla_storage_proxy_coordinator_reads_local_node"
HIT_RATE = "scylla_column_family_cache_hit_rate.*cf=.*standard1"
METRICS = [READS, HIT_RATE]
OP_CNT = 5000
METRICS_COUNT = 60


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


async def node_metrics(manager: ScyllaClusterManager, server, names) -> dict:
    """The dtests' get_node_metrics(): the sum of the values of the metric
    lines matching each regular expression."""
    res = {k: 0 for k in names}
    for line in (await manager.metrics.query(server.ip_addr)).lines:
        if line.startswith("#"):
            continue
        for name in names:
            if re.search(name, line):
                res[name] += float(line.split()[-1])
    return res


async def wait_for_read_load(manager, servers, read_task, min_delta=50, timeout=300, debug=False) -> None:
    """Wait until every node serves at least min_delta reads per second.
    Starting cassandra-stress can take minutes on a loaded machine."""
    prev = {i: None for i in range(3)}
    deltas = {}
    deadline = time.time() + timeout
    while time.time() < deadline and not read_task.done():
        t = time.time()
        deltas = {}
        for i, s in enumerate(servers):
            val = (await node_metrics(manager, s, [READS]))[READS]
            deltas[i] = val - prev[i] if prev[i] is not None else 0
            prev[i] = val
        if all(d >= min_delta for d in deltas.values()):
            return
        await asyncio.sleep(max(0, 1 - (time.time() - t)))
    if read_task.done():
        read_task.result()
    msg = f"Read load was not established: fewer than {min_delta} reads/s on at least one node, last per-node deltas: {deltas}"
    if debug:
        logger.warning(msg)
        return
    raise AssertionError(msg)


async def sample_metrics(manager, servers) -> dict:
    """METRICS_COUNT samples, a second apart, of each node's metrics: their
    values and their growth since the previous sample."""
    samples = {k: {i: [] for i in range(3)} for k in METRICS}
    for _ in range(METRICS_COUNT):
        t = time.time()
        for i, s in enumerate(servers):
            for k, v in (await node_metrics(manager, s, METRICS)).items():
                delta = v - samples[k][i][-1]["val"] if samples[k][i] else 0
                samples[k][i].append(dict(val=v, delta=delta))
        await asyncio.sleep(max(0, 1 - (time.time() - t)))
    return samples


def eligible_window(series: dict, field: str, zero, min_count: int = 30) -> tuple[int, int]:
    """The first window of at least min_count samples where no node's value
    in `field` is `zero`."""
    first, last = None, len(series[1])
    for i in range(last):
        if all(series[n][i][field] != zero for n in range(3)):
            if first is None:
                first = i
        elif first is not None:
            if i - first >= min_count:
                last = i
                break
            first = None
    assert first is not None, "Did not find eligible samples range"
    assert last - first >= min_count, f"Expected at least {min_count} eligible samples, found {last - first}"
    return first, last


def verify_metrics(metrics: dict, cached: bool = True) -> None:
    """The dtests' verify_metrics(): node2 (index 1) is the restarted one."""
    reads = metrics[READS]
    first, last = eligible_window(reads, "delta", 0)
    first += 10
    last -= 10
    if cached:
        for i in range(first + 1, last - 1):
            for n in (0, 2):
                ratio = reads[n][i]["delta"] / reads[1][i]["delta"]
                assert 0.25 <= ratio <= 4, f"reads of node{n + 1} and node2 at {i}: ratio {ratio}"
    else:
        mean_window = 5
        last -= mean_window
        for i in range(first, last):
            for n in (0, 2):
                mean = sum(reads[n][j]["delta"] for j in range(i, i + mean_window)) / mean_window
                node2_mean = sum(reads[1][j]["delta"] for j in range(i, i + mean_window)) / mean_window
                ratio = mean / node2_mean
                assert 0.8 <= ratio <= 11 + 2 * (last - i) / (last - first), f"reads of node{n + 1} and node2 at {i}: ratio {ratio}"
    hit = metrics[HIT_RATE]
    first, last = eligible_window(hit, "val", 0.0)
    first += 5
    last -= 5
    last_drop = None
    for i in range(first, last):
        for n in (0, 2):
            if cached:
                assert hit[n][i]["val"] == hit[1][i]["val"]
            else:
                assert hit[n][i]["val"] >= hit[1][i]["val"]
        if not cached:
            ratio = hit[1][i]["val"] / hit[1][i - 1]["val"]
            if ratio < 1.0:
                # one slight drop at most, then a plateau
                assert ratio >= 0.98
                assert last_drop is None
                last_drop = i
            elif ratio > 1.0:
                last_drop = None
    if not cached:
        values = [hit[1][i]["val"] for i in range(first, last)]
        min_pos = first + values.index(min(values))
        max_pos = first + values.index(max(values))
        assert max_pos > min_pos
        assert (first + last) / 2 > min_pos
        assert max_pos >= (first + last) / 2


async def run_heat_weighted_load_balancing(manager: ScyllaClusterManager, build_mode, cl):
    """The dtests' run_heat_weighted_load_balancing()."""
    debug = build_mode in DEBUG_MODES
    manager.ignore_log_patterns += [r"sstable read queue overloaded"]
    servers = await manager.servers_add(3, config=CONFIG, property_file=[{"dc": "datacenter1", "rack": f"rack{i}"} for i in range(1, 4)])
    node1 = servers[0]
    cql = manager.get_cql()
    # The dtests' create_stress_compatible_table().
    await cql.run_async("CREATE KEYSPACE keyspace1 WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3 };")
    await cql.run_async("""CREATE TABLE keyspace1.standard1(key blob PRIMARY KEY, "C0" blob, "C1" blob, "C2" blob, "C3" blob, "C4" blob)
        WITH bloom_filter_fp_chance = 0.01 AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'} AND comment = ''
        AND compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': '100'} AND compression = {}
        AND crc_check_chance = 1.0 AND default_time_to_live = 0 AND gc_grace_seconds = 864000 AND max_index_interval = 2048
        AND memtable_flush_period_in_ms = 0 AND min_index_interval = 128 AND speculative_retry = '99.0PERCENTILE';""")
    res = await asyncio.to_thread(run_cqlsh_at, node1.ip_addr, 9042, "DESCRIBE SCHEMA; // post-line comment")
    logger.debug(res.stdout)
    out = await cassandra_stress(manager, node1, "write", f"cl={cl}", f"n={OP_CNT}", "-schema", "replication(factor=3)", "-rate", "threads=4",
                                 "-pop", f"seq=1..{OP_CNT}", create_keyspace=False)
    assert "total partitions:write" in stress_results(out), f"Error running stress test: {out}"
    for s in servers:
        await run_nodetool(manager, s, "flush")

    async def run_read():
        out = await cassandra_stress(manager, node1, "read", "cl=QUORUM", "duration=1m", "-schema", "replication(factor=3)",
                                     "-rate", "threads>=4", "threads<=64", "-pop", f"seq=1..{OP_CNT}", create_keyspace=False)
        if "total partitions:read" not in stress_results(out):
            raise Exception(f"Error running stress test: {out}")

    def read_load():
        return asyncio.create_task(run_read())

    for cached in (True, False):
        if not cached:
            await manager.server_stop_gracefully(servers[1].server_id)
            await manager.others_not_see_server(servers[1].ip_addr)
            await manager.server_start(servers[1].server_id)
            await manager.servers_see_each_other(servers)
        read = read_load()
        await wait_for_read_load(manager, servers, read, debug=debug)
        metrics = await sample_metrics(manager, servers)
        if read.done():
            read.result()
        if not debug:
            verify_metrics(metrics, cached=cached)
        await read


GATE = [pytest.mark.tier3, pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")]


# Ported from scylla-dtest heat_weighted_load_balancing_test.py::TestHeatWeightedLB::test_heat_weighted_load_balancing_cl_two.
@GATE[0]
@GATE[1]
async def test_heat_weighted_load_balancing_cl_two(manager: ScyllaClusterManager, build_mode):
    await run_heat_weighted_load_balancing(manager, build_mode, "TWO")


# Ported from scylla-dtest heat_weighted_load_balancing_test.py::TestHeatWeightedLB::test_heat_weighted_load_balancing_cl_any.
@GATE[0]
@GATE[1]
async def test_heat_weighted_load_balancing_cl_any(manager: ScyllaClusterManager, build_mode):
    await run_heat_weighted_load_balancing(manager, build_mode, "ANY")


# Ported from scylla-dtest heat_weighted_load_balancing_test.py::TestHeatWeightedLB::test_heat_weighted_load_balancing_cl_quorum.
@GATE[0]
@GATE[1]
async def test_heat_weighted_load_balancing_cl_quorum(manager: ScyllaClusterManager, build_mode):
    await run_heat_weighted_load_balancing(manager, build_mode, "QUORUM")
