#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Ported from scylla-dtest read_amplification_test.py: reads and repairs
# do not read or transfer much more than the size of the data, judged by
# node-wide counters - so each test needs a cluster of its own.

import asyncio
import logging
import math
import time
from collections import defaultdict

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from test import DEBUG_MODES
from test.cluster.dtest_helpers import create_cf, create_ks, dtest_session, insert_c1c2
from test.cluster.test_paging import PageFetcher
from test.cluster.util import reconnect_driver, run_nodetool
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

KBYTE = 1024
PARTITION_READ = "partition"
SCAN_READ = "scan"
# The dtests' nodes: vnodes keyspaces with 256 tokens per node.
CONFIG = {"tablets_mode_for_new_keyspaces": "disabled", "num_tokens": 256}


async def get_metrics(manager: ScyllaClusterManager, metric_names, servers) -> dict:
    """The dtests' get_metrics_safely(): each metric summed over the nodes
    (and their shards), 0 if not exposed."""
    metrics = {n: 0 for n in metric_names}
    for s in servers:
        node_metrics = await manager.metrics.query(s.ip_addr)
        for k in metrics:
            metrics[k] += node_metrics.get(k) or 0
    logger.debug(metrics)
    return metrics


def prepare_ks_cf(cql, rf: int) -> None:
    """create_ks(session, "ks", rf) (with its USE) and create_cf(session, "cf",
    read_repair=0.0, columns={"c1": "text", "c2": "text"})."""
    create_ks(cql, "ks", rf)
    cql.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    cql.execute("USE ks")
    create_cf(cql, "cf", {"c1": "text", "c2": "text"}, read_repair=0.0)


async def no_read_amplification_on_repair(manager: ScyllaClusterManager, with_mv: bool, count: int = 500) -> None:
    """TestReadAmplification.no_read_amplification_on_repair()."""
    config = {**CONFIG, "hinted_handoff_enabled": False, "compaction_enforce_min_threshold": True}
    servers = await manager.servers_add(4, config=config, property_file=[
        {"dc": "dc1", "rack": "rack1"}, {"dc": "dc1", "rack": "rack1"}, {"dc": "dc1", "rack": "rack2"}, {"dc": "dc1", "rack": "rack3"}])
    cql = dtest_session(manager.get_cql())
    prepare_ks_cf(cql, 3)
    if with_mv:
        cql.execute("CREATE MATERIALIZED VIEW ks.cf_mv AS SELECT * FROM ks.cf WHERE key is not null and c1 is not null PRIMARY KEY (c1, key)")
        cql.execute("ALTER MATERIALIZED VIEW ks.cf_mv WITH read_repair_chance=0.0")

    keys = range(1, 100)
    insert_c1c2(cql, keys=keys, consistency=ConsistencyLevel.ALL, c1_values=["value1"] * len(keys), c2_values=["value2"] * len(keys))

    node_to_repair = servers[1]
    await manager.server_stop_gracefully(node_to_repair.server_id)
    await manager.others_not_see_server(node_to_repair.ip_addr)

    # The dtest uses count / 5 in debug mode, where this test does not run.
    cnt = count
    size = 2 * KBYTE
    one_kb = "a" * 1024
    insert_c1c2(cql, keys=range(1, cnt + 1), consistency=ConsistencyLevel.QUORUM, c1_values=[one_kb] * cnt,
                c2_values=[one_kb] * cnt, concurrency=10)

    await manager.server_start(node_to_repair.server_id)
    await manager.servers_see_each_other(servers)

    metric_names = ["scylla_repair_rx_row_bytes", "scylla_repair_tx_row_bytes"]
    start_metrics = await get_metrics(manager, metric_names, servers)

    async def diffs():
        end = await get_metrics(manager, metric_names, servers)
        return {k: end[k] - start_metrics[k] for k in end}

    # ccm's node.repair(local=True, keyspace="ks", tables=["cf"]).
    repair = asyncio.create_task(run_nodetool(manager, node_to_repair, "repair", "ks", "cf", "--in-local-dc"))
    amplification_rate = 3
    deadline = time.time() + 600
    while not repair.done():
        for value in (await diffs()).values():
            assert 0 <= value < size * cnt * amplification_rate
        assert time.time() < deadline, "repair timed out after 600 seconds"
        await asyncio.sleep(1)
    await repair

    for key, value in (await diffs()).items():
        assert 0 < value < size * cnt * amplification_rate
        logger.info(f"{key}: {value}(+{int(math.fabs(value - (cnt * size)) * 100 / (cnt * size))}%)")


# Ported from scylla-dtest read_amplification_test.py::TestReadAmplification::test_no_read_amplification_on_repair.
# Four nodes in three racks, RF=3: a node that missed 500 rows of 2 KB is
# repaired, and the repair row bytes sent and received stay below three
# times the missed data, during and after the repair.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_no_read_amplification_on_repair(manager: ScyllaClusterManager):
    await no_read_amplification_on_repair(manager, with_mv=False)


# Ported from scylla-dtest read_amplification_test.py::TestReadAmplification::test_no_read_amplification_on_repair_with_mv.
# The same with a materialized view on the table.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_no_read_amplification_on_repair_with_mv(manager: ScyllaClusterManager):
    await no_read_amplification_on_repair(manager, with_mv=True)


async def read_amplification(manager: ScyllaClusterManager, read_type: str, read_size: int, wait_interval: float = 1,
                             max_ratio_expected: float = 10) -> None:
    """TestReadAmplification.read_amplification(): one node (populate(1)),
    read_size bytes of 2 KB rows written at CL=ONE, flushed and compacted,
    the node restarted to empty the cache; then all rows read - by key or
    with a paged scan - and the bytes read from disk (once the counters
    settled after the restart) at most max_ratio_expected times the data."""
    server = await manager.server_add(config={**CONFIG, "endpoint_snitch": "SimpleSnitch"},
                                      property_file={"dc": "datacenter1", "rack": "rack1"})
    cql = dtest_session(manager.get_cql())
    prepare_ks_cf(cql, 1)
    size = KBYTE * 2
    cnt = read_size // size
    logger.info(f"count: {cnt}")
    insert_c1c2(cql, keys=range(cnt), consistency=ConsistencyLevel.ONE, c1_values=["a" * KBYTE] * cnt, c2_values=["b" * KBYTE] * cnt)
    await run_nodetool(manager, server, "flush")
    await run_nodetool(manager, server, "compact")
    await manager.server_stop_gracefully(server.server_id)
    await manager.server_start(server.server_id)
    cql = dtest_session(await reconnect_driver(manager))
    await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)

    metric_names = ["scylla_reactor_aio_bytes_read", "scylla_reactor_aio_reads"]
    # Post-boot disk reads must not count: wait until the AIO counters stop moving for 4 s.
    before = await get_metrics(manager, metric_names, [server])
    stable = 0
    deadline = time.time() + 60
    while stable < 4:
        if time.time() >= deadline:
            logger.warning(f"AIO read counters did not settle within 60s after restart, using {before}")
            break
        await asyncio.sleep(1)
        current = await get_metrics(manager, metric_names, [server])
        stable = stable + 1 if current == before else 0
        before = current

    def run_read():
        for idx in range(cnt):
            result = list(cql.execute(f"SELECT c1,c2 FROM ks.cf where key = 'k{idx}'"))
            assert len(result) == 1, f'The expected result size is "1" (got "{len(result)}")'

    def run_scan_read():
        # The dtest's PageFetcher polls for each page every 0.1 s, so a scan
        # of many pages runs long enough for the later pages (which reuse
        # the first page's client timestamp) to drift past 33.5 s.
        future = cql.execute_async(SimpleStatement("select * from ks.cf", fetch_size=25))
        page_fetcher = PageFetcher(future).request_all(timeout=30)
        assert sum(page_fetcher.num_results_all()) == cnt

    def total_read_bytes(metrics):
        return metrics["scylla_reactor_aio_bytes_read"] - before["scylla_reactor_aio_bytes_read"]

    reader = asyncio.create_task(asyncio.to_thread(run_read if read_type == PARTITION_READ else run_scan_read))
    total = 0
    while not reader.done() or total == 0:
        total = total_read_bytes(await get_metrics(manager, metric_names, [server]))
        await asyncio.sleep(wait_interval)
    await reader

    total = total_read_bytes(await get_metrics(manager, metric_names, [server]))
    ampl = total / (cnt * size)
    logger.info(f"Read amplification for {read_size} bytes: {ampl} times")
    assert ampl <= max_ratio_expected, f"Read amplification is too large: {ampl} times"


# Ported from scylla-dtest read_amplification_test.py::TestReadAmplification::test_no_amplification_on_read_20kb.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_no_amplification_on_read_20kb(manager: ScyllaClusterManager):
    await read_amplification(manager, PARTITION_READ, KBYTE * 20, 1, 20)


# Ported from scylla-dtest read_amplification_test.py::TestReadAmplification::test_no_amplification_on_read_400kb.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_no_amplification_on_read_400kb(manager: ScyllaClusterManager):
    await read_amplification(manager, PARTITION_READ, KBYTE * 400)


# Ported from scylla-dtest read_amplification_test.py::TestReadAmplification::test_no_amplification_on_read_20mb.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_no_amplification_on_read_20mb(manager: ScyllaClusterManager):
    await read_amplification(manager, PARTITION_READ, KBYTE * KBYTE * 20)


# Ported from scylla-dtest read_amplification_test.py::TestReadAmplification::test_no_amplification_on_scanning_read_20kb.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_no_amplification_on_scanning_read_20kb(manager: ScyllaClusterManager):
    await read_amplification(manager, SCAN_READ, KBYTE * 20)


# Ported from scylla-dtest read_amplification_test.py::TestReadAmplification::test_no_amplification_on_scanning_read_2mb.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_no_amplification_on_scanning_read_2mb(manager: ScyllaClusterManager):
    await read_amplification(manager, SCAN_READ, KBYTE * KBYTE * 2)


# Ported from scylla-dtest read_amplification_test.py::TestReadAmplification::test_no_amplification_on_scanning_read_20mb.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_no_amplification_on_scanning_read_20mb(manager: ScyllaClusterManager):
    await read_amplification(manager, SCAN_READ, KBYTE * KBYTE * 20)


# A single-token read creates a shard reader only on the shard owning the
# token, on every replica (#8161), judged by the tracing events of the
# read: on one node, and on three nodes (two racks) with RF=2. Like the
# dtests, the nodes have two shards and 1 GB (test.py's default), use vnodes, keep no
# partitions in the cache (max_cached_partition_size_in_bytes 1), hold ten
# partitions of four 1 MB blobs, and are restarted to empty the cache
# before the traced read.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("racks", [["rack1"], ["rack1", "rack1", "rack2"]], ids=["1node", "3nodes"])
async def test_create_reader_on_one_shard(manager: ScyllaClusterManager, racks):
    rf = 1 if len(racks) == 1 else 2
    servers = await manager.servers_add(len(racks), config={"tablets_mode_for_new_keyspaces": "disabled", "num_tokens": 256, "max_cached_partition_size_in_bytes": 1},
                                        cmdline=["--smp", "2"],
                                        property_file=[{"dc": "dc1", "rack": r} for r in racks])
    cql = manager.get_cql()
    await cql.run_async(f"CREATE KEYSPACE ks WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}}")
    await cql.run_async("CREATE COLUMNFAMILY ks.cf (key varchar PRIMARY KEY, c1 blob, c2 blob, c3 blob, c4 blob) WITH comment='test cf' "
                        "AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'} AND compression = {}")
    await cql.run_async("SELECT * FROM ks.cf LIMIT 1")
    value = "a" * 1000000
    insert = cql.prepare(f"INSERT INTO ks.cf (key, c1, c2, c3, c4) VALUES (?, textAsBlob('{value}'), textAsBlob('{value}'), "
                         f"textAsBlob('{value}'), textAsBlob('{value}'))")
    insert.consistency_level = ConsistencyLevel.QUORUM
    await asyncio.gather(*[cql.run_async(insert, [f"k{k}"]) for k in range(10)])
    for s in servers:
        await manager.api.flush_all_keyspaces(s.ip_addr)
    [row] = await cql.run_async("select token(key) from ks.cf where key = 'k9'")
    for s in servers:
        await manager.server_stop_gracefully(s.server_id)
        await manager.server_start(s.server_id)
    cql = await reconnect_driver(manager)
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    query = f"select count(*) from ks.cf where token(key) = {row[0]}"
    await cql.run_async(query, trace=True)
    session_ids = []
    for _ in range(30):
        # At CL=ONE, as the dtest's session: system_traces has RF=2.
        sessions = await cql.run_async(SimpleStatement("select session_id, parameters from system_traces.sessions",
                                                       consistency_level=ConsistencyLevel.ONE))
        session_ids = [s.session_id for s in sessions if s.parameters["query"] == query]
        if session_ids:
            break
        await asyncio.sleep(1)
    assert session_ids, "no tracing session of the query"
    shards_by_source = defaultdict(set)
    for _ in range(30):
        events = list(await cql.run_async(SimpleStatement(f"select source, activity, thread from system_traces.events where session_id = {session_ids[0]}",
                                                          consistency_level=ConsistencyLevel.ONE)))
        for activity in ["Creating shard reader on shard", "scheduling bulk DMA read", "Reading partition range"]:
            for event in events:
                if activity in event.activity:
                    shards_by_source[event.source].update(event.thread.split("/")[0].replace("shard ", ""))
        if shards_by_source:
            break
        await asyncio.sleep(1)
    assert shards_by_source, "no reader shards in the tracing events"
    many = {source: shards for source, shards in shards_by_source.items() if len(shards) > 1}
    assert not many, f"readers created on more than one shard: {many}"
