#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# CDC log invariants while the cluster or the table changes, ported from
# scylla-dtest cdc_test.py, one test per dtest: writes to a CDC-enabled table
# of a vnodes keyspace while nothing happens, a node is bootstrapped or
# decommissioned, or a column is added or dropped, then checks of the log
# against the base table and the CDC generations. Also check-and-repair of
# the CDC streams after a decommission, and the CDC log of COMPACT STORAGE
# tables.

import asyncio
import bisect
import itertools
import logging
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest
from cassandra import ConsistencyLevel, InvalidRequest, OperationTimedOut, Unavailable, WriteTimeout
from cassandra.connection import ConnectionException
from cassandra.cluster import EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy, FallthroughRetryPolicy, TokenAwarePolicy
from cassandra.query import SimpleStatement
from cassandra.util import datetime_from_uuid1

from test.cluster.util import stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)

NUM_TOKENS = 16
# How long the writers run in each phase before the invariants are checked.
WRITE_SECONDS = 5
# Fewer confirmed writes would make the checks of the log vacuous.
MIN_WRITES = 50
CDC_OP_PREIMAGE = 0
CDC_OP_INSERT = 2


def utcnow() -> datetime:
    # The driver returns timestamps as naive UTC datetimes
    return datetime.now(timezone.utc).replace(tzinfo=None)


def generation_timestamps(cql) -> list[datetime]:
    return sorted(r.time for r in cql.execute(SimpleStatement(
        "SELECT time FROM system_distributed.cdc_generation_timestamps WHERE key = 'timestamps'",
        consistency_level=ConsistencyLevel.QUORUM)))


def stream_to_generation(cql) -> dict[bytes, datetime]:
    """Map each stream of every generation to the generation's timestamp."""
    assignment = {}
    for row in cql.execute(SimpleStatement("SELECT time, streams FROM system_distributed.cdc_streams_descriptions_v2",
                                           consistency_level=ConsistencyLevel.QUORUM)):
        for stream_id in row.streams:
            assert stream_id not in assignment
            assignment[stream_id] = row.time
    return assignment


async def wait_for_generation_active(cql, after: datetime | None = None) -> datetime:
    """Wait until a generation newer than `after` exists and its timestamp has
    passed, so new writes use it. Returns its timestamp."""
    async def active():
        timestamps = generation_timestamps(cql)
        if timestamps and (after is None or timestamps[-1] > after) and utcnow() > timestamps[-1] + timedelta(seconds=1):
            return timestamps[-1]
        return None
    return await wait_for(active, time.time() + 120, period=0.5, label="CDC generation active")


async def wait_for_ring(cql, nodes: int) -> list[int]:
    """Wait until the driver's token map has the vnodes of `nodes` nodes."""
    async def ring():
        tokens = [t.value for t in cql.cluster.metadata.token_map.ring]
        return tokens if len(tokens) == nodes * NUM_TOKENS else None
    return await wait_for(ring, time.time() + 60, period=0.5, label=f"token ring of {nodes} nodes")


def vnode_of(ring: list[int], token: int) -> tuple[int, int]:
    idx = bisect.bisect_left(ring, token)
    if idx == 0 or idx == len(ring):
        return (ring[-1], ring[0])
    return (ring[idx - 1], ring[idx])


def generation_entries(cql, gen_timestamp: datetime) -> list:
    """The token ranges and streams of a generation, read like the dtests did,
    with a prepared statement."""
    query = cql.prepare("SELECT * FROM system_distributed.cdc_streams_descriptions_v2 WHERE time = ?")
    query.consistency_level = ConsistencyLevel.QUORUM
    return list(cql.execute(query, (gen_timestamp,)))


def check_generation(cql, gen_timestamp: datetime, ring: list[int]) -> None:
    """The generation's token ranges refine the vnodes, and every vnode has a
    stream whose token lies inside it."""
    entries = generation_entries(cql, gen_timestamp)
    assert set(ring) <= {e.range_end for e in entries}, "Vnodes should contain generation token ranges"
    prev_range_end = entries[-1].range_end
    for entry in entries:
        range_end = entry.range_end
        vnode_size = range_end - prev_range_end if range_end > prev_range_end else 2**63 - 1 - prev_range_end + range_end
        if vnode_size > 1:
            assert any(int.from_bytes(stream[0:8], byteorder="big", signed=True) != range_end for stream in entry.streams)
        prev_range_end = range_end



@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test."""
    yield
    await stop_servers_gracefully(manager)


def connect(manager: ScyllaClusterManager, server):
    """A session through `server` like the dtests' patient_cql_connection():
    the driver's default token-aware, DC-aware load balancing, statements at
    consistency level ONE."""
    cluster = manager.con_gen([server.ip_addr], load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()))
    cql = cluster.connect()
    cql.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.ONE
    return cql


# The dtests' TestCdc options, in vnodes keyspaces: hinted handoff off,
# num_tokens (256 in the dtests) and ring_delay_ms 0: a new generation
# starts at "now", not now + 2 * ring_delay + 5 s. That is unsafe when
# writes cross a generation switch, so the expansion tests, as the dtests,
# use 1 s, which puts the new generation's timestamp into the future, so
# that all nodes learn it before it starts operating. Decommission creates
# no vnode generation, so the reduction tests run at 0.
CDC_CONFIG = {"tablets_mode_for_new_keyspaces": "disabled", "num_tokens": NUM_TOKENS, "ring_delay_ms": 0,
              "hinted_handoff_enabled": False}
CDC_EXPANSION_CONFIG = CDC_CONFIG | {"ring_delay_ms": 1000}


def cluster_config(name: str, build_mode: str) -> tuple[dict, str]:
    """The dtests' cluster_config fixture: (topology {dc: {rack: nodes}},
    replication). One DC of three racks of one node with RF 3, or three
    such DCs (two DCs of two racks in debug mode) with RF per DC."""
    if name == "Single_cluster":
        nodes = 3
        topology = {"datacenter1": {f"rack{r + 1}": 1 for r in range(nodes)}}
        return topology, f"{{'class': 'NetworkTopologyStrategy', 'replication_factor': {nodes}}}"
    dcs, nodes = (2, 2) if build_mode == "debug" else (3, 3)
    topology = {f"datacenter{d + 1}": {f"rack{r + 1}": 1 for r in range(nodes)} for d in range(dcs)}
    replication = ", ".join(f"'{dc}': {nodes}" for dc in topology)
    return topology, f"{{'class': 'NetworkTopologyStrategy', {replication}}}"


def layout_of(topology: dict) -> list[dict]:
    """The property files of the nodes of a ccm topology, in ccm's node order."""
    return [{"dc": dc, "rack": rack} for dc, racks in topology.items() for rack, n in racks.items() for _ in range(n)]


CLUSTER_CONFIGS = pytest.mark.parametrize("cluster_config_name", ["Single_cluster", "Multi_DC_cluster"])


def run_writes_with_counting(cql, replication: str, with_preimage: bool = False, additional_fields: list[str] = ()):
    """The dtests' run_writes_with_counting(): creates ks.cf with CDC (and
    preimage), and starts writers inserting (i, worker id) for i = 0, 1, ...
    at QUORUM without driver retries (10 writers, or 1 with preimage).
    Returns the finisher, which stops them and returns the counts of
    confirmed and unconfirmed writes."""
    cdc_options = "'enabled': true"
    if with_preimage:
        cdc_options += ", 'preimage': true"
        workers_count = 1
    else:
        workers_count = 10
    cql.execute(f"CREATE KEYSPACE ks WITH replication = {replication}")
    fields = ["a int", "b text", *additional_fields]
    cql.execute(f"CREATE TABLE ks.cf ({', '.join(fields)}, PRIMARY KEY(a)) WITH cdc = {{{cdc_options}}}")

    executor = ThreadPoolExecutor(max_workers=workers_count)
    stop_event = threading.Event()

    def run_writes(worker_id):
        confirmed_writes = 0
        unconfirmed_writes = 0
        i = 0
        stmt = cql.prepare("INSERT INTO ks.cf (a, b) VALUES (?, ?)")
        stmt.consistency_level = ConsistencyLevel.QUORUM
        # Driver retries would make the writes counted wrong.
        stmt.retry_policy = FallthroughRetryPolicy()
        while not stop_event.is_set():
            try:
                cql.execute(stmt, (i, str(worker_id)))
                confirmed_writes += 1
            except (ConnectionException, WriteTimeout, OperationTimedOut) as e:
                # We cannot determine if the write was successful.
                logger.info(f"Worker #{worker_id}: unconfirmed write: {e}")
                unconfirmed_writes += 1
            except Unavailable as e:
                # Not enough live replicas while the ring changes: not written.
                logger.info(f"Worker #{worker_id}: unavailable: {e}")
            except InvalidRequest as e:
                # The write's timestamp is older than the current generation:
                # rejected, not logged.
                if "cdc: attempted to get a stream from an earlier generation than the currently used" not in str(e):
                    raise
            i += 1
        return confirmed_writes, unconfirmed_writes

    futs = [executor.submit(run_writes, i) for i in range(workers_count)]

    def finisher():
        stop_event.set()
        total_confirmed = total_unconfirmed = 0
        for i, fut in enumerate(futs):
            confirmed, unconfirmed = fut.result()
            logger.info(f"Worker #{i} did {confirmed} successful writes, and {unconfirmed} unconfirmed writes")
            total_confirmed += confirmed
            total_unconfirmed += unconfirmed
        executor.shutdown(wait=True)
        return total_confirmed, total_unconfirmed

    return finisher


def cdc_descriptions(cql) -> list[tuple[datetime, list[bytes]]]:
    """(time, streams) of every generation, like get_all_cdc_description_rows()."""
    return [(r.time, r.streams) for r in cql.execute(SimpleStatement(
        "SELECT * FROM system_distributed.cdc_streams_descriptions_v2", consistency_level=ConsistencyLevel.QUORUM))]


def get_base_and_log_rows(cql) -> tuple[list, list]:
    base_rows = list(cql.execute(SimpleStatement("SELECT a, b, token(a) AS tok FROM ks.cf", consistency_level=ConsistencyLevel.ALL)))
    log_rows = list(cql.execute(SimpleStatement(
        'SELECT "cdc$stream_id", "cdc$time", "cdc$batch_seq_no", a, b, "cdc$operation", "cdc$ttl", '
        'token("cdc$stream_id") AS tok FROM ks.cf_scylla_cdc_log', consistency_level=ConsistencyLevel.ALL)))
    logger.info(f"There are {len(base_rows)} base table rows and {len(log_rows)} cdc log rows")
    return base_rows, log_rows


def get_sorted_update_rows(cql, log_rows) -> list:
    """The log's INSERT rows, in the order of the generations of their streams."""
    assignment = stream_to_generation(cql)
    return sorted((r for r in log_rows if r.cdc_operation == CDC_OP_INSERT), key=lambda r: assignment[r.cdc_stream_id])


def check_common_invariants(cql, base_rows, log_rows, update_rows, write_count, with_preimage) -> None:
    if with_preimage:
        # The preimage reflects the previous state of the row.
        latest_rows = {}
        for _, write in itertools.groupby(log_rows, key=lambda r: r.cdc_time):
            writes = list(write)
            if len(writes) > 1:
                preimage_row, update_row = writes
                assert preimage_row.cdc_operation == CDC_OP_PREIMAGE
                assert update_row.cdc_operation == CDC_OP_INSERT
                assert preimage_row.a == update_row.a
                assert preimage_row.b is not None
                assert preimage_row.b == latest_rows[update_row.a].b, "Preimage did not contain previous state of the row"
            else:
                update_row = writes[0]
                assert update_row.cdc_operation == CDC_OP_INSERT
            latest_rows[update_row.a] = update_row

    # There are as many "insert" rows in the log as there were writes.
    confirmed_count, unconfirmed_count = write_count
    assert confirmed_count >= MIN_WRITES, f"only {confirmed_count} confirmed writes, the checks would be vacuous"
    assert len(update_rows) >= confirmed_count
    assert len(update_rows) <= confirmed_count + unconfirmed_count

    # The most recent log rows correspond to the base table.
    latest_rows = {}
    for row in update_rows:
        if row.a in latest_rows:
            assert datetime_from_uuid1(row.cdc_time) >= datetime_from_uuid1(latest_rows[row.a].cdc_time), f"Update rows are not sorted, pk: {row.a}"
        latest_rows[row.a] = row
    assert len(latest_rows) == len(base_rows)
    for row in base_rows:
        assert row.a in latest_rows
        assert row.b == latest_rows[row.a].b

    # Within a generation, a partition is written to one stream only.
    generation_times = sorted(t for t, _ in cdc_descriptions(cql))
    stream_for_partition = {}
    for row in update_rows:
        idx = (bisect.bisect(generation_times, datetime_from_uuid1(row.cdc_time)), row.a)
        if idx not in stream_for_partition:
            stream_for_partition[idx] = row.cdc_stream_id
        else:
            assert stream_for_partition[idx] == row.cdc_stream_id, "A partition was written to more than one stream within one generation"

    # Log entries are not earlier than their stream.
    stream_to_timestamp = stream_to_generation(cql)
    for row in update_rows:
        assert row.cdc_stream_id in stream_to_timestamp
        assert stream_to_timestamp[row.cdc_stream_id] <= datetime_from_uuid1(row.cdc_time)


def check_same_vnode(base_rows, update_rows, ring: list[int], time_range_begin: datetime | None = None,
                     time_range_end: datetime | None = None) -> None:
    """Log entries written in [begin, end) and their base partitions are in
    the same vnode of `ring` (base partitions in a vnode no log entry is in
    are skipped)."""
    pk_to_vnode = {r.a: vnode_of(ring, r.tok) for r in base_rows}
    log_vnodes = [vnode_of(ring, r.tok) for r in update_rows]
    good_vnodes = set(log_vnodes)
    for row, log_row_vnode in zip(update_rows, log_vnodes):
        time_ = datetime_from_uuid1(row.cdc_time)
        if time_range_begin is not None and time_ < time_range_begin:
            continue
        if time_range_end is not None and time_range_end <= time_:
            continue
        base_row_vnode = pk_to_vnode[row.a]
        if base_row_vnode not in good_vnodes:
            continue
        assert base_row_vnode == log_row_vnode, f"Log entry written at {time_} is in a stream of another vnode"


async def start_cluster(manager: ScyllaClusterManager, topology: dict, cmdline: list[str] | None = None,
                        config: dict = CDC_CONFIG):
    """Starts the nodes of the ccm topology, with the TestCdc options."""
    return await manager.servers_add(sum(sum(r.values()) for r in topology.values()), config=config,
                                     property_file=layout_of(topology), cmdline=cmdline)


async def simple_cdc_template(manager: ScyllaClusterManager, topology: dict, replication: str, with_preimage: bool) -> None:
    """Writes for WRITE_SECONDS, then checks the log."""
    servers = await start_cluster(manager, topology)
    cql = connect(manager, servers[0])
    try:
        gen_timestamp = await wait_for_generation_active(cql)
        ring = await wait_for_ring(cql, len(servers))
        check_generation(cql, gen_timestamp, ring)

        finish_writing = run_writes_with_counting(cql, replication, with_preimage=with_preimage)
        try:
            await asyncio.sleep(WRITE_SECONDS)
        finally:
            write_count = finish_writing()
        base_rows, log_rows = get_base_and_log_rows(cql)
        update_rows = get_sorted_update_rows(cql, log_rows)
        check_common_invariants(cql, base_rows, log_rows, update_rows, write_count, with_preimage)
        check_same_vnode(base_rows, update_rows, ring)
    finally:
        cql.cluster.shutdown()


async def cluster_expansion_with_cdc_template(manager: ScyllaClusterManager, topology: dict, replication: str, with_preimage: bool) -> None:
    """Writes through a random node while a node is bootstrapped in its DC
    (into rack RAC1, as ccm's new_node() with only a DC), and for
    WRITE_SECONDS more, then checks the log against the rings before and
    after, and that rows were logged after the expansion."""
    servers = await start_cluster(manager, topology, config=CDC_EXPANSION_CONFIG)
    layout = layout_of(topology)
    idx = random.randrange(len(servers))
    node1 = servers[idx]
    cql = connect(manager, node1)
    try:
        gen_timestamp = await wait_for_generation_active(cql)
        ring_before_expansion = await wait_for_ring(cql, len(servers))
        check_generation(cql, gen_timestamp, ring_before_expansion)

        finish_writing = run_writes_with_counting(cql, replication, with_preimage=with_preimage)
        try:
            expansion_start_time = utcnow()
            await manager.server_add(config=CDC_EXPANSION_CONFIG, property_file={"dc": layout[idx]["dc"], "rack": "RAC1"})
            gen_timestamp = await wait_for_generation_active(cql, after=gen_timestamp)
            await asyncio.sleep(WRITE_SECONDS)
        finally:
            write_count = finish_writing()

        ring_after_expansion = await wait_for_ring(cql, len(servers) + 1)
        assert ring_before_expansion != ring_after_expansion
        check_generation(cql, gen_timestamp, ring_after_expansion)

        base_rows, log_rows = get_base_and_log_rows(cql)
        update_rows = get_sorted_update_rows(cql, log_rows)
        check_common_invariants(cql, base_rows, log_rows, update_rows, write_count, with_preimage)
        check_same_vnode(base_rows, update_rows, ring_before_expansion, time_range_end=expansion_start_time)
        after_expansion_timestamp = min(t for t, _ in cdc_descriptions(cql) if t > expansion_start_time)
        assert any(datetime_from_uuid1(r.cdc_time) >= after_expansion_timestamp for r in update_rows), "no rows after expansion"
        check_same_vnode(base_rows, update_rows, ring_after_expansion, time_range_begin=after_expansion_timestamp)
    finally:
        cql.cluster.shutdown()


async def cluster_reduction_with_cdc_template(manager: ScyllaClusterManager, topology: dict, replication: str, with_preimage: bool) -> None:
    """With a node added to the first rack of every DC, writes for
    WRITE_SECONDS, decommissions the other node of the first rack of the
    first DC, writes for WRITE_SECONDS more, then checks the log against the
    original ring."""
    expanded_topology = {dc: {rack: n + 1 if i == 0 else n for i, (rack, n) in enumerate(racks.items())}
                         for dc, racks in topology.items()}
    servers = await start_cluster(manager, expanded_topology)
    cql = connect(manager, servers[0])
    try:
        gen_timestamp = await wait_for_generation_active(cql)
        ring = await wait_for_ring(cql, len(servers))
        check_generation(cql, gen_timestamp, ring)

        finish_writing = run_writes_with_counting(cql, replication, with_preimage=with_preimage)
        try:
            await asyncio.sleep(WRITE_SECONDS)
            # ccm's node order: the first rack of the first DC holds node1 and node2.
            await manager.decommission_node(servers[1].server_id)
            await asyncio.sleep(WRITE_SECONDS)
        finally:
            write_count = finish_writing()

        base_rows, log_rows = get_base_and_log_rows(cql)
        update_rows = get_sorted_update_rows(cql, log_rows)
        check_common_invariants(cql, base_rows, log_rows, update_rows, write_count, with_preimage)
        check_same_vnode(base_rows, update_rows, ring)
    finally:
        cql.cluster.shutdown()


async def schema_change_template(manager: ScyllaClusterManager, alter_query: str, topology: dict, replication: str,
                                 with_preimage: bool = False, additional_fields: list[str] = ()) -> None:
    """Writes for WRITE_SECONDS, alters the table, writes for WRITE_SECONDS
    more, then checks the log. As in the dtests, migration_manager logs at trace level."""
    servers = await start_cluster(manager, topology, cmdline=["--logger-log-level", "migration_manager=trace"])
    cql = connect(manager, servers[0])
    try:
        gen_timestamp = await wait_for_generation_active(cql)
        ring = await wait_for_ring(cql, len(servers))
        check_generation(cql, gen_timestamp, ring)

        finish_writing = run_writes_with_counting(cql, replication, with_preimage=with_preimage, additional_fields=additional_fields)
        try:
            await asyncio.sleep(WRITE_SECONDS)
            cql.execute(alter_query)
            await asyncio.sleep(WRITE_SECONDS)
        finally:
            write_count = finish_writing()

        base_rows, log_rows = get_base_and_log_rows(cql)
        update_rows = get_sorted_update_rows(cql, log_rows)
        check_common_invariants(cql, base_rows, log_rows, update_rows, write_count, with_preimage)
        check_same_vnode(base_rows, update_rows, ring)
    finally:
        cql.cluster.shutdown()


GATING = pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")


# Ported from scylla-dtest cdc_test.py::TestCdc::test_simple_cdc.
# Writes for 5 s and checks the log invariants.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@CLUSTER_CONFIGS
async def test_simple_cdc(manager: ScyllaClusterManager, cluster_config_name: str, build_mode: str):
    await simple_cdc_template(manager, *cluster_config(cluster_config_name, build_mode), with_preimage=False)


# Ported from scylla-dtest cdc_test.py::TestCdc::test_simple_cdc_with_preimage.
@GATING
@CLUSTER_CONFIGS
async def test_simple_cdc_with_preimage(manager: ScyllaClusterManager, cluster_config_name: str, build_mode: str):
    await simple_cdc_template(manager, *cluster_config(cluster_config_name, build_mode), with_preimage=True)


# Ported from scylla-dtest cdc_test.py::TestCdc::test_cluster_expansion_with_cdc.
# A node is bootstrapped while writing: a new generation starts, matching
# the new ring.
@GATING
@CLUSTER_CONFIGS
async def test_cluster_expansion_with_cdc(manager: ScyllaClusterManager, cluster_config_name: str, build_mode: str):
    await cluster_expansion_with_cdc_template(manager, *cluster_config(cluster_config_name, build_mode), with_preimage=False)


# Ported from scylla-dtest cdc_test.py::TestCdc::test_cluster_expansion_with_cdc_and_preimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@CLUSTER_CONFIGS
async def test_cluster_expansion_with_cdc_and_preimage(manager: ScyllaClusterManager, cluster_config_name: str, build_mode: str):
    await cluster_expansion_with_cdc_template(manager, *cluster_config(cluster_config_name, build_mode), with_preimage=True)


# Ported from scylla-dtest cdc_test.py::TestCdc::test_cluster_reduction_with_cdc.
# A node is decommissioned while writing: no new generation, the streams
# still follow the original ring.
@GATING
@CLUSTER_CONFIGS
async def test_cluster_reduction_with_cdc(manager: ScyllaClusterManager, cluster_config_name: str, build_mode: str):
    await cluster_reduction_with_cdc_template(manager, *cluster_config(cluster_config_name, build_mode), with_preimage=False)


# Ported from scylla-dtest cdc_test.py::TestCdc::test_cluster_reduction_with_cdc_and_preimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@CLUSTER_CONFIGS
async def test_cluster_reduction_with_cdc_and_preimage(manager: ScyllaClusterManager, cluster_config_name: str, build_mode: str):
    await cluster_reduction_with_cdc_template(manager, *cluster_config(cluster_config_name, build_mode), with_preimage=True)


# Ported from scylla-dtest cdc_test.py::TestCdc::test_add_field_with_cdc.
# A column is added while writing.
@GATING
@CLUSTER_CONFIGS
async def test_add_field_with_cdc(manager: ScyllaClusterManager, cluster_config_name: str, build_mode: str):
    await schema_change_template(manager, "ALTER TABLE ks.cf ADD c int", *cluster_config(cluster_config_name, build_mode))


# Ported from scylla-dtest cdc_test.py::TestCdc::test_add_field_with_cdc_and_preimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@CLUSTER_CONFIGS
async def test_add_field_with_cdc_and_preimage(manager: ScyllaClusterManager, cluster_config_name: str, build_mode: str):
    await schema_change_template(manager, "ALTER TABLE ks.cf ADD c int", *cluster_config(cluster_config_name, build_mode), with_preimage=True)


# Ported from scylla-dtest cdc_test.py::TestCdc::test_remove_field_with_cdc.
# A column is dropped while writing.
@GATING
@CLUSTER_CONFIGS
async def test_remove_field_with_cdc(manager: ScyllaClusterManager, cluster_config_name: str, build_mode: str):
    await schema_change_template(manager, "ALTER TABLE ks.cf DROP c", *cluster_config(cluster_config_name, build_mode), additional_fields=["c int"])


# Ported from scylla-dtest cdc_test.py::TestCdc::test_remove_field_with_cdc_and_preimage.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@CLUSTER_CONFIGS
async def test_remove_field_with_cdc_and_preimage(manager: ScyllaClusterManager, cluster_config_name: str, build_mode: str):
    await schema_change_template(manager, "ALTER TABLE ks.cf DROP c", *cluster_config(cluster_config_name, build_mode),
                                 with_preimage=True, additional_fields=["c int"])


@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_check_and_repair_cdc_streams_after_decommission(manager: ScyllaClusterManager):
    """
    After a node is decommissioned, the CDC streams no longer match the token
    ring: check-and-repair (nodetool checkAndRepairCdcStreams) creates a new
    generation whose token ranges are the ring's vnodes, and running it again
    creates no other generation.
    """
    cfg = {"tablets_mode_for_new_keyspaces": "disabled", "num_tokens": NUM_TOKENS, "hinted_handoff_enabled": False}
    servers = await manager.servers_add(3, config=cfg, auto_rack_dc="dc1")
    cql = await manager.get_cql_exclusive(servers[0])

    gen_timestamp = generation_timestamps(cql)[-1]
    ring = await wait_for_ring(cql, 3)
    check_generation(cql, gen_timestamp, ring)

    await manager.decommission_node(servers[2].server_id)
    ring = await wait_for_ring(cql, 2)

    async def check_and_repair():
        await asyncio.wait_for(manager.api.client.post("/storage_service/cdc_streams_check_and_repair", servers[0].ip_addr), 60)

    await check_and_repair()
    async def new_generation():
        timestamps = generation_timestamps(cql)
        return timestamps[-1] if timestamps[-1] > gen_timestamp else None
    gen_timestamp = await wait_for(new_generation, time.time() + 60, period=1, label="new CDC generation")
    assert {e.range_end for e in generation_entries(cql, gen_timestamp)} == set(ring), "New generation should match the token ring"
    check_generation(cql, gen_timestamp, ring)

    await check_and_repair()
    assert generation_timestamps(cql)[-1] == gen_timestamp, "2nd run of check-and-repair should not create a generation"


# The configurations of TestCdcWithCompactStorage: every preimage option
# ('full', true, false - given even when false) with postimage and a regular
# column on and off, with the dtest's ids.
def compact_storage_params():
    params = []
    for preimage in ["'full'", "true", "false"]:
        for postimage, use_regular_column in itertools.product([False, True], repeat=2):
            config = {"preimage": preimage, "postimage": postimage, "use_regular_column": use_regular_column}
            params.append(pytest.param(config, id="-".join(f"{k}:{v}".lower() for k, v in config.items()).replace("'", "")))
    return params


COMPACT_STORAGE_EXPECTED_FIELDS = ["cdc_stream_id", "cdc_time", "cdc_batch_seq_no", "cdc_end_of_batch", "cdc_operation", "cdc_ttl",
                                   "pk", "ck", "reg_column", "cdc_deleted_reg_column"]


# Ported from scylla-dtest cdc_test.py::TestCdcWithCompactStorage::test_artificial_column_with_type_empty_is_missing.
# A COMPACT STORAGE table without a regular column has a hidden regular
# column of type "empty", which must not show up in the CDC log. On two
# nodes (datacenter1, rack1 and rack2) with RF 2: an INSERT, DELETEs, a
# logged batch of 50 INSERTs, each followed by a check of the log's columns,
# and a range deletion, which compact storage tables do not support.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("test_config", compact_storage_params())
async def test_artificial_column_with_type_empty_is_missing(manager: ScyllaClusterManager, test_config: dict):
    preimage, postimage, use_reg_column = test_config["preimage"], test_config["postimage"], test_config["use_regular_column"]
    config = {"tablets_mode_for_new_keyspaces": "disabled", "enable_create_table_with_compact_storage": True}
    servers = await manager.servers_add(2, config=config, property_file=[{"dc": "datacenter1", "rack": "rack1"},
                                                                         {"dc": "datacenter1", "rack": "rack2"}])
    cql = connect(manager, servers[0])
    try:
        cql.execute("CREATE KEYSPACE ks1 WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : 2};")
        create_table = "CREATE TABLE ks1.cf1 (pk int, ck int, "
        if use_reg_column:
            create_table += "reg_column text, "
        create_table += "PRIMARY KEY (pk, ck)) with COMPACT STORAGE and cdc = {'enabled': true"
        if preimage:
            create_table += f", 'preimage': {preimage}"
        if postimage:
            create_table += f", 'postimage': {postimage}"
        cql.execute(create_table + " };")

        def assert_columns():
            rows = list(cql.execute(SimpleStatement("SELECT * FROM ks1.cf1_scylla_cdc_log", consistency_level=ConsistencyLevel.ALL)))
            for row in rows:
                for column in row._fields:
                    assert column in COMPACT_STORAGE_EXPECTED_FIELDS, f"CDC log row doesn't have column {column}"

        insert_query = ["INSERT INTO ks1.cf1 (pk, ck, reg_column) VALUES (1, 1, '1');"] if use_reg_column else ["INSERT INTO ks1.cf1 (pk, ck) VALUES (1, 1);"]
        delete_query = (["DELETE reg_column FROM ks1.cf1 WHERE ck = 1 AND pk = 1", "DELETE FROM ks1.cf1 WHERE ck = 1 AND pk = 1"]
                        if use_reg_column else ["DELETE FROM ks1.cf1 WHERE ck = 1 AND pk = 1"])
        queries = []
        for i in range(5):
            for j in range(10):
                if use_reg_column:
                    queries.append(f"INSERT INTO ks1.cf1 (pk, ck, reg_column) VALUES ({i}, {j}, '{j + i}');")
                else:
                    queries.append(f"INSERT INTO ks1.cf1 (pk, ck) VALUES ({i}, {j});")
        batch_insert_query = [f"BEGIN BATCH {''.join(queries)} APPLY BATCH;"]
        delete_range_query = (["DELETE FROM ks1.cf1 WHERE pk = 1 AND ck > 3 and ck < 6", "DELETE reg_column FROM ks1.cf1 WHERE pk = 1 AND ck > 6 and ck < 9"]
                              if use_reg_column else ["DELETE FROM ks1.cf1 WHERE pk = 1 AND ck > 3 and ck < 6"])

        for queries in [insert_query, delete_query, batch_insert_query]:
            for query in queries:
                cql.execute(query)
            assert_columns()

        with pytest.raises(InvalidRequest, match='Range deletions on "compact storage" schemas are not supported'):
            for query in delete_range_query:
                cql.execute(query)
            assert_columns()
    finally:
        cql.cluster.shutdown()
