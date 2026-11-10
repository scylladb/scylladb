#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
"scylla nodetool" commands run against real nodes, their output parsed the
way an operator reads it: compaction, flush, ring, status, info,
describecluster, histograms, refresh, scrub and validate, and others.

Ported from scylla-dtest nodetool_additional_test.py. Like the dtests'
nodes, the nodes use vnodes (256 tokens per node where the dtest counted
tokens), the default snitch (datacenter1/rack1) and compaction debug
logging; load is written with cassandra-stress (cassandra_stress()), as in
the dtests.
"""

import asyncio
import glob
import json
import logging
import os
import random
import re
import shutil
import stat
import tempfile
import time
import uuid

import pytest
import yaml
from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.query import SimpleStatement

from test import DEBUG_MODES
from test.cluster.util import cassandra_stress, get_table_dir, run_nodetool, stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

# Like the dtests' nodes, new keyspaces use vnodes by default, 256 per node.
CONFIG = {'tablets_mode_for_new_keyspaces': 'disabled', 'num_tokens': 256}
# The nodetool dtests ran with compaction debug logging.
CMDLINE = ["--logger-log-level", "compaction=debug"]
KS_OPTS = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
RESOURCES = os.path.join(os.path.dirname(__file__), "..", "resource", "sstables")


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


def to_cfstats(out: str) -> dict:
    """Parse "nodetool cfstats" output into {keyspace: {..., "tables": {table: {...}}}}."""
    p = re.compile(r"^\s*([^:]+)\s*:\s*(\S.*)\s*$")
    res, ks, obj = {}, None, {}
    for line in [s.strip() for s in out.splitlines()]:
        m = p.match(line)
        if m:
            if m.group(1) == "Table":
                if ks is None:
                    ks = obj
                    ks["tables"] = {}
                elif "Table" in obj:
                    ks["tables"][obj["Table"]] = obj
                obj = {}
            obj[m.group(1).strip()] = m.group(2).strip()
        elif line.find("----------------") >= 0 and ks is not None:
            if obj != {} and "Table" in obj:
                ks["tables"][obj["Table"]] = obj
                obj = {}
            res[ks["Keyspace"]] = ks
            ks = None
    return res


async def standard1_stats(manager: ScyllaClusterManager, server) -> dict:
    out, _, _ = await run_nodetool(manager, server, "cfstats", "keyspace1.standard1")
    return to_cfstats(out)["keyspace1"]["tables"]["standard1"]


def assert_ip(addr: str) -> None:
    assert re.match(r"\d+\.\d+\.\d+\.\d+", addr), f"bad ip format: {addr}"


# Major compaction of all tables, or of keyspace1.standard1, after two
# flushed batches of 1000 stress writes (with compaction's min threshold
# enforced, so they are not compacted before): "nodetool cfstats" shows at
# least two sstables before and fewer after.
async def compact(manager: ScyllaClusterManager, args):
    """The dtests' _compact()."""
    server = await manager.server_add(config=CONFIG, cmdline=CMDLINE + ["--compaction-enforce-min-threshold", "true"])
    for _ in range(2):
        await cassandra_stress(manager, server, "write", "cl=ALL", "no-warmup", "n=1000", "-pop", "seq=1..1000")
        await run_nodetool(manager, server, "flush")
    table = await standard1_stats(manager, server)
    sstables = int(table["SSTable count"])
    assert sstables >= 2
    out, _, _ = await run_nodetool(manager, server, "compactionhistory")
    await run_nodetool(manager, server, "compact", *args)
    assert int((await standard1_stats(manager, server))["SSTable count"]) < sstables


# Ported from scylla-dtest nodetool_additional_test.py::TestNodetool::test_general_compact.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_nodetool_general_compact(manager: ScyllaClusterManager):
    await compact(manager, [])


# Ported from scylla-dtest nodetool_additional_test.py::TestNodetool::test_specific_compact.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_nodetool_specific_compact(manager: ScyllaClusterManager):
    await compact(manager, ["keyspace1", "standard1"])


# "nodetool statusbinary" reports the CQL server running, not running after
# "disablebinary", and running again after "enablebinary".
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_nodetool_binary(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG, cmdline=CMDLINE)
    async def running():
        out, _, _ = await run_nodetool(manager, server, "statusbinary")
        if re.search(r"^\s*running\s*$", out):
            return True
        assert re.search(r"^\s*not running\s*$", out), out
        return False
    assert await running()
    await run_nodetool(manager, server, "disablebinary")
    assert not await running()
    await run_nodetool(manager, server, "enablebinary")
    assert await running()


# "nodetool ring" on two nodes of 256 vnodes each, after stress writes:
# the datacenter and 512 well-formed token lines, without ownership as no
# keyspace is given.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_nodetool_ring(manager: ScyllaClusterManager):
    servers = await manager.servers_add(2, config={**CONFIG, 'num_tokens': 256}, cmdline=CMDLINE)
    await cassandra_stress(manager, servers[0], "write", "cl=ALL", "no-warmup", "n=100")
    out, _, _ = await run_nodetool(manager, servers[0], "ring")
    dc = re.findall(r"^\s*Datacenter: ([^\s]+)\s*$", out, re.MULTILINE)
    assert dc == ["datacenter1"]
    load_unit = r"(?:\s(?:bytes|[KMGTPE]i?B))?"
    tokens = re.findall(r"^\s*([\d\.]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)" + load_unit + r"\s+([^\s]+)(?:\s[^\s]{2})?\s+(\-?[\d]+)\s*$",
                        out, re.MULTILINE)
    assert len(tokens) == 512
    for address, rack, status, state, _, owns, token in tokens:
        assert_ip(address)
        assert re.match(r"[a-z0-9]+", rack)
        assert status in ["Up", "Down"]
        assert state == "Normal"
        assert owns == "?"
        assert re.match(r"-?[0-9]+", token)


# One stress write stays in the memtable (no sstable), and "nodetool
# flush" of all tables, of keyspace1 or of keyspace1.standard1 writes one.
async def flush(manager: ScyllaClusterManager, args):
    """The dtests' _flush()."""
    server = await manager.server_add(config=CONFIG, cmdline=CMDLINE)
    await cassandra_stress(manager, server, "write", "cl=ALL", "no-warmup", "n=1")
    assert (await standard1_stats(manager, server))["SSTable count"] == "0"
    await run_nodetool(manager, server, "flush", *args)
    assert (await standard1_stats(manager, server))["SSTable count"] == "1"


# Ported from scylla-dtest nodetool_additional_test.py::TestNodetool::test_general_flush.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_nodetool_general_flush(manager: ScyllaClusterManager):
    await flush(manager, [])


# Ported from scylla-dtest nodetool_additional_test.py::TestNodetool::test_keyspace_flush.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_nodetool_keyspace_flush(manager: ScyllaClusterManager):
    await flush(manager, ["keyspace1"])


# Ported from scylla-dtest nodetool_additional_test.py::TestNodetool::test_keyspace_column_family_flush.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_nodetool_keyspace_column_family_flush(manager: ScyllaClusterManager):
    await flush(manager, ["keyspace1", "standard1"])


def get_cfhistogram(out: str) -> dict:
    m = re.findall(r"^([^\s]+)\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)\s*$", out, re.MULTILINE)
    heads = ["Percentile", "SSTables", "Write Latency", "Read Latency", "Partition Size", "Cell Count"]
    return {val[0]: {h: float(v) for h, v in zip(heads[1:], val[1:])} for val in m if val}


# With autocompaction off and ten flushes of overlapping writes, reads of
# 100 keys at a time touch several sstables: the Min and Max "SSTables" of
# "nodetool cfhistograms" match the sstables_per_read_histogram of the REST
# API.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_sstables_in_tablehistograms(manager: ScyllaClusterManager, build_mode):
    server = await manager.server_add(config=CONFIG, cmdline=CMDLINE)
    cql = manager.get_cql()
    await cql.run_async(f"CREATE KEYSPACE ks {KS_OPTS}")
    await cql.run_async("CREATE COLUMNFAMILY ks.cf (key varchar PRIMARY KEY, c1 text, c2 text) WITH comment='test cf' "
                        "AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'} AND compression = {}")
    await cql.run_async("SELECT * FROM ks.cf LIMIT 1")
    await run_nodetool(manager, server, "disableautocompaction")
    # The dtests' insert_c1c2() writes at QUORUM; their session reads at ONE.
    insert = cql.prepare("INSERT INTO ks.cf (key, c1, c2) VALUES (?, 'value1', 'value2')")
    insert.consistency_level = ConsistencyLevel.QUORUM
    flushes = 5
    # The dtest's sizes, smaller in debug mode as there.
    times, reads = (50, 100) if build_mode in DEBUG_MODES else (100, 1000)
    for i in range(flushes):
        await asyncio.gather(*[cql.run_async(insert, [f"k{k}"]) for k in range(times * i + 1, times * (i + 1))])
        await run_nodetool(manager, server, "flush")
    for _ in range(flushes):
        beg = random.randint(1, times * flushes)
        await asyncio.gather(*[cql.run_async(insert, [f"k{k}"]) for k in range(beg, beg + random.randint(1, times))])
        await run_nodetool(manager, server, "flush")
    for _ in range(reads):
        keys = ", ".join(f"'k{random.randint(1, flushes * times)}'" for _ in range(100))
        await cql.run_async(SimpleStatement(f"SELECT * FROM ks.cf WHERE key IN ({keys}) ALLOW FILTERING BYPASS CACHE;",
                                            consistency_level=ConsistencyLevel.ONE))
    out, _, _ = await run_nodetool(manager, server, "cfhistograms", "ks", "cf")
    hist = get_cfhistogram(out)
    api = json.loads(await manager.api.client.get_text("/column_family/metrics/sstables_per_read_histogram/ks:cf", host=server.ip_addr))
    offsets, buckets = api["bucket_offsets"], api["buckets"]
    assert buckets[-1] == 0, "Histogram overflows"
    bucket_max = next((offset for bucket, offset in reversed(list(zip(buckets, offsets))) if bucket > 0), 0)
    bucket_min = next((0 if i == 0 else 1 + offsets[i - 1] for i in range(len(buckets)) if buckets[i] > 0), 0)
    assert hist["Max"]["SSTables"] == bucket_max
    assert hist["Min"]["SSTables"] == bucket_min


# "nodetool describecluster" on three nodes: the cluster name (test.py's,
# where the dtest's was "test"), partitioner, snitch, and one schema version
# shared by the three nodes.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_nodetool_describecluster(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config=CONFIG, cmdline=CMDLINE)
    cluster_name = (await manager.get_cql().run_async("SELECT cluster_name FROM system.local"))[0].cluster_name
    out, _, _ = await run_nodetool(manager, servers[0], "describecluster")
    info = yaml.safe_load(out.replace("\t", "  "))["Cluster Information"]
    assert info["Name"] == cluster_name
    assert info["Partitioner"] == "org.apache.cassandra.dht.Murmur3Partitioner"
    assert info["Snitch"].startswith("org.apache.cassandra.locator.")
    for version, nodes in info["Schema versions"].items():
        assert len(nodes) == 3, f"wrong schema version for {version}: {nodes}"


# "nodetool getendpoints" of a key of an RF=1 table on three nodes prints
# the address of one of them.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_nodetool_getendpoints(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config=CONFIG, cmdline=CMDLINE)
    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE ks1 WITH replication = { 'class':'NetworkTopologyStrategy', 'replication_factor':1}")
    await cql.run_async("CREATE TABLE ks1.tbl1 (col1 int,col2 text ,PRIMARY KEY (col1))")
    await cql.run_async("INSERT INTO ks1.tbl1 (col1,col2) VALUES (4,'abc')")
    out, _, _ = await run_nodetool(manager, servers[0], "getendpoints", "ks1", "tbl1", "4")
    assert out.strip() in [s.ip_addr for s in servers]


async def schema_versions_count(manager: ScyllaClusterManager, server) -> int:
    out, _, _ = await run_nodetool(manager, server, "describecluster")
    lines = [s.strip() for s in out.splitlines() if s.strip()]
    return len(lines[lines.index("Schema versions:") + 1:])


# A CREATE TABLE with an invalid compaction option fails and leaves the two
# nodes agreeing on the schema; the valid CREATE that follows changes the
# schema version on the other node too (#7811).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("strategy", ["TimeWindowCompactionStrategy", "SizeTieredCompactionStrategy"])
async def test_invalid_compaction_option_schema_agreement(manager: ScyllaClusterManager, strategy):
    servers = await manager.servers_add(2, config=CONFIG, cmdline=CMDLINE, auto_rack_dc="dc1")
    cql = await manager.get_cql_exclusive(servers[0])
    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    if strategy == "TimeWindowCompactionStrategy":
        option, expected_error = "compaction_window_unit", "Invalid window unit NOPE for compaction_window_unit"
    else:
        option, expected_error = "min_sstable_size", "Invalid long value NOPE for 'min_sstable_size'"
    create = ("CREATE COLUMNFAMILY ks.cf (key varchar, c varchar, v varchar, PRIMARY KEY(key, c)) WITH comment='test cf' "
              "AND compaction={} AND compression = {{}}")
    with pytest.raises(Exception, match=re.escape(expected_error)):
        await cql.run_async(create.format({'class': strategy, option: 'NOPE'}))
    assert await schema_versions_count(manager, servers[0]) == 1
    log = await manager.server_open_log(servers[1].server_id)
    mark = await log.mark()
    await cql.run_async(create.format({'class': strategy}))
    await cql.run_async("SELECT * FROM ks.cf LIMIT 1")
    await log.wait_for("schema_tables - Schema version changed to", from_mark=mark, timeout=10)
    assert await schema_versions_count(manager, servers[0]) == 1


def status_nodes(out: str) -> list[dict]:
    """The nodes "nodetool status" lists, as the dtests' nodetool_status()
    parses them."""
    return [m.groupdict() for m in re.finditer(
        r"^(?P<status>[UDNLJM]+)\s+(?P<address>[\d\.]+)\s+(?P<load>\?|[^\s]+\s+[^\s]+)\s+(?P<tokens>[^\s]+)\s+(?P<owns>[^\s]+)"
        r"(?:\s[^\s]{2})?\s+(?P<host_id>[^\s]+)\s+(?P<rack>[^\s]+)\s*", out, re.MULTILINE)]


# "nodetool status" on two nodes after stress writes, for the whole
# cluster, keyspace1 and keyspace1.standard1: the datacenter, and both nodes
# with their state, address, load, tokens, ownership, host id and rack.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_nodetool_status(manager: ScyllaClusterManager):
    servers = await manager.servers_add(2, config=CONFIG, cmdline=CMDLINE)
    await cassandra_stress(manager, servers[0], "write", "cl=ALL", "no-warmup", "n=10000")
    for args, expect_owns in [([], False), (["keyspace1"], True), (["keyspace1", "standard1"], True)]:
        out, _, _ = await run_nodetool(manager, servers[0], "status", *args)
        assert re.findall(r"Datacenter: ([^\s]+)", out, re.MULTILINE)[0] == "datacenter1"
        nodes = status_nodes(out)
        assert len(nodes) == 2
        for node in nodes:
            assert re.match("[UD][NLJM]?", node["status"])
            assert_ip(node["address"])
            assert re.match(r"\d+\.?\d*\s+[KMGT]i?B", node["load"]), node["load"]
            if expect_owns:
                assert re.match(r"\d+\.?\d*(%|\s+[KMGT]i?B)", node["owns"]), node["owns"]
            else:
                assert node["owns"] == "?"
            assert re.match(r"\d+", node["tokens"])
            uuid.UUID(node["host_id"])
            assert re.match(r"[\w-]+", node["rack"])


# "nodetool decommission" run on the second of two nodes: "nodetool status"
# on the first lists both nodes before and only itself after.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_nodetool_decommission(manager: ScyllaClusterManager):
    servers = await manager.servers_add(2, config=CONFIG, cmdline=CMDLINE)
    out, _, _ = await run_nodetool(manager, servers[0], "status")
    assert len(status_nodes(out)) == 2, out
    await run_nodetool(manager, servers[1], "decommission")
    out, _, _ = await run_nodetool(manager, servers[0], "status")
    assert len(status_nodes(out)) == 1, out


def parse_info(out: str) -> dict:
    res = {}
    for key, value in re.findall(r"^\s*([^\s][^:]*[^:\s])\s*:\s+(.*)\s*$", out, re.MULTILINE):
        res[key] = value
    return res


# "nodetool info" on one of two nodes has its fields, with the datacenter,
# rack and no exceptions; ten seconds later its uptime grew by about that.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_nodetool_info(manager: ScyllaClusterManager):
    servers = await manager.servers_add(2, config=CONFIG, cmdline=CMDLINE)
    out, _, _ = await run_nodetool(manager, servers[0], "info")
    info = parse_info(out)
    for key in ["ID", "Native Transport active", "Load", "Generation No", "Key Cache", "Row Cache", "Counter Cache", "Token"]:
        assert key in info, f"{key} is missing"
    assert info["Gossip active"] == "true"
    assert "Heap Memory (MB)" in info or "Heap Memory (MiB)" in info
    assert "Off Heap Memory (MB)" in info or "Off Heap Memory (MiB)" in info
    assert info["Data Center"] == "datacenter1"
    assert info["Rack"] == "rack1"
    assert info["Exceptions"] == "0"
    uptime = int(info["Uptime (seconds)"])
    # The uptime is wall-clock time, so wait for it.
    await asyncio.sleep(10)
    out, _, _ = await run_nodetool(manager, servers[0], "info")
    assert uptime + 10 <= int(parse_info(out)["Uptime (seconds)"]) <= uptime + 40


# "nodetool refresh -las" loads new sstables with load-and-stream, and
# plain "nodetool refresh" without it (vnodes), as the node logs.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_nodetool_refresh_with_load_and_stream(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG, cmdline=CMDLINE)
    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE ks WITH replication = { 'class':'NetworkTopologyStrategy', 'replication_factor':1}")
    await cql.run_async("CREATE TABLE ks.cf (pk text,ck int,s int,v int ,PRIMARY KEY (pk, ck))")
    log = await manager.server_open_log(server.server_id)
    await run_nodetool(manager, server, "refresh", "-las", "--", "ks", "cf")
    await log.wait_for("Loading new SSTables for keyspace=ks, table=cf, load_and_stream=true, primary_replica_only=false", timeout=10)
    await run_nodetool(manager, server, "refresh", "--", "ks", "cf")
    await log.wait_for("Loading new SSTables for keyspace=ks, table=cf, load_and_stream=false, primary_replica_only=false", timeout=10)


# After stress writes, mixed stress reads and writes and ten token-range
# scans on two nodes, the read, write and range latency percentiles of
# "nodetool proxyhistograms" are non-zero and grow monotonically.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_nodetool_proxyhistograms(manager: ScyllaClusterManager):
    servers = await manager.servers_add(2, config=CONFIG, cmdline=CMDLINE)
    cql = manager.get_cql()
    # As in the dtest, cassandra-stress creates its keyspace itself.
    await cassandra_stress(manager, servers[0], "write", "n=10000", "-rate", "threads=4", create_keyspace=False)
    await cassandra_stress(manager, servers[0], "mixed", "n=10000", "-rate", "threads=4", create_keyspace=False)
    keys = ["0x" + r.key.hex() for r in await cql.run_async("Select * from keyspace1.standard1 limit 100")]
    for i in range(10):
        rows = await cql.run_async(f"SELECT * FROM keyspace1.standard1 WHERE token(key) >= token({keys[i * 10]}) and token(key) <= token({keys[i * 10 + 9]})")
        assert len(list(rows)) == 10
    out, _, _ = await run_nodetool(manager, servers[0], "proxyhistograms")
    histogram = {m[0]: dict(zip(["Read Latency", "Write Latency", "Range Latency", "CAS Read", "CAS Write", "View Write"], m[1:]))
                 for m in re.findall(r"^\s*([^\s]+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s*$", out, re.MULTILINE)}
    for latency_type in ("Read Latency", "Write Latency", "Range Latency"):
        cur = float(histogram["Min"][latency_type])
        for v in ["50%", "75%", "95%", "98%", "99%", "Max"]:
            latency = float(histogram[v][latency_type])
            assert latency >= cur, f"{latency_type} is not monotonic: {latency}({v}), was {cur}"
            cur = latency


# "nodetool version" prints the Cassandra-compatible release version.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_nodetool_version(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG, cmdline=CMDLINE)
    out, _, _ = await run_nodetool(manager, server, "version")
    assert re.match(r"ReleaseVersion: 3\.\d+\.\d+", out)


# sstables with an invalid fragment (from scylla-dtest's test-sstables,
# generated by test/boost/sstable_datafile_test.cc) placed into a table's
# directory while the node is down are scrubbed with "nodetool scrub -m
# SKIP|SEGREGATE|VALIDATE" of the table or its keyspace: the node logs
# the invalid fragments and finishes scrubbing; nodetool fails only with
# the scrub's own error.
async def scrub_sstable_with_invalid_fragment(manager: ScyllaClusterManager, mode, scrub_keyspace=False):
    """The dtests' _scrub_sstable_with_invalid_fragment()."""
    server = await manager.server_add(config=CONFIG, cmdline=CMDLINE)
    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE ks WITH replication = { 'class':'NetworkTopologyStrategy', 'replication_factor':1}")
    await cql.run_async("CREATE TABLE ks.cf (pk text,ck int,s int static,v int ,PRIMARY KEY (pk, ck))")
    await cql.run_async("ALTER TABLE ks.cf WITH compaction = { 'class' : 'NullCompactionStrategy' }")
    await run_nodetool(manager, server, "flush")
    await manager.server_stop_gracefully(server.server_id)
    table_dir = await get_table_dir(manager, server, "ks", "cf")
    src = os.path.join(RESOURCES, "sstable_with_invalid_fragment", "ks", "cf-test")
    for f in os.listdir(src):
        shutil.copy(os.path.join(src, f), table_dir)
    await manager.server_start(server.server_id)
    expected_errs = [r"\[.* compaction ks.cf\] (Invalid|out-of-order) (clustering row|partition)", r"\[.* compaction ks.cf\]  mismatching index/data"]
    if mode == "SKIP":
        expected_errs.append(r"Skipping invalid (clustering row|partition)")
    manager.ignore_log_patterns += expected_errs + ["[Ii]nvalid clustering row fragment", "[Ii]nvalid partition",
                                                    "(Scrub|Validate) compaction.*Invalid", "Validating .* failed",
                                                    "scrub compaction found invalid data", "[Ss]crubbing"]
    log = await manager.server_open_log(server.server_id)
    mark = await log.mark()
    _, err, rc = await run_nodetool(manager, server, "scrub", "-m", mode, "ks", *([] if scrub_keyspace else ["cf"]), check=False)
    if rc != 0:
        assert "scrub failed: aborted" in err or "scrub failed: there are invalid sstables" in err, err
    await log.wait_for("|".join(expected_errs), "Finished scrubbing", from_mark=mark, timeout=90)


# Ported from scylla-dtest nodetool_additional_test.py::TestNodetool::test_scrub_sstable_with_invalid_fragment.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_scrub_sstable_with_invalid_fragment(manager: ScyllaClusterManager):
    await scrub_sstable_with_invalid_fragment(manager, "SKIP")


# Ported from scylla-dtest nodetool_additional_test.py::TestNodetool::test_scrub_ks_sstable_with_invalid_fragment.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_scrub_ks_sstable_with_invalid_fragment(manager: ScyllaClusterManager):
    await scrub_sstable_with_invalid_fragment(manager, "SKIP", scrub_keyspace=True)


# Ported from scylla-dtest nodetool_additional_test.py::TestNodetool::test_scrub_segregate_sstable_with_invalid_fragment.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_scrub_segregate_sstable_with_invalid_fragment(manager: ScyllaClusterManager):
    await scrub_sstable_with_invalid_fragment(manager, "SEGREGATE")


# Ported from scylla-dtest nodetool_additional_test.py::TestNodetool::test_scrub_segregate_ks_sstable_with_invalid_fragment.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_scrub_segregate_ks_sstable_with_invalid_fragment(manager: ScyllaClusterManager):
    await scrub_sstable_with_invalid_fragment(manager, "SEGREGATE", scrub_keyspace=True)


# Ported from scylla-dtest nodetool_additional_test.py::TestNodetool::test_validate_sstable_with_invalid_fragment.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_validate_sstable_with_invalid_fragment(manager: ScyllaClusterManager):
    await scrub_sstable_with_invalid_fragment(manager, "VALIDATE")


# Ported from scylla-dtest nodetool_additional_test.py::TestNodetool::test_validate_ks_sstable_with_invalid_fragment.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_validate_ks_sstable_with_invalid_fragment(manager: ScyllaClusterManager):
    await scrub_sstable_with_invalid_fragment(manager, "VALIDATE", scrub_keyspace=True)


# "nodetool sstableinfo" of a flushed table names its keyspace and table.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_nodetool_sstableinfo(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG, cmdline=CMDLINE)
    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE ks1 WITH replication = { 'class':'NetworkTopologyStrategy', 'replication_factor':1}")
    await cql.run_async("CREATE TABLE ks1.tbl1 (col1 int,col2 text ,PRIMARY KEY (col1))")
    await cql.run_async("INSERT INTO ks1.tbl1 (col1,col2) VALUES (4,'abc')")
    await run_nodetool(manager, server, "flush")
    out, _, _ = await run_nodetool(manager, server, "sstableinfo", "ks1", "tbl1")
    assert "ks1" in out and "tbl1" in out


# On a three-node cluster (two racks), "nodetool settraceprobability" with
# values out of [0, 1] or not a number fails with an error, and leaves the
# probability set before (per "nodetool gettraceprobability").
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_invalid_value_not_changing_trace_probability(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config=CONFIG, property_file=[{"dc": "dc1", "rack": "rack1"}, {"dc": "dc1", "rack": "rack1"},
                                                                        {"dc": "dc1", "rack": "rack2"}])
    invalid_values = {
        "-0.1": "(Trace probability must be between 0 and 1|error processing arguments: trace probability must be between 0 and 1)",
        "1.01": "(Trace probability must be between 0 and 1|error processing arguments: trace probability must be between 0 and 1)",
        "a": "(can not convert|error: the argument \\('a'\\) for option '--trace_probability' is invalid)",
    }
    for invalid_value, message in invalid_values.items():
        await run_nodetool(manager, servers[0], "settraceprobability", "0.001")
        out, err, rc = await run_nodetool(manager, servers[0], "settraceprobability", "--", invalid_value, check=False)
        assert rc != 0 and re.search(message, out + err), f"invalid_value={invalid_value}: {out} {err}"
        out, _, _ = await run_nodetool(manager, servers[0], "gettraceprobability")
        assert float(re.match(r"Current trace probability: (?P<probability>[0-9.eE]+)(\n)*$", out).group(1)) == 0.001


def cfstats_keyspaces(out: str) -> dict[str, set[str]]:
    """The keyspaces and tables in "nodetool cfstats" output, parsed like the
    dtest's _to_cfstats(): a "Keyspace : <name>" line starts a keyspace,
    "Table: <name>" lines name its tables, and a line of dashes ends it."""
    res, ks = {}, None
    for line in (s.strip() for s in out.splitlines()):
        m = re.match(r"^([^:]+?)\s*:\s*(\S.*?)\s*$", line)
        if m and m.group(1) == "Keyspace":
            ks = m.group(2)
            res[ks] = set()
        elif m and m.group(1) == "Table" and ks is not None:
            res[ks].add(m.group(2))
        elif "----------------" in line:
            ks = None
    return {k: v for k, v in res.items() if "system" not in k and "audit" not in k}


# Ported from scylla-dtest nodetool_additional_test.py::TestNodetool::test_cfstats_syntax.
# "nodetool cfstats" with no argument, keyspaces, keyspace.table and
# keyspace/table lists exactly the keyspaces and tables asked for.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_cfstats_syntax(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG)
    cql = manager.get_cql()
    tables = {}
    for ks in ["ks1", "ks2"]:
        cql.execute(f"CREATE KEYSPACE {ks} WITH replication = {{'class':'NetworkTopologyStrategy', 'replication_factor':1}}")
        cql.execute(f"USE {ks}")
        tables[ks] = {f"{ks}_cf1", f"{ks}_cf2"}
        for cf in sorted(tables[ks]):
            cql.execute(f"CREATE TABLE {cf} (id int primary key, val int)")
    for options, expected in [("", tables), ("ks1", {"ks1": tables["ks1"]}), ("ks2 ks1", tables),
                              ("ks2.ks2_cf2", {"ks2": {"ks2_cf2"}}), ("ks1/ks1_cf2", {"ks1": {"ks1_cf2"}}),
                              ("ks1/", {"ks1": tables["ks1"]})]:
        out, _, _ = await run_nodetool(manager, server, "cfstats", *options.split())
        assert cfstats_keyspaces(out) == expected, f"nodetool cfstats {options}"


# The dtests' cluster.populate(n): SimpleSnitch, every node in datacenter1/rack1.
SIMPLE_SNITCH = {"endpoint_snitch": "SimpleSnitch"}
# generate_cluster_topology(rack_num=n): datacenter1, one node per rack.
def racks_layout(racks: list[str], dc: str = "datacenter1") -> list[dict]:
    return [{"dc": dc, "rack": r} for r in racks]


async def stress_write(manager: ScyllaClusterManager, server, *opts: str, times: int | None = None, duration: str | None = None):
    """The dtests' stress_write(): keyspace1 (RF=1, kept if it exists), then
    "write cl=ALL no-warmup n=<times>|duration=<duration> <opts>"."""
    return await cassandra_stress(manager, server, "write", "cl=ALL", "no-warmup",
                                  f"duration={duration}" if duration else f"n={times}", *opts)


async def nodetool_out(manager: ScyllaClusterManager, server, *args: str) -> str:
    out, _, _ = await run_nodetool(manager, server, *args)
    return out


def sstables_compression(workdir: str, keyspace: str = "keyspace1") -> list[str]:
    """The dtests' get_node_sstables_compression(): for each CompressionInfo
    file of the keyspace, its printable strings that name a Compressor
    ("strings <file> | grep Compressor")."""
    compressions = []
    for path in glob.glob(os.path.join(workdir, "data", keyspace, "**", "*CompressionInfo*"), recursive=True):
        with open(path, "rb") as f:
            found = [s.decode() for s in re.findall(rb"[\x20-\x7e\t]{4,}", f.read()) if b"Compressor" in s]
        if found:
            compressions.append("\n".join(found).strip())
    return compressions


# Ported from scylla-dtest nodetool_additional_test.py::TestNodetool::test_shutdown_during_sstable_upgrade.
# Two nodes (racks rack1, rack2) with one shard, keyspace1 RF=2 with an LZ4
# table, autocompaction off on node1 and three flushed rounds of stress
# writes; the table is altered to Snappy and node1 is stopped while
# "nodetool upgradesstables -a" runs: its sstables keep LZ4, and after a
# restart a second upgradesstables rewrites them with Snappy.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_shutdown_during_sstable_upgrade(manager: ScyllaClusterManager):
    servers = await manager.servers_add(2, config=CONFIG, cmdline=CMDLINE + ["--smp", "1"], property_file=racks_layout(["rack1", "rack2"]))
    node1 = servers[0]
    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE keyspace1 WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '2'}")
    await cql.run_async("""CREATE TABLE keyspace1.standard1 (key blob PRIMARY KEY,"C0" blob,"C1" blob,"C2" blob,"C3" blob,"C4" blob)
            WITH compression = {'sstable_compression' : 'LZ4Compressor', 'chunk_length_in_kb': 64, 'crc_check_chance': 0.5} """)
    await run_nodetool(manager, node1, "disableautocompaction")
    writes_per_sstable = 10000
    for start_pk in range(1, 3 * (writes_per_sstable + 1), writes_per_sstable):
        await stress_write(manager, node1, "-pop", f"seq={start_pk}..{start_pk + writes_per_sstable}", "-rate", "threads=10", duration="10s")
        await run_nodetool(manager, node1, "flush")
    workdir = await manager.server_get_workdir(node1.server_id)
    compressions = sstables_compression(workdir)
    assert "SnappyCompressor" not in compressions, f"Compression type unexpectedly updated for sstables: {compressions}"

    await cql.run_async("ALTER TABLE keyspace1.standard1 WITH compression = {'sstable_compression' : 'SnappyCompressor', 'chunk_length_in_kb': 64, 'crc_check_chance': 0.5}")

    async def upgradesstables():
        out, err, rc = await run_nodetool(manager, node1, "upgradesstables", "-a", check=False)
        logger.info(f"upgradesstables: rc={rc} output: {out} errors: {err}")

    upgrade = asyncio.create_task(upgradesstables())
    await manager.server_stop_gracefully(node1.server_id)
    await manager.server_not_sees_other_server(servers[1].ip_addr, node1.ip_addr)
    await upgrade
    compressions = sstables_compression(workdir)
    assert "SnappyCompressor" not in compressions, f"Compression type unexpectedly updated for node1 sstables: {compressions}"
    await manager.server_start(node1.server_id)
    await upgradesstables()
    compressions = sstables_compression(workdir)
    assert "SnappyCompressor" in compressions, "Compression type unexpectedly not updated for node1 sstables"


async def isrunning(manager: ScyllaClusterManager, server, cmd: str) -> bool:
    """The dtests' isrunning(): "nodetool <cmd>" says running or not running."""
    out = await nodetool_out(manager, server, cmd)
    if re.search(r"^\s*running\s*$", out):
        return True
    if re.search(r"^\s*not running\s*$", out):
        return False
    pytest.fail(f"{cmd} return wrong value: {out}")


# Ported from scylla-dtest nodetool_additional_test.py::TestNodetool::test_gossip_control.
# "nodetool info" and "statusgossip" show gossip active, not active after
# "disablegossip", and active again after "enablegossip".
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_gossip_control(manager: ScyllaClusterManager):
    server = await manager.server_add(config={**CONFIG, **SIMPLE_SNITCH}, cmdline=CMDLINE)
    for command, active, status in [(None, "true", r"\s*running\s*"), ("disablegossip", "false", r"\s*not running\s*"),
                                    ("enablegossip", "true", r"\s*running\s*")]:
        if command:
            await run_nodetool(manager, server, command)
        assert parse_info(await nodetool_out(manager, server, "info"))["Gossip active"] == active
        assert re.fullmatch(status, await nodetool_out(manager, server, "statusgossip")), "wrong gossip status"


# Ported from scylla-dtest nodetool_additional_test.py::TestNodetool::test_backup.
# Incremental backup is off, on after "nodetool enablebackup" and off after
# "disablebackup", per "statusbackup".
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_backup(manager: ScyllaClusterManager):
    server = await manager.server_add(config={**CONFIG, **SIMPLE_SNITCH}, cmdline=CMDLINE)
    assert not await isrunning(manager, server, "statusbackup"), "backup is working"
    await run_nodetool(manager, server, "enablebackup")
    assert await isrunning(manager, server, "statusbackup"), "Fail to enable backup"
    await run_nodetool(manager, server, "disablebackup")
    assert not await isrunning(manager, server, "statusbackup"), "Fail to disable backup"


def parse_gossipinfo(out: str) -> dict:
    """The dtests' nodetool_gossipinfo()."""
    gossipinfo, current = {}, None
    for line in out.splitlines():
        try:
            if current and current not in gossipinfo:
                gossipinfo[current] = {}
            key, value = line.strip().split(":", 1)
            gossipinfo[current].update({key: value})
        except Exception:  # noqa: BLE001
            current = line.strip()[1:]
    return gossipinfo


# Ported from scylla-dtest nodetool_additional_test.py::TestNodetool::test_gossipinfo.
# "nodetool gossipinfo" on one of two nodes lists both, each with its
# generation, heartbeat and application states.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_gossipinfo(manager: ScyllaClusterManager):
    servers = await manager.servers_add(2, config={**CONFIG, **SIMPLE_SNITCH}, cmdline=CMDLINE)
    deadline = time.time() + 60
    while True:
        try:
            out, err, _ = await run_nodetool(manager, servers[0], "gossipinfo")
            assert not err, f"nodetool gossipinfo failed: {err}"
            gi = parse_gossipinfo(out)
            assert len(gi) == 2, "wrong number of nodes"
            for info in gi.values():
                for key in ["generation", "heartbeat", "STATUS", "HOST_ID", "RELEASE_VERSION", "SCHEMA", "NET_VERSION",
                            "LOAD", "RACK", "RPC_ADDRESS", "DC"]:
                    assert key in info, key
            break
        except Exception:  # noqa: BLE001
            if time.time() > deadline:
                raise
            await asyncio.sleep(0.25)


# Ported from scylla-dtest nodetool_additional_test.py::TestNodetool::test_nodetool_refresh_with_data_perms.
# With the data directory made unwritable (mode 644, decimal, as in the
# dtest), "enablebinary" and "enablegossip" succeed without permission
# errors in the log, and "nodetool refresh" fails with Permission denied.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_nodetool_refresh_with_data_perms(manager: ScyllaClusterManager):
    error_to_track = re.compile("Storage I/O error: 13|Permission denied")
    server = await manager.server_add(config={**CONFIG, **SIMPLE_SNITCH}, cmdline=CMDLINE)
    await stress_write(manager, server, times=10000)
    await run_nodetool(manager, server, "flush")
    await run_nodetool(manager, server, "compact")
    await run_nodetool(manager, server, "refresh", "keyspace1", "standard1")
    data_dir = os.path.join(await manager.server_get_workdir(server.server_id), "data")
    log = await manager.server_open_log(server.server_id)

    async def errors():
        return [line for line, _ in await log.grep(error_to_track.pattern) if "perf_event" not in line]

    try:
        os.chmod(data_dir, 644)
        for command in ["enablebinary", "enablegossip"]:
            out, err, _ = await run_nodetool(manager, server, command)
            assert (out, err) == ("", ""), f"{command} not set"
            found = await errors()
            assert not found, f"Permission denied errors found: {found}"
        out, err, rc = await run_nodetool(manager, server, "refresh", "keyspace1", "standard1", check=False)
        assert rc != 0
        assert error_to_track.search(out + err), f"expected error not found in nodetool error message: {out} {err}"
    finally:
        os.chmod(data_dir, stat.S_IWRITE | stat.S_IREAD | stat.S_IEXEC)


async def refresh_expect_fail(manager: ScyllaClusterManager, server, expected_error: str):
    """The dtests' _nodetool_refresh_expect_fail()."""
    manager.ignore_log_patterns.append(expected_error)
    out, err, rc = await run_nodetool(manager, server, "refresh", "--", "keyspace1", "standard1", check=False)
    assert rc != 0, "nodetool refresh unexpectedly succeeded"
    assert re.search(expected_error, out + err), f"/{expected_error}/ not found in '{out} {err}'"


# Ported from scylla-dtest nodetool_additional_test.py::TestNodetool::test_nodetool_refresh_with_wrong_upload_modes.
# "nodetool refresh" fails when the upload directory is not writable, when
# the sstables in it are not readable, and when it holds a symlink.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_nodetool_refresh_with_wrong_upload_modes(manager: ScyllaClusterManager):
    server = await manager.server_add(config={**CONFIG, **SIMPLE_SNITCH}, cmdline=CMDLINE)
    await stress_write(manager, server, times=10000)
    await run_nodetool(manager, server, "flush")
    cf_dir = await get_table_dir(manager, server, "keyspace1", "standard1")
    upload_dir = os.path.join(cf_dir, "upload")
    for f in os.listdir(cf_dir):
        pathname = os.path.join(cf_dir, f)
        if stat.S_ISREG(os.lstat(pathname).st_mode):
            shutil.copy2(pathname, os.path.join(upload_dir, f))

    os.chmod(upload_dir, 0o555)
    await refresh_expect_fail(manager, server, r"Directory cannot be accessed .* write")
    os.chmod(upload_dir, 0o755)

    for f in os.listdir(upload_dir):
        os.chmod(os.path.join(upload_dir, f), 0o044)
    await refresh_expect_fail(manager, server, r"File cannot be accessed for read|open failed: Permission denied")
    for f in os.listdir(upload_dir):
        os.chmod(os.path.join(upload_dir, f), 0o644)

    symlink_path = os.path.join(upload_dir, "test_symlink")
    os.symlink("broken", symlink_path)
    await refresh_expect_fail(manager, server, r"Must be either a regular file or a directory")
    os.remove(symlink_path)


async def create_table(cql, ks: str, table: str, columns: dict, key: str, rf: int = 1):
    """The dtests' create_table() for one keyspace and table."""
    await cql.run_async(f"CREATE KEYSPACE {ks} WITH replication = {{ 'class':'NetworkTopologyStrategy', 'replication_factor':{rf}}}")
    cols = ",".join(f"{k} {v}" for k, v in columns.items())
    await cql.run_async(f"CREATE TABLE {ks}.{table} ({cols} ,PRIMARY KEY ({key}))")


# Ported from scylla-dtest nodetool_additional_test.py::TestNodetool::test_get_sstable.
# "nodetool getsstables" names the sstable holding an int, a blob and a text
# key, and prints nothing for a key no sstable holds.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_get_sstable(manager: ScyllaClusterManager):
    server = await manager.server_add(config={**CONFIG, **SIMPLE_SNITCH}, cmdline=CMDLINE)
    cql = manager.get_cql()
    for ks, table, key_type, value in [("ks1", "tbl1", "int", "4"), ("ks2", "tbl2", "blob", "0x39303138374b4d343830"),
                                       ("ks3", "tbl3", "text", "'keytest'")]:
        await create_table(cql, ks, table, {"col1": key_type, "col2": "text"}, "col1")
        await cql.run_async(f"INSERT INTO {ks}.{table} (col1,col2) VALUES ({value},'abc')")
    await run_nodetool(manager, server, "flush")
    for ks, table, key, found in [("ks1", "tbl1", "4", True), ("ks1", "tbl1", "5", False),
                                  ("ks2", "tbl2", "39303138374b4d343830", True), ("ks2", "tbl2", "39303138374b4d343831", False),
                                  ("ks3", "tbl3", "keytest", True), ("ks3", "tbl3", "keytest1", False)]:
        out = await nodetool_out(manager, server, "getsstables", ks, table, key)
        if found:
            assert f"{ks}/{table}" in out, "key was not found in the sstable"
        else:
            assert out == "", "unexpected sstable return for the key"


def status_of(out: str, address: str):
    return next((s["status"] for s in status_nodes(out) if s["address"] == address), None)


# Ported from scylla-dtest nodetool_additional_test.py::TestNodetool::test_node_graceful_stop_during_stress_and_decommission.
# On four nodes with hinted handoff and batch commitlog, stress writes at
# QUORUM run through node2 while node4 is decommissioned; once node4 starts
# streaming, node3 is stopped gracefully. The stop completes (scylladb#4491)
# and node2 sees node3 down.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_node_graceful_stop_during_stress_and_decommission(manager: ScyllaClusterManager):
    config = {**CONFIG, **SIMPLE_SNITCH, "hinted_handoff_enabled": True, "commitlog_sync": "batch",
              "commitlog_sync_batch_window_in_ms": 5}
    servers = await manager.servers_add(4, config=config, cmdline=CMDLINE + ["--logger-log-level", "stream_session=debug"])
    _, node2, node3, node4 = servers
    # node2.stress(): cassandra-stress creates keyspace1 itself. test.py's
    # nodes require authentication, so it logs in. Its output goes to a file,
    # logged at the end.
    stress_out = tempfile.TemporaryFile()
    stress = await asyncio.create_subprocess_exec(
        "cassandra-stress", "write", "cl=QUORUM", "no-warmup", "duration=15m",
        "-mode", "cql3", "native", "user=cassandra", "password=cassandra", "-rate", "threads=300",
        "-pop", "seq=1..100000000", "-log", "interval=5", "-node", node2.ip_addr,
        stdout=stress_out, stderr=asyncio.subprocess.STDOUT)
    try:
        node4_log = await manager.server_open_log(node4.server_id)
        mark = await node4_log.mark()
        node4_host_id = await manager.get_host_id(node4.server_id)
        manager.ignore_log_patterns += [
            rf"Failed to handle STREAM_MUTATION_FRAGMENTS.*peer=(?:{re.escape(node4.ip_addr)}|{node4_host_id})",
            "decommission.*Operation failed", "raft_topology - Decommission failed.",
            r"raft_topology - raft_topology_cmd.*failed with: (?:std::runtime_error[ :]+\(?repair|repair)",
            r"raft_topology - raft_topology_cmd.*failed with: raft::request_aborted[ :]+\(?Request is aborted by a caller\)?",
            r"raft_topology - send_raft_topology_cmd\(stream_ranges\) failed with exception \(node state is decommissioning\)",
            r"raft_topology - raft_topology_cmd stream_ranges failed with: .*Stream failed",
            rf"[Rr]epair.*mandatory neighbor=(?:{node3.ip_addr}|{await manager.get_host_id(node3.server_id)}) is not alive",
        ]
        # node4.decommission(): its failure is ignored.
        decommission = asyncio.create_task(run_nodetool(manager, node4, "decommission", check=False))
        await node4_log.wait_for(f"raft_topology - request decommission for: {node4_host_id}", from_mark=mark)
        await node4_log.wait_for("sync data for keyspace=.*, status=started|Beginning stream session", from_mark=mark)
        await asyncio.sleep(2)
        await manager.server_stop_gracefully(node3.server_id)
        # The dtest checks the decommission thread every 10 s, while the
        # stress keeps writing.
        while True:
            await asyncio.sleep(10)
            if decommission.done():
                break
        await decommission
        out = await nodetool_out(manager, node2, "status")
        node3_status = status_of(out, node3.ip_addr)
        assert node3_status is not None, f"{node3.ip_addr} not found in {status_nodes(out)}"
        assert node3_status == "DN", f"Node 3 status is incorrect (should be DN) Instead we got {node3_status}"
        assert node3.server_id not in {s.server_id for s in await manager.running_servers()}, "Node 3 process didn't stop/exit correctly"
    finally:
        exited = stress.returncode
        if stress.returncode is None:
            stress.kill()
            await stress.wait()
        stress_out.seek(0)
        output = stress_out.read().decode(errors="replace")
        stress_out.close()
        logger.info("cassandra-stress (%s): %s", "killed" if exited is None else f"exited with {exited}", output[-20000:])


async def wait_for_nodes_status(manager: ScyllaClusterManager, server, expected: dict[str, str], timeout: float = 90):
    """The dtests' wait_for_nodes_status(), which expects the statuses in the
    order "nodetool status" lists the nodes: by address, so node1 (127.0.0.1)
    first. Here the nodes' addresses need not be in node order, so the
    expected statuses are keyed by address."""
    deadline = time.time() + timeout
    while True:
        actual = {s["address"]: s["status"] for s in status_nodes(await nodetool_out(manager, server, "status"))}
        if actual == expected:
            return
        assert time.time() < deadline, f"found statuses: {actual}, expected: {expected}"
        await asyncio.sleep(1)


# Ported from scylla-dtest nodetool_additional_test.py::TestNodetool::test_disablebinary_and_disablegossip.
# Three nodes in three racks, 100000 rows written at ALL by cassandra-stress
# (RF=3), reads at QUORUM going on: with the native transport and gossip of
# node1 disabled, node2 sees it down and node1 compacts; once both are
# enabled again, node1 logs no "gate closed", is seen up, and all rows read
# at ALL through it.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_disablebinary_and_disablegossip(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config=CONFIG, cmdline=CMDLINE, property_file=racks_layout(["rack1", "rack2", "rack3"]))
    node1, node2, _ = servers
    number_of_keys = 100000

    async def run_stress(server, mode, consistency, rate="threads=10"):
        try:
            await cassandra_stress(manager, server, mode, f"n={number_of_keys}", f"cl={consistency}", "-rate", rate,
                                   "-schema", "replication(factor=3)", create_keyspace=False)
        except AssertionError as e:
            # The dtest does not check the result of the stress runs.
            logger.info(f"cassandra-stress failed: {e}")

    await run_stress(node2, "write", "ALL")
    read_stress = asyncio.create_task(run_stress(node2, "read", "QUORUM", "threads=10 throttle=5000/s"))
    for s in servers:
        await run_nodetool(manager, s, "flush")
    await run_nodetool(manager, node1, "disablebinary")
    try:
        await run_nodetool(manager, node1, "disablegossip")
        try:
            await wait_for_nodes_status(manager, node2, {s.ip_addr: "DN" if s == node1 else "UN" for s in servers}, timeout=120)
            await run_nodetool(manager, node1, "compact")
        finally:
            await run_nodetool(manager, node1, "enablegossip")
    finally:
        await run_nodetool(manager, node1, "enablebinary")
    log = await manager.server_open_log(node1.server_id)
    mark = await log.mark()
    await asyncio.sleep(10)
    assert not await log.grep("gate closed", from_mark=mark), "After executing enablebinary, the node still prints 'gate closed' messages"
    await wait_for_nodes_status(manager, node2, {s.ip_addr: "UN" for s in servers}, timeout=120)
    node1_status = status_of(await nodetool_out(manager, node2, "status"), node1.ip_addr)
    assert node1_status, f"{node1.ip_addr} not found"
    assert node1_status == "UN", f"Node 1's status is expected to be UN, but instead it's {node1_status}"
    await read_stress
    await run_stress(node1, "read", "ALL")


# TestGetTraceProbability: three nodes, rack1 x2 and rack2, in dc1.
TRACE_LAYOUT = [{"dc": "dc1", "rack": "rack1"}, {"dc": "dc1", "rack": "rack1"}, {"dc": "dc1", "rack": "rack2"}]
TRACE_VALID_VALUES = {0.01: 10000, 0: 500, 0.6: 10000, 1: 500}
TRACE_VALID_TOLERANCE = {0.01: (0.005, 0.015), 0: (0, 0), 0.6: (0.3, 0.9), 1: (1, 1)}


async def get_node_probability(manager: ScyllaClusterManager, server) -> float:
    out = await nodetool_out(manager, server, "gettraceprobability")
    m = re.match("Current trace probability: (?P<probability>[0-9.eE]+)(\\n)*$", out)
    if not m:
        raise ValueError(f"Not found probability in {out}")
    return float(m.group("probability"))


async def set_node_probability(manager: ScyllaClusterManager, server, value) -> None:
    await run_nodetool(manager, server, "settraceprobability", str(value))


def random_valid_values() -> list:
    values = list(TRACE_VALID_VALUES)
    random.shuffle(values)
    return values


# Ported from scylla-dtest nodetool_additional_test.py::TestGetTraceProbability::test_after_stop_start_value_is_default.
# For each valid value, set on node1, node1 has the default probability 0
# again after a stop and start.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_after_stop_start_value_is_default(manager: ScyllaClusterManager):
    node1, node2, node3 = await manager.servers_add(3, config=CONFIG, property_file=TRACE_LAYOUT)
    for value in random_valid_values():
        await set_node_probability(manager, node1, value)
        await manager.server_stop_gracefully(node1.server_id)
        for other in (node2, node3):
            await manager.server_not_sees_other_server(other.ip_addr, node1.ip_addr)
        await manager.server_start(node1.server_id)
        assert await get_node_probability(manager, node1) == 0, f"node1 Expect: {value}"


# Ported from scylla-dtest nodetool_additional_test.py::TestGetTraceProbability::test_valid_value_affect_only_one_node.
# Setting the probability on node1 leaves node2's value alone.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_valid_value_affect_only_one_node(manager: ScyllaClusterManager):
    node1, node2, _ = await manager.servers_add(3, config=CONFIG, property_file=TRACE_LAYOUT)
    node2_value = 0.1234
    await set_node_probability(manager, node2, node2_value)
    for value in random_valid_values():
        await set_node_probability(manager, node1, value)
        assert await get_node_probability(manager, node1) == value
        assert await get_node_probability(manager, node2) == node2_value


# Ported from scylla-dtest nodetool_additional_test.py::TestGetTraceProbability::test_value_affect_tracing_table.
# With each valid probability set on every node, unprepared inserts through
# node1 (RF=2) are traced in system_traces.sessions in about that share.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_value_affect_tracing_table(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config=CONFIG, property_file=TRACE_LAYOUT)
    node1 = servers[0]
    cql = manager.con_gen([node1.ip_addr], load_balancing_policy=WhiteListRoundRobinPolicy([node1.ip_addr])).connect()
    try:
        cql.execute("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':2}")
        cql.execute("USE ks")
        cql.execute("CREATE COLUMNFAMILY cf (key varchar PRIMARY KEY, c1 text, c2 text) WITH comment='test cf' "
                    "AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'} AND compression = {} "
                    "AND read_repair_chance=0.000000")
        cql.execute("SELECT * FROM cf LIMIT 1")
        prev_count = 0
        for value in random_valid_values():
            for s in servers:
                await set_node_probability(manager, s, value)
            probability = await get_node_probability(manager, node1)
            num_keys = TRACE_VALID_VALUES[value]
            execute_concurrent(cql, [(SimpleStatement(f"INSERT INTO ks.cf (key, c1, c2) VALUES ('k{k}', 'value1', 'value2')",
                                                      consistency_level=ConsistencyLevel.QUORUM), ()) for k in range(num_keys)])
            # Tracing sessions are written every 2 seconds (tracing::write_period).
            count = None
            for _ in range(5):
                await asyncio.sleep(2)
                rows = list(cql.execute(SimpleStatement("SELECT parameters FROM system_traces.sessions")))
                cur_count = sum(len(re.findall("INSERT INTO", row[0]["query"])) for row in rows) if rows else 0
                if count == cur_count:
                    break
                count = cur_count
            actual = (count - prev_count) / num_keys
            allowed_min, allowed_max = TRACE_VALID_TOLERANCE[value]
            assert allowed_min <= actual <= allowed_max, \
                f"Error: probability={probability}: actual={actual} allowed_tolerance=[{allowed_min}, {allowed_max})"
            prev_count = count
    finally:
        cql.cluster.shutdown()
