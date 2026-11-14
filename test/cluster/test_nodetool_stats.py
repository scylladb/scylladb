#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Tests running the real "scylla nodetool" against a real cluster for the
# commands which report table and cluster statistics: tablestats (cfstats),
# tablehistograms (cfhistograms) and describecluster. test/nodetool/ tests
# the same commands against a mocked REST API, test/rest_api/ tests the
# REST endpoints they read against a single node.
#
# Ported from dtest's nodetool_additional_test.py::TestNodetool:
# test_cfstats, test_cfstats_syntax, test_cfhistograms,
# test_sstables_in_tablehistograms and test_describecluster.

import asyncio
import logging
import re
import subprocess
import time

import pytest
from cassandra import ConsistencyLevel

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.prepare_3_racks_cluster

RF3 = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"


async def nodetool(manager: ScyllaClusterManager, server, *args: str) -> str:
    cmd = [await manager.server_get_exe(server.server_id), "nodetool", "-h", server.ip_addr, *args]
    logger.info(f"Running {cmd}")
    res = await asyncio.to_thread(subprocess.run, cmd, capture_output=True, text=True)
    assert res.returncode == 0, f"{cmd} failed with {res.returncode}: {res.stderr}"
    return res.stdout


KEYSPACE_STATS = ["Read Count", "Read Latency", "Write Count", "Write Latency", "Pending Flushes"]

TABLE_STATS = [
    "SSTable count", "Space used (live)", "Space used (total)", "Space used by snapshots (total)",
    "Off heap memory used (total)", "SSTable Compression Ratio", "Number of partitions (estimate)",
    "Memtable cell count", "Memtable data size", "Memtable off heap memory used", "Memtable switch count",
    "Local read count", "Local read latency", "Local write count", "Local write latency", "Pending flushes",
    "Bloom filter false positives", "Bloom filter false ratio", "Bloom filter space used",
    "Bloom filter off heap memory used", "Index summary off heap memory used",
    "Compression metadata off heap memory used", "Compacted partition minimum bytes",
    "Compacted partition maximum bytes", "Compacted partition mean bytes",
    "Average live cells per slice (last five minutes)", "Maximum live cells per slice (last five minutes)",
    "Average tombstones per slice (last five minutes)", "Maximum tombstones per slice (last five minutes)",
]


def parse_tablestats(out: str) -> dict:
    """Parses the plain text output of nodetool tablestats into
    {keyspace: {"tables": {table: {stat: value}}, stat: value}}"""
    res = {}
    ks = None
    table = None
    for line in out.splitlines():
        key, sep, value = line.strip().partition(":")
        if not sep:
            continue
        key = key.strip()
        value = value.strip()
        if key == "Keyspace":
            ks = res.setdefault(value, {"tables": {}})
            table = None
        elif key == "Table":
            table = ks["tables"].setdefault(value, {})
        elif table is not None:
            table[key] = value
        elif ks is not None:
            ks[key] = value
    return res


def number(value: str) -> float:
    # Strip units, like in "0.123 ms".
    return float(value.split()[0])


def parse_tablehistograms(out: str) -> dict:
    """Parses the output of nodetool tablehistograms into
    {row label ("50%", ..., "Min", "Max"): {column: value}}"""
    columns = ["SSTables", "Write Latency", "Read Latency", "Partition Size", "Cell Count"]
    res = {}
    for line in out.splitlines():
        fields = line.split()
        if len(fields) == len(columns) + 1 and re.match(r"^(\d+%|Min|Max)$", fields[0]):
            res[fields[0]] = dict(zip(columns, map(float, fields[1:])))
    return res


async def write_rows(cql, table: str, keys, value: str):
    stmt = cql.prepare(f"INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)")
    stmt.consistency_level = ConsistencyLevel.ALL
    for pk, ck in keys:
        await cql.run_async(stmt, [pk, ck, value])


async def test_nodetool_tablestats_and_tablehistograms(manager: ScyllaClusterManager):
    servers = await manager.running_servers()
    cql, _ = await manager.get_ready_cql(servers)
    node = servers[0]
    async with new_test_keyspace(manager, RF3) as ks:
        cf = "cf"
        table = f"{ks}.{cf}"
        await cql.run_async(f"CREATE TABLE {table} (pk int, ck int, v text, PRIMARY KEY (pk, ck))")
        await nodetool(manager, node, "disableautocompaction", ks, cf)

        # Every node is a replica of every row, and writes are CL=ALL, so
        # node's local counters know about all of them.
        partitions = 20
        rows = 10
        flushes = 3
        for i in range(flushes):
            await write_rows(cql, table, [(i * partitions + pk, ck) for pk in range(partitions) for ck in range(rows)], "x" * 100)
            await nodetool(manager, node, "flush", ks, cf)
        writes = flushes * partitions * rows
        # Keep something in the memtable.
        await write_rows(cql, table, [(0, ck) for ck in range(rows)], "y" * 100)
        writes += rows

        read = cql.prepare(f"SELECT * FROM {table} WHERE pk = ? BYPASS CACHE")
        read.consistency_level = ConsistencyLevel.ALL
        for pk in range(flushes * partitions):
            await cql.run_async(read, [pk])

        stats = parse_tablestats(await nodetool(manager, node, "tablestats", table))
        assert list(stats.keys()) == [ks]
        assert list(stats[ks]["tables"].keys()) == [cf]
        ks_stats = stats[ks]
        assert int(ks_stats["Write Count"]) == writes
        assert int(ks_stats["Read Count"]) >= 1
        assert int(ks_stats["Pending Flushes"]) == 0

        t = ks_stats["tables"][cf]
        logger.info(f"tablestats of {table}: {t}")
        assert int(t["SSTable count"]) >= flushes
        assert number(t["Space used (live)"]) > 0
        assert number(t["Space used (total)"]) >= number(t["Space used (live)"])
        assert number(t["Space used by snapshots (total)"]) == 0
        assert 0 < float(t["SSTable Compression Ratio"]) <= 1
        assert int(t["Number of partitions (estimate)"]) >= flushes * partitions
        assert int(t["Memtable cell count"]) > 0
        assert number(t["Memtable data size"]) > 0
        assert number(t["Memtable off heap memory used"]) > 0
        assert int(t["Memtable switch count"]) >= flushes
        assert int(t["Local write count"]) == writes
        assert int(t["Local read count"]) == int(ks_stats["Read Count"])
        assert number(t["Local read latency"]) >= 0
        assert number(t["Local write latency"]) >= 0
        assert int(t["Pending flushes"]) == 0
        assert int(t["Bloom filter false positives"]) >= 0
        assert 0 <= float(t["Bloom filter false ratio"]) <= 1
        assert number(t["Bloom filter space used"]) > 0
        assert number(t["Bloom filter off heap memory used"]) > 0
        assert number(t["Index summary off heap memory used"]) > 0
        assert number(t["Off heap memory used (total)"]) >= \
            number(t["Bloom filter off heap memory used"]) + number(t["Index summary off heap memory used"])
        min_size = number(t["Compacted partition minimum bytes"])
        mean_size = number(t["Compacted partition mean bytes"])
        max_size = number(t["Compacted partition maximum bytes"])
        assert 0 < min_size <= mean_size <= max_size
        assert float(t["Average live cells per slice (last five minutes)"]) >= 0
        assert float(t["Maximum live cells per slice (last five minutes)"]) >= 0
        assert float(t["Average tombstones per slice (last five minutes)"]) >= 0
        assert float(t["Maximum tombstones per slice (last five minutes)"]) >= 0

        # tablehistograms, in its three spellings.
        for args in [[ks, cf], [table], [f"{ks}/{cf}"]]:
            out = await nodetool(manager, node, "tablehistograms", *args)
            assert out.startswith(f"{ks}/{cf} histograms"), out
        hist = parse_tablehistograms(out)
        assert list(hist.keys()) == ["50%", "75%", "95%", "98%", "99%", "Min", "Max"], out
        for column in ["Write Latency", "Read Latency", "Partition Size", "Cell Count", "SSTables"]:
            prev = 0
            for row in ["50%", "75%", "95%", "98%", "99%", "Max"]:
                assert hist[row][column] >= prev, f"{column} is not monotonic:\n{out}"
                prev = hist[row][column]
            assert hist["Min"][column] <= hist["50%"][column], out
        assert hist["Max"]["Write Latency"] > 0, out
        assert hist["Max"]["Partition Size"] > 0, out
        assert hist["Max"]["Cell Count"] >= rows, out

        # The SSTables column agrees with the REST API (dtest's test_sstables_in_tablehistograms).
        api_hist = await manager.api.client.get_json(f"/column_family/metrics/sstables_per_read_histogram/{ks}:{cf}", host=node.ip_addr)
        offsets = api_hist["bucket_offsets"]
        buckets = api_hist["buckets"]
        assert buckets[-1] == 0, f"histogram overflows: {api_hist}"
        assert sum(buckets) > 0, api_hist
        non_empty = [i for i, b in enumerate(buckets) if b > 0]
        assert hist["Max"]["SSTables"] == offsets[non_empty[-1]]
        assert hist["Min"]["SSTables"] == (0 if non_empty[0] == 0 else 1 + offsets[non_empty[0] - 1])
        assert 1 <= hist["Max"]["SSTables"] <= flushes + 1


async def test_nodetool_tablestats_syntax(manager: ScyllaClusterManager):
    servers = await manager.running_servers()
    await manager.get_ready_cql(servers)
    node = servers[0]
    cql = manager.get_cql()
    async with new_test_keyspace(manager, RF3) as ks1, new_test_keyspace(manager, RF3) as ks2:
        tables = {}
        for ks in (ks1, ks2):
            tables[ks] = [f"{ks}_cf1", f"{ks}_cf2"]
            for cf in tables[ks]:
                await cql.run_async(f"CREATE TABLE {ks}.{cf} (id int PRIMARY KEY, val int)")

        async def check(args, expected, exact=True):
            res = parse_tablestats(await nodetool(manager, node, "tablestats", *args))
            got = {ks: sorted(v["tables"].keys()) for ks, v in res.items()}
            if exact:
                assert got == expected, f"tablestats {args}"
            else:
                for ks, cfs in expected.items():
                    assert got.get(ks) == cfs, f"tablestats {args}: {got}"
            return res

        # Every table of every keyspace, system ones included, has all the stats.
        everything = await check([], tables, exact=False)
        assert "system" in everything
        for ks, ks_stats in everything.items():
            for key in KEYSPACE_STATS:
                assert key in ks_stats, f"{key} missing for keyspace {ks}"
            for cf, table_stats in ks_stats["tables"].items():
                for key in TABLE_STATS:
                    assert key in table_stats, f"{key} missing for {ks}.{cf}"
        await check([ks1], {ks1: tables[ks1]})
        await check([ks2, ks1], tables)
        await check([f"{ks2}.{ks2}_cf2"], {ks2: [f"{ks2}_cf2"]})
        await check([f"{ks1}/{ks1}_cf2"], {ks1: [f"{ks1}_cf2"]})
        await check([f"{ks1}/"], {ks1: tables[ks1]})
        await check([f"{ks1}.{ks1}_cf1", f"{ks2}/{ks2}_cf2"], {ks1: [f"{ks1}_cf1"], ks2: [f"{ks2}_cf2"]})
        # cfstats is an alias.
        res = parse_tablestats(await nodetool(manager, node, "cfstats", ks1))
        assert list(res.keys()) == [ks1]
        # --ignore excludes the listed keyspaces / tables.
        ignored = parse_tablestats(await nodetool(manager, node, "tablestats", "-i", ks1, f"{ks2}.{ks2}_cf1"))
        assert ks1 not in ignored
        assert sorted(ignored[ks2]["tables"].keys()) == [f"{ks2}_cf2"]
        assert "system" in ignored


def parse_describecluster(out: str) -> dict:
    res = {"Schema versions": {}}
    in_versions = False
    for line in out.splitlines():
        if not line.strip() or line.strip() == "Cluster Information:":
            continue
        key, _, value = line.strip().partition(": ")
        if in_versions:
            hosts = value.strip().removeprefix("[").removesuffix("]")
            res["Schema versions"][key] = [h.strip() for h in hosts.split(",")]
        elif key == "Schema versions:":
            in_versions = True
        else:
            res[key] = value
    return res


async def test_nodetool_describecluster(manager: ScyllaClusterManager):
    servers = await manager.running_servers()
    cql, _ = await manager.get_ready_cql(servers)
    local = (await cql.run_async("SELECT cluster_name FROM system.local"))[0]
    ips = sorted(s.ip_addr for s in servers)

    # Agreement is asynchronous after a schema change, so wait for it.
    async with new_test_keyspace(manager, RF3):
        for server in servers:
            async def agreed():
                res = parse_describecluster(await nodetool(manager, server, "describecluster"))
                versions = res["Schema versions"]
                if len(versions) == 1 and sorted(next(iter(versions.values()))) == ips:
                    return res
                logger.info(f"Schema versions on {server.ip_addr} not in agreement yet: {versions}")
                return None
            res = await wait_for(agreed, time.time() + 60)
            assert res["Name"] == local.cluster_name
            assert res["Partitioner"] == "org.apache.cassandra.dht.Murmur3Partitioner"
            assert res["Snitch"].startswith("org.apache.cassandra.locator."), res
            assert res["DynamicEndPointSnitch"] == "disabled"
            assert "UNREACHABLE" not in res["Schema versions"]

    out = await nodetool(manager, servers[0], "version")
    assert re.match(r"^ReleaseVersion: \d+\.\d+", out), out
