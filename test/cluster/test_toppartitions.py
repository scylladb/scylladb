#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
"scylla nodetool toppartitions" sampling the most read and written
partitions of a node while a load runs: argument validation, the top-k and
capacity options, the reads/writes sampler filter, keyspace and table
filters, and cassandra-stress loads with gaussian key distributions.

Ported from scylla-dtest toppartitions_test.py. Like the dtests, each test
has a node of its own, keyspaces use vnodes, and the gaussian loads use
the dtests' cassandra-stress profiles (in test/resource/cassandra-stress).
"""

import asyncio
import logging
import math
import os
import re

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.cluster.util import run_nodetool, stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

# Like the dtests' nodes, new keyspaces use vnodes by default, 256 per node.
CONFIG = {'tablets_mode_for_new_keyspaces': 'disabled', 'num_tokens': 256}
PROFILES = os.path.join(os.path.dirname(__file__), "..", "resource", "cassandra-stress")


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


async def prepare_c1c2(manager: ScyllaClusterManager, keyspaces=("ks",), column_families=(("cf",),)):
    """A node with the given keyspaces of c1/c2 tables (the dtests'
    create_c1c2_table())."""
    server = await manager.server_add(config=CONFIG)
    cql = manager.get_cql()
    for ks, cfs in zip(keyspaces, column_families):
        await cql.run_async(f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}")
        for cf in cfs:
            await cql.run_async(f"CREATE COLUMNFAMILY {ks}.{cf} (key varchar PRIMARY KEY, c1 text, c2 text) WITH comment='test cf' "
                                "AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'} AND compression = {}")
            await cql.run_async(f"SELECT * FROM {ks}.{cf} LIMIT 1")
    return server, cql


async def run_operations_c1c2(cql, mode="write", keys=None, w_keys=1, r_keys=1, w_num=1, r_num=1, ks="ks", cf="cf"):
    """The dtests' run_operations_c1c2(): unprepared INSERTs of keys k<i>,
    or prepared SELECTs of them, each key w_num/r_num times, concurrently."""
    if mode == "write":
        if keys:
            keys_list = keys * w_num
            values = list(range(w_num)) * len(keys)
        else:
            keys_list = list(range(w_keys)) * w_num
            values = list(range(w_num * w_keys))
        stmts = [SimpleStatement(f"INSERT INTO {ks}.{cf} (key, c1, c2) VALUES ('k{k}', '{v}', '{v}')",
                                consistency_level=ConsistencyLevel.QUORUM) for k, v in zip(keys_list, values)]
        for i in range(0, len(stmts), 100):
            await asyncio.gather(*[cql.run_async(s) for s in stmts[i:i + 100]])
    else:
        keys_list = (keys if keys else list(range(r_keys))) * r_num
        select = cql.prepare(f"SELECT c1, c2 FROM {ks}.{cf} WHERE key=?")
        select.consistency_level = ConsistencyLevel.QUORUM
        for i in range(0, len(keys_list), 100):
            await asyncio.gather(*[cql.run_async(select, [f"k{k}"]) for k in keys_list[i:i + 100]])


def parse_toppartitions(output: str) -> dict:
    """{sampler: {"cardinality", "capacity", "toppartitions", "partitions": {partition: {"count", "margin"}}}}"""
    pattern1 = (r"(?P<sampler>[A-Z]+)\sSampler:\W+Cardinality:\s~(?P<cardinality>[0-9]+)\s\((?P<capacity>[0-9]+)\scapacity\)"
                r"\W+Top\s(?P<toppartitions>[0-9]+)\spartitions:")
    pattern2 = r"(?P<partition>\([\w:]+\)\s[\w:]+)\s+(?P<count>[\d]+)\s+(?P<margin>[\d]+)"
    result = {}
    for out in output.split("\n\n"):
        data = re.match(pattern1, out, re.MULTILINE).groupdict()
        data["partitions"] = {p: {"count": c, "margin": m} for p, c, m in re.findall(pattern2, out, re.MULTILINE)}
        result[data.pop("sampler")] = data
    return result


async def toppartitions_during(manager: ScyllaClusterManager, server, args: list[str], *loads) -> dict:
    """Run "nodetool toppartitions <args>", start the loads once the node
    logs that it started sampling, and return the parsed output."""
    log = await manager.server_open_log(server.server_id)
    mark = await log.mark()
    tp = asyncio.create_task(run_nodetool(manager, server, "toppartitions", *args))
    await log.wait_for("toppartitions query:", from_mark=mark, timeout=30)
    await asyncio.gather(*loads)
    out, err, _ = await tp
    assert not err
    return parse_toppartitions(out)


def check_counters(actual: dict, expected: list) -> None:
    """The sampler lists exactly the expected partitions, each counted at
    least 90% of the expected times (the dtests' verify_counters_for_sample)."""
    assert len(actual["partitions"]) == len(expected), f"expected {expected}, got {actual['partitions']}"
    for partition, counter in expected:
        assert partition in actual["partitions"], f"partition {partition} not found in {actual['partitions']}"
        assert int(actual["partitions"][partition]["count"]) >= 0.9 * int(counter), f"expected {expected}, got {actual['partitions']}"


async def toppartitions_fails(manager: ScyllaClusterManager, server, *args) -> str:
    out, err, rc = await run_nodetool(manager, server, "toppartitions", *args, check=False)
    assert rc != 0
    return out + err


# toppartitions needs a keyspace, table and duration, or nothing: with no
# arguments it succeeds, with only some of them it fails with that message;
# the top count must be smaller than the capacity.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_toppartitions_required_parameters(manager: ScyllaClusterManager):
    server, _ = await prepare_c1c2(manager, ["keyspace1"], [["columnfamily1"]])
    await run_nodetool(manager, server, "toppartitions")
    error = "toppartitions requires either a keyspace, column family name and duration or no arguments at all"
    for args in [["keyspace1"], ["keyspace1", "columnfamily1"], ["columnfamily1", "100"]]:
        assert error in await toppartitions_fails(manager, server, *args)


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_toppartitions_count_smaller_than_capacity(manager: ScyllaClusterManager):
    server, _ = await prepare_c1c2(manager, ["keyspace1"], [["columnfamily1"]])
    out = await toppartitions_fails(manager, server, "-k", "15", "-s", "12", "keyspace1", "columnfamily1", "3000")
    assert re.search(r"TopK count \(-k\) option must be smaller th[ae]n the summary capacity \(-s\)", out), out


# Sampling a new, empty table finds no reads and no writes.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_toppartitions_empty_table(manager: ScyllaClusterManager):
    server, _ = await prepare_c1c2(manager, ["keyspace1"], [["columnfamily1"]])
    out, err, _ = await run_nodetool(manager, server, "toppartitions", "keyspace1", "columnfamily1", "500")
    assert not err
    result = parse_toppartitions(out)
    assert not result["WRITES"]["partitions"] and not result["READS"]["partitions"]


# Ported from scylla-dtest toppartitions_test.py::TestTopPartitions::test_writes_sample_for_10_partitions_with_100_op_and_empty_reads_sample.
# 1000 writes into each of 10 partitions while sampling: the write sampler
# counts each of them, the read sampler is empty.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_toppartitions_writes_10_partitions(manager: ScyllaClusterManager):
    server, cql = await prepare_c1c2(manager, ["ks"], [["cf"]])
    result = await toppartitions_during(manager, server, ["ks", "cf", "10000"],
                                        run_operations_c1c2(cql, "write", w_keys=10, w_num=1000))
    assert set(result) == {"WRITES", "READS"}
    check_counters(result["WRITES"], [(f"(ks:cf) k{i}", "100") for i in range(10)])
    check_counters(result["READS"], [])


# Ported from scylla-dtest toppartitions_test.py::TestTopPartitions::test_reads_sample_for_10_partitions_with_100_op_and_empty_writes_sample.
# 10 partitions are written, then each is read 1000 times while sampling:
# the read sampler counts each of them, the write sampler is empty.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_toppartitions_reads_10_partitions(manager: ScyllaClusterManager):
    server, cql = await prepare_c1c2(manager, ["ks"], [["cf"]])
    await run_operations_c1c2(cql, "write", w_keys=10)
    result = await toppartitions_during(manager, server, ["ks", "cf", "10000"],
                                        run_operations_c1c2(cql, "read", r_keys=10, r_num=1000))
    assert set(result) == {"WRITES", "READS"}
    check_counters(result["READS"], [(f"(ks:cf) k{i}", "100") for i in range(10)])
    check_counters(result["WRITES"], [])


# Ported from scylla-dtest toppartitions_test.py::TestTopPartitions::test_top_5_partitions_write_samplers_and_empty_read_sample.
# With "-k 5", only the five most written of 10 partitions are listed (#4529).
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_toppartitions_top_5_writes(manager: ScyllaClusterManager):
    server, cql = await prepare_c1c2(manager, ["ks"], [["cf"]])
    result = await toppartitions_during(manager, server, ["-k", "5", "ks", "cf", "10000"],
                                        run_operations_c1c2(cql, "write", keys=list(range(5)), w_num=1000),
                                        run_operations_c1c2(cql, "write", keys=list(range(5, 10)), w_num=1500))
    assert set(result) == {"WRITES", "READS"}
    check_counters(result["WRITES"], [(f"(ks:cf) k{i}", "100") for i in range(5, 10)])
    check_counters(result["READS"], [])


# Ported from scylla-dtest toppartitions_test.py::TestTopPartitions::test_top_5_partitions_for_read_samplers_and_empty_write_sample.
# With "-k 5", only the five most read of 10 partitions are listed (#4529).
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_toppartitions_top_5_reads(manager: ScyllaClusterManager):
    server, cql = await prepare_c1c2(manager, ["ks"], [["cf"]])
    await run_operations_c1c2(cql, "write", w_keys=10)
    result = await toppartitions_during(manager, server, ["-k", "5", "ks", "cf", "10000"],
                                        run_operations_c1c2(cql, "read", keys=list(range(5)), r_num=1500),
                                        run_operations_c1c2(cql, "read", keys=list(range(10)), r_num=1000))
    assert set(result) == {"WRITES", "READS"}
    check_counters(result["READS"], [(f"(ks:cf) k{i}", "100") for i in range(5)])
    check_counters(result["WRITES"], [])


# Ported from scylla-dtest toppartitions_test.py::TestTopPartitions::test_param_k.
# "--samplers writes --capacity 20 --size k" with 20 written partitions
# lists exactly k of them
# (https://github.com/scylladb/scylla-enterprise/issues/2859).
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("size", [5, 10, 15])
async def test_toppartitions_param_k(manager: ScyllaClusterManager, size):
    server, cql = await prepare_c1c2(manager, ["ks"], [["cf"]])
    result = await toppartitions_during(manager, server, ["--samplers", "writes", "--capacity", "20", "--size", str(size), "ks", "cf", "10000"],
                                        run_operations_c1c2(cql, "write", keys=list(range(20)), w_num=1000))
    assert set(result) == {"WRITES"}
    assert len(result["WRITES"]["partitions"]) == size


# Ported from scylla-dtest toppartitions_test.py::TestTopPartitions::test_write_into_one_partition_to_different_rows.
# Writes into many rows of the partitions of a compound partition key are
# counted per partition, shown as "key1:key2".
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_toppartitions_one_partition_different_rows(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG)
    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE keyspace1 WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    await cql.run_async("CREATE TABLE IF NOT EXISTS keyspace1.columnfamily1 (key1 int, key2 int, ckey int, val text, PRIMARY KEY ((key1, key2), ckey));")
    insert = cql.prepare("INSERT INTO keyspace1.columnfamily1 (key1, key2, ckey, val) VALUES (?, ?, ?, ?)")
    insert.consistency_level = ConsistencyLevel.ONE

    async def write(rows):
        # Like execute_concurrent_with_args(), 100 statements at a time.
        for i in range(0, len(rows), 100):
            await asyncio.gather(*[cql.run_async(insert, row) for row in rows[i:i + 100]])

    # 25 rows into each of the partitions 1:0..1:9, and 2500 writes of 25
    # rows into the partition 1:1.
    rows_10_partitions = [[1, x, y, f"value{y}"] for x, y in zip(list(range(10)) * 250, range(2500))]
    rows_one_partition = [[1, 1, y, f"value{a}"] for y, a in zip(list(range(25)) * 100, range(2500))]
    result = await toppartitions_during(manager, server, ["keyspace1", "columnfamily1", "10000"],
                                        write(rows_10_partitions), write(rows_one_partition))
    expected = [("(keyspace1:columnfamily1) 1:1", "150"), ("(keyspace1:columnfamily1) 1:0", "15")]
    expected += [(f"(keyspace1:columnfamily1) 1:{i}", "15") for i in range(2, 10)]
    assert set(result) == {"WRITES", "READS"}
    check_counters(result["WRITES"], expected)
    check_counters(result["READS"], [])


# Ported from scylla-dtest toppartitions_test.py::TestTopPartitions::test_one_keyspace_multiple_column_families_writes_sample_and_empty_reads_sample.
# With a keyspace filter, the writes into the three tables of the keyspace
# are sampled, and no reads.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_toppartitions_keyspace_filter_writes(manager: ScyllaClusterManager):
    cfs = ["cf1", "cf2", "cf3"]
    server, cql = await prepare_c1c2(manager, ["ks"], [cfs])
    result = await toppartitions_during(manager, server, ["--ks-filters", "ks", "-d", "3000"],
                                        *[run_operations_c1c2(cql, "write", w_keys=3, w_num=1000, ks="ks", cf=cf) for cf in cfs])
    assert set(result) == {"WRITES", "READS"}
    check_counters(result["WRITES"], [(f"(ks:{cf}) k{i}", "100") for cf in cfs for i in range(3)])
    check_counters(result["READS"], [])


# Ported from scylla-dtest toppartitions_test.py::TestTopPartitions::test_all_column_families_writes_only.
# Without filters and with "-a writes", the writes into all tables of two
# keyspaces are sampled.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_toppartitions_all_tables_writes(manager: ScyllaClusterManager):
    keyspaces, families = ["ks1", "ks2"], [["cf1", "cf2", "cf3"], ["cf1", "cf2"]]
    server, cql = await prepare_c1c2(manager, keyspaces, families)
    result = await toppartitions_during(manager, server, ["-a", "writes", "-d", "10000"],
                                        *[run_operations_c1c2(cql, "write", w_keys=1, w_num=5000, ks=ks, cf=cf)
                                          for ks, cfs in zip(keyspaces, families) for cf in cfs])
    assert set(result) == {"WRITES"}
    check_counters(result["WRITES"], [(f"({ks}:{cf}) k0", 100) for ks, cfs in zip(keyspaces, families) for cf in cfs])


# With "-k 3", only the three most written (or read) partitions are listed,
# and with "-a writes" ("-a reads") only that sampler.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_toppartitions_top_3_writes_only(manager: ScyllaClusterManager):
    server, cql = await prepare_c1c2(manager)
    result = await toppartitions_during(manager, server, ["-k", "3", "-a", "writes", "ks", "cf", "10000"],
                                        run_operations_c1c2(cql, "write", keys=list(range(5)), w_num=1000),
                                        run_operations_c1c2(cql, "write", keys=list(range(2, 5)), w_num=1000),
                                        run_operations_c1c2(cql, "write", keys=list(range(4, 10)), w_num=1000))
    assert set(result) == {"WRITES"}
    check_counters(result["WRITES"], [("(ks:cf) k4", "100"), ("(ks:cf) k3", "100"), ("(ks:cf) k2", "100")])


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_toppartitions_top_3_reads_only(manager: ScyllaClusterManager):
    server, cql = await prepare_c1c2(manager)
    await run_operations_c1c2(cql, "write", w_keys=20)
    result = await toppartitions_during(manager, server, ["-k", "3", "-a", "reads", "ks", "cf", "10000"],
                                        run_operations_c1c2(cql, "read", keys=list(range(9)), r_num=1000),
                                        run_operations_c1c2(cql, "read", keys=list(range(7, 15)), r_num=1000),
                                        run_operations_c1c2(cql, "read", keys=list(range(7, 10)), r_num=1000))
    assert set(result) == {"READS"}
    check_counters(result["READS"], [("(ks:cf) k7", "100"), ("(ks:cf) k8", "100"), ("(ks:cf) k9", "100")])


# The same with a capacity of 15 ("-s 15").
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_toppartitions_writes_with_capacity(manager: ScyllaClusterManager):
    server, cql = await prepare_c1c2(manager)
    result = await toppartitions_during(manager, server, ["-a", "writes", "-s", "15", "-k", "3", "ks", "cf", "10000"],
                                        run_operations_c1c2(cql, "write", keys=list(range(0, 20, 2)), w_num=1000),
                                        run_operations_c1c2(cql, "write", keys=list(range(5, 20, 5)), w_num=2000),
                                        run_operations_c1c2(cql, "write", keys=list(range(1, 20, 2)), w_num=1000))
    assert set(result) == {"WRITES"}
    check_counters(result["WRITES"], [("(ks:cf) k5", "100"), ("(ks:cf) k10", "100"), ("(ks:cf) k15", "100")])


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_toppartitions_reads_with_capacity(manager: ScyllaClusterManager):
    server, cql = await prepare_c1c2(manager)
    await run_operations_c1c2(cql, "write", w_keys=20)
    result = await toppartitions_during(manager, server, ["-a", "reads", "-s", "15", "-k", "3", "ks", "cf", "10000"],
                                        run_operations_c1c2(cql, "read", keys=list(range(0, 20, 2)), r_num=1000),
                                        run_operations_c1c2(cql, "read", keys=list(range(6, 20, 6)), r_num=2000),
                                        run_operations_c1c2(cql, "read", keys=list(range(1, 20, 2)), r_num=1000))
    assert set(result) == {"READS"}
    check_counters(result["READS"], [("(ks:cf) k6", "100"), ("(ks:cf) k12", "100"), ("(ks:cf) k18", "100")])


# With a keyspace filter, the reads of the three tables of the keyspace are
# sampled, and no writes.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_toppartitions_keyspace_filter_reads(manager: ScyllaClusterManager):
    cfs = ["cf1", "cf2", "cf3"]
    server, cql = await prepare_c1c2(manager, ["ks"], [cfs])
    for cf in cfs:
        await run_operations_c1c2(cql, "write", w_keys=10, cf=cf)
    result = await toppartitions_during(manager, server, ["--ks-filters", "ks", "-d", "10000"],
                                        *[run_operations_c1c2(cql, "read", r_keys=3, r_num=1000, ks="ks", cf=cf) for cf in cfs])
    assert set(result) == {"WRITES", "READS"}
    check_counters(result["READS"], [(f"(ks:{cf}) k{i}", "100") for cf in cfs for i in range(3)])
    check_counters(result["WRITES"], [])


# Without filters, the reads of all tables of two keyspaces are sampled.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_toppartitions_all_tables_reads(manager: ScyllaClusterManager):
    keyspaces, families = ["ks1", "ks2"], [["cf1", "cf2", "cf3"], ["cf1", "cf2"]]
    server, cql = await prepare_c1c2(manager, keyspaces, families)
    for ks, cfs in zip(keyspaces, families):
        for cf in cfs:
            await run_operations_c1c2(cql, "write", w_keys=1, ks=ks, cf=cf)
    result = await toppartitions_during(manager, server, ["-k", "5", "-d", "10000"],
                                        *[run_operations_c1c2(cql, "read", r_keys=1, r_num=1000, ks=ks, cf=cf)
                                          for ks, cfs in zip(keyspaces, families) for cf in cfs])
    assert set(result) == {"WRITES", "READS"}
    check_counters(result["READS"], [(f"({ks}:{cf}) k0", 100) for ks, cfs in zip(keyspaces, families) for cf in cfs])
    check_counters(result["WRITES"], [])


async def start_cassandra_stress(server, profile: str, ops: str, duration: str, threads: int = 1):
    return await asyncio.create_subprocess_exec(
        "cassandra-stress", "user", f"profile={os.path.join(PROFILES, profile)}", f"duration={duration}", f"ops({ops})", "no-warmup",
        "-rate", f"threads={threads}", "-node", server.ip_addr, "-mode", "native", "cql3", "user=cassandra", "password=cassandra",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)


async def sample_top_keys_while_stress_runs(manager: ScyllaClusterManager, server, stress, sampler: str, num_samples: int = 3) -> list:
    """The dtests' sample_top_keys_while_stress_runs(): once cassandra-stress
    created its table, take 2-second "-k 5" samples until there are
    num_samples non-empty ones after the first (partial) one."""
    cql = manager.get_cql()
    async def table_created():
        assert stress.returncode is None, "cassandra-stress finished before creating keyspace1.standard1"
        return list(await cql.run_async("SELECT * FROM system_schema.tables WHERE keyspace_name='keyspace1' AND table_name='standard1'"))
    for _ in range(1200):
        if await table_created():
            break
        await asyncio.sleep(0.1)
    results, warmed_up = [], False
    while len(results) < num_samples:
        assert stress.returncode is None, f"cassandra-stress finished before {num_samples} samples: {results}"
        out, _, _ = await run_nodetool(manager, server, "toppartitions", "-k", "5", "keyspace1", "standard1", "2000")
        keys = list(parse_toppartitions(out)[sampler]["partitions"].keys())
        if keys or warmed_up:
            assert keys, f"empty {sampler} sample, previous samples: {results}"
            if warmed_up:
                results.append(keys)
            warmed_up = True
    return results


async def stop(stress) -> None:
    if stress.returncode is None:
        stress.terminate()
    await stress.communicate()


def gaussian_top_keys(min_pk, max_pk, stdvrng, confidence) -> range:
    mean_pk = (min_pk + max_pk) // 2
    interval_pk = int(math.ceil((mean_pk - min_pk) / stdvrng) * confidence)
    return range(mean_pk - interval_pk, mean_pk + interval_pk + 1)


# Under a cassandra-stress write (read) load with a gaussian(1..3000,1500)
# key distribution, the top written (read) partitions sampled are near the
# middle of the range.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_toppartitions_gaussian_writes(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG)
    stress = await start_cassandra_stress(server, "cs_normal_distribution.yaml", "insert=1", "60s")
    try:
        samples = await sample_top_keys_while_stress_runs(manager, server, stress, "WRITES")
    finally:
        await stop(stress)
    expected = [f"(keyspace1:standard1) {k}" for k in gaussian_top_keys(1, 3000, 1500, 6)]
    for keys in samples:
        for key in keys:
            assert key in expected, f"key {key} not found"


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_toppartitions_gaussian_reads(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG)
    writer = await start_cassandra_stress(server, "cs_normal_distribution.yaml", "insert=1", "3s")
    out, _ = await writer.communicate()
    assert writer.returncode == 0, out.decode()
    stress = await start_cassandra_stress(server, "cs_normal_distribution.yaml", "single=1", "60s")
    try:
        samples = await sample_top_keys_while_stress_runs(manager, server, stress, "READS")
    finally:
        await stop(stress)
    expected = [f"(keyspace1:standard1) {k}" for k in gaussian_top_keys(1, 3000, 1500, 6)]
    for keys in samples:
        for key in keys:
            assert key in expected, f"key {key} not found"


COMPOUND_TOP_KEYS = [f"(keyspace1:standard1) {a}:{b}" for a in [1500, 1501, 1499, 1498, 1502] for b in [1501, 1500, 1499, 1502, 1498]]


# The same with a compound partition key (each part gaussian) and many rows
# per partition: the top partitions are near 1500:1500.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_toppartitions_gaussian_writes_compound_key(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG)
    stress = await start_cassandra_stress(server, "cs_large_numbers_rows_per_partition.yaml", "insert=1", "60s")
    try:
        samples = await sample_top_keys_while_stress_runs(manager, server, stress, "WRITES")
    finally:
        await stop(stress)
    for keys in samples:
        for key in keys:
            assert key in COMPOUND_TOP_KEYS, f"key {key} not found"


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_toppartitions_gaussian_reads_compound_key(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG, cmdline=["--abort-on-lsa-bad-alloc", "0"])
    manager.ignore_log_patterns += ["std::bad_alloc"]
    writer = await start_cassandra_stress(server, "cs_large_numbers_rows_per_partition.yaml", "insert=1", "15s")
    out, _ = await writer.communicate()
    assert writer.returncode == 0, out.decode()
    stress = await start_cassandra_stress(server, "cs_large_numbers_rows_per_partition.yaml", "multi_row=2", "60s", threads=2)
    try:
        samples = await sample_top_keys_while_stress_runs(manager, server, stress, "READS")
    finally:
        await stop(stress)
    for keys in samples:
        for key in keys:
            assert key in COMPOUND_TOP_KEYS, f"key {key} not found"
