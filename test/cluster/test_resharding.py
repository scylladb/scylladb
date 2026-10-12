#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# A node restarted with another shard count or another
# murmur3_partitioner_ignore_msb_bits reshards its sstables at startup.
# Ported from scylla-dtest resharding_test.py, one test per dtest. As there,
# the clusters are ccm's populate(n) (SimpleSnitch, every node in
# datacenter1/rack1) and use vnodes keyspaces: tablets do not support
# lowering the shard count (scylladb/scylladb#16739).

import asyncio
import glob
import json
import logging
import multiprocessing
import random
import re
from collections import Counter

import pytest
from cassandra import ConsistencyLevel  # type: ignore
from cassandra.cluster import EXEC_PROFILE_DEFAULT  # type: ignore
from cassandra.concurrent import execute_concurrent_with_args  # type: ignore
from cassandra.policies import WhiteListRoundRobinPolicy  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore

from test import DEBUG_MODES
from test.cluster.test_compaction import wait_for_compactions
from test.cluster.util import (cassandra_stress, dump_sstables, get_table_dir, run_nodetool,
                               stop_servers_gracefully)
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.cluster.mv.dtest_mv_util import wait_for_view as dtest_wait_for_view

logger = logging.getLogger(__name__)

TESTED_STRATEGIES = ["LeveledCompactionStrategy", "SizeTieredCompactionStrategy", "TimeWindowCompactionStrategy",
                     "IncrementalCompactionStrategy"]
MURMUR3 = 15
DEFAULT_MURMUR3_PARTITIONER = 12
MURMUR3_PARTITIONER_FOR_DECREASE = 10
MURMUR3_PARTITIONER_FOR_INCREASE = 17

# ccm's populate(n): SimpleSnitch, every node in datacenter1/rack1.
DC1 = {"dc": "datacenter1", "rack": "rack1"}
CONFIG = {"endpoint_snitch": "SimpleSnitch", "tablets_mode_for_new_keyspaces": "disabled"}

# The dtest framework's default filter of the ERROR lines check_errors()
# reports (dtest_setup.py __filter_errors()).
DTEST_IGNORED_ERRORS = [
    r"Compaction for .* deliberately stopped",
    r"update compaction history failed:.*ignored",
    r"Failed to complete splitting of table .* due to seastar::rpc::remote_verb_error",
    r"Failed to complete splitting of table .* due to seastar::named_gate_closed_exception",
    r"(Startup|start) failed:.*(seastar::sleep_aborted|raft::request_aborted)",
    r"Timer callback failed: seastar::gate_closed_exception",
    r"rpc - client .*(connection dropped|fail to connect)",
    r"rpc::unknown_verb_error",
    r"raft_rpc - Failed to send",
    r"raft_topology.*(seastar::broken_promise|rpc::closed_error)",
    r"Failed to handle STREAM_MUTATION_FRAGMENTS.*rpc::stream_closed",
]


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


def connect(manager: ScyllaClusterManager, server, keyspace=None, exclusive=False, request_timeout=None):
    """The dtests' patient_cql_connection(node): statements default to
    consistency level ONE."""
    if exclusive:
        cql = manager.con_gen([server.ip_addr], load_balancing_policy=WhiteListRoundRobinPolicy([server.ip_addr])).connect()
    else:
        cql = manager.con_gen([server.ip_addr]).connect()
    profile = cql.get_execution_profile(EXEC_PROFILE_DEFAULT)
    profile.consistency_level = ConsistencyLevel.ONE
    if request_timeout:
        profile.request_timeout = request_timeout
    if keyspace:
        cql.set_keyspace(keyspace)
    return cql


def create_ks(cql, name: str, rf: int) -> None:
    cql.execute(f"CREATE KEYSPACE {name} WITH replication={{'class':'NetworkTopologyStrategy', 'replication_factor':{rf}}}")
    cql.execute(f"USE {name}")


def cql_timeout(build_mode: str, seconds: int) -> int:
    """The dtests' cql_timeout(): longer in the slower build modes."""
    return seconds * (1 if build_mode == "release" else 3 if build_mode in DEBUG_MODES else 2)


async def stress_object(manager: ScyllaClusterManager, server, *args: str) -> dict:
    """ccm's node.stress_object(): runs cassandra-stress (which creates its
    keyspace itself, from its -schema options) and parses its "Results:"."""
    out = await cassandra_stress(manager, server, *args, create_keyspace=False)
    res = {}
    started = False
    for line in (s.strip() for s in out.splitlines()):
        if started:
            if m := re.match(r"^\s*([^:]+)\s*:\s*(\S.*)\s*$", line):
                key, val = m.group(1).strip().lower(), m.group(2).strip()
                if nm := re.match(r"^\s*([\d\.\,]+\d?)", val):
                    res[key] = float(nm.group(1).replace(",", ""))
                else:
                    res[key] = val
        elif line == "Results:":
            started = True
    return res


async def check_errors_all_nodes(manager: ScyllaClusterManager, servers) -> None:
    """The dtests' check_errors_all_nodes(): no critical errors and no ERROR
    lines, besides those the dtest framework always ignores, in any node's log."""
    ignored = re.compile("|".join(DTEST_IGNORED_ERRORS))
    critical, found = [], []
    for s in servers:
        log = await manager.server_open_log(s.server_id)
        matches = await log.grep(r"Assertion.*failed|AddressSanitizer|Aborting on shard")
        if matches:
            critical.append((s.server_id, [m[0].strip() for m in matches]))
        errors = [e for e in await log.grep_for_errors(distinct_errors=True) if not ignored.search(e)]
        if errors:
            found.append((s.server_id, errors))
    assert not critical, f"Critical errors found: {critical}\nOther errors: {found}"
    assert not found, f"Errors found: {found}"


async def wait_for_resharding(log, timeout: int = 60, reshard_found: bool = False) -> bool:
    """The dtests' _wait_for_resharding(): polls the log for "Reshard"
    messages every 5 seconds, until they stop changing; whether any was found."""
    to = 0
    sleep_time = 5
    prev_out = []
    while to <= timeout:
        out = await log.grep("Reshard")
        m = bool(out)
        if m and prev_out == [o[0] for o in out]:
            break
        prev_out = [o[0] for o in out]
        if not m:
            if not timeout:
                return reshard_found
            await asyncio.sleep(sleep_time)
            return await wait_for_resharding(log, timeout=0, reshard_found=reshard_found)
        reshard_found = True
        await asyncio.sleep(sleep_time)
        to += sleep_time
    return reshard_found


class Resharding:
    """The dtests' ReshardingBase: node_count nodes started with a shard
    count derived from the machine's CPUs, node1 restarted with resharding."""

    def __init__(self, manager: ScyllaClusterManager, build_mode: str, node_count: int, compaction_strategy: str,
                 murmur3: int):
        self.manager = manager
        self.build_mode = build_mode
        self.compaction_strategy = compaction_strategy or "LeveledCompactionStrategy"
        cpu_count = multiprocessing.cpu_count()
        assert cpu_count >= 4, "Resharding tests require a minimum of 4 cpus"
        self.smp = min(cpu_count // 2 + 1, 5)
        self.smp_for_increase = min(self.smp + 1, cpu_count, 9)
        self.smp_for_decrease = max(2, self.smp // 2)
        self.murmur3 = murmur3 or DEFAULT_MURMUR3_PARTITIONER
        self.nodes = node_count or 1
        self.rf = 1 if self.nodes < 3 else 3
        self.count_request_timeout = cql_timeout(build_mode, 400)

    @staticmethod
    def cmdline(smp: int) -> list[str]:
        return ["--smp", str(smp), "-m", f"{512 * int(smp)}M"]

    async def prepare(self, configuration_options: dict) -> None:
        config = {**CONFIG, **configuration_options, "murmur3_partitioner_ignore_msb_bits": self.murmur3}
        self.servers = await self.manager.servers_add(self.nodes, config=config, cmdline=self.cmdline(self.smp),
                                                      property_file=DC1)
        self.node = self.servers[0]
        self.log = await self.manager.server_open_log(self.node.server_id)

    async def reload_with_resharding(self, murmur3: int = DEFAULT_MURMUR3_PARTITIONER, smp: int | None = None,
                                     ks: str = "keyspace1", cf: str = "standard1") -> int:
        smp = smp or self.smp
        await self.manager.server_stop_gracefully(self.node.server_id)
        for s in self.servers[1:]:
            await self.manager.server_not_sees_other_server(s.ip_addr, self.node.ip_addr)
        data_files_num_before = await self.number_of_data_files(ks, cf)
        await self.manager.server_update_config(self.node.server_id, "murmur3_partitioner_ignore_msb_bits", murmur3,
                                                 reload=False)
        await self.manager.server_update_cmdline(self.node.server_id, self.cmdline(smp))
        await self.manager.server_start(self.node.server_id, wait_others=len(self.servers) - 1)
        return data_files_num_before

    async def number_of_data_files(self, ks: str = "keyspace1", cf: str = "standard1") -> int:
        return len(glob.glob(f"{await get_table_dir(self.manager, self.node, ks, cf)}/*-TOC.txt"))

    async def verify_number_of_data_files(self, data_files_num_before: int, reshard_to: int,
                                          actual_data_files_num: int | None = None,
                                          ks: str = "keyspace1", cf: str = "standard1") -> None:
        expected_num = data_files_num_before * reshard_to
        if not actual_data_files_num:
            actual_data_files_num = await self.number_of_data_files(ks, cf)
        assert actual_data_files_num <= expected_num, \
            f"{actual_data_files_num} not less than or equal to {expected_num}"

    async def run_stress(self, op_cnt: int, *stress_cmd: str) -> None:
        res = await stress_object(self.manager, self.node, *stress_cmd)
        assert res["total errors"] == 0
        assert res["total partitions"] >= op_cnt

    async def verify_data(self, *stress_cmd: str) -> None:
        res = await stress_object(self.manager, self.node, *stress_cmd)
        assert res["total errors"] == 0

    def verify_row_number(self, cf: str, expected_row_num: int, keyspace: str = "keyspace1") -> None:
        cql = connect(self.manager, self.node)
        try:
            q = SimpleStatement(f"SELECT count(*) FROM {keyspace}.{cf}", consistency_level=ConsistencyLevel.ONE)
            row_number = cql.execute(q, timeout=self.count_request_timeout).one()[0]
        finally:
            cql.shutdown()
        assert row_number == expected_row_num

    async def resharding_basic(self, reshard_to: int, rows: int, murmur3: int) -> None:
        await self.prepare({"range_request_timeout_in_ms": self.count_request_timeout * 1000})
        if self.build_mode in DEBUG_MODES and rows > 1000:
            rows = 1000
        op_cnt = rows
        await self.run_stress(op_cnt, "write", f"n={op_cnt}", "no-warmup", "-schema", f"replication(factor={self.rf})",
                              f"compaction(strategy={self.compaction_strategy},enabled=false)")
        self.verify_row_number("standard1", op_cnt)

        data_files_num_before = await self.reload_with_resharding(smp=reshard_to, murmur3=murmur3)
        data_files_num_during = await self.number_of_data_files()

        res = await wait_for_resharding(self.log)
        if reshard_to == self.smp and murmur3 == self.murmur3:
            assert not res, "Unexpected re-sharding recognized"
        else:
            assert res, "Failed to recognize re-sharding finish"
        await check_errors_all_nodes(self.manager, self.servers)

        # The number of data files during resharding, and after it and compaction.
        await self.verify_number_of_data_files(data_files_num_before, reshard_to, data_files_num_during)
        await self.verify_number_of_data_files(data_files_num_before, reshard_to)

        await self.verify_data("read", f"n={op_cnt}", "no-warmup", "-errors", "ignore", "-rate", "threads=200")
        self.verify_row_number("standard1", op_cnt)

        await self.verify_number_of_data_files(data_files_num_before, reshard_to)


VARIANTS = pytest.mark.parametrize(
    "node_count,compaction_strategy,murmur3",
    [(node_count, strategy, MURMUR3) for node_count in [1, 4] for strategy in TESTED_STRATEGIES])
VARIANTS_GATE = pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest runs it nightly in release mode only")


# Ported from scylla-dtest resharding_test.py::TestReshardingVariants::test_resharding_by_murmur3_increase.
# A cassandra-stress data set is all there after node1 is restarted with a
# higher murmur3_partitioner_ignore_msb_bits, which reshards its sstables.
@pytest.mark.tier3
@VARIANTS_GATE
@VARIANTS
async def test_resharding_by_murmur3_increase(manager: ScyllaClusterManager, build_mode, node_count,
                                              compaction_strategy, murmur3):
    r = Resharding(manager, build_mode, node_count, compaction_strategy, murmur3)
    await r.resharding_basic(r.smp, rows=1000, murmur3=MURMUR3_PARTITIONER_FOR_INCREASE)


# Ported from scylla-dtest resharding_test.py::TestReshardingVariants::test_resharding_by_murmur3_decrease.
# The same, with a lower murmur3_partitioner_ignore_msb_bits.
@pytest.mark.tier3
@VARIANTS_GATE
@VARIANTS
async def test_resharding_by_murmur3_decrease(manager: ScyllaClusterManager, build_mode, node_count,
                                              compaction_strategy, murmur3):
    r = Resharding(manager, build_mode, node_count, compaction_strategy, murmur3)
    await r.resharding_basic(r.smp, rows=1000, murmur3=MURMUR3_PARTITIONER_FOR_DECREASE)


# Ported from scylla-dtest resharding_test.py::TestReshardingVariants::test_resharding_by_smp_increase.
# The same, with more shards.
@pytest.mark.tier3
@VARIANTS_GATE
@VARIANTS
async def test_resharding_by_smp_increase(manager: ScyllaClusterManager, build_mode, node_count,
                                          compaction_strategy, murmur3):
    r = Resharding(manager, build_mode, node_count, compaction_strategy, murmur3)
    await r.resharding_basic(r.smp_for_increase, rows=10000, murmur3=r.murmur3)


# Ported from scylla-dtest resharding_test.py::TestReshardingVariants::test_resharding_by_smp_decrease.
# The same, with fewer shards.
@pytest.mark.tier3
@VARIANTS_GATE
@VARIANTS
async def test_resharding_by_smp_decrease(manager: ScyllaClusterManager, build_mode, node_count,
                                          compaction_strategy, murmur3):
    r = Resharding(manager, build_mode, node_count, compaction_strategy, murmur3)
    await r.resharding_basic(r.smp_for_decrease, rows=100000, murmur3=r.murmur3)


# Ported from scylla-dtest resharding_test.py::TestReshardingVariants::test_resharding_by_same_smp.
# node1 restarted with the same shard count and msb bits does not reshard.
@pytest.mark.tier3
@VARIANTS_GATE
@VARIANTS
async def test_resharding_by_same_smp(manager: ScyllaClusterManager, build_mode, node_count,
                                      compaction_strategy, murmur3):
    r = Resharding(manager, build_mode, node_count, compaction_strategy, murmur3)
    await r.resharding_basic(r.smp, rows=1000, murmur3=r.murmur3)


# Ported from scylla-dtest resharding_test.py::TestReshardingVariants::test_resharding_by_murmur3_smp.
# Both the shard count and the msb bits change.
@pytest.mark.tier3
@VARIANTS_GATE
@VARIANTS
async def test_resharding_by_murmur3_smp(manager: ScyllaClusterManager, build_mode, node_count,
                                         compaction_strategy, murmur3):
    r = Resharding(manager, build_mode, node_count, compaction_strategy, murmur3)
    await r.resharding_basic(r.smp_for_increase, rows=1000, murmur3=MURMUR3_PARTITIONER_FOR_INCREASE)


# Ported from scylla-dtest resharding_test.py::TestReshardingVariants::test_resharding_counter.
# A cassandra-stress counter data set is all there after node1 is restarted
# with more shards (and the default msb bits), which reshards its sstables.
@pytest.mark.tier3
@VARIANTS_GATE
@VARIANTS
async def test_resharding_counter(manager: ScyllaClusterManager, build_mode, node_count, compaction_strategy, murmur3):
    r = Resharding(manager, build_mode, node_count, compaction_strategy, murmur3)
    await r.prepare({"range_request_timeout_in_ms": r.count_request_timeout * 1000})
    cql = connect(manager, r.node)
    cql.execute("DROP KEYSPACE IF EXISTS keyspace1")
    cql.execute("CREATE KEYSPACE keyspace1 WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '1'}")
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
    cql.shutdown()

    op_cnt = 10000
    await r.run_stress(op_cnt, "counter_write", f"n={op_cnt}", "no-warmup", "-schema", f"replication(factor={r.rf})",
                       f"compaction(strategy={r.compaction_strategy})")
    r.verify_row_number("counter1", op_cnt)

    data_files_num_before = await r.reload_with_resharding(smp=r.smp_for_increase)
    await r.verify_number_of_data_files(data_files_num_before, r.smp_for_increase)

    assert await wait_for_resharding(r.log), "Failed to recognize re-sharding finish"

    await r.verify_number_of_data_files(data_files_num_before, r.smp_for_increase)
    await check_errors_all_nodes(manager, r.servers)

    await r.verify_data("counter_read", f"n={op_cnt}", "no-warmup", "-rate", "threads=200")
    r.verify_row_number("counter1", op_cnt)


# Ported from scylla-dtest resharding_test.py::TestReshardingVariants::test_resharding_mv.
# A table with a materialized view (both with the tested compaction
# strategy) keeps all its rows, in the table and in the view, after node1
# is restarted with more shards (and the default msb bits).
@pytest.mark.tier3
@VARIANTS_GATE
@VARIANTS
async def test_resharding_mv(manager: ScyllaClusterManager, build_mode, node_count, compaction_strategy, murmur3):
    r = Resharding(manager, build_mode, node_count, compaction_strategy, murmur3)
    await r.prepare({"range_request_timeout_in_ms": r.count_request_timeout * 1000})
    cql = connect(manager, r.node)
    create_ks(cql, "ks", r.rf)
    compaction = f"{{'class': '{r.compaction_strategy}'}}"
    op_cnt = 10000
    # The dtest's TableManager table: an "id varint" key and one int column,
    # and a view keyed by both.
    table, mv, mv_pk_name = "tm_table", "tm_table_mv_0", "clmn_int0"
    cql.execute(f"CREATE TABLE IF NOT EXISTS ks.{table} (id varint, {mv_pk_name} int, PRIMARY KEY((id))) "
                f"WITH compaction = {compaction}")
    cql.execute(f"CREATE MATERIALIZED VIEW ks.{mv} AS SELECT {mv_pk_name} FROM ks.{table} "
                f"WHERE id IS NOT NULL and {mv_pk_name} IS NOT NULL PRIMARY KEY (id, {mv_pk_name})")
    # The dtest's wait_for_view(): its query of system_distributed.view_build_status
    # (a virtual table read through a multishard reader), not of the v2 table.
    await asyncio.to_thread(dtest_wait_for_view, cql, len(r.servers), "ks", mv)
    cql.execute(f"ALTER MATERIALIZED VIEW ks.{mv} WITH compaction = {compaction}")

    # TableManager.prefill_table(): random values 1..100 (ten of them,
    # repeated), written at QUORUM, then every node flushed.
    values = [random.randint(1, 100) for _ in range(10)] * (op_cnt // 10)
    stmt = cql.prepare(f"INSERT INTO ks.{table} (id, {mv_pk_name}) VALUES (?,?)")
    stmt.consistency_level = ConsistencyLevel.QUORUM
    execute_concurrent_with_args(cql, stmt, [[k, values[k]] for k in range(op_cnt)])
    for s in r.servers:
        await run_nodetool(manager, s, "flush")
    cql.shutdown()

    r.verify_row_number(table, op_cnt, keyspace="ks")
    r.verify_row_number(mv, op_cnt, keyspace="ks")

    data_files_num_before = await r.reload_with_resharding(smp=r.smp_for_increase, ks="ks", cf=table)
    await r.verify_number_of_data_files(data_files_num_before, r.smp_for_increase, ks="ks", cf=table)

    assert await wait_for_resharding(r.log), "Failed to recognize re-sharding finish"

    await r.verify_number_of_data_files(data_files_num_before, r.smp_for_increase, ks="ks", cf=table)
    await check_errors_all_nodes(manager, r.servers)

    cql = connect(manager, r.node)
    try:
        for i in range(op_cnt):
            for t in (table, mv):
                assert list(cql.execute(f"select count(*) from ks.{t} where id={i}").one()) == [1]

        r.verify_row_number(table, op_cnt, keyspace="ks")
        r.verify_row_number(mv, op_cnt, keyspace="ks")

        # assert_two_queries_equal() of the table with itself, at ALL,
        # grouped by the view's key column.
        def grouped():
            rows = cql.execute(SimpleStatement(f"select * from ks.{table}", consistency_level=ConsistencyLevel.ALL),
                               timeout=120)
            return sorted(Counter(getattr(row, mv_pk_name) for row in rows).items())
        assert grouped() == grouped()
    finally:
        cql.shutdown()


def compactions_count(cql, ks: str, cf: str) -> int:
    rows = cql.execute(f"select count(*) from system.compaction_history where keyspace_name='{ks}' "
                       f"and columnfamily_name='{cf}' allow filtering")
    return rows.one()[0]


async def dump_sstables_of_stopped_node(manager: ScyllaClusterManager, server, ks: str, cf: str) -> list[dict]:
    """ccm's node.dump_sstables() of a node that is down: "scylla sstable
    dump-data --merge" of the Data files in the table's directory."""
    table_dir = await get_table_dir(manager, server, ks, cf)
    data_files = glob.glob(f"{table_dir}/*-Data.db")
    if not data_files:
        return []
    exe = await manager.server_get_exe(server.server_id)
    proc = await asyncio.create_subprocess_exec(exe, "sstable", "dump-data", "--merge", *data_files, cwd=table_dir,
                                                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    out, err = await proc.communicate()
    assert proc.returncode == 0, err.decode()
    return json.loads(out)["sstables"]["anonymous"]


def number_of_marked_to_delete(partitions: list[dict]) -> int:
    return sum("tombstone" in partition for partition in partitions)


# Ported from scylla-dtest resharding_test.py::TestReshardingTombstonesSingleNode::test_disable_tombstone_removal_during_reshard.
# Resharding does not purge tombstones that are purgeable (tombstone_gc
# mode "immediate") from the sstables it splits, as they may cover data in
# other sstables of the other shards: no deleted row comes back, and the tombstones go away only
# with a later major compaction.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("compaction_strategy", TESTED_STRATEGIES)
async def test_disable_tombstone_removal_during_reshard(manager: ScyllaClusterManager, compaction_strategy):
    keyspace, table, keys = "ks1", "cf1", 100
    node1 = await manager.server_add(config={**CONFIG, "enable_sstable_key_validation": True},
                                     cmdline=["--smp", "2", "--logger-log-level", "compaction=debug"],
                                     property_file=DC1)
    cql = connect(manager, node1)
    create_ks(cql, keyspace, 1)
    cql.execute(f"create table {keyspace}.{table} (key int PRIMARY KEY, val int) "
                f"with compaction = {{'class':'SizeTieredCompactionStrategy'}} and tombstone_gc = {{'mode': 'immediate'}};")

    cql.execute(f"alter table {keyspace}.{table} with compaction = {{'class':'{compaction_strategy}', 'enabled':'true'}}")
    for i in range(keys):
        cql.execute(f"insert into {keyspace}.{table} (key, val) values ({i}, 1)")
    await run_nodetool(manager, node1, "flush")

    # Delete all keys and flush, to have a table with expired rows.
    for i in range(keys):
        cql.execute(f"delete from {keyspace}.{table} where key = {i}")
    await run_nodetool(manager, node1, "flush", keyspace, table)

    # Force flushes until a compaction (removing data and tombstones) runs on
    # a single shard.
    compactions_2 = compactions_1 = compactions_count(cql, keyspace, table)
    # immediate mode purges only when deletion_time < now (1s granularity); time is the input here
    await asyncio.sleep(1)
    while compactions_1 == compactions_2:
        cql.execute(f"insert into {keyspace}.{table} (key, val) values ({keys + 1},1);")
        await run_nodetool(manager, node1, "flush", keyspace, table)
        compactions_2 = compactions_count(cql, keyspace, table)
    await wait_for_compactions(manager, node1)
    compactions_2 = compactions_count(cql, keyspace, table)
    logger.debug(f"{compactions_2 - compactions_1} compaction(s) completed")

    cql.execute(f"alter table {keyspace}.{table} with compaction = {{'class':'{compaction_strategy}', 'enabled':'false'}}")
    cql.shutdown()

    await manager.server_stop(node1.server_id, convict=False)

    # Only some deletion markers are left: compaction removed the others.
    numfound = number_of_marked_to_delete(await dump_sstables_of_stopped_node(manager, node1, keyspace, table))
    assert numfound > 0, "All tombstones were removed"

    log = await manager.server_open_log(node1.server_id)
    mark = await log.mark()
    await manager.server_update_cmdline(node1.server_id, ["--smp", "4"])
    await manager.server_start(node1.server_id)
    await log.wait_for(rf"Resharded.*{keyspace}/{table}", from_mark=mark, timeout=60)

    cql = connect(manager, node1, keyspace=keyspace)
    try:
        # Not all deletion markers were removed by resharding.
        numfound = number_of_marked_to_delete(await dump_sstables(manager, node1, keyspace, table))
        assert numfound != 0, "All tombstones were removed during resharding"

        # No data was resurrected.
        for x in range(keys):
            assert list(cql.execute(f"select * from {keyspace}.{table} where key = {x}")) == []
    finally:
        cql.shutdown()

    # A major compaction removes the remaining tombstones.
    await run_nodetool(manager, node1, "compact")
    await wait_for_compactions(manager, node1)
    numfound = number_of_marked_to_delete(await dump_sstables(manager, node1, keyspace, table))
    assert numfound == 0, "All tombstones were not removed during resharding"
