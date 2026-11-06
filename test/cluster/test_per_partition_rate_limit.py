#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import itertools
import logging
import math
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import pytest
from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.policies import RoundRobinPolicy, TokenAwarePolicy
from cassandra.protocol import RateLimitReached

from test.cluster.dtest_helpers import create_ks, dtest_session, topology
from test.cluster.util import new_test_keyspace, new_test_table
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

# The dtests' nodes: vnodes keyspaces with 256 tokens per node.
CONFIG = {"tablets_mode_for_new_keyspaces": "disabled", "num_tokens": 256}
# populate(1): SimpleSnitch, datacenter1/rack1.
POPULATE_CONFIG = {**CONFIG, "endpoint_snitch": "SimpleSnitch"}

# TestPerPartitionRateLimiting's pacing: 200 requests per second, 50 at a
# time, 5 s of warmup and 15 s measured.
WARMUP_SECONDS = 5.0
MEASURE_SECONDS = 15.0
CONCURRENCY = 50
RATE = 200.0


@dataclass
class ConcurrentExecutionStats:
    successes: int
    rejects: int
    duration: float

    def total(self):
        return self.successes + self.rejects

    def accept_rate(self):
        return self.successes / self.duration

    def validate_no_rejects(self):
        assert self.rejects == 0, self

    def validate_rate_limited(self, rate_per_second: float, rf_error: int):
        # The cluster is expected to accept between `rate` and `rate * RF`
        # requests per second; the hit tracking undercounts by one for each
        # 10k req/s, and rejecting is probabilistic, so a 50% error is allowed.
        accept_rate = self.accept_rate()
        precision_error = math.ceil((self.total() / self.duration) / 10000.0)
        lower_bound = rate_per_second * 0.5
        upper_bound = rate_per_second * rf_error * 1.5 + precision_error
        logger.info(f"Accept rate is {accept_rate}, expecting it to be in range ({lower_bound}, {upper_bound})")
        assert lower_bound < accept_rate < upper_bound


def single_partition_workload():
    for ck in itertools.count():
        yield (0, ck, ck)


def sequential_no_repeat_workload():
    for pk in itertools.count():
        yield (pk, 0, pk)


def create_table(cql, table_name: str = "cf") -> None:
    # Speculative retries slightly interfere with rate limiting for reads.
    cql.execute(f"CREATE TABLE {table_name} (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH speculative_retry = 'NONE'")


def set_limits(cql, table_name: str = "cf", read_limit: int | None = None, write_limit: int | None = None) -> None:
    limits = []
    if read_limit:
        limits.append(f"'max_reads_per_second': {read_limit}")
    if write_limit:
        limits.append(f"'max_writes_per_second': {write_limit}")
    cql.execute(f"ALTER TABLE {table_name} WITH per_partition_rate_limit = {{{', '.join(limits)}}}")


def execute_concurrently(session, statement, workload) -> ConcurrentExecutionStats:
    """The dtest's execute_concurrently(): the workload paced at RATE
    requests per second, CONCURRENCY at a time, counting the results that
    arrive after the warmup."""
    start_time = time.time()
    measure_start_time = start_time + WARMUP_SECONDS
    end_time = measure_start_time + MEASURE_SECONDS

    def workload_wrapper():
        recip_rate = 1.0 / RATE
        for i, x in enumerate(workload):
            time.sleep(max(0, start_time + i * recip_rate - time.time()))
            yield x
            if time.time() >= end_time:
                return

    results = execute_concurrent_with_args(session, statement, workload_wrapper(), concurrency=CONCURRENCY,
                                           raise_on_first_error=False, results_generator=True)
    stats = ConcurrentExecutionStats(successes=0, rejects=0, duration=end_time - measure_start_time)
    failure = None
    for success, result in results:
        if time.time() < measure_start_time:
            continue
        if success:
            stats.successes += 1
        elif isinstance(result, RateLimitReached):
            stats.rejects += 1
        else:
            # Consume all the results before raising, not to block the executor.
            failure = result
    if failure is not None:
        raise failure
    return stats


def execute_writes(session, workload, table_name="cf", cl=ConsistencyLevel.LOCAL_QUORUM) -> ConcurrentExecutionStats:
    stmt = session.prepare(f"INSERT INTO {table_name} (pk, ck, v) VALUES (?, ?, ?)")
    stmt.consistency_level = cl
    return execute_concurrently(session, stmt, workload)


def execute_reads(session, workload, table_name="cf", cl=ConsistencyLevel.LOCAL_QUORUM) -> ConcurrentExecutionStats:
    stmt = session.prepare(f"SELECT v FROM {table_name} WHERE pk = ? AND ck = ?")
    stmt.consistency_level = cl
    return execute_concurrently(session, stmt, ((pk, ck) for pk, ck, _ in workload))


def validate_limited_writes(session, limit, cl, rf):
    logger.info(f"Validating writes with limit={limit} and consistency={cl}")
    stats = execute_writes(session, single_partition_workload(), cl=cl)
    logger.info(f"  Stats: {stats}")
    stats.validate_rate_limited(limit, rf_error=rf)


def validate_limited_reads(session, limit, cl, rf):
    logger.info(f"Validating reads with limit={limit} and consistency={cl}")
    stats = execute_reads(session, single_partition_workload(), cl=cl)
    logger.info(f"  Stats: {stats}")
    stats.validate_rate_limited(limit, rf_error=rf)


def validate_limited_both(session, limit, cl, rf):
    validate_limited_writes(session, limit, cl, rf)
    validate_limited_reads(session, limit, cl, rf)


async def prepare_single_node(manager: ScyllaClusterManager):
    """TestPerPartitionRateLimiting.prepare(nodes=1, rf=1): the session (CL
    ONE by default) uses keyspace ks."""
    await manager.server_add(config=POPULATE_CONFIG, property_file={"dc": "datacenter1", "rack": "rack1"})
    cql = dtest_session(manager.get_cql())
    create_ks(cql, "ks", 1)
    cql.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    cql.execute("USE ks")
    return cql


async def check_both_policies(manager: ScyllaClusterManager, node1, fn) -> None:
    """The dtest's check_both_policies(): fn with a session through node1
    with a token-aware policy shuffling replicas (which the rate limiter
    assumes of shard-aware clients), then with a round-robin one."""
    for name, lbp in [("token-aware", TokenAwarePolicy(child_policy=RoundRobinPolicy(), shuffle_replicas=True)),
                      ("round-robin", RoundRobinPolicy())]:
        logger.info(f"Checking {name} policy")
        cluster = manager.con_gen([node1.ip_addr], load_balancing_policy=lbp)
        try:
            session = dtest_session(cluster.connect())
            session.execute("USE ks")
            await asyncio.to_thread(fn, session)
        finally:
            cluster.shutdown()


# Ported from scylla-dtest per_partition_rate_limiting_test.py::TestPerPartitionRateLimiting::test_writes.
# On one node, writes to a single partition are not rejected under a read
# limit, are accepted at about the write limit under a write limit, and
# writes to a different partition each are not rejected.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_writes(manager: ScyllaClusterManager):
    limit = 10
    cql = await prepare_single_node(manager)
    create_table(cql)
    set_limits(cql, read_limit=limit)
    stats = await asyncio.to_thread(execute_writes, cql, single_partition_workload())
    logger.info(stats)
    stats.validate_no_rejects()
    set_limits(cql, write_limit=limit)
    stats = await asyncio.to_thread(execute_writes, cql, single_partition_workload())
    logger.info(stats)
    stats.validate_rate_limited(limit, rf_error=1)
    stats = await asyncio.to_thread(execute_writes, cql, sequential_no_repeat_workload())
    logger.info(stats)
    stats.validate_no_rejects()


# Ported from scylla-dtest per_partition_rate_limiting_test.py::TestPerPartitionRateLimiting::test_reads.
# The same for reads: not rejected under a write limit, accepted at about
# the read limit under a read limit, and reads of a different partition each
# are not rejected.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_reads(manager: ScyllaClusterManager):
    limit = 10
    cql = await prepare_single_node(manager)
    create_table(cql)
    set_limits(cql, write_limit=limit)
    stats = await asyncio.to_thread(execute_reads, cql, single_partition_workload())
    logger.info(stats)
    stats.validate_no_rejects()
    set_limits(cql, read_limit=limit)
    stats = await asyncio.to_thread(execute_reads, cql, single_partition_workload())
    stats.validate_rate_limited(limit, rf_error=1)
    stats = await asyncio.to_thread(execute_reads, cql, sequential_no_repeat_workload())
    logger.info(stats)
    stats.validate_no_rejects()


def run_writes_and_reads(cql) -> tuple[ConcurrentExecutionStats, ConcurrentExecutionStats]:
    with ThreadPoolExecutor() as exe:
        f_write = exe.submit(execute_writes, cql, single_partition_workload())
        f_read = exe.submit(execute_reads, cql, single_partition_workload())
        return f_write.result(), f_read.result()


# Ported from scylla-dtest per_partition_rate_limiting_test.py::TestPerPartitionRateLimiting::test_read_write_independence.
# Concurrent writes and reads of one partition: a write limit limits only
# the writes, then a read limit only the reads.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_read_write_independence(manager: ScyllaClusterManager):
    limit = 10
    cql = await prepare_single_node(manager)
    create_table(cql)
    set_limits(cql, write_limit=limit)
    write_stats, read_stats = await asyncio.to_thread(run_writes_and_reads, cql)
    logger.info(write_stats)
    logger.info(read_stats)
    write_stats.validate_rate_limited(limit, rf_error=1)
    read_stats.validate_no_rejects()
    set_limits(cql, read_limit=limit)
    write_stats, read_stats = await asyncio.to_thread(run_writes_and_reads, cql)
    logger.info(write_stats)
    logger.info(read_stats)
    write_stats.validate_no_rejects()
    read_stats.validate_rate_limited(limit, rf_error=1)


# Ported from scylla-dtest per_partition_rate_limiting_test.py::TestPerPartitionRateLimiting::test_table_independence.
# Concurrent writes to the same partition of two tables: the write limit of
# the first table does not limit the second.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_table_independence(manager: ScyllaClusterManager):
    limit = 10
    cql = await prepare_single_node(manager)
    create_table(cql, "cf1")
    create_table(cql, "cf2")
    set_limits(cql, "cf1", write_limit=limit)

    def run():
        with ThreadPoolExecutor() as exe:
            f1 = exe.submit(execute_writes, cql, single_partition_workload(), table_name="cf1")
            f2 = exe.submit(execute_writes, cql, single_partition_workload(), table_name="cf2")
            return f1.result(), f2.result()
    stats1, stats2 = await asyncio.to_thread(run)
    logger.info(stats1)
    logger.info(stats2)
    stats1.validate_rate_limited(limit, rf_error=1)
    stats2.validate_no_rejects()


# Ported from scylla-dtest per_partition_rate_limiting_test.py::TestPerPartitionRateLimiting::test_multinode_accuracy.
# Four nodes in four racks of datacenter1, limits of 10 writes and reads
# per second: with RF=4, a single partition's writes and reads at ONE, TWO,
# THREE and ALL are accepted at between half the limit and 1.5 times the
# limit times RF, through a token-aware and a round-robin client; then the
# same at ALL with the keyspace recreated with RF=1.
# Not gating in scylla-dtest: "This test is quite heavy test and consistently passing".
@pytest.mark.tier3
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest runs it nightly in release mode only")
async def test_multinode_accuracy(manager: ScyllaClusterManager):
    limit = 10
    # Scylla's default, as on the dtest nodes; test.py turns it off.
    servers = await manager.servers_add(4, config={**CONFIG, "auto_snapshot": True},
                                        property_file=topology({"rack1": 1, "rack2": 1, "rack3": 1, "rack4": 1}))
    node1 = servers[0]
    cql = dtest_session(manager.get_cql())
    create_ks(cql, "ks", 4)
    cql.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    cql.execute("USE ks")
    create_table(cql)
    set_limits(cql, write_limit=limit, read_limit=limit)

    def check_rf4(session):
        for cl in [ConsistencyLevel.ONE, ConsistencyLevel.TWO, ConsistencyLevel.THREE, ConsistencyLevel.ALL]:
            validate_limited_both(session, limit, cl, rf=4)
    await check_both_policies(manager, node1, check_rf4)

    logger.info("Changing the replication factor to 1")
    cql.execute("DROP KEYSPACE IF EXISTS ks")
    create_ks(cql, "ks", 1)
    cql.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    cql.execute("USE ks")
    create_table(cql)
    set_limits(cql, write_limit=limit, read_limit=limit)

    def check_rf1(session):
        validate_limited_both(session, limit, ConsistencyLevel.ALL, rf=1)
    await check_both_policies(manager, node1, check_rf1)


# Ported from scylla-dtest per_partition_rate_limiting_test.py::TestPerPartitionRateLimiting::test_multidc_accuracy.
# Two DCs (two and three nodes, one rack per node) and RF {dc1: 2, dc2: 3}:
# with limits of 10 writes and reads per second, a single partition's
# writes and reads at ONE, TWO, THREE, ALL, LOCAL_ONE and LOCAL_QUORUM, and
# writes at EACH_QUORUM, are accepted at between half the limit and 1.5
# times the limit times the five replicas, through a token-aware and a
# round-robin client.
# Not gating in scylla-dtest: "This test is quite heavy test and consistently passing".
@pytest.mark.tier3
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest runs it nightly in release mode only")
async def test_multidc_accuracy(manager: ScyllaClusterManager):
    limit = 10
    servers = await manager.servers_add(5, config=CONFIG, property_file=topology({"rack1": 1, "rack2": 1}, dc="dc1")
                                        + topology({"rack1": 1, "rack2": 1, "rack3": 1}, dc="dc2"))
    node1 = servers[0]
    cql = dtest_session(manager.get_cql())
    create_ks(cql, "ks", {"dc1": 2, "dc2": 3})
    cql.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    rr = manager.con_gen([node1.ip_addr], load_balancing_policy=RoundRobinPolicy())
    try:
        session = dtest_session(rr.connect())
        session.execute("USE ks")
        create_table(session)
        set_limits(session, write_limit=limit, read_limit=limit)
    finally:
        rr.shutdown()

    def check_multidc(session):
        CL = ConsistencyLevel
        for cl in [CL.ONE, CL.TWO, CL.THREE, CL.ALL, CL.LOCAL_ONE, CL.LOCAL_QUORUM]:
            validate_limited_both(session, limit, cl, rf=5)
        # EACH_QUORUM is only allowed for writes.
        validate_limited_writes(session, limit, CL.EACH_QUORUM, rf=5)
    await check_both_policies(manager, node1, check_multidc)


async def get_metric_sum(manager: ScyllaClusterManager, servers, name: str) -> float:
    metrics = await asyncio.gather(*[manager.metrics.query(s.ip_addr) for s in servers])
    return sum(m.get(name) or 0 for m in metrics)


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.skip_mode(mode='debug', reason='measures the achieved request rate, too slow in debug mode')
async def test_per_partition_rate_limit(manager: ScyllaClusterManager):
    """
    Hammer a single partition of a table with per_partition_rate_limit, first
    with writes then with reads, and check that:
      - the accepted rate is roughly the configured limit,
      - the replica counted rate-limited operations,
      - the coordinators' rate-limited metric equals the number of rejections
        the client saw.
    There are two nodes and RF=1, and the requests alternate between them, so
    half of them go through a coordinator that is not the replica, and the
    rejection has to be propagated back to it.
    """
    servers = await manager.servers_add(2, config={"tablets_mode_for_new_keyspaces": "disabled"})
    cql, hosts = await manager.get_ready_cql(servers)
    limit = 10
    duration = 5
    # The limiter lets through far more than the limit in the first seconds
    # of a burst (its per-second counters start from zero), and converges to
    # the limit after that. So warm up before measuring.
    warmup = 3

    async def hammer(stmt, duration) -> tuple[int, int]:
        sent = 0
        rejected = 0
        end_time = time.time() + duration
        while time.time() < end_time:
            try:
                await cql.run_async(stmt, host=hosts[sent % 2])
            except RateLimitReached:
                rejected += 1
            sent += 1
        return sent, rejected

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        async with new_test_table(manager, ks, "a int PRIMARY KEY, b int",
                                  f" WITH per_partition_rate_limit = {{'max_reads_per_second': {limit}, 'max_writes_per_second': {limit}}}") as table:
            for op, stmt in [("write", f"INSERT INTO {table} (a, b) VALUES (1, 1)"),
                             ("read", f"SELECT * FROM {table} WHERE a = 1")]:
                replica_metric = f"scylla_database_total_{op}s_rate_limited"
                coordinator_metric = f"scylla_storage_proxy_coordinator_{op}_rate_limited"
                await hammer(stmt, warmup)
                replica_before = await get_metric_sum(manager, servers, replica_metric)
                coordinator_before = await get_metric_sum(manager, servers, coordinator_metric)

                sent, rejected = await hammer(stmt, duration)

                replica_rejected = await get_metric_sum(manager, servers, replica_metric) - replica_before
                coordinator_rejected = await get_metric_sum(manager, servers, coordinator_metric) - coordinator_before
                accepted_rate = (sent - rejected) / duration
                assert rejected > 0
                assert replica_rejected > 0
                assert coordinator_rejected == rejected, f"{op}s: coordinator metric {coordinator_rejected} != {rejected} rejections seen by the client"
                # The limiter is probabilistic, and over a few seconds the accepted
                # rate fluctuates noticeably around the limit, so the bounds are loose.
                assert 0.5 * limit < accepted_rate < 2 * limit, f"{op}s: accepted rate {accepted_rate}/s, limit {limit}/s"
