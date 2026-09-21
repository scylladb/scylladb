import itertools
import logging
import math
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import Session
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.policies import RoundRobinPolicy, TokenAwarePolicy

from dtest_class import Tester, create_ks
from tools.cluster_topology import generate_cluster_topology
from tools.marks import unmark
from tools.rate_limit import rate_limit_expected_errors

logger = logging.getLogger(__file__)


class LBPType(Enum):
    ROUND_ROBIN = "round-robin"
    TOKEN_AWARE = "token-aware"


def make_policy(typ: LBPType):
    if typ == LBPType.ROUND_ROBIN:
        return RoundRobinPolicy()
    elif typ == LBPType.TOKEN_AWARE:
        return TokenAwarePolicy(child_policy=RoundRobinPolicy())
    assert False


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
        assert self.rejects == 0

    def validate_rate_limited(self, rate_per_second: float, rf_error: float | int):
        # Due to the distributed nature of scylla, the cluster is expected
        # to accept between `rate` and `rate * RF` requests per second.
        # Additionally, the data structure used to track the hits loses
        # a bit of precision when the throughput is large and undercounts
        # the throughput of each partition by one for each 10k req/s.
        # Finally, because rejecting is probabilistic, we allow for a 50%
        # error.
        accept_rate = self.accept_rate()

        precision_error = math.ceil((self.total() / self.duration) / 10000.0)
        lower_bound = rate_per_second * 0.5
        upper_bound = rate_per_second * rf_error * 1.5 + precision_error

        logger.info(f"Accept rate is {accept_rate}, expecting it to be in range ({lower_bound}, {upper_bound})")

        assert accept_rate > lower_bound and accept_rate < upper_bound


def is_per_partition_limit_reached_error(err):
    # If the driver supports the scylla-specific exception meaning that the rate
    # limit was reached, scylla will use it - otherwise, ConfigurationException
    # will be returned.
    #
    return isinstance(err, rate_limit_expected_errors)


@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestPerPartitionRateLimiting(Tester):
    warmup_seconds = 5.0
    measure_seconds = 15.0
    concurrency = 50

    rate = 200.0

    def prepare(self, nodes: int, rf: int | dict[str, int]):
        cluster = self.cluster

        cluster.populate(nodes).start(wait_for_binary_proto=True, wait_other_notice=True)
        session = self.patient_cql_connection(cluster.nodelist()[0])
        create_ks(session, "ks", rf)

        return session

    def create_keyspace(self, session: Session, rf: int | dict[str, int]):
        session.execute("DROP KEYSPACE IF EXISTS ks")
        create_ks(session, "ks", rf)

    def create_table(self, session: Session, table_name: str = "cf"):
        # Speculative retries slightly interfere with rate limiting for reads,
        # so they are explicitly disabled here
        session.execute(
            f"CREATE TABLE {table_name} (pk int, ck int, v int, PRIMARY KEY (pk, ck)) \
                WITH speculative_retry = 'NONE'"
        )

    def set_limits(self, session: Session, table_name: str = "cf", read_limit: int | None = None, write_limit: int | None = None):
        limits = []
        if read_limit:
            limits.append(f"'max_reads_per_second': {read_limit}")
        if write_limit:
            limits.append(f"'max_writes_per_second': {write_limit}")
        extension = f"per_partition_rate_limit = {{{', '.join(limits)}}}"

        session.execute(f"ALTER TABLE {table_name} WITH {extension}")

    def single_partition_workload(self):
        for ck in itertools.count():
            yield (0, ck, ck)

    def sequential_no_repeat_workload(self):
        for pk in itertools.count():
            yield (pk, 0, pk)

    def execute_concurrently(self, session: Session, statement, workload) -> ConcurrentExecutionStats:
        start_time = time.time()
        # The rate limiting implementation does not kick in immediately, it needs
        # a few seconds to start rejecting properly when the throughput stabilizes
        measure_start_time = start_time + self.warmup_seconds
        end_time = measure_start_time + self.measure_seconds

        def workload_wrapper():
            recip_rate = 1.0 / self.rate
            for i, x in enumerate(workload):
                exec_at = start_time + i * recip_rate
                time.sleep(max(0, exec_at - time.time()))
                yield x
                if time.time() >= end_time:
                    return

        results = execute_concurrent_with_args(session=session, statement=statement, parameters=workload_wrapper(), concurrency=self.concurrency, raise_on_first_error=False, results_generator=True)

        stats = ConcurrentExecutionStats(successes=0, rejects=0, duration=end_time - measure_start_time)
        failure = None
        for success, result in results:
            try:
                if time.time() < measure_start_time:
                    continue

                if success:
                    stats.successes += 1
                elif is_per_partition_limit_reached_error(result):
                    stats.rejects += 1
                else:
                    raise result
            except Exception as e:  # noqa: BLE001
                # Handle the exception only after consuming the whole `results`
                # so that we do not leave the executor threads blocked.
                failure = e

        if failure is not None:
            raise failure
        return stats

    def execute_writes(self, session, workload, table_name="cf", cl=ConsistencyLevel.LOCAL_QUORUM) -> ConcurrentExecutionStats:
        stmt = session.prepare(f"INSERT INTO {table_name} (pk, ck, v) VALUES (?, ?, ?)")
        stmt.consistency_level = cl
        return self.execute_concurrently(session, stmt, workload=workload)

    def execute_reads(self, session, workload, table_name="cf", cl=ConsistencyLevel.LOCAL_QUORUM) -> ConcurrentExecutionStats:
        stmt = session.prepare(f"SELECT v FROM {table_name} WHERE pk = ? AND ck = ?")
        stmt.consistency_level = cl
        return self.execute_concurrently(session, stmt, workload=((pk, ck) for pk, ck, _ in workload))

    def validate_limited_writes(self, session, limit, cl, rf):
        logger.info(f"Validating writes with limit={limit} and consistency={cl}")
        stats = self.execute_writes(session, self.single_partition_workload(), cl=cl)
        logger.info(f"  Stats: {stats}")
        stats.validate_rate_limited(limit, rf_error=rf)

    def validate_limited_reads(self, session, limit, cl, rf):
        logger.info(f"Validating reads with limit={limit} and consistency={cl}")
        stats = self.execute_reads(session, self.single_partition_workload(), cl=cl)
        logger.info(f"  Stats: {stats}")
        stats.validate_rate_limited(limit, rf_error=rf)

    def validate_limited_both(self, session, limit, cl, rf):
        self.validate_limited_writes(session, limit, cl, rf)
        self.validate_limited_reads(session, limit, cl, rf)

    def test_writes(self):
        limit = 10

        session = self.prepare(nodes=1, rf=1)
        self.create_table(session)

        # Set a read limit to show that it doesn't affect writes
        self.set_limits(session, read_limit=limit)

        # Write to one partition with high intensity for 10s without limiting
        # Everything should succeed
        stats = self.execute_writes(session, self.single_partition_workload())
        logger.info(stats)
        stats.validate_no_rejects()

        # Set per-partition limit to a low value and start writing with high intensity
        # Nearly all writes should fail
        self.set_limits(session, write_limit=limit)
        stats = self.execute_writes(session, self.single_partition_workload())
        logger.info(stats)
        stats.validate_rate_limited(limit, rf_error=1)

        # Keep the limit and write with high intensity, but each write modifies
        # a different partition
        stats = self.execute_writes(session, self.sequential_no_repeat_workload())
        logger.info(stats)
        stats.validate_no_rejects()

    def test_reads(self):
        limit = 10

        session = self.prepare(nodes=1, rf=1)
        self.create_table(session)

        # Set a write limit to show that it doesn't affect reads
        self.set_limits(session, write_limit=limit)

        # Read from one partition with high intensity for 10s without limiting
        # Everything should succeed
        stats = self.execute_reads(session, self.single_partition_workload())
        logger.info(stats)
        stats.validate_no_rejects()

        # Set per-partition limit to a low value and start reading with high intensity
        # Nearly all reads should fail
        self.set_limits(session, read_limit=limit)
        stats = self.execute_reads(session, self.single_partition_workload())
        stats.validate_rate_limited(limit, rf_error=1)

        # Keep the limit and read with high intensity, but each write modifies
        # a different partition
        stats = self.execute_reads(session, self.sequential_no_repeat_workload())
        logger.info(stats)
        stats.validate_no_rejects()

    def test_read_write_independence(self):
        limit = 10

        session = self.prepare(nodes=1, rf=1)
        self.create_table(session)

        self.set_limits(session, write_limit=limit)

        with ThreadPoolExecutor() as exe:
            f_write = exe.submit(self.execute_writes, session, self.single_partition_workload())
            f_read = exe.submit(self.execute_reads, session, self.single_partition_workload())

            write_stats = f_write.result()
            read_stats = f_read.result()

            logger.info(write_stats)
            logger.info(read_stats)

            write_stats.validate_rate_limited(limit, rf_error=1)
            read_stats.validate_no_rejects()

        self.set_limits(session, read_limit=limit)

        with ThreadPoolExecutor() as exe:
            f_write = exe.submit(self.execute_writes, session, self.single_partition_workload())
            f_read = exe.submit(self.execute_reads, session, self.single_partition_workload())

            write_stats = f_write.result()
            read_stats = f_read.result()

            logger.info(write_stats)
            logger.info(read_stats)

            write_stats.validate_no_rejects()
            read_stats.validate_rate_limited(limit, rf_error=1)

    def test_table_independence(self):
        limit = 10
        table_name1 = "cf1"
        table_name2 = "cf2"

        session = self.prepare(nodes=1, rf=1)
        self.create_table(session, table_name1)
        self.create_table(session, table_name2)

        # Set limits on the first table only
        self.set_limits(session, table_name1, write_limit=limit)

        with ThreadPoolExecutor() as exe:
            f1 = exe.submit(self.execute_writes, session, self.single_partition_workload(), table_name=table_name1)
            f2 = exe.submit(self.execute_writes, session, self.single_partition_workload(), table_name=table_name2)

            stats1 = f1.result()
            stats2 = f2.result()

            logger.info(stats1)
            logger.info(stats2)

            stats1.validate_rate_limited(limit, rf_error=1)
            stats2.validate_no_rejects()

    def check_both_policies(self, fn):
        # The `shuffle_replicas` flag need to be set to True (default is False).
        # Rate limiting implementation assumes that shard-aware clients load-balance
        # requests between the replicas - and it only happens when shuffle_replicas=True,
        # otherwise the same coordinator will always be chosen and rate limiting
        # will be applied too strongly.
        logger.info("Checking token-aware policy")
        session = self.patient_cql_connection(self.cluster.nodelist()[0], load_balancing_policy=TokenAwarePolicy(child_policy=RoundRobinPolicy(), shuffle_replicas=True))
        session.execute("USE ks")
        fn(session)

        logger.info("Checking round-robin policy")
        session = self.patient_cql_connection(self.cluster.nodelist()[0], load_balancing_policy=RoundRobinPolicy())
        session.execute("USE ks")
        fn(session)

    @unmark.next_gating(reason="This test is quite heavy test and consistently passing")
    @pytest.mark.dtest_heavy
    def test_multinode_accuracy(self):
        nodes = generate_cluster_topology(rack_num=4)
        limit = 10

        logger.info(f"Preparing a {nodes}-node, single DC cluster and a keyspace with RF=4")
        session = self.prepare(nodes=nodes, rf=4)
        self.create_table(session)
        self.set_limits(session, write_limit=limit, read_limit=limit)

        def check_rf4(session):
            self.validate_limited_both(session, limit, ConsistencyLevel.ONE, rf=4)
            self.validate_limited_both(session, limit, ConsistencyLevel.TWO, rf=4)
            self.validate_limited_both(session, limit, ConsistencyLevel.THREE, rf=4)
            self.validate_limited_both(session, limit, ConsistencyLevel.ALL, rf=4)

        self.check_both_policies(check_rf4)

        # Change the replication factor to 1 so that the data is not replicated over all nodes
        logger.info("Changing the replication factor to 1")
        self.create_keyspace(session, rf=1)
        session.execute("USE ks")
        self.create_table(session)
        self.set_limits(session, write_limit=limit, read_limit=limit)

        def check_rf1(session):
            self.validate_limited_both(session, limit, ConsistencyLevel.ALL, rf=1)

        self.check_both_policies(check_rf1)

    @unmark.next_gating(reason="This test is quite heavy test and consistently passing")
    @pytest.mark.dtest_heavy
    def test_multidc_accuracy(self):
        limit = 10
        nodes = {
            "dc1": [1, 1],
            "dc2": [1, 1, 1],
        }
        rf = {"dc1": 2, "dc2": 3}

        logger.info(f"Preparing a {len(nodes)}-DC cluster with nodes {nodes} and a keyspace with RF={rf}")
        self.prepare(nodes=nodes, rf=rf)
        session = self.patient_cql_connection(self.cluster.nodelist()[0], load_balancing_policy=RoundRobinPolicy())
        session.execute("USE ks")
        self.create_table(session)
        self.set_limits(session, write_limit=limit, read_limit=limit)

        def check_multidc(session):
            self.validate_limited_both(session, limit, ConsistencyLevel.ONE, rf=5)
            self.validate_limited_both(session, limit, ConsistencyLevel.TWO, rf=5)
            self.validate_limited_both(session, limit, ConsistencyLevel.THREE, rf=5)
            self.validate_limited_both(session, limit, ConsistencyLevel.ALL, rf=5)

            self.validate_limited_both(session, limit, ConsistencyLevel.LOCAL_ONE, rf=5)
            self.validate_limited_both(session, limit, ConsistencyLevel.LOCAL_QUORUM, rf=5)
            self.validate_limited_writes(session, limit, ConsistencyLevel.EACH_QUORUM, rf=5)  # EACH_QUORUM is only allowed for writes

        self.check_both_policies(check_multidc)
