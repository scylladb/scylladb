"""Tests for limiting streaming/repair/compaction and other throughput limits"""

import logging
import signal
from time import time

import pytest
from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.scylla_node import ScyllaNode

from dtest_class import Tester
from dtest_setup_overrides import DTestSetupOverrides
from tools.cluster import new_node
from tools.marks import issue_open, with_feature
from tools.metrics import get_node_metrics
from tools.misc import ImmutableMapping
from tools.rate_limit import rate_limit_expected_errors

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.next_gating


@pytest.fixture(scope="function", autouse=True)
def fixture_dtest_setup_overrides(dtest_config):
    dtest_setup_overrides = DTestSetupOverrides()
    dtest_setup_overrides.cluster_options = ImmutableMapping(
        {
            "logger_log_level": {"compaction": "debug"}  # so we see compaction start/end log messages
        }
    )
    return dtest_setup_overrides


class TestPerPartitionRateLimiter(Tester):
    """Tests for per-partition rate limiter that limits read/write ops/s for given partition.

    Feature introduced in Scylla 5.1:
    https://github.com/scylladb/scylla/commit/dab56b82fae5e36f7aa2ca6700d8cfa5baa2b515"""

    @pytest.mark.dtest_full
    def test_per_partition_rate_limit(self):
        # Create 2 node cluster to verify that it works also with non-shard aware driver
        # when half of requests go to coordinator node instead of replica node.
        self.cluster.populate(2).start()
        node_1, node_2 = self.cluster.nodelist()

        # Create kesypace/table with feature enabled
        KEYSPACE = "test_ks"
        max_reads_per_second = 10
        max_writes_per_second = 10

        session = self.patient_cql_connection(node_1)
        session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS %s
            WITH replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': '1' }
            """
            % KEYSPACE
        )
        session.execute(
            f"""CREATE TABLE IF NOT EXISTS {KEYSPACE}.standard1 (a int PRIMARY KEY, b int)
             WITH per_partition_rate_limit = {{'max_reads_per_second': {max_reads_per_second},
             'max_writes_per_second': {max_writes_per_second}}}"""
        )

        # Run queries for given duration as fast as possible
        duration = 20
        end_time = time() + duration
        queries_count = 0
        queries_passed = 0
        while time() < end_time:
            try:
                queries_count += 1
                session.execute(f"insert into {KEYSPACE}.standard1 (a, b) values (1, 1)")
                queries_passed += 1
            except rate_limit_expected_errors:
                # For drivers that don't recognize rate limit error, ConfigurationException is raised
                pass
        metrics_node_1 = get_node_metrics(
            node_ip=node_1.address(),
            metrics=[
                "total_writes_rate_limited",
                "scylla_storage_proxy_coordinator_write_rate_limited",
            ],
        )
        metrics_node_2 = get_node_metrics(
            node_ip=node_2.address(),
            metrics=[
                "total_writes_rate_limited",
                "scylla_storage_proxy_coordinator_write_rate_limited",
            ],
        )
        logger.debug(metrics_node_1)
        logger.debug(metrics_node_2)
        # there should be rejections by replica present when non shard-aware driver queries node without given token
        # one metric is 0 (non replica), but we don't know which one
        rejected_by_replica = metrics_node_1["total_writes_rate_limited"] or metrics_node_2["total_writes_rate_limited"]
        assert rejected_by_replica, "Missing value for total_writes_rate_limited metric"

        # validate rejected queries metric
        rejected_queries_metric = metrics_node_1["scylla_storage_proxy_coordinator_write_rate_limited"] + metrics_node_2["scylla_storage_proxy_coordinator_write_rate_limited"]
        rejected_queries_actual = queries_count - queries_passed
        assert rejected_queries_actual == rejected_queries_metric, f"writes limited metric shows wrong number: {rejected_queries_metric} != {rejected_queries_actual}"

        # validate the rate (due limited precision and use of non-shard aware driver verify in (0.9x, 2x) range
        assert 0.9 * max_writes_per_second < queries_passed / duration < 2 * max_writes_per_second, "Actual rate is different specified write rate limit"

        # verification for read rate limit
        queries_count = 0
        queries_passed = 0
        end_time = time() + duration
        while time() < end_time:
            try:
                queries_count += 1
                session.execute("select * from test_ks.standard1 where a = 1")
                queries_passed += 1
            except rate_limit_expected_errors:
                pass
        metrics_node_1 = get_node_metrics(
            node_ip=node_1.address(),
            metrics=[
                "total_reads_rate_limited",
                "scylla_storage_proxy_coordinator_read_rate_limited",
            ],
        )
        metrics_node_2 = get_node_metrics(
            node_ip=node_2.address(),
            metrics=[
                "total_reads_rate_limited",
                "scylla_storage_proxy_coordinator_read_rate_limited",
            ],
        )
        logger.debug(metrics_node_1)
        logger.debug(metrics_node_2)
        # there should be rejections by replica present when non shard-aware driver queries node without given token
        # one metric is 0 (non replica), but we don't know which one
        rejected_by_replica = metrics_node_1["total_reads_rate_limited"] or metrics_node_2["total_reads_rate_limited"]
        assert rejected_by_replica, "Missing value for total_read_rate_limited metric"

        # validate the rate
        assert 0.9 * max_reads_per_second < queries_passed / duration < 2 * max_reads_per_second, "Actual rate is different specified read rate limit"

        # validate rejected queries metric
        rejected_queries_metric = metrics_node_1["scylla_storage_proxy_coordinator_read_rate_limited"] + metrics_node_2["scylla_storage_proxy_coordinator_read_rate_limited"]
        rejected_queries_actual = queries_count - queries_passed

        assert rejected_queries_actual == rejected_queries_metric, f"reads limited metric shows wrong number: {rejected_queries_metric} != {rejected_queries_actual}"
