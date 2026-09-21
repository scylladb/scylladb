import logging
import os
import re
import time
from re import Pattern
from typing import Any

import pytest
from cassandra.cluster import ResultSet, Session
from ccmlib.scylla_cluster import ScyllaCluster

from dtest_class import Tester
from tools.cluster import restart_cluster
from tools.cluster_topology import generate_cluster_topology_based_rf
from tools.marks import issue_open, with_feature
from tools.misc import is_coverage, set_trace_probability
from tools.session import wait_reconnection

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
@pytest.mark.next_gating
@pytest.mark.use_cassandra_stress
class TestDistributedAggregations(Tester):
    CS_KEYSPACE = "ks"
    CS_TABLE = "counter_cf"
    CS_PROFILE_PATH = profile_path = os.path.join(os.path.dirname(__file__), "test_data/c-s-profiles/cassandra-stress-custom-counters-1.yaml")
    OPS = 100_000
    CONFIG_OPTIONS_WITH_PARALLELIZED_AGGREGATION = {"enable_parallelized_aggregation": "true", "murmur3_ignore_msb_bits": 1, "num_tokens": 3}
    CONFIG_OPTIONS_WITHOUT_PARALLELIZED_AGGREGATION = {"enable_parallelized_aggregation": "false", "murmur3_ignore_msb_bits": 1, "num_tokens": 3}
    JVM_ARGS = ["--smp", "4", "--memory", "4G"]

    def prepare(self, nodes=3, wait_for_binary_proto=True, jvm_args=None, configuration_options: dict[str, str] | None = None) -> tuple[Any, Session]:
        self.cluster: ScyllaCluster
        self.cluster.set_configuration_options(values=configuration_options)
        self.cluster.populate(generate_cluster_topology_based_rf(nodes=nodes, rf=2)).start(wait_for_binary_proto=wait_for_binary_proto, jvm_args=jvm_args)
        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        if self.cluster.scylla_mode == "debug" or is_coverage(self.cluster.get_install_dir()):
            self.OPS = 10_000
        self._populate_according_to_profile(node=node1, ops=self.OPS, profile_path=self.CS_PROFILE_PATH)
        set_trace_probability(nodes=[node1], probability_value=1.0)
        self.cluster.flush()

        return node1, session

    @pytest.mark.skip_if(with_feature("tablets") & issue_open("#16709"))
    def test_distributed_count_all(self):
        """
        Test the new feature flag for enabling parallelized aggregation
        introduced with commit:
        https://github.com/scylladb/scylladb/commit/fe65122ccd40a2a3577121aebdb9a5b50deb4a90

        The test runs a count(*) query first without, and with the feature
        flag enabled. We check for 3 things:
        1. Equality of the results.
        2. Query distribution: i.e. that the query is being forwarded
        when the feature is enabled and is not forwarded when the feature
        is disabled.
        3. That the distributed query returns quicker than the
        non-distributed one.
        """
        self.fixture_dtest_setup.allow_log_errors = True
        self.fixture_dtest_setup.ignore_log_patterns += ["TYPED_ERRORS_IN_READ_RPC"]
        aggregate_query = f"SELECT count(*) FROM {self.CS_KEYSPACE}.{self.CS_TABLE} USING TIMEOUT 180s"

        _node1, session = self.prepare(configuration_options=self.CONFIG_OPTIONS_WITHOUT_PARALLELIZED_AGGREGATION, jvm_args=self.JVM_ARGS)

        no_parallelization_result, _no_parallelization_duration = self._execute_timed_aggregate_query(session, aggregate_query)
        no_parallelization_trace = self._trace_aggregate_query(session, aggregate_query)

        restart_cluster(cluster=self.cluster, new_cluster_options=self.CONFIG_OPTIONS_WITH_PARALLELIZED_AGGREGATION, start_in_parallel="consistent-topology-changes" in self.scylla_features, jvm_args=self.JVM_ARGS)
        wait_reconnection(session)

        parallelization_result, _parallelization_duration = self._execute_timed_aggregate_query(session, aggregate_query)
        parallelization_trace = self._trace_aggregate_query(session, aggregate_query)

        mapreduce_request_log_line = re.compile(r"Dispatching (forward|mapreduce)_request to 3 endpoints|Dispatching (\d+) ranges")

        assert parallelization_result[0] == no_parallelization_result[0] == self.OPS, "Aggregation query results where different with and without parallelized query flag."
        assert not self._is_regex_in_query_traces(no_parallelization_trace, mapreduce_request_log_line), "Tracing record showed forwarding the aggregate requests with 'enable_parallelized_aggregation' set to false."
        assert self._is_regex_in_query_traces(parallelization_trace, mapreduce_request_log_line), "Tracing record did not include forwarding the aggregate requests with 'enable_parallelized_aggregation' set to true."

    @staticmethod
    def _is_regex_in_query_traces(result: ResultSet, regex_pattern: Pattern) -> bool:
        query_trace = result.get_query_trace()

        for trace_event in query_trace.events:
            if regex_pattern.search(str(trace_event)):
                return True
        return False

    @staticmethod
    def _trace_aggregate_query(session: Session, aggregate_query: str) -> ResultSet:
        query_response = session.execute(query=aggregate_query, trace=True)
        return query_response

    @staticmethod
    def _execute_timed_aggregate_query(session: Session, aggregate_query: str) -> tuple[ResultSet, float]:
        start = time.perf_counter()
        query_response = session.execute(query=aggregate_query).one()
        end = time.perf_counter()
        return query_response, end - start

    @staticmethod
    def _populate_according_to_profile(node, ops: int, profile_path: str):
        logger.debug("Run stress update counters with user profile")
        resp = node.stress(["user", f"profile={profile_path}", "ops(insert=1)", f"n={ops}", "cl=LOCAL_QUORUM", "-rate", "threads=4"])
        logger.debug("Finished running the stress object")
        return resp
