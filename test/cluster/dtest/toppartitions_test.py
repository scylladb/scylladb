import logging
import math
import re
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event

import pytest
from cassandra.concurrent import execute_concurrent_with_args
from ccmlib.node import NodetoolError
from ccmlib.scylla_cluster import ScyllaCluster
from packaging.version import Version

from dtest_class import Tester, create_ks
from tools.data import create_c1c2_table
from tools.toppartitions import (
    parse_toppartitions_output,
    run_operations_c1c2,
    verify_counters_for_sample,
    verify_empty_result,
    verify_error_message,
    verify_partition_keys,
    verify_samples_present_in_result,
    verify_thread_execution,
    wait_nodetool_toppartitions_start,
)

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
@pytest.mark.single_node
@pytest.mark.next_gating
class TestTopPartitions(Tester):
    """Class to test new functioanality of nodetool command toppartitions

    Return the most usable(writen/read) partitions in column family for appropriate period
    Doc Links:
    - https://docs.datastax.com/en/cassandra/3.0/cassandra/tools/toolsToppartitions.html

    Extends:
        Tester
    """

    def prepare_cluster_with_ks_cfs_c1c2(self, keyspaces=None, column_families=None):
        if column_families is None:
            column_families = [["cf"]]
        if keyspaces is None:
            keyspaces = ["ks"]
        self.cluster.populate([1]).start()
        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        for ks, cfs in zip(keyspaces, column_families):
            create_ks(session, ks, 1)
            for cf in cfs:
                create_c1c2_table(session, cf=cf)

        return node, session

    def prepare_cluster_with_ks_cf_complex_primary_key(self, ks, cf):
        self.cluster.populate([1]).start()
        node = self.cluster.nodelist()[0]

        query = f"CREATE TABLE IF NOT EXISTS {ks}.{cf} (key1 int, key2 int, ckey int, val text, PRIMARY KEY ((key1, key2), ckey));"
        session = self.patient_cql_connection(node)
        create_ks(session, ks, 1)
        session.execute(query)

        return node, session

    def get_nodetool_toppartition_cmd(self, ks="", cf="", duration="", optional_params=""):
        return f"toppartitions {optional_params} {ks} {cf} {duration}"

    def run_toppartitions_with_wrong_parameters(self, node, ks="", cf="", duration="", optional_params=""):
        self.cmd = self.get_nodetool_toppartition_cmd(ks, cf, duration, optional_params)
        with pytest.raises(NodetoolError) as exc_info:
            node.nodetool(self.cmd)
        return exc_info.value

    def run_toppartition_for(self, node, ks, cf, duration=10000, optional_params=""):
        self.cmd = self.get_nodetool_toppartition_cmd(ks, cf, duration, optional_params)

        logger.debug(f"Running nodetool {self.cmd}")
        out, err = node.nodetool(self.cmd)
        if err:
            pytest.fail(msg=str(err))
        logger.debug(f"nodetool {self.cmd} output={out}")
        return parse_toppartitions_output(out)

    def run_generic_toppartitions_for(self, node, keyspaces=None, tables=None, duration=10000, optional_params=""):
        ks_filters = f"--ks-filters {','.join(keyspaces)}" if keyspaces is not None else ""
        cf_filters = f"--cf-filters {','.join(tables)}" if tables is not None else ""
        d = f"-d {duration}" if duration != "" else ""
        optional_params = " ".join([optional_params, ks_filters, cf_filters, d])
        return self.run_toppartition_for(node, "", "", "", optional_params)

    @staticmethod
    def get_profile_full_path(profile_name: str) -> str:
        p = Path(__file__).parent / "test_data" / "c-s-profiles" / profile_name
        assert p.exists()
        return str(p.absolute())

    def test_help_description(self):
        """Check help subcommand output

        Check that nodetool help toppartitions is not empty
        """

        def get_help_toppartitions_description(node):
            stdout, stderr = node.nodetool("help toppartitions")
            assert not stderr, f"Error found {stderr}"
            return stdout, stderr

        def verify_help_output(out, err):
            assert not err, f"Next error found {err}"
            msg = "Sample and print the most active partitions for"
            assert msg in out, f"No found help message"

        node = self.cluster.populate([1]).nodelist()[0]
        stdout, stderr = get_help_toppartitions_description(node)
        verify_help_output(stdout, stderr)

    def test_any_of_required_parameters_is_missing(self):
        """test required parameters

        If not keyspace, column family, duration provided,
        command terminated

        """
        node, _session = self.prepare_cluster_with_ks_cfs_c1c2(["keyspace1"], [["columnfamily1"]])
        # no requied parameters
        if not isinstance(self.cluster, ScyllaCluster) or Version(self.cluster.version()) <= Version("4.5"):
            # allowed since scylladb/scylla-tools-java@a8a3f6cb13367ddac06e4fe6c4fccf019f314bfb
            logger.debug("Running toppartitions with no parameters - expected to fail")
            details = self.run_toppartitions_with_wrong_parameters(node)
            self.verify_error_message(details)
        else:
            logger.debug("Running toppartitions with no parameters - expected to succeed")
            cmd = self.get_nodetool_toppartition_cmd(ks="", cf="", duration="", optional_params="")
            node.nodetool(cmd)
        # only ks required parameter is passed
        details = self.run_toppartitions_with_wrong_parameters(node, ks="keyspace1")
        verify_error_message(details)
        # duration required parameter is not passed
        details = self.run_toppartitions_with_wrong_parameters(node, ks="keyspace1", cf="columnfamily1")
        verify_error_message(details)
        # ks is not passed
        details = self.run_toppartitions_with_wrong_parameters(node, cf="columnfamily1", duration=100)
        verify_error_message(details)

    def test_for_empty_ks_cf(self):
        """Validate empty results for just created
        keyspace and columnfamily

        """
        node, _session = self.prepare_cluster_with_ks_cfs_c1c2(["keyspace1"], [["columnfamily1"]])

        stdout = self.run_toppartition_for(node, ks="keyspace1", cf="columnfamily1", duration=500)
        verify_empty_result(stdout)

    def test_writes_sample_for_10_partitions_with_100_op_and_empty_reads_sample(self):
        """validate that write operations is correctly counted
        for 10 partitions

        Flow:
        1. Create KS, CF
        2. Run toppartition with duration 3 seconds
        3. Execute 1 write operation for 10 partitions
        4. Assert write sampler, empty read sampler
        """
        node, session = self.prepare_cluster_with_ks_cfs_c1c2(["ks"], [["cf"]])
        sync_starter = Event()
        with ThreadPoolExecutor(max_workers=2) as executor:
            run_tp_future = executor.submit(self.run_toppartition_for, node, ks="ks", cf="cf")
            executor.submit(run_operations_c1c2, session, mode="write", w_keys=10, w_num=1000, ready_event=sync_starter)
            wait_nodetool_toppartitions_start(node, self.cmd, timeout=30)
            sync_starter.set()

            verify_thread_execution(run_tp_future)
            toppartion_results = run_tp_future.result()

        expected_write_toppartition_key_count = [(f"(ks:cf) k{i}", "100") for i in range(10)]
        verify_samples_present_in_result(["WRITES", "READS"], toppartion_results)
        verify_counters_for_sample(actual_results=toppartion_results["WRITES"], expected_results=expected_write_toppartition_key_count)
        verify_counters_for_sample(actual_results=toppartion_results["READS"], expected_results=[])

    def test_reads_sample_for_10_partitions_with_100_op_and_empty_writes_sample(self):
        """validate that read operations is correctly counted
        for 10 partitions

        Flow:
        1. Create KS, CF
        2. Run toppartition with duration 3 seconds
        3. Execute 1 read operation for 10 partitions
        4. Assert read sampler, empty write sampler
        """
        node, session = self.prepare_cluster_with_ks_cfs_c1c2(["ks"], [["cf"]])
        run_operations_c1c2(session, mode="write", w_keys=10)
        sync_starter = Event()
        with ThreadPoolExecutor(max_workers=2) as executor:
            run_tp_future = executor.submit(self.run_toppartition_for, node, ks="ks", cf="cf")
            executor.submit(run_operations_c1c2, session, mode="read", r_keys=10, r_num=1000, ready_event=sync_starter)
            wait_nodetool_toppartitions_start(node, self.cmd, timeout=30)
            sync_starter.set()

            verify_thread_execution(run_tp_future)
            toppartion_results = run_tp_future.result()

        expected_read_toppartitions_keys_count = [(f"(ks:cf) k{i}", "100") for i in range(10)]
        verify_samples_present_in_result(["WRITES", "READS"], toppartion_results)
        verify_counters_for_sample(actual_results=toppartion_results["READS"], expected_results=expected_read_toppartitions_keys_count)
        verify_counters_for_sample(actual_results=toppartion_results["WRITES"], expected_results=[])

    def test_top_5_paritions_write_samplers_and_empty_read_sample(self):
        """validate only top 5 partions displayed

        Flow
        1. Create KS and column family
        2. run toppartitions command for 3 seconds
        3. run in thread 1 write operations for 10 partitions
        4. assert that only latest 5 are displayed.

        #4529
        """
        node, session = self.prepare_cluster_with_ks_cfs_c1c2(["ks"], [["cf"]])
        futures = []
        sync_starter = Event()
        with ThreadPoolExecutor(max_workers=3) as executor:
            run_tp_future = executor.submit(self.run_toppartition_for, node, ks="ks", cf="cf", optional_params="-k 5")
            futures.append(run_tp_future)
            futures.append(executor.submit(run_operations_c1c2, session, mode="write", keys=list(range(5)), w_num=1000, ready_event=sync_starter))
            futures.append(executor.submit(run_operations_c1c2, session, mode="write", keys=list(range(5, 10)), w_num=1500, ready_event=sync_starter))
            wait_nodetool_toppartitions_start(node, self.cmd, timeout=30)
            sync_starter.set()

            for future in futures:
                verify_thread_execution(future)

            toppartion_results = run_tp_future.result()

        expected_write_toppartition_key_count = [(f"(ks:cf) k{i}", "100") for i in range(5, 10)]

        verify_samples_present_in_result(["WRITES", "READS"], toppartion_results)
        verify_counters_for_sample(actual_results=toppartion_results["WRITES"], expected_results=expected_write_toppartition_key_count)
        verify_counters_for_sample(actual_results=toppartion_results["READS"], expected_results=[])

    def test_top_5_paritions_for_read_samplers_and_empty_write_sample(self):
        """validate only top 5 partions displayed

        Flow
        1. Create KS and column family
        2. run toppartitions command for 3 seconds
        3. run in thread 1 read operations for 10 partitions
        4. assert that only latest 5 are displayed.

        #4529
        """
        node, session = self.prepare_cluster_with_ks_cfs_c1c2(["ks"], [["cf"]])
        futures = []
        sync_starter = Event()
        run_operations_c1c2(session, mode="write", w_keys=10)
        with ThreadPoolExecutor(max_workers=3) as executor:
            run_tp_future = executor.submit(self.run_toppartition_for, node, ks="ks", cf="cf", optional_params="-k 5")
            futures.append(run_tp_future)
            futures.append(executor.submit(run_operations_c1c2, session, mode="read", keys=list(range(5)), r_num=1500, ready_event=sync_starter))
            futures.append(executor.submit(run_operations_c1c2, session, mode="read", keys=list(range(10)), r_num=1000, ready_event=sync_starter))
            wait_nodetool_toppartitions_start(node, self.cmd, timeout=30)
            sync_starter.set()

            for future in futures:
                verify_thread_execution(future)

            toppartion_results = run_tp_future.result()

        expected_read_toppartition_key_count = [(f"(ks:cf) k{i}", "100") for i in range(5)]
        verify_samples_present_in_result(["WRITES", "READS"], toppartion_results)
        verify_counters_for_sample(actual_results=toppartion_results["READS"], expected_results=expected_read_toppartition_key_count)
        verify_counters_for_sample(actual_results=toppartion_results["WRITES"], expected_results=[])

    def test_top_3_paritions_for_write_samplers_only(self):
        """validate only top 3 partions are displayed

        Flow
        1. Create KS and column family
        2. run toppartitions command for 3 seconds
        3. run in thread with write operations for 10 partitions
        4. assert that only latest 3 are displayed.

        """
        node, session = self.prepare_cluster_with_ks_cfs_c1c2(["ks"], [["cf"]])
        futures = []

        sync_starter = Event()
        with ThreadPoolExecutor(max_workers=4) as executor:
            run_tp_future = executor.submit(self.run_toppartition_for, node, ks="ks", cf="cf", optional_params="-k 3 -a writes")
            futures.append(run_tp_future)
            futures.append(executor.submit(run_operations_c1c2, session, mode="write", keys=list(range(5)), w_num=1000, ready_event=sync_starter))
            futures.append(executor.submit(run_operations_c1c2, session, mode="write", keys=list(range(2, 5)), w_num=1000, ready_event=sync_starter))
            futures.append(executor.submit(run_operations_c1c2, session, mode="write", keys=list(range(4, 10)), w_num=1000, ready_event=sync_starter))

            wait_nodetool_toppartitions_start(node, self.cmd, timeout=30)
            sync_starter.set()

            for future in futures:
                verify_thread_execution(future)

            toppartion_results = run_tp_future.result()

        expected_write_toppartition_key_count = [("(ks:cf) k4", "100"), ("(ks:cf) k3", "100"), ("(ks:cf) k2", "100")]
        verify_samples_present_in_result(["WRITES"], toppartion_results)
        verify_counters_for_sample(actual_results=toppartion_results["WRITES"], expected_results=expected_write_toppartition_key_count)

    def test_top_3_paritions_for_read_samplers_only(self):
        """validate only top 3 partions displayed

        Flow
        1. Create KS and column family
        2. run toppartitions command for 3 seconds
        3. run in thread with read operations for 10 partitions
        4. assert that only latest 3 are displayed.

        #4529
        """
        node, session = self.prepare_cluster_with_ks_cfs_c1c2(["ks"], [["cf"]])
        futures = []
        sync_starter = Event()
        run_operations_c1c2(session, mode="write", w_keys=20)
        with ThreadPoolExecutor(max_workers=4) as executor:
            run_tp_future = executor.submit(self.run_toppartition_for, node, ks="ks", cf="cf", optional_params="-k 3 -a reads")
            futures.append(run_tp_future)
            futures.append(executor.submit(run_operations_c1c2, session, mode="read", keys=list(range(9)), r_num=1000, ready_event=sync_starter))
            futures.append(executor.submit(run_operations_c1c2, session, mode="read", keys=list(range(7, 15)), r_num=1000, ready_event=sync_starter))
            futures.append(executor.submit(run_operations_c1c2, session, mode="read", keys=list(range(7, 10)), r_num=1000, ready_event=sync_starter))
            wait_nodetool_toppartitions_start(node, self.cmd, timeout=30)
            sync_starter.set()

            for future in futures:
                verify_thread_execution(future)

            toppartion_results = run_tp_future.result()

        expected_read_toppartition_key_count = [("(ks:cf) k7", "100"), ("(ks:cf) k8", "100"), ("(ks:cf) k9", "100")]
        verify_samples_present_in_result(["READS"], toppartion_results)
        verify_counters_for_sample(actual_results=toppartion_results["READS"], expected_results=expected_read_toppartition_key_count)

    def test_param_sampler_writes_and_capacity_size(self):
        """validate result include only write samples with
        capacity size equal to parameter

        Flow
        1. Create KS and column family
        2. run toppartitions command for 3 seconds
        3. run in thread 1 write operations for 10 partitions
        4. assert only write samplers in output
        """
        node, session = self.prepare_cluster_with_ks_cfs_c1c2(["ks"], [["cf"]])
        futures = []

        sync_starter = Event()

        with ThreadPoolExecutor(max_workers=4) as executor:
            run_tp_future = executor.submit(self.run_toppartition_for, node, ks="ks", cf="cf", optional_params="-a writes -s 15 -k 3")
            futures.append(run_tp_future)
            futures.append(executor.submit(run_operations_c1c2, session, mode="write", keys=list(range(0, 20, 2)), w_num=1000, ready_event=sync_starter))
            futures.append(executor.submit(run_operations_c1c2, session, mode="write", keys=list(range(5, 20, 5)), w_num=2000, ready_event=sync_starter))
            futures.append(executor.submit(run_operations_c1c2, session, mode="write", keys=list(range(1, 20, 2)), w_num=1000, ready_event=sync_starter))
            wait_nodetool_toppartitions_start(node, self.cmd, timeout=30)
            sync_starter.set()

            for future in futures:
                verify_thread_execution(future)
            toppartion_results = run_tp_future.result()

        expected_write_toppartition_key_count = [("(ks:cf) k5", "100"), ("(ks:cf) k10", "100"), ("(ks:cf) k15", "100")]
        verify_samples_present_in_result(["WRITES"], toppartion_results)
        verify_counters_for_sample(actual_results=toppartion_results["WRITES"], expected_results=expected_write_toppartition_key_count)

    def test_param_sampler_read_and_capacity_size(self):
        """validate result include only read samples with
        capacity size equal to parameter

        Flow
        1. Create KS and column family
        2. run toppartitions command for 3 seconds
        3. run in thread read operations for 10 partitions
        4. assert only read samplers in output
        """
        node, session = self.prepare_cluster_with_ks_cfs_c1c2(["ks"], [["cf"]])
        futures = []
        sync_starter = Event()
        run_operations_c1c2(session, mode="write", w_keys=20)
        with ThreadPoolExecutor(max_workers=4) as executor:
            run_tp_future = executor.submit(self.run_toppartition_for, node, ks="ks", cf="cf", optional_params="-a reads -s 15 -k 3")
            futures.append(run_tp_future)
            futures.append(executor.submit(run_operations_c1c2, session, mode="read", keys=list(range(0, 20, 2)), r_num=1000, ready_event=sync_starter))
            futures.append(executor.submit(run_operations_c1c2, session, mode="read", keys=list(range(6, 20, 6)), r_num=2000, ready_event=sync_starter))
            futures.append(executor.submit(run_operations_c1c2, session, mode="read", keys=list(range(1, 20, 2)), r_num=1000, ready_event=sync_starter))
            wait_nodetool_toppartitions_start(node, self.cmd, timeout=30)
            sync_starter.set()

            for future in futures:
                verify_thread_execution(future)
            toppartion_results = run_tp_future.result()

        # expected counters are set less, due to different computer performance where dtest are rans,
        # and validate correctness of partitions names and approximated counters
        expected_write_toppartition_key_count = [("(ks:cf) k6", "100"), ("(ks:cf) k12", "100"), ("(ks:cf) k18", "100")]

        verify_samples_present_in_result(["READS"], toppartion_results)
        verify_counters_for_sample(toppartion_results["READS"], expected_write_toppartition_key_count)

    @pytest.mark.parametrize("size", [5, 10, 15])
    def test_param_k(self, size):
        """toppartitions allows user to set the number of top partitions to
        list. the number is known as the "k" in top-k, or the "size" of the top
        partitions. it defaults to 10.

        test for https://github.com/scylladb/scylla-enterprise/issues/2859

        Flow
        1. create KS and CF
        2. run topparititions command with given "k" for 3 seconds
        3. run in thread write operations for 20 parititions
        4. assert that the returned partition matches with the specified "k"
        """
        sampler = "writes"
        capacity = 20
        assert size <= 20, "K should be smaller or equal to the summary capacity"

        ks, cf = "ks", "cf"
        params = {"samplers": sampler, "capacity": capacity, "size": size}
        options = " ".join(f"--{name} {value}" for name, value in params.items())

        node, session = self.prepare_cluster_with_ks_cfs_c1c2([ks], [[cf]])
        futures = []
        sync_starter = Event()
        with ThreadPoolExecutor(max_workers=4) as executor:
            run_tp_future = executor.submit(self.run_toppartition_for, node, ks=ks, cf=cf, optional_params=options)
            futures.append(run_tp_future)
            futures.append(executor.submit(run_operations_c1c2, session, mode="write", keys=list(range(20)), w_num=1000, ready_event=sync_starter))
            wait_nodetool_toppartitions_start(node, self.cmd, timeout=30)
            sync_starter.set()

            for future in futures:
                verify_thread_execution(future)
            toppartion_results = run_tp_future.result()

        # only "writes" ops are sampled, so we expect WRITES in the results
        sampler_name = sampler.upper()
        verify_samples_present_in_result([sampler_name], toppartion_results)
        # only check if the number of partitions is identical to the "size"
        # parameter
        partitions = toppartion_results[sampler_name]["partitions"]
        num_partitions = len(partitions)
        assert num_partitions == size, f"{num_partitions} != {size}"

    @staticmethod
    def _calc_expected_top_keys(min_pk, max_pk, stdvrng, confidence):
        # plese note, the confidence is not the confidence level of a normal
        # distribution. strictly speaking,
        # confidence_interval = [mean - margin_of_error, mean + margin_of_error]
        # margin_of_error = z * stddev / sqrt(N),
        # where N is the number of samples, z is a function of the confidence level.
        # but we just use a safe estimation here to represent z * sqrt(N)
        mean_pk = (min_pk + max_pk) // 2
        stddev_pk = math.ceil((mean_pk - min_pk) / stdvrng)
        # NOTE: the confidence interval is a function of the distribution, the level
        # of confidence and the number of samples. but no matter how high the level of
        # confidence is, it is always a nondeterministic process. in order to stablize
        # the test, what we can do in this test to increase the confidence level, in
        # other words, to increase the width of confidence interval.
        interval_pk = int(stddev_pk * confidence)
        return range(mean_pk - interval_pk, mean_pk + interval_pk + 1)

    def test_write_by_gaussian_key_distribution(self):
        """Validate that top write partitions are correlate to gaussian distribution

        Stress command will populate table on write operations for keys with gaussian distribution
        The custom profile file is used test_data/c-s-profiles/cs_normal_distribution.yaml
        The partition key will be populated with gaussian distribution : gaussian(1..3000,1500).
        This means that toppartitions for write operation should be: 1500, 1501, 1499, 1498, 1502

        the stress command will be running  for 30 seconds. toppartition command will be executed several
        times, each time result for top partition should be the same.
        """
        self.cluster.populate([1]).start()
        node = self.cluster.nodelist()[0]

        top_5_write_partitions_keys_results = []
        with ThreadPoolExecutor(max_workers=1) as excutor:
            stress_cmd = f"user profile={self.get_profile_full_path('cs_normal_distribution.yaml')} duration=30s ops(insert=1) \
                          no-warmup -port jmx=6868 -mode cql3 native -rate threads=1"
            future = excutor.submit(self.cluster.stress, stress_cmd.split(" "))
            for i in range(3):
                time.sleep(10)
                if i == 0:
                    assert future.running(), "Thread running stress command is not running."
                toppartition_result = self.run_toppartition_for(node, ks="keyspace1", cf="standard1", duration=2000, optional_params="-k 5")
                top_5_write_partitions_keys_results.append(toppartition_result["WRITES"]["partitions"].keys())
            verify_thread_execution(future)

        # the parameters come from the distribution of gaussian(1..3000,1500)
        # see also c-s-profiles/cs_normal_distribution.yaml
        top_keys = self._calc_expected_top_keys(min_pk=1, max_pk=3000, stdvrng=1500, confidence=6)
        expected_average_top_partition_keys = [f"(keyspace1:standard1) {key}" for key in top_keys]

        for actual_results in top_5_write_partitions_keys_results:
            verify_partition_keys(actual_partition_keys=actual_results, expected_toppartition_keys=expected_average_top_partition_keys)

    def test_read_by_gaussian_key_distribution(self):
        """Validate that top read partitions are correlate to gaussian distribution

        Stress command will populate table on read operations for keys distributed by gaus
        The custom profile file is used test_data/c-s-profiles/cs_normal_distribution.yaml
        The partition key will be populated with guas distribution : gaussian(1..3000,1500).
        This means that toppartitions for read operation should be: 1500, 1501, 1499, 1498, 1502

        the stress command will be running  for 30 seconds. toppartition command will be executed several
        times, each time result for top partition should be the same.
        """
        self.cluster.populate([1]).start()
        node = self.cluster.nodelist()[0]

        top_5_read_partitions_keys_results = []
        with ThreadPoolExecutor(max_workers=1) as excutor:
            # prepare the keyspace and tables, and write date to db
            stress_cmd = f"user profile={self.get_profile_full_path('cs_normal_distribution.yaml')} duration=10s ops(insert=1) \
                          no-warmup -port jmx=6868 -mode cql3 native -rate threads=1"
            future = excutor.submit(self.cluster.stress, stress_cmd.split(" "))
            verify_thread_execution(future)
            # start read queries
            stress_cmd = f"user profile={self.get_profile_full_path('cs_normal_distribution.yaml')} duration=30s ops(single=1) \
                          no-warmup -port jmx=6868 -mode cql3 native -rate threads=1"
            future = excutor.submit(self.cluster.stress, stress_cmd.split(" "))
            for i in range(3):
                time.sleep(10)
                if i == 0:
                    assert future.running(), "Thread running stress command is not running."
                toppartition_result = self.run_toppartition_for(node, ks="keyspace1", cf="standard1", duration=2000, optional_params="-k 5")
                top_5_read_partitions_keys_results.append(toppartition_result["READS"]["partitions"].keys())
            verify_thread_execution(future)

        # the parameters come from the distribution of gaussian(1..3000,1500)
        # see also c-s-profiles/cs_normal_distribution.yaml
        top_keys = self._calc_expected_top_keys(min_pk=1, max_pk=3000, stdvrng=1500, confidence=6)
        expected_average_top_partition_keys = [f"(keyspace1:standard1) {key}" for key in top_keys]

        for actual_results in top_5_read_partitions_keys_results:
            verify_partition_keys(actual_partition_keys=actual_results, expected_toppartition_keys=expected_average_top_partition_keys)

    def test_top_count_should_be_smaller_than_capacity(self):
        node, _session = self.prepare_cluster_with_ks_cfs_c1c2(["keyspace1"], [["columnfamily1"]])

        result = self.run_toppartitions_with_wrong_parameters(node, ks="keyspace1", cf="columnfamily1", duration=3000, optional_params="-k 15 -s 12")

        err_msg = r"TopK count \(-k\) option must be smaller th[ae]n the summary capacity \(-s\)"
        out = result.stdout + result.stderr
        assert re.search(err_msg, out), f"Wrong error message in output: \n {out}"

    def test_write_into_one_paritions_to_different_rows(self):
        """ """

        def write_25_ops_for_10_partitions(session, ks, cf):
            column_values = map(lambda x, y, z: [1, x, y, z], list(range(10)) * 250, list(range(2500)), [f"value{a}" for a in range(2500)])
            statement = session.prepare(f"INSERT INTO {ks}.{cf} (key1, key2, ckey, val) VALUES (?, ?, ?, ?)")
            if sync_starter.wait(30):
                logger.info("write_25_ops_for_10_partitions")
                execute_concurrent_with_args(session, statement, column_values)

        def write_into_one_partition_to_different_rows(session, ks, cf):
            column_values = map(lambda y, z: [1, 1, y, z], list(range(25)) * 100, [f"value{a}" for a in range(2500)])

            statement = session.prepare(f"INSERT INTO {ks}.{cf} (key1, key2, ckey, val) VALUES (?, ?, ?, ?)")
            if sync_starter.wait(30):
                logger.info("write_into_one_partition_to_different_rows")
                execute_concurrent_with_args(session, statement, column_values)

        node, session = self.prepare_cluster_with_ks_cf_complex_primary_key(ks="keyspace1", cf="columnfamily1")
        futures = []
        sync_starter = Event()
        with ThreadPoolExecutor(max_workers=3) as executor:
            run_tp_future = executor.submit(self.run_toppartition_for, node, ks="keyspace1", cf="columnfamily1")
            futures.append(run_tp_future)
            futures.append(executor.submit(write_25_ops_for_10_partitions, session, ks="keyspace1", cf="columnfamily1"))
            futures.append(executor.submit(write_into_one_partition_to_different_rows, session, ks="keyspace1", cf="columnfamily1"))
            wait_nodetool_toppartitions_start(node, self.cmd, timeout=30)
            time.sleep(1)
            sync_starter.set()
            for future in futures:
                verify_thread_execution(future)

            toppartition_result = run_tp_future.result()
        # expected first partitions
        expected_write_toppartition_results = [("(keyspace1:columnfamily1) 1:1", "150"), ("(keyspace1:columnfamily1) 1:0", "15")]
        # expected rest of partitions
        for i in range(2, 10):
            expected_write_toppartition_results.append((f"(keyspace1:columnfamily1) 1:{i}", "15"))
        verify_samples_present_in_result(["WRITES", "READS"], toppartition_result)
        verify_counters_for_sample(actual_results=toppartition_result["WRITES"], expected_results=expected_write_toppartition_results)
        verify_counters_for_sample(actual_results=toppartition_result["READS"], expected_results=[])

    def test_write_by_gaussian_key_distribution_for_compound_primary_key_and_large_rows_number(self):
        """validate that top write partitions with compound partition key (p_key, p_key1) and large number of rows
        per partition are correlate to guas key disribution

        The column family has next configuration:
            CREATE TABLE standard1 (
                p_key bigint,
                p_key1 bigint,
                cl_key bigint,
                c1 text,
                PRIMARY KEY((p_key,p_key1),cl_key)
            );

        The write operation has gaussian key distribution for each field of partition key:
              - name: p_key
                population: gaussian(1..3000,1500)
              - name: p_key1
                population: gaussian(1..3000,1500)

        this assumes that top write parition should be around next keys:
        1500:1500, 1500:1501, 1500:1499, 1499:1500 and simalar.

        Using profile file for c-s tool run write operation and validate the nodetool toppartition result
        """

        self.cluster.populate([1]).start()
        node = self.cluster.nodelist()[0]

        top_5_write_partitions_keys_results = []
        with ThreadPoolExecutor(max_workers=1) as excutor:
            stress_cmd = f"user profile={self.get_profile_full_path('cs_large_numbers_rows_per_partition.yaml')}  duration=60s ops(insert=1) \
                          no-warmup -port jmx=6868 -mode cql3 native -rate threads=1"
            future = excutor.submit(self.cluster.stress, stress_cmd.split(" "))
            time.sleep(5)

            for _ in range(3):
                time.sleep(15)
                toppartition_result = self.run_toppartition_for(node, ks="keyspace1", cf="standard1", duration=2000, optional_params="-k 5")

                top_5_write_partitions_keys_results.append(toppartition_result["WRITES"]["partitions"].keys())

            verify_thread_execution(future)

        expected_average_top_partition_keys = list(
            map(
                lambda x: f"(keyspace1:standard1) {x}",
                [
                    "1500:1501",
                    "1500:1500",
                    "1500:1499",
                    "1500:1502",
                    "1500:1498",
                    "1501:1500",
                    "1501:1499",
                    "1501:1501",
                    "1501:1498",
                    "1501:1502",
                    "1499:1500",
                    "1499:1499",
                    "1499:1501",
                    "1499:1498",
                    "1499:1502",
                    "1498:1500",
                    "1498:1499",
                    "1498:1501",
                    "1498:1498",
                    "1498:1502",
                    "1502:1500",
                    "1502:1499",
                    "1502:1501",
                    "1502:1498",
                    "1502:1502",
                ],
            )
        )

        for actual_results in top_5_write_partitions_keys_results:
            verify_partition_keys(actual_partition_keys=actual_results, expected_toppartition_keys=expected_average_top_partition_keys)

    def test_read_by_gaussian_key_distribution_for_compound_primary_key_and_large_rows_number(self):
        """validate that top read partitions with compound partition key (p_key, p_key1) and large number of rows
        per partition are correlate to guas key disribution

        The column family has next configuration:
            CREATE TABLE standard1 (
                p_key bigint,
                p_key1 bigint,
                cl_key bigint,
                c1 text,
                PRIMARY KEY((p_key,p_key1),cl_key)
            );

        The read operation has gaussian key distribution for each field of partition key:
              - name: p_key
                population: gaussian(1..3000,1500)
              - name: p_key1
                population: gaussian(1..3000,1500)
            and next read queries:


        this assumes that top read parition should be around next keys:
        1500:1500, 1500:1501, 1500:1499, 1499:1500 and simalar.

        Using profile file for c-s tool run read operation and validate the nodetool toppartition result
        """
        self.cluster.populate([1]).start(jvm_args=["--abort-on-lsa-bad-alloc", "0"])
        node = self.cluster.nodelist()[0]

        self.ignore_log_patterns += ["std::bad_alloc"]

        top_5_read_partitions_keys_results = []
        with ThreadPoolExecutor(max_workers=1) as excutor:
            # prepare keyspace and columnfamily
            stress_cmd = f"user profile={self.get_profile_full_path('cs_large_numbers_rows_per_partition.yaml')} duration=15s ops(insert=1) \
                          no-warmup -port jmx=6868 -mode cql3 native -rate threads=1"
            future = excutor.submit(self.cluster.stress, stress_cmd.split(" "))
            verify_thread_execution(future)

            stress_cmd = f"user profile={self.get_profile_full_path('cs_large_numbers_rows_per_partition.yaml')} duration=60s ops(multi_row=2) \
                          no-warmup -port jmx=6868 -mode cql3 native -rate threads=2"
            future = excutor.submit(self.cluster.stress, stress_cmd.split(" "))
            time.sleep(5)

            for _ in range(3):
                time.sleep(15)
                toppartition_result = self.run_toppartition_for(node, ks="keyspace1", cf="standard1", duration=2000, optional_params="-k 5")

                top_5_read_partitions_keys_results.append(toppartition_result["READS"]["partitions"].keys())

            verify_thread_execution(future)

        expected_average_top_partition_keys = list(
            map(
                lambda x: f"(keyspace1:standard1) {x}",
                [
                    "1500:1501",
                    "1500:1500",
                    "1500:1499",
                    "1500:1502",
                    "1500:1498",
                    "1501:1500",
                    "1501:1499",
                    "1501:1501",
                    "1501:1498",
                    "1501:1502",
                    "1499:1500",
                    "1499:1499",
                    "1499:1501",
                    "1499:1498",
                    "1499:1502",
                    "1498:1500",
                    "1498:1499",
                    "1498:1501",
                    "1498:1498",
                    "1498:1502",
                    "1502:1500",
                    "1502:1499",
                    "1502:1501",
                    "1502:1498",
                    "1502:1502",
                ],
            )
        )

        for actual_results in top_5_read_partitions_keys_results:
            verify_partition_keys(actual_partition_keys=actual_results, expected_toppartition_keys=expected_average_top_partition_keys)

    def test_one_keyspace_multiple_column_families_reads_sample_and_empty_writes_sample(self):
        """validate that read operations are correctly counted
        among multiple families in one keyspace

        Flow:
        1. Create KS and CF1, CF2, CF3
        2. Run toppartitions with duration 10 seconds
        3. Execute 1 read operation per family, for 3 partitions each
        4. Assert read sampler, empty write sampler
        """
        cfs = ["cf1", "cf2", "cf3"]
        node, session = self.prepare_cluster_with_ks_cfs_c1c2(keyspaces=["ks"], column_families=[cfs])

        for cf in cfs:
            run_operations_c1c2(session, mode="write", w_keys=10, cf=cf)

        sync_starter = Event()
        futures = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            run_tp_future = executor.submit(self.run_generic_toppartitions_for, node, keyspaces=["ks"], tables=None)
            for cf in cfs:
                logger.debug("Column family: %s", cf)
                futures.append(executor.submit(run_operations_c1c2, session, mode="read", r_keys=3, r_num=1000, ks="ks", cf=cf, ready_event=sync_starter))
            wait_nodetool_toppartitions_start(node, self.cmd, timeout=30)
            sync_starter.set()
            for f_read in futures:
                verify_thread_execution(f_read)

            verify_thread_execution(run_tp_future)
            toppartition_results = run_tp_future.result()

        expected_read_toppartitions_key_count = []
        for cf in cfs:
            expected_read_toppartitions_key_count += [(f"(ks:{cf}) k{i}", "100") for i in range(3)]

        verify_samples_present_in_result(["WRITES", "READS"], toppartition_results)
        verify_counters_for_sample(actual_results=toppartition_results["READS"], expected_results=expected_read_toppartitions_key_count)
        verify_counters_for_sample(actual_results=toppartition_results["WRITES"], expected_results=[])

    def test_one_keyspace_multiple_column_families_writes_sample_and_empty_reads_sample(self):
        """validate that write operations is correctly counted
        among multiple families in one keyspace

        Flow:
        1. Create KS and CF1, CF2, CF3
        2. Run toppartitions with duration 10 seconds
        3. Execute 1 write operation per family, for 3 partitions each
        4. Assert write sampler, empty read sampler
        """
        cfs = ["cf1", "cf2", "cf3"]
        node, session = self.prepare_cluster_with_ks_cfs_c1c2(keyspaces=["ks"], column_families=[cfs])

        sync_starter = Event()
        futures = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            run_tp_future = executor.submit(self.run_generic_toppartitions_for, node, keyspaces=["ks"], tables=None, duration=3000)
            for cf in cfs:
                futures.append(executor.submit(run_operations_c1c2, session, mode="write", w_keys=3, w_num=1000, ks="ks", cf=cf, ready_event=sync_starter))
            wait_nodetool_toppartitions_start(node, self.cmd, timeout=30)
            sync_starter.set()
            for f_write in futures:
                verify_thread_execution(f_write)

            verify_thread_execution(run_tp_future)
            toppartition_results = run_tp_future.result()

        expected_write_toppartitions_key_count = []
        for cf in cfs:
            expected_write_toppartitions_key_count += [(f"(ks:{cf}) k{i}", "100") for i in range(3)]

        verify_samples_present_in_result(["WRITES", "READS"], toppartition_results)
        verify_counters_for_sample(actual_results=toppartition_results["WRITES"], expected_results=expected_write_toppartitions_key_count)
        verify_counters_for_sample(actual_results=toppartition_results["READS"], expected_results=[])

    def test_all_column_families_writes_only(self):
        """validate that write operations in all families
        from all keyspaces are included in a general query

        Flow:
        1. Create KS1 with CF1, CF2, CF3 and KS2 with CF1, CF2
        2. Run toppartitions with duration 10 seconds
        3. Execute 1 write operation per family, for 1 partition each
        4. Assert write sampler, empty read sampler
        """
        keyspaces = ["ks1", "ks2"]
        families = [["cf1", "cf2", "cf3"], ["cf1", "cf2"]]
        node, session = self.prepare_cluster_with_ks_cfs_c1c2(keyspaces, families)

        sync_starter = Event()
        futures = []
        with ThreadPoolExecutor(max_workers=6) as executor:
            run_tp_future = executor.submit(self.run_generic_toppartitions_for, node, optional_params="-a writes")
            for ks, cfs in zip(keyspaces, families):
                for cf in cfs:
                    futures.append(executor.submit(run_operations_c1c2, session, mode="write", w_keys=1, w_num=5000, ks=ks, cf=cf, ready_event=sync_starter))
            wait_nodetool_toppartitions_start(node, self.cmd, timeout=30)
            sync_starter.set()
            for f_write in futures:
                verify_thread_execution(f_write)

            verify_thread_execution(run_tp_future)
            toppartition_results = run_tp_future.result()

        expected_write_toppartitions_key_count = [(f"({ks}:{cf}) k0", 100) for ks, cfs in zip(keyspaces, families) for cf in cfs]
        verify_samples_present_in_result(["WRITES"], toppartition_results)
        verify_counters_for_sample(actual_results=toppartition_results["WRITES"], expected_results=expected_write_toppartitions_key_count)

    def test_all_column_families_reads_only(self):
        """validate that read operations in all families
        from all keyspaces are included in a general query

        Flow:
        1. Create KS1 with CF1, CF2, CF3 and KS2 with CF1, CF2
        2. Run toppartitions with duration 10 seconds
        3. Execute 1 read operation per family, for 1 partition each
        4. Assert read sampler, empty write sampler
        """
        keyspaces = ["ks1", "ks2"]
        families = [["cf1", "cf2", "cf3"], ["cf1", "cf2"]]
        node, session = self.prepare_cluster_with_ks_cfs_c1c2(keyspaces, families)

        for ks, cfs in zip(keyspaces, families):
            for cf in cfs:
                run_operations_c1c2(session, mode="write", w_keys=1, ks=ks, cf=cf)

        sync_starter = Event()
        futures = []
        with ThreadPoolExecutor(max_workers=6) as executor:
            run_tp_future = executor.submit(self.run_generic_toppartitions_for, node, optional_params="-k 5")
            for ks, cfs in zip(keyspaces, families):
                for cf in cfs:
                    futures.append(executor.submit(run_operations_c1c2, session, mode="read", r_keys=1, r_num=1000, ks=ks, cf=cf, ready_event=sync_starter))
            wait_nodetool_toppartitions_start(node, self.cmd, timeout=30)
            sync_starter.set()
            for f_read in futures:
                verify_thread_execution(f_read)

            verify_thread_execution(run_tp_future)
            toppartition_results = run_tp_future.result()

        expected_read_toppartitions_key_count = [(f"({ks}:{cf}) k0", 100) for ks, cfs in zip(keyspaces, families) for cf in cfs]

        verify_samples_present_in_result(["WRITES", "READS"], toppartition_results)
        verify_counters_for_sample(actual_results=toppartition_results["READS"], expected_results=expected_read_toppartitions_key_count)
        verify_counters_for_sample(actual_results=toppartition_results["WRITES"], expected_results=[])
