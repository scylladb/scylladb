import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from cassandra import InvalidRequest
from cassandra.cluster import NoHostAvailable
from ccmlib.node import NodetoolError

from dtest_class import Tester, wait_for
from tools.data import rows_to_list
from tools.marks import unmark
from tools.rackdc import update_properties
from tools.stress import assert_cs_success, format_cs_output

logger = logging.getLogger(__name__)


@pytest.mark.next_gating
@pytest.mark.dtest_full
class TestNodetool(Tester):
    @staticmethod
    def filter_asan_warning(output):
        lines = []
        for line in output.split("\n"):
            # when ASan intercepts swapcontext calls, it warns like:
            #
            # ==<pid>==: WARNING: ASan doesn't fully support \
            # makecontext/swapcontext functions and may produce false
            # positives in some cases!
            #
            # and this error message is printed to stderr. but this is not
            # an error emitted from the tool because of a failure, so
            # let's filter it out
            if "WARNING: ASan" in line:
                continue
            lines.append(line)
        return "\n".join(lines)

    def test_decommission_after_drain_is_invalid(self):
        """
        @jira_ticket CASSANDRA-8741

        Running a decommission after a drain should generate
        an unsupported operation message and exit with an error
        code (which we receive as a NodetoolError exception).
        """
        cluster = self.cluster
        cluster.populate([3]).start()

        node = cluster.nodelist()[0]
        node.drain(block_on_log=True)

        with pytest.raises(expected_exception=(NodetoolError,)) as err:
            node.decommission()
        expected_msg = "Node in DRAINED state"
        assert expected_msg in err.value.stdout or expected_msg in err.value.stderr

    def test_correct_dc_rack_in_nodetool_info(self):
        """
        @jira_ticket CASSANDRA-10382

        Test that nodetool info returns the correct rack and dc
        """

        cluster = self.cluster
        cluster.populate([2, 2])
        cluster.set_configuration_options(values={"endpoint_snitch": "org.apache.cassandra.locator.GossipingPropertyFileSnitch"})

        for idx, node in enumerate(cluster.nodelist()):
            update_properties(nodes=[node], properties={"rack": f"rack{idx % 2}"})

        cluster.start(wait_for_binary_proto=True)

        for idx, node in enumerate(cluster.nodelist()):
            out, err = node.nodetool("info")
            assert not self.filter_asan_warning(err), err
            logger.info(out)
            for line in out.split(os.linesep):
                if line.startswith("Data Center"):
                    assert line.endswith(node.data_center), f"Expected dc {node.data_center} for {node.address()} but got {line.rsplit(None, 1)[-1]}"
                elif line.startswith("Rack"):
                    rack = f"rack{idx % 2}"
                    assert line.endswith(rack), f"Expected rack {rack} for {node.address()} but got {line.rsplit(None, 1)[-1]}"

    @staticmethod
    def _background_workload(node):
        logger.info("start write workload in background...")
        cs_result = node.stress(
            ["write", "duration=60s", "no-warmup", "-schema", "replication(factor=3)", "-rate", "threads=10", "-log", "interval=10", "-errors", "retries=10", "delay-policy=exponential", "min-delay-ms=20", "max-delay-ms=20000"]
        )
        logger.info("background workload finished")
        logger.info(format_cs_output(cs_result))
        return cs_result

    def _remove_seed(self, method="kill"):
        """
        We have a old issue (scylla/issues/2090), cassandra-stress will exit if
        seed node is decomission or killed. This new subtest is used to reproduce it.
        """
        self.cluster.populate({"DC1": {"RAC1": 2, "RAC2": 1, "RAC3": 1}}).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()[0:2]

        executor = ThreadPoolExecutor(max_workers=1)
        thread = executor.submit(self._background_workload, node1)

        # Wait for cassandra-stress to start writing data instead of arbitrary sleep
        session = self.patient_cql_connection(node2)

        def stress_has_written_data():
            try:
                result = rows_to_list(session.execute("SELECT * FROM keyspace1.standard1 LIMIT 1"))
                return len(result) > 0
            except (NoHostAvailable, InvalidRequest):
                # NoHostAvailable: cluster not ready yet
                # InvalidRequest: keyspace/table doesn't exist yet (stress hasn't created it)
                return False

        try:
            logger.info("Waiting for cassandra-stress to start writing data...")
            wait_for(stress_has_written_data, timeout=30, step=1, text="cassandra-stress to write data")
            logger.info("Cassandra-stress has started writing data")
        finally:
            session.shutdown()

        if method == "kill":
            logger.info("start to kill node1 ...")
            node1.stop(gently=False)
            logger.info("node1 has been killed")
        elif method == "decommission":
            logger.info("start to decommission node1 ...")
            node1.decommission()
            logger.info("decommission node1 finished")
        else:
            raise Exception("Unknown method: %s" % method)

        out = node2.nodetool("status", capture_output=True)[0]
        logger.info(out)
        cs_result = thread.result()
        assert_cs_success(cs_result)

    @pytest.mark.parametrize("method", ["decommission", "kill"])
    @pytest.mark.use_cassandra_stress
    @unmark.next_gating  # https://github.com/scylladb/scylla-dtest/issues/3372
    def test_seed(self, method):
        """
        Test if cassandra-stress works well when seed node is  "decommission" or "killed".
        """
        self._remove_seed(method=method)
