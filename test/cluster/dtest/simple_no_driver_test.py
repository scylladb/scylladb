import logging

import pytest
from ccmlib.scylla_cluster import ScyllaCluster

from dtest_class import Tester
from dtest_setup_overrides import DTestSetupOverrides
from tools.misc import ImmutableMapping

logger = logging.getLogger(__name__)
pytestmark = pytest.mark.next_gating


@pytest.mark.dtest_full
@pytest.mark.single_node
@pytest.mark.use_cassandra_stress
@pytest.mark.parametrize("smp_options", [["--smp", "1"], ["--smp", "2"]], ids=["SMP=1", "SMP=2"])
class TestSimple(Tester):
    @pytest.fixture(scope="function", autouse=True)
    def fixture_dtest_setup_overrides(self, dtest_config, smp_options):
        dtest_setup_overrides = DTestSetupOverrides()
        dtest_setup_overrides.cluster_options = ImmutableMapping({"start_rpc": "true"})
        self.scylla_args = smp_options
        return dtest_setup_overrides

    def prepare(self):
        """
        Sets up cluster to test against. Currently 3 CCM Nodes
        """
        cluster = self.cluster
        return cluster

    def stress_write(self, node):
        """
        Writes data via stress. Should write exact data expected by stress_read()
        """
        node.stress(["write", "n=100000", "-mode", "native", "cql3", "-rate", "threads=1", "-pop", "seq=1..100000"])

    def stress_read(self, node):
        """
        Reads previously written data via stress. Should check for exact data written
        by stress_write().
        """
        # Verify the data
        return node.stress(["read", "n=100000", "-mode", "cql3", "simplenative", "-rate", "threads=1", "-pop", "seq=1..100000"])

    def validate_stress_output(self, stress_output, expect_failure=False, expect_errors=False):
        """
        Validates if data was lost, or determines if stress encountered errors.
        Should be updated once stress has more sophisticated validation.

        outfile - an output file with stress output.
        expect_failure - if data loss should be expected
        expect_errors - if exceptions should be expected
        """
        output = stress_output.stdout + stress_output.stderr

        logger.debug(output)
        failure = output.find("Data returned was not validated")
        if expect_failure:
            assert failure >= 0, "No missing data detected, despite data loss"
        else:
            assert failure == -1, "Stress failed to validate all data"

        failure = output.find("Exception")
        if expect_errors:
            assert failure >= 0, "No errors detected, despite invalid cluster state"
        else:
            assert failure == -1, "Error while reading data"

    def test_simple_single_node_write_read(self):
        """
        A basic test that writes data at CL=ONE, RF=1
        Tests to ensure no data is lost or errors thrown.
        """
        cluster = self.prepare()
        jvm_args = []
        if type(cluster) is ScyllaCluster:
            jvm_args = self.scylla_args
        cluster.populate(1).start(jvm_args=jvm_args)
        node1 = cluster.nodelist()[0]
        self.stress_write(node1)
        out = self.stress_read(node1)
        self.validate_stress_output(out)
