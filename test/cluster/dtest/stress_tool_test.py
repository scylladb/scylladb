import pytest
from cassandra.cluster import Session

from dtest_class import Tester, create_ks
from tools.cassandra_stess import CassandraStressDocker
from tools.data import rows_to_list

pytestmark = pytest.mark.next_gating


@pytest.mark.dtest_full
@pytest.mark.single_node
@pytest.mark.use_cassandra_stress
class TestStressSparsenessRatio(Tester):
    """
    @jira_ticket CASSANDRA-9522

    Tests for the `row-population-ratio` parameter to `cassandra-stress`.
    """

    def test_uniform_ratio(self):
        """
        Tests that the ratio-specifying string 'uniform(5..15)/50' results in
        ~80% of the values written being non-null.
        """
        self.distribution_template(ratio_spec="uniform(5..15)/50", expected_ratio=0.8, delta=0.1)

    def test_fixed_ratio(self):
        """
        Tests that the string 'fixed(1)/3' results in ~1/3 of the values
        written being non-null.
        """
        self.distribution_template(ratio_spec="fixed(1)/3", expected_ratio=1 - 1 / 3, delta=0.01)

    def distribution_template(self, ratio_spec, expected_ratio, delta):
        """
        @param ratio_spec the string passed to `row-population-ratio` in the call to `cassandra-stress`
        @param expected_ratio the expected ratio of null/non-null values in the values written
        @param delta the acceptable delta between the expected and actual ratios

        A parameterized test for the `row-population-ratio` parameter to
        `cassandra-stress`.
        """
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        node = self.cluster.nodelist()[0]
        node.stress(["write", "n=1000", "-rate", "threads=50", "-col", "n=FIXED(50)", "-insert", f"row-population-ratio={ratio_spec}"])
        session = self.patient_cql_connection(node)
        written = rows_to_list(session.execute("SELECT * FROM keyspace1.standard1;"))

        num_nones = sum(row.count(None) for row in written)
        num_results = sum(len(row) for row in written)

        assert pytest.approx(float(num_nones) / num_results, abs=delta) == expected_ratio


@pytest.mark.dtest_full
class TestCassandraStress(Tester):
    def test_cassandra_stress_sanity(self):
        self.cluster.populate(2).start(wait_for_binary_proto=True, wait_other_notice=True)
        with CassandraStressDocker(node=self.cluster.nodelist()[0], stress_cmd="cassandra-stress write duration=1m no-warmup -mode cql3 native  -rate threads=1 throttle=500/s", timeout=180) as cassandra_stress_docker:
            cassandra_stress_docker.run()
            result = cassandra_stress_docker.wait_for_stress_results()
            assert result.rc == 0, result.stderr

    def test_cassandra_stress_multi_dc(self):
        nodes = {"dc1": {"r1": 1, "r2": 1, "r3": 1}, "dc2": {"r1": 1, "r2": 1, "r3": 1}}
        self.cluster.populate(nodes).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1 = self.cluster.nodelist()[0]
        with CassandraStressDocker(
            node=node1, stress_cmd="cassandra-stress write duration=1m no-warmup  -rate threads=2 throttle=500/s -schema replication(strategy=NetworkTopologyStrategy,dc1=3,dc2=3)", timeout=180
        ) as cassandra_stress_docker:
            cassandra_stress_docker.run()
            result = cassandra_stress_docker.wait_for_stress_results()
            assert result.rc == 0, result.stderr

    @pytest.mark.skip(reason="internal validation test for docker resource limits, not a real dtest")
    @pytest.mark.tools_unittest
    def test_cassandra_stress_docker_resource_limits(self):
        """
        Test that Docker containers created by CassandraStressDocker have proper resource limits.
        Verifies that containers are limited to 1 CPU and 256MB of memory.
        """
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        with CassandraStressDocker(node=self.cluster.nodelist()[0], stress_cmd="cassandra-stress write duration=10s -mode cql3 native -rate threads=1 throttle=500/s", timeout=30) as cassandra_stress_docker:
            container = cassandra_stress_docker.container
            container.reload()

            assert container.attrs["HostConfig"]["Memory"] == 268435456
            assert container.attrs["HostConfig"]["NanoCpus"] == 1000000000

            cassandra_stress_docker.run()
            result = cassandra_stress_docker.wait_for_stress_results()
            assert result.rc == 0, result.stderr
