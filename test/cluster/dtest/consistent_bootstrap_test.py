import logging

import pytest
from cassandra import ConsistencyLevel

from dtest_class import Tester, create_ks
from tools.cluster_topology import generate_cluster_topology
from tools.data import create_c1c2_table, insert_c1c2, query_c1c2

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
class TestBootstrapConsistency(Tester):
    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_consistent_reads_after_bootstrap(self):
        logger.info("Creating a ring")
        cluster = self.cluster
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False, "write_request_timeout_in_ms": 60000, "read_request_timeout_in_ms": 60000, "dynamic_snitch_badness_threshold": 0.0}, batch_commitlog=True)
        cluster_topology = generate_cluster_topology(rack_num=2)
        cluster.populate(cluster_topology).start()
        node1, node2 = cluster.nodelist()
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)

        logger.info("Set to talk to node 2")
        n2session = self.patient_cql_connection(node2)
        create_ks(n2session, "ks", 2)
        create_c1c2_table(n2session)

        logger.info("Generating some data for all nodes")
        insert_c1c2(n2session, keys=range(10, 20), consistency=ConsistencyLevel.ALL)

        node1.flush()
        logger.info("Taking down node1")
        node1.stop(wait_other_notice=True)

        logger.info("Writing data to only node2")
        insert_c1c2(n2session, keys=range(30, 1000), consistency=ConsistencyLevel.ONE)
        node2.flush()

        logger.info("Restart node1")
        node1.start(wait_other_notice=True)

        logger.info("Bootstraping node3 in rack2")
        # add new node in rack2 because rf-rack-valid-keyspaces doesn't allow to
        # create new rack. Issue: scylladb/scylladb#23426
        node3 = self.cluster.new_node(3, data_center="datacenter1", rack="rack2")
        node3.start(wait_for_binary_proto=True)

        n3session = self.patient_cql_connection(node3)
        n3session.execute("USE ks")
        logger.info("Checking that no data was lost")
        for n in range(10, 20):
            query_c1c2(n3session, n, ConsistencyLevel.ALL)

        for n in range(30, 1000):
            query_c1c2(n3session, n, ConsistencyLevel.ALL)
