import logging

import pytest
from cassandra import ConsistencyLevel

from dtest_class import Tester

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.next_gating


@pytest.mark.dtest_full
# EverywhereStrategy is not supported with tablets
@pytest.mark.required_features("!tablets")
class TestEverywhereConsistency(Tester):
    def test_bootstrap_second_dc(self):
        """
        Check if data in EverywhereReplicationStrategy is synced to second DC
        when we bootstrap a node in that DC to a cluster that currently only
        has nodes in the first DC.
        """
        cluster = self.cluster

        # We want to bootstrap in single DC for now, but configure the cluster
        # in multi-DC mode. Passing a list to populate(...) will do that
        cluster.populate([1]).start()

        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)

        logger.info("Creating Everywhere strategy keyspace and table")
        session.execute("create keyspace ks with replication = {'class': 'EverywhereStrategy'}")
        session.execute("create table ks.t (pk int primary key)")

        logger.info("Inserting rows to the table with CL=ALL (dc1)")
        num = 100
        stmt = session.prepare("insert into ks.t (pk) values (?)")
        stmt.consistency_level = ConsistencyLevel.ALL
        for i in range(num):
            session.execute(stmt, (i,))

        logger.info("Bootstrapping another node in dc2")
        node2 = self.cluster.new_node(2, data_center="dc2")
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)

        logger.info("Stopping node in dc1")
        node1.stop(gently=True, wait_other_notice=True)

        logger.info("Driver connecting to node in dc2")
        session = self.patient_exclusive_cql_connection(node2)

        logger.info("Selecting rows with CL=LOCAL_ONE")
        stmt = session.prepare("select pk from ks.t")
        stmt.consistency_level = ConsistencyLevel.LOCAL_ONE
        keys = set(r[0] for r in session.execute(stmt))
        assert keys == set(range(100))
