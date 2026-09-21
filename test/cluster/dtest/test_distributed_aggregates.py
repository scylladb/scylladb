import os
import signal

import pytest
from cassandra import ConsistencyLevel, OperationTimedOut
from cassandra.cluster import NoHostAvailable
from cassandra.query import SimpleStatement

from dtest_class import Tester, create_ks
from tools.assertions import assert_exception, assert_row_count
from tools.cluster_topology import generate_cluster_topology
from tools.data import drop_table


@pytest.mark.dtest_full
class TestDistributedAggregates(Tester):
    def prepare(self, rf, jvm_args=None, options=None):
        assert rf > 0
        assert rf <= 2, "There cannot be more nodes than the value of RF"

        if jvm_args is None:
            jvm_args = []
        if options:
            self.cluster.set_configuration_options(values=options)

        topology_layout = generate_cluster_topology(dc_num=1, rack_num=rf, nodes_per_rack=2 // rf)

        self.cluster.populate(topology_layout).start(jvm_args=jvm_args)
        [self.node1, self.node2] = self.cluster.nodelist()
        self.session1 = self.patient_exclusive_cql_connection(self.node1)

        create_ks(self.session1, "ks", rf)
        drop_table(session=self.session1, table_name="tbl", if_exists=True)
        self.session1.execute(
            """
            CREATE TABLE tbl (a int primary key, b int)
        """
        )

    def test_retrying_dispatcher(self):
        # Even if RF=2 on 2 nodes setup, forward_service will distribute
        # the aggregation query to both nodes. In this case, retrying_dispatcher
        # should execute the failed subquery on super-coordinator with success.

        self.fixture_dtest_setup.allow_log_errors = True
        self.prepare(rf=2)
        node2_pid = self.node2.pid

        insert_statement = self.session1.prepare("INSERT INTO ks.tbl(a, b) VALUES (?, ?)")
        self.session1.execute(insert_statement.bind((1, 1)))
        self.session1.execute(insert_statement.bind((2, 2)))
        self.session1.execute(insert_statement.bind((3, 3)))
        self.session1.execute(insert_statement.bind((4, 4)))

        # Send SIGSTOP instead of `node.stop()` to not inform node1 about stopping node2
        os.kill(node2_pid, signal.SIGSTOP)
        assert_row_count(self.session1, "ks.tbl", 4)
        os.kill(node2_pid, signal.SIGCONT)

    def test_retrying_dispatcher_fails(self):
        # With RF=1 and because a node is not working (not only single connection error),
        # failure of any subquery means failure of the whole aggregation query.

        self.fixture_dtest_setup.allow_log_errors = True
        self.prepare(rf=1)
        node2_pid = self.node2.pid

        insert_statement = self.session1.prepare("INSERT INTO ks.tbl(a, b) VALUES (?, ?)")
        self.session1.execute(insert_statement.bind((1, 1)))
        self.session1.execute(insert_statement.bind((2, 2)))
        self.session1.execute(insert_statement.bind((3, 3)))
        self.session1.execute(insert_statement.bind((4, 4)))

        # Send SIGSTOP instead of `node.stop()` to not inform node1 about stopping node2
        os.kill(node2_pid, signal.SIGSTOP)
        assert_exception(self.session1, "SELECT COUNT(*) FROM ks.tbl", expected=(NoHostAvailable, OperationTimedOut))
        os.kill(node2_pid, signal.SIGCONT)
