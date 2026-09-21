import logging
import time
from collections import namedtuple

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from ccmlib.scylla_cluster import ScyllaCluster
from packaging.version import Version

from dtest_class import Tester, create_cf, create_ks, get_ip_from_node
from tools.cluster_topology import generate_cluster_topology
from tools.data import insert_c1c2, query_c1c2
from tools.files import get_list_of_sstables
from tools.marks import with_feature
from tools.metrics import get_node_metrics

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.next_gating


@pytest.mark.dtest_full
class TestRepair(Tester):
    def check_repair_logs(self):
        return not isinstance(self.cluster, ScyllaCluster)

    def check_rows_on_node(
        self,
        node_to_check,
        rows,
        found=None,
        missings=None,
        restart=True,
    ):
        if found is None:
            found = []
        if missings is None:
            missings = []
        stopped_nodes = []

        for node in self.cluster.nodes.values():
            if node.is_running() and node is not node_to_check:
                stopped_nodes.append(node)
                node.stop(wait_other_notice=True)

        session = self.patient_cql_connection(node_to_check, "ks")
        result = list(session.execute("SELECT * FROM cf LIMIT %d" % (rows * 2)))
        assert len(result) == rows, len(result)

        for k in found:
            query_c1c2(session, k, ConsistencyLevel.ONE)

        for k in missings:
            query = SimpleStatement("SELECT c1, c2 FROM cf WHERE key='k%d'" % k, consistency_level=ConsistencyLevel.ONE)
            res = list(session.execute(query))
            assert len(list(filter(lambda x: len(x) != 0, res))) == 0, res

        if restart:
            for node in stopped_nodes:
                node.start(wait_other_notice=True, wait_for_binary_proto=True)

    @pytest.mark.skip_if(with_feature("tablets"))
    def test_simple_sequential_repair(self):
        self._simple_repair(sequential=True)

    @pytest.mark.next_gating
    # @pytest.mark.'dtest-debug' - https://github.com/scylladb/scylla/issues/4384
    @pytest.mark.skip_if(with_feature("tablets"))
    def test_simple_parallel_repair(self):
        self._simple_repair(sequential=False)

    @pytest.mark.skip_if(with_feature("tablets"))
    def test_empty_vs_gcable_sequential_repair(self):
        self._empty_vs_gcable_no_repair(sequential=True)

    @pytest.mark.skip_if(with_feature("tablets"))
    def test_empty_vs_gcable_parallel_repair(self):
        self._empty_vs_gcable_no_repair(sequential=False)

    def _simple_repair(self, sequential=True, metrics=None):
        metrics, metrics_data = metrics or set(), dict()
        if not isinstance(metrics, set):
            metrics = set(metrics) if not isinstance(metrics, str) else {metrics}
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False}, batch_commitlog=True)
        logger.info("Starting cluster..")
        topo = generate_cluster_topology(rack_num=3)
        cluster.populate(topo).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 3)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        # Insert 1000 keys, kill node 3, insert 1 key, restart node 3, insert 1000 more keys
        logger.info("Inserting data...")
        insert_c1c2(session, n=1000, consistency=ConsistencyLevel.ALL)
        node3.flush()
        node3.stop(wait_other_notice=True)
        insert_c1c2(session, keys=(1000,), consistency=ConsistencyLevel.TWO)
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        insert_c1c2(session, keys=range(1001, 2001), consistency=ConsistencyLevel.ALL)

        cluster.flush()

        # Verify that node3 has only 2000 keys
        logger.info("Checking data on node3...")
        self.check_rows_on_node(node3, 2000, missings=[1000])

        # Verify that node1 has 2001 keys
        logger.info("Checking data on node1...")
        self.check_rows_on_node(node1, 2001, found=[1000])

        # Verify that node2 has 2001 keys
        logger.info("Checking data on node2...")
        self.check_rows_on_node(node2, 2001, found=[1000])

        time.sleep(10)  # see CASSANDRA-4373
        # Run repair
        start = time.time()
        logger.info("starting repair...")
        node1.repair(keyspace="ks")
        logger.info(f"Repair time: {time.time() - start}")
        if metrics:
            metrics_data = get_node_metrics(node_ip=get_ip_from_node(node1), metrics=metrics)

        # Validate that only one range was transfered
        if self.check_repair_logs():
            out_of_sync_logs = node1.grep_log(r"([0-9.]+) and ([0-9.]+) have ([0-9]+) range(s) out of sync")
            assert len(out_of_sync_logs) == 2, "Lines matching: " + str([elt[0] for elt in out_of_sync_logs])
            valid = [(node1.address(), node3.address()), (node3.address(), node1.address()), (node2.address(), node3.address()), (node3.address(), node2.address())]
            for _, match in out_of_sync_logs:
                assert int(match.group(3)) == 1, "Expecting 1 range out of sync, got " + match.group(3)
                assert (match.group(1), match.group(2)) in valid, str((match.group(1), match.group(2)))
                valid.remove((match.group(1), match.group(2)))
                valid.remove((match.group(2), match.group(1)))

        # Check node3 now has the key
        self.check_rows_on_node(node3, 2001, found=[1000], restart=False)
        return metrics_data

    def _empty_vs_gcable_no_repair(self, sequential):
        """
        Repairing empty partition and tombstoned partition older than gc grace
        should be treated as the same and no repair is necessary.
        See CASSANDRA-8979.
        """
        cluster = self.cluster
        topo = generate_cluster_topology(rack_num=2)
        cluster.populate(topo)
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False}, batch_commitlog=True)
        cluster.start()
        node1, node2 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        # create keyspace with RF=2 to be able to be repaired
        create_ks(session, "ks", 2)
        # we create two tables, one has low gc grace seconds so that the data
        # can be dropped during test (but we don't actually drop them).
        # the other has default gc.
        # compaction is disabled not to purge data
        query = """
            CREATE TABLE cf1 (
                key text,
                c1 text,
                c2 text,
                PRIMARY KEY (key, c1)
            )
            WITH gc_grace_seconds=1
            AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'};
        """
        session.execute(query)
        time.sleep(0.5)
        query = """
            CREATE TABLE cf2 (
                key text,
                c1 text,
                c2 text,
                PRIMARY KEY (key, c1)
            )
            WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'};
        """
        session.execute(query)
        time.sleep(0.5)

        # take down node2, so that only node1 has gc-able data
        node2.stop(wait_other_notice=True)
        for cf in ["cf1", "cf2"]:
            # insert some data
            for i in range(10):
                for j in range(1000):
                    query = SimpleStatement("INSERT INTO %s (key, c1, c2) VALUES ('k%d', 'v%d', 'value')" % (cf, i, j), consistency_level=ConsistencyLevel.ONE)
                    session.execute(query)
            node1.flush()
            # delete those data, half with row tombstone, and the rest with cell range tombstones
            for i in range(5):
                query = SimpleStatement("DELETE FROM %s WHERE key='k%d'" % (cf, i), consistency_level=ConsistencyLevel.ONE)
                session.execute(query)
            node1.flush()
            for i in range(5, 10):
                for j in range(1000):
                    query = SimpleStatement("DELETE FROM %s WHERE key='k%d' AND c1='v%d'" % (cf, i, j), consistency_level=ConsistencyLevel.ONE)
                    session.execute(query)
            node1.flush()

        # sleep until gc grace seconds pass so that cf1 can be dropped
        time.sleep(2)

        # bring up node2 and repair
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)
        node2.repair(keyspace="ks")

        # check no rows will be returned
        for cf in ["cf1", "cf2"]:
            for i in range(10):
                query = SimpleStatement("SELECT c1, c2 FROM %s WHERE key='k%d'" % (cf, i), consistency_level=ConsistencyLevel.ALL)
                res = list(session.execute(query))
                assert len(list(filter(lambda x: len(x) != 0, res))) == 0, res

        # check log for no repair happened for gcable data
        if self.check_repair_logs():
            out_of_sync_logs = node2.grep_log(r"([0-9.]+) and ([0-9.]+) have ([0-9]+) range(s) out of sync for cf1")
            assert len(out_of_sync_logs) == 0, "GC-able data does not need to be repaired with empty data: " + str([elt[0] for elt in out_of_sync_logs])
            # check log for actual repair for non gcable data
            out_of_sync_logs = node2.grep_log(r"([0-9.]+) and /([0-9.]+) have ([0-9]+) range(s) out of sync for cf2")
            assert len(out_of_sync_logs) > 0, "Non GC-able data should be repaired"

    @pytest.mark.skip_if(with_feature("tablets"))
    def test_local_dc_repair(self):
        cluster = self._setup_multi_dc()
        node1 = cluster.nodes["node1"]
        node2 = cluster.nodes["node2"]

        logger.info("starting repair...")
        node1.repair(keyspace="ks", local=True)

        # Verify that only nodes in dc1 are involved in repair
        if self.check_repair_logs():
            out_of_sync_logs = node1.grep_log(r"([0-9.]+) and ([0-9.]+) have ([0-9]+) range(s) out of sync")
            assert len(out_of_sync_logs) == 1, "Lines matching: %d" % len(out_of_sync_logs)
            _, match = out_of_sync_logs[0]
            assert int(match.group(3)) == 1, "Expecting 1 range out of sync, got " + match.group(3)
            valid = [node1.address(), node2.address()]
            assert match.group(1) in valid, "Unrelated node found in local repair: " + match.group(1)
            valid.remove(match.group(1))
            assert match.group(2) in valid, "Unrelated node found in local repair: " + match.group(2)
        # Check node2 now has the key
        self.check_rows_on_node(node2, 2001, found=[1000], restart=False)

    def test_dc_repair(self):
        cluster = self._setup_multi_dc()
        node1 = cluster.nodes["node1"]
        node2 = cluster.nodes["node2"]
        node3 = cluster.nodes["node3"]

        logger.info("starting repair...")
        node1.repair(keyspace="ks", dcs=["dc1", "dc2"])

        # Verify that only nodes in dc1 and dc2 are involved in repair
        if self.check_repair_logs():
            out_of_sync_logs = node1.grep_log(r"([0-9.]+) and ([0-9.]+) have ([0-9]+) range(s) out of sync")
            assert len(out_of_sync_logs) == 2, "Lines matching: " + str([elt[0] for elt in out_of_sync_logs])
            valid = [(node1.address(), node2.address()), (node2.address(), node1.address()), (node2.address(), node3.address()), (node3.address(), node2.address())]
            for _, match in out_of_sync_logs:
                assert int(match.group(3)) == 1, "Expecting 1 range out of sync, got " + match.group(3)
                assert (match.group(1), match.group(2)) in valid, str((match.group(1), match.group(2)))
                valid.remove((match.group(1), match.group(2)))
                valid.remove((match.group(2), match.group(1)))
        # Check node2 now has the key
        self.check_rows_on_node(node2, 2001, found=[1000], restart=False)

    def _setup_multi_dc(self):
        """
        Sets up 3 DCs (2 nodes in 'dc1', and one each in 'dc2' and 'dc3').
        After set up, node2 in dc1 lacks some data and needs to be repaired.
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False}, batch_commitlog=True)
        logger.info("Starting cluster..")
        # populate 2 nodes in dc1, and one node each in dc2 and dc3
        topo = {"dc1": {"rack1": 1, "rack2": 1}, "dc2": {"rack1": 1}, "dc3": {"rack1": 1}}
        cluster.populate(topo).start()

        [node1, node2, node3, node4] = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        session.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 1, 'dc3':1};")
        session.execute("USE ks")
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        # Insert 1000 keys, kill node 3, insert 1 key, restart node 3, insert 1000 more keys
        logger.info("Inserting data...")
        insert_c1c2(session, n=1000, consistency=ConsistencyLevel.ALL)
        node2.flush()
        node2.stop(wait_other_notice=True)
        insert_c1c2(session, keys=(1000,), consistency=ConsistencyLevel.THREE)
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)
        node1.watch_log_for_alive(node2)
        insert_c1c2(session, keys=range(1001, 2001), consistency=ConsistencyLevel.ALL)

        cluster.flush()

        # Verify that only node2 has only 2000 keys and others have 2001 keys
        logger.info("Checking data...")
        self.check_rows_on_node(node2, 2000, missings=[1000])
        for node in [node1, node3, node4]:
            self.check_rows_on_node(node, 2001, found=[1000])
        return cluster

    @pytest.mark.skip_if(with_feature("tablets"))
    def test_two_consecutive_repair(self):
        """
        - Create a cluster
        - Insert data
        - cause one node to miss some data
        - trigger repair to fix it
        - trigger another repair and verify no data was streamed.
        """
        metric_name = "scylla_repair_tx_row_bytes"

        sequential = True
        metric_data = self._simple_repair(sequential=sequential, metrics=[metric_name])[metric_name]
        logger.info(f"Verifying the metric value of '{metric_name}' after the first repair is greater from '0'")
        assert metric_data > 0, f"Got incorrect metric value '{metric_data}'"
        for node_idx, node in enumerate(self.cluster.nodelist()):
            if node.status.lower() == "down":
                logger.info(f"Starting node{node_idx} because is in state down")
        self.cluster.start()
        node1 = self.cluster.nodelist()[0]

        start = time.time()
        logger.info("Starting second repair...")
        node1.repair(keyspace="ks")
        logger.info(f"Repair time: {time.time() - start}")
        logger.info(f"Verifying the metric value of '{metric_name}' after the second repair is  '0'")
        metric_data = get_node_metrics(node_ip=get_ip_from_node(node1), metrics=[metric_name])[metric_name]
        assert metric_data == 0, "Got incorrect value '{metric_data}'"

    def test_repair_with_disabled_compaction(self):
        """
        Based on issue 7525
        There was done a change to the write flow to avoid writing a new file for each follower.
        For further information, please check PR 7528
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False}, batch_commitlog=True)
        logger.debug("Starting cluster..")
        topo = generate_cluster_topology(rack_num=3)
        cluster.populate(topo).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 3)
        create_cf(session, "cf", columns={"c1": "text", "c2": "text"}, compaction={"class": "TimeWindowCompactionStrategy", "enabled": "false"})

        # Insert 1000 keys, kill node 3, insert 1 key, restart node 3, insert 1000 more keys
        logger.debug("Inserting data...")
        insert_c1c2(session, n=1000, consistency=ConsistencyLevel.ALL)
        node3.flush()
        node3.stop(wait_other_notice=True)
        insert_c1c2(session, keys=(1001, 2001), consistency=ConsistencyLevel.TWO)
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        insert_c1c2(session, keys=range(2001), consistency=ConsistencyLevel.ALL)

        cluster.flush()
        before = get_list_of_sstables(node3, "ks", "cf", suffix=".db")
        node2.repair(keyspace="ks")
        after = get_list_of_sstables(node3, "ks", "cf", suffix=".db")
        assert before < after, "Number of sstables should have increased because of the repair"
