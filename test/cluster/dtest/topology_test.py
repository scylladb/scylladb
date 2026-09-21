import logging
import re
import time
from threading import Thread

import pytest
from cassandra import ConsistencyLevel
from ccmlib.node import NodetoolError
from ccmlib.scylla_node import ScyllaNode

from dtest_class import Tester, create_cf, create_ks
from tools.assertions import assert_almost_equal
from tools.cluster_topology import generate_cluster_topology
from tools.data import insert_c1c2, query_c1c2
from tools.marks import unmark
from tools.status import wait_for_nodes_status

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestTopology(Tester):
    REMOVENODE_REJECT_MSG = r"Rejected removenode operation.*the node being removed is alive, maybe you should use decommission instead"
    REMOVENODE_HOSTID_NOT_IN_CLUSTER = "Host ID not found in the cluster"

    def prepare_cluster(self, cluster_topology: dict[str, dict[str, int]], rf: int):
        self.cluster.populate(cluster_topology).start(wait_other_notice=True)
        node: ScyllaNode = self.cluster.nodelist()[0]
        with self.patient_cql_connection(node) as session:
            create_ks(session, "ks", rf)
            create_cf(
                session=session,
                name="cf",
                columns={"c1": "text", "c2": "text"},
            )
        logger.debug(f"Insert 1000 rows ...")
        with self.patient_exclusive_cql_connection(node, "ks") as session1:
            insert_c1c2(session1, keys=range(1000), consistency=ConsistencyLevel.QUORUM)

        for current_node in self.cluster.nodelist():
            current_node.flush()

    def test_decommissioned_node_cant_rejoin(self, fixture_dtest_setup):
        """
        @jira_ticket CASSANDRA-8801

        Test that a decommissioned node can't rejoin the cluster by:

        - creating a cluster,
        - decommissioning a node, and
        - asserting that the "decommissioned node won't rejoin" error is in the
        logs for that node and
        - asserting that the node is not running.
        """
        rejoin_err = "This node was decommissioned and will not rejoin the ring"

        fixture_dtest_setup.allow_log_errors = True
        fixture_dtest_setup.ignore_log_patterns += [rejoin_err]
        cluster_topology = generate_cluster_topology(rack_num=3)
        self.cluster.populate(cluster_topology).start(wait_for_binary_proto=True)
        [_node1, _node2, node3] = self.cluster.nodelist()

        logger.debug("decommissioning...")
        node3.decommission()
        logger.debug("stopping...")
        node3.stop()
        logger.debug("attempting restart...")
        node3.start(no_wait=True)

        node3.watch_log_for(rejoin_err, timeout=60)
        logger.debug("waiting for node to stop...")
        start = time.time()
        while node3.is_running() and time.time() - start < 60:
            time.sleep(1)
        assert not node3.is_running()

    @pytest.mark.dtest_debug
    # FIXME: https://github.com/scylladb/scylla-dtest/issues/5310
    @pytest.mark.cluster_options(enable_small_table_optimization_for_rbno=False)
    def test_crash_during_decommission(self):
        """
        If a node crashes whilst another node is being decommissioned,
        upon restarting the crashed node should not have invalid entries
        for the decommissioned node
        @jira_ticket CASSANDRA-10231
        """

        # this error is expected in teardown after this test in raft topology mode
        ignore_error = "raft_topology - Decommission failed"
        self.ignore_log_patterns += [ignore_error]
        cluster = self.cluster
        # Test relies on slow bootstrap / decommission, so force RBNO without small table optimization
        cluster.set_configuration_options({"enable_repair_based_node_ops": True, "allowed_repair_based_node_ops": "replace,removenode,rebuild,bootstrap,decommission"})
        cluster_topology = generate_cluster_topology(rack_num=3)
        cluster.populate(cluster_topology).start(wait_other_notice=True)

        node1, node2, node3 = cluster.nodelist()

        t = DecommissionInParallel(node1)
        t.start()

        self.ignore_log_patterns += [
            r"decommission.*failed",
            r"raft_topology - Decommission failed\. See earlier errors",
            r"raft_topology - .* failed with seastar::rpc::closed_error[ :]+\(?connection is closed\)?",
            r"raft::request_aborted[ :]+\(?Request is aborted by a caller\)?",
        ]

        counter = 0
        while t.is_alive():
            counter += 1
            out = self.show_status(node2)
            if " null " in out:
                logger.debug("Matched null status entry")
                break
            logger.debug("Restarting node2")
            node2.stop(gently=False)
            if counter < 10:
                node2.start(wait_for_binary_proto=True, wait_other_notice=False)
            else:
                logger.debug("Waiting for decommission failed")
                #  after 10 kills,  wait for decommission to fail before restarting
                wait_for_nodes_status(node3, ["UN", "DN", "UN"])
                t.join(timeout=30)
                node2.start(wait_for_binary_proto=True, wait_other_notice=False)
                break
        else:
            logger.debug("Waiting for decommission to complete")
            t.join()
        self.show_status(node2)

        logger.debug("Sleeping for 30 seconds to allow gossip updates")
        time.sleep(30)
        out = self.show_status(node2)
        assert " null " not in out

    def show_status(self, node):
        out, _err = node.nodetool("status")
        logger.debug(f"Status as reported by node {node.address()}")
        logger.debug(out)
        return out

    @unmark.next_gating
    def test_remove_node_alive(self):
        cluster_topology = generate_cluster_topology(rack_num=3)
        self.prepare_cluster(cluster_topology, rf=3)
        remove_node = self.cluster.nodelist()[-1]
        removenode_hostid = remove_node.hostid()
        node: ScyllaNode = self.cluster.nodelist()[0]

        mark = node.mark_log()
        with pytest.raises(NodetoolError) as nodetool_exc:
            logger.debug(f"Remove node {remove_node.name} (host id {removenode_hostid}) which is alive")
            node.removenode(removenode_hostid)
            if not re.match(self.REMOVENODE_REJECT_MSG, nodetool_exc):
                raise nodetool_exc

        found_messages = node.grep_log(expr=self.REMOVENODE_REJECT_MSG, from_mark=mark)
        if not found_messages:
            raise Exception("Removenode reject message was not found in logs")

    def test_remove_node_alive_in_gossip(self):
        cluster_topology = generate_cluster_topology(rack_num=3)
        self.prepare_cluster(cluster_topology, rf=3)
        remove_node = self.cluster.nodelist()[-1]
        removenode_hostid = remove_node.hostid()
        node: ScyllaNode = self.cluster.nodelist()[0]

        mark = node.mark_log()
        logger.debug(f"Stopping node {remove_node.name} (host id {removenode_hostid}) so node stay in gossip alive")
        remove_node.stop(gently=False, wait_other_notice=False)

        with pytest.raises(NodetoolError) as nodetool_exc:
            logger.debug(f"Remove node {remove_node.name} (host id {removenode_hostid}) which is alive")
            node.removenode(removenode_hostid)
            if not re.match(self.REMOVENODE_REJECT_MSG, nodetool_exc):
                logger.debug(f"Nodetool failed with error: {nodetool_exc}")
                raise nodetool_exc

        found_messages = node.grep_log(expr=self.REMOVENODE_REJECT_MSG, from_mark=mark)
        if not found_messages:
            raise Exception("Removenode reject message was not found in logs")

    def test_removenode_rejected_before_decommision_node(self):
        cluster_topology = generate_cluster_topology(rack_num=2, nodes_per_rack=2)
        # tablets enforces the RF constraints, so add one more node to the
        # cluster, so that we are allowed to decommission a node in it even if
        # tablets is enabled.
        self.prepare_cluster(cluster_topology, rf=2)
        remove_node: ScyllaNode = self.cluster.nodelist()[-1]
        removenode_hostid = remove_node.hostid()
        node: ScyllaNode = self.cluster.nodelist()[0]

        mark = node.mark_log()
        # removenode operation should fail with error
        with pytest.raises(NodetoolError) as nodetool_exc:
            logger.debug(f"Remove node {remove_node.name} (host id {removenode_hostid}) which is alive")
            node.removenode(removenode_hostid)
            if not re.match(self.REMOVENODE_REJECT_MSG, nodetool_exc):
                logger.debug(f"Nodetool failed with error: {nodetool_exc}")
                raise nodetool_exc

        found_messages = node.grep_log(expr=self.REMOVENODE_REJECT_MSG, from_mark=mark)
        if not found_messages:
            raise Exception("Removenode error message was not found in logs")

        logger.debug(f"Decommission node {remove_node.name}")
        remove_node.decommission()
        with pytest.raises(NodetoolError) as nodetool_exc:
            logger.debug(f"Remove node {remove_node.name} (host id {removenode_hostid}) which was decommissioned")
            node.removenode(removenode_hostid)
            if not re.match(self.REMOVENODE_HOSTID_NOT_IN_CLUSTER, nodetool_exc):
                logger.debug(f"Nodetool failed with error: {nodetool_exc}")
                raise nodetool_exc


class DecommissionInParallel(Thread):
    def __init__(self, node):
        Thread.__init__(self)
        self.node = node

    def run(self):
        node = self.node
        mark = node.mark_log()
        try:
            out, err = node.nodetool("decommission")
            node.watch_log_for("DECOMMISSIONED", from_mark=mark)
            logger.debug(out)
            logger.debug(err)
        except NodetoolError as e:
            logger.debug("Decommission failed with exception: " + str(e))
