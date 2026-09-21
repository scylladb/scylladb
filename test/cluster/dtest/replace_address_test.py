import datetime
import logging
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from time import sleep

import pytest
from cassandra import (
    ConsistencyLevel,
    OperationTimedOut,
    ReadFailure,
    ReadTimeout,
    Unavailable,
)
from cassandra.query import SimpleStatement
from ccmlib.node import NodeError
from ccmlib.scylla_cluster import ScyllaCluster

from bootstrap_test import bootstrap_done_log_pat, bootstrap_start_log_pat
from dtest_class import Tester, create_cf, create_ks, wait_for
from dtest_setup import DTestSetup
from dtest_setup_overrides import DTestSetupOverrides
from tools.assertions import (
    assert_all,
    assert_lists_equal_ignoring_order,
    assert_row_count,
)
from tools.cluster_topology import generate_cluster_topology
from tools.data import insert_c1c2, rows_to_list
from tools.marks import issue_open, unmark_if, with_feature
from tools.metrics import get_node_metrics
from tools.misc import ImmutableMapping
from tools.stress import assert_cs_success, create_stress_compatible_table, format_cs_output


class NodeUnavailableError(Exception):
    pass


logger = logging.getLogger(__name__)


@pytest.mark.next_gating
@pytest.mark.dtest_full
@pytest.mark.parametrize("rbo_status", [True, False], ids=["rbo_enabled", "rbo_disabled"])
class TestReplaceAddress(Tester):
    rbo_enabled: bool

    @pytest.fixture(scope="function", autouse=True)
    def fixture_dtest_setup_overrides(self, dtest_config, rbo_status):
        dtest_setup_overrides = DTestSetupOverrides()
        dtest_setup_overrides.cluster_options = ImmutableMapping({"start_rpc": "true"})
        self.rbo_enabled = rbo_status
        return dtest_setup_overrides

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup: DTestSetup):
        fixture_dtest_setup.ignore_log_patterns += [
            # This one occurs when trying to send the migration to a
            # node that hasn't started yet, and when it does, it gets
            # replayed and everything is fine.
            r"Can\'t send migration request: node.*is down",
            # This is caused by starting a node improperly (replacing active/nonexistent)
            r"Exception encountered during startup",
            # This is caused by trying to replace a nonexistent node
            r"Exception in thread Thread",
            # ignore streaming error during bootstrap
            r"Streaming error occurred",
        ]

    def init_cluster(self, num_nodes=3, configuration_options=None):
        configuration_options = configuration_options or {}
        rbo_status = "true" if self.rbo_enabled else "false"
        configuration_options.update({"enable_repair_based_node_ops": rbo_status})
        logger.debug(f"Setting cluster configuration options: {configuration_options}")
        self.debug_mode = isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode == "debug"
        self.cluster.populate(num_nodes)
        self.cluster.set_configuration_options(values=configuration_options)
        self.cluster.start(no_wait=False, wait_for_binary_proto=True, wait_other_notice=True)

    def get_sorted_tokens(self, node, address=None, nodetool_ring_params=None):
        # sorted([line.split()[-1] for line in node3.nodetool('ring')[0].splitlines()
        #         if node3.address() in line])

        if nodetool_ring_params is None:
            nodetool_ring_params = []
        if address is None:
            address = node.address()
        ring_lines = [line for line in node.nodetool(" ".join(["ring ", *nodetool_ring_params]))[0].splitlines() if address in line]
        tokens_list = [token.split()[-1] for token in ring_lines]
        return sorted(tokens_list)

    @pytest.mark.unmark_if("next_gating", condition=issue_open("https://github.com/scylladb/scylladb/issues/15786"))  # cause of https://github.com/scylladb/scylladb/issues/15786
    @pytest.mark.use_cassandra_stress
    def test_replace_stopped_node(self):
        """
        Test that we can replace a node that is not shutdown gracefully.
        """
        self._replace_node_test(gently=False)

    @pytest.mark.unmark_if("next_gating", condition=issue_open("https://github.com/scylladb/scylladb/issues/15602"))
    @pytest.mark.use_cassandra_stress
    def test_replace_shutdown_node(self):
        """
        @jira_ticket CASSANDRA-9871
        Test that we can replace a node that is shutdown gracefully.
        """
        self._replace_node_test(gently=True)

    def _replace_node_test(self, gently: bool):
        """
        Check that the replace address function correctly replaces a node that has failed in a cluster.
        Create a cluster, cause a node to fail, and bring up a new node with the replace-node-first-boot parameter.
        Check that tokens are migrated and that data is replicated properly.
        """
        logger.info("Starting cluster with 3 nodes.")
        self.init_cluster(num_nodes=generate_cluster_topology(rack_num=3, dc_name_prefix="dc", rack_name_prefix="r"))
        node1, node2, node3 = self.cluster.nodelist()

        tokens = self.get_sorted_tokens(node3)

        logger.info(len(tokens))

        logger.info("Inserting Data...")
        node1.stress(["write", "n=10000", "-schema", "replication(factor=3)"])

        session = self.patient_cql_connection(node1)
        stress_table = "keyspace1.standard1"
        query = SimpleStatement("select * from %s LIMIT 1" % stress_table, consistency_level=ConsistencyLevel.THREE)
        initialData = list(session.execute(query))

        # stop node, query should not work with consistency 3
        logger.info("Stopping node 3.")
        node3.stop(gently=gently, wait_other_notice=True)

        logger.info("Testing node stoppage (query should fail).")
        with pytest.raises(expected_exception=(Unavailable, ReadTimeout, ReadFailure, OperationTimedOut)):
            query = SimpleStatement("select * from %s LIMIT 1" % stress_table, consistency_level=ConsistencyLevel.THREE)
            session.execute(query)

        # replace node 3 with node 4
        logger.info("Starting node 4 to replace node 3")

        node4 = self.cluster.new_node(4, auto_bootstrap=True, is_seed=False, data_center="dc1", rack="r3")
        node4.start(replace_node_host_id=node3.hostid(), wait_for_binary_proto=True)

        # query should work again
        logger.info("Verifying querying works again.")
        query = SimpleStatement("select * from %s LIMIT 1" % stress_table, consistency_level=ConsistencyLevel.THREE)
        finalData = list(session.execute(query))
        assert_lists_equal_ignoring_order(initialData, finalData)

        logger.info("Verifying tokens migrated successfully")
        moved_tokens_list = self.get_sorted_tokens(node4)
        logger.info(len(moved_tokens_list))
        assert moved_tokens_list == tokens
        assert self.get_sorted_tokens(node1, node3.address()) == []

        logger.info("Starting node 3 and verifying that it is not listening")
        node3.start(no_wait=True)
        wait_for(lambda: node3.hostid() is not None, timeout=60, step=2)

        for node in (node1, node2, node4):
            status = node.nodetool("status")
            assert node3.hostid() not in status
            assert node3.address() not in status

    @pytest.mark.no_boot_speedups
    def test_replace_node_using_the_same_ip_then_shut_down(self):
        self._template_replace_node_then_shut_down(use_same_ip=True)
        # see https://github.com/scylladb/scylladb/issues/15713
        expected_error = "raft::transport_error"
        self.ignore_log_patterns.append(expected_error)

    def test_replace_node_using_new_ip_then_shut_down(self):
        self._template_replace_node_then_shut_down(use_same_ip=False)

    def _template_replace_node_then_shut_down(self, use_same_ip):
        executor = ThreadPoolExecutor(max_workers=1)
        consistency_level_key = "QUORUM"
        # SCYLLADB-2753: Use a short stress duration since we only need to
        # verify writes survive the node4 shutdown, not the entire replacement
        # bootstrap.  The previous 6-minute fixed-duration stress was racy:
        # in debug+tablets builds, replacement streaming can exceed 20 minutes,
        # causing stress to expire while only 2 of 3 replicas are alive; a
        # single CI-induced reactor stall then breaks QUORUM.
        stress_duration_minutes = 1 if self.cluster.scylla_mode != "debug" else 2
        replication_factor = 3
        ks_name = "keyspace2"
        write_stress_cmd = [
            "write",
            f"cl={consistency_level_key}",
            f"duration={stress_duration_minutes}m",
            "-rate",
            "threads=10 throttle=1000/s",
            "-log",
            "interval=5",
            "-schema",
            f"replication(factor={replication_factor}) keyspace={ks_name}",
        ]

        logger.info("Starting cluster with 3 nodes.")

        self.init_cluster(generate_cluster_topology(rack_num=2, dc_name_prefix="dc", rack_name_prefix="r"))

        node1, _ = self.cluster.nodelist()
        # Adding the node separately so it won't be a seed node, since you can't replace a seed node in this matter
        node3 = self.cluster.new_node(3, data_center="dc1", rack="r3", is_seed=False)
        node3.start(wait_for_binary_proto=True)

        logger.info("Stopping node 3.")
        node3_hostid = node3.hostid()
        node3.stop(gently=False, wait_other_notice=True)

        # replace node 3 with node 4
        logger.info("Starting node 4 to replace node 3")

        node4 = self.cluster.new_node(4, data_center="dc1", rack="r3", is_seed=False)
        replace_node_host_id = node3_hostid
        if use_same_ip:
            node3_address = node3.address()
            node4.set_configuration_options(values={"listen_address": node3_address, "rpc_address": node3_address, "api_address": node3_address})
            node4.network_interfaces = {k: (node3_address, v[1]) for k, v in node4.network_interfaces.items()}
            logger.debug(f"Start node4 again with ip address {node3_address}")
        else:
            logger.debug("Start node4 with new ip address")

        # SCYLLADB-2753: Wait for node4 to fully complete replacement bootstrap
        # before starting the QUORUM stress.  In debug+tablets builds,
        # replacement streaming can exceed 20 minutes; wait_for_binary_proto
        # handles this (ccm allows up to ~45 min in debug mode via retry logic).
        node4.start(replace_node_host_id=replace_node_host_id, wait_for_binary_proto=True)

        # SCYLLADB-2753: Launch stress AFTER node4 is NORMAL so that QUORUM is
        # guaranteed satisfiable: all 3 replicas (node1, node2, node4) are
        # serving.  When node4 is subsequently stopped, 2 of 3 replicas remain
        # and QUORUM (needs ceil((3+1)/2) = 2) is still met.
        logger.debug("starting stress after replacement is complete")
        stress_thread = executor.submit(lambda: node1.stress(stress_options=write_stress_cmd))

        node4.stop(gently=False, wait_other_notice=True)
        results = stress_thread.result()
        logger.debug(format_cs_output(results))
        assert_cs_success(results)

    @pytest.mark.skip_if(with_feature("tablets"))
    @pytest.mark.no_boot_speedups
    def test_serve_writes_during_bootstrap(self):
        """
        When replacing a node, the new node should serve writes while data is streamed into it, ensuring that when
        the operation completes it will have up-to-date data.
        """
        logger.info("Starting cluster with 3 nodes.")
        self.init_cluster(num_nodes=3)
        node1, node2, node3 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)

        keyspace_name = "ks"
        table_name = "cf"

        create_ks(session, keyspace_name, rf=3)
        session.execute(f"USE {keyspace_name}")

        session.execute(f"CREATE TABLE {table_name} (pk int, ck int, v int, primary key (pk, ck))")

        keys = 30 if self.debug_mode else 300
        rows = 500
        total_rows = keys * rows
        logger.debug(f"Insert {keys} partitions of {rows} rows (total {total_rows} rows).")
        insert_stmt = session.prepare(f"INSERT INTO {table_name} (pk, ck, v) VALUES (?, ?, ?)")
        data = []
        for i in range(keys):
            for k in range(rows):
                data.append([i, k, k])
                session.execute(insert_stmt, (i, k, k))

        logger.info("Flush cluster")
        self.cluster.flush()

        tokens = self.get_sorted_tokens(node3)

        assert_row_count(session, table_name, total_rows)

        # stop node
        logger.info("Stopping node 3.")
        node3.stop(gently=True, wait_other_notice=True)

        # replace node 3 with node 4
        logger.info("Starting node 4 to replace node 3")
        node4 = self.cluster.new_node(4, auto_bootstrap=True, is_seed=False)
        node4.start(replace_node_host_id=node3.hostid(), no_wait=True, jvm_args=["--logger-log-level", "stream_session=debug"])

        log_timeout = 600
        if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode == "debug":
            log_timeout *= 3

        node4.watch_log_for([bootstrap_start_log_pat, "Beginning stream session|sync data for keyspace=ks, status=started"], timeout=log_timeout)

        logger.debug("Insert 1000 rows more.")
        for i in range(keys, keys + 10):
            for k in range(rows, rows + 100):
                data.append([i, k, k])
                session.execute(insert_stmt, (i, k, k))

        assert_row_count(session, table_name, total_rows + 1000, consistency_level=ConsistencyLevel.QUORUM)

        logger.info("Waiting for node4 is up")
        node4.watch_log_for("initialization completed")

        logger.info("Verifying tokens migrated successfully")
        moved_tokens_list = self.get_sorted_tokens(node4)
        assert moved_tokens_list == tokens
        assert self.get_sorted_tokens(node1, node3.address()) == []

        # stop all nodes except new one
        logger.info("Stopping nodes 1 and 2")
        for node in [node1, node2]:
            node.stop(gently=True, wait_other_notice=True)

        # validate data
        node4.flush()
        session = self.patient_cql_connection(node4)
        session.execute(f"USE {keyspace_name}")
        assert_row_count(session, table_name, total_rows + 1000)
        assert_all(session, f"select * from {table_name}", data, ignore_order=True)

    def test_shutdown_all_and_replace_node(self):
        logger.info("Starting cluster with 3 nodes.")
        self.init_cluster(num_nodes=3)
        node1, node2, node3 = self.cluster.nodelist()

        self.cluster.stop_nodes([node1, node2, node3])
        self.cluster.start_nodes([node1, node2], wait_for_binary_proto=True)

        logger.info("Starting node 4 to replace node 3")
        node4 = self.cluster.new_node(4, auto_bootstrap=True, is_seed=False)

        node4.start(wait_for_binary_proto=True, replace_node_host_id=node3.hostid())

    @pytest.mark.dtest_debug
    def test_replace_active_node(self):
        logger.info("Starting cluster with 3 nodes.")
        self.init_cluster(num_nodes=3)
        _node1, _node2, node3 = self.cluster.nodelist()

        # replace active node 3 with node 4
        logger.info("Starting node 4 to replace active node 3")
        node4 = self.cluster.new_node(4, auto_bootstrap=True, is_seed=False)

        if "consistent-topology-changes" in self.scylla_features:
            expected_message = f"tried to replace alive node {node3.hostid()}"
        else:
            expected_message = "Cannot replace a live node"

        self.ignore_log_patterns += [expected_message]

        mark = node4.mark_log()
        node4.start(replace_node_host_id=node3.hostid(), no_wait=True)
        node4.watch_log_for(expected_message, from_mark=mark)
        self.check_not_running(node4)

    def test_replace_nonexistent_node(self):
        logger.info("Starting cluster with 3 nodes.")
        self.init_cluster(num_nodes=3)

        logger.info("Start node 4 and replace an address with no node")
        node4 = self.cluster.new_node(4, auto_bootstrap=True, is_seed=False)

        replace_node_host_id = str(uuid.uuid4())
        expected_message = f"Replaced node with Host ID {replace_node_host_id} not found"

        self.ignore_log_patterns += [expected_message]

        # try to replace an unassigned ip address
        mark = node4.mark_log()
        try:
            node4.start(replace_node_host_id=replace_node_host_id, no_wait=True)
        except NodeError:
            pass  # node doesn't start as expected
        node4.watch_log_for(expected_message, from_mark=mark)
        self.check_not_running(node4)

    def check_not_running(self, node):
        attempts = 0
        while node.is_running() and attempts < 10:
            sleep(1)
            attempts = attempts + 1

        assert not node.is_running()

    def test_replace_first_boot(self):
        logger.info("Starting cluster with 3 nodes.")
        self.init_cluster(num_nodes=generate_cluster_topology(rack_num=3, dc_name_prefix="dc", rack_name_prefix="r"), configuration_options={"range_request_timeout_in_ms": 10000})
        node1, _node2, node3 = self.cluster.nodelist()

        tokens = self.get_sorted_tokens(node3)

        logger.info(len(tokens))

        logger.info("Inserting Data...")
        node1.stress(["write", "n=10000", "-schema", "replication(factor=3)"])

        session = self.patient_cql_connection(node1)
        stress_table = "keyspace1.standard1"
        query = SimpleStatement("select * from %s LIMIT 1" % stress_table, consistency_level=ConsistencyLevel.THREE)
        initialData = list(session.execute(query))

        # stop node, query should not work with consistency 3
        logger.info("Stopping node 3.")
        node3.stop(wait_other_notice=True)

        logger.info("Testing node stoppage (query should fail).")
        with pytest.raises(expected_exception=(Unavailable, ReadTimeout, ReadFailure, OperationTimedOut)):
            session.execute(query, timeout=30)

        # replace node 3 with node 4
        logger.info("Starting node 4 to replace node 3")
        node4 = self.cluster.new_node(4, auto_bootstrap=True, is_seed=False, data_center="dc1", rack="r3")
        node4.start(replace_node_host_id=node3.hostid(), wait_for_binary_proto=True)

        # query should work again
        logger.info("Verifying querying works again.")
        finalData = list(session.execute(query))
        assert_lists_equal_ignoring_order(initialData, finalData)

        logger.info("Verifying tokens migrated successfully")
        moved_tokens_list = self.get_sorted_tokens(node4)
        logger.info(len(moved_tokens_list))
        assert moved_tokens_list == tokens
        assert self.get_sorted_tokens(node1, node3.address()) == []

        logger.info("Try to restart node 3 (should fail)")
        node3.start(no_wait=True)
        self.check_not_running(node3)

        # Wait for the other nodes to observe both sides of the restart so the
        # checks below do not race with gossip state convergence.
        node4.stop(gently=False, wait_other_notice=True)
        node4.start(wait_for_binary_proto=True, wait_other_notice=True)

        logger.info("Verifying querying works again.")
        finalData = list(session.execute(query))
        assert_lists_equal_ignoring_order(initialData, finalData)

        # we redo this check because restarting node should not result in tokens being moved again.
        # ie tokens should be same
        logger.info("Verifying tokens migrated successfully")
        moved_tokens_list = self.get_sorted_tokens(node4)
        logger.info(len(moved_tokens_list))
        assert moved_tokens_list == tokens
        assert self.get_sorted_tokens(node1, node3.address()) == []

    @pytest.mark.skip_if(issue_open("scylladb/scylladb#21718"))
    def test_replace_node_no_hibernate_state(self):  # noqa: PLR0915
        """Test that there is no HIBERNATE status for a replacing node.

        See https://github.com/scylladb/scylla/issues/5449 for details.
        """
        logger.info("Starting cluster with 3 nodes.")
        self.init_cluster({"dc1": {"r1": 2, "r2": 1}})
        node1, node2, node3 = self.cluster.nodelist()
        logger.info(f"Node 1 address is {self.cluster.get_node_ip(1)}")

        node2_address = self.cluster.get_node_ip(2)
        logger.info(f"Node 2 address is {node2_address}")

        tokens = self.get_sorted_tokens(node2)
        logger.info(f"Detected number of tokens: {len(tokens)}")

        node3_address = self.cluster.get_node_ip(3)
        logger.info(f"Node 3 address is {node3_address}")

        logger.info("Inserting Data...")
        node1.stress(["write", "n=10000", "-schema", "replication(factor=2)"])

        session = self.patient_cql_connection(node1)
        stress_table = "keyspace1.standard1"
        query = SimpleStatement(f"SELECT * FROM {stress_table} LIMIT 1", consistency_level=ConsistencyLevel.ALL)
        initial_data = list(session.execute(query))

        logger.info("Stopping node 2.")
        node2.stop()

        logger.info("Starting node 4 to replace node 2, but stop it in the middle of the replace.")
        node4 = self.cluster.new_node(4, data_center="dc1", rack="r1", auto_bootstrap=True, is_seed=False)
        node4.start(replace_node_host_id=node2.hostid(), no_wait=True)

        # this error is expected in teardown after this test in raft topology mode
        ignore_error = rf"raft_topology - raft_topology_cmd.*failed with: (?:service::wait_for_ip_timeout \(failed to obtain an IP for {node4.hostid()} in 30s\)|failed to obtain an IP for {node4.hostid()} in 30s)"
        self.ignore_log_patterns += [ignore_error]

        node4_address = self.cluster.get_node_ip(4)
        logger.info(f"Node 4 is {node4_address}")

        self.ignore_log_patterns += ["Startup failed"]
        node4.stop()

        status1, _err1 = node1.nodetool("gossipinfo")
        logger.info(f"gossipinfo:\n{status1}")
        assert "STATUS:hibernate,true" not in status1, "There is a node in HIBERNATE status."

        logger.info("Starting node 5 to replace node 2.")
        node5 = self.cluster.new_node(5, data_center="dc1", rack="r1", auto_bootstrap=True, is_seed=False)
        node5.start(replace_node_host_id=node2.hostid(), wait_for_binary_proto=True, wait_other_notice=True)

        node5_address = self.cluster.get_node_ip(5)
        node5_hostid = node5.hostid()
        logger.info(f"Node 5 is {node5_address}/{node5_hostid}")

        status2, _err2 = node1.nodetool("gossipinfo")
        logger.info(f"gossipinfo:\n{status2}")
        assert "STATUS:hibernate,true" not in status2, "There is a node in HIBERNATE status."
        assert f"/{node4_address}\n" not in status2, "Node 4 stays in gossip."

        logger.info("Verifying querying works.")
        final_data = list(session.execute(query))
        assert_lists_equal_ignoring_order(initial_data, final_data)

        logger.info("Verifying tokens migrated successfully.")
        moved_tokens_list = self.get_sorted_tokens(node5)
        logger.info(len(moved_tokens_list))
        assert moved_tokens_list == tokens
        assert self.get_sorted_tokens(node1, node2.address()) == []

        logger.info("Verifying system.peers table.")
        expected_peers = [(node3.address(), node3.hostid(), 256), (node5_address, node5_hostid, 256)]
        session = self.exclusive_cql_connection(node1)
        res = session.execute("SELECT peer, host_id, tokens FROM system.peers")
        peers = [(peer, str(host_id), len(tokens)) for peer, host_id, tokens in sorted(rows_to_list(res))]
        assert peers == expected_peers, f"Unexpected peers result. Expected {expected_peers}, got {peers}"

    @unmark_if("next_gating", condition=issue_open("scylladb/scylladb#19645"))
    def test_replace_with_background_workload(self):
        """
        The subtest is used to reproduce https://github.com/scylladb/scylla/issues/4705
        the background write workload continue running more than 30 seconds,
        the gossiper reached a timeout, and nodes raise 'unknown endpoint' error.

        The cluster has two nodes in rack r3 so that one r3 node remains in
        node_state::normal while the other is being replaced. With
        rf_rack_valid_keyspaces=True (the dtest default), get_allowed_racks
        and the tablet allocator filter racks by node::is_normal(), which is
        false for both being_replaced and replacing nodes. With a single node
        per rack, the replace window transiently drops the visible rack count
        for dc1 from 3 to 2, and the workload's recurring CREATE KEYSPACE IF
        NOT EXISTS keyspace1 ... RF=3 issued by cassandra-stress fails with
        'Replication factor 3 exceeds the number of racks (2) in dc dc1'.
        Keeping a second normal node in r3 throughout the replace avoids that
        race; the test still exercises the original gossip 'unknown endpoint'
        scenario from scylladb/scylla#4705.
        """
        self.init_cluster({"dc1": {"r1": 1, "r2": 1, "r3": 2}}, configuration_options={"failure_detector_timeout_in_ms": 5000})

        node1 = self.cluster.nodelist()[0]
        node3 = self.cluster.nodelist()[2]
        logger.info(node1.nodetool("status")[0])

        logger.info("Create cassandra-stress schema before starting the background workload")
        create_stress_compatible_table(self, node1, rf=3)

        enable_nodetool_debug = False

        def nodetool_thread():
            logger.info("nodetool thread")
            for key in range(20):
                logger.info(f"enable_nodetool_debug: {enable_nodetool_debug}")
                logger.info(node1.nodetool("status")[0])
                logger.info(node1.nodetool("gossipinfo", True)[0])
            logger.info("nodetool thread: completed")

        # node3 is killed and replaced during the test. Whitelist the nodes that
        # stay alive for the whole test to prevent cassandra-stress from
        # potentially getting stuck due to a bug in the Java driver.
        # For more details, see: https://github.com/scylladb/java-driver/issues/920.
        stress_node_whitelist = ",".join(node.address() for node in self.cluster.nodelist() if node is not node3)

        def workload_thread():
            def run_stress(duration):
                timeout_grace = 60
                node1.stress(
                    stress_options=["write", f"duration={duration}s", "no-warmup", "cl=QUORUM", "-rate", "threads=2 throttle=500/s", "-schema", "replication(factor=3)", "-pop", "seq=1..1000", "-node", "whitelist", stress_node_whitelist],
                    timeout=duration + timeout_grace,
                )

            logger.info("workload thread: start")
            run_stress(100)
            while not self.replace_done_time or (datetime.datetime.now() - self.replace_done_time).total_seconds() < 30:
                logger.debug("Rerunning stress to continue running more than 30 seconds after new node is added")
                run_stress(30)
            logger.info("workload thread: completed")

        executor = ThreadPoolExecutor(max_workers=2)
        cs_thread = executor.submit(workload_thread)

        if enable_nodetool_debug:
            nodetool_thread = executor.submit(nodetool_thread)

        logger.info("Wait for the background workload to start")
        with self.patient_cql_connection(node1) as session:

            def check_workload_started():
                return session.execute("SELECT * FROM keyspace1.standard1 LIMIT 1").one() is not None

            # The cassandra-stress docker container itself may take up to 60s just to reach
            # "running" (see tools/stress_thread_docker.py), so this wait needs double that budget.
            wait_for(check_workload_started, timeout=120, text="Waiting for background workload to write its first row")

        logger.info("Start to kill node3 ...")
        logger.debug("Kill node and wait while other nodes mark it down")
        node3.stop(gently=False, wait_other_notice=True)
        logger.info("node3 has been killed")

        logger.info("Add a new node to replace the dead node")
        self.replace_done_time = None
        added_node = self.cluster.new_node(5, data_center="dc1", rack="r3", is_seed=False)
        added_node.start(replace_node_host_id=node3.hostid(), wait_for_binary_proto=True)
        logger.info("Successfully add a new node to replace node3")
        self.replace_done_time = datetime.datetime.now()

        cs_thread.result(timeout=300)
        if enable_nodetool_debug:
            nodetool_thread.result(timeout=300)

        for node in self.cluster.nodelist():
            err_log = node.grep_log("unknown endpoint")[0:3]
            logger.info(f"{node.name}: {err_log}")
            assert not err_log

    def test_replace_stopped_node_with_schema_rf_1(self):
        """
        Test that we can replace a node that is not shutdown gracefully
        and schema have replication factor equal 1

        """
        self.replace_node_with_schema_rf_1(gently=False)

    def test_replace_shutdown_node_with_schema_rf_1(self):
        """
        Test that we can replace a node that is shutdown gracefully
        and schema have replication factor equal 1
        """
        self.replace_node_with_schema_rf_1(gently=True)

    def replace_node_with_schema_rf_1(self, gently: bool):
        self.init_cluster(num_nodes=3)
        node1, _node2, node3 = self.cluster.nodelist()

        node3_tokens = self.get_sorted_tokens(node3)

        logger.info("Inserting Data...")
        node1.stress(["write", "n=10000", "-schema", "replication(factor=1)"])

        logger.info("Stopping node 3.")
        node3.stop(gently=gently, wait_other_notice=True)

        # replace node 3 with node 4
        logger.info("Starting node 4 to replace node 3")
        node4 = self.cluster.new_node(4, auto_bootstrap=True, is_seed=False)
        node4.start(replace_node_host_id=node3.hostid(), wait_for_binary_proto=True)
        if not self.rbo_enabled:
            node4.watch_log_for([r"WARN .* Unable to find sufficient sources to stream range .* for keyspace .* with RF = 1 for replace operation"])
        else:
            logger.info("Issue #6351 is not related to scylla with repair based operations enabled")

        logger.info("Verifying tokens migrated successfully")
        moved_tokens_list = self.get_sorted_tokens(node4)

        assert moved_tokens_list == node3_tokens, "Tokens were not moved correctly to node4"
        assert self.get_sorted_tokens(node1, node3.address()) == []
        assert node4.is_live(), "Node4 is not alive after node4 has replaced node3"

    @pytest.mark.skip_if(with_feature("tablets"))
    def test_replace_node_diff_ip(self):
        logger.info("Starting cluster with 5 nodes.")
        cluster = self.cluster
        cluster.populate(5).start(wait_for_binary_proto=True)
        node1, node2, node3, node4, node5 = cluster.nodelist()

        ks, cf = "ks", "cf"
        session = self.patient_cql_connection(node5)
        create_ks(session, ks, 3)
        create_cf(session, cf, read_repair=0.0, columns={"c1": "text", "c2": "text"})
        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ALL)

        ring_params = "" if "tablets" not in self.scylla_features else [ks, cf]
        node5_tokens = self.get_sorted_tokens(node5, nodetool_ring_params=ring_params)
        node5.stop()

        logger.info("Starting node 6 to replace node 5")
        node6 = cluster.new_node(6, auto_bootstrap=True, is_seed=False)
        node6.start(wait_for_binary_proto=True, replace_node_host_id=node5.hostid())

        if "consistent-topology-changes" in self.scylla_features:
            log_message_to_wait = f"gossip - Finished to force remove node ({node5.address()}|{node5.hostid()})"
        else:
            log_message_to_wait = f"FatClient .*({node5.address()}|{node5.hostid()}) has been silent for .*ms, removing from gossip"
        for node in [node1, node2, node3, node4, node6]:
            node.watch_log_for(log_message_to_wait)

        logger.info("Verifying tokens migrated successfully")
        moved_tokens_list = self.get_sorted_tokens(node6, nodetool_ring_params=ring_params)
        assert moved_tokens_list == node5_tokens
        assert self.get_sorted_tokens(node1, node5.address(), ring_params) == []

    def test_replace_node_same_ip(self, fixture_dtest_setup: DTestSetup):
        logger.info("Starting cluster with 5 nodes.")
        cluster = self.cluster
        cluster.populate({"dc1": {"r1": 2, "r2": 2, "r3": 1}}).start(wait_for_binary_proto=True)
        node1, _node2, _node3, _node4, node5 = cluster.nodelist()

        session = self.patient_cql_connection(node5)
        create_ks(session, "ks", 3)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})
        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ALL)

        node5.stop()

        logger.info("Starting node 5 to replace node 5")
        node5.clear()
        jvm_args = ["--auto-bootstrap", "true", "--seed-provider-parameters", f"seeds={node1.address()}"]
        node5.start(wait_for_binary_proto=True, replace_node_host_id=node5.hostid(), jvm_args=jvm_args)

        # Sometimes we got closed_error for the old RPC client for the old node5.  Just ignore it.
        fixture_dtest_setup.ignore_log_patterns.append(r".*seastar::rpc::closed_error[ :]+\(?connection is closed\)?.*")

    @pytest.mark.skip_if(with_feature("tablets"))
    @pytest.mark.dtest_heavy
    def test_replace_node_diff_ip_take_write(self):  # noqa: PLR0915
        logger.info("Starting cluster with 5 nodes.")
        cluster = self.cluster
        cluster.populate(5).start(wait_for_binary_proto=True)
        node1, node2, node3, node4, node5 = cluster.nodelist()

        session = self.patient_cql_connection(node5)
        create_ks(session, "ks", 3)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})
        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ALL)

        node5.stop()

        rounds_cnt = 100 if not hasattr(cluster, "scylla_mode") or cluster.scylla_mode != "debug" else 10
        keys_per_round = 1000
        writes_diff_cnt = rounds_cnt * keys_per_round / 400

        stop = threading.Event()

        def insert_data(session, rounds, keys_per_round, stop):
            session.execute("use ks;")
            logger.info(f"Started to write rounds={rounds}")
            for i in range(rounds):
                if stop.is_set():
                    logger.info("Write thread is stopped")
                    break
                start = i * keys_per_round
                end = start + keys_per_round
                if start % 100000 == 0:
                    logger.info(f"Writing keys start={start} , end={end}")
                insert_c1c2(session, range(start, end), consistency=ConsistencyLevel.QUORUM)
            logger.info(f"Finished to write rounds={rounds}")
            return rounds

        logger.info("Starting node 6 to replace node 5")
        node6 = cluster.new_node(6, auto_bootstrap=True, is_seed=False)
        node6.start(wait_for_binary_proto=False, replace_node_host_id=node5.hostid(), wait_other_notice=False)

        if "consistent-topology-changes" in self.scylla_features:
            node6.watch_log_for("raft_topology - start streaming")
            remove_node_confirm_message = f"gossip - Finished to force remove node ({node5.address()}|{node5.hostid()})"
        else:
            node6.watch_log_for("Wait until peer nodes know the bootstrap tokens of local node|Started replace operation|repair - replace_with_repair: started with keyspace")
            remove_node_confirm_message = f"FatClient .*({node5.address()}|{node5.hostid()}) has been silent for .*ms, removing from gossip"

        executor = ThreadPoolExecutor(max_workers=1)
        session = self.patient_cql_connection(node1)
        write_thread = executor.submit(insert_data, session, rounds_cnt, keys_per_round, stop)

        logger.info("Get metrics when other knows replacing node = HIBERNATE")
        metrics = ["scylla_database_total_writes", "scylla_database_total_reads"]
        writes_when_replace_ops_started = 0
        for node in [6, 1, 2, 3, 4]:
            node_metrics = get_node_metrics(node_ip=self.cluster.get_node_ip(node), metrics=metrics)
            logger.debug(f"scylla_database_total_writes: node{node}={node_metrics}")
            if node == 6:
                writes_when_replace_ops_started = node_metrics["scylla_database_total_writes"]
                logger.info(f"writes_when_replace_ops_started={writes_when_replace_ops_started}")

        node6.watch_log_for(bootstrap_done_log_pat)

        if "tablets" in self.scylla_features:
            node6.watch_log_for("Tablet rebuild with .* for keyspace=ks succeeded")

        logger.info("Get metrics when other knows replacing node = NORMAL")
        metrics = ["scylla_database_total_writes", "scylla_database_total_reads"]
        writes_when_replace_ops_done = 0
        for node in [6, 1, 2, 3, 4]:
            node_metrics = get_node_metrics(node_ip=self.cluster.get_node_ip(node), metrics=metrics)
            logger.debug(f"scylla_database_total_writes: node{node}={node_metrics}")
            if node == 6:
                writes_when_replace_ops_done = node_metrics["scylla_database_total_writes"]
                logger.info(f"writes_when_replace_ops_done={writes_when_replace_ops_done}")

        assert writes_when_replace_ops_done - writes_when_replace_ops_started > writes_diff_cnt

        stop.set()

        # Wait for node6 to finish the replace ops
        session = self.patient_cql_connection(node6)

        for node in [node1, node2, node3, node4, node6]:
            node.watch_log_for(remove_node_confirm_message)

        write_thread.result()

    @pytest.mark.skip_if(with_feature("tablets"))
    @pytest.mark.dtest_heavy
    def test_replace_node_same_ip_take_write(self):  # noqa: PLR0915
        logger.info("Starting cluster with 5 nodes.")
        cluster = self.cluster
        cluster.populate(5).start(wait_for_binary_proto=True)
        node1, _node2, _node3, _node4, node5 = cluster.nodelist()

        session = self.patient_cql_connection(node5)
        create_ks(session, "ks", 3)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})
        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ALL)

        node5.stop()
        mark = node5.mark_log()

        rounds_cnt = 100 if not hasattr(cluster, "scylla_mode") or cluster.scylla_mode != "debug" else 10
        keys_per_round = 1000
        writes_diff_cnt = rounds_cnt * keys_per_round / 800

        stop = threading.Event()

        def insert_data(session, rounds, keys_per_round, stop):
            session.execute("use ks;")
            logger.info(f"Started to write and read rounds={rounds}")
            for i in range(1, rounds + 1):
                if stop.is_set():
                    logger.info("Write thread is stopped")
                    break
                start = i * keys_per_round
                end = start + keys_per_round
                if start % 100000 == 0:
                    logger.info(f"Writing keys start={start} , end={end}")
                insert_c1c2(session, range(start, end), consistency=ConsistencyLevel.QUORUM)
            logger.info(f"Finished to write and read rounds={rounds}")
            return rounds

        logger.info("Starting node 5 to replace node 5")
        node5.clear()
        jvm_args = ["--auto-bootstrap", "true", "--seed-provider-parameters", f"seeds={node1.address()}"]
        node5.start(wait_for_binary_proto=False, wait_other_notice=False, replace_node_host_id=node5.hostid(), jvm_args=jvm_args)

        if "consistent-topology-changes" in self.scylla_features:
            node5.watch_log_for("raft_topology - start streaming", from_mark=mark)
        else:
            node5.watch_log_for("Wait until peer nodes know the bootstrap tokens of local node|Started replace operation", from_mark=mark)

        executor = ThreadPoolExecutor(max_workers=1)
        session = self.patient_cql_connection(node1)
        write_thread = executor.submit(insert_data, session, rounds_cnt, keys_per_round, stop)

        metrics = ["scylla_database_total_writes", "scylla_database_total_reads"]
        logger.info("Get metrics when other knows replacing node = HIBERNATE")
        writes_when_replace_ops_started = 0
        for node in [5, 4, 3, 2, 1]:
            node_metrics = get_node_metrics(node_ip=self.cluster.get_node_ip(node), metrics=metrics)
            logger.info(f"metrics: node{node}={node_metrics}")
            if node == 5:
                writes_when_replace_ops_started = node_metrics["scylla_database_total_writes"]
                logger.info(f"writes_when_replace_ops_started={writes_when_replace_ops_started}")

        node5.watch_log_for(bootstrap_done_log_pat, from_mark=mark)

        if "tablets" in self.scylla_features:
            node5.watch_log_for("Tablet rebuild with .* for keyspace=ks succeeded")

        logger.info("Get metrics when other knows replacing node = NORMAL")
        metrics = ["scylla_database_total_writes", "scylla_database_total_reads"]
        writes_when_replace_ops_done = 0
        for node in [5, 4, 3, 2, 1]:
            node_metrics = get_node_metrics(node_ip=self.cluster.get_node_ip(node), metrics=metrics)
            logger.info(f"metrics: node{node}={node_metrics}")
            if node == 5:
                writes_when_replace_ops_done = node_metrics["scylla_database_total_writes"]
                logger.info(f"writes_when_replace_ops_done={writes_when_replace_ops_done}")

        assert writes_when_replace_ops_done - writes_when_replace_ops_started > writes_diff_cnt

        stop.set()
        # Wait for node5 to finish the replace ops
        session = self.patient_cql_connection(node5)

        write_thread.result()
