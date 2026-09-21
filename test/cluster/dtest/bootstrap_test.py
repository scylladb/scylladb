import logging
import os
import random
import re
import subprocess
import tempfile
import time
from concurrent.futures.thread import ThreadPoolExecutor

import pytest
from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args
from ccmlib.node import NodeError, TimeoutError, ToolError
from ccmlib.scylla_node import ScyllaNode
from psutil import Process

from dtest_class import Tester, create_cf, create_ks, get_ip_from_node
from dtest_setup import DTestSetup
from dtest_setup_overrides import DTestSetupOverrides
from tools.assertions import assert_all, assert_almost_equal, assert_one
from tools.cluster import new_node
from tools.cluster_topology import generate_cluster_topology
from tools.data import create_c1c2_table, insert_c1c2, query_c1c2
from tools.files import wipe_node_data_directories
from tools.intervention import InterruptBootstrap, KillOnBootstrap
from tools.misc import ImmutableMapping
from tools.session import wait_reconnection
from tools.status import wait_for_nodes_status
from tools.stress import assert_cs_success, format_cs_output

logger = logging.getLogger(__name__)

bootstrap_start_log_pat = r"Starting to bootstrap|raft topology: start streaming|raft_topology - start streaming"
bootstrap_done_log_pat = r"Bootstrap completed!|raft topology: streaming completed|raft_topology - streaming completed"


@pytest.mark.dtest_full
class TestBootstrap(Tester):
    @pytest.fixture(scope="function", autouse=True)
    def fixture_dtest_setup_overrides(self, dtest_config):
        dtest_setup_overrides = DTestSetupOverrides()
        dtest_setup_overrides.cluster_options = ImmutableMapping({"start_rpc": "true"})
        return dtest_setup_overrides

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup: DTestSetup):
        fixture_dtest_setup.ignore_log_patterns += [
            # This one occurs when trying to send the migration to a
            # node that hasn't started yet, and when it does, it gets
            # replayed and everything is fine.
            r"Can\'t send migration request: node.*is down",
            # ignore streaming error during bootstrap
            r"Exception encountered during startup",
            r"Streaming error occurred",
        ]

    def get_space_used(self, node, table_name="cf"):
        output, *_ = node.nodetool("cfstats")
        if output.find(table_name) != -1:
            output = output[output.find(table_name) :]
            output = output[output.find("Space used (total)") :]
            initial_value = int(output[output.find(":") + 1 : output.find("\n")].strip())
            return initial_value
        return -1

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    @pytest.mark.dtest_smoke
    @pytest.mark.single_node
    def test_start_stop(self):
        logger.info("populating cluster with one node")
        cluster = self.cluster
        cluster.populate(1)
        logger.info("starting cluster")
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
        logger.info("stopping cluster")
        cluster.stop()
        logger.info("done")

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    @pytest.mark.dtest_smoke
    def test_start_stop_node(self):
        logger.info("populating cluster with three nodes")
        cluster = self.cluster
        cluster.populate(3)
        logger.info("starting cluster")
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
        logger.info("stopping node")
        node1 = cluster.nodelist()[0]
        node1.stop(wait_other_notice=True, wait_seconds=10)
        logger.info("stopping cluster")
        cluster.stop()
        logger.info("done")

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_add_node(self):
        logger.info("populating cluster with three nodes")
        cluster = self.cluster
        cluster.populate(2)
        logger.info("starting cluster")
        cluster.start(wait_other_notice=True)
        logger.info("adding node3")
        node3 = cluster.new_node(3)
        logger.info("starting node3")
        node3.start(wait_other_notice=True)
        logger.info("stopping cluster")
        cluster.stop()
        logger.info("done")

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_add_detached_node(self, request: pytest.FixtureRequest):
        logger.info("populating cluster with three nodes")
        cluster = self.cluster
        cluster.populate(2)
        logger.info("starting cluster")
        cluster.start(wait_other_notice=True)
        logger.info("adding node3")
        node3 = cluster.new_node(3, add_node=False)

        def stop_node3():
            logger.info("stopping node3")
            node3.stop()

        request.addfinalizer(stop_node3)

        logger.info("starting node3")
        node3.start(wait_other_notice=True)
        logger.info("stopping cluster")
        cluster.stop()
        logger.info("done")

    @pytest.mark.next_gating
    # Tablets are migrated asynchronously post bootstrap,
    # without offstrastegy compaction.
    @pytest.mark.required_features("!tablets")
    def test_off_strategy_during_bootstrap(self):
        """
        Compaction will be disabled during repair-based bootstrap and replace.
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={"enable_repair_based_node_ops": True})
        keys = 10000
        keyspace_name = "ks"
        table_name = "cf"

        # Create a single node cluster
        cluster.populate(1)
        node1 = cluster.nodelist()[0]
        cluster.start()

        with self.patient_cql_connection(node1) as session:
            create_ks(session, keyspace_name, 1)
            create_cf(session, table_name, columns={"c1": "text", "c2": "text"})

            insert_statement = session.prepare(f"INSERT INTO {keyspace_name}.{table_name} (key, c1, c2) VALUES (?, 'value1', 'value2')")
            execute_concurrent_with_args(session, insert_statement, [["k%d" % k] for k in range(keys)])

        node1.flush()

        # Bootstrapping a new node
        node2 = new_node(cluster)
        node2.start(wait_for_binary_proto=True)

        self._validate_off_strategy_started(node=node2, keyspace=keyspace_name, table=table_name, from_mark=0)

        matched_logs = node2.grep_log(f"Compacted .* sstables to |{bootstrap_start_log_pat}|{bootstrap_done_log_pat}")
        bootstrap_status = None
        for item in matched_logs:
            line = item[0]
            if re.search(bootstrap_start_log_pat, line):
                bootstrap_status = "START"
            elif re.search(bootstrap_done_log_pat, line):
                bootstrap_status = "END"
                break
            if bootstrap_status == "START" and f"Compact {keyspace_name}.{table_name} " in line and "Compacted " in line:
                raise Exception("Unexpected compaction of test table occurred during bootstrap, off-strategy doesn't work")
        assert bootstrap_status == "END"

        session = self.patient_cql_connection(node2)
        assert_one(session, "SELECT count(*) from ks.cf", [keys], cl=ConsistencyLevel.ONE)

    @pytest.mark.next_gating
    def test_simple_bootstrap(self):
        cluster = self.cluster
        tokens = cluster.balanced_tokens(2)
        cluster.set_configuration_options(values={"num_tokens": 1})

        logger.info("[node1, node2] tokens: %r", tokens)

        keys = 10000

        # Create a single node cluster
        cluster.populate(1)
        node1 = cluster.nodelist()[0]
        node1.set_configuration_options(values={"initial_token": tokens[0]})
        cluster.start(wait_other_notice=True)

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 1)
        create_cf(session, "cf", columns={"c1": "text", "c2": "text"})

        insert_statement = session.prepare("INSERT INTO ks.cf (key, c1, c2) VALUES (?, 'value1', 'value2')")
        execute_concurrent_with_args(session, insert_statement, [["k%d" % k] for k in range(keys)])

        node1.flush()
        node1.compact()

        data_total_size_node1 = self.get_space_used(node1)
        logger.info(f"before={data_total_size_node1}")

        # Reads inserted data all during the bootstrap process. We shouldn't
        # get any error
        reader = self.go(lambda _: query_c1c2(session, random.randint(0, keys - 1), ConsistencyLevel.ONE))

        # Bootstraping a new node
        node2 = cluster.new_node(2)
        node2.set_configuration_options(values={"initial_token": tokens[1]})
        node2.start(wait_for_binary_proto=True)
        node2.flush()
        node2.compact()

        reader.check()
        # nodetool cleanup is not required with tablets
        # as tablets are automatically cleaned up as an
        # integral part of tablet migration
        if not "tablets" in self.scylla_features:
            node1.cleanup()
        node1.compact()
        time.sleep(0.5)
        reader.check()

        data_total_size_node1_after = self.get_space_used(node1)
        data_total_size_node2_after = self.get_space_used(node2)

        logger.info(f"before={data_total_size_node1}, after={data_total_size_node1_after} + {data_total_size_node2_after}={data_total_size_node1_after + data_total_size_node2_after}")
        assert_almost_equal(data_total_size_node1, data_total_size_node1_after + data_total_size_node2_after, error=0.3)
        assert_almost_equal(data_total_size_node1_after, data_total_size_node2_after, error=0.3)

    @pytest.mark.use_cassandra_stress
    def test_read_from_bootstrapped_node(self):
        """Test bootstrapped node sees existing data, eg. CASSANDRA-6648"""
        cluster = self.cluster
        cluster.populate(3)
        cluster.start()

        node1 = cluster.nodes["node1"]
        node1.stress(["write", "n=10000", "-rate", "threads=8"])

        session = self.patient_cql_connection(node1)
        stress_table = "keyspace1.standard1"
        original_rows = list(session.execute(f"SELECT * FROM {stress_table}"))

        node4 = cluster.new_node(4)
        node4.start(wait_for_binary_proto=True)

        session = self.patient_exclusive_cql_connection(node4)
        new_rows = list(session.execute(f"SELECT * FROM {stress_table}"))
        assert original_rows == new_rows

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    @pytest.mark.use_cassandra_stress
    def test_manual_bootstrap(self):
        """Test adding a new node and bootstrapping it manually. No auto_bootstrap.
        This test also verify that all data are OK after the addition of the new node.
        eg. CASSANDRA-9022
        """
        cluster = self.cluster
        cluster.populate(2).start(wait_other_notice=True)
        (node1, node2) = cluster.nodelist()

        node1.stress(["write", "n=1000", "-schema", "replication(factor=1)", "-rate", "threads=1", "-pop", "dist=UNIFORM(1..1000)"])

        session = self.patient_exclusive_cql_connection(node2)
        stress_table = "keyspace1.standard1"

        original_rows = list(session.execute("SELECT * FROM %s" % stress_table))

        # Add a new node
        node3 = cluster.new_node(3, auto_bootstrap=False)
        node3.start(wait_for_binary_proto=True)
        if "tablets" not in self.scylla_features:
            node3.repair(keyspace="keyspace1", tables=["standard1"])
        node1.cleanup()

        current_rows = list(session.execute("SELECT * FROM %s" % stress_table))
        assert original_rows == current_rows

    @pytest.mark.scylla_mode("!debug")
    @pytest.mark.next_gating
    @pytest.mark.use_cassandra_stress
    def test_local_quorum_bootstrap(self, tmp_path):
        """Test that CL local_quorum works while a node is bootstrapping. CASSANDRA-8058"""

        cluster = self.cluster
        cluster.populate([1, 1])
        cluster.start()

        node1 = cluster.nodes["node1"]
        yaml_config = """
        # Create the keyspace and table
        keyspace: keyspace1
        keyspace_definition: |
          CREATE KEYSPACE keyspace1 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1, 'dc2': 1};
        table: users
        table_definition:
          CREATE TABLE users (
            username text,
            first_name text,
            last_name text,
            email text,
            PRIMARY KEY(username)
          ) WITH compaction = {'class':'SizeTieredCompactionStrategy'};
        insert:
          partitions: fixed(1)
          batchtype: UNLOGGED
        queries:
          read:
            cql: select * from users where username = ?
            fields: samerow
        """
        stress_config = tmp_path / "stress.yaml"
        stress_config.write_text(yaml_config)
        node1.stress(["user", "profile=" + str(stress_config), "n=2000000", "ops(insert=1)", "-rate", "threads=50"])

        node3 = cluster.new_node(3, data_center="dc2")
        node3.start(no_wait=True)
        time.sleep(3)

        output = node1.stress(["user", "profile=" + str(stress_config), "ops(insert=1)", "n=500000", "cl=LOCAL_QUORUM", "-rate", "threads=5", "-errors", "retries=2"])

        regex = re.compile("Operation.+error inserting key.+Exception")
        failure = regex.search(str(output.stderr)) or regex.search(str(output.stdout))
        assert failure is None, "Error during stress while bootstrapping"

        # Avoid reporting bootstrap errors in logs
        node3.stop(gently=False)

    def _validate_off_strategy_started(self, node: ScyllaNode, keyspace: str, table: str, from_mark: int):
        logger.debug(f"Validate off-strategy start on the {node.name} node")
        off_strategy_message = f"Starting off-strategy compaction for {keyspace}.{table}"
        matched_logs = node.grep_log(f"{off_strategy_message}|{bootstrap_start_log_pat}", from_mark=from_mark)
        bootstrap_status = None
        off_strategy_run = False
        for item in matched_logs:
            line = item[0]
            if re.search(bootstrap_start_log_pat, line):
                bootstrap_status = "START"

            if bootstrap_status == "START" and off_strategy_message in line:
                off_strategy_run = True
                break

        assert off_strategy_run, "off-strategy was not started during bootstrap"

    @pytest.mark.use_cassandra_stress
    def test_decommissioned_wiped_node_can_join(self):
        """
        @jira_ticket CASSANDRA-9765
        Test that if we decommission a node and then wipe its data, it can join the cluster.
        """
        cluster = self.cluster
        cluster.populate(3)
        cluster.start(wait_for_binary_proto=True)

        keyspace_name = "keyspace1"
        table_name = "standard1"
        query = f"SELECT * FROM {keyspace_name}.{table_name}"

        # write some data
        node1 = cluster.nodelist()[0]
        node1.stress(["write", "n=10K", "-rate", "threads=8"])

        with self.patient_cql_connection(node1) as session:
            original_rows = list(session.execute(query))

        # Add a new node, bootstrap=True ensures that it is not a seed
        logger.info("Starting node4")
        node4 = cluster.new_node(4, auto_bootstrap=True)
        node4.start(wait_for_binary_proto=True, wait_other_notice=True)

        with self.patient_cql_connection(node4) as session:
            assert_all(session=session, query=query, expected=original_rows, cl=ConsistencyLevel.QUORUM, ignore_order=True)

        # Decommission the new node and wipe its data
        logger.info("Decommissioning node4")
        mark = node1.mark_log()
        node4.decommission()
        logger.debug("Stopping node4")
        node4.stop(wait_other_notice=False)
        node1.watch_log_for(f"({node4.address()}|{node4.hostid()}) is now (dead|DOWN)", from_mark=mark)
        data_dir = os.path.join(node4.get_path(), "data")
        commitlog_dir = os.path.join(node4.get_path(), "commitlogs")
        logger.debug(f"Deleting {data_dir}")
        node4.rmtree(data_dir)
        node4.rmtree(commitlog_dir)

        # Now start it, it should be allowed to join
        if "consistent-topology-changes" not in self.scylla_features:
            ip4 = get_ip_from_node(node=node4)
            node1.watch_log_for(f"({ip4}|{node4.hostid()}) gossip quarantine over")
        logger.debug("Restarting node4")
        mark = node4.mark_log()
        node4.start(wait_for_binary_proto=True, wait_other_notice=True)
        logger.debug("Waiting for node4 to join")
        node4.watch_log_for(bootstrap_start_log_pat, from_mark=mark, timeout=0)

        if "tablets" not in self.scylla_features:
            self._validate_off_strategy_started(node=node4, keyspace=keyspace_name, table=table_name, from_mark=mark)

    @pytest.mark.use_cassandra_stress
    def test_failed_bootstrap_wiped_node_can_join(self):
        """
        @jira_ticket CASSANDRA-9765
        Test that if a node fails to bootstrap, it can join the cluster even if the data is wiped.
        """
        cluster = self.cluster
        cluster.populate(1)
        cluster.start(wait_for_binary_proto=True)

        stress_table = "keyspace1.standard1"

        # write some data, enough for the bootstrap to fail later on
        node1 = cluster.nodelist()[0]
        node1.stress(["write", "n=10k", "-rate", "threads=8"])
        node1.flush()

        session = self.patient_cql_connection(node1)
        original_rows = list(session.execute(f"SELECT * FROM {stress_table}"))

        # Add a new node, bootstrap=True ensures that it is not a seed
        node2 = cluster.new_node(2, auto_bootstrap=True)
        node2.set_configuration_options(values={"stream_io_throughput_mb_per_sec": 1})

        # kill node2 in the middle of bootstrap
        thread = KillOnBootstrap(node2)
        thread.start()

        mark = node1.mark_log()
        logger.info("Starting node2")
        node2.start(wait_for_binary_proto=False, wait_other_notice=False)
        thread.join()
        assert not node2.is_running()
        logger.info("node2 killed during bootstrap. Waiting for other nodes to notice...")

        message = f"({node2.address()}|{node2.hostid()}) has been silent .* removing from gossip"
        if "consistent-topology-changes" in self.scylla_features:
            message = f"Finished to force remove node ({node2.address()}|{node2.hostid()})"
        node1.watch_log_for(message, from_mark=mark)

        # wipe any data for node2
        wipe_node_data_directories(node2)

        # Now start it again, it should be allowed to join
        if "consistent-topology-changes" not in self.scylla_features:
            ip2 = get_ip_from_node(node=node2)
            node1.watch_log_for(f"({ip2}|{node2.hostid()}) gossip quarantine over")
        # Remove the limit on streaming throughput to speed up the test
        # - on rejoin, we don't need the streaming speed to be limited
        node2.set_configuration_options(values={"stream_io_throughput_mb_per_sec": 0})
        mark = node2.mark_log()

        self.ignore_log_patterns.extend(
            [
                "raft topology: CDC generation publisher fiber got error exceptions::unavailable_exception",
                "raft_topology - CDC generation publisher fiber got error exceptions::unavailable_exception",
                r"raft_topology - send_raft_topology_cmd\(stream_ranges\) failed with exception \(node state is bootstrapping\)",
            ]
        )

        logger.debug("Restarting node2")
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)
        logger.debug("Waiting for node2 to join")
        node2.watch_log_for(bootstrap_start_log_pat, from_mark=mark, timeout=0)

    def _full_cluster_recovery_after_stop(self, gently, num_of_nodes, rf):
        """
        steps:
        - Create N node cluster (RF=rf)
        - Insert some data with cassandra-stress (wait all data is inserted, no flush manually)
        - Flush (only needed on D-test otherwise the test will fail - no data will be written w/o the flush)
        - Stop cluster (gently/forcibly)
        - Start the cluster
        - read data make sure all data is alive
        """
        # Create/Start cluster
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=rf, nodes_per_rack=num_of_nodes // rf)
        cluster.populate(cluster_topology).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)

        logger.info("Preparing a KS and a CF...")
        create_ks(session, name="ks", rf=rf)
        create_c1c2_table(session)

        logger.info("Populating the data...")
        insert_c1c2(session, n=10000, consistency=ConsistencyLevel.QUORUM)
        # This flush will only be needed on d-test otherwise the test will fail (no data will be written)
        cluster.flush()

        logger.info("Saving nodes process list")
        pid_ls = [node.pid for node in cluster.nodelist()]
        process_ls = []
        for pid in pid_ls:
            process = Process(pid)
            process_ls.append(process)

        logger.info("Killing all nodes")
        cluster.stop_nodes(gently=gently, wait_seconds=20)

        logger.info("Making sure all node processes are down")
        for process in process_ls:
            assert not process.is_running(), f"Node with the following pid {process.pid} didn't stop/exit correctly"

        logger.info("Starting all nodes")
        cluster.start_nodes(no_wait=False)
        wait_reconnection(session)

        logger.info("Checking that no data was lost")
        for key in range(10000):
            query_c1c2(session, key, ConsistencyLevel.QUORUM)

    def test_full_cluster_recovery_after_forcibly_stop_3_nodes_rf_3(self):
        self._full_cluster_recovery_after_stop(gently=False, num_of_nodes=3, rf=3)

    def test_full_cluster_recovery_after_gentle_stop_6_nodes_rf_2(self):
        self._full_cluster_recovery_after_stop(gently=True, num_of_nodes=6, rf=2)

    def test_full_cluster_recovery_after_forcibly_stop_4_nodes_rf_1(self):
        self._full_cluster_recovery_after_stop(gently=False, num_of_nodes=4, rf=1)

    @pytest.mark.parametrize(
        "is_gracefully",
        [
            pytest.param(True, id="gracefully"),
            pytest.param(False, id="force"),
        ],
    )
    @pytest.mark.use_cassandra_stress
    def test_cluster_become_unavailable_when_kill_node_during_bootstrap(
        self,
        is_gracefully,
    ):
        """
        Add n1,n2
        Create ks with RF =2
        Insert data with CL = 2
        Bootstrap n3
        Kill n3 before n3 finishes bootstrap
        Check n1 and n2 will notice n3 is gone
        Check writes with CL = 2 will recover
        """
        tablets_enabled = "tablets" in self.scylla_features
        executor = ThreadPoolExecutor(max_workers=2)
        kill_node_err_msg = "The process is dead, returncode={}"
        ks_name, consistency_level_key = "keyspace", "QUORUM" if tablets_enabled else "TWO"
        sync_ks_name = "system_distributed" if tablets_enabled else ks_name
        beginning_stream_session_msg = f"Beginning stream session|sync data for keyspace={sync_ks_name}, status=started"
        replication_factor = 3 if tablets_enabled else 2
        cluster_size = 3 if tablets_enabled else 2

        cluster = self.cluster
        cluster.set_configuration_options({"allowed_repair_based_node_ops": "replace,removenode,rebuild,bootstrap,decommission"})
        nodes = generate_cluster_topology(dc_num=1, rack_num=cluster_size, nodes_per_rack=1, dc_name_prefix="dc", rack_name_prefix="r")
        logger.info(f"Creating new cluster with topology: {nodes}")
        cluster.populate(nodes=nodes).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1 = cluster.nodelist()[0]

        def write_in_background(node, duration_seconds):
            write_stress_cmd = ["write", f"cl={consistency_level_key}", f"duration={duration_seconds}s", "-rate", "threads=10", "-log", "interval=5", "-schema", f"replication(factor={replication_factor}) keyspace={ks_name}"]

            logger.info(f"Starting stress command on {node.name}: {write_stress_cmd}")
            return executor.submit(lambda: node.stress(stress_options=write_stress_cmd))

        # Pre-populate some data on the node
        # to ensure the keyspace will be included in the new node bootstrap
        write_in_background(node1, duration_seconds=5).result()

        # And then continue to write in background while bootstrapping a new node
        stress_thread = write_in_background(node1, duration_seconds=30)

        logger.info("Adding new node")
        new_node = cluster.new_node(i=cluster_size + 1, debug=True, auto_bootstrap=True, is_seed=False, data_center="dc1", rack="r1")
        start_new_node_thread = executor.submit(lambda: new_node.start(wait_for_binary_proto=True, jvm_args=["--logger-log-level", "stream_session=debug"], no_wait=True))
        mark_log = new_node.mark_log()

        logger.info("Trying to find the following '%s' message in logs of node '%s'", bootstrap_start_log_pat, new_node.name)
        new_node.watch_log_for(exprs=bootstrap_start_log_pat, from_mark=mark_log)
        logger.info("Trying to find the following '%s' message in logs of node '%s'", beginning_stream_session_msg, new_node.name)
        new_node.watch_log_for(exprs=beginning_stream_session_msg, from_mark=mark_log)

        nodes = cluster.nodelist()[0:-1]
        mark_log_list = [node.mark_log() for node in nodes]
        logger.info("%s killing node'%s' (PID is '%s')", {"Gracefully" if is_gracefully else "Force"}, new_node.name, new_node.pid)
        self.ignore_log_patterns.extend(
            [
                r"bootstrap.*failed",
                r"raft_topology - raft_topology_cmd.*failed with: (?:seastar::abort_requested_exception|abort requested)",
            ]
        )
        new_node.stop(wait=True, gently=is_gracefully, wait_other_notice=True)
        if not tablets_enabled:
            removing_from_gossip_msg = f"gossip - Removed endpoint ({new_node.address()}|{new_node.hostid()})"
            for node, mark_log in zip(nodes, mark_log_list):
                logger.info("Checking the following message '%s' exits in node '%s'", removing_from_gossip_msg, node.name)
                node.watch_log_for(exprs=removing_from_gossip_msg, from_mark=mark_log)

        if not is_gracefully and not tablets_enabled:
            assert kill_node_err_msg.format(-9) == str(start_new_node_thread.exception()), f"The node '{new_node.name}' should be killed by SIGKILL signal"

        logger.info("Waiting until stress thread will finish running")
        # c-s is expected to fail with CL=TWO when the bootstrapped node is killed
        try:
            results = stress_thread.result()
            assert_cs_success(results)
        except ToolError:
            pass

        # Now, after bootstrapping was aborted, c-s must pass
        stress_thread = write_in_background(node1, duration_seconds=5)
        results = stress_thread.result()
        assert_cs_success(results)

    @pytest.mark.parametrize(
        "is_gracefully",
        [
            pytest.param(True, id="gracefully"),
            pytest.param(False, id="force"),
        ],
    )
    @pytest.mark.use_cassandra_stress
    @pytest.mark.required_features("tablets")
    def test_cluster_become_unavailable_when_kill_node_during_tablets_bootstrap(self, is_gracefully, dtest_config):
        """
        Add n1,n2
        Create ks with RF =2
        Insert data with CL = 2
        Bootstrap n3
        Kill n3 before n3 finishes bootstrap
        Check n1 and n2 will notice n3 is gone
        Check writes with CL = 2 will recover
        """
        assert "tablets" in self.scylla_features
        executor = ThreadPoolExecutor(max_workers=2)
        kill_node_err_msg = "The process is dead, returncode={}"
        ks_name, consistency_level_key = "keyspace", "QUORUM"
        beginning_stream_session_msg = "Streaming for tablet migration|Beginning stream session"
        replication_factor = 3
        cluster_size = 3

        cluster = self.cluster
        cluster.set_configuration_options({"allowed_repair_based_node_ops": "replace,removenode,rebuild,bootstrap,decommission"})
        logger.info("Creating new cluster with '%s' nodes", cluster_size)
        cluster_topology = generate_cluster_topology(rack_num=cluster_size, dc_name_prefix="dc", rack_name_prefix="r")
        cluster.populate(nodes=cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1 = cluster.nodelist()[0]

        def write_in_background(node, duration_seconds):
            write_stress_cmd = ["write", f"cl={consistency_level_key}", f"duration={duration_seconds}s", "-rate", "threads=10", "-log", "interval=5", "-schema", f"replication(factor={replication_factor}) keyspace={ks_name}"]

            logger.info(f"Starting stress command on {node.name}: {write_stress_cmd}")
            return executor.submit(lambda: node.stress(stress_options=write_stress_cmd))

        # Pre-populate some data on the node
        # to ensure the keyspace will be included in the new node bootstrap
        write_in_background(node1, duration_seconds=5).result()

        # And then continue to write in background while bootstrapping a new node
        stress_thread = write_in_background(node1, duration_seconds=30)

        logger.info("Adding new node")
        new_node = cluster.new_node(i=cluster_size + 1, debug=True, auto_bootstrap=True, is_seed=False, data_center="dc1", rack="r3")
        start_new_node_thread = executor.submit(lambda: new_node.start(wait_for_binary_proto=True, jvm_args=["--logger-log-level", "stream_session=debug"], no_wait=True))
        mark_log = new_node.mark_log()

        logger.info("Trying to find the following '%s' message in logs of node '%s'", bootstrap_start_log_pat, new_node.name)
        new_node.watch_log_for(exprs=bootstrap_start_log_pat, from_mark=mark_log)
        logger.info("Trying to find the following '%s' message in logs of node '%s'", beginning_stream_session_msg, new_node.name)
        new_node.watch_log_for(exprs=beginning_stream_session_msg, from_mark=mark_log)

        nodes = cluster.nodelist()[0:-1]
        mark_log_list = [node.mark_log() for node in nodes]
        logger.info("%s killing node'%s' (PID is '%s')", {"Gracefully" if is_gracefully else "Force"}, new_node.name, new_node.pid)
        self.ignore_log_patterns.extend(
            [
                r"bootstrap.*failed",
                r"raft_topology - raft_topology_cmd.*failed with: (?:seastar::abort_requested_exception|abort requested)",
            ]
        )
        new_node.stop(wait=True, gently=is_gracefully, wait_other_notice=True)
        logger.info("Waiting until stress thread will finish running")
        # c-s is expected to fail with CL=TWO when the bootstrapped node is killed
        try:
            results = stress_thread.result()
            assert_cs_success(results)
        except ToolError:
            pass

        # Now, after bootstrapping was aborted, c-s must pass
        stress_thread = write_in_background(node1, duration_seconds=5)
        results = stress_thread.result()
        assert_cs_success(results)

        new_node.start(wait_other_notice=True)
        stress_thread = write_in_background(node1, duration_seconds=5)
        results = stress_thread.result()
        assert_cs_success(results)
        logger.debug(format_cs_output(results))

    def test_reject_node_bootstrap_no_gossip(self):
        """
        Node n4 will learn the ip and uuid of n3, but it does not know the gossip status of n3 since gossip status is
        published only by the node itself.
        After full cluster shutdown, gossip status of n3 will not be present until n3 is restarted again.
        So n4 will not think n3 is part of the ring.
        In this case, it is better to reject the bootstrap.

        A test for rejecting new node(4) bootstrap
        if one of the previous nodes is down after cluster restart.
        The new node does not know the gossip status of n3
        since gossip status is published only by the node itself.
        According to task: https://github.com/scylladb/scylla-dtest/issues/2858
        """
        expected_error = r"Startup failed:.* has gossip status=UNKNOWN|Startup failed:.*topology coordinator rejected request to join the cluster|received notification of being banned from the cluster"

        self.fixture_dtest_setup.ignore_log_patterns += [expected_error]

        logger.info("Populating cluster with one node")
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        cluster.populate(cluster_topology)
        logger.info("Starting cluster")
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
        (node1, node2, node3) = cluster.nodelist()
        logger.info("Inserting some data to the cluster")
        session = self.patient_exclusive_cql_connection(node1)
        n_of_keys = 1000
        create_ks(session, "ks", 3)
        create_cf(session, "cf", columns={"c1": "text", "c2": "text"})
        insert_c1c2(session, n=n_of_keys, consistency=ConsistencyLevel.ALL)
        logger.info("Stopping cluster")
        cluster.stop(gently=True)
        logger.info("Stopping cluster has finished.")
        logger.info("Starting node 1")
        node1.start(wait_other_notice=True)
        logger.info("Starting node 2")
        node2.start(wait_other_notice=True)

        node4: ScyllaNode = cluster.new_node(4, data_center=1, rack=2)
        mark4 = node4.mark_log()
        node4.start(wait_other_notice=False, wait_for_binary_proto=False)
        node4.watch_log_for(expected_error, from_mark=mark4)
        node4.stop(wait_other_notice=False)

        node3.start(wait_other_notice=True)
        if "consistent-topology-changes" in self.scylla_features:
            # clear data on node4 before start
            wipe_node_data_directories(node4)
        node4.start(wait_other_notice=True, wait_for_binary_proto=True)
        node3.stop(wait_other_notice=False)
        session = self.patient_exclusive_cql_connection(node4)
        for k in range(n_of_keys):
            query_c1c2(session, k, consistency=ConsistencyLevel.QUORUM)

    def test_reject_bootstrap_wiped_node_misspelled_seeds(self):
        """
        Regression test for https://github.com/scylladb/scylla-enterprise/issues/3523
        as it is already solved in master.

        A stopped and wiped node with a configuration file with a misspelled seeds
        keyword, should not join the cluster when restarted with the same ip address
        and same cluster name.
        """
        logger.info("populating cluster with three nodes")
        cluster = self.cluster
        cluster.populate(3)
        logger.info("starting cluster")
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3 = cluster.nodelist()
        logger.info("stopping node #2")
        node2 = cluster.nodelist()[1]
        node2.stop(wait_other_notice=True, wait_seconds=10)
        logger.info("wiping node #2 data")
        wipe_node_data_directories(node2)

        logger.info("misspelling seeds in the config file")
        config = os.path.join(node2.get_path(), "conf", "scylla.yaml")
        with open(config) as fh:
            content = fh.read()

        content = content.replace("seeds:", "seed:")

        with open(config, "w") as fh:
            fh.write(content)

        logger.info("restarting node #2")
        try:
            node2.start(wait_other_notice=True, wait_for_binary_proto=False)
        except TimeoutError:
            pass

        logger.info("checking cluster status")
        for node in [node1, node3]:
            wait_for_nodes_status(node, ["UN", "DN", "UN"])

        logger.info("stopping cluster")
        cluster.stop()
        logger.info("done")
