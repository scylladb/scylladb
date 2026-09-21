#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import collections
import logging
import random
import re
import secrets
import string
import tempfile
import time
import traceback
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, suppress
from datetime import datetime
from itertools import permutations
from threading import Event

import pytest
import requests
from cassandra import ConsistencyLevel, InvalidRequest, OperationTimedOut, Unavailable, WriteTimeout
from cassandra.cluster import ExecutionProfile, NoHostAvailable
from cassandra.policies import ConstantSpeculativeExecutionPolicy, FallthroughRetryPolicy
from cassandra.query import SimpleStatement
from ccmlib.node import NodetoolError, TimeoutError, ToolError
from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.scylla_node import ScyllaNode
from packaging.version import Version

from bootstrap_test import bootstrap_start_log_pat
from dtest_class import (
    Tester,
    WaitTimeoutExpiredError,
    create_cf,
    create_ks,
    create_ks_query,
    get_ip_from_node,
    retry_till_success,
    wait_for,
)
from tools.assertions import assert_invalid, assert_row_count
from tools.cluster import minimum_scylla_version, new_node, run_rest_api
from tools.cluster_topology import generate_cluster_topology
from tools.data import (
    create_c1c2_table,
    insert_c1c2,
    insert_c1cn,
    query_c1c2,
    query_c1c2_concurrent,
    rows_to_list,
)
from tools.docker_utils import running_in_podman
from tools.files import wipe_node_data_directories
from tools.group0_and_token_ring import find_and_clean_garbage_from_group0, verify_group0_and_token_ring_members, wait_for_token_ring_and_group0_consistency
from tools.iptables import IPTable, IPTableRule
from tools.marks import issue_open, with_feature
from tools.rackdc import update_properties
from tools.schema import change_schema_safely, describe_rf, get_replication_options
from tools.status import (
    nodetool_gossipinfo,
    nodetool_status,
    verify_nodes_status,
    wait_for_nodes_status,
)

logger = logging.getLogger(__name__)


def generate_test_name(val):
    if isinstance(val, bool):
        return ""
    return val.replace(" ", "_")


@contextmanager
def template_file(src_file, /, **kwds):
    with open(src_file, encoding="utf-8") as f:
        template = string.Template(f.read())
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False) as output_file:
        output_file.write(template.substitute(kwds))
        output_file.flush()
        yield output_file.name


@pytest.mark.next_gating
@pytest.mark.dtest_full
class TestUpdateClusterLayout(Tester):
    @staticmethod
    def default_config_options(hinted_handoff_enabled=False, enable_sstable_key_validation=True):
        values = {}
        if hinted_handoff_enabled is not None:
            values.update({"hinted_handoff_enabled": hinted_handoff_enabled})
        if enable_sstable_key_validation is not None:
            values.update({"enable_sstable_key_validation": enable_sstable_key_validation})
        return values

    def check_rows_on_node(  # noqa: PLR0913
        self,
        node_to_check,
        rows,
        found=None,
        missings=None,
        restart=True,
        ks="ks",
        cf="cf",
        timeout=None,
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

        if not timeout:
            timeout = self.cql_timeout(300)

        session = self.patient_cql_connection(node_to_check, ks)
        query = SimpleStatement(f"SELECT * FROM {ks}.{cf} LIMIT {rows * 2}", consistency_level=ConsistencyLevel.ONE)
        result = session.execute(query, timeout=timeout)
        if rows > 1000:
            # count the number by iterating the resultset for smaller memory footprint
            count = sum(1 for _ in result)
        else:
            count = len(list(result))
        assert count == rows

        for k in found:
            query_c1c2(session, k, ConsistencyLevel.ONE)

        for k in missings:
            query_c1c2(session, k, ConsistencyLevel.ONE, must_be_missing=True)

        if restart:
            self.cluster.start_nodes()

    @pytest.mark.parametrize("test_stream_plan_ranges_fraction", [pytest.param(False), pytest.param(True, marks=pytest.mark.skip_if(with_feature("tablets") & issue_open("https://github.com/scylladb/scylladb/issues/23457")))])
    def test_simple_add_node(self, test_stream_plan_ranges_fraction):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a single node with rf=1, insert data
        2. Add a new node in new rack
        3. alter keyspace with rf = 2
        3. Check that each node has all the data
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values=self.default_config_options(), batch_commitlog=True)

        if test_stream_plan_ranges_fraction:
            self.cluster.set_configuration_options(values={"stream_plan_ranges_fraction": 1, "enable_repair_based_node_ops": False})
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=1)

        cluster.populate(cluster_topology).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 1)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ONE)
        self.check_rows_on_node(node1, 1000)
        node2 = new_node(cluster, data_center=node1.data_center, rack="rack2")
        node2.start(wait_for_binary_proto=True)
        session = self.patient_exclusive_cql_connection(node2)
        rf = get_replication_options(session, "ks")[node1.data_center]
        rf = ["rack1", "rack2"] if type(rf) is list else 2
        session.execute(f"ALTER KEYSPACE ks WITH replication = {{ 'class' : 'NetworkTopologyStrategy', '{node1.data_center}': {describe_rf(rf)}}};")
        node1.watch_log_for_alive(node2)
        node2.watch_log_for_alive(node1)
        # need to run repair after change rf only with vnodes
        if "tablets" not in self.scylla_features:
            for node in cluster.nodelist():
                node.nodetool("repair -pr")
        session.execute("use ks;")

        insert_c1c2(session, keys=range(1000, 2000), consistency=ConsistencyLevel.TWO)
        self.check_rows_on_node(node2, 2000)
        self.check_rows_on_node(node1, 2000)

        if test_stream_plan_ranges_fraction:
            pattern = f"Streaming plan for Bootstrap-ks-index-"
            matchings = node2.grep_log(pattern)
            assert len(matchings) == 1

    @pytest.mark.parametrize(
        "iterations,node_count,rf",
        [
            (3, 1, 1),
            (2, 3, 1),
            (3, 1, 2),
            (2, 3, 2),
        ],
    )
    def test_iterative_add_decommission_nodes(self, iterations, node_count, rf):
        """
        Test growing and shrinking a cluster
        1. Create a cluster with dc 1, racks: rf, 1 node in each rack
        2. Create keyspace with 'replication factor = rf', insert data
        3. In a loop add new nodes to one of the rack (working rack)
        4. Check that all data exists
        5. In a loop remove all nodes from working rack except the last added node
        6. Check that all data exists
        """
        cluster = self.cluster

        # With rf-rack-valid-keyspaces feature configure cluster always with rf == # racks
        initial_racks = rf
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=initial_racks, nodes_per_rack=1)
        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(cluster_topology).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", rf)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        consistency = ConsistencyLevel.ALL
        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ONE)

        query = SimpleStatement("SELECT key FROM ks.cf limit 3000", consistency_level=consistency)
        for iteration in range(1, iterations + 1):
            rack = cluster.nodelist()[iteration % initial_racks].rack
            for i in range(1, node_count + 1):
                node_i = new_node(cluster, data_center=node1.data_center, rack=rack)
                node_i.start(wait_for_binary_proto=True, wait_other_notice=True)
                session_i = self.patient_exclusive_cql_connection(node_i)
                session_i.execute("use ks;")
                insert_c1c2(session_i, keys=range(iteration * 100000 + i * 2000, iteration * 100000 + i * 2000 + 100), consistency=consistency)
                logger.debug(f"added {node_i.name}")

            result = list(session.execute(query))
            assert len(result) == iteration * node_count * 100 + 1000, "data loss after increasing size to %d expecting %d rows %d" % (len(cluster.nodelist()), iteration * node_count * 100 + 1000, len(result))

            # choose nodes for decommission from rack where they were added except latest one added
            nodes_to_decommission = [node for node in cluster.nodelist() if node.rack == rack and node != node_i]
            for node_i in nodes_to_decommission:
                if node1.name != node_i.name and node_i.is_live():
                    node_i.decommission()
                    node_i.stop()
                    logger.debug(f"decommissioned {node_i.name}")

            last_node = cluster.nodelist()[-1]
            session = self.patient_cql_connection(last_node)
            result = list(session.execute("SELECT key FROM ks.cf limit 3000"))
            assert len(result) == iteration * node_count * 100 + 1000, "data loss after shrinking to 2 node execpeting %d rows %d" % (iteration * node_count * 100 + 1000, len(result))

        node1.decommission()
        node1.stop()
        logger.debug(f"decommissioned {node1.name}")
        last_node = cluster.nodelist()[-1]
        session = self.patient_cql_connection(last_node)
        result = list(session.execute("SELECT key FROM ks.cf limit 3000"))
        assert len(result) == iterations * node_count * 100 + 1000, "data loss after shrinking to 1 node %s expecting %d rows %d" % (last_node.name, iterations * node_count * 100 + 1000, len(result))

    def _create_some_data(self, node, rf: int, num_keys: int):
        with self.patient_cql_connection(node) as session:
            create_ks(session, "ks", rf)
            create_cf(session, "cf", columns={"c1": "text", "c2": "text"})
            logger.debug("Inserting %s keys", num_keys)
            insert_c1c2(session, keys=range(num_keys), consistency=ConsistencyLevel.ALL)

    def _ignore_tablets_rack_decommission_errors(self):
        if "tablets" in self.scylla_features:
            # these errors are expected in teardown after a decommission error with tablets
            ignore_errors = [
                r"raft_topology - tablets draining failed with std::runtime_error[ :]+\(?Unable to find new replica for tablet",
                r"raft_topology - Decommission failed. See earlier errors \(Rolled back: Failed to drain tablets: std::runtime_error[ :]+\(?Unable to find new replica for tablet",
                r"raft_topology - tablets draining failed with std::runtime_error[ :]+\(?There are nodes with tablets to drain but no candidate nodes in DC DC1. Consider adding new nodes or reducing replication factor.\)?. Aborting the topology operation",
                r"raft_topology - Decommission failed. See earlier errors \(Rolled back: Failed to drain tablets: std::runtime_error[ :]+\(?There are nodes with tablets to drain but no candidate nodes in DC DC1. Consider adding new nodes or reducing replication factor.\)?\)",
                r"raft_topology - Decommission failed. See earlier errors \(node decommission rejected: Cannot remove the node because its removal would make some existing keyspace RF-rack-invalid\)",
                r"raft_topology - Decommission failed.*No candidate nodes",
            ]
            self.ignore_log_patterns += ignore_errors

    def test_decommission_with_multi_rack(self):
        """
        Test node decommission scenarios with multi rack.
        """
        cluster = self.cluster
        nodes = {"DC1": {"RACK1": 2, "RACK2": 1, "RACK3": 2}}
        cluster.populate(nodes).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, _, _, node4, node5 = cluster.nodelist()
        num_keys = 500
        self._create_some_data(node1, rf=3, num_keys=num_keys)
        logger.info(f"Decommission {node5.name}")
        node5.decommission()
        try:
            self._ignore_tablets_rack_decommission_errors()
            logger.info(f"Decommission {node4.name}")
            node4.decommission()
            assert "tablets" not in self.scylla_features, "A full rack decommission is currently not supported with tablets"
        except ToolError as error:
            if "tablets" not in self.scylla_features:
                raise

            drain_err_msg = "There are nodes with tablets to drain but no candidate nodes in DC DC1. Consider adding new nodes or reducing replication factor"
            drain_err_msg2 = "No candidate nodes"
            replica_err_msg = "Unable to find new replica for tablet"
            rf_rack_valid_err_msg = "because its removal would make some existing keyspace RF-rack-invalid"

            if drain_err_msg2 not in error.stderr and drain_err_msg not in error.stderr and replica_err_msg not in error.stderr and rf_rack_valid_err_msg not in error.stderr:
                raise

        with self.patient_cql_connection(node1) as session:
            assert_row_count(session=session, table_name="ks.cf", expected=num_keys)

    def test_decommission_last_node_in_rack(self):
        """
        reproducer for the following issues:
        - https://github.com/scylladb/scylla-enterprise/issues/3106
        - https://github.com/scylladb/scylladb/issues/14184
        - https://github.com/scylladb/scylla-operator/issues/1271

        1) create 3 nodes in rack1
        2) change a keyspace to `NetworkTopologyStrategy`
        3) add node4 in rack2
        4) decommission node4
        """
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=3)
        cluster.populate(cluster_topology)
        for node in cluster.nodelist():
            update_properties(nodes=[node], properties={"dc": node.data_center, "rack": node.rack, "prefer_local": "false"})
        cluster.start()
        node1 = cluster.nodelist()[0]
        # with consistent topology auth-v2 is enabled and it doesn't allow nor require to change RF as it replicates via raft
        if "consistent-topology-changes" not in self.scylla_features:
            node1, *_ = cluster.nodelist()
            dc = node1.get_datacenter_name()
            cql = f"ALTER KEYSPACE system_auth WITH replication = {{'class': 'NetworkTopologyStrategy', '{dc}': '3'}}"
            with self.patient_cql_connection(node1) as session:
                session.execute(cql)

        node4 = new_node(cluster)
        update_properties(nodes=[node4], properties={"dc": node1.data_center, "rack": "rack2", "prefer_local": "false"})

        node4.start(wait_other_notice=True, wait_for_binary_proto=True)
        node4.decommission()

    @pytest.mark.no_boot_speedups
    @pytest.mark.parametrize(
        "test_case",
        [
            pytest.param(0, id="case_0"),
            pytest.param(1, id="case_1"),
            pytest.param(2, id="case_2"),
        ],
    )
    # Unsuitable for tablets: Datacenter datacenter1 doesn't have enough nodes for replication_factor=3
    @pytest.mark.required_features("!tablets")
    def test_simple_add_two_nodes_in_parallel(self, test_case: int) -> None:  # noqa: PLR0915
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a single node with rf=3, insert data
        2. Add two nodes
        3. Check that first added node succeeds to join the cluster and completes bootstrap
        4a. Check that the second node succeeds too in Raft topology mode
        4b. ...or check that the second node fails with correct cause otherwise
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfere with the test (this must be after it's been populated.)
        cluster.set_configuration_options(values=self.default_config_options(), batch_commitlog=True)
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=1)
        cluster.populate(cluster_topology).start()
        node1 = cluster.nodelist()[0]

        node1_session = self.patient_cql_connection(node1)
        create_ks(node1_session, "ks", 3)
        create_cf(node1_session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        num_keys = 500000 if isinstance(cluster, ScyllaCluster) and cluster.scylla_mode != "debug" else 10000
        logger.debug("Inserting %s keys", num_keys)
        insert_c1c2(node1_session, keys=range(num_keys), consistency=ConsistencyLevel.ONE)

        node2 = new_node(cluster, data_center=node1.data_center, rack=node1.rack)

        # Creating an additional node without actually adding it to the cluster.
        i = len(cluster.nodes) + 1
        node3 = cluster.new_node(i, auto_bootstrap=True, add_node=False)

        logger.debug("Starting node2")
        node2.start(no_wait=True)
        if "consistent-topology-changes" in self.scylla_features:
            node2.watch_log_for(["BOOTSTRAP", "raft_topology - start streaming"])
        else:
            node2.watch_log_for(["BOOTSTRAP", "Gossip settled|No gossip backlog"])
        mark = node2.mark_log()

        # Select a test case that determines when to start node3 relative to node2's timeline.
        #
        # case 0: start immediately after node2 has reached "BOOTSTRAP"
        # case 1: wait 5-30 seconds after node2 has started "BOOTSTRAP"
        # case 2: wait up to 30 seconds after node2 has reached "Starting to bootstrap"
        #
        # In Raft topology mode all three cases should succeed, otherwise in case 0 node3 is expected to fail to start,
        # as reported by "Startup failed" message, on cases 1 or 2 node3 may or may not succeed to start.
        late_start = test_case > 0
        logger.debug("Testing case %s: late_start=%s", test_case, late_start)

        if test_case == 1:
            timeout = 5 + random.random() * 25
            logger.debug("Waiting for %.2f seconds", timeout)
            time.sleep(timeout)
        elif test_case == 2:
            msg = bootstrap_start_log_pat
            timeout = 30
            logger.debug("Watching %s log for msg='%s': timeout=%.2f seconds", node2.name, msg, timeout)
            with suppress(TimeoutError):
                node2.watch_log_for(msg, from_mark=mark, timeout=timeout)
            timeout = random.random() * 30
            logger.debug("Waiting for %.2f seconds", timeout)
            time.sleep(timeout)

        expected_errors = [
            "Other bootstrapping/leaving/moving nodes detected, cannot bootstrap while consistent_rangemovement is true",
            f"Node .*({node2.address()}|{node2.hostid()}) has gossip status=UNKNOWN. Try fixing it before adding new node to the cluster",
        ]
        self.ignore_log_patterns += expected_errors

        logger.debug("Starting node3")
        cluster.add(node3, is_seed=False, data_center=node1.data_center, rack=node1.rack)
        node3.start(no_wait=True)

        msg = "initialization completed"
        if "consistent-topology-changes" not in self.scylla_features:
            # Let's check that it detected there was another bootstrapping in progress.
            if not late_start:
                logger.debug("Waiting until node3 notices other node was booting")
                res = node3.watch_log_for(
                    "|".join(
                        [
                            "Checking bootstrapping/leaving.* sleep 1 second and check again",
                            *expected_errors,
                        ]
                    )
                )
                logger.debug("Log messages: %s", res)
                msg = "init - Startup failed"
            else:
                # node3 may succeed booting if starting late.
                msg += "|init - Startup failed"

        logger.debug("Waiting for %s startup to complete", node3.name)
        res = node3.watch_log_for(msg)
        logger.debug("Log messages: %s", res)

        nodes_expected_to_succeed = [node1, node2]
        if "consistent-topology-changes" in self.scylla_features:
            nodes_expected_to_succeed.append(node3)

        for node in nodes_expected_to_succeed:
            logger.debug("Checking %s started successfully", node.name)
            node.watch_log_for("Starting listening for CQL clients")

        logger.debug("Verifying all nodes expected to stay alive see each other")
        for watch_log_node, node_to_watch in permutations(nodes_expected_to_succeed, 2):
            watch_log_node.watch_log_for_alive(node_to_watch)

        logger.debug("Inserting more data")
        node2_session = self.patient_exclusive_cql_connection(node2)
        node2_session.execute("use ks;")
        insert_c1c2(node2_session, keys=range(num_keys, 2 * num_keys), consistency=len(nodes_expected_to_succeed))

        logger.debug("Verifying data")
        for node in nodes_expected_to_succeed:
            self.check_rows_on_node(node, 2 * num_keys)

    @pytest.mark.required_features("tablets")
    def test_tablets_add_three_nodes_in_parallel(self):  # noqa: PLR0915
        """
        1. Create a cluster with 3 racks with 1 node in each and rf=3, insert data
        2. Add 1 node in parallel to each rack
        3. Check that both nodes succeeded to join the cluster and completed bootstrap
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        config_vals = self.default_config_options()

        # Force capacity based balancing because the test relies on tablet count to validate balance
        config_vals["force_capacity_based_balancing"] = True

        cluster.set_configuration_options(values=config_vals, batch_commitlog=True)
        racks_num = rf = 3
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=racks_num, nodes_per_rack=1)
        cluster.populate(cluster_topology).start()
        node1, node2, node3 = old_nodes = cluster.nodelist()

        num_keys = 10000
        keyspace = "ks"
        table = "cf"
        with self.patient_cql_connection(node1) as session:
            create_ks(session, keyspace, rf, tablets=128)
            create_cf(session, table, read_repair=0.0, columns={"c1": "text", "c2": "text"})

            logger.debug(f"Inserting {num_keys} keys")
            insert_c1c2(session, keys=range(num_keys), consistency=ConsistencyLevel.ALL)

        def verify_load_balancing(nodes):
            def get_tablet_count(node):
                tablet_count = dict()
                with self.patient_exclusive_cql_connection(node) as session:
                    result = session.execute(f"SELECT replicas FROM system.tablets WHERE keyspace_name='{keyspace}' and table_name='{table}' ALLOW FILTERING")
                    for row in list(result):
                        for uuid, shard in row[0]:
                            host_id = str(uuid)
                            if not host_id in tablet_count:
                                tablet_count[host_id] = defaultdict(int)
                            tablet_count[host_id][int(shard)] += 1
                return tablet_count

            def verify_nodes_tablet_count(nodes):
                per_node_tablet_count = dict()
                for node in nodes:
                    host_id = str(node.hostid())
                    per_node_tablet_count[host_id] = sum(tablet_count[host_id].values())
                avg_per_node = sum(n for n in per_node_tablet_count.values()) / len(per_node_tablet_count)
                tolerance = 1
                for host_id, count in per_node_tablet_count.items():
                    assert abs(count - avg_per_node) <= tolerance
                    avg_per_shard = count / len(tablet_count[host_id])
                    for shard_tablets_count in tablet_count[host_id].values():
                        assert abs(shard_tablets_count - avg_per_shard) <= tolerance

            logger.debug(f"Verify tablet load-balancing for nodes={nodes}")
            tablet_count = get_tablet_count(nodes[0])
            logger.debug(f"tablet_count={tablet_count}")
            for node in nodes:
                host_id = str(node.hostid())
                assert host_id in tablet_count
                assert len(tablet_count[host_id]) == node._smp
            verify_nodes_tablet_count(nodes)

        verify_load_balancing(old_nodes)

        node4 = new_node(cluster, data_center=node1.data_center, rack=node1.rack)
        node5 = new_node(cluster, data_center=node1.data_center, rack=node2.rack)
        node6 = new_node(cluster, data_center=node1.data_center, rack=node3.rack)
        new_nodes = [node4, node5, node6]
        all_nodes = cluster.nodelist()

        logger.debug("Starting node4 and node5 in parallel")
        for node in new_nodes:
            node.start(no_wait=True)

        for node in all_nodes:
            other_nodes = [n for n in all_nodes if n is not node]
            for other in other_nodes:
                node.watch_rest_for_alive(other)
                other.watch_rest_for_alive(node)

        logger.debug("Check that nodes started successfully")
        for node in new_nodes:
            node.watch_log_for("Starting listening for CQL clients")

        logger.debug(f"Waiting until tablet load balancing finishes")
        run_rest_api(node1, api_method="POST", cmd="/storage_service/quiesce_topology")
        verify_load_balancing(cluster.nodelist())

        logger.debug("Inserting more data")
        with self.patient_exclusive_cql_connection(node4) as session:
            session.execute(f"use {keyspace}")
            insert_c1c2(session, keys=range(num_keys, 2 * num_keys), consistency=ConsistencyLevel.ALL)

            logger.debug("Verifying...")
            query = SimpleStatement(f"SELECT * FROM {keyspace}.{table}", consistency_level=ConsistencyLevel.ONE)
            result = session.execute(query)
            assert len(list(result)) == 2 * num_keys

    def wait_for_node_streaming(self, node, keyspace=".*", tablets=False):
        exprs = [bootstrap_start_log_pat]
        if tablets:
            exprs.append(f"Tablet migration with .* for keyspace={keyspace} started")
        else:
            exprs.append(f"Beginning stream session|sync data for keyspace={keyspace}, status=started")
        log_timeout = 600
        if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode == "debug":
            log_timeout *= 3
        found = node.watch_log_for(exprs, timeout=log_timeout)
        logger.debug(f"Bootstrap {node.name}: started streaming/repair: {found}")

    def test_simple_restart_streaming_node_while_bootstrapping(self):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a three racks with 1 node in each and rf=3, insert data
        2. Add node to working rack (rack2), wait for node to start bootstrapping
        3. Kill original cluster node in working rack (rack2) while it is streaming info to the new node
        4. Check that the new node fails bootstrap
        5. Check restarting of the node previously killed
        6. Check that the node that failed too bootstrap is able to join the cluster after being wiped-out.
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values=self.default_config_options(), batch_commitlog=True)
        racks = rf = 3
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=racks, nodes_per_rack=1)
        cluster.populate(cluster_topology).start()
        node1, node2, _ = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", rf)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})
        num_keys = 1000
        insert_c1c2(session, keys=range(num_keys), consistency=ConsistencyLevel.THREE)

        logger.debug("Inserting more data to make streaming process longer...")
        create_ks(session, "ks1", rf, tablets=0)
        create_cf(session, "ks1.cf1", read_repair=0.0, columns={"c1": "text", "c2": "text"})
        num_additional_keys = 10000 if cluster.scylla_mode == "debug" else 100000
        insert_c1c2(session, keys=range(num_additional_keys), consistency=ConsistencyLevel.THREE, ks="ks1", cf="cf1")

        logger.debug("Flush cluster...")
        self.cluster.flush()

        logger.debug("Start node 4...")
        node4 = new_node(cluster, data_center=node1.data_center, rack=node2.rack)
        node4.start(jvm_args=["--logger-log-level", "stream_session=debug"], no_wait=True)
        self.wait_for_node_streaming(node4, "ks1")
        node4_host_id = node4.hostid()

        self.ignore_log_patterns += [
            rf"[Rr]epair.*mandatory neighbor={node2.address()} is not alive",
            r"Startup failed:.*Failed to repair for keyspace=ks[1-3]?",
            r"Startup failed: std::runtime_error[ :]+.*\(?repair .* failed",
            r"Startup failed: std::runtime_error[ :]+.*rpc::closed_error",
            r"Startup failed: seastar::sleep_aborted",
            r"Startup failed: std::runtime_error[ :]+.*ranges failed",
            r"Startup failed: std::runtime_error.*Failed stream ranges",
            rf"stream_session .* Failed to handle STREAM_MUTATION_FRAGMENTS .* peer={node2.address()}",
            r"storage_service .* fail to update tokens for .*: exceptions::mutation_write_failure_exception",
            r"storage_service .* Operation failed",
            r"Abort bootstrap operation",
            r"Startup failed: seastar::rpc::closed_error",
            r"Startup failed: streaming::stream_exception[ :]+\(?Stream failed\)?",
            r"bootstrap.* failed.* std::runtime_error[ :]+.*(rpc::closed_error|repair.*failed)",
            r"raft_topology - send_raft_topology_cmd\(stream_ranges\) failed with exception \(node state is bootstrapping\)",
            r"raft_topology - drain rpc failed, proceed to fence old writes",
            r"raft_topology - transition_state::left_token_ring, raft_topology_cmd::command::barrier failed",
            r"raft_topology - raft_topology_cmd.*failed",
        ]

        logger.debug("Stop node 2...")
        node2.stop(gently=False)

        logger.debug("Look for Stream/Startup failed in node 4...")
        # The keep alive timer expires in 10 minutes.
        # Wait 5 minutes more in the test to wait for the stream to fail
        log_timeout = 900
        if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode == "debug":
            log_timeout *= 2
        node4.watch_log_for("Stream failed|Startup failed|sync data for keyspace=ks[1-3]?, status=failed", timeout=log_timeout)
        node4.stop()

        nodes = self.cluster.nodelist()[0:-1]
        marks = [(node, node.mark_log()) for node in nodes]

        logger.debug("Make sure that the killed node(s) can be restarted")
        cluster.start_nodes(nodes, wait_other_notice=True)

        logger.debug("Make sure that the cluster ignores the failed-to-bootstrap node")
        for node in nodes:
            wait_for_nodes_status(node, ["UN"] * len(nodes))

        if "consistent-topology-changes" not in self.scylla_features:
            logger.debug(f"Waiting for {node4.address()} gossip quarantine over")
            for node, mark in marks:
                node.watch_log_for(f"({node4.address()}|{node4_host_id}) gossip quarantine over", from_mark=mark)

        logger.debug(f"Wiping out and restarting {node4.name}")
        wipe_node_data_directories(node4)
        node4.start(wait_for_binary_proto=True, wait_other_notice=True)

        logger.debug(f"Verifying data")
        for key in range(num_keys):
            query_c1c2(session, key, ks="ks", cf="cf")
        for key in range(num_additional_keys):
            query_c1c2(session, key, ks="ks1", cf="cf1")

    @pytest.mark.use_cassandra_stress
    # FIXME: https://github.com/scylladb/scylla-dtest/issues/5310
    @pytest.mark.cluster_options(enable_small_table_optimization_for_rbno=False)
    def test_simple_kill_new_node_while_bootstrapping(self):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a three racks with 1 node in each rack with rf=1, insert data
        2. Add node to random rack, wait for each to start bootstrapping and kill it
        3. Add node to random rack, wait for each to start bootstrapping and kill it
        4. Check that the cluster returns all
        """

        # this error is expected in teardown after this test in raft topology mode
        ignore_error = r"raft_topology - send_raft_topology_cmd\(stream_ranges\) failed with exception \(node state is bootstrapping\)"
        self.ignore_log_patterns += [ignore_error]
        racks = rf = 3
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=racks, nodes_per_rack=1)
        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        # This test relies on bootstrap taking long time, so keep RBNO enabled for bootstrap
        cluster.set_configuration_options(values=self.repair_based_node_ops_config_options(True), batch_commitlog=True)
        cluster.populate(cluster_topology).start()
        node1 = cluster.nodelist()[0]
        node2 = cluster.nodelist()[1]
        node3 = cluster.nodelist()[2]

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 1)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ONE)

        logger.debug("Inserting more data to make streaming process longer...")
        node1.stress(["write", "n=5000", "no-warmup", "-schema", f"replication(factor={rf}) keyspace=ks1"])
        node2.stress(["write", "n=5000", "no-warmup", "-schema", f"replication(factor={rf}) keyspace=ks2"])
        node3.stress(["write", "n=5000", "no-warmup", "-schema", f"replication(factor={rf}) keyspace=ks3"])

        for i in range(4, 5):
            # creating an additional node without actually adding it to the cluster
            new_node = cluster.new_node(i, auto_bootstrap=True, add_node=False)
            rack = cluster.nodelist()[i % racks].rack
            cluster.add(new_node, is_seed=True, data_center=node1.data_center, rack=rack)
            logger.debug("Start Node %d" % i)
            new_node.start(jvm_args=["--logger-log-level", "stream_session=debug"], no_wait=True)
            # wait for any keyspace streaming to make test more stable
            self.wait_for_node_streaming(new_node, keyspace=".*")

            if "consistent-topology-changes" in self.scylla_features:
                wait_for_nodes_status(node1, ["UN", "UN", "UN", "UJ"])

            logger.debug("Stop Node %d" % i)
            new_node.stop(gently=False)

            if "consistent-topology-changes" not in self.scylla_features:
                # Sleep 1 second to make sure other nodes knows this node is joining through gossip
                time.sleep(1)

                # Check the status:
                # We expect the new node will not be added to the cluster
                # status looks like below, new_node should not be in UN state but in UJ state.
                # UN  127.0.0.1  99823      256     ?       a7498138-1878-421d-8f11-cc98b204090a  rack1
                # UN  127.0.0.2  37278      256     ?       f118383c-c569-49d1-9aa6-223d3b224caa  rack1
                # UN  127.0.0.3  24834      256     ?       78b7e6ba-3039-4fc6-a875-a71661f8cd04  rack1
                # UJ  127.0.0.4  ?          256     ?       637edd3f-8888-48ab-b0ea-3ea81f8e9865  rack1
                wait_for_nodes_status(node1, ["UN", "UN", "UN", "UJ"])

                node1.watch_log_for("FatClient .* has been silent for .*ms, removing from gossip")
                node2.watch_log_for("FatClient .* has been silent for .*ms, removing from gossip")
                node3.watch_log_for("FatClient .* has been silent for .*ms, removing from gossip")

            # Check status again:
            # status looks like below, new_node should not be in UN state
            # UN  127.0.0.1  99823      256     ?       a7498138-1878-421d-8f11-cc98b204090a  rack1
            # UN  127.0.0.2  37278      256     ?       f118383c-c569-49d1-9aa6-223d3b224caa  rack1
            # UN  127.0.0.3  24834      256     ?       78b7e6ba-3039-4fc6-a875-a71661f8cd04  rack1
            wait_for_nodes_status(node1, ["UN", "UN", "UN"])

        result = list(session.execute("SELECT * FROM cf"))
        assert len(result) == 1000

    @pytest.mark.use_cassandra_stress
    # FIXME: https://github.com/scylladb/scylla-dtest/issues/5310
    @pytest.mark.cluster_options(enable_small_table_optimization_for_rbno=False)
    def test_simple_kill_new_node_while_bootstrapping_with_parallel_writes(self):  # noqa: PLR0915
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a three nodes with rf=3, insert data
        2. Add node, wait for each to start bootstrapping and write additional data
        3. kill it while writing data
        4. Check that the operation exists with an expected exception
        """

        # this error is expected in teardown after this test in raft topology mode
        ignore_error = r"raft_topology - send_raft_topology_cmd\(stream_ranges\) failed with exception \(node state is bootstrapping\)"
        self.ignore_log_patterns += [ignore_error]

        cluster = self.cluster
        racks = rf = 3
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=racks, nodes_per_rack=1)
        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1 = cluster.nodelist()[0]
        node2 = cluster.nodelist()[1]
        node3 = cluster.nodelist()[2]

        session = self.cql_connection(node1)
        create_ks(session, "ks", rf)
        create_c1c2_table(session)

        keys = 1000
        insert_c1c2(session, keys=range(keys), consistency=ConsistencyLevel.ALL)

        logger.debug("Inserting more data to make streaming process longer...")
        node1.stress(["write", "n=5000", "no-warmup", "-schema", f"replication(factor={rf}) keyspace=ks1"])
        node2.stress(["write", "n=5000", "no-warmup", "-schema", f"replication(factor={rf}) keyspace=ks2"])
        node3.stress(["write", "n=5000", "no-warmup", "-schema", f"replication(factor={rf}) keyspace=ks3"])

        for i in range(4, 5):
            # creating an additional node without actually adding it to the cluster
            new_node = cluster.new_node(i, auto_bootstrap=True, add_node=False)
            rack = cluster.nodelist()[i % racks].rack
            cluster.add(new_node, True, data_center=node1.data_center, rack=rack)
            failed = None

            def run(stop_run):
                nonlocal keys, failed
                logger.debug("start write")
                while not stop_run.is_set():
                    # working around the default retry_policy that attempts 5 times
                    statement = SimpleStatement("INSERT INTO cf (key, c1, c2) VALUES ('k%d', 'value1', 'value2')" % keys, consistency_level=ConsistencyLevel.QUORUM, retry_policy=FallthroughRetryPolicy())
                    tbefore = str(datetime.now())
                    try:
                        session.execute(statement)
                        keys += 1
                    except Unavailable as e:
                        tfailed = str(datetime.now())
                        logger.debug(f"exception thrown Unavailable {e}")
                    except WriteTimeout as e:
                        tfailed = str(datetime.now())
                        logger.debug(f"exception thrown WriteTimeout {e}")
                    except OperationTimedOut as e:
                        tfailed = str(datetime.now())
                        failed = f"Server side exception not thrown  driver side exception thrown OperationTimeout {e} {tbefore} {tfailed}"
                        logger.debug(failed)
                logger.debug("end write")

            executor = ThreadPoolExecutor(max_workers=1)
            stop_run = Event()
            t = executor.submit(run, stop_run)
            logger.debug("Start Node %d" % i)
            new_node.start(jvm_args=["--logger-log-level", "stream_session=debug"], no_wait=True)
            # wait for any keyspace streaming to make test more stable
            self.wait_for_node_streaming(new_node, keyspace=".*")
            logger.debug("Stop Node %d" % i)
            new_node.stop(gently=False, wait_other_notice=True)
            for node in [node1, node2, node3]:
                wait_for_nodes_status(node, [["UN", "UN", "UN"], ["UN", "UN", "UN", "DN"]])
            stop_run.set()
            t.result()
            assert failed is None

            for node in [node1, node2, node3]:
                status = nodetool_status(node, "ks")
                logger.debug(f"nodetool status from {node.name}: {status}")

            logger.debug("Query Again")
            query = SimpleStatement("SELECT * FROM cf", consistency_level=ConsistencyLevel.QUORUM)
            rows = list(session.execute(query))
            assert len(rows) >= keys and len(rows) <= keys + 1, f"Expected between {keys} and {keys + 1} rows, but got {len(rows)}"

    @pytest.mark.use_cassandra_stress
    # FIXME: https://github.com/scylladb/scylla-dtest/issues/5310
    @pytest.mark.cluster_options(enable_small_table_optimization_for_rbno=False)
    def test_simple_kill_new_node_while_bootstrapping_with_parallel_writes_in_multidc(self):  # noqa: PLR0915
        """
        Test bootstrapped node streams all data
        1. Create a multidc cluster with 1 rack per dc and a keyspace with rf=1 in dc, insert data
        2. Add node, wait for each to start bootstrapping and write additional data
        3. kill it
        4. Check that the cluster returns all data
        """

        # this error is expected in teardown after this test in raft topology mode
        ignore_error = r"raft_topology - send_raft_topology_cmd\(stream_ranges\) failed with exception \(node state is bootstrapping\)"
        self.ignore_log_patterns += [ignore_error]

        cluster = self.cluster
        cluster_topology = {"dc1": {"rack1": 1}, "dc2": {"rack1": 1}}

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1 = cluster.nodelist()[0]
        node2 = cluster.nodelist()[1]

        session = self.cql_connection(node1)
        create_ks(session, "ks", {"dc1": 1, "dc2": 1})
        create_c1c2_table(session)

        keys = 1000
        insert_c1c2(session, keys=range(keys), consistency=ConsistencyLevel.EACH_QUORUM)

        logger.debug("Inserting more data to make streaming process longer...")

        def stress_options(rf, ks):
            return ["write", "n=5000", "no-warmup", "-schema", f"replication(factor={rf}) keyspace={ks}"]

        node1.stress(stress_options(1, "ks1"))
        node2.stress(stress_options(1, "ks2"))

        # create a new node and adding it - we cannot do this more then once
        a_new_node = new_node(cluster, data_center=node2.data_center, rack=node2.rack)
        failed = None

        def run(stop_run):
            nonlocal keys, failed

            logger.debug("start write")
            while not stop_run.is_set():
                # working around the default retry_policy that attempts 5 times
                statement = SimpleStatement("INSERT INTO cf (key, c1, c2) VALUES ('k%d', 'value1', 'value2')" % keys, consistency_level=ConsistencyLevel.EACH_QUORUM, retry_policy=FallthroughRetryPolicy())
                tbefore = str(datetime.now())
                try:
                    session.execute(statement)
                    keys += 1
                except Unavailable as e:
                    tfailed = str(datetime.now())
                    logger.debug(f"exception thrown Unavailable {e}")
                except WriteTimeout as e:
                    tfailed = str(datetime.now())
                    logger.debug(f"exception thrown WriteTimeout {e}")
                except OperationTimedOut as e:
                    tfailed = str(datetime.now())
                    failed = f"Server side exception not thrown driver side exception thrown OperationTimeout {e} {tbefore} {tfailed}"
                    logger.debug(failed)
            logger.debug("end write")

        executor = ThreadPoolExecutor(max_workers=1)
        stop_run = Event()
        t = executor.submit(run, stop_run)
        logger.debug("Start Node")
        a_new_node.start(jvm_args=["--logger-log-level", "stream_session=debug"], no_wait=True)
        # wait for any keyspace streaming to make test more stable
        self.wait_for_node_streaming(a_new_node, keyspace=".*")
        logger.debug("Stop Node")
        a_new_node.stop(gently=False, wait_other_notice=True)
        wait_for_nodes_status(node1, [["UN", "UN"], ["UN", "UN", "DN"]])
        stop_run.set()
        t.result()
        assert failed is None

        for node in [node1, node2]:
            status = nodetool_status(node, "ks")
            logger.debug(f"nodetool status from {node.name}: {status}")

        logger.debug("Query Again")
        query = SimpleStatement("SELECT * FROM cf", consistency_level=ConsistencyLevel.QUORUM)
        rows = list(session.execute(query))
        assert len(rows) >= keys and len(rows) <= keys + 1, f"Expected between {keys} and {keys + 1} rows, but got {len(rows)}"

    @pytest.mark.parametrize(
        "rf, bootstrap_method",
        [
            (1, "streaming"),
            (1, "rbno"),
            (2, "streaming"),
            (2, "rbno"),
        ],
    )
    def test_add_new_node_while_adding_data(self, rf: int, bootstrap_method: str):
        """
        Test bootstrapped node streams all data

        1. Create a cluster with 3 nodes in 2 racks, insert data
        3. Add node, while node is bootstrapping insert data
        4. Check that the cluster returns all
        """
        cluster = self.cluster

        if "force_gossip_topology_changes" in cluster._config_options and bootstrap_method == "streaming":
            cluster.set_configuration_options({"skip_wait_for_gossip_to_settle": -1})

        consistency = {1: ConsistencyLevel.ONE, 2: ConsistencyLevel.TWO}[rf]
        cluster_topology = {"dc1": {"rack1": 1, "rack2": 2}}
        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        config = self.repair_based_node_ops_config_options(bootstrap_method == "rbno", ops=["bootstrap"])
        cluster.set_configuration_options(values=config, batch_commitlog=True)

        num_keys = 4000
        cluster.populate(cluster_topology).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", rf)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        insert_c1c2(session, keys=range(num_keys // 2), consistency=consistency)

        logger.debug("Adding a node")
        node4 = new_node(cluster, data_center=node1.data_center, rack=node1.rack)
        node4.start(jvm_args=["--logger-log-level", "stream_session=debug"], no_wait=True)
        self.wait_for_node_streaming(node4, keyspace=".*")

        logger.debug("Inserting more data")
        insert_c1c2(session, keys=range(num_keys // 2, num_keys), consistency=consistency)

        logger.debug("Waiting for new node to start listenting for CQL")
        node4.wait_for_binary_interface()

        logger.debug("Verifying data")
        query = SimpleStatement("SELECT * FROM cf", consistency_level=consistency)
        result = list(session.execute(query))
        assert len(result) == num_keys

        for k in range(num_keys):
            query_c1c2(session, k, consistency)

    def repair_based_node_ops_config_options(self, enable_repair_based_node_ops: bool, ops=None):
        if ops is None:
            ops = ["bootstrap", "replace", "removenode", "decommission", "rebuild"]
        config_options = self.default_config_options()
        config_options.update({"enable_repair_based_node_ops": enable_repair_based_node_ops})
        # Configuring allowed_repair_based_node_ops is required
        # since scylladb/scylla@97bb2e47ff004b32b2d72f1b1f085710a14cb4e2
        if enable_repair_based_node_ops and Version(self.cluster.version()) >= Version("4.6.dev"):
            config_options.update({"allowed_repair_based_node_ops": ",".join(ops)})
        return config_options

    @pytest.mark.parametrize("enable_repair_based_node_ops", [False, True])
    def test_simple_add_new_node_while_schema_changes(self, enable_repair_based_node_ops: bool):
        """
        Test bootstrapped node sync all data

        1. Create a cluster with three nodes in 1 rack with rf=1, insert data
        2. Add node, while node is bootstrapping remove keyspace
        3. Still while bootstrapping add a keyspace and insert data
        4. Check that node was connected and the cluster returns all
        """
        cluster = self.cluster
        rf = 1
        cluster_topology = {"datacenter1": {"rack1": 3}}
        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        config_options = self.repair_based_node_ops_config_options(enable_repair_based_node_ops)
        cluster.set_configuration_options(values=config_options, batch_commitlog=True)
        cluster.populate(cluster_topology).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        # Disable tablets for the initial keyspace,
        # otherwise, it will be streamed as part of tablets load balancing
        # that happen after bootstrap is already done,
        # defeating the purpose of this test
        create_ks(session, "ks", rf, tablets=0)
        create_cf(session, "ks.cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        insert_c1c2(session, keys=range(4000), consistency=ConsistencyLevel.ONE)

        def run(min_keys, stop_run):
            query = SimpleStatement("DROP KEYSPACE ks")
            list(session.execute(query))

            create_ks(session, "ks1", rf)
            create_cf(session, "ks1.cf1", read_repair=0.0, columns={"c1": "text", "c2": "text"})
            for i in range(min_keys) or not stop_run.is_set():
                insert = SimpleStatement("insert into ks1.cf1 (key,c1,c2) values ('%d','%d','%d')" % (i, i, i), consistency_level=ConsistencyLevel.ONE)
                session.execute(insert)

        executor = ThreadPoolExecutor(max_workers=2)

        logger.debug("Adding new node")
        node4 = new_node(cluster, data_center=node1.data_center, rack=node1.rack)

        def start_in_background(node):
            node.start(jvm_args=["--logger-log-level", "stream_session=debug"], wait_other_notice=True)
            return self.patient_cql_connection(node)

        node4_start_thread = executor.submit(start_in_background, node4)
        self.wait_for_node_streaming(node4)

        self.ignore_log_patterns += [
            r"ks=ks, cf=cf, .*no_such_column_family",
            r"raft_topology - .*node state is bootstrapping",
            r"no_such_keyspace.*\(Can't find a keyspace ks\)",
            "Startup failed",
        ]

        min_keys = 100
        stop_run = Event()
        t = executor.submit(run, min_keys, stop_run)

        if enable_repair_based_node_ops:
            bootstrap_success_msg = "completed successfully, keyspace=ks"
            bootstrap_failed_msg = r"sync data for keyspace=ks, status=failed: keyspace does not exist any more|no_such_keyspace.*\(Can't find a keyspace ks\)"
            bootstrap_skipped_msg = r"keyspace=ks does not exist any more, ignoring it|keyspace ks does not exist, skipping"
            node4.watch_log_for(f"{bootstrap_success_msg}|{bootstrap_failed_msg}|{bootstrap_skipped_msg}")

        # Wait for node4 to start, and other notice to notice it
        session = node4_start_thread.result()

        stop_run.set()
        t.result()

        logger.debug("Verify keyspace ks is deleted")
        assert_invalid(session, "SELECT * FROM ks.cf", "Keyspace ks does not exist")

        logger.debug("Verify data on ks1")
        query = SimpleStatement("SELECT * FROM ks1.cf1", consistency_level=ConsistencyLevel.ONE)
        result = list(session.execute(query))
        assert len(result) >= min_keys, f"Expected at least {min_keys} items in table"

    @pytest.mark.parametrize("rf", [1, 2])
    def test_simple_add_new_node_while_query_info(self, rf):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with num of racks == rf, insert data
        3. Add node, while node is bootstrapping query data
        4. Check that the cluster returns all
        """
        cluster = self.cluster
        consistency = {1: ConsistencyLevel.ONE, 2: ConsistencyLevel.TWO}[rf]
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=rf, nodes_per_rack=1)
        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(cluster_topology).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", rf)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        num_keys = 2000
        insert_c1c2(session, keys=range(num_keys), consistency=consistency)

        def run(stop_run):
            query = session.prepare("SELECT * FROM cf")
            query.consistency_level = consistency
            logger.debug("Background SELECT loop starting")
            while not stop_run.is_set():
                result = list(session.execute(query))
                assert len(result) == num_keys
                time.sleep(0.01)
            logger.debug("Background SELECT loop done")

        executor = ThreadPoolExecutor(max_workers=1)

        logger.debug("Adding new node")
        node4 = new_node(cluster, data_center=node1.data_center, rack=node1.rack)
        node4.start(jvm_args=["--logger-log-level", "stream_session=debug"], no_wait=True)
        self.wait_for_node_streaming(node4)

        stop_run = Event()
        t = executor.submit(run, stop_run)

        node4.watch_log_for("Starting listening for CQL clients")
        stop_run.set()
        t.result()

        logger.debug("Verifying data")
        query = SimpleStatement("SELECT * FROM cf", consistency_level=consistency)

        result = list(session.execute(query))
        assert len(result) == num_keys
        for k in range(num_keys):
            query_c1c2(session, k, consistency)

    def test_simple_decommission_node_1(self):
        """
        Test decommissioned node streams all data
        1. Create a cluster with 2 racks with rf=1, insert data
        2. Decommission one node
        3. Check that the last node has all the data
        """
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=2)
        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 1)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ONE)

        node2.decommission()
        # lets verify new connection can not be openned to a decomissioned node
        with pytest.raises(NoHostAvailable):
            self.patient_cql_connection(node2)
        node2.stop()

        self.check_rows_on_node(node1, 1000, restart=False)

    def test_simple_decommission_node_2(self):
        """
        Test that on decommission row cache entries of non owned transfered range are invalidated

        1. Create a cluster with a single node with rf=1,insert data
        2. Check the row cache can be used as an estimator
        3. Add a new node
        4. Cleanup data on original node (cache not cleared)
        5. Check that all data can be read
        6. Delete all the data
        7. Compact data on new node
        8. Restart new node (it should not have any data including tombstones)
        9. Decommission the new node
        10. Test if any data exists in the cluster
        """

        cluster = self.cluster
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=1)
        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(cluster_topology).start()
        node1 = cluster.nodelist()[0]

        session_node1 = self.patient_cql_connection(node1)
        create_ks(session_node1, "ks", 1)
        create_cf(session_node1, "cf", gc_grace=0, read_repair=0.0, columns={"c1": "text", "c2": "text"})

        node1.flush()

        insert_c1c2(session_node1, keys=range(1000), consistency=ConsistencyLevel.ONE)

        node1.flush()

        # We booted the new node and it got part of the items
        node2 = new_node(cluster, data_center=node1.data_center, rack=node1.rack)
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)
        node1.cleanup()

        session_node2 = self.patient_exclusive_cql_connection(node2)
        session_node2.execute("use ks;")
        node1.watch_log_for_alive(node2)
        node2.watch_log_for_alive(node1)

        result = list(session_node1.execute("SELECT * FROM ks.cf limit 2000;"))
        assert len(result) == 1000, "expected 1000 lines got %d" % len(result)

        for i in range(0, 1000, 100):
            session_node1.execute("DELETE from ks.cf where key in ('k%s');" % "','k".join(str(x) for x in range(i, i + 100)))

        result = list(session_node1.execute("SELECT * FROM ks.cf limit 2000;"))
        assert len(result) == 0, "expected 0 lines got %d %s" % (len(result), result)

        cluster.flush()
        # lets make sure all the data in ssstables is removed
        node2.compact()
        # restart the node to make sure no data is left
        node2.stop()
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)

        node2.decommission()
        node1.flush()

        result = list(session_node1.execute("SELECT * FROM ks.cf"))
        assert len(result) == 0, "expected 0 lines got %d" % len(result)

    # FIXME: https://github.com/scylladb/scylla-dtest/issues/5310
    @pytest.mark.cluster_options(enable_small_table_optimization_for_rbno=False)
    def test_simple_kill_node_while_decommissioning(self):
        """
        Test a decommissioning node killed is able to rejoin the cluster with data
        1. Create a cluster with 1 rack and 3 nodes with rf=1, insert data
        2. Decommission a node
        3. While node is decommissioning kill it
        4. Boot the node back up
        5. Check that the node rejoins the cluster and works correctly
        """
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=3)
        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True, jvm_args=["--logger-log-level", "stream_session=debug"])
        node1, node2, _ = cluster.nodelist()

        with self.cql_cluster_session(node1) as session:
            create_ks(session, "ks", 1)
            create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

            insert_c1c2(session, keys=range(10000), consistency=ConsistencyLevel.ONE)

        executor = ThreadPoolExecutor(max_workers=1)
        mark = node2.mark_log()
        decomission_thread = executor.submit(node2.decommission)

        self.ignore_log_patterns += [
            r"seastar::rpc::closed_error[ :]+\(?connection is closed\)?",
        ]

        # check node2 has started decommission stream of any keyspace
        node2.watch_log_for("Beginning stream session|sync data for keyspace=.*, status=started", from_mark=mark)

        self.ignore_log_patterns += [f"Failed to handle STREAM_MUTATION_FRAGMENTS.*peer={node2.address()}"]

        logger.debug("Stop node2 ")
        node2.stop(gently=False)

        logger.debug("wait for decommission to fail")
        with pytest.raises(NodetoolError):
            decomission_thread.result(timeout=120)

        # starting node2 - it should reconnect and run as is
        logger.debug("Start node2 ")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        with self.cql_cluster_session(node2) as session2:
            # stabilize the flakiness of  cassandra.OperationTimedOut error
            result = list(session2.execute("SELECT * FROM ks.cf", timeout=60))
            assert len(result) == 10000

        verify_nodes_status(node1, ["UN", "UN", "UN"])

    # FIXME: https://github.com/scylladb/scylla-dtest/issues/5310
    @pytest.mark.cluster_options(enable_small_table_optimization_for_rbno=False)
    def test_simple_kill_remained_node_while_decommissioning(self):
        """
        Test a decommissioning node killed is able to rejoin the cluster with data
        1. Create a cluster with a three nodes with rf=1, insert data
        2. Decommission a node
        3. While node is decommissioning kill another one
        4. Boot the node back up
        5. Check that the node rejoins the cluster and works correctly
        """
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=3)
        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values=self.repair_based_node_ops_config_options(True), batch_commitlog=True)
        cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True, jvm_args=["--logger-log-level", "stream_session=debug"])
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 1)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})
        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ONE)
        mark = node2.mark_log()

        def run():
            try:
                node2.decommission()
            except Exception:  # noqa: BLE001
                pass

        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(run)

        # check node2 has started decommission stream of any keyspace
        node2.watch_log_for("Beginning stream session|sync data for keyspace=.*, status=started", from_mark=mark)
        verify_nodes_status(node1, ["UN", "UL", "UN"])

        node1.stop(gently=False)
        #  wait for node 1 marked as Down for other nodes and Decommission failed(node 2 marked as Normal)
        wait_for_nodes_status(node2, ["DN", "UN", "UN"])
        wait_for_nodes_status(node3, ["DN", "UN", "UN"])

        self.ignore_log_patterns += [
            "decommission.*Operation failed",
            "raft_topology - Decommission failed.",
            r"raft_topology - raft_topology_cmd.*failed with: raft::request_aborted[ :]+\(?Request is aborted by a caller\)?",
            r"raft_topology - send_raft_topology_cmd\(stream_ranges\) failed with exception \(node state is decommissioning\)",
            r"raft_topology - raft_topology_cmd.*failed with: (?:seastar::abort_requested_exception[ :]+\(?abort requested\)?|abort requested)",
            r"seastar::rpc::closed_error[ :]+\(?connection is closed\)?",
        ]

        # starting node1 - it should reconnect and run as is
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        result = list(session.execute("SELECT * FROM cf"))
        assert len(result) == 1000, "Should have 1000 items in table"

    @pytest.mark.parametrize("rf", [1, 2])
    def test_simple_decommission_node_while_adding_info(self, rf):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a 1 dc 2 racks: rack1 1 node, rack2 2 nodes with rf, insert data
        2. Decommission node in rack2, while node is decommissioning insert data
        3. Check that the cluster returns all
        """
        cluster = self.cluster
        consistency = {1: ConsistencyLevel.ONE, 2: ConsistencyLevel.TWO}[rf]
        cluster_topology = {"datacenter1": {"rack1": 1, "rack2": 2}}
        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(cluster_topology).start()
        node1, node2 = cluster.nodelist()[:2]

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", rf)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        insert_c1c2(session, keys=range(2000), consistency=consistency)

        def run():
            insert_c1c2(session, keys=range(2000, 4000), consistency=consistency)

            query = SimpleStatement("SELECT * FROM cf", consistency_level=consistency)
            result = list(session.execute(query))
            assert len(result) == 4000, "should have 4000 items in table"

        executor = ThreadPoolExecutor(max_workers=1)
        t = executor.submit(run)

        node2.decommission()

        t.result()
        node2.stop()
        query = SimpleStatement("SELECT * FROM cf", consistency_level=consistency)
        result = list(session.execute(query))
        assert len(result) == 4000, "should have 4000 items in table"
        for k in range(4000):
            query_c1c2(session, k, consistency)

    @pytest.mark.required_features("consistent-topology-changes")  # scylladb/scylladb#17903
    @pytest.mark.parametrize("rf", [1, 2])
    def test_simple_decommission_node_while_query_info(self, rf):
        """
        Test decommissioning node streams all data
        1. Create a cluster with a 1 dc 2 racks: rack1 1 node, rack2 2 nodes with rf, insert data
        2. Decommission node, while node is decommissioning query data
        3. Check that the cluster returns all
        """
        cluster = self.cluster
        consistency = {1: ConsistencyLevel.ONE, 2: ConsistencyLevel.TWO}[rf]
        cluster_topology = {"datacenter1": {"rack1": 1, "rack2": 2}}

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(cluster_topology).start()
        node1, node2 = cluster.nodelist()[:2]

        ep = ExecutionProfile(speculative_execution_policy=ConstantSpeculativeExecutionPolicy(delay=5, max_attempts=10))
        session = self.patient_cql_connection(node1, execution_profiles={"speculative": ep})
        create_ks(session, "ks", rf)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        num_keys = 2000
        insert_c1c2(session, keys=range(num_keys), consistency=consistency)

        def run(stop_run):
            logger.debug("Background SELECT loop starting")
            query = SimpleStatement("SELECT * FROM cf", is_idempotent=True)
            query.consistency_level = consistency
            while not stop_run.is_set():
                # stabilize the flakiness of  cassandra.OperationTimedOut error
                result = list(session.execute(query, timeout=60, execution_profile="speculative"))
                assert len(result) == num_keys
                time.sleep(0.01)
            logger.debug("Background SELECT loop done")

        executor = ThreadPoolExecutor(max_workers=1)
        stop_run = Event()
        t = executor.submit(run, stop_run)

        logger.debug("Decommissioning node2")
        node2.decommission()

        stop_run.set()
        t.result()

        logger.debug("Verifying data")
        query = SimpleStatement("SELECT * FROM cf", consistency_level=consistency)
        result = list(session.execute(query))
        assert len(result) == num_keys

        logger.debug("Stopping node2")
        node2.stop()

        logger.debug("Verifying data")
        query = SimpleStatement("SELECT * FROM cf", consistency_level=consistency)
        result = list(session.execute(query))
        assert len(result) == num_keys
        for k in range(num_keys):
            query_c1c2(session, k, consistency)

    def _prepare_cluster(self, cluster_topology: dict[str, dict[str, int]], rf: int, keys_range: int) -> list[ScyllaNode]:
        cluster = self.cluster
        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False}, batch_commitlog=True)
        cluster.populate(cluster_topology).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", rf)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        insert_c1c2(session, keys=range(keys_range), consistency=ConsistencyLevel.ALL)
        return cluster.nodelist()

    def test_simple_removenode_1(self):
        """
        Test removenode with rf>1 (no data should be lost)
        1. Create a cluster with a two racks and 2 nodes in each rack with rf=2, insert data
        2. stop nodes in one rack and remove a node
        3. Check that the data is accessible
        """
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=2, nodes_per_rack=2)
        node1, _, node3, node4 = self._prepare_cluster(cluster_topology, rf=2, keys_range=100)
        session = self.patient_cql_connection(node1)

        node3_hostid = node3.hostid()
        # stop nodes in rack2 and check that nodes in rack1 has all data
        node3.stop(wait_other_notice=True)
        node4.stop(wait_other_notice=True)
        query = SimpleStatement("SELECT * FROM ks.cf", consistency_level=ConsistencyLevel.ONE)
        result = list(session.execute(query))
        assert len(result) == 100
        # Start one node in rack2 to remove node from rack
        node4.start(wait_other_notice=True)
        node1.nodetool(f"removenode {node3_hostid}")
        time.sleep(2)
        query = SimpleStatement("SELECT * FROM ks.cf", consistency_level=ConsistencyLevel.TWO)
        result = list(session.execute(query))
        assert len(result) == 100
        insert_c1c2(session, keys=range(120), consistency=ConsistencyLevel.TWO)

    def test_simple_removenode_2(self):
        """
        Test removenode when rf=1 (data will be lost)
        1. Create a cluster with a three node with rf=1, insert data
        2. stop and remove a node
        3. Check that the data is accessible
        """
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=3)
        node1, node2, _ = self._prepare_cluster(cluster_topology, rf=1, keys_range=10)
        session = self.patient_cql_connection(node1)
        node2_hostid = node2.hostid()
        node2.stop(wait_other_notice=True)
        try:
            query = SimpleStatement("SELECT * FROM ks.cf", consistency_level=ConsistencyLevel.ONE)
            result = list(session.execute(query))
        except Unavailable:
            pass

        node1.nodetool(f"removenode {node2_hostid}")
        insert_c1c2(session, keys=range(10), consistency=ConsistencyLevel.ALL)
        query = SimpleStatement("SELECT * FROM ks.cf", consistency_level=ConsistencyLevel.ONE)
        result = list(session.execute(query))
        assert len(result) == 10

    @staticmethod
    def _run_removenode_api(run_on_node: ScyllaNode, remove_node_hostid: str, ignore_hostids: list | None = None):
        api_cmd = f"http://{run_on_node.address()}:{run_on_node.api_port}/storage_service/remove_node/?host_id={remove_node_hostid}"
        if ignore_hostids:
            ignore_nodes_ids = ",".join(ignore_hostids)
            api_cmd += f"&ignore_nodes={ignore_nodes_ids}"

        logger.debug("Send restful api: " + api_cmd)
        r = requests.post(api_cmd)
        logger.debug(r.text)
        if not ignore_hostids:
            assert r.status_code != requests.codes.ok
        else:
            r.raise_for_status()
            logger.debug("Node2 is removed from the cluster")

    def _run_removenode_nodetool(self, run_on_node: ScyllaNode, remove_node_hostid: str, ignore_hostids: list | None = None):
        options = ""
        if ignore_hostids:
            ignore_nodes_ids = ",".join(ignore_hostids)
            options = f"--ignore-dead-nodes {ignore_nodes_ids}"
        try:
            run_on_node.nodetool(f"removenode {options} {remove_node_hostid}")
            logger.debug("Node2 is removed from the cluster")
        except NodetoolError as exc:
            if not ignore_hostids:
                if self.nodetool_removenode_error_message not in exc.stdout and self.nodetool_removenode_error_message not in exc.stderr:
                    raise
                logger.debug(
                    "Nodes={127.0.28.5} needed for removenode operation are down. "
                    "It is highly recommended to fix the down nodes and try again. "
                    "Run with best-effort mode (which might cause data inconsistency), "
                    "run nodetool removenode --ignore-dead-nodes <list_of_dead_nodes> <host_id>."
                )
            else:
                raise

    @property
    def nodetool_removenode_error_message(self):
        """This error message is specific to this test, which calls removenode when a non-ignored node is down."""

        return "removenode: Rejected removenode operation for node"

    @pytest.mark.parametrize(
        "removenode_method, ignore_two_nodes",
        [
            ("api", False),
            ("api", True),
            ("nodetool", False),
            ("nodetool", True),
        ],
    )
    def test_simple_removenode_3(self, removenode_method: str, ignore_two_nodes: bool):
        """
        remove node from rack1 while nodes stopped in different racks: rack2 and rack3
        :param removenode_method: "api" or "nodetool"
        """
        cluster_topology = {"datacenter1": {"rack1": 3, "rack2": 2, "rack3": 2}}
        node1, node2, _node3, node4, node5, *_ = self._prepare_cluster(cluster_topology, rf=3, keys_range=1000)
        session = self.patient_cql_connection(node1)
        node2_hostid = node2.hostid()
        ignore_node_hostids = [node2_hostid]
        node2.stop(wait_other_notice=True)
        query = SimpleStatement("SELECT * FROM ks.cf", consistency_level=ConsistencyLevel.TWO)
        result = list(session.execute(query))
        assert len(result) == 1000, "should have 1000 items in table"

        ignore_node_hostids.append(node5.hostid())
        node5.stop(wait_other_notice=True)
        if ignore_two_nodes:
            ignore_node_hostids.append(node4.hostid())
            node4.stop(wait_other_notice=True)
        self.ignore_log_patterns += [
            "raft_topology - Removenode failed. See earlier errors",
            "raft_topology - raft_topology_cmd.*failed with: raft::request_aborted",
        ]
        # removenode should fail since node2 is down
        if removenode_method == "api":
            self._run_removenode_api(run_on_node=node1, remove_node_hostid=node2_hostid)
        elif removenode_method == "nodetool":
            self._run_removenode_nodetool(run_on_node=node1, remove_node_hostid=node2_hostid)

        # removenode should succeed since we ignore the down node node2
        if removenode_method == "api":
            self._run_removenode_api(run_on_node=node1, remove_node_hostid=node2_hostid, ignore_hostids=ignore_node_hostids)
        elif removenode_method == "nodetool":
            self._run_removenode_nodetool(run_on_node=node1, remove_node_hostid=node2_hostid, ignore_hostids=ignore_node_hostids)

    def _kill_node_thread(self, to_kill_coordinator: bool, marks: dict[ScyllaNode, int]):
        logger.debug("kill node thread")
        node1, node2, node3, node4, node5 = list(marks.keys())
        if "consistent-topology-changes" not in self.scylla_features:
            node3.watch_log_for(f"Added node=.*{node2.address()} as leaving node, coordinator=.*{node1.address()}")
            node4.watch_log_for(f"Added node=.*{node2.address()} as leaving node, coordinator=.*{node1.address()}")
            node5.watch_log_for(f"Added node=.*{node2.address()} as leaving node, coordinator=.*{node1.address()}")

            logger.debug("Wait for node 5 to start to sync data")
            node5.watch_log_for("Started to sync data for removing node")
        if to_kill_coordinator:
            if "consistent-topology-changes" in self.scylla_features:
                node1.watch_log_for("raft_topology - start streaming", from_mark=marks[node1])
            logger.debug("Stop node1 gently=False")
            node1.stop(gently=False)
        else:
            if "consistent-topology-changes" in self.scylla_features:
                node5.watch_log_for("raft_topology - start streaming", from_mark=marks[node5])
            logger.debug("Stop node5 gently=False")
            node5.stop(gently=False)

    def _start_removenode_operation_and_abort_after_streaming_started(self, verification_node: ScyllaNode, removing_hostid: str, to_kill_coordinator: bool, marks: dict[ScyllaNode, int]):
        executor = ThreadPoolExecutor(max_workers=1)
        t = executor.submit(self._kill_node_thread, to_kill_coordinator, marks)

        api_cmd = f"http://{verification_node.address()}:{verification_node.api_port}/storage_service/remove_node/?host_id={removing_hostid}"
        logger.debug("Send restful api: " + api_cmd)
        try:
            r = requests.post(api_cmd)
            logger.debug(r.text)
            r.raise_for_status()
        except Exception as e:  # noqa: BLE001
            logger.debug(f"It is except to see the restful api to node1 to fail because node1 is killed: {e}")
        t.result()

    def _do_simple_removenode(self, to_kill_coordinator: bool):
        """
        Test removenode while kill coordinator or peer node
        1. Create a cluster with 5 nodes {'datacenter1': {rack1: 2, rack2: 1, rack3: 2}} with rf=3
        2. Stop node2 and removenode node2
        3. Kill node1 or node5 in the middle
        4. Check node2 is added as pending when removenode starts
        5. Check node2 is removed as pending when removenode aborts
        """
        if "consistent-topology-changes" in self.scylla_features:
            self.ignore_log_patterns += [
                r"raft_topology - send_raft_topology_cmd\(stream_ranges\) failed with exception",
                "raft_topology - Removenode failed. See earlier errors",
            ]
        else:
            self.ignore_log_patterns += [
                "connection dropped",
                "Failed to handle STREAM_MUTATION_FRAGMENTS",
                r"removenode.*failed",
            ]
        cluster_topology = {"datacenter1": {"rack1": 2, "rack2": 1, "rack3": 2}}
        node1, node2, node3, node4, node5 = self._prepare_cluster(cluster_topology, rf=3, keys_range=10000)
        node2_hostid = node2.hostid()
        node2.stop(wait_other_notice=True)
        marks = {node: node.mark_log() for node in self.cluster.nodelist()}
        self._start_removenode_operation_and_abort_after_streaming_started(node1, node2_hostid, to_kill_coordinator, marks)
        alive_nodes = [node3, node4]

        if "consistent-topology-changes" in self.scylla_features:
            if to_kill_coordinator:
                logger.info("Find new topology coordinator and wait rollback is finished")
                alive_nodes.append(node5)
                new_topology_coordinator = None
                for node in alive_nodes:
                    try:
                        node.watch_log_for("raft_topology - start topology coordinator fiber", timeout=10, from_mark=marks[node])
                        new_topology_coordinator: ScyllaNode = node
                        break
                    except TimeoutError:
                        continue
                new_topology_coordinator.watch_log_for("raft_topology - complete rollback", timeout=60, from_mark=marks[new_topology_coordinator])
                logger.info("Start node1")
                node1.start(wait_for_binary_proto=True, wait_other_notice=True)
                alive_nodes.append(node1)
            else:
                alive_nodes.append(node1)
                node5.start(wait_other_notice=True)
                alive_nodes.append(node5)

            # node2 is banned from cluster even if removenode was failed.
            node2.start(wait_other_notice=False)
            with pytest.raises(NodetoolError):
                node2.nodetool("rebuild")
            # we can remove it again and run rebuild
            node1.removenode(node2_hostid)
        else:
            alive_nodes.append(node5) if to_kill_coordinator else alive_nodes.append(node1)
            for node in alive_nodes:
                node.watch_log_for(f"Removed node=.*{node2.address()} as leaving node, coordinator=.*{node1.address()}", from_mark=marks[node])

            node1.start(wait_other_notice=True) if to_kill_coordinator else node5.start(wait_other_notice=True)
            node2.start(wait_other_notice=True)
            alive_nodes.append(node2)

        cluster_status = nodetool_status(node3)
        logger.debug(f"Cluster status {cluster_status}")
        for node in alive_nodes:
            node.nodetool("repair")

        cluster_status = nodetool_status(node3)
        logger.debug(f"Cluster status {cluster_status}")

    def test_simple_removenode_4(self):
        logger.debug("Test kill removenode coordinator node in the middle")
        self._do_simple_removenode(to_kill_coordinator=True)

    def test_simple_removenode_5(self):
        logger.debug("Test kill removenode peer node in the middle")
        self._do_simple_removenode(to_kill_coordinator=False)

    def test_removenode_rejoin(self):
        """
        Use cluster topology {datacenter1: {rack1: 1}, {rack2: 2}}
        Start cluster
        Stop node3
        Run nodetool removenode $host_id_of_node3
        Restart node3
        Test node3 can not join the cluster after removenode
        """
        cluster = self.cluster
        cluster.populate({"datacenter1": {"rack1": 1, "rack2": 2}}).start()
        node1, _node2, node3 = cluster.nodelist()
        node3_hostid = node3.hostid()
        node3.stop(wait_other_notice=True)
        node1.nodetool(f"removenode {node3_hostid}")
        self.ignore_log_patterns += [
            "gossip - is_safe_for_restart:",
            "Startup failed: std::runtime_error",
        ]
        node3.start(no_wait=True)
        if "consistent-topology-changes" not in self.scylla_features:
            node3.watch_log_for("Can not restart the removed node to join the cluster again")
        else:
            node3.watch_log_for("received notification of being banned from the cluster from", timeout=30)
        cluster_state = nodetool_status(node1)
        assert node3.address() not in [status["address"] for status in cluster_state["nodes"]], "Remove node returned to cluster"

    @pytest.mark.parametrize("when", ["before", "during", "after"])
    def test_add_new_node_while_add_new_table(self, when):
        """
        Test bootstrapped node get data in the new table
        1. Create a cluster with a three nodes with rf=1, insert data
        2. Add node, while node is bootstrapping add new table and insert data
        4. Check that node was connected and the cluster returns all data inserted
        """
        cluster = self.cluster
        rf = 1
        consistency = ConsistencyLevel.ONE
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(cluster_topology).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", rf)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        insert_c1c2(session, keys=range(4000), consistency=consistency)

        self.ignore_log_patterns += [
            r"raft_topology - streaming for tablet .* failed",
            "raft_topology - raft_topology_cmd.*failed with: raft::request_aborted",
        ]

        def run(min_keys):
            logger.debug("Creating keyspace 'ks1'")
            create_ks(session, "ks1", rf)
            create_cf(session, "cf1", read_repair=0.0, columns={"c1": "text", "c2": "text"})
            for i in range(min_keys):
                insert = SimpleStatement("insert into ks1.cf1 (key,c1,c2) values ('%d','%d','%d')" % (i, i, i), consistency_level=ConsistencyLevel.ALL)
                session.execute(insert)

        executor = ThreadPoolExecutor(max_workers=1)

        min_keys = 1000

        # Create table and insert data before bootstrapping of the new node
        if when == "before":
            t = executor.submit(run, min_keys)

        logger.debug("Adding new node")
        nodes = cluster.nodelist()
        node4 = new_node(cluster, data_center=node1.data_center, rack=node1.rack)
        node4.start(jvm_args=["--logger-log-level", "stream_session=debug"], no_wait=True)
        self.wait_for_node_streaming(node4)

        # Create table and insert data during bootstrapping of the new node
        if when == "during":
            t = executor.submit(run, min_keys)

        for node in nodes:
            node.watch_rest_for_alive(node4)
            node4.watch_rest_for_alive(node)

        node4.watch_log_for("Starting listening for CQL clients")

        # Create table and insert data after bootstrapping of the new node
        if when == "after":
            # Wait for the driver to discover node4 before creating the table.
            # Otherwise, schema agreement check after CREATE TABLE may skip node4,
            # and a subsequent query routed to node4 could fail with "unconfigured table".
            # See https://github.com/scylladb/scylladb/issues/21371
            # TODO: Remove this workaround once the scylladb/python-driver#604 is resolved.
            node4_ip = get_ip_from_node(node4)
            wait_for(
                func=lambda: (host := session.cluster.metadata.get_host(node4_ip)) is not None and host.is_up,
                timeout=60,
                step=0.5,
                text=f"Waiting for driver to discover node4 ({node4_ip})",
            )
            t = executor.submit(run, min_keys)

        t.result()

        logger.debug("Verifying data")
        query = SimpleStatement("SELECT * FROM ks1.cf1", consistency_level=consistency)
        result = list(session.execute(query))
        assert len(result) >= min_keys, f"Expected at least {min_keys} items in table"

    def _get_gossipinfo(self, output):
        """
        Parse gossipinfo output and put it into a python dict.

        Trailing slash on node ips is removed.

        :param output: 'nodetool gossip' stdout
        :returns: Dict with nodetool info. Example follows.
        {'127.0.0.1': {'DC': 'datacenter1',
                       'HOST_ID': 'bb821819-9049-4929-b7cc-7b2edf1eec10',
                       'LOAD': '128982',
                       'NET_VERSION': '0',
                       'RACK': 'rack1',
                       'RELEASE_VERSION': '2.1.8',
                       'RPC_ADDRESS': '127.0.0.1',
                       'SCHEMA': '2576e940-0936-3ff6-a12c-9c4ed9571175',
                       'STATUS': 'NORMAL,996695790724469087',
                       'generation': '1457611493',
                       'heartbeat': '118'}}
        """
        gossipinfo = {}
        current_node = None
        for line in output.splitlines():
            try:
                if current_node and current_node not in gossipinfo:
                    gossipinfo[current_node] = {}
                key, value = line.strip().split(":")
                gossipinfo[current_node].update({key: value})
            except:
                current_node = line.strip()[1:]
        return gossipinfo

    def test_remove_node_from_gossip(self):
        """
        Test a node can be removed from gossip
        1. Create a cluster with 3 nodes
        2. Add node, wait for node to start bootstrappig
        3. Kill the new node
        4. Check that the new node will be removed from
        4. Check gossip on_remove callback in storage_service will not cause deadlock
        """

        # this error is expected in teardown after this test in raft topology mode
        ignore_error = r"raft_topology - send_raft_topology_cmd\(stream_ranges\) failed with exception \(node state is bootstrapping\)"
        self.ignore_log_patterns += [ignore_error]

        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfere with the test (this must be after the populate)
        cluster.set_configuration_options(values=self.default_config_options(), batch_commitlog=True)
        cluster.populate({"datacenter1": {"rack1": 2, "rack2": 1}}).start()
        node1, _node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 2)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        insert_c1c2(session, keys=range(2000), consistency=ConsistencyLevel.TWO)

        logger.debug("Start node 4...")
        self.ignore_log_patterns += ["Startup failed"]
        # add new node to rack2
        node4 = new_node(cluster, data_center=node1.data_center, rack=node3.rack)
        node4.start(jvm_args=["--logger-log-level", "stream_session=debug"], no_wait=True)
        streaming_keyspace = "system_distributed" if "tablets" in self.scylla_features else "ks"
        self.wait_for_node_streaming(node4, keyspace=streaming_keyspace)

        logger.debug("Hard-stop node 4 ...")
        node4.stop(gently=False)

        logger.debug("Check node 1 removed node4  ...")
        message = f"FatClient .*({node4.address()}|{node4.hostid()}) has been silent for .*ms, removing from gossip"
        if "consistent-topology-changes" in self.scylla_features:
            message = f"Finished to force remove node ({node4.address()}|{node4.hostid()})"
        node1.watch_log_for(message, timeout=120)

        logger.debug("Check the hearbeat of node 1 ...")
        status1, _err1 = node1.nodetool("gossipinfo")
        gossipinfo_1 = self._get_gossipinfo(status1)
        heartbeat_1 = int(gossipinfo_1[cluster.get_node_ip(1)]["heartbeat"])

        time.sleep(3)

        logger.debug("Check the hearbeat of node 1 updated ...")
        status2, _err2 = node1.nodetool("gossipinfo")
        gossipinfo_2 = self._get_gossipinfo(status2)
        heartbeat_2 = int(gossipinfo_2[cluster.get_node_ip(1)]["heartbeat"])
        e_msg = f"Heartbeat for status 2 '{heartbeat_2}' is not greater than for status 1 '{heartbeat_1}', something is wrong"
        logger.debug("heartbeat_2 = %d, heartbeat_1 = %d" % (heartbeat_2, heartbeat_1))
        assert heartbeat_2 > heartbeat_1, e_msg

    @pytest.mark.use_cassandra_stress
    @pytest.mark.high_memory
    def test_add_node_when_cluster_is_filled(self):
        cluster = self.cluster
        config_options = self.default_config_options(hinted_handoff_enabled=None)
        cluster.set_configuration_options(values=config_options)
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        cluster.populate(cluster_topology).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]
        logger.debug("Cluster is up, start stressing...")

        num_keys = 1000000 if not hasattr(cluster, "scylla_mode") or cluster.scylla_mode != "debug" else 10000
        num_threads = 700 if not hasattr(cluster, "scylla_mode") or cluster.scylla_mode != "debug" else 70
        node1.stress(["write", "cl=QUORUM", f"n={num_keys}", "no-warmup", f"-rate threads={num_threads}"])

        logger.debug("Adding new node...")
        node4 = new_node(cluster, data_center=node1.data_center, rack=node1.rack)
        node4.start(wait_for_binary_proto=True)
        logger.debug("New node added...")

    # tablets do no support creating keyspace with rf > number_of_nodes
    @pytest.mark.required_features("!tablets")
    def test_add_node_with_large_partition1(self):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a single node with rf=2, insert data with large partition
        2. Add a new node
        3. Check that each node has all the data
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values=self.default_config_options(), batch_commitlog=True)
        cluster.populate({"datacenter1": {"rack1": 1}}).start()
        node1 = cluster.nodelist()[0]

        logger.debug("Node 1 started")
        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 2)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        nr_rows = 100
        c1 = "a" * 1024 * 100  # 100KB
        c2 = "b" * 1024 * 300  # 300KB
        c1s = [c1] * nr_rows
        c2s = [c2] * nr_rows
        logger.debug("Insert data")
        # lower concurency than the default to prevent connection overloading
        # due to all tests `test_add_node_with_large_partitionX` being consecutive, they will be run in concurrently with parallelization
        insert_c1c2(session, keys=range(nr_rows), consistency=ConsistencyLevel.ONE, c1_values=c1s, c2_values=c2s, concurrency=10)

        node2 = new_node(cluster, data_center=node1.data_center, rack="rack2")
        node2.start(wait_for_binary_proto=True)
        logger.debug("Node 2 started")

        logger.debug("Check rows on node2")
        self.check_rows_on_node(node2, nr_rows)
        logger.debug("Check rows on node1")
        self.check_rows_on_node(node1, nr_rows)

    # tablets do no support creating keyspace with rf > number_of_nodes
    @pytest.mark.required_features("!tablets")
    def test_add_node_with_large_partition2(self):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a single node with rf=2, insert data with large partition
        2. Add a new node
        3. Check that each node has all the data
        """
        nr_columns = 250
        nr_rows = 100
        column_size = 1 * 1024  # 1KB

        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values=self.default_config_options(), batch_commitlog=True)
        cluster.populate({"datacenter1": {"rack1": 1}}).start()
        node1 = cluster.nodelist()[0]

        logger.debug("Node 1 started")
        session = self.patient_cql_connection(node1)

        # create ks
        create_ks(session, "ks", 2)

        # Create cf
        columns = collections.OrderedDict()
        for i in range(1, nr_columns + 1):
            columns[f"c{i}"] = "text"
        create_cf(session, "cf", read_repair=0.0, columns=columns)

        # Insert data
        logger.debug("Insert data")
        # lower concurency than the default to prevent connection overloading
        # due to all tests `test_add_node_with_large_partitionX` being consecutive, they will be run in concurrently with parallelization
        insert_c1cn(session, keys=range(nr_rows), consistency=ConsistencyLevel.ONE, nr_columns=nr_columns, column_size=column_size, concurrency=10)

        node2 = new_node(cluster, data_center=node1.data_center, rack="rack2")
        node2.start(wait_for_binary_proto=True)
        logger.debug("Node 2 started")

        logger.debug("Check rows on node2")
        self.check_rows_on_node(node2, nr_rows)
        logger.debug("Check rows on node1")
        self.check_rows_on_node(node1, nr_rows)

    # tablets do no support creating keyspace with rf > number_of_nodes
    @pytest.mark.required_features("!tablets")
    def test_add_node_with_large_partition3(self):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a single node with rf=2, insert data with mixed large partition and small partion
        2. Add a new node
        3. Check that each node has all the data
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values=self.default_config_options(), batch_commitlog=True)
        cluster.populate({"datacenter1": {"rack1": 1}}).start()
        node1 = cluster.nodelist()[0]

        logger.debug("Node 1 started")
        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 2)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        nr_rows = 100
        v1 = "a" * 1024 * 10  # 10KB
        v2 = "b" * 1024 * 300  # 300KB
        v3 = "c" * 1024 * 1  # 1KB
        v4 = "d" * 1024 * 3  # 3KB
        c1s = []
        c2s = []
        for n in range(nr_rows):
            if n % 2 == 0:
                c1s.append(v1)
                c2s.append(v2)
            else:
                c1s.append(v3)
                c2s.append(v4)
        logger.debug("Insert data")
        # lower concurency than the default to prevent connection overloading
        # due to all tests `test_add_node_with_large_partitionX` being consecutive, they will be run in concurrently with parallelization
        insert_c1c2(session, keys=range(nr_rows), consistency=ConsistencyLevel.ONE, c1_values=c1s, c2_values=c2s, concurrency=10)

        node2 = new_node(cluster, data_center=node1.data_center, rack="rack2")
        node2.start(wait_for_binary_proto=True)
        logger.debug("Node 2 started")

        logger.debug("Check rows on node2")
        self.check_rows_on_node(node2, nr_rows)
        logger.debug("Check rows on node1")
        self.check_rows_on_node(node1, nr_rows)

    # tablets do no support creating keyspace with rf > number_of_nodes
    @pytest.mark.required_features("!tablets")
    def test_add_node_with_large_partition4(self):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a single node with rf=2, insert data with large partitions
        2. Add a new node
        3. Check that each node has all the data
        """
        timeout = self.cql_timeout(300)
        values = {
            "range_request_timeout_in_ms": timeout * 1000,
        }
        logger.debug(f"Setting cluster configuration options: {values}")
        cluster = self.cluster
        cluster.set_configuration_options(values=values)

        nr_partitions = 100  # 100 fails 10 works
        if hasattr(self.cluster, "scylla_mode") and self.cluster.scylla_mode == "debug":
            nr_partitions //= 10
        # each partition has 3000 cql rows, so there will be nr_partitions * 3000 cql rows
        rows_per_partition = 3000
        nr_rows = nr_partitions * rows_per_partition

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values=self.default_config_options(), batch_commitlog=True)
        cluster.populate({"datacenter1": {"rack1": 1}}).start()
        node1 = cluster.nodelist()[0]
        ks = "ks"
        tbl = "test"
        logger.debug(f"Populate {node1.name} with {nr_partitions} partitions, {rows_per_partition} rows in each")
        with self.patient_cql_connection(node1) as session:
            create_ks(session, ks, 2)
            session.execute(f"CREATE TABLE {ks}.{tbl} (pk varchar, ck varchar, PRIMARY KEY(pk, ck))")
            stmt = session.prepare(f"INSERT into {ks}.{tbl} (pk, ck) VALUES (?, ?)")
            for pk in range(nr_partitions):
                for ck in range(rows_per_partition):
                    session.execute(stmt, (f"key{pk}", f"row{ck}"))
        logger.debug("Data population completed")

        node2 = new_node(cluster, data_center=node1.data_center, rack="rack2")
        node2.start(wait_for_binary_proto=True)
        logger.debug("Node 2 started")

        logger.debug("Check rows on node2")
        self.check_rows_on_node(node2, nr_rows, ks=ks, cf=tbl, timeout=timeout)
        logger.debug("Check rows on node1")
        self.check_rows_on_node(node1, nr_rows, ks=ks, cf=tbl, timeout=timeout)

    @pytest.mark.skip_if(with_feature("tablets") & issue_open("#18180"))
    def test_increment_decrement_counters_in_threads_nodes_restarted(self):  # noqa: PLR0915
        """
        increment/decrement 2 counters(2 inc vs 1 dec) * 1000 times * 120 threads
        1. Create a cluster with 3 nodes with rf=3
        2. Start increment/decrement counters CL=QUORUM
        3. Stop one node and wait 10 seconds
        4. Start the node and wait 10 seconds
        5. Stop another 2 nodes
        6. Wait when all counter ops complete
        7. Start 2 nodes
        8. Verify counters consistency
        """
        # TODO: Remove it when https://github.com/scylladb/scylla-dtest/issues/3686 is solved
        logging.getLogger("cassandra").setLevel(logging.DEBUG)
        logging.getLogger("cluster").setLevel(logging.DEBUG)

        cluster = self.cluster
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        config_options = self.default_config_options(hinted_handoff_enabled=None)
        cluster.set_configuration_options(values=config_options)
        cluster.populate(cluster_topology).start()
        nodes = cluster.nodelist()

        with self.patient_cql_connection(nodes[0]) as session:
            create_ks(session, "ks", 3)
            create_cf(session, "cf", validation="CounterColumnType", columns={"c": "counter"})

        nb_increment = 500
        nb_counter = 2

        def run(connection, decrement):
            _return = dict.fromkeys([i for i in range(nb_counter)], 0)
            for i in range(nb_increment):
                for c in range(nb_counter):
                    if decrement:
                        query = SimpleStatement("UPDATE cf SET c = c - 1 WHERE key = 'counter%i'" % c, consistency_level=ConsistencyLevel.ONE)
                    else:
                        query = SimpleStatement("UPDATE cf SET c = c + 1 WHERE key = 'counter%i'" % c, consistency_level=ConsistencyLevel.ONE)
                    connection.execute(query)
                    if decrement:
                        _return[c] -= 1
                    else:
                        _return[c] += 1
                    time.sleep(0.01)
            return _return

        result = dict.fromkeys([i for i in range(nb_counter)], 0)

        num_threads = 120

        executor = ThreadPoolExecutor(max_workers=num_threads)

        sessions = [self.patient_cql_connection(node, "ks") for node in nodes]

        # stop and restart one node for a while
        nodes[2].stop(wait_other_notice=True)

        threads = []
        for x in range(num_threads):
            conn = sessions[x % (len(nodes) - 1)]
            decrement = (x % len(nodes)) == 0
            threads.append(executor.submit(run, conn, decrement))

        for t in threads:
            t_result = t.result()
            result = {k: result.get(k, 0) + t_result.get(k, 0) for k in set(result)}

        nodes[2].start(wait_other_notice=True, wait_for_binary_proto=True)

        nodes[0].stop(wait_other_notice=True)
        nodes[1].stop(wait_other_notice=True)

        sessions[2] = self.patient_cql_connection(nodes[2], "ks")

        threads = []
        for x in range(num_threads):
            conn = sessions[2]
            decrement = (x % len(nodes)) == 0
            threads.append(executor.submit(run, conn, decrement))

        for t in threads:
            t_result = t.result()
            result = {k: result.get(k, 0) + t_result.get(k, 0) for k in set(result)}

        nodes[0].start(wait_other_notice=True, wait_for_binary_proto=True)
        nodes[1].start(wait_other_notice=True, wait_for_binary_proto=True)

        keys = ",".join(["'counter%i'" % c for c in range(nb_counter)])
        query = SimpleStatement("SELECT key, c FROM cf WHERE key IN (%s)" % keys, consistency_level=ConsistencyLevel.ALL)
        res = list(sessions[0].execute(query))
        assert res == list(sessions[1].execute(query)), "different counter values in node0 and node1"
        assert res == list(sessions[2].execute(query)), "different counter values in node0 and node2"

        for c in range(nb_counter):
            assert result[c] == res[c][1], "Expecting counter%i = %i, got %i" % (c, result[c], res[c][1])

    # tablets do no support creating keyspace with rf > number_of_nodes
    @pytest.mark.required_features("!tablets")
    def test_verify_latest_copy_add_node(self):
        """
        Test bootstrapped node streams latest copy
        1. Create a cluster with a single node with rf=3
        2. Add a new node
        3. Check that new node has all the latest data
        """
        cluster = self.cluster

        config_options = self.default_config_options()
        config_options.update(self.repair_based_node_ops_config_options(True))
        cluster.set_configuration_options(values=config_options, batch_commitlog=True)
        cluster.populate({"datacenter1": {"rack1": 1, "rack2": 1}}).start()
        node1, node2 = cluster.nodelist()

        # Insert on node1 and node2
        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 3)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})
        cs = ["0"] * 1000
        logger.debug("Insert data on node 1 and node 2")
        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.TWO, c1_values=cs, c2_values=cs)

        # Insert on node1 only
        node2.stop()
        session = self.patient_cql_connection(node1)
        cs = ["1"] * 500
        logger.debug("Insert data on node 1")
        insert_c1c2(session, keys=range(500), consistency=ConsistencyLevel.ONE, c1_values=cs, c2_values=cs)

        # Insert on node2 only
        node2.start(wait_for_binary_proto=True)
        node1.stop()
        session = self.patient_cql_connection(node2)
        cs = ["2"] * 500
        logger.debug("Insert data on node 2")
        insert_c1c2(session, keys=range(500, 1000), consistency=ConsistencyLevel.ONE, c1_values=cs, c2_values=cs)
        node1.start(wait_for_binary_proto=True)

        # Bootstrap a new node
        node3 = new_node(cluster, data_center=node1.data_center, rack="rack3")
        node3.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node3)
        session.execute("use ks;")
        logger.debug("Node 3 started")

        # Shtudown node1 and node2
        node1.stop()
        node2.stop()

        logger.debug("Check rows on node 3 have latest copy")
        cs = ["1"] * 500
        query_c1c2_concurrent(session, keys=range(500), consistency=ConsistencyLevel.ONE, c1_values=cs, c2_values=cs)
        cs = ["2"] * 500
        query_c1c2_concurrent(session, keys=range(500, 1000), consistency=ConsistencyLevel.ONE, c1_values=cs, c2_values=cs)

    def test_verify_latest_copy_replace_node(self):
        cluster = self.cluster
        config_options = self.repair_based_node_ops_config_options(True)
        cluster.set_configuration_options(values=config_options, batch_commitlog=True)
        logger.debug("Starting cluster with 3 nodes.")
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        cluster.populate(cluster_topology).start(wait_for_binary_proto=True)
        node1, node2, node3 = cluster.nodelist()

        # Insert on node1, node2 and node3
        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 3)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})
        cs = ["0"] * 1000
        logger.debug("Insert data on node 1 and node 2")
        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.THREE, c1_values=cs, c2_values=cs)

        # Stop node 3
        node3_host_id = node3.hostid()
        node3.stop()

        # Insert on node1 only
        node2.stop()
        session = self.patient_cql_connection(node1)
        cs = ["1"] * 500
        logger.debug("Insert data on node 1")
        insert_c1c2(session, keys=range(500), consistency=ConsistencyLevel.ONE, c1_values=cs, c2_values=cs)

        # Insert on node2 only
        node2.start(wait_for_binary_proto=True)
        node1.stop()
        session = self.patient_cql_connection(node2)
        cs = ["2"] * 500
        logger.debug("Insert data on node 2")
        insert_c1c2(session, keys=range(500, 1000), consistency=ConsistencyLevel.ONE, c1_values=cs, c2_values=cs)
        node1.start(wait_for_binary_proto=True)

        # Replacing node3 with node4
        logger.debug("Starting node 4 to replace node 3")
        node4 = new_node(cluster, bootstrap=True, token=None, remote_debug_port="0", data_center=node3.data_center, rack=node3.rack)
        node4.start(wait_for_binary_proto=True, replace_node_host_id=node3_host_id)
        session = self.patient_cql_connection(node4)
        session.execute("use ks;")
        logger.debug("Node 4 finished replacing node 3")

        # Shtudown node1 and node2
        node1.stop()
        node2.stop()

        logger.debug("Check rows on node 4 have latest copy")
        cs = ["1"] * 500
        query_c1c2_concurrent(session, keys=range(500), consistency=ConsistencyLevel.ONE, c1_values=cs, c2_values=cs)
        cs = ["2"] * 500
        query_c1c2_concurrent(session, keys=range(500, 1000), consistency=ConsistencyLevel.ONE, c1_values=cs, c2_values=cs)

    @pytest.mark.skip_if(with_feature("tablets"))
    def test_verify_latest_copy_rebuild_node(self):
        cluster = self.cluster
        config_options = self.repair_based_node_ops_config_options(True)
        cluster.set_configuration_options(values=config_options, batch_commitlog=True)
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        logger.debug("Starting cluster with 3 nodes.")
        cluster.populate(cluster_topology).start(wait_for_binary_proto=True)
        node1, node2, node3 = cluster.nodelist()

        # Insert on node1, node2 and node3
        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 3)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})
        cs = ["0"] * 1000
        logger.debug("Insert data on node 1 and node 2")
        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.THREE, c1_values=cs, c2_values=cs)

        # Stop node 3
        node3.stop()

        # Insert on node1 only
        node2.stop()
        session = self.patient_cql_connection(node1)
        cs = ["1"] * 500
        logger.debug("Insert data on node 1")
        insert_c1c2(session, keys=range(500), consistency=ConsistencyLevel.ONE, c1_values=cs, c2_values=cs)

        # Insert on node2 only
        node2.start(wait_for_binary_proto=True)
        node1.stop()
        session = self.patient_cql_connection(node2)
        cs = ["2"] * 500
        logger.debug("Insert data on node 2")
        insert_c1c2(session, keys=range(500, 1000), consistency=ConsistencyLevel.ONE, c1_values=cs, c2_values=cs)
        node1.start(wait_for_binary_proto=True)

        # Rebuild node3
        logger.debug("Starting node 3")
        node3.start(wait_for_binary_proto=True)
        node3.nodetool("rebuild")
        logger.debug("Node 3 finished rebuild")
        session = self.patient_cql_connection(node3)
        session.execute("use ks;")

        # Shtudown node1 and node2
        node1.stop()
        node2.stop()

        logger.debug("Check rows on node 3 have latest copy")
        cs = ["1"] * 500
        query_c1c2_concurrent(session, keys=range(500), consistency=ConsistencyLevel.ONE, c1_values=cs, c2_values=cs)
        cs = ["2"] * 500
        query_c1c2_concurrent(session, keys=range(500, 1000), consistency=ConsistencyLevel.ONE, c1_values=cs, c2_values=cs)

    # Cannot decommission node3 with tablets when rf == number of nodes
    @pytest.mark.required_features("!tablets")
    def test_verify_latest_copy_decommission_node(self):
        cluster = self.cluster
        config_options = self.repair_based_node_ops_config_options(True)
        cluster.set_configuration_options(values=config_options, batch_commitlog=True)
        logger.debug("Starting cluster with 3 nodes.")
        cluster_topology = generate_cluster_topology(rack_num=3)
        cluster.populate(cluster_topology).start(wait_for_binary_proto=True)
        node1, node2, node3 = cluster.nodelist()

        # Insert on node1, node2 and node3
        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 3)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})
        cs = ["0"] * 1000
        logger.debug("Insert data on node 1 and node 2")
        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.THREE, c1_values=cs, c2_values=cs)

        # Stop node 3
        node3.stop()

        # Insert on node1 only
        node2.stop()
        session = self.patient_cql_connection(node1)
        cs = ["1"] * 500
        logger.debug("Insert data on node 1")
        insert_c1c2(session, keys=range(500), consistency=ConsistencyLevel.ONE, c1_values=cs, c2_values=cs)

        # Insert on node2 only
        node2.start(wait_for_binary_proto=True)
        node1.stop()
        session = self.patient_cql_connection(node2)
        cs = ["2"] * 500
        logger.debug("Insert data on node 2")
        insert_c1c2(session, keys=range(500, 1000), consistency=ConsistencyLevel.ONE, c1_values=cs, c2_values=cs)
        node1.start(wait_for_binary_proto=True)

        # Decommission node 1
        logger.debug("Starting node 3")
        node3.start(wait_for_binary_proto=True)
        node1.nodetool("decommission")
        logger.debug("Node 1 finished decommission")
        session = self.patient_cql_connection(node3)
        session.execute("use ks;")

        logger.debug("Check rows on node 2 and 3 have latest copy")
        cs = ["1"] * 500
        query_c1c2_concurrent(session, keys=range(500), consistency=ConsistencyLevel.TWO, c1_values=cs, c2_values=cs)
        cs = ["2"] * 500
        query_c1c2_concurrent(session, keys=range(500, 1000), consistency=ConsistencyLevel.TWO, c1_values=cs, c2_values=cs)

    def test_verify_latest_copy_removenode_node(self):  # noqa: PLR0915
        cluster = self.cluster
        config_options = self.repair_based_node_ops_config_options(True)
        cluster.set_configuration_options(values=config_options, batch_commitlog=True)
        cluster_topology = {"datacenter1": {"rack1": 2, "rack2": 1, "rack3": 1}}
        logger.debug("Starting cluster with 4 nodes: %s.", cluster_topology)
        cluster.populate(cluster_topology).start(wait_for_binary_proto=True)
        node1, node2, node3, node4 = cluster.nodelist()

        # Insert on node1, node2, node3, node4
        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 3)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})
        cs = ["0"] * 1000
        logger.debug("Insert data on node 1, node 2, node 3 and node 4")
        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.THREE, c1_values=cs, c2_values=cs)

        # Insert on node1, node2 and node3
        node4.stop()
        session = self.patient_cql_connection(node1)
        cs = ["1"] * 250
        logger.debug("Insert data on node 1, node 2 and node 3")
        insert_c1c2(session, keys=range(250), consistency=ConsistencyLevel.TWO, c1_values=cs, c2_values=cs)

        # Insert on node1, node2 and node4
        node4.start(wait_for_binary_proto=True)
        node3.stop()
        session = self.patient_cql_connection(node4)
        cs = ["2"] * 250
        logger.debug("Insert data on node 1, node 2 and node 4")
        insert_c1c2(session, keys=range(250, 500), consistency=ConsistencyLevel.TWO, c1_values=cs, c2_values=cs)

        # Insert on node1, node3 and node4
        node3.start(wait_for_binary_proto=True)
        node2.stop()
        session = self.patient_cql_connection(node3)
        cs = ["3"] * 250
        logger.debug("Insert data on node 1, node 3 and node 4")
        insert_c1c2(session, keys=range(500, 750), consistency=ConsistencyLevel.TWO, c1_values=cs, c2_values=cs)

        # Insert on node2, node3 and node4
        node2.start(wait_for_binary_proto=True)
        node1.stop()
        session = self.patient_cql_connection(node2)
        cs = ["4"] * 250
        logger.debug("Insert data on node 2, node 3 and node 4")
        insert_c1c2(session, keys=range(750, 1000), consistency=ConsistencyLevel.TWO, c1_values=cs, c2_values=cs)

        hostid = node2.hostid()
        node2.stop(wait_other_notice=True)
        node1.start(wait_for_binary_proto=True)
        node1.nodetool(f"removenode {hostid}")
        logger.debug("Node 1 finished removenode node 2")
        session = self.patient_cql_connection(node1)
        session.execute("use ks;")

        # Shtudown node4
        node4.stop()

        logger.debug("Check rows on node 1 and node 3 have latest copy")
        cs = ["1"] * 250
        query_c1c2_concurrent(session, keys=range(250), consistency=ConsistencyLevel.TWO, c1_values=cs, c2_values=cs)
        cs = ["2"] * 250
        query_c1c2_concurrent(session, keys=range(250, 500), consistency=ConsistencyLevel.TWO, c1_values=cs, c2_values=cs)
        cs = ["3"] * 250
        query_c1c2_concurrent(session, keys=range(500, 750), consistency=ConsistencyLevel.TWO, c1_values=cs, c2_values=cs)
        cs = ["4"] * 250
        query_c1c2_concurrent(session, keys=range(750, 1000), consistency=ConsistencyLevel.TWO, c1_values=cs, c2_values=cs)

    def _do_check_peer_and_local_table(self, nodes):
        peers_and_local = []
        for node in nodes:
            status = nodetool_status(node)
            logger.debug(f"nodetool status from {node.name}: {status}")
            s = self.patient_exclusive_cql_connection(node)
            peers = rows_to_list(s.execute("SELECT host_id, peer FROM system.peers"))
            local = rows_to_list(s.execute("SELECT host_id, broadcast_address FROM system.local"))
            peers_and_local.append(str(sorted(peers + local)))
            assert len(peers) == len(nodes) - 1, f"There are more peers {peers} than expected: {len(nodes) - 1}"
            assert len(local) == 1, f"There are more local {local} than expected: 1"
            logger.debug(f"Check peer table for {node.name} with ip address {node.address()} : peers={peers} ")
            logger.debug(f"Check peer table for {node.name} with ip address {node.address()} : local={local} ")
            assert str(local[0][0]) == node.hostid()
            assert str(local[0][1]) == node.address()
        logger.debug(f"peers_and_local={peers_and_local}")
        assert len(set(peers_and_local)) == 1

    def check_peer_and_local_table(self, nodes):
        i = 0
        max_iterations = 20
        while i < max_iterations:
            i += 1
            try:
                self._do_check_peer_and_local_table(nodes)
                break
            except AssertionError:
                if i == max_iterations:
                    raise
                else:
                    logger.debug("check_peer_and_local_table(): try again")
                    time.sleep(1)
                    continue

    @pytest.mark.parametrize("rf", [2, 3], ids=["rack=rf=2", "rack=rf=3"])
    def test_change_node_ip(self, rf):
        """
        Start cluster number of racks == rf
        Stop a node
        Change IP address of the node
        Start node again
        Verify all nodes in the cluster notice the node uses the new ip address
        """
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=rf, nodes_per_rack=1)
        cluster.set_configuration_options(values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(cluster_topology).start()
        node1 = cluster.nodelist()[0]
        target_node = cluster.nodelist()[-1]
        consistency_level = {2: ConsistencyLevel.TWO, 3: ConsistencyLevel.THREE}[rf]
        session = self.patient_cql_connection(node1)

        create_ks(session, "ks1", 1)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})
        insert_c1c2(session, ks="ks1", keys=range(1000), consistency=ConsistencyLevel.ONE)

        create_ks(session, f"ks{rf}", rf)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})
        insert_c1c2(session, ks=f"ks{rf}", keys=range(1000), consistency=consistency_level)

        logger.debug(f"Stop {target_node.name}")
        target_node.stop()

        logger.debug(f"Change IP address for {target_node.name}")
        ip_prefix = cluster.get_ipprefix()
        new_ip = f"{ip_prefix}33"
        target_node.set_configuration_options(values={"listen_address": new_ip, "rpc_address": new_ip, "api_address": new_ip})
        target_node.network_interfaces = {k: (new_ip, v[1]) for k, v in target_node.network_interfaces.items()}
        logger.debug(f"Start target node {target_node.name} again with ip address {new_ip}")

        target_node.start(wait_for_binary_proto=True, wait_other_notice=False)
        logger.debug(f"Target_node {target_node.name} is now up")
        target_node_hostid = target_node.hostid()
        # Verify all nodes in the cluster see node3 is using the new ip address
        for node in cluster.nodelist():
            status = nodetool_status(node)
            logger.debug(f"nodetool status from {node.name}: {status}")
            for n in status["nodes"]:
                if n["address"] == new_ip:
                    assert n["host id"] == target_node_hostid

        # Verify peers and local table are valid
        self.check_peer_and_local_table(cluster.nodelist())

        # Verify data returned is still valid after ip change
        for k in range(1000):
            query_c1c2(session, k, ConsistencyLevel.ONE, ks="ks1")
            query_c1c2(session, k, consistency_level, ks=f"ks{rf}")

    def test_decommission_after_changing_node_ip(self):
        """Changes to cluster topology after node ip changed"""

        cluster = self.cluster
        logger.info("starting cluster")
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True)

        logger.info("stopping node3")
        _node1, _node2, node3 = cluster.nodelist()
        node3.stop(gently=True)

        logger.info("replace node3 address")
        old_ip3 = node3.address()
        ip3 = f"{old_ip3}3"
        node3.set_configuration_options(values={"listen_address": ip3, "rpc_address": ip3, "api_address": ip3})
        node3.network_interfaces = {k: (ip3, v[1]) for k, v in node3.network_interfaces.items()}

        logger.info("decommission node3")
        node3.start(wait_for_binary_proto=False, wait_other_notice=True)
        timeout = self.cql_timeout(120)
        retry_till_success(node3.decommission, timeout=timeout)

        def is_shutdown(endpoint=old_ip3):
            found = False
            for node in cluster.nodelist():
                gs = nodetool_gossipinfo(node)
                if endpoint in gs:
                    logger.debug(gs[endpoint])
                    if "shutdown" not in gs[endpoint]["STATUS"]:
                        found |= True
            return not found

        logger.info(f"Waiting for {old_ip3} gossip status=shutdown")
        wait_for(is_shutdown, step=10, timeout=timeout)

        logger.info("add new node4")
        node4 = cluster.new_node(4, data_center=node3.data_center, rack=node3.rack)
        node4.start(wait_for_binary_proto=True)
        logger.info("done")

    @pytest.mark.skip_if(with_feature("tablets") & issue_open("https://github.com/scylladb/scylladb/issues/23525"))
    def test_decommission_after_decreasing_rf(self, dtest_config):  # noqa: PLR0915
        """
        Test a node decommission after altering a keyspace to a lower replication-factor value.
        1. Create a keyspace with RF=3
        2. Write data
        3. Alter the keyspace to RF=2
        4. Decommission a random node
        5. Verify data exists.
        6. Add a new node.
        7. Alter the keyspace back to RF=3
        8. Verify data exists.
        """
        cluster = self.cluster
        cluster.set_configuration_options({"hinted_handoff_enabled": "false"})
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1, dc_name_prefix="dc", rack_name_prefix="rack")
        cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3 = cluster.nodelist()
        keyspace = "ks"
        dc = node1.get_datacenter_name()
        num_keys = 500

        # Sanity check. We rely on this because we won't be able to decommission
        # node3 if we don't stop replicating data in its rack (with tablets).
        assert node3.rack == "rack3"

        def verify_data(session, consistency_level=ConsistencyLevel.QUORUM):
            logger.info(f"Verify data exists")
            for key in range(num_keys):
                query_c1c2(session, key, consistency=consistency_level)

        def alter_ks(session, cluster, keyspace: str, dc: str, rf: int):
            stmt = None
            if "tablets" in self.scylla_features:
                replication_factor = ", ".join([f"'rack{i + 1}'" for i in range(rf)])
                replication_factor = f"[{replication_factor}]"
            else:
                replication_factor = str(rf)
            stmt = f"ALTER KEYSPACE {keyspace} WITH replication = {{'class': 'NetworkTopologyStrategy', '{dc}': {replication_factor}}}"
            # With tablets, ALTER KEYSPACE returns only after the tablet migrations it triggers are done.
            # This took over 30s on a slow CI machine, so use a 10x margin over that.
            change_schema_safely(session, cluster.nodelist(), stmt, timeout=300)

        logger.info(f"Create a keyspace with rf=3")
        with self.patient_cql_connection(node1) as session:
            if "tablets" in self.scylla_features:
                create_ks_stmt = f"CREATE KEYSPACE {keyspace} WITH replication = {{'class': 'NetworkTopologyStrategy', '{dc}': ['rack1', 'rack2', 'rack3']}}"
                create_ks_query(session=session, name=keyspace, query=create_ks_stmt)
            else:
                create_ks(session=session, name=keyspace, rf={dc: 3})
            create_c1c2_table(session, speculative_retry="NONE")

            logger.info(f"Insert {num_keys} keys")
            insert_c1c2(session, n=num_keys)

            logger.info(f"Change replication factor to rf=2")
            alter_ks(session, cluster, keyspace, dc, 2)
            logger.info(f"Decommission {node3.name}")
            node3.decommission()
            node3.stop()
            verify_data(session)

            logger.info(f"Adding a new node")
            node4 = new_node(cluster, data_center=node3.data_center, rack=node3.rack)
            node4.start(wait_for_binary_proto=True, wait_other_notice=True)
            logger.info(f"Change replication factor back to rf=3")
            alter_ks(session, cluster, keyspace, dc, 3)

        if "tablets" not in self.scylla_features:
            # Following the above "alter keyspace" command completion, all replicas are populated.
            # In case tablets are not used, the new node should run repair.
            node4.repair(keyspace="ks")
        all_nodes = [node1, node2, node4]
        logger.info(f"Verify data exists on all nodes")
        for node in all_nodes:
            other_nodes = [n for n in all_nodes if n is not node]
            cluster.stop_nodes(other_nodes)
            with self.patient_cql_connection(node) as session:
                verify_data(session, consistency_level=ConsistencyLevel.ONE)
            cluster.start_nodes(other_nodes, wait_other_notice=True)

    def test_replace_after_changing_node_ip(self):
        """Changes to cluster topology after node ip changed"""

        cluster = self.cluster
        logger.info("starting cluster")
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True)

        logger.info("stopping node3")
        node1, node2, node3 = cluster.nodelist()
        node3_host_id = node3.hostid()
        node3.stop(gently=True)

        logger.info("replace node3 address")
        old_ip3 = node3.address()
        ip3 = f"{old_ip3}3"
        node3.set_configuration_options(values={"listen_address": ip3, "rpc_address": ip3, "api_address": ip3})
        node3.network_interfaces = {k: (ip3, v[1]) for k, v in node3.network_interfaces.items()}
        node3.start(wait_for_binary_proto=True, wait_other_notice=True)

        logger.info("stop node3")
        node3.stop(wait_other_notice=True)

        def is_shutdown(endpoint=old_ip3):
            found = False
            for node in [node1, node2]:
                gs = nodetool_gossipinfo(node)
                if endpoint in gs:
                    logger.debug(gs[endpoint])
                    if "shutdown" not in gs[endpoint]["STATUS"]:
                        found |= True
            return not found

        logger.info(f"Waiting for {old_ip3} gossip status=shutdown")
        timeout = self.cql_timeout(120)
        wait_for(is_shutdown, step=10, timeout=timeout)

        logger.info("Replace node3 with node4")
        node4 = new_node(cluster, bootstrap=True, token=None, remote_debug_port="0", data_center=node3.data_center, rack=node3.rack)
        node4.start(wait_for_binary_proto=True, replace_node_host_id=node3_host_id)

    def test_change_node_ip_full_cluster_down(self):
        """
        Start 3 nodes
        Stop node 1 2 3
        Change IP address of node 1 2 3
        Start node 1 2 3 again
        Verify all nodes in the cluster notice other nodes use the new ip address
        """
        cluster = self.cluster

        cluster.set_configuration_options(values=self.default_config_options(), batch_commitlog=True)
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        cluster.populate(cluster_topology).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 3)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})
        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ONE)

        origin_hostid1 = node1.hostid()
        origin_hostid2 = node2.hostid()
        origin_hostid3 = node3.hostid()

        cluster.stop()

        ip_prefix = cluster.get_ipprefix()
        for node in cluster.nodelist():
            old_ip = node.address()
            lower_ip = int(old_ip.split(".")[-1]) + 10
            assert lower_ip < 255
            last = str(lower_ip)
            ip = f"{ip_prefix}{last}"
            logger.debug(f"Change IP address for {node.name} from {old_ip} to {ip}")
            node.set_configuration_options(values={"listen_address": ip, "rpc_address": ip, "api_address": ip})
            node.network_interfaces = {k: (ip, v[1]) for k, v in node.network_interfaces.items()}

        for node in cluster.nodelist():
            logger.debug(f"Start {node.name} again with ip address {node.address()}")
            node.start(wait_for_binary_proto=True, wait_other_notice=False)

        # Verify all nodes in the cluster see node3 is using the new ip address
        hostid1 = node1.hostid()
        hostid2 = node2.hostid()
        hostid3 = node3.hostid()
        ip1 = node1.address()
        ip2 = node2.address()
        ip3 = node3.address()

        for node in [node1, node2, node3]:
            status = nodetool_status(node)
            logger.debug(f"nodetool status from {node.name}: {status}")
            for n in status["nodes"]:
                logger.debug(f"n={n}")
                if n["address"] == ip3:
                    assert n["host id"] == hostid3
                    assert origin_hostid3 == hostid3
                if n["address"] == ip2:
                    assert n["host id"] == hostid2
                    assert origin_hostid2 == hostid2
                if n["address"] == ip1:
                    assert n["host id"] == hostid1
                    assert origin_hostid1 == hostid1

    @pytest.fixture(scope="function", name="ip_tables")
    def fixture_ip_tables(self, request: pytest.FixtureRequest):
        """
        rule name should be less than 28 chars to avoid "chain name too long" error
        random suffix is added to avoid conflict with other rules
        """

        def cleanup_chan():
            try:
                iptables_obj.delete_chain()
            except Exception as ex:  # noqa: BLE001
                logger.warning(f"failed to delete chain: {ex}")

        request.addfinalizer(cleanup_chan)

        chain_name = f"dtest-{secrets.token_hex(2)}-{request.node.name}"[:28]
        logger.info(f"Chain name: {chain_name}")
        iptables_obj = IPTable(chain_name=chain_name)
        iptables_obj.create_new_chain()

        return iptables_obj

    @pytest.mark.require("jira:SCYLLADB-608")
    @pytest.mark.skipif(condition=running_in_podman(), reason="can't use iptables within podman")
    def test_decommission_node_while_gossip_partly_blocked(self, ip_tables):
        """reproducer scylladb/scylla-operator#982 and scylladb/scylladb#11302

        restart and decommission a node while other nodes can't send gossip communication to it
        """
        logger.info("populating cluster with three nodes")
        cluster = self.cluster
        # add node to 2nd rack, so decommission not failed with tablets
        cluster.populate({"datacenter1": {"rack1": 1, "rack2": 2}})
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)

        logger.info("stopping node3")
        node1, node2, node3 = cluster.nodelist()
        node3_ip_address = get_ip_from_node(node=node3)
        node3.stop(gently=False)

        logger.info("block gossip communication to node3")
        node1_ip_address = get_ip_from_node(node=node1)
        node2_ip_address = get_ip_from_node(node=node2)
        rule1 = IPTableRule(protocol="tcp", source=f"{node1_ip_address}/32", destination_port=7000, target="DROP", destination=node3_ip_address)
        rule2 = IPTableRule(protocol="tcp", source=f"{node2_ip_address}/32", destination_port=7000, target="DROP", destination=node3_ip_address)
        ip_tables.add_rule(rule1)
        ip_tables.add_rule(rule2)

        logger.info("start node3")
        node3.start(wait_for_binary_proto=True, wait_other_notice=False)

        for n in cluster.nodelist():
            stdout, stderr = n.nodetool("status")
            logger.info(stdout)
            logger.info(stderr)

        wait_for_nodes_status(node1, ["UN", "UN", "DN"])
        wait_for_nodes_status(node2, ["UN", "UN", "DN"])

        self.ignore_log_patterns += [
            r"[Dd]ecommission.*failed",
        ]

        logger.info("try to decommission node3")
        try:
            node3.decommission()
        except NodetoolError as exc:
            logger.info(traceback.format_exc())
            error = repr(exc)
            assert "Rejected decommission operation" in error or "Cannot start" in error or "raft_operation_timeout_error" in error
        else:
            raise AssertionError("decommission must fail")

        logger.info("resume gossip communication")
        ip_tables.delete_chain()

        # Wait for node1 and node2 to observe node3's restart (new generation)
        # and fully process the resulting gossip events (including on_restart
        # callbacks in handle_major_state_change) before starting decommission.
        # Without this, the belated generation change detection can kill
        # decommission streaming via stream_manager::on_restart (see SCYLLADB-1018).
        logger.info("wait for other nodes to see node3 as alive")
        for node in [node1, node2]:
            node.watch_rest_for_alive(node3)

        logger.info("decommission node3")
        retry_till_success(node3.decommission, timeout=120)

        logger.info("add new node4")
        node4 = cluster.new_node(4, data_center=node3.data_center, rack=node3.rack)
        node4.start(wait_for_binary_proto=True)
        logger.info("done")

    @pytest.mark.require("jira:SCYLLADB-608")
    @pytest.mark.require("scylladb/scylladb#12892")
    @pytest.mark.skipif(condition=running_in_podman(), reason="can't use iptables within podman")
    def test_removenode_while_gossip_partly_blocked(self, ip_tables):
        """
        restart and remove a node while other nodes can't send gossip communication to it
        """
        logger.info("populating cluster with three nodes")
        cluster = self.cluster
        # add node to 2nd rack, so removenode not failed with tablets
        cluster.populate({"datacenter1": {"rack1": 1, "rack2": 2}})
        logger.info("starting cluster")
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)

        logger.info("stopping node3")
        node1, node2, node3 = cluster.nodelist()
        node3.stop(gently=False)

        logger.info("block gossip communication to node3")
        node1_ip_address = get_ip_from_node(node=node1)
        node2_ip_address = get_ip_from_node(node=node2)
        rule1 = IPTableRule(protocol="tcp", source=f"{node1_ip_address}/32", destination_port=7000, target="DROP")
        rule2 = IPTableRule(protocol="tcp", source=f"{node2_ip_address}/32", destination_port=7000, target="DROP")

        ip_tables.add_rule(rule1)
        ip_tables.add_rule(rule2)

        logger.info("start node3")
        node3.start(wait_for_binary_proto=True, wait_other_notice=False)
        for n in cluster.nodelist():
            stdout, stderr = n.nodetool("status")
            logger.info(stdout)
            logger.info(stderr)

        logger.info("try to remove node 3")
        try:
            node2.nodetool(f"removenode {node3.hostid()}")
        except NodetoolError as exc:
            logger.info(traceback.format_exc())
            # Nodes={127.0.98.1, 127.0.98.2} needed for removenode operation are down.
            assert re.search("Nodes=.* needed for removenode operation are down", repr(exc))
        else:
            raise AssertionError("removenode must fail")

        logger.info("resume gossip communication")
        ip_tables.delete_chain()

        logger.info("stopping node3")
        node3_hostid = node3.hostid()
        node3.stop(gently=False)

        logger.info("remove node 3")
        retry_till_success(node2.nodetool, f"removenode {node3_hostid}", timeout=120)

        logger.info("add new node4")
        node4 = cluster.new_node(4, data_center=node3.data_center, rack=node3.rack)
        node4.start(wait_for_binary_proto=True)
        logger.info("done")

    # test designed to run without raft topology only
    @pytest.mark.required_features("!consistent-topology-changes")
    @pytest.mark.parametrize(
        "log_message,is_removed_from_token_ring", [("left token ring", True), ("Announcing that I have left the ring", False), ("became a group 0 non-voter", False), ("leaving token ring", False)], ids=generate_test_name
    )
    @pytest.mark.use_cassandra_stress
    def test_remove_garbage_members_from_group0_after_abort_decommission(self, log_message, is_removed_from_token_ring, fixture_dtest_setup):
        """Clean group0 from garbage after aborted decommission

        If decommission aborted when node left token ring but stay in group0,
        garbage nodes could affect on raft group0 functionality. Such host_id
        of the nodes have to be removed from group0.
        Decommission process is going to be aborted by kill scylla node.

        If decommission process finished fast or node was already removed
        from token ring and group0, verify that number of node in cluster
        less on 1 and all nodes are voters.
        """
        fixture_dtest_setup.allow_log_errors = True
        self.cluster.set_configuration_options(values={"ring_delay_ms": 3000})
        logger.debug("populating cluster with three nodes")
        cluster: ScyllaCluster = self.cluster
        debug_mode = cluster.scylla_mode == "debug"
        nodeops_watchdog_timeout_seconds = 30 if debug_mode else 10
        cluster.set_configuration_options(values={"nodeops_watchdog_timeout_seconds": nodeops_watchdog_timeout_seconds, "nodeops_heartbeat_interval_seconds": 1})
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        logger.debug("starting cluster")
        cluster.populate(cluster_topology).start(wait_other_notice=True)

        node1 = cluster.nodelist()[0]
        node1, _, node3 = cluster.nodelist()
        node3_hostid = node3.hostid()
        stress_cmd = "write cl=QUORUM n=4000 -schema replication(factor=3) -col size=fixed(200) n=FIXED(5)"
        cluster.stress(stress_cmd.split())

        logger.debug("Verify group0 and token ring members are consistent")
        verify_group0_and_token_ring_members(node1, expected_num_of_members=3)

        logger.debug("Decommission node3 ...")
        marks = {}
        for node in cluster.nodelist():
            marks[node] = node.mark_log()
        node3.nodetool("decommission", capture_output=False, wait=False)
        node3.watch_log_for(log_message, from_mark=marks[node3])
        logger.debug("Abort decommission by killing the node")
        node3.stop(gently=False, wait=False)

        if "left token ring" not in log_message:
            for n in cluster.nodelist():
                if n != node3:
                    n.watch_log_for(rf"decommission.*Removed node=.*{node3.address()} as leaving node", timeout=nodeops_watchdog_timeout_seconds * 2, from_mark=marks[n])

        find_and_clean_garbage_from_group0(node1, node3_hostid, is_removed_from_token_ring, expected_num_of_members=2)

        verify_group0_and_token_ring_members(node1, expected_num_of_members=2)

        logger.debug("Check that new node could be added to cluster")
        node = new_node(cluster, data_center=node3.data_center, rack=node3.rack)
        node.start(wait_other_notice=True)
        verify_group0_and_token_ring_members(node1, expected_num_of_members=3)

    # test designed to run without raft topology only
    @pytest.mark.required_features("!consistent-topology-changes")
    @pytest.mark.parametrize(
        "log_message,is_removed_from_token_ring",
        [("removing node.*from Raft group 0", True), (r"made node.*a non-voter in group 0", False), ("storage_service - Removing tokens", True), ("Started to sync data for removing node", False)],
        ids=generate_test_name,
    )
    @pytest.mark.use_cassandra_stress
    def test_remove_garbage_members_from_group0_after_abort_removenode(self, log_message, is_removed_from_token_ring, fixture_dtest_setup):
        fixture_dtest_setup.allow_log_errors = True
        self.cluster.set_configuration_options(values={"ring_delay_ms": 3000})
        logger.debug("populating cluster with three nodes")
        cluster: ScyllaCluster = self.cluster
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        logger.debug("starting cluster")
        cluster.populate(cluster_topology).start(wait_other_notice=True)
        node1, _, node3 = cluster.nodelist()
        node3_hostid = node3.hostid()
        stress_cmd = "write cl=QUORUM n=4000 -schema replication(factor=3) -col size=fixed(200) n=FIXED(5)"
        cluster.stress(stress_cmd.split())

        logger.debug("Verify group0 and token ring members are consistent")
        verify_group0_and_token_ring_members(node1, expected_num_of_members=3)

        logger.debug("Stop node3 for next removenode operation")
        node3.stop()
        mark = node1.mark_log()
        logger.debug("Start removenode operation for node3 from node1")
        node1.nodetool(f"removenode {node3_hostid}", capture_output=False, wait=False)
        node1.watch_log_for(log_message, from_mark=mark)
        logger.debug("Abort removenode operation after log message with node1 reboot")
        node1.stop()
        node1.start()

        find_and_clean_garbage_from_group0(node1, node3_hostid, is_removed_from_token_ring, expected_num_of_members=2)
        verify_group0_and_token_ring_members(node1, expected_num_of_members=2)

        logger.debug("Check that new node could be added to cluster")
        node = new_node(cluster, data_center=node3.data_center, rack=node3.rack)
        node.start(wait_other_notice=True)
        verify_group0_and_token_ring_members(node1, expected_num_of_members=3)


@pytest.mark.dtest_full
class TestStopNodeEarly(Tester):
    @pytest.mark.parametrize("gently,wait_other_notice", [(False, False), (False, True), (True, False), (True, True)])
    def test_stop_node_while_restarting(self, gently, wait_other_notice):
        """
        Test that other nodes handle node stopping early during initialization
        """
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=2, nodes_per_rack=1)
        cluster.populate(cluster_topology).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(cluster.nodelist()[1])
        create_ks(session, "ks", rf=2)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ALL)

        logger.debug("Restarting node1")
        node1.stop(wait_other_notice=True)
        mark1 = node1.mark_log()
        other_marks = []
        for node in cluster.nodelist()[1:]:
            other_marks.append((node, node.mark_log()))
        node1.start(no_wait=True)
        if wait_other_notice:
            # we need other nodes to first notice the stopping node to be UP
            # before we actually stop the node, if we want other nodes to notice
            # the node to be DOWN during stop procedure.
            msg = f"({node1.address()}|{node1.hostid()}) is now UP"
            logger.debug(f"Waiting for '{msg}'")
            for node, mark in other_marks:
                node.watch_log_for(msg, from_mark=mark)
        else:
            # view update starting is an arbitrary point
            # in the startup sequence known to be early enough,
            # before gossipping starts and others notice the
            # starting node as UP.
            msg = "starting view update generator"
            logger.debug(f"Waiting for '{msg}'")
            node1.watch_log_for(msg, from_mark=mark1)

        expected_errors = [
            "Failed to start a Raft group",
        ]
        self.ignore_log_patterns += expected_errors

        logger.debug(f"Stopping node1 early: gently={gently} wait_other_notice={wait_other_notice}")
        node1.stop(gently=gently, wait_other_notice=wait_other_notice)

        logger.debug("Verifying data")
        result = list(session.execute("SELECT * FROM cf"))
        assert len(result) == 1000


@pytest.mark.dtest_full
@pytest.mark.dtest_long
@pytest.mark.dtest_heavy
class TestLargeScaleCluster(Tester):
    @pytest.mark.timeout(4200)
    @pytest.mark.require("#22244")
    def test_add_many_nodes_under_load(self):  # noqa: PLR0915
        """
        Test large scale cluster (40 nodes cluster, or 12 in debug mode).
        Cluster starts with a starting_size=3 and grow to node_count=50 during a c-s write in the background (low load)
        and c-s read after adding all nodes to make sure all data was written successfully.
        In addition, while adding each node inserting 100 keys and verifying that all keys were written.
        E.Result: All nodes were added and c-s read successfully read all keys

        nodes will be added to 3 racks
        """

        cluster = self.cluster
        debug_mode = isinstance(cluster, ScyllaCluster) and cluster.scylla_mode == "debug"
        node_count = 40 if not debug_mode else 10
        starting_size = 3
        rf = 3
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=rf, nodes_per_rack=1, rack_name_prefix="RACK")
        nodes_str = f"up to {node_count}" if debug_mode else f"{node_count}"
        logger.info(f"Test adding {nodes_str} nodes under load: starting_size={starting_size} rf={rf}")

        timeout = self.cql_timeout(120)

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfere with the test (this must be after the populate)
        config_options = {
            "hinted_handoff_enabled": False,
            "enable_sstable_key_validation": True,
            "range_request_timeout_in_ms": timeout * 1000,
        }
        cluster.set_configuration_options(values=config_options, batch_commitlog=True)
        cluster.populate(cluster_topology).start()
        sessions = dict()
        for n in cluster.nodelist():
            sessions[n] = self.patient_exclusive_cql_connection(n)
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", rf)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})
        create_cf(session, "test", columns={"val": "int"})

        keys = 0
        max_keys = 1000000000 if not debug_mode else 60000
        stress_done = Event()
        add_nodes_done = Event()
        test_keys = 10000
        values = {}
        for i in range(test_keys):
            values[i] = None

        def run():
            nonlocal cluster, sessions, keys, max_keys, stress_done, add_nodes_done, values
            # each iteration just writes a small batch
            # so we check add_nodes_done in a reasonable frequently
            n = 1000

            logger.debug(f"Stress: starting to write up to {max_keys} keys in batches of {n}")
            while keys < max_keys and not add_nodes_done.is_set():
                node = random.choice(list(sessions.keys()))
                session = sessions[node]
                insert_query = session.prepare(f"INSERT INTO ks.test (key, val) VALUES (?, ?)")
                insert_query.consistency_level = ConsistencyLevel.QUORUM
                logger.debug(f"Stress: writing keys {keys}..{keys + n} using {node.name}")
                for k in random.choices(range(test_keys), k=n):
                    v = (values[k] or random.randint(0, 1000000)) + 1
                    session.execute(insert_query, (f"key{k}", v))
                    values[k] = v
                keys += n
            logger.debug(f"Stress: wrote {keys} keys: add_nodes_done={add_nodes_done.is_set()}")
            stress_done.set()

        executor = ThreadPoolExecutor(max_workers=node_count)
        t = executor.submit(run)

        consistency = ConsistencyLevel.ALL
        logger.debug("just before first insert")
        insert_c1c2(session, keys=range(starting_size * 100 + 1000), consistency=ConsistencyLevel.ONE)

        query = SimpleStatement("SELECT key FROM ks.cf", fetch_size=100, consistency_level=consistency)
        logger.debug(f"Starting to add nodes {starting_size + 1} to {node_count + 1}")
        for i in range(starting_size + 1, node_count + 1):
            node_i = new_node(cluster, data_center=node1.data_center, rack=f"RACK{i % rf + 1}")
            node_i.start(wait_for_binary_proto=True, wait_other_notice=True)
            sessions[node_i] = self.patient_exclusive_cql_connection(node_i)
            sessions[node_i].execute("use ks;")
            insert_c1c2(sessions[node_i], keys=range(100000 + i * 2000, 100000 + i * 2000 + 100), consistency=consistency)
            logger.debug(f"added {node_i.name}")

            result = list(session.execute(query, timeout=timeout))
            assert len(result) == i * 100 + 1000, "data loss after increasing size to %d expecting %d rows %d" % (len(cluster.nodelist()), i * 100 + 1000, len(result))

            if stress_done.is_set():
                break
        logger.debug(f"Done adding nodes: stress_done={stress_done.is_set()}")
        add_nodes_done.set()

        t.result()

        logger.debug("Cleanup on all nodes: starting")
        cleanup_futures = []
        for n in cluster.nodelist():
            cleanup_futures.append(executor.submit(n.cleanup))
        for t in cleanup_futures:
            t.result()
        logger.debug("Cleanup on all nodes: done")

        n = keys
        logger.debug(f"Stress: read {n} keys: starting")
        query = session.prepare("SELECT val FROM ks.test WHERE key = ?")
        query.consistency_level = ConsistencyLevel.QUORUM
        for k in range(test_keys):
            res = list(session.execute(query, (f"key{k}",)))
            if values[k] is not None:
                assert len(res) == 1
                assert res[0].val == values[k]
            else:
                assert len(res) == 0
        logger.debug(f"Stress: read {n} keys: done")


from tools.raft_topology import TopologyCoordinatorFinder


@pytest.mark.required_features("consistent-topology-changes")
@pytest.mark.dtest_full
@pytest.mark.parametrize(
    "num_of_racks",
    [pytest.param(3, id="3_racks"), pytest.param(5, id="5_racks")],
)
class TestUpdateClusterLayoutWithRaftTopology(Tester):
    """Test run only with toplogy-consistent-changes feature"""

    def prepare_cluster(self, num_of_racks: int, fixture_dtest_setup):
        fixture_dtest_setup.allow_log_errors = True
        self.cluster.set_configuration_options(values={"ring_delay_ms": 3000, "allowed_repair_based_node_ops": "replace,removenode,rebuild,bootstrap,decommission"})
        logger.debug("populating cluster with three nodes")
        cluster: ScyllaCluster = self.cluster
        debug_mode = cluster.scylla_mode == "debug"
        self.nodeops_watchdog_timeout_seconds = 30 if debug_mode else 10
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=num_of_racks, nodes_per_rack=1)
        # to run removenode/decommission, add additional node to rack3 where node3 is located.
        dc_name = next(iter(cluster_topology.keys()))
        cluster_topology[dc_name][list(cluster_topology[dc_name].keys())[2]] += 1
        cluster.populate(cluster_topology)
        logger.debug("starting cluster")
        cluster.start(wait_other_notice=True)
        self.coordinator_finder = TopologyCoordinatorFinder(self)
        self.coordinator_finder.wait_topology_coordinator_elected()

    def run_stress_to_populate_cluster(self, num_of_nodes: int):
        rf = num_of_nodes
        stress_cmd = f"write cl=QUORUM n=4000 -schema replication(factor={rf}) -col size=fixed(200) n=FIXED(5)"
        self.cluster.stress(stress_cmd.split())

    # Waits for the removenode topology operation for the given node_id to complete.
    # The operation may complete in one of three ways: success, rollback, or cancel.
    #
    # - Success: the node was removed successfully.
    # - Rollback: the operation was started but rolled back because some node went down
    #   during streaming.
    # - Cancel: the operation was cancelled because some node was already down before
    #   the operation started.
    #
    # The function detects the outcome by searching for specific log entries.
    # We cannot use node.watch_log_for, because any node may become the topology coordinator,
    # so we must grep the logs on all nodes.
    #
    # The function returns true if the removenode succeeded and false otherwise.
    @staticmethod
    def wait_for_removenode(removed_node_id: str, log_marks: dict[ScyllaNode, int], timeout: int = 600) -> bool:
        start_ts = time.time()
        deadline = start_ts + timeout
        last_log_ts = 0.0
        expr = (
            rf"(?P<success>raft_topology - updating topology state: finished removing node {removed_node_id})"
            r"|"
            rf"(?P<rollback>raft_topology - updating topology state: complete rollback of {removed_node_id} to state normal after removing failure)"
            r"|"
            rf"(?P<cancel>raft_topology - updating topology state: cancel all topology requests)"
        )
        while True:
            ts = time.time()
            if ts > deadline:
                raise TimeoutError(f"timeout waiting for removenode {removed_node_id} to finish, start time {datetime.fromtimestamp(start_ts):%F %T}, current time {datetime.fromtimestamp(ts):%F %T}")

            for node, mark in log_marks.items():
                matches = node.grep_log(expr, from_mark=mark)
                if len(matches) > 0:
                    _, match = matches[0]
                    logger.debug(f"wait_for_removenode: done waiting for removenode of {removed_node_id} to finish, match: {match.group(0)}")
                    return match.group("success") is not None

            if ts - last_log_ts >= 1.0:
                last_log_ts = ts
                logger.debug(f"wait_for_removenode: waiting for removenode of {removed_node_id} to finish")
            time.sleep(0.1)

    @staticmethod
    def is_streaming_completed_for_decommission(coordinator: ScyllaNode, from_mark: int | None = None) -> bool:
        logger.debug("Check whether the streaming was realy finished and coordinator got approval")
        try:
            coordinator.watch_log_for("raft_topology - updating topology state: decommissioning: streaming completed", from_mark, timeout=60)
            return True
        except TimeoutError:
            logger.debug("Streaming was not finished")
        return False

    @staticmethod
    def is_decommission_failed(node: ScyllaNode, from_mark: int | None = None) -> bool:
        logger.debug("Check whether decommission failed")
        try:
            node.watch_log_for("raft_topology - Decommission failed. See earlier errors", from_mark=from_mark, timeout=90)
            return True
        except TimeoutError:
            logger.debug("Decommission was not aborted")
        return False

    @pytest.mark.parametrize(
        "log_message",
        [
            r"raft_topology - updating topology state: start removenode",
            r"repair - removenode_with_repair",
            r"removenode: waiting for completion",
            r"raft_group0 - making server.*non-voter",
            r"raft_topology - start streaming",
            r"raft_topology - streaming completed",
        ],
        ids=generate_test_name,
    )
    @pytest.mark.use_cassandra_stress
    # FIXME: https://github.com/scylladb/scylla-dtest/issues/5310
    @pytest.mark.cluster_options(enable_small_table_optimization_for_rbno=False)
    def test_no_garbage_members_left_after_abort_removenode_by_kill_coordinator(self, log_message, num_of_racks, fixture_dtest_setup):
        """Raft topology should no left any garbage after removenode was aborted

        If coordinator got approval that streaming finished, then removing node
        will be fully removed from cluster even if coordinator node was restarted

        If removenode aborted when there is no quorum for electing new topology coordinator,
        removenode operation will continue after coordinator node started again.

        If removenode aborted and there is a quorum for electing new topology coordinator,
        then the removing node will be banned by rest nodes of the cluster, and it have to
        be removed again before adding new node

        Group0 and token ring shouldn't have any garbage node after operation finished
        """
        cluster: ScyllaCluster = self.cluster
        rf = num_of_racks
        self.prepare_cluster(num_of_racks, fixture_dtest_setup)
        num_of_nodes = len(cluster.nodelist())
        self.run_stress_to_populate_cluster(rf)

        coordinator_node: ScyllaNode = self.coordinator_finder.get_topology_coordinator_node()
        node3: ScyllaNode = cluster.nodelist()[2]
        node3_hostid = node3.hostid()

        logger.debug("Verify group0 and token ring members are consistent")
        verify_group0_and_token_ring_members(coordinator_node, expected_num_of_members=num_of_nodes)

        logger.debug("Stop node3 for next removenode operation")
        node3.stop(wait_other_notice=True)
        log_marks = {node: node.mark_log() for node in self.cluster.nodelist()}

        logger.debug("Start removenode operation for node3 from coordinator")
        coordinator_node.nodetool(f"removenode {node3_hostid}", capture_output=False, wait=False)
        coordinator_node.watch_log_for(log_message, from_mark=log_marks[coordinator_node])

        logger.debug("Abort removenode operation after log message by reboot coordinator node")
        coordinator_node.stop(gently=False, wait_other_notice=True)

        logger.debug("Restart coordinator node")
        coordinator_node.start(wait_other_notice=True)

        logger.debug("Waiting electing of new coordinator")
        self.coordinator_finder.wait_topology_coordinator_elected()
        coordinator_node = self.coordinator_finder.get_topology_coordinator_node()

        succeeded = self.wait_for_removenode(node3_hostid, log_marks)

        if succeeded:
            logger.debug("Wait consistency after removenode operation was finished")
            wait_for_token_ring_and_group0_consistency(coordinator_node, expected_num_of_members=num_of_nodes - 1)
        else:
            logger.debug("Node3 is now banned, so remove it before bootstrap new node")
            coordinator_node.nodetool(f"removenode {node3_hostid}", capture_output=False, wait=True)
            wait_for_token_ring_and_group0_consistency(coordinator_node, expected_num_of_members=num_of_nodes - 1)

        logger.debug("Check that new node could be added to cluster")
        node = new_node(cluster, data_center=node3.data_center, rack=node3.rack)
        node.start(wait_other_notice=True)
        verify_group0_and_token_ring_members(coordinator_node, expected_num_of_members=num_of_nodes)

    @pytest.mark.parametrize(
        "log_message",
        [
            r"raft_topology - updating topology state: start removenode",
            r"repair - removenode_with_repair",
            r"raft_group0 - making server.*non-voter",
            r"raft_topology - start streaming",
            r"raft_topology - streaming completed",
        ],
        ids=generate_test_name,
    )
    @pytest.mark.use_cassandra_stress
    # FIXME: https://github.com/scylladb/scylla-dtest/issues/5310
    @pytest.mark.cluster_options(enable_small_table_optimization_for_rbno=False)
    def test_no_garbage_members_left_after_abort_removenode_by_kill_peer_node(self, log_message, num_of_racks, fixture_dtest_setup):
        """Raft topology should no left any garbage after removenode was aborted

        If coordinator got approval that streaming finished, then removing node
        will be fully removed from cluster even if coordinator node was restarted

        If removenode aborted when there is no quorum for electing new topology coordinator,
        removenode operation will continue after coordinator node started again.

        If removenode aborted and there is a quorum for electing new topology coordinator,
        then the removing node will be banned by rest nodes of the cluster, and it have to
        be removed again before adding new node

        Group0 and token ring shouldn't have any garbage node after operation finished
        """
        cluster: ScyllaCluster = self.cluster
        rf = num_of_racks
        self.prepare_cluster(num_of_racks, fixture_dtest_setup)
        num_of_nodes = len(cluster.nodelist())
        self.run_stress_to_populate_cluster(rf)

        coordinator_node: ScyllaNode = self.coordinator_finder.get_topology_coordinator_node()
        node3: ScyllaNode = cluster.nodelist()[2]
        node3_hostid = node3.hostid()

        logger.debug("Verify group0 and token ring members are consistent")
        verify_group0_and_token_ring_members(coordinator_node, expected_num_of_members=num_of_nodes)

        peers_nodes: list[ScyllaNode] = [node for node in cluster.nodelist() if node not in (node3, coordinator_node)]

        logger.debug("Stop node3 for next removenode operation")
        node3.stop()
        log_marks = {node: node.mark_log() for node in self.cluster.nodelist()}

        logger.debug("Start removenode operation for node3 from topology coordinator")
        coordinator_node.nodetool(f"removenode {node3_hostid}", capture_output=False, wait=False)
        coordinator_node.watch_log_for(log_message, from_mark=log_marks[coordinator_node])

        logger.debug("Abort removenode operation after log message with peer node reboot")
        peers_nodes[0].stop(gently=False, wait_other_notice=True)

        logger.debug("Check whether streaming was finished and coordinator received confirmation")
        succeeded = self.wait_for_removenode(node3_hostid, log_marks)

        logger.debug("Start stopped peer node %s", peers_nodes[0].name)
        peers_nodes[0].start(wait_other_notice=True)

        if succeeded:
            logger.debug("Wait consistency after removenode was finished")
            wait_for_token_ring_and_group0_consistency(coordinator_node, expected_num_of_members=num_of_nodes - 1)
        else:
            logger.debug("Removenode operation failed.")
            coordinator_node.watch_log_for("raft_topology - Removenode failed. See earlier errors", from_mark=log_marks[coordinator_node], timeout=60)

            logger.debug("Node3 is now banned, so remove it before bootstrap new node")
            coordinator_node.nodetool(f"removenode {node3_hostid}", capture_output=False, wait=True)
            wait_for_token_ring_and_group0_consistency(coordinator_node, expected_num_of_members=num_of_nodes - 1)

        logger.debug("Check that new node could be added to cluster")
        node = new_node(cluster, data_center=node3.data_center, rack=node3.rack)
        node.start(wait_other_notice=True)
        verify_group0_and_token_ring_members(coordinator_node, expected_num_of_members=num_of_nodes)

    @pytest.mark.parametrize(
        "log_message",
        ["raft_topology - streaming completed", "storage_service - DECOMMISSIONING: done", "raft_topology - start streaming", r"repair - repair.*: completed successfully"],
        ids=generate_test_name,
    )
    @pytest.mark.use_cassandra_stress
    # FIXME: https://github.com/scylladb/scylla-dtest/issues/5310
    @pytest.mark.cluster_options(enable_small_table_optimization_for_rbno=False)
    def test_no_garbage_left_after_abort_decommission_by_kill_decommissioned_node(self, log_message, num_of_racks, fixture_dtest_setup):
        """Raft topology should no left any garbage after decommission was aborted

        If decommission aborted when streaming done, raft topology
        should remove node from cluster totally

        If decommission aborted before or during streaming, rollback procedure
        should return decommissionning node back to cluster

        Decommission process is going to be aborted by kill scylla node.
        """
        cluster: ScyllaCluster = self.cluster
        cluster.set_configuration_options({"allowed_repair_based_node_ops": "replace,removenode,rebuild,bootstrap,decommission"})
        rf = num_of_racks
        self.prepare_cluster(num_of_racks, fixture_dtest_setup)
        num_of_nodes = len(cluster.nodelist())
        self.run_stress_to_populate_cluster(rf)

        coordinator_node = self.coordinator_finder.get_topology_coordinator_node()
        node3: ScyllaNode = cluster.nodelist()[2]

        logger.debug("Verify group0 and token ring members are consistent")
        verify_group0_and_token_ring_members(coordinator_node, expected_num_of_members=num_of_nodes)

        logger.debug("Decommission node3 and abort it by killing the node where decommission is running")
        marks = {node: node.mark_log() for node in cluster.nodelist()}
        node3.nodetool("decommission", capture_output=False, wait=False)
        node3.watch_log_for(log_message, from_mark=marks[node3])
        node3.stop(gently=False, wait_other_notice=False)

        logger.debug("Check whether streaming is done and coordinator recieved confirmation")
        streaming_done = self.is_streaming_completed_for_decommission(coordinator_node, marks[coordinator_node])

        if streaming_done:
            logger.debug("Wait while all nodes in cluster update status of decommissioned node")
            if "tablets" not in self.scylla_features:
                for node in [node for node in self.cluster.nodelist() if node != node3]:
                    node.watch_log_for(rf"gossip - Removed endpoint ({node3.address()}|{node3.hostid()})", timeout=self.nodeops_watchdog_timeout_seconds * 2, from_mark=marks[node])
                logger.debug("Verify that decommissioned node removed from cluster")
            wait_for_token_ring_and_group0_consistency(coordinator_node, expected_num_of_members=num_of_nodes - 1)
        else:
            logger.debug("Verify that node left in cluster")
            wait_for_token_ring_and_group0_consistency(coordinator_node, expected_num_of_members=num_of_nodes)
            logger.debug("Start node after aborted decommission")
            node3.start()
            logger.debug("Verify that all nodes are in cluster")
            wait_for_token_ring_and_group0_consistency(coordinator_node, expected_num_of_members=num_of_nodes)
            # increase number of expected nodes in cluster before add new node
            num_of_nodes += 1

        logger.debug("Check that new node could be added to cluster")
        node = new_node(cluster, data_center=node3.data_center, rack=node3.rack)
        node.start(wait_other_notice=True)
        verify_group0_and_token_ring_members(coordinator_node, expected_num_of_members=num_of_nodes)

    @pytest.mark.parametrize(
        "log_message",
        ["raft_topology - streaming completed", "storage_service - DECOMMISSIONING: done", "raft_topology - start streaming", r"repair - repair.*: completed successfully"],
        ids=generate_test_name,
    )
    @pytest.mark.use_cassandra_stress
    # FIXME: https://github.com/scylladb/scylla-dtest/issues/5310
    @pytest.mark.cluster_options(enable_small_table_optimization_for_rbno=False)
    def test_no_garbage_left_after_abort_decomission_by_kill_coordinator(self, log_message, num_of_racks, fixture_dtest_setup):
        """Raft topology should no left any garbage after decommission was aborted
        when topology coordinator was killed

        If decommission aborted when streaming done and the coordinator managed to receive confirmation
        about streaming done before kill, decomissioned node should be remove node from cluster totally

        If decommission aborted before or during streaming, rollback procedure
        should return decommissionning node back to cluster and after coordinator node
        would be restarted, no any garbage should be in group0.
        """
        cluster: ScyllaCluster = self.cluster
        rf = num_of_racks
        self.prepare_cluster(num_of_racks, fixture_dtest_setup)
        num_of_nodes = len(cluster.nodelist())
        self.run_stress_to_populate_cluster(rf)

        node3: ScyllaNode = self.cluster.nodelist()[2]
        coordinator_node: ScyllaNode = self.coordinator_finder.get_topology_coordinator_node()

        logger.debug("Verify group0 and token ring members are consistent")
        verify_group0_and_token_ring_members(coordinator_node, expected_num_of_members=num_of_nodes)

        logger.debug("Decommission node3 and abort it by killing the node where decommission is running")
        marks = {node: node.mark_log() for node in self.cluster.nodelist()}
        node3.nodetool("decommission", capture_output=False, wait=False)
        node3.watch_log_for(log_message, from_mark=marks[node3])
        coordinator_node.stop(gently=False)

        logger.debug("Check whether streaming is done and coordinator recieved confirmation")
        streaming_done = self.is_streaming_completed_for_decommission(coordinator_node, marks[coordinator_node])
        decommission_failed = self.is_decommission_failed(node3, marks[node3])

        logger.debug("Start old coordinator node")
        coordinator_node.start(wait_other_notice=False)
        alive_nodes = [node for node in self.cluster.nodelist() if node not in (node3, coordinator_node)]
        for node in alive_nodes:
            node.watch_rest_for_alive(nodes=[coordinator_node])
        coordinator_node.watch_rest_for_alive(nodes=alive_nodes)

        logger.debug("Find new coordinator node")
        self.coordinator_finder.wait_topology_coordinator_elected()
        old_coordinator, coordinator_node = coordinator_node, self.coordinator_finder.get_topology_coordinator_node()

        # recheck streaming was finished if new coordinator was elected
        if not streaming_done:
            streaming_done = self.is_streaming_completed_for_decommission(coordinator_node, marks[coordinator_node])

        if streaming_done and not decommission_failed:
            logger.debug("Wait while all nodes in cluster update status of decommissioned node")
            if "tablets" not in self.scylla_features:
                for node in [node for node in self.cluster.nodelist() if node not in (node3, old_coordinator)]:
                    node.watch_log_for(rf"gossip - Removed endpoint ({node3.address()}|{node3.hostid()})", timeout=self.nodeops_watchdog_timeout_seconds * 3, from_mark=marks[node])
                logger.debug("Verify that decommissioned node removed from cluster")
            node3.watch_log_for(rf"DECOMMISSIONING: done", timeout=self.nodeops_watchdog_timeout_seconds * 3, from_mark=marks[node3])
            coordinator_node = self.coordinator_finder.get_topology_coordinator_node()
            wait_for_token_ring_and_group0_consistency(coordinator_node, expected_num_of_members=num_of_nodes - 1)
            logger.debug("Stop node3 because it was decommissioned")
            node3.stop()
        else:
            logger.debug("Verify that node left in cluster and stay alive")
            wait_for_token_ring_and_group0_consistency(coordinator_node, expected_num_of_members=num_of_nodes)
            # increase number of expected nodes in cluster before add new node
            num_of_nodes += 1

        logger.debug("Check that new node could be added to cluster")
        node = new_node(cluster, data_center=node3.data_center, rack=node3.rack)
        node.start(wait_other_notice=True)
        verify_group0_and_token_ring_members(coordinator_node, expected_num_of_members=num_of_nodes)
