#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging
import os
import random
import re
import shutil
import string
import time
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from subprocess import getoutput, getstatusoutput
from typing import Any, Optional

import pytest
from cassandra import ConsistencyLevel, InvalidRequest, Unavailable
from cassandra.cluster import NoHostAvailable
from cassandra.query import SimpleStatement
from ccmlib.node import Node, NodetoolError
from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.scylla_node import ScyllaNode

from dtest_class import Tester, create_cf, create_ks, get_ip_from_node, wait_for
from dtest_setup_overrides import DTestSetupOverrides
from tools import commitlog
from tools.assertions import assert_row_count
from tools.cluster import run_rest_api
from tools.cluster_topology import generate_cluster_topology
from tools.data import insert_c1c2, query_c1c2
from tools.files import get_node_cf_dir, remove_files_in_folder
from tools.marks import issue_open, with_feature
from tools.metrics import get_node_metrics
from tools.misc import ImmutableMapping, dump_sstables
from tools.schema import change_schema_safely

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function", autouse=True)
def fixture_dtest_setup_overrides(dtest_config):
    dtest_setup_overrides = DTestSetupOverrides()
    dtest_setup_overrides.cluster_options = ImmutableMapping(
        {
            "logger_log_level": {"compaction": "debug"}  # so we see compaction start/end log messages
        }
    )
    return dtest_setup_overrides


def parallel_repair_on_nodes(nodes: list[ScyllaNode], keyspace: str, tables: list[str] | None = None, partitioner_range: bool = False) -> None:
    with ThreadPoolExecutor(max_workers=len(nodes)) as pool:
        threads = []
        for node in nodes:
            threads.append(pool.submit(node.repair, keyspace=keyspace, tables=tables, partitioner_range=partitioner_range))
        for thread in threads:
            thread.result()


class RepairAdditionalBase(Tester):
    KEYSPACE_NAME = "ks"
    TABLE_NAME = "cf"
    NUM_OF_COLUMNS = 50
    NUM_OF_NODES = 3
    RF = 3
    NUM_OF_PEERS = RF - 1
    LIST_ROW_LEVEL_REPAIR_METRICS = ["tx_row_nr", "rx_row_nr", "tx_hashes_nr", "rx_hashes_nr"]
    PARTITIONS = 100
    ROWS_IN_PARTITION = 20
    BIG_PARTITION_ROWS = 10000
    OFF_STRATEGY_REPAIR_TIMEOUT = 300

    # Match a comma-separated list of ip-addresses or host UUIDs, surrounded by parenthesis or square brackets.
    # For example, rf"Started Row Level Repair \(Master\).+ peers={self.PEERS_RE}" needs to match the following string:
    #     Started Row Level Repair (Master): local=b5804090-4550-450f-9ac5-8d4bf5faf285, peers=[90c50329-5414-45a0-8f2b-525775e6cfb2, 8fbd0137-b255-4c25-8896-91c1670e39ac, 86357706-ae6d-4ed6-860c-5a8012681c07]
    PEERS_RE = r"[\[(](?P<peers>(?:(?:,\s*)?[\w.-]+)*)[)\]]"

    @staticmethod
    def default_config_options():
        return {"hinted_handoff_enabled": False, "enable_sstable_key_validation": True}

    def check_rows_on_node(self, node_to_check, rows, found=None, missings=None, consistency_level=ConsistencyLevel.ONE):
        if found is None:
            found = []
        if missings is None:
            missings = []

        logger.debug(f"check_rows_on_node[{node_to_check.name}]: Stopping cluster and restarting node")

        # restarting node_to_check would run
        # reshape compaction, if needed post repair.
        self.cluster.stop()
        node_to_check.start()
        cs = self.patient_cql_cluster_session(node_to_check, "ks", exclusive=True, consistency_level=consistency_level)
        session = cs.session
        logger.debug(f"check_rows_on_node[{node_to_check.name}]: Querying data, expected to get {rows} rows")
        query = SimpleStatement("SELECT * FROM cf LIMIT %d" % (rows * 2), consistency_level=consistency_level)
        result = list(session.execute(query))
        assert len(result) == rows, len(result)

        if found:
            logger.debug(f"check_rows_on_node[{node_to_check.name}]: Verifying {len(found)} keys that must exist: [{found[0]}..{found[-1]}]")
        for k in found:
            query_c1c2(session, k, consistency_level)

        if missings:
            logger.debug(f"check_rows_on_node[{node_to_check.name}]: Verifying {len(missings)} keys that must not exist: [{missings[0]}..{missings[-1]}]")
        for k in missings:
            query = SimpleStatement("SELECT c1, c2 FROM cf WHERE key='k%d'" % k, consistency_level=consistency_level)
            res = list(session.execute(query))
            assert len(filter(lambda x: len(x) != 0, res)) == 0, res

    def get_value(self, log_string, key):
        pattern = rf"{key}=(\d+\.\d+|\d+)"
        matches = re.findall(pattern, log_string)
        if matches:
            return float(matches[0]) if "." in matches[0] else int(matches[0])
        else:
            return 0

    def get_repair_rpc_calls(self, node_to_check):
        nr = 0
        for line in node_to_check.grep_log("stats: repair_reason=repair"):
            nr += self.get_value(line[0], "rpc_call_nr")
        return nr

    def get_repair_duration(self, node_to_check):
        nr = 0.0
        for line in node_to_check.grep_log("stats: repair_reason=repair"):
            nr = max(nr, self.get_value(line[0], "duration"))
        return nr

    def check_repair_tx_rx_rows(self, node_to_check, expected_tx_row_nr, expected_rx_row_nr):
        tx = 0
        rx = 0
        for _line in node_to_check.grep_log("stats: repair_reason=repair"):
            line = _line[0]
            logger.debug(line)
            kv = re.findall(r"tx_row_nr=\d*", line)[0].split("=")
            logger.debug(kv)
            tx += int(kv[1])
            kv = re.findall(r"rx_row_nr=\d*", line)[0].split("=")
            logger.debug(kv)
            rx += int(kv[1])
        assert tx == expected_tx_row_nr
        assert rx == expected_rx_row_nr

    def _stop_all_nodes_except_for(self, node):
        logger.debug(f"Stopping all nodes except for: {node.name}")

        for c_node in [n for n in self.cluster.nodelist() if n != node]:
            c_node.flush()
            c_node.stop(wait_other_notice=True)

    def _start_all_nodes_except_for(self, node):
        logger.debug(f"Starting all nodes except for: {node.name}")
        for c_node in [n for n in self.cluster.nodelist() if n != node]:
            c_node.start(wait_other_notice=True, wait_for_binary_proto=True)

    def create_update_command(self, column_expr, pk, ck, table_name=TABLE_NAME):
        cql_update_cmd = f"update {table_name} set {column_expr} where pk={pk} and ck={ck}"
        logger.debug(f"Generated CQL: {cql_update_cmd}")
        return cql_update_cmd

    def create_insert_command(self, pk, ck, table_name=TABLE_NAME):
        stmt = f"insert into {table_name} (pk, ck) values ({pk}, {ck})"
        logger.debug(f"Generated CQL: {stmt}")
        return stmt

    def verify_num_of_rows_on_nodes(self, list_nodes, total_rows):
        for node in list_nodes:
            self._verify_num_of_rows_on_node(node=node, total_rows=total_rows)

    def _verify_num_of_rows_on_node(self, node, total_rows):
        # Check for correct number of rows on node
        logger.debug(f"Check for {total_rows} rows on node {node.name}...")
        self.check_rows_on_node(node, total_rows)
        logger.debug("Verify rows number is done")

    def verify_repair_tx_rx_rows(self, node_idx, expected_tx_row_nr, expected_rx_row_nr, list_metrics):
        metrics_res = get_node_metrics(node_ip=self.cluster.get_node_ip(node_idx), metrics=list_metrics)
        for metric in list_metrics:
            if metric not in metrics_res:
                metrics_res[metric] = "N/A"
        logger.debug(f"Check expected rx ({expected_rx_row_nr}) tx ({expected_tx_row_nr}) rows.")
        assert metrics_res["tx_row_nr"] <= expected_tx_row_nr, "TX rows {} is not as expected: {}".format(metrics_res["tx_row_nr"], expected_tx_row_nr)
        assert metrics_res["rx_row_nr"] <= expected_rx_row_nr, "RX rows {} is not as expected: {}".format(metrics_res["rx_row_nr"], expected_rx_row_nr)
        if expected_rx_row_nr > 0:
            assert metrics_res["rx_row_nr"] > 0, "No received rows found ({})".format(metrics_res["rx_row_nr"])
        if expected_tx_row_nr > 0:
            assert metrics_res["tx_row_nr"] > 0, "No transferred rows found ({})".format(metrics_res["tx_row_nr"])

    def create_cluster_and_keyspace(self, num_of_nodes, rf, configuration_options=None):
        assert 0 < rf, "Trying to create a keyspace that does not replicate data"
        assert rf <= num_of_nodes, "Creating a cluster with RF greater than the number of nodes is impossible with `rf_rack_valid_keyspaces` set to true"

        if configuration_options:
            self.cluster.set_configuration_options(values=configuration_options)

        rack_layout = {f"rack{i}": num_of_nodes // rf for i in range(1, rf + 1)}
        for i in range(1, num_of_nodes % rf + 1):
            rack_layout[f"rack{i}"] += 1

        self.cluster.populate({"dc1": rack_layout}).start()
        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", rf=rf)
        return session

    def prefill_table_data(  # noqa: PLR0913
        self,
        session,
        partition_range_end,
        rows_in_partition,
        partition_range_start=1,
        table_name=TABLE_NAME,
        num_of_columns=NUM_OF_COLUMNS,
    ):
        logger.debug(f"Create {partition_range_end} partitions of {num_of_columns} columns with {rows_in_partition} rows")
        for i in range(partition_range_start, partition_range_end + 1):
            for k in range(1, rows_in_partition + 1):
                random_string = "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
                stmt = "insert into {table_name} (pk, ck, {columns}, clist, cset, cmap) values ({ilist}, {klist}, {int_values}, [{ilist}, {klist}], {open}{set_value}{close}, {map_value})".format(
                    table_name=table_name,
                    columns=", ".join("c%d" % l for l in range(1, num_of_columns)),
                    int_values=", ".join("%d" % l for l in range(1, num_of_columns)),
                    ilist=i,
                    klist=k,
                    open="{'",
                    set_value=random_string,
                    close="'}",
                    map_value="{%d: '%s'}" % (k, random_string),
                )
                session.execute(stmt)

    def write_table_updates(  # noqa: PLR0913
        self,
        node,
        partitions_range_end,
        rows_in_partition,
        num_of_updates,
        partitions_range_start=1,
        keyspace=KEYSPACE_NAME,
        int_columns=NUM_OF_COLUMNS,
    ):
        logger.debug(f"Updating table data through node {node.name}...")
        session = self.patient_cql_connection(node)
        session.set_keyspace(keyspace)
        stmts = []
        logger.debug(f"Going to generate {num_of_updates} CQL updates, via node {node.name} for partition range of: {partitions_range_start} - {partitions_range_end}")
        for i in range(1, num_of_updates + 1):
            # Update/delete int columns to a random big partition
            column = random.randint(1, int_columns - 1)
            column_name = f"c{column}"
            new_value = random.choice(["NULL", random.randint(0, 500000)])
            column_expr = f"{column_name} = {new_value}"
            logger.debug(f"#{i} cmd - ")
            pk = random.randint(partitions_range_start, partitions_range_end)
            stmts.append(self.create_update_command(column_expr=column_expr, pk=pk, ck=random.randint(1, rows_in_partition)))

        for stmt in stmts:
            session.execute(stmt)

    def _run_repair_api(
        self,
        run_on_node: ScyllaNode,
        keyspace: str,
        ignore_nodes: list | None = None,
        await_completion: bool = True,
        small_table_optimization: bool = False,
    ):
        """
        :param run_on_node: node to send the REST API command.
        :param keyspace: mandatory parameter for repair.
        :param ignore_nodes: list of nodes to be excluded by repair.
        :param await_completion: wait for repair command to complete or continue immediately.
        :return:
        """
        repair_cmd = f"/storage_service/repair_async/{keyspace}"
        params = {}
        if ignore_nodes:
            ignore_nodes_ips = ",".join(node.address() for node in ignore_nodes)
            params["ignore_nodes"] = f"{ignore_nodes_ips}"
        if small_table_optimization:
            params["small_table_optimization"] = "true"

        result = run_rest_api(run_on_node=run_on_node, cmd=repair_cmd, params=params)
        timeout = 120 if self.cluster.scylla_mode != "debug" else 360
        if await_completion:
            self.wait_for_repair(run_on_node=run_on_node, repair_id=result.json(), timeout=timeout)

    def _setup_cluster_with_table(self):
        cluster = self.cluster
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False}, batch_commitlog=True)
        cluster.populate(generate_cluster_topology(dc_num=1, rack_num=3)).start()
        node1 = cluster.nodelist()[0]
        keyspace = "ks"
        table = "cf"
        with self.patient_cql_connection(node1) as session:
            # Create keyspace and table.
            create_ks(session, keyspace, 3)
            create_cf(session, table, read_repair=0.0, columns={"c1": "text", "c2": "text"})
        return keyspace, table

    @staticmethod
    def wait_for_repair(run_on_node, repair_id, timeout=120):
        repair_id = str(repair_id)
        await_cmd = f"/storage_service/repair_status"
        await_params = {"id": repair_id}
        wait_for(func=lambda: run_rest_api(run_on_node=run_on_node, cmd=await_cmd, params=await_params, api_method="get").json() == "SUCCESSFUL", timeout=timeout, text=f"[{run_on_node.name}] Waiting for repair {repair_id} completion..")

    @staticmethod
    def _run_repair_and_check_completed(node, ks: str, cf: str, from_mark: int, aux_cf: str | None = None):
        repair_logs = [
            "repair - repair.*: Started to shutdown off-strategy compaction updater",
            "repair - repair.*: Finished to shutdown off-strategy compaction updater",
            "repair - repair.* completed successfully",
        ]

        logger.debug("Start repair on the %s.%s", ks, cf)
        node.repair(keyspace=ks, tables=[cf])

        logger.debug("Check if repair completed successfully")
        assert node.watch_log_for(
            exprs=repair_logs,
            from_mark=from_mark,
            timeout=120,
        ), "Failed to wait for repair logs"

        if aux_cf:
            logger.debug("Run repair on other table to make sure that repair on one table does not affect off-strategy compaction timer on other table")
            node.repair(keyspace=ks, tables=[aux_cf])

    def _run_repair_and_wait_for_compactions(self, node, ks: str, cf: str, aux_cf: str | None = None):
        off_strategy_compaction_logs = [f"Starting off-strategy compaction for {ks}.{cf}", f"Done with off-strategy compaction for {ks}.{cf}"]
        from_mark = node.mark_log()
        self._run_repair_and_check_completed(node=node, ks=ks, cf=cf, from_mark=from_mark, aux_cf=aux_cf)

        try:
            run_rest_api(node, f"/storage_service/keyspace_offstrategy_compaction/{ks}", params={"cf": cf})
        except Exception as e:  # noqa: BLE001
            logger.warn(f"Triggering off-strategy compaction on {node.name} failed: {e}")

        logger.debug("Wait till off-strategy compactions have been started and completed")
        assert node.watch_log_for(
            exprs=off_strategy_compaction_logs,
            from_mark=from_mark,
            timeout=self.OFF_STRATEGY_REPAIR_TIMEOUT + 60,
        ), f"Off-strategy compactions did not start after {self.OFF_STRATEGY_REPAIR_TIMEOUT // 60} minutes"

    def _repair_disjoint_data_test(self):
        """
        On each of three replicas, insert completely different data.
        Confirm that repairing a single of these nodes brings all the data
        to all three replicas.
        """
        logger.debug("Starting cluster...")
        # Disable hinted handoff so it doesn't do what we expect repair to do
        self.cluster.set_configuration_options(values=self.default_config_options())
        # Create a cluster of 3 nodes, and a keyspace with RF=3 on all nodes
        # (disable read repair, as we want to test the full repair).
        self.cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3 = self.cluster.nodelist()
        with self.patient_cql_connection(node1) as session:
            create_ks(session, "ks", 3)
            create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        # Insert 1000 keys *only* on node 1, another 1000 keys *only* on node 2,
        # another 1000 *only on node 3:
        logger.debug("Adding data only on node 1...")
        node2.flush()
        node2.stop(wait_other_notice=True)
        node3.flush()
        node3.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node1, "ks") as session1:
            insert_c1c2(session1, keys=range(1000, 2000), consistency=ConsistencyLevel.ONE)
        self.cluster.flush()
        logger.debug("Adding data only on node 2...")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            insert_c1c2(session2, keys=range(2000, 3000), consistency=ConsistencyLevel.ONE)
        logger.debug("Adding data only on node 3...")
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        node2.flush()
        node2.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node3, "ks") as session3:
            insert_c1c2(session3, keys=range(3000, 4000), consistency=ConsistencyLevel.ONE)

        # Bring up all 3 nodes, each should have different data
        self.cluster.start_nodes([node1, node2], wait_other_notice=True, wait_for_binary_proto=True)

        # Run repair on (arbitrarily), node 3
        time.sleep(10)  # see CASSANDRA-4373
        logger.debug("starting repair...")
        info = node3.repair(keyspace="ks")
        logger.debug(info[0])
        logger.debug(info[1])

        # Check that all nodes have all data
        self.check_rows_on_node(node1, 3000)
        self.check_rows_on_node(node2, 3000)
        self.check_rows_on_node(node3, 3000)

    def _repair_schema_test(self):
        """
        In a keyspace with three replicas, insert a new column family on two
        replicas only (while the third node is down), and initiate repair from
        the node with the data. Verify that the data (and its schema) have been
        correctly replicated to the third node.
        """
        logger.debug("Starting cluster...")
        # Start a cluster of two nodes, and create a keyspace with RF=2.
        # Do *not* create a table yet - we'll do that with one node down
        self.cluster.set_configuration_options(values=self.default_config_options())
        self.cluster.populate(generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 3)

        # Take node2 down, and create a new table and data on node1 only.
        logger.debug("Creating table and data only on node 1 and 3...")
        node2.flush()
        node2.stop(wait_other_notice=True)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})
        insert_c1c2(session, keys=range(1000, 2000), consistency=ConsistencyLevel.QUORUM)

        # At this point node2 is not only missing some data, it is actually
        # missing an entire table. Let's bring node2 back up, start repair on
        # node1, and see if node2 gets the new table, and all its data.
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        time.sleep(10)  # see CASSANDRA-4373
        logger.debug("starting repair on node1...")
        info = node1.repair(keyspace="ks")
        logger.debug(info[0])
        logger.debug(info[1])

        # Check that all nodes have all data
        logger.debug("checking data on node1...")
        self.check_rows_on_node(node1, 1000)
        logger.debug("checking data on node2...")
        self.check_rows_on_node(node2, 1000)
        logger.debug("checking data on node3...")
        self.check_rows_on_node(node3, 1000)

        self.ignore_log_patterns.append(r".*migration_task - Can\'t send migration request.*")

    def _repair_schema_2_test(self):
        """
        In a keyspace with three replicas, insert a new column family on two
        replicas only (while the third node is down), and initiate repair from
        the node *without* the data. Verify that the data (and its schema) have been
        correctly replicated to this node.
        The difference between this test and repair_schema_test is that this one
        starts the repair from the node *without* the table. This is a slightly
        harder test, because there is a risk our code will not try to repair the
        cf it doesn't know about.
        """
        logger.debug("Starting cluster...")
        # Start a cluster of two nodes, and create a keyspace with RF=2.
        # Do *not* create a table yet - we'll do that with one node down
        self.cluster.set_configuration_options(values=self.default_config_options())
        self.cluster.populate(generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 3)

        # Take node2 down, and create a new table and data on node1 and node3 only.
        logger.debug("Creating table and data only on node 1...")
        node2.flush()
        node2.stop(wait_other_notice=True)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})
        insert_c1c2(session, keys=range(1000, 2000), consistency=ConsistencyLevel.QUORUM)

        # At this point node2 is not only missing some data, it is actually
        # missing an entire table. Let's bring node2 back up, start repair on
        # node2, and see if node2 gets the new table, and all its data.
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        time.sleep(10)  # see CASSANDRA-4373
        logger.debug("starting repair on node2...")
        info = node2.repair(keyspace="ks")
        logger.debug(info[0])
        logger.debug(info[1])

        # Check that all nodes have all data
        logger.debug("checking data on node1...")
        self.check_rows_on_node(node1, 1000)
        logger.debug("checking data on node2...")
        self.check_rows_on_node(node2, 1000)
        logger.debug("checking data on node3...")
        self.check_rows_on_node(node3, 1000)

        self.ignore_log_patterns.append(r".*migration_task - Can\'t send migration request.*")

    def _repair_cell_update_test(self):
        """
        With data replicated on two nodes, update an existing partition on only
        one of these nodes (with the other node down). Then confirm that repair can
        fix this on the second node as well.
        """
        logger.debug("Starting cluster and inserting data...")
        # Start a cluster of two nodes, and create a keyspace with RF=2, and
        # a table with one partition. Hinted handoff and read repair are disabled
        # so they don't fix the problems which repair is supposed to fix
        self.cluster.set_configuration_options(values=self.default_config_options())
        self.cluster.populate(generate_cluster_topology(dc_num=1, rack_num=2, nodes_per_rack=1)).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()
        with self.patient_cql_connection(node1) as session:
            create_ks(session, "ks", 2)
            create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})
            query = SimpleStatement("INSERT INTO cf (key, c1, c2) VALUES ('key', 'hello', 'hi')", consistency_level=ConsistencyLevel.ALL)
            session.execute(query)

        # Bring down node2, and change the existing data on node 1
        logger.debug("Bringing down node2 and updating data on node 1...")
        node2.flush()
        node2.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node1, "ks") as session1:
            query = SimpleStatement("INSERT INTO cf (key, c1, c2) VALUES ('key', 'new', 'yo')", consistency_level=ConsistencyLevel.ONE)
            session1.execute(query)

            # Confirm that node1 has new data, and (by bringing only node 2 up) that
            # node2 still has old data
            result = list(session1.execute("SELECT * from cf"))
            assert len(result) == 1, len(result)
            assert result[0].key == "key", result[0].key
            assert result[0].c1 == "new", result[0].c1
            assert result[0].c2 == "yo", result[0].c2
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            result = list(session2.execute("SELECT * from cf"))
            assert len(result) == 1, len(result)
            assert result[0].key == "key", result[0].key
            assert result[0].c1 == "hello", result[0].c1
            assert result[0].c2 == "hi", result[0].c2

        # Finally bring both nodes up, repair, and confirm (by bringing up only
        # node 2) that the data on node2 is now up to date.
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        info = node2.repair(keyspace="ks")
        logger.debug(info[0])
        logger.debug(info[1])
        node1.flush()
        node1.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            result = list(session2.execute("SELECT * from cf"))
            assert len(result) == 1, len(result)
            assert result[0].key == "key", result[0].key
            assert result[0].c1 == "new", result[0].c1
            assert result[0].c2 == "yo", result[0].c2

    def _repair_cell_delete_test(self):
        """
        With data replicated on two nodes, update an existing partition on only
        one of these nodes (with the other node down) to delete an existing cell.
        Then confirm that repair can fix this on the second node as well.
        """
        logger.debug("Starting cluster and inserting data...")
        # Start a cluster of two nodes, and create a keyspace with RF=2, and
        # a table with one partition. Hinted handoff and read repair are disabled
        # so they don't fix the problems which repair is supposed to fix
        self.cluster.set_configuration_options(values=self.default_config_options())
        self.cluster.populate(generate_cluster_topology(dc_num=1, rack_num=2, nodes_per_rack=1)).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()
        with self.patient_cql_connection(node1) as session:
            create_ks(session, "ks", 2)
            create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})
            query = SimpleStatement("INSERT INTO cf (key, c1, c2) VALUES ('key', 'hello', 'hi')", consistency_level=ConsistencyLevel.ALL)
            session.execute(query)

        # Bring down node2, and change the existing data on node 1
        logger.debug("Bringing down node2 and updating data on node 1...")
        node2.flush()
        node2.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node1, "ks") as session1:
            query = SimpleStatement("DELETE c1 FROM cf WHERE key ='key'", consistency_level=ConsistencyLevel.ONE)
            session1.execute(query)

            # Confirm that node1 has new data, and (by bringing only node 2 up) that
            # node2 still has old data
            result = list(session1.execute("SELECT * from cf"))
            assert len(result) == 1, len(result)
            assert result[0].key == "key", result[0].key
            assert result[0].c1 == None, result[0].c1
            assert result[0].c2 == "hi", result[0].c2
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            result = list(session2.execute("SELECT * from cf"))
            assert len(result) == 1, len(result)
            assert result[0].key == "key", result[0].key
            assert result[0].c1 == "hello", result[0].c1
            assert result[0].c2 == "hi", result[0].c2

        # Finally bring both nodes up, repair, and confirm (by bringing up only
        # node 2) that the data on node2 is now up to date.
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        info = node2.repair(keyspace="ks")
        logger.debug(info[0])
        logger.debug(info[1])
        node1.flush()
        node1.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            result = list(session2.execute("SELECT * from cf"))
            assert len(result) == 1, len(result)
            assert result[0].key == "key", result[0].key
            assert result[0].c1 == None, result[0].c1
            assert result[0].c2 == "hi", result[0].c2

    def _repair_row_delete_test(self):
        """
        With data replicated on two nodes, update an existing partition on only
        one of these nodes (with the other node down) to delete an existing CQL row.
        Such a delete will result in a range tombstone.
        Then confirm that repair can fix this on the second node as well.
        """
        logger.debug("Starting cluster and inserting data...")
        # Start a cluster of two nodes, and create a keyspace with RF=2, and
        # a table with one partition. Hinted handoff and read repair are disabled
        # so they don't fix the problems which repair is supposed to fix
        self.cluster.set_configuration_options(values=self.default_config_options())
        self.cluster.populate(generate_cluster_topology(dc_num=1, rack_num=2, nodes_per_rack=1)).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()
        with self.patient_cql_connection(node1) as session:
            create_ks(session, "ks", 2)
            session.execute("CREATE TABLE cf (name text, pet text, age int, PRIMARY KEY ((name), pet)) WITH compression = {} AND read_repair_chance = 0.0;")

            query = SimpleStatement("INSERT INTO cf (name, pet, age) VALUES ('nadav', 'kitty', 5)", consistency_level=ConsistencyLevel.ALL)
            session.execute(query)
            query = SimpleStatement("INSERT INTO cf (name, pet, age) VALUES ('nadav', 'adamdami', 1)", consistency_level=ConsistencyLevel.ALL)
            session.execute(query)

        # Bring down node2, and change the existing data on node 1
        logger.debug("Bringing down node2 and updating data on node 1...")
        node2.flush()
        node2.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node1, "ks") as session1:
            query = SimpleStatement("DELETE FROM cf WHERE name = 'nadav' AND pet = 'kitty'", consistency_level=ConsistencyLevel.ONE)
            session1.execute(query)

            # Confirm that node1 has new data, and (by bringing only node 2 up) that
            # node2 still has old data
            result = list(session1.execute("SELECT * from cf"))
            assert len(result) == 1, len(result)
            assert result[0].name == "nadav", result[0].name
            assert result[0].pet == "adamdami", result[0].pet
            assert result[0].age == 1, result[0].age
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            result = list(session2.execute("SELECT * from cf"))
            assert len(result) == 2, len(result)

        # Finally bring both nodes up, repair, and confirm (by bringing up only
        # node 2) that the data on node2 is now up to date.
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        info = node2.repair(keyspace="ks")
        logger.debug(info[0])
        logger.debug(info[1])
        node1.flush()
        node1.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            result = list(session2.execute("SELECT * from cf"))
            assert len(result) == 1, len(result)
            assert result[0].name == "nadav", result[0].name
            assert result[0].pet == "adamdami", result[0].pet
            assert result[0].age == 1, result[0].age

    def _repair_partition_delete_test(self):
        """
        With data replicated on two nodes, delete partition on only one of these
        nodes (with the other node down). Then confirm that repair can fix this on
        the second node as well.
        """
        logger.debug("Starting cluster and inserting data...")
        # Start a cluster of two nodes, and create a keyspace with RF=2, and
        # a table with three partitions. Hinted handoff and read repair are disabled
        # so they don't fix the problems which repair is supposed to fix
        self.cluster.set_configuration_options(values=self.default_config_options())
        self.cluster.populate(generate_cluster_topology(dc_num=1, rack_num=2, nodes_per_rack=1)).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()
        with self.patient_cql_connection(node1) as session:
            create_ks(session, "ks", 2)
            create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})
            query = SimpleStatement("INSERT INTO cf (key, c1, c2) VALUES ('k1', 'v11', 'v12')", consistency_level=ConsistencyLevel.ALL)
            session.execute(query)
            query = SimpleStatement("INSERT INTO cf (key, c1, c2) VALUES ('k2', 'v21', 'v22')", consistency_level=ConsistencyLevel.ALL)
            session.execute(query)
            query = SimpleStatement("INSERT INTO cf (key, c1, c2) VALUES ('k3', 'v31', 'v32')", consistency_level=ConsistencyLevel.ALL)
            session.execute(query)

        # Bring down node2, and change the existing data on node 1
        logger.debug("Bringing down node2 and updating data on node 1...")
        node2.flush()
        node2.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node1, "ks") as session1:
            query = SimpleStatement("DELETE FROM cf WHERE key = 'k1';", consistency_level=ConsistencyLevel.ONE)
            session1.execute(query)
            query = SimpleStatement("DELETE FROM cf WHERE key = 'k3';", consistency_level=ConsistencyLevel.ONE)
            session1.execute(query)

            # Confirm that node1 has new data, and (by bringing only node 2 up) that
            # node2 still has old data
            result = list(session1.execute("SELECT * from cf"))
            assert len(result) == 1, len(result)
            assert result[0].key == "k2", result[0].key
            assert result[0].c1 == "v21", result[0].c1
            assert result[0].c2 == "v22", result[0].c2
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            result = list(session2.execute("SELECT * from cf"))
            assert len(result) == 3, len(result)

        # Finally bring both nodes up, repair, and confirm (by bringing up only
        # node 2) that the data on node2 is now up to date.
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        info = node2.repair(keyspace="ks")
        logger.debug(info[0])
        logger.debug(info[1])
        node1.flush()
        node1.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            result = list(session2.execute("SELECT * from cf"))
            assert len(result) == 1, len(result)
            assert result[0].key == "k2", result[0].key
            assert result[0].c1 == "v21", result[0].c1
            assert result[0].c2 == "v22", result[0].c2

    @classmethod
    def _cells_with_name(cls, node, ks: str, cf: str, column_name: str) -> Generator[dict[str, Any]]:
        # dump_sstable() returns a list like:
        #
        # [{'key': {'token': '-6847573755651342660',
        #   'raw': '00036b6579',
        #   'value': 'key'},
        #  'clustering_elements': [{'type': 'clustering-row',
        #    'key': {'raw': '', 'value': ''},
        #    'marker': {'timestamp': 1691723022125706},
        #    'columns': {'c1': {'is_live': True,
        #      'type': 'regular',
        #      'timestamp': 1691723027979972,
        #      'ttl': '1234s',
        #      'expiry': '2023-08-11 03:24:21z',
        #      'value': 'new'},
        #     'c2': {'is_live': True,
        #      'type': 'regular',
        #      'timestamp': 1691723022125706,
        #      'value': 'hi'}}}]}]
        partitions = dump_sstables(node, ks, cf)

        for partition in partitions:
            for clustering_element in partition.get("clustering_elements", []):
                cell = clustering_element["columns"].get(column_name)
                if cell is not None:
                    yield cell

    @classmethod
    def _first_cell_with_name(cls, node, ks: str, cf: str, column_name: str) -> dict[str, Any] | None:
        try:
            return next(cls._cells_with_name(node, ks, cf, column_name))
        except StopIteration:
            return None

    def _repair_ttl_update_test(self):  # noqa: PLR0915
        """
        With data replicated on two nodes, update an existing partition on only
        one of these nodes (with the other node down). Then confirm that repair can
        fix this on the second node as well.
        This test is identical to repair_cell_update_test, except the update also
        involves setting a TTL (and we confirm the repaired value also gets this ttl)
        """
        # Start a cluster of two nodes, and create a keyspace with RF=2, and
        # a table with one partition. Hinted handoff and read repair are disabled
        # so they don't fix the problems which repair is supposed to fix
        self.cluster.set_configuration_options(values=self.default_config_options())
        self.cluster.populate(generate_cluster_topology(dc_num=1, rack_num=2, nodes_per_rack=1)).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()
        with self.patient_cql_connection(node1) as session:
            create_ks(session, "ks", 2)
            create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})
            query = SimpleStatement("INSERT INTO cf (key, c1, c2) VALUES ('key', 'hello', 'hi')", consistency_level=ConsistencyLevel.ALL)
            session.execute(query)

        # Bring down node2, and change the existing data on node 1
        node2.flush()
        node2.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node1, "ks") as session1:
            query = SimpleStatement("UPDATE cf using TTL 1234 SET c1='new' WHERE key = 'key'", consistency_level=ConsistencyLevel.ONE)
            session1.execute(query)

            # Confirm that node1 has the new data, with the TTL. Unfortunately, to
            # verify the TTL we cannot simply use "SELECT TTL(c1) from cf",
            # because the TTL we get from that is not the original TTL we had set,
            # but rather the *remaining* TTL at this time. To verify the original
            # TTL set, we need to resort to reading the sstable.
            result = list(session1.execute("SELECT * from cf"))
            assert len(result) == 1, len(result)
            assert result[0].key == "key", result[0].key
            assert result[0].c1 == "new", result[0].c1
            assert result[0].c2 == "hi", result[0].c2
        node1.flush()
        c1_cell_ttl = self._first_cell_with_name(node1, "ks", "cf", "c1")
        # The "c1" cell should have an expiration time and will look something
        # like this:
        # {'is_live': True,
        #  'type': 'regular',
        #  'timestamp': 1691723027979972,
        #  'ttl': '1234s',
        #  'expiry': '2023-08-11 03:24:21z',
        #  'value': 'new'}
        # We need to verify the number "1234" is the same as we set
        assert c1_cell_ttl is not None, "TTL set in sstable"
        assert c1_cell_ttl.get("ttl") == "1234s", "TTL set to 1234"

        # Confirm (by bringing only node 2 up) that node2 still has old data
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            result = list(session2.execute("SELECT * from cf"))
            assert len(result) == 1, len(result)
            assert result[0].key == "key", result[0].key
            assert result[0].c1 == "hello", result[0].c1
            assert result[0].c2 == "hi", result[0].c2
        c1_cell = self._first_cell_with_name(node2, "ks", "cf", "c1")
        if c1_cell is not None:
            assert "expiry" not in c1_cell, "TTL should not be set"

        # sstable2json has a bug (see CASSANDRA-8616) where it writes commit
        # log files. Since Scylla can't read those (they are in Cassandra
        # format) we need to remove them before we can restart node 1.
        # This may also end up deleting Scylla commit logs, but those should
        # not exist anyway (as we used node1.flush()).
        commitlog_dir = node1.get_path() + "/commitlog/"
        commitlog.cleanup(commitlog_dir)

        # Finally bring both nodes up, repair, and confirm (by bringing up only
        # node 2) that the data on node2 is now up to date.
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        info = node2.repair(keyspace="ks")
        logger.debug(info[0])
        logger.debug(info[1])
        node1.flush()
        node1.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            result = list(session2.execute("SELECT * from cf"))
            assert len(result) == 1, len(result)
            assert result[0].key == "key", result[0].key
            assert result[0].c1 == "new", result[0].c1
            assert result[0].c2 == "hi", result[0].c2
        node2.flush()
        # Confirm that one of the sstables contains the expected value and
        # expiration time (because we didn't do compaction, we'll see both
        # the old and new values in different sstables)
        c1_ttl_cell_found = False
        for c1_cell in self._cells_with_name(node2, "ks", "cf", "c1"):
            if c1_cell == c1_cell_ttl:
                c1_ttl_cell_found = True
                break
        assert c1_ttl_cell_found, "expected c1 value and timeout in sstable"

    def assert_repair_option_pr_rows(self, session, min_count, max_count, consistency_level=ConsistencyLevel.ONE):
        select_query = SimpleStatement("SELECT * FROM cf", consistency_level=consistency_level)
        rows = list(session.execute(select_query))
        count_query = SimpleStatement("SELECT count(*) from cf", consistency_level=consistency_level)
        count = session.execute(count_query)[0][0]
        assert count == len(rows), f"count {count} must be equal to len(rows)\nrows: {rows}"
        logger.debug(f"Asserting pr repair count: {count} in [{min_count}..{max_count}]")
        assert min_count <= count <= max_count, f"expected pr repair to repair between {min_count} to {max_count} rows, but count is {count}\nrows: {rows}"

    def _repair_option_pr_test(self):
        """
        Test the "partitioner range" (-pr) option. We start two nodes and a
        keyspace with RF=2, and put 1000 different rows on each of the nodes
        (as in repair_disjoint_data_set). Each node has in "partioner ranges"
        only half the key space, so that starting a repair with "-pr" on one
        node will bring in around 500 missing partitions, but the other 500
        will continue to be missing until we start a repair with "-pr" on the
        second node as well.
        """
        # Start a cluster of two nodes, and create a keyspace ks with RF=2,
        # and a table cf. Hinted handoff and read repair are disabled so
        # they don't fix the problems which repair is supposed to fix.
        self.cluster.set_configuration_options(values=self.default_config_options())
        self.cluster.populate(2).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()
        tablets = 256 * len(self.cluster.nodelist()) if "tablets" in self.scylla_features else None
        with self.patient_cql_cluster_session(node1) as session:
            create_ks(session, "ks", 2, tablets=tablets)
            create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        # Insert 1000 keys *only* on node 1, another 1000 keys *only* on node 2:
        logger.debug("Adding data only on node 1...")
        node2.flush()
        node2.stop(wait_other_notice=True)
        with self.patient_cql_cluster_session(node1, "ks", exclusive=True) as session1:
            insert_c1c2(session1, keys=range(1000, 2000), consistency=ConsistencyLevel.ONE)
        self.cluster.flush()
        logger.debug("Adding data only on node 2...")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        with self.patient_cql_cluster_session(node2, "ks", exclusive=True) as session2:
            insert_c1c2(session2, keys=range(2000, 3000), consistency=ConsistencyLevel.ONE)

        # Bring up both nodes, each should have different data
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Run partioner-range repair on node 1
        info = node1.repair(keyspace="ks", partitioner_range=True)
        logger.debug(info[0])
        logger.debug(info[1])

        # We expect "-pr" repair to have repared only half of the ranges
        # (those for which node 1 is their primary replica), so both nodes
        # should now have around 1500 partitions. We don't know the exact
        # number, but given the assumed random distribution of tokens and keys,
        # it is unlikely to be far from 1500 - let's assert it is between
        # 1200 and 1800
        node1.flush()
        node1.stop(wait_other_notice=True)
        with self.patient_cql_cluster_session(node2, "ks", exclusive=True) as session2:
            self.assert_repair_option_pr_rows(session2, 1200, 1800)
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        node2.flush()
        node2.stop(wait_other_notice=True)
        with self.patient_cql_cluster_session(node1, "ks", exclusive=True) as session1:
            self.assert_repair_option_pr_rows(session1, 1200, 1800)
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Run a second "-pr" repair, this time on node 2. This should repair
        # all the ranges not previously repared (i.e., this times the ranges
        # whose primary is node 2), and at the end, all data, 2000 partitions,
        # should be on both nodes.
        info = node2.repair(keyspace="ks", partitioner_range=True)
        logger.debug(info[0])
        logger.debug(info[1])
        self.check_rows_on_node(node1, 2000)
        self.check_rows_on_node(node2, 2000)

    def _repair_option_pr_dc_host_test(self):
        """
        Test how the "partitioner range" (-pr) option interacts with the
        options which restrict the nodes participating in the repair -
        -dc, -local and -hosts.
        Since -pr usually assigns each token to just one node in the entire
        cluster, it is generally forbidden to restrict the repair to only
        part of the cluster otherwise some ranges will never be repaired.
        Nevertheless, combining -pr with restriction to the local dc
        ("-local") *is* allowed, and changes the meaning of -pr to not
        pick just one node in the cluster as the primary for every token -
        but rather one node in every dc.
        In this test we verify that forbidden option combinations are
        indeed forbidden, and the supported combination "-pr -local" is
        supported correctly - so if we loop on all nodes of just one dc
        and repair them with "-pr -local", it will repair the data center
        completely, over the entire token range.
        """
        # Start a cluster of three data centers with two nodes each, and
        # create a keyspace ks with RF=2, and a table cf.
        # Hinted handoff and read repair are disabled so they don't fix the
        # problems which repair is supposed to fix.
        self.cluster.set_configuration_options(values=self.default_config_options())
        self.cluster.populate([2, 2, 2]).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1_1, node1_2, node2_1, node2_2, node3_1, node3_2 = self.cluster.nodelist()
        tablets = 256 * len(self.cluster.nodelist()) if "tablets" in self.scylla_features else None
        with self.patient_cql_cluster_session(node1_1) as session:
            create_ks(session, "ks", {"dc1": 2, "dc2": 2, "dc3": 2}, tablets=tablets)
            create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        # Repair with "-pr" that restricts the repair to a subset of
        # data centers or a subset of hosts is forbidden, and should
        # cause a failure. Since generally repair not including the
        # current dc or host is forbidden, we check a case which does
        # include the current dc and the current host.
        # Interestingly, both nodetool (Repair.java) and Scylla have
        # code to fail this case, so we only test the outer layer
        # (Repair.java).
        # Note that combining -pr with -local is a special case,
        # which is supported, and we'll test below.
        with pytest.raises(NodetoolError):
            node1_1.repair(keyspace="ks", partitioner_range=True, dcs=["dc1", "dc3"])

        # Same issue with combination of -pr with -hosts
        with pytest.raises(NodetoolError):
            node1_1.repair(keyspace="ks", partitioner_range=True, hosts=[node1_1.address()])

        # Although combining -pr with -local is allowed (see below),
        # the supposedly equivalent "-pr -dc dc1" (when dc1 is the local
        # dc) is NOT allowed, caught by nodetool (Repair.java).
        # Let's test that this is indeed the case.
        # I think this is deliberate, and the thinking is that we want to
        # allow only a command which, if run on every node, will work.
        # So while "-pr -local" will work (repair using the local cluster
        # on every node), "-pr -dc dc1" will not (for nodes in dc2, dc2
        # would need to be used instead).
        # Note that as far as Scylla is concerned, there is no difference
        # between "-local" or "-dc dc1" when dc1 is the local dc1. So this
        # case *could* have worked if nodetool didn't forbid it.
        with pytest.raises(NodetoolError):
            node1_1.repair(keyspace="ks", partitioner_range=True, dcs=["dc1"])

        # However, "-pr" combined with restriction to the *local* datacenter
        # is supported, and should be supported correctly (see issue #3557).
        # In that case, if we run repair with "-pr -local" on all the nodes
        # of this datacenter only, all token ranges will be repaired and not
        # parts. Let's start with the trivial test that -pr -local doesn't
        # cause an error. Then we'll check a more elaborate example with
        # actual data, repair again and verify it actually repairs data.
        node1_1.repair(keyspace="ks", partitioner_range=True, local=True)

        # Insert 1000 keys *only* on node 1, another 1000 keys *only* on node 2
        # both in the first data center. The other data centers will be
        # completely missing this data:
        logger.debug("Adding data only on node 1...")
        self.cluster.stop_nodes([node1_2, node2_1, node2_2, node3_1, node3_2], wait_other_notice=True)
        with self.patient_cql_cluster_session(node1_1, "ks", exclusive=True, consistency_level=ConsistencyLevel.LOCAL_ONE) as session1:
            insert_c1c2(session1, keys=range(1000, 2000), consistency=ConsistencyLevel.LOCAL_ONE)
        self.cluster.flush()
        logger.debug("Adding data only on node 2...")
        node1_2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1_1.stop(wait_other_notice=True)
        with self.patient_cql_cluster_session(node1_2, "ks", exclusive=True, consistency_level=ConsistencyLevel.LOCAL_ONE) as session2:
            insert_c1c2(session2, keys=range(2000, 3000), consistency=ConsistencyLevel.LOCAL_ONE)

        # Bring up all nodes, each node on dc 1 should have different data
        # and all the nodes of the two other clusters are empty (but that's
        # not important in this case).
        logger.debug("Bring back all nodes...")
        self.cluster.start_nodes(wait_other_notice=True, wait_for_binary_proto=True)

        # Run dc-local partioner-range repair on node 1
        info = node1_1.repair(keyspace="ks", partitioner_range=True, local=True)
        logger.debug(info[0])
        logger.debug(info[1])
        # We expect "-pr" repair to have repaired only half of the ranges
        # (those for which node 1 is their primary replica), so both nodes
        # should now have around 1500 partitions. We don't know the exact
        # number, but given the assumed random distribution of tokens and keys,
        # it is unlikely to be far from 1500 - let's assert it is between
        # 1200 and 1800
        # Note that if the "-local" *was* not obeyed, we would see a
        # failure here because without "-local", "-pr" repair of just one
        # node in a cluster of 6 would just repair 1/6th of the range,
        # not 1/2.
        logger.debug("Stopping node1_1")
        node1_1.flush()
        node1_1.stop(wait_other_notice=True)
        with self.patient_cql_cluster_session(node1_2, "ks", exclusive=True, consistency_level=ConsistencyLevel.LOCAL_ONE) as session2:
            self.assert_repair_option_pr_rows(session2, 1200, 1800, consistency_level=ConsistencyLevel.LOCAL_ONE)

        logger.debug("Restarting node1_2")
        node1_1.start(wait_other_notice=True, wait_for_binary_proto=True)
        logger.debug("Stopping node1_2")
        node1_2.flush()
        node1_2.stop(wait_other_notice=True)
        with self.patient_cql_cluster_session(node1_1, "ks", exclusive=True, consistency_level=ConsistencyLevel.LOCAL_ONE) as session1:
            self.assert_repair_option_pr_rows(session1, 1200, 1800, consistency_level=ConsistencyLevel.LOCAL_ONE)

        logger.debug("Restarting node1_2")
        node1_2.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Run a second dc-local "-pr" repair, this time on node 2. This
        # should repair all the ranges not previously repared (i.e., this
        # times the ranges whose primary is node 2), and at the end, all
        # data, 2000 partitions, should be on both nodes.
        # Note that if the "-local" *was* not obeyed, we would see a
        # failure here because without "-local", one would need to do
        # a "-pr" repair on all six nodes of the cluster to cover the
        # entire token range.
        info = node1_2.repair(keyspace="ks", partitioner_range=True, local=True)
        logger.debug(info[0])
        logger.debug(info[1])
        self.check_rows_on_node(node1_1, 2000, consistency_level=ConsistencyLevel.LOCAL_ONE)
        self.check_rows_on_node(node1_2, 2000, consistency_level=ConsistencyLevel.LOCAL_ONE)

    def _repair_option_pr_multi_dc_test(self):  # noqa: PLR0915
        """
        Test how the "partitioner range" (-pr) option interacts with the
        a multi-dc setup (but without a "-local" parameter tested above).
        A user needs to do a -pr repair on each and every one of the nodes -
        on all data centers - to achieve a full repair.
        """
        if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode != "debug":
            num_dcs, num_keys = 3, 3000
        else:
            num_dcs, num_keys = 2, 1000
        # Start a cluster of {num_dcs} data centers with two nodes each, and
        # create a keyspace ks with RF=2, and a table cf.
        # Hinted handoff and read repair are disabled so they don't fix the
        # problems which repair is supposed to fix.
        num_nodes_per_dc = 2
        num_nodes = num_dcs * num_nodes_per_dc
        replication_opts = dict()
        for i in range(num_dcs):
            replication_opts[f"dc{i + 1}"] = num_nodes_per_dc
        logger.debug(f"Starting {num_nodes} nodes in {num_dcs} data centers as: {replication_opts}...")
        self.cluster.set_configuration_options(values=self.default_config_options())
        self.cluster.populate([num_nodes_per_dc] * num_dcs).start(wait_for_binary_proto=True, wait_other_notice=True)
        nodes = [[]] * num_dcs
        for i in range(num_dcs * num_nodes_per_dc):
            nodes[i // num_nodes_per_dc].append(self.cluster.nodelist()[i])
        node1_1 = nodes[0][0]
        node1_2 = nodes[0][1]
        tablets = 256 * len(self.cluster.nodelist()) if "tablets" in self.scylla_features else None
        with self.patient_cql_cluster_session(nodes[0][0]) as session:
            create_ks(session, "ks", replication_opts, tablets=tablets)
            create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        # Insert {num_keys} keys *only* on node 1, another {num_keys} keys *only* on node 2
        # both in the first data center. The other data centers will be
        # completely missing this data:
        logger.debug("Adding data only on node 1...")
        nodes_to_stop = [n for n in self.cluster.nodelist() if n != node1_1]
        self.cluster.stop_nodes(nodes_to_stop, wait_other_notice=True)
        with self.patient_cql_cluster_session(node1_1, "ks", exclusive=True, consistency_level=ConsistencyLevel.LOCAL_ONE) as session1:
            insert_c1c2(session1, keys=range(1 * num_keys, 2 * num_keys), consistency=ConsistencyLevel.LOCAL_ONE)
        self.cluster.flush()
        logger.debug("Adding data only on node 2...")
        node1_2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1_1.stop(wait_other_notice=True)
        with self.patient_cql_cluster_session(node1_2, "ks", exclusive=True, consistency_level=ConsistencyLevel.LOCAL_ONE) as session2:
            insert_c1c2(session2, keys=range(2 * num_keys, 3 * num_keys), consistency=ConsistencyLevel.LOCAL_ONE)

        # Bring up all nodes, each should have different data
        # (all the nodes of the two other clusters are empty, but that's
        # not important in this case).
        logger.debug("Bring back all nodes...")
        self.cluster.start_nodes(wait_other_notice=True, wait_for_binary_proto=True)

        # Run dc-local partioner-range repair on node 1
        logger.debug("Repair with -pr on node 1...")
        info = node1_1.repair(keyspace="ks", partitioner_range=True)
        logger.debug(info[0])
        logger.debug(info[1])
        # We expect "-pr" repair to have repaired only 1/6th of the ranges
        # (those for which node 1 is their primary replica), so node 1
        # should now have around 1166 partitions. We don't know the exact
        # number, but given the assumed random distribution of tokens and keys,
        # it is unlikely to be far from 1166 - let's assert it is between
        # 1050 and 1300
        logger.debug("Stopping node1_2")
        node1_2.flush()
        node1_2.stop(wait_other_notice=True)
        with self.patient_cql_cluster_session(node1_1, "ks", exclusive=True, consistency_level=ConsistencyLevel.LOCAL_ONE) as session1:
            self.assert_repair_option_pr_rows(session1, int(num_keys * 1.05), int(num_keys * 1.667), consistency_level=ConsistencyLevel.LOCAL_ONE)

        logger.debug("Restarting node1_2")
        node1_2.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Run dc-local "-pr" repair on all other nodes. This should repair
        # all the ranges not previously repared and at the end, all
        # data, 2000 partitions, should be on all nodes.
        for node in self.cluster.nodelist():
            if node != node1_1:
                logger.debug("Repair with -pr on " + node.name)
                info = node.repair(keyspace="ks", partitioner_range=True)
                logger.debug(info[0])
                logger.debug(info[1])
        for node in self.cluster.nodelist():
            logger.debug("Checking data on " + node.name)
            self.check_rows_on_node(node, 2 * num_keys, consistency_level=ConsistencyLevel.LOCAL_ONE)

    def _repair_option_cf_test(self):  # noqa: PLR0915
        """
        Test that we can specify the list of column families to repair. We
        create 3 column families in need of repair, and ask to repair only 2
        of them, and confirm that 2 were repaired (so a list of cfs is
        supported correctly) and the third was not. Finally, confirm that a
        repair without a column family list repairs all of them.
        """
        # Start a cluster of two nodes, and create a keyspace ks with RF=2,
        # and 3 tables. Hinted handoff and read repair are disabled so
        # they don't fix the problems which repair is supposed to fix.
        self.cluster.set_configuration_options(values=self.default_config_options())
        self.cluster.populate(generate_cluster_topology(dc_num=1, rack_num=2, nodes_per_rack=1)).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()
        with self.patient_cql_connection(node1) as session:
            create_ks(session, "ks", 2)
            create_cf(session, "cf1", read_repair=0.0, columns={"c1": "text"})
            create_cf(session, "cf2", read_repair=0.0, columns={"c1": "text"})
            create_cf(session, "cf3", read_repair=0.0, columns={"c1": "text"})

        # Insert one key in each cf *only* on node 1, another key *only* on node 2:
        node2.flush()
        node2.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node1, "ks") as session1:
            query = SimpleStatement("INSERT INTO cf1 (key, c1) VALUES ('k11', 'v11')", consistency_level=ConsistencyLevel.ONE)
            session1.execute(query)
            query = SimpleStatement("INSERT INTO cf2 (key, c1) VALUES ('k21', 'v21')", consistency_level=ConsistencyLevel.ONE)
            session1.execute(query)
            query = SimpleStatement("INSERT INTO cf3 (key, c1) VALUES ('k31', 'v31')", consistency_level=ConsistencyLevel.ONE)
            session1.execute(query)
        self.cluster.flush()
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            query = SimpleStatement("INSERT INTO cf1 (key, c1) VALUES ('k11a', 'v11a')", consistency_level=ConsistencyLevel.ONE)
            session2.execute(query)
            query = SimpleStatement("INSERT INTO cf2 (key, c1) VALUES ('k21a', 'v21a')", consistency_level=ConsistencyLevel.ONE)
            session2.execute(query)
            query = SimpleStatement("INSERT INTO cf3 (key, c1) VALUES ('k31a', 'v31a')", consistency_level=ConsistencyLevel.ONE)
            session2.execute(query)

        # Bring up both nodes, each should have different data
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Run partioner-range repair on node 1
        info = node1.repair(keyspace="ks", tables=["cf1", "cf3"])
        logger.debug(info[0])
        logger.debug(info[1])

        # We expect each node to now have 2 partitions in each of cf1 and cf3
        # because those have been repaired - but only 1 in cf2.
        node1.flush()
        node1.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            assert len(list(session2.execute("SELECT * from cf1"))) == 2, "cf1 on node2"
            assert len(list(session2.execute("SELECT * from cf2"))) == 1, "cf2 on node2"
            assert len(list(session2.execute("SELECT * from cf3"))) == 2, "cf2 on node2"
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        node2.flush()
        node2.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node1, "ks") as session1:
            assert len(list(session1.execute("SELECT * from cf1"))) == 2, "cf1 on node1"
            assert len(list(session1.execute("SELECT * from cf2"))) == 1, "cf2 on node1"
            assert len(list(session1.execute("SELECT * from cf3"))) == 2, "cf2 on node1"

        # repair again without a cf option, and see that all cfs, and in
        # particular cf2 (which we haven't repaired so far), get repaired.
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        info = node1.repair(keyspace="ks")
        logger.debug(info[0])
        logger.debug(info[1])
        node1.flush()
        node1.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            assert len(list(session2.execute("SELECT * from cf2"))) == 2, "cf2 on node2"

    def _repair_option_invalid_ks_cf_test(self):
        """
        Test that specifying a non-existant keyspace or column family to
        repair results in failure.
        """
        self.cluster.set_configuration_options(values=self.default_config_options())
        self.cluster.populate(generate_cluster_topology(dc_num=1, rack_num=2, nodes_per_rack=1)).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, _node2 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 2)
        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text"})

        # Repairing an invalid column family in a valid keyspace
        with pytest.raises(NodetoolError):
            node1.repair(keyspace="ks", tables=["badcf"])
        # Repairing an invalid keyspace
        with pytest.raises(NodetoolError):
            node1.repair(keyspace="badks")
        # Repair with one of the cfs being invalid
        with pytest.raises(NodetoolError):
            node1.repair(keyspace="badks", tables=["cf", "badcf"])
        # Finally, sanity check that a valid repair succeeds:
        node1.repair(keyspace="ks", tables=["cf"])

    def _repair_option_dc_test(self):  # noqa: PLR0915
        """
        Test the "-dc" and "-local" repair options: Create 3 data centers, the
        first with 2 nodes, second with 1 node, and third with 1 node. We then
        update data on one of the nodes in the first data center, and check
        that repairing with "-dc" and "-local" repairs the nodes of the
        requested datacenters, and not more.
        Finally, check that error conditions (like non-existant data center name,
        or not listing the current data center) are caught.
        """
        # Create 3 data centers, dc1 with 2 nodes, dc2 with 1 node, and dc3
        # with 1 node. Then create a keyspace ks replicated on all nodes,
        # and one cf. Hinted handoff and read repair are disabled so they
        # don't fix the problems which repair is supposed to fix.
        self.cluster.set_configuration_options(values=self.default_config_options())
        topology_layout = {"dc1": {"rack1_1": 1, "rack1_2": 1}, "dc2": {"rack2": 1}, "dc3": {"rack3": 1}}
        self.cluster.populate(topology_layout).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3, node4 = self.cluster.nodelist()
        with self.patient_cql_connection(node1) as session:
            session.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2' : 1, 'dc3': 1};")
            session.set_keyspace("ks")
            create_cf(session, "cf", read_repair=0.0, columns={"c1": "text"})

            # Insert one key *only* on node 1 (of dc1). All the other nodes will
            # be missing this data.
            # Insert one key in each cf *only* on node 1, another key *only* on node 2:
            node2.flush()
            node2.stop(wait_other_notice=True)
            node3.flush()
            node3.stop(wait_other_notice=True)
            node4.flush()
            node4.stop(wait_other_notice=True)
            query = SimpleStatement("INSERT INTO cf (key, c1) VALUES ('k11', 'v11')", consistency_level=ConsistencyLevel.ONE)
            session.execute(query)

        # Start all nodes, do a repair limited to dc1 and dc3, and confirm the
        # data was correctly copied to node2 (in dc1) and node4 (in dc3) but
        # not to node3 (in dc2):
        self.cluster.start_nodes([node2, node3, node4], wait_other_notice=True, wait_for_binary_proto=True)
        info = node1.repair(keyspace="ks", dcs=["dc1", "dc3"])
        logger.debug(info[0])
        logger.debug(info[1])
        node1.flush()
        node1.stop(wait_other_notice=True)
        node3.flush()
        node3.stop(wait_other_notice=True)
        node4.flush()
        node4.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            assert len(list(session2.execute("SELECT * from cf"))) == 1, "cf on node2"
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        node2.flush()
        node2.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node3, "ks") as session3:
            assert len(list(session3.execute("SELECT * from cf"))) == 0, "cf on node3"
        node4.start(wait_other_notice=True, wait_for_binary_proto=True)
        node3.flush()
        node3.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node4, "ks") as session4:
            assert len(list(session4.execute("SELECT * from cf"))) == 1, "cf on node4"

        self.cluster.start_nodes([node1, node2, node3], wait_other_notice=True, wait_for_binary_proto=True)

        if "tablets" not in self.scylla_features:
            # Repair with one of the data centers specified being invalid should
            # cause a failure
            with pytest.raises(NodetoolError):
                node1.repair(keyspace="ks", dcs=["dc1", "baddc"])

            # Repair with data centers specified *without* the current data center
            # is an error too.
            with pytest.raises(NodetoolError):
                node1.repair(keyspace="ks", dcs=["dc2", "dc3"])

        # Repair again without a "-dc" option - should repair all nodes in all
        # data centers, and in particular node3 (in dc2) .
        info = node1.repair(keyspace="ks")
        logger.debug(info[0])
        logger.debug(info[1])
        node1.flush()
        node1.stop(wait_other_notice=True)
        node2.flush()
        node2.stop(wait_other_notice=True)
        node4.flush()
        node4.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node3, "ks") as session3:
            assert len(list(session3.execute("SELECT * from cf"))) == 1, "cf on node3"

        if "tablets" not in self.scylla_features:
            # Similiarly test the "-local" option: Add one more partition to node1
            # (in dc1), repair node1 with "-local" and confirm that only node2 (the
            # other node in dc1) gets another partition, but node3 (dc2) and node4
            # (dc3) don't.
            node1.start(wait_other_notice=True, wait_for_binary_proto=True)
            node3.flush()
            node3.stop(wait_other_notice=True)
            with self.patient_exclusive_cql_connection(node1, "ks") as session1:
                query = SimpleStatement("INSERT INTO cf (key, c1) VALUES ('k12', 'v12')", consistency_level=ConsistencyLevel.ONE)
                session1.execute(query)
            self.cluster.start_nodes([node2, node3, node4], wait_other_notice=True, wait_for_binary_proto=True)
            info = node1.repair(keyspace="ks", local=True)
            logger.debug(info[0])
            logger.debug(info[1])
            node1.flush()
            node1.stop(wait_other_notice=True)
            node3.flush()
            node3.stop(wait_other_notice=True)
            node4.flush()
            node4.stop(wait_other_notice=True)
            with self.patient_exclusive_cql_connection(node2, "ks") as session2:
                assert len(list(session2.execute("SELECT * from cf"))) == 2, "cf on node2"
            node3.start(wait_other_notice=True, wait_for_binary_proto=True)
            node2.flush()
            node2.stop(wait_other_notice=True)
            with self.patient_exclusive_cql_connection(node3, "ks") as session3:
                assert len(list(session3.execute("SELECT * from cf"))) == 1, "cf on node3"
            node4.start(wait_other_notice=True, wait_for_binary_proto=True)
            node3.flush()
            node3.stop(wait_other_notice=True)
            with self.patient_exclusive_cql_connection(node4, "ks") as session4:
                assert len(list(session4.execute("SELECT * from cf"))) == 1, "cf on node4"

    def _repair_multiple_test(self, partitioner_range=False):
        """
        Starting multiple repairs in parallel from multiple nodes (without
        "-pr") is a waste, but besides being wasteful, should not cause any
        harm, and should produce correct results.
        """
        # Disable hinted handoff so it doesn't do what we expect repair to do
        self.cluster.set_configuration_options(values=self.default_config_options())
        # Create a cluster of 3 nodes, and a keyspace with RF=3 on all nodes
        # (disable read repair, as we want to test the full repair).
        self.cluster.populate(generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3 = self.cluster.nodelist()
        with self.patient_cql_connection(node1) as session:
            create_ks(session, "ks", 3)
            create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        # Insert 1000 keys *only* on node 1, another 1000 keys *only* on node 2,
        # another 1000 *only on node 3:
        logger.debug("Adding data only on node 1...")
        node2.flush()
        node2.stop(wait_other_notice=True)
        node3.flush()
        node3.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node1, "ks") as session1:
            insert_c1c2(session1, keys=range(1000, 2000), consistency=ConsistencyLevel.ONE)
        self.cluster.flush()
        logger.debug("Adding data only on node 2...")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            insert_c1c2(session2, keys=range(2000, 3000), consistency=ConsistencyLevel.ONE)
        logger.debug("Adding data only on node 3...")
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        node2.flush()
        node2.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node3, "ks") as session3:
            insert_c1c2(session3, keys=range(3000, 4000), consistency=ConsistencyLevel.ONE)

        # Bring up all 3 nodes, each should have different data
        self.cluster.start_nodes([node1, node2], wait_other_notice=True, wait_for_binary_proto=True)

        if "tablets" in self.scylla_features:
            node1.repair(keyspace="ks")
        else:
            parallel_repair_on_nodes(nodes=[node1, node2, node3], keyspace="ks", partitioner_range=partitioner_range)

        # Check that all nodes have all data
        self.check_rows_on_node(node1, 3000)
        self.check_rows_on_node(node2, 3000)
        self.check_rows_on_node(node3, 3000)

    def _repair_multiple_pr_test(self):
        """
        If a user plans to start repair from multiple nodes in parallel, he
        should at least use the "-pr" (partitioner range) option to avoid
        the waste of repairing the same data multiple times. Let's check that
        this actually works.
        """
        self._repair_multiple_test(partitioner_range=True)

    def _repair_option_seq_test(self):
        """
        Test that the "-seq" repair options works. In Scylla, it doesn't
        actually change anything, but we need to test it doesn't do anything
        bad.
        """
        self._repair_disjoint_data_test()

    def repair_n_gt_rf(self):
        """
        Another basic test for repair, this time we have more nodes than
        replication factor, so different ranges of tokens have a different
        set of replicas - so the repair is forced to retrieve different
        sections of the data from different replicas.
        """
        logger.debug("Starting cluster...")
        # Disable hinted handoff so it doesn't do what we expect repair to do
        self.cluster.set_configuration_options(values=self.default_config_options())
        # Create a cluster of 3 nodes, and a keyspace with RF=2 on all nodes
        # (disable read repair, as we want to test the full repair).
        self.cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3 = self.cluster.nodelist()
        with self.patient_cql_connection(node1) as session:
            create_ks(session, "ks", 2)
            create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        # Insert 1000 keys while node 3 is down. Because RF=2, all the data
        # will have a replica in one of the two available nodes
        logger.debug("Adding data with node 3 down...")
        node3.flush()
        node3.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node1, "ks") as session1:
            insert_c1c2(session1, keys=range(1000, 2000), consistency=ConsistencyLevel.ONE)
        self.cluster.flush()

        # Bring node 3 back up, it will not yet have any data
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Run repair on node 3. It should copy to node 3 all data that node 3 should
        # hold
        time.sleep(10)  # see CASSANDRA-4373
        logger.debug("starting repair...")
        info = node3.repair(keyspace="ks")
        logger.debug(info[0])
        logger.debug(info[1])

        # check that node 3 can read all 1000 partitions, even if node 1 or
        # or node 2 is down. Note that if both were down, it can't, because
        # about a third of the data is only replicated on node1 and node2.
        with self.patient_exclusive_cql_connection(node3, "ks") as session3:
            logger.debug("Checking read with no node down...")
            result = list(session3.execute("SELECT * FROM cf LIMIT 2000"))
            assert len(result) == 1000, len(result)
            logger.debug("Checking read with node 1 down...")
            node1.flush()
            node1.stop(wait_other_notice=True)
            result = list(session3.execute("SELECT * FROM cf LIMIT 2000"))
            assert len(result) == 1000, len(result)
            node1.start(wait_other_notice=True, wait_for_binary_proto=True)
            logger.debug("Checking read with node 2 down...")
            node2.flush()
            node2.stop(wait_other_notice=True)
            result = list(session3.execute("SELECT * FROM cf LIMIT 2000"))
            assert len(result) == 1000, len(result)
            node2.start(wait_other_notice=True, wait_for_binary_proto=True)

    def _repair_kill_1_test(self, kill_master=True):
        """
        Killing the master node of a repair stops the repair (obviously), but
        does not otherwise cause problems on the other nodes.
        """
        # Start a cluster of two nodes, and create a keyspace with RF=2, and
        # a table with one partition. Hinted handoff and read repair are disabled
        # so they don't fix the problems which repair is supposed to fix.
        self.cluster.set_configuration_options(values=self.default_config_options())
        self.cluster.populate(2).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()
        with self.patient_cql_connection(node1) as session:
            create_ks(session, "ks", 2)
            create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})
        # Insert 1000 keys *only* on node 1, another 1000 keys *only* on node 2
        node2.flush()
        node2.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node1, "ks") as session1:
            insert_c1c2(session1, keys=range(1000, 2000), consistency=ConsistencyLevel.ONE)
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            insert_c1c2(session2, keys=range(2000, 3000), consistency=ConsistencyLevel.ONE)
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Run repair on node 1, and kill this node quickly after repair started
        def do_repair():
            try:
                info = node1.repair(keyspace="ks")
                logger.debug(info[0])
                logger.debug(info[1])
            except NodetoolError:
                pass

        executor = ThreadPoolExecutor(max_workers=1)
        thread1 = executor.submit(do_repair)

        node1.watch_log_for("starting user-requested repair")
        time.sleep(random.uniform(0.0, 0.5))
        if kill_master:
            node1.stop(wait_other_notice=True)
        else:
            node2.stop(wait_other_notice=True)
        thread1.result()

        # Check that we can still read from the unkilled node normally.
        # We expect to see at least 1000 partitions - potentially up to
        # 2000 depending on how far the repair progressed.
        if kill_master:
            session = self.patient_exclusive_cql_connection(node2, "ks")
        else:
            session = self.patient_exclusive_cql_connection(node1, "ks")
        count = len(list(session.execute("SELECT * FROM cf LIMIT 3000")))
        logger.debug("count is %d" % count)
        assert count >= 1000 and count <= 2000

        if kill_master:
            node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        else:
            node2.start(wait_other_notice=True, wait_for_binary_proto=True)

    def _repair_kill_2_test(self):
        """
        Killing a participant (non-master) of a repair stops the repair with
        an error. Note that this doesn't work on Cassandra - see
        https://support.datastax.com/hc/en-us/articles/204226119-Troubleshooting-hanging-repairs
        Moreover, the other nodes continue to work correctly.
        """
        self._repair_kill_1_test(False)

    def _repair_kill_3_test(self):
        """
        When a node busy in being a repair master is killed, check that it
        shuts down normally and doesn't crash because of shut down bugs.
        Scylla issue #699 caused this test to fail - the repair continues
        through the shutdown, and then crashes (with an assertion failure)
        when it suddenly noticed the data structures it uses are gone.
        """
        # Start a cluster of two nodes, and create a keyspace with RF=2, and
        # a table with one partition. Hinted handoff and read repair are disabled
        # so they don't fix the problems which repair is supposed to fix.
        self.cluster.set_configuration_options(values=self.default_config_options())
        self.cluster.populate(2).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()
        with self.patient_cql_connection(node1) as session:
            create_ks(session, "ks", 2)
            create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})
        # Insert 1000 keys *only* on node 1, another 1000 keys *only* on node 2
        node2.flush()
        node2.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node1, "ks") as session1:
            insert_c1c2(session1, keys=range(1000, 2000), consistency=ConsistencyLevel.ONE)
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            insert_c1c2(session2, keys=range(2000, 3000), consistency=ConsistencyLevel.ONE)
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Run repair on node 1, and kill this node as soon as repair started
        def do_repair():
            try:
                info = node1.repair(keyspace="ks")
                logger.debug(info[0])
                logger.debug(info[1])
            except NodetoolError:
                pass

        executor = ThreadPoolExecutor(max_workers=1)
        thread1 = executor.submit(do_repair)

        node1.watch_log_for("starting user-requested repair")
        node1.stop(wait_other_notice=True)
        thread1.result()

        # We don't want to see any assertion failures like in isue #699 :-(
        match = node1.grep_log("Assertion .* failed.")
        logger.debug(match)
        assert len(match) == 0

    def _repair_during_update_test(self):
        """
        Test that a repair works correctly in parallel with data being
        updated: We set up a cluster of two replicas with different data,
        and run a repair on it in parallel with adding more data to both
        nodes - and verify that at the end both nodes have all the data.
        """
        # Start a cluster of two nodes, and create a keyspace with RF=2, and
        # a table with one partition. Hinted handoff and read repair are disabled
        # so they don't fix the problems which repair is supposed to fix.
        num_keys = 10000 if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode != "debug" else 1000
        self.cluster.set_configuration_options(values=self.default_config_options())
        self.cluster.populate(generate_cluster_topology(dc_num=1, rack_num=2, nodes_per_rack=1)).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()
        with self.patient_cql_connection(node1) as session:
            create_ks(session, "ks", 2)
            create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        # Insert num_keys keys *only* on node 1, another num_keys keys *only* on node 2:
        node2.flush()
        node2.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node1, "ks") as session1:
            insert_c1c2(session1, keys=range(num_keys), consistency=ConsistencyLevel.ONE)
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            insert_c1c2(session2, keys=range(num_keys, 2 * num_keys), consistency=ConsistencyLevel.ONE)
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Run repair on node 1 in the background
        def do_repair():
            try:
                logger.debug(f"Repairing {node1.name}")
                info = node1.repair(keyspace="ks")
                logger.debug(f"Repairing {node1.name} done")
                logger.debug(info[0])
                logger.debug(info[1])
            except NodetoolError:
                pass

        executor = ThreadPoolExecutor(max_workers=1)
        thread1 = executor.submit(do_repair)

        # In parallel with the repair, for as long as it doesn't finish,
        # we write more data to both nodes
        original_count = 2 * num_keys
        count = original_count
        session = self.patient_cql_connection(node1, "ks")
        while not thread1.done():
            prev_count = count
            add_keys = num_keys // 10 if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode != "debug" else 1
            count = count + add_keys
            logger.debug(f"Inserting {add_keys} key(s)")
            insert_c1c2(session, keys=range(prev_count, count), consistency=ConsistencyLevel.TWO)
        logger.debug("wrote %d partitions in parallel with repair" % (count - original_count))
        thread1.result()

        # Check that all nodes have all data
        self.check_rows_on_node(node1, count)
        self.check_rows_on_node(node2, count)

    def _repair_with_down_nodes_1_test(self):
        """
        Test that a repair fails when no replica can be found for one of the
        ranges being repaired, because of a down node.
        """
        # Start a cluster of three nodes, and create a keyspace with RF=2, and
        # an empty table. We don't need any data in the table to check whether
        # repair complains about the missing neighbors.
        self.cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 2)
        create_cf(session, "cf", columns={"c1": "text", "c2": "text"})

        # Bring down node 3, and start repair on node 2. Note that because we
        # have 3 nodes and RF=2, half of the vnodes in node 2 will have as
        # their only other replica the dead node 3.
        node3.stop(wait_other_notice=True)
        with pytest.raises(NodetoolError):
            node2.repair(keyspace="ks")

    def _repair_with_down_nodes_1a_test(self):
        """
        When we have 3 nodes with RF=2, bring down one of the nodes and
        start repair on another. This repair will fail because for some of
        the vnodes, its second replica is the dead node. However, the
        purpose of this test is to verify that beyond the repair failing,
        it actually repairs what we could repair - i.e., data in vnodes
        replicated only in the live nodes. When the dead node later is
        brought back up, and repair is started on it, the entire cluster
        will become repaired and have all the partitions.
        Note that had the first repair done nothing, the second repair would
        not have been enough, because the second repair only repairs the
        token ranges held by one node - and these are not all the ranges.
        """
        # Start a cluster of 3 nodes, and a keyspace with RF=2, and a table.
        self.cluster.set_configuration_options(values=self.default_config_options())
        topology_layout = {"dc1": {"r1": 2, "r2": 1}}
        self.cluster.populate(topology_layout).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3 = self.cluster.nodelist()
        with self.patient_cql_connection(node1) as session:
            create_ks(session, "ks", 2)
            create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})
        # We want to put different data on node 1 and on node 2 so repair
        # of these nodes has something to do. We can't write specific
        # partitions specifically to node 1 directly because on 3 nodes with
        # RF=2, node 1 only carries part of the token ranges! So we need to
        # write to pairs of nodes - the pair 1&3 and the pair 2&3.
        node2.flush()
        node2.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node1, "ks") as session1:
            insert_c1c2(session1, keys=range(1000), consistency=ConsistencyLevel.ONE)
        # let ConsistencyLevel.ONE delayed replication succeed (to node 3) or
        # timeout (to node 2)
        time.sleep(10)

        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            insert_c1c2(session2, keys=range(1000, 2000), consistency=ConsistencyLevel.ONE)
        time.sleep(10)

        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Shut down node 3, and start repair on node 2. Note that because we
        # have 3 nodes and RF=2, half of the vnodes in node 2 will have as
        # their only other replica the dead node 3, so the repair is supposed
        # to fail (this was also tested by the previous test function).
        # But the other half of the vnodes to have their other replica alive
        # (node 1), and may be repaired by the repair command.
        node3.flush()
        node3.stop(wait_other_notice=True)

        # Before the repair, doing SELECT * will return *around* (but not
        # exactly!) 1000 partitions. We can't check it because it's not
        # exactly 1000.
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            result = list(session2.execute("SELECT * from cf"))
            logger.debug(len(result))

        if "tablets" not in self.scylla_features:
            with pytest.raises(NodetoolError):
                node2.repair(keyspace="ks")

        # Check whether despite node 3 being down and the repair failing,
        # the vnodes whose replicas are node 1 and 2 (these are one half of
        # the data on node 2) could have been repaired. Unfortunately, we
        # cannot do a "SELECT *" on just node 2 because it is missing some
        # of the ranges. So we need to run the query with both node 1 and 2
        # alive (we can't be sure which of these will be queried, but we
        # assume that after a repair they will have the same data for
        # ranges they both own).
        # Despite the above repair failing, it did something. We will now
        # see about 1300 partitions in the following query. But we can't
        # check this number because it is not exact.
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            result = list(session2.execute("SELECT * from cf"))
            logger.debug(len(result))

        # Bring up also node 3. Trying "SELECT *" again will still not show
        # all 2000 partitions, because we still have different data in node 3
        # and node 2 because node 3 was down during the above repair.
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            result = list(session2.execute("SELECT * from cf"))
            logger.debug(len(result))

        # Repair node 3's ranges. This will not repair the ranges held only
        # by node 1 and 2, but we were hoping that the failed repair above
        # already did this. So after this additional repair, so should finally
        # have the full 2000 partitions.
        node3.repair(keyspace="ks")
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            result = list(session2.execute("SELECT * from cf"))
            assert len(result) == 2000

    def _repair_with_down_nodes_2_test(self):
        """
        Test that a repair fails when one of the replicas of one of the ranges
        being repaired is missing. The fact that another replica does exist is
        not enough.
        """
        # Start a cluster of 4 nodes, and create a keyspace with RF=3, and
        # an empty table. We don't need any data in the table to check whether
        # repair complains about the missing neighbors.
        self.cluster.populate(4).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3, _node4 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 3)
        create_cf(session, "cf", columns={"c1": "text", "c2": "text"})

        # Bring down node 3, and start repair on node 2. Note that because we
        # have 4 nodes and RF=3, 2/3rds of the vnodes in node 2 will have the
        # dead node 3 as one of their replicas.
        node3.stop(wait_other_notice=True)
        with pytest.raises(NodetoolError):
            node2.repair(keyspace="ks")

    def _repair_with_down_nodes_2a_test(self):
        """
        This test is similar to repair_with_down_nodes_1a_test, except we
        have 4 nodes with RF=3, and shut down node 4.
        Because RF=3, repair's failure mechanism is now slightly differently
        the one the 1a test: In this test, all token ranges (vnodes) have at
        least one other replica alive (as opposed to 1a where some of them
        had no living replica to repair with); Some ranges have all replicas
        alive (in nodes 1,2,3) and can be fully repaired as in test 1a. Yet
        other token ranges have one replica alive and one dead (in node 4),
        and we want to check whether we make an effort to repair between these
        living replicas, or not.

        For the same test we did in 1a - of whether a second repair of dead node
        once it comes up completes the repair of everything - it is enough
        that the partial repair only repairs ranges for which all replicas
        is alive. This test does NOT test what the partial repair did with the
        ranges for which one of the replicas was dead. Test 2b below does that.
        """
        # Start a cluster of 4 nodes, and a keyspace with RF=3, and a table.
        self.cluster.set_configuration_options(values=self.default_config_options())
        topology_layout = {"dc1": {"r1": 2, "r2": 1, "r3": 1}}
        self.cluster.populate(topology_layout).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, _node3, node4 = self.cluster.nodelist()
        with self.patient_cql_connection(node1) as session:
            create_ks(session, "ks", 3)
            create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})
        # We want to put different data on node 1 and on node 2 so repair
        # of these nodes has something to do. We can't write specific
        # partitions specifically to node 1 directly because on 4 nodes with
        # RF=3, node 1 only carries part of the token ranges. So we need to
        # write to triplets of nodes - the pair 1,3,4 and the pair 2,3,4.
        node2.flush()
        node2.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node1, "ks") as session1:
            insert_c1c2(session1, keys=range(1000), consistency=ConsistencyLevel.ONE)
        # let ConsistencyLevel.ONE delayed replication succeed (to node 3,4) or
        # timeout (to node 2)
        time.sleep(10)

        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            insert_c1c2(session2, keys=range(1000, 2000), consistency=ConsistencyLevel.ONE)
        time.sleep(10)

        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Shut down node 4, and start repair on node 2.
        node4.flush()
        node4.stop(wait_other_notice=True)

        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            result = list(session2.execute("SELECT * from cf"))
            logger.debug(len(result))

        if "tablets" not in self.scylla_features:
            with pytest.raises(NodetoolError):
                node2.repair(keyspace="ks")

        # NOTE: In test 1a, at this point we needed to bring back the down
        # node and repair it, before we have all the data available for query.
        # But in this test, because of the RF=3, if repair tried hard enough
        # to use the living replicas and not give up prematurely, at this
        # point we could have already seen the full data. We don't test this
        # in this test (we will below, in test 2b), and merely test that like
        # in test 1a, another repair of the revived node will make all the
        # data available.
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            result = list(session2.execute("SELECT * from cf"))
            logger.debug(len(result))

        node4.start(wait_other_notice=True, wait_for_binary_proto=True)
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            result = list(session2.execute("SELECT * from cf"))
            logger.debug(len(result))

        # Repair node 4's ranges. This will not repair the ranges held only
        # by node 1,2,3, but we were hoping that the failed repair above
        # already did this. So after this additional repair, so should finally
        # have the full 2000 partitions.
        node4.repair(keyspace="ks")
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            result = list(session2.execute("SELECT * from cf"))
            logger.debug(len(result))
            assert len(result) == 2000

    def _repair_with_down_nodes_2b_test(self):
        """
        This is a stricter version of test 2a above. We keep it as a separate
        test because it fails miserably on Apache Cassandra (and older
        versions of Scylla). In this test we confirm that when repair sees
        some replicas are dead and some are alive, it does its best to
        repair the data between the live nodes, instead of giving up early.
        Apparently neither Apache Cassandra nor old versions of Scylla tried
        hard enough.
        """
        # Start a cluster of 4 nodes, and a keyspace with RF=3, and a table.
        self.cluster.set_configuration_options(values=self.default_config_options())
        self.cluster.populate(4).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3, node4 = self.cluster.nodelist()
        with self.patient_cql_connection(node1) as session:
            create_ks(session, "ks", 3)
            create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})
        # We want to put different data on node 1 and on node 2 so repair
        # of these nodes has something to do. We can't write specific
        # partitions specifically to node 1 directly because on 4 nodes with
        # RF=3, node 1 only carries part of the token ranges. So we need to
        # write to triplets of nodes - the pair 1,3,4 and the pair 2,3,4.
        node2.flush()
        node2.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node1, "ks") as session1:
            insert_c1c2(session1, keys=range(1000), consistency=ConsistencyLevel.TWO)
        # let ConsistencyLevel.TWO delayed replication succeed (to node 3,4) or
        # timeout (to node 2)
        time.sleep(10)

        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            insert_c1c2(session2, keys=range(1000, 2000), consistency=ConsistencyLevel.TWO)
        time.sleep(10)

        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Shut down node 4, and start repair on nodes 1,2,3. To repair all
        # the ranges held by these three nodes, we unfortunately need to
        # start a full repair on two of them - "-pr" repair would not be
        # enough because the ranges whose primary is the dead node 4 will
        # not be repaired.
        # The purpose of this test is to confirm whether the repair done on
        # the 3 living nodes will try hard enough to reconcile their data despite
        # the fact that some of the replicas - on node 4 - are not available.
        node4.flush()
        node4.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            result = list(session2.execute("SELECT * from cf"))
            logger.debug(len(result))

        with pytest.raises(NodetoolError):
            node2.repair(keyspace="ks")
        with pytest.raises(NodetoolError):
            node3.repair(keyspace="ks")

        # Try "SELECT *" again, with 4 still down. This should already return
        # the full list of 2000 partitions, even without repairing node 4 (or
        # bringing it up), because we have RF=3 so none of the data lives only
        # on node 4, and if repair was diligent enough, it could repair the
        # 3 living nodes.
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            result = list(session2.execute("SELECT * from cf"))
            logger.debug(len(result))
            assert len(result) == 2000

    def _repair_abort_test(self):  # noqa: PLR0915
        """
        Add different data to each node, then start repair in background,
        try to abort repair before complete, verify the repair streaming stops,
        and some keys aren't synced.
        """
        # Disable hinted handoff so it doesn't do what we expect repair to do
        self.cluster.set_configuration_options(values=self.default_config_options())
        # Create a cluster of 3 nodes, and a keyspace with RF=3 on all nodes
        # (disable read repair, as we want to test the full repair).
        self.cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3 = self.cluster.nodelist()
        with self.patient_cql_connection(node1) as session:
            create_ks(session, "ks", 3)
            create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        keys_unit = 3000
        # Insert 3000 keys *only* on node 1, another 3000 keys *only* on node 2,
        # another 3000 *only on node 3:
        logger.debug("Adding data only on node 1...")
        node2.flush()
        node2.stop(wait_other_notice=True)
        node3.flush()
        node3.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node1, "ks") as session1:
            insert_c1c2(session1, keys=range(keys_unit), consistency=ConsistencyLevel.ONE)
        self.cluster.flush()

        logger.debug("Adding data only on node 2...")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            insert_c1c2(session2, keys=range(keys_unit, 2 * keys_unit), consistency=ConsistencyLevel.ONE)

        logger.debug("Adding data only on node 3...")
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        node2.flush()
        node2.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node3, "ks") as session3:
            insert_c1c2(session3, keys=range(2 * keys_unit, 3 * keys_unit), consistency=ConsistencyLevel.ONE)

        # Bring up all 3 nodes, each should have different data
        self.cluster.start_nodes([node1, node2], wait_other_notice=True, wait_for_binary_proto=True)

        # Run repair on (arbitrarily), node 3
        time.sleep(10)  # see CASSANDRA-4373
        logger.debug("starting repair...")

        def checking_keys_num(prefix="", less_than_num=None):
            rows = 3 * keys_unit
            for node_to_check in self.cluster.nodes.values():
                stopped_nodes = []
                for node in self.cluster.nodes.values():
                    if node.is_running() and node is not node_to_check:
                        stopped_nodes.append(node)
                        node.stop(wait_other_notice=True)

                session = self.patient_exclusive_cql_connection(node_to_check, "ks")
                result = list(session.execute("SELECT * FROM cf LIMIT %d" % (rows * 2)))
                logger.debug(f"{prefix} - {node_to_check.name}, keys num: {len(result)}")
                if less_than_num:
                    assert len(result) <= less_than_num

                for node in stopped_nodes:
                    node.start(wait_other_notice=True, wait_for_binary_proto=True)

        def repair_thread(keyspace):
            try:
                logger.debug("Start repair")
                info = node3.repair(keyspace=keyspace)
                logger.debug(info[0])
                logger.debug(info[1])
            except Exception as ex:  # noqa: BLE001
                logger.debug(ex)

        checking_keys_num("Before Repair")

        mark = node3.mark_log()

        executor = ThreadPoolExecutor(max_workers=1)
        thread1 = executor.submit(repair_thread, "ks")

        logger.debug("Wait for Repair to start")
        # Older scylla reports x out of y ranges is being repaired.
        # Newer scylla reports m out of n tables is being repaired.
        node3.watch_log_for("Repair 5 out of|Started to repair 1 out of", timeout=200, from_mark=mark)
        logger.debug("Repair has started")

        logger.debug("Abort repair sessions")
        url = f"http://{get_ip_from_node(node3)}:{node3.api_port}/storage_service/force_terminate_repair"
        getoutput(f'curl -X POST  --header "Accept: application/json" {url}')
        thread1.result(timeout=120)

        logger.debug("Sleep 10 seconds")
        time.sleep(10)
        self.cluster.flush()
        checking_keys_num("After Abort", less_than_num=keys_unit * 3)

    def _repair_one_missing_row_test(self, same_shard_count=True):
        """
        Insert 999 keys on node1 and node2
        Insert another 1 key on node1 only
        Repair on node2
        Make sure node2 receives 1 row from node1 and send 0 row to node1
        """
        logger.debug("Starting cluster...")
        # Disable hinted handoff so it doesn't do what we expect repair to do
        self.cluster.set_configuration_options(values=self.default_config_options())
        self.cluster.populate(generate_cluster_topology(dc_num=1, rack_num=2, nodes_per_rack=1))
        node1, node2 = self.cluster.nodelist()
        if not same_shard_count:
            node1.set_smp(2)
            node2.set_smp(3)
            logger.debug("Set node1.smp=2, node2.smp=3")
        self.cluster.start(wait_for_binary_proto=True, wait_other_notice=True)

        nr_rows = 10000
        with self.patient_cql_connection(node1) as session:
            create_ks(session, "ks", 2)
            create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

            # Add nr_rows -1  keys on node 1 and node2
            insert_c1c2(session, keys=range(nr_rows - 1), consistency=ConsistencyLevel.ALL)

        # Insert 1 more keys on node1
        logger.debug("Adding data only on node 1...")
        node2.flush()
        node2.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node1, "ks") as session1:
            insert_c1c2(session1, keys=range(nr_rows - 1, nr_rows), consistency=ConsistencyLevel.ONE)

        # Bring up Node 2
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        logger.debug("starting repair...")
        node2.repair(keyspace="ks")
        if "tablets" not in self.scylla_features:
            # Node 1 is expected to receive 1 data row from node1
            self.check_repair_tx_rx_rows(node2, expected_tx_row_nr=0, expected_rx_row_nr=1)

        logger.debug("Check rows on node 1...")
        # Check that all nodes have all data
        self.check_rows_on_node(node1, nr_rows)
        logger.debug("Check rows on node 2...")
        self.check_rows_on_node(node2, nr_rows)
        logger.debug("Check rows done")

    def _repair_one_deleted_row_test(self, same_shard_count=True):
        """
        Insert 1000 keys on node1 and node2
        Delete 1 key on node1 only
        Repair on node2
        Make sure node2 receives 1 row (tombstone) from node1 and send 0 key to node1
        """
        logger.debug("Starting cluster...")
        # Disable hinted handoff so it doesn't do what we expect repair to do
        self.cluster.set_configuration_options(values=self.default_config_options())
        # Create a cluster of 2 nodes, and a keyspace with RF=3 on all nodes
        # (disable read repair, as we want to test the full repair).
        self.cluster.populate(generate_cluster_topology(dc_num=1, rack_num=2, nodes_per_rack=1))
        node1, node2 = self.cluster.nodelist()
        if not same_shard_count:
            node1.set_smp(2)
            node2.set_smp(3)
            logger.debug("Set node1.smp=2, node2.smp=3")
        self.cluster.start(wait_for_binary_proto=True, wait_other_notice=True)

        nr_rows = 10000
        with self.patient_cql_connection(node1) as session:
            create_ks(session, "ks", 2)
            create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

            # Add nr_rows keys on node 1 and node2
            insert_c1c2(session, keys=range(nr_rows), consistency=ConsistencyLevel.ALL)

        # Insert 1 more keys on node1
        logger.debug("Delete data only on node 1...")
        node2.flush()
        node2.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node1, "ks") as session1:
            query = SimpleStatement("DELETE FROM cf WHERE key ='key1'", consistency_level=ConsistencyLevel.ONE)
            session1.execute(query)

        # Bring up Node 2
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        logger.debug("starting repair...")
        node2.repair(keyspace="ks")
        if "tablets" not in self.scylla_features:
            # Node 2 is expected to receive 1 tombstone row from node1
            self.check_repair_tx_rx_rows(node2, expected_tx_row_nr=0, expected_rx_row_nr=1)

        # Check that all nodes have all data
        logger.debug("Check rows on node 1...")
        self.check_rows_on_node(node1, nr_rows)
        logger.debug("Check rows on node 2...")
        self.check_rows_on_node(node2, nr_rows)
        logger.debug("Check rows done")

    def _repair_disjoint_row_2nodes_test(self, same_shard_count=True):
        """
        RF = 2. On each of 2 replicas, insert completely different data.
        Confirm that repairing a single of these nodes brings all the data
        to all three replicas.
        Make sure node2 sends 1000 rows and receives 1000 rows
        """
        logger.debug("Starting cluster...")
        self.cluster.set_configuration_options(values=self.default_config_options())

        self.cluster.populate(generate_cluster_topology(dc_num=1, rack_num=2, nodes_per_rack=1))
        node1, node2 = self.cluster.nodelist()
        if not same_shard_count:
            node1.set_smp(2)
            node2.set_smp(3)
            logger.debug("Set node1.smp=2, node2.smp=3")
        self.cluster.start(wait_for_binary_proto=True, wait_other_notice=True)

        with self.patient_cql_connection(node1) as session:
            create_ks(session, "ks", 2)
            create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        # Insert 1000 keys *only* on node 1, another 1000 keys *only* on node 2,
        logger.debug("Adding data only on node 1...")
        node2.flush()
        node2.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node1, "ks") as session1:
            insert_c1c2(session1, keys=range(1000), consistency=ConsistencyLevel.ONE)
        self.cluster.flush()

        logger.debug("Adding data only on node 2...")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            insert_c1c2(session2, keys=range(1000, 2000), consistency=ConsistencyLevel.ONE)

        # Bring up all 2 nodes, each should have different data
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Run repair on (arbitrarily), node 2
        logger.debug("starting repair...")
        node2.repair(keyspace="ks")
        if "tablets" not in self.scylla_features:
            # Check repair synced the correct number of rows
            self.check_repair_tx_rx_rows(node2, expected_tx_row_nr=1000, expected_rx_row_nr=1000)

        # Check that all nodes have all data
        logger.debug("Check rows on node 1...")
        self.check_rows_on_node(node1, 2000)
        logger.debug("Check rows on node 2...")
        self.check_rows_on_node(node2, 2000)
        logger.debug("Check rows done")

    def _repair_disjoint_row_3nodes_test(self, same_shard_count=True):
        """
        RF = 3. On each of 3 replicas, insert completely different data.
        Confirm that repairing a single of these nodes brings all the data
        to all three replicas.
        Make sure node3 sends 4000 rows and receives 2000 rows
        """
        logger.debug("Starting cluster...")
        # Disable hinted handoff so it doesn't do what we expect repair to do
        self.cluster.set_configuration_options(values=self.default_config_options())
        self.cluster.populate(generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1))
        node1, node2, node3 = self.cluster.nodelist()
        if not same_shard_count:
            node1.set_smp(2)
            node2.set_smp(2)
            node3.set_smp(3)
            logger.debug("Set node1.smp=2, node2.smp=2, node3.smp=3")
        self.cluster.start(wait_for_binary_proto=True, wait_other_notice=True)

        with self.patient_cql_connection(node1) as session:
            create_ks(session, "ks", 3)
            create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        # Insert 1000 keys *only* on node 1, another 1000 keys *only* on node 2,
        # another 1000 *only on node 3:
        logger.debug("Adding data only on node 1...")
        node2.flush()
        node2.stop(wait_other_notice=True)
        node3.flush()
        node3.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node1, "ks") as session1:
            insert_c1c2(session1, keys=range(1000, 2000), consistency=ConsistencyLevel.ONE)
        self.cluster.flush()
        logger.debug("Adding data only on node 2...")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            insert_c1c2(session2, keys=range(2000, 3000), consistency=ConsistencyLevel.ONE)
        logger.debug("Adding data only on node 3...")
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        node2.flush()
        node2.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node3, "ks") as session3:
            insert_c1c2(session3, keys=range(3000, 4000), consistency=ConsistencyLevel.ONE)

        # Bring up all 3 nodes, each should have different data
        self.cluster.start_nodes([node1, node2], wait_other_notice=True, wait_for_binary_proto=True)

        logger.debug("starting repair...")
        node3.repair(keyspace="ks")
        if "tablets" not in self.scylla_features:
            # Check repair synced the correct number of rows
            self.expected_tx_row_nr = 4000
            self.expected_rx_row_nr = 2000
            self.check_repair_tx_rx_rows(node3, expected_tx_row_nr=self.expected_tx_row_nr, expected_rx_row_nr=self.expected_rx_row_nr)

        # Check that all nodes have all data
        logger.debug("Check rows on node 1...")
        self.check_rows_on_node(node1, 3000)
        logger.debug("Check rows on node 2...")
        self.check_rows_on_node(node2, 3000)
        logger.debug("Check rows on node 3...")
        self.check_rows_on_node(node3, 3000)
        logger.debug("Check rows done")

    def _repair_joint_row_3nodes_same_key_same_value_test(self, same_shard_count=True):
        """
        Create data as follows

        Insert 10 to 15 to node 1
        Insert 25 to 30 to node 2
        Insert 15 to 20 into node 1 and node 3
        Insert 20 to 25 into node 2 and node 3

        So that

        Node 1 has range 10 20
        Node 2 has range 20 30
        Node 3 has range 15 25

        and

        Range 15 to 20 on node 1 and node 3 has the same key and value
        Range 20 to 25 on node 2 and node 3 has the same key and value

        That is

        Node1   10 15
        Node1,3 15 20
        Node2,3 20 25
        Node2   25 30

        Node3 will rx 5 rows (10 to 15) from node1 and rx 5 rows (25 to 30)
        from node2, tx 10 rows (20 to 25 and 25 to 30) to node 1 and tx 10
        rows (10 to 15 and 15 to 20) to node2
        """
        logger.debug("Starting 3 node cluster...")
        # Disable hinted handoff so it doesn't do what we expect repair to do
        self.cluster.set_configuration_options(values=self.default_config_options())
        self.cluster.populate(generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1))
        node1, node2, node3 = self.cluster.nodelist()
        if not same_shard_count:
            node1.set_smp(2)
            node2.set_smp(2)
            node3.set_smp(3)
            logger.debug("Set node1.smp=2, node2.smp=2, node3.smp=3")
        self.cluster.start(wait_for_binary_proto=True, wait_other_notice=True)

        with self.patient_cql_connection(node1) as session:
            create_ks(session, "ks", 3)
            create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        logger.debug("Adding data only on node 1...")
        node2.flush()
        node2.stop(wait_other_notice=True)
        node3.flush()
        node3.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node1, "ks") as session1:
            insert_c1c2(session1, keys=range(10, 15), consistency=ConsistencyLevel.ONE)
        self.cluster.flush()

        logger.debug("Adding data only on node 2...")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            insert_c1c2(session2, keys=range(25, 30), consistency=ConsistencyLevel.ONE)

        logger.debug("Adding data only on node 2 3...")
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        with self.patient_exclusive_cql_connection(node3, "ks") as session3:
            insert_c1c2(session3, keys=range(20, 25), consistency=ConsistencyLevel.TWO)

        logger.debug("Adding data only on node 1 3...")
        node2.flush()
        node2.stop(wait_other_notice=True)
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        with self.patient_exclusive_cql_connection(node1, "ks") as session1:
            insert_c1c2(session1, keys=range(15, 20), consistency=ConsistencyLevel.TWO)

        # Bring up all 3 nodes, each should have different data
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        logger.debug("starting repair...")
        node3.repair(keyspace="ks")
        if "tablets" not in self.scylla_features:
            # Check repair synced the correct number of rows
            self.check_repair_tx_rx_rows(node3, expected_tx_row_nr=20, expected_rx_row_nr=10)

        # Check that all nodes have all data
        logger.debug("Check rows on node 1...")
        self.check_rows_on_node(node1, 20)
        logger.debug("Check rows on node 2...")
        self.check_rows_on_node(node2, 20)
        logger.debug("Check rows on node 3...")
        self.check_rows_on_node(node3, 20)
        logger.debug("Check rows done")

    def _repair_joint_row_3nodes_same_key_diff_value_test(self, same_shard_count=True):
        """
        Create data as follows

        node1 10 20
        node2 20 30
        node3 15 25

        Since the value is different for the overlap ranges, range 15 to 20
        on node 1 and node 3 will have different hashes, range 20 to 25 on node
        2 and node 3 will have different hashes. Node 3 will rx 10 rows (range
        10 to 20) from node 1 and rx 10 rows (range 20 to 30) from node 2, tx 20
        rows (range 15 to 25 from node 3 and range 20 to 30 from node 2) to
        node1 and tx 20 rows (range 10 to 20 from node 1 and range 15 to 25
        from node2) to node 2.
        """

        logger.debug("Starting 3 node cluster...")
        # Disable hinted handoff so it doesn't do what we expect repair to do
        self.cluster.set_configuration_options(values=self.default_config_options())
        self.cluster.populate(generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1))
        node1, node2, node3 = self.cluster.nodelist()
        if not same_shard_count:
            node1.set_smp(2)
            node2.set_smp(2)
            node3.set_smp(3)
            logger.debug("Set node1.smp=2, node2.smp=2, node3.smp=3")
        self.cluster.start(wait_for_binary_proto=True, wait_other_notice=True)

        with self.patient_cql_connection(node1) as session:
            create_ks(session, "ks", 3)
            create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        logger.debug("Adding data only on node 1...")
        node2.flush()
        node2.stop(wait_other_notice=True)
        node3.flush()
        node3.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node1, "ks") as session1:
            insert_c1c2(session1, keys=range(10, 20), consistency=ConsistencyLevel.ONE)
        self.cluster.flush()
        logger.debug("Adding data only on node 2...")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node2, "ks") as session2:
            insert_c1c2(session2, keys=range(20, 30), consistency=ConsistencyLevel.ONE)
        logger.debug("Adding data only on node 3...")
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        node2.flush()
        node2.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(node3, "ks") as session3:
            insert_c1c2(session3, keys=range(15, 25), consistency=ConsistencyLevel.ONE)

        # Bring up all 3 nodes, each should have different data
        self.cluster.start_nodes([node1, node2], wait_other_notice=True, wait_for_binary_proto=True)

        logger.debug("starting repair...")
        info = node3.repair(keyspace="ks")
        logger.debug(info[0])
        logger.debug(info[1])

        if "tablets" not in self.scylla_features:
            # Check repair synced the correct number of rows
            self.check_repair_tx_rx_rows(node3, expected_tx_row_nr=40, expected_rx_row_nr=20)

        # Check that all nodes have all data
        logger.debug("Check rows on node 1...")
        self.check_rows_on_node(node1, 20)
        logger.debug("Check rows on node 2...")
        self.check_rows_on_node(node2, 20)
        logger.debug("Check rows on node 3...")
        self.check_rows_on_node(node3, 20)
        logger.debug("Check rows done")

    def _setup_cluster_prefilled_with_large_partitions(self):
        test_session = self.create_cluster_and_keyspace(num_of_nodes=self.NUM_OF_NODES, rf=self.RF, configuration_options=self.default_config_options())

        stmt = "create table {} (pk int, ck int, {}, clist list<int>, cset set<text>, cmap map<int, text>, PRIMARY KEY(pk, ck))".format(self.TABLE_NAME, ", ".join("c%d int" % i for i in range(1, self.NUM_OF_COLUMNS)))
        test_session.execute(stmt)

        # Prefill
        partitions = self.PARTITIONS
        rows_in_partition = self.ROWS_IN_PARTITION
        self.prefill_table_data(session=test_session, partition_range_end=partitions, rows_in_partition=rows_in_partition)

        big_partition = self.PARTITIONS + 1
        logger.debug(f"Create partition where pk = {big_partition} with {self.BIG_PARTITION_ROWS} rows")
        self.prefill_table_data(session=test_session, partition_range_start=big_partition, partition_range_end=big_partition, rows_in_partition=self.BIG_PARTITION_ROWS)

    def _repair_same_row_diff_value_3nodes_test(self, same_shard_count=True):
        """
        Create rows with same partition and clustering keys but with different value
        Run repair
        Make sure we do not write rows with same partition and clustering key into sstable writer
        """
        logger.debug("Starting 3 node cluster with hinted_handoff disabled...")
        # Disable hinted handoff so it doesn't do what we expect repair to do
        self.cluster.set_configuration_options(values={"hinted_handoff_enabled": False})
        self.cluster.populate(3)
        node1, node2, node3 = self.cluster.nodelist()
        if not same_shard_count:
            node1.set_smp(2)
            node2.set_smp(2)
            node3.set_smp(3)
            logger.debug("Set node1.smp=2, node2.smp=2, node3.smp=3")
        self.cluster.start(wait_for_binary_proto=True, wait_other_notice=True)

        session = self.patient_cql_connection(node1)

        session.execute("CREATE KEYSPACE ks WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3 };")
        session.execute("CREATE TABLE ks.tb (pk int, ck int, c0 int, c1 int, PRIMARY KEY(pk, ck));")

        nr_rows = 3

        logger.debug("Adding data only on node 1...")
        node2.stop(wait_other_notice=True)
        node3.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node1)
        session.execute("INSERT into ks.tb (pk,ck,c0,c1) values (0, 0, 1, 1)")
        session.execute("INSERT into ks.tb (pk,ck,c0,c1) values (0, 1, 1, 1)")
        session.execute("INSERT into ks.tb (pk,ck,c0,c1) values (0, 2, 1, 1)")
        self.cluster.flush()
        logger.debug("Adding data only on node 2...")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2)
        session.execute("INSERT into ks.tb (pk,ck,c0,c1) values (0, 0, 2, 2)")
        session.execute("INSERT into ks.tb (pk,ck,c0,c1) values (0, 1, 2, 2)")
        session.execute("INSERT into ks.tb (pk,ck,c0,c1) values (0, 2, 2, 2)")
        logger.debug("Adding data only on node 3...")
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        node2.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node3)
        session.execute("INSERT into ks.tb (pk,ck,c0,c1) values (0, 0, 3, 3)")
        session.execute("INSERT into ks.tb (pk,ck,c0,c1) values (0, 1, 3, 3)")
        session.execute("INSERT into ks.tb (pk,ck,c0,c1) values (0, 2, 3, 3)")

        # Bring up all 3 nodes, each should have different data
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        logger.debug("starting repair...")
        info = node3.repair(keyspace="ks")
        logger.debug(info[0])
        logger.debug(info[1])

        logger.debug("Run compact on node1, node2 and node3")
        self.cluster.compact()

        # Check repair synced the correct number of rows
        self.check_repair_tx_rx_rows(node3, expected_tx_row_nr=4 * nr_rows, expected_rx_row_nr=2 * nr_rows)


@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestRepairAdditional(RepairAdditionalBase):
    @pytest.mark.dtest_debug
    @pytest.mark.skip_if(with_feature("tablets"))
    def test_repair_triggering_off_strategy_compaction(self):
        """
        This test is checking that repair triggers off strategy on a timer
        """
        self.cluster.set_configuration_options(values=self.default_config_options())

        # Create a cluster of 2 nodes, and a keyspace with RF=2 on all nodes
        # (disable read repair, as we want to test the full repair).
        logger.debug("Starting cluster...")
        self.cluster.populate(2).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()
        with self.patient_cql_connection(node1) as session:
            create_ks(session, "ks", 2)
            create_cf(session, "cf1", read_repair=0.0, columns={"c1": "text", "c2": "text"})
            create_cf(session, "cf2", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        # Bring 2nd node down to make a keys for repair to work on
        node2.flush()
        node2.stop(wait_other_notice=True)

        # Populating some data
        with self.patient_exclusive_cql_connection(node1, "ks") as session1:
            insert_c1c2(session1, cf="cf1", keys=range(1000, 2000), consistency=ConsistencyLevel.ONE)
            insert_c1c2(session1, cf="cf2", keys=range(1000, 2000), consistency=ConsistencyLevel.ONE)

        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Test if compactions triggered properly
        self._run_repair_and_wait_for_compactions(node=node2, ks="ks", cf="cf1", aux_cf="cf2")

    @pytest.mark.dtest_debug
    def test_repair_schema(self):
        return self._repair_schema_test()

    def test_repair_schema_2(self):
        return self._repair_schema_2_test()

    def test_repair_cell_update(self):
        return self._repair_cell_update_test()

    def test_repair_cell_delete(self):
        return self._repair_cell_delete_test()

    def test_repair_row_delete(self):
        return self._repair_row_delete_test()

    def test_repair_partition_delete(self):
        return self._repair_partition_delete_test()

    @pytest.mark.dtest_debug
    def test_repair_ttl_update(self):
        return self._repair_ttl_update_test()

    @pytest.mark.skip_if(with_feature("tablets"))
    def test_repair_option_pr(self):
        return self._repair_option_pr_test()

    @pytest.mark.dtest_debug
    @pytest.mark.skip_if(with_feature("tablets"))
    def test_repair_option_pr_dc_host(self):
        return self._repair_option_pr_dc_host_test()

    @pytest.mark.dtest_debug
    @pytest.mark.skip_if(with_feature("tablets"))
    def test_repair_option_pr_multi_dc(self):
        return self._repair_option_pr_multi_dc_test()

    def test_repair_option_cf(self):
        return self._repair_option_cf_test()

    def test_repair_option_invalid_ks_cf(self):
        return self._repair_option_invalid_ks_cf_test()

    def test_repair_option_dc(self):
        return self._repair_option_dc_test()

    def test_repair_multiple(self):
        return self._repair_multiple_test()

    @pytest.mark.skip_if(with_feature("tablets"))
    def test_repair_multiple_pr(self):
        return self._repair_multiple_pr_test()

    @pytest.mark.skip_if(with_feature("tablets"))
    def test_repair_option_seq(self):
        return self._repair_option_seq_test()

    @pytest.mark.skip_if(with_feature("tablets"))
    def test_repair_kill_1(self, kill_master=True):
        return self._repair_kill_1_test()

    @pytest.mark.skip_if(with_feature("tablets"))
    def test_repair_kill_2(self):
        return self._repair_kill_2_test()

    @pytest.mark.skip_if(with_feature("tablets"))
    def test_repair_kill_3(self):
        return self._repair_kill_3_test()

    def test_repair_during_update(self):
        return self._repair_during_update_test()

    @pytest.mark.skip_if(with_feature("tablets"))
    def test_repair_with_down_nodes_1(self):
        return self._repair_with_down_nodes_1_test()

    def test_repair_with_down_nodes_1a(self):
        return self._repair_with_down_nodes_1a_test()

    @pytest.mark.skip_if(with_feature("tablets"))
    def test_repair_with_down_nodes_2(self):
        return self._repair_with_down_nodes_2_test()

    def test_repair_with_down_nodes_2a(self):
        return self._repair_with_down_nodes_2a_test()

    @pytest.mark.skip_if(with_feature("tablets"))
    def test_repair_with_down_nodes_2b(self):
        return self._repair_with_down_nodes_2b_test()

    @pytest.mark.skip_if(with_feature("tablets"))
    def test_repair_abort(self):
        return self._repair_abort_test()

    def test_repair_one_missing_row(self):
        return self._repair_one_missing_row_test()

    def test_repair_one_deleted_row(self):
        return self._repair_one_deleted_row_test()

    def test_repair_disjoint_row_2nodes(self):
        return self._repair_disjoint_row_2nodes_test()

    def test_repair_disjoint_row_3nodes(self):
        return self._repair_disjoint_row_3nodes_test()

    @pytest.mark.skip_if(with_feature("tablets"))
    def test_no_streaming_on_second_repair(self):
        self._repair_disjoint_row_3nodes_test()
        node1, node2, node3 = self.cluster.nodelist()
        self.cluster.start_nodes([node1, node2], wait_other_notice=True, wait_for_binary_proto=True)
        logger.debug("starting a second repair on node3...")
        node3.repair(keyspace="ks")
        # Check that the second repair did not sync any additional rows.
        # The same number of rx/tx of the first repair should be found in log without any additions.
        self.check_repair_tx_rx_rows(node3, expected_tx_row_nr=self.expected_tx_row_nr, expected_rx_row_nr=self.expected_rx_row_nr)

    def test_repair_joint_row_3nodes_1(self):
        return self._repair_joint_row_3nodes_same_key_same_value_test()

    def test_repair_joint_row_3nodes_2(self):
        return self._repair_joint_row_3nodes_same_key_diff_value_test()

    @pytest.mark.dtest_heavy
    def test_repair_one_missing_row_diff_shard_count(self):
        return self._repair_one_missing_row_test(same_shard_count=False)

    @pytest.mark.dtest_heavy
    def test_repair_one_deleted_row_diff_shard_count(self):
        return self._repair_one_deleted_row_test(same_shard_count=False)

    @pytest.mark.dtest_heavy
    def test_repair_disjoint_row_2nodes_diff_shard_count(self):
        return self._repair_disjoint_row_2nodes_test(same_shard_count=False)

    @pytest.mark.dtest_heavy
    def test_repair_disjoint_row_3nodes_diff_shard_count(self):
        return self._repair_disjoint_row_3nodes_test(same_shard_count=False)

    @pytest.mark.dtest_heavy
    def test_repair_joint_row_3nodes_1_diff_shard_count(self):
        return self._repair_joint_row_3nodes_same_key_same_value_test(same_shard_count=False)

    @pytest.mark.dtest_heavy
    def test_repair_joint_row_3nodes_2_diff_shard_count(self):
        return self._repair_joint_row_3nodes_same_key_diff_value_test(same_shard_count=False)

    @pytest.mark.dtest_heavy
    @pytest.mark.skip_if(with_feature("tablets"))
    def test_repair_same_row_diff_value_3nodes(self):
        return self._repair_same_row_diff_value_3nodes_test(same_shard_count=True)

    @pytest.mark.dtest_heavy
    @pytest.mark.skip_if(with_feature("tablets"))
    def test_repair_same_row_diff_value_3nodes_diff_shard_count(self):
        return self._repair_same_row_diff_value_3nodes_test(same_shard_count=False)

    @pytest.mark.skip_if(with_feature("tablets"))
    def test_repair_while_table_is_dropped(self):  # noqa: PLR0915
        """
        This test tries to drop table when parallel repair is executing, scylla will ignore the error, and repair won't fail.

        1. Create a cluster of 3 nodes with rf=3
        2. Stop node 2
        3. Insert data
        4. Start node 2
        5. Start repairs of two nodes in parallel
        6. Drop one table of ks when repair of ks starts
        7. Check log to verify dropped table is ignored during repair
        8. Read verify after repair
        """
        logger.debug("Starting cluster...")
        # Start a cluster of three nodes, and create a keyspace with RF=3.
        config_options = self.default_config_options().update({"enable_repair_based_node_ops": True})
        self.cluster.set_configuration_options(values=config_options, batch_commitlog=True)
        self.cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, _node3 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 3)

        # Take node2 down, and create a new table and data on node1 only.
        logger.debug("Creating table and data only on node 1...")
        node2.flush()
        node2.stop(wait_other_notice=True)

        create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})
        insert_c1c2(session, keys=range(2000), consistency=ConsistencyLevel.QUORUM, cf="cf")

        delete_table_num = 8
        for i in range(delete_table_num):
            cf = f"cf_del{i}"
            create_cf(session, cf, read_repair=0.0, columns={"c1": "text", "c2": "text"})
            insert_c1c2(session, keys=range(2000), consistency=ConsistencyLevel.QUORUM, cf=cf)

        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Run repairs of two tables on node1
        executor = ThreadPoolExecutor(max_workers=2)
        thread1 = executor.submit(lambda: node1.repair())
        thread2 = executor.submit(lambda: node2.repair())
        thread3 = executor.submit(lambda: node2.repair())

        res = node2.watch_log_for("sync data for keyspace=ks, status=started|starting user-requested repair for keyspace ks,")
        logger.debug(res)
        msg = res[0]
        m = re.search(r"\[(?:.*uuid=)?(?P<uuid>[\w-]+)\]", msg)
        assert m is not None, f"Could not find repair task uuid in '{msg}'"
        uuid = m.group("uuid")

        for i in range(delete_table_num):
            session.execute(f"DROP TABLE ks.cf_del{i}")
        logger.debug("Repair of ks just started, drop table ks.cf_del*")

        thread1.result()
        thread2.result()
        thread3.result()

        # verify that repair completed, and the dropped table is ignored during repair
        res = node2.watch_log_for(f"repair - repair.*{uuid}.* completed successfully$")
        logger.debug(res)

        # verify that the cf_del* were really deleted
        # and that cf contains the expected data
        for n in [1, 2]:
            logger.debug(f"checking data on node{n}...")
            node = self.cluster.nodelist()[n - 1]
            other = self.cluster.nodelist()[2 - n]
            if other.is_running():
                other.stop(wait_other_notice=False)
            if not node.is_running():
                node.start(wait_other_notice=False)
            cs = self.patient_cql_cluster_session(node, "ks", exclusive=True, consistency_level=ConsistencyLevel.ONE)
            session = cs.session
            for i in range(delete_table_num):
                cf = f"cf_del{i}"
                out, err = node.run_cqlsh(f"describe table ks.{cf}", return_output=True)
                expr = rf"{cf}.* not found"
                assert re.search(expr, out + err), f"{expr} not found in {out + err}"
                query = SimpleStatement(f"SELECT * FROM {cf} LIMIT 1", consistency_level=ConsistencyLevel.ONE)
                with pytest.raises(InvalidRequest):
                    list(session.execute(query))

            query = SimpleStatement(f"SELECT * FROM cf LIMIT 3000", consistency_level=ConsistencyLevel.ONE)
            result = list(session.execute(query))
            assert len(result) == 2000, len(result)

    def test_repair_large_partition_new_rows(self):
        """
        Add new keys on large-partitions-table for all nodes except for node2
        Repair on node2
        Make sure node2 receives/transfer the correct number of rows
        """
        self._setup_cluster_prefilled_with_large_partitions()

        big_partition = self.PARTITIONS + 1
        total_rows = self.PARTITIONS * self.ROWS_IN_PARTITION + self.BIG_PARTITION_ROWS

        # Test adding new rows

        node1 = self.cluster.nodelist()[0]
        repaired_node = self.cluster.nodelist()[1]  # zero-based "2"

        self.cluster.flush()
        logger.debug(f"Stopping: {repaired_node.name}")
        repaired_node.stop(wait_other_notice=True)

        logger.debug("Inserting new data on all nodes except for node 2...")
        session = self.patient_cql_connection(node1)
        session.set_keyspace(self.KEYSPACE_NAME)
        num_of_new_rows = 50

        stmts = []
        logger.debug(f"Going to generate {num_of_new_rows} CQL inserts, for table {self.TABLE_NAME}")
        for i in range(1, num_of_new_rows + 1):
            logger.debug(f"#{i} cmd - ")
            stmts.append(self.create_insert_command(pk=big_partition + i, ck=random.randint(1, self.ROWS_IN_PARTITION)))

        for stmt in stmts:
            session.execute(stmt)

        # Bring up Node 2
        logger.debug(f"Starting: {repaired_node.name}")
        repaired_node.start(wait_other_notice=True, wait_for_binary_proto=True)

        logger.debug(f"starting repair on {repaired_node.name}")
        repaired_node.repair(keyspace=self.KEYSPACE_NAME)

        # Check for correct number of rows on nodes
        total_rows += num_of_new_rows

        # Node 2 is expected to receive num_of_new_rows from other nodes, and transfer nothing.
        expected_tx_row_nr = 0
        expected_rx_row_nr = num_of_new_rows
        self.verify_repair_tx_rx_rows(node_idx=2, expected_tx_row_nr=expected_tx_row_nr, expected_rx_row_nr=expected_rx_row_nr, list_metrics=self.LIST_ROW_LEVEL_REPAIR_METRICS)

        self.verify_num_of_rows_on_nodes(list_nodes=[node1, repaired_node], total_rows=total_rows)

    def test_repair_large_partition_existing_rows(self):
        """
        Insert keys on large partitions for all nodes
        Insert some updates for existing keys on all nodes except for node2
        Repair on node2
        Make sure node2 receives/transfer the correct number of rows
        """

        self._setup_cluster_prefilled_with_large_partitions()

        # Prefill
        partitions = self.PARTITIONS
        rows_in_partition = self.ROWS_IN_PARTITION

        big_partition = self.PARTITIONS + 1
        big_partition_rows = 10000
        total_rows = partitions * rows_in_partition + big_partition_rows

        node1 = self.cluster.nodelist()[0]
        repaired_node = self.cluster.nodelist()[1]  # zero-based "2"

        # Test updating existing rows #################################################################################

        self.cluster.flush()
        logger.debug(f"Stopping: {repaired_node.name}")
        repaired_node.stop(wait_other_notice=True)
        num_of_updates = 50
        num_of_total_updates = num_of_updates * 2
        logger.debug(f"Updating data on all nodes except for {repaired_node.name}...")
        self.write_table_updates(node=node1, partitions_range_end=partitions, rows_in_partition=rows_in_partition, num_of_updates=num_of_updates)

        self.write_table_updates(node=node1, partitions_range_end=big_partition, partitions_range_start=big_partition, rows_in_partition=big_partition_rows, num_of_updates=num_of_updates)

        # Bring up repaired_node
        logger.debug(f"Starting: {repaired_node.name}")
        repaired_node.start(wait_other_notice=True, wait_for_binary_proto=True)

        logger.debug(f"starting repair on {repaired_node.name}")
        repaired_node.repair(keyspace=self.KEYSPACE_NAME)

        # repaired_node is expected to receive up-to num_of_updates rows from other nodes,
        # and transfer as twice(NUM_OF_PEERS) much.
        self.verify_repair_tx_rx_rows(node_idx=2, expected_tx_row_nr=num_of_total_updates * self.NUM_OF_PEERS, expected_rx_row_nr=num_of_total_updates, list_metrics=self.LIST_ROW_LEVEL_REPAIR_METRICS)

        self.verify_num_of_rows_on_nodes(list_nodes=[node1, repaired_node], total_rows=total_rows)

    @pytest.mark.skip_if(with_feature("tablets"))
    def test_repair_ignore_nodes(self):
        """
        Test that a repair succeeds when the ignore_nodes parameter is used for a down node.
        """
        keyspace, table = self._setup_cluster_with_table()
        node1, node2, node3 = self.cluster.nodelist()
        query_cl1 = SimpleStatement(f"SELECT * FROM {table}", consistency_level=ConsistencyLevel.ONE)
        query_cl2 = SimpleStatement(f"SELECT * FROM {table}", consistency_level=ConsistencyLevel.TWO)

        logger.debug("Stop node2 to be repaired before writing data.")
        node2.stop(wait_other_notice=True)
        with self.patient_cql_connection(node1, keyspace) as session:
            logger.debug("Writing data to 2 other nodes")
            insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.TWO)
            result = list(session.execute(query_cl2))
            assert len(result) == 1000

        logger.debug("Bring up node2 and take down the node to be ignored - node3.")
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)
        node3.stop(wait_other_notice=True)

        logger.debug("Repair node2, ignoring node3")
        self._run_repair_api(run_on_node=node2, keyspace=keyspace, ignore_nodes=[node3])
        logger.debug("Verify node2 data.")
        with self.patient_cql_connection(node2, keyspace) as session:
            result = list(session.execute(query_cl1))
            assert len(result) == 1000

    @pytest.mark.skip_if(with_feature("tablets"))
    def test_repair_ignore_nodes_errors(self):
        """
        Test that a repair succeeds when the ignore_nodes parameter is used for cluster nodes.
        """
        keyspace, table = self._setup_cluster_with_table()
        node1, node2, node3 = self.cluster.nodelist()
        query_cl1 = SimpleStatement(f"SELECT * FROM {table}", consistency_level=ConsistencyLevel.ONE)
        query_cl2 = SimpleStatement(f"SELECT * FROM {table}", consistency_level=ConsistencyLevel.TWO)

        node2.flush()
        node2.stop(wait_other_notice=True)

        logger.debug("Writing data to 2 other nodes")
        with self.patient_cql_connection(node1, keyspace) as session:
            insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.TWO)
            result = list(session.execute(query_cl2))
            assert len(result) == 1000

        logger.debug("Bring up node2 and take down the node to be ignored - node3.")
        node3.flush()
        node3.stop(wait_other_notice=True)
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)

        logger.debug("Repair node2 using ignore_nodes of the 2 other replicas to cause a 'short-circuit' repair.")
        self._run_repair_api(run_on_node=node2, keyspace=keyspace, ignore_nodes=[node1, node3])

        logger.debug("Verify node2 has no data following this repair.")
        node1.flush()
        node1.stop(wait_other_notice=True)
        with self.patient_cql_connection(node2, keyspace) as session:
            result = list(session.execute(query_cl1))
            assert len(result) == 0

        logger.debug("Run a second repair on node2 when node1 is back up, ignoring node3 only => expected to get all data.")
        node1.start(wait_for_binary_proto=True, wait_other_notice=True)
        self._run_repair_api(run_on_node=node2, keyspace=keyspace, ignore_nodes=[node3])
        node1.flush()
        node1.stop(wait_other_notice=True)

        logger.debug("Verify node2 has all data following this repair.")
        with self.patient_cql_connection(node2, keyspace) as session:
            result = list(session.execute(query_cl1))
            assert len(result) == 1000

    def test_repair_streams_data_from_closest_node(self):  # noqa: PLR0915
        """To reduce cross-dc communication, when repairing, Scylla should get data from closest nodes first.
        This test verifies the order of nodes from which repair fetches the data."""
        # prepare multi-dc cluster with some data
        self.ignore_log_patterns += ["Could not find CDC generation"]
        config_options = self.default_config_options() | {"enable_repair_based_node_ops": True, "allowed_repair_based_node_ops": "bootstrap,replace,removenode,decommission,rebuild"}
        self.cluster.set_configuration_options(values=config_options)
        cluster_topology = generate_cluster_topology(dc_num=2, rack_num=2, nodes_per_rack=1, dc_name_prefix="dc", rack_name_prefix="rack")
        self.cluster.populate(cluster_topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1_1, node1_2, node2_1, node2_2 = self.cluster.nodelist()
        with self.patient_cql_cluster_session(node1_1) as session:
            create_ks(session, "ks", {"dc1": 2, "dc2": 2})
            create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        num_keys = 1000
        with self.patient_cql_cluster_session(node1_1, "ks") as session1:
            insert_c1c2(session1, keys=range(num_keys), consistency=ConsistencyLevel.ALL)
        self.cluster.flush()

        node1_2.stop(wait_other_notice=True)

        # delete data on node2 to have something to repair
        table_folder = get_node_cf_dir(node=node1_2, ks_name="ks", cf_name="cf")
        logger.info(f"Remove SSTables from {node1_2.name} '{table_folder}' folder")
        remove_files_in_folder(table_folder)

        node1_2.start(wait_other_notice=True, wait_for_binary_proto=True)

        # set debug logging to see exact peers order that repair will get data from
        rc, out = getstatusoutput(f"curl -X POST http://{node1_2.address()}:{node1_2.api_port}/system/logger/repair?level=debug")
        assert rc == 0, f"error during setting log level: {out}"

        # start repair on node2 and wait until it finishes
        from_mark = node1_2.mark_log()

        split_regex = re.compile(r"\s*,\s*")

        def get_peers(s):
            ret = split_regex.split(s)
            assert ret, f"found no peers in '{s}'"
            return ret

        if "tablets" not in self.scylla_features:
            RepairAdditionalBase._run_repair_and_check_completed(node=node1_2, ks="ks", cf="cf", from_mark=from_mark)

            # verify that first peer node is the one from the same DC
            matchings = node1_2.grep_log(rf"Started Row Level Repair \(Master\).+ peers={self.PEERS_RE}, repair_meta_id")
            assert matchings
            for matches in matchings:
                peers = get_peers(matches[1].group("peers"))
                assert peers[0] == node1_1.address() or peers[0] == node1_1.hostid(), "Missing rows should be fetched from a node from the same dc first"

        # add new node to test RBNO
        self.cluster.flush()
        node2_2.stop(wait_other_notice=True)
        new_node: Node = self.cluster.new_node(5, data_center="dc2", rack="rack2", is_seed=False)
        new_node.start(replace_node_host_id=node2_2.hostid(), no_wait=False, jvm_args=["--logger-log-level", "repair=debug"])

        # verify that new node will get data from the same DC
        new_node.watch_log_for("initialization completed")
        if "tablets" not in self.scylla_features:
            repair_re = rf"repair .+ keyspace=ks, .+ peers={self.PEERS_RE}, live_peers"
            matchings = new_node.grep_log(repair_re)
            assert matchings
            for matches in matchings:
                peers = get_peers(matches[1].group("peers"))
                assert set(peers) == set([node2_1.address()]) or set(peers) == set([node2_1.hostid()]), "New node should fetched data only from the same dc"
        else:
            expected_nodes = [node1_1, node1_2, node2_1]

            repair_re = rf"stream_blob - stream_sstables.*Finished sending"
            for node in expected_nodes:
                matchings = node.grep_log(repair_re)
                if node == node2_1:
                    assert matchings, "New node expected to fetch data from a node from the same dc"
                else:
                    assert not matchings, "New node expected to NOT fetch data from a node from a different dc"

    @pytest.mark.skip_if(with_feature("tablets"))
    def test_postpone_reshape_sstables_created_by_repair(self):
        """
        This test covers https://github.com/scylladb/scylla/commit/b6828e899ae214d8571464ec121f237069c1c4f1 commit.
        Since 5.0
        Before this commit reshaping was run synchronous, meaning that node would only become online once all reshape
        activity completed.
        Using of off-strategy means that reshape runs asynchronous and node will be available faster.

        The test scenario:
        - delete SSTables on first node
        - run repair on second node
        - wait for repair is finished - new SSTables are recreated on the fist node by repair
        - re-start first node
        - validate that reshaping was started by off-strategy mechanism
        - validate rows count
        """
        cluster = self.cluster
        cluster.populate(2).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = cluster.nodelist()
        keyspace = "ks"
        table = "cf"
        with self.patient_cql_connection(node1) as session:
            create_ks(session, keyspace, 2)
            create_cf(session, table, read_repair=0.0, columns={"c1": "text", "c2": "text"})

            rows = 10000
            logger.info(f"Insert {rows} rows")
            insert_c1c2(session=session, keys=range(rows), consistency=ConsistencyLevel.ONE)
            cluster.flush()

            table_folder = get_node_cf_dir(node=node1, ks_name=keyspace, cf_name=table)
            logger.info(f"Remove SSTables from '{table_folder}' folder")
            remove_files_in_folder(table_folder)

            # Restart the node1 because node1 is holding file descriptors to deleted files in data dir,
            # so it can still read from them even though they cannot be found in the directory listing
            logger.info(f"Restart {node1.name} node")
            node1.stop(wait_other_notice=True)
            node1.start(wait_for_binary_proto=True, wait_other_notice=True)

            from_mark = node2.mark_log()
            self._run_repair_and_check_completed(node=node2, ks=keyspace, cf=table, from_mark=from_mark)

            logger.info(f"Stop {node1.name} node")
            node1.stop(wait_other_notice=True)

            from_mark = node1.mark_log()
            logger.info(f"Start {node1.name} node")
            node1.start(wait_for_binary_proto=True, wait_other_notice=True)

            assert node1.grep_log(rf"Starting off-strategy compaction for {keyspace}.{table}", from_mark=from_mark), "Expected that off-strategy compaction is started, but it was not started"

            reshape_msg = node1.grep_log(rf"Reshape {keyspace}.{table}", from_mark=from_mark)
            if "tablets" in self.scylla_features:
                assert not reshape_msg, "Expected that reshape is not required with tablets, but it was started"
            else:
                assert reshape_msg, "Expected that reshape is started, but it was not started"

            assert_row_count(session=session, table_name=table, expected=rows, consistency_level=ConsistencyLevel.QUORUM)

    def test_repair_compacts_data(self):
        cluster = self.cluster
        cluster.set_configuration_options({"hinted_handoff_enabled": "false"})
        cluster.populate(generate_cluster_topology(dc_num=1, rack_num=2, nodes_per_rack=1)).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        keyspace = "ks"
        table = "tbl"
        create_ks(session, keyspace, 2)
        session.execute(f"CREATE TABLE {keyspace}.{table} (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH compaction = {{'class': 'NullCompactionStrategy'}}")

        def write_data_to_node1(pk):
            node2.stop(wait_other_notice=True)

            for ck in range(10):
                session.execute(f"INSERT INTO {keyspace}.{table} (pk, ck, v) VALUES ({pk}, {ck}, 0)")

            node1.flush()

            session.execute(f"DELETE FROM {keyspace}.{table} WHERE pk = {pk}")

            node1.flush()

            node2.start(wait_other_notice=True)

        write_data_to_node1(0)

        node1_base_metrics = get_node_metrics(node_ip=self.cluster.get_node_ip(1), metrics=self.LIST_ROW_LEVEL_REPAIR_METRICS)
        node1.repair(keyspace=keyspace, tables=[table])
        self.verify_repair_tx_rx_rows(
            node_idx=1,
            # with compaction, a single row (the tombstone) is sent over
            expected_tx_row_nr=node1_base_metrics["tx_row_nr"] + 1,
            expected_rx_row_nr=node1_base_metrics["rx_row_nr"],
            list_metrics=self.LIST_ROW_LEVEL_REPAIR_METRICS,
        )

        write_data_to_node1(1)

        # Disable compaction on repair
        session.execute("UPDATE system.config SET value = '0' WHERE name = 'enable_compacting_data_for_streaming_and_repair'")

        node1_base_metrics = get_node_metrics(node_ip=self.cluster.get_node_ip(1), metrics=self.LIST_ROW_LEVEL_REPAIR_METRICS)
        node1.repair(keyspace=keyspace, tables=[table])
        self.verify_repair_tx_rx_rows(
            node_idx=1,
            # without compaction, all rows are sent over
            expected_tx_row_nr=node1_base_metrics["tx_row_nr"] + 21,  # 2 * 10 rows + 1 partition tombstone
            expected_rx_row_nr=node1_base_metrics["rx_row_nr"],
            list_metrics=self.LIST_ROW_LEVEL_REPAIR_METRICS,
        )

    @pytest.mark.skip_if(with_feature("tablets"))
    def test_repair_one_node(self):
        """
        Test that repairing a single node picks up all tokens that belong to it,
        whether the node is their primary owner or just a replica.
        1. Create a cluster with RF=3
        2. Write data on single nodes at a time by stopping other node(s)
        3. Start all nodes
        4. Repair one of the nodes
        5. Verify the data exclusively on all node
        """
        cluster = self.cluster
        cluster.set_configuration_options({"hinted_handoff_enabled": "false"})
        num_nodes = 3
        cluster.populate(num_nodes).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1 = cluster.nodelist()[0]
        keyspace = "ks"
        table = "tbl"
        dc = node1.get_datacenter_name()

        logger.debug(f"Create {keyspace}.{table} with rf={num_nodes}")
        with self.patient_cql_connection(node1) as session:
            session.execute(f"CREATE KEYSPACE {keyspace} WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{dc}' : {num_nodes} }};")
            session.execute(f"CREATE TABLE {keyspace}.{table} (pk int PRIMARY KEY, v int) WITH compaction = {{'class': 'NullCompactionStrategy'}} AND speculative_retry = 'NONE'")

        keys_per_node = 100
        num_keys = keys_per_node * num_nodes
        expected_values = []
        for i in range(num_nodes):
            node = cluster.nodelist()[i]
            logger.debug(f"Insert {num_keys // num_nodes} keys on {node.name}")
            nodes_to_stop = [n for n in cluster.nodelist() if n != node]
            cluster.stop_nodes(nodes_to_stop, wait_other_notice=True)
            with self.patient_exclusive_cql_connection(node, keyspace) as session1:
                insert_query = session1.prepare(f"INSERT INTO {keyspace}.{table} (pk, v) VALUES (?, ?)")
                expected_values.append([(pk, pk) for pk in range(i, num_keys, num_nodes)])
                for k, v in expected_values[i]:
                    session1.execute(insert_query, (k, v))
            cluster.start_nodes(nodes_to_stop, wait_for_binary_proto=True, wait_other_notice=True)

        for i in range(num_nodes):
            node = cluster.nodelist()[i]
            logger.debug(f"Verify partial data exclusively on {node.name}")
            nodes_to_stop = [n for n in cluster.nodelist() if n != node]
            cluster.stop_nodes(nodes_to_stop, wait_other_notice=True)
            query = SimpleStatement(f"SELECT * from {keyspace}.{table}", consistency_level=ConsistencyLevel.ONE)
            with self.patient_exclusive_cql_connection(node, keyspace) as session1:
                res = session1.execute(query)
            values = [(r.pk, r.v) for r in sorted(res)]
            assert values == expected_values[i]
            cluster.start_nodes(nodes_to_stop, wait_for_binary_proto=True, wait_other_notice=True)

        node = random.choice(cluster.nodelist())
        logger.debug(f"Repair only {node.name}")
        node.nodetool("repair")

        for node in cluster.nodelist():
            logger.debug(f"Verify data exclusively on {node.name}")
            nodes_to_stop = [n for n in cluster.nodelist() if n != node]
            cluster.stop_nodes(nodes_to_stop, wait_other_notice=True)
            query = SimpleStatement(f"SELECT * from {keyspace}.{table}", consistency_level=ConsistencyLevel.ONE)
            with self.patient_exclusive_cql_connection(node, keyspace) as session1:
                res = session1.execute(query)
            values = [(r.pk, r.v) for r in sorted(res)]
            expected = [(i, i) for i in range(num_keys)]
            assert values == expected
            cluster.start_nodes(nodes_to_stop, wait_for_binary_proto=True, wait_other_notice=True)

    def test_repair_one_node_alter_rf(self):  # noqa: PLR0915
        """
        Test that repairing a single node picks up all tokens that belong to it,
        whether the node is their primary owner or just a replica.
        1. Create a cluster with RF=1
        2. Write data
        3. Start all nodes
        4. Repair one of the nodes
        5. Verify the data exclusively on all node
        """
        cluster = self.cluster
        cluster.set_configuration_options({"hinted_handoff_enabled": "false"})

        # The test modifies the replication factor of the keyspace from 1 to 3.
        # With the current limitations, we can only change the value of the replication
        # factor by 1. That would require us to go 1 -> 2 -> 3. No matter what we do,
        # we'll try to produce an RF-rack-invalid keyspace, so we need to disable
        # the option for now.
        #
        # This can be removed after scylladb/scylladb#23525 is merged.
        cluster.set_configuration_options({"rf_rack_valid_keyspaces": "false"})

        # The load balancer can migrate tablets while checking PK count, causing data in migrated tablets
        # to be counted twice or not at all. Force capacity based balancing to avoid this.
        cluster.set_configuration_options({"force_capacity_based_balancing": "true"})

        num_nodes = 3
        cluster.populate({"dc1": {"r1": 2, "r2": 1}}).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1 = cluster.nodelist()[0]
        keyspace = "ks"
        table = "tbl"
        dc = node1.get_datacenter_name()
        keys_per_node = 100
        num_keys = keys_per_node * num_nodes

        logger.debug(f"Create {keyspace}.{table} with rf=1")
        with self.patient_cql_connection(node1) as session:
            session.execute(f"CREATE KEYSPACE {keyspace} WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{dc}' : 1 }};")
            session.execute(f"CREATE TABLE {keyspace}.{table} (pk int PRIMARY KEY, v int) WITH compaction = {{'class': 'NullCompactionStrategy'}} AND speculative_retry = 'NONE'")

            logger.debug(f"Insert {num_keys} keys")
            insert_query = session.prepare(f"INSERT INTO {keyspace}.{table} (pk, v) VALUES (?, ?)")
            for pk in range(num_keys):
                session.execute(insert_query, (pk, pk))

        total_found = 0
        for i in range(num_nodes):
            node = cluster.nodelist()[i]
            logger.debug(f"Collect partial data exclusively on {node.name}")
            nodes_to_stop = [n for n in cluster.nodelist() if n != node]
            cluster.stop_nodes(nodes_to_stop, wait_other_notice=True)
            found = 0
            with self.patient_exclusive_cql_connection(node, keyspace) as session1:
                for pk in range(num_keys):
                    query = SimpleStatement(f"SELECT * from {keyspace}.{table} WHERE pk={pk}", consistency_level=ConsistencyLevel.ONE)
                    try:
                        session1.execute(query)
                        found += 1
                    except (NoHostAvailable, Unavailable):
                        pass
            cluster.start_nodes(nodes_to_stop, wait_for_binary_proto=True, wait_other_notice=True)
            logger.debug(f"Found {found} keys on {node.name}")
            total_found += found
        assert total_found == num_keys

        logger.debug(f"Change replication factor to rf={num_nodes}")
        # With tablets, ALTER KEYSPACE changing RF returns only after the
        # resulting tablet rebuilds finish, which can exceed the default timeout.
        with self.patient_cql_connection(node1, request_timeout=300) as session:
            # With tablets replication factor can be altered only in single steps
            rf_steps = [i for i in range(2, num_nodes + 1)] if "tablets" in self.scylla_features else [num_nodes]
            for rf in rf_steps:
                change_schema_safely(session, cluster.nodelist(), f"ALTER KEYSPACE {keyspace} WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{dc}' : {rf} }};")

        node = random.choice(cluster.nodelist())
        logger.debug(f"Repair only {node.name}")
        node.nodetool("repair")

        for node in cluster.nodelist():
            logger.debug(f"Verify data exclusively on {node.name}")
            nodes_to_stop = [n for n in cluster.nodelist() if n != node]
            cluster.stop_nodes(nodes_to_stop, wait_other_notice=True)
            query = SimpleStatement(f"SELECT * from {keyspace}.{table}", consistency_level=ConsistencyLevel.ONE)
            with self.patient_exclusive_cql_connection(node, keyspace) as session1:
                res = session1.execute(query)
            values = [(r.pk, r.v) for r in sorted(res)]
            expected = [(i, i) for i in range(num_keys)]
            assert values == expected
            cluster.start_nodes(nodes_to_stop, wait_for_binary_proto=True, wait_other_notice=True)

    @pytest.mark.skip_if(with_feature("tablets"))
    def test_repair_small_table_optimization(self):
        cluster = self.cluster
        cluster.set_configuration_options(values={"endpoint_snitch": "org.apache.cassandra.locator.GossipingPropertyFileSnitch"})
        logger.info("Starting multiple dc cluster..")
        cluster.populate({"dc1": 2, "dc2": 2, "dc3": 2})
        cluster.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]
        node2 = cluster.nodelist()[1]

        with self.patient_cql_cluster_session(node1) as session:
            create_ks(session, "test_repair", {"dc1": 2, "dc2": 2, "dc3": 2})
            create_cf(session, "cf", read_repair=0.0, columns={"c1": "text", "c2": "text"})

        config1 = {"node": node1, "rpc_calls": 0, "duration": 0.0, "small_table_optimization": False}
        config2 = {"node": node2, "rpc_calls": 0, "duration": 0.0, "small_table_optimization": True}

        for config in [config1, config2]:
            node = config["node"]
            self._run_repair_api(run_on_node=node, keyspace="test_repair", await_completion=True, small_table_optimization=config["small_table_optimization"])
            rpc_calls = self.get_repair_rpc_calls(node)
            duration = self.get_repair_duration(node)
            config["rpc_calls"] = rpc_calls
            config["duration"] = duration
            logger.info(f"Got repair for node = {node.name} rpc_calls = {rpc_calls} duration = {duration}")

        logger.info(f"Repair with small_table_optimization is {config1['duration'] / config2['duration']} times faster")

        assert config1["rpc_calls"] > 10 * config2["rpc_calls"], "Repair with small_table_optimization is supposed to have fewer rpc calls"
        assert config1["duration"] > 10 * config2["duration"], "Repair with small_table_optimization is supposed to finish faster"
