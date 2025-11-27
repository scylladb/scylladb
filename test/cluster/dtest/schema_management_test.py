#
# Copyright (C) 2015-present The Apache Software Foundation
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import functools
import logging
import string
import time
from concurrent import futures
from typing import NamedTuple

import pytest
from cassandra import AlreadyExists, ConsistencyLevel, InvalidRequest
from cassandra.cluster import ThreadPoolExecutor
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import SimpleStatement, dict_factory
from ccmlib.scylla_cluster import ScyllaCluster

from dtest_class import Tester, create_cf, create_ks, read_barrier
from tools.assertions import assert_all, assert_invalid
from tools.cluster_topology import generate_cluster_topology
from tools.data import create_c1c2_table, insert_c1c2, query_c1c2, rows_to_list

logger = logging.getLogger(__name__)

class TestSchemaManagement(Tester):
    def prepare(self, racks_num: int, has_config: bool = True):
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(rack_num=racks_num)

        if has_config:
            config = {
                "ring_delay_ms": 5000,
            }
            cluster.set_configuration_options(values=config)

        cluster.populate(cluster_topology)
        cluster.start(wait_other_notice=True)

        return cluster


    def test_prepared_statements_work_after_node_restart_after_altering_schema_without_changing_columns(self):
        cluster = self.prepare(racks_num=3)

        [node1, node2, node3] = cluster.nodelist()

        session = self.patient_cql_connection(node1)

        logger.debug("Creating schema...")
        create_ks(session, "ks", 3)
        session.execute(
            """
            CREATE TABLE users (
                id int,
                firstname text,
                lastname text,
                PRIMARY KEY (id)
             );
         """
        )

        insert_statement = session.prepare("INSERT INTO users (id, firstname, lastname) VALUES (?, 'A', 'B')")
        insert_statement.consistency_level = ConsistencyLevel.ALL
        session.execute(insert_statement, [0])

        logger.debug("Altering schema")
        session.execute("ALTER TABLE users WITH comment = 'updated'")

        logger.debug("Restarting node2")
        node2.stop(gently=True)
        node2.start(wait_for_binary_proto=True)

        logger.debug("Restarting node3")
        node3.stop(gently=True)
        node3.start(wait_for_binary_proto=True, wait_other_notice=True)

        n_partitions = 20
        for i in range(n_partitions):
            session.execute(insert_statement, [i])

        rows = session.execute("SELECT * FROM users")
        res = sorted(rows)
        assert len(res) == n_partitions
        for i in range(n_partitions):
            expected = [i, "A", "B"]
            assert list(res[i]) == expected, f"Expected {expected}, got {res[i]}"

    def test_dropping_keyspace_with_many_columns(self):
        """
        Exploits https://github.com/scylladb/scylla/issues/1484
        """
        cluster = self.prepare(racks_num=1, has_config=False)

        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)

        session.execute("CREATE KEYSPACE testxyz WITH replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }")
        for i in range(8):
            session.execute(f"CREATE TABLE testxyz.test_{i} (k int, c int, PRIMARY KEY (k),)")
        session.execute("drop keyspace testxyz")

        for node in cluster.nodelist():
            s = self.patient_cql_connection(node)
            s.execute("CREATE KEYSPACE testxyz WITH replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }")
            s.execute("drop keyspace testxyz")

    def test_multiple_create_table_in_parallel(self):
        """
        Run multiple create table statements via different nodes
        1. Create a cluster of 3 nodes
        2. Run create table with different table names in parallel - check all complete
        3. Run create table with the same table name in parallel - check if they complete
        """
        logger.debug("1. Create a cluster of 3 nodes")
        nodes_count = 3
        cluster = self.prepare(racks_num=nodes_count)
        sessions = [self.patient_exclusive_cql_connection(node) for node in cluster.nodelist()]
        ks = "ks"
        create_ks(sessions[0], ks, nodes_count)

        def create_table(session, table_name):
            create_statement = f"CREATE TABLE {ks}.{table_name} (p int PRIMARY KEY, c0 text, c1 text, c2 text, c3 text, c4 text, c5 text, c6 text, c7 text, c8 text, c9 text);"
            logger.debug(f"create_statement {create_statement}")
            session.execute(create_statement)

        logger.debug("2. Run create table with different table names in parallel - check all complete")
        step2_tables = [f"t{i}" for i in range(nodes_count)]
        with ThreadPoolExecutor(max_workers=nodes_count) as executor:
            list(executor.map(create_table, sessions, step2_tables))

        for table in step2_tables:
            sessions[0].execute(SimpleStatement(f"INSERT INTO {ks}.{table} (p) VALUES (1)", consistency_level=ConsistencyLevel.ALL))
            rows = sessions[0].execute(SimpleStatement(f"SELECT * FROM {ks}.{table}", consistency_level=ConsistencyLevel.ALL))
            assert len(rows_to_list(rows)) == 1, f"Expected 1 row but got rows:{rows} instead"

        logger.debug("3. Run create table with the same table name in parallel - check if they complete")
        step3_table = "test"
        step3_tables = [step3_table for i in range(nodes_count)]
        with ThreadPoolExecutor(max_workers=nodes_count) as executor:
            res_futures = [executor.submit(create_table, *args) for args in zip(sessions, step3_tables)]
            for res_future in res_futures:
                try:
                    res_future.result()
                except AlreadyExists as e:
                    logger.info(f"expected cassandra.AlreadyExists error {e}")

        sessions[0].execute(SimpleStatement(f"INSERT INTO {ks}.{step3_table} (p) VALUES (1)", consistency_level=ConsistencyLevel.ALL))
        sessions[0].execute(f"SELECT * FROM {ks}.{step3_table}")
        rows = sessions[0].execute(SimpleStatement(f"SELECT * FROM {ks}.{step3_table}", consistency_level=ConsistencyLevel.ALL))
        assert len(rows_to_list(rows)) == 1, f"Expected 1 row but got rows:{rows} instead"

    @pytest.mark.parametrize("case", ("write", "read", "read_and_write"))
    def test_alter_table_in_parallel_to_read_and_write(self, case):
        """
        Create a table and write into while altering the table
        1. Create a cluster of 3 nodes and populate a table
        2. Run write/read/read_and_write" statement in a loop
        3. Alter table while inserts are running
        """
        logger.debug("1. Create a cluster of 3 nodes and populate a table")
        cluster = self.prepare(racks_num=3)
        col_number = 20

        [node1, node2, node3] = cluster.nodelist()
        session = self.patient_exclusive_cql_connection(node1)

        def alter_table():
            alter_statement = f'ALTER TABLE keyspace1.standard1 DROP ("C{col_number - 1}", "C{col_number - 2}")'
            logger.debug(f"alter_statement {alter_statement}")
            return session.execute(alter_statement)

        def cs_run(stress_type, col=col_number - 2):
            node2.stress_object([stress_type, "n=10000", "cl=QUORUM", "-schema", "replication(factor=3)", "-col", f"n=FIXED({col})", "-rate", "threads=1"])

        logger.debug("Populate")
        cs_run("write", col_number)

        case_map = {
            "read": functools.partial(cs_run, "read"),
            "write": functools.partial(cs_run, "write"),
            "read_and_write": functools.partial(cs_run, "mixed"),
        }
        with ThreadPoolExecutor(max_workers=5) as executor:
            logger.debug(f"2. Run {case} statement in a loop")
            statement_future = executor.submit(case_map[case])

            logger.debug(f"let's {case} statement work some time")
            time.sleep(2)

            logger.debug("3. Alter table while inserts are running")
            alter_result = alter_table()
            logger.debug(alter_result.all())

            logger.debug(f"wait till {case} statement finished")
            statement_future.result()

        rows = session.execute(SimpleStatement("SELECT * FROM keyspace1.standard1 LIMIT 1;", consistency_level=ConsistencyLevel.ALL))
        assert len(rows_to_list(rows)[0]) == col_number - 1, f"Expected {col_number - 1} columns but got rows:{rows} instead"

        logger.debug("reade and check data")
        cs_run("read")

    @pytest.mark.skip("unimplemented")
    def commitlog_replays_after_schema_change(self):
        """
        Commitlog can be replayed even though schema has been changed
        1. Create a table and insert data
        2. Alter table
        3. Kill node
        4. Boot node and verify that commitlog have been replayed and that all data is restored
        """
        raise NotImplementedError

    @pytest.mark.parametrize("case", ("create_table", "alter_table", "drop_table"))
    def test_update_schema_while_node_is_killed(self, case):
        """
        Check that a node that is killed durring a table creation/alter/drop is able to rejoin and to synch on schema
        """

        logger.debug("1. Create a cluster and insert data")
        cluster = self.prepare(racks_num=3)

        [node1, node2, node3] = cluster.nodelist()

        session = self.patient_cql_connection(node1)

        def create_table_case():
            try:
                logger.debug("Creating table")
                create_c1c2_table(session)
                logger.debug("Populating")
                insert_c1c2(session, n=10)
            except AlreadyExists:
                # the CQL command can be called multiple time case of retries
                pass

        def alter_table_case():
            try:
                session.execute("ALTER TABLE ks.cf ADD (c3 text);", timeout=180)
            except InvalidRequest as exc:
                # the CQL command can be called multiple time case of retries
                assert "Invalid column name c3" in str(exc)

        def drop_table_case():
            try:
                session.execute("DROP TABLE cf;", timeout=180)
            except InvalidRequest as exc:
                # the CQL command can be called multiple time case of retries
                assert "Cannot drop non existing table" in str(exc)

        logger.debug("Creating keyspace")
        create_ks(session, "ks", 3)
        if case != "create_table":
            create_table_case()

        case_map = {
            "create_table": create_table_case,
            "alter_table": alter_table_case,
            "drop_table": drop_table_case,
        }
        with ThreadPoolExecutor(max_workers=2) as executor:
            logger.debug(f"2. kill node during {case}")
            kill_node_future = executor.submit(node2.stop, gently=False, wait_other_notice=True)
            case_map[case]()
            kill_node_future.result()

        logger.debug("3. Start the stopped node2")
        node2.start(wait_for_binary_proto=True)

        session = self.patient_exclusive_cql_connection(node2)
        read_barrier(session)

        def create_or_alter_table_expected_result(col_mun):
            rows = session.execute(SimpleStatement("SELECT * FROM ks.cf LIMIT 1;", consistency_level=ConsistencyLevel.QUORUM))
            assert len(rows_to_list(rows)[0]) == col_mun, f"Expected {col_mun} columns but got rows:{rows} instead"
            for key in range(10):
                query_c1c2(session=session, key=key, consistency=ConsistencyLevel.QUORUM)

        expected_case_result_map = {
            "create_table": functools.partial(create_or_alter_table_expected_result, 3),
            "alter_table": functools.partial(create_or_alter_table_expected_result, 4),
            "drop_table": functools.partial(assert_invalid, session, "SELECT * FROM test1"),
        }
        logger.debug("verify that commitlog has been replayed and that all data is restored")
        expected_case_result_map[case]()

    @pytest.mark.parametrize("is_gently_stop", [True, False])
    def test_nodes_rejoining_a_cluster_synch_on_schema(self, is_gently_stop):
        """
        Nodes rejoining the cluster synch on schema changes
        1. Create a cluster and insert data
        2. Stop a node
        3. Alter table
        4. Insert additional data
        5. Start the stopped node
        6. Verify the stopped node synchs on the updated schema
        """

        logger.debug("1. Create a cluster and insert data")
        cluster = self.prepare(racks_num=3)

        [node1, node2, node3] = cluster.nodelist()

        session = self.patient_cql_connection(node1)

        logger.debug("Creating schema")
        create_ks(session, "ks", 3)
        create_c1c2_table(session)
        create_cf(session, "cf", key_name="p", key_type="int", columns={"v": "text"})

        logger.debug("Populating")
        insert_c1c2(session, n=10, consistency=ConsistencyLevel.ALL)

        logger.debug("2 Stop a node1")
        node1.stop(gently=is_gently_stop, wait_other_notice=True)

        logger.debug("3 Alter table")
        session = self.patient_cql_connection(node2)
        session.execute("ALTER TABLE ks.cf ADD (c3 text);", timeout=180)

        logger.debug("4 Insert additional data")
        session.execute(SimpleStatement("INSERT INTO ks.cf (key, c1, c2, c3) VALUES ('test', 'test', 'test', 'test')", consistency_level=ConsistencyLevel.QUORUM))

        logger.debug("5. Start the stopped node1")
        node1.start(wait_for_binary_proto=True)

        logger.debug("6. Verify the stopped node synchs on the updated schema")
        session = self.patient_exclusive_cql_connection(node1)
        read_barrier(session)

        rows = session.execute(SimpleStatement("SELECT * FROM ks.cf WHERE key='test'", consistency_level=ConsistencyLevel.ALL))
        expected = [["test", "test", "test", "test"]]
        assert rows_to_list(rows) == expected, f"Expected {expected} but got {rows} instead"
        for key in range(10):
            query_c1c2(session=session, key=key, consistency=ConsistencyLevel.ALL)

    def test_reads_schema_recreated_while_node_down(self):
        cluster = self.prepare(racks_num=3)

        [node1, node2, node3] = cluster.nodelist()

        session = self.patient_cql_connection(node1)

        logger.debug("Creating schema")
        create_ks(session, "ks", 3)
        session.execute("CREATE TABLE cf (p int PRIMARY KEY, v text);")

        logger.debug("Populating")
        session.execute(SimpleStatement("INSERT INTO cf (p, v) VALUES (1, '1')", consistency_level=ConsistencyLevel.ALL))

        logger.debug("Stopping node2")
        node2.stop(gently=True)

        logger.debug("Re-creating schema")
        session.execute("DROP TABLE cf;")
        session.execute("CREATE TABLE cf (p int PRIMARY KEY, v1 bigint, v2 text);")

        logger.debug("Restarting node2")
        node2.start(wait_for_binary_proto=True)
        session2 = self.patient_cql_connection(node2)
        read_barrier(session2)

        rows = session.execute(SimpleStatement("SELECT * FROM cf", consistency_level=ConsistencyLevel.ALL))
        assert rows_to_list(rows) == [], f"Expected an empty result set, got {rows}"

    def test_writes_schema_recreated_while_node_down(self):
        cluster = self.prepare(racks_num=3)

        [node1, node2, node3] = cluster.nodelist()

        session = self.patient_cql_connection(node1)

        logger.debug("Creating schema")
        create_ks(session, "ks", 3)
        session.execute("CREATE TABLE cf (p int PRIMARY KEY, v text);")

        logger.debug("Populating")
        session.execute(SimpleStatement("INSERT INTO cf (p, v) VALUES (1, '1')", consistency_level=ConsistencyLevel.ALL))

        logger.debug("Stopping node2")
        node2.stop(gently=True, wait_other_notice=True)

        logger.debug("Re-creating schema")
        session.execute("DROP TABLE cf;")
        session.execute("CREATE TABLE cf (p int PRIMARY KEY, v text);")

        logger.debug("Restarting node2")
        node2.start(wait_for_binary_proto=True)
        session2 = self.patient_cql_connection(node2)
        read_barrier(session2)

        session.execute(SimpleStatement("INSERT INTO cf (p, v) VALUES (2, '2')", consistency_level=ConsistencyLevel.ALL))

        rows = session.execute(SimpleStatement("SELECT * FROM cf", consistency_level=ConsistencyLevel.ALL))
        expected = [[2, "2"]]
        assert rows_to_list(rows) == expected, f"Expected {expected}, got {rows_to_list(rows)}"


class TestLargePartitionAlterSchema(Tester):
    # Issue scylladb/scylla: #5135:
    #
    # Issue: Cache reads may miss some writes if schema alter followed by a read happened concurrently with preempted
    # partition entry update
    # Affects only tables with multi-row partitions, which are the only ones that can experience the update of partition
    # entry being preempted.
    #
    # The scenario in which the problem could have happened has to involve:
    # - a large partition with many rows, large enough for preemption (every 0.5ms) to happen during the scan of the partition.
    # - appending writes to the partition (not overwrites)
    # - scans of the partition
    # - schema alter of that table. The issue is exposed only by adding or dropping a column, such that the added/dropped
    #   column lands in the middle (in alphabetical order) of the old column set.
    #
    # Memtable flush has to happen after a schema alter concurrently with a read.
    #
    # The bug could result in cache corruption which manifests as some past writes being missing (not visible to reads).

    PARTITIONS = 50
    STRING_VALUE = string.ascii_lowercase

    def prepare(self, cluster_topology: dict[str, dict[str, int]], rf: int):
        if not self.cluster.nodelist():
            self.cluster.populate(cluster_topology)
            self.cluster.start(wait_other_notice=True)

        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node=node1)
        self.create_schema(session=session, rf=rf)

        return session

    def create_schema(self, session, rf):
        logger.debug("Creating schema")
        create_ks(session=session, name="ks", rf=rf)

        session.execute(
            """
            CREATE TABLE lp_table (
                pk int,
                ck1 int,
                val1 text,
                val2 text,
                PRIMARY KEY (pk, ck1)
            );
        """
        )

    def populate(self, session, data, ck_start, ck_end):
        logger.debug(f"Start populate DB: {self.PARTITIONS} partitions with {ck_end - ck_start} records in each partition")

        ck_rows = [ck_start, ck_end]

        stmt = session.prepare("INSERT INTO lp_table (pk, ck1, val1, val2) VALUES (?, ?, ?, ?)")

        for pk in range(self.PARTITIONS):
            for ck in range(ck_rows[0], ck_rows[1]):
                data.append([pk, ck, self.STRING_VALUE, self.STRING_VALUE])

        execute_concurrent_with_args(session=session, statement=stmt, parameters=data)
        logger.debug(f"Finish populate DB: {self.PARTITIONS} partitions with {ck_end - ck_start} records in each partition")
        return data

    def read(self, session, ck_max):
        logger.debug(f"Start reading..")

        for _ in range(2):
            for pk in range(self.PARTITIONS):
                for ck in range(ck_max):
                    session.execute(f"select * from lp_table where pk = {pk} and ck1 = {ck}")

        logger.debug(f"Finish reading..")

    def add_column(self, session, column_name, column_type):
        logger.debug(f"Add {column_name} column")
        session.execute(f"ALTER TABLE lp_table ADD {column_name} {column_type}")

    def drop_column(self, session, column_name):
        logger.debug(f"Drop {column_name} column")
        session.execute(f"ALTER TABLE lp_table DROP {column_name}")

    def test_large_partition_with_add_column(self):
        cluster_topology = generate_cluster_topology()
        session = self.prepare(cluster_topology, rf=1)
        data = self.populate(session=session, data=[], ck_start=0, ck_end=10)

        threads = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            # Insert new rows in background
            threads.append(executor.submit(self.populate, session=session, data=data, ck_start=10, ck_end=1500))
            threads.append(executor.submit(self.read, session=session, ck_max=1500))
            # Wait for running load
            time.sleep(10)
            self.add_column(session, "new_clmn", "int")

            # Memtable flush has to happen after a schema alter concurrently with a read
            logger.debug("Flush data")
            self.cluster.nodelist()[0].flush()

            for future in futures.as_completed(threads, timeout=300):
                try:
                    future.result()
                except Exception as exc:  # noqa: BLE001
                    pytest.fail(f"Generated an exception: {exc}")

        for i, _ in enumerate(data):
            data[i].append(None)

        assert_all(session, f"select pk, ck1, val1, val2, new_clmn from lp_table", data, ignore_order=True, print_result_on_failure=False)

    def test_large_partition_with_drop_column(self):
        cluster_topology = generate_cluster_topology()
        session = self.prepare(cluster_topology, rf=1)
        data = self.populate(session=session, data=[], ck_start=0, ck_end=10)

        threads = []
        timeout = 300
        ck_end = 1500
        if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode == "debug":
            timeout = 900
            ck_end = 150
        with ThreadPoolExecutor(max_workers=5) as executor:
            # Insert new rows in background
            threads.append(executor.submit(self.populate, session=session, data=data, ck_start=10, ck_end=ck_end))
            threads.append(executor.submit(self.read, session=session, ck_max=ck_end))
            # Wait for running load
            time.sleep(10)
            self.drop_column(session=session, column_name="val1")

            # Memtable flush has to happen after a schema alter concurrently with a read
            logger.debug("Flush data")
            self.cluster.nodelist()[0].flush()

            result = []
            for future in futures.as_completed(threads, timeout=timeout):
                try:
                    result.append(future.result())
                except Exception as exc:  # noqa: BLE001
                    # "Unknown identifier val1" is expected error
                    if not len(exc.args) or "Unknown identifier val1" not in exc.args[0]:
                        pytest.fail(f"Generated an exception: {exc}")


class HistoryVerifier:
    def __init__(self, table_name="table1", keyspace_name="lwt_load_ks"):
        """
        Initialize parameters for further verification of schema history.
        :param table_name: table thats we change it's schema and verify schema history accordingly.
        """

        self.table_name = table_name
        self.keyspace_name = keyspace_name
        self.versions = []
        self.versions_dict = {}
        self.query = ""

    def verify(self, session, expected_current_diff, expected_prev_diff, query):
        """
        Verify current schema history entry by comparing to previous schema entry.
        :param session: python cql session
        :param expected_current_diff: difference of current schema from previous schema
        :param expected_prev_diff: difference of previous schema from current schema
        :param query: The query that created new schema
        """

        def get_table_id(session, keyspace_name, table_name):
            assert keyspace_name, f"Input kesyspcase should have value, keyspace_name={keyspace_name}"
            assert table_name, f"Input table_name should have value, table_name={table_name}"
            query = "select keyspace_name,table_name,id from system_schema.tables"
            query += f" WHERE keyspace_name='{keyspace_name}' AND table_name='{table_name}'"
            current_rows = session.execute(query).current_rows
            assert len(current_rows) == 1, f"Not found table description, ks={keyspace_name} table_name={table_name}"
            res = current_rows[0]
            return res["id"]

        def read_schema_history_table(session, cf_id):
            """
            read system.scylla_table_schema_history and verify current version diff from previous vesion
            :param session: python cql session
            :param cf_id: uuid of the table we changed it's schema
            """

            query = f"select * from system.scylla_table_schema_history WHERE cf_id={cf_id}"
            res = session.execute(query).current_rows
            new_versions = list(set(filter(lambda uuid: str(uuid) not in self.versions, map(lambda entry: entry["schema_version"], res))))
            msg = f"Expect 1, got len(new_versions)={len(new_versions)}"
            assert len(new_versions) == 1, msg
            current_version = str(new_versions[0])
            logger.debug(f"New schema_version {current_version} after executing '{self.query}'")
            regular_entries = filter(lambda entry: entry["kind"] == "regular" and current_version == str(entry["schema_version"]), res)
            columns_list = map(lambda entry: {"column_name": entry["column_name"], "type": entry["type"]}, regular_entries)
            self.versions_dict[current_version] = {}
            for item in columns_list:
                self.versions_dict[current_version][item["column_name"]] = item["type"]

            self.versions.append(current_version)
            if len(self.versions) > 1:
                current_id = self.versions[-1]
                previous_id = self.versions[-2]
                set_current = set(self.versions_dict[current_id].items())
                set_previous = set(self.versions_dict[previous_id].items())
                current_diff = set_current - set_previous
                previous_diff = set_previous - set_current
                msg1 = f"Expect diff(new schema,old schema) to be {expected_current_diff} got {current_diff}"
                msg2 = f" query is '{self.query}' versions={current_id},{previous_id}"
                if current_diff != expected_current_diff:
                    logger.debug(msg1 + msg2)
                assert current_diff == expected_current_diff, msg1 + msg2
                msg1 = f"Expect diff(old schema,new schema) to be {expected_prev_diff} got {previous_diff}"
                assert previous_diff == expected_prev_diff, msg1 + msg2

        self.query = query
        cf_id = get_table_id(session, keyspace_name=self.keyspace_name, table_name=self.table_name)
        read_schema_history_table(session, cf_id)


class DDL(NamedTuple):
    ddl_command: str
    expected_current_diff: set | None
    expected_prev_diff: set | None


class TestSchemaHistory(Tester):
    def prepare(self):
        cluster = self.cluster
        # in case support tablets and rf-rack-valid-keyspaces
        # create cluster with 3 racks with 1 node in each rack
        cluster_topology = generate_cluster_topology(rack_num=3)
        rf = 3
        cluster.populate(cluster_topology).start(wait_other_notice=True)
        self.session = self.patient_cql_connection(self.cluster.nodelist()[0], row_factory=dict_factory)
        create_ks(self.session, "lwt_load_ks", rf)

    def test_schema_history_alter_table(self):
        """test schema history changes following alter table cql commands"""
        self.prepare()
        verifier = HistoryVerifier(table_name="table2")
        queries_and_expected_diffs = [
            DDL(ddl_command="CREATE TABLE IF NOT EXISTS lwt_load_ks.table2 (pk int PRIMARY KEY, v int, int_col int)", expected_current_diff=None, expected_prev_diff=None),
            DDL(ddl_command="ALTER TABLE lwt_load_ks.table2 ALTER v TYPE varint", expected_current_diff={("v", "varint")}, expected_prev_diff={("v", "int")}),
            DDL(ddl_command="ALTER TABLE lwt_load_ks.table2 ADD (v2 int, v3 int)", expected_current_diff={("v2", "int"), ("v3", "int")}, expected_prev_diff=set()),
            DDL(ddl_command="ALTER TABLE lwt_load_ks.table2 ALTER int_col TYPE varint", expected_current_diff={("int_col", "varint")}, expected_prev_diff={("int_col", "int")}),
            DDL(ddl_command="ALTER TABLE lwt_load_ks.table2 DROP int_col", expected_current_diff=set(), expected_prev_diff={("int_col", "varint")}),
            DDL(ddl_command="ALTER TABLE lwt_load_ks.table2 ADD int_col bigint", expected_current_diff={("int_col", "bigint")}, expected_prev_diff=set()),
            DDL(ddl_command="ALTER TABLE lwt_load_ks.table2 DROP (int_col,v)", expected_current_diff=set(), expected_prev_diff={("int_col", "bigint"), ("v", "varint")}),
        ]
        for ddl in queries_and_expected_diffs:
            self.session.execute(ddl.ddl_command)
            verifier.verify(self.session, ddl.expected_current_diff, ddl.expected_prev_diff, query=ddl.ddl_command)
