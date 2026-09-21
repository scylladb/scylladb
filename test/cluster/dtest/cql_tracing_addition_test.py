import logging
import re
from random import choice, randint
from uuid import uuid4

import pytest
from cassandra.cluster import Session
from cassandra.concurrent import execute_concurrent_with_args
from ccmlib.scylla_node import ScyllaNode

from dtest_class import Tester, create_cf, create_ks
from tools.assertions import PytestRegex
from tools.cluster_topology import generate_cluster_topology
from tools.data import rows_to_list
from tools.files import get_node_cf_dir, get_sstables_files
from tools.misc import set_trace_probability

logger = logging.getLogger(__name__)


class TracingReadAccessHelper:
    def prepare_cluster(  # noqa: PLR0913
        self,
        nodes=1,
        rf=None,
        disable_cache=True,
        compaction=None,
        create_index=False,
        create_mv=False,
    ):
        """Create, start cluster, create ks, cf

        create cf with column name:type:
            key (pk): uuid
            name : text
            rate : int

        :param nodes: [description], defaults to 1
        :type nodes: number, optional
        :param disable_cache: run cluster without cache
        :type disable_cache: boolean
        :returns: opened session
        :rtype: {Session}
        """
        if disable_cache:
            self.cluster.set_configuration_options(values={"enable_cache": "false"})
        self.cluster.populate(nodes).start(wait_for_binary_proto=True)
        node = self.cluster.nodelist()[0]  # type: ScyllaNode
        session = self.patient_cql_connection(node)  # type: Session
        if not rf:
            rf = nodes
        create_ks(session, self.keyspace, rf)
        create_cf(session, self.table, key_type="text", columns={"name": "text", "rate": "int"}, compaction=compaction)
        if create_index:
            session.execute(f"CREATE INDEX ON {self.keyspace}.{self.table} (rate)")
        if create_mv:
            session.execute(
                f"CREATE MATERIALIZED VIEW {self.table}_by_rate AS \
                             SELECT * FROM {self.table} WHERE rate IS NOT NULL AND key IS NOT NULL \
                             PRIMARY KEY (rate, key)"
            )
        return session

    def restart_node(self, node):
        """restart node
        :param node: Node to restart
        :type node: ScyllaNode
        """
        node.stop()
        node.start(wait_for_binary_proto=True)

    def insertinto_table(self, session, rows=1):
        """Fill table with data

        use cf with columns created in self.prepare_cluster
        :param session: Cql session
        :type session: Session
        :param rows: number of rows, defaults to 1
        :type rows: number, optional
        """
        statement = session.prepare(
            f"INSERT INTO {self.keyspace}.{self.table} (key, name, rate) \
                                    VALUES (?, ?, ?)"
        )
        execute_concurrent_with_args(session, statement, map(lambda x, y, z: [x, y, z], [f"{uuid4()}" for _ in range(rows)], [f"lastname_{randint(1, 1000)}" for _ in range(rows)], [randint(1, 100) for _ in range(rows)]))

    def update_table(self, session, rows=1):
        """Fill table with data

        use cf with columns created in self.prepare_cluster
        :param session: Cql session
        :type session: Session
        :param rows: number of rows, defaults to 1
        :type rows: number, optional
        """

        result = session.execute(f"SELECT key FROM {self.table}")
        keys = rows_to_list(result)

        statement = session.prepare(
            f"INSERT INTO {self.keyspace}.{self.table} (key, name, rate) \
                                    VALUES (?, ?, ?)"
        )
        execute_concurrent_with_args(session, statement, map(lambda x, y, z: [x, y, z], [key[0] for key in keys[:rows]], [f"lastname_{randint(1, 1000)}" for _ in range(rows)], [randint(1, 100) for _ in range(rows)]))

    def select_all_with_tracing(self, node):
        output, _err = node.run_cqlsh(
            f"TRACING ON; \
                                     SELECT * \
                                     FROM {self.keyspace}.{self.table}",
            return_output=True,
            cqlsh_options=["--no-color"],
        )
        return output

    def select_single_key_with_tracing(self, node):
        session = self.patient_cql_connection(node)
        result = session.execute(f"SELECT key FROM {self.keyspace}.{self.table}")
        keys = rows_to_list(result)
        key = choice(keys)

        output, _err = node.run_cqlsh(
            f"TRACING ON; \
                                     SELECT * \
                                     FROM {self.keyspace}.{self.table} \
                                     WHERE key = '{key[0]}'",
            return_output=True,
            cqlsh_options=["--no-color"],
        )
        return output

    def select_all_from_mv_with_tracing(self, node):
        output, _err = node.run_cqlsh(
            f"TRACING ON; \
                                     SELECT * \
                                     FROM {self.keyspace}.{self.table}_by_rate",
            return_output=True,
            cqlsh_options=["--no-color"],
        )
        return output

    def select_all_by_index_with_tracing(self, node):
        output, _err = node.run_cqlsh(
            f"TRACING ON; \
                                     SELECT * \
                                     FROM {self.keyspace}.{self.table} \
                                     WHERE rate > 0 ALLOW FILTERING",
            return_output=True,
            cqlsh_options=["--no-color"],
        )
        return output

    def select_one_by_index_with_tracing(self, node):
        session = self.patient_cql_connection(node)
        result = session.execute(f"SELECT key, rate FROM {self.keyspace}.{self.table}")
        keys = rows_to_list(result)
        key = choice(keys)
        output, _err = node.run_cqlsh(
            f"TRACING ON; \
                                     SELECT * \
                                     FROM {self.keyspace}.{self.table} \
                                     WHERE rate = {key[1]} ALLOW FILTERING",
            return_output=True,
            cqlsh_options=["--no-color"],
        )
        return output

    def get_tables_list_for_node(self, node, table_name, table_type="Data"):
        """get list of table files for the node

        Scan data folder and return list of files by table_type
        for specified node
        :param node: Node to get files
        :type node: ScyllaNode
        :param table_name: Collect data for table with table_name name
        :type table_name: str
        :param table_type: file type, defaults to "Data"
        :type table_type: str, optional
        :returns: list of files with specified type
        :rtype: {list}
        """
        return get_sstables_files(get_node_cf_dir(node, self.keyspace, table_name), table_type)

    def _verify_tracing_info(self, output, node, table_name, element):
        sstables = self.get_tables_list_for_node(node, table_name, table_type="Data")
        addr = re.escape(node.address())
        for sstable in sstables:
            assert output == PytestRegex(rf"Reading {element} .* from sstable .*{sstable}.*| {addr} |")
            data_or_index = re.sub("Data", "(Data|Index)", sstable)
            assert output == PytestRegex(rf"{data_or_index}: scheduling bulk DMA read of size " rf"[\d]* at offset [\d]*.*| {addr} |")
            assert output == PytestRegex(rf"{data_or_index}: finished bulk DMA read of size " rf"[\d]* at offset [\d]*, successfully read [\d]* bytes.*| {addr} |")

    def verify_tracing_info_sstable_read_access_all_partitions(self, output, node, table_name):
        """verify tracing info in output

        Verify that result contains tracing info
        for I/O read sstables
        :param output: result of query with sstable
        :type output: str
        :param node: Node where operations run
        :type node: ScyllaNode
        """
        self._verify_tracing_info(output, node, table_name, "partition range")

    def verify_sstable_read_access_one_key(self, output, node, table_name):
        """verify tracing info in output

        Verify that result contains tracing info
        for I/O read sstables if select by one key
        :param output: result of query with sstable
        :type output: str
        :param node: Node where operations run
        :type node: ScyllaNode
        """
        self._verify_tracing_info(output, node, table_name, "key")


@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestTracingReadAccess(Tester, TracingReadAccessHelper):
    keyspace = "ks"
    table = "cf"

    @pytest.mark.single_node
    def test_tracing_info_for_all_partitions(self):
        """test tracing read access of sstable

        Testing that read from 1 sstable displayed
        correctly
        """
        session = self.prepare_cluster(nodes=1)  # type: Session
        node = self.cluster.nodelist()[0]  # type: ScyllaNode
        self.insertinto_table(session, rows=1)

        # flush memtable to sstable
        node.flush()

        # Read all data with tracing on
        out = self.select_all_with_tracing(node)

        logger.info(out)
        # Assert Reading partitions from sstable
        self.verify_tracing_info_sstable_read_access_all_partitions(out, node, self.table)

    @pytest.mark.single_node
    def test_tracing_info_for_mv(self):
        """test tracing read access of sstable

        Testing that read from 1 sstable displayed
        correctly
        """
        session = self.prepare_cluster(nodes=1, create_mv=True)  # type: Session
        node = self.cluster.nodelist()[0]  # type: ScyllaNode
        self.insertinto_table(session, rows=1)

        # flush memtable to sstable
        node.flush()

        # Read all data with tracing on
        out = self.select_all_from_mv_with_tracing(node)
        logger.info(out)
        # Assert Reading partitions from sstable
        mv_table_name = self.table + "_by_rate"
        self.verify_tracing_info_sstable_read_access_all_partitions(out, node, mv_table_name)

    @pytest.mark.single_node
    def test_tracing_info_selecting_by_one_key(self):
        """validate that tracing info if select one key"""
        session = self.prepare_cluster(nodes=1)  # type: Session
        node = self.cluster.nodelist()[0]  # type: ScyllaNode
        self.insertinto_table(session, rows=5)
        node.flush()

        out = self.select_single_key_with_tracing(node)
        # verify tracing info
        logger.info(out)
        self.verify_sstable_read_access_one_key(out, node, self.table)

    @pytest.mark.single_node
    def test_tracing_info_for_index_read_range(self):
        session = self.prepare_cluster(nodes=1, create_index=True)
        node = self.cluster.nodelist()[0]
        self.insertinto_table(session, rows=5)

        node.flush()

        out = self.select_all_by_index_with_tracing(node)
        logger.info(out)

        self.verify_tracing_info_sstable_read_access_all_partitions(out, node, self.table)

    @pytest.mark.single_node
    def test_tracing_info_for_index_read_one(self):
        session = self.prepare_cluster(nodes=1, create_index=True)
        node = self.cluster.nodelist()[0]
        self.insertinto_table(session, rows=5)

        node.flush()

        out = self.select_one_by_index_with_tracing(node)
        logger.info(out)
        self.verify_sstable_read_access_one_key(out, node, self.table)

    @pytest.mark.single_node
    def test_tracing_info_read_from_several_sstables(self):
        """test tracing I/O reads for several sstables

        switch compaction strategy to allow create several sstables.
        """
        session = self.prepare_cluster(nodes=1, compaction="{'class':'LeveledCompactionStrategy'}")
        node = self.cluster.nodelist()[0]  # type: ScyllaNode
        set_trace_probability(nodes=[node], probability_value=1.0)

        self.insertinto_table(session, rows=4)
        node.flush()
        self.insertinto_table(session, rows=4)
        node.flush()
        self.update_table(session, rows=4)
        node.flush()
        # Read all data with tracing on
        out = self.select_all_with_tracing(node)
        logger.info(out)
        # Assert Reading partitions from sstable
        self.verify_tracing_info_sstable_read_access_all_partitions(out, node, self.table)

    def test_tracing_info_sstables_locally_on_each_node_from_replica(self):
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        session = self.prepare_cluster(nodes=cluster_topology, rf=3)
        self.insertinto_table(session, rows=50)
        set_trace_probability(nodes=self.cluster.nodelist(), probability_value=1.0)
        for node in self.cluster.nodelist():
            node.flush()

        for node in self.cluster.nodelist():
            out = self.select_all_with_tracing(node)
            self.verify_tracing_info_sstable_read_access_all_partitions(out, node, self.table)

    def test_tracing_info_for_sstables_on_each_node_from_replica_with_cache_enabled(self):
        cluster_topology = generate_cluster_topology(rack_num=3)
        session = self.prepare_cluster(nodes=cluster_topology, disable_cache=False, rf=3)
        self.insertinto_table(session, rows=50)
        set_trace_probability(nodes=self.cluster.nodelist(), probability_value=1.0)
        for node in self.cluster.nodelist():
            node.flush()

        for node in self.cluster.nodelist():
            self.restart_node(node)
            out = self.select_all_with_tracing(node)
            self.verify_tracing_info_sstable_read_access_all_partitions(out, node, self.table)

    def test_tracing_info_from_remote_sstables(self):
        node1_session = self.prepare_cluster(nodes=2, rf=1)
        self.insertinto_table(node1_session, rows=10)
        node1 = self.cluster.nodelist()[0]
        node1.flush()

        node2 = self.cluster.nodelist()[1]
        out = self.select_all_with_tracing(node2)
        self.verify_tracing_info_sstable_read_access_all_partitions(out, node1, self.table)
