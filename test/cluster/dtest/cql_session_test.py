import logging
import random
import time

import pytest
from cassandra import ConsistencyLevel, ReadFailure, Unavailable
from cassandra.cluster import NoHostAvailable

from dtest_class import Tester, create_cf, create_ks
from tools.retrying import retrying

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestCqlSession(Tester):
    def prepare_cluster(self, nodes=1, options_dict=None, version=None):
        logger.debug(f"Start cluster with {nodes} nodes")
        cluster = self.cluster
        if version:
            self.cluster.set_install_dir(version=version)
        if options_dict:
            cluster.set_configuration_options(values=options_dict)
        cluster.populate(nodes).start(wait_for_binary_proto=True)
        return cluster

    def _test_session_after_node_restart(self, node, session):
        query = "SELECT * FROM system.local LIMIT 1"
        rows = list(session.execute(query))
        assert len(rows) == 1, rows

        logger.debug(f"Restarting {node.name}")
        node.stop(gently=False)
        node.start(wait_other_notice=False, wait_for_binary_proto=True)

        start_time = time.time()
        timeout = 10
        while time.time() - start_time < timeout:
            try:
                logger.debug(f"{query}")
                rows = list(session.execute(query, timeout=10))
                assert len(rows) == 1, rows
                return
            except NoHostAvailable:
                logger.debug("Retrying")
                time.sleep(1)
        raise TimeoutError

    @pytest.mark.single_node
    def test_patient_cql_connection_after_node_restart(self):
        cluster = self.prepare_cluster()
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        self._test_session_after_node_restart(node1, session)

    @pytest.mark.single_node
    def test_cql_cluster_session_after_node_restart(self):
        cluster = self.prepare_cluster()
        node1 = cluster.nodelist()[0]

        with self.cql_cluster_session(node1) as session:
            self._test_session_after_node_restart(node1, session)

    @pytest.mark.single_node
    def test_exclusive_cql_cluster_session_after_node_restart(self):
        cluster = self.prepare_cluster()
        node1 = cluster.nodelist()[0]

        with self.cql_cluster_session(node1, exclusive=True) as session:
            self._test_session_after_node_restart(node1, session)

    def test_cluster_patient_cql_connection_after_node_restart(self):
        cluster = self.prepare_cluster(nodes=2)
        node1, _node2 = cluster.nodelist()
        session = self.patient_cql_connection(cluster.nodelist())
        self._test_session_after_node_restart(node1, session)

    def test_cluster_patient_cql_connection_after_node_restart_other(self):
        cluster = self.prepare_cluster(nodes=2)
        _node1, node2 = cluster.nodelist()
        session = self.patient_cql_connection(cluster.nodelist())
        self._test_session_after_node_restart(node2, session)

    def test_cluster_exclusive_cql_cluster_session_after_node_restart(self):
        cluster = self.prepare_cluster(nodes=2)
        node1 = cluster.nodelist()[0]

        with self.cql_cluster_session(node1, exclusive=True) as session:
            self._test_session_after_node_restart(node1, session)

    def test_cluster_exclusive_cql_cluster_session_after_node_restart_other(self):
        cluster = self.prepare_cluster(nodes=2)
        node1, node2 = cluster.nodelist()

        with self.cql_cluster_session(node1, exclusive=True) as session:
            self._test_session_after_node_restart(node2, session)

    def test_data_integrity_with_down_nodes(self):
        rf = 3
        num_keys = 100
        additional_keys = 25
        num_rows = 100
        additional_rows = 1
        keyspace_name = "test_keyspace"
        table_name = "test_table"
        expected_rows = {}

        def insert_data(session, start_key, num_keys, start_row, num_rows):
            logger.debug(f"Inserting keys[{start_key}:{start_key + num_keys}] rows[{start_row}:{start_row + num_rows}]")
            query = session.prepare(f"INSERT INTO {keyspace_name}.{table_name} (key, c, v) VALUES (?, ?, ?)")
            query.consistency_level = ConsistencyLevel.QUORUM
            futures = {}
            for k in range(start_key, start_key + num_keys):
                for i in range(start_row, start_row + num_rows):
                    f = session.execute_async(query, [f"k{k}", f"c{i}", f"v{i}"])
                    futures.setdefault(k, []).append(f)
            for k in range(start_key, start_key + num_keys):
                for f in futures.pop(k):
                    res = list(f.result())
                    assert len(res) == 0
                if k in expected_rows:
                    expected_rows[k] += num_rows
                else:
                    expected_rows[k] = num_rows

        @retrying(num_attempts=5, sleep_time=1, allowed_exceptions=(Unavailable, ReadFailure))
        def verify_data(session, start_key, num_keys):
            logger.debug(f"Verifying keys[{start_key}:{start_key + num_keys}]")
            query = session.prepare(f"SELECT * FROM {keyspace_name}.{table_name} WHERE key = ?")
            query.consistency_level = ConsistencyLevel.QUORUM
            futures = {}
            for k in range(start_key, start_key + num_keys):
                f = session.execute_async(query, [f"k{k}"])
                futures[k] = f
            for k in range(start_key, start_key + num_keys):
                f = futures[k]
                res = list(f.result())
                assert len(res) == expected_rows[k], res

        cluster = self.prepare_cluster(nodes={"dc1": {"rack1": 2, "rack2": 1, "rack3": 1}})
        session = self.patient_cql_connection(cluster.nodelist())

        create_ks(session, keyspace_name, rf)
        create_cf(session, table_name)

        insert_data(session, start_key=0, num_keys=num_keys, start_row=0, num_rows=num_rows)
        verify_data(session, start_key=0, num_keys=num_keys)

        for node in cluster.nodelist():
            logger.debug(f"Stopping {node.name}")
            node.stop(gently=False)

            verify_data(session, start_key=0, num_keys=num_keys)
            insert_data(session, start_key=0, num_keys=num_keys + additional_keys, start_row=num_rows, num_rows=additional_rows)
            num_keys += additional_keys
            num_rows += additional_keys

            logger.debug(f"Restarting {node.name}")
            node.start(wait_other_notice=True, wait_for_binary_proto=True)
            verify_data(session, start_key=0, num_keys=num_keys)
