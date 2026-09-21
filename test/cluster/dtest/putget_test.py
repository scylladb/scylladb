import binascii
import logging
import sys
import time

import pytest
from cassandra import ConsistencyLevel

from dtest_class import Tester, create_cf, create_ks
from tools.cluster_topology import generate_cluster_topology
from tools.data import (
    create_c1c2_table,
    insert_c1c2,
    insert_columns,
    putget,
    query_c1c2,
    query_columns,
    range_putget,
)
from tools.misc import retry_till_success

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestPutGet(Tester):
    @pytest.fixture(scope="function", autouse=True)
    def fixture_set_cluster_settings(self, fixture_dtest_setup):
        fixture_dtest_setup.cluster.set_configuration_options({"start_rpc": "true"})

    @pytest.fixture(scope="function")
    def cluster_setup_3_nodes(self):
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=3, nodes_per_rack=1)
        cluster.populate(cluster_topology).start()

    def test_putget(self, cluster_setup_3_nodes):
        """Simple put/get on a single row, hitting multiple sstables"""
        self._putget()

    def test_putget_snappy(self, cluster_setup_3_nodes):
        """Simple put/get on a single row, but hitting multiple sstables (with snappy compression)"""
        self._putget(compression="Snappy")

    def test_putget_deflate(self, cluster_setup_3_nodes):
        """Simple put/get on a single row, but hitting multiple sstables (with deflate compression)"""
        self._putget(compression="Deflate")

    # Simple queries, but with flushes in between inserts to make sure we hit
    # sstables (and more than one) on reads
    def _putget(self, compression=None):
        node1, _node2, _node3 = self.cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 3)
        create_cf(session, "cf", compression=compression)

        putget(self.cluster, session)

    def test_non_local_read(self, cluster_setup_3_nodes):
        """This test reads from a coordinator we know has no copy of the data"""
        node1, _node2, _node3 = self.cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 3)
        create_c1c2_table(session)

        # insert and get at CL.QUORUM (since RF=2, node1 won't have all key locally)
        insert_c1c2(session, n=1000, consistency=ConsistencyLevel.QUORUM)
        for n in range(1000):
            query_c1c2(session, n, ConsistencyLevel.QUORUM)

    def test_rangeputget(self, cluster_setup_3_nodes):
        """Simple put/get on ranges of rows, hitting multiple sstables"""
        node1, _node2, _node3 = self.cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 3)
        create_cf(session, "cf")

        range_putget(self.cluster, session)

    def test_wide_row(self):
        """Test wide row slices"""
        cluster = self.cluster

        cluster.populate(3).start()
        node1, _node2, _node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 1)
        create_cf(session, "cf")

        key = "wide"

        for x in range(1, 5001):
            insert_columns(session, key, 100, offset=x - 1)

        for size in (10, 100, 1000):
            for x in range(1, (50001 - size) // size):
                query_columns(session, key, size, offset=x * size - 1)
