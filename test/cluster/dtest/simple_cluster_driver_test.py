import logging
import time

import pytest
from cassandra import ConsistencyLevel, Unavailable
from cassandra.query import SimpleStatement
from ccmlib.scylla_cluster import ScyllaCluster

from dtest_class import Tester, create_ks
from tools.cluster_topology import generate_cluster_topology
from tools.marks import with_feature
from tools.schema import get_replication_options

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestSimpleCluster(Tester):
    __scylla_args__ = []

    def prepare(self):
        """
        Sets up cluster to test against.
        """
        cluster = self.cluster
        return cluster

    def test_simple_create_insert_select(self):
        cluster = self.prepare()
        jvm_args = []
        if type(cluster) is ScyllaCluster:
            jvm_args = self.__scylla_args__
        cluster_topology = generate_cluster_topology(rack_num=3)
        cluster.populate(cluster_topology).start(jvm_args=jvm_args)
        node1, node2, node3 = cluster.nodelist()
        session1 = self.patient_cql_connection(node1)
        session2 = self.patient_cql_connection(node2)
        session3 = self.patient_cql_connection(node3)
        create_ks(session1, "ks", 3)

        session2.execute(
            """
            CREATE TABLE ks.test1 (
                k int PRIMARY KEY,
                c int
            )
        """
        )

        insert = SimpleStatement("insert into ks.test1  (k,c) values (1,2);", consistency_level=ConsistencyLevel.QUORUM)
        session3.execute(insert)

        # Select
        query1 = SimpleStatement("SELECT * FROM ks.test1 WHERE k=1", consistency_level=ConsistencyLevel.QUORUM)
        res = list(session1.execute(query1))
        assert len(res) == 1, res
        res = list(session2.execute(query1))
        assert len(res) == 1, res
        res = list(session3.execute(query1))
        assert len(res) == 1, res

        # Select
        query2 = SimpleStatement("SELECT * FROM ks.test1 WHERE k=2", consistency_level=ConsistencyLevel.QUORUM)
        res = list(session1.execute(query2))
        assert len(res) == 0, res
        res = list(session2.execute(query2))
        assert len(res) == 0, res
        res = list(session3.execute(query2))
        assert len(res) == 0, res

    def clname(self, cl):
        cl_mapping = {ConsistencyLevel.ANY: "ANY", ConsistencyLevel.ONE: "ONE", ConsistencyLevel.TWO: "TWO", ConsistencyLevel.THREE: "THREE", ConsistencyLevel.QUORUM: "QUORUM", ConsistencyLevel.ALL: "ALL"}
        return cl_mapping[cl]

    def simple_consistency_level_validate(self, session, node, read_keys, read_cls_pass, read_cls_fail, read_cls_pass_all, write_keys, write_cls_pass, write_cls_fail, write_cls_pass_all):  # noqa: PLR0913
        for cl in read_cls_pass:
            logger.info("read %s cl %s", node, self.clname(cl))
            read_pass = 0
            read_fail = 0
            for key in read_keys:
                try:
                    query1 = SimpleStatement("SELECT * FROM ks.test1 WHERE k=%s" % key, consistency_level=cl)
                    res = list(session.execute(query1))
                    assert len(res) == 1, res
                    read_pass = read_pass + 1
                except Exception:  # noqa: BLE001
                    read_fail = read_fail + 1
            assert read_fail == 0 or not read_cls_pass_all, f"Expected all reads to pass, pass {read_pass}, fail {read_fail}"
            assert read_fail > 0 or read_cls_pass_all, f"Expected some reads to fail, pass {read_pass}, fail {read_fail}"

        for cl in read_cls_fail:
            query1 = SimpleStatement("SELECT * FROM ks.test1 WHERE k=1", consistency_level=cl)
            with pytest.raises(Unavailable):
                list(session.execute(query1, timeout=1))
                pytest.fail(f"Consistency level {self.clname(cl)} is not possible")

        for cl in write_cls_pass:
            logger.info("write %s cl %s", node, self.clname(cl))
            write_pass = 0
            write_fail = 0
            for key in write_keys:
                try:
                    insert = SimpleStatement(f"insert into ks.test1  (k,c) values ({key},{key})", consistency_level=cl)
                    list(session.execute(insert))
                    write_pass = write_pass + 1
                except Exception:  # noqa: BLE001
                    write_fail = write_fail + 1
            assert write_fail == 0 or not write_cls_pass_all, f"Expected all writes to pass, pass {write_pass}, fail {write_fail}"
            assert write_fail > 0 or write_cls_pass_all, f"Expected some writes to fail, pass {write_pass}, fail {write_fail}"

        for cl in write_cls_fail:
            insert = SimpleStatement("insert into ks.test1 (k,c) values (101,101)", consistency_level=cl)
            with pytest.raises(Unavailable):
                list(session.execute(insert, timeout=1))
                pytest.fail(f"Consistency level {self.clname(cl)} is not possible")

    def prepare_cluster(self, ks_rf):
        cluster = self.prepare()
        jvm_args = []
        if type(cluster) is ScyllaCluster:
            jvm_args = self.__scylla_args__
        cluster_topology = generate_cluster_topology(rack_num=3)
        cluster.populate(cluster_topology).start(jvm_args=jvm_args)
        node1 = cluster.nodelist()[0]

        with self.patient_cql_connection(node1) as session1:
            create_ks(session1, "ks", ks_rf)
            session1.execute(
                """
                CREATE TABLE ks.test1 (
                    k int PRIMARY KEY,
                c int
            )
        """
            )
        return cluster

    def test_simple_rf_3_consistency_level(self):
        cluster = self.prepare_cluster(3)
        node1, node2, node3 = cluster.nodelist()
        session1 = self.patient_cql_connection(node1)
        session2 = self.patient_cql_connection(node2)
        session3 = self.patient_cql_connection(node3)

        keys = range(1, 100)
        for val in keys:
            insert = SimpleStatement(f"insert into ks.test1  (k,c) values ({val},{val})", consistency_level=ConsistencyLevel.ALL)
            session1.execute(insert)

        logger.info("3 nodes, node1,node2,node3 are running")
        read_cls_pass = [ConsistencyLevel.ONE, ConsistencyLevel.TWO, ConsistencyLevel.THREE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL]
        write_cls_pass = [ConsistencyLevel.ANY, ConsistencyLevel.ONE, ConsistencyLevel.TWO, ConsistencyLevel.THREE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL]
        self.simple_consistency_level_validate(session1, "node 1", keys, read_cls_pass, [], True, range(101, 200), write_cls_pass, [], True)
        self.simple_consistency_level_validate(session2, "node 2", keys, read_cls_pass, [], True, range(201, 300), write_cls_pass, [], True)
        self.simple_consistency_level_validate(session3, "node 3", keys, read_cls_pass, [], True, range(301, 400), write_cls_pass, [], True)

        node1.stop(wait_other_notice=True)
        logger.info("node 1 stopped, node2,node3 are running")
        read_cls_pass = [ConsistencyLevel.ONE, ConsistencyLevel.TWO, ConsistencyLevel.QUORUM]
        read_cls_fail = [ConsistencyLevel.THREE, ConsistencyLevel.ALL]
        write_cls_pass = [ConsistencyLevel.ANY, ConsistencyLevel.ONE, ConsistencyLevel.TWO, ConsistencyLevel.QUORUM]
        write_cls_fail = [ConsistencyLevel.THREE, ConsistencyLevel.ALL]
        self.simple_consistency_level_validate(session2, "node 2", keys, read_cls_pass, read_cls_fail, True, range(401, 500), write_cls_pass, write_cls_fail, True)
        self.simple_consistency_level_validate(session3, "node 3", keys, read_cls_pass, read_cls_fail, True, range(501, 600), write_cls_pass, write_cls_fail, True)

        node2.stop(wait_other_notice=True)
        logger.info("node 2 stopped, node3 is running")
        read_cls_pass = [ConsistencyLevel.ONE]
        read_cls_fail = [ConsistencyLevel.TWO, ConsistencyLevel.THREE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL]
        write_cls_pass = [ConsistencyLevel.ANY, ConsistencyLevel.ONE]
        write_cls_fail = [ConsistencyLevel.TWO, ConsistencyLevel.THREE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL]
        self.simple_consistency_level_validate(session3, "node 3", keys, read_cls_pass, read_cls_fail, True, range(601, 700), write_cls_pass, write_cls_fail, True)

        # should add additional tests once a node can be entered back into a cluster

    # The test assumes that replicas are distributed across all nodes. Not the case with rack-list and RF=1.
    # This case is tested by test_simple_rf_1_consistency_level_racklist
    @pytest.mark.skip_if(with_feature("tablets"))
    def test_simple_rf_1_consistency_level_no_racklist(self):
        cluster = self.prepare_cluster(1)
        node1, node2, node3 = cluster.nodelist()
        session1 = self.patient_cql_connection(node1)
        session2 = self.patient_cql_connection(node2)
        session3 = self.patient_cql_connection(node3)

        keys = range(1, 100)
        for val in keys:
            insert = SimpleStatement(f"insert into ks.test1  (k,c) values ({val},{val})", consistency_level=ConsistencyLevel.ALL)
            session1.execute(insert)

        logger.info("3 nodes, node1,node2,node3 are running")
        read_cls_pass = [ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL]
        read_cls_fail = [ConsistencyLevel.TWO, ConsistencyLevel.THREE]
        write_cls_pass = [ConsistencyLevel.ANY, ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL]
        write_cls_fail = [ConsistencyLevel.TWO, ConsistencyLevel.THREE]
        self.simple_consistency_level_validate(session1, "node 1", keys, read_cls_pass, read_cls_fail, True, range(101, 200), write_cls_pass, write_cls_fail, True)
        self.simple_consistency_level_validate(session2, "node 2", keys, read_cls_pass, read_cls_fail, True, range(201, 300), write_cls_pass, write_cls_fail, True)
        self.simple_consistency_level_validate(session3, "node 3", keys, read_cls_pass, read_cls_fail, True, range(301, 400), write_cls_pass, write_cls_fail, True)

        node1.stop()
        logger.info("node 1 stopped, node2,node3 are running")
        read_cls_pass = [ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL]
        read_cls_fail = [ConsistencyLevel.TWO, ConsistencyLevel.THREE]
        write_cls_pass = [ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL]
        write_cls_fail = [ConsistencyLevel.TWO, ConsistencyLevel.THREE]
        self.simple_consistency_level_validate(session2, "node 2", keys, read_cls_pass, read_cls_fail, False, range(401, 500), write_cls_pass, write_cls_fail, False)
        self.simple_consistency_level_validate(session3, "node 3", keys, read_cls_pass, read_cls_fail, False, range(501, 600), write_cls_pass, write_cls_fail, False)

        node2.stop()
        logger.info("node 2 stopped, node3 is running")
        read_cls_pass = [ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL]
        read_cls_fail = [ConsistencyLevel.TWO, ConsistencyLevel.THREE]
        write_cls_pass = [ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL]
        write_cls_fail = [ConsistencyLevel.TWO, ConsistencyLevel.THREE]
        self.simple_consistency_level_validate(session3, "node 3", keys, read_cls_pass, read_cls_fail, False, range(601, 700), write_cls_pass, write_cls_fail, False)

        # should add additional tests once a node can be entered back into a cluster

    @pytest.mark.skip_if(~with_feature("tablets"))
    def test_simple_rf_1_consistency_level_racklist(self):
        cluster = self.prepare_cluster({"datacenter1": ["rack3"]})
        node1, node2, node3 = cluster.nodelist()
        session1 = self.patient_cql_connection(node1)
        session2 = self.patient_cql_connection(node2)
        session3 = self.patient_cql_connection(node3)

        rf = get_replication_options(session1, "ks")[node3.data_center]
        assert rf == ["rack3"]

        keys = range(1, 100)
        for val in keys:
            insert = SimpleStatement(f"insert into ks.test1  (k,c) values ({val},{val})", consistency_level=ConsistencyLevel.ALL)
            session1.execute(insert)

        logger.info("3 nodes, node1,node2,node3 are running")
        read_cls_pass = [ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL]
        read_cls_fail = [ConsistencyLevel.TWO, ConsistencyLevel.THREE]
        write_cls_pass = [ConsistencyLevel.ANY, ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL]
        write_cls_fail = [ConsistencyLevel.TWO, ConsistencyLevel.THREE]
        self.simple_consistency_level_validate(session1, "node 1", keys, read_cls_pass, read_cls_fail, True, range(101, 200), write_cls_pass, write_cls_fail, True)
        self.simple_consistency_level_validate(session2, "node 2", keys, read_cls_pass, read_cls_fail, True, range(201, 300), write_cls_pass, write_cls_fail, True)
        self.simple_consistency_level_validate(session3, "node 3", keys, read_cls_pass, read_cls_fail, True, range(301, 400), write_cls_pass, write_cls_fail, True)

        node1.stop()
        logger.info("node 1 stopped, node2,node3 are running")
        read_cls_pass = [ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL]
        read_cls_fail = [ConsistencyLevel.TWO, ConsistencyLevel.THREE]
        write_cls_pass = [ConsistencyLevel.ANY, ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL]
        write_cls_fail = [ConsistencyLevel.TWO, ConsistencyLevel.THREE]
        self.simple_consistency_level_validate(session2, "node 2", keys, read_cls_pass, read_cls_fail, True, range(201, 300), write_cls_pass, write_cls_fail, True)
        self.simple_consistency_level_validate(session3, "node 3", keys, read_cls_pass, read_cls_fail, True, range(301, 400), write_cls_pass, write_cls_fail, True)

        node3.stop()
        logger.info("node 3 stopped, node2 is running")
        read_cls_pass = []
        read_cls_fail = [ConsistencyLevel.TWO, ConsistencyLevel.THREE, ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL]
        write_cls_pass = []
        write_cls_fail = [ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL, ConsistencyLevel.TWO, ConsistencyLevel.THREE]
        self.simple_consistency_level_validate(session2, "node 2", keys, read_cls_pass, read_cls_fail, True, range(601, 700), write_cls_pass, write_cls_fail, True)

        # should add additional tests once a node can be entered back into a cluster

    def simple_query_validate(self, session, node, read_keys, query, read_cls_pass, read_cls_fail, read_cls_pass_all):  # noqa: PLR0913
        for cl in read_cls_pass:
            logger.info("read %s cl %s", node, self.clname(cl))
            read_pass = 0
            read_fail = 0
            errors = []
            try:
                query1 = SimpleStatement(query, consistency_level=cl)
                res = list(session.execute(query1))
                assert len(res) == read_keys, f"got {len(res)} expected {read_keys} : {res}"
                read_pass = read_pass + 1
            except Exception as ex:  # noqa: BLE001
                read_fail = read_fail + 1
                errors = [*errors, ex]
            assert read_fail == 0 or not read_cls_pass_all, f"Expected all reads to pass, pass {read_pass}, fail {read_fail} {errors}"
            assert read_fail > 0 or read_cls_pass_all, f"Expected some reads to fail, pass {read_pass}, fail {read_fail}"

        for cl in read_cls_fail:
            query1 = SimpleStatement(query, consistency_level=cl)
            with pytest.raises(Unavailable):
                list(session.execute(query1))
                pytest.fail(f"Consistency level {self.clname(cl)} is not possible")

    def test_simple_rf_1_query(self):
        cluster = self.prepare_cluster(1)

        node1, node2, node3 = cluster.nodelist()
        session1 = self.patient_cql_connection(node1)
        session2 = self.patient_cql_connection(node2)
        session3 = self.patient_cql_connection(node3)

        keys = range(1, 100)
        for val in keys:
            insert = SimpleStatement(f"insert into ks.test1  (k,c) values ({val},{val})", consistency_level=ConsistencyLevel.ALL)
            session1.execute(insert)

        logger.info("3 nodes, node1,node2,node3 are running")
        read_cls_pass = [ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL]
        read_cls_fail = [ConsistencyLevel.TWO, ConsistencyLevel.THREE]
        self.simple_query_validate(session1, "node 1", len(keys), "SELECT * FROM ks.test1", read_cls_pass, read_cls_fail, True)
        self.simple_query_validate(session1, "node 1", 10, "SELECT * FROM ks.test1 where k in (1,10,20,30,40,50,60,70,80,90) ", read_cls_pass, read_cls_fail, True)
        self.simple_query_validate(session2, "node 2", len(keys), "SELECT * FROM ks.test1", read_cls_pass, read_cls_fail, True)
        self.simple_query_validate(session2, "node 2", 10, "SELECT * FROM ks.test1 where k in (1,10,20,30,40,50,60,70,80,90) ", read_cls_pass, read_cls_fail, True)
        self.simple_query_validate(session3, "node 3", len(keys), "SELECT * FROM ks.test1", read_cls_pass, read_cls_fail, True)
        self.simple_query_validate(session3, "node 3", 10, "SELECT * FROM ks.test1 where k in (1,10,20,30,40,50,60,70,80,90) ", read_cls_pass, read_cls_fail, True)

    def test_simple_rf_3_query(self):
        cluster = self.prepare_cluster(3)

        node1, node2, node3 = cluster.nodelist()
        session1 = self.patient_cql_connection(node1)
        session2 = self.patient_cql_connection(node2)
        session3 = self.patient_cql_connection(node3)

        keys = range(1, 100)
        for val in keys:
            insert = SimpleStatement(f"insert into ks.test1  (k,c) values ({val},{val})", consistency_level=ConsistencyLevel.ALL)
            session1.execute(insert)

        logger.info("3 nodes, node1,node2,node3 are running")
        read_cls_pass = [ConsistencyLevel.ONE, ConsistencyLevel.TWO, ConsistencyLevel.THREE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL]
        self.simple_query_validate(session1, "node 1", len(keys), "SELECT * FROM ks.test1", read_cls_pass, [], True)
        self.simple_query_validate(session1, "node 1", 10, "SELECT * FROM ks.test1 where k in (1,10,20,30,40,50,60,70,80,90) ", read_cls_pass, [], True)
        self.simple_query_validate(session2, "node 2", len(keys), "SELECT * FROM ks.test1", read_cls_pass, [], True)
        self.simple_query_validate(session2, "node 2", 10, "SELECT * FROM ks.test1 where k in (1,10,20,30,40,50,60,70,80,90) ", read_cls_pass, [], True)
        self.simple_query_validate(session3, "node 3", len(keys), "SELECT * FROM ks.test1", read_cls_pass, [], True)
        self.simple_query_validate(session3, "node 3", 10, "SELECT * FROM ks.test1 where k in (1,10,20,30,40,50,60,70,80,90) ", read_cls_pass, [], True)
