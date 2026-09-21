import time

import pytest
from ccmlib.scylla_cluster import ScyllaCluster

from dtest_class import Tester, create_ks


@pytest.mark.next_gating
@pytest.mark.dtest_debug
@pytest.mark.dtest_full
@pytest.mark.single_node
class TestSimpleDriver(Tester):
    @pytest.fixture(params=(["--smp", "1"], ["--smp", "2"]), ids=["SMP=1", "SMP=2"], autouse=True)
    def fixture_scylla_args(self, request):
        self.__scylla_args__ = request.param

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
        cluster.populate(1).start(jvm_args=jvm_args)
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 1)

        session.execute(
            """
            CREATE TABLE test1 (
                k int PRIMARY KEY,
                c int
            )
        """
        )

        session.execute("insert into test1  (k,c) values (1,2);")

        # Select
        res = session.execute(
            """
                SELECT * FROM test1
                WHERE k=1
        """
        )

        assert len(list(res)) == 1, list(res)

        # Select
        res = session.execute(
            """
                SELECT * FROM test1
                WHERE k=2
        """
        )

        assert len(list(res)) == 0, list(res)
        time.sleep(10)

    def test_simple_composite_partition_key_create_insert_select(self):
        cluster = self.prepare()
        jvm_args = []
        if type(cluster) is ScyllaCluster:
            jvm_args = self.__scylla_args__
        cluster.populate(1).start(jvm_args=jvm_args)
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 1)

        session.execute(
            """
            CREATE TABLE test1 (
                k1 int,
                k2 int,
                c int,
                PRIMARY KEY ((k1,k2))
            )
        """
        )

        session.execute("insert into test1 (k1,k2,c) values (1,1,3);")
        session.execute("insert into test1 (k1,k2,c) values (1,2,4);")

        # Select
        res = session.execute(
            """
                SELECT * FROM test1
                WHERE k1=1 and k2=1
        """
        )
        assert len(list(res)) == 1, list(res)

        res = session.execute(
            """
                SELECT * FROM test1
                WHERE k1=1 and k2=2
        """
        )
        assert len(list(res)) == 1, list(res)

        # Select
        res = session.execute(
            """
                SELECT * FROM test1
                WHERE k1=2 and k2=1
        """
        )
        assert len(list(res)) == 0, list(res)

        res = session.execute(
            """
                SELECT * FROM test1
                WHERE k1=1 and k2=3
        """
        )
        assert len(list(res)) == 0, list(res)

        time.sleep(1)

    def test_simple_compound_primary_key_create_insert_select(self):
        cluster = self.prepare()
        jvm_args = []
        if type(cluster) is ScyllaCluster:
            jvm_args = self.__scylla_args__
        cluster.populate(1).start(jvm_args=jvm_args)
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 1)

        session.execute(
            """
            CREATE TABLE test1 (
                k1 int,
                c1 int,
                c2 int,
                c3 int,
                PRIMARY KEY (k1,c1,c2)
            )
        """
        )

        session.execute("insert into test1 (k1,c1,c2,c3) values (1,1,1,1);")
        session.execute("insert into test1 (k1,c1,c2,c3) values (1,1,2,2);")

        # Select
        res = session.execute(
            """
                SELECT * FROM test1
                WHERE k1=1
        """
        )
        assert len(list(res)) == 2, list(res)

        res = session.execute(
            """
                SELECT * FROM test1
                WHERE k1=1 and c1=1 and c2=1
        """
        )
        assert len(list(res)) == 1, list(res)

        # Select
        res = session.execute(
            """
                SELECT * FROM test1
                WHERE k1=1 and c1=2
        """
        )

        res = session.execute(
            """
                SELECT * FROM test1
                WHERE k1=1 and c1=1 and c2=3
        """
        )
        assert len(list(res)) == 0, list(res)

        time.sleep(1)
