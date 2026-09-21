import pytest

from dtest_class import Tester, create_ks


@pytest.mark.dtest_full
@pytest.mark.single_node
@pytest.mark.next_gating
class TestSimpleBootShutdown(Tester):
    def prepare(self):
        """
        Sets up node to test against.
        """
        cluster = self.cluster
        return cluster

    def test_boot_create_keyspace_table_shutdown_boot_insert_select(self):
        cluster = self.prepare()
        cluster.populate(1).start()
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

        node1.flush()
        node1.stop()

        node1.start(update_pid=True)
        session = self.patient_cql_connection(node1, "ks")

        session.execute("insert into ks.test1  (k,c) values (1,2);")

        # Select
        res = list(
            session.execute(
                """
                SELECT * FROM ks.test1
                WHERE k=1
        """
            )
        )

        assert len(res) == 1, f"expected length=1 got {res}"

        # Select
        res = list(
            session.execute(
                """
                SELECT * FROM ks.test1
                WHERE k=2
        """
            )
        )

        assert len(res) == 0, f"expected length=0 got {res}"

    def test_boot_create_keyspace_table_insert_shutdown_select(self):
        cluster = self.prepare()
        cluster.populate(1).start()
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

        session.execute("insert into ks.test1  (k,c) values (1,2);")

        node1.flush()
        node1.stop()

        node1.start(update_pid=True)
        session = self.patient_cql_connection(node1, "ks")
        # Select
        res = list(
            session.execute(
                """
                SELECT * FROM ks.test1
                WHERE k=1
        """
            )
        )

        assert len(res) == 1, f"expected length=1 got {res}"

        # Select
        res = list(
            session.execute(
                """
                SELECT * FROM ks.test1
                WHERE k=2
        """
            )
        )

        assert len(res) == 0, f"expected length=0 got {res}"

    @pytest.mark.dtest_debug
    def test_boot_create_keyspace_table_insert_shutdown_commitlog_replay_select(self):
        cluster = self.prepare()
        cluster.populate(1).start()
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

        session.execute("insert into ks.test1  (k,c) values (1,2);")

        # wait for the commitlog to be fsynched
        node1.flush()
        node1.stop(gently=False)

        node1.start(update_pid=True, wait_for_binary_proto=True)
        session = self.patient_cql_connection(node1, "ks")
        # Select
        res = list(
            session.execute(
                """
                SELECT * FROM ks.test1
                WHERE k=1
        """
            )
        )

        assert len(res) == 1, f"expected length=1 got {res}"

        # Select
        res = list(
            session.execute(
                """
                SELECT * FROM ks.test1
                WHERE k=2
        """
            )
        )

        assert len(res) == 0, f"expected length=0 got {res}"
