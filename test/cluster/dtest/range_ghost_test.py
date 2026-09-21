import time

import pytest

from dtest_class import Tester, create_cf, create_ks


@pytest.mark.dtest_full
@pytest.mark.single_node
@pytest.mark.next_gating
class TestRangeGhosts(Tester):
    def test_ghosts(self):
        """Check range ghost are correctly removed by the system"""
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()

        time.sleep(0.5)
        session = self.cql_connection(node1)
        create_ks(session, "ks", 1)
        create_cf(session, "cf", gc_grace=0, columns={"c": "text"})

        rows = 1000

        for i in range(rows):
            session.execute("UPDATE cf SET c = 'value' WHERE key = 'k%i'" % i)

        res = list(session.execute("SELECT * FROM cf LIMIT 10000"))
        assert len(res) == rows, res

        node1.flush()

        for i in range(rows // 2):
            session.execute("DELETE FROM cf WHERE key = 'k%i'" % i)

        res = list(session.execute("SELECT * FROM cf LIMIT 10000"))
        # no ghosts in 1.2+
        assert len(res) == rows // 2, len(res)

        node1.flush()
        time.sleep(1)  # make sure tombstones are collected
        node1.compact()

        res = list(session.execute("SELECT * FROM cf LIMIT 10000"))
        assert len(res) == rows // 2, len(res)
