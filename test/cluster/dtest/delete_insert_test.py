import concurrent.futures
import logging
import random
import uuid
from concurrent.futures import ThreadPoolExecutor
from math import floor

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from dtest_class import Tester, create_ks
from tools.cluster_topology import generate_cluster_topology

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestDeleteInsert(Tester):
    """
    Examines scenarios around deleting data and adding data back with the same key
    """

    ROWS = [(str(uuid.uuid1()), x, random.choice(["group1", "group2", "group3", "group4"])) for x in range(1000)]

    def create_ddl(self, session, rf=None):
        if rf is None:
            rf = {"dc1": 2, "dc2": 2}
        create_ks(session, "delete_insert_search_test", rf)
        session.execute("CREATE TABLE test (id uuid PRIMARY KEY, val1 text, group text)")
        session.execute("CREATE INDEX group_idx ON test (group)")

    def delete_group_rows(self, session, group):
        """Delete rows from a given group and return them"""
        rows = [r for r in self.ROWS if r[2] == group]
        ids = [r[0] for r in rows]

        # Now allowed select more then 100 PKs:
        # <Error from server: code=0000 [Server error]
        # message="partition key cartesian product size 270 is greater than maximum 100">
        start = 0
        last_portion = len(ids)
        if len(ids) >= 100:
            last_portion = len(ids) % 100  # if elements amount in the list is not divided by 100
            for i in range(floor(len(ids) / 100)):
                end = 100 * (i + 1)
                session.execute(f"DELETE FROM test WHERE id in ({', '.join(ids[start:end])})")
                start = end

        if last_portion:
            session.execute(f"DELETE FROM test WHERE id in ({', '.join(ids[start:-1])})")
        return rows

    def insert_all_rows(self, session):
        self.insert_some_rows(session, self.ROWS)

    def insert_some_rows(self, session, rows):
        for row in rows:
            session.execute("INSERT INTO test (id, val1, group) VALUES ({}, '{}', '{}')".format(*row))

    def test_delete_insert_search(self):
        cluster = self.cluster
        topology_layout = generate_cluster_topology(dc_num=2, rack_num=2, nodes_per_rack=1, dc_name_prefix="dc")
        cluster.populate(topology_layout).start()
        node1 = cluster.nodelist()[0]

        session = self.cql_connection(node1)
        session.consistency_level = "LOCAL_QUORUM"

        self.create_ddl(session)
        # Create 1000 rows:
        self.insert_all_rows(session)
        # Delete all of group2:
        deleted = self.delete_group_rows(session, "group2")
        # Put that group back:
        self.insert_some_rows(session, rows=deleted)

        # Verify that all of group2 is back, 20 times, in parallel
        # querying across all nodes:

        def run_query(connection):
            query = SimpleStatement("SELECT * FROM delete_insert_search_test.test WHERE group = 'group2'", consistency_level=ConsistencyLevel.LOCAL_QUORUM)
            rows = connection.execute(query)
            assert len(rows) == len(deleted), f"Expected {len(deleted)}, got {len(rows)}"

        max_workers = 20
        executor = ThreadPoolExecutor(max_workers=max_workers)
        threads = []
        for x in range(max_workers):
            conn = self.cql_connection(random.choice(cluster.nodelist()))
            threads.append(executor.submit(run_query, conn))

        concurrent.futures.wait(threads, return_when=concurrent.futures.ALL_COMPLETED)
