# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2020 ScyllaDB

# From cassandra/test/unit/org/apache/cassandra/cql3/validation/operations/BatchTest.java
import logging
import time
from collections import Counter

import pytest
import requests
from cassandra.query import BatchStatement

from dtest_class import Tester, get_ip_from_node
from tools.marks import issue_open, with_feature

KEYSPACE = "batch_ks"
logger = logging.getLogger(__file__)


@pytest.mark.dtest_full
@pytest.mark.single_node
@pytest.mark.next_gating
class TestBatch(Tester):
    """
    Tests for pushed native protocol notification from Cassandra.
    """

    def prepare(self, nodes=1):
        cluster = self.cluster
        if not cluster.nodelist():
            cluster.populate(nodes).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]
        session = self.fixture_dtest_setup.patient_cql_connection(node1)
        return session

    @pytest.mark.lwt
    def test_batch_prepared_with_slow_query_log(self):
        """
        batch prepared in combination of slow query should not fail with error.
        scylladb/scylla/#5843
        """
        session = self.prepare()
        node1 = self.cluster.nodelist()[0]
        node_ip = get_ip_from_node(node1)
        slow_query_url = f"http://{node_ip}:{node1.api_port}/storage_service/slow_query"
        logger.info(f"Enabling slow logging on node {node_ip}")
        response = requests.post(slow_query_url, params={"enable": True, "ttl": 604800, "threshold": 0})
        logger.info(f"respone={response.content!s}")
        update_command = "UPDATE clustering SET val=? WHERE id=? AND clustering1=? AND clustering2=? AND clustering3=? IF val=?"
        self.batch_ttl_conditional_interaction(session=session, update_command=update_command)
        requests.post(slow_query_url, params={"enable": False})
        errors = node1.grep_log("No .query. parameter set for a session requesting a slow_query_log record")
        assert len(errors) == 0, f"No errors expected when enabling slow logging, {errors}"
        result = session.execute(query="SELECT * FROM system_traces.node_slow_log")
        counter = Counter(getattr(row, "command") for row in result.current_rows)
        assert counter[update_command] > 0, f"not found slow query logging of command={update_command}"

    @pytest.mark.lwt
    def test_batch_ttl_conditional_interaction(self):
        session = self.prepare()
        self.batch_ttl_conditional_interaction(session)

    def batch_ttl_conditional_interaction(  # noqa: PLR0915
        self,
        session,
        update_command=("UPDATE clustering SET val=? WHERE id=? AND clustering1=? AND clustering2=? AND clustering3=? IF val=?"),
    ):
        session.execute(f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} WITH replication = {{ 'class': 'NetworkTopologyStrategy', 'replication_factor': '1' }}")

        session.set_keyspace(KEYSPACE)
        session.execute("CREATE TABLE clustering (id int, clustering1 int, clustering2 int, clustering3 int, val int, PRIMARY KEY(id, clustering1, clustering2, clustering3))")

        clustering_insert = session.prepare("INSERT INTO clustering(id, clustering1, clustering2, clustering3, val) VALUES(?, ?, ?, ?, ?)")
        clustering_conditional_update = session.prepare(update_command)
        clustering_delete = session.prepare("DELETE FROM clustering WHERE id=? AND clustering1=? AND clustering2=? AND clustering3=?")
        clustering_range_delete = "DELETE FROM clustering WHERE id=? AND clustering1=?"
        clustering_update = "UPDATE clustering SET val=? WHERE id=? AND clustering1=? AND clustering2=? AND clustering3=?"
        clustering_conditional_delete = "DELETE FROM clustering WHERE id=? AND clustering1=? AND clustering2=? AND clustering3=? IF val=?"
        clustering_conditional_insert = "INSERT INTO clustering (id, clustering1, clustering2, clustering3, val) VALUES(?, ?, ?, ?, ?) IF NOT EXISTS"
        clustering_ttl_insert = "INSERT INTO clustering(id, clustering1, clustering2, clustering3, val) VALUES(?, ?, ?, ?, ?) USING TTL ?"
        clustering_conditional_ttl_update = "UPDATE clustering USING TTL ? SET val=? WHERE id=? AND clustering1=? AND clustering2=? AND clustering3=? IF val=?"
        clustering_conditional_ttl_insert = "INSERT INTO clustering(id, clustering1, clustering2, clustering3, val) VALUES(?, ?, ?, ?, ?)  IF NOT EXISTS USING TTL ?"
        clustering_ttl_update = "UPDATE clustering USING TTL ? SET val=? WHERE id=? AND clustering1=? AND clustering2=? AND clustering3=?"
        get_rows_pk = session.prepare("SELECT * FROM clustering WHERE id=?")

        row_01 = (1, 1, 1, 1, 1)
        session.execute(clustering_insert, row_01)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert rows == [row_01]

        batch = BatchStatement()
        row_02 = (1, 1, 1, 2, 2)
        batch.add(clustering_insert, row_02)
        batch.add(clustering_conditional_update, (11, *row_01))
        row_03 = (1, 1, 1, 1, 11)  # changed row_01
        session.execute(batch)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert rows == [row_03, row_02]

        batch = BatchStatement()
        row_04 = (1, 1, 2, 3, 23)
        batch.add(clustering_insert, row_04)
        batch.add(clustering_conditional_update, (22, *row_02))
        row_05 = (1, 1, 1, 2, 22)  # changed row_02
        batch.add(clustering_delete, row_01[:-1])
        session.execute(batch)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert sorted(rows) == sorted([row_04, row_05])

        batch = BatchStatement()
        row_06 = (1, 2, 3, 4, 1234)
        batch.add(clustering_insert, row_06)
        batch.add(clustering_conditional_update, (234, *row_05))
        row_07 = (1, 1, 1, 2, 234)  # changed row 05
        session.execute(batch)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert sorted(rows) == sorted([row_04, row_06, row_07])

        # cmd5
        batch = BatchStatement()
        clustering_range_delete = "DELETE FROM clustering WHERE id=1 AND clustering1=2"
        batch.add(clustering_range_delete)  # row_06[:2]
        batch.add(clustering_conditional_update, (1234, *row_07))
        row_08 = (1, 1, 1, 2, 1234)  # changed row 07
        session.execute(batch)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert sorted(rows) == sorted([row_04, row_08])

        batch = BatchStatement()
        clustering_update = "UPDATE clustering SET val=345 WHERE id=1 AND clustering1=3 AND clustering2=4 AND clustering3=5"
        row_09 = (1, 3, 4, 5, 345)  # new row from update
        batch.add(clustering_update)  # row_09[-1:] + row_09[:-1])
        batch.add(clustering_conditional_update, (1, *row_08))
        row_10 = (1, 1, 1, 2, 1)  # changed row_08
        session.execute(batch)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert sorted(rows) == sorted([row_04, row_09, row_10])

        # If any condition is not fulfilled the entire batch is skipped
        # so no delete, either
        batch = BatchStatement()
        batch.add(clustering_delete, row_09[:-1])
        batch.add(clustering_conditional_update, (2300, 1, 1, 2, 3, 1))  # no match
        session.execute(batch)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert sorted(rows) == sorted([row_04, row_09, row_10])

        batch = BatchStatement()
        clustering_conditional_delete = "DELETE FROM clustering WHERE id=1 AND clustering1=3 AND clustering2=4 AND clustering3=5 IF val=345"
        batch.add(clustering_conditional_delete)  # row_09)
        clustering_range_delete = "DELETE FROM clustering WHERE id=1 AND clustering1=1"
        batch.add(clustering_range_delete)  # , (1, 1))  # deletes row_04, row_05
        row_11 = (1, 2, 3, 4, 5)
        batch.add(clustering_insert, row_11)
        session.execute(batch)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert sorted(rows) == sorted([row_11])

        # cmd9
        batch = BatchStatement()
        clustering_conditional_insert = "INSERT INTO clustering (id, clustering1, clustering2, clustering3, val) VALUES(1, 3, 4, 5, 345) IF NOT EXISTS"
        batch.add(clustering_conditional_insert)  # row_09)
        batch.add(clustering_delete, row_11[:-1])
        session.execute(batch)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert sorted(rows) == sorted([row_09])

        batch = BatchStatement()
        clustering_ttl_insert = "INSERT INTO clustering(id, clustering1, clustering2, clustering3, val) VALUES(1, 2, 3, 4, 5) USING TTL 5"
        batch.add(clustering_ttl_insert)  # row_11 + (5,))
        clustering_conditional_ttl_update = "UPDATE clustering USING TTL 10 SET val=5 WHERE id=1 AND clustering1=3 AND clustering2=4 AND clustering3=5 IF val=345"
        batch.add(clustering_conditional_ttl_update)  # (10, 5) + row_09)
        row_13 = (1, 3, 4, 5, 5)  # changed row_09
        session.execute(batch)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert sorted(rows) == sorted([row_11, row_13])
        time.sleep(6)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert sorted(rows) == sorted([row_13])

        # cmd11
        batch = BatchStatement()
        clustering_conditional_ttl_insert = "INSERT INTO clustering(id, clustering1, clustering2, clustering3, val) VALUES(1, 2, 3, 4, 5)  IF NOT EXISTS USING TTL 5"
        batch.add(clustering_conditional_ttl_insert)  # row_11 + (5,))
        row_14 = (1, 4, 5, 6, 7)
        batch.add(clustering_insert, row_14)
        session.execute(batch)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert sorted(rows) == sorted([row_11, row_13, row_14])
        time.sleep(6)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        row_15 = (1, 3, 4, 5, None)  # updated row_13[:-1] + (None,)
        assert sorted(rows) == sorted([row_14, row_15])

        batch = BatchStatement()
        clustering_conditional_ttl_update = "UPDATE clustering USING TTL 5 SET val=5 WHERE id=1 AND clustering1=3 AND clustering2=4 AND clustering3=5 IF val=NULL"
        batch.add(clustering_conditional_ttl_update)  # (5, 5) + row_15) # row_13
        clustering_ttl_update = "UPDATE clustering USING TTL 5 SET val=8 WHERE id=1 AND clustering1=4 AND clustering2=5 AND clustering3=6"
        row_16 = (1, 4, 5, 6, 8)  # row_14[:-1] + [8]
        row_17 = (1, 4, 5, 6, None)  # row_16[:-1] + [None]
        batch.add(clustering_ttl_update)  # (5, 8) + row_14[:-1])
        session.execute(batch)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert sorted(rows) == sorted([row_13, row_16])
        time.sleep(6)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert sorted(rows) == sorted([row_15, row_17])

    @pytest.mark.lwt
    def test_batch_static_ttl_conditional_interaction(self):  # noqa: PLR0915
        session = self.prepare()

        session.execute(f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} WITH replication = {{ 'class': 'NetworkTopologyStrategy', 'replication_factor': '1' }}")

        session.set_keyspace(KEYSPACE)
        session.execute("CREATE TABLE clustering_static (id int, clustering1 int, clustering2 int, clustering3 int, sval int static, val int, PRIMARY KEY(id, clustering1, clustering2, clustering3))")
        session.execute("DELETE FROM clustering_static WHERE id=1")

        get_rows_pk = session.prepare("SELECT * FROM clustering_static WHERE id=?")
        clustering_static_insert = "INSERT INTO clustering_static(id, clustering1, clustering2, clustering3, sval, val) VALUES(?, ?, ?, ?, ?, ?)"
        clustering_insert = "INSERT INTO clustering_static(id, clustering1, clustering2, clustering3, val) VALUES(?, ?, ?, ?, ?)"
        clustering_static_conditional_update = "UPDATE clustering_static SET val=? WHERE id=? AND clustering1=? AND clustering2=? AND clustering3=? IF sval=?"
        clustering_static_update = "UPDATE clustering_static SET sval=? WHERE id=?"
        clustering_delete = "DELETE FROM clustering_static WHERE id=? AND clustering1=? AND clustering2=? AND clustering3=?"
        clustering_static_conditional_ttl_update = "UPDATE clustering_static USING TTL ? SET val=? WHERE id=? AND clustering1=? AND clustering2=? AND clustering3=? IF sval=?"
        clustering_range_delete = "DELETE FROM clustering_static WHERE id=? AND clustering1=?"
        clustering_update = "UPDATE clustering_static SET val=? WHERE id=? AND clustering1=? AND clustering2=? AND clustering3=?"
        clustering_conditional_delete = "DELETE FROM clustering_static WHERE id=? AND clustering1=? AND clustering2=? AND clustering3=? IF val=?"
        clustering_conditional_insert = "INSERT INTO clustering_static(id, clustering1, clustering2, clustering3, val) VALUES(?, ?, ?, ?, ?) IF NOT EXISTS"
        clustering_ttl_insert = "INSERT INTO clustering_static(id, clustering1, clustering2, clustering3, val) VALUES(?, ?, ?, ?, ?) USING TTL ?"
        clustering_conditional_ttl_update = "UPDATE clustering_static USING TTL ? SET val=? WHERE id=? AND clustering1=? AND clustering2=? AND clustering3=? IF val=?"
        clustering_conditional_ttl_insert = "INSERT INTO clustering_static(id, clustering1, clustering2, clustering3, val) VALUES(?, ?, ?, ?, ?)  IF NOT EXISTS USING TTL ?"
        clustering_ttl_update = "UPDATE clustering_static USING TTL ? SET val=? WHERE id=? AND clustering1=? AND clustering2=? AND clustering3=?"
        clustering_static_conditional_delete = "DELETE FROM " + KEYSPACE + ".clustering_static WHERE id=%s AND clustering1=%s AND clustering2=%s AND clustering3=%s IF sval=%s"
        clustering_static_conditional_static_update = "UPDATE clustering_static SET sval=? WHERE id=? IF sval=?"

        batch = BatchStatement()
        row_01 = (1, 1, 1, 1, 1, 1)
        clustering_static_insert = "INSERT INTO clustering_static(id, clustering1, clustering2, clustering3, sval, val) VALUES(1, 1, 1, 1, 1, 1)"
        batch.add(clustering_static_insert)  # row_01)
        session.execute(batch)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert sorted(rows) == sorted([row_01])

        batch = BatchStatement()
        row_02 = (1, 1, 1, 2, 1, 2)  # static sval
        clustering_insert = "INSERT INTO clustering_static(id, clustering1, clustering2, clustering3, val) VALUES(1, 1, 1, 2, 2)"
        batch.add(clustering_insert)  # row_02)
        row_03 = (1, 1, 1, 1, 1, 11)  # changed row_01
        clustering_static_conditional_update = "UPDATE clustering_static SET val=11 WHERE id=1 AND clustering1=1 AND clustering2=1 AND clustering3=1 IF sval=1"
        batch.add(clustering_static_conditional_update)  # (11,) + row_01[:-2] + row_01[-1:])
        session.execute(batch)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert sorted(rows) == sorted([row_02, row_03])

        batch = BatchStatement()
        row_04 = (1, 1, 2, 3, 1, 23)  # static sval
        clustering_insert = "INSERT INTO clustering_static(id, clustering1, clustering2, clustering3, val) VALUES(1, 1, 2, 3, 23)"
        batch.add(clustering_insert)  # row_04 no sval
        clustering_static_update = "UPDATE clustering_static SET sval=22 WHERE id=1"
        batch.add(clustering_static_update)  # (22, 1))
        row_04 = (1, 1, 1, 2, 22, 2)  # row_02 changed
        row_05 = (1, 1, 2, 3, 22, 23)  # row_04 changed
        clustering_delete = "DELETE FROM clustering_static WHERE id=1 AND clustering1=1 AND clustering2=1 AND clustering3=1"
        batch.add(clustering_delete)  # row_03[:-2])
        session.execute(batch)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert sorted(rows) == sorted([row_04, row_05])

        # cmd4
        batch = BatchStatement()
        clustering_insert = "INSERT INTO clustering_static(id, clustering1, clustering2, clustering3, val) VALUES(1, 2, 3, 4, 1234)"
        batch.add(clustering_insert)
        row_06 = (1, 2, 3, 4, 22, 1234)  # static sval
        clustering_static_conditional_ttl_update = "UPDATE clustering_static USING TTL 5 SET val=234 WHERE id=1 AND clustering1=1 AND clustering2=1 AND clustering3=2 IF sval=22"
        row_07 = (1, 1, 1, 2, 22, 234)  # changed row_04
        batch.add(clustering_static_conditional_ttl_update)
        session.execute(batch)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert sorted(rows) == sorted([row_05, row_06, row_07])
        time.sleep(6)
        row_08 = (1, 1, 1, 2, 22, None)  # changed row_07 after ttl
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert sorted(rows) == sorted([row_05, row_06, row_08])

        batch = BatchStatement()
        clustering_range_delete = "DELETE FROM clustering_static WHERE id=1 AND clustering1=2"
        batch.add(clustering_range_delete)  # delete row_06
        clustering_static_conditional_update = "UPDATE clustering_static SET val=1234 WHERE id=1 AND clustering1=1 AND clustering2=1 AND clustering3=2 IF sval=22"
        batch.add(clustering_static_conditional_update)
        row_09 = (1, 1, 1, 2, 22, 1234)  # changed row_08
        session.execute(batch)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert sorted(rows) == sorted([row_05, row_09])

        batch = BatchStatement()
        clustering_update = "UPDATE clustering_static SET val=345 WHERE id=1 AND clustering1=3 AND clustering2=4 AND clustering3=5"
        batch.add(clustering_update)
        row_10 = (1, 3, 4, 5, 22, 345)  # new
        clustering_static_conditional_update = "UPDATE clustering_static SET val=1 WHERE id=1 AND clustering1=1 AND clustering2=1 AND clustering3=2 IF sval=22"
        batch.add(clustering_static_conditional_update)
        row_11 = (1, 1, 1, 2, 22, 1)  # changed row_09
        session.execute(batch)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert sorted(rows) == sorted([row_05, row_10, row_11])

        # cmd7
        batch = BatchStatement()
        clustering_delete = "DELETE FROM clustering_static WHERE id=1 AND clustering1=3 AND clustering2=4 AND clustering3=5"
        batch.add(clustering_delete)  # delete row_10  (1, 3, 4, 5)
        clustering_static_conditional_update = "UPDATE clustering_static SET val=2300 WHERE id=1 AND clustering1=1 AND clustering2=2 AND clustering3=3 IF sval=1"
        batch.add(clustering_static_conditional_update)  # no match (!= sval)
        session.execute(batch)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert sorted(rows) == sorted([row_05, row_10, row_11])

        batch = BatchStatement()
        clustering_conditional_delete = "DELETE FROM clustering_static WHERE id=1 AND clustering1=3 AND clustering2=4 AND clustering3=5 IF val=345"
        batch.add(clustering_conditional_delete)  # (1, 3, 4, 5, 345))  # row_10
        clustering_range_delete = "DELETE FROM clustering_static WHERE id=1 AND clustering1=1"
        batch.add(clustering_range_delete)  # (1, 1)  # row_05/11
        clustering_insert = "INSERT INTO clustering_static(id, clustering1, clustering2, clustering3, val) VALUES(1, 2, 3, 4, 5)"
        batch.add(clustering_insert)  # (1, 2, 3, 4, 5)
        row_12 = (1, 2, 3, 4, 22, 5)
        session.execute(batch)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert sorted(rows) == sorted([row_12])

        batch = BatchStatement()
        clustering_conditional_insert = "INSERT INTO clustering_static(id, clustering1, clustering2, clustering3, val) VALUES(1, 3, 4, 5, 345) IF NOT EXISTS"
        batch.add(clustering_conditional_insert)  # (1, 3, 4, 5, 345))
        row_13 = (1, 3, 4, 5, 22, 345)
        clustering_delete = "DELETE FROM clustering_static WHERE id=1 AND clustering1=2 AND clustering2=3 AND clustering3=4"
        batch.add(clustering_delete)  # (1, 2, 3, 4))
        session.execute(batch)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert sorted(rows) == sorted([row_13])

        batch = BatchStatement()
        clustering_ttl_insert = "INSERT INTO clustering_static(id, clustering1, clustering2, clustering3, val) VALUES(1, 2, 3, 4, 5) USING TTL 5"
        batch.add(clustering_ttl_insert)  # (1, 2, 3, 4, 5, 5))
        row_14 = (1, 2, 3, 4, 22, 5)
        clustering_conditional_ttl_update = "UPDATE clustering_static USING TTL 10 SET val=5 WHERE id=1 AND clustering1=3 AND clustering2=4 AND clustering3=5 IF val=345"
        batch.add(clustering_conditional_ttl_update)  # (10, 5, 1, 3, 4, 5, 345))
        row_15 = (1, 3, 4, 5, 22, 5)  # changed row_13
        session.execute(batch)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert sorted(rows) == sorted([row_14, row_15])
        time.sleep(6)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert sorted(rows) == sorted([row_15])

        batch = BatchStatement()
        row_16 = (1, 2, 3, 4, 22, 5)
        clustering_conditional_ttl_insert = "INSERT INTO clustering_static(id, clustering1, clustering2, clustering3, val) VALUES(1, 2, 3, 4, 5)  IF NOT EXISTS USING TTL 5"
        batch.add(clustering_conditional_ttl_insert)  # row_16[:-2] + (5,) + (5,)) # val, ttl
        clustering_insert = "INSERT INTO clustering_static(id, clustering1, clustering2, clustering3, val) VALUES(1, 4, 5, 6, 7)"
        batch.add(clustering_insert)  # (1, 4, 5, 6, 7))
        row_17 = (1, 4, 5, 6, 22, 7)
        session.execute(batch)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert sorted(rows) == sorted([row_15, row_16, row_17])
        time.sleep(6)
        row_18 = (1, 3, 4, 5, 22, None)  # changed row_15 after ttl
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert sorted(rows) == sorted([row_17, row_18])

        # cmd12
        batch = BatchStatement()
        clustering_conditional_ttl_update = "UPDATE clustering_static USING TTL 5 SET val=5 WHERE id=1 AND clustering1=3 AND clustering2=4 AND clustering3=5 IF val=NULL"
        row_19 = (1, 3, 4, 5, 22, 5)  # changed row_18
        batch.add(clustering_conditional_ttl_update)
        clustering_ttl_update = "UPDATE clustering_static USING TTL 5 SET val=8 WHERE id=1 AND clustering1=4 AND clustering2=5 AND clustering3=6"
        batch.add(clustering_ttl_update)  # (5, 8, 1, 4, 5, 6))
        row_20 = (1, 4, 5, 6, 22, 8)  # changed row_17
        session.execute(batch)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert sorted(rows) == sorted([row_19, row_20])
        time.sleep(6)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        row_21 = (1, 3, 4, 5, 22, None)  # changed row_19
        row_22 = (1, 4, 5, 6, 22, None)  # changed row_20
        assert sorted(rows) == sorted([row_21, row_22])

        # cmd13
        batch = BatchStatement()
        batch.add(clustering_static_conditional_delete, (1, 3, 4, 5, 22))
        clustering_insert = "INSERT INTO clustering_static(id, clustering1, clustering2, clustering3, val) VALUES(1, 2, 3, 4, 5)"
        row_23 = (1, 2, 3, 4, 22, 5)  # changed row_21
        batch.add(clustering_insert)  # (1, 2, 3, 4, 5))
        session.execute(batch)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert sorted(rows) == sorted([row_22, row_23])

        batch = BatchStatement()
        clustering_static_conditional_static_update = "UPDATE clustering_static SET sval=23 WHERE id=1 IF sval=22"
        batch.add(clustering_static_conditional_static_update)  # (23, 1, 22))
        row_24 = (1, 2, 3, 4, 23, 5)  # changed row_23
        clustering_delete = "DELETE FROM clustering_static WHERE id=1 AND clustering1=4 AND clustering2=5 AND clustering3=6"
        batch.add(clustering_delete)  # delete row_22
        session.execute(batch)
        rows = session.execute(get_rows_pk, (1,)).current_rows
        assert sorted(rows) == sorted([row_24])
