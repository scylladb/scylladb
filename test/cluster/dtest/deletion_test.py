import logging
import time
from datetime import datetime, timedelta

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from dtest_class import Tester, create_ks
from tools.assertions import assert_all, assert_invalid, assert_one
from tools.cluster import new_node
from tools.cluster_topology import generate_cluster_topology, generate_rack_topology_based_rf
from tools.marks import issue_open, with_feature

logger = logging.getLogger(__file__)

pytestmark = pytest.mark.next_gating


@pytest.mark.dtest_full
class TestRangeDeletion(Tester):
    compaction_strategy = None

    @pytest.fixture(params=["LeveledCompactionStrategy", "SizeTieredCompactionStrategy", "TimeWindowCompactionStrategy"], autouse=True)
    def fixture_compaction_strategy(self, request):
        self.compaction_strategy = request.param

    def prepare(  # noqa: PLR0913
        self,
        create_keyspace=True,
        use_cache=False,
        nodes=1,
        rf=1,
        protocol_version=None,
        user=None,
        password=None,
        **kwargs,
    ):
        cluster = self.cluster

        if use_cache:
            cluster.set_configuration_options(values={"row_cache_size_in_mb": 100})

        start_rpc = kwargs.pop("start_rpc", False)
        if start_rpc:
            cluster.set_configuration_options(values={"start_rpc": True})

        if user:
            config = {"authenticator": "org.apache.cassandra.auth.PasswordAuthenticator", "authorizer": "org.apache.cassandra.auth.CassandraAuthorizer", "permissions_validity_in_ms": 0}
            cluster.set_configuration_options(values=config)

        if not cluster.nodelist():
            rack_layout = nodes if isinstance(nodes, dict) else generate_rack_topology_based_rf(nodes, rf)
            topology_layout = {"dc1": rack_layout}
            cluster.populate(topology_layout).start(wait_for_binary_proto=True)

        node1 = cluster.nodelist()[0]

        session = self.fixture_dtest_setup.patient_cql_connection(node1, protocol_version=protocol_version, user=user, password=password)
        if create_keyspace:
            create_ks(session, "ks", rf)
        return session

    def create_cf_1pk_1ck(self, session):
        query = "CREATE TABLE ks.test1 (pk int, ck date, v1 int, PRIMARY KEY(pk, ck)) WITH compaction = {'class': '%s' }" % self.compaction_strategy
        logger.info(query)
        session.execute(query)

    def create_cf_2ck_int(self, session):
        query = "CREATE TABLE ks.test1 (pk1 int, ck1 int, ck2 int, v1 int, PRIMARY KEY(pk1, ck1, ck2)) WITH compaction = {'class': '%s' }" % self.compaction_strategy
        logger.info(query)
        session.execute(query)

    def create_cf_2ck(self, session):
        query = "CREATE TABLE ks.test1 (pk1 int, ck1 int, ck2 varchar, v1 int, PRIMARY KEY(pk1, ck1, ck2)) WITH compaction = {'class': '%s' }" % self.compaction_strategy
        logger.info(query)
        session.execute(query)

    @staticmethod
    def insert_data_cf_1pk_1ck(conn, rows_in_pk, ttl=None):
        """Create data for 2 partitions
        Data example:
            [[1, '2019-11-18', 0],
            [1, '2019-11-19', 1],
            [1, '2019-11-20', 2],
            [1, '2019-11-21', 3],
            [1, '2019-11-22', 4],
            ..................
            [2, '2019-11-23', 5],
            [2, '2019-11-24', 6],
            [2, '2019-11-25', 7],
            [2, '2019-11-26', 8],
            [2, '2019-11-27', 9]]
        """
        current_date = datetime.now()
        # Data for first partition
        data = list([1, (current_date + timedelta(days=i)).strftime("%Y-%m-%d"), i] for i in range(rows_in_pk))
        # Data for second partition
        data.extend(list([2, (current_date + timedelta(days=i)).strftime("%Y-%m-%d"), i] for i in range(rows_in_pk)))

        ttl_clause = " USING TTL %d" % ttl if ttl else ""

        for pk, ck, v1 in data:
            conn.execute(SimpleStatement(f"INSERT INTO ks.test1 (pk, ck, v1) VALUES ({pk}, '{ck}', {v1}){ttl_clause}", consistency_level=ConsistencyLevel.QUORUM))
        return data

    def insert_data_cf_2ck_matrix(self, conn, num_of_pks, rows_per_ck, flush: bool = True):
        """Create data for 2 partitions
        Data example:
        [[0, 0, 0, 0], [0, 0, 1, 1], [0, 0, 2, 2], [0, 1, 0, 0], [0, 1, 1, 1], [0, 1, 2, 2],
         [0, 2, 0, 0], [0, 2, 1, 1], [0, 2, 2, 2], [1, 0, 0, 0], [1, 0, 1, 1], [1, 0, 2, 2],
         [1, 1, 0, 0], [1, 1, 1, 1], [1, 1, 2, 2], [1, 2, 0, 0], [1, 2, 1, 1], [1, 2, 2, 2]]
        """
        data = list()
        sub_partition_rows = 2
        # Inserting data per number of partitions
        for pkey in range(num_of_pks):  # pk1 value
            for ckey1 in range(rows_per_ck):  # ck1 values
                for ckey2 in range(rows_per_ck):  # ck1 values
                    data.append([pkey, ckey1, ckey2, ckey2])

        if flush:
            rows_in_pk = rows_per_ck**2
            start_row = 0
            for _ in range(num_of_pks):
                end_row = start_row + rows_in_pk
                for pk1, ck1, ck2, v1 in data[start_row:end_row]:
                    conn.execute(f"INSERT INTO ks.test1 (pk1, ck1, ck2, v1) VALUES ({pk1}, {ck1}, {ck2}, {v1})")
                self.cluster.flush()
                start_row += rows_in_pk

        else:
            for pk1, ck1, ck2, v1 in data:
                conn.execute(f"INSERT INTO ks.test1 (pk1, ck1, ck2, v1) VALUES ({pk1}, {ck1}, {ck2}, {v1})")
        return data

    @staticmethod
    def insert_data_cf_2ck(conn, rows_in_pk):
        """Create data for 2 partitions
        Data example:
            [[0, 0, 'ck0', 0],
            [0, 0, 'ck1', 1],
            [0, 0, 'ck2', 2],
            [0, 0, 'ck3', 3],
            [0, 0, 'ck4', 4],
            [0, 0, 'ck5', 5],
            ..................
            [1, 1, 'ck5', 5],
            [1, 1, 'ck6', 6],
            [1, 1, 'ck7', 7],
            [1, 1, 'ck8', 8],
            [1, 1, 'ck9', 9]]
        """
        data = list()
        sub_partition_rows = 2
        # Data for first and second partitions
        for p in [0, 1]:  # pk1 value
            for k in range(rows_in_pk):  # ck1 and ck2 values
                data.append([p, p, "ck%d" % k, k])

        for pk1, ck1, ck2, v1 in data:
            conn.execute(SimpleStatement(f"INSERT INTO ks.test1 (pk1, ck1, ck2, v1) VALUES ({pk1}, {ck1}, '{ck2}', {v1})", consistency_level=ConsistencyLevel.QUORUM))
        return data

    def test_delete_by_2ck_range_in(self):
        """
        The table has 1 PKs and 2 CKs
        Delete range of data using in condition on both CK columns
        """
        session = self.prepare(nodes=3, rf=3)
        # Create table with 1 PKs and 2 CKs
        self.create_cf_2ck(session=session)

        data = self.insert_data_cf_2ck(conn=session, rows_in_pk=10)

        select_query = "SELECT * FROM ks.test1"
        assert_all(session=session, query=select_query, expected=data, cl=ConsistencyLevel.QUORUM, ignore_order=True)
        self.cluster.flush()

        range_indexes = [4, 6, 8]  # indexes of element in "data" variable - all these rows should be deleted
        pk_index = range_indexes[0]
        query = "DELETE FROM ks.test1 WHERE pk1={pk1} and ck1 in ({ck1}) and ck2 in ({ck2})".format(
            pk1=data[pk_index][0], ck1=", ".join(str(data[indx][1]) for indx in range_indexes), ck2=", ".join("'%s'" % data[indx][2] for indx in range_indexes)
        )
        logger.info(query)
        session.execute(query)
        self.cluster.flush()

        assert_one(session, "select count(*) from ks.test1", [17])

        # Prepare list with expected data
        for indx in sorted(range_indexes, reverse=True):
            del data[indx]

        assert_all(session=session, query=select_query, expected=data, cl=ConsistencyLevel.ALL, ignore_order=True)

    def test_delete_multiple_ranges_by_2ck(self):  # noqa: PLR0915
        """
        The table has 1 PKs and 2 CKs.
        Delete ranges of data using conditions on PKs and both CK columns.

        """
        session = self.prepare(nodes=2, rf=2)
        node1, node2 = self.cluster.nodelist()
        node1.nodetool("disableautocompaction")
        self.create_cf_2ck_int(session=session)
        node2.stop(wait_other_notice=True)
        num_of_pks = 2
        rows_per_ck = 4
        total_rows_num = num_of_pks * rows_per_ck**2
        ck_deletion_skip = 2
        data = self.insert_data_cf_2ck_matrix(conn=session, num_of_pks=num_of_pks, rows_per_ck=rows_per_ck)

        select_query = "SELECT * FROM ks.test1"
        assert_all(session=session, query=select_query, expected=data, cl=ConsistencyLevel.ONE, ignore_order=True)

        overlap_index = 2  # index for deletion ranges intersection
        deletion_range = 2
        delete_query = "DELETE FROM ks.test1 WHERE pk1={pk1} and ck1 = {ck1} and ck2 >= {ck2_min} and ck2 < {ck2_max}"
        # For each pk: for some of ck1: generate 2 overlapping deletion ranges of ck2
        # For example:
        # DELETE FROM ks.test1 WHERE pk1=0 and ck1 = 0 and ck2 >= 0 and ck2 < 2
        # DELETE FROM ks.test1 WHERE pk1=0 and ck1 = 0 and ck2 >= 1 and ck2 < 3
        # DELETE FROM ks.test1 WHERE pk1=0 and ck1 = 2 and ck2 >= 0 and ck2 < 2
        # DELETE FROM ks.test1 WHERE pk1=0 and ck1 = 2 and ck2 >= 1 and ck2 < 3
        for pkey in range(num_of_pks):
            for ckey1 in range(0, rows_per_ck, ck_deletion_skip):
                query = delete_query.format(pk1=pkey, ck1=ckey1, ck2_min=overlap_index - deletion_range, ck2_max=overlap_index)
                logger.info(query)
                session.execute(query)
                query = delete_query.format(pk1=pkey, ck1=ckey1, ck2_min=overlap_index - deletion_range + 1, ck2_max=overlap_index + 1)
                logger.info(query)
                session.execute(query)
            self.cluster.flush()

        num_of_deleted_rows = total_rows_num // ck_deletion_skip // (rows_per_ck / (deletion_range + 1))
        total_left_rows = total_rows_num - num_of_deleted_rows
        assert_one(session, "select count(*) from ks.test1", [total_left_rows])

        # For each pk: delete a half of the rows by ck1 filtering
        # For example:
        # DELETE FROM ks.test1 WHERE pk1=1 and ck1 >= 2 (causing another overlap range-tombstones).
        for pkey in range(num_of_pks):
            query = f"DELETE FROM ks.test1 WHERE pk1={pkey} and ck1 >= {rows_per_ck // 2}"
            logger.info(query)
            session.execute(query)
            self.cluster.flush()

        total_left_rows //= 2
        assert_one(session, "select count(*) from ks.test1", [total_left_rows])

        # For a single pk: delete all its rows by ck1 filtering:
        # DELETE FROM ks.test1 WHERE pk1 = 0 and ck1 >= 0
        query = f"DELETE FROM ks.test1 WHERE pk1 = 0 and ck1 >= 0"
        logger.info(query)
        session.execute(query)
        self.cluster.flush()

        total_left_rows -= total_left_rows / num_of_pks
        assert_one(session, "select count(*) from ks.test1", [total_left_rows])

        logger.debug("start and repair node 2")
        node2.start(wait_for_binary_proto=True)
        node2.repair()
        logger.debug("Check for correct number of table rows after a repair with all range tombstones")
        node1.stop(wait_other_notice=True)
        session = self.fixture_dtest_setup.patient_exclusive_cql_connection(node2)
        assert_one(session, "select count(*) from ks.test1", [total_left_rows])
        node1.start(wait_for_binary_proto=True)
        node2.stop(wait_other_notice=True)
        logger.debug("Check for correct number of table rows after a major compaction with all range tombstones")
        node1.compact()
        session = self.fixture_dtest_setup.patient_exclusive_cql_connection(node1)
        assert_one(session, "select count(*) from ks.test1 bypass cache", [total_left_rows])

    def test_delete_by_2ck_range_equal_and_not_equal(self):
        """
        The table has 2 CKs
        Delete range of data using equal condition on first CK column and non-EQ on second CK column
        """
        session = self.prepare(nodes=3, rf=3)
        # Create table with 2 PKs and 2 CKs
        self.create_cf_2ck(session=session)

        data = self.insert_data_cf_2ck(conn=session, rows_in_pk=10)

        select_query = "SELECT * FROM ks.test1"
        assert_all(session=session, query=select_query, expected=data, cl=ConsistencyLevel.QUORUM, ignore_order=True)
        self.cluster.flush()

        lower_index = 4  # index of element in "data" variable - all rows before it should be deleted
        query = f"DELETE FROM ks.test1 WHERE pk1={data[lower_index][0]} and ck1 = {data[lower_index][1]} and ck2 >= '{data[lower_index][2]}'"
        logger.info(query)
        session.execute(query)
        self.cluster.flush()

        assert_one(session, "select count(*) from ks.test1", [14])

        assert_all(session=session, query=select_query, expected=data[:lower_index] + data[lower_index + 6 :], cl=ConsistencyLevel.ALL, ignore_order=True)

    def test_delete_by_2ck_range_one_non_equal(self):
        """
        The table has 2 CKs
        Delete range of data using ">=" condition on first CK column
        """
        session = self.prepare(nodes=3, rf=3)
        # Create table with 2 PKs and 2 CKs
        self.create_cf_2ck(session=session)

        data = self.insert_data_cf_2ck(conn=session, rows_in_pk=10)

        select_query = "SELECT * FROM ks.test1"
        assert_all(session=session, query=select_query, expected=data, cl=ConsistencyLevel.QUORUM, ignore_order=True)
        self.cluster.flush()

        lower_index = 4  # index of element in "data" variable - select rows for deleted
        query = f"DELETE FROM ks.test1 WHERE pk1={data[lower_index][0]} and ck1 >= {data[lower_index][1]}"
        logger.info(query)
        session.execute(query)
        self.cluster.flush()

        assert_one(session, "select count(*) from ks.test1", [10])

        assert_all(session=session, query=select_query, expected=data[10:], cl=ConsistencyLevel.ALL, ignore_order=True)

    def test_update_by_1ck_range(self):
        """
        Update by range is not allowed - validate the query return valid error message
        """
        session = self.prepare(nodes=3, rf=3)
        self.create_cf_1pk_1ck(session=session)

        data = self.insert_data_cf_1pk_1ck(conn=session, rows_in_pk=10)

        lower_index = 4  # index of element in "data" variable
        query = f"UPDATE ks.test1 SET v1 = 100 WHERE pk={data[lower_index][0]} and ck < '{data[lower_index][1]}'"
        logger.info(query)
        assert_invalid(session=session, query=query, matching="Invalid operator in where clause")

    @pytest.mark.single_node
    def test_delete_by_2ck_range_failure(self):
        """
        Unsupported deletion - validate the query return valid error message
        """
        session = self.prepare(nodes=1)
        self.create_cf_2ck(session=session)

        # Filter by ck2
        query = "DELETE FROM ks.test1 WHERE pk1=0 and ck1 > 3 and ck2 = 'ck3'"
        logger.info(query)
        assert_invalid(session=session, query=query, matching='preceding column "ck1" is restricted by a non-EQ relation')

        # Filter by ck1 non-EQ relation
        query = "DELETE FROM ks.test1 WHERE pk1=0 and ck2 > 'ck3'"
        logger.info(query)
        assert_invalid(session=session, query=query, matching='cannot be restricted as preceding column "ck1" is not restricted')

        # Filter by ck1 non-EQ relation
        query = "DELETE FROM ks.test1 WHERE pk1=0 and ck1 > 3 and ck2 > 'ck3'"
        logger.info(query)
        assert_invalid(session=session, query=query, matching='preceding column "ck1" is restricted by a non-EQ relation')

        # Filter by ck1 non-EQ relation
        query = "DELETE FROM ks.test1 WHERE pk1=0 and ck1 > 3 and ck2 in ('ck3')"
        logger.info(query)
        assert_invalid(session=session, query=query, matching='preceding column "ck1" is restricted by a non-EQ relation')

    def test_delete_by_1ck_range_in(self):
        """
        Delete range of data using "in" condition on CK column
        """
        session = self.prepare(nodes=3, rf=3)
        # Create table with 1 clustering key
        self.create_cf_1pk_1ck(session=session)

        data = self.insert_data_cf_1pk_1ck(conn=session, rows_in_pk=10)
        self.cluster.flush()

        # Validate that all data inserted
        select_query = "SELECT pk, cast(ck as text), v1 FROM ks.test1"
        assert_all(session=session, query=select_query, expected=data, cl=ConsistencyLevel.QUORUM, ignore_order=True)

        first_index = 4  # index of element in "data" variable - this row should be deleted
        second_index = 12  # index of element in "data" variable - this row should be deleted
        query = f"DELETE FROM ks.test1 WHERE pk in ({data[first_index][0]}, {data[second_index][0]}) and ck in ('{data[first_index][1]}', '{data[second_index][1]}')"
        logger.info(query)
        session.execute(query)
        self.cluster.flush()

        assert_one(session, "select count(*) from ks.test1", [16])

        # Prepare list with expected data
        for indx in [first_index + 10, second_index, first_index, second_index - 10]:
            del data[indx]

        assert_all(session=session, query=select_query, expected=data, cl=ConsistencyLevel.ALL, ignore_order=True)

    def test_delete_by_1ck_range_less(self):
        """
        Delete range of data using "<" condition on CK column
        """
        session = self.prepare(nodes=3, rf=3)
        # Create table with 1 clustering key
        self.create_cf_1pk_1ck(session=session)

        data = self.insert_data_cf_1pk_1ck(conn=session, rows_in_pk=10)
        self.cluster.flush()

        # Validate that all data inserted
        select_query = "SELECT pk, cast(ck as text), v1 FROM ks.test1"
        assert_all(session=session, query=select_query, expected=data, cl=ConsistencyLevel.QUORUM, ignore_order=True)

        lower_index = 4  # index of element in "data" variable - all rows before it should be deleted
        query = f"DELETE FROM ks.test1 WHERE pk={data[lower_index][0]} and ck < '{data[lower_index][1]}'"
        logger.info(query)
        session.execute(query)
        self.cluster.flush()

        assert_one(session, "select count(*) from ks.test1", [16])
        assert_all(session=session, query=select_query, expected=data[lower_index:], cl=ConsistencyLevel.ALL, ignore_order=True)

    def test_delete_by_1ck_range_less_more(self):
        """
        Delete range of data using "<" and ">=" conditions on CK column
        """
        session = self.prepare(nodes=3, rf=3)
        # Create table with 1 clustering key
        self.create_cf_1pk_1ck(session=session)

        data = self.insert_data_cf_1pk_1ck(conn=session, rows_in_pk=10)
        self.cluster.flush()

        # Validate that all data inserted
        select_query = "SELECT pk, cast(ck as text), v1 FROM ks.test1"
        assert_all(session=session, query=select_query, expected=data, cl=ConsistencyLevel.QUORUM, ignore_order=True)

        lower_index = 4  # index of element in "data" variable - all rows before it should be deleted
        upper_index = 6  # index of element in "data" variable - all rows before it should be deleted
        query = f"DELETE FROM ks.test1 WHERE pk={data[lower_index][0]} and ck < '{data[upper_index][1]}' and ck >= '{data[lower_index][1]}'"
        logger.info(query)
        session.execute(query)
        self.cluster.flush()

        assert_one(session, "select count(*) from ks.test1", [18])
        assert_all(session=session, query=select_query, expected=data[:lower_index] + data[upper_index:], cl=ConsistencyLevel.ALL, ignore_order=True)

    def test_delete_when_node_stopped(self):
        """
        Task: https://trello.com/c/NHO1Gek9/1583-open-range-tombstones-new-tests-in-dtest
        Test open range deletion when one of the nodes is stopped
        After the node is returned back, correct data should be returned from this node

        current scenario in verifying hintedhandoff feature snd
        should be moved to that test set

        we already have similar test test_hintedhandoff_basic_check
        with write insteadof delete. so we can create new test or update existing one
        """
        session = self.prepare(nodes=3, rf=3)
        # Create table with 1 clustering key
        self.create_cf_1pk_1ck(session=session)
        data = self.insert_data_cf_1pk_1ck(conn=session, rows_in_pk=10)
        self.cluster.flush()

        # Validate that all data inserted
        select_query = "SELECT pk, cast(ck as text), v1 FROM ks.test1"
        assert_all(session=session, query=select_query, expected=data, cl=ConsistencyLevel.QUORUM, ignore_order=True)

        node2 = self.cluster.nodelist()[1]
        logger.info(f"Stop node {node2.name}")
        node2.stop(wait_other_notice=True)

        lower_index = 16  # index of element in "data" variable - all rows after it should be deleted
        query = f"DELETE FROM ks.test1 WHERE pk={data[lower_index][0]} and ck > '{data[lower_index][1]}'"
        logger.info(query)
        session.execute(query)
        self.cluster.flush()

        assert_one(session, "select count(*) from ks.test1", [17])
        assert_all(session=session, query=select_query, expected=data[: lower_index + 1], cl=ConsistencyLevel.QUORUM, ignore_order=True)

        logger.info(f"Start node {node2.name}")
        node2.start(wait_for_binary_proto=True)

        logger.info(f"Wait for hinted handoff to flush")
        time.sleep(15)

        for node in self.cluster.nodelist():
            if node is not node2:
                logger.info(f"Stop node {node.name}")
                node.stop(wait_other_notice=True)
        session = self.fixture_dtest_setup.patient_exclusive_cql_connection(node2)

        assert_one(session, "select count(*) from ks.test1", [17])
        assert_all(session=session, query=select_query, expected=data[: lower_index + 1], cl=ConsistencyLevel.ONE, ignore_order=True)

    @pytest.mark.required_features("!tablets")
    def test_delete_when_decommission_node(self):
        """
        Task: https://trello.com/c/NHO1Gek9/1583-open-range-tombstones-new-tests-in-dtest
        Test open range deletion when one of the nodes is decommissioned
        Add new node instead of decommissioned. Correct data should be returned from this node
        """
        # The test intentionally shrinks the cluster below the RF. This does not work with tablets
        # and does not have a chance to work with rf-rack-valid keyspaces. Fortunately, the rf-rack-valid
        # restriction does not apply to vnodes, so let's just put everything into one rack.
        nodes = {"rack1": 3}
        session = self.prepare(nodes=nodes, rf=3)
        # Create table with 1 clustering key
        self.create_cf_1pk_1ck(session=session)
        data = self.insert_data_cf_1pk_1ck(conn=session, rows_in_pk=10)
        self.cluster.flush()

        # Validate that all data inserted
        select_query = "SELECT pk, cast(ck as text), v1 FROM ks.test1"
        assert_all(session=session, query=select_query, expected=data, cl=ConsistencyLevel.QUORUM, ignore_order=True)

        node2 = self.cluster.nodelist()[1]
        logger.info(f"Decommission node {node2.name}")
        node2.decommission()

        lower_index = 16  # index of element in "data" variable - all rows after it should be deleted
        query = f"DELETE FROM ks.test1 WHERE pk={data[lower_index][0]} and ck > '{data[lower_index][1]}'"
        logger.info(query)
        session.execute(query)
        self.cluster.flush()

        assert_one(session, "select count(*) from ks.test1", [17])
        assert_all(session=session, query=select_query, expected=data[: lower_index + 1], cl=ConsistencyLevel.QUORUM, ignore_order=True)

        node_new = new_node(self.cluster, data_center="dc1", rack="rack1")
        logger.info(f"Add new node {node_new.name}")
        node_new.start(wait_for_binary_proto=True, wait_other_notice=True)

        for node in self.cluster.nodelist():
            if not (node in (node_new, node2)):
                logger.info(f"Stop node {node.name}")
                node.stop()

        session = self.fixture_dtest_setup.patient_exclusive_cql_connection(node_new)

        assert_one(session, "select count(*) from ks.test1", [17])
        assert_all(session=session, query=select_query, expected=data[: lower_index + 1], cl=ConsistencyLevel.ONE, ignore_order=True)

    @pytest.mark.single_node
    def test_delete_by_1ck_range_condition_if_exists_failed(self):
        session = self.prepare(nodes=1, rf=1)
        self.create_cf_1pk_1ck(session=session)
        data = self.insert_data_cf_1pk_1ck(session, 5)

        query = f"DELETE FROM ks.test1 where pk={data[0][0]} and ck < '{data[3][1]}' and ck >= '{data[1][1]}' IF EXISTS"
        logger.info(query)
        assert_invalid(session=session, query=query, matching="DELETE statements must restrict all PRIMARY KEY columns with equality relations in order to delete non static columns")

    @pytest.mark.single_node
    def test_delete_by_1ck_range_condition_if_equality_failed(self):
        session = self.prepare(nodes=1, rf=1)
        self.create_cf_1pk_1ck(session=session)
        data = self.insert_data_cf_1pk_1ck(session, 5)

        query = f"DELETE FROM ks.test1 WHERE pk={data[0][0]} and ck < '{data[3][1]}' and ck >= '{data[1][1]}' IF v1 = {data[1][2]}"
        logger.info(query)
        assert_invalid(session=session, query=query, matching="DELETE statements must restrict all PRIMARY KEY columns with equality relations in order to delete non static columns")

    @pytest.mark.single_node
    def test_delete_by_1ck_range_if_range_failed(self):
        session = self.prepare(nodes=1, rf=1)
        self.create_cf_1pk_1ck(session=session)
        data = self.insert_data_cf_1pk_1ck(session, 5)

        query = f"DELETE FROM ks.test1 WHERE pk={data[0][0]} and ck < '{data[3][1]}' and ck >= '{data[1][1]}' \
                IF v1 >= {data[1][2]} and v1 < {data[3][2]}"
        logger.info(query)
        assert_invalid(session=session, query=query, matching="DELETE statements must restrict all PRIMARY KEY columns with equality relations in order to delete non static columns")

    @pytest.mark.lwt
    def test_delete_by_1ck_range_conditional_batch(self):
        """if in batch at least one operation with IF, whole batch is conditional"""
        num_rows = 5
        session = self.prepare(nodes=3, rf=3)
        self.create_cf_1pk_1ck(session=session)
        data = self.insert_data_cf_1pk_1ck(session, num_rows)

        select_query = "SELECT pk, cast(ck as text), v1 FROM ks.test1"
        assert_all(session=session, query=select_query, expected=data, cl=ConsistencyLevel.QUORUM, ignore_order=True)

        lower_index = 1
        upper_index = 3

        # build batch with range delete from lower_index(included) to upper_index
        # (not included) from data list
        query = f""" BEGIN BATCH DELETE FROM ks.test1 where pk={data[0][0]} and ck < '{data[upper_index][1]}' and ck >= '{data[lower_index][1]}';
            """
        # generate new data row
        current_date = datetime.now()
        for i in range(lower_index, upper_index):
            data[i][1] = (current_date + timedelta(days=i + num_rows)).strftime("%Y-%m-%d")
            data[i][2] = i * num_rows
        # add into batch conditional insert operation
        for i in range(lower_index, upper_index):
            query += f" INSERT INTO ks.test1 (pk, ck, v1) VALUES ({data[0][0]}, '{data[i][1]}', {data[i][2]}) IF NOT EXISTS;"
        query += "APPLY BATCH;"
        logger.info(query)
        # execute batch and verify it is applied
        assert_all(session, query, expected=[[True, None, None, None], [True, None, None, None], [True, None, None, None]], cl=ConsistencyLevel.QUORUM)

        assert_all(session=session, query=select_query, expected=data, cl=ConsistencyLevel.QUORUM, ignore_order=True)

    @pytest.mark.lwt
    def test_delete_by_1ck_range_conditional_batch_update(self):
        """if in batch at least one operation with IF, whole batch is conditional"""
        num_rows = 5
        lower_index = 1
        upper_index = 3
        session = self.prepare(nodes=3, rf=3)

        self.create_cf_1pk_1ck(session=session)
        data = self.insert_data_cf_1pk_1ck(session, num_rows)

        select_query = "SELECT pk, cast(ck as text), v1 FROM ks.test1"
        assert_all(session=session, query=select_query, expected=data, cl=ConsistencyLevel.QUORUM, ignore_order=True)

        lower_index = 1
        upper_index = 3

        # build batch with range delete from lower_index(included) to upper_index
        # (not included) from data list
        query = f""" BEGIN BATCH DELETE FROM ks.test1 where pk={data[0][0]} and ck < '{data[upper_index][1]}' and ck >= '{data[lower_index][1]}';
            """

        # add into batch conditional insert operation
        for i in range(upper_index, num_rows):
            query += f" UPDATE ks.test1 SET v1 = {data[i][2] * num_rows} WHERE pk = {data[0][0]} and ck = '{data[i][1]}' IF v1 = {data[i][2]};"
        query += "APPLY BATCH;"
        logger.info(query)
        # execute batch and verify it is applied
        conditinal_batch_result = [[True] + [None] * len(data[upper_index])]  # result row for the first DELETE
        for row in data[upper_index:num_rows]:
            batch_row = row[:]
            batch_row[1] = datetime.strptime(batch_row[1], "%Y-%m-%d").date()
            batch_row.insert(0, True)
            conditinal_batch_result.append(batch_row)

        assert_all(session, query, expected=conditinal_batch_result, cl=ConsistencyLevel.QUORUM)

        for row in data[upper_index:num_rows]:
            row[2] *= num_rows

        del data[lower_index:upper_index]

        assert_all(session=session, query=select_query, expected=data, cl=ConsistencyLevel.QUORUM, ignore_order=True)
