import logging
import os
import sys

import pytest
from cassandra.protocol import ConfigurationException
from cassandra.query import UNSET_VALUE

from dtest_class import Tester, create_ks
from tools.cluster_topology import generate_cluster_topology
from tools.data import rows_to_list
from tools.marks import issue_open, with_feature

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.next_gating


@pytest.mark.skip_if(with_feature("tablets") & issue_open("#18180"))
@pytest.mark.dtest_full
class TestCounters(Tester):
    @pytest.mark.single_node
    def test_int_rollover(self):
        """
        currently the counter will rollover when it reaches to MAX_INT.
        https://github.com/scylladb/scylla/issues/2225 (WONTFIX)
        Expected result: rollover
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={"cache_hit_rate_read_balancing": False})

        cluster.populate(1).start()
        (node1,) = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, "counter_tests", 1)

        session.execute("CREATE TABLE counter_bug (t int, c counter, primary key(t))")

        logger.debug("Created counter table, try to update one counter to MAX_INT")
        session.execute("UPDATE counter_bug SET c = c + %s where t = 0" % sys.maxsize)
        res = session.execute("SELECT * from counter_bug")
        rows = rows_to_list(res)
        assert len(rows) == 1
        logger.debug(rows)
        assert rows == [[0, sys.maxsize]], "Failed to update counter to MAX_INT"

        logger.debug("Update the counter to make it rollover")
        session.execute("UPDATE counter_bug SET c = c + 1 where t = 0")
        res = session.execute("SELECT * from counter_bug")
        rows = rows_to_list(res)
        assert len(rows) == 1
        logger.debug(rows)
        assert rows == [[0, -sys.maxsize - 1]], "Int counter isn't rollover"

        logger.debug("Update the counter to make it recover")
        session.execute("UPDATE counter_bug SET c = c - 1 where t = 0")
        res = session.execute("SELECT * from counter_bug")
        rows = rows_to_list(res)
        assert len(rows) == 1
        logger.debug(rows)
        assert rows == [[0, sys.maxsize]], "Int counter isn't recovered"

    @pytest.mark.single_node
    def test_prepare_unset_value(self):
        """
        Try to update counter with UNSET_VALUE
        Expected result: nothing is changed
        """
        cluster = self.cluster

        cluster.set_configuration_options(values={"cache_hit_rate_read_balancing": False})

        cluster.populate(1).start()
        (node1,) = cluster.nodelist()
        # protocol version >= 4
        session = self.patient_cql_connection(node1, protocol_version=4)
        create_ks(session, "counter_tests", 1)

        session.execute("CREATE TABLE counter_bug (t int, c counter, primary key(t))")

        logger.debug("Created counter table, try to update one counter")
        session.execute("UPDATE counter_bug SET c = c + 1 where t = 0")
        res = session.execute("SELECT * from counter_bug")
        rows = rows_to_list(res)
        assert len(rows) == 1
        assert rows == [[0, 1]]

        keys_num = 1000
        logger.debug("Update %s counters with UNSET_VALUE by prepare statement" % keys_num)

        for key in range(keys_num):
            statement = session.prepare("update counter_tests.counter_bug set c = c + ? where t = ?")
            session.execute(statement.bind((UNSET_VALUE, key)))

        res = session.execute("SELECT * from counter_bug")
        rows = rows_to_list(res)
        logger.debug(rows)
        assert len(rows) == 1, "Update with UNSET_VALUE unexpectedly changed number of counters"
        assert rows == [[0, 1]], "Update with UNSET_VALUE unexpectedly changed value of first counter"
        logger.debug("Verified that all counters aren't updated by UNSET_VALUE")

    @pytest.mark.single_node
    def test_alter_non_counter_with_counter(self):
        """
        ALTER table with counter, should fail with configuration error
        and shouldn't crash

        Reproducer for:
        https://github.com/scylladb/scylla/issues/7065

        Fix:
        https://github.com/scylladb/scylla/commit/1c29f0a43d00028d728068e9e194e00ca5ec7b67
        """
        cluster = self.cluster

        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        create_ks(session, "counter_tests", 1)

        session.execute(
            """
            CREATE TABLE non_counter (
                a text,
                b text,
                PRIMARY KEY (a, b))
                WITH CLUSTERING ORDER BY (b ASC);
            """
        )
        with pytest.raises(ConfigurationException):
            session.execute(
                """
                ALTER TABLE non_counter ADD "c" counter;
            """
            )


@pytest.mark.dtest_full
@pytest.mark.skip_if(with_feature("tablets") & issue_open("#18180"))
class TestCountersStress(Tester):
    @pytest.fixture(scope="function", autouse=True)
    def setup(self):
        cluster = self.cluster

        cluster.set_configuration_options(values={"cache_hit_rate_read_balancing": False})
        cluster.populate(generate_cluster_topology(rack_num=2)).start(wait_other_notice=True, wait_for_binary_proto=True)
        self.node = cluster.nodelist()[0]
        self._op_cnt = 100000
        if hasattr(self.cluster, "scylla_mode") and self.cluster.scylla_mode == "debug":
            self._op_cnt //= 10

    @pytest.mark.use_cassandra_stress
    def test_counter_stress(self):
        """
        Run cassandra stress test with multiple concurrent updates/reads of counters
        Result: written/read count is as expected
        """
        session = self.patient_cql_connection(self.node)
        session.execute(
            """
            CREATE KEYSPACE keyspace1
            WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '2'} AND durable_writes = true;
        """
        )
        session.execute(
            """
            CREATE TABLE keyspace1.counter1 (
                key blob PRIMARY KEY,
                "C0" counter,
                "C1" counter,
                "C2" counter,
                "C3" counter,
                "C4" counter
            ) WITH comment = ''
                AND bloom_filter_fp_chance = 0.01
                AND caching = '{"keys":"ALL","rows_per_partition":"ALL"}'
                AND compaction = {'class': 'SizeTieredCompactionStrategy'}
                AND compression = {}
                AND dclocal_read_repair_chance = 0.1
                AND default_time_to_live = 0
                AND gc_grace_seconds = 864000
                AND max_index_interval = 2048
                AND memtable_flush_period_in_ms = 0
                AND min_index_interval = 128
                AND read_repair_chance = 0.0
                AND speculative_retry = '99.0PERCENTILE';
        """
        )

        logger.debug("Run stress counter_write")
        resp = self.node.stress_object(["counter_write", f"n={self._op_cnt}", "-rate", "threads=4"])
        if not resp or "total partitions:write" not in resp:
            raise Exception(f"Error running stress test: {resp}")
        assert resp["total partitions:write"] >= self._op_cnt
        logger.debug("Verifying data count")
        rows = rows_to_list(session.execute("SELECT count(*) FROM keyspace1.counter1;"))
        assert rows[0][0] == self._op_cnt

        logger.debug("Run stress counter_read")
        resp = self.node.stress_object(["counter_read", f"n={self._op_cnt}", "-rate", "threads=4"])
        if not resp or "total partitions:read" not in resp:
            raise Exception(f"Error running stress test: {resp}")
        assert resp["total partitions:read"] >= self._op_cnt

    @pytest.mark.use_cassandra_stress
    def test_counter_stress_user_profile(self):
        """
        Run cassandra stress test updates/reads of counters with user profile
        Result: able to work with custom columns table
        """
        profile_path = os.path.join(os.path.dirname(__file__), "test_data/c-s-profiles/cassandra-stress-custom-counters-1.yaml")

        logger.debug("Run stress update counters with user profile")
        resp = self.node.stress_object(["user", f"profile={profile_path}", "ops(insert=1)", f"n={self._op_cnt}", "-rate", "threads=4"])
        if not resp or "total partitions" not in resp:
            raise Exception(f"Error running stress test: {resp}")
        assert resp["total partitions"] >= self._op_cnt
        session = self.patient_cql_connection(self.node)
        logger.debug("Verifying data count")
        rows = rows_to_list(session.execute("SELECT count(*) FROM ks.counter_cf;"))
        assert rows[0][0] == self._op_cnt

        logger.debug("Run stress read counters with user profile")
        resp = self.node.stress_object(["user", f"profile={profile_path}", "ops(read1=1)", f"n={self._op_cnt}", "-rate", "threads=4"])
        if not resp or "total partitions" not in resp:
            raise Exception(f"Error running stress test: {resp}")
        assert resp["total partitions"] >= self._op_cnt
