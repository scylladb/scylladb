import glob
import logging
import os
import pprint
import re
import time
from concurrent.futures.thread import ThreadPoolExecutor
from random import randrange

import pytest
from cassandra.concurrent import execute_concurrent
from ccmlib.scylla_cluster import ScyllaCluster

from dtest_class import Tester, create_ks
from tools.assertions import assert_row_count_in_select
from tools.cluster import new_node
from tools.cluster_topology import generate_cluster_topology
from tools.marks import unmark
from tools.retrying import retrying
from tools.tables_view_manager import index_is_built

logger = logging.getLogger(__name__)


def wait(delay=2):
    """
    An abstraction so that the sleep delays can easily be modified.
    """
    time.sleep(delay)


@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestConcurrentSchemaChanges(Tester):
    @pytest.fixture(scope="function", autouse=True)
    def fixture_set_cluster_settings(self, fixture_dtest_setup):
        fixture_dtest_setup.cluster.set_configuration_options({"start_rpc": "true"})

    def prepare_for_changes(self, session, namespace="ns1", rf=2):
        """
        prepares for schema changes by creating a keyspace and column family.
        """
        logger.debug("prepare_for_changes() " + str(namespace))
        # create a keyspace that will be used
        create_ks(session, "ks_%s" % namespace, rf)
        session.execute("USE ks_%s" % namespace)

        # create a column family with an index and a row of data
        query = (
            """
            CREATE TABLE cf_%s (
                col1 text PRIMARY KEY,
                col2 text,
                col3 text
            );
        """
            % namespace
        )
        session.execute(query)
        wait(1)
        session.execute("INSERT INTO cf_%s (col1, col2, col3) VALUES ('a', 'b', 'c');" % namespace)

        # create an index
        session.execute(f"CREATE INDEX index_{namespace} ON cf_{namespace}(col2)")

        # create a column family that can be deleted later.
        query = (
            """
            CREATE TABLE cf2_%s (
                col1 uuid PRIMARY KEY,
                col2 text,
                col3 text
            );
        """
            % namespace
        )
        session.execute(query)

        # make a keyspace that can be deleted
        create_ks(session, "ks2_%s" % namespace, rf)

    def make_schema_changes(self, session, namespace="ns1", rf=2):
        """
        makes a heap of changes.

        create keyspace
        drop keyspace
        create column family
        drop column family
        update column family
        # drop index
        # create index (modify column family and add a key)
        # rebuild index (via jmx)
        set default_validation_class
        """
        logger.debug("make_schema_changes() " + str(namespace))
        session.execute("USE ks_%s" % namespace)
        # drop keyspace
        session.execute("DROP KEYSPACE ks2_%s" % namespace)
        wait(2)

        # create keyspace
        create_ks(session, "ks3_%s" % namespace, rf)
        session.execute("USE ks_%s" % namespace)

        wait(2)
        # drop column family
        session.execute("DROP COLUMNFAMILY cf2_%s" % namespace)

        # create column family
        query = """
            CREATE TABLE cf3_%s (
                col1 uuid PRIMARY KEY,
                col2 text,
                col3 text,
                col4 text
            );
        """ % (namespace)
        session.execute(query)

        # alter column family
        query = (
            """
           ALTER COLUMNFAMILY cf_%s
           ADD col4 text;
        """
            % namespace
        )
        session.execute(query)

        # add index
        session.execute(f"CREATE INDEX index2_{namespace} ON cf_{namespace}(col3)")

        # remove an index
        session.execute("DROP INDEX index_%s" % namespace)

    def validate_schema_consistent(self, node, num_attempts=None):
        """Makes sure that there is only one schema"""

        if num_attempts is None:
            num_attempts = 180 if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode == "debug" else 60

        @retrying(num_attempts=num_attempts, sleep_time=1)
        def __validate_schema():
            response = node.nodetool("describecluster", True)[0]
            schemas = response.split("Schema versions:")[1].strip()
            num_schemas = len(re.findall(r"\[.*?\]", schemas))
            assert num_schemas == 1, "There were multiple schema versions: " + pprint.pformat(schemas)

        __validate_schema()

    def test_create_lots_of_tables_concurrently(self):
        """
        create tables across multiple threads concurrently
        """
        cluster = self.cluster
        cluster.populate(3).start()

        node1, node2, node3 = cluster.nodelist()
        session = self.cql_connection(node1)
        session.execute("create keyspace lots_o_tables WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};")
        session.execute("use lots_o_tables")

        cmds = [(f"create table t_{n} (id uuid primary key, c1 text, c2 text, c3 text, c4 text)", ()) for n in range(250)]
        results = execute_concurrent(session, cmds, raise_on_first_error=True, concurrency=200)

        for success, result in results:
            assert success, f"didn't get success on table create: {result}"

        self.validate_schema_consistent(node1)
        self.validate_schema_consistent(node2, num_attempts=1)
        self.validate_schema_consistent(node3, num_attempts=1)

        session.cluster.refresh_schema_metadata()
        table_meta = session.cluster.metadata.keyspaces["lots_o_tables"].tables
        assert 250 == len(table_meta), f"expected 250, got len(table_meta)={len(table_meta)} "

    def test_create_lots_of_alters_concurrently(self):
        """
        create alters across multiple threads concurrently
        """
        cluster = self.cluster
        cluster.populate(3).start()

        node1, node2, node3 = cluster.nodelist()
        session = self.cql_connection(node1)
        session.execute("create keyspace lots_o_alters WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};")
        session.execute("use lots_o_alters")
        for n in range(10):
            session.execute(f"create table base_{n} (id uuid primary key)")
        wait(5)

        cmds = [(f"alter table base_{randrange(0, 10)} add c_{n} int", ()) for n in range(500)]

        logger.debug("executing 500 alters")
        results = execute_concurrent(session, cmds, raise_on_first_error=True, concurrency=150)

        for success, result in results:
            assert success, f"didn't get success on table create: {result}"

        logger.debug("waiting for alters to propagate")
        wait(30)

        session.cluster.refresh_schema_metadata()
        table_meta = session.cluster.metadata.keyspaces["lots_o_alters"].tables
        column_ct = sum([len(table.columns) for table in table_meta.values()])

        # primary key + alters
        assert 510 == column_ct, f"expected 510, column_ct = {column_ct}"
        self.validate_schema_consistent(node1)
        self.validate_schema_consistent(node2, num_attempts=1)
        self.validate_schema_consistent(node3, num_attempts=1)

    @pytest.mark.required_features("!tablets")  # Due to https://github.com/scylladb/scylladb/issues/17603
    def test_create_lots_of_indexes_concurrently(self, fixture_dtest_setup):
        """
        create indexes across multiple threads concurrently
        """
        cluster = self.cluster
        cluster.populate(2).start()

        node1, node2 = cluster.nodelist()
        session = self.cql_connection(node1)
        session.execute("create keyspace lots_o_indexes WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};")
        session.execute("use lots_o_indexes")
        for n in range(5):
            session.execute(f"create table base_{n} (id uuid primary key, c1 int, c2 int)")
            for ins in range(1000):
                session.execute(f"insert into base_{n} (id, c1, c2) values (uuid(), {ins}, {ins})")
        wait(5)

        fixture_dtest_setup.ignore_log_patterns += [
            r"view - (\(rate limiting dropped [0-9]+ similar messages\) )?Error applying view update to .*: exceptions::mutation_write_failure_exception",
            r"view - (\(rate limiting dropped [0-9]+ similar messages\) )?Failed to apply mutation from .*: data_dictionary::no_such_column_family",
        ]

        logger.debug("creating indexes")
        cmds = []
        for n in range(5):
            cmds.append((f"create index ix_base_{n}_c1 on base_{n} (c1)", ()))
            cmds.append((f"create index ix_base_{n}_c2 on base_{n} (c2)", ()))

        results = execute_concurrent(session, cmds, raise_on_first_error=True)

        for success, result in results:
            assert success, f"didn't get success on table create: {result}"

        wait(5)

        logger.debug("validating schema and index list")
        session.cluster.control_connection.wait_for_schema_agreement()
        session.cluster.refresh_schema_metadata()
        index_meta = session.cluster.metadata.keyspaces["lots_o_indexes"].indexes
        self.validate_schema_consistent(node1)
        self.validate_schema_consistent(node2, num_attempts=1)
        assert 10 == len(index_meta), f"expect 10 , got len(index_meta)={len(index_meta)}"
        for n in range(5):
            assert f"ix_base_{n}_c1" in index_meta, f"ix_base_{n}_c1 not found in {index_meta}"
            assert f"ix_base_{n}_c2" in index_meta, f"ix_base_{n}_c2 not found in in {index_meta}"

        logger.debug("waiting for indexes to fill in")
        wait(45)
        logger.debug("querying all values by secondary index")
        for n in range(5):
            for ins in range(1000):
                assert_row_count_in_select(session, f"select * from base_{n} where c1 = {ins}", 1)
                assert_row_count_in_select(session, f"select * from base_{n} where c2 = {ins}", 1)

    @unmark.next_gating  # https://github.com/scylladb/scylladb/issues/14934
    def test_create_lots_of_mv_concurrently(self):
        """
        create materialized views across multiple threads concurrently
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, _node2, _node3 = cluster.nodelist()
        session = self.cql_connection(node1)
        session.execute("create keyspace lots_o_views WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};")
        session.execute("use lots_o_views")
        wait(10)
        session.execute("create table source_data (id uuid primary key, c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int, c9 int, c10 int);")
        insert_stmt = session.prepare("insert into source_data (id, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10) values (uuid(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);")
        wait(10)
        for n in range(4000):
            session.execute(insert_stmt, [n] * 10)

        wait(10)
        for n in range(1, 11):
            session.execute(f"CREATE MATERIALIZED VIEW src_by_c{n} AS SELECT * FROM source_data WHERE c{n} IS NOT NULL AND id IS NOT NULL PRIMARY KEY (c{n}, id)")
            session.cluster.control_connection.wait_for_schema_agreement()

        logger.debug("waiting for indexes to fill in")
        wait(60)
        # Error from server: code=0000 [Server error] message="Not implemented: INDEXES"
        result = list(session.execute("SELECT * FROM system_schema.views WHERE keyspace_name='lots_o_views' AND base_table_name='source_data' ALLOW FILTERING"))
        assert 10 == len(result), f"missing some mv from source_data table, expected 10, got len(result)={len(result)}"

        for n in range(1, 11):
            result = list(session.execute(f"select * from src_by_c{n}"))
            result_count = len(result)
            assert 4000 == result_count, f"expect 4000, got len(result)={result_count}"

    def _do_lots_of_schema_actions(self, session):
        for n in range(20):
            session.execute(f"create table alter_me_{n} (id uuid primary key, s1 int, s2 int, s3 int, s4 int, s5 int, s6 int, s7 int);")
            session.execute(f"create table index_me_{n} (id uuid primary key, c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int);")

        wait(10)
        cmds = []
        for n in range(20):
            cmds.append((f"create table new_table_{n} (id uuid primary key, c1 int, c2 int, c3 int, c4 int);", ()))
            for a in range(1, 8):
                cmds.append((f"alter table alter_me_{n} drop s{a};", ()))
                cmds.append((f"alter table alter_me_{n} add c{a} int;", ()))
                cmds.append((f"create index ix_index_me_{n}_c{a} on index_me_{n} (c{a});", ()))

        results = execute_concurrent(session, cmds, concurrency=100, raise_on_first_error=True)
        for success, result in results:
            assert success, f"didn't get success: {result}"

    def _verify_lots_of_schema_actions(self, session):
        session.cluster.control_connection.wait_for_schema_agreement()

        # the above should guarantee this -- but to be sure
        node1, node2, node3 = self.cluster.nodelist()
        self.validate_schema_consistent(node1, num_attempts=120)
        self.validate_schema_consistent(node2, num_attempts=1)
        self.validate_schema_consistent(node3, num_attempts=1)

        session.cluster.refresh_schema_metadata()
        table_meta = session.cluster.metadata.keyspaces["lots_o_churn"].tables
        errors = []
        for n in range(20):
            assert f"new_table_{n}" in table_meta

        #            if 7 != len(table_meta["index_me_{0}".format(n)].indexes):
        #                errors.append("index_me_{0} expected indexes ix_index_me_c0->7, got: {1}".format(n, sorted(list(table_meta["index_me_{0}".format(n)].indexes))))
        #            altered = table_meta["alter_me_{0}".format(n)]
        #            for col in altered.columns:
        #                if not col.startswith("c") and col != "id":
        #                    errors.append("alter_me_{0} column[{1}] does not start with c and should have been dropped: {2}".format(n, col, sorted(list(altered.columns))))
        #            if 8 != len(altered.columns):
        #                errors.append("alter_me_{0} expected c1 -> c7, id, got: {1}".format(n, sorted(list(altered.columns))))

        assert 0 == len(errors), "\n".join(errors)

    # Reason to exclude from next_gating: the test has failed runs in enterprise daily job
    @unmark.next_gating
    def test_create_lots_of_schema_churn(self):
        """
        create tables, indexes, alters across multiple threads concurrently
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, _node2, _node3 = cluster.nodelist()
        session = self.cql_connection(node1)
        session.execute("create keyspace lots_o_churn WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};")
        session.execute("use lots_o_churn")

        self._do_lots_of_schema_actions(session)
        self._verify_lots_of_schema_actions(session)

    # Reason to exclude from next_gating: the test has failed runs in enterprise daily jobs
    @unmark.next_gating  # https://github.com/scylladb/scylla-enterprise/issues/3231
    def test_create_lots_of_schema_churn_with_node_down(self, fixture_dtest_setup):
        """
        create tables, indexes, alters across multiple threads concurrently with a node down
        """
        cluster = self.cluster

        cluster.populate(3).start()
        node1, node2, _node3 = cluster.nodelist()
        session = self.cql_connection(node1)
        session.execute("create keyspace lots_o_churn WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};")
        session.execute("use lots_o_churn")

        node2.stop()
        self._do_lots_of_schema_actions(session)
        # workaround for issue scylladb/scylladb#20588
        fixture_dtest_setup.ignore_log_patterns += [
            r"Transferring snapshot.*connection is closed",
            r"ignore outdated snapshot",
            r"Snapshot application aborted",
            r"Abort requested while transferring snapshot",
        ]
        node2.start(wait_other_notice=True)
        self._verify_lots_of_schema_actions(session)

    def test_basic(self):
        """
        make several schema changes on the same node.
        """
        logger.debug("basic_test()")

        cluster = self.cluster
        cluster_topology = generate_cluster_topology(rack_num=2)
        cluster.populate(cluster_topology).start()

        node1 = cluster.nodelist()[0]
        wait(2)
        session = self.cql_connection(node1)

        self.prepare_for_changes(session, namespace="ns1")

        self.make_schema_changes(session, namespace="ns1")

    @pytest.mark.dtest_debug
    def test_changes_to_different_nodes(self):
        logger.debug("changes_to_different_nodes_test()")
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(rack_num=2)
        cluster.populate(cluster_topology).start()
        node1, node2 = cluster.nodelist()
        wait(2)
        session = self.cql_connection(node1)
        self.prepare_for_changes(session, namespace="ns1")
        self.make_schema_changes(session, namespace="ns1")
        wait(3)
        self.validate_schema_consistent(node1)

        # wait for changes to get to the first node
        wait(20)

        session = self.cql_connection(node2)
        self.prepare_for_changes(session, namespace="ns2")
        self.make_schema_changes(session, namespace="ns2")
        wait(3)
        self.validate_schema_consistent(node1)
        # check both, just because we can
        self.validate_schema_consistent(node2, num_attempts=1)

    def test_changes_while_node_down(self, fixture_dtest_setup):
        """
        makes schema changes while a node is down.
        Make schema changes to node 1 while node 2 is down.
        Then bring up 2 and make sure it gets the changes.
        """
        logger.debug("changes_while_node_down_test()")
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(rack_num=3)
        cluster.populate(cluster_topology).start(wait_other_notice=True, wait_for_binary_proto=True)
        node1, node2, _node3 = cluster.nodelist()
        session = self.patient_exclusive_cql_connection(node1)

        node2.stop(wait_other_notice=True)
        fixture_dtest_setup.ignore_log_patterns += [r"Column .* in view .* was not found in the base table"]
        self.prepare_for_changes(session, namespace="ns1", rf=3)
        self.make_schema_changes(session, namespace="ns1", rf=3)

        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        self.validate_schema_consistent(node2)

        assert index_is_built(self.cluster, session, ks_name=f"ks_ns1", table_name=f"cf_ns1", index_name=f"index2_ns1")

    def test_changes_while_node_toggle(self, fixture_dtest_setup):
        """
        makes schema changes while a node is down.

        Bring down 1 and change 2.
        Bring down 2, bring up 1, and finally bring up 2.
        1 should get the changes.
        """
        logger.debug("changes_while_node_toggle_test()")
        cluster = self.cluster

        cluster_topology = generate_cluster_topology(rack_num=3)
        cluster.populate(cluster_topology).start(wait_other_notice=True, wait_for_binary_proto=True)
        node1, node2, node3 = cluster.nodelist()
        session = self.patient_exclusive_cql_connection(node2)

        self.prepare_for_changes(session, namespace="ns2", rf=3)
        node1.stop(wait_other_notice=True)

        fixture_dtest_setup.ignore_log_patterns += [r"Column .* in view .* was not found in the base table"]
        self.make_schema_changes(session, namespace="ns2", rf=3)

        node2.stop(wait_other_notice=True)
        node3.stop(wait_other_notice=True)

        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        self.validate_schema_consistent(node1)

    def test_decommission_node(self):
        logger.debug("decommission_node_test()")
        cluster = self.cluster

        initial_nodes = 2 if "tablets" in self.scylla_features else 1

        cluster_topology = generate_cluster_topology(rack_num=initial_nodes)
        cluster.populate(cluster_topology)
        # create and add a new node, I must not be a seed, otherwise
        # we get schema disagreement issues for awhile after decommissioning it.
        # adding to the rack1 to not increase the number of racks (expected 2 for tablets)
        new_node(cluster, data_center="datacenter1", rack="rack1")

        cluster.start()
        node1 = cluster.nodelist()[0]
        node_to_decommission = cluster.nodelist()[-1]

        session = self.patient_cql_connection(node1)
        self.prepare_for_changes(session)

        node_to_decommission.decommission()

        self.validate_schema_consistent(node1)
        self.make_schema_changes(session, namespace="ns1")

        # create and add a new node
        # using rack2 to not increase the number of racks (expected 2 for tablets)
        # in particular, we had only one server in rack2 so it is possible that only rack1 nodes left when one
        # server is decommissioned, therefore adding another node to rack2 to make sure we have at least one
        # node in both rack1 and rack2
        added_node = new_node(cluster, data_center="datacenter1", rack="rack2")
        added_node.start(wait_other_notice=True, wait_for_binary_proto=True)

        self.validate_schema_consistent(node1)

    @pytest.mark.dtest_debug
    def test_snapshot(self):
        logger.debug("snapshot_test()")
        cluster = self.cluster
        cluster_topology = generate_cluster_topology(rack_num=2)
        cluster.populate(cluster_topology).start()
        node1, node2 = cluster.nodelist()
        wait(2)
        session = self.cql_connection(node1)
        self.prepare_for_changes(session, namespace="ns2")

        wait(2)
        cluster.flush()

        wait(2)
        node1.nodetool("snapshot -t testsnapshot")
        node2.nodetool("snapshot -t testsnapshot")

        wait(2)
        self.make_schema_changes(session, namespace="ns2")

        wait(2)

        cluster.stop()

        # restore the snapshots
        # clear the commitlogs and data
        dirs = (
            "%s/commitlogs" % node1.get_path(),
            "%s/commitlogs" % node2.get_path(),
            "%s/data/ks_ns2/cf_*/*" % node1.get_path(),
            "%s/data/ks_ns2/cf_*/*" % node2.get_path(),
        )
        for dirr in dirs:
            for f in glob.glob(os.path.join(dirr)):
                if os.path.isfile(f):
                    os.unlink(f)

        # copy the snapshot. TODO: This could be replaced with the creation of hard links.
        os.system(f"cp -p {node1.get_path()}/data/ks_ns2/cf_*/snapshots/testsnapshot/* {node1.get_path()}/data/ks_ns2/cf_*/")
        os.system(f"cp -p {node2.get_path()}/data/ks_ns2/cf_*/snapshots/testsnapshot/* {node2.get_path()}/data/ks_ns2/cf_*/")

        # restart the cluster
        cluster.start()

        wait(2)
        self.validate_schema_consistent(node1)

    @pytest.mark.single_node
    @pytest.mark.use_cassandra_stress
    def test_load(self):
        """
        apply schema changes while the cluster is under load.
        """
        logger.debug("load_test()")

        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        wait(2)
        session = self.cql_connection(node1)

        def stress(args=None):
            if args is None:
                args = []
            logger.debug("Stressing")
            node1.stress(args)
            logger.debug("Done Stressing")

        def compact():
            logger.debug("Compacting...")
            node1.nodetool("compact")
            logger.debug("Done Compacting.")

        # put some data into the cluster
        stress(["write", "n=30000", "-rate", "threads=8"])

        # now start stressing and compacting at the same time
        executor = ThreadPoolExecutor(max_workers=1)

        tcompact = executor.submit(compact)
        wait(1)

        # now the cluster is under a lot of load. Make some schema changes.
        session.execute("USE keyspace1")
        wait(1)
        session.execute("DROP TABLE standard1")
        wait(3)
        session.execute("CREATE TABLE standard1 (KEY text PRIMARY KEY)")

        tcompact.result()
