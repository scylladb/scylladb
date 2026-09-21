import logging
import multiprocessing
import re
import time

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import Session
from cassandra.query import SimpleStatement
from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.scylla_node import ScyllaNode

from dtest_class import Tester, create_ks
from dtest_setup_overrides import DTestSetupOverrides
from tools.assertions import assert_none, assert_one, assert_two_queries_equal
from tools.data import rows_to_list
from tools.files import get_node_cf_dir, get_sstables_files
from tools.misc import ImmutableMapping, dump_sstables
from tools.tables_view_manager import MaterializedViewManager, TableManager

logger = logging.getLogger(__name__)

TESTED_STRATEGIES = ["LeveledCompactionStrategy", "SizeTieredCompactionStrategy", "TimeWindowCompactionStrategy", "IncrementalCompactionStrategy"]
MURMUR3 = 15

pytestmark = pytest.mark.dtest_full


class ReshardingBase(Tester):
    DEFAULT_MURMUR3_PARTITIONER = 12
    DEFAULT_NODES = 1
    MURMUR3_PARTITIONER_FOR_DECREASE = 10
    MURMUR3_PARTITIONER_FOR_INCREASE = 17

    @pytest.fixture(scope="function", autouse=True)
    def fixture_dtest_setup_overrides(self, dtest_config, node_count, compaction_strategy, murmur3):
        self.compaction_strategy = compaction_strategy or "LeveledCompactionStrategy"
        cpu_count = multiprocessing.cpu_count()
        assert cpu_count >= 4, "Resharding tests require a minimum of 4 cpus"
        self.smp = min(cpu_count // 2 + 1, 5)
        self.SMP_FOR_INCREASE = min(self.smp + 1, cpu_count, 9)
        self.SMP_FOR_DECREASE = max(2, self.smp // 2)

        self.murmur3 = murmur3 or self.DEFAULT_MURMUR3_PARTITIONER
        self.nodes = node_count or self.DEFAULT_NODES
        self.rf = 1 if self.nodes < 3 else 3
        self.mem = self.set_memory_param(self.smp)

    def prepare(self, configuration_options=None):
        if configuration_options is None:
            configuration_options = {}
        cluster = self.cluster
        cluster = cluster.populate(self.nodes)
        configuration_options.update({"murmur3_partitioner_ignore_msb_bits": self.murmur3})
        cluster.set_configuration_options(values=configuration_options)
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True, jvm_args=["--smp", str(self.smp), "--memory", self.mem])
        self.node = cluster.nodelist()[0]

    @staticmethod
    def set_memory_param(smp):
        return f"{512 * int(smp)}M"

    def _reload_with_resharding(self, murmur3=DEFAULT_MURMUR3_PARTITIONER, smp=None, ks="keyspace1", cf="standard1"):
        logger.debug(f"Reload node with resharding:\n CPU: from {self.smp} to {smp}\n murmur3 parameter: from {self.murmur3} to {murmur3}")
        smp = self.smp if not smp else smp
        self.node.stop(wait_other_notice=True)

        data_files_num_before = self._get_number_of_data_files(ks, cf)
        self.node.set_configuration_options(values={"murmur3_partitioner_ignore_msb_bits": murmur3})
        self.node.start(jvm_args=["--smp", str(smp), "--memory", self.set_memory_param(smp)], wait_other_notice=True, wait_for_binary_proto=True)
        logger.debug("Node has been started")
        return data_files_num_before

    def _get_number_of_data_files(self, ks="keyspace1", cf="standard1"):
        data_files = get_sstables_files(get_node_cf_dir(self.node, ks, cf), f_type="TOC")
        logger.debug(f"data files: {data_files}")
        return len(data_files)

    def _verify_number_of_data_files(
        self,
        data_files_num_before,
        reshard_to,
        actual_data_files_num=None,
        ks="keyspace1",
        cf="standard1",
    ):
        logger.debug("Verify number of data files")
        expected_num = data_files_num_before * reshard_to
        if not actual_data_files_num:
            actual_data_files_num = self._get_number_of_data_files(ks, cf)
        assert actual_data_files_num <= expected_num, (
            f"{actual_data_files_num} not less than or equal to {expected_num}. Data files amount after resharding should be not more then data files amount before resharding multiplying by {reshard_to}."
        )

    def _wait_for_resharding(self, timeout=60, reshard_found=False):
        """
        wait until there's no RESHARD listed in compactionstats
        sleep for more 5 seconds
        break if there's no RESHARD in compactionstats
        """
        logger.debug("Wait for re-sharding to be finished")
        patt = re.compile("RESHARD")
        to = 0
        sleep_time = 5
        m = False
        prev_out = []
        while to <= timeout:
            # Commented because of "compactionstats" does not recognize resharding if there are few rows
            # because of resharding is going fast and finishs before this function calls.
            # out, err = self.node.nodetool("compactionstats", capture_output=True)
            # m = patt.search(out)
            # Temporary solution while the "compactionstats" problem will be resolved
            out = self.node.grep_log("Reshard")
            m = False if not out else True
            if m and prev_out == [o[0] for o in out]:
                break
            prev_out = [o[0] for o in out]
            # END - Temporary solution while the "compactionstats" problem will be resolved
            # logger.debug(out)
            # if not to or to == timeout:
            #     logger.debug(out)
            if not m:
                if not timeout:
                    return reshard_found
                time.sleep(sleep_time)
                return self._wait_for_resharding(timeout=0, reshard_found=reshard_found)
            reshard_found = True
            time.sleep(sleep_time)
            to += sleep_time
        return reshard_found

    def _remove_existent_ks(self, session, keyspace_name):
        session.execute(f"DROP KEYSPACE IF EXISTS {keyspace_name}")

    def _run_stress(self, op_cnt, stress_cmd):
        res = self.node.stress_object(stress_cmd)
        if not isinstance(res, dict):
            raise Exception(f"Error running cassandra-stress: {res}")
        assert res["total errors"] == 0
        assert res["total partitions"] >= op_cnt

    def _verify_row_number(self, cf, expected_row_num, keyspace="keyspace1", consistency_level=ConsistencyLevel.ONE):
        session = self.patient_cql_connection(self.node)
        q = SimpleStatement(f"SELECT count(*) FROM {keyspace}.{cf}", consistency_level=consistency_level)
        logger.debug("Verifying number of rows")
        resp = session.execute(q, timeout=self.count_request_timeout)
        row_number = rows_to_list(resp)[0][0]
        logger.debug(f"number of rows: {row_number}")
        assert row_number == expected_row_num

    def _verify_data(self, op_cnt, stress_cmd):
        logger.debug("Read data")
        res = self.node.stress_object(stress_cmd)
        assert isinstance(res, dict), "failed to run stress test"
        assert res["total errors"] == 0

    def _resharding_basic(self, reshard_to, rows, murmur3):
        configuration_options = {"range_request_timeout_in_ms": self.count_request_timeout * 1000}
        self.prepare(configuration_options=configuration_options)
        logger.debug("Run stress test on node1")

        debug_mode = isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode == "debug"
        if debug_mode and rows > 1000:
            rows = 1000
        op_cnt = rows
        stress_cmd = ["write", f"n={op_cnt}", "no-warmup", "-schema", f"replication(factor={self.rf})", f"compaction(strategy={self.compaction_strategy},enabled=false)"]
        self._run_stress(op_cnt, stress_cmd)

        self._verify_row_number("standard1", op_cnt)

        data_files_num_before = self._reload_with_resharding(smp=reshard_to, murmur3=murmur3)

        data_files_num_during = self._get_number_of_data_files()

        res = self._wait_for_resharding()
        exp_res, msg = (False, "Unexpected re-sharding recognized") if reshard_to == self.smp and murmur3 == self.murmur3 else (True, "Failed to recognize re-sharding finish")
        assert res == exp_res, msg
        self.check_errors_all_nodes()

        # Verify data files number during resharding
        self._verify_number_of_data_files(data_files_num_before=data_files_num_before, reshard_to=reshard_to, actual_data_files_num=data_files_num_during)

        # Verify data files number after resharding and compaction
        self._verify_number_of_data_files(data_files_num_before=data_files_num_before, reshard_to=reshard_to)

        stress_cmd = ["read", f"n={op_cnt}", "no-warmup", "-errors ignore", "-rate", "threads=200"]
        self._verify_data(op_cnt, stress_cmd)
        self._verify_row_number("standard1", op_cnt)

        # Verify data files number after resharding and compaction
        self._verify_number_of_data_files(data_files_num_before=data_files_num_before, reshard_to=reshard_to)


@pytest.mark.dtest_full
@pytest.mark.single_node
@pytest.mark.parametrize("node_count,compaction_strategy,murmur3", [(1, strategy, MURMUR3) for strategy in TESTED_STRATEGIES])
@pytest.mark.required_features("!tablets")  # scylladb/scylladb#16739
@pytest.mark.use_cassandra_stress
class TestReshardingTombstonesSingleNode(Tester):
    SMP = 2
    NEW_SMP = 4
    keyspace = "ks1"
    table = "cf1"
    gc_grace_seconds = 10
    keys = 100
    compaction_strategy = "SizeTieredCompactionStrategy"

    @pytest.fixture(scope="function", autouse=True)
    def fixture_dtest_setup_overrides(self, dtest_config):
        dtest_setup_overrides = DTestSetupOverrides()
        dtest_setup_overrides.cluster_options = ImmutableMapping(
            {
                "logger_log_level": {"compaction": "debug"}  # so we see compaction start/end log messages
            }
        )
        return dtest_setup_overrides

    def prepare(self, nodes, wait_for_binary_proto=True, jvm_args=None, configuration_options=None):
        if configuration_options is None:
            configuration_options = {}
        configuration_options.update({"enable_sstable_key_validation": True})
        self.cluster.set_configuration_options(values=configuration_options)
        self.cluster.populate(nodes).start(wait_for_binary_proto=wait_for_binary_proto, jvm_args=jvm_args)
        node1: ScyllaNode = self.cluster.nodelist()[0]
        session: Session = self.patient_cql_connection(node1)
        create_ks(session, self.keyspace, nodes)
        logging.debug(f"Inserting {self.keys} keys with gc_grace_seconds={self.gc_grace_seconds}")
        session.execute(
            f"create table {self.keyspace}.{self.table} (key int PRIMARY KEY, val int) \
                        with compaction = {{'class':'{self.compaction_strategy}'}} and gc_grace_seconds = {self.gc_grace_seconds};"
        )

    @staticmethod
    def compactions_count(session, ks, cf):
        rows = session.execute(
            f"select count(*) from system.compaction_history \
                               where keyspace_name='{ks}' and columnfamily_name='{cf}' \
                               allow filtering"
        )
        return rows[0][0]

    @staticmethod
    def get_number_of_marked_to_delete(node, keyspace, table):
        jsoninfo = dump_sstables(node, keyspace, table)
        return sum("tombstone" in partition for partition in jsoninfo)

    def test_disable_tombstone_removal_during_reshard(self, node_count, compaction_strategy, murmur3):
        """
        Test that data is not resurected when shared sstables
        are used
        1. smp=2 create sstable A with 100 keys
        2. delete all keys
        3. wait past gc_preiod
        4. insert a key forcing flush multiple times till a compaction is triggered
        5. stop and start the node with smp=4
        7. check that not all tombstones were cleared after resharding the deletion markers still exist
        8. check that data was resurected and that some of the deletion markers still exist
        8. Run compaction
        9. check that no deletion marker is left and files have been removed
        """
        logging.debug(f"Start 1 node with {self.SMP} cpu")
        self.prepare(nodes=1, jvm_args=["--smp", f"{self.SMP}"])
        node1: ScyllaNode = self.cluster.nodelist()[0]
        session: Session = self.patient_cql_connection(node1)

        session.execute(f"alter table {self.keyspace}.{self.table} " + f"with compaction = {{'class':'{compaction_strategy}', 'enabled':'true'}}")

        for i in range(self.keys):
            session.execute(f"insert into {self.keyspace}.{self.table} (key, val) values ({i}, 1)")
        logging.debug("Flush sstables")
        node1.flush()

        # Delete all keys and flush to have table withexpired rows.
        logging.debug(f"Deleting {self.keys} keys")
        for i in range(self.keys):
            session.execute(f"delete from {self.keyspace}.{self.table} where key = {i}")
        node1.flush(self.keyspace, self.table)

        # we passed gc_period and force an update so that compaction will
        # be triggered on a single shard (removing data and tombstone)
        compactions_2 = compactions_1 = self.compactions_count(session, self.keyspace, self.table)
        logging.debug(f"Waiting gc_grace_seconds={self.gc_grace_seconds} to pass")
        time.sleep(self.gc_grace_seconds + 1)
        logging.debug("Inserting data and waiting for new compaction")
        while compactions_1 == compactions_2:
            session.execute(f"insert into {self.keyspace}.{self.table} (key, val) values ({self.keys + 1},1);")
            node1.flush(self.keyspace, self.table)
            compactions_2 = self.compactions_count(session, self.keyspace, self.table)
        node1.wait_for_compactions()
        compactions_2 = self.compactions_count(session, self.keyspace, self.table)

        num_compactions = compactions_2 - compactions_1
        logging.debug(f"{num_compactions} compaction(s) completed")

        session.execute(f"alter table {self.keyspace}.{self.table} " + f"with compaction = {{'class':'{compaction_strategy}', 'enabled':'false'}}")

        # Stop node and start with increased smp number
        logging.debug("Stopping node1")
        node1.stop(gently=False)

        # verify that only some deletion markers will be kept
        # and gc_period passed so some tombstones have been removed by compaction
        numfound = self.get_number_of_marked_to_delete(node1, self.keyspace, self.table)

        logging.debug("{} keys are now marked_deleted (0 {} expected < {})".format(numfound, "<" if num_compactions < 2 else "<=", self.keys))
        assert numfound > 0, "All tombstones were removed"

        logging.debug(f"Start node1 with {self.NEW_SMP} cpus")
        m = node1.mark_log()
        node1.start(wait_for_binary_proto=True, jvm_args=["--smp", f"{self.NEW_SMP}"])
        # validate that resharding for test keyspace was run
        node1.watch_log_for([rf"Resharded.*{self.keyspace}/{self.table}"], from_mark=m, timeout=60)

        session: Session = self.patient_cql_connection(node1, self.keyspace)

        # validate that not all deletion markers have been removed after resharding
        numfound = self.get_number_of_marked_to_delete(node1, self.keyspace, self.table)
        logging.debug(f"{numfound} keys are now marked_deleted (0 < expected < {self.keys})")
        assert numfound != 0, "All tombstones were removed during resharding"

        logging.debug("Verify that no data was resurrected")
        for x in range(self.keys):
            assert_none(session, f"select * from {self.keyspace}.{self.table} where key = {x}")

        logging.debug("Run compaction and validate that no tombstones are left")
        node1.compact()
        node1.wait_for_compactions()
        numfound = self.get_number_of_marked_to_delete(node1, self.keyspace, self.table)
        logging.debug(f"{numfound} keys are now marked_deleted (Excpecting 0)")
        assert numfound == 0, "All tombstones were not removed during resharding"


@pytest.mark.dtest_full
@pytest.mark.dtest_heavy
@pytest.mark.use_cassandra_stress
@pytest.mark.parametrize(
    "node_count,compaction_strategy,murmur3",
    [(node_count, strategy, MURMUR3) for node_count in [1, 4] for strategy in TESTED_STRATEGIES],
)
@pytest.mark.required_features("!tablets")  # scylladb/scylladb#16739
class TestReshardingVariants(ReshardingBase):
    def test_resharding_by_murmur3_increase(self, node_count, compaction_strategy, murmur3):
        """
        Resharding with 10M objects after increasing the MURMUR3 parameter
        and restarting the cluster
        """
        self._resharding_basic(self.smp, rows=1000, murmur3=self.MURMUR3_PARTITIONER_FOR_INCREASE)

    def test_resharding_by_murmur3_decrease(self, node_count, compaction_strategy, murmur3):
        """
        Resharding with 10M objects after decreasing the MURMUR3 parameter
        and restarting the cluster
        """
        self._resharding_basic(self.smp, rows=1000, murmur3=self.MURMUR3_PARTITIONER_FOR_DECREASE)

    def test_resharding_by_smp_increase(self, node_count, compaction_strategy, murmur3):
        """
        Resharding with 10M objects after increasing the SMP parameter
        and restarting the cluster
        """
        self._resharding_basic(self.SMP_FOR_INCREASE, rows=10000, murmur3=self.murmur3)

    def test_resharding_by_smp_decrease(self, node_count, compaction_strategy, murmur3):
        """
        Resharding with 10M objects after decreasing the SMP parameter
        and restarting the cluster
        """
        self._resharding_basic(self.SMP_FOR_DECREASE, rows=100000, murmur3=self.murmur3)

    def test_resharding_by_same_smp(self, node_count, compaction_strategy, murmur3):
        """
        Cluster with 10M objects. Both SMP and MURMUR3 parameter are not changed.
        No resharding expected
        """
        self._resharding_basic(self.smp, rows=1000, murmur3=self.murmur3)

    def test_resharding_by_murmur3_smp(self, node_count, compaction_strategy, murmur3):
        """
        Cluster with 10M objects. Both SMP and MURMUR3 parameter are changed
        and restarting the cluster
        """
        self._resharding_basic(self.SMP_FOR_INCREASE, rows=1000, murmur3=self.MURMUR3_PARTITIONER_FOR_INCREASE)

    def test_resharding_counter(self, node_count, compaction_strategy, murmur3):
        """
        Resharding with small counter data set(c-s 1M counter objects) after changing the parameter
        and restarting the cluster
        """
        configuration_options = {"range_request_timeout_in_ms": self.count_request_timeout * 1000}
        self.prepare(configuration_options=configuration_options)
        keyspace_name = "keyspace1"
        session = self.patient_cql_connection(self.node)
        # If test failed and re-run by @flaky decorator, the existent keyspace should be re-created
        self._remove_existent_ks(session=session, keyspace_name=keyspace_name)
        session.execute(
            """
            CREATE KEYSPACE %s
            WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '1'};
        """
            % keyspace_name
        )
        session.execute(
            """
            CREATE TABLE %s.counter1 (
                key blob PRIMARY KEY,
                "C0" counter,
                "C1" counter,
                "C2" counter,
                "C3" counter,
                "C4" counter
            ) WITH comment = ''
                AND bloom_filter_fp_chance = 0.01
                AND caching = '{"keys":"ALL","rows_per_partition":"ALL"}'
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
            % keyspace_name
        )

        logger.debug("Run counter_write stress test on node1")
        op_cnt = 10000
        stress_cmd = ["counter_write", f"n={op_cnt}", "no-warmup", "-schema", f"replication(factor={self.rf})", f"compaction(strategy={self.compaction_strategy})"]

        self._run_stress(op_cnt, stress_cmd)

        self._verify_row_number("counter1", op_cnt)

        data_files_num_before = self._reload_with_resharding(smp=self.SMP_FOR_INCREASE)

        self._verify_number_of_data_files(data_files_num_before=data_files_num_before, reshard_to=self.SMP_FOR_INCREASE)

        res = self._wait_for_resharding()
        assert res, "Failed to recognize re-sharding finish"

        self._verify_number_of_data_files(data_files_num_before=data_files_num_before, reshard_to=self.SMP_FOR_INCREASE)
        self.check_errors_all_nodes()

        stress_cmd = ["counter_read", f"n={op_cnt}", "no-warmup", "-rate", "threads=200"]
        self._verify_data(op_cnt, stress_cmd)
        self._verify_row_number("counter1", op_cnt)

    def test_resharding_mv(self, node_count, compaction_strategy, murmur3):
        """
        Resharding with small counter data set(c-s 1M counter objects) after changing the parameter
        and restarting the cluster
        """
        configuration_options = {"range_request_timeout_in_ms": self.count_request_timeout * 1000}
        self.prepare(configuration_options=configuration_options)
        session = self.patient_cql_connection(self.node)
        create_ks(session, "ks", self.rf)
        compaction = {"compaction": {"class": self.compaction_strategy}}
        op_cnt = 10000
        tm = TableManager(session, self.cluster, columns={"int": {"amount": 1, "frozen": False, "value length": {"min": 1, "max": 100}}}, pk_columns={}, cl_columns={}, table_options=compaction)
        tm.create_table()

        mv = MaterializedViewManager(tm)
        mv_pk_name = tm.column_names_list[-1]
        mv.create_materialized_view(mv_columns={"int": {"names": [mv_pk_name]}}, mv_pk_column={"type": "int"}, options=compaction)

        tm.prefill_table(op_cnt)

        self._verify_row_number(tm.table_name, op_cnt, keyspace=tm.keyspace)
        self._verify_row_number(mv.mv_name, op_cnt, keyspace=tm.keyspace)

        data_files_num_before = self._reload_with_resharding(smp=self.SMP_FOR_INCREASE, ks=tm.keyspace, cf=tm.table_name)

        self._verify_number_of_data_files(data_files_num_before=data_files_num_before, reshard_to=self.SMP_FOR_INCREASE, ks=tm.keyspace, cf=tm.table_name)

        res = self._wait_for_resharding()
        assert res, "Failed to recognize re-sharding finish"

        self._verify_number_of_data_files(data_files_num_before=data_files_num_before, reshard_to=self.SMP_FOR_INCREASE, ks=tm.keyspace, cf=tm.table_name)
        self.check_errors_all_nodes()

        # Read data
        session = self.patient_cql_connection(self.node)
        query = "select count(*) from {0}.{1} where id={2}"
        for i in range(op_cnt):
            assert_one(session, query.format(tm.keyspace, tm.table_name, i), [1])
            assert_one(session, query.format(tm.keyspace, mv.mv_name, i), [1])

        # Validate data
        self._verify_row_number(tm.table_name, op_cnt, keyspace=tm.keyspace)
        self._verify_row_number(mv.mv_name, op_cnt, keyspace=tm.keyspace)
        query = "select * from {0}.{1}"
        assert_two_queries_equal(
            session,
            query.format(tm.keyspace, tm.table_name),
            session,
            query.format(tm.keyspace, tm.table_name),
            consistency_level=ConsistencyLevel.ALL,
            session_timeout=120,
            group=True,
            groupby_column1=mv_pk_name,
            groupby_column2=mv_pk_name,
        )
