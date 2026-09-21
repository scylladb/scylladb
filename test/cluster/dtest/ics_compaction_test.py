import logging
import os
import random
import shutil
import string

import pytest

from dtest_class import Tester, create_cf, create_ks, wait_for
from tools.assertions import assert_row_count
from tools.cluster_topology import generate_cluster_topology_based_rf
from tools.files import get_node_cf_dir, get_sstables_files
from tools.marks import issue_open, unmark, unmark_if, with_feature
from tools.scylla_defines import (
    FULL_TABLE_NAME,
    KB,
    KEYSPACE_NAME,
    MB,
    TABLE_NAME,
    CompactionStrategy,
)
from tools.snapshots import (
    make_snapshot,
    restore_snapshot_with_refresh,
)
from tools.stress import assert_cs_success, format_cs_output

NUM_OF_NODES = 2
RF = NUM_OF_NODES
WRITE_SIZE_UNIT_IN_MB = 1
COLUMN_DEFAULT_SIZE = MB
NUM_OF_COLUMNS = 50
PARTITIONS = 100
ROWS_IN_PARTITION = 20
BIG_PARTITION_ROWS = 10000
NUM_OF_GENERATED_SSTABLES = 4
NUM_WRITES_PER_SSTABLE = 1
START_INDEX = 1
NON_ICS_STRATEGIES = [CompactionStrategy.SIZE_TIERED, CompactionStrategy.TIME_WINDOW, CompactionStrategy.LEVELED]

logger = logging.getLogger(__name__)


def create_table(  # noqa: PLR0913
    session,
    compaction_strategy=None,
    table_name=TABLE_NAME,
    keyspace_name=KEYSPACE_NAME,
    sstable_size_in_mb=None,
    is_large_partitions=False,
    compaction_additional_params: dict | None = None,
):
    """

    Creating a table, specifying a compaction strategy and its parameters.
    """
    session.execute(f"USE {keyspace_name}")
    if not is_large_partitions:
        query = f"""
            CREATE TABLE {table_name} (
                key blob PRIMARY KEY,
                "C0" blob
            )
        """
    else:
        query = "create table {} (pk int, ck int, {}, clist list<int>, cset set<text>, cmap map<int, text>, PRIMARY KEY(pk, ck))".format(table_name, ", ".join("c%d int" % i for i in range(1, NUM_OF_COLUMNS)))
    compaction_params = {"class": compaction_strategy._value_}
    if compaction_strategy in [CompactionStrategy.INCREMENTAL, CompactionStrategy.LEVELED]:
        compaction_params["sstable_size_in_mb"] = str(sstable_size_in_mb)
    if compaction_additional_params:
        compaction_params.update(compaction_additional_params)
    query += f" WITH compaction = {compaction_params}"

    logger.debug(f"query is:{query}")

    session.execute(query)


@pytest.mark.dtest_full
@pytest.mark.next_gating
@pytest.mark.use_cassandra_stress
class TestIcsCompaction(Tester):
    #######################   Helper Functions Start  ###########################################################################
    def alter_table_compaction(  # noqa: PLR0913
        self,
        compaction_strategy=None,
        table_name=TABLE_NAME,
        keyspace_name=KEYSPACE_NAME,
        sstable_size_in_mb=None,
        additional_compaction_params=None,
        assert_altered_compaction=False,
    ):
        """
        1. Alters table compaction like: ALTER TABLE mykeyspace.mytable WITH compaction = {'class' : 'IncrementalCompactionStrategy'}
        2. Can verify the new strategy is successfully applied to table.
        """

        base_query = f"ALTER TABLE {keyspace_name}.{table_name} WITH compaction = "
        dict_requested_compaction = {}
        if compaction_strategy:
            dict_requested_compaction["class"] = compaction_strategy._value_

        if sstable_size_in_mb and compaction_strategy in [CompactionStrategy.INCREMENTAL, CompactionStrategy.LEVELED]:
            dict_requested_compaction["sstable_size_in_mb"] = sstable_size_in_mb

        if additional_compaction_params:
            for param in additional_compaction_params:
                dict_requested_compaction.update(param)

        full_alter_query = base_query + str(dict_requested_compaction)
        logger.debug(f"query is: {full_alter_query}")
        self.execute_session_cql_query(query=full_alter_query)

        if assert_altered_compaction:
            assert self._get_table_compaction_strategy(keyspace_name=keyspace_name, table_name=table_name) == compaction_strategy

    def _get_table_compaction_strategy(self, table_name=TABLE_NAME, keyspace_name=KEYSPACE_NAME):
        verify_query = f"SELECT keyspace_name, table_name, compaction FROM system_schema.tables WHERE keyspace_name = '{keyspace_name}' AND table_name = '{table_name}'"
        result_matrix = list(self.execute_session_cql_query(query=verify_query))
        logger.debug(result_matrix)
        retrieved_compaction = "Unknown"
        if result_matrix[0].keyspace_name == keyspace_name and result_matrix[0].table_name == table_name:
            retrieved_compaction = CompactionStrategy.from_str(result_matrix[0].compaction["class"])
            logger.debug(f"Retrieved compaction strategy for {keyspace_name}.{table_name} is: {retrieved_compaction}")
        return retrieved_compaction

    def create_cluster(self, num_of_nodes, rf, configuration_options=None, jvm_args=None):
        if configuration_options is None:
            configuration_options = {}
        configuration_options["tablets_initial_scale_factor"] = 1  # Tests expect 1 sstable per shard
        if configuration_options:
            self.cluster.set_configuration_options(values=configuration_options)
        topology = generate_cluster_topology_based_rf(nodes=num_of_nodes, rf=rf)
        self.cluster.populate(topology).start(jvm_args=jvm_args, wait_for_binary_proto=True)
        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        return session

    def prepare(  # noqa: PLR0913
        self,
        num_of_nodes=NUM_OF_NODES,
        r_factor=RF,
        compaction_strategy=CompactionStrategy.SIZE_TIERED,
        table_name=TABLE_NAME,
        keyspace_name=KEYSPACE_NAME,
        sstable_size_in_mb=10,
        is_large_partitions=False,
        compaction_additional_params: dict | None = None,
        jvm_args=None,
    ):
        all_jvm_args = ["--compaction-enforce-min-threshold", "true"]
        if jvm_args:
            all_jvm_args += jvm_args
        session = self.create_cluster(num_of_nodes=num_of_nodes, rf=r_factor, jvm_args=all_jvm_args)
        create_ks(session=session, name=keyspace_name, rf=r_factor)
        create_table(
            session=session,
            compaction_strategy=compaction_strategy,
            keyspace_name=keyspace_name,
            table_name=table_name,
            sstable_size_in_mb=sstable_size_in_mb,
            is_large_partitions=is_large_partitions,
            compaction_additional_params=compaction_additional_params,
        )
        return session

    def execute_session_cql_query(self, query, node=None):
        node = node or self.cluster.nodelist()[0]
        with self.patient_cql_connection(node) as session:
            res = session.execute(query)
        return res

    def _count_table_entries(self, keyspace=KEYSPACE_NAME, table=TABLE_NAME):
        session = self.patient_cql_connection(self.cluster.nodelist()[0])
        res = session.execute(f"SELECT COUNT(*) FROM {keyspace}.{table}")
        count = res.current_rows[0].count
        logger.debug(f"Current count of DB entries: {count}")
        return count

    @staticmethod
    def _get_sstables_file_size_in_mb(list_sstable_files, cf_dir):
        logger.debug(f"Get sstables file size for: {list_sstable_files}")
        list_files_size = []
        for file in list_sstable_files:
            # A sample returned value of os.path.getsize is: '3165790' - which is converted to 3MB
            file_size_in_mb = os.path.getsize(os.path.join(cf_dir, file)) >> 20
            list_files_size.append(file_size_in_mb)
        return list_files_size

    def _check_sstable_file_size_limit(self, list_sstable_files, sstable_size_in_mb):
        logger.debug(f"Validating maximum sstable file size of {sstable_size_in_mb} for: {list_sstable_files}")
        cf_dir = get_node_cf_dir(self.cluster.nodelist()[0], ks_name=KEYSPACE_NAME, cf_name=TABLE_NAME)
        for file in list_sstable_files:
            file_size_in_mb = os.path.getsize(os.path.join(cf_dir, file)) >> 20
            assert file_size_in_mb <= sstable_size_in_mb + 1, f"File size is bigger than: {sstable_size_in_mb}"

    def _create_table_and_alter_compaction(self, original_compaction, new_compaction):
        self.prepare(num_of_nodes=1, r_factor=1, compaction_strategy=original_compaction)

        list_additional_params = [{"bucket_high": 1.2}, {"bucket_low": 0.8}, {"min_sstable_size": 10}, {"min_threshold": 2}, {"max_threshold": 80}]
        node1 = self.cluster.nodelist()[0]
        self._write_and_flush_sstables(num_of_generated_sstables=5, start_index=1, increasing_write_size=True, num_writes_per_sstable=8)
        if new_compaction in [CompactionStrategy.INCREMENTAL, CompactionStrategy.SIZE_TIERED]:
            additional_compaction_params = list_additional_params
        else:
            additional_compaction_params = None
        self.alter_table_compaction(compaction_strategy=new_compaction, additional_compaction_params=additional_compaction_params, assert_altered_compaction=True)
        node1.wait_for_compactions()
        node1.compact()
        node1.wait_for_compactions()

    def _get_sstable_files_and_sizes(self):
        """
        Returns sstable files and their sizes for a table.
        :return: sstables_files, files_size
        """
        cf_dir = get_node_cf_dir(self.cluster.nodelist()[0], ks_name=KEYSPACE_NAME, cf_name=TABLE_NAME)
        sstables_files = get_sstables_files(cf_dir, f_type="Data")
        files_size = self._get_sstables_file_size_in_mb(list_sstable_files=sstables_files, cf_dir=cf_dir)
        logger.debug(f"Files sizes are: {files_size}")
        return sstables_files, files_size

    def _write_and_flush_sstables(  # noqa: PLR0913
        self,
        start_index=START_INDEX,
        num_of_generated_sstables=NUM_OF_GENERATED_SSTABLES,
        write_range=1,
        write_size_unit_in_mb=WRITE_SIZE_UNIT_IN_MB,
        increasing_write_size=False,
        num_writes_per_sstable=NUM_WRITES_PER_SSTABLE,
    ):
        self._read_or_write_and_flush_sstables(
            num_of_generated_sstables=num_of_generated_sstables,
            start_index=start_index,
            write_range=write_range,
            write_size_unit_in_mb=write_size_unit_in_mb,
            increasing_write_size=increasing_write_size,
            num_writes_per_sstable=num_writes_per_sstable,
            read_only=False,
        )

    def _read_generated_sstables_data(  # noqa: PLR0913
        self,
        start_index=START_INDEX,
        num_of_generated_sstables=NUM_OF_GENERATED_SSTABLES,
        write_range=1,
        write_size_unit_in_mb=WRITE_SIZE_UNIT_IN_MB,
        increasing_write_size=False,
        num_writes_per_sstable=NUM_WRITES_PER_SSTABLE,
    ):
        self._read_or_write_and_flush_sstables(
            num_of_generated_sstables=num_of_generated_sstables,
            start_index=start_index,
            write_range=write_range,
            write_size_unit_in_mb=write_size_unit_in_mb,
            increasing_write_size=increasing_write_size,
            num_writes_per_sstable=num_writes_per_sstable,
            read_only=True,
        )

    def _read_or_write_and_flush_sstables(  # noqa: PLR0913
        self,
        num_of_generated_sstables,
        start_index,
        write_range=1,
        write_size_unit_in_mb=WRITE_SIZE_UNIT_IN_MB,
        increasing_write_size=False,
        num_writes_per_sstable=NUM_WRITES_PER_SSTABLE,
        read_only=False,
    ):
        """
        Generates sstables of sizes: write_size_unit or write_size_unit...write_size_unit * num_of_generated_sstables
        """
        node1 = self.cluster.nodelist()[0]
        op_mode = "write" if not read_only else "read"
        for idx in range(1, num_of_generated_sstables + 1):
            write_size_unit_in_bytes = write_size_unit_in_mb * MB
            write_size = write_size_unit_in_bytes * idx if increasing_write_size else write_size_unit_in_bytes
            stress_params = [op_mode, "no-warmup", f"n={num_writes_per_sstable}", "-pop", f"seq={start_index}..{start_index + write_range}", "-col", "n=fixed(1)", f"size=fixed({write_size})", "-rate", "threads=1"]
            logger.debug(f"stress node1 #{idx}: ( {stress_params} )")
            results = node1.stress(stress_params)
            logger.debug("Stress results:\n" + format_cs_output(results))
            assert_cs_success(results)
            if not read_only:
                logger.debug(f"flush #{idx}")
                node1.flush()
                self._count_table_entries()
            start_index += write_range

    def _generate_cluster_with_table_data_snapshot(self, compaction_strategy, sstable_size_in_mb=2):
        """
        Base testing method:

        1. Create a keyspace and a table with chosen compaction strategy
        2. Generate sstables with rows
        4. Take a snapshot
        5. return the snapshot.

        """
        if compaction_strategy is not CompactionStrategy.TIME_WINDOW:
            session = self.prepare(num_of_nodes=1, r_factor=1, compaction_strategy=compaction_strategy, keyspace_name=KEYSPACE_NAME, table_name=TABLE_NAME, sstable_size_in_mb=sstable_size_in_mb)
        else:
            session = self.prepare(num_of_nodes=1, r_factor=1, compaction_strategy=compaction_strategy, keyspace_name=KEYSPACE_NAME, table_name=TABLE_NAME)
        node1 = self.cluster.nodelist()[0]
        self._write_and_flush_sstables(num_of_generated_sstables=NUM_OF_GENERATED_SSTABLES, start_index=1, increasing_write_size=True)
        node1.wait_for_compactions()
        snapshot_dir = make_snapshot(node1, KEYSPACE_NAME, TABLE_NAME, name="basic")
        return snapshot_dir, session, node1

    @staticmethod
    def insert_large_partitions_table_data(  # noqa: PLR0913
        session,
        partition_range_end,
        rows_in_partition,
        partition_range_start=1,
        table_name=TABLE_NAME,
        num_of_columns=NUM_OF_COLUMNS,
    ):
        logger.debug(f"Create {partition_range_end} partitions of {num_of_columns} columns with {rows_in_partition} rows")
        for i in range(partition_range_start, partition_range_end + 1):
            for k in range(1, rows_in_partition + 1):
                random_string = "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
                stmt = "insert into {table_name} (pk, ck, {columns}, clist, cset, cmap) values ({ilist}, {klist}, {int_values}, [{ilist}, {klist}], {open}{set_value}{close}, {map_value})".format(
                    table_name=table_name,
                    columns=", ".join("c%d" % l for l in range(1, num_of_columns)),
                    int_values=", ".join("%d" % l for l in range(1, num_of_columns)),
                    ilist=i,
                    klist=k,
                    open="{'",
                    set_value=random_string,
                    close="'}",
                    map_value="{%d: '%s'}" % (k, random_string),
                )
                session.execute(stmt)

    #######################   Helper Functions End  ###########################################################################

    @unmark.next_gating
    def test_check_default_compaction_strategy(self):
        session = self.create_cluster(num_of_nodes=1, rf=1)
        create_ks(session=session, name=KEYSPACE_NAME, rf=1)
        create_cf(session=session, name=TABLE_NAME, columns={"c1": "text", "c2": "text"}, compaction_strategy=None)
        compaction = self._get_table_compaction_strategy()
        assert compaction == CompactionStrategy.INCREMENTAL, f"Default compaction is: {compaction}"

    @pytest.mark.single_node
    @pytest.mark.parametrize(
        "first,second,third",
        [
            [CompactionStrategy.SIZE_TIERED, CompactionStrategy.LEVELED, CompactionStrategy.INCREMENTAL],
            [CompactionStrategy.SIZE_TIERED, CompactionStrategy.INCREMENTAL, CompactionStrategy.SIZE_TIERED],
        ],
    )
    def test_alter_table_strategy_twice(self, first, second, third):
        self._create_table_and_alter_compaction(original_compaction=first, new_compaction=second)
        self.alter_table_compaction(compaction_strategy=third, assert_altered_compaction=True)
        node1 = self.cluster.nodelist()[0]
        node1.compact()
        node1.wait_for_compactions()

    @pytest.mark.single_node
    @pytest.mark.parametrize("original_strategy", NON_ICS_STRATEGIES)
    def test_alter_table_to_ics(self, original_strategy):
        self._create_table_and_alter_compaction(original_compaction=original_strategy, new_compaction=CompactionStrategy.INCREMENTAL)

    @pytest.mark.single_node
    @pytest.mark.parametrize("new_strategy", NON_ICS_STRATEGIES)
    def test_alter_table_from_ics(self, new_strategy):
        self._create_table_and_alter_compaction(original_compaction=CompactionStrategy.INCREMENTAL, new_compaction=new_strategy)

    @pytest.mark.single_node
    def test_ics_snapshot_and_restore(self):
        """
        Base testing method:

        1. Create a keyspace and a table with chosen compaction strategy
        2. Generate sstables with rows
        4. Take a snapshot
        5. Generate more sstables with rows after the snapshot
        6. Drop the keyspace, assure we have no data after the deletion
        7. Restore the snapshot
        8. Verify we have the same num of rows prior to the snapshot.
        9. Rewrite some data to trigger regular compaction and wait for it.
        10. Verify that the compacted sstables conform the sstable_size_in_mb
            and their total size is as expected.
        """
        sstable_size_in_mb = 2
        snapshot_dir, session, node1 = self._generate_cluster_with_table_data_snapshot(compaction_strategy=CompactionStrategy.INCREMENTAL, sstable_size_in_mb=sstable_size_in_mb)

        # Write more data after the snapshot, this will get thrown away when we restore:
        self._write_and_flush_sstables(num_of_generated_sstables=2, start_index=NUM_OF_GENERATED_SSTABLES + 1)
        assert_row_count(session=session, table_name=FULL_TABLE_NAME, expected=NUM_OF_GENERATED_SSTABLES + 2)

        # Drop the keyspace, make sure we have no data:
        session.execute(f"DROP KEYSPACE {KEYSPACE_NAME}")
        shutil.rmtree(os.path.join(node1.get_path(), "data", KEYSPACE_NAME))

        create_ks(session, name=KEYSPACE_NAME, rf=1)
        create_table(session=session, compaction_strategy=CompactionStrategy.INCREMENTAL, keyspace_name=KEYSPACE_NAME, table_name=TABLE_NAME, sstable_size_in_mb=sstable_size_in_mb)

        assert_row_count(session=session, table_name=FULL_TABLE_NAME, expected=0)

        # Restore data from snapshot:
        restore_snapshot_with_refresh(snapshot_dir, node1, KEYSPACE_NAME, TABLE_NAME)
        # Check that the number of table entries on snapshot is restored.
        logger.debug("Verifying data")
        assert_row_count(session=session, table_name=FULL_TABLE_NAME, expected=NUM_OF_GENERATED_SSTABLES)

        logger.debug("Writing more data")
        self._write_and_flush_sstables(num_of_generated_sstables=1, start_index=1)
        node1.compact()
        logger.debug("Stopping node")
        node1.stop()
        _sstables_files1, files_size = self._get_sstable_files_and_sizes()
        for size in files_size:
            assert size >= sstable_size_in_mb, f"Found sstable smaller than {sstable_size_in_mb} MB. files_size: {files_size}"
        total_size = sum(files_size)
        expected_total_size = sum([1, 2, 3, 4])
        assert total_size == expected_total_size, f"Expected total sstable size of {expected_total_size}, but found {total_size}. files_size: {files_size}"

        # clean up
        logger.debug("removing snapshot_dir: " + snapshot_dir)
        shutil.rmtree(snapshot_dir)

    @pytest.mark.single_node
    @pytest.mark.parametrize("strategy", NON_ICS_STRATEGIES)
    def test_strategy_to_ics_snapshot_refresh(self, strategy):
        """
        Test snapshot restore with ICS and nodetool refresh of given strategy sstables.
        """
        sstable_size_in_mb = 2
        snapshot_dir, _session, node1 = self._generate_cluster_with_table_data_snapshot(compaction_strategy=strategy, sstable_size_in_mb=sstable_size_in_mb)
        self.alter_table_compaction(compaction_strategy=CompactionStrategy.INCREMENTAL, sstable_size_in_mb=sstable_size_in_mb, assert_altered_compaction=True)
        restore_snapshot_with_refresh(snapshot_dir=snapshot_dir, node=node1, keyspace=KEYSPACE_NAME, table=TABLE_NAME)
        self._read_generated_sstables_data(increasing_write_size=True)

    @pytest.mark.single_node
    @unmark_if("next_gating", condition=with_feature("tablets") & issue_open("scylladb/scylla-enterprise#4640"))
    def test_ics_refresh_with_big_sstable_files(self):
        """

        1. Create a keyspace and a table with STCS
        2. Generate sstables with rows
        3. Take a snapshot
        4. Delete the STCS table.
        5. Recreate the table with ICS.
        6. Restore the snapshot on the newly created ICS table.
        7. Run major compaction
        8. Verify we have the correct num of rows and file sizes after the major compaction.
        """

        sstable_size_in_mb = 2
        # create an STCS table with data
        session = self.prepare(num_of_nodes=1, r_factor=1, compaction_strategy=CompactionStrategy.SIZE_TIERED, keyspace_name=KEYSPACE_NAME, table_name=TABLE_NAME)

        node1 = self.cluster.nodelist()[0]
        num_rows_per_sstable = 50
        num_of_generated_sstables = 2
        max_generated_sstable_size = WRITE_SIZE_UNIT_IN_MB * num_of_generated_sstables
        max_compacted_sstable_size = sstable_size_in_mb + max_generated_sstable_size - 1

        self._write_and_flush_sstables(num_of_generated_sstables=num_of_generated_sstables, start_index=1, increasing_write_size=True, num_writes_per_sstable=num_rows_per_sstable, write_range=num_rows_per_sstable)

        node1.wait_for_compactions()
        # Create a snapshot
        snapshot_dir = make_snapshot(node1, KEYSPACE_NAME, TABLE_NAME, name="basic")

        # Drop the keyspace, make sure we have no data.
        session.execute(f"DROP KEYSPACE {KEYSPACE_NAME}")
        shutil.rmtree(os.path.join(node1.get_path(), "data", KEYSPACE_NAME))

        # Re-create a clean table as ICS
        create_ks(session, name=KEYSPACE_NAME, rf=1)
        create_table(session=session, compaction_strategy=CompactionStrategy.INCREMENTAL, keyspace_name=KEYSPACE_NAME, table_name=TABLE_NAME, sstable_size_in_mb=sstable_size_in_mb)

        # Restore data from snapshot of big file:
        restore_snapshot_with_refresh(snapshot_dir, node1, KEYSPACE_NAME, TABLE_NAME)
        # Run major compaction
        node1.compact()
        _sstables_files1, files_size = self._get_sstable_files_and_sizes()
        # Check that sstable file sizes maximum limit
        assert all([size <= max_compacted_sstable_size for size in files_size]), "Found larger sstable file size than expected"
        # Check that the number of table rows after refresh is correct.
        assert_row_count(session=session, table_name=FULL_TABLE_NAME, expected=num_rows_per_sstable * num_of_generated_sstables)

        # clean up
        logger.debug("removing snapshot_dir: " + snapshot_dir)
        shutil.rmtree(snapshot_dir)

    @pytest.mark.single_node
    def test_ics_sstables_refresh_with_collisions(self):
        """

        1. Create a keyspace and a table with chosen compaction strategy
        2. Generate sstables with rows
        4. Take a snapshot
        5. Generate more sstables with rows after the snapshot
        6. Restore the snapshot
        7. Verify we have the correct num of rows after the refresh.
        """

        sstable_size_in_mb = 2
        snapshot_dir, session, node1 = self._generate_cluster_with_table_data_snapshot(compaction_strategy=CompactionStrategy.INCREMENTAL, sstable_size_in_mb=sstable_size_in_mb)

        # Write more data after the snapshot, this will add some more rows not found on the snapshot of duplicated sstables:
        self._write_and_flush_sstables(num_of_generated_sstables=4, start_index=5)

        # Restore data from snapshot:
        restore_snapshot_with_refresh(snapshot_dir, node1, KEYSPACE_NAME, TABLE_NAME)
        # Check that the number of table entries after refresh is correct.
        assert_row_count(session=session, table_name=FULL_TABLE_NAME, expected=8)

        # clean up
        logger.debug("removing snapshot_dir: " + snapshot_dir)
        shutil.rmtree(snapshot_dir)

    def test_ics_with_partitions_larger_than_sstable_size(self):
        """
        Check ics with variable size partitions, smaller and larger than sstable size on flush and on compaction.
        """
        # TODO: Add the below steps comments as pytest steps after moving to pytest
        # (1) Test sstables number and sizes after flushes
        sstable_size_in_mb = 2
        compaction_strategy = CompactionStrategy.INCREMENTAL
        self.prepare(
            num_of_nodes=NUM_OF_NODES, r_factor=RF, compaction_strategy=compaction_strategy, sstable_size_in_mb=sstable_size_in_mb, compaction_additional_params={"min_threshold": "2", "min_sstable_size": "1"}, jvm_args=["--smp", "1"]
        )
        num_of_generated_sstables = 4
        # The maximum expected compacted sstable size is the addition of the 2 largets generated sstables.
        max_expected_file_size = WRITE_SIZE_UNIT_IN_MB * (2 * num_of_generated_sstables - 1)
        start_index = 1
        self._write_and_flush_sstables(num_of_generated_sstables=num_of_generated_sstables, start_index=start_index, increasing_write_size=True)

        def is_compaction_executed():
            sstables_files1, _ = self._get_sstable_files_and_sizes()
            logger.debug(f"Found {len(sstables_files1)} sstables, out of {num_of_generated_sstables} originally created")
            return len(sstables_files1) < num_of_generated_sstables

        node1 = self.cluster.nodelist()[0]
        node1.compact()
        node1.wait_for_compactions()
        wait_for(func=is_compaction_executed, text=str(is_compaction_executed), timeout=100)
        sstables_files1, files_size = self._get_sstable_files_and_sizes()
        max_found_file_size = max(files_size)
        logger.debug(f"Number of files after {num_of_generated_sstables} flushes is: {len(sstables_files1)} , {sstables_files1}")
        assert len(sstables_files1) > 1, "More than 1 sstable is expected"
        assert max_found_file_size <= max_expected_file_size, f"Maximum file size exceeds expected limit of {max_expected_file_size}: {max_found_file_size}"

    @pytest.mark.single_node
    def test_lcs_major_compaction_then_ics_major_compaction(self):
        """
        Check number and size of incremental compaction strategy sstables after generating load and running a major compaction via nodetool.
        """
        sstable_size_in_mb = 2
        self.prepare(num_of_nodes=1, r_factor=1, compaction_strategy=CompactionStrategy.LEVELED, sstable_size_in_mb=sstable_size_in_mb)
        node1 = self.cluster.nodelist()[0]
        self._write_and_flush_sstables(num_of_generated_sstables=3, start_index=1, write_range=3, num_writes_per_sstable=3)
        node1.compact()
        self.alter_table_compaction(compaction_strategy=CompactionStrategy.INCREMENTAL, assert_altered_compaction=True)
        node1.compact()

    def test_sstable_files_validations_with_ics_compaction(self):
        """
        Check number and size of incremental compaction strategy sstables after generating load and running a major compaction via nodetool.
        """
        sstable_size_in_mb = 10
        self.prepare(num_of_nodes=NUM_OF_NODES, r_factor=RF, compaction_strategy=CompactionStrategy.INCREMENTAL, sstable_size_in_mb=sstable_size_in_mb)
        node1 = self.cluster.nodelist()[0]
        num_of_generated_sstables = 3
        self._write_and_flush_sstables(num_of_generated_sstables=num_of_generated_sstables, start_index=1, write_range=10, num_writes_per_sstable=10)
        sstables_files1, _files_size = self._get_sstable_files_and_sizes()
        self._check_sstable_file_size_limit(list_sstable_files=sstables_files1, sstable_size_in_mb=sstable_size_in_mb)
        node1.compact()
        sstables_files2, _files_size2 = self._get_sstable_files_and_sizes()
        table = ".".join([KEYSPACE_NAME, TABLE_NAME])
        assert len(sstables_files2) >= num_of_generated_sstables, f"Less than {num_of_generated_sstables} SSTable files found for {table} after ICS compaction!"
        self._check_sstable_file_size_limit(list_sstable_files=sstables_files2, sstable_size_in_mb=sstable_size_in_mb)

    def test_ics_sstables_basic_large_partitions(self):
        """
        Add new keys on large-partitions-table for cluster nodes.
        """
        test_session = self.prepare(is_large_partitions=True, compaction_strategy=CompactionStrategy.INCREMENTAL)

        # Prefill
        self.insert_large_partitions_table_data(session=test_session, partition_range_end=PARTITIONS, rows_in_partition=ROWS_IN_PARTITION)

        big_partition = PARTITIONS + 1
        logger.debug(f"Create partition where pk = {big_partition} with {BIG_PARTITION_ROWS} rows")
        self.insert_large_partitions_table_data(session=test_session, partition_range_start=big_partition, partition_range_end=big_partition, rows_in_partition=BIG_PARTITION_ROWS)

        total_rows = PARTITIONS * ROWS_IN_PARTITION + BIG_PARTITION_ROWS

        # Test adding new rows

        node1 = self.cluster.nodelist()[0]
        self.cluster.flush()

        logger.debug("Inserting new data to nodes...")
        session = self.patient_cql_connection(node1)
        session.set_keyspace(KEYSPACE_NAME)
        num_of_new_rows = 50
        num_of_flushes = 5
        num_of_new_rows_per_flush = num_of_new_rows // num_of_flushes
        current_row_index = 1
        stmts = []
        logger.debug(f"Going to generate {num_of_new_rows} CQL inserts, for table {TABLE_NAME}")
        for flush in range(num_of_flushes):
            for i in range(current_row_index, current_row_index + num_of_new_rows_per_flush):
                logger.debug(f"#{i} cmd - ")
                stmt = f"insert into {TABLE_NAME} (pk, ck) values ({big_partition + i}, {random.randint(1, ROWS_IN_PARTITION)})"
                stmts.append(stmt)

            for stmt in stmts:
                session.execute(stmt)
            self.cluster.flush()
            current_row_index += num_of_new_rows_per_flush

        total_rows += num_of_new_rows
        self.cluster.flush()
        assert_row_count(session=session, table_name=FULL_TABLE_NAME, expected=total_rows)

    @pytest.mark.single_node
    def test_space_amplification_goal_trigger(self):
        """
        Check that space_amplification_goal triggers a compaction of 2 tiers appropriately when threshold is met.
        """
        sstable_size_in_mb = 10
        # Create a table with 1.20 goal and compaction min thereshold of 6.
        jvm_args = ["--smp", "1"]
        self.prepare(
            num_of_nodes=1,
            r_factor=1,
            compaction_strategy=CompactionStrategy.INCREMENTAL,
            sstable_size_in_mb=sstable_size_in_mb,
            compaction_additional_params={"space_amplification_goal": "1.20", "min_threshold": "6", "min_sstable_size": "1"},
            jvm_args=["--smp", "1"],
        )
        node1 = self.cluster.nodelist()[0]
        # Create a 10MB sstable consist of 10 partitions.
        self._write_and_flush_sstables(num_of_generated_sstables=1, start_index=1, write_range=10, num_writes_per_sstable=10, write_size_unit_in_mb=1)
        # Run major compaction for the sstable to be in last largest tier.
        node1.compact()
        node1.wait_for_compactions()
        # Create 4 1MB-size sstables with data the overlaps the first 10MB sstable.
        # so that the sum of the second tier sstables size will exceed space_amplification_goal
        # and will trigger a compaction.
        self._write_and_flush_sstables(num_of_generated_sstables=4, start_index=1, num_writes_per_sstable=1)
        # Check that compaction was triggered and the new sstables are compacted with the big one.
        node1.wait_for_compactions()
        _sstables_files1, files_size = self._get_sstable_files_and_sizes()
        assert sorted(files_size) == [1, 10], "Cross-tier compaction was not triggered after space_amplification_goal is exceeded!"

    @pytest.mark.single_node
    def test_space_amplification_goal_3_buckets(self):
        """
        Check that space_amplification_goal triggers a compaction of 2 tiers appropriately when threshold is met.
        """
        sstable_size_in_mb = 10
        # Create a table with 1.20 goal and compaction min thereshold of 6.

        self.prepare(
            num_of_nodes=1,
            r_factor=1,
            compaction_strategy=CompactionStrategy.INCREMENTAL,
            sstable_size_in_mb=sstable_size_in_mb,
            compaction_additional_params={"space_amplification_goal": "1.70", "min_threshold": "4", "min_sstable_size": "4"},
            jvm_args=["--smp", "1"],
        )
        node1 = self.cluster.nodelist()[0]
        # Create a 10MB sstable consist of 10 partitions.
        self._write_and_flush_sstables(num_of_generated_sstables=1, start_index=1, write_range=10, num_writes_per_sstable=10, write_size_unit_in_mb=1)
        # Run major compaction for the sstable to be in last largest tier.
        node1.compact()
        node1.wait_for_compactions()
        # Create 4 1MB-size sstables with data the overlaps the first 10MB sstable.
        # so that the sum of the second tier sstables size will NOT exceed space_amplification_goal
        # and will NOT trigger a cross-tier compaction.
        self._write_and_flush_sstables(num_of_generated_sstables=4, start_index=1, num_writes_per_sstable=1)
        # Check that compaction was triggered by min_threshold and the new sstables are compacted without the big one.
        node1.wait_for_compactions()
        _sstables_files1, files_size = self._get_sstable_files_and_sizes()
        assert sorted(files_size) == [4, 10], "Unexpected compacted sstable sizes."

        self._write_and_flush_sstables(num_of_generated_sstables=1, write_range=5, start_index=1, num_writes_per_sstable=5)
        # Check that cross-tier compaction was triggered since 5 + 4 sstables > 7 (70% of 10 )
        node1.wait_for_compactions()
        _sstables_files1, files_size = self._get_sstable_files_and_sizes()
        assert files_size == [10], "Cross-tier compaction was not triggered after space_amplification_goal is exceeded!"
