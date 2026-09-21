import logging
import re
from itertools import groupby
from operator import attrgetter

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import Session, SimpleStatement

from cdc_batch_test import Column, DataGenerator, Row
from cdc_test import CDCInitializeHelper, CdcLogOperations
from dtest_class import Tester, create_ks
from tools.cdc_utils import get_next_timestamp, mkident

logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)


class DataGeneratorWithStaticColumn(DataGenerator):
    """
    Base class for generating data with
    static and non-static columns

    Instance of class can generate object object of dataset
    with provided in subclass data type for columns. it generates
    row with provided number of primary and cluster key, number of
    regular columns and static columns.

    These data set is used in cdc tests for putting into base
    table and check expected results in scylla_cdc_log table
    """

    default_ttl = 1000

    def __init__(self, pk_num, ck_num, cols_num, static_col_num=None, use_ttl=False, use_ts=False):  # noqa: PLR0913
        self.static_col_num = static_col_num
        super().__init__(pk_num, ck_num, cols_num, use_ttl, use_ts)

    def _build_batch_data(self, data, only_columns=None, only_stat_columns=None):
        """
        Build dataset as batch of data row
        """
        batch = []
        for i in range(self.pk_num):
            for j in range(self.ck_num):
                cols = []
                cols_ids = list(range(self.cols_num)) if not only_columns else only_columns
                for k in cols_ids:
                    col_ttl = self.default_ttl + k if self.use_ttl else None
                    col_ts = get_next_timestamp() if self.use_ts else None
                    cols.append(Column(name=f"cval{k}", value=data[k], ttl=col_ttl, timestamp=col_ts))
                cols_ids = list(range(self.static_col_num)) if not only_stat_columns else only_stat_columns
                for k in cols_ids:
                    col_ttl = self.default_ttl + k if self.use_ttl else None
                    col_ts = get_next_timestamp() if self.use_ts else None
                    cols.append(Column(name=f"stval{k}", value=data[k], ttl=col_ttl, timestamp=col_ts, is_static=True))
                batch.append(Row(i, j, cols))

        return batch

    def build_batch_empty_data(self):
        return self._build_batch_data([None] * (self.cols_num + self.static_col_num))


class IntDataGenerator(DataGeneratorWithStaticColumn):
    def _generate_data(self):
        return [i + self.seed for i in range(self.cols_num + self.static_col_num)]


class BigintDataGenerator(DataGeneratorWithStaticColumn):
    def _generate_data(self):
        return [32000 + i + self.seed for i in range(self.cols_num + self.static_col_num)]


class MapIntIntDataGenerator(DataGeneratorWithStaticColumn):
    def _generate_data(self):
        return [{i + self.seed: i + self.seed + 10} for i in range(self.cols_num + self.static_col_num)]


class TextDataGenerator(DataGeneratorWithStaticColumn):
    def _generate_data(self):
        return [f"text{i + self.seed}" for i in range(self.cols_num + self.static_col_num)]


class VarcharDataGenerator(DataGeneratorWithStaticColumn):
    def _generate_data(self):
        return [f"varchar{i + self.seed}" for i in range(self.cols_num + self.static_col_num)]


class FrozensetintDataGenerator(DataGeneratorWithStaticColumn):
    def _generate_data(self):
        return [{i + self.seed, i + self.seed + 10} for i in range(self.cols_num + self.static_col_num)]


class FrozensettextDataGenerator(DataGeneratorWithStaticColumn):
    def _generate_data(self):
        return [{f"frozenset{i + self.seed}", f"frozenset{i + self.seed + 10}"} for i in range(self.cols_num + self.static_col_num)]


class FrozenlistintDataGenerator(DataGeneratorWithStaticColumn):
    def _generate_data(self):
        return [[i + self.seed, i + self.seed + 10] for i in range(self.cols_num + self.static_col_num)]


class SetintDataGenerator(DataGeneratorWithStaticColumn):
    def _generate_data(self):
        return [{i + self.seed, i + self.seed + 10} for i in range(self.cols_num + self.static_col_num)]


class ListintDataGenerator(DataGeneratorWithStaticColumn):
    def _generate_data(self):
        return [[i + self.seed, i + self.seed + 10] for i in range(self.cols_num + self.static_col_num)]


checking_types = ["int", "bigint", "text", "map<int,int>", "varchar", "frozen<set<int>>", "frozen<set<text>>", "frozen<list<int>>", "set<int>", "list<int>"]


def get_generator(data_type):
    for subclass in DataGeneratorWithStaticColumn.__subclasses__():
        name = re.sub("[<>,]", "", data_type)
        if subclass.__name__.lower().startswith(name):
            return subclass


def get_row_by_pk_and_ck(dataset, pk, ck):
    return next(filter(lambda x: x.pk == pk and x.ck == ck, dataset))


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestCDCStaticRow(Tester, CDCInitializeHelper):
    """
    Check correctness of data in cdc log table for static row

    CDC feature process static row in different way than regular
    row. If mutation have only static column modification, the cdc
    log rows written as usual
    if mutation query contains static column and regular column,
    cdc split such single query and build batch, where cdc log row contains
    only static column modification data and then cdc log row with
    regular columng data

    More information by: https://docs.scylladb.com/using-scylla/cdc/cdc-basic-operations/
    """

    keyspace = "ks"
    table = "cf"
    num_of_columns = 0
    num_of_static = 2
    num_of_rows = 1
    num_of_partitions = 1
    columns_type = "int"
    data_generator_class = None

    @pytest.fixture(scope="function", params=[pytest.param(t, id=mkident(t)) for t in checking_types], autouse=True)
    def select_data_type(self, request):
        self.columns_type = request.param
        self.data_generator_class = get_generator(request.param)

    def test_single_static_row(self):
        self.check_single_row_static()

    def test_single_static_row_preimage(self):
        self.check_single_row_static(preimage_enable=True)

    def test_single_static_row_postimage(self):
        self.check_single_row_static(postimage_enable=True)

    def test_single_static_row_preimage_postimage(self):
        self.check_single_row_static(preimage_enable=True, postimage_enable=True)

    def test_several_partitions_with_single_static_row_preimage_postimage(self):
        self.num_of_partitions = 5
        self.num_of_rows = 1
        self.check_single_row_static(preimage_enable=True, postimage_enable=True)

    def test_several_partitions_with_single_static_row(self):
        self.num_of_partitions = 10
        self.num_of_rows = 1
        self.check_single_row_static()

    def test_several_partitions_with_single_static_row_preimage(self):
        self.num_of_partitions = 5
        self.num_of_rows = 1
        self.check_single_row_static(preimage_enable=True)

    def test_several_partitions_with_single_static_row_postimage(self):
        self.num_of_partitions = 3
        self.num_of_rows = 1
        self.check_single_row_static(postimage_enable=True)

    def test_several_partitions_with_single_static_row_full_postimage(self):
        self.num_of_partitions = 3
        self.num_of_rows = 1
        self.check_single_row_static(preimage_enable="full", postimage_enable=True)

    def test_operations_with_static_row_and_regular_columns_preimage_postimage(self):
        self.num_of_partitions = 3
        self.num_of_rows = 3
        self.num_of_columns = 2
        self.check_static_common_field(preimage_enable=True, postimage_enable=True)

    def test_operations_with_static_row_and_regular_columns_preimage(self):
        self.num_of_partitions = 3
        self.num_of_rows = 3
        self.num_of_columns = 2
        self.check_static_common_field(preimage_enable=True)

    def test_operations_with_static_row_and_regular_columns_postimage(self):
        self.num_of_partitions = 3
        self.num_of_rows = 3
        self.num_of_columns = 2
        self.check_static_common_field(postimage_enable=True)

    def test_operations_with_static_row_and_regular_columns(self):
        self.num_of_partitions = 3
        self.num_of_rows = 3
        self.num_of_columns = 2
        self.check_static_common_field()

    def test_operations_with_large_number_of_static_and_regular_columns_postimage_preimage(self):
        self.num_of_partitions = 3
        self.num_of_rows = 3
        self.num_of_static = 10
        self.num_of_columns = 10
        self.check_static_common_field(preimage_enable=True, postimage_enable=True)

    def test_operations_with_large_number_of_static_and_regular_columns(self):
        self.num_of_partitions = 3
        self.num_of_rows = 3
        self.num_of_static = 10
        self.num_of_columns = 10
        self.check_static_common_field()

    def prepare_cluster(self, num_nodes=1, rf=1) -> Session:
        self.populate_sequentially(num_nodes)
        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node)
        self.wait_for_last_generation_to_be_active(session)
        self.wait_for_metadata_update(session, cluster_size=num_nodes)
        session.execute(f"ALTER keyspace system_distributed with replication={{'class': 'SimpleStrategy', 'replication_factor': {rf}}}")

        return session

    def create_schema_with_static_fields(self, session, rf=1, preimage_enable=False, postimage_enable=False):
        s = ""
        for i in range(self.num_of_static):
            s += f"stval{i} {self.columns_type} STATIC, "
        for i in range(self.num_of_columns):
            s += f"cval{i} {self.columns_type}, "

        statement = f"CREATE TABLE {self.keyspace}.{self.table} \
                    (pk bigint, \
                     ck bigint, \
                     {s} \
                     PRIMARY KEY (pk, ck)\
                    ) WITH cdc={{'enabled': true"
        if preimage_enable:
            statement += f", 'preimage': '{preimage_enable}'"
        if postimage_enable:
            statement += f", 'postimage': '{postimage_enable}'"
        statement += "}"
        create_ks(session, self.keyspace, rf=rf)
        session.execute(statement)

    def execute_insert_static(self, session, dataset):
        for row in dataset:
            ins_cols = ", ".join([col.name for col in row.cols if col.is_static])
            ins_vals = ", ".join(["%s"] * len([col.name for col in row.cols if col.is_static]))
            stm = f"INSERT INTO {self.keyspace}.{self.table} (pk, {ins_cols}) VALUES (%s, {ins_vals})"
            session.execute(stm, [row.pk, *[col.value for col in row.cols if col.is_static]])

    def execute_update_static(self, session, dataset):
        for row in dataset:
            upd_cols = ", ".join([f"{col.name} = %s" for col in row.cols if col.is_static])
            stm = f"UPDATE {self.keyspace}.{self.table} SET {upd_cols} WHERE pk = %s;"
            session.execute(stm, [*[col.value for col in row.cols if col.is_static], row.pk])

    def execute_delete_static(self, session, dataset):
        for row in dataset:
            del_cols = ", ".join([f"{col.name}" for col in row.cols if col.is_static])
            stm = f"DELETE {del_cols} FROM {self.keyspace}.{self.table} WHERE pk = %s;"
            session.execute(stm, [row.pk])

    def check_single_row_static(self, preimage_enable=False, postimage_enable=False):
        session: Session = self.prepare_cluster()
        self.create_schema_with_static_fields(session, preimage_enable=preimage_enable, postimage_enable=postimage_enable)
        self.dataset_generator = self.data_generator_class(pk_num=self.num_of_partitions, ck_num=self.num_of_rows, cols_num=self.num_of_columns, static_col_num=self.num_of_static, use_ttl=False, use_ts=False)

        ins_dataset = self.dataset_generator.build_new_dataset_for_batch()
        self.execute_insert_static(session, ins_dataset)
        cdc_rows = self.get_latest_cdc_rows_after_timestamp(session)
        expected_data = self.generate_expected_cdc_rows(ins_dataset, preimage=preimage_enable, postimage=postimage_enable, only_static=True)
        last_timestamps = self.verify_cdc_rows_data(cdc_rows, expected_data, CdcLogOperations.UPDATE, preimage=preimage_enable, postimage=postimage_enable)

        upd_dataset = self.dataset_generator.build_new_dataset_for_batch()
        self.execute_update_static(session, upd_dataset)
        expected_data = self.generate_expected_cdc_rows(upd_dataset, ins_dataset, preimage=preimage_enable, postimage=postimage_enable, only_static=True)
        cdc_rows = self.get_latest_cdc_rows_after_timestamp(session, last_timestamps)
        last_timestamps = self.verify_cdc_rows_data(cdc_rows, expected_data, CdcLogOperations.UPDATE, preimage=preimage_enable, postimage=postimage_enable)

        del_dataset = self.dataset_generator.build_batch_empty_data()
        self.execute_delete_static(session, del_dataset)
        expected_data = self.generate_expected_cdc_rows(del_dataset, upd_dataset, preimage=preimage_enable, postimage=postimage_enable, only_static=True)
        cdc_rows = self.get_latest_cdc_rows_after_timestamp(session, last_timestamps)
        last_timestamps = self.verify_cdc_rows_data(cdc_rows, expected_data, CdcLogOperations.UPDATE, preimage=preimage_enable, postimage=postimage_enable)

        upd_dataset = self.dataset_generator.build_new_dataset_for_batch()
        self.execute_update_static(session, upd_dataset)
        expected_data = self.generate_expected_cdc_rows(upd_dataset, preimage=preimage_enable, postimage=postimage_enable, only_static=True)
        cdc_rows = self.get_latest_cdc_rows_after_timestamp(session, last_timestamps)
        last_timestamps = self.verify_cdc_rows_data(cdc_rows, expected_data, CdcLogOperations.UPDATE, preimage=preimage_enable, postimage=postimage_enable)

    def execute_update_static_and_regular_column(self, session, ds):
        for row in ds:
            upd_cols = ", ".join([f"{col.name} = %s" for col in row.cols])
            stm = f"UPDATE {self.keyspace}.{self.table} SET {upd_cols} WHERE pk = %s and ck = %s"
            session.execute(stm, [*[col.value for col in row.cols], row.pk, row.ck])

    def execute_insert_static_and_regular_column(self, session, ds):
        for row in ds:
            ins_cols = ", ".join([col.name for col in row.cols])
            ins_vals = ", ".join(["%s"] * len([col.name for col in row.cols]))
            stm = f"INSERT INTO {self.keyspace}.{self.table} (pk, ck, {ins_cols}) VALUES (%s, %s, {ins_vals})"
            session.execute(stm, [row.pk, row.ck, *[col.value for col in row.cols]])

    def check_static_common_field(self, preimage_enable=False, postimage_enable=False):
        session: Session = self.prepare_cluster()
        self.create_schema_with_static_fields(session, preimage_enable=preimage_enable, postimage_enable=postimage_enable)
        self.dataset_generator = self.data_generator_class(pk_num=self.num_of_partitions, ck_num=self.num_of_rows, cols_num=self.num_of_columns, static_col_num=self.num_of_static, use_ttl=False, use_ts=False)

        ins_dataset = self.dataset_generator.build_new_dataset_for_batch()

        self.execute_insert_static_and_regular_column(session, ins_dataset)
        cdc_rows = self.get_latest_cdc_rows_after_timestamp(session)
        expected_data = self.generate_expected_cdc_rows(ins_dataset, preimage=preimage_enable, postimage=postimage_enable)

        last_timestamps = self.verify_cdc_rows_data(cdc_rows, expected_data, CdcLogOperations.INSERT, preimage=preimage_enable, postimage=postimage_enable)
        upd_dataset = self.dataset_generator.build_new_dataset_for_batch()

        self.execute_update_static_and_regular_column(session, upd_dataset)
        cdc_rows = self.get_latest_cdc_rows_after_timestamp(session, last_timestamps)
        expected_data = self.generate_expected_cdc_rows(upd_dataset, ins_dataset, preimage=preimage_enable, postimage=postimage_enable)

        last_timestamps = self.verify_cdc_rows_data(cdc_rows, expected_data, CdcLogOperations.UPDATE, preimage=preimage_enable, postimage=postimage_enable)

        upd_none_dataset = self.dataset_generator.build_batch_empty_data()
        self.execute_update_static_and_regular_column(session, upd_none_dataset)
        cdc_rows = self.get_latest_cdc_rows_after_timestamp(session, last_timestamps)
        expected_data = self.generate_expected_cdc_rows(upd_none_dataset, upd_dataset, preimage=preimage_enable, postimage=postimage_enable)
        last_timestamps = self.verify_cdc_rows_data(cdc_rows, expected_data, CdcLogOperations.UPDATE)

        upd_dataset = self.dataset_generator.build_new_dataset_for_batch()

        self.execute_update_static_and_regular_column(session, upd_dataset)
        cdc_rows = self.get_latest_cdc_rows_after_timestamp(session, last_timestamps)
        expected_data = self.generate_expected_cdc_rows(upd_dataset, upd_none_dataset, preimage=preimage_enable, postimage=postimage_enable)

        self.verify_cdc_rows_data(cdc_rows, expected_data, CdcLogOperations.UPDATE, preimage=preimage_enable, postimage=postimage_enable)

    def verify_cdc_rows_data(
        self,
        cdc_rows,
        expected_data,
        delta_operation,
        preimage=False,
        postimage=False,
    ):
        preimage_rows = []
        postimage_rows = []
        delta_rows = []
        # group all cdc rows by stream_id, cdc time.
        group_by_cdc_time = attrgetter("cdc_stream_id", "cdc_time", "pk")
        cdc_groups = []
        for key, rows in groupby(cdc_rows, key=group_by_cdc_time):
            cdc_groups.append((key, [r for r in rows]))

        last_timestamps = []
        i = 0
        for (_, last_cdc_timestamp, pk), rows in cdc_groups:
            # collect timestamp per stream for further filtering
            last_timestamps.append(f'"cdc$time" > {last_cdc_timestamp}')

            preimage_rows = [row for row in rows if row.cdc_operation == CdcLogOperations.PREIMAGE]
            postimage_rows = [row for row in rows if row.cdc_operation == CdcLogOperations.POSTIMAGE]
            delta_rows = [row for row in rows if row.cdc_operation not in [CdcLogOperations.PREIMAGE, CdcLogOperations.POSTIMAGE]]
            if preimage:
                logger.debug("Expected preimage: %s", expected_data[pk][i]["preimage"])
                logger.debug("Actual preimage row: %s", preimage_rows)
                assert len(preimage_rows) == len(expected_data[pk][i]["preimage"]), "Actual preimage differs from expected"

            logger.debug("Expected delta: %s", expected_data[pk][i]["delta"])
            logger.debug("Actual delta: %s", delta_rows)
            assert len(delta_rows) == len(expected_data[pk][i]["delta"]), f"Actual delta differs from expected"
            if postimage:
                logger.debug("Expected postimage: %s", expected_data[pk][i]["postimage"])
                logger.debug("Actual postimage row: %s", postimage_rows)
                assert len(postimage_rows) == len(expected_data[pk][i]["postimage"]), "Actual postimage differs from expected"

            for actual, expected in zip(delta_rows, expected_data[pk][i]["delta"]):
                assert actual.pk == expected.pk
                assert actual.ck == expected.ck
                if actual.ck is None:
                    assert actual.cdc_operation == CdcLogOperations.UPDATE
                else:
                    assert actual.cdc_operation == delta_operation
                self.assert_columns(actual, expected)

            if preimage:
                for actual, expected in zip(preimage_rows, expected_data[pk][i]["preimage"]):
                    assert actual.pk == expected.pk
                    assert actual.ck == expected.ck
                    assert actual.cdc_operation == CdcLogOperations.PREIMAGE
                    self.assert_columns(actual, expected)

            # verify postimage
            if postimage:
                for actual, expected in zip(postimage_rows, expected_data[pk][i]["postimage"]):
                    assert actual.pk == expected.pk
                    assert actual.ck == expected.ck
                    assert actual.cdc_operation == CdcLogOperations.POSTIMAGE
                    self.assert_columns(actual, expected)
            i += 1
            if i >= self.num_of_rows:
                i = 0
        return last_timestamps

    def get_latest_cdc_rows_after_timestamp(self, session, last_timestamps=None):
        if last_timestamps:
            where = " AND ".join(last_timestamps)
            statement = SimpleStatement(f"SELECT * from {self.keyspace}.{self.table}_scylla_cdc_log where {where} ALLOW FILTERING", consistency_level=ConsistencyLevel.ALL)
        else:
            statement = f"SELECT * FROM {self.keyspace}.{self.table}_scylla_cdc_log"
        return list(session.execute(statement))

    def assert_columns(self, actual, expected):
        for expected_column in expected.cols:
            actual_column_value = getattr(actual, expected_column.name)
            if self.columns_type.startswith("list") and actual_column_value:
                actual_column_value = list(actual_column_value.values())
            assert actual_column_value == expected_column.value, f"Column {expected_column.name} has different value in cdc_row: {actual_column_value} vs expected {expected_column.value}\n {actual} \n {expected}"

    @staticmethod
    def generate_expected_cdc_rows(dataset, prev_dataset=None, preimage=False, postimage=False, only_static=False):  # noqa: PLR0912
        group_by_pk = attrgetter("pk")

        expected_data = {}

        for pk, rows in groupby(dataset, key=group_by_pk):
            expected_partition_data = {}
            expected_stats_row = {
                "preimage": None,
                "delta": None,
                "postimage": None,
            }
            for row in rows:
                prev_row = prev_stat_row = prev_regular_row = None
                if prev_dataset:
                    prev_row = get_row_by_pk_and_ck(prev_dataset, pk, row.ck)

                    prev_stats_cols = [col for col in prev_row.cols if col.is_static]
                    prev_regular_cols = [col for col in prev_row.cols if not col.is_static]

                    if all([col.value for col in prev_stats_cols]):
                        prev_stat_row = Row(pk, None, prev_stats_cols)
                    prev_regular_row = Row(pk, row.ck, prev_regular_cols)

                expected_row_data = {"preimage": [], "delta": [], "postimage": []}

                static_cols = [col for col in row.cols if col.is_static]
                regular_cols = [col for col in row.cols if not col.is_static]

                if static_cols:
                    static_row = Row(pk, None, static_cols)

                expected_stats_row["preimage"] = expected_stats_row["postimage"] or prev_stat_row
                if expected_stats_row["preimage"]:
                    res = [col.value for col in expected_stats_row["preimage"].cols]
                    if not all(res):
                        expected_stats_row["preimage"] = None
                expected_stats_row["delta"] = static_row
                expected_stats_row["postimage"] = static_row

                regular_row = Row(pk, row.ck, regular_cols)

                if expected_stats_row["preimage"]:
                    expected_row_data["preimage"].append(expected_stats_row["preimage"])
                if prev_regular_row and not only_static:
                    expected_row_data["preimage"].append(prev_regular_row)
                expected_row_data["delta"] = [expected_stats_row["delta"]]
                if not only_static:
                    expected_row_data["delta"].append(regular_row)
                expected_row_data["postimage"] = [expected_stats_row["postimage"]]
                if not only_static:
                    expected_row_data["postimage"].append(regular_row)

                expected_partition_data[row.ck] = dict()
                if preimage:
                    expected_partition_data[row.ck]["preimage"] = expected_row_data["preimage"]
                expected_partition_data[row.ck]["delta"] = expected_row_data["delta"]
                if postimage:
                    expected_partition_data[row.ck]["postimage"] = expected_row_data["postimage"]

            expected_data[pk] = expected_partition_data

        return expected_data
