import logging
import re
from itertools import groupby
from operator import attrgetter

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import BatchStatement, Session, SimpleStatement
from cassandra.query import BatchType
from ccmlib.scylla_node import ScyllaNode

from cdc_test import CDCInitializeHelper, CdcLogOperations
from dtest_class import Tester, create_ks
from tools.cdc_utils import get_next_timestamp, mkident

logger = logging.getLogger(__name__)


class Column:
    def __init__(self, name, value=None, ttl=None, timestamp=None, is_static=False):
        self.name = name
        self.value = value
        self.ttl = ttl
        self.timestamp = timestamp
        self.is_static = is_static

    def __str__(self):
        s = f"{{{self.name}:{self.value}}}"
        if self.ttl:
            s += f" ttl:{self.ttl}"
        if self.timestamp:
            s += f" timestamp:{self.timestamp}"
        return s

    def __repr__(self):
        return self.__str__()


class Row:
    def __init__(self, pk, ck, columns=None):
        self.pk = pk
        self.ck = ck
        self.cols = columns or []

    def __str__(self):
        return f"pk:{self.pk};ck:{self.ck}:{[col for col in self.cols]}"

    def __repr__(self):
        return self.__str__()


class DataGenerator:
    default_ttl = 1000

    def __init__(self, pk_num, ck_num, cols_num, use_ttl=False, use_ts=False):
        self.pk_num = pk_num
        self.ck_num = ck_num
        self.cols_num = cols_num
        self.use_ttl = use_ttl
        self.use_ts = use_ts
        self._counter = 0

    @property
    def seed(self):
        self._counter += 1
        return self._counter

    def _generate_data(self):
        raise NotImplementedError

    def _build_batch_data(self, data, only_columns=None):
        batch = []
        for i in range(self.pk_num):
            for j in range(self.ck_num):
                cols = []
                cols_ids = list(range(self.cols_num)) if not only_columns else only_columns
                for k in cols_ids:
                    vals = []
                    vals.append(f"cval{k}")
                    vals.append(data[k])
                    if self.use_ttl:
                        vals.append(self.default_ttl + k)
                    if self.use_ts:
                        vals.append(get_next_timestamp())
                    cols.append(Column(*vals))
                batch.append(Row(i, j, cols))

        return batch

    def build_batch_empty_data(self):
        return self._build_batch_data([None for i in range(self.cols_num)])

    def build_new_dataset_for_batch(self):
        _data = self._generate_data()
        return self._build_batch_data(_data)

    def build_new_odd_batch_dataset(self):
        _data = self._generate_data()
        columns_ids = list(range(1, self.cols_num, 2))
        return self._build_batch_data(_data, columns_ids)

    def build_new_even_batch_dataset(self):
        _data = self._generate_data()
        columns_ids = list(range(0, self.cols_num, 2))
        return self._build_batch_data(_data, columns_ids)

    def combine_datasets(self, first, second):
        if not first:
            first = self.build_batch_empty_data()
        new_data = []
        for first_row in first:
            second_rows = list(filter(lambda x: x.pk == first_row.pk and x.ck == first_row.ck, second))
            if second_rows:
                r = Row(first_row.pk, first_row.ck)
                for fcol in first_row.cols:
                    scol = list(filter(lambda x: x.name == fcol.name, second_rows[0].cols))
                    if scol:
                        r.cols.append(scol[0])
                    else:
                        r.cols.append(fcol)
                new_data.append(r)

        return new_data

    def exclude_dataset(self, first, second):
        new_data = []
        for first_row in first:
            second_rows = list(filter(lambda x: x.pk == first_row.pk and x.ck == first_row.ck, second))
            if second_rows:
                r = Row(first_row.pk, first_row.ck)
                for fcol in first_row.cols:
                    scol = list(filter(lambda x: x.name == fcol.name, second_rows[0].cols))
                    if scol:
                        r.cols.append(fcol)
                    else:
                        r.cols.append(Column(fcol.name, None))
                new_data.append(r)

        return new_data


class IntDataGenerator(DataGenerator):
    def _generate_data(self):
        return [i + self.seed for i in range(self.cols_num)]


class BigintDataGenerator(DataGenerator):
    def _generate_data(self):
        return [32000 + i + self.seed for i in range(self.cols_num)]


class TextDataGenerator(DataGenerator):
    def _generate_data(self):
        return [f"text{i + self.seed}" for i in range(self.cols_num)]


class VarcharDataGenerator(DataGenerator):
    def _generate_data(self):
        return [f"varchar{i + self.seed}" for i in range(self.cols_num)]


class FrozensetintDataGenerator(DataGenerator):
    def _generate_data(self):
        return [{i + self.seed, i + self.seed + 10} for i in range(self.cols_num)]


class FrozensettextDataGenerator(DataGenerator):
    def _generate_data(self):
        return [{f"frozenset{i + self.seed}", f"frozenset{i + self.seed + 10}"} for i in range(self.cols_num)]


class FrozenlistintDataGenerator(DataGenerator):
    def _generate_data(self):
        return [[i + self.seed, i + self.seed + 10] for i in range(self.cols_num)]


class SetintDataGenerator(DataGenerator):
    def _generate_data(self):
        return [{i + self.seed, i + self.seed + 10} for i in range(self.cols_num)]


class ListintDataGenerator(DataGenerator):
    def _generate_data(self):
        return [[i + self.seed, i + self.seed + 10] for i in range(self.cols_num)]


class MapIntIntDataGenerator(DataGenerator):
    def _generate_data(self):
        return [{i + self.seed: i + self.seed + 10} for i in range(self.cols_num)]


class MaptextblobDataGenerator(DataGenerator):
    def _generate_data(self):
        return [{f"file{i + self.seed}": b"\x00" * (i + self.seed)} for i in range(self.cols_num)]


def get_generator(data_type):
    for subclass in DataGenerator.__subclasses__():
        name = re.sub("[<>,]", "", data_type)
        if subclass.__name__.lower().startswith(name):
            return subclass


checking_types = ["int", "bigint", "text", "varchar", "frozen<set<int>>", "frozen<set<text>>", "frozen<list<int>>", "list<int>", "set<int>", "map<int,int>", "map<text,blob>"]


@pytest.mark.dtest_full
@pytest.mark.single_node
@pytest.mark.next_gating
class TestCDCBatchesSimple(Tester, CDCInitializeHelper):
    keyspace = "ks"
    table = "cf"
    num_of_columns = 10
    num_of_rows = 3
    num_of_partitions = 5
    columns_type: str = "int"
    data_generator_class = None

    @pytest.fixture(scope="function", params=[pytest.param(t, id=mkident(t)) for t in checking_types], autouse=True)
    def select_data_type(self, request):
        self.columns_type = request.param
        self.data_generator_class = get_generator(request.param)

    def prepare_cluster(self, num_nodes=1, rf=1) -> tuple[ScyllaNode, Session]:
        self.populate_sequentially(num_nodes)
        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node)
        self.wait_for_last_generation_to_be_active(session)
        self.wait_for_metadata_update(session, cluster_size=num_nodes)
        session.execute(f"ALTER keyspace system_distributed with replication={{'class': 'SimpleStrategy', 'replication_factor': {rf}}}")

        return session

    def create_schema_with_large_number_of_columns_with_cdc(self, session, rf=1, preimage_enable=False, postimage_enable=False):
        columns = []

        for i in range(self.num_of_columns):
            columns.append(f"cval{i} {self.columns_type}")

        statement = f"CREATE TABLE {self.keyspace}.{self.table} \
                    (pk bigint, \
                     ck bigint, \
                     {','.join(columns)}, \
                     PRIMARY KEY (pk, ck)\
                    )"
        statement += " WITH cdc={'enabled': true"
        if preimage_enable:
            statement += f", 'preimage': '{preimage_enable}'"
        if postimage_enable:
            statement += ", 'postimage': true"
        statement += "}"
        create_ks(session, self.keyspace, rf=rf)
        session.execute(statement)

    def create_batch_with_insert_stm(self, data):
        batch = BatchStatement(batch_type=BatchType.LOGGED)
        for row in data:
            for column in row.cols:
                use = []
                if column.ttl:
                    use.append(f"TTL {column.ttl}")
                if column.timestamp:
                    use.append(f"TIMESTAMP {column.timestamp}")
                use = " and ".join(use)
                use = f"USING {use}" if use else ""
                stm = f"INSERT INTO {self.keyspace}.{self.table} (pk, ck, {column.name}) VALUES (%s, %s, %s) {use}"

                batch.add(stm, [row.pk, row.ck, column.value])

        return batch

    def create_batch_with_update_stm(self, data):
        batch = BatchStatement(batch_type=BatchType.LOGGED)
        for row in data:
            for column in row.cols:
                use = []
                if column.ttl:
                    use.append(f"TTL {column.ttl}")
                if column.timestamp:
                    use.append(f"TIMESTAMP {column.timestamp}")
                use = " and ".join(use)
                use = f"USING {use}" if use else ""
                stm = f"UPDATE {self.keyspace}.{self.table} SET {column.name} = %s {use} WHERE pk = %s and ck = %s"
                batch.add(stm, [column.value, row.pk, row.ck])

        return batch

    def test_delta_batch_only(self):
        self.run_batch_checks(preimage_enable=False, postimage_enable=False)

    def test_preimage_true_delta_batch(self):
        self.run_batch_checks(preimage_enable=True, postimage_enable=False)

    def test_preimage_on_delta_batch(self):
        self.run_batch_checks(preimage_enable="true", postimage_enable=False)

    def test_preimage_full_delta_batch(self):
        self.run_batch_checks(preimage_enable="full", postimage_enable=False)

    def test_delta_postimage_batch(self):
        self.run_batch_checks(preimage_enable=False, postimage_enable=True)

    def test_preimage_full_delta_postimage(self):
        self.run_batch_checks(preimage_enable="full", postimage_enable=True)

    def test_preimage_on_delta_postimage(self):
        self.run_batch_checks(preimage_enable="true", postimage_enable=True)

    def run_batch_checks(self, preimage_enable, postimage_enable):
        self.dataset_generator = self.data_generator_class(self.num_of_partitions, self.num_of_rows, self.num_of_columns)
        self.current_dataset = None
        self.last_timestamps = []

        session = self.prepare_cluster(num_nodes=1, rf=1)
        self.create_schema_with_large_number_of_columns_with_cdc(session, rf=1, preimage_enable=preimage_enable, postimage_enable=postimage_enable)

        logger.debug("Build, run and verify batch inserting first rows into partitions with INSERT stm")
        self._check_batch_with_insert_first_rows(session, preimage_enable, postimage_enable)

        logger.debug("Build, run and verify batch updating all columns in a row for each partition")
        self._check_batch_with_update_all_columns_in_each_row(session, preimage_enable, postimage_enable)

        logger.debug("Build, run and verify batch updating only even columns in a row")
        self._check_batch_with_update_only_even_columns_per_row(session, preimage_enable, postimage_enable)

        logger.debug("Build, run and verify batch updating only odd columns in a row")
        self._check_batch_with_update_only_odd_columns_per_row(session, preimage_enable, postimage_enable)

        logger.debug("Build, run and verify batch deleting data by updating with null all columns in a row")
        self._check_batch_with_update_all_columns_with_null_for_each_row(session, preimage_enable, postimage_enable)

        logger.debug("Build, run and verify batch with updating all columns in a row after data removed")
        self._check_batch_with_update_all_columns_for_each_row_after_delete(session, preimage_enable, postimage_enable)

    def _check_batch_with_insert_first_rows(self, session, preimage_enable, postimage_enable):
        """insert data in a batch first time

        Arguments:
            session {Session} -- opened session to scylla cluster
            preimage_enable {str} -- status of preimage mode
            postimage_enable {bool} -- status of postimage mode
        """
        new_data = self.dataset_generator.build_new_dataset_for_batch()

        expected_data = self._build_expected_data(new_data, self.current_dataset, preimage_enable, postimage_enable)
        batch = self.create_batch_with_insert_stm(new_data)
        session.execute(batch)

        statement = SimpleStatement(f"SELECT * from {self.keyspace}.{self.table}_scylla_cdc_log", consistency_level=ConsistencyLevel.ALL)

        cdc_rows = list(session.execute(statement))
        self.last_timestamps = self.verify_cdc_rows(cdc_rows, expected_data, CdcLogOperations.INSERT, preimage=preimage_enable, postimage=postimage_enable, first_record=True)

    def _check_batch_with_update_all_columns_in_each_row(self, session, preimage_enable, postimage_enable):
        """update data in a batch

        Update each column for each row in each partition in the batch and verify that
        cdc table contains correct data

        Arguments:
            session {Session} -- opened session to scylla cluster
            data_set {DataGenerator} -- generator of data sets
            preimage_enable {str} -- status of preimage mode
            postimage_enable {bool} -- status of postimage mode

        """
        new_data = self.dataset_generator.build_new_dataset_for_batch()
        # because all columns are going to be updated
        # in each preimage mode all columns will be returned

        expected_data = self._build_expected_data(new_data, self.current_dataset, preimage_enable, postimage_enable)

        batch = self.create_batch_with_update_stm(new_data)
        session.execute(batch)

        where = " AND ".join(self.last_timestamps)
        statement = SimpleStatement(f"SELECT * from {self.keyspace}.{self.table}_scylla_cdc_log where {where} ALLOW FILTERING", consistency_level=ConsistencyLevel.ALL)
        cdc_rows = list(session.execute(statement))

        self.last_timestamps = self.verify_cdc_rows(cdc_rows, expected_data, CdcLogOperations.UPDATE, preimage=preimage_enable, postimage=postimage_enable, first_record=False)

    def _check_batch_with_update_only_even_columns_per_row(self, session, preimage_enable, postimage_enable):
        """update only columns with even index

        Build batch with updating only even columns for each row
        in each partition and verify cdc table content

        Arguments:
            session {Session} -- opened session to scylla cluster
            data_set {DataGenerator} -- generator of data sets
            preimage_enable {str} -- status of preimage mode
            postimage_enable {bool} -- status of postimage mode

        """

        new_data = self.dataset_generator.build_new_even_batch_dataset()
        expected_data = self._build_expected_data(new_data, self.current_dataset, preimage_enable, postimage_enable)

        batch = self.create_batch_with_update_stm(new_data)
        session.execute(batch)

        where = " AND ".join(self.last_timestamps)
        statement = SimpleStatement(f"SELECT * from {self.keyspace}.{self.table}_scylla_cdc_log where {where} ALLOW FILTERING", consistency_level=ConsistencyLevel.ALL)
        cdc_rows = list(session.execute(statement))

        self.last_timestamps = self.verify_cdc_rows(cdc_rows, expected_data, CdcLogOperations.UPDATE, preimage=preimage_enable, postimage=postimage_enable, first_record=False)

    def _check_batch_with_update_only_odd_columns_per_row(self, session, preimage_enable, postimage_enable):
        """Update only odd columns in batch

        Build batch with updating only odd column for each row
        in each partition and verify cdc table content

        Arguments:
            session {Session} -- opened session to scylla cluster
            data_set {DataGenerator} -- generator of data sets
            data_set {DataGenerator} -- generator of data sets
            preimage_enable {str} -- status of preimage mode
            postimage_enable {bool} -- status of postimage mode

        """
        new_data = self.dataset_generator.build_new_odd_batch_dataset()
        expected_data = self._build_expected_data(new_data, self.current_dataset, preimage_enable, postimage_enable)
        batch = self.create_batch_with_update_stm(new_data)
        session.execute(batch)

        where = " AND ".join(self.last_timestamps)
        statement = SimpleStatement(f"SELECT * from {self.keyspace}.{self.table}_scylla_cdc_log where {where} ALLOW FILTERING", consistency_level=ConsistencyLevel.ALL)
        cdc_rows = list(session.execute(statement))

        self.last_timestamps = self.verify_cdc_rows(cdc_rows, expected_data, CdcLogOperations.UPDATE, preimage=preimage_enable, postimage=postimage_enable, first_record=False)

    def _check_batch_with_update_all_columns_with_null_for_each_row(self, session, preimage_enable, postimage_enable):
        """Update all columns with null value in a batch

        Build a batch with set the null value for all columns for
        for each rows in each partition

        Arguments:
            session {Session} -- opened session to scylla cluster
            data_set {DataGenerator} -- generator of data sets
            preimage_enable {str} -- status of preimage mode
            postimage_enable {bool} -- status of postimage mode

        """
        new_data = self.dataset_generator.build_batch_empty_data()
        expected_data = self._build_expected_data(new_data, self.current_dataset, preimage_enable, postimage_enable)
        batch = self.create_batch_with_update_stm(new_data)
        session.execute(batch)

        where = " AND ".join(self.last_timestamps)
        statement = SimpleStatement(f"SELECT * from {self.keyspace}.{self.table}_scylla_cdc_log where {where} ALLOW FILTERING", consistency_level=ConsistencyLevel.ALL)
        cdc_rows = list(session.execute(statement))

        self.last_timestamps = self.verify_cdc_rows(cdc_rows, expected_data, CdcLogOperations.UPDATE, preimage=preimage_enable, postimage=postimage_enable, first_record=False)

    def _check_batch_with_update_all_columns_for_each_row_after_delete(self, session, preimage_enable, postimage_enable):
        """Update all data columns in batch

        Build batch with updating all data columns for each
        row in each partition. if preimage enabled,
        it should contains all columns with null values

        Arguments:
            session {Session} -- opened session to scylla cluster
            preimage_enable {str} -- status of preimage mode
            postimage_enable {bool} -- status of postimage mode

        """
        new_data = self.dataset_generator.build_new_dataset_for_batch()
        # all data were removed, so preimage will be empty. same as preimage disabled

        expected_data = self._build_expected_data(new_data, self.current_dataset, preimage=preimage_enable, postimage=postimage_enable)
        batch = self.create_batch_with_update_stm(new_data)
        session.execute(batch)

        where = " AND ".join(self.last_timestamps)
        statement = SimpleStatement(f"SELECT * from {self.keyspace}.{self.table}_scylla_cdc_log where {where} ALLOW FILTERING", consistency_level=ConsistencyLevel.ALL)
        cdc_rows = list(session.execute(statement))

        self.last_timestamps = self.verify_cdc_rows(cdc_rows, expected_data, CdcLogOperations.UPDATE, preimage=preimage_enable, postimage=postimage_enable, first_record=False)

    def _build_expected_data(self, new_data, prev_data, preimage, postimage):
        expected_data = {"preimage": [], "delta": [], "postimage": []}

        if not prev_data:
            expected_data["preimage"] = []
        elif preimage == "full":
            expected_data["preimage"] = prev_data
        elif preimage == "true" or preimage is True:
            expected_data["preimage"] = self.dataset_generator.exclude_dataset(prev_data, new_data)
        else:
            expected_data["preimage"] = []

        if postimage:
            expected_data["postimage"] = self.dataset_generator.combine_datasets(prev_data, new_data)
        else:
            expected_data["postimage"] = []

        expected_data["delta"] = new_data
        self.current_dataset = self.dataset_generator.combine_datasets(prev_data, new_data)
        return expected_data

    def verify_cdc_rows(self, cdc_rows, expected_data, delta_operation, preimage=False, postimage=False, first_record=True):  # noqa: PLR0913
        preimage_rows = []
        postimage_rows = []
        delta_rows = []

        # group all cdc rows by stream_id and cdc time.
        # for simple batch it is stream_id + timestamp is single cdc group
        group_by_cdc_time = attrgetter("cdc_stream_id", "cdc_time")
        cdc_groups = []
        for key, rows in groupby(cdc_rows, key=group_by_cdc_time):
            cdc_groups.append((key, [r for r in rows]))

        last_timestamps = []

        assert len(cdc_groups) == self.num_of_partitions, f"Number of cdc groups: {len(cdc_groups)} != number of partions {self.num_of_partitions}"

        for (_, last_cdc_timestamp), rows in cdc_groups:
            # collect time stamp per stream for further filtering
            last_timestamps.append(f'"cdc$time" > {last_cdc_timestamp}')
            logger.debug(rows)
            # count number of expected rows in cdc groups
            # if only delta enabled
            expected_num_of_row = self.num_of_rows
            # if preimage enabled ("full", "true", True) and row contains data
            if preimage and not first_record:
                expected_num_of_row += self.num_of_rows
            # if postimage enabled
            if postimage:
                expected_num_of_row += self.num_of_rows

            assert len(rows) == expected_num_of_row, f"Number of rows {len(rows)} in cdc group != expected number {expected_num_of_row}"

            # cdc group consists from 3 parts.
            # first part is cdc rows with preimage status
            # for all rows affected by batch.

            if preimage and not first_record:
                preimage_rows = rows[: self.num_of_rows]
                delta_rows_start = self.num_of_rows
            else:
                delta_rows_start = 0

            # third part is cdc rows with postimage status
            # for all rows affected by batch
            if postimage:
                postimage_rows = rows[-self.num_of_rows :]
                delta_rows_end = len(rows) - self.num_of_rows
            else:
                delta_rows_end = len(rows) - 1

            # second part is cdc rows with deltas for each row
            # affected by batch.
            delta_rows = rows[delta_rows_start:delta_rows_end]
            self._verify_columns_in_rows(preimage_rows, CdcLogOperations.PREIMAGE, expected_data["preimage"])
            self._verify_columns_in_rows(delta_rows, delta_operation, expected_data["delta"])
            self._verify_columns_in_rows(postimage_rows, CdcLogOperations.POSTIMAGE, expected_data["postimage"])

        return last_timestamps

    def _verify_columns_in_rows(self, cdc_rows, expected_operation, expected_data):
        for row in cdc_rows:
            assert row.cdc_operation == expected_operation, f"Wrong delta operation {row.cdc_operation}. Expected {expected_operation}"
            # choose from expected preimage data set required row
            expected_row = next(r for r in expected_data if r.pk == row.pk and r.ck == row.ck)
            for expected_column in expected_row.cols:
                actual_column_value = getattr(row, expected_column.name)
                if self.columns_type.startswith("list") and actual_column_value:
                    actual_column_value = list(actual_column_value.values())
                assert actual_column_value == expected_column.value, f"Column {expected_column.name} has different value in cdc_row: {actual_column_value} vs expected {expected_column.value}\n {row} \n {expected_row}"
