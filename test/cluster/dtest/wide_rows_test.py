#
# Copyright (C) 2013-present The Apache Software Foundation
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import datetime
import logging
import random
import time
from collections import defaultdict
from pathlib import Path

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement, dict_factory

from dtest_class import Tester, create_ks
from tools.assertions import assert_equal_more_with_deviation, assert_less_equal_lists
from tools.cluster_topology import generate_cluster_topology
from tools.misc import wait_for_view

logger = logging.getLogger(__name__)


status_messages = (
    "I''m going to the Cassandra Summit in June!",
    "C* is awesome!",
    "All your sstables are belong to us.",
    "Just turned on another 50 C* nodes at <insert tech startup here>, scales beautifully.",
    "Oh, look! Cats, on reddit!",
    "Netflix recommendations are really good, wonder why?",
    "Spotify playlists are always giving me good tunes, wonder why?",
)

clients = ("Android", "iThing", "Chromium", "Mozilla", "Emacs")


@pytest.mark.parametrize(
    "strategy",
    [
        pytest.param("LeveledCompactionStrategy"),
        pytest.param("SizeTieredCompactionStrategy"),
        pytest.param("TimeWindowCompactionStrategy"),
        pytest.param("IncrementalCompactionStrategy"),
    ],
)
class TestWideRows(Tester):
    BLOB_SIZE_10k = 1024 * 10
    BLOB_SIZE_1MB = 1024 * 1024
    KEYSPACE_NAME = "wide_row"
    TABLE_NAME = "user_events"
    date = datetime.datetime.now()

    @pytest.fixture(autouse=True)
    def setup_compaction_strategy(self, strategy):
        self.compaction_option = f"compaction = {{'class': '{strategy}'}}"
        self.compaction_strategy = strategy

    def prepare_cluster(self, nodes=1, version=None, keyspace_name=KEYSPACE_NAME, rf=1, options_dict=None):
        logger.debug("Run test with %s compaction strategy" % self.compaction_strategy)
        logger.debug("Start cluster with %d nodes" % nodes)
        cluster = self.cluster
        if version:
            self.cluster.set_install_dir(version=version)
        options = {
            "compaction_large_data_records_per_sstable": 1000,  # For backward compatibility, after https://github.com/scylladb/scylladb/pull/29257
            "logger_log_level": {"large_data": "debug", "mc_writer": "trace"},
        }
        if options_dict:
            options.update(options_dict)
        cluster.set_configuration_options(values=options)
        cluster.populate(generate_cluster_topology(dc_num=1, rack_num=nodes)).start(wait_for_binary_proto=True)

        session = self.patient_cql_connection(cluster.nodelist())
        logger.debug("Create %s keyspace" % keyspace_name)
        create_ks(session=session, name=keyspace_name, rf=rf)
        return session

    def validation_small_entity(self, entity_type, keyspace_name, table_name):
        """
        :param entity_type: expected "partition" or "row"
        """
        for node in self.cluster.nodelist():
            session = self.patient_exclusive_cql_connection(node=node, keyspace=keyspace_name)
            system_data_size_dict = self.get_large_entity_info(session=session, keyspace_name=keyspace_name, table_name=table_name, entity_type=entity_type, expect_system_report=False)

            assert not system_data_size_dict, f"Not expected large {entity_type} found"

            # Search warning in the log
            self.search_warning(node=node, warning_text=f"Writing large {entity_type} {keyspace_name}/{table_name}", marked_logs_dict={}, expect_warning=False)

    def create_large_partition_table(self, session, table_name, with_static_column: bool = False):
        logger.debug(f"Create table {table_name} with large partition")
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (userid text, event text, value blob, "
        if with_static_column:
            create_table_query += "static_value text static, "
        create_table_query += f"PRIMARY KEY (userid, event)) with compression = {{ }} and {self.compaction_option}"
        session.execute(create_table_query)

    def create_large_partition_data(  # noqa: PLR0913
        self,
        session,
        table_name,
        partition_rows,
        partitions_num,
        one_blob_size,
        start_partition_index,
    ):
        expected_row_size = (one_blob_size + 8 + 8) * partition_rows  # aproximately partition size
        expected_rows = {}

        value = "a" * one_blob_size  # 1K value in the blob column

        logger.debug(f"Prefill table {table_name} with {partitions_num} partition(s), {partition_rows} row(s) each")
        for k in range(start_partition_index, start_partition_index + partitions_num):
            user = "user%d" % k

            # Write the first row of the partition and obtain the write timestamp
            date_str = (self.date + datetime.timedelta(0)).strftime("%Y-%m-%d")
            # We need to run the the UPDATE and the SELECT below with CL:QUORUM to avoid cases where we get an empty
            # result set when quering a node which has not yet received a replica of the data we just inserted
            query = f"UPDATE {table_name} SET value = textAsBlob('{value}') WHERE userid='{user}' and event='{date_str}'"
            statement = SimpleStatement(query, consistency_level=ConsistencyLevel.QUORUM)
            session.execute(statement)
            expected_rows[user] = expected_row_size
            # Use this timestamp below, for all the remaining rows of the partition
            # This guarantees that when using TWCS, the partition won't be segregated into two windows, breaking the test.
            query = f"SELECT writetime(value) as ts FROM {table_name} WHERE userid='{user}' and event='{date_str}'"
            statement = SimpleStatement(query, consistency_level=ConsistencyLevel.QUORUM)
            timestamp = session.execute(statement).one().ts

            for i in range(1, partition_rows):
                date_str = (self.date + datetime.timedelta(i)).strftime("%Y-%m-%d")
                session.execute(f"UPDATE {table_name} USING TIMESTAMP {timestamp} SET value = textAsBlob('{value}') WHERE userid='{user}' and event='{date_str}'")
        return expected_rows

    def create_large_row_table(self, session, table_name, columns_num, entity_type="row"):
        logger.debug(f"Create table {table_name} with large {entity_type}s")
        long_text_columns = ", ".join(["value%d blob" % i for i in range(columns_num)])
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (userid text, event text, {long_text_columns}, PRIMARY KEY (userid, event)) with compression = {{ }} and {self.compaction_option}"
        session.execute(create_table_query)

    def create_too_many_rows_table(self, session, table_name, columns_num):
        logger.debug(f"Create table {table_name} with too many rows")
        long_text_columns = ", ".join(["value%d blob" % i for i in range(columns_num)])
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (userid text, event text, {long_text_columns}, PRIMARY KEY (userid, event)) with compression = {{ }} and {self.compaction_option}"
        session.execute(create_table_query)

    def create_too_many_collection_elements_table(self, session, table_name, collection_type="map"):
        match collection_type:
            case "map":
                col_type_str = "kvmap map<text, text>"
            case "set":
                col_type_str = "myset set<text>"
            case "list":
                col_type_str = "mylist list<text>"
            case _:
                raise Exception(f"{collection_type=} is not supported by function 'create_too_many_elements_table' \nUse only 'map', 'set', or 'list''")

        logger.debug(f"Create table {table_name} with too many collection items")
        create_table_query = f"CREATE TABLE IF NOT EXISTS %s (userid text, event text, value0 blob, {col_type_str}, PRIMARY KEY (userid, event)) with compression = {{ }} and %s" % (table_name, self.compaction_option)
        logger.debug(f"Running query: {create_table_query}")
        session.execute(create_table_query)

    def create_large_row_static_data(self, session, table_name, rows_num):
        """
        This will generate varied MB-size data and insert it to requested number of rows.
        The size ranges from 1mb to 10mb since it assumed to be a threshold to trigger large partition detector.
        """
        large_data_1_mb = "x" * 1024 * 1024
        logger.debug(f"Prefill table {table_name} with {rows_num} rows")
        for index in range(1, rows_num + 1):
            userid = f"user{index}"
            event = (self.date + datetime.timedelta(index)).strftime("%Y-%m-%d")
            # Default large data threshold for cells is 1 mb, for rows it is 10 mb.
            large_data = large_data_1_mb * random.choice(range(1, 11))
            query = f"INSERT INTO {table_name} (userid, event, static_value) VALUES ('{userid}', '{event}', '{large_data}')"
            session.execute(query)

    def create_large_row_data(self, session, table_name, rows_num, columns_num, one_blob_size, start_row_index):  # noqa: PLR0913
        expected_rows = {}
        expected_row_size = columns_num * one_blob_size  # approximately row size

        logger.debug(f"Prefill table {table_name} with {rows_num} rows")
        for k in range(start_row_index, start_row_index + rows_num):
            user = "user%d" % k
            value = "a" * int(one_blob_size)
            event = (self.date + datetime.timedelta(k)).strftime("%Y-%m-%d")
            for i in range(columns_num):
                out = session.execute(f"UPDATE {table_name} SET value{i} = textAsBlob('{value}') WHERE userid='{user}' and event='{event}'")
            expected_rows[f"{user}.{event}"] = expected_row_size

        return expected_rows

    def create_too_many_rows_data(  # noqa: PLR0913
        self,
        session,
        table_name,
        rows_num,
        columns_num,
        one_blob_size,
        partition_index,
        start_row_index,
        collection_type="map",
        collection_elements=0,
        start_collection_element_index=0,
    ):
        expected_rows = {}
        expected_row_size = columns_num * one_blob_size  # approximately row size

        logger.debug(f"Prefill table {table_name} with {rows_num} rows")
        for k in range(start_row_index, start_row_index + rows_num):
            user = f"user{partition_index}"
            value = "a" * int(one_blob_size)
            event = (self.date + datetime.timedelta(k)).strftime("%Y-%m-%d")
            for i in range(columns_num):
                session.execute(f"UPDATE {table_name} SET value{i} = textAsBlob('{value}') WHERE userid='{user}' and event='{event}'")
            if collection_elements:
                match collection_type:
                    case "map":
                        collection_type_string = "kvmap['key{j}'] = 'val{j}'"
                    case "set":
                        collection_type_string = "myset = myset + {{ 'cat{j}' }}"
                    case "list":
                        collection_type_string = "mylist = mylist + [ '{j}' ]"
                    case _:
                        raise Exception(f"{collection_type=} is not supported by function 'create_too_many_rows_data' \nUse only 'map', 'set', or 'list''")
                for i in range(start_collection_element_index, start_collection_element_index + collection_elements):
                    logger.debug(f"UPDATE {table_name} SET {collection_type_string.format(j=i)} WHERE userid='{user}' and event='{event}'")
                    session.execute(f"UPDATE {table_name} SET {collection_type_string.format(j=i)} WHERE userid='{user}' and event='{event}'")
            expected_rows[f"{user}.{event}"] = expected_row_size

        return expected_rows

    def delete_too_many_rows_data(  # noqa: PLR0913
        self,
        session,
        table_name,
        rows_num,
        columns_num,
        start_col_index,
        partition_index,
        start_row_index,
        collection_type="map",
        collection_elements=0,
        start_collection_element_index=0,
    ):
        logger.debug(f"Delete from table {table_name} with {rows_num} rows, {columns_num} columns, and {collection_elements} collection items")
        for k in range(start_row_index, start_row_index + rows_num):
            user = f"user{partition_index}"
            event = (self.date + datetime.timedelta(k)).strftime("%Y-%m-%d")
            if columns_num:
                for i in range(start_col_index, start_col_index + columns_num):
                    session.execute(f"DELETE value{i} FROM {table_name} WHERE userid='{user}' and event='{event}'")
            elif collection_elements:
                match collection_type:
                    case "map":
                        remove_collection_element_cmd = "DELETE kvmap['key{i}'] FROM {table_name} WHERE userid='{user}' and event='{event}'"
                    case "set":
                        remove_collection_element_cmd = "UPDATE {table_name} SET myset = myset - {{ 'cat{i}' }} WHERE userid='{user}' and event='{event}'"
                    case "list":
                        remove_collection_element_cmd = "DELETE mylist[{i}] FROM {table_name} WHERE userid='{user}' and event='{event}'"
                    case _:
                        raise Exception(f"{collection_type=} is not supported by function 'delete_too_many_rows_data' \nUse only 'map', 'set', 'list''")
                for i in range(start_collection_element_index, start_collection_element_index + collection_elements):
                    logger.debug(remove_collection_element_cmd.format(i=i, table_name=table_name, user=user, event=event))
                    session.execute(remove_collection_element_cmd.format(i=i, table_name=table_name, user=user, event=event))
            else:
                session.execute(f"DELETE FROM {table_name} WHERE userid='{user}' and event='{event}'")

    def search_warning(self, node, warning_text, marked_logs_dict, expect_warning=True):
        from_mark = marked_logs_dict.get(node.name) or 0

        try:
            res = node.watch_log_for(exprs=warning_text, from_mark=from_mark, timeout=10)
        except Exception:  # noqa: BLE001
            res = None

        if expect_warning:
            assert res, f"Expected warning {warning_text} is not found in the log of node {node.name}"
        else:
            assert not res, f"Non expect warning {warning_text} was found in the log of node {node.name}"

    def check_warning(self, node, warning_text, marked_logs_dict):
        from_mark = marked_logs_dict.get(node.name) or 0

        try:
            res = node.watch_log_for(exprs=warning_text, from_mark=from_mark, timeout=10)
        except Exception:  # noqa: BLE001
            res = None

        return res is not None

    def get_cluster_system_state(self, entity_type, keyspace_name, table_name, with_collection=False):
        cluster_state = {}
        for node in self.cluster.nodelist():
            entity_info = defaultdict(int)
            rows_entity_info = defaultdict(int)
            collection_elements_info = defaultdict(int)
            entities = set()
            sstables_set = set()
            sstables_on_disk = set()
            key_appearance = defaultdict(int)
            if node.is_running():
                session = self.patient_exclusive_cql_connection(node=node, keyspace=keyspace_name, row_factory=dict_factory)
                # Get large partition/row details from system.large_partitions/large_rows tables
                data_columns = []
                if entity_type in ("row", "cell"):
                    # the clustering_key value is part of the `entity_info` key
                    # for the large rows/cells tables
                    data_columns.append("clustering_key")
                entity_size_column = f"{entity_type}_size"
                data_columns.append(entity_size_column)
                if entity_type == "partition":
                    data_columns.append("rows")
                if with_collection and entity_type == "cell":
                    data_columns.append("collection_elements")
                query = f"select sstable_name, partition_key, {','.join(data_columns)} from system.large_{entity_type}s where keyspace_name='{keyspace_name}' and table_name='{table_name}'"
                logger.debug(query)
                result = list(session.execute(query))
                logger.debug(f"{node.name} result: {result}")

                for row in result:
                    if entity_type == "partition":
                        key = row["partition_key"]
                        rows_entity_info[key] = row["rows"]
                    else:
                        key = f"{row['partition_key']}.{row['clustering_key']}"
                        if "collection_elements" in row:
                            collection_elements_info[key] = row["collection_elements"]
                    entities.add(key)
                    entity_info[key] += row[entity_size_column]
                    if entity_type == "cell":
                        key_appearance[key] += 1
                    sstables_set.add(Path(row["sstable_name"]).name)

                # Get DB files for the keyspace_name and table_nam
                files = node.get_sstables(keyspace_name, table_name)
                assert files is not None, "Data file has not found"

                for file in files:
                    sstables_on_disk.add(Path(file).name)

                node_state = {
                    "info_from_system_table": {
                        "partition_keys": entities,
                        entity_size_column: entity_info,
                        "rows": rows_entity_info,
                        "collection_elements": collection_elements_info,
                        "sstables": sstables_set,
                        "key_appearance": key_appearance,
                    },
                    "sstables_from_disk": sstables_on_disk,
                    "node_status": "UP" if node.is_running() else "DOWN",
                }
                logger.debug(f"{node.name} state: {node_state}")
                cluster_state[node.name] = node_state

        return cluster_state

    def validate_entities_recognized_as_large(self, entity_type, cluster_state, expected_count):
        large_primary_keys = set()
        key_appearance = 0
        for node_info in cluster_state.values():
            large_primary_keys.update(node_info["info_from_system_table"]["partition_keys"])
            key_appearance = sum(node_info["info_from_system_table"]["key_appearance"].values())

        if entity_type == "cell":
            entities_count = key_appearance
        else:
            entities_count = len(large_primary_keys)
        msg = f"Expected {expected_count} large {entity_type}s, in system.large_{entity_type}s, got {entities_count}"
        assert entities_count == expected_count, msg

    def validate_entities_not_recognized_as_large(self, entity_type, cluster_state, pk_max_index):
        large_primary_keys = set()
        for node_info in cluster_state.values():
            large_primary_keys.update(node_info["info_from_system_table"]["partition_keys"])

        if entity_type == "partition":
            wrong_large_entity_in_system = [key for key in large_primary_keys if int(key.replace("user", "")) > pk_max_index]
        else:
            wrong_large_entity_in_system = [key for key in large_primary_keys if int(key.split(".")[0].replace("user", "")) > pk_max_index]
        msg = f"Small {entity_type}s detected large: {'/n'.join(e for e in wrong_large_entity_in_system)}"
        assert not wrong_large_entity_in_system, msg

    def validate_entity_size(self, cluster_state, expected_entity_data_size, entity_type, data_column):
        # size_threshold (in percent) is allowable deviation for row/partition size, reported by
        # system.large_row_size/system.large_partition_size
        row_size_threshold = 3
        for node_name, node_info in cluster_state.items():
            info = node_info["info_from_system_table"][data_column]
            if expected_entity_data_size is None:
                assert not info, f"Node {node_name} has unexpected info from system table for '{entity_type}' in data_column '{data_column}': {node_info}"
                continue
            else:
                assert info, f"Node {node_name} has no info from system table for '{entity_type}' in data_column '{data_column}'"
            logger.debug(f"{node_name} info_from_system_table[{data_column}]: {info}")
            for pk, size in info.items():
                expected_size = expected_entity_data_size if type(expected_entity_data_size) is int else expected_entity_data_size.get(pk)
                msg = f'The {entity_type} with primary key "{pk}" is not reported as large {entity_type}'
                assert expected_size is not None, msg
                assert_equal_more_with_deviation(size, expected_size, row_size_threshold)

    def validate_sstables_on_disk(self, cluster_state):
        for node_name, node_info in cluster_state.items():
            if not node_info["info_from_system_table"]["partition_keys"] or node_info["node_status"] != "UP":
                continue

            sstables_from_system = sorted(list(node_info["info_from_system_table"]["sstables"]))
            sstables_from_disk = sorted(list(node_info["sstables_from_disk"]))

            assert_less_equal_lists(sstables_from_system, sstables_from_disk, msg=f"Expected sstables on the node {node_name}: {sstables_from_disk}; Actual sstables: {sstables_from_system}")

    def validate_system_table(  # noqa: PLR0913
        self,
        entity_type,
        keyspace_name,
        table_name,
        expected_entity_number,
        expected_entity_data_size,
        pk_max_index=None,
        data_column=None,
        with_collection=False,
    ):
        cluster_state = self.get_cluster_system_state(entity_type=entity_type, keyspace_name=keyspace_name, table_name=table_name, with_collection=with_collection)
        self.validate_entities_recognized_as_large(entity_type=entity_type, cluster_state=cluster_state, expected_count=expected_entity_number)
        # In case there are small partitions/rows - verify the they didn't recognized as large
        if pk_max_index is not None:
            self.validate_entities_not_recognized_as_large(entity_type=entity_type, cluster_state=cluster_state, pk_max_index=pk_max_index)

        if not data_column:
            data_column = "collection_elements" if with_collection else f"{entity_type}_size"
        expected_entity_data_size = expected_entity_data_size if expected_entity_number else None
        self.validate_entity_size(cluster_state=cluster_state, expected_entity_data_size=expected_entity_data_size, entity_type=entity_type, data_column=data_column)
        self.validate_sstables_on_disk(cluster_state=cluster_state)
        return cluster_state

    def validate_log_warnings(  # noqa: PLR0913
        self,
        cluster_state,
        entity_type,
        keyspace_name,
        table_name,
        marked_logs_dict=None,
        expect_warning=True,
    ):
        for node in self.cluster.nodelist():
            # If large partition/row wasn't found - expect don't find the warnings
            if not node.name in cluster_state or not cluster_state[node.name]["info_from_system_table"]["partition_keys"]:
                if not self.check_warning(node=node, warning_text=f"Dropping entries from large_{entity_type}s: ks = {keyspace_name}, table = {table_name},", marked_logs_dict=marked_logs_dict or {}):
                    self.search_warning(node=node, warning_text=f"Writing large {entity_type} {keyspace_name}/{table_name}:", marked_logs_dict=marked_logs_dict or {}, expect_warning=False)
                continue

            # Search warning in the log
            current_node_info = cluster_state[node.name]
            expect_warning = expect_warning if not expect_warning else bool(current_node_info["sstables_from_disk"])
            self.search_warning(node=node, warning_text=f"Writing large {entity_type} {keyspace_name}/{table_name}:", marked_logs_dict=marked_logs_dict or {}, expect_warning=expect_warning)

    def get_large_entity_info(self, session, keyspace_name, table_name, entity_type, expect_system_report=True):
        """
        :param entity_type: expected "partition" or "row"
        """
        clustering_key = "clustering_key, " if entity_type == "row" else ""
        query = f"select sstable_name, partition_key, {clustering_key}{entity_type}_size from system.large_{entity_type}s where keyspace_name='{{keyspace_name}}' and table_name='{{table_name}}'"
        result = list(session.execute(query))
        if not expect_system_report:
            assert not result, f"Not expected large {entity_type} info in the system.large_{entity_type}s, but it found"
            return None

        entity_info = defaultdict(int)
        sstables_set = set()
        for row in result:
            key = row[1] if len(row) == 3 else f"{row[1]}.{row[2]}"
            entity_info[key] += row[-1]
            sstables_set.add(row[0])
        return entity_info, sorted(list(sstables_set))

    def set_ttl_on_few_rows_in_partition(  # noqa: PLR0913
        self,
        session,
        keyspace_name,
        table_name,
        partition_num,
        expected_partitions,
        ttl_rows_amount,
    ):
        userid = "user%d" % random.randint(0, partition_num - 1)
        cks_for_ttl = list(session.execute(f"SELECT event FROM {table_name} WHERE userid='{userid}'"))

        one_blob_size = 1024
        value = "b" * one_blob_size
        ttl = 60
        # TTL part of rows in partition or full partition
        logger.debug('Update %d rows of partition where PK "%s" with TTL %d' % (ttl_rows_amount, userid, ttl))
        for i, event_row in enumerate(cks_for_ttl):
            if i < ttl_rows_amount:
                event = event_row[0]
                session.execute(f"UPDATE {table_name} USING TTL {ttl} SET value = textAsBlob('{value}') WHERE userid='{userid}' and event='{event}'")

        self.cluster.flush()
        logger.debug("Wait %d sec while the TTLed rows expiration" % ttl)
        time.sleep(ttl + 5)
        self.cluster.nodetool(f"compact {self.KEYSPACE_NAME} {self.TABLE_NAME}")
        expected_partitions.pop(userid)

        return expected_partitions

    def set_ttl_on_few_large_rows(  # noqa: PLR0913
        self,
        session,
        keyspace_name,
        table_name,
        rows_num,
        columns_num,
        expected_rows,
    ):
        userid = "user%d" % random.randint(0, rows_num - 1)
        ck_for_ttl = list(session.execute(f"SELECT event FROM {table_name} WHERE userid='{userid}' LIMIT 1"))

        value = "b" * self.BLOB_SIZE_10k
        ttl = 60
        # TTL one row
        event = ck_for_ttl[0][0]
        columns = ", ".join(["value%d = textAsBlob('%s')" % (i, value) for i in range(columns_num)])

        logger.debug('Update row where USERID="%s" and EVENT="%s" with TTL %d' % (userid, event, ttl))

        session.execute(f"UPDATE {table_name} USING TTL {ttl} SET {columns} WHERE userid='{userid}' and event='{event}'")

        self.cluster.flush()
        logger.debug("Wait %d sec while the TTLed rows expiration" % ttl)
        time.sleep(ttl + 5)
        self.cluster.nodetool(f"compact {self.KEYSPACE_NAME} {self.TABLE_NAME}")
        expected_rows.pop(f"{userid}.{event}")
        return expected_rows

    def mark_log_on_all_nodes(self):
        mark_log_by_node = {}
        for node in self.cluster.nodelist():
            mark_log_by_node[node.name] = node.mark_log()
        return mark_log_by_node

    def trigger_compaction_by_data_write_and_flush(self, session, entity_type, index):
        row_number = 2
        size = 1

        if entity_type == "partition":
            func = self.create_large_partition_data
        else:
            func = self.create_large_row_data

        for i in range(5):
            func(session, self.TABLE_NAME, row_number, 1, size, index)
            self.cluster.flush()
            time.sleep(0.5)
        self.cluster.compact()
        return row_number

    @pytest.mark.single_node
    def test_wide_rows(self):
        self.write_wide_rows()

    def write_wide_rows(self, version=None):
        session = self.prepare_cluster(version=version, options_dict={"compaction_large_partition_warning_threshold_mb": 1, "compaction_large_row_warning_threshold_mb": 1})
        # Simple timeline:  user -> {date: value, ...}
        logger.debug("Create Table....")
        session.execute("CREATE TABLE user_events (userid text, event timestamp, value text, PRIMARY KEY (userid, event)) WITH %s" % self.compaction_option)
        # Create a large timeline for each of a group of users:
        for user in ("ryan", "cathy", "mallen", "joaquin", "erin", "ham"):
            logger.debug("Writing values for: %s" % user)
            for day in range(5000):
                date_str = (self.date + datetime.timedelta(day)).strftime("%Y-%m-%d")
                client = random.choice(clients)
                msg = random.choice(status_messages)
                query = f"UPDATE user_events SET value = '{{msg:{msg}, client:{client}}}' WHERE userid='{user}' and event='{date_str}';"
                # logger.debug(query)
                session.execute(query)

        # Pick out an update for a specific date:
        query = "SELECT value FROM user_events WHERE userid='ryan' and event='%s'" % (self.date + datetime.timedelta(10)).strftime("%Y-%m-%d")
        rows = session.execute(query)
        for value in rows:
            logger.debug(value)
            assert len(value[0]) > 0, f"expects >0, len(value[0])={len(value[0])} "

    @pytest.mark.single_node
    def test_too_many_rows(self):
        columns_num = 14
        initial_rows_number = 200
        additional_rows_number = 1
        entity_type = "partition"

        session = self.prepare_cluster(nodes=1, rf=1, options_dict={"compaction_large_row_warning_threshold_mb": 10, "compaction_rows_count_warning_threshold": initial_rows_number})

        self.create_too_many_rows_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num)
        self.create_too_many_rows_data(session=session, table_name=self.TABLE_NAME, columns_num=columns_num, rows_num=initial_rows_number, one_blob_size=128, partition_index=0, start_row_index=0)

        self.cluster.compact()

        self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, data_column="rows", expected_entity_number=0, expected_entity_data_size=initial_rows_number)

        self.create_too_many_rows_data(session=session, table_name=self.TABLE_NAME, columns_num=columns_num, rows_num=additional_rows_number, one_blob_size=128, partition_index=0, start_row_index=initial_rows_number)
        self.cluster.compact()

        self.validate_system_table(
            entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, data_column="rows", expected_entity_number=1, expected_entity_data_size=(initial_rows_number + additional_rows_number)
        )

    @pytest.mark.single_node
    @pytest.mark.parametrize("collection_type", ["map", "set", "list"])
    def test_too_many_collection_elements(self, collection_type):
        """
        Test the sstables holding a collection with number of items over the threshold
        is recorded in system.large_cells
        Starting 5.2 (See https://github.com/scylladb/scylladb/issues/11449)
        """
        columns_num = 1
        initial_collection_elements_number = 10
        additional_collection_elements_number = 1
        entity_type = "cell"

        session = self.prepare_cluster(nodes=1, rf=1, options_dict={"compaction_collection_elements_count_warning_threshold": initial_collection_elements_number})
        node1 = self.cluster.nodelist()[0]

        self.create_too_many_collection_elements_table(session=session, table_name=self.TABLE_NAME, collection_type=collection_type)
        gc_grace_seconds = 1
        session.execute(f"ALTER TABLE {self.TABLE_NAME} WITH gc_grace_seconds = {gc_grace_seconds}")

        logger.debug("Populating table")
        self.create_too_many_rows_data(
            session=session, table_name=self.TABLE_NAME, columns_num=columns_num, rows_num=1, one_blob_size=128, partition_index=0, start_row_index=0, collection_elements=initial_collection_elements_number, collection_type=collection_type
        )

        self.cluster.compact()

        logger.debug("No large cells expected")
        self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=0, expected_entity_data_size=initial_collection_elements_number, with_collection=True)

        logger.debug("Adding collection item")
        self.create_too_many_rows_data(
            session=session,
            table_name=self.TABLE_NAME,
            columns_num=columns_num,
            rows_num=1,
            one_blob_size=128,
            partition_index=0,
            start_row_index=0,
            collection_elements=additional_collection_elements_number,
            start_collection_element_index=initial_collection_elements_number,
            collection_type=collection_type,
        )

        self.cluster.compact()

        logger.debug(f"1 large cell(s) expected")
        self.validate_system_table(
            entity_type=entity_type,
            keyspace_name=self.KEYSPACE_NAME,
            table_name=self.TABLE_NAME,
            expected_entity_number=1,
            expected_entity_data_size=(initial_collection_elements_number + additional_collection_elements_number),
            with_collection=True,
        )

        node1.nodetool("snapshot")

        start_collection_element_index = random.randint(0, initial_collection_elements_number + additional_collection_elements_number - 1)
        logger.debug("Deleting 1 colection element: key{start_collection_element_index}")
        self.delete_too_many_rows_data(
            session=session,
            table_name=self.TABLE_NAME,
            partition_index=0,
            rows_num=1,
            start_row_index=0,
            columns_num=0,
            start_col_index=0,
            collection_elements=1,
            start_collection_element_index=start_collection_element_index,
            collection_type=collection_type,
        )

        self.cluster.flush()

        logger.debug(f"Sleeping for {gc_grace_seconds + 1} seconds for gc_grace_seconds to expire")
        time.sleep(gc_grace_seconds + 1)

        logger.debug(f"Compacting {self.TABLE_NAME}")
        self.cluster.compact()

        logger.debug("No large cells expected")
        self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=0, expected_entity_data_size=initial_collection_elements_number, with_collection=True)

    @pytest.mark.single_node
    def test_column_index_stress(self):
        """Write a large number of columns to a single row and set
        'column_index_size_in_kb' to a sufficiently low value to force
        the creation of a column index. The test will then randomly
        read columns from that row and ensure that all data is
        returned. See CASSANDRA-5225.
        """
        session = self.prepare_cluster(options_dict={"column_index_size_in_kb": 1})  # reduce column_index_size_in_kb
        # value to force column index creation
        create_table_query = "CREATE TABLE test_table (row varchar, name varchar, value int, PRIMARY KEY (row, name)) WITH %s" % self.compaction_option
        session.execute(create_table_query)

        # Now insert 100,000 columns to row 'row0'
        insert_column_query = "UPDATE test_table SET value = {value} WHERE row = '{row}' AND name = '{name}';"
        for i in range(100000):
            row = "row0"
            name = "val" + str(i)
            session.execute(insert_column_query.format(value=i, row=row, name=name))

        # now randomly fetch columns: 1 to 3 at a time
        for i in range(10000):
            select_column_query = "SELECT value FROM test_table WHERE row='row0' AND name in ('{name1}', '{name2}', '{name3}');"
            values2fetch = [str(random.randint(0, 99999)) for i in range(3)]
            # values2fetch is a list of random values.  Because they are random, they will not be unique necessarily.
            # To simplify the template logic in the select_column_query I will not expect the query to
            # necessarily return 3 values.  Hence I am computing the number of unique values in values2fetch
            # and using that in the assert at the end.
            expected_rows = len(set(values2fetch))
            rows = list(session.execute(select_column_query.format(name1="val" + values2fetch[0], name2="val" + values2fetch[1], name3="val" + values2fetch[2])))
            assert len(rows) == expected_rows, f"expects {expected_rows}, actual len(rows)={len(rows)}"

    def test_large_row_with_static_cell(self):
        """
        https://github.com/scylladb/scylla/issues/6780
        1. Create table with large rows by inserting data to static cell.
        2. Flush multiple sstables ( + one node down )
        3. run a major compaction on nodes.
        """

        self.prepare_cluster(nodes=3, rf=3, options_dict={"compaction_large_partition_warning_threshold_mb": 1, "hinted_handoff_enabled": False})
        node1, node2 = self.cluster.nodelist()[0:2]
        logger.debug(f"Stop {node2.name}")
        node2.stop(wait_other_notice=True)

        session = self.patient_cql_connection(node1, keyspace=self.KEYSPACE_NAME)
        self.create_large_partition_table(session=session, table_name=self.TABLE_NAME, with_static_column=True)
        logger.debug("Create large rows with static cell content and flush multiple times")
        for _ in range(10):
            self.create_large_row_static_data(session=session, table_name=self.TABLE_NAME, rows_num=10)
            self.cluster.flush()
        logger.debug(f"Start and repair {node2.name}")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node2.repair()
        logger.debug("Run compaction")
        self.cluster.compact()

    def test_large_partition_detector_with_node_stop(self):
        """
        Create table with one large row when one node is stopped and validate that row is reported in the
        system.large_rows table and there are warning in the log
        """
        entity_type = "partition"
        partition_num = 1
        extra_partitions = 0

        self.prepare_cluster(nodes=3, rf=3, options_dict={"compaction_large_partition_warning_threshold_mb": 1, "hinted_handoff_enabled": False})

        node1, node2 = self.cluster.nodelist()[0:2]
        logger.debug(f"Stop {node2.name}")
        node2.stop(wait_other_notice=True)

        session = self.patient_cql_connection(node1, keyspace=self.KEYSPACE_NAME)
        self.create_large_partition_table(session=session, table_name=self.TABLE_NAME)
        expected_partition_data_size = self.create_large_partition_data(session=session, table_name=self.TABLE_NAME, partition_rows=10000, partitions_num=partition_num, one_blob_size=1024, start_partition_index=0)

        self.cluster.flush()

        extra_partitions += self.trigger_compaction_by_data_write_and_flush(session, entity_type, partition_num)

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=partition_num, expected_entity_data_size=expected_partition_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME)

        logger.debug(f"Start {node2.name}")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node2.repair()

        extra_partitions += self.trigger_compaction_by_data_write_and_flush(session, entity_type, partition_num + extra_partitions)

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=partition_num, expected_entity_data_size=expected_partition_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME)

    @pytest.mark.single_node
    def test_large_partition_detector_multipartition(self):
        """
        Create table with 10 large partition and validate that partition is reported in the system.large_partitions
        table and there are warning in the log
        """
        partition_rows = 15000
        partition_num = 5
        entity_type = "partition"

        session = self.prepare_cluster(nodes=1, rf=1, options_dict={"compaction_large_partition_warning_threshold_mb": 1})

        pk_max_index = None
        self.create_large_partition_table(session=session, table_name=self.TABLE_NAME)
        expected_partition_data_size = self.create_large_partition_data(session=session, table_name=self.TABLE_NAME, partition_rows=partition_rows, partitions_num=partition_num, one_blob_size=1024, start_partition_index=0)
        self.cluster.flush()

        self.trigger_compaction_by_data_write_and_flush(session, entity_type, partition_num)

        cluster_state = self.validate_system_table(
            entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=partition_num, expected_entity_data_size=expected_partition_data_size, pk_max_index=pk_max_index
        )
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME)

    @pytest.mark.single_node
    def test_large_partition_detector_with_small_partitions(self):
        """
        Create table with one large partition and one small partition and validate that partition is reported in
        the system.large_partitions table and there are warning in the log
        """
        partition_rows = 10000
        partition_num = 1
        small_partition_num = 10
        entity_type = "partition"

        session = self.prepare_cluster(nodes=1, rf=1, options_dict={"compaction_large_partition_warning_threshold_mb": 4})

        self.create_large_partition_table(session=session, table_name=self.TABLE_NAME)
        expected_partition_data_size = self.create_large_partition_data(session=session, table_name=self.TABLE_NAME, partition_rows=partition_rows, partitions_num=partition_num, one_blob_size=1024, start_partition_index=0)

        pk_max_index = partition_num - 1
        self.create_large_partition_data(session=session, table_name=self.TABLE_NAME, partition_rows=3000, partitions_num=small_partition_num, one_blob_size=1024, start_partition_index=partition_num + 1)
        self.cluster.flush()

        self.trigger_compaction_by_data_write_and_flush(session, entity_type, partition_num + small_partition_num)

        cluster_state = self.validate_system_table(
            entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=partition_num, expected_entity_data_size=expected_partition_data_size, pk_max_index=pk_max_index
        )
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME)

    @pytest.mark.single_node
    def test_large_partition_detector_with_ttl_on_few_row_in_partition(self):
        """
        Validate that when most of rows in the large partition are expired, this partition is not reported in
        the system.large_partitions table and there are no warning in the log
        """
        partition_rows = 10000
        partition_num = 1
        extra_partitions = 0
        entity_type = "partition"

        session = self.prepare_cluster(nodes=1, rf=1, options_dict={"compaction_large_partition_warning_threshold_mb": 4})

        self.create_large_partition_table(session=session, table_name=self.TABLE_NAME)
        expected_partition_data_size = self.create_large_partition_data(session=session, table_name=self.TABLE_NAME, partition_rows=partition_rows, partitions_num=partition_num, one_blob_size=1024, start_partition_index=0)

        self.cluster.flush()

        extra_partitions += self.trigger_compaction_by_data_write_and_flush(session, entity_type, partition_num)

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=partition_num, expected_entity_data_size=expected_partition_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME)

        expected_partitions = self.set_ttl_on_few_rows_in_partition(
            session=session, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, partition_num=partition_num, expected_partitions=expected_partition_data_size, ttl_rows_amount=partition_rows - 1000
        )
        extra_partitions += self.trigger_compaction_by_data_write_and_flush(session, entity_type, partition_num + extra_partitions)

        mark_logs = self.mark_log_on_all_nodes()

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=partition_num - 1, expected_entity_data_size=expected_partitions)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, marked_logs_dict=mark_logs)

    @pytest.mark.single_node
    def test_large_partition_detector_with_ttl_on_partition(self):
        """
        Validate that when all rows in the large partition are expired, this partition is not reported in
        the system.large_partitions table and there are no warning in the log
        """
        partition_rows = 10000
        partition_num = 1
        extra_partitions = 0
        entity_type = "partition"

        session = self.prepare_cluster(nodes=1, rf=1, options_dict={"compaction_large_partition_warning_threshold_mb": 4})

        self.create_large_partition_table(session=session, table_name=self.TABLE_NAME)
        expected_partition_data_size = self.create_large_partition_data(session=session, table_name=self.TABLE_NAME, partition_rows=partition_rows, partitions_num=partition_num, one_blob_size=1024, start_partition_index=0)

        self.cluster.flush()

        extra_partitions += self.trigger_compaction_by_data_write_and_flush(session, entity_type, partition_num)

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=partition_num, expected_entity_data_size=expected_partition_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME)

        expected_partitions = self.set_ttl_on_few_rows_in_partition(
            session=session, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, partition_num=partition_num, expected_partitions=expected_partition_data_size, ttl_rows_amount=partition_rows
        )
        extra_partitions += self.trigger_compaction_by_data_write_and_flush(session, entity_type, partition_num + extra_partitions)

        mark_logs = self.mark_log_on_all_nodes()

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=partition_num - 1, expected_entity_data_size=expected_partitions)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, marked_logs_dict=mark_logs)

    def test_large_row_detector_with_node_stop(self):
        """
        Create table with one large row when one node is stopped and validate that row is reported in the
        system.large_rows table and there are warning in the log
        """
        rows_number = 5
        entity_type = "row"
        columns_num = 15
        extra_rows = 0

        self.prepare_cluster(nodes=3, rf=3, options_dict={"compaction_large_row_warning_threshold_mb": 1, "hinted_handoff_enabled": False})

        node1, node2 = self.cluster.nodelist()[0:2]
        logger.debug(f"Stop {node2.name}")
        node2.stop(wait_other_notice=True)

        session = self.patient_cql_connection(node1, keyspace=self.KEYSPACE_NAME)
        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num)
        expected_rows_data_size = self.create_large_row_data(session=session, table_name=self.TABLE_NAME, columns_num=columns_num, rows_num=rows_number, one_blob_size=self.BLOB_SIZE_10k * 10, start_row_index=0)
        self.cluster.flush()
        extra_rows += self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number)

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=rows_number, expected_entity_data_size=expected_rows_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME)

        logger.debug(f"Start {node2.name}")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node2.repair()

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=rows_number, expected_entity_data_size=expected_rows_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME)

    @pytest.mark.single_node
    def test_simple_large_row_detector(self):
        """
        Create table with large rows. Validate that it's reported in the
        system.large_rows table and there are warning in the log
        """
        columns_num = 14
        rows_number = 5
        entity_type = "row"

        session = self.prepare_cluster(nodes=1, rf=1, options_dict={"compaction_large_row_warning_threshold_mb": 1})

        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num)
        expected_rows_data_size = self.create_large_row_data(session=session, table_name=self.TABLE_NAME, columns_num=columns_num, rows_num=rows_number, one_blob_size=self.BLOB_SIZE_10k * 10, start_row_index=0)

        self.cluster.flush()

        self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number)

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=rows_number, expected_entity_data_size=expected_rows_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME)

    @pytest.mark.single_node
    def test_large_row_detector_with_ttl_on_row(self):
        """
        Create table with large rows. Validate that it's reported in the
        system.large_rows table and there are warning in the log
        """
        rows_number = 2
        columns_num = 20
        extra_rows = 0
        entity_type = "row"

        session = self.prepare_cluster(nodes=1, rf=1, options_dict={"compaction_large_row_warning_threshold_mb": 1})

        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num)
        expected_rows_data_size = self.create_large_row_data(session=session, table_name=self.TABLE_NAME, columns_num=columns_num, rows_num=rows_number, one_blob_size=self.BLOB_SIZE_10k * 10, start_row_index=0)
        self.cluster.flush()

        extra_rows += self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number)

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=rows_number, expected_entity_data_size=expected_rows_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME)

        expected_rows_data_size = self.set_ttl_on_few_large_rows(session=session, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, rows_num=rows_number, columns_num=columns_num, expected_rows=expected_rows_data_size)

        extra_rows += self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number + extra_rows)

        mark_logs = self.mark_log_on_all_nodes()

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=rows_number - 1, expected_entity_data_size=expected_rows_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, marked_logs_dict=mark_logs, expect_warning=False)

    @pytest.mark.single_node
    def test_large_row_detector_with_small_rows(self):
        """
        Create table with one large row and small rows. Validate that just large row is reported in the
        system.large_rows table and there are warning in the log
        """
        small_rows_number = 10
        rows_number = 1
        columns_num = 12
        entity_type = "row"

        session = self.prepare_cluster(nodes=1, rf=1, options_dict={"compaction_large_row_warning_threshold_mb": 1})

        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num)
        # Insert large row
        expected_rows_data_size = self.create_large_row_data(session=session, table_name=self.TABLE_NAME, columns_num=columns_num, rows_num=rows_number, one_blob_size=self.BLOB_SIZE_10k * 10, start_row_index=0)

        # Insert small rows
        maximum_primary_key_value = rows_number - 1
        self.create_large_row_data(session=session, table_name=self.TABLE_NAME, one_blob_size=10, columns_num=columns_num, rows_num=small_rows_number, start_row_index=rows_number)

        self.cluster.flush()

        self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number + small_rows_number)

        cluster_state = self.validate_system_table(
            entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=rows_number, expected_entity_data_size=expected_rows_data_size, pk_max_index=maximum_primary_key_value
        )
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME)

    @pytest.mark.single_node
    def test_large_partition_detector_small_partition(self):
        """
        Create table with one small partition and validate that partition isn't reported in the system.large_partitions
        table and there are no warning in the log
        """
        entity_type = "partition"
        session = self.prepare_cluster()
        self.create_large_partition_table(session=session, table_name=self.TABLE_NAME)
        self.create_large_partition_data(session=session, table_name=self.TABLE_NAME, partition_rows=600, partitions_num=1, one_blob_size=1024, start_partition_index=0)
        self.cluster.flush()

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=0, expected_entity_data_size=None)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expect_warning=False)

    @pytest.mark.single_node
    def test_large_row_detector_small_row(self):
        """
        Create table with one small row and validate that row isn't reported in the system.large_rows
        table and there are no warning in the log
        """
        entity_type = "row"
        columns_num = 10
        session = self.prepare_cluster()
        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num)
        self.create_large_row_data(session=session, table_name=self.TABLE_NAME, columns_num=columns_num, rows_num=1, one_blob_size=self.BLOB_SIZE_10k, start_row_index=0)
        self.cluster.flush()

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=0, expected_entity_data_size=None)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expect_warning=False)

    def test_large_row_in_materialized_view(self):
        """
        Create table with one large row. Create materialized view on the table.
        Validate that just large row is reported in the system.large_rows table and there are warning in the log for
        both base table and materialized view
        """
        rows_number = 1
        columns_num = 200
        entity_type = "row"
        view_name = "%s_view" % self.TABLE_NAME

        session = self.prepare_cluster(nodes=3, rf=3, options_dict={"compaction_large_row_warning_threshold_mb": 1})

        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num)
        # Insert large row
        expected_rows_data_size = self.create_large_row_data(session=session, table_name=self.TABLE_NAME, columns_num=columns_num, rows_num=rows_number, one_blob_size=self.BLOB_SIZE_10k, start_row_index=0)

        session.execute(f"create materialized view {view_name} as select * from {self.TABLE_NAME} where userid is not null and event is not null primary key (userid, event)")
        wait_for_view(cluster=self.cluster, session=session, ks=self.KEYSPACE_NAME, view=view_name)
        self.cluster.flush()

        self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number)

        # Validate base table
        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=rows_number, expected_entity_data_size=expected_rows_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME)

        # Validate view
        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=view_name, expected_entity_number=rows_number, expected_entity_data_size=expected_rows_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=view_name)

    def test_large_partition_in_materialized_view(self):
        """
        Create table with one large partition. Create materialized view on the table.
        Validate that just large row is reported in the system.large_rows table and there are warning in the log for
        both base table and materialized view
        """
        partition_rows = 15000
        partition_num = 1
        entity_type = "partition"
        view_name = "%s_view" % self.TABLE_NAME

        session = self.prepare_cluster(nodes=3, rf=3, options_dict={"compaction_large_partition_warning_threshold_mb": 1})

        self.create_large_partition_table(session=session, table_name=self.TABLE_NAME)
        expected_partition_data_size = self.create_large_partition_data(session=session, table_name=self.TABLE_NAME, partition_rows=partition_rows, partitions_num=partition_num, one_blob_size=1024, start_partition_index=0)

        session.execute(f"create materialized view {view_name} as select * from {self.TABLE_NAME} where userid is not null and event is not null primary key (userid, event)")
        wait_for_view(cluster=self.cluster, session=session, ks=self.KEYSPACE_NAME, view=view_name)

        self.cluster.nodetool(f"compact {self.KEYSPACE_NAME} {self.TABLE_NAME}")

        # Validate base table
        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=partition_num, expected_entity_data_size=expected_partition_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME)

        # Validate view
        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=view_name, expected_entity_number=partition_num, expected_entity_data_size=expected_partition_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=view_name)

    @pytest.mark.single_node
    def test_large_cell_detector_with_small_cells(self):
        """
        Create table with one large cell and few small cells. Validate that just large cell is reported in the
        system.large_cells table and there are warning in the log
        """
        rows_number = 10
        columns_num = 3
        entity_type = "cell"
        session = self.prepare_cluster()

        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num, entity_type=entity_type)
        # Insert large row
        expected_cells_data_size = self.create_large_row_data(session=session, table_name=self.TABLE_NAME, columns_num=columns_num, rows_num=rows_number, one_blob_size=self.BLOB_SIZE_1MB, start_row_index=0)

        # Insert small cells
        maximum_primary_key_value = rows_number - 1
        self.create_large_row_data(session=session, table_name=self.TABLE_NAME, one_blob_size=self.BLOB_SIZE_10k, columns_num=columns_num, rows_num=rows_number, start_row_index=rows_number)

        self.cluster.flush()

        self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number + rows_number)

        cluster_state = self.validate_system_table(
            entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=columns_num * rows_number, expected_entity_data_size=expected_cells_data_size, pk_max_index=maximum_primary_key_value
        )
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME)

    @pytest.mark.single_node
    def test_multiple_rows_with_large_cells_detector(self):
        """
        Create table with 10 large cells. Validate that it's reported in the
        system.large_cells table and there are warning in the log
        """
        columns_num = 3
        rows_number = 10
        entity_type = "cell"

        session = self.prepare_cluster()

        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num, entity_type=entity_type)
        expected_rows_data_size = self.create_large_row_data(session=session, table_name=self.TABLE_NAME, rows_num=rows_number, columns_num=columns_num, one_blob_size=self.BLOB_SIZE_1MB, start_row_index=0)
        self.cluster.flush()

        self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number)

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=columns_num * rows_number, expected_entity_data_size=expected_rows_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME)

    @pytest.mark.single_node
    def test_multiple_columns_with_large_cells_detector(self):
        """
        Create table with 10 large cells and 5 rows (total of 50 large cell warnings). Validate that they are
        reported in the system.large_cells table and there are warning in the log
        """
        columns_num = 10
        rows_number = 5
        entity_type = "cell"

        session = self.prepare_cluster()

        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num, entity_type=entity_type)
        expected_rows_data_size = self.create_large_row_data(session=session, table_name=self.TABLE_NAME, rows_num=rows_number, columns_num=columns_num, one_blob_size=self.BLOB_SIZE_1MB, start_row_index=0)
        self.cluster.flush()

        self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number)

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=columns_num * rows_number, expected_entity_data_size=expected_rows_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME)

    @pytest.mark.single_node
    def test_multiple_rows_and_columns_with_large_cells_detector(self):
        """
        Create table with 5 large cells on 10 rows. Validate that it's reported in the
        system.large_cells table and there are warning in the log
        """
        columns_num = 5
        rows_number = 10
        entity_type = "cell"

        session = self.prepare_cluster()

        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num, entity_type=entity_type)
        expected_rows_data_size = self.create_large_row_data(session=session, table_name=self.TABLE_NAME, rows_num=rows_number, columns_num=columns_num, one_blob_size=self.BLOB_SIZE_1MB, start_row_index=0)
        self.cluster.flush()

        self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number)

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=columns_num * rows_number, expected_entity_data_size=expected_rows_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME)

    def test_large_cell_in_materialized_view(self):
        """
        Create table with one large cell. Create materialized view on the table.
        Validate that just large cell is reported in the system.large_cells table and there are warning in the
        log for both base table and materialized view
        """
        rows_number = 10
        columns_num = 10
        entity_type = "cell"
        view_name = "%s_view" % self.TABLE_NAME

        session = self.prepare_cluster(nodes=3, rf=3)

        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num, entity_type=entity_type)
        # Insert large row
        expected_cells_data_size = self.create_large_row_data(session=session, table_name=self.TABLE_NAME, columns_num=columns_num, rows_num=rows_number, one_blob_size=self.BLOB_SIZE_1MB, start_row_index=0)

        session.execute(f"create materialized view {view_name} as select * from {self.TABLE_NAME} where userid is not null and event is not null primary key (userid, event)")
        wait_for_view(cluster=self.cluster, session=session, ks=self.KEYSPACE_NAME, view=view_name)
        self.cluster.flush()

        self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number)

        # Validate base table
        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=columns_num * rows_number, expected_entity_data_size=expected_cells_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME)

        # Validate view
        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=view_name, expected_entity_number=columns_num * rows_number, expected_entity_data_size=expected_cells_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=view_name)

    @pytest.mark.single_node
    def test_large_cell_detector_with_ttl_on_row(self):
        """
        Create table with large cells. Validate that it's reported in the
        system.large_cells table and there are warning in the log.
        After that, we create more data, setting TTL to some of them and wait for the TTL timeout and then
        verify they are not reported anymore.
        """
        rows_number = 10
        columns_num = 6
        extra_rows = 0
        entity_type = "cell"

        session = self.prepare_cluster()

        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num, entity_type=entity_type)
        expected_cells_data_size = self.create_large_row_data(session=session, table_name=self.TABLE_NAME, columns_num=columns_num, rows_num=rows_number, one_blob_size=self.BLOB_SIZE_1MB, start_row_index=0)
        self.cluster.flush()

        extra_rows += self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number)

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=columns_num * rows_number, expected_entity_data_size=expected_cells_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME)

        expected_cells_data_size = self.set_ttl_on_few_large_rows(session=session, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, rows_num=rows_number, columns_num=columns_num, expected_rows=expected_cells_data_size)

        extra_rows += self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number + extra_rows)

        mark_logs = self.mark_log_on_all_nodes()

        cluster_state = self.validate_system_table(
            entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=columns_num * rows_number - columns_num, expected_entity_data_size=expected_cells_data_size
        )
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, marked_logs_dict=mark_logs, expect_warning=False)

    @pytest.mark.single_node
    def test_large_cell_after_threshold_change(self):
        """
        Create table with ten large cells and few smaller cells after threshold changing. Validate that just cell
        bigger than the new threshold is reported in the system.large_cells table and there are warning in the log
        """
        small_rows = 20
        rows_number = 10
        columns_num = 3
        extra_rows = 0
        entity_type = "cell"
        session = self.prepare_cluster(options_dict={"compaction_large_cell_warning_threshold_mb": 2})

        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num, entity_type=entity_type)
        # Insert small cells
        maximum_primary_key_value = rows_number - 1
        self.create_large_row_data(session=session, table_name=self.TABLE_NAME, one_blob_size=self.BLOB_SIZE_1MB, columns_num=columns_num, rows_num=small_rows, start_row_index=0)

        self.cluster.flush()

        extra_rows += self.trigger_compaction_by_data_write_and_flush(session, entity_type, small_rows)

        # this will validate that no alerts were triggered so far
        self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=0, expected_entity_data_size=0, pk_max_index=0)

        # Insert large row
        expected_cells_data_size = self.create_large_row_data(session=session, table_name=self.TABLE_NAME, columns_num=columns_num, rows_num=rows_number, one_blob_size=self.BLOB_SIZE_1MB * 2, start_row_index=small_rows + extra_rows)

        # Insert small cells
        maximum_primary_key_value = small_rows + rows_number + extra_rows - 1
        self.create_large_row_data(session=session, table_name=self.TABLE_NAME, one_blob_size=self.BLOB_SIZE_1MB, columns_num=columns_num, rows_num=rows_number, start_row_index=rows_number + small_rows + extra_rows)

        self.cluster.flush()

        extra_rows += self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number + rows_number + small_rows + extra_rows)

        cluster_state = self.validate_system_table(
            entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=columns_num * rows_number, expected_entity_data_size=expected_cells_data_size, pk_max_index=maximum_primary_key_value
        )
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME)

    @pytest.mark.single_node
    def test_large_cell_and_then_increase_threshold(self):
        """
        Create table with one large cell and few smaller cells after threshold changing. Validate that just cell
        bigger than the new threshold is reported in the system.large_cells table and there are warning in the log
        """
        small_rows = 10
        rows_number = 5
        columns_num = 3
        extra_rows = 0
        entity_type = "cell"
        session = self.prepare_cluster()

        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num, entity_type=entity_type)
        # Insert large cells that after increasing the threshold should disappear
        maximum_primary_key_value = rows_number - 1
        expected_cells_data_size = self.create_large_row_data(session=session, table_name=self.TABLE_NAME, one_blob_size=self.BLOB_SIZE_1MB, columns_num=columns_num, rows_num=rows_number, start_row_index=0)

        self.cluster.flush()
        extra_rows += self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number)

        # this will validate that no alerts were triggered so far
        cluster_state = self.validate_system_table(
            entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=columns_num * rows_number, expected_entity_data_size=expected_cells_data_size, pk_max_index=maximum_primary_key_value
        )
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME)

        # Increase the threshold and see the large cell warnings disappear
        logger.debug("increasing warning threshold to 2mb")
        self.cluster.set_configuration_options({"compaction_large_cell_warning_threshold_mb": 2})

        # To apply threshold change, nodes must be restarted
        for node in self.cluster.nodelist():
            logger.debug(f"restarting node {node.name} to have threshold change to take effect")
            node.stop()
            node.start(wait_other_notice=True, wait_for_binary_proto=True)

        # reconnect after single node restart to prevent NoHostAvailable
        session = self.patient_cql_connection(self.cluster.nodelist(), keyspace=self.KEYSPACE_NAME)

        self.cluster.flush()
        extra_rows += self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number + extra_rows)

        # Insert what small cells (or what is not enough for triggering large cell warnings
        self.create_large_row_data(session=session, table_name=self.TABLE_NAME, one_blob_size=self.BLOB_SIZE_1MB, columns_num=columns_num, rows_num=small_rows, start_row_index=rows_number + extra_rows)

        self.cluster.flush()
        extra_rows += self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number + small_rows + extra_rows)

        # Now we expect that the previous warnings will be removed
        self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=0, expected_entity_data_size=0, pk_max_index=0)

    @pytest.mark.single_node
    def test_large_cell_detector_with_full_compaction(self):
        """
        Create table with one large cell and few small cells. Validate that just large cell is reported in the
        system.large_cells table and there are warning in the log with running full compaction
        """
        rows_num = 10
        columns_num = 10
        entity_type = "cell"
        session = self.prepare_cluster()

        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num, entity_type=entity_type)
        # Insert large row
        expected_cells_data_size = self.create_large_row_data(session=session, table_name=self.TABLE_NAME, columns_num=columns_num, rows_num=rows_num, one_blob_size=self.BLOB_SIZE_1MB, start_row_index=0)

        # Insert small cells
        maximum_primary_key_value = rows_num - 1
        self.create_large_row_data(session=session, table_name=self.TABLE_NAME, one_blob_size=self.BLOB_SIZE_1MB / 2, columns_num=columns_num, rows_num=rows_num, start_row_index=rows_num)

        self.cluster.nodetool(f"compact {self.KEYSPACE_NAME} {self.TABLE_NAME}")

        cluster_state = self.validate_system_table(
            entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=columns_num * rows_num, expected_entity_data_size=expected_cells_data_size, pk_max_index=maximum_primary_key_value
        )
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME)

    @pytest.mark.single_node
    def test_large_row_detector_with_full_compaction(self):
        """
        Create table with one large row and small rows. Validate that just large row is reported in the
        system.large_rows table and there are warning in the log with running full compaction
        """
        rows_num = 1
        columns_num = 15
        small_row_num = 10
        entity_type = "row"

        session = self.prepare_cluster(options_dict={"compaction_large_row_warning_threshold_mb": 1})

        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num)
        # Insert large row
        expected_rows_data_size = self.create_large_row_data(session=session, table_name=self.TABLE_NAME, columns_num=columns_num, rows_num=rows_num, one_blob_size=self.BLOB_SIZE_10k * 10, start_row_index=0)

        # Insert small rows
        maximum_primary_key_value = rows_num - 1
        self.create_large_row_data(session=session, table_name=self.TABLE_NAME, one_blob_size=10, columns_num=columns_num, rows_num=small_row_num, start_row_index=rows_num)

        self.cluster.nodetool(f"compact {self.KEYSPACE_NAME} {self.TABLE_NAME}")

        cluster_state = self.validate_system_table(
            entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=rows_num, expected_entity_data_size=expected_rows_data_size, pk_max_index=maximum_primary_key_value
        )
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME)

    @pytest.mark.single_node
    def test_large_partition_detector_with_full_compaction(self):
        """
        Create table with one large partition and one small partition and validate that partition is reported in
        the system.large_partitions table and there are warning in the log with running full compaction
        """
        partition_rows = 10000
        partition_num = 1
        entity_type = "partition"

        session = self.prepare_cluster(options_dict={"compaction_large_partition_warning_threshold_mb": 4})

        self.create_large_partition_table(session=session, table_name=self.TABLE_NAME)
        expected_partition_data_size = self.create_large_partition_data(session=session, table_name=self.TABLE_NAME, partition_rows=partition_rows, partitions_num=partition_num, one_blob_size=1024, start_partition_index=0)

        pk_max_index = partition_num - 1
        self.create_large_partition_data(session=session, table_name=self.TABLE_NAME, partition_rows=3000, partitions_num=10, one_blob_size=1024, start_partition_index=partition_num + 1)
        self.cluster.nodetool(f"compact {self.KEYSPACE_NAME} {self.TABLE_NAME}")

        # adding more data and running flush again to give time to the compaction of the large partition to finish
        self.create_large_partition_data(session=session, table_name=self.TABLE_NAME, partition_rows=3000, partitions_num=10, one_blob_size=1024, start_partition_index=partition_num + partition_num + 1)
        self.cluster.nodetool(f"compact {self.KEYSPACE_NAME} {self.TABLE_NAME}")

        cluster_state = self.validate_system_table(
            entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME, expected_entity_number=partition_num, expected_entity_data_size=expected_partition_data_size, pk_max_index=pk_max_index
        )
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME, table_name=self.TABLE_NAME)
