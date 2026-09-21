import datetime
import json
import logging
import os
import random
import re
import shutil
import string
import subprocess
import tempfile
import time
import uuid

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from ccmlib.common import BIN_DIR, join_bin
from ccmlib.node import NodetoolError, ToolError
from ccmlib.scylla_node import ScyllaNode

from dtest_class import Tester, create_cf, create_ks
from dtest_setup_overrides import DTestSetupOverrides
from tools.assertions import assert_one
from tools.cassandra_helpers import CassandraCluster, java_version_exist
from tools.cluster_topology import generate_cluster_topology
from tools.data import (
    check_c1c2_result_one,
    create_c1c2_table,
    create_index,
    drop_table,
    query_c1c2,
    rows_to_list,
)
from tools.files import (
    copy_files_to,
    get_node_cf_dir,
    get_sstables_files,
    safe_mkdtemp,
)
from tools.marks import issue_open, with_feature
from tools.misc import ImmutableMapping
from tools.retrying import retrying
from tools.stress import create_stress_compatible_table
from tools.tables_view_manager import wait_for_view

logger = logging.getLogger(__name__)


class BaseHelpers(Tester):
    @staticmethod
    def populate_cluster(cluster, extra_values=None, nodes=1):
        # Disable hinted handoff and set batch commit log so this doesn't
        # interfere with the test (this must be after the populate)
        values = {"hinted_handoff_enabled": False}
        if extra_values:
            values.update(extra_values)
        cluster.set_configuration_options(values, batch_commitlog=True)
        logger.debug(f"Starting a cluster of {nodes} node(s)...")
        cluster.populate(nodes)

    @staticmethod
    def start_cluster(cluster):
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)

    @staticmethod
    def get_node(cluster, node_idx):
        return cluster.nodelist()[node_idx]

    def start_cluster_and_get_node1(self, nodes: dict | int = 1, configuration_options: dict | None = None):
        cluster = self.cluster
        if configuration_options:
            logger.debug(f"Setting cluster configuration_options: {configuration_options}")
            cluster.set_configuration_options(values=configuration_options)

        self.populate_cluster(cluster, nodes=nodes)
        self.start_cluster(cluster)
        node1 = self.get_node(cluster, 0)
        return node1

    @staticmethod
    def copy_files_to(from_dir, to_dir):
        copy_files_to(from_dir, to_dir, files_only=True)

    def check_number_of_rows(  # noqa: PLR0913
        self,
        node,
        expected_number_of_rows,
        keyspace="ks",
        table="cf",
        consistency_level=ConsistencyLevel.ONE,
        is_view=False,
    ):
        logger.debug(f"Checking rows on {node.name}, {keyspace}.{table}...")
        query = f"SELECT COUNT(*) FROM {table}"
        statement = SimpleStatement(query, consistency_level=consistency_level)
        s = self.patient_cql_connection(node, keyspace)
        if is_view:
            # Before checking the number of rows we wait until the view is build, but that does not include waiting until view updates are generated
            # after loading base table data. We also don't have an API to wait for that, so we just retry until the view has the expected number of rows
            @retrying(num_attempts=60, sleep_time=1, allowed_exceptions=(AssertionError,))
            def wait_for_view():
                result = list(s.execute(statement))
                assert result[0].count == expected_number_of_rows, f"Expected {expected_number_of_rows} rows in {keyspace}.{table} on {node.name}. Got {result[0].count}"

            wait_for_view()

    def get_all_rows_for_check(self, node1):
        logger.info("Checking rows content on node1...")
        query = "SELECT * FROM ks.cf"
        statement = SimpleStatement(query)
        s = self.patient_cql_connection(node1, "ks")
        result = list(s.execute(statement))
        logger.info(result)
        return result

    def create_ks_and_cf(self, node, columns, compression, compact_storage, query=None):
        logger.info("Creating a CQL connection...")
        session = self.patient_cql_connection(node)

        logger.info("Creating a keyspace 'ks'...")
        create_ks(session, "ks", 1)

        logger.info("Creating a column family 'cf'...")
        if isinstance(query, str):
            session.execute(query)
            time.sleep(0.2)
        elif query is not None:
            for q in query:
                session.execute(q)
                time.sleep(0.2)
        else:
            create_cf(session, "cf", read_repair=0.0, columns=columns, compression=compression, compact_storage=compact_storage)

        logger.info("Flushing a keyspace...")
        node.nodetool("flush -- ks")

    def get_cassandra_sstable_dir(self, version, migrated_files_dir):
        return f"{os.path.dirname(os.path.realpath(__file__))}/cassandra-sstables/migration/{version}/{migrated_files_dir}"

    def load_migrated_tables(
        self,
        node,
        migrated_files_dir,
        ks="ks",
        cf="cf",
        partitioner="org.apache.cassandra.dht.Murmur3Partitioner",
    ):
        cassandra_sstable_dir = self.get_cassandra_sstable_dir(self.version, migrated_files_dir)
        logger.info(f"cassandra sstable dir is {cassandra_sstable_dir}")

        cf_dir = get_node_cf_dir(node, ks, cf)
        logger.info(f"Column family directory is {cf_dir}")

        upload_dir = os.path.join(cf_dir, "upload")
        logger.info(f"Column family upload directory is {upload_dir}")

        logger.info("Copying sstables created by Cassandra...")
        self.copy_files_to(cassandra_sstable_dir, upload_dir)
        logger.info(f"Running 'nodetool refresh -- {ks} {cf}' to load migrated sstables")
        node.nodetool(f"refresh -- {ks} {cf}")

    def _run_basic_migration_test(  # noqa: PLR0913
        self,
        migrated_files_dir,
        row_content,
        compression=None,
        compact_storage=False,
        sleep=0,
        query=None,
    ):
        configuration_options = {"enable_create_table_with_compact_storage": True} if compact_storage else None
        node1 = self.start_cluster_and_get_node1(configuration_options=configuration_options)

        self.create_ks_and_cf(node1, columns={"c1": "text", "c2": "text"}, compression=compression, compact_storage=compact_storage, query=query)
        self.load_migrated_tables(node1, migrated_files_dir)

        time.sleep(sleep)

        expected_keys = 1
        if row_content is None:
            expected_keys = 0
        self.check_number_of_rows(node1, expected_keys)

        if row_content is not None:
            result = self.get_all_rows_for_check(node1)
            for k, error_string in [("key", "check partition key"), ("c1", "check column c1"), ("c2", "check column c2"), ("pk", "check partition key"), ("ck", "check clustering key"), ("v1", "check column v1")]:
                if k in row_content:
                    assert getattr(result[0], k) == row_content[k], error_string


@pytest.mark.dtest_full
@pytest.mark.single_node
class MigrationTestBase(BaseHelpers):
    __test__ = False

    @pytest.mark.dtest_debug
    def test_migrate_sstable_without_compression(self):
        # Content generated with:
        # INSERT INTO ks.cf (key, c2) VALUES ('abc', 'cde');
        self._run_basic_migration_test("without_compression", {"key": "abc", "c1": None, "c2": "cde"})

    def test_migrate_sstable_with_lz4_compression(self):
        # Content generated with:
        # INSERT INTO ks.cf (key, c1, c2) VALUES ('a', 'abc', 'cde');
        self._run_basic_migration_test("with_lz4_compression", {"key": "a", "c1": "abc", "c2": "cde"}, compression="LZ4")

    def test_migrate_sstable_with_compact_storage(self):
        # Content generated with:
        # INSERT INTO ks.cf (key, c1, c2) VALUES ('a', 'abc', 'cde');
        self._run_basic_migration_test("with_compact_storage", {"key": "a", "c1": "abc", "c2": "cde"}, compact_storage=True)

    def test_migrate_sstable_with_compact_storage_and_composite_key(self):
        """
        Test that we can migrate a cassandra sstable with compact storage and clustering key
        """
        query = "CREATE COLUMNFAMILY  ks.cf (pk varchar, ck1 text, v1 text, PRIMARY KEY (pk, ck1)) WITH COMPACT STORAGE"
        self._run_basic_migration_test("with_compact_storage_and_composite_key", {"pk": "a", "ck1": "b", "v1": "abc"}, compact_storage=True, query=query)

    def test_migrate_sstable_with_expired_ttl(self):
        # Data inserted in c* with the following query: INSERT INTO ks.cf (key, c1, c2) VALUES ('a', 'abc', 'cde') USING TTL 1;
        # Expect no keys because the only one inserted is expired.
        self._run_basic_migration_test("with_expired_ttl", None, sleep=10)

    def test_migrate_sstable_with_cell_tombstone(self):
        # Content generated with:
        # INSERT INTO ks.cf (key, c1, c2) VALUES ('a', 'abc', 'cde');
        # nodetool flush
        # DELETE c2 FROM ks.cf where key = 'a';
        self._run_basic_migration_test("with_cell_tombstone", {"key": "a", "c1": "abc", "c2": None})

    def test_migrate_sstable_with_row_tombstone(self):
        # Content generated with:
        # INSERT INTO ks.cf (key, c1, c2) VALUES ('a', 'abc', 'cde');
        # nodetool flush
        # DELETE FROM ks.cf where key = 'a';
        self._run_basic_migration_test("with_row_tombstone", None)

    def test_migrate_sstable_with_range_boundary_tombstone(self):
        if self.version in ("2_1_x", "2_2_x"):
            pytest.skip("Test not supported in version 2.1.x or 2.2.x")

        node1 = self.start_cluster_and_get_node1()

        query = "CREATE COLUMNFAMILY ks.cf (pk int, ck int, PRIMARY KEY (pk, ck))"
        self.create_ks_and_cf(node1, None, None, False, query=query)

        self.load_migrated_tables(node1, "with_range_boundary_tombstone")

        self.check_number_of_rows(node1, 1)

        result = self.get_all_rows_for_check(node1)
        # https://github.com/scylladb/scylla-tools-java/issues/205
        # Content generated with:
        # CREATE COLUMNFAMILY ks.cf (pk int, ck int, PRIMARY KEY (pk, ck));
        # INSERT INTO ks.cf (pk, ck) VALUES (1, 1);
        # INSERT INTO ks.cf (pk, ck) VALUES (1, 2);
        # INSERT INTO ks.cf (pk, ck) VALUES (1, 3);
        # INSERT INTO ks.cf (pk, ck) VALUES (1, 4);
        # INSERT INTO ks.cf (pk, ck) VALUES (1, 5);
        # nodetool flush
        # DELETE FROM ks.cf WHERE pk = 1 AND ck >= 2 AND ck < 3;
        # DELETE FROM ks.cf WHERE pk = 1 AND ck >= 3;

        assert result[0].pk == 1, "check partition key"
        assert result[0].ck == 1, "check clustering key"

    def test_migrate_sstable_with_range_tombstone(self):
        # Content generated with:
        # INSERT INTO ks.cf (key, c1, c2) VALUES ('a', 'abc', 'cde');
        # INSERT INTO ks.cf (key, c1, c2) VALUES ('b', 'abc', 'cde');
        # INSERT INTO ks.cf (key, c1, c2) VALUES ('c', 'abc', 'cde');
        # nodetool flush
        # DELETE FROM ks.cf WHERE key IN ('a', 'b');
        self._run_basic_migration_test("with_range_tombstone", {"key": "c", "c1": "abc", "c2": "cde"})

    def test_migrate_sstable_with_clustering_key_range_tombstone(self):
        if self.version in ("2_1_x", "2_2_x"):
            pytest.skip("Test not supported in version 2.1.x or 2.2.x")

        node1 = self.start_cluster_and_get_node1()

        query = "CREATE COLUMNFAMILY ks.cf (pk int, ck int, v int, PRIMARY KEY (pk, ck))"
        self.create_ks_and_cf(node1, None, None, False, query=query)

        self.load_migrated_tables(node1, "with_clustering_key_range_tombstone")

        self.check_number_of_rows(node1, 2)

        result = self.get_all_rows_for_check(node1)
        # https://github.com/scylladb/scylla-tools-java/issues/204
        # Content generated with:
        # CREATE COLUMNFAMILY ks.cf (pk int, ck int, v int, PRIMARY KEY (pk, ck));
        # INSERT INTO ks.cf (pk, ck, v) VALUES (1, 1, 1);
        # INSERT INTO ks.cf (pk, ck, v) VALUES (1, 2, 1);
        # INSERT INTO ks.cf (pk, ck, v) VALUES (1, 3, 1);
        # INSERT INTO ks.cf (pk, ck, v) VALUES (1, 4, 1);
        # INSERT INTO ks.cf (pk, ck, v) VALUES (1, 5, 1);
        # nodetool flush
        # DELETE FROM ks.cf WHERE pk = 1 AND ck >= 2 AND ck <= 4;

        assert result[0].pk == 1, "check partition key of row 1"
        assert result[0].ck == 1, "check clustering key of row 1"
        assert result[0].v == 1, "check data of row 1"

        assert result[1].pk == 1, "check partition key of row 2"
        assert result[1].ck == 5, "check clustering key of row 2"
        assert result[1].v == 1, "check data of row 2"

    def test_migrate_sstable_with_wide_row(self):
        node1 = self.start_cluster_and_get_node1()

        # CREATE COLUMNFAMILY ks.cf (key varchar, c varchar, v varchar, PRIMARY KEY(key, c))
        self.create_ks_and_cf(node1, None, None, False)
        self.load_migrated_tables(node1, "with_wide_rows")

        self.check_number_of_rows(node1, 3)

        result = self.get_all_rows_for_check(node1)
        # INSERT INTO ks.cf (key, c, v) VALUES ('a', 'a', 'b');
        assert result[0].key == "a", "check partition key"
        assert result[0].c == "a", "check column c1"
        assert result[0].v == "b", "check column c1"
        # INSERT INTO ks.cf (key, c, v) VALUES ('b', 'a', 'a');
        assert result[1].key == "b", "check partition key"
        assert result[1].c == "a", "check column c1"
        assert result[1].v == "a", "check column c1"
        # INSERT INTO ks.cf (key, c, v) VALUES ('b', 'b', 'b');
        assert result[2].key == "b", "check partition key"
        assert result[2].c == "b", "check column c1"
        assert result[2].v == "b", "check column c1"

    def test_migrate_sstable_with_collection_set(self):
        # CREATE COLUMNFAMILY ks.cf (key varchar PRIMARY KEY, messages set<text>);
        # INSERT INTO ks.cf (key, messages) VALUES ( 'a', {'hello world', 'scylla', 'scylladb', 'test'});
        self._run_migration_test_for_collection("with_collection_set", "set<text>", {"a": {"hello world", "scylla", "scylladb", "test"}})

    def test_migrate_sstable_with_collection_list(self):
        # CREATE COLUMNFAMILY ks.cf (key varchar PRIMARY KEY, messages list<text>);
        # INSERT INTO ks.cf (key, messages) VALUES ( 'a', ['scylladb', 'scylla', 'hello world', 'test']);
        self._run_migration_test_for_collection("with_collection_list", "list<text>", {"a": ["scylladb", "scylla", "hello world", "test"]})

    def test_migrate_sstable_with_collection_map(self):
        # CREATE COLUMNFAMILY ks.cf (key varchar PRIMARY KEY, messages map<varchar, text>)
        # INSERT INTO ks.cf (key, messages) VALUES ( 'a', { 'a':'value1', 'b':'value2' });
        self._run_migration_test_for_collection("with_collection_map", "map<varchar, text>", {"a": {"a": "value1", "b": "value2"}})

    def test_migrate_sstable_with_frozen_collection_map(self):
        # CREATE COLUMNFAMILY ks.cf (key varchar PRIMARY KEY, messages frozen<map<varchar, text>>) ...
        # C* returns [Row(key=u'a', messages=OrderedMapSerializedKey([(u'a', u'value1'), (u'b', u'value2')])),
        # Row(key=u'b', messages=OrderedMapSerializedKey([(u'a', u'value1'), (u'b', u'value2')]))] when
        # querying the whole content of sstable with frozen collection map
        self._run_migration_test_for_collection("with_frozen_collection_map", "frozen<map<varchar, text>>", {"a": {"a": "value1", "b": "value2"}, "b": {"a": "value1", "b": "value2"}})

    def test_migrate_sstable_with_static_cell(self):
        node1 = self.start_cluster_and_get_node1()

        query = "CREATE COLUMNFAMILY ks.cf (key varchar, s text STATIC, i int, PRIMARY KEY (key, i)) WITH comment='test cf' AND read_repair_chance=0.000000"
        self.create_ks_and_cf(node1, None, None, False, query=query)

        self.load_migrated_tables(node1, "with_static_cell")

        self.check_number_of_rows(node1, 2)

        result = self.get_all_rows_for_check(node1)
        # contents generated with:
        # INSERT INTO ks.cf (key, s, i) VALUES ('k', 'old', 0);
        # INSERT INTO ks.cf (key, s, i) VALUES ('k', 'new', 1);
        assert result[0].key == "k", "check partition key"
        assert result[0].i == 0, "check clustering key"
        assert result[0].s == "new", "check static cell"
        assert result[1].key == "k", "check partition key"
        assert result[1].i == 1, "check clustering key"
        assert result[1].s == "new", "check static cell"

    def test_migrate_sstable_with_overlapping_tombstones(self):
        node1 = self.start_cluster_and_get_node1()

        query = "create COLUMNFAMILY  ks.cf (pk text, ck1 text, ck2 text, data text, primary key(pk, ck1, ck2))"
        self.create_ks_and_cf(node1, None, None, False, query=query)

        self.load_migrated_tables(node1, "with_overlapping_tombstones")

        self.check_number_of_rows(node1, 1)

        result = self.get_all_rows_for_check(node1)
        # contents generated with:
        # insert into ks.cf (pk, ck1, ck2, data) values('pk', 'aaa', 'bbb', 'ccc');
        # insert into ks.cf (pk, ck1, ck2, data) values('pk', 'aaa', 'ccc', 'ddd');
        # insert into ks.cf (pk, ck1, ck2, data) values('pk', 'aaa', 'ddd', 'eee');
        # insert into ks.cf (pk, ck1, ck2, data) values('pk', 'bbb', 'aaa', 'fff');
        # delete from ks.cf where pk='pk' and ck1='aaa';
        # delete from ks.cf where pk='pk' and ck1='aaa' and ck2='bbb';
        #
        # ->
        # [
        #     {"key": "pk",
        #      "cells": [["aaa:_","aaa:bbb:_",1459842756489757,"t",1459842756],
        #                ["aaa:bbb:_","aaa:bbb:!",1459842776570351,"t",1459842776],
        #                ["aaa:bbb:!","aaa:!",1459842756489757,"t",1459842756],
        #                ["bbb:aaa:","",1459842718297591],
        #                ["bbb:aaa:data","fff",1459842718297591]]}
        # ]

        assert result[0].pk == "pk", "check partition key"
        assert result[0].ck1 == "bbb", "check clustering key"
        assert result[0].ck2 == "aaa", "check partition key"
        assert result[0].data == "fff", "check data"

    def test_migrate_sstable_with_user_defined_types(self):
        node1 = self.start_cluster_and_get_node1()

        query = ["create type ks.ut1 (f1 text, f2 text)", "create type ks.ut2 (f1 text, f2 frozen<ut1>)", "create table ks.cf (id uuid primary key, c frozen<ut2>)"]
        self.create_ks_and_cf(node1, None, None, False, query=query)

        self.load_migrated_tables(node1, "with_user_types")

        self.check_number_of_rows(node1, 2)

        result = self.get_all_rows_for_check(node1)

        # Content created by:
        # INSERT INTO ks.cf (id, c) VALUES (62c36092-82a1-3a00-93d1-46196ee77202, { b1: 'a', b2: { a1: 'b', a2: 'c' } });
        # INSERT INTO ks.cf (id, c) VALUES (62c36092-82a1-3a00-93d1-46196ee77242, { b1: 'x', b2: { a1: 'y', a2: 'z' } });
        assert result[0].id == uuid.UUID("62c36092-82a1-3a00-93d1-46196ee77202"), "check id row 0"
        assert result[0].c == ("a", ("b", "c")), "check c row 0"
        assert result[1].id == uuid.UUID("62c36092-82a1-3a00-93d1-46196ee77242"), "check id row 1"
        assert result[1].c == ("x", ("y", "z")), "check c row 1"

    # Test that scylla's issue 1212 is fixed, look: https://github.com/scylladb/scylla/issues/1212
    # Refresh procedure should ask row cache to evict some rows covered by new sstables.
    def test_migrate_sstable_to_check_consistency(self):
        node1 = self.start_cluster_and_get_node1()

        query = "CREATE COLUMNFAMILY ks.cf (p1 text, r1 int, PRIMARY KEY (p1)) WITH read_repair_chance=0.000000"
        self.create_ks_and_cf(node1, None, None, False, query=query)

        # load row key1 with value 1
        # Content created with:
        # INSERT INTO ks.cf (p1, r1) VALUES ('key1', 1);
        self.load_migrated_tables(node1, "to_check_consistency/1")
        self.check_number_of_rows(node1, 1)

        # read key1 content for it to be cached
        result = self.get_all_rows_for_check(node1)
        assert result[0].p1 == "key1", "check partition key"
        assert result[0].r1 == 1, "check value"

        # load row key1 with value 2
        # Content created with:
        # INSERT INTO ks.cf (p1, r1) VALUES ('key1', 1);
        # nodetool flush
        # UPDATE SET ks.cf r1 = 2 WHERE p1 = 'key1';
        self.load_migrated_tables(node1, "to_check_consistency/2")
        self.check_number_of_rows(node1, 1)

        # read key1 content and expect that it's correct because refresh invalidated cache.
        result = self.get_all_rows_for_check(node1)
        assert result[0].p1 == "key1", "check partition key"
        assert result[0].r1 == 2, "check value"

    def test_migrate_sstable_with_variant_data_types(self):
        node1 = self.start_cluster_and_get_node1()
        query = (
            "CREATE COLUMNFAMILY ks.cf (aascii ascii,"
            "abigint bigint,"
            "ablob blob,"
            "aboolean boolean,"
            "adouble double,"
            "adecimal decimal,"
            "afloat float,"
            "ainet inet,"
            "aint int,"
            "atext text,"
            "atimestamp timestamp,"
            "atimeuuid timeuuid,"
            "auuid uuid,"
            "avarchar varchar,"
            "avarint varint,"
            "alist list<int>,"
            "amap map<int,int>,"
            "aset set<int>,"
            "PRIMARY KEY (aascii, abigint)) "
            "WITH comment='test cf' AND read_repair_chance=0.000000"
        )
        self.create_ks_and_cf(node1, None, None, False, query=query)
        node1.flush()
        self.load_migrated_tables(node1, "with_variant_data_types")
        self.check_number_of_rows(node1, 3)
        # Content created using:
        # INSERT INTO ks.cf (aascii, abigint, ablob, aboolean, adouble, adecimal, afloat, ainet, aint, atext, atimestamp, atimeuuid, auuid, avarchar, avarint, alist, amap, aset) VALUES ('tzach', 1999, 0x0000000000000003, true, 10.10, 10, 11.11, '204.202.130.223', 17, 'text', '2016-08-30 07:01:00.000Z', e23f450f-53a6-11e2-7f7f-7f7f7f7f7f7f, 123e4567-e89b-12d3-a456-426655440000, 'tzachvarchar', 17, [1, 2, 3], {1: 2}, {1, 2, 3, 4});
        # INSERT INTO ks.cf (aascii, abigint, ablob, aboolean, adouble, adecimal, afloat, ainet, aint, atext, atimestamp, atimeuuid, auuid, avarchar, avarint, alist, amap, aset) VALUES ('tzach', 2000, 0x0000000000000003, true, 10.10, 10, 11.11, '204.202.130.223', 17, 'text', '2016-08-30 07:01:00.000Z', e23f450f-53a6-11e2-7f7f-7f7f7f7f7f7f, 123e4567-e89b-12d3-a456-426655440000, 'tzachvarchar', 17, [1, 2, 3], {1: 2}, {1, 2, 3, 4});
        # INSERT INTO ks.cf (aascii, abigint, ablob, aboolean, adouble, adecimal, afloat, ainet, aint, atext, atimestamp, atimeuuid, auuid, avarchar, avarint, alist, amap, aset) VALUES ('livyatan', 2001, 0x0000000000000003, true, 10.10, 10, 11.11, '204.202.130.223', 17, 'text', '2016-08-30 07:01:00.000Z', e23f450f-53a6-11e2-7f7f-7f7f7f7f7f7f, 123e4567-e89b-12d3-a456-426655440000, 'tzachvarchar', 17, [1, 2, 3], {1: 2}, {1, 2, 3, 4});
        result = self.get_all_rows_for_check(node1)
        for i in range(3):
            if i in (0, 1):
                assert result[i].aascii == "tzach", "check ascii column"
            if i == 2:
                assert result[i].aascii == "livyatan", "check ascii column"
            assert result[i].abigint == 1999 + i, "check bigint column"
            assert result[i].ablob.hex() == "0000000000000003", "check blob column"
            assert result[i].aboolean == True, "check boolean column"
            assert result[i].adecimal == 10, "check decimal column"
            assert result[i].adouble == 10.10, "check double column"
            assert round(result[i].afloat, 2) == 11.11, "check afloat column"
            assert result[i].ainet == "204.202.130.223", "check ainet column"
            assert result[i].aint == 17, "check inet column"
            assert result[i].atext == "text", "check text column"
            assert result[i].atimestamp == datetime.datetime(2016, 8, 30, 7, 1), "check timestamp column"
            assert result[i].atimeuuid == uuid.UUID("e23f450f-53a6-11e2-7f7f-7f7f7f7f7f7f"), "check timeuuid column"
            assert result[i].auuid == uuid.UUID("123e4567-e89b-12d3-a456-426655440000"), "check uuid column"
            assert result[i].avarchar == "tzachvarchar", "check varchar column"
            assert result[i].avarint == 17, "check varint column"
            assert result[i].alist == [1, 2, 3], "check list column"
            assert result[i].amap == {1: 2}, "check map column"
            assert result[i].aset == {1, 2, 3, 4}, "check set column"

    def migrate_sstable_with_old_format_counter_test_expect_fail(self):
        if self.version != "2_1_x":
            pytest.skip("Test only relevant to old-format counters")

        """
        create cassandra cluster version 2.0.x
        CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':1};
        CREATE TABLE ks.cf (pk int PRIMARY KEY, cnt COUNTER);
        add 10 counters
        create cassandra cluster version 2.1.x
        create ks and cf
        copy sstables from 2.0.x
        start node and run nodetool upgradesstables
        add more 10 counters
        """
        cluster = self.cluster
        self.populate_cluster(cluster)
        node1 = self.cluster.nodelist()[0]
        node1.set_configuration_options()
        self.start_cluster(cluster)

        query = "CREATE TABLE ks.cf (pk int PRIMARY KEY, cnt COUNTER);"
        self.create_ks_and_cf(node1, None, None, False, query=query)
        expected_message = "Direct loading non-Scylla SSTables containing counters is not supported."
        self.load_migrated_tables_expect_fail(node1, "with_old_format_counter", message=expected_message)

    def migrate_sstable_with_counter_test_expect_fail(self):
        """
        https://github.com/scylladb/scylla/issues/2119
        CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':1};
        CREATE TABLE ks.cf (first_name varchar, last_name varchar, cnt counter, PRIMARY KEY(first_name, last_name));
        UPDATE ks.cf SET cnt = cnt + 1 WHERE first_name='albert' AND last_name='einstein';
        UPDATE ks.cf SET cnt = cnt + 2 WHERE first_name='thomas' AND last_name='edison';
        flush
        UPDATE ks.cf SET cnt = cnt + 10 WHERE first_name='albert' AND last_name='einstein';
        UPDATE ks.cf SET cnt = cnt + 3 WHERE first_name='marie' AND last_name='curie';
        UPDATE ks.cf SET cnt = cnt - 5 WHERE first_name='albert' AND last_name='einstein';
        flush
        """
        cluster = self.cluster
        self.populate_cluster(cluster)
        node1 = self.cluster.nodelist()[0]
        node1.set_configuration_options()
        self.start_cluster(cluster)

        query = "CREATE TABLE ks.cf (first_name varchar, last_name varchar, cnt counter, PRIMARY KEY(first_name, last_name));"
        self.create_ks_and_cf(node1, None, None, False, query=query)
        expected_message = "Direct loading non-Scylla SSTables containing counters is not supported."
        self.load_migrated_tables_expect_fail(node1, "with_counter", message=expected_message)

    @pytest.mark.skip_if(with_feature("tablets") & issue_open("#18180"))
    def test_migrate_sstable_with_counter(self):
        """
        https://github.com/scylladb/scylla/issues/2119
        CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':1};
        CREATE TABLE ks.cf (first_name varchar, last_name varchar, cnt counter, PRIMARY KEY(first_name, last_name));
        UPDATE ks.cf SET cnt = cnt + 1 WHERE first_name='albert' AND last_name='einstein';
        UPDATE ks.cf SETcnt = cnt + 2 WHERE first_name='thomas' AND last_name='edison';
        flush
        UPDATE ks.cf SET cnt = cnt + 10 WHERE first_name='albert' AND last_name='einstein';
        UPDATE ks.cf SET cnt = cnt + 3 WHERE first_name='marie' AND last_name='curie';
        UPDATE ks.cf SET cnt = cnt - 5 WHERE first_name='albert' AND last_name='einstein';
        flush
        """
        cluster = self.cluster
        self.populate_cluster(cluster, extra_values={"enable_dangerous_direct_import_of_cassandra_counters": True})
        node1 = self.cluster.nodelist()[0]
        node1.set_configuration_options()
        self.start_cluster(cluster)

        query = "CREATE TABLE ks.cf (first_name varchar, last_name varchar, cnt counter, PRIMARY KEY(first_name, last_name));"
        self.create_ks_and_cf(node1, None, None, False, query=query)
        self.load_migrated_tables(node1, "with_counter")

    def migrate_sstable_with_wrong_partitioner_test_expect_fail(self):
        """
        https://github.com/scylladb/scylla/issues/4331
        Partitioner: org.apache.cassandra.dht.RandomPartitioner
        initial_token: 1
        CREATE KEYSPACE ks
            WITH replication={
                'class':'NetworkTopologyStrategy', 'replication_factor':1
            };
        CREATE TABLE ks.cf ( pk INT, ck INT, v INT, PRIMARY KEY(pk, ck))
            WITH compression = { 'sstable_compression' : '' };
        INSERT INTO ks.cf (pk, ck, v) VALUES (1, 10, 100);
        INSERT INTO ks.cf (pk, ck, v) VALUES (2, 20, 200);
        INSERT INTO ks.cf (pk, ck, v) VALUES (3, 30, 300);
        flush
        """
        cluster = self.cluster
        self.populate_cluster(cluster)
        node1 = self.cluster.nodelist()[0]
        node1.set_configuration_options()
        self.start_cluster(cluster)

        query = "CREATE TABLE ks.cf (pk INT, ck INT, v INT, PRIMARY KEY(pk, ck))" + " WITH compression = { 'sstable_compression' : '' }"
        self.create_ks_and_cf(node1, None, None, False, query=query)
        self.load_migrated_tables_expect_fail(node1, "with_wrong_partitioner", message=self.get_wrong_partitioner_error_message())

    # ######################## Helper functions ####################################

    def check_number_of_rows(self, node, expected_number_of_rows):
        logger.info("Checking rows on node1...")
        query = "SELECT COUNT(*) FROM cf"
        statement = SimpleStatement(query)
        s = self.patient_cql_connection(node, "ks")
        result = list(s.execute(statement))
        assert result[0].count == expected_number_of_rows, "Expected {} rows. Got {}".format(expected_number_of_rows, list(s.execute("SELECT * FROM ks.cf")))

    def _run_migration_test_for_collection(self, migration_dir_name, collection_type, collection_content):
        node1 = self.start_cluster_and_get_node1()

        # CREATE COLUMNFAMILY ks.cf (key varchar PRIMARY KEY, messages collection_type<text>)
        self.create_ks_and_cf(node1, {"messages": f"{collection_type}"}, None, False)
        self.load_migrated_tables(node1, migration_dir_name)

        self.check_number_of_rows(node1, len(collection_content))

        result = self.get_all_rows_for_check(node1)
        idx = 0
        for key, value in collection_content.items():
            assert result[idx].key == key, "check partition key"
            # INSERT INTO ks.cf (key, messages) VALUES('a', {'scylladb', 'scylla', 'hello world', 'test'});
            assert result[idx].messages == value, "check messages"
            idx += 1

    def recursive_copy_to(self, from_dir, to_dir):
        shutil.copytree(from_dir, to_dir, dirs_exist_ok=True)

    def get_sstable_version(self, cf_dir, assert_only_one_version=True):
        file_list = os.listdir(cf_dir)
        logger.info(f"{file_list}")
        sstable_version_regex = re.compile(r"(\w+)-[^-]+-(.+)\.(db|txt|sha1|crc32)")

        sstable_versions = list(set([sstable_version_regex.search(f).group(1) for f in file_list if sstable_version_regex.search(f)]))

        if assert_only_one_version:
            if len(sstable_versions) != 1:
                print(f"Expected only one version, got {sstable_versions}. File list: {file_list}")
            assert len(sstable_versions) == 1, sstable_versions
        if sstable_versions:
            return sstable_versions[0]
            #       else:
            return None


# Dtest created to test migration of data from C* to Scylla
#


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestMigration(MigrationTestBase):
    __test__ = True

    @pytest.fixture(params=["2_1_x", "2_2_x", "3_0_mc", "3_0_md"], autouse=True)
    def select_version(self, request):
        self.version = request.param

    @pytest.fixture(scope="function", autouse=True)
    def fixture_dtest_setup_overrides(self, dtest_config):
        dtest_setup_overrides = DTestSetupOverrides()
        dtest_setup_overrides.cluster_options = ImmutableMapping({"start_rpc": "true"})
        return dtest_setup_overrides

    def load_migrated_tables_expect_fail(self, node, migrated_files_dir, message=None, ks="ks", cf="cf"):
        if message:
            self.ignore_log_patterns += [message]
        cassandra_sstable_dir = self.get_cassandra_sstable_dir(self.version, migrated_files_dir)
        logger.info(f"cassandra sstable dir is {cassandra_sstable_dir}")

        cf_dir = get_node_cf_dir(node, ks, cf)
        logger.info(f"Column family directory is {cf_dir}")

        logger.info("Copying sstables created by Cassandra...")
        self.copy_files_to(cassandra_sstable_dir, cf_dir + "/upload")

        logger.info(f"Running 'nodetool refresh -- {ks} {cf}' to load migrated sstables")
        try:
            node.nodetool(f"refresh -- {ks} {cf}")
            assert False
        except NodetoolError as error:
            if message:
                assert message in str(error), error

    @pytest.mark.dtest_debug
    @pytest.mark.skip_if(with_feature("tablets") & issue_open("#18180"))
    def test_migrate_sstable_with_counter(self):
        super().test_migrate_sstable_with_counter()

    def test_migrate_sstable_with_variant_data_types(self):
        super().test_migrate_sstable_with_variant_data_types()

    def get_wrong_partitioner_error_message(self):
        return "uses org.apache.cassandra.dht.RandomPartitioner" + " partitioner which is different than" + " org.apache.cassandra.dht.Murmur3Partitioner" + " partitioner used by the database"


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestMigrationUpgradeSSTables(TestMigration):
    __test__ = True

    @pytest.fixture(params=["2_1_x", "2_2_x", "3_0_mc", "3_0_md"], autouse=True)
    def select_version(self, request):
        self.version = request.param

    @pytest.mark.skip("test isn't relevant when using nodetool upgradesstables")
    def test_migrate_sstable_with_row_tombstone(self):
        # since the row tombstone data doesn't create files on disk
        pass

    @pytest.mark.skip("test isn't relevant when using nodetool upgradesstables")
    def test_migrate_sstable_to_check_consistency(self):
        # since this test load multiple versions, that conflicts with version created upgradesstables
        pass

    @pytest.mark.skip("test isn't relevant when using nodetool upgradesstables")
    def test_migrate_sstable_with_expired_ttl(self):
        # since expired ttl data doens't create files on disk
        pass

    def load_migrated_tables(self, node, migrated_files_dir, ks="ks", cf="cf"):
        super().load_migrated_tables(node, migrated_files_dir, ks="ks", cf="cf")

        cf_dir = get_node_cf_dir(node, ks, cf)
        logger.info(f"Column family directory is {cf_dir}")

        source_dir = self.get_cassandra_sstable_dir(self.version, migrated_files_dir)
        before_sstable_version = self.get_sstable_version(source_dir, assert_only_one_version=False)

        logger.info(f"Running 'nodetool upgradesstables {ks} {cf}'")
        node.nodetool(f"upgradesstables {ks} {cf}")
        node.flush()

        after_sstable_version = self.get_sstable_version(cf_dir)

        # check that sstable version was upgraded, or if that version equals latest version `mc`
        assert after_sstable_version > before_sstable_version or (before_sstable_version == after_sstable_version and after_sstable_version in ["mc", "md"]), (
            f"upgradesstable failed to upgrade sstables [before_version={before_sstable_version} after_version={after_sstable_version}]"
        )


# @skip('not run every build')
# @attr('long','compare-cassandra')
@pytest.mark.dtest_full
class TestTTLWithMigrate(Tester):
    """Test Time To Live Feature with Migration"""

    def prepare(  # noqa: PLR0913
        self,
        default_time_to_live=None,
        create_table_statement=None,
        nodes=1,
        rf=1,
        configuration_options=None,
        custom_args=None,
    ):
        if configuration_options:
            logger.info(f"Setting cluster configuration options: {configuration_options}")
            self.cluster.set_configuration_options(values=configuration_options)
        self.cluster.populate(nodes).start(jvm_args=custom_args)
        node1 = self.cluster.nodelist()[0]
        self.session1 = self.patient_cql_connection(node1)
        create_ks(self.session1, "ks", rf=rf)

        drop_table(session=self.session1, table_name="ttl_table", if_exists=True)

        if create_table_statement is None:
            query = """
                CREATE TABLE ttl_table (
                    key int primary key,
                    col1 int,
                    col2 int,
                    col3 int,
                )
            """
        else:
            query = create_table_statement
        if default_time_to_live:
            query += f" WITH default_time_to_live = {default_time_to_live};"

        self.session1.execute(query)

    # @pytest.mark.next_gating      # Removing from gating for now, till it passes consistently
    # timeuuid based identifier was introduced in Cassandra 4.1. so we cannot test it with
    # Cassandra 3.x. see @jira_ticket CASSANDRA-17048
    @pytest.mark.skipif(condition=not java_version_exist(8), reason="test depends on cassandra 3.x, and needs java 8 to run")
    @pytest.mark.skip_if(with_feature("tablets") & issue_open("jira:DTEST-58"))
    @pytest.mark.cluster_options(uuid_sstable_identifiers_enabled=False)
    def test_big_table_with_ttls(self, request):  # noqa: PLR0915
        """
        Test validates migration from Scylla to Cassandra of large partition table with TTLs.
         - Create the big table with different kind of columns, create 10 partitions with 1000 rows each partition and 1 partition with 100000 rows.
         - Run updates/removes on all columns
         - Take dump
         - Migrate data to Cassandra
         - Take dump
         - Compare dumps
        """
        timeout = self.cql_timeout(300)
        self.prepare(
            nodes=4,
            rf=3,
            custom_args=["--smp", "1", "--memory", "512M"],
            configuration_options={
                "range_request_timeout_in_ms": timeout * 1000,
            },
        )
        keyspace_name = "ks"
        table_name = "cf"
        int_columns = 99
        stmt = "create table {} (pk int, ck int, {}, clist list<int>, cset set<text>, cmap map<int, text>, PRIMARY KEY(pk, ck)) WITH compaction = {{'class': 'SizeTieredCompactionStrategy'}}".format(
            table_name, ", ".join("c%d int" % i for i in range(1, int_columns))
        )
        self.session1.execute(stmt)

        min_ttl = 120

        def create_update_command(ttl, column_expr, pk, ck, table_name=table_name):
            assert ttl > min_ttl, f"TTL {ttl} must be greater than {min_ttl}"
            return f"update {table_name} USING TTL {ttl} set {column_expr} where pk={pk} and ck={ck}"

        # Prefill
        partitions = 10
        rows_in_partition = 1000
        logger.debug(f"Create {partitions} partitions with {rows_in_partition} rows")
        for i in range(1, partitions + 1):
            for k in range(1, rows_in_partition + 1):
                s = "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
                stmt = "insert into {table_name} (pk, ck, {columns}, clist, cset, cmap) values ({ilist}, {klist}, {int_values}, [{ilist}, {klist}], {open}{set_value}{close}, {map_value})".format(
                    table_name=table_name,
                    columns=", ".join("c%d" % l for l in range(1, int_columns)),
                    int_values=", ".join("%d" % l for l in range(1, int_columns)),
                    ilist=i,
                    klist=k,
                    open="{'",
                    set_value=s,
                    close="'}",
                    map_value="{%d: '%s'}" % (k, s),
                )
                self.session1.execute(stmt)

        big_partition = partitions + 1
        big_partition_rows = 100000
        if hasattr(self.cluster, "scylla_mode") and self.cluster.scylla_mode == "debug":
            big_partition_rows //= 10
        logger.debug(f"Create partition where pk = {big_partition} with {big_partition_rows} rows")
        for k in range(1, big_partition_rows + 1):
            s = "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
            stmt = "insert into {table_name} (pk, ck, {columns}, clist, cset, cmap) values ({ilist}, {klist}, {int_values}, [{ilist}, {klist}], {open}{set_value}{close}, {map_value})".format(
                table_name=table_name,
                columns=", ".join("c%d" % l for l in range(1, int_columns)),
                int_values=", ".join("%d" % l for l in range(1, int_columns)),
                ilist=big_partition,
                klist=k,
                open="{'",
                set_value=s,
                close="'}",
                map_value="{%d: '%s'}" % (k, s),
            )
            self.session1.execute(stmt)

        logger.info(f"Verifying that big_partition where pk = {big_partition} has {big_partition_rows} rows")
        count_query = f"select count(*) from {keyspace_name}.{table_name} where pk = {big_partition}"
        scylla_big_partition_count = next(iter(self.session1.execute(count_query, timeout=timeout)))[0]
        assert scylla_big_partition_count == big_partition_rows, f"Expected {big_partition_rows} rows in the big partition before update, but received {scylla_big_partition_count}"

        node1 = self.cluster.nodelist()[0]
        self.cluster.flush()

        ttl_boundaries = [1800, 3600]
        logger.info(f"Run updates using TTLs in the {ttl_boundaries} range")

        for _ in range(1, big_partition + 1):
            # Update int columns
            stmts = [
                create_update_command(
                    ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]),
                    column_expr="c%d = %d" % (random.randint(1, int_columns - 1), random.randint(0, 500000)),
                    pk=random.randint(1, partitions),
                    ck=random.randint(1, rows_in_partition),
                )
            ]
            # Update big partition
            stmts.append(
                create_update_command(
                    ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]), column_expr="c%d = %d" % (random.randint(1, int_columns - 1), random.randint(0, 500000)), pk=big_partition, ck=random.randint(1, big_partition_rows)
                )
            )

            # Delete int value
            stmts.append(
                create_update_command(ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]), column_expr="c%d = NULL" % (random.randint(1, int_columns - 1)), pk=random.randint(1, partitions), ck=random.randint(1, rows_in_partition))
            )
            # Delete int value in big partition
            stmts.append(create_update_command(ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]), column_expr="c%d = NULL" % (random.randint(1, int_columns - 1)), pk=big_partition, ck=random.randint(1, big_partition_rows)))
            # Update collection columns
            s = "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
            # APPEND to set column - small partitions
            stmts.append(create_update_command(ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]), column_expr="cset = cset+{'%s'}" % (s), pk=random.randint(1, partitions), ck=random.randint(1, rows_in_partition)))
            # APPEND to set column - Big partition
            stmts.append(create_update_command(ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]), column_expr="cset = cset+{'%s'}" % (s), pk=big_partition, ck=random.randint(1, big_partition_rows)))
            # APPEND to list column - small partitions
            stmts.append(
                create_update_command(ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]), column_expr="clist = clist+[%d]" % (random.randint(0, 500000)), pk=random.randint(1, partitions), ck=random.randint(1, rows_in_partition))
            )
            # APPEND to list column - Big partition
            stmts.append(create_update_command(ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]), column_expr="clist = clist+[%d]" % (random.randint(0, 500000)), pk=big_partition, ck=random.randint(1, big_partition_rows)))
            # APPEND to map column - small partitions
            stmts.append(
                create_update_command(
                    ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]), column_expr="cmap = cmap+{%d: '%s'}" % (random.randint(0, 500000), s), pk=random.randint(1, partitions), ck=random.randint(1, rows_in_partition)
                )
            )
            # APPEND to map column - Big partition
            stmts.append(create_update_command(ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]), column_expr="cmap = cmap+{%d: '%s'}" % (random.randint(0, 500000), s), pk=big_partition, ck=random.randint(1, big_partition_rows)))
            # OVERWRITE set column - small partitions
            stmts.append(create_update_command(ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]), column_expr="cset = {'%s'}" % (s), pk=random.randint(1, partitions), ck=random.randint(1, rows_in_partition)))
            # OVERWRITE set column - Big partition
            stmts.append(create_update_command(ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]), column_expr="cset = {'%s'}" % (s), pk=big_partition, ck=random.randint(1, big_partition_rows)))
            # OVERWRITE list column - small partitions
            stmts.append(create_update_command(ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]), column_expr="clist = [%d]" % (random.randint(0, 500000)), pk=random.randint(1, partitions), ck=random.randint(1, rows_in_partition)))
            # OVERWRITE list column - Big partition
            stmts.append(create_update_command(ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]), column_expr="clist = [%d]" % (random.randint(0, 500000)), pk=big_partition, ck=random.randint(1, big_partition_rows)))
            # OVERWRITE map column - small partitions
            stmts.append(
                create_update_command(ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]), column_expr="cmap = {%d: '%s'}" % (random.randint(0, 500000), s), pk=random.randint(1, partitions), ck=random.randint(1, rows_in_partition))
            )
            # OVERWRITE map column - Big partition
            stmts.append(create_update_command(ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]), column_expr="cmap = {%d: '%s'}" % (random.randint(0, 500000), s), pk=big_partition, ck=random.randint(1, big_partition_rows)))

            for stmt in stmts:
                self.session1.execute(stmt)

        scylla_data_json = self._dump_data(cluster=self.cluster, node=node1, node_owner="Scylla", compaction=True)

        logger.info(f"Verifying that big_partition where pk = {big_partition} has {big_partition_rows} rows")
        count_query = f"select count(*) from {keyspace_name}.{table_name} where pk = {big_partition}"
        scylla_big_partition_count = next(iter(self.session1.execute(count_query, timeout=timeout)))[0]
        assert scylla_big_partition_count == big_partition_rows, f"Expected {big_partition_rows} rows in the big partition, but received {scylla_big_partition_count}"

        # Create Cassandra cluster, migrate the data and take the dump
        cassandra_data_json = self.migrate_to_cassandra(keyspace_name=keyspace_name, table_name=table_name, scylla_node=node1, scylla_big_partition_count=scylla_big_partition_count, count_query=count_query, request=request)

        assert scylla_data_json == cassandra_data_json, "Data dumps is not same in Scylla and Cassandra. "

    def migrate_to_cassandra(self, keyspace_name, table_name, scylla_node, take_dump=True, scylla_big_partition_count=None, count_query="", request=None):  # noqa: PLR0913
        cassandra_data_json = ""
        cc = CassandraCluster(cassandra_version="3.11.16", request=request, test_instance=self)
        cassandra_node1 = cc.run_migration(scylla_cluster=self.cluster, scylla_test_path=self.test_path, keyspace_names_list=[keyspace_name], table_names=[table_name])
        if take_dump:
            cassandra_data_json = self._dump_data(cluster=cc.cluster, node=cassandra_node1, scylla_node=scylla_node, node_owner="Cassandra")

        # We want to validate the rows amount in the large partition.
        # But the count query fails on timeout in Cassandra. Comment meanwhile
        # Error in the log: org.apache.cassandra.service.DigestMismatchException: Mismatch for key DecoratedKey
        # https://stackoverflow.com/questions/39765813/datastax-mismatch-for-key-issue
        # if scylla_big_partition_count is not None:
        #     cassandra_session = self.patient_cql_connection(cassandra_node1, keyspace=keyspace_name)
        #     assert_one(cassandra_session, count_query, [scylla_big_partition_count], cl=ConsistencyLevel.ALL, timeout=300)

        return cassandra_data_json

    def _dump_data(self, cluster, node, node_owner, scylla_node=None, keyspace_name="ks", table_name="cf", compaction=True):  # noqa: PLR0913
        if compaction:
            if node.is_scylla() or node.get_cassandra_version() < "2.2":
                log_file = "system.log"
            else:
                log_file = "debug.log"
        logger.info("Flush data to the disk before dump")
        cluster.flush()
        if compaction:
            mark = node.mark_log(filename=log_file)
            logger.info("Compacting sstables")
            node.nodetool(f"compact {keyspace_name} {table_name}")
            node.watch_log_for("Compacted", from_mark=mark, filename=log_file)
            if node_owner == "Cassandra":
                # Cassandra deletes the input sstable after compaction is over.
                # Based on the logs, there can be as much as 100ms between the
                # two, enough that we attempt to dump the deleted sstable below
                # and loose the race.
                # To prevent the race, wait for the deletion to be done, before
                # proceeding with the dump.
                node.watch_log_for("Deleting", from_mark=mark, filename=log_file)
        logger.info("Run sstabledump")

        if node_owner == "Scylla":
            return node.dump_sstables(keyspace_name, table_name)

        if scylla_node is None or type(scylla_node) is not ScyllaNode:
            raise RuntimeError(f"scylla_node has unexpected type {type(scylla_node)!s}, expected ScyllaNode")

        sstables = node.get_sstablespath(keyspace=keyspace_name, tables=[table_name])
        scylla_path = join_bin(scylla_node.get_path(), BIN_DIR, "scylla")
        args = [scylla_path, "sstable", "dump-data", "--merge", *sstables]
        res = subprocess.run(args, capture_output=True, text=True, check=False, env=scylla_node._get_environ())
        if res.returncode:
            raise ToolError(command=" ".join(args), exit_status=res.returncode, stdout=res.stdout, stderr=res.stderr)
        return json.loads(res.stdout)["sstables"]["anonymous"]


@pytest.mark.dtest_full
class TestLoadAndStream(BaseHelpers):
    KEYSPACE_NAME = "keyspace1"
    TABLE_NAME = "standard1"
    EXPECTED_ROWS_NUMBER = 1000
    __test__ = True

    @pytest.fixture(params=["3_0_md"], autouse=True)
    def select_version(self, request):
        self.version = request.param

    def test_load_and_stream_decrease_cluster(self):
        """
        Test for the feature load_and_stream:
        https://github.com/scylladb/scylla/commit/df3ef800c20c60d7929ffa600649793aa6c73064

        Test data was created on the 4-nodes cluster, RF=3, Scylla was started with SMP 1
        Load and stream sstables from 4-nodes cluster to 2-nodes cluster and validate the data
        """
        node1 = self.start_cluster_and_get_node1(nodes=generate_cluster_topology(rack_num=2))
        session = self.patient_cql_connection(node1)
        create_ks(session=session, name=self.KEYSPACE_NAME, rf=2)
        create_c1c2_table(session, cf=self.TABLE_NAME)

        # Copy sstables from 2 nodes to node1
        for node_load_from in ["node1", "node2"]:
            logger.debug(f"Copy sstables of {node_load_from} to node1")
            self.copy_sstables_to_node(copy_to_node=node1, migrated_files_dir=f"from-cluster-4-nodes/{node_load_from}")
            load_and_stream_result, mark = self.run_load_and_stream(node1)
            assert load_and_stream_result, f"Failed to run load and stream."
            result = self.wait_for_finish_load_and_stream(node=node1, mark=mark)
            assert result, f"Load and stream was not finished"

        # Copy sstables from 2 nodes to node2
        for node_load_from in ["node3", "node4"]:
            logger.debug(f"Copy sstables of {node_load_from} to node2")
            self.copy_sstables_to_node(copy_to_node=self.cluster.nodelist()[1], migrated_files_dir=f"from-cluster-4-nodes/{node_load_from}")
            load_and_stream_result, mark = self.run_load_and_stream(self.cluster.nodelist()[1])
            assert load_and_stream_result, f"Failed to run load and stream."
            result = self.wait_for_finish_load_and_stream(node=self.cluster.nodelist()[1], mark=mark)
            assert result, f"Load and stream was not finished"

        for node in self.cluster.nodelist():
            self.check_number_of_rows(node, self.EXPECTED_ROWS_NUMBER, keyspace=self.KEYSPACE_NAME, table=self.TABLE_NAME)

        for n in range(self.EXPECTED_ROWS_NUMBER):
            query_c1c2(session, key=n, consistency=ConsistencyLevel.QUORUM, c1_value=f"customtext1{n}", c2_value=f"customtext2{n}", ks=self.KEYSPACE_NAME, cf=self.TABLE_NAME)

    def test_load_and_stream_decrease_cluster_with_mv(self):
        """
        Test for the feature load_and_stream:
        https://github.com/scylladb/scylla/commit/df3ef800c20c60d7929ffa600649793aa6c73064

        Test data was created on the 4-nodes cluster, RF=3, Scylla was started with SMP 1
        Base table has secondary index
        Load and stream sstables from 4-nodes cluster to 2-nodes cluster and validate the data
        """
        mv_name = "test_mv"
        node1 = self.start_cluster_and_get_node1(nodes=generate_cluster_topology(rack_num=2))
        session = self.patient_cql_connection(node1)
        create_ks(session=session, name=self.KEYSPACE_NAME, rf=2)
        create_c1c2_table(session, cf=self.TABLE_NAME)

        session.execute(f"CREATE MATERIALIZED VIEW {mv_name} AS SELECT c1 FROM {self.KEYSPACE_NAME}.{self.TABLE_NAME} where c1 IS NOT NULL and key IS NOT NULL PRIMARY KEY (c1, key)")

        # Copy sstables from 2 nodes to node1
        for node_load_from in ["node1", "node2"]:
            logger.debug(f"Copy sstables of {node_load_from} to node1")
            self.copy_sstables_to_node(copy_to_node=node1, migrated_files_dir=f"from-cluster-4-nodes/{node_load_from}")
            load_and_stream_result, mark = self.run_load_and_stream(node1)
            assert load_and_stream_result, f"Failed to run load and stream."
            result = self.wait_for_finish_load_and_stream(node=node1, mark=mark)
            assert result, f"Load and stream was not finished"

        # Copy sstables from 2 nodes to node2
        for node_load_from in ["node3", "node4"]:
            logger.debug(f"Copy sstables of {node_load_from} to node2")
            self.copy_sstables_to_node(copy_to_node=self.cluster.nodelist()[1], migrated_files_dir=f"from-cluster-4-nodes/{node_load_from}")
            load_and_stream_result, mark = self.run_load_and_stream(self.cluster.nodelist()[1])
            assert load_and_stream_result, f"Failed to run load and stream."
            result = self.wait_for_finish_load_and_stream(node=self.cluster.nodelist()[1], mark=mark)
            assert result, f"Load and stream was not finished"

        wait_for_view(cluster=self.cluster, session=session, ks=self.KEYSPACE_NAME, view=mv_name)

        for node in self.cluster.nodelist():
            self.check_number_of_rows(node, self.EXPECTED_ROWS_NUMBER, keyspace=self.KEYSPACE_NAME, table=self.TABLE_NAME)
            self.check_number_of_rows(node, self.EXPECTED_ROWS_NUMBER, keyspace=self.KEYSPACE_NAME, table=mv_name, is_view=True)

        for n in range(self.EXPECTED_ROWS_NUMBER):
            query_c1c2(session, key=n, consistency=ConsistencyLevel.QUORUM, c1_value=f"customtext1{n}", c2_value=f"customtext2{n}", ks=self.KEYSPACE_NAME, cf=self.TABLE_NAME)
            assert_one(session, query=f"select key from {self.KEYSPACE_NAME}.{mv_name} where c1 = 'customtext1{n}' and key='k{n}'", expected=[f"k{n}"], cl=ConsistencyLevel.QUORUM)

    def test_load_and_stream_decrease_cluster_with_index_view(self):
        """
        Test for the feature load_and_stream:
        https://github.com/scylladb/scylla/commit/df3ef800c20c60d7929ffa600649793aa6c73064

        Test data was created on the 4-nodes cluster, RF=3, Scylla was started with SMP 1
        Base table has secondary index
        Load and stream sstables from 4-nodes cluster to 2-nodes cluster and validate the data
        """
        index_name = "c2_ind"
        node1 = self.start_cluster_and_get_node1(nodes=generate_cluster_topology(rack_num=2))
        session = self.patient_cql_connection(node1)
        create_ks(session=session, name=self.KEYSPACE_NAME, rf=2)
        create_c1c2_table(session, cf=self.TABLE_NAME)

        create_index(session, self.TABLE_NAME, "c2", index_name)

        # Copy sstables from 2 nodes to node1
        for node_load_from in ["node1", "node2"]:
            logger.debug(f"Copy sstables of {node_load_from} to node1")
            self.copy_sstables_to_node(copy_to_node=node1, migrated_files_dir=f"from-cluster-4-nodes/{node_load_from}")
            load_and_stream_result, mark = self.run_load_and_stream(node1)
            assert load_and_stream_result, f"Failed to run load and stream."
            result = self.wait_for_finish_load_and_stream(node=node1, mark=mark)
            assert result, f"Load and stream was not finished"

        # Copy sstables from 2 nodes to node2
        for node_load_from in ["node3", "node4"]:
            logger.debug(f"Copy sstables of {node_load_from} to node2")
            self.copy_sstables_to_node(copy_to_node=self.cluster.nodelist()[1], migrated_files_dir=f"from-cluster-4-nodes/{node_load_from}")
            load_and_stream_result, mark = self.run_load_and_stream(self.cluster.nodelist()[1])
            assert load_and_stream_result, f"Failed to run load and stream."
            result = self.wait_for_finish_load_and_stream(node=self.cluster.nodelist()[1], mark=mark)
            assert result, f"Load and stream was not finished"

        wait_for_view(cluster=self.cluster, session=session, ks=self.KEYSPACE_NAME, view=f"{index_name}_index")

        for node in self.cluster.nodelist():
            self.check_number_of_rows(node, self.EXPECTED_ROWS_NUMBER, keyspace=self.KEYSPACE_NAME, table=self.TABLE_NAME)
            self.check_number_of_rows(node, self.EXPECTED_ROWS_NUMBER, keyspace=self.KEYSPACE_NAME, table=f"{index_name}_index", is_view=True)

        for n in range(self.EXPECTED_ROWS_NUMBER):
            query_c1c2(session, key=n, consistency=ConsistencyLevel.QUORUM, c1_value=f"customtext1{n}", c2_value=f"customtext2{n}", ks=self.KEYSPACE_NAME, cf=self.TABLE_NAME)
            assert_one(session, query=f"select key from {self.KEYSPACE_NAME}.{self.TABLE_NAME} where c2 = 'customtext2{n}'", expected=[f"k{n}"], cl=ConsistencyLevel.QUORUM)

    def test_load_and_stream_increase_cluster_with_index(self):
        """
        Test for the feature load_and_stream:
        https://github.com/scylladb/scylla/commit/df3ef800c20c60d7929ffa600649793aa6c73064

        Test data was created on the 2-nodes cluster, RF=2, Scylla was started with SMP 1
        Base table with materialized view
        Load and stream sstables from 2-nodes cluster to 4-nodes cluster and validate the data
        """
        index_name = "c2_ind"
        node1 = self.start_cluster_and_get_node1(nodes=generate_cluster_topology(rack_num=2, nodes_per_rack=2))
        node4 = self.cluster.nodelist()[3]
        session = self.patient_cql_connection(node1)
        create_ks(session=session, name=self.KEYSPACE_NAME, rf=2)
        create_c1c2_table(session, cf=self.TABLE_NAME)
        create_index(session, self.TABLE_NAME, "c2", index_name)

        logger.debug("Copy sstables of node1 to node4")
        self.copy_sstables_to_node(copy_to_node=node4, migrated_files_dir="from-cluster-2-nodes/node1")

        logger.debug("Copy sstables of node2 to node1")
        self.copy_sstables_to_node(copy_to_node=node1, migrated_files_dir="from-cluster-2-nodes/node2")

        for node in [node1, node4]:
            load_and_stream_result, mark = self.run_load_and_stream(node)
            assert load_and_stream_result, f"Failed to run load and stream."
            result = self.wait_for_finish_load_and_stream(node=node, mark=mark)
            assert result, f"Load and stream was not finished"

        wait_for_view(cluster=self.cluster, session=session, ks=self.KEYSPACE_NAME, view=f"{index_name}_index")

        for node in self.cluster.nodelist():
            self.check_number_of_rows(node, self.EXPECTED_ROWS_NUMBER, keyspace=self.KEYSPACE_NAME, table=self.TABLE_NAME)
            self.check_number_of_rows(node, self.EXPECTED_ROWS_NUMBER, keyspace=self.KEYSPACE_NAME, table=f"{index_name}_index", is_view=True)

        for n in range(self.EXPECTED_ROWS_NUMBER):
            query_c1c2(session, key=n, consistency=ConsistencyLevel.QUORUM, c1_value=f"customtext1{n}", c2_value=f"customtext2{n}", ks=self.KEYSPACE_NAME, cf=self.TABLE_NAME)
            assert_one(session, query=f"select key from {self.KEYSPACE_NAME}.{self.TABLE_NAME} where c2 = 'customtext2{n}'", expected=[f"k{n}"], cl=ConsistencyLevel.QUORUM)

    def test_load_and_stream_increase_cluster_test_data_with_smp2(self):
        """
        Test for the feature load_and_stream:
        https://github.com/scylladb/scylla/commit/df3ef800c20c60d7929ffa600649793aa6c73064

        Test data was created on the 2-nodes cluster, RF=2 (SMP=2)
        Load and stream sstables from 2-nodes cluster to 4-nodes cluster and validate the data
        """
        cluster_topology = {"dc1": {"rack1": 2, "rack2": 1, "rack3": 1}}
        node1 = self.start_cluster_and_get_node1(nodes=cluster_topology)
        node3, node4 = self.cluster.nodelist()[2:]
        create_stress_compatible_table(self, node1, rf=3)

        logger.debug("Copy sstables of node1 to node4")
        self.copy_sstables_to_node(copy_to_node=node4, migrated_files_dir="from-cluster-2-nodes-c-s-smp2/node1")

        logger.debug("Copy sstables of node2 to node3")
        self.copy_sstables_to_node(copy_to_node=node3, migrated_files_dir="from-cluster-2-nodes-c-s-smp2/node2")

        for node in [node3, node4]:
            load_and_stream_result, mark = self.run_load_and_stream(node)
            assert load_and_stream_result, f"Failed to run load and stream."
            result = self.wait_for_finish_load_and_stream(node=node, mark=mark)
            assert result, f"Load and stream was not finished"

        for node in self.cluster.nodelist():
            self.check_number_of_rows(node, self.EXPECTED_ROWS_NUMBER, keyspace=self.KEYSPACE_NAME, table=self.TABLE_NAME)

    def test_load_and_stream_increase_cluster(self):
        """
        Test for the feature load_and_stream:
        https://github.com/scylladb/scylla/commit/df3ef800c20c60d7929ffa600649793aa6c73064

        Test data was created on the 2-nodes cluster, RF=2, Scylla was started with SMP 1
        Load and stream sstables from 2-nodes cluster to 4-nodes cluster and validate the data
        """
        node1 = self.start_cluster_and_get_node1(nodes=generate_cluster_topology(rack_num=2, nodes_per_rack=2))
        node4 = self.cluster.nodelist()[3]
        session = self.patient_cql_connection(node1)
        create_ks(session=session, name=self.KEYSPACE_NAME, rf=2)
        create_c1c2_table(session, cf=self.TABLE_NAME)

        logger.debug("Copy sstables of node1 to node4")
        self.copy_sstables_to_node(copy_to_node=node4, migrated_files_dir="from-cluster-2-nodes/node1")

        logger.debug("Copy sstables of node2 to node1")
        self.copy_sstables_to_node(copy_to_node=node1, migrated_files_dir="from-cluster-2-nodes/node2")

        for node in [node1, node4]:
            load_and_stream_result, mark = self.run_load_and_stream(node)
            assert load_and_stream_result, f"Failed to run load and stream."
            result = self.wait_for_finish_load_and_stream(node=node, mark=mark)
            assert result, f"Load and stream was not finished"

        for node in self.cluster.nodelist():
            self.check_number_of_rows(node, self.EXPECTED_ROWS_NUMBER, keyspace=self.KEYSPACE_NAME, table=self.TABLE_NAME)

        for n in range(self.EXPECTED_ROWS_NUMBER):
            query_c1c2(session, key=n, consistency=ConsistencyLevel.QUORUM, c1_value=f"customtext1{n}", c2_value=f"customtext2{n}", ks=self.KEYSPACE_NAME, cf=self.TABLE_NAME)

    def test_load_and_stream_from_one_node_increase_cluster(self):
        """
        Test for the feature load_and_stream:
        https://github.com/scylladb/scylla/commit/df3ef800c20c60d7929ffa600649793aa6c73064

        Test data was created on the 2-nodes cluster, RF=2, Scylla was started with SMP 1
        Load and stream sstables from 2-nodes cluster to 4-nodes cluster and validate the data
        """
        node1 = self.start_cluster_and_get_node1(nodes=generate_cluster_topology(rack_num=2, nodes_per_rack=2))
        node3 = self.cluster.nodelist()[2]
        session = self.patient_cql_connection(node1)
        create_ks(session=session, name=self.KEYSPACE_NAME, rf=2)
        create_c1c2_table(session, cf=self.TABLE_NAME)

        logger.debug("Copy all sstables of node1 to node3")
        for source_files in ["from-cluster-2-nodes/node1", "from-cluster-2-nodes/node2"]:
            self.copy_sstables_to_node(copy_to_node=node3, migrated_files_dir=source_files)
            load_and_stream_result, mark = self.run_load_and_stream(node3)
            assert load_and_stream_result, f"Failed to run load and stream."
            result = self.wait_for_finish_load_and_stream(node=node3, mark=mark)
            assert result, f"Load and stream was not finished"

        for node in self.cluster.nodelist():
            self.check_number_of_rows(node, self.EXPECTED_ROWS_NUMBER, keyspace=self.KEYSPACE_NAME, table=self.TABLE_NAME)

        for n in range(self.EXPECTED_ROWS_NUMBER):
            query_c1c2(session, key=n, consistency=ConsistencyLevel.QUORUM, c1_value=f"customtext1{n}", c2_value=f"customtext2{n}", ks=self.KEYSPACE_NAME, cf=self.TABLE_NAME)

    def test_load_and_stream_increase_cluster_with_mv(self):
        """
        Test for the feature load_and_stream:
        https://github.com/scylladb/scylla/commit/df3ef800c20c60d7929ffa600649793aa6c73064

        Test data was created on the 2-nodes cluster, RF=2, Scylla was started with SMP 1
        Base table with materialized view
        Load and stream sstables from 2-nodes cluster to 4-nodes cluster and validate the data
        """
        mv_name = "test_mv"
        node1 = self.start_cluster_and_get_node1(nodes=generate_cluster_topology(rack_num=2, nodes_per_rack=2))
        node4 = self.cluster.nodelist()[3]
        session = self.patient_cql_connection(node1)
        create_ks(session=session, name=self.KEYSPACE_NAME, rf=2)
        create_c1c2_table(session, cf=self.TABLE_NAME)
        session.execute(f"CREATE MATERIALIZED VIEW {mv_name} AS SELECT c1 FROM {self.KEYSPACE_NAME}.{self.TABLE_NAME} where c1 IS NOT NULL and key IS NOT NULL PRIMARY KEY (c1, key)")

        logger.debug("Copy sstables of node1 to node4")
        self.copy_sstables_to_node(copy_to_node=node4, migrated_files_dir="from-cluster-2-nodes/node1")

        logger.debug("Copy sstables of node2 to node1")
        self.copy_sstables_to_node(copy_to_node=node1, migrated_files_dir="from-cluster-2-nodes/node2")

        for node in [node1, node4]:
            load_and_stream_result, mark = self.run_load_and_stream(node)
            assert load_and_stream_result, f"Failed to run load and stream."
            result = self.wait_for_finish_load_and_stream(node=node, mark=mark)
            assert result, f"Load and stream was not finished"

        wait_for_view(cluster=self.cluster, session=session, ks=self.KEYSPACE_NAME, view=mv_name)

        for node in self.cluster.nodelist():
            self.check_number_of_rows(node, self.EXPECTED_ROWS_NUMBER, keyspace=self.KEYSPACE_NAME, table=self.TABLE_NAME)
            self.check_number_of_rows(node, self.EXPECTED_ROWS_NUMBER, keyspace=self.KEYSPACE_NAME, table=mv_name, is_view=True)

        for n in range(self.EXPECTED_ROWS_NUMBER):
            query_c1c2(session, key=n, consistency=ConsistencyLevel.QUORUM, c1_value=f"customtext1{n}", c2_value=f"customtext2{n}", ks=self.KEYSPACE_NAME, cf=self.TABLE_NAME)
            assert_one(session, query=f"select key from {self.KEYSPACE_NAME}.{mv_name} where c1 = 'customtext1{n}' and key='k{n}'", expected=[f"k{n}"], cl=ConsistencyLevel.QUORUM)

    def test_load_and_stream_asymmetric_cluster(self):
        """
        Test for the feature load_and_stream:
        https://github.com/scylladb/scylla/commit/df3ef800c20c60d7929ffa600649793aa6c73064

        Asymmetric cluster: Scylla is started with different SMP on the every node.
                            Test data was created on the cluster where SMP is same (2 nodes, RF=2, SMP 1)
        Load and stream sstables from 2-nodes cluster to 4-nodes cluster and validate the data
        """
        node1 = self.start_cluster_and_get_node1(nodes=generate_cluster_topology(rack_num=2, nodes_per_rack=2))
        node3 = self.cluster.nodelist()[2]

        for i, node in enumerate(self.cluster.nodelist()[1:]):
            node.stop(wait_other_notice=True)
            node.start(jvm_args=["--smp", str(i + 2)], wait_other_notice=True, wait_for_binary_proto=True)

        session = self.patient_cql_connection(node1)
        create_ks(session=session, name=self.KEYSPACE_NAME, rf=2)
        create_c1c2_table(session, cf=self.TABLE_NAME)

        for node_map in zip(["node1", "node2"], [node1, node3]):
            logger.debug(f"Copy sstables of {node_map[0]} to {node_map[1].name}")
            self.copy_sstables_to_node(copy_to_node=node1, migrated_files_dir=f"from-cluster-2-nodes/{node_map[0]}")
            load_and_stream_result, mark = self.run_load_and_stream(node_map[1])
            assert load_and_stream_result, f"Failed to run load and stream."
            result = self.wait_for_finish_load_and_stream(node=node_map[1], mark=mark)
            assert result, f"Load and stream was not finished"

        for node in self.cluster.nodelist():
            self.check_number_of_rows(node, self.EXPECTED_ROWS_NUMBER, keyspace=self.KEYSPACE_NAME, table=self.TABLE_NAME)

        for n in range(self.EXPECTED_ROWS_NUMBER):
            query_c1c2(session, key=n, consistency=ConsistencyLevel.QUORUM, c1_value=f"customtext1{n}", c2_value=f"customtext2{n}", ks=self.KEYSPACE_NAME, cf=self.TABLE_NAME)

    def test_load_and_stream_primary_replica_only(self):
        """
        Test for the feature load_and_stream:
        https://github.com/scylladb/scylla/commit/df3ef800c20c60d7929ffa600649793aa6c73064

        Test data was created on the 2-nodes cluster, RF=2, Scylla was started with SMP 1
        - Load and stream sstables to primary replica only: primary_replica_only=True

          "primary_replica_only" parameter meaning:

            For a given partition, if we set primary_replica_only to true, the data will be sent to only the
            primary replica.
            For example, RF = 3,  with primary_replica_only = true, data will be sent to node1,
            with primary_replica_only = false, data will be sent to node1,node2,node3

        - Run "nodetool repair" on all nodes to send the data to the all replicas
        - Validate the data
        """
        cluster_topology = {"dc1": {"rack1": 2, "rack2": 1}}
        node1 = self.start_cluster_and_get_node1(nodes=cluster_topology)
        node2 = self.cluster.nodelist()[1]
        session = self.patient_cql_connection(node1)
        create_ks(session=session, name=self.KEYSPACE_NAME, rf=2)
        create_c1c2_table(session, cf=self.TABLE_NAME)

        logger.debug("Copy sstables of node1 to node2")
        self.copy_sstables_to_node(copy_to_node=node2, migrated_files_dir="from-cluster-2-nodes/node1")

        load_and_stream_result, mark = self.run_load_and_stream(node2, primary_replica_only=True)
        assert load_and_stream_result, f"Failed to run load and stream."
        result = self.wait_for_finish_load_and_stream(node=node2, mark=mark)
        assert result, f"Load and stream was not finished"

        for node in self.cluster.nodelist():
            node.nodetool("repair -pr")

        for node in self.cluster.nodelist():
            self.check_number_of_rows(node, self.EXPECTED_ROWS_NUMBER, keyspace=self.KEYSPACE_NAME, table=self.TABLE_NAME)

        for n in range(self.EXPECTED_ROWS_NUMBER):
            query_c1c2(session, key=n, consistency=ConsistencyLevel.QUORUM, c1_value=f"customtext1{n}", c2_value=f"customtext2{n}", ks=self.KEYSPACE_NAME, cf=self.TABLE_NAME)

    def test_load_and_stream_frozen_pk(self):
        """
        Test for the feature load_and_stream:
        https://github.com/scylladb/scylla/commit/df3ef800c20c60d7929ffa600649793aa6c73064

        Test data was created on the 4-nodes cluster, RF=3, Scylla was started with SMP 1
        - Load and stream sstables of table with frozen(UDT) primary key and validate the data
        """
        cluster_topology = {"dc1": {"rack1": 2, "rack2": 1}}
        node1 = self.start_cluster_and_get_node1(nodes=cluster_topology)
        node2, node3 = self.cluster.nodelist()[1:]
        session = self.patient_cql_connection(node1)
        create_ks(session=session, rf=2, name=self.KEYSPACE_NAME)
        session.execute(f"CREATE TYPE {self.KEYSPACE_NAME}.frozen_fullname (firstname text,lastname text)")
        session.execute(f"CREATE TABLE {self.KEYSPACE_NAME}.{self.TABLE_NAME}(pk frozen<frozen_fullname>, ck int, v1 text, v2 text, PRIMARY KEY(pk, ck))")

        for node in ["node1", "node2"]:
            logger.debug(f"Copy sstables of {node} to node1")
            self.copy_sstables_to_node(copy_to_node=node1, migrated_files_dir=f"from-cluster-4-nodes-frozen-pk/{node}")
            load_and_stream_result, mark = self.run_load_and_stream(node1)
            assert load_and_stream_result, f"Failed to run load and stream."
            result = self.wait_for_finish_load_and_stream(node=node1, mark=mark)
            assert result, f"Load and stream was not finished"

        logger.debug(f"Copy sstables of node3 to node2")
        self.copy_sstables_to_node(copy_to_node=node2, migrated_files_dir=f"from-cluster-4-nodes-frozen-pk/node3")

        logger.debug(f"Copy sstables of node4 to node3")
        self.copy_sstables_to_node(copy_to_node=node3, migrated_files_dir=f"from-cluster-4-nodes-frozen-pk/node4")

        for node in [node2, node3]:
            load_and_stream_result, mark = self.run_load_and_stream(node)
            assert load_and_stream_result, f"Failed to run load and stream."
            result = self.wait_for_finish_load_and_stream(node=node, mark=mark)
            assert result, f"Load and stream was not finished"

        for node in self.cluster.nodelist():
            self.check_number_of_rows(node, self.EXPECTED_ROWS_NUMBER, keyspace=self.KEYSPACE_NAME, table=self.TABLE_NAME)

        logger.debug("Validate data")
        for n in range(self.EXPECTED_ROWS_NUMBER):
            pk_value = f"('firstname{n}', 'lastname{n}')"
            self.validate_row_data(session, key=n, query=f"SELECT v1, v2 FROM {self.KEYSPACE_NAME}.{self.TABLE_NAME} WHERE pk={pk_value} and ck={n}")

    def test_load_and_stream_2_columns_pk(self):
        """
        Test for the feature load_and_stream:
        https://github.com/scylladb/scylla/commit/df3ef800c20c60d7929ffa600649793aa6c73064

        Test data was created on the 4-nodes cluster, RF=3, Scylla was started with SMP 1
        - Load and stream sstables of table with 2 columns primary key and validate the data
        """
        cluster_topology = {"dc1": {"rack1": 2, "rack2": 1}}
        node1 = self.start_cluster_and_get_node1(nodes=cluster_topology)
        node2, node3 = self.cluster.nodelist()[1:]
        session = self.patient_cql_connection(node1)
        create_ks(session=session, rf=2, name=self.KEYSPACE_NAME)
        session.execute(f"CREATE TABLE {self.KEYSPACE_NAME}.{self.TABLE_NAME}(pk1 text, pk2 int, v1 text, v2 text, PRIMARY KEY((pk1, pk2)))")

        for node in ["node3", "node2"]:
            logger.debug(f"Copy sstables of {node} to node1")
            self.copy_sstables_to_node(copy_to_node=node1, migrated_files_dir=f"from-cluster-4-nodes-2-columns-pk/{node}")
            load_and_stream_result, mark = self.run_load_and_stream(node1)
            assert load_and_stream_result, f"Failed to run load and stream."
            result = self.wait_for_finish_load_and_stream(node=node1, mark=mark)
            assert result, f"Load and stream was not finished"

        logger.debug(f"Copy sstables of node1 to node2")
        self.copy_sstables_to_node(copy_to_node=node2, migrated_files_dir=f"from-cluster-4-nodes-2-columns-pk/node1")

        logger.debug(f"Copy sstables of node4 to node3")
        self.copy_sstables_to_node(copy_to_node=node3, migrated_files_dir=f"from-cluster-4-nodes-2-columns-pk/node4")

        for node in [node2, node3]:
            load_and_stream_result, mark = self.run_load_and_stream(node)
            assert load_and_stream_result, f"Failed to run load and stream."
            result = self.wait_for_finish_load_and_stream(node=node, mark=mark)
            assert result, f"Load and stream was not finished"

        for node in self.cluster.nodelist():
            self.check_number_of_rows(node, self.EXPECTED_ROWS_NUMBER, keyspace=self.KEYSPACE_NAME, table=self.TABLE_NAME)

        logger.debug("Validate data")
        for n in range(self.EXPECTED_ROWS_NUMBER):
            self.validate_row_data(session, key=n, query=f"SELECT v1, v2 FROM {self.KEYSPACE_NAME}.{self.TABLE_NAME} WHERE pk1='pk{n}' and pk2={n}")

    @staticmethod
    def validate_row_data(session, key, query):
        query = SimpleStatement(query, consistency_level=ConsistencyLevel.QUORUM)
        rows = list(session.execute(query))
        check_c1c2_result_one(success=True, rows=rows, tolerate_missing=False, must_be_missing=False, c1_value=f"customtext1{key}", c2_value=f"customtext2{key}")

    def run_load_and_stream(self, node: ScyllaNode, primary_replica_only: bool = False):
        mark = node.mark_log()

        logger.debug(f"Running load and stream on the node {node.name} for {self.KEYSPACE_NAME}.{self.TABLE_NAME}'")
        nodetool_cmd = f"refresh --load-and-stream"
        if primary_replica_only:
            nodetool_cmd += f" --primary-replica-only"
        nodetool_cmd += f" -- {self.KEYSPACE_NAME} {self.TABLE_NAME}"

        try:
            node.nodetool(nodetool_cmd)
        except NodetoolError:
            raise

        result = node.watch_log_for(f"Loading new SSTables for keyspace={self.KEYSPACE_NAME}, table={self.TABLE_NAME}, load_and_stream=true, primary_replica_only={str(primary_replica_only).lower()}", from_mark=mark, timeout=10)
        return result, mark

    def wait_for_finish_load_and_stream(self, node: ScyllaNode, mark):
        load_and_stream_done_expr = r"(?:storage_service|sstables_loader) - " rf"Done loading new SSTables for keyspace={self.KEYSPACE_NAME}, table={self.TABLE_NAME}, " r"load_and_stream=true.*status=(.*)"
        result = node.watch_log_for(load_and_stream_done_expr, from_mark=mark, timeout=10)
        return result

    def copy_sstables_to_node(self, copy_to_node: ScyllaNode, migrated_files_dir: str):
        dtest_path = os.path.dirname(os.path.realpath(__file__))

        cassandra_sstable_dir = f"{dtest_path}/cassandra-sstables/load-and-stream/{self.version}/{migrated_files_dir}/{self.TABLE_NAME}"
        logger.debug(f"cassandra sstables dir is {cassandra_sstable_dir}")
        assert os.path.isdir(cassandra_sstable_dir), f"Migrated files folder {cassandra_sstable_dir} doesn't exist"

        cf_dir = get_node_cf_dir(copy_to_node, self.KEYSPACE_NAME, self.TABLE_NAME)
        logger.debug(f"Column family directory is {cf_dir}")
        assert cf_dir, f"Failed to get column family directory {cf_dir}"
        assert os.path.isdir(cf_dir), f"Column family directory {cf_dir} doesn't exist"

        upload_dir = os.path.join(cf_dir, "upload")
        logger.debug(f"Column family upload directory is {upload_dir}")

        logger.debug("Copying sstables created by Cassandra...")
        self.copy_files_to(cassandra_sstable_dir, upload_dir)
