#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import logging
import math

import pytest

from dtest_class import Tester, create_ks

logger = logging.getLogger(__name__)
# Those are ideal values according to c* specifications
# they should pass

LIMIT_64_K = 64 * 1024
LIMIT_32K = 32 * 1024
LIMIT_128K = 128 * 1024
LIMIT_2GB = 2 * 1024 * 1024 * 1024

MAX_KEY_SIZE = LIMIT_64_K
MAX_BLOB_SIZE = 8388608  # theoretical limit LIMIT_2GB
MAX_COLUMNS = LIMIT_128K
MAX_TUPLES = LIMIT_32K
MAX_BATCH_SIZE = 50 * 1024
MAX_CELLS_COLUMNS = LIMIT_32K
MAX_CELLS_BATCH_SIZE = 50
MAX_CELLS = 16777216

# Those are values used to validate the tests code
# MAX_KEY_SIZE = 1000
# MAX_BLOB_SIZE = 1000
# MAX_COLUMNS = 1000
# MAX_TUPLES = 1000
# MAX_BATCH_SIZE = 1000
# MAX_CELLS_COLUMNS = 100
# MAX_CELLS_BATCH_SIZE = 100
# MAX_CELLS = 1000


@pytest.mark.single_node
class TestLimits(Tester):
    def prepare(self):
        """
        Sets up node to test against.
        """
        cluster = self.cluster
        return cluster

    def _do_test_max_key_length(self, session, node, size, expect_failure=False):
        print("Testing max key length for {}.{}".format(size, " Expected failure..." if expect_failure else ""))
        key_name = "k" * size

        c = f"CREATE TABLE test1 ({key_name} int PRIMARY KEY)"
        if expect_failure:
            expected_error = r"Key size too large: \d+ > 65535"
            self.ignore_log_patterns += [expected_error]
            with pytest.raises(Exception, match=expected_error):
                session.execute(c)
            return

        session.execute(c)

        session.execute("insert into ks.test1  (%s) values (1);" % key_name)
        session.execute("insert into ks.test1  (%s) values (2);" % key_name)

        node.flush()
        # Select
        res = session.execute(
            """
                SELECT * FROM ks.test1
                WHERE %s=1
        """
            % key_name
        )

        assert len(res.current_rows) == 1

        res = session.execute(
            """
                SELECT * FROM ks.test1
                WHERE %s=2
        """
            % key_name
        )

        assert len(res.current_rows) == 1
        session.execute("""DROP TABLE test1""")

    def test_max_key_length(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        create_ks(session, "ks", 1)

        # biggest that will currently work in scylla
        # key_name = "k" * 65526
        self._do_test_max_key_length(session, node, MAX_KEY_SIZE, expect_failure=True)
        self._do_test_max_key_length(session, node, MAX_KEY_SIZE - 9, expect_failure=True)

        self._do_test_max_key_length(session, node, MAX_KEY_SIZE - 10)

        size = MAX_KEY_SIZE // 2
        while size >= 1:
            self._do_test_max_key_length(session, node, size)
            size >>= 3

    def _do_test_blob_size(self, session, node, size):
        print("Testing blob size %i" % size)

        blob_a = "a" * size
        blob_b = "b" * size

        session.execute(
            """
            CREATE TABLE test1 (
                user ascii PRIMARY KEY,
                payload blob,
            )
        """
        )

        session.execute("insert into ks.test1  (user, payload) values ('tintin', textAsBlob('%s'));" % blob_a)
        session.execute("insert into ks.test1  (user, payload) values ('milou', textAsBlob('%s'));" % blob_b)

        node.flush()
        # Select
        res = session.execute(
            """
                SELECT * FROM ks.test1
                WHERE user='tintin'
        """
        )

        assert len(list(res)) == 1

        res = session.execute(
            """
                SELECT * FROM ks.test1
                WHERE user='milou'
        """
        )

        assert len(list(res)) == 1
        session.execute("""DROP TABLE test1""")

    def test_max_column_value_size(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        create_ks(session, "ks", 1)

        size = 1
        for i in range(int(math.log(MAX_BLOB_SIZE, 2))):
            size <<= 1
            self._do_test_blob_size(session, node, size - 1)

    def _do_test_max_tuples(self, session, node, count):
        print("Testing max tuples for %i" % count)
        t = ""
        v = ""
        for i in range(count):
            t += "int, "
            v += "1, "
        t = t[:-2]
        v = v[:-2]

        c = (
            """
            CREATE TABLE stuff (
              k int PRIMARY KEY,
              v frozen<tuple<%s>>
            );
            """
            % t
        )
        session.execute(c)

        c = "INSERT INTO stuff (k, v) VALUES(0, (%s));" % v
        session.execute(c)

        c = "SELECT * FROM STUFF;"
        res = session.execute(c)
        assert len(res.current_rows) == 1

        session.execute("""DROP TABLE stuff""")

    def test_max_tuple(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        create_ks(session, "ks", 1)

        count = 1
        for i in range(int(math.log(MAX_TUPLES, 2))):
            count <<= 1
            self._do_test_max_tuples(session, node, count - 1)

    def _do_test_max_batch_size(self, session, node, size):
        print("Testing max batch size for size=%i" % size)
        c = """
            CREATE TABLE stuff (
              k int PRIMARY KEY,
              v text
            );
            """
        session.execute(c)

        c = "BEGIN UNLOGGED  BATCH\n"
        row_size = 1000
        overhead = 100
        blob = (row_size - overhead) * "x"
        rows = size // row_size
        for i in range(rows):
            c += "INSERT INTO stuff (k, v) VALUES(%i, '%s')\n" % (i, blob)
        c += "APPLY BATCH;\n"

        session.execute(c)

        c = "SELECT * FROM STUFF;"
        res = session.execute(c)

        assert len(list(res)) == rows
        session.execute("""DROP TABLE STUFF""")

    def test_max_batch_size(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        create_ks(session, "ks", 1)

        size = 1
        for i in range(int(math.log(MAX_BATCH_SIZE, 2))):
            size <<= 1
            self._do_test_max_batch_size(session, node, size - 1)

    def _do_test_max_cell_count(self, session, cells):
        print("Testing max cells count for %i" % cells)
        keys = ""
        keys_create = ""
        columns = MAX_CELLS_COLUMNS
        for i in range(columns):
            keys += "key" + str(i) + ", "
            keys_create += "key" + str(i) + " int, "
        values = "1, " * columns

        c = """CREATE TABLE test1 (%s blub int PRIMARY KEY,)""" % keys_create
        session.execute(c)

        batch_size = MAX_CELLS_BATCH_SIZE
        rows = cells // columns
        c = "BEGIN UNLOGGED  BATCH\n"
        for i in range(rows):
            c += "insert into ks.test1  (%s blub) values (%s %i);\n" % (keys, values, i)
            if i == rows - 1 or (i + 1) % batch_size == 0:
                c += "APPLY BATCH;\n"
                session.execute(c)
                c = "BEGIN UNLOGGED  BATCH\n"

        session.execute("""DROP TABLE test1""")

    def test_max_cells(self):
        if self.cluster.scylla_mode == "debug":
            pytest.skip("client times out in debug mode")
        cluster = self.prepare()
        cluster.set_configuration_options(values={"query_tombstone_page_limit": 9999999, "batch_size_warn_threshold_in_kb": 1024 * 1024, "batch_size_fail_threshold_in_kb": 1024 * 1024, "commitlog_segment_size_in_mb": 64})
        cluster.populate(1).start(jvm_args=["--smp", "1", "--memory", "2G", "--logger-log-level", "lsa-timing=debug"])
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        create_ks(session, "ks", 1)

        cells = 1
        for i in range(int(math.log(MAX_CELLS, 2))):
            cells <<= 1
            self._do_test_max_cell_count(session, cells - 1)
