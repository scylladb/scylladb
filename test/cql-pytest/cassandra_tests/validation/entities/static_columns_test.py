# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 6ca34f81386dc8f6020cdf2ea4246bca2a0896c5
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0

from cassandra_tests.porting import *

#Migrated from cql_tests.py:TestCQL.static_columns_test()
def testStaticColumns(cql, test_keyspace):
    for force_flush in [True, False]:
        with create_table(cql, test_keyspace, "(k int, p int, s int static, v int, PRIMARY KEY (k, p))") as table:
            execute(cql, table, "INSERT INTO %s(k, s) VALUES (0, 42)")
            if force_flush:
                flush(cql, table)
            assert_rows(execute(cql, table, "SELECT * FROM %s"), [0, None, 42, None])

            # Check that writetime works (CASSANDRA-7081) -- we can't predict
            # the exact value easily so we just check that it's non zero
            row = execute(cql, table, "SELECT s, writetime(s) FROM %s WHERE k=0").one()
            assert row[0] == 42
            assert row[1] > 0

            execute(cql, table, "INSERT INTO %s (k, p, s, v) VALUES (0, 0, 12, 0)")
            execute(cql, table, "INSERT INTO %s (k, p, s, v) VALUES (0, 1, 24, 1)")
            if force_flush:
                flush(cql, table)

            # Check the static columns in indeed "static"
            assert_rows(execute(cql, table, "SELECT * FROM %s"), [0, 0, 24, 0], [0, 1, 24, 1])

            # Check we do correctly get the static column value with a SELECT *, even
            # if we're only slicing part of the partition
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k=0 AND p=0"), [0, 0, 24, 0])
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k=0 AND p=1"), [0, 1, 24, 1])

            # Test for IN on the clustering key (CASSANDRA-6769)
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k=0 AND p IN (0, 1)"), [0, 0, 24, 0], [0, 1, 24, 1])

            # Check things still work if we don't select the static column.
            # We also want this to not request the static columns internally
            # at all, though that part require debugging to assert
            assert_rows(execute(cql, table, "SELECT p, v FROM %s WHERE k=0 AND p=1"), [1, 1])

            # Check selecting only a static column with distinct only yield
            # one value (as we only query the static columns)
            assert_rows(execute(cql, table, "SELECT DISTINCT s FROM %s WHERE k=0"), [24])
            # But without DISTINCT, we still get one result per row
            assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k=0"), [24],[24])
            # but that querying other columns does correctly yield the full partition
            assert_rows(execute(cql, table, "SELECT s, v FROM %s WHERE k=0"), [24, 0], [24, 1])
            assert_rows(execute(cql, table, "SELECT s, v FROM %s WHERE k=0 AND p=1"), [24, 1])
            assert_rows(execute(cql, table, "SELECT p, s FROM %s WHERE k=0 AND p=1"), [1, 24])
            assert_rows(execute(cql, table, "SELECT k, p, s FROM %s WHERE k=0 AND p=1"), [0, 1, 24])

            # Check that deleting a row don't implicitly deletes statics
            execute(cql, table, "DELETE FROM %s WHERE k=0 AND p=0")
            if force_flush:
                flush(cql, table)
            assert_rows(execute(cql, table, "SELECT * FROM %s"), [0, 1, 24, 1])

            # But that explicitly deleting the static column does remove it
            execute(cql, table, "DELETE s FROM %s WHERE k=0")
            if force_flush:
                flush(cql, table)
            assert_rows(execute(cql, table, "SELECT * FROM %s"), [0, 1, None, 1])

            # Check we can add a static column ...
            execute(cql, table, "ALTER TABLE %s ADD s2 int static")
            assert_rows(execute(cql, table, "SELECT * FROM %s"), [0, 1, None, None, 1])
            execute(cql, table, "INSERT INTO %s (k, p, s2, v) VALUES(0, 2, 42, 2)")
            assert_rows(execute(cql, table, "SELECT * FROM %s"), [0, 1, None, 42, 1], [0, 2, None, 42, 2])
            # ... and that we can drop it
            execute(cql, table, "ALTER TABLE %s DROP s2")
            assert_rows(execute(cql, table, "SELECT * FROM %s"), [0, 1, None, 1], [0, 2, None, 2])

# Migrated from cql_tests.py:TestCQL.static_columns_with_2i_test()
def testStaticColumnsWithSecondaryIndex(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, p int, s int static, v int, PRIMARY KEY (k, p))") as table:
        execute(cql, table, "CREATE INDEX ON %s (v)")
        execute(cql, table, "INSERT INTO %s (k, p, s, v) VALUES (0, 0, 42, 1)")
        execute(cql, table, "INSERT INTO %s (k, p, v) VALUES (0, 1, 1)")
        execute(cql, table, "INSERT INTO %s (k, p, v) VALUES (0, 2, 2)")

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE v = 1"), [0, 0, 42, 1], [0, 1, 42, 1])
        assert_rows(execute(cql, table, "SELECT p, s FROM %s WHERE v = 1"), [0, 42], [1, 42])
        assert_rows(execute(cql, table, "SELECT p FROM %s WHERE v = 1"), [0], [1])
        # Reproduces issue #8869:
        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE v = 1"), [42], [42])

def checkDistinctRows(rows, sort, *ranges):
    assert len(ranges) % 2 == 0
    numdim = len(ranges) // 2
    _from = [None] * numdim
    _to = [None] * numdim

    i = 0
    j = 0
    while i < len(ranges) and j < numdim:
        _from[j] = ranges[i]
        _to[j] = ranges[i+1]
        i += 2
        j += 1
    # sort the rows
    for i in range(numdim):
        vals = [None] * len(rows)
        for j in range(len(rows)):
            vals[j] = rows[j][i]
        if sort:
            vals.sort()
        for j in range(_from[i], _to[i]):
            assert j == vals[j - _from[i]]

# Migrated from cql_tests.py:TestCQL.static_columns_with_distinct_test()
def testStaticColumnsWithDistinct(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, p int, s int static, PRIMARY KEY (k, p))") as table:
        execute(cql, table, "INSERT INTO %s (k, p) VALUES (1, 1)")
        execute(cql, table, "INSERT INTO %s (k, p) VALUES (1, 2)")

        assert_rows(execute(cql, table, "SELECT k, s FROM %s"), [1, None], [1, None])
        assert_rows(execute(cql, table, "SELECT DISTINCT k, s FROM %s"), [1, None])

        assert execute(cql, table, "SELECT DISTINCT s FROM %s WHERE k=1").one()[0] == None
        assert_empty(execute(cql, table, "SELECT DISTINCT s FROM %s WHERE k=2"))

        execute(cql, table, "INSERT INTO %s (k, p, s) VALUES (2, 1, 3)")
        execute(cql, table, "INSERT INTO %s (k, p) VALUES (2, 2)")

        assert_rows(execute(cql, table, "SELECT k, s FROM %s"), [1, None], [1, None], [2, 3], [2, 3])
        assert_rows(execute(cql, table, "SELECT DISTINCT k, s FROM %s"), [1, None], [2, 3])
        assert execute(cql, table, "SELECT DISTINCT s FROM %s WHERE k=1").one()[0] == None
        assert_rows(execute(cql, table, "SELECT DISTINCT s FROM %s WHERE k=2"), [3])

        assert_invalid(cql, table, "SELECT DISTINCT s FROM %s")

        # paging to test for CASSANDRA-8108
        execute(cql, table, "TRUNCATE %s")
        for i in range(10):
            for j in range(10):
                execute(cql, table, "INSERT INTO %s (k, p, s) VALUES (?, ?, ?)", i, j, i)

        rows = list(execute(cql, table, "SELECT DISTINCT k, s FROM %s"))
        checkDistinctRows(rows, True, 0, 10, 0, 10)

        keys = "0, 1, 2, 3, 4, 5, 6, 7, 8, 9"
        rows = list(execute(cql, table, "SELECT DISTINCT k, s FROM %s WHERE k IN (" + keys + ")"))
        checkDistinctRows(rows, False, 0, 10, 0, 10)

    # additional testing for CASSANRA-8087
    with create_table(cql, test_keyspace, "(k int, c1 int, c2 int, s1 int static, s2 int static, PRIMARY KEY (k, c1, c2))") as table:
        for i in range(10):
            for j in range(5):
                for k in range(5):
                    execute(cql, table, "INSERT INTO %s (k, c1, c2, s1, s2) VALUES (?, ?, ?, ?, ?)", i, j, k, i, i + 1)
        rows = list(execute(cql, table, "SELECT DISTINCT k, s1 FROM %s"))
        checkDistinctRows(rows, True, 0, 10, 0, 10)

        rows = list(execute(cql, table, "SELECT DISTINCT k, s2 FROM %s"))
        checkDistinctRows(rows, True, 0, 10, 1, 11)

        rows = list(execute(cql, table, "SELECT DISTINCT k, s1 FROM %s LIMIT 10"))
        checkDistinctRows(rows, True, 0, 10, 0, 10)

        rows = list(execute(cql, table, "SELECT DISTINCT k, s1 FROM %s WHERE k IN (" + keys + ")"))
        checkDistinctRows(rows, False, 0, 10, 0, 10)

        rows = list(execute(cql, table, "SELECT DISTINCT k, s2 FROM %s WHERE k IN (" + keys + ")"))
        checkDistinctRows(rows, False, 0, 10, 1, 11)

        rows = list(execute(cql, table, "SELECT DISTINCT k, s1 FROM %s WHERE k IN (" + keys + ")"))
        checkDistinctRows(rows, True, 0, 10, 0, 10)


# Test LIMIT when static columns are present (CASSANDRA-6956),
# migrated from cql_tests.py:TestCQL.static_with_limit_test()
def testStaticColumnsWithLimit(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, s int static, v int, PRIMARY KEY (k, v))") as table:
        execute(cql, table, "INSERT INTO %s (k, s) VALUES(0, 42)")
        for i in range(4):
            execute(cql, table, "INSERT INTO %s(k, v) VALUES(0, ?)", i)

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 0 LIMIT 1"),
                   [0, 0, 42])
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 0 LIMIT 2"),
                   [0, 0, 42],
                   [0, 1, 42])
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = 0 LIMIT 3"),
                   [0, 0, 42],
                   [0, 1, 42],
                   [0, 2, 42])

# Test for bug of CASSANDRA-7455,
# migrated from cql_tests.py:TestCQL.static_with_empty_clustering_test()
def testStaticColumnsWithEmptyClustering(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pkey text, ckey text, value text, static_value text static, PRIMARY KEY (pkey, ckey))") as table:
        execute(cql, table, "INSERT INTO %s (pkey, static_value) VALUES ('partition1', 'static value')")
        execute(cql, table, "INSERT INTO %s (pkey, ckey, value) VALUES('partition1', '', 'value')")
        assert_rows(execute(cql, table, "SELECT * FROM %s"),
                   ["partition1", "", "static value", "value"])

# Migrated from cql_tests.py:TestCQL.alter_clustering_and_static_test()
def testAlterClusteringAndStatic(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(bar int, PRIMARY KEY (bar))") as table:
        # We shouldn't allow static when there is not clustering columns
        assert_invalid(cql, table, "ALTER TABLE %s ADD bar2 text static")

# Ensure that deleting and compacting a static row that should be purged doesn't throw.
# This is a test for CASSANDRA-11988.
def testStaticColumnPurging(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pkey text, ckey text, value text, static_value text static, PRIMARY KEY (pkey, ckey)) WITH gc_grace_seconds = 0") as table:
        execute(cql, table, "INSERT INTO %s (pkey, ckey, static_value, value) VALUES (?, ?, ?, ?)", "k1", "c1", "s1", "v1")
        flush(cql, table)
        execute(cql, table, "DELETE static_value FROM %s WHERE pkey = ?", "k1")
        flush(cql, table)
        #Thread.sleep(1000);
        compact(cql, table)
        assert_rows(execute(cql, table, "SELECT * FROM %s"), ["k1", "c1", None, "v1"])
