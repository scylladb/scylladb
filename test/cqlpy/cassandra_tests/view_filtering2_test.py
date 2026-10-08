# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

from .porting import *

# The original Java test class is parameterized to run each test with each of
# the CQL protocol versions which Cassandra supports. We use only the Python
# driver's default protocol version.

# Like ViewFiltering1Test, the Java test sets Cassandra's
# "cassandra.mv.allow_filtering_nonkey_columns_unsafe" system property,
# which most of the tests below need, because their views restrict regular
# columns of the base table. We can't set it through CQL, so on Cassandra
# create_view() skips these tests if the property isn't set.

def flushIf(cql, keyspace, flush):
    if flush:
        nodetool.flush_keyspace(cql, keyspace)

def testAllTypes(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(a int, b uuid, c set<text>)") as myType:
        columnNames = ("asciival, " +
                       "bigintval, " +
                       "blobval, " +
                       "booleanval, " +
                       "dateval, " +
                       "decimalval, " +
                       "doubleval, " +
                       "floatval, " +
                       "inetval, " +
                       "intval, " +
                       "textval, " +
                       "timeval, " +
                       "timestampval, " +
                       "timeuuidval, " +
                       "uuidval," +
                       "varcharval, " +
                       "varintval, " +
                       "frozenlistval, " +
                       "frozensetval, " +
                       "frozenmapval, " +
                       "tupleval, " +
                       "udtval")

        with create_table(cql, test_keyspace, "(" +
            "asciival ascii, " +
            "bigintval bigint, " +
            "blobval blob, " +
            "booleanval boolean, " +
            "dateval date, " +
            "decimalval decimal, " +
            "doubleval double, " +
            "floatval float, " +
            "inetval inet, " +
            "intval int, " +
            "textval text, " +
            "timeval time, " +
            "timestampval timestamp, " +
            "timeuuidval timeuuid, " +
            "uuidval uuid," +
            "varcharval varchar, " +
            "varintval varint, " +
            "frozenlistval frozen<list<int>>, " +
            "frozensetval frozen<set<uuid>>, " +
            "frozenmapval frozen<map<ascii, int>>," +
            "tupleval frozen<tuple<int, ascii, uuid>>," +
            "udtval frozen<" + myType + ">, " +
            "PRIMARY KEY (" + columnNames + "))") as table:

            with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE " +
                       "asciival = 'abc' AND " +
                       "bigintval = 123 AND " +
                       "blobval = 0xfeed AND " +
                       "booleanval = true AND " +
                       "dateval = '1987-03-23' AND " +
                       "decimalval = 123.123 AND " +
                       "doubleval = 123.123 AND " +
                       "floatval = 123.123 AND " +
                       "inetval = '127.0.0.1' AND " +
                       "intval = 123 AND " +
                       "textval = 'abc' AND " +
                       "timeval = '07:35:07.000111222' AND " +
                       "timestampval = 123123123 AND " +
                       "timeuuidval = 6BDDC89A-5644-11E4-97FC-56847AFE9799 AND " +
                       "uuidval = 6BDDC89A-5644-11E4-97FC-56847AFE9799 AND " +
                       "varcharval = 'abc' AND " +
                       "varintval = 123123123 AND " +
                       "frozenlistval = [1, 2, 3] AND " +
                       "frozensetval = {6BDDC89A-5644-11E4-97FC-56847AFE9799} AND " +
                       "frozenmapval = {'a': 1, 'b': 2} AND " +
                       "tupleval = (1, 'foobar', 6BDDC89A-5644-11E4-97FC-56847AFE9799) AND " +
                       "udtval = {a: 1, b: 6BDDC89A-5644-11E4-97FC-56847AFE9799, c: {'foo', 'bar'}} " +
                       "PRIMARY KEY (" + columnNames + ")") as view:

                execute(cql, table, "INSERT INTO %s (" + columnNames + ") VALUES (" +
                        "'abc'," +
                        "123," +
                        "0xfeed," +
                        "true," +
                        "'1987-03-23'," +
                        "123.123," +
                        "123.123," +
                        "123.123," +
                        "'127.0.0.1'," +
                        "123," +
                        "'abc'," +
                        "'07:35:07.000111222'," +
                        "123123123," +
                        "6BDDC89A-5644-11E4-97FC-56847AFE9799," +
                        "6BDDC89A-5644-11E4-97FC-56847AFE9799," +
                        "'abc'," +
                        "123123123," +
                        "[1, 2, 3]," +
                        "{6BDDC89A-5644-11E4-97FC-56847AFE9799}," +
                        "{'a': 1, 'b': 2}," +
                        "(1, 'foobar', 6BDDC89A-5644-11E4-97FC-56847AFE9799)," +
                        "{a: 1, b: 6BDDC89A-5644-11E4-97FC-56847AFE9799, c: {'foo', 'bar'}})")

                assert list(execute(cql, view, "SELECT * FROM %s"))

                execute(cql, table, "ALTER TABLE %s RENAME inetval TO foo")
                assert list(execute(cql, view, "SELECT * FROM %s"))

# Scylla allows a view to restrict a regular column of the base table only if
# it is in the view's primary key, while d isn't.
# Reproduces #4250 (views with a filter on non-key columns).
@pytest.mark.xfail(reason="#4250")
def testMVCreationWithNonPrimaryRestrictions(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, PRIMARY KEY (a, b))") as table:
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                     "WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL AND d = 1 " +
                                     "PRIMARY KEY (a, b, c)"):
            pass

def testNonPrimaryRestrictions(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, PRIMARY KEY (a, b))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 1, 0)

        # only accept rows where c = 1
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                     "WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL AND c = 1 " +
                                     "PRIMARY KEY (a, b, c)") as view:

            assert_rows_ignoring_order(execute(cql, view, "SELECT a, b, c, d FROM %s"),
                                       row(0, 0, 1, 0),
                                       row(0, 1, 1, 0),
                                       row(1, 0, 1, 0),
                                       row(1, 1, 1, 0))

            # insert new rows that do not match the filter
            execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 0, 0, 0)
            execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 1, 2, 0)
            assert_rows_ignoring_order(execute(cql, view, "SELECT a, b, c, d FROM %s"),
                                       row(0, 0, 1, 0),
                                       row(0, 1, 1, 0),
                                       row(1, 0, 1, 0),
                                       row(1, 1, 1, 0))

            # insert new row that does match the filter
            execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 2, 1, 0)
            assert_rows_ignoring_order(execute(cql, view, "SELECT a, b, c, d FROM %s"),
                                       row(0, 0, 1, 0),
                                       row(0, 1, 1, 0),
                                       row(1, 0, 1, 0),
                                       row(1, 1, 1, 0),
                                       row(1, 2, 1, 0))

            # update rows that don't match the filter
            execute(cql, table, "UPDATE %s SET d = ? WHERE a = ? AND b = ?", 2, 2, 0)
            execute(cql, table, "UPDATE %s SET d = ? WHERE a = ? AND b = ?", 1, 2, 1)
            assert_rows_ignoring_order(execute(cql, view, "SELECT a, b, c, d FROM %s"),
                                       row(0, 0, 1, 0),
                                       row(0, 1, 1, 0),
                                       row(1, 0, 1, 0),
                                       row(1, 1, 1, 0),
                                       row(1, 2, 1, 0))

            # update a row that does match the filter
            execute(cql, table, "UPDATE %s SET d = ? WHERE a = ? AND b = ?", 1, 1, 0)
            assert_rows_ignoring_order(execute(cql, view, "SELECT a, b, c, d FROM %s"),
                                       row(0, 0, 1, 0),
                                       row(0, 1, 1, 0),
                                       row(1, 0, 1, 1),
                                       row(1, 1, 1, 0),
                                       row(1, 2, 1, 0))

            # delete rows that don't match the filter
            execute(cql, table, "DELETE FROM %s WHERE a = ? AND b = ?", 2, 0)
            assert_rows_ignoring_order(execute(cql, view, "SELECT a, b, c, d FROM %s"),
                                       row(0, 0, 1, 0),
                                       row(0, 1, 1, 0),
                                       row(1, 0, 1, 1),
                                       row(1, 1, 1, 0),
                                       row(1, 2, 1, 0))

            # delete a row that does match the filter
            execute(cql, table, "DELETE FROM %s WHERE a = ? AND b = ?", 1, 2)
            assert_rows_ignoring_order(execute(cql, view, "SELECT a, b, c, d FROM %s"),
                                       row(0, 0, 1, 0),
                                       row(0, 1, 1, 0),
                                       row(1, 0, 1, 1),
                                       row(1, 1, 1, 0))

            # delete a partition that matches the filter
            execute(cql, table, "DELETE FROM %s WHERE a = ?", 1)
            assert_rows_ignoring_order(execute(cql, view, "SELECT a, b, c, d FROM %s"),
                                       row(0, 0, 1, 0),
                                       row(0, 1, 1, 0))

def testcomplexRestrictedTimestampUpdateTestWithFlush(cql, test_keyspace):
    complexRestrictedTimestampUpdateTest(cql, test_keyspace, True)

def testcomplexRestrictedTimestampUpdateTestWithoutFlush(cql, test_keyspace):
    complexRestrictedTimestampUpdateTest(cql, test_keyspace, False)

def complexRestrictedTimestampUpdateTest(cql, test_keyspace, flush):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, e int, PRIMARY KEY (a, b))") as table:
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                     "WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL AND c = 1 " +
                                     "PRIMARY KEY (c, a, b)") as mv, \
             nodetool.no_autocompaction_context(cql, mv):

            #Set initial values TS=0, matching the restriction and verify view
            execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (0, 0, 1, 0) USING TIMESTAMP 0")
            assert_rows(execute(cql, mv, "SELECT d FROM %s WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0))

            flushIf(cql, test_keyspace, flush)

            #update c's timestamp TS=2
            execute(cql, table, "UPDATE %s USING TIMESTAMP 2 SET c = ? WHERE a = ? and b = ? ", 1, 0, 0)
            assert_rows(execute(cql, mv, "SELECT d FROM %s WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0))

            flushIf(cql, test_keyspace, flush)

            #change c's value and TS=3, tombstones c=1 and adds c=0 record
            execute(cql, table, "UPDATE %s USING TIMESTAMP 3 SET c = ? WHERE a = ? and b = ? ", 0, 0, 0)
            assert_empty(execute(cql, mv, "SELECT d FROM %s WHERE c = ? and a = ? and b = ?", 0, 0, 0))

            if flush:
                nodetool.compact(cql, mv)
                nodetool.flush_keyspace(cql, test_keyspace)

            #change c's value back to 1 with TS=4, check we can see d
            execute(cql, table, "UPDATE %s USING TIMESTAMP 4 SET c = ? WHERE a = ? and b = ? ", 1, 0, 0)
            if flush:
                nodetool.compact(cql, mv)
                nodetool.flush_keyspace(cql, test_keyspace)

            assert_rows(execute(cql, mv, "SELECT d, e FROM %s WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0, None))

            #Add e value @ TS=1
            execute(cql, table, "UPDATE %s USING TIMESTAMP 1 SET e = ? WHERE a = ? and b = ? ", 1, 0, 0)
            assert_rows(execute(cql, mv, "SELECT d, e FROM %s WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0, 1))

            flushIf(cql, test_keyspace, flush)

            #Change d value @ TS=2
            execute(cql, table, "UPDATE %s USING TIMESTAMP 2 SET d = ? WHERE a = ? and b = ? ", 2, 0, 0)
            assert_rows(execute(cql, mv, "SELECT d FROM %s WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(2))

            flushIf(cql, test_keyspace, flush)

            #Change d value @ TS=3
            execute(cql, table, "UPDATE %s USING TIMESTAMP 3 SET d = ? WHERE a = ? and b = ? ", 1, 0, 0)
            assert_rows(execute(cql, mv, "SELECT d FROM %s WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(1))

            #Tombstone c
            execute(cql, table, "DELETE FROM %s WHERE a = ? and b = ?", 0, 0)
            assert_empty(execute(cql, mv, "SELECT d FROM %s"))
            assert_empty(execute(cql, mv, "SELECT d FROM %s"))

            #Add back without D
            execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (0, 0, 1)")

            #Make sure D doesn't pop back in.
            assert_rows(execute(cql, mv, "SELECT d FROM %s WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(None))

            #New partition
            # insert a row with timestamp 0
            execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP 0", 1, 0, 1, 0, 0)

            # overwrite pk and e with timestamp 1, but don't overwrite d
            execute(cql, table, "INSERT INTO %s (a, b, c, e) VALUES (?, ?, ?, ?) USING TIMESTAMP 1", 1, 0, 1, 0)

            # delete with timestamp 0 (which should only delete d)
            execute(cql, table, "DELETE FROM %s USING TIMESTAMP 0 WHERE a = ? AND b = ?", 1, 0)
            assert_rows(execute(cql, mv, "SELECT a, b, c, d, e FROM %s WHERE c = ? and a = ? and b = ?", 1, 1, 0),
                        row(1, 0, 1, None, 0))

            execute(cql, table, "UPDATE %s USING TIMESTAMP 2 SET c = ? WHERE a = ? AND b = ?", 1, 1, 1)
            execute(cql, table, "UPDATE %s USING TIMESTAMP 3 SET c = ? WHERE a = ? AND b = ?", 1, 1, 0)
            assert_rows(execute(cql, mv, "SELECT a, b, c, d, e FROM %s WHERE c = ? and a = ? and b = ?", 1, 1, 0),
                        row(1, 0, 1, None, 0))

            execute(cql, table, "UPDATE %s USING TIMESTAMP 3 SET d = ? WHERE a = ? AND b = ?", 0, 1, 0)
            assert_rows(execute(cql, mv, "SELECT a, b, c, d, e FROM %s WHERE c = ? and a = ? and b = ?", 1, 1, 0),
                        row(1, 0, 1, 0, 0))

def testRestrictedRegularColumnTimestampUpdates(cql, test_keyspace):
    # Regression test for CASSANDRA-10910
    with create_table(cql, test_keyspace, "(" +
                      "k int PRIMARY KEY, " +
                      "c int, " +
                      "val int)") as table:
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                     "WHERE k IS NOT NULL AND c IS NOT NULL AND c = 1 " +
                                     "PRIMARY KEY (k,c)") as view:

            execute(cql, table, "UPDATE %s SET c = ?, val = ? WHERE k = ?", 0, 0, 0)
            execute(cql, table, "UPDATE %s SET val = ? WHERE k = ?", 1, 0)
            execute(cql, table, "UPDATE %s SET c = ? WHERE k = ?", 1, 0)
            assert_rows(execute(cql, view, "SELECT c, k, val FROM %s"), row(1, 0, 1))

            execute(cql, table, "TRUNCATE %s")

            execute(cql, table, "UPDATE %s USING TIMESTAMP 1 SET c = ?, val = ? WHERE k = ?", 0, 0, 0)
            execute(cql, table, "UPDATE %s USING TIMESTAMP 3 SET c = ? WHERE k = ?", 1, 0)
            execute(cql, table, "UPDATE %s USING TIMESTAMP 2 SET val = ? WHERE k = ?", 1, 0)
            execute(cql, table, "UPDATE %s USING TIMESTAMP 4 SET c = ? WHERE k = ?", 1, 0)
            execute(cql, table, "UPDATE %s USING TIMESTAMP 3 SET val = ? WHERE k = ?", 2, 0)
            assert_rows(execute(cql, view, "SELECT c, k, val FROM %s"), row(1, 0, 2))

def testOldTimestampsWithRestrictions(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "k int, " +
                      "c int, " +
                      "val text, " + "" +
                      "PRIMARY KEY(k, c))") as table:
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                     "WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL AND val = 'baz' " +
                                     "PRIMARY KEY (val,k,c)") as view:

            for i in range(100):
                execute(cql, table, "INSERT into %s (k,c,val)VALUES(?,?,?)", 0, i % 2, "baz")

            nodetool.flush(cql, table)

            assert_row_count(execute(cql, table, "select * from %s"), 2)
            assert_row_count(execute(cql, view, "select * from %s"), 2)

            assert_rows(execute(cql, table, "SELECT val from %s where k = 0 and c = 0"), row("baz"))
            assert_rows(execute(cql, view, "SELECT c from %s where k = 0 and val = ?", "baz"), row(0), row(1))

            #Make sure an old TS does nothing
            execute(cql, table, "UPDATE %s USING TIMESTAMP 100 SET val = ? where k = ? AND c = ?", "bar", 0, 1)
            assert_rows(execute(cql, table, "SELECT val from %s where k = 0 and c = 1"), row("baz"))
            assert_rows(execute(cql, view, "SELECT c from %s where k = 0 and val = ?", "baz"), row(0), row(1))
            assert_empty(execute(cql, view, "SELECT c from %s where k = 0 and val = ?", "bar"))

            #Latest TS
            execute(cql, table, "UPDATE %s SET val = ? where k = ? AND c = ?", "bar", 0, 1)
            assert_rows(execute(cql, table, "SELECT val from %s where k = 0 and c = 1"), row("bar"))
            assert_empty(execute(cql, view, "SELECT c from %s where k = 0 and val = ?", "bar"))
            assert_rows(execute(cql, view, "SELECT c from %s where k = 0 and val = ?", "baz"), row(0))
