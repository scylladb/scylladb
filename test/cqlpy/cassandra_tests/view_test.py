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
from .. import nodetool
from ..util import new_cql
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.protocol import SyntaxException, ConfigurationException

# The Java test testClientWarningOnCreate was not translated: it checks that
# creating a materialized view warns that "Materialized views are
# experimental and are not recommended for production use". Scylla doesn't
# consider materialized views experimental, so doesn't print this warning.
#
# The Java test testDisableMaterializedViews was not translated: it checks
# Cassandra's "materialized_views_enabled" configuration option by setting
# it through Cassandra's internal APIs, which we can't do through CQL.
#
# The Java test testTruncateWhileBuilding was not translated: it uses Byteman
# to block Cassandra's view builder in the middle of building a view, and
# checks Cassandra's internal view-builder metrics.

# Cassandra fails dropping a non-existent view with an InvalidRequest error
# "Materialized view 'ks.view' doesn't exist", Scylla with a
# ConfigurationException error "Cannot drop non existing materialized view
# 'view' in keyspace 'ks'." We accept both.
NON_EXISTENT_VIEW = "doesn't exist|Cannot drop non existing materialized view"

def testNonExistingOnes(cql, test_keyspace):
    with pytest.raises((InvalidRequest, ConfigurationException), match=NON_EXISTENT_VIEW):
        cql.execute("DROP MATERIALIZED VIEW " + test_keyspace + ".view_does_not_exist")
    with pytest.raises((InvalidRequest, ConfigurationException), match=NON_EXISTENT_VIEW):
        cql.execute("DROP MATERIALIZED VIEW keyspace_does_not_exist.view_does_not_exist")

    cql.execute("DROP MATERIALIZED VIEW IF EXISTS " + test_keyspace + ".view_does_not_exist")
    cql.execute("DROP MATERIALIZED VIEW IF EXISTS keyspace_does_not_exist.view_does_not_exist")

def testStaticTable(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "k int, " +
                      "c int, " +
                      "sval text static, " +
                      "val text, " +
                      "PRIMARY KEY(k,c))") as table:

        # Use of static column in a MV primary key should fail
        with pytest.raises(InvalidRequest):
            with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE sval IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (sval,k,c)"):
                pass

        # Explicit select of static column in MV should fail
        with pytest.raises(InvalidRequest):
            with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT val, sval FROM %s WHERE val IS NOT NULL AND  k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val, k, c)"):
                pass

        # Implicit select of static column in MV should fail
        with pytest.raises(InvalidRequest):
            with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val,k,c)"):
                pass

        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT val,k,c FROM %s WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val,k,c)") as view:

            for i in range(100):
                execute(cql, table, "INSERT into %s (k,c,sval,val)VALUES(?,?,?,?)", 0, i % 2, "bar" + str(i), "baz")

            assert_row_count(execute(cql, table, "select * from %s"), 2)

            assert_rows(execute(cql, table, "SELECT sval from %s"), row("bar99"), row("bar99"))

            assert_row_count(execute(cql, view, "select * from %s"), 2)

            assert_invalid(cql, view, "SELECT sval from %s")

def testOldTimestamps(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "k int, " +
                      "c int, " +
                      "val text, " +
                      "PRIMARY KEY(k,c))") as table:

        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val,k,c)") as view:

            for i in range(100):
                execute(cql, table, "INSERT into %s (k,c,val)VALUES(?,?,?)", 0, i % 2, "baz")

            flush(cql, table)

            assert_row_count(execute(cql, table, "select * from %s"), 2)
            assert_row_count(execute(cql, view, "select * from %s"), 2)

            assert_rows(execute(cql, table, "SELECT val from %s where k = 0 and c = 0"), row("baz"))
            assert_rows(execute(cql, view, "SELECT c from %s where k = 0 and val = ?", "baz"), row(0), row(1))

            #Make sure an old TS does nothing
            execute(cql, table, "UPDATE %s USING TIMESTAMP 100 SET val = ? where k = ? AND c = ?", "bar", 0, 0)
            assert_rows(execute(cql, table, "SELECT val from %s where k = 0 and c = 0"), row("baz"))
            assert_rows(execute(cql, view, "SELECT c from %s where k = 0 and val = ?", "baz"), row(0), row(1))
            assert_empty(execute(cql, view, "SELECT c from %s where k = 0 and val = ?", "bar"))

            #Latest TS
            execute(cql, table, "UPDATE %s SET val = ? where k = ? AND c = ?", "bar", 0, 0)
            assert_rows(execute(cql, table, "SELECT val from %s where k = 0 and c = 0"), row("bar"))
            assert_rows(execute(cql, view, "SELECT c from %s where k = 0 and val = ?", "bar"), row(0))
            assert_rows(execute(cql, view, "SELECT c from %s where k = 0 and val = ?", "baz"), row(1))

def testCountersTable(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "k int PRIMARY KEY, " +
                      "count counter)") as table:

        # MV on counter should fail
        with pytest.raises(InvalidRequest):
            with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE count IS NOT NULL AND k IS NOT NULL PRIMARY KEY (count,k)"):
                pass

def testDurationsTable(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "k int PRIMARY KEY, " +
                      "result duration)") as table:

        # MV on duration should fail
        # Scylla's message is "Cannot use Duration column 'result' in
        # PRIMARY KEY of materialized view".
        with pytest.raises(InvalidRequest, match="duration type is not supported for PRIMARY KEY column 'result'|Cannot use Duration column 'result' in PRIMARY KEY"):
            with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE result IS NOT NULL AND k IS NOT NULL PRIMARY KEY (result,k)"):
                pass

def testBuilderWidePartition(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "k int, " +
                      "c int, " +
                      "intval int, " +
                      "PRIMARY KEY (k, c))") as table:

        # The Java test runs these INSERTs one after another. To make the
        # test faster, we run them concurrently.
        stmt = cql.prepare(f"INSERT INTO {table} (k, c, intval) VALUES (?, ?, ?)")
        execute_concurrent_with_args(cql, stmt, [(0, i, 0) for i in range(1024)], concurrency=100, raise_on_first_error=True)

        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE k IS NOT NULL AND c IS NOT NULL AND intval IS NOT NULL PRIMARY KEY (intval, c, k)") as view:

            assert_rows(execute(cql, table, "SELECT count(*) from %s WHERE k = ?", 0), row(1024))
            assert_rows(execute(cql, view, "SELECT count(*) from %s WHERE intval = ?", 0), row(1024))

# Reproduces SCYLLADB-5141 (snake_case names of native functions - here,
# from_json()).
@pytest.mark.xfail(reason="SCYLLADB-5141")
def testCollections(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "k int, " +
                      "intval int, " +
                      "listval list<int>, " +
                      "PRIMARY KEY (k))") as table:

        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE k IS NOT NULL AND intval IS NOT NULL PRIMARY KEY (intval, k)") as view:

            execute(cql, table, "INSERT INTO %s (k, intval, listval) VALUES (?, ?, from_json(?))", 0, 0, "[1, 2, 3]")
            assert_rows(execute(cql, table, "SELECT k, listval FROM %s WHERE k = ?", 0), row(0, [1, 2, 3]))
            assert_rows(execute(cql, view, "SELECT k, listval from %s WHERE intval = ?", 0), row(0, [1, 2, 3]))

            execute(cql, table, "INSERT INTO %s (k, intval) VALUES (?, ?)", 1, 1)
            execute(cql, table, "INSERT INTO %s (k, listval) VALUES (?, from_json(?))", 1, "[1, 2, 3]")
            assert_rows(execute(cql, table, "SELECT k, listval FROM %s WHERE k = ?", 1), row(1, [1, 2, 3]))
            assert_rows(execute(cql, view, "SELECT k, listval from %s WHERE intval = ?", 1), row(1, [1, 2, 3]))

# Reproduces SCYLLADB-5141 (snake_case names of native functions - here,
# from_json()).
@pytest.mark.xfail(reason="SCYLLADB-5141")
def testFrozenCollectionsWithComplicatedInnerType(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, intval int,  listval frozen<list<tuple<text,text>>>, PRIMARY KEY (k))") as table:

        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE k IS NOT NULL AND listval IS NOT NULL PRIMARY KEY (k, listval)") as view:

            execute(cql, table, "INSERT INTO %s (k, intval, listval) VALUES (?, ?, from_json(?))",
                    0,
                    0,
                    "[[\"a\",\"1\"], [\"b\",\"2\"], [\"c\",\"3\"]]")

            # verify input
            assert_rows(execute(cql, table, "SELECT k, listval FROM %s WHERE k = ?", 0),
                        row(0, [("a", "1"), ("b", "2"), ("c", "3")]))
            assert_rows(execute(cql, view, "SELECT k, listval from %s"),
                        row(0, [("a", "1"), ("b", "2"), ("c", "3")]))

            # update listval with the same value and it will be compared in view generator
            execute(cql, table, "INSERT INTO %s (k, listval) VALUES (?, from_json(?))",
                    0,
                    "[[\"a\",\"1\"], [\"b\",\"2\"], [\"c\",\"3\"]]")
            # verify result
            assert_rows(execute(cql, table, "SELECT k, listval FROM %s WHERE k = ?", 0),
                        row(0, [("a", "1"), ("b", "2"), ("c", "3")]))
            assert_rows(execute(cql, view, "SELECT k, listval from %s"),
                        row(0, [("a", "1"), ("b", "2"), ("c", "3")]))

def testUpdate(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "k int, " +
                      "intval int, " +
                      "PRIMARY KEY (k))") as table:

        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE k IS NOT NULL AND intval IS NOT NULL PRIMARY KEY (intval, k)") as view:

            execute(cql, table, "INSERT INTO %s (k, intval) VALUES (?, ?)", 0, 0)
            assert_rows(execute(cql, table, "SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 0))
            assert_rows(execute(cql, view, "SELECT k, intval from %s WHERE intval = ?", 0), row(0, 0))

            execute(cql, table, "INSERT INTO %s (k, intval) VALUES (?, ?)", 0, 1)
            assert_rows(execute(cql, table, "SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 1))
            assert_rows(execute(cql, view, "SELECT k, intval from %s WHERE intval = ?", 1), row(0, 1))

def testIgnoreUpdate(cql, test_keyspace):
    # regression test for CASSANDRA-10614

    with create_table(cql, test_keyspace, "(" +
                      "a int, " +
                      "b int, " +
                      "c int, " +
                      "d int, " +
                      "PRIMARY KEY (a, b))") as table:

        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT a, b, c FROM %s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b, a)") as view:

            execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0)
            assert_rows(execute(cql, view, "SELECT a, b, c from %s WHERE b = ?", 0), row(0, 0, 0))

            execute(cql, table, "UPDATE %s SET d = ? WHERE a = ? AND b = ?", 0, 0, 0)
            assert_rows(execute(cql, view, "SELECT a, b, c from %s WHERE b = ?", 0), row(0, 0, 0))

            # Note: errors here may result in the test hanging when the memtables are flushed as part of the table drop,
            # because empty rows in the memtable will cause the flush to fail.  This will result in a test timeout that
            # should not be ignored.
            execute(cql, table, "BEGIN BATCH " +
                    "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?); " + # should be accepted
                    "UPDATE %s SET d = ? WHERE a = ? AND b = ?; " +  # should be accepted
                    "APPLY BATCH",
                    0, 0, 0, 0,
                    1, 0, 1)
            assert_rows(execute(cql, view, "SELECT a, b, c from %s WHERE b = ?", 0), row(0, 0, 0))
            assert_rows(execute(cql, view, "SELECT a, b, c from %s WHERE b = ?", 1), row(0, 1, None))

            # The Java test then flushes the view and checks the number of
            # its sstables through Cassandra's internal APIs. We only flush.
            flush(cql, view)

def testrowDeletionTest(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "a int," +
                      "b int," +
                      "c int," +
                      "d int," +
                      "PRIMARY KEY (a, b))") as table:

        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE c IS NOT NULL AND a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (c, a, b)") as view:

            cql.execute("DELETE FROM " + table + " USING TIMESTAMP 6 WHERE a = 1 AND b = 1;")
            execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?) USING TIMESTAMP 3", 1, 1, 1, 1)
            assert_row_count(execute(cql, view, "SELECT * FROM %s WHERE c = 1 AND a = 1 AND b = 1"), 0)

def testMultipleDeletes(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "a int," +
                      "b int," +
                      "PRIMARY KEY (a, b))") as table:

        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b, a)") as view:

            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", 1, 1)
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", 1, 2)
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", 1, 3)

            mvRows = execute(cql, view, "SELECT a, b FROM %s")
            assert_rows_ignoring_order(mvRows, row(1, 1), row(1, 2), row(1, 3))

            cql.execute(("BEGIN UNLOGGED BATCH " +
                         "DELETE FROM {} WHERE a = 1 AND b > 1 AND b < 3;" +
                         "DELETE FROM {} WHERE a = 1;" +
                         "APPLY BATCH").format(table, table))

            mvRows = execute(cql, view, "SELECT a, b FROM %s")
            assert_empty(mvRows)

def testCollectionInView(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "a int," +
                      "b int," +
                      "c map<int, text>," +
                      "PRIMARY KEY (a))") as table:

        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT a, b FROM %s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b, a)") as view:

            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", 0, 0)
            mvRows = execute(cql, view, "SELECT a, b FROM %s WHERE b = ?", 0)
            assert_rows(mvRows, row(0, 0))

            execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 1, {1: "1"})
            mvRows = execute(cql, view, "SELECT a, b FROM %s WHERE b = ?", 1)
            assert_rows(mvRows, row(1, 1))

            execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, {0: "0"})
            mvRows = execute(cql, view, "SELECT a, b FROM %s WHERE b = ?", 0)
            assert_rows(mvRows, row(0, 0))

def testReservedKeywordsInMV(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(\"token\" int PRIMARY KEY, \"keyspace\" int)") as table:

        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS" +
                                     "  SELECT \"keyspace\", \"token\"" +
                                     "  FROM %s" +
                                     "  WHERE \"keyspace\" IS NOT NULL AND \"token\" IS NOT NULL" +
                                     "  PRIMARY KEY (\"keyspace\", \"token\")") as view:

            execute(cql, table, "INSERT INTO %s (\"token\", \"keyspace\") VALUES (?, ?)", 0, 1)

            assert_rows(execute(cql, table, "SELECT * FROM %s"), row(0, 1))
            assert_rows(execute(cql, view, "SELECT * FROM %s"), row(1, 0))

# The Java test repeats the following with 1, 2, 4 and 8 concurrent view
# builders, and with one compaction thread, which it sets through Cassandra's
# internal APIs. We can't set these through CQL, so we run it just once.
def testViewBuilderResume(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "k int, " +
                      "c int, " +
                      "val text, " +
                      "PRIMARY KEY(k,c))") as table:

        # The Java test runs these INSERTs one after another. To make the
        # test faster, we run them concurrently.
        stmt = cql.prepare(f"INSERT into {table} (k,c,val)VALUES(?,?,?)")
        with nodetool.no_autocompaction_context(cql, table):
            for _ in range(4):
                execute_concurrent_with_args(cql, stmt, [(i, i, str(i)) for i in range(1024)], concurrency=100, raise_on_first_error=True)
                flush(cql, table)

        # Leaving no_autocompaction_context re-enabled autocompaction, which
        # can now compact the four sstables while the views are built.
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                     "WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val,k,c)", wait=False) as mv1:

            #Force a second MV on the same base table, which will restart the first MV builder...
            with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT val, k, c FROM %s " +
                                         "WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val,k,c)"):

                wait_for_view_built(cql, mv1)

                assert_rows(execute(cql, mv1, "SELECT count(*) FROM %s"), row(1024))

# Scylla stores "IS NOT NULL" in the where_clause of system_schema.views as
# "IS NOT null". This is valid CQL, so we accept it.
def where_clause(cql, view):
    ks, name = view.split('.')
    return [(r.where_clause.replace("IS NOT null", "IS NOT NULL"),) for r in
            cql.execute("SELECT where_clause FROM system_schema.views WHERE keyspace_name = %s AND view_name = %s", [ks, name])]

def testQuotedIdentifiersInWhereClause(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(\"theKey\" int, \"theClustering_1\" int, \"theClustering_2\" int, \"theValue\" int, PRIMARY KEY (\"theKey\", \"theClustering_1\", \"theClustering_2\"))") as table:

        # The Java test's views also have the restriction
        # '"theValue" IS NOT NULL'. Cassandra silently ignores an IS NOT NULL
        # restriction on a column outside the view's primary key (it only
        # stores it in the where_clause checked below), but Scylla
        # deliberately rejects it (see #10365, and
        # test_materialized_view.py::test_is_not_null_forbidden_in_filter), so
        # we removed it from the views and from the expected where_clause.
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE \"theKey\" IS NOT NULL AND \"theClustering_1\" IS NOT NULL AND \"theClustering_2\" IS NOT NULL  PRIMARY KEY (\"theKey\", \"theClustering_1\", \"theClustering_2\");") as mv1, \
             create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE \"theKey\" IS NOT NULL AND (\"theClustering_1\", \"theClustering_2\") = (1, 2)  PRIMARY KEY (\"theKey\", \"theClustering_1\", \"theClustering_2\");") as mv2:

            # The Java test reads all the views in system_schema.views,
            # because its keyspace has only these two views.
            assert_rows(where_clause(cql, mv1),
                        row("\"theKey\" IS NOT NULL AND \"theClustering_1\" IS NOT NULL AND \"theClustering_2\" IS NOT NULL"))
            assert_rows(where_clause(cql, mv2),
                        row("\"theKey\" IS NOT NULL AND (\"theClustering_1\", \"theClustering_2\") = (1, 2)"))

def testemptyViewNameTest(cql, test_keyspace):
    with pytest.raises(SyntaxException):
        cql.execute("CREATE MATERIALIZED VIEW " + test_keyspace + ".\"\" AS SELECT a, b FROM " + test_keyspace + ".tbl WHERE b IS NOT NULL PRIMARY KEY (b, a)")

def testemptyBaseTableNameTest(cql, test_keyspace):
    with pytest.raises(SyntaxException):
        cql.execute("CREATE MATERIALIZED VIEW " + test_keyspace + ".myview AS SELECT a, b FROM " + test_keyspace + ".\"\" WHERE b IS NOT NULL PRIMARY KEY (b, a)")

# Reproduces SCYLLADB-5198 (Scylla doesn't quote case-sensitive function
# names, like "FUN", in the where_clause it stores for a view, so it later
# calls the wrong function) and #13746 (a user-defined function in a WHERE
# clause can't be executed - here, every write to the base table fails).
@pytest.mark.skip_bug(
    link="https://github.com/scylladb/scylladb/issues/13746",
    reason="UDF can only be used in SELECT, and abort when used in WHERE, or in INSERT/UPDATE/DELETE commands",
)
def testFunctionInWhereClause(cql, test_keyspace):
    # The Java test runs "USE keyspace" so that the views can call the
    # functions it creates in this keyspace without the keyspace name. We
    # do this on a separate connection, so as not to affect other tests.
    with new_cql(cql) as session:
        session.execute("USE " + test_keyspace)

        # Native token function with lowercase, should be unquoted in the schema where clause
        assert_empty(functionInWhereClause(session, test_keyspace, "(k bigint PRIMARY KEY, v int)",
                                           None,
                                           "CREATE MATERIALIZED VIEW %s AS" +
                                           "   SELECT * FROM %s WHERE k = token(1) AND v IS NOT NULL " +
                                           "   PRIMARY KEY (v, k)",
                                           "k = token(1) AND v IS NOT NULL",
                                           "INSERT INTO %s(k, v) VALUES (0, 1)",
                                           "INSERT INTO %s(k, v) VALUES (2, 3)"))

        # Native token function with uppercase, should be unquoted and lowercased in the schema where clause
        assert_empty(functionInWhereClause(session, test_keyspace, "(k bigint PRIMARY KEY, v int)",
                                           None,
                                           "CREATE MATERIALIZED VIEW %s AS" +
                                           "   SELECT * FROM %s WHERE k = TOKEN(1) AND v IS NOT NULL" +
                                           "   PRIMARY KEY (v, k)",
                                           "k = token(1) AND v IS NOT NULL",
                                           "INSERT INTO %s(k, v) VALUES (0, 1)",
                                           "INSERT INTO %s(k, v) VALUES (2, 3)"))

        # UDF with lowercase name, shouldn't be quoted in the schema where clause
        assert_rows(functionInWhereClause(session, test_keyspace, "(k int PRIMARY KEY, v int)",
                                          ("fun", "()" +
                                           "   CALLED ON NULL INPUT" +
                                           "   RETURNS int " + java_or_lua(cql, "return 2;", "return 2")),
                                          "CREATE MATERIALIZED VIEW %s AS " +
                                          "   SELECT * FROM %s WHERE k = fun() AND v IS NOT NULL" +
                                          "   PRIMARY KEY (v, k)",
                                          "k = fun() AND v IS NOT NULL",
                                          "INSERT INTO %s(k, v) VALUES (0, 1)",
                                          "INSERT INTO %s(k, v) VALUES (2, 3)"), row(3, 2))

        # UDF with uppercase name, should be quoted in the schema where clause
        assert_rows(functionInWhereClause(session, test_keyspace, "(k int PRIMARY KEY, v int)",
                                          ("\"FUN\"", "()" +
                                           "   CALLED ON NULL INPUT" +
                                           "   RETURNS int" +
                                           "   " + java_or_lua(cql, "return 2;", "return 2")),
                                          "CREATE MATERIALIZED VIEW %s AS " +
                                          "   SELECT * FROM %s WHERE k = \"FUN\"() AND v IS NOT NULL" +
                                          "   PRIMARY KEY (v, k)",
                                          "k = \"FUN\"() AND v IS NOT NULL",
                                          "INSERT INTO %s(k, v) VALUES (0, 1)",
                                          "INSERT INTO %s(k, v) VALUES (2, 3)"), row(3, 2))

        # UDF with uppercase name conflicting with TOKEN keyword but not with native token function name,
        # should be quoted in the schema where clause
        assert_rows(functionInWhereClause(session, test_keyspace, "(k int PRIMARY KEY, v int)",
                                          ("\"TOKEN\"", "(x int)" +
                                           "   CALLED ON NULL INPUT" +
                                           "   RETURNS int" +
                                           "   " + java_or_lua(cql, "return x;", "return x")),
                                          "CREATE MATERIALIZED VIEW %s AS" +
                                          "   SELECT * FROM %s WHERE k = \"TOKEN\"(2) AND v IS NOT NULL" +
                                          "   PRIMARY KEY (v, k)",
                                          "k = \"TOKEN\"(2) AND v IS NOT NULL",
                                          "INSERT INTO %s(k, v) VALUES (0, 1)",
                                          "INSERT INTO %s(k, v) VALUES (2, 3)"), row(3, 2))

        # UDF with lowercase name conflicting with both TOKEN keyword and native token function name,
        # requires specifying the keyspace and should be quoted in the schema where clause
        assert_rows(functionInWhereClause(session, test_keyspace, "(k int PRIMARY KEY, v int)",
                                          ("\"token\"", "(x int)" +
                                           "   CALLED ON NULL INPUT" +
                                           "   RETURNS int" +
                                           "   " + java_or_lua(cql, "return x;", "return x")),
                                          "CREATE MATERIALIZED VIEW %s AS" +
                                          "   SELECT * FROM %s " +
                                          "   WHERE k = " + test_keyspace + ".\"token\"(2) AND v IS NOT NULL" +
                                          "   PRIMARY KEY (v, k)",
                                          "k = " + test_keyspace + ".\"token\"(2) AND v IS NOT NULL",
                                          "INSERT INTO %s(k, v) VALUES (0, 1)",
                                          "INSERT INTO %s(k, v) VALUES (2, 3)"), row(3, 2))

# The function, if given, is a pair of the function's name and the rest of
# its definition, so that we can drop it at the end.
def functionInWhereClause(session, keyspace, createTableQuery,
                          function,
                          createViewQuery,
                          expectedSchemaWhereClause,
                          *insertQueries):
    with create_table(session, keyspace, createTableQuery) as table, \
         ExitStack() as stack:

        if function is not None:
            functionName, definition = function
            session.execute("CREATE FUNCTION " + functionName + definition)
            stack.callback(lambda: session.execute("DROP FUNCTION " + functionName))

        with create_view(session, table, createViewQuery) as view:

            # Test the where clause stored in system_schema.views
            assert_rows(where_clause(session, view), row(expectedSchemaWhereClause))

            for insert in insertQueries:
                execute(session, table, insert)

            return list(execute(session, view, "SELECT * FROM %s"))
