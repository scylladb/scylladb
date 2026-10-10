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
from contextlib import ExitStack

# The original Java test class is parameterized to run each test with each of
# the CQL protocol versions which Cassandra supports. We use only the Python
# driver's default protocol version.

# The Java test sets Cassandra's "cassandra.mv.allow_filtering_nonkey_columns_unsafe"
# system property, to allow views whose WHERE clause filters on columns that
# are not in the base table's primary key. Only the tests which Cassandra
# marks @Ignore, testViewFiltering and testMVFilteringWithComplexColumn, need
# it, and we mark them cassandra_bug, so we don't need to set it. Scylla
# doesn't allow such views at all, see issue #4250.

def flushIf(cql, keyspace, flush):
    if flush:
        nodetool.flush_keyspace(cql, keyspace)

# TODO will revise the non-pk filter condition in MV, see CASSANDRA-11500
# (The Java test is marked @Ignore.)
# Reproduces #4250 (views with a filter on non-key columns).
@pytest.mark.xfail(reason="#4250")
def testViewFilteringWithFlush(cql, test_keyspace, cassandra_bug):
    viewFiltering(cql, test_keyspace, True)

# TODO will revise the non-pk filter condition in MV, see CASSANDRA-11500
# Reproduces #4250 (views with a filter on non-key columns).
@pytest.mark.xfail(reason="#4250")
def testViewFilteringWithoutFlush(cql, test_keyspace, cassandra_bug):
    viewFiltering(cql, test_keyspace, False)

def viewFiltering(cql, test_keyspace, flush):
    # CASSANDRA-13547: able to shadow entire view row if base column used in filter condition is modified
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, PRIMARY KEY (a))") as table, \
         ExitStack() as stack:
        mvs = [stack.enter_context(create_view(cql, table, q, wait=False)) for q in [
            "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
            "WHERE a IS NOT NULL AND b IS NOT NULL and c = 1  PRIMARY KEY (a, b)",
            "CREATE MATERIALIZED VIEW %s AS SELECT c, d FROM %s " +
            "WHERE a IS NOT NULL AND b IS NOT NULL and c = 1 and d = 1 PRIMARY KEY (a, b)",
            "CREATE MATERIALIZED VIEW %s AS SELECT a, b, c, d FROM %s " +
            "WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (a, b)",
            "CREATE MATERIALIZED VIEW %s AS SELECT c FROM %s " +
            "WHERE a IS NOT NULL AND b IS NOT NULL and c = 1 PRIMARY KEY (a, b)",
            "CREATE MATERIALIZED VIEW %s AS SELECT c FROM %s " +
            "WHERE a IS NOT NULL and d = 1 PRIMARY KEY (a, d)",
            "CREATE MATERIALIZED VIEW %s AS SELECT c FROM %s " +
            "WHERE a = 1 and d IS NOT NULL PRIMARY KEY (a, d)"]]
        for mv in mvs:
            wait_for_view_built(cql, mv)
        mv1, mv2, mv3, mv4, mv5, mv6 = mvs
        stack.enter_context(nodetool.no_autocompaction_context(cql, *mvs))

        def check(*expected):
            # expected[i] is the rows expected in mv(i+1), or None for no rows
            for mv, rows in zip(mvs, expected):
                if rows is None:
                    assert_row_count(cql.execute("SELECT * FROM " + mv), 0)
                else:
                    assert_rows_ignoring_order(cql.execute("SELECT * FROM " + mv), rows)

        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?) using timestamp 0", 1, 1, 1, 1)
        flushIf(cql, test_keyspace, flush)

        # views should be updated.
        check(row(1, 1, 1, 1), row(1, 1, 1, 1), row(1, 1, 1, 1), row(1, 1, 1), row(1, 1, 1), row(1, 1, 1))

        execute(cql, table, "UPDATE %s using timestamp 1 set c = ? WHERE a=?", 0, 1)
        flushIf(cql, test_keyspace, flush)

        check(None, None, row(1, 1, 0, 1), None, row(1, 1, 0), row(1, 1, 0))

        execute(cql, table, "UPDATE %s using timestamp 2 set c = ? WHERE a=?", 1, 1)
        flushIf(cql, test_keyspace, flush)

        # row should be back in views.
        check(row(1, 1, 1, 1), row(1, 1, 1, 1), row(1, 1, 1, 1), row(1, 1, 1), row(1, 1, 1), row(1, 1, 1))

        execute(cql, table, "UPDATE %s using timestamp 3 set d = ? WHERE a=?", 0, 1)
        flushIf(cql, test_keyspace, flush)

        check(row(1, 1, 1, 0), None, row(1, 1, 1, 0), row(1, 1, 1), None, row(1, 0, 1))

        execute(cql, table, "UPDATE %s using timestamp 4 set c = ? WHERE a=?", 0, 1)
        flushIf(cql, test_keyspace, flush)

        check(None, None, row(1, 1, 0, 0), None, None, row(1, 0, 0))

        execute(cql, table, "UPDATE %s using timestamp 5 set d = ? WHERE a=?", 1, 1)
        flushIf(cql, test_keyspace, flush)

        # should not update as c=0
        check(None, None, row(1, 1, 0, 1), None, row(1, 1, 0), row(1, 1, 0))

        execute(cql, table, "UPDATE %s using timestamp 6 set c = ? WHERE a=?", 1, 1)

        # row should be back in views.
        check(row(1, 1, 1, 1), row(1, 1, 1, 1), row(1, 1, 1, 1), row(1, 1, 1), row(1, 1, 1), row(1, 1, 1))

        execute(cql, table, "UPDATE %s using timestamp 7 set b = ? WHERE a=?", 2, 1)
        if flush:
            nodetool.flush_keyspace(cql, test_keyspace)
            for view in mvs:
                nodetool.compact(cql, view)
        # row should be back in views.
        check(row(1, 2, 1, 1), row(1, 2, 1, 1), row(1, 2, 1, 1), row(1, 2, 1), row(1, 1, 1), row(1, 1, 1))

        execute(cql, table, "DELETE b, c FROM %s using timestamp 6 WHERE a=?", 1)
        flushIf(cql, test_keyspace, flush)

        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s"), row(1, 2, None, 1))
        check(None, None, row(1, 2, None, 1), None, row(1, 1, None), row(1, 1, None))

        execute(cql, table, "DELETE FROM %s using timestamp 8 where a=?", 1)
        flushIf(cql, test_keyspace, flush)

        check(None, None, None, None, None, None)

        execute(cql, table, "UPDATE %s using timestamp 9 set b = ?,c = ? where a=?", 1, 1, 1) # upsert
        flushIf(cql, test_keyspace, flush)

        check(row(1, 1, 1, None), None, row(1, 1, 1, None), row(1, 1, 1), None, None)

        execute(cql, table, "DELETE FROM %s using timestamp 10 where a=?", 1)
        flushIf(cql, test_keyspace, flush)

        check(None, None, None, None, None, None)

        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?) using timestamp 11", 1, 1, 1, 1)
        flushIf(cql, test_keyspace, flush)

        # row should be back in views.
        check(row(1, 1, 1, 1), row(1, 1, 1, 1), row(1, 1, 1, 1), row(1, 1, 1), row(1, 1, 1), row(1, 1, 1))

        execute(cql, table, "DELETE FROM %s using timestamp 12 where a=?", 1)
        flushIf(cql, test_keyspace, flush)

        check(None, None, None, None, None, None)

# TODO will revise the non-pk filter condition in MV, see CASSANDRA-11500
# Reproduces #4250 (views with a filter on non-key columns) and SCYLLADB-5192
# (a parenthesized value, here "contains (1)", is treated as a tuple).
@pytest.mark.xfail(reason="#4250, SCYLLADB-5192")
def testMVFilteringWithComplexColumn(cql, test_keyspace, cassandra_bug):
    with create_table(cql, test_keyspace, "(a int, b int, c int, l list<int>, s set<int>, m map<int,int>, PRIMARY KEY (a, b))") as table, \
         ExitStack() as stack:
        mvs = [stack.enter_context(create_view(cql, table, q, wait=False)) for q in [
            "CREATE MATERIALIZED VIEW %s AS SELECT a,b,c FROM %s " +
            "WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL AND l contains (1) " +
            "AND s contains (1) AND m contains key (1) " +
            "PRIMARY KEY (a, b, c)",
            "CREATE MATERIALIZED VIEW %s AS SELECT a,b FROM %s " +
            "WHERE a IS NOT NULL and b IS NOT NULL AND l contains (1) " +
            "PRIMARY KEY (a, b)",
            "CREATE MATERIALIZED VIEW %s AS SELECT a,b FROM %s " +
            "WHERE a IS NOT NULL AND b IS NOT NULL AND s contains (1) " +
            "PRIMARY KEY (a, b)",
            "CREATE MATERIALIZED VIEW %s AS SELECT a,b FROM %s " +
            "WHERE a IS NOT NULL AND b IS NOT NULL AND m contains key (1) " +
            "PRIMARY KEY (a, b)"]]
        for mv in mvs:
            wait_for_view_built(cql, mv)
        mv1, mv2, mv3, mv4 = mvs

        # not able to drop base column filtered in view
        assert_invalid_message(cql, table, "Cannot drop column l, depended on by materialized views", "ALTER TABLE %s DROP l")
        assert_invalid_message(cql, table, "Cannot drop column s, depended on by materialized views", "ALTER TABLE %s DROP s")
        assert_invalid_message(cql, table, "Cannot drop column m, depended on by materialized views", "ALTER TABLE %s DROP m")

        stack.enter_context(nodetool.no_autocompaction_context(cql, *mvs))

        execute(cql, table, "INSERT INTO %s (a, b, c, l, s, m) VALUES (?, ?, ?, ?, ?, ?) ",
                1,
                1,
                1,
                [1, 1, 2],
                {1, 2},
                {1: 1, 2: 2})
        nodetool.flush_keyspace(cql, test_keyspace)

        assert_rows_ignoring_order(cql.execute("SELECT * FROM " + mv1), row(1, 1, 1))
        assert_rows_ignoring_order(cql.execute("SELECT * FROM " + mv2), row(1, 1))
        assert_rows_ignoring_order(cql.execute("SELECT * FROM " + mv3), row(1, 1))
        assert_rows_ignoring_order(cql.execute("SELECT * FROM " + mv4), row(1, 1))

        execute(cql, table, "UPDATE %s SET l=l-[1] WHERE a = 1 AND b = 1")
        nodetool.flush_keyspace(cql, test_keyspace)

        assert_empty(cql.execute("SELECT * FROM " + mv1))
        assert_empty(cql.execute("SELECT * FROM " + mv2))
        assert_rows_ignoring_order(cql.execute("SELECT * FROM " + mv3), row(1, 1))
        assert_rows_ignoring_order(cql.execute("SELECT * FROM " + mv4), row(1, 1))

        execute(cql, table, "UPDATE %s SET s=s-{2}, m=m-{2} WHERE a = 1 AND b = 1")
        nodetool.flush_keyspace(cql, test_keyspace)

        assert_rows_ignoring_order(execute(cql, table, "SELECT a,b,c FROM %s"), row(1, 1, 1))
        assert_empty(cql.execute("SELECT * FROM " + mv1))
        assert_empty(cql.execute("SELECT * FROM " + mv2))
        assert_rows_ignoring_order(cql.execute("SELECT * FROM " + mv3), row(1, 1))
        assert_rows_ignoring_order(cql.execute("SELECT * FROM " + mv4), row(1, 1))

        execute(cql, table, "UPDATE %s SET  m=m-{1} WHERE a = 1 AND b = 1")
        nodetool.flush_keyspace(cql, test_keyspace)

        assert_rows_ignoring_order(execute(cql, table, "SELECT a,b,c FROM %s"), row(1, 1, 1))
        assert_empty(cql.execute("SELECT * FROM " + mv1))
        assert_empty(cql.execute("SELECT * FROM " + mv2))
        assert_rows_ignoring_order(cql.execute("SELECT * FROM " + mv3), row(1, 1))
        assert_empty(cql.execute("SELECT * FROM " + mv4))

        # filter conditions result not changed
        execute(cql, table, "UPDATE %s SET  l=l+[2], s=s-{0}, m=m+{3:3} WHERE a = 1 AND b = 1")
        nodetool.flush_keyspace(cql, test_keyspace)

        assert_rows_ignoring_order(execute(cql, table, "SELECT a,b,c FROM %s"), row(1, 1, 1))
        assert_empty(cql.execute("SELECT * FROM " + mv1))
        assert_empty(cql.execute("SELECT * FROM " + mv2))
        assert_rows_ignoring_order(cql.execute("SELECT * FROM " + mv3), row(1, 1))
        assert_empty(cql.execute("SELECT * FROM " + mv4))

def mvCreationSelectRestrictions(cql, test_keyspace, goodStatements):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, e int, PRIMARY KEY((a, b), c, d))") as table:

        # IS NOT NULL is required on all PK statements that are not otherwise restricted
        badStatements = [
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE b IS NOT NULL AND c IS NOT NULL AND d is NOT NULL PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a IS NOT NULL AND c IS NOT NULL AND d is NOT NULL PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a IS NOT NULL AND b IS NOT NULL AND d is NOT NULL PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a IS NOT NULL AND b IS NOT NULL AND c is NOT NULL PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a = ? AND b IS NOT NULL AND c is NOT NULL PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a = blob_as_int(?) AND b IS NOT NULL AND c is NOT NULL PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s PRIMARY KEY (a, b, c, d)"
        ]

        for badStatement in badStatements:
            with pytest.raises(InvalidRequest):
                with create_view(cql, table, badStatement):
                    pass

        # To make the test faster, we create all the views first, and only
        # then wait for them to be built.
        with ExitStack() as stack:
            mvs = [stack.enter_context(create_view(cql, table, goodStatement, wait=False)) for goodStatement in goodStatements]
            for mv in mvs:
                wait_for_view_built(cql, mv)
                cql.execute("ALTER MATERIALIZED VIEW " + mv + " WITH compaction = { 'class' : 'LeveledCompactionStrategy' }")

# The Java test has one more statement in goodStatements, using the
# blob_as_int() function, which we moved to a separate test below.
def testMVCreationSelectRestrictions(cql, test_keyspace):
    mvCreationSelectRestrictions(cql, test_keyspace, [
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a = 1 AND b = 1 AND c IS NOT NULL AND d is NOT NULL PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a IS NOT NULL AND b IS NOT NULL AND c = 1 AND d IS NOT NULL PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a IS NOT NULL AND b IS NOT NULL AND c = 1 AND d = 1 PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a = 1 AND b = 1 AND c = 1 AND d = 1 PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a = 1 AND b = 1 AND c > 1 AND d IS NOT NULL PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a = 1 AND b = 1 AND c = 1 AND d IN (1, 2, 3) PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a = 1 AND b = 1 AND (c, d) = (1, 1) PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a = 1 AND b = 1 AND (c, d) > (1, 1) PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a = 1 AND b = 1 AND (c, d) IN ((1, 1), (2, 2)) PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a = (int) 1 AND b = 1 AND c = 1 AND d = 1 PRIMARY KEY ((a, b), c, d)",
    ])

# The last statement of goodStatements in the Java testMVCreationSelectRestrictions
# Reproduces SCYLLADB-5141 (snake_case names of native functions).
@pytest.mark.xfail(reason="SCYLLADB-5141")
def testMVCreationSelectRestrictionsWithFunction(cql, test_keyspace):
    mvCreationSelectRestrictions(cql, test_keyspace, [
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a = blob_as_int(int_as_blob(1)) AND b = 1 AND c = 1 AND d = 1 PRIMARY KEY ((a, b), c, d)"
    ])

def testCaseSensitivity(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(\"theKey\" int, \"theClustering\" int, \"the\"\"Value\" int, PRIMARY KEY (\"theKey\", \"theClustering\"))") as table:
        execute(cql, table, "INSERT INTO %s (\"theKey\", \"theClustering\", \"the\"\"Value\") VALUES (?, ?, ?)", 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (\"theKey\", \"theClustering\", \"the\"\"Value\") VALUES (?, ?, ?)", 0, 1, 0)
        execute(cql, table, "INSERT INTO %s (\"theKey\", \"theClustering\", \"the\"\"Value\") VALUES (?, ?, ?)", 1, 0, 0)
        execute(cql, table, "INSERT INTO %s (\"theKey\", \"theClustering\", \"the\"\"Value\") VALUES (?, ?, ?)", 1, 1, 0)

        # The Java test's views also have the restriction
        # 'AND "the""Value" IS NOT NULL'. Cassandra silently ignores an IS NOT
        # NULL restriction on a column outside the view's primary key, but
        # Scylla deliberately rejects it (see #10365, and
        # test_materialized_view.py::test_is_not_null_forbidden_in_filter), so
        # we removed it - this doesn't change the test on Cassandra.
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                     "WHERE \"theKey\" = 1 AND \"theClustering\" = 1 " +
                                     "PRIMARY KEY (\"theKey\", \"theClustering\")") as mv1, \
             create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT \"theKey\", \"theClustering\", \"the\"\"Value\" FROM %s " +
                                     "WHERE \"theKey\" = 1 AND \"theClustering\" = 1 " +
                                     "PRIMARY KEY (\"theKey\", \"theClustering\")") as mv2:

            for mvname in [mv1, mv2]:
                assert_rows_ignoring_order(cql.execute("SELECT \"theKey\", \"theClustering\", \"the\"\"Value\" FROM " + mvname),
                                           row(1, 1, 0))

            execute(cql, table, "ALTER TABLE %s RENAME \"theClustering\" TO \"Col\"")

            for mvname in [mv1, mv2]:
                assert_rows_ignoring_order(cql.execute("SELECT \"theKey\", \"Col\", \"the\"\"Value\" FROM " + mvname),
                                           row(1, 1, 0))

def filterTest(cql, test_keyspace, viewQuery):
    with create_table(cql, test_keyspace, "(a int, b int, c int, PRIMARY KEY (a, b))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 1, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 0, 2)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 1, 3)

        with create_view(cql, table, viewQuery) as view:
            assert_rows(execute(cql, view, "SELECT a, b, c FROM %s"),
                        row(1, 0, 2),
                        row(1, 1, 3))

            execute(cql, table, "ALTER TABLE %s RENAME a TO foo")

            assert_rows(execute(cql, view, "SELECT foo, b, c FROM %s"),
                        row(1, 0, 2),
                        row(1, 1, 3))

# Reproduces SCYLLADB-5141 (snake_case names of native functions).
@pytest.mark.xfail(reason="SCYLLADB-5141")
def testFilterWithFunction(cql, test_keyspace):
    filterTest(cql, test_keyspace, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                   "WHERE a = blob_as_int(int_as_blob(1)) AND b IS NOT NULL " +
                                   "PRIMARY KEY (a, b)")

def testFilterWithTypecast(cql, test_keyspace):
    filterTest(cql, test_keyspace, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                   "WHERE a = (int) 1 AND b IS NOT NULL " +
                                   "PRIMARY KEY (a, b)")
