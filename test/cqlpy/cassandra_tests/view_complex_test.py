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

# Translation of ViewAbstractParameterizedTest.updateViewWithFlush(). The
# Java updateView() waits for asynchronous view updates; on a single node
# both Scylla and Cassandra apply them synchronously, so we just execute().
def updateViewWithFlush(cql, table, query, flush, *args):
    execute(cql, table, query, *args)
    if flush:
        nodetool.flush_keyspace(cql, table.split('.')[0])

def testNonBaseColumnInViewPkWithFlush(cql, test_keyspace):
    nonBaseColumnInViewPk(cql, test_keyspace, True)

def testNonBaseColumnInViewPkWithoutFlush(cql, test_keyspace):
    nonBaseColumnInViewPk(cql, test_keyspace, False)

def nonBaseColumnInViewPk(cql, test_keyspace, flush):
    with create_table(cql, test_keyspace, "(p1 int, p2 int, v1 int, v2 int, primary key (p1,p2))") as table:
        with create_view(cql, table, "create materialized view %s as select * from %s " +
                                     "where p1 is not null and p2 is not null primary key (p2, p1) " +
                                     "with gc_grace_seconds=5") as view, \
             nodetool.no_autocompaction_context(cql, view):

            updateViewWithFlush(cql, table, "UPDATE %s USING TIMESTAMP 1 set v1 =1 where p1 = 1 AND p2 = 1;", flush)
            assert_rows_ignoring_order(execute(cql, table, "SELECT p1, p2, v1, v2 from %s"), row(1, 1, 1, None))
            assert_rows_ignoring_order(execute(cql, view, "SELECT p1, p2, v1, v2 from %s"), row(1, 1, 1, None))

            updateViewWithFlush(cql, table, "UPDATE %s USING TIMESTAMP 2 set v1 = null, v2 = 1 where p1 = 1 AND p2 = 1;", flush)
            assert_rows_ignoring_order(execute(cql, table, "SELECT p1, p2, v1, v2 from %s"), row(1, 1, None, 1))
            assert_rows_ignoring_order(execute(cql, view, "SELECT p1, p2, v1, v2 from %s"), row(1, 1, None, 1))

            updateViewWithFlush(cql, table, "UPDATE %s USING TIMESTAMP 2 set v2 = null where p1 = 1 AND p2 = 1;", flush)
            assert_empty(execute(cql, table, "SELECT p1, p2, v1, v2 from %s"))
            assert_empty(execute(cql, view, "SELECT p1, p2, v1, v2 from %s"))

            updateViewWithFlush(cql, table, "INSERT INTO %s (p1,p2) VALUES(1,1) USING TIMESTAMP 3;", flush)
            assert_rows_ignoring_order(execute(cql, table, "SELECT p1, p2, v1, v2 from %s"), row(1, 1, None, None))
            assert_rows_ignoring_order(execute(cql, view, "SELECT p1, p2, v1, v2 from %s"), row(1, 1, None, None))

            updateViewWithFlush(cql, table, "DELETE FROM %s USING TIMESTAMP 4 WHERE p1 =1 AND p2 = 1;", flush)
            assert_empty(execute(cql, table, "SELECT p1, p2, v1, v2 from %s"))
            assert_empty(execute(cql, view, "SELECT p1, p2, v1, v2 from %s"))

            updateViewWithFlush(cql, table, "UPDATE %s USING TIMESTAMP 5 set v2 = 1 where p1 = 1 AND p2 = 1;", flush)
            assert_rows_ignoring_order(execute(cql, table, "SELECT p1, p2, v1, v2 from %s"), row(1, 1, None, 1))
            assert_rows_ignoring_order(execute(cql, view, "SELECT p1, p2, v1, v2 from %s"), row(1, 1, None, 1))

def testMVWithDifferentColumnsWithFlush(cql, test_keyspace):
    MVWithDifferentColumns(cql, test_keyspace, True)

def testMVWithDifferentColumnsWithoutFlush(cql, test_keyspace):
    MVWithDifferentColumns(cql, test_keyspace, False)

def MVWithDifferentColumns(cql, test_keyspace, flush):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, e int, f int, PRIMARY KEY(a, b))") as table, \
         ExitStack() as stack:

        viewNames = []
        mvStatements = [
                        # all selected
                        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (a,b)",
                        # unselected e,f
                        "CREATE MATERIALIZED VIEW %s AS SELECT a,b,c,d FROM %s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (a,b)",
                        # no selected
                        "CREATE MATERIALIZED VIEW %s AS SELECT a,b FROM %s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (a,b)",
                        # all selected, re-order keys
                        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b,a)",
                        # unselected e,f, re-order keys
                        "CREATE MATERIALIZED VIEW %s AS SELECT a,b,c,d FROM %s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b,a)",
                        # no selected, re-order keys
                        "CREATE MATERIALIZED VIEW %s AS SELECT a,b FROM %s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b,a)"]

        for statement in mvStatements:
            name = stack.enter_context(create_view(cql, table, statement, wait=False))
            viewNames.append(name)
        # To make the test faster, we create all the views first, and only
        # then wait for them to be built.
        for name in viewNames:
            wait_for_view_built(cql, name)
        stack.enter_context(nodetool.no_autocompaction_context(cql, *viewNames))

        # insert
        updateViewWithFlush(cql, table, "INSERT INTO %s (a,b,c,d,e,f) VALUES(1,1,1,1,1,1) using timestamp 1", flush)
        assertBaseViews(cql, table, row(1, 1, 1, 1, 1, 1), viewNames)

        updateViewWithFlush(cql, table, "UPDATE %s using timestamp 2 SET c=0, d=0 WHERE a=1 AND b=1", flush)
        assertBaseViews(cql, table, row(1, 1, 0, 0, 1, 1), viewNames)

        updateViewWithFlush(cql, table, "UPDATE %s using timestamp 2 SET e=0, f=0 WHERE a=1 AND b=1", flush)
        assertBaseViews(cql, table, row(1, 1, 0, 0, 0, 0), viewNames)

        updateViewWithFlush(cql, table, "DELETE FROM %s using timestamp 2 WHERE a=1 AND b=1", flush)
        assertBaseViews(cql, table, None, viewNames)

        # partial update unselected, selected
        updateViewWithFlush(cql, table, "UPDATE %s using timestamp 3 SET f=1 WHERE a=1 AND b=1", flush)
        assertBaseViews(cql, table, row(1, 1, None, None, None, 1), viewNames)

        updateViewWithFlush(cql, table, "UPDATE %s using timestamp 4 SET e = 1, f=null WHERE a=1 AND b=1", flush)
        assertBaseViews(cql, table, row(1, 1, None, None, 1, None), viewNames)

        updateViewWithFlush(cql, table, "UPDATE %s using timestamp 4 SET e = null WHERE a=1 AND b=1", flush)
        assertBaseViews(cql, table, None, viewNames)

        updateViewWithFlush(cql, table, "UPDATE %s using timestamp 5 SET c = 1 WHERE a=1 AND b=1", flush)
        assertBaseViews(cql, table, row(1, 1, 1, None, None, None), viewNames)

        updateViewWithFlush(cql, table, "UPDATE %s using timestamp 5 SET c = null WHERE a=1 AND b=1", flush)
        assertBaseViews(cql, table, None, viewNames)

        updateViewWithFlush(cql, table, "UPDATE %s using timestamp 6 SET d = 1 WHERE a=1 AND b=1", flush)
        assertBaseViews(cql, table, row(1, 1, None, 1, None, None), viewNames)

        updateViewWithFlush(cql, table, "UPDATE %s using timestamp 7 SET d = null WHERE a=1 AND b=1", flush)
        assertBaseViews(cql, table, None, viewNames)

        updateViewWithFlush(cql, table, "UPDATE %s using timestamp 8 SET f = 1 WHERE a=1 AND b=1", flush)
        assertBaseViews(cql, table, row(1, 1, None, None, None, 1), viewNames)

        updateViewWithFlush(cql, table, "UPDATE %s using timestamp 6 SET c = 1 WHERE a=1 AND b=1", flush)
        assertBaseViews(cql, table, row(1, 1, 1, None, None, 1), viewNames)

        # view row still alive due to c=1@6
        updateViewWithFlush(cql, table, "UPDATE %s using timestamp 8 SET f = null WHERE a=1 AND b=1", flush)
        assertBaseViews(cql, table, row(1, 1, 1, None, None, None), viewNames)

        updateViewWithFlush(cql, table, "UPDATE %s using timestamp 6 SET c = null WHERE a=1 AND b=1", flush)
        assertBaseViews(cql, table, None, viewNames)

def assertBaseViews(cql, table, r, viewNames):
    result = list(execute(cql, table, "SELECT * FROM %s"))
    if r is None:
        assert_empty(result)
    else:
        assert_rows_ignoring_order(result, r)
    for viewName in viewNames:
        assertBaseView(result, list(execute(cql, viewName, "SELECT * FROM %s")), viewName)

def assertBaseView(baseData, viewData, mv):
    if len(baseData) != len(viewData):
        pytest.fail(f"Mismatch number of rows in view {mv}: <{viewData}>, in base <{baseData}>")
    if len(baseData) == 0:
        return
    if len(viewData) != 1:
        pytest.fail(f"Expect only one row in view {mv}, but got <{viewData}>")

    baseValues = baseData[0]._asdict()
    for name, viewValue in viewData[0]._asdict().items():
        if name not in baseValues:
            pytest.fail(f"Extra column: {name} with value {viewValue} in view")
        elif baseValues[name] != viewValue:
            pytest.fail(f"Non equal column: {name}, expected <{baseValues[name]}> but got <{viewValue}>")
