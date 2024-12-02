# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of Cassandra 4.1.1 (commit 8d91b469afd3fcafef7ef85c10c8acc11703ba2d)
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0

from cassandra_tests.porting import *
import random

# InsertUpdateIfConditionCollectionsTest class has been split into multiple ones because of timeout issues (CASSANDRA-16670)
# Any changes here check if they apply to the other classes
# - InsertUpdateIfConditionStaticsTest
# - InsertUpdateIfConditionCollectionsTest
# - InsertUpdateIfConditionTest
#
# Migrated from cql_tests.py:TestCQL.bug_6069_test()

# As explained in kb/lwt-differences.rst, Scylla is different from Cassandra in that
# it always returns the full version of the old row, even if the old row didn't exist
# (so it's all NULLs) or if the condition failed - but in those cases Cassandra only
# returns the success boolean and not the whole row.
@pytest.fixture(scope="module")
def is_scylla(cql):
    names = [row.table_name for row in cql.execute("SELECT * FROM system_schema.tables WHERE keyspace_name = 'system'")]
    yield any('scylla' in name for name in names)

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18066")]), "vnodes"],
                         indirect=True)
def testInsertSetIfNotExists(cql, test_keyspace, is_scylla):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, s set<int>)") as table:
        assertRows(execute(cql, table, "INSERT INTO %s (k, s) VALUES (0, {1, 2, 3}) IF NOT EXISTS"),
                   row(True,None,None) if is_scylla else row(True))
        assertRows(execute(cql, table, "SELECT * FROM %s "), row(0, {1, 2, 3}))

@pytest.mark.xfail(reason="Issue #13586")
def testWholeUDT(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(a int, b text)") as typename:
        for frozen in [False, True]:   
            f = f"frozen<{typename}>" if frozen else typename
            with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, v {f})") as table:
                v = user_type("a", 0, "b", "abc")
                execute(cql, table, "INSERT INTO %s (k, v) VALUES (0, ?)", v)

                checkAppliesUDT(cql, table, "v = {a: 0, b: 'abc'}", v)
                checkAppliesUDT(cql, table, "v != null", v)
                checkAppliesUDT(cql, table, "v != {a: 1, b: 'abc'}", v)
                checkAppliesUDT(cql, table, "v != {a: 0, b: 'def'}", v)
                checkAppliesUDT(cql, table, "v > {a: -1, b: 'abc'}", v)
                checkAppliesUDT(cql, table, "v > {a: 0, b: 'aaa'}", v)
                checkAppliesUDT(cql, table, "v > {a: 0}", v)
                checkAppliesUDT(cql, table, "v >= {a: 0, b: 'aaa'}", v)
                checkAppliesUDT(cql, table, "v >= {a: 0, b: 'abc'}", v)
                checkAppliesUDT(cql, table, "v < {a: 0, b: 'zzz'}", v)
                checkAppliesUDT(cql, table, "v < {a: 1, b: 'abc'}", v)
                checkAppliesUDT(cql, table, "v < {a: 1}", v)
                checkAppliesUDT(cql, table, "v <= {a: 0, b: 'zzz'}", v)
                checkAppliesUDT(cql, table, "v <= {a: 0, b: 'abc'}", v)
                checkAppliesUDT(cql, table, "v IN (null, {a: 0, b: 'abc'}, {a: 1})", v)

                # multiple conditions
                checkAppliesUDT(cql, table, "v > {a: -1, b: 'abc'} AND v > {a: 0}", v)
                checkAppliesUDT(cql, table, "v != null AND v IN ({a: 0, b: 'abc'})", v)

                # should not apply
                checkDoesNotApplyUDT(cql, table, "v = {a: 0, b: 'def'}", v)
                checkDoesNotApplyUDT(cql, table, "v = {a: 1, b: 'abc'}", v)
                checkDoesNotApplyUDT(cql, table, "v = null", v)
                checkDoesNotApplyUDT(cql, table, "v != {a: 0, b: 'abc'}", v)
                checkDoesNotApplyUDT(cql, table, "v > {a: 1, b: 'abc'}", v)
                checkDoesNotApplyUDT(cql, table, "v > {a: 0, b: 'zzz'}", v)
                checkDoesNotApplyUDT(cql, table, "v >= {a: 1, b: 'abc'}", v)
                checkDoesNotApplyUDT(cql, table, "v >= {a: 0, b: 'zzz'}", v)
                checkDoesNotApplyUDT(cql, table, "v < {a: -1, b: 'abc'}", v)
                checkDoesNotApplyUDT(cql, table, "v < {a: 0, b: 'aaa'}", v)
                checkDoesNotApplyUDT(cql, table, "v <= {a: -1, b: 'abc'}", v)
                checkDoesNotApplyUDT(cql, table, "v <= {a: 0, b: 'aaa'}", v)
                checkDoesNotApplyUDT(cql, table, "v IN ({a: 0}, {b: 'abc'}, {a: 0, b: 'def'}, null)", v)
                checkDoesNotApplyUDT(cql, table, "v IN ()", v)

                # multiple conditions
                checkDoesNotApplyUDT(cql, table, "v IN () AND v IN ({a: 0, b: 'abc'})", v)
                checkDoesNotApplyUDT(cql, table, "v > {a: 0, b: 'aaa'} AND v < {a: 0, b: 'aaa'}", v)

                # invalid conditions
                checkInvalidUDT(cql, table, "v = {a: 1, b: 'abc', c: 'foo'}", v, InvalidRequest)
                checkInvalidUDT(cql, table, "v = {foo: 'foo'}", v, InvalidRequest)
                checkInvalidUDT(cql, table, "v < {a: 1, b: 'abc', c: 'foo'}", v, InvalidRequest)
                checkInvalidUDT(cql, table, "v < null", v, InvalidRequest)
                checkInvalidUDT(cql, table, "v <= {a: 1, b: 'abc', c: 'foo'}", v, InvalidRequest)
                checkInvalidUDT(cql, table, "v <= null", v, InvalidRequest)
                checkInvalidUDT(cql, table, "v > {a: 1, b: 'abc', c: 'foo'}", v, InvalidRequest)
                checkInvalidUDT(cql, table, "v > null", v, InvalidRequest)
                checkInvalidUDT(cql, table, "v >= {a: 1, b: 'abc', c: 'foo'}", v, InvalidRequest)
                checkInvalidUDT(cql, table, "v >= null", v, InvalidRequest)
                checkInvalidUDT(cql, table, "v IN null", v, SyntaxException)
                checkInvalidUDT(cql, table, "v IN 367", v, SyntaxException)
                # Reproduces #13586:
                checkInvalidUDT(cql, table, "v CONTAINS KEY 123", v, InvalidRequest)
                checkInvalidUDT(cql, table, "v CONTAINS 'bar'", v, InvalidRequest)

                #/////////////////// null suffix on stored udt ////////////////////
                v = user_type("a", 0, "b", None)
                execute(cql, table, "INSERT INTO %s (k, v) VALUES (0, ?)", v)

                checkAppliesUDT(cql, table, "v = {a: 0}", v)
                checkAppliesUDT(cql, table, "v = {a: 0, b: null}", v)
                checkAppliesUDT(cql, table, "v != null", v)
                checkAppliesUDT(cql, table, "v != {a: 1, b: null}", v)
                checkAppliesUDT(cql, table, "v != {a: 1}", v)
                checkAppliesUDT(cql, table, "v != {a: 0, b: 'def'}", v)
                checkAppliesUDT(cql, table, "v > {a: -1, b: 'abc'}", v)
                checkAppliesUDT(cql, table, "v > {a: -1}", v)
                checkAppliesUDT(cql, table, "v >= {a: 0}", v)
                checkAppliesUDT(cql, table, "v >= {a: -1, b: 'abc'}", v)
                checkAppliesUDT(cql, table, "v < {a: 0, b: 'zzz'}", v)
                checkAppliesUDT(cql, table, "v < {a: 1, b: 'abc'}", v)
                checkAppliesUDT(cql, table, "v < {a: 1}", v)
                checkAppliesUDT(cql, table, "v <= {a: 0, b: 'zzz'}", v)
                checkAppliesUDT(cql, table, "v <= {a: 0}", v)
                checkAppliesUDT(cql, table, "v IN (null, {a: 0, b: 'abc'}, {a: 0})", v)

                # multiple conditions
                checkAppliesUDT(cql, table, "v > {a: -1, b: 'abc'} AND v >= {a: 0}", v)
                checkAppliesUDT(cql, table, "v != null AND v IN ({a: 0}, {a: 0, b: null})", v)

                # should not apply
                checkDoesNotApplyUDT(cql, table, "v = {a: 0, b: 'def'}", v)
                checkDoesNotApplyUDT(cql, table, "v = {a: 1}", v)
                checkDoesNotApplyUDT(cql, table, "v = {b: 'abc'}", v)
                checkDoesNotApplyUDT(cql, table, "v = null", v)
                checkDoesNotApplyUDT(cql, table, "v != {a: 0}", v)
                checkDoesNotApplyUDT(cql, table, "v != {a: 0, b: null}", v)
                checkDoesNotApplyUDT(cql, table, "v > {a: 1, b: 'abc'}", v)
                checkDoesNotApplyUDT(cql, table, "v > {a: 0}", v)
                checkDoesNotApplyUDT(cql, table, "v >= {a: 1, b: 'abc'}", v)
                checkDoesNotApplyUDT(cql, table, "v >= {a: 1}", v)
                checkDoesNotApplyUDT(cql, table, "v < {a: -1, b: 'abc'}", v)
                checkDoesNotApplyUDT(cql, table, "v < {a: -1}", v)
                checkDoesNotApplyUDT(cql, table, "v < {a: 0}", v)
                checkDoesNotApplyUDT(cql, table, "v <= {a: -1, b: 'abc'}", v)
                checkDoesNotApplyUDT(cql, table, "v <= {a: -1}", v)
                checkDoesNotApplyUDT(cql, table, "v IN ({a: 1}, {b: 'abc'}, {a: 0, b: 'def'}, null)", v)
                checkDoesNotApplyUDT(cql, table, "v IN ()", v)

                # multiple conditions
                checkDoesNotApplyUDT(cql, table, "v IN () AND v IN ({a: 0})", v)
                checkDoesNotApplyUDT(cql, table, "v > {a: -1} AND v < {a: 0}", v)

                #/////////////////// null prefix on stored udt ////////////////////
                v = user_type("a", None, "b", "abc")
                execute(cql, table, "INSERT INTO %s (k, v) VALUES (0, ?)", v)

                checkAppliesUDT(cql, table, "v = {a: null, b: 'abc'}", v)
                checkAppliesUDT(cql, table, "v = {b: 'abc'}", v)
                checkAppliesUDT(cql, table, "v != null", v)
                checkAppliesUDT(cql, table, "v != {a: 0, b: 'abc'}", v)
                checkAppliesUDT(cql, table, "v != {a: 0}", v)
                checkAppliesUDT(cql, table, "v != {b: 'def'}", v)
                checkAppliesUDT(cql, table, "v > {a: null, b: 'aaa'}", v)
                checkAppliesUDT(cql, table, "v > {b: 'aaa'}", v)
                checkAppliesUDT(cql, table, "v >= {a: null, b: 'aaa'}", v)
                checkAppliesUDT(cql, table, "v >= {b: 'abc'}", v)
                checkAppliesUDT(cql, table, "v < {a: null, b: 'zzz'}", v)
                checkAppliesUDT(cql, table, "v < {a: 0, b: 'abc'}", v)
                checkAppliesUDT(cql, table, "v < {a: 0}", v)
                checkAppliesUDT(cql, table, "v < {b: 'zzz'}", v)
                checkAppliesUDT(cql, table, "v <= {a: null, b: 'zzz'}", v)
                checkAppliesUDT(cql, table, "v <= {a: 0}", v)
                checkAppliesUDT(cql, table, "v <= {b: 'abc'}", v)
                checkAppliesUDT(cql, table, "v IN (null, {a: null, b: 'abc'}, {a: 0})", v)
                checkAppliesUDT(cql, table, "v IN (null, {a: 0, b: 'abc'}, {b: 'abc'})", v)

                # multiple conditions
                checkAppliesUDT(cql, table, "v > {b: 'aaa'} AND v >= {b: 'abc'}", v)
                checkAppliesUDT(cql, table, "v != null AND v IN ({a: 0}, {a: null, b: 'abc'})", v)

                # should not apply
                checkDoesNotApplyUDT(cql, table, "v = {a: 0, b: 'def'}", v)
                checkDoesNotApplyUDT(cql, table, "v = {a: 1}", v)
                checkDoesNotApplyUDT(cql, table, "v = {b: 'def'}", v)
                checkDoesNotApplyUDT(cql, table, "v = null", v)
                checkDoesNotApplyUDT(cql, table, "v != {b: 'abc'}", v)
                checkDoesNotApplyUDT(cql, table, "v != {a: null, b: 'abc'}", v)
                checkDoesNotApplyUDT(cql, table, "v > {a: 1, b: 'abc'}", v)
                checkDoesNotApplyUDT(cql, table, "v > {a: null, b: 'zzz'}", v)
                checkDoesNotApplyUDT(cql, table, "v > {b: 'zzz'}", v)
                checkDoesNotApplyUDT(cql, table, "v >= {a: null, b: 'zzz'}", v)
                checkDoesNotApplyUDT(cql, table, "v >= {a: 1}", v)
                checkDoesNotApplyUDT(cql, table, "v >= {b: 'zzz'}", v)
                checkDoesNotApplyUDT(cql, table, "v < {a: null, b: 'aaa'}", v)
                checkDoesNotApplyUDT(cql, table, "v < {b: 'aaa'}", v)
                checkDoesNotApplyUDT(cql, table, "v <= {a: null, b: 'aaa'}", v)
                checkDoesNotApplyUDT(cql, table, "v <= {b: 'aaa'}", v)
                checkDoesNotApplyUDT(cql, table, "v IN ({a: 1}, {a: 1, b: 'abc'}, {a: null, b: 'def'}, null)", v)
                checkDoesNotApplyUDT(cql, table, "v IN ()", v)

                # multiple conditions
                checkDoesNotApplyUDT(cql, table, "v IN () AND v IN ({b: 'abc'})", v)
                checkDoesNotApplyUDT(cql, table, "v IN () AND v IN ({a: null, b: 'abc'})", v)
                checkDoesNotApplyUDT(cql, table, "v > {a: -1} AND v < {a: 0}", v)

                #/////////////////// null udt ////////////////////
                v = null
                execute(cql, table, "INSERT INTO %s (k, v) VALUES (0, ?)", v)

                checkAppliesUDT(cql, table, "v = null", v)
                checkAppliesUDT(cql, table, "v IN (null, {a: null, b: 'abc'}, {a: 0})", v)
                checkAppliesUDT(cql, table, "v IN (null, {a: 0, b: 'abc'}, {b: 'abc'})", v)

                # multiple conditions
                checkAppliesUDT(cql, table, "v = null AND v IN (null, {a: 0}, {a: null, b: 'abc'})", v)

                # should not apply
                checkDoesNotApplyUDT(cql, table, "v = {a: 0, b: 'def'}", v)
                checkDoesNotApplyUDT(cql, table, "v = {a: 1}", v)
                checkDoesNotApplyUDT(cql, table, "v = {b: 'def'}", v)
                checkDoesNotApplyUDT(cql, table, "v != null", v)
                checkDoesNotApplyUDT(cql, table, "v > {a: 1, b: 'abc'}", v)
                checkDoesNotApplyUDT(cql, table, "v > {a: null, b: 'zzz'}", v)
                checkDoesNotApplyUDT(cql, table, "v > {b: 'zzz'}", v)
                checkDoesNotApplyUDT(cql, table, "v >= {a: null, b: 'zzz'}", v)
                checkDoesNotApplyUDT(cql, table, "v >= {a: 1}", v)
                checkDoesNotApplyUDT(cql, table, "v >= {b: 'zzz'}", v)
                checkDoesNotApplyUDT(cql, table, "v < {a: null, b: 'aaa'}", v)
                checkDoesNotApplyUDT(cql, table, "v < {b: 'aaa'}", v)
                checkDoesNotApplyUDT(cql, table, "v <= {a: null, b: 'aaa'}", v)
                checkDoesNotApplyUDT(cql, table, "v <= {b: 'aaa'}", v)
                checkDoesNotApplyUDT(cql, table, "v IN ({a: 1}, {a: 1, b: 'abc'}, {a: null, b: 'def'})", v)
                checkDoesNotApplyUDT(cql, table, "v IN ()", v)

                # multiple conditions
                checkDoesNotApplyUDT(cql, table, "v IN () AND v IN ({b: 'abc'})", v)
                checkDoesNotApplyUDT(cql, table, "v > {a: -1} AND v < {a: 0}", v)

def checkAppliesUDT(cql, table, condition, value):
    # Whereas Cassandra only returns "True" for success, Scylla also returns
    # the previous content of the row. So we just check the boolean return to
    # be true and ignore the rest in this test.
    # UPDATE statement
    assert list(execute(cql, table, "UPDATE %s SET v = ? WHERE k = 0 IF " + condition, value))[0][0] == True
    assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, value))
    # DELETE statement
    assert list(execute(cql, table, "DELETE FROM %s WHERE k = 0 IF " + condition))[0][0] == True
    assertEmpty(execute(cql, table, "SELECT * FROM %s"))
    execute(cql, table, "INSERT INTO %s (k, v) VALUES (0, ?)", value)

def checkDoesNotApplyUDT(cql, table, condition, value):
    # UPDATE statement
    assertRows(execute(cql, table, "UPDATE %s SET v = ? WHERE k = 0 IF " + condition, value),
               row(False, value))
    assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, value))
    # DELETE statement
    assertRows(execute(cql, table, "DELETE FROM %s WHERE k = 0 IF " + condition),
               row(False, value))
    assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, value))

def checkInvalidUDT(cql, table, condition, value, expected):
    # UPDATE statement
    assertInvalidThrow(cql, table, expected, "UPDATE %s SET v = ?  WHERE k = 0 IF " + condition, value)
    assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, value))
    # DELETE statement
    assertInvalidThrow(cql, table, expected, "DELETE FROM %s WHERE k = 0 IF " + condition)
    assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, value))

@pytest.mark.xfail(reason="Issue #13624")
def testUDTField(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(a int, b text)") as typename:
        for frozen in [False, True]:   
            f = f"frozen<{typename}>" if frozen else typename
            with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, v {f})") as table:
                v = user_type("a", 0, "b", "abc")
                execute(cql, table, "INSERT INTO %s (k, v) VALUES (0, ?)", v)

                checkAppliesUDT(cql, table, "v.a = 0", v)
                checkAppliesUDT(cql, table, "v.b = 'abc'", v)
                checkAppliesUDT(cql, table, "v.a < 1", v)
                checkAppliesUDT(cql, table, "v.b < 'zzz'", v)
                checkAppliesUDT(cql, table, "v.b <= 'bar'", v)
                checkAppliesUDT(cql, table, "v.b > 'aaa'", v)
                checkAppliesUDT(cql, table, "v.b >= 'abc'", v)
                checkAppliesUDT(cql, table, "v.a != -1", v)
                checkAppliesUDT(cql, table, "v.b != 'xxx'", v)
                checkAppliesUDT(cql, table, "v.a != null", v)
                checkAppliesUDT(cql, table, "v.b != null", v)
                checkAppliesUDT(cql, table, "v.a IN (null, 0, 1)", v)
                checkAppliesUDT(cql, table, "v.b IN (null, 'xxx', 'abc')", v)
                checkAppliesUDT(cql, table, "v.b > 'aaa' AND v.b < 'zzz'", v)
                checkAppliesUDT(cql, table, "v.a = 0 AND v.b > 'aaa'", v)

                # do not apply
                checkDoesNotApplyUDT(cql, table, "v.a = -1", v)
                checkDoesNotApplyUDT(cql, table, "v.b = 'xxx'", v)
                checkDoesNotApplyUDT(cql, table, "v.a < -1", v)
                checkDoesNotApplyUDT(cql, table, "v.b < 'aaa'", v)
                checkDoesNotApplyUDT(cql, table, "v.b <= 'aaa'", v)
                checkDoesNotApplyUDT(cql, table, "v.b > 'zzz'", v)
                checkDoesNotApplyUDT(cql, table, "v.b >= 'zzz'", v)
                checkDoesNotApplyUDT(cql, table, "v.a != 0", v)
                checkDoesNotApplyUDT(cql, table, "v.b != 'abc'", v)
                checkDoesNotApplyUDT(cql, table, "v.a IN (null, -1)", v)
                checkDoesNotApplyUDT(cql, table, "v.b IN (null, 'xxx')", v)
                checkDoesNotApplyUDT(cql, table, "v.a IN ()", v)
                checkDoesNotApplyUDT(cql, table, "v.b IN ()", v)
                checkDoesNotApplyUDT(cql, table, "v.b != null AND v.b IN ()", v)

                # invalid
                checkInvalidUDT(cql, table, "v.c = null", v, InvalidRequest)
                checkInvalidUDT(cql, table, "v.a < null", v, InvalidRequest)
                checkInvalidUDT(cql, table, "v.a <= null", v, InvalidRequest)
                checkInvalidUDT(cql, table, "v.a > null", v, InvalidRequest)
                checkInvalidUDT(cql, table, "v.a >= null", v, InvalidRequest)
                checkInvalidUDT(cql, table, "v.a IN null", v, SyntaxException)
                checkInvalidUDT(cql, table, "v.a IN 367", v, SyntaxException)
                checkInvalidUDT(cql, table, "v.b IN (1, 2, 3)", v, InvalidRequest)
                checkInvalidUDT(cql, table, "v.a CONTAINS 367", v, SyntaxException)
                checkInvalidUDT(cql, table, "v.a CONTAINS KEY 367", v, SyntaxException)

                #///////////// null suffix on udt ////////////////
                v = user_type("a", 0, "b", None)
                execute(cql, table, "INSERT INTO %s (k, v) VALUES (0, ?)", v)

                checkAppliesUDT(cql, table, "v.a = 0", v)
                checkAppliesUDT(cql, table, "v.b = null", v)
                checkAppliesUDT(cql, table, "v.b != 'xxx'", v)
                checkAppliesUDT(cql, table, "v.a != null", v)
                checkAppliesUDT(cql, table, "v.a IN (null, 0, 1)", v)
                checkAppliesUDT(cql, table, "v.b IN (null, 'xxx', 'abc')", v)
                checkAppliesUDT(cql, table, "v.a = 0 AND v.b = null", v)

                # do not apply
                checkDoesNotApplyUDT(cql, table, "v.b = 'abc'", v)
                checkDoesNotApplyUDT(cql, table, "v.a < -1", v)
                checkDoesNotApplyUDT(cql, table, "v.b < 'aaa'", v)
                checkDoesNotApplyUDT(cql, table, "v.b <= 'aaa'", v)
                checkDoesNotApplyUDT(cql, table, "v.b > 'zzz'", v)
                checkDoesNotApplyUDT(cql, table, "v.b >= 'zzz'", v)
                checkDoesNotApplyUDT(cql, table, "v.a != 0", v)
                checkDoesNotApplyUDT(cql, table, "v.b != null", v)
                checkDoesNotApplyUDT(cql, table, "v.a IN (null, -1)", v)
                checkDoesNotApplyUDT(cql, table, "v.b IN ('xxx', 'abc')", v)
                checkDoesNotApplyUDT(cql, table, "v.a IN ()", v)
                checkDoesNotApplyUDT(cql, table, "v.b IN ()", v)
                checkDoesNotApplyUDT(cql, table, "v.b != null AND v.b IN ()", v)

                #///////////// null prefix on udt ////////////////
                v = user_type("a", None, "b", "abc")
                execute(cql, table, "INSERT INTO %s (k, v) VALUES (0, ?)", v)

                checkAppliesUDT(cql, table, "v.a = null", v)
                checkAppliesUDT(cql, table, "v.b = 'abc'", v)
                checkAppliesUDT(cql, table, "v.a != 0", v)
                checkAppliesUDT(cql, table, "v.b != null", v)
                checkAppliesUDT(cql, table, "v.a IN (null, 0, 1)", v)
                checkAppliesUDT(cql, table, "v.b IN (null, 'xxx', 'abc')", v)
                checkAppliesUDT(cql, table, "v.a = null AND v.b = 'abc'", v)

                # do not apply
                checkDoesNotApplyUDT(cql, table, "v.a = 0", v)
                checkDoesNotApplyUDT(cql, table, "v.a < -1", v)
                checkDoesNotApplyUDT(cql, table, "v.b >= 'zzz'", v)
                checkDoesNotApplyUDT(cql, table, "v.a != null", v)
                checkDoesNotApplyUDT(cql, table, "v.b != 'abc'", v)
                checkDoesNotApplyUDT(cql, table, "v.a IN (-1, 0)", v)
                checkDoesNotApplyUDT(cql, table, "v.b IN (null, 'xxx')", v)
                checkDoesNotApplyUDT(cql, table, "v.a IN ()", v)
                checkDoesNotApplyUDT(cql, table, "v.b IN ()", v)
                checkDoesNotApplyUDT(cql, table, "v.b != null AND v.b IN ()", v)

                #///////////// null udt ////////////////
                v = None
                execute(cql, table, "INSERT INTO %s (k, v) VALUES (0, ?)", v)

                checkAppliesUDT(cql, table, "v.a = null", v)
                checkAppliesUDT(cql, table, "v.b = null", v)
                checkAppliesUDT(cql, table, "v.a != 0", v)
                checkAppliesUDT(cql, table, "v.b != 'abc'", v)
                checkAppliesUDT(cql, table, "v.a IN (null, 0, 1)", v)
                checkAppliesUDT(cql, table, "v.b IN (null, 'xxx', 'abc')", v)
                checkAppliesUDT(cql, table, "v.a = null AND v.b = null", v)

                # do not apply
                checkDoesNotApplyUDT(cql, table, "v.a = 0", v)
                checkDoesNotApplyUDT(cql, table, "v.a < -1", v)
                checkDoesNotApplyUDT(cql, table, "v.b >= 'zzz'", v)
                checkDoesNotApplyUDT(cql, table, "v.a != null", v)
                checkDoesNotApplyUDT(cql, table, "v.b != null", v)
                checkDoesNotApplyUDT(cql, table, "v.a IN (-1, 0)", v)
                checkDoesNotApplyUDT(cql, table, "v.b IN ('xxx', 'abc')", v)
                checkDoesNotApplyUDT(cql, table, "v.a IN ()", v)
                checkDoesNotApplyUDT(cql, table, "v.b IN ()", v)
                checkDoesNotApplyUDT(cql, table, "v.b != null AND v.b IN ()", v)

# Migrated from cql_tests.py:TestCQL.whole_list_conditional_test()
@pytest.mark.xfail(reason="Issue #13586")
def testWholeList(cql, test_keyspace):
    for frozen in [False, True]:   
        typename = "list<text>"
        f = f"frozen<{typename}>" if frozen else typename
        with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, l {f})") as table:
            execute(cql, table, "INSERT INTO %s(k, l) VALUES (0, ['foo', 'bar', 'foobar'])")

            check_applies_list(cql, table, "l = ['foo', 'bar', 'foobar']")
            check_applies_list(cql, table, "l != ['baz']")
            check_applies_list(cql, table, "l > ['a']")
            check_applies_list(cql, table, "l >= ['a']")
            check_applies_list(cql, table, "l < ['z']")
            check_applies_list(cql, table, "l <= ['z']")
            check_applies_list(cql, table, "l IN (null, ['foo', 'bar', 'foobar'], ['a'])")
            # Reproduces #13586:
            check_applies_list(cql, table, "l CONTAINS 'bar'")

            # multiple conditions
            check_applies_list(cql, table, "l > ['aaa', 'bbb'] AND l > ['aaa']")
            check_applies_list(cql, table, "l != null AND l IN (['foo', 'bar', 'foobar'])")
            # Reproduces #13586:
            check_applies_list(cql, table, "l CONTAINS 'foo' AND l CONTAINS 'foobar'")

            # should not apply
            check_does_not_apply_list(cql, table, "l = ['baz']")
            check_does_not_apply_list(cql, table, "l != ['foo', 'bar', 'foobar']")
            check_does_not_apply_list(cql, table, "l > ['z']")
            check_does_not_apply_list(cql, table, "l >= ['z']")
            check_does_not_apply_list(cql, table, "l < ['a']")
            check_does_not_apply_list(cql, table, "l <= ['a']")
            check_does_not_apply_list(cql, table, "l IN (['a'], null)")
            check_does_not_apply_list(cql, table, "l IN ()")
            # Reproduces #13586:
            check_does_not_apply_list(cql, table, "l CONTAINS 'baz'")

            # multiple conditions
            check_does_not_apply_list(cql, table, "l IN () AND l IN (['foo', 'bar', 'foobar'])")
            check_does_not_apply_list(cql, table, "l > ['zzz'] AND l < ['zzz']")
            # Reproduces #13586:
            check_does_not_apply_list(cql, table, "l CONTAINS 'bar' AND l CONTAINS 'baz'")

            # Scylla does allow the check for l = [null], it just is never true.
            #check_invalid_list(cql, table, "l = [null]", InvalidRequest)
            check_invalid_list(cql, table, "l < null", InvalidRequest)
            check_invalid_list(cql, table, "l <= null", InvalidRequest)
            check_invalid_list(cql, table, "l > null", InvalidRequest)
            check_invalid_list(cql, table, "l >= null", InvalidRequest)
            check_invalid_list(cql, table, "l IN null", SyntaxException)
            check_invalid_list(cql, table, "l IN 367", SyntaxException)
            # Reproduces #13586:
            check_invalid_list(cql, table, "l CONTAINS KEY 123", InvalidRequest)
            check_invalid_list(cql, table, "l CONTAINS null", InvalidRequest)

def check_applies_list(cql, table, condition):
    # UPDATE statement
    assert list(execute(cql, table, "UPDATE %s SET l = ['foo', 'bar', 'foobar'] WHERE k=0 IF " + condition))[0][0] == True
    assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, ["foo", "bar", "foobar"]))
    # DELETE statement
    assert list(execute(cql, table, "DELETE FROM %s WHERE k=0 IF " + condition))[0][0] == True
    assertEmpty(execute(cql, table, "SELECT * FROM %s"))
    execute(cql, table, "INSERT INTO %s(k, l) VALUES (0, ['foo', 'bar', 'foobar'])")

def check_does_not_apply_list(cql, table, condition):
    # UPDATE statement
    assertRows(execute(cql, table, "UPDATE %s SET l = ['foo', 'bar', 'foobar'] WHERE k=0 IF " + condition),
               row(False, ["foo", "bar", "foobar"]))
    assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, ["foo", "bar", "foobar"]))
    # DELETE statement
    assertRows(execute(cql, table, "DELETE FROM %s WHERE k=0 IF " + condition),
               row(False, ["foo", "bar", "foobar"]))
    assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, ["foo", "bar", "foobar"]))

def check_invalid_list(cql, table, condition, expected):
    # UPDATE statement
    assertInvalidThrow(cql, table, expected, "UPDATE %s SET l = ['foo', 'bar', 'foobar'] WHERE k=0 IF " + condition)
    assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, ["foo", "bar", "foobar"]))
    # DELETE statement
    assertInvalidThrow(cql, table, expected, "DELETE FROM %s WHERE k=0 IF " + condition)
    assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, ["foo", "bar", "foobar"]))

# Migrated from cql_tests.py:TestCQL.list_item_conditional_test()
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18066")]), "vnodes"],
                         indirect=True)
def testListItem(cql, test_keyspace):
    for frozen in [False, True]:   
        typename = "list<text>"
        f = f"frozen<{typename}>" if frozen else typename
        with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, l {f})") as table:
            execute(cql, table, "INSERT INTO %s(k, l) VALUES (0, ['foo', 'bar', 'foobar'])")
            # Whereas Cassandra gives an InvalidRequest error on l[null] or l[-2], Scylla
            # deliberately (see commit 8e972b52c526f1ffbb7ed52e53708abed8c4b547) allows
            # such indexes, and returns null. So we comment out Cassandra's check:
            #assertInvalidMessage(cql, table, "Invalid null value for list element access",
            #                     "DELETE FROM %s WHERE k=0 IF l[?] = ?", None, "foobar")
            #assertInvalidMessage(cql, table, "Invalid negative list index -2",
            #                     "DELETE FROM %s WHERE k=0 IF l[?] = ?", -2, "foobar")
            assertInvalidSyntax(cql, table, "DELETE FROM %s WHERE k=0 IF l[?] CONTAINS ?", 0, "bar")
            assertInvalidSyntax(cql, table, "DELETE FROM %s WHERE k=0 IF l[?] CONTAINS KEY ?", 0, "bar")
            assertRows(execute(cql, table, "DELETE FROM %s WHERE k=0 IF l[?] = ?", 1, None), row(False, ["foo", "bar", "foobar"]))
            assertRows(execute(cql, table, "DELETE FROM %s WHERE k=0 IF l[?] = ?", 1, "foobar"), row(False, ["foo", "bar", "foobar"]))
            assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, ["foo", "bar", "foobar"]))

            assert list(execute(cql, table, "DELETE FROM %s WHERE k=0 IF l[?] = ?", 1, "bar"))[0][0] == True
            assertEmpty(execute(cql, table, "SELECT * FROM %s"))

# Test expanded functionality from CASSANDRA-6839,
# migrated from cql_tests.py:TestCQL.expanded_list_item_conditional_test()
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18066")]), "vnodes"],
                         indirect=True)
def testExpandedListItem(cql, test_keyspace):
    for frozen in [False, True]:   
        typename = "list<text>"
        f = f"frozen<{typename}>" if frozen else typename
        with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, l {f})") as table:
            execute(cql, table, "INSERT INTO %s (k, l) VALUES (0, ['foo', 'bar', 'foobar'])")

            check_applies_list(cql, table, "l[1] < 'zzz'")
            check_applies_list(cql, table, "l[1] <= 'bar'")
            check_applies_list(cql, table, "l[1] > 'aaa'")
            check_applies_list(cql, table, "l[1] >= 'bar'")
            check_applies_list(cql, table, "l[1] != 'xxx'")
            check_applies_list(cql, table, "l[1] != null")
            check_applies_list(cql, table, "l[1] IN (null, 'xxx', 'bar')")
            check_applies_list(cql, table, "l[1] > 'aaa' AND l[1] < 'zzz'")

            # check beyond end of list
            check_applies_list(cql, table, "l[3] = null")
            check_applies_list(cql, table, "l[3] IN (null, 'xxx', 'bar')")

            check_does_not_apply_list(cql, table, "l[1] < 'aaa'")
            check_does_not_apply_list(cql, table, "l[1] <= 'aaa'")
            check_does_not_apply_list(cql, table, "l[1] > 'zzz'")
            check_does_not_apply_list(cql, table, "l[1] >= 'zzz'")
            check_does_not_apply_list(cql, table, "l[1] != 'bar'")
            check_does_not_apply_list(cql, table, "l[1] IN (null, 'xxx')")
            check_does_not_apply_list(cql, table, "l[1] IN ()")
            check_does_not_apply_list(cql, table, "l[1] != null AND l[1] IN ()")

            # check beyond end of list
            check_does_not_apply_list(cql, table, "l[3] != null")
            check_does_not_apply_list(cql, table, "l[3] = 'xxx'")

            check_invalid_list(cql, table, "l[1] < null", InvalidRequest)
            check_invalid_list(cql, table, "l[1] <= null", InvalidRequest)
            check_invalid_list(cql, table, "l[1] > null", InvalidRequest)
            check_invalid_list(cql, table, "l[1] >= null", InvalidRequest)
            check_invalid_list(cql, table, "l[1] IN null", SyntaxException)
            check_invalid_list(cql, table, "l[1] IN 367", SyntaxException)
            check_invalid_list(cql, table, "l[1] IN (1, 2, 3)", InvalidRequest)
            check_invalid_list(cql, table, "l[1] CONTAINS 367", SyntaxException)
            check_invalid_list(cql, table, "l[1] CONTAINS KEY 367", SyntaxException)
            # ScyllaDB allows l[null] so this test is commented out.
            #check_invalid_list(cql, table, "l[null] = null", InvalidRequest)

# Migrated from cql_tests.py:TestCQL.whole_set_conditional_test()
@pytest.mark.xfail(reason="Issue #13586")
def testWholeSet(cql, test_keyspace):
    for frozen in [False, True]:   
        typename = "set<text>"
        f = f"frozen<{typename}>" if frozen else typename
        with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, s {f})") as table:
            execute(cql, table, "INSERT INTO %s (k, s) VALUES (0, {'bar', 'foo'})")

            check_applies_set(cql, table, "s = {'bar', 'foo'}")
            check_applies_set(cql, table, "s = {'foo', 'bar'}")
            check_applies_set(cql, table, "s != {'baz'}")
            check_applies_set(cql, table, "s > {'a'}")
            check_applies_set(cql, table, "s >= {'a'}")
            check_applies_set(cql, table, "s < {'z'}")
            check_applies_set(cql, table, "s <= {'z'}")
            check_applies_set(cql, table, "s IN (null, {'bar', 'foo'}, {'a'})")
            # Reproduces #13586:
            check_applies_set(cql, table, "s CONTAINS 'foo'")

            # multiple conditions
            check_applies_set(cql, table, "s > {'a'} AND s < {'z'}")
            check_applies_set(cql, table, "s IN (null, {'bar', 'foo'}, {'a'}) AND s IN ({'a'}, {'bar', 'foo'}, null)")
            # Reproduces #13586:
            check_applies_set(cql, table, "s CONTAINS 'foo' AND s CONTAINS 'bar'")

            # should not apply
            check_does_not_apply_set(cql, table, "s = {'baz'}")
            check_does_not_apply_set(cql, table, "s != {'bar', 'foo'}")
            check_does_not_apply_set(cql, table, "s > {'z'}")
            check_does_not_apply_set(cql, table, "s >= {'z'}")
            check_does_not_apply_set(cql, table, "s < {'a'}")
            check_does_not_apply_set(cql, table, "s <= {'a'}")
            check_does_not_apply_set(cql, table, "s IN ({'a'}, null)")
            check_does_not_apply_set(cql, table, "s IN ()")
            check_does_not_apply_set(cql, table, "s != null AND s IN ()")
            # Reproduces #13586:
            check_does_not_apply_set(cql, table, "s CONTAINS 'baz'")

            # ScyllaDB does allow check =null, so this test is commented out
            #check_invalid_set(cql, table, "s = {null}", InvalidRequest)
            check_invalid_set(cql, table, "s < null", InvalidRequest)
            check_invalid_set(cql, table, "s <= null", InvalidRequest)
            check_invalid_set(cql, table, "s > null", InvalidRequest)
            check_invalid_set(cql, table, "s >= null", InvalidRequest)
            check_invalid_set(cql, table, "s IN null", SyntaxException)
            check_invalid_set(cql, table, "s IN 367", SyntaxException)
            # Reproduces #13586:
            check_invalid_set(cql, table, "s CONTAINS null", InvalidRequest)
            check_invalid_set(cql, table, "s CONTAINS KEY 123", InvalidRequest)

            # element access is not allow for sets
            check_invalid_set(cql, table, "s['foo'] = 'foobar'", InvalidRequest)

def check_applies_set(cql, table, condition):
    # UPDATE statement
    assert list(execute(cql, table, "UPDATE %s SET s = {'bar', 'foo'} WHERE k=0 IF " + condition))[0][0] == True
    assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, {"bar", "foo"}))
    # DELETE statement
    assert list(execute(cql, table, "DELETE FROM %s WHERE k=0 IF " + condition))[0][0] == True
    assertEmpty(execute(cql, table, "SELECT * FROM %s"))
    execute(cql, table, "INSERT INTO %s (k, s) VALUES (0, {'bar', 'foo'})")

def check_does_not_apply_set(cql, table, condition):
    # UPDATE statement
    assertRows(execute(cql, table, "UPDATE %s SET s = {'bar', 'foo'} WHERE k=0 IF " + condition), row(False, {"bar", "foo"}))
    assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, {"bar", "foo"}))
    # DELETE statement
    assertRows(execute(cql, table, "DELETE FROM %s WHERE k=0 IF " + condition), row(False, {"bar", "foo"}))
    assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, {"bar", "foo"}))

def check_invalid_set(cql, table, condition, expected):
    # UPDATE statement
    assertInvalidThrow(cql, table, expected, "UPDATE %s SET s = {'bar', 'foo'} WHERE k=0 IF " + condition)
    assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, {"bar", "foo"}))
    # DELETE statement
    assertInvalidThrow(cql, table, expected, "DELETE FROM %s WHERE k=0 IF " + condition)
    assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, {"bar", "foo"}))

# Migrated from cql_tests.py:TestCQL.whole_map_conditional_test()
@pytest.mark.xfail(reason="Issue #13586")
def testWholeMap(cql, test_keyspace):
    for frozen in [False, True]:   
        typename = "map<text,text>"
        f = f"frozen<{typename}>" if frozen else typename
        with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, m {f})") as table:
            execute(cql, table, "INSERT INTO %s (k, m) VALUES (0, {'foo' : 'bar'})")

            check_applies_map(cql, table, "m = {'foo': 'bar'}")
            check_applies_map(cql, table, "m > {'a': 'a'}")
            check_applies_map(cql, table, "m >= {'a': 'a'}")
            check_applies_map(cql, table, "m < {'z': 'z'}")
            check_applies_map(cql, table, "m <= {'z': 'z'}")
            check_applies_map(cql, table, "m != {'a': 'a'}")
            check_applies_map(cql, table, "m IN (null, {'a': 'a'}, {'foo': 'bar'})")
            # Reproduces #13586:
            check_applies_map(cql, table, "m CONTAINS 'bar'")
            check_applies_map(cql, table, "m CONTAINS KEY 'foo'")

            # multiple conditions
            check_applies_map(cql, table, "m > {'a': 'a'} AND m < {'z': 'z'}")
            check_applies_map(cql, table, "m != null AND m IN (null, {'a': 'a'}, {'foo': 'bar'})")
            # Reproduces #13586:
            check_applies_map(cql, table, "m CONTAINS 'bar' AND m CONTAINS KEY 'foo'")

            # should not apply
            check_does_not_apply_map(cql, table, "m = {'a': 'a'}")
            check_does_not_apply_map(cql, table, "m > {'z': 'z'}")
            check_does_not_apply_map(cql, table, "m >= {'z': 'z'}")
            check_does_not_apply_map(cql, table, "m < {'a': 'a'}")
            check_does_not_apply_map(cql, table, "m <= {'a': 'a'}")
            check_does_not_apply_map(cql, table, "m != {'foo': 'bar'}")
            check_does_not_apply_map(cql, table, "m IN ({'a': 'a'}, null)")
            check_does_not_apply_map(cql, table, "m IN ()")
            check_does_not_apply_map(cql, table, "m = null AND m != null")
            # Reproduces #13586:
            check_does_not_apply_map(cql, table, "m CONTAINS 'foo'")
            check_does_not_apply_map(cql, table, "m CONTAINS KEY 'bar'")

            check_invalid_map(cql, table, "m = {null: null}", InvalidRequest)
            check_invalid_map(cql, table, "m = {'a': null}", InvalidRequest)
            check_invalid_map(cql, table, "m = {null: 'a'}", InvalidRequest)
            # Reproduces #13586:
            check_invalid_map(cql, table, "m CONTAINS null", InvalidRequest)
            check_invalid_map(cql, table, "m CONTAINS KEY null", InvalidRequest)
            check_invalid_map(cql, table, "m < null", InvalidRequest)
            check_invalid_map(cql, table, "m IN null", SyntaxException)

# Migrated from cql_tests.py:TestCQL.map_item_conditional_test()
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18066")]), "vnodes"],
                         indirect=True)
def testMapItem(cql, test_keyspace):
    for frozen in [False, True]:   
        typename = "map<text,text>"
        f = f"frozen<{typename}>" if frozen else typename
        with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, m {f})") as table:
            execute(cql, table, "INSERT INTO %s (k, m) VALUES (0, {'foo' : 'bar'})")
            # Scylla does allow comparison to null (which is false)
            #assertInvalidMessage(cql, table, "Invalid null value for map element access",
            #                     "DELETE FROM %s WHERE k=0 IF m[?] = ?", None, "foo")
            assertInvalidSyntax(cql, table, "DELETE FROM %s WHERE k=0 IF m[?] CONTAINS ?", "foo", "bar")
            assertInvalidSyntax(cql, table, "DELETE FROM %s WHERE k=0 IF m[?] CONTAINS KEY ?", "foo", "bar")
            assertRows(execute(cql, table, "DELETE FROM %s WHERE k=0 IF m[?] = ?", "foo", "foo"), row(False, {"foo": "bar"}))
            assertRows(execute(cql, table, "DELETE FROM %s WHERE k=0 IF m[?] = ?", "foo", None), row(False, {"foo": "bar"}))
            assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, {"foo": "bar"}))

            assert list(execute(cql, table, "DELETE FROM %s WHERE k=0 IF m[?] = ?", "foo", "bar"))[0][0] == True
            assertEmpty(execute(cql, table, "SELECT * FROM %s"))

            execute(cql, table, "INSERT INTO %s(k, m) VALUES (1, null)")
            if frozen:
                # Reproduces #13657:
                assertInvalidMessage(cql, table, "Invalid operation (m['foo'] = 'bar') for frozen collection column m",
                                     "UPDATE %s set m['foo'] = 'bar', m['bar'] = 'foo' WHERE k = 1 IF m[?] IN (?, ?)", "foo", "blah", None)
            else:
                assert list(execute(cql, table, "UPDATE %s set m['foo'] = 'bar', m['bar'] = 'foo' WHERE k = 1 IF m[?] IN (?, ?)", "foo", "blah", None))[0][0] == True

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18066")]), "vnodes"],
                         indirect=True)
def testFrozenWithNullValues(cql, test_keyspace):
    with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, m frozen<list<text>>)") as table:
        execute(cql, table, "INSERT INTO %s (k, m) VALUES (0, null)")

        assertRows(execute(cql, table, "UPDATE %s SET m = ? WHERE k = 0 IF m = ?", ["test"], ["comparison"]), row(False, None))

    with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, m frozen<map<text, int>>)") as table:
        execute(cql, table, "INSERT INTO %s (k, m) VALUES (0, null)")

        assertRows(execute(cql, table, "UPDATE %s SET m = ? WHERE k = 0 IF m = ?", {"test": 3}, {"comparison": 2}), row(False, None))

    with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, m frozen<set<text>>)") as table:
        execute(cql, table, "INSERT INTO %s (k, m) VALUES (0, null)")

        assertRows(execute(cql, table, "UPDATE %s SET m = ? WHERE k = 0 IF m = ?", {"test"}, {"comparison"}), row(False, None))

# Test expanded functionality from CASSANDRA-6839,
# migrated from cql_tests.py:TestCQL.expanded_map_item_conditional_test()
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18066")]), "vnodes"],
                         indirect=True)
def testExpandedMapItem(cql, test_keyspace):
    for frozen in [False, True]:   
        typename = "map<text,text>"
        f = f"frozen<{typename}>" if frozen else typename
        with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, m {f})") as table:
            execute(cql, table, "INSERT INTO %s (k, m) VALUES (0, {'foo' : 'bar'})")

            check_applies_map(cql, table, "m['xxx'] = null")
            check_applies_map(cql, table, "m['foo'] < 'zzz'")
            check_applies_map(cql, table, "m['foo'] <= 'bar'")
            check_applies_map(cql, table, "m['foo'] > 'aaa'")
            check_applies_map(cql, table, "m['foo'] >= 'bar'")
            check_applies_map(cql, table, "m['foo'] != 'xxx'")
            check_applies_map(cql, table, "m['foo'] != null")
            check_applies_map(cql, table, "m['foo'] IN (null, 'xxx', 'bar')")
            check_applies_map(cql, table, "m['xxx'] IN (null, 'xxx', 'bar')"); # m['xxx'] is not set

            # multiple conditions
            check_applies_map(cql, table, "m['foo'] < 'zzz' AND m['foo'] > 'aaa'")

            check_does_not_apply_map(cql, table, "m['foo'] < 'aaa'")
            check_does_not_apply_map(cql, table, "m['foo'] <= 'aaa'")
            check_does_not_apply_map(cql, table, "m['foo'] > 'zzz'")
            check_does_not_apply_map(cql, table, "m['foo'] >= 'zzz'")
            check_does_not_apply_map(cql, table, "m['foo'] != 'bar'")
            check_does_not_apply_map(cql, table, "m['xxx'] != null");  # m['xxx'] is not set
            check_does_not_apply_map(cql, table, "m['foo'] IN (null, 'xxx')")
            check_does_not_apply_map(cql, table, "m['foo'] IN ()")
            check_does_not_apply_map(cql, table, "m['foo'] != null AND m['foo'] = null")

            check_invalid_map(cql, table, "m['foo'] < null", InvalidRequest)
            check_invalid_map(cql, table, "m['foo'] <= null", InvalidRequest)
            check_invalid_map(cql, table, "m['foo'] > null", InvalidRequest)
            check_invalid_map(cql, table, "m['foo'] >= null", InvalidRequest)
            check_invalid_map(cql, table, "m['foo'] IN null", SyntaxException)
            check_invalid_map(cql, table, "m['foo'] IN 367", SyntaxException)
            check_invalid_map(cql, table, "m['foo'] IN (1, 2, 3)", InvalidRequest)
            check_invalid_map(cql, table, "m['foo'] CONTAINS 367", SyntaxException)
            check_invalid_map(cql, table, "m['foo'] CONTAINS KEY 367", SyntaxException)
            # m[null] is allowed in Scylla
            # check_invalid_map(cql, table, "m[null] = null", InvalidRequest)

def check_applies_map(cql, table, condition):
    # UPDATE statement
    assert list(execute(cql, table, "UPDATE %s SET m = {'foo': 'bar'} WHERE k=0 IF " + condition))[0][0] == True
    assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, {"foo": "bar"}))
    # DELETE statement
    assert list(execute(cql, table, "DELETE FROM %s WHERE k=0 IF " + condition))[0][0] == True
    assertEmpty(execute(cql, table, "SELECT * FROM %s"))
    execute(cql, table, "INSERT INTO %s (k, m) VALUES (0, {'foo' : 'bar'})")

def check_does_not_apply_map(cql, table, condition):
    assertRows(execute(cql, table, "UPDATE %s SET m = {'foo': 'bar'} WHERE k=0 IF " + condition), row(False, {"foo": "bar"}))
    assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, {"foo": "bar"}))
    # DELETE statement
    assertRows(execute(cql, table, "DELETE FROM %s WHERE k=0 IF " + condition), row(False, {"foo": "bar"}))
    assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, {"foo": "bar"}))

def check_invalid_map(cql, table, condition, expected):
    # UPDATE statement
    assertInvalidThrow(cql, table, expected, "UPDATE %s SET m = {'foo': 'bar'} WHERE k=0 IF " + condition)
    assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, {"foo": "bar"}))
    # DELETE statement
    assertInvalidThrow(cql, table, expected, "DELETE FROM %s WHERE k=0 IF " + condition)
    assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, {"foo": "bar"}))

@pytest.mark.xfail(reason="Issue #13586")
def testInMarkerWithUDTs(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(a int, b text)") as typename:
        for frozen in [False, True]:
            f = f"frozen<{typename}>" if frozen else typename
            with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, v {f})") as table:
                v = user_type("a", 0, "b", "abc")
                execute(cql, table, "INSERT INTO %s (k, v) VALUES (0, ?)", v)

                # Does not apply
                assertRows(execute(cql, table, "UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v IN (?, ?)", userType("a", 1, "b", "abc"), userType("a", 0, "b", "ac")),
                       row(False, v))
                assertRows(execute(cql, table, "UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v IN (?, ?)", userType("a", 1, "b", "abc"), None),
                       row(False, v))
                assertRows(execute(cql, table, "UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v IN (?, ?)", None, None),
                       row(False, v))
                # Cassandra since, commit https://github.com/apache/cassandra/commit/70e33d96e1f1236788afb50c1f02fbc64d760281
                # for CASSANDRA-12981, allows unset() values in IN and they are skipped.
                # Scylla doesn't allow this - see discussion in #13659.
                #assertRows(execute(cql, table, "UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v IN (?, ?)", userType("a", 1, "b", "abc"), unset()),
                #       row(False, v))
                #assertRows(execute(cql, table, "UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v IN (?, ?)", unset(), unset()),
                #       row(False, v))
                assertRows(execute(cql, table, "UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v IN ?", [userType("a", 1, "b", "abc"), userType("a", 0, "b", "ac")]),
                       row(False, v))
                # Reproduces #13624:
                assertRows(execute(cql, table, "UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v.a IN (?, ?)", 1, 2),
                       row(False, v))
                assertRows(execute(cql, table, "UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v.a IN (?, ?)", 1, None),
                       row(False, v))
                assertRows(execute(cql, table, "UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v.a IN ?", [1, 2]),
                       row(False, v))
                assertRows(execute(cql, table, "UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v.a IN (?, ?)", 1, unset()),
                       row(False, v))

                # Does apply
                assert list(execute(cql, table, "UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v IN (?, ?)", userType("a", 0, "b", "abc"), userType("a", 0, "b", "ac")))[0][0] == True
                assert list(execute(cql, table, "UPDATE %s SET v = {a: 1, b: 'bc'} WHERE k = 0 IF v IN (?, ?)", userType("a", 0, "b", "bc"), None))[0][0] == True
                assert list(execute(cql, table, "UPDATE %s SET v = {a: 0, b: 'abc'} WHERE k = 0 IF v IN ?", [userType("a", 1, "b", "bc"), userType("a", 0, "b", "ac")]))[0][0] == True
                # Scylla doesn't allow this - see discussion in #13659.
                #assert list(execute(cql, table, "UPDATE %s SET v = {a: 1, b: 'bc'} WHERE k = 0 IF v IN (?, ?, ?)", userType("a", 1, "b", "bc"), unset(), userType("a", 0, "b", "abc")))[0][0] == True
                # Reproduces #13624:
                assert list(execute(cql, table, "UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v.a IN (?, ?)", 1, 0))[0][0] == True
                assert list(execute(cql, table, "UPDATE %s SET v = {a: 0, b: 'abc'} WHERE k = 0 IF v.a IN (?, ?)", 0, None))[0][0] == True
                assert list(execute(cql, table, "UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v.a IN ?", [0, 1]))[0][0] == True
                assert list(execute(cql, table, "UPDATE %s SET v = {a: 0, b: 'abc'} WHERE k = 0 IF v.a IN (?, ?, ?)", 1, unset(), 0))[0][0] == True

                # Scylla does allow "IN null", the condition simply fails.
                #assertInvalidMessage(cql, table, "Invalid null list in IN condition",
                #                 "UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v IN ?", None)
                assertInvalidMessage(cql, table, "unset",
                                 "UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v IN ?", unset())
                # Reproduces #13624:
                assertInvalidMessage(cql, table, "unset",
                                 "UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v.a IN ?", unset())

@pytest.mark.xfail(reason="Issues #13586, #5855")
def testNonFrozenEmptyCollection(cql, test_keyspace):
    with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, l list<text>)") as table:
        execute(cql, table, "INSERT INTO %s (k, l) VALUES (0, null)")

        # Does apply
        assert list(execute(cql, table, "UPDATE %s SET l = null WHERE k = 0 IF l = ?", None))[0][0] == True
        # Reproduces #5855:
        assert list(execute(cql, table, "UPDATE %s SET l = null WHERE k = 0 IF l = ?", []))[0][0] == True
        assert list(execute(cql, table, "UPDATE %s SET l = null WHERE k = 0 IF l != ?", ["bar"]))[0][0] == True
        # Reproduces #5855:
        assert list(execute(cql, table, "UPDATE %s SET l = null WHERE k = 0 IF l < ?", ["a"]))[0][0] == True
        assert list(execute(cql, table, "UPDATE %s SET l = null WHERE k = 0 IF l <= ?", ["a"]))[0][0] == True
        assert list(execute(cql, table, "UPDATE %s SET l = null WHERE k = 0 IF l IN (?, ?)", None, ["bar"]))[0][0] == True

        # Does not apply
        assertRows(execute(cql, table, "UPDATE %s SET l = null WHERE k = 0 IF l = ?", ["bar"]),
                   row(False, None))
        assertRows(execute(cql, table, "UPDATE %s SET l = null WHERE k = 0 IF l > ?", ["a"]),
                   row(False, None))
        assertRows(execute(cql, table, "UPDATE %s SET l = null WHERE k = 0 IF l >= ?", ["a"]),
                   row(False, None))
        # Reproduces #13586:
        assertRows(execute(cql, table, "UPDATE %s SET l = null WHERE k = 0 IF l CONTAINS ?", "bar"),
                   row(False, None))
        assertRows(execute(cql, table, "UPDATE %s SET l = null WHERE k = 0 IF l CONTAINS ?", unset()),
                   row(False, None))

        assertInvalidMessage(cql, table, "Invalid comparison with null for operator \"CONTAINS\"",
                             "UPDATE %s SET l = null WHERE k = 0 IF l CONTAINS ?", None)

    with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, s set<text>)") as table:
        execute(cql, table, "INSERT INTO %s (k, s) VALUES (0, null)")

        # Does apply
        assert list(execute(cql, table, "UPDATE %s SET s = null WHERE k = 0 IF s = ?", None))[0][0] == True
        # Reproduces #5855:
        assert list(execute(cql, table, "UPDATE %s SET s = null WHERE k = 0 IF s = ?", set()))[0][0] == True
        assert list(execute(cql, table, "UPDATE %s SET s = null WHERE k = 0 IF s != ?", {"bar"}))[0][0] == True
        # Reproduces #5855:
        assert list(execute(cql, table, "UPDATE %s SET s = null WHERE k = 0 IF s < ?", {"a"}))[0][0] == True
        assert list(execute(cql, table, "UPDATE %s SET s = null WHERE k = 0 IF s <= ?", {"a"}))[0][0] == True
        assert list(execute(cql, table, "UPDATE %s SET s = null WHERE k = 0 IF s IN (?, ?)", None, {"bar"}))[0][0] == True

        # Does not apply
        assertRows(execute(cql, table, "UPDATE %s SET s = null WHERE k = 0 IF s = ?", {"bar"}),
                   row(False, None))
        assertRows(execute(cql, table, "UPDATE %s SET s = null WHERE k = 0 IF s > ?", {"a"}),
                   row(False, None))
        assertRows(execute(cql, table, "UPDATE %s SET s = null WHERE k = 0 IF s >= ?", {"a"}),
                   row(False, None))
        # Reproduces #13586:
        assertRows(execute(cql, table, "UPDATE %s SET s = null WHERE k = 0 IF s CONTAINS ?", "bar"),
                   row(False, None))
        assertRows(execute(cql, table, "UPDATE %s SET s = null WHERE k = 0 IF s CONTAINS ?", unset()),
                   row(False, None))

        assertInvalidMessage(cql, table, "Invalid comparison with null for operator \"CONTAINS\"",
                             "UPDATE %s SET s = null WHERE k = 0 IF s CONTAINS ?", None)

    with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, m map<text, text>)") as table:
        execute(cql, table, "INSERT INTO %s (k, m) VALUES (0, null)")

        # Does apply
        assert list(execute(cql, table, "UPDATE %s SET m = null WHERE k = 0 IF m = ?", None))[0][0] == True
        # Reproduces #5855:
        assert list(execute(cql, table, "UPDATE %s SET m = null WHERE k = 0 IF m = ?", {}))[0][0] == True
        assert list(execute(cql, table, "UPDATE %s SET m = null WHERE k = 0 IF m != ?", {"foo":"bar"}))[0][0] == True
        # Reproduces #5855:
        assert list(execute(cql, table, "UPDATE %s SET m = null WHERE k = 0 IF m < ?", {"a":"a"}))[0][0] == True
        assert list(execute(cql, table, "UPDATE %s SET m = null WHERE k = 0 IF m <= ?", {"a":"a"}))[0][0] == True
        assert list(execute(cql, table, "UPDATE %s SET m = null WHERE k = 0 IF m IN (?, ?)", None, {"foo":"bar"}))[0][0] == True

        # Does not apply
        assertRows(execute(cql, table, "UPDATE %s SET m = null WHERE k = 0 IF m = ?", {"foo":"bar"}),
                   row(False, None))
        assertRows(execute(cql, table, "UPDATE %s SET m = null WHERE k = 0 IF m > ?", {"a": "a"}),
                   row(False, None))
        assertRows(execute(cql, table, "UPDATE %s SET m = null WHERE k = 0 IF m >= ?", {"a": "a"}),
                   row(False, None))
        # Reproduces #13586:
        assertRows(execute(cql, table, "UPDATE %s SET m = null WHERE k = 0 IF m CONTAINS ?", "bar"),
                   row(False, None))
        assertRows(execute(cql, table, "UPDATE %s SET m = {} WHERE k = 0 IF m CONTAINS ?", unset()),
                   row(False, None))
        assertRows(execute(cql, table, "UPDATE %s SET m = {} WHERE k = 0 IF m CONTAINS KEY ?", "foo"),
                   row(False, None))
        assertRows(execute(cql, table, "UPDATE %s SET m = {} WHERE k = 0 IF m CONTAINS KEY ?", unset()),
                   row(False, None))

        assertInvalidMessage(cql, table, "Invalid comparison with null for operator \"CONTAINS\"",
                             "UPDATE %s SET m = {} WHERE k = 0 IF m CONTAINS ?", None)
        assertInvalidMessage(cql, table, "Invalid comparison with null for operator \"CONTAINS KEY\"",
                             "UPDATE %s SET m = {} WHERE k = 0 IF m CONTAINS KEY ?", None)
