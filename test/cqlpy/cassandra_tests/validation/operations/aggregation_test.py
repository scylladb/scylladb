# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1


from ...porting import *
from decimal import Decimal
from cassandra.protocol import FunctionFailure
import datetime

# Scylla's error messages for dropping a non-existent aggregate are
# different from Cassandra's: "No function named ks.f found" when no
# argument types are given, and "User function ks.f(int, text) doesn't exist"
# when they are. So we accept either.
def doesntExistMessage(name):
    return re.escape(f"Aggregate '{name}' doesn't exist") + "|" + re.escape(f"No function named {name} found") + "|" + re.escape(f"User function {name} doesn't exist")

def testNonExistingOnes(cql, test_keyspace):
    assert_invalid_throw_message_re(cql, test_keyspace, doesntExistMessage(f"{test_keyspace}.aggr_does_not_exist"),
                              InvalidRequest,
                              "DROP AGGREGATE " + test_keyspace + ".aggr_does_not_exist")

    assert_invalid_throw_message_re(cql, test_keyspace, doesntExistMessage(f"{test_keyspace}.aggr_does_not_exist(int, text)"),
                              InvalidRequest,
                              "DROP AGGREGATE " + test_keyspace + ".aggr_does_not_exist(int,text)")

    assert_invalid_throw_message_re(cql, test_keyspace, doesntExistMessage("keyspace_does_not_exist.aggr_does_not_exist"),
                              InvalidRequest,
                              "DROP AGGREGATE keyspace_does_not_exist.aggr_does_not_exist")

    assert_invalid_throw_message_re(cql, test_keyspace, doesntExistMessage("keyspace_does_not_exist.aggr_does_not_exist(int, text)"),
                              InvalidRequest,
                              "DROP AGGREGATE keyspace_does_not_exist.aggr_does_not_exist(int,text)")

    execute(cql, test_keyspace, "DROP AGGREGATE IF EXISTS " + test_keyspace + ".aggr_does_not_exist")
    execute(cql, test_keyspace, "DROP AGGREGATE IF EXISTS " + test_keyspace + ".aggr_does_not_exist(int,text)")
    execute(cql, test_keyspace, "DROP AGGREGATE IF EXISTS keyspace_does_not_exist.aggr_does_not_exist")
    execute(cql, test_keyspace, "DROP AGGREGATE IF EXISTS keyspace_does_not_exist.aggr_does_not_exist(int,text)")

def testFunctions(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c double, d decimal, e smallint, f tinyint, primary key (a, b))") as table:
        # Test with empty table
        with original_column_names(cql):
            assert_column_names(execute(cql, table, "SELECT COUNT(*) FROM %s"), "count")
        assert_rows(execute(cql, table, "SELECT COUNT(*) FROM %s"), row(0))
        with original_column_names(cql):
            assert_column_names(execute(cql, table, "SELECT max(b), min(b), sum(b), avg(b)," +
                                  "max(c), sum(c), avg(c)," +
                                  "sum(d), avg(d)," +
                                  "max(e), min(e), sum(e), avg(e)," +
                                  "max(f), min(f), sum(f), avg(f) FROM %s"),
                          "system.max(b)", "system.min(b)", "system.sum(b)", "system.avg(b)",
                          "system.max(c)", "system.sum(c)", "system.avg(c)",
                          "system.sum(d)", "system.avg(d)",
                          "system.max(e)", "system.min(e)", "system.sum(e)", "system.avg(e)",
                          "system.max(f)", "system.min(f)", "system.sum(f)", "system.avg(f)")
        assert_rows(execute(cql, table, "SELECT max(b), min(b), sum(b), avg(b)," +
                           "max(c), sum(c), avg(c)," +
                           "sum(d), avg(d)," +
                           "max(e), min(e), sum(e), avg(e)," +
                           "max(f), min(f), sum(f), avg(f) FROM %s"),
                   row(null, null, 0, 0, null, 0.0, 0.0, Decimal("0"), Decimal("0"),
                       null, null, 0, 0,
                       null, null, 0, 0))

        execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 1, 11.5, 11.5, 1, 1)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 2, 9.5, 1.5, 2, 2)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 3, 9.0, 2.0, 3, 3)")

        assert_rows(execute(cql, table, "SELECT max(b), min(b), sum(b), avg(b) , max(c), sum(c), avg(c), sum(d), avg(d)," +
                           "max(e), min(e), sum(e), avg(e)," +
                           "max(f), min(f), sum(f), avg(f)" +
                           " FROM %s"),
                   row(3, 1, 6, 2, 11.5, 30.0, 10.0, Decimal("15.0"), Decimal("5.0"),
                       3, 1, 6, 2,
                       3, 1, 6, 2))

        execute(cql, table, "INSERT INTO %s (a, b, d) VALUES (1, 5, 1.0)")
        assert_rows(execute(cql, table, "SELECT COUNT(*) FROM %s"), row(4))
        assert_rows(execute(cql, table, "SELECT COUNT(1) FROM %s"), row(4))
        assert_rows(execute(cql, table, "SELECT COUNT(b), count(c), count(e), count(f) FROM %s"), row(4, 3, 3, 3))
        # Makes sure that LIMIT does not affect the result of aggregates
        assert_rows(execute(cql, table, "SELECT COUNT(b), count(c), count(e), count(f) FROM %s LIMIT 2"), row(4, 3, 3, 3))
        assert_rows(execute(cql, table, "SELECT COUNT(b), count(c), count(e), count(f) FROM %s WHERE a = 1 LIMIT 2"),
                   row(4, 3, 3, 3))
        assert_rows(execute(cql, table, "SELECT AVG(CAST(b AS double)) FROM %s"), row(11.0/4))

# Reproduces SCYLLADB-4920 (the column name of COUNT(1); also reported as
# SCYLLADB-4777).
@pytest.mark.xfail(reason="SCYLLADB-4920")
def testCountStarFunction(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c double, primary key (a, b))") as table:
        # Test with empty table
        with original_column_names(cql):
            assert_column_names(execute(cql, table, "SELECT COUNT(*) FROM %s"), "count")
        assert_rows(execute(cql, table, "SELECT COUNT(*) FROM %s"), row(0))
        with original_column_names(cql):
            assert_column_names(execute(cql, table, "SELECT COUNT(1) FROM %s"), "count")
        assert_rows(execute(cql, table, "SELECT COUNT(1) FROM %s"), row(0))
        # Both columns are called "count", so we can't use assert_column_names()
        # which reads the names from a row (that can't have the same key twice),
        # and check the result's column_names instead.
        assert execute(cql, table, "SELECT COUNT(*), COUNT(*) FROM %s").column_names == ["count", "count"]
        assert_rows(execute(cql, table, "SELECT COUNT(*), COUNT(*) FROM %s"), row(0, 0))

        # Test with alias
        with original_column_names(cql):
            assert_column_names(execute(cql, table, "SELECT COUNT(*) as myCount FROM %s"), "mycount")
        assert_rows(execute(cql, table, "SELECT COUNT(*) as myCount FROM %s"), row(0))
        with original_column_names(cql):
            assert_column_names(execute(cql, table, "SELECT COUNT(1) as myCount FROM %s"), "mycount")
        assert_rows(execute(cql, table, "SELECT COUNT(1) as myCount FROM %s"), row(0))

        # Test with other aggregates
        with original_column_names(cql):
            assert_column_names(execute(cql, table, "SELECT COUNT(*), max(b), b FROM %s"), "count", "system.max(b)", "b")
        assert_rows(execute(cql, table, "SELECT COUNT(*), max(b), b  FROM %s"), row(0, null, null))
        with original_column_names(cql):
            assert_column_names(execute(cql, table, "SELECT COUNT(1), max(b), b FROM %s"), "count", "system.max(b)", "b")
        assert_rows(execute(cql, table, "SELECT COUNT(1), max(b), b  FROM %s"), row(0, null, null))

        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 1, 11.5)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 2, 9.5)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 3, 9.0)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 5, 1.0)")

        assert_rows(execute(cql, table, "SELECT COUNT(*) FROM %s"), row(4))
        assert_rows(execute(cql, table, "SELECT COUNT(1) FROM %s"), row(4))
        assert_rows(execute(cql, table, "SELECT max(b), b, COUNT(*) FROM %s"), row(5, 1, 4))
        assert_rows(execute(cql, table, "SELECT max(b), COUNT(1), b FROM %s"), row(5, 4, 1))
        # Makes sure that LIMIT does not affect the result of aggregates
        assert_rows(execute(cql, table, "SELECT max(b), COUNT(1), b FROM %s LIMIT 2"), row(5, 4, 1))
        assert_rows(execute(cql, table, "SELECT max(b), COUNT(1), b FROM %s WHERE a = 1 LIMIT 2"), row(5, 4, 1))

def testMaxAggregationDescending(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, primary key (a, b)) WITH CLUSTERING ORDER BY (b DESC)") as table:
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (1, 1000)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (1, 100)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (1, 1)")

        assert_rows(execute(cql, table, "SELECT count(b), max(b) as max FROM %s WHERE a = 1"),
                   row(3, 1000))

        execute(cql, table, "INSERT INTO %s (a, b) VALUES (2, 4000)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (3, 100)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (4, 0)")

        assert_rows(execute(cql, table, "SELECT count(b), max(b) as max FROM %s"),
                   row(6, 4000))

def testMinAggregationDescending(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, primary key (a, b)) WITH CLUSTERING ORDER BY (b DESC)") as table:
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (1, 1000)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (1, 100)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (1, 1)")

        assert_rows(execute(cql, table, "SELECT count(b), min(b) as min FROM %s WHERE a = 1"),
                   row(3, 1))

        execute(cql, table, "INSERT INTO %s (a, b) VALUES (2, 4000)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (3, 100)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (4, 0)")

        assert_rows(execute(cql, table, "SELECT count(b), min(b) as min FROM %s"),
                   row(6, 0))

def testMaxAggregationAscending(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, primary key (a, b)) WITH CLUSTERING ORDER BY (b ASC)") as table:
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (1, 1000)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (1, 100)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (1, 1)")

        assert_rows(execute(cql, table, "SELECT count(b), max(b) as max FROM %s WHERE a = 1"),
                   row(3, 1000))

        execute(cql, table, "INSERT INTO %s (a, b) VALUES (2, 4000)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (3, 100)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (4, 5)")

        assert_rows(execute(cql, table, "SELECT count(b), max(b) as max FROM %s"),
                   row(6, 4000))

def testMinAggregationAscending(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, primary key (a, b)) WITH CLUSTERING ORDER BY (b ASC)") as table:
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (1, 1000)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (1, 100)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (1, 1)")

        assert_rows(execute(cql, table, "SELECT count(b), min(b) as min FROM %s WHERE a = 1"),
                   row(3, 1))

        execute(cql, table, "INSERT INTO %s (a, b) VALUES (2, 4000)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (3, 100)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (4, 0)")

        assert_rows(execute(cql, table, "SELECT count(b), min(b) as min FROM %s"),
                   row(6, 0))

def testAggregateWithColumns(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, primary key (a, b))") as table:
        # Test with empty table
        with original_column_names(cql):
            assert_column_names(execute(cql, table, "SELECT count(b), max(b) as max, b, c as first FROM %s"),
                          "system.count(b)", "max", "b", "first")
        assert_rows(execute(cql, table, "SELECT count(b), max(b) as max, b, c as first FROM %s"),
                           row(0, null, null, null))

        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 2, null)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (2, 4, 6)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (4, 8, 12)")

        assert_rows(execute(cql, table, "SELECT count(b), max(b) as max, b, c as first FROM %s"),
                   row(3, 8, 2, null))

def testAggregateOnCounters(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b counter, primary key (a))") as table:
        # Test with empty table
        with original_column_names(cql):
            assert_column_names(execute(cql, table, "SELECT count(b), max(b) as max, b FROM %s"),
                          "system.count(b)", "max", "b")
        assert_rows(execute(cql, table, "SELECT count(b), max(b) as max, b FROM %s"),
                   row(0, null, null))

        execute(cql, table, "UPDATE %s SET b = b + 1 WHERE a = 1")
        execute(cql, table, "UPDATE %s SET b = b + 1 WHERE a = 1")

        assert_rows(execute(cql, table, "SELECT count(b), max(b) as max, min(b) as min, avg(b) as avg, sum(b) as sum FROM %s"),
                   row(1, 2, 2, 2, 2))
        flush(cql, table)
        assert_rows(execute(cql, table, "SELECT count(b), max(b) as max, min(b) as min, avg(b) as avg, sum(b) as sum FROM %s"),
                   row(1, 2, 2, 2, 2))

        execute(cql, table, "UPDATE %s SET b = b + 2 WHERE a = 1")

        assert_rows(execute(cql, table, "SELECT count(b), max(b) as max, min(b) as min, avg(b) as avg, sum(b) as sum FROM %s"),
                   row(1, 4, 4, 4, 4))

        execute(cql, table, "UPDATE %s SET b = b - 2 WHERE a = 1")

        assert_rows(execute(cql, table, "SELECT count(b), max(b) as max, min(b) as min, avg(b) as avg, sum(b) as sum FROM %s"),
                   row(1, 2, 2, 2, 2))
        flush(cql, table)
        assert_rows(execute(cql, table, "SELECT count(b), max(b) as max, min(b) as min, avg(b) as avg, sum(b) as sum FROM %s"),
                   row(1, 2, 2, 2, 2))

        execute(cql, table, "UPDATE %s SET b = b + 1 WHERE a = 2")
        execute(cql, table, "UPDATE %s SET b = b + 1 WHERE a = 2")
        execute(cql, table, "UPDATE %s SET b = b + 2 WHERE a = 2")

        assert_rows(execute(cql, table, "SELECT count(b), max(b) as max, min(b) as min, avg(b) as avg, sum(b) as sum FROM %s"),
                   row(2, 4, 2, 3, 6))

def testAggregateWithSets(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, s set<int>, fs frozen<set<int>>)") as table:
        # Test with empty table
        select = "SELECT count(s), count(fs), min(s), min(fs), max(s), max(fs) FROM %s"
        with original_column_names(cql):
            assert_column_names(execute(cql, table, select),
                          "system.count(s)", "system.count(fs)",
                          "system.min(s)", "system.min(fs)",
                          "system.max(s)", "system.max(fs)")
        assert_rows(execute(cql, table, select), row(0, 0, null, null, null, null))

        # Test with not-empty table
        execute(cql, table, "INSERT INTO %s (k, s, fs) VALUES (1, {1, 2}, {1, 2})")
        execute(cql, table, "INSERT INTO %s (k, s, fs) VALUES (2, {1, 2, 3}, {1, 2, 3})")
        execute(cql, table, "INSERT INTO %s (k, s, fs) VALUES (3, {2, 1}, {2, 1})")
        assert_rows(execute(cql, table, select), row(3, 3, {1, 2}, {1, 2}, {1, 2, 3}, {1, 2, 3}))

def testAggregateWithLists(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, l list<int>, fl frozen<list<int>>)") as table:
        # Test with empty table
        select = "SELECT count(l), count(fl), min(l), min(fl), max(l), max(fl) FROM %s"
        with original_column_names(cql):
            assert_column_names(execute(cql, table, select),
                          "system.count(l)", "system.count(fl)",
                          "system.min(l)", "system.min(fl)",
                          "system.max(l)", "system.max(fl)")
        assert_rows(execute(cql, table, select), row(0, 0, null, null, null, null))

        # Test with not-empty table
        execute(cql, table, "INSERT INTO %s (k, l, fl) VALUES (1, [1, 2], [1, 2])")
        execute(cql, table, "INSERT INTO %s (k, l, fl) VALUES (2, [1, 2, 3], [1, 2, 3])")
        execute(cql, table, "INSERT INTO %s (k, l, fl) VALUES (3, [2, 1], [2, 1])")
        assert_rows(execute(cql, table, select),
                   row(3, 3, [1, 2], [1, 2], [2, 1], [2, 1]))

def testAggregateWithMaps(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, m map<int, int>, fm frozen<map<int, int>>)") as table:
        # Test with empty table
        select = "SELECT count(m), count(fm), min(m), min(fm), max(m), max(fm) FROM %s"
        with original_column_names(cql):
            assert_column_names(execute(cql, table, select),
                          "system.count(m)", "system.count(fm)",
                          "system.min(m)", "system.min(fm)",
                          "system.max(m)", "system.max(fm)")
        assert_rows(execute(cql, table, select), row(0, 0, null, null, null, null))

        # Test with not-empty table
        execute(cql, table, "INSERT INTO %s (k, m, fm) VALUES (1, {1:10, 2:20}, {1:10, 2:20})")
        execute(cql, table, "INSERT INTO %s (k, m, fm) VALUES (2, {1:10, 2:20, 3:30}, {1:10, 2:20, 3:30})")
        execute(cql, table, "INSERT INTO %s (k, m, fm) VALUES (3, {2:20, 1:10}, {2:20, 1:10})")
        assert_rows(execute(cql, table, select),
                   row(3, 3,
                       {1: 10, 2: 20}, {1: 10, 2: 20},
                       {1: 10, 2: 20, 3: 30}, {1: 10, 2: 20, 3: 30}))

def testAggregateWithTuples(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, t tuple<int, text, boolean>)") as table:
        # Test with empty table
        select = "SELECT count(t), min(t), max(t) FROM %s"
        with original_column_names(cql):
            assert_column_names(execute(cql, table, select), "system.count(t)", "system.min(t)", "system.max(t)")
        assert_rows(execute(cql, table, select), row(0, null, null))

        # Test with not-empty table
        execute(cql, table, "INSERT INTO %s (k, t) VALUES (1, (1, 'a', false))")
        execute(cql, table, "INSERT INTO %s (k, t) VALUES (2, (2, 'b', true))")
        execute(cql, table, "INSERT INTO %s (k, t) VALUES (3, (3, null, true))")
        assert_rows(execute(cql, table, select), row(3, (1, "a", False), (3, None, True)))

def testAggregateWithUDTs(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(x int)") as udt:
        with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, u frozen<{udt}>, fu frozen<{udt}>)") as table:
            # Test with empty table
            select = "SELECT count(u), count(fu), min(u), min(fu), max(u), max(fu) FROM %s"
            with original_column_names(cql):
                assert_column_names(execute(cql, table, select),
                              "system.count(u)", "system.count(fu)",
                              "system.min(u)", "system.min(fu)",
                              "system.max(u)", "system.max(fu)")
            assert_rows(execute(cql, table, select), row(0, 0, null, null, null, null))

            # Test with not-empty table
            execute(cql, table, "INSERT INTO %s (k, u, fu) VALUES (1, {x: 2}, null)")
            execute(cql, table, "INSERT INTO %s (k, u, fu) VALUES (2, {x: 4}, {x: 6})")
            execute(cql, table, "INSERT INTO %s (k, u, fu) VALUES (3, null, {x: 8})")
            assert_rows(execute(cql, table, select),
                       row(2, 2, user_type("x", 2), user_type("x", 6), user_type("x", 4), user_type("x", 8)))

def testAggregateWithUdtFields(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(x int)") as myType:
        with create_table(cql, test_keyspace, f"(a int primary key, b frozen<{myType}>, c frozen<{myType}>)") as table:
            # Test with empty table
            with original_column_names(cql):
                assert_column_names(execute(cql, table, "SELECT count(b.x), max(b.x) as max, b.x, c.x as first FROM %s"),
                              "system.count(b.x)", "max", "b.x", "first")
            assert_rows(execute(cql, table, "SELECT count(b.x), max(b.x) as max, b.x, c.x as first FROM %s"),
                               row(0, null, null, null))

            execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, {x:2}, null)")
            execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (2, {x:4}, {x:6})")
            execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (4, {x:8}, {x:12})")

            assert_rows(execute(cql, table, "SELECT count(b.x), max(b.x) as max, b.x, c.x as first FROM %s"),
                       row(3, 8, 2, null))

            assert_rows(execute(cql, table, "SELECT count(b), min(b).x, max(b).x, count(c), min(c).x, max(c).x FROM %s"),
                       row(3, 2, 8, 2, 6, 12))

COPY_SIGN_JAVA = "return Double.valueOf(Math.copySign(magnitude, sign));"
# (Scylla's Lua environment doesn't include Lua's "math" library)
COPY_SIGN_LUA = "if magnitude < 0 then magnitude = -magnitude end if sign < 0 then return -magnitude else return magnitude end"

def testAggregateWithFunctions(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b double, c double, primary key(a, b))") as table:
        with create_function(cql, test_keyspace, "(magnitude double, sign double) " +
                                     "RETURNS NULL ON NULL INPUT " +
                                     "RETURNS double " +
                                     java_or_lua(cql, COPY_SIGN_JAVA, COPY_SIGN_LUA)) as copySign:
            # Test with empty table
            with original_column_names(cql):
                assert_column_names(execute(cql, table, "SELECT count(b), max(b) as max, " + copySign + "(b, c), " + copySign + "(c, b) as first FROM %s"),
                              "system.count(b)", "max", copySign + "(b, c)", "first")
            assert_rows(execute(cql, table, "SELECT count(b), max(b) as max, " + copySign + "(b, c), " + copySign + "(c, b) as first FROM %s"),
                               row(0, null, null, null))

            execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (0, -1.2, 2.1)")
            execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (0, 1.3, -3.4)")
            execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (0, 1.4, 1.2)")

            assert_rows(execute(cql, table, "SELECT count(b), max(b) as max, " + copySign + "(b, c), " + copySign + "(c, b) as first FROM %s"),
                       row(3, 1.4, 1.2, -2.1))

            execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, -1.2, null)")
            execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 1.3, -3.4)")
            execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 1.4, 1.2)")
            assert_rows(execute(cql, table, "SELECT count(b), max(b) as max, " + copySign + "(b, c), " + copySign + "(c, b) as first FROM %s WHERE a = 1"),
                       row(3, 1.4, null, null))

def testAggregateWithWriteTimeOrTTL(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int primary key, b int, c int)") as table:
        # Test with empty table
        with original_column_names(cql):
            assert_column_names(execute(cql, table, "SELECT count(writetime(b)), min(ttl(b)) as min, writetime(b), ttl(c) as first FROM %s"),
                          "system.count(writetime(b))", "min", "writetime(b)", "first")
        assert_rows(execute(cql, table, "SELECT count(writetime(b)), min(ttl(b)) as min, writetime(b), ttl(c) as first FROM %s"),
                           row(0, null, null, null))

        today = int(time.time() * 1000) * 1000
        yesterday = today - (24 * 60 * 60 * 1000 * 1000)

        secondsPerMinute = 60
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 2, null) USING TTL " + str(20 * secondsPerMinute))
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (2, 4, 6) USING TTL " + str(10 * secondsPerMinute))
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (4, 8, 12) USING TIMESTAMP " + str(yesterday))

        assert_rows(execute(cql, table, "SELECT count(writetime(b)), count(ttl(b)) FROM %s"),
                   row(3, 2))

        with original_column_names(cql):
            resultSet = list(execute(cql, table, "SELECT min(ttl(b)), ttl(b) FROM %s"))
            assert 1 == len(resultSet)
            r = resultSet[0]
            assert r["ttl(b)"] > (10 * secondsPerMinute)
            assert r["system.min(ttl(b))"] <= (10 * secondsPerMinute)

            resultSet = list(execute(cql, table, "SELECT min(writetime(b)), writetime(b) FROM %s"))
            assert 1 == len(resultSet)
            r = resultSet[0]

            assert r["writetime(b)"] >= today
            assert r["system.min(writetime(b))"] == yesterday

def testInvalidCalls(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, primary key (a, b))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 1, 10)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 2, 9)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 3, 8)")

        # Cassandra considers this a syntax error, while Scylla gives a clearer
        # error message: "Aggregation functions are not supported in the WHERE
        # clause". We accept either.
        assert_invalid_throw(cql, table, (SyntaxException, InvalidRequest), "SELECT max(b), max(c) FROM %s WHERE max(a) = 1")
        # Scylla's error message is different from Cassandra's, so we accept
        # either.
        assert_invalid_message_re(cql, table, re.escape("aggregate functions cannot be used as arguments of aggregate functions") + "|" +
                                  re.escape("SELECT clause contains aggeregation of an aggregation"), "SELECT max(sum(c)) FROM %s")

def testReversedType(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, primary key (a, b)) WITH CLUSTERING ORDER BY (b DESC)") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 1, 10)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 2, 9)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 3, 8)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1, 4, 7)")

        assert_rows(execute(cql, table, "SELECT max(c), min(c), avg(c) FROM %s WHERE a = 1 AND b > 1"), row(9, 7, 8))

# Reproduces SCYLLADB-5141 (snake_case names of native functions).
@pytest.mark.xfail(reason="SCYLLADB-5141")
def testNestedFunctions(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int primary key, b timeuuid, c double, d double)") as table:
        with create_function(cql, test_keyspace, "(magnitude double, sign double) " +
                                     "RETURNS NULL ON NULL INPUT " +
                                     "RETURNS double " +
                                     java_or_lua(cql, COPY_SIGN_JAVA, COPY_SIGN_LUA)) as copySign:
            with original_column_names(cql):
                assert_column_names(execute(cql, table, "SELECT max(a), max(to_unix_timestamp(b)) FROM %s"), "system.max(a)", "system.max(system.to_unix_timestamp(b))")
            assert_rows(execute(cql, table, "SELECT max(a), max(to_unix_timestamp(b)) FROM %s"), row(null, null))
            with original_column_names(cql):
                assert_column_names(execute(cql, table, "SELECT max(a), to_unix_timestamp(max(b)) FROM %s"), "system.max(a)", "system.to_unix_timestamp(system.max(b))")
            assert_rows(execute(cql, table, "SELECT max(a), to_unix_timestamp(max(b)) FROM %s"), row(null, null))

            with original_column_names(cql):
                assert_column_names(execute(cql, table, "SELECT max(" + copySign + "(c, d)) FROM %s"), "system.max(" + copySign + "(c, d))")
            assert_rows(execute(cql, table, "SELECT max(" + copySign + "(c, d)) FROM %s"), row(null))

            execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (1, max_timeuuid('2011-02-03 04:05:00+0000'), -1.2, 2.1)")
            execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (2, max_timeuuid('2011-02-03 04:06:00+0000'), 1.3, -3.4)")
            execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (3, max_timeuuid('2011-02-03 04:10:00+0000'), 1.4, 1.2)")

            date = int(datetime.datetime(2011, 2, 3, 4, 10, 0, tzinfo=datetime.timezone.utc).timestamp() * 1000)

            assert_rows(execute(cql, table, "SELECT max(a), max(to_unix_timestamp(b)) FROM %s"), row(3, date))
            assert_rows(execute(cql, table, "SELECT max(a), to_unix_timestamp(max(b)) FROM %s"), row(3, date))

            assert_rows(execute(cql, table, "SELECT " + copySign + "(max(c), min(c)) FROM %s"), row(-1.4))
            assert_rows(execute(cql, table, "SELECT " + copySign + "(c, d) FROM %s"), row(1.2), row(-1.3), row(1.4))
            assert_rows(execute(cql, table, "SELECT max(" + copySign + "(c, d)) FROM %s"), row(1.4))
            assert_rows(execute(cql, table, "SELECT " + copySign + "(c, max(c)) FROM %s"), row(1.2))
            assert_rows(execute(cql, table, "SELECT " + copySign + "(max(c), c) FROM %s"), row(-1.4))

# The following tests create user-defined functions and aggregates. To make
# cleanup easy - even when a test drops some of them itself - each such test
# creates them in a new keyspace of its own, using the following helpers
# which are similar to those of Cassandra's CQLTester: The "%s" in the
# given statement is replaced by a new unique name in the given keyspace,
# and that full name is returned.
REPLICATION = "replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"

def createFunction(cql, keyspace, query):
    name = keyspace + "." + unique_name()
    cql.execute(query.replace("%s", name, 1))
    return name

def createFunctionOverload(cql, name, query):
    cql.execute(query.replace("%s", name, 1))

def createAggregate(cql, keyspace, query):
    return createFunction(cql, keyspace, query)

def shortFunctionName(name):
    return name.split(".")[1]

# In several cases, Scylla's error messages are different from Cassandra's,
# so the tests below accept either:
MULTIPLE_FUNCTIONS_MESSAGE = "matches multiple function definitions|There are multiple functions named"
FUNCTION_DOESNT_EXIST_MESSAGE = "doesn't exist|not found"
STILL_REFERENCED_MESSAGE = "still referenced by|as it is used by user-defined aggregate"
NOT_AN_AGGREGATE_MESSAGE = "doesn't exist|is not a user defined aggregate"
NOT_A_FUNCTION_MESSAGE = "doesn't exist|is not a user defined function"
NOT_A_SCALAR_FUNCTION_MESSAGE = "isn't a scalar function|not found"
WRONG_STATE_TYPE_MESSAGE = re.escape("return type must be the same as the first argument type - check STYPE, argument and return types") + "|doesn't return state"

# Java bodies of some functions used in several tests below, and the
# equivalent Lua bodies we use on Scylla.
SUM_STATE_JAVA = "return Integer.valueOf((a!=null?a.intValue():0) + b.intValue());"
SUM_STATE_LUA = "if a == nil then a = 0 end return a + b"
TO_STRING_JAVA = "return a.toString();"
TO_STRING_LUA = "return tostring(a)"

# The test testSchemaChange was not translated, because it checks the schema
# change events that the server sends, using Cassandra's internal APIs.

def testDropStatements(cql):
    with create_keyspace(cql, REPLICATION) as ks:
        f = createFunction(cql, ks,
                           "CREATE OR REPLACE FUNCTION %s(state double, val double) " +
                           "RETURNS NULL ON NULL INPUT " +
                           "RETURNS double " +
                           java_or_lua(cql, "return state;", "return state"))

        createFunctionOverload(cql, f,
                               "CREATE OR REPLACE FUNCTION %s(state int, val int) " +
                               "RETURNS NULL ON NULL INPUT " +
                               "RETURNS int " +
                               java_or_lua(cql, " return state;", "return state"))

        # DROP AGGREGATE must not succeed against a scalar
        assert_invalid_message_re(cql, ks, MULTIPLE_FUNCTIONS_MESSAGE, "DROP AGGREGATE " + f)
        assert_invalid_message_re(cql, ks, NOT_AN_AGGREGATE_MESSAGE, "DROP AGGREGATE " + f + "(double, double)")

        a = createAggregate(cql, ks,
                            "CREATE OR REPLACE AGGREGATE %s(double) " +
                            "SFUNC " + shortFunctionName(f) + " " +
                            "STYPE double " +
                            "INITCOND 0")
        createFunctionOverload(cql, a,
                               "CREATE OR REPLACE AGGREGATE %s(int) " +
                               "SFUNC " + shortFunctionName(f) + " " +
                               "STYPE int " +
                               "INITCOND 0")

        # DROP FUNCTION must not succeed against an aggregate
        assert_invalid_message_re(cql, ks, MULTIPLE_FUNCTIONS_MESSAGE, "DROP FUNCTION " + a)
        assert_invalid_message_re(cql, ks, NOT_A_FUNCTION_MESSAGE, "DROP FUNCTION " + a + "(double)")

        # ambigious
        assert_invalid_message_re(cql, ks, MULTIPLE_FUNCTIONS_MESSAGE, "DROP AGGREGATE " + a)
        assert_invalid_message_re(cql, ks, MULTIPLE_FUNCTIONS_MESSAGE, "DROP AGGREGATE IF EXISTS " + a)

        execute(cql, ks, "DROP AGGREGATE IF EXISTS " + ks + ".non_existing")
        execute(cql, ks, "DROP AGGREGATE IF EXISTS " + a + "(int, text)")

        execute(cql, ks, "DROP AGGREGATE " + a + "(double)")

        execute(cql, ks, "DROP AGGREGATE IF EXISTS " + a + "(double)")

def testDropReferenced(cql):
    with create_keyspace(cql, REPLICATION) as ks:
        f = createFunction(cql, ks,
                           "CREATE OR REPLACE FUNCTION %s(state double, val double) " +
                           "RETURNS NULL ON NULL INPUT " +
                           "RETURNS double " +
                           java_or_lua(cql, " return state;", "return state"))

        a = createAggregate(cql, ks,
                            "CREATE OR REPLACE AGGREGATE %s(double) " +
                            "SFUNC " + shortFunctionName(f) + " " +
                            "STYPE double " +
                            "INITCOND 0")

        # DROP FUNCTION must not succeed because the function is still referenced by the aggregate
        assert_invalid_message_re(cql, ks, STILL_REFERENCED_MESSAGE, "DROP FUNCTION " + f)

        execute(cql, ks, "DROP AGGREGATE " + a + "(double)")

def testJavaAggregateNoInit(cql):
    with create_keyspace(cql, REPLICATION) as ks, create_table(cql, ks, "(a int primary key, b int)") as table:
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (1, 1)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (2, 2)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (3, 3)")

        fState = createFunction(cql, ks,
                                "CREATE FUNCTION %s(a int, b int) " +
                                "CALLED ON NULL INPUT " +
                                "RETURNS int " +
                                java_or_lua(cql, SUM_STATE_JAVA, SUM_STATE_LUA))

        fFinal = createFunction(cql, ks,
                                "CREATE FUNCTION %s(a int) " +
                                "CALLED ON NULL INPUT " +
                                "RETURNS text " +
                                java_or_lua(cql, TO_STRING_JAVA, TO_STRING_LUA))

        a = createAggregate(cql, ks,
                            "CREATE AGGREGATE %s(int) " +
                            "SFUNC " + shortFunctionName(fState) + " " +
                            "STYPE int " +
                            "FINALFUNC " + shortFunctionName(fFinal))

        # 1 + 2 + 3 = 6
        assert_rows(execute(cql, table, "SELECT " + a + "(b) FROM %s"), row("6"))

        execute(cql, table, "DROP AGGREGATE " + a + "(int)")

        assert_invalid_message(cql, table, "Unknown function", "SELECT " + a + "(b) FROM %s")

def testJavaAggregateNullInitcond(cql):
    with create_keyspace(cql, REPLICATION) as ks, create_table(cql, ks, "(a int primary key, b int)") as table:
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (1, 1)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (2, 2)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (3, 3)")

        fState = createFunction(cql, ks,
                                "CREATE FUNCTION %s(a int, b int) " +
                                "CALLED ON NULL INPUT " +
                                "RETURNS int " +
                                java_or_lua(cql, SUM_STATE_JAVA, SUM_STATE_LUA))

        fFinal = createFunction(cql, ks,
                                "CREATE FUNCTION %s(a int) " +
                                "CALLED ON NULL INPUT " +
                                "RETURNS text " +
                                java_or_lua(cql, TO_STRING_JAVA, TO_STRING_LUA))

        a = createAggregate(cql, ks,
                            "CREATE AGGREGATE %s(int) " +
                            "SFUNC " + shortFunctionName(fState) + " " +
                            "STYPE int " +
                            "FINALFUNC " + shortFunctionName(fFinal) + " " +
                            "INITCOND null")

        # 1 + 2 + 3 = 6
        assert_rows(execute(cql, table, "SELECT " + a + "(b) FROM %s"), row("6"))

        execute(cql, table, "DROP AGGREGATE " + a + "(int)")

        assert_invalid_message(cql, table, "Unknown function", "SELECT " + a + "(b) FROM %s")

def testJavaAggregateInvalidInitcond(cql):
    with create_keyspace(cql, REPLICATION) as ks:
        fState = createFunction(cql, ks,
                                "CREATE FUNCTION %s(a int, b int) " +
                                "CALLED ON NULL INPUT " +
                                "RETURNS int " +
                                java_or_lua(cql, SUM_STATE_JAVA, SUM_STATE_LUA))

        fFinal = createFunction(cql, ks,
                                "CREATE FUNCTION %s(a int) " +
                                "CALLED ON NULL INPUT " +
                                "RETURNS text " +
                                java_or_lua(cql, TO_STRING_JAVA, TO_STRING_LUA))

        assert_invalid_message(cql, ks, "Invalid STRING constant (foobar)",
                             "CREATE AGGREGATE " + ks + ".aggrInvalid(int)" +
                             "SFUNC " + shortFunctionName(fState) + " " +
                             "STYPE int " +
                             "FINALFUNC " + shortFunctionName(fFinal) + " " +
                             "INITCOND 'foobar'")

# This test fails on Cassandra because of a Cassandra bug: when an authorizer
# is enabled (as in our test setup), CREATE AGGREGATE with a non-existent
# SFUNC or FINALFUNC fails with a NoSuchElementException server error,
# instead of an InvalidRequest. This is CASSANDRA-21734.
def testJavaAggregateIncompatibleTypes(cql, cassandra_bug):
    with create_keyspace(cql, REPLICATION) as ks:
        fState = createFunction(cql, ks,
                                "CREATE FUNCTION %s(a int, b int) " +
                                "CALLED ON NULL INPUT " +
                                "RETURNS int " +
                                java_or_lua(cql, SUM_STATE_JAVA, SUM_STATE_LUA))

        fFinal = createFunction(cql, ks,
                                "CREATE FUNCTION %s(a int) " +
                                "CALLED ON NULL INPUT " +
                                "RETURNS text " +
                                java_or_lua(cql, TO_STRING_JAVA, TO_STRING_LUA))

        fState2 = createFunction(cql, ks,
                                 "CREATE FUNCTION %s(a double, b double) " +
                                 "CALLED ON NULL INPUT " +
                                 "RETURNS double " +
                                 java_or_lua(cql, "return Double.valueOf((a!=null?a.doubleValue():0d) + b.doubleValue());", SUM_STATE_LUA))

        fFinal2 = createFunction(cql, ks,
                                 "CREATE FUNCTION %s(a double) " +
                                 "CALLED ON NULL INPUT " +
                                 "RETURNS text " +
                                 java_or_lua(cql, TO_STRING_JAVA, TO_STRING_LUA))

        assert_invalid_message_re(cql, ks, FUNCTION_DOESNT_EXIST_MESSAGE,
                             "CREATE AGGREGATE " + ks + ".aggrInvalid(double)" +
                             "SFUNC " + shortFunctionName(fState) + " " +
                             "STYPE double " +
                             "FINALFUNC " + shortFunctionName(fFinal))
        assert_invalid_message_re(cql, ks, FUNCTION_DOESNT_EXIST_MESSAGE,
                             "CREATE AGGREGATE " + ks + ".aggrInvalid(int)" +
                             "SFUNC " + shortFunctionName(fState) + " " +
                             "STYPE double " +
                             "FINALFUNC " + shortFunctionName(fFinal))
        assert_invalid_message_re(cql, ks, FUNCTION_DOESNT_EXIST_MESSAGE,
                             "CREATE AGGREGATE " + ks + ".aggrInvalid(double)" +
                             "SFUNC " + shortFunctionName(fState) + " " +
                             "STYPE int " +
                             "FINALFUNC " + shortFunctionName(fFinal))
        assert_invalid_message_re(cql, ks, FUNCTION_DOESNT_EXIST_MESSAGE,
                             "CREATE AGGREGATE " + ks + ".aggrInvalid(double)" +
                             "SFUNC " + shortFunctionName(fState) + " " +
                             "STYPE int")
        assert_invalid_message_re(cql, ks, FUNCTION_DOESNT_EXIST_MESSAGE,
                             "CREATE AGGREGATE " + ks + ".aggrInvalid(int)" +
                             "SFUNC " + shortFunctionName(fState) + " " +
                             "STYPE double")

        assert_invalid_message_re(cql, ks, FUNCTION_DOESNT_EXIST_MESSAGE,
                             "CREATE AGGREGATE " + ks + ".aggrInvalid(double)" +
                             "SFUNC " + shortFunctionName(fState2) + " " +
                             "STYPE double " +
                             "FINALFUNC " + shortFunctionName(fFinal))

        assert_invalid_message_re(cql, ks, FUNCTION_DOESNT_EXIST_MESSAGE,
                             "CREATE AGGREGATE " + ks + ".aggrInvalid(double)" +
                             "SFUNC " + shortFunctionName(fState) + " " +
                             "STYPE double " +
                             "FINALFUNC " + shortFunctionName(fFinal2))

# This test fails on Cassandra because of a Cassandra bug: when an authorizer
# is enabled (as in our test setup), CREATE AGGREGATE with a non-existent
# SFUNC or FINALFUNC fails with a NoSuchElementException server error,
# instead of an InvalidRequest. This is CASSANDRA-21734.
def testJavaAggregateNonExistingFuncs(cql, cassandra_bug):
    with create_keyspace(cql, REPLICATION) as ks:
        fState = createFunction(cql, ks,
                                "CREATE FUNCTION %s(a int, b int) " +
                                "CALLED ON NULL INPUT " +
                                "RETURNS int " +
                                java_or_lua(cql, SUM_STATE_JAVA, SUM_STATE_LUA))

        fFinal = createFunction(cql, ks,
                                "CREATE FUNCTION %s(a int) " +
                                "CALLED ON NULL INPUT " +
                                "RETURNS text " +
                                java_or_lua(cql, TO_STRING_JAVA, TO_STRING_LUA))

        assert_invalid_message_re(cql, ks, FUNCTION_DOESNT_EXIST_MESSAGE,
                             "CREATE AGGREGATE " + ks + ".aggrInvalid(int)" +
                             "SFUNC " + shortFunctionName(fState) + "_not_there " +
                             "STYPE int " +
                             "FINALFUNC " + shortFunctionName(fFinal))

        assert_invalid_message_re(cql, ks, FUNCTION_DOESNT_EXIST_MESSAGE,
                             "CREATE AGGREGATE " + ks + ".aggrInvalid(int)" +
                             "SFUNC " + shortFunctionName(fState) + " " +
                             "STYPE int " +
                             "FINALFUNC " + shortFunctionName(fFinal) + "_not_there")

        execute(cql, ks, "CREATE AGGREGATE " + ks + ".aggrInvalid(int)" +
                "SFUNC " + shortFunctionName(fState) + " " +
                "STYPE int " +
                "FINALFUNC " + shortFunctionName(fFinal))
        execute(cql, ks, "DROP AGGREGATE " + ks + ".aggrInvalid(int)")

# On Scylla, the failing functions below are written in Lua, so the error
# message doesn't mention java.lang.RuntimeException, but it does include
# the message thrown by the function.
FAILING_FUNCTION_MESSAGE = "java.lang.RuntimeException|thrown to unit test - not a bug"

# Reproduces SCYLLADB-5159 (wrong error code for a failing function)
@pytest.mark.xfail(reason="SCYLLADB-5159")
def testJavaAggregateFailingFuncs(cql):
    with create_keyspace(cql, REPLICATION) as ks, create_table(cql, ks, "(a int primary key, b int)") as table:
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (1, 1)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (2, 2)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (3, 3)")

        fState = createFunction(cql, ks,
                                "CREATE FUNCTION %s(a int, b int) " +
                                "CALLED ON NULL INPUT " +
                                "RETURNS int " +
                                java_or_lua(cql, "throw new RuntimeException(\"thrown to unit test - not a bug\");",
                                            "error(\"thrown to unit test - not a bug\")"))

        fStateOK = createFunction(cql, ks,
                                  "CREATE FUNCTION %s(a int, b int) " +
                                  "CALLED ON NULL INPUT " +
                                  "RETURNS int " +
                                  java_or_lua(cql, "return Integer.valueOf(42);", "return 42"))

        fFinal = createFunction(cql, ks,
                                "CREATE FUNCTION %s(a int) " +
                                "CALLED ON NULL INPUT " +
                                "RETURNS text " +
                                java_or_lua(cql, "throw new RuntimeException(\"thrown to unit test - not a bug\");",
                                            "error(\"thrown to unit test - not a bug\")"))

        fFinalOK = createFunction(cql, ks,
                                  "CREATE FUNCTION %s(a int) " +
                                  "CALLED ON NULL INPUT " +
                                  "RETURNS text " +
                                  java_or_lua(cql, "return \"foobar\";", "return \"foobar\""))

        a0 = createAggregate(cql, ks,
                             "CREATE AGGREGATE %s(int) " +
                             "SFUNC " + shortFunctionName(fState) + " " +
                             "STYPE int " +
                             "FINALFUNC " + shortFunctionName(fFinal) + " " +
                             "INITCOND null")
        a1 = createAggregate(cql, ks,
                             "CREATE AGGREGATE %s(int) " +
                             "SFUNC " + shortFunctionName(fStateOK) + " " +
                             "STYPE int " +
                             "FINALFUNC " + shortFunctionName(fFinal) + " " +
                             "INITCOND null")
        a2 = createAggregate(cql, ks,
                             "CREATE AGGREGATE %s(int) " +
                             "SFUNC " + shortFunctionName(fStateOK) + " " +
                             "STYPE int " +
                             "FINALFUNC " + shortFunctionName(fFinalOK) + " " +
                             "INITCOND null")

        assert_invalid_throw_message_re(cql, table, FAILING_FUNCTION_MESSAGE, FunctionFailure, "SELECT " + a0 + "(b) FROM %s")
        assert_invalid_throw_message_re(cql, table, FAILING_FUNCTION_MESSAGE, FunctionFailure, "SELECT " + a1 + "(b) FROM %s")
        assert_rows(execute(cql, table, "SELECT " + a2 + "(b) FROM %s"), row("foobar"))

# This test fails on Cassandra because of a Cassandra bug: when an authorizer
# is enabled (as in our test setup), CREATE AGGREGATE with a non-existent
# SFUNC or FINALFUNC fails with a NoSuchElementException server error,
# instead of an InvalidRequest. This is CASSANDRA-21734.
def testJavaAggregateWithoutStateOrFinal(cql, cassandra_bug):
    with create_keyspace(cql, REPLICATION) as ks:
        assert_invalid_message_re(cql, ks, FUNCTION_DOESNT_EXIST_MESSAGE,
                             "CREATE AGGREGATE " + ks + ".jSumFooNE1(int) " +
                             "SFUNC jSumFooNEstate " +
                             "STYPE int")

        f = createFunction(cql, ks,
                           "CREATE FUNCTION %s(a int, b int) " +
                           "RETURNS NULL ON NULL INPUT " +
                           "RETURNS int " +
                           java_or_lua(cql, "return Integer.valueOf(a + b);", "return a + b"))

        assert_invalid_message_re(cql, ks, FUNCTION_DOESNT_EXIST_MESSAGE,
                             "CREATE AGGREGATE " + ks + ".jSumFooNE2(int) " +
                             "SFUNC " + shortFunctionName(f) + " " +
                             "STYPE int " +
                             "FINALFUNC jSumFooNEfinal")

        execute(cql, ks, "DROP FUNCTION " + f + "(int, int)")

def testJavaAggregate(cql):
    with create_keyspace(cql, REPLICATION) as ks, create_table(cql, ks, "(a int primary key, b int)") as table:
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (1, 1)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (2, 2)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (3, 3)")

        fState = createFunction(cql, ks,
                                "CREATE FUNCTION %s(a int, b int) " +
                                "CALLED ON NULL INPUT " +
                                "RETURNS int " +
                                java_or_lua(cql, SUM_STATE_JAVA, SUM_STATE_LUA))

        fFinal = createFunction(cql, ks,
                                "CREATE FUNCTION %s(a int) " +
                                "CALLED ON NULL INPUT " +
                                "RETURNS text " +
                                java_or_lua(cql, TO_STRING_JAVA, TO_STRING_LUA))

        a = createAggregate(cql, ks,
                            "CREATE AGGREGATE %s(int) " +
                            "SFUNC " + shortFunctionName(fState) + " " +
                            "STYPE int " +
                            "FINALFUNC " + shortFunctionName(fFinal) + " " +
                            "INITCOND 42")

        assert_rows(execute(cql, table, "SELECT initcond FROM system_schema.aggregates WHERE keyspace_name=? AND aggregate_name=?", ks, shortFunctionName(a)),
                   row("42"))

        # 42 + 1 + 2 + 3 = 48
        assert_rows(execute(cql, table, "SELECT " + a + "(b) FROM %s"), row("48"))

        execute(cql, table, "DROP AGGREGATE " + a + "(int)")

        execute(cql, table, "DROP FUNCTION " + fFinal + "(int)")
        execute(cql, table, "DROP FUNCTION " + fState + "(int, int)")

        assert_invalid_message(cql, table, "Unknown function", "SELECT " + a + "(b) FROM %s")

def testJavaAggregateSimple(cql):
    with create_keyspace(cql, REPLICATION) as ks, create_table(cql, ks, "(a int primary key, b int)") as table:
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (1, 1)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (2, 2)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (3, 3)")

        fState = createFunction(cql, ks,
                                "CREATE FUNCTION %s(a int, b int) " +
                                "CALLED ON NULL INPUT " +
                                "RETURNS int " +
                                java_or_lua(cql, SUM_STATE_JAVA, SUM_STATE_LUA))

        a = createAggregate(cql, ks,
                            "CREATE AGGREGATE %s(int) " +
                            "SFUNC " + shortFunctionName(fState) + " " +
                            "STYPE int")

        # 1 + 2 + 3 = 6
        assert_rows(execute(cql, table, "SELECT " + a + "(b) FROM %s"), row(6))

        execute(cql, table, "DROP AGGREGATE " + a + "(int)")

        execute(cql, table, "DROP FUNCTION " + fState + "(int, int)")

        assert_invalid_message(cql, table, "Unknown function", "SELECT " + a + "(b) FROM %s")

def testJavaAggregateEmpty(cql):
    with create_keyspace(cql, REPLICATION) as ks, create_table(cql, ks, "(a int primary key, b int)") as table:
        fState = createFunction(cql, ks,
                                "CREATE FUNCTION %s(a int, b int) " +
                                "CALLED ON NULL INPUT " +
                                "RETURNS int " +
                                java_or_lua(cql, SUM_STATE_JAVA, SUM_STATE_LUA))

        a = createAggregate(cql, ks,
                            "CREATE AGGREGATE %s(int) " +
                            "SFUNC " + shortFunctionName(fState) + " " +
                            "STYPE int")

        assert_rows(execute(cql, table, "SELECT " + a + "(b) FROM %s"), row(None))

# Reproduces #14404 (an aggregate with an empty collection as INITCOND returns
# null instead of the empty collection).
@pytest.mark.xfail(reason="#14404")
def testJavaAggregateStateEmpty(cql):
    with create_keyspace(cql, REPLICATION) as ks, create_table(cql, ks, "(a int primary key, b uuid)") as table:
        fState = createFunction(cql, ks,
                                "CREATE FUNCTION %s(state map<uuid, int>, type uuid) " +
                                "RETURNS NULL ON NULL INPUT " +
                                "RETURNS map<uuid, int> " +
                                java_or_lua(cql, "return state;", "return state"))

        a = createAggregate(cql, ks,
                            "CREATE AGGREGATE %s(uuid) " +
                            "SFUNC " + shortFunctionName(fState) + " " +
                            "STYPE map<uuid, int> " +
                            "INITCOND {}")

        assert_rows(execute(cql, table, "SELECT " + a + "(b) FROM %s"), row({}))

def testJavaAggregateComplex(cql):
    with create_keyspace(cql, REPLICATION) as ks, create_table(cql, ks, "(a int primary key, b int)") as table:
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (1, 1)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (2, 2)")
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (3, 3)")

        # build an average aggregation function using
        # tuple<bigint,int> as state
        # double as finaltype

        fState = createFunction(cql, ks,
                                "CREATE FUNCTION %s(a tuple<bigint, int>, b int) " +
                                "CALLED ON NULL INPUT " +
                                "RETURNS tuple<bigint, int> " +
                                java_or_lua(cql,
                                    "a.setLong(0, a.getLong(0) + b.intValue());" +
                                    "a.setInt(1, a.getInt(1) + 1);" +
                                    "return a;",
                                    "a[1] = a[1] + b a[2] = a[2] + 1 return a"))

        fFinal = createFunction(cql, ks,
                                "CREATE FUNCTION %s(a tuple<bigint, int>) " +
                                "RETURNS NULL ON NULL INPUT " +
                                "RETURNS double " +
                                java_or_lua(cql,
                                    "double r = a.getLong(0);" +
                                    "r /= a.getInt(1);" +
                                    "return Double.valueOf(r);",
                                    "return a[1] / a[2]"))

        a = createAggregate(cql, ks,
                            "CREATE AGGREGATE %s(int) " +
                            "SFUNC " + shortFunctionName(fState) + " " +
                            "STYPE tuple<bigint, int> "+
                            "FINALFUNC " + shortFunctionName(fFinal) + " " +
                            "INITCOND (0, 0)")

        assert_rows(execute(cql, table, "SELECT initcond FROM system_schema.aggregates WHERE keyspace_name=? AND aggregate_name=?", ks, shortFunctionName(a)),
                   row("(0, 0)"))

        # 1 + 2 + 3 = 6 / 3 = 2
        assert_rows(execute(cql, table, "SELECT " + a + "(b) FROM %s"), row(2.0))

# The test testFunctionDropPreparedStatement was not translated, because it
# checks Cassandra's internal cache of prepared statements.

# This test fails on Cassandra because of a Cassandra bug: when an authorizer
# is enabled (as in our test setup), CREATE AGGREGATE with a non-existent
# SFUNC or FINALFUNC fails with a NoSuchElementException server error,
# instead of an InvalidRequest. This is CASSANDRA-21734.
def testAggregatesReferencedInAggregates(cql, cassandra_bug):
    with create_keyspace(cql, REPLICATION) as ks:
        fState = createFunction(cql, ks,
                                "CREATE FUNCTION %s(a int, b int) " +
                                "CALLED ON NULL INPUT " +
                                "RETURNS int " +
                                java_or_lua(cql, " return a + b;", "return a + b"))

        a = createAggregate(cql, ks,
                            "CREATE AGGREGATE %s(int) " +
                            "SFUNC " + shortFunctionName(fState) + " " +
                            "STYPE int ")

        assert_invalid_message_re(cql, ks, FUNCTION_DOESNT_EXIST_MESSAGE,
                             "CREATE AGGREGATE " + ks + ".aggInv(int) " +
                             "SFUNC " + shortFunctionName(a) + " " +
                             "STYPE int ")

        assert_invalid_message_re(cql, ks, NOT_A_SCALAR_FUNCTION_MESSAGE,
                             "CREATE AGGREGATE " + ks + ".aggInv(int) " +
                             "SFUNC " + shortFunctionName(fState) + " " +
                             "STYPE int " +
                             "FINALFUNC " + shortFunctionName(a))

# Reproduces SCYLLADB-5158 (CREATE AGGREGATE should reject a RETURNS NULL ON
# NULL INPUT state function without INITCOND) and #24344 (such a state
# function called with null should leave the state unchanged).
@pytest.mark.xfail(reason="SCYLLADB-5158, #24344")
def testCalledOnNullInput(cql):
    with create_keyspace(cql, REPLICATION) as ks:
        fStateNonNull = createFunction(cql, ks,
                                       "CREATE OR REPLACE FUNCTION %s(state int, val int) " +
                                       "RETURNS NULL ON NULL INPUT " +
                                       "RETURNS int " +
                                       java_or_lua(cql, "return Integer.valueOf(state + val);", "return state + val"))
        fStateNull = createFunction(cql, ks,
                                    "CREATE OR REPLACE FUNCTION %s(state int, val int) " +
                                    "CALLED ON NULL INPUT " +
                                    "RETURNS int " +
                                    java_or_lua(cql, "return Integer.valueOf(" +
                                                "   (state != null ? state.intValue() : 0) " +
                                                "   + (val != null ? val.intValue() : 0));",
                                                "if state == nil then state = 0 end if val == nil then val = 0 end return state + val"))
        fStateAlwaysNull = createFunction(cql, ks,
                                    "CREATE OR REPLACE FUNCTION %s(state int, val int) " +
                                    "CALLED ON NULL INPUT " +
                                    "RETURNS int " +
                                    java_or_lua(cql, "return null;", "return nil"))
        fFinalNonNull = createFunction(cql, ks,
                                       "CREATE OR REPLACE FUNCTION %s(state int) " +
                                       "RETURNS NULL ON NULL INPUT " +
                                       "RETURNS int " +
                                       java_or_lua(cql, "return Integer.valueOf(state);", "return state"))
        fFinalNull = createFunction(cql, ks,
                                    "CREATE OR REPLACE FUNCTION %s(state int) " +
                                    "CALLED ON NULL INPUT " +
                                    "RETURNS int " +
                                    java_or_lua(cql, "return state;", "return state"))

        assert_invalid(cql, ks, "CREATE AGGREGATE " + ks + ".invAggr(int) " +
                      "SFUNC " + shortFunctionName(fStateNonNull) + " " +
                      "STYPE int")
        assert_invalid(cql, ks, "CREATE AGGREGATE " + ks + ".invAggr(int) " +
                      "SFUNC " + shortFunctionName(fStateNonNull) + " " +
                      "STYPE int " +
                      "FINALFUNC " + shortFunctionName(fFinalNonNull))

        aStateNull = createAggregate(cql, ks,
                                     "CREATE AGGREGATE %s(int) " +
                                     "SFUNC " + shortFunctionName(fStateNull) + " " +
                                     "STYPE int")
        aStateNullFinalNull = createAggregate(cql, ks,
                                              "CREATE AGGREGATE %s(int) " +
                                              "SFUNC " + shortFunctionName(fStateNull) + " " +
                                              "STYPE int " +
                                              "FINALFUNC " + shortFunctionName(fFinalNull))
        aStateNullFinalNonNull = createAggregate(cql, ks,
                                                 "CREATE AGGREGATE %s(int) " +
                                                 "SFUNC " + shortFunctionName(fStateNull) + " " +
                                                 "STYPE int " +
                                                 "FINALFUNC " + shortFunctionName(fFinalNonNull))
        aStateNonNull = createAggregate(cql, ks,
                                        "CREATE AGGREGATE %s(int) " +
                                        "SFUNC " + shortFunctionName(fStateNonNull) + " " +
                                        "STYPE int " +
                                        "INITCOND 0")
        aStateNonNullFinalNull = createAggregate(cql, ks,
                                                 "CREATE AGGREGATE %s(int) " +
                                                 "SFUNC " + shortFunctionName(fStateNonNull) + " " +
                                                 "STYPE int " +
                                                 "FINALFUNC " + shortFunctionName(fFinalNull) + " " +
                                                 "INITCOND 0")
        aStateNonNullFinalNonNull = createAggregate(cql, ks,
                                                    "CREATE AGGREGATE %s(int) " +
                                                    "SFUNC " + shortFunctionName(fStateNonNull) + " " +
                                                    "STYPE int " +
                                                    "FINALFUNC " + shortFunctionName(fFinalNonNull) + " " +
                                                    "INITCOND 0")
        aStateAlwaysNullFinalNull = createAggregate(cql, ks,
                                                    "CREATE AGGREGATE %s(int) " +
                                                    "SFUNC " + shortFunctionName(fStateAlwaysNull) + " " +
                                                    "STYPE int " +
                                                    "FINALFUNC " + shortFunctionName(fFinalNull))
        aStateAlwaysNullFinalNonNull = createAggregate(cql, ks,
                                                       "CREATE AGGREGATE %s(int) " +
                                                       "SFUNC " + shortFunctionName(fStateAlwaysNull) + " " +
                                                       "STYPE int " +
                                                       "FINALFUNC " + shortFunctionName(fFinalNonNull))

        with create_table(cql, ks, "(key int PRIMARY KEY, i int)") as table:
            execute(cql, table, "INSERT INTO %s (key, i) VALUES (0, null)")
            execute(cql, table, "INSERT INTO %s (key, i) VALUES (1, 1)")
            execute(cql, table, "INSERT INTO %s (key, i) VALUES (2, 2)")
            execute(cql, table, "INSERT INTO %s (key, i) VALUES (3, 3)")

            assert_rows(execute(cql, table, "SELECT " + aStateNull + "(i) FROM %s WHERE key = 0"), row(0))
            assert_rows(execute(cql, table, "SELECT " + aStateNullFinalNull + "(i) FROM %s WHERE key = 0"), row(0))
            assert_rows(execute(cql, table, "SELECT " + aStateNullFinalNonNull + "(i) FROM %s WHERE key = 0"), row(0))
            assert_rows(execute(cql, table, "SELECT " + aStateNonNull + "(i) FROM %s WHERE key = 0"), row(0))
            assert_rows(execute(cql, table, "SELECT " + aStateNonNullFinalNull + "(i) FROM %s WHERE key = 0"), row(0))
            assert_rows(execute(cql, table, "SELECT " + aStateNonNullFinalNonNull + "(i) FROM %s WHERE key = 0"), row(0))
            assert_rows(execute(cql, table, "SELECT " + aStateAlwaysNullFinalNull + "(i) FROM %s WHERE key = 0"), row(None))
            assert_rows(execute(cql, table, "SELECT " + aStateAlwaysNullFinalNonNull + "(i) FROM %s WHERE key = 0"), row(None))

            assert_rows(execute(cql, table, "SELECT " + aStateNull + "(i) FROM %s WHERE key = 1"), row(1))
            assert_rows(execute(cql, table, "SELECT " + aStateNullFinalNull + "(i) FROM %s WHERE key = 1"), row(1))
            assert_rows(execute(cql, table, "SELECT " + aStateNullFinalNonNull + "(i) FROM %s WHERE key = 1"), row(1))
            assert_rows(execute(cql, table, "SELECT " + aStateNonNull + "(i) FROM %s WHERE key = 1"), row(1))
            assert_rows(execute(cql, table, "SELECT " + aStateNonNullFinalNull + "(i) FROM %s WHERE key = 1"), row(1))
            assert_rows(execute(cql, table, "SELECT " + aStateNonNullFinalNonNull + "(i) FROM %s WHERE key = 1"), row(1))
            assert_rows(execute(cql, table, "SELECT " + aStateAlwaysNullFinalNull + "(i) FROM %s WHERE key = 1"), row(None))
            assert_rows(execute(cql, table, "SELECT " + aStateAlwaysNullFinalNonNull + "(i) FROM %s WHERE key = 1"), row(None))

            assert_rows(execute(cql, table, "SELECT " + aStateNull + "(i) FROM %s WHERE key IN (1, 2, 3)"), row(6))
            assert_rows(execute(cql, table, "SELECT " + aStateNullFinalNull + "(i) FROM %s WHERE key IN (1, 2, 3)"), row(6))
            assert_rows(execute(cql, table, "SELECT " + aStateNullFinalNonNull + "(i) FROM %s WHERE key IN (1, 2, 3)"), row(6))
            assert_rows(execute(cql, table, "SELECT " + aStateNonNull + "(i) FROM %s WHERE key IN (1, 2, 3)"), row(6))
            assert_rows(execute(cql, table, "SELECT " + aStateNonNullFinalNull + "(i) FROM %s WHERE key IN (1, 2, 3)"), row(6))
            assert_rows(execute(cql, table, "SELECT " + aStateNonNullFinalNonNull + "(i) FROM %s WHERE key IN (1, 2, 3)"), row(6))
            assert_rows(execute(cql, table, "SELECT " + aStateAlwaysNullFinalNull + "(i) FROM %s WHERE key IN (1, 2, 3)"), row(None))
            assert_rows(execute(cql, table, "SELECT " + aStateAlwaysNullFinalNonNull + "(i) FROM %s WHERE key IN (1, 2, 3)"), row(None))

def testWrongStateType(cql):
    with create_keyspace(cql, REPLICATION) as ks, create_table(cql, ks, "(key int primary key, val int)") as table:
        execute(cql, table, "INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1)

        fState = createFunction(cql, ks,
                                "CREATE FUNCTION %s(a int, b int) " +
                                "CALLED ON NULL INPUT " +
                                "RETURNS double " +
                                java_or_lua(cql, "return Double.valueOf(1.0);", "return 1.0"))

        fFinal = createFunction(cql, ks,
                                "CREATE FUNCTION %s(a int) " +
                                "CALLED ON NULL INPUT " +
                                "RETURNS int " +
                                java_or_lua(cql, "return Integer.valueOf(1);", "return 1"))

        assert_invalid_message_re(cql, ks, WRONG_STATE_TYPE_MESSAGE,
                             "CREATE AGGREGATE " + ks + "." + unique_name() + "(int) " +
                             "SFUNC " + shortFunctionName(fState) + ' ' +
                             "STYPE int " +
                             "FINALFUNC " + shortFunctionName(fFinal) + ' ' +
                             "INITCOND 1")

def testWrongKeyspace(cql):
    with create_keyspace(cql, REPLICATION) as KEYSPACE, create_keyspace(cql, REPLICATION) as KEYSPACE_PER_TEST:
        with create_type(cql, KEYSPACE, "(txt text, i int)") as type:
            fState = createFunction(cql, KEYSPACE_PER_TEST,
                                    "CREATE FUNCTION %s(a int, b int) " +
                                    "CALLED ON NULL INPUT " +
                                    "RETURNS double " +
                                    java_or_lua(cql, "return Double.valueOf(1.0);", "return 1.0"))

            fFinal = createFunction(cql, KEYSPACE_PER_TEST,
                                    "CREATE FUNCTION %s(a int) " +
                                    "CALLED ON NULL INPUT " +
                                    "RETURNS int " +
                                    java_or_lua(cql, "return Integer.valueOf(1);", "return 1"))

            fStateWrong = createFunction(cql, KEYSPACE,
                                    "CREATE FUNCTION %s(a int, b int) " +
                                    "CALLED ON NULL INPUT " +
                                    "RETURNS double " +
                                    java_or_lua(cql, "return Double.valueOf(1.0);", "return 1.0"))

            fFinalWrong = createFunction(cql, KEYSPACE,
                                    "CREATE FUNCTION %s(a int) " +
                                    "CALLED ON NULL INPUT " +
                                    "RETURNS int " +
                                    java_or_lua(cql, "return Integer.valueOf(1);", "return 1"))

            assert_invalid_message(cql, KEYSPACE, f"Statement on keyspace {KEYSPACE_PER_TEST} cannot refer to a user type in keyspace {KEYSPACE}; user types can only be used in the keyspace they are defined in",
                                 "CREATE AGGREGATE " + KEYSPACE_PER_TEST + ".test_wrong_ks(int) " +
                                 "SFUNC " + shortFunctionName(fState) + ' ' +
                                 "STYPE " + type + " " +
                                 "FINALFUNC " + shortFunctionName(fFinal) + ' ' +
                                 "INITCOND 1")

            # Scylla's syntax error messages are different from Cassandra's, so we
            # don't check the messages of the following syntax errors
            assert_invalid_syntax(cql, KEYSPACE, # specifying a function using "keyspace.functionname" is a syntax error
                                 "CREATE AGGREGATE " + KEYSPACE_PER_TEST + ".test_wrong_ks(int) " +
                                 "SFUNC " + fStateWrong + ' ' +
                                 "STYPE " + type + " " +
                                 "FINALFUNC " + shortFunctionName(fFinal) + ' ' +
                                 "INITCOND 1")

            assert_invalid_syntax(cql, KEYSPACE, # specifying a function using "keyspace.functionname" is a syntax error
                                 "CREATE AGGREGATE " + KEYSPACE_PER_TEST + ".test_wrong_ks(int) " +
                                 "SFUNC " + shortFunctionName(fState) + ' ' +
                                 "STYPE " + type + " " +
                                 "FINALFUNC " + fFinalWrong + ' ' +
                                 "INITCOND 1")

            assert_invalid_syntax(cql, KEYSPACE, # specifying a function using "keyspace.functionname" is a syntax error
                                 "CREATE AGGREGATE " + KEYSPACE_PER_TEST + ".test_wrong_ks(int) " +
                                 "SFUNC " + shortFunctionName(fState) + ' ' +
                                 "STYPE " + type + ' ' +
                                 "FINALFUNC system.min " +
                                 "INITCOND 1")
