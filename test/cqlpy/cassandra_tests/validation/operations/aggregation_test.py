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
