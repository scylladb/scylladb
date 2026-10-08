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
