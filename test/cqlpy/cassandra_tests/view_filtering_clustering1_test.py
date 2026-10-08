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

# Each test below checks a view filtering on a clustering column of the base
# table, for five different primary keys of the view. As explained in
# porting.py, we create all these views on the same base table.
mvPrimaryKeys = ["((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)"]

def create_views_where(cql, table, where):
    return create_views(cql, table, ["CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                     where + " PRIMARY KEY " + mvPrimaryKey
                                     for mvPrimaryKey in mvPrimaryKeys])

def assertViewsRows(cql, views, *rows):
    assert_views_rows_ignoring_order(cql, views, "SELECT a, b, c, d FROM %s", *rows)

def testClusteringKeyEQRestrictions(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, PRIMARY KEY (a, b, c))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 1, 0)

        # only accept rows where b = 1
        with create_views_where(cql, table, "WHERE a IS NOT NULL AND b = 1 AND c IS NOT NULL") as views:

            assertViewsRows(cql, views,
                            row(0, 1, 0, 0),
                            row(0, 1, 1, 0),
                            row(1, 1, 0, 0),
                            row(1, 1, 1, 0))

            # insert new rows that do not match the filter
            execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 0, 0, 0)
            execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 2, 0, 0)
            assertViewsRows(cql, views,
                            row(0, 1, 0, 0),
                            row(0, 1, 1, 0),
                            row(1, 1, 0, 0),
                            row(1, 1, 1, 0))

            # insert new row that does match the filter
            execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 2, 0)
            assertViewsRows(cql, views,
                            row(0, 1, 0, 0),
                            row(0, 1, 1, 0),
                            row(1, 1, 0, 0),
                            row(1, 1, 1, 0),
                            row(1, 1, 2, 0))

            # update rows that don't match the filter
            execute(cql, table, "UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 2, 0, 0)
            execute(cql, table, "UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 2, 2, 0)
            assertViewsRows(cql, views,
                            row(0, 1, 0, 0),
                            row(0, 1, 1, 0),
                            row(1, 1, 0, 0),
                            row(1, 1, 1, 0),
                            row(1, 1, 2, 0))

            # update a row that does match the filter
            execute(cql, table, "UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 1, 1, 0)
            assertViewsRows(cql, views,
                            row(0, 1, 0, 0),
                            row(0, 1, 1, 0),
                            row(1, 1, 0, 1),
                            row(1, 1, 1, 0),
                            row(1, 1, 2, 0))

            # delete rows that don't match the filter
            execute(cql, table, "DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 2, 0, 0)
            execute(cql, table, "DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 2, 2, 0)
            execute(cql, table, "DELETE FROM %s WHERE a = ? AND b = ?", 0, 0)
            assertViewsRows(cql, views,
                            row(0, 1, 0, 0),
                            row(0, 1, 1, 0),
                            row(1, 1, 0, 1),
                            row(1, 1, 1, 0),
                            row(1, 1, 2, 0))

            # delete a row that does match the filter
            execute(cql, table, "DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 1, 1, 0)
            assertViewsRows(cql, views,
                            row(0, 1, 0, 0),
                            row(0, 1, 1, 0),
                            row(1, 1, 1, 0),
                            row(1, 1, 2, 0))

            # delete a partition that matches the filter
            execute(cql, table, "DELETE FROM %s WHERE a = ?", 1)
            assertViewsRows(cql, views,
                            row(0, 1, 0, 0),
                            row(0, 1, 1, 0))

def testClusteringKeySliceRestrictions(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, PRIMARY KEY (a, b, c))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 1, 0)

        with create_views_where(cql, table, "WHERE a IS NOT NULL AND b >= 1 AND c IS NOT NULL") as views:

            assertViewsRows(cql, views,
                            row(0, 1, 0, 0),
                            row(0, 1, 1, 0),
                            row(1, 1, 0, 0),
                            row(1, 1, 1, 0))

            # insert new rows that do not match the filter
            execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, -1, 0, 0)
            execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 0, 0, 0)
            assertViewsRows(cql, views,
                            row(0, 1, 0, 0),
                            row(0, 1, 1, 0),
                            row(1, 1, 0, 0),
                            row(1, 1, 1, 0))

            # insert new row that does match the filter
            execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 2, 0)
            assertViewsRows(cql, views,
                            row(0, 1, 0, 0),
                            row(0, 1, 1, 0),
                            row(1, 1, 0, 0),
                            row(1, 1, 1, 0),
                            row(1, 1, 2, 0))

            # update rows that don't match the filter
            execute(cql, table, "UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 2, -1, 0)
            execute(cql, table, "UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 2, 0, 0)
            assertViewsRows(cql, views,
                            row(0, 1, 0, 0),
                            row(0, 1, 1, 0),
                            row(1, 1, 0, 0),
                            row(1, 1, 1, 0),
                            row(1, 1, 2, 0))

            # update a row that does match the filter
            execute(cql, table, "UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 1, 1, 0)
            assertViewsRows(cql, views,
                            row(0, 1, 0, 0),
                            row(0, 1, 1, 0),
                            row(1, 1, 0, 1),
                            row(1, 1, 1, 0),
                            row(1, 1, 2, 0))

            # delete rows that don't match the filter
            execute(cql, table, "DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 2, -1, 0)
            execute(cql, table, "DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 2, 0, 0)
            execute(cql, table, "DELETE FROM %s WHERE a = ? AND b = ?", 0, 0)
            assertViewsRows(cql, views,
                            row(0, 1, 0, 0),
                            row(0, 1, 1, 0),
                            row(1, 1, 0, 1),
                            row(1, 1, 1, 0),
                            row(1, 1, 2, 0))

            # delete a row that does match the filter
            execute(cql, table, "DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 1, 1, 0)
            assertViewsRows(cql, views,
                            row(0, 1, 0, 0),
                            row(0, 1, 1, 0),
                            row(1, 1, 1, 0),
                            row(1, 1, 2, 0))

            # delete a partition that matches the filter
            execute(cql, table, "DELETE FROM %s WHERE a = ?", 1)
            assertViewsRows(cql, views,
                            row(0, 1, 0, 0),
                            row(0, 1, 1, 0))

def testClusteringKeyINRestrictions(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, PRIMARY KEY (a, b, c))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 2, 1, 0)

        # only accept rows where b = 1
        with create_views_where(cql, table, "WHERE a IS NOT NULL AND b IN (1, 2) AND c IS NOT NULL") as views:

            assertViewsRows(cql, views,
                            row(0, 1, 0, 0),
                            row(0, 1, 1, 0),
                            row(1, 1, 0, 0),
                            row(1, 1, 1, 0),
                            row(1, 2, 1, 0))

            # insert new rows that do not match the filter
            execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, -1, 0, 0)
            execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 0, 0, 0)
            assertViewsRows(cql, views,
                            row(0, 1, 0, 0),
                            row(0, 1, 1, 0),
                            row(1, 1, 0, 0),
                            row(1, 1, 1, 0),
                            row(1, 2, 1, 0))

            # insert new row that does match the filter
            execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 2, 0)
            assertViewsRows(cql, views,
                            row(0, 1, 0, 0),
                            row(0, 1, 1, 0),
                            row(1, 1, 0, 0),
                            row(1, 1, 1, 0),
                            row(1, 1, 2, 0),
                            row(1, 2, 1, 0))

            # update rows that don't match the filter
            execute(cql, table, "UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 2, -1, 0)
            execute(cql, table, "UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 2, 0, 0)
            assertViewsRows(cql, views,
                            row(0, 1, 0, 0),
                            row(0, 1, 1, 0),
                            row(1, 1, 0, 0),
                            row(1, 1, 1, 0),
                            row(1, 1, 2, 0),
                            row(1, 2, 1, 0))

            # update a row that does match the filter
            execute(cql, table, "UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 1, 1, 0)
            assertViewsRows(cql, views,
                            row(0, 1, 0, 0),
                            row(0, 1, 1, 0),
                            row(1, 1, 0, 1),
                            row(1, 1, 1, 0),
                            row(1, 1, 2, 0),
                            row(1, 2, 1, 0))

            # delete rows that don't match the filter
            execute(cql, table, "DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 2, -1, 0)
            execute(cql, table, "DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 2, 0, 0)
            execute(cql, table, "DELETE FROM %s WHERE a = ? AND b = ?", 0, 0)
            assertViewsRows(cql, views,
                            row(0, 1, 0, 0),
                            row(0, 1, 1, 0),
                            row(1, 1, 0, 1),
                            row(1, 1, 1, 0),
                            row(1, 1, 2, 0),
                            row(1, 2, 1, 0))

            # delete a row that does match the filter
            execute(cql, table, "DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 1, 1, 0)
            assertViewsRows(cql, views,
                            row(0, 1, 0, 0),
                            row(0, 1, 1, 0),
                            row(1, 1, 1, 0),
                            row(1, 1, 2, 0),
                            row(1, 2, 1, 0))

            # delete a partition that matches the filter
            execute(cql, table, "DELETE FROM %s WHERE a = ?", 1)
            assertViewsRows(cql, views,
                            row(0, 1, 0, 0),
                            row(0, 1, 1, 0))
