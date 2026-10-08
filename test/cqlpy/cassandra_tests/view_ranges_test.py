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

def testExistingRangeTombstoneWithFlush(cql, test_keyspace):
    existingRangeTombstone(cql, test_keyspace, True)

def testExistingRangeTombstoneWithoutFlush(cql, test_keyspace):
    existingRangeTombstone(cql, test_keyspace, False)

def existingRangeTombstone(cql, test_keyspace, flush):
    with create_table(cql, test_keyspace, "(k1 int, c1 int, c2 int, v1 int, v2 int, PRIMARY KEY (k1, c1, c2))") as table:
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                     "WHERE k1 IS NOT NULL AND c1 IS NOT NULL AND c2 IS NOT NULL " +
                                     "PRIMARY KEY (k1, c2, c1)") as view:

            execute(cql, table, "DELETE FROM %s USING TIMESTAMP 10 WHERE k1 = 1 and c1=1")

            if flush:
                nodetool.flush(cql, table)

            execute(cql, table, "BEGIN BATCH " +
                    "INSERT INTO %s (k1, c1, c2, v1, v2) VALUES (1, 0, 0, 0, 0) USING TIMESTAMP 5; " +
                    "INSERT INTO %s (k1, c1, c2, v1, v2) VALUES (1, 0, 1, 0, 1) USING TIMESTAMP 5; " +
                    "INSERT INTO %s (k1, c1, c2, v1, v2) VALUES (1, 1, 0, 1, 0) USING TIMESTAMP 5; " +
                    "INSERT INTO %s (k1, c1, c2, v1, v2) VALUES (1, 1, 1, 1, 1) USING TIMESTAMP 5; " +
                    "INSERT INTO %s (k1, c1, c2, v1, v2) VALUES (1, 1, 2, 1, 2) USING TIMESTAMP 5; " +
                    "INSERT INTO %s (k1, c1, c2, v1, v2) VALUES (1, 1, 3, 1, 3) USING TIMESTAMP 5; " +
                    "INSERT INTO %s (k1, c1, c2, v1, v2) VALUES (1, 2, 0, 2, 0) USING TIMESTAMP 5; " +
                    "APPLY BATCH")

            assert_rows_ignoring_order(execute(cql, table, "select * from %s"),
                                       row(1, 0, 0, 0, 0),
                                       row(1, 0, 1, 0, 1),
                                       row(1, 2, 0, 2, 0))
            assert_rows_ignoring_order(execute(cql, view, "select k1,c1,c2,v1,v2 from %s"),
                                       row(1, 0, 0, 0, 0),
                                       row(1, 0, 1, 0, 1),
                                       row(1, 2, 0, 2, 0))

def testRangeTombstone(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "k int, " +
                      "asciival ascii, " +
                      "bigintval bigint, " +
                      "textval1 text, " +
                      "textval2 text, " +
                      "PRIMARY KEY((k, asciival), bigintval, textval1)" +
                      ")") as table:

        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                     "WHERE textval2 IS NOT NULL AND k IS NOT NULL AND asciival IS NOT NULL AND bigintval IS NOT NULL AND textval1 IS NOT NULL " +
                                     "PRIMARY KEY ((textval2, k), asciival, bigintval, textval1)") as view1:

            for i in range(100):
                execute(cql, table, "INSERT into %s (k,asciival,bigintval,textval1,textval2) VALUES (?,?,?,?,?)",
                        0, "foo", i % 2, "bar" + str(i), "baz")

            assert_row_count(execute(cql, table, "select * from %s where k = 0 and asciival = 'foo' and bigintval = 0"), 50)
            assert_row_count(execute(cql, table, "select * from %s where k = 0 and asciival = 'foo' and bigintval = 1"), 50)

            assert_row_count(execute(cql, view1, "select * from %s"), 100)

            #Check the builder works
            with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                         "WHERE textval2 IS NOT NULL AND k IS NOT NULL AND asciival IS NOT NULL AND bigintval IS NOT NULL AND textval1 IS NOT NULL " +
                                         "PRIMARY KEY ((textval2, k), asciival, bigintval, textval1)") as view2:

                assert_row_count(execute(cql, view2, "select * from %s"), 100)

                with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                             "WHERE textval2 IS NOT NULL AND k IS NOT NULL AND asciival IS NOT NULL AND bigintval IS NOT NULL AND textval1 IS NOT NULL " +
                                             "PRIMARY KEY ((textval2, k), bigintval, textval1, asciival)") as view3:

                    assert_row_count(execute(cql, view3, "select * from %s"), 100)
                    assert_row_count(execute(cql, view3, "select asciival from %s where textval2 = ? and k = ?", "baz", 0), 100)

                    #Write a RT and verify the data is removed from index
                    execute(cql, table, "DELETE FROM %s WHERE k = ? AND asciival = ? and bigintval = ?", 0, "foo", 0)

                    assert_row_count(execute(cql, view3, "select asciival from %s where textval2 = ? and k = ?", "baz", 0), 50)

def testRangeTombstone2(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "k int, " +
                      "asciival ascii, " +
                      "bigintval bigint, " +
                      "textval1 text, " +
                      "PRIMARY KEY((k, asciival), bigintval)" +
                      ")") as table:

        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                     "WHERE textval1 IS NOT NULL AND k IS NOT NULL AND asciival IS NOT NULL AND bigintval IS NOT NULL " +
                                     "PRIMARY KEY ((textval1, k), asciival, bigintval)") as view:

            for i in range(100):
                execute(cql, table, "INSERT into %s (k,asciival,bigintval,textval1)VALUES(?,?,?,?)", 0, "foo", i % 2, "bar" + str(i))

            assert_row_count(execute(cql, table, "select * from %s where k = 0 and asciival = 'foo' and bigintval = 0"), 1)
            assert_row_count(execute(cql, table, "select * from %s where k = 0 and asciival = 'foo' and bigintval = 1"), 1)

            assert_row_count(execute(cql, table, "select * from %s"), 2)
            assert_row_count(execute(cql, view, "select * from %s"), 2)

            #Write a RT and verify the data is removed from index
            execute(cql, table, "DELETE FROM %s WHERE k = ? AND asciival = ? and bigintval = ?", 0, "foo", 0)

            assert_row_count(execute(cql, table, "select * from %s"), 1)
            assert_row_count(execute(cql, view, "select * from %s"), 1)

def testRangeTombstone3(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "k int, " +
                      "asciival ascii, " +
                      "bigintval bigint, " +
                      "textval1 text, " +
                      "PRIMARY KEY((k, asciival), bigintval)" +
                      ")") as table:

        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                     "WHERE textval1 IS NOT NULL AND k IS NOT NULL AND asciival IS NOT NULL AND bigintval IS NOT NULL " +
                                     "PRIMARY KEY ((textval1, k), asciival, bigintval)") as view:

            for i in range(100):
                execute(cql, table, "INSERT into %s (k,asciival,bigintval,textval1)VALUES(?,?,?,?)", 0, "foo", i % 2, "bar" + str(i))

            assert_row_count(execute(cql, table, "select * from %s where k = 0 and asciival = 'foo' and bigintval = 0"), 1)
            assert_row_count(execute(cql, table, "select * from %s where k = 0 and asciival = 'foo' and bigintval = 1"), 1)

            assert_row_count(execute(cql, table, "select * from %s"), 2)
            assert_row_count(execute(cql, view, "select * from %s"), 2)

            #Write a RT and verify the data is removed from index
            execute(cql, table, "DELETE FROM %s WHERE k = ? AND asciival = ? and bigintval >= ?", 0, "foo", 0)

            assert_row_count(execute(cql, table, "select * from %s"), 0)
            assert_row_count(execute(cql, view, "select * from %s"), 0)
