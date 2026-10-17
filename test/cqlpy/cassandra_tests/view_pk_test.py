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
from cassandra import AlreadyExists

def testPartitionTombstone(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k1 int, c1 int , val int, PRIMARY KEY (k1, c1))") as table:
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT k1, c1, val FROM %s " +
                                     "WHERE k1 IS NOT NULL AND c1 IS NOT NULL AND val IS NOT NULL " +
                                     "PRIMARY KEY (val, k1, c1)") as view:

            execute(cql, table, "INSERT INTO %s (k1, c1, val) VALUES (1, 2, 200)")
            execute(cql, table, "INSERT INTO %s (k1, c1, val) VALUES (1, 3, 300)")

            assert_row_count(execute(cql, table, "select * from %s"), 2)
            assert_row_count(execute(cql, view, "select * from %s"), 2)

            execute(cql, table, "DELETE FROM %s WHERE k1 = 1")

            assert_row_count(execute(cql, table, "select * from %s"), 0)
            assert_row_count(execute(cql, view, "select * from %s"), 0)

def testcreateMvWithUnrestrictedPKParts(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k1 int, c1 int , val int, PRIMARY KEY (k1, c1))") as table:
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT val, k1, c1 FROM %s " +
                                     "WHERE k1 IS NOT NULL AND c1 IS NOT NULL AND val IS NOT NULL " +
                                     "PRIMARY KEY (val, k1, c1)"):
            pass

def testClusteringKeyTombstone(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k1 int, c1 int , val int, PRIMARY KEY (k1, c1))") as table:
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT k1, c1, val FROM %s " +
                                     "WHERE k1 IS NOT NULL AND c1 IS NOT NULL AND val IS NOT NULL " +
                                     "PRIMARY KEY (val, k1, c1)") as view:

            execute(cql, table, "INSERT INTO %s (k1, c1, val) VALUES (1, 2, 200)")
            execute(cql, table, "INSERT INTO %s (k1, c1, val) VALUES (1, 3, 300)")

            assert_row_count(execute(cql, table, "select * from %s"), 2)
            assert_row_count(execute(cql, view, "select * from %s"), 2)

            execute(cql, table, "DELETE FROM %s WHERE k1 = 1 and c1 = 3")

            assert_row_count(execute(cql, table, "select * from %s"), 1)
            assert_row_count(execute(cql, view, "select * from %s"), 1)

PK_NOT_RESTRICTED = "Primary key columns k must be restricted|Primary key column 'k' is required to be filtered"

def testPrimaryKeyIsNotNull(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "k int, " +
                      "asciival ascii, " +
                      "bigintval bigint, " +
                      "PRIMARY KEY((k, asciival)))") as table:

        # Must include "IS NOT NULL" for primary keys
        # (Cassandra's syntax error message contains "mismatched input",
        # Scylla's says just "Syntax error".)
        with pytest.raises(SyntaxException, match="mismatched input|Syntax error"):
            with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s"):
                pass

        # Must include both when the partition key is composite
        # (Cassandra's message is "Primary key columns k must be restricted
        # with 'IS NOT NULL' or otherwise", Scylla's is "Primary key column 'k'
        # is required to be filtered by 'IS NOT NULL'".)
        with pytest.raises(InvalidRequest, match=PK_NOT_RESTRICTED):
            with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                         "WHERE bigintval IS NOT NULL AND asciival IS NOT NULL " +
                                         "PRIMARY KEY (bigintval, k, asciival)"):
                pass

# The second part of the Java testPrimaryKeyIsNotNull, for a table with a
# non-composite partition key, which Scylla handles differently.
# Reproduces #11979 (Scylla doesn't require "IS NOT NULL" on the base table's
# non-composite partition key).
@pytest.mark.xfail(reason="#11979")
def testPrimaryKeyIsNotNullNonCompositePartitionKey(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "k int, " +
                      "asciival ascii, " +
                      "bigintval bigint, " +
                      "PRIMARY KEY(k, asciival))") as table:
        with pytest.raises(SyntaxException, match="mismatched input|Syntax error"):
            with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s"):
                pass

        # Must still include both even when the partition key is composite
        with pytest.raises(InvalidRequest, match=PK_NOT_RESTRICTED):
            with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                         "WHERE bigintval IS NOT NULL AND asciival IS NOT NULL " +
                                         "PRIMARY KEY (bigintval, k, asciival)"):
                pass

# Reproduces SCYLLADB-5141 (snake_case names of native functions - here,
# from_json()).
@pytest.mark.xfail(reason="SCYLLADB-5141")
def testCompoundPartitionKey(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "k int, " +
                      "asciival ascii, " +
                      "bigintval bigint, " +
                      "PRIMARY KEY((k, asciival)))") as table, \
         ExitStack() as stack:

        # The Java test iterates over the table's columns from its metadata.
        # None of these columns is multi-cell, and k and asciival are in the
        # partition key.
        partitionKey = {"k", "asciival"}
        views = []
        for name in ["k", "asciival", "bigintval"]:
            asciival = "" if name == "asciival" else "AND asciival IS NOT NULL "
            try:
                query = ("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE " + name + " IS NOT NULL AND k IS NOT NULL "
                         + asciival + "PRIMARY KEY ("
                         + name + ", k" + ("" if name == "asciival" else ", asciival") + ")")
                views.append(stack.enter_context(create_view(cql, table, query, wait=False, name="mv1_" + name)))
            except Exception:
                if name not in partitionKey:
                    pytest.fail("MV creation failed on " + name)

            try:
                query = ("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE " + name + " IS NOT NULL AND k IS NOT NULL "
                         + asciival + " PRIMARY KEY ("
                         + name + ", asciival" + ("" if name == "k" else ", k") + ")")
                views.append(stack.enter_context(create_view(cql, table, query, wait=False, name="mv2_" + name)))
            except Exception:
                if name not in partitionKey:
                    pytest.fail("MV creation failed on " + name)

            try:
                query = ("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE " + name + " IS NOT NULL AND k IS NOT NULL "
                         + asciival + "PRIMARY KEY ((" + name + ", k), asciival)")
                views.append(stack.enter_context(create_view(cql, table, query, wait=False, name="mv3_" + name)))
            except Exception:
                if name not in partitionKey:
                    pytest.fail("MV creation failed on " + name)

            # Should fail on duplicate name
            with pytest.raises((InvalidRequest, AlreadyExists)):
                query = ("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE " + name + " IS NOT NULL AND k IS NOT NULL "
                         + asciival + "PRIMARY KEY ((" + name + ", k), asciival)")
                cql.execute(query.replace('%s', test_keyspace + ".mv3_" + name, 1).replace('%s', table, 1))

            # Should fail with unknown base column
            with pytest.raises(InvalidRequest):
                query = ("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE " + name + " IS NOT NULL AND k IS NOT NULL "
                         + asciival + "PRIMARY KEY ((" + name + ", k), nonexistentcolumn)")
                with create_view(cql, table, query, wait=False, name="mv4_" + name):
                    pass

        # To make the test faster, we didn't wait for each view to be built
        # after creating it, and wait for all of them together here.
        for view in views:
            wait_for_view_built(cql, view)

        ks = test_keyspace + "."
        execute(cql, table, "INSERT INTO %s (k, asciival, bigintval) VALUES (?, ?, from_json(?))", 0, "ascii text", "123123123123")
        execute(cql, table, "INSERT INTO %s (k, asciival) VALUES (?, from_json(?))", 0, '"ascii text"')
        assert_rows(execute(cql, table, "SELECT bigintval FROM %s WHERE k = ? and asciival = ?", 0, "ascii text"), row(123123123123))

        #Check the MV
        assert_rows(cql.execute(f"SELECT k, bigintval from {ks}mv1_asciival WHERE asciival = %s", ["ascii text"]), row(0, 123123123123))
        assert_rows(cql.execute(f"SELECT k, bigintval from {ks}mv2_k WHERE asciival = %s and k = %s", ["ascii text", 0]), row(0, 123123123123))
        assert_rows(cql.execute(f"SELECT k from {ks}mv1_bigintval WHERE bigintval = %s", [123123123123]), row(0))
        assert_rows(cql.execute(f"SELECT asciival from {ks}mv3_bigintval where bigintval = %s AND k = %s", [123123123123, 0]), row("ascii text"))

        #UPDATE BASE
        execute(cql, table, "INSERT INTO %s (k, asciival, bigintval) VALUES (?, ?, from_json(?))", 0, "ascii text", "1")
        assert_rows(execute(cql, table, "SELECT bigintval FROM %s WHERE k = ? and asciival = ?", 0, "ascii text"), row(1))

        #Check the MV
        assert_rows(cql.execute(f"SELECT k, bigintval from {ks}mv1_asciival WHERE asciival = %s", ["ascii text"]), row(0, 1))
        assert_rows(cql.execute(f"SELECT k, bigintval from {ks}mv2_k WHERE asciival = %s and k = %s", ["ascii text", 0]), row(0, 1))
        assert_empty(cql.execute(f"SELECT k from {ks}mv1_bigintval WHERE bigintval = %s", [123123123123]))
        assert_empty(cql.execute(f"SELECT asciival from {ks}mv3_bigintval where bigintval = %s AND k = %s", [123123123123, 0]))
        assert_rows(cql.execute(f"SELECT asciival from {ks}mv3_bigintval where bigintval = %s AND k = %s", [1, 0]), row("ascii text"))

        #test truncate also truncates all MV
        execute(cql, table, "TRUNCATE %s")

        assert_empty(execute(cql, table, "SELECT bigintval FROM %s WHERE k = ? and asciival = ?", 0, "ascii text"))
        assert_empty(cql.execute(f"SELECT k, bigintval from {ks}mv1_asciival WHERE asciival = %s", ["ascii text"]))
        assert_empty(cql.execute(f"SELECT k, bigintval from {ks}mv2_k WHERE asciival = %s and k = %s", ["ascii text", 0]))
        assert_empty(cql.execute(f"SELECT asciival from {ks}mv3_bigintval where bigintval = %s AND k = %s", [1, 0]))

def testClusteringOrder(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "a int," +
                      "b int," +
                      "c int," +
                      "d int," +
                      "PRIMARY KEY (a, b, c))" +
                      "WITH CLUSTERING ORDER BY (b ASC, c DESC)") as table:

        with create_views(cql, table, [
                "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL PRIMARY KEY (a, b, c) WITH CLUSTERING ORDER BY (b DESC, c ASC)",
                "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL PRIMARY KEY (a, c, b) WITH CLUSTERING ORDER BY (c ASC, b ASC)",
                "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL PRIMARY KEY (a, b, c)",
                "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL PRIMARY KEY (a, c, b) WITH CLUSTERING ORDER BY (c DESC, b ASC)"]) as (mv1, mv2, mv3, mv4):

            execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 1, 1)
            execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 2, 2, 2)

            assert_rows(cql.execute("SELECT b FROM " + mv1), row(2), row(1))
            assert_rows(cql.execute("SELECT c FROM " + mv2), row(1), row(2))
            assert_rows(cql.execute("SELECT b FROM " + mv3), row(1), row(2))
            assert_rows(cql.execute("SELECT c FROM " + mv4), row(2), row(1))

def testPrimaryKeyOnlyTable(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "a int," +
                      "b int," +
                      "PRIMARY KEY (a, b))") as table:

        # Cannot use SELECT *, as those are always handled by the includeAll shortcut in View.updateAffectsView
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT a, b FROM %s " +
                                     "WHERE a IS NOT NULL AND b IS NOT NULL " +
                                     "PRIMARY KEY (b, a)") as view:

            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", 1, 1)

            assert_rows(execute(cql, view, "SELECT a, b FROM %s"), row(1, 1))

def testPartitionKeyOnlyTable(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "a int," +
                      "b int," +
                      "PRIMARY KEY ((a, b)))") as table:

        # Cannot use SELECT *, as those are always handled by the includeAll shortcut in View.updateAffectsView
        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT a, b FROM %s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b, a)") as view:

            execute(cql, table, "INSERT INTO %s (a, b) VALUES (?, ?)", 1, 1)

            assert_rows(execute(cql, view, "SELECT a, b FROM %s"), row(1, 1))

def testDeleteSingleColumnInViewClustering(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "a int," +
                      "b int," +
                      "c int," +
                      "d int," +
                      "PRIMARY KEY (a, b))") as table:

        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                     "WHERE a IS NOT NULL AND b IS NOT NULL AND d IS NOT NULL " +
                                     "PRIMARY KEY (a, d, b)") as view:

            execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0)
            assert_rows(execute(cql, view, "SELECT a, d, b, c FROM %s"), row(0, 0, 0, 0))

            execute(cql, table, "DELETE c FROM %s WHERE a = ? AND b = ?", 0, 0)
            assert_rows(execute(cql, view, "SELECT a, d, b, c FROM %s"), row(0, 0, 0, None))

            execute(cql, table, "DELETE d FROM %s WHERE a = ? AND b = ?", 0, 0)
            assert_empty(execute(cql, view, "SELECT a, d, b FROM %s"))

def testDeleteSingleColumnInViewPartitionKey(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "a int," +
                      "b int," +
                      "c int," +
                      "d int," +
                      "PRIMARY KEY (a, b))") as table:

        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                     "WHERE a IS NOT NULL AND b IS NOT NULL AND d IS NOT NULL " +
                                     "PRIMARY KEY (d, a, b)") as view:

            execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0)
            assert_rows(execute(cql, view, "SELECT a, d, b, c FROM %s"), row(0, 0, 0, 0))

            execute(cql, table, "DELETE c FROM %s WHERE a = ? AND b = ?", 0, 0)
            assert_rows(execute(cql, view, "SELECT a, d, b, c FROM %s"), row(0, 0, 0, None))

            execute(cql, table, "DELETE d FROM %s WHERE a = ? AND b = ?", 0, 0)
            assert_empty(execute(cql, view, "SELECT a, d, b FROM %s"))

def testMultipleNonPrimaryKeysInView(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                      "a int," +
                      "b int," +
                      "c int," +
                      "d int," +
                      "e int," +
                      "PRIMARY KEY ((a, b), c))") as table:

        # Should have rejected a query including multiple non-primary key base columns
        with pytest.raises(InvalidRequest, match="Cannot include more than one non-primary key column"):
            with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL AND d IS NOT NULL AND e IS NOT NULL PRIMARY KEY ((d, a), b, e, c)"):
                pass

        with pytest.raises(InvalidRequest, match="Cannot include more than one non-primary key column"):
            with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL AND d IS NOT NULL AND e IS NOT NULL PRIMARY KEY ((a, b), c, d, e)"):
                pass

def testNullInClusteringColumns(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id1 int, id2 int, v1 text, v2 text, PRIMARY KEY (id1, id2))") as table:

        with create_view(cql, table, "CREATE MATERIALIZED VIEW %s AS" +
                                     "  SELECT id1, v1, id2, v2" +
                                     "  FROM %s" +
                                     "  WHERE id1 IS NOT NULL AND v1 IS NOT NULL AND id2 IS NOT NULL" +
                                     "  PRIMARY KEY (id1, v1, id2)" +
                                     "  WITH CLUSTERING ORDER BY (v1 DESC, id2 ASC)") as view:

            execute(cql, table, "INSERT INTO %s (id1, id2, v1, v2) VALUES (?, ?, ?, ?)", 0, 1, "foo", "bar")

            assert_rows(execute(cql, table, "SELECT * FROM %s"), row(0, 1, "foo", "bar"))
            assert_rows(execute(cql, view, "SELECT * FROM %s"), row(0, "foo", 1, "bar"))

            execute(cql, table, "UPDATE %s SET v1=? WHERE id1=? AND id2=?", None, 0, 1)
            assert_rows(execute(cql, table, "SELECT * FROM %s"), row(0, 1, None, "bar"))
            assert_empty(execute(cql, view, "SELECT * FROM %s"))

            execute(cql, table, "UPDATE %s SET v2=? WHERE id1=? AND id2=?", "rab", 0, 1)
            assert_rows(execute(cql, table, "SELECT * FROM %s"), row(0, 1, None, "rab"))
            assert_empty(execute(cql, view, "SELECT * FROM %s"))
