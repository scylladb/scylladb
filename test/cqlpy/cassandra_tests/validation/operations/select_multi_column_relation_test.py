# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit a87055d56a33a9b17606f14535f48eb461965b82
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0

from cassandra_tests.porting import *

TOO_BIG = bytearray([1])*1024*65
REQUIRES_ALLOW_FILTERING_MESSAGE = "Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING"

def testSingleClusteringInvalidQueries(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, primary key (a, b))") as table:
        assertInvalidSyntax(cql, table, "SELECT * FROM %s WHERE () = (?, ?)", 1, 2)
        assertInvalidMessage(cql, table, "cannot be restricted by",
                             "SELECT * FROM %s WHERE a = 0 AND (b) = (?) AND (b) > (?)", 0, 0)
        assertInvalidMessage(cql, table, "More than one restriction was found for the start bound on b",
                             "SELECT * FROM %s WHERE a = 0 AND (b) > (?) AND (b) > (?)", 0, 1)
        # Cassandra complains that "More than one restriction was found for
        # the start bound on b", but Scylla because of #4244 complains
        # that single- and multi-column relations are mixed
        assertInvalid(cql, table,
                             "SELECT * FROM %s WHERE a = 0 AND (b) > (?) AND b > ?", 0, 1)
        assertInvalidMessage(cql, table, "Multi-column relations can only be applied to clustering columns but was applied to: a",
                             "SELECT * FROM %s WHERE (a, b) = (?, ?)", 0, 0)

# We need to skip this test because issue #13241 causes it to frequently
# crash Scylla, and not just fail cleanly.
@pytest.mark.skip(reason="Issue #13241")
@pytest.mark.xfail(reason="Issue #4244")
def testMultiClusteringInvalidQueries(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, primary key (a, b, c, d))") as table:
        assertInvalidSyntax(cql, table, "SELECT * FROM %s WHERE a = 0 AND (b, c) > ()")
        assertInvalidMessage(cql, table, "Expected 2 elements in value tuple, but got 3: (?, ?, ?)",
                             "SELECT * FROM %s WHERE a = 0 AND (b, c) > (?, ?, ?)", 1, 2, 3)
        assertInvalidMessage(cql, table, "Invalid null value in condition for column c",
                             "SELECT * FROM %s WHERE a = 0 AND (b, c) > (?, ?)", 1, None)

        # Wrong order of columns
        assertInvalidMessage(cql, table, "PRIMARY KEY order",
                             "SELECT * FROM %s WHERE a = 0 AND (d, c, b) = (?, ?, ?)", 0, 0, 0)
        assertInvalidMessage(cql, table, "PRIMARY KEY order",
                             "SELECT * FROM %s WHERE a = 0 AND (d, c, b) > (?, ?, ?)", 0, 0, 0)

        # Wrong number of values
        # Reproduces #13241:
        assertInvalidMessage(cql, table, "Expected 3 elements in value tuple, but got 2: (?, ?)",
                             "SELECT * FROM %s WHERE a=0 AND (b, c, d) IN ((?, ?))", 0, 1)
        # Scylla and Cassandra have very different error messages here, but
        # both mention "tuple"
        assertInvalidMessage(cql, table, "tuple",
                             "SELECT * FROM %s WHERE a=0 AND (b, c, d) IN ((?, ?, ?, ?, ?))", 0, 1, 2, 3, 4)

        # Missing first clustering column
        # Scylla and Cassandra have very different error messages here
        # with barely the word "column" in common
        assertInvalidMessage(cql, table, "column",
                             "SELECT * FROM %s WHERE a = 0 AND (c, d) = (?, ?)", 0, 0)
        assertInvalidMessage(cql, table, "column",
                             "SELECT * FROM %s WHERE a = 0 AND (c, d) > (?, ?)", 0, 0)

        # Nulls
        assertInvalidMessage(cql, table, "Invalid null value",
                             "SELECT * FROM %s WHERE a = 0 AND (b, c, d) = (?, ?, ?)", 1, 2, None)
        assertInvalidMessage(cql, table, "Invalid null value",
                             "SELECT * FROM %s WHERE a = 0 AND (b, c, d) IN ((?, ?, ?))", 1, 2, None)
        # Reproduces #13217
        assertInvalidMessage(cql, table, "Invalid null value in condition for column",
                             "SELECT * FROM %s WHERE a = 0 AND (b, c, d) IN ((?, ?, ?), (?, ?, ?))", 1, 2, None, 2, 1, 4)

        # Wrong type for 'd'
        # Cannot be tested in Python (the driver recognizes the wrong type
        # in the bound variable) - so commented out
        #assertInvalid(cql, table, "SELECT * FROM %s WHERE a = 0 AND (b, c, d) = (?, ?, ?)", 1, 2, "foobar")
        #assertInvalid(cql, table, "SELECT * FROM %s WHERE a = 0 AND b = (?, ?, ?)", 1, 2, 3)

        # Mix single and tuple inequalities
        # All of these tests reproduce #4244 - because of this issue Scylla
        # complains that single- and multi-column relations are mixed -
        # instead of complaining about the real error that we try to check.
        # When #4244 is fixed, it is quite likely we'll need to change this
        # test to accept Scylla's error messages, which might be different
        # from Cassandra's.
        assertInvalidMessage(cql, table, "Column \"c\" cannot be restricted by two inequalities not starting with the same column",
                             "SELECT * FROM %s WHERE a = 0 AND (b, c, d) > (?, ?, ?) AND c < ?", 0, 1, 0, 1)
        assertInvalidMessage(cql, table, "Column \"c\" cannot be restricted by two inequalities not starting with the same column",
                            "SELECT * FROM %s WHERE a = 0 AND c > ? AND (b, c, d) < (?, ?, ?)", 1, 1, 1, 0)

        assertInvalidMessage(cql, table, "Multi-column relations can only be applied to clustering columns but was applied to: a",
                             "SELECT * FROM %s WHERE (a, b, c, d) IN ((?, ?, ?, ?))", 0, 1, 2, 3)
        assertInvalidMessage(cql, table, "PRIMARY KEY column \"c\" cannot be restricted as preceding column \"b\" is not restricted",
                             "SELECT * FROM %s WHERE (c, d) IN ((?, ?))", 0, 1)

        assertInvalidMessage(cql, table, "Clustering column \"c\" cannot be restricted (preceding column \"b\" is restricted by a non-EQ relation)",
                             "SELECT * FROM %s WHERE a = ? AND b > ?  AND (c, d) IN ((?, ?))", 0, 0, 0, 0)

        assertInvalidMessage(cql, table, "Clustering column \"c\" cannot be restricted (preceding column \"b\" is restricted by a non-EQ relation)",
                             "SELECT * FROM %s WHERE a = ? AND b > ?  AND (c, d) > (?, ?)", 0, 0, 0, 0)
        assertInvalidMessage(cql, table, "PRIMARY KEY column \"c\" cannot be restricted (preceding column \"b\" is restricted by a non-EQ relation)",
                             "SELECT * FROM %s WHERE a = ? AND (c, d) > (?, ?) AND b > ?  ", 0, 0, 0, 0)

        assertInvalidMessage(cql, table, "Column \"c\" cannot be restricted by two inequalities not starting with the same column",
                             "SELECT * FROM %s WHERE a = ? AND (b, c) > (?, ?) AND (b) < (?) AND (c) < (?)", 0, 0, 0, 0, 0)
        assertInvalidMessage(cql, table, "Column \"c\" cannot be restricted by two inequalities not starting with the same column",
                             "SELECT * FROM %s WHERE a = ? AND (c) < (?) AND (b, c) > (?, ?) AND (b) < (?)", 0, 0, 0, 0, 0)
        assertInvalidMessage(cql, table, "Clustering column \"c\" cannot be restricted (preceding column \"b\" is restricted by a non-EQ relation)",
                             "SELECT * FROM %s WHERE a = ? AND (b) < (?) AND (c) < (?) AND (b, c) > (?, ?)", 0, 0, 0, 0, 0)
        assertInvalidMessage(cql, table, "Clustering column \"c\" cannot be restricted (preceding column \"b\" is restricted by a non-EQ relation)",
                             "SELECT * FROM %s WHERE a = ? AND (b) < (?) AND c < ? AND (b, c) > (?, ?)", 0, 0, 0, 0, 0)

        assertInvalidMessage(cql, table, "Column \"c\" cannot be restricted by two inequalities not starting with the same column",
                             "SELECT * FROM %s WHERE a = ? AND (b, c) > (?, ?) AND (c) < (?)", 0, 0, 0, 0)

@pytest.mark.xfail(reason="Issue #64, #4244")
def testMultiAndSingleColumnRelationMix(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, primary key (a, b, c, d))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 1)

        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 1)

        # Reproduces #64:
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and b = ? and (c, d) = (?, ?)", 0, 1, 0, 0),
                   row(0, 1, 0, 0))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and b IN (?, ?) and (c, d) = (?, ?)", 0, 0, 1, 0, 0),
                   row(0, 0, 0, 0),
                   row(0, 1, 0, 0))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and b = ? and (c) IN ((?))", 0, 1, 0),
                   row(0, 1, 0, 0))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and b IN (?, ?) and (c) IN ((?))", 0, 0, 1, 0),
                   row(0, 0, 0, 0),
                   row(0, 1, 0, 0))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and b = ? and (c) IN ((?), (?))", 0, 1, 0, 1),
                   row(0, 1, 0, 0),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and b = ? and (c, d) IN ((?, ?))", 0, 1, 0, 0),
                   row(0, 1, 0, 0))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and b = ? and (c, d) IN ((?, ?), (?, ?))", 0, 1, 0, 0, 1, 1),
                   row(0, 1, 0, 0),
                   row(0, 1, 1, 1))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and b IN (?, ?) and (c, d) IN ((?, ?), (?, ?))", 0, 0, 1, 0, 0, 1, 1),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 1),
                   row(0, 1, 0, 0),
                   row(0, 1, 1, 1))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and b = ? and (c, d) > (?, ?)", 0, 1, 0, 0),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and b IN (?, ?) and (c, d) > (?, ?)", 0, 0, 1, 0, 0),
                   row(0, 0, 1, 0),
                   row(0, 0, 1, 1),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and b = ? and (c, d) > (?, ?) and (c) <= (?) ", 0, 1, 0, 0, 1),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and b = ? and (c, d) > (?, ?) and c <= ? ", 0, 1, 0, 0, 1),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and b = ? and (c, d) >= (?, ?) and (c, d) < (?, ?)", 0, 1, 0, 0, 1, 1),
                   row(0, 1, 0, 0),
                   row(0, 1, 1, 0))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and (b, c) = (?, ?) and d = ?", 0, 0, 1, 0),
                   row(0, 0, 1, 0))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and (b, c) IN ((?, ?), (?, ?)) and d = ?", 0, 0, 1, 0, 0, 0),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 0))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and b = ? and (c) = (?) and d = ?", 0, 0, 1, 0),
                   row(0, 0, 1, 0))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and (b, c) = (?, ?) and d IN (?, ?)", 0, 0, 1, 0, 2),
                   row(0, 0, 1, 0))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and b = ? and (c) = (?) and d IN (?, ?)", 0, 0, 1, 0, 2),
                   row(0, 0, 1, 0))

        # Reproduces #4244:
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and (b, c) = (?, ?) and d >= ?", 0, 0, 1, 0),
                   row(0, 0, 1, 0),
                   row(0, 0, 1, 1))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and d < 1 and (b, c) = (?, ?) and d >= ?", 0, 0, 1, 0),
                   row(0, 0, 1, 0))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and d < 1 and (b, c) IN ((?, ?), (?, ?)) and d >= ?", 0, 0, 1, 0, 0, 0),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 0))

@pytest.mark.xfail(reason="Issue #64, #4244")
def testSeveralMultiColumnRelation(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, primary key (a, b, c, d))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 1)

        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 1)

        # Reproduces #64:
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and (b) = (?) and (c, d) = (?, ?)", 0, 1, 0, 0),
                   row(0, 1, 0, 0))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and (b) IN ((?), (?)) and (c, d) = (?, ?)", 0, 0, 1, 0, 0),
                   row(0, 0, 0, 0),
                   row(0, 1, 0, 0))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and (b) = (?) and (c) IN ((?))", 0, 1, 0),
                   row(0, 1, 0, 0))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and (b) IN ((?),(?)) and (c) IN ((?))", 0, 0, 1, 0),
                   row(0, 0, 0, 0),
                   row(0, 1, 0, 0))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and (b) = (?) and (c) IN ((?), (?))", 0, 1, 0, 1),
                   row(0, 1, 0, 0),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and (b) = (?) and (c, d) IN ((?, ?))", 0, 1, 0, 0),
                   row(0, 1, 0, 0))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and (b) = (?) and (c, d) IN ((?, ?), (?, ?))", 0, 1, 0, 0, 1, 1),
                   row(0, 1, 0, 0),
                   row(0, 1, 1, 1))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and (b) IN ((?), (?)) and (c, d) IN ((?, ?), (?, ?))", 0, 0, 1, 0, 0, 1, 1),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 1),
                   row(0, 1, 0, 0),
                   row(0, 1, 1, 1))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and (b) = (?) and (c, d) > (?, ?)", 0, 1, 0, 0),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and (b) IN ((?),(?)) and (c, d) > (?, ?)", 0, 0, 1, 0, 0),
                   row(0, 0, 1, 0),
                   row(0, 0, 1, 1),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and (b) = (?) and (c, d) > (?, ?) and (c) <= (?) ", 0, 1, 0, 0, 1),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and (b) = (?) and (c, d) > (?, ?) and c <= ? ", 0, 1, 0, 0, 1),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and (b) = (?) and (c, d) >= (?, ?) and (c, d) < (?, ?)", 0, 1, 0, 0, 1, 1),
                   row(0, 1, 0, 0),
                   row(0, 1, 1, 0))

        # Reproduces #4244:
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and (b, c) = (?, ?) and d = ?", 0, 0, 1, 0),
                   row(0, 0, 1, 0))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and (b, c) IN ((?, ?), (?, ?)) and d = ?", 0, 0, 1, 0, 0, 0),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 0))

        # Reproduces #64:
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and (d) < (1) and (b, c) = (?, ?) and (d) >= (?)", 0, 0, 1, 0),
                   row(0, 0, 1, 0))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and (d) < (1) and (b, c) IN ((?, ?), (?, ?)) and (d) >= (?)", 0, 0, 1, 0, 0, 0),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 0))

def testSinglePartitionInvalidQueries(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int primary key, b int)") as table:
        assertInvalidMessage(cql, table, "Multi-column relations can only be applied to clustering columns but was applied to: a",
                             "SELECT * FROM %s WHERE (a) > (?)", 0)
        assertInvalidMessage(cql, table, "Multi-column relations can only be applied to clustering columns but was applied to: a",
                             "SELECT * FROM %s WHERE (a) = (?)", 0)
        assertInvalidMessage(cql, table, "Multi-column relations can only be applied to clustering columns but was applied to: b",
                             "SELECT * FROM %s WHERE (b) = (?)", 0)

@pytest.mark.xfail(reason="Issue #4244")
def testSingleClustering(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, primary key (a, b))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 2, 0)

        # Equalities

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b) = (?)", 0, 1),
                   row(0, 1, 0)
        )

        # Same but check the whole tuple can be prepared
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b) = ?", 0, (1,)),
                   row(0, 1, 0)
        )

        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b) = (?)", 0, 3))

        # Inequalities

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b) > (?)", 0, 0),
                   row(0, 1, 0),
                   row(0, 2, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b) >= (?)", 0, 1),
                   row(0, 1, 0),
                   row(0, 2, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b) < (?)", 0, 2),
                   row(0, 0, 0),
                   row(0, 1, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b) <= (?)", 0, 1),
                   row(0, 0, 0),
                   row(0, 1, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b) > (?) AND (b) < (?)", 0, 0, 2),
                   row(0, 1, 0)
        )

        # Reproduces #4244:
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b) > (?) AND b < ?", 0, 0, 2),
                   row(0, 1, 0)
        )
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND b > ? AND (b) < (?)", 0, 0, 2),
                   row(0, 1, 0)
        )

def testNonEqualsRelation(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int primary key, b int)") as table:
        assertInvalidMessage(cql, table, "Unsupported \"!=\" relation: (b) != (0)",
                             "SELECT * FROM %s WHERE a = 0 AND (b) != (0)")

@pytest.mark.xfail(reason="Issue #4244")
def testMultipleClustering(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, PRIMARY KEY (a, b, c, d))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 1)

        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 1)

        # Empty query
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE a = 0 AND (b, c, d) IN ()"))

        # Equalities

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b) = (?)", 0, 1),
                   row(0, 1, 0, 0),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1)
        )

        # Same with whole tuple prepared
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b) = ?", 0, (1,)),
                   row(0, 1, 0, 0),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c) = (?, ?)", 0, 1, 1),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1)
        )

        # Same with whole tuple prepared
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c) = ?", 0, (1, 1)),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c, d) = (?, ?, ?)", 0, 1, 1, 1),
                   row(0, 1, 1, 1)
        )

        # Same with whole tuple prepared
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c, d) = ?", 0, (1, 1, 1)),
                   row(0, 1, 1, 1)
        )

        # Inequalities

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b) > (?)", 0, 0),
                   row(0, 1, 0, 0),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b) >= (?)", 0, 0),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 0),
                   row(0, 0, 1, 1),
                   row(0, 1, 0, 0),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c) > (?, ?)", 0, 1, 0),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c) >= (?, ?)", 0, 1, 0),
                   row(0, 1, 0, 0),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c, d) > (?, ?, ?)", 0, 1, 1, 0),
                   row(0, 1, 1, 1)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c, d) >= (?, ?, ?)", 0, 1, 1, 0),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b) < (?)", 0, 1),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 0),
                   row(0, 0, 1, 1)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b) <= (?)", 0, 1),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 0),
                   row(0, 0, 1, 1),
                   row(0, 1, 0, 0),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c) < (?, ?)", 0, 0, 1),
                   row(0, 0, 0, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c) <= (?, ?)", 0, 0, 1),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 0),
                   row(0, 0, 1, 1)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c, d) < (?, ?, ?)", 0, 0, 1, 1),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c, d) <= (?, ?, ?)", 0, 0, 1, 1),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 0),
                   row(0, 0, 1, 1)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c, d) > (?, ?, ?) AND (b) < (?)", 0, 0, 1, 0, 1),
                   row(0, 0, 1, 1)
        )

        # Reproduces #4244:
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c, d) > (?, ?, ?) AND b < ?", 0, 0, 1, 0, 1),
                   row(0, 0, 1, 1)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c, d) > (?, ?, ?) AND (b, c) < (?, ?)", 0, 0, 1, 1, 1, 1),
                   row(0, 1, 0, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c, d) > (?, ?, ?) AND (b, c, d) < (?, ?, ?)", 0, 0, 1, 1, 1, 1, 0),
                   row(0, 1, 0, 0)
        )

        # Same with whole tuple prepared
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c, d) > ? AND (b, c, d) < ?", 0, (0, 1, 1), (1, 1, 0)),
                   row(0, 1, 0, 0)
        )

        # reversed
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b) > (?) ORDER BY b DESC, c DESC, d DESC", 0, 0),
                   row(0, 1, 1, 1),
                   row(0, 1, 1, 0),
                   row(0, 1, 0, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b) >= (?) ORDER BY b DESC, c DESC, d DESC", 0, 0),
                   row(0, 1, 1, 1),
                   row(0, 1, 1, 0),
                   row(0, 1, 0, 0),
                   row(0, 0, 1, 1),
                   row(0, 0, 1, 0),
                   row(0, 0, 0, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c) > (?, ?) ORDER BY b DESC, c DESC, d DESC", 0, 1, 0),
                   row(0, 1, 1, 1),
                   row(0, 1, 1, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c) >= (?, ?) ORDER BY b DESC, c DESC, d DESC", 0, 1, 0),
                   row(0, 1, 1, 1),
                   row(0, 1, 1, 0),
                   row(0, 1, 0, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c, d) > (?, ?, ?) ORDER BY b DESC, c DESC, d DESC", 0, 1, 1, 0),
                   row(0, 1, 1, 1)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c, d) >= (?, ?, ?) ORDER BY b DESC, c DESC, d DESC", 0, 1, 1, 0),
                   row(0, 1, 1, 1),
                   row(0, 1, 1, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b) < (?) ORDER BY b DESC, c DESC, d DESC", 0, 1),
                   row(0, 0, 1, 1),
                   row(0, 0, 1, 0),
                   row(0, 0, 0, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b) <= (?) ORDER BY b DESC, c DESC, d DESC", 0, 1),
                   row(0, 1, 1, 1),
                   row(0, 1, 1, 0),
                   row(0, 1, 0, 0),
                   row(0, 0, 1, 1),
                   row(0, 0, 1, 0),
                   row(0, 0, 0, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c) < (?, ?) ORDER BY b DESC, c DESC, d DESC", 0, 0, 1),
                   row(0, 0, 0, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c) <= (?, ?) ORDER BY b DESC, c DESC, d DESC", 0, 0, 1),
                   row(0, 0, 1, 1),
                   row(0, 0, 1, 0),
                   row(0, 0, 0, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c, d) < (?, ?, ?) ORDER BY b DESC, c DESC, d DESC", 0, 0, 1, 1),
                   row(0, 0, 1, 0),
                   row(0, 0, 0, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c, d) <= (?, ?, ?) ORDER BY b DESC, c DESC, d DESC", 0, 0, 1, 1),
                   row(0, 0, 1, 1),
                   row(0, 0, 1, 0),
                   row(0, 0, 0, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c, d) > (?, ?, ?) AND (b) < (?) ORDER BY b DESC, c DESC, d DESC", 0, 0, 1, 0, 1),
                   row(0, 0, 1, 1)
        )

        # Reproduces #4244:
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c, d) > (?, ?, ?) AND b < ? ORDER BY b DESC, c DESC, d DESC", 0, 0, 1, 0, 1),
                   row(0, 0, 1, 1)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c, d) > (?, ?, ?) AND (b, c) < (?, ?) ORDER BY b DESC, c DESC, d DESC", 0, 0, 1, 1, 1, 1),
                   row(0, 1, 0, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c, d) > (?, ?, ?) AND (b, c, d) < (?, ?, ?) ORDER BY b DESC, c DESC, d DESC", 0, 0, 1, 1, 1, 1, 0),
                   row(0, 1, 0, 0)
        )

        # IN

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c, d) IN ((?, ?, ?), (?, ?, ?))", 0, 0, 1, 0, 0, 1, 1),
                   row(0, 0, 1, 0),
                   row(0, 0, 1, 1)
        )

        # same query but with whole tuple prepared
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c, d) IN (?, ?)", 0, (0, 1, 0), (0, 1, 1)),
                   row(0, 0, 1, 0),
                   row(0, 0, 1, 1)
        )

        # same query but with whole IN list prepared
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c, d) IN ?", 0, [(0, 1, 0), (0, 1, 1)]),
                   row(0, 0, 1, 0),
                   row(0, 0, 1, 1)
        )

        # same query, but reversed order for the IN values
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c, d) IN (?, ?)", 0, (0, 1, 1), (0, 1, 0)),
                   row(0, 0, 1, 0),
                   row(0, 0, 1, 1)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and (b, c) IN ((?, ?))", 0, 0, 1),
                   row(0, 0, 1, 0),
                   row(0, 0, 1, 1)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and (b) IN ((?))", 0, 0),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 0),
                   row(0, 0, 1, 1)
        )

        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE a = ? and (b) IN ()", 0))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c) IN ((?, ?)) ORDER BY b DESC, c DESC, d DESC", 0, 0, 1),
                   row(0, 0, 1, 1),
                   row(0, 0, 1, 0)
        )

        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c) IN () ORDER BY b DESC, c DESC, d DESC", 0))

        # IN on both partition key and clustering key
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 1, 1)

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a IN (?, ?) AND (b, c, d) IN (?, ?)", 0, 1, (0, 1, 0), (0, 1, 1)),
                   row(0, 0, 1, 0),
                   row(0, 0, 1, 1),
                   row(1, 0, 1, 0),
                   row(1, 0, 1, 1)
        )

        # same but with whole IN lists prepared
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a IN ? AND (b, c, d) IN ?", [0, 1], [(0, 1, 0), (0, 1, 1)]),
                   row(0, 0, 1, 0),
                   row(0, 0, 1, 1),
                   row(1, 0, 1, 0),
                   row(1, 0, 1, 1)
        )

        # same query, but reversed order for the IN values
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a IN (?, ?) AND (b, c, d) IN (?, ?)", 1, 0, (0, 1, 1), (0, 1, 0)),
                   row(0, 0, 1, 0),
                   row(0, 0, 1, 1),
                   row(1, 0, 1, 0),
                   row(1, 0, 1, 1)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a IN (?, ?) and (b, c) IN ((?, ?))", 0, 1, 0, 1),
                   row(0, 0, 1, 0),
                   row(0, 0, 1, 1),
                   row(1, 0, 1, 0),
                   row(1, 0, 1, 1)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a IN (?, ?) and (b) IN ((?))", 0, 1, 0),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 0),
                   row(0, 0, 1, 1),
                   row(1, 0, 0, 0),
                   row(1, 0, 1, 0),
                   row(1, 0, 1, 1)
        )

def testMultipleClusteringReversedComponents(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, PRIMARY KEY (a, b, c, d)) WITH CLUSTERING ORDER BY (b DESC, c ASC, d DESC)") as table:
        # b and d are reversed in the clustering order
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 0)

        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 0)


        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b) > (?)", 0, 0),
                   row(0, 1, 0, 0),
                   row(0, 1, 1, 1),
                   row(0, 1, 1, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b) >= (?)", 0, 0),
                   row(0, 1, 0, 0),
                   row(0, 1, 1, 1),
                   row(0, 1, 1, 0),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 1),
                   row(0, 0, 1, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b) < (?)", 0, 1),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 1),
                   row(0, 0, 1, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b) <= (?)", 0, 1),
                   row(0, 1, 0, 0),
                   row(0, 1, 1, 1),
                   row(0, 1, 1, 0),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 1),
                   row(0, 0, 1, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND (b, c, d) IN ((?, ?, ?), (?, ?, ?))", 0, 1, 1, 1, 0, 1, 1),
                   row(0, 1, 1, 1),
                   row(0, 0, 1, 1)
        )

        # same query, but reversed order for the IN values
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a=? AND (b, c, d) IN ((?, ?, ?), (?, ?, ?))", 0, 0, 1, 1, 1, 1, 1),
                   row(0, 1, 1, 1),
                   row(0, 0, 1, 1)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c, d) IN (?, ?, ?, ?, ?, ?)",
                           0, (1, 0, 0), (1, 1, 1), (1, 1, 0), (0, 0, 0), (0, 1, 1), (0, 1, 0)),
                   row(0, 1, 0, 0),
                   row(0, 1, 1, 1),
                   row(0, 1, 1, 0),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 1),
                   row(0, 0, 1, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c) IN (?)", 0, (0, 1)),
                   row(0, 0, 1, 1),
                   row(0, 0, 1, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c) IN (?)", 0, (0, 0)),
                   row(0, 0, 0, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b) IN ((?))", 0, 0),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 1),
                   row(0, 0, 1, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c) > (?, ?)", 0, 1, 0),
                   row(0, 1, 1, 1),
                   row(0, 1, 1, 0)
        )

@pytest.mark.xfail(reason="Issue #4178, #13250")
def testMultipleClusteringWithIndex(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, e int, PRIMARY KEY (a, b, c, d))") as table:
        execute(cql, table, "CREATE INDEX ON %s (b)")
        execute(cql, table, "CREATE INDEX ON %s (e)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 0, 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 0, 1, 0, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 0, 1, 1, 2)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 1, 2)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 2, 0, 0, 0)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a= ? AND (b) = (?)", 0, 1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, 1, 2))
        # Reproduces #13250:
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE (b) = (?)", 1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, 1, 2))

        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE (b, c) = (?, ?)", 1, 1)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c) = (?, ?)", 0, 1, 1),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, 1, 2))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE (b, c) = (?, ?) ALLOW FILTERING", 1, 1),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, 1, 2))

        # Reproduces #4178:
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c) = (?, ?) AND e = ?", 0, 1, 1, 2),
                   row(0, 1, 1, 1, 2))
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE (b, c) = (?, ?) AND e = ?", 1, 1, 2)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE (b, c) = (?, ?) AND e = ? ALLOW FILTERING", 1, 1, 2),
                   row(0, 1, 1, 1, 2))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b) IN ((?)) AND e = ? ALLOW FILTERING", 0, 1, 2),
                   row(0, 1, 1, 1, 2))
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE (b) IN ((?)) AND e = ?", 1, 2)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE (b) IN ((?)) AND e = ? ALLOW FILTERING", 1, 2),
                   row(0, 1, 1, 1, 2))

        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE (b) IN ((?), (?)) AND e = ?", 0, 1, 2)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE (b) IN ((?), (?)) AND e = ? ALLOW FILTERING", 0, 1, 2),
                   row(0, 0, 1, 1, 2),
                   row(0, 1, 1, 1, 2))

        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE (b, c) IN ((?, ?)) AND e = ?", 0, 1, 2)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE (b, c) IN ((?, ?)) AND e = ? ALLOW FILTERING", 0, 1, 2),
                   row(0, 0, 1, 1, 2))

        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE (b, c) IN ((?, ?), (?, ?)) AND e = ?", 0, 1, 1, 1, 2)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE (b, c) IN ((?, ?), (?, ?)) AND e = ? ALLOW FILTERING", 0, 1, 1, 1, 2),
                   row(0, 0, 1, 1, 2),
                   row(0, 1, 1, 1, 2))

        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE (b) >= (?) AND e = ?", 1, 2)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE (b) >= (?) AND e = ? ALLOW FILTERING", 1, 2),
                   row(0, 1, 1, 1, 2))

        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE (b, c) >= (?, ?) AND e = ?", 1, 1, 2)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE (b, c) >= (?, ?) AND e = ? ALLOW FILTERING", 1, 1, 2),
                   row(0, 1, 1, 1, 2))

        # Scylla allows comparison with null, so this check is commented out:
        #assertInvalidMessage(cql, table, "Unsupported null value for column e",
        #                     "SELECT * FROM %s WHERE (b, c) >= (?, ?) AND e = ?  ALLOW FILTERING", 1, 1, None)

        assertInvalidMessage(cql, table, "unset value",
                             "SELECT * FROM %s WHERE (b, c) >= (?, ?) AND e = ?  ALLOW FILTERING", 1, 1, UNSET_VALUE)

@pytest.mark.xfail(reason="Issue #8627")
def testMultipleClusteringWithIndexAndValueOver64K(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b blob, c int, d int, PRIMARY KEY (a, b, c))") as table:
        execute(cql, table, "CREATE INDEX ON %s (b)")

        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, b'x', 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, b'xx', 1, 0)

        # Reproduces #8627:
        assertInvalidMessage(cql, table, "Index expression values may not be larger than 64K",
                             "SELECT * FROM %s WHERE (b, c) = (?, ?) AND d = ?  ALLOW FILTERING", TOO_BIG, 1, 2)

def testMultiColumnRestrictionsWithIndex(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, e int, v int, PRIMARY KEY (a, b, c, d, e))") as table:
        execute(cql, table, "CREATE INDEX ON %s (v)")
        for i in range(1,6):
            execute(cql, table, "INSERT INTO %s (a,b,c,d,e,v) VALUES (?,?,?,?,?,?)", 0, i, 0, 0, 0, 0)
            execute(cql, table, "INSERT INTO %s (a,b,c,d,e,v) VALUES (?,?,?,?,?,?)", 0, i, i, 0, 0, 0)
            execute(cql, table, "INSERT INTO %s (a,b,c,d,e,v) VALUES (?,?,?,?,?,?)", 0, i, i, i, 0, 0)
            execute(cql, table, "INSERT INTO %s (a,b,c,d,e,v) VALUES (?,?,?,?,?,?)", 0, i, i, i, i, 0)
            execute(cql, table, "INSERT INTO %s (a,b,c,d,e,v) VALUES (?,?,?,?,?,?)", 0, i, i, i, i, i)

        # Scylla and Cassandra give different error messages here. Cassandra
        # says "Multi-column slice restrictions cannot be used for filtering."
        # and Scylla: "Clustering columns may not be skipped in multi-column
        # relations. They should appear in the PRIMARY KEY order".
        errorMsg = "ulti-column"
        assertInvalidMessage(cql, table, errorMsg,
                             "SELECT * FROM %s WHERE a = 0 AND (c,d) < (2,2) AND v = 0 ALLOW FILTERING")
        assertInvalidMessage(cql, table, errorMsg,
                             "SELECT * FROM %s WHERE a = 0 AND (d,e) < (2,2) AND b = 1 AND v = 0 ALLOW FILTERING")
        assertInvalidMessage(cql, table, errorMsg,
                             "SELECT * FROM %s WHERE a = 0 AND b = 1 AND (d,e) < (2,2) AND v = 0 ALLOW FILTERING")
        assertInvalidMessage(cql, table, errorMsg,
                             "SELECT * FROM %s WHERE a = 0 AND b > 1 AND (d,e) < (2,2) AND v = 0 ALLOW FILTERING")
        assertInvalidMessage(cql, table, errorMsg,
                             "SELECT * FROM %s WHERE a = 0 AND (b,c) > (1,0) AND (d,e) < (2,2) AND v = 0 ALLOW FILTERING")

@pytest.mark.xfail(reason="Issue #4178")
def testMultiplePartitionKeyAndMultiClusteringWithIndex(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, e int, f int, PRIMARY KEY ((a, b), c, d, e))") as table:
        execute(cql, table, "CREATE INDEX ON %s (c)")
        execute(cql, table, "CREATE INDEX ON %s (f)")

        execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 0, 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 0, 1, 0, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 0, 1, 1, 2)

        execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 1, 0, 0, 3)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 1, 1, 0, 4)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 1, 1, 1, 5)

        execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 2, 0, 0, 5)

        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = ? AND (c) = (?)")
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (c) = (?) ALLOW FILTERING", 0, 1),
                   row(0, 0, 1, 0, 0, 3),
                   row(0, 0, 1, 1, 0, 4),
                   row(0, 0, 1, 1, 1, 5))

        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = ? AND (c, d) = (?, ?)", 0, 1, 1)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (c, d) = (?, ?) ALLOW FILTERING", 0, 1, 1),
                   row(0, 0, 1, 1, 0, 4),
                   row(0, 0, 1, 1, 1, 5))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (c, d) IN ((?, ?)) ALLOW FILTERING", 0, 1, 1),
                row(0, 0, 1, 1, 0, 4),
                row(0, 0, 1, 1, 1, 5))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (c, d) >= (?, ?) ALLOW FILTERING", 0, 1, 1),
                row(0, 0, 1, 1, 0, 4),
                row(0, 0, 1, 1, 1, 5),
                row(0, 0, 2, 0, 0, 5))

        # Reproduces #4178:
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND b = ? AND (c) IN ((?)) AND f = ?", 0, 0, 1, 5),
                   row(0, 0, 1, 1, 1, 5))

        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = ? AND (c) IN ((?), (?)) AND f = ?", 0, 1, 3, 5)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (c) IN ((?), (?)) AND f = ? ALLOW FILTERING", 0, 1, 3, 5),
                   row(0, 0, 1, 1, 1, 5))

        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = ? AND (c) IN ((?), (?)) AND f = ?", 0, 1, 2, 5)

        # Reproduces #4178:
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND b = ? AND (c) IN ((?), (?)) AND f = ?", 0, 0, 1, 2, 5),
                   row(0, 0, 1, 1, 1, 5),
                   row(0, 0, 2, 0, 0, 5))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (c) IN ((?), (?)) AND f = ? ALLOW FILTERING", 0, 1, 2, 5),
                   row(0, 0, 1, 1, 1, 5),
                   row(0, 0, 2, 0, 0, 5))

        # Reproduces #4178:
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND b = ? AND (c, d) IN ((?, ?)) AND f = ?", 0, 0, 1, 0, 3),
                   row(0, 0, 1, 0, 0, 3))

        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = ? AND (c, d) IN ((?, ?)) AND f = ?", 0, 1, 0, 3)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (c, d) IN ((?, ?)) AND f = ? ALLOW FILTERING", 0, 1, 0, 3),
                   row(0, 0, 1, 0, 0, 3))

        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = ? AND (c) >= (?) AND f = ?", 0, 1, 5)

        # Reproduces #4178:
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND b = ? AND (c) >= (?) AND f = ?", 0, 0, 1, 5),
                   row(0, 0, 1, 1, 1, 5),
                   row(0, 0, 2, 0, 0, 5))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (c) >= (?) AND f = ? ALLOW FILTERING", 0, 1, 5),
                   row(0, 0, 1, 1, 1, 5),
                   row(0, 0, 2, 0, 0, 5))

        # Reproduces #4178:
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND b = ? AND (c, d) >= (?, ?) AND f = ?", 0, 0, 1, 1, 5),
                   row(0, 0, 1, 1, 1, 5),
                   row(0, 0, 2, 0, 0, 5))

        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = ? AND (c, d) >= (?, ?) AND f = ?", 0, 1, 1, 5)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (c, d) >= (?, ?) AND f = ? ALLOW FILTERING", 0, 1, 1, 5),
                   row(0, 0, 1, 1, 1, 5),
                   row(0, 0, 2, 0, 0, 5))

def testINWithDuplicateValue(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k1 int, k2 int, v int, PRIMARY KEY (k1, k2))") as table:
        execute(cql, table, "INSERT INTO %s (k1,  k2, v) VALUES (?, ?, ?)", 1, 1, 1)

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE k1 IN (?, ?) AND (k2) IN ((?), (?))", 1, 1, 1, 2),
                   row(1, 1, 1))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE k1 = ? AND (k2) IN ((?), (?))", 1, 1, 1),
                   row(1, 1, 1))

@pytest.mark.xfail(reason="Issue #13250")
def testWithUnsetValues(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, i int, j int, s text, PRIMARY KEY (k,i,j))") as table:
        execute(cql, table, "CREATE INDEX s_index ON %s (s)")

        assertInvalidMessage(cql, table, "unset value",
                             "SELECT * from %s WHERE (i, j) = (?,?) ALLOW FILTERING", unset(), 1)
        assertInvalidMessage(cql, table, "unset value",
                             "SELECT * from %s WHERE (i, j) IN ((?,?)) ALLOW FILTERING", unset(), 1)
        assertInvalidMessage(cql, table, "unset value",
                             "SELECT * from %s WHERE (i, j) > (1,?) ALLOW FILTERING", unset())
        assertInvalidMessage(cql, table, "unset value",
                             "SELECT * from %s WHERE (i, j) = ? ALLOW FILTERING", unset())
        # Reproduces 13250:
        assertInvalidMessage(cql, table, "unset value",
                             "SELECT * from %s WHERE i = ? AND (j) > ? ALLOW FILTERING", 1, unset())
        assertInvalidMessage(cql, table, "unset value",
                             "SELECT * from %s WHERE (i, j) IN (?, ?) ALLOW FILTERING", unset(), (1, 1))
        assertInvalidMessage(cql, table, "unset value",
                             "SELECT * from %s WHERE (i, j) IN ? ALLOW FILTERING", unset())

def testMixedOrderColumns1(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, e int, PRIMARY KEY (a, b, c, d, e)) WITH CLUSTERING ORDER BY (b DESC, c ASC, d DESC, e ASC)") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 2, 0, -1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 2, 0, -1, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 2, 0, 1, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, -1, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, -1, 1, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, -1, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 1, -1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 1, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 0, -1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 0, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, -1, -1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, -1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, -1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, -1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 0, 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, -1, 0, -1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, -1, 0, 0, 0)
        assertRows(execute(cql, table, 
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d,e)<=(?,?,?,?) " +
        "AND (b)>(?)", 0, 2, 0, 1, 1, -1),

                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 0, 0, 0, 0)
        )


        assertRows(execute(cql, table, 
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d,e)<=(?,?,?,?) " +
        "AND (b)>=(?)", 0, 2, 0, 1, 1, -1),

                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0)
        )

        assertRows(execute(cql, table, 
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d)>=(?,?,?)" +
        "AND (b,c,d,e)<(?,?,?,?) ", 0, 1, 1, 0, 1, 1, 0, 1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0)

        )

        assertRows(execute(cql, table, 
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d,e)>(?,?,?,?)" +
        "AND (b,c,d)<=(?,?,?) ", 0, -1, 0, -1, -1, 2, 0, -1),

                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0)
        )

        assertRows(execute(cql, table, 
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d,e) < (?,?,?,?) " +
        "AND (b,c,d,e)>(?,?,?,?)", 0, 1, 0, 0, 0, 1, 0, -1, -1),
                   row(0, 1, 0, 0, -1)
        )

        assertRows(execute(cql, table, 
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d,e) <= (?,?,?,?) " +
        "AND (b,c,d,e)>(?,?,?,?)", 0, 1, 0, 0, 0, 1, 0, -1, -1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0)
        )

        assertRows(execute(cql, table, 
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b)<(?) " +
        "AND (b,c,d,e)>(?,?,?,?)", 0, 2, -1, 0, -1, -1),

                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0)

        )


        assertRows(execute(cql, table, 
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b)<(?) " +
        "AND (b)>(?)", 0, 2, -1),

                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 0, 0, 0, 0)

        )

        assertRows(execute(cql, table, 
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b)<(?) " +
        "AND (b)>=(?)", 0, 2, -1),

                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0)
        )

        assertRows(execute(cql, table, 
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d,e)<=(?,?,?,?) " +
        "AND (b,c,d,e)>(?,?,?,?)", 0, 2, 0, 1, 1, -1, 0, -1, -1),

                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0)
        )

        assertRows(execute(cql, table, 
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c)<=(?,?) " +
        "AND (b,c,d,e)>(?,?,?,?)", 0, 2, 0, -1, 0, -1, -1),

                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0)
        )

        assertRows(execute(cql, table, 
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d)<=(?,?,?) " +
        "AND (b,c,d,e)>(?,?,?,?)", 0, 2, 0, -1, -1, 0, -1, -1),

                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0)
        )

        assertRows(execute(cql, table, 
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d,e)>(?,?,?,?)" +
        "AND (b,c,d)<=(?,?,?) ", 0, -1, 0, -1, -1, 2, 0, -1),

                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0)
        )

        assertRows(execute(cql, table, 
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d)>=(?,?,?)" +
        "AND (b,c,d,e)<(?,?,?,?) ", 0, 1, 1, 0, 1, 1, 0, 1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0)
        )
        assertRows(execute(cql, table, 
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d,e)<(?,?,?,?) " +
        "AND (b,c,d)>=(?,?,?)", 0, 1, 1, 0, 1, 1, 1, 0),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0)

        )

        assertRows(execute(cql, table, 
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c)<(?,?) " +
        "AND (b,c,d,e)>(?,?,?,?)", 0, 2, 0, -1, 0, -1, -1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0)
        )

        assertRows(execute(cql, table, 
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c)<(?,?) " +
        "AND (b,c,d,e)>(?,?,?,?)", 0, 2, 0, -1, 0, -1, -1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b,c,d,e) <= (?,?,?,?)", 0, 1, 0, 0, 0),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, -1, -1),
                   row(0, 0, 0, 0, 0),
                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b,c,d,e) > (?,?,?,?)", 0, 1, 0, 0, 0),
                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b,c,d,e) >= (?,?,?,?)", 0, 1, 0, 0, 0),
                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b,c,d) >= (?,?,?)", 0, 1, 0, 0),
                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b,c,d) > (?,?,?)", 0, 1, 0, 0),
                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0)
        )

def testMixedOrderColumns2(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, e int, PRIMARY KEY (a, b, c, d, e)) WITH CLUSTERING ORDER BY (b DESC, c ASC, d ASC, e ASC)") as table:
        # b and d are reversed in the clustering order
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 2, 0, -1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 2, 0, -1, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, -1, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, -1, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, -1, 1, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 1, -1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 1, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 0, -1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 0, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, -1, -1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, -1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, -1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, -1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 0, 0, 0, 0)

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b,c,d,e) <= (?,?,?,?)", 0, 1, 0, 0, 0),
                   row(0, 1, -1, 0, 0),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 0, 0, 0, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b,c,d,e) > (?,?,?,?)", 0, 1, 0, 0, 0),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1)
        )
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b,c,d,e) >= (?,?,?,?)", 0, 1, 0, 0, 0),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1)
        )

def testMixedOrderColumns3(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, PRIMARY KEY (a, b, c)) WITH CLUSTERING ORDER BY (b DESC, c ASC)") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?,?,?);", 0, 2, 3)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?,?,?);", 0, 2, 4)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?,?,?);", 0, 4, 4)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?,?,?);", 0, 3, 4)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?,?,?);", 0, 4, 5)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?,?,?);", 0, 4, 6)


        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b,c)>=(?,?) AND (b,c)<(?,?) ALLOW FILTERING", 0, 2, 3, 4, 5),
                   row(0, 4, 4), row(0, 3, 4), row(0, 2, 3), row(0, 2, 4)
        )
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b,c)>=(?,?) AND (b,c)<=(?,?) ALLOW FILTERING", 0, 2, 3, 4, 5),
                   row(0, 4, 4), row(0, 4, 5), row(0, 3, 4), row(0, 2, 3), row(0, 2, 4)
        )
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b,c)<(?,?) ALLOW FILTERING", 0, 4, 5),
                   row(0, 4, 4), row(0, 3, 4), row(0, 2, 3), row(0, 2, 4)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b,c)>(?,?) ALLOW FILTERING", 0, 4, 5),
                   row(0, 4, 6)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b)<(?) and (b)>(?) ALLOW FILTERING", 0, 4, 2),
                   row(0, 3, 4)
        )

def testMixedOrderColumns4(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, e int, PRIMARY KEY (a, b, c, d, e)) WITH CLUSTERING ORDER BY (b ASC, c DESC, d DESC, e ASC)") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 2, 0, -1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 2, 0, -1, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 2, 0, 1, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 2, -1, 1, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 2, -3, 1, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, -1, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, -1, 1, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, -1, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 1, -1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 1, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 0, -1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 0, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, -1, -1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, -1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, -1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, -1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 0, 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, -1, 0, -1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, -1, 0, 0, 0)

        assertRows(execute(cql, table, 
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d,e)<(?,?,?,?) " +
        "AND (b,c,d,e)>(?,?,?,?)", 0, 2, 0, 1, 1, -1, 0, -1, -1),

                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 2, -1, 1, 1),
                   row(0, 2, -3, 1, 1)

        )


        assertRows(execute(cql, table, 
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d,e) < (?,?,?,?) " +
        "AND (b,c,d,e)>(?,?,?,?)", 0, 1, 0, 0, 0, 1, 0, -1, -1),
                   row(0, 1, 0, 0, -1)
        )

        assertRows(execute(cql, table, 
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d,e) <= (?,?,?,?) " +
        "AND (b,c,d,e)>(?,?,?,?)", 0, 1, 0, 0, 0, 1, 0, -1, -1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0)
        )


        assertRows(execute(cql, table, 
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d,e)<=(?,?,?,?) " +
        "AND (b,c,d,e)>(?,?,?,?)", 0, 2, 0, 1, 1, -1, 0, -1, -1),

                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 2, -1, 1, 1),
                   row(0, 2, -3, 1, 1)
        )

        assertRows(execute(cql, table, 
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c)<=(?,?) " +
        "AND (b,c,d,e)>(?,?,?,?)", 0, 2, 0, -1, 0, -1, -1),

                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 2, -1, 1, 1),
                   row(0, 2, -3, 1, 1)
        )

        assertRows(execute(cql, table, 
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c)<(?,?) " +
        "AND (b,c,d,e)>(?,?,?,?)", 0, 2, 0, -1, 0, -1, -1),
                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 2, -1, 1, 1),
                   row(0, 2, -3, 1, 1)
        )

        assertRows(execute(cql, table, 
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d,e)<=(?,?,?,?) " +
        "AND (b)>=(?)", 0, 2, 0, 1, 1, -1),

                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 2, -1, 1, 1),
                   row(0, 2, -3, 1, 1)
        )

        assertRows(execute(cql, table, 
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d,e)<=(?,?,?,?) " +
        "AND (b)>(?)", 0, 2, 0, 1, 1, -1),

                   row(0, 0, 0, 0, 0),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 2, -1, 1, 1),
                   row(0, 2, -3, 1, 1)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b,c,d,e) <= (?,?,?,?)", 0, 1, 0, 0, 0),
                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b,c,d,e) > (?,?,?,?)", 0, 1, 0, 0, 0),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, 1),
                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 2, -1, 1, 1),
                   row(0, 2, -3, 1, 1)

        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b,c,d,e) >= (?,?,?,?)", 0, 1, 0, 0, 0),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 2, -1, 1, 1),
                   row(0, 2, -3, 1, 1)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b,c,d) >= (?,?,?)", 0, 1, 0, 0),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 2, -1, 1, 1),
                   row(0, 2, -3, 1, 1)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b,c,d) > (?,?,?)", 0, 1, 0, 0),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 2, -1, 1, 1),
                   row(0, 2, -3, 1, 1)
        )

        assertRows(execute(cql, table, 
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b) < (?) ", 0, 0),
                   row(0, -1, 0, 0, 0), row(0, -1, 0, -1, 0)
        )
        assertRows(execute(cql, table, 
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b) <= (?) ", 0, -1),
                   row(0, -1, 0, 0, 0), row(0, -1, 0, -1, 0)
        )
        assertRows(execute(cql, table, 
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d,e) < (?,?,?,?) and (b,c,d,e) > (?,?,?,?) ", 0, 2, 0, 0, 0, 2, -2, 0, 0),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 2, -1, 1, 1)
        )

def testMixedOrderColumnsInReverse(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, PRIMARY KEY (a, b, c)) WITH CLUSTERING ORDER BY (b ASC, c DESC)") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (0, 1, 3)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (0, 1, 2)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (0, 1, 1)")

        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (0, 2, 3)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (0, 2, 2)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (0, 2, 1)")

        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (0, 3, 3)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (0, 3, 2)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (0, 3, 1)")

        assertRows(execute(cql, table, "SELECT b, c FROM %s WHERE a = 0 AND (b, c) >= (2, 2) ORDER BY b DESC, c ASC;"),
                   row(3, 1),
                   row(3, 2),
                   row(3, 3),
                   row(2, 2),
                   row(2, 3))

# Check select on tuple relations, see CASSANDRA-8613
# migrated from cql_tests.py:TestCQL.simple_tuple_query_test()
@pytest.mark.xfail(reason="Issue #64")
def testSimpleTupleQuery(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, e int, PRIMARY KEY (a, b, c, d, e))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (0, 2, 0, 0, 0)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (0, 1, 0, 0, 0)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (0, 0, 0, 0, 0)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (0, 0, 1, 1, 1)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (0, 0, 2, 2, 2)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (0, 0, 3, 3, 3)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d, e) VALUES (0, 0, 1, 1, 1)")

        # Reproduces #64:
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE b=0 AND (c, d, e) > (1, 1, 1) ALLOW FILTERING"),
                   row(0, 0, 2, 2, 2),
                   row(0, 0, 3, 3, 3))

def testInvalidColumnNames(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, PRIMARY KEY (a, b, c))") as table:
        assertInvalidMessage(cql, table, "name e", "SELECT * FROM %s WHERE (b, e) = (0, 0)")
        assertInvalidMessage(cql, table, "name e", "SELECT * FROM %s WHERE (b, e) IN ((0, 1), (2, 4))")
        assertInvalidMessage(cql, table, "name e", "SELECT * FROM %s WHERE (b, e) > (0, 1) and b <= 2")
        # Scylla and Cassandra complain about different things in the following
        # queries. Cassandra complains that undefined e is used in the WHERE.
        # Scylla complains that this e is an alias (defined by AS) and can't
        # be used in the where.
        assertInvalid(cql, table, "SELECT c AS e FROM %s WHERE (b, e) = (0, 0)")
        assertInvalid(cql, table, "SELECT c AS e FROM %s WHERE (b, e) IN ((0, 1), (2, 4))")
        assertInvalid(cql, table, "SELECT c AS e FROM %s WHERE (b, e) > (0, 1) and b <= 2")
