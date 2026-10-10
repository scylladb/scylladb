# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2023-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

from ...porting import *

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

# The steps that Cassandra 6 added to testSingleClusteringInvalidQueries for
# its new BETWEEN operator. They are in a separate test so the original test
# keeps running on older Cassandra and on Scylla.
# Reproduces SCYLLADB-5153 (the BETWEEN operator)
@pytest.mark.xfail(reason="SCYLLADB-5153")
def testSingleClusteringInvalidQueriesWithBetween(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(a int, b int, c int, primary key (a, b))") as table:
        assertInvalidMessage(cql, table, "cannot be restricted by",
                             "SELECT * FROM %s WHERE a = 0 AND (b) = (?) AND (b) BETWEEN (?) AND (?)", 0, 0, 0)
        # As in testSingleClusteringInvalidQueries, Scylla complains about
        # mixing single- and multi-column relations (#4244), so we don't
        # check the message here.
        assertInvalid(cql, table,
                             "SELECT * FROM %s WHERE a = 0 AND (b) > (?) AND b BETWEEN ? AND ?", 0, 1, 1)
        assertInvalid(cql, table,
                             "SELECT * FROM %s WHERE a = 0 AND (b) > (?) AND b BETWEEN (?) AND (?)", 0, 1, 0)

# Issue #13241 used to crash Scylla on this test; it is now fixed.
# The test is still expected to fail due to the unrelated issue #4244.
@pytest.mark.xfail(reason="Issue #4244")
def testMultiClusteringInvalidQueries(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, primary key (a, b, c, d))") as table:
        assertInvalidSyntax(cql, table, "SELECT * FROM %s WHERE a = 0 AND (b, c) > ()")
        # Cassandra 6 changed the following messages. Scylla's messages are
        # the same as Cassandra 5's.
        assertInvalidMessageRE(cql, table, r"Expected 2 elements in value tuple, but got 3: \(\?, \?, \?\)|Invalid tuple literal for \(b, c\): too many elements. Type frozen<tuple<int, int>> expects 2 but got 3",
                             "SELECT * FROM %s WHERE a = 0 AND (b, c) > (?, ?, ?)", 1, 2, 3)
        assertInvalidMessageRE(cql, table, r"Invalid null value in condition for column c|Invalid null value for c in tuple \(b, c\)",
                             "SELECT * FROM %s WHERE a = 0 AND (b, c) > (?, ?)", 1, None)

        # Wrong order of columns
        assertInvalidMessage(cql, table, "PRIMARY KEY order",
                             "SELECT * FROM %s WHERE a = 0 AND (d, c, b) = (?, ?, ?)", 0, 0, 0)
        assertInvalidMessage(cql, table, "PRIMARY KEY order",
                             "SELECT * FROM %s WHERE a = 0 AND (d, c, b) > (?, ?, ?)", 0, 0, 0)

        # Wrong number of values
        # Reproduces #13241:
        assertInvalidMessageRE(cql, table, r"Expected 3 elements in value (for )?tuple( \(b, c, d\))?, but got 2: \(\?, \?\)",
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
        assertInvalidMessage(cql, table, "Invalid null value",
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

# The steps that Cassandra 6 added to testMultiClusteringInvalidQueries for
# its new BETWEEN operator. They are in a separate test so the original test
# keeps running on older Cassandra and on Scylla.
# Reproduces SCYLLADB-5153 (the BETWEEN operator) and #4244 (see the comments
# in testMultiClusteringInvalidQueries)
@pytest.mark.xfail(reason="SCYLLADB-5153, Issue #4244")
def testMultiClusteringInvalidQueriesWithBetween(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, primary key (a, b, c, d))") as table:
        # Nulls
        # Scylla does not consider a comparison with null an error, rather it
        # just matches nothing. See discussion in
        # test_null.py::test_filtering_inequality_null
        #assertInvalidMessage(cql, table, "Invalid null value for column b",
        #                     "SELECT * FROM %s WHERE a = 0 AND b BETWEEN ? AND ?", 1, None)

        assertInvalidMessage(cql, table, "Clustering column \"c\" cannot be restricted (preceding column \"b\" is restricted by a non-EQ relation)",
                             "SELECT * FROM %s WHERE a = ? AND b BETWEEN ? AND ?  AND (c, d) > (?, ?)", 0, 0, 0, 0, 0)

        assertInvalidMessage(cql, table, "Column \"c\" cannot be restricted by two inequalities not starting with the same column",
                             "SELECT * FROM %s WHERE a = ? AND (c) < (?) AND (b, c) > (?, ?) AND (b) BETWEEN (?) AND (?)", 0, 0, 0, 0, 0, 0)

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

# The steps that Cassandra 6 added to testMultiAndSingleColumnRelationMix for
# its new BETWEEN operator. They are in a separate test so the original test
# keeps running on older Cassandra and on Scylla.
# Reproduces SCYLLADB-5153 (the BETWEEN operator) and #4244 (mixing single-
# and multi-column restrictions)
@pytest.mark.xfail(reason="SCYLLADB-5153, Issue #4244")
def testMultiAndSingleColumnRelationMixWithBetween(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, primary key (a, b, c, d))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 1)

        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 1)

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? and d BETWEEN 0 AND 0 and (b, c) IN ((?, ?), (?, ?))", 0, 0, 1, 0, 0),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 0))

        # BETWEEN with inverted bounds returns empty result (first bound > second bound)
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c, d) BETWEEN (?, ?, ?) AND (?, ?, ?)", 0, 0, 1, 0, 0, 0, 0))

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

# The step that Cassandra 6 added to testSingleClustering for its new BETWEEN
# operator. It is in a separate test so the original test keeps running on
# older Cassandra and on Scylla.
# Reproduces SCYLLADB-5153 (the BETWEEN operator) and SCYLLADB-5192 (the
# parenthesized "(?)" in "b BETWEEN (?) AND (?)" is treated as a tuple)
@pytest.mark.xfail(reason="SCYLLADB-5153, SCYLLADB-5192")
def testSingleClusteringWithBetween(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(a int, b int, c int, primary key (a, b))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 2, 0)

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND b BETWEEN (?) AND (?)", 0, 1, 1),
                   row(0, 1, 0)
        )

# Before Cassandra 6, this test checked that the "!=" operator is not
# supported. Cassandra 6 added support for it (CASSANDRA-18584), and
# replaced this test by the following test of this support.
# Reproduces #12911 (the "!=" operator in WHERE)
@pytest.mark.xfail(reason="#12911")
def testNonEqualsRelation(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(a int, b int, c int, PRIMARY KEY (a, b, c))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 1, 1)

        # Excluding subtrees
        assertRows(execute(cql, table, "SELECT a, b, c FROM %s WHERE a = ? AND (b) != (?)", 0, 0),
                   row(0, 1, 0),
                   row(0, 1, 1)
        )
        assertRows(execute(cql, table, "SELECT a, b, c FROM %s WHERE a = ? AND (b) != (?)", 0, 1),
                   row(0, 0, 0),
                   row(0, 0, 1)
        )
        assertRows(execute(cql, table, "SELECT a, b, c FROM %s WHERE a = ? AND (b) != (?)", 0, -1),
                   row(0, 0, 0),
                   row(0, 0, 1),
                   row(0, 1, 0),
                   row(0, 1, 1)
        )
        assertRows(execute(cql, table, "SELECT a, b, c FROM %s WHERE a = ? AND (b) != (?)", 0, 2),
                   row(0, 0, 0),
                   row(0, 0, 1),
                   row(0, 1, 0),
                   row(0, 1, 1)
        )

        # Excluding single rows
        assertRows(execute(cql, table, "SELECT a, b, c FROM %s WHERE a = ? AND (b, c) != (?, ?)", 0, -1, -1),
                   row(0, 0, 0),
                   row(0, 0, 1),
                   row(0, 1, 0),
                   row(0, 1, 1)
        )
        assertRows(execute(cql, table, "SELECT a, b, c FROM %s WHERE a = ? AND (b, c) != (?, ?)", 0, 0, 1),
                   row(0, 0, 0),
                   row(0, 1, 0),
                   row(0, 1, 1)
        )
        assertRows(execute(cql, table, "SELECT a, b, c FROM %s WHERE a = ? AND (b, c) != (?, ?)", 0, 2, 2),
                   row(0, 0, 0),
                   row(0, 0, 1),
                   row(0, 1, 0),
                   row(0, 1, 1)
        )

        # Merging multiple != =
        assertRows(execute(cql, table, "SELECT a, b, c FROM %s WHERE a = ? AND (b, c) != ? AND (b, c) != ?", 0, (0, 1), (1, 0)),
                   row(0, 0, 0),
                   row(0, 1, 1)
        )
        assertRows(execute(cql, table, "SELECT a, b, c FROM %s WHERE a = ? AND b != ? AND (b, c) != ?", 0, 1, (0, 1)),
                   row(0, 0, 0)
        )
        assertRows(execute(cql, table, "SELECT a, b, c FROM %s WHERE a = ? AND (b) != (?) AND (b, c) != ?", 0, 1, (0, 1)),
                   row(0, 0, 0)
        )

        # Merging with < <= >= >
        assertRows(execute(cql, table, "SELECT a, b, c FROM %s WHERE a = ? AND b < ? AND (b, c) != ?", 0, 1, (0, 1)),
                   row(0, 0, 0)
        )
        assertRows(execute(cql, table, "SELECT a, b, c FROM %s WHERE a = ? AND b <= ? AND (b, c) != ?", 0, 0, (0, 1)),
                   row(0, 0, 0)
        )
        assertRows(execute(cql, table, "SELECT a, b, c FROM %s WHERE a = ? AND b > ? AND (b, c) != ?", 0, 0, (1, 1)),
                   row(0, 1, 0)
        )
        assertRows(execute(cql, table, "SELECT a, b, c FROM %s WHERE a = ? AND b >= ? AND (b, c) != ?", 0, 1, (1, 1)),
                   row(0, 1, 0)
        )
        assertRows(execute(cql, table, "SELECT a, b, c FROM %s WHERE a = ? AND (b, c) < ? AND (b, c) != ?", 0, (0, 2), (0, 1)),
                   row(0, 0, 0)
        )
        assertRows(execute(cql, table, "SELECT a, b, c FROM %s WHERE a = ? AND (b, c) <= ? AND (b, c) != ?", 0, (0, 2), (0, 1)),
                   row(0, 0, 0)
        )
        assertRows(execute(cql, table, "SELECT a, b, c FROM %s WHERE a = ? AND (b, c) > ? AND (b, c) != ?", 0, (0, 0), (1, 1)),
                   row(0, 0, 1),
                   row(0, 1, 0)
        )
        assertRows(execute(cql, table, "SELECT a, b, c FROM %s WHERE a = ? AND (b, c) >= ? AND (b, c) != ?", 0, (0, 1), (1, 1)),
                   row(0, 0, 1),
                   row(0, 1, 0)
        )

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

# The steps that Cassandra 6 added to testMultipleClustering for its new
# BETWEEN operator. They are in a separate test so the original test keeps
# running on older Cassandra and on Scylla.
# Reproduces SCYLLADB-5153 (the BETWEEN operator)
@pytest.mark.xfail(reason="SCYLLADB-5153")
def testMultipleClusteringWithBetween(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, PRIMARY KEY (a, b, c, d))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 1)

        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 1)

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b) BETWEEN (?) AND (?)", 0, 0, 1),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 0),
                   row(0, 0, 1, 1),
                   row(0, 1, 0, 0),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c) BETWEEN (?, ?) AND (?, ?)", 0, 1, 0, 1, 1),
                   row(0, 1, 0, 0),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c, d) BETWEEN (?, ?, ?) AND (?, ?, ?)", 0, 1, 1, 0, 1, 1, 1),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b) BETWEEN (?) AND (?) ORDER BY b DESC, c DESC, d DESC", 0, 0, 1),
                   row(0, 1, 1, 1),
                   row(0, 1, 1, 0),
                   row(0, 1, 0, 0),
                   row(0, 0, 1, 1),
                   row(0, 0, 1, 0),
                   row(0, 0, 0, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c) BETWEEN (?, ?) AND (?, ?) ORDER BY b DESC, c DESC, d DESC", 0, 1, 0, 1, 1),
                   row(0, 1, 1, 1),
                   row(0, 1, 1, 0),
                   row(0, 1, 0, 0)
        )

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c, d) BETWEEN (?, ?, ?) AND (?, ?, ?) ORDER BY b DESC, c DESC, d DESC", 0, 1, 1, 0, 1, 1, 1),
                   row(0, 1, 1, 1),
                   row(0, 1, 1, 0)
        )

        # BETWEEN with inverted bounds returns empty result (first bound > second bound)
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c) BETWEEN (?, ?) AND (?, ?) ORDER BY b DESC, c DESC, d DESC", 0, 0, 1, 0, 0))

        # BETWEEN with inverted bounds returns empty result (first bound > second bound)
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b, c, d) BETWEEN (?, ?, ?) AND (?, ?, ?) ORDER BY b DESC, c DESC, d DESC", 0, 0, 1, 1, 0, 0, 0))

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

# The step that Cassandra 6 added to testMultipleClusteringReversedComponents
# for its new BETWEEN operator. It is in a separate test so the original test
# keeps running on older Cassandra and on Scylla.
# Reproduces SCYLLADB-5153 (the BETWEEN operator)
@pytest.mark.xfail(reason="SCYLLADB-5153")
def testMultipleClusteringReversedComponentsWithBetween(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, PRIMARY KEY (a, b, c, d)) WITH CLUSTERING ORDER BY (b DESC, c ASC, d DESC)") as table:
        # b and d are reversed in the clustering order
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 0)

        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 1)
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 0)

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b) BETWEEN (?) AND (?)", 0, 0, 1),
                   row(0, 1, 0, 0),
                   row(0, 1, 1, 1),
                   row(0, 1, 1, 0),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 1),
                   row(0, 0, 1, 0)
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
        #assertInvalidMessage(cql, table, "Invalid null value for column e",
        #                     "SELECT * FROM %s WHERE (b, c) >= (?, ?) AND e = ?  ALLOW FILTERING", 1, 1, None)

        assertInvalidMessage(cql, table, "unset value",
                             "SELECT * FROM %s WHERE (b, c) >= (?, ?) AND e = ?  ALLOW FILTERING", 1, 1, UNSET_VALUE)

# The steps that Cassandra 6 added to testMultipleClusteringWithIndex for its
# new BETWEEN operator. They are in a separate test so the original test keeps
# running on older Cassandra and on Scylla.
# Reproduces SCYLLADB-5153 (the BETWEEN operator)
@pytest.mark.xfail(reason="SCYLLADB-5153")
def testMultipleClusteringWithIndexWithBetween(cql, test_keyspace, new_to_cassandra_6):
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

        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE (b) BETWEEN (?) AND (?) AND e = ?", 1, 10, 2)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE (b) BETWEEN (?) AND (?) AND e = ? ALLOW FILTERING", 1, 10, 2),
                   row(0, 1, 1, 1, 2))

        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                            "SELECT * FROM %s WHERE (b, c) BETWEEN (?, ?) AND (?, ?) AND e = ?", 1, 1, 1, 2, 2)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE (b, c) BETWEEN (?, ?) AND (?, ?) AND e = ? ALLOW FILTERING", 1, 1, 2, 2, 2),
                   row(0, 1, 1, 1, 2))

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

# The step that Cassandra 6 added to testMultiColumnRestrictionsWithIndex for
# its new BETWEEN operator. It is in a separate test so the original test
# keeps running on older Cassandra and on Scylla.
# Reproduces SCYLLADB-5153 (the BETWEEN operator)
@pytest.mark.xfail(reason="SCYLLADB-5153")
def testMultiColumnRestrictionsWithIndexWithBetween(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(a int, b int, c int, d int, e int, v int, PRIMARY KEY (a, b, c, d, e))") as table:
        execute(cql, table, "CREATE INDEX ON %s (v)")
        for i in range(1,6):
            execute(cql, table, "INSERT INTO %s (a,b,c,d,e,v) VALUES (?,?,?,?,?,?)", 0, i, 0, 0, 0, 0)
            execute(cql, table, "INSERT INTO %s (a,b,c,d,e,v) VALUES (?,?,?,?,?,?)", 0, i, i, 0, 0, 0)
            execute(cql, table, "INSERT INTO %s (a,b,c,d,e,v) VALUES (?,?,?,?,?,?)", 0, i, i, i, 0, 0)
            execute(cql, table, "INSERT INTO %s (a,b,c,d,e,v) VALUES (?,?,?,?,?,?)", 0, i, i, i, i, 0)
            execute(cql, table, "INSERT INTO %s (a,b,c,d,e,v) VALUES (?,?,?,?,?,?)", 0, i, i, i, i, i)

        # As in testMultiColumnRestrictionsWithIndex, Scylla and Cassandra
        # give different error messages here.
        errorMsg = "ulti-column"
        assertInvalidMessage(cql, table, errorMsg,
                             "SELECT * FROM %s WHERE a = 0 AND (b,c) > (1,0) AND (d,e) BETWEEN (1,0) AND (2,2) AND v = 0 ALLOW FILTERING")

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

# The steps that Cassandra 6 added to testMultiplePartitionKeyAndMultiClusteringWithIndex
# for its new BETWEEN operator. They are in a separate test so the original
# test keeps running on older Cassandra and on Scylla.
# Reproduces SCYLLADB-5153 (the BETWEEN operator) and #4178 (the queries
# with "a = ? AND b = ?" and without ALLOW FILTERING, as in the original test)
@pytest.mark.xfail(reason="SCYLLADB-5153, Issue #4178")
def testMultiplePartitionKeyAndMultiClusteringWithIndexWithBetween(cql, test_keyspace, new_to_cassandra_6):
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

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (c, d) BETWEEN (?, ?) AND (?, ?) ALLOW FILTERING", 0, 1, 1, 4, 3),
                   row(0, 0, 1, 1, 0, 4),
                   row(0, 0, 1, 1, 1, 5),
                   row(0, 0, 2, 0, 0, 5))

        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = ? AND (c) BETWEEN (?) AND (?) AND f = ?", 0, 1, 1, 5)

        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = ? AND (c) BETWEEN (?) AND (?) AND f = ?", 0, 1, -5, 5)

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND b = ? AND (c) BETWEEN (?) AND (?) AND f = ?", 0, 0, 1, 2, 5),
                   row(0, 0, 1, 1, 1, 5),
                   row(0, 0, 2, 0, 0, 5))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND b = ? AND (c) BETWEEN (?) AND (?) AND f = ?", 0, 0, 0, 4, 5),
                   row(0, 0, 1, 1, 1, 5),
                   row(0, 0, 2, 0, 0, 5))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (c) BETWEEN (?) AND (?) AND f = ? ALLOW FILTERING", 0, 1, 2, 5),
                   row(0, 0, 1, 1, 1, 5),
                   row(0, 0, 2, 0, 0, 5))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (c) BETWEEN (?) AND (?) AND f = ? ALLOW FILTERING", 0, 1, 4, 5),
                   row(0, 0, 1, 1, 1, 5),
                   row(0, 0, 2, 0, 0, 5))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND b = ? AND (c, d) BETWEEN (?, ?) AND (?, ?) AND f = ?", 0, 0, 1, 1, 2, 0, 5),
                   row(0, 0, 1, 1, 1, 5),
                   row(0, 0, 2, 0, 0, 5))
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = ? AND (c, d) BETWEEN (?, ?) AND (?, ?) AND f = ?", 0, 1, 1, 2, 0, 5)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (c, d) >= (?, ?) AND f = ? ALLOW FILTERING", 0, 1, 1, 5),
                   row(0, 0, 1, 1, 1, 5),
                   row(0, 0, 2, 0, 0, 5))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND b = ? AND (c, d) BETWEEN (?, ?) AND (?, ?) AND f = ?", 0, 0, 1, 1, 2, 0, 5),
                   row(0, 0, 1, 1, 1, 5),
                   row(0, 0, 2, 0, 0, 5))
        assertInvalidMessage(cql, table, REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = ? AND (c, d) BETWEEN (?, ?) AND (?, ?) AND f = ?", 0, 1, 1, 5, -6, 3)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (c, d) BETWEEN (?, ?) AND (?, ?) AND f = ? ALLOW FILTERING", 0, 1, 1, 2, 0, 5),
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

# The steps that Cassandra 6 added to testMixedOrderColumns1 for its new BETWEEN
# operator. They are in a separate test so the original test keeps running on
# older Cassandra and on Scylla.
# Reproduces SCYLLADB-5153 (the BETWEEN operator)
@pytest.mark.xfail(reason="SCYLLADB-5153")
def testMixedOrderColumns1WithBetween(cql, test_keyspace, new_to_cassandra_6):
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

        # BETWEEN with inverted bounds returns empty result (first bound > second bound in logical order)
        assertEmpty(execute(cql, table,
                   "SELECT * FROM %s" +
                   " WHERE a = ? " +
                   "AND (b,c,d,e) BETWEEN (?,?,?,?) " +
                   "AND (?,?,?,?)", 0, 2, 0, 1, 1, -1, -1, -1, -1))

        # BETWEEN with inverted bounds returns empty result (first bound > second bound in logical order)
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b,c,d,e) BETWEEN (?,?,?,?) AND (?,?,?,?)", 0, 1, 0, 0, 0, -10, -1, -4, -1))

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

# The step that Cassandra 6 added to testMixedOrderColumns2 for its new BETWEEN
# operator. It is in a separate test so the original test keeps running on
# older Cassandra and on Scylla.
# Reproduces SCYLLADB-5153 (the BETWEEN operator)
@pytest.mark.xfail(reason="SCYLLADB-5153")
def testMixedOrderColumns2WithBetween(cql, test_keyspace, new_to_cassandra_6):
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

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b,c,d,e) BETWEEN (?,?,?,?) AND (?,?,?,?)", 0, 1, 0, 0, 0, 2, 2, 2, 2),
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

# The step that Cassandra 6 added to testMixedOrderColumns3 for its new BETWEEN
# operator. It is in a separate test so the original test keeps running on
# older Cassandra and on Scylla.
# Reproduces SCYLLADB-5153 (the BETWEEN operator)
@pytest.mark.xfail(reason="SCYLLADB-5153")
def testMixedOrderColumns3WithBetween(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(a int, b int, c int, PRIMARY KEY (a, b, c)) WITH CLUSTERING ORDER BY (b DESC, c ASC)") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?,?,?);", 0, 2, 3)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?,?,?);", 0, 2, 4)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?,?,?);", 0, 4, 4)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?,?,?);", 0, 3, 4)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?,?,?);", 0, 4, 5)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (?,?,?);", 0, 4, 6)

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b,c) BETWEEN (?,?) AND (?,?) ALLOW FILTERING", 0, 2, 3, 4, 5),
                   row(0, 4, 4), row(0, 4, 5), row(0, 3, 4), row(0, 2, 3), row(0, 2, 4)
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

# The steps that Cassandra 6 added to testMixedOrderColumns4 for its new BETWEEN
# operator. They are in a separate test so the original test keeps running on
# older Cassandra and on Scylla.
# Reproduces SCYLLADB-5153 (the BETWEEN operator)
@pytest.mark.xfail(reason="SCYLLADB-5153")
def testMixedOrderColumns4WithBetween(cql, test_keyspace, new_to_cassandra_6):
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

        # BETWEEN with inverted bounds returns empty result (first bound > second bound in logical order)
        assertEmpty(execute(cql, table,
                   "SELECT * FROM %s" +
                   " WHERE a = ? " +
                   "AND (b,c,d,e) BETWEEN (?,?,?,?) " +
                   "AND (?,?,?,?)", 0, 2, 0, 1, 1, -1, -10, -10, -10))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = ? AND (b,c,d) BETWEEN (?,?,?) AND (?,?,?)", 0, 1, 0, 0, 2, 2, 2),
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
# The step that Cassandra 6 added to testMixedOrderColumnsInReverse for its new BETWEEN
# operator. It is in a separate test so the original test keeps running on
# older Cassandra and on Scylla.
# Reproduces SCYLLADB-5153 (the BETWEEN operator)
@pytest.mark.xfail(reason="SCYLLADB-5153")
def testMixedOrderColumnsInReverseWithBetween(cql, test_keyspace, new_to_cassandra_6):
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

        assertRows(execute(cql, table, "SELECT b, c FROM %s WHERE a = 0 AND (b, c) BETWEEN (2, 2) AND (3, 3) ORDER BY b DESC, c ASC;"),
                   row(3, 1),
                   row(3, 2),
                   row(3, 3),
                   row(2, 2),
                   row(2, 3))

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

# Reproduces #13250 (one-element multi-column restriction "(c2)" should be
# handled like a single-column restriction "c2")
@pytest.mark.xfail(reason="Issue #13250")
def testInRestrictionsWithAllowFiltering(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int, c1 text, c2 int, c3 int, v int, primary key(pk, c1, c2, c3))") as table:
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, c3, v) values (?, ?, ?, ?, ?)", 1, "0", 0, 1, 3)
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, c3, v) values (?, ?, ?, ?, ?)", 1, "1", 0, 2, 4)
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, c3, v) values (?, ?, ?, ?, ?)", 1, "1", 1, 3, 5)
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, c3, v) values (?, ?, ?, ?, ?)", 1, "2", 1, 4, 6)
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, c3, v) values (?, ?, ?, ?, ?)", 1, "2", 2, 5, 7)

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE (c2) IN ((?), (?)) ALLOW FILTERING", 1, 3),
                   row(1, "1", 1, 3, 5),
                   row(1, "2", 1, 4, 6))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE c2 IN (?, ?) ALLOW FILTERING", 1, 3),
                   row(1, "1", 1, 3, 5),
                   row(1, "2", 1, 4, 6))

        # Scylla's error message is different: "Clustering columns may not
        # be skipped in multi-column relations. They should appear in the
        # PRIMARY KEY order"
        assertInvalidMessageRE(cql, table, "Multicolumn IN filters are not supported|Clustering columns may not be skipped in multi-column relations",
                             "SELECT * FROM %s WHERE (c2, c3) IN ((?, ?), (?, ?)) ALLOW FILTERING", 1, 0, 2, 0)

# Reproduces #13250 (one-element multi-column restriction "(c3)" should be
# handled like a single-column restriction "c3")
@pytest.mark.xfail(reason="Issue #13250")
def testInRestrictionsWithIndex(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int, c1 text, c2 int, c3 int, v int, primary key(pk, c1, c2, c3))") as table:
        execute(cql, table, "CREATE INDEX ON %s (c3)")
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, c3, v) values (?, ?, ?, ?, ?)", 1, "0", 0, 1, 3)
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, c3, v) values (?, ?, ?, ?, ?)", 1, "1", 0, 2, 4)
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, c3, v) values (?, ?, ?, ?, ?)", 1, "1", 1, 3, 5)

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE (c3) IN ((?), (?)) ALLOW FILTERING", 1, 3),
                   row(1, "0", 0, 1, 3),
                   row(1, "1", 1, 3, 5))

        # Scylla's error message is different: "Clustering columns may not
        # be skipped in multi-column relations. They should appear in the
        # PRIMARY KEY order"
        assertInvalidMessageRE(cql, table, "PRIMARY KEY column \"c2\" cannot be restricted as preceding column \"c1\" is not restricted|Clustering columns may not be skipped in multi-column relations",
                             "SELECT * FROM %s WHERE (c2, c3) IN ((?, ?), (?, ?))", 1, 0, 2, 0)

        assertInvalidMessageRE(cql, table, "Multicolumn IN filters are not supported|Clustering columns may not be skipped in multi-column relations",
                             "SELECT * FROM %s WHERE (c2, c3) IN ((?, ?), (?, ?)) ALLOW FILTERING", 1, 0, 2, 0)

# Reproduces #12911 (multi-column NOT IN)
@pytest.mark.xfail(reason="#12911")
def testNotInRestrictionsWithClustering(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(pk int, c1 int, c2 int, v int, primary key(pk, c1, c2))") as table:
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) values (?, ?, ?, ?)", 1, 0, 1, 11)
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) values (?, ?, ?, ?)", 1, 0, 2, 12)
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) values (?, ?, ?, ?)", 1, 0, 3, 13)
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) values (?, ?, ?, ?)", 1, 0, 4, 14)
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) values (?, ?, ?, ?)", 1, 1, 1, 21)
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) values (?, ?, ?, ?)", 1, 1, 2, 22)
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) values (?, ?, ?, ?)", 1, 1, 3, 23)
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) values (?, ?, ?, ?)", 1, 1, 4, 24)

        # empty NOT IN
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) NOT IN ()", 1),
                   row(1, 0, 1, 11),
                   row(1, 0, 2, 12),
                   row(1, 0, 3, 13),
                   row(1, 0, 4, 14),
                   row(1, 1, 1, 21),
                   row(1, 1, 2, 22),
                   row(1, 1, 3, 23),
                   row(1, 1, 4, 24))

        # non existent NOT IN:
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) NOT IN ((?, ?))", 1, 2000, 2001),
                   row(1, 0, 1, 11),
                   row(1, 0, 2, 12),
                   row(1, 0, 3, 13),
                   row(1, 0, 4, 14),
                   row(1, 1, 1, 21),
                   row(1, 1, 2, 22),
                   row(1, 1, 3, 23),
                   row(1, 1, 4, 24))

        # existing values in NOT IN, different ways of passing them:
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) NOT IN ((0, 2), (0, 3), (1, 1), (1, 4))", 1),
                   row(1, 0, 1, 11),
                   row(1, 0, 4, 14),
                   row(1, 1, 2, 22),
                   row(1, 1, 3, 23))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) NOT IN ((?, ?), (?, ?), (?, ?), (?, ?))",
                           1, 0, 2, 0, 3, 1, 1, 1, 4),
                   row(1, 0, 1, 11),
                   row(1, 0, 4, 14),
                   row(1, 1, 2, 22),
                   row(1, 1, 3, 23))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) NOT IN (?, ?, ?, ?)",
                           1, (0, 2), (0, 3), (1, 1), (1, 4)),
                   row(1, 0, 1, 11),
                   row(1, 0, 4, 14),
                   row(1, 1, 2, 22),
                   row(1, 1, 3, 23))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) NOT IN ?",
                           1, [(0, 2), (0, 3), (1, 1), (1, 4)]),
                   row(1, 0, 1, 11),
                   row(1, 0, 4, 14),
                   row(1, 1, 2, 22),
                   row(1, 1, 3, 23))

        # Tuples given in arbitrary order:
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) NOT IN (?, ?, ?, ?)",
                           1, (0, 3), (1, 4), (1, 1), (0, 2)),
                   row(1, 0, 1, 11),
                   row(1, 0, 4, 14),
                   row(1, 1, 2, 22),
                   row(1, 1, 3, 23))

        # Multiple NOT IN:
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) NOT IN (?, ?) AND (c1, c2) NOT IN (?, ?)",
                           1, (0, 2), (0, 3), (1, 1), (1, 4)),
                   row(1, 0, 1, 11),
                   row(1, 0, 4, 14),
                   row(1, 1, 2, 22),
                   row(1, 1, 3, 23))

        # Multiple NOT IN, mixed markers:
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) NOT IN (?, ?) AND (c1, c2) NOT IN ?",
                           1, (0, 2), (0, 3), [(1, 1), (1, 4)]),
                   row(1, 0, 1, 11),
                   row(1, 0, 4, 14),
                   row(1, 1, 2, 22),
                   row(1, 1, 3, 23))

        # Multiple NOT IN, mixed markers and values:
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) NOT IN ((0, 2), (0, 3)) AND (c1, c2) NOT IN ?",
                           1, [(1, 1), (1, 4)]),
                   row(1, 0, 1, 11),
                   row(1, 0, 4, 14),
                   row(1, 1, 2, 22),
                   row(1, 1, 3, 23))

        # Mixed single-column and multicolumn restrictions:
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND c1 NOT IN ? AND (c1, c2) NOT IN ?",
                           1, [0], [(1, 1), (1, 4)]),
                   row(1, 1, 2, 22),
                   row(1, 1, 3, 23))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND c1 NOT IN (?) AND (c1, c2) NOT IN (?, ?)",
                           1, 0, (1, 1), (1, 4)),
                   row(1, 1, 2, 22),
                   row(1, 1, 3, 23))

# Reproduces #12911 (multi-column NOT IN)
@pytest.mark.xfail(reason="#12911")
def testNotInRestrictionsWithClusteringAndSlices(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(pk int, c1 int, c2 int, v int, primary key(pk, c1, c2))") as table:
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) values (?, ?, ?, ?)", 1, 0, 1, 11)
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) values (?, ?, ?, ?)", 1, 0, 2, 12)
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) values (?, ?, ?, ?)", 1, 0, 3, 13)
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) values (?, ?, ?, ?)", 1, 0, 4, 14)
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) values (?, ?, ?, ?)", 1, 1, 1, 21)
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) values (?, ?, ?, ?)", 1, 1, 2, 22)
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) values (?, ?, ?, ?)", 1, 1, 3, 23)
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) values (?, ?, ?, ?)", 1, 1, 4, 24)

        # NOT IN values outside of slice bounds
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) < ? AND (c1, c2) NOT IN (?)",
                           1, (1, 2), (2, 5)),
                   row(1, 0, 1, 11),
                   row(1, 0, 2, 12),
                   row(1, 0, 3, 13),
                   row(1, 0, 4, 14),
                   row(1, 1, 1, 21))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) > ? AND (c1, c2) NOT IN (?)",
                           1, (1, 2), (1, 1)),
                   row(1, 1, 3, 23),
                   row(1, 1, 4, 24))

        # Empty result set
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) > ? AND (c1, c2) NOT IN (?, ?)",
                           1, (1, 2), (1, 3), (1, 4)))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) < ? AND (c1, c2) NOT IN (?, ?)",
                           1, (0, 3), (0, 2), (0, 1)))

        # NOT IN values inside slice bounds
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) < ? AND (c1, c2) NOT IN (?)",
                           1, (1, 2), (0, 2)),
                   row(1, 0, 1, 11),
                   row(1, 0, 3, 13),
                   row(1, 0, 4, 14),
                   row(1, 1, 1, 21))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) <= ? AND (c1, c2) NOT IN (?)",
                           1, (1, 2), (0, 2)),
                   row(1, 0, 1, 11),
                   row(1, 0, 3, 13),
                   row(1, 0, 4, 14),
                   row(1, 1, 1, 21),
                   row(1, 1, 2, 22))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) > ? AND (c1, c2) NOT IN (?)",
                           1, (1, 1), (1, 3)),
                   row(1, 1, 2, 22),
                   row(1, 1, 4, 24))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) >= ? AND (c1, c2) NOT IN (?)",
                           1, (1, 1), (1, 3)),
                   row(1, 1, 1, 21),
                   row(1, 1, 2, 22),
                   row(1, 1, 4, 24))


        # One NOT IN value exactly the same as the slice bound
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) < ? AND (c1, c2) NOT IN (?, ?)",
                           1, (1, 2), (0, 2), (1, 2)),
                   row(1, 0, 1, 11),
                   row(1, 0, 3, 13),
                   row(1, 0, 4, 14),
                   row(1, 1, 1, 21))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) <= ? AND (c1, c2) NOT IN (?, ?)",
                           1, (1, 2), (0, 2), (1, 2)),
                   row(1, 0, 1, 11),
                   row(1, 0, 3, 13),
                   row(1, 0, 4, 14),
                   row(1, 1, 1, 21))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) > ? AND (c1, c2) NOT IN (?)",
                           1, (1, 1), (1, 1)),
                   row(1, 1, 2, 22),
                   row(1, 1, 3, 23),
                   row(1, 1, 4, 24))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) >= ? AND (c1, c2) NOT IN (?)",
                           1, (1, 1), (1, 1)),
                   row(1, 1, 2, 22),
                   row(1, 1, 3, 23),
                   row(1, 1, 4, 24))

        # NOT IN with both upper and lower bound
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) > ? AND (c1, c2) < ? AND (c1, c2) NOT IN (?)",
                           1, (0, 2), (1, 2), (0, 4)),
                   row(1, 0, 3, 13),
                   row(1, 1, 1, 21))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) >= ? AND (c1, c2) < ? AND (c1, c2) NOT IN (?)",
                           1, (0, 2), (1, 2), (0, 4)),
                   row(1, 0, 2, 12),
                   row(1, 0, 3, 13),
                   row(1, 1, 1, 21))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) > ? AND (c1, c2) <= ? AND (c1, c2) NOT IN (?)",
                           1, (0, 2), (1, 2), (0, 4)),
                   row(1, 0, 3, 13),
                   row(1, 1, 1, 21),
                   row(1, 1, 2, 22))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) >= ? AND (c1, c2) <= ? AND (c1, c2) NOT IN (?)",
                           1, (0, 2), (1, 2), (0, 4)),
                   row(1, 0, 2, 12),
                   row(1, 0, 3, 13),
                   row(1, 1, 1, 21),
                   row(1, 1, 2, 22))

        # Mixed multi-column NOT IN with single column slice restriction:
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND c1 > ? AND (c1, c2) NOT IN (?)",
                           1, 0, (1, 2)),
                   row(1, 1, 1, 21),
                   row(1, 1, 3, 23),
                   row(1, 1, 4, 24))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND c1 >= ? AND (c1, c2) NOT IN (?)",
                           1, 1, (1, 2)),
                   row(1, 1, 1, 21),
                   row(1, 1, 3, 23),
                   row(1, 1, 4, 24))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND c1 < ? AND (c1, c2) NOT IN (?)",
                           1, 1, (0, 4)),
                   row(1, 0, 1, 11),
                   row(1, 0, 2, 12),
                   row(1, 0, 3, 13))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND c1 <= ? AND (c1, c2) NOT IN (?)",
                           1, 0, (0, 4)),
                   row(1, 0, 1, 11),
                   row(1, 0, 2, 12),
                   row(1, 0, 3, 13))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND c1 >= ? AND c1 <= ? AND (c1, c2) NOT IN (?)",
                           1, 0, 1, (0, 4)),
                   row(1, 0, 1, 11),
                   row(1, 0, 2, 12),
                   row(1, 0, 3, 13),
                   row(1, 1, 1, 21),
                   row(1, 1, 2, 22),
                   row(1, 1, 3, 23),
                   row(1, 1, 4, 24))

        # Mixed single-column NOT IN with multi-column slice restriction:
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) > ? AND c1 NOT IN (?)",
                           1, (0, 1), 1),
                   row(1, 0, 2, 12),
                   row(1, 0, 3, 13),
                   row(1, 0, 4, 14))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) >= ? AND c1 NOT IN (?)",
                           1, (0, 1), 1),
                   row(1, 0, 1, 11),
                   row(1, 0, 2, 12),
                   row(1, 0, 3, 13),
                   row(1, 0, 4, 14))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) < ? AND c1 NOT IN (?)",
                           1, (1, 3), 1),
                   row(1, 0, 1, 11),
                   row(1, 0, 2, 12),
                   row(1, 0, 3, 13),
                   row(1, 0, 4, 14))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) < ? AND c1 NOT IN (?)",
                           1, (1, 3), 0),
                   row(1, 1, 1, 21),
                   row(1, 1, 2, 22))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) <= ? AND c1 NOT IN (?)",
                           1, (1, 3), 0),
                   row(1, 1, 1, 21),
                   row(1, 1, 2, 22),
                   row(1, 1, 3, 23))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) >= ? AND (c1, c2) <= ? AND c1 NOT IN (?)",
                           1, (0, 2), (1, 3), 0),
                   row(1, 1, 1, 21),
                   row(1, 1, 2, 22),
                   row(1, 1, 3, 23))

# Reproduces #12911 (multi-column NOT IN)
@pytest.mark.xfail(reason="#12911")
def testNotInRestrictionsWithMixedOrderClusteringAndSlices(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(pk int, c1 int, c2 int, v int, primary key(pk, c1, c2)) WITH CLUSTERING ORDER BY (c1 DESC, c2 ASC)") as table:
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) values (?, ?, ?, ?)", 1, 0, 1, 11)
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) values (?, ?, ?, ?)", 1, 0, 2, 12)
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) values (?, ?, ?, ?)", 1, 0, 3, 13)
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) values (?, ?, ?, ?)", 1, 0, 4, 14)
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) values (?, ?, ?, ?)", 1, 1, 1, 21)
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) values (?, ?, ?, ?)", 1, 1, 2, 22)
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) values (?, ?, ?, ?)", 1, 1, 3, 23)
        execute(cql, table, "INSERT INTO %s (pk, c1, c2, v) values (?, ?, ?, ?)", 1, 1, 4, 24)

        # NOT IN values outside of slice bounds
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) < ? AND (c1, c2) NOT IN (?)",
                           1, (1, 2), (2, 5)),
                   row(1, 1, 1, 21),
                   row(1, 0, 1, 11),
                   row(1, 0, 2, 12),
                   row(1, 0, 3, 13),
                   row(1, 0, 4, 14))

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) > ? AND (c1, c2) NOT IN (?)",
                           1, (1, 2), (1, 1)),
                   row(1, 1, 3, 23),
                   row(1, 1, 4, 24))

        # Empty result set
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) > ? AND (c1, c2) NOT IN (?, ?)",
                           1, (1, 2), (1, 3), (1, 4)))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) < ? AND (c1, c2) NOT IN (?, ?)",
                           1, (0, 3), (0, 2), (0, 1)))

        # NOT IN values inside slice bounds
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) < ? AND (c1, c2) NOT IN (?)",
                           1, (1, 2), (0, 2)),
                   row(1, 1, 1, 21),
                   row(1, 0, 1, 11),
                   row(1, 0, 3, 13),
                   row(1, 0, 4, 14))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) <= ? AND (c1, c2) NOT IN (?)",
                           1, (1, 2), (0, 2)),
                   row(1, 1, 1, 21),
                   row(1, 1, 2, 22),
                   row(1, 0, 1, 11),
                   row(1, 0, 3, 13),
                   row(1, 0, 4, 14))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) > ? AND (c1, c2) NOT IN (?)",
                           1, (1, 1), (1, 3)),
                   row(1, 1, 2, 22),
                   row(1, 1, 4, 24))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) >= ? AND (c1, c2) NOT IN (?)",
                           1, (1, 1), (1, 3)),
                   row(1, 1, 1, 21),
                   row(1, 1, 2, 22),
                   row(1, 1, 4, 24))

        # One NOT IN value exactly the same as the slice bound
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) < ? AND (c1, c2) NOT IN (?, ?)",
                           1, (1, 2), (0, 2), (1, 2)),
                   row(1, 1, 1, 21),
                   row(1, 0, 1, 11),
                   row(1, 0, 3, 13),
                   row(1, 0, 4, 14))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) <= ? AND (c1, c2) NOT IN (?, ?)",
                           1, (1, 2), (0, 2), (1, 2)),
                   row(1, 1, 1, 21),
                   row(1, 0, 1, 11),
                   row(1, 0, 3, 13),
                   row(1, 0, 4, 14))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) > ? AND (c1, c2) NOT IN (?)",
                           1, (1, 1), (1, 1)),
                   row(1, 1, 2, 22),
                   row(1, 1, 3, 23),
                   row(1, 1, 4, 24))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) >= ? AND (c1, c2) NOT IN (?)",
                           1, (1, 1), (1, 1)),
                   row(1, 1, 2, 22),
                   row(1, 1, 3, 23),
                   row(1, 1, 4, 24))


        # NOT IN with both upper and lower bound
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) > ? AND (c1, c2) < ? AND (c1, c2) NOT IN (?)",
                           1, (0, 2), (1, 2), (0, 4)),
                   row(1, 1, 1, 21),
                   row(1, 0, 3, 13))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) >= ? AND (c1, c2) < ? AND (c1, c2) NOT IN (?)",
                           1, (0, 2), (1, 2), (0, 4)),
                   row(1, 1, 1, 21),
                   row(1, 0, 2, 12),
                   row(1, 0, 3, 13))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) > ? AND (c1, c2) <= ? AND (c1, c2) NOT IN (?)",
                           1, (0, 2), (1, 2), (0, 4)),
                   row(1, 1, 1, 21),
                   row(1, 1, 2, 22),
                   row(1, 0, 3, 13))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) >= ? AND (c1, c2) <= ? AND (c1, c2) NOT IN (?)",
                           1, (0, 2), (1, 2), (0, 4)),
                   row(1, 1, 1, 21),
                   row(1, 1, 2, 22),
                   row(1, 0, 2, 12),
                   row(1, 0, 3, 13))

        # Mixed multi-column NOT IN with single column slice restriction:
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND c1 > ? AND (c1, c2) NOT IN (?)",
                           1, 0, (1, 2)),
                   row(1, 1, 1, 21),
                   row(1, 1, 3, 23),
                   row(1, 1, 4, 24))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND c1 >= ? AND (c1, c2) NOT IN (?)",
                           1, 1, (1, 2)),
                   row(1, 1, 1, 21),
                   row(1, 1, 3, 23),
                   row(1, 1, 4, 24))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND c1 < ? AND (c1, c2) NOT IN (?)",
                           1, 1, (0, 4)),
                   row(1, 0, 1, 11),
                   row(1, 0, 2, 12),
                   row(1, 0, 3, 13))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND c1 <= ? AND (c1, c2) NOT IN (?)",
                           1, 0, (0, 4)),
                   row(1, 0, 1, 11),
                   row(1, 0, 2, 12),
                   row(1, 0, 3, 13))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND c1 >= ? AND c1 <= ? AND (c1, c2) NOT IN (?)",
                           1, 0, 1, (0, 4)),
                   row(1, 1, 1, 21),
                   row(1, 1, 2, 22),
                   row(1, 1, 3, 23),
                   row(1, 1, 4, 24),
                   row(1, 0, 1, 11),
                   row(1, 0, 2, 12),
                   row(1, 0, 3, 13))

        # Mixed single-column NOT IN with multi-column slice restriction:
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) > ? AND c1 NOT IN (?)",
                           1, (0, 1), 1),
                   row(1, 0, 2, 12),
                   row(1, 0, 3, 13),
                   row(1, 0, 4, 14))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) >= ? AND c1 NOT IN (?)",
                           1, (0, 1), 1),
                   row(1, 0, 1, 11),
                   row(1, 0, 2, 12),
                   row(1, 0, 3, 13),
                   row(1, 0, 4, 14))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) < ? AND c1 NOT IN (?)",
                           1, (1, 3), 1),
                   row(1, 0, 1, 11),
                   row(1, 0, 2, 12),
                   row(1, 0, 3, 13),
                   row(1, 0, 4, 14))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) < ? AND c1 NOT IN (?)",
                           1, (1, 3), 0),
                   row(1, 1, 1, 21),
                   row(1, 1, 2, 22))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) <= ? AND c1 NOT IN (?)",
                           1, (1, 3), 0),
                   row(1, 1, 1, 21),
                   row(1, 1, 2, 22),
                   row(1, 1, 3, 23))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) >= ? AND (c1, c2) <= ? AND c1 NOT IN (?)",
                           1, (0, 2), (1, 3), 0),
                   row(1, 1, 1, 21),
                   row(1, 1, 2, 22),
                   row(1, 1, 3, 23))

        # Mixed single-column and multi column slices with multi column NOT IN:
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) > ? AND c1 < ? AND (c1, c2) NOT IN (?)",
                           1, (0, 1), 1, (0, 3)),
                   row(1, 0, 2, 12),
                   row(1, 0, 4, 14))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND (c1, c2) > ? AND c1 <= ? AND (c1, c2) NOT IN (?)",
                           1, (0, 1), 0, (0, 3)),
                   row(1, 0, 2, 12),
                   row(1, 0, 4, 14))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND c1 > ? AND (c1, c2) < ? AND (c1, c2) NOT IN (?)",
                           1, 0, (1, 4), (1, 2)),
                   row(1, 1, 1, 21),
                   row(1, 1, 3, 23))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND c1 >= ? AND (c1, c2) < ? AND (c1, c2) NOT IN (?)",
                           1, 1, (1, 4), (1, 2)),
                   row(1, 1, 1, 21),
                   row(1, 1, 3, 23))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND c1 > ? AND c1 < ? AND (c1, c2) NOT IN (?)",
                           1, 0, 2, (1, 2)),
                   row(1, 1, 1, 21),
                   row(1, 1, 3, 23),
                   row(1, 1, 4, 24))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND c1 >= ? AND c1 <= ? AND (c1, c2) NOT IN (?)",
                           1, 1, 1, (1, 2)),
                   row(1, 1, 1, 21),
                   row(1, 1, 3, 23),
                   row(1, 1, 4, 24))
