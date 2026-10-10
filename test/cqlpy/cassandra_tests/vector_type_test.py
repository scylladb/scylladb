# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# This is a translation of VectorTypeTest.java from Cassandra's
# test/unit/org/apache/cassandra/index/sai/cql directory, which tests vector
# search. On Scylla, these tests need a vector store (run test/cqlpy/run with
# the "--vs" option), and they wait for the vector store to catch up after
# creating an index or writing - see the explanation in vector_tester.py.
#
# The Java test is parameterized by forceBruteForceQueries, an internal
# switch of Cassandra's SAI, so it runs each test three times. We run each
# test once.

from .porting import *
from .vector_tester import create_table, create_index, wait_for_vector_writes
from ..util import wait_for_vector_search
from cassandra.protocol import InvalidRequest

def assertContainsInt(result, columnName, columnValue):
    for row in result:
        if getattr(row, columnName) == columnValue:
            return
    raise AssertionError(f"Result set does not contain a row with {columnName} = {columnValue}")

def testendToEndTest(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', [3.0, 4.0, 5.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (3, 'D', [4.0, 5.0, 6.0])")
        wait_for_vector_writes(cql, table, 4)

        result = execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 3")
        assertRowCount(result, 3)

        flush(cql, table)
        result = execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 3")
        assertRowCount(result, 3)

        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (4, 'E', [5.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (5, 'F', [6.0, 3.0, 4.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (6, 'G', [7.0, 4.0, 5.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (7, 'H', [8.0, 5.0, 6.0])")

        flush(cql, table)
        compact(cql, table)
        wait_for_vector_writes(cql, table, 8)

        result = execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 5")
        assertRowCount(result, 5)

        # some data that only lives in memtable
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (8, 'I', [9.0, 5.0, 6.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (9, 'J', [10.0, 6.0, 7.0])")
        wait_for_vector_writes(cql, table, 10)
        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [9.5, 5.5, 6.5] LIMIT 5"))
        assertContainsInt(result, "pk", 8)
        assertContainsInt(result, "pk", 9)

        # data from sstables
        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 2"))
        assertContainsInt(result, "pk", 1)
        assertContainsInt(result, "pk", 2)

# The test warningIsIssuedOnIndexCreation was not translated, because it
# checks Cassandra's warning, on creating a vector index, that its SAI vector
# indexes are experimental and don't support paging or consistency levels
# higher than ONE - this warning is about Cassandra's implementation.

def testcreateIndexAfterInsertTest(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))") as table:
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', [3.0, 4.0, 5.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (3, 'D', [4.0, 5.0, 6.0])")

        flush(cql, table)
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")
        wait_for_vector_writes(cql, table, 4)

        result = execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 3")
        assertRowCount(result, 3)

# The tests testTwoPredicates, testTwoPredicatesManyRows and
# testThreePredicates were not translated, because they create SAI indexes
# on non-vector columns (to filter a vector search by them), which Scylla
# doesn't support. Scylla can filter a vector search only by columns that
# were declared as part of the vector index itself.

def testSameVectorMultipleRows(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (1, 'A', [1.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (2, 'A', [1.0, 2.0, 3.0])")
        wait_for_vector_writes(cql, table, 3)

        result = execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 3")
        assertRowCount(result, 3)

        flush(cql, table)
        compact(cql, table)

        result = execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 3")
        assertRowCount(result, 3)

def testQueryEmptyTable(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        result = execute(cql, table, "SELECT * FROM %s ORDER BY val ANN OF [2.5, 3.5, 4.5] LIMIT 1")
        assertRowCount(result, 0)

def testQueryTableWithNulls(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', null)")
        result = execute(cql, table, "SELECT * FROM %s ORDER BY val ANN OF [2.5, 3.5, 4.5] LIMIT 1")
        assertRowCount(result, 0)

        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [4.0, 5.0, 6.0])")
        wait_for_vector_writes(cql, table, 1)
        result = execute(cql, table, "SELECT pk FROM %s ORDER BY val ANN OF [2.5, 3.5, 4.5] LIMIT 1")
        assertRows(result, row(1))

def testLimitLessThanInsertedRowCount(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        # Insert more rows than the query limit
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [4.0, 5.0, 6.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', [7.0, 8.0, 9.0])")
        wait_for_vector_writes(cql, table, 3)

        # Query with limit less than inserted row count
        result = execute(cql, table, "SELECT * FROM %s ORDER BY val ANN OF [2.5, 3.5, 4.5] LIMIT 2")
        assertRowCount(result, 2)

def testQueryMoreRowsThanInserted(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])")
        wait_for_vector_writes(cql, table, 1)

        result = execute(cql, table, "SELECT * FROM %s ORDER BY val ANN OF [2.5, 3.5, 4.5] LIMIT 2")
        assertRowCount(result, 1)

# In Cassandra, the options maximum_node_connections and
# construction_beam_width are rejected unless the system property
# cassandra.sai.vector.allow_custom_parameters is set (by default, it isn't).
# The Java test checks this property, which we can't, so we check if the
# index creation failed instead. Scylla supports these options.
def testchangingOptionsTest(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))") as table:
        try:
            create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = " +
                        "{'maximum_node_connections' : 10, 'construction_beam_width' : 200, 'similarity_function' : 'euclidean' }")
        except InvalidRequest:
            return

        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', [3.0, 4.0, 5.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (3, 'D', [4.0, 5.0, 6.0])")
        wait_for_vector_writes(cql, table, 4)

        result = execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 3")
        assertRowCount(result, 3)

        flush(cql, table)
        result = execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 3")
        assertRowCount(result, 3)

        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (4, 'E', [5.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (5, 'F', [6.0, 3.0, 4.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (6, 'G', [7.0, 4.0, 5.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (7, 'H', [8.0, 5.0, 6.0])")

        flush(cql, table)
        compact(cql, table)
        wait_for_vector_writes(cql, table, 8)

        result = execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 5")
        assertRowCount(result, 5)

def testbindVariablesTest(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', ?)", [1.0, 2.0 ,3.0])
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', ?)", [2.0 ,3.0, 4.0])
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', ?)", [3.0, 4.0, 5.0])
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (3, 'D', ?)", [4.0, 5.0, 6.0])
        wait_for_vector_writes(cql, table, 4)

        result = execute(cql, table, "SELECT * FROM %s ORDER BY val ann of ? LIMIT 3", [2.5, 3.5, 4.5])
        assertRowCount(result, 3)

# The tests intersectedSearcherTest and nullVectorTest were not translated,
# because they create an SAI index on a non-vector column, which Scylla
# doesn't support (see above).

def testlwtTest(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(p int, c int, v text, vec vector<float, 2>, PRIMARY KEY(p, c))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex'")

        execute(cql, table, "INSERT INTO %s (p, c, v) VALUES (?, ?, ?)", 0, 0, "test")
        execute(cql, table, "INSERT INTO %s (p, c, v) VALUES (?, ?, ?)", 0, 1, "00112233445566")

        execute(cql, table, "UPDATE %s SET v='00112233', vec=[0.9, 0.7] WHERE p = 0 AND c = 0 IF v = 'test'")
        wait_for_vector_writes(cql, table, 1)

        result = execute(cql, table, "SELECT * FROM %s ORDER BY vec ANN OF [0.1, 0.9] LIMIT 100")

        assertRowCount(result, 1)

def testtwoVectorFieldsTest(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, v2 vector<float, 2>, v3 vector<float, 3>, PRIMARY KEY(pk))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(v2) USING 'StorageAttachedIndex'")
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(v3) USING 'StorageAttachedIndex'")

# The Java test searches with "WHERE pk = ? ORDER BY val ANN OF ...", and we
# added ALLOW FILTERING to these queries. Scylla requires it, because its
# vector index is a global index over the whole table: It finds the nearest
# vectors in one partition by searching the entire index, and filtering out
# the other partitions. With many partitions, this may need to traverse much
# of the index, so its cost depends on the size of the table rather than of
# the partition - which is the kind of query that ALLOW FILTERING is meant to
# warn about. Cassandra instead reads just the one partition and compares its
# vectors to the query vector, so it doesn't need ALLOW FILTERING - but it
# also accepts it, so the modified test still checks the same thing on both.
# A Scylla user who needs efficient searches within one partition should
# create a local vector index, CREATE CUSTOM INDEX ON t((pk), val).
def testprimaryKeySearchTest(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, val vector<float, 3>, i int, PRIMARY KEY(pk))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        N = 5
        for i in range(N):
            execute(cql, table, "INSERT INTO %s (pk, val) VALUES (?, ?)", i, [1.0 + i, 2.0 + i, 3.0 + i])
        wait_for_vector_writes(cql, table, N)

        for i in range(N):
            result = execute(cql, table, "SELECT pk FROM %s WHERE pk = ? ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 2 ALLOW FILTERING", i)
            assertRows(result, row(i))

        flush(cql, table)
        for i in range(N):
            result = execute(cql, table, "SELECT pk FROM %s WHERE pk = ? ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 2 ALLOW FILTERING", i)
            assertRows(result, row(i))

# As in testprimaryKeySearchTest above, we added ALLOW FILTERING to the
# searches restricted to one partition, which Scylla requires.
def testpartitionKeySearchTest(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(partition int, row int, val vector<float, 2>, PRIMARY KEY(partition, row))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}")

        nPartitions = 5
        rowsPerPartition = 10

        for i in range(1, nPartitions + 1):
            for j in range(1, rowsPerPartition + 1):
                execute(cql, table, "INSERT INTO %s (partition, row, val) VALUES (?, ?, ?)", i, j, [float(i), float(j)])
        wait_for_vector_writes(cql, table, nPartitions * rowsPerPartition)

        queryVector = [1.5, 1.5]
        for i in range(1, nPartitions + 1):
            result = execute(cql, table, "SELECT partition, row FROM %s WHERE partition = ? ORDER BY val ann of ? LIMIT 2 ALLOW FILTERING", i, queryVector)
            assertRowsIgnoringOrder(result,
                                    row(i, 1),
                                    row(i, 2))

        flush(cql, table)
        for i in range(1, nPartitions + 1):
            result = execute(cql, table, "SELECT partition, row FROM %s WHERE partition = ? ORDER BY val ann of ? LIMIT 2 ALLOW FILTERING", i, queryVector)
            assertRowsIgnoringOrder(result,
                                    row(i, 1),
                                    row(i, 2))

# Reproduces VECTOR-687: Scylla accepts an index on a table with a vector
# clustering key, but the vector store never serves it.
@pytest.mark.xfail(reason="VECTOR-687")
def testclusteringKeyIndexTest(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, ck vector<float, 2>, PRIMARY KEY(pk, ck))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(ck) USING 'StorageAttachedIndex'")

        execute(cql, table, "INSERT INTO %s (pk, ck) VALUES (1, [1.0, 2.0])")
        wait_for_vector_writes(cql, table, 1)

        assertRows(execute(cql, table, "SELECT * FROM %s ORDER BY ck ANN OF [1.0, 2.0] LIMIT 1"), row(1, [1.0, 2.0]))

# The Java test uses 100 partitions, so it runs about 40,000 queries (twice).
# We use 10 partitions, to keep the test fast. Instead of computing the
# tokens of the keys, as the Java test does, we read them with token().
# Reproduces VECTOR-1050: Scylla doesn't support token() restrictions in
# an ANN search.
@pytest.mark.xfail(reason="VECTOR-1050")
def testrangeSearchTest(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(partition int, val vector<float, 2>, PRIMARY KEY(partition))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}")

        nPartitions = 10

        for i in range(1, nPartitions + 1):
            execute(cql, table, "INSERT INTO %s (partition, val) VALUES (?, ?)", i, [float(i), float(i)])
        wait_for_vector_writes(cql, table, nPartitions)

        tokens = {r.partition: r.t for r in execute(cql, table, "SELECT partition, token(partition) AS t FROM %s")}
        min_token = -2**63
        max_token = 2**63 - 1
        def keysInTokenRange(left, leftInclusive, right, rightInclusive):
            return sorted(k for k, t in tokens.items()
                          if (left < t or left == t and leftInclusive) and (t < right or t == right and rightInclusive))
        def keysWithLowerBound(leftKey, leftInclusive):
            return keysInTokenRange(tokens[leftKey], leftInclusive, max_token, True)
        def keysWithUpperBound(rightKey, rightInclusive):
            return keysInTokenRange(min_token, True, tokens[rightKey], rightInclusive)
        def keysInBounds(leftKey, leftInclusive, rightKey, rightInclusive):
            return keysInTokenRange(tokens[leftKey], leftInclusive, tokens[rightKey], rightInclusive)
        def keys(result):
            return sorted(r.partition for r in result)

        queryVector = [1.5, 1.5]
        def tester():
            for i in range(1, nPartitions + 1):
                result = execute(cql, table, "SELECT partition FROM %s WHERE token(partition) > token(?) ORDER BY val ann of ? LIMIT 1000", i, queryVector)
                assert keys(result) == keysWithLowerBound(i, False)

                result = execute(cql, table, "SELECT partition FROM %s WHERE token(partition) >= token(?) ORDER BY val ann of ? LIMIT 1000", i, queryVector)
                assert keys(result) == keysWithLowerBound(i, True)

                result = execute(cql, table, "SELECT partition FROM %s WHERE token(partition) < token(?) ORDER BY val ann of ? LIMIT 1000", i, queryVector)
                assert keys(result) == keysWithUpperBound(i, False)

                result = execute(cql, table, "SELECT partition FROM %s WHERE token(partition) <= token(?) ORDER BY val ann of ? LIMIT 1000", i, queryVector)
                assert keys(result) == keysWithUpperBound(i, True)

                for j in range(1, nPartitions + 1):
                    result = execute(cql, table, "SELECT partition FROM %s WHERE token(partition) >= token(?) AND token(partition) <= token(?) ORDER BY val ann of ? LIMIT 1000", i, j, queryVector)
                    assert keys(result) == keysInBounds(i, True, j, True)

                    result = execute(cql, table, "SELECT partition FROM %s WHERE token(partition) > token(?) AND token(partition) <= token(?) ORDER BY val ann of ? LIMIT 1000", i, j, queryVector)
                    assert keys(result) == keysInBounds(i, False, j, True)

                    result = execute(cql, table, "SELECT partition FROM %s WHERE token(partition) >= token(?) AND token(partition) < token(?) ORDER BY val ann of ? LIMIT 1000", i, j, queryVector)
                    assert keys(result) == keysInBounds(i, True, j, False)

                    result = execute(cql, table, "SELECT partition FROM %s WHERE token(partition) > token(?) AND token(partition) < token(?) ORDER BY val ann of ? LIMIT 1000", i, j, queryVector)
                    assert keys(result) == keysInBounds(i, False, j, False)

        tester()

        flush(cql, table)

        tester()

# This test doesn't do vector search, so it doesn't need a vector store.
def testselectFloatVectorFunctions(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int primary key, value vector<float, 2>)") as table:
        # basic functionality
        q = [1.0, 2.0]
        execute(cql, table, "INSERT INTO %s (pk, value) VALUES (0, ?)", [1.0, 2.0])
        execute(cql, table, "SELECT similarity_cosine(value, value) FROM %s WHERE pk=0")

        # type inference checks
        result = execute(cql, table, "SELECT similarity_cosine(value, ?) FROM %s WHERe pk=0", q)
        assertRows(result, row(1.0))
        result = execute(cql, table, "SELECT similarity_euclidean(value, ?) FROM %s WHERe pk=0", q)
        assertRows(result, row(1.0))
        execute(cql, table, "SELECT similarity_cosine(?, value) FROM %s WHERE pk=0", q)
        assertInvalidMessage(cql, table, "Cannot infer type of argument ?",
                             "SELECT similarity_cosine(?, ?) FROM %s WHERE pk=0", q, q)

        # The checks "with explicit typing" are in
        # testselectFloatVectorFunctionsWithTypeHints below.

# The steps of selectFloatVectorFunctions "with explicit typing", which use
# type hints such as "(vector<float, 2>) ?". Scylla doesn't yet support type
# hints of non-native types in the selection clause, so these steps are in
# a separate test, which is xfail, while the rest of the test runs.
# Reproduces #5411 (terms, such as type hints, in the selection clause)
@pytest.mark.xfail(reason="#5411")
def testselectFloatVectorFunctionsWithTypeHints(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int primary key, value vector<float, 2>)") as table:
        q = [1.0, 2.0]
        execute(cql, table, "INSERT INTO %s (pk, value) VALUES (0, ?)", [1.0, 2.0])

        # with explicit typing
        execute(cql, table, "SELECT similarity_cosine((vector<float, 2>) ?, ?) FROM %s WHERE pk=0", q, q)
        execute(cql, table, "SELECT similarity_cosine(?, (vector<float, 2>) ?) FROM %s WHERE pk=0", q, q)
        execute(cql, table, "SELECT similarity_cosine((vector<float, 2>) ?, (vector<float, 2>) ?) FROM %s WHERE pk=0", q, q)

def testselectSimilarityWithAnn(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', [3.0, 4.0, 5.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (3, 'D', [4.0, 5.0, 6.0])")
        wait_for_vector_writes(cql, table, 4)

        q = [1.5, 2.5, 3.5]
        result = execute(cql, table, "SELECT str_val, similarity_cosine(val, ?) FROM %s ORDER BY val ANN OF ? LIMIT 2",
                q, q)

        # The Java test expects the exact float values that Cassandra
        # computes. Scylla computes the same similarity, but its float
        # arithmetic may round the result differently: For "A" the exact
        # similarity is 0.99870745..., which Scylla rounds to the nearest
        # float, 0.99870747, but Cassandra's computation gives the next
        # float down, 0.99870741. So we only compare the similarity up to
        # float precision.
        assert sorted((r[0], r[1]) for r in result) == [
                ("A", pytest.approx(0.9987074, rel=1e-6)),
                ("B", pytest.approx(0.9993764, rel=1e-6))]

# The tests testTwoPredicatesWithUnnecessaryAllowFiltering,
# testMultipleVectorsInMemoryWithPredicate and
# multiPartitionUpdateMultiIndexTest were not translated, because they create
# SAI indexes on non-vector columns, which Scylla doesn't support (see above).

# Reproduces VECTOR-1049: Scylla accepts a vector index on a static column,
# but the vector store never serves it.
@pytest.mark.xfail(reason="VECTOR-1049")
def testStaticVectorColumnIndex(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, ck int, val vector<float, 2> static, PRIMARY KEY(pk, ck))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        execute(cql, table, "INSERT INTO %s (pk, ck, val) VALUES (0, 1, [1,0])")
        execute(cql, table, "INSERT INTO %s (pk, ck)      VALUES (0, 2)")
        execute(cql, table, "INSERT INTO %s (pk, ck, val) VALUES (1, 3, [0,-1])")
        execute(cql, table, "INSERT INTO %s (pk, ck, val) VALUES (2, 4, [0,1])")
        # The static column has 3 different values, in 3 partitions, so we
        # wait for the first search to see all of them.
        wait_for_vector_search(cql, f"SELECT ck FROM {table} ORDER BY val ANN OF [0,1] LIMIT 3",
                               lambda rows: len(rows) == 3, "Vector store didn't index all the rows")

        for _ in before_and_after_flush(cql, table):
            assertRows(execute(cql, table, "SELECT ck FROM %s ORDER BY val ANN OF [0,1] LIMIT 3"), row(4), row(1), row(2))
            assertRows(execute(cql, table, "SELECT ck FROM %s ORDER BY val ANN OF [0,1] LIMIT 2"), row(4), row(1))

# The test testTooManyMaterializedKeys was not translated, because it
# changes an internal constant of Cassandra's SAI (MAX_MATERIALIZED_KEYS),
# and creates an SAI index on a non-vector column.
