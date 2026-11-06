# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# This is a translation of VectorLocalTest.java from Cassandra's
# test/unit/org/apache/cassandra/index/sai/cql directory, which tests vector
# search on larger amounts of data. On Scylla, these tests need a vector
# store (run test/cqlpy/run with the "--vs" option), and they wait for the
# vector store to catch up after writing - see the explanation in
# vector_tester.py.
#
# The Java tests take their vectors from a file of 3,000 50-dimensional
# GloVe word vectors (Cassandra's test/resources/glove.3K.50d.txt). The
# tests translated here check the order of search results, and the effect
# of key restrictions, which don't depend on the vectors being real word
# vectors, so instead of copying this file we use random 50-dimensional
# vectors.

import math
import random
import struct

from cassandra.murmur3 import murmur3

from .porting import *
from .vector_tester import create_table, create_index, wait_for_vector_writes

DIMENSION = 50

# The given number, rounded to a float. The vectors we write are stored as
# floats, so we round them in advance, to be able to compare them to the
# vectors that we read back.
def to_float32(x):
    return struct.unpack('f', struct.pack('f', x))[0]

# Replaces the Java test's randomVector(), which returns a random one of the
# word vectors (see above).
def randomVector():
    return [to_float32(random.uniform(-1, 1)) for _ in range(DIMENSION)]

def vectorString(vector):
    return str(vector)

# Cassandra's token (with the Murmur3 partitioner, which both Cassandra and
# Scylla use by default) for an int partition key.
def token(key):
    return murmur3(struct.pack('>i', key))

# VectorSimilarityFunction.COSINE.compare(), which Cassandra (and Scylla)
# return as the cosine similarity mapped to the range [0, 1].
def cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    norm = math.sqrt(sum(x * x for x in a) * sum(y * y for y in b))
    return (1 + dot / norm) / 2

def assertDescendingScore(queryVector, resultVectors):
    prevScore = -1
    for current in resultVectors:
        score = cosine(current, queryVector)
        if prevScore >= 0:
            # We compute the similarity in double precision, while the
            # database computes it in float precision, so we allow a
            # difference of a float rounding error.
            assert score <= prevScore + 1e-6
        prevScore = score

def getVectorsFromResult(result):
    return [list(row.val) for row in result]

def isCloseTo(actual, expected, percentage):
    return abs(actual - expected) <= expected * percentage / 100

def search(cql, table, queryVector, limit):
    result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of " + vectorString(queryVector) + " LIMIT " + str(limit)))
    assert isCloseTo(len(result), limit, 5)
    return result

def searchWithRange(cql, table, queryVector, minToken, maxToken, expectedSize):
    result = list(execute(cql, table, "SELECT * FROM %s WHERE token(pk) <= " + str(maxToken) + " AND token(pk) >= " + str(minToken) + " ORDER BY val ann of " + vectorString(queryVector) + " LIMIT 1000"))
    assert isCloseTo(len(result), expectedSize, 5)
    return getVectorsFromResult(result)

def searchWithNonExistingKey(cql, table, queryVector, key):
    searchWithKey(cql, table, queryVector, key, 0)

# The Java test searches with "WHERE pk = ...", and we added ALLOW FILTERING.
# As explained in vector_type_test.py's testprimaryKeySearchTest, Scylla
# requires ALLOW FILTERING for a search restricted to one partition with a
# global vector index, and Cassandra accepts it.
def searchWithKey(cql, table, queryVector, key, expectedSize):
    result = list(execute(cql, table, "SELECT * FROM %s WHERE pk = " + str(key) + " ORDER BY val ann of " + vectorString(queryVector) + " LIMIT 1000 ALLOW FILTERING"))

    # VSTODO maybe we should have different methods for these cases
    if expectedSize < 10:
        assert len(result) == expectedSize
    else:
        assert isCloseTo(len(result), expectedSize, 5)
    for row in result:
        assert row.pk == key

def recallMatch(expected, actual, topK):
    if not expected and not actual:
        return 1.0
    actual = set(tuple(v) for v in actual)
    matches = sum(1 for v in expected if tuple(v) in actual)
    return matches / topK

def testkeyRestrictionsWithFilteringTest(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, v vector<float, 1>)") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}")
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (1, [1])")
        wait_for_vector_writes(cql, table, 1)

        assertRows(execute(cql, table, "SELECT k, v FROM %s WHERE k > 0 LIMIT 4 ALLOW FILTERING"), row(1, [1.0]))
        assertRows(execute(cql, table, "SELECT k, v FROM %s WHERE k = 1 ORDER BY v ANN OF [0] LIMIT 4 ALLOW FILTERING"), row(1, [1.0]))

        flush(cql, table)
        assertRows(execute(cql, table, "SELECT k, v FROM %s WHERE k > 0 LIMIT 4 ALLOW FILTERING"), row(1, [1.0]))
        assertRows(execute(cql, table, "SELECT k, v FROM %s WHERE k = 1 ORDER BY v ANN OF [0] LIMIT 4 ALLOW FILTERING"), row(1, [1.0]))

def testrandomizedTest(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, f"(pk int, str_val text, val vector<float, {DIMENSION}>, PRIMARY KEY(pk))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        vectorCount = random.randint(500, 1000)
        vectors = [randomVector() for _ in range(vectorCount)]

        pk = 0
        for vector in vectors:
            execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (?, 'A', ?)", pk, vector)
            pk += 1
        wait_for_vector_writes(cql, table, pk)

        # query memtable index
        limit = min(random.randint(30, 50), len(vectors))
        queryVector = randomVector()
        resultSet = search(cql, table, queryVector, limit)
        assertDescendingScore(queryVector, getVectorsFromResult(resultSet))

        flush(cql, table)

        # query on-disk index
        queryVector = randomVector()
        resultSet = search(cql, table, queryVector, limit)
        assertDescendingScore(queryVector, getVectorsFromResult(resultSet))

        # populate some more vectors
        additionalVectorCount = random.randint(500, 1000)
        additionalVectors = [randomVector() for _ in range(additionalVectorCount)]
        for vector in additionalVectors:
            execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (?, 'A', ?)", pk, vector)
            pk += 1
        wait_for_vector_writes(cql, table, pk)

        vectors.extend(additionalVectors)

        # query both memtable index and on-disk index
        queryVector = randomVector()
        resultSet = search(cql, table, queryVector, limit)
        assertDescendingScore(queryVector, getVectorsFromResult(resultSet))

        flush(cql, table)

        # query multiple on-disk indexes
        queryVector = randomVector()
        resultSet = search(cql, table, queryVector, limit)
        assertDescendingScore(queryVector, getVectorsFromResult(resultSet))

        compact(cql, table)

        # query compacted on-disk index
        queryVector = randomVector()
        resultSet = search(cql, table, queryVector, limit)
        assertDescendingScore(queryVector, getVectorsFromResult(resultSet))

# The test multiSSTablesTest was not translated, because it checks the
# number of sstables, and measures recall with Cassandra's internal vector
# graph library (rawIndexedRecall()).

# Reproduces VECTOR-1051: An ANN search restricted to one partition (with
# ALLOW FILTERING, see searchWithKey()) may miss some of the partition's rows.
# The test's vectors and keys are random, so it doesn't always fail, and the
# xfail is not strict. test_vector_index.py has a smaller and faster test
# reproducing this issue, test_vector_search_restricted_to_partition.
@pytest.mark.xfail(reason="VECTOR-1051", strict=False)
def testpartitionRestrictedTest(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, f"(pk int, str_val text, val vector<float, {DIMENSION}>, PRIMARY KEY(pk))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        vectorCount = random.randint(500, 1000)
        vectors = [randomVector() for _ in range(vectorCount)]

        for pk in range(vectorCount):
            execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (?, 'A', ?)", pk, vectors[pk])
        wait_for_vector_writes(cql, table, vectorCount)

        # query memtable index

        for executionCount in range(50):
            key = random.randint(1, vectorCount - 1)
            queryVector = vectors[random.randint(0, vectorCount - 1)]
            searchWithKey(cql, table, queryVector, key, 1)

        flush(cql, table)

        # query on-disk index with existing key:
        for executionCount in range(50):
            key = random.randint(1, vectorCount - 1)
            queryVector = vectors[random.randint(0, vectorCount - 1)]
            searchWithKey(cql, table, queryVector, key, 1)

        # query on-disk index with non-existing key:
        for executionCount in range(50):
            nonExistingKey = random.randint(1, vectorCount) + vectorCount
            queryVector = vectors[random.randint(0, vectorCount - 1)]
            searchWithNonExistingKey(cql, table, queryVector, nonExistingKey)

# Reproduces VECTOR-1051: An ANN search restricted to one partition (with
# ALLOW FILTERING, see searchWithKey()) may miss some of the partition's rows.
# The test's vectors and keys are random, so it doesn't always fail, and the
# xfail is not strict. test_vector_index.py has a smaller and faster test
# reproducing this issue, test_vector_search_restricted_to_partition.
@pytest.mark.xfail(reason="VECTOR-1051", strict=False)
def testpartitionRestrictedWidePartitionTest(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, f"(pk int, ck int, val vector<float, {DIMENSION}>, PRIMARY KEY(pk, ck))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        partitions = random.randint(20, 40)
        vectorCountPerPartition = random.randint(50, 100)
        vectorCount = partitions * vectorCountPerPartition
        vectors = [randomVector() for _ in range(vectorCount)]

        i = 0
        for pk in range(1, partitions + 1):
            for ck in range(1, vectorCountPerPartition + 1):
                vector = vectors[i]
                i += 1
                execute(cql, table, "INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", pk, ck, vector)
        wait_for_vector_writes(cql, table, vectorCount)

        # query memtable index
        for executionCount in range(50):
            key = random.randint(1, partitions)
            queryVector = randomVector()
            searchWithKey(cql, table, queryVector, key, vectorCountPerPartition)

        flush(cql, table)

        # query on-disk index with existing key:
        for executionCount in range(50):
            key = random.randint(1, partitions)
            queryVector = randomVector()
            searchWithKey(cql, table, queryVector, key, vectorCountPerPartition)

        # query on-disk index with non-existing key:
        for executionCount in range(50):
            nonExistingKey = random.randint(1, partitions) + partitions
            queryVector = randomVector()
            searchWithNonExistingKey(cql, table, queryVector, nonExistingKey)

# The two range-restricted tests below run the same 50 random searches on
# the memtable and on-disk index. This function is that loop.
def rangeRestrictedSearches(cql, table, vectorCount, vectorsByToken, vectors):
    for executionCount in range(50):
        key1 = random.randint(1, vectorCount * 2)
        token1 = token(key1)
        key2 = random.randint(1, vectorCount * 2)
        token2 = token(key2)

        minToken = min(token1, token2)
        maxToken = max(token1, token2)
        expected = [v for t, v in vectorsByToken if t >= minToken and t <= maxToken]

        queryVector = vectors[random.randint(0, vectorCount - 1)]

        resultVectors = searchWithRange(cql, table, queryVector, minToken, maxToken, len(expected))
        assertDescendingScore(queryVector, resultVectors)

        if not expected:
            assert resultVectors == []
        else:
            recall = recallMatch(expected, resultVectors, len(expected))
            assert recall >= 0.8

# Reproduces VECTOR-1050: Scylla doesn't support token() restrictions in
# an ANN search.
@pytest.mark.xfail(reason="VECTOR-1050")
def testrangeRestrictedTest(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, f"(pk int, str_val text, val vector<float, {DIMENSION}>, PRIMARY KEY(pk))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        vectorCount = random.randint(500, 1000)
        vectors = [randomVector() for _ in range(vectorCount)]

        pk = 0
        vectorsByToken = []
        for index in range(vectorCount):
            vector = vectors[index]
            vectorsByToken.append((token(pk), vector))
            execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (?, ?, ?)", pk, str(index), vector)
            pk += 1
        wait_for_vector_writes(cql, table, vectorCount)

        # query memtable index
        rangeRestrictedSearches(cql, table, vectorCount, vectorsByToken, vectors)

        flush(cql, table)

        # query on-disk index with existing key:
        rangeRestrictedSearches(cql, table, vectorCount, vectorsByToken, vectors)

# Reproduces VECTOR-1050: Scylla doesn't support token() restrictions in
# an ANN search.
@pytest.mark.xfail(reason="VECTOR-1050")
def testrangeRestrictedWidePartitionTest(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, f"(pk int, ck int, str_val text, val vector<float, {DIMENSION}>, PRIMARY KEY(pk, ck ))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        vectorCount = random.randint(500, 1000)
        vectors = [randomVector() for _ in range(vectorCount)]

        pk = 0
        ck = 0
        vectorsByToken = []
        for index in range(vectorCount):
            vector = vectors[index]
            vectorsByToken.append((token(pk), vector))
            execute(cql, table, "INSERT INTO %s (pk, ck, str_val, val) VALUES (?, ?, ?, ?)", pk, ck, str(index), vector)
            ck += 1
            if ck == 10:
                ck = 0
                pk += 1
        wait_for_vector_writes(cql, table, vectorCount)

        # query memtable index
        rangeRestrictedSearches(cql, table, vectorCount, vectorsByToken, vectors)

        flush(cql, table)

        # query on-disk index with existing key:
        rangeRestrictedSearches(cql, table, vectorCount, vectorsByToken, vectors)

# The tests multipleSegmentsMultiplePostingsTest and multipleNonAnnSegmentsTest
# were not translated, because they change the size of SAI's internal index
# segments (SegmentBuilder.updateLastValidSegmentRowId()), and the latter
# also creates an SAI index on a non-vector column, which Scylla doesn't
# support (see vector_tester.py).
# The test flushSuccessfullyVectorIndexToShardedSSTable was not translated,
# because it tests flushing Cassandra's index into the sharded sstables of
# Cassandra's UnifiedCompactionStrategy, which Scylla doesn't have.
