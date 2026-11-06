# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# This is a translation of VectorUpdateDeleteTest.java from Cassandra's
# test/unit/org/apache/cassandra/index/sai/cql directory, which tests vector
# search after updates and deletions. On Scylla, these tests need a vector
# store (run test/cqlpy/run with the "--vs" option), and they wait for the
# vector store to catch up after writing - see the explanation in
# vector_tester.py.
#
# Many of these tests are about how Cassandra's SAI combines a row's data
# from memtables and different sstables, so they flush between writes. On
# Scylla, the vector index is maintained by the vector store, which doesn't
# care about flushes, but the tests are still useful as tests of updates
# and deletions.

import random

from .porting import *
from .vector_tester import create_table, create_index, wait_for_vector_writes, wait_for_search
from .vector_type_test import assertContainsInt

# The primary keys in the given search result rows
def pks(rows):
    return [r.pk for r in rows]

# partition delete won't trigger UpdateTransaction#onUpdated
def testpartitionDeleteVectorInMemoryTest(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', [3.0, 4.0, 5.0])")
        wait_for_vector_writes(cql, table, 3)

        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 3"))
        assertRowCount(result, 3)

        execute(cql, table, "UPDATE %s SET val = null WHERE pk = 0")
        wait_for_vector_writes(cql, table, 2)

        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [1.1, 2.1, 3.1] LIMIT 1")) # closer to row 0
        assertRowCount(result, 1)
        assertContainsInt(result, "pk", 1)

        execute(cql, table, "DELETE from %s WHERE pk = 1")
        wait_for_vector_writes(cql, table, 1)
        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [2.1, 3.1, 4.1] LIMIT 1")) # closer to row 1
        assertRowCount(result, 1)
        assertContainsInt(result, "pk", 2)

        flush(cql, table)

        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [2.1, 3.1, 4.1] LIMIT 1"))  # closer to row 1
        assertRowCount(result, 1)
        assertContainsInt(result, "pk", 2)

# row delete will trigger UpdateTransaction#onUpdated
def testrowDeleteVectorInMemoryAndFlushTest(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, ck int, str_val text, val vector<float, 3>, PRIMARY KEY(pk, ck))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        execute(cql, table, "INSERT INTO %s (pk, ck, str_val, val) VALUES (0, 0, 'A', [1.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, ck, str_val, val) VALUES (1, 1, 'B', [2.0, 3.0, 4.0])")
        wait_for_vector_writes(cql, table, 2)
        execute(cql, table, "DELETE from %s WHERE pk = 1 and ck = 1")
        wait_for_vector_writes(cql, table, 1)

        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 1"))
        assertRowCount(result, 1)
        assertContainsInt(result, "pk", 0)

        flush(cql, table)

        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 1"))
        assertRowCount(result, 1)
        assertContainsInt(result, "pk", 0)

def testFlushWithDeletedVectors(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, v vector<float, 2>, PRIMARY KEY(pk))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'")

        execute(cql, table, "INSERT INTO %s (pk, v) VALUES (0, [1.0, 2.0])")
        wait_for_vector_writes(cql, table, 1)
        execute(cql, table, "INSERT INTO %s (pk, v) VALUES (0, null)")
        wait_for_vector_writes(cql, table, 0)

        flush(cql, table)

        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY v ann of [2.5, 3.5] LIMIT 1"))
        assertRowCount(result, 0)

# range delete won't trigger UpdateTransaction#onUpdated
# Reproduces VECTOR-647: the vector store ignores a range deletion, so the
# deleted vectors remain in the index.
@pytest.mark.xfail(reason="VECTOR-647")
def testrangeDeleteVectorInMemoryAndFlushTest(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, ck int, ck2 int, str_val text, val vector<float, 3>, PRIMARY KEY(pk, ck, ck2))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        execute(cql, table, "INSERT INTO %s (pk, ck, ck2, str_val, val) VALUES (0, 0, 0, 'A', [1.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, ck, ck2, str_val, val) VALUES (1, 1, 1, 'B', [2.0, 3.0, 4.0])")
        wait_for_vector_writes(cql, table, 2)
        execute(cql, table, "DELETE from %s WHERE pk = 1 and ck = 1")
        wait_for_vector_writes(cql, table, 1)

        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 1"))
        assertRowCount(result, 1)
        assertContainsInt(result, "pk", 0)

        flush(cql, table)

        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 1"))
        assertRowCount(result, 1)
        assertContainsInt(result, "pk", 0)

def testupdateVectorInMemoryAndFlushTest(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])")
        wait_for_vector_writes(cql, table, 2)
        execute(cql, table, "UPDATE %s SET val = null WHERE pk = 1")
        wait_for_vector_writes(cql, table, 1)

        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 1"))
        assertRowCount(result, 1)
        assertContainsInt(result, "pk", 0)

        flush(cql, table)

        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 3"))
        assertRowCount(result, 1)
        assertContainsInt(result, "pk", 0)

def testdeleteVectorPostFlushTest(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])")
        wait_for_vector_writes(cql, table, 2)

        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 2"))
        assertRowCount(result, 2)
        flush(cql, table)

        execute(cql, table, "UPDATE %s SET val = null WHERE pk = 0")
        wait_for_vector_writes(cql, table, 1)
        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 2"))
        assertRowCount(result, 1)
        assertContainsInt(result, "pk", 1)

        execute(cql, table, "DELETE from %s WHERE pk = 1")
        wait_for_vector_writes(cql, table, 0)
        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 2"))
        assertEmpty(result)
        flush(cql, table)

        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 2"))
        assertEmpty(result)

def testdeletedInOtherSSTablesTest(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', [3.0, 4.0, 5.0])")
        wait_for_vector_writes(cql, table, 3)

        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1"))
        assertRowCount(result, 1)
        assertContainsInt(result, "pk", 0)
        flush(cql, table)

        execute(cql, table, "DELETE from %s WHERE pk = 0")
        execute(cql, table, "DELETE from %s WHERE pk = 1")
        wait_for_vector_writes(cql, table, 1)
        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1"))
        assertRowCount(result, 1)
        assertContainsInt(result, "pk", 2)

# The test deletedInOtherSSTablesMultiIndexTest was not translated, because
# it creates an SAI index on a non-vector column, which Scylla doesn't
# support (see vector_tester.py).

# Reproduces VECTOR-647: the vector store ignores a range deletion, so the
# deleted vectors remain in the index.
@pytest.mark.xfail(reason="VECTOR-647")
def testrangeDeletedInOtherSSTablesTest(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, ck1 int, ck2 int, str_val text, val vector<float, 3>, PRIMARY KEY(pk, ck1, ck2))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        execute(cql, table, "INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (0, 0, 1, 'A', [1.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (0, 0, 2, 'B', [2.0, 3.0, 4.0])")
        execute(cql, table, "INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (0, 1, 3, 'C', [3.0, 4.0, 5.0])")
        execute(cql, table, "INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (0, 1, 4, 'D', [3.0, 5.0, 6.0])")
        wait_for_vector_writes(cql, table, 4)

        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1"))
        assertRowCount(result, 1)
        assertContainsInt(result, "ck1", 0)
        flush(cql, table)

        execute(cql, table, "DELETE from %s WHERE pk = 0 and ck1 = 0")
        wait_for_vector_writes(cql, table, 2)

        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1"))
        assertRowCount(result, 1)
        assertContainsInt(result, "ck1", 1)


        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 2"))
        assertRowCount(result, 2)

# Reproduces VECTOR-647: the vector store ignores a partition deletion in a table with
# clustering columns, so the
# deleted vectors remain in the index.
@pytest.mark.xfail(reason="VECTOR-647")
def testpartitionDeletedInOtherSSTablesTest(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, ck1 int, ck2 int, str_val text, val vector<float, 3>, PRIMARY KEY(pk, ck1, ck2))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        execute(cql, table, "INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (0, 0, 1, 'A', [1.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (0, 0, 2, 'B', [2.0, 3.0, 4.0])")
        execute(cql, table, "INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (1, 1, 3, 'C', [3.0, 4.0, 5.0])")
        execute(cql, table, "INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (1, 1, 4, 'D', [3.0, 5.0, 6.0])")
        wait_for_vector_writes(cql, table, 4)

        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1"))
        assertRowCount(result, 1)
        assertContainsInt(result, "pk", 0)
        flush(cql, table)

        execute(cql, table, "DELETE from %s WHERE pk = 0")
        wait_for_vector_writes(cql, table, 2)

        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1"))
        assertRowCount(result, 1)
        assertContainsInt(result, "pk", 1)


        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 2"))
        assertRowCount(result, 2)

def testupsertTest(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])")
        wait_for_vector_writes(cql, table, 2)

        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 2"))
        assertRowCount(result, 2)
        assertContainsInt(result, "pk", 0)
        assertContainsInt(result, "pk", 1)
        flush(cql, table)

        # These writes don't change the data, so there is nothing to wait for.
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])")
        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 2"))
        assertRowCount(result, 2)
        assertContainsInt(result, "pk", 0)
        assertContainsInt(result, "pk", 1)
        flush(cql, table)

        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 2"))
        assertRowCount(result, 2)
        assertContainsInt(result, "pk", 0)
        assertContainsInt(result, "pk", 1)

def testupdateTest(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        # overwrite row A a bunch of times; also write row B with the same vector as a deleted A value
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [2.0, 3.0, 4.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [3.0, 4.0, 5.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [4.0, 5.0, 6.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [5.0, 6.0, 7.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])")
        # Overwriting row A doesn't change the number of indexed vectors, so
        # we also wait for the searches near A and B to stop finding an old
        # vector of A. All of A's vectors from then on give the same results.
        wait_for_vector_writes(cql, table, 2)
        wait_for_search(cql, table, "SELECT pk FROM %s ORDER BY val ann of [4.5, 5.5, 6.5] LIMIT 1",
                        lambda rows: pks(rows) == [0])
        wait_for_search(cql, table, "SELECT pk FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1",
                        lambda rows: pks(rows) == [1])

        # check that queries near A and B get the right row
        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [4.5, 5.5, 6.5] LIMIT 1"))
        assertRowCount(result, 1)
        assertContainsInt(result, "pk", 0)
        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1"))
        assertRowCount(result, 1)
        assertContainsInt(result, "pk", 1)

        # flush, and re-check same queries
        flush(cql, table)
        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [4.5, 5.5, 6.5] LIMIT 1"))
        assertRowCount(result, 1)
        assertContainsInt(result, "pk", 0)
        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1"))
        assertRowCount(result, 1)
        assertContainsInt(result, "pk", 1)

        # overwite A more in the new memtable
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [6.0, 7.0, 8.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [7.0, 8.0, 9.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [8.0, 9.0, 10.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [9.0, 10.0, 11.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [10.0, 11.0, 12.0])")
        # These overwrites don't change the result of any of the searches
        # below, so there is nothing to wait for.

        # query near A and B again
        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [9.5, 10.5, 11.5] LIMIT 1"))
        assertRowCount(result, 1)
        assertContainsInt(result, "pk", 0)
        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1"))
        assertRowCount(result, 1)
        assertContainsInt(result, "pk", 1)

        # flush, and re-check same queries
        flush(cql, table)
        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [9.5, 10.5, 11.5] LIMIT 1"))
        assertRowCount(result, 1)
        assertContainsInt(result, "pk", 0)
        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1"))
        assertRowCount(result, 1)
        assertContainsInt(result, "pk", 1)

def testupdateOtherColumnsTest(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])")
        wait_for_vector_writes(cql, table, 2)
        execute(cql, table, "UPDATE %s SET str_val='C' WHERE pk=0")

        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 2"))
        assertRowCount(result, 2)

def testupdateManySSTablesTest(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])")
        flush(cql, table)
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [2.0, 3.0, 4.0])")
        flush(cql, table)
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [3.0, 4.0, 5.0])")
        flush(cql, table)
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [4.0, 5.0, 6.0])")
        flush(cql, table)
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [5.0, 6.0, 7.0])")
        flush(cql, table)
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [6.0, 7.0, 8.0])")
        flush(cql, table)
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [7.0, 8.0, 9.0])")
        flush(cql, table)
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [8.0, 9.0, 10.0])")
        flush(cql, table)
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [9.0, 10.0, 11.0])")
        flush(cql, table)
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [10.0, 11.0, 12.0])")
        flush(cql, table)
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])")
        flush(cql, table)
        # Overwriting row A doesn't change the number of indexed vectors, so
        # we also wait for the search near A to find A's last vector.
        wait_for_vector_writes(cql, table, 2)
        wait_for_search(cql, table, "SELECT pk FROM %s ORDER BY val ann of [9.5, 10.5, 11.5] LIMIT 1",
                        lambda rows: pks(rows) == [0])

        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [9.5, 10.5, 11.5] LIMIT 1"))
        assertRowCount(result, 1)
        assertContainsInt(result, "pk", 0)
        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1"))
        assertRowCount(result, 1)
        assertContainsInt(result, "pk", 1)

# The Java test calls disableCompaction(), so that the row and its deletion
# remain in different sstables. Scylla's vector index doesn't care, so we
# don't need it.
def testshadowedPrimaryKeyInDifferentSSTable(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int primary key, str_val text, val vector<float, 3>)") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        # flush a sstable with one vector
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])")
        wait_for_vector_writes(cql, table, 1)
        flush(cql, table)

        # flush another sstable to shadow the vector row
        execute(cql, table, "DELETE FROM %s where pk = 0")
        wait_for_vector_writes(cql, table, 0)
        flush(cql, table)

        # flush another sstable with one new vector row
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])")
        wait_for_vector_writes(cql, table, 1)
        flush(cql, table)

        # the shadow vector has the highest score
        result = list(execute(cql, table, "SELECT * FROM %s ORDER BY val ann of [1.0, 2.0, 3.0] LIMIT 1"))
        assertRowCount(result, 1)

# The tests shadowedPrimaryKeyWithSharedVectorAndOtherPredicates,
# testVectorRowWhereUpdateMakesRowMatchNonOrderingPredicates,
# testUpdateVectorWithSplitRow,
# testUpdateNonVectorColumnWhereNoSingleSSTableRowMatchesAllPredicates and
# shadowedPrimaryKeyWithUpdatedPredicateMatchingIntValue were not
# translated, because they create SAI indexes on non-vector columns, which
# Scylla doesn't support (see vector_tester.py).

# Reproduces VECTOR-1050: Scylla doesn't support token() restrictions in
# an ANN search.
@pytest.mark.xfail(reason="VECTOR-1050")
def testrangeRestrictedTestWithDuplicateVectorsAndADelete(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, str_val text, val vector<float, 2>, PRIMARY KEY(pk))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        execute(cql, table, "INSERT INTO %s (pk, val) VALUES (0, [1.0, 2.0])") # -3485513579396041028
        execute(cql, table, "INSERT INTO %s (pk, val) VALUES (1, [1.0, 2.0])") # -4069959284402364209
        execute(cql, table, "INSERT INTO %s (pk, val) VALUES (2, [1.0, 2.0])") # -3248873570005575792
        execute(cql, table, "INSERT INTO %s (pk, val) VALUES (3, [1.0, 2.0])") # 9010454139840013625
        wait_for_vector_writes(cql, table, 4)

        flush(cql, table)

        # Show the result set is as expected
        assertRows(execute(cql, table, "SELECT pk FROM %s WHERE token(pk) <= -3248873570005575792 AND " +
                           "token(pk) >= -3485513579396041028 ORDER BY val ann of [1,2] LIMIT 1000"), row(0), row(2))

        # Delete one of the rows
        execute(cql, table, "DELETE FROM %s WHERE pk = 0")
        wait_for_vector_writes(cql, table, 3)

        flush(cql, table)
        assertRows(execute(cql, table, "SELECT pk FROM %s WHERE token(pk) <= -3248873570005575792 AND " +
                           "token(pk) >= -3485513579396041028 ORDER BY val ann of [1,2] LIMIT 1000"), row(2))

def testrangeRestrictedTestWithDuplicateVectorsAndAddNullVector(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, str_val text, val vector<float, 2>, PRIMARY KEY(pk))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")


        execute(cql, table, "INSERT INTO %s (pk, val) VALUES (0, [1.0, 2.0])")
        execute(cql, table, "INSERT INTO %s (pk, val) VALUES (1, [1.0, 2.0])")
        execute(cql, table, "INSERT INTO %s (pk, val) VALUES (2, [1.0, 2.0])")
        # Add a str_val to make sure pk has a row id in the sstable
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (3, 'a', null)")
        # Add another row to test a different part of the code
        execute(cql, table, "INSERT INTO %s (pk, val) VALUES (4, [1.0, 2.0])")
        wait_for_vector_writes(cql, table, 4)
        execute(cql, table, "DELETE FROM %s WHERE pk = 2")
        wait_for_vector_writes(cql, table, 3)
        flush(cql, table)

        # Delete one of the rows to trigger a shadowed primary key
        execute(cql, table, "DELETE FROM %s WHERE pk = 0")
        wait_for_vector_writes(cql, table, 2)
        execute(cql, table, "INSERT INTO %s (pk, val) VALUES (2, [2.0, 2.0])")
        wait_for_vector_writes(cql, table, 3)
        flush(cql, table)

        # Delete more rows.
        execute(cql, table, "DELETE FROM %s WHERE pk = 2")
        execute(cql, table, "DELETE FROM %s WHERE pk = 3")
        wait_for_vector_writes(cql, table, 2)

        # Rows 1 and 4 have the same vector, so they are equally similar to
        # the query vector, and their order in the result isn't defined. The
        # Java test expects the order that Cassandra happens to return, while
        # Scylla may return them in the opposite order, so we ignore the order.
        for _ in before_and_after_flush(cql, table):
            assertRowsIgnoringOrder(execute(cql, table, "SELECT pk FROM %s ORDER BY val ann of [1,2] LIMIT 1000"),
                       row(1), row(4))

# This test intentionally has extra rows with primary keys that are above and below the
# deleted primary key so that we do not short circuit certain parts of the shadowed key logic.
# The Java test calls disableCompaction(), which we don't need - see
# testshadowedPrimaryKeyInDifferentSSTable above.
def testshadowedPrimaryKeyInDifferentSSTableEachWithMultipleRows(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int primary key, str_val text, val vector<float, 3>)") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        # flush a sstable with one vector
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (1, 'A', [1.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (2, 'A', [1.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (3, 'A', [1.0, 2.0, 3.0])")
        wait_for_vector_writes(cql, table, 3)
        flush(cql, table)

        # flush another sstable to shadow the vector row
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (1, 'A', [1.0, 2.0, 3.0])")
        execute(cql, table, "DELETE FROM %s where pk = 2")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (3, 'A', [1.0, 2.0, 3.0])")
        wait_for_vector_writes(cql, table, 2)
        flush(cql, table)

        # flush another sstable with one new vector row
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (0, 'B', [2.0, 3.0, 4.0])")
        execute(cql, table, "INSERT INTO %s (pk, str_val, val) VALUES (4, 'B', [2.0, 3.0, 4.0])")
        wait_for_vector_writes(cql, table, 4)
        flush(cql, table)

        # the shadow vector has the highest score
        result = list(execute(cql, table, "SELECT pk FROM %s ORDER BY val ann of [1.0, 2.0, 3.0] LIMIT 4"))
        # The Java test expects the rows in the order 1, 3, 0, 4. But rows 1
        # and 3 have the same vector, as do rows 0 and 4, so the order within
        # each of these pairs isn't defined - Cassandra and Scylla return them
        # in different orders. So we check only that rows 1 and 3 come before
        # rows 0 and 4.
        assert set(pks(result[:2])) == {1, 3}
        assert set(pks(result[2:])) == {0, 4}

# The test shadowedPrimaryKeysRequireDeeperSearch was not translated, because
# it creates an SAI index on a non-vector column, which Scylla doesn't
# support (see vector_tester.py).

def testUpdateVectorToWorseAndBetterPositions(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, val vector<float, 2>, PRIMARY KEY(pk))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        execute(cql, table, "INSERT INTO %s (pk, val) VALUES (0, [1.0, 2.0])")
        execute(cql, table, "INSERT INTO %s (pk, val) VALUES (1, [1.0, 3.0])")
        wait_for_vector_writes(cql, table, 2)

        flush(cql, table)
        execute(cql, table, "INSERT INTO %s (pk, val) VALUES (0, [1.0, 4.0])")
        wait_for_search(cql, table, "SELECT pk FROM %s ORDER BY val ann of [1.0, 2.0] LIMIT 1",
                        lambda rows: pks(rows) == [1])

        for _ in before_and_after_flush(cql, table):
            assertRows(execute(cql, table, "SELECT pk FROM %s ORDER BY val ann of [1.0, 2.0] LIMIT 1"), row(1))
            assertRows(execute(cql, table, "SELECT pk FROM %s ORDER BY val ann of [1.0, 2.0] LIMIT 2"), row(1), row(0))

        # And now update pk 1 to show that we can get 0 too
        execute(cql, table, "INSERT INTO %s (pk, val) VALUES (1, [1.0, 5.0])")
        wait_for_search(cql, table, "SELECT pk FROM %s ORDER BY val ann of [1.0, 2.0] LIMIT 1",
                        lambda rows: pks(rows) == [0])

        for _ in before_and_after_flush(cql, table):
            assertRows(execute(cql, table, "SELECT pk FROM %s ORDER BY val ann of [1.0, 2.0] LIMIT 1"), row(0))
            assertRows(execute(cql, table, "SELECT pk FROM %s ORDER BY val ann of [1.0, 2.0] LIMIT 2"), row(0), row(1))

        # And now update both PKs so that the stream of ranked rows is PKs: 0, 1, [1], 0, 1, [0], where the numbers
        # wrapped in brackets are the "real" scores of the vectors. This test makes sure that we correctly remove
        # PrimaryKeys from the updatedKeys map so that we don't accidentally duplicate PKs.
        execute(cql, table, "INSERT INTO %s (pk, val) VALUES (1, [1.0, 3.5])")
        wait_for_search(cql, table, "SELECT pk FROM %s ORDER BY val ann of [1.0, 2.0] LIMIT 1",
                        lambda rows: pks(rows) == [1])
        execute(cql, table, "INSERT INTO %s (pk, val) VALUES (0, [1.0, 6.0])")
        # This overwrite doesn't change the result of the searches below, so
        # there is nothing to wait for.

        for _ in before_and_after_flush(cql, table):
            assertRows(execute(cql, table, "SELECT pk FROM %s ORDER BY val ann of [1.0, 2.0] LIMIT 1"), row(1))
            assertRows(execute(cql, table, "SELECT pk FROM %s ORDER BY val ann of [1.0, 2.0] LIMIT 2"), row(1), row(0))

# The test updatedPrimaryKeysRequireResumeSearch was not translated, because
# it creates an SAI index on a non-vector column, which Scylla doesn't
# support (see vector_tester.py).

# Cassandra's randomVectorBoxed()
def randomVector(dimension):
    return [random.random() for _ in range(dimension)]

# Reproduces VECTOR-1050: Scylla doesn't support token() restrictions in
# an ANN search.
@pytest.mark.xfail(reason="VECTOR-1050")
def testBruteForceRangeQueryWithUpdatedVectors1536D(cql, test_keyspace, needs_vector_store):
    do_testBruteForceRangeQueryWithUpdatedVectors(cql, test_keyspace, 1536)

# Reproduces VECTOR-1050: Scylla doesn't support token() restrictions in
# an ANN search.
@pytest.mark.xfail(reason="VECTOR-1050")
def testBruteForceRangeQueryWithUpdatedVectors2D(cql, test_keyspace, needs_vector_store):
    do_testBruteForceRangeQueryWithUpdatedVectors(cql, test_keyspace, 2)

def do_testBruteForceRangeQueryWithUpdatedVectors(cql, test_keyspace, vectorDimension):
    with create_table(cql, test_keyspace, f"(pk int, val vector<float, {vectorDimension}>, PRIMARY KEY(pk))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        # Insert 100 vectors
        for i in range(100):
            execute(cql, table, "INSERT INTO %s (pk, val) VALUES (?, ?)", i, randomVector(vectorDimension))
        wait_for_vector_writes(cql, table, 100)

        # Update those vectors so some ordinals are changed
        for i in range(100):
            execute(cql, table, "INSERT INTO %s (pk, val) VALUES (?, ?)", i, randomVector(vectorDimension))

        # Delete the first 50 PKs.
        for i in range(50):
            execute(cql, table, "DELETE FROM %s WHERE pk = ?", i)
        wait_for_vector_writes(cql, table, 50)

        # All of the above inserts and deletes are performed on the same index to verify internal index behavior
        # for both memtables and sstables.
        for _ in before_and_after_flush(cql, table):
            # Query for the first 10 vectors, we don't care which.
            # Use a range query to hit the right brute force code path
            results = list(execute(cql, table, "SELECT pk FROM %s WHERE token(pk) < 0 ORDER BY val ann of ? LIMIT 10",
                                  randomVector(vectorDimension)))
            assert len(results) == 10
            # Make sure we don't get any of the deleted PKs
            assert all(row.pk >= 50 for row in results)

# The test testVectorIndexWithAllOrdinalsDeletedAndSomeViaRangeDeletion was
# not translated, because it creates an SAI index on a non-vector column,
# which Scylla doesn't support (see vector_tester.py).
# The test ensureCompressedVectorsCanFlush was not translated, because it
# checks Cassandra's internal sstable index files (verifySSTableIndexes()),
# and depends on an internal constant of its vector index (MIN_PQ_ROWS).

# This test mimics having rf > 1.
# The Java test calls disableCompaction(), which we don't need - see
# testshadowedPrimaryKeyInDifferentSSTable above.
# Reproduces #24430: The vector index requires CDC on the table, and Scylla's
# CDC refuses writes with a timestamp far in the past, such as the
# "USING TIMESTAMP 1" in this test.
@pytest.mark.xfail(reason="#24430")
def testSameRowInMultipleSSTablesWithSameTimestamp(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, ck int, val vector<float, 3>, PRIMARY KEY(pk, ck))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        # This test is fairly contrived, but covers the case where the first row we attempt to materialize in the
        # ScoreOrderedResultRetriever is shadowed by a row in a different sstable. And then, when we go to pull in
        # the next row, we find that the PK is already pulled in, so we need to skip it.
        execute(cql, table, "INSERT INTO %s (pk, ck, val) VALUES (0, 0, [1.0, 2.0, 3.0])")
        execute(cql, table, "INSERT INTO %s (pk, ck, val) VALUES (0, 1, [1.0, 2.0, 3.0]) USING TIMESTAMP 1")
        wait_for_vector_writes(cql, table, 2)
        flush(cql, table)
        # Now, delete row pk=0, ck=0 so that we can test that the shadowed row is not returned and that we need
        # to get the next row from the score ordered iterator.
        execute(cql, table, "DELETE FROM %s WHERE pk = 0 AND ck = 0")
        execute(cql, table, "INSERT INTO %s (pk, ck, val) VALUES (0, 1, [1.0, 2.0, 3.0]) USING TIMESTAMP 1")
        wait_for_vector_writes(cql, table, 1)

        for _ in before_and_after_flush(cql, table):
            assertRows(execute(cql, table, "SELECT ck FROM %s ORDER BY val ANN OF [1.0, 2.0, 3.0] LIMIT 2"), row(1))

def testMemtableInsertSearchUpdateSearchHandling(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(id text PRIMARY KEY, embedding vector<float, 5>)") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(embedding) USING 'StorageAttachedIndex' " +
                    "WITH OPTIONS = {'similarity_function': 'dot_product'}")

        # Insert initial data
        execute(cql, table, "INSERT INTO %s (id, embedding) VALUES ('row1', [0.1, 0.1, 0.1, 0.1, 0.1])")
        execute(cql, table, "INSERT INTO %s (id, embedding) VALUES ('row2', [0.9, 0.9, 0.9, 0.9, 0.9])")
        wait_for_vector_writes(cql, table, 2)

        # Query 100 times to try to guarantee all graph searchers are initialized
        for i in range(100):
            # Initial vector search
            initialSearch = execute(cql, table, "SELECT * FROM %s ORDER BY embedding ANN OF [0.8, 0.8, 0.8, 0.8, 0.8] LIMIT 1")
            assertRowCount(initialSearch, 1)

        # Update one of the rows (this update wasn't observed due to state leaked between queries previously)
        execute(cql, table, "UPDATE %s SET embedding = [0.7, 0.7, 0.7, 0.7, 0.7] WHERE id = 'row1'")
        # This update doesn't change the number of rows that the searches
        # below return, so there is nothing to wait for.

        # Query 100 times to make sure it works as expected
        for j in range(100):
            # Get all data to verify we have 2 rows
            allData = execute(cql, table, "SELECT * FROM %s ORDER BY embedding ANN OF [0.8, 0.8, 0.8, 0.8, 0.8] LIMIT 1000")
            assertRowCount(allData, 2)

# Reproduces VECTOR-1049: Scylla accepts a vector index on a static column,
# but the vector store never serves it.
@pytest.mark.xfail(reason="VECTOR-1049")
def testUpdatedVectorStaticVectorColumnIndex(cql, test_keyspace, needs_vector_store):
    with create_table(cql, test_keyspace, "(pk int, ck int, val vector<float, 2> static, PRIMARY KEY(pk, ck))") as table:
        create_index(cql, table, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'")

        # This counts as an update because the indexed column is static and is therefore operated on at the partition
        # level.
        execute(cql, table, "INSERT INTO %s (pk, ck, val) VALUES (0, 1, [0,2])")
        execute(cql, table, "INSERT INTO %s (pk, ck, val) VALUES (0, 2, [1,0])")
        execute(cql, table, "INSERT INTO %s (pk, ck, val) VALUES (1, 3, [0,1])")
        # The static column has 2 different values, in 2 partitions, and the
        # search near [1,0] should find the new value of partition 0.
        wait_for_search(cql, table, "SELECT ck FROM %s ORDER BY val ANN OF [1,0] LIMIT 2",
                        lambda rows: [r.ck for r in rows] == [1, 2])

        for _ in before_and_after_flush(cql, table):
            assertRows(execute(cql, table, "SELECT ck FROM %s ORDER BY val ANN OF [1,0] LIMIT 2"), row(1), row(2))
