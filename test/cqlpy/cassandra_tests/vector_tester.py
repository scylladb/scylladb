# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Helpers shared by the translations of Cassandra's vector search tests
# (test/unit/org/apache/cassandra/index/sai/cql/Vector*Test.java in the
# Cassandra source repository), in place of their Java base classes
# VectorTester.java and SAITester.java.
#
# These tests perform real vector searches, so on Scylla they need a vector
# store - test/cqlpy/run runs one with the "--vs" option. The tests use the
# needs_vector_store fixture, so they are skipped without it.
#
# HOW SCYLLA'S VECTOR SEARCH DIFFERS FROM CASSANDRA'S
#
# Scylla's CQL vector search API was designed to be compatible with
# Cassandra's, so most of the syntax used by Cassandra's tests works
# unchanged on Scylla:
#
# * The vector<float, N> type, its literals, and the similarity_cosine(),
#   similarity_euclidean() and similarity_dot_product() functions.
# * Creating an index on a vector column with Cassandra's
#   CREATE CUSTOM INDEX ... USING 'StorageAttachedIndex' (or 'sai', or the
#   full Java class name). Scylla doesn't have SAI, but on a vector column it
#   rewrites such an index to its own "vector_index", which accepts the same
#   'similarity_function' option.
# * Searching with SELECT ... ORDER BY v ANN OF [...] LIMIT n, with the same
#   restrictions - e.g., a LIMIT is required, and aggregation isn't allowed.
#
# The differences are mostly not in the syntax, but in the implementation
# behind it:
#
# 1. In Cassandra, SAI is a general-purpose secondary index, which can index
#    any column, and an ANN search can be combined with WHERE restrictions on
#    other columns that have their own SAI index. Scylla refuses to create an
#    SAI on a non-vector column. In Scylla, the columns that an ANN search
#    can be filtered on are part of the vector index itself
#    (CREATE CUSTOM INDEX ON t(v, f1, f2)), and even then a global vector
#    index requires ALLOW FILTERING for such a search - also when the
#    restriction is on the partition key. Scylla also has local vector
#    indexes, CREATE CUSTOM INDEX ON t((pk), v), for searches within a
#    single partition. See docs/cql/secondary-indexes.rst.
# 2. In Cassandra, the vector index is part of the database node, and is
#    updated synchronously with each write. In Scylla, it is maintained by a
#    separate vector store, asynchronously - see the next section.
# 3. Scylla's vector store has its own limitations, e.g., it cannot index a
#    table that has a vector column in its primary key (VECTOR-687).
#
# WHAT WE COULD, AND COULD NOT, TRANSLATE
#
# Of Cassandra's vector search tests (Vector*Test.java), we translated the
# ones that use only vector indexes, possibly combined with restrictions on
# primary key columns. Where Scylla behaves differently, the translated test
# is marked xfail with the relevant issue. We did not translate:
#
# * Tests that create an SAI index on non-vector columns, to combine an ANN
#   search with restrictions on those columns, or to search them without
#   ANN (difference 1 above). These are about 30 tests, out of 75 in the
#   files other than VectorInvalidQueryTest.java. Rewriting them to use Scylla's filtering columns would require different
#   CREATE INDEX statements on the two databases, and would test something
#   other than what the original test checked.
# * Tests that check SAI's internals through Java APIs, such as how the index
#   is split into segments, or its memory limits. Scylla's vector store has
#   no such structure. Tests that merely call flush() or compact() between
#   their steps were translated - on Scylla, these don't affect the vector
#   index, but they also do no harm.
# * Tests of Cassandra-only features or messages, like the warning that
#   Cassandra prints when creating an SAI index.
#
# Each translated file lists, near the tests' original location, the tests
# it did not translate and why.
#
# WHY THE TESTS WAIT FOR THE VECTOR INDEX
#
# In Cassandra, a vector index is a storage-attached index (SAI), which is
# part of the Cassandra node itself: Cassandra's createIndex() waits until a
# new index is built, and every write updates the vector index synchronously,
# as part of the write. So Cassandra's tests search the index right after
# creating it, and right after writing, and expect to see all the data.
#
# In Scylla, a vector index is built and searched by a separate process, the
# vector store. When the index is created, the vector store first scans the
# existing data, and only then starts serving searches; Later writes reach it
# asynchronously, through CDC. So on Scylla, a search right after creating
# the index may fail because the index is not ready yet, and a search right
# after a write may not see the write yet. The translated tests therefore
# wait, with the helpers below, until the vector store has caught up:
#
# * create_index() replaces Cassandra's createIndex(): it creates the index
#   and waits until the vector store is ready to serve searches on it.
# * wait_for_vector_writes() should be called after writes, before a search,
#   and waits until the vector store has indexed the expected number of
#   vectors.
# * When a write doesn't change the number of indexed vectors (e.g., it
#   overwrites a vector), a test can use wait_for_search(), which repeats a
#   search until it returns the expected result.
#
# A wait must not be satisfied before all the writes it waits for reached
# the vector store. For example, if a test inserts two rows and deletes one,
# waiting for one indexed vector may return after the vector store saw just
# the first insert. So in such cases, the translated tests wait after each
# step - e.g., for two vectors after the inserts, and then for one after the
# delete - although the original Java test had no wait between these steps.
#
# On Cassandra, all these helpers return immediately, because there is
# nothing to wait for.

from contextlib import contextmanager
from .porting import *
from ..util import is_scylla, unique_name, wait_for_vector_index, wait_for_vector_search

# Replaces porting.create_table() for the vector search tests. That one
# reuses the names of dropped tables, to exercise dropping and re-creating
# a table with the same name. But the vector store doesn't handle this
# well, and an index on a re-created table may be reported ready while the
# vector store still serves the dropped table's index (VECTOR-1008), or may
# never be served at all (VECTOR-1048). Each test using a fresh table name
# avoids one test's table breaking an unrelated test.
@contextmanager
def create_table(cql, keyspace, schema):
    table = keyspace + "." + unique_name()
    cql.execute("CREATE TABLE " + table + " " + schema)
    try:
        yield table
    finally:
        cql.execute("DROP TABLE " + table)

# The names of the vector indexes of the given table.
def vector_index_names(cql, table):
    keyspace, table_name = table.split('.')
    rows = cql.execute("SELECT index_name, options FROM system_schema.indexes WHERE keyspace_name = %s AND table_name = %s",
                       (keyspace, table_name))
    return [r.index_name for r in rows if r.options.get('class_name') in ('vector_index', 'StorageAttachedIndex')]

# Cassandra's createIndex(): run the given CREATE INDEX statement, and wait
# until the index is ready (see the explanation above).
def create_index(cql, table, statement):
    execute(cql, table, statement)
    wait_for_vector_writes(cql, table)

# Wait until the vector store is serving all the vector indexes of the given
# table and, if expected_size is given, until each of them has indexed
# expected_size vectors (rows whose vector is null are not counted). See the
# explanation above. On Cassandra, returns immediately.
def wait_for_vector_writes(cql, table, expected_size=None):
    if not is_scylla(cql):
        return
    keyspace = table.split('.')[0]
    for index in vector_index_names(cql, table):
        wait_for_vector_index(cql, keyspace, index, expected_size)

# Repeat the given search (a statement with %s standing for the table) until
# condition(rows) is true for its result rows. See the explanation above. On
# Cassandra, returns immediately.
def wait_for_search(cql, table, query, condition):
    if not is_scylla(cql):
        return
    wait_for_vector_search(cql, subs_table(query, table), condition,
                           "The vector store didn't return the expected search result")
