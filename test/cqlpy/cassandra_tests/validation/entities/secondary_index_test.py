# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 6ca34f81386dc8f6020cdf2ea4246bca2a0896c5
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0


# SCYLLA NOTE (FIXME): The following tests, originally written for Cassandra,
# implicitly assume that index writes are synchronous, i.e., immediately
# after updating the base table entry, it can be found using the index.
# In Scylla, where secondary indexes are implemented using materialized
# views, this is *not* always the case - an index write may be asynchronous
# and may not be available for read immediately after the base write.
# Nevertheless, there are situations where Scylla's MV (and SI) writes are
# guaranteed synchronous. One of them is when N=RF, or in particular, when
# N=1. In this case, the base and view replica are always the same node,
# so writes can be done synchronously - and Scylla does guarantee this.
#
# All this means that when run on a 1-node cluster (N=1) and test_keyspace
# uses RF=1, the following tests will run as expected, but in other cases,
# reads may not find the expected data if not retried. If we want to support
# N>1, we can solve this problem by adding retry loops to the test, or by
# replacing test_keyspace with a keyspace that uses RF=N instead of RF=1.

# Note: During the translation of the Cassandra tests to Python, I removed
# many tests which required accessing Cassandra internals and not (or not
# just) CQL. This includes tests using various ad-hoc Index classes
# (StubIndex, IndexBlockingOnInitialization, ReadOnlyOnFailureIndex,
# WriteOnlyOnFailureIndex), or require changing Cassandra's caching
# internals, or access to prepared statements implementation details not
# available through the Python driver.
# The deleted tests include:
# * testMultipleIndexesOnOneColumn
# * testDeletions
# * testUpdatesToMemtableData
# * testIndexQueriesWithIndexNotReady
# * testReadOnlyIndex
# * testWriteOnlyIndex
# * testCanQuerySecondaryIndex
# * testDroppingIndexInvalidatesPreparedStatements

from cassandra_tests.porting import *
from cassandra.protocol import SyntaxException, AlreadyExists, InvalidRequest, ConfigurationException
from uuid import UUID

# Test creating and dropping an index with the specified name.
# @param indexName         the index name
# @param addKeyspaceOnDrop add the keyspace name in the drop statement
# @throws Throwable if an error occurs
def dotestCreateAndDropIndex(cql, table, indexName, addKeyspaceOnDrop):
    KEYSPACE = table.split('.')[0]
    INDEX = indexName.replace('"', '').lower()
    # The failure message in Scylla and Cassandra differ: Cassandra has
    # "Index '{KEYSPACE}.{INDEX}' doesn't exist", while Scylla has
    # "Index '{INDEX}' could not be found in any of the tables of keyspace '{KEYSPACE}
    assert_invalid_message(cql, table, INDEX,
        f"DROP INDEX {KEYSPACE}.{indexName}")

    execute(cql, table, "CREATE INDEX " + indexName + " ON %s(b);")
    execute(cql, table, "CREATE INDEX IF NOT EXISTS " + indexName + " ON %s(b);")
    # The failure message in Scylla and Cassandra differ: Cassandra has
    # "Index '{INDEX}' already exists", while Scylla has "Index already
    # exists" (without the index name)
    assert_invalid_message(cql, table, f"already exists",
        "CREATE INDEX " + indexName + " ON %s(b)")

    # IF NOT EXISTS should apply in cases where the new index differs from
    # an existing one in name only
    otherIndexName = "index_" + unique_name()
    execute(cql, table, "CREATE INDEX IF NOT EXISTS " + otherIndexName + " ON %s(b)")
    assert_invalid_message(cql, table, f"Index {otherIndexName} is a duplicate of existing index {INDEX}",
        "CREATE INDEX " + otherIndexName + " ON %s(b)")
    execute(cql, table, "INSERT INTO %s (a, b) values (?, ?);", 0, 0)
    execute(cql, table, "INSERT INTO %s (a, b) values (?, ?);", 1, 1)
    execute(cql, table, "INSERT INTO %s (a, b) values (?, ?);", 2, 2)
    execute(cql, table, "INSERT INTO %s (a, b) values (?, ?);", 3, 1)

    assert_rows(execute(cql, table, "SELECT * FROM %s where b = ?", 1), [1, 1], [3, 1])

    if addKeyspaceOnDrop:
        execute(cql, table, f"DROP INDEX {KEYSPACE}.{indexName}")
    else:
        execute(cql, table, f"USE {KEYSPACE}")
        execute(cql, table, f"DROP INDEX {indexName}")
    assert_invalid_message(cql, table, "ALLOW FILTERING",
                         "SELECT * FROM %s where b = ?", 1)
    execute(cql, table, f"DROP INDEX IF EXISTS {KEYSPACE}.{indexName}")
    # The failure message in Scylla and Cassandra differ: Cassandra has
    # "Index '{KEYSPACE}.{INDEX}' doesn't exist", while Scylla has
    # "Index '{INDEX}' could not be found in any of the tables of keyspace '{KEYSPACE}
    assert_invalid_message(cql, table, INDEX,
        f"DROP INDEX {KEYSPACE}.{indexName}")

@pytest.fixture(scope="module")
# FIXME: LWT is not supported with tablets yet. See #18066
def table1(cql, test_keyspace_vnodes):
    with create_table(cql, test_keyspace_vnodes, "(a int primary key, b int)") as table:
        yield table

# Reproduces #8717 (CREATE INDEX IF NOT EXISTS was broken):
def testCreateAndDropIndex(cql, table1):
    dotestCreateAndDropIndex(cql, table1, "test", False)
    dotestCreateAndDropIndex(cql, table1, "test2", True)

# Reproduces #8717 (CREATE INDEX IF NOT EXISTS was broken):
def testCreateAndDropIndexWithQuotedIdentifier(cql, table1):
    dotestCreateAndDropIndex(cql, table1, "\"quoted_ident\"", False)
    dotestCreateAndDropIndex(cql, table1, "\"quoted_ident2\"", True)

# Reproduces #8717 (CREATE INDEX IF NOT EXISTS was broken):
def testCreateAndDropIndexWithCamelCaseIdentifier(cql, table1):
    dotestCreateAndDropIndex(cql, table1, "CamelCase", False)
    dotestCreateAndDropIndex(cql, table1, "CamelCase2", True)

# Check that you can query for an indexed column even with a key EQ clause,
# migrated from cql_tests.py:TestCQL.static_cf_test()
def testSelectWithEQ(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(userid uuid PRIMARY KEY, firstname text, lastname text, age int)") as table:
        execute(cql, table, "CREATE INDEX byAge ON %s(age)")

        id1 = UUID("550e8400-e29b-41d4-a716-446655440000")
        id2 = UUID("f47ac10b-58cc-4372-a567-0e02b2c3d479")

        execute(cql, table, "INSERT INTO %s (userid, firstname, lastname, age) VALUES (?, 'Frodo', 'Baggins', 32)", id1)
        execute(cql, table, "UPDATE %s SET firstname = 'Samwise', lastname = 'Gamgee', age = 33 WHERE userid = ?", id2)

        for _ in before_and_after_flush(cql, table):
            assert_empty(execute(cql, table, "SELECT firstname FROM %s WHERE userid = ? AND age = 33", id1))
            assert_rows(execute(cql, table, "SELECT firstname FROM %s WHERE userid = ? AND age = 33", id2), ["Samwise"])

# Check CREATE INDEX without name and validate the index can be dropped,
# migrated from cql_tests.py:TestCQL.nameless_index_test()
# See also Scylla issue #4146
def testNamelessIndex(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id text PRIMARY KEY, birth_year int)") as table:
        execute(cql, table, "CREATE INDEX on %s (birth_year)")

        execute(cql, table, "INSERT INTO %s (id, birth_year) VALUES ('Tom', 42)")
        execute(cql, table, "INSERT INTO %s (id, birth_year) VALUES ('Paul', 24)")
        execute(cql, table, "INSERT INTO %s (id, birth_year) VALUES ('Bob', 42)")

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT id FROM %s WHERE birth_year = 42"),
                       ["Tom"],
                       ["Bob"])

        execute(cql, table, "DROP INDEX %s_birth_year_idx")

        assert_invalid(cql, table, "SELECT id FROM users WHERE birth_year = 42")

# Test range queries with 2ndary indexes (CASSANDRA-4257),
# migrated from cql_tests.py:TestCQL.range_query_2ndary_test()
def testRangeQuery(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id int primary key, row int, setid int)") as table:
        execute(cql, table, "CREATE INDEX indextest_setid_idx ON %s (setid)")

        execute(cql, table, "INSERT INTO %s (id, row, setid) VALUES (?, ?, ?)", 0, 0, 0)
        execute(cql, table, "INSERT INTO %s (id, row, setid) VALUES (?, ?, ?)", 1, 1, 0)
        execute(cql, table, "INSERT INTO %s (id, row, setid) VALUES (?, ?, ?)", 2, 2, 0)
        execute(cql, table, "INSERT INTO %s (id, row, setid) VALUES (?, ?, ?)", 3, 3, 0)

        assert_invalid(cql, table, "SELECT * FROM %s WHERE setid = 0 AND row < 1")

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE setid = 0 AND row < 1 ALLOW FILTERING"),
                       [0, 0, 0])

# Check for unknown compression parameters options (CASSANDRA-4266),
# migrated from cql_tests.py:TestCQL.compression_option_validation_test()
def testUnknownCompressionOptions(cql, test_keyspace):
    with pytest.raises(SyntaxException):
        with create_table(cql, test_keyspace, "(key varchar PRIMARY KEY, password varchar, gender varchar) WITH compression_parameters:sstable_compressor = 'DeflateCompressor'"):
            pass
    with pytest.raises(ConfigurationException):
        with create_table(cql, test_keyspace, "(key varchar PRIMARY KEY, password varchar, gender varchar) WITH compression = { 'sstable_compressor': 'DeflateCompressor' }"):
            pass

# Migrated from cql_tests.py:TestCQL.indexes_composite_test()
# Uses wait_for_index() so reproduces #8600.
def testIndexOnComposite(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(blog_id int, timestamp int, author text, content text, PRIMARY KEY (blog_id, timestamp))") as table:
        execute(cql, table, "INSERT INTO %s (blog_id, timestamp, author, content) VALUES (?, ?, ?, ?)", 0, 0, "bob", "1st post")
        execute(cql, table, "INSERT INTO %s (blog_id, timestamp, author, content) VALUES (?, ?, ?, ?)", 0, 1, "tom", "2nd post")
        execute(cql, table, "INSERT INTO %s (blog_id, timestamp, author, content) VALUES (?, ?, ?, ?)", 0, 2, "bob", "3rd post")
        execute(cql, table, "INSERT INTO %s (blog_id, timestamp, author, content) VALUES (?, ?, ?, ?)", 0, 3, "tom", "4th post")
        execute(cql, table, "INSERT INTO %s (blog_id, timestamp, author, content) VALUES (?, ?, ?, ?)", 1, 0, "bob", "5th post")
        execute(cql, table, "CREATE INDEX authoridx ON %s (author)")

        assert wait_for_index(cql, table, "authoridx")

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT blog_id, timestamp FROM %s WHERE author = 'bob'"),
                       [1, 0],
                       [0, 0],
                       [0, 2])

        execute(cql, table, "INSERT INTO %s (blog_id, timestamp, author, content) VALUES (?, ?, ?, ?)", 1, 1, "tom", "6th post")
        execute(cql, table, "INSERT INTO %s (blog_id, timestamp, author, content) VALUES (?, ?, ?, ?)", 1, 2, "tom", "7th post")
        execute(cql, table, "INSERT INTO %s (blog_id, timestamp, author, content) VALUES (?, ?, ?, ?)", 1, 3, "bob", "8th post")

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT blog_id, timestamp FROM %s WHERE author = 'bob'"),
                       [1, 0],
                       [1, 3],
                       [0, 0],
                       [0, 2])

        execute(cql, table, "DELETE FROM %s WHERE blog_id = 0 AND timestamp = 2")

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT blog_id, timestamp FROM %s WHERE author = 'bob'"),
                       [1, 0],
                       [1, 3],
                       [0, 0])

# Test for the validation bug of CASSANDRA-4709,
# migrated from cql_tests.py:TestCQL.refuse_in_with_indexes_test()
def testInvalidIndexSelect(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk varchar primary key, col1 varchar, col2 varchar)") as table:
        execute(cql, table, "create index on %s (col1)")
        execute(cql, table, "create index on %s (col2)")

        execute(cql, table, "insert into %s (pk, col1, col2) values ('pk1','foo1','bar1')")
        execute(cql, table, "insert into %s (pk, col1, col2) values ('pk1a','foo1','bar1')")
        execute(cql, table, "insert into %s (pk, col1, col2) values ('pk1b','foo1','bar1')")
        execute(cql, table, "insert into %s (pk, col1, col2) values ('pk1c','foo1','bar1')")
        execute(cql, table, "insert into %s (pk, col1, col2) values ('pk2','foo2','bar2')")
        execute(cql, table, "insert into %s (pk, col1, col2) values ('pk3','foo3','bar3')")
        assert_invalid(cql, table, "select * from %s where col2 in ('bar1', 'bar2')")

    #Migrated from cql_tests.py:TestCQL.bug_6050_test()
    with create_table(cql, test_keyspace, "(k int primary key, a int, b int)") as table:
        execute(cql, table, "CREATE INDEX ON %s (a)")
        assert_invalid(cql, table, "SELECT * FROM %s WHERE a = 3 AND b IN (1, 3)")

# Migrated from cql_tests.py:TestCQL.edge_2i_on_complex_pk_test()
def testIndexesOnComplexPrimaryKey(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk0 int, pk1 int, ck0 int, ck1 int, ck2 int, value int, PRIMARY KEY ((pk0, pk1), ck0, ck1, ck2))") as table:
        execute(cql, table, "CREATE INDEX ON %s (pk0)")
        execute(cql, table, "CREATE INDEX ON %s (ck0)")
        execute(cql, table, "CREATE INDEX ON %s (ck1)")
        execute(cql, table, "CREATE INDEX ON %s (ck2)")

        execute(cql, table, "INSERT INTO %s (pk0, pk1, ck0, ck1, ck2, value) VALUES (0, 1, 2, 3, 4, 5)")
        execute(cql, table, "INSERT INTO %s (pk0, pk1, ck0, ck1, ck2, value) VALUES (1, 2, 3, 4, 5, 0)")
        execute(cql, table, "INSERT INTO %s (pk0, pk1, ck0, ck1, ck2, value) VALUES (2, 3, 4, 5, 0, 1)")
        execute(cql, table, "INSERT INTO %s (pk0, pk1, ck0, ck1, ck2, value) VALUES (3, 4, 5, 0, 1, 2)")
        execute(cql, table, "INSERT INTO %s (pk0, pk1, ck0, ck1, ck2, value) VALUES (4, 5, 0, 1, 2, 3)")
        execute(cql, table, "INSERT INTO %s (pk0, pk1, ck0, ck1, ck2, value) VALUES (5, 0, 1, 2, 3, 4)")

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT value FROM %s WHERE pk0 = 2"),
                       [1])

            assert_rows(execute(cql, table, "SELECT value FROM %s WHERE ck0 = 0"),
                       [3])

            assert_rows(execute(cql, table, "SELECT value FROM %s WHERE pk0 = 3 AND pk1 = 4 AND ck1 = 0"),
                       [2])

            assert_rows(execute(cql, table, "SELECT value FROM %s WHERE pk0 = 5 AND pk1 = 0 AND ck0 = 1 AND ck2 = 3 ALLOW FILTERING"),
                       [4])

# Test for CASSANDRA-5240,
# migrated from cql_tests.py:TestCQL.bug_5240_test()
def testIndexOnCompoundRowKey(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(interval text, seq int, id int, severity int, PRIMARY KEY ((interval, seq), id)) WITH CLUSTERING ORDER BY (id DESC)") as table:
        execute(cql, table, "CREATE INDEX ON %s (severity)")

        execute(cql, table, "insert into %s (interval, seq, id , severity) values('t',1, 1, 1)")
        execute(cql, table, "insert into %s (interval, seq, id , severity) values('t',1, 2, 1)")
        execute(cql, table, "insert into %s (interval, seq, id , severity) values('t',1, 3, 2)")
        execute(cql, table, "insert into %s (interval, seq, id , severity) values('t',1, 4, 3)")
        execute(cql, table, "insert into %s (interval, seq, id , severity) values('t',2, 1, 3)")
        execute(cql, table, "insert into %s (interval, seq, id , severity) values('t',2, 2, 3)")
        execute(cql, table, "insert into %s (interval, seq, id , severity) values('t',2, 3, 1)")
        execute(cql, table, "insert into %s (interval, seq, id , severity) values('t',2, 4, 2)")

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "select * from %s where severity = 3 and interval = 't' and seq =1"),
                       ["t", 1, 4, 3])

# Migrated from cql_tests.py:TestCQL.secondary_index_counters()
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18180")]), "vnodes"],
                         indirect=True)
def testIndexOnCountersInvalid(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, c counter)") as table:
        assert_invalid(cql, table, "CREATE INDEX ON %s(c)")

# Migrated from cql_tests.py:TestCQL.collection_indexing_test()
def testIndexOnCollections(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, v int, l list<int>, s set<text>, m map<text, int>, PRIMARY KEY (k, v))") as table:
        execute(cql, table, "CREATE INDEX ON %s (l)")
        execute(cql, table, "CREATE INDEX ON %s (s)")
        execute(cql, table, "CREATE INDEX ON %s (m)")

        execute(cql, table, "INSERT INTO %s (k, v, l, s, m) VALUES (0, 0, [1, 2],    {'a'},      {'a' : 1})")
        execute(cql, table, "INSERT INTO %s (k, v, l, s, m) VALUES (0, 1, [3, 4],    {'b', 'c'}, {'a' : 1, 'b' : 2})")
        execute(cql, table, "INSERT INTO %s (k, v, l, s, m) VALUES (0, 2, [1],       {'a', 'c'}, {'c' : 3})")
        execute(cql, table, "INSERT INTO %s (k, v, l, s, m) VALUES (1, 0, [1, 2, 4], {},         {'b' : 1})")
        execute(cql, table, "INSERT INTO %s (k, v, l, s, m) VALUES (1, 1, [4, 5],    {'d'},      {'a' : 1, 'b' : 3})")

        for _ in before_and_after_flush(cql, table):
            # lists
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE l CONTAINS 1"), [1, 0], [0, 0], [0, 2])
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE k = 0 AND l CONTAINS 1"), [0, 0], [0, 2])
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE l CONTAINS 2"), [1, 0], [0, 0])
            assert_empty(execute(cql, table, "SELECT k, v FROM %s WHERE l CONTAINS 6"))

            # sets
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE s CONTAINS 'a'"), [0, 0], [0, 2])
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE k = 0 AND s CONTAINS 'a'"), [0, 0], [0, 2])
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE s CONTAINS 'd'"), [1, 1])
            assert_empty(execute(cql, table, "SELECT k, v FROM %s  WHERE s CONTAINS 'e'"))

            # maps
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE m CONTAINS 1"), [1, 0], [1, 1], [0, 0], [0, 1])
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE k = 0 AND m CONTAINS 1"), [0, 0], [0, 1])
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE m CONTAINS 2"), [0, 1])
            assert_empty(execute(cql, table, "SELECT k, v FROM %s  WHERE m CONTAINS 4"))

def testSelectOnMultiIndexOnCollectionsWithNull(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, v int, x text, l list<int>, s set<text>, m map<text, int>, PRIMARY KEY (k, v))") as table:
        execute(cql, table, "CREATE INDEX ON %s (x)")
        execute(cql, table, "CREATE INDEX ON %s (v)")
        execute(cql, table, "CREATE INDEX ON %s (s)")
        execute(cql, table, "CREATE INDEX ON %s (m)")

        execute(cql, table, "INSERT INTO %s (k, v, x, l, s, m) VALUES (0, 0, 'x', [1, 2],    {'a'},      {'a' : 1})")
        execute(cql, table, "INSERT INTO %s (k, v, x, l, s, m) VALUES (0, 1, 'x', [3, 4],    {'b', 'c'}, {'a' : 1, 'b' : 2})")
        execute(cql, table, "INSERT INTO %s (k, v, x, l, s, m) VALUES (0, 2, 'x', [1],       {'a', 'c'}, {'c' : 3})")
        execute(cql, table, "INSERT INTO %s (k, v, x, l, s, m) VALUES (1, 0, 'x', [1, 2, 4], {},         {'b' : 1})")
        execute(cql, table, "INSERT INTO %s (k, v, x, l, s, m) VALUES (1, 1, 'x', [4, 5],    {'d'},      {'a' : 1, 'b' : 3})")
        execute(cql, table, "INSERT INTO %s (k, v, x, l, s, m) VALUES (1, 2, 'x', null,      null,       null)")

        for _ in before_and_after_flush(cql, table):
            # lists
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE x = 'x' AND l CONTAINS 1 ALLOW FILTERING"), [1, 0], [0, 0], [0, 2])
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE x = 'x' AND k = 0 AND l CONTAINS 1 ALLOW FILTERING"), [0, 0], [0, 2])
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE x = 'x' AND l CONTAINS 2 ALLOW FILTERING"), [1, 0], [0, 0])
            assert_empty(execute(cql, table, "SELECT k, v FROM %s WHERE x = 'x' AND l CONTAINS 6 ALLOW FILTERING"))

            # sets
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE x = 'x' AND s CONTAINS 'a' ALLOW FILTERING" ), [0, 0], [0, 2])
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE x = 'x' AND k = 0 AND s CONTAINS 'a' ALLOW FILTERING"), [0, 0], [0, 2])
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE x = 'x' AND s CONTAINS 'd' ALLOW FILTERING"), [1, 1])
            assert_empty(execute(cql, table, "SELECT k, v FROM %s  WHERE x = 'x' AND s CONTAINS 'e' ALLOW FILTERING"))

            # maps
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE x = 'x' AND m CONTAINS 1 ALLOW FILTERING"), [1, 0], [1, 1], [0, 0], [0, 1])
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE x = 'x' AND k = 0 AND m CONTAINS 1 ALLOW FILTERING"), [0, 0], [0, 1])
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE x = 'x' AND m CONTAINS 2 ALLOW FILTERING"), [0, 1])
            assert_empty(execute(cql, table, "SELECT k, v FROM %s  WHERE x = 'x' AND m CONTAINS 4 ALLOW FILTERING"))

# Migrated from cql_tests.py:TestCQL.map_keys_indexing()
def testIndexOnMapKeys(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, v int, m map<text, int>, PRIMARY KEY (k, v))") as table:
        execute(cql, table, "CREATE INDEX ON %s(keys(m))")

        execute(cql, table, "INSERT INTO %s (k, v, m) VALUES (0, 0, {'a' : 1})")
        execute(cql, table, "INSERT INTO %s (k, v, m) VALUES (0, 1, {'a' : 1, 'b' : 2})")
        execute(cql, table, "INSERT INTO %s (k, v, m) VALUES (0, 2, {'c' : 3})")
        execute(cql, table, "INSERT INTO %s (k, v, m) VALUES (1, 0, {'b' : 1})")
        execute(cql, table, "INSERT INTO %s (k, v, m) VALUES (1, 1, {'a' : 1, 'b' : 3})")

        # maps
        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE m CONTAINS KEY 'a'"), [1, 1], [0, 0], [0, 1])
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE k = 0 AND m CONTAINS KEY 'a'"), [0, 0], [0, 1])
            assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE m CONTAINS KEY 'c'"), [0, 2])
            assert_empty(execute(cql, table, "SELECT k, v FROM %s  WHERE m CONTAINS KEY 'd'"))

# Test for CASSANDRA-6950 bug,
# migrated from cql_tests.py:TestCQL.key_index_with_reverse_clustering()
def testIndexOnKeyWithReverseClustering(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k1 int, k2 int, v int, PRIMARY KEY ((k1, k2), v)) WITH CLUSTERING ORDER BY (v DESC)") as table:
        execute(cql, table, "CREATE INDEX ON %s (k2)")

        execute(cql, table, "INSERT INTO %s (k1, k2, v) VALUES (0, 0, 1)")
        execute(cql, table, "INSERT INTO %s (k1, k2, v) VALUES (0, 1, 2)")
        execute(cql, table, "INSERT INTO %s (k1, k2, v) VALUES (0, 0, 3)")
        execute(cql, table, "INSERT INTO %s (k1, k2, v) VALUES (1, 0, 4)")
        execute(cql, table, "INSERT INTO %s (k1, k2, v) VALUES (1, 1, 5)")
        execute(cql, table, "INSERT INTO %s (k1, k2, v) VALUES (2, 0, 7)")
        execute(cql, table, "INSERT INTO %s (k1, k2, v) VALUES (2, 1, 8)")
        execute(cql, table, "INSERT INTO %s (k1, k2, v) VALUES (3, 0, 1)")

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k2 = 0 AND v >= 2 ALLOW FILTERING"),
                       [2, 0, 7],
                       [0, 0, 3],
                       [1, 0, 4])

# Test for CASSANDRA-6612,
# migrated from cql_tests.py:TestCQL.bug_6612_test()
def testSelectCountOnIndexedColumn(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(username text, session_id text, app_name text, account text, last_access timestamp, created_on timestamp, PRIMARY KEY (username, session_id, app_name, account))") as table:
        execute(cql, table, "create index ON %s (app_name)")
        execute(cql, table, "create index ON %s (last_access)")

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "select count(*) from %s where app_name='foo' and account='bar' and last_access > 4 allow filtering"), [0])

        execute(cql, table, "insert into %s (username, session_id, app_name, account, last_access, created_on) values ('toto', 'foo', 'foo', 'bar', 12, 13)")

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "select count(*) from %s where app_name='foo' and account='bar' and last_access > 4 allow filtering"), [1])

def createAndDropCollectionValuesIndex(cql, keyspace, table, columnName):
    indexName = columnName + "_idx"
    execute(cql, table, f"CREATE INDEX {indexName} on %s({columnName})")
    execute(cql, table, f"DROP INDEX {keyspace}.{indexName}")
    execute(cql, table, f"CREATE INDEX {indexName} on %s(values({columnName}))")
    execute(cql, table, f"DROP INDEX {keyspace}.{indexName}")

def testSyntaxVariationsForIndexOnCollectionsValue(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, m map<int, int>, l list<int>, s set<int>, PRIMARY KEY (k))") as table:
        createAndDropCollectionValuesIndex(cql, test_keyspace, table, "m")
        createAndDropCollectionValuesIndex(cql, test_keyspace, table, "l")
        createAndDropCollectionValuesIndex(cql, test_keyspace, table, "s")

def createAndDropIndexWithQuotedColumnIdentifier(cql, keyspace, table, target):
    indexName = "test_mixed_case_idx"
    execute(cql, table, f"CREATE INDEX {indexName} ON %s({target})")
    execute(cql, table, f"DROP INDEX {keyspace}.{indexName}")

def testCreateIndexWithQuotedColumnNames(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(" +
                    " k int," +
                    " v int, " +
                    " lower_case_map map<int, int>," +
                    " \"MixedCaseMap\" map<int, int>," +
                    " lower_case_frozen_list frozen<list<int>>," +
                    " \"UPPER_CASE_FROZEN_LIST\" frozen<list<int>>," +
                    " \"set name with spaces\" set<int>," +
                    " \"column_name_with\"\"escaped quote\" int," +
                    " PRIMARY KEY (k))") as table:
        createAndDropIndexWithQuotedColumnIdentifier(cql, test_keyspace, table, "\"v\"")
        createAndDropIndexWithQuotedColumnIdentifier(cql, test_keyspace, table, "keys(\"lower_case_map\")")
        createAndDropIndexWithQuotedColumnIdentifier(cql, test_keyspace, table, "keys(\"MixedCaseMap\")")
        createAndDropIndexWithQuotedColumnIdentifier(cql, test_keyspace, table, "full(\"lower_case_frozen_list\")")
        createAndDropIndexWithQuotedColumnIdentifier(cql, test_keyspace, table, "full(\"UPPER_CASE_FROZEN_LIST\")")
        createAndDropIndexWithQuotedColumnIdentifier(cql, test_keyspace, table, "values(\"set name with spaces\")")
        createAndDropIndexWithQuotedColumnIdentifier(cql, test_keyspace, table, "\"column_name_with\"\"escaped quote\"")

TOO_BIG = 1024 * 65

# CASSANDRA-8280/8081
# reject updates with indexed values where value > 64k
# make sure we check conditional and unconditional statements,
# both singly and in batches (CASSANDRA-10536)
# Reproduces #8627
@pytest.mark.xfail(reason="issue #8627")
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18066")]), "vnodes"],
                         indirect=True)
def testIndexOnCompositeValueOver64k(cql, test_keyspace):
    too_big = bytearray([1])*TOO_BIG
    with create_table(cql, test_keyspace, "(a int, b int, c blob, PRIMARY KEY (a))") as table:
        execute(cql, table, "CREATE INDEX ON %s(c)")
        with pytest.raises(InvalidRequest):
            execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (0, 0, ?)", too_big)
        with pytest.raises(InvalidRequest):
            execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (0, 0, ?) IF NOT EXISTS", too_big)
        with pytest.raises(InvalidRequest):
            execute(cql, table, "BEGIN BATCH\n" +
                   "INSERT INTO %s (a, b, c) VALUES (0, 0, ?);\n" +
                   "APPLY BATCH",
                   too_big)
        with pytest.raises(InvalidRequest):
            execute(cql, table, "BEGIN BATCH\n" +
                   "INSERT INTO %s (a, b, c) VALUES (0, 0, ?) IF NOT EXISTS;\n" +
                   "APPLY BATCH",
                   too_big)

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18066")]), "vnodes"],
                         indirect=True)
def testIndexOnPartitionKeyInsertValueOver64k(cql, test_keyspace):
    too_big = bytearray([1])*TOO_BIG
    with create_table(cql, test_keyspace, "(a int, b int, c blob, PRIMARY KEY ((a, b)))") as table:
        execute(cql, table, "CREATE INDEX ON %s(a)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (0, 0, ?) IF NOT EXISTS", too_big)
        flush(cql, table)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (0, 0, ?)", too_big)
        flush(cql, table)
        execute(cql, table, "BEGIN BATCH\n" +
                      "INSERT INTO %s (a, b, c) VALUES (0, 0, ?);\n" +
                      "APPLY BATCH", too_big)
        flush(cql, table)
        #I did not translate the following part of the test, it's too messy
        #to convert to what we can do through a CQL driver.
        #// the indexed value passes validation, but the batch size will
        #// exceed the default failure threshold, so temporarily raise it
        #// (the non-conditional batch doesn't hit this because
        #// BatchStatement::executeLocally skips the size check but CAS
        #// path does not)
        #long batchSizeThreshold = DatabaseDescriptor.getBatchSizeFailThreshold();
        #try
        #{
        #    DatabaseDescriptor.setBatchSizeFailThresholdInKB( (TOO_BIG / 1024) * 2);
        #    succeedInsert("BEGIN BATCH\n" +
        #                  "INSERT INTO %s (a, b, c) VALUES (1, 1, ?) IF NOT EXISTS;\n" +
        #                  "APPLY BATCH", ByteBuffer.allocate(TOO_BIG));
        #}
        #finally
        #{
        #    DatabaseDescriptor.setBatchSizeFailThresholdInKB((int) (batchSizeThreshold / 1024));
        #}

# Reproduces issue #8708:
@pytest.mark.xfail(reason="issue #8708")
def testIndexOnPartitionKeyWithStaticColumnAndNoRows(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk1 int, pk2 int, c int, s int static, v int, PRIMARY KEY((pk1, pk2), c))") as table:
        execute(cql, table, "CREATE INDEX ON %s (pk2)")
        execute(cql, table, "INSERT INTO %s (pk1, pk2, c, s, v) VALUES (?, ?, ?, ?, ?)", 1, 1, 1, 9, 1)
        execute(cql, table, "INSERT INTO %s (pk1, pk2, c, s, v) VALUES (?, ?, ?, ?, ?)", 1, 1, 2, 9, 2)
        execute(cql, table, "INSERT INTO %s (pk1, pk2, s) VALUES (?, ?, ?)", 2, 1, 9)
        execute(cql, table, "INSERT INTO %s (pk1, pk2, c, s, v) VALUES (?, ?, ?, ?, ?)", 3, 1, 1, 9, 1)

        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE pk2 = ?", 1),
                   [2, 1, None, 9, None],
                   [1, 1, 1, 9, 1],
                   [1, 1, 2, 9, 2],
                   [3, 1, 1, 9, 1])

        execute(cql, table, "UPDATE %s SET s=?, v=? WHERE pk1=? AND pk2=? AND c=?", 9, 1, 1, 10, 2)
        assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE pk2 = ?", 10), [1, 10, 2, 9, 1])

        execute(cql, table, "UPDATE %s SET s=? WHERE pk1=? AND pk2=?", 9, 1, 20)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk2 = ?", 20), [1, 20, None, 9, None])

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18066")]), "vnodes"],
                         indirect=True)
def testIndexOnClusteringColumnInsertValueOver64k(cql, test_keyspace):
    too_big = bytearray([1])*TOO_BIG
    with create_table(cql, test_keyspace, "(a int, b int, c blob, PRIMARY KEY ((a, b)))") as table:
        execute(cql, table, "CREATE INDEX ON %s(b)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (0, 0, ?) IF NOT EXISTS", too_big)
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (0, 0, ?)", too_big)
        execute(cql, table, "BEGIN BATCH\n" +
                      "INSERT INTO %s (a, b, c) VALUES (0, 0, ?);\n" +
                      "APPLY BATCH", too_big)

        #I did not translate the following part of the test, it's too messy
        #to convert to what we can do through a CQL driver.
        #// the indexed value passes validation, but the batch size will
        #// exceed the default failure threshold, so temporarily raise it
        #// (the non-conditional batch doesn't hit this because
        #// BatchStatement::executeLocally skips the size check but CAS
        #// path does not)
        #long batchSizeThreshold = DatabaseDescriptor.getBatchSizeFailThreshold();
        #try
        #{
        #    DatabaseDescriptor.setBatchSizeFailThresholdInKB( (TOO_BIG / 1024) * 2);
        #    succeedInsert("BEGIN BATCH\n" +
        #                  "INSERT INTO %s (a, b, c) VALUES (1, 1, ?) IF NOT EXISTS;\n" +
        #                  "APPLY BATCH", ByteBuffer.allocate(TOO_BIG));
        #}
        #finally
        #{
        #    DatabaseDescriptor.setBatchSizeFailThresholdInKB((int)(batchSizeThreshold / 1024));
        #}

# Reproduces #8627
@pytest.mark.xfail(reason="issue #8627")
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18066")]), "vnodes"],
                         indirect=True)
def testIndexOnFullCollectionEntryInsertCollectionValueOver64k(cql, test_keyspace):
    too_big = bytearray([1])*TOO_BIG
    map = {0: too_big}
    with create_table(cql, test_keyspace, "(a int, b frozen<map<int, blob>>, PRIMARY KEY (a))") as table:
        execute(cql, table, "CREATE INDEX ON %s(full(b))")
        with pytest.raises(InvalidRequest):
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (0, ?)", map)
        with pytest.raises(InvalidRequest):
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (0, ?) IF NOT EXISTS", map)
        with pytest.raises(InvalidRequest):
            execute(cql, table, "BEGIN BATCH\n" +
                   "INSERT INTO %s (a, b) VALUES (0, ?);\n" +
                   "APPLY BATCH", map)
        with pytest.raises(InvalidRequest):
            execute(cql, table, "BEGIN BATCH\n" +
                   "INSERT INTO %s (a, b) VALUES (0, ?) IF NOT EXISTS;\n" +
                   "APPLY BATCH", map)

@pytest.mark.xfail(reason="issue #2203")
def testPrepareStatementsWithLIKEClauses(cql, test_keyspace):
    SASI = 'org.apache.cassandra.index.sasi.SASIIndex'
    with create_table(cql, test_keyspace, "(a int, c1 text, c2 text, v1 text, v2 text, v3 int, PRIMARY KEY (a, c1, c2))") as table:
        execute(cql, table, "CREATE CUSTOM INDEX c1_idx on %s(c1) USING '"+SASI+"' WITH OPTIONS = {'mode' : 'PREFIX'}")
        execute(cql, table, "CREATE CUSTOM INDEX c2_idx on %s(c2) USING '"+SASI+"' WITH OPTIONS = {'mode' : 'CONTAINS'}")
        execute(cql, table, "CREATE CUSTOM INDEX v1_idx on %s(v1) USING '"+SASI+"' WITH OPTIONS = {'mode' : 'PREFIX'}")
        execute(cql, table, "CREATE CUSTOM INDEX v2_idx on %s(v2) USING '"+SASI+"' WITH OPTIONS = {'mode' : 'CONTAINS'}")
        execute(cql, table, "CREATE CUSTOM INDEX v3_idx on %s(v3) USING '"+SASI+"'")

        # prefix mode indexes support prefix/contains/matches
        assert_invalid_message(cql, table, "c1 LIKE '%<term>' abc is only supported on properly indexed columns",
                             "SELECT * FROM %s WHERE c1 LIKE ?",
                             "%abc")
        assert_invalid_message(cql, table, "c1 LIKE '%<term>%' abc is only supported on properly indexed columns",
                             "SELECT * FROM %s WHERE c1 LIKE ?",
                             "%abc%")
        execute(cql, table, "SELECT * FROM %s WHERE c1 LIKE ?", "abc%")
        execute(cql, table, "SELECT * FROM %s WHERE c1 LIKE ?", "abc")
        assert_invalid_message(cql, table, "v1 LIKE '%<term>' abc is only supported on properly indexed columns",
                             "SELECT * FROM %s WHERE v1 LIKE ?",
                             "%abc")
        assert_invalid_message(cql, table, "v1 LIKE '%<term>%' abc is only supported on properly indexed columns",
                             "SELECT * FROM %s WHERE v1 LIKE ?",
                             "%abc%")
        execute(cql, table, "SELECT * FROM %s WHERE v1 LIKE ?", "abc%")
        execute(cql, table, "SELECT * FROM %s WHERE v1 LIKE ?", "abc")

        # contains mode indexes support prefix/suffix/contains/matches
        execute(cql, table, "SELECT * FROM %s WHERE c2 LIKE ?", "abc%")
        execute(cql, table, "SELECT * FROM %s WHERE c2 LIKE ?", "%abc")
        execute(cql, table, "SELECT * FROM %s WHERE c2 LIKE ?", "%abc%")
        execute(cql, table, "SELECT * FROM %s WHERE c2 LIKE ?", "abc")
        execute(cql, table, "SELECT * FROM %s WHERE v2 LIKE ?", "abc%")
        execute(cql, table, "SELECT * FROM %s WHERE v2 LIKE ?", "%abc")
        execute(cql, table, "SELECT * FROM %s WHERE v2 LIKE ?", "%abc%")
        execute(cql, table, "SELECT * FROM %s WHERE v2 LIKE ?", "abc")

        # LIKE is not supported on indexes of non-literal values
        # this is rejected before binding, so the value isn't available in the error message
        assert_invalid_message(cql, table, "LIKE restriction is only supported on properly indexed columns. v3 LIKE ? is not valid",
                             "SELECT * FROM %s WHERE v3 LIKE ?",
                             "%abc")
        assert_invalid_message(cql, table, "LIKE restriction is only supported on properly indexed columns. v3 LIKE ? is not valid",
                             "SELECT * FROM %s WHERE v3 LIKE ?",
                             "%abc%")
        assert_invalid_message(cql, table, "LIKE restriction is only supported on properly indexed columns. v3 LIKE ? is not valid",
                             "SELECT * FROM %s WHERE v3 LIKE ?",
                             "%abc%")
        assert_invalid_message(cql, table, "LIKE restriction is only supported on properly indexed columns. v3 LIKE ? is not valid",
                             "SELECT * FROM %s WHERE v3 LIKE ?",
                             "abc")

# Migrated from cql_tests.py:TestCQL.clustering_indexing_test()
def testIndexesOnClustering(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id1 int, id2 int, author text, time bigint, v1 text, v2 text, PRIMARY KEY ((id1, id2), author, time))") as table:
        execute(cql, table, "CREATE INDEX ON %s (time)")
        execute(cql, table, "CREATE INDEX ON %s (id2)")

        execute(cql, table, "INSERT INTO %s (id1, id2, author, time, v1, v2) VALUES(0, 0, 'bob', 0, 'A', 'A')")
        execute(cql, table, "INSERT INTO %s (id1, id2, author, time, v1, v2) VALUES(0, 0, 'bob', 1, 'B', 'B')")
        execute(cql, table, "INSERT INTO %s (id1, id2, author, time, v1, v2) VALUES(0, 1, 'bob', 2, 'C', 'C')")
        execute(cql, table, "INSERT INTO %s (id1, id2, author, time, v1, v2) VALUES(0, 0, 'tom', 0, 'D', 'D')")
        execute(cql, table, "INSERT INTO %s (id1, id2, author, time, v1, v2) VALUES(0, 1, 'tom', 1, 'E', 'E')")

        assert_rows(execute(cql, table, "SELECT v1 FROM %s WHERE time = 1"),
                   ["B"], ["E"])

        assert_rows(execute(cql, table, "SELECT v1 FROM %s WHERE id2 = 1"),
                   ["C"], ["E"])

        assert_rows(execute(cql, table, "SELECT v1 FROM %s WHERE id1 = 0 AND id2 = 0 AND author = 'bob' AND time = 0"),
                   ["A"])
        assert_rows(execute(cql, table, "SELECT v1 FROM %s WHERE id1 = 0 AND id2 = 0 AND author = 'bob'"),
                    ["A"], ["B"])

        # Test for CASSANDRA-8206
        execute(cql, table, "UPDATE %s SET v2 = null WHERE id1 = 0 AND id2 = 0 AND author = 'bob' AND time = 1")

        assert_rows(execute(cql, table, "SELECT v1 FROM %s WHERE id2 = 0"),
                   ["A"], ["B"], ["D"])

        assert_rows(execute(cql, table, "SELECT v1 FROM %s WHERE time = 1"),
                   ["B"], ["E"])

# See CASSANDRA-11021
def testIndexesOnNonStaticColumnsWhereSchemaIncludesStaticColumns(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int static, d int, PRIMARY KEY (a, b))") as table:
        execute(cql, table, "CREATE INDEX b_idx on %s(b)")
        execute(cql, table, "CREATE INDEX d_idx on %s(d)")

        execute(cql, table, "INSERT INTO %s (a, b, c ,d) VALUES (0, 0, 0, 0)")
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (1, 1, 1, 1)")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b = 0"), [0, 0, 0, 0])
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE d = 1"), [1, 1, 1, 1])

        execute(cql, table, "UPDATE %s SET c = 2 WHERE a = 0")
        execute(cql, table, "UPDATE %s SET c = 3, d = 4 WHERE a = 1 AND b = 1")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b = 0"), [0, 0, 2, 0])
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE d = 4"), [1, 1, 3, 4])

        execute(cql, table, "DELETE FROM %s WHERE a = 0")
        execute(cql, table, "DELETE FROM %s WHERE a = 1 AND b = 1")
        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE b = 0"))
        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE d = 3"))

# Reproduces Scylla issues #4244 (mixing single-column and multi-columns
# restriction) and #8711 (Finding or filtering with an empty string with
# a secondary index seems to be broken).
@pytest.mark.xfail(reason="issues #4244, #8711")
def testWithEmptyRestrictionValueAndSecondaryIndex(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk blob, c blob, v blob, PRIMARY KEY ((pk), c))") as table:
        execute(cql, table, "CREATE INDEX ON %s(c)")
        execute(cql, table, "CREATE INDEX ON %s(v)")

        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", b"foo123", b"1", b"1")
        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", b"foo123", b"2", b"1")

        for _ in before_and_after_flush(cql, table):
            # Test clustering columns restrictions
            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c = textAsBlob('');"))
            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) = (textAsBlob(''));"))
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c IN (textAsBlob(''), textAsBlob('1'));"),
                       [b"foo123", b"1", b"1"])
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) IN ((textAsBlob('')), (textAsBlob('1')));"),
                       [b"foo123", b"1", b"1"])
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c > textAsBlob('') AND v = textAsBlob('1') ALLOW FILTERING;"),
                       [b"foo123", b"1", b"1"],
                       [b"foo123", b"2", b"1"])
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c >= textAsBlob('') AND v = textAsBlob('1') ALLOW FILTERING;"),
                       [b"foo123", b"1", b"1"],
                       [b"foo123", b"2", b"1"])
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) >= (textAsBlob('')) AND v = textAsBlob('1') ALLOW FILTERING;"),
                       [b"foo123", b"1", b"1"],
                       [b"foo123", b"2", b"1"])
            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c <= textAsBlob('') AND v = textAsBlob('1') ALLOW FILTERING;"))
            # The following two requests reproduce #8711:
            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) <= (textAsBlob('')) AND v = textAsBlob('1') ALLOW FILTERING;"))
            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) < (textAsBlob('')) AND v = textAsBlob('1') ALLOW FILTERING;"))

            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c < textAsBlob('') AND v = textAsBlob('1') ALLOW FILTERING;"))
            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c > textAsBlob('') AND c < textAsBlob('') AND v = textAsBlob('1') ALLOW FILTERING;"))
            # The following reproduces #8711:
            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) > (textAsBlob('')) AND (c) < (textAsBlob('')) AND v = textAsBlob('1') ALLOW FILTERING;"))

        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                b"foo123", b"", b"1")

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c = textAsBlob('');"),
                       [b"foo123", b"", b"1"])
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) = (textAsBlob(''));"),
                       [b"foo123", b"", b"1"])
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c IN (textAsBlob(''), textAsBlob('1'));"),
                       [b"foo123", b"", b"1"],
                       [b"foo123", b"1", b"1"])
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) IN ((textAsBlob('')), (textAsBlob('1')));"),
                       [b"foo123", b"", b"1"],
                       [b"foo123", b"1", b"1"])
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c > textAsBlob('') AND v = textAsBlob('1') ALLOW FILTERING;"),
                       [b"foo123", b"1", b"1"],
                       [b"foo123", b"2", b"1"])
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c >= textAsBlob('') AND v = textAsBlob('1') ALLOW FILTERING;"),
                       [b"foo123", b"", b"1"],
                       [b"foo123", b"1", b"1"],
                       [b"foo123", b"2", b"1"])
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) >= (textAsBlob('')) AND v = textAsBlob('1') ALLOW FILTERING;"),
                       [b"foo123", b"", b"1"],
                       [b"foo123", b"1", b"1"],
                       [b"foo123", b"2", b"1"])
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c <= textAsBlob('') AND v = textAsBlob('1') ALLOW FILTERING;"),
                       [b"foo123", b"", b"1"])
            # The following reproduces #8711:
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) <= (textAsBlob('')) AND v = textAsBlob('1') ALLOW FILTERING;"),
                       [b"foo123", b"", b"1"])

            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c < textAsBlob('') AND v = textAsBlob('1') ALLOW FILTERING;"))

            # The following reproduces #8711:
            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) < (textAsBlob('')) AND v = textAsBlob('1') ALLOW FILTERING;"))

            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND c >= textAsBlob('') AND c < textAsBlob('') AND v = textAsBlob('1') ALLOW FILTERING;"))
            # The following reproduce #4244 (mixing single-column and multi-columns restriction).
            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND (c) >= (textAsBlob('')) AND c < textAsBlob('') AND v = textAsBlob('1') ALLOW FILTERING;"))

            # Test restrictions on non-primary key value
            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND v = textAsBlob('');"))

        execute(cql, table, "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                b"foo123", b"3", b"")

        for _ in before_and_after_flush(cql, table):
            # The following reproduces #8711:
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = textAsBlob('foo123') AND v = textAsBlob('');"),
                       [b"foo123", b"3", b""])

def testPartitionKeyWithIndex(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b int, c int, PRIMARY KEY ((a, b)))") as table:
        execute(cql, table, "CREATE INDEX ON %s(a)")
        execute(cql, table, "CREATE INDEX ON %s(b)")

        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (1,2,3)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (2,3,4)")
        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (5,6,7)")

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 1"),
                       [1, 2, 3])
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE b = 3"),
                       [2, 3, 4])
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=5 AND b=6"),
                       [5, 6, 7])
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a=5 AND b=5"))

def testAllowFilteringOnPartitionKeyWithSecondaryIndex(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk1 int, pk2 int, c1 int, c2 int, v int, PRIMARY KEY ((pk1, pk2), c1, c2))") as table:
        execute(cql, table, "CREATE INDEX v_idx_1 ON %s (v)")

        for i in range(1, 5+1):
            for j in range(1, 2+1):
                execute(cql, table, "INSERT INTO %s (pk1, pk2, c1, c2, v) VALUES (?, ?, ?, ?, ?)", j, 1, 1, 1, i)
                execute(cql, table, "INSERT INTO %s (pk1, pk2, c1, c2, v) VALUES (?, ?, ?, ?, ?)", j, 1, 1, i, i)
                execute(cql, table, "INSERT INTO %s (pk1, pk2, c1, c2, v) VALUES (?, ?, ?, ?, ?)", j, 1, i, i, i)
                execute(cql, table, "INSERT INTO %s (pk1, pk2, c1, c2, v) VALUES (?, ?, ?, ?, ?)", j, i, i, i, i)

        for _ in before_and_after_flush(cql, table):
            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk1 = 1 AND  c1 > 0 AND c1 < 5 AND c2 = 1 AND v = 3 ALLOW FILTERING;"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk1 = 1 AND  c1 > 0 AND c1 < 5 AND c2 = 3 AND v = 3 ALLOW FILTERING;"),
                       [1, 3, 3, 3, 3],
                       [1, 1, 1, 3, 3],
                       [1, 1, 3, 3, 3])

            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE pk1 = 1 AND  c2 > 1 AND c2 < 5 AND v = 1 ALLOW FILTERING;"))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk1 = 1 AND  c1 > 1 AND c2 > 2 AND v = 3 ALLOW FILTERING;"),
                       [1, 3, 3, 3, 3],
                       [1, 1, 3, 3, 3])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk1 = 1 AND  pk2 > 1 AND c2 > 2 AND v = 3 ALLOW FILTERING;"),
                       [1, 3, 3, 3, 3])

            assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE pk2 > 1 AND  c1 IN(0,1,2) AND v <= 3 ALLOW FILTERING;"),
                                    [1, 2, 2, 2, 2],
                                    [2, 2, 2, 2, 2])

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk1 >= 2 AND pk2 <=3 AND  c1 IN(0,1,2) AND c2 IN(0,1,2) AND v < 3  ALLOW FILTERING;"),
                       [2, 2, 2, 2, 2],
                       [2, 1, 1, 2, 2],
                       [2, 1, 2, 2, 2])

            # Issue #8714: Scylla and Cassandra differ on the specific error
            # message in this case: Scylla complains that inequality is not
            # allowed on partition keys, while Cassandra says that it would
            # have been allowed with ALLOW FILTERING. Arguably, the Cassandra
            # error message is more useful.
            #assert_invalid_message(cql, table, "ALLOW FILTERING",
            assert_invalid(cql, table,
                 "SELECT * FROM %s WHERE pk1 >= 1 AND pk2 <=3 AND  c1 IN(0,1,2) AND c2 IN(0,1,2) AND v = 3")

def testAllowFilteringOnPartitionKeyWithIndexForContains(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k1 int, k2 int, v set<int>, PRIMARY KEY ((k1, k2)))") as table:
        execute(cql, table, "CREATE INDEX ON %s (k2)")
        execute(cql, table, "INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 0, 0, {1, 2, 3})
        execute(cql, table, "INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 0, 1, {2, 3, 4})
        execute(cql, table, "INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 1, 0, {3, 4, 5})
        execute(cql, table, "INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 1, 1, {4, 5, 6})

        for _ in before_and_after_flush(cql, table):
            # Issue #8714: Scylla and Cassandra differ on the specific error
            # message in this case: Scylla complains that inequality is not
            # allowed on partition keys, while Cassandra says that it would
            # have been allowed with ALLOW FILTERING. Arguably, the Cassandra
            # error message is more useful.
            #assert_invalid_message(cql, table, "ALLOW FILTERING",
            assert_invalid(cql, table,
                                 "SELECT * FROM %s WHERE k2 > ?", 0)
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k2 > ? ALLOW FILTERING", 0),
                       [0, 1, {2, 3, 4}],
                       [1, 1, {4, 5, 6}])
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k2 >= ? AND v CONTAINS ? ALLOW FILTERING", 1, 6),
                       [1, 1, {4, 5, 6}])
            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE k2 < ? AND v CONTAINS ? ALLOW FILTERING", 0, 7))

def testIndexOnStaticColumnWithPartitionWithoutRows(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int, c int, s int static, v int, PRIMARY KEY (pk, c))") as table:
        execute(cql, table, "CREATE INDEX ON %s (s)")

        execute(cql, table,"INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?)", 1, 1, 9, 1)
        execute(cql, table, "INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?)", 1, 2, 9, 2)
        execute(cql, table, "INSERT INTO %s (pk, s) VALUES (?, ?)", 2, 9)
        execute(cql, table, "INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?)", 3, 1, 9, 1)
        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE s = ?", 9),
                   [1, 1, 9, 1],
                   [1, 2, 9, 2],
                   [2, None, 9, None],
                   [3, 1, 9, 1])

        execute(cql, table, "DELETE FROM %s WHERE pk = ?", 3)

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE s = ?", 9),
                   [1, 1, 9, 1],
                   [1, 2, 9, 2],
                   [2, None, 9, None])

def testIndexOnRegularColumnWithPartitionWithoutRows(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int, c int, s int static, v int, PRIMARY KEY (pk, c))") as table:
        execute(cql, table, "CREATE INDEX ON %s (v)")

        execute(cql, table, "INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?)", 1, 1, 9, 1)
        execute(cql, table, "INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?)", 1, 2, 9, 2)
        execute(cql, table, "INSERT INTO %s (pk, s) VALUES (?, ?)", 2, 9)
        execute(cql, table, "INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?)", 3, 1, 9, 1)
        flush(cql, table)

        execute(cql, table, "DELETE FROM %s WHERE pk = ? and c = ?", 3, 1)

        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE v = ?", 1),
                   [1, 1, 9, 1])

def testIndexOnDurationColumn(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, d duration)") as table:
        assert_invalid_message(cql, table, "Secondary indexes are not supported on duration columns",
                             "CREATE INDEX ON %s (d)")
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, l list<duration>)") as table:
        assert_invalid_message(cql, table, "Secondary indexes are not supported on collections containing durations",
                             "CREATE INDEX ON %s (l)")

    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, m map<int, duration>)") as table:
        assert_invalid_message(cql, table, "Secondary indexes are not supported on collections containing durations",
                             "CREATE INDEX ON %s (m)")

    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, t tuple<int, duration>)") as table:
        assert_invalid_message(cql, table, "Secondary indexes are not supported on tuples containing durations",
                             "CREATE INDEX ON %s (t)")

    with create_type(cql, test_keyspace, "(i int, d duration)") as udt:
        with create_table(cql, test_keyspace, "(k int PRIMARY KEY, t " + udt +")") as table:
            # This error message reproduces #8724:
            assert_invalid_message(cql, table, "Secondary indexes are not supported on UDTs containing durations",
                             "CREATE INDEX ON %s (t)")

def testIndexOnFrozenUDT(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(a int)") as t:
        with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, v frozen<{t}>)") as table:
            a_tuple = collections.namedtuple('a_tuple', ['a'])
            udt1 = a_tuple(1)
            udt2 = a_tuple(2)

            execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", 0, udt1)
            execute(cql, table, "CREATE INDEX ON %s (v)")

            execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", 1, udt2)
            execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", 1, udt1)
            index_name = table.split('.')[1] + "_v_idx"
            assert(wait_for_index(cql, table, index_name))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE v = ?", udt1), [1, udt1], [0, udt1])
            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE v = ?", udt2))

            execute(cql, table, "DELETE FROM %s WHERE k = 0")
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE v = ?", udt1), [1, udt1])

            execute(cql, table, f"DROP INDEX {test_keyspace}.{index_name}")
            assert_invalid_message(cql, table, index_name,
                    f"DROP INDEX {test_keyspace}.{index_name}")
            assert_invalid_message(cql, table, "ALLOW FILTERING",
                             "SELECT * FROM %s WHERE v = ?", udt1)

def testIndexOnFrozenCollectionOfUDT(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(a int)") as t:
        with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, v frozen<set<frozen<{t}>>>)") as table:
            a_tuple = collections.namedtuple('a_tuple', ['a'])
            udt1 = a_tuple(1)
            udt2 = a_tuple(2)
            execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", 1, {udt1, udt2})
            # The error messages printed by Cassandra and Scylla in this case
            # are different. Cassandra reports "Frozen collections are
            # immutable and must be fully indexed" while Scylla reports
            # "Cannot create index on index_keys of frozen collection column v".
            # (we used to wrongly say frozen<map> here, see issue #8744)
            assert_invalid(cql, table, "CREATE INDEX idx ON %s (keys(v))")
            # Reproduces #8745:
            assert_invalid(cql, table, "CREATE INDEX idx ON %s (values(v))")
            index_name = unique_name()
            execute(cql, table, f"CREATE INDEX {index_name} ON %s (full(v))")

            execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", 2, {udt2})
            assert(wait_for_index(cql, table, index_name))

            assert_invalid_message(cql, table, "ALLOW FILTERING",
                             "SELECT * FROM %s WHERE v CONTAINS ?", udt1)

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE v = ?", {udt1, udt2}), [1, {udt1, udt2}])
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE v = ?", {udt2}), [2, {udt2}])

            execute(cql, table, "DELETE FROM %s WHERE k = 2")
            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE v = ?", {udt2}))

            execute(cql, table, f"DROP INDEX {test_keyspace}.{index_name}")
            assert_invalid(cql, table, f"DROP INDEX {test_keyspace}.{index_name}")
            assert_invalid_message(cql, table, "ALLOW FILTERING",
                             "SELECT * FROM %s WHERE v CONTAINS ?", udt1)

def testIndexOnNonFrozenCollectionOfFrozenUDT(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(a int)") as t:
        with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, v set<frozen<{t}>>)") as table:
            a_tuple = collections.namedtuple('a_tuple', ['a'])
            udt1 = a_tuple(1)
            udt2 = a_tuple(2)
            execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", 1, {udt1})

            assert_invalid_message_re(cql, table, "Cannot create (secondary )?index on keys of column v with non-map type", "CREATE INDEX ON %s (keys(v))")
            assert_invalid_message(cql, table, "full() indexes can only be created on frozen collections", "CREATE INDEX ON %s (full(v))")
            index_name = unique_name()
            # Reproduces #8745:
            execute(cql, table, f"CREATE INDEX {index_name} ON %s (values(v))")
            execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", 2, {udt2})
            execute(cql, table, "UPDATE %s SET v = v + ? WHERE k = ?", {udt2}, 1)
            assert(wait_for_index(cql, table, index_name))

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE v CONTAINS ?", udt1), [1, {udt1, udt2}])
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE v CONTAINS ?", udt2), [1, {udt1, udt2}], [2, {udt2}])

            execute(cql, table, "DELETE FROM %s WHERE k = 1")
            assert_empty(execute(cql, table, "SELECT * FROM %s WHERE v CONTAINS ?", udt1))
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE v CONTAINS ?", udt2), [2, {udt2}])

            execute(cql, table, f"DROP INDEX {test_keyspace}.{index_name}")

            scylla_message = re.escape(f"Index '{index_name}' could not be found in any of the tables of keyspace '{test_keyspace}'")
            cassandra_message = re.escape(f"Index '{test_keyspace}.{index_name}' doesn't exist")
            assert_invalid_message_re(cql, table, f'({scylla_message}|{cassandra_message})', f"DROP INDEX {test_keyspace}.{index_name}")

            assert_invalid_message(cql, table, "ALLOW FILTERING",
                             "SELECT * FROM %s WHERE v CONTAINS ?", udt1)

@pytest.mark.xfail(reason="#8745")
def testIndexOnNonFrozenUDT(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(a int)") as t:
        with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, v {t})") as table:
            assert_invalid_message(cql, table, "index on non-frozen", "CREATE INDEX ON %s (v)")
            # Cassandra and Scylla focus on different parts of the error
            # below - Cassandra complains that a because this is not a
            # collection keys()/values() cannot be supported, while Scylla
            # complains that this is an unfrozen UDT so it cannot be indexed.
            assert_invalid(cql, table, "CREATE INDEX ON %s (keys(v))")
            # Reproduces #8745:
            assert_invalid(cql, table, "CREATE INDEX ON %s (values(v))")
            assert_invalid(cql, table, "CREATE INDEX ON %s (full(v))")

# TODO: The following tests unfortunately take 1-2 second each. It would
# have been nice to provide a REST API to move the server's clock forward -
# like we do in C++ unit tests. Alternatively, could we have a sub-second
# TTL feature?
def testIndexOnPartitionKeyInsertExpiringColumn(cql, test_keyspace):
    with create_table(cql, test_keyspace, f"(k1 int, k2 int, a int, b int, PRIMARY KEY ((k1, k2)))") as table:
        execute(cql, table, "CREATE INDEX on %s(k1)")
        execute(cql, table, "INSERT INTO %s (k1, k2, a, b) VALUES (1, 2, 3, 4)")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k1 = 1"), [1, 2, 3, 4])
        execute(cql, table, "UPDATE %s USING TTL 1 SET b = 10 WHERE k1 = 1 AND k2 = 2")
        time.sleep(1.1)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k1 = 1"), [1, 2, 3, None])

def testIndexOnClusteringKeyInsertExpiringColumn(cql, test_keyspace):
    with create_table(cql, test_keyspace, f"(pk int, ck int, a int, b int, PRIMARY KEY (pk, ck))") as table:
        execute(cql, table, "CREATE INDEX on %s(ck)")
        execute(cql, table, "INSERT INTO %s (pk, ck, a, b) VALUES (1, 2, 3, 4)")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE ck = 2"), [1, 2, 3, 4])
        execute(cql, table, "UPDATE %s USING TTL 1 SET b = 10 WHERE pk = 1 AND ck = 2")
        time.sleep(1.1)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE ck = 2"), [1, 2, 3, None])

def testIndexOnRegularColumnInsertExpiringColumn(cql, test_keyspace):
    with create_table(cql, test_keyspace, f"(pk int, ck int, a int, b int, PRIMARY KEY (pk, ck))") as table:
        execute(cql, table, "CREATE INDEX on %s(a)")
        execute(cql, table, "INSERT INTO %s (pk, ck, a, b) VALUES (1, 2, 3, 4)")
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 3"), [1, 2, 3, 4])

        execute(cql, table, "UPDATE %s USING TTL 1 SET b = 10 WHERE pk = 1 AND ck = 2")
        time.sleep(1.1)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 3"), [1, 2, 3, None])

        execute(cql, table, "UPDATE %s USING TTL 1 SET a = 5 WHERE pk = 1 AND ck = 2")
        time.sleep(1.1)
        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE a = 3"))
        assert_empty(execute(cql, table, "SELECT * FROM %s WHERE a = 5"))
