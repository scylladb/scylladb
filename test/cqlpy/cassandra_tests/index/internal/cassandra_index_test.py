# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# This is a translation of CassandraIndexTest.java from Cassandra's
# test/unit/org/apache/cassandra/index/internal directory:
# Smoke tests of built-in secondary index implementations

from ...porting import *
from cassandra.protocol import InvalidRequest

REQUIRES_ALLOW_FILTERING_MESSAGE = "ALLOW FILTERING"

# Cassandra's createIndex() waits until the new index is built (and in
# Scylla, too, an index is built asynchronously). This function runs the
# given CREATE INDEX statement, and waits until all the table's indexes
# are built.
def create_index(cql, table, statement):
    execute(cql, table, statement)
    keyspace, table_name = table.split('.')
    for r in cql.execute("SELECT index_name FROM system_schema.indexes WHERE keyspace_name = %s AND table_name = %s",
                         (keyspace, table_name)):
        assert wait_for_index(cql, table, r.index_name)

def testindexOnRegularColumn(cql, test_keyspace):
    run_test_script(cql, test_keyspace,
                    tableDefinition="(k int, c int, v int, PRIMARY KEY (k, c))",
                    target="v",
                    firstRow=row(0, 0, 0),
                    secondRow=row(1, 1, 1),
                    missingIndexMessage=REQUIRES_ALLOW_FILTERING_MESSAGE,
                    firstQueryExpression="v=0",
                    secondQueryExpression="v=1",
                    updateExpression="SET v=2",
                    postUpdateQueryExpression="v=2")

# Reproduces #8708: a secondary index misses partitions which have only a
# static row.
@pytest.mark.xfail(reason="#8708")
def testIndexOnPartitionKeyWithPartitionWithoutRows(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk1 int, pk2 int, c int, s int static, v int, PRIMARY KEY((pk1, pk2), c))") as table:
        create_index(cql, table, "CREATE INDEX ON %s (pk2)")

        execute(cql, table, "INSERT INTO %s (pk1, pk2, c, s, v) VALUES (?, ?, ?, ?, ?)", 1, 1, 1, 9, 1)
        execute(cql, table, "INSERT INTO %s (pk1, pk2, c, s, v) VALUES (?, ?, ?, ?, ?)", 1, 1, 2, 9, 2)
        execute(cql, table, "INSERT INTO %s (pk1, pk2, c, s, v) VALUES (?, ?, ?, ?, ?)", 3, 1, 1, 9, 1)
        execute(cql, table, "INSERT INTO %s (pk1, pk2, c, s, v) VALUES (?, ?, ?, ?, ?)", 4, 1, 1, 9, 1)
        flush(cql, table)

        assertRowsIgnoringOrder(execute(cql, table, "SELECT * FROM %s WHERE pk2 = ?", 1),
                                row(1, 1, 1, 9, 1),
                                row(1, 1, 2, 9, 2),
                                row(3, 1, 1, 9, 1),
                                row(4, 1, 1, 9, 1))

        execute(cql, table, "DELETE FROM %s WHERE pk1 = ? AND pk2 = ? AND c = ?", 3, 1, 1)

        assertRowsIgnoringOrder(execute(cql, table, "SELECT * FROM %s WHERE pk2 = ?", 1),
                                row(1, 1, 1, 9, 1),
                                row(1, 1, 2, 9, 2),
                                row(3, 1, None, 9, None),
                                row(4, 1, 1, 9, 1))

def testindexOnFirstClusteringColumn(cql, test_keyspace):
    # No update allowed on primary key columns, so this script has no update expression
    run_test_script(cql, test_keyspace,
                    tableDefinition="(k int, c int, v int, PRIMARY KEY (k, c))",
                    target="c",
                    firstRow=row(0, 0, 0),
                    secondRow=row(1, 1, 1),
                    missingIndexMessage=REQUIRES_ALLOW_FILTERING_MESSAGE,
                    firstQueryExpression="c=0",
                    secondQueryExpression="c=1")

def testindexOnSecondClusteringColumn(cql, test_keyspace):
    # No update allowed on primary key columns, so this script has no update expression
    run_test_script(cql, test_keyspace,
                    tableDefinition="(k int, c1 int, c2 int, v int, PRIMARY KEY (k, c1, c2))",
                    target="c2",
                    firstRow=row(0, 0, 0, 0),
                    secondRow=row(1, 1, 1, 1),
                    missingIndexMessage="PRIMARY KEY column \"%s\" cannot be restricted as preceding column \"%s\" is not restricted" % ("c2", "c1"),
                    firstQueryExpression="c2=0",
                    secondQueryExpression="c2=1")

def testindexOnFirstPartitionKeyColumn(cql, test_keyspace):
    # No update allowed on primary key columns, so this script has no update expression
    run_test_script(cql, test_keyspace,
                    tableDefinition="(k1 int, k2 int, c1 int, c2 int, v int, PRIMARY KEY ((k1, k2), c1, c2))",
                    target="k1",
                    firstRow=row(0, 0, 0, 0, 0),
                    secondRow=row(1, 1, 1, 1, 1),
                    missingIndexMessage=REQUIRES_ALLOW_FILTERING_MESSAGE,
                    firstQueryExpression="k1=0",
                    secondQueryExpression="k1=1")

def testindexOnSecondPartitionKeyColumn(cql, test_keyspace):
    # No update allowed on primary key columns, so this script has no update expression
    run_test_script(cql, test_keyspace,
                    tableDefinition="(k1 int, k2 int, c1 int, c2 int, v int, PRIMARY KEY ((k1, k2), c1, c2))",
                    target="k2",
                    firstRow=row(0, 0, 0, 0, 0),
                    secondRow=row(1, 1, 1, 1, 1),
                    missingIndexMessage=REQUIRES_ALLOW_FILTERING_MESSAGE,
                    firstQueryExpression="k2=0",
                    secondQueryExpression="k2=1")

def testindexOnNonFrozenListWithReplaceOperation(cql, test_keyspace):
    run_test_script(cql, test_keyspace,
                    tableDefinition="(k int, c int, l list<int>, PRIMARY KEY (k, c))",
                    target="l",
                    firstRow=row(0, 0, [10, 20, 30]),
                    secondRow=row(1, 1, [11, 21, 31]),
                    missingIndexMessage=REQUIRES_ALLOW_FILTERING_MESSAGE,
                    firstQueryExpression="l CONTAINS 10",
                    secondQueryExpression="l CONTAINS 11",
                    updateExpression="SET l = [40, 50, 60]",
                    postUpdateQueryExpression="l CONTAINS 40")

def testindexOnNonFrozenListWithInPlaceOperation(cql, test_keyspace):
    run_test_script(cql, test_keyspace,
                    tableDefinition="(k int, c int, l list<int>, PRIMARY KEY (k, c))",
                    target="l",
                    firstRow=row(0, 0, [10, 20, 30]),
                    secondRow=row(1, 1, [11, 21, 31]),
                    missingIndexMessage=REQUIRES_ALLOW_FILTERING_MESSAGE,
                    firstQueryExpression="l CONTAINS 10",
                    secondQueryExpression="l CONTAINS 11",
                    updateExpression="SET l = l - [10]",
                    postUpdateQueryExpression="l CONTAINS 20")

def testindexOnNonFrozenSetWithReplaceOperation(cql, test_keyspace):
    run_test_script(cql, test_keyspace,
                    tableDefinition="(k int, c int, s set<int>, PRIMARY KEY (k, c))",
                    target="s",
                    firstRow=row(0, 0, {10, 20, 30}),
                    secondRow=row(1, 1, {11, 21, 31}),
                    missingIndexMessage=REQUIRES_ALLOW_FILTERING_MESSAGE,
                    firstQueryExpression="s CONTAINS 10",
                    secondQueryExpression="s CONTAINS 11",
                    updateExpression="SET s = {40, 50, 60}",
                    postUpdateQueryExpression="s CONTAINS 40")

def testindexOnNonFrozenSetWithInPlaceOperation(cql, test_keyspace):
    run_test_script(cql, test_keyspace,
                    tableDefinition="(k int, c int, s set<int>, PRIMARY KEY (k, c))",
                    target="s",
                    firstRow=row(0, 0, {10, 20, 30}),
                    secondRow=row(1, 1, {11, 21, 31}),
                    missingIndexMessage=REQUIRES_ALLOW_FILTERING_MESSAGE,
                    firstQueryExpression="s CONTAINS 10",
                    secondQueryExpression="s CONTAINS 11",
                    updateExpression="SET s = s - {10}",
                    postUpdateQueryExpression="s CONTAINS 20")

def testindexOnNonFrozenMapValuesWithReplaceOperation(cql, test_keyspace):
    run_test_script(cql, test_keyspace,
                    tableDefinition="(k int, c int, m map<text,int>, PRIMARY KEY (k, c))",
                    target="m",
                    firstRow=row(0, 0, {"a": 10, "b": 20, "c": 30}),
                    secondRow=row(1, 1, {"d": 11, "e": 21, "f": 31}),
                    missingIndexMessage=REQUIRES_ALLOW_FILTERING_MESSAGE,
                    firstQueryExpression="m CONTAINS 10",
                    secondQueryExpression="m CONTAINS 11",
                    updateExpression="SET m = {'x':40, 'y':50, 'z':60}",
                    postUpdateQueryExpression="m CONTAINS 40")

def testindexOnNonFrozenMapValuesWithInPlaceOperation(cql, test_keyspace):
    run_test_script(cql, test_keyspace,
                    tableDefinition="(k int, c int, m map<text,int>, PRIMARY KEY (k, c))",
                    target="m",
                    firstRow=row(0, 0, {"a": 10, "b": 20, "c": 30}),
                    secondRow=row(1, 1, {"d": 11, "e": 21, "f": 31}),
                    missingIndexMessage=REQUIRES_ALLOW_FILTERING_MESSAGE,
                    firstQueryExpression="m CONTAINS 10",
                    secondQueryExpression="m CONTAINS 11",
                    updateExpression="SET m['a'] = 40",
                    postUpdateQueryExpression="m CONTAINS 40")

def testindexOnNonFrozenMapKeysWithReplaceOperation(cql, test_keyspace):
    run_test_script(cql, test_keyspace,
                    tableDefinition="(k int, c int, m map<text,int>, PRIMARY KEY (k, c))",
                    target="keys(m)",
                    firstRow=row(0, 0, {"a": 10, "b": 20, "c": 30}),
                    secondRow=row(1, 1, {"d": 11, "e": 21, "f": 31}),
                    missingIndexMessage=REQUIRES_ALLOW_FILTERING_MESSAGE,
                    firstQueryExpression="m CONTAINS KEY 'a'",
                    secondQueryExpression="m CONTAINS KEY 'd'",
                    updateExpression="SET m = {'x':40, 'y':50, 'z':60}",
                    postUpdateQueryExpression="m CONTAINS KEY 'x'")

def testindexOnNonFrozenMapKeysWithInPlaceOperation(cql, test_keyspace):
    run_test_script(cql, test_keyspace,
                    tableDefinition="(k int, c int, m map<text,int>, PRIMARY KEY (k, c))",
                    target="keys(m)",
                    firstRow=row(0, 0, {"a": 10, "b": 20, "c": 30}),
                    secondRow=row(1, 1, {"d": 11, "e": 21, "f": 31}),
                    missingIndexMessage=REQUIRES_ALLOW_FILTERING_MESSAGE,
                    firstQueryExpression="m CONTAINS KEY 'a'",
                    secondQueryExpression="m CONTAINS KEY 'd'",
                    updateExpression="SET m['a'] = NULL",
                    postUpdateQueryExpression="m CONTAINS KEY 'b'")

def testindexOnNonFrozenMapEntriesWithReplaceOperation(cql, test_keyspace):
    run_test_script(cql, test_keyspace,
                    tableDefinition="(k int, c int, m map<text,int>, PRIMARY KEY (k, c))",
                    target="entries(m)",
                    firstRow=row(0, 0, {"a": 10, "b": 20, "c": 30}),
                    secondRow=row(1, 1, {"d": 11, "e": 21, "f": 31}),
                    missingIndexMessage=REQUIRES_ALLOW_FILTERING_MESSAGE,
                    firstQueryExpression="m['a'] = 10",
                    secondQueryExpression="m['d'] = 11",
                    updateExpression="SET m = {'x':40, 'y':50, 'z':60}",
                    postUpdateQueryExpression="m['x'] = 40")

def testindexOnNonFrozenMapEntriesWithInPlaceOperation(cql, test_keyspace):
    run_test_script(cql, test_keyspace,
                    tableDefinition="(k int, c int, m map<text,int>, PRIMARY KEY (k, c))",
                    target="entries(m)",
                    firstRow=row(0, 0, {"a": 10, "b": 20, "c": 30}),
                    secondRow=row(1, 1, {"d": 11, "e": 21, "f": 31}),
                    missingIndexMessage=REQUIRES_ALLOW_FILTERING_MESSAGE,
                    firstQueryExpression="m['a'] = 10",
                    secondQueryExpression="m['d'] = 11",
                    updateExpression="SET m['a'] = 40",
                    postUpdateQueryExpression="m['a'] = 40")

def testindexOnFrozenList(cql, test_keyspace):
    run_test_script(cql, test_keyspace,
                    tableDefinition="(k int, c int, l frozen<list<int>>, PRIMARY KEY (k, c))",
                    target="full(l)",
                    firstRow=row(0, 0, [10, 20, 30]),
                    secondRow=row(1, 1, [11, 21, 31]),
                    missingIndexMessage=REQUIRES_ALLOW_FILTERING_MESSAGE,
                    firstQueryExpression="l = [10, 20, 30]",
                    secondQueryExpression="l = [11, 21, 31]",
                    updateExpression="SET l = [40, 50, 60]",
                    postUpdateQueryExpression="l = [40, 50, 60]")

def testindexOnFrozenSet(cql, test_keyspace):
    run_test_script(cql, test_keyspace,
                    tableDefinition="(k int, c int, s frozen<set<int>>, PRIMARY KEY (k, c))",
                    target="full(s)",
                    firstRow=row(0, 0, {10, 20, 30}),
                    secondRow=row(1, 1, {11, 21, 31}),
                    missingIndexMessage=REQUIRES_ALLOW_FILTERING_MESSAGE,
                    firstQueryExpression="s = {10, 20, 30}",
                    secondQueryExpression="s = {11, 21, 31}",
                    updateExpression="SET s = {40, 50, 60}",
                    postUpdateQueryExpression="s = {40, 50, 60}")

def testindexOnFrozenMap(cql, test_keyspace):
    run_test_script(cql, test_keyspace,
                    tableDefinition="(k int, c int, m frozen<map<text,int>>, PRIMARY KEY (k, c))",
                    target="full(m)",
                    firstRow=row(0, 0, {"a": 10, "b": 20, "c": 30}),
                    secondRow=row(1, 1, {"d": 11, "e": 21, "f": 31}),
                    missingIndexMessage=REQUIRES_ALLOW_FILTERING_MESSAGE,
                    firstQueryExpression="m = {'a':10, 'b':20, 'c':30}",
                    secondQueryExpression="m = {'d':11, 'e':21, 'f':31}",
                    updateExpression="SET m = {'x':40, 'y':50, 'z':60}",
                    postUpdateQueryExpression="m = {'x':40, 'y':50, 'z':60}")

def testindexOnRegularColumnWithCompactStorage(cql, test_keyspace, compact_storage):
    run_test_script(cql, test_keyspace,
                    tableDefinition="(k int, v int, PRIMARY KEY (k)) WITH COMPACT STORAGE",
                    target="v",
                    firstRow=row(0, 0),
                    secondRow=row(1,1),
                    missingIndexMessage=REQUIRES_ALLOW_FILTERING_MESSAGE,
                    firstQueryExpression="v=0",
                    secondQueryExpression="v=1",
                    updateExpression="SET v=2",
                    postUpdateQueryExpression="v=2")

def testindexOnStaticColumn(cql, test_keyspace):
    row1 = row("k0", "c0", "s0")
    row2 = row("k0", "c1", "s0")
    row3 = row("k1", "c0", "s1")
    row4 = row("k1", "c1", "s1")

    with create_table(cql, test_keyspace, "(k text, c text, s text static, PRIMARY KEY (k, c))") as table:
        create_index(cql, table, "CREATE INDEX sc_index on %s(s)")

        execute(cql, table, "INSERT INTO %s (k, c, s) VALUES (?, ?, ?)", *row1)
        execute(cql, table, "INSERT INTO %s (k, c, s) VALUES (?, ?, ?)", *row2)
        execute(cql, table, "INSERT INTO %s (k, c, s) VALUES (?, ?, ?)", *row3)
        execute(cql, table, "INSERT INTO %s (k, c, s) VALUES (?, ?, ?)", *row4)

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE s = ?", "s0"), row1, row2)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE s = ?", "s1"), row3, row4)

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE s = ? AND token(k) >= token(?)", "s0", "k0"), row1, row2)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE s = ? AND token(k) >= token(?)", "s1", "k1"), row3, row4)

        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE s = ? AND token(k) < token(?)", "s0", "k0"))
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE s = ? AND token(k) < token(?)", "s1", "k1"))

        row1 = row("s0")
        row2 = row("s0")
        row3 = row("s1")
        row4 = row("s1")

        assertRows(execute(cql, table, "SELECT s FROM %s WHERE s = ?", "s0"), row1, row2)
        assertRows(execute(cql, table, "SELECT s FROM %s WHERE s = ?", "s1"), row3, row4)

        assertRows(execute(cql, table, "SELECT s FROM %s WHERE s = ? AND token(k) >= token(?)", "s0", "k0"), row1, row2)
        assertRows(execute(cql, table, "SELECT s FROM %s WHERE s = ? AND token(k) >= token(?)", "s1", "k1"), row3, row4)

        assertEmpty(execute(cql, table, "SELECT s FROM %s WHERE s = ? AND token(k) < token(?)", "s0", "k0"))
        assertEmpty(execute(cql, table, "SELECT s FROM %s WHERE s = ? AND token(k) < token(?)", "s1", "k1"))

        execute(cql, table, f"DROP INDEX {test_keyspace}.sc_index")

        assertRows(execute(cql, table, "SELECT s FROM %s WHERE s = ? ALLOW FILTERING", "s0"), row1, row2)
        assertRows(execute(cql, table, "SELECT s FROM %s WHERE s = ? ALLOW FILTERING", "s1"), row3, row4)

        assertRows(execute(cql, table, "SELECT s FROM %s WHERE s = ? AND token(k) >= token(?) ALLOW FILTERING", "s0", "k0"), row1, row2)
        assertRows(execute(cql, table, "SELECT s FROM %s WHERE s = ? AND token(k) >= token(?) ALLOW FILTERING", "s1", "k1"), row3, row4)

        assertEmpty(execute(cql, table, "SELECT s FROM %s WHERE s = ? AND token(k) < token(?) ALLOW FILTERING", "s0", "k0"))
        assertEmpty(execute(cql, table, "SELECT s FROM %s WHERE s = ? AND token(k) < token(?) ALLOW FILTERING", "s1", "k1"))

def testindexOnClusteringColumnWithoutRegularColumns(cql, test_keyspace):
    row1 = row("k0", "c0")
    row2 = row("k0", "c1")
    row3 = row("k1", "c0")
    row4 = row("k1", "c1")
    with create_table(cql, test_keyspace, "(k text, c text, PRIMARY KEY(k, c))") as table:
        create_index(cql, table, "CREATE INDEX no_regulars_idx ON %s(c)")

        execute(cql, table, "INSERT INTO %s (k, c) VALUES (?, ?)", *row1)
        execute(cql, table, "INSERT INTO %s (k, c) VALUES (?, ?)", *row2)
        execute(cql, table, "INSERT INTO %s (k, c) VALUES (?, ?)", *row3)
        execute(cql, table, "INSERT INTO %s (k, c) VALUES (?, ?)", *row4)

        assertRowsIgnoringOrder(execute(cql, table, "SELECT * FROM %s WHERE c = ?", "c0"), row1, row3)
        assertRowsIgnoringOrder(execute(cql, table, "SELECT * FROM %s WHERE c = ?", "c1"), row2, row4)
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE c = ?", "c3"))

        execute(cql, table, f"DROP INDEX {test_keyspace}.no_regulars_idx")
        create_index(cql, table, "CREATE INDEX no_regulars_idx ON %s(c)")

        assertRowsIgnoringOrder(execute(cql, table, "SELECT * FROM %s WHERE c = ?", "c0"), row1, row3)
        assertRowsIgnoringOrder(execute(cql, table, "SELECT * FROM %s WHERE c = ?", "c1"), row2, row4)
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE c = ?", "c3"))

def testcreateIndexesOnMultipleMapDimensions(cql, test_keyspace):
    row1 = row(0, 0, {"a": 10, "b": 20, "c": 30})
    row2 = row(1, 1, {"d": 11, "e": 21, "f": 32})
    with create_table(cql, test_keyspace, "(k int, c int, m map<text, int>, PRIMARY KEY(k, c))") as table:
        create_index(cql, table, "CREATE INDEX ON %s(keys(m))")
        create_index(cql, table, "CREATE INDEX ON %s(m)")

        execute(cql, table, "INSERT INTO %s (k, c, m) VALUES (?, ?, ?)", *row1)
        execute(cql, table, "INSERT INTO %s (k, c, m) VALUES (?, ?, ?)", *row2)

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE m CONTAINS KEY 'a'"), row1)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE m CONTAINS 20"), row1)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE m CONTAINS KEY 'f'"), row2)
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE m CONTAINS 32"), row2)

def testinsertWithTombstoneRemovesEntryFromIndex(cql, test_keyspace):
    key = 0
    indexedValue = 99
    with create_table(cql, test_keyspace, "(k int, v int, PRIMARY KEY(k))") as table:
        create_index(cql, table, "CREATE INDEX ON %s(v)")
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", key, indexedValue)

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE v = ?", indexedValue), row(key, indexedValue))
        execute(cql, table, "DELETE v FROM %s WHERE k=?", key)
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE v = ?", indexedValue))

# The test updateTTLOnIndexedClusteringValue was not translated, because it
# reads the TTL of rows in Cassandra's internal index table directly.

def testindexBatchStatements(cql, test_keyspace):
    # see CASSANDRA-10536
    with create_table(cql, test_keyspace, "(a int, b int, c int, PRIMARY KEY (a, b))") as table:
        create_index(cql, table, "CREATE INDEX ON %s(c)")

        # Multi partition batch
        execute(cql, table, "BEGIN BATCH\n" +
                "UPDATE %s SET c = 0 WHERE a = 0 AND b = 0;\n" +
                "UPDATE %s SET c = 1 WHERE a = 1 AND b = 1;\n" +
                "APPLY BATCH")
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE c = 0"), row(0, 0, 0))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE c = 1"), row(1, 1, 1))

        # Single Partition batch
        execute(cql, table, "BEGIN BATCH\n" +
                "UPDATE %s SET c = 2 WHERE a = 2 AND b = 0;\n" +
                "UPDATE %s SET c = 3 WHERE a = 2 AND b = 1;\n" +
                "APPLY BATCH")
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE c = 2"), row(2, 0, 2))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE c = 3"), row(2, 1, 3))

def testindexStatementsWithConditions(cql, test_keyspace):
    # see CASSANDRA-10536
    with create_table(cql, test_keyspace, "(a int, b int, c int, PRIMARY KEY (a, b))") as table:
        create_index(cql, table, "CREATE INDEX ON %s(c)")

        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (0, 0, 0) IF NOT EXISTS")
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE c = 0"), row(0, 0, 0))

        execute(cql, table, "INSERT INTO %s (a, b, c) VALUES (0, 0, 1) IF NOT EXISTS")
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE c = 1"))

        execute(cql, table, "UPDATE %s SET c = 1 WHERE a = 0 AND b =0 IF c = 0")
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE c = 1"), row(0, 0, 1))
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE c = 0"))

        execute(cql, table, "DELETE FROM %s WHERE a = 0 AND b = 0 IF c = 0")
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE c = 1"), row(0, 0, 1))

        execute(cql, table, "DELETE FROM %s WHERE a = 0 AND b = 0 IF c = 1")
        assertEmpty(execute(cql, table, "SELECT * FROM %s WHERE c = 1"))

        execute(cql, table, "BEGIN BATCH\n" +
                "INSERT INTO %s (a, b, c) VALUES (2, 2, 2) IF NOT EXISTS;\n" +
                "INSERT INTO %s (a, b, c) VALUES (2, 3, 3)\n" +
                "APPLY BATCH")
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE c = 2"), row(2, 2, 2))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE c = 3"), row(2, 3, 3))

# The Java test reads the whole system."IndexInfo" table, and expects to see
# in it also Cassandra's internal indexes of its own system tables. We read
# only the rows of this test's keyspace (confusingly, IndexInfo's
# "table_name" column holds the keyspace name). The Java test also rebuilds
# the index through Cassandra's internal API, which we can't do. Finally,
# Cassandra's IndexInfo has a third column, "value", which is always null,
# and Scylla's IndexInfo doesn't have it, so we read only the first two.
def testindexCorrectlyMarkedAsBuildAndRemoved(cql, test_keyspace):
    selectBuiltIndexesQuery = f"SELECT table_name, index_name FROM system.\"IndexInfo\" WHERE table_name = '{test_keyspace}'"

    indexName = "build_remove_test_idx"
    with create_table(cql, test_keyspace, "(a int, b int, c int, PRIMARY KEY (a, b))") as table:
        indexRows = [r for r in cql.execute(selectBuiltIndexesQuery) if r.index_name != indexName]
        create_index(cql, table, "CREATE INDEX %s ON %%s(c)" % indexName)

        # check that there are no other rows in the built indexes table
        assertRowsIgnoringOrder(cql.execute(selectBuiltIndexesQuery), row(test_keyspace, indexName), *indexRows)

        # check that dropping the index removes it from the built indexes table
        execute(cql, table, f"DROP INDEX {test_keyspace}.{indexName}")
        assertRowsIgnoringOrder(cql.execute(selectBuiltIndexesQuery), *indexRows)

# Used in order to generate the unique names for indexes
indexCounter = 0

# Cassandra's TestScript class: runs a common scenario of creating, using
# and dropping an index on the given target column.
# The Java test also reloads the table's internal ColumnFamilyStore at some
# points, and checks that this doesn't change the query results. We can't
# do this, so we skip these steps.
def run_test_script(cql, keyspace, tableDefinition, target, firstRow, secondRow,
                    missingIndexMessage, firstQueryExpression, secondQueryExpression,
                    updateExpression=None, postUpdateQueryExpression=None):
    global indexCounter
    if updateExpression is not None:
        assert postUpdateQueryExpression is not None

    # first, create the table as we need the Tablemetadata to build the other cql statements
    with create_table(cql, keyspace, tableDefinition) as table:
        tableName = table.split('.')[1]
        indexName = "index_%s_%d" % (tableName, indexCounter)
        indexCounter += 1

        # The table's columns, in the order of SELECT *, and the primary key
        # columns.
        allColumns = cql.execute(f"SELECT * FROM {table} LIMIT 1").column_names
        columns = list(cql.execute("SELECT column_name, kind, position FROM system_schema.columns WHERE keyspace_name = %s AND table_name = %s", (keyspace, tableName)))
        partitionKeyColumns = [c.column_name for c in sorted((c for c in columns if c.kind == 'partition_key'), key=lambda c: c.position)]
        clusteringColumns = [c.column_name for c in sorted((c for c in columns if c.kind == 'clustering'), key=lambda c: c.position)]
        isCompactTable = 'COMPACT STORAGE' in tableDefinition.upper()
        # In a compact table, the Java test uses only the partition key
        # columns as the primary key.
        primaryKeyColumns = partitionKeyColumns if isCompactTable else partitionKeyColumns + clusteringColumns

        # now setup the cql statements the test will run through. Some are dependent on
        # the table definition, others are not.
        createIndexCql = "CREATE INDEX %s ON %%s(%s)" % (indexName, target)
        dropIndexCql = "DROP INDEX %s.%s" % (keyspace, indexName)

        selectFirstRowCql = "SELECT * FROM %%s WHERE %s" % firstQueryExpression
        selectSecondRowCql = "SELECT * FROM %%s WHERE %s" % secondQueryExpression
        insertCql = "INSERT INTO %%s (%s) VALUES (%s)" % (", ".join(allColumns), ", ".join("?" for _ in allColumns))
        deleteRowCql = "DELETE FROM %s WHERE " + " AND ".join(c + "=?" for c in primaryKeyColumns)
        deletePartitionCql = "DELETE FROM %s WHERE " + " AND ".join(c + "=?" for c in partitionKeyColumns)
        def getPrimaryKeyValues(row):
            return row[:len(primaryKeyColumns)]
        def getPartitionKeyValues(row):
            return row[:len(partitionKeyColumns)]

        # everything setup, run through the smoke test
        execute(cql, table, insertCql, *firstRow)
        # before creating the index, check we cannot query on the indexed column
        assert_invalid_message(cql, table, missingIndexMessage, selectFirstRowCql)

        # create the index, wait for it to be built then validate the indexed value
        create_index(cql, table, createIndexCql)
        assertRows(execute(cql, table, selectFirstRowCql), firstRow)
        assertEmpty(execute(cql, table, selectSecondRowCql))

        # flush and check again
        flush(cql, table)
        assertRows(execute(cql, table, selectFirstRowCql), firstRow)
        assertEmpty(execute(cql, table, selectSecondRowCql))

        # force major compaction and query again
        compact(cql, table)
        assertRows(execute(cql, table, selectFirstRowCql), firstRow)
        assertEmpty(execute(cql, table, selectSecondRowCql))

        # drop the index and assert we can no longer query using it
        execute(cql, table, dropIndexCql)
        assert_invalid_message(cql, table, missingIndexMessage, selectFirstRowCql)

        flush(cql, table)
        compact(cql, table)

        # insert second row, re-create the index and query for both indexed values
        execute(cql, table, insertCql, *secondRow)
        create_index(cql, table, createIndexCql)
        assertRows(execute(cql, table, selectFirstRowCql), firstRow)
        assertRows(execute(cql, table, selectSecondRowCql), secondRow)

        # modify the indexed value in the first row, assert we can query by the new value & not the original one
        # note: this is not possible if the indexed column is part of the primary key, so we skip it in that case
        if updateExpression is not None:
            updateCql = "UPDATE %%s %s WHERE %s" % (updateExpression, " AND ".join(c + "=?" for c in primaryKeyColumns))
            execute(cql, table, updateCql, *getPrimaryKeyValues(firstRow))
            assertEmpty(execute(cql, table, selectFirstRowCql))
            # update the select statement to query using the updated value
            selectFirstRowCql = "SELECT * FROM %%s WHERE %s" % postUpdateQueryExpression
            # we can't check the entire row b/c we've modified something.
            # so we just check the primary key columns, as they cannot have changed
            result = list(execute(cql, table, selectFirstRowCql))
            assert result
            columnCount = len(partitionKeyColumns) + (0 if isCompactTable else len(clusteringColumns))
            assert list(result[0])[:columnCount] == list(firstRow)[:columnCount]

        # delete row, check that it cannot be found via index query
        execute(cql, table, deleteRowCql, *getPrimaryKeyValues(firstRow))
        assertEmpty(execute(cql, table, selectFirstRowCql))

        # delete partition, check that its rows cannot be retrieved via index query
        execute(cql, table, deletePartitionCql, *getPartitionKeyValues(secondRow))
        assertEmpty(execute(cql, table, selectSecondRowCql))

        # flush & compact, then verify that deleted values stay gone
        flush(cql, table)
        compact(cql, table)
        assertEmpty(execute(cql, table, selectFirstRowCql))
        assertEmpty(execute(cql, table, selectSecondRowCql))

        # add back both rows, reset the select for the first row to query on the original value & verify
        execute(cql, table, insertCql, *firstRow)
        selectFirstRowCql = "SELECT * FROM %%s WHERE %s" % firstQueryExpression
        assertRows(execute(cql, table, selectFirstRowCql), firstRow)
        execute(cql, table, insertCql, *secondRow)
        assertRows(execute(cql, table, selectSecondRowCql), secondRow)

        # flush and compact, verify again & we're done
        flush(cql, table)
        compact(cql, table)
        assertRows(execute(cql, table, selectFirstRowCql), firstRow)
        assertRows(execute(cql, table, selectSecondRowCql), secondRow)
