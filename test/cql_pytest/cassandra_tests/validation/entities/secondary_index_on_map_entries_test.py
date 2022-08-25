# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 6ca34f81386dc8f6020cdf2ea4246bca2a0896c5
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0

from cassandra_tests.porting import *

def assertIndexInvalidForColumn(cql, table, colname):
    assert_invalid(cql, table, f"CREATE INDEX ON %s(ENTRIES({colname}))")

def testShouldNotCreateIndexOnFrozenMaps(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k TEXT PRIMARY KEY, v FROZEN<MAP<TEXT, TEXT>>)") as table:
        assertIndexInvalidForColumn(cql, table, "v")

def testShouldNotCreateIndexOnNonMapTypes(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k TEXT PRIMARY KEY, i INT, t TEXT, b BLOB, s SET<TEXT>, l LIST<TEXT>, tu TUPLE<TEXT>)") as table:
        assertIndexInvalidForColumn(cql, table, "i")
        assertIndexInvalidForColumn(cql, table, "t")
        assertIndexInvalidForColumn(cql, table, "b")
        assertIndexInvalidForColumn(cql, table, "s")
        assertIndexInvalidForColumn(cql, table, "l")
        assertIndexInvalidForColumn(cql, table, "tu")

# Most tests below used the same table created by createSimpleTableAndIndex()
# Luckily, although the original Java tests created a new table for each test,
# the tests work correctly even when reusing the same table, so we can save a
# lot of test time by reusing the same fixture instance for all tests in this
# file (hence the "module" fixture scope).
@pytest.fixture(scope="module")
def simple_table_and_index(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k TEXT PRIMARY KEY, v MAP<TEXT, INT>)") as table:
        cql.execute(f"CREATE INDEX ON {table}(ENTRIES(v))")
        yield table

def testShouldValidateMapKeyAndValueTypes(cql, simple_table_and_index):
    # The Java version of this test used prepared statements, but the Python
    # driver verifies the types of the bound variables before using them,
    # so here we use unprepared statements to check the server's verification.
    query = "SELECT * FROM %s WHERE v[{}] = {}"
    validKey = "'valid key'"
    invalidKey = 31415
    validValue = 31415
    invalidValue = "'invalid value'"
    assert_invalid(cql, simple_table_and_index, query.format(invalidKey, invalidValue))
    assert_invalid(cql, simple_table_and_index, query.format(invalidKey, validValue))
    assert_invalid(cql, simple_table_and_index, query.format(validKey, invalidValue))
    assert_empty(execute(cql, simple_table_and_index, query.format(validKey, validValue)))

def insertIntoSimpleTable(cql, table, key, value):
    query = "INSERT INTO %s (k, v) VALUES (?, ?)"
    execute(cql, table, query, key, value)
    return [key, value]

def assertRowsForConditions(cql, table, whereClause, params, *rows):
    assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s WHERE " + whereClause + " ALLOW FILTERING", *params), *rows)

def assertNoRowsForConditions(cql, table, whereClause, params):
    assert_empty(execute(cql, table, "SELECT * FROM %s WHERE " + whereClause + " ALLOW FILTERING", *params))
 
def testShouldFindRowsMatchingSingleEqualityRestriction(cql, simple_table_and_index):
    foo = insertIntoSimpleTable(cql, simple_table_and_index, "foo", {"a": 1, "c": 3})
    bar = insertIntoSimpleTable(cql, simple_table_and_index, "bar", {"a": 1, "b": 2})
    baz = insertIntoSimpleTable(cql, simple_table_and_index, "baz", {"b": 2, "c": 5, "d": 4})
    qux = insertIntoSimpleTable(cql, simple_table_and_index, "qux", {"b": 2, "d": 4})

    assertRowsForConditions(cql, simple_table_and_index, "v[?]=?", ["a", 1], bar, foo)
    assertRowsForConditions(cql, simple_table_and_index, "v[?]=?", ["b", 2], bar, baz, qux)
    assertRowsForConditions(cql, simple_table_and_index, "v[?]=?", ["c", 3], foo)
    assertRowsForConditions(cql, simple_table_and_index, "v[?]=?", ["c", 5], baz)
    assertRowsForConditions(cql, simple_table_and_index, "v[?]=?", ["d", 4], baz, qux)

def testRequireFilteringDirectiveIfMultipleRestrictionsSpecified(cql, simple_table_and_index):
    baseQuery = "SELECT * FROM %s WHERE v['foo'] = 31415 AND v['baz'] = 31416"
    assert_invalid(cql, simple_table_and_index, baseQuery)
    assert_empty(execute(cql, simple_table_and_index, baseQuery + " ALLOW FILTERING"))

def testShouldFindRowsMatchingMultipleEqualityRestrictions(cql, simple_table_and_index):
    foo = insertIntoSimpleTable(cql, simple_table_and_index, "foo", {"k1": 1})
    bar = insertIntoSimpleTable(cql, simple_table_and_index, "bar", {"k1": 1, "k2": 2})
    baz = insertIntoSimpleTable(cql, simple_table_and_index, "baz", {"k2": 2, "k3": 3})
    qux = insertIntoSimpleTable(cql, simple_table_and_index, "qux", {"k2": 2, "k3": 3, "k4": 4})
    assertRowsForConditions(cql, simple_table_and_index, "v[?]=?", ["k1", 1], bar, foo)
    assertRowsForConditions(cql, simple_table_and_index, "v[?]=? AND v[?]=?", ["k1", 1, "k2", 2], bar)
    assertNoRowsForConditions(cql, simple_table_and_index, "v[?]=? AND v[?]=? AND v[?]=?", ["k1", 1, "k2", 2, "k3", 3])
    assertRowsForConditions(cql, simple_table_and_index, "v[?]=? AND v[?]=?", ["k2", 2, "k3", 3], baz, qux)
    assertRowsForConditions(cql, simple_table_and_index, "v[?]=? AND v[?]=? AND v[?]=?", ["k2", 2, "k3", 3, "k4", 4], qux)
    assertRowsForConditions(cql, simple_table_and_index, "v[?]=? AND v[?]=?", ["k3", 3, "k4", 4], qux)
    assertNoRowsForConditions(cql, simple_table_and_index, "v[?]=? AND v[?]=? AND v[?]=?", ["k3", 3, "k4", 4, "k5", 5])

def testShouldFindRowsMatchingEqualityAndContainsRestrictions(cql, simple_table_and_index):
    foo = insertIntoSimpleTable(cql, simple_table_and_index, "foo", {"common": 31415, "k1": 1, "k2": 2, "k3": 3})
    bar = insertIntoSimpleTable(cql, simple_table_and_index, "bar", {"common": 31415, "k3": 3, "k4": 4, "k5": 5})
    baz = insertIntoSimpleTable(cql, simple_table_and_index, "baz", {"common": 31415, "k5": 5, "k6": 6, "k7": 7})

    assertRowsForConditions(cql, simple_table_and_index, "v[?]=?", ["common", 31415], bar, baz, foo)
    assertRowsForConditions(cql, simple_table_and_index, "v[?]=? AND v CONTAINS KEY ?", ["common", 31415, "k1"], foo)
    assertRowsForConditions(cql, simple_table_and_index, "v[?]=? AND v CONTAINS KEY ?", ["common", 31415, "k2"], foo)
    assertRowsForConditions(cql, simple_table_and_index, "v[?]=? AND v CONTAINS KEY ?", ["common", 31415, "k3"], bar, foo)
    assertRowsForConditions(cql, simple_table_and_index, "v[?]=? AND v CONTAINS KEY ? AND v CONTAINS ?", ["common", 31415, "k3", 2], foo)
    assertRowsForConditions(cql, simple_table_and_index, "v[?]=? AND v CONTAINS KEY ? AND v CONTAINS ?", ["common", 31415, "k3", 3], bar, foo)
    assertRowsForConditions(cql, simple_table_and_index, "v[?]=? AND v CONTAINS KEY ? AND v CONTAINS ?", ["common", 31415, "k3", 4], bar)
    assertRowsForConditions(cql, simple_table_and_index, "v[?]=? AND v CONTAINS KEY ? AND v CONTAINS KEY ?", ["common", 31415, "k3", "k5"], bar)
    assertRowsForConditions(cql, simple_table_and_index, "v[?]=? AND v CONTAINS KEY ?", ["common", 31415, "k5"], bar, baz)
    assertRowsForConditions(cql, simple_table_and_index, "v[?]=? AND v CONTAINS KEY ? AND v CONTAINS ?", ["common", 31415, "k5", 4], bar)
    assertRowsForConditions(cql, simple_table_and_index, "v[?]=? AND v CONTAINS KEY ? AND v CONTAINS ?", ["common", 31415, "k5", 5], bar, baz)
    assertRowsForConditions(cql, simple_table_and_index, "v[?]=? AND v CONTAINS KEY ? AND v CONTAINS ?", ["common", 31415, "k5", 6], baz)
    assertNoRowsForConditions(cql, simple_table_and_index, "v[?]=? AND v CONTAINS KEY ? AND v CONTAINS ?", ["common", 31415, "k5", 8])

def assertInvalidRelation(cql, table, rel):
    query = "SELECT * FROM %s WHERE v " + rel
    assert_invalid(cql, table, query)

def testShouldNotAcceptUnsupportedRelationsOnEntries(cql, simple_table_and_index):
    assertInvalidRelation(cql, simple_table_and_index, "< 31415")
    assertInvalidRelation(cql, simple_table_and_index, "<= 31415")
    assertInvalidRelation(cql, simple_table_and_index, "> 31415")
    assertInvalidRelation(cql, simple_table_and_index, ">= 31415")
    assertInvalidRelation(cql, simple_table_and_index, "IN (31415, 31416, 31417)")
    assertInvalidRelation(cql, simple_table_and_index, "CONTAINS 31415")
    assertInvalidRelation(cql, simple_table_and_index, "CONTAINS KEY 'foo'")

def updateMapInSimpleTable(cql, table, row, mapKey, mapValue):
    execute(cql, table, "UPDATE %s SET v[?] = ? WHERE k = ?", mapKey, mapValue, row[0])
    results = list(execute(cql, table, "SELECT * FROM %s WHERE k = ?", row[0]))
    value = row[1]
    if mapValue == None:
        del value[mapKey]
    else:
        value[mapKey] = mapValue
    row[1] = value
    return row

def testShouldRecognizeAlteredOrDeletedMapEntries(cql, simple_table_and_index):
    foo = insertIntoSimpleTable(cql, simple_table_and_index, "foo", {"common": 31415, "target": 8192})
    bar = insertIntoSimpleTable(cql, simple_table_and_index, "bar", {"common": 31415, "target": 8192})
    baz = insertIntoSimpleTable(cql, simple_table_and_index, "baz", {"common": 31415, "target": 8192})

    assertRowsForConditions(cql, simple_table_and_index, "v[?]=?", ["target", 8192], bar, baz, foo)
    baz = updateMapInSimpleTable(cql, simple_table_and_index, baz, "target", 4096)
    assertRowsForConditions(cql, simple_table_and_index, "v[?]=?", ["target", 8192], bar, foo)
    bar = updateMapInSimpleTable(cql, simple_table_and_index, bar, "target", None)
    assertRowsForConditions(cql, simple_table_and_index, "v[?]=?", ["target", 8192], foo)
    execute(cql, simple_table_and_index, "DELETE FROM %s WHERE k = 'foo'")
    assertNoRowsForConditions(cql, simple_table_and_index, "v[?]=?", ["target", 8192])
    assertRowsForConditions(cql, simple_table_and_index, "v[?]=?", ["common", 31415], bar, baz)
    assertRowsForConditions(cql, simple_table_and_index, "v[?]=?", ["target", 4096], baz)


# Scylla does not consider "= null" an error, it just matches nothing.
# See issue #4776.
#def testShouldRejectQueriesForNullEntries(cql, simple_table_and_index):
#    assert_invalid(cql, simple_table_and_index, "SELECT * FROM %s WHERE v['somekey'] = null")

def testShouldTreatQueriesAgainstFrozenMapIndexesAsInvalid(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k TEXT PRIMARY KEY, v FROZEN<MAP<TEXT, TEXT>>)") as table:
        execute(cql, table, "CREATE INDEX ON %s(FULL(V))")
        assert_invalid_message(cql, table,
            "Map-entry equality predicates on frozen map column v are not supported",
            "SELECT * FROM %s WHERE v['somekey'] = 'somevalue'")
