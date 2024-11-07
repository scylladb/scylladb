# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of Cassandra 4.1.3 (commit 2a4cd36475de3eb47207cd88d2d472b876c6816d)
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0

from cassandra_tests.porting import *

# InsertUpdateIfConditionCollectionsTest class has been split into multiple ones because of timeout issues (CASSANDRA-16670)
# Any changes here check if they apply to the other classes
# - InsertUpdateIfConditionStaticsTest
# - InsertUpdateIfConditionCollectionsTest
# - InsertUpdateIfConditionTest

# As explained in docs/kb/lwt-differences.rst, Scylla is *different* from
# Cassandra in that it always returns the full version of the old row, even
# if the old row didn't exist (so it's all NULLs) or if the condition failed -
# but in those cases Cassandra only returns the success boolean and not the
# whole row. Moreover, for batch statements, Scylla returns an old row for
# every conditional statement in the batch, and Cassandra doesn't always.
# Note (and this is relevant for the tests in this file) that if the row
# didn't exist but a static row did exist in that partition, the static row's
# value is also returned by Scylla.
@pytest.fixture(scope="module")
def is_scylla(cql):
    names = [row.table_name for row in cql.execute("SELECT * FROM system_schema.tables WHERE keyspace_name = 'system'")]
    yield any('scylla' in name for name in names)

# Migrated from cql_tests.py:TestCQL.static_columns_cas_test()
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18066")]), "vnodes"],
                         indirect=True)
def testStaticColumnsCas(cql, test_keyspace, is_scylla):
    with create_table(cql, test_keyspace, "(id int, k text, version int static, v text, PRIMARY KEY (id, k))") as table:
        # Test that INSERT IF NOT EXISTS concerns only the static column if no clustering nor regular columns
        # is provided, but concerns the CQL3 row targetted by the clustering columns otherwise
        execute(cql, table, "INSERT INTO %s (id, k, v) VALUES (1, 'foo', 'foo')")
        assertRows(execute(cql, table, "INSERT INTO %s (id, k, version) VALUES (1, 'foo', 1) IF NOT EXISTS"), row(False, 1, "foo", null, "foo"))
        assertRows(execute(cql, table, "INSERT INTO %s (id, version) VALUES (1, 1) IF NOT EXISTS"), row(True, 1, null, null, null) if is_scylla else row(True))
        assertRows(execute(cql, table, "SELECT * FROM %s"), row(1, "foo", 1, "foo"))
        execute(cql, table, "DELETE FROM %s WHERE id = 1")

        execute(cql, table, "INSERT INTO %s(id, version) VALUES (0, 0)")

        assertRows(execute(cql, table, "UPDATE %s SET v='foo', version=1 WHERE id=0 AND k='k1' IF version = ?", 0), row(True, 0) if is_scylla else row(True))
        assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, "k1", 1, "foo"))

        assertRows(execute(cql, table, "UPDATE %s SET v='bar', version=1 WHERE id=0 AND k='k2' IF version = ?", 0), row(False, 1))
        assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, "k1", 1, "foo"))

        assertRows(execute(cql, table, "UPDATE %s SET v='bar', version=2 WHERE id=0 AND k='k2' IF version = ?", 1), row(True, 1) if is_scylla else row(True))
        assertRows(execute(cql, table, "SELECT * FROM %s"), row(0, "k1", 2, "foo"), row(0, "k2", 2, "bar"))

        # Batch output is slightly different from non-batch CAS, since a full PK is included to disambiguate
        # cases when conditions span across multiple rows.
        assertRows(execute(cql, table, "UPDATE %s SET version=3 WHERE id=0 IF version=1; "),
                   row(False, 2))
        # Testing batches
        assert_rows_list(execute(cql, table, "BEGIN BATCH " +
                           "UPDATE %s SET v='foobar' WHERE id=0 AND k='k1'; " +
                           "UPDATE %s SET v='barfoo' WHERE id=0 AND k='k2'; " +
                           "UPDATE %s SET version=3 WHERE id=0 IF version=1; " +
                           "APPLY BATCH "),
                   [row(False, 0, "k1", 2),row(False,0,None,2),row(False,0,None,2)] if is_scylla else [row(False, 0, "k1", 2)])

        assert_rows_list(execute(cql, table, "BEGIN BATCH " +
                           "UPDATE %s SET v = 'foobar' WHERE id = 0 AND k = 'k1'; " +
                           "UPDATE %s SET v = 'barfoo' WHERE id = 0 AND k = 'k2'; " +
                           "UPDATE %s SET version = 3 WHERE id = 0 IF version = 2; " +
                           "APPLY BATCH "),
                   [row(True,0,"k1",2),row(True,0,None,2),row(True,0,None,2)] if is_scylla else [row(True)])

        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row(0, "k1", 3, "foobar"),
                   row(0, "k2", 3, "barfoo"))

        assert_rows_list(execute(cql, table, "BEGIN BATCH " +
                           "UPDATE %s SET version = 4 WHERE id = 0 IF version = 3; " +
                           "UPDATE %s SET v='row1' WHERE id=0 AND k='k1' IF v='foo'; " +
                           "UPDATE %s SET v='row2' WHERE id=0 AND k='k2' IF v='bar'; " +
                           "APPLY BATCH "),
                   [row(False, 0, None, 3, None),
                   row(False, 0, "k1", 3, "foobar"),
                   row(False, 0, "k2", 3, "barfoo")] if is_scylla else
                   [row(False, 0, "k1", 3, "foobar"),
                   row(False, 0, "k2", 3, "barfoo")])

        assert_rows_list(execute(cql, table, "BEGIN BATCH " +
                           "UPDATE %s SET version = 4 WHERE id = 0 IF version = 3; " +
                           "UPDATE %s SET v='row1' WHERE id = 0 AND k='k1' IF v='foobar'; " +
                           "UPDATE %s SET v='row2' WHERE id = 0 AND k='k2' IF v='barfoo'; " +
                           "APPLY BATCH "),
                   [row(True,0,None,3,None),row(True,0,"k1",3,"foobar"),row(True,0,"k2",3,"barfoo")] if is_scylla else [row(True)])

        assertRows(execute(cql, table, "SELECT * FROM %s"),
                   row(0, "k1", 4, "row1"),
                   row(0, "k2", 4, "row2"))

        assertInvalid(cql, table, "BEGIN BATCH " +
                      "UPDATE %s SET version=5 WHERE id=0 IF version=4; " +
                      "UPDATE %s SET v='row1' WHERE id=0 AND k='k1'; " +
                      "UPDATE %s SET v='row2' WHERE id=1 AND k='k2'; " +
                      "APPLY BATCH ")

        assert_rows_list(execute(cql, table, "BEGIN BATCH " +
                           "INSERT INTO %s (id, k, v) VALUES (1, 'k1', 'val1') IF NOT EXISTS; " +
                           "INSERT INTO %s (id, k, v) VALUES (1, 'k2', 'val2') IF NOT EXISTS; " +
                           "APPLY BATCH "),
                   [row(True,None,None,None,None),row(True,None,None,None,None)] if is_scylla else [row(True)])

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE id=1"),
                   row(1, "k1", null, "val1"),
                   row(1, "k2", null, "val2"))

        assertRows(execute(cql, table, "INSERT INTO %s (id, k, v) VALUES (1, 'k2', 'val2') IF NOT EXISTS"), row(False, 1, "k2", null, "val2"))

        assert_rows_list(execute(cql, table, "BEGIN BATCH " +
                           "INSERT INTO %s (id, k, v) VALUES (1, 'k2', 'val2') IF NOT EXISTS; " +
                           "INSERT INTO %s (id, k, v) VALUES (1, 'k3', 'val3') IF NOT EXISTS; " +
                           "APPLY BATCH"),
                   [row(False,1,"k2",None,"val2"),row(False,None,None,None,None)] if is_scylla else [row(False, 1, "k2", null, "val2")])

        assert_rows_list(execute(cql, table, "BEGIN BATCH " +
                           "UPDATE %s SET v = 'newVal' WHERE id = 1 AND k = 'k2' IF v = 'val0'; " +
                           "INSERT INTO %s (id, k, v) VALUES (1, 'k3', 'val3') IF NOT EXISTS; " +
                           "APPLY BATCH"),
                   [row(False,1,"k2",None,"val2"),row(False,None,None,None,None)] if is_scylla else [row(False, 1, "k2", null, "val2")])

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE id=1"),
                   row(1, "k1", null, "val1"),
                   row(1, "k2", null, "val2"))

        assert_rows_list(execute(cql, table, "BEGIN BATCH " +
                           "UPDATE %s SET v = 'newVal' WHERE id = 1 AND k = 'k2' IF v = 'val2'; " +
                           "INSERT INTO %s (id, k, v, version) VALUES(1, 'k3', 'val3', 1) IF NOT EXISTS; " +
                           "APPLY BATCH"),
                   [row(True,1,"k2",None,"val2"),row(True,None,None,None,None)] if is_scylla else [row(True)])

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE id=1"),
                   row(1, "k1", 1, "val1"),
                   row(1, "k2", 1, "newVal"),
                   row(1, "k3", 1, "val3"))

        assert_rows_list(execute(cql, table, "BEGIN BATCH " +
                           "UPDATE %s SET v = 'newVal1' WHERE id = 1 AND k = 'k2' IF v = 'val2'; " +
                           "UPDATE %s SET v = 'newVal2' WHERE id = 1 AND k = 'k2' IF v = 'val3'; " +
                           "APPLY BATCH"),
                   [row(False,1,"k2","newVal"),row(False,1,"k2","newVal")] if is_scylla else [row(False, 1, "k2", "newVal")])

# Test CASSANDRA-10532
@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18066")]), "vnodes"],
                         indirect=True)
def testStaticColumnsCasDelete(cql, test_keyspace, is_scylla):
    with create_table(cql, test_keyspace, "(pk int, ck int, static_col int static, value int, PRIMARY KEY (pk, ck))") as table:
        execute(cql, table, "INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", 1, 1, 2)
        execute(cql, table, "INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", 1, 3, 4)
        execute(cql, table, "INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", 1, 5, 6)
        execute(cql, table, "INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", 1, 7, 8)
        execute(cql, table, "INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", 2, 1, 2)
        execute(cql, table, "INSERT INTO %s (pk, static_col) VALUES (?, ?)", 1, 1)

        assertRows(execute(cql, table, "DELETE static_col FROM %s WHERE pk = ? IF static_col = ?", 1, 2), row(False, 1))
        assertRows(execute(cql, table, "DELETE static_col FROM %s WHERE pk = ? IF static_col = ?", 1, 1), row(True,1) if is_scylla else row(True))

        assertRows(execute(cql, table, "SELECT pk, ck, static_col, value FROM %s WHERE pk = 1"),
                   row(1, 1, null, 2),
                   row(1, 3, null, 4),
                   row(1, 5, null, 6),
                   row(1, 7, null, 8))
        execute(cql, table, "INSERT INTO %s (pk, static_col) VALUES (?, ?)", 1, 1)

        # Scylla's and Cassandra's error messages here focus on different things
        assertInvalid(cql, table,
                             "DELETE static_col FROM %s WHERE ck = ? IF static_col = ?", 1, 1)

        assertInvalidMessage(cql, table, "Invalid restrictions on clustering columns since the DELETE statement modifies only static columns",
                             "DELETE static_col FROM %s WHERE pk = ? AND ck = ? IF static_col = ?", 1, 1, 1)

        # Scylla's and Cassandra's error messages here are different,
        # sharing only the word "primary key"
        assert_invalid_message_ignore_case(cql, table, "primary key",
                             "DELETE static_col, value FROM %s WHERE pk = ? IF static_col = ?", 1, 1)

        # Same query but with an invalid condition
        assert_invalid_message_ignore_case(cql, table, "primary key",
                             "DELETE static_col, value FROM %s WHERE pk = ? IF static_col = ?", 1, 2)

        # DELETE of an underspecified PRIMARY KEY should not succeed if static is not only restriction
        assert_invalid_message_ignore_case(cql, table, "primary key",
                             "DELETE static_col FROM %s WHERE pk = ? IF value = ? AND static_col = ?", 1, 2, 1)

        print("NYH")
        print(list(execute(cql, table, "DELETE value FROM %s WHERE pk = ? AND ck = ? IF value = ? AND static_col = ?", 1, 1, 2, 2)))
        # Scylla returns the static column first, Cassandra returns
        # delete column first. Since both also say which columns they
        # return, neither is more correct than the other.
        assertRows(execute(cql, table, "DELETE value FROM %s WHERE pk = ? AND ck = ? IF value = ? AND static_col = ?", 1, 1, 2, 2), row(False, 1, 2) if is_scylla else row(False, 2, 1))
        assertRows(execute(cql, table, "DELETE value FROM %s WHERE pk = ? AND ck = ? IF value = ? AND static_col = ?", 1, 1, 2, 1), row(True, 1, 2) if is_scylla else row(True))
        assertRows(execute(cql, table, "SELECT pk, ck, static_col, value FROM %s WHERE pk = 1"),
                   row(1, 1, 1, null),
                   row(1, 3, 1, 4),
                   row(1, 5, 1, 6),
                   row(1, 7, 1, 8))

        assertRows(execute(cql, table, "DELETE static_col FROM %s WHERE pk = ? AND ck = ? IF value = ?", 1, 5, 10), row(False, 6))
        assertRows(execute(cql, table, "DELETE static_col FROM %s WHERE pk = ? AND ck = ? IF value = ?", 1, 5, 6), row(True, 6) if is_scylla else row(True))
        assertRows(execute(cql, table, "SELECT pk, ck, static_col, value FROM %s WHERE pk = 1"),
                   row(1, 1, null, null),
                   row(1, 3, null, 4),
                   row(1, 5, null, 6),
                   row(1, 7, null, 8))

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18066")]), "vnodes"],
                         indirect=True)
def testStaticColumnsCasUpdate(cql, test_keyspace, is_scylla):
    with create_table(cql, test_keyspace, "(pk int, ck int, static_col int static, value int, PRIMARY KEY (pk, ck))") as table:
        execute(cql, table, "INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", 1, 1, 2)
        execute(cql, table, "INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", 1, 3, 4)
        execute(cql, table, "INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", 1, 5, 6)
        execute(cql, table, "INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", 1, 7, 8)
        execute(cql, table, "INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", 2, 1, 2)
        execute(cql, table, "INSERT INTO %s (pk, static_col) VALUES (?, ?)", 1, 1)

        assertRows(execute(cql, table, "UPDATE %s SET static_col = ? WHERE pk = ? IF static_col = ?", 3, 1, 2), row(False, 1))
        assertRows(execute(cql, table, "UPDATE %s SET static_col = ? WHERE pk = ? IF static_col = ?", 2, 1, 1), row(True, 1) if is_scylla else row(True))

        assertRows(execute(cql, table, "SELECT pk, ck, static_col, value FROM %s WHERE pk = 1"),
                   row(1, 1, 2, 2),
                   row(1, 3, 2, 4),
                   row(1, 5, 2, 6),
                   row(1, 7, 2, 8))

        # Scylla and Cassandra have different error messages
        assertInvalid(cql, table,
                             "UPDATE %s SET static_col = ? WHERE ck = ? IF static_col = ?", 3, 1, 1)

        assertInvalidMessage(cql, table, "Invalid restrictions on clustering columns since the UPDATE statement modifies only static columns",
                             "UPDATE %s SET static_col = ? WHERE pk = ? AND ck = ? IF static_col = ?", 3, 1, 1, 1)

        # Scylla and Cassandra have different error messages
        assertInvalid(cql, table,
                             "UPDATE %s SET static_col = ?, value = ? WHERE pk = ? IF static_col = ?", 3, 1, 1, 2)

        # Same query but with an invalid condition
        assertInvalidMessage(cql, table, "ck",
                             "UPDATE %s SET static_col = ?, value = ? WHERE pk = ? IF static_col = ?", 3, 1, 1, 1)

        assertInvalidMessage(cql, table, "ck",
                             "UPDATE %s SET static_col = ? WHERE pk = ? IF value = ? AND static_col = ?", 3, 1, 4, 2)

        assertRows(execute(cql, table, "UPDATE %s SET value = ? WHERE pk = ? AND ck = ? IF value = ? AND static_col = ?", 3, 1, 1, 3, 2), row(False, 2, 2))
        assertRows(execute(cql, table, "UPDATE %s SET value = ? WHERE pk = ? AND ck = ? IF value = ? AND static_col = ?", 1, 1, 1, 2, 2), row(True, 2, 2) if is_scylla else row(True))
        assertRows(execute(cql, table, "SELECT pk, ck, static_col, value FROM %s WHERE pk = 1"),
                   row(1, 1, 2, 1),
                   row(1, 3, 2, 4),
                   row(1, 5, 2, 6),
                   row(1, 7, 2, 8))

        assertRows(execute(cql, table, "UPDATE %s SET static_col = ? WHERE pk = ? AND ck = ? IF value = ?", 3, 1, 1, 2), row(False, 1))
        assertRows(execute(cql, table, "UPDATE %s SET static_col = ? WHERE pk = ? AND ck = ? IF value = ?", 1, 1, 1, 1), row(True, 1) if is_scylla else row(True))
        assertRows(execute(cql, table, "SELECT pk, ck, static_col, value FROM %s WHERE pk = 1"),
                   row(1, 1, 1, 1),
                   row(1, 3, 1, 4),
                   row(1, 5, 1, 6),
                   row(1, 7, 1, 8))

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18066")]), "vnodes"],
                         indirect=True)
def testConditionalUpdatesOnStaticColumns(cql, test_keyspace, is_scylla):
    with create_table(cql, test_keyspace, "(a int, b int, s int static, d text, PRIMARY KEY (a, b))") as table:
        assertInvalidMessage(cql, table, "unset", "UPDATE %s SET s = 6 WHERE a = 6 IF s = ?", unset())

        # pre-existing row
        execute(cql, table, "INSERT INTO %s (a, b, s, d) values (6, 6, 100, 'a')")
        assertRows(execute(cql, table, "UPDATE %s SET s = 6 WHERE a = 6 IF s = 100"),
                   row(true, 100) if is_scylla else row(true))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 6"),
                   row(6, 6, 6, "a"))

        execute(cql, table, "INSERT INTO %s (a, b, s, d) values (7, 7, 100, 'a')")
        assertRows(execute(cql, table, "UPDATE %s SET s = 7 WHERE a = 7 IF s = 101"),
                   row(false, 100))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 7"),
                   row(7, 7, 100, "a"))

        # pre-existing row with null in the static column
        execute(cql, table, "INSERT INTO %s (a, b, d) values (7, 7, 'a')")
        assertRows(execute(cql, table, "UPDATE %s SET s = 7 WHERE a = 7 IF s = NULL"),
                   row(false, 100))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 7"),
                   row(7, 7, 100, "a"))

        # deleting row before CAS makes it effectively non-existing
        execute(cql, table, "DELETE FROM %s WHERE a = 8;")
        assertRows(execute(cql, table, "UPDATE %s SET s = 8 WHERE a = 8 IF s = NULL"),
                   row(true, null) if is_scylla else row(true))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 8"),
                   row(8, null, 8, null))

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18066")]), "vnodes"],
                         indirect=True)
def testStaticsWithMultipleConditions(cql, test_keyspace, is_scylla):
    with create_table(cql, test_keyspace, "(a int, b int, s1 int static, s2 int static, d int, PRIMARY KEY (a, b))") as table:
        for i in range(1,6):
            execute(cql, table, "INSERT INTO %s (a, b, d) VALUES (?, ?, ?)", i, 1, 5)
            execute(cql, table, "INSERT INTO %s (a, b, d) VALUES (?, ?, ?)", i, 2, 6)

        assert_rows_list(execute(cql, table, "BEGIN BATCH\n"
                           + "UPDATE %s SET s2 = 102 WHERE a = 1 IF s1 = null;\n"
                           + "UPDATE %s SET s1 = 101 WHERE a = 1 IF s2 = null;\n"
                           + "APPLY BATCH"),
                   [row(true,1,None,None,None),row(true,1,None,None,None)] if is_scylla else [row(true)])
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 1"),
                   row(1, 1, 101, 102, 5),
                   row(1, 2, 101, 102, 6))

        assert_rows_list(execute(cql, table, "BEGIN BATCH\n"
                           + "UPDATE %s SET s2 = 202 WHERE a = 2 IF s1 = null;\n"
                           + "UPDATE %s SET s1 = 201 WHERE a = 2 IF s2 = null;\n"
                           + "UPDATE %s SET d = 203 WHERE a = 2 AND b = 1 IF d = 5;\n"
                           + "UPDATE %s SET d = 204 WHERE a = 2 AND b = 2 IF d = 6;\n"
                           + "APPLY BATCH"),
                   [row(true,2,None,None,None,None),row(true,2,None,None,None,None),row(true,2,1,None,None,5),row(true,2,2,None,None,6)] if is_scylla else [row(true)])

        assertRows(execute(cql, table, "SELECT * FROM %s WHERE a = 2"),
                   row(2, 1, 201, 202, 203),
                   row(2, 2, 201, 202, 204))

        assert_rows_list(execute(cql, table, "BEGIN BATCH\n"
                           + "UPDATE %s SET s2 = 202 WHERE a = 20 IF s1 = null;\n"
                           + "UPDATE %s SET s1 = 201 WHERE a = 20 IF s2 = null;\n"
                           + "UPDATE %s SET d = 203 WHERE a = 20 AND b = 1 IF d = 5;\n"
                           + "UPDATE %s SET d = 204 WHERE a = 20 AND b = 2 IF d = 6;\n"
                           + "APPLY BATCH"),
                   [row(false,None,None,None,None,None),row(false,None,None,None,None,None),row(false,None,None,None,None,None),row(false,None,None,None,None,None)] if is_scylla else [row(false)])

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18066")]), "vnodes"],
                         indirect=True)
def testStaticColumnsCasUpdateWithNullStaticColumn(cql, test_keyspace, is_scylla):
    with create_table(cql, test_keyspace, "(pk int, ck int, s1 int static, s2 int static, value int, PRIMARY KEY (pk, ck))") as table:
        execute(cql, table, "INSERT INTO %s (pk, s1, s2) VALUES (1, 1, 1) USING TIMESTAMP 1000")
        execute(cql, table, "INSERT INTO %s (pk, s1, s2) VALUES (2, 1, 1) USING TIMESTAMP 1001")
        flush(cql, table)
        execute(cql, table, "INSERT INTO %s (pk, s1) VALUES (1, 2) USING TIMESTAMP 2000")
        execute(cql, table, "INSERT INTO %s (pk, s1) VALUES (2, 2) USING TIMESTAMP 2001")
        flush(cql, table)
        execute(cql, table, "DELETE s1 FROM %s USING TIMESTAMP 3000 WHERE pk = 1")
        execute(cql, table, "DELETE s1 FROM %s USING TIMESTAMP 3001 WHERE pk = 2")
        flush(cql, table)

        assertRows(execute(cql, table, "UPDATE %s SET s1 = ? WHERE pk = ? IF s1 = NULL", 2, 1), row(true, null) if is_scylla else row(true))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ?", 1), row(1, null, 2, 1, null))
        assertRows(execute(cql, table, "UPDATE %s SET s1 = ? WHERE pk = ? IF EXISTS", 2, 2), row(true,2,null,null,1,null) if is_scylla else row(true))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ?", 2), row(2, null, 2, 1, null))

@pytest.mark.parametrize("test_keyspace",
                         [pytest.param("tablets", marks=[pytest.mark.xfail(reason="issue #18066")]), "vnodes"],
                         indirect=True)
def testStaticColumnsCasDeleteWithNullStaticColumn(cql, test_keyspace, is_scylla):
    with create_table(cql, test_keyspace, "(pk int, ck int, s1 int static, s2 int static, value int, PRIMARY KEY (pk, ck))") as table:
        execute(cql, table, "INSERT INTO %s (pk, s1, s2) VALUES (1, 1, 1) USING TIMESTAMP 1000")
        execute(cql, table, "INSERT INTO %s (pk, s1, s2) VALUES (2, 1, 1) USING TIMESTAMP 1001")
        flush(cql, table)
        execute(cql, table, "INSERT INTO %s (pk, s1) VALUES (1, 2) USING TIMESTAMP 2000")
        execute(cql, table, "INSERT INTO %s (pk, s1) VALUES (2, 2) USING TIMESTAMP 2001")
        flush(cql, table)
        execute(cql, table, "DELETE s1 FROM %s USING TIMESTAMP 3000 WHERE pk = 1")
        execute(cql, table, "DELETE s1 FROM %s USING TIMESTAMP 3001 WHERE pk = 2")
        flush(cql, table)

        assertRows(execute(cql, table, "DELETE s2 FROM %s WHERE pk = ? IF s1 = NULL", 1), row(true,null) if is_scylla else row(true))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ?", 1))
        assertRows(execute(cql, table, "DELETE s2 FROM %s WHERE pk = ? IF EXISTS", 2), row(true,2,null,null,1,null) if is_scylla else row(true))
        assertRows(execute(cql, table, "SELECT * FROM %s WHERE pk = ?", 2))
