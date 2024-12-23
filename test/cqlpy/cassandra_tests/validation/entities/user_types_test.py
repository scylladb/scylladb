# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 6ca34f81386dc8f6020cdf2ea4246bca2a0896c5
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0

from cassandra_tests.porting import *
from cassandra.query import UNSET_VALUE
from uuid import UUID

def testInvalidField(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(f int)") as my_type:
        with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, v frozen<{my_type}>)") as table:
            # 's' is not a field of myType
            assert_invalid(cql, table, "INSERT INTO %s (k, v) VALUES (?, {s : ?})", 0, 1)

# The test testInvalidInputForUserType was not translated, because it tries
# to pass invalid user-type values via a bound parameter to the server and
# see the errors, but the Python drivers catches those errors before passing
# them to the server.

def testCassandra8105(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(a int, b int)") as ut1:
        with create_type(cql, test_keyspace, f"(j frozen<{ut1}>, k int)") as ut2:
            with create_table(cql, test_keyspace, f"(x int PRIMARY KEY, y set<frozen<{ut2}>>)") as table:
                execute(cql, table, "INSERT INTO %s (x, y) VALUES (1, { { k: 1 } })")
    with create_type(cql, test_keyspace, "(a int, b int)") as ut3:
        with create_type(cql, test_keyspace, f"(j frozen<{ut3}>, k int)") as ut4:
            with create_table(cql, test_keyspace, f"(x int PRIMARY KEY, y list<frozen<{ut4}>>)") as table:
                execute(cql, table, "INSERT INTO %s (x, y) VALUES (1, [ { k: 1 } ])")
    with create_type(cql, test_keyspace, "(a int, b int)") as ut5:
        with create_type(cql, test_keyspace, f"(i int, j frozen<{ut5}>)") as ut6:
            with create_table(cql, test_keyspace, f"(x int PRIMARY KEY, y set<frozen<{ut6}>>)") as table:
                execute(cql, table, "INSERT INTO %s (x, y) VALUES (1, { { i: 1 } })")

def testFor7684(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(x double)") as my_type:
        with create_table(cql, test_keyspace, f"(k int, v frozen<{my_type}>, b boolean static, PRIMARY KEY (k,v))") as table:
            execute(cql, table, "INSERT INTO %s(k, v) VALUES (?, {x:?})", 1, -104.99251)
            execute(cql, table, "UPDATE %s SET b = ? WHERE k = ?", True, 1)

            for _ in before_and_after_flush(cql, table):
                assert_rows(execute(cql, table, "SELECT v.x FROM %s WHERE k = ? AND v = {x:?}", 1, -104.99251), [-104.99251])

def testInvalidUDTStatements(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(a int)") as myType:
        # non-frozen UDTs in a table PK
        # Cassandra's and Scylla's error messages are a bit different here.
        assert_invalid_message_re(cql, test_keyspace, "Invalid non-frozen user-defined type.*for PRIMARY KEY.*k",
                "CREATE TABLE " + test_keyspace + ".wrong (k " + myType + " PRIMARY KEY , v int)")
        assert_invalid_message_re(cql, test_keyspace, "Invalid non-frozen user-defined type.*for PRIMARY KEY.*k2",
                "CREATE TABLE " + test_keyspace + ".wrong (k1 int, k2 " + myType + ", v int, PRIMARY KEY (k1, k2))")

        # non-frozen UDTs in a collection
        # Cassandra's and Scylla's error messages are a bit different here.
        assert_invalid_message_re(cql, test_keyspace, "Non-frozen.*inside collections: list<" + myType + ">",
                "CREATE TABLE " + test_keyspace + ".wrong (k int PRIMARY KEY, v list<" + myType + ">)")
        assert_invalid_message_re(cql, test_keyspace, "Non-frozen.*inside collections: set<" + myType + ">",
                "CREATE TABLE " + test_keyspace + ".wrong (k int PRIMARY KEY, v set<" + myType + ">)")
        assert_invalid_message_re(cql, test_keyspace, "Non-frozen.*inside collections: map<" + myType + ", int>",
                "CREATE TABLE " + test_keyspace + ".wrong (k int PRIMARY KEY, v map<" + myType + ", int>)")
        assert_invalid_message_re(cql, test_keyspace, "Non-frozen.*inside collections: map<int, " + myType + ">",
                "CREATE TABLE " + test_keyspace + ".wrong (k int PRIMARY KEY, v map<int, " + myType + ">)")

        # non-frozen UDT in a collection (as part of a UDT definition)
        assert_invalid_message_re(cql, test_keyspace, "Non-frozen.*inside collections: list<" + myType + ">",
                "CREATE TYPE " + test_keyspace + ".wrong (a int, b list<" + myType + ">)")

        # non-frozen UDT in a UDT
        assert_invalid_message(cql, test_keyspace, "A user type cannot contain non-frozen",
                "CREATE TYPE " + test_keyspace + ".wrong (a int, b " + myType + ")")

        # referencing a UDT in another keyspace
        assert_invalid_message(cql, test_keyspace, "Statement on keyspace " + test_keyspace + " cannot refer to a user type in keyspace otherkeyspace;" +
                             " user types can only be used in the keyspace they are defined in",
                             "CREATE TABLE " + test_keyspace + ".wrong (k int PRIMARY KEY, v frozen<otherKeyspace.myType>)")

        # referencing an unknown UDT
        assert_invalid_message(cql, test_keyspace, "Unknown type " + test_keyspace + ".unknowntype",
                             "CREATE TABLE " + test_keyspace + ".wrong (k int PRIMARY KEY, v frozen<" + test_keyspace + '.' + "unknownType>)")

        with create_table(cql, test_keyspace, f"(a int PRIMARY KEY, b frozen<{myType}>, c int)") as table:
            # bad deletions on frozen UDTs
            assert_invalid_message(cql, table, "Frozen UDT column b does not support field deletion", "DELETE b.a FROM %s WHERE a = 0")
            assert_invalid_message_re(cql, table, "Invalid.*deletion operation for non-UDT column c", "DELETE c.a FROM %s WHERE a = 0")

            # bad updates on frozen UDTs
            assert_invalid_message(cql, table, "Invalid operation (b.a = 0) for frozen UDT column b", "UPDATE %s SET b.a = 0 WHERE a = 0")
            assert_invalid_message(cql, table, "Invalid operation (c.a = 0) for non-UDT column c", "UPDATE %s SET c.a = 0 WHERE a = 0")

        with create_table(cql, test_keyspace, f"(a int PRIMARY KEY, b {myType}, c int)") as table:
            # bad deletions on non-frozen UDTs
            assert_invalid_message(cql, table, "UDT column b does not have a field named foo", "DELETE b.foo FROM %s WHERE a = 0")

            # bad updates on non-frozen UDTs
            assert_invalid_message(cql, table, "UDT column b does not have a field named foo", "UPDATE %s SET b.foo = 0 WHERE a = 0")

            # bad insert on non-frozen UDTs
            assert_invalid_message(cql, table, "Unknown field 'foo' in value of user defined type", "INSERT INTO %s (a, b, c) VALUES (0, {a: 0, foo: 0}, 0)")
            # I'm not sure why, but the Python driver seems to hide this
            # error, so commenting out this test.
            #assert_invalid_message(cql, table, "Unknown field 'foo' in value of user defined type " + myType,
            #        "INSERT INTO %s (a, b, c) VALUES (0, ?, 0)", user_type("a", 0, "foo", 0))

    # non-frozen UDT with non-frozen nested collection
    with create_type(cql, test_keyspace, "(bar int, foo list<int>)") as myType2:
        assert_invalid_message(cql, test_keyspace, "Non-frozen UDTs with nested non-frozen collections are not supported",
                "CREATE TABLE " + test_keyspace + ".wrong (k int PRIMARY KEY, v " + myType2 + ")")

def testAlterUDT(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(a int)") as myType:
        with create_table(cql, test_keyspace, f"(a int PRIMARY KEY, b frozen<{myType}>)") as table:
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (1, ?)", user_type("a", 1))
            assert_rows(execute(cql, table, "SELECT b.a FROM %s"), [1])
            flush(cql, table)
            execute(cql, table, "ALTER TYPE " + myType + " ADD b int")
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (2, ?)", user_type("a", 2, "b", 2))
            for _ in before_and_after_flush(cql, table):
                assert_rows(execute(cql, table, "SELECT b.a, b.b FROM %s"),
                           [1, None],
                           [2, 2])

def testAlterNonFrozenUDT(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(a int, b text)") as myType:
        with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, v {myType})") as table:
            execute(cql, table, "INSERT INTO %s (k, v) VALUES (0, ?)", user_type("a", 1, "b", "abc"))
            for _ in before_and_after_flush(cql, table):
                assert_rows(execute(cql, table, "SELECT v FROM %s"), [user_type("a", 1, "b", "abc")])
                assert_rows(execute(cql, table, "SELECT v.a FROM %s"), [1])
                assert_rows(execute(cql, table, "SELECT v.b FROM %s"), ["abc"])
            execute(cql, table, "ALTER TYPE " + myType + " RENAME b TO foo")
            assert_rows(execute(cql, table, "SELECT v FROM %s"), [user_type("a", 1, "b", "abc")])
            assert_rows(execute(cql, table, "SELECT v.a FROM %s"), [1])
            assert_rows(execute(cql, table, "SELECT v.foo FROM %s"), ["abc"])

            execute(cql, table, "UPDATE %s SET v.foo = 'def' WHERE k = 0")
            assert_rows(execute(cql, table, "SELECT v FROM %s"), [user_type("a", 1, "foo", "def")])
            assert_rows(execute(cql, table, "SELECT v.a FROM %s"), [1])
            assert_rows(execute(cql, table, "SELECT v.foo FROM %s"), ["def"])

            execute(cql, table, "INSERT INTO %s (k, v) VALUES (0, ?)", user_type("a", 2, "foo", "def"))
            assert_rows(execute(cql, table, "SELECT v FROM %s"), [user_type("a", 2, "foo", "def")])
            assert_rows(execute(cql, table, "SELECT v.a FROM %s"), [2])
            assert_rows(execute(cql, table, "SELECT v.foo FROM %s"), ["def"])

            execute(cql, table, "ALTER TYPE " + myType + " ADD c int")
            assert_rows(execute(cql, table, "SELECT v FROM %s"), [user_type("a", 2, "foo", "def", "c", None)])
            assert_rows(execute(cql, table, "SELECT v.a FROM %s"), [2])
            assert_rows(execute(cql, table, "SELECT v.foo FROM %s"), ["def"])
            assert_rows(execute(cql, table, "SELECT v.c FROM %s"), [None])

            execute(cql, table, "INSERT INTO %s (k, v) VALUES (0, ?)", user_type("a", 3, "foo", "abc", "c", 0))
            for _ in before_and_after_flush(cql, table):
                assert_rows(execute(cql, table, "SELECT v FROM %s"), [user_type("a", 3, "foo", "abc", "c", 0)])
                assert_rows(execute(cql, table, "SELECT v.c FROM %s"), [0])

def testUDTWithUnsetValues(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(x int, y int)") as myType:
        with create_type(cql, test_keyspace, f"(a frozen<{myType}>)") as myOtherType:
            with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, v frozen<{myType}>, z frozen<{myOtherType}>)") as table:
                assert_invalid_message(cql, table, "unset",
                    "INSERT INTO %s (k, v) VALUES (10, {x:?, y:?})", 1, UNSET_VALUE)
                # Reproduces issue #9671:
                assert_invalid_message(cql, table, "unset",
                    "INSERT INTO %s (k, v, z) VALUES (10, {x:?, y:?}, {a:{x: ?, y: ?}})", 1, 1, 1, UNSET_VALUE)

def testAlteringUserTypeNestedWithinMap(cql, test_keyspace):
    columnTypePrefixes = ["frozen<map<text, ", "map<text, frozen<"]
    for columnTypePrefix in columnTypePrefixes:
        with create_type(cql, test_keyspace, "(a int)") as ut1:
            columnType = columnTypePrefix + ut1 + ">>"
            with create_table(cql, test_keyspace, f"(x int PRIMARY KEY, y {columnType})") as table:
                execute(cql, table, "INSERT INTO %s (x, y) VALUES(1, ?)", {"firstValue": user_type("a", 1)})
                assert_rows(execute(cql, table, "SELECT * FROM %s"), [1, {"firstValue": user_type("a", 1)}])
                flush(cql, table)
                execute(cql, table, "ALTER TYPE " + ut1 + " ADD b int")
                execute(cql, table, "INSERT INTO %s (x, y) VALUES(2, ?)", {"secondValue": user_type("a", 2, "b", 2)})
                execute(cql, table, "INSERT INTO %s (x, y) VALUES(3, ?)", {"thirdValue": user_type("a", 3, "b", None)})
                execute(cql, table, "INSERT INTO %s (x, y) VALUES(4, ?)", {"fourthValue": user_type("a", None, "b", 4)})
                for _ in before_and_after_flush(cql, table):
                    # The Cassandra test had an error here that I don't know
                    # how it wasn't caught (the Python version failed on
                    # Cassandra) - although firstValue was inserted before
                    # the b column was added to the user type, when we read
                    # it now, we get a b column as well (with a null value).
                    assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s"),
                        [1, {"firstValue": user_type("a", 1, "b", None)}],
                        [2, {"secondValue": user_type("a", 2, "b", 2)}],
                        [3, {"thirdValue": user_type("a", 3, "b", None)}],
                        [4, {"fourthValue": user_type("a", None, "b", 4)}])

def testAlteringUserTypeNestedWithinNonFrozenMap(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(a int)") as ut1:
        with create_table(cql, test_keyspace, f"(x int PRIMARY KEY, y map<text, frozen<{ut1}>>)") as table:
            execute(cql, table, "INSERT INTO %s (x, y) VALUES(1, {'firstValue': {a: 1}})")
            assert_rows(execute(cql, table, "SELECT * FROM %s"),
                   [1, {"firstValue": user_type("a", 1)}])
            flush(cql, table)
            execute(cql, table, "ALTER TYPE " + ut1 + " ADD b int")
            execute(cql, table, "UPDATE %s SET y['secondValue'] = {a: 2, b: 2} WHERE x = 1")
            for _ in before_and_after_flush(cql, table):
                assert_rows(execute(cql, table, "SELECT * FROM %s"),
                    [1, {"firstValue": user_type("a", 1, "b", None),
                         "secondValue": user_type("a", 2, "b", 2)}])

def testAlteringUserTypeNestedWithinSet(cql, test_keyspace):
    # test frozen and non-frozen collections
    columnTypePrefixes = ["frozen<set<", "set<frozen<"]
    for columnTypePrefix in columnTypePrefixes:
        with create_type(cql, test_keyspace, "(a int)") as ut1:
            columnType = columnTypePrefix + ut1 + ">>"
            with create_table(cql, test_keyspace, f"(x int PRIMARY KEY, y {columnType})") as table:
                execute(cql, table, "INSERT INTO %s (x, y) VALUES(1, ?)", {user_type("a", 1)})
                assert_rows(execute(cql, table, "SELECT * FROM %s"), [1, {user_type("a", 1)}])
                flush(cql, table)
                execute(cql, table, "ALTER TYPE " + ut1 + " ADD b int")
                execute(cql, table, "INSERT INTO %s (x, y) VALUES(2, ?)", {user_type("a", 2, "b", 2)})
                execute(cql, table, "INSERT INTO %s (x, y) VALUES(3, ?)", {user_type("a", 3, "b", None)})
                execute(cql, table, "INSERT INTO %s (x, y) VALUES(4, ?)", {user_type("a", None, "b", 4)})

                for _ in before_and_after_flush(cql, table):
                    assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s"),
                        [1, {user_type("a", 1, "b", None)}],
                        [2, {user_type("a", 2, "b", 2)}],
                        [3, {user_type("a", 3, "b", None)}],
                        [4, {user_type("a", None, "b", 4)}])

def testAlteringUserTypeNestedWithinList(cql, test_keyspace):
    # test frozen and non-frozen collections
    columnTypePrefixes = {"frozen<list<", "list<frozen<"}
    for columnTypePrefix in columnTypePrefixes:
        with create_type(cql, test_keyspace, "(a int)") as ut1:
            columnType = columnTypePrefix + ut1 + ">>"
            with create_table(cql, test_keyspace, f"(x int PRIMARY KEY, y {columnType})") as table:
                execute(cql, table, "INSERT INTO %s (x, y) VALUES(1, ?)", [user_type("a", 1)])
                assert_rows(execute(cql, table, "SELECT * FROM %s"), [1, [user_type("a", 1)]])
                flush(cql, table)
                execute(cql, table, "ALTER TYPE " + ut1 + " ADD b int")
                execute(cql, table, "INSERT INTO %s (x, y) VALUES (2, ?)", [user_type("a", 2, "b", 2)])
                execute(cql, table, "INSERT INTO %s (x, y) VALUES (3, ?)", [user_type("a", 3, "b", None)])
                execute(cql, table, "INSERT INTO %s (x, y) VALUES (4, ?)", [user_type("a", None, "b", 4)])

                for _ in before_and_after_flush(cql, table):
                    assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s"),
                        [1, [user_type("a", 1, "b", None)]],
                        [2, [user_type("a", 2, "b", 2)]],
                        [3, [user_type("a", 3, "b", None)]],
                        [4, [user_type("a", None, "b", 4)]])

def testAlteringUserTypeNestedWithinTuple(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(a int, b int)") as ut1:
        with create_table(cql, test_keyspace, f"(a int PRIMARY KEY, b frozen<tuple<int, {ut1}>>)") as table:
            execute(cql, table, "INSERT INTO %s (a, b) VALUES(1, (1, ?))", user_type("a", 1, "b", 1))
            assert_rows(execute(cql, table, "SELECT * FROM %s"), [1, (1, user_type("a", 1, "b", 1))])
            flush(cql, table)
            execute(cql, table, "ALTER TYPE " + ut1 + " ADD c int")
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (2, (2, ?))", user_type("a", 2, "b", 2, "c", 2))
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (3, (3, ?))", user_type("a", 3, "b", 3, "c", None))
            execute(cql, table, "INSERT INTO %s (a, b) VALUES (4, (4, ?))", user_type("a", None, "b", 4, "c", None))
            for _ in before_and_after_flush(cql, table):
                assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s"),
                    [1, (1, user_type("a", 1, "b", 1, "c", None))],
                    [2, (2, user_type("a", 2, "b", 2, "c", 2))],
                    [3, (3, user_type("a", 3, "b", 3, "c", None))],
                    [4, (4, user_type("a", None, "b", 4, "c", None))])

def testAlteringUserTypeNestedWithinNestedTuple(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(a int, b int)") as ut1:
        with create_table(cql, test_keyspace, f"(a int PRIMARY KEY, b frozen<tuple<int, tuple<int, {ut1}>>>)") as table:
            execute(cql, table, "INSERT INTO %s (a, b) VALUES(1, (1, (1, ?)))", user_type("a", 1, "b", 1))
            assert_rows(execute(cql, table, "SELECT * FROM %s"), [1, (1, (1, user_type("a", 1, "b", 1)))])
            flush(cql, table)
            execute(cql, table, "ALTER TYPE " + ut1 + " ADD c int")
            execute(cql, table, "INSERT INTO %s (a, b) VALUES(2, (2, (1, ?)))", user_type("a", 2, "b", 2, "c", 2))
            execute(cql, table, "INSERT INTO %s (a, b) VALUES(3, (3, ?))", (1, user_type("a", 3, "b", 3, "c", None)))
            execute(cql, table, "INSERT INTO %s (a, b) VALUES(4, ?)", (4, (1, user_type("a", None, "b", 4, "c", None))))
            for _ in before_and_after_flush(cql, table):
                assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s"),
                    [1, (1, (1, user_type("a", 1, "b", 1, "c", None)))],
                    [2, (2, (1, user_type("a", 2, "b", 2, "c", 2)))],
                    [3, (3, (1, user_type("a", 3, "b", 3, "c", None)))],
                    [4, (4, (1, user_type("a", None, "b", 4, "c", None)))])

def testAlteringUserTypeNestedWithinUserType(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(a int, b int)") as ut1:
        with create_type(cql, test_keyspace, f"(x frozen<{ut1}>)") as otherType:
            with create_table(cql, test_keyspace, f"(a int PRIMARY KEY, b frozen<{otherType}>)") as table:
                execute(cql, table, "INSERT INTO %s (a, b) VALUES(1, {x: ?})", user_type("a", 1, "b", 1))
                assert_rows(execute(cql, table, "SELECT b.x.a, b.x.b FROM %s"), [1, 1])
                execute(cql, table, "INSERT INTO %s (a, b) VALUES(1, ?)", user_type("x", user_type("a", 1, "b", 1)))
                assert_rows(execute(cql, table, "SELECT b.x.a, b.x.b FROM %s"), [1, 1])
                flush(cql, table)
                execute(cql, table, "ALTER TYPE " + ut1 + " ADD c int")
                execute(cql, table, "INSERT INTO %s (a, b) VALUES(2, {x: ?})", user_type("a", 2, "b", 2, "c", 2))
                execute(cql, table, "INSERT INTO %s (a, b) VALUES(3, {x: ?})", user_type("a", 3, "b", 3, "c", None))
                execute(cql, table, "INSERT INTO %s (a, b) VALUES(4, {x: ?})", user_type("a", None, "b", 4, "c", None))
                for _ in before_and_after_flush(cql, table):
                    assert_rows_ignoring_order(execute(cql, table, "SELECT b.x.a, b.x.b, b.x.c FROM %s"),
                       [1, 1, None],
                       [2, 2, 2],
                       [3, 3, None],
                       [None, 4, None])

# Migrated from cql_tests.py:TestCQL.user_types_test()
def testUserTypes(cql, test_keyspace):
    userID_1 = UUID("550e8400-e29b-41d4-a716-446655440000")
    with create_type(cql, test_keyspace, "(street text, city text, zip_code int, phones set<text >)") as addressType:
        with create_type(cql, test_keyspace, f"(firstname text, lastname text)") as nameType:
            with create_table(cql, test_keyspace, f"(id uuid PRIMARY KEY, name frozen<{nameType}>, addresses map < text, frozen < {addressType} >>)") as table:
                execute(cql, table, "INSERT INTO %s (id, name) VALUES(?, { firstname: 'Paul', lastname: 'smith' } )", userID_1)

                assert_rows(execute(cql, table, "SELECT name.firstname FROM %s WHERE id = ?", userID_1), ["Paul"])

                execute(cql, table, "UPDATE %s SET addresses = addresses + { 'home': { street: '...', city:'SF', zip_code:94102, phones:{ } } } WHERE id = ?", userID_1)
                # TODO: deserialize the value here and check it 's right.
                execute(cql, table, "SELECT addresses FROM %s WHERE id = ? ", userID_1)

# Test user type test that does a little more nesting,
# migrated from cql_tests.py:TestCQL.more_user_types_test()
def testNestedUserTypes(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(s set<text>, m map<text, text>, l list<text>)") as type1:
        with create_type(cql, test_keyspace, f"(s set <frozen <{type1}>>,)") as type2:
            with create_table(cql, test_keyspace, f"(id int PRIMARY KEY, val frozen<{type2}>)") as table:
                execute(cql, table, "INSERT INTO %s (id, val) VALUES (0, ?)",
                    user_type("s", [user_type("s", {"foo", "bar"}, "m", {"foo": "bar"}, "l", ["foo", "bar"])]))
                assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s"),
                    [0, (frozenset({(frozenset({'bar', 'foo'}), frozenset({('foo', 'bar')}), ('foo', 'bar'))}),)])

# Migrated from cql_tests.py:TestCQL.add_field_to_udt_test()
def testAddFieldToUdt(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(fooint int, fooset set<text>)") as typeName:
        with create_table(cql, test_keyspace, f"(key int PRIMARY KEY, data frozen<{typeName}>)") as table:
            execute(cql, table, "INSERT INTO %s (key, data) VALUES (1, ?)", user_type("fooint", 1, "fooset", frozenset({"2"})))
            execute(cql, table, "ALTER TYPE " + typeName + " ADD foomap map <int,text>")
            execute(cql, table, "INSERT INTO %s (key, data) VALUES (1, ?)", user_type("fooint", 1, "fooset", {"2"}, "foomap", {3: "bar"}))
            assert_rows_ignoring_order(execute(cql, table, "SELECT * FROM %s"),
                [1, (1, frozenset({'2'}), frozenset({(3, 'bar')}))])

def testCircularReferences(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(foo int)") as type1:
        with create_type(cql, test_keyspace, f"(bar frozen<{type1}>)") as typeX:
            assert_invalid_message(cql, test_keyspace, "would create a circular reference", "ALTER TYPE " + type1 + " ADD needs_to_fail frozen<" + typeX + '>')

        with create_type(cql, test_keyspace, f"(bar frozen<list<{type1}>>)") as typeX:
            assert_invalid_message(cql, test_keyspace, "would create a circular reference", "ALTER TYPE " + type1 + " ADD needs_to_fail frozen<" + typeX + '>')

        with create_type(cql, test_keyspace, f"(bar frozen<set<{type1}>>)") as typeX:
            assert_invalid_message(cql, test_keyspace, "would create a circular reference", "ALTER TYPE " + type1 + " ADD needs_to_fail frozen<" + typeX + '>')

        with create_type(cql, test_keyspace, f"(bar frozen<map<text, {type1}>>)") as typeX:
            assert_invalid_message(cql, test_keyspace, "would create a circular reference", "ALTER TYPE " + type1 + " ADD needs_to_fail frozen<" + typeX + '>')

        with create_type(cql, test_keyspace, f"(bar frozen<map<{type1}, text>>)") as typeX:
            assert_invalid_message(cql, test_keyspace, "would create a circular reference", "ALTER TYPE " + type1 + " ADD needs_to_fail frozen<" + typeX + '>')

        ##
        with create_type(cql, test_keyspace, f"(foo frozen<{type1}>)") as type2:
            with create_type(cql, test_keyspace, f"(bar frozen<{type2}>)") as typeX:
                assert_invalid_message(cql, test_keyspace, "would create a circular reference", "ALTER TYPE " + type1 + " ADD needs_to_fail frozen<" + typeX + '>')
            with create_type(cql, test_keyspace, f"(bar frozen<list<{type2}>>)") as typeX:
                assert_invalid_message(cql, test_keyspace, "would create a circular reference", "ALTER TYPE " + type1 + " ADD needs_to_fail frozen<" + typeX + '>')
            with create_type(cql, test_keyspace, f"(bar frozen<set<{type2}>>)") as typeX:
                assert_invalid_message(cql, test_keyspace, "would create a circular reference", "ALTER TYPE " + type1 + " ADD needs_to_fail frozen<" + typeX + '>')
            with create_type(cql, test_keyspace, f"(bar frozen<map<text, {type2}>>)") as typeX:
                assert_invalid_message(cql, test_keyspace, "would create a circular reference", "ALTER TYPE " + type1 + " ADD needs_to_fail frozen<" + typeX + '>')
            with create_type(cql, test_keyspace, f"(bar frozen<map<{type2}, text>>)") as typeX:
                assert_invalid_message(cql, test_keyspace, "would create a circular reference", "ALTER TYPE " + type1 + " ADD needs_to_fail frozen<" + typeX + '>')
        ##
        assert_invalid_message(cql, test_keyspace, "would create a circular reference", "ALTER TYPE " + type1 + " ADD needs_to_fail frozen<list<" + type1 + '>>')

# The test testTypeAlterUsedInFunction was not translated, because it uses
# user-defined functions which differ between Scylla and Cassandra.

def testInsertNonFrozenUDT(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(a int, b text)") as typeName:
        with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, v {typeName})") as table:
            execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, {a: ?, b: ?})", 0, 0, "abc")
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ?", 0), [0, user_type("a", 0, "b", "abc")])

            execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", 0, user_type("a", 0, "b", "abc"))
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ?", 0), [0, user_type("a", 0, "b", "abc")])

            execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, {a: ?, b: ?})", 0, 0, None)
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ?", 0), [0, user_type("a", 0, "b", None)])

            execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", 0, user_type("a", None, "b", "abc"))
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ?", 0), [0, user_type("a", None, "b", "abc")])

def testUpdateNonFrozenUDT(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(a int, b text)") as typeName:
        with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, v {typeName})") as table:
            execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", 0, user_type("a", 0, "b", "abc"))
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ?", 0), [0, user_type("a", 0, "b", "abc")])

            # overwrite the whole UDT
            execute(cql, table, "UPDATE %s SET v = ? WHERE k = ?", user_type("a", 1, "b", "def"), 0)
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ?", 0), [0, user_type("a", 1, "b", "def")])

            execute(cql, table, "UPDATE %s SET v = ? WHERE k = ?", user_type("a", 0, "b", None), 0)
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ?", 0), [0, user_type("a", 0, "b", None)])

            execute(cql, table, "UPDATE %s SET v = ? WHERE k = ?", user_type("a", None, "b", "abc"), 0)
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ?", 0), [0, user_type("a", None, "b", "abc")])

            # individually set fields to non-null values
            execute(cql, table, "UPDATE %s SET v.a = ? WHERE k = ?", 1, 0)
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ?", 0), [0, user_type("a", 1, "b", "abc")])

            execute(cql, table, "UPDATE %s SET v.b = ? WHERE k = ?", "def", 0)
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ?", 0), [0, user_type("a", 1, "b", "def")])

            execute(cql, table, "UPDATE %s SET v.a = ?, v.b = ? WHERE k = ?", 0, "abc", 0)
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ?", 0), [0, user_type("a", 0, "b", "abc")])

            execute(cql, table, "UPDATE %s SET v.b = ?, v.a = ? WHERE k = ?", "abc", 0, 0)
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ?", 0), [0, user_type("a", 0, "b", "abc")])

            # individually set fields to null values
            execute(cql, table, "UPDATE %s SET v.a = ? WHERE k = ?", None, 0)
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ?", 0), [0, user_type("a", None, "b", "abc")])

            execute(cql, table, "UPDATE %s SET v.a = ? WHERE k = ?", 0, 0)
            execute(cql, table, "UPDATE %s SET v.b = ? WHERE k = ?", None, 0)
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ?", 0), [0, user_type("a", 0, "b", None)])

            execute(cql, table, "UPDATE %s SET v.a = ? WHERE k = ?", None, 0)
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ?", 0), [0, None])

def testDeleteNonFrozenUDT(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(a int, b text)") as typeName:
        with create_table(cql, test_keyspace, f"(k int PRIMARY KEY, v {typeName})") as table:
            execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", 0, user_type("a", 0, "b", "abc"))
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ?", 0), [0, user_type("a", 0, "b", "abc")])

            execute(cql, table, "DELETE v.b FROM %s WHERE k = ?", 0)
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ?", 0), [0, user_type("a", 0, "b", None)])

            execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", 0, user_type("a", 0, "b", "abc"))
            execute(cql, table, "DELETE v.a FROM %s WHERE k = ?", 0)
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ?", 0), [0, user_type("a", None, "b", "abc")])

            execute(cql, table, "DELETE v.b FROM %s WHERE k = ?", 0)
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ?", 0), [0, None])

            # delete both fields at once
            execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", 0, user_type("a", 0, "b", "abc"))
            execute(cql, table, "DELETE v.a, v.b FROM %s WHERE k = ?", 0)
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ?", 0), [0, None])

            # same, but reverse field order
            execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", 0, user_type("a", 0, "b", "abc"))
            execute(cql, table, "DELETE v.b, v.a FROM %s WHERE k = ?", 0)
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ?", 0), [0, None])

            # delete the whole thing at once
            execute(cql, table, "INSERT INTO %s (k, v) VALUES (?, ?)", 0, user_type("a", 0, "b", "abc"))
            execute(cql, table, "DELETE v FROM %s WHERE k = ?", 0)
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE k = ?", 0), [0, None])

            assert_invalid(cql, table, "DELETE v.bad FROM %s WHERE k = ?", 0)

def testReadAfterAlteringUserTypeNestedWithinSet(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(a int)") as columnType:
        with create_table(cql, test_keyspace, f"(x int PRIMARY KEY, y set<frozen<{columnType}>>)") as table:
            # FIXME: this was in the original test and wasn't translated:
            # disableCompaction();

            execute(cql, table, "INSERT INTO %s (x, y) VALUES(1, ?)", [user_type("a", 1), user_type("a", 2)])
            assert_rows(execute(cql, table, "SELECT * FROM %s"), [1, frozenset([user_type("a", 1), user_type("a", 2)])])
            flush(cql, table)
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE x = 1"),
                       [1, frozenset([user_type("a", 1), user_type("a", 2)])])

            execute(cql, table, "ALTER TYPE " + columnType + " ADD b int")
            execute(cql, table, "UPDATE %s SET y = y + ? WHERE x = 1",
                    [user_type("a", 1, "b", 1), user_type("a", 1, "b", 2), user_type("a", 2, "b", 1)])

            flush(cql, table)
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE x = 1"),
                           [1, {(1, None),
                                (1, 1),
                                (1, 2),
                                (2, None),
                                (2, 1)}])

            #compact();

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE x = 1"),
                           [1, {(1, None),
                                (1, 1),
                                (1, 2),
                                (2, None),
                                (2, 1)}])
            #finally
            #{
            #    enableCompaction();
            #}

def testReadAfterAlteringUserTypeNestedWithinMap(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(a int)") as columnType:
        with create_table(cql, test_keyspace, f"(x int PRIMARY KEY, y map<frozen<{columnType}>, int>)") as table:
            #disableCompaction();
            execute(cql, table, "INSERT INTO %s (x, y) VALUES(1, ?)", {user_type("a", 1): 1, user_type("a", 2): 2})
            assert_rows(execute(cql, table, "SELECT * FROM %s"), [1, {user_type("a", 1): 1, user_type("a", 2): 2}])
            flush(cql, table)

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE x = 1"),
                       [1, {user_type("a", 1): 1, user_type("a", 2): 2}])

            execute(cql, table, "ALTER TYPE " + columnType + " ADD b int")
            execute(cql, table, "UPDATE %s SET y = y + ? WHERE x = 1",
                    {user_type("a", 1, "b", 1): 1, user_type("a", 1, "b", 2): 1, user_type("a", 2, "b", 1): 2})

            flush(cql, table)
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE x = 1"),
                           [1, {user_type("a", 1, "b", None): 1,
                                      user_type("a", 1, "b", 1): 1,
                                      user_type("a", 1, "b", 2): 1,
                                      user_type("a", 2, "b", None): 2,
                                      user_type("a", 2, "b", 1): 2}])

            #compact();

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE x = 1"),
                           [1, {user_type("a", 1, "b", None): 1,
                                      user_type("a", 1, "b", 1): 1,
                                      user_type("a", 1, "b", 2): 1,
                                      user_type("a", 2, "b", None): 2,
                                      user_type("a", 2, "b", 1): 2}])
            #finally
            #{
            #enableCompaction();
            #}

def testReadAfterAlteringUserTypeNestedWithinList(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(a int)") as columnType:
        with create_table(cql, test_keyspace, f"(x int PRIMARY KEY, y list<frozen<{columnType}>>)") as table:
            #disableCompaction();
            execute(cql, table, "INSERT INTO %s (x, y) VALUES(1, ?)", [user_type("a", 1), user_type("a", 2)])
            assert_rows(execute(cql, table, "SELECT * FROM %s"), [1, [user_type("a", 1), user_type("a", 2)]])
            flush(cql, table)

            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE x = 1"),
                       [1, [user_type("a", 1), user_type("a", 2)]])

            execute(cql, table, "ALTER TYPE " + columnType + " ADD b int")
            execute(cql, table, "UPDATE %s SET y = y + ? WHERE x = 1",
                    [user_type("a", 1, "b", 1), user_type("a", 1, "b", 2), user_type("a", 2, "b", 1)])

            flush(cql, table)
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE x = 1"),
                           [1, [user_type("a", 1, "b", None),
                                       user_type("a", 2, "b", None),
                                       user_type("a", 1, "b", 1),
                                       user_type("a", 1, "b", 2),
                                       user_type("a", 2, "b", 1)]])
            #compact();
            assert_rows(execute(cql, table, "SELECT * FROM %s WHERE x = 1"),
                           [1, [user_type("a", 1, "b", None),
                                       user_type("a", 2, "b", None),
                                       user_type("a", 1, "b", 1),
                                       user_type("a", 1, "b", 2),
                                       user_type("a", 2, "b", 1)]])
            #finally
            #{
            #    enableCompaction();
            #}

def testAlteringUserTypeNestedWithinSetWithView(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(a int)") as columnType:
        with create_table(cql, test_keyspace, f"(pk int, c int, v int, s set<frozen<{columnType}>>, PRIMARY KEY (pk, c))") as table:
            with create_materialized_view(cql, test_keyspace, f"AS SELECT c, pk, v FROM {table} WHERE pk IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, pk)") as mv:
                execute(cql, table, "INSERT INTO %s (pk, c, v, s) VALUES(?, ?, ?, ?)", 1, 1, 1, {user_type("a", 1), user_type("a", 2)})
                flush(cql, table)
                execute(cql, table, "ALTER TYPE " + columnType + " ADD b int")
                execute(cql, table, "UPDATE %s SET s = s + ?, v = ? WHERE pk = ? AND c = ?",
                    {user_type("a", 1, "b", 1), user_type("a", 1, "b", 2), user_type("a", 2, "b", 1)}, 2, 1, 1)

                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = ? AND c = ?", 1, 1),
                       [1, 1, {user_type("a", 1, "b", None), user_type("a", 1, "b", 1), user_type("a", 1, "b", 2), user_type("a", 2, "b", None), user_type("a", 2, "b", 1)}, 2])

def testSyntaxExceptions(cql, test_keyspace):
    with pytest.raises(SyntaxException):
        cql.execute('CREATE TYPE "" (a int, b int)')
    with pytest.raises(SyntaxException):
        cql.execute('CREATE TYPE mytype ("" int, b int)')
    with create_type(cql, test_keyspace, "(a int, b int)") as columnType:
        with pytest.raises(SyntaxException):
            cql.execute(f'ALTER TYPE {columnType} RENAME b to ""')
