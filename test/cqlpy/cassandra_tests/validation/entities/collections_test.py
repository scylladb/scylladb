# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 6ca34f81386dc8f6020cdf2ea4246bca2a0896c5
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0

from cassandra_tests.porting import *
from cassandra.query import UNSET_VALUE

from uuid import UUID
import random
import time
import string
import collections

def testMapBulkRemoval(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, m map<text, text>)") as table:
        execute(cql, table, "INSERT INTO %s(k, m) VALUES (?, ?)", 0, {"k1": "v1", "k2": "v2", "k3": "v3"})
        assert_rows(execute(cql, table, "SELECT * FROM %s"),
            [0, {"k1": "v1", "k2": "v2", "k3": "v3"}])

        execute(cql, table, "UPDATE %s SET m = m - ? WHERE k = ?", {"k2"}, 0)

        assert_rows(execute(cql, table, "SELECT * FROM %s"),
            [0, {"k1": "v1", "k3": "v3"}])

        execute(cql, table, "UPDATE %s SET m = m + ?, m = m - ? WHERE k = ?", {"k4": "v4"}, {"k3"}, 0)

        assert_rows(execute(cql, table, "SELECT * FROM %s"),
            [0, {"k1": "v1", "k4": "v4"}])

def testInvalidCollectionsMix(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, l list<text>, s set<text>, m map<text, text>)") as table:
        # Unfortunately, we cannot test sending the wrong type as a binding
        # for a prepared statement, because the Python driver never sends
        # the wrong type when binding a list. Sometimes it succeeds to do a
        # weird conversion (e.g., it implicitly converts a map to the desired
        # list type by taking only the keys), and in other cases it complains
        # that it can't do the conversion. But in any case it prevents us from
        # feeding the wrong type into the prepared statement. So all these
        # tests need to use non-prepared statements.
        assert_invalid(cql, table, "UPDATE %s SET l = l + { 'a', 'b' } WHERE k = 0")
        assert_invalid(cql, table, "UPDATE %s SET l = l - { 'a', 'b' } WHERE k = 0")
        assert_invalid(cql, table, "UPDATE %s SET l = l + { 'a': 'b', 'c': 'd' } WHERE k = 0")
        assert_invalid(cql, table, "UPDATE %s SET l = l - { 'a': 'b', 'c': 'd' } WHERE k = 0")

        assert_invalid(cql, table, "UPDATE %s SET s = s + [ 'a', 'b' ] WHERE k = 0")
        assert_invalid(cql, table, "UPDATE %s SET s = s - [ 'a', 'b' ] WHERE k = 0")
        assert_invalid(cql, table, "UPDATE %s SET s = s + { 'a': 'b', 'c': 'd'} WHERE k = 0")
        assert_invalid(cql, table, "UPDATE %s SET s = s - { 'a': 'b', 'c': 'd'} WHERE k = 0")

        assert_invalid(cql, table, "UPDATE %s SET m = m + [ 'a', 'b' ] WHERE k = 0")
        assert_invalid(cql, table, "UPDATE %s SET m = m - [ 'a', 'b' ] WHERE k = 0")
        assert_invalid(cql, table, "UPDATE %s SET m = m + { 'a', 'b' } WHERE k = 0")
        assert_invalid(cql, table, "UPDATE %s SET m = m - { 'a': 'b', 'c': 'd' } WHERE k = 0")


@pytest.mark.xfail(reason="Cassandra 3.10's += syntax not yet supported. Issue #7735")
def testSets(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, s set<text>)") as table:
        execute(cql, table, "INSERT INTO %s(k, s) VALUES (0, ?)", {"v1", "v2", "v3", "v4"})

        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k = 0"),
            [{"v1", "v2", "v3", "v4"}])

        execute(cql, table, "DELETE s[?] FROM %s WHERE k = 0", "v1")

        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k = 0"),
                   [{"v2", "v3", "v4"}])

        # Full overwrite
        execute(cql, table, "UPDATE %s SET s = ? WHERE k = 0", {"v6", "v5"})

        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k = 0"),
                   [{"v5", "v6"}])

        execute(cql, table, "UPDATE %s SET s = s + ? WHERE k = 0", {"v7"})

        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k = 0"),
                   [{"v5", "v6", "v7"}])

        execute(cql, table, "UPDATE %s SET s = s - ? WHERE k = 0", {"v6", "v5"})

        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k = 0"),
                   [{"v7"}])

        execute(cql, table, "UPDATE %s SET s += ? WHERE k = 0", {"v5"})
        execute(cql, table, "UPDATE %s SET s += {'v6'} WHERE k = 0")

        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k = 0"),
                   [{"v5", "v6", "v7"}])

        execute(cql, table, "UPDATE %s SET s -= ? WHERE k = 0", {"v5"})
        execute(cql, table, "UPDATE %s SET s -= {'v6'} WHERE k = 0")

        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k = 0"),
                   [{"v7"}])

        execute(cql, table, "DELETE s[?] FROM %s WHERE k = 0", "v7")

        # Deleting an element that does not exist will succeed
        execute(cql, table, "DELETE s[?] FROM %s WHERE k = 0", "v7")

        execute(cql, table, "DELETE s FROM %s WHERE k = 0")

        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k = 0"),
                   [None])

@pytest.mark.xfail(reason="Cassandra 3.10's += syntax not yet supported. Issue #7735")
def testMaps(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, m map<text, int>)") as table:
        # Cassandra's and Scylla's messages are completely different here. Cassandra has
        # "Value for a map addition has to be a map, but was: '{1}'", Scylla has
        # "Invalid set literal for m of type map<text, int>".
        # The only thing in common is the word "map"...
        assert_invalid_message(cql, table, "map",
                             "UPDATE %s SET m = m + {1} WHERE k = 0;")
        # As noted above, the CQL driver prevents us from sending mismatched
        # types for prepared statement binding, so we comment out these tests
        #assert_invalid_message(cql, table, "Not enough bytes to read a map",
        #                     "UPDATE %s SET m += ? WHERE k = 0", {"v1"})
        # Cassandra's and Scylla's messages are completely different here. Cassandra has
        # "Value for a map subtraction has to be a set, but was: '{'v1': 1}'", Scylla has
        # "Invalid map literal for m of type frozen<set<text>".
        assert_invalid_message(cql, table, "map",
                             "UPDATE %s SET m = m - {'v1': 1} WHERE k = 0", {"v1": 1})
        #assert_invalid_message(cql, table, "Unexpected extraneous bytes after set value",
        #                     "UPDATE %s SET m -= ? WHERE k = 0", {"v1": 1})

        execute(cql, table, "INSERT INTO %s(k, m) VALUES (0, ?)", {"v1": 1, "v2": 2})

        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 0"),
            [{"v1": 1, "v2": 2}])

        execute(cql, table, "UPDATE %s SET m[?] = ?, m[?] = ? WHERE k = 0", "v3", 3, "v4", 4)

        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 0"),
            [{"v1": 1, "v2": 2, "v3": 3, "v4": 4}])

        execute(cql, table, "DELETE m[?] FROM %s WHERE k = 0", "v1")

        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 0"),
            [{"v2": 2, "v3": 3, "v4": 4}])

        # Full overwrite
        execute(cql, table, "UPDATE %s SET m = ? WHERE k = 0", {"v6": 6, "v5": 5})

        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 0"),
                   [{"v5": 5, "v6": 6}])

        execute(cql, table, "UPDATE %s SET m = m + ? WHERE k = 0", {"v7": 7})

        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 0"),
                   [{"v5": 5, "v6": 6, "v7": 7}])

        execute(cql, table, "UPDATE %s SET m = m - ? WHERE k = 0", {"v7"})

        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 0"),
                   [{"v5": 5, "v6": 6}])

        execute(cql, table, "UPDATE %s SET m += ? WHERE k = 0", {"v7": 7})

        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 0"),
                   [{"v5": 5, "v6": 6, "v7": 7}])

        execute(cql, table, "UPDATE %s SET m -= ? WHERE k = 0", {"v7"})

        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 0"),
                   [{"v5": 5, "v6": 6}])

        execute(cql, table, "UPDATE %s SET m += {'v7': 7} WHERE k = 0")

        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 0"),
                   [{"v5": 5, "v6": 6, "v7": 7}])

        execute(cql, table, "UPDATE %s SET m -= {'v7'} WHERE k = 0")

        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 0"),
                   [{"v5": 5, "v6": 6}])

        execute(cql, table, "DELETE m[?] FROM %s WHERE k = 0", "v6")

        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 0"),
                   [{"v5": 5}])

        execute(cql, table, "DELETE m[?] FROM %s WHERE k = 0", "v5")

        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 0"),
                   [None])

        # Deleting a non-existing key should succeed
        execute(cql, table, "DELETE m[?] FROM %s WHERE k = 0", "v5")

        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 0"),
                   [None])

        # The empty map is parsed as an empty set (because we don't have enough info at parsing
        # time when we see a {}) and special cased later. This test checks this work properly
        execute(cql, table, "UPDATE %s SET m = {} WHERE k = 0")

        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 0"),
            [None])

@pytest.mark.xfail(reason="Cassandra 3.10's += syntax not yet supported. Issue #7735")
def testLists(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, l list<text>)") as table:
        execute(cql, table, "INSERT INTO %s(k, l) VALUES (0, ?)", ["v1", "v2", "v3"])

        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 0"), [["v1", "v2", "v3"]])

        execute(cql, table, "DELETE l[?] FROM %s WHERE k = 0", 1)

        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 0"), [["v1", "v3"]])

        execute(cql, table, "UPDATE %s SET l[?] = ? WHERE k = 0", 1, "v4")

        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 0"), [["v1", "v4"]])

        # Full overwrite
        execute(cql, table, "UPDATE %s SET l = ? WHERE k = 0", ["v6", "v5"])

        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 0"), [["v6", "v5"]])

        execute(cql, table, "UPDATE %s SET l = l + ? WHERE k = 0", ["v7", "v8"])

        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 0"), [["v6", "v5", "v7", "v8"]])

        execute(cql, table, "UPDATE %s SET l = ? + l WHERE k = 0", ["v9"])

        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 0"), [["v9", "v6", "v5", "v7", "v8"]])

        execute(cql, table, "UPDATE %s SET l = l - ? WHERE k = 0", ["v5", "v8"])

        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 0"), [["v9", "v6", "v7"]])

        execute(cql, table, "UPDATE %s SET l += ? WHERE k = 0", ["v8"])

        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 0"), [["v9", "v6", "v7", "v8"]])

        execute(cql, table, "UPDATE %s SET l -= ? WHERE k = 0", ["v6", "v8"])

        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 0"), [["v9", "v7"]])

        execute(cql, table, "DELETE l FROM %s WHERE k = 0")

        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 0"), [None])

        assert_invalid_message(cql, table, "Attempted to delete an element from a list which is null",
                             "DELETE l[0] FROM %s WHERE k=0 ")

        assert_invalid_message(cql, table, "Attempted to set an element on a list which is null",
                             "UPDATE %s SET l[0] = ? WHERE k=0", "v10")

        execute(cql, table, "UPDATE %s SET l = l - ? WHERE k=0", ["v11"])

        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 0"), [None]);

        execute(cql, table, "UPDATE %s SET l = l + ? WHERE k = 0", ["v1", "v2", "v1", "v2", "v1", "v2"])
        execute(cql, table, "UPDATE %s SET l = l - ? WHERE k=0", ["v1", "v2"])
        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 0"), [None]);

def testMapWithUnsetValues(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, m map<text, text>)") as table:
        # set up
        m = {"k": "v"}
        execute(cql, table, "INSERT INTO %s (k, m) VALUES (10, ?)", m)
        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 10"), [m])

        # test putting an unset map, should not delete the contents
        execute(cql, table, "INSERT INTO %s (k, m) VALUES (10, ?)", UNSET_VALUE)
        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 10"), [m])
        # test unset variables in a map update operation, should not delete the contents
        execute(cql, table, "UPDATE %s SET m['k'] = ? WHERE k = 10", UNSET_VALUE)
        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 10"), [m])
        assert_invalid_message(cql, table, "unset", "UPDATE %s SET m[?] = 'foo' WHERE k = 10", UNSET_VALUE)

        # test unset value for map key
        assert_invalid_message(cql, table, "unset", "DELETE m[?] FROM %s WHERE k = 10", UNSET_VALUE)

def testListWithUnsetValues(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, l list<text>)") as table:
        # set up
        l = ["foo", "foo"]
        execute(cql, table, "INSERT INTO %s (k, l) VALUES (10, ?)", l)
        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 10"), [l])

        # replace list with unset value
        execute(cql, table, "INSERT INTO %s (k, l) VALUES (10, ?)", UNSET_VALUE)
        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 10"), [l])

        # add to position
        execute(cql, table, "UPDATE %s SET l[1] = ? WHERE k = 10", UNSET_VALUE)
        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 10"), [l])

        # set in index
        assert_invalid_message(cql, table, "unset value", "UPDATE %s SET l[?] = 'foo' WHERE k = 10", UNSET_VALUE)

        # remove element by index
        execute(cql, table, "DELETE l[?] FROM %s WHERE k = 10", UNSET_VALUE)
        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 10"), [l])

        # remove all occurrences of element
        execute(cql, table, "UPDATE %s SET l = l - ? WHERE k = 10", UNSET_VALUE)
        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 10"), [l])

        # select with in clause
        assert_invalid_message(cql, table, "unset value", "SELECT * FROM %s WHERE k IN ?", UNSET_VALUE)
        # The following cannot be tested with the Python driver because it
        # captures this case before sending it to the server.
        #assert_invalid_message(cql, table, "unset value", "SELECT * FROM %s WHERE k IN (?)", UNSET_VALUE)

def testSetWithUnsetValues(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, s set<text>)") as table:
        s = {"bar", "baz", "foo"}
        execute(cql, table, "INSERT INTO %s (k, s) VALUES (10, ?)", s)
        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k = 10"), [s])

        # replace set with unset value
        execute(cql, table, "INSERT INTO %s (k, s) VALUES (10, ?)", UNSET_VALUE)
        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k = 10"), [s])

        # add to set
        execute(cql, table, "UPDATE %s SET s = s + ? WHERE k = 10", UNSET_VALUE)
        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k = 10"), [s]) 

        # remove all occurrences of element
        execute(cql, table, "UPDATE %s SET s = s - ? WHERE k = 10", UNSET_VALUE)
        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k = 10"), [s])

# Migrated from cql_tests.py:TestCQL.set_test()
def testSet(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(fn text, ln text, tags set<text>, PRIMARY KEY (fn, ln))") as table:
        execute(cql, table, "UPDATE %s SET tags = tags + { 'foo' } WHERE fn='Tom' AND ln='Bombadil'")
        execute(cql, table, "UPDATE %s SET tags = tags + { 'bar' } WHERE fn='Tom' AND ln='Bombadil'")
        execute(cql, table, "UPDATE %s SET tags = tags + { 'foo' } WHERE fn='Tom' AND ln='Bombadil'")
        execute(cql, table, "UPDATE %s SET tags = tags + { 'foobar' } WHERE fn='Tom' AND ln='Bombadil'")
        execute(cql, table, "UPDATE %s SET tags = tags - { 'bar' } WHERE fn='Tom' AND ln='Bombadil'")

        assert_rows(execute(cql, table, "SELECT tags FROM %s"), [{"foo", "foobar"}])

        execute(cql, table, "UPDATE %s SET tags = { 'a', 'c', 'b' } WHERE fn='Bilbo' AND ln='Baggins'")
        assert_rows(execute(cql, table, "SELECT tags FROM %s WHERE fn='Bilbo' AND ln='Baggins'"),
                   [{"a", "b", "c"}])

        execute(cql, table, "UPDATE %s SET tags = { 'm', 'n' } WHERE fn='Bilbo' AND ln='Baggins'")
        assert_rows(execute(cql, table, "SELECT tags FROM %s WHERE fn='Bilbo' AND ln='Baggins'"),
                   [{"m", "n"}])

        execute(cql, table, "DELETE tags['m'] FROM %s WHERE fn='Bilbo' AND ln='Baggins'")
        assert_rows(execute(cql, table, "SELECT tags FROM %s WHERE fn='Bilbo' AND ln='Baggins'"),
                   [{"n"}])

        execute(cql, table, "DELETE tags FROM %s WHERE fn='Bilbo' AND ln='Baggins'")
        assert_empty(execute(cql, table, "SELECT tags FROM %s WHERE fn='Bilbo' AND ln='Baggins'"))

# Migrated from cql_tests.py:TestCQL.map_test()
def testMap(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(fn text, ln text, m map<text, int>, PRIMARY KEY (fn, ln))") as table:
        execute(cql, table, "UPDATE %s SET m['foo'] = 3 WHERE fn='Tom' AND ln='Bombadil'")
        execute(cql, table, "UPDATE %s SET m['bar'] = 4 WHERE fn='Tom' AND ln='Bombadil'")
        execute(cql, table, "UPDATE %s SET m['woot'] = 5 WHERE fn='Tom' AND ln='Bombadil'")
        execute(cql, table, "UPDATE %s SET m['bar'] = 6 WHERE fn='Tom' AND ln='Bombadil'")
        execute(cql, table, "DELETE m['foo'] FROM %s WHERE fn='Tom' AND ln='Bombadil'")

        assert_rows(execute(cql, table, "SELECT m FROM %s"), [{"bar": 6, "woot": 5}])

        execute(cql, table, "UPDATE %s SET m = { 'a' : 4 , 'c' : 3, 'b' : 2 } WHERE fn='Bilbo' AND ln='Baggins'")
        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE fn='Bilbo' AND ln='Baggins'"),
                   [{"a": 4, "b": 2, "c": 3}])

        execute(cql, table, "UPDATE %s SET m =  { 'm' : 4 , 'n' : 1, 'o' : 2 } WHERE fn='Bilbo' AND ln='Baggins'")
        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE fn='Bilbo' AND ln='Baggins'"),
                   [{"m": 4, "n": 1, "o": 2}])

        execute(cql, table, "UPDATE %s SET m = {} WHERE fn='Bilbo' AND ln='Baggins'")
        assert_empty(execute(cql, table, "SELECT m FROM %s WHERE fn='Bilbo' AND ln='Baggins'"))

# Migrated from cql_tests.py:TestCQL.list_test()
def testList(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(fn text, ln text, tags list<text>, PRIMARY KEY (fn, ln))") as table:
        execute(cql, table, "UPDATE %s SET tags = tags + [ 'foo' ] WHERE fn='Tom' AND ln='Bombadil'")
        execute(cql, table, "UPDATE %s SET tags = tags + [ 'bar' ] WHERE fn='Tom' AND ln='Bombadil'")
        execute(cql, table, "UPDATE %s SET tags = tags + [ 'foo' ] WHERE fn='Tom' AND ln='Bombadil'")
        execute(cql, table, "UPDATE %s SET tags = tags + [ 'foobar' ] WHERE fn='Tom' AND ln='Bombadil'")

        assert_rows(execute(cql, table, "SELECT tags FROM %s"),
                   [["foo", "bar", "foo", "foobar"]])

        execute(cql, table, "UPDATE %s SET tags = [ 'a', 'c', 'b', 'c' ] WHERE fn='Bilbo' AND ln='Baggins'")
        assert_rows(execute(cql, table, "SELECT tags FROM %s WHERE fn='Bilbo' AND ln='Baggins'"),
                   [["a", "c", "b", "c"]])

        execute(cql, table, "UPDATE %s SET tags = [ 'm', 'n' ] + tags WHERE fn='Bilbo' AND ln='Baggins'")
        assert_rows(execute(cql, table, "SELECT tags FROM %s WHERE fn='Bilbo' AND ln='Baggins'"),
                   [["m", "n", "a", "c", "b", "c"]])

        execute(cql, table, "UPDATE %s SET tags[2] = 'foo', tags[4] = 'bar' WHERE fn='Bilbo' AND ln='Baggins'")
        assert_rows(execute(cql, table, "SELECT tags FROM %s WHERE fn='Bilbo' AND ln='Baggins'"),
                   [["m", "n", "foo", "c", "bar", "c"]])

        execute(cql, table, "DELETE tags[2] FROM %s WHERE fn='Bilbo' AND ln='Baggins'")
        assert_rows(execute(cql, table, "SELECT tags FROM %s WHERE fn='Bilbo' AND ln='Baggins'"),
                   [["m", "n", "c", "bar", "c"]])

        execute(cql, table, "UPDATE %s SET tags = tags - [ 'bar' ] WHERE fn='Bilbo' AND ln='Baggins'")
        assert_rows(execute(cql, table, "SELECT tags FROM %s WHERE fn='Bilbo' AND ln='Baggins'"),
                   [["m", "n", "c", "c"]])

# Migrated from cql_tests.py:TestCQL.multi_collection_test()
def testMultiCollections(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k uuid PRIMARY KEY, L list<int>, M map<text, int>, S set<int>)") as table:
        id = UUID("b017f48f-ae67-11e1-9096-005056c00008")

        execute(cql, table, "UPDATE %s SET L = [1, 3, 5] WHERE k = ?", id)
        execute(cql, table, "UPDATE %s SET L = L + [7, 11, 13] WHERE k = ?;", id)
        execute(cql, table, "UPDATE %s SET S = {1, 3, 5} WHERE k = ?", id)
        execute(cql, table, "UPDATE %s SET S = S + {7, 11, 13} WHERE k = ?", id)
        execute(cql, table, "UPDATE %s SET M = {'foo': 1, 'bar' : 3} WHERE k = ?", id)
        execute(cql, table, "UPDATE %s SET M = M + {'foobar' : 4} WHERE k = ?", id)

        assert_rows(execute(cql, table, "SELECT L, M, S FROM %s WHERE k = ?", id),
                   [[1, 3, 5, 7, 11, 13], {"bar": 3, "foo": 1, "foobar": 4}, {1, 3, 5, 7, 11, 13}])

# Migrated from cql_tests.py:TestCQL.collection_and_regular_test()
def testCollectionAndRegularColumns(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, l list<int>, c int)") as table:
        execute(cql, table, "INSERT INTO %s (k, l, c) VALUES(3, [0, 1, 2], 4)")
        execute(cql, table, "UPDATE %s SET l[0] = 1, c = 42 WHERE k = 3")
        assert_rows(execute(cql, table, "SELECT l, c FROM %s WHERE k = 3"),
                   [[1, 1, 2], 42]);

# Migrated from cql_tests.py:TestCQL.multi_list_set_test()
def testMultipleLists(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, l1 list<int>, l2 list<int>)") as table:
        execute(cql, table, "INSERT INTO %s (k, l1, l2) VALUES (0, [1, 2, 3], [4, 5, 6])")
        execute(cql, table, "UPDATE %s SET l2[1] = 42, l1[1] = 24  WHERE k = 0")

        assert_rows(execute(cql, table, "SELECT l1, l2 FROM %s WHERE k = 0"),
                   [[1, 24, 3], [4, 42, 6]])

# Test you can add columns in a table with collections (CASSANDrA-4982),
# migrated from cql_tests.py:TestCQL.alter_with_collections_test()
def testAlterCollections(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(key int PRIMARY KEY, aset set<text>)") as table:
        execute(cql, table, "ALTER TABLE %s ADD c text")
        execute(cql, table, "ALTER TABLE %s ADD alist list<text>")

# Migrated from cql_tests.py:TestCQL.collection_function_test()
def testFunctionsOnCollections(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, l set<int>)") as table:
        assert_invalid(cql, table, "SELECT ttl(l) FROM %s WHERE k = 0")
        assert_invalid(cql, table, "SELECT writetime(l) FROM %s WHERE k = 0")

def testInRestrictionWithCollection(cql, test_keyspace):
    for frozen in [True, False]:
        with create_table(cql, test_keyspace,
            "(a int, b int, c int, d frozen<list<int>>, e frozen<map<int, int>>, f frozen<set<int>>, PRIMARY KEY (a, b, c))" if frozen else "(a int, b int, c int, d list<int>, e map<int, int>, f set<int>, PRIMARY KEY (a, b, c))") as table:
            execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 1, 1, [1, 2], {1: 2}, {1, 2})")
            execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 1, 2, [1, 3], {1: 3}, {1, 3})")
            execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 1, 3, [1, 4], {1: 4}, {1, 4})")
            execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 2, 3, [1, 3], {1: 3}, {1, 3})")
            execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 2, 4, [1, 3], {1: 3}, {1, 3})")
            execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f) VALUES (2, 1, 1, [1, 2], {2: 2}, {1, 2})")

            for _ in before_and_after_flush(cql, table):
                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a in (1,2)"),
                    [1, 1, 1, [1, 2], {1: 2}, {1, 2}],
                    [1, 1, 2, [1, 3], {1: 3}, {1, 3}],
                    [1, 1, 3, [1, 4], {1: 4}, {1, 4}],
                    [1, 2, 3, [1, 3], {1: 3}, {1, 3}],
                    [1, 2, 4, [1, 3], {1: 3}, {1, 3}],
                    [2, 1, 1, [1, 2], {2: 2}, {1, 2}])

                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 AND b IN (1,2)"),
                    [1, 1, 1, [1, 2], {1: 2}, {1, 2}],
                    [1, 1, 2, [1, 3], {1: 3}, {1, 3}],
                    [1, 1, 3, [1, 4], {1: 4}, {1, 4}],
                    [1, 2, 3, [1, 3], {1: 3}, {1, 3}],
                    [1, 2, 4, [1, 3], {1: 3}, {1, 3}])

                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 AND b = 1 AND c in (1,2)"),
                    [1, 1, 1, [1, 2], {1: 2}, {1, 2}],
                    [1, 1, 2, [1, 3], {1: 3}, {1, 3}])

                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 AND b IN (1, 2) AND c in (1,2,3)"),
                    [1, 1, 1, [1, 2], {1: 2}, {1, 2}],
                    [1, 1, 2, [1, 3], {1: 3}, {1, 3}],
                    [1, 1, 3, [1, 4], {1: 4}, {1, 4}],
                    [1, 2, 3, [1, 3], {1: 3}, {1, 3}])

                assert_rows(execute(cql, table, "SELECT * FROM %s WHERE a = 1 AND b IN (1, 2) AND c in (1,2,3) AND d CONTAINS 4 ALLOW FILTERING"),
                    [1, 1, 3, [1, 4], {1: 4}, {1, 4}])

# Test for CASSANDRA-5795,
# migrated from cql_tests.py:TestCQL.nonpure_function_collection_test()
def testNonPureFunctionCollection(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, v list<timeuuid>)") as table:
        # we just want to make sure this doesn't throw
        execute(cql, table, "INSERT INTO %s (k, v) VALUES (0, [now()])")

# Test for CASSANDRA-5805,
# migrated from cql_tests.py:TestCQL.collection_flush_test()
def testCollectionFlush(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, s set<int>)") as table:
        execute(cql, table, "INSERT INTO %s (k, s) VALUES (1, {1})")
        flush(cql, table)

        execute(cql, table, "INSERT INTO %s (k, s) VALUES (1, {2})")
        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT * FROM %s"), [1, {2}])

# Test for CASSANDRA-6276,
# migrated from cql_tests.py:TestCQL.drop_and_readd_collection_test()
def testDropAndReaddCollection(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, v set<text>, x int)") as table:
        execute(cql, table, "insert into %s (k, v) VALUES (0, {'fffffffff'})")
        flush(cql, table)
        execute(cql, table, "alter table %s drop v")
        assert_invalid(cql, table, "alter table %s add v set<int>")

def testDropAndReaddDroppedCollection(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, v set<text>, x int)") as table:
        execute(cql, table, "insert into %s (k, v) VALUES (0, {'fffffffff'})")
        flush(cql, table)
        execute(cql, table, "alter table %s drop v")
        execute(cql, table, "alter table %s add v set<text>")

# FIXME: this test is 20 times slower than the rest (run pytest with "--durations=5"
# to see the 5 slowest tests). Is it checking anything worth this slowness??
def testMapWithLargePartition(cql, test_keyspace):
    seed = time.time()
    print(f"Seed {seed}")
    random.seed(seed)
    length = (1024 * 1024)//100
    with create_table(cql, test_keyspace, "(userid text PRIMARY KEY, properties map<int, text>) with compression = {}") as table:
        numKeys = 200
        for i in range(numKeys):
            s = ''.join(random.choice(string.ascii_uppercase) for x in range(length))
            execute(cql, table,"UPDATE %s SET properties[?] = ? WHERE userid = 'user'", i, s)

        flush(cql, table)

        rows = list(execute(cql, table, "SELECT properties from %s where userid = 'user'"))
        assert 1 == len(rows)
        assert numKeys == len(rows[0][0])

def testMapWithTwoSStables(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(userid text PRIMARY KEY, properties map<int, text>) with compression = {}") as table:
        numKeys = 100
        for i in range(numKeys):
            execute(cql, table, "UPDATE %s SET properties[?] = ? WHERE userid = 'user'", i, "prop_" + str(i))

        flush(cql, table)

        for i in range(numKeys):
            execute(cql, table, "UPDATE %s SET properties[?] = ? WHERE userid = 'user'", i + numKeys, "prop_" + str(i + numKeys))

        flush(cql, table)

        rows = list(execute(cql, table, "SELECT properties from %s where userid = 'user'"))
        assert 1 == len(rows)
        assert (numKeys * 2) == len(rows[0][0])


def testSetWithTwoSStables(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(userid text PRIMARY KEY, properties set<text>) with compression = {}") as table:
        numKeys = 100
        for i in range(numKeys):
            execute(cql, table, "UPDATE %s SET properties = properties + ? WHERE userid = 'user'", {"prop_" + str(i)})

        flush(cql, table)

        for i in range(numKeys):
            execute(cql, table, "UPDATE %s SET properties = properties + ? WHERE userid = 'user'", {"prop_" + str(i + numKeys)})

        flush(cql, table)

        rows = list(execute(cql, table, "SELECT properties from %s where userid = 'user'"))
        assert 1 == len(rows)
        assert (numKeys * 2) == len(rows[0][0])

def testUpdateStaticList(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k1 text, k2 text, s_list list<int> static, PRIMARY KEY (k1, k2))") as table:
        execute(cql, table, "insert into %s (k1, k2) VALUES ('a','b')")
        execute(cql, table, "update %s set s_list = s_list + [0] where k1='a'")
        assert_rows(execute(cql, table, "select s_list from %s where k1='a'"), [[0]])

        execute(cql, table, "update %s set s_list[0] = 100 where k1='a'")
        assert_rows(execute(cql, table, "select s_list from %s where k1='a'"), [[100]])

        execute(cql, table, "update %s set s_list = s_list + [0] where k1='a'")
        assert_rows(execute(cql, table, "select s_list from %s where k1='a'"), [[100, 0]])

        execute(cql, table, "delete s_list[0] from %s where k1='a'")
        assert_rows(execute(cql, table, "select s_list from %s where k1='a'"), [[0]])

def testListWithElementsBiggerThan64K(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, l list<text>)") as table:
        largeText = 'x' * (65536 + 10)
        largeText2 = 'y' * (65536 + 10)

        execute(cql, table, "INSERT INTO %s(k, l) VALUES (0, ?)", [largeText, "v2"])
        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 0"), [[largeText, "v2"]])

        execute(cql, table, "DELETE l[?] FROM %s WHERE k = 0", 0)

        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 0"), [["v2"]])

        execute(cql, table, "UPDATE %s SET l[?] = ? WHERE k = 0", 0, largeText)

        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 0"), [[largeText]])

        # Full overwrite
        execute(cql, table, "UPDATE %s SET l = ? WHERE k = 0", ["v1", largeText])
        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 0"), [["v1", largeText]])

        execute(cql, table, "UPDATE %s SET l = l + ? WHERE k = 0", ["v2", largeText2])

        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 0"), [["v1", largeText, "v2", largeText2]])

        execute(cql, table, "UPDATE %s SET l = l - ? WHERE k = 0", [largeText, "v2"])

        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 0"), [["v1", largeText2]])

        execute(cql, table, "DELETE l FROM %s WHERE k = 0")

        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 0"), [None])

        execute(cql, table, "INSERT INTO %s(k, l) VALUES (0, ['" + largeText + "', 'v2'])")
        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 0"), [[largeText, "v2"]])

@pytest.mark.xfail(reason="Map key wrongly limited to 64K in unprepared statement. Issue #7745")
def testMapsWithElementsBiggerThan64K(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, m map<text, text>)") as table:
        largeText = 'x' * (65536 + 10)
        largeText2 = 'y' * (65536 + 10)

        execute(cql, table, "INSERT INTO %s(k, m) VALUES (0, ?)", {"k1": largeText, largeText: "v2"})
        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 0"),
                   [{"k1": largeText, largeText: "v2"}])

        execute(cql, table, "UPDATE %s SET m[?] = ? WHERE k = 0", "k3", largeText)

        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 0"),
                   [{"k1": largeText, largeText: "v2", "k3": largeText}])

        execute(cql, table, "UPDATE %s SET m[?] = ? WHERE k = 0", largeText2, "v4")

        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 0"),
                   [{"k1": largeText, largeText: "v2", "k3": largeText, largeText2: "v4"}])

        execute(cql, table, "DELETE m[?] FROM %s WHERE k = 0", "k1")

        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 0"),
                   [{largeText: "v2", "k3": largeText, largeText2: "v4"}])

        execute(cql, table, "DELETE m[?] FROM %s WHERE k = 0", largeText2)
        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 0"),
                   [{largeText: "v2", "k3": largeText}])

        # Full overwrite
        execute(cql, table, "UPDATE %s SET m = ? WHERE k = 0", {"k5": largeText, largeText: "v6"})
        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 0"),
                   [{"k5": largeText, largeText: "v6"}])

        execute(cql, table, "UPDATE %s SET m = m + ? WHERE k = 0", {"k7": largeText})

        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 0"),
                   [{"k5": largeText, largeText: "v6", "k7": largeText}])

        execute(cql, table, "UPDATE %s SET m = m + ? WHERE k = 0", {largeText2: "v8"})
        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 0"),
                   [{"k5": largeText, largeText: "v6", "k7": largeText, largeText2: "v8"}])

        execute(cql, table, "DELETE m FROM %s WHERE k = 0")

        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 0"), [None])

        execute(cql, table, "INSERT INTO %s(k, m) VALUES (0, {'" + largeText + "' : 'v1', 'k2' : '" + largeText + "'})")
        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 0"),
                   [{largeText: "v1", "k2": largeText}])

@pytest.mark.xfail(reason="Set item wrongly limited to 64K in unprepared statement. Issue #7745")
def testSetsWithElementsBiggerThan64K(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, s set<text>)") as table:
        largeText = 'x' * (65536 + 10)
        largeText2 = 'y' * (65536 + 10)

        execute(cql, table, "INSERT INTO %s(k, s) VALUES (0, ?)", {largeText, "v2"})
        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k = 0"), [{largeText, "v2"}])

        execute(cql, table, "DELETE s[?] FROM %s WHERE k = 0", largeText)

        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k = 0"), [{"v2"}])

        # Full overwrite
        execute(cql, table, "UPDATE %s SET s = ? WHERE k = 0", {"v1", largeText})
        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k = 0"), [{"v1", largeText}])

        execute(cql, table, "UPDATE %s SET s = s + ? WHERE k = 0", {"v2", largeText2})

        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k = 0"), [{"v1", largeText, "v2", largeText2}])

        execute(cql, table, "UPDATE %s SET s = s - ? WHERE k = 0", {largeText, "v2"})

        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k = 0"), [{"v1", largeText2}])

        execute(cql, table, "DELETE s FROM %s WHERE k = 0")

        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k = 0"), [None])

        execute(cql, table, "INSERT INTO %s(k, s) VALUES (0, {'" + largeText + "', 'v2'})")
        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k = 0"), [{largeText, "v2"}])

def testRemovalThroughUpdate(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, l list<int>)") as table:
         execute(cql, table, "INSERT INTO %s(k, l) VALUES(?, ?)", 0, [1, 2, 3])
         assert_rows(execute(cql, table, "SELECT * FROM %s"), [0, [1, 2, 3]])

         execute(cql, table, "UPDATE %s SET l[0] = null WHERE k=0")
         assert_rows(execute(cql, table, "SELECT * FROM %s"), [0, [2, 3]])

# FIXME: the following tests are skipped out because the Python driver
# checks the validity of the prepared-statement binding before passing it
# to the server, so the following tests fail in the client library, not
# in the server.
@pytest.mark.skip(reason="Python driver checks this before reaching server")
def testInvalidInputForList(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int PRIMARY KEY, l list<text>)") as table:
        assert_invalid_message(cql, table, "Not enough bytes to read a list",
                             "INSERT INTO %s (pk, l) VALUES (?, ?)", 1, "test")
        assert_invalid_message(cql, table, "Not enough bytes to read a list",
                             "INSERT INTO %s (pk, l) VALUES (?, ?)", 1, Long.MAX_VALUE);
        assert_invalid_message(cql, table, "Not enough bytes to read a list",
                             "INSERT INTO %s (pk, l) VALUES (?, ?)", 1, "");
        assert_invalid_message(cql, table, "The data cannot be deserialized as a list",
                             "INSERT INTO %s (pk, l) VALUES (?, ?)", 1, -1);

@pytest.mark.skip(reason="Python driver checks this before reaching server")
def testInvalidInputForSet(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int PRIMARY KEY, s set<text>)") as table:
        assert_invalid_message(cql, table, "Not enough bytes to read a set",
                             "INSERT INTO %s (pk, s) VALUES (?, ?)", 1, "test");
        assert_invalid_message(cql, table, "String didn't validate.",
                             "INSERT INTO %s (pk, s) VALUES (?, ?)", 1, Long.MAX_VALUE);
        assert_invalid_message(cql, table, "Not enough bytes to read a set",
                             "INSERT INTO %s (pk, s) VALUES (?, ?)", 1, "");
        assert_invalid_message(cql, table, "The data cannot be deserialized as a set",
                             "INSERT INTO %s (pk, s) VALUES (?, ?)", 1, -1);

@pytest.mark.skip(reason="Python driver checks this before reaching server")
def testInvalidInputForMap(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int PRIMARY KEY, m set<text, text>)") as table:
        assert_invalid_message(cql, table, "Not enough bytes to read a map",
                             "INSERT INTO %s (pk, m) VALUES (?, ?)", 1, "test");
        assert_invalid_message(cql, table, "String didn't validate.",
                             "INSERT INTO %s (pk, m) VALUES (?, ?)", 1, Long.MAX_VALUE);
        assert_invalid_message(cql, table, "Not enough bytes to read a map",
                             "INSERT INTO %s (pk, m) VALUES (?, ?)", 1, "");
        assert_invalid_message(cql, table, "The data cannot be deserialized as a map",
                             "INSERT INTO %s (pk, m) VALUES (?, ?)", 1, -1);

@pytest.mark.xfail(reason="Handing of multiple list operation in same request changed in Cassandra. Issue #7747")
def testMultipleOperationOnListWithinTheSameQuery(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int PRIMARY KEY, l list<int>)") as table:
        execute(cql, table, "INSERT INTO %s (pk, l) VALUES (1, [1, 2, 3, 4])")

        # Checks that when the same element is updated twice the update with the greatest value is the one taken into account
        execute(cql, table, "UPDATE %s SET l[?] = ?, l[?] = ?  WHERE pk = ?", 2, 7, 2, 8, 1)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = ?", 1) , [1, [1, 2, 8, 4]])

        execute(cql, table, "UPDATE %s SET l[?] = ?, l[?] = ?  WHERE pk = ?", 2, 9, 2, 6, 1)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = ?", 1) , [1, [1, 2, 9, 4]])

        # Checks that deleting twice the same element will result in the deletion of the element with the index
        # and of the following element.
        execute(cql, table, "DELETE l[?], l[?] FROM %s WHERE pk = ?", 2, 2, 1)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = ?", 1) , [1, [1, 2]])

        # Checks that the set operation is performed on the added elements and that the greatest value win
        execute(cql, table, "UPDATE %s SET l = l + ?, l[?] = ?  WHERE pk = ?", [3, 4], 3, 7, 1)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = ?", 1) , [1, [1, 2, 3, 7]])

        execute(cql, table, "UPDATE %s SET l = l + ?, l[?] = ?  WHERE pk = ?", [6, 8], 4, 5, 1)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = ?", 1) , [1, [1, 2, 3, 7, 6, 8]])

        # Checks that the order of the operations matters
        assert_invalid_message(cql, table, "List index 6 out of bound, list has size 6",
                             "UPDATE %s SET l[?] = ?, l = l + ? WHERE pk = ?", 6, 5, [9], 1)

        # Checks that the updated element is deleted.
        execute(cql, table, "UPDATE %s SET l[?] = ? , l = l - ? WHERE pk = ?", 2, 8, [8], 1)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = ?", 1) , [1, [1, 2, 7, 6]])

        # Checks that we cannot update an element that has been removed.
        assert_invalid_message(cql, table, "List index 3 out of bound, list has size 3",
                             "UPDATE %s SET l = l - ?, l[?] = ?  WHERE pk = ?", [6], 3, 4, 1)

        # Checks that the element is updated before the other ones are shifted.
        execute(cql, table, "UPDATE %s SET l[?] = ? , l = l - ? WHERE pk = ?", 2, 8, [1], 1)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = ?", 1) , [1, [2, 8, 6]])

        # Checks that the element are shifted before the element is updated.
        execute(cql, table, "UPDATE %s SET l = l - ?, l[?] = ?  WHERE pk = ?", [2, 6], 0, 9, 1)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = ?", 1) , [1, [9]])

def testMultipleOperationOnMapWithinTheSameQuery(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int PRIMARY KEY, m map<int, int>)") as table:
        execute(cql, table, "INSERT INTO %s (pk, m) VALUES (1, {0 : 1, 1 : 2, 2 : 3, 3 : 4})")

        # Checks that when the same element is updated twice the update with the greatest value is the one taken into account
        execute(cql, table, "UPDATE %s SET m[?] = ?, m[?] = ?  WHERE pk = ?", 2, 7, 2, 8, 1)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = 1") , [1, {0: 1, 1: 2, 2: 8, 3: 4}])

        execute(cql, table, "UPDATE %s SET m[?] = ?, m[?] = ?  WHERE pk = ?", 2, 9, 2, 6, 1)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = 1") , [1, {0: 1, 1: 2, 2: 9, 3: 4}])

        # Checks that deleting twice the same element has no side effect
        execute(cql, table, "DELETE m[?], m[?] FROM %s WHERE pk = ?", 2, 2, 1)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = ?", 1) , [1, {0: 1, 1: 2, 3: 4}])

        # Checks that the set operation is performed on the added elements and that the greatest value win
        execute(cql, table, "UPDATE %s SET m = m + ?, m[?] = ?  WHERE pk = ?", {4: 5}, 4, 7, 1)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = ?", 1) , [1, {0: 1, 1: 2, 3: 4, 4: 7}])

        execute(cql, table, "UPDATE %s SET m = m + ?, m[?] = ?  WHERE pk = ?", {4: 8}, 4, 6, 1)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = ?", 1) , [1, {0: 1, 1: 2, 3: 4, 4: 8}])

        # Checks that, as tombstones win over updates for the same timestamp, the removed element is not re-added
        execute(cql, table, "UPDATE %s SET m = m - ?, m[?] = ?  WHERE pk = ?", {4}, 4, 9, 1)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = ?", 1) , [1, {0: 1, 1: 2, 3: 4}])

        # Checks that the update is taken into account before the removal
        execute(cql, table, "UPDATE %s SET m[?] = ?,  m = m - ?  WHERE pk = ?", 5, 9, {5}, 1)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = ?", 1) , [1, {0: 1, 1: 2, 3: 4}])

        # Checks that the set operation is merged with the change of the append and that the greatest value win
        execute(cql, table, "UPDATE %s SET m[?] = ?, m = m + ?  WHERE pk = ?", 5, 9, {5: 8, 6: 9}, 1)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = ?", 1) , [1, {0: 1, 1: 2, 3: 4, 5: 9, 6: 9}])

        execute(cql, table, "UPDATE %s SET m[?] = ?, m = m + ?  WHERE pk = ?", 7, 1, {7: 2}, 1)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = ?", 1) , [1, {0: 1, 1: 2, 3: 4, 5: 9, 6: 9, 7: 2}])

def testMultipleOperationOnSetWithinTheSameQuery(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int PRIMARY KEY, s set<int>)") as table:
        execute(cql, table, "INSERT INTO %s (pk, s) VALUES (1, {0, 1, 2})")

        # Checks that the two operation are merged and that the tombstone always win
        execute(cql, table, "UPDATE %s SET s = s + ? , s = s - ?  WHERE pk = ?", {3, 4}, {3}, 1)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = 1") , [1, {0, 1, 2, 4}])

        execute(cql, table, "UPDATE %s SET s = s - ? , s = s + ?  WHERE pk = ?", {3}, {3, 4}, 1)
        assert_rows(execute(cql, table, "SELECT * FROM %s WHERE pk = 1") , [1, {0, 1, 2, 4}])

@pytest.mark.xfail(reason="Cassandra 4.0 feature of selecting part of map or list not yet supported. Issue #7751")
def testMapOperation(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, c int, l text, m map<text, text>, fm frozen<map<text, text>>, sm map<text, text> STATIC, fsm frozen<map<text, text>> STATIC, o int, PRIMARY KEY (k, c))") as table:
        execute(cql, table, "INSERT INTO %s(k, c, l, m, fm, sm, fsm, o) VALUES (0, 0, 'foobar', ?, ?, ?, ?, 42)",
                {"22": "value22", "333": "value333"},
                {"1": "fvalue1", "22": "fvalue22", "333": "fvalue333"},
                {"22": "svalue22", "333": "svalue333"},
                {"1": "fsvalue1", "22": "fsvalue22", "333": "fsvalue333"})

        execute(cql, table, "INSERT INTO %s(k, c, l, m, fm, sm, fsm, o) VALUES (2, 0, 'row2', ?, ?, ?, ?, 88)",
                {"22": "2value22", "333": "2value333"},
                {"1": "2fvalue1", "22": "2fvalue22", "333": "2fvalue333"},
                {"22": "2svalue22", "333": "2svalue333"},
                {"1": "2fsvalue1", "22": "2fsvalue22", "333": "2fsvalue333"})

        flush(cql, table)

        execute(cql, table, "UPDATE %s SET m = m + ? WHERE k = 0 AND c = 0", {"1": "value1"})

        execute(cql, table, "UPDATE %s SET sm = sm + ? WHERE k = 0", {"1": "svalue1"})

        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT m['22'] FROM %s WHERE k = 0 AND c = 0"),
                   ["value22"])
        assert_rows(execute(cql, table, "SELECT m['1'], m['22'], m['333'] FROM %s WHERE k = 0 AND c = 0"),
                   ["value1", "value22", "value333"])
        assert_rows(execute(cql, table, "SELECT m['2'..'3'] FROM %s WHERE k = 0 AND c = 0"),
                   [{"22": "value22"}])

        execute(cql, table, "INSERT INTO %s(k, c, l, m, fm, o) VALUES (0, 1, 'foobar', ?, ?, 42)",
                {"1": "value1_2", "333": "value333_2"},
                {"1": "fvalue1_2", "333": "fvalue333_2"})

        assert_rows(execute(cql, table, "SELECT c, m['1'], fm['1'] FROM %s WHERE k = 0"),
                   [0, "value1", "fvalue1"],
                   [1, "value1_2", "fvalue1_2"])
        assert_rows(execute(cql, table, "SELECT c, sm['1'], fsm['1'] FROM %s WHERE k = 0"),
                   [0, "svalue1", "fsvalue1"],
                   [1, "svalue1", "fsvalue1"])
        assert_rows(execute(cql, table, "SELECT c, m['1'], fm['1'] FROM %s WHERE k = 0 AND c = 0"),
                   [0, "value1", "fvalue1"])
        assert_rows(execute(cql, table, "SELECT c, m['1'], fm['1'] FROM %s WHERE k = 0"),
                   [0, "value1", "fvalue1"],
                   [1, "value1_2", "fvalue1_2"])
        assert_column_names(execute(cql, table, "SELECT k, l, m['1'] as mx, o FROM %s WHERE k = 0"),
                          "k", "l", "mx", "o")
        # The Python driver has a function _clean_column_name() which "cleans" returned column
        # names by dropping any non-alphanumeric character at the beginning or end of the name
        # and replacing such characters in the middle by a "_".  So "m['1']" is converted to "m__1"
        assert_column_names(execute(cql, table, "SELECT k, l, m['1'], o FROM %s WHERE k = 0"),
                          "k", "l", "m__1", "o")
        assert_rows(execute(cql, table, "SELECT k, l, m['22'], o FROM %s WHERE k = 0"),
                   [0, "foobar", "value22", 42],
                   [0, "foobar", None, 42])
        assert_column_names(execute(cql, table, "SELECT k, l, m['22'], o FROM %s WHERE k = 0"),
                          "k", "l", "m__22", "o")
        assert_rows(execute(cql, table, "SELECT k, l, m['333'], o FROM %s WHERE k = 0"),
                   [0, "foobar", "value333", 42],
                   [0, "foobar", "value333_2", 42])
        assert_rows(execute(cql, table, "SELECT k, l, m['foobar'], o FROM %s WHERE k = 0"),
                   [0, "foobar", None, 42],
                   [0, "foobar", None, 42])
        assert_rows(execute(cql, table, "SELECT k, l, m['1'..'22'], o FROM %s WHERE k = 0"),
                   [0, "foobar", {"1": "value1", "22": "value22"}, 42],
                   [0, "foobar", {"1": "value1_2"}, 42])
        assert_rows(execute(cql, table, "SELECT k, l, m[''..'23'], o FROM %s WHERE k = 0"),
                   [0, "foobar", {"1": "value1", "22": "value22"}, 42],
                   [0, "foobar", {"1": "value1_2"}, 42])
        assert_column_names(execute(cql, table, "SELECT k, l, m[''..'23'], o FROM %s WHERE k = 0"),
                          "k", "l", "m______23", "o");
        assert_rows(execute(cql, table, "SELECT k, l, m['2'..'3'], o FROM %s WHERE k = 0"),
                   [0, "foobar", {"22": "value22"}, 42],
                   [0, "foobar", None, 42])
        assert_rows(execute(cql, table, "SELECT k, l, m['22'..], o FROM %s WHERE k = 0"),
                   [0, "foobar", {"22": "value22", "333": "value333"}, 42],
                   [0, "foobar", {"333": "value333_2"}, 42])
        assert_rows(execute(cql, table, "SELECT k, l, m[..'22'], o FROM %s WHERE k = 0"),
                   [0, "foobar", {"1": "value1", "22": "value22"}, 42],
                   [0, "foobar", {"1": "value1_2"}, 42])

        assert_rows(execute(cql, table, "SELECT k, l, m, o FROM %s WHERE k = 0"),
                   [0, "foobar", {"1": "value1", "22": "value22", "333": "value333"}, 42],
                   [0, "foobar", {"1": "value1_2", "333": "value333_2"}, 42])
        assert_rows(execute(cql, table, "SELECT k, l, m, m as m2, o FROM %s WHERE k = 0"),
                   [0, "foobar", {"1": "value1", "22": "value22", "333": "value333"}, {"1": "value1", "22": "value22", "333": "value333"}, 42],
                   [0, "foobar", {"1": "value1_2", "333": "value333_2"}, {"1": "value1_2", "333": "value333_2"}, 42])

        # FIXME: the following tests need *Java* as a UDF language, so work on
        # Cassandra, but to work on Scylla we'll need to translate to Lua:

        # with UDF as slice arg

        with create_function(cql, test_keyspace, "(arg text) CALLED ON NULL INPUT RETURNS TEXT " +
                                  "LANGUAGE java AS 'return arg;'") as f:
            assert_rows(execute(cql, table, "SELECT k, c, l, m[" + f +"('1').." + f +"('22')], o FROM %s WHERE k = 0"),
                   [0, 0, "foobar", {"1": "value1", "22": "value22"}, 42],
                   [0, 1, "foobar", {"1": "value1_2"}, 42])

            assert_rows(execute(cql, table, "SELECT k, c, l, m[" + f +"(?).." + f +"(?)], o FROM %s WHERE k = 0", "1", "22"),
                   [0, 0, "foobar", {"1": "value1", "22": "value22"}, 42],
                   [0, 1, "foobar", {"1": "value1_2"}, 42])

        # with UDF taking a map
        # FIXME: I think the test is wrong (and was wrong in Cassandra) -
        # it doesn't actually check a map...

        with create_function(cql, test_keyspace, "(m text) CALLED ON NULL INPUT RETURNS TEXT " +
                                  "LANGUAGE java AS $$return m;$$") as f:
            assert_rows(execute(cql, table, "SELECT k, c, " + f + "(m['1']) FROM %s WHERE k = 0"),
                   [0, 0, "value1"],
                   [0, 1, "value1_2"])

        # with UDF taking multiple cols

        with create_function(cql, test_keyspace, "(m1 map<text, text>, m2 text, k int, c int) CALLED ON NULL INPUT RETURNS TEXT " +
                                  "LANGUAGE java AS $$return m1.get(\"1\") + ':' + m2 + ':' + k + ':' + c;$$") as f:
            assert_rows(execute(cql, table, "SELECT " + f + "(m, m['1'], k, c) FROM %s WHERE k = 0"),
                   ["value1:value1:0:0"],
                   ["value1_2:value1_2:0:1"])

        # with nested UDF + aggregation and multiple cols

        with create_function(cql, test_keyspace, "(k int, c int) CALLED ON NULL INPUT RETURNS INT " +
                                  "LANGUAGE java AS $$return k + c;$$") as f:
            # The Python driver changes non-alpha-numeric characters in the
            # middle of column names to "_" and drops them in the start or
            # end of the column name - i.e., 
            # system.max(cql_test_1607272438822.cql_test_1607272439614(k, c))
            # to
            # system_max_cql_test_1607272438822_cql_test_1607272439614_k__c
            assert_column_names(execute(cql, table, "SELECT max(" + f + "(k, c)) as sel1, max(" + f + "(k, c)) FROM %s WHERE k = 0"),
                          "sel1", "system_max_" + f.replace('.', '_') + "_k__c")
            assert_rows(execute(cql, table, "SELECT max(" + f + "(k, c)) as sel1, max(" + f + "(k, c)) FROM %s WHERE k = 0"),
                   [1, 1])

            assert_column_names(execute(cql, table, "SELECT max(" + f + "(k, c)) as sel1, max(" + f + "(k, c)) FROM %s"),
                "sel1", "system_max_" + f.replace('.', '_') + "_k__c")
            assert_rows(execute(cql, table, "SELECT max(" + f + "(k, c)) as sel1, max(" + f + "(k, c)) FROM %s"),
                [2, 2])

        # prepared parameters
        assert_rows(execute(cql, table, "SELECT c, m[?], fm[?] FROM %s WHERE k = 0", "1", "1"),
                   [0, "value1", "fvalue1"],
                   [1, "value1_2", "fvalue1_2"])
        assert_rows(execute(cql, table, "SELECT c, sm[?], fsm[?] FROM %s WHERE k = 0", "1", "1"),
                   [0, "svalue1", "fsvalue1"],
                   [1, "svalue1", "fsvalue1"])
        assert_rows(execute(cql, table, "SELECT k, l, m[?..?], o FROM %s WHERE k = 0", "1", "22"),
                   [0, "foobar", {"1": "value1", "22": "value22"}, 42],
                   [0, "foobar", {"1": "value1_2"}, 42])

@pytest.mark.xfail(reason="Cassandra 4.0 feature of selecting part of map or list not yet supported. Issue #7751")
def testMapOperationWithIntKey(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, c int, l text, m map<int, text>, fm frozen<map<int, text>>, sm map<int, text> STATIC, fsm frozen<map<int, text>> STATIC, o int, PRIMARY KEY (k, c))") as table:
        # used type "int" as map key intentionally since CQL parsing relies on "BigInteger"
        execute(cql, table, "INSERT INTO %s(k, c, l, m, fm, sm, fsm, o) VALUES (0, 0, 'foobar', ?, ?, ?, ?, 42)",
                {22: "value22", 333: "value333"},
                {1: "fvalue1", 22: "fvalue22", 333: "fvalue333"},
                {22: "svalue22", 333: "svalue333"},
                {1: "fsvalue1", 22: "fsvalue22", 333: "fsvalue333"})

        execute(cql, table, "INSERT INTO %s(k, c, l, m, fm, sm, fsm, o) VALUES (2, 0, 'row2', ?, ?, ?, ?, 88)",
                {22: "2value22", 333: "2value333"},
                {1: "2fvalue1", 22: "2fvalue22", 333: "2fvalue333"},
                {22: "2svalue22", 333: "2svalue333"},
                {1: "2fsvalue1", 22: "2fsvalue22", 333: "2fsvalue333"})

        flush(cql, table)

        execute(cql, table, "UPDATE %s SET m = m + ? WHERE k = 0 AND c = 0",
                {1: "value1"})

        execute(cql, table, "UPDATE %s SET sm = sm + ? WHERE k = 0",
                {1: "svalue1"})

        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT m[22] FROM %s WHERE k = 0 AND c = 0"),
                   ["value22"])
        assert_rows(execute(cql, table, "SELECT m[1], m[22], m[333] FROM %s WHERE k = 0 AND c = 0"),
                   ["value1", "value22", "value333"])
        assert_rows(execute(cql, table, "SELECT m[20 .. 25] FROM %s WHERE k = 0 AND c = 0"),
                   [{22: "value22"}])

        execute(cql, table, "INSERT INTO %s(k, c, l, m, fm, o) VALUES (0, 1, 'foobar', ?, ?, 42)",
                {1: "value1_2", 333: "value333_2"},
                {1: "fvalue1_2", 333: "fvalue333_2"})

        assert_rows(execute(cql, table, "SELECT c, m[1], fm[1] FROM %s WHERE k = 0"),
                   [0, "value1", "fvalue1"],
                   [1, "value1_2", "fvalue1_2"])
        assert_rows(execute(cql, table, "SELECT c, sm[1], fsm[1] FROM %s WHERE k = 0"),
                   [0, "svalue1", "fsvalue1"],
                   [1, "svalue1", "fsvalue1"])

        # FIXME: the following tests need *Java* as a UDF language, so work on
        # Cassandra, but to work on Scylla we'll need to translate to Lua:

        # with UDF as slice arg
        with create_function(cql, test_keyspace, "(arg int) CALLED ON NULL INPUT RETURNS int " +
                                  "LANGUAGE java AS 'return arg;'") as f:
            assert_rows(execute(cql, table, "SELECT k, c, l, m[" + f +"(1).." + f +"(22)], o FROM %s WHERE k = 0"),
                   [0, 0, "foobar", {1: "value1", 22: "value22"}, 42],
                   [0, 1, "foobar", {1: "value1_2"}, 42])

            assert_rows(execute(cql, table, "SELECT k, c, l, m[" + f +"(?).." + f +"(?)], o FROM %s WHERE k = 0", 1, 22),
                   [0, 0, "foobar", {1: "value1", 22: "value22"}, 42],
                   [0, 1, "foobar", {1: "value1_2"}, 42])

        # with UDF taking a map
        # FIXME: I think the test is wrong (and was wrong in Cassandra) -
        # it doesn't actually check a map...
        with create_function(cql, test_keyspace, "(m text) CALLED ON NULL INPUT RETURNS TEXT " +
                                  "LANGUAGE java AS $$return m;$$") as f:
            assert_rows(execute(cql, table, "SELECT k, c, " + f + "(m[1]) FROM %s WHERE k = 0"),
                   [0, 0, "value1"],
                   [0, 1, "value1_2"])

        # with UDF taking multiple cols
        with create_function(cql, test_keyspace, "(m1 map<int, text>, m2 text, k int, c int) CALLED ON NULL INPUT RETURNS TEXT " +
                                  "LANGUAGE java AS $$return m1.get(1) + ':' + m2 + ':' + k + ':' + c;$$") as f:
            assert_rows(execute(cql, table, "SELECT " + f + "(m, m[1], k, c) FROM %s WHERE k = 0"),
                   ["value1:value1:0:0"],
                   ["value1_2:value1_2:0:1"])

        # with nested UDF + aggregation and multiple cols
        with create_function(cql, test_keyspace, "(k int, c int) CALLED ON NULL INPUT RETURNS int " +
                                  "LANGUAGE java AS $$return k + c;$$") as f:
            # The Python driver changes non-alpha-numeric characters in the
            # middle of column names to "_" and drops them in the start or
            # end of the column name - i.e., 
            # system.max(cql_test_1607272438822.cql_test_1607272439614(k, c))
            # to
            # system_max_cql_test_1607272438822_cql_test_1607272439614_k__c
            assert_column_names(execute(cql, table, "SELECT max(" + f + "(k, c)) as sel1, max(" + f + "(k, c)) FROM %s WHERE k = 0"),
                          "sel1", "system_max_" + f.replace('.', '_') + "_k__c")
            assert_rows(execute(cql, table, "SELECT max(" + f + "(k, c)) as sel1, max(" + f + "(k, c)) FROM %s WHERE k = 0"),
                   [1, 1])

            assert_column_names(execute(cql, table, "SELECT max(" + f + "(k, c)) as sel1, max(" + f + "(k, c)) FROM %s"),
                          "sel1", "system_max_" + f.replace('.', '_') + "_k__c")
            assert_rows(execute(cql, table, "SELECT max(" + f + "(k, c)) as sel1, max(" + f + "(k, c)) FROM %s"),
                   [2, 2])

        # prepared parameters

        assert_rows(execute(cql, table, "SELECT c, m[?], fm[?] FROM %s WHERE k = 0", 1, 1),
                   [0, "value1", "fvalue1"],
                   [1, "value1_2", "fvalue1_2"])
        assert_rows(execute(cql, table, "SELECT c, sm[?], fsm[?] FROM %s WHERE k = 0", 1, 1),
                   [0, "svalue1", "fsvalue1"],
                   [1, "svalue1", "fsvalue1"])
        assert_rows(execute(cql, table, "SELECT k, l, m[?..?], o FROM %s WHERE k = 0", 1, 22),
                   [0, "foobar", {1: "value1", 22: "value22"}, 42],
                   [0, "foobar", {1: "value1_2"}, 42])

@pytest.mark.xfail(reason="Cassandra 4.0 feature of selecting part of map or list not yet supported. Issue #7751")
def testMapOperationOnPartKey(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k frozen<map<text, text>> PRIMARY KEY, l text, o int)") as table:
        execute(cql, table, "INSERT INTO %s(k, l, o) VALUES (?, 'foobar', 42)", {"1": "value1", "22": "value22", "333": "value333"})

        assert_rows(execute(cql, table, "SELECT l, k['1'], o FROM %s WHERE k = ?", {"1": "value1", "22": "value22", "333": "value333"}),
                   ["foobar", "value1", 42])

        assert_rows(execute(cql, table, "SELECT l, k['22'], o FROM %s WHERE k = ?", {"1": "value1", "22": "value22", "333": "value333"}),
                   ["foobar", "value22", 42])

        assert_rows(execute(cql, table, "SELECT l, k['333'], o FROM %s WHERE k = ?", {"1": "value1", "22": "value22", "333": "value333"}),
                   ["foobar", "value333", 42])

        assert_rows(execute(cql, table, "SELECT l, k['foobar'], o FROM %s WHERE k = ?", {"1": "value1", "22": "value22", "333": "value333"}),
                   ["foobar", None, 42])

        assert_rows(execute(cql, table, "SELECT l, k['1'..'22'], o FROM %s WHERE k = ?", {"1": "value1", "22": "value22", "333": "value333"}),
                   ["foobar", {"1": "value1", "22": "value22"}, 42])

        assert_rows(execute(cql, table, "SELECT l, k[''..'23'], o FROM %s WHERE k = ?", {"1": "value1", "22": "value22", "333": "value333"}),
                   ["foobar", {"1": "value1", "22": "value22"}, 42])

        assert_rows(execute(cql, table, "SELECT l, k['2'..'3'], o FROM %s WHERE k = ?", {"1": "value1", "22": "value22", "333": "value333"}),
                   ["foobar", {"22": "value22"}, 42])

        assert_rows(execute(cql, table, "SELECT l, k['22'..], o FROM %s WHERE k = ?", {"1": "value1", "22": "value22", "333": "value333"}),
                   ["foobar", {"22": "value22", "333": "value333"}, 42])

        assert_rows(execute(cql, table, "SELECT l, k[..'22'], o FROM %s WHERE k = ?", {"1": "value1", "22": "value22", "333": "value333"}),
                   ["foobar", {"1": "value1", "22": "value22"}, 42])

        assert_rows(execute(cql, table, "SELECT l, k, o FROM %s WHERE k = ?", {"1": "value1", "22": "value22", "333": "value333"}),
                   ["foobar", {"1": "value1", "22": "value22", "333": "value333"}, 42])

@pytest.mark.xfail(reason="Cassandra 4.0 feature of selecting part of map or list not yet supported. Issue #7751")
def testMapOperationOnClustKey(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, c frozen<map<text, text>>, l text, o int, PRIMARY KEY (k,c))") as table:
        execute(cql, table, "INSERT INTO %s(k, c, l, o) VALUES (0, ?, 'foobar', 42)", {"1": "value1", "22": "value22", "333": "value333"})

        assert_rows(execute(cql, table, "SELECT k, l, c['1'], o FROM %s WHERE k = 0 AND c = ?", {"1": "value1", "22": "value22", "333": "value333"}),
                   [0, "foobar", "value1", 42])

        assert_rows(execute(cql, table, "SELECT k, l, c['22'], o FROM %s WHERE k = 0 AND c = ?", {"1": "value1", "22": "value22", "333": "value333"}),
                   [0, "foobar", "value22", 42])

        assert_rows(execute(cql, table, "SELECT k, l, c['333'], o FROM %s WHERE k = 0 AND c = ?", {"1": "value1", "22": "value22", "333": "value333"}),
                   [0, "foobar", "value333", 42])

        assert_rows(execute(cql, table, "SELECT k, l, c['foobar'], o FROM %s WHERE k = 0 AND c = ?", {"1": "value1", "22": "value22", "333": "value333"}),
                   [0, "foobar", None, 42])

        assert_rows(execute(cql, table, "SELECT k, l, c['1'..'22'], o FROM %s WHERE k = 0 AND c = ?", {"1": "value1", "22": "value22", "333": "value333"}),
                   [0, "foobar", {"1": "value1", "22": "value22"}, 42])

        assert_rows(execute(cql, table, "SELECT k, l, c[''..'23'], o FROM %s WHERE k = 0 AND c = ?", {"1": "value1", "22": "value22", "333": "value333"}),
                   [0, "foobar", {"1": "value1", "22": "value22"}, 42])

        assert_rows(execute(cql, table, "SELECT k, l, c['2'..'3'], o FROM %s WHERE k = 0 AND c = ?", {"1": "value1", "22": "value22", "333": "value333"}),
                   [0, "foobar", {"22": "value22"}, 42])

        assert_rows(execute(cql, table, "SELECT k, l, c['22'..], o FROM %s WHERE k = 0 AND c = ?", {"1": "value1", "22": "value22", "333": "value333"}),
                   [0, "foobar", {"22": "value22", "333": "value333"}, 42])

        assert_rows(execute(cql, table, "SELECT k, l, c[..'22'], o FROM %s WHERE k = 0 AND c = ?", {"1": "value1", "22": "value22", "333": "value333"}),
                   [0, "foobar", {"1": "value1", "22": "value22"}, 42])

        assert_rows(execute(cql, table, "SELECT k, l, c, o FROM %s WHERE k = 0 AND c = ?", {"1": "value1", "22": "value22", "333": "value333"}),
                   [0, "foobar", {"1": "value1", "22": "value22", "333": "value333"}, 42])

@pytest.mark.xfail(reason="Cassandra 4.0 feature of selecting part of map or list not yet supported. Issue #7751")
def testSetOperation(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, c int, l text, s set<text>, fs frozen<set<text>>, ss set<text> STATIC, fss frozen<set<text>> STATIC, o int, PRIMARY KEY (k, c))") as table:
        execute(cql, table, "INSERT INTO %s(k, c, l, s, fs, ss, fss, o) VALUES (0, 0, 'foobar', ?, ?, ?, ?, 42)",
                {"1", "22", "333"},
                {"f1", "f22", "f333"},
                {"s1", "s22", "s333"},
                {"fs1", "fs22", "fs333"})

        flush(cql, table)

        execute(cql, table, "UPDATE %s SET s = s + ? WHERE k = 0 AND c = 0", {"22_2"})

        execute(cql, table, "UPDATE %s SET ss = ss + ? WHERE k = 0", {"s22_2"})

        flush(cql, table)

        execute(cql, table, "INSERT INTO %s(k, c, l, s, o) VALUES (0, 1, 'foobar', ?, 42)",
                {"22", "333"})

        assert_rows(execute(cql, table, "SELECT c, s, fs, ss, fss FROM %s WHERE k = 0"),
                   [0, {"1", "22", "22_2", "333"}, {"f1", "f22", "f333"}, {"s1", "s22", "s22_2", "s333"}, {"fs1", "fs22", "fs333"}],
                   [1, {"22", "333"}, None, {"s1", "s22", "s22_2", "s333"}, {"fs1", "fs22", "fs333"}])

        assert_rows(execute(cql, table, "SELECT c, s['1'], fs['f1'], ss['s1'], fss['fs1'] FROM %s WHERE k = 0"),
                   [0, "1", "f1", "s1", "fs1"],
                   [1, None, None, "s1", "fs1"])

        assert_rows(execute(cql, table, "SELECT s['1'], fs['f1'], ss['s1'], fss['fs1'] FROM %s WHERE k = 0 AND c = 0"),
                   ["1", "f1", "s1", "fs1"])

        assert_rows(execute(cql, table, "SELECT k, c, l, s['1'], fs['f1'], ss['s1'], fss['fs1'], o FROM %s WHERE k = 0"),
                   [0, 0, "foobar", "1", "f1", "s1", "fs1", 42],
                   [0, 1, "foobar", None, None, "s1", "fs1", 42])

        assert_column_names(execute(cql, table, "SELECT k, l, s['1'], o FROM %s WHERE k = 0"),
                          "k", "l", "s__1", "o")

        assert_rows(execute(cql, table, "SELECT k, l, s['22'], o FROM %s WHERE k = 0 AND c = 0"),
                   [0, "foobar", "22", 42])

        assert_rows(execute(cql, table, "SELECT k, l, s['333'], o FROM %s WHERE k = 0 AND c = 0"),
                   [0, "foobar", "333", 42])

        assert_rows(execute(cql, table, "SELECT k, l, s['foobar'], o FROM %s WHERE k = 0 AND c = 0"),
                   [0, "foobar", None, 42])

        assert_rows(execute(cql, table, "SELECT k, l, s['1'..'22'], o FROM %s WHERE k = 0 AND c = 0"),
                   [0, "foobar", {"1", "22"}, 42])
        assert_column_names(execute(cql, table, "SELECT k, l, s[''..'22'], o FROM %s WHERE k = 0"),
                          "k", "l", "s______22", "o")

        assert_rows(execute(cql, table, "SELECT k, l, s[''..'23'], o FROM %s WHERE k = 0 AND c = 0"),
                   [0, "foobar", {"1", "22", "22_2"}, 42])

        assert_rows(execute(cql, table, "SELECT k, l, s['2'..'3'], o FROM %s WHERE k = 0 AND c = 0"),
                   [0, "foobar", {"22", "22_2"}, 42])

        assert_rows(execute(cql, table, "SELECT k, l, s['22'..], o FROM %s WHERE k = 0"),
                   [0, "foobar", {"22", "22_2", "333"}, 42],
                   [0, "foobar", {"22", "333"}, 42])

        assert_rows(execute(cql, table, "SELECT k, l, s[..'22'], o FROM %s WHERE k = 0"),
                   [0, "foobar", {"1", "22"}, 42],
                   [0, "foobar", {"22"}, 42])

        assert_rows(execute(cql, table, "SELECT k, l, s, o FROM %s WHERE k = 0"),
                   [0, "foobar", {"1", "22", "22_2", "333"}, 42],
                   [0, "foobar", {"22", "333"}, 42])

@pytest.mark.xfail(reason="Cassandra 4.0 feature of selecting part of map or list not yet supported. Issue #7751")
def testCollectionSliceOnMV(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, c int, l text, m map<text, text>, o int, PRIMARY KEY (k, c))") as table:
        assert_invalid_message(cql, table, "Can only select columns by name when defining a materialized view (got m['abc'])",
                             "CREATE MATERIALIZED VIEW " + test_keyspace + ".view1 AS SELECT m['abc'] FROM %s WHERE k IS NOT NULL AND c IS NOT NULL AND m IS NOT NULL PRIMARY KEY (c, k)");
        assert_invalid_message(cql, table, "Can only select columns by name when defining a materialized view (got m['abc'..'def'])",
                             "CREATE MATERIALIZED VIEW " + test_keyspace + ".view1 AS SELECT m['abc'..'def'] FROM %s WHERE k IS NOT NULL AND c IS NOT NULL AND m IS NOT NULL PRIMARY KEY (c, k)");

@pytest.mark.xfail(reason="Cassandra 4.0 feature of selecting part of map or list not yet supported. Issue #7751")
def testElementAccessOnList(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(pk int PRIMARY KEY, l list<int>)") as table:
        execute(cql, table, "INSERT INTO %s (pk, l) VALUES (1, [1, 2, 3])");

        assert_invalid_message(cql, table, "Element selection is only allowed on sets and maps, but l is a list",
                             "SELECT pk, l[0] FROM %s");

        assert_invalid_message(cql, table, "Slice selection is only allowed on sets and maps, but l is a list",
                "SELECT pk, l[1..3] FROM %s");

@pytest.mark.xfail(reason="Cassandra 4.0 feature of selecting part of map or list not yet supported. Issue #7751")
def testCollectionOperationResultSetMetadata(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, m map<text, text>, fm frozen<map<text, text>>, s set<text>, fs frozen<set<text>>)") as table:
        execute(cql, table, "INSERT INTO %s (k, m, fm, s, fs) VALUES (?, ?, ?, ?, ?)",
                0,
                {"1": "one", "2": "two"},
                {"1": "one", "2": "two"},
                {"1", "2", "3"},
                {"1", "2", "3"})

        cmd = "SELECT k, m, m['2'], m['2'..'3'], m[..'2'], m['3'..], fm, fm['2'], fm['2'..'3'], fm[..'2'], fm['3'..], s, s['2'], s['2'..'3'], s[..'2'], s['3'..], fs, fs['2'], fs['2'..'3'], fs[..'2'], fs['3'..] FROM %s WHERE k = 0"
        result = execute(cql, table, cmd)
        # FIXME: still need to port this part to Python
        """
        Iterator<ColumnSpecification> meta = result.metadata().iterator();
        meta.next();
        for (int i = 0; i < 4; i++)
        {
            // take the "full" collection selection
            ColumnSpecification ref = meta.next();
            ColumnSpecification selSingle = meta.next();
            assertEquals(ref.toString(), UTF8Type.instance, selSingle.type);
            for (int selOrSlice = 0; selOrSlice < 3; selOrSlice++)
            {
                ColumnSpecification selSlice = meta.next();
                assertEquals(ref.toString(), ref.type, selSlice.type);
            }
        }
        """

        assert_rows(result,
                   [0,
                       {"1": "one", "2": "two"}, "two", {"2": "two"}, {"1": "one", "2": "two"}, None,
                       {"1": "one", "2": "two"}, "two", {"2": "two"}, {"1": "one", "2": "two"}, dict(),
                       {"1", "2", "3"}, "2", {"2", "3"}, {"1", "2"}, {"3"},
                       {"1", "2", "3"}, "2", {"2", "3"}, {"1", "2"}, {"3"}])

        # FIXME: still need to port this part to Python
        """
        Session session = sessionNet();
        ResultSet rset = session.execute(cql);
        ColumnDefinitions colDefs = rset.getColumnDefinitions();
        Iterator<ColumnDefinitions.Definition> colDefIter = colDefs.asList().iterator();
        colDefIter.next();
        for (int i = 0; i < 4; i++)
        {
            // take the "full" collection selection
            ColumnDefinitions.Definition ref = colDefIter.next();
            ColumnDefinitions.Definition selSingle = colDefIter.next();
            assertEquals(ref.getName(), DataType.NativeType.text(), selSingle.getType());
            for (int selOrSlice = 0; selOrSlice < 3; selOrSlice++)
            {
                ColumnDefinitions.Definition selSlice = colDefIter.next();
                assertEquals(ref.getName() + ' ' + ref.getType(), ref.getType(), selSlice.getType());
            }
        }
        """

@pytest.mark.xfail(reason="Cassandra 4.0 feature of selecting part of map or list not yet supported. Issue #7751")
def testFrozenCollectionNestedAccess(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, m map<text, frozen<map<text, set<int>>>>)") as table:
        execute(cql, table, "INSERT INTO %s(k, m) VALUES (0, ?)", {"1": {"a": {1, 2, 4}, "b": {3}}, "2": {"a": {2, 4}}})

        assert_rows(execute(cql, table, "SELECT m[?] FROM %s WHERE k = 0", "1"), [{"a": {1, 2, 4}, "b": {3}}])
        assert_rows(execute(cql, table, "SELECT m[?][?] FROM %s WHERE k = 0", "1", "a"), [{1, 2, 4}])
        assert_rows(execute(cql, table, "SELECT m[?][?][?] FROM %s WHERE k = 0", "1", "a", 2), [2])
        assert_rows(execute(cql, table, "SELECT m[?][?][?..?] FROM %s WHERE k = 0", "1", "a", 2, 3), [{2}])

        # Checks it still work after flush
        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT m[?] FROM %s WHERE k = 0", "1"), [{"a": {1, 2, 4}, "b": {3}}])
        assert_rows(execute(cql, table, "SELECT m[?][?] FROM %s WHERE k = 0", "1", "a"), [{1, 2, 4}])
        assert_rows(execute(cql, table, "SELECT m[?][?][?] FROM %s WHERE k = 0", "1", "a", 2), [2])
        assert_rows(execute(cql, table, "SELECT m[?][?][?..?] FROM %s WHERE k = 0", "1", "a", 2, 3), [{2}])

@pytest.mark.xfail(reason="Cassandra 4.0 feature of selecting part of map or list not yet supported. Issue #7751")
def testUDTAndCollectionNestedAccess(cql, test_keyspace):
    sm_tuple = collections.namedtuple('sm_tuple', ['s', 'm'])
    with create_type(cql, test_keyspace, "(s set<int>, m map<text, text>)") as type_name:
        # The message returned by Scylla (Non-frozen user types or collections
        # are not allowed inside collections) is slightly different than that
        # returned by Cassandra (Non-frozen UDTs are not allowed inside collections)
        assert_invalid_message(cql, test_keyspace + "." + unique_name(), "not allowed inside collections",
            "CREATE TABLE %s (k int PRIMARY KEY, v map<text, " + type_name + ">)")
        mapType = "map<text, frozen<" + type_name + ">>"
        for frozen in [False, True]:
            mapType = "frozen<" + mapType + ">" if frozen else mapType

            with create_table(cql, test_keyspace, "(k int PRIMARY KEY, v " + mapType + ")") as table:
                execute(cql, table, "INSERT INTO %s(k, v) VALUES (0, ?)", {"abc": sm_tuple({2, 4, 6}, {"a": "v1", "d": "v2"})})

                for _ in before_and_after_flush(cql, table):
                    assert_rows(execute(cql, table, "SELECT v[?].s FROM %s WHERE k = 0", "abc"), [{2, 4, 6}])
                    assert_rows(execute(cql, table, "SELECT v[?].m[..?] FROM %s WHERE k = 0", "abc", "b"), [{"a": "v1"}])
                    assert_rows(execute(cql, table, "SELECT v[?].m[?] FROM %s WHERE k = 0", "abc", "d"), ["v2"])

        assert_invalid_message(cql, test_keyspace + "." + unique_name(), "Non-frozen UDTs with nested non-frozen collections are not supported",
                             "CREATE TABLE %s (k int PRIMARY KEY, v " + type_name + ")")

    with create_type(cql, test_keyspace, "(s frozen<set<int>>, m frozen<map<text, text>>)") as type_name:
        for frozen in [False, True]:
            type_name = "frozen<" + type_name + ">" if frozen else type_name

            with create_table(cql, test_keyspace, "(k int PRIMARY KEY, v " + type_name + ")") as table:
                execute(cql, table, "INSERT INTO %s(k, v) VALUES (0, ?)", sm_tuple({2, 4, 6}, {"a": "v1", "d": "v2"}))

                for _ in before_and_after_flush(cql, table):
                    assert_rows(execute(cql, table, "SELECT v.s[?] FROM %s WHERE k = 0", 2), [2])
                    assert_rows(execute(cql, table, "SELECT v.s[?..?] FROM %s WHERE k = 0", 2, 5), [{2, 4}])
                    assert_rows(execute(cql, table, "SELECT v.s[..?] FROM %s WHERE k = 0", 3), [{2}])
                    assert_rows(execute(cql, table, "SELECT v.m[..?] FROM %s WHERE k = 0", "b"), [{"a": "v1"}])
                    assert_rows(execute(cql, table, "SELECT v.m[?] FROM %s WHERE k = 0", "d"), ["v2"])

@pytest.mark.xfail(reason="Cassandra 4.0 feature of selecting part of map or list not yet supported. Issue #7751")
def testMapOverlappingSlices(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, m map<int,int>)") as table:
        execute(cql, table, "INSERT INTO %s(k, m) VALUES (?, ?)", 0, {0: 0, 1: 1, 2: 2, 3: 3, 4: 4, 5: 5})

        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT m[7..8] FROM %s WHERE k=?", 0),
                   [None])

        assert_rows(execute(cql, table, "SELECT m[0..3] FROM %s WHERE k=?", 0),
                   [{0: 0, 1: 1, 2: 2, 3: 3}])

        assert_rows(execute(cql, table, "SELECT m[0..3], m[2..4] FROM %s WHERE k=?", 0),
                   [{0: 0, 1: 1, 2: 2, 3: 3}, {2: 2, 3: 3, 4: 4}])

        assert_rows(execute(cql, table, "SELECT m, m[2..4] FROM %s WHERE k=?", 0),
                   [{0: 0, 1: 1, 2: 2, 3: 3, 4: 4, 5: 5}, {2: 2, 3: 3, 4: 4}])

        assert_rows(execute(cql, table, "SELECT m[..3], m[3..4] FROM %s WHERE k=?", 0),
                   [{0: 0, 1: 1, 2: 2, 3: 3}, {3: 3, 4: 4}])

        assert_rows(execute(cql, table, "SELECT m[1..3], m[2] FROM %s WHERE k=?", 0),
                   [{1: 1, 2: 2, 3: 3}, 2])

@pytest.mark.xfail(reason="Cassandra 4.0 feature of selecting part of map or list not yet supported. Issue #7751")
def testMapOverlappingSlicesWithDoubles(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, m map<double, double>)") as table:
        execute(cql, table, "INSERT INTO %s(k, m) VALUES (?, ?)", 0, {0.0: 0.0, 1.1: 1.1, 2.2: 2.2, 3.0: 3.0, 4.4: 4.4, 5.5: 5.5})

        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT m[0.0..3.0] FROM %s WHERE k=?", 0),
                   [{0.0: 0.0, 1.1: 1.1, 2.2: 2.2, 3.0: 3.0}])

        assert_rows(execute(cql, table, "SELECT m[0...3.], m[2.2..4.4] FROM %s WHERE k=?", 0),
                   [{0.0: 0.0, 1.1: 1.1, 2.2: 2.2, 3.0: 3.0}, {2.2: 2.2, 3.0: 3.0, 4.4: 4.4}])

        assert_rows(execute(cql, table, "SELECT m, m[2.2..4.4] FROM %s WHERE k=?", 0),
                   [{0.0: 0.0, 1.1: 1.1, 2.2: 2.2, 3.0: 3.0, 4.4: 4.4, 5.5: 5.5}, {2.2: 2.2, 3.0: 3.0, 4.4: 4.4}])

        assert_rows(execute(cql, table, "SELECT m[..3.], m[3...4.4] FROM %s WHERE k=?", 0),
                   [{0.0: 0.0, 1.1: 1.1, 2.2: 2.2, 3.0: 3.0}, {3.0: 3.0, 4.4: 4.4}])

        assert_rows(execute(cql, table, "SELECT m[1.1..3.0], m[2.2] FROM %s WHERE k=?", 0),
                   [{1.1: 1.1, 2.2: 2.2, 3.0: 3.0}, 2.2])

@pytest.mark.xfail(reason="Cassandra 4.0 feature of selecting part of map or list not yet supported. Issue #7751")
def testNestedAccessWithNestedMap(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id text PRIMARY KEY, m map<float,frozen<map<int, text>>>)") as table:
        execute(cql, table, "INSERT INTO %s (id,m) VALUES ('1', {1: {2: 'one-two'}})")

        flush(cql, table)

        assert_rows(execute(cql, table, "SELECT m[1][2] FROM %s WHERE id = '1'"),
                   ["one-two"])

        assert_rows(execute(cql, table, "SELECT m[1..][2] FROM %s WHERE id = '1'"),
                   [None])

        assert_rows(execute(cql, table, "SELECT m[1][..2] FROM %s WHERE id = '1'"),
                   [{2: "one-two"}])

        assert_rows(execute(cql, table, "SELECT m[1..][..2] FROM %s WHERE id = '1'"),
                   [{1.0: {2: "one-two"}}])

def testInsertingCollectionsWithInvalidElements(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, s frozen<set<tuple<int, text, double>>>)") as table:
        # Unfortunately, the Python driver has its own checking of the prepared
        # statement bound parameters, so I commented out these tests.
        #assert_invalid_message(cql, table, "Invalid remaining data after end of tuple value",
        #                     "INSERT INTO %s (k, s) VALUES (0, ?)",
        #                     {(1, "1", 1.0, 1), (2, "2", 2.0, 2)})

        assert_invalid_message(cql, table, "Invalid set literal for s: value (1, '1', 1.0, 1) is not of type frozen<tuple<int, text, double>>",
                             "INSERT INTO %s (k, s) VALUES (0, {(1, '1', 1.0, 1)})")

    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, l frozen<list<tuple<int, text, double>>>)") as table:
        #assert_invalid_message(cql, table, "Invalid remaining data after end of tuple value",
        #                     "INSERT INTO %s (k, l) VALUES (0, ?)",
        #                     [(1, "1", 1.0, 1), (2, "2", 2.0, 2)])

        assert_invalid_message(cql, table, "Invalid list literal for l: value (1, '1', 1.0, 1) is not of type frozen<tuple<int, text, double>>",
                             "INSERT INTO %s (k, l) VALUES (0, [(1, '1', 1.0, 1)])")

    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, m frozen<map<tuple<int, text, double>, int>>)") as table:
        #assert_invalid_message(cql, table, "Invalid remaining data after end of tuple value",
        #                    "INSERT INTO %s (k, m) VALUES (0, ?)",
        #                    {(1, "1", 1.0, 1): 1, (2, "2", 2.0, 2): 2})

        assert_invalid_message(cql, table, "Invalid map literal for m: key (1, '1', 1.0, 1) is not of type frozen<tuple<int, text, double>>",
                             "INSERT INTO %s (k, m) VALUES (0, {(1, '1', 1.0, 1) : 1})")

    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, m frozen<map<int, tuple<int, text, double>>>)") as table:
        #assert_invalid_message(cql, table, "Invalid remaining data after end of tuple value",
        #                     "INSERT INTO %s (k, m) VALUES (0, ?)",
        #                     {1: (1, "1", 1.0, 1), 2: (2, "2", 2.0, 2)})

        assert_invalid_message(cql, table, "Invalid map literal for m: value (1, '1', 1.0, 1) is not of type frozen<tuple<int, text, double>>",
                             "INSERT INTO %s (k, m) VALUES (0, {1 : (1, '1', 1.0, 1)})")

@pytest.mark.xfail(reason="Cassandra 4.0 feature of selecting part of map or list not yet supported. Issue #7751")
def testSelectionOfEmptyCollections(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, m frozen<map<text, int>>, s frozen<set<int>>)") as table:
        execute(cql, table, "INSERT INTO %s(k) VALUES (0)")
        execute(cql, table, "INSERT INTO %s(k, m, s) VALUES (1, {}, {})")
        execute(cql, table, "INSERT INTO %s(k, m, s) VALUES (2, ?, ?)", dict(), {})
        execute(cql, table, "INSERT INTO %s(k, m, s) VALUES (3, {'2':2}, {2})")

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT m, s FROM %s WHERE k = 0"), [None, None])
            assert_rows(execute(cql, table, "SELECT m['0'], s[0] FROM %s WHERE k = 0"), [None, None])
            assert_rows(execute(cql, table, "SELECT m['0'..'1'], s[0..1] FROM %s WHERE k = 0"), [None, None])
            assert_rows(execute(cql, table, "SELECT m['0'..'1']['3'..'5'], s[0..1][3..5] FROM %s WHERE k = 0"), [None, None])

            assert_rows(execute(cql, table, "SELECT m, s FROM %s WHERE k = 1"), [dict(), set()])
            assert_rows(execute(cql, table, "SELECT m['0'], s[0] FROM %s WHERE k = 1"), [None, None])
            assert_rows(execute(cql, table, "SELECT m['0'..'1'], s[0..1] FROM %s WHERE k = 1"), [dict(), set()])
            assert_rows(execute(cql, table, "SELECT m['0'..'1']['3'..'5'], s[0..1][3..5] FROM %s WHERE k = 1"), [dict(), set()])

            assert_rows(execute(cql, table, "SELECT m, s FROM %s WHERE k = 2"), [dict(), set()])
            assert_rows(execute(cql, table, "SELECT m['0'], s[0] FROM %s WHERE k = 2"), [None, None])
            assert_rows(execute(cql, table, "SELECT m['0'..'1'], s[0..1] FROM %s WHERE k = 2"), [dict(), set()])
            assert_rows(execute(cql, table, "SELECT m['0'..'1']['3'..'5'], s[0..1][3..5] FROM %s WHERE k = 2"), [dict(), set()])

            assert_rows(execute(cql, table, "SELECT m, s FROM %s WHERE k = 3"), [{"2": 2}, {2}])
            assert_rows(execute(cql, table, "SELECT m['0'], s[0] FROM %s WHERE k = 3"), [None, None])
            assert_rows(execute(cql, table, "SELECT m['0'..'1'], s[0..1] FROM %s WHERE k = 3"), [dict(), set()])
            assert_rows(execute(cql, table, "SELECT m['0'..'1']['3'..'5'], s[0..1][3..5] FROM %s WHERE k = 3"), [dict(), set()])

    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, m map<text, int>, s set<int>)") as table:
        execute(cql, table, "INSERT INTO %s(k) VALUES (0)")
        execute(cql, table, "INSERT INTO %s(k, m, s) VALUES (1, {}, {})")
        execute(cql, table, "INSERT INTO %s(k, m, s) VALUES (2, ?, ?)", dict(), {})
        execute(cql, table, "INSERT INTO %s(k, m, s) VALUES (3, {'2':2}, {2})")

        for _ in before_and_after_flush(cql, table):
            assert_rows(execute(cql, table, "SELECT m, s FROM %s WHERE k = 0"), [None, None])
            assert_rows(execute(cql, table, "SELECT m['0'], s[0] FROM %s WHERE k = 0"), [None, None])
            assert_rows(execute(cql, table, "SELECT m['0'..'1'], s[0..1] FROM %s WHERE k = 0"), [None, None])
            assert_rows(execute(cql, table, "SELECT m['0'..'1']['3'..'5'], s[0..1][3..5] FROM %s WHERE k = 0"), [None, None])

            assert_rows(execute(cql, table, "SELECT m, s FROM %s WHERE k = 1"), [None, None])
            assert_rows(execute(cql, table, "SELECT m['0'], s[0] FROM %s WHERE k = 1"), [None, None])
            assert_rows(execute(cql, table, "SELECT m['0'..'1'], s[0..1] FROM %s WHERE k = 1"), [None, None])
            assert_rows(execute(cql, table, "SELECT m['0'..'1']['3'..'5'], s[0..1][3..5] FROM %s WHERE k = 1"), [None, None])

            assert_rows(execute(cql, table, "SELECT m, s FROM %s WHERE k = 2"), [None, None])
            assert_rows(execute(cql, table, "SELECT m['0'], s[0] FROM %s WHERE k = 2"), [None, None])
            assert_rows(execute(cql, table, "SELECT m['0'..'1'], s[0..1] FROM %s WHERE k = 2"), [None, None])
            assert_rows(execute(cql, table, "SELECT m['0'..'1']['3'..'5'], s[0..1][3..5] FROM %s WHERE k = 2"), [None, None])

            assert_rows(execute(cql, table, "SELECT m, s FROM %s WHERE k = 3"), [{"2": 2}, {2}])
            assert_rows(execute(cql, table, "SELECT m['0'], s[0] FROM %s WHERE k = 3"), [None, None])
            assert_rows(execute(cql, table, "SELECT m['0'..'1'], s[0..1] FROM %s WHERE k = 3"), [None, None])
            assert_rows(execute(cql, table, "SELECT m['0'..'1']['3'..'5'], s[0..1][3..5] FROM %s WHERE k = 3"), [None, None])
