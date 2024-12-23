
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Various tests for the handling of frozen collections. Note that Cassandra
# also had extensive tests for frozen collections, which we ported in 
# cassandra_tests/validation/entities/frozen_collections_test.py. The tests
# here are either additional ones, or focusing on more esoteric issues or
# small tests aiming to reproduce bugs discovered by bigger Cassandra tests.
#############################################################################

import pytest
from cassandra.util import SortedSet, OrderedMapSerializedKey
from util import unique_name, new_test_table, unique_key_int


# A test table with a (frozen) nested collection as its primary key.
@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (k frozen<map<set<int>, int>> PRIMARY KEY)")
    yield table
    cql.execute("DROP TABLE " + table)

# In Scylla, Sets are stored in sorted order (by the natural order of their
# contained elements) no matter what order the elements are added. It is also
# natural to assume that this should also be true for *frozen* sets, although
# this is not documented anywhere. This test reproduces issue #7856, where if
# we use a prepared statement, we can cause Scylla to store a frozen set in
# the wrong order, and therefore be unable to find it later via a different
# order.
# Note that issue #7856 can only be reproduced for a nested frozen collection,
# where a set is the key of a map. If we had just a frozen set, this issue
# does not reproduce (see test_wrong_set_order() below).
def test_wrong_set_order_in_nested(cql, table1):
    k = unique_key_int()
    # When inserting or selecting with a frozen<map<set<int>, int>> key
    # where the value is specified inline in CQL, the order of the set
    # does not matter, as expected:
    def s(*args):
        return ",".join([str(i) for i in args])
    cql.execute("INSERT INTO " + table1 + " (k) VALUES ({{" + s(k, k+1, k+2) + "}: 1})")
    assert len(list(cql.execute("SELECT * FROM " + table1 + " WHERE k = {{" + s(k, k+1, k+2) + "}: 1}"))) == 1
    assert len(list(cql.execute("SELECT * FROM " + table1 + " WHERE k = {{" + s(k+1, k, k+2) + "}: 1}"))) == 1
    k = k + 1 # advance k so the next test will write to a different item
    cql.execute("INSERT INTO " + table1 + " (k) VALUES ({{" + s(k+1, k, k+2) + "}: 1})")
    assert len(list(cql.execute("SELECT * FROM " + table1 + " WHERE k = {{" + s(k, k+1, k+2) + "}: 1}"))) == 1
    assert len(list(cql.execute("SELECT * FROM " + table1 + " WHERE k = {{" + s(k+1, k, k+2) + "}: 1}"))) == 1
    k = k + 1

    # Try the same with prepared statements. Here we can trigger issue #7856.
    insert = cql.prepare(f"INSERT INTO {table1} (k) VALUES (?)")
    lookup = cql.prepare(f"SELECT * FROM {table1} WHERE k = ?")
    cql.execute(insert, [{tuple([k, k+1, k+2]): 1}])
    assert len(list(cql.execute("SELECT * FROM " + table1 + " WHERE k = {{" + s(k, k+1, k+2) + "}: 1}"))) == 1
    assert len(list(cql.execute("SELECT * FROM " + table1 + " WHERE k = {{" + s(k+1, k, k+2) + "}: 1}"))) == 1
    assert len(list(cql.execute(lookup, [{tuple([k, k+1, k+2]): 1}]))) == 1
    # This lookup by wrong order triggers issue #7856: 
    assert len(list(cql.execute(lookup, [{tuple([k+1, k, k+2]): 1}]))) == 1
    k = k + 1
    # This insert by wrong order triggers issue #7856 in the lookups which follow.
    cql.execute(insert, [{tuple([k+1, k, k+2]): 1}])
    assert len(list(cql.execute("SELECT * FROM " + table1 + " WHERE k = {{" + s(k, k+1, k+2) + "}: 1}"))) == 1
    assert len(list(cql.execute("SELECT * FROM " + table1 + " WHERE k = {{" + s(k+1, k, k+2) + "}: 1}"))) == 1
    assert len(list(cql.execute(lookup, [{tuple([k, k+1, k+2]): 1}]))) == 1
    assert len(list(cql.execute(lookup, [{tuple([k+1, k, k+2]): 1}]))) == 1


# This demonstrates a strange side-effect of issue #7856: The Python driver
# does some unnecessary back-and-forth conversions of a frozen set used as
# a map key between a serialized string and a Python object, and gets
# confused when the frozen set is not correctly sorted and gets re-sorted
# by the serialization. In light of the simpler reproducer above in
# test_wrong_set_order_in_nested(), this one is unnecessary, but historically
# this was the first problem we noticed. Actually, this is a simplification
# of a problem in cassandra_tests/validation/entities/frozen_collections_test.py
# testNestedPartitionKeyUsage where we inserted set set as a Python frozenset()
# which does not use any specific order.
def test_wrong_set_order_in_nested_2(cql, table1):
    k = unique_key_int()
    insert = cql.prepare(f"INSERT INTO {table1} (k) VALUES (?)")
    # Insert map with frozen-set key in "wrong" order:
    cql.execute(insert, [{tuple([k+1, k, k+2]): 1}])
    # This is an inefficient scan of the entire table, it can see more than
    # the item we just inserted, but it doesn't matter.
    for row in cql.execute(f"SELECT * FROM {table1}"):
        assert isinstance(row.k, OrderedMapSerializedKey)
        # The Python driver implements k.items() inefficiently - it has
        # a list of keys as SortedSet objects, and converts them back to
        # serialized strings to look them up in k._index. But this
        # conversion re-sorts the set and then it can't be found because
        # of issue #7856, so k.items() will throw KeyError when iterated.
        list(row.k.items())

# In test_wrong_set_order() we demonstrate that issue #7856 reproduced by
# test_wrong_set_order_nested() is somehow specific to the nested collection
# setup described there, it does not happen with just a frozen<set>.
# With just a frozen<set>, we can insert it or look it up in a wrong
# order, and everything is fine.
@pytest.fixture(scope="module")
def table_fsi(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (k frozen<set<int>> PRIMARY KEY)")
    yield table
    cql.execute("DROP TABLE " + table)
def test_wrong_set_order(cql, table_fsi):
    i = unique_key_int()
    insert = cql.prepare(f"INSERT INTO {table_fsi} (k) VALUES (?)")
    lookup = cql.prepare(f"SELECT * FROM {table_fsi} WHERE k = ?")
    cql.execute(insert, [tuple([i+1, i, i+2])])
    assert len(list(cql.execute(lookup, [tuple([i, i+1, i+2])]))) == 1
    assert len(list(cql.execute(lookup, [tuple([i+1, i, i+2])]))) == 1

# This test shows we can use a frozen collection as a clustering key, and
# insert such items (with unprepared and prepared statements). The next
# test, test_clustering_key_reverse_frozen_collection, does exactly the same
# thing but with reversed sort order, and there we start having problems.
def test_clustering_key_frozen_collection(cql, test_keyspace):
    with new_test_table(cql, test_keyspace,
            "a int, b frozen<set<int>>, PRIMARY KEY (a,b)") as table:
        # Insert with unprepared statement:
        cql.execute("INSERT INTO " + table + " (a, b) VALUES (0, {1, 2, 3})")
        assert list(cql.execute("SELECT * FROM " + table)) == [(0, {1, 2, 3})]
        # Insert with prepared statement:
        stmt = cql.prepare(f"INSERT INTO {table} (a, b) VALUES (?, ?)")
        cql.execute(stmt, [0, {2, 3, 4}])
        assert list(cql.execute("SELECT * FROM " + table)) == [
            (0, {1, 2, 3}), (0, {2, 3, 4})]

# This is the same test as above (test_clustering_key_frozen_collection),
# just the clustering key is sorted in reverse order. Somehow, this broke
# things, both for unprepared and for prepared statements.
# We have the same test for frozen sets, and then lists and maps.
# Reproduces issues #7868 and #7875.
def test_clustering_key_reverse_frozen_set(cql, test_keyspace):
    with new_test_table(cql, test_keyspace,
            "a int, b frozen<set<int>>, PRIMARY KEY (a,b)",
            "WITH CLUSTERING ORDER BY (b DESC)") as table:
        # Insert with unprepared statement:
        # Issue #7875: Scylla produces an error "Invalid set literal for b
        # of type frozen<set<int>>", while this should be valid (and we saw
        # above it is allowed for increasing order!)
        cql.execute("INSERT INTO " + table + " (a, b) VALUES (0, {1, 2, 3})")
        assert list(cql.execute("SELECT * FROM " + table)) == [(0, {1, 2, 3})]
        # Insert with prepared statement:
        # Issue #7868: The prepare() of the following statement crashes
        # Scylla with an assertion failure.
        stmt = cql.prepare(f"INSERT INTO {table} (a, b) VALUES (?, ?)")
        cql.execute(stmt, [0, {2, 3, 4}])
        assert list(cql.execute("SELECT * FROM " + table)) == [
            (0, {2, 3, 4}), (0, {1, 2, 3})]

# Reproduces issues #7868 and #7875.
def test_clustering_key_reverse_frozen_list(cql, test_keyspace):
    with new_test_table(cql, test_keyspace,
            "a int, b frozen<list<int>>, PRIMARY KEY (a,b)",
            "WITH CLUSTERING ORDER BY (b DESC)") as table:
        # Insert with unprepared statement. Issue #7875.
        cql.execute("INSERT INTO " + table + " (a, b) VALUES (0, [1, 2, 3])")
        assert list(cql.execute("SELECT * FROM " + table)) == [(0, [1, 2, 3])]
        # Insert with prepared statement. Issue #7868.
        stmt = cql.prepare(f"INSERT INTO {table} (a, b) VALUES (?, ?)")
        cql.execute(stmt, [0, [2, 3, 4]])
        assert list(cql.execute("SELECT * FROM " + table)) == [
            (0, [2, 3, 4]), (0, [1, 2, 3])]

# Reproduces issues #7868 and #7875.
def test_clustering_key_reverse_frozen_map(cql, test_keyspace):
    with new_test_table(cql, test_keyspace,
            "a int, b frozen<map<int, int>>, PRIMARY KEY (a,b)",
            "WITH CLUSTERING ORDER BY (b DESC)") as table:
        # Insert with unprepared statement. Issue #7875.
        cql.execute("INSERT INTO " + table + " (a, b) VALUES (0, {1: 2, 3: 4})")
        assert list(cql.execute("SELECT * FROM " + table)) == [(0, {1: 2, 3: 4})]
        # Insert with prepared statement. Issue #7868.
        stmt = cql.prepare(f"INSERT INTO {table} (a, b) VALUES (?, ?)")
        cql.execute(stmt, [0, {2: 3, 4: 5}])
        assert list(cql.execute("SELECT * FROM " + table)) == [
            (0, {2: 3, 4: 5}), (0, {1: 2, 3: 4})]
