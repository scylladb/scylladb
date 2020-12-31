
#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

#############################################################################
# Various tests for the handling of frozen collections. Note that Cassandra
# also had extensive tests for frozen collections, which we ported in 
# cassandra_tests/validation/entities/frozen_collections_test.py. The tests
# here are either additional ones, focusing on more esoteric issues.
#############################################################################

import pytest
import random
from cassandra.util import SortedSet, OrderedMapSerializedKey
from util import unique_name


# A test table with a (frozen) nested collection as its primary key.
@pytest.fixture(scope="session")
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
@pytest.mark.xfail(reason="issue #7856")
def test_wrong_set_order_in_nested(cql, table1):
    k = random.randint(1,1000000000)
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
@pytest.mark.xfail(reason="issue #7856")
def test_wrong_set_order_in_nested_2(cql, table1):
    k = random.randint(1,1000000000)
    insert = cql.prepare(f"INSERT INTO {table1} (k) VALUES (?)")
    # Insert map with frozen-set key in "wrong" order:
    cql.execute(insert, [{tuple([k+1, k, k+2]): 1}])
    # This is an inefficient scan of the entire table, it can see more than
    # the item we just inserted, but it doesn't matter.
    for row in cql.execute(f"SELECT * FROM {table1}"):
        assert isinstance(row.k, OrderedMapSerializedKey)
        # The Python driver implements k.items() inefficiently - it has
        # a list of keys as SortedSet objects, and converts them back to
        # seralized strings to look them up in k._index. But this
        # conversion re-sorts the set and then it can't be found because
        # of issue #7856, so k.items() will throw KeyError when iterated.
        list(row.k.items())

# In test_wrong_set_order() we demonstrate that issue #7856 reproduced by
# test_wrong_set_order_nested() is somehow specific to the nested collection
# setup described there, it does not happen with just a frozen<set>.
# With just a frozen<set>, we can insert it or look it up in a wrong
# order, and everything is fine.
@pytest.fixture(scope="session")
def table_fsi(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (k frozen<set<int>> PRIMARY KEY)")
    yield table
    cql.execute("DROP TABLE " + table)
def test_wrong_set_order(cql, table_fsi):
    i = random.randint(1,1000000000)
    insert = cql.prepare(f"INSERT INTO {table_fsi} (k) VALUES (?)")
    lookup = cql.prepare(f"SELECT * FROM {table_fsi} WHERE k = ?")
    cql.execute(insert, [tuple([i+1, i, i+2])])
    assert len(list(cql.execute(lookup, [tuple([i, i+1, i+2])]))) == 1
    assert len(list(cql.execute(lookup, [tuple([i+1, i, i+2])]))) == 1
