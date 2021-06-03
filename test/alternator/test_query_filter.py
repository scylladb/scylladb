# Copyright 2020-present ScyllaDB
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

# Tests for the QueryFilter parameter of the Query operation.
# QueryFilter is the older version of the newer FilterExpression syntax,
# which is tested in test_filter_expression.py.

import pytest
from botocore.exceptions import ClientError, ParamValidationError
import random
from util import full_query, full_query_and_counts, random_string, random_bytes

# The test_table_sn_with_data fixture is the regular test_table_sn fixture
# with a partition inserted with 20 items. The sort key 'c' of the items
# are just increasing integers - QueryFilter doesn't support filtering
# on the sort key, so testing it is not our goal. Each item includes
# additional attributes of different types with random values, on which we
# can test various filtering conditions.
# The table, the partition key, and the items are returned by the fixture.
# The items are sorted (by column 'c'), so they have the same order as
# expected to be returned by a query.
# This fixture is useful for writing many small filtering tests which read
# the# same input data without needing to re-insert data for every test, so
# overall the test suite is faster.
def random_i():
    return random.randint(1,1000000)
def random_s():
    return random_string(length=random.randint(7,15))
def random_b():
    return random_bytes(length=random.randint(7,15))
def random_l():
    return [random_i() for i in range(random.randint(1,3))]
def random_m():
    return {random_s(): random_i() for i in range(random.randint(1,3))}
def random_set():
    return set([random_i() for i in range(random.randint(1,3))])
def random_sets():
    return set([random_s() for i in range(random.randint(1,3))])
def random_bool():
    return bool(random.randint(0,1))
def random_item(p, i):
    item = {'p': p, 'c': i, 's': random_s(), 'b': random_b(), 'i': random_i(),
            'l': random_l(), 'm': random_m(), 'ns': random_set(),
            'ss': random_sets(), 'bool': random_bool() }
    # The "r" attribute doesn't appears on all items, and when it does it has a random type
    if i == 0:
        # Ensure that the first item always has an 'r' value.
        t = random.randint(1,4)
    else:
        t = random.randint(0,4)
    if t == 1:
        item['r'] = random_i()
    elif t == 2:
        item['r'] = random_s()
    elif t == 3:
        item['r'] = random_b()
    elif t == 4:
        item['r'] = [random_i(), random_i()]   # a list
    # Some of the items have j=i, others don't
    if t == 0:
        item['j'] = item['i']
    return item
@pytest.fixture(scope="module")
def test_table_sn_with_data(test_table_sn):
    p = random_string()
    # TODO: because we use random items here, it may be difficult to reproduce
    # a failing test. We should use the same seed to generate these items.
    items = [random_item(p, i) for i in range(20)]
    with test_table_sn.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
        # Add another partition just to make sure that a query of just
        # partition p can't just match the entire table and still succeed
        batch.put_item({'p': random_string(), 'c': 123, 'a': random_string()})
    yield test_table_sn, p, items


# QueryFilter cannot be used to filter on the partition key - the user
# must use KeyCondition or KeyConditionExpression instead.
# In the first test we don't use a KeyCondition at all and get a generic
# error message about Query always needing one. In the second test we do
# have a KeyCondition plus a (redundent) QueryFilter on the key attribute,
# and that isn't allowed either.
def test_query_filter_partition_key_1(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    with pytest.raises(ClientError, match='ValidationException.*Condition'):
        full_query(table, QueryFilter={
            'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }})

def test_query_filter_partition_key_2(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    with pytest.raises(ClientError, match='ValidationException.* p'):
        full_query(table,
            QueryFilter={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
            KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }})

# QueryFilter is also not allowed on the sort key.
def test_query_filter_sort_key(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    with pytest.raises(ClientError, match='ValidationException.* key '):
        full_query(table,
            KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
            QueryFilter={ 'c': { 'AttributeValueList': [3], 'ComparisonOperator': 'EQ' }})

# Having a filter on a key column is the problem - it doesn't help if we
# also have an additional filter on a non-key column.
def test_query_filter_sort_key_2(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    with pytest.raises(ClientError, match='ValidationException.* key '):
        full_query(table,
            KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
            QueryFilter={ 'c': { 'AttributeValueList': [3], 'ComparisonOperator': 'EQ' },
                            'i': { 'AttributeValueList': [3], 'ComparisonOperator': 'EQ' }})

# Tests for the different ComparisonOperator operators. These tests will
# each test a single condition. Later we will have tests for boolean
# combinations (AND or OR) of multiple conditions.

# Test the EQ operator on different types of attributes (numeric, string,
# bytes, list, map, set, bool):
def test_query_filter_eq(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    for xn in ['i', 's', 'b', 'l', 'm', 'ns', 'bool']:
        xv = items[2][xn]
        got_items = full_query(table,
            KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
            QueryFilter={ xn: { 'AttributeValueList': [xv], 'ComparisonOperator': 'EQ' }})
        expected_items = [item for item in items if item[xn] == xv]
        assert(got_items == expected_items)

# Test the EQ operator on the 'r' attribute, which only exists for some
# of the items and has different types when it does. Obviously, equality
# happens when the attribute has the expected type, and the same value.
def test_query_filter_r_eq(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    # note that random_item() guarantees the first item has an 'r':
    r = items[0]['r']
    got_items = full_query(table,
        KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
        QueryFilter={ 'r': { 'AttributeValueList': [r], 'ComparisonOperator': 'EQ' }})
    expected_items = [item for item in items if 'r' in item and item['r'] == r]
    assert(got_items == expected_items)

# Test the NE operator on different types of attributes (numeric, string,
# bytes, list, map, set, bool):
def test_query_filter_ne(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    for xn in ['i', 's', 'b', 'l', 'm', 'ns', 'bool']:
        xv = items[2][xn]
        got_items = full_query(table,
            KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
            QueryFilter={ xn: { 'AttributeValueList': [xv], 'ComparisonOperator': 'NE' }})
        expected_items = [item for item in items if item[xn] != xv]
        assert(got_items == expected_items)

# Test the NE operator on the 'r' attribute, which only exists for some
# of the items and has different types when it does. If an attribute doesn't
# exist at all, or has the wrong type, it is considered "not equal".
def test_query_filter_r_ne(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    # note that random_item() guarantees the first item has an 'r':
    r = items[0]['r']
    got_items = full_query(table,
        KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
        QueryFilter={ 'r': { 'AttributeValueList': [r], 'ComparisonOperator': 'NE' }})
    expected_items = [item for item in items if not 'r' in item or item['r'] != r]
    assert(got_items == expected_items)

# Test the LT operator on a numeric, string and bytes attributes:
# Note that the DynamoDB documentation specifies that bytes are considered
# unsigned (0...255) and sorted as such. This is the same order that
# Python guarantees, the Python's "<" operator does exactly what we want.
def test_query_filter_lt(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    for xn in ['i', 's', 'b']:
        print("testing {}".format(xn))
        xv = items[2][xn]
        got_items = full_query(table,
            KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
            QueryFilter={ xn: { 'AttributeValueList': [xv], 'ComparisonOperator': 'LT' }})
        expected_items = [item for item in items if item[xn] < xv]
        assert(got_items == expected_items)

# Other types - lists, maps, sets and bool - cannot be used as a parameter
# to LT - or any of the other comparison operators - LE, GT and GE.
def test_query_filter_uncomparable_types(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    for op in ['LT', 'LE', 'GT', 'GE']:
        for xn in ['l', 'm', 'ns', 'bool']:
            xv = items[2][xn]
            with pytest.raises(ClientError, match='ValidationException.* type'):
                full_query(table,
                    KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
                    QueryFilter={ xn: { 'AttributeValueList': [xv], 'ComparisonOperator': op }})

# Test the LE operator on a numeric, string and bytes attributes:
def test_query_filter_le(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    for xn in ['i', 's', 'b']:
        xv = items[2][xn]
        got_items = full_query(table,
            KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
            QueryFilter={ xn: { 'AttributeValueList': [xv], 'ComparisonOperator': 'LE' }})
        expected_items = [item for item in items if item[xn] <= xv]
        assert(got_items == expected_items)

# Test the GT operator on a numeric, string and bytes attributes:
def test_query_filter_gt(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    for xn in ['i', 's', 'b']:
        xv = items[2][xn]
        got_items = full_query(table,
            KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
            QueryFilter={ xn: { 'AttributeValueList': [xv], 'ComparisonOperator': 'GT' }})
        expected_items = [item for item in items if item[xn] > xv]
        assert(got_items == expected_items)

# Test the GE operator on a numeric, string and bytes attributes:
def test_query_filter_ge(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    for xn in ['i', 's', 'b']:
        xv = items[2][xn]
        got_items = full_query(table,
            KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
            QueryFilter={ xn: { 'AttributeValueList': [xv], 'ComparisonOperator': 'GE' }})
        expected_items = [item for item in items if item[xn] >= xv]
        assert(got_items == expected_items)

# Test the "BETWEEN" operator on a numeric, string and bytes attributes:
def test_query_filter_between(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    for xn in ['i', 's', 'b']:
        xv1 = items[2][xn]
        xv2 = items[3][xn]
        if xv1 > xv2:
            xv1, xv2 = xv2, xv1
        got_items = full_query(table,
            KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
            QueryFilter={ xn: { 'AttributeValueList': [xv1, xv2], 'ComparisonOperator': 'BETWEEN' }})
        expected_items = [item for item in items if item[xn] >= xv1 and item[xn] <= xv2]
        assert(got_items == expected_items)

# BETWEEN requires the upper bound to be greater than or equal to the lower bound.
def test_query_filter_num_between_reverse(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    i1 = items[2]['i']
    i2 = items[3]['i']
    if i1 < i2:
        i1, i2 = i2, i1
    if i1 == i2:
        i1 = i1 + 1
    with pytest.raises(ClientError, match='ValidationException.* BETWEEN'):
        full_query(table,
            KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
            QueryFilter={ 'i': { 'AttributeValueList': [i1, i2], 'ComparisonOperator': 'BETWEEN' }})

# If the two arguments to BETWEEN have different types, this is an error
# (not just matching nothing)
def test_query_filter_between_different_types(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    with pytest.raises(ClientError, match='ValidationException.* type'):
        full_query(table,
            KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
            QueryFilter={ 'i': { 'AttributeValueList': ['a', 3], 'ComparisonOperator': 'BETWEEN' }})

# Other types - lists, maps, sets and bool - cannot be used as parameters
# to BETWEEN, just like we tested above they cannot be used for LT, GT, etc.
def test_query_filter_between_uncomparable_types(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    for xn in ['l', 'm', 'ns', 'bool']:
        xv1 = items[2][xn]
        xv2 = items[3][xn]
        with pytest.raises(ClientError, match='ValidationException.* type'):
            full_query(table,
                KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
                QueryFilter={ xn: { 'AttributeValueList': [xv1, xv2], 'ComparisonOperator': 'BETWEEN' }})

# The BETWEEN operator needs exactly two parameters
def test_query_filter_between_needs_two(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    for params in [[], [2], [2,3,4]]:
        with pytest.raises(ClientError, match='ValidationException.*BETWEEN'):
            full_query(table,
                KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
                QueryFilter={ 'i': { 'AttributeValueList': params, 'ComparisonOperator': 'BETWEEN' }})

# Test the IN operator on different types of attributes. Interestingly,
# only numeric, string, and bytes are supported - list, map, set, or bool
# are not supported - I don't know why.
def test_query_filter_in(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    for xn in ['i', 's', 'b']:
        xv1 = items[2][xn]
        xv2 = items[7][xn]
        xv3 = items[4][xn]
        got_items = full_query(table,
            KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
            QueryFilter={ xn: { 'AttributeValueList': [xv1, xv2, xv3], 'ComparisonOperator': 'IN' }})
        expected_items = [item for item in items if item[xn] == xv1 or item[xn] == xv2 or item[xn] == xv3]
        assert(got_items == expected_items)

# The IN operator can have any number of parameters, but *not* zero.
def test_query_filter_in_empty(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    with pytest.raises(ClientError, match='ValidationException.*IN'):
        full_query(table,
            KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
            QueryFilter={ 'i': { 'AttributeValueList': [], 'ComparisonOperator': 'IN' }})

# If the arguments to IN have different types, this is considered an error.
# Unlike BETWEEN which has the same requirement, in IN different types could
# have been supported - but they are not.
def test_query_filter_in_different_types(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    with pytest.raises(ClientError, match='ValidationException.* type'):
        full_query(table,
            KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
            QueryFilter={ 'i': { 'AttributeValueList': ['a', 3], 'ComparisonOperator': 'IN' }})

# Test the BEGINS_WITH operator on the two types it supports - strings and
# byte, and that it fails on all other types.
def test_query_filter_begins(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    for xn in ['s', 'b']:
        start = items[2][xn][0:2]
        got_items = full_query(table,
            KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
            QueryFilter={ xn: { 'AttributeValueList': [start], 'ComparisonOperator': 'BEGINS_WITH' }})
        expected_items = [item for item in items if item[xn].startswith(start)]
        assert(got_items == expected_items)
    for xn in ['i', 'l', 'm', 'ns', 'bool']:
        xv = items[2][xn]
        with pytest.raises(ClientError, match='ValidationException.* type'):
            full_query(table,
                KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
            QueryFilter={ xn: { 'AttributeValueList': [xv], 'ComparisonOperator': 'BEGINS_WITH' }})

# The "CONTAINS" operator checks for two unrelated conditions:
#  * Whether an attribute is a string and contains a given string as a
#    substring.
#    (and same for byte arrays)
#  * Whether an attribute is a list or set and contains a given value as one
#    of its member.
# The following two tests check those two distinct cases.
# They also check the negated condition, "NOT_CONTAINS".
#
# NOTE: Don't confuse this definition of CONTAINS for other things (equality
# check, subset check, etc.) which contains() does not do.

def test_query_filter_contains_member(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    for op in ['CONTAINS', 'NOT_CONTAINS']:
        # Check that we can find members in both lists and sets.
        # We also check ss, with string members, to ensure that for a string
        # parameter membership check is also done, not just substring check.
        for xn in ['ns', 'ss', 'l']:
            xv = next(iter(items[2][xn]))
            got_items = full_query(table,
                KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
                QueryFilter={ xn: { 'AttributeValueList': [xv], 'ComparisonOperator': op }})
            if op == 'CONTAINS':
                expected_items = [item for item in items if xv in item[xn]]
            else:
                expected_items = [item for item in items if not xv in item[xn]]
            assert(got_items == expected_items)

def test_query_filter_contains_substring(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    for op in ['CONTAINS', 'NOT_CONTAINS']:
        # Test for both string and bytes substring checks:
        for xn in ['s', 'b']:
            substring = items[2][xn][2:4]
            got_items = full_query(table,
                KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
                QueryFilter={ xn: { 'AttributeValueList': [substring], 'ComparisonOperator': op }})
            if op == 'CONTAINS':
                expected_items = [item for item in items if substring in item[xn]]
            else:
                expected_items = [item for item in items if not substring in item[xn]]
            assert(got_items == expected_items)

# Test the NULL and NOT_NULL operators. Note that despite the operator's
# name, these do *not* check for the "NULL" value - rather they just check
# for items that have (or don't have) the given attribute.
def test_query_filter_null(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    for op in ['NULL', 'NOT_NULL']:
        # Note the "r" attribute is missing from some items, so is useful
        # for this test.
        got_items = full_query(table,
            KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
            QueryFilter={ 'r': { 'AttributeValueList': [], 'ComparisonOperator': op }})
        if op == 'NULL':
            expected_items = [item for item in items if not 'r' in item]
        else:
            expected_items = [item for item in items if 'r' in item]
        assert(got_items == expected_items)

# Operator names are case sensitive. "EQ" is fine, "eq" is not.
def test_query_filter_case_sensitive(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    with pytest.raises(ClientError, match='ValidationException.*eq'):
        full_query(table,
            KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
            QueryFilter={ 'i': { 'AttributeValueList': [3], 'ComparisonOperator': 'eq' }})

# Obviously, an unknown operators is an error too.
def test_query_filter_unknown_operator(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    with pytest.raises(ClientError, match='ValidationException.*DOG'):
        full_query(table,
            KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
            QueryFilter={ 'i': { 'AttributeValueList': [3], 'ComparisonOperator': 'DOG' }})

# Filtering may not match any item and return an empty result set.
# This is fine.
def test_query_filter_empty_results(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    got_items = full_query(table,
        KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
        QueryFilter={ 'non_existent': { 'AttributeValueList': [3], 'ComparisonOperator': 'EQ' }})
    assert(got_items == [])

# All the tests above involved just one condition. Let's test now multiple
# conditions, which by default are ANDed, unless ConditionalOperator is set
# to OR.
def test_query_filter_and(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    # The default when two conditions are given is to AND them:
    i = items[2]['i']
    s = items[2]['s']
    got_items = full_query(table,
        KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
        QueryFilter={ 'i': { 'AttributeValueList': [i], 'ComparisonOperator': 'EQ' },
                      's': { 'AttributeValueList': [s], 'ComparisonOperator': 'EQ' }})
    expected_items = [item for item in items if item['i'] == i and item['s'] == s]
    assert(got_items == expected_items)
    # Setting ConditionalOperator to "AND" explicitly chooses AND:
    got_items = full_query(table,
        KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
        QueryFilter={ 'i': { 'AttributeValueList': [i], 'ComparisonOperator': 'EQ' },
                      's': { 'AttributeValueList': [s], 'ComparisonOperator': 'EQ' }},
        ConditionalOperator='AND')
    assert(got_items == expected_items)

def test_query_filter_or(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    i = items[2]['i']
    s = items[3]['s']
    got_items = full_query(table,
        KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
        QueryFilter={ 'i': { 'AttributeValueList': [i], 'ComparisonOperator': 'EQ' },
                      's': { 'AttributeValueList': [s], 'ComparisonOperator': 'EQ' }},
        ConditionalOperator='OR')
    expected_items = [item for item in items if item['i'] == i or item['s'] == s]
    assert(got_items == expected_items)

# Check that an unknown ConditionalOperator causes error. The only two allowed
# values are "AND" and "OR" (case sensitive).
def test_query_filter_invalid_conditional_operator(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    for conditional_operator in ['DOG', 'and']:
        with pytest.raises(ClientError, match='ValidationException.*'+conditional_operator):
            full_query(table,
                KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
                QueryFilter={ 'i': { 'AttributeValueList': [1], 'ComparisonOperator': 'EQ' },
                              's': { 'AttributeValueList': ['a'], 'ComparisonOperator': 'EQ' }},
                ConditionalOperator=conditional_operator)

# An empty QueryFilter allowed, and is equivalent to a missing QueryFilter
def test_query_filter_empty(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    got_items = full_query(table,
        KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
        QueryFilter={ })
    assert(got_items == items)

# Test Count and ScannedCount feature of Query, with and without a QueryFilter
def test_query_filter_counts(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    # First test without a filter - both Count (postfilter_count) and
    # ScannedCount (prefilter_count) should return the same count of items.
    (prefilter_count, postfilter_count, pages, got_items) = full_query_and_counts(table,
            KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }})
    assert(got_items == items)
    assert(prefilter_count == len(items))
    assert(postfilter_count == len(items))
    # Now use a filter, on the "bool" attribute so the filter will match
    # roughly half of the items. ScannedCount (prefilter_count) should still
    # returns the full number of items, but Count (postfilter_count) returns
    # should return just the number of matched items.
    (prefilter_count, postfilter_count, pages, got_items) = full_query_and_counts(table,
            KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
            QueryFilter={ 'bool': { 'AttributeValueList': [True], 'ComparisonOperator': 'EQ' }})
    expected_items = [item for item in items if item['bool'] == True]
    assert(got_items == expected_items)
    assert(prefilter_count == len(items))
    assert(postfilter_count == len(expected_items))

# Test paging of Query with a filter. We want to confirm the understanding
# that Limit controls the number of pre-filter items, not post-filter
# results. Response pages can even be completely empty if Limit results
# were all filtered out.
# In the example which we try, we use Limit=1, so if the filter matches half
# the items, half of the returned pages will be empty. Specifically, if we
# have 20 items and only match 10, we will get a grand total of 10 results
# but in 20 pages.
def test_query_filter_paging(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    # Filter on the "bool" attribute so the filter will match roughly half of
    # the items.
    (prefilter_count, postfilter_count, pages, got_items) = full_query_and_counts(table,
            KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
            QueryFilter={ 'bool': { 'AttributeValueList': [True], 'ComparisonOperator': 'EQ' }},
            Limit=1)
    expected_items = [item for item in items if item['bool'] == True]
    assert(got_items == expected_items)
    # The total number of pages may be len(items) or len(items)+1, depending
    # on the implementation: The "+1" can happen if the 20th page found one
    # last item to consider (it may or may not have passed the filter), but
    # doesn't know it is really the last item - it may take one more query to
    # discover there are no more. Currently, Alternator returns len(items)+1
    # while DynamoDB returns len(items), but neither is more correct than the
    # other - nor should any user case about this difference, as empty pages
    # are a documented possibility.
    assert(pages == len(items) or pages == len(items) + 1)

# Test that a QueryFilter and AttributesToGet may be given together.
# In particular, test that QueryFilter may inspect attributes which will
# not be returned by the query, because the AttributesToGet.
# This test reproduces issue #6951.
def test_query_filter_and_attributes_to_get(test_table):
    p = random_string()
    test_table.put_item(Item={'p': p, 'c': 'hi', 'x': 'dog', 'y': 'cat'})
    test_table.put_item(Item={'p': p, 'c': 'yo', 'x': 'mouse', 'y': 'horse'})
    # Note that the filter is on the column x, but x is not included in the
    # results because of AttributesToGet. The filter should still work.
    got_items = full_query(test_table,
        KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
        QueryFilter={ 'x': { 'AttributeValueList': ['mouse'], 'ComparisonOperator': 'EQ' }},
        AttributesToGet=['y'])
    # Note that:
    # 1. Exactly one item matches the filter on x
    # 2. The returned record for that item will include *only* the attribute y
    #    as requestd by AttributesToGet. It won't include x - it was just
    #    needed for the filter, but didn't appear in ProjectionExpression.
    expected_items = [{'y': 'horse'}]
    assert(got_items == expected_items)

# It is not allowed to combine the old-style QueryFilter with the
# new-style ProjectionExpression. You must use AttributesToGet instead
# (tested in test_query_filter_and_attributes_to_get() above).
def test_query_filter_and_projection_expression(test_table):
    p = random_string()
    # DynamoDB complains: "Can not use both expression and non-expression
    # parameters in the same request: Non-expression parameters:
    # {QueryFilter} Expression parameters: {ProjectionExpression}.
    with pytest.raises(ClientError, match='ValidationException.* both'):
        full_query(test_table,
            KeyConditions={ 'p': { 'AttributeValueList': [p], 'ComparisonOperator': 'EQ' }},
            QueryFilter={ 'x': { 'AttributeValueList': ['mouse'], 'ComparisonOperator': 'EQ' }},
            ProjectionExpression='y')
