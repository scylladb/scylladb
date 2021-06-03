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

# Tests for the FilterExpression parameter of the Query and Scan operations.
# FilterExpression is a newer version of the older "QueryFilter" and
# "ScanFilter" syntax.

import pytest
from botocore.exceptions import ClientError, ParamValidationError
import random
from util import full_query, full_scan, random_string, random_bytes, multiset

# The test_table_sn_with_data fixture is the regular test_table_sn fixture
# with a partition inserted with many items. The sort key 'c' of the items
# are just increasing integers (FilterExpression doesn't support filtering
# on the sort key, so testing it is not our goal) but each item includes
# additional attributes of different types with random values, so they can
# be filtered on.
# The table, the partition key, and the items are returned - the items are
# sorted (by column 'c'), so they have the same order as expected to be
# returned by a query.
# This fixture is useful for writing many small query tests which read the
# same input data without needing to re-insert data for every test, so
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
            'l': random_l(), 'm': random_m(), 'ns': random_set(), 'ss': random_sets(), 'bool': random_bool() }
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
    yield test_table_sn, p, items

# FilterExpression cannot be used to filter on the partition key - the user
# must use KeyCondition or KeyConditionExpression instead. In the first test
# we don't use a KeyCondition at all and get a generic error message about
# Query always needing one. In the second test we do have a KeyCondition plus
# a (redundent) FilterExpression on the key attribute, and that isn't allowed
# either.
def test_filter_expression_partition_key_1(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    with pytest.raises(ClientError, match='ValidationException.*Condition'):
        got_items = full_query(table, FilterExpression='p=:p', ExpressionAttributeValues={':p': p})

def test_filter_expression_partition_key_2(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    with pytest.raises(ClientError, match='ValidationException.* p'):
        got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='p=:p', ExpressionAttributeValues={':p': p})

# FilterExpression is also not allowed on the sort key.
def test_filter_expression_sort_key(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    with pytest.raises(ClientError, match='ValidationException.* key '):
        got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='c=:c',
            ExpressionAttributeValues={':p': p, ':c': 3})

# Test the "=" operator on different types of attributes (numeric, string,
# bytes, list, map, set, bool):
def test_filter_expression_eq(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    for xn in ['i', 's', 'b', 'l', 'm', 'ns', 'bool']:
        xv = items[2][xn]
        got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression=(xn+'=:xv'),
            ExpressionAttributeValues={':p': p, ':xv': xv})
        expected_items = [item for item in items if item[xn] == xv]
        assert(got_items == expected_items)

# As usual, whitespace in expression is ignored
def test_filter_expression_whitespace(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    i = items[2]['i']
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='   i =  :i   ',
        ExpressionAttributeValues={':p': p, ':i': i})
    expected_items = [item for item in items if item['i'] == i]
    assert(got_items == expected_items)

# As usual, if we have an expression referring to a missing attribute value,
# we get an error:
def test_filter_expression_missing_val(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    with pytest.raises(ClientError, match='ValidationException.*:i'):
        full_query(table, KeyConditionExpression='p=:p', FilterExpression='i=:i',
            ExpressionAttributeValues={':p': p})

# As usual, if we have an expression with unused values, we get an error:
def test_filter_expression_unused_val(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    with pytest.raises(ClientError, match='ValidationException.*qq'):
        full_query(table, KeyConditionExpression='p=:p', FilterExpression='i=:i',
            ExpressionAttributeValues={':p': p, ':i': 3, ':qq': 3})

# Test the "=" operator on the 'r' attribute, which only exists for some
# of the items and has different types when it does. Obviously, equality
# happens when the attribute has the expected type, and the same value.
def test_filter_expression_r_eq(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    # note that random_item() guarantees the first item has an 'r':
    r = items[0]['r']
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='r=:r',
        ExpressionAttributeValues={':p': p, ':r': r})
    expected_items = [item for item in items if 'r' in item and item['r'] == r]
    assert(got_items == expected_items)

# Test the "<>" operator on a numeric, string and bytes attributes:
def test_filter_expression_neq(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    for xn in ['i', 's', 'b']:
        xv = items[2][xn]
        got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression=(xn+'<>:xv'),
            ExpressionAttributeValues={':p': p, ':xv': xv})
        expected_items = [item for item in items if item[xn] != xv]
        assert(got_items == expected_items)

# Test the "<>" operator on the 'r' attribute, which only exists for some
# of the items and has different types when it does. If an attribute doesn't
# exist at all, or has the wrong type, it is considered "not equal".
def test_filter_expression_r_neq(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    # note that random_item() guarantees the first item has an 'r':
    r = items[0]['r']
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='r<>:r',
        ExpressionAttributeValues={':p': p, ':r': r})
    expected_items = [item for item in items if not 'r' in item or item['r'] != r]
    assert(got_items == expected_items)

# Test the "<" operator on a numeric, string and bytes attributes:
def test_filter_expression_lt(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    for xn in ['i', 's', 'b']:
        xv = items[2][xn]
        got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression=(xn+'<:xv'),
            ExpressionAttributeValues={':p': p, ':xv': xv})
        expected_items = [item for item in items if item[xn] < xv]
        assert(got_items == expected_items)

# The attribute name does not necessarily need to appear on the left side
# of the operator, it can also appear on the right side! Note how for
# "<", reversing the order obviously reverses the operator in the condition.
def test_filter_expression_num_eq_reverse(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    i = items[2]['i']
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression=':i=i',
        ExpressionAttributeValues={':p': p, ':i': i})
    expected_items = [item for item in items if item['i'] == i]
    assert(got_items == expected_items)

def test_filter_expression_num_lt_reverse(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    i = items[2]['i']
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression=':i<i',
        ExpressionAttributeValues={':p': p, ':i': i})
    expected_items = [item for item in items if i < item['i']]
    assert(got_items == expected_items)

# Test the "<=" operator on a numeric, string or bytes attribute:
def test_filter_expression_le(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    for xn in ['i', 's', 'b']:
        xv = items[2][xn]
        got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression=(xn+'<=:xv'),
            ExpressionAttributeValues={':p': p, ':xv': xv})
        expected_items = [item for item in items if item[xn] <= xv]
        assert(got_items == expected_items)

# Test the ">" operator on a numeric, string and bytes attribute:
def test_filter_expression_gt(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    for xn in ['i', 's', 'b']:
        xv = items[2][xn]
        got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression=(xn+'>:xv'),
            ExpressionAttributeValues={':p': p, ':xv': xv})
        expected_items = [item for item in items if item[xn] > xv]
        assert(got_items == expected_items)

# Test the ">=" operator on a numeric, string and bytes attribute:
def test_filter_expression_ge(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    for xn in ['i', 's', 'b']:
        xv = items[2][xn]
        got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression=(xn+'>=:xv'),
            ExpressionAttributeValues={':p': p, ':xv': xv})
        expected_items = [item for item in items if item[xn] >= xv]
        assert(got_items == expected_items)

# Comparison operators such as >= or BETWEEN only work on numbers, strings or
# bytes. When an expression's operands come from the item and has a wrong type
# (e.g., a list), the result is that the item is skipped - aborting the scan
# with a ValidationException is a bug (this was issue #8043).
def test_filter_expression_le_bad_type(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='l <= :xv',
        ExpressionAttributeValues={':p': p, ':xv': 3})
    assert got_items == []
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression=':xv <= l',
        ExpressionAttributeValues={':p': p, ':xv': 3})
    assert got_items == []
def test_filter_expression_between_bad_type(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='s between :xv and l',
        ExpressionAttributeValues={':p': p, ':xv': 'cat'})
    assert got_items == []
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='s between l and :xv',
        ExpressionAttributeValues={':p': p, ':xv': 'cat'})
    assert got_items == []
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='s between i and :xv',
        ExpressionAttributeValues={':p': p, ':xv': 'cat'})
    assert got_items == []

# Test the "BETWEEN/AND" ternary operator on a numeric, string and bytes
# attribute. These keywords are case-insensitive.
def test_filter_expression_between(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    for xn in ['i', 's', 'b']:
        xv1 = items[2][xn]
        xv2 = items[3][xn]
        if xv1 > xv2:
            xv1, xv2 = xv2, xv1
        got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression=(xn+' BetWEeN :xv1 AnD :xv2'),
            ExpressionAttributeValues={':p': p, ':xv1': xv1, ':xv2': xv2})
        expected_items = [item for item in items if item[xn] >= xv1 and item[xn] <= xv2]
        assert(got_items == expected_items)

# BETWEEN requires the upper bound to be greater than or equal to the lower bound.
def test_filter_expression_num_between_reverse(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    i1 = items[2]['i']
    i2 = items[3]['i']
    if i1 < i2:
        i1, i2 = i2, i1
    if i1 == i2:
        i1 = i1 + 1
    with pytest.raises(ClientError, match='ValidationException.* BETWEEN'):
        full_query(table, KeyConditionExpression='p=:p', FilterExpression='i BetWEeN :i1 AnD :i2',
            ExpressionAttributeValues={':p': p, ':i1': i1, ':i2': i2})

# Test the "IN" operator on a numeric, string or bytes attribute.
def test_filter_expression_in(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    for xn in ['i', 's', 'b']:
        xv1 = items[2][xn]
        xv2 = items[7][xn]
        xv3 = items[4][xn]
        got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression=(xn+' In (:xv1, :xv2, :xv3)'),
            ExpressionAttributeValues={':p': p, ':xv1': xv1, ':xv2': xv2, ':xv3': xv3})
        expected_items = [item for item in items if item[xn] == xv1 or item[xn] == xv2 or item[xn] == xv3]
        assert(got_items == expected_items)

# The begins_with function does *not* work on a numeric attributes - it only
# works on strings or bytes - we'll check those next:
def test_filter_expression_num_begins(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    i = items[2]['i']
    with pytest.raises(ClientError, match='ValidationException.*begins_with'):
        full_query(table, KeyConditionExpression='p=:p', FilterExpression='begins_with(i, :i)',
            ExpressionAttributeValues={':p': p, ':i': i})

def test_filter_expression_string_begins(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    start = items[2]['s'][0:2]
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='begins_with(s, :start)',
        ExpressionAttributeValues={':p': p, ':start': start})
    expected_items = [item for item in items if item['s'].startswith(start)]
    assert(got_items == expected_items)

def test_filter_expression_bytes_begins(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    start = items[2]['b'][0:2]
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='begins_with(b, :start)',
        ExpressionAttributeValues={':p': p, ':start': start})
    expected_items = [item for item in items if item['b'].startswith(start)]
    assert(got_items == expected_items)

# The DynamoDB document of contains() is confusing. It turns out it checks
# for two unrelated conditions:
#
#  * Whether an attribute is a string and contains a given string as a
#    substring.
#    (and same for byte arrays)
#
#  * Whether an attribute is a list or set and contains a given value as one
#    of its member.
#
# Don't confuse this definition for other things (equality check, subset
# check, etc.) which contains() does not do.

# As explained above, a number can be "contained" in an attribute if it is
# a set or list and contains this number as an element.
def test_filter_expression_num_contains(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    for xn in ['ns', 'l']:
        xv = next(iter(items[2][xn]))
        got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='contains('+xn+', :i)',
        ExpressionAttributeValues={':p': p, ':i': xv})
        expected_items = [item for item in items if xv in item[xn]]
        assert(got_items == expected_items)

# As explained above, contains() can check for the given value being a
# *member* or some set or list attribute - not the value being a *subset*
# of the attribute.
def test_filter_expression_set_contains(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    s = items[2]['ns']
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='contains(ns, :s)',
        ExpressionAttributeValues={':p': p, ':s': s})
    # A set is not a member of itself, so nothing matches.
    assert(got_items == [])

def test_filter_expression_string_contains(test_table_sn_with_data):
    # Test the "obvious" case - find strings with a given substring:
    table, p, items = test_table_sn_with_data
    substring = items[2]['s'][2:4]
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='contains(s, :substring)',
        ExpressionAttributeValues={':p': p, ':substring': substring})
    expected_items = [item for item in items if substring in item['s']]
    assert(got_items == expected_items)
    # But a string can also match sets or lists with it as a member!
    # Note that this is just a special case of matching any value as a member
    s = next(iter(items[2]['ss']))
    print(s)
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='contains(ss, :s)',
        ExpressionAttributeValues={':p': p, ':s': s})
    expected_items = [item for item in items if s in item['ss']]
    assert(got_items == expected_items)

def test_filter_expression_bytes_contains(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    substring = items[2]['b'][2:4]
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='contains(b, :substring)',
        ExpressionAttributeValues={':p': p, ':substring': substring})
    expected_items = [item for item in items if substring in item['b']]
    assert(got_items == expected_items)
    # As for strings, a byte array may also match sets or lists with it as
    # a member.

# We could also imagine what contains() could do for searching maps (check if
# the map contains the given key or value), DynamoDB doesn't support this use
# case. Nothing matches.
def test_filter_expression_map_contains(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    # One key from a map:
    i = next(iter(items[2]['m']))
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='contains(m, :i)',
        ExpressionAttributeValues={':p': p, ':i': i})
    #The following could have made sense, but it's what DynamoDB does:
    #expected_items = [item for item in items if i in item['m']]
    expected_items = []
    assert(got_items == expected_items)
    # One value from a map:
    i = next(iter(items[2]['m']))
    v = items[2]['m'][i]
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='contains(m, :i)',
        ExpressionAttributeValues={':p': p, ':i': i})
    #The following could have made sense, but it's what DynamoDB does:
    #expected_items = [item for item in items if i in item['m'].itervalues()]
    expected_items = []
    assert(got_items == expected_items)

# For a bool constant, contains() will look for lists with this value -
# it won't match the actual value. It is not not equivalent to an equality
# check.
def test_filter_expression_bool_contains(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='contains(bool, :i)',
        ExpressionAttributeValues={':p': p, ':i': True})
    assert(got_items == [])

# Test the "attribute_exists" function on the "r" attribute (which is missing
# for some of the items, and has different types for others)
def test_filter_expression_r_exists(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='attribute_exists(r)',
        ExpressionAttributeValues={':p': p})
    expected_items = [item for item in items if 'r' in item]
    assert(got_items == expected_items)

# Test the "attribute_not_exists" function on the "r" attribute
def test_filter_expression_r_not_exists(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='attribute_not_exists(r)',
        ExpressionAttributeValues={':p': p})
    expected_items = [item for item in items if not 'r' in item]
    assert(got_items == expected_items)

# Test the attribute_type(), which should support all types:
# S, SS, B, BS, N, NS, BOOL, NULL, L, M.
def test_filter_expression_attribute_type(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    # Searching for string attributes:
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='attribute_type(r, :t)',
        ExpressionAttributeValues={':p': p, ':t': 'S'})
    expected_items = [item for item in items if 'r' in item and isinstance(item['r'], str)]
    assert(got_items == expected_items)
    # Searching for number attributes:
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='attribute_type(r, :t)',
        ExpressionAttributeValues={':p': p, ':t': 'N'})
    expected_items = [item for item in items if 'r' in item and isinstance(item['r'], int)]
    assert(got_items == expected_items)
    # Searching for bytes attributes:
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='attribute_type(r, :t)',
        ExpressionAttributeValues={':p': p, ':t': 'B'})
    expected_items = [item for item in items if 'r' in item and isinstance(item['r'], bytearray)]
    assert(got_items == expected_items)
    # Test additional attribute types that we have on other attributes in all our items:
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='attribute_type(ns, :t)',
        ExpressionAttributeValues={':p': p, ':t': 'NS'})
    assert(got_items == items)
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='attribute_type(bool, :t)',
        ExpressionAttributeValues={':p': p, ':t': 'BOOL'})
    assert(got_items == items)
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='attribute_type(m, :t)',
        ExpressionAttributeValues={':p': p, ':t': 'M'})
    assert(got_items == items)
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='attribute_type(l, :t)',
        ExpressionAttributeValues={':p': p, ':t': 'L'})
    assert(got_items == items)
    # TODO: we didn't test the SS (string set), BS (bytes set) and NULL types here.
    # Other strings are not allowed as attribute_type parameters:
    with pytest.raises(ClientError, match='ValidationException.*HELLO'):
        full_query(table, KeyConditionExpression='p=:p', FilterExpression='attribute_type(r, :t)',
            ExpressionAttributeValues={':p': p, ':t': 'HELLO'})

# Test the size() function, which can only be used inside an expression such
# as size(s) < :i. The next test focuses on size() of strings (more tests below
# test what size() does on other types), and checks the different expressions
# it can be used inside.
def test_filter_expression_string_size(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    s = items[2]['s']
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='size(s) < :i',
        ExpressionAttributeValues={':p': p, ':i': len(s)})
    expected_items = [item for item in items if len(item['s']) < len(s)]
    assert(got_items == expected_items)
    # Test the rest of the operations: <=, >, >=, =, <>, BETWEEN, IN
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='size(s) <= :i',
        ExpressionAttributeValues={':p': p, ':i': len(s)})
    expected_items = [item for item in items if len(item['s']) <= len(s)]
    assert(got_items == expected_items)
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='size(s) > :i',
        ExpressionAttributeValues={':p': p, ':i': len(s)})
    expected_items = [item for item in items if len(item['s']) > len(s)]
    assert(got_items == expected_items)
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='size(s) >= :i',
        ExpressionAttributeValues={':p': p, ':i': len(s)})
    expected_items = [item for item in items if len(item['s']) >= len(s)]
    assert(got_items == expected_items)
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='size(s) = :i',
        ExpressionAttributeValues={':p': p, ':i': len(s)})
    expected_items = [item for item in items if len(item['s']) == len(s)]
    assert(got_items == expected_items)
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='size(s) <> :i',
        ExpressionAttributeValues={':p': p, ':i': len(s)})
    expected_items = [item for item in items if len(item['s']) != len(s)]
    assert(got_items == expected_items)
    s2 = items[3]['s']
    len1 = len(s)
    len2 = len(s2)
    if len1 > len2:
        len1, len2 = len2, len1
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='size(s) BETWEEN :i1 AND :i2',
        ExpressionAttributeValues={':p': p, ':i1': len1, ':i2': len2})
    expected_items = [item for item in items if len(item['s']) >= len1 and len(item['s']) <= len2]
    assert(got_items == expected_items)
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='size(s) IN (:i1, :i2)',
        ExpressionAttributeValues={':p': p, ':i1': len1, ':i2': len2})
    expected_items = [item for item in items if len(item['s']) == len1 or len(item['s']) == len2]
    assert(got_items == expected_items)

def test_filter_expression_bytes_size(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    b = items[2]['b']
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='size(b) < :i',
        ExpressionAttributeValues={':p': p, ':i': len(b)})
    expected_items = [item for item in items if len(item['b']) < len(b)]
    assert(got_items == expected_items)
    # We don't test here the other operators (>, >= etc.), it's enough to test
    # them once for strings, above.

# size() function for lists, maps and sets checks the number of their elements
def test_filter_expression_collection_size(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    for xn in ['l', 'm', 'ns']:
        xv = items[2][xn]
        got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='size('+xn+') = :len',
        ExpressionAttributeValues={':p': p, ':len': len(xv)})
        expected_items = [item for item in items if len(item[xn]) == len(xv)]
        assert(got_items == expected_items)
    # We don't test here the other operators (>, >= etc.), it's enough to test
    # them once for strings, above.

# The size() function on an integer or boolean doesn't match anything -
# it's not >=3 nor <3.
def test_filter_expression_num_size(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    for xn in ['i', 'bool']:
        got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='size('+xn+') < :i',
            ExpressionAttributeValues={':p': p, ':i': 3})
        assert(got_items == [])
        got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='size('+xn+') >= :i',
            ExpressionAttributeValues={':p': p, ':i': 3})
        assert(got_items == [])

# Unknown functions like dog() results in errors
def test_filter_expression_unknown_function(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    with pytest.raises(ClientError, match='ValidationException.*dog'):
        full_query(table, KeyConditionExpression='p=:p', FilterExpression='dog(i, :i)',
        ExpressionAttributeValues={':p': p, ':i': 3})

# Test various boolean expressions - OR, AND, NOT, parentheses. Check them
# once for numeric attributes, it's enough.
# Interestingly, while KeyConditionExpression doesn't allow multiple
# conditions on the same attribute and this is why BETWEEN and IN exist
# (see test_key_condition_expression_multi()), this is allowed here in
# FilterExpression.
def test_filter_expression_or(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    i1 = items[2]['i']
    i2 = items[5]['i']
    i3 = items[7]['i']
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='i=:i1 OR i=:i2 oR i=:i3',
            ExpressionAttributeValues={':p': p, ':i1': i1, ':i2': i2, ':i3': i3})
    expected_items = [item for item in items if item['i'] == i1 or item['i'] == i2 or item['i'] == i3]
    assert(got_items == expected_items)

def test_filter_expression_and(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    i1 = items[2]['i']
    s1 = items[2]['s']
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='i=:i1 aND s=:s1',
            ExpressionAttributeValues={':p': p, ':i1': i1, ':s1': s1})
    expected_items = [item for item in items if item['i'] == i1 and item['s'] == s1]
    assert(got_items == expected_items)

def test_filter_expression_complex(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    i1 = items[2]['i']
    s1 = items[2]['s']
    i2 = items[5]['i']
    i3 = items[7]['i']
    i4 = items[1]['i']
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='(((i=:i1) AND s=:s1) OR i=:i2 oR i=:i3) or NoT i<:i4',
            ExpressionAttributeValues={':p': p, ':i1': i1, ':s1': s1, ':i2': i2, ':i3': i3, ':i4': i4})
    expected_items = [item for item in items if item['i'] == i1 or item['i'] == i2 or item['i'] == i3 or item['i'] >= i4]
    assert(got_items == expected_items)

def test_filter_expression_precedence(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    i1 = items[2]['i']
    s1 = items[2]['s']
    i2 = items[5]['i']
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='i=:i2 OR i=:i1 AND s=:s1',
            ExpressionAttributeValues={':p': p, ':i1': i1, ':s1': s1, ':i2': i2})
    expected_items = [item for item in items if item['i'] == i1 or item['i'] == i2]
    assert(got_items == expected_items)

# A simple case of syntax error - unknown operator "!=".
def test_filter_expression_syntax(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    with pytest.raises(ClientError, match='ValidationException.*yntax'):
        full_query(table, KeyConditionExpression='p=:p', FilterExpression='i != :i',
            ExpressionAttributeValues={':p': p, ':i': 3})

# An empty FilterExpression is not allowed (it's not equivalent to a missing
# FilterExpression)
def test_filter_expression_empty(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    with pytest.raises(ClientError, match='ValidationException.*empty'):
        full_query(table, KeyConditionExpression='p=:p', FilterExpression='',
            ExpressionAttributeValues={':p': p})

# Unlike KeyConditionExpression (test_key_condition_expression_bad_compare())
# here a condition may compare two attributes to each other, not just to
# constants.
def test_filter_expression_compare_attributes(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='i=j',
            ExpressionAttributeValues={':p': p})
    expected_items = [item for item in items if 'j' in item and item['i'] == item['j']]
    assert(got_items == expected_items)

    # Interestingly,  to enforce the rule that you cannot filter on a key,
    # it is not allowed to compare an attribute to the key!
    with pytest.raises(ClientError, match='ValidationException.* c'):
        full_query(table, KeyConditionExpression='p=:p', FilterExpression='i=c',
            ExpressionAttributeValues={':p': p})

    # More than one attribute can be used not just in binary operators, the
    # same is also true for IN:
    table, p, items = test_table_sn_with_data
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='i in (j, :k)',
            ExpressionAttributeValues={':p': p, ':k': 3})
    expected_items = [item for item in items if item['i'] == 3 or ('j' in item and item['i'] == item['j'])]
    assert(got_items == expected_items)

# All tests above had the attribute names written explicitly in the expression.
# Try the same with attribute name references (#something):
def test_filter_expression_name_ref(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    i = items[2]['i']
    got_items = full_query(table, KeyConditionExpression='p=:p', FilterExpression='#name=:i',
        ExpressionAttributeValues={':p': p, ':i': i},
        ExpressionAttributeNames={'#name': 'i'})
    expected_items = [item for item in items if item['i'] == i]
    assert(got_items == expected_items)

# Missing or unused attribute name references cause errors:
def test_filter_expression_name_ref_error(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    with pytest.raises(ClientError, match='ValidationException.*name'):
        full_query(table, KeyConditionExpression='p=:p', FilterExpression='#name=:i',
        ExpressionAttributeValues={':p': p, ':i': 3})
    with pytest.raises(ClientError, match='ValidationException.*another'):
        full_query(table, KeyConditionExpression='p=:p', FilterExpression='#name=:i',
        ExpressionAttributeValues={':p': p, ':i': 3},
        ExpressionAttributeNames={'#name': 'i', '#another': 'j'})

# In all the tests above we only used KeyConditionExpression for specifying
# the partition key. But it can also give a condition on the sort key, in
# which case this condition is applied (efficiently) before the filtering.
def test_filter_expression_and_sort_key_condition(test_table_sn_with_data):
    table, p, items = test_table_sn_with_data
    i = items[2]['i']
    got_items = full_query(table, KeyConditionExpression='p=:p and c<=:c', FilterExpression='i>:i',
        ExpressionAttributeValues={':p': p, ':c': 7, ':i': i})
    expected_items = [item for item in items if item['i'] > i and item['c'] <= 7]
    assert(got_items == expected_items)

# Test that a FilterExpression and ProjectionExpression may be given together.
# In particular, test that FilterExpression may inspect attributes which will
# not be returned by the query, because of the ProjectionExpression.
# This test reproduces issue #6951.
def test_filter_expression_and_projection_expression(test_table):
    p = random_string()
    test_table.put_item(Item={'p': p, 'c': 'hi', 'x': 'dog', 'y': 'cat'})
    test_table.put_item(Item={'p': p, 'c': 'yo', 'x': 'mouse', 'y': 'horse'})
    # Note that the filter is on the column x, but x is not included in the
    # results because of ProjectionExpression. The filter should still work.
    got_items = full_query(test_table,
        KeyConditionExpression='p=:p',
        FilterExpression='x=:x',
        ProjectionExpression='y',
        ExpressionAttributeValues={':p': p, ':x': 'mouse'})
    # Note that:
    # 1. Exactly one item matches the filter on x
    # 2. The returned record for that item will include *only* the attribute y
    #    as requestd by ProjectionExpression. It won't include x - it was just
    #    needed for the filter, but didn't appear in ProjectionExpression.
    expected_items = [{'y': 'horse'}]
    assert(got_items == expected_items)

# Same test as test_filter_expression_and_projection_expression above, just
# here the projected attribute is a key column p, whereas it was a non-key
# in the previous test. Although only key columns are being projected, it
# is important that the implementation also reads the non-key columns (the
# ":attrs" column) - they are needed for the filter. The current code reads
# all the columns in any case, so passing this test is easy, but if in the
# future we add an optimization to not read ":attrs" when not needed, this
# test will make sure "when not needed" remembers also the filtering.
# This test also reproduces issue #6951.
def test_filter_expression_and_projection_expression_2(test_table):
    p = random_string()
    test_table.put_item(Item={'p': p, 'c': 'yo', 'x': 'mouse', 'y': 'horse'})
    # Note that the filter is on the column x, but x is not included in the
    # results because of ProjectionExpression. The filter should still work.
    got_items = full_query(test_table,
        KeyConditionExpression='p=:p',
        FilterExpression='x=:x',
        ProjectionExpression='p',
        ExpressionAttributeValues={':p': p, ':x': 'mouse'})
    assert(got_items == [{'p': p}])

# It is not allowed to combine the new-style FilterExpression with the
# old-style AttributesToGet. You must use ProjectionExpression instead
# (tested in test_filter_expression_and_projection_expression() above).
def test_filter_expression_and_attributes_to_get(test_table):
    p = random_string()
    # DynamoDB complains: "Can not use both expression and non-expression
    # parameters in the same request: Non-expression parameters:
    # {AttributesToGet} Expression parameters: {FilterExpression,
    # KeyConditionExpression}.
    with pytest.raises(ClientError, match='ValidationException.* both'):
        full_query(test_table,
            KeyConditionExpression='p=:p',
            FilterExpression='x=:x',
            AttributesToGet=['y'],
            ExpressionAttributeValues={':p': p, ':x': 'mouse'})

# All the previous tests involved top-level attributes to be filtered.
# But FilterExpression also allows reading nested attributes, and we should
# support that too. This test just checks one operators - we don't
# test all the different operators again here, we will assume the same
# code is used internally so if one operator worked, all will work.
def test_filter_expression_nested_attribute(test_table):
    p = random_string()
    test_table.put_item(Item={'p': p, 'c': 'hi', 'x': {'a': 'dog', 'b': 3}})
    test_table.put_item(Item={'p': p, 'c': 'yo', 'x': {'a': 'mouse', 'b': 4}})
    got_items = full_query(test_table,
        KeyConditionExpression='p=:p',
        FilterExpression='x.a=:a',
        ExpressionAttributeValues={':p': p, ':a': 'mouse'})
    assert(got_items == [{'p': p, 'c': 'yo', 'x': {'a': 'mouse', 'b': 4}}])

# This test is a version of test_filter_expression_and_projection_expression
# involving nested attributes. In that test, we had a filter and projection
# involving different attributes. Nested attributes open new corner cases:
# The different filter and projection attributes may now be two different
# sub-attributes of the same top-level attribute, or one be a sub-attribute
# of the other. We need to verify that these corner cases work correctly.
# This test reproduces issue #6951 (for the nested attribute case).
def test_filter_expression_and_projection_expression_nested(test_table):
    p = random_string()
    test_table.put_item(Item={'p': p, 'c': 'hi', 'x': {'a': 'dog', 'b': 3}, 'y': 'cat'})
    test_table.put_item(Item={'p': p, 'c': 'yo', 'x': {'a': 'mouse', 'b': 4}, 'y': 'horse'})
    # Case 1: filter on x.a, but project only x.b:
    got_items = full_query(test_table,
        KeyConditionExpression='p=:p',
        FilterExpression='x.a=:a',
        ProjectionExpression='x.b',
        ExpressionAttributeValues={':p': p, ':a': 'mouse'})
    assert(got_items == [{'x': {'b': 4}}])
    # Case 2: filter on x.a, project entire x:
    got_items = full_query(test_table,
        KeyConditionExpression='p=:p',
        FilterExpression='x.a=:a',
        ProjectionExpression='x',
        ExpressionAttributeValues={':p': p, ':a': 'mouse'})
    assert(got_items == [{'x': {'a': 'mouse', 'b': 4}}])
    # Case 3: filter on entire x, project only x.a:
    got_items = full_query(test_table,
        KeyConditionExpression='p=:p',
        FilterExpression='x=:x',
        ProjectionExpression='x.a',
        ExpressionAttributeValues={':p': p, ':x': {'a': 'mouse', 'b': 4}})
    assert(got_items == [{'x': {'a': 'mouse'}}])

# All the tests above were for the Query operation. Let's verify that
# FilterExpression also works for Scan. We will assume the same code is
# used internally, so don't need to check all the details of the
# implementation again, but we need to verify that the basic feature works,
# and some differences between Scan and Query.

def test_filter_expression_scan(filled_test_table):
    table, items = filled_test_table
    got_items = full_scan(table, FilterExpression='#n=:a',
        ExpressionAttributeNames={'#n': 'attribute'},
        ExpressionAttributeValues={':a': 'xxxx'})
    expected_items = [item for item in items if 'attribute' in item and item['attribute'] == 'xxxx']
    assert multiset(expected_items) == multiset(got_items)

# Although filtering using the partition key or sort key is NOT allowed by
# Query, it *is* allowed for Scan.
# For partition keys allowing this is understandable - they aren't sorted, so
# there is no efficient way to filter them other than by FilterExpression.
#
# For sort keys, allowing filtering them through FilterExpression is often
# a bad idea for very long partitions - KeyConditionExpression should be used
# instead to define a slice of each partition and fetch it efficiently
# (relatively). But DynamoDB does *not* support KeyConditionExpression in
# Scan... So FilterExpression is the only option to do such filtering, and
# it's in fact allowed.
def test_filter_expression_scan_partition_key(filled_test_table):
    table, items = filled_test_table
    got_items = full_scan(table, FilterExpression='p=:a',
        ExpressionAttributeValues={':a': '3'})
    expected_items = [item for item in items if item['p'] == '3']
    assert multiset(expected_items) == multiset(got_items)

def test_filter_expression_scan_sort_key(filled_test_table):
    table, items = filled_test_table
    got_items = full_scan(table, FilterExpression='c=:a',
        ExpressionAttributeValues={':a': 3})
    expected_items = [item for item in items if item['c'] == 3]
    print(expected_items)
    assert multiset(expected_items) == multiset(got_items)
