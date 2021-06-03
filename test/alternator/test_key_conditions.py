# -*- coding: utf-8 -*-
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

# Tests for the KeyConditions parameter of the Query operation.
# KeyConditions is the older version of the newer "KeyConditionExpression"
# syntax. That newer syntax is tested in test_key_conditions.py.

import pytest
from botocore.exceptions import ClientError
from util import random_string, random_bytes, full_query, multiset

# The test_table_{sn,ss,sb}_with_sorted_partition fixtures are the regular
# test_table_{sn,ss,sb} fixture with a partition inserted with many items.
# The table, the partition key, and the items are returned - the items are
# sorted (by column 'c'), so they can be easily used to test various range
# queries. This fixture is useful for writing many small query tests which
# read the same input data without needing to re-insert data for every test,
# so overall the test suite is faster.
@pytest.fixture(scope="module")
def test_table_sn_with_sorted_partition(test_table_sn):
    p = random_string()
    items = [{'p': p, 'c': i, 'a': random_string()} for i in range(12)]
    with test_table_sn.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
        # Add another partition just to make sure that a query of just
        # partition p can't just match the entire table and still succeed
        batch.put_item({'p': random_string(), 'c': 123, 'a': random_string()})
    yield test_table_sn, p, items

@pytest.fixture(scope="module")
def test_table_ss_with_sorted_partition(test_table):
    p = random_string()
    items = [{'p': p, 'c': str(i).zfill(3), 'a': random_string()} for i in range(12)]
    with test_table.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
        batch.put_item({'p': random_string(), 'c': '123', 'a': random_string()})
    yield test_table, p, items

@pytest.fixture(scope="module")
def test_table_sb_with_sorted_partition(test_table_sb):
    p = random_string()
    items = [{'p': p, 'c': bytearray(str(i).zfill(3), 'ascii'), 'a': random_string()} for i in range(12)]
    with test_table_sb.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
        batch.put_item({'p': random_string(), 'c': bytearray('123', 'ascii'), 'a': random_string()})
    yield test_table_sb, p, items

# A key condition with just a partition key, returning the entire partition
def test_key_conditions_partition(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    got_items = full_query(table, KeyConditions={
        'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}})
    # Both items and got_items are already sorted, so simple equality check is enough
    assert(got_items == items)

# The "ComparisonOperator" is case sensitive, "EQ" works but "eq" does not:
def test_key_conditions_comparison_operator_case(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    with pytest.raises(ClientError, match='ValidationException.*eq'):
        full_query(table, KeyConditions={
            'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'eq'}})

# Only the "EQ" operator is allowed for the partition keys. The operators "LE",
# "LT", "GE", "GT", "BEGINS_WITH" and "BETWEEN" are allowed for the sort key
# (we'll test this below), but NOT for the partition key. Other random strings
# also are not allowed, but with a different error message.
def test_key_conditions_partition_only_eq(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    for op in ['LE', 'LT', 'GE', 'GT', 'BEGINS_WITH']:
        with pytest.raises(ClientError, match='ValidationException.*not supported'):
            full_query(table, KeyConditions={
                'p' : {'AttributeValueList': [p], 'ComparisonOperator': op}})
    # 'BETWEEN' also fails the test above, but with a different error (it needs
    # two arguments). So let's test it fails also with two arguments:
    with pytest.raises(ClientError, match='ValidationException.*not supported'):
        full_query(table, KeyConditions={
            'p' : {'AttributeValueList': [p, p], 'ComparisonOperator': 'BETWEEN'}})
    # An unknown operator, e.g., "DOG", is also not allowed, but with a
    # different error message.
    with pytest.raises(ClientError, match='ValidationException.*DOG'):
        full_query(table, KeyConditions={
            'p' : {'AttributeValueList': [p, p], 'ComparisonOperator': 'DOG'}})
    # The operators 'NULL', 'NOT_NULL', 'IN', 'CONTAINS', 'NOT_CONTAINS', 'NE'
    # exist for ComparisonOperator and are allowed in filters, but not allowed
    # in key conditions:
    for op in ['IN', 'CONTAINS', 'NOT_CONTAINS', 'NE']:
        with pytest.raises(ClientError, match='ValidationException'):
            full_query(table, KeyConditions={
                'p' : {'AttributeValueList': [p], 'ComparisonOperator': op}})
    for op in ['NULL', 'NOT_NULL']:
        with pytest.raises(ClientError, match='ValidationException'):
            full_query(table, KeyConditions={
                'p' : {'AttributeValueList': [], 'ComparisonOperator': op}})

# The "EQ" operator requires exactly one argument, not more, not less.
def test_key_conditions_partition_one(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    with pytest.raises(ClientError, match='ValidationException.*EQ'):
        full_query(table, KeyConditions={
            'p' : {'AttributeValueList': [], 'ComparisonOperator': 'EQ'}})
    with pytest.raises(ClientError, match='ValidationException.*EQ'):
        full_query(table, KeyConditions={
            'p' : {'AttributeValueList': [p, p], 'ComparisonOperator': 'EQ'}})

# A KeyCondition must define the partition key p, it can't be just on the
# sort key c.
def test_key_conditions_partition_missing(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    with pytest.raises(ClientError, match='ValidationException.* miss'):
        full_query(table, KeyConditions={
            'c' : {'AttributeValueList': [3], 'ComparisonOperator': 'EQ'}})

# The following tests test the various condition operators on the sort key,
# for a *numeric* sort key:

# Test the EQ operator on a numeric sort key:
def test_key_conditions_num_eq(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    got_items = full_query(table, KeyConditions={
        'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
        'c' : {'AttributeValueList': [3], 'ComparisonOperator': 'EQ'}})
    expected_items = [item for item in items if item['c'] == 3]
    assert(got_items == expected_items)

# Test the LT operator on a numeric sort key:
def test_key_conditions_num_lt(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    got_items = full_query(table, KeyConditions={
        'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
        'c' : {'AttributeValueList': [5], 'ComparisonOperator': 'LT'}})
    expected_items = [item for item in items if item['c'] < 5]
    assert(got_items == expected_items)

# Test the LE operator on a numeric sort key:
def test_key_conditions_num_le(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    got_items = full_query(table, KeyConditions={
        'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
        'c' : {'AttributeValueList': [5], 'ComparisonOperator': 'LE'}})
    expected_items = [item for item in items if item['c'] <= 5]
    assert(got_items == expected_items)

# Test the GT operator on a numeric sort key:
def test_key_conditions_num_gt(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    got_items = full_query(table, KeyConditions={
        'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
        'c' : {'AttributeValueList': [5], 'ComparisonOperator': 'GT'}})
    expected_items = [item for item in items if item['c'] > 5]
    assert(got_items == expected_items)

# Test the GE operator on a numeric sort key:
def test_key_conditions_num_ge(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    got_items = full_query(table, KeyConditions={
        'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
        'c' : {'AttributeValueList': [5], 'ComparisonOperator': 'GE'}})
    expected_items = [item for item in items if item['c'] >= 5]
    assert(got_items == expected_items)

# Test the BETWEEN operator on a numeric sort key:
def test_key_conditions_num_between(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    got_items = full_query(table, KeyConditions={
        'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
        'c' : {'AttributeValueList': [4, 7], 'ComparisonOperator': 'BETWEEN'}})
    expected_items = [item for item in items if item['c'] >= 4 and item['c'] <= 7]
    assert(got_items == expected_items)

# The BEGINS_WITH operator does *not* work on a numeric sort key (it only
# works on strings or bytes - we'll check this later):
def test_key_conditions_num_begins(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    with pytest.raises(ClientError, match='ValidationException.*BEGINS_WITH'):
        full_query(table, KeyConditions={
            'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
            'c' : {'AttributeValueList': [3], 'ComparisonOperator': 'BEGINS_WITH'}})

# The following tests test the various condition operators on the sort key,
# for a *string* sort key:

# Test the EQ operator on a string sort key:
def test_key_conditions_str_eq(test_table_ss_with_sorted_partition):
    table, p, items = test_table_ss_with_sorted_partition
    got_items = full_query(table, KeyConditions={
        'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
        'c' : {'AttributeValueList': ['003'], 'ComparisonOperator': 'EQ'}})
    expected_items = [item for item in items if item['c'] == '003']
    assert(got_items == expected_items)

# Test the LT operator on a string sort key:
def test_key_conditions_str_lt(test_table_ss_with_sorted_partition):
    table, p, items = test_table_ss_with_sorted_partition
    got_items = full_query(table, KeyConditions={
        'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
        'c' : {'AttributeValueList': ['005'], 'ComparisonOperator': 'LT'}})
    expected_items = [item for item in items if item['c'] < '005']
    assert(got_items == expected_items)

# Test the LE operator on a string sort key:
def test_key_conditions_str_le(test_table_ss_with_sorted_partition):
    table, p, items = test_table_ss_with_sorted_partition
    got_items = full_query(table, KeyConditions={
        'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
        'c' : {'AttributeValueList': ['005'], 'ComparisonOperator': 'LE'}})
    expected_items = [item for item in items if item['c'] <= '005']
    assert(got_items == expected_items)

# Test the GT operator on a string sort key:
def test_key_conditions_str_gt(test_table_ss_with_sorted_partition):
    table, p, items = test_table_ss_with_sorted_partition
    got_items = full_query(table, KeyConditions={
        'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
        'c' : {'AttributeValueList': ['005'], 'ComparisonOperator': 'GT'}})
    expected_items = [item for item in items if item['c'] > '005']
    assert(got_items == expected_items)

# Test the GE operator on a string sort key:
def test_key_conditions_str_ge(test_table_ss_with_sorted_partition):
    table, p, items = test_table_ss_with_sorted_partition
    got_items = full_query(table, KeyConditions={
        'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
        'c' : {'AttributeValueList': ['005'], 'ComparisonOperator': 'GE'}})
    expected_items = [item for item in items if item['c'] >= '005']
    assert(got_items == expected_items)

# Test the BETWEEN operator on a string sort key:
def test_key_conditions_str_between(test_table_ss_with_sorted_partition):
    table, p, items = test_table_ss_with_sorted_partition
    got_items = full_query(table, KeyConditions={
        'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
        'c' : {'AttributeValueList': ['004', '007'], 'ComparisonOperator': 'BETWEEN'}})
    expected_items = [item for item in items if item['c'] >= '004' and item['c'] <= '007']
    assert(got_items == expected_items)

# Test the BEGINS_WITH operator on a string sort key:
def test_key_conditions_str_begins_with(test_table_ss_with_sorted_partition):
    table, p, items = test_table_ss_with_sorted_partition
    got_items = full_query(table, KeyConditions={
        'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
        'c' : {'AttributeValueList': ['00'], 'ComparisonOperator': 'BEGINS_WITH'}})
    expected_items = [item for item in items if item['c'].startswith('00')]
    assert(got_items == expected_items)

# This tests BEGINS_WITH with some more elaborate characters
def test_begins_with_more_str(dynamodb, test_table_ss):
    p = random_string()
    items = [{'p': p, 'c': sort_key, 'str': 'a'} for sort_key in [u'ÿÿÿ', u'cÿbÿ', u'cÿbÿÿabg'] ]
    with test_table_ss.batch_writer() as batch:
        for item in items:
            batch.put_item(item)

    got_items = full_query(test_table_ss, KeyConditions={
            'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
            'c' : {'AttributeValueList': [u'ÿÿ'], 'ComparisonOperator': 'BEGINS_WITH'}})
    assert sorted([d['c'] for d in got_items]) == sorted([d['c'] for d in items if d['c'].startswith(u'ÿÿ')])

    got_items = full_query(test_table_ss, KeyConditions={
            'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
            'c' : {'AttributeValueList': [u'cÿbÿ'], 'ComparisonOperator': 'BEGINS_WITH'}})
    assert sorted([d['c'] for d in got_items]) == sorted([d['c'] for d in items if d['c'].startswith(u'cÿbÿ')])


# The following tests test the various condition operators on the sort key,
# for a *bytes* sort key:

# Test the EQ operator on a bytes sort key:
def test_key_conditions_bytes_eq(test_table_sb_with_sorted_partition):
    table, p, items = test_table_sb_with_sorted_partition
    got_items = full_query(table, KeyConditions={
        'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
        'c' : {'AttributeValueList': [bytearray('003', 'ascii')], 'ComparisonOperator': 'EQ'}})
    expected_items = [item for item in items if item['c'] == bytearray('003', 'ascii')]
    assert(got_items == expected_items)

# Test the LT operator on a bytes sort key:
def test_key_conditions_bytes_lt(test_table_sb_with_sorted_partition):
    table, p, items = test_table_sb_with_sorted_partition
    got_items = full_query(table, KeyConditions={
        'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
        'c' : {'AttributeValueList': [bytearray('005', 'ascii')], 'ComparisonOperator': 'LT'}})
    expected_items = [item for item in items if item['c'] < bytearray('005', 'ascii')]
    assert(got_items == expected_items)

# Test the LE operator on a bytes sort key:
def test_key_conditions_bytes_le(test_table_sb_with_sorted_partition):
    table, p, items = test_table_sb_with_sorted_partition
    got_items = full_query(table, KeyConditions={
        'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
        'c' : {'AttributeValueList': [bytearray('005', 'ascii')], 'ComparisonOperator': 'LE'}})
    expected_items = [item for item in items if item['c'] <= bytearray('005', 'ascii')]
    assert(got_items == expected_items)

# Test the GT operator on a bytes sort key:
def test_key_conditions_bytes_gt(test_table_sb_with_sorted_partition):
    table, p, items = test_table_sb_with_sorted_partition
    got_items = full_query(table, KeyConditions={
        'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
        'c' : {'AttributeValueList': [bytearray('005', 'ascii')], 'ComparisonOperator': 'GT'}})
    expected_items = [item for item in items if item['c'] > bytearray('005', 'ascii')]
    assert(got_items == expected_items)

# Test the GE operator on a bytes sort key:
def test_key_conditions_bytes_ge(test_table_sb_with_sorted_partition):
    table, p, items = test_table_sb_with_sorted_partition
    got_items = full_query(table, KeyConditions={
        'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
        'c' : {'AttributeValueList': [bytearray('005', 'ascii')], 'ComparisonOperator': 'GE'}})
    expected_items = [item for item in items if item['c'] >= bytearray('005', 'ascii')]
    assert(got_items == expected_items)

# Test the BETWEEN operator on a bytes sort key:
def test_key_conditions_bytes_between(test_table_sb_with_sorted_partition):
    table, p, items = test_table_sb_with_sorted_partition
    got_items = full_query(table, KeyConditions={
        'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
        'c' : {'AttributeValueList': [bytearray('004', 'ascii'), bytearray('007', 'ascii')], 'ComparisonOperator': 'BETWEEN'}})
    expected_items = [item for item in items if item['c'] >= bytearray('004', 'ascii') and item['c'] <= bytearray('007', 'ascii')]
    assert(got_items == expected_items)

# Test the BEGINS_WITH operator on a bytes sort key:
def test_key_conditions_bytes_begins_with(test_table_sb_with_sorted_partition):
    table, p, items = test_table_sb_with_sorted_partition
    got_items = full_query(table, KeyConditions={
        'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
        'c' : {'AttributeValueList': [bytearray('00', 'ascii')], 'ComparisonOperator': 'BEGINS_WITH'}})
    expected_items = [item for item in items if item['c'].startswith(bytearray('00', 'ascii'))]
    assert(got_items == expected_items)

# Test the special case of the 0xFF character for which we have special handling
def test_key_conditions_bytes_begins_with_ff(dynamodb, test_table_sb):
    p = random_string()
    items = [{'p': p, 'c': sort_key, 'str': 'a'} for sort_key in [bytearray([1, 2,3]), bytearray([255, 3, 4]), bytearray([255, 255, 255])] ]
    with test_table_sb.batch_writer() as batch:
        for item in items:
            batch.put_item(item)

    got_items = full_query(test_table_sb, KeyConditions={
            'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
            'c' : {'AttributeValueList': [bytearray([255])], 'ComparisonOperator': 'BEGINS_WITH'}})
    expected_items = [item for item in items if item['c'].startswith(bytearray([255]))]
    assert(got_items == expected_items)

# Other ComparisonOperator values besides what we tested above may be supported by
# filters, but *not* by KeyConditions, not even on the sort keys:
def test_key_conditions_sort_unsupported(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    # The operators 'NULL', 'NOT_NULL', 'IN', 'CONTAINS', 'NOT_CONTAINS', 'NE'
    # exist for ComparisonOperator and are allowed in filters, but not allowed
    # in key conditions:
    for op in ['IN', 'CONTAINS', 'NOT_CONTAINS', 'NE']:
        with pytest.raises(ClientError, match='ValidationException'):
            full_query(table, KeyConditions={
                'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
                'c' : {'AttributeValueList': [3], 'ComparisonOperator': op}})
    for op in ['NULL', 'NOT_NULL']:
        with pytest.raises(ClientError, match='ValidationException'):
            full_query(table, KeyConditions={
                'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
                'c' : {'AttributeValueList': [], 'ComparisonOperator': op}})
    # An unknown operator, e.g., "DOG", is also not allowed.
    with pytest.raises(ClientError, match='ValidationException.*DOG'):
        full_query(table, KeyConditions={
            'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
            'c' : {'AttributeValueList': [p], 'ComparisonOperator': 'DOG'}})

# We cannot have a key condition on a non-key column (not instead of a
# condition on the sort key, and not in addition to it).
def test_key_conditions_non_key(test_table_sn_with_sorted_partition):
    table, p, items = test_table_sn_with_sorted_partition
    with pytest.raises(ClientError, match='ValidationException'):
        full_query(table, KeyConditions={
            'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
            'a' : {'AttributeValueList': [5], 'ComparisonOperator': 'EQ'}})
    with pytest.raises(ClientError, match='ValidationException'):
        full_query(table, KeyConditions={
            'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
            'c' : {'AttributeValueList': [3], 'ComparisonOperator': 'EQ'},
            'a' : {'AttributeValueList': [5], 'ComparisonOperator': 'EQ'}})

# Test for trying to use a KeyCondition with an operand type which doesn't
# match the key type:
def test_key_conditions_wrong_type(test_table_ss_with_sorted_partition):
    table, p, items = test_table_ss_with_sorted_partition
    with pytest.raises(ClientError, match='ValidationException.* type'):
        full_query(table, KeyConditions={
            'p' : {'AttributeValueList': [3], 'ComparisonOperator': 'EQ'}})
    with pytest.raises(ClientError, match='ValidationException.* type'):
        full_query(table, KeyConditions={
            'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
            'c' : {'AttributeValueList': [3], 'ComparisonOperator': 'EQ'}})

# Query and KeyConditions work also on a table with just a partition
# key and no sort key, although obviously it isn't very useful (for such
# tables, Query is just an elaborate way to do a GetItem).
def test_key_conditions_hash_only_s(test_table_s):
    p = random_string()
    item = {'p': p, 'val': 'hello'}
    test_table_s.put_item(Item=item)
    got_items = full_query(test_table_s, KeyConditions={
        'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}})
    assert(got_items == [item])

def test_key_conditions_hash_only_b(test_table_b):
    p = random_bytes()
    item = {'p': p, 'val': 'hello'}
    test_table_b.put_item(Item=item)
    got_items = full_query(test_table_b, KeyConditions={
        'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}})
    assert(got_items == [item])

# TODO: add a test_table_n fixture, and test this case too.

# Demonstrate that issue #6573 was not a bug for KeyConditions: binary
# strings are ordered as unsigned bytes, i.e., byte 128 comes after 127,
# not as signed bytes.
# Test the five ordering operators: LT, LE, GT, GE, BETWEEN.
def test_key_conditions_unsigned_bytes(test_table_sb):
    p = random_string()
    items = [{'p': p, 'c': bytearray([i])} for i in range(126,129)]
    with test_table_sb.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    got_items = full_query(test_table_sb, KeyConditions={
        'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
        'c' : {'AttributeValueList': [bytearray([127])], 'ComparisonOperator': 'LT'}})
    expected_items = [item for item in items if item['c'] < bytearray([127])]
    assert(got_items == expected_items)
    got_items = full_query(test_table_sb, KeyConditions={
        'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
        'c' : {'AttributeValueList': [bytearray([127])], 'ComparisonOperator': 'LE'}})
    expected_items = [item for item in items if item['c'] <= bytearray([127])]
    assert(got_items == expected_items)
    got_items = full_query(test_table_sb, KeyConditions={
        'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
        'c' : {'AttributeValueList': [bytearray([127])], 'ComparisonOperator': 'GT'}})
    expected_items = [item for item in items if item['c'] > bytearray([127])]
    assert(got_items == expected_items)
    got_items = full_query(test_table_sb, KeyConditions={
        'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
        'c' : {'AttributeValueList': [bytearray([127])], 'ComparisonOperator': 'GE'}})
    expected_items = [item for item in items if item['c'] >= bytearray([127])]
    assert(got_items == expected_items)
    got_items = full_query(test_table_sb, KeyConditions={
        'p' : {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
        'c' : {'AttributeValueList': [bytearray([127]), bytearray([128])], 'ComparisonOperator': 'BETWEEN'}})
    expected_items = [item for item in items if item['c'] >= bytearray([127]) and item['c'] <= bytearray([128])]
    print(expected_items)
    assert(got_items == expected_items)


# The following is an older test we had, and is probably no longer needed.
# It tests one limited use case for KeyConditions. It uses filled_test_table
# (the one we also use in test_scan.py) instead of the fixtures defined in this
# file. It also uses Boto3's paginator instead of our full_query utility.
def test_query_basic_restrictions(dynamodb, filled_test_table):
    test_table, items = filled_test_table
    paginator = dynamodb.meta.client.get_paginator('query')

    # EQ
    got_items = []
    for page in paginator.paginate(TableName=test_table.name, KeyConditions={
            'p' : {'AttributeValueList': ['long'], 'ComparisonOperator': 'EQ'}
        }):
        got_items += page['Items']
    print(got_items)
    assert multiset([item for item in items if item['p'] == 'long']) == multiset(got_items)

    # LT
    got_items = []
    for page in paginator.paginate(TableName=test_table.name, KeyConditions={
            'p' : {'AttributeValueList': ['long'], 'ComparisonOperator': 'EQ'},
            'c' : {'AttributeValueList': ['12'], 'ComparisonOperator': 'LT'}
        }):
        got_items += page['Items']
    print(got_items)
    assert multiset([item for item in items if item['p'] == 'long' and item['c'] < '12']) == multiset(got_items)

    # LE
    got_items = []
    for page in paginator.paginate(TableName=test_table.name, KeyConditions={
            'p' : {'AttributeValueList': ['long'], 'ComparisonOperator': 'EQ'},
            'c' : {'AttributeValueList': ['14'], 'ComparisonOperator': 'LE'}
        }):
        got_items += page['Items']
    print(got_items)
    assert multiset([item for item in items if item['p'] == 'long' and item['c'] <= '14']) == multiset(got_items)

    # GT
    got_items = []
    for page in paginator.paginate(TableName=test_table.name, KeyConditions={
            'p' : {'AttributeValueList': ['long'], 'ComparisonOperator': 'EQ'},
            'c' : {'AttributeValueList': ['15'], 'ComparisonOperator': 'GT'}
        }):
        got_items += page['Items']
    print(got_items)
    assert multiset([item for item in items if item['p'] == 'long' and item['c'] > '15']) == multiset(got_items)

    # GE
    got_items = []
    for page in paginator.paginate(TableName=test_table.name, KeyConditions={
            'p' : {'AttributeValueList': ['long'], 'ComparisonOperator': 'EQ'},
            'c' : {'AttributeValueList': ['14'], 'ComparisonOperator': 'GE'}
        }):
        got_items += page['Items']
    print(got_items)
    assert multiset([item for item in items if item['p'] == 'long' and item['c'] >= '14']) == multiset(got_items)

    # BETWEEN
    got_items = []
    for page in paginator.paginate(TableName=test_table.name, KeyConditions={
            'p' : {'AttributeValueList': ['long'], 'ComparisonOperator': 'EQ'},
            'c' : {'AttributeValueList': ['155', '164'], 'ComparisonOperator': 'BETWEEN'}
        }):
        got_items += page['Items']
    print(got_items)
    assert multiset([item for item in items if item['p'] == 'long' and item['c'] >= '155' and item['c'] <= '164']) == multiset(got_items)

    # BEGINS_WITH
    got_items = []
    for page in paginator.paginate(TableName=test_table.name, KeyConditions={
            'p' : {'AttributeValueList': ['long'], 'ComparisonOperator': 'EQ'},
            'c' : {'AttributeValueList': ['11'], 'ComparisonOperator': 'BEGINS_WITH'}
        }):
        print([item for item in items if item['p'] == 'long' and item['c'].startswith('11')])
        got_items += page['Items']
    print(got_items)
    assert multiset([item for item in items if item['p'] == 'long' and item['c'].startswith('11')]) == multiset(got_items)

