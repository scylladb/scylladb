# Copyright 2019 ScyllaDB
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

# Tests for the CRUD item operations: PutItem, GetItem, UpdateItem, DeleteItem

import pytest
from botocore.exceptions import ClientError
from decimal import Decimal
from util import random_string, random_bytes

# Basic test for creating a new item with a random name, and reading it back
# with strong consistency.
# Only the string type is used for keys and attributes. None of the various
# optional PutItem features (Expected, ReturnValues, ReturnConsumedCapacity,
# ReturnItemCollectionMetrics, ConditionalOperator, ConditionExpression,
# ExpressionAttributeNames, ExpressionAttributeValues) are used, and
# for GetItem strong consistency is requested as well as all attributes,
# but no other optional features (AttributesToGet, ReturnConsumedCapacity,
# ProjectionExpression, ExpressionAttributeNames)
def test_basic_string_put_and_get(test_table):
    p = random_string()
    c = random_string()
    val = random_string()
    val2 = random_string()
    test_table.put_item(Item={'p': p, 'c': c, 'attribute': val, 'another': val2})
    item = test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)['Item']
    assert item['p'] == p
    assert item['c'] == c
    assert item['attribute'] == val
    assert item['another'] == val2

# Similar to test_basic_string_put_and_get, just uses UpdateItem instead of
# PutItem. Because the item does not yet exist, it should work the same.
def test_basic_string_update_and_get(test_table):
    p = random_string()
    c = random_string()
    val = random_string()
    val2 = random_string()
    test_table.update_item(Key={'p': p, 'c': c}, AttributeUpdates={'attribute': {'Value': val, 'Action': 'PUT'}, 'another': {'Value': val2, 'Action': 'PUT'}})
    item = test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)['Item']
    assert item['p'] == p
    assert item['c'] == c
    assert item['attribute'] == val
    assert item['another'] == val2

# Test put_item and get_item of various types for the *attributes*,
# including both scalars as well as nested documents, lists and sets.
# The full list of types tested here:
#    number, boolean, bytes, null, list, map, string set, number set,
#    binary set.
# The keys are still strings.
# Note that only top-level attributes are written and read in this test -
# this test does not attempt to modify *nested* attributes.
# See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/dynamodb.html
# on how to pass these various types to Boto3's put_item().
def test_put_and_get_attribute_types(test_table):
    key = {'p': random_string(), 'c': random_string()}
    test_items = [
        Decimal("12.345"),
        42,
        True,
        False,
        b'xyz',
        None,
        ['hello', 'world', 42],
        {'hello': 'world', 'life': 42},
        {'hello': {'test': 'hi', 'hello': True, 'list': [1, 2, 'hi']}},
        set(['hello', 'world', 'hi']),
        set([1, 42, Decimal("3.14")]),
        set([b'xyz', b'hi']),
    ]
    item = { str(i) : test_items[i] for i in range(len(test_items)) }
    item.update(key)
    test_table.put_item(Item=item)
    got_item = test_table.get_item(Key=key, ConsistentRead=True)['Item']
    assert item == got_item

# The test_empty_* tests below verify support for empty items, with no
# attributes except the key. This is a difficult case for Scylla, because
# for an empty row to exist, Scylla needs to add a "CQL row marker".
# There are several ways to create empty items - via PutItem, UpdateItem
# and deleting attributes from non-empty items, and we need to check them
# all, in several test_empty_* tests:
def test_empty_put(test_table):
    p = random_string()
    c = random_string()
    test_table.put_item(Item={'p': p, 'c': c})
    item = test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)['Item']
    assert item == {'p': p, 'c': c}
def test_empty_put_delete(test_table):
    p = random_string()
    c = random_string()
    test_table.put_item(Item={'p': p, 'c': c, 'hello': 'world'})
    test_table.update_item(Key={'p': p, 'c': c}, AttributeUpdates={'hello': {'Action': 'DELETE'}})
    item = test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)['Item']
    assert item == {'p': p, 'c': c}
def test_empty_update(test_table):
    p = random_string()
    c = random_string()
    test_table.update_item(Key={'p': p, 'c': c}, AttributeUpdates={})
    item = test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)['Item']
    assert item == {'p': p, 'c': c}
def test_empty_update_delete(test_table):
    p = random_string()
    c = random_string()
    test_table.update_item(Key={'p': p, 'c': c}, AttributeUpdates={'hello': {'Value': 'world', 'Action': 'PUT'}})
    test_table.update_item(Key={'p': p, 'c': c}, AttributeUpdates={'hello': {'Action': 'DELETE'}})
    item = test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)['Item']
    assert item == {'p': p, 'c': c}

# Test error handling of UpdateItem passed a bad "Action" field.
def test_update_bad_action(test_table):
    p = random_string()
    c = random_string()
    val = random_string()
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.update_item(Key={'p': p, 'c': c}, AttributeUpdates={'attribute': {'Value': val, 'Action': 'NONEXISTENT'}})

# A more elaborate UpdateItem test, updating different attributes at different
# times. Includes PUT and DELETE operations.
def test_basic_string_more_update(test_table):
    p = random_string()
    c = random_string()
    val1 = random_string()
    val2 = random_string()
    val3 = random_string()
    val4 = random_string()
    test_table.update_item(Key={'p': p, 'c': c}, AttributeUpdates={'a3': {'Value': val1, 'Action': 'PUT'}})
    test_table.update_item(Key={'p': p, 'c': c}, AttributeUpdates={'a1': {'Value': val1, 'Action': 'PUT'}})
    test_table.update_item(Key={'p': p, 'c': c}, AttributeUpdates={'a2': {'Value': val2, 'Action': 'PUT'}})
    test_table.update_item(Key={'p': p, 'c': c}, AttributeUpdates={'a1': {'Value': val3, 'Action': 'PUT'}})
    test_table.update_item(Key={'p': p, 'c': c}, AttributeUpdates={'a3': {'Action': 'DELETE'}})
    item = test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)['Item']
    assert item['p'] == p
    assert item['c'] == c
    assert item['a1'] == val3
    assert item['a2'] == val2
    assert not 'a3' in item

# Test that item operations on a non-existent table name fail with correct
# error code.
def test_item_operations_nonexistent_table(dynamodb):
    with pytest.raises(ClientError, match='ResourceNotFoundException'):
        dynamodb.meta.client.put_item(TableName='non_existent_table',
            Item={'a':{'S':'b'}})

# Fetching a non-existent item. According to the DynamoDB doc, "If there is no
# matching item, GetItem does not return any data and there will be no Item
# element in the response."
def test_get_item_missing_item(test_table):
    p = random_string()
    c = random_string()
    assert not "Item" in test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)

# Test that if we have a table with string hash and sort keys, we can't read
# or write items with other key types to it.
def test_put_item_wrong_key_type(test_table):
    b = random_bytes()
    s = random_string()
    n = Decimal("3.14")
    # Should succeed (correct key types)
    test_table.put_item(Item={'p': s, 'c': s})
    assert test_table.get_item(Key={'p': s, 'c': s}, ConsistentRead=True)['Item'] == {'p': s, 'c': s}
    # Should fail (incorrect hash key types)
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.put_item(Item={'p': b, 'c': s})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.put_item(Item={'p': n, 'c': s})
    # Should fail (incorrect sort key types)
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.put_item(Item={'p': s, 'c': b})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.put_item(Item={'p': s, 'c': n})
    # Should fail (missing hash key)
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.put_item(Item={'c': s})
    # Should fail (missing sort key)
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.put_item(Item={'p': s})
def test_update_item_wrong_key_type(test_table, test_table_s):
    b = random_bytes()
    s = random_string()
    n = Decimal("3.14")
    # Should succeed (correct key types)
    test_table.update_item(Key={'p': s, 'c': s}, AttributeUpdates={})
    assert test_table.get_item(Key={'p': s, 'c': s}, ConsistentRead=True)['Item'] == {'p': s, 'c': s}
    # Should fail (incorrect hash key types)
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.update_item(Key={'p': b, 'c': s}, AttributeUpdates={})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.update_item(Key={'p': n, 'c': s}, AttributeUpdates={})
    # Should fail (incorrect sort key types)
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.update_item(Key={'p': s, 'c': b}, AttributeUpdates={})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.update_item(Key={'p': s, 'c': n}, AttributeUpdates={})
    # Should fail (missing hash key)
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.update_item(Key={'c': s}, AttributeUpdates={})
    # Should fail (missing sort key)
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.update_item(Key={'p': s}, AttributeUpdates={})
    # Should fail (spurious key columns)
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.get_item(Key={'p': s, 'c': s, 'spurious': s})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.get_item(Key={'p': s, 'c': s})
def test_get_item_wrong_key_type(test_table, test_table_s):
    b = random_bytes()
    s = random_string()
    n = Decimal("3.14")
    # Should succeed (correct key types) but have empty result
    assert not "Item" in test_table.get_item(Key={'p': s, 'c': s}, ConsistentRead=True)
    # Should fail (incorrect hash key types)
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.get_item(Key={'p': b, 'c': s})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.get_item(Key={'p': n, 'c': s})
    # Should fail (incorrect sort key types)
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.get_item(Key={'p': s, 'c': b})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.get_item(Key={'p': s, 'c': n})
    # Should fail (missing hash key)
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.get_item(Key={'c': s})
    # Should fail (missing sort key)
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.get_item(Key={'p': s})
    # Should fail (spurious key columns)
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.get_item(Key={'p': s, 'c': s, 'spurious': s})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.get_item(Key={'p': s, 'c': s})
def test_delete_item_wrong_key_type(test_table, test_table_s):
    b = random_bytes()
    s = random_string()
    n = Decimal("3.14")
    # Should succeed (correct key types)
    test_table.delete_item(Key={'p': s, 'c': s})
    # Should fail (incorrect hash key types)
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.delete_item(Key={'p': b, 'c': s})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.delete_item(Key={'p': n, 'c': s})
    # Should fail (incorrect sort key types)
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.delete_item(Key={'p': s, 'c': b})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.delete_item(Key={'p': s, 'c': n})
    # Should fail (missing hash key)
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.delete_item(Key={'c': s})
    # Should fail (missing sort key)
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.delete_item(Key={'p': s})
    # Should fail (spurious key columns)
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.delete_item(Key={'p': s, 'c': s, 'spurious': s})
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.delete_item(Key={'p': s, 'c': s})

# Most of the tests here arbitrarily used a table with both hash and sort keys
# (both strings). Let's check that a table with *only* a hash key works ok
# too, for PutItem, GetItem, and UpdateItem.
def test_only_hash_key(test_table_s):
    s = random_string()
    test_table_s.put_item(Item={'p': s, 'hello': 'world'})
    assert test_table_s.get_item(Key={'p': s}, ConsistentRead=True)['Item'] == {'p': s, 'hello': 'world'}
    test_table_s.update_item(Key={'p': s}, AttributeUpdates={'hi': {'Value': 'there', 'Action': 'PUT'}})
    assert test_table_s.get_item(Key={'p': s}, ConsistentRead=True)['Item'] == {'p': s, 'hello': 'world', 'hi': 'there'}

# Tests for item operations in tables with non-string hash or sort keys.
# These tests focus only on the type of the key - everything else is as
# simple as we can (string attributes, no special options for GetItem
# and PutItem). These tests also focus on individual items only, and
# not about the sort order of sort keys - this should be verified in
# test_query.py, for example.
def test_bytes_hash_key(test_table_b):
    # Bytes values are passed using base64 encoding, which has weird cases
    # depending on len%3 and len%4. So let's try various lengths.
    for len in range(10,18):
        p = random_bytes(len)
        val = random_string()
        test_table_b.put_item(Item={'p': p, 'attribute': val})
        assert test_table_b.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'attribute': val}
def test_bytes_sort_key(test_table_sb):
    p = random_string()
    c = random_bytes()
    val = random_string()
    test_table_sb.put_item(Item={'p': p, 'c': c, 'attribute': val})
    assert test_table_sb.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)['Item'] == {'p': p, 'c': c, 'attribute': val}

# Tests for using a large binary blob as hash key, sort key, or attribute.
# DynamoDB strictly limits the size of the binary hash key to 2048 bytes,
# and binary sort key to 1024 bytes, and refuses anything larger. The total
# size of an item is limited to 400KB, which also limits the size of the
# largest attributes. For more details on these limits, see
# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html
# Alternator currently does *not* have these limitations, and can accept much
# larger keys and attributes, but what we do in the following tests is to verify
# that items up to DynamoDB's maximum sizes also work well in Alternator.
def test_large_blob_hash_key(test_table_b):
    b = random_bytes(2048)
    test_table_b.put_item(Item={'p': b})
    assert test_table_b.get_item(Key={'p': b}, ConsistentRead=True)['Item'] == {'p': b}
def test_large_blob_sort_key(test_table_sb):
    s = random_string()
    b = random_bytes(1024)
    test_table_sb.put_item(Item={'p': s, 'c': b})
    assert test_table_sb.get_item(Key={'p': s, 'c': b}, ConsistentRead=True)['Item'] == {'p': s, 'c': b}
def test_large_blob_attribute(test_table):
    p = random_string()
    c = random_string()
    b = random_bytes(409500)  # a bit less than 400KB
    test_table.put_item(Item={'p': p, 'c': c, 'attribute': b })
    assert test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)['Item'] == {'p': p, 'c': c, 'attribute': b}

# Checks what it is not allowed to use in a single UpdateItem request both
# old-style AttributeUpdates and new-style UpdateExpression.
def test_update_item_two_update_methods(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Value': 3, 'Action': 'PUT'}},
            UpdateExpression='SET b = :val1',
            ExpressionAttributeValues={':val1': 4})

# Verify that having neither AttributeUpdates nor UpdateExpression is
# allowed, and results in creation of an empty item.
def test_update_item_no_update_method(test_table_s):
    p = random_string()
    assert not "Item" in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)
    test_table_s.update_item(Key={'p': p})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p}

# Test GetItem with the AttributesToGet parameter. Result should include the
# selected attributes only - if one wants the key attributes as well, one
# needs to select them explicitly. When no key attributes are selected,
# some items may have *none* of the selected attributes. Those items are
# returned too, as empty items - they are not outright missing.
def test_getitem_attributes_to_get(dynamodb, test_table):
    p = random_string()
    c = random_string()
    item = {'p': p, 'c': c, 'a': 'hello', 'b': 'hi'}
    test_table.put_item(Item=item)
    for wanted in [ ['a'],             # only non-key attribute
                    ['c', 'a'],        # a key attribute (sort key) and non-key
                    ['p', 'c'],        # entire key
                    ['nonexistent']    # Our item doesn't have this
                   ]:
        got_item = test_table.get_item(Key={'p': p, 'c': c}, AttributesToGet=wanted, ConsistentRead=True)['Item']
        expected_item = {k: item[k] for k in wanted if k in item}
        assert expected_item == got_item

# Basic test for DeleteItem, with hash key only
def test_delete_item_hash(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p})
    assert 'Item' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)
    test_table_s.delete_item(Key={'p': p})
    assert not 'Item' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)

# Basic test for DeleteItem, with hash and sort key
def test_delete_item_sort(test_table):
    p = random_string()
    c = random_string()
    key = {'p': p, 'c': c}
    test_table.put_item(Item=key)
    assert 'Item' in test_table.get_item(Key=key, ConsistentRead=True)
    test_table.delete_item(Key=key)
    assert not 'Item' in test_table.get_item(Key=key, ConsistentRead=True)

# Test that PutItem completely replaces an existing item. It shouldn't merge
# it with a previously existing value, as UpdateItem does!
# We test for a table with just hash key, and for a table with both hash and
# sort keys.
def test_put_item_replace(test_table_s, test_table):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'a': 'hi'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 'hi'}
    test_table_s.put_item(Item={'p': p, 'b': 'hello'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'b': 'hello'}
    c = random_string()
    test_table.put_item(Item={'p': p, 'c': c, 'a': 'hi'})
    assert test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)['Item'] == {'p': p, 'c': c, 'a': 'hi'}
    test_table.put_item(Item={'p': p, 'c': c, 'b': 'hello'})
    assert test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)['Item'] == {'p': p, 'c': c, 'b': 'hello'}

# Test what UpdateItem does on a non-existent item. An operation that puts an
# attribute, creates this item. Even an empty operation creates an item
# (this is test_empty_update() above). But an operation that only deletes
# attributes, does not create an empty item. This reproduces issue #5862.
def test_update_item_non_existent(test_table_s):
    # An update that puts an attribute on a non-existent item, creates it:
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Value': 3, 'Action': 'PUT'}})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 3}
    # An update that does *nothing* on a non-existent item, still creates it:
    p = random_string()
    test_table_s.update_item(Key={'p': p}, AttributeUpdates={})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p}
    # HOWEVER, an update that only deletes an attribute on a non-existent
    # item, does NOT creates it: (issue #5862 was about Alternator wrongly
    # creating and empty item in this case).
    p = random_string()
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Action': 'DELETE'}})
    assert not 'Item' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)
    # Test the same thing - that an attribute-deleting update does not
    # create a non-existing item - but now with the update expression syntax:
    p = random_string()
    test_table_s.update_item(Key={'p': p}, UpdateExpression='REMOVE a')
    assert not 'Item' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)

# UpdateItem's AttributeUpdate's DELETE operations has two different
# meanings. It can be used to delete an entire attribute, but can also be
# used to delete elements from a set attribute. The latter is a RMW operation,
# because it requires testing the existing value of the attribute, if it
# is indeed a set of the desired type.
@pytest.mark.xfail(reason="UpdateItem AttributeUpdates DELETE not implemented for sets")
def test_update_item_delete(test_table_s):
    p = random_string()
    a = random_string()
    test_table_s.put_item(Item={'p': p, 'a': a})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': a}
    # An Value-less DELETE just deletes the entire attribute
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Action': 'DELETE'}})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p}
    # The "Value" parameter is rejected for *most* types, except sets.
    # Even lists (supported by the ADD operation) are rejected.
    for value in ['b', 3, bytearray('b', 'utf-8'), True, False, None,
                  [2,3], {'a': 3}]:
        test_table_s.put_item(Item={'p': p, 'a': value})
        with pytest.raises(ClientError, match='ValidationException.*type'):
            test_table_s.update_item(Key={'p': p},
                AttributeUpdates={'a': {'Action': 'DELETE', 'Value': value}})
    # "Value" is allowed for sets, but the existing content of the attribute
    # must be a set as well, otherwise we get an error on mismatched type
    with pytest.raises(ClientError, match='ValidationException.*mismatch'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Action': 'DELETE', 'Value': set([1, 2])}})
    # When "Value" is a set and the attribute is a set of the same type,
    # DELETE remove these items from the set attribute. It works on all
    # three set types (string, bytes, number):
    test_table_s.put_item(Item={'p': p, 'a': set([1, 2, 3, 4, 5])})
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Action': 'DELETE', 'Value': set([2, 4])}})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': set([1, 3, 5])}
    test_table_s.put_item(Item={'p': p, 'a': set(['dog', 'cat', 'lion'])})
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Action': 'DELETE', 'Value': set(['dog'])}})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': set(['cat', 'lion'])}
    test_table_s.put_item(Item={'p': p, 'a': set([b'dog', b'cat', b'lion'])})
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Action': 'DELETE', 'Value': set([b'cat'])}})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': set([b'dog', b'lion'])}
    # If the item and value are both sets, but not of the same type, we
    # get an error on the mismatched types:
    test_table_s.put_item(Item={'p': p, 'a': set([1, 2, 3, 4, 5])})
    with pytest.raises(ClientError, match='ValidationException.*mismatch'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Action': 'DELETE', 'Value': set(['hi'])}})
    # An empty set is not allowed as Value:
    test_table_s.put_item(Item={'p': p, 'a': set([1, 2, 3])})
    with pytest.raises(ClientError, match='ValidationException.*empty'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Action': 'DELETE', 'Value': set([])}})
    # Deleting all the elments from a set doesn't leave an empty set
    # (which DynamoDB doesn't allow) but rather deletes the attribute:
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Action': 'DELETE', 'Value': set([1, 2, 3])}})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p}
    # Removing an item not already in the set is fine, and ignores it:
    test_table_s.put_item(Item={'p': p, 'a': set([1, 2, 3])})
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Action': 'DELETE', 'Value': set([4])}})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': set([1, 2, 3])}
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Action': 'DELETE', 'Value': set([2, 5])}})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': set([1, 3])}
    # Asking to delete an attribute or parts of a set attribute is silently
    # ignored if the item doesn't exist (no error, and item isn't created).
    p = random_string()
    assert not 'Item' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Action': 'DELETE'}})
    assert not 'Item' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Action': 'DELETE', 'Value': set([4])}})
    assert not 'Item' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)

# Test for UpdateItem's AttributeUpdate's ADD operation, which has different
# meanings for numbers and sets - but not for other types.
@pytest.mark.xfail(reason="UpdateItem AttributeUpdates ADD not implemented")
def test_update_item_add(test_table_s):
    p = random_string()

    # ADD operations on numbers:
    test_table_s.put_item(Item={'p': p, 'a': 7})
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Action': 'ADD', 'Value': 2}})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 9}
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Action': 'ADD', 'Value': Decimal(-3.5)}})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': Decimal(5.5)}
    # Incrementing a non-existent attribute is allowed (as if the value is 0)
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'b': {'Action': 'ADD', 'Value': 2}})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': Decimal(5.5), 'b': 2}

    # ADD operation on sets:
    test_table_s.put_item(Item={'p': p, 'a': set([1, 2])})
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Action': 'ADD', 'Value': set([3, 4])}})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': set([1, 2, 3, 4])}
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Action': 'ADD', 'Value': set([3, 5])}})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': set([1, 2, 3, 4, 5])}
    # Adding a set to a non-existent attribute is allowed (as if empty set)
    # The DynamoDB documentation suggests this is only allowed for a set
    # of numbers, but it actually works for any set type.
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'b': {'Action': 'ADD', 'Value': set([7])}})
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'c': {'Action': 'ADD', 'Value': set(['a', 'b'])}})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': set([1, 2, 3, 4, 5]), 'b': set([7]), 'c': set(['a', 'b'])}
    # The set type in the attribute and the Value argument needs to match:
    with pytest.raises(ClientError, match='ValidationException.*mismatch'):
        test_table_s.put_item(Item={'p': p, 'a': set([1, 2])})
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Action': 'ADD', 'Value': set(['a'])}})

    # ADD operation on lists (not documented, but works similar to sets!)
    test_table_s.put_item(Item={'p': p, 'a': [1, 2]})
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Action': 'ADD', 'Value': [3, 4]}})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': [1, 2, 3, 4]}
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'a': {'Action': 'ADD', 'Value': [3, 5]}})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': [1, 2, 3, 4, 3, 5]}
    # Adding a list to a non-existent attribute is allowed (as if empty list)
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'b': {'Action': 'ADD', 'Value': [7]}})
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'c': {'Action': 'ADD', 'Value': ['a', 'b']}})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': [1, 2, 3, 4, 3, 5], 'b': [7], 'c': ['a', 'b']}
    # Unlike sets which have a homogeneous element type, lists don't, and
    # elements of any type can be appended to a list:
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'c': {'Action': 'ADD', 'Value': [3]}})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['c'] == ['a', 'b', 3]

    # ADD doesn't support any other type as the value parameter.
    # In particular, it can't do things like append strings, or add items to
    # a map, or add a single string value to a set.
    with pytest.raises(ClientError, match='ValidationException.*type'):
        test_table_s.put_item(Item={'p': p, 'a': 'dog'})
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Action': 'ADD', 'Value': 's'}})
    with pytest.raises(ClientError, match='ValidationException.*type'):
        test_table_s.put_item(Item={'p': p, 'a': {'x': 1}})
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Action': 'ADD', 'Value': {'y': 2}}})
    with pytest.raises(ClientError, match='ValidationException.*type'):
        test_table_s.put_item(Item={'p': p, 'a': set(['dog'])})
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Action': 'ADD', 'Value': 'cat'}})

    # If the entire item doesn't exist, ADD can create it just like we
    # tested above that it can create an attribute.
    for value in [3, set([1, 2]), set(['a', 'b']), [1, 2]]:
        test_table_s.delete_item(Key={'p': p})
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Action': 'ADD', 'Value': value}})
        assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': value}

# DynamoDB Does not allow empty strings, empty byte arrays, or empty sets.
# Trying to ask UpdateItem to PUT one of these in an attribute should be
# forbidden. Empty lists and maps *are* allowed.
def test_update_item_empty_attribute(test_table_s):
    p = random_string()
    # Empty string, byte array and set are *not* allowed
    with pytest.raises(ClientError, match='ValidationException.*empty'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'a': {'Action': 'PUT', 'Value': ''}})
    with pytest.raises(ClientError, match='ValidationException.*empty'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'b': {'Action': 'PUT', 'Value': bytearray('', 'utf-8')}})
    with pytest.raises(ClientError, match='ValidationException.*empty'):
        test_table_s.update_item(Key={'p': p},
            AttributeUpdates={'c': {'Action': 'PUT', 'Value': set([])}})
    assert not 'Item' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)
    # But empty lists and maps *are* allowed:
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'d': {'Action': 'PUT', 'Value': []}})
    test_table_s.update_item(Key={'p': p},
        AttributeUpdates={'e': {'Action': 'PUT', 'Value': {}}})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'd': [], 'e': {}}

# Same as the above test (that we cannot create empty strings, blobs or
# sets), but using PutItem
def test_put_item_empty_attribute(test_table_s):
    p = random_string()
    # Empty string, byte array and set are *not* allowed
    with pytest.raises(ClientError, match='ValidationException.*empty'):
        test_table_s.put_item(Item={'p': p, 'a': ''})
    with pytest.raises(ClientError, match='ValidationException.*empty'):
        test_table_s.put_item(Item={'p': p, 'a': bytearray('', 'utf-8')})
    with pytest.raises(ClientError, match='ValidationException.*empty'):
        test_table_s.put_item(Item={'p': p, 'a': set([])})
    assert not 'Item' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)
    # But empty lists and maps *are* allowed:
    test_table_s.put_item(Item={'p': p, 'a': [], 'b': {}})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': [], 'b': {}}
