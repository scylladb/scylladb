# Copyright 2019-present ScyllaDB
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

# Tests of GSI (Global Secondary Indexes)
#
# Note that many of these tests are slower than usual, because many of them
# need to create new tables and/or new GSIs of different types, operations
# which are extremely slow in DynamoDB, often taking minutes (!).

import pytest
import time
from botocore.exceptions import ClientError, ParamValidationError
from util import create_test_table, random_string, full_scan, full_query, multiset, list_tables

# GSIs only support eventually consistent reads, so tests that involve
# writing to a table and then expect to read something from it cannot be
# guaranteed to succeed without retrying the read. The following utility
# functions make it easy to write such tests.
# Note that in practice, there repeated reads are almost never necessary:
# Amazon claims that "Changes to the table data are propagated to the global
# secondary indexes within a fraction of a second, under normal conditions"
# and indeed, in practice, the tests here almost always succeed without a
# retry.
# However, it is worthwhile to differenciate between the case where the
# result set is not *yet* complete (which is ok, and requires retry), and
# the case that the result set has wrong data. In the latter case, the
# test will surely fail and no amount of retry will help, so we should
# fail quickly, to avoid xfailing tests being very slow.
def assert_index_query(table, index_name, expected_items, **kwargs):
    expected = multiset(expected_items)
    for i in range(5):
        got = multiset(full_query(table, IndexName=index_name, ConsistentRead=False, **kwargs))
        if expected == got:
            return
        elif got - expected:
            # If we got any items that weren't expected, there's no point to retry.
            pytest.fail("assert_index_query() found unexpected items: " + str(got - expected))
        print('assert_index_query retrying')
        time.sleep(1)
    assert multiset(expected_items) == multiset(full_query(table, IndexName=index_name, ConsistentRead=False, **kwargs))

def assert_index_scan(table, index_name, expected_items, **kwargs):
    expected = multiset(expected_items)
    for i in range(5):
        got =  multiset(full_scan(table, IndexName=index_name, ConsistentRead=False, **kwargs))
        if expected == got:
            return
        elif got - expected:
            # If we got any items that weren't expected, there's no point to retry.
            pytest.fail("assert_index_scan() found unexpected items: " + str(got - expected))
        print('assert_index_scan retrying')
        time.sleep(1)
    assert multiset(expected_items) == multiset(full_scan(table, IndexName=index_name, ConsistentRead=False, **kwargs))

# Although quite silly, it is actually allowed to create an index which is
# identical to the base table.
# The following test does not work for KA/LA tables due to #6157,
# so it's hereby skipped.
@pytest.mark.skip
def test_gsi_identical(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
        GlobalSecondaryIndexes=[
            {   'IndexName': 'hello',
                'KeySchema': [{ 'AttributeName': 'p', 'KeyType': 'HASH' }],
                'Projection': { 'ProjectionType': 'ALL' }
            }
        ])
    items = [{'p': random_string(), 'x': random_string()} for i in range(10)]
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    # Scanning the entire table directly or via the index yields the same
    # results (in different order).
    assert multiset(items) == multiset(full_scan(table))
    assert_index_scan(table, 'hello', items)
    # We can't scan a non-existent index
    with pytest.raises(ClientError, match='ValidationException'):
        full_scan(table, ConsistentRead=False, IndexName='wrong')
    table.delete()

# One of the simplest forms of a non-trivial GSI: The base table has a hash
# and sort key, and the index reverses those roles. Other attributes are just
# copied.
@pytest.fixture(scope="module")
def test_table_gsi_1(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'c', 'KeyType': 'RANGE' }
        ],
        AttributeDefinitions=[
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'c', 'AttributeType': 'S' },
        ],
        GlobalSecondaryIndexes=[
            {   'IndexName': 'hello',
                'KeySchema': [
                    { 'AttributeName': 'c', 'KeyType': 'HASH' },
                    { 'AttributeName': 'p', 'KeyType': 'RANGE' },
                ],
                'Projection': { 'ProjectionType': 'ALL' }
            }
        ],
        )
    yield table
    table.delete()

def test_gsi_simple(test_table_gsi_1):
    items = [{'p': random_string(), 'c': random_string(), 'x': random_string()} for i in range(10)]
    with test_table_gsi_1.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    c = items[0]['c']
    # The index allows a query on just a specific sort key, which isn't
    # allowed on the base table.
    with pytest.raises(ClientError, match='ValidationException'):
        full_query(test_table_gsi_1, KeyConditions={'c': {'AttributeValueList': [c], 'ComparisonOperator': 'EQ'}})
    expected_items = [x for x in items if x['c'] == c]
    assert_index_query(test_table_gsi_1, 'hello', expected_items,
        KeyConditions={'c': {'AttributeValueList': [c], 'ComparisonOperator': 'EQ'}})
    # Scanning the entire table directly or via the index yields the same
    # results (in different order).
    assert_index_scan(test_table_gsi_1, 'hello', full_scan(test_table_gsi_1))

def test_gsi_same_key(test_table_gsi_1):
    c = random_string();
    # All these items have the same sort key 'c' but different hash key 'p'
    items = [{'p': random_string(), 'c': c, 'x': random_string()} for i in range(10)]
    with test_table_gsi_1.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    assert_index_query(test_table_gsi_1, 'hello', items,
        KeyConditions={'c': {'AttributeValueList': [c], 'ComparisonOperator': 'EQ'}})

# Check we get an appropriate error when trying to read a non-existing index
# of an existing table. Although the documentation specifies that a
# ResourceNotFoundException should be returned if "The operation tried to
# access a nonexistent table or index", in fact in the specific case that
# the table does exist but an index does not - we get a ValidationException.
def test_gsi_missing_index(test_table_gsi_1):
    with pytest.raises(ClientError, match='ValidationException.*wrong_name'):
        full_query(test_table_gsi_1, IndexName='wrong_name',
            KeyConditions={'x': {'AttributeValueList': [1], 'ComparisonOperator': 'EQ'}})
    with pytest.raises(ClientError, match='ValidationException.*wrong_name'):
        full_scan(test_table_gsi_1, IndexName='wrong_name')

# Nevertheless, if the table itself does not exist, a query should return
# a ResourceNotFoundException, not ValidationException:
def test_gsi_missing_table(dynamodb):
    with pytest.raises(ClientError, match='ResourceNotFoundException'):
        dynamodb.meta.client.query(TableName='nonexistent_table', IndexName='any_name', KeyConditions={'x': {'AttributeValueList': [1], 'ComparisonOperator': 'EQ'}})
    with pytest.raises(ClientError, match='ResourceNotFoundException'):
        dynamodb.meta.client.scan(TableName='nonexistent_table', IndexName='any_name')

# Verify that strongly-consistent reads on GSI are *not* allowed.
def test_gsi_strong_consistency(test_table_gsi_1):
    with pytest.raises(ClientError, match='ValidationException.*Consistent'):
        full_query(test_table_gsi_1, KeyConditions={'c': {'AttributeValueList': ['hi'], 'ComparisonOperator': 'EQ'}}, IndexName='hello', ConsistentRead=True)
    with pytest.raises(ClientError, match='ValidationException.*Consistent'):
        full_scan(test_table_gsi_1, IndexName='hello', ConsistentRead=True)

# Test that setting an indexed string column to an empty string is illegal,
# since keys cannot contain empty strings
def test_gsi_empty_value(test_table_gsi_2):
    with pytest.raises(ClientError, match='ValidationException.*empty'):
        test_table_gsi_2.put_item(Item={'p': random_string(), 'x': ''})

# Verify that a GSI is correctly listed in describe_table
@pytest.mark.xfail(reason="DescribeTable provides index names only, no size or item count")
def test_gsi_describe(test_table_gsi_1):
    desc = test_table_gsi_1.meta.client.describe_table(TableName=test_table_gsi_1.name)
    assert 'Table' in desc
    assert 'GlobalSecondaryIndexes' in desc['Table']
    gsis = desc['Table']['GlobalSecondaryIndexes']
    assert len(gsis) == 1
    gsi = gsis[0]
    assert gsi['IndexName'] == 'hello'
    assert 'IndexSizeBytes' in gsi     # actual size depends on content
    assert 'ItemCount' in gsi
    assert gsi['Projection'] == {'ProjectionType': 'ALL'}
    assert gsi['IndexStatus'] == 'ACTIVE'
    assert gsi['KeySchema'] == [{'KeyType': 'HASH', 'AttributeName': 'c'},
                                {'KeyType': 'RANGE', 'AttributeName': 'p'}]
    # TODO: check also ProvisionedThroughput, IndexArn

# When a GSI's key includes an attribute not in the base table's key, we
# need to remember to add its type to AttributeDefinitions.
def test_gsi_missing_attribute_definition(dynamodb):
    with pytest.raises(ClientError, match='ValidationException.*AttributeDefinitions'):
        create_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
            AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' } ],
            GlobalSecondaryIndexes=[
                {   'IndexName': 'hello',
                    'KeySchema': [ { 'AttributeName': 'c', 'KeyType': 'HASH' } ],
                    'Projection': { 'ProjectionType': 'ALL' }
                }
            ])

# test_table_gsi_1_hash_only is a variant of test_table_gsi_1: It's another
# case where the index doesn't involve non-key attributes. Again the base
# table has a hash and sort key, but in this case the index has *only* a
# hash key (which is the base's hash key). In the materialized-view-based
# implementation, we need to remember the other part of the base key as a
# clustering key.
@pytest.fixture(scope="module")
def test_table_gsi_1_hash_only(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'c', 'KeyType': 'RANGE' }
        ],
        AttributeDefinitions=[
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'c', 'AttributeType': 'S' },
        ],
        GlobalSecondaryIndexes=[
            {   'IndexName': 'hello',
                'KeySchema': [
                    { 'AttributeName': 'c', 'KeyType': 'HASH' },
                ],
                'Projection': { 'ProjectionType': 'ALL' }
            }
        ],
        )
    yield table
    table.delete()

def test_gsi_key_not_in_index(test_table_gsi_1_hash_only):
    # Test with items with different 'c' values:
    items = [{'p': random_string(), 'c': random_string(), 'x': random_string()} for i in range(10)]
    with test_table_gsi_1_hash_only.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    c = items[0]['c']
    expected_items = [x for x in items if x['c'] == c]
    assert_index_query(test_table_gsi_1_hash_only, 'hello', expected_items,
        KeyConditions={'c': {'AttributeValueList': [c], 'ComparisonOperator': 'EQ'}})
    # Test items with the same sort key 'c' but different hash key 'p'
    c = random_string();
    items = [{'p': random_string(), 'c': c, 'x': random_string()} for i in range(10)]
    with test_table_gsi_1_hash_only.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    assert_index_query(test_table_gsi_1_hash_only, 'hello', items,
        KeyConditions={'c': {'AttributeValueList': [c], 'ComparisonOperator': 'EQ'}})
    # Scanning the entire table directly or via the index yields the same
    # results (in different order).
    assert_index_scan(test_table_gsi_1_hash_only, 'hello', full_scan(test_table_gsi_1_hash_only))


# A second scenario of GSI. Base table has just hash key, Index has a
# different hash key - one of the non-key attributes from the base table.
@pytest.fixture(scope="module")
def test_table_gsi_2(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        AttributeDefinitions=[
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'x', 'AttributeType': 'S' },
        ],
        GlobalSecondaryIndexes=[
            {   'IndexName': 'hello',
                'KeySchema': [
                    { 'AttributeName': 'x', 'KeyType': 'HASH' },
                ],
                'Projection': { 'ProjectionType': 'ALL' }
            }
        ])
    yield table
    table.delete()

def test_gsi_2(test_table_gsi_2):
    items1 = [{'p': random_string(), 'x': random_string()} for i in range(10)]
    x1 = items1[0]['x']
    x2 = random_string()
    items2 = [{'p': random_string(), 'x': x2} for i in range(10)]
    items = items1 + items2
    with test_table_gsi_2.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    expected_items = [i for i in items if i['x'] == x1]
    assert_index_query(test_table_gsi_2, 'hello', expected_items,
        KeyConditions={'x': {'AttributeValueList': [x1], 'ComparisonOperator': 'EQ'}})
    expected_items = [i for i in items if i['x'] == x2]
    assert_index_query(test_table_gsi_2, 'hello', expected_items,
        KeyConditions={'x': {'AttributeValueList': [x2], 'ComparisonOperator': 'EQ'}})

# Test that when a table has a GSI, if the indexed attribute is missing, the
# item is added to the base table but not the index.
def test_gsi_missing_attribute(test_table_gsi_2):
    p1 = random_string()
    x1 = random_string()
    test_table_gsi_2.put_item(Item={'p':  p1, 'x': x1})
    p2 = random_string()
    test_table_gsi_2.put_item(Item={'p':  p2})

    # Both items are now in the base table:
    assert test_table_gsi_2.get_item(Key={'p':  p1}, ConsistentRead=True)['Item'] == {'p': p1, 'x': x1}
    assert test_table_gsi_2.get_item(Key={'p':  p2}, ConsistentRead=True)['Item'] == {'p': p2}

    # But only the first item is in the index: It can be found using a
    # Query, and a scan of the index won't find it (but a scan on the base
    # will).
    assert_index_query(test_table_gsi_2, 'hello', [{'p': p1, 'x': x1}],
        KeyConditions={'x': {'AttributeValueList': [x1], 'ComparisonOperator': 'EQ'}})
    assert any([i['p'] == p1 for i in full_scan(test_table_gsi_2)])
    # Note: with eventually consistent read, we can't really be sure that
    # and item will "never" appear in the index. We do this test last,
    # so if we had a bug and such item did appear, hopefully we had enough
    # time for the bug to become visible. At least sometimes.
    assert not any([i['p'] == p2 for i in full_scan(test_table_gsi_2, ConsistentRead=False, IndexName='hello')])

# Test when a table has a GSI, if the indexed attribute has the wrong type,
# the update operation is rejected, and is added to neither base table nor
# index. This is different from the case of a *missing* attribute, where
# the item is added to the base table but not index.
# The following three tests test_gsi_wrong_type_attribute_{put,update,batch}
# test updates using PutItem, UpdateItem, and BatchWriteItem respectively.
def test_gsi_wrong_type_attribute_put(test_table_gsi_2):
    # PutItem with wrong type for 'x' is rejected, item isn't created even
    # in the base table.
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException.*mismatch'):
        test_table_gsi_2.put_item(Item={'p':  p, 'x': 3})
    assert not 'Item' in test_table_gsi_2.get_item(Key={'p': p}, ConsistentRead=True)

def test_gsi_wrong_type_attribute_update(test_table_gsi_2):
    # An UpdateItem with wrong type for 'x' is also rejected, but naturally
    # if the item already existed, it remains as it was.
    p = random_string()
    x = random_string()
    test_table_gsi_2.put_item(Item={'p':  p, 'x': x})
    with pytest.raises(ClientError, match='ValidationException.*mismatch'):
        test_table_gsi_2.update_item(Key={'p':  p}, AttributeUpdates={'x': {'Value': 3, 'Action': 'PUT'}})
    assert test_table_gsi_2.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'x': x}

# Since a GSI key x cannot be a map or an array, in particular updates to
# nested attributes like x.y or x[1] are not legal. The error that DynamoDB
# reports is "Key attributes must be scalars; list random access '[]' and map
# lookup '.' are not allowed: IndexKey: x".
def test_gsi_wrong_type_attribute_update_nested(test_table_gsi_2):
    p = random_string()
    x = random_string()
    test_table_gsi_2.put_item(Item={'p':  p, 'x': x})
    # We can't write a map into a GSI key column, which in this case can only
    # be a string and in any case can never be a map. DynamoDB and Alternator
    # report different errors here: DynamoDB reports a type mismatch (exactly
    # like in test test_gsi_wrong_type_attribute_update), but Alternator
    # reports the obscure message "Malformed value object for key column x".
    # Alternator's error message should probably be improved here, but let's
    # not test it in this test.
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_gsi_2.update_item(Key={'p': p}, UpdateExpression='SET x = :val1',
            ExpressionAttributeValues={':val1': {'a': 3, 'b': 4}})
    # Here we try to set x.y for the GSI key column x. Again DynamoDB and
    # Alternator produce different error messages - but both make sense.
    # DynamoDB says "Key attributes must be scalars; list random access '[]'
    # and map # lookup '.' are not allowed: IndexKey: x", while Alternator
    # complains that "document paths not valid for this item: x.y".
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_gsi_2.update_item(Key={'p': p}, UpdateExpression='SET x.y = :val1',
            ExpressionAttributeValues={':val1': 3})

def test_gsi_wrong_type_attribute_batch(test_table_gsi_2):
    # In a BatchWriteItem, if any update is forbidden, the entire batch is
    # rejected, and none of the updates happen at all.
    p1 = random_string()
    p2 = random_string()
    p3 = random_string()
    items = [{'p': p1, 'x': random_string()},
             {'p': p2, 'x': 3},
             {'p': p3, 'x': random_string()}]
    with pytest.raises(ClientError, match='ValidationException.*mismatch'):
        with test_table_gsi_2.batch_writer() as batch:
            for item in items:
                batch.put_item(item)
    for p in [p1, p2, p3]:
        assert not 'Item' in test_table_gsi_2.get_item(Key={'p': p}, ConsistentRead=True)

# A third scenario of GSI. Index has a hash key and a sort key, both are
# non-key attributes from the base table. This scenario may be very
# difficult to implement in Alternator because Scylla's materialized-views
# implementation only allows one new key column in the view, and here
# we need two (which, also, aren't actual columns, but map items).
@pytest.fixture(scope="module")
def test_table_gsi_3(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        AttributeDefinitions=[
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'a', 'AttributeType': 'S' },
                    { 'AttributeName': 'b', 'AttributeType': 'S' }
        ],
        GlobalSecondaryIndexes=[
            {   'IndexName': 'hello',
                'KeySchema': [
                    { 'AttributeName': 'a', 'KeyType': 'HASH' },
                    { 'AttributeName': 'b', 'KeyType': 'RANGE' }
                ],
                'Projection': { 'ProjectionType': 'ALL' }
            }
        ])
    yield table
    table.delete()

def test_gsi_3(test_table_gsi_3):
    items = [{'p': random_string(), 'a': random_string(), 'b': random_string()} for i in range(10)]
    with test_table_gsi_3.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    assert_index_query(test_table_gsi_3, 'hello', [items[3]],
        KeyConditions={'a': {'AttributeValueList': [items[3]['a']], 'ComparisonOperator': 'EQ'},
                       'b': {'AttributeValueList': [items[3]['b']], 'ComparisonOperator': 'EQ'}})

def test_gsi_update_second_regular_base_column(test_table_gsi_3):
    items = [{'p': random_string(), 'a': random_string(), 'b': random_string(), 'd': random_string()} for i in range(10)]
    with test_table_gsi_3.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    items[3]['b'] = 'updated'
    test_table_gsi_3.update_item(Key={'p':  items[3]['p']}, AttributeUpdates={'b': {'Value': 'updated', 'Action': 'PUT'}})
    assert_index_query(test_table_gsi_3, 'hello', [items[3]],
        KeyConditions={'a': {'AttributeValueList': [items[3]['a']], 'ComparisonOperator': 'EQ'},
                       'b': {'AttributeValueList': [items[3]['b']], 'ComparisonOperator': 'EQ'}})

# Test that when a table has a GSI, if the indexed attribute is missing, the
# item is added to the base table but not the index.
# This is the same feature we already tested in test_gsi_missing_attribute()
# above, but on a different table: In that test we used test_table_gsi_2,
# with one indexed attribute, and in this test we use test_table_gsi_3 which
# has two base regular attributes in the view key, and more possibilities
# of which value might be missing. Reproduces issue #6008.
def test_gsi_missing_attribute_3(test_table_gsi_3):
    p = random_string()
    a = random_string()
    b = random_string()
    # First, add an item with a missing "a" value. It should appear in the
    # base table, but not in the index:
    test_table_gsi_3.put_item(Item={'p':  p, 'b': b})
    assert test_table_gsi_3.get_item(Key={'p':  p}, ConsistentRead=True)['Item'] == {'p': p, 'b': b}
    # Note: with eventually consistent read, we can't really be sure that
    # an item will "never" appear in the index. We hope that if a bug exists
    # and such an item did appear, sometimes the delay here will be enough
    # for the unexpected item to become visible.
    assert not any([i['p'] == p for i in full_scan(test_table_gsi_3, ConsistentRead=False, IndexName='hello')])
    # Same thing for an item with a missing "b" value:
    test_table_gsi_3.put_item(Item={'p':  p, 'a': a})
    assert test_table_gsi_3.get_item(Key={'p':  p}, ConsistentRead=True)['Item'] == {'p': p, 'a': a}
    assert not any([i['p'] == p for i in full_scan(test_table_gsi_3, ConsistentRead=False, IndexName='hello')])
    # And for an item missing both:
    test_table_gsi_3.put_item(Item={'p':  p})
    assert test_table_gsi_3.get_item(Key={'p':  p}, ConsistentRead=True)['Item'] == {'p': p}
    assert not any([i['p'] == p for i in full_scan(test_table_gsi_3, ConsistentRead=False, IndexName='hello')])

# A fourth scenario of GSI. Two GSIs on a single base table.
@pytest.fixture(scope="module")
def test_table_gsi_4(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        AttributeDefinitions=[
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'a', 'AttributeType': 'S' },
                    { 'AttributeName': 'b', 'AttributeType': 'S' }
        ],
        GlobalSecondaryIndexes=[
            {   'IndexName': 'hello_a',
                'KeySchema': [
                    { 'AttributeName': 'a', 'KeyType': 'HASH' },
                ],
                'Projection': { 'ProjectionType': 'ALL' }
            },
            {   'IndexName': 'hello_b',
                'KeySchema': [
                    { 'AttributeName': 'b', 'KeyType': 'HASH' },
                ],
                'Projection': { 'ProjectionType': 'ALL' }
            }
        ])
    yield table
    table.delete()

# Test that a base table with two GSIs updates both as expected.
def test_gsi_4(test_table_gsi_4):
    items = [{'p': random_string(), 'a': random_string(), 'b': random_string()} for i in range(10)]
    with test_table_gsi_4.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    assert_index_query(test_table_gsi_4, 'hello_a', [items[3]],
        KeyConditions={'a': {'AttributeValueList': [items[3]['a']], 'ComparisonOperator': 'EQ'}})
    assert_index_query(test_table_gsi_4, 'hello_b', [items[3]],
        KeyConditions={'b': {'AttributeValueList': [items[3]['b']], 'ComparisonOperator': 'EQ'}})

# Verify that describe_table lists the two GSIs.
def test_gsi_4_describe(test_table_gsi_4):
    desc = test_table_gsi_4.meta.client.describe_table(TableName=test_table_gsi_4.name)
    assert 'Table' in desc
    assert 'GlobalSecondaryIndexes' in desc['Table']
    gsis = desc['Table']['GlobalSecondaryIndexes']
    assert len(gsis) == 2
    assert multiset([g['IndexName'] for g in gsis]) == multiset(['hello_a', 'hello_b'])

# A scenario for GSI in which the table has both hash and sort key
@pytest.fixture(scope="module")
def test_table_gsi_5(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, { 'AttributeName': 'c', 'KeyType': 'RANGE' } ],
        AttributeDefinitions=[
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'c', 'AttributeType': 'S' },
                    { 'AttributeName': 'x', 'AttributeType': 'S' },
        ],
        GlobalSecondaryIndexes=[
            {   'IndexName': 'hello',
                'KeySchema': [
                    { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'x', 'KeyType': 'RANGE' },
                ],
                'Projection': { 'ProjectionType': 'ALL' }
            }
        ])
    yield table
    table.delete()

def test_gsi_5(test_table_gsi_5):
    items1 = [{'p': random_string(), 'c': random_string(), 'x': random_string()} for i in range(10)]
    p1, x1 = items1[0]['p'], items1[0]['x']
    p2, x2 = random_string(), random_string()
    items2 = [{'p': p2, 'c': random_string(), 'x': x2} for i in range(10)]
    items = items1 + items2
    with test_table_gsi_5.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    expected_items = [i for i in items if i['p'] == p1 and i['x'] == x1]
    assert_index_query(test_table_gsi_5, 'hello', expected_items,
        KeyConditions={'p': {'AttributeValueList': [p1], 'ComparisonOperator': 'EQ'},
                       'x': {'AttributeValueList': [x1], 'ComparisonOperator': 'EQ'}})
    expected_items = [i for i in items if i['p'] == p2 and i['x'] == x2]
    assert_index_query(test_table_gsi_5, 'hello', expected_items,
        KeyConditions={'p': {'AttributeValueList': [p2], 'ComparisonOperator': 'EQ'},
                       'x': {'AttributeValueList': [x2], 'ComparisonOperator': 'EQ'}})

# Verify that DescribeTable correctly returns the schema of both base-table
# and secondary indexes. KeySchema is given for each of the base table and
# indexes, and AttributeDefinitions is merged for all of them together.
def test_gsi_5_describe_table_schema(test_table_gsi_5):
    got = test_table_gsi_5.meta.client.describe_table(TableName=test_table_gsi_5.name)['Table']
    # Copied from test_table_gsi_5 fixture
    expected_base_keyschema = [
                    { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'c', 'KeyType': 'RANGE' } ]
    expected_gsi_keyschema = [
                    { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'x', 'KeyType': 'RANGE' } ]
    expected_all_attribute_definitions = [
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'c', 'AttributeType': 'S' },
                    { 'AttributeName': 'x', 'AttributeType': 'S' } ]
    assert got['KeySchema'] == expected_base_keyschema
    gsis = got['GlobalSecondaryIndexes']
    assert len(gsis) == 1
    assert gsis[0]['KeySchema'] == expected_gsi_keyschema
    # The list of attribute definitions may be arbitrarily reordered
    assert multiset(got['AttributeDefinitions']) == multiset(expected_all_attribute_definitions)

# Similar DescribeTable schema test for test_table_gsi_2. The peculiarity
# in that table is that the base table has only a hash key p, and index
# only hash hash key x; Now, while internally Scylla needs to add "p" as a
# clustering key in the materialized view (in Scylla the view key always
# contains the base key), when describing the table, "p" shouldn't be
# returned as a range key, because the user didn't ask for it.
# This test reproduces issue #5320.
@pytest.mark.xfail(reason="GSI DescribeTable spurious range key (#5320)")
def test_gsi_2_describe_table_schema(test_table_gsi_2):
    got = test_table_gsi_2.meta.client.describe_table(TableName=test_table_gsi_2.name)['Table']
    # Copied from test_table_gsi_2 fixture
    expected_base_keyschema = [ { 'AttributeName': 'p', 'KeyType': 'HASH' } ]
    expected_gsi_keyschema = [ { 'AttributeName': 'x', 'KeyType': 'HASH' } ]
    expected_all_attribute_definitions = [
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'x', 'AttributeType': 'S' } ]
    assert got['KeySchema'] == expected_base_keyschema
    gsis = got['GlobalSecondaryIndexes']
    assert len(gsis) == 1
    assert gsis[0]['KeySchema'] == expected_gsi_keyschema
    # The list of attribute definitions may be arbitrarily reordered
    assert multiset(got['AttributeDefinitions']) == multiset(expected_all_attribute_definitions)

# All tests above involved "ProjectionType: ALL". This test checks how
# "ProjectionType:: KEYS_ONLY" works. We note that it projects both
# the index's key, *and* the base table's key. So items which had different
# base-table keys cannot suddenly become the same item in the index.
@pytest.mark.xfail(reason="GSI projection not supported - issue #5036")
def test_gsi_projection_keys_only(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        AttributeDefinitions=[
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'x', 'AttributeType': 'S' },
        ],
        GlobalSecondaryIndexes=[
            {   'IndexName': 'hello',
                'KeySchema': [
                    { 'AttributeName': 'x', 'KeyType': 'HASH' },
                ],
                'Projection': { 'ProjectionType': 'KEYS_ONLY' }
            }
        ])
    items = [{'p': random_string(), 'x': random_string(), 'y': random_string()} for i in range(10)]
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    wanted = ['p', 'x']
    expected_items = [{k: x[k] for k in wanted if k in x} for x in items]
    assert_index_scan(table, 'hello', expected_items)
    table.delete()

# Test for "ProjectionType:: INCLUDE". The secondary table includes the
# its own and the base's keys (as in KEYS_ONLY) plus the extra keys given
# in NonKeyAttributes.
@pytest.mark.xfail(reason="GSI projection not supported - issue #5036")
def test_gsi_projection_include(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        AttributeDefinitions=[
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'x', 'AttributeType': 'S' },
        ],
        GlobalSecondaryIndexes=[
            {   'IndexName': 'hello',
                'KeySchema': [
                    { 'AttributeName': 'x', 'KeyType': 'HASH' },
                ],
                'Projection': { 'ProjectionType': 'INCLUDE',
                                'NonKeyAttributes': ['a', 'b'] }
            }
        ])
    # Some items have the projected attributes a,b and some don't:
    items = [{'p': random_string(), 'x': random_string(), 'a': random_string(), 'b': random_string(), 'y': random_string()} for i in range(10)]
    items = items + [{'p': random_string(), 'x': random_string(), 'y': random_string()} for i in range(10)]
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    wanted = ['p', 'x', 'a', 'b']
    expected_items = [{k: x[k] for k in wanted if k in x} for x in items]
    assert_index_scan(table, 'hello', expected_items)
    print(len(expected_items))
    table.delete()

# DynamoDB's says the "Projection" argument of GlobalSecondaryIndexes is
# mandatory, and indeed Boto3 enforces that it must be passed. The
# documentation then goes on to claim that the "ProjectionType" member of
# "Projection" is optional - and Boto3 allows it to be missing. But in
# fact, it is not allowed to be missing: DynamoDB complains: "Unknown
# ProjectionType: null".
@pytest.mark.xfail(reason="GSI projection not supported - issue #5036")
def test_gsi_missing_projection_type(dynamodb):
    with pytest.raises(ClientError, match='ValidationException.*ProjectionType'):
        create_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
            GlobalSecondaryIndexes=[
                {   'IndexName': 'hello',
                    'KeySchema': [{ 'AttributeName': 'p', 'KeyType': 'HASH' }],
                    'Projection': {}
                }
            ])

# update_table() for creating a GSI is an asynchronous operation.
# The table's TableStatus changes from ACTIVE to UPDATING for a short while
# and then goes back to ACTIVE, but the new GSI's IndexStatus appears as
# CREATING, until eventually (after a *long* time...) it becomes ACTIVE.
# During the CREATING phase, at some point the Backfilling attribute also
# appears, until it eventually disappears. We need to wait until all three
# markers indicate completion.
# Unfortunately, while boto3 has a client.get_waiter('table_exists') to
# wait for a table to exists, there is no such function to wait for an
# index to come up, so we need to code it ourselves.
def wait_for_gsi(table, gsi_name):
    start_time = time.time()
    # Surprisingly, even for tiny tables this can take a very long time
    # on DynamoDB - often many minutes!
    for i in range(300):
        time.sleep(1)
        desc = table.meta.client.describe_table(TableName=table.name)
        table_status = desc['Table']['TableStatus']
        if table_status != 'ACTIVE':
            print('%d Table status still %s' % (i, table_status))
            continue
        index_desc = [x for x in desc['Table']['GlobalSecondaryIndexes'] if x['IndexName'] == gsi_name]
        assert len(index_desc) == 1
        index_status = index_desc[0]['IndexStatus']
        if index_status != 'ACTIVE':
            print('%d Index status still %s' % (i, index_status))
            continue
        # When the index is ACTIVE, this must be after backfilling completed
        assert not 'Backfilling' in index_desc[0]
        print('wait_for_gsi took %d seconds' % (time.time() - start_time))
        return
    raise AssertionError("wait_for_gsi did not complete")

# Similarly to how wait_for_gsi() waits for a GSI to finish adding,
# this function waits for a GSI to be finally deleted.
def wait_for_gsi_gone(table, gsi_name):
    start_time = time.time()
    for i in range(300):
        time.sleep(1)
        desc = table.meta.client.describe_table(TableName=table.name)
        table_status = desc['Table']['TableStatus']
        if table_status != 'ACTIVE':
            print('%d Table status still %s' % (i, table_status))
            continue
        if 'GlobalSecondaryIndexes' in desc['Table']:
            index_desc = [x for x in desc['Table']['GlobalSecondaryIndexes'] if x['IndexName'] == gsi_name]
            if len(index_desc) != 0:
                index_status = index_desc[0]['IndexStatus']
                print('%d Index status still %s' % (i, index_status))
                continue
        print('wait_for_gsi_gone took %d seconds' % (time.time() - start_time))
        return
    raise AssertionError("wait_for_gsi_gone did not complete")

# All tests above involved creating a new table with a GSI up-front. This
# test will test creating a base table *without* a GSI, putting data in
# it, and then adding a GSI with the UpdateTable operation. This starts
# a backfilling stage - where data is copied to the index - and when this
# stage is done, the index is usable. Items whose indexed column contains
# the wrong type are silently ignored and not added to the index (it would
# not have been possible to add such items if the GSI was already configured
# when they were added).
@pytest.mark.xfail(reason="GSI not supported")
def test_gsi_backfill(dynamodb):
    # First create, and fill, a table without GSI. The items in items1
    # will have the appropriate string type for 'x' and will later get
    # indexed. Items in item2 have no value for 'x', and in item3 'x' is in
    # not a string; So the items in items2 and items3 will be missing
    # in the index we'll create later.
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' } ])
    items1 = [{'p': random_string(), 'x': random_string(), 'y': random_string()} for i in range(10)]
    items2 = [{'p': random_string(), 'y': random_string()} for i in range(10)]
    items3 = [{'p': random_string(), 'x': i} for i in range(10)]
    items = items1 + items2 + items3
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    assert multiset(items) == multiset(full_scan(table))
    # Now use UpdateTable to create the GSI
    dynamodb.meta.client.update_table(TableName=table.name,
        AttributeDefinitions=[{ 'AttributeName': 'x', 'AttributeType': 'S' }],
        GlobalSecondaryIndexUpdates=[ {  'Create':
            {  'IndexName': 'hello',
                'KeySchema': [{ 'AttributeName': 'x', 'KeyType': 'HASH' }],
                'Projection': { 'ProjectionType': 'ALL' }
            }}])
    # update_table is an asynchronous operation. We need to wait until it
    # finishes and the table is backfilled.
    wait_for_gsi(table, 'hello')
    # As explained above, only items in items1 got copied to the gsi,
    # and Scan on them works as expected.
    # Note that we don't need to retry the reads here (i.e., use the
    # assert_index_scan() or assert_index_query() functions) because after
    # we waited for backfilling to complete, we know all the pre-existing
    # data is already in the index.
    assert multiset(items1) == multiset(full_scan(table, ConsistentRead=False, IndexName='hello'))
    # We can also use Query on the new GSI, to search on the attribute x:
    assert multiset([items1[3]]) == multiset(full_query(table,
        ConsistentRead=False, IndexName='hello',
        KeyConditions={'x': {'AttributeValueList': [items1[3]['x']], 'ComparisonOperator': 'EQ'}}))
    # Let's also test that we cannot add another index with the same name
    # that already exists
    with pytest.raises(ClientError, match='ValidationException.*already exists'):
        dynamodb.meta.client.update_table(TableName=table.name,
            AttributeDefinitions=[{ 'AttributeName': 'y', 'AttributeType': 'S' }],
            GlobalSecondaryIndexUpdates=[ {  'Create':
                {  'IndexName': 'hello',
                    'KeySchema': [{ 'AttributeName': 'y', 'KeyType': 'HASH' }],
                    'Projection': { 'ProjectionType': 'ALL' }
                }}])
    table.delete()

# Test deleting an existing GSI using UpdateTable
@pytest.mark.xfail(reason="GSI not supported")
def test_gsi_delete(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        AttributeDefinitions=[
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'x', 'AttributeType': 'S' },
        ],
        GlobalSecondaryIndexes=[
            {   'IndexName': 'hello',
                'KeySchema': [
                    { 'AttributeName': 'x', 'KeyType': 'HASH' },
                ],
                'Projection': { 'ProjectionType': 'ALL' }
            }
        ])
    items = [{'p': random_string(), 'x': random_string()} for i in range(10)]
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    # So far, we have the index for "x" and can use it:
    assert_index_query(table, 'hello', [items[3]],
        KeyConditions={'x': {'AttributeValueList': [items[3]['x']], 'ComparisonOperator': 'EQ'}})
    # Now use UpdateTable to delete the GSI for "x"
    dynamodb.meta.client.update_table(TableName=table.name,
        GlobalSecondaryIndexUpdates=[{  'Delete':
            { 'IndexName': 'hello' } }])
    # update_table is an asynchronous operation. We need to wait until it
    # finishes and the GSI is removed.
    wait_for_gsi_gone(table, 'hello')
    # Now index is gone. We cannot query using it.
    with pytest.raises(ClientError, match='ValidationException.*hello'):
        full_query(table, ConsistentRead=False, IndexName='hello',
            KeyConditions={'x': {'AttributeValueList': [items[3]['x']], 'ComparisonOperator': 'EQ'}})
    table.delete()

# Utility function for creating a new table a GSI with the given name,
# and, if creation was successful, delete it. Useful for testing which
# GSI names work.
def create_gsi(dynamodb, index_name):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
        GlobalSecondaryIndexes=[
            {   'IndexName': index_name,
                'KeySchema': [{ 'AttributeName': 'p', 'KeyType': 'HASH' }],
                'Projection': { 'ProjectionType': 'ALL' }
            }
        ])
    # Verify that the GSI wasn't just ignored, as Scylla originally did ;-)
    assert 'GlobalSecondaryIndexes' in table.meta.client.describe_table(TableName=table.name)['Table']
    table.delete()

# Like table names (tested in test_table.py), index names must must also
# be 3-255 characters and match the regex [a-zA-Z0-9._-]+. This test
# is similar to test_create_table_unsupported_names(), but for GSI names.
# Note that Scylla is actually more limited in the length of the index
# names, because both table name and index name, together, have to fit in
# 221 characters. But we don't verify here this specific limitation.
def test_gsi_unsupported_names(dynamodb):
    # Unfortunately, the boto library tests for names shorter than the
    # minimum length (3 characters) immediately, and failure results in
    # ParamValidationError. But the other invalid names are passed to
    # DynamoDB, which returns an HTTP response code, which results in a
    # CientError exception.
    with pytest.raises(ParamValidationError):
        create_gsi(dynamodb, 'n')
    with pytest.raises(ParamValidationError):
        create_gsi(dynamodb, 'nn')
    with pytest.raises(ClientError, match='ValidationException.*nnnnn'):
        create_gsi(dynamodb, 'n' * 256)
    with pytest.raises(ClientError, match='ValidationException.*nyh'):
        create_gsi(dynamodb, 'nyh@test')

# On the other hand, names following the above rules should be accepted. Even
# names which the Scylla rules forbid, such as a name starting with .
def test_gsi_non_scylla_name(dynamodb):
    create_gsi(dynamodb, '.alternator_test')

# Index names with 255 characters are allowed in Dynamo. In Scylla, the
# limit is different - the sum of both table and index length cannot
# exceed 211 characters. So we test a much shorter limit.
# (compare test_create_and_delete_table_very_long_name()).
def test_gsi_very_long_name(dynamodb):
    #create_gsi(dynamodb, 'n' * 255)   # works on DynamoDB, but not on Scylla
    create_gsi(dynamodb, 'n' * 190)

# Verify that ListTables does not list materialized views used for indexes.
# This is hard to test, because we don't really know which table names
# should be listed beyond those we created, and don't want to assume that
# no other test runs in parallel with us. So the method we chose is to use a
# unique random name for an index, and check that no table contains this
# name. This assumes that materialized-view names are composed using the
# index's name (which is currently what we do).

@pytest.fixture(scope="module")
def test_table_gsi_random_name(dynamodb):
    index_name = random_string()
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'c', 'KeyType': 'RANGE' }
        ],
        AttributeDefinitions=[
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'c', 'AttributeType': 'S' },
        ],
        GlobalSecondaryIndexes=[
            {   'IndexName': index_name,
                'KeySchema': [
                    { 'AttributeName': 'c', 'KeyType': 'HASH' },
                    { 'AttributeName': 'p', 'KeyType': 'RANGE' },
                ],
                'Projection': { 'ProjectionType': 'ALL' }
            }
        ],
        )
    yield [table, index_name]
    table.delete()

def test_gsi_list_tables(dynamodb, test_table_gsi_random_name):
    table, index_name = test_table_gsi_random_name
    # Check that the random "index_name" isn't a substring of any table name:
    tables = list_tables(dynamodb)
    for name in tables:
        assert not index_name in name
    # But of course, the table's name should be in the list:
    assert table.name in tables
