# Copyright 2019-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# Tests of GSI (Global Secondary Indexes)
#
# Note that many of these tests are slower than usual, because many of them
# need to create new tables and/or new GSIs of different types, operations
# which are extremely slow in DynamoDB, often taking minutes (!).

import pytest
import time
from botocore.exceptions import ClientError
from test.alternator.util import create_test_table, random_string, full_scan, full_query, multiset, list_tables, new_test_table

# GSIs only support eventually consistent reads, so tests that involve
# writing to a table and then expect to read something from it cannot be
# guaranteed to succeed without retrying the read. The following utility
# functions make it easy to write such tests.
# Note that in practice, there repeated reads are almost never necessary:
# Amazon claims that "Changes to the table data are propagated to the global
# secondary indexes within a fraction of a second, under normal conditions"
# and indeed, in practice, the tests here almost always succeed without a
# retry.
# However, it is worthwhile to differentiate between the case where the
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
# The following test does not work for KA/LA tables due to #6157, but we
# no longer allow writing those in Scylla.
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

def test_gsi_empty_value_with_range_key(test_table_gsi_3):
    with pytest.raises(ClientError, match='ValidationException.*empty'):
        test_table_gsi_3.put_item(Item={'p': random_string(), 'a': '', 'b': random_string()})
    with pytest.raises(ClientError, match='ValidationException.*empty'):
        test_table_gsi_3.put_item(Item={'p': random_string(), 'a': random_string(), 'b': ''})

# Dynamodb supports special way of setting NULL value.
# It's different than non existing value.
def test_gsi_null_value(test_table_gsi_2):
    with pytest.raises(ClientError, match='ValidationException.*NULL'):
        test_table_gsi_2.put_item(Item={'p': random_string(), 'x': None})

def test_gsi_null_value_with_range_key(test_table_gsi_3):
    with pytest.raises(ClientError, match='ValidationException.*NULL'):
        test_table_gsi_3.put_item(Item={'p': random_string(), 'a': None, 'b': random_string()})
    with pytest.raises(ClientError, match='ValidationException.*NULL'):
        test_table_gsi_3.put_item(Item={'p': random_string(), 'a': random_string(), 'b': None})

# Verify that a GSI is correctly listed in describe_table
def test_gsi_describe(test_table_gsi_1):
    desc = test_table_gsi_1.meta.client.describe_table(TableName=test_table_gsi_1.name)
    assert 'Table' in desc
    assert 'GlobalSecondaryIndexes' in desc['Table']
    gsis = desc['Table']['GlobalSecondaryIndexes']
    assert len(gsis) == 1
    gsi = gsis[0]
    assert gsi['IndexName'] == 'hello'
    assert gsi['Projection'] == {'ProjectionType': 'ALL'}
    assert gsi['KeySchema'] == [{'KeyType': 'HASH', 'AttributeName': 'c'},
                                {'KeyType': 'RANGE', 'AttributeName': 'p'}]
    # The index's ARN should look like the table's ARN followed by /index/<indexname>.
    assert gsi['IndexArn'] == desc['Table']['TableArn'] + '/index/hello'
    # TODO: check also ProvisionedThroughput

# In addition to the basic listing of an GSI in DescribeTable tested above,
# in this test we check additional fields that should appear in each GSI's
# description.
@pytest.mark.xfail(reason="issues #7550, #11466, #11471")
def test_gsi_describe_fields(test_table_gsi_1):
    desc = test_table_gsi_1.meta.client.describe_table(TableName=test_table_gsi_1.name)
    gsis = desc['Table']['GlobalSecondaryIndexes']
    assert len(gsis) == 1
    gsi = gsis[0]
    assert 'IndexSizeBytes' in gsi    # actual size depends on content
    assert 'ItemCount' in gsi
    assert gsi['IndexStatus'] == 'ACTIVE'

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

# Test reproducing issue #11801: In issue #5006 we noticed that in the special
# case of a GSI with with two non-key attributes as keys (test_table_gsi_3),
# an update of the second attribute forgot to delete the old row. We fixed
# that bug, but a bug remained for updates which update the value to the *same*
# value - in that case the old row shouldn't be deleted, but we did - as
# noticed in issue #11801.
def test_11801(test_table_gsi_3):
    p = random_string()
    a = random_string()
    b = random_string()
    item = {'p': p, 'a': a, 'b': b, 'd': random_string()}
    test_table_gsi_3.put_item(Item=item)
    assert_index_query(test_table_gsi_3, 'hello', [item],
        KeyConditions={'a': {'AttributeValueList': [a], 'ComparisonOperator': 'EQ'},
                       'b': {'AttributeValueList': [b], 'ComparisonOperator': 'EQ'}})
    # Update the attribute 'b' to the same value b that it already had.
    # This shouldn't change anything in the base table or in the GSI
    test_table_gsi_3.update_item(Key={'p':  p}, AttributeUpdates={'b': {'Value': b, 'Action': 'PUT'}})
    assert item == test_table_gsi_3.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    # In issue #11801, the following assertion failed (the view row was
    # deleted and nothing matched the query).
    assert_index_query(test_table_gsi_3, 'hello', [item],
        KeyConditions={'a': {'AttributeValueList': [a], 'ComparisonOperator': 'EQ'},
                       'b': {'AttributeValueList': [b], 'ComparisonOperator': 'EQ'}})
    # Above we checked that setting 'b' to the same value didn't remove
    # the old GSI row. But the same update may actually modify the GSI row
    # (e.g., an unrelated attribute d) -  check this modification took place:
    item['d'] = random_string()
    test_table_gsi_3.update_item(Key={'p':  p},
        AttributeUpdates={'b': {'Value': b, 'Action': 'PUT'},
                          'd': {'Value': item['d'], 'Action': 'PUT'}})
    assert item == test_table_gsi_3.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    assert_index_query(test_table_gsi_3, 'hello', [item],
        KeyConditions={'a': {'AttributeValueList': [a], 'ComparisonOperator': 'EQ'},
                       'b': {'AttributeValueList': [b], 'ComparisonOperator': 'EQ'}})

# This test is the same as test_11801, but updating the first attribute (a)
# instead of the second (b). This test didn't fail, showing that issue #11801
# is - like #5006 - specific to the case of updating the second attribute.
def test_11801_variant1(test_table_gsi_3):
    p = random_string()
    a = random_string()
    b = random_string()
    d = random_string()
    item = {'p': p, 'a': a, 'b': b, 'd': d}
    test_table_gsi_3.put_item(Item=item)
    assert_index_query(test_table_gsi_3, 'hello', [item],
        KeyConditions={'a': {'AttributeValueList': [a], 'ComparisonOperator': 'EQ'},
                       'b': {'AttributeValueList': [b], 'ComparisonOperator': 'EQ'}})
    test_table_gsi_3.update_item(Key={'p':  p}, AttributeUpdates={'a': {'Value': a, 'Action': 'PUT'}})
    assert_index_query(test_table_gsi_3, 'hello', [item],
        KeyConditions={'a': {'AttributeValueList': [a], 'ComparisonOperator': 'EQ'},
                       'b': {'AttributeValueList': [b], 'ComparisonOperator': 'EQ'}})

# This test is the same as test_11801, but updates b to a different value
# (newb) instead of to the same one. This test didn't fail, showing that
# issue #11801 is specific to updates to the same value. This test basically
# reproduces the already-fixed #5006 (we also have another test above which
# reproduces that issue - test_gsi_update_second_regular_base_column())
def test_11801_variant2(test_table_gsi_3):
    p = random_string()
    a = random_string()
    b = random_string()
    item = {'p': p, 'a': a, 'b': b, 'd': random_string()}
    test_table_gsi_3.put_item(Item=item)
    assert_index_query(test_table_gsi_3, 'hello', [item],
        KeyConditions={'a': {'AttributeValueList': [a], 'ComparisonOperator': 'EQ'},
                       'b': {'AttributeValueList': [b], 'ComparisonOperator': 'EQ'}})
    newb = random_string()
    item['b'] = newb
    test_table_gsi_3.update_item(Key={'p':  p}, AttributeUpdates={'b': {'Value': newb, 'Action': 'PUT'}})
    assert_index_query(test_table_gsi_3, 'hello', [],
        KeyConditions={'a': {'AttributeValueList': [a], 'ComparisonOperator': 'EQ'},
                       'b': {'AttributeValueList': [b], 'ComparisonOperator': 'EQ'}})
    assert_index_query(test_table_gsi_3, 'hello', [item],
        KeyConditions={'a': {'AttributeValueList': [a], 'ComparisonOperator': 'EQ'},
                       'b': {'AttributeValueList': [newb], 'ComparisonOperator': 'EQ'}})

# This test is the same as test_11801, but uses a different table schema
# (test_table_gsi_5) where there is only one new key column in the view (x).
# This test passed, showing that issue #11801 was specific to the special
# case of a view with two new key columns (test_table_gsi_3).
def test_11801_variant3(test_table_gsi_5):
    p = random_string()
    c = random_string()
    x = random_string()
    item = {'p': p, 'c': c, 'x': x, 'd': random_string()}
    test_table_gsi_5.put_item(Item=item)
    assert_index_query(test_table_gsi_5, 'hello', [item],
        KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
                       'x': {'AttributeValueList': [x], 'ComparisonOperator': 'EQ'}})
    test_table_gsi_5.update_item(Key={'p':  p, 'c': c}, AttributeUpdates={'x': {'Value': x, 'Action': 'PUT'}})
    assert item == test_table_gsi_5.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)['Item']
    assert_index_query(test_table_gsi_5, 'hello', [item],
        KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
                       'x': {'AttributeValueList': [x], 'ComparisonOperator': 'EQ'}})

# Another test similar to test_11801, but instead of updating a view key
# column to the same value it already has, simply don't update it at all
# (and just modify some other regular column). This test passed, showing
# that issue #11801 is specific to the case of updating a view key column
# to the same value it already had.
def test_11801_variant4(test_table_gsi_3):
    p = random_string()
    a = random_string()
    b = random_string()
    item = {'p': p, 'a': a, 'b': b, 'd': random_string()}
    test_table_gsi_3.put_item(Item=item)
    assert_index_query(test_table_gsi_3, 'hello', [item],
        KeyConditions={'a': {'AttributeValueList': [a], 'ComparisonOperator': 'EQ'},
                       'b': {'AttributeValueList': [b], 'ComparisonOperator': 'EQ'}})
    # An update that doesn't change the GSI keys (a or b), just a regular
    # column d.
    item['d'] = random_string()
    test_table_gsi_3.update_item(Key={'p':  p}, AttributeUpdates={'d': {'Value': item['d'], 'Action': 'PUT'}})
    assert item == test_table_gsi_3.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    assert_index_query(test_table_gsi_3, 'hello', [item],
        KeyConditions={'a': {'AttributeValueList': [a], 'ComparisonOperator': 'EQ'},
                       'b': {'AttributeValueList': [b], 'ComparisonOperator': 'EQ'}})

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
    with new_test_table(dynamodb,
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
        ]) as table:
        items = [{'p': random_string(), 'x': random_string(), 'y': random_string()} for i in range(10)]
        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(item)
        wanted = ['p', 'x']
        expected_items = [{k: x[k] for k in wanted if k in x} for x in items]
        assert_index_scan(table, 'hello', expected_items)

# Test for "ProjectionType: INCLUDE". The secondary table includes the
# its own and the base's keys (as in KEYS_ONLY) plus the extra keys given
# in NonKeyAttributes.
@pytest.mark.xfail(reason="GSI projection not supported - issue #5036")
def test_gsi_projection_include(dynamodb):
    with new_test_table(dynamodb,
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
        ]) as table:
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

# Despite the name "NonKeyAttributes", key attributes *may* be listed.
# But they have no effect - because key attributes are always projected
# anyway.
@pytest.mark.xfail(reason="GSI projection not supported - issue #5036")
def test_gsi_projection_include_keyattributes(dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        AttributeDefinitions=[
            { 'AttributeName': 'p', 'AttributeType': 'S' },
            { 'AttributeName': 'x', 'AttributeType': 'S' } ],
        GlobalSecondaryIndexes=[
            {   'IndexName': 'hello',
                'KeySchema': [
                    { 'AttributeName': 'x', 'KeyType': 'HASH' },
                ],
                'Projection': { 'ProjectionType': 'INCLUDE',
                                'NonKeyAttributes': ['a', 'x'] }
            }
        ]) as table:
        p = random_string();
        x = random_string();
        a = random_string();
        b = random_string();
        table.put_item(Item={'p': p, 'x': x, 'a': a, 'b': b})
        # We expect both key attributes ('p' and 'x') to be projected, as
        # well as 'a' listed on NonKeyAttributes. The fact that 'x' was
        # also listed on NonKeyAttributes and 'p' wasn't doesn't make a
        # difference. The non-key 'b' wasn't listed, so it will not be
        # retrieved from the GSI.
        expected_items = [{'p': p, 'x': x, 'a': a}]
        assert_index_scan(table, 'hello', expected_items)

# In this test, we add two GSIs, one projecting the other GSI's key.
# This is an interesting case in Alternator's implementation, because
# GSI keys currently become actual Scylla columns - while regular attributes
# do not (they are elements of a single map column), so we need to remember
# to project both real columns and map elements into the view.
@pytest.mark.xfail(reason="GSI projection not supported - issue #5036")
def test_gsi_projection_include_otherkey(dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        AttributeDefinitions=[
            { 'AttributeName': 'p', 'AttributeType': 'S' },
            { 'AttributeName': 'x', 'AttributeType': 'S' },
            { 'AttributeName': 'a', 'AttributeType': 'S' },
            { 'AttributeName': 'z', 'AttributeType': 'S' } ],
        GlobalSecondaryIndexes=[
            {   'IndexName': 'indexx',
                'KeySchema': [
                    { 'AttributeName': 'x', 'KeyType': 'HASH' },
                ],
                'Projection': { 'ProjectionType': 'INCLUDE',
                                'NonKeyAttributes': ['a', 'b'] }
            },
            {   'IndexName': 'indexa',
                'KeySchema': [
                    { 'AttributeName': 'a', 'KeyType': 'HASH' },
                ],
                'Projection': { 'ProjectionType': 'KEYS_ONLY' }
            },
            {   'IndexName': 'indexz',
                'KeySchema': [
                    { 'AttributeName': 'z', 'KeyType': 'HASH' },
                ],
                'Projection': { 'ProjectionType': 'KEYS_ONLY' }
            }
        ]) as table:
        p = random_string();
        x = random_string();
        a = random_string();
        b = random_string();
        c = random_string();
        z = random_string();
        table.put_item(Item={'p': p, 'x': x, 'a': a, 'b': b, 'c': c, 'z': z})
        # When scanning indexx, we expect both its key attributes ('x')
        # and the base table's ('p') to be projected, as well as 'a' and 'b'
        # listed on NonKeyAttributes. 'c' isn't projected, and neither is 'z'
        # despite being some other GSI's key. Note that the projected 'a'
        # also happens to be some other GSI's key, while 'b' isn't, allowing
        # us to exercise both code paths (Alternator stores regular attributes
        # differently from attributes which are keys of some GSI).
        expected_items = [{'p': p, 'x': x, 'a': a, 'b': b}]
        assert_index_scan(table, 'indexx', expected_items)

# With ProjectionType=INCLUDE, NonKeyAttributes must not be missing:
@pytest.mark.xfail(reason="GSI projection not supported - issue #5036")
def test_gsi_projection_error_missing_nonkeyattributes(dynamodb):
    with pytest.raises(ClientError, match='ValidationException.*NonKeyAttributes'):
        with new_test_table(dynamodb,
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
                    'Projection': { 'ProjectionType': 'INCLUDE' }
                }
            ]) as table:
            pass

# With ProjectionType!=INCLUDE, NonKeyAttributes must not be present:
@pytest.mark.xfail(reason="GSI projection not supported - issue #5036")
def test_gsi_projection_error_superflous_nonkeyattributes(dynamodb):
    with pytest.raises(ClientError, match='ValidationException.*NonKeyAttributes'):
        with new_test_table(dynamodb,
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
                    'Projection': { 'ProjectionType': 'ALL',
                                    'NonKeyAttributes': ['a'] }
                }
            ]) as table:
            pass

# Duplicate attribute names in NonKeyAttributes of INCLUDE are not allowed:
@pytest.mark.xfail(reason="GSI projection not supported - issue #5036")
def test_gsi_projection_error_duplicate(dynamodb):
    with pytest.raises(ClientError, match='ValidationException.*Duplicate'):
        with new_test_table(dynamodb,
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
                                    'NonKeyAttributes': ['a', 'a'] }
                }
            ]) as table:
            pass

# NonKeyAttributes must be a list of strings. Non-strings in this list
# result, for some reason, in SerializationException instead of the more
# usual ValidationException.
@pytest.mark.xfail(reason="GSI projection not supported - issue #5036")
def test_gsi_projection_error_nonstring_nonkeyattributes(dynamodb):
    with pytest.raises(ClientError, match='SerializationException'):
        with new_test_table(dynamodb,
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
                                    'NonKeyAttributes': ['a', 123] }
                }
            ]) as table:
            pass

# An unsupported ProjectionType value should result in an error:
@pytest.mark.xfail(reason="GSI projection not supported - issue #5036")
def test_gsi_bad_projection_type(dynamodb):
    with pytest.raises(ClientError, match='ValidationException.*nonsense'):
        with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
            GlobalSecondaryIndexes=[
                {   'IndexName': 'hello',
                    'KeySchema': [{ 'AttributeName': 'p', 'KeyType': 'HASH' }],
                    'Projection': { 'ProjectionType': 'nonsense' }
                }
            ]) as table:
            pass

# DynamoDB's says the "Projection" argument of GlobalSecondaryIndexes is
# mandatory, and indeed Boto3 enforces that it must be passed. The
# documentation then goes on to claim that the "ProjectionType" member of
# "Projection" is optional - and Boto3 allows it to be missing. But in
# fact, it is not allowed to be missing: DynamoDB complains: "Unknown
# ProjectionType: null".
@pytest.mark.xfail(reason="GSI projection not supported - issue #5036")
def test_gsi_missing_projection_type(dynamodb):
    with pytest.raises(ClientError, match='ValidationException.*ProjectionType'):
        with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
            GlobalSecondaryIndexes=[
                {   'IndexName': 'hello',
                    'KeySchema': [{ 'AttributeName': 'p', 'KeyType': 'HASH' }],
                    'Projection': {}
                }
            ]) as table:
            pass

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
    for i in range(600):
        time.sleep(1)
        desc = table.meta.client.describe_table(TableName=table.name)
        table_status = desc['Table']['TableStatus']
        if table_status != 'ACTIVE':
            print(f'{i} Table {table.name} status still {table_status}')
            continue
        index_desc = [x for x in desc['Table']['GlobalSecondaryIndexes'] if x['IndexName'] == gsi_name]
        assert len(index_desc) == 1
        index_status = index_desc[0]['IndexStatus']
        if index_status != 'ACTIVE':
            print(f'{i} Index {gsi_name} status still {index_status}')
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
    for i in range(600):
        time.sleep(1)
        desc = table.meta.client.describe_table(TableName=table.name)
        table_status = desc['Table']['TableStatus']
        if table_status != 'ACTIVE':
            print(f'{i} Table {table.name} status still {table_status}')
            continue
        if 'GlobalSecondaryIndexes' in desc['Table']:
            index_desc = [x for x in desc['Table']['GlobalSecondaryIndexes'] if x['IndexName'] == gsi_name]
            if len(index_desc) != 0:
                index_status = index_desc[0]['IndexStatus']
                print(f'{i} Index {gsi_name} status still {index_status}')
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
# Reproduces issue #5022.
@pytest.mark.xfail(reason="issue #5022")
def test_gsi_backfill(dynamodb):
    # First create, and fill, a table without GSI. The items in items1
    # will have the appropriate string type for 'x' and will later get
    # indexed. Items in item2 have no value for 'x', and in item3 'x' is in
    # not a string; So the items in items2 and items3 will be missing
    # in the index we'll create later.
    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' } ]) as table:
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

# Test deleting an existing GSI using UpdateTable
# Reproduces issue #5022.
@pytest.mark.xfail(reason="issue #5022")
def test_gsi_delete(dynamodb):
    with new_test_table(dynamodb,
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
        ]) as table:
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
    with pytest.raises(ClientError, match='ValidationException.*3'):
        create_gsi(dynamodb, 'n')
    with pytest.raises(ClientError, match='ValidationException.*3'):
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

# As noted above in test_gsi_empty_value(), setting an indexed string column
# to an empty string is rejected, since keys (including GSI keys) are not
# allowed to be empty strings or binary blobs.
# However, empty strings *are* legal for ordinary non-indexed attributes, so
# if the user adds a GSI to an existing table with pre-existing data, it might
# contain empty string values for the indexed keys. Such values should be
# skipped while filling the GSI - even if Scylla actually capable of
# representing such empty view keys (see issue #9375).
# Reproduces issue #5022 and #9424.
@pytest.mark.xfail(reason="issue #11567, #9424")
def test_gsi_backfill_empty_string(dynamodb):
    # First create, and fill, a table without GSI:
    with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' },
                        { 'AttributeName': 'c', 'KeyType': 'RANGE' } ],
            AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' },
                                   { 'AttributeName': 'c', 'AttributeType': 'S' } ]) as table:
        p1 = random_string()
        p2 = random_string()
        c = random_string()
        # Create two items, one has an empty "x" attribute, the other is
        # non-empty.
        table.put_item(Item={'p': p1, 'c': c, 'x': 'hello'})
        table.put_item(Item={'p': p2, 'c': c, 'x': ''})
        # Now use UpdateTable to create two GSIs. In one of them "x" will be
        # the partition key, and in the other "x" will be a sort key.
        # DynamoDB limits the number of indexes that can be added in one
        # UpdateTable command to just one, so we need to do it in two separate
        # commands and wait for each to complete.
        dynamodb.meta.client.update_table(TableName=table.name,
            AttributeDefinitions=[{ 'AttributeName': 'x', 'AttributeType': 'S' },
                                  { 'AttributeName': 'c', 'AttributeType': 'S' }],
            GlobalSecondaryIndexUpdates=[
                { 'Create': { 'IndexName': 'index1',
                              'KeySchema': [{ 'AttributeName': 'x', 'KeyType': 'HASH' }],
                              'Projection': { 'ProjectionType': 'ALL' }}
                }
            ])
        wait_for_gsi(table, 'index1')
        dynamodb.meta.client.update_table(TableName=table.name,
            AttributeDefinitions=[{ 'AttributeName': 'x', 'AttributeType': 'S' },
                                  { 'AttributeName': 'c', 'AttributeType': 'S' }],
            GlobalSecondaryIndexUpdates=[
                { 'Create': { 'IndexName': 'index2',
                              'KeySchema': [{ 'AttributeName': 'c', 'KeyType': 'HASH' },
                                            { 'AttributeName': 'x', 'KeyType': 'RANGE' }],
                              'Projection': { 'ProjectionType': 'ALL' }}
                }
            ])
        wait_for_gsi(table, 'index2')
        # Verify that the items with the empty-string x are missing from both
        # GSIs, so only the one item with x != '' should appear in both.
        # Note that we don't need to retry the reads here (i.e., use the
        # assert_index_scan() or assert_index_query() functions) because after
        # we waited for backfilling to complete, we know all the pre-existing
        # data is already in the index.
        assert [{'p': p1, 'c': c, 'x': 'hello'}] == full_scan(table, ConsistentRead=False, IndexName='index1')
        assert [{'p': p1, 'c': c, 'x': 'hello'}] == full_scan(table, ConsistentRead=False, IndexName='index2')

# Test the "Select" parameter of a Query on a GSI. We have in test_query.py
# a test 'test_query_select' for this parameter on a query of a normal (base)
# table, but for GSI and LSI the ALL_PROJECTED_ATTRIBUTES is additionally
# allowed, and we want to test it. Moreover, in a GSI, when only a subset of
# the attributes were projected into the GSI, it is impossible to request
# that Select return other attributes.
# We split the test into two, the first just requiring proper implementation
# of Select, and the second requiring also a proper implementation of
# projection of just a subset of the attributes.
def test_gsi_query_select_1(test_table_gsi_1):
    items = [{'p': random_string(), 'c': random_string(), 'x': random_string(), 'y': random_string()} for i in range(10)]
    with test_table_gsi_1.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    c = items[0]['c']
    expected_items = [x for x in items if x['c'] == c]
    assert_index_query(test_table_gsi_1, 'hello', expected_items,
        KeyConditions={'c': {'AttributeValueList': [c], 'ComparisonOperator': 'EQ'}})
    # Unlike in base tables, here Select=ALL_PROJECTED_ATTRIBUTES is
    # allowed, and in this case (all attributes are projected into this
    # index) returns all attributes.
    assert_index_query(test_table_gsi_1, 'hello', expected_items,
        Select='ALL_PROJECTED_ATTRIBUTES',
        KeyConditions={'c': {'AttributeValueList': [c], 'ComparisonOperator': 'EQ'}})
    # Because in this GSI all attributes are projected into the index,
    # ALL_ATTRIBUTES is allowed as well. And so is SPECIFIC_ATTRIBUTES
    # (with AttributesToGet / ProjectionExpression) for any attributes,
    # and of course so is COUNT.
    assert_index_query(test_table_gsi_1, 'hello', expected_items,
        Select='ALL_ATTRIBUTES',
        KeyConditions={'c': {'AttributeValueList': [c], 'ComparisonOperator': 'EQ'}})
    expected_items = [{'y': x['y']} for x in items if x['c'] == c]
    assert_index_query(test_table_gsi_1, 'hello', expected_items,
        Select='SPECIFIC_ATTRIBUTES',
        AttributesToGet=['y'],
        KeyConditions={'c': {'AttributeValueList': [c], 'ComparisonOperator': 'EQ'}})
    assert not 'Items' in test_table_gsi_1.query(ConsistentRead=False,
        IndexName='hello',
        Select='COUNT',
        KeyConditions={'c': {'AttributeValueList': [c], 'ComparisonOperator': 'EQ'}})

@pytest.mark.xfail(reason="Projection not supported yet. Issue #5036")
def test_gsi_query_select_2(dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' } ],
        AttributeDefinitions=[
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'x', 'AttributeType': 'S' },
        ],
        GlobalSecondaryIndexes=[
            {   'IndexName': 'hello',
                'KeySchema': [ { 'AttributeName': 'x', 'KeyType': 'HASH' } ],
                'Projection': { 'ProjectionType': 'INCLUDE',
                                'NonKeyAttributes': ['a'] }
            }
        ]) as table:
        items = [{'p': random_string(), 'x': random_string(), 'a': random_string(), 'b': random_string()} for i in range(10)]
        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(item)
        x = items[0]['x']
        # Unlike in base tables, here Select=ALL_PROJECTED_ATTRIBUTES is
        # allowed, and only the projected attributes are returned (in this
        # case the key of both base and GSI ('p' and 'x') and 'a' - but not
        # 'b'. Moreover, it is the default if Select isn't specified at all.
        expected_items = [{'p': z['p'], 'x': z['x'], 'a': z['a']} for z in items if z['x'] == x]
        assert_index_query(table, 'hello', expected_items,
            KeyConditions={'x': {'AttributeValueList': [x], 'ComparisonOperator': 'EQ'}})
        assert_index_query(table, 'hello', expected_items,
            Select='ALL_PROJECTED_ATTRIBUTES',
            KeyConditions={'x': {'AttributeValueList': [x], 'ComparisonOperator': 'EQ'}})
        # Because in this GSI not all attributes are projected into the index,
        # Select=ALL_ATTRIBUTES is *not* allowed.
        with pytest.raises(ClientError, match='ValidationException.*ALL_ATTRIBUTES'):
            assert_index_query(table, 'hello', expected_items,
                Select='ALL_ATTRIBUTES',
                KeyConditions={'x': {'AttributeValueList': [x], 'ComparisonOperator': 'EQ'}})
        # SPECIFIC_ATTRIBUTES (with AttributesToGet / ProjectionExpression)
        # is allowed for the projected attributes, but not for unprojected
        # attributes.
        expected_items = [{'a': z['a']} for z in items if z['x'] == x]
        assert_index_query(table, 'hello', expected_items,
            Select='SPECIFIC_ATTRIBUTES',
            AttributesToGet=['a'],
            KeyConditions={'x': {'AttributeValueList': [x], 'ComparisonOperator': 'EQ'}})
        # Select=COUNT is also allowed, and doesn't return item content
        assert not 'Items' in table.query(ConsistentRead=False,
            IndexName='hello',
            Select='COUNT',
            KeyConditions={'x': {'AttributeValueList': [x], 'ComparisonOperator': 'EQ'}})

# Test that trying to create a table with two GSIs with the same name is an
# error.
# In test_gsi_backfill we also check that adding a second GSI with the same
# name later, after the already exists table exists, is also an error.
# See also test_lsi.py::test_lsi_and_gsi_same_same which shows even GSIs
# and LSIs may not have the same name (and explains why)
def test_gsi_same_name_forbidden(dynamodb):
    with pytest.raises(ClientError, match='ValidationException.*Duplicate.*index1'):
        with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[
                { 'AttributeName': 'p', 'AttributeType': 'S' },
                { 'AttributeName': 'x', 'AttributeType': 'S' },
                { 'AttributeName': 'y', 'AttributeType': 'S' }
            ],
            GlobalSecondaryIndexes=[
                {   'IndexName': 'index1',
                    'KeySchema': [{ 'AttributeName': 'x', 'KeyType': 'HASH' }],
                    'Projection': { 'ProjectionType': 'ALL' }
                },
                {   'IndexName': 'index1', # different index, samed name!
                    'KeySchema': [{ 'AttributeName': 'y', 'KeyType': 'HASH' }],
                    'Projection': { 'ProjectionType': 'ALL' }
                }
            ]) as table:
            pass
    # Even if the two indexes are identical twins - having the same definition
    # exactly - it's not allowed.
    with pytest.raises(ClientError, match='ValidationException.*Duplicate.*index1'):
        with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[
                { 'AttributeName': 'p', 'AttributeType': 'S' },
                { 'AttributeName': 'x', 'AttributeType': 'S' },
            ],
            GlobalSecondaryIndexes=[
                {   'IndexName': 'index1',
                    'KeySchema': [{ 'AttributeName': 'x', 'KeyType': 'HASH' }],
                    'Projection': { 'ProjectionType': 'ALL' }
                },
                {   'IndexName': 'index1',
                    'KeySchema': [{ 'AttributeName': 'x', 'KeyType': 'HASH' }],
                    'Projection': { 'ProjectionType': 'ALL' }
                }
            ]) as table:
            pass

# Trying to create two differently-named GSIs in the same table indexing
# the same attribute with exactly the same parameters is redundant, but
# allowed, and works (and not recommended because it is very wasteful...)
def test_gsi_duplicate_with_different_name(dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[
            { 'AttributeName': 'p', 'AttributeType': 'S' },
            { 'AttributeName': 'x', 'AttributeType': 'S' },
        ],
        GlobalSecondaryIndexes=[
            {   'IndexName': 'index1',
                'KeySchema': [{ 'AttributeName': 'x', 'KeyType': 'HASH' }],
                'Projection': { 'ProjectionType': 'ALL' }
            },
            {   'IndexName': 'index2', # same index, different name
                'KeySchema': [{ 'AttributeName': 'x', 'KeyType': 'HASH' }],
                'Projection': { 'ProjectionType': 'ALL' }
            }
        ]) as table:
        pass

# But, trying to create two different GSIs with different type for the same
# key is NOT allowed. The reason is that DynamoDB wants to insist that future
# writes to this attribute must have the declared type - and can't insist on
# two different types at the same time.
# We have two versions of this test: One when the conflict happens during
# the table creation, and one where the second GSI is added after the table
# already exists with the first GSI.
# Reproduces #13870 (see also test_create_table_duplicate_attribute_name in
# test_table.py).
def test_gsi_key_type_conflict_on_create(dynamodb):
    with pytest.raises(ClientError, match='ValidationException.*uplicate.*xyz'):
        with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
            # Note how because how this API works, having two different types
            # for the same attribute 'xyz' looks very strange (there is not even
            # a way to say which definition applies to which index) so
            # unsurprisingly it's not accepted.
            AttributeDefinitions=[
                { 'AttributeName': 'p', 'AttributeType': 'S' },
                { 'AttributeName': 'xyz', 'AttributeType': 'S' },
                { 'AttributeName': 'xyz', 'AttributeType': 'N' },
            ],
            GlobalSecondaryIndexes=[
                {   'IndexName': 'index1',
                    'KeySchema': [{ 'AttributeName': 'xyz', 'KeyType': 'HASH' }],
                    'Projection': { 'ProjectionType': 'ALL' }
            },
                {   'IndexName': 'index2',
                    'KeySchema': [{ 'AttributeName': 'xyz', 'KeyType': 'HASH' }],
                    'Projection': { 'ProjectionType': 'ALL' }
                }
            ]) as table:
            pass

@pytest.mark.xfail(reason="issue #11567")
def test_gsi_key_type_conflict_on_update(dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }],
        AttributeDefinitions=[
            { 'AttributeName': 'p', 'AttributeType': 'S' },
            { 'AttributeName': 'xyz', 'AttributeType': 'S' },
        ],
        GlobalSecondaryIndexes=[
            {   'IndexName': 'index1',
                'KeySchema': [{ 'AttributeName': 'xyz', 'KeyType': 'HASH' }],
                'Projection': { 'ProjectionType': 'ALL' }
            }
        ]) as table:
        # Now use UpdateTable to create a second GSI, with a different
        # type for attribute xyz. DynamoDB gives a lengthy error message:
        #   "One or more parameter values were invalid: Attributes cannot be
        #    redefined. Please check that your attribute has the same type as
        #    previously defined.
        #    Existing schema: Schema:[SchemaElement: key{xyz:S:HASH}]
        #    New schema: Schema:[SchemaElement: key{xyz:N:HASH}]"
        with pytest.raises(ClientError, match='ValidationException.*redefined'):
            dynamodb.meta.client.update_table(TableName=table.name,
                AttributeDefinitions=[{ 'AttributeName': 'xyz', 'AttributeType': 'N' }],
                GlobalSecondaryIndexUpdates=[ {  'Create':
                    {  'IndexName': 'index2',
                        'KeySchema': [{ 'AttributeName': 'xyz', 'KeyType': 'HASH' }],
                        'Projection': { 'ProjectionType': 'ALL' }
                    }}])

# Test similar to test_11801 and test_11801_variant2, but this test first
# updates the range key b to a new value (like variant2) and then sets it
# back to its original value. It reproduces issue #17119 - the last
# modification was lost because the wrong timestamp was used.
# The bug is specific to the case that the GSI has two non-key columns
# as its keys, so we test it on test_table_gsi_3 which has this feature.
def test_17119(test_table_gsi_3):
    p = random_string()
    a = random_string()
    b = random_string()
    item = {'p': p, 'a': a, 'b': b, 'd': random_string()}
    test_table_gsi_3.put_item(Item=item)
    assert_index_query(test_table_gsi_3, 'hello', [item],
        KeyConditions={'a': {'AttributeValueList': [a], 'ComparisonOperator': 'EQ'},
                       'b': {'AttributeValueList': [b], 'ComparisonOperator': 'EQ'}})
    # Change the GSI range key b to a different value newb.
    newb = random_string()
    test_table_gsi_3.update_item(Key={'p':  p}, AttributeUpdates={'b': {'Value': newb, 'Action': 'PUT'}})
    item['b'] = newb
    assert item == test_table_gsi_3.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    # The item newb should appear in the GSI, item b should be gone:
    assert_index_query(test_table_gsi_3, 'hello', [item],
        KeyConditions={'a': {'AttributeValueList': [a], 'ComparisonOperator': 'EQ'},
                       'b': {'AttributeValueList': [newb], 'ComparisonOperator': 'EQ'}})
    assert_index_query(test_table_gsi_3, 'hello', [],
        KeyConditions={'a': {'AttributeValueList': [a], 'ComparisonOperator': 'EQ'},
                       'b': {'AttributeValueList': [b], 'ComparisonOperator': 'EQ'}})
    # Change the GSI range key b back to its original value. Item newb
    # should disappear from the GSI, and item b should reappear:
    test_table_gsi_3.update_item(Key={'p':  p}, AttributeUpdates={'b': {'Value': b, 'Action': 'PUT'}})
    item['b'] = b
    assert item == test_table_gsi_3.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    assert_index_query(test_table_gsi_3, 'hello', [],
        KeyConditions={'a': {'AttributeValueList': [a], 'ComparisonOperator': 'EQ'},
                       'b': {'AttributeValueList': [newb], 'ComparisonOperator': 'EQ'}})
    # This assertion failed in issue #17119:
    assert_index_query(test_table_gsi_3, 'hello', [item],
        KeyConditions={'a': {'AttributeValueList': [a], 'ComparisonOperator': 'EQ'},
                       'b': {'AttributeValueList': [b], 'ComparisonOperator': 'EQ'}})

# This test is like test_17119 above, just in a table with just one new
# key column in the GSI. The bug of #17119 doesn't reproduce here, showing
# the problem was specific to the case of two new GSI key columns.
def test_17119a(test_table_gsi_2):
    p = random_string()
    x = random_string()
    item = {'p': p, 'x': x, 'z': random_string()}
    test_table_gsi_2.put_item(Item=item)
    assert_index_query(test_table_gsi_2, 'hello', [item],
        KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
                       'x': {'AttributeValueList': [x], 'ComparisonOperator': 'EQ'}})
    # Change the GSI range key x to a different value.
    newx = random_string()
    test_table_gsi_2.update_item(Key={'p':  p}, AttributeUpdates={'x': {'Value': newx, 'Action': 'PUT'}})
    item['x'] = newx
    assert item == test_table_gsi_2.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    # The item newx should appear in the GSI, item x should be gone:
    assert_index_query(test_table_gsi_2, 'hello', [item],
        KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
                       'x': {'AttributeValueList': [newx], 'ComparisonOperator': 'EQ'}})
    assert_index_query(test_table_gsi_2, 'hello', [],
        KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
                       'x': {'AttributeValueList': [x], 'ComparisonOperator': 'EQ'}})
    # Change the GSI range key x back to its original value. Item newx
    # should disappear from the GSI, and item x should reappear:
    test_table_gsi_2.update_item(Key={'p':  p}, AttributeUpdates={'x': {'Value': x, 'Action': 'PUT'}})
    item['x'] = x
    assert item == test_table_gsi_2.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    assert_index_query(test_table_gsi_2, 'hello', [],
        KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
                       'x': {'AttributeValueList': [newx], 'ComparisonOperator': 'EQ'}})
    assert_index_query(test_table_gsi_2, 'hello', [item],
        KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'},
                       'x': {'AttributeValueList': [x], 'ComparisonOperator': 'EQ'}})
