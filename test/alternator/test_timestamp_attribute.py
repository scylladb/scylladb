# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# Tests for the system:timestamp_attribute Scylla-specific feature.
# This feature allows users to control the write timestamp of PutItem and
# UpdateItem operations by specifying an attribute name in the table's
# system:timestamp_attribute tag. When that attribute is present in the
# write request with a numeric value (microseconds since Unix epoch), it
# is used as the write timestamp. The attribute itself is not stored in
# the item data.
#
# This is a Scylla-specific feature and is not tested on DynamoDB.

import pytest
from botocore.exceptions import ClientError
from decimal import Decimal

from test.pylib.runner import testpy_test_fixture_scope

from .util import create_test_table, random_string

# A large timestamp in microseconds (far future, year ~2033)
LARGE_TS = Decimal('2000000000000000')
# A medium timestamp in microseconds (year ~2001)
MEDIUM_TS = Decimal('1000000000000000')
# A small timestamp in microseconds (year ~1970+)
SMALL_TS = Decimal('100000000000000')

# Fixtures for tables with the system:timestamp_attribute tag. The tables
# are created once per module and shared between all tests that use them,
# to avoid the overhead of creating and deleting tables for each test.

# A table with only a hash key and system:timestamp_attribute='ts' tag.
@pytest.fixture(scope=testpy_test_fixture_scope)
def test_table_ts(scylla_only, dynamodb):
    table = create_test_table(dynamodb,
        Tags=[{'Key': 'system:timestamp_attribute', 'Value': 'ts'}],
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}])
    yield table
    table.delete()

# A table with hash and range keys and system:timestamp_attribute='ts' tag.
@pytest.fixture(scope=testpy_test_fixture_scope)
def test_table_ts_sc(scylla_only, dynamodb):
    table = create_test_table(dynamodb,
        Tags=[{'Key': 'system:timestamp_attribute', 'Value': 'ts'}],
        KeySchema=[
            {'AttributeName': 'p', 'KeyType': 'HASH'},
            {'AttributeName': 'c', 'KeyType': 'RANGE'},
        ],
        AttributeDefinitions=[
            {'AttributeName': 'p', 'AttributeType': 'S'},
            {'AttributeName': 'c', 'AttributeType': 'S'},
        ])
    yield table
    table.delete()

# A table with only a hash key, system:timestamp_attribute='ts' tag, and
# system:write_isolation='only_rmw_uses_lwt' for testing LWT rejection.
@pytest.fixture(scope=testpy_test_fixture_scope)
def test_table_ts_lwt(scylla_only, dynamodb):
    table = create_test_table(dynamodb,
        Tags=[{'Key': 'system:timestamp_attribute', 'Value': 'ts'},
              {'Key': 'system:write_isolation', 'Value': 'only_rmw_uses_lwt'}],
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}])
    yield table
    table.delete()

# Test that PutItem with the timestamp attribute uses the given numeric
# value as the write timestamp, and the timestamp attribute is NOT stored
# in the item.
def test_timestamp_attribute_put_item_basic(test_table_ts):
    p = random_string()
    # Put an item with the timestamp attribute
    test_table_ts.put_item(Item={'p': p, 'val': 'hello', 'ts': LARGE_TS})
    # Read the item back
    item = test_table_ts.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    # 'val' should be stored normally
    assert item['val'] == 'hello'
    # 'ts' (the timestamp attribute) should NOT be stored in the item
    assert 'ts' not in item

# Test that PutItem respects the write timestamp ordering: a write with a
# larger timestamp should win over a write with a smaller timestamp,
# regardless of wall-clock order.
def test_timestamp_attribute_put_item_ordering(test_table_ts):
    p = random_string()
    # First, write item with a LARGE timestamp
    test_table_ts.put_item(Item={'p': p, 'val': 'large_ts', 'ts': LARGE_TS})
    # Then write item with a SMALL timestamp (should lose since SMALL < LARGE)
    test_table_ts.put_item(Item={'p': p, 'val': 'small_ts', 'ts': SMALL_TS})
    # The item with the larger timestamp (val='large_ts') should win
    item = test_table_ts.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    assert item['val'] == 'large_ts'

    # Now try to overwrite with a LARGER timestamp (should win)
    test_table_ts.put_item(Item={'p': p, 'val': 'latest', 'ts': LARGE_TS + 1})
    item = test_table_ts.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    assert item['val'] == 'latest'

# Test that UpdateItem with the timestamp attribute in AttributeUpdates
# uses the given numeric value as the write timestamp, and the timestamp
# attribute is NOT stored in the item.
def test_timestamp_attribute_update_item_attribute_updates(test_table_ts):
    p = random_string()
    # Use UpdateItem with AttributeUpdates, setting 'val' and 'ts'
    test_table_ts.update_item(
        Key={'p': p},
        AttributeUpdates={
            'val': {'Value': 'hello', 'Action': 'PUT'},
            'ts': {'Value': LARGE_TS, 'Action': 'PUT'},
        }
    )
    item = test_table_ts.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    assert item['val'] == 'hello'
    # 'ts' should NOT be stored in the item
    assert 'ts' not in item

    # Update with a smaller timestamp - should NOT overwrite
    test_table_ts.update_item(
        Key={'p': p},
        AttributeUpdates={
            'val': {'Value': 'overwritten', 'Action': 'PUT'},
            'ts': {'Value': SMALL_TS, 'Action': 'PUT'},
        }
    )
    item = test_table_ts.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    # The item with the larger timestamp should still win
    assert item['val'] == 'hello'

# Test that UpdateItem with the timestamp attribute in UpdateExpression
# uses the given numeric value as the write timestamp, and the timestamp
# attribute is NOT stored in the item.
def test_timestamp_attribute_update_item_update_expression(test_table_ts):
    p = random_string()
    # Use UpdateItem with UpdateExpression to set 'val' and 'ts'
    test_table_ts.update_item(
        Key={'p': p},
        UpdateExpression='SET val = :v, ts = :t',
        ExpressionAttributeValues={':v': 'hello', ':t': LARGE_TS}
    )
    item = test_table_ts.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    assert item['val'] == 'hello'
    # 'ts' should NOT be stored in the item
    assert 'ts' not in item

    # Update with a smaller timestamp - should NOT overwrite
    test_table_ts.update_item(
        Key={'p': p},
        UpdateExpression='SET val = :v, ts = :t',
        ExpressionAttributeValues={':v': 'overwritten', ':t': SMALL_TS}
    )
    item = test_table_ts.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    # The item with the larger timestamp should still win
    assert item['val'] == 'hello'

# Test that when the timestamp attribute is not present in the write request,
# the operation behaves normally (no custom timestamp is applied).
def test_timestamp_attribute_absent(test_table_ts):
    p = random_string()
    # Put item without the timestamp attribute
    test_table_ts.put_item(Item={'p': p, 'val': 'hello'})
    item = test_table_ts.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    assert item['val'] == 'hello'
    # No 'ts' attribute expected either
    assert 'ts' not in item

# Test that using a condition expression (which requires LWT) together with
# the timestamp attribute is rejected.
def test_timestamp_attribute_with_condition_rejected(test_table_ts_lwt):
    p = random_string()
    # Put an initial item (no timestamp attribute, so LWT is ok)
    test_table_ts_lwt.put_item(Item={'p': p, 'val': 'initial'})
    # Try to put with a ConditionExpression and a timestamp - should be rejected
    with pytest.raises(ClientError, match='ValidationException'):
        test_table_ts_lwt.put_item(
            Item={'p': p, 'val': 'updated', 'ts': LARGE_TS},
            ConditionExpression='attribute_exists(p)'
        )

# Test that when the timestamp attribute has a non-numeric value, it is
# treated as if the timestamp attribute is absent (use current timestamp),
# and the non-numeric attribute IS stored in the item normally.
def test_timestamp_attribute_non_numeric(test_table_ts):
    p = random_string()
    # Put item with the timestamp attribute as a string (non-numeric)
    test_table_ts.put_item(Item={'p': p, 'val': 'hello', 'ts': 'not_a_number'})
    item = test_table_ts.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    assert item['val'] == 'hello'
    # When the timestamp attribute has a non-numeric value, it should
    # be stored normally (not used as a timestamp and not omitted)
    assert item['ts'] == 'not_a_number'

# Test that the timestamp attribute tag can be set on a table with a sort key.
def test_timestamp_attribute_with_range_key(test_table_ts_sc):
    p = random_string()
    c = random_string()
    # Write with a large timestamp
    test_table_ts_sc.put_item(Item={'p': p, 'c': c, 'val': 'large', 'ts': LARGE_TS})
    # Write with a small timestamp (should lose)
    test_table_ts_sc.put_item(Item={'p': p, 'c': c, 'val': 'small', 'ts': SMALL_TS})
    item = test_table_ts_sc.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)['Item']
    assert item['val'] == 'large'
    assert 'ts' not in item

# Test that BatchWriteItem also respects the timestamp attribute.
def test_timestamp_attribute_batch_write(test_table_ts):
    p = random_string()
    # Write item via BatchWriteItem with a large timestamp
    with test_table_ts.batch_writer() as batch:
        batch.put_item(Item={'p': p, 'val': 'large_ts', 'ts': LARGE_TS})
    # Write item via BatchWriteItem with a small timestamp (should lose)
    with test_table_ts.batch_writer() as batch:
        batch.put_item(Item={'p': p, 'val': 'small_ts', 'ts': SMALL_TS})
    item = test_table_ts.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    assert item['val'] == 'large_ts'
    assert 'ts' not in item
