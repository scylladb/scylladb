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

from .util import new_test_table, random_string

# A large timestamp in microseconds (far future, year ~2033)
LARGE_TS = Decimal('2000000000000000')
# A medium timestamp in microseconds (year ~2001)
MEDIUM_TS = Decimal('1000000000000000')
# A small timestamp in microseconds (year ~1970+)
SMALL_TS = Decimal('100000000000000')

# Test that PutItem with the timestamp attribute uses the given numeric
# value as the write timestamp, and the timestamp attribute is NOT stored
# in the item.
def test_timestamp_attribute_put_item_basic(scylla_only, dynamodb):
    """
    Test that system:timestamp_attribute causes PutItem to use a custom write
    timestamp. The attribute itself is not stored in the item.
    """
    with new_test_table(dynamodb,
        Tags=[{'Key': 'system:timestamp_attribute', 'Value': 'ts'}],
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        p = random_string()
        # Put an item with the timestamp attribute
        table.put_item(Item={'p': p, 'val': 'hello', 'ts': LARGE_TS})
        # Read the item back
        item = table.get_item(Key={'p': p}, ConsistentRead=True)['Item']
        # 'val' should be stored normally
        assert item['val'] == 'hello'
        # 'ts' (the timestamp attribute) should NOT be stored in the item
        assert 'ts' not in item

# Test that PutItem respects the write timestamp ordering: a write with a
# larger timestamp should win over a write with a smaller timestamp,
# regardless of wall-clock order.
def test_timestamp_attribute_put_item_ordering(scylla_only, dynamodb):
    """
    Test that when two PutItem operations write to the same key with different
    custom timestamps, the one with the larger timestamp wins (last-write-wins).
    """
    with new_test_table(dynamodb,
        Tags=[{'Key': 'system:timestamp_attribute', 'Value': 'ts'}],
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        p = random_string()
        # First, write item with a LARGE timestamp
        table.put_item(Item={'p': p, 'val': 'large_ts', 'ts': LARGE_TS})
        # Then write item with a SMALL timestamp (should lose since SMALL < LARGE)
        table.put_item(Item={'p': p, 'val': 'small_ts', 'ts': SMALL_TS})
        # The item with the larger timestamp (val='large_ts') should win
        item = table.get_item(Key={'p': p}, ConsistentRead=True)['Item']
        assert item['val'] == 'large_ts'

        # Now try to overwrite with a LARGER timestamp (should win)
        table.put_item(Item={'p': p, 'val': 'latest', 'ts': LARGE_TS + 1})
        item = table.get_item(Key={'p': p}, ConsistentRead=True)['Item']
        assert item['val'] == 'latest'

# Test that UpdateItem with the timestamp attribute in AttributeUpdates
# uses the given numeric value as the write timestamp, and the timestamp
# attribute is NOT stored in the item.
def test_timestamp_attribute_update_item_attribute_updates(scylla_only, dynamodb):
    """
    Test that system:timestamp_attribute works with UpdateItem using
    AttributeUpdates. The timestamp attribute is not stored in the item.
    """
    with new_test_table(dynamodb,
        Tags=[{'Key': 'system:timestamp_attribute', 'Value': 'ts'}],
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        p = random_string()
        # Use UpdateItem with AttributeUpdates, setting 'val' and 'ts'
        table.update_item(
            Key={'p': p},
            AttributeUpdates={
                'val': {'Value': 'hello', 'Action': 'PUT'},
                'ts': {'Value': LARGE_TS, 'Action': 'PUT'},
            }
        )
        item = table.get_item(Key={'p': p}, ConsistentRead=True)['Item']
        assert item['val'] == 'hello'
        # 'ts' should NOT be stored in the item
        assert 'ts' not in item

        # Update with a smaller timestamp - should NOT overwrite
        table.update_item(
            Key={'p': p},
            AttributeUpdates={
                'val': {'Value': 'overwritten', 'Action': 'PUT'},
                'ts': {'Value': SMALL_TS, 'Action': 'PUT'},
            }
        )
        item = table.get_item(Key={'p': p}, ConsistentRead=True)['Item']
        # The item with the larger timestamp should still win
        assert item['val'] == 'hello'

# Test that UpdateItem with the timestamp attribute in UpdateExpression
# uses the given numeric value as the write timestamp, and the timestamp
# attribute is NOT stored in the item.
def test_timestamp_attribute_update_item_update_expression(scylla_only, dynamodb):
    """
    Test that system:timestamp_attribute works with UpdateItem using
    UpdateExpression. The timestamp attribute is not stored in the item.
    """
    with new_test_table(dynamodb,
        Tags=[{'Key': 'system:timestamp_attribute', 'Value': 'ts'}],
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        p = random_string()
        # Use UpdateItem with UpdateExpression to set 'val' and 'ts'
        table.update_item(
            Key={'p': p},
            UpdateExpression='SET val = :v, ts = :t',
            ExpressionAttributeValues={':v': 'hello', ':t': LARGE_TS}
        )
        item = table.get_item(Key={'p': p}, ConsistentRead=True)['Item']
        assert item['val'] == 'hello'
        # 'ts' should NOT be stored in the item
        assert 'ts' not in item

        # Update with a smaller timestamp - should NOT overwrite
        table.update_item(
            Key={'p': p},
            UpdateExpression='SET val = :v, ts = :t',
            ExpressionAttributeValues={':v': 'overwritten', ':t': SMALL_TS}
        )
        item = table.get_item(Key={'p': p}, ConsistentRead=True)['Item']
        # The item with the larger timestamp should still win
        assert item['val'] == 'hello'

# Test that when the timestamp attribute is not present in the write request,
# the operation behaves normally (no custom timestamp is applied).
def test_timestamp_attribute_absent(scylla_only, dynamodb):
    """
    Test that when the timestamp attribute tag is set on the table but the
    attribute is not included in the PutItem request, the write proceeds
    normally without a custom timestamp.
    """
    with new_test_table(dynamodb,
        Tags=[{'Key': 'system:timestamp_attribute', 'Value': 'ts'}],
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        p = random_string()
        # Put item without the timestamp attribute
        table.put_item(Item={'p': p, 'val': 'hello'})
        item = table.get_item(Key={'p': p}, ConsistentRead=True)['Item']
        assert item['val'] == 'hello'
        # No 'ts' attribute expected either
        assert 'ts' not in item

# Test that using a condition expression (which requires LWT) together with
# the timestamp attribute is rejected.
def test_timestamp_attribute_with_condition_rejected(scylla_only, dynamodb):
    """
    Test that using system:timestamp_attribute together with a ConditionExpression
    is rejected, since they are incompatible (LWT cannot use custom timestamps).
    """
    with new_test_table(dynamodb,
        Tags=[{'Key': 'system:timestamp_attribute', 'Value': 'ts'},
              {'Key': 'system:write_isolation', 'Value': 'only_rmw_uses_lwt'}],
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        p = random_string()
        # Put an initial item
        table.put_item(Item={'p': p, 'val': 'initial'})
        # Try to put with a ConditionExpression and a timestamp - should be rejected
        with pytest.raises(ClientError, match='ValidationException'):
            table.put_item(
                Item={'p': p, 'val': 'updated', 'ts': LARGE_TS},
                ConditionExpression='attribute_exists(p)'
            )

# Test that when the timestamp attribute has a non-numeric value, it is
# treated as if the timestamp attribute is absent (use current timestamp),
# and the non-numeric attribute IS stored in the item normally.
def test_timestamp_attribute_non_numeric(scylla_only, dynamodb):
    """
    Test that when the timestamp attribute has a non-numeric value, it is
    stored normally in the item (no special timestamp handling occurs).
    """
    with new_test_table(dynamodb,
        Tags=[{'Key': 'system:timestamp_attribute', 'Value': 'ts'}],
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        p = random_string()
        # Put item with the timestamp attribute as a string (non-numeric)
        table.put_item(Item={'p': p, 'val': 'hello', 'ts': 'not_a_number'})
        item = table.get_item(Key={'p': p}, ConsistentRead=True)['Item']
        assert item['val'] == 'hello'
        # When the timestamp attribute has a non-numeric value, it should
        # be stored normally (not used as a timestamp and not omitted)
        assert item['ts'] == 'not_a_number'

# Test that the timestamp attribute tag can be set on a table with a sort key.
def test_timestamp_attribute_with_range_key(scylla_only, dynamodb):
    """
    Test that system:timestamp_attribute works on tables with both hash and
    range keys.
    """
    with new_test_table(dynamodb,
        Tags=[{'Key': 'system:timestamp_attribute', 'Value': 'ts'}],
        KeySchema=[
            {'AttributeName': 'p', 'KeyType': 'HASH'},
            {'AttributeName': 'c', 'KeyType': 'RANGE'},
        ],
        AttributeDefinitions=[
            {'AttributeName': 'p', 'AttributeType': 'S'},
            {'AttributeName': 'c', 'AttributeType': 'S'},
        ]) as table:
        p = random_string()
        c = random_string()
        # Write with a large timestamp
        table.put_item(Item={'p': p, 'c': c, 'val': 'large', 'ts': LARGE_TS})
        # Write with a small timestamp (should lose)
        table.put_item(Item={'p': p, 'c': c, 'val': 'small', 'ts': SMALL_TS})
        item = table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)['Item']
        assert item['val'] == 'large'
        assert 'ts' not in item

# Test that BatchWriteItem also respects the timestamp attribute.
def test_timestamp_attribute_batch_write(scylla_only, dynamodb):
    """
    Test that BatchWriteItem also uses the system:timestamp_attribute when
    writing items.
    """
    with new_test_table(dynamodb,
        Tags=[{'Key': 'system:timestamp_attribute', 'Value': 'ts'}],
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        p = random_string()
        # Write item via BatchWriteItem with a large timestamp
        dynamodb.batch_write_item(RequestItems={
            table.name: [
                {'PutRequest': {'Item': {'p': p, 'val': 'large_ts', 'ts': LARGE_TS}}}
            ]
        })
        # Write item via BatchWriteItem with a small timestamp (should lose)
        dynamodb.batch_write_item(RequestItems={
            table.name: [
                {'PutRequest': {'Item': {'p': p, 'val': 'small_ts', 'ts': SMALL_TS}}}
            ]
        })
        item = table.get_item(Key={'p': p}, ConsistentRead=True)['Item']
        assert item['val'] == 'large_ts'
        assert 'ts' not in item
