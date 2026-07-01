# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for the system:timestamp_attribute Alternator-only feature.
# This feature allows users to control the write timestamp of PutItem and
# UpdateItem operations by specifying an attribute name in the table's
# system:timestamp_attribute tag. When that attribute is present in the
# write request with a numeric value (microseconds since Unix epoch), it
# is used as the write timestamp. The attribute itself is not stored in
# the item data.
#
# This is an Alternator-only feature and the tests are skipped on DynamoDB.

import time
from decimal import Decimal

import pytest
from botocore.exceptions import ClientError

from .util import new_test_table, random_string

# A large timestamp in microseconds (far future, year ~2033)
LARGE_TS = Decimal('2000000000000000')
# A medium timestamp in microseconds (year ~2001)
MEDIUM_TS = Decimal('1000000000000000')
# A small timestamp in microseconds (year ~1970)
SMALL_TS = Decimal('100000000000000')

# Fixtures for tables with the system:timestamp_attribute tag, shared by
# many tests below. Because system:timestamp_attribute is a Scylla-only
# feature, all tests using these fixtures are marked scylla_only and skipped
# on DynamoDB.

# A table with only a hash key and system:timestamp_attribute='ts' tag.
# We explicitly set write isolation to only_rmw_uses_lwt so the tests remain
# correct even if the test default changes to always_use_lwt in the future.
@pytest.fixture(scope="module")
def test_table_ts(scylla_only, dynamodb):
    with new_test_table(dynamodb,
        Tags=[{'Key': 'system:timestamp_attribute', 'Value': 'ts'},
              {'Key': 'system:write_isolation', 'Value': 'only_rmw_uses_lwt'}],
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        yield table

# A table with string hash and range keys and system:timestamp_attribute='ts'.
# We explicitly set write isolation to only_rmw_uses_lwt so the tests remain
# correct even if the test default changes to always_use_lwt in the future.
@pytest.fixture(scope="module")
def test_table_ts_ss(scylla_only, dynamodb):
    with new_test_table(dynamodb,
        Tags=[{'Key': 'system:timestamp_attribute', 'Value': 'ts'},
              {'Key': 'system:write_isolation', 'Value': 'only_rmw_uses_lwt'}],
        KeySchema=[
            {'AttributeName': 'p', 'KeyType': 'HASH'},
            {'AttributeName': 'c', 'KeyType': 'RANGE'},
        ],
        AttributeDefinitions=[
            {'AttributeName': 'p', 'AttributeType': 'S'},
            {'AttributeName': 'c', 'AttributeType': 'S'},
        ]) as table:
        yield table

# A table with hash key, system:timestamp_attribute='ts' tag, and
# system:write_isolation='always' to test rejection in LWT_ALWAYS mode.
# In always_use_lwt mode, every write uses LWT, so the timestamp attribute
# feature cannot be used at all.
@pytest.fixture(scope="module")
def test_table_ts_lwt(scylla_only, dynamodb):
    with new_test_table(dynamodb,
        Tags=[{'Key': 'system:timestamp_attribute', 'Value': 'ts'},
              {'Key': 'system:write_isolation', 'Value': 'always'}],
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        yield table

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
    # We do not check that the desired timestamp was actually used as the
    # write timestamp, because there is no API to read an item's timestamp
    # with the DynamoDB API. In the following test we'll use CQL to read
    # the timestamp, and we also have additional tests below that check the
    # timestamp's correctness indirectly by verifying that writes with larger
    # timestamps win over smaller ones.

# As explained in test_timestamp_attribute_put_item_basic above, we couldn't
# read the write timestamp of an item using the DynamoDB API. So here we use
# CQL to read the timestamp of an item written with a custom timestamp, and
# verify that it matches the expected value.
def test_timestamp_attribute_put_item_read_cql(test_table_ts, cql):
    p = random_string()
    test_table_ts.put_item(Item={'p': p, 'val': 'hello', 'ts': LARGE_TS})
    # Read the 'val' attribute's write timestamp via CQL. Alternator stores
    # non-key attributes in the ':attrs' map<text,blob> column. We can read
    # the write timestamp of a specific map entry with WRITETIME(col[key]).
    ks = 'alternator_' + test_table_ts.name
    rows = list(cql.execute(
        f'SELECT WRITETIME(":attrs"[\'val\']) FROM "{ks}"."{test_table_ts.name}" WHERE p = %s',
        [p]))
    assert rows == [(int(LARGE_TS),)]

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

    # Now overwrite with a larger timestamp (LARGE_TS + 1) - should win
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
    assert item['val'] == 'hello'
    # But update with a larger timestamp (LARGE_TS + 1) should overwrite
    test_table_ts.update_item(
        Key={'p': p},
        AttributeUpdates={
            'val': {'Value': 'overwritten', 'Action': 'PUT'},
            'ts': {'Value': LARGE_TS + 1, 'Action': 'PUT'},
        }
    )
    item = test_table_ts.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    assert item['val'] == 'overwritten'

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
    assert item['val'] == 'hello'

    # But update with a larger timestamp (LARGE_TS + 1) should overwrite
    test_table_ts.update_item(
        Key={'p': p},
        UpdateExpression='SET val = :v, ts = :t',
        ExpressionAttributeValues={':v': 'overwritten', ':t': LARGE_TS + 1}
    )
    item = test_table_ts.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    assert item['val'] == 'overwritten'

# Test that when the timestamp attribute has an invalid value (non-numeric
# type or a negative or excessively large number), writes are rejected with a
# ValidationException. The test covers PutItem, UpdateItem, DeleteItem, and
# BatchWriteItem.
def test_timestamp_attribute_invalid_value(test_table_ts):
    p = random_string()
    for invalid_ts in ['not_a_number', [1,2,3], Decimal('-1'), Decimal('99999999999999999999')]:
        with pytest.raises(ClientError, match='ValidationException.*write timestamp'):
            test_table_ts.put_item(Item={'p': p, 'val': 'hello', 'ts': invalid_ts})
        with pytest.raises(ClientError, match='ValidationException.*write timestamp'):
            test_table_ts.update_item(
                Key={'p': p},
                AttributeUpdates={
                    'val': {'Value': 'hello', 'Action': 'PUT'},
                    'ts': {'Value': invalid_ts, 'Action': 'PUT'},
                }
            )
        with pytest.raises(ClientError, match='ValidationException.*write timestamp'):
            test_table_ts.update_item(
                Key={'p': p},
                UpdateExpression='SET val = :v, ts = :t',
                ExpressionAttributeValues={':v': 'hello', ':t': invalid_ts}
            )
        with pytest.raises(ClientError, match='ValidationException.*write timestamp'):
            test_table_ts.delete_item(Key={'p': p, 'ts': invalid_ts})
        with pytest.raises(ClientError, match='ValidationException.*write timestamp'):
            test_table_ts.meta.client.batch_write_item(RequestItems={
                test_table_ts.name: [{'PutRequest': {'Item': {'p': p, 'val': 'hello', 'ts': invalid_ts}}}]
            })

# Test that the timestamp attribute value is interpreted in microseconds since
# the Unix epoch, and also test that writes without custom timestamps are
# ordered correctly with respect to writes with custom timestamps.
def test_timestamp_attribute_microseconds(test_table_ts):
    # Get current time in microseconds from the Python client side.
    now_us = int(time.time() * 1_000_000)
    one_hour_us = 3600 * 1_000_000

    # Part 1: write with one hour in the past as the custom timestamp, then
    # overwrite without a custom timestamp.  The second write uses the
    # server's current time, so it should win.
    past_us = now_us - one_hour_us
    p = random_string()
    test_table_ts.put_item(Item={'p': p, 'val': 'old', 'ts': Decimal(str(past_us))})
    test_table_ts.put_item(Item={'p': p, 'val': 'new'})
    item = test_table_ts.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    assert item['val'] == 'new'

    # Part 2: write with a timestamp one hour in the future, then overwrite
    # without a custom timestamp.  The server's current time is much less
    # than now_us + one_hour_us, so the first write should win.
    p = random_string()
    future_us = now_us + one_hour_us
    test_table_ts.put_item(Item={'p': p, 'val': 'future', 'ts': Decimal(str(future_us))})
    test_table_ts.put_item(Item={'p': p, 'val': 'now'})
    item = test_table_ts.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    assert item['val'] == 'future'

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

# Test that DeleteItem respects the timestamp attribute: a delete with a
# smaller timestamp than the item's write timestamp should not take effect.
def test_timestamp_attribute_delete_item(test_table_ts):
    p = random_string()
    # Write an item with a large timestamp
    test_table_ts.put_item(Item={'p': p, 'val': 'hello', 'ts': LARGE_TS})
    item = test_table_ts.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    assert item['val'] == 'hello'
    # Delete with a small timestamp - the delete should lose (item still exists)
    test_table_ts.delete_item(Key={'p': p, 'ts': SMALL_TS})
    item = test_table_ts.get_item(Key={'p': p}, ConsistentRead=True).get('Item')
    assert item is not None and item['val'] == 'hello'
    # Delete with a large timestamp - the delete should win (item is removed)
    test_table_ts.delete_item(Key={'p': p, 'ts': LARGE_TS + 1})
    item = test_table_ts.get_item(Key={'p': p}, ConsistentRead=True).get('Item')
    assert item is None

# Test that DeleteItem without the timestamp attribute in the key behaves
# normally (no custom timestamp is applied).
def test_timestamp_attribute_delete_item_no_ts(test_table_ts):
    p = random_string()
    # Use SMALL_TS so the delete (which uses the current server time) wins.
    # If we used LARGE_TS (far future), the delete without an explicit timestamp
    # would use current time which is smaller than LARGE_TS and the delete would lose.
    test_table_ts.put_item(Item={'p': p, 'val': 'hello', 'ts': SMALL_TS})
    # Delete without a timestamp attribute - should succeed normally
    test_table_ts.delete_item(Key={'p': p})
    assert 'Item' not in test_table_ts.get_item(Key={'p': p}, ConsistentRead=True)
    # Verify that an item written with a far-future timestamp is NOT deleted by
    # a delete without an explicit timestamp (server time < LARGE_TS).
    p = random_string()
    test_table_ts.put_item(Item={'p': p, 'val': 'hello', 'ts': LARGE_TS})
    test_table_ts.delete_item(Key={'p': p})
    item = test_table_ts.get_item(Key={'p': p}, ConsistentRead=True).get('Item')
    assert item is not None and item['val'] == 'hello'

# Test that BatchWriteItem DeleteRequest also respects the timestamp attribute.
def test_timestamp_attribute_batch_delete(test_table_ts):
    p = random_string()
    # Write an item with a large timestamp
    test_table_ts.put_item(Item={'p': p, 'val': 'hello', 'ts': LARGE_TS})
    # Delete via BatchWriteItem with a small timestamp - delete should lose
    test_table_ts.meta.client.batch_write_item(RequestItems={
        test_table_ts.name: [{'DeleteRequest': {'Key': {'p': p, 'ts': SMALL_TS}}}]
    })
    item = test_table_ts.get_item(Key={'p': p}, ConsistentRead=True).get('Item')
    assert item is not None and item['val'] == 'hello'
    # Delete via BatchWriteItem with a large timestamp - delete should win
    test_table_ts.meta.client.batch_write_item(RequestItems={
        test_table_ts.name: [{'DeleteRequest': {'Key': {'p': p, 'ts': LARGE_TS + 1}}}]
    })
    assert 'Item' not in test_table_ts.get_item(Key={'p': p}, ConsistentRead=True)


# LWT does not allow custom timestamps (because LWT needs to choose its own
# timestamp). So any configuration or request type that needs to use LWT to
# achieve the desired write isolation must reject custom timestamps. The
# following tests verify that custom timestamps are indeed rejected in the
# appropriate situations, and accepted when LWT is not required.

# Test that PutItem with a condition (which requires LWT) together with the
# timestamp attribute is rejected.
def test_put_item_timestamp_attribute_with_condition_rejected(test_table_ts):
    p = random_string()
    # Put an initial item (no timestamp attribute, so LWT is ok)
    test_table_ts.put_item(Item={'p': p, 'val': 'initial'})
    # Try to put with a ConditionExpression and a timestamp - should be rejected
    with pytest.raises(ClientError, match='ValidationException.*timestamp_attribute'):
        test_table_ts.put_item(
            Item={'p': p, 'val': 'updated', 'ts': LARGE_TS},
            ConditionExpression='attribute_exists(p)'
        )
    # Also check the old-style Expected condition - should also be rejected
    with pytest.raises(ClientError, match='ValidationException.*timestamp_attribute'):
        test_table_ts.put_item(
            Item={'p': p, 'val': 'updated', 'ts': LARGE_TS},
            Expected={'p': {'Value': p}}
        )

# Test that PutItem with a ReturnValues=ALL_OLD, which also requires LWT,
# rejects a timestamp attribute. If ReturnValues=ALL_NEW was supported,
# it would not have required LWT and might allow the timestamp attribute,
# but DynamoDB does not support ReturnValues=ALL_NEW for PutItem.
def test_put_item_timestamp_attribute_with_returnvalues_rejected(test_table_ts):
    p = random_string()
    test_table_ts.put_item(Item={'p': p, 'val': 'initial'})
    # ReturnValues=ALL_OLD requires a read-before-write (LWT), so combining it
    # with a custom timestamp should be rejected.
    with pytest.raises(ClientError, match='ValidationException.*timestamp_attribute'):
        test_table_ts.put_item(
            Item={'p': p, 'val': 'updated', 'ts': LARGE_TS},
            ReturnValues='ALL_OLD'
        )

# Test that using the timestamp attribute with the 'always' write isolation
# policy is rejected, because in always_use_lwt mode every write uses LWT
# (including unconditional ones), which is incompatible with custom timestamps.
def test_put_item_timestamp_attribute_lwt_always_rejected(test_table_ts_lwt):
    p = random_string()
    # Even a plain PutItem with a timestamp is rejected in LWT_ALWAYS mode
    with pytest.raises(ClientError, match='ValidationException.*timestamp_attribute'):
        test_table_ts_lwt.put_item(Item={'p': p, 'val': 'hello', 'ts': LARGE_TS})

# Test that UpdateItem rejects a custom timestamp in various situations where
# (in only_rmw_uses_lwt write isolation mode) LWT is required: conditions,
# ReturnValues, and UpdateExpressions or AttributeUpdates that read
# the previous value.
def test_update_item_timestamp_attribute_lwt_rejected(test_table_ts):
    p = random_string()
    test_table_ts.put_item(Item={'p': p, 'val': 1})
    # ConditionExpression requires LWT
    with pytest.raises(ClientError, match='ValidationException.*timestamp_attribute'):
        test_table_ts.update_item(
            Key={'p': p},
            UpdateExpression='SET val = :v, ts = :t',
            ExpressionAttributeValues={':v': 2, ':t': LARGE_TS},
            ConditionExpression='attribute_exists(p)'
        )
    # Old-style Expected condition requires LWT
    with pytest.raises(ClientError, match='ValidationException.*timestamp_attribute'):
        test_table_ts.update_item(
            Key={'p': p},
            AttributeUpdates={'val': {'Value': 2, 'Action': 'PUT'},
                              'ts':  {'Value': LARGE_TS, 'Action': 'PUT'}},
            Expected={'p': {'Value': p}}
        )
    # ReturnValues=ALL_OLD requires LWT for reading the previous item
    with pytest.raises(ClientError, match='ValidationException.*timestamp_attribute'):
        test_table_ts.update_item(
            Key={'p': p},
            UpdateExpression='SET val = :v, ts = :t',
            ExpressionAttributeValues={':v': 2, ':t': LARGE_TS},
            ReturnValues='ALL_OLD'
        )
    # ReturnValues=ALL_NEW also requires reading the current item (to return
    # attributes that were not modified by the update), so it is also rejected.
    with pytest.raises(ClientError, match='ValidationException.*timestamp_attribute'):
        test_table_ts.update_item(
            Key={'p': p},
            UpdateExpression='SET val = :v, ts = :t',
            ExpressionAttributeValues={':v': 2, ':t': LARGE_TS},
            ReturnValues='ALL_NEW'
        )
    # ReturnValues=UPDATED_OLD also requires reading the previous item.
    with pytest.raises(ClientError, match='ValidationException.*timestamp_attribute'):
        test_table_ts.update_item(
            Key={'p': p},
            UpdateExpression='SET val = :v, ts = :t',
            ExpressionAttributeValues={':v': 2, ':t': LARGE_TS},
            ReturnValues='UPDATED_OLD'
        )
    # But ReturnValues=UPDATED_NEW only returns the attributes that were just
    # set, which are known without reading the previous item, so it doesn't
    # need LWT, and is allowed to set a custom timestamp.
    test_table_ts.update_item(
        Key={'p': p},
        UpdateExpression='SET val = :v, ts = :t',
        ExpressionAttributeValues={':v': 2, ':t': LARGE_TS},
        ReturnValues='UPDATED_NEW'
    )
    # UpdateExpression that reads the previous value requires LWT.
    with pytest.raises(ClientError, match='ValidationException.*timestamp_attribute'):
        test_table_ts.update_item(
            Key={'p': p},
            UpdateExpression='SET val = val + :v, ts = :t',
            ExpressionAttributeValues={':v': 2, ':t': LARGE_TS}
        )
    # UpdateExpression ADD clause reads and modifies the previous value,
    # so uses LWT. Note: ts must be set via SET (not ADD) for the custom
    # timestamp to be recognized; ADD is used for the other attribute.
    with pytest.raises(ClientError, match='ValidationException.*timestamp_attribute'):
        test_table_ts.update_item(
            Key={'p': p},
            UpdateExpression='SET ts = :t ADD val :v',
            ExpressionAttributeValues={':v': Decimal('1'), ':t': LARGE_TS}
        )
    # UpdateExpression DELETE clause (removes elements from a set) also uses LWT.
    # Again, ts must be set via SET for the custom timestamp to be recognized.
    with pytest.raises(ClientError, match='ValidationException.*timestamp_attribute'):
        test_table_ts.update_item(
            Key={'p': p},
            UpdateExpression='SET ts = :t DELETE myset :e',
            ExpressionAttributeValues={':e': {'a'}, ':t': LARGE_TS}
        )
    # UpdateExpression that doesn't read the previous value doesn't need
    # LWT, so is allowed with a custom timestamp.
    test_table_ts.update_item(
        Key={'p': p},
        UpdateExpression='SET val = :v, ts = :t',
        ExpressionAttributeValues={':v': 2, ':t': LARGE_TS}
    )
    # AttributeUpdates ADD action reads the previous value so needs LWT.
    with pytest.raises(ClientError, match='ValidationException.*timestamp_attribute'):
        test_table_ts.update_item(
            Key={'p': p},
            AttributeUpdates={'counter': {'Value': Decimal('1'), 'Action': 'ADD'},
                              'ts':      {'Value': LARGE_TS, 'Action': 'PUT'}}
        )
    # AttributeUpdates DELETE action with a Value (removes set elements) also uses LWT.
    with pytest.raises(ClientError, match='ValidationException.*timestamp_attribute'):
        test_table_ts.update_item(
            Key={'p': p},
            AttributeUpdates={'myset': {'Value': {'a'}, 'Action': 'DELETE'},
                              'ts':    {'Value': LARGE_TS, 'Action': 'PUT'}}
        )
    # AttributeUpdates DELETE action without a Value just removes the attribute
    # entirely and does not need to read the previous item, so does not use LWT
    # and is allowed to have a custom timestamp.
    test_table_ts.update_item(
        Key={'p': p},
        AttributeUpdates={'some_attr': {'Action': 'DELETE'},
                          'ts':        {'Value': LARGE_TS, 'Action': 'PUT'}}
    )

# Test that DeleteItem with a condition (ConditionExpression or Expected) and
# a custom timestamp is rejected, because conditional writes require LWT which
# is incompatible with custom timestamps.
def test_timestamp_attribute_delete_item_condition_rejected(test_table_ts):
    p = random_string()
    test_table_ts.put_item(Item={'p': p, 'val': 'hello'})
    with pytest.raises(ClientError, match='ValidationException.*timestamp_attribute'):
        test_table_ts.delete_item(
            Key={'p': p, 'ts': SMALL_TS},
            ConditionExpression='attribute_exists(p)'
        )
    # Old-style Expected condition also requires LWT and should be rejected.
    with pytest.raises(ClientError, match='ValidationException.*timestamp_attribute'):
        test_table_ts.delete_item(
            Key={'p': p, 'ts': SMALL_TS},
            Expected={'p': {'Value': p}}
        )

# Test that DeleteItem with ReturnValues=ALL_OLD and a custom timestamp is
# rejected, because returning the previous item requires reading it first (LWT).
def test_timestamp_attribute_delete_item_returnvalues_rejected(test_table_ts):
    p = random_string()
    test_table_ts.put_item(Item={'p': p, 'val': 'hello'})
    with pytest.raises(ClientError, match='ValidationException.*timestamp_attribute'):
        test_table_ts.delete_item(
            Key={'p': p, 'ts': SMALL_TS},
            ReturnValues='ALL_OLD'
        )

# Test that DeleteItem with a custom timestamp is rejected when the table uses
# always_use_lwt isolation, because every write uses LWT in that mode.
def test_timestamp_attribute_delete_item_lwt_always_rejected(test_table_ts_lwt):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException.*timestamp_attribute'):
        test_table_ts_lwt.delete_item(Key={'p': p, 'ts': SMALL_TS})

# Test that BatchWriteItem with a custom timestamp is rejected when the table
# uses always_use_lwt isolation.
# Note that BatchWriteItem doesn't support any of the other reasons that make
# other write operations use LWT (conditions, or update expressions, or return
# values are not supported by BatchWriteItem), so this test is short.
def test_timestamp_attribute_batch_write_lwt_always_rejected(test_table_ts_lwt):
    p = random_string()
    # Check that both PutRequest and DeleteRequest are rejected, because
    # the write isolation policy (always_use_lwt) forces it to use LWT
    # so setting the timestamp attribute is not allowed.
    with pytest.raises(ClientError, match='ValidationException.*timestamp_attribute'):
        test_table_ts_lwt.meta.client.batch_write_item(RequestItems={
            test_table_ts_lwt.name: [{'PutRequest': {'Item': {'p': p, 'val': 'v', 'ts': SMALL_TS}}}]
        })
    with pytest.raises(ClientError, match='ValidationException.*timestamp_attribute'):
        test_table_ts_lwt.meta.client.batch_write_item(RequestItems={
            test_table_ts_lwt.name: [{'DeleteRequest': {'Key': {'p': p, 'ts': SMALL_TS}}}]
        })


# All the previous tests were on a table with a partition key only. Test that
# custom timestamps also work in a table with a range key. We check the four
# write operations: PutItem, UpdateItem, DeleteItem, and BatchWriteItem.
def test_timestamp_attribute_with_range_key(test_table_ts_ss):
    p = random_string()

    ### PutItem:
    c1 = random_string()
    test_table_ts_ss.put_item(Item={'p': p, 'c': c1, 'val': 'large', 'ts': LARGE_TS})
    # Write with a smaller timestamp - should lose
    test_table_ts_ss.put_item(Item={'p': p, 'c': c1, 'val': 'small', 'ts': SMALL_TS})
    item = test_table_ts_ss.get_item(Key={'p': p, 'c': c1}, ConsistentRead=True)['Item']
    assert item['val'] == 'large'
    assert 'ts' not in item
    # Write with a larger timestamp (LARGE_TS + 1) - should win
    test_table_ts_ss.put_item(Item={'p': p, 'c': c1, 'val': 'larger', 'ts': LARGE_TS + 1})
    item = test_table_ts_ss.get_item(Key={'p': p, 'c': c1}, ConsistentRead=True)['Item']
    assert item['val'] == 'larger'

    ### UpdateItem:
    c2 = random_string()
    test_table_ts_ss.update_item(
        Key={'p': p, 'c': c2},
        UpdateExpression='SET val = :v, ts = :t',
        ExpressionAttributeValues={':v': 'update_large', ':t': LARGE_TS}
    )
    item = test_table_ts_ss.get_item(Key={'p': p, 'c': c2}, ConsistentRead=True)['Item']
    assert item['val'] == 'update_large'
    # Update with a smaller timestamp - should lose
    test_table_ts_ss.update_item(
        Key={'p': p, 'c': c2},
        UpdateExpression='SET val = :v, ts = :t',
        ExpressionAttributeValues={':v': 'update_small', ':t': SMALL_TS}
    )
    item = test_table_ts_ss.get_item(Key={'p': p, 'c': c2}, ConsistentRead=True)['Item']
    assert item['val'] == 'update_large'
    # Update with a larger timestamp - should win
    test_table_ts_ss.update_item(
        Key={'p': p, 'c': c2},
        UpdateExpression='SET val = :v, ts = :t',
        ExpressionAttributeValues={':v': 'update_larger', ':t': LARGE_TS + 1}
    )
    item = test_table_ts_ss.get_item(Key={'p': p, 'c': c2}, ConsistentRead=True)['Item']
    assert item['val'] == 'update_larger'

    ### DeleteItem:
    c3 = random_string()
    test_table_ts_ss.put_item(Item={'p': p, 'c': c3, 'val': 'hello', 'ts': LARGE_TS})
    # Delete with a small timestamp - should lose (item still exists)
    test_table_ts_ss.delete_item(Key={'p': p, 'c': c3, 'ts': SMALL_TS})
    item = test_table_ts_ss.get_item(Key={'p': p, 'c': c3}, ConsistentRead=True).get('Item')
    assert item is not None and item['val'] == 'hello'
    # Delete with a large timestamp - should win (item is removed)
    test_table_ts_ss.delete_item(Key={'p': p, 'c': c3, 'ts': LARGE_TS + 1})
    assert 'Item' not in test_table_ts_ss.get_item(Key={'p': p, 'c': c3}, ConsistentRead=True)

    ### BatchWriteItem PutRequest:
    c4 = random_string()
    test_table_ts_ss.meta.client.batch_write_item(RequestItems={
        test_table_ts_ss.name: [{'PutRequest': {'Item': {'p': p, 'c': c4, 'val': 'batch_large', 'ts': LARGE_TS}}}]
    })
    item = test_table_ts_ss.get_item(Key={'p': p, 'c': c4}, ConsistentRead=True)['Item']
    assert item['val'] == 'batch_large'
    # Write with a smaller timestamp - should lose
    test_table_ts_ss.meta.client.batch_write_item(RequestItems={
        test_table_ts_ss.name: [{'PutRequest': {'Item': {'p': p, 'c': c4, 'val': 'batch_small', 'ts': SMALL_TS}}}]
    })
    item = test_table_ts_ss.get_item(Key={'p': p, 'c': c4}, ConsistentRead=True)['Item']
    assert item['val'] == 'batch_large'
    # Write with a larger timestamp - should win
    test_table_ts_ss.meta.client.batch_write_item(RequestItems={
        test_table_ts_ss.name: [{'PutRequest': {'Item': {'p': p, 'c': c4, 'val': 'batch_larger', 'ts': LARGE_TS + 1}}}]
    })
    item = test_table_ts_ss.get_item(Key={'p': p, 'c': c4}, ConsistentRead=True)['Item']
    assert item['val'] == 'batch_larger'

    ### BatchWriteItem DeleteRequest:
    # Delete with a small timestamp - should lose (item still exists)
    test_table_ts_ss.meta.client.batch_write_item(RequestItems={
        test_table_ts_ss.name: [{'DeleteRequest': {'Key': {'p': p, 'c': c4, 'ts': SMALL_TS}}}]
    })
    item = test_table_ts_ss.get_item(Key={'p': p, 'c': c4}, ConsistentRead=True).get('Item')
    assert item is not None and item['val'] == 'batch_larger'
    # Delete with a large enough timestamp (LARGE_TS + 2) - should win (item is removed)
    test_table_ts_ss.meta.client.batch_write_item(RequestItems={
        test_table_ts_ss.name: [{'DeleteRequest': {'Key': {'p': p, 'c': c4, 'ts': LARGE_TS + 2}}}]
    })
    assert 'Item' not in test_table_ts_ss.get_item(Key={'p': p, 'c': c4}, ConsistentRead=True)

# Test that in unsafe_rmw write isolation mode, even read-modify-write
# operations (e.g., a ConditionExpression) which normally require LWT and
# (as tested above) refuse custom timestamps - do allow custom timestamps.
def test_timestamp_attribute_unsafe_rmw_allows_rmw(scylla_only, dynamodb):
    with new_test_table(dynamodb,
        Tags=[{'Key': 'system:timestamp_attribute', 'Value': 'ts'},
              {'Key': 'system:write_isolation', 'Value': 'unsafe_rmw'}],
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        p = random_string()
        table.put_item(Item={'p': p, 'val': 'initial'})
        # The following operations all require LWT in other modes, but not in
        # unsafe_rmw mode, so a custom timestamp is allowed. We confirm the
        # custom timestamp is honored by checking that we can write the
        # timestamp attribute ('ts') but it doesn't get stored in the item.
        # PutItem with ConditionExpression:
        table.put_item(
            Item={'p': p, 'val': 'conditional', 'ts': LARGE_TS},
            ConditionExpression='attribute_exists(p)'
        )
        item = table.get_item(Key={'p': p}, ConsistentRead=True)['Item']
        assert item['val'] == 'conditional'
        assert 'ts' not in item
        # PutItem with Expected:
        table.put_item(
            Item={'p': p, 'val': 'expected', 'ts': LARGE_TS + 1},
            Expected={'p': {'Value': p}}
        )
        item = table.get_item(Key={'p': p}, ConsistentRead=True)['Item']
        assert item['val'] == 'expected'
        # PutItem with ReturnValues=ALL_OLD:
        table.put_item(
            Item={'p': p, 'val': 'ret_all_old', 'ts': LARGE_TS + 2},
            ReturnValues='ALL_OLD'
        )
        item = table.get_item(Key={'p': p}, ConsistentRead=True)['Item']
        assert item['val'] == 'ret_all_old'
        assert 'ts' not in item
        # UpdateItem with ConditionExpression:
        table.update_item(
            Key={'p': p},
            UpdateExpression='SET val = :v, ts = :t',
            ExpressionAttributeValues={':v': 'cond_update', ':t': LARGE_TS + 3},
            ConditionExpression='attribute_exists(p)'
        )
        item = table.get_item(Key={'p': p}, ConsistentRead=True)['Item']
        assert item['val'] == 'cond_update'
        assert 'ts' not in item
        # UpdateItem with ReturnValues=ALL_OLD, ALL_NEW, and UPDATED_OLD:
        for rv in ['ALL_OLD', 'ALL_NEW', 'UPDATED_OLD']:
            table.update_item(
                Key={'p': p},
                UpdateExpression='SET val = :v, ts = :t',
                ExpressionAttributeValues={':v': f'rv_{rv}', ':t': LARGE_TS + 4},
                ReturnValues=rv
            )
        item = table.get_item(Key={'p': p}, ConsistentRead=True)['Item']
        assert 'ts' not in item
        # UpdateItem with UpdateExpression that reads the previous value (ADD clause):
        table.put_item(Item={'p': p, 'counter': Decimal('10'), 'ts': LARGE_TS + 5})
        table.update_item(
            Key={'p': p},
            UpdateExpression='SET ts = :t ADD counter :inc',
            ExpressionAttributeValues={':t': LARGE_TS + 6, ':inc': Decimal('1')}
        )
        item = table.get_item(Key={'p': p}, ConsistentRead=True)['Item']
        assert item['counter'] == Decimal('11')
        assert 'ts' not in item
        # UpdateItem with AttributeUpdates ADD action (reads previous value):
        table.update_item(
            Key={'p': p},
            AttributeUpdates={'counter': {'Value': Decimal('1'), 'Action': 'ADD'},
                              'ts':      {'Value': LARGE_TS + 7, 'Action': 'PUT'}}
        )
        item = table.get_item(Key={'p': p}, ConsistentRead=True)['Item']
        assert item['counter'] == Decimal('12')
        assert 'ts' not in item
        # DeleteItem with ConditionExpression:
        table.put_item(Item={'p': p, 'val': 'to_delete', 'ts': LARGE_TS + 8})
        table.delete_item(
            Key={'p': p, 'ts': LARGE_TS + 9},
            ConditionExpression='attribute_exists(p)'
        )
        assert 'Item' not in table.get_item(Key={'p': p}, ConsistentRead=True)
        # DeleteItem with Expected (old-style condition):
        table.put_item(Item={'p': p, 'val': 'to_delete2', 'ts': LARGE_TS + 10})
        table.delete_item(
            Key={'p': p, 'ts': LARGE_TS + 11},
            Expected={'p': {'Value': p}}
        )
        assert 'Item' not in table.get_item(Key={'p': p}, ConsistentRead=True)
        # DeleteItem with ReturnValues=ALL_OLD:
        table.put_item(Item={'p': p, 'val': 'to_delete3', 'ts': LARGE_TS + 12})
        table.delete_item(
            Key={'p': p, 'ts': LARGE_TS + 13},
            ReturnValues='ALL_OLD'
        )
        assert 'Item' not in table.get_item(Key={'p': p}, ConsistentRead=True)
