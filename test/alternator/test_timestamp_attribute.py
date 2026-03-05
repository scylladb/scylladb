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

import json
import time
from decimal import Decimal

import botocore
import pytest
from botocore.exceptions import ClientError
from packaging.version import Version

from test.pylib.skip_types import skip_env

from .test_gsi import assert_index_query
from .util import ManualRequestError, manual_request, new_test_table, random_string

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

# The timestamp attribute is documented to be an integer, but like other
# integer parameters in DynamoDB's API, a fractional value is accepted and
# truncated to an integer. Verify this exact truncation (not rounding)
# by writing a timestamp with a fractional microsecond part and reading back
# the resulting write timestamp via CQL, as in
# test_timestamp_attribute_put_item_read_cql above.
def test_timestamp_attribute_put_item_fractional_truncated(test_table_ts, cql):
    p = random_string()
    ts = Decimal('1234567890123.456789')
    test_table_ts.put_item(Item={'p': p, 'val': 'hello', 'ts': ts})
    ks = 'alternator_' + test_table_ts.name
    rows = list(cql.execute(
        f'SELECT WRITETIME(":attrs"[\'val\']) FROM "{ks}"."{test_table_ts.name}" WHERE p = %s',
        [p]))
    assert rows == [(1234567890123,)]

# A timestamp given in scientific notation with a positive exponent (e.g.,
# "1.2345E+14") has a negative decimal "scale", which exercises a different
# (multiplication, instead of truncation) code path than the plain-integer
# or fractional cases above. Verify it is handled correctly.
def test_timestamp_attribute_put_item_scientific_notation(test_table_ts, cql):
    p = random_string()
    ts = Decimal('1.2345E+14')
    test_table_ts.put_item(Item={'p': p, 'val': 'hello', 'ts': ts})
    ks = 'alternator_' + test_table_ts.name
    rows = list(cql.execute(
        f'SELECT WRITETIME(":attrs"[\'val\']) FROM "{ks}"."{test_table_ts.name}" WHERE p = %s',
        [p]))
    assert rows == [(123450000000000,)]

# Test the edges of the documented range of valid timestamps, 1 to 2^63-1:
# both edges are accepted and used as the write timestamp. Values just
# outside this range are rejected, as checked in
# test_timestamp_attribute_invalid_value below.
def test_timestamp_attribute_put_item_range_edges(test_table_ts, cql):
    ks = 'alternator_' + test_table_ts.name
    for ts in [1, 2**63 - 1]:
        p = random_string()
        test_table_ts.put_item(Item={'p': p, 'val': 'hello', 'ts': Decimal(ts)})
        rows = list(cql.execute(
            f'SELECT WRITETIME(":attrs"[\'val\']) FROM "{ks}"."{test_table_ts.name}" WHERE p = %s',
            [p]))
        assert rows == [(ts,)]

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
# type, or a number which after truncation to an integer is outside the
# documented range of 1 to 2^63-1), writes are rejected with a
# ValidationException. The test covers PutItem, UpdateItem, DeleteItem, and
# BatchWriteItem.
def test_timestamp_attribute_invalid_value(test_table_ts):
    p = random_string()
    for invalid_ts in ['not_a_number', [1,2,3], Decimal('-1'), Decimal('0'),
                       Decimal('0.5'), Decimal('9223372036854775808'),
                       Decimal('1E+19'), Decimal('99999999999999999999')]:
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

# Test that a malformed AttributeUpdates entry for the timestamp attribute
# (an Action that isn't a string, or an entry that isn't an object at all) is
# rejected with an error, and doesn't crash Scylla. UpdateItem looks for the
# timestamp attribute in AttributeUpdates before the entries are fully
# validated, so this lookup must not assume the entry is well-formed. Some of
# that validation happens even earlier, while checking if the update needs a
# read-before-write, but that check stops at the first ADD action - so in
# this test the malformed entry comes after an ADD. We use manual_request()
# because boto3 refuses to send these malformed requests.
def test_timestamp_attribute_malformed_attribute_updates(dynamodb, test_table_ts):
    p = random_string()
    for ts_update in [{'Action': 5, 'Value': {'N': '1'}}, 5]:
        payload = json.dumps({
            'TableName': test_table_ts.name,
            'Key': {'p': {'S': p}},
            'AttributeUpdates': {
                'a': {'Action': 'ADD', 'Value': {'N': '1'}},
                'ts': ts_update}})
        with pytest.raises(ManualRequestError):
            manual_request(dynamodb, 'UpdateItem', payload)

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


# Test a BatchWriteItem with several items, each with a different custom
# timestamp or with no custom timestamp at all: each item must be written
# with its own timestamp (or the current time). We read the write timestamps
# back with CQL's WRITETIME, as in test_timestamp_attribute_put_item_read_cql.
# Also test a BatchWriteItem writing to two tables, where only the second
# table doesn't allow custom timestamps (because it has Streams enabled):
# the entire batch is rejected, and nothing is written to either table.
def test_timestamp_attribute_batch_write_multiple(test_table_ts, dynamodb, cql):
    ks = 'alternator_' + test_table_ts.name
    def writetime(p):
        rows = list(cql.execute(
            f'SELECT WRITETIME(":attrs"[\'val\']) FROM "{ks}"."{test_table_ts.name}" WHERE p = %s',
            [p]))
        assert len(rows) == 1
        return rows[0][0]
    p1, p2, p3, p4 = [random_string() for i in range(4)]
    test_table_ts.put_item(Item={'p': p4, 'val': 'to_delete', 'ts': MEDIUM_TS + 30})
    now_us = int(time.time() * 1_000_000)
    one_hour_us = 3600 * 1_000_000
    test_table_ts.meta.client.batch_write_item(RequestItems={test_table_ts.name: [
        {'PutRequest': {'Item': {'p': p1, 'val': 'one', 'ts': MEDIUM_TS + 10}}},
        {'PutRequest': {'Item': {'p': p2, 'val': 'two'}}},
        {'PutRequest': {'Item': {'p': p3, 'val': 'three', 'ts': MEDIUM_TS + 20}}},
        {'DeleteRequest': {'Key': {'p': p4, 'ts': MEDIUM_TS + 40}}}]})
    assert writetime(p1) == MEDIUM_TS + 10
    # p2 has no custom timestamp, so it was written with the current time
    # (we allow the client's clock to be off by up to an hour).
    assert now_us - one_hour_us < writetime(p2) < now_us + one_hour_us
    assert writetime(p3) == MEDIUM_TS + 20
    # The delete of p4 had a newer timestamp than p4's write, so it won.
    assert 'Item' not in test_table_ts.get_item(Key={'p': p4}, ConsistentRead=True)
    for p in [p1, p2, p3]:
        assert 'ts' not in test_table_ts.get_item(Key={'p': p}, ConsistentRead=True)['Item']

    with new_test_table(dynamodb,
        Tags=[{'Key': 'system:timestamp_attribute', 'Value': 'ts'}],
        StreamSpecification={'StreamEnabled': True, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table_streams:
        p5, p6, p7 = [random_string() for i in range(3)]
        with pytest.raises(ClientError, match='ValidationException.*Streams'):
            test_table_ts.meta.client.batch_write_item(RequestItems={
                test_table_ts.name: [
                    {'PutRequest': {'Item': {'p': p5, 'val': 'hello', 'ts': MEDIUM_TS}}}],
                # On the Streams table, only the second item has a custom
                # timestamp, but this is enough to reject the batch.
                table_streams.name: [
                    {'PutRequest': {'Item': {'p': p6, 'val': 'hello'}}},
                    {'PutRequest': {'Item': {'p': p7, 'val': 'hello', 'ts': MEDIUM_TS}}}]})
        assert 'Item' not in test_table_ts.get_item(Key={'p': p5}, ConsistentRead=True)
        assert 'Item' not in table_streams.get_item(Key={'p': p6}, ConsistentRead=True)
        assert 'Item' not in table_streams.get_item(Key={'p': p7}, ConsistentRead=True)

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
        # Write the initial item with a timestamp far in the future, and give
        # each following write a slightly larger timestamp. If any of these
        # writes ignored its custom timestamp and used the current time, it
        # would lose to the previous write, and the checks below would fail.
        table.put_item(Item={'p': p, 'val': 'initial', 'ts': LARGE_TS})
        # The following operations all require LWT in other modes, but not in
        # unsafe_rmw mode, so a custom timestamp is allowed. We confirm the
        # custom timestamp is honored by checking that the write took effect
        # (see above), and that the timestamp attribute ('ts') didn't get
        # stored in the item.
        # PutItem with ConditionExpression:
        table.put_item(
            Item={'p': p, 'val': 'conditional', 'ts': LARGE_TS + 1},
            ConditionExpression='attribute_exists(p)'
        )
        item = table.get_item(Key={'p': p}, ConsistentRead=True)['Item']
        assert item['val'] == 'conditional'
        assert 'ts' not in item
        # PutItem with Expected:
        table.put_item(
            Item={'p': p, 'val': 'expected', 'ts': LARGE_TS + 2},
            Expected={'p': {'Value': p}}
        )
        item = table.get_item(Key={'p': p}, ConsistentRead=True)['Item']
        assert item['val'] == 'expected'
        # PutItem with ReturnValues=ALL_OLD:
        table.put_item(
            Item={'p': p, 'val': 'ret_all_old', 'ts': LARGE_TS + 3},
            ReturnValues='ALL_OLD'
        )
        item = table.get_item(Key={'p': p}, ConsistentRead=True)['Item']
        assert item['val'] == 'ret_all_old'
        assert 'ts' not in item
        # UpdateItem with ConditionExpression:
        table.update_item(
            Key={'p': p},
            UpdateExpression='SET val = :v, ts = :t',
            ExpressionAttributeValues={':v': 'cond_update', ':t': LARGE_TS + 4},
            ConditionExpression='attribute_exists(p)'
        )
        item = table.get_item(Key={'p': p}, ConsistentRead=True)['Item']
        assert item['val'] == 'cond_update'
        assert 'ts' not in item
        # UpdateItem with ReturnValues=ALL_OLD, ALL_NEW, and UPDATED_OLD:
        for i, rv in enumerate(['ALL_OLD', 'ALL_NEW', 'UPDATED_OLD']):
            table.update_item(
                Key={'p': p},
                UpdateExpression='SET val = :v, ts = :t',
                ExpressionAttributeValues={':v': f'rv_{rv}', ':t': LARGE_TS + 5 + i},
                ReturnValues=rv
            )
            item = table.get_item(Key={'p': p}, ConsistentRead=True)['Item']
            assert item['val'] == f'rv_{rv}'
            assert 'ts' not in item
        # UpdateItem with UpdateExpression that reads the previous value (ADD clause):
        table.put_item(Item={'p': p, 'counter': Decimal('10'), 'ts': LARGE_TS + 8})
        table.update_item(
            Key={'p': p},
            UpdateExpression='SET ts = :t ADD counter :inc',
            ExpressionAttributeValues={':t': LARGE_TS + 9, ':inc': Decimal('1')}
        )
        item = table.get_item(Key={'p': p}, ConsistentRead=True)['Item']
        assert item['counter'] == Decimal('11')
        assert 'ts' not in item
        # UpdateItem with AttributeUpdates ADD action (reads previous value):
        table.update_item(
            Key={'p': p},
            AttributeUpdates={'counter': {'Value': Decimal('1'), 'Action': 'ADD'},
                              'ts':      {'Value': LARGE_TS + 10, 'Action': 'PUT'}}
        )
        item = table.get_item(Key={'p': p}, ConsistentRead=True)['Item']
        assert item['counter'] == Decimal('12')
        assert 'ts' not in item
        # DeleteItem with ConditionExpression:
        table.put_item(Item={'p': p, 'val': 'to_delete', 'ts': LARGE_TS + 11})
        table.delete_item(
            Key={'p': p, 'ts': LARGE_TS + 12},
            ConditionExpression='attribute_exists(p)'
        )
        assert 'Item' not in table.get_item(Key={'p': p}, ConsistentRead=True)
        # DeleteItem with Expected (old-style condition):
        table.put_item(Item={'p': p, 'val': 'to_delete2', 'ts': LARGE_TS + 13})
        table.delete_item(
            Key={'p': p, 'ts': LARGE_TS + 14},
            Expected={'p': {'Value': p}}
        )
        assert 'Item' not in table.get_item(Key={'p': p}, ConsistentRead=True)
        # DeleteItem with ReturnValues=ALL_OLD:
        table.put_item(Item={'p': p, 'val': 'to_delete3', 'ts': LARGE_TS + 15})
        table.delete_item(
            Key={'p': p, 'ts': LARGE_TS + 16},
            ReturnValues='ALL_OLD'
        )
        assert 'Item' not in table.get_item(Key={'p': p}, ConsistentRead=True)

# Test that writes with a custom timestamp are rejected on a table with
# Streams enabled: Streams can only record writes with timestamps close to
# the current time, so the main use case of custom timestamps - writing
# data with old timestamps - can't work on such a table. Writes without a
# custom timestamp still work normally.
def test_timestamp_attribute_streams_rejected(scylla_only, dynamodb):
    with new_test_table(dynamodb,
        Tags=[{'Key': 'system:timestamp_attribute', 'Value': 'ts'}],
        StreamSpecification={'StreamEnabled': True, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        p = random_string()
        table.put_item(Item={'p': p, 'val': 'hello'})
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'val': 'hello'}
        with pytest.raises(ClientError, match='ValidationException.*Streams'):
            table.put_item(Item={'p': p, 'val': 'hi', 'ts': LARGE_TS})
        with pytest.raises(ClientError, match='ValidationException.*Streams'):
            table.update_item(Key={'p': p},
                UpdateExpression='SET val = :v, ts = :t',
                ExpressionAttributeValues={':v': 'hi', ':t': LARGE_TS})
        with pytest.raises(ClientError, match='ValidationException.*Streams'):
            table.update_item(Key={'p': p},
                AttributeUpdates={'val': {'Value': 'hi', 'Action': 'PUT'},
                                  'ts': {'Value': LARGE_TS, 'Action': 'PUT'}})
        with pytest.raises(ClientError, match='ValidationException.*Streams'):
            table.delete_item(Key={'p': p, 'ts': LARGE_TS})
        with pytest.raises(ClientError, match='ValidationException.*Streams'):
            table.meta.client.batch_write_item(RequestItems={
                table.name: [{'PutRequest': {'Item': {'p': p, 'val': 'hi', 'ts': LARGE_TS}}}]})
        with pytest.raises(ClientError, match='ValidationException.*Streams'):
            table.meta.client.batch_write_item(RequestItems={
                table.name: [{'DeleteRequest': {'Key': {'p': p, 'ts': LARGE_TS}}}]})
        # None of the rejected writes modified the item:
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'val': 'hello'}

# The timestamp attribute isn't part of the item, so an UpdateItem which sets
# it behaves exactly like the same UpdateItem without it (except for the
# write timestamp). In particular, on a non-existent item (see
# test_update_item_non_existent in test_item.py): an update that only sets
# the timestamp attribute behaves like an empty update, which creates the
# item; but an update that sets the timestamp attribute and deletes
# attributes behaves like an update that only deletes attributes, which
# does not create the item.
def test_timestamp_attribute_update_non_existent(test_table_ts):
    p = random_string()
    test_table_ts.update_item(Key={'p': p},
        UpdateExpression='SET ts = :t',
        ExpressionAttributeValues={':t': LARGE_TS})
    assert test_table_ts.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p}
    p = random_string()
    test_table_ts.update_item(Key={'p': p},
        AttributeUpdates={'ts': {'Value': LARGE_TS, 'Action': 'PUT'}})
    assert test_table_ts.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p}
    p = random_string()
    test_table_ts.update_item(Key={'p': p},
        UpdateExpression='SET ts = :t REMOVE a',
        ExpressionAttributeValues={':t': LARGE_TS})
    assert 'Item' not in test_table_ts.get_item(Key={'p': p}, ConsistentRead=True)
    p = random_string()
    test_table_ts.update_item(Key={'p': p},
        AttributeUpdates={'ts': {'Value': LARGE_TS, 'Action': 'PUT'},
                          'a': {'Action': 'DELETE'}})
    assert 'Item' not in test_table_ts.get_item(Key={'p': p}, ConsistentRead=True)

# The timestamp attribute is never stored in the item, so UpdateItem can't
# ADD to it - with UpdateExpression's ADD or with AttributeUpdates' ADD
# action. Removing it (UpdateExpression's REMOVE or AttributeUpdates' DELETE
# action) is allowed, but does nothing.
def test_timestamp_attribute_add_rejected(test_table_ts):
    p = random_string()
    test_table_ts.put_item(Item={'p': p, 'val': 'hello'})
    with pytest.raises(ClientError, match='ValidationException.*ADD'):
        test_table_ts.update_item(Key={'p': p},
            UpdateExpression='ADD ts :n',
            ExpressionAttributeValues={':n': LARGE_TS})
    with pytest.raises(ClientError, match='ValidationException.*ADD'):
        test_table_ts.update_item(Key={'p': p},
            AttributeUpdates={'ts': {'Value': LARGE_TS, 'Action': 'ADD'}})
    test_table_ts.update_item(Key={'p': p}, UpdateExpression='REMOVE ts')
    test_table_ts.update_item(Key={'p': p}, AttributeUpdates={'ts': {'Action': 'DELETE'}})
    assert test_table_ts.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'val': 'hello'}

# Test that the system:timestamp_attribute tag is ignored if it names one of
# the table's key attributes: a key attribute must be stored in the item (it
# is the item's key), so it can't be consumed as a write timestamp. The
# writes work as if the tag wasn't there.
def test_timestamp_attribute_tag_on_key_ignored(scylla_only, dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'},
                   {'AttributeName': 'c', 'KeyType': 'RANGE'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'},
                              {'AttributeName': 'c', 'AttributeType': 'N'}]) as table:
        arn = table.meta.client.describe_table(TableName=table.name)['Table']['TableArn']
        for key_attr in ['p', 'c']:
            table.meta.client.tag_resource(ResourceArn=arn,
                Tags=[{'Key': 'system:timestamp_attribute', 'Value': key_attr}])
            # The sort key c is a number which would be a valid timestamp.
            key = {'p': random_string(), 'c': LARGE_TS}
            table.put_item(Item={**key, 'val': 'hello'})
            assert table.get_item(Key=key, ConsistentRead=True)['Item'] == {**key, 'val': 'hello'}
            table.delete_item(Key=key)
            assert 'Item' not in table.get_item(Key=key, ConsistentRead=True)
            table.meta.client.batch_write_item(RequestItems={
                table.name: [{'PutRequest': {'Item': {**key, 'val': 'hi'}}}]})
            assert table.get_item(Key=key, ConsistentRead=True)['Item'] == {**key, 'val': 'hi'}
            table.meta.client.batch_write_item(RequestItems={
                table.name: [{'DeleteRequest': {'Key': key}}]})
            assert 'Item' not in table.get_item(Key=key, ConsistentRead=True)

# Test that the system:timestamp_attribute tag is ignored if it names a key
# attribute of one of the table's GSIs or LSIs: if it were consumed as a
# write timestamp, it wouldn't be stored in the item, so the item would be
# missing from the index. The attribute is stored as an ordinary attribute.
def test_timestamp_attribute_tag_on_index_key_ignored(scylla_only, dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'},
                   {'AttributeName': 'c', 'KeyType': 'RANGE'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'},
                              {'AttributeName': 'c', 'AttributeType': 'S'},
                              {'AttributeName': 'g', 'AttributeType': 'N'},
                              {'AttributeName': 'l', 'AttributeType': 'N'}],
        GlobalSecondaryIndexes=[{'IndexName': 'gsi',
            'KeySchema': [{'AttributeName': 'g', 'KeyType': 'HASH'}],
            'Projection': {'ProjectionType': 'ALL'}}],
        LocalSecondaryIndexes=[{'IndexName': 'lsi',
            'KeySchema': [{'AttributeName': 'p', 'KeyType': 'HASH'},
                          {'AttributeName': 'l', 'KeyType': 'RANGE'}],
            'Projection': {'ProjectionType': 'ALL'}}]) as table:
        arn = table.meta.client.describe_table(TableName=table.name)['Table']['TableArn']
        for index_attr in ['g', 'l']:
            table.meta.client.tag_resource(ResourceArn=arn,
                Tags=[{'Key': 'system:timestamp_attribute', 'Value': index_attr}])
            key = {'p': random_string(), 'c': random_string()}
            # Both index keys are numbers which would be valid timestamps.
            item = {**key, 'g': LARGE_TS, 'l': LARGE_TS}
            table.put_item(Item=item)
            assert table.get_item(Key=key, ConsistentRead=True)['Item'] == item
            table.update_item(Key=key,
                UpdateExpression='SET #a = :v',
                ExpressionAttributeNames={'#a': index_attr},
                ExpressionAttributeValues={':v': LARGE_TS + 1})
            item[index_attr] = LARGE_TS + 1
            assert table.get_item(Key=key, ConsistentRead=True)['Item'] == item
            # The LSI can be read with ConsistentRead, so we can check that
            # the item is in it (the GSI can't, so we don't check it).
            assert table.query(IndexName='lsi', ConsistentRead=True,
                KeyConditionExpression='p = :p',
                ExpressionAttributeValues={':p': key['p']})['Items'] == [item]

# Test that the system:timestamp_attribute tag is ignored if it names an
# attribute used by a vector index - its vector attribute, or a SearchSchema
# attribute: if it were consumed as a write timestamp, it wouldn't be stored
# in the item, so the vector index couldn't use it. The attribute is stored
# as an ordinary attribute.
def test_timestamp_attribute_tag_on_vector_index_attribute_ignored(scylla_only, dynamodb):
    if Version(botocore.__version__) < Version('1.43.64'):
        skip_env('Botocore version 1.43.64 or above required to run this test')
    with new_test_table(dynamodb,
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'},
                              {'AttributeName': 'h', 'AttributeType': 'N'}],
        VectorIndexes=[{'IndexName': 'vind',
            'VectorAttribute': {'AttributeName': 'v'}, 'Dimensions': 3,
            'DistanceFunction': 'COSINE',
            'Projection': {'ProjectionType': 'KEYS_ONLY'},
            'SearchSchema': [{'AttributeName': 'h', 'SearchSchemaElementType': 'HASH'}]}]) as table:
        arn = table.meta.client.describe_table(TableName=table.name)['Table']['TableArn']
        for vector_attr in ['v', 'h']:
            table.meta.client.tag_resource(ResourceArn=arn,
                Tags=[{'Key': 'system:timestamp_attribute', 'Value': vector_attr}])
            p = random_string()
            # h is a number which would be a valid timestamp.
            item = {'p': p, 'v': [Decimal(1), Decimal(2), Decimal(3)], 'h': LARGE_TS}
            table.put_item(Item=item)
            assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == item

# Test that writes with a custom timestamp are rejected on a table with an
# index whose key has two or more attributes which aren't key attributes of
# the table - here a GSI with both a hash key and a range key. For such an
# index, writes that don't arrive in timestamp order can cause an item to go
# missing from the index. Writes without a custom timestamp still work.
def test_timestamp_attribute_multi_attribute_index_rejected(scylla_only, dynamodb):
    with new_test_table(dynamodb,
        Tags=[{'Key': 'system:timestamp_attribute', 'Value': 'ts'}],
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'},
                              {'AttributeName': 'g1', 'AttributeType': 'S'},
                              {'AttributeName': 'g2', 'AttributeType': 'S'}],
        GlobalSecondaryIndexes=[{'IndexName': 'gsi',
            'KeySchema': [{'AttributeName': 'g1', 'KeyType': 'HASH'},
                          {'AttributeName': 'g2', 'KeyType': 'RANGE'}],
            'Projection': {'ProjectionType': 'ALL'}}]) as table:
        p = random_string()
        table.put_item(Item={'p': p, 'val': 'hello'})
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'val': 'hello'}
        with pytest.raises(ClientError, match='ValidationException.*index gsi'):
            table.put_item(Item={'p': p, 'val': 'hi', 'ts': LARGE_TS})
        with pytest.raises(ClientError, match='ValidationException.*index gsi'):
            table.update_item(Key={'p': p},
                UpdateExpression='SET val = :v, ts = :t',
                ExpressionAttributeValues={':v': 'hi', ':t': LARGE_TS})
        with pytest.raises(ClientError, match='ValidationException.*index gsi'):
            table.delete_item(Key={'p': p, 'ts': LARGE_TS})
        with pytest.raises(ClientError, match='ValidationException.*index gsi'):
            table.meta.client.batch_write_item(RequestItems={
                table.name: [{'PutRequest': {'Item': {'p': p, 'val': 'hi', 'ts': LARGE_TS}}}]})
        with pytest.raises(ClientError, match='ValidationException.*index gsi'):
            table.meta.client.batch_write_item(RequestItems={
                table.name: [{'DeleteRequest': {'Key': {'p': p, 'ts': LARGE_TS}}}]})
        # None of the rejected writes modified the item:
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'val': 'hello'}

# Unlike the previous test, custom timestamps *are* allowed on a table whose
# indexes each have just one key attribute which isn't a key attribute of
# the table - here a GSI and an LSI. Check that such indexes remain correct
# when writes don't arrive in timestamp order.
def test_timestamp_attribute_single_attribute_index(scylla_only, dynamodb):
    with new_test_table(dynamodb,
        Tags=[{'Key': 'system:timestamp_attribute', 'Value': 'ts'}],
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'},
                   {'AttributeName': 'c', 'KeyType': 'RANGE'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'},
                              {'AttributeName': 'c', 'AttributeType': 'S'},
                              {'AttributeName': 'x', 'AttributeType': 'S'}],
        GlobalSecondaryIndexes=[{'IndexName': 'gsi',
            'KeySchema': [{'AttributeName': 'x', 'KeyType': 'HASH'}],
            'Projection': {'ProjectionType': 'ALL'}}],
        LocalSecondaryIndexes=[{'IndexName': 'lsi',
            'KeySchema': [{'AttributeName': 'p', 'KeyType': 'HASH'},
                          {'AttributeName': 'x', 'KeyType': 'RANGE'}],
            'Projection': {'ProjectionType': 'ALL'}}]) as table:
        key = {'p': random_string(), 'c': random_string()}
        a = random_string()
        b = random_string()
        def set_x(x, ts):
            table.update_item(Key=key,
                UpdateExpression='SET x = :x, ts = :t',
                ExpressionAttributeValues={':x': x, ':t': ts})
        def check_indexes(x, other_x):
            item = {**key, 'x': x}
            assert table.get_item(Key=key, ConsistentRead=True)['Item'] == item
            assert_index_query(table, 'gsi', [item],
                KeyConditions={'x': {'AttributeValueList': [x], 'ComparisonOperator': 'EQ'}})
            assert_index_query(table, 'gsi', [],
                KeyConditions={'x': {'AttributeValueList': [other_x], 'ComparisonOperator': 'EQ'}})
            assert_index_query(table, 'lsi', [item],
                KeyConditions={'p': {'AttributeValueList': [key['p']], 'ComparisonOperator': 'EQ'}})
        set_x(a, MEDIUM_TS + 1)
        set_x(b, MEDIUM_TS + 3)
        check_indexes(b, a)
        # This write has an older timestamp than the previous one, so it loses:
        set_x(a, MEDIUM_TS + 2)
        check_indexes(b, a)
        # This write has the newest timestamp, so it wins:
        set_x(a, MEDIUM_TS + 4)
        check_indexes(a, b)

# The system:timestamp_attribute tag's value is an attribute name, so it is
# not limited like an ordinary tag value (to 256 characters, and only some
# characters): any attribute name - such as a long one, or one with
# characters not allowed in tag values - can be used as the timestamp
# attribute, set either in CreateTable or with TagResource. But a value
# longer than the longest allowed attribute name (65535 bytes) is rejected.
def test_timestamp_attribute_tag_unusual_names(scylla_only, dynamodb):
    names = ['x' * 257, 'a#b,c "d"', 'caf\u00e9']
    def check(table, name):
        p = random_string()
        table.put_item(Item={'p': p, 'val': 'large_ts', name: LARGE_TS})
        table.put_item(Item={'p': p, 'val': 'small_ts', name: SMALL_TS})
        assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'val': 'large_ts'}
    for name in names:
        with new_test_table(dynamodb,
            Tags=[{'Key': 'system:timestamp_attribute', 'Value': name}],
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
            check(table, name)
    with new_test_table(dynamodb,
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        arn = table.meta.client.describe_table(TableName=table.name)['Table']['TableArn']
        for name in names:
            table.meta.client.tag_resource(ResourceArn=arn,
                Tags=[{'Key': 'system:timestamp_attribute', 'Value': name}])
            check(table, name)
        with pytest.raises(ClientError, match='ValidationException.*system:timestamp_attribute'):
            table.meta.client.tag_resource(ResourceArn=arn,
                Tags=[{'Key': 'system:timestamp_attribute', 'Value': 'x' * 65536}])

# Test that the system:timestamp_attribute tag is ignored if it names the
# table's TTL attribute (set with UpdateTimeToLive): if it were consumed as
# a write timestamp, it wouldn't be stored in the item, so the item would
# never expire. The attribute is stored as an ordinary attribute. We check
# both orders - setting the tag before enabling TTL, and after.
def test_timestamp_attribute_tag_on_ttl_attribute_ignored(scylla_only, dynamodb):
    for tag_first in [True, False]:
        with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
            client = table.meta.client
            arn = client.describe_table(TableName=table.name)['Table']['TableArn']
            def set_tag():
                client.tag_resource(ResourceArn=arn,
                    Tags=[{'Key': 'system:timestamp_attribute', 'Value': 'expiration'}])
            def enable_ttl():
                client.update_time_to_live(TableName=table.name,
                    TimeToLiveSpecification={'AttributeName': 'expiration', 'Enabled': True})
            if tag_first:
                set_tag()
                enable_ttl()
            else:
                enable_ttl()
                set_tag()
            p = random_string()
            # The expiration time (in seconds) would also be a valid write
            # timestamp (in microseconds).
            expiration = int(time.time()) + 3600
            item = {'p': p, 'val': 'hello', 'expiration': expiration}
            table.put_item(Item=item)
            assert table.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == item
