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

# Tests for batch operations - BatchWriteItem, BatchReadItem.
# Note that various other tests in other files also use these operations,
# so they are actually tested by other tests as well.

import pytest
import random
from botocore.exceptions import ClientError
from util import random_string, full_scan, full_query, multiset

# Test ensuring that items inserted by a batched statement can be properly extracted
# via GetItem. Schema has both hash and sort keys.
def test_basic_batch_write_item(test_table):
    count = 7

    with test_table.batch_writer() as batch:
        for i in range(count):
            batch.put_item(Item={
                'p': "batch{}".format(i),
                'c': "batch_ck{}".format(i),
                'attribute': str(i),
                'another': 'xyz'
            })

    for i in range(count):
        item = test_table.get_item(Key={'p': "batch{}".format(i), 'c': "batch_ck{}".format(i)}, ConsistentRead=True)['Item']
        assert item['p'] == "batch{}".format(i)
        assert item['c'] == "batch_ck{}".format(i)
        assert item['attribute'] == str(i)
        assert item['another'] == 'xyz' 

# Try a batch which includes both multiple writes to the same partition
# and several partitions. The LWT code collects multiple mutations to the
# same partition together, and we want to test that this worked correctly.
def test_batch_write_item_mixed(test_table):
    partitions = [random_string() for i in range(4)]
    items = [{'p': p, 'c': str(i)} for p in partitions for i in range(4)]
    with test_table.batch_writer() as batch:
        # Reorder items randomly, just for the heck of it
        for item in random.sample(items, len(items)):
            batch.put_item(item)
    for item in items:
        assert test_table.get_item(Key={'p': item['p'], 'c': item['c']}, ConsistentRead=True)['Item'] == item

# Test batch write to a table with only a hash key
def test_batch_write_hash_only(test_table_s):
    items = [{'p': random_string(), 'val': random_string()} for i in range(10)]
    with test_table_s.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    for item in items:
        assert test_table_s.get_item(Key={'p': item['p']}, ConsistentRead=True)['Item'] == item

# Test batch delete operation (DeleteRequest): We create a bunch of items, and
# then delete them all.
def test_batch_write_delete(test_table_s):
    items = [{'p': random_string(), 'val': random_string()} for i in range(10)]
    with test_table_s.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    for item in items:
        assert test_table_s.get_item(Key={'p': item['p']}, ConsistentRead=True)['Item'] == item
    with test_table_s.batch_writer() as batch:
        for item in items:
            batch.delete_item(Key={'p': item['p']})
    # Verify that all items are now missing:
    for item in items:
        assert not 'Item' in test_table_s.get_item(Key={'p': item['p']}, ConsistentRead=True)

# Test the same batch including both writes and delete. Should be fine.
def test_batch_write_and_delete(test_table_s):
    p1 = random_string()
    p2 = random_string()
    test_table_s.put_item(Item={'p': p1})
    assert 'Item' in test_table_s.get_item(Key={'p': p1}, ConsistentRead=True)
    assert not 'Item' in test_table_s.get_item(Key={'p': p2}, ConsistentRead=True)
    with test_table_s.batch_writer() as batch:
        batch.put_item({'p': p2})
        batch.delete_item(Key={'p': p1})
    assert not 'Item' in test_table_s.get_item(Key={'p': p1}, ConsistentRead=True)
    assert 'Item' in test_table_s.get_item(Key={'p': p2}, ConsistentRead=True)

# It is forbidden to update the same key twice in the same batch.
# DynamoDB says "Provided list of item keys contains duplicates".
def test_batch_write_duplicate_write(test_table_s, test_table):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException.*duplicates'):
        with test_table_s.batch_writer() as batch:
            batch.put_item({'p': p})
            batch.put_item({'p': p})
    c = random_string()
    with pytest.raises(ClientError, match='ValidationException.*duplicates'):
        with test_table.batch_writer() as batch:
            batch.put_item({'p': p, 'c': c})
            batch.put_item({'p': p, 'c': c})
    # But it is fine to touch items with one component the same, but the other not.
    other = random_string()
    with test_table.batch_writer() as batch:
        batch.put_item({'p': p, 'c': c})
        batch.put_item({'p': p, 'c': other})
        batch.put_item({'p': other, 'c': c})

def test_batch_write_duplicate_delete(test_table_s, test_table):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException.*duplicates'):
        with test_table_s.batch_writer() as batch:
            batch.delete_item(Key={'p': p})
            batch.delete_item(Key={'p': p})
    c = random_string()
    with pytest.raises(ClientError, match='ValidationException.*duplicates'):
        with test_table.batch_writer() as batch:
            batch.delete_item(Key={'p': p, 'c': c})
            batch.delete_item(Key={'p': p, 'c': c})
    # But it is fine to touch items with one component the same, but the other not.
    other = random_string()
    with test_table.batch_writer() as batch:
        batch.delete_item(Key={'p': p, 'c': c})
        batch.delete_item(Key={'p': p, 'c': other})
        batch.delete_item(Key={'p': other, 'c': c})

def test_batch_write_duplicate_write_and_delete(test_table_s, test_table):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException.*duplicates'):
        with test_table_s.batch_writer() as batch:
            batch.delete_item(Key={'p': p})
            batch.put_item({'p': p})
    c = random_string()
    with pytest.raises(ClientError, match='ValidationException.*duplicates'):
        with test_table.batch_writer() as batch:
            batch.delete_item(Key={'p': p, 'c': c})
            batch.put_item({'p': p, 'c': c})
    # But it is fine to touch items with one component the same, but the other not.
    other = random_string()
    with test_table.batch_writer() as batch:
        batch.delete_item(Key={'p': p, 'c': c})
        batch.put_item({'p': p, 'c': other})
        batch.put_item({'p': other, 'c': c})

# The BatchWriteIem API allows writing to more than one table in the same
# batch. This test verifies that the duplicate-key checking doesn't mistake
# updates to the same key in different tables to be duplicates.
def test_batch_write_nonduplicate_multiple_tables(test_table_s, test_table_s_2):
    p = random_string()
    # The batch_writer() function used in previous tests can't write to more
    # than one table. So we use the lower level interface boto3 gives us.
    reply = test_table_s.meta.client.batch_write_item(RequestItems = {
        test_table_s.name: [{'PutRequest': {'Item': {'p': p, 'a': 'hi'}}}],
        test_table_s_2.name: [{'PutRequest': {'Item': {'p': p, 'b': 'hello'}}}]
    })
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 'hi'}
    assert test_table_s_2.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'b': 'hello'}

# Test that BatchWriteItem's PutRequest completely replaces an existing item.
# It shouldn't merge it with a previously existing value. See also the same
# test for PutItem - test_put_item_replace().
def test_batch_put_item_replace(test_table_s, test_table):
    p = random_string()
    with test_table_s.batch_writer() as batch:
        batch.put_item(Item={'p': p, 'a': 'hi'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'a': 'hi'}
    with test_table_s.batch_writer() as batch:
        batch.put_item(Item={'p': p, 'b': 'hello'})
    assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item'] == {'p': p, 'b': 'hello'}
    c = random_string()
    with test_table.batch_writer() as batch:
        batch.put_item(Item={'p': p, 'c': c, 'a': 'hi'})
    assert test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)['Item'] == {'p': p, 'c': c, 'a': 'hi'}
    with test_table.batch_writer() as batch:
        batch.put_item(Item={'p': p, 'c': c, 'b': 'hello'})
    assert test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)['Item'] == {'p': p, 'c': c, 'b': 'hello'}

# Test that if one of the batch's operations is invalid, because a key
# column is missing or has the wrong type, the entire batch is rejected
# before any write is done.
def test_batch_write_invalid_operation(test_table_s):
    # test key attribute with wrong type:
    p1 = random_string()
    p2 = random_string()
    items = [{'p': p1}, {'p': 3}, {'p': p2}]
    with pytest.raises(ClientError, match='ValidationException'):
        with test_table_s.batch_writer() as batch:
            for item in items:
                batch.put_item(item)
    for p in [p1, p2]:
        assert not 'item' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)
    # test missing key attribute:
    p1 = random_string()
    p2 = random_string()
    items = [{'p': p1}, {'x': 'whatever'}, {'p': p2}]
    with pytest.raises(ClientError, match='ValidationException'):
        with test_table_s.batch_writer() as batch:
            for item in items:
                batch.put_item(item)
    for p in [p1, p2]:
        assert not 'item' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)

# In test_item.py we have a bunch of test_empty_* tests on different ways to
# create an empty item (which in Scylla requires the special CQL row marker
# to be supported correctly). BatchWriteItems provides yet another way of
# creating items, so check the empty case here too:
def test_empty_batch_write(test_table):
    p = random_string()
    c = random_string()
    with test_table.batch_writer() as batch:
        batch.put_item({'p': p, 'c': c})
    assert test_table.get_item(Key={'p': p, 'c': c}, ConsistentRead=True)['Item'] == {'p': p, 'c': c}

# Test that BatchWriteItems allows writing to multiple tables in one operation
def test_batch_write_multiple_tables(test_table_s, test_table):
    p1 = random_string()
    c1 = random_string()
    p2 = random_string()
    # We use the low-level batch_write_item API for lack of a more convenient
    # API (the batch_writer() API can only write to one table). At least it
    # spares us the need to encode the key's types...
    reply = test_table.meta.client.batch_write_item(RequestItems = {
        test_table.name: [{'PutRequest': {'Item': {'p': p1, 'c': c1, 'a': 'hi'}}}],
        test_table_s.name: [{'PutRequest': {'Item': {'p': p2, 'b': 'hello'}}}]
    })
    assert test_table.get_item(Key={'p': p1, 'c': c1}, ConsistentRead=True)['Item'] == {'p': p1, 'c': c1, 'a': 'hi'}
    assert test_table_s.get_item(Key={'p': p2}, ConsistentRead=True)['Item'] == {'p': p2, 'b': 'hello'}

# Basic test for BatchGetItem, reading several entire items.
# Schema has both hash and sort keys.
def test_batch_get_item(test_table):
    items = [{'p': random_string(), 'c': random_string(), 'val': random_string()} for i in range(10)]
    with test_table.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    keys = [{k: x[k] for k in ('p', 'c')} for x in items]
    # We use the low-level batch_get_item API for lack of a more convenient
    # API. At least it spares us the need to encode the key's types...
    reply = test_table.meta.client.batch_get_item(RequestItems = {test_table.name: {'Keys': keys, 'ConsistentRead': True}})
    print(reply)
    got_items = reply['Responses'][test_table.name]
    assert multiset(got_items) == multiset(items)

# Same, with schema has just hash key.
def test_batch_get_item_hash(test_table_s):
    items = [{'p': random_string(), 'val': random_string()} for i in range(10)]
    with test_table_s.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    keys = [{k: x[k] for k in ('p')} for x in items]
    reply = test_table_s.meta.client.batch_get_item(RequestItems = {test_table_s.name: {'Keys': keys, 'ConsistentRead': True}})
    got_items = reply['Responses'][test_table_s.name]
    assert multiset(got_items) == multiset(items)

# Test what do we get if we try to read two *missing* values in addition to
# an existing one. It turns out the missing items are simply not returned,
# with no sign they are missing.
def test_batch_get_item_missing(test_table_s):
    p = random_string();
    test_table_s.put_item(Item={'p': p})
    reply = test_table_s.meta.client.batch_get_item(RequestItems = {test_table_s.name: {'Keys': [{'p': random_string()}, {'p': random_string()}, {'p': p}], 'ConsistentRead': True}})
    got_items = reply['Responses'][test_table_s.name]
    assert got_items == [{'p' : p}]

# If all the keys requested from a particular table are missing, we still
# get a response array for that table - it's just empty.
def test_batch_get_item_completely_missing(test_table_s):
    reply = test_table_s.meta.client.batch_get_item(RequestItems = {test_table_s.name: {'Keys': [{'p': random_string()}], 'ConsistentRead': True}})
    got_items = reply['Responses'][test_table_s.name]
    assert got_items == []

# Test GetItem with AttributesToGet
def test_batch_get_item_attributes_to_get(test_table):
    items = [{'p': random_string(), 'c': random_string(), 'val1': random_string(), 'val2': random_string()} for i in range(10)]
    with test_table.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    keys = [{k: x[k] for k in ('p', 'c')} for x in items]
    for wanted in [['p'], ['p', 'c'], ['val1'], ['p', 'val2']]:
        reply = test_table.meta.client.batch_get_item(RequestItems = {test_table.name: {'Keys': keys, 'AttributesToGet': wanted, 'ConsistentRead': True}})
        got_items = reply['Responses'][test_table.name]
        expected_items = [{k: item[k] for k in wanted if k in item} for item in items]
        assert multiset(got_items) == multiset(expected_items)

# Test GetItem with ProjectionExpression (just a simple one, with
# top-level attributes)
def test_batch_get_item_projection_expression(test_table):
    items = [{'p': random_string(), 'c': random_string(), 'val1': random_string(), 'val2': random_string()} for i in range(10)]
    with test_table.batch_writer() as batch:
        for item in items:
            batch.put_item(item)
    keys = [{k: x[k] for k in ('p', 'c')} for x in items]
    for wanted in [['p'], ['p', 'c'], ['val1'], ['p', 'val2']]:
        reply = test_table.meta.client.batch_get_item(RequestItems = {test_table.name: {'Keys': keys, 'ProjectionExpression': ",".join(wanted), 'ConsistentRead': True}})
        got_items = reply['Responses'][test_table.name]
        expected_items = [{k: item[k] for k in wanted if k in item} for item in items]
        assert multiset(got_items) == multiset(expected_items)

# Test that we return the required UnprocessedKeys/UnprocessedItems parameters
def test_batch_unprocessed(test_table_s):
    p = random_string()
    write_reply = test_table_s.meta.client.batch_write_item(RequestItems = {
        test_table_s.name: [{'PutRequest': {'Item': {'p': p, 'a': 'hi'}}}],
    })
    assert 'UnprocessedItems' in write_reply and write_reply['UnprocessedItems'] == dict()

    read_reply = test_table_s.meta.client.batch_get_item(RequestItems = {
        test_table_s.name: {'Keys': [{'p': p}], 'ProjectionExpression': 'p, a', 'ConsistentRead': True}
    })
    assert 'UnprocessedKeys' in read_reply and read_reply['UnprocessedKeys'] == dict()

# According to the DynamoDB document, a single BatchWriteItem operation is
# limited to 25 update requests, up to 400 KB each, or 16 MB total (25*400
# is only 10 MB, but the JSON format has additional overheads). If we write
# less than those limits in a single BatchWriteItem operation, it should
# work. Testing a large request exercises our code which calculates the
# request signature, and parses a long request (issue #7213) as well as the
# resulting long write (issue #8183).
def test_batch_write_item_large(test_table_sn):
    p = random_string()
    long_content = random_string(100)*500
    write_reply = test_table_sn.meta.client.batch_write_item(RequestItems = {
        test_table_sn.name: [{'PutRequest': {'Item': {'p': p, 'c': i, 'content': long_content}}} for i in range(25)],
    })
    assert 'UnprocessedItems' in write_reply and write_reply['UnprocessedItems'] == dict()
    assert full_query(test_table_sn, KeyConditionExpression='p=:p', ExpressionAttributeValues={':p': p}
        ) == [{'p': p, 'c': i, 'content': long_content} for i in range(25)]

# According to the DynamoDB documention, a single BatchGetItem operation is
# limited to retrieving up to 100 items or a total of 16 MB of data,
# whichever is smaller. If we read less than those limits in a single
# BatchGetItem operation, it should work - though may still return only
# partial results (filling UnprocessedKeys instead). Testing a BatchGetItem
# with a large response exercises our code which builds such large responses
# and how it may cause warnings about excessively large allocations or long
# unpreemptable computation (see issues #8661, #7926).
def test_batch_get_item_large(test_table_sn):
    p = random_string()
    long_content = random_string(100)*500
    count = 30
    with test_table_sn.batch_writer() as batch:
        for i in range(count):
            batch.put_item(Item={
                'p': p, 'c': i, 'content': long_content})
    # long_content is 49 KB, 30 such items is about 1.5 MB, well below the
    # BatchGetItem response limit, so the following BatchGetItem call should
    # be able to return all items we just wrote in one response - and in fact
    # does so in Alternator and thus exercises its handling of large
    # responses. Strangely, in DynamoDB, even though we're below the limit,
    # it often returns only partial results (with the unread keys in
    # UnprocessedKeys), so for reliable success of this test we need a loop:
    responses = []
    to_read = { test_table_sn.name: {'Keys': [{'p': p, 'c': c} for c in range(count)], 'ConsistentRead': True } }
    while to_read:
        reply = test_table_sn.meta.client.batch_get_item(RequestItems = to_read)
        assert 'UnprocessedKeys' in reply
        to_read = reply['UnprocessedKeys']
        assert 'Responses' in reply
        assert test_table_sn.name in reply['Responses']
        responses.extend(reply['Responses'][test_table_sn.name])
    assert multiset(responses) == multiset(
        [{'p': p, 'c': i, 'content': long_content} for i in range(count)])
