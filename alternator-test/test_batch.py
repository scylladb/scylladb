# Tests for batch operations - BatchWriteItem, BatchReadItem.
# Note that various other tests in other files also use these operations,
# so they are actually tested by other tests as well.

import pytest
import random
import string

from botocore.exceptions import ClientError

def random_string(length=10, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(length))

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
@pytest.mark.xfail(reason="BatchWriteItem does not yet check for duplicates")
def test_batch_write_duplicate_write(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException.*duplicates'):
        with test_table_s.batch_writer() as batch:
            batch.put_item({'p': p})
            batch.put_item({'p': p})

@pytest.mark.xfail(reason="BatchWriteItem does not yet check for duplicates")
def test_batch_write_duplicate_delete(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException.*duplicates'):
        with test_table_s.batch_writer() as batch:
            batch.delete_item(Key={'p': p})
            batch.delete_item(Key={'p': p})

@pytest.mark.xfail(reason="BatchWriteItem does not yet check for duplicates")
def test_batch_write_duplicate_write_and_delete(test_table_s):
    p = random_string()
    with pytest.raises(ClientError, match='ValidationException.*duplicates'):
        with test_table_s.batch_writer() as batch:
            batch.delete_item(Key={'p': p})
            batch.put_item({'p': p})

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
