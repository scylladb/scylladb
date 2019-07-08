# Various utility functions which are useful for multiple tests

import string
import random
import collections
import time

def random_string(length=10, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(length))

def random_bytes(length=10):
    return bytearray(random.getrandbits(8) for _ in range(length))

# Utility functions for scan and query into an array of items:
# TODO: add to full_scan and full_query by default ConsistentRead=True, as
# it's not useful for tests without it!
def full_scan(table, **kwargs):
    response = table.scan(**kwargs)
    items = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'], **kwargs)
        items.extend(response['Items'])
    return items

# Utility function for fetching the entire results of a query into an array of items
def full_query(table, **kwargs):
    response = table.query(**kwargs)
    items = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.query(ExclusiveStartKey=response['LastEvaluatedKey'], **kwargs)
        items.extend(response['Items'])
    return items

# To compare two lists of items (each is a dict) without regard for order,
# "==" is not good enough because it will fail if the order is different.
# The following function, multiset() converts the list into a multiset
# (set with duplicates) where order doesn't matter, so the multisets can
# be compared.

def freeze(item):
    if isinstance(item, dict):
        return frozenset((key, freeze(value)) for key, value in item.items())
    elif isinstance(item, list):
        return tuple(freeze(value) for value in item)
    return item

def multiset(items):
    return collections.Counter([freeze(item) for item in items])


test_table_prefix = 'alternator_test_'
def test_table_name():
    current_ms = int(round(time.time() * 1000))
    # In the off chance that test_table_name() is called twice in the same millisecond...
    if test_table_name.last_ms >= current_ms:
        current_ms = test_table_name.last_ms + 1
    test_table_name.last_ms = current_ms
    return test_table_prefix + str(current_ms)
test_table_name.last_ms = 0

def create_test_table(dynamodb, **kwargs):
    name = test_table_name()
    print("fixture creating new table {}".format(name))
    table = dynamodb.create_table(TableName=name,
        BillingMode='PAY_PER_REQUEST', **kwargs)
    waiter = table.meta.client.get_waiter('table_exists')
    # recheck every second instead of the default, lower, frequency. This can
    # save a few seconds on AWS with its very slow table creation, but can
    # more on tests on Scylla with its faster table creation turnaround.
    waiter.config.delay = 1
    waiter.config.max_attempts = 60
    waiter.wait(TableName=name)
    return table
